<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://doc.hyperf.io
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */

namespace Kaikeba\Mongo;

use Hyperf\Contract\ConnectionInterface;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Pool\Connection as BaseConnection;
use Hyperf\Pool\Exception\ConnectionException;
use Hyperf\Pool\Pool;
use Kaikeba\Mongo\Exception\MongoConnectionException;
use Kaikeba\Mongo\Util\Protocol;
use Kaikeba\Mongo\Util\Socket;
use Psr\Container\ContainerInterface;

/**
 * @method bool select(int $db)
 */
class MongoConnection extends BaseConnection implements ConnectionInterface
{

    /**
     * @var array
     */
    protected $config = [
        'url' => '127.0.0.1:27017,mongo.com:27017',
        'db' => 'admin',
        'username' => '',
        'password' => '',
        'auth_source' => '',
        'replica_set' => '',
        'replica_username' => '',
        'replica_password' => '',
        'replica_auth_source' => '',
        'pool' => [
            'min_connections' => 1,
            'max_connections' => 20,
            'connect_timeout' => 1.0,
            'wait_timeout' => 3.0,
            'heartbeat' => -1,
            'max_idle_time' => 60,
        ],
    ];

    protected $replicaSock = null;
    protected $replicaProtocol = null;
    protected $replicaMongoDb = null;

    protected $sock = null;
    protected $protocol = null;
    protected $mongoDb = null;

    public function __construct(ContainerInterface $container, Pool $pool, array $config)
    {
        parent::__construct($container, $pool);
        $this->config = array_replace($this->config, $config);
        $urls = explode(',', $this->config['url']);
        $urlsArr = [];
        foreach ($urls as $k => $v) {
            $tmp = explode(':', $v);
            $urlsArr[] = $tmp;
        }
        $this->config['url'] = $urlsArr;
    }

    public function __call($name, $arguments)
    {
        try {
            $result = $this->mongoDb->{$name}(...$arguments);
        } catch (\Throwable $exception) {
            $result = $this->retry($name, $arguments, $exception);
        }
        return $result;
    }

    public function reconnect(): bool
    {
        $this->close();
        if ($this->config['replica_set']) {
            $conRes = $this->connectToReplSet();
            $this->closeReplica();
        } else {
            $conRes = $this->connectToFirstAvailableHost();
        }
        $this->lastUseTime = microtime(true);
        return $conRes;
    }

    public function close(): bool
    {
        if ($this->sock) {
            $this->sock->close();
            $this->sock = null;
        }
        if ($this->protocol) {
            $this->protocol = null;
        }
        if ($this->mongoDb) {
            $this->mongoDb = null;
        }
        return true;
    }


    private function closeReplica()
    {
        if ($this->replicaSock) {
            $this->replicaSock->close();
            $this->replicaSock = null;
        }
        if ($this->replicaProtocol) {
            $this->replicaProtocol = null;
        }
        if ($this->replicaMongoDb) {
            $this->replicaMongoDb = null;
        }
    }

    public function getActiveConnection()
    {
        if ($this->check()) {
            return $this;
        }
        if (!$this->reconnect()) {
            throw new ConnectionException('Connection reconnect failed.');
        }

        return $this;
    }

    private function connectToReplSet(): bool
    {

        if (!$this->connectToFirstAvailableHost(true)) {
            return false;
        }
        try {
            $isMaster = $this->replicaMongoDb->command([
                'isMaster' => 1,
            ], [
                'protocol' => $this->replicaProtocol
            ]);
        } catch (\Throwable $exception) {
            return false;
        }
        if (!isset($isMaster['setName']) || $isMaster['setName'] != $this->config['replica_set']) {
            return false;
        }

        $mHosts = isset($isMaster['hosts']) ? $isMaster['hosts'] : [];
        $rpHosts = isset($isMaster['passive']) ? $isMaster['passive'] : [];
        $allHosts = array_merge($mHosts, $rpHosts);
        foreach ($allHosts as $hostStr) {
            try {
                $hostPart = $this->parseHostString($hostStr);
                if ($hostStr === $isMaster['primary']) {
                    return $this->connectToHost($hostPart['host'], $hostPart['port'], false);
                }
            } catch (\Exception $e) {
                continue;
            }
        }
        return false;
    }

    protected function parseHostString($host_str)
    {
        if (preg_match('/\A([a-zA-Z0-9_.-]+)(:([0-9]+))?\z/', $host_str, $matches)) {
            $host = $matches[1];
            $port = (isset($matches[3])) ? $matches[3] : static::DEFAULT_PORT;
            if ($port > 0 && $port <= 65535) {
                return array('host' => $host, 'port' => $port, 'hash' => "$host:$port");
            }
        }
        throw new MongoConnectionException('malformed host string: ' . $host_str);
    }

    private function connectToFirstAvailableHost($isReplica = false): bool
    {
        $latest_error = null;
        shuffle($this->config['url']);
        foreach ($this->config['url'] as $host) {
            try {
                if ($this->connectToHost($host[0], $host[1], $isReplica)) {
                    return true;
                }
            } catch (\Exception $e) {
                continue;
            }
        }
        return false;
    }

    private function connectToHost($host, $port, $isReplica = false): bool
    {

        $sock = new Socket($host, $port, $this->config['pool']['connect_timeout']);
        $sock->connect();
        if (!$isReplica) {
            $this->sock = $sock;
            $this->protocol = new Protocol($this->sock);
            $this->mongoDb = new MongoDB($this->protocol, $this->config['db'], $this->config['auth_source']);
            if ($this->config['username'] != '') {
                if (!$this->mongoDb->auth($this->config['username'], $this->config['password'])) {
                    $this->sock->close();
                    $this->sock = null;
                    $this->protocol = null;
                    $this->mongoDb = null;
                    return false;
                }
            }
        } else {
            $this->replicaSock = $sock;
            $this->replicaProtocol = new Protocol($this->replicaSock);
            $this->replicaMongoDb = new MongoDB($this->replicaProtocol, $this->config['replica_auth_source'] ? $this->config['replica_auth_source'] : 'admin', $this->config['replica_auth_source']);
            if ($this->config['replica_username'] != '') {
                if (!$this->replicaMongoDb->auth($this->config['replica_username'], $this->config['replica_password'])) {
                    $this->replicaSock->close();
                    $this->replicaSock = null;
                    $this->protocol = null;
                    $this->mongoDb = null;
                    return false;
                }
            }
        }
        return true;
    }

    public function release(): void
    {
        parent::release();
    }

    protected function retry($name, $arguments, \Throwable $exception)
    {
        $logger = $this->container->get(StdoutLoggerInterface::class);
        $logger->warning(sprintf('mongo::__call failed, because ' . $exception->getMessage()));
        try {
            $this->reconnect();
            $result = $this->mongoDb->{$name}(...$arguments);
        } catch (\Throwable $exception) {
            $this->lastUseTime = 0.0;
            throw $exception;
        }

        return $result;
    }
}
