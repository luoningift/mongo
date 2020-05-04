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
     * @var MongoDB
     */
    protected $connection;

    /**
     * @var array
     */
    protected $config = [
        'url' => '127.0.0.1:27017,mongo.com:27017',
        'db' => 'admin',
        'username' => '',
        'password' => '',
        'replica_set' => '',
        'auth_source' => 'admin',
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

    /**
     * Current redis database.
     * @var null|int
     */
    protected $database;

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
        $this->reconnect();
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
        if ($conRes) {
            $this->connection = $this->mongoDb;
        }
        return $conRes;
    }

    public function close(): bool
    {
        $this->sock->close();
        $this->sock = null;
        $this->protocol = null;
        $this->mongoDb = null;
        return true;
    }


    private function closeReplica()
    {

        $this->replicaSock->close();
        $this->replicaSock = null;
        $this->replicaProtocol = null;
        $this->replicaMongoDb = null;
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
                return $this->connectToHost($host[0], $host[1], $isReplica);
            } catch (\Exception $e) {
                continue;
            }
        }
        return false;
    }

    private function connectToHost($host, $port, $isReplica = false): bool
    {

        $sock = new Socket($host, $port, $this->config['pool']['connect_timeout']);
        if (!$isReplica) {
            $this->sock = $sock;
            $this->protocol = new Protocol($sock);
            $this->mongoDb = new MongoDB($this->protocol, $this->config['db']);
            if ($this->config['username'] != '') {
                $this->mongoDb->authenticate($this->config['username'], $this->config['password'], [
                    'protocol' => $this->protocol,
                ]);
            }
        } else {
            $this->replicaSock = $sock;
            $this->replicaProtocol = new Protocol($sock);
            $this->replicaMongoDb = new MongoDB($this->replicaProtocol, $this->config['auth_source']);
            if ($this->config['username'] != '') {
                $this->replicaMongoDb->authenticate($this->config['username'], $this->config['password'], [
                    'protocol' => $this->protocol,
                ]);
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