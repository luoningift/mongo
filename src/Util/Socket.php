<?php

namespace Kaikeba\Mongo\Util;

use Kaikeba\Mongo\Exception\MongoConnectionException;
use Swoole\Coroutine\Client;

class Socket
{
    private $sock;
    private $host;
    private $port;
    private $connectTimeout;

    private $lastRequestId = 3;

    public function __construct($host, $port, $connectTimeout)
    {
        $this->host = $host;
        $this->port = $port;
        $this->connectTimeout = $connectTimeout;
    }

    public function connect()
    {
        if (filter_var($this->host, FILTER_VALIDATE_IP)) {
            $ip = $this->host;
        } else {
            $ip = gethostbyname($this->host);
            if ($ip == $this->host) {
                throw new MongoConnectionException(sprintf(
                    'couldn\'t get host info for %s',
                    $this->host
                ));
            }
        }
        $sock = new Client(SWOOLE_SOCK_TCP);
        $sock->set(array(
            'timeout' => 0.5,
            'connect_timeout' => $this->connectTimeout,
            'write_timeout' => 100.0,
            'read_timeout' => 100.0,
        ));
        if (!$sock->connect($ip, $this->port, $this->connectTimeout)) {
            throw new MongoConnectionException(
                sprintf(
                    'Error Connecting to server(%s): %s ',
                    $sock->errCode,
                    swoole_strerror($sock->errCode)
                ),
                $sock->errCode
            );
        }
        $this->sock = $sock;
    }

    public function close()
    {
        if (isset($this->sock) && $this->sock instanceof Client) {
            $this->sock->close();
        }
        $this->sock = null;
    }

    /**
     * Reconnects the socket.
     */
    public function reconnect()
    {
        $this->close();
        $this->connect();
    }

    public function putReadMessage($opCode, $opData, $timeout)
    {
        $requestId = $this->getNextLastRequestId();
        $payload = $this->packMessage($requestId, $opCode, $opData);

        $this->putMessage($payload);

        return $this->getMessage($requestId, $timeout);
    }

    public function putWriteMessage($opCode, $opData, array $options, $timeout)
    {
        $requestId = $this->getNextLastRequestId();
        $payload = $this->packMessage($requestId, $opCode, $opData);

        $lastError = $this->createLastErrorMessage($options);
        if ($lastError) {
            $requestId = $this->getNextLastRequestId();
            $payload .= $this->packMessage($requestId, Protocol::OP_QUERY, $lastError);
        }

        $this->putMessage($payload);

        if ($lastError) {
            $response = $this->getMessage($requestId, $timeout);
            return $response['result'][0];
        }

        return true;
    }

    public function getServerHash()
    {
        return "$this->host:$this->port";
    }

    protected function getNextLastRequestId()
    {
        if ($this->lastRequestId > 99999999) {
            $this->lastRequestId = 3;
        }
        return $this->lastRequestId++;
    }

    protected function createLastErrorMessage(array $options)
    {
        $command = array_merge(['getLastError' => 1], $options);
        if (!isset($command['w'])) {
            $command['w'] = 1;
        }
        if (!isset($command['j'])) {
            $command['j'] = false;
        }
        if (!isset($command['wtimeout'])) {
            $command['wtimeout'] = 10000;
        }
        if ($command['w'] === 0 && $command['j'] === false) {
            return;
        }
        return pack('Va*VVa*', 0, "admin.\$cmd\0", 0, -1, Bson::encode($command));
    }

    protected function packMessage($requestId, $opCode, $opData, $responseTo = 0xffffffff)
    {
        $bytes = strlen($opData) + Protocol::MSG_HEADER_SIZE;

        return pack('V4', $bytes, $requestId, $responseTo, $opCode) . $opData;
    }

    protected function putMessage($payload)
    {
        $buffer = $this->sock->send($payload);
        if ($buffer === false) {
            throw new \RuntimeException('Error sending data');
        }
    }

    protected function getMessage($requestId, $timeout)
    {

        $header = $this->readHeaderFromSocket();
        if ($requestId != $header['responseTo']) {
            throw new \RuntimeException(sprintf(
                'request/cursor mismatch: %d vs %d',
                $requestId,
                $header['responseTo']
            ));
        }
        $data = $this->readFromSocket(
            $header['messageLength'] - Protocol::MSG_HEADER_SIZE
        );
        $header = substr($data, 0, 20);
        $vars = unpack('Vflags/V2cursorId/VstartingFrom/VnumberReturned', $header);

        $documents = Bson::decode_multiple(substr($data, 20));
        if ($documents) {
            throw new \RuntimeException(sprintf(
                'not document request/cursor mismatch: %d vs %d',
                $requestId,
                $header['responseTo']
            ));
        } else {
            $documents = [];
        }
        return [
            'result' => $documents,
            'cursorId' => Util::decodeInt64($vars['cursorId1'], $vars['cursorId2']) ?: null,
            'start' => $vars['startingFrom'],
            'count' => $vars['numberReturned'],
        ];
    }

    protected function readHeaderFromSocket()
    {
        $data = $this->readFromSocket(Protocol::MSG_HEADER_SIZE);
        $header = unpack('VmessageLength/VrequestId/VresponseTo/Vopcode', $data);

        return $header;
    }

    public function readFromSocket($len)
    {
        $buffer = '';
        do {
            if ($len <= strlen($buffer)) {
                return substr($buffer, 0, $len);
            }
            if (!$this->sock->connected) {
                throw new \RuntimeException('read failed');
            }
            $readBuffer = $this->sock->recv(100.0);
            if ($readBuffer === false) {
                throw new \RuntimeException('read failed');
            }
            if ($readBuffer === '') {
                throw new \RuntimeException('read failed');
            }
            $buffer .= $readBuffer;
        } while (true);
        return false;
    }
}
