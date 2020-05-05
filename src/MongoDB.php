<?php

namespace Kaikeba\Mongo;

use Kaikeba\Mongo\Types\MongoBinData;
use Kaikeba\Mongo\Util\Protocol;

class MongoDB
{
    const PROFILING_OFF = 0;
    const PROFILING_SLOW = 1;
    const PROFILING_ON = 2;
    const NAMESPACES_COLLECTION = 'system.namespaces';
    const INDEX_COLLECTION = 'system.indexes';

    /**
     * @var int
     */
    public $w = 1;

    /**
     * @var int
     */
    public $w_timeout = 10000;

    /**
     * @var string
     */
    private $name;

    /**
     * @var MongoConnection
     */
    private $client;

    /**
     * @var array
     */
    private $collections = [];

    private $authDbName = 'admin';

    /**
     * Gets a collection
     *
     * @param string $name - The name of the collection.
     *
     * @return MongoCollection - Returns the collection.
     */
    public function __get($name)
    {
        return $this->selectCollection($name);
    }

    /**
     * Creates a new database
     *
     * @param MongoConnection $client - Database connection.
     * @param string $name - Database name.
     */
    public function __construct(Protocol $client, $name, $authDbName)
    {
        $this->name = $name;
        $this->client = $client;
        if ($authDbName) {
            $this->authDbName = $authDbName;
        }
    }

    public function getProtoCol()
    {
        return $this->client;
    }

    /**
     * Gets a collection
     *
     * @param string $name - The collection name.
     *
     * @return MongoCollection - Returns a new collection object.
     */
    public function selectCollection($name)
    {
        if (!isset($this->collections[$name])) {
            $this->collections[$name] = new MongoCollection($this, $name);
        }

        return $this->collections[$name];
    }

    public function _getFullCollectionName($collectionName)
    {
        return $this->name . '.' . $collectionName;
    }

    /**
     * Execute a database command
     *
     * @param array $command - The query to send.
     * @param array $options - This parameter is an associative array of
     *   the form array("optionname" => boolean, ...).
     *
     * @return array - Returns database response.
     */
    public function command(array $cmd, array $options = [], $isAuth = false)
    {
        $timeout = MongoCursor::$timeout;
        if (!empty($options['timeout'])) {
            $timeout = $options['timeout'];
        }

        $protocol = empty($options['protocol'])
            ? $this->client
            : $options['protocol'];

        $dbname = $isAuth ? $this->authDbName : $this->name;

        $response = $protocol->opQuery(
            "{$dbname}.\$cmd",
            $cmd,
            0, -1, 0,
            $timeout
        );

        return $response['result'][0];
    }

    /**
     * Get all collections from this database
     *
     * @param bool $includeSystemCollections -
     *
     * @return array - Returns the names of the all the collections in the
     *   database as an array.
     */
    public function getCollectionNames($includeSystemCollections = false)
    {
        $collections = [];
        $namespaces = $this->selectCollection(self::NAMESPACES_COLLECTION);
        foreach ($namespaces->find() as $collection) {
            if (
                !$includeSystemCollections &&
                $this->isSystemCollection($collection['name'])
            ) {
                continue;
            }

            if ($this->isAnIndexCollection($collection['name'])) {
                continue;
            }

            $collections[] = $this->getCollectionName($collection['name']);
        }

        return $collections;
    }

    /**
     * Gets an array of all MongoCollections for this database
     *
     * @param bool $includeSystemCollections -
     *
     * @return array - Returns an array of MongoCollection objects.
     */
    public function listCollections($includeSystemCollections = false)
    {
        $collections = [];
        $names = $this->getCollectionNames($includeSystemCollections);
        foreach ($names as $name) {
            $collections[] = $this->selectCollection($name);
        }

        return $collections;
    }

    private function isAnIndexCollection($namespace)
    {
        return !strpos($namespace, '$') === false;
    }


    private function isSystemCollection($namespace)
    {
        return !strpos($namespace, '.system.') === false;
    }

    private function getCollectionName($namespace)
    {
        $dot = strpos($namespace, '.');

        return substr($namespace, $dot + 1);
    }

    public function getIndexesCollection()
    {
        return $this->selectCollection(self::INDEX_COLLECTION);
    }

    /**
     * Log in to this database
     *
     * @param string $username - The username.
     * @param string $password - The password (in plaintext).
     * @param array $options - This parameter is an associative array of
     *   the form array("optionname" => boolean, ...).
     *
     * @return array - Returns database response. If the login was
     *   successful, it will return    If something went wrong, it will
     *   return    ("auth fails" could be another message, depending on
     *   database version and what when wrong).
     */
    private function authenticateCR($username, $password, $options = [])
    {
        $response = $this->command(['getnonce' => 1], $options, true);
        if (!isset($response['nonce'])) {
            throw new \Exception('Cannot get nonce');
        }

        $nonce = $response['nonce'];

        $passwordDigest = md5(sprintf('%s:mongo:%s', $username, $password));
        $digest = md5(sprintf('%s%s%s', $nonce, $username, $passwordDigest));

        return $this->command([
            'authenticate' => 1,
            'user' => $username,
            'nonce' => $nonce,
            'key' => $digest,
        ], $options, true);
    }

    /**
     * 认证服务
     * @param $username
     * @param $password
     * @param array $options
     * @return array|bool
     * @throws \Exception
     */
    public function auth($username, $password, $options = []) {

        $auth = $this->authenticateSHA256($username, $password, $options);
        if (!$auth) {
            $auth = $this->authenticateSHA1($username, $password, $options);
        }
        if (!$auth) {
            $auth = $this->authenticateCR($username, $password, $options);
        }
        return $auth;
    }

    private function authenticateSHA256($username, $password, $options = [])
    {

        $nonce = mt_rand(0, 10000);
        $username = str_replace('=', '=3D', $username);
        $username = str_replace(',', '=2C', $username);
        $payLoad = 'n,,n=' . $username . ',r=' . $nonce;
        $response = $this->command([
            'saslStart' => 1,
            'mechanism' => 'SCRAM-SHA-256',
            'payload' => new MongoBinData($payLoad, MongoBinData::GENERIC),
            'autoAuthorize' => 1,
        ], array_merge($options, ['skipEmptyExchange' => true]), true);
        if (is_array($response) && isset($response['ok']) && $response['ok'] != 1) {
            return false;
        }
        $dist = explode(',', $response['payload']->bin);
        $tmpDist = [];
        foreach ($dist as $k => $v) {
            $tmpDist[substr($v, 0, 1)] = substr($v, 2);
        }
        $iterations = intval($tmpDist['i']);
        if ($iterations < 4096) {
            return false;
        }
        $salt = base64_decode($tmpDist['s']);
        $rnonce = $tmpDist['r'];
        if (substr($rnonce, 0, 5) == 'nonce') {
            return false;
        }
        $h = function ($data) {
            return hash('sha256', $data, true);
        };
        $hmac = function ($data, $key) {
            return hash_hmac('sha256', $data, $key, true);
        };
        $hi = function ($password, $salt, $iterations) {
            return hash_pbkdf2('sha256', $password, $salt, $iterations, 32, true);
        };
        $withoutProof = 'c=biws,r=' . $rnonce;
        $processedPassword = $password;
        $saltedPassword = $hi(
            $processedPassword,
            $salt,
            $iterations,
        );
        $clientKey = $hmac('Client Key', $saltedPassword);
        $serverKey = $hmac('Server Key', $saltedPassword);
        $storedKey = $h($clientKey);
        $authMessage = 'n=' . $username . ',r=' . $nonce . ',' . $response['payload']->bin . ',' . $withoutProof;
        $clientSignature = $hmac($authMessage, $storedKey);
        $clientProof = 'p=' . base64_encode($clientKey ^ $clientSignature);
        $clientFinal = $withoutProof . ',' . $clientProof;
        $serverSignature = $hmac($authMessage, $serverKey);
        $saslContinueCmd = [
            'saslContinue' => 1,
            'conversationId' => $response['conversationId'],
            'payload' => new MongoBinData($clientFinal, MongoBinData::GENERIC)
        ];
        $secondResponse = $this->command($saslContinueCmd, $options, true);
        if (is_array($secondResponse) && isset($secondResponse['ok']) && $secondResponse['ok'] != 1) {
            return false;
        }
        $dist = explode(',', $secondResponse['payload']->bin);
        $tmpDist = [];
        foreach ($dist as $k => $v) {
            $tmpDist[substr($v, 0, 1)] = substr($v, 2);
        }
        if ($tmpDist['v'] !== base64_encode($serverSignature)) {
            return false;
        }
        $retrySaslContinueCmd = [
            'saslContinue' => 1,
            'conversationId' => $secondResponse['conversationId'],
            'payload' => ''
        ];
        $thirdResponse = $this->command($retrySaslContinueCmd, $options, true);
        if (is_array($thirdResponse) && isset($thirdResponse['done']) && $thirdResponse['done'] == 1) {
            return true;
        }
        return false;
    }

    private function authenticateSHA1($username, $password, $options = [])
    {

        $nonce = mt_rand(0, 10000);
        $username = str_replace('=', '=3D', $username);
        $username = str_replace(',', '=2C', $username);
        $payLoad = 'n,,n=' . $username . ',r=' . $nonce;
        $response = $this->command([
            'saslStart' => 1,
            'mechanism' => 'SCRAM-SHA-1',
            'payload' => new MongoBinData($payLoad, MongoBinData::GENERIC),
            'autoAuthorize' => 1,
        ], array_merge($options, ['skipEmptyExchange' => true]), true);
        if (is_array($response) && isset($response['ok']) && $response['ok'] != 1) {
            return false;
        }
        $dist = explode(',', $response['payload']->bin);
        $tmpDist = [];
        foreach ($dist as $k => $v) {
            $tmpDist[substr($v, 0, 1)] = substr($v, 2);
        }
        $iterations = intval($tmpDist['i']);
        if ($iterations < 4096) {
            return false;
        }
        $salt = base64_decode($tmpDist['s']);
        $rnonce = $tmpDist['r'];
        if (substr($rnonce, 0, 5) == 'nonce') {
            return false;
        }
        $h = function ($data) {
            return hash('sha1', $data, true);
        };
        $hmac = function ($data, $key) {
            return hash_hmac('sha1', $data, $key, true);
        };
        $hi = function ($password, $salt, $iterations) {
            return hash_pbkdf2('sha1', $password, $salt, $iterations, 20, true);
        };
        $withoutProof = 'c=biws,r=' . $rnonce;
        $processedPassword = md5($username . ':mongo:' . $password);
        $saltedPassword = $hi(
            $processedPassword,
            $salt,
            $iterations,
        );
        $clientKey = $hmac('Client Key', $saltedPassword);
        $serverKey = $hmac('Server Key', $saltedPassword);
        $storedKey = $h($clientKey);
        $authMessage = 'n=' . $username . ',r=' . $nonce . ',' . $response['payload']->bin . ',' . $withoutProof;
        $clientSignature = $hmac($authMessage, $storedKey);
        $clientProof = 'p=' . base64_encode($clientKey ^ $clientSignature);
        $clientFinal = $withoutProof . ',' . $clientProof;
        $serverSignature = $hmac($authMessage, $serverKey);
        $saslContinueCmd = [
            'saslContinue' => 1,
            'conversationId' => $response['conversationId'],
            'payload' => new MongoBinData($clientFinal, MongoBinData::GENERIC)
        ];
        $secondResponse = $this->command($saslContinueCmd, $options, true);
        if (is_array($secondResponse) && isset($secondResponse['ok']) && $secondResponse['ok'] != 1) {
            return false;
        }
        $dist = explode(',', $secondResponse['payload']->bin);
        $tmpDist = [];
        foreach ($dist as $k => $v) {
            $tmpDist[substr($v, 0, 1)] = substr($v, 2);
        }
        if ($tmpDist['v'] !== base64_encode($serverSignature)) {
            return false;
        }
        $retrySaslContinueCmd = [
            'saslContinue' => 1,
            'conversationId' => $secondResponse['conversationId'],
            'payload' => ''
        ];
        $thirdResponse = $this->command($retrySaslContinueCmd, $options, true);
        if (is_array($thirdResponse) && isset($thirdResponse['done']) && $thirdResponse['done'] == 1) {
            return true;
        }
        return false;
    }

    /**
     * Creates a collection
     *
     * @param string $name - The name of the collection.
     * @param array $options - An array containing options for the
     *   collections. Each option is its own element in the options array,
     *   with the option name listed below being the key of the element. The
     *   supported options depend on the MongoDB server version. At the
     *   moment, the following options are supported:      capped    If the
     *   collection should be a fixed size.      size    If the collection is
     *   fixed size, its size in bytes.      max    If the collection is
     *   fixed size, the maximum number of elements to store in the
     *   collection.      autoIndexId    If capped is TRUE you can specify
     *   FALSE to disable the automatic index created on the _id field.
     *   Before MongoDB 2.2, the default value for autoIndexId was FALSE.
     *
     * @return MongoCollection - Returns a collection object representing
     *   the new collection.
     */
    public function createCollection($name, array $options = [])
    {
        $options['create'] = $name;

        return $this->command($options);
    }

    /**
     * The name of this database
     *
     * @return string - Returns this databases name.
     */
    public function __toString()
    {
        return $this->name;
    }
}
