<?php
namespace Kaikeba\Mongo;

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
    public function __construct(Protocol $client, $name)
    {
        $this->name = $name;
        $this->client = $client;
    }

    public function getProtoCol() {
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
    public function command(array $cmd, array $options = [])
    {
        $timeout = MongoCursor::$timeout;
        if (!empty($options['timeout'])) {
            $timeout = $options['timeout'];
        }

        $protocol = empty($options['protocol'])
            ? $this->client
            : $options['protocol'];

        $response = $protocol->opQuery(
            "{$this->name}.\$cmd",
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
    public function authenticate($username, $password, $options = [])
    {
        $response = $this->command(['getnonce' => 1], $options);
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
            'key' => $digest
        ], $options);
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
