<?php

namespace Kaikeba\Mongo;

use Kaikeba\Mongo\Util\Protocol;

/**
 * A cursor is used to iterate through the results of a database query.
 */
class MongoCursor implements \Iterator
{
    const DEFAULT_BATCH_SIZE = 100;

    /**
     * @var integer
     */
    public static $timeout = 30000;

    /**
     * @var Protocol
     */
    private $client;

    /**
     * Full collection name
     * @var string
     */
    private $fcn;

    /**
     * @var array[]
     */
    private $documents = [];

    /**
     * @var int
     */
    private $currKey = -1;

    /**
     * @var null|int
     */
    private $cursorId = null;

    /**
     * @var bool
     */
    private $fetching = false;

    /**
     * @var bool
     */
    private $end = false;

    /**
     * @var bool
     */
    private $hasMore = false;

    /**
     * @var array
     */
    private $query = [];

    /**
     * @var int
     */
    private $queryLimit = 0;

    /**
     * @var int
     */
    private $querySkip = 0;

    /**
     * @var int
     */
    private $queryTimeout = null;

    /**
     * @var int
     */
    private $batchSize = self::DEFAULT_BATCH_SIZE;

    /**
     * @var int
     */
    private $flags = 0;

    private $fields = [];

    /**
     * Create a new cursor
     *
     * @param Protocol $client - Database connection.
     * @param string $ns - Full name of database and collection.
     * @param array $query - Database query.
     * @param array $fields - Fields to return.
     */
    public function __construct(Protocol $client, $ns, array $query = [], array $fields = [])
    {
        $this->client = $client;
        $this->fcn = $ns;
        $this->fields = $fields;
        $this->query['$query'] = $query;
        $this->queryTimeout = self::$timeout;
    }

    /**
     * Clears the cursor
     *
     * @return void - NULL.
     */
    public function reset()
    {
        $this->documents = [];
        $this->currKey = 0;
        $this->cursorId = null;
        $this->end = false;
        $this->fetching = false;
        $this->hasMore = false;
    }

    /**
     * Gives the database a hint about the query
     *
     * @param mixed $index - Index to use for the query. If a string is
     *   passed, it should correspond to an index name. If an array or object
     *   is passed, it should correspond to the specification used to create
     *   the index (i.e. the first argument to
     *   MongoCollection::ensureIndex()).
     *
     * @return MongoCursor - Returns this cursor.
     */
    public function hint($index)
    {
        if (is_object($index)) {
            $index = get_object_vars($index);
        }

        if (is_array($index)) {
            $index = MongoCollection::_toIndexString($index);
        }

        $this->query['$hint'] = $index;

        return $this;
    }

    /**
     * Use snapshot mode for the query
     *
     * @return MongoCursor - Returns this cursor.
     */
    public function snapshot()
    {
        $this->query['$snapshot'] = true;

        return $this;
    }

    /**
     * Sorts the results by given fields
     *
     * @param array $fields - An array of fields by which to sort. Each
     *   element in the array has as key the field name, and as value either
     *   1 for ascending sort, or -1 for descending sort.
     *
     * @return MongoCursor - Returns the same cursor that this method was
     *   called on.
     */
    public function sort(array $fields)
    {
        $this->query['$orderby'] = $fields;

        return $this;
    }

    /**
     * Return an explanation of the query, often useful for optimization and
     * debugging
     *
     * @return array - Returns an explanation of the query.
     */
    public function explain()
    {
        $query = [
            '$query' => $this->getQuery(),
            '$explain' => true
        ];

        $response = $this->client->opQuery(
            $this->fcn,
            $query,
            $this->querySkip,
            $this->calculateRequestLimit(),
            $this->flags | Protocol::QF_SLAVE_OK,
            MongoCursor::$timeout,
            $this->fields
        );

        return $response['result'][0];
    }

    /**
     * Sets the fields for a query
     *
     * @param array $fields - Fields to return (or not return).
     *
     * @return MongoCursor - Returns this cursor.
     */
    public function fields(array $fields)
    {
        $this->fields = $fields;

        return $this;
    }

    /**
     * Limits the number of results returned
     *
     * @param int $num - The number of results to return.
     *
     * @return MongoCursor - Returns this cursor.
     */
    public function limit($num)
    {
        $this->queryLimit = $num;

        return $this;
    }

    /**
     * Skips a number of results
     *
     * @param int $num - The number of results to skip.
     *
     * @return MongoCursor - Returns this cursor.
     */
    public function skip($num)
    {
        $this->querySkip = $num;

        return $this;
    }

    /**
     * Limits the number of elements returned in one batch.
     *
     * @param int $batchSize - The number of results to return per batch.
     *   Each batch requires a round-trip to the server.
     *
     * @return MongoCursor - Returns this cursor.
     */
    public function batchSize($batchSize)
    {
        $this->batchSize = $batchSize;

        return $this;
    }

    /**
     * Gets the query, fields, limit, and skip for this cursor
     *
     * @return array - Returns the namespace, limit, skip, query, and
     *   fields for this cursor.
     */
    public function info()
    {
        return [
            'ns' => $this->fcn,
            'limit' => $this->queryLimit,
            'batchSize' => $this->batchSize,
            'skip' => $this->querySkip,
            'flags' => $this->flags | Protocol::QF_SLAVE_OK,
            'query' => $this->query['$query'],
            'fields' => $this->fields,
            'started_iterating' => $this->fetching,
            'id' => $this->cursorId,
        ];
    }

    /**
     * Counts the number of results for this query
     *
     * @param bool $foundOnly -
     *
     * @return int - The number of documents returned by this cursor's
     *   query.
     */
    public function count($foundOnly = false)
    {
        $this->doQuery();

        if ($foundOnly) {
            return $this->countLocalData();
        }

        return $this->countQuerying();
    }

    private function countQuerying()
    {
        $ns = explode('.', $this->fcn, 2);

        $query = [
            'count' => $ns[1],
            'query' => $this->query['$query']
        ];

        $response = $this->client->opQuery(
            $ns[0] . '.$cmd',
            $query, 0, -1, 0,
            $this->queryTimeout
        );

        return (int)$response['result'][0]['n'];
    }

    private function countLocalData()
    {
        return iterator_count($this);
    }

    /**
     * Execute the query.
     *
     * @return void - NULL.
     */
    protected function doQuery()
    {
        if (!$this->fetching) {
            $this->fetchDocuments();
        }
    }

    private function fetchDocuments()
    {
        $this->fetching = true;
        $response = $this->client->opQuery(
            $this->fcn,
            $this->getQuery(),
            $this->querySkip,
            $this->calculateRequestLimit(),
            $this->flags | Protocol::QF_SLAVE_OK,
            $this->queryTimeout,
            $this->fields
        );

        $this->cursorId = $response['cursorId'];
        $this->setDocuments($response);
    }

    private function getQuery()
    {
        if (isset($this->query['$query']) && count($this->query) == 1) {
            return $this->query['$query'];
        }

        return $this->query;
    }

    private function calculateRequestLimit()
    {
        if ($this->queryLimit < 0) {
            return $this->queryLimit;
        } elseif ($this->batchSize < 0) {
            return $this->batchSize;
        }

        if ($this->queryLimit > $this->batchSize) {
            return $this->batchSize;
        } else {
            return $this->queryLimit;
        }
    }

    private function fetchMoreDocumentsIfNeeded()
    {
        if (isset($this->documents[$this->currKey + 1])) {
            return;
        }

        if ($this->cursorId) {
            $this->fetchMoreDocuments();
        } else {
            $this->end = true;
        }
    }

    private function fetchMoreDocuments()
    {
        $limit = $this->calculateNextRequestLimit();
        if ($this->end) {
            return;
        }

        $response = $this->client->opGetMore(
            $this->fcn,
            $limit,
            $this->cursorId,
            $this->queryTimeout
        );

        $this->setDocuments($response);
    }

    private function calculateNextRequestLimit()
    {
        $current = count($this->documents);
        if ($this->queryLimit && $current >= $this->queryLimit) {
            $this->end = true;
            return 0;
        }

        if ($this->queryLimit >= $current) {
            $remaining = $this->queryLimit - $current;
        } else {
            $remaining = $this->queryLimit;
        }

        if ($remaining > $this->batchSize) {
            return $this->batchSize;
        }

        return $remaining;
    }

    private function setDocuments(array $response)
    {
        if (0 === $response['count']) {
            $this->end = true;
        }

        $this->documents = array_merge($this->documents, $response['result']);
    }

    /**
     * Return the next object to which this cursor points, and advance the
     * cursor
     *
     * @return array - Returns the next object.
     */
    public function getNext()
    {
        $this->next();

        return $this->current();
    }

    /**
     * Checks if there are any more elements in this cursor
     *
     * @return bool - Returns if there is another element.
     */
    public function hasNext()
    {
        $this->doQuery();
        $this->fetchMoreDocumentsIfNeeded();

        return isset($this->documents[$this->currKey + 1]);
    }

    /**
     * Sets a client-side timeout for this query
     *
     * @param int $ms -
     *
     * @return MongoCursor - This cursor.
     */
    public function timeout($ms)
    {
        $this->queryTimeout = $ms;

        return $this;
    }

    /**
     * Returns the current element
     *
     * @return array - The current result as an associative array.
     */
    public function current()
    {
        $this->doQuery();
        $this->fetchMoreDocumentsIfNeeded();

        if (!isset($this->documents[$this->currKey])) {
            return null;
        }

        return $this->documents[$this->currKey];
    }

    /**
     * Advances the cursor to the next result
     *
     * @return void - NULL.
     */
    public function next()
    {
        $this->doQuery();
        $this->fetchMoreDocumentsIfNeeded();

        $this->currKey++;
    }

    /**
     * Returns the current results _id
     *
     * @return string - The current results _id as a string.
     */
    public function key()
    {
        $record = $this->current();
        if (!$record) {
            return null;
        }

        if (!isset($record['_id'])) {
            return $this->currKey;
        }

        return (string)$record['_id'];
    }

    /**
     * Checks if the cursor is reading a valid result.
     *
     * @return bool - If the current result is not null.
     */
    public function valid()
    {
        $this->doQuery();

        return !$this->end;
    }

    /**
     * Returns the cursor to the beginning of the result set
     *
     * @return void - NULL.
     */
    public function rewind()
    {
        $this->currKey = 0;
        $this->end = false;
    }
}
