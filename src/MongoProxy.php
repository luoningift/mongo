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

use Kaikeba\Mongo\Pool\PoolFactory;

class MongoProxy extends Mongo
{
    protected $poolName;

    public function __construct(PoolFactory $factory, string $pool)
    {
        parent::__construct($factory);

        $this->poolName = $pool;
    }

    public function __call($name, $arguments)
    {
        return parent::__call($name, $arguments);
    }
}
