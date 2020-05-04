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

use Hyperf\Contract\ConfigInterface;
use Kaikeba\Mongo\Exception\InvalidMongoProxyException;

class MongoFactory
{
    /**
     * @var MongoProxy[]
     */
    protected $proxies;

    public function __construct(ConfigInterface $config)
    {
        $mongoConfig = $config->get('mongo');

        foreach ($mongoConfig as $poolName => $item) {
            $this->proxies[$poolName] = make(MongoProxy::class, ['pool' => $poolName]);
        }
    }

    /**
     * @return MongoProxy
     */
    public function get(string $poolName)
    {
        $proxy = $this->proxies[$poolName] ?? null;
        if (! $proxy instanceof MongoProxy) {
            throw new InvalidMongoProxyException('Invalid Redis proxy.');
        }
        return $proxy;
    }
}
