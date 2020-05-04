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
namespace Kaikeba\Mongo\Exception;
class MongoCursorException extends MongoException
{
    /**
     * The hostname of the server that encountered the error
     *
     * @return string - Returns the hostname, or NULL if the hostname is
     *   unknown.
     */
    public function getHost()
    {
        throw new \Exception('Not Implemented');
    }
}
