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
class MongoResultException extends MongoException
{
    /**
     * Retrieve the full result document
     *
     * @return array - The full result document as an array, including
     *   partial data if available and additional keys.
     */
    public function getDocument()
    {
        throw new \Exception('Not Implemented');
    }
}
