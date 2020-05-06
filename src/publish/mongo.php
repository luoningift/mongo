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
return [
    'default' => [
        'url' => '127.0.0.1:27017',
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
    ],
];
