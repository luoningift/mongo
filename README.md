# mongo
mongo 协程客户端 支持基本的curd 暂不支持ssl
项目地址：https://github.com/luoningift/mongo
安装：composer require kaikeba/mongo:dev-master
安装后执行：
php bin/hyperf.php vendor:publish kaikeba/mongo
配置文件位置: config/autoload/mongo.php

配置文件格式：
return [
    'default' => [
        //ip和端口 多个以,隔开
        'url' => '127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019',
        //要操作的库 必填
        'db' => 'homestead',
        //用户名
        'username' => 'test',
        //密码
        'password' => '123456',
        //验证的库
        'auth_source' => 'homestead',
        //副本集名称
        'replica_set' => 'test',
        //副本集用户名
        'replica_username' => 'admin',
        //副本集密码
        'replica_password' => '123456',
        //副本集验证的库 默认admin
        'replica_auth_source' => 'admin',
        //连接池配置
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

实例：
$container = ApplicationContext::getContainer();
$mongo = $container->get(Mongo::class);
$data = ['mongo' => mt_rand(1000, 9999), 'name' => '123456'];
$mongo->selectCollection('test')->insert($data);
$cursor = $mongo->selectCollection('test')->find();
$docs = [];
foreach ($cursor as $doc) {
$docs[] = $doc;
}
