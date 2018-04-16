<?php
/**
 *  数据库连接池支持类
 */
class ConnectPool{

    // 单例对象
    public static $intance = null;
    // 连接属性
    public static $config = [];
    // 空闲连接池
    private $freeConnect = [];
    // 忙碌连接池
    private $busyConnect = [];
    // 初始化状态
    private $initStatus = false;
    // 当前进程id
    public static $conId = 0;

    private function getMysql()
    {
        return new Swoole\Coroutine\MySQL();
    }

    private function connect(\Swoole\Coroutine\MySQL $mysql)
    {
        $mysql->connect([
            'host' => $this->getConfig('host'),
            'port' => $this->getConfig('port'),
            'user' => $this->getConfig('user'),
            'password' => $this->getConfig('password'),
            'database' => $this->getConfig('database'),
        ]);
    }

    /** 生成连接 */
    private function getConnect($create=true)
    {
        if ($create == false) {
            $connect = array_pop($this->freeConnect);
        } else {
            $mysql = $this->getMysql();
            $this->connect($mysql);
            $connect = ['connector' => $mysql, 'created_at' => time(), 'id' => ++self::$conId];
        }
        return empty($connect) ? false : ($this->busyConnect[$connect['id']] = $connect);
    }

    /** 获取连接 */
    private function getMysqlConnect()
    {
        if ((false == $connect = $this->getConnect(false)) && $this->getTotalCount() < $this->getConfig('max')) {
            $connect = $this->getConnect();
        }
        return $connect;
    }

    private function transFree(array $connector)
    {
        $id = $connector['id'];
        unset($this->busyConnect[$id]);
        $this->freeConnect[$id] = $connector;
    }

    private function exec($sql)
    {
        while (($retry = $retry ?? 0) <= 1) {
            $connectInfo = $this->getMysqlConnect();
            if (empty($connectInfo)) {
                break;
            }
            $connector = $connectInfo['connector'];
            $res = $connector->query($sql);
            if ($res != false) {
                break;
            }
        }
        if (isset($connectInfo) && $connectInfo) {
            $this->transFree($connectInfo);
        }
        return $res ?? null;
    }

    public function initConnect()
    {
        $min = self::getConfig('min');
        for ($i=0; ($min>0 && $i<$min); $i++) {
            $this->transFree($this->getConnect());
        }
    }

    public static function setConfig($config)
    {
        self::$config = $config;
    }

    public static function getConfig($key)
    {
        return $key ? self::$config[$key] : self::$config;
    }

    private function tick()
    {
        $internal = self::getConfig('internal');
        swoole_timer_tick(1000, function() use ($internal){
            $free = self::getFreeConnect();
            $allCount = count($free) + count(self::getBusyConnect());
            $expired = array_filter($free, function($connect) use ($internal){
                if (time() - $connect['created_at'] >= $internal) {
                    return true;
                } else {
                    return false;
                }
            });
            $retained = $allCount - count($expired);
            if (($freeRetained = self::getConfig('min') - $retained) > 0) {
                $deleteConnects = array_slice($expired, count($freeRetained));
            } else {
                $deleteConnects = $retained;
            }
            $this->setInvalid($deleteConnects);
        });
    }

    private function setInvalid($connects) {
        $connects = !is_array($connects) ? [$connects] : $connects;
        foreach ($connects as $connect) {
            unset($this->freeConnect[$connect['id']]);
        }
    }

    private function getFreeConnect()
    {
        return $this->freeConnect;
    }

    private function getBusyConnect()
    {
        return $this->busyConnect;
    }

    private function getTotalCount()
    {
        return count($this->freeConnect) + count($this->busyConnect);
    }

    private function initParam()
    {
        $this->freeConnect = [];
        $this->busyConnect = [];
    }

    /** 初始化数据 */
    public static function init()
    {
        $intance = empty(self::$intance) ? (self::$intance = new static()) : self::$intance;
        if ($intance->initStatus == false) {
            $intance->initStatus = true;
            $intance->initConnect();
            $intance->tick();
            $intance->initParam();
        }
        return $intance;
    }

    public static function __callStatic($name, $arguments)
    {
        $intance = self::init();
        return call_user_func_array([$intance, $name], $arguments);
    }

}

$serv = new swoole_http_server("0.0.0.0", 9500);
$serv->set(array(
    'worker_num' => 1,
));
$config = [
    'host' => '127.0.0.1',
    'port' => 3306,
    'user' => 'root',
    'password' => 'password',
    'database' => 'database',
    'min' => 3,   // 最小数据库连接数
    'max' => 10,  // 最大数据库连接数
    'internal' => '60', // 持久连接断开间隔时间
];

ConnectPool::setConfig($config);

function onWorkerStart(swoole_server $server, int $worker_id)
{
    ConnectPool::init();
}

function onRequest($req, $resp)
{
    $query = $req->get['query'] ?? '';
    if (!empty($query)) {
        $res = ConnectPool::exec($query);
        $resp->end(var_export($res, true) . posix_getpid() .'|' .  "\n");
    }

}

$serv->on('Request', 'onRequest');
$serv->on('WorkerStart', 'onWorkerStart');
$serv->start();