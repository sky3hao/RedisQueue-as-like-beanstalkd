<?php

namespace Queue;

use Config;
use Log;

/**
 * 消息队列操作类
 *
 * @namespace Queue
 * @author kevin<sky3hao@qq.com> 
 */
class Queue
{
    /**
     * 默认的配置文件
     */
    const DEFAULT_CONFIG_FILE = 'queue.ini';

    /**
     * 默认的配置指针
     */
    const DEFAULT_CONFIG_INDEX = 'queue';

    /**
     * 实例
     *
     * @var Queue
     */
    public static $instance;

    /**
     * Redis资源
     *
     * @var \Redis
     */
    public $client;

    /**
     * 队列的名字
     *
     * @var
     */
    public $name;

    /**
     * 分布式模式
     *
     * @var bool
     */
    public $distributed;

    /**
     * 配置参数
     *
     * @var array
     */
    protected $options = array(
        'host'          => 'localhost',
        'port'          => '6379',
        'db'            => 0,
        'client'        => null,
        'queue'         => 'queue',
        'distributed'   => false,
    );

    /**
     * 实例化入口
     *
     * @param string $configKey
     * @param string $configFile
     * @return Queue
     */
    public static function getInstance($configKey = self::DEFAULT_CONFIG_INDEX, $configFile = self::DEFAULT_CONFIG_FILE)
    {
        if (!(self::$instance instanceof self)) {
            $configFile = CONF_PATH . DIRECTORY_SEPARATOR . $configFile;
            $config = Config::getConfig($configKey, $configFile);

            self::$instance = new self($config['redis']);
        }
        return self::$instance;
    }

    /**
     * Queue\Queue
     *
     * @param array $options
     */
    private function __construct(array $options = array())
    {
        Util::handleError();

        foreach ($options as $option) {
            $this->options = $option + $this->options;
            $this->options['distributed'] = ($this->options['distributed'] == 0) ? false : true;
            $this->name = $this->options['queue'];
            $this->distributed = $this->options['distributed'];

            $this->client = &$this->options['client'];
            if (!$this->client) {
                $this->client = new \Redis();
                $ret = $this->client->pconnect($this->options['host'], $this->options['port']);
                if ($ret === true) {
                    $this->client->select($this->options['db']);
                    break;
                }
                Log::warn('connect to redis failed', 'ret:', $ret);
            }
        }
    }

    /**
     * 创建任务并写入队列
     *
     * @param string $tube
     * @param mixed  $data
     * @param array $options
     * @return bool
     */
    public function putInTube($tube, $data, $options = array())
    {
        $invoker = new Invoker($tube, $data);

        if (array_key_exists('delay', $options)) {
            $invoker->delay($options['delay']);
        }

        if (array_key_exists('timing', $options)) {
            $invoker->timing($options['timing']);
        }

        if (array_key_exists('periodic', $options)) {
            $invoker->periodic($options['periodic']);
        }

        if (array_key_exists('attempts', $options)) {
            $invoker->attempts($options['attempts']);
        }

        return $invoker->save();
    }

    /**
     * 创建处理任务的守护进程
     *
     * @param string $tube
     * @param callable $callback
     * @throws \Exception
     */
    public function doWork($tube, $callback)
    {
        if (!is_string($tube)) {
            throw new \Exception('The tube name must be a string.');
        }
        if (!is_callable($callback)) {
            throw new \Exception('The callback is invalid.');
        }

        $worker = new Portal($this);
        $worker->doWork($tube, $callback);
    }

    /**
     * 获取所有管道(tube)名
     *
     * @return mixed
     */
    public function getAllTubesNames()
    {
        return $this->client->smembers($this->name . ':' . Job::TUBES_TAB);
    }

    /**
     * 根据状态获取所有任务的ID
     *
     * @param string $state
     * @param string $tubeName
     * @return mixed
     */
    public function getIdsByState($state, $tubeName = null)
    {
        $key = is_null($tubeName)
            ? $key = $this->name . ':' . Job::JOBS_TAB . ':' . $state
            : $this->name . ':' . Job::JOBS_TAB . ':' . $tubeName . ':' . $state;
        return $this->client->zrange($key, 0, -1);
    }

    /**
     * 获取特定状态下任务数量
     *
     * @param string $state
     * @param string $tubeName
     * @return mixed
     */
    public function countByState($state, $tubeName = null)
    {
        $key = is_null($tubeName)
            ? $key = $this->name . ':' . Job::JOBS_TAB . ':' . $state
            : $this->name . ':' . Job::JOBS_TAB . ':' . $tubeName . ':' . $state;
        return $this->client->zcard($key);
    }

    /**
     * 获取任务的结构体
     *
     * @param string $jobId
     * @return array
     */
    public function getJobStruct($jobId)
    {
        return $this->client->hGetAll($this->name . ':' . Job::JOB_TAB . ':' . $jobId);
    }

    /**
     * 获取周期任务的ID列表
     *
     * @param string|null $tubeName
     * @return array
     */
    public function getPeriodic($tubeName = null)
    {
        $container = array();
        $ret = $this->getDelayed($tubeName);
        if (is_array($ret)) {
            foreach ($ret as $jobId) {
                $periodic = $this->client->hGet($this->name . ':' . Job::JOB_TAB . ':' . $jobId, 'periodic');
                if ($periodic > 0) {
                    $container[] = $jobId;
                }
            }
        }
        return $container;
    }

    /**
     * 魔术方法__call
     * 支持根据任务的状态获取任务ID列表, 例如: getReady()
     *
     * @param string $name
     * @param array $params
     * @return bool|mixed
     */
    public function __call($name, $params)
    {
        if (substr($name, 0, 3) == 'get') {
            $const = 'STATE_' . strtoupper(substr($name, 3));

            $obj = new \ReflectionClass(__NAMESPACE__ . '\\Job');
            $constVal = $obj->getConstant($const);
            if ($constVal !== false) {
                if (empty($params)) {
                    return $this->getIdsByState($constVal);
                } else {
                    return $this->getIdsByState($constVal, $params[0]);
                }
            }
        }

        return false;
    }

}
