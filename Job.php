<?php

namespace Queue;

/**
 * 抽象任务类
 *
 * @namespace Queue
 * @author kevin<sky3hao@qq.com> 
 */
abstract class Job
{
    /**
     * 准备状态
     */
    const STATE_READY = 'ready';

    /**
     * 订阅状态
     */
    const STATE_RESERVED = 'reserved';

    /**
     * 延迟状态
     */
    const STATE_DELAYED = 'delayed';

    /**
     * 失败状态
     */
    const STATE_FAILED = 'failed';

    /**
     * 具体JOB的标签名
     */
    const JOB_TAB = 'job';

    /**
     * JOB队列的标签名
     */
    const JOBS_TAB = 'jobs';

    /**
     * 管道的标签名
     */
    const TUBES_TAB = 'tubes';

    /**
     * 消息任务的数据结构
     *
     * @var array
     */
    protected $injectors = array(
        'id'           => null,
        'tube'         => null,
        'data'         => array(),
        'state'        => self::STATE_READY,
        'created_at'   => '',
        'updated_at'   => '',
        'error'        => '',
        'failed_at'    => '',
        'timing'       => 0,    // 定时, 单位毫秒
        'attempts'     => 0,    // 尝试次数计数器
        'max_attempts' => 3,    // 最大尝试次数
        'retry_times'  => 0,    // 重新处理过的次数
        'periodic'     => 0,    // 周期任务生命周期时长, 单位秒
    );

    /**
     * 队列资源
     *
     * @var Queue
     */
    protected $queue;

    /**
     * Redis资源
     *
     * @var \Redis
     */
    protected $client;

    /**
     * 锁的生命周期, 单位毫秒
     */
    const LOCK_TIME = 300000;

    /**
     * Queue\Job
     *
     * @param string $tube
     * @param mixed $data
     * @param array $oldJobData
     */
    public function __construct($tube, $data = array(), $oldJobData = array())
    {
        if (empty($oldJobData)) {
            $this->injectors['id'] = Util::genUniqueid();
            $this->injectors['tube'] = $tube;
            $this->injectors['data'] = $data;
            $this->injectors['created_at'] = Util::now();
        } else {
            $this->injectors = $oldJobData;
        }

        $this->queue = Queue::$instance;
        $this->client = Queue::$instance->client;
    }

    /**
     * 加载任务
     *
     * @param string $id
     * @return bool|Caster|Invoker
     */
    final public static function reload($id)
    {
        $client = Queue::$instance->client;

        if (Queue::$instance->distributed && substr(get_called_class(), -6) == 'Caster') {
            $lockKey = Queue::$instance->name . ':' . self::JOB_TAB . ':' . $id . ':lock';
            if(!$client->setnx($lockKey, Util::now())) {
                $oldTime = $client->get($lockKey);
                if (($oldTime === false) || (Util::now() - $oldTime) < self::LOCK_TIME) {
                    Caster::$conflict = true;
                    return false;
                }
                if ($client->getSet($lockKey, strval(Util::now())) != $oldTime) {
                    $client->set($lockKey, $oldTime);
                    Caster::$conflict = true;
                    return false;
                }
            }
        }

        if (!$oldJobData = $client->hGetAll(Queue::$instance->name . ':' . self::JOB_TAB . ':' . $id)) {
            return false;
        }

        $oldJobData['data'] = json_decode($oldJobData['data'], true);
        $job = new static($oldJobData['tube'], array(), $oldJobData);

        return $job;
    }

    /**
     * 获取任务属性
     *
     * @param string $key
     * @return mixed
     */
    protected function get($key)
    {
        return $this->client->hGet($this->queue->name . ':' . self::JOB_TAB . ':' . $this->injectors['id'], $key);
    }

    /**
     * 设置任务属性
     *
     * @param string $key
     * @param string $val
     */
    protected function set($key, $val)
    {
        $this->injectors[$key] = $val;
        $this->client->hSet($this->queue->name . ':' . self::JOB_TAB . ':' . $this->injectors['id'], $key, $val);
    }

    /**
     * 改变状态
     *
     * @param $state
     * @return $this
     */
    protected function state($state)
    {
        $setKey = $this->queue->name . ':' . self::JOBS_TAB;
        $this->client->zRem($setKey . ':' . $this->injectors['state'], $this->injectors['id']);
        $this->client->zRem($setKey . ':' . $this->injectors['tube'] . ':' . $this->injectors['state'], $this->injectors['id']);

        $this->set('state', $state);

        $score = $this->injectors['timing'];
        $this->client->zAdd($setKey . ':' . $state, $score, $this->injectors['id']);
        $this->client->zAdd($setKey . ':' . $this->injectors['tube'] . ':' . $state, $score, $this->injectors['id']);

        $this->set('updated_at', Util::now());

        return $this;
    }

}
