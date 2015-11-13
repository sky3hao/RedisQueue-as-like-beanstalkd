<?php

namespace Queue;

use Log;

/**
 * 消费者进程管理类
 *
 * @namespace Queue
 * @author kevin<sky3hao@qq.com> 
 */
class Portal
{
    /**
     * 队列资源
     *
     * @var Queue
     */
    private $queue;

    /**
     * 类型
     *
     * @var string
     */
    private $tube;

    /**
     * Redis扩展资源
     *
     * @var \Redis
     */
    private $client;

    /**
     * 执行任务的回调方法
     *
     * @var callable
     */
    private $callback;

    /**
     * 操作的任务ID
     *
     * @var string
     */
    private $jobId;

    /**
     * 周期任务,如果被设置,则其值大于0
     *
     * @var int
     */
    private $periodic = 0;

    /**
     * Queue\Portal
     *
     * @param Queue $queue
     * @throws \Exception
     */
    public function __construct(Queue $queue)
    {
        if (substr(php_sapi_name(), 0, 3) !== 'cli') {
            throw new \Exception("This Programe can only be run in CLI mode.");
        }

        $this->queue = $queue;
        $this->client = $queue->client;
    }

    /**
     * 创建处理任务的进程
     *
     * @param string $tube
     * @param callable $callback
     * @throws \Exception
     */
    public function doWork($tube, $callback)
    {
        if (!extension_loaded('pcntl')) {
            throw new \Exception('The pcntl extension is required for workers.');
        }

        $this->tube = $tube;
        $this->callback = $callback;

        set_time_limit(0);
        do {
            $job = $this->getReadyJob();
            if ($job && ($job instanceof Caster)) {
                $this->spawn($callback, $job);
            } else {
                Caster::$conflict ? usleep(rand(7000, 10000)) : sleep(10);
            }
        } while (true);
    }

    /**
     * 进程控制
     *
     * @param callable $callable
     * @param Caster $job
     * @return bool
     * @throws \Exception
     */
    private function spawn($callable, Caster $job)
    {
        $invoker = Invoker::reload($this->jobId);

        $pid = pcntl_fork();
        switch ($pid) {
            case -1:
                throw new \Exception('Fork failed, bailing.');
                break;
            case 0:
                try {
                    $job->reserve();
                    call_user_func($callable, $job);
                } catch (\Exception $e) {
                    Log::error(array(
                        'msg'       => 'spawn error',
                        'errCode'   => $e->getCode(),
                        'errMsg'    => $e->getMessage(),
                        'jobId'     => $this->jobId,
                    ));
                    $invoker->error($e);
                    $invoker->attempt(function ($remaining) use ($job) {
                        $remaining ? $job->release() : $job->bury();
                    });
                    exit(0);
                }
                break;
            default:
                pcntl_waitpid($pid, $status);
                $result = pcntl_wexitstatus($status);
                if ($result != 0) {
                    return false;
                }
                try {
                    if ($this->periodic > 0) {
                        $job->release($this->periodic);
                    } else if ($invoker->get('state') == Job::STATE_RESERVED) {
                        $job->delete();
                    }
                } catch (\Exception $e) {
                    $invoker->error($e);
                }

                break;
        }

        return true;
    }

    /**
     * 获取一个任务
     *
     * @return bool|Caster
     */
    private function getReadyJob()
    {
        $keyChip = $this->queue->name . ':' . Job::JOBS_TAB . ':' . $this->tube . ':';
        $timePort = Util::now();

        $this->expireActivate($keyChip, $timePort);

        if (!$id = $this->pop($keyChip . Job::STATE_READY, $timePort)) {
            return false;
        }
        if (!$job = Caster::reload($id)) {
            return false;
        }

        $this->client->hIncrBy($this->queue->name . ':' . Job::JOB_TAB . ':' . $id, 'retry_times', 1);
        $this->jobId = $id;
        $this->periodic = $this->client->hGet($this->queue->name . ':' . Job::JOB_TAB . ':' . $id, 'periodic');

        return $job;
    }

    /**
     * 获取一个任务(并不是弹出)
     *
     * @param string $key
     * @param int $timePort
     * @return mixed
     */
    private function pop($key, $timePort)
    {
        $ret = $this->client->zRevRangeByScore($key, $timePort, '-inf', array('limit' => array(0, 1)));
        if (!$ret) {
            return false;
        }

        return $ret[0];
    }


    /**
     * 激活到期的延时任务
     *
     * @param string $keyChip
     * @param int $timePort
     */
    private function expireActivate($keyChip, $timePort)
    {
        $page = 1000;
        $key = $keyChip . Job::STATE_DELAYED;

        $count = $this->client->zCount($key, '-inf', $timePort);
        $batchNum = ceil($count / $page);
        for ($i = 0; $i < $batchNum; $i++) {
            $ret = $this->client->zRevRangeByScore($key, $timePort, '-inf', array('limit' => array($i * $page, $page)));
            if (is_array($ret)) {
                foreach ($ret as $id) {
                    $job = Invoker::reload($id);
                    if (!$job) {
                        continue;
                    }
                    $job->state(Job::STATE_READY);
                }
            }
        }
    }

}
