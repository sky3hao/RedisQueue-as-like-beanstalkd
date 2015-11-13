<?php

namespace Queue;

/**
 * 消费任务
 *
 * @namespace Queue
 * @author kevin<sky3hao@qq.com>
 */
class Caster extends Job
{
    /**
     * 并发冲突
     *
     * @var bool
     */
    public static $conflict = false;

    /**
     * 获取主体数据
     *
     * @return array
     */
    public function getBody()
    {
        return $this->injectors['data'];
    }

    /**
     * 获取处理过的次数
     *
     * @return mixed
     */
    public function getRetryTimes()
    {
        return $this->injectors['retry_times'];
    }

    /**
     * 调度动作-置为准备状态
     *
     * @param integer|null $second
     * @return bool
     * @throws \Exception
     */
    public function release($second = null)
    {
        if (is_null($second)) {
            $state = $this->get('state');
            if ($state != self::STATE_RESERVED) {
                throw new \Exception("You are not allowed to " . __FUNCTION__ . " job when it's on " . $state . " state.");
            }
            return $this->state(self::STATE_READY)->delete($state);
        } else {
            $state = $this->get('state');
            if ($state != self::STATE_RESERVED) {
                throw new \Exception("You are not allowed to " . __FUNCTION__ . " job when it's on " . $state ." state.");
            }
            $delayedTime = (time() + $second) * 1000;
            $this->set('timing', $delayedTime);
            return $this->state(self::STATE_DELAYED)->delete($state);
        }
    }

    /**
     * 调度动作-置为订阅状态
     *
     * @return bool
     * @throws \Exception
     */
    public function reserve()
    {
        $state = $this->get('state');
        if ($state != self::STATE_READY) {
            throw new \Exception("You are not allowed to " . __FUNCTION__ . " job when it's on " . $state ." state.");
        }

        return $this->state(self::STATE_RESERVED)->delete($state);
    }

    /**
     * 调度动作-置为失败状态
     * 失败状态的任务系统不会自动清除, 需要消费者手动清除(delete)或者kick到准备队列里
     *
     * @return bool
     * @throws \Exception
     */
    public function bury()
    {
        $state = $this->get('state');
        if ($state != self::STATE_RESERVED) {
            throw new \Exception("You are not allowed to " . __FUNCTION__ . " job when it's on " . $state ." state.");
        }

        return $this->state(self::STATE_FAILED)->delete($state);
    }

    /**
     * 调度动作-将失败任务重置为准备状态
     *
     * @return bool
     * @throws \Exception
     */
    public function kick()
    {
        $state = $this->get('state');
        if ($state != self::STATE_FAILED) {
            throw new \Exception("You are not allowed to " . __FUNCTION__ . " job when it's on " . $state ." state.");
        }

        return $this->state(self::STATE_READY)->delete($state);
    }

    /**
     * 调度动作-移除任务
     *
     * @param string|null $state
     * @return bool
     * @throws \Exception
     */
    public function delete($state = null)
    {
        if (is_null($state)) {
            $state = $this->get('state');
            if (!in_array($state, array(self::STATE_RESERVED, self::STATE_FAILED,))) {
                throw new \Exception("You are not allowed to " . __FUNCTION__ . " job when it's on " . $state ." state.");
            }
            $this->client->del($this->queue->name . ':' . self::JOB_TAB . ':' . $this->injectors['id']);
            if ($this->queue->distributed) {
                $this->client->del($this->queue->name . ':' . self::JOB_TAB . ':' . $this->injectors['id'] . ':lock');
            }
        }

        $setKey = $this->queue->name . ':' . Job::JOBS_TAB;
        $this->client->zRem($setKey . ':' . $state, $this->injectors['id']);
        $this->client->zRem($setKey . ':' . $this->injectors['tube'] . ':' . $state, $this->injectors['id']);

        return true;
    }

}
