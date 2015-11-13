<?php

namespace Queue;

/**
 * 生产任务
 *
 * @author kevin<sky3hao@qq.com> 
 */
class Invoker extends Job
{
    /**
     * 设置定时任务
     *
     * @param int|string $time 时间文字表示,或者未来某个时间点的时间戳
     */
    public function timing($time)
    {
        if (!is_numeric($time)) {
            $time = strtotime($time);
        }

        $this->injectors['timing'] = $time * 1000;
        $this->injectors['state'] = self::STATE_DELAYED;
    }

    /**
     * 设置延时任务
     *
     * @param int $s 需要延迟的秒数
     */
    public function delay($s)
    {
        $this->timing(time() + $s);
    }

    /**
     * 设置最大重试次数
     *
     * @param int $num
     */
    public function attempts($num)
    {
        $this->injectors['max_attempts'] = $num;
    }

    /**
     * 设置周期任务
     *
     * @param $second
     */
    public function periodic($second)
    {
        $this->injectors['periodic'] = $second;
        $this->timing(time() + $second);
    }

    /**
     * 保存任务
     *
     * @return bool
     */
    public function save()
    {
        if ($this->update() === false) {
            return false;
        }

        $this->client->sAdd($this->queue->name . ':' . self::TUBES_TAB, $this->injectors['tube']);
        return true;
    }

    /**
     * 更新任务
     *
     * @return bool
     */
    private function update()
    {
        if (!$this->injectors['id']) {
            return false;
        }

        $this->injectors['updated_at'] = Util::now();

        $job = $this->injectors;
        $job['data'] = json_encode($job['data']);
        $rs = $this->client->hMset($this->queue->name . ':' . self::JOB_TAB . ':' . $this->injectors['id'], $job);
        if ($rs === false) {
            return false;
        }

        $this->state($job['state']);
        return true;
    }

    /**
     * 重试方法
     *
     * @param $fn
     */
    public function attempt($fn)
    {
        $max = $this->get('max_attempts');
        $attempts = $this->client->hIncrBy($this->queue->name . ':' . self::JOB_TAB . ':' . $this->injectors['id'], 'attempts', 1);
        $fn($max - $attempts);
    }

    /**
     * 设置错误
     *
     * @param \Exception|null $error
     * @return mixed
     */
    public function error($error = null)
    {
        if ($error === null) {
            return $this->injectors['error'];
        }

        if ($error instanceof \Exception) {
            $str = get_class($error) . ' Error on: ' . $error->getFile() . ' ' . $error->getLine() . ' ';
            $str .= 'Error Msg: ' . $error->getMessage() . ' ';
            $str .= 'Error Code: ' . $error->getCode() . ' ';
            $str .= 'Trace: ' . $error->getTraceAsString();
        } else {
            $str = $error;
        }

        $this->set('error', $str);
        $this->set('failed_at', Util::now());

        if ($this->queue->distributed) {
            $this->client->del($this->queue->name . ':' . self::JOB_TAB . ':' . $this->injectors['id'] . ':lock');
        }
    }

    /**
     * 更新状态
     *
     * @param string $state
     * @return $this
     */
    public function state($state)
    {
        return parent::state($state);
    }

    /**
     * 获取属性
     *
     * @param string $key
     * @return mixed
     */
    public function get($key)
    {
        return parent::get($key);
    }

    /**
     * 设置属性
     *
     * @param string $key
     * @param string $val
     */
    public function set($key, $val)
    {
        parent::set($key, $val);
    }

}
