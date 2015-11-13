<?php

namespace Queue;

/**
 * 工具类
 *
 * @namespace Queue
 * @author kevin<sky3hao@qq.com> 
 */
class Util
{
    /**
     * 当前时间的毫秒表示
     *
     * @return int
     */
    public static function now()
    {
        return microtime(true) * 1000 | 0;
    }

    /**
     * 生成一个唯一ID
     *
     * @return string
     */
    public static function genUniqueid()
    {
        $rand = mt_rand(1000, 9999);
        return sha1(uniqid(getmypid() . $rand));
    }

    /**
     * Set handle error
     */
    public static function handleError()
    {
        set_error_handler(function ($type, $message, $file, $line) {
            if (error_reporting() & $type) {
                throw new \ErrorException($message, $type, 0, $file, $line);
            }
        });
    }
}
