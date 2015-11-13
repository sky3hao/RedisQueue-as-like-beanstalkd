# 简介

目标: beanstalkd是一款很优秀的消息队列系统, 但是还是需要额外的运维, 这里模仿beanstalkd,使用PHP和Redis提供使用简单而功能强大的消息队列系统.

# 安装

目前内嵌到框架, 也可以独立使用, 经过简单的修改可与其他核心模块完全解耦.

# 概述

- [创建队列](#创建队列)
  - [自定义配置创建](#自定义配置创建)
- [创建任务](#创建任务)
  - [创建延时任务](#创建延时任务)
  - [创建定时任务](#创建定时任务)
  - [创建周期任务](#创建周期任务)
  - [创建失败重试任务](#创建失败重试任务)
- [处理任务](#处理任务)
  - [任务状态调度控制](#任务状态调度控制)
  - [并发模式](#并发模式)

# 使用说明

## 配置文件

在Chip里的conf目录下创建queue.ini, 根据自己的机器增加以下配置项:

```php
[product]
[dev : product]
queue.redis.0.host = 127.0.0.1      ; 机器IP
queue.redis.0.port = 6379           ; 机器端口
queue.redis.0.db = 0                ; redis 数据库
queue.redis.0.name = queue          ; 队列名
queue.redis.0.distributed = 0       ; 是否开启并发模式
queue.redis.1.host = 127.0.0.1
queue.redis.1.port = 6379
queue.redis.1.db = 0
queue.redis.1.name = queue
queue.redis.1.distributed = 0
[test : dev]
```

## 创建队列

```php
$queue = \Queue::getInstance();
```

## 自定义配置创建

如果不想使用默认配置文件里的配置,可以自定义配置文件和配置索引

```php
$queue = \Queue::getInstance('myself.ini', 'mytest');
```

## 创建任务

创建任务是将一个消息写入一个有名字的管道, 下面管道名为"mytube"

```php
$data = array(
    'name' => 'kevin',
    'content' => array(
        'test'  => 'some string',
        'num' => null,
    ),
);
$queue = Queue::getInstance();
$queue->putInTube('mytube', $data);
```

###　创建延时任务

延时任务是在入队列后, 不会马上被处理进程获取, 在延迟指定时间后才会被处理.

```php
// 3600秒后才会被处理
$option = array(
    'delay' => 3600, // 单位为秒
);
$queue->putInTube('mytube', $data, $option);
```

### 创建定时任务

定时任务入队列后, 在特定时间点才会被处理进程获取并处理.

```php
// 在2015-10-29 23:34任务才会被处理
$option = array(
    'timing' => '2015-10-29 23:34',
);
$queue->putInTube('mytube', $data, $option);
```

定时参数的格式遵循PHP的日期和时间格式 [PHP date and time formats](http://php.net/manual/en/datetime.formats.php), 可以使用:

- `Next Monday`
- `+1 days`
- `last day of next month`
- `2013-09-13 00:00:00`
- and so on..

> 注意的是, 如果创建了一个过去时间的定时任务, 任务不会被丢弃, 而是会马上被触发.


###　创建周期任务

周期任务可以代替非具体时间的周期性的crontab.

```php
// 每3600秒被触发一次,不会销毁.
$option = array(
    'periodic' => 3600,
);
$queue->putInTube('mytube', $data, $option);
```

###　创建失败重试任务

处理的时候如果处理失败后, 可以设定一个重试次数, 来重复尝试处理这个任务:
这个参数可以与上面的时间控制的参数一起使用.

```php
$option = array(
    'attempts' => 5,
);
$queue->putInTube('mytube', $data, $option);
```


## 处理任务

处理消息任务需要创建一个守护进程的脚本, 必须在CLI模式下执行, 这里可以使用框架里的[CLI-TASK](https://github.com/sky3hao/Cli-Control):

```php
class DaemonTask extends TaskBase
{
    /**
     * 创建守护进程, 需要在CLI模式下执行
     */
    public function indexAction()
    {
        $queue = Queue::getInstance();
        // 创建进程, 监听'mytube'这个管道, 处理队列任务
        $queue->doWork('mytube', function(Caster $job) {
            $data = $job->getBody();

            // process

            // 特别注意的一点, 匿名函数里的程序是以子进程形式存在的
            // 如果正常处理完一个JOB后, 需要发送结束信号: exit(0)
            exit(0);
        });

    }
}
```

### 任务状态调度控制

消息的状态有四种: 准备,订阅,延迟,失败, 这些状态的生命周期如下所示:

```php
put with delay               release with delay
  ----------------> [DELAYED] <------------.
                        |                   |
                        | (time passes)     |
                        |                   |
   put                  v     reserve       |       delete
  -----------------> [READY] ---------> [RESERVED] --------> *poof*
                       ^  ^                |  |
                       |   \  release      |  |
                       |    `-------------'   |
                       |                      |
                       | kick                 |
                       |                      |
                       |       bury           |
                    [FAILED] <---------------'
                       |
                       |  delete
                        `--------> *poof*
```

处理进程中的任务处在订阅状态(RESERVED), 一般处理完后会自动delete, 有异常的话会bury到失败队列.
开发者也可以主动delete,release,或者bury这个任务:

```php
$queue->doWork('mytube', function(Caster $job) {
    $data = $job->getBody();

    if ($data['name'] != 'kevin') {
        $job->bury();
    } else {
        $job->release(120); // 将任务回置为延迟任务
    }

    exit(0);
});
```

处理失败状态的任务:

```php
$queue = Queue::getInstance();
$ids = $queue->getIdsByState(Caster::STATE_FAILED, 'mytube');
foreach ($ids as $id) {
    $job = Caster::reload($id);
    $job->kick();  // 将失败的任务重新放置到准备队列里
}
```


### 并发模式

多数情况,消息队列保持"FIFO"(先进先出)的原则, 然而也有业务会用到分布式场景.
分布式处理消息会提高处理效率,为了避免因并发读取而出现的问题,需要将配置文件中distributed 设置为1

```php
...
queue.redis.1.distributed = 1
```

