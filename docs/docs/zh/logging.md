---
title: OceanBase 系统日志介绍
---

# OceanBase 系统日志介绍

## Introduction

本篇文档主要介绍OceanBase的系统日志，包括日志的分类、级别，如何在程序中输出日志，以及一些日志的实现细节。


## System Log Introduction

与常见的应用系统类似，系统日志是OceanBase开发人员排查问题时常用的重要手段之一。

OceanBase 的系统日志存储在observer安装路径下的log目录下面。系统日志主要分为两类：

1. 普通日志：log 后缀，打印某一模块的所有日志（包含警告日志）。
1. Ordinary logs: with log suffix, printed all logs (including warning logs) of a certain module.

2. 警告日志：log.wf 后缀，只打印某一模块的WARN级别及以上的日志。
2. Warning log: with log.wf suffix, only printed the warn level of a module and above.

| 日志文件名称 log file name  | 记录的信息 record information    |
| ------------------ | ----------------------------- |
| observer.log[.wf]       | 一般日志（警告日志、一般查询日志、其他日志） General logs (warning logs, general query logs, other logs)       |
| rootservice.log[.wf]    | rootservice模块的日志（包含全局ddl日志）rootService module log (including global DDL log)     |
| election.log[.wf]       | 选举相关日志 Election related logs                       |
| trace.log          | 全链路追踪日志 Full link tracking log |

> 比较特殊的是，trace.log 没有对应的".wf"日志。
> wf 日志除了输出普通的日志，还有一条特殊的INFO日志，就是每次日志文件创建时会记录一些当前系统、进程的信息。

### 日志参数 Log Parameters

目前syslog相关的有7个参数，都是动态生效，即可以在运行时动态调整。

| 配置项                         | 数据类型 | 值域                                        | 默认值    | 描述                                    |
| --------------------------- | ---- | ----------------------------------------- | ------ | ------------------------------------- |
| enable_syslog_recycle       | 布尔   |                                           | False  | 是否回收重启之前的旧日志（时间戳最小的日志文件）              |
| enable_syslog_wf            | 布尔   |                                           | True   | 是否打印WARN级别及以上的日志到单独的wf文件中             |
| enable_async_syslog         | 布尔   |                                           | True   | 是否异步打印日志                              |
| max_syslog_file_count       | 整型   | \[0, +∞)                                  | 0      | 每种只读日志文件最大保留数量                        |
| syslog_io_bandwidth_limit   | 字符串  | 0，其他合法大小                                  | "30MB" | 日志IO带宽限制                              |
| syslog_level                | 字符串  | DEBUG, TRACE, WDIAG, EDIAG, INFO, WARN, ERROR | WDIAG | 日志打印最低等级，该等级及以上的日志都打印                 |
| diag_syslog_per_error_limit | 整型   | \[0, +∞)                                  | 200    | 每个错误码每秒允许的 DIAG 系统日志数，超过该阈值后，日志将不再打印。 |

> 这里所有的参数都是集群级的，并且都是动态生效。
> 参数定义可以参考 ob_parameter_seed.ipp 文件。

## 日志回收

OceanBase的日志可以配置文件个数上限，以防止日志文件占用过大的磁盘空间。

如果 `enable_syslog_recycle = true` 且 `max_syslog_file_count > 0` ，每种日志文件的数量不能超过 `max_syslog_file_count`。日志内容刷新到磁盘中时触发旧日志文件回收。

每次执行 `flush_logs_to_file` 函数写日志时，如果某种文件写入了数据，就要检查是否需要将当前日志文件进行归档（有可能达到了大小上限）。
在 `max_syslog_file_count > 0` 的前提下，就可能会调用 `ObLogger::rotate_log` 函数，如果该种日志文件数量超过上限 `max_syslog_file_count`，就会删掉最旧的一个日志文件。

新日志文件都会在开头打印一个特殊日志，信息包含当前节点的IP和端口、版本号、以及一些系统信息，参考 `ObLogger::log_new_file_info`。

```
[2023-12-26 13:15:58.612579] INFO  New syslog file info: [address: "127.0.0.1:2882", observer version: OceanBase_CE 4.2.1.1, revision: 101010012023111012-2f6924cd5a576f09d6e7f212fac83f1a15ff531a, sysname: Linux, os release: 3.10.0-327.ali2019.alios7.x86_64, machine: x86_64, tz GMT offset: 08:00]
```

## 日志级别

与常见的系统打日志方法类似，OceanBase 也提供了日志宏来打印不同级别的日志：

| 级别    | 代码宏       | 说明 |
| ----- | --------- | ---- |
| DEBUG | LOG_DEBUG | 开发人员调试日志 |
| TRACE | LOG_TRACE | 事件跟踪日志，通常也是开发人员查看 |
| INFO  | LOG_INFO  | 系统状态变化日志 |
| WARN  | LOG_DBA_WARN  | 面向DBA的日志。出现非预期场景，observer能提供服务，但行为可能不符合预期，比如我们的写入限流 |
| ERROR | LOG_DBA_ERROR | 面向DBA的日志。observer不能提供正常服务的异常，如磁盘满监听端口被占用等。也可以是我们产品化后的一些内部检查报错，如我们的4377(dml defensive check error), 4103 (data checksum error)等，需DBA干预恢复 |
| WDIAG | LOG_WARN | Warning Diagnosis, 协助故障排查的诊断信息，预期内的错误，如函数返回失败。级别与WARN相同 |
| EDIAG | LOG_ERROR | Error Diagnosis, 协助故障排查的诊断信息，非预期的逻辑错误，如函数参数不符合预期等，通常为OceanBase程序BUG。级别与ERROR相同 |


> 这里仅介绍了最常用的日志级别，更详细的信息参考 `ob_parameter_seed.ipp` 中关于 `syslog_level` 的配置，以及`ob_log_module.h` 文件中 `LOG_ERROR` 等宏定义。

**如何设置日志级别？**

有三种方式调整日志级别：

- OceanBase 程序在启动时会读取配置文件或通过命令行参数输入系统日志级别配置，配置项名称是`syslog_level`。
- 启动后也可以通过MySQL客户端连接，执行SQL命令 `alter system set syslog_level='DEBUG`;
- 通过SQL HINT修改执行请求时的日志级别，比如 `select /*+ log_level("ERROR") */ * from foo;`。这种方式只对当前SQL请求相关的日志有效。

动态修改日志的代码可以参考 `ObReloadConfig::reload_ob_logger_set`。

```cpp
if (OB_FAIL(OB_LOGGER.parse_set(conf_->syslog_level,
                                    static_cast<int32_t>(STRLEN(conf_->syslog_level)),
                                    (conf_->syslog_level).version()))) {
      OB_LOG(ERROR, "fail to parse_set syslog_level",
             K(conf_->syslog_level.str()), K((conf_->syslog_level).version()), K(ret));
```

## 如何打印日志

常见的系统会使用C++ stream方式或C fprintf风格打印日志，但是OceanBase略有不同。接下来从示例入手看如何打印日志。

### 打印日志的例子

与fprintf不一样，OceanBase的系统日志没有formt string，而只有"info"参数，和各个参数信息。比如：

```cpp
LOG_INFO("start stmt", K(ret),
             K(auto_commit),
             K(session_id),
             K(snapshot),
             K(savepoint),
             KPC(tx_desc),
             K(plan_type),
             K(stmt_type),
             K(has_for_update),
             K(query_start_time),
             K(use_das),
             K(nested_level),
             KPC(session),
             K(plan),
             "consistency_level_in_plan_ctx", plan_ctx->get_consistency_level(),
             K(trans_result));
```

其中 `"start stmt"` 就是INFO信息，而后面通常使用 `K` 宏与对象。

### 日志各字段介绍

上面示例代码的一个输出：

```textile
[2023-12-11 18:00:55.711877] INFO  [SQL.EXE] start_stmt (ob_sql_trans_control.cpp:619) 
[99178][T1004_TeRec][T1003][YD9F97F000001-00060C36119D4757-0-0] [lt=15] 
start stmt(ret=0, auto_commit=true, session_id=1, 
snapshot={this:0x7f3184fca0e8, valid:true, source:2, 
core:{version:{val:1702288855549635029, v:0}, tx_id:{txid:167035}, 
scn:1702288855704049}, uncertain_bound:0, snapshot_lsid:{id:1}, 
snapshot_ls_role:0, parts:[{left:{id:1}, right:491146514786417}]}, 
savepoint=1702288855704049, tx_desc={this:0x7f31df697420, 
tx_id:{txid:167035}, state:2, addr:"127.0.0.1:55801", tenant_id:1003, 
session_id:1, assoc_session_id:1, xid:NULL, xa_mode:"", 
xa_start_addr:"0.0.0.0:0", access_mode:0, tx_consistency_type:0, 
isolation:1, snapshot_version:{val:18446744073709551615, v:3}, 
snapshot_scn:0, active_scn:1702288855704040, op_sn:6, alloc_ts:1702288855706134, 
active_ts:1702288855706134, commit_ts:-1, finish_ts:-1, timeout_us:29999942, 
lock_timeout_us:-1, expire_ts:1702288885706076, coord_id:{id:-1}, 
parts:[{id:{id:1}, addr:"127.0.0.1:55801", epoch:491146514786417, 
first_scn:1702288855704043, last_scn:1702288855704048, last_touch_ts:1702288855704044}], 
exec_info_reap_ts:1702288855704043, commit_version:{val:18446744073709551615, v:3}, 
commit_times:0, commit_cb:null, cluster_id:1, cluster_version:17180065792, 
flags_.SHADOW:false, flags_.INTERRUPTED:false, flags_.BLOCK:false, 
flags_.REPLICA:false, can_elr:true, cflict_txs:[], abort_cause:0, 
commit_expire_ts:0, commit_task_.is_registered():false, ref:2}, 
plan_type=1, stmt_type=5, has_for_update=false, query_start_time=1702288855711692, 
use_das=false, nested_level=0, session={this:0x7f31de2521a0, id:1, 
deser:false, tenant:"sys", tenant_id:1, effective_tenant:"sys", 
effective_tenant_id:1003, database:"oceanbase", user:"root@%", 
consistency_level:3, session_state:0, autocommit:true, tx:0x7f31df697420}, 
plan=0x7f31565ba050, consistency_level_in_plan_ctx=3, 
trans_result={incomplete:false, parts:[], touched_ls_list:[], 
cflict_txs:[]})
```

> NOTE: 实际日志中一行日志并没有换行，这里为了方便查看增加了换行

一条日志中主要包含了以下几个部分：

| 内容       | 示例                                   | 描述                                  |
| -------- | ------------------------------------ | ----------------------------------- |
| 时间       | [2023-12-11 18:00:55.711877]         | 日志打印时间                              |
| 日志级别     | INFO                                 | 日志级别                                |
| 模块       | [SQL.EXE]                            | 日志打印模块                              |
| 函数名称     | start_stmt                           | 打印日志的函数名称                           |
| 代码位置信息   | (ob_sql_trans_control.cpp:619)       | 打印日志的代码位置。包含文件名和行号                  |
| 线程标识     | [99178][T1004_TeRec]                 | 打印日志的线程号、线程名称                       |
| 租户信息     | [T1003]                              | 租户信息                                |
| Trace ID | [YD9F97F000001-00060C36119D4757-0-0] | 某个请求的全局唯一标识。通常可以通过这个标识获取所有当前请求的所有日志 |
| 打印日志时间   | [lt=15]                              | 当前线程打印上一条日志花费的时间，单位微秒               |
| 日志信息     | start stmt(...)                      | 日志信息                                |

### 日志参数常用宏介绍

对于开发者来说，我们只需要关心如何输出我们的对象信息。通常我们编写`K(obj)`即可将我们想要的信息输出到日志中。下面介绍一些细节。

OceanBase 为了避免format string的一些错误，使用自动识别类型然后序列化来解决这个问题。日志任意参数中会识别为多个Key Value对，其中Key是要打印的字段名称，Value是字段的值。比如上面的示例中的 `"consistency_level_in_plan_ctx", plan_ctx->get_consistency_level()` ，就是打印了一个字段的名称和值，OceanBase 自动识别 Value 的类型并转换为字符串。

因为大部分日志都是打印对象的某个字段，所以OceanBase提供了一些宏来简化打印日志的操作。最常用的就是 `K`，以上面的例子 `K(ret)`，其展开后在代码中是：

```cpp
"ret", ret
```

最终展示在日志中是：
```cpp
ret=-5595
```

OceanBase 还提供了一些其它的宏，在不同的场景下使用不同的宏。

> 日志参数宏定义可以在 `ob_log_module.h` 文件中找到。

| 宏                           | 示例                                 | 说明                                                                                                                                                                                        |
| --------------------------- | ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| K                           | K(ret)                             | 展开后就是 `"ret", ret`。参数可以是一个简单值，也可以是一个普通对象                                                                                                                                                  |
| K_                          | K_(consistency_level)              | 展开后是 `"consistency_level", consistency_level_`。与 K 不同的是，会在展开的 Value 后自动增加 `_`后缀，用在类成员变量的打印                                                                                                |
| KR                          | KR(ret)                            | 展开后是 `"ret", ret, "ret", common::ob_error_name(ret)`。这个宏是为了方便打印错误码与错误码名称。在 OceanBase 中，通常使用 `ret` 作为函数的返回值，而每个返回值会有一个对应的字符串描述。`ob_error_name` 就可以获得错误码对应的字符串描述。注意，这个宏只能用在非lib代码中          |
| KCSTRING/<br/>KCSTRING_     | KCSTRING(consistency_level_name)   | 展开后是 `"consistency_level_name", consistency_level_name`。这个宏是为了打印 C 格式的字符串。由于`const char *` 类型的变量在C++中未必表示一个字符串，比如一个二进制buffer，那么打印这个变量的值时，当做C字符串来打印输出的话，会遇到访问非法内存的错误，所以增加了这个宏，用来明确表示打印C字符串 |
| KP/KP_                      | KP(plan)                           | 展开后是 `"plan", plan`，其中 `plan` 是一个指针。这个宏将会打印出某个指针的十六进制值                                                                                                                                    |
| KPC/KPC_                    | KPC(session)                       | 输入参数是对象指针。如果是NULL时会输出NULL，否则调用指针的to_string方法输出字符串 |
| KTIME                       | KTIME(cur_time)                    | 时间戳转换为字符串。时间戳单位微秒                                                                                                                                                                         |
| KTIMERANGE/<br/>KTIMERANGE_ | KTIMERANGE(cur_time, HOUR, SECOND) | 时间戳转换为字符串，仅获取指定范围，比如示例中的获取小时到秒这一段                                                                                                                                                         |
| KPHEX/KPHEX_                | KPHEX(buf, 20)                     | 十六进制打印buf内容                                                                                                                                                                               |
| KERRMSG                     | KERRMSG                            | 输出系统错误码信息                                                                                                                                                                                 |
| KERRNOMSG                   | KERRNOMSG(2)                       | 指定错误码输出系统错误信息                                                                                                                                                                             |

## 日志中的一些实现细节

### 值如何转换为字符串

OceanBase 会自动识别日志中想要打印的值的类型，然后将其转换为字符串。比如上面的例子中，`ret` 是一个 `int` 类型的变量，而 `plan_ctx->get_consistency_level()` 返回的是一个 `enum` 类型的变量，这两个变量都会被转换为字符串。

不过由于OceanBase不知道如何将一个普通对象转换为字符串，所以需要用户自己实现一个 `TO_STRING_KV` 函数，来将对象转换为字符串。比如上面的例子中，`snapshot` 是一个 `ObTxReadSnapshot` 类型的对象，这个对象实现了 `TO_STRING_KV` 函数，所以可以直接打印。

**普通值转换为字符串**


OceanBase 可以自动识别简单类型的值，比如`int`、`int64_t`、`double`、`bool`、`const char *` 等，将其转换为字符串。对于枚举类型，会当做数字来处理。对于指针，将会使用十六进制的方式输出指针值。



**类对象转换为字符串**


由于C++没有反射机制，所以无法自动识别一个类对象的成员变量，将其转换为字符串。所以需要用户自己实现一个 `TO_STRING_KV` 函数，来将对象转换为字符串。比如上面的例子中，`snapshot` 是一个 `ObTxReadSnapshot` 类型的对象，这个对象实现了 `TO_STRING_KV` 函数，可以参考实现代码如下：

```cpp
class ObTxReadSnapshot {
   ...
   TO_STRING_KV(KP(this),
               K_(valid),
               K_(source),
               K_(core),
               K_(uncertain_bound),
               K_(snapshot_lsid),
               K_(parts));
};
```

可以看到，在 `TO_STRING_KV` 直接使用与打印日志类似的宏，"列出"想要输出的成员变量名称即可。

> NOTE: TO_STRING_KV 其实是一个宏定义，具体实现可以参考 ob_print_utils.h。 TO_STRING_KV 将输入参数转换为字符串输出到一个buffer中。



### 日志模块

OceanBase 的日志是区分模块的，而且可以支持子模块。比如上面的例子中，`[SQL.EXE]` 就是一个模块，`SQL` 是一个主模块，`EXE` 是一个子模块。日志模块的定义可以参考 `ob_log_module.h` 文件中 `LOG_MOD_BEGIN` 和 `DEFINE_LOG_SUB_MOD` 相关的代码。



**日志模块是如何输出到日志中的？**


通常情况下，我们仅仅使用像`LOG_WARN`这样的宏来打印日志，就会输出不同的模块，这也是通过宏定义来实现的。仍然以上面的日志为例，在 `ob_sql_trans_control.cpp` 文件的开始可以看到一个宏定义 `#define USING_LOG_PREFIX SQL_EXE`，这个宏定义了当前文件的日志模块，也就是说，当前文件中的所有日志都会打印 `[SQL.EXE]` 这个模块。

> 这里也有一个缺陷，就是当前实现文件中引入的头文件，默认也会使用这个模块来打印日志。



**如何明确的指定模块名称？**


使用上面的方式确实有些不够灵活，OceanBase 还有另一种方式来指定模块名称，即使用宏 `OB_MOD_LOG` 或 `OB_SUB_MOD_LOG`。这两个宏的使用方法与 `LOG_WARN` 类似，只是多了模块参数与日志级别：

```cpp
OB_MOD_LOG(parMod, level, info_string, args...)
OB_SUB_MOD_LOG(parMod, subMod, level, info_string, args...)
```

**设置模块的日志级别**
OceanBase 除了可以设置全局和当前线程的日志级别，还可以调整某个模块的日志级别。当前可以通过 SQL HINT 的方式修改执行请求时的某个模块的日志级别，比如：

```sql
select /*+ log_level("SHARE.SCHEMA:ERROR") */ * from foo;
```

其中 `SHARE` 是主模块，`SCHEMA` 是子模块，`ERROR` 是日志级别。这个 SQL HINT 的作用是将 `SHARE.SCHEMA` 模块的日志级别设置为 `ERROR`，并且只对当前请求有效。

### 日志时间

OceanBase 的日志时间是当前本地时间的微秒数。
由于将时间戳转换为字符串是一个相当耗时的事情，OceanBase 将时间戳转换缓存下来以加速该过程，具体可以参考 `ob_fast_localtime` 函数。

### 线程标识

当前会记录两个线程相关的信息：

- 线程号：系统调用 `__NR_gettid` 返回的信息（系统调用比较低效，这个值会缓存下来）；
- 线程名称：线程名称字段中可能会包含租户ID、线程池类型和线程池中的编号。OceanBase的线程号是通过 `set_thread_name` 函数设置的，还会显示在 `top` 命令中。

> NOTE：线程名称是由创建的线程决定的，由于创建线程的租户可能与此线程后续运行的租户不同，所以线程名称中的租户可能不对。

### 日志限速

OceanBase 支持两种日志限速：一个普通系统日志磁盘IO带宽限制，一个是WARN系统日志限制。


**系统日志带宽限速**

OceanBase 会按照磁盘带宽来限制日志输出。日志带宽限速不会针对不同的日志级别限速。如果日志限速，可能会打印限速日志，关键字 `REACH SYSLOG RATE LIMIT `。

限速日志示例：

```txt
[2023-12-26 09:46:04.621435] INFO  [SHARE.LOCATION] fetch_vtable_location_ (ob_vtable_location_service.cpp:281) [35675][VTblLocAsyncUp0][T0][YB427F000001-00060D52A9614571-0-0] [lt=0]  REACH SYSLOG RATE LIMIT [bandwidth]
```

可以通过配置项 `syslog_io_bandwidth_limit` 来调整限速。

限速的代码细节请参考 `check_tl_log_limiter` 函数。

**WDIAG 日志限速**
OceanBase 对WARN级别的日志做了限流，每个错误码每秒钟默认限制输出200条日志。超过限制会输出限流日志，关键字 `Throttled WDIAG logs in last second`。可以通过配置项 `diag_syslog_per_error_limit` 来调整限流阈值。

限流日志示例：

```txt
[2023-12-25 18:01:15.527519] WDIAG [SHARE] refresh (ob_task_define.cpp:402) [35585][LogLimiterRefre][T0][Y0-0000000000000000-0-0] [lt=8][errcode=0] Throttled WDIAG logs in last second(details {error code, dropped logs, earliest tid}=[{errcode:-4006, dropped:31438, tid:35585}])
```

限流代码参考 `ObSyslogPerErrLimiter::do_acquire`。




## 一些其它细节

### DBA日志

OceanBase 中还有两类特殊的日志，LOG_DBA_WARN 和 LOG_DBA_ERROR，分别对应了WARN和ERROR日志。由于OceanBase日志量特别大，并且大部分只有研发人员才能理解，为DBA运维排查问题带来了一定的负担，因此增加这两种日志，希望让DBA仅关注少量的这两类日志即可排查系统问题。而使用 LOG_WARN 和 LOG_ERROR 输出的日志被转换成 WDIAG 和 EDIAG 日志，帮助开发人员排查问题。

### 输出提示信息到用户终端

有时候我们希望将错误信息直接输出到用户的终端，可以更方便的了解当前发生了什么错误。这时我们可以使用 `LOG_USER_ERROR`、`LOG_USER_WARN`、`LOG_USER_INFO`等宏来打印日志。每个错误码都有一个对应的`USER_ERROR_MSG`，如果这个`USER_ERROR_MSG`需要输入参数，那么我们在打印日志的时候也需要提供对应的参数。比如错误码`OB_NOT_SINGLE_RESOURCE_POOL`，有对应的 `OB_NOT_SINGLE_RESOURCE_POOL__USER_ERROR_MSG`，信息是 "create tenant only support single resource pool now, but pool list is %s"，我们就需要提供一个字符串。

LOG_USER_ERROR 宏定义如下：

```cpp
#define LOG_USER_ERROR(errcode, args...)
```

其它宏的使用方法类似。

> 错误码的定义可以在 `src/share/ob_errno.h` 中找到。

由于`LOG_USER_XXX` 提供了固定的错误信息，如果我们想要输出一些自定义的信息，可以使用 `FORWARD_USER_XXX`，比如 `FORWARD_USER_ERROR`，`FORWARD_USER_WARN`等。以 `FORWARD_USER_ERROR`为例，其定义如下：

```cpp
#define FORWARD_USER_ERROR(errcode, args...)
```

### 健康日志

OceanBase 会周期性的输出一些内部状态信息，比如各模块、租户的内存信息，到日志中，方便查找问题。这种日志通常一条会输出多行数据，比如：

```txt
[2023-12-26 13:15:58.608131] INFO  [LIB] print_usage (ob_tenant_ctx_allocator.cpp:176) [35582][MemDumpTimer][T0][Y0-0000000000000000-0-0] [lt=116]
[MEMORY] tenant_id=  500 ctx_id=                    GLIBC hold=      4,194,304 used=      1,209,328 limit= 9,223,372,036,854,775,807
[MEMORY] idle_size=         0 free_size=         0
[MEMORY] wash_related_chunks=         0 washed_blocks=         0 washed_size=         0
[MEMORY] hold=        858,240 used=        575,033 count=   3,043 avg_used=            188 block_cnt=      93 chunk_cnt=       2 mod=glibc_malloc
[MEMORY] hold=        351,088 used=        104,389 count=   3,290 avg_used=             31 block_cnt=      51 chunk_cnt=       1 mod=Buffer
[MEMORY] hold=      1,209,328 used=        679,422 count=   6,333 avg_used=            107 mod=SUMMARY
```

这种数据会查找历史问题很有帮助。

### ERROR 日志
对系统出现的一般错误，比如处理某个请求时，出现了异常，会以WARN级别输出日志。只有影响到OceanBase进程正常运行，或者认为有严重问题时，会以ERROR级别输出日志。因此如果遇到进程异常退出，或者无法启动时，搜索ERROR日志会更有效地查找问题原因。
