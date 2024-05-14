---
title: 调试
---

# 背景

这里介绍一些调试 OceanBase 的方法。我们有很多种调试方法，比如 gdb，日志等。

建议编译 OceanBase 时使用 debug 模式，这样更容易调试。

# GDB
GDB 是一个强大的调试工具，但是使用 gdb 调试 OceanBase 是比较困难的，而且场景比较有限。

如果要调试单进程并且只有某一个线程，可以使用 gdb，否则建议使用日志。

假设已经部署了源码编译的 oceanbase。

调试 OceanBase 与调试其他 C++ 程序类似，你可以使用 gdb，如下：

1. 找到进程 id
```bash
ps -ef | grep observer
```

或者
```bash
pidof observer
```

2. attach 进程
```bash
gdb observer <pid>
```

接着就可以设置断点，打印变量等。更多信息请参考 [gdb 手册](https://sourceware.org/gdb/current/onlinedocs/gdb.html/)。

## 使用 debug-info 包调试 OceanBase

要调试RPM部署的OceanBase，或者查看 coredump 文件，需要先安装或者加载 debug-info 包。推荐使用加载的模式，因为系统中会有很多 debug-info 包，而且很难清理。

首先，从网上下载 debug-info 包，然后加载到gdb。之后，你就可以很容易地调试 OceanBase 了。

下面是一些提示。

**如何查找 debug-info 包**

使用下面的命令获取包的revision。
```bash
# in the observer runtime path
clusters/local/bin [83] $ ./observer -V
./observer -V
observer (OceanBase_CE 4.1.0.1)

REVISION: 102000042023061314-43bca414d5065272a730c92a645c3e25768c1d05
BUILD_BRANCH: HEAD
BUILD_TIME: Jun 13 2023 14:26:23
BUILD_FLAGS: RelWithDebInfo
BUILD_INFO:

Copyright (c) 2011-2022 OceanBase Inc.
```

如果看到下面的错误信息
```
./observer -V
./observer: error while loading shared libraries: libmariadb.so.3: cannot open shared object file: No such file or directory
```

就换成这个命令来获取revision
```bash
clusters/local/bin [83] $ LD_LIBRARY_PATH=../lib:$LD_LIBRARY_PATH ./observer -V
./observer -V
observer (OceanBase_CE 4.1.0.1)

REVISION: 102000042023061314-43bca414d5065272a730c92a645c3e25768c1d05
BUILD_BRANCH: HEAD
BUILD_TIME: Jun 13 2023 14:26:23
BUILD_FLAGS: RelWithDebInfo
BUILD_INFO:

Copyright (c) 2011-2022 OceanBase Inc.
```

**下载 debug-info 包**

上面输出的版本信息中，我们需要的是 revision 字段的前半部分，即
```
REVISION: 102000042023061314-43bca414d5065272a730c92a645c3e25768c1d05
```
这个是我们需要的：`102000042023061314`。

接着在RPM网站上搜索 `102000042023061314`。

![download debug info package](images/download-debug-info-package.png)

RPM网站列表：
- [x86_64 for el7](http://mirrors.aliyun.com/oceanbase/community/stable/el/7/x86_64/)
- [x86_64 for el8](https://mirrors.aliyun.com/oceanbase/community/stable/el/8/x86_64/)
- [aarch64(arm) for el7](https://mirrors.aliyun.com/oceanbase/community/stable/el/7/aarch64/)
- [aarch64(arm) for el8](https://mirrors.aliyun.com/oceanbase/community/stable/el/8/aarch64/)

**从RPM中提取 debug-info package**

从RPM中提取 debug-info 包，例如
```bash
rpm2cpio oceanbase-ce-debuginfo-4.1.0.1-102000042023061314.el7.x86_64.rpm | cpio -div
```

解开后是这样的

```bash
~/tmp/debug-info [83] $ tree -a
.
└── usr
    └── lib
        └── debug
            ├── .build-id
            │   └── ee
            │       ├── f87ee72d228069aab083d8e6d2fa2fcb5c03f2 -> ../../../../../home/admin/oceanbase/bin/observer
            │       └── f87ee72d228069aab083d8e6d2fa2fcb5c03f2.debug -> ../../home/admin/oceanbase/bin/observer.debug
            └── home
                └── admin
                    └── oceanbase
                        └── bin
                            └── observer.debug
```

`observer.debug` 是我们要的 debug-info 包，`f87ee72d228069aab083d8e6d2fa2fcb5c03f2.debug` 是一个软链接。

**使用 debug-info 包调试 OceanBase**

使用gdb命令 attch 到一个进程或者打开coredump文件。

```bash
# attach 进程
gdb ./observer `pidof observer`
```

或

```bash
# 打开coredump文件
gdb ./observer <coredump file name>
```

正常情况下，会看到这个信息

```
Type "apropos word" to search for commands related to "word"...
Reading symbols from clusters/local/bin/observer...
(No debugging symbols found in clusters/local/bin/observer)
Attaching to program: clusters/local/bin/observer, process 57296
```

意思是没有调试符号。

运行一些调试命令，比如 `bt`，会看到这个信息。

```bash
(gdb) bt
#0  0x00007fb6e9c36d62 in pthread_cond_timedwait@@GLIBC_2.3.2 () from /lib64/libpthread.so.0
#1  0x00007fb6f9f44862 in ob_pthread_cond_timedwait ()
#2  0x00007fb6eee8d206 in oceanbase::common::ObThreadCond::wait_us(unsigned long) ()
#3  0x00007fb6f34b21c8 in oceanbase::observer::ObUniqTaskQueue<oceanbase::observer::ObServerSchemaTask, oceanbase::observer::ObServerSchemaUpdater>::run1() ()
#4  0x00007fb6f9f44259 in oceanbase::lib::Threads::run(long) ()
#5  0x00007fb6f9f40aca in oceanbase::lib::Thread::__th_start(void*) ()
```

看不到源码文件名和参数信息。

现在加载 debug-info 包。

```bash
(gdb) symbol-file usr/lib/debug/home/admin/oceanbase/bin/observer.debug
Reading symbols from usr/lib/debug/home/admin/oceanbase/bin/observer.debug...
```

> 使用debug-info文件的全路径更好

再次执行调试命令，就可以看到详细信息。

```bash
(gdb) bt
#0  0x00007fb6e9c36d62 in pthread_cond_timedwait@@GLIBC_2.3.2 () from /lib64/libpthread.so.0
#1  0x00007fb6f9f44862 in ob_pthread_cond_timedwait (__cond=0x7fb6fb1d5340, __mutex=0x7fb6fb1d5318, __abstime=0x7fb6b3ed41d0)
    at deps/oblib/src/lib/thread/ob_tenant_hook.cpp:124
#2  0x00007fb6eee8d206 in oceanbase::common::ObThreadCond::wait_us (this=<optimized out>, time_us=140422679606016)
    at deps/oblib/src/lib/lock/ob_thread_cond.cpp:106
#3  0x00007fb6f34b21c8 in oceanbase::common::ObThreadCond::wait (this=0x7fb6fb1d5310, time_ms=200)
    at deps/oblib/src/lib/lock/ob_thread_cond.h:69
#4  oceanbase::observer::ObUniqTaskQueue<oceanbase::observer::ObServerSchemaTask, oceanbase::observer::ObServerSchemaUpdater>::run1 (
    this=<optimized out>) at src/observer/ob_uniq_task_queue.h:417
```

# 日志

日志是调试 OceanBase 最常用的方法，易于使用，适用于大多数场景。
在常见的场景中，可以在代码中添加日志并打印变量，然后重新编译和部署 oceanbase。

## 如何加日志

可以在源码中找到日志代码，比如
```cpp
LOG_DEBUG("insert sql generated", K(insert_sql));
```

`LOG_DEBUG` 是打印DEBUG级别的日志宏。

这跟其它的程序有点不太一样。第一个参数是一个字符串，其他的参数通常是 `K(variable_name)`。`K` 是一个宏，用来打印变量名和值。

## 如何搜索日志

日志文件在 OceanBase 运行目录的 `log` 目录下。你可以使用 `grep` 命令搜索日志。

一个日志的例子。
```
[2023-07-05 16:40:42.635136] INFO  [SQL.EXE] explicit_start_trans (ob_sql_trans_control.cpp:194) [88022][T1003_ArbSer][T1003][YD9F97F000001-0005FFB71FCF95C7-0-0] [lt=42] start_trans(ret=0, tx_id={txid:2118151}, session={this:0x7ff2663d6188, id:1, tenant:"sys", tenant_id:1, effective_tenant:"sys", effective_tenant_id:1003, database:"oceanbase", user:"root@%", consistency_level:3, session_state:0, autocommit:true, tx:0x7ff26b0e4300}, read_only=false, ctx.get_execution_id()=18446744073709551615)
```

时间戳(`[2023-07-05 16:40:42.635136]`), 日志级别(`INFO`), 模块名称(`[SQL.EXE]`), 函数名称(`explicit_start_trans`), 代码文件(`ob_sql_trans_control.cpp`), 行号(`194`), 线程号(`88022`), 线程名称(`T1003_ArbSer`), trace id(`YD9F97F000001-0005FFB71FCF95C7-0-0`), 等等.

每个SQL请求都有一个唯一的 `trace id`。可以通过 `trace id` 来找到所有与指定SQL请求相关的日志。

## 日志的一些技巧
**Trace ID**

使用下面的SQL命令可以获取上一次执行SQL请求的 trace id。
```sql
select last_trace_id();
```
**日志级别**

使用下面的命令调整日志级别。
```sql
set ob_log_level=debug;
```
**Log 流量控制**

如果找不到想要的日志，可能是被限流了，可以使用下面的命令调整日志流量控制。

```sql
alter system set syslog_io_bandwidth_limit='1G';
alter system set diag_syslog_per_error_limit=1000;
```
**同步打印日志**

用下面的命令可以同步打印日志。
```sql
alter system set enable_async_syslog='False';
```
**打印调用栈**

在日志中可以这样打印调用栈
```cpp
LOG_DEBUG("insert sql generated", K(insert_sql), K(lbt()));
```
假设看到这样的信息：
```txt
lbt()="0x14371609 0xe4ce783 0x54fd9b6 0x54ebb1b 0x905e62e 0x92a4dc8 0x905df11 0x905dc94 0x13d2278e 0x13d22be3 0x6b10b81 0x6b0f0f7 0x62e2491 0x10ff6409 0x1475f87a 0x10ff6428 0x1475f1c2 0x1476ba83 0x14767fb5 0x14767ae8 0x7ff340250e25 0x7ff33fd0ff1d"
```

用这个命令查看调用栈信息：
```bash
addr2line -pCfe ./bin/observer 0x14371609 0xe4ce783 0x54fd9b6 0x54ebb1b 0x905e62e 0x92a4dc8 0x905df11 0x905dc94 0x13d2278e 0x13d22be3 0x6b10b81 0x6b0f0f7 0x62e2491 0x10ff6409 0x1475f87a 0x10ff6428 0x1475f1c2 0x1476ba83 0x14767fb5 0x14767ae8 0x7ff340250e25 0x7ff33fd0ff1d
```

我测试时看到的是这样的

```txt
oceanbase::common::lbt() at /home/distcc/tmp/./deps/oblib/src/lib/utility/ob_backtrace.cpp:130 (discriminator 2)
operator() at /home/distcc/tmp/./src/sql/session/ob_basic_session_info.cpp:599 (discriminator 2)
oceanbase::sql::ObBasicSessionInfo::switch_tenant(unsigned long) at /home/distcc/tmp/./src/sql/session/ob_basic_session_info.cpp:604
oceanbase::observer::ObInnerSQLConnection::switch_tenant(unsigned long) at /home/distcc/tmp/./src/observer/ob_inner_sql_connection.cpp:1813 (discriminator 2)
...
oceanbase::lib::Thread::run() at /home/distcc/tmp/./deps/oblib/src/lib/thread/thread.cpp:162
oceanbase::lib::Thread::__th_start(void*) at /home/distcc/tmp/./deps/oblib/src/lib/thread/thread.cpp:312
?? ??:0
?? ??:0
```

# SQL

一些SQL命令也可以帮助调试，首先运行下面的命令开启trace功能。
```sql
-- on 4.x
set ob_enable_show_trace=1;
```

然后运行SQL命令，比如：
```sql
select * from t, t1 where t.id=t1.id;
```

之后，你可以运行下面的命令获取trace信息。
```sql
show trace;
```

假设看到这样的信息
```txt
obclient> show trace;
+-------------------------------------------+----------------------------+------------+
| Operation                                 | StartTime                  | ElapseTime |
+-------------------------------------------+----------------------------+------------+
| com_query_process                         | 2023-07-06 15:30:49.907532 | 9.547 ms   |
| └── mpquery_single_stmt                   | 2023-07-06 15:30:49.907552 | 9.506 ms   |
|     ├── sql_compile                       | 2023-07-06 15:30:49.907615 | 6.605 ms   |
|     │   ├── pc_get_plan                   | 2023-07-06 15:30:49.907658 | 0.024 ms   |
|     │   └── hard_parse                    | 2023-07-06 15:30:49.907763 | 6.421 ms   |
|     │       ├── parse                     | 2023-07-06 15:30:49.907773 | 0.119 ms   |
|     │       ├── resolve                   | 2023-07-06 15:30:49.907952 | 0.780 ms   |
|     │       ├── rewrite                   | 2023-07-06 15:30:49.908857 | 1.320 ms   |
|     │       ├── optimize                  | 2023-07-06 15:30:49.910209 | 3.002 ms   |
|     │       ├── code_generate             | 2023-07-06 15:30:49.913243 | 0.459 ms   |
|     │       └── pc_add_plan               | 2023-07-06 15:30:49.914016 | 0.140 ms   |
|     └── sql_execute                       | 2023-07-06 15:30:49.914239 | 2.675 ms   |
|         ├── open                          | 2023-07-06 15:30:49.914246 | 0.217 ms   |
|         ├── response_result               | 2023-07-06 15:30:49.914496 | 1.956 ms   |
|         │   └── do_local_das_task         | 2023-07-06 15:30:49.914584 | 0.862 ms   |
|         └── close                         | 2023-07-06 15:30:49.916474 | 0.415 ms   |
|             ├── close_das_task            | 2023-07-06 15:30:49.916486 | 0.037 ms   |
|             └── end_transaction           | 2023-07-06 15:30:49.916796 | 0.064 ms   |
+-------------------------------------------+----------------------------+------------+
18 rows in set (0.01 sec)
```

# Debug Sync

在使用 gdb 调试 OceanBase 的时候，可能会出现问题，因为 gdb 会挂起进程，而 OceanBase 依赖心跳来正常工作。所以我们提供了一个 debug sync 机制来解决这个问题。

在代码中增加一个 debug sync 点，特定的线程会在这个点挂起，然后你可以做一些事情来调试这个进程，比如使用 gdb attach 进程，或者执行一些 SQL 命令来获取一些信息。

> Debug Sync 在 release 模式下也可以用，所以它在生产环境中是开启的。

## 如何使用

**在代码中增加一个debug sync**

打开文件 `ob_debug_sync_point.h`，在宏 `OB_DEBUG_SYNC_POINT_DEF` 中增加 debug sync 定义。比如：

```cpp
#define OB_DEBUG_SYNC_POINT_DEF(ACT)                               \
    ACT(INVALID_DEBUG_SYNC_POINT, = 0)                             \
    ACT(NOW,)                                                      \
    ACT(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT,)                \
    ACT(BEFORE_REBALANCE_TASK_EXECUTE,)                            \
    ACT(REBALANCE_TASK_MGR_BEFORE_EXECUTE_OVER,)                   \
    ACT(UNIT_BALANCE_BEFORE_PARTITION_BALANCE,)
```

要调试哪个函数，就在对应的函数加上 debug sync。比如：

```cpp
int ObRootService::do_restart()
{
  int ret = OB_SUCCESS;

  const int64_t tenant_id = OB_SYS_TENANT_ID;
  SpinWLockGuard rs_list_guard(broadcast_rs_list_lock_);
  ...
  DEBUG_SYNC(BEFORE_UNIT_MANAGER_LOAD);
  ...
}
```

可以在任何地方加 debug sync。

**开启 Debug Sync**

默认情况下，debug sync 是关闭的，通过下面的 SQL 命令开启它。

```sql
alter system set debug_sync_timeout='100000s';
```

将 `debug_sync_timeout` 设置为大于0的数字就可以开启。

> 注意：`debug_sync_timeout` 的单位是微秒。

**开启你自己的 debug sync point**

用这个命令开启自己加上的 debug sync。
```sql
set ob_global_debug_sync = 'BEFORE_UNIT_MANAGER_LOAD wait_for signal_name execute 10000';
```

`execute` 的意思是 debug sync 动作会在执行 10000 次后被禁用。

`signal_name` 是唤醒的名字。

当某个线程执行到debug sync时，就会停下来，这时候就可以执行一些调试操作。

**唤醒 debug sync point**

用下面的命令唤醒 debug sync。
```sql
set ob_global_debug_sync = 'now signal signal_name';
-- or
set ob_global_debug_sync = 'now broadcast signal_name';
```
`signal_name` 是你在开启 debug sync 点时设置的名字。

然后hang的线程会继续执行。

**清理 debug sync point**

调试完成后，需要清理掉 debug sync 点，可以使用下面的命令清理掉。
```sql
set ob_global_debug_sync = 'BEFORE_UNIT_MANAGER_LOAD clear';
```

**关闭 debug sync**

使用下面的命令关闭 debug sync。
```sql
alter system set debug_sync_timeout=0;
```

## debug sync 工作原理

当运行到指定的 debug sync 时，进程会使用 `condition_variable` 来等待信号，然后会挂起在 debug sync 点。当收到信号后，进程会继续执行。

可以查看代码 `ob_debug_sync.cpp/.h` 来了解更多关于 debug sync 机制的信息。
