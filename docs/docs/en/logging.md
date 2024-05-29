---
title: System Log
---

# OceanBase System Log Introduction

## Introduction

This document mainly introduces the system logs of Oceanbase, including the classification and level of the log, how to output logs in the program, and the details of some log implementation.


## System Log Introduction

Similar to common application systems, system logs are one of the important means for Oceanbase developers to investigate problems.
Oceanbase's system log is stored under the log directory under the observer installation path. The system log is mainly divided into two categories:

1. Ordinary logs: with ".log" suffix, printed all logs (including warning logs) of a certain module.

2. Warning log: with ".log.wf" suffix, only printed the warn level of a module and above.

| log file name  | record information    |
| ------------------ | ----------------------------- |
| observer.log[.wf]       | General logs (warning logs, general query logs, other logs)       |
| rootservice.log[.wf]    | rootservice module log (including global DDL log)     |
| election.log[.wf]       | Election related logs                       |
| trace.log          | Full link tracking log |

Especially, trace.log does not have the corresponding ".wf" log.

In addition to output ordinary logs, `wf` logs also have a special `info` log, that is, every time the log file is created, some current systems and processes information will be recorded.

### Log Parameters

There are 7 parameters related to syslog, which are dynamically effective, that is, it can be adjusted dynamically during runtime.

| Configuration Item           | Type | Range   | Default Value    | Describtion   |
| --------------------------- | ---- | ----------------------------------------- | ------ | ------------------------------------- |
| enable_syslog_recycle       | Boolean   |  | False  | Whether to recycle the old log files  |
| enable_syslog_wf            | Boolean   |  | True   | Whether to print the WARN log level and above to a separate WF file |
| enable_async_syslog         | Boolean   | | True   | Whether to print the log asynchronous |
| max_syslog_file_count       | Integer   | \[0, +∞)     | 0      | The maximum number of each log file |
| syslog_io_bandwidth_limit   | String  | 0, Other legal size  | "30MB" | Log IO bandwidth limit |
| syslog_level                | String  | DEBUG, TRACE, WDIAG, EDIAG, INFO, WARN, ERROR | WDIAG | Log level|
| diag_syslog_per_error_limit | Integer   | \[0, +∞) | 200 | The maximum number of each error code of DIAG system log per second. |

> All the parameters here are cluster-level and dynamic effect.
> Refer to ob_parameter_seed.ipp file for more details.

## Log File Recycle

OceanBase's log can be configured with the upper limit of the number of files to prevent the log file from occupying too much disk space.

If `enable_syslog_recycle = true` and `max_syslog_file_count > 0`, the number of each type log files can not exceed `max_syslog_file_count`. OceanBase will detect and delete old log files periodically.

The new log files will print a special log at the beginning. The information contains the IP and ports of the current node, version number, and some system information. Refer to `ObLogger::log_new_file_info` for more details.

```
[2023-12-26 13:15:58.612579] INFO  New syslog file info: [address: "127.0.0.1:2882", observer version: OceanBase_CE 4.2.1.1, revision: 101010012023111012-2f6924cd5a576f09d6e7f212fac83f1a15ff531a, sysname: Linux, os release: 3.10.0-327.ali2019.alios7.x86_64, machine: x86_64, tz GMT offset: 08:00]
```

## Log Level

Similar to the common system, Oceanbase also provides log macro to print different levels of logs:

| Level    | Macro | Describtion |
| ----- | --------- | ---- |
| DEBUG | LOG_DEBUG | Developers debug logs |
| TRACE | LOG_TRACE | Incident tracking logs are usually viewed by developers |
| INFO  | LOG_INFO  | System state change log |
| WARN  | LOG_DBA_WARN  | For DBA. observer can provide services, but the behavior not meet expectations |
| ERROR | LOG_DBA_ERROR | For DBA. observer cannot provide services, such as the disk full of monitoring ports occupied. Need DBA intervention to restore service |
| WDIAG | LOG_WARN | Warning Diagnosis. Assisting the diagnostic information of fault investigation, and the errors in the expected expectations, if the function returns failure. The level is the same as WARN |
| EDIAG | LOG_ERROR | Error Diagnosis. Assisting the diagnostic information of faulty investigation, unexpected logical errors, such as the function parameters do not meet the expected, are usually Oceanbase program bugs. The level is the same as ERROR |


> Only the most commonly used log levels are introduced here. For more detailed information, please refer to the configuration of syslog_level in `ob_parameter_seed.ipp`, and macro definitions such as `LOG_ERROR` in the `ob_log_module.h` file.

**How to set up log level?**

There are three ways to adjust the log level:

- When the OceanBase process starts, it reads the log level config from configuration file or command line parameters. The configuration item name is `syslog_level`;
- After startup, you can also connect through the MySQL client and execute the SQL command `alter system set syslog_level='DEBUG'`;
- Modify the log level when the request is executed through the SQL Hint. For example `select /*+ log_level("ERROR") */ * from foo;`. This method is only effective for the current SQL request related logs.

You can refer to the code of dynamic modification log settings `ObReloadConfig::reload_ob_logger_set`。

```cpp
if (OB_FAIL(OB_LOGGER.parse_set(conf_->syslog_level,
                                    static_cast<int32_t>(STRLEN(conf_->syslog_level)),
                                    (conf_->syslog_level).version()))) {
      OB_LOG(ERROR, "fail to parse_set syslog_level",
             K(conf_->syslog_level.str()), K((conf_->syslog_level).version()), K(ret));
```

## How to Print Logs

Common systems use C ++ Stream mode or C fprintf style printing log, but Oceanbase is slightly different. Let's start with the example to see how to print logs.

### An Example of Printing Log

Unlike `fprintf`, Oceanbase's system log does not have a format string, but only "info" parameter, and each parameter information. For example:

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

Among the example, "start stmt" is the `INFO` information, and we uses the `K` macro to print objects.

### Log Field Introduction

A output of the example code above:

```text
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

> NOTE: The log output is wrapped for readability.

A log mainly contains the following parts:

| field       | example                                   | description                |
| -------- | ------------------------------------ | ----------------------------------- |
| time       | [2023-12-11 18:00:55.711877]         | The time of printing this log |
| level     | INFO   | The log level  |
| module       | [SQL.EXE] | The module printing the log |
| function name    | start_stmt | The function printing the log  |
| code location   | (ob_sql_trans_control.cpp:619)       | The location of code, including file name and line |
| thread identifier     | [99178][T1004_TeRec]  | The thread ID and name |
| tenant id     | [T1003] | The tenant ID                                |
| Trace ID | [YD9F97F000001-00060C36119D4757-0-0] | The global ID of a specific request. You can usually get all logs related one request according the trace ID |
| The cost of printing log   | [lt=15] | The cost in microsecond of printing last log |
| information | start stmt(...)  | The log information |

### Commonly Used Log Parameters Macro Introduction

For developers, we only need to care about how to output our object information. Usually we write `K(obj)` to output the information we want in the log. Below are some details。

In order to avoid some errors in format string, OceanBase uses automatic recognition of types and then serialization to solve this problem. Any parameter in the log will be identified as multiple Key Value pairs, where Key is the name of the field to be printed and Value is the value of the field. For example, `"consistency_level_in_plan_ctx", plan_ctx->get_consistency_level()` in the above example prints the name and value of a field. OceanBase automatically recognizes the type of Value and converts it to a string. The final output in the log may be "consistency_level_in_plan_ctx=3".

Because most logs print the original name and value of the specified object, OceanBase provides some macros to simplify the operation of printing logs. The most commonly used one is `K`. Taking the above example `K(ret)`, its expansion in the code is:

```cpp
"ret", ret
```

The final information in the log is:
```cpp
ret=-5595
```

OceanBase also provides some other macros, which are used in different scenarios.

> Log parameter macro definitions can be found in the `ob_log_module.h` file.

| macro  | example         | description |
| ------ | --------------- | ----------- |
| K | K(ret) | After expansion, it is `"ret", ret`. The parameter can be a simple value or an ordinary object |
| K_      | K_(consistency_level)   | After expansion, it is `"consistency_level", consistency_level_`. Different from K, the `_` suffix will be automatically added after the expanded Value, which is used for printing class member variables. |
| KR                          | KR(ret)                            | After expansion, it is `"ret", ret, "ret", common::ob_error_name(ret)`. This macro is for the convenience of printing error code and error code name. In OceanBase, `ret` is usually used as the return value of a function, and each return value has a corresponding string description. `ob_error_name` can get the string description corresponding to the error code. Note that this macro can only be used in non-lib code |
| KCSTRING/<br/>KCSTRING_     | KCSTRING(consistency_level_name)   | After expansion, it is `"consistency_level_name", consistency_level_name`. This macro is used to print C-formatted strings. Since a variable of type `const char *` does not necessarily represent a string in C++, such as a binary buffer, when printing the value of this variable, if it is printed as a C string, an illegal memory access error will occur, so this macro has been added to explicitly print C strings |
| KP/KP_                      | KP(plan)  | After expansion, it is `"plan", plan`, where `plan` is a pointer. This macro will print out the hexadecimal value of a pointer  |
| KPC/KPC_                    | KPC(session)  | The input parameters are object pointers. If it is NULL, "NULL" will be output. Otherwise, the `to_string` method of the pointer will be called to output the string. |
| KTIME                       | KTIME(cur_time)     | Convert timestamp converted to string. Timestamp unit microseconds  |
| KTIMERANGE/<br/>KTIMERANGE_ | KTIMERANGE(cur_time, HOUR, SECOND) | Convert the timestamp to a string and only obtain the specified range, such as the hour to second period in the example |
| KPHEX/KPHEX_                | KPHEX(buf, 20)                     | Print buf content in hexadecimal |
| KERRMSG                     | KERRMSG                            | Output system error code information |
| KERRNOMSG                   | KERRNOMSG(2)                       | Specify error code to output system error information |

## Some Implementation Details in the Log

### How to Convert Value to String

OceanBase automatically identifies the type of value you want to print in the log and converts it to a string. For example, in the above example, `ret` is an `int` type variable, and `plan_ctx->get_consistency_level()` returns an `enum` type variable. Both variables will be converted to strings.

However, since OceanBase does not know how to convert an ordinary object into a string, the user needs to implement a `TO_STRING_KV` function to convert the object into a string. For example, in the above example, `snapshot` is an object of type `ObTxReadSnapshot`. This object implements the `TO_STRING_KV` function, so it can be printed directly.

**Convert normal value to string**

OceanBase can automatically identify simple type values, such as `int`, `int64_t`, `double`, `bool`, `const char *`, etc., and convert them into strings. For enumeration types, they will be treated as numbers. For pointers, the pointer value will be output in hexadecimal format.

**Convert class object to string**

Since C++ does not have a reflection mechanism, it cannot automatically identify the member variables of a class object and convert them into strings. Therefore, the user needs to implement a `TO_STRING_KV` function to convert the object into a string. For example, in the above example, `snapshot` is an object of type `ObTxReadSnapshot`. This object implements the `TO_STRING_KV` function. You can refer to the implementation code as follows:

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

As you can see, in `TO_STRING_KV`, you can directly use a macro similar to printing logs to "list" the member variable names you want to output.

> NOTE: TO_STRING_KV is actually a macro definition. For specific implementation, please refer to `ob_print_utils.h`. TO_STRING_KV converts input parameters into strings and outputs them to a buffer.



### Log Module

OceanBase's logs are module-specific and can support sub-modules. For example, in the above example, `[SQL.EXE]` is a module, `SQL` is a main module, and `EXE` is a submodule. For the definition of the log module, please refer to the `LOG_MOD_BEGIN` and `DEFINE_LOG_SUB_MOD` related codes in the `ob_log_module.h` file.

**How does the log module output to the log?**

Normally, we just use macros like `LOG_WARN` to print logs, and different modules will be output, which is also achieved through macro definitions. Still taking the above log as an example, you can see a macro definition `#define USING_LOG_PREFIX SQL_EXE` at the beginning of the `ob_sql_trans_control.cpp` file. This macro defines the log module of the current file, that is, all logs in the current file the module `[SQL.EXE]` will be printed.

> There is also an issue here, that is, the header file introduced in the current implementation file will also use this module to print logs by default.


**How to specify module name explicitly?**

The above method is indeed a bit inflexible. OceanBase has another way to specify the module name, which is to use the macro `OB_MOD_LOG` or `OB_SUB_MOD_LOG`. The usage of these two macros is similar to `LOG_WARN`, except that there are additional module parameters and log levels:

```cpp
OB_MOD_LOG(parMod, level, info_string, args...)
OB_SUB_MOD_LOG(parMod, subMod, level, info_string, args...)
```

**Set the module's log level**

In addition to setting the global and current thread log levels, OceanBase can also adjust the log level of a certain module. Currently, you can use `SQL HINT` to modify the log level of a module when executing a request, for example:

```sql
select /*+ log_level("SHARE.SCHEMA:ERROR") */ * from foo;
```

Where `SHARE` is the main module, `SCHEMA` is the submodule, and `ERROR` is the log level. The function of this SQL HINT is to set the log level of the `SHARE.SCHEMA` module to `ERROR`, and is only valid for the current request.

### Log Time

OceanBase's log time is the number of microseconds in the current local time.
Since converting a timestamp into a string is a time-consuming task, OceanBase caches the timestamp conversion to speed up the process. For details, please refer to the `ob_fast_localtime` function.

### Thread Identifier

Currently, two information related to thread will be recorded:

- Thread ID: the information returned by the system call `__NR_gettid` (the system call is relatively inefficient, and this value will be cached);
- Thread name: The thread name field may contain the tenant ID, thread pool type, and thread pool index. The thread name of OceanBase is set through the `set_thread_name` function and will also be displayed in the `top` command.

> NOTE：The thread name is determined by the created thread. Since the tenant of the created thread may be different from the tenant of subsequent runs of this thread, the tenant in the thread name may be incorrect.

### Log Rate Limit

OceanBase supports two log rate limits: a common system log disk IO bandwidth limit and a WDIAG system log limit.


**System log bandwidth rate limit**

OceanBase will limit log output according to disk bandwidth. The log bandwidth rate limit does not limit the rate for different log levels. If the log rate is limited, the rate limit log may be printed with the keyword `REACH SYSLOG RATE LIMIT`.

Rate limit log example:

```txt
[2023-12-26 09:46:04.621435] INFO  [SHARE.LOCATION] fetch_vtable_location_ (ob_vtable_location_service.cpp:281) [35675][VTblLocAsyncUp0][T0][YB427F000001-00060D52A9614571-0-0] [lt=0]  REACH SYSLOG RATE LIMIT [bandwidth]
```

The rate limit can be adjusted through the configuration item `syslog_io_bandwidth_limit`.

Please refer to the `check_tl_log_limiter` function for rate limiting code details.

**WDIAG log rate limit**

OceanBase has implemented a current limit for WARN level logs. Each error code is limited to 200 logs per second by default. If the limit is exceeded, the current limiting log will be output, keyword `Throttled WDIAG logs in last second`. The current limiting threshold can be adjusted through the configuration item `diag_syslog_per_error_limit`.

Limiting log example:

```txt
[2023-12-25 18:01:15.527519] WDIAG [SHARE] refresh (ob_task_define.cpp:402) [35585][LogLimiterRefre][T0][Y0-0000000000000000-0-0] [lt=8][errcode=0] Throttled WDIAG logs in last second(details {error code, dropped logs, earliest tid}=[{errcode:-4006, dropped:31438, tid:35585}])
```

Limiting code reference `ObSyslogPerErrLimiter::do_acquire`。


## Some Other Details

### Logs for DBA

There are also two types of special logs in OceanBase, LOG_DBA_WARN and LOG_DBA_ERROR, which correspond to WARN and ERROR logs respectively. Since the volume of OceanBase logs is extremely large, and most of them can only be understood by R&D personnel, it brings a certain burden to DBA operation and maintenance troubleshooting problems. Therefore, these two types of logs are added, hoping that the DBA can only focus on a small amount of these two types of logs to troubleshoot system problems. The logs output using LOG_WARN and LOG_ERROR are converted into WDIAG and EDIAG logs to help developers troubleshoot problems.

### Output Prompt Information to the User Terminal

Sometimes we want to output the error message directly to the user's terminal, so that it can be more convenient for users to understand what error is currently occurring. At this time we can use `LOG_USER_ERROR`, `LOG_USER_WARN`, `LOG_USER_INFO` and other macros to print logs. Each error code has a corresponding `USER_ERROR_MSG`. If this `USER_ERROR_MSG` requires input parameters, then we also need to provide the corresponding parameters when printing the log. For example, the error code `OB_NOT_SINGLE_RESOURCE_POOL` has the corresponding `OB_NOT_SINGLE_RESOURCE_POOL__USER_ERROR_MSG`, and it's message is "create tenant only support single resource pool now, but pool list is %s", we just need to provide a string.

The LOG_USER_ERROR macro is defined as follows：

```cpp
#define LOG_USER_ERROR(errcode, args...)
```

The usage of other macros is similar.

> Error code definitions can be found in `src/share/ob_errno.h`.

Since `LOG_USER_XXX` provides fixed error information, if we want to output some customized information, we can use `FORWARD_USER_XXX`, such as `FORWARD_USER_ERROR`, `FORWARD_USER_WARN`, etc. Taking `FORWARD_USER_ERROR` as an example, its definition is as follows:

```cpp
#define FORWARD_USER_ERROR(errcode, args...)
```

### Health Log

OceanBase will periodically output some internal status information, such as the memory information of each module and tenant, to the log to facilitate problem finding. This kind of log usually outputs multiple lines of data in one log, such as:

```txt
[2023-12-26 13:15:58.608131] INFO  [LIB] print_usage (ob_tenant_ctx_allocator.cpp:176) [35582][MemDumpTimer][T0][Y0-0000000000000000-0-0] [lt=116]
[MEMORY] tenant_id=  500 ctx_id=                    GLIBC hold=      4,194,304 used=      1,209,328 limit= 9,223,372,036,854,775,807
[MEMORY] idle_size=         0 free_size=         0
[MEMORY] wash_related_chunks=         0 washed_blocks=         0 washed_size=         0
[MEMORY] hold=        858,240 used=        575,033 count=   3,043 avg_used=            188 block_cnt=      93 chunk_cnt=       2 mod=glibc_malloc
[MEMORY] hold=        351,088 used=        104,389 count=   3,290 avg_used=             31 block_cnt=      51 chunk_cnt=       1 mod=Buffer
[MEMORY] hold=      1,209,328 used=        679,422 count=   6,333 avg_used=            107 mod=SUMMARY
```

This kind of data can be helpful for finding historical issues.

### ERROR Log
For general errors that occur in the system, such as an exception when processing a certain request, logs will be output at WARN level. Only when the normal operation of the OceanBase process is affected, or if there is a serious problem, the log will be output at the ERROR level. Therefore, if a process exits abnormally or cannot be started, searching the ERROR log will more effectively find the cause of the problem.
