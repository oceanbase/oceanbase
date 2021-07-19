# clog 日志格式说明

通常 clog 文件中包含多种类型的日志，其中 OB_LOG_SUBMIT 为主要的日志类型，表示对应的 clog 日志为其他模块（非 clog 模块）提交的日志，包含事务模块向 clog 提交的事务日志。本节介绍事务日志的打印格式。

事务日志的打印格式主要包含三部分：摘要（abstract）、头部（header）和主体（content）。日志形式如下所示：

```bash
$$$abstract |header |||   |||content
```

## 摘要

摘要描述了日志记录在日志文件中的相关信息，内容形式如下所示：

```bash
file_id:*** offset:*** len:***  OB_LOG_SUBMIT
```

下表列出了字段名的详细信息。

| 字段名 | 说明 |
| --- | --- |
| file_id | clog 日志文件名 |
| offset | 日志记录在日志文件中的偏移量 |
| len | 日志记录的长度 |
| OB_LOG_SUBMIT | clog 日志类型为 OB_LOG_SUBMIT |

## 头部

头部描述了日志记录自身的相关信息，内容形式如下所示：

```bash
ObLogEntryHeader:{magic:***, version:***, type:***, partition_key:***, log_id:***, data_len:***, generation_timestamp:***, epoch_id:***, proposal_id:***, submit_timestamp:***, is_batch_committed:***, is_trans_log:***, data_checksum:***, active_memstore_version:***, header_checksum:***}
```

下表列出了部分字段名的详细信息。

| 字段名 | 说明 |
| --- | --- |
| partition_key | 日志记录对应的数据分区的分区标识符 |
| log_id | 日志记录的标识符，可看作日志序列号 |
| generation_timestamp_ | 日志记录产生的时间点（Unix 时间戳） |
| epoch_id | 选举相关变量，选举周期标识符（Unix 时间戳） |
| proposal_id | Paxos 协议相关变量 |
| is_trans_log | 日志记录是否是事务记录 |

## 主体

主体描述了该日志记录包含的数据。如果该日志记录是事务日志记录，内容形式如下所示：

```bash
Trans: log_type:***, trans_inc:*** {[trans_log]}||| [mutator]
```

下表列出了部分字段名的详细信息。

| 字段名 | 说明 |
| --- | --- |
| `log_type` | 事务日志的类型 |
| `trans_id` | 事务日志对应的事务标识符 |
| `[trans_log]` | 事务日志的相关信息 |
| `[mutator]` | 如果事务日志类型为 redo，则该日志包含事务修改内容 |

由于 OceanBase 数据库限制一条日志记录的大小，因此当一个事务的修改数据量较大时，可能产生多条 redo 类型的事务日志记录。
