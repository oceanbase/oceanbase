# clog_tool

OceanBase 数据库的 clog 日志类似于传统数据库的 Redo 日志。clog 日志负责在事务提交时持久化事务数据。事务数据是事务对数据库的修改记录。clog_tool 用于解析并打印 clog 文件。目前，clog_tool 集成在 ob_admin 工具中，您可以通过执行 `ob_admin` 命令调用 clog_tool。

<!-- 为了保证高可用和一致性，OceanBase 数据库采用了 Paxos 协议对影响数据库状态的操作进行持久化。持久化产生的一致性日志文件即为 clog 日志。 -->

## clog_tool 命令

下表列出了 clog_tool 的常用命令。

| 命令名 | 说明 |
| --- | --- |
| `dump_all` | 解析并打印指定的 clog 文件。 |
| `dump_hex` | 解析并打印指定的 clog 文件，并将不可打印的字符转换为十六进制字符打印。 |
| `dump_format` | 解析指定的 clog 文件，并将这个日志文件按照类 SQL 形式打印。 |
| `dump_filter` | 解析指定的 clog 文件，并打印这个日志文件中满足过滤条件的记录。 |
| `stat_clog` | 解析指定的 clog 文件，并打印这个日志文件的统计信息。|

### `dump_all` 命令

```bash
ob_admin clog_tool dump_all <clog_file_name>
```

使用 `dump_all` 命令解析并打印指定的 clog 文件。解析多个文件时，文件名需要使用空格隔开。

其中 `clog_file_name` 为指定 clog 文件名。`clog_file_name` 可以包含文件路径。例如：

解析并打印单个 clog 文件。此处待解析的 clog 文件名为 `101`，且这个文件位于当前目录，则运行以下命令解析并打印该文件。

```bash
ob_admin clog_tool dump_all 101
```

解析并打印多个 clog 文件。此处待解析的 clog 文件名为 `101` 和 `103`，且这两个文件均位于当前目录，则运行以下命令解析并打印这两个文件。

```bash
ob_admin clog_tool dump_all 101 103
```

解析并打印多个文件名递增的 clog 文件。此处待解析的 clog 文件名为 `101`、`102`、`103` 和 `104`，且这些文件均位于当前目录，则运行以下命令解析并打印这些文件。

```bash
ob_admin clog_tool dump_all {101..104}
```

### `dump_hex` 命令

```bash
ob_admin clog_tool dump_hex <clog_file_name>
```

如果 clog 日志中包含不可打印的字符，那么 ob_admin 将无法通过 `dump_all` 命令打印这部分内容。使用 `dump_hex` 命令将不可打印的字符转为十六进制字符并打印。

`<clog_file_name>` 详情，参考 `dump_all` 命令。

### `dump_format` 命令

```bash
ob_admin clog_tool dump_format <clog_file_name>
```

通过 `dump_all` 命令打印的日志不便于阅读。为了增强可读性，使用 `dump_format` 命令解析指定的 clog 文件，并将这个日志文件按照类 SQL 形式打印。

`<clog_file_name>` 详情，参考 `dump_all` 命令。

### `dump_filter`

```bash
ob_admin clog_tool dump_filter '<filter_name>' <clog_file_name>
```

使用 `dump_filter` 命令过滤不需要的 clog 日志记录。

`<clog_file_name>` 详情，参考 `dump_all` 命令。

目前，`filter_name` 的取值如下：

| 过滤条件名称 | 说明 |
| --- | --- |
| `table_id` | 数据表标识符 |
| `partition_id` | 数据分区标识符 |

`filter_name` 可以单独使用，也可以组合使用。过滤多个条件时，条件名称需要用英文分号（;）隔开。例如：

`table_id` 为 123， clog 文件名为 `101` 且位于当前目录，则运行以下命令解析并打印该文件。

```bash
ob_admin clog_tool dump_filter 'table_id=123' 101
```

`table_id` 为 `123`，`partition_id` 为 `2`，需要解析的 clog 文件名为 `101` 且位于当前目录，则运行以下命令解析并打印该文件。

```bash
ob_admin clog_tool dump_filter 'table_id=123;partition_id=2' 101
```

### `stat_clog` 命令

```bash
ob_admin clog_tool stat_clog <clog_file_name>
```

使用 `stat_clog` 命令获取指定 clog 文件的统计信息，包括日志文件大小、日志记录个数、事务个数等信息。

<!-- ## 事务的相关信息

### 事务日志的类型

根据事务日志产生的阶段，可以将事务日志分为事务执行时日志和事务提交时日志。

#### 事务执行时日志

在事务执行阶段，对事务的修改进行持久化时产生日志记录即为事务执行时日志。事务执行时日志记录的类型如下所示：

| 日志类型 | 内部类型值 | 说明 |
| --- | --- | --- |
| OB_LOG_MUTATOR | 0x200 | 事务的 mutator 日志，包含事务的修改内容 |
| OB_LOG_TRANS_STATE | 0x400 | 事务的状态日志 |
| OB_LOG_MUTATOR_WITH_STATE | 0x600 | 事务的状态日志，携带 mutator 日志 |
| OB_LOG_MUTATOR_ABORT | 0x800 | 事务的 abort 日志，用于标记该事务已回滚 |

#### 事务提交时日志

在事务提交阶段，对事务的修改进行持久化时产生的日志即为事务提交时日志。根据不同的事务类型，事务提交时日志可以分为分布式事务的提交时日志和本地单分区事务的提交时日志。

分布式事务的提交时日志的类型如下所示：

| 日志类型 | 内部类型值 | 说明 |
| --- | --- | --- |
| OB_LOG_TRANS_REDO | 0x1 | 分布式事务的 redo 日志，包含事务的修改内容（redo 日志） |
| OB_LOG_TRANS_PREPARE | 0x2 | 分布式事务的 prepare 日志 |
| OB_LOG_TRANS_REDO_WITH_PREPARE | 0x3 | 分布式事务的 prepare 日志，包含事务的修改内容（redo 日志） |
| OB_LOG_TRANS_COMMIT | 0x4 | 分布式事务的 commit 日志，标记该事务已提交 |
| OB_LOG_TRANS_ABORT | 0x8 | 分布式事务的 abort 日志，标记该事务已回滚 |
| OB_LOG_TRANS_CLEAR | 0x10 | 分布式事务的 clear 日志，用于释放事务上下文信息 |

本地单分区事务的提交时日志的类型如下所示：

| 日志类型 | 内部类型值 | 说明 |
| --- | --- | --- |
| OB_LOG_SP_TRANS_REDO | 0x20 | 单分区事务的 redo 日志，包含事务的修改内容（redo 日志） |
| OB_LOG_SP_TRANS_COMMIT | 0x40 | 单分区事务的 commit 日志，标记该事务已提交，包含事务的修改内容（redo 日志） |
| OB_LOG_SP_TRANS_ABORT | 0x80 | 单分区事务的 abort 日志，标记该事务已回滚 |

### 事务标识符（Transaction ID）

事务标识符用于区分 OBServer 内部的不同事务。因此，事务标识符需要具有**唯一性**，即不同事务具有不同的事务标识符。

为了实现事务标识符的唯一性，事务标识符的结构如下所示：

```bash
class ObTransID
{
  uint64_t hv_;
  common::ObAddr server_;
  int64_t inc_;
  int64_t timestamp_;
}
```

各字段说明如下所示：

| 字段名 | 说明 |
| --- | --- |
| server_ | 维护该事务标识符的 OBServer 的物理地址（包括 IP 地址和端口号） |
| inc_ | OBServer 维护的单调递增值，在 OBServer 重启后重新计数 |
| timestamp_ | 创建该事务标志符时的本地物理时钟值（Unix 时间戳） |
| hv_ | 根据以上三个字段计算得到的 Hash 值 |

由于 OceanBase 数据库采用了 64 位 Hash 值，两个不同事务标识符的 Hash 值产生冲突的概率极低。因此，在大多数情况下，可以使用 Hash 值来区分不同事务。

### 事务类型

根据事务的操作类型以及访问数据的分布，可以将事务分为本地单分区事务与分布式事务。

- 本地单分区事务：事务信息的维护与该事务的访问数据位于同一个节点，且该事务仅访问同一个数据分区内的数据。
- 分布式事务：事务信息的维护与该事务的访问数据位于不同节点，或者事务访问多个数据分区。 -->
