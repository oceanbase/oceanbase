# OceanBase 安装路径

OceanBase 数据库默认安装在 `/home/admin/oceanbase` 下，主要由以下子目录构成：

```text
|-- admin
|-- bin
|   `-- observer -> /root/.obd/repository/oceanbase-ce/3.1.0/b73bcd531bdf3f087391991b290ff2cbcdaa0dc9/bin/observer
|-- etc
|   |-- observer.config.bin
|   `-- observer.config.bin.history
|-- lib
|   |-- libaio.so -> /root/.obd/repository/oceanbase-ce/3.1.0/b73bcd531bdf3f087391991b290ff2cbcdaa0dc9/lib/libaio.so
|   |-- libaio.so.1 -> /root/.obd/repository/oceanbase-ce/3.1.0/b73bcd531bdf3f087391991b290ff2cbcdaa0dc9/lib/libaio.so.1
|   |-- libaio.so.1.0.1 -> /root/.obd/repository/oceanbase-ce/3.1.0/b73bcd531bdf3f087391991b290ff2cbcdaa0dc9/lib/libaio.so.1.0.1
|   |-- libmariadb.so -> /root/.obd/repository/oceanbase-ce/3.1.0/b73bcd531bdf3f087391991b290ff2cbcdaa0dc9/lib/libmariadb.so
|   `-- libmariadb.so.3 -> /root/.obd/repository/oceanbase-ce/3.1.0/b73bcd531bdf3f087391991b290ff2cbcdaa0dc9/lib/libmariadb.so.3
|-- log
|   |-- election.log
|   |-- election.log.wf
|   |-- observer.log
|   |-- observer.log.wf
|   |-- rootservice.log
|   |-- rootservice.log.wf
|-- run
|   |-- mysql.sock
|   `-- observer.pid
`-- store
    |-- clog
    |-- ilog
    |-- slog
    |-- sort_dir
    `-- sstable
```

`admin`
`bin` 目录存放安装文件。
`etc`
`lib` 目录存放依赖。
`log` 目录存放运行日志，包括选举模块日志、启动和运行日志和 RootService 日志。
`store` 目录存放事务日志。`clog` 为提交日志（Commit Log），记录事务和 PartitionService 提供的原始日志内容，所有分区共用。`ilog` （Index Log）为索引日志，记录分区内部 log_id > clog（file_id 和 offset）的索引信息，所有分区共用。`slog` （Storage Log）为存储日志，记录 SSTable 操作的信息。
