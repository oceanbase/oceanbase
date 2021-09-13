# OceanBase 安装路径

OceanBase 数据库默认安装在 `/home/admin/oceanbase` 下，主要由以下子目录构成：

```text
|-- bin
|-- etc
|   |-- observer.config.bin
|   `-- observer.config.bin.history
|-- lib
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

`bin` 目录存放二进制文件。
`etc` 目录存放配置文件。
`lib` 目录存放依赖。
`log` 目录存放运行日志，包括选举模块日志、启动和运行日志和 RootService 日志。
`run` 目录存放运行文件。
`store` 目录存放数据文件。`clog` 为提交日志（Commit Log），记录事务和 PartitionService 提供的原始日志内容，所有分区共用。`ilog` （Index Log）为存储日志目录，记录分区内部 log_id > clog（file_id 和 offset）的索引信息，所有分区共用。`slog` （Storage Log）为存储日志，记录 SSTable 操作的信息。
