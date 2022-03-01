# 常见问题

本文列出了在使用 ob_admin 时可能遇到的常见问题。您可以使用文档中心或者浏览器的搜索功能查找相应问题。如果按照本文的建议无法解决问题，请提交 GitHub issue。

## 关于 clog_tool

### clog 磁盘空间不足

当部分分区转储卡住时，clog 磁盘空间将不足。按照以下方式解决此问题：

- 调整 `clog_disk_usage_limit_percentage` 参数

    ```sql
    ALTER SYSTEM SET clog_disk_usage_limit_percentage = 98 server ='xxx:2882';
    ```

    参数调整完成后，落后的副本会立即触发追日志，如果落后很多会触发 rebuild（从 leader 复制 data+clog），通过如下方式观察恢复过程：

    ```sql
    -- 检查 clog 不同步的分区数是否有减少
    SELECT svr_ip, count(*) FROM __all_virtual_clog_stat WHERE is_offline = 0 AND is_in_sync = 0 GROUP BY 1;
    -- 如果没有快速减少，查询是否有正在做 rebuild 的副本
    SELECT svr_ip, count(*) FROM __all_virtual_partition_migration_status  WHERE ACTION != 'END' GROUP BY 1;
    -- 若上述查询结果非 0，则继续检查 rebuild 任务并发相关的配置项
    SHOW PARAMETERS LIKE "%data_copy%";
    -- 如果 server_data_copy_in_concurrency、server_data_copy_out_concurrency 默认值均为 2，将二者均调整为 10，以加快多个副本 rebuild 的并发
    ALTER SYSTEM SET server_data_copy_in_concurrency = 10;
    ALTER SYSTEM SET server_data_copy_in_concurrency = 10;
    ```

- 如果调整 `clog_disk_usage_limit_percentage` 参数后，clog 盘未恢复正常。按照以下方式进行操作：

  - 将 clog 文件移动至其他磁盘
    从 clog 目录下文件名字最小的文件开始，连续移动一部分文件（移动文件数 = clog 盘容量*10%/64M）到其他磁盘，以确保 clog 盘的水位降到 95% 以下。在文件能够正常回收之前，不能重启 observer 进程，否则启动会失败。观察 clog 盘是否能自动开始回收。注意此处是移动，不是删除，被移动的日志后续可能需要移动回来。
  - 如果 clog 盘空间使用率缓慢下降，并回落到 80%，则说明 observer 恢复顺利，继续等待至所有副本达到 sync 状态即可。建议您对集群进行转储，避免被移走的 clog 还被依赖。如果 clog 盘中的 clog 文件持续被回收中（在有数据持续写入的场景下），则说明之前移走的 clog 文件已不再需要。
- 当 `is_in_sync= 0` 的副本数量降为 0 之后，将 `clog_disk_usage_limit_percentage` 参数调整为默认值。

### flush_pos_ 大于 clog 最后一个文件的有效位置

报错信息如下：

```bash
[2021-05-31 15:05:40.789026] ERROR [CLOG] load_file (ob_clog_file_writer.cpp:155) [3133][0][Y0-0000000000000000] [lt=7] [dc=0] The clog start pos is unexpected, (ret=-4016, file_id=1018, offset=51658630, shm_buf_->file_flush_pos_={file_id:1018, file_offset:51663293}, shm_buf_->file_write_pos_={file_id:1018, file_offset:51667058}, shm_buf_->log_dir_="/home/admin/oceanbase/store/ob2r2uojeodxj4/clog_shm") BACKTRACE:0xcc279ca 0xcb46db2 0x7372324 0x7372bf7 0x73671a1 0x7363164 0x7518eaa 0x72f8e62 0x72f8fd1 0x72f9057 0x725489e 0x8050c4e 0x93aec0e 0x31a9088 0x7f886a0b0b15 0x3196029
```

删除 store 目录下的 clog_shm 文件，然后重新启动即可恢复正常。

### 迭代 clog 文件出错

迭代 clog 文件出错通常是由于 clog 文件本身出了问题。报错信息如下：

```bash
[2021-08-02 16:56:42.340821] ERROR [CLOG] notify_scan_finished_ (ob_log_scan_runnable.cpp:660) [3710][1861][Y0-0000000000000000] [lt=6] [dc=0] invalid scan_confirmed_log_cnt(ret=-4016, ret="OB_ERR_UNEXPECTED", scan_confirmed_log_cnt=2, next_ilog_id=22, last_replay_log_id=16, pkey={tid:1099511627971, partition_id:14, part_cnt:0}) BACKTRACE:0x771c1ba 0x7653a12 0x1a51854 0x1a51f65 0x5acec7c 0x5aca534 0x5acda8a 0x2f426ea 0x1e90dec 0x74ff497 0x74faf5f 0x74f366f
[2021-08-02 16:56:42.340958] INFO  [CLOG] ob_log_scan_runnable.cpp:682 [3710][1861][Y0-0000000000000000] [lt=133] [dc=0] notify_scan_finished_ finished(ret=-4016, cost_time=154)
[2021-08-02 16:56:42.340965] ERROR [CLOG] do_scan_log_ (ob_log_scan_runnable.cpp:216) [3710][1861][Y0-0000000000000000] [lt=6] [dc=0] notify_scan_finished_ failed(ret=-4016) BACKTRACE:0x771c1ba 0x7653a12 0x1a00088 0x1a006d2 0x1a02aca 0x5acdc97 0x2f426ea 0x1e90dec 0x74ff497 0x74faf5f 0x74f366f
[2021-08-02 16:56:42.341025] ERROR [CLOG] do_scan_log_ (ob_log_scan_runnable.cpp:223) [3710][1861][Y0-0000000000000000] [lt=59] [dc=0] log scan runnable exit error(ret=-4016) BACKTRACE:0x771c1ba 0x7653a12 0x1a00088 0x1a006d2 0x1a02aca 0x5acdbf4 0x2f426ea 0x1e90dec 0x74ff497 0x74faf5f 0x74f366f
```

如果是少数副本的迭代 clog 文件出错，您可以通过删除出错副本进行恢复。如果多数副本的迭代 clog 文件出错，建议您联系专业人员解决。按照以下步骤解决此问题：

1. 下线故障机器。

    ```sql
    ALTER SYSTEM DELETE SERVER '$obs0_ip:$obs0_port';
    ```

2. 清空故障机器上的数据。
3. 向集群中加入新的机器。

    ```sql
    ALTER SYSTEM ADD SERVER '$obs0_ip:$obs0_port';
    ```

    > 说明：向集群中添加新机器后，集群会自动补齐副本。您需要确认 `enable_replication` 的值为 `ON`。
