# 生产环境推荐配置

本文介绍 OceanBase 数据库在生产环境中的推荐配置。

## 磁盘（必须遵守）

OceanBase 数据库的运行日志、事务日志和数据文件必须分开存储。**日志盘和数据盘建议使用 SSD。不推荐使用 sata 盘。**推荐配置如下：
![image.png](https://intranetproxy.alipay.com/skylark/lark/0/2021/png/3591/1626955341755-2d682593-dc82-4942-a06d-3198ed3d70f6.png#clientId=uf2227f2d-33a9-4&from=paste&height=144&id=u4182a9b9&margin=%5Bobject%20Object%5D&name=image.png&originHeight=288&originWidth=1466&originalType=binary&ratio=1&size=108494&status=done&style=none&taskId=ub54fe223-05cf-4644-a722-aa372119cf1&width=733)
如果您没有 3 块磁盘，建议您使用 LVM 将一块盘分为 3 个分区。

## 时钟同步（必须遵守）

OceanBase 集群中各服务器的时间必须保持一致，否则 OceanBase 集群将无法启动。时钟同步在生产环境中**非常非常重要**，物理机与时钟服务器的误差在 100 ms 以内可认为时钟是同步状态。OceanBase 数据库最大容忍误差是 200 ms。当误差超过 200 ms 时，会出现无主状况，恢复时钟同步后，重启 OceanBase 服务即可恢复正常。
使用以下命令，查看各服务器间的时钟是否同步：

```bash
sudo clockdiff  $IP
```

更多信息，参考 [配置时钟源](6.optional-configuring-clock-sources.md)。

## 网络时延（必须遵守）

服务器间的网络时延不能超过 200 ms，否则数据同步会严重滞后，并且可能影响选举。涉及跨城部署时，您需要评估好服务器间的网络时延，跨城网络时延建议不超过 30ms。

## 网卡设置

建议配置 2 块万兆网卡，bond 模式可以取名为 bond0、mode1 或 mode4。推荐使用 mode4。如果 bond 模式是 mode4，则交换机需要配置 802.3ad。网卡名建议使用 eth0 或 eth1。建议使用 network 服务，不要使用 NetworkManager。

## 参数设置

- 当系统的写入 TPS 过高时，止转储速度交低于写入速度，此时 memstore 内存会爆，执行以下命令开启写入限速：

   ```sql
   ALTER SYSTEM SET writing_throttling_trigger_percentage=75 tenant=all(或者具体 tenantname);
   ```

- 内存设置
  - 租户的 CPU 和内存规格比例建议不低于 1:4，否则会导致租户内存满。
  - 普通租户的最小内存必须大于 5G。
  - 租户内存太小时建议调大 `ob_sql_work_area_percentage` 变量。`ob_sql_work_area_percentage` 默认值为 5%。租户内存小于 10G 时建议将 `ob_sql_work_area_percentage` 设置为 20% 左右。
  - 分区数量受内存限制，每个副本预留内存为 168 KB，因此 10000 个副本至少需要预留 1.68 G 内存，即 1 G 的租户最多能建约六千个分区。建议您根据分区数设置租户内存。
  - 物理内存使用限制 `memstore_limit_percentage` 参数的默认值为 50。如果您的服务器内存大于 256 G，建议您将 `memstore_limit_percentage` 参数设置为 80。如果您的服务器内存小于 256 G，建议您采用默认配置。执行以下命令，设置 `memstore_limit_percentage` 参数：

      ```sql
      ALTER SYSTEM SET memstore_limit_percentage = '80';
      ```

  - `memory_limit_percentage` 的默认值为 80，如果您的服务器内存大于 256 G，建议您将 `memory_limit_percentage` 参数设置为 90。执行以下命令，设置 `memory_limit_percentage` 参数：
- 慢查询执行时间阈值 `trace_log_slow_query_watermark` 参数的默认值为 100 ms。您可以根据业务特点调整这一参数。如果阈值太小，OceanBase 将打印大量 trace 日志，进而会影响性能。慢查询时间的默认值为 5s。执行以下命令，进行设置：

   ```sql
   ALTER SYSTEM SET trace_log_slow_query_watermark = '1s';
   ```

- CPU 并发度调整

   ```sql
   -- CPU 并发度参数，建议配置为 4，arm 系统为 2
   ALTER SYSTEM SET cpu_quota_concurrency = '4';

   -- 资源软负载开关，控制资源均衡水位，默认为 50%，即 CPU 内存使用超过 50% 就进行 unit 均衡，线上建议调整为 100，达到手工控制 unit 分布的效果
   ALTER SYSTEM SET resource_soft_limit = '100';
   ```

- 转储合并相关

   ```sql
   -- 配置转储 50 次
   ALTER SYSTEM SET minor_freeze_times = 50;

   -- 转储触发水位百分比，建议 256 G 以上配置调整为 70，256 G 以下调整为 60
   ALTER SYSTEM SET freeze_trigger_percentage = '60';

   -- 数据拷贝并发为 100
   ALTER SYSTEM SET data_copy_concurrency = 100;
   -- 服务器上数据传出并发为 10
   ALTER SYSTEM SET server_data_copy_out_concurrency = 10;
   -- 服务器上数据传入并发为 10
   ALTER SYSTEM SET server_data_copy_in_concurrency = 10;

   -- 转储预热时间，默认 30s，设置了会延后转储释放的时间，改成 0s
   ALTER SYSTEM SET minor_warm_up_duration_time = '0s';
   -- 配置 chunk 内存大小(建议保持默认值 0，OceanBase 自行分配)
   ALTER SYSTEM SET memory_chunk_cache_size = 0;


   -- 最大包括版本数量，影响磁盘可用空间，默认为 2，将多保留一个版本的数据在数据盘中，需调整为 1
   ALTER SYSTEM SET max_kept_major_version_number = '1';
   ALTER SYSTEM SET max_stale_time_for_weak_consistency = '2h';
   ```

- 事务相关

   ```sql
   ALTER SYSTEM SET clog_sync_time_warn_threshold = '1s';
   ALTER SYSTEM SET trx_try_wait_lock_timeout = '0ms';（默认值为 0ms，无需修改）

   -- 建议关闭一阶段提交，该参数值默认是 false
   ALTER SYSTEM SET enable_one_phase_commit='False';
   ```

- 分区迁移速度控制，若是集群负载很低，可以通过加大并发任务数加快分区迁移速度，调大迁移并发数

   ```sql
   ALTER SYSTEM SET data_copy_concurrency=40;
   ALTER SYSTEM SET server_data_copy_out_concurrency=20;
   ALTER SYSTEM SET server_data_copy_in_concurrency=20;
   ```

- 压缩相关

   ```sql
   -- （默认就是 zstd_1.0，无需修改）, 不过系统支持多种压缩算法
   ALTER SYSTEM SET default_compress_func = 'zstd_1.0';
   ```

- 缓存刷新相关

   ```sql
   ALTER SYSTEM SET autoinc_cache_refresh_interval = '43200s';
   ```

- prepare statement 和 server 端 ps `受_ob_enable_prepared_statement` 参数控制，除了 JDBC 和 OBClient 的用户可以按照文档提供的说明使用 server 端 ps，其他情况不建议使用。

   ```sql
   -- Prepared Statement 参数，不用 java 建联的配置建议设为 0
   ALTER SYSTEM SET _ob_enable_prepared_statement = 0;
   ```

- 系统相关

   ```sql
   ALTER SYSTEM SET server_permanent_offline_time = '7200s';


   -- 公有云建议 5M，生产环境建议 5M，否则建议默认值 30M
   ALTER SYSTEM SET syslog_io_bandwidth_limit = '5M';
   ```

- 集群升级策略

版本升级时，机器临时上下线，可以先进行一次转储，以减少启动服务器的恢复时间。

- 批量导入大量数据最佳策略

   如果是多租户集群，某个租户需要大批量导入数据，为避免影响其他租户, 导完数据后可以恢复以上两个参数:

  - 将 `cpu_quota_concurrency` 设置为 1，防止租户间 CPU 抢占
  - 开启多轮转储减少合并触发，这样可以提高导入速度
  
- 租户 primary_zone 配置
  - primar_zone 配置到具体某个 zone 中。适用场景：业务使用单表、zone_name1 与应用在同机房、zone_name2 和 zone_name3 作为从副本平时无业务流量，具体 zone 顺序需按机房优先级、按应用和 OceanBase 的机房配置。

      ```sql
      ALTER TENANT SET PRIMARY_ZONE = 'zone_name1;zone_name2,zone_name3';
      ```

  - 打散  primary_zone 到所有全功能 zone 中。使用场景：业务使用分区表、集群中所有副本都在同一机房或不同 zone 机房间网络延迟在 1ms 内，需要将 primary_zone 打散到所有副本。
  
      ```sql
      ALTER TENANT SET PRIMARY_ZONE = 'zone_name1,zone_name2,zone_name3';
      ```

## 租户设置

- 并发度设置

   ```sql
   -- 最大并发度，默认 32，有大查询业务的建议调整为 128
   SET GLOBAL ob_max_parallel_degree = 128;


   /*
   parallel_max_servers 推荐设置为测试租户分配的 resource unit cpu 数的 10 倍
   如测试租户使用的 unit 配置为：create resource unit $unit_name max_cpu 26
   那么该值设置为 260
   parallel_server_target 推荐设置为 parallel_max_servers * 机器数*0.8
   那么该值为 260*3*0.8=624
   */
   SET GLOBAL parallel_max_servers=260;
   SET GLOBAL parallel_servers_target=624;
   ```

- 回收站设置

   ```sql
   -- 回收站参数，DDL 执行频率过大的场景一定要关闭，避免ddl执行过多引起租户性能异常
   SET GLOBAL recyclebin = 0;

   -- truncate回滚参数，truncate执行频率过大的场景一定要关闭
   SET GLOBAL ob_enable_truncate_flashback = 0;
   ```

- 客户端命令长度

OceanBase 客户端可发的命令长度受限于租户系统变量 `max_allowed_packet` 的限制（缺省4M)。可以酌情调大。

## ODP 配置

- ODP 探活

   ```sql
   ALTER proxyconfig SET sock_option_flag_out = 2;  --  2 代表 keepalive
   ALTER proxyconfig SET server_tcp_keepidle = 5;  --  启动 keepalive 探活前的空闲时间，5 秒
   ALTER proxyconfig SET server_tcp_keepintvl = 5;  -- 两个 keepalive 探活包之间的时间间隔，5 秒
   ALTER proxyconfig SET server_tcp_keepcnt = 2;  --  最多发送多少个 keepaliv e包，2 个。最长 5+5*2=15 秒发现 dead_socket
   ALTER proxyconfig SET server_tcp_user_timeout = 5;  --  等待 TCP 层 ACK 确认消息的超时时长，5 秒
   ```
