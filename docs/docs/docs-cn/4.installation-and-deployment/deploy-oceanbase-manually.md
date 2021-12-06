# 部署 OceanBase 数据库

您可以可用 OBD 部署 OceanBase 数据库，也可以不借助任何工具部署 OceanBase 数据库。本文介绍如何在不借助任何工具的情况下部署 OceanBase 数据库。本文提供的部署方式将省略操作系统内核参数和数据库内核参数的配置，因此**不适用**于性能测试。

## 环境介绍

本文使用 16 核 64 G 阿里云 ECS，并配置了独立的磁盘（/dev/sdb），磁盘空间为 256 G。

## 配置操作系统

依次执行以下命令，将物理盘格式化为文件系统 `/data`。

```bash
fdisk /dev/vdb
mkfs -t ext4 /dev/vdb1
mkdir -p /data
mount -t ext4 /dev/vdb1 /data
```

## 下载 OceanBase 数据库

执行以下命令，下载 OceanBase 数据库：

```bash
yum install -y yum-utils
yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo
# 创建一个目录用于存放安装包
mkdir rpm
cat > rpm_list <<EOF
oceanbase-ce-3.1.0-1.el7.x86_64.rpm
oceanbase-ce-libs-3.1.0-1.el7.x86_64.rpm
obproxy-3.1.0-1.el7.x86_64.rpm
EOF
wget -B https://mirrors.aliyun.com/oceanbase/community/stable/el/7/x86_64/ -i rpm_list -P rpm
```

## 安装 OceanBase 数据库

OceanBase 数据库默认安装在 root 用户下。为确保数据安全，建议您将 OceanBase 数据库安装在普通用户下。此处 `admin` 用户为例，将 OceanBase 数据库运行在 `admin` 用户下。

```bash
useradd admin
rpm -ivh rpm/*
```

执行以下命令，依次查询 RPM 安装包的安装目录：

```bash
rpm -ql oceanbase-ce-3.1.0-1.el7.x86_64rpm -ivh rpm/*
rpm -ql obproxy-3.1.0-1.el7
```

## 启动 OceanBase 进程

依次执行以下命令，使用 `admin` 用户，在 `oceanbase` 目录启动 OceanBase 进程：

```bash
su - admin
mkdir -p /data/observer01/store/{sort_dir,sstable,clog,ilog,slog}
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/admin/oceanbase/lib/
cd /data/observer01 && /home/admin/oceanbase/bin/observer -r 172.20.249.39:2882:2881 -o __min_full_resource_pool_memory=268435456,memory_limit=8G,system_memory=4G,stack_size=512K,cpu_count=16,cache_wash_threshold=1G,workers_per_cpu_quota=10,schema_history_expire_time=1d,net_thread_count=4,sys_bkgd_migration_retry_num=3,minor_freeze_times=10,enable_separate_sys_clog=0,enable_merge_by_turn=False,datafile_size=50G,enable_syslog_recycle=True,max_syslog_file_count=10 -z zone1 -p 2881 -P 2882 -c 1 -d /data/observer01/store -i eth0 -l INFO
```

> **注意：** 您必须使用您真实的 IP、启动目录和网卡名称。示例中的启动目录为 `/data/observer01`，运行日志将存储在这个目录。

各选项详情见下表：

选项名称 | 描述
---- | -----
-d | 数据文件和日志文件的目录。这个目录下必须存在几个子目录。重装 OceanBase 数据库后，此目录必须清空，然后重新创建。
-r | 节点信息。
-o | OceanBase 数据库的启动参数。例如，上述命令中，`memory_limit=8G`，表示启动一个 8 G 的进程。您可以可以您机器的实际内存调整 `memory_limit`。
-i | 网卡名称，必须与 -r 后的 IP 对应。

执行以下命令，查看 OceanBase 数据库是否启动成功：

```bash
ps -ef|grep observer
```

## BootStrap 集群

OceanBase 数据库进程首次启动后，您可以使用以下方式连接 OceanBase 数据库：

- 使用 MySQL 客户端（5.5 版本及以上）连接
- 使用 OBClient 客户端（需要另外安装）连接
  
    ```bash
    # 安装 OBClient
    yum -y install obclient
    ```

按照以下步骤连接 OceanBase 数据库，并 BootStrap 集群：

```bash
# 连接 OceanBase 数据库
mysql -h127.1 -uroot -P2881
```

```sql
-- BootStrap 集群
oceanbase> SET SESSION ob_query_timeout=1000000000;
oceanbase> ALTER SYSTEM bootstrap ZONE 'zone1' SERVER '172.20.249.39:2882';
```

看到 oceanbase 这个数据库，那就是真正成功了。
> **注意：** 当内存不足、目录权限不对、磁盘空间不足、多节点时间不同步（超过 5ms）、多节点网络延时超过 50 ms时，可能会导致 BootStrap 集群失败。您必须杀掉 OceanBase 数据库的进程，清空数据文件，重新启动 OceanBase 数据库，重新 BootStrap 集群。

## 后续操作

要使用 OceanBase 数据库，您必须创建普通租户。更多信息，参考 [创建租户]()。