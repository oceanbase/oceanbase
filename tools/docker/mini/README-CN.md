# 如何使用这个镜像

部署 OceanBase 数据库的方式有很多，使用 Docker 是其中较方便的一种。本仓库提供了 OceanBase 数据库的 Docker 镜像 obce-mini。obce-mini 是 OceanBase 数据库社区版的小规格单机体验镜像，仅供研究、学习和评估使用，不适用于生产环境或性能测试场景。

## 前提条件

在部署 obce-mini 镜像之前，您需要确认以下信息：

- 确保您的机器至少提供 2 核 10GB 以上的资源。
- 您的机器已经安装以下程序：

    应用程序 | 推荐版本 | 参考文档
    ---     | ------  | -----
    Docker | 最新版 | [Docker 文档](https://docs.docker.com/get-docker/)
- 您的机器已经启动 Docker 服务。

## 启动 OceanBase 实例

运行以下命令，启动 OceanBase 的实例：

```bash
docker run -p 2881:2881 --name some-obce -d oceanbase/obce-mini
```

启动预计需要 2-5 分钟。执行以下命令，如果返回 `boot success!`，则启动成功。

```bash
$ docker logs some-obce | tail -1
boot success!
```

## 连接 OceanBase 实例

obce-mini 镜像安装了 OceanBase 数据库客户端 obclient，并提供了默认连接脚本 ob-mysql。

```bash
docker exec -it some-obce ob-mysql sys # 连接 sys 租户
docker exec -it some-obce ob-mysql root # 连接用户租户的 root 账户
docker exec -it some-obce ob-mysql test # 连接用户租户的 test 账户
```

您也可以运行以下命令，使用您本机的 obclient 或者 MySQL 客户端连接实例。

```bash
$mysql -uroot -h127.1 -P2881
```

连接成功后，终端将显示如下内容：

```mysql
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 167310
Server version: 5.7.25 OceanBase 3.1.0 (r-00672c3c730c3df6eef3b359eae548d8c2db5ea2) (Built Jun 22 2021 12:46:28)

Copyright (c) 2000, 2021, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

## 支持的环境变量

下表列出了当前版本的 obce-mini 镜像支持的环境变量：

变量名称 | 默认值 | 描述
------- | ----- | ---
OB_HOME_PATH | /root/ob | OceanBase 数据库实例的部署路径。
OB_MYSQL_PORT | 2881 | OceanBase 数据库实例的 MySQL 协议端口。
OB_RPC_PORT | 2882 | OceanBase 数据库实例的 RPC 通信端口。
OB_ROOT_PASSWORD | empty |  OceanBase 数据库实例 sys 租户的密码。
OB_CLUSTER_NAME | mini-ce | OceanBase 数据库实例名称，OBD 将使用这个名称作为集群名。
OB_TENANT_NAME | test | OceanBase 数据库实例默认初始化的用户租户的名称。

## 运行 Sysbench 脚本

obce-mini 镜像默认安装了 Sysbench 工具，并进行了简单配置。您可以依次执行以下命令，使用默认配置运行 Sysbench 脚本。

```bash
docker exec -it some-obce sysbench cleanup # 清理数据
docker exec -it some-obce sysbench prepare # 准备数据
docker exec -it some-obce sysbench run # 进行测试

docker exec -it some-obce sysbench # 依次执行上述三条命令
```

