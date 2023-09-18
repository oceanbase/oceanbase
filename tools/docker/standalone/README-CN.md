# 如何使用Docker部署OceanBase

OceanBase 提供了一个独立部署的测试镜像[oceanbase-ce](https://hub.docker.com/r/oceanbase/oceanbase-ce)。默认情况下，这个镜像会部署一个MINI_MODE OceanBase 实例。

**注意**

- oceanbase-ce docker镜像仅能用做学习研究或测试使用；
- 如果想要在k8s中部署oceanbase，请使用[oceanbase-operator](https://github.com/oceanbase/ob-operator)；
- 千万不要使用此镜像用于带有重要数据的场景，比如生产环境。

理由：

1. 这个集群仅包含一个实例，所以没有容灾能力；
2. 因为oceanbase实例出现异常无法正常启动时，docker容器也无法启动，所以在异常时难以恢复，这意味着对应的容器(container)就没用了。如果没有使用docker volume 将数据目录挂载到其它地方，那么容器中的数据也丢失了；
3. observer进程退出的时候，对应的容器不会退出，所以k8s也没有机会去启动一个新的pod。

## 前置条件

在部署 oceanbase-ce 镜像之前，您需要确认以下信息：

- 确保您的机器至少提供 2 核 8GB 以上的资源。
- 您的机器已经安装以下程序：

    应用程序 | 推荐版本 | 参考文档
    ---     | ------  | -----
    Docker | 最新版 | [Docker 文档](https://docs.docker.com/get-docker/)
- 您的机器已经启动 Docker 服务。

## 启动 OceanBase 实例

运行以下命令，启动 OceanBase 的实例：

```bash
# 部署最小规格实例
docker run -p 2881:2881 --name oceanbase-ce -d oceanbase/oceanbase-ce

# 根据当前容器情况部署最小规格的实例
docker run -p 2881:2881 --name oceanbase-ce -e MODE=slim -e OB_MEMORY_LIMIT=5G -v {init_sql_folder_path}:/root/boot/init.d -d oceanbase/oceanbase-ce

# 根据当前容器情况部署最大规格的实例
docker run -p 2881:2881 --name oceanbase-ce -e MODE=normal -d oceanbase/oceanbase-ce
```

启动预计需要 2-5 分钟。执行以下命令，如果返回 `boot success!`，则启动成功。

```bash
$ docker logs oceanbase-ce | tail -1
boot success!
```

启动成功后，容器中会启动一个oceanbase进程实例，可以使用root用户进行连接，不需要使用密码。

**注意：** 如果observer进程出现异常退出，容器不会自动退出。

## 连接 OceanBase 实例

oceanbase-ce 镜像安装了 OceanBase 数据库客户端 obclient，并提供了默认连接脚本 `ob-mysql`。

```bash
docker exec -it oceanbase-ce ob-mysql sys # 连接 sys 租户
docker exec -it oceanbase-ce ob-mysql root # 连接用户租户的 root 账户
docker exec -it oceanbase-ce ob-mysql test # 连接用户租户的 test 账户
```

您也可以运行以下命令，使用您本机的 obclient 或者 MySQL 客户端连接实例。

```bash
mysql -uroot -h127.1 -P2881
```

## 支持的环境变量

下表列出了当前版本的 oceanbase-ce 镜像支持的环境变量。

| 变量名称 | 默认值 | 描述                                                  |
| ---------------- | ------------- | ------------------------------------------------------------ |
| MODE             | {mini, slim, normal}  | mini或者不赋值变量表示使用mini模式部署OceanBase数据库实例，仅用来研究学习使用。不适合用于生产或性能测试。slim适用于更小的自定义配置，移除obagent，支持自定义的初始化脚本在绑定目录/root/boot/init.d，如果不绑定该目录，docker不会执行该租户的初始化sql。|
| EXIT_WHILE_ERROR | true          | OceanBase 如果启动失败，是否退出容器。比如初次run镜像失败，或start容器失败，可以将此参数设置为false,那么OB启动失败，也可以进入容器，查看OceanBase的运行日志，然后进行排查。 |
| OB_CLUSTER_NAME  | obcluster  | oceanbase集群名 |
| OB_TENANT_NAME   | test       | oceanbase mysql租户名|
| OB_MEMORY_LIMIT  | 6G         | oceanbase启动memory_limit参数配置 |
| OB_DATAFILE_SIZE | 5G         | oceanbase启动datafile_size参数配置 |
| OB_LOG_DISK_SIZE | 5G         | oceanbase启动log_disk_size参数配置 |
| OB_ROOT_PASSWORD |            | oceanbase启动sys租户的root用户密码配置 |
| OB_SYSTEM_MEMORY | 1G         | oceanbase启动system_memory参数配置 |
| OB_TENANT_MINI_CPU      |            | oceanbase租户mini_cpu参数配置 |
| OB_TENANT_MEMORY_SIZE   |            | oceanbase租户memory_size参数配置 |
| OB_TENANT_LOG_DISK_SIZE |            | oceanbase租户log_disk_size参数配置 |

## 运行 Sysbench 脚本

oceanbase-ce 镜像默认安装了 Sysbench 工具，并进行了简单配置。您可以依次执行以下命令，使用默认配置运行 Sysbench 脚本。

```bash
docker exec -it oceanbase-ce obd test sysbench obcluster
```

## Mount Volumn
如果想要将容器中的数据持久化保存下来，通常的做法是在`run` docker镜像时，使用 `-v /host/path:/container/path` 的方式将数据保存在宿主机上。
oceanbase-ce镜像的数据库数据默认保存在/root/ob目录下。但是仅仅映射/root/ob目录，会导致新的镜像无法启动，因为oceanbase-ce镜像是使用[obd](https://github.com/oceanbase/obdeploy) 来管理集群的，新的镜像启动时，没有oceanbase的集群信息，所以需要同时挂载/root/ob和/root/.obd目录。

挂载目录运行示例：

```bash
docker run -d -p 2881:2881 -v $PWD/ob:/root/ob -v $PWD/obd:/root/.obd --name oceanbase-ce oceanbase/oceanbase-ce
```

注意需要按照实际情况调整自己的目录。

`oceanbase-ce` docker默认会将数据保存到 /root/ob 目录。必须同时绑定 /root/ob 和 /root/.obd 目录。如果仅仅绑定 /root/ob 目录的话，容器就没办法重启了，因为oceanbase-ce 是使用 [obd](https://github.com/oceanbase/obdeploy)来管理数据库集群的，而启动一个全新的docker容器时，里面没有任何数据库集群信息。

docker -v 参数的详细说明可以参考 [docker volumn](https://docs.docker.com/storage/volumes/)。
