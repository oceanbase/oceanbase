# 快速入门指南

本指南以单机安装 OceanBase 数据库为例指导您如何快速使用 OceanBase 数据库。

## 前提条件

在安装 OceanBase 数据库之前，确保您的机器可以连接公网。并且您的软硬件环境满足以下要求：

| 项目 | 描述 |
| ---- | --- |
| 系统     | Red Hat Enterprise Linux Server 7.2 版本（内核 Linux 3.10.0 版本及以上） <br /> CentOS Linux 7.2 版本（内核 Linux 3.10.0 版本及以上）    |
| 内存     | 至少 16G   <br /> 推荐 64G 及以上|
| 磁盘类型   | 推荐使用 SSD|
| 磁盘存储空间 | 至少 100G|
| 文件系统   | EXT4 戓 XFS|
| 网卡     | 千兆互联及以上|

 >**注意：** 以下内容以 x86 架构的 CentOS Linux 7.2 镜像作为环境，其他环境可能略有不同。

## 步骤 1：下载安装 OBD

获取 OceanBase 数据库最快的方式是使用数据库部署工具 OceanBase Deployer（简称 OBD），因此推荐您使用此方式体验 OceanBase 数据库。按照以下步骤下载并安装 OBD：

### 方案1：通过 YUM 软件源安装 OBD

如您的机器可以访问公网，并能够添加三方 YUM 软件源，您可以运行以下命令，使用 OceanBase 的官方软件源安装 OBD：

   ```bash
   sudo yum install -y yum-utils
   sudo yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo
   sudo yum install -y ob-deploy
   ```

### 方案2: 离线安装

1. 下载 OBD 离线 RPM 安装包。

2. 运行以下命令安装 OBD。

    ```bash
    yum install -y ob-deploy-1.0.0-1.el7.x86_64.rpm
    source /etc/profile.d/obd.sh
    ```

## （可选）步骤 2：下载 OceanBase 数据库

如果您的机器可以连接公网，则直接跳过此步骤。因为在您执行了 `obd cluster deploy` 命令时，OBD 将检查您的目标机器是否已存在 OceanBase 数据库软件包。如未找到，OBD 将自动从 YUM 源获取。

如果您的机器机器不能连接公网，按照以下步骤下载 OceanBase 数据库的离线软件包：

1. 运行以下命令，下载 OceanBase 数据库的离线软件包：

    ```bash
    # 创建一个目录用于下载
    mkdir rpm
    cat > rpm_list <<EOF
    oceanbase-ce-3.1.0-1.el7.x86_64.rpm
    oceanbase-ce-libs-3.1.0-1.el7.x86_64.rpm
    obproxy-3.1.0-1.el7.x86_64.rpm
    EOF

    wget -B https://mirrors.aliyun.com/oceanbase/community/stable/el/7/x86_64/ -i rpm_list -P rpm
    ```

    **注意：** 示例中的安装包可能不是最新版本，建议您下载最新的安装包。详细信息，参考 [OceanBase 数据库发行版本](https://github.com/oceanbase/oceanbase/releases)。

2. 运行以下命令，将软件包拷贝到您的中控机器：

    ```bash
    scp -r rpm <user>@<your-machine-ip>
    ```

3. 在中控机器运行以下命令，将 OceanBase 数据库的离线软件包加入本地镜像：

    ```bash
    cd ~/rpm
    obd mirror clone *.rpm
    ```

## 步骤 3：部署 OceanBase 数据库

本节介绍如何使用 OBD 部署 OceanBase 数据库。按照以下步骤部署 OceanBase 数据库：

1. 从 Gitee/GitHub 上下载对应的配置文件模板。

    - 如果您采用本地安装，即中控机器和目标机器是同一台机器，请下载 [本地安装配置文件](https://gitee.com/oceanbase/obdeploy/blob/master/example/mini-local-example.yaml)。
    - 如果您采用单机安装，即中控机器和目标机器不是同一台机器，且目标机器只有一台，请下载 [单机安装配置文件](https://gitee.com/oceanbase/obdeploy/blob/master/example/mini-single-example.yaml)。
    - 如果您采用分布式安装，即中控机器和目标机器不是同一台机器，且目标机器有多台，请下载 [分布式安装配置文件](https://gitee.com/oceanbase/obdeploy/blob/master/example/mini-distributed-example.yaml)。
    > **注意：** 此处以本地安装为例，介绍如何修改配置文件。您必须按照您的安装方式选择对应的配置文件模板。

2. 如您的中控机器和目标机器不是同一台，您还需要在配置文件顶部添加远程 SSH 访问的相关配置。

    ```yaml
    user:
      username: <您的账号名>
      password: <您的登录密码>
      key_file: <您的私钥路径>
    ```

    其中，`username` 为登录到目标机器的用户名，确保您的用户名有 `home_path` 权限。
    > **注意：** 通常情况下，您只能使用登录密码或者私钥登录中的一种方式登录目标机器，如果同时填写，`password` 将被认为是您私钥的密码。

3. （可选）修改配置文件中的 IP 地址。

    ```yaml
    oceanbase-ce:
      servers:
        - name: z1
        # Please don't use hostname, only IP can be supported
        ip: 11.166.80.01
    ```

4. 修改配置文件中的 `devname` 变量和 `home_path`。

    ```yaml
    oceanbase-ce:
      global:
        home_path: <your_observer_work_path>
        # Please set devname as the network adaptor's name whose ip is in the setting of severs.
        # if set severs as "127.0.0.1", please set devname as "lo"
        # if current ip is 192.168.1.10, and the ip's network adaptor'sname is "eth0", please use "eth0"
        devname: bond0
    ```

## 步骤 4：连接 OceanBase 数据库

按照以下步骤部署并启动 OceanBase 数据库实例：

1. 运行以下命令部署集群：

    ```bash
    obd cluster deploy <deploy_name> -c <deploy_config_path>
    ```

   其中，`deploy_name` 为集群名称。一个集群只能有一个名称，且集群名称不能重复。

2. 运行以下命令启动集群：

    ```bash
    obd cluster start <deploy_name>
    ```

3. 运行以下命令查看集群状态：

    ```bash
    obd cluster display <deploy_name>
    ```

## 步骤 5：连接 OceanBase 数据库

按照以下步骤连接 OceanBase 数据库：

1. 安装 OceanBase 数据库客户端 OBClient：

    如您的机器已添加 OceanBase 官方 YUM 源作为软件源，使用以下命令直接安装：

    ```bash
    sudo yum install -y obclient
    ```

    否则您需要在机器上准备好离线安装包，并执行以下命令安装：

    ```bash
    sudo yum install -y obclient-2.0.0-1.el7.x86_64.rpm 
    ```

    **注意：** 示例中的安装包可能不是最新版本，建议您下载最新的安装包。详细信息，参考 [OceanBase 官网下载中心](https://github.com/oceanbase/oceanbase/releases)。

2. 运行以下命令，使用 OBClient 客户端连接 OceanBase 数据库：

    ```bash
    obclient -h<your_ip> -P<observer_mysql_port> -uroot
    ```

    其中，`<your_ip>` 为您 OceanBase 实例所在的机器 IP 地址。`observer` 默认使用端口 `2883` 连接 OBClient。如果您对端口做了更改，此处使用您实际的端口号。

    返回以下信息：

    ```bash
    Welcome to the MariaDB monitor.  Commands end with ; or \g.
    Your MySQL connection id is 3221546072
    Server version: 5.7.25 OceanBase 3.1.0 (r1-) (Built Apr  7 2021 08:14:49)

    Copyright (c) 2000, 2018, Oracle, MariaDB Corporation Ab and others.

    Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

    MySQL [(none)]>
    ```

## （可选）步骤 6：创建租户

创建租户详细信息，参考 [创建用户租户](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.0/create-a-user-tenant)。
