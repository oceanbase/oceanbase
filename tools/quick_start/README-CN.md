快速上手

> 本文将会介绍如何在本地部署一本地单机OceanBase实例。
> 注意：这里的介绍不适用于生产环境部署，如果想要在生产环境部署OceanBase，可以参考[OceanBase 社区版网站](open.oceanbase.com)中的文档。

这里介绍的操作步骤适用于使用yum管理安装包的系统，比如CentOS。

**资源要求**

项目 | 描述
-- | --
CPU | 2 核以上，建议8核
内存 | 8G 以上

您需要确保能访问互联网。

> 以下操作步骤以CentOS 7.9 为例

**在 $HOME/oceanbase 目录安装 oceanbase**

```bash
# 1. 安装 obd
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo
sudo yum install -y ob-deploy

# 下载配置文件
wget https://gitee.com/oceanbase/oceanbase/raw/master/tools/quick_start/quick_start_demo.yaml -O quick_start.yaml

# 修改配置文件中的 home_path
# 这里将配置文件设置在$HOME/oceanbase下
sed -i "s?home_path:?home_path: $HOME/oceanbase?g" quick_start.yaml

# 使用obd 部署并启动 oceanbase，并创建一个test租户
# 部署会比较慢，需要等待2-10分钟
obd cluster autodeploy obcluster -c quick_start.yaml -A

obd cluster display obcluster
```

**使用客户端连接 oceanbase**

安装后，可以使用mysql客户端或obclient连接oceanbase

```bash
mysql -uroot@test -P 2881 -h 127.0.0.1
```
或
```bash
obclient -uroot@test -P 2881 -h 127.0.0.1
```

如果您没有mysql客户端或obclient，可以使用下面的命令安装：

```bash
# 安装 mysql 客户端
sudo yum install -y mysql

# 安装 obclient
sudo yum install -y obclient
```
