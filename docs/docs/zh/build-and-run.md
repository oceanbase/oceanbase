# 获取代码，编译运行

## 前置条件

检查支持的操作系统列表（[安装工具链](toolchain.md)）和GLIBC版本要求，以及如何安装C++工具链。

## Clone 代码

把代码clone到本地：

```shell
git clone https://github.com/oceanbase/oceanbase.git
```

## 构建

构建debug或release版本的OceanBase源码：

### Debug 模式

```shell
bash build.sh debug --init --make
```

### Release 模式

```shell
bash build.sh release --init --make
```

## 运行

`observer` 二进制文件已经编译出来了，可以使用 `obd.sh` 工具部署一个 OceanBase 实例：

```shell
./tools/deploy/obd.sh prepare -p /tmp/obtest
./tools/deploy/obd.sh deploy -c ./tools/deploy/single.yaml
```

OceanBase 服务程序会监听 10000 端口。

## 连接

可以使用官方的 MySQL 客户端连接 OceanBase：

```shell
mysql -uroot -h127.0.0.1 -P10000
```

也可以使用 `obclient` 连接 OceanBase：

```shell
./deps/3rd/u01/obclient/bin/obclient -h127.0.0.1 -P10000 -uroot -Doceanbase -A
```

## 停止

停止服务并清理部署：

```shell
./tools/deploy/obd.sh destroy --rm -n single
```
