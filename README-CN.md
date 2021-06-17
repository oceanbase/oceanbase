# 什么是 OceanBase 数据库

OceanBase 数据库是一个原生的分布式关系数据库，它是完全由阿里巴巴和蚂蚁集团自主研发的项目。OceanBase 数据库构建在通用服务器集群上，基于 Paxos 协议和分布式架构，提供金融级高可用和线性伸缩能力，不依赖特定硬件架构，具备高可用、线性扩展、高性能、低成本等核心技术优势。

OceanBase 数据库具有如下特点：

- 高可用
    单服务器故障能够自愈，支持跨城多机房容灾，数据零丢失，可满足金融行业 6 级容灾标准（RPO=0，RTO<=30 秒）。
- 线性扩展
    透明扩展，自动负载均衡，应用透明的水平扩展，集群规模可超过 1500 节点，数据量可达 PB 级，单表记录万亿行。
- MySQL 高度兼容
    兼容 MySQL 协议、语法和使用习惯，MySQL 客户端工具可以直接访问 OceanBase 数据库。
- 高性能
    准内存级数据变更操作、独创的编码压缩技术，结合线性水平扩展，TPC-C 测试达到 7.07 亿 tpmC。
- 低成本
    使用 PC 服务器和低端 SSD，高存储压缩率降低存储成本，高性能降低计算成本，多租户混部充分利用系统资源。
- 多租户
    原生支持多租户构架，同一套数据库集群可以为多个独立业务提供服务，租户间数据隔离，降低部署和运维成本。

OceanBase 数据库支持支付宝的全部核心业务，以及银行、保险、证券、运营商等多个行业的数百个客户的核心业务系统。

## 快速上手

请查看 [快速使用指南](https://open.oceanbase.com/quickStart) 开始试用 OceanBase 数据库。

## 文档

- [简体中文](https://open.oceanbase.com/docs)
- 英文（English）（Coming soon）

## 支持的客户端

- [OBClient](https://github.com/oceanbase/obclient)

## 许可证

OceanBase 数据库使用 [MulanPubL - 2.0](https://license.coscl.org.cn/MulanPubL-2.0/index.html) 许可证。您可以免费复制及使用源代码。当您修改或分发源代码时，请遵守木兰协议。

## 兼容性列表

| 操作系统 | 版本 | 架构 | 编译 | 包部署 | 编译部署 | mysqltest |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| CentOS | 7.2,8.3 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| Debian | 9.8,10.9 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| Fedora | 33 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| MacOS | any | x86_64 | ❌ | ❌ | ❌ | ❌ |
| openSUSE | 15.2 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| OpenAnolis | 8.2 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| SUSE | 15.2 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| Ubuntu | 16.04,18.04,20.04 | x86_64 | ✅ | ✅ | ✅ | ✅ |

## 如何构建

### 前置准备

在构建前您需要确认您的机器已经安装必要的软件

#### Fedora 系列 （包括 CentOS，Fedora，OpenAnolis，RedHat 等）
```sh
yum install git wget rpm* cpio make glibc-devel glibc-headers binutils
```

#### Debian 系列 （包括 Debian，ubuntu 等）
```sh
apt-get install git wget rpm rpm2cpio cpio make build-essential binutils
```

#### SUSE 系列 （包括 SUSE，openSUSE 等）
```sh
zypper install git wget rpm cpio make glibc-devel binutils
```

### debug 模式
```bash
bash build.sh debug --init --make
```

### release 模式
```bash
bash build.sh release --init --make
```

### 构建 rpm 软件包
```bash
bash build.sh rpm --init && cd build_rpm && make -j16 rpm
```

详细信息，参考 [使用源码构建 OceanBase 数据库](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.0/get-the-oceanbase-database-by-using-source-code)。

## 如何贡献

我们十分欢迎并感谢您为我们贡献。以下是您参与贡献的几种方式：

- 向我们提 [issue](https://github.com/oceanbase/oceanbase/issues)。
- 提交 PR，详情参见 [如何贡献](CONTRIBUTING.md)。

## 获取帮助

如果您在使用 OceanBase 数据库时遇到任何问题，欢迎通过以下方式寻求帮助：

- [GitHub Issue](https://github.com/oceanbase/oceanbase/issues)
- [官方网站](https://open.oceanbase.com/)
- 知识问答（Coming soon）
