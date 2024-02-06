<p align="center">
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="OceanBase Logo" src="images/logo.svg" width="50%" />
    </a>
</p>
<p align="center">
    <a href="https://github.com/oceanbase/oceanbase/blob/master/LICENSE">
        <img alt="license" src="https://img.shields.io/badge/license-MulanPubL--2.0-blue" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/releases/latest">
        <img alt="license" src="https://img.shields.io/badge/dynamic/json?color=blue&label=release&query=tag_name&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase%2Freleases%2Flatest" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="stars" src="https://img.shields.io/badge/dynamic/json?color=blue&label=stars&query=stargazers_count&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="forks" src="https://img.shields.io/badge/dynamic/json?color=blue&label=forks&query=forks&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase" />
    </a>
    <a href="https://en.oceanbase.com/docs/oceanbase-database">
        <img alt="English doc" src="https://img.shields.io/badge/docs-English-blue" />
    </a>
    <a href="https://www.oceanbase.com/docs/oceanbase-database-cn">
        <img alt="Chinese doc" src="https://img.shields.io/badge/文档-简体中文-blue" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/actions/workflows/compile.yml">
        <img alt="building status" src="https://img.shields.io/github/actions/workflow/status/oceanbase/oceanbase/compile.yml?branch=master" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/commits/master">
        <img alt="last commit" src="https://img.shields.io/github/last-commit/oceanbase/oceanbase/master" />
    </a>
    <a href="https://join.slack.com/t/oceanbase/shared_invite/zt-1e25oz3ol-lJ6YNqPHaKwY_mhhioyEuw">
        <img alt="Join Slack" src="https://img.shields.io/badge/slack-Join%20Oceanbase-brightgreen?logo=slack" />
    </a>
</p>

[English](README.md) | 中文版

**OceanBase Database** 是一个分布式关系型数据库。完全由蚂蚁集团自主研发。 OceanBase 基于 [Paxos](https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf) 协议以及分布式架构，实现了高可用和线性扩展。OceanBase 数据库运行在常见的服务器集群上，不依赖特殊的硬件架构。

# 关键特性

- **水平扩展**

    实现透明水平扩展，支持业务快速的扩容缩容，同时通过准内存处理架构实现高性能。支持集群节点超过数千个，单集群最大数据量超过 3PB，最大单表行数达万亿级。

- **极致性能**
    
    唯一一个刷新了 TPC-C 记录（7.07 亿 tmpC）和 TPC-H 记录（1526 万 QphH @30000GB）的分布式数据库。

- **实时分析**
    
    基于“同一份数据，同一个引擎”，同时支持在线实时交易及实时分析两种场景，“一份数据”的多个副本可以存储成多种形态，用于不同工作负载，从根本上保持数据一致性。

- **高可用**
    
    独创“三地五中心”容灾架构方案，建立金融行业无损容灾新标准。支持同城/异地容灾，可实现多地多活，满足金融行业 6 级容灾标准（RPO=0，RTO< 8s），数据零丢失。

- **MySQL 兼容**
     
    高度兼容 MySQL，覆盖绝大多数常见功能，支持过程语言、触发器等高级特性，提供自动迁移工具，支持迁移评估和反向同步以保障数据迁移安全，可支撑金融、政府、运营商等关键行业核心场景替代。

- **低成本**

    基于 LSM-Tree 的高压缩引擎，存储成本降低 70% - 90%；原生支持多租户架构，同集群可为多个独立业务提供服务，租户间数据隔离，降低部署和运维成本。

更多信息请参考 [OceanBase 产品](https://www.oceanbase.com/product/oceanbase)。

# 快速开始

## 🔥 使用 all-in-one

可以执行下面的命令快速部署一个 OceanBase 数据库实例。

**注意**: 只能在 Linux 平台上使用。

```shell
# 下载并安装 all-in-one （需要联网）
bash -c "$(curl -s https://obbusiness-private.oss-cn-shanghai.aliyuncs.com/download-center/opensource/oceanbase-all-in-one/installer.sh)"
source ~/.oceanbase-all-in-one/bin/env.sh

# 快速部署 OceanBase database
obd demo
```

## 🐳 使用 docker

1. 启动 OceanBase 数据库实例

    ```shell
    # 部署一个mini模式实例
    docker run -p 2881:2881 --name oceanbase-ce -e MINI_MODE=1 -d oceanbase/oceanbase-ce
    ```

2. 连接 OceanBase

    ```shell
    docker exec -it oceanbase-ce ob-mysql sys # 连接root用户sys租户
    ```

更多信息参考[快速体验 OceanBase 数据库](https://open.oceanbase.com/quickStart)。

## 👨‍💻 使用源码编译部署

参考 [OceanBase 开发者文档](https://github.com/oceanbase/oceanbase/wiki/Compile)了解如何编译和部署手动编译的observer。

# Roadmap

请参考 [Roadmap 2023](https://github.com/oceanbase/oceanbase/issues/1364) 了解OceanBase规划。 更多详细信息请参考 [OceanBase Roadmap](https://github.com/orgs/oceanbase/projects)。

# 案例

OceanBase 已服务超过 400 家来自不同行业的客户，包括金融服务、电信、零售、互联网等。

更详细的信息请参考[客户案例](https://www.oceanbase.com/customer/home)和[谁在使用 OceanBase](https://github.com/oceanbase/oceanbase/issues/1301)。

# 系统架构

[系统架构介绍](https://www.oceanbase.com/docs/common-oceanbase-database-cn-10000000001687855)

# 社区贡献

非常欢迎社区贡献。请阅读[开发指南](docs/README.md)。

# License

OceanBase 数据库根据 Mulan 公共许可证版本 2 获得许可。有关详细信息，请参阅 [LICENSE](LICENSE) 文件。

# 社区

有以下加入社区的方法：

* [中文论坛](https://ask.oceanbase.com/)
* [Slack Workspace](https://join.slack.com/t/oceanbase/shared_invite/zt-1e25oz3ol-lJ6YNqPHaKwY_mhhioyEuw)
* 钉钉群: 33254054 ([二维码](images/dingtalk.svg))
* 微信群 (添加微信小助手: OBCE666)
