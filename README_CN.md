<p align="center">
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="OceanBase Logo" src="images/logo.svg" width="50%" />
    </a>
</p>
<p align="center">
    <a href="https://www.oceanbase.com/docs/oceanbase-database-cn">
        <img alt="Chinese doc" src="https://img.shields.io/badge/文档-简体中文-blue" />
    </a>
    <a href="https://en.oceanbase.com/docs/oceanbase-database">
        <img alt="English doc" src="https://img.shields.io/badge/docs-English-blue" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/commits/master">
        <img alt="last commit" src="https://img.shields.io/github/last-commit/oceanbase/oceanbase/master" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="stars" src="https://img.shields.io/badge/dynamic/json?color=blue&label=stars&query=stargazers_count&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/actions/workflows/compile.yml">
        <img alt="building status" src="https://img.shields.io/github/actions/workflow/status/oceanbase/oceanbase/compile.yml?branch=master" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/blob/master/LICENSE">
        <img alt="license" src="https://img.shields.io/badge/license-MulanPubL--2.0-blue" />
    </a>
</p>

<p align="center">
    <a href="https://ask.oceanbase.com">
        <img alt="中文论坛" src="https://img.shields.io/badge/Support-Join%20OceanBase-brightgreen?logo=support" />
    </a>
    <a href="images/dingtalk.png">
        <img alt="DingTalk" src="https://img.shields.io/badge/Dingtalk-Dingtalk-brightgreen?logo=Dingtalk" />
    </a>
        <a href="https://www.youtube.com/@OceanBaseDB">
        <img alt="Static Badge" src="https://img.shields.io/badge/YouTube-red?logo=youtube">
    </a>
    <a href="https://www.linkedin.com/company/oceanbase" target="_blank">
        <img src="https://custom-icon-badges.demolab.com/badge/LinkedIn-0A66C2?logo=linkedin-white&logoColor=fff" alt="follow on LinkedIn">
    </a>
    <a href="https://en.oceanbase.com/">
        <img alt="Static Badge" src="https://img.shields.io/badge/OceanBase-Official%20Website-blue">
    </a>
    <a href="https://oceanbase.github.io/docs/blogs/users/1st-financial">
        <img alt="Static Badge" src="https://img.shields.io/badge/OceanBase-Official%20Blog-blue">
    </a>
    <a href="https://x.com/OceanBaseDB">
        <img alt="Static Badge" src="https://img.shields.io/badge/Follow-%40OceanBaseDB-white?logo=x&labelColor=black">
    </a>
    <a href="https://github.com/oceanbase/oceanbase/graphs/commit-activity" target="_blank">
        <img alt="Static Badge" src="https://img.shields.io/badge/commit%20activity%201020%2Fmonth-yellow">
    </a>
</p>

[English](README.md) | 中文版 | [日本語版](README_JA.md)

**OceanBase Database** 是一个分布式关系型数据库。完全由蚂蚁集团自主研发。 OceanBase 基于 [Paxos](https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf) 协议以及分布式架构，实现了高可用和线性扩展。OceanBase 数据库运行在常见的服务器集群上，不依赖特殊的硬件架构，并支持向量数据库功能，为 AI 和大规模检索场景提供高效的向量查询能力。

# 关键特性

- **向量检索**：支持向量索引和高效查询，可用于 AI 应用、推荐系统和语义搜索，提供高吞吐、低延迟的向量搜索能力；
- **水平扩展**：单机群支持超过1500节点、PB级数据量和单表超万亿行数据；
- **极致性能**：TPC-C 7.07亿tmpC和TPC-H 1526 万 QphH @30000GB；
- **低成本**：存储成本节省70%-90%；
- **实时分析**：不需要额外开销，支持HTAP；
- **高可用**：RPO = 0（0数据丢失），RTO < 8秒（恢复时间）；
- **MySQL 兼容**：很容易的从MySQL迁移过来。


更多信息请参考 [OceanBase 产品](https://www.oceanbase.com/product/oceanbase)。

# 快速开始

更多信息参考[快速体验 OceanBase 数据库](https://open.oceanbase.com/quickStart)。

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

**注意**: 我们在 [dockerhub](https://hub.docker.com/r/oceanbase/oceanbase-ce/tags), [quay.io](https://quay.io/repository/oceanbase/oceanbase-ce?tab=tags) 和 [ghcr.io](https://github.com/oceanbase/docker-images/pkgs/container/oceanbase-ce) 提供镜像。如果您在从 dockerhub 拉取镜像时遇到问题，请尝试其他两个镜像库。

1. 启动 OceanBase 数据库实例

    ```shell
    # 部署一个mini模式实例
    docker run -p 2881:2881 --name oceanbase-ce -e MODE=mini -d oceanbase/oceanbase-ce

    # 使用 quay.io 仓库的镜像部署 OceanBase.
    # docker run -p 2881:2881 --name oceanbase-ce -e MODE=mini -d quay.io/oceanbase/oceanbase-ce

    # 使用 ghcr.io 仓库的镜像部署 OceanBase.
    # docker run -p 2881:2881 --name oceanbase-ce -e MODE=mini -d ghcr.io/oceanbase/oceanbase-ce
    ```

2. 连接 OceanBase

    ```shell
    docker exec -it oceanbase-ce obclient -h127.0.0.1 -P2881 -uroot # 连接root用户sys租户
    ```

更多信息参考[docker 文档](https://github.com/oceanbase/docker-images/blob/main/oceanbase-ce/README_CN.md)。

## ☸️ 使用 Kubernetes

使用 [ob-operator](https://github.com/oceanbase/ob-operator) 可在 Kubernetes 环境中快速部署和管理 OceanBase 数据库实例，可参考文档 [ob-operator 快速上手](https://oceanbase.github.io/ob-operator/zh-Hans/)了解具体的使用方法。

## 👨‍💻 使用源码编译部署

参考 [OceanBase 开发者文档](https://oceanbase.github.io/oceanbase/build-and-run)了解如何编译和部署手动编译的observer。

# Roadmap

请参考 [产品迭代进展](https://github.com/oceanbase/oceanbase/issues/1839) 了解OceanBase规划。 更多详细信息请参考 [OceanBase Roadmap](https://github.com/orgs/oceanbase/projects/4)。

# 案例

OceanBase 已服务超过 2000 家来自不同行业的客户，包括金融服务、电信、零售、互联网等。

更详细的信息请参考[客户案例](https://www.oceanbase.com/customer/home)和[谁在使用 OceanBase](https://github.com/oceanbase/oceanbase/issues/1301)。

# 系统架构

[系统架构介绍](https://www.oceanbase.com/docs/common-oceanbase-database-cn-10000000001687855)

# 社区贡献

非常欢迎社区贡献。请阅读[开发指南](https://oceanbase.github.io/oceanbase)。

# License

OceanBase 数据库根据 Mulan 公共许可证版本 2 获得许可。有关详细信息，请参阅 [LICENSE](LICENSE) 文件。

# 社区

有以下加入社区的方法：

* [中文论坛](https://ask.oceanbase.com/)(推荐): 社区交流与分享, 寻求帮助, 获取最新信息.
* [钉钉群:  33254054](images/dingtalk.png): 寻求帮助, 获取最新信息.
* 微信群 (添加微信小助手: OBCE666): 社区交流与分享, 获取最新信息.
* [GitHub Issues](https://github.com/oceanbase/oceanbase/issues): 反馈使用 OceanBase 时遇到的问题或建议。

