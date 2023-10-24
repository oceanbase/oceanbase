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

English | [中文版](README_CN.md)

**OceanBase Database** is a distributed relational database. It is developed entirely by Ant Group. OceanBase Database is built on a common server cluster. Based on the [Paxos](https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf) protocol and its distributed structure, OceanBase Database provides high availability and linear scalability. OceanBase Database is not dependent on specific hardware architectures.

# Key features

- **Transparent Scalability**

    An OceanBase cluster can be scaled out to 1,500 nodes transparently, handling petabytes of data and a trillion rows of records.

- **Ultra-fast Performance**
    
    The only distributed database that has refreshed both TPC-C record, at 707 million tmpC, and TPC-H record, at 15.26 million QphH @30000GB.

- **Real-time Operational Analytics**
    
    A unified system for both transactional and real-time operational analytics workloads.

- **Continuous Availability**
    
    OceanBase Database adopts Paxos Consensus algorithm to achieve Zero RPO and less than 8 seconds of RTO. Supports intra-city/remote disaster recovery, enabling multi-activity in multiple locations and zero data loss.

- **MySQL Compatible**
     
     OceanBase Database is highly compatible with MySQL, which ensures that zero or few modification is needed for migration.

- **Cost Efficiency**

    The cutting-edge compression technology saves 70%-90% of storage costs without compromising performance. The multi-tenancy architecture achieves higher resource utilization.

See also [key features](https://en.oceanbase.com/product/opensource) for more details.

# Quick start

## 🔥 Start with all-in-one

You can quickly deploy a standalone OceanBase Database to experience with the following commands.

**Note**: Linux Only

```shell
# download and install all-in-one package (internet connection is required)
bash -c "$(curl -s https://obbusiness-private.oss-cn-shanghai.aliyuncs.com/download-center/opensource/oceanbase-all-in-one/installer.sh)"
source ~/.oceanbase-all-in-one/bin/env.sh

# quickly deploy OceanBase database
obd demo
```

## 🐳 Start with docker

1. Start an OceanBase Database instance:

    ```shell
    # Deploy a mini standalone instance.
    docker run -p 2881:2881 --name oceanbase-ce -e MINI_MODE=1 -d oceanbase/oceanbase-ce
    ```

2. Connect to the OceanBase Database instance:

    ```shell
    docker exec -it oceanbase-ce ob-mysql sys # Connect to the root user of the sys tenant.
    ```

See also [Quick experience](https://en.oceanbase.com/docs/community-observer-en-10000000000829647) or [Quick Start (Simplified Chinese)](https://open.oceanbase.com/quickStart) for more details.

## 👨‍💻 Start developing

See [OceanBase Developer Document](https://github.com/oceanbase/oceanbase/wiki/Compile) to learn how to compile and deploy a manually compiled observer.

# Roadmap

For future plans, see [Roadmap 2023](https://github.com/oceanbase/oceanbase/issues/1364). See also [OceanBase Roadmap](https://github.com/orgs/oceanbase/projects) for more details.

# Case study

OceanBase has been serving more than 400 customers upgrade their database from different industries, including Financial Services, Telecom, Retail, Internet and more.

See also [success stories](https://en.oceanbase.com/customer/home) and [Who is using OceanBase](https://github.com/oceanbase/oceanbase/issues/1301) for more details.

# System architecture

[Introduction to system architecture](https://en.oceanbase.com/docs/community-observer-en-10000000000829641)

# Contributing

Contributions are highly appreciated. Read the [development guide](docs/README.md) to getting started.

# License

OceanBase Database is licensed under the Mulan Public License, Version 2. See the [LICENSE](LICENSE) file for more info.

# Community

Join the OceanBase community via:

* [Slack Workspace](https://join.slack.com/t/oceanbase/shared_invite/zt-1e25oz3ol-lJ6YNqPHaKwY_mhhioyEuw)
* [Chinese User Forum](https://ask.oceanbase.com/)
* DingTalk Group: 33254054 ([QR code](images/dingtalk.svg))
* WeChat Group (Add the assistant with WeChat ID: OBCE666)
