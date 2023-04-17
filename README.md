<p align="center">
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="OceanBase Logo" src="docs/Oceanbase-LogoRGB.svg" width="50%" />
    </a>
</p>
<p align="center">
    <a href="https://github.com/oceanbase/oceanbase/blob/master/LICENSE">
        <img alt="license" src="https://img.shields.io/badge/license-MulanPubL--2.0-blue" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/releases">
        <img alt="license" src="https://img.shields.io/badge/dynamic/json?color=blue&label=release&query=tag_name&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase%2Freleases%2Flatest" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="stars" src="https://img.shields.io/badge/dynamic/json?color=blue&label=stars&query=stargazers_count&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase">
        <img alt="forks" src="https://img.shields.io/badge/dynamic/json?color=blue&label=forks&query=forks&url=https%3A%2F%2Fapi.github.com%2Frepos%2Foceanbase%2Foceanbase" />
    </a>
    <a href="https://en.oceanbase.com/docs/community-observer-en-10000000000829617">
        <img alt="English doc" src="https://img.shields.io/badge/docs-English-blue" />
    </a>
    <a href="https://www.oceanbase.com/docs/oceanbase-database-cn">
        <img alt="Chinese doc" src="https://img.shields.io/badge/文档-简体中文-blue" />
    </a>
    <a href="https://github.com/oceanbase/oceanbase/commits/master">
        <img alt="last commit" src="https://img.shields.io/github/last-commit/oceanbase/oceanbase/master" />
    </a>
    <a href="https://join.slack.com/t/oceanbase/shared_invite/zt-1e25oz3ol-lJ6YNqPHaKwY_mhhioyEuw">
        <img alt="Join Slack" src="https://img.shields.io/badge/slack-Join%20Oceanbase-brightgreen?logo=slack" />
    </a>
</p>

**OceanBase Database** is a distributed relational database. It is developed entirely by Ant Group. OceanBase Database is built on a common server cluster. Based on the [Paxos](https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf) protocol and its distributed structure, OceanBase Database provides high availability and linear scalability. OceanBase Database is not dependent on specific hardware architectures.

# Key features
* Transparent Scalability
* Ultra-fast Performance
* Real-time Operational Analytics
* Continuous Availability
* MySQL Compatible
* Cost Effeciency

See [key features](https://en.oceanbase.com/product/opensource) to learn more details.

# System architecture
The operation of an OceanBase Database is supported by different components in the storage, replication, transaction, SQL, and access layers:
* **Storage layer**: Stores the data of a table or a table partition.
* **Replication layer**: Ensures data consistency between replicas of a partition based on consensus algorithms.
* **Transaction layer**: Ensures the atomicity and isolation of the modifications to one or multiple partitions.
* **SQL layer**: Translates SQL queries into executions over the storage.
* **Access layer**: Forwards user queries to the appropriate OceanBase Database instances for processing.

![image.png](https://cdn.nlark.com/yuque/0/2022/png/25820454/1667369873624-c1707034-471a-4f79-980f-6d1760dac8eb.png)

[Learn More](https://en.oceanbase.com/docs/community-observer-en-10000000000829641)
# Quick start

## How to deploy

### 🔥 Deploy by all-in-one

You can quickly deploy a standalone OceanBase Database to experience with the following commands.

```shell
# download and install all-in-one package (internet connection is required)
$> bash -c "$(curl -s https://obbusiness-private.oss-cn-shanghai.aliyuncs.com/download-center/opensource/oceanbase-all-in-one/installer.sh)"
$> source ~/.oceanbase-all-in-one/bin/env.sh

# quickly deploy OceanBase database
$> obd demo
```
### 🐳 Deploy by docker

1. Pull OceanBase image (optional)
```shell
$> docker pull oceanbase/oceanbase-ce
```
2. Start an OceanBase Database instance
```shell
# Deploy an instance with the maximum specifications supported by the container.
$> docker run -p 2881:2881 --name obstandalone -e MINI_MODE=0 -d oceanbase/oceanbase-ce
# Or deploy a mini standalone instance.
$> docker run -p 2881:2881 --name obstandalone -e MINI_MODE=1 -d oceanbase/oceanbase-ce
```
3. Connect to the OceanBase Database instance
```shell
$> docker exec -it obstandalone ob-mysql sys # Connect to the root user of the sys tenant.
$> docker exec -it obstandalone ob-mysql root # Connect to the root user of the test tenant.
$> docker exec -it obstandalone ob-mysql test # Connect to the test user of the test tenant.
```

See also [Quick experience](https://en.oceanbase.com/docs/community-observer-en-10000000000829647) or [Quick Start (Simplified Chinese)](https://www.oceanbase.com/docs/common-oceanbase-database-cn-10000000001692850) for more details.

## How to build

See [OceanBase Developer Document](https://github.com/oceanbase/oceanbase/wiki/Compile) to learn how to compile and deploy a munually compiled observer.

# Roadmap

See [OceanBase Roadmap](https://github.com/orgs/oceanbase/projects).
![image.png](https://cdn.nlark.com/yuque/0/2022/png/25820454/1667369873613-44957682-76fe-42c2-b4c7-9356ed5b35f0.png)

# Case study

See [success stories](https://en.oceanbase.com/customer/home).

# Contributing

Contributions are highly appreciated. Read the [Contribute to OceanBase](https://github.com/oceanbase/oceanbase/wiki/Contribute-to-OceanBase) guide to getting started.

# License

OceanBase Database is licensed under the Mulan Public License, Version 2. See the [LICENSE](LICENSE) file for more info.

# Community

Join the OceanBase community via:

* [Slack Workspace](https://join.slack.com/t/oceanbase/shared_invite/zt-1e25oz3ol-lJ6YNqPHaKwY_mhhioyEuw)
* [Chinese User Forum](https://ask.oceanbase.com/)
* DingTalk Group: 33254054 ([QR code](docs/DingTalk.JPG))
* WeChat Group (Add the assistant with WeChat ID: OBCE666)