# What is OceanBase Database
OceanBase Database is a native distributed relational database. It is developed entirely by Ant Group. OceanBase Database is built on a common server cluster. Based on the Paxos protocol and its distributed structure, OceanBase Database provides high availability and linear scalability. OceanBase Database is not dependent on specific hardware architectures.

## Core features

- Scalable OLTP
   - Linear scalability by adding nodes to the cluster
   - Partition-level leader distribution and transparent data shuffling 
   - Optimized performance for distributed transaction through "table group" technology
   - High concurrency updates on hot row through early lock release (ELR)
   - 80000+ connections per node and unlimited connections in one instance through multi threads and coroutines
   - Prevent silent data corruption (SDC) through multidimensional data consistency checksum
   - No.1 in TPC-C benchmark with 707 million tpmC
- Operational OLAP
   - Process analytical tasks in one engine, no need to migrate data to OLAP engine
   - Analyze large amounts of data on multiple nodes in one OceanBase cluster with MPP architecture
   - Advanced SQL engine with CBO optimizer, distributed execution scheduler and global index
   - Fast data loading through parallel DML, and with only 50% storage cost under compression
   - Broke world record with 15.26 million QphH in TPC-H 30TB benchmark in 2021
- Multi-tenant
   - Create multiple tenants (instances) in one OceanBase cluster with isolated resource and access
   - Multidimensional and transparently scale up/out for each tenant, and scaling up takes effect immediately
   - Database consolidation: multi-tenant and flexible scaling can achieve resource pooling and improve utilization
   - Improve management efficiency and reduce costs without compromising performance and availability

## Quick start
See [Quick start](https://open.oceanbase.com/quickStart) to try out OceanBase Database.

## System architecture

![image.png](https://cdn.nlark.com/yuque/0/2022/png/25820454/1667369873624-c1707034-471a-4f79-980f-6d1760dac8eb.png)

## Roadmap

![image.png](https://cdn.nlark.com/yuque/0/2022/png/25820454/1667369873613-44957682-76fe-42c2-b4c7-9356ed5b35f0.png)

## Case study
For our success stories, see [Success stories](https://www.oceanbase.com/en/customer/home).

## Contributing
Your contributions to our code will be highly appreciated. For details about how to contribute to OceanBase, see [Contribute to OceanBase](https://github.com/oceanbase/oceanbase/wiki/Contribute-to-OceanBase).

## Licensing
OceanBase Database is under [MulanPubL - 2.0](http://license.coscl.org.cn/MulanPubL-2.0/#english) license. You can freely copy and use the source code. When you modify or distribute the source code, please follow the MulanPubL - 2.0 license.

## Community

- [oceanbase.slack](https://join.slack.com/t/oceanbase/shared_invite/zt-1e25oz3ol-lJ6YNqPHaKwY_mhhioyEuw)
- [Forum (Simplified Chinese)](https://ask.oceanbase.com/)
- [DingTalk 33254054 (Simplified Chinese)](https://h5.dingtalk.com/circle/healthCheckin.html?corpId=ding12cfbe0afb058f3cde5ce625ff4abdf6&53108=bb418&cbdbhh=qwertyuiop&origin=1)
- [WeChat (Simplified Chinese)](https://gw.alipayobjects.com/zos/oceanbase/0a69627f-8005-4c46-be1f-aac7a2b85c13/image/2022-03-01/85d42796-4e22-463a-9658-57402d7b9bc3.png)
