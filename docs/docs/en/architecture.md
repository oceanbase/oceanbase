---
title: Architecture
---

# OceanBase DataBase Architecture

OceanBase Database adopts a shared-nothing architecture, where each node is equal to each other. That is, each node has its own SQL engine, storage engine, and transaction engine, and runs on a cluster of ordinary PC servers. OceanBase Database provides core features such as high scalability, high availability, high performance, cost-effectiveness, and strong compatibility with mainstream databases.

## Zone

A cluster of OceanBase database consists of several nodes. These nodes belong to several availability zones, and each node belongs to an availability zone. Availability zone is a logical concept. OceanBase uses zones to achieve high availability and disaster recovery features of data. Availability zones can be in different computer rooms, regions, etc., to achieve disaster recovery in different scenarios. OceanBase uses the strong consistency protocol Paxos to achieve high availability. Data under the same Paxos group is located in different availability zones.

## Partition

In the OceanBase database, the data of a table can be horizontally split into multiple shards according to certain partitioning rules. Each shard is called a table partition, or Partition. Partition support includes hash, range, list and other types, and also supports secondary partitioning. For example, the order table in the transaction database can first be divided into several first-level partitions according to user ID, and then each first-level partition can be divided into several second-level partitions according to the month. For a second-level partition table, each sub-partition of the second level is a physical partition, while the first-level partition is just a logical concept. Several partitions of a table can be distributed on multiple nodes within an Availability Zone. Each physical partition has a storage layer object used to store data, called Tablet, which is used to store ordered data records.

## Log Stream

When the user modifies the records in the Tablet, in order to ensure data persistence, the redo log (REDO) needs to be recorded to the Log Stream corresponding to the Tablet. One log stream corresponds to multiple Tablets on the node where it is located. Tablet uses a multi-replication mechanism to ensure high availability. Typically, replicas are spread across different Availability Zones. Among multiple replicas, only one replica accepts modification operations, which is called the leader, and the others are called followers. Data consistency between leader and follower replicas is achieved through a distributed consensus protocol based on Multi-Paxos. Multi-Paxos uses Log Stream to implement data replication. Tablets can be migrated between Log Streams to achieve load balancing of resources.

## OBServer

A service process called observer runs on each node in the cluster. Each service is responsible for accessing partitioned data on its own node, and is also responsible for parsing and executing SQL statements routed to the node. These service processes communicate with each other through the TCP/IP protocol. At the same time, each service will listen for connection requests from external applications, establish connections and database sessions, and provide database services.

## Multi-Tenant

In order to simplify the management of large-scale deployment of multiple business databases and reduce resource costs, OceanBase database provides multi-tenant features. Within an OceanBase cluster, multiple isolated database "instances" can be created, called a tenant. From an application perspective, each tenant is equivalent to an independent database instance. Each tenant can choose MySQL or Oracle compatibility mode. After the application is connected to the MySQL tenant, users and databases can be created under the tenant, and the experience is the same as using an independent MySQL instance. After a new cluster is initialized, there will be a special tenant named sys, called the system tenant. The system tenant stores the metadata of the cluster and it is in MySQL compatibility mode.

In addition to the system tenant and user tenant, OceanBase also has a tenant called Meta. Every time a user tenant is created, the system will automatically create a corresponding Meta tenant, whose life cycle is consistent with the user tenant. The Meta tenant is used to store and manage the cluster private data of user tenants. This data does not require cross-instance physical synchronization and physical backup and recovery. The data includes: configuration items, location information, replica information, log stream status, backup and recovery related information, merge information, etc.

## Resource Unit

In order to isolate tenant resources, each observer process can have multiple virtual containers belonging to different tenants, called resource units. Resource units include CPU, memory and disk resources. Multiple resource units form a resource pool. Using the resource pool, you can specify which resource unit to use, how many resource units, and availability zones for resource distribution. When creating a tenant, specify the list of resource pools used to control the resources and data distribution locations used by the tenant.

## obproxy

Applications usually do not directly establish a connection with OBServer, but connect to obproxy, and then obproxy forwards the SQL request to the appropriate OBServer node. obproxy will cache information related to data partitions and can route SQL requests to the most appropriate OBServer node. Obproxy is a stateless service. Multiple obproxy nodes can provide a unified network address to applications through network load balancing (SLB).


