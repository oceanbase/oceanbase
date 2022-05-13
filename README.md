# What is OceanBase Database

OceanBase Database is a native distributed relational database. It is developed entirely by Alibaba and Ant Group. OceanBase Database is built on a common server cluster. Based on the Paxos protocol and its distributed structure, OceanBase Database provides high availability and linear scalability. OceanBase Database is not dependent on specific hardware architectures.

OceanBase Database has these features:

- High availability
    
    Single server failure recovers automatically. OceanBase Database supports cross-city disaster tolerance for multiple IDCs and zero data loss. OceanBase Database meets the financial industry Level 6 disaster recovery standard (RPO=0, RTO<=30 seconds).
- Linear scalability
    
    OceanBase Database scales transparently to applications and balances the system load automatically. Its cluster can contain more than 1500 nodes. The data volume can reach petabytes. The records in a single table can be more than a trillion rows.
- Highly compatible with MySQL
    
    OceanBase Database is compatible with MySQL protocol and syntax. You can access to OceanBase Database by using MySQL client.
- High performance
    
    OceanBase Database supports quasi memory level data change and exclusive encoding compression. Together with the linear scalability, OceanBase Database provides high performance.
- Low cost
    
    OceanBase Database uses PC servers and cheap SSDs. Its high storage compression ratio and high performance also reduce the storage and computing cost.
- Multi-tenancy
    
    OceanBase Database supports native multi-tenancy architecture. One cluster supports multiple businesses. Data is isolated among tenants. This reduces the deployment, operation, and maintenance costs.

OceanBase Database supports the entire core business of Alipay and the core systems of hundreds of financial institutions, such as banks and insurance companies.

## Quick start

Refer to the [Get Started guide](https://open.oceanbase.com/quickStart) to try out OceanBase Database.

## Documentation

- English (Coming soon)
- [Simplified Chinese](https://open.oceanbase.com/docs) (简体中文)

## Supported clients

- [OBClient](https://github.com/oceanbase/obclient)
- [MySQLClient](https://dev.mysql.com/downloads/)

## Licencing

OceanBase Database is under [MulanPubL - 2.0](http://license.coscl.org.cn/MulanPubL-2.0) license. You can freely copy and use the source code. When you modify or distribute the source code, please obey the MulanPubL - 2.0 license.

## OS compatibility list

| OS | Ver. | Arch | Compilable | Package Deployable | Compiled Binary Deployable | Mysqltest Passed |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| Alibaba Cloud Linux | 2.1903 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| Arch | Rolling | x86_64 | ✅ | ❌ | ❌ | ❌ |
| CentOS | 7.2, 8.3 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| Debian | 9.8, 10.9 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| Fedora | 33 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| MacOS | any | x86_64 | ❌ | ❌ | ❌ | ❌ |
| openSUSE | 15.2 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| OpenAnolis | 8.2 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| SUSE | 15.2 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| Ubuntu | 16.04, 18.04, 20.04 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| UOS | 20 | x86_64 | ✅ | ✅ | ✅ | ✅ |

## Developer Manual

1. [How to build](https://github.com/oceanbase/oceanbase/wiki/how_to_build)
2. [How to setup IDE](https://github.com/oceanbase/oceanbase/wiki/how_to_setup_ide)
3. [How to contribute](https://github.com/oceanbase/oceanbase/wiki/how_to_contribute)
4. [How to modify document](https://github.com/oceanbase/oceanbase/wiki/how_to_modify_docs)
5. [How to debug OceanBase](https://github.com/oceanbase/oceanbase/wiki/how_to_debug)
6. [How to run test](https://github.com/oceanbase/oceanbase/wiki/how_to_test)
7. [How to fix one_bug](https://github.com/oceanbase/oceanbase/wiki/how_to_fix_bug)




## Support

In case you have any problems when using OceanBase Database, welcome reach out for help:

- [GitHub Issue](https://github.com/oceanbase/oceanbase/issues)
- [Official Website](https://open.oceanbase.com/)

## Community

 - [Forum](https://open.oceanbase.com/answer)
 - [DingTalk 33254054](https://h5.dingtalk.com/circle/healthCheckin.html?corpId=ding0e936c01b36c156d60c3ef38bbf6dadc&594d9=30470&cbdbhh=qwertyuiop&origin=1)
 - [Wechat oceanbasecommunity](https://gw.alipayobjects.com/zos/oceanbase/0a69627f-8005-4c46-be1f-aac7a2b85c13/image/2022-03-01/85d42796-4e22-463a-9658-57402d7b9bc3.png)
 - [oceanbase.slack](https://oceanbase.slack.com/)
 - Mailist [Comming soon]


## Roadmap

Please refer to [Roadmap](https://github.com/oceanbase/oceanbase/wiki/roadmap) for details. 
