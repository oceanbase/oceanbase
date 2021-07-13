# What is OceanBase database

OceanBase Database is a native distributed relational database. It is developed entirely by Alibaba and Ant Group. OceanBase Database is built on a common server cluster. Based on the Paxos protocol and its distributed structure, OceanBase Database provides high availability and linear scalability. OceanBase Database is not dependent on any specific hardware architecture.

OceanBase Database has these features:

- High availability</br>
    OceanBase Database recovers from single server failure automatically. OceanBase Database supports cross-city disaster tolerance for multiple IDCs and zero data loss. OceanBase Database meets the financial industry Level 6 disaster recovery standard (RPO=0, RTO<=30 seconds).
- Linear scalability</br>
    OceanBase Database scales transparently to applications and balances the system load automatically. It can manage more than 1500 nodes in a cluster. The data volume can reach petabytes. The records in a single table can be more than a trillion rows.
- High compatibility with MySQL</br>
    OceanBase Database is compatible with MySQL protocol and syntax. You can access OceanBase Database by MySQL client.
- High performance</br>
    OceanBase Database supports quasi memory level data change and exclusive encoding compression. Together with the linear scalability, OceanBase Database provides high performance.
- Low cost</br>
    OceanBase Database leverages PC servers and cheap SSDs. Its high storage compression ratio and high performance also reduce the storage and computing costs.
- Multi-tenancy</br>
    OceanBase Database supports native multi-tenancy architecture. One cluster supports multiple businesses. Data is isolated among tenants. This reduces the deployment, operation, and maintenance costs.

OceanBase Database supports the entire core business of Alipay and the core systems of hundreds of financial institutions, such as banks and insurance companies.

## Quick start

Refer to the [Get Started guide](https://open.oceanbase.com/quickStart) (Simplified Chinese, English will be ready soon) to try out OceanBase Database.

## Documentation

- English (Coming soon)
- [Simplified Chinese](https://open.oceanbase.com/docs) (简体中文)

## Supported clients

- [OBClient](https://github.com/oceanbase/obclient)

## Licencing

OceanBase Database is under [MulanPubL - 2.0](https://license.coscl.org.cn/MulanPubL-2.0/index.html) license. You can freely copy and use the source code. When you modify or distribute the source code, please obey the MulanPubL - 2.0 license.

## How to build

### debug mode
```sh
sh build.sh debug --init --make
```

### release mode
```sh
sh build.sh release --init --make
```

### rpm packages
```sh
sh build.sh rpm --init && cd build_rpm && make -j16 rpm
```

## Contributing

Contributions are warmly welcomed and greatly appreciated. Here are a few ways you can contribute:

- Raise us an [issue](https://github.com/oceanbase/oceanbase/issues).
- Submit Pull Requests. For details, see [How to contribute](CONTRIBUTING.md).

## Support

In case you have any problems when using OceanBase Database, welcome to reach out for help:

- [GitHub Issue](https://github.com/oceanbase/oceanbase/issues)
- [Official Website](https://open.oceanbase.com/)
- Knowledge base [link TODO]
