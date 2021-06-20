# What is OceanBase database

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
    OceanBase Database uses PC servers and cheap SSDs. Its high storage compression ratio and high performance also reduce the storage cost and the computing cost.
- Multi-tenancy
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

## OS compatibility list

| OS | Ver. | Arch | Compilable | Package Deployable | Compiled Binary Deployable | Mysqltest Passed |
| ---- | ---- | ---- | ---- | ---- | ---- | ---- |
| CentOS | 7.2,8.3 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| Debian | 9.8,10.9 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| Fedora | 33 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| MacOS | any | x86_64 | ❌ | ❌ | ❌ | ❌ |
| openSUSE | 15.2 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| OpenAnolis | 8.2 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| SUSE | 15.2 | x86_64 | ✅ | ✅ | ✅ | ✅ |
| Ubuntu | 16.04,18.04,20.04 | x86_64 | ✅ | ✅ | ✅ | ✅ |

## How to build

### Preparation

Before building, you need to confirm that your device has installed the necessary software.

#### Fedora based (, including CentOS, Fedora, OpenAnolis, RedHat, etc.)
```sh
yum install git wget rpm* cpio make glibc-devel glibc-headers binutils
```

#### Debian based (, including Debian, Ubuntu, etc.)
```sh
apt-get install git wget rpm rpm2cpio cpio make build-essential binutils
```

#### SUSE based (, including SUSE, openSUSE, etc.)
```sh
zypper install git wget rpm cpio make glibc-devel binutils
```

### debug mode
```bash
bash build.sh debug --init --make
```

### release mode
```bash
bash build.sh release --init --make
```

### rpm packages
```bash
bash build.sh rpm --init && cd build_rpm && make -j16 rpm
```

## Contributing

Contributions are warmly welcomed and greatly appreciated. Here are a few ways you can contribute:

- Raise us an [issue](https://github.com/oceanbase/oceanbase/issues).
- Submit Pull Requests. For details, see [How to contribute](CONTRIBUTING.md).

## Support

In case you have any problems when using OceanBase Database, welcome reach out for help:

- [GitHub Issue](https://github.com/oceanbase/oceanbase/issues)
- [Official Website](https://open.oceanbase.com/)
- Knowledge base [link TODO]
