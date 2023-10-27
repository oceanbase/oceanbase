# Install toolchain

To build OceanBase from source code, you need to install the C++ toolchain in your development environment first. If the C++ toolchain is not installed yet, you can follow the instructions in this document for installation.

## Supported OS

OceanBase makes strong assumption on the underlying operator systems. Not all the operator systems are supported; especially, Windows and Mac OS X are not supported yet.

Below is the OS compatibility list:

| OS                  | Version               | Arch   | Compilable | Package Deployable | Compiled Binary Deployable | MYSQLTEST Passed |
| ------------------- | --------------------- | ------ | ---------- | ------------------ | -------------------------- | ---------------- |
| Alibaba Cloud Linux | 2.1903                | x86_64 | Yes        | Yes                | Yes                        | Yes              |
| CentOS              | 7.2 / 8.3             | x86_64 | Yes        | Yes                | Yes                        | Yes              |
| Debian              | 9.8 / 10.9            | x86_84 | Yes        | Yes                | Yes                        | Yes              |
| Fedora              | 33                    | x86_84 | Yes        | Yes                | Yes                        | Yes              |
| openSUSE            | 15.2                  | x86_84 | Yes        | Yes                | Yes                        | Yes              |
| OpenAnolis          | 8.2                   | x86_84 | Yes        | Yes                | Yes                        | Yes              |
| StreamOS            | 3.4.8                 | x86_84 | Unknown    | Yes                | Yes                        | Unknown          |
| SUSE                | 15.2                  | x86_84 | Yes        | Yes                | Yes                        | Yes              |
| Ubuntu              | 16.04 / 18.04 / 20.04 | x86_84 | Yes        | Yes                | Yes                        | Yes              |

> **Note**:
>
> Other Linux distributions _may_ work. If you verify that OceanBase can compile and deploy on a distribution except ones listed above, feel free to submit a pull request to add it.

## Supported GLIBC

OceanBase and its dependencies dynamically link to The GNU C Library (GLIBC). And the version of GLIBC share library is restrict to be less than or equal to 2.34.

See [ISSUE-1337](https://github.com/oceanbase/oceanbase/issues/1337) for more details.

## Installation

The installation instructions vary among the operator systems and package managers you develop with. Below are the instructions for some popular environments:

### Fedora based

This includes CentOS, Fedora, OpenAnolis, RedHat, UOS, etc.

```shell
yum install git wget rpm* cpio make glibc-devel glibc-headers binutils m4 libtool libaio
```

### Debian based

This includes Debian, Ubuntu, etc.

```shell
apt-get install git wget rpm rpm2cpio cpio make build-essential binutils m4
```

### SUSE based

This includes SUSE, openSUSE, etc.

```shell
zypper install git wget rpm cpio make glibc-devel binutils m4
```
