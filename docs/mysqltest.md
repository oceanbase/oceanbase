# 使用 obd.sh 执行 mysqltest

使用 obd.sh 执行 mysqltest 测试时需使用通过 obd.sh 部署的 OceanBase 数据库，本文结合示例介绍如何从编译源码开始使用 obd.sh 部署 OceanBase 数据库并执行 mysqltest 测试。

## 背景介绍

为简化开发者的操作步骤，降低开发者的理解成本，我们将 OBD 的一些命令封装到 obd.sh 脚本中，并将该脚本存放在 OceanBase 源码 [oceanbase/tools/deploy](https://github.com/oceanbase/oceanbase/blob/master/tools/deploy/obd.sh) 目录下。本文执行 mysqltest 测试调用的是 OBD 中 [obd test mysqltest](https://www.oceanbase.com/docs/community-obd-cn-10000000002048173) 命令。

## 概念介绍

mysqltest 是 OceanBase 数据库准入测试中的一个测试，简单来讲就是将编写好的 case 文件作为输入，将数据库的输出与期望的输出进行比较。OceanBase 数据库中 mysqltest 测试的 case 均位于 OceanBase 源码 [oceanbase/tools/deploy/mysql_test](https://github.com/oceanbase/oceanbase/tree/master/tools/deploy/mysql_test) 目录下。

case 是 mysqltest 的最小执行单元， 一个 case 包含至少一个测试文件和一个结果文件。将 case 进行分类就形成了 suite，suite 是一套 case 的集合。

执行 mysqltest 测试时根据选择的节点不同分为不同的模式，常见 mysqltest 模式如下。

> **说明**
> 
> 执行 mysqltest 涉及到的配置参数请参见下文 [附录](mysqltest.md#附录)。

* c 模式：连接 Primary Zone 所在的 server 执行 mysqltest，如使用配置文件 distributed.yaml 部署集群后连接 server1 执行测试。
  
  ```shell
  ./obd.sh mysqltest -n <name> --suite acs --test-server=server1
  ```

* slave 模式：连接非 Primary Zone 所在的 server 执行 mysqltest，如使用配置文件 distributed.yaml 部署集群后连接 server2 执行测试。

  ```shell
  ./obd.sh mysqltest -n <name> --suite acs --test-server=server2
  ```

* proxy 模式：通过 ODP 连接集群进行 mysqltest 测试，如使用配置文件 distributed-with-proxy.yaml 部署集群后执行测试。

  ```shell
  ./obd.sh mysqltest -n <name> --all
  ```

## 操作步骤

本文以 x86 架构的 CentOS Linux 7.9 作为环境介绍如何操作，仅供参考，详细编译部署过程可参考 [Wiki](https://github.com/oceanbase/oceanbase/wiki/Compile_CN)。

### 步骤一：源码编译 OceanBase 数据库

1. 下载 OceanBase 源码

   ```shell
   [admin@obtest ~]$ git clone https://github.com/oceanbase/oceanbase.git
   ```

2. 根据操作系统准备依赖包

   ```shell
   [admin@obtest ~]$ sudo yum install git wget rpm* cpio make glibc-devel glibc-headers binutils m4
   ```

3. 编译

   ```shell
   [admin@obtest ~]$ cd oceanbase
   [admin@obtest oceanbase]$ bash build.sh release --init --make
   ```

### 步骤二：部署编译生成的 OceanBase 文件

1. 准备配置文件

   ```shell
   [admin@obtest ~]$ sudo yum install -y yum-utils
   [admin@obtest ~]$ sudo yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo
   [admin@obtest ~]$ sudo yum install -y libtool libaio obclient
   [admin@obtest ~]$ cd oceanbase/tools/deploy
   [admin@obtest deploy]$ ./obd.sh prepare
   ```

   obd.sh prepare 命令会从编译目录（例如 build_release）复制二进制文件并制作成镜像，然后根据当前环境信息，生成默认配置文件。如果同时存在多个编译目录，该命令默认选择第一个。

   部署时您可选择使用生成的默认配置文件，也可在 [OBD GitHub 仓库](https://github.com/oceanbase/obdeploy/tree/master/example) 中下载其他示例配置文件修改使用。您需根据 mysqltest 测试的模式选择对应的配置文件，具体如下。

   > **说明**
   > 
   > mysqltest 测试的模式介绍可参见上文 [概念介绍](#概念介绍)。

   * 执行 c 模式的 mysqltest 测试可使用任一配置文件部署。

   * 执行 slave 模式的 mysqltest 测试，您需使用配置文件 distributed.yaml 或 distributed-with-proxy.yaml 部署两节点集群。

   * 执行 proxy 模式的 mysqltest 测试，您需使用配置文件 distributed-with-proxy.yaml 部署 OceanBase + ODP 集群。

3. （可选）修改配置文件

   如果您需修改端口、目录等信息，可通过如下命令打开配置文件并修改。

   ```shell
   [admin@obtest deploy]$ vim distributed-with-proxy.yaml
   ```

   您需关注的配置项如下表所示。

   | 配置项     | 是否必选   | 说明                                 |
   |------------|-----------|--------------------------------------|
   | servers     | 必选      | 每台机器需要用 `- name: 机器标识名 (换行)ip: 机器 IP` 指定，多个机器就指定多次，机器标识名不能重复。</br>在机器 IP 不重复的情况下，也可以使用 `- <ip> （换行）- <ip>` 的格式指定，此时 `- <ip>` 的格式相当于 `- name: 机器 IP（换行）ip: 机器 IP`。  |
   | mysql_port | 必选      | 设置 SQL 服务协议端口号。 |
   | rpc_port   | 必选      | 设置远程访问的协议端口号，是 observer 进程跟其他节点进程之间的 RPC 通信端口。 |
   | home_path  | 必选      | 组件的安装路径。   |
   | production_mode | 必选 | 生产模式参数，开启后会检查服务器资源是否满足生产环境使用，测试场景建议关闭，否则可能部署失败。 |
   | devname    | 可选      | 和 `servers` 里指定的 IP 对应的网卡，通过 `ip addr` 命令可以查看 IP 和网卡对应关系。    |
   | listen_port  | 必选    | ODP 监听端口。  |
   | prometheus_listen_port | 必选  | ODP prometheus 监听端口。  |

4. 部署 OceanBase 数据库

   ```shell
   [admin@obtest deploy]$ ./obd.sh deploy -c distributed-with-proxy.yaml -n test
   ```

   其中，`-c` 用于指定部署所需的配置文件路径，此处以 distributed-with-proxy.yaml 为例；`-n` 用于指定部署集群名称，此处以 test 为例。

### 步骤三：执行 mysqltest 测试

您可选择全量测试，也可指定 case 或 suite 进行测试。执行 obd.sh 脚本时用到的参数具体含义可参考 [附录](#附录)。

* 全量测试，即执行 `mysql_test/test_suite` 目录下全部 suite，可参考如下命令。
  
  ```shell
  [admin@obtest ~]$ cd oceanbase/tools/deploy
  [admin@obtest deploy]$ ./obd.sh mysqltest -n test --all
  ```

* 指定 case 进行测试，例如指定 `mysql_test/test_suite/alter/t/alter_log_archive_option.test`，可参考如下命令。
  
  ```shell
  [admin@obtest ~]$ cd oceanbase/tools/deploy
  [admin@obtest deploy]$ ./obd.sh mysqltest -n test --test-dir ./mysql_test/test_suite/alter/t --result-dir ./mysql_test/test_suite/alter/r --test-set alter_log_archive_option
  ```

* 指定 suite 测试，例如对 `mysql_test/test_suite` 目录下指定 suite 执行测试，可参考如下命令。
  
  ```shell
  [admin@obtest ~]$ cd oceanbase/tools/deploy
  [admin@obtest deploy]$ ./obd.sh mysqltest -n test --suite acs
  ```

## 附录

执行 mysqltest 测试时可根据实际情况配置一些参数，各参数说明见下表：

| 参数名 | 是否必选 | 数据类型 | 默认值 | 说明 |
|--------|---------|----------|-------|------|
| -n     | 是  | string | 默认为空 | 执行测试的集群名。 |
| --component | 否  | string | 默认为空 | 待测试的组件名。候选项为 obproxy、obproxy-ce、oceanbase 和 oceanbase-ce。为空时，按 obproxy、obproxy-ce、oceanbase、oceanbase-ce 的顺序进行检查。检查到组件存在则不再遍历，使用命中的组件进行后续测试。 |
| --test-server | 否  | string | 默认为指定的组件下服务器中的第一个节点 | 待测试的机器，可设置为 yaml 文件中 servers 对应的 name 值，若 servers 后未配置 name 值，则使用 ip 值，必须是指定的组件下的某个节点名。 |
| --user | 否  | string | admin | 执行测试的用户名，一般情况下不需要修改。 |
| --password | 否  | string | admin | 执行测试的用户密码。 |
| --database | 否  | string | test | 执行测试的数据库。 |
| --mysqltest-bin | 否  | string | /u01/obclient/bin/mysqltest | mysqltest 二进制文件路径。 |
| --obclient-bin | 否  | string | obclient | OBClient 二进制文件路径。 |
| --test-dir | 否  | string | ./mysql_test/t | mysqltest 所需的 test-file 存放的目录。test 文件找不到时会尝试在 OBD 内置中查找。 |
| --test-file-suffix | 否  | string | .test | mysqltest 所需的 test-file 的后缀。 |
| --result-dir | 否  | string | ./mysql_test/r | mysqltest 所需的 result-file 存放的目录。result 文件找不到时会尝试在 OBD 内置中查找。 |
| --result-file-suffix | 否  | string | .result | mysqltest 所需的 result-file 的后缀。 |
| --record | 否  | bool | false | 仅记录 mysqltest 的执行结果作为 record-file。 |
| --record-dir | 否  | string | ./record | 记录 mysqltest 的执行结果所存放的目录。 |
| --record-file-suffix | 否  | string | .record | 记录 mysqltest 的执行结果的后缀。 |
| --tmp-dir | 否  | string | ./tmp | 为 mysqltest tmpdir 选项。 |
| --var-dir | 否  | string | ./var | 将在该目录下创建 log 目录并作为 logdir 选项传入 mysqltest。 |
| --test-set | 否  | string | 无 | test case 数组。多个 case 使用英文逗号（`,`）间隔。 |
| --exclude | 否  | string | 无 | 需要除外的 test case 数组。多个 case 中使用英文逗号（`,`）间隔。 |
| --test-pattern | 否  | string | 无 | test 文件名匹配的正则表达式。所有匹配表达式的 case 将覆盖 test-set 选项。 |
| --suite | 否  | string | 无 | suite 数组。一个 suite 下包含多个 test，可以使用英文逗号（`,`）间隔。 |
| --suite-dir | 否  | string | ./mysql_test/test_suite | 存放 suite 目录的目录。suite 目录找不到时会尝试在 OBD 内置中查找。 |
| --all | 否  | bool | false | 执行 --suite-dir 下全部的 case。存放 suite 目录的目录。 |
| --need-init | 否  | bool | false | 执行 init sql 文件。一个新的集群要执行 mysqltest 前可能需要执行一些初始化文件，比如创建 case 所需要的账号和租户等。存放 suite 目录的目录。默认不开启。 |
| --init-sql-dir | 否  | string | ./ | init sql 文件所在目录。sql 文件找不到时会尝试在 OBD 内置中查找。 |
| --init-sql-files | 否  | string | 默认为空 | 需要 init 时执行的 init sql 文件数组。英文逗号（`,`）间隔。不填时，如果需要 init，OBD 会根据集群配置执行内置的 init。 |
| --auto-retry | 否  | bool | false | 失败时自动重部署集群进行重试。 |
| --psmall | 否  | bool | false | 执行 psmall 模式的 case。 |
| --slices | 否  | int | 默认为空 | 需要执行的 case 将被分成的组数。 |
| --slice-idx | 否  | int | 默认为空 | 指定当前分组 id。 |
| --slb-host | 否  | string | 默认为空 | 指定软负载均衡中心。 |
| --exec-id | 否  | string | 默认为空 | 指定执行 id。 |
| --case-filter | 否  | string | ./mysql_test/filter.py | filter.py 文件，维护了需要过滤的 case。 |
| --reboot-timeout | 否  | int | 0 | 重启的超时时间。 |
| --reboot-retries | 否  | int | 5 | 重启失败重试次数。 |
| --collect-all | 否  | bool | false | 是否收集组件日志。 |
| --log-dir | 否  | string | 默认为 tmp_dir 下的 log | mysqltest 的日志存放路径。 |
| --log-pattern | 否  | string | *.log | 收集日志文件名匹配的正则表达式，命中的文件将被收集。 |
| --case-timeout | 否  | int | 3600 | mysqltest 单个测试的超时时间。 |
| --disable-reboot | 否  | bool | false | 在执行测试的过程中不再重启。 |
| --collect-components | 否  | string | 默认为空 | 用来指定要进行日志收集的组件，多个组件以英文逗号（`,`）间隔。 |
| --init-only | 否  | bool | false | 为 true 时表示仅执行 init SQL。 |
