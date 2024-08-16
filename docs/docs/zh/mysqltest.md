# 使用 obd.sh 运行 mysqltest

要使用 obd.sh 运行 mysqltest 测试，需要通过 obd.sh 部署 OceanBase 数据库。本文从编译源代码开始，使用示例介绍如何使用 obd.sh 部署 OceanBase 数据库并运行 mysqltest 测试。

## 背景

为了简化开发者的操作步骤，降低其理解成本，我们将一些 OBD 命令封装到 obd.sh 脚本中，并将脚本存放在 OceanBase 源代码的 oceanbase/tools/deploy 目录下。本文通过在 OBD 中调用 [obd test mysqltest](https://www.oceanbase.com/docs/community-obd-cn-10000000002048173) 命令，运行 mysqltest 测试。

## 相关概念

mysqltest 是OceanBase数据库中的一种准入测试，简单来说，它以编写的 case 文件为输入，将数据库的输出与预期输出进行比较。OceanBase数据库中 mysqltest 测试的 case 都位于 `tools/deploy/mysql_test` 目录下。

`case` 是 mysqltest 的最小执行单元，一个 `case` 至少包含一个 test 文件和一个 result 文件。对 case 进行分类形成 `suite`，`suite` 是 case 的集合。

mysqltest 有多种运行模式，取决于选择的节点，常见的 mysqltest 模式如下。

* c 模式：连接到 Primary Zone 所在的服务器运行 mysqltest。例如，使用配置文件 distributed.yaml 部署集群，然后连接到 server1 运行测试。

  ```shell
  ./obd.sh mysqltest -n <name> --suite acs --test-server=server1
  ```
* Slave 模式：连接到 Primary Zone 以外的服务器运行 mysqltest。例如，使用配置文件 distributed.yaml 部署集群，然后连接到 server2 运行测试。

  ```shell
  ./obd.sh mysqltest -n <name> --suite acs --test-server=server2
  ```

* Proxy 模式：通过 ODP 连接到集群进行 mysqltest 测试。例如，使用配置文件 distributed-with-proxy.yaml 部署集群，然后运行测试。

  ```shell
  ./obd.sh mysqltest -n <name> --all
  ```

## 执行步骤

### 步骤 1: 编译源代码

查看[编译运行](./build-and-run.md)文档，编译源代码。

### 步骤 2: 运行 mysqltest 测试

可以全部运行测试，也可以指定测试的 `case` 或 `suite`。具体执行 obd.sh 脚本时使用的参数含义，请参考[附录](#附录)。

* 全部测试，即运行 `mysql_test/test_suite` 目录下的所有 suite，请参考以下命令。

  ```shell
  [admin@obtest ~]$ cd oceanbase/tools/deploy
  [admin@obtest deploy]$ ./obd.sh mysqltest -n test --all
  ```

* 指定 case 测试，例如指定 `mysql_test/test_suite/alter/t/alter_log_archive_option.test`。请参考以下命令。

  ```shell
  [admin@obtest ~]$ cd oceanbase/tools/deploy
  [admin@obtest deploy]$ ./obd.sh mysqltest -n test --test-dir ./mysql_test/test_suite/alter/t --result-dir ./mysql_test/test_suite/alter/r --test-set alter_log_archive_option
  ```

* 指定一个 suite 测试，例如在 `mysql_test/test_suite` 目录下指定一个 suite 进行测试，请参考以下命令。

  ```shell
  [admin@obtest ~]$ cd oceanbase/tools/deploy
  [admin@obtest deploy]$ ./obd.sh mysqltest -n test --suite acs
  ```

## 附录

在执行 mysqltest 测试时，可以根据实际情况配置一些参数。参数说明如下表：

| 参数名称 | 是否必须 | 类型 | 默认值 | 说明 |
|--------|---------|----------|-------|------|
| -n     | Y  | 字符串 | null | 集群名称 |
| --component | N  | 字符串 | null | T要测试的组件名称。可选值：obproxy、obproxy-ce、oceanbase和oceanbase-ce。如果是空，按照 obproxy、obproxy-ce、oceanbase、oceanbase-ce的顺序检查。如果检测到某个组件存在，就会停止遍历，然后执行相应的测试。 |
| --test-server | N  | 字符串 | 默认为指定组件下的服务器中的第一个节点 | 要测试的机器，可以设置为 yaml 文件中 servers 下指定组件下的 name 值对应的服务器。如果 servers 下没有配置 name 值，将使用 ip 值，必须是指定组件下的某个节点的 name 值。 |
| --user | N  | 字符串 | admin | 执行测试的用户名，一般不需要修改。 |
| --password | N | 字符串 | admin | 密码 |
| --database | N  | 字符串 | test | database |
| --mysqltest-bin | N  | 字符串 | /u01/obclient/bin/mysqltest | mysqltest 二进制文件路径 |
| --obclient-bin | N  | 字符串 | obclient | obclient 二进制文件路径 |
| --test-dir | N  | 字符串 | ./mysql_test/t | test-file 的目录。如果找不到 test-file，将尝试在 OBD 内置中查找。 |
| --test-file-suffix | N  | 字符串 | .test | mysqltest 所需的 test-file 的后缀。 |
| --result-dir | N  | 字符串 | ./mysql_test/r | result-file 的目录。如果找不到 result-file，将尝试在 OBD 内置中查找。 |
| --result-file-suffix | N  | 字符串 | .result | result-file 文件的后缀。 |
| --record | N  | bool | false | 仅记录执行结果，不执行测试。 |
| --record-dir | N  | 字符串 | ./record | 执行结果记录的目录。 |
| --record-file-suffix | N  | 字符串 | .record | 执行结果记录的文件后缀。 |
| --tmp-dir | N  | 字符串 | ./tmp | mysqltest 的 tmpdir 选项。 |
| --var-dir | N  | 字符串 | ./var | mysqltest 的 logdir 选项。 |
| --test-set | N  | 字符串 | no | 测试用例数组。使用逗号（`,`）分隔多个用例。 |
| --exclude | N  | 字符串 | no | 需要排除的测试用例数组。使用逗号（`,`）分隔多个用例。 |
| --test-pattern | N  | 字符串 | no | 测试文件匹配的正则表达式。所有匹配表达式的用例将覆盖 test-set 选项。 |
| --suite | N  | 字符串 | no | suite 数组。一个 suite 包含多个测试，可以用逗号（`,`）分隔。 |
| --suite-dir | N  | 字符串 | ./mysql_test/test_suite | suite 目录。如果找不到 suite 目录，将尝试在 OBD 内置中查找。 |
| --all | N  | bool | false | 执行所有 --suite-dir 下的 case。suite 目录所在的目录下的所有 case。 |
| --need-init | N  | bool | false | 执行初始化SQL文件。新集群可能需要在执行 mysqltest 之前执行一些初始化文件，例如创建用于 case 的账户和租户。 |
| --init-sql-dir | N  | 字符串 | ./ | 初始化SQL文件所在的目录。当找不到 sql 文件时，将尝试在 OBD 内置中查找。 |
| --init-sql-files | N  | 字符串 | Default is empty | 初始化SQL文件数组，当需要初始化时执行。使用逗号（`,`）分隔。如果未填写，如果需要初始化，OBD 将根据集群配置执行内置初始化。 |
| --auto-retry | N  | bool | false | 测试失败时，自动重新部署集群并重试。 |
| --psmall | N  | bool | false | 以psmall模式执行case。 |
| --slices | N  | int | null | 测试用例拆分为多少组。 |
| --slice-idx | N  | int | null | 指定当前组 ID。 |
| --slb-host | N  | 字符串 | null | 指定软负载均衡中心。 |
| --exec-id | N  | 字符串 | null | 指定执行 ID。 |
| --case-filter | N  | 字符串 | ./mysql_test/filter.py | filter.py 文件维护需要过滤的 case。 |
| --reboot-timeout | N  | int | 0 | 重启超时时间。 |
| --reboot-retries | N  | int | 5 | 失败重启的重试次数。 |
| --collect-all | N  | bool | false | 是否收集组件日志。 |
| --log-dir | N  | 字符串 | 日志默认在tmp_dir下 | mysqltest 的日志存储路径。 |
| --log-pattern | N  | 字符串 | *.log | 收集与正则表达式匹配的日志文件名称，命中的文件将被收集。 |
| --case-timeout | N  | int | 3600 | 单个测试用例的超时时间。 |
| --disable-reboot | N  | bool | false | 测试执行期间不再重启。 |
| --collect-components | N  | 字符串 | null | 收集哪些组件的日志。多个组件用逗号（`,`）分隔。 |
| --init-only | N  | bool | false | 仅指定初始化SQL文件。 |
