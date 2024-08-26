# Running mysqltest by obd.sh

When using obd.sh to run the mysqltest test, you need to use the OceanBase database deployed through obd.sh. This article uses examples to introduce how to use obd.sh to deploy the OceanBase database and run the mysqltest test starting from compiling the source code.

## Background

In order to simplify the operating steps for developers and reduce their understanding costs, we encapsulate some OBD commands into the obd.sh script and store the script in the oceanbase/tools/deploy directory of the OceanBase source code. This article runs the mysqltest test by calling the [obd test mysqltest](https://www.oceanbase.com/docs/community-obd-cn-10000000002048173) commands in OBD.

## Concepts

mysqltest is a test in the OceanBase database access test. Simply put, it takes the written case file as input and compares the output of the database with the expected output. The cases tested by mysqltest in the OceanBase database are all located in the `tools/deploy/mysql_test` directory of the OceanBase source code.

`case` is the smallest execution unit of mysqltest. A `case` contains at least one test file and one result file. Classifying cases forms a `suite`, and a `suite` is a collection of cases.

When running the mysqltest test, it is divided into different modes according to the selected nodes. The common mysqltest modes are as follows.

* c mode: Connect to the server where the Primary Zone is located to run mysqltest. For example, use the configuration file distributed.yaml to deploy the cluster and then connect to server1 to run the test.

  ```shell
  ./obd.sh mysqltest -n <name> --suite acs --test-server=server1
  ```

* Slave mode: Connect to a server other than the Primary Zone to run mysqltest. For example, use the configuration file distributed.yaml to deploy the cluster and then connect to server2 to run the test.

  ```shell
  ./obd.sh mysqltest -n <name> --suite acs --test-server=server2
  ```

* Proxy mode: Connect to the cluster through ODP for mysqltest testing. For example, use the configuration file distributed-with-proxy.yaml to deploy the cluster and run the test.

  ```shell
  ./obd.sh mysqltest -n <name> --all
  ```

## Steps

### Step 1: Compile OceanBase database from source code

Please refer to [build-and-run](./build-and-run.md) to compile the OceanBase database from source code.

### Step 2: Run mysqltest test

You can choose to test in full or specify a `case` or `suite` for testing. For the specific meaning of parameters used when executing the obd.sh script, please refer to [Appendix](#Appendix).

* Full test, that is, run all suites in the `mysql_test/test_suite` directory, please refer to the following command.

  ```shell
  [admin@obtest ~]$ cd oceanbase/tools/deploy
  [admin@obtest deploy]$ ./obd.sh mysqltest -n test --all
  ```

* Specify case for testing, for example, specify `mysql_test/test_suite/alter/t/alter_log_archive_option.test`. Please refer to the following command.

  ```shell
  [admin@obtest ~]$ cd oceanbase/tools/deploy
  [admin@obtest deploy]$ ./obd.sh mysqltest -n test --test-dir ./mysql_test/test_suite/alter/t --result-dir ./mysql_test/test_suite/alter/r --test-set alter_log_archive_option
  ```

* To specify a suite test, for example, to execute a test on a specified suite in the `mysql_test/test_suite` directory, please refer to the following command.

  ```shell
  [admin@obtest ~]$ cd oceanbase/tools/deploy
  [admin@obtest deploy]$ ./obd.sh mysqltest -n test --suite acs
  ```

## Appendix

When executing the mysqltest test, you can configure some parameters according to the actual situation. The parameters are explained in the following table:

| Parameter Name | Required | Type | Default | Note |
|--------|---------|----------|-------|------|
| -n     | Y  | string | null | The cluster name. |
| --component | N  | string | null | The name of the component to be tested. Candidates are obproxy, obproxy-ce, oceanbase, and oceanbase-ce. When empty, checks are performed in the order obproxy, obproxy-ce, oceanbase, oceanbase-ce. If it is detected that the component exists, it will no longer be traversed, and the hit component will be used for subsequent testing. |
| --test-server | N  | string | The default is the first node in the server under the specified component | The machine to be tested can be set to the name value corresponding to the servers in the yaml file. If the name value is not configured after servers, the ip value will be used, which must be under the specified component. A certain node name. |
| --user | N  | string | admin | The username for executing the test, generally does not need to be modified.。 |
| --password | N | string | admin | Password |
| --database | N  | string | test | database |
| --mysqltest-bin | N  | string | /u01/obclient/bin/mysqltest | mysqltest binary file path. |
| --obclient-bin | N  | string | obclient | obclient binary file path. |
| --test-dir | N  | string | ./mysql_test/t | The directory where the test-file required by mysqltest is stored. If the test file cannot be found, it will try to find it in the OBD built-in. |
| --test-file-suffix | N  | string | .test | mysqltest 所需的 test-file 的后缀。 |
| --result-dir | N  | string | ./mysql_test/r | The directory where the result-file required by mysqltest is stored. If the result file is not found, it will try to find it in the OBD built-in. |
| --result-file-suffix | N  | string | .result | The suffix of result-file required by mysqltest. |
| --record | N  | bool | false | Only the execution results of mysqltest are recorded as record-file. |
| --record-dir | N  | string | ./record | The directory where the execution results of mysqltest are recorded. |
| --record-file-suffix | N  | string | .record | The suffix that records the execution results of mysqltest. |
| --tmp-dir | N  | string | ./tmp | tmpdir option for mysqltest. |
| --var-dir | N  | string | ./var | The log directory will be created under this directory and passed to mysqltest as the logdir option. |
| --test-set | N  | string | no | test case array. Use commas (`,`) to separate multiple cases. |
| --exclude | N  | string | no | The test case array needs to be excluded. Use commas (`,`) to separate multiple cases. |
| --test-pattern | N  | string | no | The regular expression that test filenames match. All cases matching the expression will override the test-set option. |
| --suite | N  | string | no | suite array. A suite contains multiple tests, which can be separated by commas (`,`). |
| --suite-dir | N  | string | ./mysql_test/test_suite | The directory where the suite directory is stored. If the suite directory is not found, it will try to find it in the OBD built-in. |
| --all | N  | bool | false |Execute all cases under --suite-dir. The directory where the suite directory is stored. |
| --need-init | N  | bool | false | Execute init sql file. A new cluster may need to execute some initialization files before executing mysqltest, such as creating the account and tenant required for the case. The directory where the suite directory is stored. Not enabled by default. |
| --init-sql-dir | N  | string | ./ | The directory where the init sql file is located. When the sql file is not found, it will try to find it in the OBD built-in. |
| --init-sql-files | N  | string | Default is empty | Array of init sql files to be executed when init is required. English comma (`,`) separation. If not filled in, if init is required, OBD will execute the built-in init according to the cluster configuration. |
| --auto-retry | N  | bool | false | Automatically redeploy the cluster and try again when it fails. |
| --psmall | N  | bool | false | Execute the case in psmall mode. |
| --slices | N  | int | null | The number of groups into which the cases to be executed will be divided. |
| --slice-idx | N  | int | null | Specify the current group id. |
| --slb-host | N  | string | null | Specify the soft load balancing center. |
| --exec-id | N  | string | null | Specify execution id. |
| --case-filter | N  | string | ./mysql_test/filter.py | The filter.py file maintains the cases that need to be filtered. |
| --reboot-timeout | N  | int | 0 | Restart timeout. |
| --reboot-retries | N  | int | 5 | Number of retries for failed restarts. |
| --collect-all | N  | bool | false | Whether to collect component logs. |
| --log-dir | N  | string | The default is log under tmp_dir | The log storage path of mysqltest. |
| --log-pattern | N  | string | *.log | Collect log file names matching the regular expression, and the hit files will be collected. |
| --case-timeout | N  | int | 3600 | mysqltest timeout for a single test. |
| --disable-reboot | N  | bool | false | No more restarts during test execution. |
| --collect-components | N  | string | null | Used to specify the components to be collected for logs. Multiple components are separated by commas (`,`). |
| --init-only | N  | bool | false | When true, only init SQL is executed. |
