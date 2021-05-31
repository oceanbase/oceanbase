# 如何执行MySQLtest

## 名词解释
- OBD：[OceanBase Deploy](https://github.com/oceanbase/obdeploy)的简称。
- case：执行MySQLtest的最小单位，一个case至少包含一个test文件和一个result文件
- suite：一组case的集合，称为suite


## 启动测试

#### __启动测试前，你需要先使用OBD启动一个集群。[使用OBD快速启动OceanBase数据库](https://github.com/oceanbase/obdeploy#%E5%BF%AB%E9%80%9F%E5%90%AF%E5%8A%A8-oceanbase-%E6%95%B0%E6%8D%AE%E5%BA%93)__

### 全量测试

执行mysql_test/test_suite下全部的测试，可以使用以下命令：

```shell
obd test mysqltest <deploy name> --all --auto-try
# 如果执行obd命令时不处于当前目录请使用 --suite-dir 选项
# 一些case中包含是相当路径，不在当前目录执行可能会导致失败
```

#### 参数与选项解释
参数 `deploy name` 为部署配置名称，可以理解为配置文件名称。

选项 `--suite-dir` 为 suite 所在目录，默认为 ./mysql_test/test_suite

选项 `--all` 为执行 `--suite-dir` 下全部suite。

选项 `--auto-try` 开启后，当case第一次执行失败时会自动重部署集群进行重试。

### 单suite测试

对mysql_test/test_suite下指定suite执行测试，可以使用以下命令：

```shell
obd test mysqltest <deploy name> --suite <suite_name>  --auto-try
# 如果执行obd命令时不处于当前目录请使用 --suite-dir 选项
# 一些case中包含是相当路径，不在当前目录执行可能会导致失败
```

#### 参数与选项解释
参数 `deploy name` 为部署配置名称，可以理解为配置文件名称。

选项 `--suite-dir` 为 suite 所在目录，默认为 ./mysql_test/test_suite

选项 `--suite` 为要执行suite的集合，多个suite之间使用英文逗号`,`间隔

选项 `--auto-try` 开启后，当case第一次执行失败时会自动重部署集群进行重试。

### 单case测试

对特定case进行测试，比如对mysql_test/test_suite/alter/t/alter_log_archive_option.test进行测试，可以使用以下命令：
```shell
obd test mysqltest <deploy name> --test-dir ./mysql_test/test_suite/alter/t --result-dir ./mysql_test/test_suite/alter/r --test-set alter_log_archive_option --auto-try
# 如果执行obd命令时不处于当前目录请调整 --test-dir 和 --result-dir
# 一些case中包含是相当路径，不在当前目录执行可能会导致失败
```

#### 参数与选项解释
参数 `deploy name` 为部署配置名称，可以理解为配置文件名称。

选项 `--test-dir` 为 test文件 所在目录，默认为 ./t

选项 `--result-dir` 为 result文件 所在目录，默认为 ./r

选项 `--test-set` 为要执行case的集合，多个case之间使用英文逗号`,`间隔

选项 `--auto-try` 开启后，当case第一次执行失败时会自动重部署集群进行重试。


