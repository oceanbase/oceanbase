# 什么是Runtime Profile？

[Runtime Profile](src/share/diagnosis/ob_runtime_profile.h) 用于记录query执行过程中的各项监控指标，可以按照json形式展示结果。
```json
{
"PHY_JOIN_FILTER":{
    "filtered row count":"60",
    "total row count":"100"
}
}
```
其内部由多个metric和子profile组成, metric记录当前profile下的的指标, sub_profile也是一组 Runtime Profile, 用于控制层级展开。

## 登记Profile信息
每个Profile有自己的名称和描述，需要在 `src/share/diagnosis/ob_profile_name_def.h` 中登记。

## 登记Metric信息
每个Metric有自己的描述，需要在 `src/share/diagnosis/ob_sql_monitor_statname.h` 中登记。


## 接入RuntimeProfile
1. 维护一个root profile, 并传入一个arena类的allocator, 如果在算子执行路径下, 不需要这一步
2. 在适当的位置使用 `ObProfileSwitcher` 将当前profile切换到你想要的层级
3. 使用 `REGISTER_METRIC`, `INC_METRIC_VAL`, `SET_METRIC_VAL` 等宏注册/更新Metric

## 如何查看RuntimeProfile
```sql
-- mysql/sys租户
select dbms_xprofile.display_profile('$trace_id');
-- oracle 租户
select * from table(dbms_xprofile.display_profile('$trace_id'));
```
详细的参数输入可以参考`src/share/inner_table/sys_package/dbms_xprofile.sql`文件中系统包的定义
