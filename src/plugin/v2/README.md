# v2/ —— 新一代插件框架

legacy 的继替者。纯 C vtable 契约:每种插件一个独立 .so,OB 通过 dlopen +
dlsym 解析单入口符号拿到整张 vtable;宿主能力(内存/IO/执行器/日志)以
回调表(host API)在运行时注入插件——**不向插件导出任何 OB 符号**
(这是与 legacy 的 obp_* 导出符号机制的本质区别)。

## 目录

| 目录 | 作用 |
| --- | --- |
| include/ | 插件契约头,插件唯一需要包含的头。`ob_external_table_plugin.h`(插件要实现的 vtable + ABI 版本 + 入口符号)、`ob_ext_host_api.h`(OB 提供给插件的宿主 API:mem/executor/io/log 回调表)、`ob_external_table_protocol.h`(控制面协议词汇表:契约 errno + JSON key)。对应 legacy/include。与 `ob-deps/patch/paimon/ob_plugin/` 下同名文件内容必须保持一致,以本仓库为源 |
| host/ | 宿主能力的 OB 侧实现:mem pool(`ob_ext_mem_pool.*`)、只读文件系统(`ob_ext_file_system.*`)、malloc 标签守卫(`ob_ext_malloc_guard.h`)、HostApi 组装(`ob_ext_host_provider.*`,把通用能力装成契约的 `ObExtTableHostApi`)。对应 legacy/export |
| loader/ | 通用 dlopen 加载器(`ob_ext_plugin_loader.*`):`ext_plugin_config` 集群参数懒加载、SONAME `lib_ob_<name>.so` 约定(先把 `_ob_additional_lib_path` ensure 进 LD_LIBRARY_PATH 再搜索;后者支持 `:` 多目录)、ABI 版本检查、加载成功后常驻(从不 dlclose) |
| external_table/ | 外表插件类型的 OB 侧消费方:插件注册与槽位(`ob_ext_format_registry.*`)、控制面 JSON 编解码(`ob_ext_json_protocol.*` + `ob_ext_json_internal.h`)、schema JSON 解析(`ob_ext_schema_parser.*`)、协议类型→OB 类型映射(`ob_ext_type_mapper.*`)、表元数据(`ob_ext_table_metadata.*`)、契约 destroy 包装(`ob_ext_plugin_util.*`) |

## 与 legacy 的角色对应

| 角色 | legacy | v2 |
| --- | --- | --- |
| 插件可见头文件 | legacy/include | v2/include |
| OB 提供给插件的服务 | legacy/export(obp_* 符号导出) | v2/host(回调表注入) |
| 插件加载/管理 | legacy/sys | v2/loader |
| 插件类型的 OB 侧消费方 | legacy/adaptor + legacy/external_table | v2/external_table |

注:驱动外表插件做表扫描的执行器 row iter 属于 SQL 执行层,在
`src/sql/engine/table/`(`ob_ext_table_plugin_row_iter.*`),不在本目录。
