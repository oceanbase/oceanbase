# plugin 代码结构说明

plugin 目录下是 OB 的两套插件体系,分为 `legacy/` 和 `v2/` 两个目录,
各目录内的详细结构见各自的 README。

| 目录 | 说明 |
| --- | --- |
| legacy/ | 最早的通用插件框架(兼容维护中):插件只依赖 include/ SDK,回调 OB 靠 `obp_*` 导出符号。详见 [legacy/README.md](legacy/README.md) |
| v2/ | 新一代插件框架:纯 C vtable 契约,宿主能力经回调表注入,无导出符号;外表格式插件是首个插件类型。详见 [v2/README.md](v2/README.md) |

注:驱动外表插件做表扫描的执行器 row iter 属于 SQL 执行层,统一放在
`src/sql/engine/table/` 下(与其他格式 row iter 并列):
`ob_ext_table_plugin_row_iter.*`(v2 外表契约)和
`ob_ext_table_java_plugin_row_iter.*`(legacy 内建插件),均不在本目录。
