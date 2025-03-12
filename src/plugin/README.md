# plugin 代码结构说明

plugin 目录下放插件相关的管理代码、对内各插件的接口代码、对外插件接口和API代码、适配器代码和样例代码。

| 文件名 | 说明 |
| --- | --- |
| sys | 系统插件管理代码。比如初始化内置插件、加载动态链接库、插件查找等 |
| include/oceanbase | 对外插件接口和API代码 |
| interface | 对内各插件的接口代码 |
| adapter | 适配器代码。将对外的C插件接口适配到内部接口 |
| export | 插件API的实现代码。这里的代码在内部通常不会使用，编译时会被删除，所以这里使用了特殊的手段保留，参考 src/observer/CMakeLists.txt 关键字 export-dynamic-symbol |
