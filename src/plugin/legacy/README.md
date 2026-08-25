# legacy/ —— 通用插件框架(兼容维护中)

OB 最早的插件体系:插件以 .so 形式独立编译,只依赖 `include/oceanbase/`
下的 SDK 头;运行时由 observer dlopen 加载,插件对 `obp_*` 符号的未定义
引用由 observer 主程序通过 `-Wl,--export-dynamic-symbol=obp_*` 导出的动态
符号表解析(参考 src/observer/CMakeLists.txt;`export/` 下的实现也靠这个
机制保留不被裁剪)。

## 目录

| 目录 | 作用 |
| --- | --- |
| include/oceanbase/ | 对外公开的 C 插件 SDK,外部插件只应依赖这里的头文件 |
| interface/ | 各类插件的内部接口描述(ftparser/kms/外部表等插件类型的内部调用模型) |
| sys/ | 插件管理:内置插件初始化、动态库加载(dl handle / entry handle)、插件查找 |
| adaptor/ | 适配器:把对外的 C 插件接口适配到 OB 内部接口(plugin / ftparser / kms) |
| export/ | 插件 API 的 OB 侧实现(obp_* 符号:allocator / charset / log / ftparser / kms)。内部不使用,靠 export-dynamic-symbol 保留 |
| share/ | 共享工具(ObProperties 等) |
| external_table/ | 内建外部表插件(java/odps 等 JNI 数据源,OB 管 schema) |

## 与 v2 的分工

新插件类型一律进 `v2/`(纯 C vtable + 回调表注入,无导出符号);
legacy 仅兼容维护,不再扩展。两个体系的角色对应关系见 `v2/README.md`。
