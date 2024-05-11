# OceanBase 开发者手册

## 介绍

* **面向人群** 手册的目标受众是OceanBase的贡献者，无论是新人还是老手。
* **目标** 手册的目标是帮助贡献者成为OceanBase的专家，熟悉其设计和实现，从而能够在现实世界中流畅地使用它以及深入开发OceanBase本身。

## 手册结构

当前，手册由以下部分组成：

1. **开始**: 设置开发环境，构建和连接到OceanBase服务器，子部分基于一个想象的新手用户旅程。
    1. [安装工具链](toolchain.md)
    2. [获取代码，编译运行](build-and-run.md)
    3. [配置IDE](ide-settings.md)
    4. [编程惯例](coding-convension.md)
    5. [编写并运行单元测试](unittest.md)
    6. [运行MySQL测试](mysqltest.md)
    7. [调试](debug.md)
    8. 提交代码和Pull Request

2. **OceanBase设计和实现**: 介绍了OceanBase的设计和实现细节，这些细节对于理解OceanBase的工作原理至关重要。
    在开始编写稍大的功能之前，你应该阅读以下内容，它可以帮助你更好地理解OceanBase。
   
    1. [日志系统](logging.md)
    2. [内存管理](memory.md)
    3. [基础数据结构](container.md)
    4. [架构](architecture.md)
    5. [编程规范](coding_standard.md)

## 用户文档

本手册不包含用户文档。

可以参考 [oceanbase-doc](https://github.com/oceanbase/oceanbase-doc) 查看用户文档。
