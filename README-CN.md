# 什么是 OceanBase 数据库

OceanBase 是蚂蚁集团历经12年完全自主研发的企业级原生分布式数据库，始于2010年，已连续 8 年稳定支撑双 11，创新推出“三地五中心”城市级容灾新标准，是全球唯一在 TPC-C 和 TPC-H 测试上都刷新了世界纪录的国产原生分布式数据库。产品采用自研的一体化架构，兼顾分布式架构的扩展性与集中式架构的性能优势，用一套引擎同时支持 TP 和 AP 的混合负载，具有数据强一致、高可用、高性能、在线扩展、高度兼容 SQL 标准和主流关系数据库、对应用透明，高性价比等特点。

OceanBase 数据库支持蚂蚁集团的全部核心业务，以及银行、保险、证券、运营商等多个行业的数百个客户的核心业务系统。

OceanBase 数据库具有如下特点：

- 稳定可信赖

    经历双 11 共 9 年的验证，蚂蚁集团百万规模使用，单服务器故障自愈，跨城多机房容灾，数据多副本存储，创新推出“三地五中心”城市级容灾新标准，最高可达金融 6 级标准（RPO=0，RTO<=30 秒）。
- 海量数据高性能

    海量数据下， 支持在线事务处理 OLTP 和在线分析处理 OLAP 业务线性扩展， 并相互隔离。TPC-C 以 6000 万数据量下获得 7.07 亿 tpmC， TPC-H 获得 1526 万 QphH @30000GB 。
- 大幅提升 KV 能力

    支持 OBKV 能力，提供 HBase 模型和 Table 模型的 NoSQL 能力。二级索引下 OBKV 性能指数级提升，有效避免 HBase 性能抖动问题。
- 更低的存储和运维成本

    支持部署运行在 PC 服务器和低端 SSD，高存储压缩率降低存储成本，无中心化设计和多租户混部大幅提升计算资源利用率。支持主流生态产品（Prometheus、Canal 等）和一体化设计让运维更轻松。
- 兼容 MySQL 开源生态

    与 MySQL生态高度兼容，提供数据库全生命周期的工具产品，组件化架构全面开放生态，从开发调试到生产运维及数据传输全方位护航。


## 快速上手

查看 [快速使用指南](https://open.oceanbase.com/quickStart) 开始试用 OceanBase 数据库。

## 系统架构
## ![image.png](https://gw.alipayobjects.com/mdn/ob_asset/afts/img/A*kMfJS7VVN70AAAAAAAAAAAAAARQnAQ)

## Roadmap

![image.png](https://cdn.nlark.com/yuque/0/2022/png/106206/1660643534293-a5f53258-a9ac-462c-b9fd-9832901853c2.png#clientId=u5d9acd3a-25ad-4&crop=0&crop=0&crop=1&crop=1&from=paste&height=352&id=u0f71535e&margin=%5Bobject%20Object%5D&name=image.png&originHeight=528&originWidth=1683&originalType=binary&ratio=1&rotation=0&showTitle=false&size=719961&status=done&style=none&taskId=u676e97dd-1309-42b9-b380-a423c27199c&title=&width=1122)

Link: [3.1.5 function list](https://github.com/oceanbase/oceanbase/milestone/6)

## 用户案例

更多用户案例, 请参考 [用户案例](https://www.oceanbase.com/customer/home).

## 文档

- [简体中文](https://open.oceanbase.com/docs)
- [English](https://www.oceanbase.com/en/docs)



## 许可证

OceanBase 数据库使用 [MulanPubL - 2.0](http://license.coscl.org.cn/MulanPubL-2.0) 许可证。您可以免费复制及使用源代码。当您修改或分发源代码时，请遵守木兰协议。



## OceanBase开发者手册
OceanBase 社区热情欢迎每一位对数据库技术热爱的开发者，期待携手开启思维碰撞之旅。无论是文档格式调整或文字修正、Bug 问题修复还是增加新功能，都是对 OceanBase 社区参与和贡献方式之一，立刻开启您的 First Contribution 吧！

1. [如何编译OceanBase源码](https://github.com/oceanbase/oceanbase/wiki/how_to_build)
2. [如何设置IDE开发环境](https://github.com/oceanbase/oceanbase/wiki/how_to_setup_ide)
3. [如何成为OceanBase Contributor](https://github.com/oceanbase/oceanbase/wiki/how_to_contribute)
4. [如何修改OceanBase文档](https://github.com/oceanbase/oceanbase/wiki/how_to_modify_docs)
5. [如何debug OceanBase](https://github.com/oceanbase/oceanbase/wiki/how_to_debug)
6. [如何运行测试](https://github.com/oceanbase/oceanbase/wiki/how_to_test)
7. [如何修bug](https://github.com/oceanbase/oceanbase/wiki/how_to_fix_bug)


## 获取帮助

如果您在使用 OceanBase 数据库时遇到任何问题，欢迎通过以下方式寻求帮助：

- [GitHub Issue](https://github.com/oceanbase/oceanbase/issues)
- [官方网站](https://open.oceanbase.com/)
- [English](https://oceanbase.com/en)


## Community

 - [论坛](https://open.oceanbase.com/answer)
 - [钉钉群 33254054](https://h5.dingtalk.com/circle/healthCheckin.html?corpId=ding12cfbe0afb058f3cde5ce625ff4abdf6&53108=bb418&cbdbhh=qwertyuiop&origin=1)
 - [微信 OBCE666](https://gw.alipayobjects.com/zos/oceanbase/0a69627f-8005-4c46-be1f-aac7a2b85c13/image/2022-03-01/85d42796-4e22-463a-9658-57402d7b9bc3.png)
 - [oceanbase.slack](https://oceanbase.slack.com/)



