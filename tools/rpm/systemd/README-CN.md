# 通过yum和systemd的方式安装OceanBase数据库
如果你想在linux rpm平台上部署oceanbase，可以使用yum进行单节点安装，并通过systemd进行简单管理

**注意**

- 该方法仅能用做学习研究或测试使用；
- 千万不要使用此方法用于带有重要数据的场景，比如生产环境。

## 安装方法
现在暂时只支持RPM平台系统，可以通过以下指令进行安装和启动：
```bash
yum install oceanbase
systemctl start oceanbase
```

## systemd介绍
Systemd提供了自动化管理oceanbase的启动和停止，可以通过systemctl指令对oceanbase进行管理控制，例如：
```bash
systemctl {start|stop|restart|status} oceanbase
```

## 通过systemd配置oceanbase
systemd提供了配置文件`/etc/oceanbase.cnf`,可以在启动前修改配置进行带参启动