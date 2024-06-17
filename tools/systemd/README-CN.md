# 通过软件源安装 OceanBase 数据库
如果你想在Linux rpm或deb平台上部署OceanBase，可以使用yum或apt进行单节点安装，并通过systemd进行简单管理

**注意**

- 该方法仅能用做学习研究或测试使用；
- 千万不要使用此方法用于带有重要数据的场景，比如生产环境。

## 通过 YUM 仓库安装 OceanBase
配置yum源，安装OceanBase，会自动安装所需依赖
```bash
yum install -y yum-utils
yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo
yum install -y oceanbase-ce
```

## 通过 APT 仓库安装 OceanBase
配置apt源，安装OceanBase，会自动安装所需依赖
```bash
apt update
apt install -y lsb-release wget gnupg2 mysql-client curl
wget http://mirrors.oceanbase.com/oceanbase/oceanbase_deb.pub && apt-key add oceanbase_deb.pub
echo "deb http://mirrors.oceanbase.com/oceanbase/community/stable/$(lsb_release -is | awk '{print tolower($0)}')/$(lsb_release -cs)/$(dpkg --print-architecture)/ ./" | tee -a /etc/apt/sources.list.d/oceanbase.list
apt update
apt install -y oceanbase-ce
```

## 依赖列表说明：
| 组件 | 版本 |
|-------|-------|
| oceanbase-ce-libs | 与oceanbase-ce相同版本 |
| jq | / |
| oniguruma | / |
| curl | / |

# 启动方法
可以通过以下指令进行启动：
```bash
systemctl start oceanbase
```

可以通过以下指令将oceanbase服务设置为开机自启动：
```bash
systemctl enable oceanbase
```

## systemd介绍
Systemd提供了自动化管理oceanbase的启动和停止，可以通过systemctl指令对oceanbase进行管理控制，例如：
```bash
systemctl {start|stop|restart|status} oceanbase
```

## 通过systemd配置OceanBase
systemd提供了配置文件`/etc/oceanbase.cnf`,可以在启动前修改配置进行带参启动
