# Installing OceanBase Database via yum/apt
If you want to deploy OceanBase on a Linux RPM platform, you can use yum/apt for single-node installation and simple management with systemd.

**WARNING**

- The installation method is just used for study or test;
- You should not deploy it with important data as it is not used in production environment.

## Installing OceanBase via yum
Config yum repo then install OceanBase, it will automatically install the required dependencies.
```bash
yum install -y yum-utils
yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo
yum install -y oceanbase-ce
```

## Installing OceanBase via apt
Config apt repo then install OceanBase, it will automatically install the required dependencies.
```bash
apt update
apt install -y lsb-release wget gnupg2 mysql-client curl
wget http://mirrors.oceanbase.com/oceanbase/oceanbase_deb.pub && apt-key add oceanbase_deb.pub
echo "deb http://mirrors.oceanbase.com/oceanbase/community/stable/$(lsb_release -is | awk '{print tolower($0)}')/$(lsb_release -cs)/$(dpkg --print-architecture)/ ./" | tee -a /etc/apt/sources.list.d/oceanbase.list
apt update
apt install -y oceanbase-ce
```

## Dependencies list：
| dependency | version |
|-------|-------|
| oceanbase-ce-libs | same version with oceanbase-ce |
| jq | / |
| oniguruma | / |
| curl | / |

# Startup Method
You can install and run OceanBase service using the following command:
```bash
systemctl start oceanbase
```
You can set the OceanBase service to start automatically on boot using the following command:
```bash
systemctl enable oceanbase
```

## Overview of systemd
Systemd provides automatic OceanBase startup and shutdown. It also enables manual server management using the systemctl command. For example:
```bash
systemctl {start|stop|restart|status} oceanbase
```

## Oceanbase configuration by systemd
Systemd provide `/etc/oceanbase.cnf` to config OceanBase before startup.
