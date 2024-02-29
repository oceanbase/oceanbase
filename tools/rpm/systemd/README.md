# Installing OceanBase Database via yum and systemd
If you want to deploy OceanBase on a Linux RPM platform, you can use yum for single-node installation and simple management with systemd.

**WARNING**

- The installation method is just used for study or test;
- You should not deploy it with important data as it is not used in production environment.

## Installation Method
Now systemd only support RPM platform and you can install and run oceanbase service using the following command:
```bash
yum install oceanbase-ce
systemctl start oceanbase
```
You can set the OceanBase service to start automatically on boot using the following command:
```bash
systemctl enable oceanbase
```

## Overview of systemd
Systemd provides automatic oceanbase startup and shutdown. It also enables manual server management using the systemctl command. For example:
```bash
systemctl {start|stop|restart|status} oceanbase
```

## Oceanbase configuration by systemd
Systemd provide `/etc/oceanbase.cnf` to config oceanbase before startup.
