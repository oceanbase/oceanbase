# Quick Start

This guide navigates you through the installation of OceanBase Database by taking standalone OceanBase Database as an example.

## Prerequisites

Your server has access to the Internet, and the software and hardware meet the following requirements:

| Item | Description |
| ---- | --- |
| System | Red Hat Enterprise Linux Server 7.2 Release with the Linux kernel version being 3.10.0 or later<br /> CentOS Linux 7.2 Release with the Linux kernel version being 3.10.0 or later |
| Memory | At least 16 GB<br /> 64 GB or larger is recommended |
| Disk type | SSDs are recommended |
| Disk space | At least 100 GB |
| File system | EXT4 or XFS |
| NIC | 1 Gbit/s or above |

> **NOTE:** The following description is based on a CentOS Linux 7.2 image in the x86 architecture. The installation procedure may be slightly different in other environments.

## Step 1: Download and install OBD

We recommend that you use OceanBase Deployer (OBD). It is the fastest tool for the deployment of OceanBase Database. Take the following steps to download and install OBD:

### Option 1: Install OBD by using Yum repositories

If your server has access to the Internet and supports third-party software from YUM repositories, you can run the following command to install OBD:

```bash
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo
sudo yum install -y ob-deploy
```

### Option 2: Install ODB offline

1. Download the RPM package of OBD.

2. Run the following command to install OBD.

   ```bash
   yum install -y ob-deploy-1.0.0-1.el7.x86_64.rpm
   source /etc/profile.d/obd.sh
   ```

## (Optional) Step 2: Download the OceanBase Database installation package

You may skip this step if your server has access to the Internet, because when you run the `obd cluster deploy` command, OBD checks whether the OceanBase Database installation package exists on your target server. If the package is not found, OBD automatically downloads it from YUM repositories.

If your server does not have Internet access, take the following steps to install the software offline:

1. Run the following command to download the OceanBase Database installation package:

   ```bash
   # Create a directory to store the package.
   mkdir rpm
   cat > rpm_list <<EOF
   oceanbase-ce-3.1.0-1.el7.x86_64.rpm
   oceanbase-ce-libs-3.1.0-1.el7.x86_64.rpm
   obproxy-3.1.0-1.el7.x86_64.rpm
   EOF

   wget -B https://mirrors.aliyun.com/oceanbase/community/stable/el/7/x86_64/ -i rpm_list -P rpm
   ```

   **NOTE:** The installation packages in the preceding example may not be the latest version. We recommend that you download the latest installation packages. For more information, see [OceanBase Database Releases](https://github.com/oceanbase/oceanbase/releases).

2. Run the following command to copy the package to your central control server:

   ```bash
   scp -r rpm <user>@<your-machine-ip>
   ```

3. Run the following command on the central control server to load the package to the local image:

   ```bash
   cd ~/rpm
   obd mirror clone *.rpm
   ```

## Step 3: Deploy OceanBase Database

This section describes how to use OBD to deploy OceanBase Database. Take the following steps:

1. Download the corresponding configuration template from GitHub. These are some sample configurations:

   - If you want to install OceanBase Database on your central control server, download the [local installation configuration file](https://github.com/oceanbase/obdeploy/blob/master/example/mini-local-example.yaml).
   - If you want to install OceanBase Database on a standalone server other than the central control server, download the [standalone installation configuration file](https://github.com/oceanbase/obdeploy/blob/master/example/mini-single-example.yaml).
   - If you want to install OceanBase Database on multiple servers other than the central control server, download the [distributed installation configuration file](https://github.com/oceanbase/obdeploy/blob/master/example/mini-distributed-example.yaml)
   > **NOTE:** The following example describes how to modify the configuration file in local installation mode. You must select the configuration template that corresponds to your installation mode.

2. If you want to install OceanBase Database on a server other than the central control server, you need to add relevant configurations for SSH remote access at the beginning of the configuration file.

   ```yaml
   user:
     username: <Your account name.>
     password: <Your logon password.>
     key_file: <The path of your private key.>
   ```

   `username` specifies the username used to log on to the target server. Make sure that your username has access to `home_path`.
   > **NOTE:** Generally, you can use only the password or the private key to log on. If you specify both of them, `password` is considered as the password of your private key.

3. (Optional) Modify the IP address in the configuration file.

   ```yaml
   oceanbase-ce:
     servers:
       - name: z1
       # Please don't use hostname, only IP can be supported
       ip: 11.166.80.01
   ```

4. Modify the `devname` and `home_path` variables in the configuration file.

   ```yaml
   oceanbase-ce:
     global:
       home_path: <your_observer_work_path>
       # Please set devname as the network adaptor's name whose ip is in the setting of severs.
       # if set severs as "127.0.0.1", please set devname as "lo"
       # if current ip is 192.168.1.10, and the ip's network adaptor'sname is "eth0", please use "eth0"
       devname: bond0
   ```

## Step 4: Connect to OceanBase Database

Take the following steps to deploy and start the OceanBase Database instance:

1. Run the following command to deploy the cluster:

   ```bash
   obd cluster deploy <deploy_name> -c <deploy_config_path>
   ```

   `deploy_name` specifies the name of the cluster. You can specify only one unique name for a cluster.

2. Run the following command to start the cluster:

   ```bash
   obd cluster start <deploy_name>
   ```

3. Run the following command to check the cluster status:

   ```bash
   obd cluster display <deploy_name>
   ```

## Step 5: Connect to OceanBase Database

Take the following steps:

1. Install the OceanBase Database client (OBClient):

   If you have added YUM repositories as the software source on your server, run the following command to install the OBClient:

   ```bash
   sudo yum install -y obclient
   ```

   Otherwise, you need to download the installation package to the server and run the following command to install it offline:

   ```bash
   sudo yum install -y obclient-2.0.0-1.el7.x86_64.rpm
   ```

   **NOTE:** The installation packages in the preceding example may not be the latest version. We recommend that you download the latest installation packages. For more information, see [OceanBase Download Center](https://github.com/oceanbase/oceanbase/releases).

2. Run the following command to connect to OceanBase Database from the OBClient:

   ```bash
   obclient -h<your_ip> -P<observer_mysql_port> -uroot
   ```

   `<your_ip>` specifies the IP address of the server where your OceanBase Database instance is located. By default, the `observer` connects to the OBClient through port `2883`. Use the actual port number if you have changed it.

   The following returned information indicates successful connection:

   ```bash
   Welcome to the MariaDB monitor.  Commands end with ; or \g.
   Your MySQL connection id is 3221546072
   Server version: 5.7.25 OceanBase 3.1.0 (r1-) (Built Apr  7 2021 08:14:49)

   Copyright (c) 2000, 2018, Oracle, MariaDB Corporation Ab and others.

   Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

   MySQL [(none)]>
   ```

## (Optional) Step 6: Create a tenant

For more information, see [Create a user tenant](https://open.oceanbase.com/docs/community/oceanbase-database/V3.1.0/create-a-user-tenant)
