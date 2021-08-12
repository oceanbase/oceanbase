# How to use this image

You can deploy OceanBase databases by using many methods. But Docker is the easiest method. This repository gives you an obce-mini image for deploying OceanBase database by using Docker. obce-mini is a mini standalone test image for OceanBase Database Community Edition. You can use it only for research/study/evaluation.  DO NOT use it for production or performance testing.

## Prerequisite

Before you deploy obce-mini image, do a check of these:

- Make sure that your machine has at least 2 physical core and 10GB memory.
- Your machine has installed these applications:

    Application | Recommended version | Documentation
    ---     | ------  | -----
    Docker | Latest | [Docker Documentation](https://docs.docker.com/get-docker/)
- You have started the Docker service on your machine.

## Start an OceanBase instance

To start an OceanBase instance, run this command:

```bash
docker run -p 2881:2881 --name some-obce -d oceanbase/obce-mini
```

Two to five minutes are necessary for the boot procedure. To make sure that the boot procedure is successful, run this command:

```bash
$ docker logs some-obce | tail -1
boot success!
```

## Connect to an OceanBase instance

obce-mini image contains obclient (OceanBase Database client) and the default connection script `ob-mysql`.

```bash
docker exec -it some-obce ob-mysql sys # Connect to sys tenant
docker exec -it some-obce ob-mysql root # Connect to the root account of a general tenant
docker exec -it some-obce ob-mysql test # Connect to the test account of a general tenant
```

Or you can run this command to connect to an OceanBase instance with your local obclient or MySQL client.

```bash
$mysql -uroot -h127.1 -P2881
```

When you connect to an OceanBase instance successfully, the terminal returns this message:

```mysql
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 167310
Server version: 5.7.25 OceanBase 3.1.0 (r-00672c3c730c3df6eef3b359eae548d8c2db5ea2) (Built Jun 22 2021 12:46:28)

Copyright (c) 2000, 2021, Oracle and/or its affiliates.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql>
```

## Supported environment variables

This table shows the supported environment variables of the current obce-mini mirror version:

Variable name | Default value | Description
------- | ----- | ---
OB_HOME_PATH | /root/ob | Home path for an OceanBase Database instance.
OB_MYSQL_PORT | 2881 | The MySQL protocol port for an OceanBase Database instance.
OB_RPC_PORT | 2882 | The RPC communication port for an OceanBase Database instance.
OB_ROOT_PASSWORD | empty |  The password for the system tenant in an OceanBase Database instance.
OB_CLUSTER_NAME | mini-ce | Instance name for OceanBase Database instance. OBD uses this value as its cluster name.
OB_TENANT_NAME | test | The default initialized general tenant name for an OceanBase Database instance.

## Run the Sysbench script

obce-mini image installs the Sysbench tool by default. And the Sysbench tool is configured. You can run these commands in sequence to run the Sysbench script with the default configurations.

```bash
docker exec -it some-obce sysbench cleanup # Clean the data
docker exec -it some-obce sysbench prepare # Prepare the data
docker exec -it some-obce sysbench run # Do the test

docker exec -it some-obce sysbench # Run the preceding three commands in order
```

