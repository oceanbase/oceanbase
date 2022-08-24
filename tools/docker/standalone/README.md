# How to use this image

You can deploy OceanBase databases by using many methods. But Docker is the easiest method. This repository gives you an oceanbase-standalone image for deploying OceanBase database by using Docker. oceanbase-standalone is a standalone test image for OceanBase Database. By default, this image deploys an instance of the largest size according to the current container. You can also deploy a mini standalone instance through the environment variable MINI_MODE.

## Prerequisite

Before you deploy oceanbase-standalone image, do a check of these:

- Make sure that your machine has at least 2 physical core and 10GB memory.
- Your machine has installed these applications:

    Application | Recommended version | Documentation
    ---     | ------  | -----
    Docker | Latest | [Docker Documentation](https://docs.docker.com/get-docker/)
- You have started the Docker service on your machine.

## Start an OceanBase instance

To start an OceanBase instance, run this command:

```bash
# deploy an instance of the largest size according to the current container
docker run -p 2881:2881 --name obstandalone -d oceanbase/oceanbase-ce-standalone

# deploy mini standalone instance
docker run -p 2881:2881 --name obstandalone -e MINI_MODE=1 -d oceanbase/oceanbase-ce-standalone
```

Two to five minutes are necessary for the boot procedure. To make sure that the boot procedure is successful, run this command:

```bash
$ docker logs obstandalone | tail -1
boot success!
```

**WARNING:** the container will not exit while the process of observer exits.

## Connect to an OceanBase instance

oceanbase-standalone image contains obclient (OceanBase Database client) and the default connection script `ob-mysql`.

```bash
docker exec -it obstandalone ob-mysql sys # Connect to sys tenant
docker exec -it obstandalone ob-mysql root # Connect to the root account of a general tenant
docker exec -it obstandalone ob-mysql test # Connect to the test account of a general tenant
```

Or you can run this command to connect to an OceanBase instance with your local obclient or MySQL client.

```bash
$mysql -uroot -h127.1 -P2881
```

When you connect to an OceanBase instance successfully, the terminal returns this message:

```mysql
Welcome to the MariaDB monitor.  Commands end with ; or \g.
Your MySQL connection id is 3221528373
Server version: 5.7.25 OceanBase 2.2.77 (r20211015104618-3510dfdb38c6b8d9c7e27747f82ccae4c8d560ee) (Built Oct 15 2021 11:19:05)

Copyright (c) 2000, 2018, Oracle, MariaDB Corporation Ab and others.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

MySQL [(none)]>
```

## Supported environment variables

This table shows the supported environment variables of the current oceanbase-standalone mirror version:

Variable name | Default value | Description
------- | ----- | ---
MINI_MODE | false | If ture, will use mini mode to deploy OceanBase Database instance, it should be used only for research/study/evaluation.  DO NOT use it for production or performance testing.
OB_HOME_PATH | /root/ob | Home path for an OceanBase Database instance.
OB_MYSQL_PORT | 2881 | The MySQL protocol port for an OceanBase Database instance.
OB_DATA_DIR | empty | The directory for data storage. The default value is $OB_HOME_PATH/store.
OB_REDO_DIR | empty | The directory for clog, ilog, and slog. The default value is the same as the OB_DATA_DIR value.
OB_RPC_PORT | 2882 | The RPC communication port for an OceanBase Database instance.
OB_ROOT_PASSWORD | empty |  The password for the system tenant in an OceanBase Database instance.
OB_CLUSTER_NAME | mini-ce | Instance name for OceanBase Database instance. OBD uses this value as its cluster name.
OB_TENANT_NAME | test | The default initialized general tenant name for an OceanBase Database instance.

## Run the Sysbench script

oceanbase-standalone image installs the Sysbench tool by default. And the Sysbench tool is configured. You can run these commands in sequence to run the Sysbench script with the default configurations.

```bash
docker exec -it obstandalone obd test sysbench [OB_CLUSTER_NAME]
```

## Mount Volumn
You can use `-v /host/path:/container/path` parameter in docker `run` command to save data in host os if you want to persistence the data of a container.

The docker image `oceanbase-ce` save the data to /root/ob directory default. You can not start a new docker image if you only bind the /root/ob directory, because the docker image oceanbase-ce use the [obd](https://github.com/oceanbase/obdeploy) to manage database clusters and there is no information about the database cluster after a new docker image started. So you should bind both the /root/ob and /root/.obd directory.

Below is an example.

```bash
docker run -d -p 2881:2881 -v $PWD/ob:/root/ob -v $PWD/obd:/root/.obd --name oceanbase oceanbase/oceanbase-ce
```

Note that you should use your own path.

You can view more information about `docker -v` at [docker volumn](https://docs.docker.com/storage/volumes/).
