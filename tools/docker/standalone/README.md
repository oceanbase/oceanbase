# How to deploy OceanBase with docker

OceanBase provide a standalone test image named [oceanbase-ce](https://hub.docker.com/r/oceanbase/oceanbase-ce) for OceanBase Database. By default, this image deploys a MINI_MODE OceanBase instance.

**WARNING**

- The oceanbase-ce docker image is just used for study or test;
- Please use [oceanbase-operator](https://github.com/oceanbase/ob-operator) instead if you want to deploy oceanbase in k8s;
- You should not deploy it with important data as it is not used in production environment.

Reasons:

1. The cluster contains only one instance, so there is no disaster tolerant ability;
2. It is very difficult to recover after failure because docker container cannot started while the oceanbase instance cannot start success, which means you lost the container and the data with it;
3. K8s can not restart a new pod because the container still exists after the observer process quit.

## Prerequisite

Before you deploy oceanbase-ce docker, do the following checks:

- Make sure that your machine has enough resource that can execute at least 2 phycical core and 8GB memory.
- Your machine has installed these applications:

    | Application | Recommended version | Documentation                                               |
    | ----------- | ------------------- | ----------------------------------------------------------- |
    | Docker      | Latest              | [Docker Documentation](https://docs.docker.com/get-docker/) |
- You have started the Docker service on your machine.

## Start an OceanBase instance

To start an OceanBase instance, run this command:

```bash
# deploy mini instance
docker run -p 2881:2881 --name oceanbase-ce -d oceanbase/oceanbase-ce

# deploy an instance of the slim size according to the current container
docker run -p 2881:2881 --name oceanbase-ce -e MODE=slim -e OB_MEMORY_LIMIT=5G -v {init_sql_folder_path}:/root/boot/init.d -d oceanbase/oceanbase-ce

# deploy an instance of the largest size according to the current container
docker run -p 2881:2881 --name oceanbase-ce -e MODE=normal -d oceanbase/oceanbase-ce

# deploy a quick-start instance in any mode as desired to the current container
docker run -p 2881:2881 --name oceanbase-ce -e FASTBOOT=true -d oceanbase/oceanbase-ce
```

Two to five minutes are necessary for the boot procedure. To make sure that the boot procedure is successful, run this command:

```bash
$ docker logs oceanbase-ce | tail -1
boot success!
```

After started, there is an oceanbase instance. You can connect to the oceanbase by root user without password.

## Connect to an OceanBase instance

oceanbase-ce image contains obclient (OceanBase Database client) and the default connection script `ob-mysql`.

```bash
docker exec -it oceanbase-ce ob-mysql sys # Connect to sys tenant
docker exec -it oceanbase-ce ob-mysql root # Connect to the root account of a general tenant
docker exec -it oceanbase-ce ob-mysql test # Connect to the test account of a general tenant
```

Or you can run this command to connect to an OceanBase instance with your local obclient or MySQL client.

```bash
mysql -uroot -h127.1 -P2881
```

## Supported environment variables

This table shows the supported environment variables of the current oceanbase-ce mirror version:

| Variable name    | Default value | Description                                                  |
| ---------------- | ------------- | ------------------------------------------------------------ |
| MODE             | {mini, slim, normal} | If it is mini, then the docker use mini mode to deploy OceanBase Database instance, it should be used only for research/study/evaluation.  DO NOT use it for production or performance testing. If it is slim, then the docker can run in a smaller instance. It remove the obagent and can run a self tenant initial sql by yourself in the mount volume /root/boot/init.d. If you do not mount the volume path the docker does not init the tenant sql. |
| FASTBOOT         | false         | The container can run in a quick-start mode when FASTBOOT=true. |
| EXIT_WHILE_ERROR | true          | Whether quit the container while start observer failed. If start observer failed, you can not explore the logs as the container will exit. But if you set the EXIT_WHILE_ERROR=false, the container will not exit while observer starting fail and you can use docker exec to debug. |
| OB_CLUSTER_NAME         | obcluster  | The oceanbase cluster name |
| OB_TENANT_NAME          | test       | The oceanbase mysql tenant name |
| OB_MEMORY_LIMIT         | 6G         | The oceanbase cluster memory_limit configuration |
| OB_DATAFILE_SIZE        | 5G         | The oceanbase cluster datafile_size configuration |
| OB_LOG_DISK_SIZE        | 5G         | The oceanbase cluster log_disk_size configuration |
| OB_ROOT_PASSWORD        |            | The oceanbase root user password of sys tenant |
| OB_SYSTEM_MEMORY        | 1G         | The oceanbase cluster system_memory configuration |
| OB_TENANT_MINI_CPU      |            | The oceanbase tenant mini_cpu configuration |
| OB_TENANT_MEMORY_SIZE   |            | The oceanbase tenant memory_size configuration |
| OB_TENANT_LOG_DISK_SIZE |            | The oceanbase tenant log_disk_size configuration |

## Run the Sysbench script

oceanbase-ce image installs the Sysbench tool by default. And the Sysbench tool is configured. You can run these commands in sequence to run the Sysbench script with the default configurations.

```bash
docker exec -it oceanbase-ce obd test sysbench obcluster
```

## Mount Volume
You can use `-v /your/host/path:/container/path` parameter in docker `run` command to save data in host os if you want to persistence the data of a container.

Below is an example.

```bash
docker run -d -p 2881:2881 -v $PWD/ob:/root/ob -v $PWD/obd:/root/.obd --name oceanbase oceanbase/oceanbase-ce
```

Note that you should use your own path.

The docker image `oceanbase-ce` saves the data to /root/ob directory default. You should bind both the /root/ob and /root/.obd. You can not start new docker image if you only bind the /root/ob directory, because the docker image oceanbase-ce uses the [obd](https://github.com/oceanbase/obdeploy) to manage database clusters and there is no information about the database cluster in a new docker container.

You can view more information about `docker -v` at [docker volume](https://docs.docker.com/storage/volumes/).

## Fast boot image building for a standalone node
The `docker_build.sh` script is provided in the `tools/docker/standalone` directory, through which the fast boot image can be built. Before running the script, please first modify the `tools/docker/standalone/boot/_env` environment configuration script:

- Optional: Modify the remaining configuration items

After the modification is completed, execute the image build script:

- build the image with latest oceanbase version `./docker_build.sh`
- build the image with specific oceanbase version `./docker_build.sh <oceanbase_rpm_version>`. For example `./docker_build.sh 4.2.1.0-100000102023092807`

After waiting for the build to be completed, you can start and test the instance in the same way as mentioned above.

## Fault Diagnosis
A series of diagnostic methods are provided to diagnose errors in Docker.
### Support for 'enable_rich_error_msg' parameter
- Initially, the 'enable_rich_error_msg' parameter is enabled by default during the Docker startup process. If an error occurs during the startup process, more error information can be obtained using the trace command. After a successful startup, Docker sets this parameter to the false state.
- Users can open this parameter to obtain more error information about SQL statements during the runtime phase. The method to open it is to connect to Docker's oceanbase using the system tenant, and then execute the following command:
```bash
alter system set enable_rich_error_msg = true;
```
