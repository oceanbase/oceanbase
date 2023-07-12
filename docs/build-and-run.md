# Get the code, build and run

## Prerequisites

Check the [Install toolchain](toolchain.md) guide for supported OS, GLIBC version requirement, and how to install the C++ toolchain.

## Clone

Clone the source code to your development machine:

```shell
git clone https://github.com/oceanbase/oceanbase.git
```

## Build

Build OceanBase from the source code in debug mode or release mode:

### Debug mode

```shell
bash build.sh debug --init --make
```

### Release mode

```shell
bash build.sh release --init --make
```

## Run

Now that you built the `observer` binary, you can deploy a OceanBase instance with the `obd.sh` utility:

```shell
./tools/deploy/obd.sh prepare -p /tmp/obtest
./tools/deploy/obd.sh deploy -c ./tools/deploy/single.yaml
```

This starts the OceanBase server listening on port 10000.

## Connect

You can use the official MySQL client to connect to OceanBase:

```shell
mysql -uroot -h127.0.0.1 -P10000
```

Alternatively, you can use the `obclient` to connect to OceanBase:

```shell
./deps/3rd/u01/obclient/bin/obclient -h127.0.0.1 -P10000 -uroot -Doceanbase -A
```

## Shutdown

You can run the following command to shut down the server and clean up the deployment, which prevents disk consuming:

```shell
./tools/deploy/obd.sh destroy --rm -n single
```
