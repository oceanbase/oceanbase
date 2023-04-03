# Description

The latest dooba stable version is 1.0

dooba is a easy tools monitoring oceanbase cluster for oceanbase admins. It's based on python curses library, and is a powerful tool for watching oceanbase cluster status with straightfoward vision.

dooba's gui is somewhat inspired by many other curses programes such as ncmpcpp, htop, wicd-curses, weechat-curses and so on.

Many other features will be involved in dooba in not so long days. Here is a simple list.

# Features

1. select dataid and cluster with list
2. auto resize widget size to adapt term size
3. monitor sql, cs, ups statistics, for each cluster
4. simple way ssh into any server, connection by mysql client
5. support multi-cluster, simple switch shortkey - 'c'
6. machine info monitor, must online environment
7. many other shortcut keys for widget

# The Next Step

1. server errors/warns preview, with this DBAs have no need ssh in some servers to look for which there're some errors for that server.
2. server manipulation: change log level, stop server and so on
3. integrate with deploy which is a powful dev tool written by Yuanqi

# How to use it

```sh
dooba -h <LMS_HOST> -p <LMS_PORT>
dooba --dataid=DATA_ID
dooba -?
dooba
```

# Changelogs

Please look into dooba script file header.

# Screenshot

## version 1.0

### dooba offline index

![screenshot v1.0](screenshot/v1_0-offline.png "screenshot for dooba v1_0 offline index")

### dooba machine

![screenshot v1.0](screenshot/v1_0-machine.png "screenshot for dooba v1_0 machine")

### dooba help

![screenshot v1.0](screenshot/v1_0-help.png "screenshot for dooba v1_0 help")

## version 0.4

### dooba shell

![screenshot v0.4](screenshot/v0_4-shell.png "screenshot for dooba v0.2 gallery")

### dooba sql

![screenshot v0.4](screenshot/v0_4-sql.png "screenshot for dooba v0.2 gallery")

### dooba UpdateServer

![screenshot v0.4](screenshot/v0_4-ups.png "screenshot for dooba v0.2 gallery")

### dooba ChunkServer

![screenshot v0.4](screenshot/v0_4-cs.png "screenshot for dooba v0.2 gallery")

## version 0.2, 0.3

### Gallery of OceanBase

![screenshot v0.2](screenshot/v0_2-gallery.png "screenshot for dooba v0.2 gallery")

### SQL of OceanBase

![screenshot v0.2](screenshot/v0_2-sql.png "screenshot for dooba v0.2 sql")

### UpdateServer of OceanBase

![screenshot v0.2](screenshot/v0_2-ups.png "screenshot for dooba v0.2 UpdateServer")

### ChunkServer of OceanBase

![screenshot v0.2](screenshot/v0_2-cs.png "screenshot for dooba v0.2 ChunkServer")

## version 0.1

![screenshot v0.1](screenshot/v0_1.png "screenshot for dooba v0.1")
