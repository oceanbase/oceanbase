# Description

The latest dooba stable version is 1.0

dooba is a easy tools monitoring oceanbase cluster for oceanbase admins. It's based on python curses library, and is a powerful tool for watching oceanbase cluster status with straightforward vision.

dooba's gui is somewhat inspired by many other curses programs such as ncmpcpp, htop, wicd-curses, weechat-curses and so on.

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
