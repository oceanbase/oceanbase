# ussl-hook

ussl-hook目的是提供一个动态库，通过动态库hook socket listen/accept/connect/read/write函数实现基本的认证。

## LD_PRELOAD usage
```
make ussl-hook.so
LD_PRELOAD=ussl-hook.so nc -l 8042
# in another terminal
LD_PRELOAD=ussl-hook.so nc 127.0.0.1 8042
```

## library usage
add ussl-hook.c to your CMakeLists.txt
```
#include "ussl-hook.h"
...
```
see `test/ussl-test.c`

## design doc


## configure files
config目录是ussl-cfg, 文件列表如下
```
$ ls ussl-cfg/
authorized  enabled  key
```
含义如下:
1. key文件保存的是自己的一个随机字符串，key文件有效则client会发送认证包。
2. authorized文件保存的是自己认可的key，当enabled=1时，server端会校验key是否在authorized列表里。
