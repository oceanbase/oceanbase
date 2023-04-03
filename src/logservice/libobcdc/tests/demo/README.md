# OBCDC Usage
We provide an example to show how to use libobcdc.so.

## prepare
This step to prepare libobcdc runtime env.
1. prepare your program, here assume as `${USER_PROGRAM_DIR}`, and copy `tests/demo/obcdc_demo.cpp` to `${USER_PROGRAM_DIR}`
2. Unzip oceanbase-cdc rpm by command like `rpm2cpio oceanbase-cdc-xxx.rpm | cpio -idv`
3. Copy include dir and lib dir to `${USER_PROGRAM_DIR}`
4. Prepare a config file (conf/libobcdc.conf) for libobcdc, which contains at least these configs:
- `cluster_url` or `rootserver_list` as entrance of source OceanBase cluster
- `cluster_user` and `cluster_password` as certificate to login OceanBase cluster, notice: `cluster_user` should belongs to sys tenant and `cluster_password` should be corresponding password for `cluster_user`

> NOTE: if you are using liboblog.so compiled by yourself with source code, please use release compile mode(command like `./build.sh realse --init --make`)

## compile
### use clang
`clang++  obcdc_demo.cpp -o obcdc_demo -I include/ -lobcdc -Llib -std=c++11 -fpic`
### use gcc
`gcc -c obcdc_demo.cpp -I include/ -std=c++11 -fpic`
`gcc -o obcdc_demo -lobcdc -Llib`

> NOTE: must use -fpic while compial your code.
