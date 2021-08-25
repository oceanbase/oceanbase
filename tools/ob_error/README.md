# What is ob_error

ob_error is the error tool of OceanBase.

An error tool can return the information, reason and solution corresponding to the error code which entered by the user.

It saves the trouble of looking up documents.

## How to build

### debug mode

```shell
bash build.sh debug --init
cd build_debug
make ob_error
cp tools/ob_error/src/ob_error /usr/local/bin
```

`ob_error` will generated in `DEBUG_BUILD_DIR/tools/ob_error/src/ob_error` by default.

### release mode

```shell
bash build.sh release --init
cd build_release
make ob_error
cp tools/ob_error/src/ob_error /usr/local/bin
```

`ob_error` will generated in `RELEASE_BUILD_DIR/tools/ob_error/src/ob_error` by default.

### RPM packges

NOTE: this is not support now.

```shell
bash build.sh rpm --init && cd build_rpm && make -j16 rpm
rpm2cpio oceanbase-ce-3.1.0-1.alios7.x86_64.rpm | cpio -idmv ./home/admin/oceanbase/bin/ob_error
cp home/admin/oceanbase/bin/ob_error /usr/local/bin
```

`ob_error` will included in `oceanbase-ce-3.1.0-1.alios7.x86_64.rpm`.
Because you just need `ob_error`, you can use `rpm2cpio` to do it.

## How to use

You can search error message by only enter the error code.

Then you will get the error message corresponding to OS, Oracle and MySQL modes, and OceanBase own error (if any exists).

such as:

```shell
$ob_error 4001

OceanBase:
    OceanBase Error Code: OB_OBJ_TYPE_ERROR(-4001)
    Message: Object type error
    Cause: Internal Error
    Solution: Contact OceanBase Support

Oracle:
    Oracle Error Code: ORA-04001
    Message: sequence parameter must be an integer
    Related OceanBase Error Code:
        OB_ERR_SEQ_OPTION_MUST_BE_INTEGER(-4317)
```

Also, you can search error message of specific mode by adding a prefix (we called facility).

When the facility is `my`, if the error code is not an error which exists in MySQL, you will get the OceanBase error info(if it exists). Otherwise, you will get the error info of MySQL mode.

such as:

```shell
$ob_error my 4000

OceanBase:
    OceanBase Error Code: OB_ERROR(-4000)
    Message: Common error
    Cause: Internal Error
    Solution: Contact OceanBase Support

$ob_error my 1210

MySQL:
    MySQL Error Code: 1210 (HY000)
    Message: Invalid argument
    Message: Miss argument
    Message: Incorrect arguments to ESCAPE
    Related OceanBase Error Code:
        OB_INVALID_ARGUMENT(-4002)
        OB_MISS_ARGUMENT(-4277)
        INCORRECT_ARGUMENTS_TO_ESCAPE(-5832)
```

When the facility is `ora` or `pls`, you will get the error info of Oracle mode(if it exists).
such as:

```shell
$ob_error ora 51

Oracle:
    Oracle Error Code: ORA-00051
    Message: timeout occurred while waiting for a resource
    Related OceanBase Error Code:
        OB_ERR_TIMEOUT_ON_RESOURCE(-5848)
```

Further more, there is an exceptional case. If you use the `-a` option, you will get the OceanBase own error info and the error info of Oracle mode (if any exists).

such as:

```shell
$ob_error ora 600 -a 5727

OceanBase:
    OceanBase Error Code: OB_ERR_PROXY_REROUTE(-5727)
    Message: SQL request should be rerouted
    Cause: Internal Error
    Solution: Contact OceanBase Support

Oracle:
    Oracle Error Code: ORA-00600
    Message: internal error code, arguments: -5727, SQL request should be rerouted
    Related OceanBase Error Code:
        OB_ERR_PROXY_REROUTE(-5727)
```

Note: `-a` option is designed to find `ORA-00600` error which has an `arguments` (those Oracle internal error).

You can find more test example in [expect_result](test/expect_result.result).

Further more, you can get the complete user manual by `--help` option.

```shell
ob_error --help
```

## How to add error cause/solution

*This part is for developers.*

For example:

The ob error `4000` in `src/oberror_errno.def` defined as

```shell
DEFINE_ERROR(OB_ERROR, -4000, -1, "HY000", "Common error");
```

If you want to add the cause and solution info, you can change the define as

```shell
DEFINE_ERROR(OB_ERROR, -4000, -1, "HY000", "Common error", "CAUSE", "SOLUTION");
```

And then regenerate the `src/lib/ob_errno.h`、`src/share/ob_errno.h` and `src/share/ob_errno.cpp` by

```shell
cd src/share
./gen_errno.pl
```

Finally back to `BUILD_DIR` to remake `ob_error`.
