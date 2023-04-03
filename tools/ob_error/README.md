# What is ob_error

ob_error is an error tool for OceanBase Database. ob_error returns information, cause, and possible solutions for the error code you enter. With the help of ob_error, it is not necessary to look up the documents for basic error information.

## How to build

### Debug mode

```bash
bash build.sh debug --init
cd build_debug
make ob_error
cp tools/ob_error/src/ob_error /usr/local/bin
```

The compiled product for `ob_error` is stored in `DEBUG_BUILD_DIR/tools/ob_error/src/ob_error` by default.

### Release mode

```bash
bash build.sh release --init
cd build_release
make ob_error
cp tools/ob_error/src/ob_error /usr/local/bin
```

The compiled product for `ob_error` is stored in `RELEASE_BUILD_DIR/tools/ob_error/src/ob_error` by default.

## How to use

You can search for error messages by only entering the error code. Then you will get the error message corresponding to the operation system, Oracle mode, MySQL mode, and OceanBase error (if any). For example:

```bash
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

Also, you can search error messages for a specific mode by adding a prefix (also known as a facility).

When the facility is `my`, if the error code is not an error in MySQL, you will get the OceanBase error info(if any). Otherwise, you will get the error info of MySQL mode.

```bash
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

When the facility is `ora` or `pls`, you will get the error info of Oracle mode(if any). For example:

```bash
$ob_error ora 51

Oracle:
    Oracle Error Code: ORA-00051
    Message: timeout occurred while waiting for a resource
    Related OceanBase Error Code:
        OB_ERR_TIMEOUT_ON_RESOURCE(-5848)
```

Furthermore, there is an exceptional case. If you use the `-a` option, you will get OceanBase error info and Oracle mode error info (if any). For example:

```bash
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

> **NOTE**: `-a` option helps to find `ORA-00600` error which has `arguments` (those Oracle internal errors).

You can find more test examples in [expect_result](test/expect_result.result).

Furthermore, you can get the complete user manual by `--help` option.

```bash
ob_error --help
```

## How to add error cause/solution

> **NOTE**: This section is for developers.

For example:

The ob error `4000` in `src/oberror_errno.def` is defined as:

```bash
DEFINE_ERROR(OB_ERROR, -4000, -1, "HY000", "Common error");
```

If you want to add the cause and solution info, you can change the definition as:

```bash
DEFINE_ERROR(OB_ERROR, -4000, -1, "HY000", "Common error", "CAUSE", "SOLUTION");
```

And then regenerate the `src/lib/ob_errno.h`、`src/share/ob_errno.h` and `src/share/ob_errno.cpp` by using these commands:

```bash
cd src/share
./gen_errno.pl
```

Then you go to `BUILD_DIR` to remake `ob_error`.
