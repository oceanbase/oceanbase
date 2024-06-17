---
title: Coding Convention
---

OceanBase is a giant project that has been developed for more than ten years and contains millions of lines of C++ code. It already has many unique programming habits. Here are some OceanBase programming habits to help people who come into contact with the OceanBase source code for the first time have an easier time accepting and understanding. For more detailed information, please refer to ["OceanBase C++ Coding Standard"](./coding_standard.md).

# Naming Convention

- File naming

Code file names in OceanBase all start with `ob_`. But there are some old exception files.

- Class naming

Classes all start with `Ob` and use camelCase/Pascal form, and there are also some exceptions for old classes.

- Function names, variables, etc.

Both function names and variables use lowercase naming separated by `_`. Member variables also have `_` added as a suffix.

# Coding Style

OceanBase uses some relatively simple coding styles to try to make the code readable and clear, such as adding necessary spaces for operator brackets, not too long codes, not too long functions, adding necessary comments, reasonable naming, etc. Since the coding style has many details, new developers can just refer to the coding style in the current code to write code. This is also a suggestion for participating in other projects for the first time. We should try to keep it consistent with the original style.

There is no need to worry about the styles that you are not sure about. You can discuss it with us, or after submitting the code, someone will give suggestions or code together.

# Functional Coding Habits

## Prohibitting STL Containers

Since OceanBase supports multi-tenants resource isolation, in order to facilitate memory control, OceanBase prohibits the use of STL, boost and other containers. At the same time, OceanBase provides its own containers, such as `ObSEArray`, etc. For more information about OceanBase containers, please refer to [OceanBase Container Introduction] (./container.md).

## Be Caution with the New C++ Standard

OceanBase does not encourage the use of some syntax of the new C++ standard, such as auto, smart pointers, move semantics, range-based loops, lambda, etc. OceanBase believes that these will bring many negative effects, such as

- Improper use of `auto` can cause serious performance problems, but it only brings syntactic convenience;
- Smart pointers cannot solve the problem of object memory usage, and improper use can also cause performance problems;
- The use of move is extremely complex, and it will lead to deeply hidden BUGs without ensuring that everyone understands it correctly.

Of course, OceanBase does not exclude all new standards, such as encouraging the use of override, final, constexpr, etc. If you are not sure whether a certain syntax can be used, you can search and confirm in ["OceanBase C++ Coding Standard"](./coding_standard.md).

## Single Entrance and Single Exit

It is mandatory for all functions to return at the end, and it is prohibited to call global jump instructions such as return, goto, and exit midway. This is also the most confusing part for everyone who comes into contact with OceanBase code for the first time.

In order to achieve this requirement, there will be a lot of `if/else if` in the code, and there are many less intuitive conditional judgments such as `OB_SUCC(ret)` in the `for` loop. At the same time, in order to reduce nesting, the macro `FALSE_IT` will be used to execute certain statements. for example

```cpp
int ObMPStmtReset::process()
{
  int ret = OB_SUCCESS;
  ...
  if (OB_ISNULL(req_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid packet", K(ret), KP(req_));
  } else if (OB_INVALID_STMT_ID == stmt_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt_id is invalid", K(ret));
  } else if (OB_FAIL(get_session(session))) {
    LOG_WARN("get session failed");
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL or invalid", K(ret), K(session));
  } else if (OB_FAIL(process_kill_client_session(*session))) {
    LOG_WARN("client session has been killed", K(ret));
  } else if (FALSE_IT(session->set_txn_free_route(pkt.txn_free_route()))) {
  } else if (OB_FAIL(process_extra_info(*session, pkt, need_response_error))) {
    LOG_WARN("fail get process extra info", K(ret));
  } else if (FALSE_IT(session->post_sync_session_info())) {
  } else if (FALSE_IT(need_disconnect = false)) {
  } else if (OB_FAIL(update_transmission_checksum_flag(*session))) {
    LOG_WARN("update transmisson checksum flag failed", K(ret));
  } else {
    // ...
  }
  return ret;
}
```

A lot of `if/else if` are used in the code, and the FALSE_IF macro is used to minimize the nesting of ifs.

It is worth mentioning that similar functions will write `int ret = OB_SUCCESS;` at the beginning of the function, using ret as the function return value, and many macros will also default to the existence of ret.

## Function Returns Error Code

For most functions, the function is required to have an int return value, and the return value can be explained using the error code `ob_errno.h`.
Most of the functions mentioned here include some functions for obtaining values, such as the `at` function of `ObSEArray`

```cpp
int at(int64_t idx, T &obj);
```

**Which functions do not need to return int values?**

Relatively simple functions that return class attributes, such as ObSEArray's function:

```cpp
int64_t get_capacity();
```

The value will be returned directly without the int error code.
Or similar simple judgment functions do not need to return int error codes.

## Need to Determine the Validity of All Return Values and Parameters

OceanBase requires that as long as the function has a return value, the return value must be tested, and "check if possible." Function parameters, especially pointers, must be checked for validity before use.

For example:

```cpp
int ObDDLServerClient::abort_redef_table(const obrpc::ObAbortRedefTableArg &arg, sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  ...
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  if (OB_UNLIKELY(!arg.is_valid())) { // Check the validity of the parameters passed in
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(common_rpc_proxy)) { // Before using pointers, check it first
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else {
  ...
  }
  return ret;
}
```

## Memory Management

Memory management is a very troublesome issue in C/C++ programs. OceanBase has done a lot of work for memory management, including efficient memory allocation, memory problem detection, tenant memory isolation, etc. OceanBase provides a set of memory management mechanisms for this purpose, and also prohibits the direct use of C/C++ native memory allocation interfaces in programs, such as malloc, new, etc.

The simplest, OceanBase provides the `ob_malloc/ob_free` interface to allocate and release memory:

```cpp
void *ptr = ob_malloc(100, ObModIds::OB_COMMON_ARRAY);

// do something

if (NULL != ptr) {
  // release resource
  ob_free(ptr, ObModIds::OB_COMMON_ARRAY);
  ptr = NULL;  // set the pointer to null after free
}
```

OceanBase requires that the pointer must be assigned to null immediately after the memory is released.
For more information about memory management, please refer to [OceanBase Memory Management](./memory.md).

# Some Conventional Interfaces

## init/destroy

OceanBase requires that only some very lightweight data initialization work can be implemented in the constructor, such as variables initialized to 0, pointers initialized to nullptr, etc. Because in the constructor, it is not easy to handle some complex exception scenarios, and the return value cannot be given. Most classes in OceanBase have an init function, which is usually executed after the constructor and has an int error code as the return value. Do some more complex initialization work here. Correspondingly, the destroy function is usually provided to do resource destruction.

## reuse/reset

Memory caching is a very effective way of improving performance. Many classes in OceanBase will have reuse/reset interfaces to facilitate the subsequent reuse of an object. Reuse usually represents lightweight cleanup work, while reset will do more resource cleanup work. But you need to look at the specific implementation class and cannot generalize.

## Operator Overloading

C++ provides operator overloading functions that are very convenient for writing programs, but these overloadings often bring a lot of burden, making the code difficult to read and the functions misused. For example, operator overloading may lead to implicit type conversion without the programmer's knowledge, or a seemingly simple operation may have a relatively high overhead.

In addition, try to avoid using `operator=` and try to copy objects using `deep_copy` and `shallow_copy`.

# Commonly Used Macros

**OB_SUCC**

Determine whether a statement returns successfully, equivalent to `OB_SUCCESS == (ret = func())`

```cpp
ret = OB_SUCCESS;
if (OB_SUCC(func())) {
  // do something
}
```

**OB_FAIL**

Similar to `OB_SUCC`, it just determines whether a certain statement fails to execute:

```cpp
ret = OB_SUCCESS;
if (OB_FAIL(func())) {
  // do something
}
```

**OB_ISNULL**

Determine whether the pointer is null, equivalent to `nullptr == ptr`,

```cpp
if (OB_ISNULL(ptr)) {
  // do something
}
```

**OB_NOT_NULL**

Determine whether the pointer is non-null, equivalent to `nullptr != ptr`,

```cpp
if (OB_NOT_NULL(ptr)) {
  // do something
}
```

**K**

Usually used in output logs, the usage K(obj), where obj can be a common type variable or a class object (must implement `to_string`), will be expanded into `"obj", obj`, and will eventually be output in the log "obj=123". for example:

```cpp
LOG_WARN("fail to exec func, ", K(ret));
```

**DISALLOW_COPY_AND_ASSIGN**

Used in a class declaration to indicate that operations such as copy assignment are prohibited.

```cpp
class LogReconfirm
{
  ...
private:
  DISALLOW_COPY_AND_ASSIGN(LogReconfirm);
};
```
