OceanBase 编程惯例

OceanBase 是一个发展了十几年的、包含几百万行C++代码的巨型工程，它已经有了很多自己特有的编程习惯，这里介绍一些OceanBase 编程习惯，以帮助首次接触OceanBase源码的人更容易接受和理解。更详细的内容可以参考[《OceanBase C++编程规范》](./coding_standard.md)。

# 命名习惯
- 文件命名
  
OceanBase中代码文件名都以`ob_`开头。但也有一些陈旧的例外文件。
- 类命名
  
类都以`Ob`开头，采用驼峰/Pascal形式，也有一些陈旧类的意外。
- 函数名、变量等
  
函数名和变量都采用`_`分割的小写命名法。成员变量还会增加 `_` 作为后缀。

# 编码风格
OceanBase 使用一些比较朴素的编码风格，尽量使代码易读、清晰，比如运算符括号等增加必要的空格、代码不要太长、函数不要太长、增加必要的注释、合理的命名等。由于编码风格细节较多，新上手开发者参考当前代码中的编码风格来写代码即可，这也是第一次参加其它项目工程的建议，我们应该尽量保持与原风格统一。

对于你不太确定的做法，也无须担心，可以与我们讨论，或者提交代码后，我们会有人来给出建议或者共同编码。

# 函数编程习惯
## 禁止使用STL容器
由于OceanBase支持多租户资源隔离，为了方便控制内存，OceanBase禁止使用STL、boost等容器。同时，OceanBase提供了自己实现的容器，比如 `ObSEArray` 等，更多OceanBase容器相关的介绍请参考[OceanBase 容器介绍](./container.md)。

## 谨慎使用C++新标准
OceanBase 并不鼓励使用C++新标准的一些语法，比如 auto、智能指针、move语义、range-based loops、lambda 等。OceanBase 认为这些会带来很多负面效果，比如 
- auto使用不当会引发严重的性能问题，而它仅仅是带来了语法上的遍历；
- 智能指针并不能解决对象内存使用问题，而且使用不当也会带来性能问题；
- move 的使用极为复杂，不能保证所有人理解正确的前提下会带来隐藏极深的BUG。

当然OceanBase没有排斥所有的新标准，比如鼓励使用override、final、constexpr等。如果你不确定是否可以使用某个语法，可以在[《OceanBase C++编程规范》](./coding_standard.md)中进行搜索确认。

## 单入口单出口
强制要求所有函数在末尾返回，禁止中途调用return、goto、exit等全局跳转指令。这一条也是所有首次接触OceanBase代码的人最迷惑的地方。
为了实现这一要求，代码中会出现很多 `if/else if` ，并且在 `for` 循环中存在 `OB_SUCC(ret)` 等多个不那么直观的条件判断。同时为了减少嵌套，会使用宏 `FALSE_IT` 执行某些语句。比如
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

代码中使用了大量的 `if/else if` ，并且使用了FALSE_IF宏尽量减少if的嵌套。

值得一提的是，类似的函数中都会在函数开头写 `int ret = OB_SUCCESS;`，将ret作为函数返回值，并且很多宏，也会默认ret的存在。

## 函数返回错误码
对于绝大多数函数，都要求函数具备int返回值，返回值可以使用错误码 `ob_errno.h` 解释。
这里说的绝大多数函数，包含一些获取值的函数，比如 ObSEArray的at函数 
```cpp
int at(int64_t idx, T &obj);
```

哪些函数不需要返回int值？

比较简单的返回类属性的函数，比如 ObSEArray 的函数：
```cpp
int64_t get_capacity();
```
将直接返回值，而不带int错误码。
或者类似的简单判断的函数不需要返回int错误码。

## 需要判断所有函数值与函数参数有效性
OceanBase 要求只要函数有返回值，就必须对返回值做检测，做到“能检就检”。对于函数参数，特别是指针，在使用前都必须检查其有效性。
比如：
```cpp
int ObDDLServerClient::abort_redef_table(const obrpc::ObAbortRedefTableArg &arg, sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  ...
  obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
  if (OB_UNLIKELY(!arg.is_valid())) { // 这里对传入的参数做有效性检查
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else if (OB_ISNULL(common_rpc_proxy)) { // 在使用指针之前，也要做检查
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common rpc proxy is null", K(ret));
  } else {
  ...
  }
  return ret;
}
```

## 内存管理
内存管理是C/C++程序中非常头疼的问题，OceanBase 为内存管理做了大量的工作，包括高效的内存分配、内存问题检测、租户内存隔离等。OceanBase 为此提供了一套内存管理的机制，也禁止在程序中直接使用C/C++原生内存分配接口，比如malloc、new等。
最简单的，OceanBase 提供了 ob_malloc/ob_free接口来申请释放内存：
```cpp
void *ptr = ob_malloc(100, ObModIds::OB_COMMON_ARRAY);
 
// do something
 
if (NULL != ptr) {
  // 释放资源
  ob_free(ptr, ObModIds::OB_COMMON_ARRAY);
  ptr = NULL;  // free之后立即将指针置空
}
```

OceanBase 要求在释放内存后必须立即将指针赋值为空。
更多关于内存管理的介绍，请参考 [OceanBase 内存管理](./memory.md)。

# 一些约定函数接口
## init/destroy
OceanBase 要求在构造函数中，仅实现一些非常轻量级的数据初始化工作，比如变量初始化为0，指针初始化为nullptr等。因为构造函数中，不太容易处理一些复杂的异常场景，并且无法给出返回值。OceanBase绝大多数的类都有 init 函数，通常在构造函数之后执行，并且拥有int错误码作为返回值。这里做一些比较复杂的初始化工作。相对应的，通常也会提供destroy函数做资源销毁工作。

## reuse/reset
内存缓存是提高性能非常有效的手段。OceanBase 很多类都会有reuse/reset接口，以方便某个对象后续重用。reuse 通常表示轻量级的清理工作，而reset会做更多的资源清理工作。但是需要看具体实现类，不能一概而论。
## 操作符重载
C++ 提供了非常方便编写程序的操作符重载功能，但是这些重载往往会带来很多负担，使得代码难以阅读、功能误用。比如操作符重载可能会导致程序员不知情的情况下出现类型隐式转换、或者看起来一个简单的操作却有比较高的开销。
另外，要尽量避免使用 `operator=` ，尽量以`deep_copy`、`shallow_copy`的方式实现对象的复制。

# 常用宏
**OB_SUCC**

判断某个语句是否返回成功，等价于OB_SUCCESS == (ret = func())
```cpp
ret = OB_SUCCESS;
if (OB_SUCC(func())) {
  // do something
}
```

**OB_FAIL**

类似OB_SUCC，只是判断某个语句是否执行失败：
```cpp
ret = OB_SUCCESS;
if (OB_FAIL(func())) {
  // do something
}
```

**OB_ISNULL**

判断指针是否为空，等价于nullptr == ptr，
```cpp
if (OB_ISNULL(ptr)) {
  // do something
}
```

**OB_NOT_NULL**

判断指针是否非空，等价于nullptr != ptr，
```cpp
if (OB_NOT_NULL(ptr)) {
  // do something
}
```

**K**

通常用在输出日志中，用法 K(obj)，其中obj可以是普通类型变量，也可以是类对象(必须实现to_string)，会被展开会 `"obj", obj`，最终在日志中会输出 "obj=123"。比如：
```cpp
LOG_WARN("fail to exec func, ", K(ret));
```

**DISALLOW_COPY_AND_ASSIGN**

用在类的声明中，表示此类禁止复制赋值等操作。
```cpp
class LogReconfirm
{
  ...
private:
  DISALLOW_COPY_AND_ASSIGN(LogReconfirm);
};
```
