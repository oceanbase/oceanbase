---
title: 内存管理
---

# 简介
内存管理是所有大型C++工程中最重要的模块之一。由于OceanBase还需要处理多租户内存资源隔离问题，因此OceanBase相较于普通的C++工程，内存管理更加复杂。通常，一个良好的内存管理模块需要考虑以下几个问题：

- 易用。设计的接口比较容器理解和使用，否则代码会很难阅读和维护，也会更容易出现内存错误；
- 高效。高效的内存分配器对性能影响至关重大，尤其是在高并发场景下；
- 诊断。随着代码量的增长，BUG在所难免。常见的内存错误，比如内存泄露、内存越界、野指针等问题让开发和运维都很头疼，如何编写一个能够帮助我们避免或排查这些问题的功能，也是衡量内存管理模块优劣的重要指标。

对于多租户模式，内存管理设计的影响主要有以下几个方面：
- 透明的接口设计。如何让开发人员无感、或极少的需要关心不同租户的内存管理工作；
- 高效准确。内存充足应该必须申请成功，租户内存耗尽应该及时察觉，是多租户内存管理的最基础条件。

本篇文章将会介绍OceanBase 中常用的内存分配接口与内存管理相关的习惯用法，关于内存管理的技术细节，请参考[内存管理](https://open.oceanbase.com/blog/8501613072)(中文版）。

# OceanBase 内存管理常用接口与方式
OceanBase 针对不同场景，提供了不同的内存分配器。另外为了提高程序执行效率，有一些约定的实现，比如reset/reuse等。

## ob_malloc

OceanBase数据库自研了一套libc风格的接口函数ob_malloc/ob_free/ob_realloc，这套接口会根据tenant_id、ctx_id、label等属性动态申请大小为size的内存块，并且为内存块打上标记，确定归属。这不仅方便了多租户的资源管理，而且对诊断内存问题有很大帮助。
ob_malloc会根据tenant_id、ctx_id索引到相应的ObTenantCtxAllocator，ObTenantCtxAllocator会按照当前租户上下文环境分配内存。
ob_free通过偏移运算求出即将释放的内存所对应的对象分配器，再将内存放回内存池。
ob_realloc与libc的realloc不同，它不是在原有地址上扩容，而是先通过ob_malloc+memcpy将数据复制到另一块内存上，再调用ob_free释放原有内存。

```cpp
inline void *ob_malloc(const int64_t nbyte, const ObMemAttr &attr = default_memattr);
inline void ob_free(void *ptr);
inline void *ob_realloc(void *ptr, const int64_t nbyte, const ObMemAttr &attr);
```

## OB_NEWx
与 ob_malloc 类似，OB_NEW提供了一套"C++"的接口，在分配释放内存的同时会调用对象的构造析构函数。

## ObArenaAllocator
设计特点是多次申请一次释放，只有reset或者析构才真正释放内存，在这之前申请的内存即使主动调用free也不会有任何效用。
ObArenaAllocator 适用于很多小内存申请，短时间内存会释放的场景。比如一次SQL请求中，会频繁申请很多小内存，并且这些小内存的生命周期会持续整个请求期间。通常情况下，一次SQL的请求处理时间也非常短。这种内存分配方式对于小内存和避免内存泄露上非常有效。在OceanBase的代码中如果遇到只有申请内存却找不到释放内存的地方，不要惊讶。

> 代码参考 `page_arena.h`

## ObMemAttr 介绍

OceanBase 使用 `ObMemAttr` 来标记一段内存。

```cpp
struct ObMemAttr
{
  uint64_t    tenant_id_;  // 租户
  ObLabel     label_;      // 标签、模块
  uint64_t    ctx_id_;     // 参考 ob_mod_define.h，每个ctx id都会分配一个ObTenantCtxAllocator
  uint64_t    sub_ctx_id_; // 忽略
  ObAllocPrio prio_;       // 优先级
};
```

> 参考文件 alloc_struct.h

**tenant_id**

内存分配管理会按照租户维护进行资源统计、限制。

**label**

在最开始，OceanBase 使用预定义的方式为各个模块创建内存标签。但是随着代码量的增长，预定义标签的方式不太适用，当前改用直接使用常量字符串的方式构造ObLabel。在使用ob_malloc时，也可以直接传入常量字符串当做ObLabel参数。

**ctx_id**

ctx id是预定义的，可以参考 `alloc_struct.h`。每个租户的每个ctx_id都会创建一个`ObTenantCtxAllocator` 对象，可以单独统计相关的内存分配使用情况。通常情况下使用`DEFAULT_CTX_ID`作为ctx id。一些特殊的模块，比如希望更方便的观察内存使用情况或者更方便的排查问题，我们为它定义特殊的ctx id，比如libeasy通讯库(LIBEASY)、Plan Cache缓存使用(PLAN_CACHE_CTX_ID)。我们可以在内存中看到周期性的内存统计信息，比如：

```txt
[2024-01-02 20:05:50.375549] INFO  [LIB] operator() (ob_malloc_allocator.cpp:537) [47814][MemDumpTimer][T0][Y0-0000000000000000-0-0] [lt=10] [MEMORY] tenant: 500, limit: 9,223,372,036,854,775,807 hold: 800,768,000 rpc_hold: 0 cache_hold: 0 cache_used: 0 cache_item_count: 0
[MEMORY] ctx_id=           DEFAULT_CTX_ID hold_bytes=    270,385,152 limit=             2,147,483,648
[MEMORY] ctx_id=                    GLIBC hold_bytes=      8,388,608 limit= 9,223,372,036,854,775,807
[MEMORY] ctx_id=                 CO_STACK hold_bytes=    106,954,752 limit= 9,223,372,036,854,775,807
[MEMORY] ctx_id=                  LIBEASY hold_bytes=      4,194,304 limit= 9,223,372,036,854,775,807
[MEMORY] ctx_id=            LOGGER_CTX_ID hold_bytes=     12,582,912 limit= 9,223,372,036,854,775,807
[MEMORY] ctx_id=                  PKT_NIO hold_bytes=     17,969,152 limit= 9,223,372,036,854,775,807
[MEMORY] ctx_id=           SCHEMA_SERVICE hold_bytes=    135,024,640 limit= 9,223,372,036,854,775,807
[MEMORY] ctx_id=        UNEXPECTED_IN_500 hold_bytes=    245,268,480 limit= 9,223,372,036,854,775,807
```

**prio**

当前支持两个内存分配优先级，Normal和High，默认为Normal，定义可以参考 `enum ObAllocPrio` 文件`alloc_struct.h`。高优先级内存可以从urgent（memory_reserved）内存中分配内存，否则不可以。参考 `AChunkMgr::update_hold`实现。

> 可以使用配置项 `memory_reserved` 查看预留内存大小。

## init/destroy/reset/reuse

缓存是提升程序性能的重要手段之一，对象重用也是缓存的一种方式，一方面减少内存申请释放的频率，另一方面可以减少一些构造析构的开销。OceanBase 中有大量的对象重用，并且形成了一些约定，比如reset和reuse函数。

**reset**

用于重置对象。把对象的状态恢复成构造函数或者init函数执行后的状态。比如 `ObNewRow::reset`。

**reuse**

相较于reset，更加轻量。尽量不去释放一些开销较大的资源，比如 `PageArena::reuse`。

OceanBase 中还有两个常见的接口是`init`和`destroy`。在构造函数中仅做一些非常轻量级的初始化工作，比如指针初始化为`nullptr`。

## SMART_VAR/HEAP_VAR
SMART_VAR是定义局部变量的辅助接口，使用该接口的变量总是优先从栈上分配，当栈内存不足时退化为从堆上分配。对于那些不易优化的大型局部变量（>8K），该接口即保证了常规场景的性能，又能将栈容量安全地降下来。接口定义如下：

```cpp
SMART_VAR(Type, Name, Args...) {
  // do...
}
```

满足以下条件时从栈上分配，否则从堆上分配
```cpp
sizeof(T) < 8K || (stack_used < 256K && stack_free > sizeof(T) + 64K) 
```

> SMART_VAR 的出现是为了解决历史问题。尽量减少大内存对象占用太多的栈内存。

HEAP_VAR 类似于 SMART_VAR，只是它一定会在堆上申请内存。

## SMART_CALL
SMART_CALL用于"准透明化"的解决那些在栈非常小的线程上可能会爆栈的递归函数调用。该接口接受一个函数调用为参数，函数调用前会自动检查当前栈的使用情况，一旦发现栈可用空间不足立即在本线程上新建一个栈执行函数，函数结束后继续回到原始栈。即保证了栈足够时的性能，也可以兜底爆栈场景。

```cpp
SMART_CALL(func(args...))
```

注意：
1. func返回值必须是表征错误码的int类型
2. SMART_CALL会返回错误码，这个可能是内部机制的也可能是func调用的
3. 支持栈级联扩展，每次扩展出一个2M栈（有一个写死的总上限，10M）

SMART_CALL 相对于直接调用多了 `check_stack_overflow` 栈移除检查。
