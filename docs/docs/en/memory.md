---
title: Memory Management
---

# Introduction
Memory management is one of the most important modules in any large C++ project. Since OceanBase also needs to deal with the issue of multi-tenant memory resource isolation, OceanBase's memory management is more complicated than ordinary C++ projects. Generally, a good memory management module needs to consider the following issues:

- Easy to use. The designed interface must be understood and used by the container, otherwise the code will be difficult to read and maintain, and memory errors will be more likely to occur;
- Efficient. An efficient memory allocator has a crucial impact on performance, especially in high-concurrency scenarios;
- Diagnosis. As the amount of code increases, bugs are inevitable. Common memory errors, such as memory leaks, memory out-of-bounds, wild pointers and other problems cause headaches for development and operation and maintenance. How to write a function that can help us avoid or troubleshoot these problems is also an important indicator to measure the quality of the memory management module.

For the multi-tenant model, the impact of memory management design mainly includes the following aspects:
- Transparent interface design. How to make developers have no awareness or little need to care about the memory management of different tenants;
- Efficient and accurate. Sufficient memory must be applied successfully, and tenant memory exhaustion must be detected in time, which is the most basic condition for multi-tenant memory management.

This article will introduce the commonly used memory allocation interfaces and memory management related idioms in OceanBase. For technical details of memory management, please refer to [Memory Management](https://open.oceanbase.com/blog/8501613072)( In Chinese).

# Common Interfaces and Methods of OceanBase Memory Management
OceanBase provides different memory allocators for different scenarios. In addition, in order to improve program execution efficiency, there are some conventional implementations, such as reset/reuse, etc.

## ob_malloc

OceanBase has developed a set of libc-style interface functions ob_malloc/ob_free/ob_realloc. This set of interfaces will dynamically apply for memory blocks of size based on tenant_id, ctx_id, label and other attributes, and mark the memory blocks to determine ownership. This not only facilitates multi-tenant resource management, but is also very helpful in diagnosing memory problems.
ob_malloc will index to the corresponding ObTenantCtxAllocator based on tenant_id and ctx_id, and ObTenantCtxAllocator will allocate memory according to the current tenant context.

ob_free uses offset operation to find the object allocator corresponding to the memory to be released, and then returns the memory to the memory pool.

ob_realloc is different from libc's realloc. It does not expand the original address, but first copies the data to another memory through ob_malloc+memcpy, and then calls ob_free to release the original memory.

```cpp
inline void *ob_malloc(const int64_t nbyte, const ObMemAttr &attr = default_memattr);
inline void ob_free(void *ptr);
inline void *ob_realloc(void *ptr, const int64_t nbyte, const ObMemAttr &attr);
```

## OB_NEW/OB_NEWx
Similar to ob_malloc, OB_NEW provides a set of "C++" interfaces that call the object's constructor and destructor when allocating and releasing memory.

```cpp
/// T is the type, label is the memory label and it can be a const string
#define OB_NEW(T, label, ...)
#define OB_NEW_ALIGN32(T, label, ...)
#define OB_DELETE(T, label, ptr)
#define OB_DELETE_ALIGN32(T, label, ptr)

/// T is the type, pool is the memory pool allocator
#define OB_NEWx(T, pool, ...)
```

> There is no OB_DELETEx, but you can release the memory by yourself.

## ObArenaAllocator

The design feature is to allocate release multiple times and only release once. Only reset or destruction can truly release the memory. The memory allocated before will not have any effect even if `free` is actively called.

ObArenaAllocator is suitable for scenarios where many small memory allocates are released in a short period of time. For example, in a SQL request, many small block memories will be frequently allocated, and the life cycle of these small memories will last for the entire request period. Usually, the processing time of an SQL request is also very short. This memory allocation method is very effective for small memory and avoiding memory leaks. In OceanBase's code, don't be surprised if you see there is only apply for memory but cannot find a place to release it.

> Code reference `page_arena.h`

## ObMemAttr Introduction

OceanBase uses `ObMemAttr` to mark a section of memory.

```cpp
struct ObMemAttr
{
  uint64_t    tenant_id_;  // tenant
  ObLabel     label_;      // label or module
  uint64_t    ctx_id_;     // refer to ob_mod_define.h, each ctx id is corresponding to a ObTenantCtxAllocator
  uint64_t    sub_ctx_id_; // please ignore it
  ObAllocPrio prio_;       // priority
};
```

> reference file alloc_struct.h

**tenant_id**

Memory allocation management perform resource statistics and restrictions based on tenant maintenance.

**label**

At the beginning, OceanBase uses a predefined method to create memory labels for each module. However, as the amount of code increases, the method of predefined labels is not suitable. Currently, ObLabel is constructed directly using constant strings. When using ob_malloc, you can also directly pass in a constant string as the ObLabel parameter, such as `buf = ob_malloc(disk_addr.size_, "ReadBuf");`.

**ctx_id**

ctx id is predefined, please refer to `alloc_struct.h`. Each ctx_id of each tenant will create an `ObTenantCtxAllocator` object, which can separately count the related memory allocation usage. Normally use `DEFAULT_CTX_ID` as ctx id. For some special modules, for example, if we want to more conveniently observe memory usage or troubleshoot problems, we define special ctx ids for them, such as libeasy communication library (LIBEASY) and Plan Cache cache usage (PLAN_CACHE_CTX_ID). We can see periodic memory statistics in log files, such as:

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

Currently, two memory allocation priorities are supported, Normal and High. The default is Normal. For definition, please refer to the `enum ObAllocPrio` in file `alloc_struct.h`. High priority memory can allocate memory from urgent (memory_reserved) memory, otherwise it cannot. Refer to `AChunkMgr::update_hold` implementation.

> You can use the configuration item `memory_reserved` to view the reserved memory size.

## init/destroy/reset/reuse

Caching is one of the important methods to improve program performance. Object reuse is also a way of caching. On the one hand, it reduces the frequency of memory allocate and release, and on the other hand, it can reduce some construction and destruction overhead. There is a lot of object reuse in OceanBase, and some conventions have been formed, such as the reset and reuse functions.

**reset**

Used to reset objects. Restore the object's state to the state after the constructor or init function was executed. For example `ObNewRow::reset`.

**reuse**

Compared with reset, it is more lightweight. Try not to release some expensive resources, such as `PageArena::reuse`.

**init/destroy**

There are two other common interfaces in OceanBase: `init` and `destroy`. `init` is used to initizalize object and `destory` to release resources. Only do some very lightweight initialization work in the constructor, such as initializing the pointer to `nullptr`.

## SMART_VAR/HEAP_VAR

SMART_VAR is an auxiliary interface for defining local variables. Variables using this interface are always allocated from the stack first. When the stack memory is insufficient, they will be allocated from the heap. For those large local variables (>8K) that are not easy to optimize, this interface not only ensures the performance of regular scenarios, but also safely reduces the stack capacity. The interface is defined as follows:

```cpp
SMART_VAR(Type, Name, Args...) {
  // do...
}
```

It allocate from the stack when the following conditions are met, otherwise allocate from the heap
```cpp
sizeof(T) < 8K || (stack_used < 256K && stack_free > sizeof(T) + 64K)
```

> SMART_VAR was created to solve historical problems. It try to reduce the amount of stack memory occupied by large memory objects.

HEAP_VAR is similar to SMART_VAR, except that it must allocate memory on the heap.

## SMART_CALL

SMART_CALL is used to "quasi-transparently" resolve recursive function calls that may explode the stack on threads with very small stacks. This interface accepts a function call as a parameter. It will automatically check the current stack usage before calling the function. Once it is found that the available stack space is insufficient, a new stack execution function will be created on this thread immediately. After the function ends, it will continue to return to the original stack. This ensures performance when the stack is sufficient, and can also avoid stack explosion scenarios.

```cpp
SMART_CALL(func(args...))
```

Notice:
1. The return value of func must be an int type representing the error code.
2. SMART_CALL will return an error code. This may be an internal mechanism or a func call.
3. It supports stack cascade expansion, each time a 2M stack is expanded (there is a hard-coded total upper limit of 10M)

Compared with direct calling, SMART_CALL only call `check_stack_overflow` to check stack.
