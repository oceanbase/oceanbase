---
title: 基础数据结构
---

# 介绍

C++ STL提供了很多很方便的容器，比如vector、map、unordered_map等，由于OceanBase编程风格与内存控制等原因，在OceanBase中禁止使用STL的容器。OceanBase 提供了一些容器实现，包括数组、链表、HashMap等，本篇文档会对这些容器做一些介绍。

> 本篇文档假设你对C++ STL 容器已经有了一定的理解。

> pair不属于容器，因此可以使用。

> 由于历史遗留原因，OceanBase中包含了一些不再建议使用但是没有删除的容器代码。

# 字符串
OceanBase 提供的字符串类是 ObString。代码参考 ob_string.h。

在介绍ObString的接口之前，先介绍一下ObSring的内存管理方式，这样会更加容易理解ObString的接口设计。

与STL string最大的区别有两个：
1. ObString 不管理内存，内存由外部传入，内存buffer的生命周期也由外部控制；
2. ObString 并不以 '\0' 结尾。

这也是使用 ObString 时需要重点关注的点。

ObString 的内存由外部传入，内部保存三个成员变量：
```cpp
  char *ptr_;  /// 内存指针
  obstr_size_t buffer_size_;  /// 内存buffer长度
  obstr_size_t data_length_;  /// 有效数据长度
```

> ObString 中都使用 obstr_size_t 表示长度，其类型是 int32_t

参考 ObString 当前内存维护模式与字符串常用的接口，ObString 常用的接口如下：

```cpp
/**
 * 构造函数
 * 
 * 构造字符串的buffer数据和有效数据长度
 * 
 * 还有一些衍生的构造函数，比如省略buffer长度（buffer长度与数据长度一致）
 */
ObString(const obstr_size_t size, const obstr_size_t length, char *ptr);

/**
 * 是否空字符串
 */
bool empty() const;

/**
 * 重新赋值一个新的buffer/字符串
 */
void assign_buffer(char *buffer, const obstr_size_t size);

/**
 * 有效数据的长度，或者称为字符串的长度
 */
obstr_size_t length() const;

/**
 * 内存buffer的长度
 */
obstr_size_t size() const;

/**
 * 获取指针
 */
const char *ptr() const;

/**
 * 不区分大小写进行比较
 *
 * @NOTE: 虽然ObString没有说明是以'\0'结尾，但是这里实现时使用strncasecmp，所以使用此函数时要留意
 */
int case_compare(const ObString &obstr) const;
int case_compare(const char *str) const;

/**
 * 区分大小写比较
 *
 * @NOTE: 与case_compare相比较来说，这里没有使用strncmp，而是使用memcmp来比较的buffer长度
 */
int compare(const ObString &obstr) const;
int32_t compare(const char *str) const;
```

ObString 还有一些其它的接口，需要时浏览下 ob_string.h 代码即可。

# 数组

OceanBase的数组接口设计与STL vector类似，只是更加符合OceanBase的风格。比如接口会有一个int返回值表示执行成功或失败。OceanBase 提供了多个不同实现的数组，不过它们提供的接口是类似的。

常用的数组实现类都继承了同一个接口 `ObIArray`。我们先看一下接口定义，然后再分别介绍不同的数组实现之间的差别。

## ObIArray

数组的接口类中并没有指定内存分配器。

```cpp
/**
 * 默认空构造函数
 */
ObIArray();

/**
 * 直接接受指定数组
 *
 * 接口类不会接管data相关的内存，内存处理需要看具体的实现类。
 */
ObIArray(T *data, const int64_t count);

/**
 * 类似 vector::push_back，在最后添加一个元素
 * @return 成功返回OB_SUCCESS
 */
int push_back(const T &obj);

/**
 * 移除最后一个元素
 * @note 很可能不会调用析构函数，需要看具体的实现类
 */
void pop_back();

/**
 * 移除最后一个元素，并将最后一个元素复制到obj
 * @return 成功返回OB_SUCCESS
 */
int pop_back(T &obj);

/**
 * 移除指定位置的元素
 */
int remove(int64_t idx);

/**
 * 获取指定位置的元素
 * @return 成功返回OB_SUCCESS。如果指定位置不存在，会返回失败
 */
int at(int64_t idx, T &obj);

/**
 * 重置数组。类似vector::clear
 */
void reset();

/**
 * 重用数组。具体看实现
 */
void reuse();

/**
 * 销毁此数组，作用与调用析构函数相同
 */
void destroy();

/**
 * 预留指定大小的内存空间。不会做对象初始化
 */
int reserve(int64_t capacity);

/**
 * 预留指定大小的内存空间，通常实现类会执行对象的构造函数
 */
int prepare_allocate(int64_t capacity);

/**
 * 从另一个数组复制并销毁当前数据
 */
int assign(const ObIArray &other);
```

## ObArray
ObArray 自己管理内存，在声明ObArray模板类时，需要指定分配器，或者使用默认分配器 `ModulePageAllocator`。由于OceanBase要求所有的动作都要判断返回值，因此ObArray的 `operator=` 等不带返回值的函数，不建议使用。

ObArray的很多行为表现与STL vector类似，每次内存扩展时表现也类似，会扩展两倍当前数据大小，但最多 `block_size_` 大小。一个 `block_size_` 默认值是 `OB_MALLOC_NORMAL_BLOCK_SIZE` （可以认为是8K）。

代码参考 ob_array.h。

## ObSEArray
与ObArray类似，扩展时也会按照两倍大小，不超过`block_size_`。

与ObArray不同的是，ObSEArray多了一个模板参数 `LOCAL_ARRAY_SIZE`，不需要额外的内存分配即可容纳一定量的元素。因此OBSEArray可能可以直接使用栈内存而不是堆内存：

```cpp
char local_data_buf_[LOCAL_ARRAY_SIZE * sizeof(T)];
```
如果后续空间不足，需要扩充，那 `local_data_buf_` 将不再存放有效数据而是另外申请内存。所以要综合考虑，给出一个合理的`LOCAL_ARRAY_SIZE`才能让ObSEArray效率比较高。

参考代码 `ob_se_array.h`。

## ObFixedArray
顾名思义，就是一个固定大小的数组。一旦容量大小确定下来就不能再变了。代码参考 `ob_fixed_array.h`。

## ObVector
ObVector 不属于 ObIArray的子类，其表现和接口设计与ObIArray很类似，所以使用 ObIArray的子类即可。如果有兴趣，请阅读源码 `ob_vector.h` 和 它的实现文件 `ob_vector.ipp`。

# 链表
链表不像数组，没有统一的接口。不过这里的接口设计与STL中也是很相近的。最常用的链表有两个，一个是 ObList，另一个是 ObDList。

## ObList

ObList 是一个普通的循环双链表，代码参考`ob_list.h`。在构造时，需要传入内存分配器。常用的接口如下。
```cpp
/**
 * 声明
 * @param T 元素类型
 * @param Allocator 内存分配器
 */
template <class T, class Allocator = ObMalloc>
class ObList;

/**
 * 构造函数。必须传入内存分配器
 */
ObList(Allocator &allocator);

/**
 * 在链表结尾插入指定元素
 */
int push_back(const value_type &value);

/**
 * 在链表开头插入指定元素
 */
int push_front(const value_type &value);

/**
 * 释放最后一个元素
 * @note 并没有执行元素的析构函数
 */
int pop_back();

/**
 * 两个pop_front函数都是把第一个元素删掉，区别是一个会复制对象一个不会
 */
int pop_front(value_type &value);
int pop_front();

/**
 * 在指定位置插入指定元素
 */
int insert(iterator iter, const value_type &value);

/**
 * 删除指定位置的元素
 * @return 返回删除成功或失败
 */
int erase(iterator iter);

/**
 * 删除第一个与value值相同的元素
 * @return 没有找到元素也会返回成功
 */
int erase(const value_type &value);

/**
 * 获取第一个元素
 */
T &get_first();
const T &get_first() const;

/**
 * 获取最后一个元素
 */
T &get_last();

/**
 * 与STL类似，ObList支持iterator相关的接口
 */
iterator begin();
const_iterator begin();
iterator end();
const_iterator end() const;

/**
 * 删除所有的元素
 */
void clear();

/**
 * 判断是否空链表
 */
bool empty() const;

/**
 * 元素个数
 */
int64_t size() const;
```

## ObDList

> 代码参考 `ob_dlist.h`。

ObDList 也是一个双链表，与ObList不同的是，它的元素内存布局与内存管理方式不一样。ObList 由用户传入对象，ObList 内部申请内存复制对象，构造链表节点的前后指针。而 ObDList 由用户直接传入包含前后节点指针的对象。由于ObDList的这个特性，会导致它与使用STL list的方法不同。

ObDList 不管理内存也完全不需要管理内存，它的模板参数没有内存分配器，只有一个 `DLinkNode`，`DLinkNode` 需要包含你需要的元素对象、前后节点指针和并实现一些通用的操作（有辅助实现基类），ObDList 的声明和一些接口如下：
```cpp
template <typename DLinkNode>
class ObDList;

/// 把当前链表上的元素都移动到list上去
int move(ObDList &list);

/// 获取头节点（不是第一个元素）
DLinkNode *get_header();
const DLinkNode *get_header() const;

/// 获取最后一个元素
DLinkNode *get_last();

/// 获取第一个元素
const DLinkNode *get_first() const;
const DLinkNode *get_first_const() const;

/// 在尾巴上添加一个节点
bool add_last(DLinkNode *e);

/// 在头上添加一个节点
bool add_first(DLinkNode *e);

/// 在指定位置添加节点
bool add_before(const DLinkNode *pos, DLinkNode *e);

/// 指定节点移动到最前面
bool move_to_first(DLinkNode *e);
/// 指定节点移动到最后
bool move_to_last(DLinkNode *e);

/// 删除最后一个节点
DLinkNode *remove_last();
/// 删除最前面一个节点
DLinkNode *remove_first();

/// 在链表开头插入另一个链表
void push_range(ObDList<DLinkNode> &range);

/// 从开头删除指定个数的元素，删除的元素放到了range中
void pop_range(int32_t num, ObDList<DLinkNode> &range);

/// 是否空链表
bool is_empty() const
/// 元素个数
int32_t get_size() const
/// 删除指定元素
DLinkNode *remove(DLinkNode *e);
/// 清空链表
void clear();
```

OceanBase 提供了辅助 `DLinkNode` 实现 `ObDLinkNode` 和 `ObDLinkDerived`，只需要使用任一复制类即可轻松地使用 ObDList。

在介绍这两个辅助类之前，先简单看一下一个基础的辅助接口实现 `ObDLinkBase`，它是上面两个辅助类的基类。它包含了 ObDList 需要的前后节点指针与一些基本的节点操作，两个辅助类都是通过继承基类来实现，而且只是使用方法不同而已。

第一个辅助类 ObDLinkNode，声明如下：
```cpp
template<typename T>
struct ObDLinkNode: public ObDLinkBase<ObDLinkNode<T> >
```

给定自己的真实链表元素类型即可，缺点是获取到链表元素时，需要使用 `ObDLinkNode::get_data` 来获取自己的对象，比如
```cpp
class MyObj;
ObDList<ObDLinkNode<MyObj>> alist;

ObDLinkNode<MyObj> *anode = OB_NEW(ObDLinkNode<MyObj>, ...);
alist.add_last(anode);

ObDLinkNode<MyObj> *nodep = alist.get_first();
MyObj &myobj = nodep->get_data();
// do something with myobj
```

第二个辅助类 ObDLinkDerived，比ObDLinkNode使用更简单一些，它的声明是这样的：
```cpp
template<typename T>
struct ObDLinkDerived: public ObDLinkBase<T>, T
```

注意看，它直接继承了模板类T本身，也就是不需要再像ObDLinkNode一样通过 get_data获取真实对象，直接可以使用T的方法，复制上面的示例：
```cpp
class MyObj;
ObDList<ObDLinkDerived<MyObj>> alist;

ObDLinkDerived<MyObj> *anode = OB_NEW(ObDLinkDerived<MyObj>, ...);
alist.add_last(anode);

ObDLinkDerived<MyObj> *nodep = alist.get_first();
// MyObj &myobj = nodep->get_data(); // no need any more
// MyObj *myobj = nodep; // nodep is a pointer to MyObj too
// do something with myobj or directly with nodep
```

由于 ObDList 不管理节点内存，那么使用时就需要特别小心，注意管理好各个元素的生命周期，在执行清理动作之前，比如`clear`、`reset`，一定要把内存先释放掉。ObDList的接口声明非常清晰，只是与STL::list命名习惯不同，可以直接参考代码 `ob_dlist.h` 的接口声明使用即可，不再罗列。

# Map
Map 是一个常用的数据结构，它的插入和查询的效率都非常高。通常情况下，Map有两种实现方法，一种是平衡查找树，典型的是红黑树，常见的编译器使用这种方式实现，一种是散列表，STL中是unordered_map。

OceanBase中实现了非常多的Map，其中包括平衡查找树实现 ObRbTree 和适用于不同场景的hash map，比如 ObHashMap、ObLinkHashMap和ObLinearHashMap。

> OceanBase 实现了很多种hash map，但是推荐使用这里介绍的几个，除非你对其它实现理解特别清晰。

## ObHashMap
ObHashMap 的实现在 ob_hashmap.h 中，为了方便理解 ObHashMap 的实现，我会对照STL::unordered_map来介绍。

### ObHashMap 介绍
在STL中，unordered_map的声明如下：
```cpp
template<
    class Key,  /// 键类型
    class T,    /// 值类型
    class Hash = std::hash<Key>,          /// 根据Key计算hash值
    class KeyEqual = std::equal_to<Key>,  /// 判断Key是否相等
    class Allocator = std::allocator<std::pair<const Key, T>> /// 内存分配器
> class unordered_map;
```

模板参数中 Key 是我们的键值，T 就是我们值的类型，Hash 是根据键值计算hash值的类或函数，KeyEqual 是判断两个键值是否相等的方法，Allocator 是一个分配器，分配的对象是键和值组成在一起的pair。

OceanBase 中的声明是类似的：

```cpp
template <class _key_type,
          class _value_type,
          class _defendmode = LatchReadWriteDefendMode,
          class _hashfunc = hash_func<_key_type>,
          class _equal = equal_to<_key_type>,
          class _allocer = SimpleAllocer<typename HashMapTypes<_key_type, _value_type>::AllocType>,
          template <class> class _bucket_array = NormalPointer,
          class _bucket_allocer = oceanbase::common::ObMalloc,
          int64_t EXTEND_RATIO = 1>
class ObHashMap;
```

其中 `_key_type`、`_value_type`、`_hashfunc`、`_equal`，与STL::unordered_map的声明参数含义是一样的。这里多了一些参数：

- `_defendmode`: OceanBase 提供了有限条件的线程安全hashmap实现，可以使用默认值，当前先忽略，稍后会介绍；
- `_allocer`与`_bucket_allocer`：STL::unordered_map只需要一个分配器，而这里要求提供两个分配器。hashmap中，通常会有一个数组作为桶(bucket)数组，元素进行hash后，找到对应桶，然后将元素”挂载“在对应的桶上。`_bucket_allocer` 就是桶数组的分配器，而`_allocer` 是元素的分配器，也就是建值对的分配器；
- EXTEND_RATIO：如果EXTEND_RATIO是1，就不会进行扩展。

### ObHashMap 接口介绍
```cpp
/**
 * ObHashMap的构造函数并不做什么事情。必须调用 create 才会进行真正的初始化。
 * create 函数的参数主要是 桶的个数 (bucket_num)和内存分配器的参数。
 * 合理的给出桶的个数，可以让hashmap运行的更加高效又不至于浪费太多内存。
 * 
 * 通过下面几个接口可以看到，可以提供两个内存分配器，一个是bucket数组的分配器，
 * 一个是元素节点的分配器
 */
int create(int64_t bucket_num, 
           const ObMemAttr &bucket_attr,
           const ObMemAttr &node_attr);
int create(int64_t bucket_num, const ObMemAttr &bucket_attr);
int create(int64_t bucket_num, 
           const lib::ObLabel &bucket_label,
           const lib::ObLabel &node_label = ObModIds::OB_HASH_NODE, 
           uint64_t tenant_id = OB_SERVER_TENANT_ID,
           uint64_t ctx_id = ObCtxIds::DEFAULT_CTX_ID);
int create(int64_t bucket_num, 
           _allocer *allocer, 
           const lib::ObLabel &bucket_label,
           const lib::ObLabel &node_label = ObModIds::OB_HASH_NODE);
int create(int64_t bucket_num, 
           _allocer *allocer, 
           _bucket_allocer *bucket_allocer);

/// 直接销毁当前对象
int destroy();

/// 这两个函数都会删除所有的元素
int clear();
int reuse();

/**
 * 获取指定键值的元素值
 * 虽然也提供了get函数，但是建议使用当前函数。
 * @param timeout_us：获取元素的超时时间。超时的实现原理后面会介绍
 * @return 找到了返回成功
 */
int get_refactored(const _key_type &key, _value_type &value, const int64_t timeout_us = 0) const;

/**
 * 设置某个键值的值
 * @param key: 建
 * @param value: 值
 * @param flag：0表示已经存在不会覆盖，否则会覆盖掉原先的值
 * @param broadcast：是否唤醒等待获取当前键的线程
 * @param overwrite_key：没使用
 * @param callback：插入或更新成功后，可以使用callback对值做一些额外操作
 */
template <typename _callback = void>
int set_refactored(const _key_type &key, 
                   const _value_type &value,
                   int flag = 0,
                   int broadcast = 0, 
                   int overwrite_key = 0, 
                   _callback *callback = nullptr);
                 
/**
 * 遍历所有元素
 * @note
 * 1. 不能在遍历的过程中做删除元素、插入等动作。
 *    因为遍历的过程中会加一些锁，而插入、删除等动作也会加锁，所以可能会产生锁冲突；
 * 2. callback 动作尽量小，因为它是在锁范围内工作的
 */
template<class _callback>
int foreach_refactored(_callback &callback) const;

/**
 * 删除指定键值。如果value指针不是空，会返回对应元素
 * @return 元素不存在会返回OB_HASH_NOT_EXIST
 */
int erase_refactored(const _key_type &key, _value_type *value = NULL);

/**
 * 不存在就插入，否则调用callback来更新
 */
template <class _callback>
int set_or_update(const _key_type &key, const _value_type &value,
                  _callback &callback);

/**
 * 删除指定键值并满足特定条件的元素
 */
template<class _pred>
int erase_if(const _key_type &key, _pred &pred, bool &is_erased, _value_type *value = NULL);

/**
 * 不需要复制元素，直接以callback的方式访问指定键值的元素
 * @note callback 是在写锁保护下执行
 */
template <class _callback>
int atomic_refactored(const _key_type &key, _callback &callback);

/**
 * 不需要将元素值复制出来，直接拿到元素通过callback来访问
 * @note callback是在读锁保护下执行的
 */
template <class _callback>
int read_atomic(const _key_type &key, _callback &callback);
```

### ObHashMap的实现
熟悉STL unordered_map实现原理的同学肯定能猜到ObHashMap的实现原理。ObHashMap的实现也是一个线性表，作为桶数组，再利用拉链表的方法解决键hash冲突。但是这里还有一些细节，希望能够帮助大家了解它的实现，而更高效地利用ObHashMap。

ObHashMap 底层依赖 ObHashTable，代码参考 `ob_hashtable.h`，ObHashMap 是在ObHashTable上封装了一下 Key Value 的语义而已。

**有条件的线程安全**

如果模板参数`_defendmode` 选择有效的锁模式，而 ObHashTable 的每个桶都有一个读写锁，那么ObHashTable就会提供有条件的线程安全。在访问桶上的元素时，都会加对应的锁，包括带有 `callback` 的接口也是，所以`callback`中的动作应该尽量轻量而且不应该再访问ObHashTable的其它元素防止死锁。

ObHashMap 在扩容时不是线程安全的。如果提供的模板参数 EXTEND_RATIO 不是1，在需要的时候就会扩容，并且这对用户是透明的。

ObHashMap `_defendmode` 的默认值就是一个有效的线程安全保护模式 `LatchReadWriteDefendMode`。

**_defendmode**

_defendmode 定义了不同的桶加锁方式，在 `ob_hashutils.h` 中提供了6中模式：

1. LatchReadWriteDefendMode
2. ReadWriteDefendMode
3. SpinReadWriteDefendMode
4. SpinMutexDefendMode
5. MultiWriteDefendMode
6. NoPthreadDefendMode

其中前5种都能提供线程安全保护，只是使用的锁模式不同。在不同的业务场景、不同的线程读写并发，选择合理的模式，可以提高效率和稳定性。而第6种模式 `NoPthreadDefendMode`，则不提供任何保护。

**get超时等待**

如果在获取某个元素时指定的元素不存在，可以设置一个等待时间。ObHashTable将会在对应的桶上插入一个 `fake` 元素，然后等待。在另一个线程插入对应元素时，会唤醒等待线程，不过需要插入元素线程明确指定需要唤醒，即 set_refactor 的 broatcast 值设置为非0。

## ObHashSet
与 ObHashMap类似，ObHashSet是基于ObHashTable封装了一个只有key没有value的实现，请参考代码ob_hashset.h，不再赘述。

## ObLinkHashMap
ObLinkHashMap 是一个读写性能兼顾、线程安全（包括扩容）的无锁hash map，使用拉链式方法解决 hash 冲突。
下面列举一下这个类的特点：

- 读写性能兼顾；
- 基于无锁方案实现线程安全；
- 引入retire station，节点会延迟释放，因此建议 Key 尽量小；
- 存在一定的内存浪费；
- 扩缩容时采用批量搬迁方式完成；
- 有热点key时，get性能由于引用计数问题不佳；
- bucket 过多扩容时，初始化Array较慢。

> 关于 retire station，请参考论文 [Reclaiming Memory for Lock-Free Data Structures:There has to be a Better Way](https://www.cs.utoronto.ca/%7Etabrown/debra/fullpaper.pdf)。

下面列一些常用的接口以及使用时的注意事项。

```cpp
/**
 * ObLinkHashMap的声明
 * 模板参数：
 * @param Key 键值类型
 * @param Value 值的类型，需要继承自 LinkHashValue（参考 ob_link_hashmap_deps.h）
 * @param AllocHandle 分配释放值和节点的类 （参考 ob_link_hashmap_deps.h）
 * @param RefHandle 引用计数的函数。如果你没有深入理解它的原理，不要修改
 * @param SHRINK_THRESHOLD 当前节点的个数太多或者太少时就会扩缩容，尽量让当前节点保持在
 *        比例[1/SHRINK_THRESHOLD, 1]之间（非精准控制）
 */
template<typename Key,
         typename Value,
         typename AllocHandle=AllocHandle<Key, Value>,
         typename RefHandle=RefHandle,
         int64_t SHRINK_THRESHOLD = 8>
class ObLinkHashMap;


/// 当前元素的个数
int64_t size() const;

/**
 * 插入一个元素
 * @note 如果返回成功，需要执行 hash.revert(value)
 */
int insert_and_get(const Key &key, Value* value);

/// 删除指定元素
int del(const Key &key);

/**
 * 获取指定元素
 * @note 如果返回成功，需要执行 revert
 */
int get(const Key &key, Value*& value);

/// 释放指定元素的引入计数。可以跨线程释放
void revert(Value* value);

/**
 * 判断是否存在指定元素
 * @return OB_ENTRY_EXIST 存在
 */
int contains_key(const Key &key);

/**
 * 遍历所有元素
 * @param fn : bool fn(Key &key, Value *value); 其中bool返回值表示是否还要继续遍历
 */
template <typename Function> int for_each(Function &fn);

/**
 * 删除满足条件的元素
 * @param fn bool fn(Key &key, Value *value); 其中bool返回值表示是否需要删除
 */
template <typename Function> int remove_if(Function &fn);
```

## ObRbTree
ObRbTree 是一个红黑树实现，支持插入、删除、查找等基本操作，非线程安全。由于ObRbTree 在OceanBase中并没有使用，因此不再介绍，有兴趣的请阅读源码 `ob_rbtree.h`。


# 其它
OceanBase 还有很多基础容器的实现，比如一些队列（ObFixedQueue、ObLightyQueue、ObLinkQueue）、bitmap(ObBitmap)、tuple(ObTuple)等。如果常见的容器不能满足你的需求，可以在 `deps/oblib/src/lib` 目录下找到更多。
