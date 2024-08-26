---
title: Basic Data Structures
---

# Introduction

C++ STL provides many convenient containers, such as vector, map, unordered_map, etc. Due to OceanBase programming style and memory control, the use of STL containers is prohibited in OceanBase. OceanBase provides some container implementations, including arrays, linked lists, HashMap, etc. This document will introduce some of these containers.

> This document assumes that you already have a certain understanding of C++ STL containers.

> pair does not belong to the container, so it can be used in OceanBase.

> Due to historical reasons, OceanBase contains some container code that is no longer recommended but has not been deleted.

# String
The string class provided by OceanBase is ObString. Code reference ob_string.h.

Before introducing ObString's interface, let's first look at ObSring's memory management method, which will make it easier to understand ObString's interface design.

There are two biggest differences from STL string:
1. ObString does not manage memory, memory is transferred from the outside, and the life cycle of the memory buffer is also controlled externally;
2. ObString does not end with '\0'.

This is also an important point to pay attention to when using ObString.

The memory of ObString is passed in from the outside, and three member variables are stored internally:
```cpp
   char *ptr_;                /// memory pointer
   obstr_size_t buffer_size_; /// Memory buffer length
   obstr_size_t data_length_; /// Valid data length
```

> obstr_size_t is used in ObString to represent the length, and its type is int32_t

Refer to the current memory maintenance mode of ObString and the commonly used interfaces for strings. The commonly used interfaces of ObString are as follows:

```cpp
/**
  * Constructor
  *
  * Construct the buffer data and effective data length of the string
  *
  * There are also some derived constructors, such as omitting the buffer length
  * (the buffer length is consistent with the data length)
  */
ObString(const obstr_size_t size, const obstr_size_t length, char *ptr);

/**
 * an empty string?
 */
bool empty() const;

/**
 * Reassign a new buffer/string
 */
void assign_buffer(char *buffer, const obstr_size_t size);

/**
 * The length of the valid data, or the length of the string
 */
obstr_size_t length() const;

/**
 * The length of the memory buffer
 */
obstr_size_t size() const;

/**
 * Data pointer
 */
const char *ptr() const;

/**
 * Case insensitively comparison
 *
 * @NOTE: Although ObString does not specify that it ends with '\0',
 * strncasecmp is used in the implementation here, so please pay attention
 * when using this function.
 */
int case_compare(const ObString &obstr) const;
int case_compare(const char *str) const;

/**
 * Case-sensitive comparison
 *
 * @NOTE: Compared with case_compare, strncmp is not used here,
 * but memcmp is used to compare the buffer length.
 */
int compare(const ObString &obstr) const;
int32_t compare(const char *str) const;
```

ObString also has some other interfaces, just browse the ob_string.h code if needed.

# Array

OceanBase's array interface design is similar to STL vector, but it is more in line with OceanBase's style. For example, the interface will have an int return value indicating success or failure of execution. OceanBase provides multiple arrays with different implementations, but the interfaces they provide are similar.

Commonly used array implementation classes all inherit the same interface `ObIArray`. Let's take a look at the interface definition first, and then introduce the differences between different array implementations.

## ObIArray

There is no memory allocator specified in the interface class of the array.

```cpp
/**
 * The default constructor
 */
ObIArray();

/**
 * Accept the specified array
 *
 * The interface class will not take over data-related memory.
 * Memory processing depends on the specific implementation class.
 */
ObIArray(T *data, const int64_t count);

/**
 * Similar to vector::push_back, adds an element at the end
 * @return Return OB_SUCCESS when successfully
 */
int push_back(const T &obj);

/**
 * Remove the last element
 * @NOTE It is very likely that the destructor will not be called.
 * You need to look at the specific implementation class.
 */
void pop_back();

/**
 * Remove the last element and copy the last element to obj
 * @return Return OB_SUCCESS when successfully
 */
int pop_back(T &obj);

/**
 * Remove element at specified position
 */
int remove(int64_t idx);

/**
 * Get the element at the specified position
 * @return OB_SUCCESS is returned successfully.
 * If the specified location does not exist, a failure will be returned.
 */
int at(int64_t idx, T &obj);

/**
 * Reset the array. Similar to vector::clear
 */
void reset();

/**
 * Reuse arrays. Depends on the implementation
 */
void reuse();

/**
 * Destroy this array, which has the same effect as calling the destructor
 */
void destroy();

/**
 * Reserve a specified amount of memory space. Does not do object initialization
 */
int reserve(int64_t capacity);

/**
 * Reserve a specified size of memory space, usually the implementation
 * class will execute the object's constructor
 */
int prepare_allocate(int64_t capacity);

/**
 * Copy and destroy current data from another array
 */
int assign(const ObIArray &other);
```

## ObArray
ObArray manages memory by itself. When declaring the ObArray template class, you need to specify an allocator, or use the default allocator `ModulePageAllocator`. Since OceanBase requires all actions to determine the return value, it is not recommended to use ObArray's `operator=` and other functions without return values.

Many behaviors of ObArray are similar to STL vectors. Each time the memory is expanded, the behavior is similar. It will expand twice the current data size, but up to `block_size_` size. A `block_size_` default value is `OB_MALLOC_NORMAL_BLOCK_SIZE` (think of it as 8K).

Code reference ob_array.h.

## ObSEArray
Similar to ObArray, it will be doubled in size when expanded, not exceeding `block_size_`.

Different from ObArray, ObSEArray has an additional template parameter `LOCAL_ARRAY_SIZE`, which can accommodate a certain amount of elements without additional memory allocation. Therefore OBSEArray may be able to directly use stack memory instead of heap memory:

```cpp
char local_data_buf_[LOCAL_ARRAY_SIZE * sizeof(T)];
```
If there is insufficient subsequent space and needs to be expanded, `local_data_buf_` will no longer store valid data but will apply for additional memory. Therefore, we must consider it comprehensively and give a reasonable `LOCAL_ARRAY_SIZE` to make ObSEArray more efficient.

Reference code `ob_se_array.h`.

## ObFixedArray
As the name suggests, it is a fixed-size array. Once the capacity size is determined, it cannot be changed. Code reference `ob_fixed_array.h`.

## ObVector
ObVector does not belong to the subclass of ObIArray. Its performance and interface design are very similar to ObIArray, so you can use the subclass of ObIArray. If you are interested, please read the source code `ob_vector.h` and its implementation file `ob_vector.ipp`.

# List
Unlike arrays, linked lists do not have a unified interface. However, the interface design here is also very similar to that in STL. There are two most commonly used linked lists, one is ObList and the other is ObDList.

## ObList

ObList is an ordinary circular double linked list, refer to `ob_list.h` for the code. During construction, the memory allocator needs to be passed in. Commonly used interfaces are as follows.

```cpp
/**
  * Class statement
  * @param T element type
  * @param Allocator memory allocator
  */
template <class T, class Allocator = ObMalloc>
class ObList;

/**
 * Constructor. You must pass a memory allocator
 */
ObList(Allocator &allocator);

/**
 * Insert the specified element at the end of the linked list
 */
int push_back(const value_type &value);

/**
 * Insert the specified element at the beginning of the linked list
 */
int push_front(const value_type &value);

/**
 * Release the last element
 * @note The destructor of the element is not executed
 */
int pop_back();

/**
 * Both pop_front functions delete the first element.
 * The difference is that one will copy the object and the other will not.
 */
int pop_front(value_type &value);
int pop_front();

/**
 * Inserts the specified element at the specified position
 */
int insert(iterator iter, const value_type &value);

/**
  * Delete the element at the specified position
  * @return Returns deletion success or failure
  */
int erase(iterator iter);

/**
  * Delete the first element with the same value as value
  * @return Success will be returned even if the element is not found
  */
int erase(const value_type &value);

/**
 * Get the first element
 */
T &get_first();
const T &get_first() const;

/**
 * Get the last element
 */
T &get_last();

/**
 * Similar to STL, ObList supports iterator-related interfaces
 */
iterator begin();
const_iterator begin();
iterator end();
const_iterator end() const;

/**
 * Delete all elements
 */
void clear();

/**
 * Determine whether the linked list is empty
 */
bool empty() const;

/**
 * Number of elements
 */
int64_t size() const;
```

## ObDList

> Code reference `ob_dlist.h`.

ObDList is also a double linked list. Unlike ObList, its element memory layout and memory management method are different. The ObList object is passed in by the user. ObList internally applies for a memory copy object and constructs the front and rear pointers of the linked list nodes. ObDList is an object containing the previous and next node pointers directly passed in by the user. Due to this feature of ObDList, it will be different from the method of using STL list.

ObDList does not manage memory and does not need to manage memory at all. Its template parameters do not have a memory allocator, only one `DLinkNode`. `DLinkNode` needs to contain the element objects you need, front and rear node pointers and implement some common operations (with assistance Implement base class), the declaration and some interfaces of ObDList are as follows:

```cpp
template <typename DLinkNode>
class ObDList;

/// Move all elements on the current linked list to list
int move(ObDList &list);

/// Get the head node (not the first element)
DLinkNode *get_header();
const DLinkNode *get_header() const;

/// Get the last element
DLinkNode *get_last();

/// Get the first element
const DLinkNode *get_first() const;
const DLinkNode *get_first_const() const;

/// Add a node to the tail
bool add_last(DLinkNode *e);

/// Add a node to the head
bool add_first(DLinkNode *e);

/// Add node at specified location
bool add_before(const DLinkNode *pos, DLinkNode *e);

/// Move the specified node to the front
bool move_to_first(DLinkNode *e);
/// Move the specified node to the end
bool move_to_last(DLinkNode *e);

/// Delete the last node
DLinkNode *remove_last();
/// Delete the first node
DLinkNode *remove_first();

/// Delete specified element
DLinkNode *remove(DLinkNode *e);

/// Clear linked list
void clear();

/// Insert another linked list at the beginning of the linked list
void push_range(ObDList<DLinkNode> &range);

/// Delete the specified number of elements from the beginning
/// and place the deleted elements in the range
void pop_range(int32_t num, ObDList<DLinkNode> &range);

/// Whether the linked list is empty
bool is_empty() const
/// Number of elements
int32_t get_size() const
```

OceanBase provides auxiliary `DLinkNode` implementations `ObDLinkNode` and `ObDLinkDerived`, making it easy to use ObDList simply by using either replication class.

Before introducing these two auxiliary classes, let's take a brief look at a basic auxiliary interface implementation `ObDLinkBase`, which is the base class of the above two auxiliary classes. It contains the front and rear node pointers required by ObDList and some basic node operations. Both auxiliary classes are implemented by inheriting the base class, and only use different methods.

The first auxiliary class, ObDLinkNode, is declared as follows:

```cpp
template<typename T>
struct ObDLinkNode: public ObDLinkBase<ObDLinkNode<T> >
```

Just give your own real linked list element type. The disadvantage is that when getting the linked list elements, you need to use `ObDLinkNode::get_data` to get your own object, such as

```cpp
class MyObj;
ObDList<ObDLinkNode<MyObj>> alist;

ObDLinkNode<MyObj> *anode = OB_NEW(ObDLinkNode<MyObj>, ...);
alist.add_last(anode);

ObDLinkNode<MyObj> *nodep = alist.get_first();
MyObj &myobj = nodep->get_data();
// do something with myobj
```

The second auxiliary class, ObDLinkDerived, is simpler to use than ObDLinkNode. Its declaration is as follows:

```cpp
template<typename T>
struct ObDLinkDerived: public ObDLinkBase<T>, T
```

Note that it directly inherits the template class T itself, that is, there is no need to obtain the real object through get_data like ObDLinkNode. You can directly use the method of T and copy the above example:

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

Since ObDList does not manage the memory of nodes, you need to be careful when using it particularly. Pay attention to managing the life cycle of each element. Before performing cleanup actions, such as `clear` and `reset`, the memory must be released first. The interface declaration of ObDList is very clear, but it is different from the naming convention of STL::list. You can directly refer to the interface declaration in the code `ob_dlist.h` and use it without listing it.

# Map
Map is a commonly used data structure, and its insertion and query efficiency are very high. Normally, there are two implementation methods for Map. One is a balanced search tree, typically a red-black tree. Common compilers use this method to implement it. The other is a hash table, which is unordered_map in STL.

There are many Maps implemented in OceanBase, including the balanced search tree implementation ObRbTree and hash maps suitable for different scenarios, such as ObHashMap, ObLinkHashMap and ObLinearHashMap.

> OceanBase implements many types of hash maps, but it is recommended to use the few introduced here unless you have a clear understanding of other implementations.

## ObHashMap
The implementation of ObHashMap is in ob_hashmap.h. In order to facilitate the understanding of the implementation of ObHashMap, I will introduce it with reference to STL::unordered_map.

### ObHashMap Introduction
In STL, unordered_map is declared as follows:
```cpp
template<
    class Key,
    class T,
    class Hash = std::hash<Key>,          /// Calculate hash value of Key
    class KeyEqual = std::equal_to<Key>,  /// Determine whether Key is equal
    class Allocator = std::allocator<std::pair<const Key, T>> /// memory allocator
> class unordered_map;
```

Key in the template parameters is our key, T is the type of our value, Hash is a class or function that calculates the hash value based on the key, KeyEqual is a method to determine whether two key values are equal, and Allocator is an allocator. An object is a pair of keys and values.

The declaration in OceanBase is similar:

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

Among them, `_key_type`, `_value_type`, `_hashfunc`, `_equal` have the same meaning as the declared parameters of STL::unordered_map. There are some more parameters here:

- `_defendmode`: OceanBase provides a thread-safe hashmap implementation with limited conditions. You can use the default value and ignore it for now, which will be introduced later;
- `_allocer` and `_bucket_allocer`: STL::unordered_map requires only one allocator, but here requires two allocators. In a hashmap, there is usually an array as a bucket array. After the elements are hashed, the corresponding bucket is found, and then the element is "mounted" on the corresponding bucket. `_bucket_allocer` is the allocator of the bucket array, and `_allocer` is the allocator of elements, that is, the allocator of key value pairs;
- EXTEND_RATIO: If EXTEND_RATIO is 1, no expansion will occur. Otherwise, the hash map is not thread-safe.

### ObHashMap Interface Introduction
```cpp
/**
  * The constructor of ObHashMap does nothing.
  * You must call create for actual initialization.
  * The parameters of the create function are mainly the number of buckets
  * (bucket_num) and the parameters of the memory allocator.
  * Providing a reasonable number of buckets can make hashmap run more efficiently
  * without wasting too much memory.
  *
  * As you can see from the following interfaces, two memory allocators can be
  * provided, one is the allocator of the bucket array,
  * and the other is the allocator of element nodes.
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

/// Destroy the current object directly
int destroy();

/// Both functions will delete all elements
int clear();
int reuse();

/**
  * Get the element value of the specified key value
  * Although the get function is also provided, it is recommended to use the current
  * function.
  * @param timeout_us: Timeout for getting elements. The implementation principle
  * of timeout will be introduced later.
  * @return found and returned successfully
  */
int get_refactored(const _key_type &key, _value_type &value, const int64_t timeout_us = 0) const;

/**
  * Set the value of a certain key value
  * @param flag: 0 means it already exists and will not be overwritten,
  *              otherwise the original value will be overwritten.
  * @param broadcast: whether to wake up the thread waiting to obtain the
  *                   current key
  * @param overwrite_key: not used. Please refer to flag
  * @param callback: After the insertion or update is successful, you can
  * use callback to perform some additional operations on the value.
  */
template <typename _callback = void>
int set_refactored(const _key_type &key,
                   const _value_type &value,
                   int flag = 0,
                   int broadcast = 0,
                   int overwrite_key = 0,
                   _callback *callback = nullptr);

/**
  * Traverse all elements
  * @note
  * 1. You cannot delete elements, insert, etc. during the traversal process.
  * Because some locks will be added during the traversal process, and locks
  * will also be added for insertion, deletion and other actions, lock
  * conflicts may occur;
  * 2. The callback action should be as small as possible because it works
  * within the lock scope.
  */
template<class _callback>
int foreach_refactored(_callback &callback) const;

/**
  * Delete the specified key value.
  * If the value pointer is not null, the corresponding element will be returned
  * @return If the element does not exist, OB_HASH_NOT_EXIST will be returned
  */
int erase_refactored(const _key_type &key, _value_type *value = NULL);

/**
 * Insert if it does not exist, otherwise call callback to update
 */
template <class _callback>
int set_or_update(const _key_type &key, const _value_type &value,
                  _callback &callback);

/**
 * Delete elements with specified key values and meeting specific conditions
 */
template<class _pred>
int erase_if(const _key_type &key, _pred &pred, bool &is_erased, _value_type *value = NULL);

/**
 * There is no need to copy elements, directly access the elements with
 * specified key values through callback.
 * @note callback executed under write lock protection
 */
template <class _callback>
int atomic_refactored(const _key_type &key, _callback &callback);

/**
 * There is no need to copy the element value, just get the element directly
 * and access it through callback.
 * @note callback executed under write lock protection
 */
template <class _callback>
int read_atomic(const _key_type &key, _callback &callback);
```

### Implementation of ObHashMap
Persons who are familiar with the implementation principle of STL unordered_map can definitely guess the implementation principle of ObHashMap. The implementation of ObHashMap is also a linear table, as a bucket array, and then uses the zipper table method to solve key hash conflicts. But here are some details, hoping to help everyone understand its implementation and use ObHashMap more efficiently.

ObHashMap relies on ObHashTable at the bottom. For the code, refer to `ob_hashtable.h`. ObHashMap just encapsulates the semantics of Key Value on ObHashTable.

**Conditional thread safe**

If the template parameter `_defendmode` selects a valid lock mode, and ObHashTable has a read-write lock for each bucket, then ObHashTable will provide conditional thread safety. When accessing elements on the bucket, corresponding locks will be added, including interfaces with `callback`, so the actions in `callback` should be as light as possible and other elements of ObHashTable should not be accessed to prevent deadlock.

ObHashMap is not thread-safe when scaling. If the provided template parameter EXTEND_RATIO is not 1, the capacity will be expanded when needed, and this is transparent to the user.

The default value of ObHashMap `_defendmode` is an effective thread-safe protection mode `LatchReadWriteDefendMode`.

**_defendmode**

_defendmode defines different bucket locking methods, and 6 modes are provided in `ob_hashutils.h`:

1. LatchReadWriteDefendMode
2. ReadWriteDefendMode
3. SpinReadWriteDefendMode
4. SpinMutexDefendMode
5. MultiWriteDefendMode
6. NoPthreadDefendMode

The first five of them can provide thread safety protection, but they use different lock modes. In different business scenarios and different thread read and write concurrency, choosing a reasonable mode can improve efficiency and stability. The sixth mode, `NoPthreadDefendMode`, does not provide any protection.

**get timeout waiting**

If the specified element does not exist when getting an element, you can set a waiting time. ObHashTable will insert a `fake` element into the corresponding bucket and wait. When another thread inserts the corresponding element, the waiting thread will be awakened. However, the thread inserting the element needs to explicitly specify that it needs to be awakened, that is, the broadcast value of set_refactor is set to non-zero.

## ObHashSet
Similar to ObHashMap, ObHashSet is based on ObHashTable and encapsulates an implementation with only keys and no values. Please refer to the code ob_hashset.h for details.

## ObLinkHashMap
ObLinkHashMap is a lock-free hash map that takes into account both read and write performance and is thread-safe (including expansion). It uses the zipper method to resolve hash conflicts.

Here are the characteristics of this class:

- Taking into account both reading and writing performance;
- Implement thread safety based on lock-free solution;
- Introducing the retirement station, the node will be delayed in release, so it is recommended that the Key be as small as possible;
- There is a certain amount of memory waste;
- When expanding or shrinking capacity, batch relocation is used;
- When there is a hotspot key, the get performance is poor due to reference counting issues;
- When the bucket is expanded too much, initializing Array will be slower.

> Regarding retire station, please refer to the paper [Reclaiming Memory for Lock-Free Data Structures:There has to be a Better Way](https://www.cs.utoronto.ca/%7Etabrown/debra/fullpaper.pdf)。

Below are some commonly used interfaces and precautions when using them.

```cpp
/**
  *Declaration of ObLinkHashMap
  * Template parameters:
  * @param Key Key type
  * @param Value The type of value, which needs to be inherited from
  * LinkHashValue (refer to ob_link_hashmap_deps.h)
  * @param AllocHandle Class to allocate release values and nodes
  * (refer to ob_link_hashmap_deps.h)
  * @param RefHandle Reference counting function. Don't modify it if you
  * don't deeply understand its principles.
  * @param SHRINK_THRESHOLD When the number of current nodes is too many or too
  * few, it will expand or shrink. Try to keep the current nodes at
  * Between the ratio [1/SHRINK_THRESHOLD, 1] (non-precise control)
  */
template<typename Key,
         typename Value,
         typename AllocHandle=AllocHandle<Key, Value>,
         typename RefHandle=RefHandle,
         int64_t SHRINK_THRESHOLD = 8>
class ObLinkHashMap;


/// Number of elements
int64_t size() const;

/**
 * Insert an element
 * @noteIf it returns successfully, you need to execute hash.revert(value)
 */
int insert_and_get(const Key &key, Value* value);

/// Delete specified element
int del(const Key &key);

/**
  * Get the specified element
  * @note If the return is successful, revert needs to be executed
  */
int get(const Key &key, Value*& value);

/// Releases the introduction count of the specified element.
/// Can be released across threads
void revert(Value* value);

/**
 * Determine whether the specified element exists
 * @return OB_ENTRY_EXIST indicating exists
 */
int contains_key(const Key &key);

/**
  * Traverse all elements
  * @param fn: bool fn(Key &key, Value *value); The bool return value
  * indicates whether to continue traversing
  */
template <typename Function> int for_each(Function &fn);

/**
  * Delete elements that meet the conditions
  * @param fn bool fn(Key &key, Value *value); The bool return value
  * indicates whether it needs to be deleted
  */
template <typename Function> int remove_if(Function &fn);
```

## ObRbTree
ObRbTree is a red-black tree implementation that supports basic operations such as insertion, deletion, and search, and is not thread-safe. Since ObRbTree is not used in OceanBase, it will not be introduced again. If you are interested, please read the source code `ob_rbtree.h`.


# Others
OceanBase also has many basic container implementations, such as some queues (ObFixedQueue, ObLightyQueue, ObLinkQueue), bitmap (ObBitmap), tuple (ObTuple), etc. If the common containers don't meet your needs, you can find more in the `deps/oblib/src/lib` directory.
