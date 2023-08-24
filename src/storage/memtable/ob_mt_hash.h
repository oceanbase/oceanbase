/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STRORAGE_MEMTABLE_OB_MT_HASH_
#define OCEANBASE_STRORAGE_MEMTABLE_OB_MT_HASH_

#include "lib/allocator/ob_allocator.h"
//#include "lib/hash/ob_hash_common.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h" // for dump row verbose

namespace oceanbase
{
namespace memtable
{
typedef ObStoreRowkeyWrapper Key;
class ObMvccRow;

// ---------------- common function ----------------
// common functions should be packed into library directory, temporarily placed here

// bit flip B63-B62-...-B1-B0 => B0-B1-...-B62-B63
OB_INLINE uint64_t bitrev(uint64_t x)
{
  x = (((x & 0xaaaaaaaaaaaaaaaaUL) >> 1) | ((x & 0x5555555555555555UL) << 1));
  x = (((x & 0xccccccccccccccccUL) >> 2) | ((x & 0x3333333333333333UL) << 2));
  x = (((x & 0xf0f0f0f0f0f0f0f0UL) >> 4) | ((x & 0x0f0f0f0f0f0f0f0fUL) << 4));
  x = (((x & 0xff00ff00ff00ff00UL) >> 8) | ((x & 0x00ff00ff00ff00ffUL) << 8));
  x = (((x & 0xffff0000ffff0000UL) >> 16) | ((x & 0x0000ffff0000ffff) << 16));
  return ((x >> 32) | (x << 32));
}

// ---------------- helper functions ----------------
// mark the lowest 2 bits
OB_INLINE static uint64_t mark_hash(const uint64_t key_hash)
{
  return (key_hash | 3ULL);
}

OB_INLINE int64_t back2idx(const int64_t bucket_hash)
{
  return bitrev(bucket_hash & (~0x3));
}

// ---------------- node definition ----------------
// the type of the array element is ObHashNode,
// the hash_ value is the bitrev of the array index
// the lowest 2 bits of hash_ represents is_bucket and is_filled respectively
struct ObHashNode
{
  ObHashNode *next_;
  uint64_t hash_;

  ObHashNode() : next_(NULL), hash_(0) {}
  ~ObHashNode() {}

  OB_INLINE void set_arr_idx(const int64_t idx)
  {
     ATOMIC_STORE(&hash_, (bitrev(idx)));
  }

  // the lowest 2 bits of the node's hash_ value are special flags:
  // 1. the lowest bit marks node type, 0 represents bucket_node and 1 represents mt_node
  // 2. the second lowest is used to control visibility, 0 means invisible and 1 means visible
  OB_INLINE void set_bucket_filled(const int64_t idx)
  {
    // idx is the array index of the node, and idx < arr_size << int64_max, so the lowest bit of hash_ is always 0
    // idx and node are in one-to-one respondence(such respondence is the same throughtout all threads)
    // so overwrites hash_ directly regardless of the result of set_arr_idx
   ATOMIC_STORE(&hash_, (bitrev(idx) | 2ULL));
  }
  OB_INLINE bool is_bucket_filled() const
  {
    return (ATOMIC_LOAD(&hash_) & 2ULL);
  }
  OB_INLINE bool is_bucket_node() const
  {
    return 0 == (ATOMIC_LOAD(&hash_) & 1ULL);
  }
};

// stores ObMemtableKey object instead of pointers
// long term consideration:
//   stores pointer along with btree
//   allocate once for ObMemtableKey object itself and it's obj array(objs_[0])
//   saves memory usage of hash/btree node(need one indirect reference for row_key comparison any way)
struct ObMtHashNode : public ObHashNode
{
  Key key_;
  ObMvccRow *value_;

  ObMtHashNode() : key_(), value_(NULL) {}
  ObMtHashNode(const Key &mtk) : key_(mtk), value_(NULL)
  {
    // the tail of data node is always 1
    hash_ = mark_hash(mtk.hash());
  }
  ObMtHashNode(const Key &mtk, const ObMvccRow *value)
    : key_(mtk),
      value_(const_cast<ObMvccRow*>(value))
  {
    hash_ = mark_hash(mtk.hash());
  }
  ~ObMtHashNode() { value_ = NULL; }
};

// caller gurantees non-empty parameters
OB_INLINE static int compare_node(const ObHashNode *n1, const ObHashNode *n2, int &cmp)
{
  int ret = common::OB_SUCCESS;
  const Key &mtk1 = (static_cast<const ObMtHashNode*>(n1))->key_;
  const Key &mtk2 = (static_cast<const ObMtHashNode*>(n2))->key_;
  bool is_equal = false;
  cmp = 0;
  if (OB_LIKELY(n1->hash_ > n2->hash_)) {
    cmp = 1;
  } else if (n1->hash_ < n2->hash_) {
    cmp = -1;
  } else if (n1->is_bucket_node()) {
    // do nothing
  } else if (OB_FAIL(mtk1.equal(mtk2, is_equal))) {
    TRANS_LOG(ERROR, "failed to compare", KR(ret), K(mtk1), K(mtk2));
  } else if (is_equal) {
    cmp = 0;
  } else if (OB_FAIL(mtk1.compare(mtk2, cmp))) {
    TRANS_LOG(ERROR, "failed to compare", KR(ret), K(mtk1), K(mtk2));
  } else {
    // do nothing
  }
  return ret;
}

// ---------------- array implementation ----------------
template<int64_t PAGE_SIZE>
class ObMtArrayBase
{
private:
  static const int64_t DIR_SIZE = PAGE_SIZE / sizeof(ObHashNode*);
  static const int64_t SEG_SIZE = PAGE_SIZE / sizeof(ObHashNode);
  static ObHashNode * const PLACE_HOLDER;
public:
  static const int64_t ARRAY_CAPABILITY = DIR_SIZE * SEG_SIZE;
public:
  explicit ObMtArrayBase(common::ObIAllocator &allocator)
    : allocator_(allocator),
      dir_(nullptr),
      alloc_memory_(0)
  {
  }
  ~ObMtArrayBase() { destroy(); }
  void destroy() {
    dir_ = nullptr;
    alloc_memory_ = 0;
  }
  int64_t get_alloc_memory() const { return ATOMIC_LOAD(&alloc_memory_) + sizeof(*this); }
  // 1. caller guraantees the validity of idx
  // 2. allocate dir_/seg on demand
  // 3. return the node pointer of the corresponding position of ret_node
  OB_INLINE int at(const int64_t idx, ObHashNode *&ret_node)
  {
    int ret = common::OB_SUCCESS;
    ObHashNode **dir = NULL;
    ObHashNode *seg = NULL;
    if (OB_FAIL(load_dir(dir))) {
    } else if (OB_FAIL(load_seg(dir, idx / SEG_SIZE, seg))) {
    } else {
      ret_node = seg + (idx % SEG_SIZE);
    }
    return ret;
  }
  int64_t to_string(char *buf, int64_t buf_len) const
  {
    return snprintf(buf, buf_len, ", dir_=%p", dir_);
  }
private:
  // return values:
  //   OB_ALLOCATE_MEMORY_FAILED : no memory
  //   OB_ENTRY_NOT_EXIST : current node is allocating
  //   common::OB_SUCCESS : ret_dir is definitely valid
  OB_INLINE int load_dir(ObHashNode **&ret_dir)
  {
    int ret = common::OB_SUCCESS;
    ret_dir = ATOMIC_LOAD(&dir_);
    if (OB_NOT_NULL(ret_dir) && OB_UNLIKELY(reinterpret_cast<ObHashNode**>(PLACE_HOLDER) != ret_dir)) {
      // good, do nothing
    } else if (NULL == ret_dir) {
      if (ATOMIC_BCAS(&dir_, NULL, reinterpret_cast<ObHashNode**>(PLACE_HOLDER))) {
        if (OB_NOT_NULL(ret_dir = reinterpret_cast<ObHashNode**>(allocator_.alloc(PAGE_SIZE)))) {
          ATOMIC_FAA(&alloc_memory_, PAGE_SIZE);
          memset(ret_dir, 0, PAGE_SIZE);
          if (OB_LIKELY(ATOMIC_BCAS(&dir_, reinterpret_cast<ObHashNode**>(PLACE_HOLDER), ret_dir))) {
          } else {
            // place_holder is a lock
            ret = common::OB_ERR_UNEXPECTED;
          }
        } else {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
        }
      } else {
        ret = common::OB_ENTRY_NOT_EXIST;
      }
    } else {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }

  // return value:
  // OB_ALLOCATE_MEMORY_FAILED : no memory
  // OB_ENTRY_NOT_EXIST : current node is allocating
  // common::OB_SUCCESS : ret_dir is definitely valid
  OB_INLINE int load_seg(ObHashNode **dir, const int64_t seg_idx, ObHashNode *&ret_seg)
  {
    int ret = common::OB_SUCCESS;
    ret_seg = ATOMIC_LOAD(dir + seg_idx);
    if (OB_NOT_NULL(ret_seg) && OB_LIKELY(PLACE_HOLDER != ret_seg)) {
      // good, do nothing
    } else if (NULL == ret_seg) {
      if (ATOMIC_BCAS(dir + seg_idx, NULL, PLACE_HOLDER)) {
        if (OB_NOT_NULL(ret_seg = reinterpret_cast<ObHashNode*>(allocator_.alloc(PAGE_SIZE)))) {
          ATOMIC_FAA(&alloc_memory_, PAGE_SIZE);
          memset(ret_seg, 0, PAGE_SIZE); // make sure all nodes are invalid
          if (OB_LIKELY(ATOMIC_BCAS(dir + seg_idx, PLACE_HOLDER, ret_seg))) {
          } else {
            ret = common::OB_ERR_UNEXPECTED;
          }
        } else {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
        }
      } else {
        ret = common::OB_ENTRY_NOT_EXIST;
      }
    } else {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }
private:
  common::ObIAllocator &allocator_;
  ObHashNode** dir_;
  int64_t alloc_memory_;
};

template<int64_t PAGE_SIZE>
ObHashNode * const ObMtArrayBase<PAGE_SIZE>::PLACE_HOLDER = (ObHashNode *)0x1;

class ObMtArray
{
public:
  explicit ObMtArray(common::ObIAllocator &allocator)
    : small_arr_(allocator),
      large_arr_(allocator)
  {
  }
  ~ObMtArray() { destroy(); }
  void destroy()
  {
    small_arr_.destroy();
    large_arr_.destroy();
  }
  int64_t get_alloc_memory() const { return small_arr_.get_alloc_memory() + large_arr_.get_alloc_memory();}
  // caller ensures idx is valid
  OB_INLINE int at(const int64_t idx, ObHashNode *&ret_node)
  {
    int ret = common::OB_SUCCESS;
    if (idx < SMALL_CAPABILITY) {
      ret = small_arr_.at(idx, ret_node);
    } else {
      ret = large_arr_.at(idx, ret_node);
    }
    return ret;
  }
  int64_t to_string(char *buf, int64_t buf_len) const
  {
    int64_t len = small_arr_.to_string(buf, buf_len);
    len += large_arr_.to_string(buf + len, buf_len - len);
    return len;
  }
private:
  // TODO memstore_allocator misc
  static const int64_t MY_NORMAL_BLOCK_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  static const int64_t MY_BIG_BLOCK_SIZE = common::OB_MALLOC_BIG_BLOCK_SIZE - 64;
public:
  // SMALL_CAPABILITY 52万，TOTAL_CAPABILITY 343亿
  static const int64_t SMALL_CAPABILITY = ObMtArrayBase<MY_NORMAL_BLOCK_SIZE>::ARRAY_CAPABILITY;
  static const int64_t TOTAL_CAPABILITY = ObMtArrayBase<MY_BIG_BLOCK_SIZE>::ARRAY_CAPABILITY + SMALL_CAPABILITY;
private:
  ObMtArrayBase<MY_NORMAL_BLOCK_SIZE> small_arr_;
  ObMtArrayBase<MY_BIG_BLOCK_SIZE> large_arr_;
};

// ---------------- hash implementation ----------------
// don't use generic: QueryEngine uses template, and instantiates with type ObMemtableKey,
// uses the type directly here
// consider to remove generic from QueryEgnine<ObMemtableKey> in the future
class ObMtHash
{
private:
  // bucket link of parent-child relation
  static const int64_t GENEALOGY_LEN = 64;
  struct Parent
  {
    Parent(): bucket_node_(nullptr), bucket_idx_(0) {}
    ObHashNode *bucket_node_;
    int64_t bucket_idx_;
    TO_STRING_KV(KP(bucket_node_), K(bucket_idx_));
  };
  struct Genealogy
  {
    struct Parent list_[GENEALOGY_LEN];
    int64_t depth_;

    Genealogy() : depth_(0) {}
    OB_INLINE void append_parent(ObHashNode *node, int64_t idx)
    {
      // depth_ is not more than 64
      if (OB_UNLIKELY(depth_ >= GENEALOGY_LEN)) {
        TRANS_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "unexpect, genealogy is too deep", K(depth_));
      } else {
        list_[depth_].bucket_node_ = node;
        list_[depth_].bucket_idx_ = idx;
      }
      ++depth_;
    }
    OB_INLINE ObHashNode* get_young()
    {
      return 0 == depth_ ? NULL : list_[0].bucket_node_;
    }
    TO_STRING_KV(K(depth_), K(list_));
  };

public:
  explicit ObMtHash(common::ObIAllocator &allocator)
    : allocator_(allocator),
      arr_(allocator),
      arr_size_(INIT_HASH_SIZE)
  {
    // tail_node_ is a sentinel node, there will be no problem
    // even the data node has a hash_ value 0xFF..FF
    tail_node_.hash_ = 0xFFFFFFFFFFFFFFFF; // can be any value
    tail_node_.next_ = NULL;
    zero_node_.set_bucket_filled(0);
    zero_node_.next_ = &tail_node_;
  }
  ~ObMtHash() { destroy(); }
  void destroy()
  {
    arr_.destroy();
    arr_size_ = 0;
    tail_node_.hash_ = 0xFFFFFFFFFFFFFFFF; // can be any value
    tail_node_.next_ = NULL;
    zero_node_.set_bucket_filled(0);
    zero_node_.next_ = &tail_node_;
  }
  int64_t get_arr_size() const { return ATOMIC_LOAD(&arr_size_); }
  int64_t get_alloc_memory() const { return sizeof(*this) + arr_.get_alloc_memory() + get_arr_size() * sizeof(ObMtHashNode) - sizeof(arr_);}
  // keep the interface unchanged, so as to not modify caller's code
  int get(const Key *query_key,
          ObMvccRow *&ret_value,
          const Key *&copy_inner_key)
  {
    // This is for history database, which has plenty of data but merely modifies them,
    // so as to avoid occupying unnecessary memory by empty memtable, otherwise
    // looking up in hash index of a memtable would
    // trigger allocating dir/seg(each memtabale is aat least 16KB)
    int ret = common::OB_ENTRY_NOT_EXIST;
    if (!is_empty()) {
      ret = do_get(query_key, ret_value, copy_inner_key);
    }
    return ret;
  }

  int get(const Key *query_key, ObMvccRow *&ret_value)
  {
    int ret = common::OB_ENTRY_NOT_EXIST;
    const Key *trival_copy_inner_key = NULL;
    if (!is_empty()) {
      ret = do_get(query_key, ret_value, trival_copy_inner_key);
    }
    return ret;
  }

  int insert(const Key *insert_key, const ObMvccRow *insert_value)
  {
    int ret = common::OB_SUCCESS;
    const uint64_t insert_key_hash = mark_hash(insert_key->hash());
    const uint64_t insert_key_so_hash = bitrev(insert_key_hash);
    ObHashNode *bucket_node = NULL;
    Genealogy genealogy;
    const int64_t arr_size = ATOMIC_LOAD(&arr_size_);
    if (OB_FAIL(get_bucket_node(arr_size, insert_key_so_hash, bucket_node, genealogy))) {
      // no memory, do nothing
    } else {
      ObHashNode *op_bucket_node = fill_bucket(bucket_node, genealogy);
      if (OB_FAIL(insert_mt_node(insert_key, insert_key_hash, insert_value, op_bucket_node))) {
        if (common::OB_ENTRY_EXIST != ret && common::OB_ALLOCATE_MEMORY_FAILED == ret) {
          TRANS_LOG(WARN, "insert mt_node error", K(ret), K(insert_key), KP(insert_value));
        }
      }
      TRANS_LOG(TRACE, "insert", K(genealogy), KP(bucket_node), K(op_bucket_node));
    }
    return ret;
  }

  void dump_hash(FILE* fd,
                 const bool print_bucket,
                 const bool print_row_value,
                 const bool print_row_value_verbose) const
  {
    dump_meta_info(fd);
    dump_list(fd, print_bucket, print_row_value, print_row_value_verbose);
  }

private:
  OB_INLINE bool is_empty() const
  {
    return ATOMIC_LOAD(&(zero_node_.next_)) == &tail_node_;
  }
  OB_INLINE static int64_t get_arr_idx(const int64_t query_key_so_hash,
                                       const int64_t bucket_count)
  {
    return (query_key_so_hash & (bucket_count - 1));
  }

  int get_bucket_node(const int64_t arr_size,
                      const uint64_t query_key_so_hash,
                      ObHashNode *&bucket_node,
                      Genealogy &genealogy)
  {
    int ret = common::OB_SUCCESS;
    int64_t bucket_count = common::next_pow2(arr_size);
    int64_t arr_idx = bucket_count;
    // if the insert position is very forward and arr_size is big, there is no
    // need to access the same element repeatedly when recursively looking up, just decrease by half.
    // Note: bucket_count can decrease more rapidly, but as long as not access array elements,
    // it's accesptabale to loop multiple times(< 64) to decreasse by half.
    int64_t last_arr_idx = arr_idx;

    while (OB_SUCC(ret) && arr_idx > 0) {
      // find parent bucket recursively, until find a filled bucket or reach zero node(always filled)
      arr_idx = get_arr_idx(query_key_so_hash, bucket_count);
      if (OB_UNLIKELY(0 == arr_idx)) {
        bucket_node = &zero_node_;
      } else if (arr_idx == last_arr_idx || arr_idx >= arr_size) {
        bucket_count >>= 1;
      } else {
        last_arr_idx = arr_idx;
        ret = arr_.at(arr_idx, bucket_node);
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(!bucket_node->is_bucket_filled())) {
            bucket_count >>= 1;
            genealogy.append_parent(bucket_node, arr_idx);
          } else {
            break;
          }
        } else if (common::OB_ENTRY_NOT_EXIST == ret) {
          // current node is allocating, take it as invisible, and go on looking up parent bucket_node
          bucket_count >>= 1;
          ret = common::OB_SUCCESS;
        } else {
          // no memory
        }
      }
    } // end while
    // TRANS_LOG(TRACE, "get_bucket_node", K(arr_size), K(bucket_count), K(query_key_so_hash), K(arr_idx), K(bucket_node), K(genealogy));
    return ret;
  }

  OB_INLINE bool not_reach_list_tail(const ObHashNode *next)
  {
    return &tail_node_ != next;
  }

  // returns the comparison result between target node and
  // the first node which is greater than or equal to target node
  OB_INLINE int search_sub_range_list(ObHashNode *bucket_node,
                                      ObHashNode *target_node,
                                      ObHashNode *&prev_node,
                                      ObHashNode *&next_node,
                                      int &cmp)
  {
    int ret = common::OB_SUCCESS;
    cmp = 1;
    prev_node = bucket_node;
    // prev < target < next
    // find the first node >= target_node or reaches the end of link list
    while (not_reach_list_tail(next_node = ATOMIC_LOAD(&(prev_node->next_)))
           && (OB_SUCC(compare_node(target_node, next_node, cmp)))
           && cmp > 0) {
      prev_node = next_node;
    }
    return ret;
  }

  // the time and process of filling bucket
  //   each get/insert operation first looks up recursively until finding an already filled bucket.
  //   during the process, record the unfilled bucket by it's recursive order, and fill them serializably
  //   from the smallest idx. when filling bucket whith multiple threads, current thread need to wait until
  //   other threads has completed filling buckets. after filling, do insert/get from the last bucket
  //   Note: filling bucket is a lazy operation, if there is no insert/get operation
  //   within a sub range, bucket filling will not be triggered.
  // fill_bucket returns the last bucket used for operation.
  OB_INLINE ObHashNode* fill_bucket(ObHashNode *parent_bucket_node, Genealogy &genealogy)
  {
    ObHashNode *parent = parent_bucket_node;
    ObHashNode *child = NULL;
    for (int64_t pos = genealogy.depth_ - 1; pos >= 0; pos--) {
      child = genealogy.list_[pos].bucket_node_;
      fill_pair(parent, child, genealogy.list_[pos].bucket_idx_);
      parent = child;
    }
    ObHashNode *op_bucket_node = genealogy.get_young();
    return ((NULL != op_bucket_node) ? op_bucket_node: parent_bucket_node);
  }

  // repeat until success. if multiple threads fill the same bucket,
  // only one thread is allowed to fill, and other threads will wait until success
  void fill_pair(ObHashNode *parent_bucket_node, ObHashNode* child_bucket_node, int64_t child_bucket_idx)
  {
    if (ATOMIC_BCAS(&(child_bucket_node->next_), NULL, reinterpret_cast<ObHashNode*>(0x02))) {
      // one thread is responsible for filling the bucket
      ObMtHashNode target_node;
      target_node.set_arr_idx(child_bucket_idx);
      while (true) {
        ObHashNode *prev_node = NULL;
        ObHashNode *next_node = NULL;
        int cmp = 0;
        int ret = common::OB_SUCCESS;
        if (OB_FAIL(search_sub_range_list(parent_bucket_node, &target_node, prev_node, next_node, cmp))) {
          break;
        }
        ATOMIC_STORE(&(child_bucket_node->next_), next_node); // next_nodewould not be NULL
        child_bucket_node->set_arr_idx(child_bucket_idx); // fill hash_ and mark as invisible
        if (OB_LIKELY(ATOMIC_LOAD(&(prev_node->next_)) == next_node)
            && OB_LIKELY(ATOMIC_BCAS(&(prev_node->next_), next_node, child_bucket_node))) {
          // make the bucket_node visible to look up queries
          child_bucket_node->set_bucket_filled(child_bucket_idx);
          break;
        } else {
          TRANS_LOG(TRACE, "try insert fill error", KP(prev_node), KP(child_bucket_node),
                    K(child_bucket_idx), K(next_node));
        }
      }
    } else {
      // child_bucket_node is filling, wait for it's completion
      while (!child_bucket_node->is_bucket_filled()) {
        PAUSE();
      }
    }
  }
  int do_get(const Key *query_key,
             ObMvccRow *&ret_value,
             const Key *&copy_inner_key)
  {
    int ret = common::OB_SUCCESS;
    const uint64_t query_key_hash = mark_hash(query_key->hash());
    const uint64_t query_key_so_hash = bitrev(query_key_hash);
    ObHashNode *bucket_node = NULL;
    Genealogy genealogy;
    const int64_t arr_size = ATOMIC_LOAD(&arr_size_);
    if (OB_FAIL(get_bucket_node(arr_size, query_key_so_hash, bucket_node, genealogy))) {
      // no memory, do nothing
    } else {
      ObHashNode *op_bucket_node = fill_bucket(bucket_node, genealogy);
      ObMtHashNode target_node(*query_key);
      ObHashNode *prev_node = NULL;
      ObHashNode *next_node = NULL;
      int cmp = 0;
      if (OB_FAIL(search_sub_range_list(op_bucket_node, &target_node, prev_node, next_node, cmp))) {
        // do nothing
      } else if (0 == cmp) {
        // find the key
        ObMtHashNode *catched_node = static_cast<ObMtHashNode*>(next_node);
        copy_inner_key = &(catched_node->key_);
        ret_value = catched_node->value_;
      } else {
        // the first node >= target is strictly greater than target,
        // or searches the whole link list
        ret = common::OB_ENTRY_NOT_EXIST;
      }
      TRANS_LOG(DEBUG, "do_get finish", K(ret), K(arr_size), K(query_key_so_hash), KP(bucket_node),
                K(op_bucket_node), K(genealogy), KP(prev_node), KP(next_node));
    }
    return ret;
  }
  int insert_mt_node(const Key *insert_key,
                     const int64_t insert_key_hash,
                     const ObMvccRow *insert_row,
                     ObHashNode *bucket_node)
  {
    ObMtHashNode target_node(*insert_key);
    ObHashNode *prev_node = NULL;
    ObHashNode *next_node = NULL;
    ObMtHashNode *new_mt_node = NULL; // allocate at most once no matter how many times repeated
    int ret = common::OB_EAGAIN;
    while (common::OB_EAGAIN == ret) {
      int cmp = 0;
      if (OB_FAIL(search_sub_range_list(bucket_node, &target_node, prev_node, next_node, cmp))) {
        // do nothing
      } else if (FALSE_IT(ret = common::OB_EAGAIN)) {
        // do nothing
      } else if (0 == cmp) {
        // mtk already exists
        ret = common::OB_ENTRY_EXIST;
      } else {
        // alloc if necessary
        if (OB_LIKELY(NULL == new_mt_node)) {
          void *buf = NULL;
          if (OB_NOT_NULL(buf = allocator_.alloc(sizeof(ObMtHashNode)))) {
            new_mt_node = new (buf) ObMtHashNode(*insert_key, insert_row);
          }
        }
        // insert new mt_node
        if (OB_ISNULL(new_mt_node)) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
        } else {
          new_mt_node->next_ = next_node;
          if (ATOMIC_LOAD(&(prev_node->next_)) == next_node
              && ATOMIC_BCAS(&(prev_node->next_), next_node, new_mt_node)) {
            try_extend(insert_key_hash);
            ret = common::OB_SUCCESS;
          } else {
            // insert new_mt_node error
            // retry
          }
        }
      }
    }
    return ret;
  }

  OB_INLINE void try_extend(const int64_t random_hash)
  {
    // use Bit[26~16] as random value
    const int64_t FLUSH_LIMIT = (1 << 10);
    if (OB_UNLIKELY(0 == ((random_hash >> 16) & (FLUSH_LIMIT - 1)))) {
      ATOMIC_FAA(&arr_size_, FLUSH_LIMIT);
    }
  }

  void dump_meta_info(FILE *fd) const
  {
    int ret = OB_SUCCESS;
    int64_t len = 0;
    const int64_t DUMP_BUF_LEN = 8 * 1024;
    HEAP_VAR(char[DUMP_BUF_LEN], buf)
    {
      memset(buf, 0, DUMP_BUF_LEN);
      len = snprintf(buf, DUMP_BUF_LEN, "zero_node_=%p, tail_node_=%p, arr_size_=%ld",
                     &zero_node_, &tail_node_, arr_size_);
      len += arr_.to_string(buf + len, DUMP_BUF_LEN - len);
      fprintf(fd, "%s\n", buf);
    }
  }

  void dump_list(FILE *fd,
                 const bool print_bucket,
                 const bool print_row_value,
                 const bool print_row_value_verbose) const
  {
    int ret = OB_SUCCESS;
    int64_t count = 0;
    int64_t bucket_node_count = 0;
    int64_t mt_node_count = 0;
    const int64_t PRINT_LIMIT = 10L * 1000L * 1000L * 1000L; // large enough
    ObHashNode *node = const_cast<ObHashNode*>(&zero_node_);
    const int64_t DUMP_BUF_LEN = 16 * 1024;
    HEAP_VAR(char[DUMP_BUF_LEN], buf)
    {
      while (count < PRINT_LIMIT && node != &tail_node_) {
        if (!node->is_bucket_node()) {
          mt_node_count++;
          fprintf(fd, "[%12ld] |  mt_node,    | addr=%14p | next_=%14p | hash_=%20lu=%16lx",
                  count, node, node->next_, node->hash_, node->hash_);
          int64_t pos = 0;
          memset(buf, 0, DUMP_BUF_LEN);
          ObMtHashNode *mt_node = static_cast<ObMtHashNode*>(node);
          Key &mtk = mt_node->key_;
          ObMvccRow *row = mt_node->value_;
          pos = mtk.to_string(buf, DUMP_BUF_LEN);
          if (pos < DUMP_BUF_LEN) {
            (void)snprintf(buf, DUMP_BUF_LEN - pos, " | mvcc_row_addr=%p, ", row);
            if (NULL != row && print_row_value) {
              pos += row->to_string(buf, DUMP_BUF_LEN - pos, print_row_value_verbose);
            }
          }
          fprintf(fd, "%s\n", buf);
        } else if (print_bucket) {
          bucket_node_count++;
          fprintf(fd, "[%12ld] |  bucket_node | addr=%14p | next_=%14p | hash_=%20lu=%16lx | back2idx=%ld\n",
                  count, node, node->next_, node->hash_, node->hash_, back2idx(node->hash_));
        } else {
          // do nothing
        }
        node = node->next_;
        count++;
      }
      if (node != &tail_node_) {
        fprintf(fd, "ERROR dump_list terminated, bucket_node_count=%ld, mt_node_count=%ld, count=%ld\n",
                bucket_node_count, mt_node_count, count);
      } else {
        fprintf(fd, "SUCCESS dump_list finish, bucket_node_count=%ld, mt_node_count=%ld, count=%ld\n",
                bucket_node_count, mt_node_count, count);
      }
    }
  }
private:
  static const int64_t INIT_HASH_SIZE = 128;
private:
  common::ObIAllocator &allocator_;
  ObHashNode zero_node_; // for idx=0, always available
  ObHashNode tail_node_; // tail node, a sentinel node, never be accessed
  ObMtArray arr_;
  int64_t arr_size_ CACHE_ALIGNED;      // size of arr_
};

} // namespace memtable
} // namespace oceanbase

#endif
