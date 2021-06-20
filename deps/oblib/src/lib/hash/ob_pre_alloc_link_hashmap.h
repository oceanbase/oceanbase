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

#ifndef SRC_LIBRARY_SRC_LIB_HASH_OB_MEMLESS_LINK_HASHMAP_H_
#define SRC_LIBRARY_SRC_LIB_HASH_OB_MEMLESS_LINK_HASHMAP_H_

#include "lib/ob_define.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array.h"
#include "lib/lock/ob_bucket_lock.h"

namespace oceanbase {
namespace common {
namespace hash {

// ObMemLessLinkHashMap is convenient for reuse of node.
// there is a simple allocator in the map.
// memory of node should be pre-allocated.
// use of storage of table and meta block.
template <class KEY, class ITEM>
struct ObPreAllocLinkHashNode {
  explicit ObPreAllocLinkHashNode(ITEM& item) : item_(item)
  {}
  virtual ~ObPreAllocLinkHashNode()
  {}
  virtual OB_INLINE bool equals(const ObPreAllocLinkHashNode& node)
  {
    return equals(node.get_key());
  }
  virtual OB_INLINE bool equals(const KEY& key)
  {
    return get_key() == key;
  }
  // derived class should override static uint64_t hash(const uint64_t &key);

  virtual const KEY& get_key() const = 0;
  VIRTUAL_TO_STRING_KV(KP(this), K_(item));

  ITEM& item_;
};

template <class KEY, class ITEM, class NODE, class ITEM_PROTECTOR>
class ObPreAllocLinkHashMap {
public:
  class ForeachFunctor {
  public:
    virtual int operator()(ITEM& item, bool& is_full) = 0;
  };

  class EraseChecker {
  public:
    virtual int operator()(ITEM& item) = 0;
  };

  class GetFunctor {
  public:
    virtual int operator()(ITEM& item) = 0;
  };

  class Iterator {
  public:
    explicit Iterator(ObPreAllocLinkHashMap& map) : items_(), item_idx_(0), bucket_pos_(0), map_(map)
    {}
    virtual ~Iterator()
    {
      release_items();
    }
    int get_next(ITEM*& item)
    {
      int ret = OB_SUCCESS;
      item = NULL;

      while (OB_SUCC(ret)) {
        if (item_idx_ < items_.count()) {
          item = items_.at(item_idx_);
          ++item_idx_;
          break;
        } else if (bucket_pos_ >= map_.buckets_count_) {
          ret = OB_ITER_END;
        } else {
          item_idx_ = 0;
          release_items();
          ObBucketRLockGuard guard(map_.buckets_lock_, bucket_pos_);
          if (NULL != map_.buckets_[bucket_pos_]) {
            NODE* node = map_.buckets_[bucket_pos_];
            while (OB_SUCC(ret) && NULL != node) {
              ITEM_PROTECTOR::hold(node->item_);
              if (OB_FAIL(items_.push_back(&node->item_))) {
                COMMON_LOG(WARN, "Failed to add item", K(ret));
                ITEM_PROTECTOR::release(node->item_);
              } else {
                node = node->next_;
              }
            }
          }
          ++bucket_pos_;
        }
      }
      return ret;
    }

  private:
    void release_items()
    {
      for (int64_t i = 0; i < items_.count(); ++i) {
        ITEM_PROTECTOR::release(*items_.at(i));
      }
      items_.reuse();
    }
    common::ObArray<ITEM*> items_;
    int64_t item_idx_;
    int64_t bucket_pos_;
    ObPreAllocLinkHashMap& map_;
    DISALLOW_COPY_AND_ASSIGN(Iterator);
  };

  ObPreAllocLinkHashMap()
      : is_inited_(false), buckets_lock_(), count_(), buckets_(NULL), buckets_count_(1), allocator_()
  {}
  virtual ~ObPreAllocLinkHashMap()
  {
    destroy();
  }

  void destroy()
  {
    for (uint64_t bucket_pos = 0; NULL != buckets_ && bucket_pos < buckets_count_; ++bucket_pos) {
      ObBucketRLockGuard bucket_guard(buckets_lock_, bucket_pos);
      NODE* cur = buckets_[bucket_pos];
      NODE* next = NULL;
      while (NULL != cur) {
        next = cur->next_;
        free_node(cur);
        cur = next;
      }
    }
    is_inited_ = false;
    buckets_lock_.destroy();
    count_ = 0;
    ob_free(buckets_);
    buckets_ = NULL;
    buckets_count_ = 1;
  }

  int init(const int64_t buckets_count, const uint32_t latch_id, const lib::ObLabel& label)
  {
    int ret = OB_SUCCESS;
    ObMemAttr mem_attr(OB_SERVER_TENANT_ID, label);
    const int64_t real_buckets_count = hash::cal_next_prime(buckets_count);

    if (is_inited_) {
      ret = OB_INIT_TWICE;
      COMMON_LOG(WARN, "cannot init twice", K(ret));
    } else if (real_buckets_count <= 0 || buckets_count <= 0) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid bucket count", K(ret), K(real_buckets_count), K(buckets_count));
    } else if (OB_FAIL(buckets_lock_.init(real_buckets_count, latch_id, label))) {
      COMMON_LOG(WARN, "failed to init buckets lock", K(ret));
    } else if (OB_ISNULL(
                   buckets_ = reinterpret_cast<NODE**>(ob_malloc(sizeof(NODE*) * real_buckets_count, mem_attr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc buckets", K(ret), K(real_buckets_count));
    } else {
      allocator_.set_label(label);
      MEMSET(buckets_, 0, sizeof(NODE*) * real_buckets_count);
      COMMON_LOG(INFO,
          "init hashmap",
          K(buckets_count),
          K(real_buckets_count),
          "buf_size",
          sizeof(NODE*) * real_buckets_count,
          K(latch_id),
          K(label));
      count_ = 0;
      buckets_count_ = real_buckets_count;
      is_inited_ = true;
    }
    return ret;
  }

  uint64_t get_count() const
  {
    return ATOMIC_LOAD(&count_);
  }
  uint64_t get_buckets_count() const
  {
    return buckets_count_;
  }

  NODE* alloc_node(ITEM& item)
  {
    return allocator_.alloc(item);
  }

  void free_node(NODE*& node)
  {
    allocator_.free(node);
    node = NULL;
  }

  int put(NODE& node)
  {
    int ret = OB_SUCCESS;

    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "not inited", K(ret));
    } else {
      const uint64_t bucket_pos = NODE::hash(node.get_key()) % buckets_count_;
      ObBucketWLockGuard bucket_guard(buckets_lock_, bucket_pos);
      NODE* cur = buckets_[bucket_pos];
      while (NULL != cur) {
        if (cur->equals(node)) {
          break;
        } else {
          cur = cur->next_;
        }
      }

      if (NULL != cur) {
        ret = OB_HASH_EXIST;
      } else {
        node.next_ = buckets_[bucket_pos];
        buckets_[bucket_pos] = &node;
        ATOMIC_INC(&count_);
      }
    }

    return ret;
  }

  // delete node which has common key
  int erase(const KEY& key, ITEM*& del_item, EraseChecker* checker = NULL)
  {
    int ret = OB_SUCCESS;
    del_item = NULL;

    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "not inited", K(ret));
    } else {
      const uint64_t bucket_pos = NODE::hash(key) % buckets_count_;
      ObBucketWLockGuard bucket_guard(buckets_lock_, bucket_pos);
      NODE* cur = buckets_[bucket_pos];
      NODE* prev = NULL;
      while (NULL != cur) {
        if (cur->equals(key)) {
          break;
        } else {
          prev = cur;
          cur = cur->next_;
        }
      }

      if (NULL == cur) {
        ret = OB_HASH_NOT_EXIST;
      } else if (NULL != checker && OB_FAIL((*checker)(cur->item_))) {
        // cannot erase now
      } else {
        if (NULL != prev) {
          prev->next_ = cur->next_;
        } else {
          buckets_[bucket_pos] = cur->next_;
        }
        cur->next_ = NULL;
        del_item = &cur->item_;
        free_node(cur);
        ATOMIC_DEC(&count_);
      }
    }

    return ret;
  }

  // delete node which has common key
  int erase(const KEY& key)
  {
    int ret = OB_SUCCESS;
    ITEM* del_item = NULL;
    if (OB_FAIL(erase(key, del_item))) {
      COMMON_LOG(WARN, "failed to erase ndoe", K(ret), K(key));
    }
    return ret;
  }

  int exist(const KEY& key)
  {
    ITEM* item = NULL;
    int ret = get(key, item);
    if (OB_SUCCESS == ret) {
      ret = OB_HASH_EXIST;
    }
    return ret;
  }

  int get(const KEY& key, GetFunctor& functor)
  {
    ITEM* item = NULL;
    return get(key, item, &functor);
  }

  int get(const KEY& key, ITEM*& item, GetFunctor* functor = NULL)
  {
    int ret = OB_SUCCESS;
    item = NULL;

    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "not inited", K(ret));
    } else {
      const uint64_t bucket_pos = NODE::hash(key) % buckets_count_;
      ObBucketRLockGuard bucket_guard(buckets_lock_, bucket_pos);
      NODE* cur = buckets_[bucket_pos];
      while (NULL != cur) {
        if (cur->equals(key)) {
          break;
        } else {
          cur = cur->next_;
        }
      }

      if (NULL == cur) {
        ret = OB_HASH_NOT_EXIST;
      } else if (NULL != functor && OB_FAIL((*functor)(cur->item_))) {
        COMMON_LOG(WARN, "failed to do get functor", K(ret), K(*cur));
      } else {
        item = &cur->item_;
      }
    }

    return ret;
  }

  int foreach (ForeachFunctor& functor)
  {
    int ret = OB_SUCCESS;
    bool is_full = false;

    if (!is_inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "not inited", K(ret));
    } else {
      for (uint64_t bucket_pos = 0; OB_SUCC(ret) && !is_full && bucket_pos < buckets_count_; ++bucket_pos) {
        ObBucketRLockGuard bucket_guard(buckets_lock_, bucket_pos);
        NODE* cur = buckets_[bucket_pos];
        while (OB_SUCC(ret) && NULL != cur && !is_full) {
          if (OB_FAIL(functor(cur->item_, is_full))) {
            COMMON_LOG(WARN, "failed to do foreach functor", K(ret));
          } else {
            cur = cur->next_;
          }
        }
      }
    }
    return ret;
  }

private:
  bool is_inited_;
  mutable common::ObBucketLock buckets_lock_;
  uint64_t count_;
  NODE** buckets_;
  uint64_t buckets_count_;
  SimpleAllocer<NODE> allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObPreAllocLinkHashMap);
};

}  // namespace hash
}  // namespace common
}  // namespace oceanbase
#endif /* SRC_LIBRARY_SRC_LIB_HASH_OB_MEMLESS_LINK_HASHMAP_H_ */
