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

#ifndef OCEANBASE_UPDATESERVER_RESOURCE_POOL_H_
#define OCEANBASE_UPDATESERVER_RESOURCE_POOL_H_
#include <typeinfo>
#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_id_map.h"
#include "lib/utility/utility.h"
#include "lib/allocator/ob_fifo_allocator.h"

DEFINE_HAS_MEMBER(RP_MAX_FREE_LIST_NUM);

namespace oceanbase {
namespace common {

template <class T>
struct RP {
  template <class Type, bool Cond = true>
  struct GetMaxFreelistNum {
    static const int64_t v = Type::RP_MAX_FREE_LIST_NUM;
  };

  template <class Type>
  struct GetMaxFreelistNum<Type, false> {
    static const int64_t v = 64;
  };
};

template <const char* str = common::ObModIds::OB_UPS_RESOURCE_POOL_NODE>
struct RPStrLabel {
  static constexpr const char* LABEL = str;
};
template <const char* str>
constexpr const char* RPStrLabel<str>::LABEL;

template <const int mod_id>
struct RPModIdLabel {
  static constexpr const int LABEL = mod_id;
};

template <class T, class RPLabel>
class ObResourcePool {
  static const int64_t ALLOCATOR_PAGE_SIZE = OB_MALLOC_BIG_BLOCK_SIZE - 1024;
  static const int64_t WARN_INTERVAL = 60000000L;              // 60s
  static const uint64_t ALLOC_MAGIC_NUM = 0x72737263706f6f6c;  // rsrcpool
  static const uint64_t ALLOC_BY_INNER_ALLOCATOR = 0x0;
  static const uint64_t ALLOC_BY_OBMALLOC = 0x1;
  static const int64_t CHECK_INTERVAL = 100000;
  static const int64_t UPDATE_INTERVAL = 5000000;
  static const int64_t MAX_FREE_LIST_NUM = RP<T>::template GetMaxFreelistNum<T, HAS_MEMBER(T, RP_MAX_FREE_LIST_NUM)>::v;
  struct Node {
    T data;
    const lib::ObLabel label;
    union {
      Node* next;
      uint64_t magic;
    };
    uint64_t flag;
    Node() : data(), label(RPLabel::LABEL), next(NULL), flag(ALLOC_BY_INNER_ALLOCATOR){};
  };
  typedef common::ObFixedQueue<Node> NodeQueue;

public:
  ObResourcePool()
      : inner_allocated_num_(0),
        inner_used_num_(0),
        slice_max_used_num_(0),
        max_idle_num_(0),
        last_check_ts_(0),
        last_update_ts_(0),
        updating_(0)
  {
    int ret = OB_SUCCESS;
    free_list_.init(MAX_FREE_LIST_NUM);
    const int64_t page_size = MIN(OB_MALLOC_BIG_BLOCK_SIZE, MAX(8 * sizeof(T), OB_MALLOC_MIDDLE_BLOCK_SIZE));
    if (OB_FAIL(allocator_.init(
            lib::ObMallocAllocator::get_instance(), page_size, lib::ObMemAttr(OB_SERVER_TENANT_ID, RPLabel::LABEL)))) {
      _COMMON_LOG(INFO, "init fifo failed, ret=%d", ret);
    }
    abort_unless(OB_SUCCESS == ret);
    _COMMON_LOG(INFO,
        "Construction ObResourcePool this=%p type=%s allocator=%p free_list=%p bt=%s",
        this,
        typeid(T).name(),
        &allocator_,
        &free_list_,
        lbt());
  };
  ~ObResourcePool()
  {
    _COMMON_LOG(INFO,
        "Destruction ObResourcePool this=%p type=%s allocator=%p free_list=%p",
        this,
        typeid(T).name(),
        &allocator_,
        &free_list_);
    Node* node = NULL;
    while (common::OB_SUCCESS == free_list_.pop(node)) {
      node->~Node();
      allocator_.free((char*)node);
    }
  };

public:
  T* alloc()
  {
    T* ret = NULL;
    Node* node = alloc_node_();
    if (NULL != node) {
      ret = &(node->data);
      node->magic = ALLOC_MAGIC_NUM;
    }
    const int64_t cur_ts = ObTimeUtility::fast_current_time();
    if (cur_ts - last_check_ts_ > CHECK_INTERVAL && ATOMIC_BCAS(&updating_, 0, 1)) {
      int64_t n = 0;
      if (slice_max_used_num_ > max_idle_num_) {
        n = slice_max_used_num_ * 1.5;
        if (n > MAX_FREE_LIST_NUM) {
          n = MAX_FREE_LIST_NUM;
        }
      } else {
        // Smooth down
        n = max_idle_num_ - 1;
        if (n < 0) {
          n = 0;
        }
      }
      max_idle_num_ = n;
      if (cur_ts - last_update_ts_ > UPDATE_INTERVAL) {
        slice_max_used_num_ = 0;
        last_update_ts_ = cur_ts;
      }
      last_check_ts_ = cur_ts;
      (void)ATOMIC_STORE(&updating_, 0);
    }
    return ret;
  };
  void free(T* ptr)
  {
    if (NULL != ptr) {
      Node* node = (Node*)ptr;
      if (ALLOC_MAGIC_NUM != node->magic) {
        _COMMON_LOG(ERROR, "node=%p magic=%lx not match %lx", node, node->magic, ALLOC_MAGIC_NUM);
      } else {
        free_node_(node);
      }
    }
  };
  int64_t get_free_num()
  {
    return free_list_.get_total();
  }

private:
  Node* alloc_node_()
  {
    Node* ret = NULL;
    if (OB_SUCCESS != free_list_.pop(ret) || NULL == ret) {
      void* buffer = NULL;
      uint64_t flag = ALLOC_BY_INNER_ALLOCATOR;
      int64_t allocated_num = ATOMIC_AAF(&inner_allocated_num_, 1);
      if (MAX_FREE_LIST_NUM >= allocated_num) {
        flag = ALLOC_BY_INNER_ALLOCATOR;
        buffer = allocator_.alloc(sizeof(Node));
        if (OB_ISNULL(buffer)) {
          (void)ATOMIC_AAF(&inner_allocated_num_, -1);
        } else {
          (void)ATOMIC_AAF(&inner_used_num_, 1);
        }
      } else {
        flag = ALLOC_BY_OBMALLOC;
        ObMemAttr memattr(OB_SERVER_TENANT_ID, RPLabel::LABEL);
        buffer = common::ob_malloc(sizeof(Node), memattr);
        (void)ATOMIC_AAF(&inner_allocated_num_, -1);
      }
      if (NULL != buffer) {
        ret = new (buffer) Node();
        ret->flag = flag;
      }
    } else {
      (void)ATOMIC_AAF(&inner_used_num_, 1);
    }
    do {
      int64_t num = ATOMIC_LOAD(&slice_max_used_num_);
      int64_t used_num = ATOMIC_LOAD(&inner_used_num_);
      if (used_num <= num || ATOMIC_BCAS(&slice_max_used_num_, num, used_num)) {
        break;
      }
    } while (true);

    return ret;
  }

  void free_node_(Node* ptr)
  {
    if (NULL != ptr) {
      ptr->data.reset();
      ptr->next = NULL;
      if (ALLOC_BY_INNER_ALLOCATOR == ptr->flag) {
        bool need_free = true;
        if (ATOMIC_LOAD(&inner_allocated_num_) < max_idle_num_) {
          if (common::OB_SUCCESS != free_list_.push(ptr)) {
            _COMMON_LOG(ERROR, "free node to list fail, size=%ld ptr=%p", free_list_.get_total(), ptr);
          } else {
            need_free = false;
          }
        }
        if (need_free) {
          (void)ATOMIC_AAF(&inner_allocated_num_, -1);
          allocator_.free(ptr);
        }
        (void)ATOMIC_AAF(&inner_used_num_, -1);
      } else if (ALLOC_BY_OBMALLOC == ptr->flag) {
        ptr->~Node();
        common::ob_free(ptr);
      } else {
        _COMMON_LOG(ERROR, "invalid flag=%lu", ptr->flag);
      }
    }
  }

private:
  common::ObFIFOAllocator allocator_;
  volatile int64_t inner_allocated_num_;
  volatile int64_t inner_used_num_;
  NodeQueue free_list_;
  volatile int64_t slice_max_used_num_;
  int64_t max_idle_num_;
  int64_t last_check_ts_;
  int64_t last_update_ts_;
  int32_t updating_;
};

template <class T, const char* LABEL>
ObResourcePool<T, RPStrLabel<LABEL>>& get_resource_pool()
{
  static ObResourcePool<T, RPStrLabel<LABEL>> resource_pool;
  static bool once = false;
  if (!once) {
    _COMMON_LOG(INFO, "get_resource_pool ptr=%p name=%s label=%s", &resource_pool, typeid(T).name(), LABEL);
    once = true;
  }
  return resource_pool;
}

template <class T, const int MOD_ID>
ObResourcePool<T, RPModIdLabel<MOD_ID>>& get_resource_pool()
{
  static ObResourcePool<T, RPModIdLabel<MOD_ID>> resource_pool;
  static bool once = false;
  if (!once) {
    _COMMON_LOG(INFO, "get_resource_pool ptr=%p name=%s label=%d", &resource_pool, typeid(T).name(), MOD_ID);
    once = true;
  }
  return resource_pool;
}

#define rp_alloc(type, label) common::get_resource_pool<type, label>().alloc()
#define rp_free(ptr, label) common::get_resource_pool<__typeof__(*ptr), label>().free(ptr)
}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_UPDATESERVER_RESOURCE_POOL_H_
