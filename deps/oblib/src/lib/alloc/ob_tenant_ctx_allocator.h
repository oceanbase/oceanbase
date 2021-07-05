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

#ifndef _OB_TENANT_CTX_ALLOCATOR_H_
#define _OB_TENANT_CTX_ALLOCATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/queue/ob_link.h"
#include "lib/alloc/object_mgr.h"
#include "lib/alloc/alloc_failed_reason.h"
#include "lib/time/ob_time_utility.h"
#include "lib/resource/ob_resource_mgr.h"
#include "lib/allocator/ob_tc_malloc.h"
#include <signal.h>

namespace oceanbase {

namespace common {
class LabelItem;
}
namespace lib {
class MemoryCutter;
class ObTenantCtxAllocator : public common::ObIAllocator, private common::ObLink {
  friend class ObMemoryCutter;
  struct ModItemWrapper {
    ModItemWrapper()
    {}
    ModItemWrapper(common::ObModItem* item, int64_t mod_id) : item_(item), mod_id_(mod_id)
    {}
    bool operator<(const ModItemWrapper& other) const
    {
      return item_->hold_ > other.item_->hold_;
    }
    common::ObModItem* item_;
    int64_t mod_id_;
  };
  using InvokeFunc = std::function<int(const ObTenantMemoryMgr*)>;

public:
  explicit ObTenantCtxAllocator(uint64_t tenant_id, uint64_t ctx_id = 0)
      : resource_handle_(),
        tenant_id_(tenant_id),
        ctx_id_(ctx_id),
        has_deleted_(false),
        obj_mgr_(*this, tenant_id_, ctx_id_),
        idle_size_(0),
        head_chunk_(0),
        chunk_cnt_(0),
        using_list_head_(0),
        r_mod_set_(&mod_set_[0]),
        w_mod_set_(&mod_set_[1])
  {
    MEMSET(&head_chunk_, 0, sizeof(AChunk));
    using_list_head_.prev2_ = &using_list_head_;
    using_list_head_.next2_ = &using_list_head_;
    ObMemAttr attr;
    attr.tenant_id_ = tenant_id;
    attr.ctx_id_ = ctx_id;
  }
  int set_tenant_memory_mgr()
  {
    int ret = common::OB_SUCCESS;
    if (resource_handle_.is_valid()) {
      ret = common::OB_INIT_TWICE;
      LIB_LOG(WARN, "resource_handle is already valid", K(ret), K_(tenant_id), K_(ctx_id));
    } else if (OB_FAIL(ObResourceMgr::get_instance().get_tenant_resource_mgr(tenant_id_, resource_handle_))) {
      LIB_LOG(ERROR, "get_tenant_resource_mgr failed", K(ret), K_(tenant_id));
    }
    return ret;
  }
  uint64_t get_tenant_id()
  {
    return tenant_id_;
  }
  uint64_t get_ctx_id()
  {
    return ctx_id_;
  }
  void set_tenant_deleted()
  {
    has_deleted_ = true;
  }
  bool has_tenant_deleted()
  {
    return has_deleted_;
  }
  inline ObTenantCtxAllocator*& get_next()
  {
    return reinterpret_cast<ObTenantCtxAllocator*&>(next_);
  }

  // will delete it
  virtual void* alloc(const int64_t size)
  {
    return alloc(size, ObMemAttr());
  }

  virtual void* alloc(const int64_t size, const ObMemAttr& attr)
  {
    abort_unless(attr.tenant_id_ == tenant_id_);
    abort_unless(attr.ctx_id_ == ctx_id_);
    BACKTRACE(WARN, !attr.label_.is_valid(), "[OB_MOD_DO_NOT_USE_ME ALLOC]size:%ld", size);
    void* ptr = NULL;
    AObject* obj = obj_mgr_.alloc_object(size, attr);
    if (NULL != obj) {
      ptr = obj->data_;
    }
    if (NULL == ptr && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      _OB_LOG(WARN, "[OOPS] alloc failed reason: %s", alloc_failed_msg());
      _OB_LOG(WARN,
          "oops, alloc failed, tenant_id=%ld, ctx_id=%ld, ctx_name=%s, ctx_hold=%ld, "
          "ctx_limit=%ld, tenant_hold=%ld, tenant_limit=%ld",
          tenant_id_,
          ctx_id_,
          common::get_global_ctx_info().get_ctx_name(ctx_id_),
          get_hold(),
          get_limit(),
          get_tenant_hold(),
          get_tenant_limit());
      // 49 is the user defined signal to dump memory
      raise(49);
    }
    return ptr;
  }

  virtual void* realloc(const void* ptr, const int64_t size, const ObMemAttr& attr)
  {
    void* nptr = NULL;
    AObject* obj = NULL;
    BACKTRACE(WARN, !attr.label_.is_valid(), "[OB_MOD_DO_NOT_USE_ME REALLOC]size:%ld", size);
    if (NULL != ptr) {
      obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
      abort_unless(obj->is_valid());
      abort_unless(obj->in_use_);
      abort_unless(obj->block()->is_valid());
      abort_unless(obj->block()->in_use_);
    }
    obj = obj_mgr_.realloc_object(obj, size, attr);
    if (obj != NULL) {
      nptr = obj->data_;
    } else if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      _OB_LOG(WARN, "[OOPS] alloc failed reason: %s", alloc_failed_msg());
      _OB_LOG(WARN,
          "oops, alloc failed, tenant_id=%ld, ctx_id=%ld, ctx_name=%s, ctx_hold=%ld, "
          "ctx_limit=%ld, tenant_hold=%ld, tenant_limit=%ld",
          tenant_id_,
          ctx_id_,
          common::get_global_ctx_info().get_ctx_name(ctx_id_),
          get_hold(),
          get_limit(),
          get_tenant_hold(),
          get_tenant_limit());
      // 49 is the user defined signal to dump memory
      raise(49);
    }
    return nptr;
  }

  virtual void free(void* ptr)
  {
    if (NULL != ptr) {
      AObject* obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
      abort_unless(NULL != obj);
      abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
      abort_unless(obj->in_use_);

      ABlock* block = obj->block();
      abort_unless(block->check_magic_code());
      abort_unless(block->in_use_);
      abort_unless(block->obj_set_ != NULL);

      ObjectSet* set = block->obj_set_;
      set->free_object(obj);
    }
  }

  // statistic related
  int set_limit(int64_t bytes)
  {
    int ret = common::OB_SUCCESS;
    if (!resource_handle_.is_valid()) {
      ret = common::OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "resource_handle is invalid", K(ret), K_(tenant_id), K_(ctx_id));
    } else if (OB_FAIL(resource_handle_.get_memory_mgr()->set_ctx_limit(ctx_id_, bytes))) {
      LIB_LOG(WARN, "memory manager set_ctx_limit failed", K(ret), K(ctx_id_), K(bytes));
    }
    return ret;
  }

  int64_t get_limit() const
  {
    int64_t limit = 0;
    uint64_t ctx_id = ctx_id_;
    with_resource_handle_invoke([&ctx_id, &limit](const ObTenantMemoryMgr* mgr) {
      mgr->get_ctx_limit(ctx_id, limit);
      return common::OB_SUCCESS;
    });
    return limit;
  }

  int64_t get_hold() const
  {
    int64_t hold = 0;
    uint64_t ctx_id = ctx_id_;
    with_resource_handle_invoke([&ctx_id, &hold](const ObTenantMemoryMgr* mgr) {
      mgr->get_ctx_hold(ctx_id, hold);
      return common::OB_SUCCESS;
    });
    return hold;
  }

  int64_t get_tenant_limit() const
  {
    int64_t limit = 0;
    with_resource_handle_invoke([&limit](const ObTenantMemoryMgr* mgr) {
      limit = mgr->get_limit();
      return common::OB_SUCCESS;
    });
    return limit;
  }

  int64_t get_tenant_hold() const
  {
    int64_t hold = 0;
    with_resource_handle_invoke([&hold](const ObTenantMemoryMgr* mgr) {
      hold = mgr->get_sum_hold();
      return common::OB_SUCCESS;
    });
    return hold;
  }

  common::ObModItem get_mod_usage(int mod_id) const
  {
    common::ObModItem item = obj_mgr_.get_mod_usage(mod_id);
    return item;
  }

  void print_memory_usage() const
  {
    print_usage();
  }

  AChunk* alloc_chunk(const int64_t size, const ObMemAttr& attr);
  void free_chunk(AChunk* chunk, const ObMemAttr& attr);
  int set_idle(const int64_t size, const bool reserve = false);
  IBlockMgr& get_block_mgr()
  {
    return obj_mgr_;
  }
  void get_chunks(AChunk** chunks, int cap, int& cnt);
  using VisitFunc = std::function<int(ObLabel& label, common::LabelItem* l_item, common::ObModItem* m_item)>;
  int iter_label(VisitFunc func) const;
  common::ObLocalModSet** get_r_mod_set()
  {
    return &r_mod_set_;
  }
  common::ObLocalModSet** get_w_mod_set()
  {
    return &w_mod_set_;
  }

private:
  void print_usage() const;
  AChunk* pop_chunk();
  void push_chunk(AChunk* chunk);
  int with_resource_handle_invoke(InvokeFunc func) const
  {
    int ret = common::OB_SUCCESS;
    if (!resource_handle_.is_valid()) {
      ret = common::OB_ERR_UNEXPECTED;
      LIB_LOG(ERROR, "resource_handle is invalid", K_(tenant_id));
    } else {
      ret = func(resource_handle_.get_memory_mgr());
    }
    return ret;
  }

private:
  ObTenantResourceMgrHandle resource_handle_;
  uint64_t tenant_id_;
  uint64_t ctx_id_;
  bool has_deleted_;
  ObjectMgr<32> obj_mgr_;
  int64_t idle_size_;
  AChunk head_chunk_;
  // Temporarily useless, leave debug
  int64_t chunk_cnt_;
  ObMutex chunk_freelist_mutex_;
  ObMutex using_list_mutex_;
  AChunk using_list_head_;
  // Used to cache statistics of the memory_dump thread to avoid wasting space caused by pre-allocated tenants
  common::ObLocalModSet mod_set_[2];
  common::ObLocalModSet* r_mod_set_;
  common::ObLocalModSet* w_mod_set_;
};  // end of class ObTenantCtxAllocator

}  // end of namespace lib
}  // end of namespace oceanbase

#endif /* _OB_TENANT_CTX_ALLOCATOR_H_ */
