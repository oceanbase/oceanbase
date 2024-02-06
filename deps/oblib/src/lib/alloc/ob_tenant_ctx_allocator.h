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

namespace oceanbase
{

namespace common
{
struct LabelItem;
}
namespace lib
{
class ObTenantCtxAllocator
    : public common::ObIAllocator,
      private common::ObLink
{
friend class ObTenantCtxAllocatorGuard;
friend class ObMallocAllocator;
using InvokeFunc = std::function<int (const ObTenantMemoryMgr*)>;
public:
  explicit ObTenantCtxAllocator(uint64_t tenant_id, uint64_t ctx_id = 0)
    : resource_handle_(), ref_cnt_(0), tenant_id_(tenant_id),
      ctx_id_(ctx_id), deleted_(false),
      obj_mgr_(*this, tenant_id_, ctx_id_, INTACT_NORMAL_AOBJECT_SIZE,
               CTX_ATTR(ctx_id).parallel_,
               CTX_ATTR(ctx_id).enable_dirty_list_,
               NULL),
      idle_size_(0), head_chunk_(), chunk_cnt_(0),
      chunk_freelist_mutex_(common::ObLatchIds::CHUNK_FREE_LIST_LOCK),
      using_list_mutex_(common::ObLatchIds::CHUNK_USING_LIST_LOCK),
      using_list_head_(), wash_related_chunks_(0), washed_blocks_(0), washed_size_(0)
  {
    MEMSET(&head_chunk_, 0, sizeof(AChunk));
    using_list_head_.prev2_ = &using_list_head_;
    using_list_head_.next2_ = &using_list_head_;
    ObMemAttr attr;
    attr.tenant_id_  = tenant_id;
    attr.ctx_id_ = ctx_id;
    chunk_freelist_mutex_.enable_record_stat(false);
    using_list_mutex_.enable_record_stat(false);
    for (int i = 0; i < ObSubCtxIds::MAX_SUB_CTX_ID; ++i) {
      new (obj_mgrs_ + i) ObjectMgr(*this, tenant_id_, ctx_id_, INTACT_MIDDLE_AOBJECT_SIZE,
                                    4/*parallel*/, false/*enable_dirty_list*/, &obj_mgr_);
    }
  }
  virtual ~ObTenantCtxAllocator()
  {
    for (int i = 0; i < ObSubCtxIds::MAX_SUB_CTX_ID; ++i) {
      obj_mgrs_[i].~ObjectMgr();
    }
  }
  int set_tenant_memory_mgr()
  {
    int ret = common::OB_SUCCESS;
    if (resource_handle_.is_valid()) {
      ret = common::OB_INIT_TWICE;
      LIB_LOG(WARN, "resource_handle is already valid", K(ret), K_(tenant_id), K_(ctx_id));
    } else if (OB_FAIL(ObResourceMgr::get_instance().get_tenant_resource_mgr(
        tenant_id_, resource_handle_))) {
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
  inline ObTenantCtxAllocator *&get_next()
  {
    return reinterpret_cast<ObTenantCtxAllocator*&>(next_);
  }

  // will delete it
  virtual void *alloc(const int64_t size)
  {
    return alloc(size, ObMemAttr());
  }

  virtual void *alloc(const int64_t size, const ObMemAttr &attr);
  virtual void* realloc(const void *ptr, const int64_t size, const ObMemAttr &attr);
  virtual void free(void *ptr);
  static int64_t get_obj_hold(void *ptr);

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
    with_resource_handle_invoke([&ctx_id, &limit](const ObTenantMemoryMgr *mgr) {
        mgr->get_ctx_limit(ctx_id, limit);
        return common::OB_SUCCESS;
      });
    return limit;
  }

  int64_t get_hold() const
  {
    int64_t hold = 0;
    uint64_t ctx_id = ctx_id_;
    with_resource_handle_invoke([&ctx_id, &hold](const ObTenantMemoryMgr *mgr) {
      mgr->get_ctx_hold(ctx_id, hold);
      return common::OB_SUCCESS;
    });
    return hold;
  }

  int64_t get_used() const;

  int64_t get_tenant_limit() const
  {
    int64_t limit = 0;
    with_resource_handle_invoke([&limit](const ObTenantMemoryMgr *mgr) {
        limit = mgr->get_limit();
        return common::OB_SUCCESS;
      });
    return limit;
  }

  int64_t get_tenant_hold() const
  {
    int64_t hold = 0;
    with_resource_handle_invoke([&hold](const ObTenantMemoryMgr *mgr) {
        hold = mgr->get_sum_hold();
        return common::OB_SUCCESS;
      });
    return hold;
  }

  common::ObLabelItem get_label_usage(ObLabel &label) const;

  void print_memory_usage() const { print_usage(); }

  AChunk *alloc_chunk(const int64_t size, const ObMemAttr &attr);
  void free_chunk(AChunk *chunk, const ObMemAttr &attr);
  bool update_hold(const int64_t size);
  int set_idle(const int64_t size, const bool reserve = false);
  IBlockMgr &get_block_mgr() { return obj_mgr_; }
  void get_chunks(AChunk **chunks, int cap, int &cnt);
  using VisitFunc = std::function<int(ObLabel &label,
                                      common::LabelItem *l_item)>;
  int iter_label(VisitFunc func) const;
  int64_t sync_wash(int64_t wash_size);
  int64_t sync_wash();
  bool check_has_unfree(char *first_label)
  {
    bool has_unfree = obj_mgr_.check_has_unfree();
    if (has_unfree) {
      bool tmp_has_unfree = obj_mgr_.check_has_unfree(first_label);
      for (int i = 0; i < ObSubCtxIds::MAX_SUB_CTX_ID && !tmp_has_unfree; ++i) {
        tmp_has_unfree = obj_mgrs_[i].check_has_unfree(first_label);
      }
    }
    return has_unfree;
  }
  void update_wash_stat(int64_t related_chunks, int64_t blocks, int64_t size);
private:
  int64_t inc_ref_cnt(int64_t cnt) { return ATOMIC_FAA(&ref_cnt_, cnt); }
  int64_t get_ref_cnt() const { return ATOMIC_LOAD(&ref_cnt_); }
  void print_usage() const;
  AChunk *pop_chunk();
  void push_chunk(AChunk *chunk);
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

public:
  template <typename T>
  static void* common_alloc(const int64_t size, const ObMemAttr &attr,
                            ObTenantCtxAllocator& ta, T &allocator);

  template <typename T>
  static void* common_realloc(const void *ptr, const int64_t size,
                              const ObMemAttr &attr, ObTenantCtxAllocator& ta,
                              T &allocator);

  static void common_free(void *ptr);

private:
  ObTenantResourceMgrHandle resource_handle_;
  int64_t ref_cnt_;
  uint64_t tenant_id_;
  uint64_t ctx_id_;
  bool deleted_;
  ObjectMgr obj_mgr_;
  int64_t idle_size_;
  AChunk head_chunk_;
  // Temporarily useless, leave debug
  int64_t chunk_cnt_;
  ObMutex chunk_freelist_mutex_;
  ObMutex using_list_mutex_;
  AChunk using_list_head_;
  int64_t wash_related_chunks_;
  int64_t washed_blocks_;
  int64_t washed_size_;
  union {
    ObjectMgr obj_mgrs_[ObSubCtxIds::MAX_SUB_CTX_ID];
  };
}; // end of class ObTenantCtxAllocator

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OB_TENANT_CTX_ALLOCATOR_H_ */
