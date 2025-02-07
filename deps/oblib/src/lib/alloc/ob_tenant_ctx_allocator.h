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
#include "lib/alloc/memory_sanity.h"
#include "lib/alloc/ob_malloc_time_monitor.h"
#include "lib/alloc/alloc_func.h"
#include <signal.h>
namespace oceanbase
{

namespace common
{
struct LabelItem;
}
namespace lib
{
extern bool malloc_sample_allowed(const int64_t size, const ObMemAttr &attr);
class ObTenantCtxAllocator
    : public common::ObIAllocator,
      private common::ObLink
{
friend class ObTenantCtxAllocatorGuard;
friend class ObMallocAllocator;
using InvokeFunc = std::function<int (const ObTenantMemoryMgr*)>;

class ChunkMgr : public IChunkMgr
{
public:
  ChunkMgr(ObTenantCtxAllocator &ta) : ta_(ta) {}
  AChunk *alloc_chunk(const uint64_t size, const ObMemAttr &attr) override
  {
    AChunk *chunk = ta_.alloc_chunk(size, attr);
    if (OB_ISNULL(chunk)) {
      ta_.req_chunk_mgr_.reclaim_chunks();
      chunk = ta_.alloc_chunk(size, attr);
    }
    return chunk;
  }
  void free_chunk(AChunk *chunk, const ObMemAttr &attr) override
  {
    ta_.free_chunk(chunk, attr);
  }
private:
  ObTenantCtxAllocator &ta_;
};

class ReqChunkMgr : public IChunkMgr
{
public:
  static constexpr int32_t MAX_PARALLEL = 64;
  ReqChunkMgr(ObTenantCtxAllocator &ta)
    : ta_(ta), parallel_(CTX_ATTR(ta_.get_ctx_id()).parallel_)
  {
    abort_unless(parallel_ <= ARRAYSIZEOF(chunks_));
    MEMSET(chunks_, 0, sizeof(chunks_));
  }
  AChunk *alloc_chunk(const uint64_t size, const ObMemAttr &attr) override
  {
    AChunk *chunk = NULL;
    if (INTACT_ACHUNK_SIZE == AChunk::calc_hold(size)) {
      const uint64_t idx = common::get_itid() % parallel_;
      chunk = ATOMIC_TAS(&chunks_[idx], NULL);
    }
    if (OB_ISNULL(chunk)) {
      chunk = ta_.alloc_chunk(size, attr);
    }
    return chunk;
  }
  void free_chunk(AChunk *chunk, const ObMemAttr &attr) override
  {
    bool freed = false;
    if (INTACT_ACHUNK_SIZE == chunk->hold()) {
      const uint64_t idx = common::get_itid() % parallel_;
      freed = ATOMIC_BCAS(&chunks_[idx], NULL, chunk);
    }
    if (!freed) {
      ta_.free_chunk(chunk, attr);
    }
  }
  void reclaim_chunks()
  {
    for (int i = 0; i < MAX_PARALLEL; i++) {
      AChunk *chunk = ATOMIC_TAS(&chunks_[i], NULL);
      if (chunk != NULL) {
        ta_.free_chunk(chunk,
                       ObMemAttr(ta_.get_tenant_id(), "unused", ta_.get_ctx_id()));
      }
    }
  }
  int64_t n_chunks() const
  {
    int64_t n = 0;
    for (int i = 0; i < MAX_PARALLEL; i++) {
      AChunk *chunk = ATOMIC_LOAD(&chunks_[i]);
      if (chunk != NULL) {
        n++;
      }
    }
    return n;
  }
  void set_parallel(int32_t parallel)
  {
    int32_t min_parallel = CTX_ATTR(ta_.get_ctx_id()).parallel_;
    if (parallel < min_parallel) {
      parallel_ = min_parallel;
    } else if (parallel > MAX_PARALLEL) {
      parallel_ = MAX_PARALLEL;
    } else {
      parallel_ = parallel;
    }
  }
private:
  ObTenantCtxAllocator &ta_;
  int32_t parallel_;
  AChunk *chunks_[MAX_PARALLEL];
};

class AChunkUsingList
{
public:
  static const uint64_t NWAY = 64;
  uint64_t get_index(AChunk *chunk)
  {
    return (((uint64_t)chunk>>21) * 0xdeece66d + 0xb) % NWAY;
  }
  void insert(AChunk *chunk)
  {
    uint64_t index = get_index(chunk);
    lib::ObMutexGuard guard(slots_[index].mutex_);
    AChunk &head = slots_[index].head_;
    chunk->prev2_ = &head;
    chunk->next2_ = head.next2_;
    head.next2_->prev2_ = chunk;
    head.next2_ = chunk;
  }
  void remove(AChunk *chunk)
  {
    uint64_t index = get_index(chunk);
    lib::ObMutexGuard guard(slots_[index].mutex_);
    chunk->prev2_->next2_ = chunk->next2_;
    chunk->next2_->prev2_ = chunk->prev2_;
  }
  void get_chunks(AChunk **chunks, int cap, int &cnt)
  {
    for (int i = 0; i < NWAY; ++i) {
      lib::ObMutexGuard guard(slots_[i].mutex_);
      AChunk &head = slots_[i].head_;
      AChunk *cur = head.next2_;
      while (cur != &head && cnt < cap) {
        chunks[cnt++] = cur;
        cur = cur->next2_;
      }
    }
  }
private:
  struct Slot {
    Slot()
      : mutex_(common::ObLatchIds::CHUNK_USING_LIST_LOCK),
        head_()
    {
      mutex_.enable_record_stat(false);
      head_.prev2_ = &head_;
      head_.next2_ = &head_;
    }
    ObMutex mutex_;
    AChunk head_;
  } slots_[NWAY];
};

public:
  explicit ObTenantCtxAllocator(uint64_t tenant_id, uint64_t ctx_id = 0)
    : resource_handle_(), ref_cnt_(0), tenant_id_(tenant_id),
      ctx_id_(ctx_id), deleted_(false),
      obj_mgr_(*this,
               CTX_ATTR(ctx_id).enable_no_log_,
               INTACT_NORMAL_AOBJECT_SIZE,
               CTX_ATTR(ctx_id).parallel_,
               CTX_ATTR(ctx_id).enable_dirty_list_,
               NULL),
      idle_size_(0), head_chunk_(), chunk_cnt_(0),
      chunk_freelist_mutex_(common::ObLatchIds::CHUNK_FREE_LIST_LOCK),
      wash_related_chunks_(0), washed_blocks_(0), washed_size_(0),
      chunk_mgr_(*this), req_chunk_mgr_(*this)
  {
    MEMSET(&head_chunk_, 0, sizeof(AChunk));
    ObMemAttr attr;
    attr.tenant_id_  = tenant_id;
    attr.ctx_id_ = ctx_id;
    chunk_freelist_mutex_.enable_record_stat(false);
    for (int i = 0; i < ObSubCtxIds::MAX_SUB_CTX_ID; ++i) {
      new (obj_mgrs_ + i) ObjectMgr(*this,
                                    CTX_ATTR(ctx_id).enable_no_log_,
                                    INTACT_MIDDLE_AOBJECT_SIZE,
                                    4/*parallel*/,
                                    false/*enable_dirty_list*/,
                                    &obj_mgr_);
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
  IChunkMgr &get_chunk_mgr() { return chunk_mgr_; }
  IChunkMgr &get_req_chunk_mgr() { return req_chunk_mgr_; }
  void get_chunks(AChunk **chunks, int cap, int &cnt);
  using VisitFunc = std::function<int(ObLabel &label,
                                      common::LabelItem *l_item)>;
  int iter_label(VisitFunc func) const;
  int64_t sync_wash(int64_t wash_size);
  int64_t sync_wash();
  bool check_has_unfree(char *first_label, char *first_bt)
  {
    bool has_unfree = obj_mgr_.check_has_unfree();
    if (has_unfree) {
      bool tmp_has_unfree = obj_mgr_.check_has_unfree(first_label, first_bt);
      for (int i = 0; i < ObSubCtxIds::MAX_SUB_CTX_ID && !tmp_has_unfree; ++i) {
        tmp_has_unfree = obj_mgrs_[i].check_has_unfree(first_label, first_bt);
      }
    }
    return has_unfree;
  }
  void update_wash_stat(int64_t related_chunks, int64_t blocks, int64_t size);
  void reset_req_chunk_mgr() { req_chunk_mgr_.reclaim_chunks(); }
  void set_req_chunkmgr_parallel(int32_t parallel) { req_chunk_mgr_.set_parallel(parallel); }
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
  static void* common_realloc(const void *ptr, const int64_t size, const ObMemAttr &attr,
      ObTenantCtxAllocator& ta, T &allocator)
  {
    ObDisableDiagnoseGuard disable_diagnose_guard;
    SANITY_DISABLE_CHECK_RANGE();
    if (!attr.label_.is_valid()) {
      LIB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "OB_MOD_DO_NOT_USE_ME REALLOC", K(size));
    }
    void *nptr = NULL;
    if (errsim_alloc(attr)) {
      // do-nothing
    } else {
      AObject *obj = NULL; // original object
      AObject *nobj = NULL; // newly allocated object
      ObMemAttr inner_attr = attr;
      if (NULL != ptr) {
        obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
        on_free(*obj);
      }
      ObLightBacktraceGuard light_backtrace_guard(is_memleak_light_backtrace_enabled()
          && ObCtxIds::GLIBC != attr.ctx_id_);
      BASIC_TIME_GUARD(time_guard, "ObMalloc");
      DEFER(ObMallocTimeMonitor::get_instance().record_malloc_time(time_guard, size, inner_attr));
      bool sample_allowed = malloc_sample_allowed(size, inner_attr);
      inner_attr.alloc_extra_info_ = sample_allowed;
      nobj = allocator.realloc_object(obj, size, inner_attr);
      if (OB_ISNULL(nobj)) {
        int64_t total_size = 0;
        if (g_alloc_failed_ctx().need_wash_block()) {
          total_size += ta.sync_wash();
          BASIC_TIME_GUARD_CLICK("WASH_BLOCK_END");
        } else if (g_alloc_failed_ctx().need_wash_chunk()) {
          total_size += CHUNK_MGR.sync_wash();
          BASIC_TIME_GUARD_CLICK("WASH_CHUNK_END");
        }
        if (total_size > 0) {
          nobj = allocator.realloc_object(obj, size, inner_attr);
        }
      }
      if (OB_UNLIKELY(NULL == nobj && NULL != obj)) {
        SANITY_UNPOISON(obj->data_, obj->alloc_bytes_);
      }
      if (OB_NOT_NULL(nobj)) {
        on_alloc(*nobj, inner_attr);
        nptr = nobj->data_;
      }
    }
    if (NULL == nptr) {
      print_alloc_failed_msg();
    }
    return nptr;
  }
  static void common_free(void *ptr);
private:
  static void on_alloc(AObject& obj, const ObMemAttr& attr);
  static void on_free(AObject& obj);

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
  AChunkUsingList using_list_;
  int64_t wash_related_chunks_;
  int64_t washed_blocks_;
  int64_t washed_size_;
  union {
    ObjectMgr obj_mgrs_[ObSubCtxIds::MAX_SUB_CTX_ID];
  };
  ChunkMgr chunk_mgr_;
  ReqChunkMgr req_chunk_mgr_;
}; // end of class ObTenantCtxAllocator

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OB_TENANT_CTX_ALLOCATOR_H_ */
