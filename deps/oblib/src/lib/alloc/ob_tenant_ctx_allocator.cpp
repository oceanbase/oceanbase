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

#define USING_LOG_PREFIX LIB

#include "ob_tenant_ctx_allocator.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/utility/ob_sort.h"
#include "lib/alloc/memory_dump.h"
#include "lib/alloc/memory_sanity.h"
#include "lib/alloc/ob_malloc_callback.h"
#include "common/ob_smart_var.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
void *ObTenantCtxAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  abort_unless(attr.tenant_id_ == tenant_id_);
  abort_unless(attr.ctx_id_ == ctx_id_);
  void *ptr = NULL;
  if (OB_LIKELY(ObSubCtxIds::MAX_SUB_CTX_ID == attr.sub_ctx_id_)) {
    ptr = common_realloc(NULL, size, attr, *this, obj_mgr_);
  } else if (OB_UNLIKELY(attr.sub_ctx_id_ < ObSubCtxIds::MAX_SUB_CTX_ID)) {
    ptr = common_realloc(NULL, size, attr, *this, obj_mgrs_[attr.sub_ctx_id_]);
  } else {
    LIB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "allocate memory with unexpected sub_ctx_id");
  }
  return ptr;
}

int64_t ObTenantCtxAllocator::get_obj_hold(void *ptr)
{
  AObject *obj = reinterpret_cast<AObject*>((char*)(ptr) - AOBJECT_HEADER_SIZE);
  abort_unless(NULL != obj);
  return obj->hold(AllocHelper::cells_per_block(obj->block()->ablock_size_));
}

void* ObTenantCtxAllocator::realloc(const void *ptr, const int64_t size, const ObMemAttr &attr)
{
  void *nptr = common_realloc(ptr, size, attr, *this, obj_mgr_);
  return nptr;
}

void ObTenantCtxAllocator::free(void *ptr)
{
  common_free(ptr);
}
int ObTenantCtxAllocator::iter_label(VisitFunc func) const
{
  int ret = OB_SUCCESS;
  struct ItemWrapper
  {
    ObLabel label_;
    LabelItem *item_;
  };
  auto &mem_dump = ObMemoryDump().get_instance();
  if (OB_UNLIKELY(!mem_dump.is_inited())) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "mem dump not inited", K(ret));
  } else {
    ObLatchRGuard guard(mem_dump.iter_lock_, common::ObLatchIds::MEM_DUMP_ITER_LOCK);
    const int item_cap = 1024;
    SMART_VAR(ItemWrapper[item_cap], items) {
      LabelItem mb_item;
      int64_t item_cnt = 0;
      auto *up_litems = mem_dump.r_stat_->up2date_items_;
      auto &tcrs = mem_dump.r_stat_->tcrs_;
      int tcr_cnt = mem_dump.r_stat_->tcr_cnt_;
      auto it = std::lower_bound(tcrs, tcrs + tcr_cnt, std::make_pair(tenant_id_, ctx_id_),
                                  &ObMemoryDump::TenantCtxRange::compare);
      if (ObCtxIds::KVSTORE_CACHE_ID == ctx_id_) {
        items[item_cnt].label_ = ObNewModIds::OB_KVSTORE_CACHE_MB;
        int len = strlen(ObNewModIds::OB_KVSTORE_CACHE_MB);
        MEMCPY(mb_item.str_, ObNewModIds::OB_KVSTORE_CACHE_MB, len);
        mb_item.str_[len] = '\0';
        mb_item.str_len_ = len;

        IGNORE_RETURN with_resource_handle_invoke([&](const ObTenantMemoryMgr *mgr) {
          mb_item.hold_ += mgr->get_cache_hold();
          mb_item.count_ += mgr->get_cache_item_count();
          return OB_SUCCESS;
        });
        items[item_cnt++].item_ = &mb_item;
      }
      if (it != tcrs + tcr_cnt &&
          it->tenant_id_ == tenant_id_ &&
          it->ctx_id_ == ctx_id_) {
        auto &tcr = *it;
        for (int64_t j = tcr.start_;
              OB_SUCC(ret) && j < tcr.end_ && item_cnt < item_cap;
              ++j) {
          items[item_cnt].label_ = up_litems[j].str_;
          items[item_cnt].item_ = &up_litems[j];
          item_cnt++;
        }
      }
      if (OB_SUCC(ret)) {
        lib::ob_sort(items, items + item_cnt,
            [](ItemWrapper &l, ItemWrapper &r)
            {
              return (l.item_->hold_  > r.item_->hold_);
            });
        for (int64_t i = 0; OB_SUCC(ret) && i < item_cnt; ++i) {
          ret = func(items[i].label_, items[i].item_);
        }
      }
    }
  }
  return ret;
}

void ObTenantCtxAllocator::print_usage() const
{
  int ret = OB_SUCCESS;
  static const int64_t BUFLEN = 1 << 16;
  SMART_VAR(char[BUFLEN], buf) {
    int64_t pos = 0;
    int64_t ctx_hold_bytes = 0;
    LabelItem sum_item;
    ret = iter_label([&](ObLabel &label, LabelItem *l_item)
    {
      int ret = OB_SUCCESS;
      if (l_item->count_ != 0) {
        ret = databuff_printf(
            buf, BUFLEN, pos,
            "[MEMORY] hold=% '15ld used=% '15ld count=% '8d avg_used=% '15ld block_cnt=% '8d chunk_cnt=% '8d mod=%s\n",
            l_item->hold_, l_item->used_, l_item->count_, l_item->used_ / l_item->count_, l_item->block_cnt_, l_item->chunk_cnt_,
            label.str_);
      }
      sum_item += *l_item;
      return ret;
    });
    if (OB_SUCC(ret) && sum_item.count_ > 0) {
      ret = databuff_printf(
          buf, BUFLEN, pos,
          "[MEMORY] hold=% '15ld used=% '15ld count=% '8d avg_used=% '15ld mod=%s\n",
          sum_item.hold_, sum_item.used_, sum_item.count_,
          sum_item.used_ / sum_item.count_,
          "SUMMARY");
    }
    if (OB_SUCC(ret)) {
      ret = with_resource_handle_invoke([&](const ObTenantMemoryMgr *mgr) {
        return mgr->get_ctx_hold(ctx_id_, ctx_hold_bytes);
      });
    }

    if (ctx_hold_bytes > 0 || sum_item.used_ > 0) {
      allow_next_syslog();
      _LOG_INFO("\n[MEMORY] tenant_id=%5ld ctx_id=%25s hold=% '15ld used=% '15ld limit=% '15ld"
                "\n[MEMORY] idle_size=% '10ld free_size=% '10ld"
                "\n[MEMORY] wash_related_chunks=% '10ld washed_blocks=% '10ld washed_size=% '10ld"
                "\n[MEMORY] request_cached_chunk_cnt=% '5ld\n%s",
          tenant_id_,
          get_global_ctx_info().get_ctx_name(ctx_id_),
          ctx_hold_bytes,
          sum_item.hold_,
          get_limit(),
          idle_size_,
          chunk_cnt_ * INTACT_ACHUNK_SIZE,
          ATOMIC_LOAD(&wash_related_chunks_),
          ATOMIC_LOAD(&washed_blocks_),
          ATOMIC_LOAD(&washed_size_),
          req_chunk_mgr_.n_chunks(),
          buf);
    }
  }
}

AChunk *ObTenantCtxAllocator::pop_chunk()
{
  ObDisableDiagnoseGuard disable_diagnose_guard;
  lib::ObMutexGuard guard(chunk_freelist_mutex_);
  AChunk *chunk = head_chunk_.next_;
  AChunk *next_chunk = nullptr == chunk ? nullptr : chunk->next_;
  head_chunk_.next_ = next_chunk;
  if (chunk != nullptr) {
    --chunk_cnt_;
  }
  return chunk;
}

void ObTenantCtxAllocator::push_chunk(AChunk *chunk)
{
  ObDisableDiagnoseGuard disable_diagnose_guard;
  lib::ObMutexGuard guard(chunk_freelist_mutex_);
  chunk->next_ = head_chunk_.next_;
  head_chunk_.next_ = chunk;
  ++chunk_cnt_;
}

AChunk *ObTenantCtxAllocator::alloc_chunk(const int64_t size, const ObMemAttr &attr)
{
  int ret = OB_SUCCESS;
  AChunk *chunk = nullptr;

  if (INTACT_ACHUNK_SIZE != AChunkMgr::hold(size)) {
    //if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
    //  LIB_LOG(WARN, "unusual chunk allocated", K(size), K(attr), K(lbt()));
    //}
  } else if (0 != idle_size_) {
    chunk = pop_chunk();
  }
  if (nullptr == chunk) {
    if (!resource_handle_.is_valid()) {
      LIB_LOG(ERROR, "resource_handle is invalid", K_(tenant_id), K_(ctx_id));
    } else {
      chunk = resource_handle_.get_memory_mgr()->alloc_chunk(size, attr);
    }
  }

  if (OB_NOT_NULL(chunk)) {
    ObDisableDiagnoseGuard disable_diagnose_guard;
    using_list_.insert(chunk);
  }

  return chunk;
}

void ObTenantCtxAllocator::free_chunk(AChunk *chunk, const ObMemAttr &attr)
{
  if (chunk != nullptr) {
    ObDisableDiagnoseGuard disable_diagnose_guard;
    using_list_.remove(chunk);
  }
  if (INTACT_ACHUNK_SIZE == chunk->hold() &&
      get_hold() - INTACT_ACHUNK_SIZE < idle_size_) {
    push_chunk(chunk);
  } else {
    if (!resource_handle_.is_valid()) {
      LIB_LOG_RET(ERROR, OB_INVALID_ERROR, "resource_handle is invalid", K_(tenant_id), K_(ctx_id));
    } else {
      resource_handle_.get_memory_mgr()->free_chunk(chunk, attr);
    }
  }
}

bool ObTenantCtxAllocator::update_hold(const int64_t size)
{
  bool update = false;
  if (!resource_handle_.is_valid()) {
    LIB_LOG_RET(ERROR, OB_INVALID_ARGUMENT, "resource_handle is invalid", K_(tenant_id), K_(ctx_id));
  } else {
    bool reach_ctx_limit = false;
    if (size <=0) {
      resource_handle_.get_memory_mgr()->update_hold(size, ctx_id_, ObLabel(), reach_ctx_limit);
      AChunkMgr::instance().update_hold(size, false);
      update = true;
    } else {
      if (!resource_handle_.get_memory_mgr()->update_hold(size, ctx_id_, ObLabel(), reach_ctx_limit)) {
        // do-nothing
      } else if (!AChunkMgr::instance().update_hold(size, false)) {
	resource_handle_.get_memory_mgr()->update_hold(-size, ctx_id_, ObLabel(), reach_ctx_limit);
      } else {
	update = true;
      }
    }
  }
  return update;
}

int ObTenantCtxAllocator::set_idle(const int64_t set_size, const bool reserve/*=false*/)
{
  int ret = OB_SUCCESS;
  const int64_t limit = get_limit();
  const int64_t size = lower_align(set_size, INTACT_ACHUNK_SIZE);
  if (size > limit || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid argument", K_(tenant_id), K_(ctx_id),
            K(size), K(limit));
  } else if (!resource_handle_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "resource_handle is invalid", K(ret), K_(tenant_id), K_(ctx_id));
  } else {
    ObMemAttr default_attr;
    default_attr.tenant_id_ = tenant_id_;
    default_attr.ctx_id_ = ctx_id_;
    const int64_t hold = get_hold();
    if (hold == size) {
      // do-nothing
    } else if (hold > size) {
      AChunk *chunk = nullptr;
      while (get_hold() - INTACT_ACHUNK_SIZE >= size && (chunk = pop_chunk()) != nullptr) {
        resource_handle_.get_memory_mgr()->free_chunk(chunk, default_attr);
      }
    } else {
      if (reserve) {
        const int64_t ori_chunk_cnt = chunk_cnt_;
        while (OB_SUCC(ret) && get_hold() < size) {
          AChunk *chunk = resource_handle_.get_memory_mgr()->alloc_chunk(ACHUNK_SIZE,
              default_attr);
          if (OB_ISNULL(chunk)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LIB_LOG(ERROR, "alloc chunk failed", K(ret), K_(tenant_id), K_(ctx_id));
          } else {
            push_chunk(chunk);
          }
        }
        // cleanup
        if (OB_FAIL(ret)) {
          AChunk *chunk = nullptr;
          int64_t to_free_chunk_cnt = chunk_cnt_ - ori_chunk_cnt;
          while ((to_free_chunk_cnt--) > 0 && (chunk = pop_chunk()) != nullptr) {
            resource_handle_.get_memory_mgr()->free_chunk(chunk, default_attr);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      idle_size_ = size;
    }
    LIB_LOG(INFO, "set idle finish", K(ret), K_(tenant_id), K_(ctx_id), K_(idle_size),
            K_(chunk_cnt));
  }
  return ret;
}

void ObTenantCtxAllocator::get_chunks(AChunk **chunks, int cap, int &cnt)
{
  ObDisableDiagnoseGuard disable_diagnose_guard;
  using_list_.get_chunks(chunks, cap, cnt);
}

int64_t ObTenantCtxAllocator::get_used() const
{
  int64_t used = 0;
  IGNORE_RETURN iter_label([&](ObLabel &label_, LabelItem *l_item)
  {
    used += l_item->hold_;
    return OB_SUCCESS;
  });
  return used;
}

ObLabelItem ObTenantCtxAllocator::get_label_usage(ObLabel &label) const
{
  ObLabelItem item;
  item.reset();
  IGNORE_RETURN iter_label([&](ObLabel &label_, LabelItem *l_item)
  {
    if (label_ == label) {
      item.hold_ = l_item->hold_;
      item.used_ = l_item->used_;
      item.count_ = l_item->count_;
    }
    return OB_SUCCESS;
  });
  return item;
}

int64_t ObTenantCtxAllocator::sync_wash(int64_t wash_size)
{
  int64_t washed_size = 0;

  auto stat = obj_mgr_.get_stat();
  const double min_utilization = 0.95;
  int64_t min_memory_fragment = 64LL << 20;
  if (stat.payload_ * min_utilization > stat.used_ ||
      stat.payload_ - stat.used_ >= min_memory_fragment) {
    washed_size = obj_mgr_.sync_wash(wash_size);
  }
  if (washed_size != 0 && REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
    _OB_LOG(INFO, "[MEM][WASH] tenant_id: %ld, ctx_id: %ld, washed_size: %ld", tenant_id_, ctx_id_, washed_size);
  }
  return washed_size;
}

int64_t ObTenantCtxAllocator::sync_wash()
{
  return ObMallocAllocator::get_instance()->sync_wash(tenant_id_, ctx_id_, INT64_MAX);
}

void ObTenantCtxAllocator::update_wash_stat(int64_t related_chunks, int64_t blocks, int64_t size)
{
  (void)ATOMIC_FAA(&wash_related_chunks_, related_chunks);
  (void)ATOMIC_FAA(&washed_blocks_, blocks);
  (void)ATOMIC_FAA(&washed_size_, size);
}

void ObTenantCtxAllocator::on_alloc(AObject& obj, const ObMemAttr& attr)
{
  obj.set_label(attr.label_.str_);
  if (attr.alloc_extra_info_) {
    void *addrs[100] = {nullptr};
    ob_backtrace(addrs, ARRAYSIZEOF(addrs));
    STATIC_ASSERT(AOBJECT_BACKTRACE_SIZE < sizeof(addrs), "AOBJECT_BACKTRACE_SIZE must be less than addrs!");
    MEMCPY(obj.bt(), (char*)addrs, AOBJECT_BACKTRACE_SIZE);
    obj.on_malloc_sample_ = true;
  }
  obj.ignore_version_ = attr.ignore_version()
      || ObMemVersionNode::tl_ignore_node
      || ObCtxIds::GLIBC == attr.ctx_id_;
  if (!obj.ignore_version_) {
    obj.version_ = ObMemVersionNode::tl_node->version_;
  }
  get_mem_leak_checker().on_alloc(obj, attr);
  SANITY_POISON(&obj, AOBJECT_HEADER_SIZE);
  SANITY_UNPOISON(obj.data_, obj.alloc_bytes_);
  SANITY_POISON(obj.data_ + obj.alloc_bytes_,
                AOBJECT_TAIL_SIZE + (obj.on_malloc_sample_ ? AOBJECT_BACKTRACE_SIZE : 0));
  if (OB_NOT_NULL(malloc_callback)) {
    const int64_t size = obj.alloc_bytes_;
    (*malloc_callback)(attr, size);
    for (auto *p = malloc_callback->next(); p != malloc_callback; p = p->next()) {
      (*p)(attr, size);
    }
  }
}

void ObTenantCtxAllocator::on_free(AObject &obj)
{

  abort_unless(obj.is_valid());
  abort_unless(obj.in_use_);

  ABlock *block = obj.block();
  abort_unless(block->is_valid());
  abort_unless(block->in_use_);
  abort_unless(NULL != block->obj_set_);

  SANITY_POISON(obj.data_, obj.alloc_bytes_);
  get_mem_leak_checker().on_free(obj);

  IBlockMgr *blk_mgr = ((ObjectSet*)block->obj_set_)->get_block_mgr();
  abort_unless(NULL != blk_mgr);

  int64_t tenant_id = blk_mgr->get_tenant_id();
  int64_t ctx_id = blk_mgr->get_ctx_id();
  char label[lib::AOBJECT_LABEL_SIZE + 1];
  MEMCPY(label, obj.label_, sizeof(label));
  ObMemAttr attr(tenant_id, label, ctx_id);
  if (OB_NOT_NULL(malloc_callback)) {
    const int64_t size = obj.alloc_bytes_;
    (*malloc_callback)(attr, -size);
    for (auto *p = malloc_callback->next(); p != malloc_callback; p = p->next()) {
      (*p)(attr, -size);
    }
  }
}

void ObTenantCtxAllocator::common_free(void *ptr)
{
  ObDisableDiagnoseGuard disable_diagnose_guard;
  SANITY_DISABLE_CHECK_RANGE(); // prevent sanity_check_range
  if (NULL != ptr) {
    AObject *obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
    on_free(*obj);
    ObjectSet *os = (ObjectSet*)obj->block()->obj_set_;
    os->free_object(obj);
  }
}
