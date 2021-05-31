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

#include "lib/alloc/ob_tenant_ctx_allocator.h"
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/alloc/memory_dump.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_smart_var.h"
#include "rpc/obrpc/ob_rpc_packet.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

int ObTenantCtxAllocator::iter_label(VisitFunc func) const
{
  int ret = OB_SUCCESS;
  struct ItemWrapper {
    ObLabel label_;
    union {
      LabelItem* l_item_;
      ObModItem* m_item_;
      void* item_;
    };
  };
  SMART_VAR(ObModItem[ObModSet::MOD_COUNT_LIMIT], m_items)
  {
    auto& mem_dump = ObMemoryDump().get_instance();
    if (OB_UNLIKELY(!mem_dump.is_inited())) {
      ret = OB_NOT_INIT;
      SERVER_LOG(WARN, "mem dump not inited", K(ret));
    } else {
      ObLatchRGuard guard(mem_dump.iter_lock_, common::ObLatchIds::MEM_DUMP_ITER_LOCK);
      const int item_cap = ARRAYSIZEOF(m_items) + 1024;
      ItemWrapper items[item_cap];
      int64_t item_cnt = 0;
      auto* up_litems = mem_dump.r_stat_->up2date_items_;
      auto& tcrs = mem_dump.r_stat_->tcrs_;
      int tcr_cnt = mem_dump.r_stat_->tcr_cnt_;
      auto it = std::lower_bound(
          tcrs, tcrs + tcr_cnt, std::make_pair(tenant_id_, ctx_id_), &ObMemoryDump::TenantCtxRange::compare);
      if (it != tcrs + tcr_cnt && it->tenant_id_ == tenant_id_ && it->ctx_id_ == ctx_id_) {
        auto& tcr = *it;
        for (int32_t idx = 0; idx < ObModSet::MOD_COUNT_LIMIT; idx++) {
          m_items[idx] = tcr.mod_set_->get_mod(idx);
          items[idx].label_ = idx;
          items[idx].item_ = &m_items[idx];
          item_cnt++;
        }
        for (int64_t j = tcr.start_; OB_SUCC(ret) && j < tcr.end_ && item_cnt < item_cap; ++j) {
          items[item_cnt].label_ = up_litems[j].str_;
          items[item_cnt].item_ = &up_litems[j];
          item_cnt++;
        }
      }
      if (OB_SUCC(ret)) {
        std::sort(items, items + item_cnt, [](ItemWrapper& l, ItemWrapper& r) {
          return (l.label_.is_str_ ? l.l_item_->hold_ : l.m_item_->hold_) >
                 (r.label_.is_str_ ? r.l_item_->hold_ : r.m_item_->hold_);
        });
        for (int64_t i = 0; OB_SUCC(ret) && i < item_cnt; ++i) {
          ret = func(items[i].label_, items[i].l_item_, items[i].m_item_);
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
  char buf[BUFLEN] = {};
  int64_t pos = 0;
  int64_t ctx_hold_bytes = 0;
  ObModItem m_sum_item;
  LabelItem l_sum_item;
  ret = iter_label([&](ObLabel& label, LabelItem* l_item, ObModItem* m_item) {
    int ret = OB_SUCCESS;
    if (label.is_str_) {
      if (l_item->count_ != 0) {
        ret = databuff_printf(buf,
            BUFLEN,
            pos,
            "[MEMORY] hold=% '15ld used=% '15ld count=% '8ld avg_used=% '15ld mod=%s\n",
            l_item->hold_,
            l_item->used_,
            l_item->count_,
            l_item->used_ / l_item->count_,
            label.str_);
      }
      l_sum_item += *l_item;
    } else {
      if (m_item->count_ != 0) {
        ret = databuff_printf(buf,
            BUFLEN,
            pos,
            "[MEMORY] hold=% '15ld used=% '15ld count=% '8ld avg_used=% '15ld mod=%s\n",
            m_item->hold_,
            m_item->used_,
            m_item->count_,
            m_item->used_ / m_item->count_,
            ObCtxIds::LIBEASY != ctx_id_ ? ObModSet::instance().get_mod_name(label.mod_id_)
                                         : obrpc::ObRpcPacketSet::instance().name_of_idx(label.mod_id_));
      }
      m_sum_item += *m_item;
    }
    return ret;
  });
  m_sum_item.hold_ += l_sum_item.hold_;
  m_sum_item.used_ += l_sum_item.used_;
  m_sum_item.count_ += l_sum_item.count_;
  if (OB_SUCC(ret) && m_sum_item.count_ > 0) {
    ret = databuff_printf(buf,
        BUFLEN,
        pos,
        "[MEMORY] hold=% '15ld used=% '15ld count=% '8ld avg_used=% '15ld mod=%s\n",
        m_sum_item.hold_,
        m_sum_item.used_,
        m_sum_item.count_,
        m_sum_item.used_ / m_sum_item.count_,
        "SUMMARY");
  }
  if (OB_SUCC(ret)) {
    ret = with_resource_handle_invoke([&](const ObTenantMemoryMgr* mgr) {
      ctx_hold_bytes = mgr->get_ctx_hold_bytes()[ctx_id_];
      return OB_SUCCESS;
    });
  }

  _LOG_INFO("\n[MEMORY] tenant_id=%5ld ctx_id=%25s hold=% '15ld used=% '15ld\n%s",
      tenant_id_,
      get_global_ctx_info().get_ctx_name(ctx_id_),
      ctx_hold_bytes,
      m_sum_item.used_,
      buf);
}

AChunk* ObTenantCtxAllocator::pop_chunk()
{
  lib::ObMutexGuard guard(chunk_freelist_mutex_);
  AChunk* chunk = head_chunk_.next_;
  AChunk* next_chunk = nullptr == chunk ? nullptr : chunk->next_;
  head_chunk_.next_ = next_chunk;
  if (chunk != nullptr) {
    --chunk_cnt_;
  }
  return chunk;
}

void ObTenantCtxAllocator::push_chunk(AChunk* chunk)
{
  lib::ObMutexGuard guard(chunk_freelist_mutex_);
  chunk->next_ = head_chunk_.next_;
  head_chunk_.next_ = chunk;
  ++chunk_cnt_;
}

AChunk* ObTenantCtxAllocator::alloc_chunk(const int64_t size, const ObMemAttr& attr)
{
  AChunk* chunk = nullptr;

  if (INTACT_ACHUNK_SIZE == AChunkMgr::aligned(size)) {
    chunk = pop_chunk();
  }
  if (nullptr == chunk) {
    if (!resource_handle_.is_valid()) {
      LIB_LOG(ERROR, "resource_handle is invalid", K_(tenant_id), K_(ctx_id));
    } else {
      chunk = resource_handle_.get_memory_mgr()->alloc_chunk(size, attr);
    }
  }

  if (chunk != nullptr) {
    lib::ObMutexGuard guard(using_list_mutex_);
    chunk->prev2_ = &using_list_head_;
    chunk->next2_ = using_list_head_.next2_;
    using_list_head_.next2_->prev2_ = chunk;
    using_list_head_.next2_ = chunk;
  }

  return chunk;
}

void ObTenantCtxAllocator::free_chunk(AChunk* chunk, const ObMemAttr& attr)
{
  if (chunk != nullptr) {
    lib::ObMutexGuard guard(using_list_mutex_);
    chunk->prev2_->next2_ = chunk->next2_;
    chunk->next2_->prev2_ = chunk->prev2_;
  }
  if (INTACT_ACHUNK_SIZE == AChunkMgr::aligned(chunk->size_) && get_hold() - INTACT_ACHUNK_SIZE < idle_size_) {
    push_chunk(chunk);
  } else {
    if (!resource_handle_.is_valid()) {
      LIB_LOG(ERROR, "resource_handle is invalid", K_(tenant_id), K_(ctx_id));
    } else {
      resource_handle_.get_memory_mgr()->free_chunk(chunk, attr);
    }
  }
}

int ObTenantCtxAllocator::set_idle(const int64_t set_size, const bool reserve /*=false*/)
{
  int ret = OB_SUCCESS;
  const int64_t limit = get_limit();
  const int64_t size = lower_align(set_size, INTACT_ACHUNK_SIZE);
  if (size > limit || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid argument", K_(tenant_id), K_(ctx_id), K(size), K(limit));
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
      AChunk* chunk = nullptr;
      while (get_hold() - INTACT_ACHUNK_SIZE >= size && (chunk = pop_chunk()) != nullptr) {
        resource_handle_.get_memory_mgr()->free_chunk(chunk, default_attr);
      }
    } else {
      if (reserve) {
        const int64_t ori_chunk_cnt = chunk_cnt_;
        while (OB_SUCC(ret) && get_hold() < size) {
          AChunk* chunk = resource_handle_.get_memory_mgr()->alloc_chunk(ACHUNK_SIZE, default_attr);
          if (OB_ISNULL(chunk)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LIB_LOG(ERROR, "alloc chunk failed", K(ret), K_(tenant_id), K_(ctx_id));
          } else {
            push_chunk(chunk);
          }
        }
        // cleanup
        if (OB_FAIL(ret)) {
          AChunk* chunk = nullptr;
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
    LIB_LOG(INFO, "set idle finish", K(ret), K_(tenant_id), K_(ctx_id), K_(idle_size), K_(chunk_cnt));
  }
  return ret;
}

void ObTenantCtxAllocator::get_chunks(AChunk** chunks, int cap, int& cnt)
{
  lib::ObMutexGuard guard(using_list_mutex_);
  AChunk* cur = using_list_head_.next2_;
  while (cur != &using_list_head_ && cnt < cap) {
    chunks[cnt++] = cur;
    cur = cur->next2_;
  }
}
