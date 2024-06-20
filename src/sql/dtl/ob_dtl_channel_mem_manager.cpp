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

#define USING_LOG_PREFIX SQL_DTL

#include "ob_dtl_channel_mem_manager.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "src/sql/dtl/ob_dtl_tenant_mem_manager.h"
#include "share/ob_occam_time_guard.h"
#include "lib/utility/ob_tracepoint.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::omt;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

ObDtlChannelMemManager::ObDtlChannelMemManager(uint64_t tenant_id, ObDtlTenantMemManager &tenant_mgr) :
  tenant_id_(tenant_id), size_per_buffer_(GCONF.dtl_buffer_size), seqno_(-1), allocator_(tenant_id), pre_alloc_cnt_(0),
  max_mem_percent_(0), memstore_limit_percent_(0), alloc_cnt_(0), free_cnt_(0), real_alloc_cnt_(0), real_free_cnt_(0), tenant_mgr_(tenant_mgr),
  mem_used_(0), last_update_memory_time_(-1)
{}

int ObDtlChannelMemManager::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id_, "SqlDtlBuf");
  if (OB_FAIL(allocator_.init(
                lib::ObMallocAllocator::get_instance(),
                OB_MALLOC_NORMAL_BLOCK_SIZE,
                attr))) {
    LOG_WARN("failed to init fifo allocator", K(ret));
  } else if (OB_FAIL(free_queue_.init(MAX_CAPACITY, "SqlDtlQueue", tenant_id_))) {
    LOG_WARN("failed to init channel memory manager", K(ret));
  } else {
    allocator_.set_label("SqlDtlBuf");
  }
  if (OB_FAIL(ret)) {
    allocator_.reset();
  }
  return ret;
}

int ObDtlChannelMemManager::get_max_mem_percent()
{
  int ret = OB_SUCCESS;
  ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (tenant_config.is_valid()) {
    max_mem_percent_ = tenant_config->_px_max_message_pool_pct;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to init tenant config", K(tenant_id_), K(ret));
  }
  return ret;
}

int ObDtlChannelMemManager::get_memstore_limit_percentage_()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id_) {
    memstore_limit_percent_ = MTL(ObTenantFreezer*)->get_memstore_limit_percentage();
  }
  return ret;
}

void ObDtlChannelMemManager::destroy()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  int64_t free_cnt = 0;
  while (OB_SUCC(ret) && 0 < free_queue_.size()) {
    if (OB_FAIL(free_queue_.pop(buf, 10))) {
      LOG_WARN("failed to pop buffer from free queue", K(ret), K(seqno_));
    } else {
      real_free(static_cast<ObDtlLinkedBuffer *>(buf));
      ++ free_cnt;
    }
  }
  LOG_INFO("pop buffer from free queue to destroy",
    K(ret), K(seqno_), K(free_cnt_), K(alloc_cnt_), K(free_queue_.size()), K(free_cnt));
  free_queue_.reset();
  free_queue_.destroy();
  allocator_.reset();
}

ObDtlLinkedBuffer *ObDtlChannelMemManager::alloc(int64_t chid, int64_t size)
{
  int ret = OB_SUCCESS;
  ObDtlLinkedBuffer *allocated_buf = NULL;
  void *buf = nullptr;
  if (size <= size_per_buffer_) {
    if (OB_SUCC(free_queue_.pop(buf, 0)) && NULL != buf) {
      allocated_buf = new (buf) ObDtlLinkedBuffer(
        static_cast<char *>(buf) + sizeof (ObDtlLinkedBuffer), size_per_buffer_);
      allocated_buf->allocated_chid() = chid;
    } else {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_TRACE("queue has no element", K(ret), K(seqno_), K(free_queue_.size()));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to pop back buffer", K(ret), K(seqno_), K(free_queue_.size()));
      }
    }
  }
  if (nullptr != allocated_buf) {
  } else if (out_of_memory()) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("the memory of dtl reach the maxinum memory limit", K(ret), K(get_used_memory_size()),
      K(get_max_tenant_memory_limit_size()), K(get_max_dtl_memory_size()),
      K(max_mem_percent_), K_(memstore_limit_percent), K(allocated_buf), K(size));
  } else {
    const int64_t alloc_size = sizeof (ObDtlLinkedBuffer)
        + std::max(size, size_per_buffer_);
    char *buf = reinterpret_cast<char*>(allocator_.alloc(alloc_size));
    if (nullptr == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else {
      allocated_buf = new (buf) ObDtlLinkedBuffer(
          buf + sizeof (ObDtlLinkedBuffer),
          alloc_size - sizeof(ObDtlLinkedBuffer));
      allocated_buf->allocated_chid() = chid;
      if (0 < get_free_queue_length()) {
        LOG_TRACE("Trace to allocate buffer", K(ret), K(seqno_), K(allocated_buf), K(lbt()),
          K(get_free_cnt()), K(get_free_queue_length()), K(queue_cnt()), K(get_alloc_cnt()));
      }
      if (nullptr != allocated_buf) {
        ++real_alloc_cnt_;
      }
      LOG_TRACE("Trace to allocate memory", K(ret), K(seqno_), K(allocated_buf), K(lbt()));
    }
  }
  if (nullptr != allocated_buf) {
    increase_alloc_cnt();
  }
  uint64_t opt = std::abs(EVENT_CALL(EventTable::EN_PX_DTL_TRACE_LOG_ENABLE));
  if (0 != opt) {
    share::ObTaskController::get().allow_next_syslog();
    LOG_INFO("alloc dtl buffer", KP(allocated_buf));
  }
  LOG_TRACE("channel memory status", K(get_alloc_cnt()), K(get_free_cnt()),
    K(get_free_queue_length()), K(get_max_tenant_memory_limit_size()), K(get_max_dtl_memory_size()),
    K(get_used_memory_size()), K(max_mem_percent_), KP(allocated_buf), K(seqno_));
  return allocated_buf;
}

int ObDtlChannelMemManager::free(ObDtlLinkedBuffer *buf, bool auto_free)
{
  int ret = OB_SUCCESS;
  if (NULL != buf) {
    buf->reset_batch_info();
    if (auto_free && buf->size() <= size_per_buffer_) {
      if (OB_FAIL(free_queue_.push(buf))) {
        LOG_TRACE("failed to push back buffer", K(ret), K(seqno_), K(free_queue_.size()));
      } else {
        increase_free_cnt();
        buf = NULL;
      }
    }
    if (NULL != buf) {
      real_free(buf);
      increase_free_cnt();
    }
    LOG_TRACE("channel memory status", K(get_alloc_cnt()), K(get_free_cnt()), K(get_free_queue_length()), KP(buf), K(seqno_), K(auto_free));
  }
  return ret;
}

void ObDtlChannelMemManager::real_free(ObDtlLinkedBuffer *buf)
{
  if (NULL != buf) {
    ++real_free_cnt_;
    buf->~ObDtlLinkedBuffer();
    allocator_.free(buf);
    LOG_TRACE("Trace to free buffer", K(seqno_), KP(buf));
  }
}

int ObDtlChannelMemManager::auto_free_on_time(int64_t cur_max_reserve_count)
{
  int ret = OB_SUCCESS;
  const int64_t ts = 10;
  if (cur_max_reserve_count <= 0) {
    cur_max_reserve_count = 1;
  }
  LOG_TRACE("trace auto free", K(get_alloc_cnt()), K(get_free_cnt()), K(get_free_queue_length()),  K(seqno_), K(cur_max_reserve_count));
  if (free_queue_.size() > cur_max_reserve_count) {
    int64_t delta = alloc_cnt_ - pre_alloc_cnt_;
    int64_t delta_per_sec = delta / ts;
    int64_t reserve_cnt = cur_max_reserve_count;
    if (delta_per_sec > cur_max_reserve_count) {
      reserve_cnt = delta_per_sec;
    }
    if (0 < reserve_cnt) {
      // 考虑到并发，这里要么释放到足够多buffer，要么free_queue_.size()大于reserve_cnt
      int64_t need_free_cnt = free_queue_.size() - reserve_cnt;
      void *buf = nullptr;
      while (OB_SUCC(ret) && (0 < need_free_cnt && reserve_cnt < free_queue_.size())) {
        if (OB_FAIL(free_queue_.pop(buf, 0))) {
          LOG_WARN("failed to pop buffer from free queue", K(ret), K(seqno_));
        } else {
          free(static_cast<ObDtlLinkedBuffer *>(buf), false);
          --need_free_cnt;
        }
      }
    }
  }
  pre_alloc_cnt_ = alloc_cnt_;
  update_max_memory_percent();
  //LOG_INFO("auto free channel buffer", K(max_mem_percent_));
  return ret;
}

int64_t ObDtlChannelMemManager::get_used_memory_size()
{
  int64_t curr_time = ::oceanbase::common::ObTimeUtility::current_time();
  if (OB_UNLIKELY(curr_time - last_update_memory_time_ >= static_cast<int64_t> (100_ms))) {
    last_update_memory_time_ = curr_time;
    mem_used_ = tenant_mgr_.get_used_memory_size();
  }
  return mem_used_;
}
