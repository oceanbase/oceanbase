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

#include "ob_dtl_tenant_mem_manager.h"
#include "share/ob_errno.h"
#include "share/config/ob_server_config.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/dtl/ob_dtl_channel_mem_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;


ObDtlTenantMemManager::ObDtlTenantMemManager(uint64_t tenant_id)
: tenant_id_(tenant_id)
{}

int ObDtlTenantMemManager::init()
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  hash_cnt_ = next_pow2(common::ObServerConfig::get_instance()._px_chunklist_count_ratio) * HASH_CNT;
  ObMemAttr attr(tenant_id_, "SqlDtlMgr");
  buf = reinterpret_cast<char*>(ob_malloc(hash_cnt_ * sizeof(ObDtlChannelMemManager), attr));
  if (nullptr == buf) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc channel memory manager", K(ret));
  } else if (OB_FAIL(mem_mgrs_.reserve(hash_cnt_))) {
    LOG_WARN("failed to reserver memory manager", K(ret));
  } else if (OB_FAIL(times_.reserve(hash_cnt_))) {
    LOG_WARN("failed to reserver times", K(ret));
  } else {
    for (int i = 0; i < hash_cnt_ && OB_SUCC(ret); ++i) {
      ObDtlChannelMemManager *ch_mem_mgr = new (buf + i * sizeof(ObDtlChannelMemManager)) ObDtlChannelMemManager (tenant_id_, *this);
      if (OB_FAIL(ch_mem_mgr->init())) {
        LOG_WARN("failed to init channel memory manager", K(ret));
      } else {
        ch_mem_mgr->set_seqno(i);
        if (OB_FAIL(mem_mgrs_.push_back(ch_mem_mgr))) {
          LOG_WARN("failed to push back memory manager", K(ret));
        }
      }
    }
    for (int i = 0; i < hash_cnt_ && OB_SUCC(ret); ++i) {
      if (OB_FAIL(times_.push_back(0))) {
        LOG_WARN("failed to push back memory manager", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != buf) {
      ObDtlChannelMemManager *ch_mem_mgr = nullptr;
      int tmp_ret = OB_SUCCESS;
      int64_t cnt = mem_mgrs_.count();
      for (int i = 0; i <  cnt && OB_SUCCESS == tmp_ret; ++i) {
        if (OB_SUCCESS != (tmp_ret = mem_mgrs_.pop_back(ch_mem_mgr))) {
          LOG_ERROR("failed to push back memory manager", K(ret));
        } else {
          ch_mem_mgr->destroy();
        }
      }
      ob_free(buf);
      buf = nullptr;
    }
    times_.reset();
    int64_t ratio = common::ObServerConfig::get_instance()._px_chunklist_count_ratio;
    LOG_WARN("failed to init tenant memory manager", K(ret),
      "dtl buffer ratio", ratio);
  }
  return ret;
}

int ObDtlTenantMemManager::get_channel_mem_manager(int64_t idx, ObDtlChannelMemManager *&mgr)
{
  int ret = OB_SUCCESS;
  mgr = nullptr;
  if (0 > idx || idx > hash_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected idx", K(ret), K(idx));
  } else {
    mgr = mem_mgrs_.at(idx);
    if (nullptr == mgr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mgr is null", K(ret), K(idx));
    }
  }
  return ret;
}

void ObDtlTenantMemManager::destroy()
{
  int ret = OB_SUCCESS;
  int64_t cnt = mem_mgrs_.count();
  if (0 < cnt) {
    ObDtlChannelMemManager *ch_mem_mgr = nullptr;
    for (int i = 0; i < cnt && OB_SUCC(ret); ++i) {
      ObDtlChannelMemManager *tmp_ch_mem_mgr = nullptr;
      if (OB_FAIL(mem_mgrs_.pop_back(tmp_ch_mem_mgr))) {
        LOG_ERROR("failed to push back memory manager", K(ret));
      }
      tmp_ch_mem_mgr->destroy();
      ch_mem_mgr = tmp_ch_mem_mgr;
    }
    if (nullptr == ch_mem_mgr) {
      LOG_ERROR("failed to destroy memory manager", K(ret));
    } else {
      ob_free(ch_mem_mgr);
    }
    ch_mem_mgr = nullptr;
  }
  mem_mgrs_.reset();
  times_.reset();
}

ObDtlLinkedBuffer *ObDtlTenantMemManager::alloc(int64_t chid, int64_t size)
{
  int ret = OB_SUCCESS;
  ObDtlLinkedBuffer *buf = nullptr;
  int64_t hash_val = hash(chid);
  if (0 > hash_val || hash_val >= hash_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the has value must be less than hash_cnt_", K(ret), KP(chid), K(hash_val));
  } else {
    ObDtlChannelMemManager *mem_mgr = mem_mgrs_.at(hash_val);
    int64_t &n_times = times_.at(hash_val);
    ++ n_times;
    buf = mem_mgr->alloc(chid, size);
    if (nullptr == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate dtl buffer memory", K(ret));
    }
  }
  return buf;
}

int ObDtlTenantMemManager::free(ObDtlLinkedBuffer *buf)
{
  int ret = OB_SUCCESS;
  int64_t hash_val = hash(buf->allocated_chid());
  if (0 > hash_val || hash_val >= hash_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the has value must be less than hash_cnt_", K(ret), K(hash_val), K(buf->allocated_chid()));
  } else {
    ObDtlChannelMemManager *mem_mgr = mem_mgrs_.at(hash_val);
    if (OB_FAIL(mem_mgr->free(buf))) {
      // report log while unexpect
      LOG_DEBUG("Free buffer queue is full", K(ret), K(hash_val),
        K(mem_mgr->get_free_queue_length()), K(mem_mgr->queue_cnt()),
        K(mem_mgr->get_alloc_cnt()), K(mem_mgr->get_free_cnt()));
      // buffer_status();
      if (OB_SIZE_OVERFLOW == ret) {
        LOG_TRACE("overflow queue capacity", K(ret), K(hash_val));
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int64_t ObDtlTenantMemManager::avg_alloc_times()
{
  int64_t avg = 0;
  int64_t total = 0;
  if (0 < times_.count()) {
    for (int i = 0; i < times_.count(); ++i) {
      int64_t n_times = times_.at(i);
      total += n_times;
    }
    avg = total / times_.count();
  }
  return avg;
}

int64_t ObDtlTenantMemManager::variance_alloc_times()
{
  int64_t avg = avg_alloc_times();
  int64_t sq = 0;
  if (0 < times_.count()) {
    for (int i = 0; i < times_.count(); ++i) {
      int64_t n_times = times_.at(i);
      int64_t diff = avg - n_times;
      sq += diff * diff;
    }
    sq = sq / times_.count();
  }
  return sq;
}

int64_t ObDtlTenantMemManager::get_min_buffer_size()
{
  int64_t reserve_buffer_min_size = 0;
  ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (tenant_config.is_valid()) {
    reserve_buffer_min_size = tenant_config->_parallel_min_message_pool;
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "failed to init tenant config", K(tenant_id_));
  }
  return reserve_buffer_min_size;
}

int ObDtlTenantMemManager::auto_free_on_time()
{
  int ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
  int64_t reserve_buffer_min_size = get_min_buffer_size();
  int64_t buffer_size = 0;
  int64_t max_reserve_count = 1;
  for (int i = 0; i < mem_mgrs_.count(); ++i) {
    ObDtlChannelMemManager *mem_mgr = mem_mgrs_.at(i);
    if (0 == buffer_size) {
      buffer_size = mem_mgr->get_buffer_size();
      max_reserve_count = reserve_buffer_min_size / buffer_size / hash_cnt_;
    }
    if (OB_SUCCESS != (tmp_ret = mem_mgr->auto_free_on_time(max_reserve_count))) {
      ret = tmp_ret;
      LOG_TRACE("failed to auto free memory buffer manager", K(ret));
    }
  }
  LOG_INFO("auto free to reserve buffer count", K(reserve_buffer_min_size), K(max_reserve_count), K(buffer_size), K(ret));
  return ret;
}

void ObDtlTenantMemManager::buffer_status()
{
  int64_t variance_val = variance_alloc_times();
  int64_t avg = avg_alloc_times();
  int64_t cnt_times = times_.count();
  LOG_INFO("tenant memory buffer status", K(tenant_id_), K(variance_val), K(avg));
  for (int i = 0; i < mem_mgrs_.count(); ++i) {
    ObDtlChannelMemManager *mem_mgr = mem_mgrs_.at(i);
    int64_t n_times = -1;
    if (i < cnt_times) {
      n_times = times_.at(i);
    }
    if (0 < mem_mgr->get_free_queue_length() || 0 < mem_mgr->get_free_cnt()) {
      LOG_TRACE("tenant memory buffer status", K(i), K(mem_mgr->get_free_queue_length()),
      K(mem_mgr->queue_cnt()), K(mem_mgr->get_alloc_cnt()), K(mem_mgr->get_free_cnt()),
      K(n_times));
    }
  }
}

int64_t ObDtlTenantMemManager::get_used_memory_size()
{
  int64_t used = 0;
  for (int64_t i = 0; i < mem_mgrs_.count(); ++i) {
    used += mem_mgrs_.at(i)->get_total_memory_size();
  }
  return used;
}
