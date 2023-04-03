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

#include "observer/virtual_table/ob_all_virtual_dtl_memory.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "share/rc/ob_context.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_basic_channel.h"
#include "sql/dtl/ob_dtl_tenant_mem_manager.h"
#include "sql/dtl/ob_op_metric.h"
#include "lib/allocator/ob_mod_define.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::observer;
using namespace oceanbase::share;


void ObAllVirtualDtlMemoryPoolInfo::set_mem_pool_info(ObTenantDfc *&tenant_dfc, ObDtlChannelMemManager *mgr)
{
  tenant_id_ = tenant_dfc->get_tenant_id();
  channel_total_cnt_ = tenant_dfc->get_channel_cnt();
  channel_block_cnt_ = tenant_dfc->get_current_total_blocked_cnt();
  max_parallel_cnt_ = tenant_dfc->get_max_parallel();
  max_blocked_buffer_size_ = tenant_dfc->get_max_blocked_buffer_size();
  accumulated_block_cnt_ =tenant_dfc->get_accumulated_blocked_cnt();
  current_buffer_used_ = tenant_dfc->get_current_buffer_used();
  seqno_ = mgr->get_seqno();
  alloc_cnt_ = mgr->get_alloc_cnt();
  free_cnt_ = mgr->get_free_cnt();
  free_queue_len_ = mgr->get_free_queue_length();
  total_memory_size_ = mgr->get_total_memory_size();
  real_alloc_cnt_ = mgr->get_real_alloc_cnt();
  real_free_cnt_ = mgr->get_real_free_cnt();
}

ObAllVirtualDtlMemoryIterator::ObAllVirtualDtlMemoryIterator(ObArenaAllocator *allocator) :
  cur_tenant_idx_(0),
  cur_mem_pool_idx_(0),
  iter_allocator_(allocator),
  tenant_ids_(),
  mem_pool_infos_()
{}

ObAllVirtualDtlMemoryIterator::~ObAllVirtualDtlMemoryIterator()
{
  destroy();
}

void ObAllVirtualDtlMemoryIterator::reset()
{
  cur_tenant_idx_ = 0;
  cur_mem_pool_idx_ = 0;
  tenant_ids_.reset();
  mem_pool_infos_.reset();
  iter_allocator_->reuse();
}

void ObAllVirtualDtlMemoryIterator::destroy()
{
  tenant_ids_.reset();
  mem_pool_infos_.reset();
  iter_allocator_ = nullptr;
}

int ObAllVirtualDtlMemoryIterator::get_tenant_ids()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == GCTX.omt_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "GCTX.omt_ shouldn't be NULL",
        K_(GCTX.omt), K(GCTX), K(ret));
  } else if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids_))) {
    LOG_WARN("failed to get_mtl_tenant_ids", K(ret));
  }
  return ret;
}

int ObAllVirtualDtlMemoryIterator::init()
{
  int ret = OB_SUCCESS;
  mem_pool_infos_.set_block_allocator(ObWrapperAllocator(iter_allocator_));
  if (OB_FAIL(get_tenant_ids())) {
    LOG_WARN("failed to get tenant ids", K(ret));
  }
  return ret;
}

int ObAllVirtualDtlMemoryIterator::get_tenant_memory_pool_infos(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObTenantDfc *tenant_dfc = MTL(ObTenantDfc*);
    ObDtlTenantMemManager *mem_mgr = tenant_dfc->get_tenant_mem_manager();
    int64_t cnt = mem_mgr->get_channel_mgr_count();
    for (int64_t i = 0; i < cnt && OB_SUCC(ret); ++i) {
      ObDtlChannelMemManager *chan_mem_mgr = nullptr;
      if (OB_FAIL(mem_mgr->get_channel_mem_manager(i, chan_mem_mgr))) {
        LOG_WARN("failed to get channel memory manager", K(ret), K(i));
      } else {
        ObAllVirtualDtlMemoryPoolInfo mem_pool_info;
        mem_pool_info.set_mem_pool_info(tenant_dfc, chan_mem_mgr);
        if (OB_FAIL(mem_pool_infos_.push_back(mem_pool_info))) {
          LOG_WARN("failed to push back memory pool info", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualDtlMemoryIterator::get_next_memory_pools()
{
  int ret = OB_SUCCESS;
  if (0 != mem_pool_infos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem pool infos must be empty", K(ret));
  } else if (cur_tenant_idx_ < tenant_ids_.count()) {
    do {
      if (OB_FAIL(get_tenant_memory_pool_infos(tenant_ids_.at(cur_tenant_idx_)))) {
        LOG_WARN("failed to get dtl memory pool infos", K(ret));
      } else {
        ++cur_tenant_idx_;
      }
    } while (OB_SUCC(ret) && 0 == mem_pool_infos_.count() && cur_tenant_idx_ < tenant_ids_.count());
  } else {
    ret = OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    if (0 == mem_pool_infos_.count()) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObAllVirtualDtlMemoryIterator::get_next_mem_pool_info(ObAllVirtualDtlMemoryPoolInfo &memory_pool_info)
{
  int ret = OB_SUCCESS;
  bool get_pool = false;
  do {
    if (cur_mem_pool_idx_ < mem_pool_infos_.count()) {
      memory_pool_info = mem_pool_infos_.at(cur_mem_pool_idx_);
      ++cur_mem_pool_idx_;
      get_pool = true;
    } else {
      cur_mem_pool_idx_ = 0;
      mem_pool_infos_.reset();
      if (OB_FAIL(get_next_memory_pools())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get dtl memory pool infos", K(ret));
        }
      }
    }
  } while (OB_SUCC(ret) && !get_pool);

  return ret;
}

ObAllVirtualDtlMemory::ObAllVirtualDtlMemory() :
  ipstr_(),
  port_(0),
  arena_allocator_(ObModIds::OB_SQL_DTL),
  iter_(&arena_allocator_)
{}

ObAllVirtualDtlMemory::~ObAllVirtualDtlMemory()
{
  destroy();
}

void ObAllVirtualDtlMemory::destroy()
{
  iter_.reset();
  arena_allocator_.reuse();
  arena_allocator_.reset();
}

void ObAllVirtualDtlMemory::reset()
{
  port_ = 0;
  ipstr_.reset();
  iter_.reset();
  arena_allocator_.reuse();
  start_to_read_ = false;
}

int ObAllVirtualDtlMemory::inner_open()
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    if (OB_FAIL(iter_.init())) {
      LOG_WARN("failed to init iterator", K(ret));
    } else {
      start_to_read_ = true;
      char ipbuf[common::OB_IP_STR_BUFF];
      const common::ObAddr &addr = GCTX.self_addr();
      if (!addr.ip_to_string(ipbuf, sizeof(ipbuf))) {
        SERVER_LOG(ERROR, "ip to string failed");
        ret = OB_ERR_UNEXPECTED;
      } else {
        ipstr_ = ObString::make_string(ipbuf);
        if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
          SERVER_LOG(WARN, "failed to write string", K(ret));
        }
        port_ = addr.get_port();
      }
    }
  }
  return ret;
}

int ObAllVirtualDtlMemory::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObAllVirtualDtlMemoryPoolInfo mem_pool_info;
  if (OB_FAIL(iter_.get_next_mem_pool_info(mem_pool_info))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next channel", K(ret));
    } else {
      arena_allocator_.reuse();
    }
  } else if (OB_FAIL(get_row(mem_pool_info, row))) {
    LOG_WARN("failed to get row from channel info", K(ret));
  }
  return ret;
}

int ObAllVirtualDtlMemory::get_row(ObAllVirtualDtlMemoryPoolInfo &mem_pool_info, ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < output_column_ids_.count(); ++cell_idx) {
    uint64_t col_id = output_column_ids_.at(cell_idx);
    switch(col_id) {
      case SVR_IP: {
        cells[cell_idx].set_varchar(ipstr_);
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case SVR_PORT: {
        cells[cell_idx].set_int(port_);
        break;
      }
      case TENANT_ID: {
        cells[cell_idx].set_int(mem_pool_info.tenant_id_);
        break;
      }
      case CHANNEL_TOTAL_CNT: {
        cells[cell_idx].set_int(mem_pool_info.channel_total_cnt_);
        break;
      }
      case CHANNEL_BLOCK_CNT: {
        cells[cell_idx].set_int(mem_pool_info.channel_block_cnt_);
        break;
      }
      case MAX_PARALLEL_CNT:{// OB_APP_MIN_COLUMN_ID + 5
        cells[cell_idx].set_int(mem_pool_info.max_parallel_cnt_);
        break;
      }
      case MAX_BLOCKED_BUFFER_SIZE: {
        cells[cell_idx].set_int(mem_pool_info.max_blocked_buffer_size_);
        break;
      }
      case ACCUMULATED_BLOCK_CNT: {
        cells[cell_idx].set_int(mem_pool_info.accumulated_block_cnt_);
        break;
      }
      case CURRENT_BUFFER_USED: {
        cells[cell_idx].set_int(mem_pool_info.current_buffer_used_);
        break;
      }
      case SQENO: {
        cells[cell_idx].set_int(mem_pool_info.seqno_);
        break;
      }
      case ALLOC_CNT: {// OB_APP_MIN_COLUMN_ID + 10
        cells[cell_idx].set_int(mem_pool_info.alloc_cnt_);
        break;
      }
      case FREE_CNT: {
        cells[cell_idx].set_int(mem_pool_info.free_cnt_);
        break;
      }
      case FREE_QUEUE_LEN: {
        cells[cell_idx].set_int(mem_pool_info.free_queue_len_);
        break;
      }
      case TOTAL_MEMORY_SIZE: {
        cells[cell_idx].set_int(mem_pool_info.total_memory_size_);
        break;
      }
      case REAL_ALLOC_CNT: {
        cells[cell_idx].set_int(mem_pool_info.real_alloc_cnt_);
        break;
      }
      case REAL_FREE_CNT: {// OB_APP_MIN_COLUMN_ID + 15
        cells[cell_idx].set_int(mem_pool_info.real_free_cnt_);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column id", K(col_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}
