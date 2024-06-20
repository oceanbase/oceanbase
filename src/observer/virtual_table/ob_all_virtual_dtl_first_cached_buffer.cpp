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

#include "observer/virtual_table/ob_all_virtual_dtl_first_cached_buffer.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "share/rc/ob_context.h"
#include "lib/allocator/ob_mod_define.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::observer;
using namespace oceanbase::share;


ObAllVirtualDtlFirstCachedBufferIterator::ObAllVirtualDtlFirstCachedBufferIterator(ObArenaAllocator *allocator) :
  cur_tenant_idx_(0),
  cur_buffer_idx_(0),
  iter_allocator_(allocator),
  tenant_ids_(),
  buffer_infos_()
{}

ObAllVirtualDtlFirstCachedBufferIterator::~ObAllVirtualDtlFirstCachedBufferIterator()
{
  destroy();
}

void ObAllVirtualDtlFirstCachedBufferIterator::reset()
{
  cur_tenant_idx_ = 0;
  cur_buffer_idx_ = 0;
  tenant_ids_.reset();
  buffer_infos_.reset();
  iter_allocator_->reuse();
}

void ObAllVirtualDtlFirstCachedBufferIterator::destroy()
{
  tenant_ids_.reset();
  buffer_infos_.reset();
  iter_allocator_ = nullptr;
}

int ObAllVirtualDtlFirstCachedBufferIterator::get_tenant_ids()
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

int ObAllVirtualDtlFirstCachedBufferIterator::init()
{
  int ret = OB_SUCCESS;
  buffer_infos_.set_block_allocator(ObWrapperAllocator(iter_allocator_));
  if (OB_FAIL(get_tenant_ids())) {
    LOG_WARN("failed to get tenant ids", K(ret));
  }
  return ret;
}

int ObAllVirtualDtlFirstCachedBufferIterator::get_tenant_buffer_infos(uint64_t tenant_id)
{
  UNUSED(tenant_id);
  return OB_SUCCESS;
}

int ObAllVirtualDtlFirstCachedBufferIterator::get_next_tenant_buffer_infos()
{
  int ret = OB_SUCCESS;
  if (0 != buffer_infos_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mem pool infos must be empty", K(ret));
  } else if (cur_tenant_idx_ < tenant_ids_.count()) {
    do {
      if (OB_FAIL(get_tenant_buffer_infos(tenant_ids_.at(cur_tenant_idx_)))) {
        LOG_WARN("failed to get dtl memory pool infos", K(ret));
      } else {
        ++cur_tenant_idx_;
      }
    } while (OB_SUCC(ret) && 0 == buffer_infos_.count() && cur_tenant_idx_ < tenant_ids_.count());
  } else {
    ret = OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    if (0 == buffer_infos_.count()) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObAllVirtualDtlFirstCachedBufferIterator::get_next_buffer_info(ObAllVirtualDtlFirstBufferInfo &buffer_info)
{
  int ret = OB_SUCCESS;
  bool get_buffer = false;
  do {
    if (cur_buffer_idx_ < buffer_infos_.count()) {
      buffer_info = buffer_infos_.at(cur_buffer_idx_);
      ++cur_buffer_idx_;
      get_buffer = true;
    } else {
      cur_buffer_idx_ = 0;
      buffer_infos_.reset();
      if (OB_FAIL(get_next_tenant_buffer_infos())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get dtl memory pool infos", K(ret));
        }
      }
    }
  } while (OB_SUCC(ret) && !get_buffer);

  return ret;
}

ObAllVirtualDtlFirstCachedBuffer::ObAllVirtualDtlFirstCachedBuffer() :
  ipstr_(),
  port_(0),
  arena_allocator_(ObModIds::OB_SQL_DTL),
  iter_(&arena_allocator_)
{}

ObAllVirtualDtlFirstCachedBuffer::~ObAllVirtualDtlFirstCachedBuffer()
{
  destroy();
}

void ObAllVirtualDtlFirstCachedBuffer::destroy()
{
  iter_.reset();
  arena_allocator_.reuse();
  arena_allocator_.reset();
}

void ObAllVirtualDtlFirstCachedBuffer::reset()
{
  port_ = 0;
  ipstr_.reset();
  iter_.reset();
  arena_allocator_.reuse();
  start_to_read_ = false;
}

int ObAllVirtualDtlFirstCachedBuffer::inner_open()
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

int ObAllVirtualDtlFirstCachedBuffer::inner_get_next_row(ObNewRow *&row)
{
  return OB_ITER_END;
}

int ObAllVirtualDtlFirstCachedBuffer::get_row(ObAllVirtualDtlFirstBufferInfo &buffer_info, ObNewRow *&row)
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
        cells[cell_idx].set_int(buffer_info.tenant_id_);
        break;
      }
      case CHANNEL_ID: {
        cells[cell_idx].set_int(buffer_info.channel_id_);
        break;
      }
      case CALCED_VAL: {
        cells[cell_idx].set_int(buffer_info.calced_val_);
        break;
      }
      case BUFFER_POOL_ID:{// OB_APP_MIN_COLUMN_ID + 5
        cells[cell_idx].set_int(buffer_info.buffer_pool_id_);
        break;
      }
      case TIMEOUT_TS: {
        cells[cell_idx].set_timestamp(buffer_info.timeout_ts_);
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
