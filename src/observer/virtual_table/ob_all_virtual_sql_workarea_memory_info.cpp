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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_all_virtual_sql_workarea_memory_info.h"
#include "lib/allocator/ob_mod_define.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_context.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;
using namespace oceanbase::share;


ObSqlWorkareaMemoryInfoIterator::ObSqlWorkareaMemoryInfoIterator() :
  memory_info_(), tenant_ids_(), cur_nth_tenant_(0)
{}

void ObSqlWorkareaMemoryInfoIterator::destroy()
{
  reset();
}

void ObSqlWorkareaMemoryInfoIterator::reset()
{
  memory_info_.enable_ = false;
  tenant_ids_.reset();
  cur_nth_tenant_ = 0;
}

int ObSqlWorkareaMemoryInfoIterator::init(const uint64_t effective_tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null of omt", KR(ret));
  } else if (is_sys_tenant(effective_tenant_id)) {
    if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids_))) {
      LOG_WARN("failed to get_mtl_tenant_ids", KR(ret));
    }
  } else if (OB_FAIL(tenant_ids_.push_back(effective_tenant_id))) {
    LOG_WARN("failed to push back tenant_id", KR(ret), K(effective_tenant_id));
  }
  return ret;
}

int ObSqlWorkareaMemoryInfoIterator::get_next_batch_wa_memory_info()
{
  int ret = OB_SUCCESS;
  if (cur_nth_tenant_ >= tenant_ids_.count()) {
    ret = OB_ITER_END;
  } else {
    uint64_t tenant_id = tenant_ids_.at(cur_nth_tenant_);
    MTL_SWITCH(tenant_id) {
      ObTenantSqlMemoryManager *sql_mem_mgr = nullptr;
      sql_mem_mgr = MTL(ObTenantSqlMemoryManager*);
      if (nullptr != sql_mem_mgr && OB_FAIL(sql_mem_mgr->get_workarea_memory_info(memory_info_))) {
        LOG_WARN("failed to get workarea stat", K(ret));
      }
    }
    ++cur_nth_tenant_;
  }
  return ret;
}

int ObSqlWorkareaMemoryInfoIterator::get_next_wa_memory_info(
  ObSqlWorkareaCurrentMemoryInfo *&memory_info,
  uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_batch_wa_memory_info())) {
    } else if (memory_info_.is_valid()) {
      break;
    }
  }
  if (OB_SUCC(ret)) {
    memory_info = &memory_info_;
    tenant_id = tenant_ids_.at(cur_nth_tenant_ - 1);
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////
ObSqlWorkareaMemoryInfo::ObSqlWorkareaMemoryInfo() :
  ipstr_(), port_(0), iter_()
{}

void ObSqlWorkareaMemoryInfo::destroy()
{
  ipstr_.reset();
  iter_.destroy();
}

void ObSqlWorkareaMemoryInfo::reset()
{
  port_ = 0;
  ipstr_.reset();
  iter_.reset();
  start_to_read_ = false;
}

int ObSqlWorkareaMemoryInfo::get_server_ip_and_port()
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  const common::ObAddr &addr = GCTX.self_addr();
  if (!addr.ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      LOG_WARN("failed to write string", K(ret));
    }
    port_ = addr.get_port();
  }
  return ret;
}

int ObSqlWorkareaMemoryInfo::fill_row(
  uint64_t tenant_id,
  ObSqlWorkareaCurrentMemoryInfo &memory_info,
  common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < output_column_ids_.count(); ++cell_idx) {
    uint64_t col_id = output_column_ids_.at(cell_idx);
    switch(col_id) {
      case SVR_IP: {
        cells[cell_idx].set_varchar(ipstr_);
        cells[cell_idx].set_collation_type(
          ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case SVR_PORT: {
        cells[cell_idx].set_int(port_);
        break;
      }
      case MAX_WORKAREA_SIZE: {
        cells[cell_idx].set_int(memory_info.get_max_workarea_size());
        break;
      }
      case WORKAREA_HOLD_SIZE: {
        cells[cell_idx].set_int(memory_info.get_workarea_hold_size());
        break;
      }
      case MAX_AUTO_WORKAREA_SIZE: {
        cells[cell_idx].set_int(memory_info.get_max_auto_workarea_size());
        break;
      }
      case MEM_TARGET: {
        cells[cell_idx].set_int(memory_info.get_mem_target());
        break;
      }
      case TOTAL_MEM_USED: {
        cells[cell_idx].set_int(memory_info.get_total_mem_used());
        break;
      }
      case GLOBAL_MEM_BOUND: {
        cells[cell_idx].set_int(memory_info.get_global_bound_size());
        break;
      }
      case DRIFT_SIZE: {
        cells[cell_idx].set_int(memory_info.get_drift_size());
        break;
      }
      case WORKAREA_COUNT: {
        cells[cell_idx].set_int(memory_info.get_workarea_cnt());
        break;
      }
      case MANUAL_CALC_COUNT: {
        cells[cell_idx].set_int(memory_info.get_manual_calc_cnt());
        break;
      }
      case TENAND_ID: {
        cells[cell_idx].set_int(tenant_id);
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

int ObSqlWorkareaMemoryInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    if (OB_FAIL(iter_.init(effective_tenant_id_))) {
      LOG_WARN("failed to init iterator", K(ret));
    } else {
      start_to_read_ = true;
      if (OB_FAIL(get_server_ip_and_port())) {
        LOG_WARN("failed to get server ip and port", K(ret));
      }
    }
  }
  ObSqlWorkareaCurrentMemoryInfo *memory_info = nullptr;
  uint64_t tenant_id = 0;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(iter_.get_next_wa_memory_info(memory_info, tenant_id))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next channel", K(ret));
    }
  } else if (OB_FAIL(fill_row(tenant_id, *memory_info, row))) {
    LOG_WARN("failed to get row from channel info", K(ret));
  }
  return ret;
}
