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

#include "ob_all_virtual_sql_workarea_active.h"
#include "lib/allocator/ob_mod_define.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_context.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;
using namespace oceanbase::share;


ObSqlWorkareaActiveIterator::ObSqlWorkareaActiveIterator() :
  wa_actives_(), tenant_ids_(), cur_nth_wa_(0),
  cur_nth_tenant_(0)
{}

void ObSqlWorkareaActiveIterator::destroy()
{
  reset();
}

void ObSqlWorkareaActiveIterator::reset()
{
  wa_actives_.reset();
  tenant_ids_.reset();
  cur_nth_wa_ = 0;
  cur_nth_tenant_ = 0;
}

int ObSqlWorkareaActiveIterator::init(const uint64_t effective_tenant_id)
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

int ObSqlWorkareaActiveIterator::get_next_batch_wa_active()
{
  int ret = OB_SUCCESS;
  cur_nth_wa_ = 0;
  wa_actives_.reset();
  if (cur_nth_tenant_ >= tenant_ids_.count()) {
    ret = OB_ITER_END;
  } else {
    uint64_t tenant_id = tenant_ids_.at(cur_nth_tenant_);
    MTL_SWITCH(tenant_id) {
      ObTenantSqlMemoryManager *sql_mem_mgr = nullptr;
      sql_mem_mgr = MTL(ObTenantSqlMemoryManager*);
      if (nullptr != sql_mem_mgr && OB_FAIL(sql_mem_mgr->get_all_active_workarea(wa_actives_))) {
        LOG_WARN("failed to get workarea stat", K(ret));
      }
    }
    ++cur_nth_tenant_;
  }
  return ret;
}

int ObSqlWorkareaActiveIterator::get_next_wa_active(
  ObSqlWorkareaProfileInfo *&wa_active,
  uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  if (0 > cur_nth_wa_ || cur_nth_wa_ > wa_actives_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: current wa exceeds total wa stats", K(ret));
  } else {
    while (OB_SUCC(ret) && cur_nth_wa_ >= wa_actives_.count()) {
      if (OB_FAIL(get_next_batch_wa_active())) {
      }
    }
  }
  if (OB_SUCC(ret)) {
    wa_active = &wa_actives_.at(cur_nth_wa_);
    tenant_id = tenant_ids_.at(cur_nth_tenant_ - 1);
    ++cur_nth_wa_;
  }
  return ret;
}

/////////////////////////////////////////////////////////////////////////////////////
ObSqlWorkareaActive::ObSqlWorkareaActive() :
  ipstr_(), port_(0), iter_()
{}

void ObSqlWorkareaActive::destroy()
{
  ipstr_.reset();
  iter_.destroy();
}

void ObSqlWorkareaActive::reset()
{
  port_ = 0;
  ipstr_.reset();
  iter_.reset();
  start_to_read_ = false;
}

int ObSqlWorkareaActive::get_server_ip_and_port()
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

int ObSqlWorkareaActive::fill_row(
  uint64_t tenant_id,
  ObSqlWorkareaProfileInfo &wa_active,
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
      case PLAN_ID: {
        cells[cell_idx].set_int(wa_active.plan_id_);
        break;
      }
      case SQL_ID: {
        ObString sql_id(wa_active.sql_id_);
        cells[cell_idx].set_varchar(sql_id);
        cells[cell_idx].set_collation_type(
          ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case SQL_EXEC_ID: {
        cells[cell_idx].set_int(wa_active.sql_exec_id_);
        break;
      }
      case OPERATION_TYPE: {
        const char* operator_str = get_phy_op_name(wa_active.profile_.get_operator_type());
        ObString op_type(operator_str);
        cells[cell_idx].set_varchar(op_type);
        cells[cell_idx].set_collation_type(
          ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case OPERATION_ID: {
        cells[cell_idx].set_int(wa_active.profile_.get_operator_id());
        break;
      }
      case SID: {
        cells[cell_idx].set_int(wa_active.session_id_);
        break;
      }
      case ACTIVE_TIME: {
        cells[cell_idx].set_int(
            ObTimeUtility::current_time() - wa_active.profile_.get_active_time());
        break;
      }
      case WORK_AREA_SIZE: {
        cells[cell_idx].set_int(wa_active.profile_.get_cache_size());
        break;
      }
      case EXPECTED_SIZE: {
        cells[cell_idx].set_int(wa_active.profile_.get_expect_size());
        break;
      }
      case ACTUAL_MEM_USED: {
        cells[cell_idx].set_int(wa_active.profile_.get_mem_used());
        break;
      }
      case MAX_MEM_USED: {
        cells[cell_idx].set_int(wa_active.profile_.get_max_mem_used());
        break;
      }
      case NUMBER_PASSES: {
        cells[cell_idx].set_int(wa_active.profile_.get_number_pass());
        break;
      }
      case TEMPSEG_SIZE: {
        cells[cell_idx].set_int(wa_active.profile_.get_max_dumped_size());
        break;
      }
      case TENAND_ID: {
        cells[cell_idx].set_int(tenant_id);
        break;
      }
      case POLICY: {
        ObString exec_str;
        if (wa_active.profile_.get_auto_policy()) {
          exec_str.assign_ptr(EXECUTION_AUTO_POLICY, strlen(EXECUTION_AUTO_POLICY));
        } else {
          exec_str.assign_ptr(EXECUTION_MANUAL_POLICY, strlen(EXECUTION_MANUAL_POLICY));
        }
        cells[cell_idx].set_varchar(exec_str);
        cells[cell_idx].set_collation_type(
          ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case DB_ID: {
        cells[cell_idx].set_int(wa_active.database_id_);
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

int ObSqlWorkareaActive::inner_get_next_row(common::ObNewRow *&row)
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
  ObSqlWorkareaProfileInfo *wa_active = nullptr;
  uint64_t tenant_id = 0;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(iter_.get_next_wa_active(wa_active, tenant_id))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next channel", K(ret));
    }
  } else if (OB_FAIL(fill_row(tenant_id, *wa_active, row))) {
    LOG_WARN("failed to get row from channel info", K(ret));
  }
  return ret;
}
