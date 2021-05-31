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

#include "observer/ob_server_struct.h"
#include "observer/virtual_table/ob_all_virtual_trans_sql_audit.h"
#include "share/rc/ob_context.h"  // WITH_CONTEXT  ObTenantSpaceFetcher
#include "observer/omt/ob_multi_tenant.h"

using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::omt;
using namespace oceanbase::share;

namespace oceanbase {
namespace observer {
ObAllVirtualTransSQLAudit::ObAllVirtualTransSQLAudit() : tenant_id_array_idx_(-1), with_tenant_ctx_(nullptr)
{}

void ObAllVirtualTransSQLAudit::reset()
{
  if (with_tenant_ctx_ != nullptr && allocator_ != nullptr) {
    with_tenant_ctx_->~ObTenantSpaceFetcher();
    allocator_->free(with_tenant_ctx_);
    with_tenant_ctx_ = nullptr;
  }
  ip_buffer_[0] = '\0';
  trans_id_buffer_[0] = '\0';
  trace_id_buffer_[0] = '\0';
  partition_buffer_[0] = '\0';
  tenant_id_array_idx_ = -1;

  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTransSQLAudit::inner_open()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(extract_tenant_ids_())) {
    SERVER_LOG(WARN, "failed to extract tenant ids", K(ret));
  } else if (NULL == allocator_) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Invalid Allocator", K(ret));
  }
  return ret;
}

int ObAllVirtualTransSQLAudit::extract_tenant_ids_()
{
  int ret = OB_SUCCESS;
  tenant_id_array_.reset();
  tenant_id_array_idx_ = -1;
  // get all tenant ids
  TenantIdList id_list(16, NULL, ObModIds::OB_COMMON_ARRAY);
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected null of omt", K(ret));
  } else {
    GCTX.omt_->get_tenant_ids(id_list);
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < id_list.size(); i++) {
      if (OB_FAIL(tenant_id_array_.push_back(id_list.at(i)))) {
        SERVER_LOG(WARN, "failed to push back tenant id", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObAllVirtualTransSQLAudit::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;

  if (!audit_iter_.is_valid()) {
    ++tenant_id_array_idx_;
    if (tenant_id_array_idx_ >= tenant_id_array_.count()) {
      ret = OB_ITER_END;
    } else {
      ObTransAuditRecordMgr* trans_audit_record_mgr = nullptr;
      uint64_t tenant_id = tenant_id_array_.at(tenant_id_array_idx_);
      // inc context ref count
      if (with_tenant_ctx_ != nullptr) {  // free old memory
        with_tenant_ctx_->~ObTenantSpaceFetcher();
        allocator_->free(with_tenant_ctx_);
        with_tenant_ctx_ = nullptr;
      }
      void* buff = nullptr;
      if (nullptr == (buff = allocator_->alloc(sizeof(ObTenantSpaceFetcher)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "failed to allocate memory", K(ret));
      } else {
        with_tenant_ctx_ = new (buff) ObTenantSpaceFetcher(tenant_id);
        if (OB_FAIL(with_tenant_ctx_->get_ret())) {
          SERVER_LOG(WARN, "failed to switch tenant context", K(tenant_id), K(ret));
        } else {
          trans_audit_record_mgr = with_tenant_ctx_->entity().get_tenant()->get<ObTransAuditRecordMgr*>();
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(trans_audit_record_mgr) && OB_FAIL(audit_iter_.init(trans_audit_record_mgr))) {
        SERVER_LOG(WARN, "audit_iter_ init error", K(ret));
      }
    }
  }

  ObTransAuditCommonInfo common_info;
  ObTransAuditStmtInfo stmt_info;
  if (OB_SUCC(ret) && audit_iter_.is_valid()) {
    if (OB_FAIL(audit_iter_.get_next(common_info, stmt_info))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "ObAllVirtualTransSQLAudit iter error", K(ret));
      } else if (tenant_id_array_idx_ >= tenant_id_array_.count()) {
        SERVER_LOG(DEBUG, "ObAllVirtualTransSQLAudit iter end success");
      } else {
        ret = OB_SUCCESS;  // Continue next tenant
      }
    } else if (OB_FAIL(fill_cells_(common_info, stmt_info))) {
      SERVER_LOG(WARN, "ObAllVirtualTransSQLAudit fill cells error", K(ret));
    } else {
      // do nothing
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

int ObAllVirtualTransSQLAudit::fill_cells_(
    const ObTransAuditCommonInfo& common_info, const ObTransAuditStmtInfo& stmt_info)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    common::ObObj& cell = cur_row_.cells_[i];
    switch (col_id) {
      case OB_APP_MIN_COLUMN_ID:
        // tenant_id
        cell.set_int(common_info.tenant_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 1:
        // svr_ip
        (void)common_info.server_addr_.ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
        cell.set_varchar(ip_buffer_);
        cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 2:
        // svr_port
        cell.set_int(common_info.server_addr_.get_port());
        break;
      case OB_APP_MIN_COLUMN_ID + 3:
        // trans_id
        (void)common_info.trans_id_.to_string(trans_id_buffer_, OB_MIN_BUFFER_SIZE);
        cell.set_varchar(trans_id_buffer_);
        cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 4:
        // pkey
        if (common_info.partition_key_.is_valid()) {
          (void)common_info.partition_key_.to_string(partition_buffer_, OB_MIN_BUFFER_SIZE);
          cell.set_varchar(partition_buffer_);
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          cell.set_varchar(ObString("NULL"));
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case OB_APP_MIN_COLUMN_ID + 5:
        // sql_no
        cell.set_int(stmt_info.sql_no_);
        break;
      case OB_APP_MIN_COLUMN_ID + 6:
        // trace_id
        (void)stmt_info.trace_id_.to_string(trace_id_buffer_, OB_MIN_BUFFER_SIZE);
        cell.set_varchar(trace_id_buffer_);
        cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 7:
        // phy_plan_type
        cell.set_int(stmt_info.phy_plan_type_);
        break;
      case OB_APP_MIN_COLUMN_ID + 8:
        // proxy_receive_us
        cell.set_int(stmt_info.proxy_receive_us_);
        break;
      case OB_APP_MIN_COLUMN_ID + 9:
        // server_receive_us
        cell.set_int(stmt_info.server_receive_us_);
        break;
      case OB_APP_MIN_COLUMN_ID + 10:
        // trans_receive_us
        cell.set_int(stmt_info.trans_receive_us_);
        break;
      case OB_APP_MIN_COLUMN_ID + 11:
        // trans_execute_us
        cell.set_int(stmt_info.trans_execute_us_);
        break;
      case OB_APP_MIN_COLUMN_ID + 12:
        // lock_for_read_cnt
        cell.set_int(stmt_info.lock_for_read_retry_count_);
        break;
      case OB_APP_MIN_COLUMN_ID + 13:
        // ctx_addr
        cell.set_int(stmt_info.ctx_addr_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid coloum_id", K(ret), K(col_id));
        break;
    }
  }

  return ret;
}

}  // namespace observer
}  // namespace oceanbase
