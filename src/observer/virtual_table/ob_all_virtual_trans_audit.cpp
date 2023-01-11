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
#include "observer/virtual_table/ob_all_virtual_trans_audit.h"
#include "share/rc/ob_context.h"  // WITH_CONTEXT  ObTenantSpaceFetcher
#include "observer/omt/ob_multi_tenant.h"

using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::omt;
using namespace oceanbase::share;

namespace oceanbase {
namespace observer {

ObAllVirtualTransAudit::ObAllVirtualTransAudit() : tenant_id_array_idx_(-1), with_tenant_ctx_(nullptr)
{}

void ObAllVirtualTransAudit::reset()
{
  if (with_tenant_ctx_ != nullptr && allocator_ != nullptr) {
    with_tenant_ctx_->~ObTenantSpaceFetcher();
    allocator_->free(with_tenant_ctx_);
    with_tenant_ctx_ = nullptr;
  }
  ip_buffer_[0] = '\0';
  trans_id_buffer_[0] = '\0';
  proxy_sessid_buffer_[0] = '\0';
  elr_trans_info_buffer_[0] = '\0';
  trace_log_buffer_[0] = '\0';
  trans_param_buffer_[0] = '\0';
  partition_buffer_[0] = '\0';
  tenant_id_array_idx_ = -1;

  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTransAudit::inner_open()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(extract_tenant_ids_())) {
    SERVER_LOG(WARN, "failed to extract tenant ids", K(ret));
  } else if (NULL == allocator_) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Invalid Allocator", K(ret));
  }
  SERVER_LOG(INFO, "trans_audit, tenant_id_array", K_(tenant_id_array));
  return ret;
}

int ObAllVirtualTransAudit::extract_tenant_ids_()
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

int ObAllVirtualTransAudit::inner_get_next_row(ObNewRow*& row)
{
  UNUSED(row);
  return OB_ITER_END;
}

int ObAllVirtualTransAudit::fill_cells_(const ObTransAuditCommonInfo& common_info, const ObTransAuditInfo& trans_info)
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
        // session_id
        cell.set_int(trans_info.session_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 6:
        // proxy_id
        if (trans_info.proxy_session_id_ > 0) {
          ObAddr client_server;
          // parse client info
          (void)get_addr_by_proxy_sessid(trans_info.proxy_session_id_, client_server);
          if (client_server.is_valid()) {
            client_server.to_string(proxy_sessid_buffer_, OB_MIN_BUFFER_SIZE);
            cell.set_varchar(proxy_sessid_buffer_);
            cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cell.set_varchar(ObString("NULL"));
            cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
        } else {
          cell.set_varchar(ObString("NULL"));
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case OB_APP_MIN_COLUMN_ID + 7:
        // trans_type
        cell.set_int(trans_info.trans_type_);
        break;
      case OB_APP_MIN_COLUMN_ID + 8:
        // ctx_create_time
        cell.set_timestamp(trans_info.ctx_create_time_);
        break;
      case OB_APP_MIN_COLUMN_ID + 9:
        // expired_time
        cell.set_timestamp(trans_info.expired_time_);
        break;
      case OB_APP_MIN_COLUMN_ID + 10:
        // trans_param
        if (trans_info.trans_param_.is_valid()) {
          trans_info.trans_param_.to_string(trans_param_buffer_, OB_MAX_BUFFER_SIZE);
          cell.set_varchar(trans_param_buffer_);
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          cell.set_varchar(ObString("NULL"));
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case OB_APP_MIN_COLUMN_ID + 11:
        // total_sql_no
        cell.set_int(trans_info.total_sql_no_);
        break;
      case OB_APP_MIN_COLUMN_ID + 12:
        // refer
        cell.set_int(trans_info.ctx_refer_);
        break;
      case OB_APP_MIN_COLUMN_ID + 13:
        // prev_trans_arr
        if (0 < trans_info.prev_trans_arr_.count()) {
          (void)trans_info.prev_trans_arr_.to_string(elr_trans_info_buffer_, OB_MAX_BUFFER_SIZE);
          cell.set_varchar(elr_trans_info_buffer_);
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          cell.set_varchar(ObString("NULL"));
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case OB_APP_MIN_COLUMN_ID + 14:
        // next_trans_arr
        if (0 < trans_info.next_trans_arr_.count()) {
          (void)trans_info.next_trans_arr_.to_string(elr_trans_info_buffer_, OB_MAX_BUFFER_SIZE);
          cell.set_varchar(elr_trans_info_buffer_);
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          cell.set_varchar(ObString("NULL"));
          cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case OB_APP_MIN_COLUMN_ID + 15:
        // ctx_addr
        cell.set_int(trans_info.ctx_addr_);
        break;
      case OB_APP_MIN_COLUMN_ID + 16:
        // ctx_type
        cell.set_int(trans_info.trans_ctx_type_);
        break;
      case OB_APP_MIN_COLUMN_ID + 17:
        // trace_log
        cell.set_varchar(trace_log_buffer_);
        cell.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 18:
        // status
        cell.set_int(trans_info.status_);
        break;
      case OB_APP_MIN_COLUMN_ID + 19:
        // for_replay
        cell.set_bool(trans_info.for_replay_);
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
