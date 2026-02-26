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

#define USING_LOG_PREFIX SERVER

#include "observer/virtual_table/ob_all_virtual_ccl_status.h"
#include "observer/ob_server_utils.h"
#include "sql/ob_sql_ccl_rule_manager.h"

using namespace oceanbase;
using namespace sql;
using namespace observer;
using namespace common;
namespace oceanbase
{
namespace observer
{

int ObGetAllCCLStatusOp::operator()(common::hash::HashMapPair<ObFormatSQLIDCCLRuleKey, ObCCLRuleConcurrencyValueWrapper*> &entry) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is NULL", K(ret));
  } else {
    ObCCLStatus ccl_status;
    ccl_status.ccl_rule_id_ = entry.second->ccl_rule_id_;
    // ccl_status.format_sqlid_ = entry.second->format_sqlid_;
    ccl_status.cur_concurrency_ = entry.second->cur_concurrency_;
    ccl_status.max_concurrency_ = entry.second->max_concurrency_;
    if (OB_FAIL(ob_write_string(*allocator_, entry.second->format_sqlid_, ccl_status.format_sqlid_))) {
      LOG_WARN("ob write client info str failed", K(ret));
    } else if (OB_FAIL(tmp_ccl_status_.push_back(ccl_status))) {
      SERVER_LOG(WARN, "fail to push_back ccl_status", K(ret), K(ccl_status));
    }
  }

  return ret;
}

int ObAllVirtualCCLStatus::set_svr_addr(common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  svr_addr_ = addr;
  if (!svr_addr_.ip_to_string(svr_ip_buf_, sizeof(svr_ip_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get server ip buffer failed", K(ret), K(svr_addr_));
  }
  return ret;
}

void ObAllVirtualCCLStatus::reset()
{
  cur_idx_ = 0;
  MEMSET(svr_ip_buf_, 0, common::OB_IP_STR_BUFF);
  tmp_ccl_status_.reset();
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualCCLStatus::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    LOG_WARN("execute fail", K(ret));
  }
  return ret;
}

bool ObAllVirtualCCLStatus::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

void ObAllVirtualCCLStatus::release_last_tenant()
{
  cur_idx_ = 0;
  tmp_ccl_status_.reset();
}

int ObAllVirtualCCLStatus::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  ObGetAllCCLStatusOp get_all_op(tmp_ccl_status_);
  start_to_read_ = true;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator_ should not be NULL", K(ret));
  } else if (FALSE_IT(get_all_op.set_allocator(allocator_))) {
  } else if (tmp_ccl_status_.count() == 0) {
    if (OB_FAIL(MTL(ObSQLCCLRuleManager *)->get_rule_level_concurrency_map().foreach_refactored(get_all_op))) {
      LOG_WARN("fail to get ccl status from rule_level_concurrency_map", K(ret));
    } else if (OB_FAIL(MTL(ObSQLCCLRuleManager *)->get_format_sqlid_level_concurrency_map().foreach_refactored(get_all_op))) {
      LOG_WARN("fail to get ccl status from format_sqlid_level_concurrency_map", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (cur_idx_ >= tmp_ccl_status_.count()) {
    ret = OB_ITER_END;
  } else {
    ObCCLStatus &ccl_status = tmp_ccl_status_.at(cur_idx_);
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
      // tenant id
      case TENANT_ID: {
        cells[i].set_int(MTL_ID());
        break;
      }
      // ip
      case SVR_IP: {
        cells[i].set_varchar(ObString::make_string(svr_ip_buf_));
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      // port
      case SVR_PORT: {
        cells[i].set_int(svr_addr_.get_port());
        break;
      }
      // ccl rule id
      case CCL_RULE_ID: {
        cells[i].set_int(ccl_status.ccl_rule_id_);
        break;
      }
      case FORMAT_SQLID: {
        cells[i].set_varchar(ccl_status.format_sqlid_);
        break;
      }
      case CURRENCT_CONCURRENCY: {
        cells[i].set_int(ccl_status.cur_concurrency_);
        break;
      }
      case MAX_CONCURRENCY: {
        cells[i].set_int(ccl_status.max_concurrency_);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(ret), K(i),
                   K(output_column_ids_), K(col_id));
        break;
      }
      }
    }

    if (OB_SUCC(ret)) {
      row = &cur_row_;
      cur_idx_++;
    }
  }
  return ret;
}
} //end namespace observer
} //end namespace oceanbase
