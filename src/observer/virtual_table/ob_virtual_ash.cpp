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
#include "ob_virtual_ash.h"
#include "common/ob_smart_call.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;
using namespace oceanbase::omt;
using namespace oceanbase::share;

ObVirtualASH::ObVirtualASH() :
    ObVirtualTableScannerIterator(),
    addr_(),
    ipstr_(),
    port_(0),
    is_first_get_(true)
{
  server_ip_[0] = '\0';
  trace_id_[0] = '\0';
}

ObVirtualASH::~ObVirtualASH()
{
  reset();
}

void ObVirtualASH::reset()
{
  ObVirtualTableScannerIterator::reset();
  port_ = 0;
  ipstr_.reset();
  is_first_get_ = true;
}

int ObVirtualASH::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_ip(addr_))) {
    SERVER_LOG(WARN, "failed to set server ip addr", K(ret));
  }
  return ret;
}

int ObVirtualASH::set_ip(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  MEMSET(server_ip_, 0, sizeof(server_ip_));
  if (!addr.is_valid()){
    ret = OB_ERR_UNEXPECTED;
  } else if (!addr.ip_to_string(server_ip_, sizeof(server_ip_))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(server_ip_);
    port_ = addr.get_port();
  }
  return ret;
}

int ObVirtualASH::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (is_first_get_) {
    is_first_get_ = false;
    iterator_ = ObActiveSessHistList::get_instance().create_iterator();
  }

  do {
    if (iterator_.has_next()) {
      const ActiveSessionStat &node = iterator_.next();
      if (OB_SYS_TENANT_ID == effective_tenant_id_ || node.tenant_id_ == effective_tenant_id_) {
        if (OB_FAIL(convert_node_to_row(node, row))) {
          LOG_WARN("fail convert row", K(ret));
        }
        break;
      }
    } else {
      ret = OB_ITER_END;
    }
  } while (OB_SUCC(ret));

  return ret;
}

int ObVirtualASH::convert_node_to_row(const ActiveSessionStat &node, ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  }
  for (int64_t cell_idx = 0;
       OB_SUCC(ret) && cell_idx < output_column_ids_.count();
       ++cell_idx) {
    const uint64_t column_id = output_column_ids_.at(cell_idx);
    switch(column_id) {
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
      case SAMPLE_ID: {
        cells[cell_idx].set_int(node.id_);
        break;
      }
      case SAMPLE_TIME: {
        cells[cell_idx].set_timestamp(node.sample_time_);
        break;
      }
      case TENANT_ID: {
        cells[cell_idx].set_int(node.tenant_id_);
        break;
      }
      case USER_ID: {
        cells[cell_idx].set_int(node.user_id_);
        break;
      }
      case SESSION_ID: {
        cells[cell_idx].set_int(node.session_id_);
        break;
      }
      case SESSION_TYPE: {
        cells[cell_idx].set_bool(node.session_type_);
        break;
      }
      case SQL_ID: {
        if ('\0' == node.sql_id_[0]) {
          cells[cell_idx].set_varchar("");
        } else {
          cells[cell_idx].set_varchar(node.sql_id_, static_cast<ObString::obstr_size_t>(OB_MAX_SQL_ID_LENGTH));
        }
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case TRACE_ID: {
        int len = node.trace_id_.to_string(trace_id_, sizeof(trace_id_));
        cells[cell_idx].set_varchar(trace_id_, len);
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case EVENT_NO: {
        cells[cell_idx].set_int(node.event_no_);
        break;
      }
      case WAIT_TIME: {
        cells[cell_idx].set_int(node.wait_time_);
        break;
      }
      case P1: {
        cells[cell_idx].set_int(node.p1_);
        break;
      }
      case P2: {
        cells[cell_idx].set_int(node.p2_);
        break;
      }
      case P3: {
        cells[cell_idx].set_int(node.p3_);
        break;
      }
      case SQL_PLAN_LINE_ID: {
        if (node.plan_line_id_ < 0) {
          cells[cell_idx].set_null();
        } else {
          cells[cell_idx].set_int(node.plan_line_id_);
        }
        break;
      }
      case IN_PARSE: {
        cells[cell_idx].set_bool(node.in_parse_);
        break;
      }
      case IN_PL_PARSE: {
        cells[cell_idx].set_bool(node.in_pl_parse_);
        break;
      }
      case IN_PLAN_CACHE: {
        cells[cell_idx].set_bool(node.in_get_plan_cache_);
        break;
      }
      case IN_SQL_OPTIMIZE: {
        cells[cell_idx].set_bool(node.in_sql_optimize_);
        break;
      }
      case IN_SQL_EXECUTION: {
        cells[cell_idx].set_bool(node.in_sql_execution_);
        break;
      }
      case IN_PX_EXECUTION: {
        cells[cell_idx].set_bool(node.in_px_execution_);
        break;
      }
      case IN_SEQUENCE_LOAD: {
        cells[cell_idx].set_bool(node.in_sequence_load_);
        break;
      }
      case MODULE: {
        cells[cell_idx].set_null(); // impl. later
        break;
      }
      case ACTION: {
        cells[cell_idx].set_null(); // impl. later
        break;
      }
      case CLIENT_ID: {
        cells[cell_idx].set_null(); // impl. later
        break;
      }
      case BACKTRACE: {
#ifndef NDEBUG
        if (node.bt_[0] == '\0') {
          cells[cell_idx].set_varchar("");
        } else {
          cells[cell_idx].set_varchar(node.bt_);
        }
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
#else
        cells[cell_idx].set_null();
#endif
        break;
      }
      case PLAN_ID: {
        cells[cell_idx].set_int(node.plan_id_);
        break;
      }
      case IS_WR_SAMPLE: {
        cells[cell_idx].set_bool(node.is_wr_sample_);
        break;
      }
      case TIME_MODEL: {
        cells[cell_idx].set_uint64(node.time_model_);
        break;
      }
      case IN_COMMITTING: {
        cells[cell_idx].set_bool(node.in_committing_);
        break;
      }
      case IN_STORAGE_READ: {
        cells[cell_idx].set_bool(node.in_storage_read_);
        break;
      }
      case IN_STORAGE_WRITE: {
        cells[cell_idx].set_bool(node.in_storage_write_);
        break;
      }
      case IN_REMOTE_DAS_EXECUTION: {
        cells[cell_idx].set_bool(node.in_das_remote_exec_);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(column_id), K(cell_idx),
                   K_(output_column_ids), K(ret));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

int ObVirtualASHI1::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  do {
    if (iterator_.has_next()) {
      const ActiveSessionStat &node = iterator_.next();
      if (OB_SYS_TENANT_ID == effective_tenant_id_ || node.tenant_id_ == effective_tenant_id_) {
        if (OB_FAIL(convert_node_to_row(node, row))) {
          LOG_WARN("fail convert row", K(ret));
        }
        break;
      }
    } else {
      ret = init_next_query_range();
    }
  } while (OB_SUCC(ret));
  return ret;
}

int ObVirtualASHI1::init_next_query_range()
{
  int ret = OB_SUCCESS;
  if (current_key_range_index_ >= key_ranges_.count()) {
    ret = OB_ITER_END;
  } else {
    ret = OB_SUCCESS;
    const common::ObNewRange & cur_range = key_ranges_.at(current_key_range_index_);
    if (OB_UNLIKELY(cur_range.start_key_.get_obj_cnt() != 1 ||
                    cur_range.end_key_.get_obj_cnt() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ash index error", KR(ret),
               K(cur_range.start_key_.get_obj_cnt()),
               K(cur_range.end_key_.get_obj_cnt()));
    } else {
      int64_t left = cur_range.start_key_.get_obj_ptr()->get_timestamp();
      int64_t right = cur_range.end_key_.get_obj_ptr()->get_timestamp();
      // sample_time's smallest granularity is 1
      if (!cur_range.border_flag_.inclusive_start()) {
        ++left;
      }
      if (!cur_range.border_flag_.inclusive_end()) {
        --right;
      }
      if (cur_range.start_key_.is_min_row()) {
        left = 0;  // minimum sample time is 0
      }
      if (cur_range.end_key_.is_max_row()) {
        right = INT64_MAX;  // maximum sample time is INT64_MAX
      }
      if (OB_UNLIKELY(cur_range.end_key_.is_min_row() || cur_range.start_key_.is_max_row())) {
        left = INT64_MAX;
        right = 0;
      }
      ++current_key_range_index_;
      iterator_ = ObActiveSessHistList::get_instance().create_iterator();
      iterator_.init_with_sample_time_index(left, right);
      LOG_DEBUG("current ash query range", K(key_ranges_), K(left), K(right), K_(iterator));
    }
  }
  return ret;
}