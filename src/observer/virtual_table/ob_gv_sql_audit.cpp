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

#include "observer/virtual_table/ob_gv_sql_audit.h"
#include <stdint.h>
#include "common/rowkey/ob_rowkey.h"
#include "common/ob_smart_call.h"
#include "share/ob_define.h"
#include "observer/ob_server.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "lib/utility/utility.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"

#include <algorithm> // std::sort

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::omt;
using namespace oceanbase::share;
namespace oceanbase {
namespace observer {

ObGvSqlAudit::ObGvSqlAudit() :
    ObVirtualTableScannerIterator(),
    cur_mysql_req_mgr_(nullptr),
    start_id_(INT64_MAX),
    end_id_(INT64_MIN),
    cur_id_(0),
    ref_(),
    addr_(NULL),
    ipstr_(),
    port_(0),
    is_first_get_(true),
    is_use_index_(false),
    tenant_id_array_(),
    tenant_id_array_idx_(-1),
    with_tenant_ctx_(nullptr)
{
}

ObGvSqlAudit::~ObGvSqlAudit() {
  reset();
}

void ObGvSqlAudit::reset()
{
  if (with_tenant_ctx_ != nullptr && allocator_ != nullptr) {
    if (cur_mysql_req_mgr_ != nullptr && ref_.idx_ != -1) {
      cur_mysql_req_mgr_->revert(&ref_);
    }
    with_tenant_ctx_->~ObTenantSpaceFetcher();
    allocator_->free(with_tenant_ctx_);
    with_tenant_ctx_ = nullptr;
  }
  ObVirtualTableScannerIterator::reset();
  is_first_get_ = true;
  is_use_index_ = false;
  cur_id_ = 0;
  tenant_id_array_.reset();
  tenant_id_array_idx_ = -1;
  start_id_ = INT64_MAX;
  end_id_ = INT64_MIN;
  cur_mysql_req_mgr_ = nullptr;
  addr_ = nullptr;
  port_ = 0;
  ipstr_.reset();
}

int ObGvSqlAudit::inner_open()
{
  int ret = OB_SUCCESS;

  // sys tenant show all tenant sql audit
  if (is_sys_tenant(effective_tenant_id_)) {
    if (OB_FAIL(extract_tenant_ids())) {
      SERVER_LOG(WARN, "failed to extract tenant ids", KR(ret), K(effective_tenant_id_));
    }
  } else {
    // user tenant show self tenant sql audit
    if (OB_FAIL(tenant_id_array_.push_back(effective_tenant_id_))) {
      SERVER_LOG(WARN, "failed to push back tenant", KR(ret), K(effective_tenant_id_));
    }
  }

  SERVER_LOG(DEBUG, "tenant ids", K(effective_tenant_id_), K(tenant_id_array_));

  if (OB_SUCC(ret)) {
    if (NULL == allocator_) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "Invalid Allocator", K(ret));
    } else if (OB_FAIL(set_ip(addr_))) {
      SERVER_LOG(WARN, "failed to set server ip addr", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObGvSqlAudit::set_ip(common::ObAddr *addr)
{
  int ret = OB_SUCCESS;
  MEMSET(server_ip_, 0, sizeof(server_ip_));
  if (NULL == addr){
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!addr_->ip_to_string(server_ip_, sizeof(server_ip_))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(server_ip_);
    port_ = addr_->get_port();
  }
  return ret;
}

int ObGvSqlAudit::check_ip_and_port(bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;

  // is_serving_tenant被改成 (svr_ip, svr_port) in (ip1, port1), (ip2, port2), ...
  // 抽出来的query range为[(ip1, port1), (ip1, port1)], [(ip2, port2), (ip2, port2)], ...
  // 需要遍历所有query range，判断本机的ip & port是否落在某一个query range中
  if (key_ranges_.count() >= 1) {
    is_valid = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < key_ranges_.count(); i++) {
      ObNewRange &req_id_range = key_ranges_.at(i);
      if (OB_UNLIKELY(req_id_range.get_start_key().get_obj_cnt() != 4
                      || req_id_range.get_end_key().get_obj_cnt() != 4)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected  # of rowkey columns",
                   K(ret),
                   "size of start key", req_id_range.get_start_key().get_obj_cnt(),
                   "size of end key", req_id_range.get_end_key().get_obj_cnt());
      } else {
        ObObj ip_obj;
        ObObj ip_low = (req_id_range.get_start_key().get_obj_ptr()[PRI_KEY_IP_IDX]);
        ObObj ip_high = (req_id_range.get_end_key().get_obj_ptr()[PRI_KEY_IP_IDX]);
        ip_obj.set_varchar(ipstr_);
        ip_obj.set_collation_type(ObCharset::get_system_collation());
        if (ip_obj.compare(ip_low) >= 0 && ip_obj.compare(ip_high) <= 0) {
          ObObj port_obj;
          port_obj.set_int32(port_);
          ObObj port_low = (req_id_range.get_start_key().get_obj_ptr()[PRI_KEY_PORT_IDX]);
          ObObj port_high = (req_id_range.get_end_key().get_obj_ptr()[PRI_KEY_PORT_IDX]);
          if (port_obj.compare(port_low) >= 0 && port_obj.compare(port_high) <= 0) {
            is_valid = true;
          }
        } else {
          // do nothing
        }
      }
    }
  }
  SERVER_LOG(DEBUG, "check ip and port", K(key_ranges_), K(is_valid), K(ipstr_), K(port_));

  return ret;
}
int ObGvSqlAudit::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "invalid argument", KP(allocator_), K(ret));
  } else if (is_first_get_) {
    bool is_valid = true;
    // init inner iterator varaibales
    tenant_id_array_idx_ = is_reverse_scan() ? tenant_id_array_.count() : -1;
    cur_mysql_req_mgr_ = nullptr;

    // if use primary key scan, we need to perform check on ip and port
    if (!is_index_scan()) {
      if (OB_FAIL(check_ip_and_port(is_valid))) {
        SERVER_LOG(WARN, "check ip and port failed", K(ret));
      } else if (!is_valid) {
        ret = OB_ITER_END;;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (nullptr == cur_mysql_req_mgr_ || (cur_id_ < start_id_ ||
                                          cur_id_ >= end_id_)) {
      obmysql::ObMySQLRequestManager *prev_req_mgr = cur_mysql_req_mgr_;
      cur_mysql_req_mgr_ = nullptr;
      while (nullptr == cur_mysql_req_mgr_ && OB_SUCC(ret)) {
        if (is_reverse_scan())  {
          tenant_id_array_idx_ -= 1;
        } else {
          tenant_id_array_idx_ += 1;
        }
        if (tenant_id_array_idx_ >= tenant_id_array_.count() ||
            tenant_id_array_idx_ < 0) {
          ret = OB_ITER_END;
          break;
        } else {
          uint64_t t_id = tenant_id_array_.at(tenant_id_array_idx_);
          // inc ref count by 1
          if (with_tenant_ctx_ != nullptr) { // free old memory
            // before freeing tenant ctx, we must release ref_ if possible
            if (nullptr != prev_req_mgr && ref_.idx_ != -1) {
              prev_req_mgr->revert(&ref_);
            }
            with_tenant_ctx_->~ObTenantSpaceFetcher();
            allocator_->free(with_tenant_ctx_);
            with_tenant_ctx_ = nullptr;
          }
          void *buff = nullptr;
          if (nullptr == (buff = allocator_->alloc(sizeof(ObTenantSpaceFetcher)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SERVER_LOG(WARN, "failed to allocate memory", K(ret));
          } else {
            with_tenant_ctx_ = new(buff) ObTenantSpaceFetcher(t_id);
            if (OB_FAIL(with_tenant_ctx_->get_ret())) {
              // 如果指定tenant id查询, 且当前机器没有该租户资源时, 获取
              // tenant space会报OB_TENANT_NOT_IN_SERVER, 此时需要忽略该报
              // 错, 返回该租户的sql audit记录为空
              if (OB_TENANT_NOT_IN_SERVER == ret) {
                ret = OB_SUCCESS;
                continue;
              } else {
                SERVER_LOG(WARN, "failed to switch tenant context", K(t_id), K(ret));
              }
            } else {
              cur_mysql_req_mgr_ = with_tenant_ctx_->entity().get_tenant()->get<ObMySQLRequestManager *>();
            }
          }

          if (nullptr == cur_mysql_req_mgr_) {
            SERVER_LOG(DEBUG, "req manager doest not exist", K(t_id));
            continue;
          } else if (OB_SUCC(ret)) {
            start_id_ = INT64_MIN;
            end_id_ = INT64_MAX;
            bool is_req_valid = true;
            if (OB_FAIL(extract_request_ids(t_id, start_id_, end_id_, is_req_valid))) {
              SERVER_LOG(WARN, "failed to extract request ids", K(ret));
            } else if (!is_req_valid) {
              SERVER_LOG(DEBUG, "invalid query range for sql audit", K(t_id), K(key_ranges_));
              ret = OB_ITER_END;
            } else {
              int64_t start_idx = cur_mysql_req_mgr_->get_start_idx();
              int64_t end_idx = cur_mysql_req_mgr_->get_end_idx();
              start_id_ = MAX(start_id_, start_idx);
              end_id_ = MIN(end_id_, end_idx);
              if (start_id_ >= end_id_) {
                SERVER_LOG(DEBUG, "cur_mysql_req_mgr_ iter end", K(start_id_), K(end_id_), K(t_id));
                prev_req_mgr = cur_mysql_req_mgr_;
                cur_mysql_req_mgr_ = nullptr;
              } else if (is_reverse_scan()) {
                cur_id_ = end_id_ - 1;
              } else {
                cur_id_ = start_id_;
              }
              SERVER_LOG(DEBUG, "start to get rows from gv_sql_audit",
                         K(start_id_), K(end_id_), K(cur_id_), K(t_id),
                         K(start_idx), K(end_idx));
            }
          }
        }
      }
      if (OB_ITER_END == ret) {
        // release last tenant's ctx
        if (with_tenant_ctx_ != nullptr) {
          if (prev_req_mgr != nullptr && ref_.idx_ != -1) {
            prev_req_mgr->revert(&ref_);
          }
          with_tenant_ctx_->~ObTenantSpaceFetcher();
          allocator_->free(with_tenant_ctx_);
          with_tenant_ctx_ = nullptr;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    void *rec = NULL;
    if (ref_.idx_ != -1) {
      cur_mysql_req_mgr_->revert(&ref_);
    }
    do {
      ref_.reset();
      if (OB_ENTRY_NOT_EXIST == (ret = cur_mysql_req_mgr_->get(cur_id_, rec, &ref_))) {
        if (is_reverse_scan()) {
          cur_id_ -= 1;
        } else {
          cur_id_ += 1;
        }
      }
    } while (OB_ENTRY_NOT_EXIST == ret && cur_id_ < end_id_ && cur_id_ >= start_id_);

    if (OB_SUCC(ret)) {
      if (NULL != rec) {
        ObMySQLRequestRecord *record = static_cast<ObMySQLRequestRecord*> (rec);

        if (OB_FAIL(fill_cells(*record))) {
          SERVER_LOG(WARN, "failed to fill cells", K(ret));
        } else {
          //finish fetch one row
          row = &cur_row_;
          SERVER_LOG(DEBUG, "request_info_table get next row succ", K(cur_id_));
        }

      } else {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected null rec",
                   K(rec), K(cur_id_), K(tenant_id_array_idx_), K(tenant_id_array_), K(ret));
      }
    }

    is_first_get_ = false;

    // move to next slot
    if (OB_SUCC(ret)) {
      if (!is_reverse_scan()) {
        // forwards
        cur_id_++;
      } else {
        // backwards
        cur_id_--;
      }
    }
    if (OB_ENTRY_NOT_EXIST == ret) {
      // may be all the sql audit is flushed, call inner_get_next_row recursively
      ret = SMART_CALL(inner_get_next_row(row));
    }
  }

  return ret;
}

int ObGvSqlAudit::extract_tenant_ids()
{
  int ret = OB_SUCCESS;
  tenant_id_array_.reset();
  tenant_id_array_idx_ = -1;
  if (!is_index_scan()) {
    // get all tenant ids
    TenantIdList id_list(16, NULL, ObNewModIds::OB_COMMON_ARRAY);

    ObRowkey start_key, end_key;
    bool is_full_scan = false;
    bool is_always_false = false;
    for (int64_t i = 0;
         OB_SUCC(ret) && !is_full_scan && !is_always_false && i < key_ranges_.count();
         i++) {
      start_key.reset();
      end_key.reset();
      start_key = key_ranges_.at(i).start_key_;
      end_key = key_ranges_.at(i).end_key_;

      if (!(start_key.get_obj_cnt() > 0)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "assert start_key.get_obj_cnt() > 0", K(ret));
      } else if (!(start_key.get_obj_cnt() == end_key.get_obj_cnt())
                 || start_key.get_obj_cnt() != 4) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "assert start_key.get_obj_cnt() == end_key.get_obj_cnt()", K(ret));
      }
      const ObObj *start_key_obj_ptr = nullptr;
      const ObObj *end_key_obj_ptr = nullptr;

      if (OB_SUCC(ret)) {
        start_key_obj_ptr = start_key.get_obj_ptr();
        end_key_obj_ptr = end_key.get_obj_ptr();
        if (OB_ISNULL(start_key_obj_ptr) || OB_ISNULL(end_key_obj_ptr)) {
          ret = OB_INVALID_ARGUMENT;
          SERVER_LOG(WARN, "invalid arguments", K(start_key_obj_ptr), K(end_key_obj_ptr));
        } else if (start_key_obj_ptr[PRI_KEY_TENANT_ID_IDX].is_min_value()
                   && end_key_obj_ptr[PRI_KEY_TENANT_ID_IDX].is_max_value()) {
          is_full_scan = true;
        } else if (start_key_obj_ptr[PRI_KEY_TENANT_ID_IDX].is_max_value() &&
                   end_key_obj_ptr[PRI_KEY_TENANT_ID_IDX].is_min_value()) {
          is_always_false = true;
          SERVER_LOG(DEBUG, "always false for tenant range", K(ret));
        } else if (!(start_key_obj_ptr[PRI_KEY_TENANT_ID_IDX].is_min_value()
                     && end_key_obj_ptr[PRI_KEY_TENANT_ID_IDX].is_max_value())
                   && start_key_obj_ptr[PRI_KEY_TENANT_ID_IDX] != end_key_obj_ptr[PRI_KEY_TENANT_ID_IDX]) {
          ret = OB_NOT_IMPLEMENT;
          SERVER_LOG(WARN, "tenant id only supports exact value", K(ret));
        } else if (start_key_obj_ptr[PRI_KEY_TENANT_ID_IDX] == end_key_obj_ptr[PRI_KEY_TENANT_ID_IDX]) {
          if (ObIntType != start_key_obj_ptr[PRI_KEY_TENANT_ID_IDX].get_type()
              || (start_key_obj_ptr[PRI_KEY_TENANT_ID_IDX].get_type()
                  != end_key_obj_ptr[PRI_KEY_TENANT_ID_IDX].get_type())) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "expect tenant id type to be int",
                       K(start_key_obj_ptr[PRI_KEY_TENANT_ID_IDX].get_type()),
                       K(end_key_obj_ptr[PRI_KEY_TENANT_ID_IDX].get_type()));
          } else {
            int64_t tenant_id = start_key_obj_ptr[PRI_KEY_TENANT_ID_IDX].get_int();
            if (tenant_id < 0) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "assert tenant_id >= 0", K(ret));
            } else if (OB_FAIL(add_var_to_array_no_dup(tenant_id_array_, static_cast<uint64_t>(tenant_id)))) {
              SERVER_LOG(WARN, "failed to add tenant_id to array no duplicate", K(ret));
            } else {
              // do nothing
            }
          }
        }
      }
    } // for end
    if (!is_full_scan) {
      // do nothing
    } else if (OB_ISNULL(GCTX.omt_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected null of omt", K(ret));
    } else {
      GCTX.omt_->get_tenant_ids(id_list);
      tenant_id_array_.reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < id_list.size(); i++) {
        if (OB_FAIL(tenant_id_array_.push_back(id_list.at(i)))) {
          SERVER_LOG(WARN, "failed to push back tenant id", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_always_false) {
        tenant_id_array_.reset();
      } else {
        std::sort(tenant_id_array_.begin(), tenant_id_array_.end());
        SERVER_LOG(DEBUG, "get tenant ids from req mgr map", K(tenant_id_array_));
      }
    }
  } else {
    // index scan
    ObRowkey start_key;
    ObRowkey end_key;
    bool is_always_false = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_always_false && i < key_ranges_.count(); i++) {
      int64_t tenant_id = -1;
      start_key.reset();
      end_key.reset();
      start_key = key_ranges_.at(i).start_key_;
      end_key = key_ranges_.at(i).end_key_;

      if (!(start_key.get_obj_cnt() > 0)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "asserr start_key.get_obj_cnt() > 0", K(ret));
      } else if (!(start_key.get_obj_cnt() == end_key.get_obj_cnt()) ||
                 start_key.get_obj_cnt() != 4) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "assert start_key.get_obj_cnt() == end_key.get_obj_cnt()", K(ret));
      }
      const ObObj *start_key_obj_ptr = nullptr;
      const ObObj *end_key_obj_ptr = nullptr;
      if (OB_SUCC(ret)) {
        start_key_obj_ptr = start_key.get_obj_ptr();
        end_key_obj_ptr = end_key.get_obj_ptr();
        if (OB_ISNULL(start_key_obj_ptr) || OB_ISNULL(end_key_obj_ptr)) {
          ret = OB_INVALID_ARGUMENT;
          SERVER_LOG(WARN, "invalid arguments", K(start_key_obj_ptr), K(end_key_obj_ptr));
        } else if (start_key_obj_ptr[IDX_KEY_TENANT_ID_IDX].is_max_value() &&
                   end_key_obj_ptr[IDX_KEY_TENANT_ID_IDX].is_min_value()) {
          is_always_false = true;
          SERVER_LOG(DEBUG, "always false for tenant range");
        } else if (!(start_key_obj_ptr[IDX_KEY_TENANT_ID_IDX].is_min_value() &&
                     end_key_obj_ptr[IDX_KEY_TENANT_ID_IDX].is_max_value()) &&
                   start_key_obj_ptr[IDX_KEY_TENANT_ID_IDX] != end_key_obj_ptr[IDX_KEY_TENANT_ID_IDX]) {
          ret = OB_NOT_IMPLEMENT;
          SERVER_LOG(WARN, "tenant id only supports exact value", K(ret));
        } else if (start_key_obj_ptr[IDX_KEY_TENANT_ID_IDX] ==
                   end_key_obj_ptr[IDX_KEY_TENANT_ID_IDX]) {
          if (ObIntType != start_key_obj_ptr[IDX_KEY_TENANT_ID_IDX].get_type() ||
              start_key_obj_ptr[IDX_KEY_TENANT_ID_IDX].get_type() !=
              end_key_obj_ptr[IDX_KEY_TENANT_ID_IDX].get_type()) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "expect tenant id type to be int",
                       K(start_key_obj_ptr[IDX_KEY_TENANT_ID_IDX]), K(end_key_obj_ptr[IDX_KEY_TENANT_ID_IDX]));
          } else {
            tenant_id = start_key_obj_ptr[IDX_KEY_TENANT_ID_IDX].get_int();
            if (tenant_id < 0) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "assert tenant_id >= 0", K(ret));
            } else if (OB_FAIL(add_var_to_array_no_dup(tenant_id_array_,
                                                       static_cast<uint64_t>(tenant_id)))) {
              SERVER_LOG(WARN, "failed to add tenant_id to array no duplicate", K(ret));
            } else {
              // do nothing
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_always_false) {
        tenant_id_array_.reset();
      } else {
        std::sort(tenant_id_array_.begin(), tenant_id_array_.end());
        SERVER_LOG(DEBUG, "get tenant ids from req mgr map", K(tenant_id_array_));
      }
    }
  }
  if (OB_FAIL(ret)) {
    tenant_id_array_.reset();
  }
  return ret;
}

bool ObGvSqlAudit::is_perf_event_dep_field(uint64_t col_id) {
  bool is_contain = false;
  switch (col_id) {
    case EVENT:
    case P1TEXT:
    case P1:
    case P2TEXT:
    case P2:
    case P3TEXT:
    case P3:
    case LEVEL:
    case WAIT_CLASS_ID:
    case WAIT_CLASS_NO:
    case WAIT_CLASS:
    case STATE:
    case WAIT_TIME_MICRO:
    case TOTAL_WAIT_TIME:
    case TOTAL_WAIT_COUNT:
    case RPC_COUNT:
    case APPLICATION_WAIT_TIME:
    case CONCURRENCY_WAIT_TIME:
    case USER_IO_WAIT_TIME:
    case SCHEDULE_TIME:
    case ROW_CACHE_HIT:
    case FUSE_ROW_CACHE_HIT:
    case BLOOM_FILTER_NOT_HIT:
    case BLOCK_CACHE_HIT:
    case DISK_READS:
    case MEMSTORE_READ_ROW_COUNT:
    case SSSTORE_READ_ROW_COUNT:
    case DATA_BLOCK_READ_CNT:
    case DATA_BLOCK_CACHE_HIT:
    case INDEX_BLOCK_READ_CNT:
    case INDEX_BLOCK_CACHE_HIT:
    case BLOCKSCAN_BLOCK_CNT:
    case BLOCKSCAN_ROW_CNT:
    case PUSHDOWN_STORAGE_FILTER_ROW_CNT: {
      is_contain = true;
      break;
    }
    default: {
      is_contain = false;
      break;
    }
  }
  return is_contain;
}


int ObGvSqlAudit::fill_cells(obmysql::ObMySQLRequestRecord &record)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  const bool is_perf_event_closed = record.data_.is_perf_event_closed_;

  if (OB_ISNULL(cells)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(cells));
  } else {
    for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; cell_idx++) {
      uint64_t col_id = output_column_ids_.at(cell_idx);
      if (is_perf_event_closed && is_perf_event_dep_field(col_id)) {
        cells[cell_idx].set_null();
      } else {
        switch(col_id) {
          //server ip
        case SERVER_IP: {
          cells[cell_idx].set_varchar(ipstr_); //ipstr_ and port_ were set in set_ip func call
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        } break;
          //server port
        case SERVER_PORT: {
          cells[cell_idx].set_int(port_);
        } break;
          //request_id
        case REQUEST_ID: {
          cells[cell_idx].set_int(record.data_.request_id_);
        } break;
          //sql_exec_id
        case SQL_EXEC_ID: {
          cells[cell_idx].set_int(record.data_.execution_id_);
        } break;
        case SESSION_ID: {
          cells[cell_idx].set_uint64(record.data_.session_id_);
        } break;
        case PROXY_SESSION_ID: {
          cells[cell_idx].set_uint64(record.data_.proxy_session_id_);
        } break;
        case TRACE_ID: {
          int len = record.data_.trace_id_.to_string(trace_id_, sizeof(trace_id_));
          cells[cell_idx].set_varchar(trace_id_, len);
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        } break;
          //client ip
        case CLIENT_IP: {
          MEMSET(client_ip_, 0, sizeof(client_ip_));
          const ObAddr &myaddr = record.data_.client_addr_;
          if (OB_UNLIKELY(!myaddr.ip_to_string(client_ip_, sizeof(client_ip_)))) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "ip to string failed", K(myaddr), K(ret));
          } else {
            cells[cell_idx].set_varchar(client_ip_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
          }
        } break;
          //client port
        case CLIENT_PORT: {
          cells[cell_idx].set_int(record.data_.client_addr_.get_port());
        } break;
        case USER_CLIENT_IP: {
          MEMSET(user_client_ip_, 0, sizeof(user_client_ip_));
          const ObAddr &myaddr = record.data_.user_client_addr_;
          if (OB_UNLIKELY(!myaddr.ip_to_string(user_client_ip_, sizeof(user_client_ip_)))) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "ip to string failed", K(myaddr), K(ret));
          } else {
            cells[cell_idx].set_varchar(user_client_ip_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
          }
          break;
        }
        case TENANT_ID: {
          cells[cell_idx].set_int(record.data_.tenant_id_);
        } break;
        case TENANT_NAME: {
          int64_t len = min(record.data_.tenant_name_len_, OB_MAX_TENANT_NAME_LENGTH);
          cells[cell_idx].set_varchar(record.data_.tenant_name_,
                                      static_cast<ObString::obstr_size_t>(len));
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        } break;
        case EFFECTIVE_TENANT_ID: {
          cells[cell_idx].set_int(record.data_.effective_tenant_id_);
        } break;
        case USER_ID: {
          cells[cell_idx].set_int(record.data_.user_id_);
        } break;
        case USER_NAME: {
          int64_t len = min(record.data_.user_name_len_, OB_MAX_USER_NAME_LENGTH);
          cells[cell_idx].set_varchar(record.data_.user_name_,
                                      static_cast<ObString::obstr_size_t>(len));
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        } break;
        case USER_GROUP: {
          cells[cell_idx].set_int(record.data_.user_group_);
          break;
        }
        case DB_ID: {
          cells[cell_idx].set_uint64(record.data_.db_id_);
        } break;
        case DB_NAME: {
          int64_t len = min(record.data_.db_name_len_, OB_MAX_DATABASE_NAME_LENGTH);
          cells[cell_idx].set_varchar(record.data_.db_name_,
                                      static_cast<ObString::obstr_size_t>(len));
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        } break;
          //sql_id
        case SQL_ID: {
          if (OB_MAX_SQL_ID_LENGTH == strlen(record.data_.sql_id_)) {
            cells[cell_idx].set_varchar(record.data_.sql_id_,
                                        static_cast<ObString::obstr_size_t>(OB_MAX_SQL_ID_LENGTH));
          } else {
            cells[cell_idx].set_varchar("");
          }
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        } break;
        case QUERY_SQL: {
          ObCollationType src_cs_type = ObCharset::is_valid_collation(record.data_.sql_cs_type_) ?
                record.data_.sql_cs_type_ : ObCharset::get_system_collation();
          ObString src_string(static_cast<int32_t>(record.data_.sql_len_), record.data_.sql_);
          ObString dst_string;
          if (OB_FAIL(ObCharset::charset_convert(row_calc_buf_,
                                                        src_string,
                                                        src_cs_type,
                                                        ObCharset::get_system_collation(),
                                                        dst_string,
                                                        ObCharset::REPLACE_UNKNOWN_CHARACTER))) {
            SERVER_LOG(WARN, "fail to convert sql string", K(ret));
          } else {
            cells[cell_idx].set_lob_value(ObLongTextType, dst_string.ptr(),
                                          min(dst_string.length(), OB_MAX_PACKET_LENGTH));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
          }
        } break;
        case PLAN_ID: {
          cells[cell_idx].set_int(record.data_.plan_id_);
        } break;
        case AFFECTED_ROWS: {
          cells[cell_idx].set_int(record.data_.affected_rows_);
        } break;
        case RETURN_ROWS: {
          cells[cell_idx].set_int(record.data_.return_rows_);
        } break;
        case PARTITION_CNT: {
          cells[cell_idx].set_int(record.data_.partition_cnt_);
        } break;
        case RET_CODE: {
          cells[cell_idx].set_int(record.data_.status_);
        } break;
        case QC_ID: {
          cells[cell_idx].set_uint64(record.data_.qc_id_);
        } break;
        case DFO_ID: {
          cells[cell_idx].set_int(record.data_.dfo_id_);
        } break;
        case SQC_ID: {
          cells[cell_idx].set_int(record.data_.sqc_id_);
        } break;
        case WORKER_ID: {
          cells[cell_idx].set_int(record.data_.worker_id_);
        } break;
          // max wait event related
        case EVENT: {
          int64_t event_no = record.data_.exec_record_.max_wait_event_.event_no_;
          if (event_no >= 0 && event_no < WAIT_EVENTS_TOTAL) {
            cells[cell_idx].set_varchar(OB_WAIT_EVENTS[event_no].event_name_);
          } else {
            cells[cell_idx].set_varchar("");
          }
          cells[cell_idx].set_default_collation_type();
          break;
        }
        case P1TEXT: {
          int64_t event_no = record.data_.exec_record_.max_wait_event_.event_no_;
          if (event_no >= 0 && event_no < WAIT_EVENTS_TOTAL) {
            cells[cell_idx].set_varchar(OB_WAIT_EVENTS[event_no].param1_);
          } else {
            cells[cell_idx].set_varchar("");
          }
          cells[cell_idx].set_default_collation_type();
          break;
        }
        case P1: {
          cells[cell_idx].set_uint64(record.data_.exec_record_.max_wait_event_.p1_);
          break;
        }
        case P2TEXT: {
          int64_t event_no = record.data_.exec_record_.max_wait_event_.event_no_;
          if (event_no >= 0 && event_no < WAIT_EVENTS_TOTAL) {
            cells[cell_idx].set_varchar(OB_WAIT_EVENTS[event_no].param2_);
          } else {
            cells[cell_idx].set_varchar("");
          }
          cells[cell_idx].set_default_collation_type();
          break;
        }
        case P2: {
          cells[cell_idx].set_uint64(record.data_.exec_record_.max_wait_event_.p2_);
          break;
        }
        case P3TEXT: {
          int64_t event_no = record.data_.exec_record_.max_wait_event_.event_no_;
          if (event_no >= 0 && event_no < WAIT_EVENTS_TOTAL) {
            cells[cell_idx].set_varchar(OB_WAIT_EVENTS[event_no].param3_);
          } else {
            cells[cell_idx].set_varchar("");
          }
          cells[cell_idx].set_default_collation_type();
          break;
        }
        case P3: {
          cells[cell_idx].set_uint64(record.data_.exec_record_.max_wait_event_.p3_);
          break;
        }
        case LEVEL: {
          cells[cell_idx].set_int(record.data_.exec_record_.max_wait_event_.level_);
          break;
        }
        case WAIT_CLASS_ID: {
          int64_t event_no = record.data_.exec_record_.max_wait_event_.event_no_;
          if (event_no >= 0 && event_no < WAIT_EVENTS_TOTAL) {
            cells[cell_idx].set_int(EVENT_NO_TO_CLASS_ID(event_no));
          } else {
            cells[cell_idx].set_int(common::OB_INVALID_ID);
          }
          break;
        }
        case WAIT_CLASS_NO: {
          int64_t event_no = record.data_.exec_record_.max_wait_event_.event_no_;
          if (event_no >= 0 && event_no < WAIT_EVENTS_TOTAL) {
            cells[cell_idx].set_int(OB_WAIT_EVENTS[event_no].wait_class_);
          } else {
            cells[cell_idx].set_int(common::OB_INVALID_ID);
          }
          break;
        }
        case WAIT_CLASS: {
          int64_t event_no = record.data_.exec_record_.max_wait_event_.event_no_;
          if (event_no >= 0 && event_no < WAIT_EVENTS_TOTAL) {
            cells[cell_idx].set_varchar(EVENT_NO_TO_CLASS(event_no));
          } else {
            cells[cell_idx].set_varchar("");
          }
          cells[cell_idx].set_default_collation_type();
          break;
        }
        case STATE: {
          if (record.data_.exec_record_.max_wait_event_.wait_time_ == 0) {
            cells[cell_idx].set_varchar("MAX_WAIT TIME ZERO");
            cells[cell_idx].set_default_collation_type();
          } else if (0 < record.data_.exec_record_.max_wait_event_.wait_time_
                    && record.data_.exec_record_.max_wait_event_.wait_time_ < 10000) {
            cells[cell_idx].set_varchar("WAITED SHORT TIME");
            cells[cell_idx].set_default_collation_type();
          } else if (record.data_.exec_record_.max_wait_event_.wait_time_ >= 10000) {
            cells[cell_idx].set_varchar("WAITED KNOWN TIME");
            cells[cell_idx].set_default_collation_type();
          } else {
            cells[cell_idx].set_varchar("UNKONEW WAIT");
            cells[cell_idx].set_default_collation_type();
          }
          break;
        }
        case WAIT_TIME_MICRO: {
          cells[cell_idx].set_int(record.data_.exec_record_.max_wait_event_.wait_time_);
          break;
        }
        case TOTAL_WAIT_TIME: {
          cells[cell_idx].set_int(record.data_.exec_record_.wait_time_);
          break;
        }
        case TOTAL_WAIT_COUNT: {
          cells[cell_idx].set_int(record.data_.exec_record_.wait_count_);
          break;
        }
        case RPC_COUNT: {
          cells[cell_idx].set_int(record.data_.exec_record_.rpc_packet_out_);
          break;
        }
        case PLAN_TYPE: {
          cells[cell_idx].set_int(record.data_.plan_type_);
          break;
        }
          //is_executor_rpc
        case IS_EXECUTOR_RPC: {
          cells[cell_idx].set_bool(record.data_.is_executor_rpc_);
          break;
        }
          // is_inner_sql
        case IS_INNER_SQL: {
          cells[cell_idx].set_bool(record.data_.is_inner_sql_);
        } break;
        case IS_HIT_PLAN: {
          cells[cell_idx].set_bool(record.data_.is_hit_plan_cache_);
        } break;
          //request timestamp
        case REQUEST_TIMESTAMP: {
          cells[cell_idx].set_int(record.data_.exec_timestamp_.receive_ts_);
        } break;
          //elapsetime
        case ELAPSED_TIME: {
          cells[cell_idx].set_int(record.data_.exec_timestamp_.elapsed_t_);
        } break;
        case NET_TIME: {
          cells[cell_idx].set_int(record.data_.exec_timestamp_.net_t_);
        } break;
        case NET_WAIT_TIME: {
          cells[cell_idx].set_int(record.data_.exec_timestamp_.net_wait_t_);
        } break;
        case QUEUE_TIME: {
          cells[cell_idx].set_int(record.data_.exec_timestamp_.queue_t_);
        } break;
        case DECODE_TIME: {
          cells[cell_idx].set_int(record.data_.exec_timestamp_.decode_t_);
        } break;
        case GET_PLAN_TIME: {
          cells[cell_idx].set_int(record.data_.exec_timestamp_.get_plan_t_);
        } break;
        case EXECUTE_TIME: {
          cells[cell_idx].set_int(record.data_.exec_timestamp_.executor_t_);
        } break;
        case APPLICATION_WAIT_TIME: {
          cells[cell_idx].set_uint64(record.data_.exec_record_.application_time_);
        } break;
        case CONCURRENCY_WAIT_TIME: {
          cells[cell_idx].set_uint64(record.data_.exec_record_.concurrency_time_);
        } break;
        case USER_IO_WAIT_TIME: {
          cells[cell_idx].set_uint64(record.data_.exec_record_.user_io_time_);
        } break;
        case SCHEDULE_TIME: {
          cells[cell_idx].set_uint64(0);
        } break;
        case ROW_CACHE_HIT: {
          cells[cell_idx].set_int(record.data_.exec_record_.row_cache_hit_);
        } break;
        case FUSE_ROW_CACHE_HIT: {
          cells[cell_idx].set_int(record.data_.exec_record_.fuse_row_cache_hit_);
        } break;
        case BLOOM_FILTER_NOT_HIT: {
          cells[cell_idx].set_int(record.data_.exec_record_.bloom_filter_filts_);
        } break;
        case BLOCK_CACHE_HIT: {
          cells[cell_idx].set_int(record.data_.exec_record_.block_cache_hit_);
        } break;
        case DISK_READS: {
          cells[cell_idx].set_int(record.data_.exec_record_.io_read_count_);
        } break;
        case RETRY_CNT: {
          cells[cell_idx].set_int(record.data_.try_cnt_ - 1);
        } break;
        case TABLE_SCAN: {
          cells[cell_idx].set_bool(record.data_.table_scan_);
        } break;
        case CONSISTENCY_LEVEL: {
          cells[cell_idx].set_int(static_cast<int64_t>(record.data_.consistency_level_));
        } break;
        case MEMSTORE_READ_ROW_COUNT: {
          cells[cell_idx].set_int(record.data_.exec_record_.memstore_read_row_count_);
        } break;
        case SSSTORE_READ_ROW_COUNT: {
          cells[cell_idx].set_int(record.data_.exec_record_.ssstore_read_row_count_);
        } break;
        case DATA_BLOCK_READ_CNT: {
          cells[cell_idx].set_int(record.data_.exec_record_.data_block_read_cnt_);
        } break;
        case DATA_BLOCK_CACHE_HIT: {
          cells[cell_idx].set_int(record.data_.exec_record_.data_block_cache_hit_);
        } break;
        case INDEX_BLOCK_READ_CNT: {
          cells[cell_idx].set_int(record.data_.exec_record_.index_block_read_cnt_);
        } break;
        case INDEX_BLOCK_CACHE_HIT: {
          cells[cell_idx].set_int(record.data_.exec_record_.index_block_cache_hit_);
        } break;
        case BLOCKSCAN_BLOCK_CNT: {
          cells[cell_idx].set_int(record.data_.exec_record_.blockscan_block_cnt_);
        } break;
        case BLOCKSCAN_ROW_CNT: {
          cells[cell_idx].set_int(record.data_.exec_record_.blockscan_row_cnt_);
        } break;
        case PUSHDOWN_STORAGE_FILTER_ROW_CNT: {
          cells[cell_idx].set_int(record.data_.exec_record_.pushdown_storage_filter_row_cnt_);
        } break;
        case REQUEST_MEMORY_USED: {
          cells[cell_idx].set_int(record.data_.request_memory_used_);
        } break;
        case EXPECTED_WORKER_COUNT: {
          cells[cell_idx].set_int(record.data_.expected_worker_cnt_);
        } break;
        case USED_WORKER_COUNT: {
          cells[cell_idx].set_int(record.data_.used_worker_cnt_);
        } break;
        case SCHED_INFO: {
          //
          cells[cell_idx].set_default_collation_type();
        } break;
        case PS_CLIENT_STMT_ID: {
          cells[cell_idx].set_int(record.data_.ps_stmt_id_);
        } break;
        case PS_INNER_STMT_ID: {
          cells[cell_idx].set_int(record.data_.ps_inner_stmt_id_);
        } break;
        case TRANSACTION_HASH: {
          cells[cell_idx].set_int(record.data_.trans_id_);
          break;
        }
        case SNAPSHOT_VERSION: {
          uint64_t set_v = record.data_.get_snapshot_version().is_valid()
              ? record.data_.get_snapshot_version().get_val_for_inner_table_field() : 0;
          cells[cell_idx].set_uint64(set_v);
          break;
        }
        case SNAPSHOT_SOURCE: {
          ObString src_name = record.data_.get_snapshot_source();
          cells[cell_idx].set_varchar(src_name);
          cells[cell_idx].set_default_collation_type();
          break;
        }
        case REQUEST_TYPE: {
          cells[cell_idx].set_int(record.data_.request_type_);
          break;
        }
        case IS_BATCHED_MULTI_STMT: {
          cells[cell_idx].set_bool(record.data_.is_batched_multi_stmt_);
          break;
        }
        case OB_TRACE_INFO: {
          cells[cell_idx].set_default_collation_type();
        } break;
        case PLAN_HASH: {
          cells[cell_idx].set_uint64(record.data_.plan_hash_);
        } break;
        case LOCK_FOR_READ_TIME: {
          cells[cell_idx].set_int(record.data_.trx_lock_for_read_elapse_);
          break;
        }
        case PARAMS_VALUE: {
          if ((record.data_.params_value_len_ > 0) && (NULL != record.data_.params_value_)) {
            cells[cell_idx].set_lob_value(ObLongTextType, record.data_.params_value_,
                                          record.data_.params_value_len_);
          } else {
            cells[cell_idx].set_lob_value(ObLongTextType, "", 0);
          }
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                            ObCharset::get_default_charset()));
        } break;
        case RULE_NAME: {
          if ((record.data_.rule_name_len_ > 0) && (NULL != record.data_.rule_name_)) {
            cells[cell_idx].set_varchar(record.data_.rule_name_, record.data_.rule_name_len_);
          } else {
            cells[cell_idx].set_varchar("");
          }
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        } break;
        case TX_INTERNAL_ROUTE_FLAG: {
          cells[cell_idx].set_uint64(record.data_.txn_free_route_flag_);
        }  break;
        case PARTITION_HIT: {
          cells[cell_idx].set_bool(record.data_.partition_hit_);
        } break;
        case TX_INTERNAL_ROUTE_VERSION: {
          cells[cell_idx].set_uint64(record.data_.txn_free_route_version_);
          break;
        }
        case FLT_TRACE_ID: {
          if (OB_MAX_UUID_STR_LENGTH == strlen(record.data_.flt_trace_id_)) {
            cells[cell_idx].set_varchar(record.data_.flt_trace_id_,
                                        static_cast<ObString::obstr_size_t>(OB_MAX_UUID_STR_LENGTH));
          } else {
            cells[cell_idx].set_varchar("");
          }
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));

        } break;
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(col_id));
        } break;
        }
      }
    }
  }
  return ret;
}

int ObGvSqlAudit::extract_request_ids(const uint64_t tenant_id,
                                      int64_t &start_id,
                                      int64_t &end_id,
                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  const int64_t req_id_key_idx = is_index_scan() ? IDX_KEY_REQ_ID_IDX : PRI_KEY_REQ_ID_IDX;
  const int64_t tenant_id_key_idx = is_index_scan() ? IDX_KEY_TENANT_ID_IDX
                                                      : PRI_KEY_TENANT_ID_IDX;
  if (key_ranges_.count() >= 1) {

    for (int i = 0; OB_SUCC(ret) && is_valid && i < key_ranges_.count(); i++) {
      ObNewRange &req_id_range = key_ranges_.at(i);
      SERVER_LOG(DEBUG, "extracting request id for tenant", K(req_id_range), K(tenant_id));
      if (OB_UNLIKELY(req_id_range.get_start_key().get_obj_cnt() != 4
                      || req_id_range.get_end_key().get_obj_cnt() != 4)
                      || OB_ISNULL(req_id_range.get_start_key().get_obj_ptr())
                      || OB_ISNULL(req_id_range.get_end_key().get_obj_ptr())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected # of rowkey columns",
                   K(ret),
                   "size of start key", req_id_range.get_start_key().get_obj_cnt(),
                   "size of end key", req_id_range.get_end_key().get_obj_cnt(),
                   K(req_id_range.get_start_key().get_obj_ptr()),
                   K(req_id_range.get_end_key().get_obj_ptr()));
      } else {
        const ObObj &tenant_obj_high = req_id_range.get_end_key().get_obj_ptr()[tenant_id_key_idx];
        const ObObj &tenant_obj_low = req_id_range.get_start_key().get_obj_ptr()[tenant_id_key_idx];

        uint64_t min_tenant_id = 0;
        uint64_t max_tenant_id = 0;
        if (tenant_obj_low.is_min_value()) {
          min_tenant_id = 0;
        } else if (tenant_obj_low.is_max_value()) {
          min_tenant_id = UINT64_MAX;
        } else {
          min_tenant_id = tenant_obj_low.get_uint64();
        }

        if (tenant_obj_high.is_min_value()) {
          max_tenant_id = 0;
        } else if (tenant_obj_high.is_max_value()) {
          max_tenant_id = UINT64_MAX;
        } else {
          max_tenant_id = tenant_obj_high.get_uint64();
        }

        if (min_tenant_id <= max_tenant_id
            && min_tenant_id <= tenant_id
            && max_tenant_id >= tenant_id) {
          const ObObj &cur_start = req_id_range.get_start_key().get_obj_ptr()[req_id_key_idx];
          const ObObj &cur_end = req_id_range.get_end_key().get_obj_ptr()[req_id_key_idx];
          int64_t cur_start_id = -1;
          int64_t cur_end_id = -1;

          if (cur_start.is_min_value()) {
            cur_start_id = 0;
          } else if (cur_start.is_max_value()) {
            cur_start_id = INT64_MAX;
          } else {
            cur_start_id = cur_start.get_int();
          }
          if (cur_end.is_min_value()) {
            cur_end_id = 0;
          } else if (cur_end.is_max_value()) {
            cur_end_id = INT64_MAX;
          } else {
            cur_end_id = cur_end.get_int() + 1;
          }

          if (0 == i) {
            start_id = cur_start_id;
            end_id = cur_end_id;
            if (start_id >= end_id) {
              is_valid = false;
            }
          } else {
            start_id = MIN(cur_start_id, start_id);
            end_id = MAX(cur_end_id, end_id);
            if (start_id >= end_id) {
              is_valid = false;
            }
          }
        }
      }
    }
  }
  return ret;
}

} //namespace observer
} //namespace oceanbase
