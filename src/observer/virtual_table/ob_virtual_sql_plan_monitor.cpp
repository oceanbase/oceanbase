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
#include "ob_virtual_sql_plan_monitor.h"
#include "common/ob_smart_call.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "sql/monitor/ob_monitor_info_manager.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "observer/ob_server.h"
#include <algorithm> // std::sort

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;
using namespace oceanbase::omt;
using namespace oceanbase::share;

ObVirtualSqlPlanMonitor::ObVirtualSqlPlanMonitor() :
    ObVirtualTableScannerIterator(),
    cur_mysql_req_mgr_(nullptr),
    start_id_(INT64_MAX),
    end_id_(INT64_MIN),
    cur_id_(0),
    ref_(),
    addr_(),
    ipstr_(),
    port_(0),
    is_first_get_(true),
    is_use_index_(false),
    tenant_id_array_(),
    tenant_id_array_idx_(-1),
    with_tenant_ctx_(nullptr),
    need_rt_node_(false),
    rt_nodes_(),
    rt_node_idx_(0),
    rt_start_idx_(INT64_MAX),
    rt_end_idx_(INT64_MIN)
{
  server_ip_[0] = '\0';
  trace_id_[0] = '\0';
}

ObVirtualSqlPlanMonitor::~ObVirtualSqlPlanMonitor()
{
  reset();
}

void ObVirtualSqlPlanMonitor::reset()
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
  port_ = 0;
  ipstr_.reset();
  need_rt_node_ = false;
  rt_nodes_.reset();
  rt_node_idx_ = 0;
}

int ObVirtualSqlPlanMonitor::inner_open()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(extract_tenant_ids())) {
    SERVER_LOG(WARN, "failed to extract tenant ids", K(ret));
  }

  SERVER_LOG(DEBUG, "tenant ids", K(tenant_id_array_));

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

int ObVirtualSqlPlanMonitor::set_ip(const common::ObAddr &addr)
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

int ObVirtualSqlPlanMonitor::check_ip_and_port(bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;

  // is_serving_tenant (svr_ip, svr_port) in (ip1, port1), (ip2, port2), ...
  // query range is [(ip1, port1), (ip1, port1)], [(ip2, port2), (ip2, port2)], ...
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
        ip_obj.set_collation_type(ip_low.get_collation_type());
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

int ObVirtualSqlPlanMonitor::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;

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
    is_first_get_ = false;
  }

  if (OB_SUCC(ret)) {
    if (!need_rt_node_ && OB_FAIL(switch_tenant_monitor_node_list())) {
      LOG_WARN("fail to switch tenant monitor node list", K(ret));
    } else if (OB_FAIL(report_rt_monitor_node(row))) {
      if (OB_ITER_END == ret) {
        reset_rt_node_info();
        ret = OB_SUCCESS;
        LOG_TRACE("finish report current tenant real time monitor node", K(ret));
      } else {
        LOG_WARN("fail to report real time monitor node", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !need_rt_node_) {
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
        ObMonitorNode *node = static_cast<ObMonitorNode *>(rec);
        if (OB_FAIL(convert_node_to_row(*node, row))) {
          LOG_WARN("fail convert node", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected null rec",
                   K(rec), K(cur_id_), K(tenant_id_array_idx_), K(tenant_id_array_), K(ret));
      }
    }

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
      // may be all the record is flushed, call inner_get_next_row recursively
      ret = SMART_CALL(inner_get_next_row(row));
    }
  }

  return ret;
}

int ObVirtualSqlPlanMonitor::report_rt_monitor_node(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (need_rt_node_ && OB_NOT_NULL(cur_mysql_req_mgr_)) {
    if (rt_nodes_.empty()) {
      if (OB_FAIL(cur_mysql_req_mgr_->convert_node_map_2_array(rt_nodes_))) {
        LOG_WARN("fail to convert node map to array", K(ret));
      } else {
        rt_start_idx_ = MAX(rt_start_idx_, 0);
        rt_end_idx_ = MIN(rt_end_idx_, rt_nodes_.count());
        if (!is_reverse_scan()) {
          rt_node_idx_ = rt_start_idx_;
        } else {
          rt_node_idx_ = rt_end_idx_ - 1;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (rt_node_idx_ >= rt_end_idx_ || rt_node_idx_ < rt_start_idx_) {
      ret = OB_ITER_END;
      LOG_WARN("rt node iter end", K(ret));
    } else if (OB_FAIL(convert_node_to_row(rt_nodes_.at(rt_node_idx_), row))) {
      LOG_WARN("fail to convert node to row", K(ret));
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpectd null row", K(ret));
    } else if (!is_reverse_scan()) {
      rt_node_idx_++;
    } else {
      rt_node_idx_--;
    }
  }
  LOG_TRACE("check rt_nodes_.count()", K(rt_nodes_.count()), K(rt_node_idx_), K(ret));
  return ret;
}

int ObVirtualSqlPlanMonitor::switch_tenant_monitor_node_list()
{
  int ret = OB_SUCCESS;
  if (nullptr == cur_mysql_req_mgr_ || (cur_id_ < start_id_ ||
                                        cur_id_ >= end_id_)) {
    sql::ObPlanMonitorNodeList *prev_req_mgr = cur_mysql_req_mgr_;
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
            // ignore error when tenant not in this server, return empty record
            if (OB_TENANT_NOT_IN_SERVER == ret) {
              ret = OB_SUCCESS;
              continue;
            } else {
              SERVER_LOG(WARN, "failed to switch tenant context", K(t_id), K(ret));
            }
          } else {
            cur_mysql_req_mgr_ = with_tenant_ctx_->entity().get_tenant()->get<sql::ObPlanMonitorNodeList*>();
          }
        }

        if (nullptr == cur_mysql_req_mgr_) {
          SERVER_LOG(DEBUG, "req manager doest not exist", K(t_id));
          continue;
        } else if (OB_SUCC(ret)) {
          start_id_ = INT64_MIN;
          end_id_ = INT64_MAX;
          reset_rt_node_info();
          bool is_req_valid = true;
          if (OB_FAIL(extract_request_ids(t_id, start_id_, end_id_, is_req_valid))) {
            SERVER_LOG(WARN, "failed to extract request ids", K(ret));
          } else if (!is_req_valid) {
            SERVER_LOG(DEBUG, "invalid query range", K(t_id), K(key_ranges_));
            ret = OB_ITER_END;
          } else {
            int64_t start_idx = cur_mysql_req_mgr_->get_start_idx();
            int64_t end_idx = cur_mysql_req_mgr_->get_end_idx();
            if (start_id_ < 0 && start_id_ < end_id_) {
              need_rt_node_ = true;
              if (end_id_ <= 0) {
                rt_start_idx_ = -end_id_;
                rt_end_idx_ = (INT64_MIN == start_id_ ? INT64_MAX : -start_id_);
              } else {
                rt_end_idx_ = (INT64_MIN == start_id_ ? INT64_MAX : -start_id_);
                rt_start_idx_ = 0;
              }
            }
            start_id_ = MAX(start_id_, start_idx);
            end_id_ = MIN(end_id_, end_idx);
            if (start_id_ >= end_id_) {
              if (need_rt_node_) {
                break;
              } else {
                SERVER_LOG(DEBUG, "cur_mysql_req_mgr_ iter end", K(start_id_), K(end_id_), K(t_id));
                prev_req_mgr = cur_mysql_req_mgr_;
                cur_mysql_req_mgr_ = nullptr;
              }
            } else if (is_reverse_scan()) {
              cur_id_ = end_id_ - 1;
            } else {
              cur_id_ = start_id_;
            }
            SERVER_LOG(DEBUG, "start to get rows from inner table",
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
  return ret;
}

int ObVirtualSqlPlanMonitor::extract_tenant_ids()
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
            } else if (!is_sys_tenant(effective_tenant_id_) && effective_tenant_id_ != tenant_id) {
              // skip
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
        if (!is_sys_tenant(effective_tenant_id_) && effective_tenant_id_ != id_list.at(i)) {
          // skip
        } else if (OB_FAIL(tenant_id_array_.push_back(id_list.at(i)))) {
          SERVER_LOG(WARN, "failed to push back tenant id", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_always_false) {
        tenant_id_array_.reset();
      } else {
        lib::ob_sort(tenant_id_array_.begin(), tenant_id_array_.end());
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
            } else if (!is_sys_tenant(effective_tenant_id_) && effective_tenant_id_ != tenant_id) {
              // skip
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
        lib::ob_sort(tenant_id_array_.begin(), tenant_id_array_.end());
        SERVER_LOG(DEBUG, "get tenant ids from req mgr map", K(tenant_id_array_));
      }
    }
  }
  if (OB_FAIL(ret)) {
    tenant_id_array_.reset();
  }
  return ret;
}

int ObVirtualSqlPlanMonitor::extract_request_ids(const uint64_t tenant_id,
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
            cur_start_id = INT64_MIN;
          } else if (cur_start.is_max_value()) {
            cur_start_id = INT64_MAX;
          } else {
            cur_start_id = cur_start.get_int();
          }
          if (cur_end.is_min_value()) {
            cur_end_id = INT64_MIN;
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

#define CASE_OTHERSTAT(N) \
        case OTHERSTAT_##N##_ID: { \
          int64_t int_value = node.otherstat_##N##_id_;\
          cells[cell_idx].set_int(int_value); \
          break; \
        } \
        case OTHERSTAT_##N##_VALUE: { \
          int64_t int_value = node.otherstat_##N##_value_;\
          cells[cell_idx].set_int(int_value); \
          break; \
        }

#define CASE_OTHERSTAT_RESERVED(N) \
        case OTHERSTAT_##N##_ID: { \
          cells[cell_idx].set_int(0); \
          break; \
        } \
        case OTHERSTAT_##N##_VALUE: { \
          cells[cell_idx].set_int(0); \
          break; \
        }

int ObVirtualSqlPlanMonitor::convert_node_to_row(ObMonitorNode &node, ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  }
  uint64_t cpu_khz = OBSERVER.get_cpu_frequency_khz();
  for (int64_t cell_idx = 0;
       OB_SUCC(ret) && cell_idx < output_column_ids_.count();
       ++cell_idx) {
    const uint64_t column_id = output_column_ids_.at(cell_idx);
    switch(column_id) {
      case TENANT_ID: {
        uint64_t tenant_id = node.tenant_id_;
        cells[cell_idx].set_int(tenant_id);
        break;
      }
      case REQUEST_ID: {
        if (need_rt_node_) {
          // rowkey cannot be null, use negative number
          cells[cell_idx].set_int(-rt_node_idx_ - 1);
        } else {
          cells[cell_idx].set_int(cur_id_);
        }
        break;
      }
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
      case TRACE_ID: {
        int len = node.get_trace_id().to_string(trace_id_, sizeof(trace_id_));
        cells[cell_idx].set_varchar(trace_id_, len);
        cells[cell_idx].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case DB_TIME: {
        // concept:
        cells[cell_idx].set_int(node.db_time_ / cpu_khz);
        break;
      }
      case USER_IO_WAIT_TIME: {
        cells[cell_idx].set_int(node.block_time_ / cpu_khz);
        break;
      }
      case FIRST_REFRESH_TIME: {
        int64_t int_value = node.open_time_;
        if (int_value) {
          cells[cell_idx].set_timestamp(int_value);
        } else {
          cells[cell_idx].set_null();
        }
        break;
      }
      case LAST_REFRESH_TIME: {
        int64_t int_value = node.close_time_;
        if (int_value) {
          cells[cell_idx].set_timestamp(int_value);
        } else {
          cells[cell_idx].set_null();
        }
        break;
      }
      case FIRST_CHANGE_TIME: {
        int64_t int_value = node.first_row_time_;
        if (int_value) {
          cells[cell_idx].set_timestamp(int_value);
        } else {
          cells[cell_idx].set_null();
        }
        break;
      }
      case LAST_CHANGE_TIME: {
        int64_t int_value = node.last_row_time_;
        if (int_value != 0) {
          cells[cell_idx].set_timestamp(int_value);
        } else {
          cells[cell_idx].set_null();
        }
        break;
      }
      CASE_OTHERSTAT(1);
      CASE_OTHERSTAT(2);
      CASE_OTHERSTAT(3);
      CASE_OTHERSTAT(4);
      CASE_OTHERSTAT(5);
      CASE_OTHERSTAT(6);
      CASE_OTHERSTAT_RESERVED(7);
      CASE_OTHERSTAT_RESERVED(8);
      CASE_OTHERSTAT_RESERVED(9);
      CASE_OTHERSTAT_RESERVED(10);
      case THREAD_ID: {
        int64_t thread_id = node.get_thread_id();
        cells[cell_idx].set_int(thread_id);
        break;
      }
      case PLAN_OPERATION: {
        const char *name = node.get_operator_name();
        cells[cell_idx].set_varchar(name);
        cells[cell_idx].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case STARTS: {
        int64_t int_value = node.rescan_times_;
        cells[cell_idx].set_int(int_value);
        break;
      }
      case OUTPUT_ROWS: {
        int64_t int_value = node.output_row_count_;
        cells[cell_idx].set_int(int_value);
        break;
      }
      case PLAN_LINE_ID: {
        int64_t int_value = node.get_op_id();
        cells[cell_idx].set_int(int_value);
        break;
      }
      case PLAN_DEPTH: {
        int64_t int_value = node.plan_depth_;
        cells[cell_idx].set_int(int_value);
        break;
      }
      case OUTPUT_BATCHES: { // for batch
        int64_t int_value = node.output_batches_;
        cells[cell_idx].set_int(int_value);
        break;
      }
      case SKIPPED_ROWS_COUNT: { // for batch
        int64_t int_value = node.skipped_rows_count_;
        cells[cell_idx].set_int(int_value);
        break;
      }
      case WORKAREA_MEM: {
        if(need_rt_node_) {
          int64_t int_value = node.workarea_mem_;
          cells[cell_idx].set_int(int_value);
        } else {
          cells[cell_idx].set_null();
        }
        break;
      }
      case WORKAREA_MAX_MEM: {
        int64_t int_value = node.workarea_max_mem_;
        cells[cell_idx].set_int(int_value);
        break;
      }
      case WORKAREA_TEMPSEG: {
        if (need_rt_node_) {
          int64_t int_value = node.workarea_tempseg_;
          cells[cell_idx].set_int(int_value);
        } else {
          cells[cell_idx].set_null();
        }
        break;
      }
      case WORKAREA_MAX_TEMPSEG: {
        int64_t int_value = node.workarea_max_tempseg_;
        cells[cell_idx].set_int(int_value);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(cell_idx),
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
