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
#include "ob_virtual_sql_monitor.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "sql/monitor/ob_monitor_info_manager.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "observer/ob_server.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;
namespace oceanbase
{
namespace observer
{
ObVirtualSqlMonitor::ObVirtualSqlMonitor() : ObVirtualTableProjector(),
    monitor_manager_(NULL),
    start_id_(0),
    end_id_(0),
    ref_(),
    plan_info_(NULL),
    tenant_id_(0),
    request_id_(0),
    plan_id_(0),
    scheduler_ipstr_(),
    scheduler_port_(0),
    ipstr_(),
    port_(0),
    execution_time_(0)
  {
    info_buf_[0] = '\0';
  }

ObVirtualSqlMonitor::~ObVirtualSqlMonitor()
{
  reset();
}

void ObVirtualSqlMonitor::reset()
{
  if (NULL != monitor_manager_) {
    if (OB_SUCCESS != monitor_manager_->revert(&ref_)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to revert ref");
    }
  }
  info_buf_[0] = '\0';
  ObVirtualTableProjector::reset();
}

int ObVirtualSqlMonitor::inner_open()
{
  int ret = OB_SUCCESS;
  int64_t start_request_id = -1;
  int64_t end_request_id = -1;
  int64_t index = -1;
  ObRaQueue::Ref ref;
  ObPhyPlanMonitorInfo *plan_info = NULL;
  if (OB_SUCC(ret)) {
    if (NULL != monitor_manager_) {
      if (key_ranges_.count() >= 1) {
        ObNewRange &req_id_range = key_ranges_.at(0);
        if (OB_UNLIKELY(req_id_range.get_start_key().get_obj_cnt() != 6
                        || req_id_range.get_end_key().get_obj_cnt() != 6)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected  # of rowkey columns",
                     K(ret),
                     "size of start key", req_id_range.get_start_key().get_obj_cnt(),
                     "size of end key", req_id_range.get_end_key().get_obj_cnt());

        } else {
          ObObj id_low = (req_id_range.get_start_key().get_obj_ptr()[3]);
          ObObj id_high = (req_id_range.get_end_key().get_obj_ptr()[3]);
          if (id_low.is_min_value()) {
            start_id_ = monitor_manager_->get_start_index();
          } else if (id_low.is_max_value()) {
            start_id_ = monitor_manager_->get_start_index() + monitor_manager_->get_count();
          } else {
            start_request_id = (req_id_range.get_start_key().get_obj_ptr()[3]).get_int();
            if (OB_FAIL(monitor_manager_->get_by_request_id(start_request_id, index, plan_info, &ref))) {
              SERVER_LOG(WARN, "fail to get by request id", K(ret), K(start_request_id));
            } else {
              start_id_ = index;
            }
            monitor_manager_->revert(&ref);
          }
          if (OB_SUCC(ret)) {
            if (id_high.is_min_value()) {
              end_id_ = monitor_manager_->get_start_index();
            } else if (id_high.is_max_value()) {
              end_id_ = monitor_manager_->get_start_index() + monitor_manager_->get_count();
            } else {
              end_request_id = (req_id_range.get_end_key().get_obj_ptr()[3]).get_int();
              if (OB_FAIL(monitor_manager_->get_by_request_id(end_request_id, index, plan_info, &ref))) {
                SERVER_LOG(WARN, "fail to get by request id", K(ret), K(end_request_id));
              } else {
                end_id_ = index;
              }
              monitor_manager_->revert(&ref);
            }
          }
        }
      } else {
        start_id_ = monitor_manager_->get_start_index();
        end_id_ = start_id_ + monitor_manager_->get_count();
      }
    }
  }
  return ret;
}

int ObVirtualSqlMonitor::get_next_monitor_info()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(monitor_manager_->revert(&ref_))) {
    SERVER_LOG(WARN, "fail to revert ref", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (start_id_ > end_id_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(monitor_manager_->get_by_index(start_id_, plan_info_, &ref_))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        SERVER_LOG(WARN, "fail to get plan info", K(ret), K(start_id_));
      }
    } else if (OB_ISNULL(plan_info_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to get plan info", K(ret), K(plan_info_));
    } else {
      plan_id_ = plan_info_->get_plan_id();
      request_id_ = plan_info_->get_request_id();
      scheduler_port_ = plan_info_->get_address().get_port();
      execution_time_ = plan_info_->get_execution_time();
      if (plan_info_->get_address().ip_to_string(scheduler_ipstr_, OB_IP_STR_BUFF)) {
      } else {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fail to print ip to string", K(ret));
      }
    }
  }
  return ret;
}

int ObVirtualSqlMonitor::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  ObPhyPlanExecInfo *plan_info = NULL;
  ObArray<Column> columns;
  if (OB_ISNULL(monitor_manager_)) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(cells) || OB_ISNULL(allocator_)) {
    ret= OB_NOT_INIT;
    SERVER_LOG(WARN, "invalid arugment", K(ret), K(cells), K(allocator_), K(monitor_manager_));
  } else {
    while (OB_SUCC(get_next_monitor_info())) {  //处理可能的空洞
      if (NULL != plan_info_) {
        start_id_++;
        break;
      }
      start_id_++;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(plan_info_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to get plan info");
    } else if (OB_FAIL(plan_info_->get_plan_info(plan_info))) {
      SERVER_LOG(WARN, "fail to get operator by index", K(ret));
    } else if (OB_ISNULL(plan_info)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to get plan info", K(ret), K(plan_info));
    } else {
      const int64_t col_count = output_column_ids_.count();
      for (int64_t cell_idx = 0; cell_idx < col_count && OB_SUCC(ret); cell_idx++) {
        uint64_t col_id = output_column_ids_.at(cell_idx);
        switch (col_id) {
          case TENANT_ID:
            cells[cell_idx].set_int(tenant_id_);
            break;
          case SVR_IP:
            cells[cell_idx].set_varchar(ipstr_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                    ObCharset::get_default_charset()));
            break;
          case SVR_PORT:
            cells[cell_idx].set_int(port_);
            break;
          case REQUEST_ID:
            cells[cell_idx].set_int(request_id_);
            break;
          case JOB_ID:
            cells[cell_idx].set_int(plan_info->get_job_id());
            break;
          case TASK_ID:
            cells[cell_idx].set_int(plan_info->get_task_id());
            break;
          case PLAN_ID:
            cells[cell_idx].set_int(plan_id_);
            break;
          case SCHEDULER_IP:
            cells[cell_idx].set_varchar(scheduler_ipstr_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                    ObCharset::get_default_charset()));
            break;
          case SCHEDULER_PORT:
            cells[cell_idx].set_int(scheduler_port_);
            break;
          case MONITOR_INFO:
            {
              int64_t size = plan_info->print_info(info_buf_, OB_MAX_INFO_LENGTH);
              cells[cell_idx].set_varchar(ObString(size, info_buf_));
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                      ObCharset::get_default_charset()));
            }
            break;
          case EXTEND_INFO:
            cells[cell_idx].set_varchar(ObString::make_string(to_cstring(plan_info_->get_trace())));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                    ObCharset::get_default_charset()));
            break;
          case SQL_EXEC_START:
            cells[cell_idx].set_timestamp(execution_time_);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(col_id));
            break;
        }
      }

      if (OB_SUCC(ret)) {
        row = &cur_row_;
        SERVER_LOG(DEBUG, "get next row", K(plan_info_), K(plan_info_->get_operator_count()),
                   K(request_id_), K(plan_id_), K(*plan_info),
                   K(start_id_), K(end_id_));
      }
    }
  }
  return ret;
}

int ObVirtualSqlMonitor::set_addr(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (!addr.ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObString ipstr = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr, ipstr_))) {
      SERVER_LOG(WARN, "failed to write string", K(ret));
    }
    port_ = addr.get_port();
  }
  return ret;
}
} //namespace observer
} //namespace oceanbase
