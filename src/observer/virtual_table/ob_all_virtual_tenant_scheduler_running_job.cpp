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

#include "lib/string/ob_string.h"
#include "share/config/ob_server_config.h"
#include "observer/virtual_table/ob_all_virtual_tenant_scheduler_running_job.h"
#include "observer/ob_server.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace observer
{

ObAllVirtualTenantSchedulerRunningJob::ObAllVirtualTenantSchedulerRunningJob()
    : ObVirtualTableScannerIterator(),
      session_mgr_(NULL),
      fill_scanner_()
{
}

ObAllVirtualTenantSchedulerRunningJob::~ObAllVirtualTenantSchedulerRunningJob()
{
  reset();
}

void ObAllVirtualTenantSchedulerRunningJob::reset()
{
  session_mgr_ = NULL;
  fill_scanner_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTenantSchedulerRunningJob::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_mgr_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "sessionMgr is NULL", K(ret));
  } else {
    if (!start_to_read_) {
      if (OB_FAIL(fill_scanner_.init(effective_tenant_id_,
                                     &scanner_,
                                     &cur_row_,
                                     output_column_ids_))) {
        SERVER_LOG(WARN, "init fill_scanner fail", K(ret));
      } else if (OB_FAIL(session_mgr_->for_each_hold_session(fill_scanner_))) {
        SERVER_LOG(WARN, "fill scanner fail", K(ret));
      } else {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
    if (OB_SUCCESS == ret && start_to_read_) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }
  return ret;
}

int ObAllVirtualTenantSchedulerRunningJob::FillScanner::operator()(
              hash::HashMapPair<uint64_t, ObSQLSessionInfo *> &entry)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *sess_info = entry.second;
  ObSQLSessionInfo::LockGuard lock_guard(sess_info->get_thread_data_lock());
  if (OB_UNLIKELY(0 == port_
                  || OB_INVALID_TENANT_ID == effective_tenant_id_
                  || NULL == scanner_
                  || NULL == cur_row_
                  || NULL == cur_row_->cells_
                  || NULL == sess_info)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN,
               "parameter or data member is NULL",
               K(ret),
               K(port_),
               K(effective_tenant_id_),
               K(scanner_),
               K(cur_row_),
               K(sess_info));
  } else if (!is_sys_tenant(effective_tenant_id_)
            && sess_info->get_effective_tenant_id() != effective_tenant_id_) {
    // skip other tenant
  } else if (OB_ISNULL(sess_info->get_job_info())) {
    // skip session without dbms scheduler job
  } else if (OB_UNLIKELY(cur_row_->count_ < output_column_ids_.count())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN,
                   "cells count is less than output column count",
                   K(ret),
                   K(cur_row_->count_),
                   K(output_column_ids_.count()));
  } else {
    uint64_t cell_idx = 0;
    const int64_t col_count = output_column_ids_.count();
    ObCharsetType default_charset = ObCharset::get_default_charset();
    ObCollationType default_collation = ObCharset::get_default_collation(default_charset);
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
        case SVR_IP: {
          cur_row_->cells_[cell_idx].set_varchar(ip_buf_);
          cur_row_->cells_[cell_idx].set_collation_type(default_collation);
          break;
        }
        case SVR_PORT: {
          cur_row_->cells_[cell_idx].set_int(port_);
          break;
        }
        case TENANT_ID: {
          cur_row_->cells_[cell_idx].set_int(sess_info->get_effective_tenant_id());
          break;
        }
        case OWNER: {
          if (sess_info->get_is_deserialized()) {
            cur_row_->cells_[cell_idx].set_varchar("");
            cur_row_->cells_[cell_idx].set_collation_type(default_collation);
          } else {
            cur_row_->cells_[cell_idx].set_varchar(sess_info->get_user_name());
            cur_row_->cells_[cell_idx].set_collation_type(default_collation);
          }
          break;
        }
        case JOB_NAME: {
          if (OB_NOT_NULL(sess_info->get_job_info())) {
            cur_row_->cells_[cell_idx].set_varchar(sess_info->get_job_info()->get_job_name());
            cur_row_->cells_[cell_idx].set_collation_type(default_collation);
          } else {
            cur_row_->cells_[cell_idx].set_null();
          }
          break;
        }
        case JOB_SUBNAME: {
          cur_row_->cells_[cell_idx].set_null();
          break;
        }
        case JOB_STYLE: {
          cur_row_->cells_[cell_idx].set_null();
          break;
        }
        case DETACHED: {
          cur_row_->cells_[cell_idx].set_null();
          break;
        }
        case SESSION_ID: {
          cur_row_->cells_[cell_idx].set_uint64(static_cast<uint64_t>(sess_info->get_sessid()));
          break;
        }
        case SLAVE_PROCESS_ID: {
          cur_row_->cells_[cell_idx].set_null();
          break;
        }
        case SLAVE_OS_PROCESS_ID: {
          cur_row_->cells_[cell_idx].set_null();
          break;
        }
        case RESOURCE_CONSUMER_GROUP: {
          cur_row_->cells_[cell_idx].set_null();
          break;
        }
        case RUNNING_INSTANCE: {
          cur_row_->cells_[cell_idx].set_null();
          break;
        }
        case ELAPSED_TIME: {
          cur_row_->cells_[cell_idx].set_int(ObTimeUtility::current_time() - sess_info->get_sess_create_time());
          break;
        }
        case CPU_USED: {
          cur_row_->cells_[cell_idx].set_null();
          break;
        }
        case DESTINATION_OWNER: {
          cur_row_->cells_[cell_idx].set_null();
          break;
        }
        case DESTINATION: {
          cur_row_->cells_[cell_idx].set_null();
          break;
        }
        case CREDENTIAL_OWNER: {
          cur_row_->cells_[cell_idx].set_null();
          break;
        }
        case CREDENTIAL_NAME: {
          cur_row_->cells_[cell_idx].set_null();
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                      K(i), K(output_column_ids_), K(col_id));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    } // for
    // The scanner supports up to 64M, so the overflow situation is not considered for the time being
    if (FAILEDx(scanner_->add_row(*cur_row_))) {
      SERVER_LOG(WARN, "fail to add row", K(ret), K(*cur_row_));
    }
  }
  return ret;
}

void ObAllVirtualTenantSchedulerRunningJob::FillScanner::reset()
{
  ip_buf_[0] = '\0';
  port_ = 0;
  effective_tenant_id_ = OB_INVALID_TENANT_ID;
  scanner_ = NULL;
  cur_row_ = NULL;
  output_column_ids_.reset();
}

int ObAllVirtualTenantSchedulerRunningJob::FillScanner::init(uint64_t effective_tenant_id,
                                               common::ObScanner *scanner,
                                               common::ObNewRow *cur_row,
                                               const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == scanner
                  || NULL == cur_row)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN,
               "some parameter is NULL", K(ret), K(scanner), K(cur_row));
  } else if (OB_FAIL(output_column_ids_.assign(column_ids))) {
    SQL_ENG_LOG(WARN, "fail to assign output column ids", K(ret), K(column_ids));
  } else if (!ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip_to_string failed", K(ret));
  } else {
    port_ = ObServer::get_instance().get_self().get_port();
    effective_tenant_id_ = effective_tenant_id;
    scanner_ = scanner;
    cur_row_ = cur_row;
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
