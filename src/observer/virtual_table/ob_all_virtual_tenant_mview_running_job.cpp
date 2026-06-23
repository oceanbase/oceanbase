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

#include "observer/virtual_table/ob_all_virtual_tenant_mview_running_job.h"
#include "observer/ob_server.h"
#include "share/config/ob_server_config.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{

ObAllVirtualTenantMviewRunningJob::ObAllVirtualTenantMviewRunningJob()
    : ObVirtualTableScannerIterator(),
      fill_scanner_()
{
}

ObAllVirtualTenantMviewRunningJob::~ObAllVirtualTenantMviewRunningJob()
{
  reset();
}

void ObAllVirtualTenantMviewRunningJob::reset()
{
  fill_scanner_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTenantMviewRunningJob::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (!start_to_read_) {
    common::ObSEArray<uint64_t, 16> tenant_ids;
    if (is_sys_tenant(effective_tenant_id_)) {
      if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
        SERVER_LOG(WARN, "failed to add tenant id", K(ret));
      }
    } else {
      if (OB_FAIL(tenant_ids.push_back(effective_tenant_id_))) {
        SERVER_LOG(WARN, "failed to push back tenant id", KR(ret), K(effective_tenant_id_),
                   K(tenant_ids));
      }
    }
    if (OB_SUCC(ret)) {
      for (int i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
        uint64_t cur_tenant_id = tenant_ids.at(i);
        MTL_SWITCH(cur_tenant_id)
        {
          ObMViewMaintenanceService *mview_service = MTL(ObMViewMaintenanceService *);
#ifdef OB_BUILD_MV_REFRESH_QUEUEING
          omt::ObTenantConfigGuard tenant_config(TENANT_CONF(cur_tenant_id));
          const bool refresh_queuing_enabled = tenant_config.is_valid() && tenant_config->_enable_mv_refresh_queuing;
          // New engine: pending task manager is the source of truth for
          // running jobs; fields not tracked in pending (session_id,
          // read_snapshot, ...) are reported as defaults. Legacy engine: jobs
          // live in MDS. The two paths are mutually exclusive.
          ObMViewPendingTaskManager *pending_task_manager = nullptr;
          if (OB_ISNULL(mview_service)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "mview maintenance service is null", K(ret), K(cur_tenant_id));
          } else if (OB_FAIL(fill_scanner_.init(cur_tenant_id,
                                                &scanner_,
                                                &cur_row_,
                                                output_column_ids_))) {
            SERVER_LOG(WARN, "init fill scanner failed", K(ret), K(cur_tenant_id),
                       K(refresh_queuing_enabled));
          } else if (!refresh_queuing_enabled) {
            if (OB_FAIL(mview_service->get_mview_mds_op().foreach_refactored(fill_scanner_))) {
              SERVER_LOG(WARN, "fill mds running jobs failed", K(ret), K(cur_tenant_id));
            }
          } else if (OB_ISNULL(pending_task_manager = mview_service->get_pending_task_manager())) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "pending task manager is null", K(ret), K(cur_tenant_id));
          } else if (OB_FAIL(pending_task_manager->foreach_running_job(fill_scanner_))) {
            SERVER_LOG(WARN, "foreach pending task running job failed", K(ret), K(cur_tenant_id));
          }
          // Refresh queuing is closed-source; only the legacy MDS path is available.
#else
          if (OB_ISNULL(mview_service)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "mview maintenance service is null", K(ret), K(cur_tenant_id));
          } else if (OB_FAIL(fill_scanner_.init(cur_tenant_id,
                                                &scanner_,
                                                &cur_row_,
                                                output_column_ids_))) {
            SERVER_LOG(WARN, "init fill scanner failed", K(ret), K(cur_tenant_id));
          } else if (OB_FAIL(mview_service->get_mview_mds_op().foreach_refactored(fill_scanner_))) {
            SERVER_LOG(WARN, "fill mds running jobs failed", K(ret), K(cur_tenant_id));
          }
#endif
        }
      }
      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
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

  return ret;
}

int ObAllVirtualTenantMviewRunningJob::FillScanner::operator()(
              const hash::HashMapPair<transaction::ObTransID, ObMViewOpArg> &entry)
{
  int ret = OB_SUCCESS;
  const ObMViewOpArg &mview_op = entry.second;

  if (OB_FAIL(check_fill_param())) {
    SERVER_LOG(WARN, "fill scanner parameter is invalid", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    ObCharsetType default_charset = ObCharset::get_default_charset();
    ObCollationType default_collation = ObCharset::get_default_collation(default_charset);
    for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; ++cell_idx) {
      uint64_t col_id = output_column_ids_.at(cell_idx);
      switch (col_id) {
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
        cur_row_->cells_[cell_idx].set_int(MTL_ID());
        break;
      }
      case TABLE_ID: {
        cur_row_->cells_[cell_idx].set_int(mview_op.table_id_);
        break;
      }
      case JOB_TYPE: {
        cur_row_->cells_[cell_idx].set_uint64(mview_op.mview_op_type_);
        break;
      }
      case SESSION_ID: {
        cur_row_->cells_[cell_idx].set_uint64(mview_op.session_id_);
        break;
      }
      case READ_SNAPSHOT: {
        cur_row_->cells_[cell_idx].set_int(mview_op.read_snapshot_);
        break;
      }
      case PARALLEL: {
        cur_row_->cells_[cell_idx].set_int(mview_op.parallel_);
        break;
      }
      case JOB_START_TIME: {
        cur_row_->cells_[cell_idx].set_timestamp(mview_op.start_ts_);
        break;
      }
      case TARGET_DATA_SYNC_SCN: {
        cur_row_->cells_[cell_idx].set_uint64(mview_op.target_data_sync_scn_.
                                              get_val_for_inner_table_field());
        break;
      }
      case REFRESH_ID: {
        cur_row_->cells_[cell_idx].set_uint64(mview_op.refresh_id_);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
      }
      }
    }
    // The scanner supports up to 64M, so the overflow situation is not considered for the time being
    if (FAILEDx(scanner_->add_row(*cur_row_))) {
      SERVER_LOG(WARN, "fail to add row", K(ret), K(*cur_row_));
    }
  }

  return ret;
}

void ObAllVirtualTenantMviewRunningJob::FillScanner::reset()
{
  ip_buf_[0] = '\0';
  port_ = 0;
  effective_tenant_id_ = OB_INVALID_TENANT_ID;
  scanner_ = NULL;
  cur_row_ = NULL;
  output_column_ids_.reset();
}

int ObAllVirtualTenantMviewRunningJob::FillScanner::init(uint64_t effective_tenant_id,
                                                         common::ObScanner *scanner,
                                                         common::ObNewRow *cur_row,
                                                         const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == scanner || NULL == cur_row)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "some parameter is NULL", K(ret), K(scanner), K(cur_row));
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

int ObAllVirtualTenantMviewRunningJob::FillScanner::check_fill_param() const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(0 == port_ ||
                  OB_INVALID_TENANT_ID == effective_tenant_id_ ||
                  NULL == scanner_ ||
                  NULL == cur_row_ ||
                  NULL == cur_row_->cells_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "parameter or data member is NULL", K(ret), K(port_), K(effective_tenant_id_),
               K(scanner_), K(cur_row_));
  } else if (OB_UNLIKELY(cur_row_->count_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells count is less than output column count", K(ret), K(cur_row_->count_),
               K(output_column_ids_.count()));
  }

  return ret;
}

#ifdef OB_BUILD_MV_REFRESH_QUEUEING
int ObAllVirtualTenantMviewRunningJob::FillScanner::operator()(
    const ObMViewPendingRunningJobInfo &job_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_fill_param())) {
    SERVER_LOG(WARN, "fill scanner parameter is invalid", K(ret), K(job_info));
  } else {
    const int64_t col_count = output_column_ids_.count();
    ObCharsetType default_charset = ObCharset::get_default_charset();
    ObCollationType default_collation = ObCharset::get_default_collation(default_charset);
    int64_t job_type = MVIEW_OP_TYPE::UNDEFINE_OP;
    get_job_type(job_info.refresh_method_, job_type);
    for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; ++cell_idx) {
      uint64_t col_id = output_column_ids_.at(cell_idx);
      switch (col_id) {
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
          cur_row_->cells_[cell_idx].set_int(job_info.tenant_id_);
          break;
        }
        case TABLE_ID: {
          cur_row_->cells_[cell_idx].set_int(job_info.mview_id_);
          break;
        }
        case JOB_TYPE: {
          cur_row_->cells_[cell_idx].set_uint64(job_type);
          break;
        }
        case SESSION_ID: {
          // 0 means the executor (possibly on a different observer) has not
          // yet stamped session_id onto the inner table row, OR the lazy
          // batch-fetch on this dump failed (best-effort degradation).
          cur_row_->cells_[cell_idx].set_uint64(job_info.session_id_);
          break;
        }
        case READ_SNAPSHOT: {
          cur_row_->cells_[cell_idx].set_int(0);
          break;
        }
        case PARALLEL: {
          cur_row_->cells_[cell_idx].set_int(job_info.refresh_parallel_);
          break;
        }
        case JOB_START_TIME: {
          const int64_t start_ts = job_info.gmt_modified_ > 0 ? job_info.gmt_modified_ : job_info.gmt_create_;
          cur_row_->cells_[cell_idx].set_timestamp(start_ts);
          break;
        }
        case TARGET_DATA_SYNC_SCN: {
          cur_row_->cells_[cell_idx].set_uint64(job_info.target_data_sync_scn_);
          break;
        }
        case REFRESH_ID: {
          cur_row_->cells_[cell_idx].set_uint64(job_info.refresh_id_);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
        }
      }
    }
    // The scanner supports up to 64M, so the overflow situation is not considered for the time being
    if (FAILEDx(scanner_->add_row(*cur_row_))) {
      SERVER_LOG(WARN, "fail to add row", K(ret), K(*cur_row_));
    }
  }

  return ret;
}
#endif

void ObAllVirtualTenantMviewRunningJob::FillScanner::get_job_type(
    const share::schema::ObMVRefreshMethod refresh_method,
    int64_t &job_type)
{
  switch (refresh_method) {
    case share::schema::ObMVRefreshMethod::COMPLETE: {
      job_type = MVIEW_OP_TYPE::COMPLETE_REFRESH;
      break;
    }
    case share::schema::ObMVRefreshMethod::FAST: {
      job_type = MVIEW_OP_TYPE::FAST_REFRESH;
      break;
    }
    default: {
      job_type = MVIEW_OP_TYPE::UNDEFINE_OP;
      break;
    }
  }
}

}/* ns observer*/
}/* ns oceanbase */
