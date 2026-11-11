/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/virtual_table/ob_all_virtual_mview_refresh_timer.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "rootserver/mview/ob_mview_maintenance_service.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
using namespace common;
using namespace rootserver;

namespace observer
{

ObAllVirtualMViewRefreshTimer::ObAllVirtualMViewRefreshTimer()
  : ObVirtualTableScannerIterator(),
    start_to_read_(false),
    ip_buf_()
{
}

ObAllVirtualMViewRefreshTimer::~ObAllVirtualMViewRefreshTimer()
{
  reset();
}

void ObAllVirtualMViewRefreshTimer::reset()
{
  start_to_read_ = false;
  ip_buf_[0] = '\0';
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualMViewRefreshTimer::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!GCONF.self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_)))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "failed to convert self address to string", KR(ret), K(GCONF.self_addr_));
  }
  return ret;
}

#ifdef OB_BUILD_MV_CLOSE_MODULES
int ObAllVirtualMViewRefreshTimer::fill_row(
    const uint64_t tenant_id,
    const ObMViewSchedulerTimerSnapshot &snapshot)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
    const uint64_t column_id = output_column_ids_.at(i);
    switch (column_id) {
      case GMT_MODIFIED:
        cur_row_.cells_[i].set_timestamp(snapshot.gmt_modified_);
        break;
      case SVR_IP:
        cur_row_.cells_[i].set_varchar(ip_buf_);
        cur_row_.cells_[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case SVR_PORT:
        cur_row_.cells_[i].set_int(GCONF.self_addr_.get_port());
        break;
      case TENANT_ID:
        cur_row_.cells_[i].set_int(tenant_id);
        break;
      case GROUP_ID:
        cur_row_.cells_[i].set_int(snapshot.group_id_);
        break;
      case MVIEW_ID:
        cur_row_.cells_[i].set_int(snapshot.mview_id_);
        break;
      case NEXT_TRIGGER_TIME:
        cur_row_.cells_[i].set_timestamp(snapshot.next_trigger_ts_);
        break;
      case GROUP_INTERVAL:
        cur_row_.cells_[i].set_int(snapshot.group_refresh_interval_);
        break;
      case EXPECTED_INTERVAL:
        cur_row_.cells_[i].set_int(snapshot.specified_refresh_interval_);
        break;
      case SCHEDULE_INTERVAL:
        cur_row_.cells_[i].set_int(snapshot.mview_refresh_interval_);
        break;
      case LAST_START_TIME:
        cur_row_.cells_[i].set_timestamp(snapshot.last_start_time_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected mview refresh timer column",
                   KR(ret), K(column_id));
        break;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(scanner_.add_row(cur_row_))) {
    SERVER_LOG(WARN, "failed to append mview refresh timer row",
               KR(ret), K(tenant_id), K(snapshot));
  }
  return ret;
}
#endif

int ObAllVirtualMViewRefreshTimer::fill_scanner()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_MV_CLOSE_MODULES
  ObSEArray<uint64_t, 16> tenant_ids;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "multi tenant service is null", KR(ret));
  } else if (is_sys_tenant(effective_tenant_id_)) {
    if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_ids))) {
      SERVER_LOG(WARN, "failed to get local tenant ids", KR(ret));
    }
  } else if (OB_FAIL(tenant_ids.push_back(effective_tenant_id_))) {
    SERVER_LOG(WARN, "failed to append effective tenant id",
               KR(ret), K_(effective_tenant_id));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
    const uint64_t tenant_id = tenant_ids.at(i);
    if (!is_virtual_tenant_id(tenant_id) && !is_meta_tenant(tenant_id)) {
      MTL_SWITCH(tenant_id)
      {
        ObMViewMaintenanceService *service = MTL(ObMViewMaintenanceService *);
        ObMViewPendingTaskManager *manager = NULL;
        ObSEArray<ObMViewSchedulerTimerSnapshot, 16> snapshots;
        if (OB_ISNULL(service)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "mview maintenance service is null", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(manager = service->get_pending_task_manager())) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "mview pending task manager is null", KR(ret), K(tenant_id));
        } else if (OB_FAIL(manager->get_scheduler_timer_snapshot(snapshots))) {
          SERVER_LOG(WARN, "failed to get mview scheduler timer snapshot",
                     KR(ret), K(tenant_id));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < snapshots.count(); ++j) {
            if (OB_FAIL(fill_row(tenant_id, snapshots.at(j)))) {
              SERVER_LOG(WARN, "failed to fill mview refresh timer row",
                         KR(ret), K(tenant_id), K(j));
            }
          }
        }
      }
    }
  }
#endif
  return ret;
}

int ObAllVirtualMViewRefreshTimer::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    if (OB_ISNULL(cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "current row cells are null", KR(ret));
    } else if (OB_FAIL(fill_scanner())) {
      SERVER_LOG(WARN, "failed to fill mview refresh timer scanner", KR(ret));
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret) && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "failed to get mview refresh timer row", KR(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
