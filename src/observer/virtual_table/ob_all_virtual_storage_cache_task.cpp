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

#include "ob_all_virtual_storage_cache_task.h"
#include "share/ob_server_struct.h"
#include "share/ash/ob_di_util.h"

using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{

static constexpr char OB_SCP_V_TABLE_ALLOCATOR[] = "SCPVTable";

ObAllVirtualStorageCacheTask::ObAllVirtualStorageCacheTask()
    : tmp_str_allocator_(OB_SCP_V_TABLE_ALLOCATOR),
      tenant_id_(OB_INVALID_TENANT_ID),
      cur_idx_(0),
      tablet_tasks_()
{
  ip_buf_[0] = '\0';
  speed_buf_[0] = '\0';
}

ObAllVirtualStorageCacheTask::~ObAllVirtualStorageCacheTask()
{
  reset();
}

void ObAllVirtualStorageCacheTask::reset()
{
  ip_buf_[0] = '\0';
  speed_buf_[0] = '\0';
  tenant_id_ = OB_INVALID_TENANT_ID;
  cur_idx_ = 0;
  tablet_tasks_.reset();
  tmp_str_allocator_.reset();
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualStorageCacheTask::inner_open()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObAllVirtualStorageCacheTask::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SERVER_LOG(WARN, "execute fail", KR(ret));
    }
  }
  return ret;
}

int ObAllVirtualStorageCacheTask::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  ObAddr addr = GCTX.self_addr();
  const int64_t col_count = output_column_ids_.count();

  if (OB_ISNULL(cells)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", KR(ret), K(cells));
  } else if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ITER_END;
  }
#ifdef OB_BUILD_SHARED_STORAGE
  if (OB_SUCC(ret)) {
    ObStorageCachePolicyService *scp_service = nullptr;
    if (OB_ISNULL(scp_service = MTL(ObStorageCachePolicyService *))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "ObStorageCachePolicyService is NULL", KR(ret));
    } else {
      tmp_str_allocator_.reset();
      if (MTL_ID() != tenant_id_) {
        tenant_id_ = MTL_ID();
        if (is_user_tenant(tenant_id_)) {
          const SCPTabletTaskMap &tablet_tasks_map =
              scp_service->get_tablet_tasks();
          if (OB_FAIL(copy_tablet_tasks_map_(tablet_tasks_map))) {
            SERVER_LOG(WARN, "fail to copy tablet tasks", KR(ret), KPC(scp_service));
          }
        } else {  // only process user tenant
          ret = OB_ITER_END;
        }
      }

      const ObStorageCacheTabletTask *task = nullptr;
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(cur_idx_ < 0)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cur_idx_ is invalid", KR(ret), K(cur_idx_));
      } else if (cur_idx_ >= tablet_tasks_.count()) {
        ret = OB_ITER_END;
      } else if (OB_ISNULL(task = tablet_tasks_.at(cur_idx_).get_ptr())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "ObStorageCacheTabletTask is NULL", KR(ret), K(cur_idx_));
      }

      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
        const uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
        case SVR_IP: {
          if (addr.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cells[i].set_varchar(ip_buf_);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", KR(ret), K(addr));
          }
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(addr.get_port());
          break;
        }
        case TENANT_ID: {
          cells[i].set_int(tenant_id_);
          break;
        }
        case TABLET_ID: {
          cells[i].set_int(task->get_tablet_id());
          break;
        }
        case STATUS: {
          const char *status_str = nullptr;
          char *alloc_status_str = nullptr;
          if (OB_ISNULL(status_str = ObStorageCacheTaskStatus::get_str(task->get_status()))) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "storage cache status_str is NULL", KR(ret), KPC(task));
          // set_varchar function performs a shallow copy, so memory allocation needs to be done
          } else if (OB_FAIL(ob_dup_cstring(tmp_str_allocator_, status_str, alloc_status_str))) {
            SERVER_LOG(WARN, "fail to deep copy status_str", KR(ret), KPC(task));
          } else {
            cells[i].set_varchar(alloc_status_str);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case SPEED: {
          const char *unit = "B/s";
          double speed = task->get_speed();
          if (speed > 1000) {
            speed /= 1000;
            unit = "KB/s";
          }
          if (speed > 1000) {
            speed /= 1000;
            unit = "MB/s";
          }
          if (speed > 1000) {
            speed /= 1000;
            unit = "GB/s";
          }

          if (OB_FAIL(databuff_printf(speed_buf_, sizeof(speed_buf_), "%.2lf %s", speed, unit))) {
            SERVER_LOG(WARN, "fail to print speed", KR(ret), KPC(task), K(speed), K(unit));
          } else {
            cells[i].set_varchar(speed_buf_);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case START_TIME: {
          cells[i].set_timestamp(task->get_start_time());
          break;
        }
        case COMPLETE_TIME: {
          cells[i].set_timestamp(task->get_end_time());
          break;
        }
        case RESULT: {
          cells[i].set_int(task->get_result());
          break;
        }
        case COMMENT: {
          char *alloc_comment_str = nullptr;
          if (OB_FAIL(ob_dup_cstring(tmp_str_allocator_, task->get_comment(), alloc_comment_str))) {
            SERVER_LOG(WARN, "fail to deep copy comment", KR(ret), KPC(task));
          } else {
            if (OB_ISNULL(alloc_comment_str)) {
              cells[i].set_varchar("");
            } else {
              cells[i].set_varchar(alloc_comment_str);
            }
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        } // end switch
      } // end for
      if (OB_SUCC(ret)) {
        row = &cur_row_;
        cur_idx_++;
      }
    }
  }
#endif
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
class ObCopyTabletTaskFunc
{
public:
  ObCopyTabletTaskFunc(ObSEArray<ObStorageCacheTabletTaskHandle, OB_VIRTUAL_TABLE_TABLET_TASK_NUM> &tablet_tasks)
      : tablet_tasks_(tablet_tasks)
  {}

  int operator()(const hash::HashMapPair<int64_t, ObStorageCacheTabletTaskHandle> &entry)
  {
    int ret = OB_SUCCESS;
    ObStorageCacheTabletTaskHandle tmp_task_handle(entry.second);
    if (OB_UNLIKELY(!tmp_task_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "ObStorageCacheTabletTaskHandle is invalid",
          KR(ret), K(entry.first), KPC(entry.second.get_ptr()));
    } else if (tmp_task_handle()->is_hot() && OB_FAIL(tablet_tasks_.push_back(tmp_task_handle))) {
      SERVER_LOG(WARN, "fail to copy store handle", KR(ret), K(tablet_tasks_.count()));
    }
    return ret;
  }

private:
  ObSEArray<ObStorageCacheTabletTaskHandle, OB_VIRTUAL_TABLE_TABLET_TASK_NUM> &tablet_tasks_;
};

int ObAllVirtualStorageCacheTask::copy_tablet_tasks_map_(
    const SCPTabletTaskMap &tablet_tasks_map)
{
  int ret = OB_SUCCESS;
  tablet_tasks_.reset();
  cur_idx_ = 0;
  ObCopyTabletTaskFunc func(tablet_tasks_);
  if (tablet_tasks_map.size() > 0 && OB_FAIL(tablet_tasks_map.foreach_refactored(func))) {
    SERVER_LOG(WARN, "fail to iterate tablets tasks map", KR(ret), K(tablet_tasks_map.size()));
  }
  return ret;
}
#endif

void ObAllVirtualStorageCacheTask::release_last_tenant()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tablet_tasks_.reset();
}

bool ObAllVirtualStorageCacheTask::is_need_process(uint64_t tenant_id)
{
  return is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_;
}

} // namespace observer
} // namespace oceanbase