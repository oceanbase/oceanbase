// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//         http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#define USING_LOG_PREFIX RS
#include "ob_all_virtual_backup_task_scheduler_stat.h"
#include "rootserver/backup/ob_backup_schedule_task.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver
{
void ObAllBackupScheduleTaskStat::Display::reset()
{
  tenant_id_ = OB_INVALID_ID;
  job_id_ = 0;
  task_id_ = 0;
  key_1_ = 0;
  job_type_ = BackupJobType::BACKUP_JOB_MAX;
  trace_id_.reset();
  dst_.reset();
  generate_ts_ = 0;
  schedule_ts_ = 0;
  executor_ts_ = 0;
}

int ObAllBackupScheduleTaskStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (!start_to_read_) {
    auto func_iterate_tenant_task_scheduler = [&]() -> int
    {
      int ret = OB_SUCCESS;
      common::ObArenaAllocator allocator;
      ObArray<ObBackupScheduleTask *> task_stats;
      ObBackupTaskScheduler *task_scheduler = MTL(ObBackupTaskScheduler *);
      if (OB_ISNULL(task_scheduler)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task scheduler can't be null", K(ret));
      } else if (OB_FAIL(task_scheduler->get_all_tasks(allocator, task_stats))) {
        LOG_WARN("fail to get tasks", K(ret));
      } else if (OB_FAIL(generate_all_row_(task_stats))) {
        LOG_WARN("fail to generate all row", K(ret));
      }
      return ret;
    };
    if (OB_FAIL(omt_->operate_in_each_tenant(func_iterate_tenant_task_scheduler))) {
      LOG_WARN("fail to operate in each tenant", K(ret));
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret) && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllBackupScheduleTaskStat::generate_all_row_(ObIArray<ObBackupScheduleTask *> &task_stats)
{
  int ret = OB_SUCCESS;
  for (int64_t j = 0; OB_SUCC(ret) && j < task_stats.count(); ++j) {
    const ObBackupScheduleTask *task_stat = task_stats.at(j);
    if (OB_UNLIKELY(nullptr == task_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task stat", K(ret));
    } else if (OB_FAIL(get_full_row_(*task_stat))) {
      LOG_WARN("fail to get full row", "task_stat", *task_stat, K(ret));
    } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
      LOG_WARN("fail to add row", K(cur_row_), K(ret));
    }
  }

  for (int64_t j = 0; j < task_stats.count(); ++j) {
    ObBackupScheduleTask *task_stat = task_stats.at(j);
    if (nullptr != task_stat) {
      task_stat->~ObBackupScheduleTask();
      task_stat = nullptr;
    }
  }
  return ret;
}

int ObAllBackupScheduleTaskStat::generate_task_stat_(
    const ObBackupScheduleTask &task_stat,
    Display &display)
{
  int ret = OB_SUCCESS;
  display.tenant_id_ = task_stat.get_tenant_id();
  display.job_id_ = task_stat.get_job_id();
  display.task_id_ = task_stat.get_task_id();
  display.key_1_ = task_stat.get_ls_id();
  display.job_type_ = task_stat.get_type();

  display.trace_id_ = task_stat.get_trace_id();
  display.dst_ = task_stat.get_dst();
  display.is_schedule = display.dst_.is_valid() ? true : false;
  display.generate_ts_ = task_stat.get_generate_time();
  display.schedule_ts_ = task_stat.get_schedule_time();
  display.executor_ts_ = task_stat.get_executor_time();
  return ret;
}

int ObAllBackupScheduleTaskStat::get_full_row_(const ObBackupScheduleTask &task_stat)
{
  int ret = OB_SUCCESS;
  Display display;
  ObAddr self_addr = GCONF.self_addr_;
  char *dst_str = nullptr;
  char *trace_id_str = nullptr;

  if (OB_ISNULL(dst_str = static_cast<char *>(allocator_.alloc(OB_MAX_SERVER_ADDR_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(dst_str));
  } else if (OB_FALSE_IT(MEMSET(dst_str, '\0', OB_MAX_SERVER_ADDR_SIZE))) {
  } else if (OB_ISNULL(trace_id_str = static_cast<char *>(allocator_.alloc(OB_MAX_TRACE_ID_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(trace_id_str));
  } else if (OB_FALSE_IT(MEMSET(trace_id_str, '\0', OB_MAX_TRACE_ID_BUFFER_SIZE))) {
  } else if (OB_FAIL(generate_task_stat_(task_stat, display))) {
    LOG_WARN("fail to generate task stat", K(task_stat), K(ret));
  } else if (display.dst_.is_valid() && OB_FAIL(display.dst_.ip_port_to_string(dst_str, OB_MAX_SERVER_ADDR_SIZE))) {
    LOG_WARN("fail to change dst to string", K(ret), K(display));
  } else if (!display.trace_id_.is_invalid()) {
      display.trace_id_.to_string(trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE);
  }

  const int64_t count = output_column_ids_.count();
  for (int i = 0; OB_SUCC(ret) && i < count; i++) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case Column::TENANT_ID: {
        cur_row_.cells_[i].set_int(display.tenant_id_);
        break;
      }
      case Column::SVR_IP: {
        char *addr_buf = nullptr;
        if (OB_ISNULL(addr_buf = static_cast<char *>(allocator_.alloc(OB_IP_PORT_STR_BUFF)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret), K(dst_str));
        } else if (OB_FALSE_IT(MEMSET(addr_buf, '\0', OB_IP_PORT_STR_BUFF))) {
        } else if (!self_addr.ip_to_string(addr_buf, common::OB_IP_PORT_STR_BUFF)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ip_to_string failed", K(ret));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(addr_buf));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case Column::SVR_PORT: {
        cur_row_.cells_[i].set_int(self_addr.get_port());
        break;
      }

      case Column::JOB_ID: {
        cur_row_.cells_[i].set_int(display.job_id_);
        break;
      }
      case Column::TASK_ID: {
        cur_row_.cells_[i].set_int(display.task_id_);
        break;
      }
      case Column::LS_ID: {
        cur_row_.cells_[i].set_int(display.key_1_);
        break;
      }
      case Column::JOB_TYPE: {
        cur_row_.cells_[i].set_int(static_cast<int64_t>(display.job_type_));
        break;
      }
      case Column::TRACE_ID: {
        cur_row_.cells_[i].set_varchar(trace_id_str);
        break;
      }
      case Column::DST: {
        cur_row_.cells_[i].set_varchar(dst_str);
        break;
      }
      case Column::IS_SCHEDULE: {
        const char *flag = display.is_schedule ? "True" : "False";
        cur_row_.cells_[i].set_varchar(flag);
        break;
      }
      case Column::GENERATE_TS: {
        cur_row_.cells_[i].set_int(display.generate_ts_);
        break;
      }
      case Column::SCHEDULE_TS: {
        cur_row_.cells_[i].set_int(display.schedule_ts_);
        break;
      }
      case Column::EXECUTOR_TS: {
        cur_row_.cells_[i].set_int(display.executor_ts_);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unkown column", K(ret), K(col_id));
        break;
      }
    }
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
