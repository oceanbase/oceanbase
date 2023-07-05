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

ObAllBackupScheduleTaskStat::ObAllBackupScheduleTaskStat()
  :inited_(false),
   schema_service_(nullptr),
   task_scheduer_(nullptr),
   arena_allocator_()
{
}

ObAllBackupScheduleTaskStat::~ObAllBackupScheduleTaskStat()
{
}

int ObAllBackupScheduleTaskStat::init(
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObBackupTaskScheduler &task_scheduler)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    task_scheduer_ = &task_scheduler;
    inited_ = true;
  }

  return ret;
}

int ObAllBackupScheduleTaskStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, allocator is null", K(ret));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard error", K(ret));
  } else if (!start_to_read_) {
    common::ObArenaAllocator allocator;
    ObArray<ObBackupScheduleTask *> task_stats;
    const ObTableSchema *table_schema = NULL;
    const uint64_t table_id = OB_ALL_VIRTUAL_BACKUP_SCHEDULE_TASK_TID;
    if (OB_FAIL(schema_guard.get_table_schema(OB_SYS_TENANT_ID, table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(table_id), K(ret));
    } else if (NULL == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", KP(table_schema), K(ret));
    } else if (OB_FAIL(task_scheduer_->get_all_tasks(allocator, task_stats))) {
      LOG_WARN("fail to get tasks", K(ret));
    } else {
      ObArray<Column> columns;
      for (int64_t j = 0; OB_SUCC(ret) && j < task_stats.count(); ++j) {
        const ObBackupScheduleTask *task_stat = task_stats.at(j);
        columns.reuse();
        if (OB_UNLIKELY(nullptr == task_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task stat", K(ret));
        } else if (OB_FAIL(get_full_row_(table_schema, *task_stat, columns))) {
          LOG_WARN("fail to get full row", "table_schema", *table_schema, "task_stat", *task_stat, K(ret));
        } else if (OB_FAIL(project_row(columns, cur_row_))) {
          LOG_WARN("fail to project row", K(columns), K(ret));
        } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
          LOG_WARN("fail to add row", K(cur_row_), K(ret));
        }
      }
    }

    for (int64_t j = 0; j < task_stats.count(); ++j) {
      ObBackupScheduleTask *task_stat = task_stats.at(j);
      if (nullptr != task_stat) {
        task_stat->~ObBackupScheduleTask();
        task_stat = nullptr;
      }
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret)) {
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

int ObAllBackupScheduleTaskStat::get_full_row_(
    const share::schema::ObTableSchema *table,
    const ObBackupScheduleTask &task_stat,
    common::ObIArray<Column> &columns)
{
  int ret = OB_SUCCESS;
  Display display;
  char *dst_str = nullptr;
  char *trace_id_str = nullptr;
  arena_allocator_.reuse();
  if (nullptr == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is nullptr", K(ret));
  } else if (OB_ISNULL(dst_str = static_cast<char *>(arena_allocator_.alloc(OB_MAX_SERVER_ADDR_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc dst ip buf failed", K(ret), "size", OB_MAX_SERVER_ADDR_SIZE);
  } else if (OB_FALSE_IT(MEMSET(dst_str, '\0', OB_MAX_SERVER_ADDR_SIZE))) {
  } else if (OB_ISNULL(trace_id_str = static_cast<char *>(arena_allocator_.alloc(OB_MAX_TRACE_ID_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc trace id buf failed", K(ret), "size", OB_MAX_TRACE_ID_BUFFER_SIZE);
  } else if (OB_FALSE_IT(MEMSET(trace_id_str, '\0', OB_MAX_TRACE_ID_BUFFER_SIZE))) {
  } else if (OB_FAIL(generate_task_stat_(task_stat, display))) {
    LOG_WARN("fail to generate task stat", K(task_stat), K(ret));
  } else if (display.dst_.is_valid() && OB_FAIL(display.dst_.ip_port_to_string(dst_str, OB_MAX_SERVER_ADDR_SIZE))) {
    LOG_WARN("fail to change dst to string", K(ret), K(display));
  } else if (!display.trace_id_.is_invalid() && OB_FALSE_IT(display.trace_id_.to_string(trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE))) {
  } else {
    ADD_COLUMN(set_int, table, "tenant_id", display.tenant_id_, columns);
    ADD_COLUMN(set_int, table, "job_id", display.job_id_, columns);
    ADD_COLUMN(set_int, table, "task_id", display.task_id_, columns);
    ADD_COLUMN(set_int, table, "ls_id", display.key_1_, columns);
    ADD_COLUMN(set_int, table, "type", static_cast<int64_t>(display.job_type_), columns);
    ADD_COLUMN(set_varchar, table, "trace_id", trace_id_str, columns);
    ADD_COLUMN(set_varchar, table, "destination", dst_str, columns);
    ADD_COLUMN(set_varchar, table, "is_schedule", display.is_schedule ? "True" : "False", columns);
    ADD_COLUMN(set_int, table, "generate_ts", display.generate_ts_, columns);
    ADD_COLUMN(set_int, table, "schedule_ts", display.schedule_ts_, columns);
    ADD_COLUMN(set_int, table, "executor_ts", display.executor_ts_, columns);
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
