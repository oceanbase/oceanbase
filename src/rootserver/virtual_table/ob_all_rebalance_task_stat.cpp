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

#include "ob_all_rebalance_task_stat.h"

#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver {
void ObAllRebalanceTaskStat::Display::reset()
{
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  partition_id_ = OB_INVALID_INDEX;
  partition_cnt_ = 0;
  src_ = NULL;
  data_src_ = NULL;
  dest_ = NULL;
  offline_ = NULL;
  task_type_ = NULL;
  is_replicate_ = NULL;
  is_scheduled_ = NULL;
  is_manual_ = NULL;
  waiting_time_ = 0;
  executing_time_ = 0;
}

ObAllRebalanceTaskStat::ObAllRebalanceTaskStat() : inited_(false), schema_service_(NULL), rebalance_task_mgr_(NULL)
{}

ObAllRebalanceTaskStat::~ObAllRebalanceTaskStat()
{}

int ObAllRebalanceTaskStat::init(ObMultiVersionSchemaService& schema_service, ObRebalanceTaskMgr& rebalance_task_mgr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    rebalance_task_mgr_ = &rebalance_task_mgr;
    inited_ = true;
  }

  return ret;
}

int ObAllRebalanceTaskStat::inner_get_next_row(ObNewRow*& row)
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
    ObArray<ObRebalanceTask*> task_stats;
    const ObTableSchema* table_schema = NULL;
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_REBALANCE_TASK_STAT_TID);
    if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(table_id), K(ret));
    } else if (NULL == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", KP(table_schema), K(ret));
    } else if (OB_FAIL(rebalance_task_mgr_->get_all_tasks(allocator, task_stats))) {
      LOG_WARN("fail to get tasks", K(ret));
    } else {
      ObArray<Column> columns;
      for (int64_t j = 0; OB_SUCC(ret) && j < task_stats.count(); ++j) {
        const ObRebalanceTask* task_stat = task_stats.at(j);
        if (OB_UNLIKELY(nullptr == task_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("task stat", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < task_stat->get_sub_task_count(); ++i) {
            columns.reuse();
            const ObRebalanceTaskInfo* task_info = task_stat->get_sub_task(i);
            if (OB_UNLIKELY(nullptr == task_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("task info ptr is null", K(ret));
            } else if (OB_FAIL(get_full_row(table_schema, *task_stat, *task_info, columns))) {
              LOG_WARN("fail to get full row", "table_schema", *table_schema, "task_stat", *task_stat, K(ret));
            } else if (OB_FAIL(project_row(columns, cur_row_))) {
              LOG_WARN("fail to project row", K(columns), K(ret));
            } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
              LOG_WARN("fail to add row", K_(cur_row), K(ret));
            }
          }
        }
      }
    }
    for (int64_t j = 0; j < task_stats.count(); ++j) {
      ObRebalanceTask* task_stat = task_stats.at(j);
      if (nullptr != task_stat) {
        task_stat->~ObRebalanceTask();
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

int ObAllRebalanceTaskStat::generate_task_stat(
    const ObRebalanceTask& task_stat, const ObRebalanceTaskInfo& task_info, Display& display)
{
  int ret = OB_SUCCESS;
  common::ObAddr src;
  common::ObAddr data_src;
  common::ObAddr dest;
  common::ObAddr offline;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(task_info.get_virtual_rebalance_task_stat_info(src, data_src, dest, offline))) {
    LOG_WARN("fail to get virtual rebalance task stat info", K(ret));
  } else {
    display.src_ = NULL;
    display.dest_ = NULL;
    display.data_src_ = NULL;
    display.offline_ = NULL;
    display.task_type_ = NULL;
    display.is_replicate_ = NULL;
    display.is_scheduled_ = NULL;
    display.is_manual_ = NULL;

    if (NULL == (display.src_ = static_cast<char*>(allocator_->alloc(OB_IP_PORT_STR_BUFF)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate source buf", "size", OB_IP_PORT_STR_BUFF, K(ret));
    } else if (OB_FAIL(src.ip_port_to_string(display.src_, OB_IP_PORT_STR_BUFF))) {
      LOG_WARN("fail to get source server ip:port", K(src), LITERAL_K(OB_IP_PORT_STR_BUFF), K(ret));
    } else if (NULL == (display.data_src_ = static_cast<char*>(allocator_->alloc(OB_IP_PORT_STR_BUFF)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate data source buf", "size", OB_IP_PORT_STR_BUFF, K(ret));
    } else if (OB_FAIL(data_src.ip_port_to_string(display.data_src_, OB_IP_PORT_STR_BUFF))) {
      LOG_WARN("fail to get source server ip:port", K(data_src), LITERAL_K(OB_IP_PORT_STR_BUFF), K(ret));
    } else if (NULL == (display.dest_ = static_cast<char*>(allocator_->alloc(OB_IP_PORT_STR_BUFF)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate destination server buf", "size", OB_IP_PORT_STR_BUFF, K(ret));
    } else if (OB_FAIL(dest.ip_port_to_string(display.dest_, OB_IP_PORT_STR_BUFF))) {
      LOG_WARN("fail to get destination server ip:port", K(dest), LITERAL_K(OB_IP_PORT_STR_BUFF), K(ret));
    } else if (NULL == (display.offline_ = static_cast<char*>(allocator_->alloc(OB_IP_PORT_STR_BUFF)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate offline server buf", "size", OB_IP_PORT_STR_BUFF, K(ret));
    } else if (OB_FAIL(offline.ip_port_to_string(display.offline_, OB_IP_PORT_STR_BUFF))) {
      LOG_WARN("fail to get offline server ip:port", K(offline), LITERAL_K(OB_IP_PORT_STR_BUFF), K(ret));
    } else if (NULL == (display.task_type_ = static_cast<char*>(allocator_->alloc(128)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate replicate status buf", "size", MAX_BOOL_STR_LENGTH, K(ret));
    } else if (NULL == (display.is_replicate_ = static_cast<char*>(allocator_->alloc(MAX_BOOL_STR_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate scheduled status buf", "size", MAX_BOOL_STR_LENGTH, K(ret));
    } else if (NULL == (display.is_scheduled_ = static_cast<char*>(allocator_->alloc(MAX_BOOL_STR_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate scheduled status buf", "size", MAX_BOOL_STR_LENGTH, K(ret));
    } else if (NULL == (display.is_manual_ = static_cast<char*>(allocator_->alloc(MAX_BOOL_STR_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate is_manual buf", "size", MAX_BOOL_STR_LENGTH, K(ret));

    } else {
      const char* task_type_str = task_stat.get_task_type_str(task_stat.get_rebalance_task_type());
      const char* replicate_bool_str =
          ObRebalanceTaskType::ADD_REPLICA == task_stat.get_rebalance_task_type() ? "Yes" : "No";
      const char* scheduled_bool_str = task_stat.in_schedule() ? "Yes" : "No";
      const char* is_manual_str = task_info.is_manual() ? "Yes" : "No";
      const int64_t now = ObTimeUtility::current_time();
      if (OB_FAIL(databuff_printf(display.task_type_, 128, "%s", task_type_str))) {
        LOG_WARN("printf failed", "buf_len", 128, "src len", strlen(task_type_str), K(ret));
      } else if (OB_FAIL(databuff_printf(display.is_replicate_, MAX_BOOL_STR_LENGTH, "%s", replicate_bool_str))) {
        LOG_WARN("printf failed", "buf_len", MAX_BOOL_STR_LENGTH, "src len", strlen(replicate_bool_str), K(ret));
      } else if (OB_FAIL(databuff_printf(display.is_scheduled_, MAX_BOOL_STR_LENGTH, "%s", scheduled_bool_str))) {
        LOG_WARN("printf failed", "buf_len", MAX_BOOL_STR_LENGTH, "src len", strlen(scheduled_bool_str), K(ret));
      } else if (OB_FAIL(databuff_printf(display.is_manual_, MAX_BOOL_STR_LENGTH, "%s", is_manual_str))) {
        LOG_WARN("printf failed", "buf_len", MAX_BOOL_STR_LENGTH, "is_manual len", strlen(is_manual_str), K(ret));
      } else {
        display.table_id_ = task_info.get_partition_key().get_table_id();
        display.tenant_id_ = task_info.get_tenant_id();
        display.partition_id_ = task_info.get_partition_key().get_partition_id();
        display.partition_cnt_ = task_info.get_partition_key().get_partition_cnt();

        if (task_stat.in_schedule()) {
          display.waiting_time_ = task_stat.get_schedule_time() - task_stat.get_generate_time();
          display.executing_time_ = now - task_stat.get_schedule_time();
        } else {
          display.waiting_time_ = now - task_stat.get_generate_time();
          display.executing_time_ = 0;
        }
      }
    }

    if (OB_FAIL(ret)) {
      if (NULL != display.src_) {
        allocator_->free(display.src_);
        display.src_ = NULL;
      }
      if (NULL != display.data_src_) {
        allocator_->free(display.data_src_);
        display.data_src_ = NULL;
      }
      if (NULL != display.dest_) {
        allocator_->free(display.dest_);
        display.dest_ = NULL;
      }
      if (NULL != display.offline_) {
        allocator_->free(display.offline_);
        display.offline_ = NULL;
      }
      if (NULL != display.task_type_) {
        allocator_->free(display.task_type_);
        display.task_type_ = NULL;
      }
      if (NULL != display.is_replicate_) {
        allocator_->free(display.is_replicate_);
        display.is_replicate_ = NULL;
      }
      if (NULL != display.is_scheduled_) {
        allocator_->free(display.is_scheduled_);
        display.is_scheduled_ = NULL;
      }
      if (NULL != display.is_manual_) {
        allocator_->free(display.is_manual_);
        display.is_manual_ = NULL;
      }
      if (NULL != display.is_manual_) {
        allocator_->free(display.is_manual_);
        display.is_manual_ = NULL;
      }
    }
  }

  return ret;
}

int ObAllRebalanceTaskStat::get_full_row(const share::schema::ObTableSchema* table, const ObRebalanceTask& task_stat,
    const ObRebalanceTaskInfo& task_info, common::ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  // Display play a role of memory medium, no memory leak occurs, since the allocator
  // is a page arena from outside.
  Display display;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(generate_task_stat(task_stat, task_info, display))) {
    LOG_WARN("fail to generate task stat", K(task_stat), K(ret));
  } else {
    ADD_COLUMN(set_uint64, table, "tenant_id", display.tenant_id_, columns);
    ADD_COLUMN(set_uint64, table, "table_id", display.table_id_, columns);
    ADD_COLUMN(set_int, table, "partition_id", display.partition_id_, columns);
    ADD_COLUMN(set_int, table, "partition_count", display.partition_cnt_, columns);
    ADD_COLUMN(set_varchar, table, "source", display.src_, columns);
    ADD_COLUMN(set_varchar, table, "data_source", display.data_src_, columns);
    ADD_COLUMN(set_varchar, table, "destination", display.dest_, columns);
    ADD_COLUMN(set_varchar, table, "offline", display.offline_, columns);
    // TODO: rename is_replicate to priority
    ADD_COLUMN(set_varchar, table, "is_replicate", display.is_replicate_, columns);
    ADD_COLUMN(set_varchar, table, "task_type", display.task_type_, columns);
    ADD_COLUMN(set_varchar, table, "is_scheduled", display.is_scheduled_, columns);
    ADD_COLUMN(set_varchar, table, "is_manual", display.is_manual_, columns);
    ADD_COLUMN(set_int, table, "waiting_time", display.waiting_time_, columns);
    ADD_COLUMN(set_int, table, "executing_time", display.executing_time_, columns);
  }

  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
