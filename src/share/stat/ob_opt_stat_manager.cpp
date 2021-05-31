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

#define USING_LOG_PREFIX COMMON
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_stat_service.h"
#include "share/stat/ob_opt_stat_sql_service.h"

#include "share/stat/ob_opt_stat_manager.h"

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common::sqlclient;
namespace common {

ObOptStatManager::ObOptStatManager() : inited_(false), stat_service_(nullptr), last_schema_version_(-1)
{}

#if 0
int ObOptStatManager::refresh_on_schema_change(int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObArray<ObSchemaOperation> schema_operations;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(sql_service_.fetch_incremental_schema_operations(last_schema_version_,
                                                                      schema_version,
                                                                      schema_operations))) {
    LOG_WARN("fetch schema operations failed.", K(ret));
  } else if (schema_operations.count() == 0) {
    // no needed schema operation, only update schema version
    last_schema_version_ = schema_version;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < schema_operations.count(); ++i) {
      const ObSchemaOperation &schema_operation = schema_operations.at(i);
      if (schema_operation.op_type_ == OB_DDL_ALTER_COLUMN) {
        int64_t column_id = OB_INVALID_ID;
        bool is_deleted = false;
        if (OB_FAIL(sql_service_.fetch_changed_column(schema_operation, column_id, is_deleted))) {
          LOG_WARN("get changed column failed.", K(ret));
        } else if (is_deleted) {
        } else {
        }
      } else if (schema_operation.op_type_ == OB_DDL_ALTER_TABLE) {
      }
    }
  }
  return ret;
}
#endif

int ObOptStatManager::init(ObOptStatService* stat_service, ObMySQLProxy* proxy, ObServerConfig* config)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("optimizer statistics manager has already been initialized.", K(ret));
  } else if (nullptr == stat_service) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("initialize optimizer statistics manager with invalid arguments.", KP(stat_service), K(ret));
  } else if (OB_FAIL(refresh_stat_task_queue_.init(1, "OptRefTask", REFRESH_STAT_TASK_NUM, REFRESH_STAT_TASK_NUM))) {
    LOG_WARN("initialize timer failed. ", K(ret));
  } else if (OB_FAIL(sql_service_.init(proxy, config))) {
  } else {
    stat_service_ = stat_service;
    inited_ = true;
  }
  return ret;
}

ObOptTableStat& ObOptStatManager::get_default_table_stat()
{
  // default row count of memtable is set to 0
  static ObOptTableStat default_table_state_(OB_EST_DEFAULT_ROW_COUNT,
      0,
      OB_EST_DEFAULT_DATA_SIZE,
      OB_EST_DEFAULT_MACRO_BLOCKS,
      OB_EST_DEFAULT_MICRO_BLOCKS,
      DEFAULT_ROW_SIZE,
      DEFAULT_ROW_SIZE);
  return default_table_state_;
}

int ObOptStatManager::add_refresh_stat_task(const obrpc::ObUpdateStatCacheArg& analyze_arg)
{
  int ret = OB_SUCCESS;
  ObRefreshStatTask refresh_task(this);
  if (OB_FAIL(refresh_task.init(analyze_arg))) {
    LOG_WARN("initialize refresh task failed. ", K(ret));
  } else if (refresh_stat_task_queue_.add_task(refresh_task)) {
    LOG_WARN("add refresh stat task to task queue failed. ", K(ret));
  }
  return ret;
}

int ObOptStatManager::get_column_stat(const ObOptColumnStat::Key& key, ObOptColumnStatHandle& handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat manager has not been initialized.", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column stat key.", K(key), K(ret));
  } else if (OB_FAIL(stat_service_->get_column_stat(key, handle))) {
    LOG_WARN("get_column_stat failed.", K(ret));
  }
  return ret;
}

int ObOptStatManager::get_table_stat(const ObOptTableStat::Key& key, ObOptTableStat& tstat)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(stat_service_->get_table_stat(key, tstat))) {
    LOG_WARN("get table stat failed", K(ret));
  }
  return ret;
}

int ObOptStatManager::update_column_stat(const ObIArray<ObOptColumnStat*>& column_stats)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(sql_service_.update_column_stat(column_stats))) {
    LOG_WARN("failed to update column stat.", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptStatManager::delete_column_stat(const ObIArray<ObOptColumnStat*>& column_stats)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(sql_service_.delete_column_stat(column_stats))) {
    LOG_WARN("failed to update column stat.", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptStatManager::refresh_column_stat(const ObOptColumnStat::Key& key)
{
  int ret = OB_SUCCESS;
  ObOptColumnStatHandle handle;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(stat_service_->load_column_stat_and_put_cache(key, handle))) {
    LOG_WARN("fetch and store statistics in cache failed. ", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptStatManager::refresh_table_stat(const ObOptTableStat::Key& key)
{
  int ret = OB_SUCCESS;
  ObOptTableStatHandle handle;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(stat_service_->load_table_stat_and_put_cache(key, handle))) {
    LOG_WARN("fetch and store statistics in cache failed. ", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObOptStatManager::ObRefreshStatTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stat_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stat manager should not be NULL.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < analyze_arg_.partition_ids_.count(); ++i) {
      ObOptTableStat::Key table_key(analyze_arg_.table_id_, analyze_arg_.partition_ids_.at(i));
      if (OB_FAIL(stat_manager_->refresh_table_stat(table_key))) {
        LOG_WARN("update table statistics failed", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < analyze_arg_.column_ids_.count(); ++j) {
        ObOptColumnStat::Key key(
            analyze_arg_.table_id_, analyze_arg_.partition_ids_.at(i), analyze_arg_.column_ids_.at(j));
        if (OB_FAIL(stat_manager_->refresh_column_stat(key))) {
          LOG_WARN("update column statistics failed", K(ret));
        }
      }
    }
  }
  return ret;
}

IObDedupTask* ObOptStatManager::ObRefreshStatTask::deep_copy(char* buffer, const int64_t buf_size) const
{
  int ret = OB_SUCCESS;
  ObRefreshStatTask* task = nullptr;
  if (OB_ISNULL(buffer) || OB_UNLIKELY(buf_size < get_deep_copy_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, buffer is NULL. ", K(ret));
  } else {
    task = new (buffer) ObRefreshStatTask(stat_manager_);
    if (OB_FAIL(task->init(analyze_arg_))) {
      LOG_WARN("initialize analyze argument failed. ", K(ret));
      task = nullptr;
    }
  }
  return task;
}

}  // namespace common
}  // namespace oceanbase
