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
#include "sql/plan_cache/ob_plan_cache.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/ob_basic_stats_estimator.h"
#include "sql/optimizer/ob_opt_selectivity.h"

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common::sqlclient;
namespace  common
{

ObOptStatManager::ObOptStatManager()
  : inited_(false),
    stat_service_(),
    last_schema_version_(-1)
{
}

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

int ObOptStatManager::init(ObMySQLProxy *proxy,
                           ObServerConfig *config)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("optimizer statistics manager has already been initialized.", K(ret));
  } else if (OB_FAIL(stat_service_.init(proxy, config))) {
    LOG_WARN("failed to init stat service", K(ret));
  } else if (OB_FAIL(refresh_stat_task_queue_.init(1, "OptRefTask", REFRESH_STAT_TASK_NUM, REFRESH_STAT_TASK_NUM))) {
    LOG_WARN("initialize timer failed. ", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObOptStatManager::stop()
{
  refresh_stat_task_queue_.stop();
}

void ObOptStatManager::wait()
{
  refresh_stat_task_queue_.wait();
}

void ObOptStatManager::destroy()
{
  refresh_stat_task_queue_.destroy();
}

int ObOptStatManager::add_refresh_stat_task(const obrpc::ObUpdateStatCacheArg &analyze_arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(handle_refresh_stat_task(analyze_arg))) {
    LOG_WARN("failed to handld refresh stat task", K(ret));
  }
  return ret;
}

int ObOptStatManager::get_column_stat(const uint64_t tenant_id,
                                      const uint64_t table_id,
                                      const ObIArray<int64_t> &part_ids,
                                      const ObIArray<uint64_t> &column_ids,
                                      ObIArray<ObOptColumnStatHandle> &handles)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat manager has not been initialized.", K(ret));
  } else {
    ObArenaAllocator arena(ObModIds::OB_SQL_PARSER);
    ObSEArray<const ObOptColumnStat::Key*, 4> keys;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < column_ids.count(); ++j) {
        void *ptr = NULL;
        if (OB_ISNULL(ptr = arena.alloc(sizeof(ObOptColumnStat::Key)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("memory is not enough", K(ret), K(ptr));
        } else {
          ObOptColumnStat::Key *key = new (ptr) ObOptColumnStat::Key(tenant_id,
                                                                     table_id,
                                                                     part_ids.at(i),
                                                                     column_ids.at(j));
          if (OB_FAIL(keys.push_back(key))) {
            LOG_WARN("failed to push back", K(ret));
          } else {/*do nothing*/}
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(stat_service_.get_column_stat(tenant_id, keys, handles))) {
      LOG_WARN("get column stat failed.", K(ret));
    } else {
      LOG_TRACE("succeed to get get column stat", K(keys), K(handles));
    }
  }
  return ret;
}

int ObOptStatManager::get_column_stat(const uint64_t tenant_id,
                                      const uint64_t ref_id,
                                      const int64_t part_id,
                                      const uint64_t col_id,
                                      ObOptColumnStatHandle &handle)
{
  ObOptColumnStat::Key key(tenant_id, ref_id, part_id, col_id);
  return get_column_stat(tenant_id, key, handle);
}

int ObOptStatManager::get_column_stat(const uint64_t tenant_id,
                                      const ObOptColumnStat::Key &key,
                                      ObOptColumnStatHandle &handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat manager has not been initialized.", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column stat key.", K(key), K(ret));
  } else if (OB_FAIL(stat_service_.get_column_stat(tenant_id, key, handle))) {
    LOG_WARN("get_column_stat failed.", K(ret));
  }
  return ret;
}

int ObOptStatManager::get_table_stat(const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     const ObIArray<int64_t> &part_ids,
                                     ObIArray<ObOptTableStat> &tstats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
    ObOptTableStat::Key key(tenant_id, table_id, part_ids.at(i));
    ObOptTableStat tstat;
    if (OB_FAIL(get_table_stat(tenant_id, key, tstat))) {
      LOG_WARN("failed to get table stat", K(ret));
    } else if (OB_FAIL(tstats.push_back(tstat))) {
      LOG_WARN("failed to push back table stat", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObOptStatManager::get_table_stat(const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     const ObIArray<int64_t> &part_ids,
                                     ObIArray<ObOptTableStatHandle> &handles)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
      ObOptTableStat::Key key(tenant_id, table_id, part_ids.at(i));
      ObOptTableStatHandle handle;
      if (OB_FAIL(stat_service_.get_table_stat(tenant_id, key, handle))) {
        LOG_WARN("get table stat failed", K(ret));
      } else if (OB_FAIL(handles.push_back(handle))) {
        LOG_WARN("failed to push back table stat", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObOptStatManager::get_table_stat(const uint64_t tenant_id,
                                     const ObOptTableStat::Key &key,
                                     ObOptTableStat &tstat)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret), K(inited_));
  } else if (OB_FAIL(stat_service_.get_table_stat(tenant_id, key, tstat))) {
    LOG_WARN("get table stat failed", K(ret));
  }
  return ret;
}

int ObOptStatManager::update_column_stat(share::schema::ObSchemaGetterGuard *schema_guard,
                                         const uint64_t tenant_id,
                                         ObMySQLTransaction &trans,
                                         const ObIArray<ObOptColumnStat *> &column_stats,
                                         bool only_update_col_stat /*default false*/,
                                         const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  int64_t current_time = ObTimeUtility::current_time();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(stat_service_.get_sql_service().update_column_stat(schema_guard,
                                                                        tenant_id,
                                                                        trans,
                                                                        column_stats,
                                                                        current_time,
                                                                        only_update_col_stat,
                                                                        false,
                                                                        print_params))) {
    LOG_WARN("failed to update column stat.", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptStatManager::update_table_stat(const uint64_t tenant_id,
                                        const ObOptTableStat *table_stats,
                                        const bool is_index_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(stat_service_.get_sql_service().update_table_stat(tenant_id,
                                                                       table_stats,
                                                                       is_index_stat))) {
    LOG_WARN("failed to update table stats", K(ret));
  }
  return ret;
}

int ObOptStatManager::update_table_stat(const uint64_t tenant_id,
                                        ObMySQLTransaction &trans,
                                        const ObIArray<ObOptTableStat*> &table_stats,
                                        const bool is_index_stat)
{
  int ret = OB_SUCCESS;
  int64_t current_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(stat_service_.get_sql_service().update_table_stat(tenant_id,
                                                                       trans,
                                                                       table_stats,
                                                                       current_time,
                                                                       is_index_stat))) {
    LOG_WARN("failed to update table stats", K(ret));
  }
  return ret;
}

int ObOptStatManager::delete_table_stat(uint64_t tenant_id,
                                        const uint64_t ref_id,
                                        int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 1> part_ids;
  bool cascade_column = true;
  return delete_table_stat(tenant_id, ref_id, part_ids, cascade_column, affected_rows);
}

int ObOptStatManager::delete_table_stat(uint64_t tenant_id,
                                        const uint64_t ref_id,
                                        const ObIArray<int64_t> &part_ids,
                                        const bool cascade_column,
                                        int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(stat_service_.get_sql_service().delete_table_stat(tenant_id,
                                                                       ref_id,
                                                                       part_ids,
                                                                       cascade_column,
                                                                       affected_rows))) {
    LOG_WARN("failed to delete table stat", K(ret));
  }
  return ret;
}

int ObOptStatManager::delete_column_stat(const uint64_t tenant_id,
                                         const uint64_t ref_id,
                                         const ObIArray<uint64_t> &column_ids,
                                         const ObIArray<int64_t> &part_ids,
                                         const bool only_histogram)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(stat_service_.get_sql_service().delete_column_stat(
                       tenant_id, ref_id, column_ids, part_ids, only_histogram))) {
    LOG_WARN("failed to delete column stat", K(ret));
  }
  return ret;
}

int ObOptStatManager::erase_column_stat(const ObOptColumnStat::Key &key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(stat_service_.erase_column_stat(key))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to erase column stat", K(ret));
    } else {
      ret = OB_SUCCESS;
      LOG_TRACE("failed to erase column stat", K(key));
    }
  }
  return ret;
}

int ObOptStatManager::erase_table_stat(const ObOptTableStat::Key &key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(stat_service_.erase_table_stat(key))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to erase table stat", K(ret));
    } else {
      LOG_TRACE("erase table stat failed", K(key));
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObOptStatManager::batch_write(share::schema::ObSchemaGetterGuard *schema_guard,
                                  const uint64_t tenant_id,
                                  ObMySQLTransaction &trans,
                                  ObIArray<ObOptTableStat *> &table_stats,
                                  ObIArray<ObOptColumnStat *> &column_stats,
                                  const int64_t current_time,
                                  const bool is_index_stat,
                                  const bool is_history_stat,
                                  const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (!table_stats.empty() &&
             OB_FAIL(stat_service_.get_sql_service().update_table_stat(
                                                    tenant_id,
                                                    trans,
                                                    table_stats,
                                                    current_time,
                                                    is_index_stat,
                                                    is_history_stat))) {
    LOG_WARN("failed to update table stats", K(ret));
  } else if (!column_stats.empty() &&
             OB_FAIL(stat_service_.get_sql_service().update_column_stat(schema_guard,
                                                                        tenant_id,
                                                                        trans,
                                                                        column_stats,
                                                                        current_time,
                                                                        false,
                                                                        is_history_stat,
                                                                        print_params))) {
    LOG_WARN("failed to update coumn stats", K(ret));
  }
  return ret;
}


int ObOptStatManager::handle_refresh_stat_task(const obrpc::ObUpdateStatCacheArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = arg.table_id_;
 for (int64_t i = 0; OB_SUCC(ret) && i < arg.partition_ids_.count(); ++i) {
    ObOptTableStat::Key table_key(arg.tenant_id_,
                                  table_id,
                                  arg.partition_ids_.at(i));
    if (OB_FAIL(erase_table_stat(table_key))) {
      LOG_WARN("update table statistics failed", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < arg.column_ids_.count(); ++j) {
      ObOptColumnStat::Key key(arg.tenant_id_,
                               table_id,
                               arg.partition_ids_.at(i),
                               arg.column_ids_.at(j));
      if (OB_FAIL(erase_column_stat(key))) {
        LOG_WARN("update column statistics failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !arg.no_invalidate_) {
    if (OB_FAIL(invalidate_plan(arg.tenant_id_, table_id))) {
      LOG_WARN("failed to invalidate plan", K(ret));
    }
  }
  return ret;
}

int ObOptStatManager::invalidate_plan(const uint64_t tenant_id, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    sql::ObPlanCache *pc = MTL(sql::ObPlanCache*);

    if (OB_FAIL(pc->evict_plan(table_id))) {
      LOG_WARN("failed to evict plan", K(ret));
      // use OB_SQL_PC_NOT_EXIST represent evict plan failed
      ret = OB_SQL_PC_NOT_EXIST;
    }
  }
  return ret;
}

int ObOptStatManager::erase_table_stat(const uint64_t tenant_id,
                                       const uint64_t table_id,
                                       const ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
    ObOptTableStat::Key key(tenant_id, table_id, part_ids.at(i));
    if (OB_FAIL(erase_table_stat(key))) {
      LOG_WARN("failed to erase table stat", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObOptStatManager::erase_column_stat(const uint64_t tenant_id,
                                        const uint64_t table_id,
                                        const ObIArray<int64_t> &part_ids,
                                        const ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < column_ids.count(); ++j) {
      ObOptColumnStat::Key key(tenant_id, table_id, part_ids.at(i), column_ids.at(j));
      if (OB_FAIL(erase_column_stat(key))) {
        LOG_WARN("failed to erase column stat", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObOptStatManager::get_default_data_size()
{
  return OB_EST_DEFAULT_DATA_SIZE;
}

int64_t ObOptStatManager::get_default_avg_row_size()
{
  return DEFAULT_ROW_SIZE;
}

int64_t ObOptStatManager::get_default_table_row_count()
{
  return DEFAULT_TABLE_ROW_COUNT;
}

int ObOptStatManager::check_opt_stat_validity(sql::ObExecContext &ctx,
                                              const uint64_t tenant_id,
                                              const uint64_t table_ref_id,
                                              const ObIArray<int64_t> &part_ids,
                                              bool &is_opt_stat_valid)
{
  int ret = OB_SUCCESS;
  is_opt_stat_valid = false;
  bool is_valid = false;
  if (OB_ISNULL(ctx.get_virtual_table_ctx().schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx.get_virtual_table_ctx().schema_guard_));
  } else if (OB_FAIL(check_stat_tables_ready(*ctx.get_virtual_table_ctx().schema_guard_, tenant_id, is_valid))) {
    LOG_WARN("failed to check stat tables ready", K(ret));
  } else if (!is_valid) {
    //do nothing
  } else if (OB_FAIL(ObDbmsStatsUtils::check_is_stat_table(*ctx.get_virtual_table_ctx().schema_guard_,
                                                           tenant_id, table_ref_id, is_valid))) {
    LOG_WARN("failed to check is stat table", K(ret));
  } else if (!is_valid) {
    //do nothing
  } else if (!part_ids.empty()) {
    is_opt_stat_valid = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_opt_stat_valid && i < part_ids.count(); ++i) {
      ObOptTableStat::Key key(tenant_id, table_ref_id, part_ids.at(i));
      ObOptTableStat opt_stat;
      if (OB_FAIL(get_table_stat(tenant_id, key, opt_stat))) {
        LOG_WARN("failed to get table stats", K(ret));
      } else if (opt_stat.get_last_analyzed() > 0) {
        //do nothing
      } else {
        is_opt_stat_valid = false;
      }
    }
  }
  return ret;
}

int ObOptStatManager::check_opt_stat_validity(sql::ObExecContext &ctx,
                                              const uint64_t tenant_id,
                                              const uint64_t tab_ref_id,
                                              const int64_t global_part_id,
                                              bool &is_opt_stat_valid)
{
  int ret = OB_SUCCESS;
  is_opt_stat_valid = false;
  ObSEArray<int64_t, 1> part_ids;
  if (OB_FAIL(part_ids.push_back(global_part_id))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(check_opt_stat_validity(ctx, tenant_id, tab_ref_id, part_ids, is_opt_stat_valid))) {
    LOG_WARN("failed to check opt stat validity", K(ret));
  }
  return ret;
}

int ObOptStatManager::get_table_stat(const uint64_t tenant_id,
                                     const uint64_t table_ref_id,
                                     const int64_t part_id,
                                     const double scale_ratio,
                                     ObGlobalTableStat &stat)
{
  int ret = OB_SUCCESS;
  ObOptTableStat::Key key(tenant_id, table_ref_id, part_id);
  ObOptTableStat opt_stat;
  if (OB_FAIL(get_table_stat(tenant_id, key, opt_stat))) {
    LOG_WARN("failed to get table stat", K(ret));
  } else if (opt_stat.get_last_analyzed() > 0) {
    stat.add(opt_stat.get_row_count() * scale_ratio,
             opt_stat.get_avg_row_size(),
             opt_stat.get_row_count() * opt_stat.get_avg_row_size() * scale_ratio,
             opt_stat.get_macro_block_num() * scale_ratio,
             opt_stat.get_micro_block_num() * scale_ratio);
    stat.set_last_analyzed(opt_stat.get_last_analyzed());
  }
  return ret;
}

int ObOptStatManager::get_table_stat(const uint64_t tenant_id,
                                     const uint64_t tab_ref_id,
                                     const ObIArray<int64_t> &part_ids,
                                     const ObIArray<int64_t> &global_part_ids,
                                     const double scale_ratio,
                                     ObGlobalTableStat &stat)
{
  int ret = OB_SUCCESS;
  bool use_global_stat_instead = !global_part_ids.empty();
  const ObIArray<int64_t> &opt_part_ids = use_global_stat_instead ? global_part_ids : part_ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < opt_part_ids.count(); ++i) {
    if (OB_FAIL(get_table_stat(tenant_id, tab_ref_id, opt_part_ids.at(i), scale_ratio, stat))) {
      LOG_WARN("failed to get table stat", K(ret));
    }
  }
  LOG_TRACE("succeed to get table stat", K(tab_ref_id), K(part_ids), K(global_part_ids),
                                         K(scale_ratio), K(scale_ratio), K(stat));
  return ret;
}

int ObOptStatManager::get_column_stat(const uint64_t tenant_id,
                                      const uint64_t tab_ref_id,
                                      const ObIArray<int64_t> &part_ids,
                                      const uint64_t column_id,
                                      const ObIArray<int64_t> &global_part_ids,
                                      const int64_t row_cnt,
                                      const double scale_ratio,
                                      ObGlobalColumnStat &stat,
                                      ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 1> cids;
  ObGlobalMinEval min_eval;
  ObGlobalMaxEval max_eval;
  ObGlobalNullEval null_eval;
  ObGlobalAvglenEval avglen_eval;
  ObGlobalNdvEval ndv_eval;
  ObArray<ObOptColumnStatHandle> new_handles;
  bool use_global_stat_instead = !global_part_ids.empty();
  if (OB_UNLIKELY(scale_ratio < 0.0 || scale_ratio > 1.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(scale_ratio), K(ret));
  } else if (OB_FAIL(cids.push_back(column_id))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(get_column_stat(tenant_id, tab_ref_id, use_global_stat_instead ? global_part_ids : part_ids,
                                     cids, new_handles))) {
    LOG_WARN("failed to get opt column stats", K(ret), K(global_part_ids), K(part_ids), K(cids));
  } else if (!new_handles.empty()) {
    bool all_column_stat_valid = true;
    for (int64_t i = 0; OB_SUCC(ret) && all_column_stat_valid && i < new_handles.count(); ++i) {
      const ObOptColumnStat *opt_col_stat = NULL;
      if (OB_ISNULL(opt_col_stat = new_handles.at(i).stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cache value is null", K(ret));
      } else if (opt_col_stat->get_last_analyzed() > 0) {
        ndv_eval.add(opt_col_stat->get_num_distinct(), opt_col_stat->get_llc_bitmap());
        null_eval.add(opt_col_stat->get_num_null());
        avglen_eval.add(opt_col_stat->get_avg_len());
        // a partition has min/max values only when it contains a valid value in the other word, ndv is not zero
        if (alloc != NULL && opt_col_stat->get_num_distinct() != 0) {
          min_eval.add(opt_col_stat->get_min_value());
          max_eval.add(opt_col_stat->get_max_value());
        }
      } else {
        all_column_stat_valid = false;
        LOG_TRACE("not all column stats are valid, replace with default column stats default",
                                                                             K(*opt_col_stat));
      }
    }
    if (OB_SUCC(ret) && all_column_stat_valid) {
      stat.null_val_ = null_eval.get() * scale_ratio;
      stat.avglen_val_ = avglen_eval.get();
      stat.ndv_val_ = !use_global_stat_instead ? ndv_eval.get() :
                        ObOptSelectivity::scale_distinct(row_cnt, row_cnt / scale_ratio, ndv_eval.get());
      if (NULL != alloc) {
        if (min_eval.is_valid() && OB_FAIL(ob_write_obj(*alloc, min_eval.get(), stat.min_val_))) {
          LOG_WARN("failed to deep copy min obj", K(ret));
        } else if (max_eval.is_valid() && OB_FAIL(ob_write_obj(*alloc, max_eval.get(), stat.max_val_))) {
          LOG_WARN("failed to deep copy max obj", K(ret));
        }
      }
    }
  }
  LOG_TRACE("succeed to get column stat", K(tab_ref_id), K(part_ids), K(column_id), K(scale_ratio),
                                          K(use_global_stat_instead), K(row_cnt), K(stat));
  return ret;
}

int ObOptStatManager::get_table_rowcnt(const uint64_t tenant_id,
                                       const uint64_t table_id,
                                       const ObIArray<ObTabletID> &all_tablet_ids,
                                       const ObIArray<ObLSID> &all_ls_ids,
                                       int64_t &table_rowcnt)
{
  return stat_service_.get_table_rowcnt(tenant_id, table_id, all_tablet_ids, all_ls_ids, table_rowcnt);
}

//we need check the stat tables are valid, now we only check the stat table are exist. in some situation,
//stat tables maybe not exist, such as the core table is created fist, and execute relation query, but
//the stat tables are not created.
int ObOptStatManager::check_stat_tables_ready(share::schema::ObSchemaGetterGuard &schema_guard,
                                              const uint64_t tenant_id,
                                              bool &are_stat_tables_ready)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = NULL;
  are_stat_tables_ready = false;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                            share::OB_ALL_TABLE_STAT_TID,
                                            table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_schema));
  } else if (OB_ISNULL(table_schema)) {
    //do nothing
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                   share::OB_ALL_COLUMN_STAT_TID,
                                                   table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_schema));
  } else if (OB_ISNULL(table_schema)) {
    //do nothing
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                   share::OB_ALL_HISTOGRAM_STAT_TID,
                                                   table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_schema));
  } else if (OB_ISNULL(table_schema)) {
    //do nothing
  } else {
    are_stat_tables_ready = true;
  }
  return ret;
}

int ObOptStatManager::get_ds_stat(const ObOptDSStat::Key &key,
                                  ObOptDSStatHandle &ds_stat_handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(stat_service_.get_ds_stat(key, ds_stat_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get ds stat failed", K(ret));
    }
  } else if (OB_ISNULL(ds_stat_handle.stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ds_stat_handle.stat_));
  } else {
    LOG_TRACE("succeed to get ds stat", KPC(ds_stat_handle.stat_));
  }
  return ret;
}

int ObOptStatManager::add_ds_stat_cache(const ObOptDSStat::Key &key,
                                        const ObOptDSStat &value,
                                        ObOptDSStatHandle &ds_stat_handle)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(stat_service_.add_ds_stat_cache(key, value, ds_stat_handle))) {
    LOG_WARN("failed to add ds stat cache", K(ret));
  }
  return ret;
}

int ObOptStatManager::erase_ds_stat(const ObOptDSStat::Key &key)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(stat_service_.erase_ds_stat(key))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to erase ds stat", K(ret));
    } else {
      LOG_TRACE("erase ds stat failed", K(key));
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObOptStatManager::update_opt_stat_gather_stat(const ObOptStatGatherStat &gather_stat)
{
  return stat_service_.get_sql_service().update_opt_stat_gather_stat(gather_stat);
}

int ObOptStatManager::update_opt_stat_task_stat(const ObOptStatTaskInfo &task_info)
{
  return stat_service_.get_sql_service().update_opt_stat_task_stat(task_info);
}

}
}
