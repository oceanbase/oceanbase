/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include "share/stat/ob_opt_stat_manager.h"
#include "share/ob_cluster_version.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "sql/ob_sql_context.h"
#include "share/catalog/ob_external_catalog.h"
#include "share/external_table/ob_external_table_utils.h"
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
  } else if (OB_FAIL(cat_stat_service_.init(proxy, config))) {
    LOG_WARN("failed to init cat stat service", K(ret));
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
  if (analyze_arg.update_system_stats_only_) {
    if (OB_FAIL(handle_refresh_system_stat_task(analyze_arg))) {
      LOG_WARN("failed to handle refresh system stat cache", K(ret));
    }
  } else if (OB_FAIL(handle_refresh_stat_task(analyze_arg))) {
    LOG_WARN("failed to handld refresh stat task", K(ret));
  }
  return ret;
}

int ObOptStatManager::add_refresh_catalog_stat_task(
    const obrpc::ObUpdateCatalogStatCacheArg &analyze_arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(handle_refresh_catalog_stat_task(analyze_arg))) {
    LOG_WARN("failed to handle refresh catalog stat task", K(ret));
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
  const static int64_t MAX_BATCH_SIZE = 1000;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat manager has not been initialized.", K(ret));
  } else {
    ObArenaAllocator arena("ObGetColStat", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
    ObSEArray<ObOptColumnStatHandle, 4> tmp_handles;
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
          } else if (MAX_BATCH_SIZE == keys.count()) {
            if (OB_FAIL(stat_service_.get_column_stat(tenant_id, keys, tmp_handles))) {
              LOG_WARN("get column stat failed.", K(ret));
            } else if (OB_FAIL(append(handles, tmp_handles))) {
              LOG_WARN("failed to append", K(ret));
            } else {
              arena.reuse();
              keys.reuse();
              tmp_handles.reuse();
            }
          }
        }
      }
    }
    if (OB_SUCC(ret) && !keys.empty()) {
      if (OB_FAIL(stat_service_.get_column_stat(tenant_id, keys, tmp_handles))) {
        LOG_WARN("get column stat failed.", K(ret));
      } else if (OB_FAIL(append(handles, tmp_handles))) {
        LOG_WARN("failed to append", K(ret));
      } else {
        arena.reuse();
      }
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
  ObSEArray<ObOptTableStatHandle, 4> handles;
  if (OB_FAIL(get_table_stat(tenant_id, table_id, part_ids, handles))) {
    LOG_WARN("failed to get table stat", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < handles.count(); ++i) {
    if (OB_ISNULL(handles.at(i).stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null.", K(ret), K(i), K(part_ids), K(handles.at(i)));
    } else if (OB_FAIL(tstats.push_back(*handles.at(i).stat_)))
      LOG_WARN("fail to push back.", K(ret));
  }
  return ret;
}

int ObOptStatManager::get_table_stat(const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     const ObIArray<int64_t> &part_ids,
                                     ObIArray<ObOptTableStatHandle> &handles)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObOptTableStat::Key *, 64> keys;
  ObArenaAllocator arena("ObTableColStat", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (part_ids.empty()) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
      ObOptTableStat::Key key(tenant_id, table_id, part_ids.at(i));
      void *ptr = NULL;
      if (OB_ISNULL(ptr = arena.alloc(sizeof(ObOptTableStat::Key)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("memory is not enough", K(ret), K(ptr));
      } else {
        ObOptTableStat::Key *key = new (ptr) ObOptTableStat::Key(tenant_id, table_id, part_ids.at(i));
        if (OB_FAIL(keys.push_back(key))) {
          LOG_WARN("push back error", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(stat_service_.batch_get_table_stats(tenant_id, keys, handles))) {
      LOG_WARN("get table stat failed", K(ret));
    }
  }
  arena.reuse();
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
                                         sqlclient::ObISQLConnection *conn,
                                         const ObIArray<ObOptColumnStat *> &column_stats,
                                         bool only_update_col_stat /*default false*/,
                                         const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  int64_t current_time = ObTimeUtility::current_time();
  ObArenaAllocator allocator("UpdateColStat", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(stat_service_.get_sql_service().update_column_stat(schema_guard,
                                                                        tenant_id,
                                                                        allocator,
                                                                        conn,
                                                                        column_stats,
                                                                        current_time,
                                                                        only_update_col_stat,
                                                                        print_params))) {
    LOG_WARN("failed to update column stat.", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptStatManager::update_catalog_table_stat(const uint64_t tenant_id,
                                                const share::ObOptCatalogTableStat *table_stat,
                                                const ObCatalogTableStatParam &table_param,
                                                sqlclient::ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(cat_stat_service_.get_sql_service()
                         .update_catalog_table_stat(tenant_id, table_stat, table_param, conn))) {
    LOG_WARN("failed to update external table stat", K(ret));
  } else {
    LOG_TRACE("successfully updated external table stat",
              K(table_param.table_identity_.catalog_id_),
              K(table_param.table_identity_.db_name_),
              K(table_param.table_identity_.tab_name_));

    // ========== TODO: Write statistics back to HMS Catalog ==========
    // After successfully writing to internal system table, optionally write back to HMS
    // This allows statistics to be synchronized with external catalog (Hive Metastore)
    //
    // Implementation plan:
    // 1. Add a configuration parameter (e.g., _enable_write_stats_to_hms) to control this feature
    // 2. Check if catalog type is HMS (table_param.catalog_id_ corresponds to HMS catalog)
    // 3. Get HMS catalog client from catalog manager
    // 4. Convert ObOptTableStat to HMS TableStatsRequest format
    // 5. Call HMS API to update table statistics
    // 6. Handle errors gracefully (log warning but don't fail the whole operation)
    //
    // Example implementation:
    // if (enable_write_stats_to_hms_ && is_hms_catalog(table_param.table_identity_.catalog_id_)) {
    //   int hms_ret = write_table_stats_to_hms(tenant_id, table_stat, table_param);
    //   if (OB_FAIL(hms_ret)) {
    //     LOG_WARN("failed to write table stats to HMS, but system table update succeeded",
    //              K(hms_ret), K(table_param.table_identity_.catalog_id_), K(table_param.table_identity_.db_name_),
    //              K(table_param.table_identity_.tab_name_));
    //     // Don't propagate error - system table update is more important
    //   } else {
    //     LOG_INFO("successfully wrote table stats to HMS", K(table_param.table_identity_.catalog_id_),
    //              K(table_param.table_identity_.db_name_), K(table_param.table_identity_.tab_name_));
    //   }
    // }
    // =================================================================
  }
  return ret;
}

int ObOptStatManager::update_catalog_column_stat(
    const uint64_t tenant_id,
    const ObIArray<share::ObOptCatalogColumnStat *> &column_stats,
    const ObIArray<ObCatalogColumnStatParam> &column_params,
    const ObCatalogTableStatParam &table_param,
    const ObObjPrintParams &print_params,
    share::schema::ObSchemaGetterGuard &schema_guard,
    sqlclient::ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ExtColStat", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(cat_stat_service_.get_sql_service().update_catalog_column_stat(tenant_id,
                                                                                    column_stats,
                                                                                    column_params,
                                                                                    table_param,
                                                                                    print_params,
                                                                                    schema_guard,
                                                                                    allocator,
                                                                                    conn))) {
    LOG_WARN("failed to update external column stat.", K(ret));
  } else {
    LOG_TRACE("successfully updated external column stats", K(column_stats.count()));

    // ========== TODO: Write column statistics back to HMS Catalog ==========
    // After successfully writing to internal system table, optionally write back to HMS
    // This allows column statistics to be synchronized with external catalog (Hive Metastore)
    //
    // Implementation plan:
    // 1. Add a configuration parameter (e.g., _enable_write_stats_to_hms) to control this feature
    // 2. Check if catalog type is HMS (table_param.catalog_id_ corresponds to HMS catalog)
    // 3. Get HMS catalog client from catalog manager
    // 4. Convert ObOptColumnStat array to HMS ColumnStatistics format
    // 5. Call HMS API to update column statistics (set_table_statistics_column or similar)
    // 6. Handle errors gracefully (log warning but don't fail the whole operation)
    //
    // Example implementation:
    // if (enable_write_stats_to_hms_ && is_hms_catalog(table_param.table_identity_.catalog_id_)) {
    //   int hms_ret = write_column_stats_to_hms(tenant_id, schema_guard,
    //                                            column_stats, column_params, table_param);
    //   if (OB_FAIL(hms_ret)) {
    //     LOG_WARN("failed to write column stats to HMS, but system table update succeeded",
    //              K(hms_ret), K(table_param.table_identity_.catalog_id_), K(table_param.table_identity_.db_name_),
    //              K(table_param.table_identity_.tab_name_), "column_count", column_stats.count());
    //     // Don't propagate error - system table update is more important
    //   } else {
    //     LOG_INFO("successfully wrote column stats to HMS", K(table_param.table_identity_.catalog_id_),
    //              K(table_param.table_identity_.db_name_), K(table_param.table_identity_.tab_name_),
    //              "column_count", column_stats.count());
    //   }
    // }
    // ========================================================================
  }
  return ret;
}

int ObOptStatManager::update_catalog_opt_stat_gather_stat(const ObOptStatGatherStat &gather_stat,
                                                          const uint64_t &catalog_id,
                                                          const ObString &db_name,
                                                          const ObString &table_name)
{
  return cat_stat_service_.update_catalog_opt_stat_gather_stat(gather_stat,
                                                               catalog_id,
                                                               db_name,
                                                               table_name);
}

int ObOptStatManager::update_table_stat(const uint64_t tenant_id,
                                        sqlclient::ObISQLConnection *conn,
                                        const ObOptTableStat *table_stats,
                                        const bool is_index_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(stat_service_.get_sql_service().update_table_stat(
                 tenant_id, conn, table_stats, is_index_stat))) {
    LOG_WARN("failed to update table stats", K(ret));
  }
  return ret;
}

int ObOptStatManager::update_table_stat(const uint64_t tenant_id,
                                        sqlclient::ObISQLConnection *conn,
                                        const ObIArray<ObOptTableStat*> &table_stats,
                                        const bool is_index_stat)
{
  int ret = OB_SUCCESS;
  int64_t current_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(stat_service_.get_sql_service().update_table_stat(
                 tenant_id, conn, table_stats, current_time, is_index_stat))) {
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
  int64_t degree = 1;
  return delete_table_stat(tenant_id, ref_id, part_ids, cascade_column, degree, affected_rows);
}

int ObOptStatManager::delete_table_stat(uint64_t tenant_id,
                                        const uint64_t ref_id,
                                        const ObIArray<int64_t> &part_ids,
                                        const bool cascade_column,
                                        const int64_t degree,
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
                                                                       degree,
                                                                       affected_rows))) {
    LOG_WARN("failed to delete table stat", K(ret));
  }
  return ret;
}

int ObOptStatManager::delete_column_stat(const uint64_t tenant_id,
                                         const uint64_t ref_id,
                                         const ObIArray<uint64_t> &column_ids,
                                         const ObIArray<int64_t> &part_ids,
                                         const bool only_histogram,
                                         const int64_t degree)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(stat_service_.get_sql_service().delete_column_stat(
                       tenant_id, ref_id, column_ids, part_ids, only_histogram, degree))) {
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

int ObOptStatManager::erase_catalog_table_stat(const share::ObOptCatalogTableStat::Key &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(cat_stat_service_.erase_catalog_table_stat(key))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to erase catalog table stat", K(ret), K(key));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObOptStatManager::erase_catalog_column_stat(const share::ObOptCatalogColumnStat::Key &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(cat_stat_service_.erase_catalog_column_stat(key))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to erase catalog column stat", K(ret), K(key));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObOptStatManager::batch_write(share::schema::ObSchemaGetterGuard *schema_guard,
                                  const uint64_t tenant_id,
                                  sqlclient::ObISQLConnection *conn,
                                  ObIArray<ObOptTableStat *> &table_stats,
                                  ObIArray<ObOptColumnStat *> &column_stats,
                                  const int64_t current_time,
                                  const bool is_index_stat,
                                  const ObObjPrintParams &print_params)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("UpdateColStat", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (!table_stats.empty() && OB_FAIL(stat_service_.get_sql_service().update_table_stat(
                                         tenant_id, conn, table_stats, current_time, is_index_stat))) {
    LOG_WARN("failed to update table stats", K(ret));
  } else if (!column_stats.empty() &&
             OB_FAIL(stat_service_.get_sql_service().update_column_stat(schema_guard,
                                                                        tenant_id,
                                                                        allocator,
                                                                        conn,
                                                                        column_stats,
                                                                        current_time,
                                                                        false,
                                                                        print_params))) {
    LOG_WARN("failed to update coumn stats", K(ret));
  }
  return ret;
}

int ObOptStatManager::update_stats_internal_stat(const uint64_t tenant_id,
                                                sqlclient::ObISQLConnection  *conn,
                                                uint64_t table_id,
                                                int64_t global_partition_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret));
  } else if (OB_FAIL(stat_service_.get_sql_service().update_stats_internal_stat(
                     tenant_id, conn, table_id, global_partition_id))) {
    LOG_WARN("failed to update stats internal stat", K(ret), K(tenant_id), K(table_id), K(global_partition_id));
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
    if (arg.standby_last_evict_time_ > 0) {
      ObOptStatMonitorManager *optstat_monitor_mgr = MTL(ObOptStatMonitorManager*);
      if (OB_ISNULL(optstat_monitor_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("optstat monitor mgr is NULL", K(ret), K(arg.tenant_id_));
      } else {
        optstat_monitor_mgr->set_last_standby_updated_time(arg.standby_last_evict_time_);
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(invalidate_plan(arg.tenant_id_, table_id, arg.plan_expired_before_))) {
      LOG_WARN("failed to invalidate plan", K(ret));
    }
  }
  return ret;
}

int ObOptStatManager::handle_refresh_system_stat_task(const obrpc::ObUpdateStatCacheArg &arg)
{
  int ret = OB_SUCCESS;
  ObOptSystemStat::Key key(arg.tenant_id_);
  if (OB_FAIL(stat_service_.erase_system_stat(key))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to erase system stat", K(ret));
    } else {
      ret = OB_SUCCESS;
      LOG_TRACE("failed to erase system stat", K(key));
    }
  }
  if (OB_SUCC(ret)) {
    MTL_SWITCH(arg.tenant_id_) {
      sql::ObPlanCache *pc = MTL(sql::ObPlanCache*);
      if (OB_FAIL(pc->flush_plan_cache())) {
        LOG_WARN("failed to evict plan", K(ret));
        // use OB_SQL_PC_NOT_EXIST represent evict plan failed
        ret = OB_SQL_PC_NOT_EXIST;
      }
    }
  }
  return ret;
}

int ObOptStatManager::handle_refresh_catalog_stat_task(
    const obrpc::ObUpdateCatalogStatCacheArg &arg)
{
  int ret = OB_SUCCESS;
  // Erase global table stat
  share::ObOptCatalogTableStat::Key table_key;
  table_key.tenant_id_ = arg.tenant_id_;
  table_key.catalog_id_ = arg.catalog_id_;
  table_key.database_name_ = arg.db_name_;
  table_key.table_name_ = arg.table_name_;
  table_key.partition_value_ = ObString("");

  if (OB_FAIL(erase_catalog_table_stat(table_key))) {
    LOG_WARN("failed to erase catalog table stat", K(ret), K(table_key));
  }

  // Erase partition table stats
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.partition_values_.count(); ++i) {
    table_key.partition_value_ = arg.partition_values_.at(i);
    if (OB_FAIL(erase_catalog_table_stat(table_key))) {
      LOG_WARN("failed to erase catalog partition table stat", K(ret), K(table_key));
    }
  }

  // Erase column stats (global + partitions)
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.column_names_.count(); ++i) {
    share::ObOptCatalogColumnStat::Key col_key;
    col_key.tenant_id_ = arg.tenant_id_;
    col_key.catalog_id_ = arg.catalog_id_;
    col_key.database_name_ = arg.db_name_;
    col_key.table_name_ = arg.table_name_;
    col_key.column_name_ = arg.column_names_.at(i);

    col_key.partition_value_ = ObString("");
    if (OB_FAIL(erase_catalog_column_stat(col_key))) {
      LOG_WARN("failed to erase catalog column stat", K(ret), K(col_key));
    }

    for (int64_t j = 0; OB_SUCC(ret) && j < arg.partition_values_.count(); ++j) {
      col_key.partition_value_ = arg.partition_values_.at(j);
      if (OB_FAIL(erase_catalog_column_stat(col_key))) {
        LOG_WARN("failed to erase catalog partition column stat", K(ret), K(col_key));
      }
    }
  }
  return ret;
}

int ObOptStatManager::invalidate_plan(const uint64_t tenant_id,
                                      const uint64_t table_id,
                                      int64_t plan_expired_before)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    sql::ObPlanCache *pc = MTL(sql::ObPlanCache*);
    if (OB_FAIL(pc->evict_plan(table_id, plan_expired_before))) {
      LOG_WARN("failed to evict plan", K(ret));
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
                                                           tenant_id, table_ref_id, true, is_valid))) {
    LOG_WARN("failed to check is stat table", K(ret));
  } else if (!is_valid) {
    //do nothing
  } else if (!part_ids.empty()) {
    is_opt_stat_valid = true;
    ObSEArray<ObOptTableStat, 4> stats;
    if (OB_FAIL(get_table_stat(tenant_id, table_ref_id, part_ids, stats))) {
      LOG_WARN("failed to get table stats", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && is_opt_stat_valid && i < stats.count(); ++i) {
        ObOptTableStat &opt_stat = stats.at(i);
        if (opt_stat.get_last_analyzed() > 0) {
          // do nothing
        } else {
          is_opt_stat_valid = false;
        }
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

int ObOptStatManager::check_system_stat_validity(sql::ObExecContext *ctx,
                                                 const uint64_t tenant_id,
                                                 bool &is_valid)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *table_schema = NULL;
  is_valid = false;
  if (OB_ISNULL(ctx) ||
      OB_ISNULL(ctx->get_virtual_table_ctx().schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ctx->get_virtual_table_ctx().schema_guard_->get_table_schema(
                                            tenant_id,
                                            share::OB_ALL_AUX_STAT_TID,
                                            table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(table_schema));
  } else if (OB_ISNULL(table_schema)) {
    //do nothing
  } else {
    is_valid = true;
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
             opt_stat.get_micro_block_num() * scale_ratio,
             opt_stat.get_sstable_row_count() * scale_ratio,
             opt_stat.get_memtable_row_count() * scale_ratio);
    stat.set_last_analyzed(opt_stat.get_last_analyzed());
    stat.set_stat_locked(opt_stat.is_locked());
    stat.set_stale_stats(opt_stat.is_stat_expired());
  }
  return ret;
}

int ObOptStatManager::get_table_stat(const uint64_t tenant_id,
                                     const uint64_t tab_ref_id,
                                     const ObIArray<int64_t> &part_ids,
                                     const double scale_ratio,
                                     ObGlobalTableStat &stat)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
    if (OB_FAIL(get_table_stat(tenant_id, tab_ref_id, part_ids.at(i), scale_ratio, stat))) {
      LOG_WARN("failed to get table stat", K(ret));
    }
  }
  LOG_TRACE("succeed to get table stat", K(tab_ref_id), K(part_ids),
                                         K(scale_ratio), K(scale_ratio), K(stat));
  return ret;
}

int ObOptStatManager::get_column_stat(const uint64_t tenant_id,
                                      const uint64_t tab_ref_id,
                                      const ObIArray<int64_t> &part_ids,
                                      const uint64_t column_id,
                                      const int64_t row_cnt,
                                      const double scale_ratio,
                                      ObGlobalColumnStat &stat,
                                      ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 1> cids;
  ObSEArray<ObGlobalColumnStat, 1> col_stats;
  if (OB_FAIL(cids.push_back(column_id))) {
    LOG_WARN("failed to push_back column stats", K(ret));
  } else if (OB_FAIL(batch_get_column_stats(
                 tenant_id, tab_ref_id, part_ids, cids, row_cnt, scale_ratio, col_stats, alloc))) {
    LOG_WARN("failed to get column stat", K(ret));
  } else if (col_stats.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get column stat", K(ret), K(cids.count()), K(col_stats.count()));
  } else {
    stat = col_stats.at(0);
  }

  return ret;
}

int ObOptStatManager::batch_get_column_stats(const uint64_t tenant_id,
                                             const uint64_t table_id,
                                             const ObIArray<int64_t> &part_ids,
                                             const ObIArray<uint64_t> &column_ids,
                                             const int64_t row_cnt,
                                             const double scale_ratio,
                                             ObIArray<ObGlobalColumnStat> &column_stats,
                                             ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  const static int64_t MAX_BATCH_SIZE = 1000;

  ObArray<ObOptColumnStatHandle> new_handles;
  ObArenaAllocator arena("ObGetColStat", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObArenaAllocator temp_allocator("ObGetColStat", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObSEArray<const ObOptColumnStat::Key *, 64> keys;
  hash::ObHashMap<uint64_t, ObGlobalAllColEvals *> column_id_col_evals;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("stat manager has not been initialized.", K(ret));
  } else if (!column_id_col_evals.created() &&
             OB_FAIL(column_id_col_evals.create(64, "colId2EvalsMap", "STATS_MANAGER"))) {
    LOG_WARN("create part_id_to_approx_part_map fail", K(ret));
  } else if (OB_UNLIKELY(scale_ratio < 0.0 || scale_ratio > 1.0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(scale_ratio), K(ret));
  } else if (OB_ISNULL(alloc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Null Allocator", K(scale_ratio), K(ret));
  } else if (OB_UNLIKELY(column_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error columns cannot be empty",
             K(ret),
             K(column_ids));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < column_ids.count(); ++j) {
      void *eval_ptr = NULL;
      if (OB_ISNULL(eval_ptr = temp_allocator.alloc(sizeof(ObGlobalAllColEvals)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("memory is not enough", K(ret), K(eval_ptr));
      } else {
        ObGlobalAllColEvals *all_col_evals = new (eval_ptr) ObGlobalAllColEvals();
        if (OB_FAIL(column_id_col_evals.set_refactored(column_ids.at(j), all_col_evals, true))) {
          LOG_WARN("column_id_col_evals set fail", K(ret), K(column_ids.at(j)));
        }
      }
    }

    int64_t start_pos = 0;
    for (int64_t j = 0; OB_SUCC(ret) && j < column_ids.count(); ++j) {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
        void *ptr = NULL;
        if (OB_ISNULL(ptr = arena.alloc(sizeof(ObOptColumnStat::Key)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("memory is not enough", K(ret), K(ptr));
        } else {
          ObOptColumnStat::Key *key =
              new (ptr) ObOptColumnStat::Key(tenant_id, table_id, part_ids.at(i), column_ids.at(j));
          if (keys.empty()) {
            start_pos = j;
          }
          if (OB_FAIL(keys.push_back(key))) {
            LOG_WARN("failed to push back", K(ret));
          } else if (MAX_BATCH_SIZE == keys.count() ||
                     (j == column_ids.count() - 1 && i == part_ids.count() - 1 && !keys.empty())) {
            if (OB_FAIL(stat_service_.get_column_stat(tenant_id, keys, new_handles))) {
              LOG_WARN("get column stat failed.", K(ret));
            } else if (OB_FAIL(trans_col_handle_to_evals(new_handles, column_id_col_evals))) {
              LOG_WARN("failed to gen opt column stats according to handles", K(ret));
            } else if (OB_FAIL(flush_evals(&temp_allocator, start_pos, j, column_ids, column_id_col_evals))) {
              LOG_WARN("failed to flush column all_evals", K(ret));
            } else {
              arena.reuse();
              keys.reuse();
              new_handles.reuse();
            }
          }
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(update_all_eval_to_stats(
                            row_cnt, scale_ratio, alloc, column_ids, column_id_col_evals, column_stats))) {
      LOG_WARN("failed to update column stats from column all_evals", K(ret));
    }

    int tmp_ret = OB_SUCCESS;
    if (column_id_col_evals.created() && OB_SUCCESS != (tmp_ret = column_id_col_evals.destroy())) {
      LOG_WARN("failed to destroy column_id_col_evals hash map", K(tmp_ret));
    }
    LOG_TRACE("succeed to get column stat",
              K(table_id),
              K(part_ids),
              K(column_ids),
              K(scale_ratio),
              K(row_cnt),
              K(stat));
  }
  return ret;
}

int ObOptStatManager::trans_col_handle_to_evals(
    const ObArray<ObOptColumnStatHandle> &stats_handles,
    hash::ObHashMap<uint64_t, ObGlobalAllColEvals *> &column_id_col_evals)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stats_handles.count(); ++i) {
    const ObOptColumnStat *opt_col_stat = stats_handles.at(i).stat_;
    if (OB_ISNULL(opt_col_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cache value is null", K(ret));
    } else {
      uint64_t column_id = opt_col_stat->get_column_id();
      ObGlobalAllColEvals *col_all_evals = NULL;
      if (OB_FAIL(column_id_col_evals.get_refactored(column_id, col_all_evals))) {
        LOG_WARN("get col all_evals failed", K(ret));
      } else if (OB_ISNULL(col_all_evals)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col all_evals cache value is null", K(ret));
      } else {
        col_all_evals->merge(*opt_col_stat);
      }
    }
  }

  return ret;
}

int ObOptStatManager::update_all_eval_to_stats(
    const int64_t row_cnt,
    const double scale_ratio,
    ObIAllocator *alloc,
    const ObIArray<uint64_t> &column_ids,
    const hash::ObHashMap<uint64_t, ObGlobalAllColEvals *> &column_id_col_evals,
    ObIArray<ObGlobalColumnStat> &column_stats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Null allocator", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    ObGlobalAllColEvals *col_all_evals = NULL;
    ObGlobalColumnStat opt_stats;
    uint64_t column_id = column_ids.at(i);
    if (OB_FAIL(column_id_col_evals.get_refactored(column_id, col_all_evals))) {
      LOG_WARN("get col all_evals  failed", K(ret));
    } else if (OB_ISNULL(col_all_evals)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col all_evals cache value is null", K(ret));
    } else if (col_all_evals->column_stat_valid_) {
      opt_stats.null_val_ = col_all_evals->null_eval_.get() * scale_ratio;
      opt_stats.avglen_val_ = col_all_evals->avglen_eval_.get();
      opt_stats.ndv_val_ = col_all_evals->ndv_eval_.get();
      if (scale_ratio < 1.0) {
        opt_stats.ndv_val_ = ObOptSelectivity::scale_distinct(row_cnt, row_cnt / scale_ratio, opt_stats.ndv_val_);
      }
      opt_stats.add_cg_blk_cnt(col_all_evals->cg_blk_eval_.get_cg_macro_blk_cnt() * scale_ratio,
                               col_all_evals->cg_blk_eval_.get_cg_micro_blk_cnt() * scale_ratio);
      if ((col_all_evals->cg_skip_rate_eval_.cg_micro_blk_cnt_ != 0) &&
          (col_all_evals->cg_skip_rate_eval_.cg_skip_rate_ != 0)) {
        opt_stats.cg_skip_rate_ = col_all_evals->cg_skip_rate_eval_.cg_skip_rate_ /
                                    col_all_evals->cg_skip_rate_eval_.cg_micro_blk_cnt_;
      }
      if (col_all_evals->min_eval_.is_valid() &&
          OB_FAIL(ob_write_obj(*alloc, col_all_evals->min_eval_.get(), opt_stats.min_val_))) {
        LOG_WARN("failed to deep copy min obj", K(ret));
      } else if (col_all_evals->max_eval_.is_valid() &&
                 OB_FAIL(ob_write_obj(*alloc, col_all_evals->max_eval_.get(), opt_stats.max_val_))) {
        LOG_WARN("failed to deep copy max obj", K(ret));
      }
    } else {
      LOG_TRACE(
          "not all column stats are valid, replace with default column stats default", K(column_id), K(opt_stats));
    }
    if (OB_SUCC(ret) && OB_FAIL(column_stats.push_back(opt_stats))) {
      LOG_WARN("failed to push-back col stats", K(ret), K(opt_stats));
    }
  }

  return ret;
}

int ObOptStatManager::flush_evals(ObIAllocator *alloc,
                                  const int64_t start_pos,
                                  const int64_t end_pos,
                                  const ObIArray<uint64_t> &column_ids,
                                  const hash::ObHashMap<uint64_t, ObGlobalAllColEvals *> &column_id_col_evals)
{
  int ret = OB_SUCCESS;
  ObGlobalAllColEvals *col_all_evals = NULL;
  if (OB_UNLIKELY(start_pos >= column_ids.count()) || end_pos >= column_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Illegal start_col_idx or end_start_col_idx", K(ret), K(start_pos), K(end_pos));
  }

  for (int64_t i = start_pos; OB_SUCC(ret) && i <= end_pos; ++i) {
    uint64_t column_id = column_ids.at(i);
    if (OB_FAIL(column_id_col_evals.get_refactored(column_id, col_all_evals))) {
      LOG_WARN("get col all_evals  failed", K(ret));
    } else if (OB_ISNULL(col_all_evals)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col all_evals cache value is null", K(ret));
    } else if (OB_FAIL(col_all_evals->flush(alloc))) {
      LOG_WARN("flush col all_evals failed", K(ret));
    }
  }
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
  int ret = OB_SUCCESS;
  if (OB_FAIL(stat_service_.get_sql_service().update_opt_stat_gather_stat(gather_stat))) {
    LOG_WARN("failed to update opt stat gather stat", K(ret));
  } else if (!gather_stat.need_flush_cache()) {
    // do nothing
  } else if (OB_FAIL(stat_service_.get_sql_service().insert_cache_invalidate_event(gather_stat))) {
    LOG_WARN("failed to insert cache invalidate event", K(ret));
  }
  return ret;
}

int ObOptStatManager::update_table_stat_failed_count(const uint64_t tenant_id,
                                                     const uint64_t table_id,
                                                     const ObIArray<int64_t> &part_ids,
                                                     int64_t &affected_rows)
{
  return stat_service_.get_sql_service().update_table_stat_failed_count(
      tenant_id, table_id, part_ids, affected_rows);
}

int ObOptStatManager::update_opt_stat_task_stat(const ObOptStatTaskInfo &task_info)
{
  return stat_service_.get_sql_service().update_opt_stat_task_stat(task_info);
}

int ObOptStatManager::get_system_stat(const uint64_t tenant_id,
                                     ObOptSystemStat &stat)
{
  int ret = OB_SUCCESS;
  ObOptSystemStat::Key key(tenant_id);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret), K(inited_));
  } else if (OB_FAIL(stat_service_.get_system_stat(tenant_id, key, stat))) {
    LOG_WARN("get system stat failed", K(ret));
  }
  return ret;
}

int ObOptStatManager::update_system_stats(const uint64_t tenant_id,
                                         const ObOptSystemStat *system_stats)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(stat_service_.get_sql_service().update_system_stats(tenant_id,
                                                                        system_stats))) {
    LOG_WARN("failed to update system stats", K(ret));
  }
  return ret;
}

int ObOptStatManager::delete_system_stats(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret), K(inited_));
  } else if (OB_FAIL(stat_service_.get_sql_service().delete_system_stats(tenant_id))) {
    LOG_WARN("delete system stat failed", K(ret));
  }
  return ret;
}

int ObOptStatManager::get_catalog_table_stat(const uint64_t tenant_id,
                                             const uint64_t ref_table_id,
                                             const ObIArray<ObString> &partition_values,
                                             const bool is_all_partitions_selected,
                                             sql::ObSqlSchemaGuard &schema_guard,
                                             ObLakeTableStat &stat)
{
  int ret = OB_SUCCESS;
  const share::ObILakeTableMetadata *lake_table_metadata = nullptr;
  stat.reset();
  bool found_stat = false;
  bool tried_hive_aggr_cache = false;
  bool fetched_from_source = false;
  bool cache_backfill = false;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("catalog table statistics not supported before cluster version 4.6.1.0",
             KR(ret), K(tenant_id), K(ref_table_id));
  } else if (OB_FAIL(schema_guard.get_lake_table_metadata(tenant_id, ref_table_id, lake_table_metadata))) {
    LOG_WARN("failed to get lake table metadata", K(ret), K(tenant_id), K(ref_table_id));
  } else if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lake table metadata is null", K(ret), K(tenant_id), K(ref_table_id));
  } else if (is_all_partitions_selected) {  // Hive 获取所有分区时的快速路径
    tried_hive_aggr_cache = true;
    ObOptCatalogTableStat::Key key;
    key.tenant_id_ = tenant_id;
    key.catalog_id_ = lake_table_metadata->catalog_id_;
    key.database_name_ = lake_table_metadata->namespace_name_;
    key.table_name_ = lake_table_metadata->table_name_;
    key.partition_value_ = ObString();
    if (OB_FAIL(cat_stat_service_.get_catalog_table_stat_from_cache(key,
                                                         stat,
                                                         found_stat))) {
      LOG_WARN("failed to get catalog table stat", K(ret));
    }
  }

  if (OB_SUCC(ret) && !found_stat) {
    const bool is_odps_global_stat = lake_table_metadata->get_format_type() == ObLakeTableFormat::ODPS;
    const bool is_iceberg_global_stat =
        lake_table_metadata->get_format_type() == ObLakeTableFormat::ICEBERG && partition_values.empty();
    // ODPS 和 Iceberg 是全表缓存
    // 全表缓存查询时用空分区值，回源时用真实分区值
    // 下层获取统计信息时 ODPS 和 Iceberg 写入的分区值都是空，Hive 会按实际分区值填写
    const int64_t part_cnt_eff = (partition_values.count() == 0 || is_odps_global_stat || is_iceberg_global_stat)
                                     ? 1
                                     : partition_values.count();  // 处理非分区表的情况, 统一代码逻辑
    ObSEArray<const ObOptCatalogTableStat::Key *, 64> keys;
    ObArenaAllocator arena("ObCatTabStat", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);

    for (int64_t pi = 0; OB_SUCC(ret) && pi < part_cnt_eff; ++pi) {
      void *ptr = NULL;
      const ObString pv = (partition_values.count() == 0 || is_odps_global_stat || is_iceberg_global_stat)
                              ? ObString()
                              : partition_values.at(pi);
      if (OB_ISNULL(ptr = arena.alloc(sizeof(ObOptCatalogTableStat::Key)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("memory is not enough", K(ret), K(ptr));
      } else {
        ObOptCatalogTableStat::Key *key = new (ptr) ObOptCatalogTableStat::Key();
        key->tenant_id_ = tenant_id;
        key->catalog_id_ = lake_table_metadata->catalog_id_;
        key->database_name_ = lake_table_metadata->namespace_name_;
        key->table_name_ = lake_table_metadata->table_name_;
        key->partition_value_ = pv;
        if (OB_FAIL(keys.push_back(key))) {
          LOG_WARN("push back error", K(ret), K(pi));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(cat_stat_service_.batch_get_catalog_table_stats(tenant_id,
                                                                       ref_table_id,
                                                                       keys,
                                                                       partition_values,
                                                                       schema_guard,
                                                                       stat))) {
      LOG_WARN("get table stat failed", K(ret));
    } else if (is_all_partitions_selected) {  // Hive 全表统计信息, 放一条聚合的结果在cache
      fetched_from_source = true;
      if (OB_FAIL(cat_stat_service_.put_catalog_aggr_table_stat_to_cache(
              tenant_id,
              lake_table_metadata->catalog_id_,
              lake_table_metadata->namespace_name_,
              lake_table_metadata->table_name_,
              stat))) {
        LOG_WARN("failed to put catalog single table stat to cache", K(ret));
      } else {
        cache_backfill = true;
      }
    } else {
      fetched_from_source = true;
    }

    arena.reuse();
  }

  if (OB_NOT_NULL(lake_table_metadata)) {
    LOG_TRACE("catalog stat table result",
             K(ret),
             K(tenant_id),
             K(ref_table_id),
             "format_type",
             lake_table_metadata->get_format_type(),
             K(is_all_partitions_selected),
             K(tried_hive_aggr_cache),
             "cache_hit",
             found_stat,
             K(fetched_from_source),
             K(cache_backfill),
             "partition_cnt",
             partition_values.count(),
             "row_cnt",
             stat.total_row_count_,
             "last_analyzed",
             stat.last_analyzed_);
  }

  return ret;
}

int ObOptStatManager::get_catalog_column_stat(ObIAllocator &alloc,
                                               const uint64_t tenant_id,
                                               const uint64_t ref_table_id,
                                               const ObIArray<ObString> &column_names,
                                               const ObIArray<ObString> &partition_values,
                                               const bool is_all_partitions_selected,
                                               sql::ObSqlSchemaGuard &schema_guard,
                                               const int64_t row_cnt,
                                               const double scale_ratio,
                                               ObIArray<ObLakeColumnStat*> &column_stats)
{
  int ret = OB_SUCCESS;
  const share::ObILakeTableMetadata *lake_table_metadata = nullptr;
  ObArenaAllocator tmp_alloc("ObCatColStat", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObSEArray<ObLakeColumnStat*, 16> cached_column_stats;
  ObSEArray<ObLakeColumnStat*, 16> fetched_column_stats;
  ObSEArray<ObString, 16> missed_columns;
  bool tried_hive_aggr_cache = false;
  bool fetched_from_source = false;
  int64_t stat_column_cnt = 0;
  int64_t cache_backfill_cnt = 0;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("catalog column statistics not supported before cluster version 4.6.1.0",
             KR(ret), K(tenant_id), K(ref_table_id));
  } else if (OB_FAIL(schema_guard.get_lake_table_metadata(tenant_id, ref_table_id, lake_table_metadata))) {
    LOG_WARN("failed to get lake table metadata", K(ret), K(tenant_id), K(ref_table_id));
  } else if (OB_ISNULL(lake_table_metadata)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lake table metadata is null", K(ret), K(tenant_id), K(ref_table_id));
  } else if (is_all_partitions_selected) {  // Hive 获取所有分区时的快速路径
    tried_hive_aggr_cache = true;
    ObSEArray<ObOptCatalogColumnStat::Key, 16> keys;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
      if (ObExternalTableUtils::is_hidden_external_column(column_names.at(i))) {
      } else {
        ObOptCatalogColumnStat::Key key;
        key.tenant_id_ = tenant_id;
        key.catalog_id_ = lake_table_metadata->catalog_id_;
        key.database_name_ = lake_table_metadata->namespace_name_;
        key.table_name_ = lake_table_metadata->table_name_;
        key.column_name_ = column_names.at(i);
        key.partition_value_ = ObString();
        if (OB_FAIL(keys.push_back(key))) {
          LOG_WARN("failed to push back key", K(ret), K(i));
        }
      }
    }

    if (OB_SUCC(ret) && !keys.empty() && OB_FAIL(cat_stat_service_.get_catalog_column_stat_from_cache(
            alloc,
            keys,
            cached_column_stats,
            missed_columns))) {
      LOG_WARN("failed to get catalog column stats", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObSEArray<const ObOptCatalogColumnStat::Key*, 16> keys;
    const bool is_odps_global_stat = lake_table_metadata->get_format_type() == ObLakeTableFormat::ODPS;
    const bool is_iceberg_global_stat =
        lake_table_metadata->get_format_type() == ObLakeTableFormat::ICEBERG && partition_values.empty();
    const ObIArray<ObString> &column_names_to_fetch
        = is_all_partitions_selected
              ? static_cast<const ObIArray<ObString>&>(missed_columns)
              : column_names;
    // ODPS 和 Iceberg 是全表缓存
    // 全表缓存查询时用空分区值，回源时用真实分区值
    // 下层获取统计信息时 ODPS 和 Iceberg 写入的分区值都是空，Hive 会按实际分区值填写
    const int64_t part_cnt_eff = (partition_values.count() == 0 || is_odps_global_stat || is_iceberg_global_stat)
                                   ? 1 : partition_values.count();
    for (int64_t pi = 0; OB_SUCC(ret) && pi < part_cnt_eff; ++pi) {
      const ObString pv = (partition_values.count() == 0 || is_odps_global_stat || is_iceberg_global_stat)
                              ? ObString()
                              : partition_values.at(pi);
      for (int64_t j = 0; OB_SUCC(ret) && j < column_names_to_fetch.count(); ++j) {
        if (ObExternalTableUtils::is_hidden_external_column(column_names_to_fetch.at(j))) {
          // do nothing
        } else {
          if (0 == pi) {
            ++stat_column_cnt;
          }
          void *ptr = nullptr;
          if (OB_ISNULL(ptr = tmp_alloc.alloc(sizeof(ObOptCatalogColumnStat::Key)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else {
            ObOptCatalogColumnStat::Key *key = new (ptr) ObOptCatalogColumnStat::Key();
            key->tenant_id_ = tenant_id;
            key->catalog_id_ = lake_table_metadata->catalog_id_;
            key->database_name_ = lake_table_metadata->namespace_name_;
            key->table_name_ = lake_table_metadata->table_name_;
            key->column_name_ = column_names_to_fetch.at(j);
            key->partition_value_ = pv;
            if (OB_FAIL(keys.push_back(key))) {
              LOG_WARN("failed to push back key", K(ret), K(pi), K(j));
            }
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (stat_column_cnt > 0
               && OB_FAIL(cat_stat_service_.batch_get_catalog_column_stats(tenant_id,
                                                                           ref_table_id,
                                                                           keys,
                                                                           column_names_to_fetch,  // 需要保证列统计信息的顺序
                                                                           partition_values,
                                                                           schema_guard,
                                                                           alloc,
                                                                           row_cnt,
                                                                           scale_ratio,
                                                                           fetched_column_stats))) {
      LOG_WARN("failed to get catalog column stats", K(ret), K(tenant_id), K(ref_table_id));
    } else {
      fetched_from_source = stat_column_cnt > 0;
      int64_t cached_idx = 0;
      int64_t fetched_idx = 0;
      int64_t missed_idx = 0;
      // is_all_partitions_selected 为 true 时，cached_column_stats 和 fetched_column_stats 各自是有序的，做一个归并
      // is_all_partitions_selected 为 false 时，fetched_column_stats 是有序的，直接使用
      for (int64_t j = 0; OB_SUCC(ret) && j < column_names.count(); ++j) {
        ObLakeColumnStat *column_stat = nullptr;
        if (ObExternalTableUtils::is_hidden_external_column(column_names.at(j))) {
          void *ptr = nullptr;
          if (OB_ISNULL(ptr = alloc.alloc(sizeof(ObLakeColumnStat)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else {
            column_stat = new (ptr) ObLakeColumnStat();
            column_stat->last_analyzed_ = 0;
          }
        } else if (is_all_partitions_selected
                   && missed_idx < missed_columns.count()
                   && column_names.at(j) == missed_columns.at(missed_idx)) {  // 通过column_names来判断应该选择cached_column_stats还是fetched_column_stats
          if (fetched_idx >= fetched_column_stats.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected fetched column stat index",
                     K(ret),
                     K(fetched_idx),
                     K(fetched_column_stats.count()),
                     K(j),
                     K(column_names.at(j)));
          } else {
            column_stat = fetched_column_stats.at(fetched_idx++);
            if (OB_FAIL(cat_stat_service_.put_catalog_aggr_column_stat_to_cache(  // 把全分区聚合的结果写一条到cache
                    tenant_id,
                    lake_table_metadata->catalog_id_,
                    lake_table_metadata->namespace_name_,
                    lake_table_metadata->table_name_,
                    column_names.at(j),
                    *column_stat))) {
              LOG_WARN("failed to put aggregated catalog column stat to cache",
                       K(ret),
                       K(tenant_id),
                       K(ref_table_id),
                       K(column_names.at(j)));
            } else {
              ++missed_idx;
              ++cache_backfill_cnt;
            }
          }
        } else if (is_all_partitions_selected) {
          if (cached_idx >= cached_column_stats.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected cached column stat index",
                     K(ret),
                     K(cached_idx),
                     K(cached_column_stats.count()),
                     K(j),
                     K(column_names.at(j)));
          } else {
            column_stat = cached_column_stats.at(cached_idx++);
          }
        } else {
          if (fetched_idx >= fetched_column_stats.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected fetched column stat index",
                     K(ret),
                     K(fetched_idx),
                     K(fetched_column_stats.count()),
                     K(j),
                     K(column_names.at(j)));
          } else {
            column_stat = fetched_column_stats.at(fetched_idx++);
          }
        }

        if (OB_SUCC(ret) && OB_FAIL(column_stats.push_back(column_stat))) {
          LOG_WARN("failed to push back column stat", K(ret));
        }
      }

      if (OB_SUCC(ret) && is_all_partitions_selected
          && (cached_idx != cached_column_stats.count()
              || fetched_idx != fetched_column_stats.count()
              || missed_idx != missed_columns.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column stat merge result",
                 K(ret),
                 K(cached_idx),
                 K(cached_column_stats.count()),
                 K(fetched_idx),
                 K(fetched_column_stats.count()),
                 K(missed_idx),
                 K(missed_columns.count()));
      } else if (OB_SUCC(ret) && !is_all_partitions_selected
                 && fetched_idx != fetched_column_stats.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected fetched column stat count",
                 K(ret),
                 K(fetched_idx),
                 K(fetched_column_stats.count()));
      }
    }
  }

  if (OB_NOT_NULL(lake_table_metadata)) {
    LOG_TRACE("catalog stat column result",
             K(ret),
             K(tenant_id),
             K(ref_table_id),
             "format_type",
             lake_table_metadata->get_format_type(),
             K(is_all_partitions_selected),
             K(tried_hive_aggr_cache),
             K(fetched_from_source),
             "requested_column_cnt",
             column_names.count(),
             "requested_stat_column_cnt",
             stat_column_cnt,
             "output_column_cnt",
             column_stats.count(),
             "cached_column_cnt",
             cached_column_stats.count(),
             "missed_column_cnt",
             missed_columns.count(),
             "fetched_column_cnt",
             fetched_column_stats.count(),
             K(cache_backfill_cnt));
  }
  return ret;
}

int ObOptStatManager::evict_all_opt_stat_kvcache(const uint64_t tenant_id,
                                                 int64_t &recent_update_time)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("optimizer statistics manager has not been initialized.", K(ret), K(inited_));
  } else if (OB_FAIL(stat_service_.evict_all_opt_stat_kvcache(tenant_id, recent_update_time))) {
    LOG_WARN("failed to evict all opt stat kvcache", K(ret));
  }
  return ret;
}

}
}
