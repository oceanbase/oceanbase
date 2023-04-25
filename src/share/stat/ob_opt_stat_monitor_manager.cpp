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
#include "share/stat/ob_opt_stat_monitor_manager.h"
#include "share/stat/ob_stat_define.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/thread/thread_mgr.h"
#include "sql/ob_sql_init.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_utils.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/ob_sql_client_decorator.h"
#include "storage/ob_locality_manager.h"

namespace oceanbase
{
namespace common
{
#define INSERT_COLUMN_USAGE "INSERT INTO __all_column_usage(tenant_id," \
                                                            "table_id," \
                                                            "column_id," \
                                                            "equality_preds," \
                                                            "equijoin_preds," \
                                                            "nonequijion_preds," \
                                                            "range_preds," \
                                                            "like_preds," \
                                                            "null_preds," \
                                                            "distinct_member," \
                                                            "groupby_member," \
                                                            "flags) VALUES" \

#define ON_DUPLICATE_UPDATE "ON DUPLICATE KEY UPDATE " \
            "equality_preds = equality_preds + if (values(flags) & 1, 1, 0)," \
            "equijoin_preds = equijoin_preds + if (values(flags) & 2, 1, 0)," \
            "nonequijion_preds = nonequijion_preds + if (values(flags) & 4, 1, 0)," \
            "range_preds = range_preds + if (values(flags) & 8, 1, 0)," \
            "like_preds = like_preds + if (values(flags) & 16, 1, 0)," \
            "null_preds = null_preds + if (values(flags) & 32, 1, 0)," \
            "distinct_member = distinct_member + if (values(flags) & 64, 1, 0)," \
            "groupby_member = groupby_member + if (values(flags) & 128, 1, 0)," \
            "flags = values(flags);"

#define SELECT_FROM_COLUMN_USAGE \
  "SELECT column_id, equality_preds, equijoin_preds, nonequijion_preds, range_preds, " \
         "like_preds, null_preds, distinct_member, groupby_member " \
  "FROM oceanbase.__all_column_usage " \
  "WHERE tenant_id = %lu and table_id = %lu and column_id in (%s);"

#define INSERT_MONITOR_MODIFIED \
  "INSERT INTO %s (tenant_id, table_id, tablet_id, inserts, updates, deletes) VALUES "

#define ON_DUPLICATE_UPDATE_MONITOR_MODIFIED \
  "ON DUPLICATE KEY UPDATE " \
  "inserts = inserts + values(inserts)," \
  "updates = updates + values(updates)," \
  "deletes = deletes + values(deletes);"

int ObOptStatMonitorFlushAllTask::init(int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObOptStatMonitorFlushAllTask init twice", K(ret));
  } else {
    is_inited_ = true;
    disable_timeout_check();
    if (OB_FAIL(TG_SCHEDULE(tg_id, *this, FLUSH_INTERVAL, true /*schedule repeatly*/))) {
      LOG_WARN("fail to schedule ObOptStatMonitorFlushAllTask", K(ret));
    }
  }
  return ret;
}

void ObOptStatMonitorFlushAllTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOptStatMonitorManager::get_instance().update_opt_stat_monitoring_info(false))) {
    LOG_WARN("failed to update column usage table", K(ret));
  }
}

int ObOptStatMonitorCheckTask::init(int tg_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObOptStatMonitorCheckTask init twice", K(ret));
  } else {
    is_inited_ = true;
    disable_timeout_check();
    if (OB_FAIL(TG_SCHEDULE(tg_id, *this, CHECK_INTERVAL, true /*schedule repeatly*/))) {
      LOG_WARN("fail to schedule ObOptStatMonitorCheckTask", K(ret));
    }
  }
  return ret;
}

void ObOptStatMonitorCheckTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOptStatMonitorManager::get_instance().update_opt_stat_monitoring_info(true))) {
    LOG_WARN("failed to update column usage table", K(ret));
  }
}

int ObOptStatMonitorManager::init(ObMySQLProxy *mysql_proxy)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(OB_SERVER_TENANT_ID, "DmlStatsHashMap");
  SET_USE_500(attr);
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("column usage manager has already been initialized.", K(ret));
  } else if (OB_ISNULL(mysql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null mysql proxy", K(ret));
  } else if (OB_FAIL(column_usage_maps_.create(100, SET_USE_500("ColUsagHashMap")))) {
    LOG_WARN("failed to create column usage maps", K(ret));
  } else if (OB_FAIL(dml_stat_maps_.create(100, attr))) {
    LOG_WARN("failed to create dml stats maps", K(ret));
  } else if (OB_FAIL(flush_all_task_.init(lib::TGDefIDs::ServerGTimer))) {
    LOG_WARN("failed to init column usage task", K(ret));
  } else if (OB_FAIL(check_task_.init(lib::TGDefIDs::ServerGTimer))) {
    LOG_WARN("failed to init column usage task", K(ret));
  } else {
    inited_ = true;
    mysql_proxy_ = mysql_proxy;
  }
  return ret;
}

void ObOptStatMonitorManager::destroy()
{
  if (inited_) {
    inited_ = false;
    for (auto iter = column_usage_maps_.begin(); iter != column_usage_maps_.end(); ++iter) {
      if (OB_ISNULL(iter->second)) {
        BACKTRACE_RET(ERROR, OB_ERR_UNEXPECTED, true, "column usage map is null");
      } else {
        iter->second->destroy();
      }
    }
    for (auto iter = dml_stat_maps_.begin(); iter != dml_stat_maps_.end(); ++iter) {
      if (OB_ISNULL(iter->second)) {
        BACKTRACE_RET(ERROR, OB_ERR_UNEXPECTED, true, "dml stats map is null");
      } else {
        iter->second->destroy();
      }
    }
  }
}

ObOptStatMonitorManager &ObOptStatMonitorManager::get_instance()
{
  static ObOptStatMonitorManager instance_;
  return instance_;
}

int ObOptStatMonitorManager::flush_database_monitoring_info(sql::ObExecContext &ctx,
                                                            const bool is_flush_col_usage,
                                                            const bool is_flush_dml_stat,
                                                            const bool ignore_failed)
{
  int ret = OB_SUCCESS;
  int64_t timeout = -1;
  ObSEArray<ObServerLocality, 4> all_server_arr;
  bool has_read_only_zone = false; // UNUSED;
  if (OB_ISNULL(ctx.get_my_session()) ||
      OB_ISNULL(GCTX.srv_rpc_proxy_) ||
      OB_ISNULL(GCTX.locality_manager_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(GCTX.srv_rpc_proxy_),
                                    K(GCTX.locality_manager_), K(ctx.get_my_session()));
  } else {
    obrpc::ObFlushOptStatArg arg(ctx.get_my_session()->get_effective_tenant_id(),
                                 is_flush_col_usage,
                                 is_flush_dml_stat);
    if (OB_FAIL(GCTX.locality_manager_->get_server_locality_array(all_server_arr,
                                                                  has_read_only_zone))) {
      LOG_WARN("fail to get server locality", K(ret));
    } else {
      ObSEArray<ObServerLocality, 4> failed_server_arr;
      for (int64_t i = 0; OB_SUCC(ret) && i < all_server_arr.count(); i++) {
        if (!all_server_arr.at(i).is_active()
            || ObServerStatus::OB_SERVER_ACTIVE != all_server_arr.at(i).get_server_status()
            || 0 == all_server_arr.at(i).get_start_service_time()
            || 0 != all_server_arr.at(i).get_server_stop_time()) {
        //server may not serving
        } else if (0 >= (timeout = THIS_WORKER.get_timeout_remain())) {
          ret = OB_TIMEOUT;
          LOG_WARN("query timeout is reached", K(ret), K(timeout));
        } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(all_server_arr.at(i).get_addr())
                                                  .timeout(timeout)
                                                  .by(ctx.get_my_session()->get_rpc_tenant_id())
                                                  .flush_local_opt_stat_monitoring_info(arg))) {
          LOG_WARN("failed to flush opt stat monitoring info caused by unknow error",
                                                K(ret), K(all_server_arr.at(i).get_addr()), K(arg));
          //ignore flush cache failed, TODO @jiangxiu.wt can aduit it and flush cache manually later.
          if (ignore_failed && OB_FAIL(failed_server_arr.push_back(all_server_arr.at(i)))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
      }
      LOG_TRACE("flush database monitoring info cache", K(arg), K(failed_server_arr), K(all_server_arr));
    }
  }
  return ret;
}

int ObOptStatMonitorManager::erase_opt_stat_monitoring_info_map(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(erase_column_usage_map(tenant_id))) {
    LOG_WARN("failed to erase column usage map", K(ret), K(tenant_id));
  } else if (OB_FAIL(erase_dml_stat_map(tenant_id))) {
    LOG_WARN("failed to erase dml stat map", K(ret), K(tenant_id));
  } else {/*do nothing*/}
  return ret;
}


int ObOptStatMonitorManager::erase_column_usage_map(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ColumnUsageMap *col_map = nullptr;
  if (OB_FAIL(column_usage_maps_.get_refactored(tenant_id, col_map))) {
    LOG_WARN("failed to get column usage map", K(ret));
  } else if (OB_FAIL(column_usage_maps_.erase_refactored(tenant_id))) {
    LOG_WARN("failed to erase column usage map", K(ret));
  } else {
    col_map->destroy();
    ob_free(col_map);
  }
  return ret;
}

int ObOptStatMonitorManager::erase_dml_stat_map(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  DmlStatMap *dml_stat_map= nullptr;
  if (OB_FAIL(dml_stat_maps_.get_refactored(tenant_id, dml_stat_map))) {
    LOG_WARN("failed to get dml stat map", K(ret));
  } else if (OB_FAIL(dml_stat_maps_.erase_refactored(tenant_id))) {
    LOG_WARN("failed to erase dml stat map", K(ret));
  } else {
    dml_stat_map->destroy();
    ob_free(dml_stat_map);
  }
  return ret;
}

int ObOptStatMonitorManager::update_local_cache(uint64_t tenant_id,
                                                common::ObIArray<ColumnUsageArg> &args)
{
  int ret = OB_SUCCESS;
  ReadMapAtomicOp atomic_op(&args);
  ObMemAttr attr(OB_SERVER_TENANT_ID, "ColUsagHashMap");
  SET_USE_500(attr);
  if (GCTX.is_standby_cluster()) {
    // standby cluster can't write __all_column_usage, so do not need to update local update
  } else if (OB_FAIL(column_usage_maps_.read_atomic(tenant_id, atomic_op))) {
    if (OB_HASH_NOT_EXIST == ret) {//not exists such tenant id map, need alloc new map
      ColumnUsageMap *col_map = NULL;
      ColumnUsageMap *tmp_col_map = NULL;
      void *buff = ob_malloc(sizeof(ColumnUsageMap), attr);
      if (OB_ISNULL(buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else if (OB_FALSE_IT(col_map = new(buff)ColumnUsageMap())) {
      } else if (OB_FAIL(col_map->create(10000, attr))) {
        LOG_WARN("failed to create column usage map", K(ret));
      } else if (OB_FAIL(column_usage_maps_.set_refactored(tenant_id, col_map))) {
        // set refacter failed, may created by other thread
        if (OB_SUCCESS == column_usage_maps_.get_refactored(tenant_id, tmp_col_map)) {
          LOG_TRACE("get column usage map succeed", K(tenant_id), K(tmp_col_map));
        } else {
          LOG_WARN("get column usage map failed", K(tenant_id), K(tmp_col_map));
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(col_map)) {//free unused memory
        col_map->~ColumnUsageMap();
        ob_free(buff);
        buff = NULL;
        col_map = tmp_col_map;
      }
      if (OB_NOT_NULL(col_map)) {
        //arrive at here, indicates get a valid dml_stat_map, we need reset error code, and atomic update values
        if (OB_FAIL(column_usage_maps_.read_atomic(tenant_id, atomic_op))) {
          LOG_WARN("failed to atomic refactored", K(ret), K(tenant_id));
        }
      }
    } else {
      LOG_WARN("failed to atomic refactored", K(ret));
    }
  }
  return ret;
}

int ObOptStatMonitorManager::update_local_cache(uint64_t tenant_id, ObOptDmlStat &dml_stat)
{
  int ret = OB_SUCCESS;
  ReadMapAtomicOp atomic_op(dml_stat);
  ObMemAttr attr(OB_SERVER_TENANT_ID, "DmlStatsHashMap");
  SET_USE_500(attr);
  if (GCTX.is_standby_cluster()) {
    // standby cluster can't write __all_monitor_modified, so do not need to update local update
  } else if (OB_FAIL(dml_stat_maps_.read_atomic(tenant_id, atomic_op))) {
    if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {//not exists such tenant id map, need alloc new map
      ret = OB_SUCCESS;
      DmlStatMap *dml_stat_map = NULL;
      DmlStatMap *tmp_dml_stat_map = NULL;
      void *buff = ob_malloc(sizeof(DmlStatMap), attr);
      if (OB_ISNULL(buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", K(ret));
      } else if (OB_FALSE_IT(dml_stat_map = new(buff)DmlStatMap())) {
      } else if (OB_FAIL(dml_stat_map->create(10000, attr))) {
        LOG_WARN("failed to create column usage map", K(ret));
      } else if (OB_FAIL(dml_stat_maps_.set_refactored(tenant_id, dml_stat_map))) {
        // set refacter failed, may created by other thread
        if (OB_SUCCESS == dml_stat_maps_.get_refactored(tenant_id, tmp_dml_stat_map)) {
          LOG_TRACE("get dml stats succeed", K(tenant_id), K(tmp_dml_stat_map));
        } else {
          LOG_WARN("get dml stats failed", K(tenant_id), K(tmp_dml_stat_map), K(ret));
        }
      }
      if (OB_FAIL(ret) && OB_NOT_NULL(dml_stat_map)) {//free unused memory
        dml_stat_map->~DmlStatMap();
        ob_free(buff);
        buff = NULL;
        dml_stat_map = tmp_dml_stat_map;
      }
      if (OB_NOT_NULL(dml_stat_map)) {
        //arrive at here, indicates get a valid dml_stat_map, we need reset error code, and atomic update values
        if (OB_FAIL(dml_stat_maps_.read_atomic(tenant_id, atomic_op))) {
          LOG_WARN("failed to atomic refactored", K(ret), K(tenant_id));
        }
      }
    } else {
      LOG_WARN("failed to atomic refactored", K(ret));
    }
  }
  return ret;
}

int ObOptStatMonitorManager::update_opt_stat_monitoring_info(const bool with_check)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_column_usage_table(with_check))) {
    LOG_WARN("failed to update column usage table", K(ret));
  } else if (OB_FAIL(update_dml_stat_info(with_check))) {
    LOG_WARN("failed to update dml stat info", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObOptStatMonitorManager::update_opt_stat_monitoring_info(const obrpc::ObFlushOptStatArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(arg));
  } else if (arg.is_flush_col_usage_ && OB_FAIL(update_tenant_column_usage_info(arg.tenant_id_))) {
    LOG_WARN("failed to update tenant column usage info", K(ret));
  } else if (arg.is_flush_dml_stat_ && OB_FAIL(update_tenant_dml_stat_info(arg.tenant_id_))) {
    LOG_WARN("failed to update tenant column usage info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptStatMonitorManager::update_column_usage_table(const bool with_check)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> tenant_ids;
  GetAllKeyOp get_all_key_op(&tenant_ids, with_check);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("opt stat monitor is not inited", K(ret));
  } else if (OB_FAIL(column_usage_maps_.foreach_refactored(get_all_key_op))) {
    LOG_WARN("failed to foreach refactored", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
    if (OB_FAIL(update_tenant_column_usage_info(tenant_ids.at(i)))) {
      LOG_WARN("failed to update tenant column usage info", K(ret));
    }
  }
  return ret;
}

int ObOptStatMonitorManager::update_tenant_column_usage_info(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SwapMapAtomicOp swap_map_op;
  ColumnUsageMap *col_map = NULL;
  bool is_writeable = false;
  if (OB_FAIL(check_table_writeable(tenant_id, is_writeable))) {
    LOG_WARN("failed to check tabke writeable", K(ret));
  } else if (!is_writeable) {
    // do nothing
  } else if (OB_FAIL(column_usage_maps_.atomic_refactored(tenant_id, swap_map_op))) {
    if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
      // tenant may not exsits on this server
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to atomic refactored", K(ret));
    }
  } else if (OB_ISNULL(col_map = swap_map_op.get_column_usage_map())) {
    // NULL means column usage map is empty, do nothing
  } else {
    ObSqlString value_sql;
    int count = 0;
    for (auto iter = col_map->begin(); OB_SUCC(ret) && iter != col_map->end(); ++iter) {
      if (OB_FAIL(get_column_usage_sql(tenant_id,
                                       iter->first,
                                       iter->second,
                                       0 != count, // need_add_comma
                                       value_sql))) {
        LOG_WARN("failed to get column usage sql", K(ret));
      } else if (UPDATE_OPT_STAT_BATCH_CNT == ++count) {
        if (OB_FAIL(exec_insert_column_usage_sql(tenant_id, value_sql))) {
          LOG_WARN("failed to exec insert sql", K(ret));
        } else {
          count = 0;
          value_sql.reset();
        }
      }
    }
    if (OB_SUCC(ret) && count != 0) {
      if (OB_FAIL(exec_insert_column_usage_sql(tenant_id, value_sql))) {
        LOG_WARN("failed to exec insert sql", K(ret));
      }
    }

    if (OB_NOT_NULL(col_map)) {
      col_map->~ColumnUsageMap();
      ob_free(col_map);
      col_map = NULL;
    }
  }
  return ret;
}

int ObOptStatMonitorManager::update_dml_stat_info(const bool with_check)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 8> tenant_ids;
  GetAllKeyOp get_all_key_op(&tenant_ids, with_check);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("opt stat monitor is not inited", K(ret));
  } else if (OB_FAIL(dml_stat_maps_.foreach_refactored(get_all_key_op))) {
    LOG_WARN("failed to foreach refactored", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
    if (OB_FAIL(update_tenant_dml_stat_info(tenant_ids.at(i)))) {
      LOG_WARN("failed to update tenant dml stat  info", K(ret));
    }
  }
  return ret;
}

int ObOptStatMonitorManager::update_tenant_dml_stat_info(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  SwapMapAtomicOp swap_map_op;
  DmlStatMap *dml_stat_map = NULL;
  bool is_writeable = false;
  if (OB_FAIL(check_table_writeable(tenant_id, is_writeable))) {
    LOG_WARN("failed to check tabke writeable", K(ret));
  } else if (!is_writeable) {
    // do nothing
  } else if (OB_FAIL(dml_stat_maps_.atomic_refactored(tenant_id, swap_map_op))) {
    if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
      // tenant may not exsits on this server
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to atomic refactored", K(ret));
    }
  } else if (OB_ISNULL(dml_stat_map = swap_map_op.get_dml_stat_map())) {
    // NULL means dml stats map is empty, do nothing
  } else {
    ObSqlString value_sql;
    int count = 0;
    for (auto iter = dml_stat_map->begin(); OB_SUCC(ret) && iter != dml_stat_map->end(); ++iter) {
      if (OB_FAIL(get_dml_stat_sql(tenant_id,
                                    iter->first,
                                    iter->second,
                                    0 != count, // need_add_comma
                                    value_sql))) {
        LOG_WARN("failed to get dml stat sql", K(ret));
      } else if (UPDATE_OPT_STAT_BATCH_CNT == ++count) {
        if (OB_FAIL(exec_insert_monitor_modified_sql(tenant_id, value_sql))) {
          LOG_WARN("failed to exec insert sql", K(ret));
        } else {
          count = 0;
          value_sql.reset();
        }
      }
    }
    if (OB_SUCC(ret) && count != 0) {
      if (OB_FAIL(exec_insert_monitor_modified_sql(tenant_id, value_sql))) {
        LOG_WARN("failed to exec insert sql", K(ret));
      }
    }

    if (OB_NOT_NULL(dml_stat_map)) {
      dml_stat_map->~DmlStatMap();
      ob_free(dml_stat_map);
      dml_stat_map = NULL;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(clean_useless_dml_stat_info(tenant_id))) {
        LOG_WARN("failed to clean useless dml stat info", K(ret));
      } else {/*do nohting*/}
    }
  }
  return ret;
}

int ObOptStatMonitorManager::get_column_usage_sql(const uint64_t tenant_id,
                                                  const StatKey &col_key,
                                                  const int64_t flags,
                                                  const bool need_add_comma,
                                                  ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t table_id = col_key.first;
  uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id);
  int64_t equality_preds = flags & EQUALITY_PREDS ? 1 : 0;
  int64_t equijoin_preds = flags & EQUIJOIN_PREDS ? 1 : 0;
  int64_t nonequijion_preds = flags & NONEQUIJOIN_PREDS ? 1 : 0;
  int64_t range_preds = flags & RANGE_PREDS ? 1 : 0;
  int64_t like_preds = flags & LIKE_PREDS ? 1 : 0;
  int64_t null_preds = flags & NULL_PREDS ? 1 : 0;
  int64_t distinct_member = flags & DISTINCT_MEMBER ? 1 : 0;
  int64_t groupby_member = flags & GROUPBY_MEMBER ? 1 : 0;
  if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("table_id", pure_table_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("column_id", col_key.second)) ||
      OB_FAIL(dml_splicer.add_column("equality_preds", equality_preds)) ||
      OB_FAIL(dml_splicer.add_column("equijoin_preds", equijoin_preds)) ||
      OB_FAIL(dml_splicer.add_column("nonequijion_preds", nonequijion_preds)) ||
      OB_FAIL(dml_splicer.add_column("range_preds", range_preds)) ||
      OB_FAIL(dml_splicer.add_column("like_preds", like_preds)) ||
      OB_FAIL(dml_splicer.add_column("null_preds", null_preds)) ||
      OB_FAIL(dml_splicer.add_column("distinct_member", distinct_member)) ||
      OB_FAIL(dml_splicer.add_column("groupby_member", groupby_member)) ||
      OB_FAIL(dml_splicer.add_column("flags", flags))) {
    LOG_WARN("failed to add dml splicer column", K(ret));
  } else if (OB_FAIL(sql_string.append_fmt("%s", need_add_comma ? ",(" : "("))) {
    LOG_WARN("failed to append string", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(sql_string))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else if (OB_FAIL(sql_string.append(")"))) {
    LOG_WARN("failed to append string", K(ret));
  }
  return ret;
}

int ObOptStatMonitorManager::exec_insert_column_usage_sql(uint64_t tenant_id,ObSqlString &values_sql)
{
  int ret = OB_SUCCESS;
  ObSqlString insert_sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(insert_sql.append(INSERT_COLUMN_USAGE))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(insert_sql.append(values_sql.ptr(), values_sql.length()))) {
    LOG_WARN("failed to append format", K(ret));
  } else if (OB_FAIL(insert_sql.append(ON_DUPLICATE_UPDATE))) {
    LOG_WARN("failed to append string", K(ret));
  } else if (OB_FAIL(mysql_proxy_->write(tenant_id, insert_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(insert_sql), K(ret));
  } else if (OB_FAIL(mysql_proxy_->write(tenant_id, "commit;", affected_rows))) {
    LOG_WARN("fail to exec sql", K(ret));
  }
  return ret;
}

int ObOptStatMonitorManager::get_column_usage_from_table(ObExecContext &ctx,
                                                         ObIArray<ObColumnStatParam *> &column_params,
                                                         uint64_t tenant_id,
                                                         uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObSqlString select_sql;
  if (OB_FAIL(construct_get_column_usage_sql(column_params, tenant_id, table_id, select_sql))) {
    LOG_WARN("failed to construct sql", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(ctx.get_sql_proxy());
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else if (OB_FAIL(client_result->next())) {
        LOG_WARN("failed to get next row", K(ret));
      }
      while (OB_SUCC(ret)) {
        ObColumnStatParam *target_param = NULL;
        int64_t flag = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < info_count + 1; ++i) {
          ObObj val;
          if (OB_FAIL(client_result->get_obj(i, val))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (!val.is_int()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected value type", K(ret), K(i));
          } else if (i == 0) {
            // column_id
            int64_t column_id = val.get_int();
            bool find = false;
            for (int64_t j = 0; !find && j < column_params.count(); ++j) {
              if (column_params.at(j)->column_id_ == column_id) {
                target_param = column_params.at(j);
                find = true;
              }
            }
          } else if (val.get_int() > 0) {
            flag |= 1 << (i - 1);
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(target_param)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else {
            target_param->column_usage_flag_ = flag;
            ret = client_result->next();
          }
        }
      }
      if (OB_LIKELY(ret == OB_ITER_END)) {
        ret = OB_SUCCESS;
      }
      if (NULL != client_result) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
  }
  return ret;
}

int ObOptStatMonitorManager::construct_get_column_usage_sql(ObIArray<ObColumnStatParam *> &column_params,
                                                            const uint64_t tenant_id,
                                                            const uint64_t table_id,
                                                            ObSqlString &select_sql)
{
  int ret = OB_SUCCESS;
  ObSqlString col_ids;
  uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_params.count(); ++i) {
    ObColumnStatParam *column_param = column_params.at(i);
    if (OB_ISNULL(column_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(col_ids.append_fmt("%s%lu",
                                          i == 0 ? "" : ", ",
                                          column_param->column_id_))) {
      LOG_WARN("failed to append format", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(select_sql.append_fmt(SELECT_FROM_COLUMN_USAGE,
                                      ext_tenant_id,
                                      pure_table_id,
                                      col_ids.ptr()))) {
      LOG_WARN("failed to append fmt", K(ret));
    }
  }
  return ret;
}

int ObOptStatMonitorManager::check_table_writeable(const uint64_t tenant_id, bool &is_writeable)
{
  int ret = OB_SUCCESS;
  is_writeable = true;
  bool in_restore = false;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->check_tenant_is_restore(NULL, tenant_id, in_restore))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_UNLIKELY(in_restore) || GCTX.is_standby_cluster()) {
    is_writeable = false;
  }
  return ret;
}

int ObOptStatMonitorManager::GetAllKeyOp::operator()(common::hash::HashMapPair<uint64_t, ColumnUsageMap *> &entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key_array_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("key_array not inited", K(ret));
  } else if (with_check_ && entry.second->size() < 10000) {
    // do nothing
  } else if (OB_FAIL(key_array_->push_back(entry.first))) {
    LOG_WARN("fail to push back key", K(ret));
  }
  return ret;
}

int ObOptStatMonitorManager::GetAllKeyOp::operator()(common::hash::HashMapPair<uint64_t, DmlStatMap *> &entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(key_array_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("key_array not inited", K(ret));
  } else if (OB_FAIL(key_array_->push_back(entry.first))) {
    LOG_WARN("fail to push back key", K(ret));
  }
  return ret;
}

// get old ColumnUsageMap, allocate a new one
int ObOptStatMonitorManager::SwapMapAtomicOp::operator() (common::hash::HashMapPair<uint64_t, ColumnUsageMap *> &entry)
{
  int ret = OB_SUCCESS;
  column_usage_map_ = NULL;
  ObMemAttr attr(OB_SERVER_TENANT_ID, "DmlStatsHashMap");
  SET_USE_500(attr);
  if (OB_ISNULL(entry.second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (entry.second->size() == 0) {
    // do nothing
  } else {
    ColumnUsageMap *col_map = NULL;
    void *buff = ob_malloc(sizeof(ColumnUsageMap), attr);
    if (OB_ISNULL(buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else if (NULL == (col_map = new(buff)ColumnUsageMap())) {
      ret = OB_NOT_INIT;
      LOG_WARN("fail to constructor column usage map", K(ret));
    } else if (OB_FAIL(col_map->create(10000, attr))) {
      LOG_WARN("failed to create column usage map", K(ret));
    } else {
      column_usage_map_ = entry.second;
      entry.second = col_map;
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(col_map)) {
      col_map->~ColumnUsageMap();
      ob_free(buff);
      buff = NULL;
      col_map = NULL;
    }
  }
  return ret;
}

// get old DmlStatMap, allocate a new one
int ObOptStatMonitorManager::SwapMapAtomicOp::operator() (common::hash::HashMapPair<uint64_t, DmlStatMap *> &entry)
{
  int ret = OB_SUCCESS;
  dml_stat_map_ = NULL;
  ObMemAttr attr(OB_SERVER_TENANT_ID, "DmlStatMap");
  SET_USE_500(attr);
  if (OB_ISNULL(entry.second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (entry.second->size() == 0) {
    // do nothing
  } else {
    DmlStatMap *dml_stat_map = NULL;
    void *buff = ob_malloc(sizeof(DmlStatMap), attr);
    if (OB_ISNULL(buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc memory failed", K(ret));
    } else if (NULL == (dml_stat_map = new(buff)DmlStatMap())) {
      ret = OB_NOT_INIT;
      LOG_WARN("fail to constructor DmlStatMap", K(ret));
    } else if (OB_FAIL(dml_stat_map->create(10000, attr))) {
      LOG_WARN("failed to create column usage map", K(ret));
    } else {
      dml_stat_map_ = entry.second;
      entry.second = dml_stat_map;
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(dml_stat_map)) {
      dml_stat_map->~DmlStatMap();
      ob_free(buff);
      buff = NULL;
      dml_stat_map = NULL;
    }
  }
  return ret;
}

int ObOptStatMonitorManager::UpdateValueAtomicOp::operator() (common::hash::HashMapPair<StatKey, int64_t> &entry)
{
  entry.second |= flags_;
  return OB_SUCCESS;
}

int ObOptStatMonitorManager::UpdateValueAtomicOp::operator() (common::hash::HashMapPair<StatKey, ObOptDmlStat> &entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(entry.second.table_id_ != dml_stat_.table_id_ ||
                  entry.second.tablet_id_ != dml_stat_.tablet_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(entry.second), K(dml_stat_));
  } else {
    entry.second.insert_row_count_ += dml_stat_.insert_row_count_;
    entry.second.update_row_count_ += dml_stat_.update_row_count_;
    entry.second.delete_row_count_ += dml_stat_.delete_row_count_;
  }
  return ret;
}

int ObOptStatMonitorManager::ReadMapAtomicOp::operator() (common::hash::HashMapPair<uint64_t, ColumnUsageMap *> &entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry.second) || OB_ISNULL(col_usage_args_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(entry.second), K(col_usage_args_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_usage_args_->count(); ++i) {
      ColumnUsageArg &arg = col_usage_args_->at(i);
      StatKey col_key(arg.table_id_, arg.column_id_);
      int64_t flags = 0;
      if (OB_FAIL(entry.second->get_refactored(col_key, flags))) {
        if (OB_LIKELY(ret == OB_HASH_NOT_EXIST)) {
          if (OB_FAIL(entry.second->set_refactored(col_key, arg.flags_))) {
            // other thread set the refactor, try update again
            if (OB_FAIL(entry.second->get_refactored(col_key, flags))) {
              LOG_WARN("failed to get refactored", K(ret));
            } else if ((~flags) & arg.flags_) {
              UpdateValueAtomicOp atomic_op(arg.flags_);
              if (OB_FAIL(entry.second->atomic_refactored(col_key, atomic_op))) {
                LOG_WARN("failed to atomic refactored", K(ret));
              }
            }
          }
        } else {
          LOG_WARN("failed to get refactored", K(ret));
        }
      } else if ((~flags) & arg.flags_) {
        UpdateValueAtomicOp atomic_op(arg.flags_);
        if (OB_FAIL(entry.second->atomic_refactored(col_key, atomic_op))) {
          LOG_WARN("failed to atomic refactored", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObOptStatMonitorManager::ReadMapAtomicOp::operator() (common::hash::HashMapPair<uint64_t, DmlStatMap *> &entry)
{
  int ret = OB_SUCCESS;
  StatKey key(dml_stat_.table_id_, dml_stat_.tablet_id_);
  UpdateValueAtomicOp atomic_op(dml_stat_);
  ObOptDmlStat tmp_dml_stat;
  if (OB_ISNULL(entry.second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(entry.second->get_refactored(key, tmp_dml_stat))) {
    if (OB_LIKELY(ret == OB_HASH_NOT_EXIST)) {
      if (OB_FAIL(entry.second->set_refactored(key, dml_stat_))) {
        // other thread set the refactor, try update again
        if (OB_FAIL(entry.second->get_refactored(key, tmp_dml_stat))) {
          LOG_WARN("failed to get refactored", K(ret));
        } else {
          UpdateValueAtomicOp atomic_op(dml_stat_);
          if (OB_FAIL(entry.second->atomic_refactored(key, atomic_op))) {
            LOG_WARN("failed to atomic refactored", K(ret));
          }
        }
      }
    } else {
      LOG_WARN("failed to get refactored", K(ret));
    }
  } else {
    UpdateValueAtomicOp atomic_op(dml_stat_);
    if (OB_FAIL(entry.second->atomic_refactored(key, atomic_op))) {
      LOG_WARN("failed to atomic refactored", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObOptStatMonitorManager::exec_insert_monitor_modified_sql(uint64_t tenant_id,
                                                              ObSqlString &values_sql)
{
  int ret = OB_SUCCESS;
  ObSqlString insert_sql;
  int64_t affected_rows = 0;
  if (OB_FAIL(insert_sql.append_fmt(INSERT_MONITOR_MODIFIED, share::OB_ALL_MONITOR_MODIFIED_TNAME))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(insert_sql.append(values_sql.ptr(), values_sql.length()))) {
    LOG_WARN("failed to append format", K(ret));
  } else if (OB_FAIL(insert_sql.append(ON_DUPLICATE_UPDATE_MONITOR_MODIFIED))) {
    LOG_WARN("failed to append string", K(ret));
  } else if (OB_FAIL(mysql_proxy_->write(tenant_id, insert_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(insert_sql), K(ret));
  }
  return ret;
}

int ObOptStatMonitorManager::get_dml_stat_sql(const uint64_t tenant_id,
                                              const StatKey &dml_stat_key,
                                              const ObOptDmlStat &dml_stat,
                                              const bool need_add_comma,
                                              ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t table_id = dml_stat_key.first;
  uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id);
  if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("table_id", pure_table_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("tablet_id", dml_stat_key.second)) ||
      OB_FAIL(dml_splicer.add_column("inserts", dml_stat.insert_row_count_)) ||
      OB_FAIL(dml_splicer.add_column("updates", dml_stat.update_row_count_)) ||
      OB_FAIL(dml_splicer.add_column("deletes", dml_stat.delete_row_count_))) {
    LOG_WARN("failed to add dml splicer column", K(ret));
  } else if (OB_FAIL(sql_string.append_fmt("%s", need_add_comma ? ",(" : "("))) {
    LOG_WARN("failed to append string", K(ret));
  } else if (OB_FAIL(dml_splicer.splice_values(sql_string))) {
    LOG_WARN("failed to get sql string", K(ret));
  } else if (OB_FAIL(sql_string.append(")"))) {
    LOG_WARN("failed to append string", K(ret));
  }
  return ret;
}

int ObOptStatMonitorManager::generate_opt_stat_monitoring_info_rows(observer::ObOptDmlStatMapsGetter &getter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml_stat_maps_.foreach_refactored(getter))) {
    LOG_WARN("fail to generate opt stat monitoring info rows", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObOptStatMonitorManager::clean_useless_dml_stat_info(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString delete_sql;
  int64_t affected_rows = 0;
  const char* all_table_name = NULL;
  if (OB_FAIL(ObSchemaUtils::get_all_table_name(tenant_id, all_table_name))) {
    LOG_WARN("failed to get all table name", K(ret));
  } else if (OB_FAIL(delete_sql.append_fmt("DELETE FROM %s m WHERE (NOT EXISTS (SELECT 1 " \
            "FROM %s t, %s db WHERE t.tenant_id = db.tenant_id AND t.database_id = db.database_id "\
            "AND t.table_id = m.table_id AND t.tenant_id = m.tenant_id AND db.database_name != '__recyclebin') "\
            "OR (tenant_id, table_id, tablet_id) IN (SELECT m.tenant_id, m.table_id, m.tablet_id FROM "\
            "%s m, %s t WHERE t.table_id = m.table_id AND t.tenant_id = m.tenant_id AND t.part_level > 0 "\
            "AND NOT EXISTS (SELECT 1 FROM %s p WHERE  p.table_id = m.table_id AND p.tenant_id = m.tenant_id AND p.tablet_id = m.tablet_id) "\
            "AND NOT EXISTS (SELECT 1 FROM %s sp WHERE  sp.table_id = m.table_id AND sp.tenant_id = m.tenant_id AND sp.tablet_id = m.tablet_id))) "\
            "AND table_id > %ld;",
            share::OB_ALL_MONITOR_MODIFIED_TNAME, all_table_name, share::OB_ALL_DATABASE_TNAME,
            share::OB_ALL_MONITOR_MODIFIED_TNAME, all_table_name, share::OB_ALL_PART_TNAME,
            share::OB_ALL_SUB_PART_TNAME, OB_MAX_INNER_TABLE_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(mysql_proxy_->write(tenant_id, delete_sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret), K(delete_sql));
  } else {
    LOG_TRACE("succeed to clean useless monitor modified_data", K(tenant_id), K(delete_sql), K(affected_rows));
  }
  return ret;
}


}
}
