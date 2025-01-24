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
#include "ob_opt_stat_monitor_manager.h"
#include "sql/ob_sql_init.h"
#include "observer/ob_sql_client_decorator.h"
#include "lib/rc/ob_rc.h"
#include "observer/ob_server.h"
#include "share/stat/ob_dbms_stats_utils.h"

namespace oceanbase
{
using namespace observer;
using namespace sqlclient;

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

#define INSERT_STALE_TABLE_STAT_SQL "INSERT /*+QUERY_TIMEOUT(60000000)*/INTO %s(tenant_id," \
                                                                                "table_id," \
                                                                                "partition_id," \
                                                                                "index_type," \
                                                                                "object_type," \
                                                                                "last_analyzed," \
                                                                                "sstable_row_cnt," \
                                                                                "sstable_avg_row_len," \
                                                                                "macro_blk_cnt," \
                                                                                "micro_blk_cnt," \
                                                                                "memtable_row_cnt," \
                                                                                "memtable_avg_row_len," \
                                                                                "row_cnt," \
                                                                                "avg_row_len," \
                                                                                "global_stats," \
                                                                                "user_stats," \
                                                                                "stattype_locked," \
                                                                                "stale_stats) VALUES %s" \
                                                                                "ON DUPLICATE KEY UPDATE " \
                                                                                "stale_stats = if(last_analyzed > 0, stale_stats, values(stale_stats))"

#define STALE_TABLE_STAT_MOCK_VALUE_PATTERN "(%lu, %lu, %ld, 0, 0, 0, -1, -1, 0, 0, -1, -1, 0, 0, 0, 0, 0, 1)"


void ObOptStatMonitorFlushAllTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(optstat_monitor_mgr_) && optstat_monitor_mgr_->inited_) {
    LOG_INFO("run opt stat monitor flush all task", K(optstat_monitor_mgr_->tenant_id_));
    uint64_t tenant_id = optstat_monitor_mgr_->tenant_id_;
    bool is_primary = true;
    THIS_WORKER.set_timeout_ts(FLUSH_INTERVAL / 2 + ObTimeUtility::current_time());
    if (OB_FAIL(ObShareUtil::mtl_check_if_tenant_role_is_primary(tenant_id, is_primary))) {
      LOG_WARN("fail to execute mtl_check_if_tenant_role_is_primary", KR(ret), K(tenant_id));
    } else if (!is_primary) {
      // do nothing
    } else if (OB_FAIL(optstat_monitor_mgr_->update_column_usage_info(false))) {
      LOG_WARN("failed to update column usage info", K(ret));
    } else if (OB_FAIL(optstat_monitor_mgr_->update_dml_stat_info())) {
      LOG_WARN("failed to failed to update dml stat info", K(ret));
    } else {/*do nothing*/}
  }
}

void ObOptStatMonitorCheckTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(optstat_monitor_mgr_) && optstat_monitor_mgr_->inited_) {
    LOG_INFO("run opt stat monitor check task", K(optstat_monitor_mgr_->tenant_id_));
    uint64_t tenant_id = optstat_monitor_mgr_->tenant_id_;
    bool is_primary = true;
    THIS_WORKER.set_timeout_ts(CHECK_INTERVAL + ObTimeUtility::current_time());
    if (OB_FAIL(ObShareUtil::mtl_check_if_tenant_role_is_primary(tenant_id, is_primary))) {
      LOG_WARN("fail to execute mtl_check_if_tenant_role_is_primary", KR(ret), K(tenant_id));
    } else if (!is_primary) {
      // do nothing
    } else if (OB_FAIL(optstat_monitor_mgr_->update_column_usage_info(true))) {
      LOG_WARN("failed to update column usage info", K(ret));
    } else if (OB_FAIL(optstat_monitor_mgr_->update_dml_stat_info())) {
      LOG_WARN("failed to failed to update dml stat info", K(ret));
    } else {/*do nothing*/}
  }
}

int ObOptStatMonitorManager::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("column usage manager has already been initialized.", K(ret));
  } else if (OB_FAIL(column_usage_map_.create(10000, "ColUsagHashMap", "ColUsagNode", tenant_id))) {
    LOG_WARN("failed to column usage map", K(ret));
  } else if (OB_FAIL(dml_stat_map_.create(10000, "DmlStatHashMap", "DmlStatNode", tenant_id))) {
    LOG_WARN("failed to create dml stat map", K(ret));
  } else {
    inited_ = true;
    mysql_proxy_ = &ObServer::get_instance().get_mysql_proxy();
    tenant_id_ = tenant_id;
    destroyed_ = false;
  }
  if (OB_FAIL(ret) && !inited_) {
    destroy();
  }
  return ret;
}

void ObOptStatMonitorManager::destroy()
{
  if (!destroyed_) {
    destroyed_ = true;
    inited_ = false;
    TG_DESTROY(tg_id_);
    SpinWLockGuard guard(lock_);
    column_usage_map_.destroy();
    dml_stat_map_.destroy();
  }
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
        timeout = std::min(MAX_OPT_STATS_PROCESS_RPC_TIMEOUT, THIS_WORKER.get_timeout_remain());
        if (!all_server_arr.at(i).is_active()
            || ObServerStatus::OB_SERVER_ACTIVE != all_server_arr.at(i).get_server_status()
            || 0 == all_server_arr.at(i).get_start_service_time()
            || 0 != all_server_arr.at(i).get_server_stop_time()) {
        //server may not serving
        } else if (0 >= timeout) {
          ret = OB_TIMEOUT;
          LOG_WARN("query timeout is reached", K(ret), K(timeout));
        } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(all_server_arr.at(i).get_addr())
                                                  .timeout(timeout)
                                                  .by(ctx.get_my_session()->get_rpc_tenant_id())
                                                  .flush_local_opt_stat_monitoring_info(arg))) {
          LOG_WARN("failed to flush opt stat monitoring info caused by unknow error",
                                                K(ret), K(all_server_arr.at(i).get_addr()), K(arg));
          //ignore flush cache failed, TODO @jiangxiu.wt can aduit it and flush cache manually later.
          if (ignore_failed) {
            LOG_USER_WARN(OB_ERR_DBMS_STATS_PL, "failed to flush opt stat monitoring info");
            if (OB_FAIL(failed_server_arr.push_back(all_server_arr.at(i)))) {
              LOG_WARN("failed to push back", K(ret));
            }
          }
        }
      }
      LOG_TRACE("flush database monitoring info cache", K(arg), K(failed_server_arr), K(all_server_arr));
    }
  }
  return ret;
}

int ObOptStatMonitorManager::update_local_cache(common::ObIArray<ColumnUsageArg> &args)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < args.count(); ++i) {
    ColumnUsageArg &arg = args.at(i);
    StatKey col_key(arg.table_id_, arg.column_id_);
    int64_t flags = 0;
    if (OB_FAIL(column_usage_map_.get_refactored(col_key, flags))) {
      if (OB_LIKELY(ret == OB_HASH_NOT_EXIST)) {
        if (OB_FAIL(column_usage_map_.set_refactored(col_key, arg.flags_))) {
          // other thread set the refactor, try update again
          if (OB_FAIL(column_usage_map_.get_refactored(col_key, flags))) {
            LOG_WARN("failed to get refactored", K(ret));
          } else if ((~flags) & arg.flags_) {
            UpdateValueAtomicOp atomic_op(arg.flags_);
            if (OB_FAIL(column_usage_map_.atomic_refactored(col_key, atomic_op))) {
              LOG_WARN("failed to atomic refactored", K(ret));
            }
          }
        }
      } else {
        LOG_WARN("failed to get refactored", K(ret));
      }
    } else if ((~flags) & arg.flags_) {
      UpdateValueAtomicOp atomic_op(arg.flags_);
      if (OB_FAIL(column_usage_map_.atomic_refactored(col_key, atomic_op))) {
        LOG_WARN("failed to atomic refactored", K(ret));
      }
    }
  }
  return ret;
}

int ObOptStatMonitorManager::update_local_cache(ObOptDmlStat &dml_stat)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  StatKey key(dml_stat.table_id_, dml_stat.tablet_id_);
  ObOptDmlStat tmp_dml_stat;
  if (OB_FAIL(dml_stat_map_.get_refactored(key, tmp_dml_stat))) {
    if (OB_LIKELY(ret == OB_HASH_NOT_EXIST)) {
      if (OB_FAIL(dml_stat_map_.set_refactored(key, dml_stat))) {
        // other thread set the refactor, try update again
        if (OB_FAIL(dml_stat_map_.get_refactored(key, tmp_dml_stat))) {
          LOG_WARN("failed to get refactored", K(ret));
        } else {
          UpdateValueAtomicOp atomic_op(dml_stat);
          if (OB_FAIL(dml_stat_map_.atomic_refactored(key, atomic_op))) {
            LOG_WARN("failed to atomic refactored", K(ret));
          }
        }
      }
    } else {
      LOG_WARN("failed to get refactored", K(ret));
    }
  } else {
    UpdateValueAtomicOp atomic_op(dml_stat);
    if (OB_FAIL(dml_stat_map_.atomic_refactored(key, atomic_op))) {
      LOG_WARN("failed to atomic refactored", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObOptStatMonitorManager::update_opt_stat_monitoring_info(const obrpc::ObFlushOptStatArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(arg));
  } else if (arg.is_flush_col_usage_ && OB_FAIL(update_column_usage_info(false))) {
    LOG_WARN("failed to update tenant column usage info", K(ret));
  } else if (arg.is_flush_dml_stat_ && OB_FAIL(update_dml_stat_info())) {
    LOG_WARN("failed to update tenant column usage info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObOptStatMonitorManager::update_column_usage_info(const bool with_check)
{
  int ret = OB_SUCCESS;
  bool is_writeable = false;
  ObArray<StatKey> col_stat_keys;
  ObArray<int64_t> col_flags;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("opt stat monitor is not inited", K(ret));
  } else if (OB_FAIL(check_table_writeable(is_writeable))) {
    LOG_WARN("failed to check tabke writeable", K(ret));
  } else if (!is_writeable) {
    // do nothing
  } else if (OB_FAIL(get_col_usage_info(with_check, col_stat_keys, col_flags))) {
    LOG_WARN("failed to get col usage info", K(ret));
  } else if (!col_stat_keys.empty() && col_stat_keys.count() == col_flags.count()) {
    ObSqlString value_sql;
    int count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_stat_keys.count(); ++i) {
      if (OB_FAIL(get_column_usage_sql(col_stat_keys.at(i), col_flags.at(i), 0 != count, value_sql))) {
        LOG_WARN("failed to get column usage sql", K(ret));
      } else if (UPDATE_OPT_STAT_BATCH_CNT == ++count) {
        if (OB_FAIL(exec_insert_column_usage_sql(value_sql))) {
          LOG_WARN("failed to exec insert sql", K(ret));
        } else {
          count = 0;
          value_sql.reset();
        }
      }
    }
    if (OB_SUCC(ret) && count != 0) {
      if (OB_FAIL(exec_insert_column_usage_sql(value_sql))) {
        LOG_WARN("failed to exec insert sql", K(ret));
      }
    }
  }
  return ret;
}

int ObOptStatMonitorManager::update_dml_stat_info()
{
  int ret = OB_SUCCESS;
  bool is_writeable = false;
  ObArray<ObOptDmlStat> dml_stats;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("opt stat monitor is not inited", K(ret));
  } else if (OB_FAIL(check_table_writeable(is_writeable))) {
    LOG_WARN("failed to check tabke writeable", K(ret));
  } else if (!is_writeable) {
    // do nothing
  } else if (OB_FAIL(get_dml_stats(dml_stats))) {
    LOG_WARN("failed to swap get dml stat", K(ret));
  } else if (!dml_stats.empty()) {
    ObSqlString value_sql;
    int count = 0;
    uint64_t tenant_id = dml_stats.at(0).tenant_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < dml_stats.count(); ++i) {
      if (OB_FAIL(get_dml_stat_sql(dml_stats.at(i), 0 != count, value_sql))) {
        LOG_WARN("failed to get dml stat sql", K(ret));
      } else if (UPDATE_OPT_STAT_BATCH_CNT == ++count) {
        if (OB_FAIL(exec_insert_monitor_modified_sql(value_sql))) {
          LOG_WARN("failed to exec insert sql", K(ret));
        } else {
          count = 0;
          value_sql.reset();
        }
      }
    }
    if (OB_SUCC(ret) && count != 0) {
      if (OB_FAIL(exec_insert_monitor_modified_sql(value_sql))) {
        LOG_WARN("failed to exec insert sql", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      bool no_check = (OB_E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS) != OB_SUCCESS;
      uint64_t data_version = 0;
      if (OB_FAIL(clean_useless_dml_stat_info())) {
        LOG_WARN("failed to clean useless dml stat info", K(ret));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
       LOG_WARN("fail to get tenant data version", KR(ret), K(tenant_id), K(data_version));
      } else if (data_version < MOCK_DATA_VERSION_4_2_4_0 ||
                 (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_3_3_0) ||
                 no_check) {
        //do nothing
      } else if (OB_FAIL(check_opt_stats_expired(dml_stats))) {
        LOG_WARN("failed to check opt stats expired", K(ret));
      } else {/*do nohting*/}
    }
  }
  return ret;
}

int ObOptStatMonitorManager::get_column_usage_sql(const StatKey &col_key,
                                                  const int64_t flags,
                                                  const bool need_add_comma,
                                                  ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t table_id = col_key.first;
  uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id_, tenant_id_);
  uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id_, table_id);
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

int ObOptStatMonitorManager::exec_insert_column_usage_sql(ObSqlString &values_sql)
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
  } else if (OB_FAIL(mysql_proxy_->write(tenant_id_, insert_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(insert_sql), K(ret));
  } else if (OB_FAIL(mysql_proxy_->write(tenant_id_, "commit;", affected_rows))) {
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

int ObOptStatMonitorManager::check_table_writeable(bool &is_writeable)
{
  int ret = OB_SUCCESS;
  is_writeable = true;
  bool is_primary = true;
  if (OB_FAIL(ObShareUtil::mtl_check_if_tenant_role_is_primary(tenant_id_, is_primary))) {
    LOG_WARN("fail to execute mtl_check_if_tenant_role_is_primary", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_primary)) {
    is_writeable = false;
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

int ObOptStatMonitorManager::exec_insert_monitor_modified_sql(ObSqlString &values_sql,
                                                              ObISQLConnection *conn)
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
  } else if (nullptr != conn &&
             OB_FAIL(conn->execute_write(tenant_id_, insert_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(insert_sql), K(ret));
  } else if (nullptr == conn &&
             OB_FAIL(mysql_proxy_->write(tenant_id_, insert_sql.ptr(), affected_rows))) {
    LOG_WARN("fail to exec sql", K(insert_sql), K(ret));
  } else {
    LOG_TRACE("succeed to exec insert monitor modified sql", K(tenant_id_), K(values_sql));
  }
  return ret;
}

int ObOptStatMonitorManager::get_dml_stat_sql(const ObOptDmlStat &dml_stat,
                                              const bool need_add_comma,
                                              ObSqlString &sql_string)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml_splicer;
  uint64_t table_id = dml_stat.table_id_;
  uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id_, tenant_id_);
  uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id_, table_id);
  if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ext_tenant_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("table_id", pure_table_id)) ||
      OB_FAIL(dml_splicer.add_pk_column("tablet_id", dml_stat.tablet_id_)) ||
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

int ObOptStatMonitorManager::generate_opt_stat_monitoring_info_rows(observer::ObOptDmlStatMapGetter &getter)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(dml_stat_map_.foreach_refactored(getter))) {
    LOG_WARN("fail to generate opt stat monitoring info rows", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObOptStatMonitorManager::clean_useless_dml_stat_info()
{
  int ret = OB_SUCCESS;
  ObSqlString delete_table_sql;
  ObSqlString delete_part_sql;
  int64_t affected_rows1 = 0;
  int64_t affected_rows2 = 0;
  const char* all_table_name = NULL;
  if (OB_FAIL(ObSchemaUtils::get_all_table_name(tenant_id_, all_table_name))) {
    LOG_WARN("failed to get all table name", K(ret));
  } else if (OB_FAIL(delete_table_sql.append_fmt("DELETE FROM %s m WHERE (NOT EXISTS (SELECT 1 " \
            "FROM %s t, %s db WHERE t.tenant_id = db.tenant_id AND t.database_id = db.database_id "\
            "AND t.table_id = m.table_id AND t.tenant_id = m.tenant_id AND db.database_name != '__recyclebin')) "\
            "AND table_id > %ld;",
            share::OB_ALL_MONITOR_MODIFIED_TNAME, all_table_name, share::OB_ALL_DATABASE_TNAME,
            OB_MAX_INNER_TABLE_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(delete_part_sql.append_fmt("DELETE /*+leading(view3, m1) use_nl(view3, m1)*/FROM %s m1 WHERE (tenant_id, table_id, tablet_id) IN ( "\
            "SELECT /*+leading(m, view1, view2, t, db) use_hash(m,view1) use_hash((m,view1),view2) use_nl((m,view1,view2),t), use_nl((m,view1,view2,t),db)*/ "\
            "m.tenant_id, m.table_id, m.tablet_id FROM %s m, %s t, %s db WHERE t.table_id = m.table_id AND t.tenant_id = m.tenant_id AND t.part_level > 0 "\
            "AND t.tenant_id = db.tenant_id AND t.database_id = db.database_id AND db.database_name != '__recyclebin' "\
            "AND NOT EXISTS (SELECT 1 FROM %s p WHERE  p.table_id = m.table_id AND p.tenant_id = m.tenant_id AND p.tablet_id = m.tablet_id) "\
            "AND NOT EXISTS (SELECT 1 FROM %s sp WHERE  sp.table_id = m.table_id AND sp.tenant_id = m.tenant_id AND sp.tablet_id = m.tablet_id)) "\
            "AND table_id > %ld;",
            share::OB_ALL_MONITOR_MODIFIED_TNAME, share::OB_ALL_MONITOR_MODIFIED_TNAME,
            all_table_name, share::OB_ALL_DATABASE_TNAME, share::OB_ALL_PART_TNAME,
            share::OB_ALL_SUB_PART_TNAME, OB_MAX_INNER_TABLE_ID))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(mysql_proxy_->write(tenant_id_, delete_table_sql.ptr(), affected_rows1))) {
    LOG_WARN("failed to execute sql", K(ret), K(delete_table_sql));
  } else if (OB_FAIL(mysql_proxy_->write(tenant_id_, delete_part_sql.ptr(), affected_rows2))) {
    LOG_WARN("failed to execute sql", K(ret), K(delete_part_sql));
  } else {
    LOG_TRACE("succeed to clean useless monitor modified_data", K(tenant_id_), K(delete_table_sql),
                                                                K(affected_rows1), K(delete_part_sql),
                                                                K(affected_rows2));
  }
  return ret;
}

int ObOptStatMonitorManager::update_dml_stat_info_from_direct_load(
    const ObIArray<ObOptDmlStat *> &dml_stats,
    ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  ObOptStatMonitorManager *optstat_monitor_mgr = NULL;
  LOG_TRACE("begin to update dml stat info from direct load", K(dml_stats));
  if (dml_stats.empty() || OB_ISNULL(dml_stats.at(0))) {
    //do nothing
  } else if (OB_UNLIKELY(dml_stats.at(0)->tenant_id_ != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(dml_stats), K(MTL_ID()));
  } else if (OB_ISNULL(optstat_monitor_mgr = MTL(ObOptStatMonitorManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(optstat_monitor_mgr));
  } else if (OB_FAIL(optstat_monitor_mgr->update_dml_stat_info(dml_stats, conn))) {
    LOG_WARN("failed to update dml stat info", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObOptStatMonitorManager::update_dml_stat_info(const ObIArray<ObOptDmlStat *> &dml_stats,
                                                  ObISQLConnection *conn)
{
  int ret = OB_SUCCESS;
  ObSqlString value_sql;
  int64_t count = 0;
  LOG_TRACE("begin to update dml stat info from direct load", K(dml_stats));
  ObSEArray<ObOptDmlStat, 16> tmp_dml_stats;
  for (int64_t i = 0; OB_SUCC(ret) && i < dml_stats.count(); ++i) {
    if (OB_ISNULL(dml_stats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpcted error", K(ret), K(dml_stats.at(i)));
    } else if (OB_FAIL(tmp_dml_stats.push_back(*dml_stats.at(i)))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      if (OB_FAIL(get_dml_stat_sql(*dml_stats.at(i), 0 != count, value_sql))) {
        LOG_WARN("failed to get dml stat sql", K(ret));
      } else if (UPDATE_OPT_STAT_BATCH_CNT == ++count) {
        if (OB_FAIL(exec_insert_monitor_modified_sql(value_sql, conn))) {
          LOG_WARN("failed to exec insert sql", K(ret));
        } else {
          count = 0;
          value_sql.reset();
        }
      }
    }
  }
  if (OB_SUCC(ret) && count != 0) {
    if (OB_FAIL(exec_insert_monitor_modified_sql(value_sql, conn))) {
      LOG_WARN("failed to exec insert sql", K(ret));
    }
  }
  if (OB_SUCC(ret) && !tmp_dml_stats.empty()) {
    bool no_check = (OB_E(EventTable::EN_LEADER_STORAGE_ESTIMATION) OB_SUCCESS) != OB_SUCCESS;
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tmp_dml_stats.at(0).tenant_id_, data_version))) {
      LOG_WARN("fail to get tenant data version", KR(ret), K(tmp_dml_stats.at(0).tenant_id_), K(data_version));
    } else if (data_version < MOCK_DATA_VERSION_4_2_4_0 ||
               (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_3_3_0) ||
               no_check) {
      //do nothing
    } else if (OB_FAIL(check_opt_stats_expired(tmp_dml_stats, true/*is_from_direct_load*/))) {
      LOG_WARN("failed to check opt stats expired", K(ret));
    } else {/*do nohting*/}
  }
  return ret;
}

int ObOptStatMonitorManager::mtl_init(ObOptStatMonitorManager* &optstat_monitor_mgr)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = lib::current_resource_owner_id();
  if (OB_LIKELY(nullptr != optstat_monitor_mgr)) {
    if (OB_FAIL(optstat_monitor_mgr->init(tenant_id))) {
      LOG_WARN("failed to init event list", K(ret));
    }
  }
  return ret;
}

int ObOptStatMonitorManager::mtl_start(ObOptStatMonitorManager* &optstat_monitor_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(nullptr != optstat_monitor_mgr)) {
    if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(), optstat_monitor_mgr->get_flush_all_task(),
                            ObOptStatMonitorFlushAllTask::FLUSH_INTERVAL, true))) {
      LOG_WARN("failed to scheduler flush all task", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(), optstat_monitor_mgr->get_check_task(),
                                   ObOptStatMonitorCheckTask::CHECK_INTERVAL, true))) {
      LOG_WARN("failed to scheduler check task", K(ret));
    } else {
      optstat_monitor_mgr->get_flush_all_task().disable_timeout_check();
      optstat_monitor_mgr->get_flush_all_task().optstat_monitor_mgr_ = optstat_monitor_mgr;
      optstat_monitor_mgr->get_check_task().disable_timeout_check();
      optstat_monitor_mgr->get_check_task().optstat_monitor_mgr_ = optstat_monitor_mgr;
    }
  }
  return ret;
}

void ObOptStatMonitorManager::mtl_stop(ObOptStatMonitorManager* &optstat_monitor_mgr)
{
  if (OB_LIKELY(nullptr != optstat_monitor_mgr)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), optstat_monitor_mgr->get_flush_all_task());
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), optstat_monitor_mgr->get_check_task());
  }
}

void ObOptStatMonitorManager::mtl_wait(ObOptStatMonitorManager* &optstat_monitor_mgr)
{
  if (OB_LIKELY(nullptr != optstat_monitor_mgr)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), optstat_monitor_mgr->get_flush_all_task());
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), optstat_monitor_mgr->get_check_task());
  }
}

int ObOptStatMonitorManager::get_col_usage_info(const bool with_check,
                                                ObIArray<StatKey> &col_stat_keys,
                                                ObIArray<int64_t> &col_flags)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (column_usage_map_.empty() ||
      (with_check && column_usage_map_.size() < 10000)) {
    //do nothing
  } else {
    for (auto iter = column_usage_map_.begin(); OB_SUCC(ret) && iter != column_usage_map_.end(); ++iter) {
      if (OB_FAIL(col_stat_keys.push_back(iter->first)) ||
          OB_FAIL(col_flags.push_back(iter->second))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      column_usage_map_.reuse();
    }
  }
  return ret;
}

int ObOptStatMonitorManager::get_dml_stats(ObIArray<ObOptDmlStat> &dml_stats)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);
  if (dml_stat_map_.empty()) {
    //do nothing
  } else {
    for (auto iter = dml_stat_map_.begin(); OB_SUCC(ret) && iter != dml_stat_map_.end(); ++iter) {
      if (OB_FAIL(dml_stats.push_back(iter->second))) {
        LOG_WARN("failed to get dml stat sql", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      dml_stat_map_.reuse();
    }
  }
  return ret;
}

int ObOptStatMonitorManager::check_opt_stats_expired(ObIArray<ObOptDmlStat> &dml_stats,
                                                     bool is_from_direct_load/*default false*/)
{
  int ret = OB_SUCCESS;
  if (!dml_stats.empty()) {
    ObSEArray<OptStatExpiredTableInfo, 4> stale_infos;
    int64_t begin_ts = ObTimeUtility::current_time();
    int64_t global_async_stale_max_table_size = DEFAULT_ASYNC_STALE_MAX_TABLE_SIZE;
    if (OB_FAIL(get_async_stale_max_table_size(dml_stats.at(0).tenant_id_,
                                               OB_INVALID_ID,
                                               global_async_stale_max_table_size))) {
      LOG_WARN("failed to get async stale max table size", K(ret));
    } else if (OB_UNLIKELY(global_async_stale_max_table_size <= 0)) {
      LOG_INFO("skip to check opt stats expired", K(global_async_stale_max_table_size));
    } else if (OB_FAIL(get_opt_stats_expired_table_info(dml_stats, stale_infos, is_from_direct_load))) {
      LOG_WARN("failed to get opt stats expired table info", K(ret));
    } else {
      const int64_t MIN_ASYNC_GATHER_TABLE_ROW_CNT = 500;
      for (int64_t i = 0; OB_SUCC(ret) && i < stale_infos.count(); ++i) {
        if (stale_infos.at(i).inserts_ <= MIN_ASYNC_GATHER_TABLE_ROW_CNT) {
          //do nothing
        } else if (OB_FAIL(mark_the_opt_stat_expired(stale_infos.at(i)))) {
          LOG_WARN("failed to mark the opt stat expired", K(ret));
        }
      }
    }
    if (ObTimeUtility::current_time() - begin_ts > ObOptStatMonitorCheckTask::CHECK_INTERVAL) {
      LOG_INFO("check opt stats expired cost too much time", K(begin_ts), K(ObTimeUtility::current_time() - begin_ts), K(dml_stats));
    }
  }
  return ret;
}

int ObOptStatMonitorManager::get_opt_stats_expired_table_info(ObIArray<ObOptDmlStat> &dml_stats,
                                                              ObIArray<OptStatExpiredTableInfo> &stale_infos,
                                                              bool is_from_direct_load)
{
  int ret = OB_SUCCESS;
  int64_t begin_idx = 0;
  while (OB_SUCC(ret) && begin_idx < dml_stats.count()) {
    ObSqlString where_list;
    int64_t end_idx = std::min(begin_idx + MAX_PROCESS_BATCH_TABLET_CNT, dml_stats.count());
    uint64_t tenant_id = dml_stats.at(begin_idx).tenant_id_;
    if (OB_FAIL(gen_tablet_list(dml_stats, begin_idx, end_idx, is_from_direct_load, where_list))) {
      LOG_WARN("failed to gen tablet list", K(ret));
    } else if (where_list.empty()) {
      //do nothing
    } else if (OB_FAIL(do_get_opt_stats_expired_table_info(tenant_id, where_list, stale_infos))) {
      LOG_WARN("failed to do get opt stats expired table info", K(ret));
    }
    begin_idx = end_idx;
  }
  return ret;
}

int ObOptStatMonitorManager::gen_tablet_list(const ObIArray<ObOptDmlStat> &dml_stats,
                                             const int64_t begin_idx,
                                             const int64_t end_idx,
                                             const bool is_from_direct_load,
                                             ObSqlString &tablet_list)
{
  int ret = OB_SUCCESS;
  tablet_list.reset();
  int64_t begin_ts = ObTimeUtility::current_time();
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(begin_idx < 0 || end_idx < 0 ||
                  begin_idx >= end_idx || end_idx > dml_stats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(begin_idx), K(end_idx), K(dml_stats));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(dml_stats.at(begin_idx).tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else {
    bool is_first = true;
    for (int64_t i = begin_idx; OB_SUCC(ret) && i < end_idx; ++i) {
      bool is_valid = is_from_direct_load && !is_inner_table(dml_stats.at(i).table_id_);
      if (!is_valid && OB_FAIL(ObDbmsStatsUtils::check_is_stat_table(schema_guard,
                                                                     dml_stats.at(i).tenant_id_,
                                                                     dml_stats.at(i).table_id_,
                                                                     false,
                                                                     is_valid))) {
        LOG_WARN("failed to check is stat table", K(ret));
      } else if (is_valid) {
        uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(dml_stats.at(i).tenant_id_, dml_stats.at(i).tenant_id_);
        uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(dml_stats.at(i).tenant_id_, dml_stats.at(i).table_id_);
        if (OB_FAIL(tablet_list.append_fmt("%s(%lu, %lu, %ld)", is_first ? "(" : " ,",
                                                                ext_tenant_id, pure_table_id,
                                                                dml_stats.at(i).tablet_id_))) {
          LOG_WARN("failed to append sql", K(ret));
        } else {
          is_first = false;
        }
      }
    }
    if (OB_SUCC(ret) && !is_first) {
      if (OB_FAIL(tablet_list.append(")"))) {
        LOG_WARN("failed to append", K(ret));
      }
    }
  }
  return ret;
}

int ObOptStatMonitorManager::do_get_opt_stats_expired_table_info(const int64_t tenant_id,
                                                                 const ObSqlString &where_str,
                                                                 ObIArray<OptStatExpiredTableInfo> &stale_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString select_sql;
  if (OB_FAIL(select_sql.append_fmt("SELECT    m.table_id, m.tablet_id, m.inserts "\
                                     "FROM      %s m " \
                                     "LEFT JOIN %s up " \
                                     "ON        m.table_id = up.table_id "\
                                     "AND       up.pname = 'ASYNC_GATHER_STALE_RATIO' "\
                                     "JOIN      %s gp "\
                                     "ON        gp.sname = 'ASYNC_GATHER_STALE_RATIO' "\
                                     "WHERE (CASE WHEN m.last_inserts = 0 THEN 1 + cast(coalesce(up.valchar, gp.spare4) as signed) "\
                                                "ELSE m.inserts * 1.0 / m.last_inserts END) > cast(coalesce(up.valchar, gp.spare4) as signed) "\
                                              "AND (m.tenant_id, m.table_id, m.tablet_id) in %s",
          share::OB_ALL_MONITOR_MODIFIED_TNAME,
          share::OB_ALL_OPTSTAT_USER_PREFS_TNAME,
          share::OB_ALL_OPTSTAT_GLOBAL_PREFS_TNAME,
          where_str.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy_);
      if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          int64_t idx1 = 0;
          int64_t idx2 = 1;
          int64_t idx3 = 2;
          ObObj obj1;
          ObObj obj2;
          ObObj obj3;
          int64_t table_id = -1;
          int64_t tablet_id = 0;
          int64_t inserts = 0;
          if (OB_FAIL(client_result->get_obj(idx1, obj1)) ||
              OB_FAIL(client_result->get_obj(idx2, obj2)) ||
              OB_FAIL(client_result->get_obj(idx3, obj3))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(obj1.get_int(table_id)) ||
                     OB_FAIL(obj2.get_int(tablet_id)) ||
                     OB_FAIL(obj3.get_int(inserts))) {
            LOG_WARN("failed to get int", K(ret), K(obj1), K(obj2), K(inserts));
          } else {
            bool is_found = false;
            for (int64_t i = 0; !is_found && OB_SUCC(ret) && i < stale_infos.count(); ++i) {
              if (table_id == stale_infos.at(i).table_id_) {
                is_found = true;
                if (OB_FAIL(stale_infos.at(i).tablet_ids_.push_back(tablet_id))) {
                  LOG_WARN("failed to push back", K(ret));
                } else {
                  stale_infos.at(i).inserts_ += inserts;
                }
              }
            }
            if (OB_SUCC(ret) && !is_found) {
              OptStatExpiredTableInfo stale_info;
              stale_info.tenant_id_ = tenant_id;
              stale_info.table_id_ = table_id;
              stale_info.inserts_ = inserts;
              if (OB_FAIL(stale_info.tablet_ids_.push_back(tablet_id))) {
                LOG_WARN("failed to push back", K(ret));
              } else if (OB_FAIL(stale_infos.push_back(stale_info))) {
                LOG_WARN("failed to push back", K(ret));
              }
            }
          }
        }
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
      }
      int tmp_ret = OB_SUCCESS;
      if (NULL != client_result) {
        if (OB_SUCCESS != (tmp_ret = client_result->close())) {
          LOG_WARN("close result set failed", K(ret), K(tmp_ret));
          ret = COVER_SUCC(tmp_ret);
        }
      }
    }
    LOG_TRACE("do get opt stats expired table info", K(select_sql), K(stale_infos));
  }
  return ret;
}

int ObOptStatMonitorManager::mark_the_opt_stat_expired(const OptStatExpiredTableInfo &expired_table_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<PartInfo, 4> part_infos;
  ObSEArray<PartInfo, 4> subpart_infos;
  ObSEArray<int64_t, 4> partition_ids;
  ObSEArray<int64_t, 4> expired_partition_ids;
  share::schema::ObPartitionLevel part_level = share::schema::ObPartitionLevel::PARTITION_LEVEL_MAX;
  ObSEArray<ObOptTableStat, 4> table_stats;
  ObSEArray<ObOptTableStat, 4> expired_table_stats;
  ObSEArray<ObOptTableStat, 4> no_table_stats;
  ObArenaAllocator allocator("OptStatMonitor", OB_MALLOC_NORMAL_BLOCK_SIZE, expired_table_info.tenant_id_);
  int64_t begin_ts = ObTimeUtility::current_time();
  int64_t async_stale_max_table_size = DEFAULT_ASYNC_STALE_MAX_TABLE_SIZE;
  if (OB_FAIL(get_expired_table_part_info(allocator, expired_table_info, part_level, part_infos, subpart_infos))) {
    LOG_WARN("failed to get expired table part info", K(ret));
  } else if (part_level == share::schema::ObPartitionLevel::PARTITION_LEVEL_MAX) {
    //do nothing
  } else if (OB_FAIL(get_need_check_opt_stat_partition_ids(expired_table_info,
                                                           part_infos,
                                                           subpart_infos,
                                                           partition_ids))) {
    LOG_WARN("failed to get need check opt stat partition ids", K(ret));
  } else if (OB_FAIL(ObOptStatManager::get_instance().get_table_stat(expired_table_info.tenant_id_,
                                                                     expired_table_info.table_id_,
                                                                     partition_ids,
                                                                     table_stats))) {
    LOG_WARN("failed to get table stat", K(ret));
  } else if (OB_FAIL(get_async_stale_max_table_size(expired_table_info.tenant_id_,
                                                    expired_table_info.table_id_,
                                                    async_stale_max_table_size))) {
    LOG_WARN("failed to get async stale max table size", K(ret));
  } else if (OB_UNLIKELY(async_stale_max_table_size <= 0)) {
    LOG_INFO("skip to mark the opt stat expired", K(async_stale_max_table_size));
  } else if (OB_FAIL(get_need_mark_opt_stats_expired(table_stats,
                                                     expired_table_info,
                                                     async_stale_max_table_size,
                                                     begin_ts,
                                                     part_level,
                                                     part_infos,
                                                     subpart_infos,
                                                     expired_table_stats,
                                                     no_table_stats))) {
    LOG_WARN("failed to get need mark opt stats expired", K(ret));
  } else if (OB_FAIL(do_mark_the_opt_stat_expired(expired_table_info.tenant_id_,
                                                  expired_table_stats,
                                                  expired_partition_ids))) {
    LOG_WARN("failed to do mark the opt stat expired", K(ret));
  } else if (OB_FAIL(do_mark_the_opt_stat_missing(expired_table_info.tenant_id_,
                                                  no_table_stats))) {
    LOG_WARN("failed to do mark the opt stat missing", K(ret));
  } else {
    obrpc::ObUpdateStatCacheArg stat_arg;
    stat_arg.tenant_id_ = expired_table_info.tenant_id_;
    stat_arg.table_id_ = expired_table_info.table_id_;
    if (OB_FAIL(append(stat_arg.partition_ids_, expired_partition_ids))) {
      LOG_WARN("failed to append", K(ret));
    } else if (OB_FAIL(pl::ObDbmsStats::update_stat_cache(expired_table_info.tenant_id_, stat_arg))) {
      LOG_WARN("failed to update stat cache", K(ret));
    }
  }
  return ret;
}

int ObOptStatMonitorManager::get_expired_table_part_info(ObIAllocator &allocator,
                                                         const OptStatExpiredTableInfo &expired_table_info,
                                                         share::schema::ObPartitionLevel &part_level,
                                                         ObIArray<PartInfo> &part_infos,
                                                         ObIArray<PartInfo> &subpart_infos)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  part_level = share::schema::ObPartitionLevel::PARTITION_LEVEL_MAX;
  part_infos.reset();
  subpart_infos.reset();
  if (OB_ISNULL(GCTX.schema_service_) || OB_UNLIKELY(!expired_table_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(expired_table_info), K(GCTX.schema_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(expired_table_info.tenant_id_, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(expired_table_info.tenant_id_,
                                                   expired_table_info.table_id_,
                                                   table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(expired_table_info.tenant_id_), K(expired_table_info.table_id_));
  } else if (OB_ISNULL(table_schema)) {//maybe table isn't exists.
    //do nothing
  } else if (OB_FAIL(pl::ObDbmsStats::get_table_part_infos(table_schema, allocator, part_infos, subpart_infos))) {
    LOG_WARN("failed to get table part infos", K(ret));
  } else {
    part_level = table_schema->get_part_level();
  }
  return ret;
}

int ObOptStatMonitorManager::get_need_check_opt_stat_partition_ids(const OptStatExpiredTableInfo &expired_table_info,
                                                                   ObIArray<PartInfo> &part_infos,
                                                                   ObIArray<PartInfo> &subpart_infos,
                                                                   ObIArray<int64_t> &partition_ids)
{
  int ret = OB_SUCCESS;
  //non partition table
  if (part_infos.empty() && subpart_infos.empty()) {
    if (OB_FAIL(partition_ids.push_back(expired_table_info.table_id_))) {
      LOG_WARN("failed to push back", K(ret));
    }
  //partition table, global stat partition id is -1
  } else if (OB_FAIL(partition_ids.push_back(-1))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (!part_infos.empty() && subpart_infos.empty()) {//part table
    if (expired_table_info.tablet_ids_.count() == part_infos.count()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_infos.count(); ++i) {
        if (OB_FAIL(partition_ids.push_back(part_infos.at(i).part_id_))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expired_table_info.tablet_ids_.count(); ++i) {
        bool found_it = false;
        for (int64_t j = 0; OB_SUCC(ret) && !found_it && j < part_infos.count(); ++j) {
          if (expired_table_info.tablet_ids_.at(i) == static_cast<int64_t>(part_infos.at(j).tablet_id_.id())) {
            if (OB_FAIL(partition_ids.push_back(part_infos.at(j).part_id_))) {
              LOG_WARN("failed to push back", K(ret));
            } else {
              found_it = true;
            }
          }
        }
      }
    }
  } else if (!part_infos.empty() && !subpart_infos.empty()) {//subpart table
    hash::ObHashMap<int64_t, bool> partition_ids_map;
    if (expired_table_info.tablet_ids_.count() == subpart_infos.count()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < part_infos.count(); ++i) {
        if (OB_FAIL(partition_ids.push_back(part_infos.at(i).part_id_))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < subpart_infos.count(); ++i) {
        if (OB_FAIL(partition_ids.push_back(subpart_infos.at(i).part_id_))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    } else if (OB_FAIL(partition_ids_map.create(part_infos.count(), "PartIdsMap", "PartIdsMapNode", expired_table_info.tenant_id_))) {
      LOG_WARN("fail to create hash map", K(ret), K(expired_table_info.tablet_ids_.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expired_table_info.tablet_ids_.count(); ++i) {
        bool found_it = false;
        for (int64_t j = 0; OB_SUCC(ret) && !found_it && j < subpart_infos.count(); ++j) {
          if (expired_table_info.tablet_ids_.at(i) == static_cast<int64_t>(subpart_infos.at(j).tablet_id_.id())) {
            bool tmp_var = false;
            if (OB_FAIL(partition_ids.push_back(subpart_infos.at(j).part_id_))) {
              LOG_WARN("failed to push back", K(ret));
            } else {
              found_it = true;
              if (OB_FAIL(partition_ids_map.get_refactored(subpart_infos.at(j).first_part_id_, tmp_var))) {
                if (OB_HASH_NOT_EXIST == ret) {
                  ret = OB_SUCCESS;
                  if (OB_FAIL(partition_ids.push_back(subpart_infos.at(j).first_part_id_))) {
                    LOG_WARN("failed to push back", K(ret));
                  } else if (OB_FAIL(partition_ids_map.set_refactored(subpart_infos.at(j).first_part_id_, true))) {
                    LOG_WARN("failed to set refactored", K(ret));
                  } else {/*do nothing*/}
                } else {
                  LOG_WARN("failed to get refactored", K(ret));
                }
              }
            }
          }
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret));
  }
  LOG_INFO("get need check opt stat partition ids", K(expired_table_info), K(part_infos),
                                                     K(subpart_infos), K(partition_ids));
  return ret;
}

int ObOptStatMonitorManager::get_need_mark_opt_stats_expired(const ObIArray<ObOptTableStat> &table_stats,
                                                             const OptStatExpiredTableInfo &expired_table_info,
                                                             const int64_t async_stale_max_table_size,
                                                             const int64_t begin_ts,
                                                             const share::schema::ObPartitionLevel &part_level,
                                                             const ObIArray<PartInfo> &part_infos,
                                                             const ObIArray<PartInfo> &subpart_infos,
                                                             ObIArray<ObOptTableStat> &expired_table_stats,
                                                             ObIArray<ObOptTableStat> &no_table_stats)
{
  int ret = OB_SUCCESS;
  bool have_table_stats = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stats.count(); ++i) {
    bool is_stat_expired = false;
    have_table_stats |= table_stats.at(i).get_last_analyzed() > 0;
    if (table_stats.at(i).get_last_analyzed() <= 0) {
      if (!table_stats.at(i).is_stat_expired()) {
        if (OB_FAIL(no_table_stats.push_back(table_stats.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    } else if (table_stats.at(i).is_stat_expired() ||
               table_stats.at(i).get_last_analyzed() >= begin_ts ||
               table_stats.at(i).get_row_count() > async_stale_max_table_size) {
      //do nothing
    } else if (part_level == share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO) {
      is_stat_expired = true;
    } else if (part_level == share::schema::ObPartitionLevel::PARTITION_LEVEL_ONE) {
      if (table_stats.count() == part_infos.count() + 1 ||
          table_stats.at(i).get_object_type() == StatLevel::PARTITION_LEVEL) {
        is_stat_expired = true;
      } else if (table_stats.at(i).get_object_type() == StatLevel::TABLE_LEVEL) {
        ObSEArray<uint64_t, 4> tablet_ids;
        if (OB_FAIL(check_table_stat_expired_by_dml_info(expired_table_info.tenant_id_,
                                                         expired_table_info.table_id_,
                                                         tablet_ids,
                                                         is_stat_expired))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpcted error", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(table_stats.at(i)));
      }
    } else if (part_level == share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO) {
      if (table_stats.count() == part_infos.count() + subpart_infos.count() + 1 ||
          table_stats.at(i).get_object_type() == StatLevel::SUBPARTITION_LEVEL) {
        is_stat_expired = true;
      } else if (table_stats.at(i).get_object_type() == StatLevel::PARTITION_LEVEL) {
        ObSEArray<uint64_t, 4> tablet_ids;
        bool is_all_subpart_expired = true;
        for (int64_t j = 0; OB_SUCC(ret) && j < subpart_infos.count(); ++j) {
          if (table_stats.at(i).get_partition_id() == subpart_infos.at(j).first_part_id_) {
            if (OB_FAIL(tablet_ids.push_back(subpart_infos.at(j).tablet_id_.id()))) {
              LOG_WARN("failed to push back", K(ret));
            } else if (is_all_subpart_expired) {
              bool found_it = false;
              for (int64_t k = 0; !found_it && k < table_stats.count(); ++k) {
                if (table_stats.at(i).get_partition_id() == subpart_infos.at(j).part_id_) {
                  found_it = true;
                  is_all_subpart_expired &= table_stats.at(i).get_last_analyzed() > 0;
                }
              }
              is_all_subpart_expired &= found_it;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (is_all_subpart_expired) {
            is_stat_expired = true;
          } else if (OB_FAIL(check_table_stat_expired_by_dml_info(expired_table_info.tenant_id_,
                                                                  expired_table_info.table_id_,
                                                                  tablet_ids,
                                                                  is_stat_expired))) {
            LOG_WARN("failed to check table stat expired by dml info", K(ret));
          }
        }
      } else if (table_stats.at(i).get_object_type() == StatLevel::TABLE_LEVEL) {
        ObSEArray<uint64_t, 4> tablet_ids;
        if (OB_FAIL(check_table_stat_expired_by_dml_info(expired_table_info.tenant_id_,
                                                         expired_table_info.table_id_,
                                                         tablet_ids,
                                                         is_stat_expired))) {
          LOG_WARN("failed to check table stat expired by dml info", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(table_stats.at(i)));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(table_stats.at(i)));
    }
    if (OB_SUCC(ret) && is_stat_expired) {
      if (OB_FAIL(expired_table_stats.push_back(table_stats.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
    if (OB_SUCC(ret) && have_table_stats) {
      no_table_stats.reset();
    }
  }
  LOG_INFO("get need mark opt stats expired", K(expired_table_stats), K(no_table_stats));
  return ret;
}

int ObOptStatMonitorManager::check_table_stat_expired_by_dml_info(const uint64_t tenant_id,
                                                                  const uint64_t table_id,
                                                                  const ObIArray<uint64_t> &tablet_ids,
                                                                  bool &is_stat_expired)
{
  int ret = OB_SUCCESS;
  ObSqlString tablet_list;
  is_stat_expired = false;
  if (OB_FAIL(gen_tablet_list(tablet_ids, tablet_list))) {
    LOG_WARN("failed to gen tablet list", K(ret));
  } else {
    uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
    uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, table_id);
    ObSqlString select_sql;
    if (OB_FAIL(select_sql.append_fmt("SELECT 1 "\
                                       "FROM  (SELECT  table_id,"\
                                                      "sum(inserts-deletes) AS row_cnt,"\
                                                      "sum(inserts+updates+deletes) AS total_modified_cnt,"\
                                                      "sum(last_inserts+last_updates+last_deletes) AS last_modified_cnt "\
                                                  "from     %s "\
                                                  "WHERE    tenant_id = %lu "\
                                                  "AND      table_id = %lu %s%s "\
                                                  "GROUP BY table_id) m "\
                                        "LEFT JOIN %s up "\
                                        "ON        m.table_id = up.table_id "\
                                        "AND       up.pname = 'ASYNC_GATHER_STALE_RATIO' "\
                                        "JOIN      %s gp "\
                                        "ON        gp.sname = 'ASYNC_GATHER_STALE_RATIO' "\
                                        "WHERE     (CASE WHEN last_modified_cnt = 0 THEN 1 + cast(coalesce(up.valchar, gp.spare4) as signed) "\
                                                    "ELSE total_modified_cnt * 1.0 / last_modified_cnt END) > cast(COALESCE(up.valchar, gp.spare4) AS signed) "\
                                        "AND row_cnt > 0;",
          share::OB_ALL_MONITOR_MODIFIED_TNAME,
          ext_tenant_id,
          pure_table_id,
          tablet_list.empty() ? " " : " AND tablet_id in ",
          tablet_list.empty() ? " " : tablet_list.ptr(),
          share::OB_ALL_OPTSTAT_USER_PREFS_TNAME,
          share::OB_ALL_OPTSTAT_GLOBAL_PREFS_TNAME
          ))) {
      LOG_WARN("failed to append fmt", K(ret));
    } else {
      LOG_TRACE("check table stat expired by dml info", K(select_sql));
      SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
        sqlclient::ObMySQLResult *client_result = NULL;
        ObSQLClientRetryWeak sql_client_retry_weak(mysql_proxy_);
        if (OB_FAIL(sql_client_retry_weak.read(proxy_result, tenant_id, select_sql.ptr()))) {
          LOG_WARN("failed to execute sql", K(ret), K(select_sql));
        } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to execute sql", K(ret));
        } else {
          while (OB_SUCC(ret) && !is_stat_expired && OB_SUCC(client_result->next())) {
            is_stat_expired = true;
          }
          ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
        }
        int tmp_ret = OB_SUCCESS;
        if (NULL != client_result) {
          if (OB_SUCCESS != (tmp_ret = client_result->close())) {
            LOG_WARN("close result set failed", K(ret), K(tmp_ret));
            ret = COVER_SUCC(tmp_ret);
          }
        }
      }
    }
    LOG_TRACE("check_table_stat_expired_by_dml_info end", K(is_stat_expired));
  }
  return ret;
}

int ObOptStatMonitorManager::gen_tablet_list(const ObIArray<uint64_t> &tablet_ids,
                                             ObSqlString &tablet_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
    char prefix = (i == 0 ? '(' : ' ');
    char suffix = (i == tablet_ids.count() - 1 ? ')' : ',');
    if (OB_FAIL(tablet_list.append_fmt("%c%lu%c", prefix, tablet_ids.at(i), suffix))) {
      LOG_WARN("failed to append sql", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObOptStatMonitorManager::do_mark_the_opt_stat_missing(const uint64_t tenant_id,
                                                          const ObIArray<ObOptTableStat> &no_table_stats)
{
  int ret = OB_SUCCESS;
  if (!no_table_stats.empty()) {
    int64_t begin_idx = 0;
    ObMySQLTransaction trans;
    if (OB_ISNULL(mysql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(mysql_proxy_));
    } else if (OB_FAIL(trans.start(mysql_proxy_, tenant_id))) {
      LOG_WARN("fail to start transaction", K(ret));
    } else {
      while (OB_SUCC(ret) && begin_idx < no_table_stats.count()) {
        ObSqlString insert_sql;
        ObSqlString values_list;
        int64_t affected_rows = 0;
        int64_t end_idx = std::min(begin_idx + MAX_PROCESS_BATCH_TABLET_CNT, no_table_stats.count());
        if (OB_FAIL(gen_values_list(tenant_id, no_table_stats, begin_idx, end_idx, values_list))) {
          LOG_WARN("failed to gen values list", K(ret));
        } else if (OB_UNLIKELY(values_list.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(values_list));
        } else if (OB_FAIL(insert_sql.append_fmt(INSERT_STALE_TABLE_STAT_SQL,
                                                 share::OB_ALL_TABLE_STAT_TNAME,
                                                 values_list.ptr()))) {
        } else if (OB_FAIL(trans.write(tenant_id, insert_sql.ptr(), affected_rows))) {
          LOG_WARN("fail to exec sql", K(insert_sql), K(ret));
        } else {
          begin_idx = end_idx;
          LOG_INFO("Succeed to do mark the opt stat expired", K(insert_sql), K(no_table_stats), K(affected_rows));
        }
      }
      //end gather trans
      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans.end(true))) {
          LOG_WARN("fail to commit transaction", K(ret));
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
          LOG_WARN("fail to roll back transaction", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObOptStatMonitorManager::do_mark_the_opt_stat_expired(const uint64_t tenant_id,
                                                          const ObIArray<ObOptTableStat> &expired_table_stats,
                                                          ObIArray<int64_t> &expired_partition_ids)
{
  int ret = OB_SUCCESS;
  int64_t begin_idx = 0;
  if (OB_ISNULL(mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mysql_proxy_));
  }
  while (OB_SUCC(ret) && begin_idx < expired_table_stats.count()) {
    ObSqlString update_sql;
    ObSqlString same_part_analyzed_list;
    ObSqlString diff_part_analyzed_list;
    int64_t affected_rows = 0;
    int64_t end_idx = std::min(begin_idx + MAX_PROCESS_BATCH_TABLET_CNT, expired_table_stats.count());
    uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
    uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, expired_table_stats.at(begin_idx).get_table_id());
    if (OB_FAIL(gen_part_analyzed_list(expired_table_stats, begin_idx, end_idx,
                                       same_part_analyzed_list,
                                       diff_part_analyzed_list,
                                       expired_partition_ids))) {
      LOG_WARN("failed to gen part analyzed list", K(ret));
    } else if (OB_FAIL(update_sql.append_fmt("update /*+QUERY_TIMEOUT(60000000)*/%s set stale_stats = 1 where tenant_id = %lu and table_id = %lu and %s",
                                              share::OB_ALL_TABLE_STAT_TNAME,
                                              ext_tenant_id,
                                              pure_table_id,
                                              !same_part_analyzed_list.empty() ? same_part_analyzed_list.ptr() : diff_part_analyzed_list.ptr()))) {
    } else if (OB_FAIL(mysql_proxy_->write(tenant_id, update_sql.ptr(), affected_rows))) {
      LOG_WARN("fail to exec sql", K(update_sql), K(ret));
    } else {
      begin_idx = end_idx;
      LOG_INFO("Succeed to do mark the opt stat expired", K(update_sql), K(expired_table_stats), K(affected_rows));
    }
  }
  return ret;
}

int ObOptStatMonitorManager::gen_part_analyzed_list(const ObIArray<ObOptTableStat> &expired_table_stats,
                                                    const int64_t begin_idx,
                                                    const int64_t end_idx,
                                                    ObSqlString &same_part_analyzed_list,
                                                    ObSqlString &diff_part_analyzed_list,
                                                    ObIArray<int64_t> &expired_partition_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(begin_idx < 0 || end_idx < 0 ||
                  begin_idx >= end_idx || end_idx > expired_table_stats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(begin_idx), K(end_idx), K(expired_table_stats));
  } else {
    int64_t last_analyzed = -1;
    for (int64_t i = begin_idx; OB_SUCC(ret) && i < end_idx; ++i) {
      char suffix = (i == end_idx - 1 ? ')' : ',');
      if (OB_FAIL(expired_partition_ids.push_back(expired_table_stats.at(i).get_partition_id()))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(diff_part_analyzed_list.append_fmt("%s(%ld,usec_to_time(%ld))%c", i == begin_idx ? "(partition_id, last_analyzed) in (" : " ",
                                                                                           expired_table_stats.at(i).get_partition_id(),
                                                                                           expired_table_stats.at(i).get_last_analyzed(),
                                                                                           suffix))) {
        LOG_WARN("failed to append sql", K(ret));
      } else if (i == begin_idx || last_analyzed == expired_table_stats.at(i).get_last_analyzed()) {
        last_analyzed = expired_table_stats.at(i).get_last_analyzed();
        if (OB_FAIL(same_part_analyzed_list.append_fmt("%s%ld%c", i == begin_idx ? "partition_id in (" : " ",
                                                                  expired_table_stats.at(i).get_partition_id(),
                                                                  suffix))) {
          LOG_WARN("failed to append sql", K(ret));
        } else if (i == end_idx - 1) {
          if (OB_FAIL(same_part_analyzed_list.append_fmt(" AND last_analyzed = usec_to_time(%ld)", last_analyzed))) {
            LOG_WARN("failed to append sql", K(ret));
          } else {
            diff_part_analyzed_list.reset();
          }
        }
      } else {
        last_analyzed = -1;
        same_part_analyzed_list.reset();
      }
    }
  }
  return ret;
}

int ObOptStatMonitorManager::gen_values_list(const uint64_t tenant_id,
                                             const ObIArray<ObOptTableStat> &no_table_stats,
                                             const int64_t begin_idx,
                                             const int64_t end_idx,
                                             ObSqlString &values_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(begin_idx < 0 || end_idx < 0 ||
                  begin_idx >= end_idx || end_idx > no_table_stats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(begin_idx), K(end_idx), K(no_table_stats));
  } else {
    for (int64_t i = begin_idx; OB_SUCC(ret) && i < end_idx; ++i) {
      ObSqlString value;
      uint64_t ext_tenant_id = share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
      uint64_t pure_table_id = share::schema::ObSchemaUtils::get_extract_schema_id(tenant_id, no_table_stats.at(i).get_table_id());
      if (OB_FAIL(value.append_fmt(STALE_TABLE_STAT_MOCK_VALUE_PATTERN,
                                   ext_tenant_id,
                                   pure_table_id,
                                   no_table_stats.at(i).get_partition_id()))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(values_list.append_fmt("%s%s", i == begin_idx ? " " : ", ", value.ptr()))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObOptStatMonitorManager::get_async_stale_max_table_size(const uint64_t tenant_id,
                                                            const uint64_t table_id,
                                                            int64_t &async_stale_max_table_size)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_alloc("OptStatPrefs", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObAsyncStaleMaxTableSizePrefs prefs;
  ObString opt_name(prefs.get_stat_pref_name());
  ObObj result;
  ObObj dest_obj;
  ObCastCtx cast_ctx(&tmp_alloc, NULL, CM_NONE, ObCharset::get_system_collation());
  async_stale_max_table_size = DEFAULT_ASYNC_MAX_SCAN_ROWCOUNT;
  if (OB_FAIL(ObDbmsStatsPreferences::get_prefs(mysql_proxy_, tmp_alloc, tenant_id,
                                                table_id, opt_name, result))) {
    LOG_WARN("failed to get prefs", K(ret));
  } else if (result.is_null()) {
    //do nothing
  } else if (OB_FAIL(ObObjCaster::to_type(ObNumberType, cast_ctx, result, dest_obj))) {
    LOG_WARN("failed to type", K(ret), K(result));
  } else if (OB_FAIL(dest_obj.get_number().extract_valid_int64_with_trunc(async_stale_max_table_size))) {
    LOG_WARN("failed to extract valid int64 with trunc", K(ret), K(result));
  } else if (async_stale_max_table_size < 0) {
    ret = OB_ERR_DBMS_STATS_PL;
    LOG_WARN("Illegal async stale max table size", K(ret), K(async_stale_max_table_size));
  }
  LOG_TRACE("get_async_stale_max_table_size", K(async_stale_max_table_size), K(result));
  return ret;
}

}
}