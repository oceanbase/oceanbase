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
#include "lib/rc/ob_rc.h"
#include "observer/ob_server.h"

namespace oceanbase
{
using namespace observer;

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

void ObOptStatMonitorFlushAllTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(optstat_monitor_mgr_) && optstat_monitor_mgr_->inited_) {
    LOG_INFO("run opt stat monitor flush all task", K(optstat_monitor_mgr_->tenant_id_));
    share::schema::ObMultiVersionSchemaService &schema_service = share::schema::ObMultiVersionSchemaService::get_instance();
    share::schema::ObSchemaGetterGuard schema_guard;
    bool in_restore = false;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(optstat_monitor_mgr_->tenant_id_, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.check_tenant_is_restore(optstat_monitor_mgr_->tenant_id_, in_restore))) {
      LOG_WARN("failed to check tenant is restore", K(ret));
    } else if (in_restore || GCTX.is_standby_cluster()) {
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
    share::schema::ObMultiVersionSchemaService &schema_service = share::schema::ObMultiVersionSchemaService::get_instance();
    share::schema::ObSchemaGetterGuard schema_guard;
    bool in_restore = false;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(optstat_monitor_mgr_->tenant_id_, schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.check_tenant_is_restore(optstat_monitor_mgr_->tenant_id_, in_restore))) {
      LOG_WARN("failed to check tenant is restore", K(ret));
    } else if (in_restore || GCTX.is_standby_cluster()) {
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

int ObOptStatMonitorManager::update_local_cache(common::ObIArray<ColumnUsageArg> &args)
{
  int ret = OB_SUCCESS;
  if (GCTX.is_standby_cluster()) {
    // standby cluster can't write __all_column_usage, so do not need to update local update
  } else {
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
  }
  return ret;
}

int ObOptStatMonitorManager::update_local_cache(ObOptDmlStat &dml_stat)
{
  int ret = OB_SUCCESS;
  if (GCTX.is_standby_cluster()) {
    // standby cluster can't write __all_monitor_modified, so do not need to update local update
  } else {
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
      if (OB_FAIL(clean_useless_dml_stat_info())) {
        LOG_WARN("failed to clean useless dml stat info", K(ret));
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
  bool in_restore = false;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->check_tenant_is_restore(NULL, tenant_id_, in_restore))) {
    LOG_WARN("failed to check tenant is restore", K(ret));
  } else if (OB_UNLIKELY(in_restore) || GCTX.is_standby_cluster()) {
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

int ObOptStatMonitorManager::exec_insert_monitor_modified_sql(ObSqlString &values_sql)
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
  } else if (OB_FAIL(mysql_proxy_->write(tenant_id_, insert_sql.ptr(), affected_rows))) {
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
  } else if (OB_FAIL(delete_part_sql.append_fmt("DELETE /*+use_nl(m1)*/FROM %s m1 WHERE (tenant_id, table_id, tablet_id) IN ( "\
            "SELECT /*+leading(db, t, m, view1, view2) use_hash(m) use_hash(view1) use_hash(view2)*/ "\
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

int ObOptStatMonitorManager::update_dml_stat_info_from_direct_load(const ObIArray<ObOptDmlStat *> &dml_stats)
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
  } else if (OB_FAIL(optstat_monitor_mgr->update_dml_stat_info(dml_stats))) {
    LOG_WARN("failed to update dml stat info", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObOptStatMonitorManager::update_dml_stat_info(const ObIArray<ObOptDmlStat *> &dml_stats)
{
  int ret = OB_SUCCESS;
  ObSqlString value_sql;
  int count = 0;
  LOG_TRACE("begin to update dml stat info from direct load", K(dml_stats));
  for (int64_t i = 0; OB_SUCC(ret) && i < dml_stats.count(); ++i) {
    if (OB_ISNULL(dml_stats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpcted error", K(ret), K(dml_stats.at(i)));
    } else {
      if (OB_FAIL(get_dml_stat_sql(*dml_stats.at(i), 0 != count, value_sql))) {
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
  }
  if (OB_SUCC(ret) && count != 0) {
    if (OB_FAIL(exec_insert_monitor_modified_sql(value_sql))) {
      LOG_WARN("failed to exec insert sql", K(ret));
    }
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

}
}
