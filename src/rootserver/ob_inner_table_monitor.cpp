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

#define USING_LOG_PREFIX SERVER

#include "ob_inner_table_monitor.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/ob_role_mgr.h"
#include "share/config/ob_server_config.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "rootserver/ob_root_service.h"
namespace oceanbase {
using namespace common;
using namespace share;

namespace rootserver {

#define PURGE_SQL1                                                              \
  "/* purge_sql1 */ "                                                           \
  " DELETE FROM %s PARTITION(p%d) WHERE (%s, schema_version) IN ( "             \
  "       SELECT %s, t1.schema_version FROM %s PARTITION(p%d) t1 "              \
  "       JOIN (SELECT %s, max(schema_version) as max_v FROM %s PARTITION(p%d)" \
  "         WHERE tenant_id=%ld and schema_version < %ld GROUP BY %s "          \
  "         ) t2 "                                                              \
  "       USING(%s) WHERE t1.schema_version < t2.max_v LIMIT 1000"              \
  "    ) LIMIT 1000"

#define PURGE_SQL2                                                                        \
  "/* purge_sql2 */ "                                                                     \
  " DELETE FROM %s PARTITION(p%d) WHERE (%s, schema_version) IN ( "                       \
  "       SELECT %s, t1.schema_version FROM %s PARTITION(p%d) t1 "                        \
  "       JOIN (SELECT %s, max(schema_version) as max_v FROM %s PARTITION(p%d)"           \
  "         WHERE tenant_id=%ld and schema_version < %ld and is_deleted = 1 GROUP BY %s " \
  "         ) t2 "                                                                        \
  "       USING(%s) LIMIT 1000) LIMIT 1000"

//__all_column_history:
#define PREFIX_PRIMARY_KEY_FOR_COLUMN_HISTORY "`tenant_id`, `table_id`, `column_id`"

// __all_table_history:
#define PREFIX_PRIMARY_KEY_FOR_TABLE_HISTORY "`tenant_id`, `table_id`"

// __all_tablegroup_history:
#define PREFIX_PRIMARY_KEY_FOR_TABLEGROUP_HISTORY "`tenant_id`, `tablegroup_id`"

// __all_outline_history:
#define PREFIX_PRIMARY_KEY_FOR_OUTLINE_HISTORY "`tenant_id`, `outline_id`"

// __all_database_history:
#define PREFIX_PRIMARY_KEY_FOR_DATABASE_HISTORY "`tenant_id`, `database_id`"

// __all_tenant_history
#define PREFIX_PRIMARY_KEY_FOR_TENANT_HISTORY "`tenant_id`"

// __all_user_history:
#define PREFIX_PRIMARY_KEY_FOR_USER_HISTORY "`tenant_id`, `user_id`"

// __all_database_privilege_history:
#define PREFIX_PRIMARY_KEY_FOR_DATABASE_PRIVILEGE_HISTORY "`tenant_id`, `user_id`, `database_name`"

// __all_table_privilege_history:
#define PREFIX_PRIMARY_KEY_FOR_TABLE_PRIVILEGE_HISTORY "`tenant_id`, `user_id`, `database_name`, `table_name`"

// __all_ddl_operation:
#define PREFIX_PRIMARY_KEY_FOR_DDL_OPERATION "`tenant_id`"

ObInnerTableMonitor::ObInnerTableMonitor() : inited_(false), rs_proxy_(NULL), sql_proxy_(NULL), root_service_(NULL)
{}

int ObInnerTableMonitor::init(ObMySQLProxy& mysql_proxy, obrpc::ObCommonRpcProxy& rs_proxy, ObRootService& root_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice");
  } else {
    sql_proxy_ = &mysql_proxy;
    rs_proxy_ = &rs_proxy;
    root_service_ = &root_service;
    inited_ = true;
  }
  return ret;
}

int ObInnerTableMonitor::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", K(ret));
  } else if (OB_ISNULL(rs_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common_proxy_ is null", K(ret));
  } else if (OB_ISNULL(root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_service is null", K(ret));
  }
  return ret;
}

int ObInnerTableMonitor::get_all_tenants_from_stats(ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    if (OB_FAIL(check_inner_stat())) {
      LOG_WARN("check inner stat failed", K(ret));
    } else if (OB_FAIL(sql.append_fmt("SELECT distinct tenant_id FROM %s", OB_ALL_TENANT_HISTORY_TNAME))) {
      LOG_WARN("assign sql string failed", K(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get result failed", K(ret));
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next result failed", K(ret));
        }
      } else {
        int64_t tenant_id = 0;
        if (OB_FAIL(result->get_int("tenant_id", tenant_id))) {
          LOG_WARN("get tenant id failed", K(ret));
        } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
          LOG_WARN("push back tenant id failed", K(ret));
        }
      }
    }
  }
  return ret;
}

// 1. select global last_merged_version from __all_zone
// 2. use last_merged_version -1 as purge_frozen_verion
// 3. select frozen_timestamp from __all_frozen_map for purge_frozen_verion
int ObInnerTableMonitor::get_last_forzen_time(int64_t& last_frozen_time)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    int64_t last_merged_version = 0;
    int64_t frozen_timestamp = 0;

    if (OB_FAIL(check_inner_stat())) {
      LOG_WARN("check inner stat failed", K(ret));
    } else if (OB_FAIL(
                   sql.append_fmt("SELECT * FROM %s WHERE name = 'last_merged_version' LIMIT 1", OB_ALL_ZONE_TNAME))) {
      LOG_WARN("assign sql string failed", K(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get result failed", K(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("get next result failed", K(ret));
    } else if (OB_FAIL(result->get_int("value", last_merged_version))) {
      LOG_WARN("get last_merged_version failed", K(ret));
    }

    // for frozen_map, initial is empty, then don't purge any record.
    // if only one record in frozen_map, also don't purge any record.
    if (OB_SUCC(ret) && last_merged_version > 0) {
      last_merged_version--;
      sql.reset();
      if (OB_FAIL(sql.append_fmt("SELECT frozen_timestamp FROM %s WHERE frozen_version = %ld",
              OB_ALL_FROZEN_MAP_TNAME,
              last_merged_version))) {
        LOG_WARN("assign sql string failed", K(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(sql), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          // do nothing, reset ret to success
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get next result failed", K(ret));
        }
      } else if (OB_FAIL(result->get_int("frozen_timestamp", frozen_timestamp))) {
        LOG_WARN("get frozen_timestamp failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      last_frozen_time = frozen_timestamp;
    }
  }
  return ret;
}

int ObInnerTableMonitor::purge_inner_table_history()
{
  bool ret = OB_SUCCESS;
  ObSEArray<uint64_t, 16> tenant_ids;
  if (OB_FAIL(get_all_tenants_from_stats(tenant_ids))) {
    LOG_WARN("get_all_tenants_from_stats failed", K(ret));
  } else if (OB_FAIL(purge_recyclebin_objects(tenant_ids))) {
    LOG_WARN("purge recyclebin objects failed", K(ret));
  }
  return ret;
}

int ObInnerTableMonitor::purge_inner_table_history(
    const uint64_t tenant_id, const int64_t last_frozen_time, int64_t& purged_rows)
{
  int ret = OB_SUCCESS;
  int last_ret = OB_SUCCESS;

  int64_t current_time = ObTimeUtility::current_time();
  const int64_t delta_sec = 3600;  // one hour
  const int64_t schema_history_expire_time = GCONF.schema_history_expire_time;
  int64_t min_schema_version = current_time - schema_history_expire_time;
  // can not delete history since last froze_time - delta
  min_schema_version = min(min_schema_version, last_frozen_time - delta_sec);

  purged_rows = 0;
  if (OB_FAIL(purge_table_column_history(tenant_id, min_schema_version, purged_rows))) {
    last_ret = ret;
    LOG_WARN("purage_table_column_history failed", K(ret));
  }
  if (OB_FAIL(purge_history(OB_ALL_TABLEGROUP_HISTORY_TNAME,
          N_PARTITION_FOR_TABLEGROUP_HISTORY,
          tenant_id,
          min_schema_version,
          PREFIX_PRIMARY_KEY_FOR_TABLEGROUP_HISTORY,
          purged_rows))) {
    last_ret = ret;
    LOG_WARN("purge_tablegroup_history failed", K(ret));
  }
  if (OB_FAIL(purge_history(OB_ALL_OUTLINE_HISTORY_TNAME,
          N_PARTITION_FOR_OUTLINE_HISTORY,
          tenant_id,
          min_schema_version,
          PREFIX_PRIMARY_KEY_FOR_OUTLINE_HISTORY,
          purged_rows))) {
    last_ret = ret;
    LOG_WARN("purge_tablegroup_history failed", K(ret));
  }
  if (OB_FAIL(purge_history(OB_ALL_DATABASE_HISTORY_TNAME,
          N_PARTITION_FOR_DATABASE_HISTORY,
          tenant_id,
          min_schema_version,
          PREFIX_PRIMARY_KEY_FOR_DATABASE_HISTORY,
          purged_rows))) {
    last_ret = ret;
    LOG_WARN("purge_database_history failed", K(ret));
  }
  if (OB_FAIL(purge_history(OB_ALL_USER_HISTORY_TNAME,
          N_PARTITION_FOR_USER_HISTORY,
          tenant_id,
          min_schema_version,
          PREFIX_PRIMARY_KEY_FOR_USER_HISTORY,
          purged_rows))) {
    last_ret = ret;
    LOG_WARN("purge_user_history failed", K(ret));
  }
  if (OB_FAIL(purge_history(OB_ALL_TENANT_HISTORY_TNAME,
          N_PARTITION_FOR_TENANT_HISTORY,
          tenant_id,
          min_schema_version,
          PREFIX_PRIMARY_KEY_FOR_TENANT_HISTORY,
          purged_rows))) {
    last_ret = ret;
    LOG_WARN("purge_tenant_history failed", K(ret));
  }
  if (OB_FAIL(purge_history(OB_ALL_DATABASE_PRIVILEGE_HISTORY_TNAME,
          N_PARTITION_FOR_DATABASE_PRIVILEGE_HISTORY,
          tenant_id,
          min_schema_version,
          PREFIX_PRIMARY_KEY_FOR_DATABASE_PRIVILEGE_HISTORY,
          purged_rows))) {
    last_ret = ret;
    LOG_WARN("purge_database_privilege_history failed", K(ret));
  }
  if (OB_FAIL(purge_history(OB_ALL_TABLE_PRIVILEGE_HISTORY_TNAME,
          N_PARTITION_FOR_TABLE_PRIVILEGE_HISTORY,
          tenant_id,
          min_schema_version,
          PREFIX_PRIMARY_KEY_FOR_TABLE_PRIVILEGE_HISTORY,
          purged_rows))) {
    last_ret = ret;
    LOG_WARN("purge_table_privilege_history failed", K(ret));
  }
  if (OB_FAIL(purge_history(OB_ALL_DDL_OPERATION_TNAME,
          N_PARTITION_FOR_DDL_OPERATION,
          tenant_id,
          min_schema_version,
          PREFIX_PRIMARY_KEY_FOR_DDL_OPERATION,
          purged_rows))) {
    last_ret = ret;
    LOG_WARN("pureg_ddl_operation failed", K(ret));
  }

  return last_ret;
}

int ObInnerTableMonitor::delete_history_for_partitions(const char* tname, const int np, const uint64_t tid,
    const int64_t schema_version, const char* prefix_pk, int64_t& purged_rows, ObMySQLTransaction& trans)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  for (int i = 0; OB_SUCC(ret) && i < np; i++) {
    if (OB_FAIL(sql.assign_fmt(PURGE_SQL1,
            tname,
            i,
            prefix_pk,
            prefix_pk,
            tname,
            i,
            prefix_pk,
            tname,
            i,
            tid,
            schema_version,
            prefix_pk,
            prefix_pk))) {
      LOG_WARN("assign_fmt failed", K(sql));
    } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
      LOG_WARN("execute sql fail", K(sql), K(ret));
    }
    purged_rows += affected_rows;

    if (OB_SUCCESS == ret &&
        !ObCharset::case_insensitive_equal(
            ObString(strlen(OB_ALL_DDL_OPERATION_TNAME), OB_ALL_DDL_OPERATION_TNAME), ObString(strlen(tname), tname))) {
      if (OB_FAIL(sql.assign_fmt(PURGE_SQL2,
              tname,
              i,
              prefix_pk,
              prefix_pk,
              tname,
              i,
              prefix_pk,
              tname,
              i,
              tid,
              schema_version,
              prefix_pk,
              prefix_pk))) {
        LOG_WARN("assign_fmt failed", K(sql));
      } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql fail", K(sql), K(ret));
      }
      purged_rows += affected_rows;
    }
  }

  return ret;
}

int ObInnerTableMonitor::purge_history(const char* tname, const int np, const uint64_t tid,
    const int64_t schema_version, const char* prefix_pk, int64_t& purged_rows)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool commit = false;
  ObMySQLTransaction trans;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    SERVER_LOG(WARN, "start transaction failed", K(ret));
  } else if (OB_FAIL(delete_history_for_partitions(tname, np, tid, schema_version, prefix_pk, purged_rows, trans))) {
    LOG_WARN("delete_history_for_partitions fail", K(ret));
  }
  if (trans.is_started()) {
    commit = (OB_SUCC(ret));
    if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
      SERVER_LOG(WARN, "end transaction failed", K(ret), K(tmp_ret), K(commit));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }

  return ret;
}
// purge table schema history and related schemas(such as column schema) history in one trans.
int ObInnerTableMonitor::purge_table_column_history(
    const uint64_t tid, const int64_t schema_version, int64_t& purged_rows)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool commit = false;
  ObMySQLTransaction trans;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else if (OB_FAIL(delete_history_for_partitions(OB_ALL_COLUMN_HISTORY_TNAME,
                 N_PARTITION_FOR_COLUMN_HISTORY,
                 tid,
                 schema_version,
                 PREFIX_PRIMARY_KEY_FOR_COLUMN_HISTORY,
                 purged_rows,
                 trans))) {
    LOG_WARN("purge_column_history failed", K(ret));
  } else if (OB_FAIL(delete_history_for_partitions(OB_ALL_TABLE_HISTORY_TNAME,
                 N_PARTITION_FOR_TABLE_HISTORY,
                 tid,
                 schema_version,
                 PREFIX_PRIMARY_KEY_FOR_TABLE_HISTORY,
                 purged_rows,
                 trans))) {
    LOG_WARN("purge_table_history failed", K(ret));
  }
  if (trans.is_started()) {
    commit = (OB_SUCC(ret));
    if (OB_SUCCESS != (tmp_ret = trans.end(commit))) {
      LOG_WARN("end transaction failed", K(ret), K(tmp_ret), K(commit));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObInnerTableMonitor::check_cancel() const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (!root_service_->in_service()) {
    ret = OB_RS_SHUTDOWN;
    LOG_WARN("root service is shutdown", K(ret));
  }
  return ret;
}

int ObInnerTableMonitor::purge_recyclebin_objects(ObIArray<uint64_t>& tenant_ids)
{
  int ret = OB_SUCCESS;
  const int64_t current_time = ObTimeUtility::current_time();
  obrpc::Int64 expire_time = current_time - GCONF.schema_history_expire_time;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else {
    const int64_t SLEEP_INTERVAL = 100 * 1000;  // 100ms
    const int64_t SLEEP_TIMES = 100;
    const int64_t PURGE_EACH_TIME = 10;
    obrpc::Int64 affected_rows = 0;
    bool is_tenant_finish = false;
    obrpc::ObPurgeRecycleBinArg arg;
    // ignore ret
    for (int i = 0; i < tenant_ids.count() && OB_SUCCESS == check_cancel(); ++i) {
      is_tenant_finish = false;
      affected_rows = 0;
      arg.tenant_id_ = tenant_ids.at(i);
      arg.purge_num_ = PURGE_EACH_TIME;
      arg.expire_time_ = expire_time;
      LOG_INFO("start purge recycle objects of tenant", K(arg));
      int retry_cnt = 0;
      while (!is_tenant_finish && OB_SUCCESS == check_cancel()) {
        // In case of holding DDL thread in long time, Each tenant only purge 10 recycle objects in one round.
        int64_t start_time = ObTimeUtility::current_time();
        if (OB_FAIL(rs_proxy_->purge_expire_recycle_objects(arg, affected_rows))) {
          LOG_WARN("purge reyclebin objects failed",
              K(ret),
              K(current_time),
              K(expire_time),
              K(affected_rows),
              K(arg),
              K(retry_cnt));
          if (retry_cnt < 3) {
            is_tenant_finish = false;
            ++retry_cnt;
          } else {
            LOG_WARN("retry purge recyclebin object of tenant failed, ignore it", K(retry_cnt), K(ret), K(arg));
            is_tenant_finish = true;
          }
        } else {
          retry_cnt = 0;
          is_tenant_finish = PURGE_EACH_TIME == affected_rows ? false : true;
        }
        int64_t cost_time = ObTimeUtility::current_time() - start_time;
        LOG_INFO("purge recycle objects",
            K(ret),
            K(cost_time),
            K(expire_time),
            K(current_time),
            K(affected_rows),
            K(is_tenant_finish));
        // sleep 10s so that will not block the rs DDL thread
        int i = 0;
        while (OB_SUCCESS == check_cancel() && i < SLEEP_TIMES) {
          usleep(SLEEP_INTERVAL);
          ++i;
        }
      }
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
