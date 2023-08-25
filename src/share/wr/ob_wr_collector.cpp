/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX WR

#include "deps/oblib/src/lib/mysqlclient/ob_mysql_proxy.h"
#include "observer/ob_server_struct.h"
#include "share/wr/ob_wr_collector.h"
#include "share/wr/ob_wr_task.h"
#include "share/ob_define.h"
#include "share/ob_dml_sql_splicer.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common::sqlclient;

namespace oceanbase
{
namespace share
{

#define WR_INSERT_BATCH_SIZE 128

ObWrCollector::ObWrCollector(int64_t snap_id, int64_t snapshot_begin_time,
    int64_t snapshot_end_time, int64_t snapshot_timeout_ts)
    : snap_id_(snap_id),
      snapshot_begin_time_(snapshot_begin_time),
      snapshot_end_time_(snapshot_end_time),
      timeout_ts_(snapshot_timeout_ts)
{
  if (OB_UNLIKELY(snapshot_begin_time_ == snapshot_end_time_)) {
    snapshot_begin_time_ = 0;
  }
}

ERRSIM_POINT_DEF(ERRSIM_WR_SNAPSHOT_COLLECTOR_FAILURE);

int ObWrCollector::collect()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(collect_sysstat())) {
    LOG_WARN("failed to collect sysstat", KR(ret));
  } else if (OB_FAIL(collect_ash())) {
    LOG_WARN("failed to collect ash", KR(ret));
  } else if (OB_FAIL(collect_statname())) {
    LOG_WARN("failed to collect statname", KR(ret));
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(ERRSIM_WR_SNAPSHOT_COLLECTOR_FAILURE)) {
    ret = ERRSIM_WR_SNAPSHOT_COLLECTOR_FAILURE;
    LOG_WARN("errsim for wr snapshot collector", KR(ret), KPC(this));
  }
  return ret;
}

int ObWrCollector::collect_sysstat()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  const uint64_t tenant_id = MTL_ID();
  int64_t cluster_id = ObServerConfig::get_instance().cluster_id;
  ObDMLSqlSplicer dml_splicer;
  ObSqlString sql;
  int64_t tmp_real_str_len = 0;
  int64_t query_timeout = timeout_ts_ - common::ObTimeUtility::current_time();
  SMART_VAR(ObISQLClient::ReadResult, res)
  {
    ObMySQLResult *result = nullptr;
    if (OB_UNLIKELY(query_timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("wr snapshot timeout", KR(ret), K_(timeout_ts));
    } else if (OB_FAIL(sql.assign_fmt(
                   "SELECT /*+ WORKLOAD_REPOSITORY_SNAPSHOT QUERY_TIMEOUT(%ld) */ svr_ip, svr_port, stat_id, value from "
                   "__all_virtual_sysstat where tenant_id=%ld",
                   query_timeout, tenant_id))) {
      LOG_WARN("failed to assign sysstat query string", KR(ret));
    } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("failed to fetch sysstat", KR(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", KR(ret));
          }
        } else {
          ObWrSysstat sysstat;
          EXTRACT_STRBUF_FIELD_MYSQL(
              *result, "svr_ip", sysstat.svr_ip_, OB_IP_STR_BUFF, tmp_real_str_len);
          EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", sysstat.svr_port_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "stat_id", sysstat.stat_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "value", sysstat.value_, int64_t);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(dml_splicer.add_pk_column(K(tenant_id)))) {
              LOG_WARN("failed to add tenant_id", KR(ret), K(tenant_id));
            } else if (OB_FAIL(dml_splicer.add_pk_column(K(cluster_id)))) {
              LOG_WARN("failed to add column cluster_id", KR(ret), K(cluster_id));
            } else if (OB_FAIL(dml_splicer.add_pk_column("SNAP_ID", snap_id_))) {
              LOG_WARN("failed to add column SNAP_ID", KR(ret), K(snap_id_));
            } else if (OB_FAIL(dml_splicer.add_pk_column("svr_ip", sysstat.svr_ip_))) {
              LOG_WARN("failed to add column svr_ip", KR(ret), K(sysstat));
            } else if (OB_FAIL(dml_splicer.add_pk_column("svr_port", sysstat.svr_port_))) {
              LOG_WARN("failed to add column svr_port", KR(ret), K(sysstat));
            } else if (OB_FAIL(dml_splicer.add_pk_column("stat_id", sysstat.stat_id_))) {
              LOG_WARN("failed to add column stat_id", KR(ret), K(sysstat));
            } else if (OB_FAIL(dml_splicer.add_column("value", sysstat.value_))) {
              LOG_WARN("failed to add column value", KR(ret), K(sysstat));
            } else if (OB_FAIL(dml_splicer.finish_row())) {
              LOG_WARN("failed to finish row", KR(ret));
            }
          }
        }
        if (OB_SUCC(ret) && dml_splicer.get_row_count() >= WR_INSERT_BATCH_SIZE) {
          if (OB_FAIL(write_to_wr(dml_splicer, OB_WR_SYSSTAT_TNAME, tenant_id))) {
            LOG_WARN("failed to batch write to wr", KR(ret));
          }
        }
      }
      if (OB_SUCC(ret) && dml_splicer.get_row_count() > 0 &&
          OB_FAIL(write_to_wr(dml_splicer, OB_WR_SYSSTAT_TNAME, tenant_id))) {
        LOG_WARN("failed to batch write remaining part to wr", KR(ret));
      }
    }
  }
  return ret;
}

int ObWrCollector::collect_ash()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  const uint64_t tenant_id = MTL_ID();
  int64_t cluster_id = ObServerConfig::get_instance().cluster_id;
  ObDMLSqlSplicer dml_splicer;
  ObSqlString sql;
  int64_t tmp_real_str_len = 0;
  int64_t query_timeout = timeout_ts_ - common::ObTimeUtility::current_time();
  int64_t collected_ash_row_count = 0;
  SMART_VAR(ObISQLClient::ReadResult, res)
  {
    ObMySQLResult *result = nullptr;
    if (OB_UNLIKELY(query_timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("wr snapshot timeout", KR(ret), K_(timeout_ts));
    } else if (OB_FAIL(sql.assign_fmt(
                   "select /*+ WORKLOAD_REPOSITORY_SNAPSHOT QUERY_TIMEOUT(%ld) */ svr_ip, svr_port, sample_id, session_id, "
                   "time_to_usec(sample_time) as sample_time, "
                   "user_id, session_type, sql_id, trace_id, event_no, time_waited, "
                   "p1, p2, p3, sql_plan_line_id, time_model, module, action, "
                   "client_id, backtrace, plan_id from "
                   "__all_virtual_ash where tenant_id=%ld and is_wr_sample=true and "
                   "sample_time between usec_to_time(%ld) and usec_to_time(%ld)",
                   query_timeout, tenant_id, snapshot_begin_time_, snapshot_end_time_))) {
      LOG_WARN("failed to assign ash query string", KR(ret));
    } else if (OB_FAIL(sql_proxy->read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("failed to fetch ash", KR(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", KR(ret));
          }
        } else {
          ObWrAsh ash;
          EXTRACT_STRBUF_FIELD_MYSQL(
              *result, "svr_ip", ash.svr_ip_, sizeof(ash.svr_ip_), tmp_real_str_len);
          EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", ash.svr_port_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "sample_id", ash.sample_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "session_id", ash.session_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "sample_time", ash.sample_time_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "user_id", ash.user_id_, int64_t);
          EXTRACT_BOOL_FIELD_MYSQL(*result, "session_type", ash.session_type_);
          EXTRACT_STRBUF_FIELD_MYSQL(
              *result, "sql_id", ash.sql_id_, sizeof(ash.sql_id_), tmp_real_str_len);
          EXTRACT_STRBUF_FIELD_MYSQL(
              *result, "trace_id", ash.trace_id_, sizeof(ash.trace_id_), tmp_real_str_len);
          EXTRACT_INT_FIELD_MYSQL(*result, "event_no", ash.event_no_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "time_waited", ash.time_waited_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "p1", ash.p1_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "p2", ash.p2_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "p3", ash.p3_, int64_t);
          EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "sql_plan_line_id", ash.sql_plan_line_id_, int64_t, true, false, -1);
          EXTRACT_UINT_FIELD_MYSQL(*result, "time_model", ash.time_model_, uint64_t);
          // ash not implement this for now.
          // EXTRACT_STRBUF_FIELD_MYSQL(*result, "module", ash.module_, sizeof(ash.module_),
          // tmp_real_str_len);
          // EXTRACT_STRBUF_FIELD_MYSQL(*result, "action", ash.action_,
          // sizeof(ash.action_), tmp_real_str_len);
          // EXTRACT_STRBUF_FIELD_MYSQL(*result,
          // "client_id", ash.client_id_, sizeof(ash.client_id_), tmp_real_str_len);
          EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(
              *result, "backtrace", ash.backtrace_, sizeof(ash.backtrace_), tmp_real_str_len);
          EXTRACT_INT_FIELD_MYSQL(*result, "plan_id", ash.plan_id_, int64_t);
          if (OB_SUCC(ret)) {
            if (OB_FAIL(dml_splicer.add_pk_column(K(tenant_id)))) {
              LOG_WARN("failed to add tenant_id", KR(ret), K(tenant_id));
            } else if (OB_FAIL(dml_splicer.add_pk_column(K(cluster_id)))) {
              LOG_WARN("failed to add column cluster_id", KR(ret), K(cluster_id));
            } else if (OB_FAIL(dml_splicer.add_pk_column("SNAP_ID", snap_id_))) {
              LOG_WARN("failed to add column SNAP_ID", KR(ret), K(snap_id_));
            } else if (OB_FAIL(dml_splicer.add_pk_column("svr_ip", ash.svr_ip_))) {
              LOG_WARN("failed to add column svr_ip", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_pk_column("svr_port", ash.svr_port_))) {
              LOG_WARN("failed to add column svr_port", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_pk_column("sample_id", ash.sample_id_))) {
              LOG_WARN("failed to add column sample_id", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_pk_column("session_id", ash.session_id_))) {
              LOG_WARN("failed to add column session_id", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_time_column("sample_time", ash.sample_time_))) {
              LOG_WARN("failed to add column sample_time", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column("user_id", ash.user_id_))) {
              LOG_WARN("failed to add column user_id", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column("session_type", ash.session_type_))) {
              LOG_WARN("failed to add column session_type", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column("sql_id", ash.sql_id_))) {
              LOG_WARN("failed to add column sql_id", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column("trace_id", ash.trace_id_))) {
              LOG_WARN("failed to add column trace_id", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column("event_no", ash.event_no_))) {
              LOG_WARN("failed to add column event_no", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column("time_waited", ash.time_waited_))) {
              LOG_WARN("failed to add column time_waited", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column("p1", ash.p1_))) {
              LOG_WARN("failed to add column p1", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column("p2", ash.p2_))) {
              LOG_WARN("failed to add column p2", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column("p3", ash.p3_))) {
              LOG_WARN("failed to add column p3", KR(ret), K(ash));
            } else if (ash.sql_plan_line_id_ < 0 &&
                       OB_FAIL(dml_splicer.add_column(true, "sql_plan_line_id"))) {
              LOG_WARN("failed to add column sql_plan_line_id", KR(ret), K(ash));
            } else if (ash.sql_plan_line_id_ >= 0 &&
                       OB_FAIL(dml_splicer.add_column("sql_plan_line_id", ash.sql_plan_line_id_))) {
              LOG_WARN("failed to add column sql_plan_line_id", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column("time_model", ash.time_model_))) {
              LOG_WARN("failed to add column time_model", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column(true, "module"))) {
              LOG_WARN("failed to add column module", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column(true, "action"))) {
              LOG_WARN("failed to add column action", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column(true, "client_id"))) {
              LOG_WARN("failed to add column client_id", KR(ret), K(ash));
            } else if (ash.backtrace_[0] == '\0' &&
                       OB_FAIL(dml_splicer.add_column(true, "backtrace"))) {
              LOG_WARN("failed to add column backtrace", KR(ret), K(ash));
            } else if (ash.backtrace_[0] != '\0' &&
                       OB_FAIL(dml_splicer.add_column("backtrace", ash.backtrace_))) {
              LOG_WARN("failed to add column backtrace", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.add_column("plan_id", ash.plan_id_))) {
              LOG_WARN("failed to add column plan_id", KR(ret), K(ash));
            } else if (OB_FAIL(dml_splicer.finish_row())) {
              LOG_WARN("failed to finish row", KR(ret));
            }
          }
        }
        if (OB_SUCC(ret) && dml_splicer.get_row_count() >= WR_INSERT_BATCH_SIZE) {
          collected_ash_row_count += dml_splicer.get_row_count();
          if (OB_FAIL(write_to_wr(dml_splicer, OB_WR_ACTIVE_SESSION_HISTORY_TNAME, tenant_id))) {
            LOG_WARN("failed to batch write to wr", KR(ret));
          }
        }
      }
      if (OB_SUCC(ret) && dml_splicer.get_row_count() > 0) {
        collected_ash_row_count += dml_splicer.get_row_count();
        if (OB_FAIL(write_to_wr(dml_splicer, OB_WR_ACTIVE_SESSION_HISTORY_TNAME, tenant_id))) {
          LOG_WARN("failed to batch write remaining part to wr", KR(ret));
        }
      }
    }
  }
  if (OB_LIKELY(collected_ash_row_count != 0)) {
    EVENT_ADD(WR_COLLECTED_ASH_ROW_COUNT, collected_ash_row_count);
  }
  return ret;
}

int ObWrCollector::collect_statname()
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();

  ObSqlString sql;
  int64_t expected_rows = 0;
  const uint64_t tenant_id = MTL_ID();
  int64_t cluster_id = ObServerConfig::get_instance().cluster_id;

  if (OB_FAIL(sql.assign_fmt("INSERT /*+ WORKLOAD_REPOSITORY */ IGNORE INTO %s "
                             "(TENANT_ID, CLUSTER_ID, STAT_ID, STAT_NAME) VALUES",
          OB_WR_STATNAME_TNAME))) {
    LOG_WARN("sql assign failed", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < ObStatEventIds::STAT_EVENT_SET_END; ++i) {
    if (i == ObStatEventIds::STAT_EVENT_ADD_END) {
      // the end of ADD stat event, do nothing
      continue;
    } else {
      int64_t stat_id = OB_STAT_EVENTS[i].stat_id_;
      ObString stat_name = ObString::make_string(OB_STAT_EVENTS[i].name_);
      expected_rows++;
      if (OB_FAIL(sql.append_fmt("%s('%lu', '%lu', '%ld', '%.*s')", (i == 0) ? " " : ", ",
              tenant_id, cluster_id, stat_id, stat_name.length(), stat_name.ptr()))) {
        LOG_WARN("sql append failed", K(ret));
      }
    }
  }  // end for

  if (OB_SUCC(ret)) {
    if (expected_rows > 0) {
      int64_t affected_rows = 0;
      uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
      if (OB_FAIL(GCTX.sql_proxy_->write(exec_tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
      } else if (OB_UNLIKELY(affected_rows > 0)) {
        LOG_TRACE("the stat name has changed, there are new statname.", K(affected_rows));
      } else if (OB_UNLIKELY(affected_rows < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected affected_rows", KR(ret), K(expected_rows), K(affected_rows));
      }
    }
  }
  LOG_TRACE(
      "init __wr_statname ", K(ret), K(tenant_id), "cost", ObTimeUtility::current_time() - start);
  return ret;
}

int ObWrCollector::write_to_wr(
    ObDMLSqlSplicer &dml_splicer, const char *table_name, int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t cur_row = dml_splicer.get_row_count();
  if (OB_FAIL(dml_splicer.splice_batch_insert_sql(table_name, sql))) {
    LOG_WARN("failed to generate sql", KR(ret), K(tenant_id));
  } else if (OB_FAIL(
                 GCTX.sql_proxy_->write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write wr data", KR(ret), K(sql), K(gen_meta_tenant_id(tenant_id)));
  } else if (affected_rows != cur_row) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid affected rows", KR(ret), K(affected_rows), K(cur_row), K(sql));
  } else {
    LOG_TRACE("execute wr batch insertion sql success", K(sql), K(affected_rows));
    dml_splicer.reset();
  }
  return ret;
}

int ObWrDeleter::do_delete()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t cluster_id = ObServerConfig::get_instance().cluster_id;
  int64_t query_timeout = 0;

  for (int64_t i = 0; OB_SUCC(ret) && i < purge_arg_.get_to_delete_snap_ids().count(); ++i) {
    int64_t snap_id = purge_arg_.get_to_delete_snap_ids().at(i);
    query_timeout = purge_arg_.get_timeout_ts() - ObTimeUtility::current_time();
    if (OB_UNLIKELY(query_timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("delete data task timed out, stop task", K(ret), K(tenant_id), K(cluster_id),
          K(snap_id), K(purge_arg_.get_timeout_ts()));
      break;
    } else if (OB_FAIL(modify_snapshot_status(tenant_id, cluster_id, snap_id, query_timeout, ObWrSnapshotStatus::DELETED))) {
      LOG_WARN("failed to modify wr snapshot status to deleted", K(ret), K(tenant_id), K(cluster_id), K(snap_id));
    } else if (OB_FAIL(delete_expired_data_from_wr_table(
                   OB_WR_SYSSTAT_TNAME, tenant_id, cluster_id, snap_id, query_timeout))) {
      LOG_WARN(
          "failed to delete __wr_sysstat data ", K(ret), K(tenant_id), K(cluster_id), K(snap_id));
    } else if (OB_FAIL(delete_expired_data_from_wr_table(OB_WR_ACTIVE_SESSION_HISTORY_TNAME,
                   tenant_id, cluster_id, snap_id, query_timeout))) {
      LOG_WARN("failed to delete __wr_acitve_session_history data ", K(ret), K(tenant_id),
          K(cluster_id), K(snap_id));
    } else if (OB_FAIL(delete_expired_data_from_wr_table(
                   OB_WR_SNAPSHOT_TNAME, tenant_id, cluster_id, snap_id, query_timeout))) {
      LOG_WARN(
          "failed to delete __wr_snapshot data ", K(ret), K(tenant_id), K(cluster_id), K(snap_id));
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(modify_snapshot_status(tenant_id, cluster_id, snap_id, query_timeout, ObWrSnapshotStatus::FAILED))) {
        LOG_WARN("failed to delete snapshot", KR(tmp_ret), K(tenant_id), K(cluster_id), K(snap_id));
      }
    }
  }  // end for
  return ret;
}

int ObWrDeleter::delete_expired_data_from_wr_table(const char *const table_name,
    const uint64_t tenant_id, const int64_t cluster_id, const int64_t snap_id,
    const int64_t query_timeout)
{
  int ret = OB_SUCCESS;
  int64_t start = ObTimeUtility::current_time();

  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(query_timeout <= 0)) {
    ret = OB_TIMEOUT;
    LOG_WARN("timeout on wr data deletion", KR(ret), K(query_timeout));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ nullptr", K(ret));
  } else if (OB_FAIL(sql.assign_fmt(
                 "DELETE /*+ WORKLOAD_REPOSITORY_PURGE QUERY_TIMEOUT(%ld) */ FROM %s WHERE ", query_timeout, table_name))) {
    LOG_WARN("sql assign failed", K(ret));
  } else if (OB_FAIL(sql.append_fmt("tenant_id='%lu' and cluster_id='%lu' and snap_id='%ld'",
                 tenant_id, cluster_id, snap_id))) {
    LOG_WARN("sql append failed", K(ret));
  } else if (OB_FAIL(GCTX.sql_proxy_->write(meta_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
  } else if (OB_UNLIKELY(affected_rows == 0)) {
    LOG_TRACE("the snapshot does not have any data.", K(snap_id), K(tenant_id), K(affected_rows),
        K(table_name));
  } else if (OB_UNLIKELY(affected_rows < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected_rows", KR(ret), K(snap_id), K(tenant_id), K(affected_rows));
  }

  LOG_TRACE("delete wr table ", K(ret), K(table_name), K(tenant_id), K(snap_id), "time_consumed",
      ObTimeUtility::current_time() - start);
  return ret;
}

int ObWrDeleter::modify_snapshot_status(const uint64_t tenant_id, const int64_t cluster_id,
    const int64_t snap_id, const int64_t query_timeout, const ObWrSnapshotStatus status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(query_timeout <= 0)) {
    ret = OB_TIMEOUT;
    LOG_WARN("timeout on wr data deletion", KR(ret), K(query_timeout));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ nullptr", K(ret));
  } else if (OB_FAIL(
                 sql.assign_fmt("UPDATE /*+ WORKLOAD_REPOSITORY_PURGE QUERY_TIMEOUT(%ld) */ %s set "
                                "status=%ld where tenant_id=%ld and cluster_id=%ld and snap_id=%ld",
                     query_timeout, OB_WR_SNAPSHOT_TNAME, status, tenant_id,
                     cluster_id, snap_id))) {
    LOG_WARN("sql assign failed", K(ret));
  } else if (OB_FAIL(GCTX.sql_proxy_->write(meta_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
  } else if (OB_UNLIKELY(affected_rows == 0)) {
    LOG_TRACE("the snapshot is already marked as deleted, execute delete process anyway", K(snap_id), K(tenant_id), K(affected_rows));
  } else if (OB_UNLIKELY(affected_rows < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected_rows", KR(ret), K(snap_id), K(tenant_id), K(affected_rows));
  }
  return ret;
}

}  // end of namespace share
}  // end of namespace oceanbase
