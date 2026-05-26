/**
 * Copyright (c) 2024 OceanBase
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

#include "rootserver/mview/ob_mview_pending_task_table_operator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;

int ObMViewPendingTaskTableOperator::execute_trans_write(common::ObISQLClient *sql_proxy,
                                                         uint64_t tenant_id,
                                                         const common::ObSqlString &sql,
                                                         int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy));
  } else {
    const uint64_t exec_tenant_id = share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObMySQLTransaction trans;
    if (OB_FAIL(trans.start(sql_proxy, tenant_id))) {
      LOG_WARN("fail to start trans", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute write", KR(ret), K(sql));
    }
    if (trans.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
        LOG_WARN("fail to end trans", KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  return ret;
}

ObMViewPendingTaskTableOperator::ObMViewPendingTaskTableOperator()
  : sql_proxy_(NULL),
    is_inited_(false)
{
}

ObMViewPendingTaskTableOperator::~ObMViewPendingTaskTableOperator()
{
  destroy();
}

int ObMViewPendingTaskTableOperator::init(common::ObISQLClient *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("pending task table operator init twice", KR(ret));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy));
  } else {
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObMViewPendingTaskTableOperator::destroy()
{
  sql_proxy_ = NULL;
  is_inited_ = false;
}

int ObMViewPendingTaskTableOperator::insert_tasks(const ObIArray<ObMViewPendingTask *> &tasks)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  ObSqlString sql;
  ObSqlString tname;
  int64_t affected_rows = 0;
  uint64_t target_tenant = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table operator not init", KR(ret));
  } else if (tasks.empty()) {
    // nothing to insert
  } else {
    target_tenant = tasks.at(0)->tenant_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObMViewPendingTask *task = tasks.at(i);
      if (OB_ISNULL(task) || OB_UNLIKELY(!task->is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid pending task", KR(ret), K(i), KPC(task));
      } else if (OB_UNLIKELY(task->tenant_id_ != target_tenant)) {
        // One insert_tasks call must land in a single trans on one tenant; reload
        // groups and new scheduling already satisfy this.
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tasks span multiple tenants", KR(ret), K(i),
                 K(target_tenant), K(task->tenant_id_));
      } else if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
                 OB_FAIL(dml.add_pk_column("refresh_id", task->refresh_id_)) ||
                 OB_FAIL(dml.add_pk_column("mview_id", task->mview_id_)) ||
                 OB_FAIL(dml.add_column("status", task->status_)) ||
                 OB_FAIL(dml.add_column("flags", task->flags_)) ||
                 OB_FAIL(dml.add_column("skip_cnt", task->skip_cnt_)) ||
                 OB_FAIL(dml.add_uint64_column("target_data_sync_scn", task->target_data_sync_scn_)) ||
                 OB_FAIL(dml.add_column("retry_count", task->retry_count_)) ||
                 (task->next_retry_ts_ > 0
                      ? OB_FAIL(dml.add_time_column("next_retry_time", task->next_retry_ts_))
                      : OB_FAIL(dml.add_column(true, "next_retry_time"))) ||
                 OB_FAIL(dml.add_column("refresh_method", static_cast<int64_t>(task->refresh_method_))) ||
                 OB_FAIL(dml.add_column("refresh_parallel", task->refresh_parallel_)) ||
                 OB_FAIL(dml.add_column("seq", task->seq_)) ||
                 OB_FAIL(dml.finish_row())) {
        LOG_WARN("splice row failed", KR(ret), K(i), KPC(task));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tname.assign_fmt("`%s`.`%s`", OB_SYS_DATABASE_NAME,
                                   OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME))) {
        LOG_WARN("fail to format table name", KR(ret));
      } else if (OB_FAIL(dml.splice_batch_insert_sql(tname.ptr(), sql))) {
        LOG_WARN("fail to splice batch insert sql", KR(ret));
      } else if (OB_FAIL(execute_trans_write(sql_proxy_, target_tenant, sql, affected_rows))) {
        LOG_WARN("fail to insert tasks", KR(ret), K(target_tenant), K(tasks.count()));
      } else if (OB_UNLIKELY(affected_rows != tasks.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert_tasks affected rows mismatch", KR(ret),
                 K(affected_rows), K(tasks.count()));
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::update_task_status(uint64_t tenant_id,
                                                        int64_t refresh_id,
                                                        uint64_t mview_id,
                                                        int64_t old_status,
                                                        int64_t new_status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || refresh_id <= 0 || OB_INVALID_ID == mview_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "UPDATE `%s`.`%s` SET status = %ld WHERE tenant_id = 0 AND refresh_id = %ld "
                 "AND mview_id = %lu AND status = %ld",
                 OB_SYS_DATABASE_NAME,
                 OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                 new_status,
                 refresh_id,
                 mview_id,
                 old_status))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else if (OB_FAIL(execute_trans_write(sql_proxy_, tenant_id, sql, affected_rows))) {
    LOG_WARN("fail to update task status", KR(ret), K(tenant_id), K(refresh_id), K(mview_id),
             K(old_status), K(new_status));
  } else if (OB_UNLIKELY(0 == affected_rows)) {
    ret = OB_EAGAIN;
    LOG_WARN("task status CAS failed, current status mismatch", KR(ret),
             K(tenant_id), K(refresh_id), K(mview_id), K(old_status), K(new_status));
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::update_task_to_running(uint64_t tenant_id,
                                                            int64_t refresh_id,
                                                            uint64_t mview_id,
                                                            const ObAddr &svr_addr)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char ip_buf[OB_IP_STR_BUFF] = {0};
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || refresh_id <= 0
                          || OB_INVALID_ID == mview_id || !svr_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(refresh_id), K(mview_id), K(svr_addr));
  } else if (OB_UNLIKELY(!svr_addr.ip_to_string(ip_buf, sizeof(ip_buf)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to format svr ip", KR(ret), K(svr_addr));
  } else if (OB_FAIL(sql.assign_fmt(
                 "UPDATE `%s`.`%s` SET status = %ld, svr_ip = '%s', svr_port = %d"
                 " WHERE tenant_id = 0 AND refresh_id = %ld"
                 " AND mview_id = %lu AND status = %ld",
                 OB_SYS_DATABASE_NAME,
                 OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                 static_cast<int64_t>(MV_TASK_RUNNING),
                 ip_buf,
                 svr_addr.get_port(),
                 refresh_id,
                 mview_id,
                 static_cast<int64_t>(MV_TASK_PENDING)))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else if (OB_FAIL(execute_trans_write(sql_proxy_, tenant_id, sql, affected_rows))) {
    LOG_WARN("fail to update task to running", KR(ret),
             K(tenant_id), K(refresh_id), K(mview_id), K(svr_addr));
  } else if (OB_UNLIKELY(0 == affected_rows)) {
    ret = OB_EAGAIN;
    LOG_WARN("update_task_to_running CAS failed, status mismatch",
             KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}


int ObMViewPendingTaskTableOperator::update_task_session_id(common::ObISQLClient *sql_proxy,
                                                             uint64_t tenant_id,
                                                             int64_t refresh_id,
                                                             uint64_t mview_id,
                                                             uint32_t session_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql proxy is null", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || refresh_id <= 0
                          || OB_INVALID_ID == mview_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "UPDATE `%s`.`%s` SET session_id = %u"
                 " WHERE tenant_id = 0 AND refresh_id = %ld AND mview_id = %lu",
                 OB_SYS_DATABASE_NAME,
                 OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                 session_id,
                 refresh_id,
                 mview_id))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else if (OB_FAIL(execute_trans_write(sql_proxy, tenant_id, sql, affected_rows))) {
    LOG_WARN("fail to update task session id",
             KR(ret), K(tenant_id), K(refresh_id), K(mview_id), K(session_id));
  } else if (OB_UNLIKELY(0 == affected_rows)) {
    LOG_WARN("update_task_session_id touched 0 rows, ignored",
             K(tenant_id), K(refresh_id), K(mview_id), K(session_id));
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::build_batch_session_ids_sql(
    const ObIArray<ObMViewPendingTaskSessionIdEntry> &session_id_entries,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.assign_fmt(
                 "SELECT refresh_id, mview_id, session_id "
                 "FROM `%s`.`%s` "
                 "WHERE tenant_id = 0 AND status = %ld AND session_id != 0 "
                 "AND (refresh_id, mview_id) IN (",
                 OB_SYS_DATABASE_NAME,
                 OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                 static_cast<int64_t>(MV_TASK_RUNNING)))) {
    LOG_WARN("assign sql prefix failed", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < session_id_entries.count(); ++i) {
    if (OB_FAIL(sql.append_fmt("%s(%ld, %lu)",
                               0 == i ? "" : ", ",
                               session_id_entries.at(i).key_.refresh_id_,
                               session_id_entries.at(i).key_.mview_id_))) {
      LOG_WARN("append (refresh_id, mview_id) tuple failed",
               KR(ret), K(i), K(session_id_entries.at(i)));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sql.append(")"))) {
    LOG_WARN("append sql closing paren failed", KR(ret));
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::fill_session_ids_from_result(
    sqlclient::ObMySQLResult &result,
    ObIArray<ObMViewPendingTaskSessionIdEntry> &session_id_entries)
{
  int ret = OB_SUCCESS;
  bool need_next = true;
  while (OB_SUCC(ret) && need_next) {
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        need_next = false;
      } else {
        LOG_WARN("result next failed", KR(ret));
      }
    } else {
      int64_t refresh_id = 0;
      int64_t mview_id = 0;
      int64_t session_id = 0;
      EXTRACT_INT_FIELD_MYSQL(result, "refresh_id", refresh_id, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "mview_id", mview_id, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "session_id", session_id, int64_t);
      if (OB_SUCC(ret) && 0 != session_id) {
        bool found = false;
        for (int64_t i = 0; !found && i < session_id_entries.count(); ++i) {
          if (session_id_entries.at(i).key_.refresh_id_ == refresh_id
              && session_id_entries.at(i).key_.mview_id_ == mview_id) {
            session_id_entries.at(i).session_id_ = static_cast<uint32_t>(session_id);
            found = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::batch_get_running_session_ids(
    uint64_t tenant_id,
    ObIArray<ObMViewPendingTaskSessionIdEntry> &session_id_entries) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (session_id_entries.empty()) {
    // caller already filtered — nothing to do
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(build_batch_session_ids_sql(session_id_entries, sql))) {
      LOG_WARN("build batch session ids sql failed", KR(ret), K(tenant_id));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("read pending task table failed", KR(ret), K(tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret));
        } else if (OB_FAIL(fill_session_ids_from_result(*result, session_id_entries))) {
          LOG_WARN("fill session ids from result failed", KR(ret), K(tenant_id));
        }
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::get_running_session_infos(
    uint64_t tenant_id,
    int64_t refresh_id,
    ObIArray<uint32_t> &out_session_ids,
    ObIArray<ObAddr> &out_addrs,
    bool &need_retry) const
{
  int ret = OB_SUCCESS;
  out_session_ids.reset();
  out_addrs.reset();
  need_retry = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || refresh_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(refresh_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
                   "SELECT session_id, svr_ip, svr_port "
                   "FROM `%s`.`%s` "
                   "WHERE tenant_id = 0 AND refresh_id = %ld AND status = %ld",
                   OB_SYS_DATABASE_NAME,
                   OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                   refresh_id,
                   static_cast<int64_t>(MV_TASK_RUNNING)))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("read pending task table failed", KR(ret), K(tenant_id), K(refresh_id));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(ret));
        } else {
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("result next failed", KR(ret));
              }
            } else {
              int64_t session_id = 0;
              ObString svr_ip;
              int64_t svr_port = 0;
              EXTRACT_INT_FIELD_MYSQL(*result, "session_id", session_id, int64_t);
              EXTRACT_VARCHAR_FIELD_MYSQL(*result, "svr_ip", svr_ip);
              EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int64_t);
              if (OB_SUCC(ret)) {
                ObAddr addr;
                if (0 == session_id) {
                  need_retry = true;
                } else if (OB_UNLIKELY(svr_ip.empty())) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("empty svr ip in pending task, will retry",
                           K(refresh_id), K(session_id));
                } else if (OB_UNLIKELY(!addr.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port)))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("invalid svr addr in pending task, will retry",
                           K(svr_ip), K(svr_port), K(refresh_id), K(session_id));
                } else if (OB_FAIL(out_session_ids.push_back(static_cast<uint32_t>(session_id)))) {
                  LOG_WARN("push back session_id failed", KR(ret));
                } else if (OB_FAIL(out_addrs.push_back(addr))) {
                  LOG_WARN("push back addr failed", KR(ret));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::update_task_to_success(uint64_t tenant_id,
                                                            int64_t refresh_id,
                                                            uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_task_status(tenant_id, refresh_id, mview_id,
                                 MV_TASK_RUNNING, MV_TASK_SUCCESS))) {
    LOG_WARN("fail to update task to success", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::update_task_to_retry_wait(uint64_t tenant_id,
                                                               int64_t refresh_id,
                                                               uint64_t mview_id,
                                                               int64_t next_retry_ts)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || refresh_id <= 0
                          || OB_INVALID_ID == mview_id || next_retry_ts < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
             K(tenant_id), K(refresh_id), K(mview_id), K(next_retry_ts));
  } else if (OB_FAIL(sql.assign_fmt(
                 "UPDATE `%s`.`%s` SET status = %ld, retry_count = retry_count + 1,"
                 " next_retry_time = usec_to_time(%ld)"
                 " WHERE tenant_id = 0 AND refresh_id = %ld"
                 " AND mview_id = %lu AND status = %ld",
                 OB_SYS_DATABASE_NAME,
                 OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                 static_cast<int64_t>(MV_TASK_RETRY_WAIT),
                 next_retry_ts,
                 refresh_id,
                 mview_id,
                 static_cast<int64_t>(MV_TASK_RUNNING)))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else if (OB_FAIL(execute_trans_write(sql_proxy_, tenant_id, sql, affected_rows))) {
    LOG_WARN("fail to update task to retry wait", KR(ret),
             K(tenant_id), K(refresh_id), K(mview_id), K(next_retry_ts));
  } else if (OB_UNLIKELY(0 == affected_rows)) {
    ret = OB_EAGAIN;
    LOG_WARN("update_task_to_retry_wait CAS failed, status mismatch",
             KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::update_task_to_pending(uint64_t tenant_id,
                                                            int64_t refresh_id,
                                                            uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_task_status(tenant_id, refresh_id, mview_id,
                                 MV_TASK_RETRY_WAIT, MV_TASK_PENDING))) {
    LOG_WARN("fail to update task to pending", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::get_task_sync_info(uint64_t tenant_id,
                                                         int64_t refresh_id,
                                                         uint64_t mview_id,
                                                         ObMViewTaskStatus &status,
                                                         int64_t &next_retry_ts)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  status = MV_TASK_PENDING;
  next_retry_ts = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || refresh_id <= 0 || OB_INVALID_ID == mview_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT status, time_to_usec(next_retry_time) AS next_retry_ts "
                 "FROM `%s`.`%s` "
                 "WHERE tenant_id = 0 AND refresh_id = %ld AND mview_id = %lu",
                 OB_SYS_DATABASE_NAME,
                 OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                 refresh_id,
                 mview_id))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(refresh_id), K(mview_id));
  } else {
    const uint64_t exec_tenant_id = share::schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
    SMART_VAR(ObISQLClient::ReadResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy_->read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("fail to get next row", KR(ret), K(sql));
        }
      } else {
        int64_t status_int = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "status", status_int, int64_t);
        EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "next_retry_ts", next_retry_ts, int64_t);
        if (OB_SUCC(ret)) {
          status = static_cast<ObMViewTaskStatus>(status_int);
        }
      }
    }
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::update_task_to_failed(uint64_t tenant_id,
                                                           int64_t refresh_id,
                                                           uint64_t failed_mview_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || refresh_id <= 0 ||
                          OB_INVALID_ID == failed_mview_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(refresh_id), K(failed_mview_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "UPDATE `%s`.`%s` SET status = %ld WHERE tenant_id = 0 AND refresh_id = %ld "
                 "AND mview_id = %lu AND status = %ld",
                 OB_SYS_DATABASE_NAME,
                 OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                 static_cast<int64_t>(MV_TASK_FAILED),
                 refresh_id,
                 failed_mview_id,
                 static_cast<int64_t>(MV_TASK_RUNNING)))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else if (OB_FAIL(execute_trans_write(sql_proxy_, tenant_id, sql, affected_rows))) {
    LOG_WARN("fail to update task to failed", KR(ret), K(tenant_id), K(refresh_id), K(failed_mview_id));
  } else if (OB_UNLIKELY(0 == affected_rows)) {
    ret = OB_EAGAIN;
    LOG_WARN("task status CAS failed for update_task_to_failed", KR(ret),
             K(tenant_id), K(refresh_id), K(failed_mview_id));
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::batch_cancel_pending_tasks(uint64_t tenant_id, int64_t refresh_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || refresh_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(refresh_id));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE `%s`.`%s` SET status = %ld WHERE tenant_id = 0 AND refresh_id = %ld "
                                    "AND status IN (%ld, %ld)",
                                    OB_SYS_DATABASE_NAME,
                                    OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                                    static_cast<int64_t>(MV_TASK_CANCELLED),
                                    refresh_id,
                                    static_cast<int64_t>(MV_TASK_PENDING),
                                    static_cast<int64_t>(MV_TASK_RETRY_WAIT)))) {
    LOG_WARN("fail to assign batch cancel sql", KR(ret));
  } else if (OB_FAIL(execute_trans_write(sql_proxy_, tenant_id, sql, affected_rows))) {
    LOG_WARN("fail to batch cancel pending tasks", KR(ret), K(tenant_id), K(refresh_id));
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::delete_tasks_by_refresh_id(uint64_t tenant_id,
                                                                int64_t refresh_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || refresh_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(refresh_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "DELETE FROM `%s`.`%s` WHERE tenant_id = 0 AND refresh_id = %ld",
                 OB_SYS_DATABASE_NAME,
                 OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                 refresh_id))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(refresh_id));
  } else if (OB_FAIL(execute_trans_write(sql_proxy_, tenant_id, sql, affected_rows))) {
    LOG_WARN("fail to delete tasks by refresh id", KR(ret), K(tenant_id), K(refresh_id));
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::delete_terminal_tasks(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table operator not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
                 "DELETE FROM `%s`.`%s` WHERE tenant_id = 0 AND refresh_id IN ("
                 "SELECT refresh_id FROM ("
                 "SELECT refresh_id FROM `%s`.`%s` WHERE tenant_id = 0 "
                 "GROUP BY refresh_id HAVING MAX(status IN (%ld, %ld, %ld)) = 0"
                 ") t)",
                 OB_SYS_DATABASE_NAME,
                 OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                 OB_SYS_DATABASE_NAME,
                 OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                 static_cast<int64_t>(MV_TASK_PENDING),
                 static_cast<int64_t>(MV_TASK_RUNNING),
                 static_cast<int64_t>(MV_TASK_RETRY_WAIT)))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
  } else if (OB_FAIL(execute_trans_write(sql_proxy_, tenant_id, sql, affected_rows))) {
    LOG_WARN("fail to delete terminal tasks", KR(ret), K(tenant_id));
  } else {
    LOG_INFO("delete terminal tasks done", KR(ret), K(tenant_id), K(affected_rows));
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::extract_task_from_result(common::sqlclient::ObMySQLResult &result,
                                                              uint64_t actual_tenant_id,
                                                              ObMViewPendingTask &task)
{
  int ret = OB_SUCCESS;
  int64_t refresh_method = 0;
  ObString svr_ip;
  int64_t svr_port = 0;
  EXTRACT_INT_FIELD_MYSQL(result, "refresh_id", task.refresh_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "mview_id", task.mview_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "seq", task.seq_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "status", task.status_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "flags", task.flags_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "skip_cnt", task.skip_cnt_, int64_t);
  EXTRACT_UINT_FIELD_MYSQL(result, "target_data_sync_scn", task.target_data_sync_scn_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "retry_count", task.retry_count_, int64_t);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "next_retry_ts", task.next_retry_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "refresh_method", refresh_method, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "refresh_parallel", task.refresh_parallel_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "gmt_create_ts", task.gmt_create_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "gmt_modified_ts", task.gmt_modified_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(result, "svr_ip", svr_ip);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(result, "svr_port", svr_port, int64_t);


  if (OB_SUCC(ret)) {
    task.tenant_id_ = actual_tenant_id;
    task.refresh_method_ = static_cast<share::schema::ObMVRefreshMethod>(refresh_method);
    task.dep_mview_id_cnt_ = 0;
    task.dep_mview_ids_ = NULL;
    task.svr_addr_.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port));
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::build_load_tasks_sql(
    common::ObSqlString &sql,
    uint64_t last_scn,
    int64_t last_refresh_id,
    int64_t refresh_limit)
{
  int ret = OB_SUCCESS;
  ObSqlString inner_sql;
  const int64_t QUERY_TIMEOUT_US = 10 * 1000 * 1000L;
  if (OB_FAIL(inner_sql.assign_fmt(
                 "SELECT DISTINCT target_data_sync_scn, refresh_id "
                 "FROM `%s`.`%s` WHERE tenant_id = 0 ",
                 OB_SYS_DATABASE_NAME,
                 OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME))) {
    LOG_WARN("fail to assign inner sql", KR(ret));
  } else if (last_scn > 0
             && OB_FAIL(inner_sql.append_fmt(
                    "AND (target_data_sync_scn > %lu "
                    "OR (target_data_sync_scn = %lu AND refresh_id > %ld)) ",
                    last_scn, last_scn, last_refresh_id))) {
    LOG_WARN("fail to append cursor condition", KR(ret), K(last_scn), K(last_refresh_id));
  } else if (OB_FAIL(inner_sql.append("ORDER BY target_data_sync_scn, refresh_id "))) {
    LOG_WARN("fail to append order by", KR(ret));
  } else if (refresh_limit > 0
             && OB_FAIL(inner_sql.append_fmt("LIMIT %ld", refresh_limit))) {
    LOG_WARN("fail to append limit", KR(ret), K(refresh_limit));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT /*+ query_timeout(%ld) */ "
                 "t.refresh_id, t.mview_id, t.seq, t.target_data_sync_scn, "
                 "t.status, t.flags, t.skip_cnt, t.retry_count, "
                 "t.next_retry_ts, t.refresh_method, t.refresh_parallel, "
                 "t.gmt_create_ts, t.gmt_modified_ts, "
                 "t.svr_ip, t.svr_port, "
                 "d.p_obj AS dep_mview_id "
                 "FROM ("
                 "SELECT r.refresh_id, r.mview_id, r.seq, r.target_data_sync_scn, "
                 "r.status, r.flags, r.skip_cnt, r.retry_count, "
                 "time_to_usec(r.next_retry_time) AS next_retry_ts, "
                 "r.refresh_method, r.refresh_parallel, "
                 "time_to_usec(r.gmt_create) AS gmt_create_ts, "
                 "time_to_usec(r.gmt_modified) AS gmt_modified_ts, "
                 "r.svr_ip, r.svr_port "
                 "FROM `%s`.`%s` r "
                 "INNER JOIN (%s) g "
                 "ON r.tenant_id = 0 "
                 "AND r.target_data_sync_scn = g.target_data_sync_scn "
                 "AND r.refresh_id = g.refresh_id "
                 "WHERE r.tenant_id = 0"
                 ") t "
                 "LEFT JOIN `%s`.`%s` d "
                 "ON d.tenant_id = 0 AND d.mview_id = t.mview_id "
                 "ORDER BY t.target_data_sync_scn, t.refresh_id, t.seq",
                 QUERY_TIMEOUT_US,
                 OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME,
                 inner_sql.ptr(),
                 OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME))) {
    LOG_WARN("fail to assign sql", KR(ret));
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::fill_task_dep_ids(
    common::ObIAllocator &alloc,
    ObMViewPendingTask &task,
    const common::ObIArray<uint64_t> &flat_deps,
    int64_t dep_range_start,
    int64_t dep_range_end,
    const common::ObIArray<ObMViewPendingTask *> &tasks,
    int64_t group_start,
    int64_t group_end)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<uint64_t, 16> valid_deps;
  for (int64_t j = dep_range_start; OB_SUCC(ret) && j < dep_range_end; ++j) {
    const uint64_t dep_id = flat_deps.at(j);
    bool found = false;
    for (int64_t k = group_start; !found && k < group_end; ++k) {
      found = (OB_NOT_NULL(tasks.at(k)) && tasks.at(k)->mview_id_ == dep_id);
    }
    if (found && OB_FAIL(valid_deps.push_back(dep_id))) {
      LOG_WARN("push valid dep failed", KR(ret), K(dep_id));
    }
  }
  if (OB_SUCC(ret) && valid_deps.count() > 0) {
    const int64_t valid_cnt = valid_deps.count();
    uint64_t *dep_arr = static_cast<uint64_t *>(
        alloc.alloc(valid_cnt * static_cast<int64_t>(sizeof(uint64_t))));
    if (OB_ISNULL(dep_arr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc dep ids", KR(ret), K(valid_cnt));
    } else {
      for (int64_t k = 0; k < valid_cnt; ++k) {
        dep_arr[k] = valid_deps.at(k);
      }
      task.dep_mview_ids_ = dep_arr;
      task.dep_mview_id_cnt_ = valid_cnt;
    }
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::assign_batch_dep_ids(
    common::ObIAllocator &alloc,
    common::ObIArray<ObMViewPendingTask *> &tasks,
    const common::ObIArray<uint64_t> &flat_deps,
    const common::ObIArray<int64_t> &dep_start)
{
  int ret = OB_SUCCESS;
  // Tasks are ordered by (target_data_sync_scn, refresh_id, seq) from the SQL,
  // so tasks sharing the same refresh_id are always contiguous. Walk group by group.
  int64_t group_start = 0;
  while (OB_SUCC(ret) && group_start < tasks.count()) {
    ObMViewPendingTask *first = tasks.at(group_start);
    if (OB_ISNULL(first)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null task in batch", KR(ret), K(group_start));
    } else {
      const int64_t refresh_id = first->refresh_id_;
      int64_t group_end = group_start + 1;
      while (group_end < tasks.count()
             && OB_NOT_NULL(tasks.at(group_end))
             && tasks.at(group_end)->refresh_id_ == refresh_id) {
        ++group_end;
      }
      for (int64_t i = group_start; OB_SUCC(ret) && i < group_end; ++i) {
        const int64_t dep_range_start = dep_start.at(i);
        const int64_t dep_range_end = (i + 1 < dep_start.count())
                                          ? dep_start.at(i + 1)
                                          : flat_deps.count();
        if (OB_FAIL(fill_task_dep_ids(alloc, *tasks.at(i), flat_deps,
                                       dep_range_start, dep_range_end,
                                       tasks, group_start, group_end))) {
          LOG_WARN("fill task dep ids failed", KR(ret), K(i));
        }
      }
      group_start = group_end;
    }
  }
  return ret;
}

int ObMViewPendingTaskTableOperator::load_tasks_batch(
    common::ObIAllocator &alloc,
    common::ObIArray<ObMViewPendingTask *> &tasks,
    uint64_t last_scn,
    int64_t last_refresh_id,
    int64_t refresh_limit)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = MTL_ID();
  tasks.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table operator not init", KR(ret));
  } else if (OB_FAIL(build_load_tasks_sql(sql, last_scn, last_refresh_id, refresh_limit))) {
    LOG_WARN("fail to build load tasks sql", KR(ret), K(last_scn), K(last_refresh_id),
             K(refresh_limit));
  } else {
    // flat_deps + dep_start stage dep ids during the streaming phase; we cannot size
    // each task's dep array until we see its last LEFT JOIN row.
    common::ObSEArray<uint64_t, 16> flat_deps;
    common::ObSEArray<int64_t, 64> dep_start;
    int64_t prev_refresh_id = -1;
    uint64_t prev_mview_id = OB_INVALID_ID;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute read", KR(ret), K(sql), K(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(sql), K(tenant_id));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          int64_t refresh_id = 0;
          uint64_t mview_id = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "refresh_id", refresh_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "mview_id", mview_id, uint64_t);
          if (OB_SUCC(ret) && (refresh_id != prev_refresh_id || mview_id != prev_mview_id)) {
            ObMViewPendingTask *task = OB_NEWx(ObMViewPendingTask, &alloc);
            if (OB_ISNULL(task)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc pending task", KR(ret));
            } else if (OB_FAIL(extract_task_from_result(*result, tenant_id, *task))) {
              LOG_WARN("fail to extract task", KR(ret), K(tenant_id));
            } else if (OB_FAIL(tasks.push_back(task))) {
              LOG_WARN("fail to push back task", KR(ret), KPC(task));
            } else if (OB_FAIL(dep_start.push_back(flat_deps.count()))) {
              LOG_WARN("fail to push dep_start", KR(ret));
            } else {
              prev_refresh_id = refresh_id;
              prev_mview_id = mview_id;
            }
          }
          if (OB_SUCC(ret)) {
            uint64_t dep_id = OB_INVALID_ID;
            EXTRACT_INT_FIELD_MYSQL(*result, "dep_mview_id", dep_id, uint64_t);
            if (OB_LIKELY(OB_ERR_NULL_VALUE == ret)) {
              ret = OB_SUCCESS;
            } else if (OB_SUCC(ret) && OB_INVALID_ID != dep_id && 0 != dep_id) {
              if (OB_FAIL(flat_deps.push_back(dep_id))) {
                LOG_WARN("fail to push dep_id", KR(ret), K(dep_id));
              }
            }
          }
        }
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("fail to get next row", KR(ret), K(sql), K(tenant_id));
        }
      }
    }
    // dep_mview_ids_ must be a subset of tasks actually loaded in this batch;
    // otherwise peek_task would loop on unsatisfiable deps and cancel walks
    // would fail to find the target. The raw __all_mview_dep join may bring in
    // base tables (p_type=1) or mviews not registered as pending tasks (e.g.
    // added by DDL after the refresh was scheduled), all of which must be
    // dropped here so the in-memory dep graph matches the snapshot persisted
    // at schedule time.
    if (OB_SUCC(ret) && OB_FAIL(assign_batch_dep_ids(alloc, tasks, flat_deps, dep_start))) {
      LOG_WARN("fail to assign batch dep ids", KR(ret));
    }
  }
  LOG_INFO("load tasks batch done", KR(ret), K(tenant_id), "task_count", tasks.count(),
           K(last_scn), K(last_refresh_id), K(refresh_limit));
  return ret;
}

int ObMViewPendingTaskTableOperator::get_min_pending_task_snapshot(share::SCN &scn)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(!is_inited_)) {
    // not inited yet, do not contribute to GC lower bound; callsite guards by is_valid()
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT /*+ query_timeout(10000000) */ target_data_sync_scn "
                 "FROM `%s`.`%s` ORDER BY tenant_id, refresh_id LIMIT 1",
                 OB_SYS_DATABASE_NAME,
                 OB_ALL_MVIEW_REFRESH_PENDING_TASK_TNAME))) {
    LOG_WARN("fail to format sql", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute read", KR(ret), K(sql), K(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next row", KR(ret));
        }
      } else {
        uint64_t target_data_sync_scn = 0;
        EXTRACT_UINT_FIELD_MYSQL(*result, "target_data_sync_scn", target_data_sync_scn, uint64_t);
        if (OB_FAIL(ret)) {
          LOG_WARN("fail to extract target_data_sync_scn", KR(ret));
        } else if (OB_FAIL(scn.convert_for_inner_table_field(target_data_sync_scn))) {
          LOG_WARN("fail to convert scn", KR(ret), K(target_data_sync_scn));
        }
      }
    }
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
