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

#include "ob_dbms_sched_job_utils.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "lib/worker.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_schema_utils.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/ob_server_struct.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h"
#include "observer/dbms_scheduler/ob_dbms_sched_table_operator.h"
#include "storage/ob_common_id_utils.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_rpc_proxy.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;
using namespace sqlclient;
using namespace sql;

namespace dbms_scheduler
{

int ObDBMSSchedJobInfo::deep_copy(ObIAllocator &allocator, const ObDBMSSchedJobInfo &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  user_id_ = other.user_id_;
  database_id_ = other.database_id_;
  job_ = other.job_;
  last_modify_ = other.last_modify_;
  last_date_ = other.last_date_;
  this_date_ = other.this_date_;
  next_date_ = other.next_date_;
  total_ = other.total_;
  failures_ = other.failures_;
  flag_ = other.flag_;
  scheduler_flags_ = other.scheduler_flags_;
  start_date_ = other.start_date_;
  end_date_ = other.end_date_;
  enabled_ = other.enabled_;
  auto_drop_ = other.auto_drop_;
  interval_ts_ = other.interval_ts_;
  is_oracle_tenant_ = other.is_oracle_tenant_;
  max_run_duration_ = other.max_run_duration_;

  OZ (ob_write_string(allocator, other.lowner_, lowner_));
  OZ (ob_write_string(allocator, other.powner_, powner_));
  OZ (ob_write_string(allocator, other.cowner_, cowner_));

  OZ (ob_write_string(allocator, other.interval_, interval_));

  OZ (ob_write_string(allocator, other.what_, what_));
  OZ (ob_write_string(allocator, other.nlsenv_, nlsenv_));
  OZ (ob_write_string(allocator, other.charenv_, charenv_));
  OZ (ob_write_string(allocator, other.field1_, field1_));
  OZ (ob_write_string(allocator, other.exec_env_, exec_env_));
  OZ (ob_write_string(allocator, other.job_name_, job_name_));
  OZ (ob_write_string(allocator, other.job_class_, job_class_));
  OZ (ob_write_string(allocator, other.program_name_, program_name_));
  OZ (ob_write_string(allocator, other.state_, state_));
  OZ (ob_write_string(allocator, other.job_action_, job_action_));
  return ret;
}

int ObDBMSSchedJobClassInfo::deep_copy(common::ObIAllocator &allocator, const ObDBMSSchedJobClassInfo &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  is_oracle_tenant_ = other.is_oracle_tenant_;
  log_history_ = other.log_history_;
  OZ (ob_write_string(allocator, other.job_class_name_, job_class_name_));
  OZ (ob_write_string(allocator, other.service_, service_));
  OZ (ob_write_string(allocator, other.resource_consumer_group_, resource_consumer_group_));
  OZ (ob_write_string(allocator, other.logging_level_, logging_level_));
  OZ (ob_write_string(allocator, other.comments_, comments_));
  return ret;
}

int ObDBMSSchedJobUtils::generate_job_id(int64_t tenant_id, int64_t &max_job_id)
{
  int ret = OB_SUCCESS;
  ObCommonID raw_id;
  if (OB_FAIL(storage::ObCommonIDUtils::gen_unique_id_by_rpc(tenant_id, raw_id))) {
    LOG_WARN("gen unique id failed", K(ret), K(tenant_id));
  } else {
    max_job_id = raw_id.id() + ObDBMSSchedTableOperator::JOB_ID_OFFSET;
  }
  return ret;
}

int ObDBMSSchedJobUtils::disable_dbms_sched_job(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObString &job_name,
    const bool if_exists)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || job_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(job_name));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(
        "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("job_name", job_name))
        || OB_FAIL(dml.add_column("enabled", false))) {
      LOG_WARN("add column failed", KR(ret));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_update(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, dml, affected_rows))) {
        LOG_WARN("execute update failed", KR(ret));
      } else if (!if_exists && !is_double_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be two", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobUtils::stop_dbms_sched_job(
    common::ObISQLClient &sql_client,
    const ObDBMSSchedJobInfo &job_info,
    const bool is_delete_after_stop)
{
  int ret = OB_SUCCESS;
  obrpc::ObDBMSSchedJobRpcProxy *rpc_proxy = GCTX.dbms_sched_job_rpc_proxy_;
  uint64_t tenant_id = job_info.tenant_id_;
  ObSqlString sql;
  CK (OB_NOT_NULL(rpc_proxy));
  uint64_t data_version = 0;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
    } else if (data_version < MOCK_DATA_VERSION_4_2_1_5
      || (data_version >= DATA_VERSION_4_2_2_0 && data_version < MOCK_DATA_VERSION_4_2_4_0)
      || (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_3_2_0)) {
      ret = OB_NOT_SUPPORTED;
    }
  }
  if (OB_SUCC(ret)) {
    dbms_scheduler::ObDBMSSchedJobInfo update_job_info;
    update_job_info.tenant_id_ = job_info.tenant_id_;
    update_job_info.is_oracle_tenant_ = job_info.is_oracle_tenant_;
    update_job_info.job_name_ = job_info.job_name_;
    if (is_delete_after_stop) {
      update_job_info.state_ = "KILLED";
    }
    if(OB_FAIL(update_dbms_sched_job_info(sql_client, update_job_info))) {
      LOG_WARN("update job info failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.append_fmt("select svr_ip, svr_port, session_id from %s where tenant_id = %lu and job_name = \'%.*s\'",
      OB_ALL_VIRTUAL_TENANT_SCHEDULER_RUNNING_JOB_TNAME, tenant_id, job_info.job_name_.length(),job_info.job_name_.ptr()))) {
      LOG_WARN("append sql failed", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, result) {
        if (OB_FAIL(sql_client.read(result, sql.ptr()))) {
          LOG_WARN("execute query failed", K(ret), K(sql), K(tenant_id), K(job_info.job_name_));
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", K(ret), K(sql), K(tenant_id), K(job_info.job_name_));
        } else {
          bool result_empty = true;
          do {
            if (OB_FAIL(result.get_result()->next())) {
              if (ret == OB_ITER_END) {
                //do nothing
              } else {
                LOG_WARN("fail to get result", K(ret));
              }
            } else {
              result_empty = false;
              uint64_t session_id = OB_INVALID_ID;
              ObAddr svr;
              ObString svr_ip;
              int64_t svr_port = OB_INVALID_INDEX;
              EXTRACT_VARCHAR_FIELD_MYSQL(*(result.get_result()), "svr_ip", svr_ip);
              EXTRACT_UINT_FIELD_MYSQL(*(result.get_result()), "session_id", session_id, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*(result.get_result()), "svr_port", svr_port, int64_t);
              if (OB_SUCC(ret)) {
                if (!svr.set_ip_addr(svr_ip, svr_port)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("set addr failed", K(svr_ip), K(svr_port));
                } else {
                  LOG_INFO("send rpc", K(tenant_id), K(job_info.job_name_), K(svr), K(session_id));
                  ObString stop_job_name = ObString(job_info.job_name_);
                  OZ (rpc_proxy->stop_dbms_sched_job(tenant_id,
                    stop_job_name,
                    svr,
                    session_id));
                }
              }
            }
          } while (OB_SUCC(ret));
          if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
          }
        }
      }
    }
  }
  LOG_INFO("stop job", K(ret), K(job_info.job_name_), K(tenant_id));
  return ret;
}

int ObDBMSSchedJobUtils::remove_dbms_sched_job(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObString &job_name,
    const bool if_exists)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || job_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(job_name));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(
        "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("job_name", job_name))) {
      LOG_WARN("add column failed", KR(ret));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_delete(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, dml, affected_rows))) {
        LOG_WARN("execute delete failed", KR(ret));
      } else if (is_zero_row(affected_rows) && !if_exists) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("execute delete failed", KR(ret), K(if_exists));
      } else if (!if_exists && !is_double_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be two", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobUtils::create_dbms_sched_job(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t job_id,
    const dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    if (OB_FAIL(add_dbms_sched_job(sql_client, tenant_id, job_id, job_info))) {
      LOG_WARN("failed to add dbms scheduler job", KR(ret));
    } else if (OB_FAIL(add_dbms_sched_job(sql_client, tenant_id, 0, job_info))) {
      LOG_WARN("failed to add dbms scheduler job", KR(ret));
    }
  }
  return ret;
}

int ObDBMSSchedJobUtils::add_dbms_sched_job(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t job_id,
    const dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    ObDMLSqlSplicer dml;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    const int64_t now = ObTimeUtility::current_time();
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get tenant data version", KR(ret), K(data_version));
    } else {
      OZ (dml.add_gmt_create(now));
      OZ (dml.add_gmt_modified(now));
      OZ (dml.add_pk_column("tenant_id",
          ObSchemaUtils::get_extract_tenant_id(job_info.tenant_id_, job_info.tenant_id_)));
      if ((MOCK_DATA_VERSION_4_2_4_0 <= data_version && DATA_VERSION_4_3_0_0 > data_version) ||
           data_version > DATA_VERSION_4_3_2_0) {
        OZ (dml.add_column("user_id", job_info.user_id_));
        OZ (dml.add_column("database_id", job_info.database_id_));
      }
      OZ (dml.add_pk_column("job", job_id));
      OZ (dml.add_column("lowner", ObHexEscapeSqlStr(job_info.lowner_)));
      OZ (dml.add_column("powner", ObHexEscapeSqlStr(job_info.powner_)));
      OZ (dml.add_column("cowner", ObHexEscapeSqlStr(job_info.cowner_)));
      OZ (dml.add_raw_time_column("next_date", job_info.start_date_));
      OZ (dml.add_column("total", 0));
      OZ (dml.add_column("`interval#`", ObHexEscapeSqlStr(
          job_info.repeat_interval_.empty() ? ObString("null") : job_info.repeat_interval_)));
      OZ (dml.add_column("flag", job_info.flag_));
      OZ (dml.add_column("job_name", ObHexEscapeSqlStr(job_info.job_name_)));
      OZ (dml.add_column("job_style", ObHexEscapeSqlStr(job_info.job_style_)));
      OZ (dml.add_column("job_type", ObHexEscapeSqlStr(job_info.job_type_)));
      OZ (dml.add_column("job_class", ObHexEscapeSqlStr(job_info.job_class_)));
      OZ (dml.add_column("job_action", ObHexEscapeSqlStr(job_info.job_action_)));
      OZ (dml.add_column("what", ObHexEscapeSqlStr(job_info.job_action_)));
      OZ (dml.add_raw_time_column("start_date", job_info.start_date_));
      OZ (dml.add_raw_time_column("end_date", job_info.end_date_));
      OZ (dml.add_column("repeat_interval", ObHexEscapeSqlStr(job_info.repeat_interval_)));
      OZ (dml.add_column("enabled", job_info.enabled_));
      OZ (dml.add_column("auto_drop", job_info.auto_drop_));
      OZ (dml.add_column("max_run_duration", job_info.max_run_duration_));
      OZ (dml.add_column("interval_ts", job_info.interval_ts_));
      OZ (dml.add_column("scheduler_flags", job_info.scheduler_flags_));
      OZ (dml.add_column("exec_env", job_info.exec_env_));
      OZ (dml.add_column("comments", ObHexEscapeSqlStr(job_info.comments_)));
    }

    if (OB_SUCC(ret) && OB_FAIL(exec.exec_insert(
        OB_ALL_TENANT_SCHEDULER_JOB_TNAME, dml, affected_rows))) {
      LOG_WARN("failed to execute insert", KR(ret));
    } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObDBMSSchedJobUtils::update_dbms_sched_job_info(
    common::ObISQLClient &sql_client,
    const ObDBMSSchedJobInfo &update_job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = update_job_info.tenant_id_;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || update_job_info.job_name_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(update_job_info));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column(
        "tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
        || OB_FAIL(dml.add_pk_column("job_name", update_job_info.job_name_))) {
      LOG_WARN("add column failed", KR(ret));
    } else if (!update_job_info.state_.empty() && OB_FAIL(dml.add_column("state", update_job_info.state_))) {
      LOG_WARN("add column failed", KR(ret), K(update_job_info.state_));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_update(OB_ALL_TENANT_SCHEDULER_JOB_TNAME, dml, affected_rows))) {
        LOG_WARN("execute update failed", KR(ret));
      } else if (is_zero_row(affected_rows)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("not change", KR(ret), K(affected_rows));
      } else if (!is_double_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be two", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobUtils::reserve_user_with_minimun_id(ObIArray<const ObUserInfo *> &user_infos)
{
  int ret = OB_SUCCESS;
  if (user_infos.count() > 1) {
    //bug:
    //resver the minimum user id to execute
    const ObUserInfo *minimum_user_info = user_infos.at(0);
    for (int64_t i = 1; OB_SUCC(ret) && i < user_infos.count(); ++i) {
      if (OB_ISNULL(minimum_user_info) || OB_ISNULL(user_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(minimum_user_info), K(user_infos.at(i)));
      } else if (minimum_user_info->get_user_id() > user_infos.at(i)->get_user_id()) {
        minimum_user_info = user_infos.at(i);
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      user_infos.reset();
      if (OB_FAIL(user_infos.push_back(minimum_user_info))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobUtils::get_dbms_sched_job_info(common::ObISQLClient &sql_client,
                                                 const uint64_t tenant_id,
                                                 const bool is_oracle_tenant,
                                                 const ObString &job_name,
                                                 common::ObIAllocator &allocator,
                                                 ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || job_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(job_name));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(sql.append_fmt("select * from %s where tenant_id = %ld and job_name = \'%.*s\' and job > 0",
                                                     OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
                                                     ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                                                     job_name.length(), job_name.ptr()))) {
        LOG_WARN("failed to assign sql", K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("execute query failed", K(ret), K(sql));
        } else {
          if (res.get_result() != NULL && OB_SUCCESS == (ret = res.get_result()->next())) {
            ObDBMSSchedTableOperator table_operator;
            OZ (table_operator.extract_info(*(res.get_result()), tenant_id, is_oracle_tenant, allocator, job_info));
          }
          if (OB_FAIL(ret)) {
            if (OB_ITER_END == ret) {
              ret = OB_ENTRY_NOT_EXIST;
            } else {
              LOG_WARN("next failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDBMSSchedJobUtils::check_dbms_sched_job_priv(const ObUserInfo *user_info,
                                                   const ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(user_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user info is NULL", KR(ret));
  } else if (is_root_user(user_info->get_user_id())) {
    // do nothing
  } else if (job_info.is_oracle_tenant_) {
    //TODO 连雨
  } else {
    if (job_info.user_id_ != OB_INVALID_ID) { //如果 job 有 user_id 优先使用
      if (job_info.user_id_ != user_info->get_user_id()) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("job user id check failed", KR(ret), K(user_info), K(job_info.user_id_));
      }
    } else if (0 != job_info.powner_.case_compare(user_info->get_user_name())) { // job 保存的 owner 可能是 root@% or root (旧)
      const char *c = job_info.powner_.reverse_find('@');
      if (OB_ISNULL(c)) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("job user id check failed", KR(ret), K(user_info), K(job_info.user_id_));
      } else {
        ObString user = job_info.powner_;
        ObString user_name;
        ObString host_name;
        user_name = user.split_on(c);
        host_name = user;
        if (0 != user_name.case_compare(user_info->get_user_name()) || 0 != host_name.case_compare(user_info->get_host_name())) {
          ret = OB_ERR_NO_PRIVILEGE;
          LOG_WARN("job user id check failed", KR(ret), K(user_info), K(job_info.user_id_));
        }
      }
    }
  }

  return ret;
}

} // end for namespace dbms_scheduler
} // end for namespace oceanbase
