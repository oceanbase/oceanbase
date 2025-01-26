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
#define USING_LOG_PREFIX SHARE

#include "ob_flush_ncomp_dll_task.h"

#include "share/ob_all_server_tracer.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h" // ObDbmsStatsMaintenanceWindow
#include "observer/dbms_scheduler/ob_dbms_sched_table_operator.h" // ObDBMSSchedTableOperator

namespace oceanbase
{
namespace share
{
using namespace common;

#define USEC_OF_HOUR (60 * 60 * 1000000LL)

int ObFlushNcompDll::check_flush_ncomp_dll_job_exists(ObMySQLTransaction &trans,
                                                        const uint64_t tenant_id,
                                                        const ObString &job_name,
                                                        bool &is_job_exists)
{
  int ret = OB_SUCCESS;
  is_job_exists = false;
  ObSqlString select_sql;
  int64_t row_count = 0;
  if (OB_FAIL(select_sql.append_fmt("SELECT count(*) FROM %s WHERE tenant_id = %ld and job_name = '%.*s';",
                                    share::OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
                                    share::schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                    job_name.length(), job_name.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, proxy_result) {
      sqlclient::ObMySQLResult *client_result = NULL;
      if (OB_FAIL(trans.read(proxy_result, tenant_id, select_sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(ret), K(select_sql));
      } else if (OB_ISNULL(client_result = proxy_result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to execute sql", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(client_result->next())) {
          int64_t idx = 0;
          ObObj obj;
          if (OB_FAIL(client_result->get_obj(idx, obj))) {
            LOG_WARN("failed to get object", K(ret));
          } else if (OB_FAIL(obj.get_int(row_count))) {
            LOG_WARN("failed to get int", K(ret), K(obj));
          } else if (OB_UNLIKELY(row_count != 2 && row_count != 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(row_count));
          } else {
            is_job_exists = row_count > 0;
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
    LOG_INFO("succeed to check flush ncomp dll job exists", K(ret), K(select_sql), K(is_job_exists), K(row_count));
  }
  return ret;
}

int ObFlushNcompDll::get_job_id(const uint64_t tenant_id,
                                ObMySQLTransaction &trans,
                                int64_t &job_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  job_id = OB_INVALID_INDEX;
  if (OB_FAIL(sql.assign_fmt("select max(job) + 1 as new_job_id from %s where job <= %ld",
      OB_ALL_TENANT_SCHEDULER_JOB_TNAME,
      dbms_scheduler::ObDBMSSchedTableOperator::JOB_ID_OFFSET))) {
    LOG_WARN("assign fmt failed", KR(ret), K(tenant_id));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(trans.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get job", KR(ret), K(sql));
      } else if (OB_FAIL(result->get_int("new_job_id", job_id))) {
        LOG_WARN("get int failed", KR(ret), K(tenant_id), K(sql));
      } else {
         LOG_INFO("get new job_id successfully", KR(ret), K(tenant_id), K(job_id));
      }
    }
  }

  return ret;
}

int ObFlushNcompDll::get_job_action(ObSqlString &job_action)
{
  int ret = OB_SUCCESS;

  common::ObZone zone;
  ObArray<ObServerInfoInTable> servers_info;
  common::hash::ObHashSet<ObServerInfoInTable::ObBuildVersion> observer_version_set;
  bool need_comma = false;

  job_action.reset();

  OZ (observer_version_set.create((4)));
  OZ (share::ObAllServerTracer::get_instance().get_servers_info(zone, servers_info));
  for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); ++i) {
    OZ (observer_version_set.set_refactored(servers_info.at(i).get_build_version()));
  }

  //OZ (get_package_and_svn(build_version, sizeof(build_version)));
  //OZ (job_action.assign_fmt("delete FROM %s where build_version != '%s'", OB_ALL_NCOMP_DLL_V2_TNAME, build_version));
  OZ (job_action.append_fmt("delete FROM %s where build_version not in (", OB_ALL_NCOMP_DLL_V2_TNAME));
  for (common::hash::ObHashSet<ObServerInfoInTable::ObBuildVersion>::const_iterator iter = observer_version_set.begin();
      OB_SUCC(ret) && iter != observer_version_set.end();
      iter++) {
    OZ(job_action.append_fmt("%s'%s'", need_comma ? ", " : "", iter->first.ptr()));
    OX (need_comma = true);
  }
  OZ(job_action.append(")"));

  if (observer_version_set.created()) {
    observer_version_set.destroy();
  }

  return ret;
}

int ObFlushNcompDll::create_flush_ncomp_dll_job_for_425(const ObSysVariableSchema &sys_variable,
                                                        const uint64_t tenant_id,
                                                        const bool is_enabled,
                                                        ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;

  bool is_job_exists = false;
  ObSqlString job_action;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_flush_ncomp_dll_job_exists(trans, tenant_id, async_flush_ncomp_dll_for_425, is_job_exists))) {
    LOG_WARN("fail to check ncomp dll job", K(ret));
  } else if (is_job_exists) {
    // do nothing
  } else if (OB_FAIL(job_action.assign_fmt("delete FROM %s", OB_ALL_NCOMP_DLL_TNAME))) {
    LOG_WARN("fail to get job action", K(ret));
  } else if (OB_FAIL(create_flush_ncomp_dll_job_common(sys_variable,
                                               tenant_id,
                                               is_enabled,
                                               trans,
                                               job_action,
                                               async_flush_ncomp_dll_for_425))) {
    LOG_WARN("fail to create flush ncomp dll job", K(ret));
  }
  return ret;
}

int ObFlushNcompDll::create_flush_ncomp_dll_job(const ObSysVariableSchema &sys_variable,
                                                const uint64_t tenant_id,
                                                const bool is_enabled,
                                                ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;

  bool is_job_exists = false;
  ObSqlString job_action;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_flush_ncomp_dll_job_exists(trans, tenant_id, async_flush_ncomp_dll, is_job_exists))) {
    LOG_WARN("fail to check ncomp dll job", K(ret));
  } else if (is_job_exists) {
    // do nothing
  } else if (OB_FAIL(get_job_action(job_action))) {
    LOG_WARN("fail to get job action", K(ret));
  } else if (OB_FAIL(create_flush_ncomp_dll_job_common(sys_variable,
                                               tenant_id,
                                               is_enabled,
                                               trans,
                                               job_action,
                                               async_flush_ncomp_dll))) {
    LOG_WARN("fail to create flush ncomp dll job", K(ret));
  }
  return ret;
}

int ObFlushNcompDll::create_flush_ncomp_dll_job_common(const ObSysVariableSchema &sys_variable,
                                                        const uint64_t tenant_id,
                                                        const bool is_enabled,
                                                        ObMySQLTransaction &trans,
                                                        const ObSqlString &job_action,
                                                        const ObString &job_name)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(trans, tenant_id);
  bool is_oracle_mode = false;
  char buf[OB_MAX_PROC_ENV_LENGTH] = {0};
  int64_t pos = 0;
  int32_t offset_sec = 0;
  ObTime ob_time;
  int64_t job_id = OB_INVALID_INDEX;
  int64_t affected_rows = 0;
  int64_t current_time = ObTimeUtility::current_time();

  if (OB_FAIL(sys_variable.get_oracle_mode(is_oracle_mode))) {
    LOG_WARN("failed to get oracle mode", KR(ret));
  } else if (OB_FAIL(sql::ObExecEnv::gen_exec_env(sys_variable, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("failed to gen exec env", KR(ret));
  } else if (OB_FAIL(ObDbmsStatsMaintenanceWindow::get_time_zone_offset(sys_variable,
                                                                        tenant_id,
                                                                        offset_sec))) {
    LOG_WARN("failed to get time zone offset", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObTimeConverter::usec_to_ob_time(current_time + offset_sec * USECS_PER_SEC,
                                                      ob_time))) {
    LOG_WARN("failed to usec to ob time", KR(ret), K(current_time), K(offset_sec));
  } else if (OB_FAIL(get_job_id(tenant_id, trans, job_id))) {
    LOG_WARN("get new job_id failed", KR(ret), K(tenant_id));
  } else {
    ObString exec_env(pos, buf);
    int64_t current_hour = ob_time.parts_[DT_HOUR];
    int64_t hours_to_next_day = HOURS_PER_DAY - current_hour;
    const int64_t start_usec = (current_time / USEC_OF_HOUR + hours_to_next_day) * USEC_OF_HOUR; // next day 00:00:00
    HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = tenant_id;
      job_info.job_ = job_id;
      job_info.job_name_ = job_name;
      job_info.job_action_ = job_action.ptr();
      job_info.lowner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.powner_ = is_oracle_mode ? ObString("SYS") : ObString("root@%");
      job_info.cowner_ = is_oracle_mode ? ObString("SYS") :  ObString("oceanbase");
      job_info.job_style_ = ObString("regular");
      job_info.job_type_ = ObString("PLSQL_BLOCK");
      job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
      job_info.start_date_ = start_usec;
      job_info.end_date_ = 64060560000000000; // 4000-01-01 00:00:00.000000
      job_info.repeat_interval_ = ObString();
      job_info.enabled_ = is_enabled;
      job_info.auto_drop_ = true;
      job_info.max_run_duration_ = SECS_PER_HOUR * 2;
      job_info.exec_env_ = exec_env;
      job_info.comments_ = ObString("used to auto flush ncomp dll table expired data");
      job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::FLUSH_NCOMP_DLL_JOB;

      if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(trans,
                                                                            tenant_id,
                                                                            job_id,
                                                                            job_info))) {
        LOG_WARN("failed to create flush ncomp dll job", KR(ret), K(job_action));
      } else {
        LOG_INFO("finish create flush ncomp dll job", K(tenant_id), K(is_enabled),
            K(offset_sec), K(job_id), K(exec_env),
            K(start_usec), K(affected_rows), K(job_action));
      }
    }
  }

  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase