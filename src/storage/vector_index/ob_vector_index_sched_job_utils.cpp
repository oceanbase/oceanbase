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

#define USING_LOG_PREFIX STORAGE

#include "storage/vector_index/ob_vector_index_sched_job_utils.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase {
using namespace common;
using namespace dbms_scheduler;
using namespace share;
using namespace share::schema;
using namespace sql;

namespace storage {

int ObVectorIndexSchedJobUtils::add_scheduler_job(
    common::ObISQLClient &sql_client, const uint64_t tenant_id,
    const int64_t job_id, const common::ObString &job_name,
    const common::ObString &job_action, const common::ObObj &start_date,
    const int64_t repeat_interval_ts, const common::ObString &exec_env) {
  int ret = OB_SUCCESS;
  ObSqlString interval_str;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(interval_str.append_fmt("FREQ=SECONDLY; INTERVAL=%ld", repeat_interval_ts / 1000000L))) {
    LOG_WARN("fail to append interval string", K(ret));
  } else {
    int64_t start_date_us = start_date.is_null() ? ObTimeUtility::current_time() + repeat_interval_ts
                                                 : start_date.get_timestamp();
    int64_t end_date_us = 64060560000000000; // 4000-01-01
    HEAP_VAR(ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = tenant_id;
      job_info.job_ = job_id;
      job_info.job_name_ = job_name;
      job_info.job_action_ = job_action;
      job_info.lowner_ = ObString("oceanbase");
      job_info.cowner_ = ObString("oceanbase");
      job_info.powner_ =
          lib::is_oracle_mode() ? ObString("SYS") : ObString("root@%");
      job_info.job_style_ = ObString("regular");
      job_info.job_type_ = ObString("PLSQL_BLOCK");
      job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
      job_info.start_date_ = start_date_us;
      job_info.end_date_ = end_date_us;
      job_info.repeat_interval_ = interval_str.string();
      job_info.enabled_ = 1;
      job_info.auto_drop_ = 0;
      job_info.max_run_duration_ = 24 * 60 * 60; // set to 1 day
      job_info.interval_ts_ = repeat_interval_ts;
      job_info.exec_env_ = exec_env;
      job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::VECTOR_INDEX_REFRESH_JOB;
      if (OB_FAIL(ObDBMSSchedJobUtils::create_dbms_sched_job(
              sql_client, tenant_id, job_id, job_info))) {
        LOG_WARN("failed to create dbms scheduler job", KR(ret));
      }
    }
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::add_vector_index_refresh_job(
    common::ObISQLClient &sql_client, const uint64_t tenant_id,
    const uint64_t vidx_table_id, const common::ObString &exec_env) {
  LOG_INFO("################## [add_vector_index_refresh_job]", K(tenant_id), K(vidx_table_id), K(exec_env));
  int ret = OB_SUCCESS;
  int64_t job_id = OB_INVALID_ID;
  common::ObObj start_date;
  start_date.set_null();
  if (OB_FAIL(ObMViewSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("failed to generate vector index refresh job id", K(ret));
  } else {
    ObSqlString job_action;
    ObSqlString refresh_job_name;
    if (OB_FAIL(job_action.assign_fmt(
            "DBMS_VECTOR.refresh_index_inner(%lu, %lu)",
            vidx_table_id,
            ObVectorIndexSchedJobUtils::DEFAULT_REFRESH_TRIGGER_THRESHOLD))) {
      LOG_WARN("failed to generate refresh index job id", K(ret));
    } else if (OB_FAIL(refresh_job_name.assign_fmt("%lu_refresh", vidx_table_id))) {
      LOG_WARN("failed to generate refresh job name", K(ret));
    } else if (OB_FAIL(ObVectorIndexSchedJobUtils::add_scheduler_job(
                   sql_client, tenant_id, job_id, refresh_job_name.string(),
                   job_action.string(), start_date,
                   ObVectorIndexSchedJobUtils::DEFAULT_REFRESH_INTERVAL_TS,
                   exec_env))) {
      LOG_WARN("failed to add refresh index job", K(ret), K(vidx_table_id),
               K(job_action), K(exec_env));
    } else {
      LOG_INFO("succeed to add refresh index job", K(ret), K(vidx_table_id),
               K(job_action), K(exec_env));
    }
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::remove_vector_index_refresh_job(
    common::ObISQLClient &sql_client, const uint64_t tenant_id,
    const uint64_t vidx_table_id) {
  LOG_INFO("################## [remove_vector_index_refresh_job]", K(vidx_table_id));
  int ret = OB_SUCCESS;
  ObSqlString refresh_job_name;
  if (OB_FAIL(refresh_job_name.assign_fmt("%lu_refresh", vidx_table_id))) {
    LOG_WARN("failed to generate refresh job name", K(ret));
  } else if (OB_FAIL(ObDBMSSchedJobUtils::remove_dbms_sched_job(
        sql_client, tenant_id, refresh_job_name.string(), true))) {
    LOG_WARN("failed to remove vector index refresh job",
        KR(ret), K(tenant_id), K(vidx_table_id));
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::add_vector_index_rebuild_job(common::ObISQLClient &sql_client,
                                                             const uint64_t tenant_id,
                                                             const uint64_t vidx_table_id,
                                                             const common::ObString &exec_env)
{
  LOG_INFO("################## [add_vector_index_rebuild_job]", K(tenant_id), K(vidx_table_id), K(exec_env));
  int ret = OB_SUCCESS;
  int64_t job_id = OB_INVALID_ID;
  common::ObObj start_date;
  start_date.set_null();
  if (OB_FAIL(ObMViewSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("failed to generate vector index refresh job id", K(ret));
  } else {
    ObSqlString job_action;
    ObSqlString rebuild_job_name;
    if (OB_FAIL(job_action.assign_fmt(
            "DBMS_VECTOR.rebuild_index_inner(%lu, %lf)",
            vidx_table_id,
            ObVectorIndexSchedJobUtils::DEFAULT_REBUILD_TRIGGER_THRESHOLD))) {
      LOG_WARN("failed to generate rebuild index job id", K(ret));
    } else if (OB_FAIL(rebuild_job_name.assign_fmt("%lu_rebuild", vidx_table_id))) {
      LOG_WARN("failed to generate rebuild job name", K(ret));
    } else if (OB_FAIL(ObVectorIndexSchedJobUtils::add_scheduler_job(
                   sql_client, tenant_id, job_id, rebuild_job_name.string(),
                   job_action.string(), start_date,
                   ObVectorIndexSchedJobUtils::DEFAULT_REBUILD_INTERVAL_TS,
                   exec_env))) {
      LOG_WARN("failed to add rebuild index job", K(ret), K(vidx_table_id),
               K(job_action), K(exec_env));
    } else {
      LOG_INFO("succeed to add rebuild index job", K(ret), K(vidx_table_id),
               K(job_action), K(exec_env));
    }
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::remove_vector_index_rebuild_job(common::ObISQLClient &sql_client,
                                                                const uint64_t tenant_id,
                                                                const uint64_t vidx_table_id)
{
  LOG_INFO("################## [remove_vector_index_rebuild_job]", K(vidx_table_id));
  int ret = OB_SUCCESS;
  ObSqlString rebuild_job_name;
  if (OB_FAIL(rebuild_job_name.assign_fmt("%lu_rebuild", vidx_table_id))) {
    LOG_WARN("failed to generate refresh job name", K(ret));
  } else if (OB_FAIL(ObDBMSSchedJobUtils::remove_dbms_sched_job(
        sql_client, tenant_id, rebuild_job_name.string(), true))) {
    LOG_WARN("failed to remove vector index rebuild job",
        KR(ret), K(tenant_id), K(vidx_table_id));
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::get_vector_index_job_info(common::ObISQLClient &sql_client,
                                                          const uint64_t tenant_id,
                                                          const uint64_t vidx_table_id,
                                                          common::ObIAllocator &allocator,
                                                          share::schema::ObSchemaGetterGuard &schema_guard,
                                                          dbms_scheduler::ObDBMSSchedJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTenantSchema *tenant_schema = NULL;
  ObSqlString refresh_job_name;
  if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant schema is null", K(tenant_id)); // skip
  } else if (OB_FAIL(refresh_job_name.assign_fmt("%lu_refresh", vidx_table_id))) {
    LOG_WARN("failed to generate refresh job name", K(ret));
  } else if (OB_FAIL(ObDBMSSchedJobUtils::get_dbms_sched_job_info(sql_client, tenant_id,
                                                                  tenant_schema->is_oracle_tenant(),
                                                                  refresh_job_name.string(),
                                                                  allocator,
                                                                  job_info))) {
    LOG_WARN("fail to get dbms schedule info", K(ret), K(tenant_id), K(refresh_job_name));
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase