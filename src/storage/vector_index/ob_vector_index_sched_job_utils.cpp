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
#include "common/object/ob_object.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"

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
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    int64_t start_date_us = start_date.is_null() ? ObTimeUtility::current_time()
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
          lib::is_oracle_mode() ? ObString("ROOT") : ObString("root@%");
      job_info.job_style_ = ObString("regular");
      job_info.job_type_ = ObString("PLSQL_BLOCK");
      job_info.job_class_ = ObString("DATE_EXPRESSION_JOB_CLASS");
      job_info.what_ = job_action;
      job_info.start_date_ = start_date_us;
      job_info.end_date_ = end_date_us;
      job_info.interval_ = job_info.repeat_interval_;
      job_info.repeat_interval_ = ObString();
      job_info.enabled_ = 1;
      job_info.auto_drop_ = 0;
      job_info.max_run_duration_ = 24 * 60 * 60; // set to 1 day
      job_info.interval_ts_ = repeat_interval_ts;
      job_info.scheduler_flags_ =
          ObDBMSSchedJobInfo::JOB_SCHEDULER_FLAG_DATE_EXPRESSION_JOB_CLASS;
      job_info.exec_env_ = exec_env;

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
    const common::ObString &vec_id_index_tb_name,
    const common::ObString &db_name, const common::ObString &table_name,
    const common::ObString &index_name, const common::ObString &exec_env) {
  int ret = OB_SUCCESS;
  int64_t job_id = OB_INVALID_ID;
  common::ObObj start_date;
  start_date.set_null();
  if (OB_FAIL(ObMViewSchedJobUtils::generate_job_id(tenant_id, job_id))) {
    LOG_WARN("failed to generate vector index refresh job id", K(ret));
  } else {
    ObSqlString job_action;
    if (OB_FAIL(job_action.assign_fmt(
            "DBMS_VECTOR.refresh_index('%.*s.%.*s', '%.*s.%.*s', '', %lu, "
            "'FAST')",
            static_cast<int>(db_name.length()), db_name.ptr(),
            static_cast<int>(index_name.length()), index_name.ptr(),
            static_cast<int>(db_name.length()), db_name.ptr(),
            static_cast<int>(table_name.length()), table_name.ptr(),
            ObVectorIndexSchedJobUtils::DEFAULT_REFRESH_TRIGGER_THRESHOLD))) {
      LOG_WARN("failed to generate refresh index job id", K(ret));
    } else if (OB_FAIL(ObVectorIndexSchedJobUtils::add_scheduler_job(
                   sql_client, tenant_id, job_id, vec_id_index_tb_name,
                   job_action.string(), start_date,
                   ObVectorIndexSchedJobUtils::DEFAULT_REFRESH_INTERVAL_TS,
                   exec_env))) {
      LOG_WARN("failed to add refresh index job", K(ret), K(vec_id_index_tb_name),
               K(job_action), K(exec_env));
    } else {
      LOG_INFO("succeed to add refresh index job", K(ret), K(vec_id_index_tb_name),
               K(job_action), K(exec_env));
    }
  }
  return ret;
}

int ObVectorIndexSchedJobUtils::remove_vector_index_refresh_job(
    common::ObISQLClient &sql_client, const uint64_t tenant_id,
    const common::ObString &vec_id_index_tb_name) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDBMSSchedJobUtils::remove_dbms_sched_job(
        sql_client, tenant_id, vec_id_index_tb_name, true))) {
    LOG_WARN("failed to remove vector index refresh job",
        KR(ret), K(tenant_id), K(vec_id_index_tb_name));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase