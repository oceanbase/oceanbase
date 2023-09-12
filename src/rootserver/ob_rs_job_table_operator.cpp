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
#include "ob_rs_job_table_operator.h"
#include "share/ob_upgrade_utils.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;

int ObRsJobInfo::deep_copy_self()
{
  int ret = OB_SUCCESS;
  allocator_.reset();
  if (OB_SUCC(ret)) {
    ret = ob_write_string(allocator_, job_type_str_, job_type_str_);
  }
  if (OB_SUCC(ret)) {
    ret = ob_write_string(allocator_, job_status_str_, job_status_str_);
  }
  if (OB_SUCC(ret)) {
    ret = ob_write_string(allocator_, tenant_name_, tenant_name_);
  }
  if (OB_SUCC(ret)) {
    ret = ob_write_string(allocator_, database_name_, database_name_);
  }
  if (OB_SUCC(ret)) {
    ret = ob_write_string(allocator_, table_name_, table_name_);
  }
  if (OB_SUCC(ret)) {
    ret = ob_write_string(allocator_, sql_text_, sql_text_);
  }
  if (OB_SUCC(ret)) {
    ret = ob_write_string(allocator_, extra_info_, extra_info_);
  }
  if (OB_SUCC(ret)) {
    job_type_ = ObRsJobTableOperator::get_job_type(job_type_str_);
    job_status_ = ObRsJobTableOperator::get_job_status(job_status_str_);
  }
  return ret;
}

const char* const ObRsJobTableOperator::TABLE_NAME = "__all_rootservice_job";

static const char* job_type_str_array[JOB_TYPE_MAX] = {
  NULL,
  "ALTER_TENANT_LOCALITY",
  "ROLLBACK_ALTER_TENANT_LOCALITY", // deprecated in V4.2
  "MIGRATE_UNIT",
  "DELETE_SERVER",
  "SHRINK_RESOURCE_TENANT_UNIT_NUM", // deprecated in V4.2
  "RESTORE_TENANT",
  "UPGRADE_STORAGE_FORMAT_VERSION",
  "STOP_UPGRADE_STORAGE_FORMAT_VERSION",
  "CREATE_INNER_SCHEMA",
  "UPGRADE_POST_ACTION",
  "UPGRADE_SYSTEM_VARIABLE",
  "UPGRADE_SYSTEM_TABLE",
  "UPGRADE_BEGIN",
  "UPGRADE_VIRTUAL_SCHEMA",
  "UPGRADE_SYSTEM_PACKAGE",
  "UPGRADE_ALL_POST_ACTION",
  "UPGRADE_INSPECTION",
  "UPGRADE_END",
  "UPGRADE_ALL",
  "ALTER_RESOURCE_TENANT_UNIT_NUM",
  "ALTER_TENANT_PRIMARY_ZONE"
};

bool ObRsJobTableOperator::is_valid_job_type(const ObRsJobType &rs_job_type)
{
  return rs_job_type > ObRsJobType::JOB_TYPE_INVALID && rs_job_type < ObRsJobType::JOB_TYPE_MAX;
}

const char* ObRsJobTableOperator::get_job_type_str(ObRsJobType job_type)
{
  STATIC_ASSERT(ARRAYSIZEOF(job_type_str_array) == JOB_TYPE_MAX,
                "type string array size mismatch with enum ObRsJobType");

  const char* str = NULL;
  if (is_valid_job_type(job_type)) {
    str = job_type_str_array[job_type];
  }
  return str;
}

ObRsJobType ObRsJobTableOperator::get_job_type(const common::ObString &job_type_str)
{
  ObRsJobType ret_job_type = JOB_TYPE_INVALID;
  for (int i = 0; i < static_cast<int>(JOB_TYPE_MAX); ++i) {
    if (NULL != job_type_str_array[i]
        && 0 == job_type_str.case_compare(job_type_str_array[i])) {
      ret_job_type = static_cast<ObRsJobType>(i);
      break;
    }
  }
  return ret_job_type;
}

static const char* job_status_str_array[JOB_STATUS_MAX] = {
  NULL,
  "INPROGRESS",
  "SUCCESS",
  "FAILED",
  "SKIP_CHECKING_LS_STATUS",
};

ObRsJobStatus ObRsJobTableOperator::get_job_status(const common::ObString &job_status_str)
{
  ObRsJobStatus ret_job_status = JOB_STATUS_INVALID;
  for (int i = 0; i < static_cast<int>(JOB_STATUS_MAX); ++i) {
    if (NULL != job_status_str_array[i]
        && 0 == job_status_str.case_compare(job_status_str_array[i])) {
      ret_job_status = static_cast<ObRsJobStatus>(i);
      break;
    }
  }
  return ret_job_status;
}


ObRsJobTableOperator::ObRsJobTableOperator()
    :inited_(false),
     max_job_id_(-1),
     row_count_(-1),
     sql_client_(NULL),
     rs_addr_()
{}

int ObRsJobTableOperator::init(common::ObMySQLProxy *sql_client, const common::ObAddr &rs_addr)
{
  int ret = OB_SUCCESS;
  if (NULL == sql_client
      || !rs_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(sql_client), K(rs_addr));
  } else if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    sql_client_ = sql_client;
    rs_addr_ = rs_addr;
    inited_ = true;
    LOG_INFO("__all_rootservice_job table operator inited", K_(rs_addr));
  }
  return ret;
}

int ObRsJobTableOperator::create_job(ObRsJobType job_type, share::ObDMLSqlSplicer &dml, int64_t &job_id, common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  const char* job_type_str = NULL;
  if (!is_valid_job_type(job_type)
      || NULL == (job_type_str = get_job_type_str(job_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job type", K(ret), K(job_type), K(job_type_str));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(alloc_job_id(job_id))) {
    LOG_WARN("failed to alloc job id", K(ret), K(job_id));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    char ip_buf[common::MAX_IP_ADDR_LENGTH];
    (void)rs_addr_.ip_to_string(ip_buf, common::MAX_IP_ADDR_LENGTH);
    if (OB_FAIL(ret)) {
		} else if (OB_FAIL(dml.add_gmt_create(now))
               || OB_FAIL(dml.add_gmt_modified(now))) {
      LOG_WARN("failed to add gmt time", K(ret), K(now));
    } else if (OB_FAIL(dml.add_column("job_id", job_id))) {
      LOG_WARN("failed to add column", K(ret), K(job_id));
    } else if (OB_FAIL(dml.add_column("job_type", job_type_str))) {
      LOG_WARN("failed to add column", K(ret), K(job_type_str));
    } else if (OB_FAIL(dml.add_column("progress", 0))) {
      LOG_WARN("failed to add column", K(ret), K(job_type_str));
    } else if (OB_FAIL(dml.add_column("job_status", job_status_str_array[JOB_STATUS_INPROGRESS]))) {
      LOG_WARN("failed to add column", K(ret), K(job_type_str));
    } else if (OB_FAIL(dml.add_column("rs_svr_ip", ip_buf))
               || OB_FAIL(dml.add_column("rs_svr_port", rs_addr_.get_port()))) {
      LOG_WARN("failed to add column", K(ret));
    }

    if (OB_SUCC(ret)) {
      common::ObSqlString sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(dml.splice_insert_sql(TABLE_NAME, sql))) {
        LOG_WARN("splice_insert_sql failed", K(ret));
      } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(sql), K(ret));
      } else if (!is_single_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert succeeded but affected_rows is not one", K(ret), K(affected_rows));
      } else {
        LOG_INFO("rootservice job started", K(job_id), "job_info", sql.ptr(), K(common::lbt()));
        (void)ATOMIC_AAF(&row_count_, 1);
      }
    }
  }
  if (OB_SUCC(ret) && job_id < 1) {
    ret = OB_SQL_OPT_ERROR;
    LOG_WARN("insert into all_rootservice_job failed", KR(ret), K(job_id));
  }
  if (OB_FAIL(ret)) {
    job_id = -1;
  }
  return ret;
}

int ObRsJobTableOperator::get_job(int64_t job_id, ObRsJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job id", K(ret), K(job_id));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSqlString sql;
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE job_id = %ld", TABLE_NAME, job_id))) {
        LOG_WARN("failed to assign sql", K(ret));
      } else if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("empty result set", K(ret));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(cons_job_info(*result, job_info))) {
        LOG_WARN("failed to construct job info", K(ret), K(job_id));
      } else {
      }
    }
  }
  return ret;
}

int ObRsJobTableOperator::cons_job_info(const sqlclient::ObMySQLResult &res, ObRsJobInfo &job_info)
{
  int ret = OB_SUCCESS;
  ////////////////
  // required fields:
  ////////////////
  EXTRACT_INT_FIELD_MYSQL(res, "job_id", job_info.job_id_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL(res, "job_type", job_info.job_type_str_);
  EXTRACT_VARCHAR_FIELD_MYSQL(res, "job_status", job_info.job_status_str_);
  EXTRACT_INT_FIELD_MYSQL(res, "progress", job_info.progress_, int64_t);
  // @FIXME
  job_info.gmt_create_ = 0;
  job_info.gmt_modified_ = 0;
  //EXTRACT_DATETIME_FIELD_MYSQL(res, "gmt_create", job_info.gmt_create_);
  //EXTRACT_DATETIME_FIELD_MYSQL(res, "gmt_modified", job_info.gmt_modified_);
  char svr_ip[OB_IP_STR_BUFF] = "";
  int64_t svr_port = 0;
  int64_t tmp_real_str_len = 0;
  UNUSED(tmp_real_str_len);
  EXTRACT_STRBUF_FIELD_MYSQL(res, "rs_svr_ip", svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
  EXTRACT_INT_FIELD_MYSQL(res, "rs_svr_port", svr_port, int64_t);
  (void)job_info.rs_addr_.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port));

  ////////////////
  // optional fields:
  ////////////////
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(res, "result_code", job_info.result_code_, int64_t);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(res, "tenant_id", job_info.tenant_id_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(res, "tenant_name", job_info.tenant_name_);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(res, "database_id", job_info.database_id_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(res, "database_name", job_info.database_name_);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(res, "table_id", job_info.table_id_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(res, "table_name", job_info.table_name_);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(res, "partition_id", job_info.partition_id_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL_SKIP_RET(res, "svr_ip", svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(res, "svr_port", svr_port, int64_t);
  (void)job_info.svr_addr_.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port));
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(res, "unit_id", job_info.unit_id_, int64_t);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(res, "sql_text", job_info.sql_text_);
  EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(res, "extra_info", job_info.extra_info_);
  EXTRACT_INT_FIELD_MYSQL_SKIP_RET(res, "resource_pool_id", job_info.resource_pool_id_, int64_t);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(job_info.deep_copy_self())) {
      LOG_INFO("failed to deep copy job info itself", K(ret));
    }
  }
  return ret;
}

int ObRsJobTableOperator::find_job(
    const ObRsJobType job_type,
    share::ObDMLSqlSplicer &pairs,
    int64_t &job_id,
    common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  const char* job_type_str = NULL;
  job_id = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_valid_job_type(job_type)
      || NULL == (job_type_str = get_job_type_str(job_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job type", K(ret), K(job_type), K(job_type_str));
  } else {
    ObSqlString sql;
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT job_id FROM %s WHERE ", TABLE_NAME))) {
        LOG_WARN("failed to assign sql", K(ret));
      } else if (OB_FAIL(pairs.add_column("job_type", job_type_str))) {
        LOG_WARN("failed to add column", K(ret), K(job_type_str));
      } else if (OB_FAIL(pairs.add_column("job_status", "INPROGRESS"))) {
        LOG_WARN("failed to add column", K(ret));
      } else if (OB_FAIL(pairs.splice_predicates(sql))) {
        LOG_WARN("failed to splice predicates", K(ret), K(sql));
      } else if (OB_FAIL(sql.append(" ORDER BY job_id DESC LIMIT 1"))) {
        LOG_WARN("fail to append sql string", K(ret));
      } else if (OB_FAIL(trans.read(res, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("empty result set", K(ret));
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "job_id", job_id, int64_t);
      }
      if (OB_SUCC(ret) && job_id < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("find an invalid job", KR(ret), K(sql), K(job_id));
      }
    }
  }
  return ret;
}

int ObRsJobTableOperator::update_job(int64_t job_id, share::ObDMLSqlSplicer &dml, common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();

    if (OB_FAIL(dml.add_gmt_modified(now))) {
      LOG_WARN("failed to add gmt time", K(ret), K(now));
    } else if (OB_FAIL(dml.add_pk_column("job_id", job_id))) {
      LOG_WARN("failed to add column", K(ret), K(job_id));
    }

    if (OB_SUCC(ret)) {
      common::ObSqlString sql;
      int64_t affected_rows = 0;
      if (OB_FAIL(dml.splice_update_sql(TABLE_NAME, sql))) {
        LOG_WARN("splice_insert_sql failed", K(ret));
      } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
        LOG_WARN("execute sql failed", K(sql), K(ret));
      } else if (OB_LIKELY(is_single_row(affected_rows))) {
        // success
      } else if (OB_UNLIKELY(is_zero_row(affected_rows))) {
        ret = OB_EAGAIN;
        LOG_WARN("[RS_JOB NOTICE] the specified rs job might has been already completed due to a new job"
            "or deleted manually", KR(ret), K(affected_rows), K(sql));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update successfully but more than one row", KR(ret), K(affected_rows), K(sql));
      }
    }
  }
  return ret;
}

int ObRsJobTableOperator::update_job_progress(int64_t job_id, int64_t progress, common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer pairs;
  if (progress < 0 || progress > 100) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid progress value", K(ret), K(job_id), K(progress));
  } else if (OB_FAIL(pairs.add_column("progress", progress))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(update_job(job_id, pairs, trans))) {
    LOG_WARN("failed to update job", K(ret), K(job_id));
  }
  return ret;
}

int ObRsJobTableOperator::complete_job(int64_t job_id, int result_code, common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer pairs;
  if (OB_SUCCESS == result_code) {
    if (OB_FAIL(pairs.add_column("result_code", 0))) {
      LOG_WARN("failed to add column", K(ret));
    } else if (OB_FAIL(pairs.add_column("progress", 100))) {
      LOG_WARN("failed to add column", K(ret));
    } else if (OB_FAIL(pairs.add_column("job_status", job_status_str_array[JOB_STATUS_SUCCESS]))) {
      LOG_WARN("failed to add column", K(ret));
    }
  } else if (OB_SKIP_CHECKING_LS_STATUS == result_code) {
    if (OB_FAIL(pairs.add_column("result_code", result_code))) {
      LOG_WARN("failed to add column", K(ret));
    } else if (OB_FAIL(pairs.add_column("job_status", job_status_str_array[JOB_STATUS_SKIP_CHECKING_LS_STATUS]))) {
      LOG_WARN("failed to add column", K(ret));
    }
  } else {
    if (OB_FAIL(pairs.add_column("result_code", result_code))) {
      LOG_WARN("failed to add column", K(ret));
    } else if (OB_FAIL(pairs.add_column("job_status", job_status_str_array[JOB_STATUS_FAILED]))) {
      LOG_WARN("failed to add column", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if(OB_FAIL(pairs.get_extra_condition().assign_fmt("job_status='%s'",
        job_status_str_array[JOB_STATUS_INPROGRESS]))) {
      LOG_WARN("fail to assign extra condition", KR(ret));
    } else if (OB_FAIL(update_job(job_id, pairs, trans))) {
      LOG_WARN("failed to update job", K(ret), K(job_id));
    } else {
      LOG_INFO("rootservice job completed", K(job_id), K(result_code));
    }
  }
  return ret;
}

int ObRsJobTableOperator::load_max_job_id(int64_t &max_job_id, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  max_job_id = -1;
  row_count = -1;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObSqlString sql;
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT count(*) as COUNT, max(job_id) as MAX_JOB_ID FROM %s", TABLE_NAME))) {
        LOG_WARN("failed to assign sql", K(ret));
      } else if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("empty result set", K(ret));
        ret = OB_ERR_UNEXPECTED;
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "COUNT", row_count, int64_t);
        if (row_count == 0) {
          max_job_id = 0;
        } else {
          EXTRACT_INT_FIELD_MYSQL(*result, "MAX_JOB_ID", max_job_id, int64_t);
          if (OB_SUCC(ret) && (max_job_id < 0)) {
            max_job_id = 0; // max_job_id < 0 may occur when OceanBase is in upgrading
          }
        }
      }
    }
  }
  return ret;
}

int ObRsJobTableOperator::alloc_job_id(int64_t &job_id)
{
  int ret = OB_SUCCESS;
  if (ATOMIC_LOAD(&max_job_id_) < 0) {
    ObLatchWGuard guard(latch_, ObLatchIds::DEFAULT_MUTEX);
    if (max_job_id_ < 0) {
      int64_t max_job_id = 0;
      int64_t row_count = 0;
      if (OB_FAIL(load_max_job_id(max_job_id, row_count)) || max_job_id < 0) {
        LOG_WARN("failed to load max job id from the table", K(ret), K(max_job_id));
      } else {
        LOG_INFO("load the max job id", K(max_job_id));
        (void)ATOMIC_SET(&max_job_id_, max_job_id);
        job_id = ATOMIC_AAF(&max_job_id_, 1);
        (void)ATOMIC_SET(&row_count_, row_count);
      }
    } else {
      job_id = ATOMIC_AAF(&max_job_id_, 1);
    }
  } else {
    job_id = ATOMIC_AAF(&max_job_id_, 1);
  }
  return ret;
}

ObRsJobTableOperator &ObRsJobTableOperatorSingleton::get_instance()
{
  static ObRsJobTableOperator the_one;
  return the_one;
}
