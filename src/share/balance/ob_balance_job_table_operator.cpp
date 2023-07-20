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

#define USING_LOG_PREFIX SHARE

#include "ob_balance_job_table_operator.h"
#include "lib/mysqlclient/ob_isql_client.h"//ObISQLClient
#include "lib/mysqlclient/ob_mysql_result.h"//MySQLResult
#include "lib/mysqlclient/ob_mysql_proxy.h"//MySQLResult
#include "lib/mysqlclient/ob_mysql_transaction.h"//ObMySQLTrans
#include "share/inner_table/ob_inner_table_schema.h"//ALL_BALANCE_JOB_TNAME
#include "share/ob_dml_sql_splicer.h"//ObDMLSqlSplicer

using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
namespace share
{
static const char* BALANCE_JOB_STATUS_ARRAY[] =
{
  "DOING", "CANCELING", "COMPLETED", "CANCELED"
};
static const char *BALANCE_JOB_TYPE[] =
{
  "LS_BALANCE", "PARTITION_BALANCE",
};

const char* ObBalanceJobStatus::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(BALANCE_JOB_STATUS_ARRAY) == BALANCE_JOB_STATUS_MAX, "array size mismatch");
  const char *type_str = "INVALID";
  if (OB_UNLIKELY(val_ >= ARRAYSIZEOF(BALANCE_JOB_STATUS_ARRAY) || val_ < 0)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "fatal error, unknown balance job status", K_(val));
  } else {
    type_str = BALANCE_JOB_STATUS_ARRAY[val_];
  }
  return type_str;
}

ObBalanceJobStatus::ObBalanceJobStatus(const ObString &str)
{
  val_ = BALANCE_JOB_STATUS_INVALID;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(BALANCE_JOB_STATUS_ARRAY); ++i) {
      if (0 == str.case_compare(BALANCE_JOB_STATUS_ARRAY[i])) {
        val_ = i;
        break;
      }
    }
  }
  if (BALANCE_JOB_STATUS_INVALID == val_) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid balance job status", K(val_), K(str));
  }
}

const char* ObBalanceJobType::to_str() const
{
  STATIC_ASSERT(
      ARRAYSIZEOF(BALANCE_JOB_TYPE) == BALANCE_JOB_MAX,
      "array size mismatch");
  const char *type_str = "INVALID";
  if (OB_UNLIKELY(val_ >= ARRAYSIZEOF(BALANCE_JOB_TYPE) || val_ < 0)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "fatal error, unknown balance job status", K_(val));
  } else {
    type_str = BALANCE_JOB_TYPE[val_];
  }
  return type_str;
}

ObBalanceJobType::ObBalanceJobType(const ObString &str)
{
  val_ = BALANCE_JOB_INVALID;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(BALANCE_JOB_TYPE); ++i) {
      if (0 == str.case_compare(BALANCE_JOB_TYPE[i])) {
        val_ = i;
        break;
      }
    }
  }
  if (BALANCE_JOB_INVALID == val_) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid balance job status", K(val_), K(str));
  }
}

int ObBalanceJob::init(const uint64_t tenant_id,
           const ObBalanceJobID job_id,
           const ObBalanceJobType job_type,
           const ObBalanceJobStatus job_status,
           const int64_t primary_zone_num,
           const int64_t unit_group_num,
           const ObString &comment,
           const ObString &balance_strategy)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || ! job_id.is_valid()
                  || !job_type.is_valid() || !job_status.is_valid()
                  || 0 == primary_zone_num || 0 == unit_group_num
                  || balance_strategy.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(job_id), K(job_type), K(job_status),
                                 K(primary_zone_num), K(unit_group_num), K(balance_strategy), K(comment));
  } else if (OB_FAIL(comment_.assign(comment))) {
    LOG_WARN("failed to assign commet", KR(ret), K(comment));
  } else if (OB_FAIL(balance_strategy_.assign(balance_strategy))) {
    LOG_WARN("failed to assign balance strategy", KR(ret), K(balance_strategy));
  } else {
    tenant_id_ = tenant_id;
    job_id_ = job_id;
    job_type_ = job_type;
    job_status_ = job_status;
    primary_zone_num_ = primary_zone_num;
    unit_group_num_ = unit_group_num;
  }
  return ret;
}

bool ObBalanceJob::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && job_id_.is_valid()
         && 0 != primary_zone_num_
         && 0 != unit_group_num_
         && job_type_.is_valid()
         && job_status_.is_valid()
         && !balance_strategy_.empty();
}

void ObBalanceJob::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  job_id_.reset();
  primary_zone_num_ = 0;
  unit_group_num_ = 0;
  job_type_.reset();
  job_status_.reset();
  comment_.reset();
  balance_strategy_.reset();
}

int ObBalanceJobTableOperator::insert_new_job(const ObBalanceJob &job,
                     ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(client, job.get_tenant_id());
  if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("job is invalid", KR(ret), K(job));
  } else if (OB_FAIL(fill_dml_spliter(dml, job))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job));
  } else if (OB_FAIL(dml.finish_row())) {
    LOG_WARN("failed to finish row", KR(ret), K(job));
  } else if (OB_FAIL(exec.exec_insert(OB_ALL_BALANCE_JOB_TNAME, dml, affected_rows))) {
    LOG_WARN("execute update failed", KR(ret), K(job));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected single row", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObBalanceJobTableOperator::get_balance_job(const uint64_t tenant_id,
                      const bool for_update,
                      ObISQLClient &client,
                      ObBalanceJob &job,
                      int64_t &start_time,
                      int64_t &finish_time)
{
  int ret = OB_SUCCESS;
  job.reset();
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("select time_to_usec(gmt_create) as "
                                    "start_time, time_to_usec(gmt_modified) "
                                    " as finish_time, * from %s",
                                    OB_ALL_BALANCE_JOB_TNAME))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_INFO("empty balance job", KR(ret), K(sql));
        } else {
          LOG_WARN("failed to get balance job", KR(ret), K(sql));
        }
      } else {
        int64_t primary_zone_num = 0;
        int64_t unit_num = 0;
        int64_t job_id = ObBalanceJobID::INVALID_ID;
        ObString comment;
        ObString balance_strategy;
        ObString job_type;
        ObString job_status;
        EXTRACT_INT_FIELD_MYSQL(*result, "start_time", start_time, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "finish_time", finish_time, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "job_id", job_id, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "target_unit_num", unit_num, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "target_primary_zone_num", primary_zone_num, int64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "job_type", job_type);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "status", job_status);
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "comment", comment);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "balance_strategy_name", balance_strategy);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get cell", KR(ret),  K(job_id), K(unit_num), K(primary_zone_num), K(job_type),
              K(job_status), K(comment), K(start_time), K(finish_time));
        } else if (OB_FAIL(job.init(tenant_id, ObBalanceJobID(job_id), ObBalanceJobType(job_type),
            ObBalanceJobStatus(job_status), primary_zone_num, unit_num, comment, balance_strategy))) {
          LOG_WARN("failed to init job", KR(ret), K(tenant_id), K(job_id), K(unit_num), K(start_time),
                      K(primary_zone_num), K(job_type), K(job_status), K(comment), K(balance_strategy));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_SUCC(result->next())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected only one row", KR(ret), K(sql));
        } else if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("error unexpected", KR(ret), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObBalanceJobTableOperator::update_job_status(const uint64_t tenant_id,
                               const ObBalanceJobID job_id,
                               const ObBalanceJobStatus old_job_status,
                               const ObBalanceJobStatus new_job_status,
                               bool update_comment, const common::ObString &new_comment,
                               ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !old_job_status.is_valid() || !new_job_status.is_valid()
                  || ! job_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(old_job_status), K(new_job_status), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt("update %s set status = '%s'",
                     OB_ALL_BALANCE_JOB_TNAME, new_job_status.to_str()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(new_job_status), K(old_job_status), K(job_id));
  } else if (update_comment && OB_FAIL(sql.append_fmt(", comment = '%.*s'", new_comment.length(), new_comment.ptr()))) {
    LOG_WARN("failed to append sql", KR(ret), K(new_comment));
  } else if (OB_FAIL(sql.append_fmt(" where status = '%s' and job_id = %ld", old_job_status.to_str(), job_id.id()))) {
    LOG_WARN("failed to append sql", KR(ret), K(old_job_status), K(job_id));
  } else if (OB_FAIL(client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", KR(ret), K(tenant_id), K(sql));
  } else if (is_zero_row(affected_rows)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("expected one row, may status change", KR(ret), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected single row", KR(ret), K(sql));
  }
  return ret;
}

int ObBalanceJobTableOperator::fill_dml_spliter(share::ObDMLSqlSplicer &dml,
                              const ObBalanceJob &job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_column("balance_strategy_name", job.get_balance_strategy()))
      || OB_FAIL(dml.add_column("job_id", job.get_job_id().id()))
      || OB_FAIL(dml.add_column("job_type", job.get_job_type().to_str()))
      || OB_FAIL(dml.add_column("status", job.get_job_status().to_str()))
      || OB_FAIL(dml.add_column("target_primary_zone_num", job.get_primary_zone_num()))
     || OB_FAIL(dml.add_column("target_unit_num", job.get_unit_group_num()))
      || OB_FAIL(dml.add_column("comment", job.get_comment().string()))) {
    LOG_WARN("failed to fill dml spliter", KR(ret), K(job));
  }
  return ret;
}

// maybe in trans TODO
// remove job from __all_balance_job to __all_balance_job_history
int ObBalanceJobTableOperator::clean_job(const uint64_t tenant_id,
                       const ObBalanceJobID job_id,
                       ObMySQLProxy &client)
{
  int ret = OB_SUCCESS;
  ObBalanceJob job;
  int64_t affected_rows = 0;
  common::ObMySQLTransaction trans;
  ObSqlString sql;
  int64_t finish_time = OB_INVALID_TIMESTAMP;
  int64_t start_time = OB_INVALID_TIMESTAMP;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || ! job_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(trans.start(&client, tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_balance_job(tenant_id, true, trans, job, start_time, finish_time))) {
    LOG_WARN("failed to get job", KR(ret), K(tenant_id));
  } else if (job_id != job.get_job_id()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("job not exist, no need clean", KR(ret), K(job_id), K(job));
  } else if (job.get_job_status().is_doing() || job.get_job_status().is_canceling()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not clean job while in progress", KR(ret), K(job));
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where job_id = %ld", OB_ALL_BALANCE_JOB_TNAME,
      job_id.id()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(job_id));
  } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write", KR(ret), K(tenant_id), K(sql));
  } else if(!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect one row", KR(ret), K(sql), K(affected_rows));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(trans, job.get_tenant_id());
    if (OB_FAIL(fill_dml_spliter(dml, job))) {
      LOG_WARN("failed to assign sql", KR(ret), K(job));
    } else if (OB_FAIL(dml.add_time_column("create_time", start_time))) {
      LOG_WARN("failed to add start time", KR(ret), K(start_time));
    } else if (OB_FAIL(dml.add_time_column("finish_time", finish_time))) {
      LOG_WARN("failed to add start time", KR(ret), K(job), K(finish_time));
    } else if (OB_FAIL(exec.exec_insert(OB_ALL_BALANCE_JOB_HISTORY_TNAME, dml,
                                        affected_rows))) {
      LOG_WARN("execute update failed", KR(ret), K(job));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect one row", KR(ret), K(sql), K(affected_rows));
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}
}//end of share
}//end of ob
