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
#include "lib/mysqlclient/ob_mysql_transaction.h"//ObMySQLTrans
#include "src/share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"//ObDMLSqlSplicer
#include "share/ob_unit_table_operator.h"
#include "observer/ob_server_struct.h"
#include "share/ob_locality_parser.h"

using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
namespace share
{
static const char* BALANCE_JOB_STATUS_ARRAY[] =
{
  "DOING", "CANCELING", "COMPLETED", "CANCELED", "SUSPEND"
};
static const char *BALANCE_JOB_TYPE[] =
{
  "LS_BALANCE", "PARTITION_BALANCE", "TRANSFER_PARTITION"
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

int ObBalanceJobDesc::init(
    const uint64_t tenant_id,
    const ObBalanceJobID &job_id,
    const ObZoneUnitCntList &zone_list,
    const int64_t primary_zone_num,
    const int64_t ls_scale_out_factor,
    const bool enable_rebalance,
    const bool enable_transfer,
    const bool enable_gts_standalone)
{
  reset();
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!job_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job_id", KR(ret), K(job_id));
  } else if (OB_FAIL(init_without_job(
      tenant_id,
      zone_list,
      primary_zone_num,
      ls_scale_out_factor,
      enable_rebalance,
      enable_transfer,
      enable_gts_standalone))) {
    LOG_WARN("init_without_job failed", KR(ret), K(job_id), K(zone_list), K(primary_zone_num),
        K(ls_scale_out_factor), K(enable_rebalance), K(enable_transfer), K(enable_gts_standalone));
  } else {
    job_id_ = job_id;
  }
  return ret;
}

int ObBalanceJobDesc::init_without_job(
    const uint64_t tenant_id,
    const ObZoneUnitCntList &zone_list,
    const int64_t primary_zone_num,
    const int64_t ls_scale_out_factor,
    const bool enable_rebalance,
    const bool enable_transfer,
    const bool enable_gts_standalone)
{
  reset();
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || zone_list.empty()
      || primary_zone_num <= 0
      || ls_scale_out_factor < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id),
        K(primary_zone_num), K(ls_scale_out_factor), K(zone_list));
  } else if (OB_FAIL(zone_unit_num_list_.assign(zone_list))) {
    LOG_WARN("failed to assign", KR(ret), K(zone_list));
  } else {
    tenant_id_ = tenant_id;
    primary_zone_num_ = primary_zone_num;
    ls_scale_out_factor_ = ls_scale_out_factor;
    enable_rebalance_ = enable_rebalance;
    enable_transfer_ = enable_transfer;
    enable_gts_standalone_ = enable_gts_standalone;
  }
  return ret;
}

int ObBalanceJobDesc::get_unit_lcm_count(int64_t &lcm_count) const
{
  int ret = OB_SUCCESS;
  lcm_count = 1;
  if (OB_UNLIKELY(0 >= zone_unit_num_list_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone_unit_num_list_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_unit_num_list_.count(); ++i) {
      const ObDisplayZoneUnitCnt &zone_unit = zone_unit_num_list_.at(i);
      const ObReplicaType &replica_type = zone_unit.get_replica_type();
      int64_t unit_num = zone_unit.get_unit_cnt();
      if (!ObReplicaTypeCheck::need_to_align_to_ug(replica_type)) {
        // skip this zone
      } else {
        if (enable_gts_standalone_ && ObReplicaTypeCheck::gts_standalone_applicable(replica_type)) {
          unit_num--;
        }
        if (unit_num <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit num is less than 0", KR(ret), K(tenant_id_), K(zone_unit), K(unit_num),
              K(replica_type), K(enable_gts_standalone_));
        } else {
          lcm_count = lcm(lcm_count, unit_num);
        }
      }
    }
  }
  return ret;
}

int ObBalanceJobDesc::assign(const ObBalanceJobDesc &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(zone_unit_num_list_.assign(other.zone_unit_num_list_))) {
      LOG_WARN("failed to assign", KR(ret), K(other));
    } else {
      tenant_id_ = other.tenant_id_;
      job_id_ = other.job_id_;
      primary_zone_num_ = other.primary_zone_num_;
      ls_scale_out_factor_ = other.ls_scale_out_factor_;
      enable_rebalance_ = other.enable_rebalance_;
      enable_transfer_ = other.enable_transfer_;
      enable_gts_standalone_ = other.enable_gts_standalone_;
    }
  }
  return ret;
}

int ObBalanceJobDesc::get_zone_unit_num(const ObZone &zone, int64_t &unit_num) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone), "job_desc", *this);
  } else {
    bool found = false;
    FOREACH_CNT_X(zone_unit, zone_unit_num_list_, OB_SUCC(ret) && !found) {
      if (OB_ISNULL(zone_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone unit is null", KR(ret), K(zone_unit_num_list_));
      } else if (zone_unit->get_zone() == zone) {
        unit_num = zone_unit->get_unit_cnt();
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("zone not exist in zone list", KR(ret), K(zone), K(zone_unit_num_list_));
    }
  }
  return ret;
}

int ObBalanceJobDesc::check_zone_in_locality(const ObZone &zone, bool &in_locality) const
{
  int ret = OB_SUCCESS;
  in_locality = false;
  int64_t unit_num = 0;
  if (OB_FAIL(get_zone_unit_num(zone, unit_num))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      in_locality = false;
    } else {
      LOG_WARN("failed to get zone unit num", KR(ret), K(zone));
    }
  } else {
    in_locality = true;
  }
  return ret;
}

#define BOOL_DIFF_STR(variable_str, self_bool, other_bool)                                          \
  do {                                                                                              \
    is_same = false;                                                                                \
    const char *self_value_str = self_bool ? "true" : "false";                                      \
    const char *other_value_str = other_bool ? "true" : "false";                                    \
    if (OB_FAIL(diff_str.assign_fmt("%s changing from %s to %s",                                    \
        variable_str, self_value_str, other_value_str))) {                                          \
      LOG_WARN("failed to assign fmt", KR(ret), K(variable_str), K(self_bool), K(other_bool));      \
    }                                                                                               \
  } while(0)

int ObBalanceJobDesc::compare(
    const ObBalanceJobDesc &other,
    bool &is_same,
    ObSqlString &diff_str)
{
  int ret = OB_SUCCESS;
  is_same = true;
  diff_str.reset();
  if (OB_UNLIKELY(!is_valid() || !other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(other), KPC(this));
  } else if (OB_UNLIKELY(tenant_id_ != other.tenant_id_)) { // job_id_ may be invalid
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("different tenant jobs can not be compared", KR(ret), K(other), KPC(this));
  } else if (enable_rebalance_ != other.enable_rebalance_) {
    BOOL_DIFF_STR("enable_rebalance", enable_rebalance_, other.enable_rebalance_);
  } else if (enable_transfer_ != other.enable_transfer_) {
    BOOL_DIFF_STR("enable_transfer", enable_transfer_, other.enable_transfer_);
  } else if (enable_gts_standalone_ != other.enable_gts_standalone_) {
    BOOL_DIFF_STR("enable_gts_standalone", enable_gts_standalone_, other.enable_gts_standalone_);
  } else if (primary_zone_num_ != other.primary_zone_num_) {
    is_same = false;
    if (OB_FAIL(diff_str.assign_fmt("primary_zone_num changing from %ld to %ld",
        primary_zone_num_, other.primary_zone_num_))) {
      LOG_WARN("assign_fmt failed", KR(ret), K(other), KPC(this));
    }
  } else if (ls_scale_out_factor_ != other.ls_scale_out_factor_) {
    is_same = false;
    if (OB_FAIL(diff_str.assign_fmt("ls_scale_out_factor changing from %ld to %ld",
        ls_scale_out_factor_, other.ls_scale_out_factor_))) {
      LOG_WARN("assign_fmt failed", KR(ret), K(other), KPC(this));
    }
  } else { // compare zone_unit_num_list
    ObZoneUnitCntList self_ordered_list(zone_unit_num_list_);
    ObZoneUnitCntList other_ordered_list(other.zone_unit_num_list_);
    lib::ob_sort(self_ordered_list.begin(), self_ordered_list.end());
    lib::ob_sort(other_ordered_list.begin(), other_ordered_list.end());
    if (self_ordered_list != other_ordered_list) {
      is_same = false;
      ObArenaAllocator allocator;
      ObString self_list_str;
      ObString other_list_str;
      if (OB_FAIL(zone_unit_num_list_.to_display_str(allocator, self_list_str))) {
        LOG_WARN("to display str failed", KR(ret), K(other), KPC(this));
      } else if (OB_FAIL(other_ordered_list.to_display_str(allocator, other_list_str))) {
        LOG_WARN("to display str failed", KR(ret), K(other), KPC(this));
      } else if (OB_FAIL(diff_str.assign_fmt("zone_unit_num_list changing from %s to %s",
          self_list_str.ptr(), other_list_str.ptr()))) {
        LOG_WARN("assign_fmt failed", KR(ret), K(self_list_str), K(other_list_str));
      }
    }
  }
  return ret;
}

int ObBalanceJob::init(const uint64_t tenant_id,
           const ObBalanceJobID job_id,
           const ObBalanceJobType job_type,
           const ObBalanceJobStatus job_status,
           const ObString &comment,
           const ObBalanceStrategy &balance_strategy,
           const int64_t max_end_time)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || ! job_id.is_valid()
                  || !job_type.is_valid() || !job_status.is_valid()
                  || !balance_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(job_id), K(job_type), K(job_status),
        K(balance_strategy), K(comment), K(max_end_time));
  } else if (OB_FAIL(comment_.assign(comment))) {
    LOG_WARN("failed to assign commet", KR(ret), K(comment));
  } else {
    tenant_id_ = tenant_id;
    job_id_ = job_id;
    job_type_ = job_type;
    job_status_ = job_status;
    max_end_time_ = max_end_time;
    balance_strategy_ = balance_strategy;
  }
  return ret;
}

// max_end_time can be OB_INVALID_TIMESTAMP
bool ObBalanceJob::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && job_id_.is_valid()
         && job_type_.is_valid()
         && job_status_.is_valid()
         && balance_strategy_.is_valid();
}

bool ObBalanceJob::is_timeout() const
{
  return OB_INVALID_TIMESTAMP < max_end_time_
      && max_end_time_ <= ObTimeUtility::current_time();
}

void ObBalanceJob::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  job_id_.reset();
  job_type_.reset();
  job_status_.reset();
  comment_.reset();
  balance_strategy_.reset();
  max_end_time_ = OB_INVALID_TIMESTAMP;
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

int ObBalanceJobTableOperator::construct_get_balance_job_sql_(
    const uint64_t tenant_id,
    const bool for_update,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "select time_to_usec(gmt_create) as start_time, time_to_usec(gmt_modified) as finish_time, *"))) {
    LOG_WARN("assign fmt failed", KR(ret), K(tenant_id), K(for_update));
  } else { // for max_end_time
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
    } else if (data_version < DATA_VERSION_4_2_4_0) {
      // skip
    } else if (OB_FAIL(sql.append_fmt(", time_to_usec(max_end_time) as max_end_time_int64"))) {
      LOG_WARN("append fmt failed", KR(ret), K(tenant_id), K(for_update));
    }
  }

  if (FAILEDx(sql.append_fmt(" from %s", OB_ALL_BALANCE_JOB_TNAME))) {
    LOG_WARN("append fmt failed", KR(ret), K(tenant_id), K(for_update), K(sql));
  } else if (!for_update) {
    // skip
  } else if (OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("append fmt failed", KR(ret), K(tenant_id), K(for_update), K(sql));
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
  } else if (OB_FAIL(construct_get_balance_job_sql_(tenant_id, for_update, sql))) {
    LOG_WARN("construct get balance job sql failed", KR(ret), K(tenant_id), K(for_update));
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
        int64_t job_id = ObBalanceJobID::INVALID_ID;
        int64_t max_end_time = OB_INVALID_TIMESTAMP;
        ObString comment;
        ObString balance_strategy_str;
        ObString job_type;
        ObString job_status;
        ObBalanceStrategy balance_strategy;
        EXTRACT_INT_FIELD_MYSQL(*result, "start_time", start_time, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "finish_time", finish_time, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "job_id", job_id, int64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "job_type", job_type);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "status", job_status);
        EXTRACT_VARCHAR_FIELD_MYSQL_SKIP_RET(*result, "comment", comment);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "balance_strategy_name", balance_strategy_str);
        EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "max_end_time_int64", max_end_time,
            int64_t, true/*skip_null_err*/, true/*skip_column_err*/, OB_INVALID_TIMESTAMP);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get cell", KR(ret),  K(job_id), K(job_type),
              K(job_status), K(comment), K(start_time), K(finish_time));
        } else if (OB_FAIL(balance_strategy.parse_from_str(balance_strategy_str))) {
          LOG_WARN("parse from str failed", KR(ret), K(balance_strategy_str));
        } else if (OB_FAIL(job.init(tenant_id, ObBalanceJobID(job_id), ObBalanceJobType(job_type),
            ObBalanceJobStatus(job_status), comment, balance_strategy, max_end_time))) {
          LOG_WARN("failed to init job", KR(ret), K(tenant_id), K(job_id), K(start_time),
              K(job_type), K(job_status), K(comment), K(balance_strategy), K(max_end_time));
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

int ObBalanceJobTableOperator::fill_dml_spliter(
    share::ObDMLSqlSplicer &dml,
    const ObBalanceJob &job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_column("balance_strategy_name", job.get_balance_strategy().str()))
      || OB_FAIL(dml.add_column("job_id", job.get_job_id().id()))
      || OB_FAIL(dml.add_column("job_type", job.get_job_type().to_str()))
      || OB_FAIL(dml.add_column("status", job.get_job_status().to_str()))
      || OB_FAIL(dml.add_column("target_primary_zone_num", OB_INVALID_COUNT))
      || OB_FAIL(dml.add_column("target_unit_num", OB_INVALID_COUNT))
      || OB_FAIL(dml.add_column("comment", job.get_comment().string()))) {
    LOG_WARN("failed to fill dml spliter", KR(ret), K(job));
  } else {
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(job.get_tenant_id(), data_version))) {
      LOG_WARN("get min data_version failed", KR(ret), K(job), K(data_version));
    } else if (data_version < DATA_VERSION_4_2_4_0) {
      // skip
    } else if (OB_INVALID_TIMESTAMP != job.get_max_end_time()
        && OB_FAIL(dml.add_time_column("max_end_time", job.get_max_end_time()))) {
      LOG_WARN("add max_end_time failed", KR(ret), K(job));
    }
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

int ObBalanceJobTableOperator::update_job_balance_strategy(
    const uint64_t tenant_id,
    const ObBalanceJobID job_id,
    const ObBalanceJobStatus job_status,
    const ObBalanceStrategy &old_strategy,
    const ObBalanceStrategy &new_strategy,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !job_id.is_valid()
      || !job_status.is_valid()
      || !old_strategy.is_valid()
      || !new_strategy.is_valid()
      || new_strategy == old_strategy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(job_id),
        K(job_status), K(old_strategy), K(new_strategy));
  } else if (OB_FAIL(sql.assign_fmt(
      "update %s set balance_strategy_name = '%s' "
      "where job_id = %ld and status = '%s' and balance_strategy_name = '%s'",
      OB_ALL_BALANCE_JOB_TNAME,
      new_strategy.str(),
      job_id.id(),
      job_status.to_str(),
      old_strategy.str()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id),
        K(job_id), K(job_status), K(old_strategy), K(new_strategy));
  } else if (OB_FAIL(client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", KR(ret), K(tenant_id), K(sql));
  } else if (is_zero_row(affected_rows)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("update nothing, status or strategy may be changed", KR(ret), K(sql), K(affected_rows));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect single row", KR(ret), K(sql), K(affected_rows));
  }
  LOG_INFO("update job strategy", KR(ret), K(sql));
  return ret;
}

int ObBalanceJobDescOperator::insert_balance_job_desc(
    const uint64_t tenant_id,
    const ObBalanceJobID &job_id,
    const ObBalanceJobDesc &job_desc,
    ObISQLClient &client)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !job_id.is_valid() || !job_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(job_id), K(job_desc));
  } else {
    uint64_t exec_tenant_id = get_private_table_exec_tenant_id(tenant_id);
    int64_t affected_rows = 0;
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(client, exec_tenant_id);
    ObArenaAllocator allocator;
    ObString zone_unit_num_list_str;
    ObString parameter_list_str;
    if (OB_FAIL(job_desc.get_zone_unit_num_list().to_display_str(allocator, zone_unit_num_list_str))) {
      LOG_WARN("to display str failed", KR(ret), K(job_desc));
    } else if (OB_FAIL(construct_parameter_list_str_(job_desc, allocator, parameter_list_str))) {
      LOG_WARN("get construct_parameter_list_str failed", KR(ret), K(job_desc));
    } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
        || OB_FAIL(dml.add_pk_column("job_id", job_id.id()))
        || OB_FAIL(dml.add_column("primary_zone_num", job_desc.get_primary_zone_num()))
        || OB_FAIL(dml.add_column("zone_unit_num_list", zone_unit_num_list_str))
        || OB_FAIL(dml.add_column("parameter_list", parameter_list_str))) {
      LOG_WARN("failed to fill dml spliter", KR(ret), K(job_desc));
    } else if (OB_FAIL(dml.finish_row())) {
      LOG_WARN("failed to finish row", KR(ret), K(job_desc));
    } else if (OB_FAIL(exec.exec_insert(OB_ALL_BALANCE_JOB_DESCRIPTION_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update failed", KR(ret), K(job_desc));
    } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expected single row", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObBalanceJobDescOperator::get_balance_job_desc(
    const uint64_t tenant_id,
    const ObBalanceJobID &job_id,
    ObISQLClient &client,
    ObBalanceJobDesc &job_desc)
{
  int ret = OB_SUCCESS;
  job_desc.reset();
  ObSqlString sql;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !job_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "select * from %s where job_id = %ld",
      OB_ALL_BALANCE_JOB_DESCRIPTION_TNAME,
      job_id.id()))) {
    LOG_WARN("assign fmt failed", KR(ret), K(tenant_id), K(job_id));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      uint64_t exec_tenant_id = get_private_table_exec_tenant_id(tenant_id);
      if (OB_FAIL(client.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("failed to read", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_INFO("empty balance job desc", KR(ret), K(exec_tenant_id), K(sql));
        } else {
          LOG_WARN("failed to get balance job", KR(ret), K(sql));
        }
      } else {
        uint64_t tenant_id = OB_INVALID_TENANT_ID;
        int64_t job_id = ObBalanceJobID::INVALID_ID;
        int64_t primary_zone_num = 0;
        int64_t ls_scale_out_factor = 0;
        ObString zone_unit_num_list_str;
        ObString parameter_list_str;
        ObZoneUnitCntList zone_unit_num_list;
        bool enable_rebalance = false;
        bool enable_transfer = false;
        bool enable_gts_standalone = false;
        EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "job_id", job_id, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "primary_zone_num", primary_zone_num, int64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "zone_unit_num_list", zone_unit_num_list_str);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "parameter_list", parameter_list_str);
        if (FAILEDx(zone_unit_num_list.parse_from_display_str(zone_unit_num_list_str))) {
          LOG_WARN("parse from display str failed", KR(ret), K(zone_unit_num_list_str));
        } else if (OB_FAIL(parse_parameter_from_str_(
            parameter_list_str,
            ls_scale_out_factor,
            enable_rebalance,
            enable_transfer,
            enable_gts_standalone))) {
          LOG_WARN("parse parameter_list from str failed", KR(ret), K(parameter_list_str));
        } else if (OB_FAIL(job_desc.init(
            tenant_id,
            ObBalanceJobID(job_id),
            zone_unit_num_list,
            primary_zone_num,
            ls_scale_out_factor,
            enable_rebalance,
            enable_transfer,
            enable_gts_standalone))) {
          LOG_WARN("init job_desc failed", KR(ret), K(job_id), K(primary_zone_num),
              K(ls_scale_out_factor), K(zone_unit_num_list), K(parameter_list_str),
              K(enable_rebalance), K(enable_transfer), K(enable_gts_standalone));
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

int ObBalanceJobDescOperator::construct_parameter_list_str_(
    const ObBalanceJobDesc &job_desc,
    ObIAllocator &allocator,
    ObString &parameter_list_str)
{
  int ret = OB_SUCCESS;
  parameter_list_str.reset();
  char *buf = NULL;
  int64_t pos = 0;
  const int64_t len = OB_TMP_BUF_SIZE_256;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "allocate memory failed", KR(ret), K(len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, len, pos,
      "ls_scale_out_factor:%ld,enable_rebalance:%s,enable_transfer:%s,enable_gts_standalone:%s",
      job_desc.get_ls_scale_out_factor(),
      job_desc.get_enable_rebalance() ? "true" : "false",
      job_desc.get_enable_transfer() ? "true" : "false",
      job_desc.get_enable_gts_standalone() ? "true" : "false"))) {
    LOG_WARN("databuff_printf failed", KR(ret), K(len), K(pos), K(buf), K(job_desc));
  } else {
    parameter_list_str.assign_ptr(buf, static_cast<ObString::obstr_size_t>(pos));
  }
  return ret;
}

int ObBalanceJobDescOperator::parse_parameter_from_str_(
    const ObString &parameter_list_str,
    int64_t &ls_scale_out_factor,
    bool &enable_rebalance,
    bool &enable_transfer,
    bool &enable_gts_standalone)
{
  int ret = OB_SUCCESS;
  errno = 0;
  ls_scale_out_factor = 0;
  char enable_rebalance_str[6] = {0};
  char enable_transfer_str[6] = {0};
  char enable_gts_standalone_str[6] = {0};
  if (OB_UNLIKELY(4 != sscanf(parameter_list_str.ptr(),
      "ls_scale_out_factor:%ld,enable_rebalance:%5[^,],enable_transfer:%5[^,],enable_gts_standalone:%5[^,]",
      &ls_scale_out_factor, enable_rebalance_str, enable_transfer_str, enable_gts_standalone_str))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameter_list_str", KR(ret), K(parameter_list_str), K(errno), KERRMSG);
  } else {
    enable_rebalance = !(0 == strcmp(enable_rebalance_str, "false"));
    enable_transfer = !(0 == strcmp(enable_transfer_str, "false"));
    enable_gts_standalone = !(0 == strcmp(enable_gts_standalone_str, "false"));
  }
  return ret;
}

int ObDisplayZoneUnitCnt::parse_from_display_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  reset();
  errno = 0;
  char zone_buf[MAX_ZONE_LENGTH + 1] = {0};
  char replica_type_str[MAX_REPLICA_TYPE_LENGTH + 1] = {0};
  if (OB_LIKELY(3 == sscanf(str.ptr(), "%128[^:]:%16[^:]:%ld", zone_buf, replica_type_str, &unit_cnt_))) { // MAX_ZONE_LENGTH = 128, MAX_REPLICA_TYPE_LENGTH = 16
    if (OB_FAIL(ObLocalityParser::parse_type(replica_type_str, strlen(replica_type_str), replica_type_))) {
      LOG_WARN("parse type failed", KR(ret), K(replica_type_str));
    }
  } else if (OB_LIKELY(2 == sscanf(str.ptr(), "%128[^:]:%ld", zone_buf, &unit_cnt_))) { // MAX_ZONE_LENGTH = 128
    // for compatibility, default to full replica type
    replica_type_ = common::ObReplicaType::REPLICA_TYPE_FULL;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid str", KR(ret), K(str), KPC(this), K(errno), KERRMSG);
  }
  if (FAILEDx(zone_.assign(zone_buf))) {
    LOG_WARN("assign failed", KR(ret), K(zone_buf), KPC(this));
  }
  return ret;
}

int ObDisplayZoneUnitCnt::to_display_str(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0 || pos < 0 || pos >= len || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(len), K(pos), KPC(this));
  } else {
    // It is ensured that tenant's unit replica_type can be NOT FULL
    //  only when tenant data_version is higher than 4.2.5.7
    // So it's safe to use new format if replica_type_ is not F.
    if (REPLICA_TYPE_FULL != replica_type_) {
      char replica_type_str[MAX_REPLICA_TYPE_LENGTH + 1] = {0};
      if (OB_FAIL(replica_type_to_string(replica_type_, replica_type_str, sizeof(replica_type_str)))) {
        LOG_WARN("replica_type_to_string failed", KR(ret), K(replica_type_));
      } else if (OB_FAIL(databuff_printf(buf, len, pos, "%s:%s:%ld", zone_.ptr(), replica_type_str, unit_cnt_))) {
        LOG_WARN("databuff_printf failed", KR(ret), K(len), K(pos), K(buf), KPC(this));
      }
    } else {
      // If replica_type_ is F, we use old format. replica_type can be parsed as F by default.
      if (OB_FAIL(databuff_printf(buf, len, pos, "%s:%ld", zone_.ptr(), unit_cnt_))) {
        LOG_WARN("databuff_printf failed", KR(ret), K(len), K(pos), K(buf), KPC(this));
      }
    }
  }
  return ret;
}

}//end of share
}//end of ob
