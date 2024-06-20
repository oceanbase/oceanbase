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

#define USING_LOG_PREFIX STORAGE

#include "share/schema/ob_mview_refresh_stats.h"
#include "observer/ob_server_struct.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;

/**
 * ObMViewRefreshRunStats
 */

ObMViewRefreshRunStats::ObMViewRefreshRunStats() { reset(); }

ObMViewRefreshRunStats::ObMViewRefreshRunStats(ObIAllocator *allocator) : ObSchema(allocator)
{
  reset();
}

ObMViewRefreshRunStats::ObMViewRefreshRunStats(const ObMViewRefreshRunStats &src_schema)
{
  reset();
  *this = src_schema;
}

ObMViewRefreshRunStats::~ObMViewRefreshRunStats() {}

ObMViewRefreshRunStats &ObMViewRefreshRunStats::operator=(const ObMViewRefreshRunStats &src_schema)
{
  if (this != &src_schema) {
    reset();
    int &ret = error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    refresh_id_ = src_schema.refresh_id_;
    run_user_id_ = src_schema.run_user_id_;
    num_mvs_total_ = src_schema.num_mvs_total_;
    num_mvs_current_ = src_schema.num_mvs_current_;
    push_deferred_rpc_ = src_schema.push_deferred_rpc_;
    refresh_after_errors_ = src_schema.refresh_after_errors_;
    purge_option_ = src_schema.purge_option_;
    parallelism_ = src_schema.parallelism_;
    heap_size_ = src_schema.heap_size_;
    atomic_refresh_ = src_schema.atomic_refresh_;
    nested_ = src_schema.nested_;
    out_of_place_ = src_schema.out_of_place_;
    number_of_failures_ = src_schema.number_of_failures_;
    start_time_ = src_schema.start_time_;
    end_time_ = src_schema.end_time_;
    elapsed_time_ = src_schema.elapsed_time_;
    log_purge_time_ = src_schema.log_purge_time_;
    complete_stats_avaliable_ = src_schema.complete_stats_avaliable_;
    if (OB_FAIL(deep_copy_str(src_schema.mviews_, mviews_))) {
      LOG_WARN("deep copy mviews failed", KR(ret), K(src_schema.mviews_));
    } else if (OB_FAIL(deep_copy_str(src_schema.base_tables_, base_tables_))) {
      LOG_WARN("deep copy base tables failed", KR(ret), K(src_schema.base_tables_));
    } else if (OB_FAIL(deep_copy_str(src_schema.method_, method_))) {
      LOG_WARN("deep copy method failed", KR(ret), K(src_schema.method_));
    } else if (OB_FAIL(deep_copy_str(src_schema.rollback_seg_, rollback_seg_))) {
      LOG_WARN("deep copy rollback seg failed", KR(ret), K(src_schema.rollback_seg_));
    } else if (OB_FAIL(deep_copy_str(src_schema.trace_id_, trace_id_))) {
      LOG_WARN("deep copy trace id failed", KR(ret), K(src_schema.trace_id_));
    }
  }
  return *this;
}

int ObMViewRefreshRunStats::assign(const ObMViewRefreshRunStats &other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

bool ObMViewRefreshRunStats::is_valid() const
{
  bool bret = false;
  if (OB_LIKELY(ObSchema::is_valid())) {
    bret = OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_ID != refresh_id_ &&
           OB_INVALID_ID != run_user_id_ && !mviews_.empty();
  }
  return bret;
}

void ObMViewRefreshRunStats::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  refresh_id_ = OB_INVALID_ID;
  run_user_id_ = OB_INVALID_ID;
  num_mvs_total_ = 0;
  num_mvs_current_ = 0;
  reset_string(mviews_);
  reset_string(base_tables_);
  reset_string(method_);
  reset_string(rollback_seg_);
  push_deferred_rpc_ = false;
  refresh_after_errors_ = false;
  purge_option_ = 0;
  parallelism_ = 0;
  heap_size_ = 0;
  atomic_refresh_ = false;
  nested_ = false;
  out_of_place_ = false;
  number_of_failures_ = 0;
  start_time_ = OB_INVALID_TIMESTAMP;
  end_time_ = OB_INVALID_TIMESTAMP;
  elapsed_time_ = 0;
  log_purge_time_ = 0;
  complete_stats_avaliable_ = false;
  reset_string(trace_id_);
  ObSchema::reset();
}

int64_t ObMViewRefreshRunStats::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObMViewRefreshRunStats));
  len += mviews_.length() + 1;
  len += base_tables_.length() + 1;
  len += method_.length() + 1;
  len += rollback_seg_.length() + 1;
  len += trace_id_.length() + 1;
  return len;
}

OB_SERIALIZE_MEMBER(ObMViewRefreshRunStats,
                    tenant_id_,
                    refresh_id_,
                    run_user_id_,
                    num_mvs_total_,
                    num_mvs_current_,
                    mviews_,
                    base_tables_,
                    method_,
                    rollback_seg_,
                    push_deferred_rpc_,
                    refresh_after_errors_,
                    purge_option_,
                    parallelism_,
                    heap_size_,
                    atomic_refresh_,
                    nested_,
                    out_of_place_,
                    number_of_failures_,
                    start_time_,
                    end_time_,
                    elapsed_time_,
                    log_purge_time_,
                    complete_stats_avaliable_,
                    trace_id_);

int ObMViewRefreshRunStats::gen_insert_run_stats_dml(uint64_t exec_tenant_id,
                                                     ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(exec_tenant_id), KPC(this));
  } else {
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("refresh_id", refresh_id_)) ||
        OB_FAIL(dml.add_column("run_user_id", run_user_id_)) ||
        OB_FAIL(dml.add_column("num_mvs_total", num_mvs_total_)) ||
        OB_FAIL(dml.add_column("num_mvs_current", num_mvs_current_)) ||
        OB_FAIL(dml.add_column("mviews", ObHexEscapeSqlStr(mviews_))) ||
        (!base_tables_.empty() &&
         OB_FAIL(dml.add_column("base_tables", ObHexEscapeSqlStr(base_tables_)))) ||
        (!method_.empty() && OB_FAIL(dml.add_column("method", ObHexEscapeSqlStr(method_)))) ||
        (!rollback_seg_.empty() &&
         OB_FAIL(dml.add_column("rollback_seg", ObHexEscapeSqlStr(rollback_seg_)))) ||
        OB_FAIL(dml.add_column("push_deferred_rpc", push_deferred_rpc_)) ||
        OB_FAIL(dml.add_column("refresh_after_errors", refresh_after_errors_)) ||
        OB_FAIL(dml.add_column("purge_option", purge_option_)) ||
        OB_FAIL(dml.add_column("parallelism", parallelism_)) ||
        OB_FAIL(dml.add_column("heap_size", heap_size_)) ||
        OB_FAIL(dml.add_column("atomic_refresh", atomic_refresh_)) ||
        OB_FAIL(dml.add_column("nested", nested_)) ||
        OB_FAIL(dml.add_column("out_of_place", out_of_place_)) ||
        OB_FAIL(dml.add_column("number_of_failures", number_of_failures_)) ||
        OB_FAIL(dml.add_time_column("start_time", start_time_)) ||
        OB_FAIL(dml.add_time_column("end_time", end_time_)) ||
        OB_FAIL(dml.add_column("elapsed_time", elapsed_time_)) ||
        OB_FAIL(dml.add_column("log_purge_time", log_purge_time_)) ||
        OB_FAIL(dml.add_column("complete_stats_avaliable", complete_stats_avaliable_)) ||
        (!trace_id_.empty() && OB_FAIL(dml.add_column("trace_id", ObHexEscapeSqlStr(trace_id_))))) {
      LOG_WARN("add column failed", KR(ret));
    }
  }
  return ret;
}

int ObMViewRefreshRunStats::insert_run_stats(ObISQLClient &sql_client,
                                             const ObMViewRefreshRunStats &run_stats)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = run_stats.get_tenant_id();
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, insert run stats is");
  } else if (OB_UNLIKELY(!run_stats.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(run_stats));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(run_stats.gen_insert_run_stats_dml(exec_tenant_id, dml))) {
      LOG_WARN("fail to gen insert run stats dml", KR(ret), K(run_stats));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert(OB_ALL_MVIEW_REFRESH_RUN_STATS_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", KR(ret));
      } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObMViewRefreshRunStats::dec_num_mvs_current(ObISQLClient &sql_client, uint64_t tenant_id,
                                                int64_t refresh_id, int64_t dec_val)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, dec num mvs current is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || refresh_id <= 0 || dec_val <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(refresh_id), K(dec_val));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(sql.assign_fmt("update %s set num_mvs_current = num_mvs_current - %ld"
                               " where tenant_id = 0 and refresh_id = %ld;",
                               OB_ALL_MVIEW_REFRESH_RUN_STATS_TNAME, dec_val, refresh_id))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(exec_tenant_id), K(sql));
    } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObMViewRefreshRunStats::drop_all_run_stats(ObISQLClient &sql_client, uint64_t tenant_id,
                                               int64_t &affected_rows, int64_t limit)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop all run stats is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("delete from %s where tenant_id = 0",
                               OB_ALL_MVIEW_REFRESH_RUN_STATS_TNAME))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (limit > 0 && OB_FAIL(sql.append_fmt(" limit %ld", limit))) {
      LOG_WARN("fail to append sql", KR(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(exec_tenant_id), K(sql));
    }
  }
  return ret;
}

int ObMViewRefreshRunStats::drop_empty_run_stats(ObISQLClient &sql_client, uint64_t tenant_id,
                                                 int64_t &affected_rows, int64_t limit)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop empty run stats is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("delete from %s where tenant_id = 0 and num_mvs_current <= 0",
                               OB_ALL_MVIEW_REFRESH_RUN_STATS_TNAME))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (limit > 0 && OB_FAIL(sql.append_fmt(" limit %ld", limit))) {
      LOG_WARN("fail to append sql", KR(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(exec_tenant_id), K(sql));
    }
  }
  return ret;
}

/**
 * ObMViewRefreshStats
 */

ObMViewRefreshStats::ObMViewRefreshStats() { reset(); }

ObMViewRefreshStats::ObMViewRefreshStats(ObIAllocator *allocator) : ObSchema(allocator) { reset(); }

ObMViewRefreshStats::ObMViewRefreshStats(const ObMViewRefreshStats &src_schema)
{
  reset();
  *this = src_schema;
}

ObMViewRefreshStats::~ObMViewRefreshStats() {}

ObMViewRefreshStats &ObMViewRefreshStats::operator=(const ObMViewRefreshStats &src_schema)
{
  if (this != &src_schema) {
    reset();
    int &ret = error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    refresh_id_ = src_schema.refresh_id_;
    mview_id_ = src_schema.mview_id_;
    retry_id_ = src_schema.retry_id_;
    refresh_type_ = src_schema.refresh_type_;
    start_time_ = src_schema.start_time_;
    end_time_ = src_schema.end_time_;
    elapsed_time_ = src_schema.elapsed_time_;
    log_purge_time_ = src_schema.log_purge_time_;
    initial_num_rows_ = src_schema.initial_num_rows_;
    final_num_rows_ = src_schema.final_num_rows_;
    num_steps_ = src_schema.num_steps_;
    result_ = src_schema.result_;
  }
  return *this;
}

int ObMViewRefreshStats::assign(const ObMViewRefreshStats &other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

bool ObMViewRefreshStats::is_valid() const
{
  bool bret = false;
  if (OB_LIKELY(ObSchema::is_valid())) {
    bret = OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_ID != refresh_id_ &&
           OB_INVALID_ID != mview_id_ && OB_INVALID_ID != retry_id_;
  }
  return bret;
}

void ObMViewRefreshStats::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  refresh_id_ = OB_INVALID_ID;
  mview_id_ = OB_INVALID_ID;
  retry_id_ = OB_INVALID_ID;
  refresh_type_ = share::schema::ObMVRefreshType::MAX;
  start_time_ = OB_INVALID_TIMESTAMP;
  end_time_ = OB_INVALID_TIMESTAMP;
  elapsed_time_ = 0;
  log_purge_time_ = 0;
  initial_num_rows_ = 0;
  final_num_rows_ = 0;
  num_steps_ = 0;
  result_ = UNEXECUTED_STATUS;
  ObSchema::reset();
}

int64_t ObMViewRefreshStats::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObMViewRefreshStats));
  return len;
}

OB_SERIALIZE_MEMBER(ObMViewRefreshStats,
                    tenant_id_,
                    refresh_id_,
                    mview_id_,
                    retry_id_,
                    refresh_type_,
                    start_time_,
                    end_time_,
                    elapsed_time_,
                    log_purge_time_,
                    initial_num_rows_,
                    final_num_rows_,
                    num_steps_,
                    result_);

int ObMViewRefreshStats::gen_insert_refresh_stats_dml(uint64_t exec_tenant_id,
                                                      ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(exec_tenant_id), KPC(this));
  } else {
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("refresh_id", refresh_id_)) ||
        OB_FAIL(dml.add_pk_column("mview_id", mview_id_)) ||
        OB_FAIL(dml.add_pk_column("retry_id", retry_id_)) ||
        OB_FAIL(dml.add_column("refresh_type", refresh_type_)) ||
        OB_FAIL(dml.add_time_column("start_time", start_time_)) ||
        OB_FAIL(dml.add_time_column("end_time", end_time_)) ||
        OB_FAIL(dml.add_column("elapsed_time", elapsed_time_)) ||
        OB_FAIL(dml.add_column("log_purge_time", log_purge_time_)) ||
        OB_FAIL(dml.add_column("initial_num_rows", initial_num_rows_)) ||
        OB_FAIL(dml.add_column("final_num_rows", final_num_rows_)) ||
        OB_FAIL(dml.add_column("num_steps", num_steps_)) ||
        OB_FAIL(dml.add_column("result", result_))) {
      LOG_WARN("add column failed", KR(ret));
    }
  }
  return ret;
}

int ObMViewRefreshStats::insert_refresh_stats(ObISQLClient &sql_client,
                                              const ObMViewRefreshStats &refresh_stats)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = refresh_stats.get_tenant_id();
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, insert refresh stats is");
  } else if (OB_UNLIKELY(!refresh_stats.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(refresh_stats));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(refresh_stats.gen_insert_refresh_stats_dml(exec_tenant_id, dml))) {
      LOG_WARN("fail to gen insert refresh stats dml", KR(ret), K(exec_tenant_id),
               K(refresh_stats));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert(OB_ALL_MVIEW_REFRESH_STATS_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", KR(ret));
      } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObMViewRefreshStats::drop_all_refresh_stats(ObISQLClient &sql_client, uint64_t tenant_id,
                                                int64_t &affected_rows, int64_t limit)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop all refresh stats is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObSqlString sql;
    if (OB_FAIL(
          sql.assign_fmt("delete from %s where tenant_id = 0", OB_ALL_MVIEW_REFRESH_STATS_TNAME))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (limit > 0 && OB_FAIL(sql.append_fmt(" limit %ld", limit))) {
      LOG_WARN("fail to append sql", KR(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(exec_tenant_id), K(sql));
    }
  }
  return ret;
}

int ObMViewRefreshStats::drop_refresh_stats_record(ObISQLClient &sql_client, uint64_t tenant_id,
                                                   const ObMViewRefreshStatsRecordId &record_id)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop refresh stats record is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !record_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(record_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("refresh_id", record_id.refresh_id_)) ||
        OB_FAIL(dml.add_pk_column("mview_id", record_id.mview_id_)) ||
        OB_FAIL(dml.add_pk_column("retry_id", record_id.retry_id_))) {
      LOG_WARN("add column failed", KR(ret));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_delete(OB_ALL_MVIEW_REFRESH_STATS_TNAME, dml, affected_rows))) {
        LOG_WARN("execute delete failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObMViewRefreshStats::collect_record_ids(ObISQLClient &sql_client, uint64_t tenant_id,
                                            const FilterParam &filter_param,
                                            ObIArray<ObMViewRefreshStatsRecordId> &record_ids,
                                            int64_t limit)
{
  int ret = OB_SUCCESS;
  record_ids.reset();
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, collect record ids is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !filter_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(filter_param));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObMySQLResult *result = nullptr;
      ObSqlString sql;
      ObMViewRefreshStatsRecordId record_id;
      if (OB_FAIL(
            sql.assign_fmt("select refresh_id, mview_id, retry_id from %s where tenant_id = 0",
                           OB_ALL_MVIEW_REFRESH_STATS_TNAME))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (filter_param.has_mview_id() &&
                 OB_FAIL(sql.append_fmt(" and mview_id = %ld", filter_param.get_mview_id()))) {
        LOG_WARN("fail to append sql", KR(ret));
      } else if (filter_param.has_retention_period() &&
                 OB_FAIL(sql.append_fmt(" and end_time < date_sub(now(), interval %ld day)",
                                        filter_param.get_retention_period()))) {
        LOG_WARN("fail to append sql", KR(ret));
      } else if (limit > 0 && OB_FAIL(sql.append_fmt(" limit %ld", limit))) {
        LOG_WARN("fail to append sql", KR(ret));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret));
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next", KR(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        }
        EXTRACT_INT_FIELD_MYSQL(*result, "refresh_id", record_id.refresh_id_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "mview_id", record_id.mview_id_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "retry_id", record_id.retry_id_, int64_t);
        OZ(record_ids.push_back(record_id));
      }
    }
  }
  return ret;
}

/**
 * ObMViewRefreshChangeStats
 */

ObMViewRefreshChangeStats::ObMViewRefreshChangeStats() { reset(); }

ObMViewRefreshChangeStats::ObMViewRefreshChangeStats(ObIAllocator *allocator) : ObSchema(allocator)
{
  reset();
}

ObMViewRefreshChangeStats::ObMViewRefreshChangeStats(const ObMViewRefreshChangeStats &src_schema)
{
  reset();
  *this = src_schema;
}

ObMViewRefreshChangeStats::~ObMViewRefreshChangeStats() {}

ObMViewRefreshChangeStats &ObMViewRefreshChangeStats::operator=(
  const ObMViewRefreshChangeStats &src_schema)
{
  if (this != &src_schema) {
    reset();
    int &ret = error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    refresh_id_ = src_schema.refresh_id_;
    mview_id_ = src_schema.mview_id_;
    retry_id_ = src_schema.retry_id_;
    detail_table_id_ = src_schema.detail_table_id_;
    num_rows_ins_ = src_schema.num_rows_ins_;
    num_rows_upd_ = src_schema.num_rows_upd_;
    num_rows_del_ = src_schema.num_rows_del_;
    num_rows_ = src_schema.num_rows_;
  }
  return *this;
}

int ObMViewRefreshChangeStats::assign(const ObMViewRefreshChangeStats &other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

bool ObMViewRefreshChangeStats::is_valid() const
{
  bool bret = false;
  if (OB_LIKELY(ObSchema::is_valid())) {
    bret = OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_ID != refresh_id_ &&
           OB_INVALID_ID != mview_id_ && OB_INVALID_ID != retry_id_ &&
           OB_INVALID_ID != detail_table_id_;
  }
  return bret;
}

void ObMViewRefreshChangeStats::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  refresh_id_ = OB_INVALID_ID;
  mview_id_ = OB_INVALID_ID;
  retry_id_ = OB_INVALID_ID;
  detail_table_id_ = OB_INVALID_ID;
  num_rows_ins_ = 0;
  num_rows_upd_ = 0;
  num_rows_del_ = 0;
  num_rows_ = 0;
  ObSchema::reset();
}

int64_t ObMViewRefreshChangeStats::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObMViewRefreshChangeStats));
  return len;
}

OB_SERIALIZE_MEMBER(ObMViewRefreshChangeStats,
                    tenant_id_,
                    refresh_id_,
                    mview_id_,
                    retry_id_,
                    detail_table_id_,
                    num_rows_ins_,
                    num_rows_upd_,
                    num_rows_del_,
                    num_rows_);

int ObMViewRefreshChangeStats::gen_insert_change_stats_dml(uint64_t exec_tenant_id,
                                                           ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(exec_tenant_id), KPC(this));
  } else {
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("refresh_id", refresh_id_)) ||
        OB_FAIL(dml.add_pk_column("mview_id", mview_id_)) ||
        OB_FAIL(dml.add_pk_column("retry_id", retry_id_)) ||
        OB_FAIL(dml.add_pk_column("detail_table_id", detail_table_id_)) ||
        OB_FAIL(dml.add_column("num_rows_ins", num_rows_ins_)) ||
        OB_FAIL(dml.add_column("num_rows_upd", num_rows_upd_)) ||
        OB_FAIL(dml.add_column("num_rows_del", num_rows_del_)) ||
        OB_FAIL(dml.add_column("num_rows", num_rows_))) {
      LOG_WARN("add column failed", KR(ret));
    }
  }
  return ret;
}
int ObMViewRefreshChangeStats::insert_change_stats(ObISQLClient &sql_client,
                                                   const ObMViewRefreshChangeStats &change_stats)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = change_stats.get_tenant_id();
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, insert change stats is");
  } else if (OB_UNLIKELY(!change_stats.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(change_stats));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(change_stats.gen_insert_change_stats_dml(exec_tenant_id, dml))) {
      LOG_WARN("fail to gen insert change stats dml", KR(ret), K(exec_tenant_id), K(change_stats));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert(OB_ALL_MVIEW_REFRESH_CHANGE_STATS_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", KR(ret));
      } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObMViewRefreshChangeStats::drop_all_change_stats(ObISQLClient &sql_client, uint64_t tenant_id,
                                                     int64_t &affected_rows, int64_t limit)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop all change stats is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("delete from %s where tenant_id = 0",
                               OB_ALL_MVIEW_REFRESH_CHANGE_STATS_TNAME))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (limit > 0 && OB_FAIL(sql.append_fmt(" limit %ld", limit))) {
      LOG_WARN("fail to append sql", KR(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(exec_tenant_id), K(sql));
    }
  }
  return ret;
}

int ObMViewRefreshChangeStats::drop_change_stats_record(
  ObISQLClient &sql_client, uint64_t tenant_id, const ObMViewRefreshStatsRecordId &record_id)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop change stats record is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !record_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(record_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("refresh_id", record_id.refresh_id_)) ||
        OB_FAIL(dml.add_pk_column("mview_id", record_id.mview_id_)) ||
        OB_FAIL(dml.add_pk_column("retry_id", record_id.retry_id_))) {
      LOG_WARN("add column failed", KR(ret));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_delete(OB_ALL_MVIEW_REFRESH_CHANGE_STATS_TNAME, dml, affected_rows))) {
        LOG_WARN("execute delete failed", KR(ret));
      }
    }
  }
  return ret;
}

/**
 * ObMViewRefreshStmtStats
 */

ObMViewRefreshStmtStats::ObMViewRefreshStmtStats() { reset(); }

ObMViewRefreshStmtStats::ObMViewRefreshStmtStats(ObIAllocator *allocator) : ObSchema(allocator)
{
  reset();
}

ObMViewRefreshStmtStats::ObMViewRefreshStmtStats(const ObMViewRefreshStmtStats &src_schema)
{
  reset();
  *this = src_schema;
}

ObMViewRefreshStmtStats::~ObMViewRefreshStmtStats() {}

ObMViewRefreshStmtStats &ObMViewRefreshStmtStats::operator=(
  const ObMViewRefreshStmtStats &src_schema)
{
  if (this != &src_schema) {
    reset();
    int &ret = error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    refresh_id_ = src_schema.refresh_id_;
    mview_id_ = src_schema.mview_id_;
    retry_id_ = src_schema.retry_id_;
    step_ = src_schema.step_;
    execution_time_ = src_schema.execution_time_;
    result_ = src_schema.result_;
    if (OB_FAIL(deep_copy_str(src_schema.sql_id_, sql_id_))) {
      LOG_WARN("deep copy sql id failed", KR(ret), K(src_schema.sql_id_));
    } else if (OB_FAIL(deep_copy_str(src_schema.stmt_, stmt_))) {
      LOG_WARN("deep copy stmt failed", KR(ret), K(src_schema.stmt_));
    } else if (OB_FAIL(deep_copy_str(src_schema.execution_plan_, execution_plan_))) {
      LOG_WARN("deep copy execution plan failed", KR(ret), K(src_schema.execution_plan_));
    }
  }
  return *this;
}

int ObMViewRefreshStmtStats::assign(const ObMViewRefreshStmtStats &other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

bool ObMViewRefreshStmtStats::is_valid() const
{
  bool bret = false;
  if (OB_LIKELY(ObSchema::is_valid())) {
    bret = OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_ID != refresh_id_ &&
           OB_INVALID_ID != mview_id_ && OB_INVALID_ID != retry_id_ && step_ > 0 && !stmt_.empty();
  }
  return bret;
}

void ObMViewRefreshStmtStats::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  refresh_id_ = OB_INVALID_ID;
  mview_id_ = OB_INVALID_ID;
  retry_id_ = OB_INVALID_ID;
  step_ = 0;
  reset_string(sql_id_);
  reset_string(stmt_);
  execution_time_ = 0;
  reset_string(execution_plan_);
  result_ = UNEXECUTED_RESULT;
  ObSchema::reset();
}

int64_t ObMViewRefreshStmtStats::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObMViewRefreshStmtStats));
  len += sql_id_.length() + 1;
  len += stmt_.length() + 1;
  len += execution_plan_.length() + 1;
  return len;
}

OB_SERIALIZE_MEMBER(ObMViewRefreshStmtStats,
                    tenant_id_,
                    refresh_id_,
                    mview_id_,
                    retry_id_,
                    step_,
                    sql_id_,
                    stmt_,
                    execution_time_,
                    execution_plan_,
                    result_);

int ObMViewRefreshStmtStats::gen_insert_stmt_stats_dml(uint64_t exec_tenant_id,
                                                       ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == exec_tenant_id || !is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(exec_tenant_id), KPC(this));
  } else {
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("refresh_id", refresh_id_)) ||
        OB_FAIL(dml.add_pk_column("mview_id", mview_id_)) ||
        OB_FAIL(dml.add_pk_column("retry_id", retry_id_)) ||
        OB_FAIL(dml.add_pk_column("step", step_)) ||
        (!sql_id_.empty() && OB_FAIL(dml.add_column("sqlid", ObHexEscapeSqlStr(sql_id_)))) ||
        OB_FAIL(dml.add_column("stmt", ObHexEscapeSqlStr(stmt_))) ||
        OB_FAIL(dml.add_column("execution_time", execution_time_)) ||
        (!execution_plan_.empty() &&
         OB_FAIL(dml.add_column("execution_plan", ObHexEscapeSqlStr(execution_plan_)))) ||
        OB_FAIL(dml.add_pk_column("result", result_))) {
      LOG_WARN("add column failed", KR(ret));
    }
  }
  return ret;
}

int ObMViewRefreshStmtStats::insert_stmt_stats(ObISQLClient &sql_client,
                                               const ObMViewRefreshStmtStats &stmt_stats)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = stmt_stats.get_tenant_id();
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, insert stmt stats is");
  } else if (OB_UNLIKELY(!stmt_stats.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(stmt_stats));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(stmt_stats.gen_insert_stmt_stats_dml(exec_tenant_id, dml))) {
      LOG_WARN("fail to gen insert stmt stats dml", KR(ret), K(exec_tenant_id), K(stmt_stats));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_insert(OB_ALL_MVIEW_REFRESH_STMT_STATS_TNAME, dml, affected_rows))) {
        LOG_WARN("execute insert failed", KR(ret));
      } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObMViewRefreshStmtStats::drop_all_stmt_stats(ObISQLClient &sql_client, uint64_t tenant_id,
                                                 int64_t &affected_rows, int64_t limit)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop all stmt stats is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("delete from %s where tenant_id = 0",
                               OB_ALL_MVIEW_REFRESH_STMT_STATS_TNAME))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (limit > 0 && OB_FAIL(sql.append_fmt(" limit %ld", limit))) {
      LOG_WARN("fail to append sql", KR(ret));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(exec_tenant_id), K(sql));
    }
  }
  return ret;
}

int ObMViewRefreshStmtStats::drop_stmt_stats_record(ObISQLClient &sql_client, uint64_t tenant_id,
                                                    const ObMViewRefreshStatsRecordId &record_id)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop stmt stats record is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || !record_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(record_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("refresh_id", record_id.refresh_id_)) ||
        OB_FAIL(dml.add_pk_column("mview_id", record_id.mview_id_)) ||
        OB_FAIL(dml.add_pk_column("retry_id", record_id.retry_id_))) {
      LOG_WARN("add column failed", KR(ret));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_delete(OB_ALL_MVIEW_REFRESH_STMT_STATS_TNAME, dml, affected_rows))) {
        LOG_WARN("execute delete failed", KR(ret));
      }
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
