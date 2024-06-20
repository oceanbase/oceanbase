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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_mlog_info.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;

ObMLogInfo::ObMLogInfo() { reset(); }

ObMLogInfo::ObMLogInfo(ObIAllocator *allocator) : ObSchema(allocator) { reset(); }

ObMLogInfo::ObMLogInfo(const ObMLogInfo &src_schema)
{
  reset();
  *this = src_schema;
}

ObMLogInfo::~ObMLogInfo() {}

ObMLogInfo &ObMLogInfo::operator=(const ObMLogInfo &src_schema)
{
  if (this != &src_schema) {
    reset();
    int &ret = error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    mlog_id_ = src_schema.mlog_id_;
    purge_mode_ = src_schema.purge_mode_;
    purge_start_ = src_schema.purge_start_;
    last_purge_scn_ = src_schema.last_purge_scn_;
    last_purge_date_ = src_schema.last_purge_date_;
    last_purge_time_ = src_schema.last_purge_time_;
    last_purge_rows_ = src_schema.last_purge_rows_;
    schema_version_ = src_schema.schema_version_;
    if (OB_FAIL(deep_copy_str(src_schema.purge_next_, purge_next_))) {
      LOG_WARN("deep copy purge next failed", KR(ret), K(src_schema.purge_next_));
    } else if (OB_FAIL(deep_copy_str(src_schema.purge_job_, purge_job_))) {
      LOG_WARN("deep copy purge job failed", KR(ret), K(src_schema.purge_job_));
    } else if (OB_FAIL(deep_copy_str(src_schema.last_purge_trace_id_, last_purge_trace_id_))) {
      LOG_WARN("deep copy last purge trace id failed", KR(ret), K(src_schema.last_purge_trace_id_));
    }
  }
  return *this;
}

int ObMLogInfo::assign(const ObMLogInfo &other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

bool ObMLogInfo::is_valid() const
{
  bool bret = false;
  if (OB_LIKELY(ObSchema::is_valid())) {
    bret = (OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_ID != mlog_id_ &&
            ObMLogPurgeMode::MAX != purge_mode_ && OB_INVALID_VERSION != schema_version_);
  }
  return bret;
}

void ObMLogInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  mlog_id_ = OB_INVALID_ID;
  purge_mode_ = ObMLogPurgeMode::MAX;
  purge_start_ = OB_INVALID_TIMESTAMP;
  reset_string(purge_next_);
  reset_string(purge_job_);
  last_purge_scn_ = OB_INVALID_SCN_VAL;
  last_purge_date_ = OB_INVALID_TIMESTAMP;
  last_purge_time_ = OB_INVALID_COUNT;
  last_purge_rows_ = OB_INVALID_COUNT;
  reset_string(last_purge_trace_id_);
  schema_version_ = OB_INVALID_VERSION;
  ObSchema::reset();
}

int64_t ObMLogInfo::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObMLogInfo));
  len += purge_next_.length() + 1;
  len += purge_job_.length() + 1;
  len += last_purge_trace_id_.length() + 1;
  return len;
}

OB_SERIALIZE_MEMBER(ObMLogInfo,
                    tenant_id_,
                    mlog_id_,
                    purge_mode_,
                    purge_start_,
                    purge_next_,
                    purge_job_,
                    last_purge_scn_,
                    last_purge_date_,
                    last_purge_time_,
                    last_purge_rows_,
                    last_purge_trace_id_,
                    schema_version_);

int ObMLogInfo::gen_insert_mlog_dml(const uint64_t exec_tenant_id, ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
      OB_FAIL(dml.add_pk_column("mlog_id", mlog_id_)) ||
      OB_FAIL(dml.add_column("purge_mode", purge_mode_)) ||
      (OB_INVALID_TIMESTAMP != purge_start_ &&
       OB_FAIL(dml.add_time_column("purge_start", purge_start_))) ||
      (!purge_next_.empty() &&
       OB_FAIL(dml.add_column("purge_next", ObHexEscapeSqlStr(purge_next_)))) ||
      (!purge_job_.empty() &&
       OB_FAIL(dml.add_column("purge_job", ObHexEscapeSqlStr(purge_job_)))) ||
      (OB_INVALID_SCN_VAL != last_purge_scn_ &&
       OB_FAIL(dml.add_uint64_column("last_purge_scn", last_purge_scn_))) ||
      (OB_INVALID_TIMESTAMP != last_purge_date_ &&
       OB_FAIL(dml.add_time_column("last_purge_date", last_purge_date_))) ||
      (OB_INVALID_COUNT != last_purge_time_ &&
       OB_FAIL(dml.add_column("last_purge_time", last_purge_time_))) ||
      (OB_INVALID_COUNT != last_purge_rows_ &&
       OB_FAIL(dml.add_column("last_purge_rows", last_purge_rows_))) ||
      (!last_purge_trace_id_.empty() &&
       OB_FAIL(dml.add_column("last_purge_trace_id", ObHexEscapeSqlStr(last_purge_trace_id_)))) ||
      OB_FAIL(dml.add_column("schema_version", schema_version_))) {
    LOG_WARN("add column failed", KR(ret));
  }
  return ret;
}

int ObMLogInfo::insert_mlog_info(ObISQLClient &sql_client, const ObMLogInfo &mlog_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = mlog_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t compat_version = 0;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, insert mlog info is");
  } else if (OB_UNLIKELY(!mlog_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(mlog_info));
  } else if (OB_FAIL(mlog_info.gen_insert_mlog_dml(exec_tenant_id, dml))) {
    LOG_WARN("fail to gen insert mlog dml", KR(ret), K(mlog_info));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_insert(OB_ALL_MLOG_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update failed", KR(ret));
    } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObMLogInfo::gen_update_mlog_attribute_dml(const uint64_t exec_tenant_id,
                                              ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
      OB_FAIL(dml.add_pk_column("mlog_id", mlog_id_)) ||
      OB_FAIL(dml.add_column("purge_mode", purge_mode_)) ||
      (OB_INVALID_TIMESTAMP != purge_start_
         ? OB_FAIL(dml.add_time_column("purge_start", purge_start_))
         : OB_FAIL(dml.add_column(true, "purge_start"))) ||
      (!purge_next_.empty() ? OB_FAIL(dml.add_column("purge_next", ObHexEscapeSqlStr(purge_next_)))
                            : OB_FAIL(dml.add_column(true, "purge_next"))) ||
      (!purge_job_.empty() ? OB_FAIL(dml.add_column("purge_job", ObHexEscapeSqlStr(purge_job_)))
                           : OB_FAIL(dml.add_column(true, "purge_job")))) {
    LOG_WARN("add column failed", KR(ret));
  }
  return ret;
}

int ObMLogInfo::update_mlog_attribute(ObISQLClient &sql_client, const ObMLogInfo &mlog_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = mlog_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t compat_version = 0;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, update mlog attribute is");
  } else if (OB_UNLIKELY(!mlog_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(mlog_info));
  } else if (OB_FAIL(mlog_info.gen_update_mlog_attribute_dml(exec_tenant_id, dml))) {
    LOG_WARN("fail to gen update mlog attribute dml", KR(ret), K(mlog_info));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_update(OB_ALL_MLOG_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update failed", KR(ret));
    } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObMLogInfo::gen_update_mlog_last_purge_info_dml(const uint64_t exec_tenant_id,
                                                    ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_SCN_VAL == last_purge_scn_ ||
                  OB_INVALID_TIMESTAMP == last_purge_date_ ||
                  OB_INVALID_COUNT == last_purge_time_ || OB_INVALID_COUNT == last_purge_rows_ ||
                  last_purge_trace_id_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mlog last purge info", KR(ret), KPC(this));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
             OB_FAIL(dml.add_pk_column("mlog_id", mlog_id_)) ||
             OB_FAIL(dml.add_uint64_column("last_purge_scn", last_purge_scn_)) ||
             OB_FAIL(dml.add_time_column("last_purge_date", last_purge_date_)) ||
             OB_FAIL(dml.add_column("last_purge_time", last_purge_time_)) ||
             OB_FAIL(dml.add_column("last_purge_rows", last_purge_rows_)) ||
             OB_FAIL(
               dml.add_column("last_purge_trace_id", ObHexEscapeSqlStr(last_purge_trace_id_)))) {
    LOG_WARN("add column failed", KR(ret));
  }
  return ret;
}

int ObMLogInfo::update_mlog_last_purge_info(ObISQLClient &sql_client, const ObMLogInfo &mlog_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = mlog_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t compat_version = 0;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, update mlog last purge info is");
  } else if (OB_UNLIKELY(!mlog_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(mlog_info));
  } else if (OB_FAIL(mlog_info.gen_update_mlog_last_purge_info_dml(exec_tenant_id, dml))) {
    LOG_WARN("fail to gen update mlog last purge info dml", KR(ret), K(mlog_info));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_update(OB_ALL_MLOG_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update failed", KR(ret));
    } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObMLogInfo::drop_mlog_info(ObISQLClient &sql_client, const ObMLogInfo &mlog_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = mlog_info.get_tenant_id();
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop mlog info is");
  } else if (OB_UNLIKELY(!mlog_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(mlog_info));
  } else if (OB_FAIL(
               drop_mlog_info(sql_client, mlog_info.get_tenant_id(), mlog_info.get_mlog_id()))) {
    LOG_WARN("fail to drop mlog info", KR(ret), K(mlog_info));
  }
  return ret;
}

int ObMLogInfo::drop_mlog_info(ObISQLClient &sql_client, const uint64_t tenant_id,
                               const uint64_t mlog_id)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop mlog info is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == mlog_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(mlog_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("mlog_id", mlog_id))) {
      LOG_WARN("add column failed", KR(ret));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_delete(OB_ALL_MLOG_TNAME, dml, affected_rows))) {
        LOG_WARN("execute update failed", KR(ret));
      } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObMLogInfo::fetch_mlog_info(ObISQLClient &sql_client, uint64_t tenant_id, uint64_t mlog_id,
                                ObMLogInfo &mlog_info, bool for_update, bool nowait)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, fetch mlog info is");
  }
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult *result = nullptr;
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = 0 AND mlog_id = %ld",
                               OB_ALL_MLOG_TNAME, mlog_id))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (for_update && !nowait && OB_FAIL(sql.append(" for update"))) {
      LOG_WARN("fail to append sql", KR(ret));
    } else if (for_update && nowait && OB_FAIL(sql.append(" for update nowait"))) {
      LOG_WARN("fail to append sql", KR(ret));
    } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next", KR(ret));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("mlog info not exist", KR(ret), K(tenant_id), K(mlog_id));
      }
    } else {
      mlog_info.set_tenant_id(tenant_id);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, mlog_id, mlog_info, uint64_t);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, purge_mode, mlog_info, ObMLogPurgeMode);
      EXTRACT_TIMESTAMP_FIELD_TO_CLASS_MYSQL_SKIP_RET(*result, purge_start, mlog_info, nullptr);
      EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(*result, purge_next, mlog_info);
      EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(*result, purge_job, mlog_info);
      EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        *result, last_purge_scn, mlog_info, uint64_t, true, false, OB_INVALID_SCN_VAL);
      EXTRACT_TIMESTAMP_FIELD_TO_CLASS_MYSQL_SKIP_RET(*result, last_purge_date, mlog_info, nullptr);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(*result, last_purge_time, mlog_info,
                                                          int64_t, true, false, OB_INVALID_COUNT);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(*result, last_purge_rows, mlog_info,
                                                          int64_t, true, false, OB_INVALID_COUNT);
      EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(*result, last_purge_trace_id, mlog_info);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, schema_version, mlog_info, int64_t);
    }
  }
  return ret;
}

int ObMLogInfo::batch_fetch_mlog_ids(ObISQLClient &sql_client, uint64_t tenant_id,
                                     uint64_t last_mlog_id, ObIArray<uint64_t> &mlog_ids,
                                     int64_t limit)
{
  int ret = OB_SUCCESS;
  mlog_ids.reset();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult *result = nullptr;
    ObSqlString sql;
    uint64_t mlog_id = OB_INVALID_ID;
    if (OB_FAIL(sql.assign_fmt("SELECT mlog_id FROM %s WHERE tenant_id = 0", OB_ALL_MLOG_TNAME))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_INVALID_ID != last_mlog_id &&
               OB_FAIL(sql.append_fmt(" and mlog_id > %ld", last_mlog_id))) {
      LOG_WARN("fail to append sql", KR(ret));
    } else if (OB_FAIL(sql.append(" order by mlog_id"))) {
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
      EXTRACT_INT_FIELD_MYSQL(*result, "mlog_id", mlog_id, uint64_t);
      OZ(mlog_ids.push_back(mlog_id));
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
