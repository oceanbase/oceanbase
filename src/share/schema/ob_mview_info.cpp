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

#include "share/schema/ob_mview_info.h"
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

ObMViewInfo::ObMViewInfo() { reset(); }

ObMViewInfo::ObMViewInfo(ObIAllocator *allocator) : ObSchema(allocator) { reset(); }

ObMViewInfo::ObMViewInfo(const ObMViewInfo &src_schema)
{
  reset();
  *this = src_schema;
}

ObMViewInfo::~ObMViewInfo() {}

ObMViewInfo &ObMViewInfo::operator=(const ObMViewInfo &src_schema)
{
  if (this != &src_schema) {
    reset();
    int &ret = error_ret_;
    tenant_id_ = src_schema.tenant_id_;
    mview_id_ = src_schema.mview_id_;
    build_mode_ = src_schema.build_mode_;
    refresh_mode_ = src_schema.refresh_mode_;
    refresh_method_ = src_schema.refresh_method_;
    refresh_start_ = src_schema.refresh_start_;
    last_refresh_scn_ = src_schema.last_refresh_scn_;
    last_refresh_type_ = src_schema.last_refresh_type_;
    last_refresh_date_ = src_schema.last_refresh_date_;
    last_refresh_time_ = src_schema.last_refresh_time_;
    schema_version_ = src_schema.schema_version_;
    if (OB_FAIL(deep_copy_str(src_schema.refresh_next_, refresh_next_))) {
      LOG_WARN("deep copy refresh next failed", KR(ret), K(src_schema.refresh_next_));
    } else if (OB_FAIL(deep_copy_str(src_schema.refresh_job_, refresh_job_))) {
      LOG_WARN("deep copy refresh job failed", KR(ret), K(src_schema.refresh_job_));
    } else if (OB_FAIL(deep_copy_str(src_schema.last_refresh_trace_id_, last_refresh_trace_id_))) {
      LOG_WARN("deep copy last refresh trace id failed", KR(ret),
               K(src_schema.last_refresh_trace_id_));
    }
  }
  return *this;
}

int ObMViewInfo::assign(const ObMViewInfo &other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

bool ObMViewInfo::is_valid() const
{
  bool bret = false;
  if (OB_LIKELY(ObSchema::is_valid())) {
    bret = (OB_INVALID_TENANT_ID != tenant_id_ && OB_INVALID_ID != mview_id_ &&
            ObMViewBuildMode::MAX != build_mode_ && ObMVRefreshMode::MAX != refresh_mode_ &&
            ObMVRefreshMethod::MAX != refresh_method_ && OB_INVALID_VERSION != schema_version_);
  }
  return bret;
}

void ObMViewInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  mview_id_ = OB_INVALID_ID;
  build_mode_ = ObMViewBuildMode::MAX;
  refresh_mode_ = ObMVRefreshMode::MAX;
  refresh_method_ = ObMVRefreshMethod::MAX;
  refresh_start_ = OB_INVALID_TIMESTAMP;
  reset_string(refresh_next_);
  reset_string(refresh_job_);
  last_refresh_scn_ = OB_INVALID_SCN_VAL;
  last_refresh_type_ = ObMVRefreshType::MAX;
  last_refresh_date_ = OB_INVALID_TIMESTAMP;
  last_refresh_time_ = OB_INVALID_COUNT;
  reset_string(last_refresh_trace_id_);
  schema_version_ = OB_INVALID_VERSION;
  ObSchema::reset();
}

int64_t ObMViewInfo::get_convert_size() const
{
  int64_t len = 0;
  len += static_cast<int64_t>(sizeof(ObMViewInfo));
  len += refresh_next_.length() + 1;
  len += refresh_job_.length() + 1;
  len += last_refresh_trace_id_.length() + 1;
  return len;
}

OB_SERIALIZE_MEMBER(ObMViewInfo,
                    tenant_id_,
                    mview_id_,
                    build_mode_,
                    refresh_mode_,
                    refresh_method_,
                    refresh_start_,
                    refresh_next_,
                    refresh_job_,
                    last_refresh_scn_,
                    last_refresh_type_,
                    last_refresh_date_,
                    last_refresh_time_,
                    last_refresh_trace_id_,
                    schema_version_);

int ObMViewInfo::gen_insert_mview_dml(const uint64_t exec_tenant_id, ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
      OB_FAIL(dml.add_pk_column("mview_id", mview_id_)) ||
      OB_FAIL(dml.add_column("build_mode", build_mode_)) ||
      OB_FAIL(dml.add_column("refresh_mode", refresh_mode_)) ||
      OB_FAIL(dml.add_column("refresh_method", refresh_method_)) ||
      (OB_INVALID_TIMESTAMP != refresh_start_ &&
       OB_FAIL(dml.add_time_column("refresh_start", refresh_start_))) ||
      (!refresh_next_.empty() &&
       OB_FAIL(dml.add_column("refresh_next", ObHexEscapeSqlStr(refresh_next_)))) ||
      (!refresh_job_.empty() &&
       OB_FAIL(dml.add_column("refresh_job", ObHexEscapeSqlStr(refresh_job_)))) ||
      (OB_INVALID_SCN_VAL != last_refresh_scn_ &&
       OB_FAIL(dml.add_uint64_column("last_refresh_scn", last_refresh_scn_))) ||
      (ObMVRefreshType::MAX != last_refresh_type_ &&
       OB_FAIL(dml.add_column("last_refresh_type", last_refresh_type_))) ||
      (OB_INVALID_TIMESTAMP != last_refresh_date_ &&
       OB_FAIL(dml.add_time_column("last_refresh_date", last_refresh_date_))) ||
      (OB_INVALID_COUNT != last_refresh_time_ &&
       OB_FAIL(dml.add_column("last_refresh_time", last_refresh_time_))) ||
      (!last_refresh_trace_id_.empty() &&
       OB_FAIL(
         dml.add_column("last_refresh_trace_id", ObHexEscapeSqlStr(last_refresh_trace_id_)))) ||
      OB_FAIL(dml.add_column("schema_version", schema_version_))) {
    LOG_WARN("add column failed", KR(ret));
  }
  return ret;
}

int ObMViewInfo::insert_mview_info(ObISQLClient &sql_client, const ObMViewInfo &mview_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = mview_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t compat_version = 0;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, insert mview info is");
  } else if (OB_UNLIKELY(!mview_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(mview_info));
  } else if (OB_FAIL(mview_info.gen_insert_mview_dml(exec_tenant_id, dml))) {
    LOG_WARN("fail to gen insert mview dml", KR(ret), K(mview_info));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_insert(OB_ALL_MVIEW_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update failed", KR(ret));
    } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObMViewInfo::gen_update_mview_attribute_dml(const uint64_t exec_tenant_id,
                                                ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
      OB_FAIL(dml.add_pk_column("mview_id", mview_id_)) ||
      OB_FAIL(dml.add_column("build_mode", build_mode_)) ||
      OB_FAIL(dml.add_column("refresh_mode", refresh_mode_)) ||
      OB_FAIL(dml.add_column("refresh_method", refresh_method_)) ||
      (OB_INVALID_TIMESTAMP != refresh_start_
         ? OB_FAIL(dml.add_time_column("refresh_start", refresh_start_))
         : OB_FAIL(dml.add_column(true, "refresh_start"))) ||
      (!refresh_next_.empty()
         ? OB_FAIL(dml.add_column("refresh_next", ObHexEscapeSqlStr(refresh_next_)))
         : OB_FAIL(dml.add_column(true, "refresh_next"))) ||
      (!refresh_job_.empty()
         ? OB_FAIL(dml.add_column("refresh_job", ObHexEscapeSqlStr(refresh_job_)))
         : OB_FAIL(dml.add_column(true, "refresh_next")))) {
    LOG_WARN("add column failed", KR(ret));
  }
  return ret;
}

int ObMViewInfo::update_mview_attribute(ObISQLClient &sql_client, const ObMViewInfo &mview_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = mview_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t compat_version = 0;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, update mview attribute is");
  } else if (OB_UNLIKELY(!mview_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(mview_info));
  } else if (OB_FAIL(mview_info.gen_update_mview_attribute_dml(exec_tenant_id, dml))) {
    LOG_WARN("fail to gen update mview attribute dml", KR(ret), K(mview_info));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_update(OB_ALL_MVIEW_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update failed", KR(ret));
    } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObMViewInfo::gen_update_mview_last_refresh_info_dml(const uint64_t exec_tenant_id,
                                                        ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_SCN_VAL == last_refresh_scn_ ||
                  ObMVRefreshType::MAX == last_refresh_type_ ||
                  OB_INVALID_TIMESTAMP == last_refresh_date_ ||
                  OB_INVALID_COUNT == last_refresh_time_ || last_refresh_trace_id_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mview last refresh info", KR(ret), KPC(this));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
             OB_FAIL(dml.add_pk_column("mview_id", mview_id_)) ||
             OB_FAIL(dml.add_uint64_column("last_refresh_scn", last_refresh_scn_)) ||
             OB_FAIL(dml.add_column("last_refresh_type", last_refresh_type_)) ||
             OB_FAIL(dml.add_time_column("last_refresh_date", last_refresh_date_)) ||
             OB_FAIL(dml.add_column("last_refresh_time", last_refresh_time_)) ||
             OB_FAIL(dml.add_column("last_refresh_trace_id",
                                    ObHexEscapeSqlStr(last_refresh_trace_id_)))) {
    LOG_WARN("add column failed", KR(ret));
  }
  return ret;
}

int ObMViewInfo::update_mview_last_refresh_info(ObISQLClient &sql_client,
                                                const ObMViewInfo &mview_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = mview_info.get_tenant_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t compat_version = 0;
  ObDMLSqlSplicer dml;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, update mview last refresh info is");
  } else if (OB_UNLIKELY(!mview_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(mview_info));
  } else if (OB_FAIL(mview_info.gen_update_mview_last_refresh_info_dml(exec_tenant_id, dml))) {
    LOG_WARN("fail to gen update mview last refresh info dml", KR(ret), K(mview_info));
  } else {
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_update(OB_ALL_MVIEW_TNAME, dml, affected_rows))) {
      LOG_WARN("execute update failed", KR(ret));
    } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
    }
  }
  return ret;
}

int ObMViewInfo::drop_mview_info(ObISQLClient &sql_client, const ObMViewInfo &mview_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = mview_info.get_tenant_id();
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop mview info is");
  } else if (OB_UNLIKELY(!mview_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(mview_info));
  } else if (OB_FAIL(drop_mview_info(sql_client, mview_info.get_tenant_id(),
                                     mview_info.get_mview_id()))) {
    LOG_WARN("fail to drop mview info", KR(ret), K(mview_info));
  }
  return ret;
}

int ObMViewInfo::drop_mview_info(ObISQLClient &sql_client, const uint64_t tenant_id,
                                 const uint64_t mview_id)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, drop mview info is");
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == mview_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(mview_id));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(dml.add_pk_column("tenant_id", 0)) ||
        OB_FAIL(dml.add_pk_column("mview_id", mview_id))) {
      LOG_WARN("add column failed", KR(ret));
    } else {
      ObDMLExecHelper exec(sql_client, exec_tenant_id);
      int64_t affected_rows = 0;
      if (OB_FAIL(exec.exec_delete(OB_ALL_MVIEW_TNAME, dml, affected_rows))) {
        LOG_WARN("execute delete failed", KR(ret));
      } else if (OB_UNLIKELY(!is_single_row(affected_rows))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows unexpected to be one", KR(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObMViewInfo::fetch_mview_info(ObISQLClient &sql_client, uint64_t tenant_id, uint64_t mview_id,
                                  ObMViewInfo &mview_info, bool for_update, bool nowait)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < DATA_VERSION_4_3_0_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant's data version is below 4.3.0.0, fetch mview info is");
  }
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult *result = nullptr;
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = 0 AND mview_id = %ld",
                               OB_ALL_MVIEW_TNAME, mview_id))) {
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
        LOG_WARN("mview info not exist", KR(ret), K(tenant_id), K(mview_id));
      }
    } else {
      mview_info.set_tenant_id(tenant_id);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, mview_id, mview_info, uint64_t);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, build_mode, mview_info, ObMViewBuildMode);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, refresh_mode, mview_info, ObMVRefreshMode);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, refresh_method, mview_info, ObMVRefreshMethod);
      EXTRACT_TIMESTAMP_FIELD_TO_CLASS_MYSQL_SKIP_RET(*result, refresh_start, mview_info, nullptr);
      EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(*result, refresh_next, mview_info);
      EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(*result, refresh_job, mview_info);
      EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        *result, last_refresh_scn, mview_info, uint64_t, true, false, OB_INVALID_SCN_VAL);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(
        *result, last_refresh_type, mview_info, ObMVRefreshType, true, false, ObMVRefreshType::MAX);
      EXTRACT_TIMESTAMP_FIELD_TO_CLASS_MYSQL_SKIP_RET(*result, last_refresh_date, mview_info,
                                                      nullptr);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(*result, last_refresh_time, mview_info,
                                                          int64_t, true, false, OB_INVALID_COUNT);
      EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL_SKIP_RET(*result, last_refresh_trace_id, mview_info);
      EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, schema_version, mview_info, int64_t);
    }
  }
  return ret;
}

int ObMViewInfo::batch_fetch_mview_ids(ObISQLClient &sql_client, uint64_t tenant_id,
                                       uint64_t last_mview_id, ObIArray<uint64_t> &mview_ids,
                                       int64_t limit)
{
  int ret = OB_SUCCESS;
  mview_ids.reset();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult *result = nullptr;
    ObSqlString sql;
    uint64_t mview_id = OB_INVALID_ID;
    if (OB_FAIL(
          sql.assign_fmt("SELECT mview_id FROM %s WHERE tenant_id = 0", OB_ALL_MVIEW_TNAME))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_INVALID_ID != last_mview_id &&
               OB_FAIL(sql.append_fmt(" and mview_id > %ld", last_mview_id))) {
      LOG_WARN("fail to append sql", KR(ret));
    } else if (OB_FAIL(sql.append(" order by mview_id"))) {
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
      EXTRACT_INT_FIELD_MYSQL(*result, "mview_id", mview_id, uint64_t);
      OZ(mview_ids.push_back(mview_id));
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
