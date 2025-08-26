/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_profile_util.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "lib/string/ob_sql_string.h"
#include "sql/engine/ob_phy_operator_type.h"

namespace oceanbase
{
using namespace sql;
using namespace observer;
using namespace common::sqlclient;
namespace common
{
enum FETCH_COLUMN
{
  OP_ID = 0,      // int
  PLAN_DEPTH,     // int
  PLAN_OPERATION, // varchar
  SVR_IP,         // varchar
  SVR_PORT,       // int
  THREAD_ID,      // int
  DB_TIME,        // int
  RAW_PROFILE     // varchar
};

int ObProfileUtil::get_profile_by_id(ObIAllocator *alloc, int64_t session_tenant_id,
                                     const ObString &trace_id, const ObString &svr_ip,
                                     int64_t svr_port, int64_t param_tenant_id, bool fetch_all_op,
                                     int64_t op_id, ObIArray<ObProfileItem> &profile_items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT \
                  PLAN_LINE_ID OP_ID,\
                  PLAN_DEPTH,\
                  PLAN_OPERATION, \
                  SVR_IP, \
                  SVR_PORT, \
                  THREAD_ID, \
                  DB_TIME, \
                  raw_profile \
                  FROM OCEANBASE.__ALL_VIRTUAL_SQL_PLAN_MONITOR \
                  WHERE TENANT_ID=%ld \
                  AND TRACE_ID='%.*s' ",
                             param_tenant_id, trace_id.length(), trace_id.ptr()))) {
    LOG_WARN("failed to assign string");
  } else if (!svr_ip.empty()
             && OB_FAIL(sql.append_fmt("AND SVR_IP='%.*s' ", svr_ip.length(), svr_ip.ptr()))) {
    LOG_WARN("failed to append svr_ip");
  } else if (svr_port != 0 && OB_FAIL(sql.append_fmt("AND SVR_PORT=%ld ", svr_port))) {
    LOG_WARN("failed to append svr_port");
  } else if (!fetch_all_op && OB_FAIL(sql.append_fmt("AND PLAN_LINE_ID=%.ld ", op_id))) {
    LOG_WARN("failed to assign op_id");
  } else if (OB_FAIL(sql.append_fmt("ORDER BY PLAN_LINE_ID "))) {
    LOG_WARN("failed to append order by op id");
  } else if (OB_FAIL(inner_get_profile(alloc, session_tenant_id, sql, profile_items))) {
    LOG_WARN("failed to get profile info");
  }
  LOG_INFO("print sql plan monitor sql", K(sql));
  return ret;
}

int ObProfileUtil::inner_get_profile(ObIAllocator *alloc, int64_t tenant_id, const ObSqlString &sql,
                                     ObIArray<ObProfileItem> &profile_items)
{
  int ret = OB_SUCCESS;
  ObISQLClient *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sql proxy");
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObMySQLResult *mysql_result = nullptr;
      if (OB_FAIL(sql_proxy->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(sql));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql fail", K(tenant_id), K(sql));
      }
      while (OB_SUCC(ret) && OB_SUCC(mysql_result->next())) {
        ObProfileItem profile_item;
        if (OB_FAIL(read_profile_from_result(alloc, *mysql_result, profile_item))) {
          LOG_WARN("failed to read profile info");
        } else if (nullptr == profile_item.profile_) {
          // without profile, skip it
        } else if (OB_FAIL(profile_items.push_back(profile_item))) {
          LOG_WARN("failed to push back info");
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObProfileUtil::read_profile_from_result(ObIAllocator *alloc, const ObMySQLResult &mysql_result,
                                            ObProfileItem &profile_item)
{
  int ret = OB_SUCCESS;
  int64_t int_value;
  ObString varchar_val;
  number::ObNumber num_val;

#define GET_NUM_VALUE(IDX, value)                                                                  \
  do {                                                                                             \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(mysql_result.get_number(IDX, num_val))) {                                   \
      if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {        \
        value = 0;                                                                                 \
        ret = OB_SUCCESS;                                                                          \
      } else {                                                                                     \
        LOG_WARN("failed to get number value", K(ret));                                            \
      }                                                                                            \
    } else if (OB_FAIL(num_val.cast_to_int64(int_value))) {                                        \
      LOG_WARN("failed to cast to int64", K(ret));                                                 \
    } else {                                                                                       \
      value = int_value;                                                                           \
    }                                                                                              \
  } while (0);

#define GET_INT_VALUE(IDX, value)                                                                  \
  do {                                                                                             \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(mysql_result.get_int(IDX, int_value))) {                                    \
      if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {        \
        value = 0;                                                                                 \
        ret = OB_SUCCESS;                                                                          \
      } else {                                                                                     \
        ret = OB_SUCCESS;                                                                          \
        /*retry number type*/                                                                      \
        GET_NUM_VALUE(IDX, value);                                                                 \
      }                                                                                            \
    } else {                                                                                       \
      value = int_value;                                                                           \
    }                                                                                              \
  } while (0);

#define GET_VARCHAR_VALUE(IDX, value)                                                              \
  do {                                                                                             \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(mysql_result.get_varchar(IDX, varchar_val))) {                              \
      if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {        \
        value = nullptr;                                                                           \
        ret = OB_SUCCESS;                                                                          \
      } else {                                                                                     \
        LOG_WARN("failed to get varchar value", K(ret));                                           \
      }                                                                                            \
    } else {                                                                                       \
      char *buf = nullptr;                                                                         \
      if (0 == varchar_val.length()) {                                                             \
        value = nullptr;                                                                           \
      } else if (OB_ISNULL(buf = (char *)alloc->alloc(varchar_val.length() + 1))) {                \
        ret = OB_ALLOCATE_MEMORY_FAILED;                                                           \
        LOG_WARN("failed to allocate memory", K(ret));                                             \
      } else {                                                                                     \
        MEMCPY(buf, varchar_val.ptr(), varchar_val.length());                                      \
        buf[varchar_val.length()] = '\0';                                                          \
        value.assign_ptr(buf, varchar_val.length());                                               \
      }                                                                                            \
    }                                                                                              \
  } while (0);

  ObString raw_profile;
  GET_INT_VALUE(FETCH_COLUMN::OP_ID, profile_item.op_id_);
  GET_INT_VALUE(FETCH_COLUMN::PLAN_DEPTH, profile_item.plan_depth_);
  GET_INT_VALUE(FETCH_COLUMN::DB_TIME, profile_item.db_time_);
  if (OB_SUCC(ret)) {
    profile_item.db_time_ = profile_item.db_time_ * 1000UL;
  }
  GET_VARCHAR_VALUE(FETCH_COLUMN::RAW_PROFILE, raw_profile);
  profile_item.raw_profile_ = raw_profile.ptr();
  profile_item.raw_profile_len_ = raw_profile.length();

  ObProfile *profile = nullptr;
  if (OB_FAIL(ret)) {
  } else if (profile_item.raw_profile_len_ == 0) {
  } else if (OB_FAIL(convert_persist_profile_to_realtime(
                 profile_item.raw_profile_, profile_item.raw_profile_len_, profile, alloc))) {
    LOG_WARN("failed to convert persist profile to realtime");
  } else {
    profile_item.profile_ = profile;
  }
#undef GET_NUM_VALUE
#undef GET_INT_VALUE
#undef GET_VARCHAR_VALUE
  return ret;
}

} // namespace common
} // namespace oceanbase
