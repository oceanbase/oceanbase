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

#include "ob_flashback_log_scn_table_operator.h"

#include "lib/utility/ob_print_utils.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
namespace share
{

static const char* FLASHBACK_LOG_SCN_OP_ARRAY[] =
{
  "INVALID",
  "FAILOVER TO PRIMARY",
  "FLASHBACK STANDBY LOG",
};

const char* ObFlashbackLogSCNType::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(FLASHBACK_LOG_SCN_OP_ARRAY) == MAX_OP, "array size mismatch");
  const char *type_str = "INVALID";
  if (OB_UNLIKELY(type_ >= ARRAYSIZEOF(FLASHBACK_LOG_SCN_OP_ARRAY)
                  || type_ < INVALID_OP)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown flashback log op", K_(type));
  } else {
    type_str = FLASHBACK_LOG_SCN_OP_ARRAY[type_];
  }
  return type_str;
}

int ObFlashbackLogSCNTableOperator::check_is_flashback_log_table_enabled(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  uint64_t meta_tenant_data_version = 0;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not user tenant", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(meta_tenant_id, meta_tenant_data_version))) {
    LOG_WARN("fail to get the meta tenant's min data version", KR(ret), K(meta_tenant_id));
  } else if (!(meta_tenant_data_version >= MOCK_DATA_VERSION_4_2_5_4 && meta_tenant_data_version < DATA_VERSION_4_3_0_0)
      && !(meta_tenant_data_version >= DATA_VERSION_4_4_1_0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("meta_tenant_data_version should be [4.2.5.4, 4.3.0.0) or [4.4.1.0, +infinity)", KR(ret), K(meta_tenant_data_version));
  }
  return ret;
}

int ObFlashbackLogSCNTableOperator::insert_flashback_log_scn(
    const uint64_t tenant_id,
    const int64_t switchover_epoch,
    const ObFlashbackLogSCNType& op_type,
    const SCN& flashback_log_scn,
    ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s "
      "(tenant_id, switchover_epoch, op_type, flashback_log_scn) value (%lu, %lu, '%s', %lu)",
      OB_ALL_TENANT_FLASHBACK_LOG_SCN_TNAME, tenant_id, switchover_epoch, op_type.to_str(),
      flashback_log_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to insert service_name", KR(ret), K(tenant_id), K(switchover_epoch),
        K(op_type), K(flashback_log_scn));
  } else if (OB_FAIL(proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
  } else if (OB_UNLIKELY(1 != affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows should be one", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObFlashbackLogSCNTableOperator::check_flashback_log_scn_based_on_switchover_epoch(
  const uint64_t tenant_id,
  const int64_t switchover_epoch,
  const SCN& flashback_log_scn,
  ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  uint64_t scn_in_table = OB_INVALID_SCN_VAL;
  if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (OB_FAIL(sql.assign_fmt("SELECT flashback_log_scn FROM %s WHERE "
      "tenant_id=%lu and switchover_epoch=%lu", OB_ALL_TENANT_FLASHBACK_LOG_SCN_TNAME, tenant_id, switchover_epoch))) {
    LOG_WARN("failed to assign sql", KR(ret), K(sql), K(tenant_id), K(switchover_epoch));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      int tmp_ret = OB_SUCCESS;
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy->read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", K(sql), K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get next", KR(ret), K(sql));
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        EXTRACT_UINT_FIELD_MYSQL(*result, "flashback_log_scn", scn_in_table, uint64_t);
        if (OB_FAIL(ret)) {
        } else if (flashback_log_scn.get_val_for_inner_table_field() != scn_in_table) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("the previous FLASHBACK STANDBY LOG is still in progress, "
              "cannot set a new flashback_log_scn", KR(ret), K(flashback_log_scn), K(scn_in_table));
          LOG_USER_ERROR(OB_OP_NOT_ALLOW, "The previous FLASHBACK STANDBY LOG is still in progress,"
              " setting a new flashback_log_scn is");
        }
      }
      if (OB_SUCC(ret) && (OB_ITER_END != (tmp_ret = result->next()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get more row than one", KR(ret), KR(tmp_ret), K(sql));
      }
    }
  }
  return ret;
}

} // share
} // oceanbase