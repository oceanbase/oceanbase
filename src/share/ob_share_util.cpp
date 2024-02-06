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
#include "share/ob_share_util.h"
#include "common/ob_timeout_ctx.h"
#include "lib/worker.h"
#include "lib/time/ob_time_utility.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_cluster_version.h" // for GET_MIN_DATA_VERSION
#include "lib/mysqlclient/ob_isql_client.h"
namespace oceanbase
{
using namespace common;
namespace share
{
int ObShareUtil::set_default_timeout_ctx(ObTimeoutCtx &ctx, const int64_t default_timeout)
{
  int ret = OB_SUCCESS;
  int64_t abs_timeout_ts = OB_INVALID_TIMESTAMP;
  int64_t ctx_timeout_ts = ctx.get_abs_timeout();
  int64_t worker_timeout_ts = THIS_WORKER.get_timeout_ts();
  if (0 < ctx_timeout_ts) {
    //ctx is already been set, use it
    abs_timeout_ts = ctx_timeout_ts;
  } else if (INT64_MAX == worker_timeout_ts) {
    //if worker's timeout_ts not be set，set to default timeout
    abs_timeout_ts = ObTimeUtility::current_time() + default_timeout;
  } else if (0 < worker_timeout_ts) {
    //use worker's timeout if only it is valid
    abs_timeout_ts = worker_timeout_ts;
  } else {
    //worker's timeout_ts is invalid, set to default timeout
    abs_timeout_ts = ObTimeUtility::current_time() + default_timeout;
  }
  if (OB_FAIL(ctx.set_abs_timeout(abs_timeout_ts))) {
    LOG_WARN("set timeout failed", KR(ret), K(abs_timeout_ts), K(ctx_timeout_ts),
        K(worker_timeout_ts), K(default_timeout));
  } else if (ctx.is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("timeouted", KR(ret), K(abs_timeout_ts), K(ctx_timeout_ts),
        K(worker_timeout_ts), K(default_timeout));
  } else {
    LOG_TRACE("set_default_timeout_ctx success", K(abs_timeout_ts),
        K(ctx_timeout_ts), K(worker_timeout_ts), K(default_timeout));
  }
  return ret;
}

int ObShareUtil::get_abs_timeout(const int64_t default_timeout, int64_t &abs_timeout)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
    LOG_WARN("fail to set default timeout ctx", KR(ret), K(default_timeout));
  } else {
    abs_timeout = ctx.get_abs_timeout();
  }
  return ret;
}

int ObShareUtil::get_ctx_timeout(const int64_t default_timeout, int64_t &timeout)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, default_timeout))) {
    LOG_WARN("fail to set default timeout ctx", KR(ret), K(default_timeout));
  } else {
    timeout = ctx.get_timeout();
  }
  return ret;
}

int ObShareUtil::check_compat_version_for_arbitration_service(
    const uint64_t tenant_id,
    bool &is_compatible)
{
  int ret = OB_SUCCESS;
  is_compatible = false;
  uint64_t data_version = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, data_version))) {
    LOG_WARN("fail to get sys tenant data version", KR(ret));
  } else if (DATA_VERSION_4_1_0_0 > data_version) {
    is_compatible = false;
  } else if (!is_sys_tenant(tenant_id)
             && OB_FAIL(GET_MIN_DATA_VERSION(gen_user_tenant_id(tenant_id), data_version))) {
    LOG_WARN("fail to get user tenant data version", KR(ret), "tenant_id", gen_user_tenant_id(tenant_id));
  } else if (!is_sys_tenant(tenant_id) && DATA_VERSION_4_1_0_0 > data_version) {
    is_compatible = false;
  } else if (!is_sys_tenant(tenant_id)
             && OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id), data_version))) {
     LOG_WARN("fail to get meta tenant data version", KR(ret), "tenant_id", gen_meta_tenant_id(tenant_id));
  } else if (!is_sys_tenant(tenant_id) && DATA_VERSION_4_1_0_0 > data_version) {
    is_compatible = false;
  } else {
    is_compatible = true;
  }
  return ret;
}

int ObShareUtil::generate_arb_replica_num(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    int64_t &arb_replica_num)
{
  int ret = OB_SUCCESS;
  arb_replica_num = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid()
                  || !ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObShareUtil::fetch_current_cluster_version(
    common::ObISQLClient &client,
    uint64_t &cluster_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  sqlclient::ObMySQLResult *result = NULL;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
  if (OB_FAIL(sql.assign_fmt(
      "select value from %s where name = '%s'",
      OB_ALL_SYS_PARAMETER_TNAME, "min_observer_version"))) {
    LOG_WARN("fail to assign fmt", KR(ret), K(sql));
  } else if (OB_FAIL(client.read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
    LOG_WARN("execute sql failed", KR(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get result", KR(ret));
  } else if (OB_FAIL(result->next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("min_observer_version not exist, may be in bootstrap stage", KR(ret));
    } else {
      LOG_WARN("fail to get next", KR(ret));
    }
  } else {
    ObString value;
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "value", value);
    if (FAILEDx(ObClusterVersion::get_version(value, cluster_version))) {
      LOG_WARN("fail to get version", KR(ret), K(value));
    }
  }
  } // end SMART_VAR
  return ret;
}

int ObShareUtil::fetch_current_data_version(
    common::ObISQLClient &client,
    const uint64_t tenant_id,
    uint64_t &data_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  sqlclient::ObMySQLResult *result = NULL;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || OB_INVALID_TENANT_ID == exec_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K(tenant_id), K(exec_tenant_id));
  } else if (OB_FAIL(sql.assign_fmt(
      "select value from %s where name = '%s' and tenant_id = %lu",
      OB_TENANT_PARAMETER_TNAME, "compatible", tenant_id))) {
    LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(sql));
  } else if (OB_FAIL(client.read(res, exec_tenant_id, sql.ptr()))) {
    LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get result", KR(ret), K(tenant_id));
  } else if (OB_FAIL(result->next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("compatible not exist, create tenant process may be doing or failed ",
               KR(ret), K(tenant_id));
    } else {
      LOG_WARN("fail to get next", KR(ret), K(tenant_id));
    }
  } else {
    ObString value;
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "value", value);
    if (FAILEDx(ObClusterVersion::get_version(value, data_version))) {
      LOG_WARN("fail to get version", KR(ret), K(value));
    }
  }
  } // end SMART_VAR
  return ret;
}
} //end namespace share
} //end namespace oceanbase
