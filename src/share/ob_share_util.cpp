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
#ifdef OB_BUILD_ARBITRATION
#include "ob_share_util.h"
#include "share/arbitration_service/ob_arbitration_service_utils.h" // ObArbitrationServiceUtils
#endif
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share::schema;
namespace share
{

void ObIDGenerator::reset()
{
  inited_ = false;
  step_ = 0;
  start_id_ = common::OB_INVALID_ID;
  end_id_ = common::OB_INVALID_ID;
  current_id_ = common::OB_INVALID_ID;
}

int ObIDGenerator::init(
    const uint64_t step,
    const uint64_t start_id,
    const uint64_t end_id)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(start_id > end_id || 0 == step)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_id/end_id", KR(ret), K(start_id), K(end_id), K(step));
  } else {
    step_ = step;
    start_id_ = start_id;
    end_id_ = end_id;
    current_id_ = start_id - step_;
    inited_ = true;
  }
  return ret;
}

int ObIDGenerator::next(uint64_t &current_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("generator is not inited", KR(ret), KPC(this));
  } else if (current_id_ >= end_id_) {
    ret = OB_ITER_END;
  } else {
    current_id_ += step_;
    current_id = current_id_;
  }
  return ret;
}

int ObIDGenerator::get_start_id(uint64_t &start_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("generator is not inited", KR(ret), KPC(this));
  } else {
    start_id = start_id_;
  }
  return ret;
}

int ObIDGenerator::get_current_id(uint64_t &current_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("generator is not inited", KR(ret), KPC(this));
  } else {
    current_id = current_id_;
  }
  return ret;
}

int ObIDGenerator::get_end_id(uint64_t &end_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("generator is not inited", KR(ret), KPC(this));
  } else {
    end_id = end_id_;
  }
  return ret;
}

int ObIDGenerator::get_id_cnt(uint64_t &cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("generator is not inited", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(end_id_ < start_id_
             || step_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid start_id/end_id/step", KR(ret), KPC(this));
  } else {
    cnt = (end_id_ - start_id_) / step_ + 1;
  }
  return ret;
}

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
  return check_compat_data_version_(DATA_VERSION_4_1_0_0, true/*check_meta*/, true/*check_user*/,
                                    tenant_id, is_compatible);
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
#ifdef OB_BUILD_ARBITRATION
  } else if (OB_FAIL(ObArbitrationServiceUtils::generate_arb_replica_num(
                         tenant_id,
                         ls_id,
                         arb_replica_num))) {
    LOG_WARN("fail to generate arb replica number", KR(ret), K(tenant_id), K(ls_id));
#endif
  }
  return ret;
}

int ObShareUtil::check_compat_version_for_readonly_replica(
    const uint64_t tenant_id,
    bool &is_compatible)
{
  return check_compat_data_version_(DATA_VERSION_4_2_0_0, true/*check_meta*/, false/*check_user*/,
                                    tenant_id, is_compatible);
}

int ObShareUtil::check_compat_version_for_columnstore_replica(
    const uint64_t tenant_id,
    bool &is_compatible)
{
  return check_compat_data_version_(DATA_VERSION_4_3_3_0, true/*check_meta*/, false/*check_user*/,
                                    tenant_id, is_compatible);
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

int ObShareUtil::parse_all_server_list(
    const ObArray<ObAddr> &excluded_server_list,
    ObArray<ObAddr> &config_all_server_list)
{
  int ret = OB_SUCCESS;
  config_all_server_list.reset();
  common::ObArenaAllocator allocator(lib::ObLabel("AllSvrList"));
  ObString all_server_list;
  LOG_TRACE("get all_server_list from GCONF", K(GCONF.all_server_list));
  if (OB_FAIL(GCONF.all_server_list.deep_copy_value_string(allocator, all_server_list))) {
    LOG_WARN("fail to deep copy GCONF.all_server_list", KR(ret), K(GCONF.all_server_list));
  } else {
    bool split_end = false;
    ObAddr addr;
    ObString sub_string;
    ObString trimed_string;
    while (!split_end && OB_SUCCESS == ret) {
      sub_string.reset();
      trimed_string.reset();
      addr.reset();
      sub_string = all_server_list.split_on(',');
      if (sub_string.empty() && NULL == sub_string.ptr()) {
        split_end = true;
        sub_string = all_server_list;
      }
      trimed_string = sub_string.trim();
      if (trimed_string.empty()) {
        //nothing todo
      } else if (OB_FAIL(addr.parse_from_string(trimed_string))) {
        LOG_WARN("fail to parser addr from string", KR(ret), K(trimed_string));
      } else if (has_exist_in_array(excluded_server_list, addr)) {
        // nothing todo
      } else if (has_exist_in_array(config_all_server_list, addr)) {
        //nothing todo
      } else if (OB_FAIL(config_all_server_list.push_back(addr))) {
        LOG_WARN("fail to push back", KR(ret), K(addr));
      }
    } // end while
  }
  return ret;
}

int ObShareUtil::get_ora_rowscn(
    common::ObISQLClient &client,
    const uint64_t tenant_id,
    const ObSqlString &sql,
    SCN &ora_rowscn)
{
  int ret = OB_SUCCESS;
  uint64_t ora_rowscn_val = 0;
  ora_rowscn.set_invalid();
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    if (OB_FAIL(client.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sql result", KR(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next row", KR(ret));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "ORA_ROWSCN", ora_rowscn_val, int64_t);
      if (FAILEDx(ora_rowscn.convert_for_inner_table_field(ora_rowscn_val))) {
        LOG_WARN("fail to convert val to SCN", KR(ret), K(ora_rowscn_val));
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(ret)) {
      //nothing todo
    } else if (OB_ITER_END != (tmp_ret = result->next())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get more row than one", KR(ret), KR(tmp_ret));
    }
  }
  return ret;
}

bool ObShareUtil::is_tenant_enable_rebalance(const uint64_t tenant_id)
{
  bool bret = false;
  if (is_valid_tenant_id(tenant_id)) {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (OB_UNLIKELY(!tenant_config.is_valid())) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "tenant config is invalid", K(tenant_id));
    } else {
      bret = tenant_config->enable_rebalance;
    }
  }
  return bret;
}

bool ObShareUtil::is_tenant_enable_transfer(const uint64_t tenant_id)
{
  bool bret = false;
  if (!is_valid_tenant_id(tenant_id)) {
    bret = false;
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
     if (OB_UNLIKELY(!tenant_config.is_valid())) {
       LOG_WARN_RET(OB_ERR_UNEXPECTED, "tenant config is invalid", K(tenant_id));
     } else if (GCONF.in_upgrade_mode()) {
      bret = false;
      LOG_TRACE("in upgrade, transfer is not allowed", K(tenant_id), K(bret));
     } else {
      bret = tenant_config->enable_transfer;
      LOG_TRACE("show enable_transfer state", K(tenant_id), K(bret),
          "enable_transfer", tenant_config->enable_transfer);
     }
  }

  return bret;
}

int ObShareUtil::check_compat_data_version_(
  const uint64_t required_data_version,
  const bool check_meta_tenant,
  const bool check_user_tenant,
  const uint64_t tenant_id,
  bool &is_compatible)
{
  int ret = OB_SUCCESS;
  is_compatible = true;
  uint64_t data_version = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(0 == required_data_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(required_data_version));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, data_version))) {
    LOG_WARN("fail to get sys tenant data version", KR(ret));
  } else if (required_data_version > data_version) {
    is_compatible = false;
  } else if (!is_sys_tenant(tenant_id)) {
    if (check_meta_tenant) {
      if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id), data_version))) {
        LOG_WARN("fail to get meta tenant data version", KR(ret), "tenant_id", gen_meta_tenant_id(tenant_id));
      } else if (required_data_version > data_version) {
        is_compatible = false;
      }
    }
    if (OB_FAIL(ret) || !is_compatible) {
      // skip
    } else if (check_user_tenant) {
      if (OB_FAIL(GET_MIN_DATA_VERSION(gen_user_tenant_id(tenant_id), data_version))) {
        LOG_WARN("fail to get user tenant data version", KR(ret), "tenant_id", gen_user_tenant_id(tenant_id));
      } else if (required_data_version > data_version) {
        is_compatible = false;
      }
    }
  }
  return ret;
}

int ObShareUtil::check_compat_version_for_clone_standby_tenant(
    const uint64_t tenant_id,
    bool &is_compatible)
{
  int ret = OB_SUCCESS;
  is_compatible = false;
  uint64_t target_data_version = DATA_VERSION_4_3_2_0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_compat_data_version_(target_data_version,
                      true/*check_meta*/, true/*check_user*/, tenant_id, is_compatible))) {
    LOG_WARN("fail to check data version for clone tenant", KR(ret),
             K(tenant_id), K(target_data_version));
  }
  return ret;
}

int ObShareUtil::check_compat_version_for_clone_tenant(
    const uint64_t tenant_id,
    bool &is_compatible)
{
  int ret = OB_SUCCESS;
  is_compatible = false;
  uint64_t target_data_version = DATA_VERSION_4_3_0_0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_compat_data_version_(target_data_version,
                      true/*check_meta*/, true/*check_user*/, tenant_id, is_compatible))) {
    LOG_WARN("fail to check data version for clone tenant", KR(ret),
             K(tenant_id), K(target_data_version));
  }
  return ret;
}

int ObShareUtil::check_compat_version_for_clone_tenant_with_tenant_role(
    const uint64_t tenant_id,
    bool &is_compatible)
{
  int ret = OB_SUCCESS;
  is_compatible = false;
  ObAllTenantInfo all_tenant_info;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
                         tenant_id, GCTX.sql_proxy_,
                         false/*for_update*/, all_tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id), K(all_tenant_info));
  } else if (all_tenant_info.is_standby()) {
    if (OB_FAIL(check_compat_version_for_clone_standby_tenant(tenant_id, is_compatible))) {
      LOG_WARN("fail to check compatible version for standby tenant", KR(ret), K(tenant_id));
    }
  } else if (all_tenant_info.is_primary()) {
    if (OB_FAIL(check_compat_version_for_clone_tenant(tenant_id, is_compatible))) {
      LOG_WARN("fail to check compatible version for primary tenant", KR(ret), K(tenant_id));
    }
  } else {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can not clone tenant with tenant role is neither PRIMARY nor STANDBY",
              KR(ret), K(all_tenant_info));
  }
  return ret;
}

int ObShareUtil::mtl_get_tenant_role(const uint64_t tenant_id, ObTenantRole::Role &tenant_role)
{
  int ret = OB_SUCCESS;
  tenant_role = ObTenantRole::INVALID_TENANT;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    tenant_role = ObTenantRole::PRIMARY_TENANT;
  } else {
    MTL_SWITCH(tenant_id) {
      tenant_role = MTL_GET_TENANT_ROLE_CACHE();
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(is_invalid_tenant(tenant_role))) {
    ret = OB_NEED_WAIT;
    LOG_WARN("tenant role is not ready, need wait", KR(ret), K(tenant_id), K(tenant_role));
  }
  return ret;
}

int ObShareUtil::mtl_check_if_tenant_role_is_primary(const uint64_t tenant_id, bool &is_primary)
{
  int ret = OB_SUCCESS;
  is_primary = false;
  ObTenantRole::Role tenant_role;
  if (OB_FAIL(mtl_get_tenant_role(tenant_id, tenant_role))) {
    LOG_WARN("fail to execute mtl_get_tenant_role", KR(ret), K(tenant_id));
  } else if (is_primary_tenant(tenant_role)) {
    is_primary = true;
  }
  return ret;
}

int ObShareUtil::mtl_check_if_tenant_role_is_standby(const uint64_t tenant_id, bool &is_standby)
{
  int ret = OB_SUCCESS;
  is_standby = false;
  ObTenantRole::Role tenant_role;
  if (OB_FAIL(mtl_get_tenant_role(tenant_id, tenant_role))) {
    LOG_WARN("fail to execute mtl_get_tenant_role", KR(ret), K(tenant_id));
  } else if (is_standby_tenant(tenant_role)) {
    is_standby = true;
  }
  return ret;
}
int ObShareUtil::table_get_tenant_role(const uint64_t tenant_id, ObTenantRole &tenant_role)
{
  int ret = OB_SUCCESS;
  tenant_role.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    tenant_role = ObTenantRole::PRIMARY_TENANT;
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::get_tenant_role(GCTX.sql_proxy_, tenant_id, tenant_role))) {
    LOG_WARN("fail to get tenant role", KR(ret), KP(GCTX.sql_proxy_), K(tenant_id));
  } else if (tenant_role.is_invalid()) {
    ret = OB_NEED_WAIT;
    LOG_WARN("tenant role is not ready, need wait", KR(ret), K(tenant_role));
  }
  return ret;
}
int ObShareUtil::table_check_if_tenant_role_is_primary(const uint64_t tenant_id, bool &is_primary)
{
  int ret = OB_SUCCESS;
  share::ObTenantRole tenant_role;
  is_primary = false;
  if (OB_FAIL(table_get_tenant_role(tenant_id, tenant_role))) {
    LOG_WARN("fail to execute table_get_tenant_role", KR(ret), K(tenant_id));
  } else if (tenant_role.is_primary()) {
    is_primary = true;
  }
  return ret;
}
int ObShareUtil::table_check_if_tenant_role_is_standby(const uint64_t tenant_id, bool &is_standby)
{
  int ret = OB_SUCCESS;
  share::ObTenantRole tenant_role;
  is_standby = false;
  if (OB_FAIL(table_get_tenant_role(tenant_id, tenant_role))) {
    LOG_WARN("fail to execute table_get_tenant_role", KR(ret), K(tenant_id));
  } else if (tenant_role.is_standby()) {
    is_standby = true;
  }
  return ret;
}
int ObShareUtil::table_check_if_tenant_role_is_restore(const uint64_t tenant_id, bool &is_restore)
{
  int ret = OB_SUCCESS;
  share::ObTenantRole tenant_role;
  is_restore = false;
  if (OB_FAIL(table_get_tenant_role(tenant_id, tenant_role))) {
    LOG_WARN("fail to execute table_get_tenant_role", KR(ret), K(tenant_id));
  } else if (tenant_role.is_restore()) {
    is_restore = true;
  }
  return ret;
}
const char *ObShareUtil::replica_type_to_string(const ObReplicaType type)
{
  const char *str = NULL;
  switch (type) {
    case ObReplicaType::REPLICA_TYPE_FULL: {
      str = FULL_REPLICA_STR;
      break;
    }
    case ObReplicaType::REPLICA_TYPE_BACKUP: {
      str = BACKUP_REPLICA_STR;
      break;
    }
    case ObReplicaType::REPLICA_TYPE_LOGONLY: {
      str = LOGONLY_REPLICA_STR;
      break;
    }
    case ObReplicaType::REPLICA_TYPE_READONLY: {
      str = READONLY_REPLICA_STR;
      break;
    }
    case ObReplicaType::REPLICA_TYPE_MEMONLY: {
      str = MEMONLY_REPLICA_STR;
      break;
    }
    case ObReplicaType::REPLICA_TYPE_ENCRYPTION_LOGONLY: {
      str = ENCRYPTION_LOGONLY_REPLICA_STR;
      break;
    }
    case ObReplicaType::REPLICA_TYPE_COLUMNSTORE: {
      str = COLUMNSTORE_REPLICA_STR;
      break;
    }
    default: {
      str = "INVALID";
      break;
    }
  }
  return str;
}

// retrun REPLICA_TYPE_INVALID if str is invaild
ObReplicaType ObShareUtil::string_to_replica_type(const char *str)
{
  return string_to_replica_type(ObString(str));
}

// retrun REPLICA_TYPE_INVALID if str is invaild
ObReplicaType ObShareUtil::string_to_replica_type(const ObString &str)
{
  ObReplicaType replica_type = REPLICA_TYPE_INVALID;
  if (OB_UNLIKELY(str.empty())) {
    replica_type = REPLICA_TYPE_INVALID;
  } else if (0 == str.case_compare(FULL_REPLICA_STR) || 0 == str.case_compare(F_REPLICA_STR)) {
    replica_type = REPLICA_TYPE_FULL;
  } else if (0 == str.case_compare(READONLY_REPLICA_STR) || 0 == str.case_compare(R_REPLICA_STR)) {
    replica_type = REPLICA_TYPE_READONLY;
  } else if (0 == str.case_compare(COLUMNSTORE_REPLICA_STR) || 0 == str.case_compare(C_REPLICA_STR)) {
    replica_type = REPLICA_TYPE_COLUMNSTORE;
  } else if (0 == str.case_compare(LOGONLY_REPLICA_STR) || 0 == str.case_compare(L_REPLICA_STR)) {
    replica_type = REPLICA_TYPE_LOGONLY;
  } else if (0 == str.case_compare(ENCRYPTION_LOGONLY_REPLICA_STR) || 0 == str.case_compare(E_REPLICA_STR)) {
    replica_type = REPLICA_TYPE_ENCRYPTION_LOGONLY;
  } else if (0 == str.case_compare(BACKUP_REPLICA_STR) || 0 == str.case_compare(B_REPLICA_STR)) {
    replica_type = REPLICA_TYPE_BACKUP;
  } else if (0 == str.case_compare(MEMONLY_REPLICA_STR) || 0 == str.case_compare(M_REPLICA_STR)) {
    replica_type = REPLICA_TYPE_MEMONLY;
  } else {
    replica_type = REPLICA_TYPE_INVALID;
  }
  return replica_type;
}

} //end namespace share
} //end namespace oceanbase
