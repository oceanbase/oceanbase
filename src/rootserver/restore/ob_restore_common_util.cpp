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

#define USING_LOG_PREFIX RS_RESTORE

#include "ob_restore_common_util.h"
#include "share/ls/ob_ls_status_operator.h" //ObLSStatusOperator
#include "share/ls/ob_ls_operator.h"//ObLSAttr
#include "rootserver/ob_ls_service_helper.h"
#include "rootserver/ob_tenant_role_transition_service.h"
#include "src/share/ob_schema_status_proxy.h"
#include "src/share/ob_rpc_struct.h"
#include "rootserver/ob_ddl_service.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif

using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;
using namespace oceanbase::common;

int ObRestoreCommonUtil::notify_root_key(
    obrpc::ObSrvRpcProxy *srv_rpc_proxy_,
    common::ObMySQLProxy *sql_proxy_,
    const uint64_t tenant_id,
    const share::ObRootKey &root_key)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(obrpc::RootKeyType::INVALID == root_key.key_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid root_key", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(srv_rpc_proxy_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null svr rpc proxy or sql proxy", KR(ret),
                                    KP(srv_rpc_proxy_), KP(sql_proxy_));
  } else {
    obrpc::ObRootKeyArg arg;
    obrpc::ObRootKeyResult result;
    ObUnitTableOperator unit_operator;
    ObArray<ObUnit> units;
    ObArray<ObAddr> addrs;
    arg.tenant_id_ = tenant_id;
    arg.is_set_ = true;
    arg.key_type_ = root_key.key_type_;
    arg.root_key_ = root_key.key_;
    if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
      LOG_WARN("failed to init unit operator", KR(ret));
    } else if (OB_FAIL(unit_operator.get_units_by_tenant(tenant_id, units))) {
      LOG_WARN("failed to get tenant unit", KR(ret), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); i++) {
      const ObUnit &unit = units.at(i);
      if (OB_FAIL(addrs.push_back(unit.server_))) {
        LOG_WARN("failed to push back addr", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLService::notify_root_key(*srv_rpc_proxy_, arg, addrs, result))) {
      LOG_WARN("failed to notify root key", KR(ret));
    }
  }
#endif
  return ret;
}

int ObRestoreCommonUtil::create_all_ls(
    common::ObMySQLProxy *sql_proxy,
    const uint64_t tenant_id,
    const share::schema::ObTenantSchema &tenant_schema,
    const common::ObIArray<share::ObLSAttr> &ls_attr_array,
    const uint64_t source_tenant_id)
{
  int ret = OB_SUCCESS;
  ObLSStatusOperator status_op;
  ObLSStatusInfo status_info;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id)
                  || !tenant_schema.is_valid()
                  || ls_attr_array.empty()
                  || OB_ISNULL(sql_proxy))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_schema),
                                              K(ls_attr_array), KP(sql_proxy));
  } else {
    common::ObMySQLTransaction trans;
    const int64_t exec_tenant_id = ObLSLifeIAgent::get_exec_tenant_id(tenant_id);

    if (OB_FAIL(trans.start(sql_proxy, exec_tenant_id))) {
      LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id));
    } else {
      //must be in trans
      //Multiple LS groups will be created here.
      //In order to ensure that each LS group can be evenly distributed in the unit group,
      //it is necessary to read the distribution of LS groups within the transaction.
      ObTenantLSInfo tenant_stat(sql_proxy, &tenant_schema, tenant_id, &trans);
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_attr_array.count(); ++i) {
        const ObLSAttr &ls_info = ls_attr_array.at(i);
        ObLSFlag ls_flag = ls_info.get_ls_flag();
        if (ls_info.get_ls_id().is_sys_ls()) {
        } else if (OB_SUCC(status_op.get_ls_status_info(tenant_id, ls_info.get_ls_id(),
                status_info, trans))) {
          LOG_INFO("[RESTORE] ls already exist", K(ls_info), K(tenant_id));
        } else if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to get ls status info", KR(ret), K(tenant_id), K(ls_info));
        } else if (OB_FAIL(ObLSServiceHelper::create_new_ls_in_trans(
                   ls_info.get_ls_id(), ls_info.get_ls_group_id(), ls_info.get_create_scn(),
                   share::NORMAL_SWITCHOVER_STATUS, tenant_stat, trans, ls_flag, source_tenant_id))) {
          LOG_WARN("failed to add new ls status info", KR(ret), K(ls_info), K(source_tenant_id));
        }
        LOG_INFO("create init ls", KR(ret), K(ls_info), K(source_tenant_id));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", KR(ret), KR(tmp_ret));
    }
  }
  return ret;
}

int ObRestoreCommonUtil::finish_create_ls(
    common::ObMySQLProxy *sql_proxy,
    const share::schema::ObTenantSchema &tenant_schema,
    const common::ObIArray<share::ObLSAttr> &ls_attr_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tenant_schema.is_valid()
                  || OB_ISNULL(sql_proxy))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_schema), KP(sql_proxy));
  } else {
    const uint64_t tenant_id = tenant_schema.get_tenant_id();
    const int64_t exec_tenant_id = ObLSLifeIAgent::get_exec_tenant_id(tenant_id);
    common::ObMySQLTransaction trans;
    ObLSStatusOperator status_op;
    ObLSStatusInfoArray ls_array;
    ObLSStatus ls_info = share::OB_LS_EMPTY;//ls status in __all_ls
    if (OB_FAIL(status_op.get_all_ls_status_by_order(tenant_id, ls_array,
                                                     *sql_proxy))) {
      LOG_WARN("failed to get all ls status", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(sql_proxy, exec_tenant_id))) {
      LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_array.count(); ++i) {
        const ObLSStatusInfo &status_info = ls_array.at(i);
        if (OB_UNLIKELY(status_info.ls_is_creating())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls should be created", KR(ret), K(status_info));
        } else {
          ret = OB_ENTRY_NOT_EXIST;
          for (int64_t j = 0; OB_ENTRY_NOT_EXIST == ret && j < ls_attr_array.count(); ++j) {
            if (ls_attr_array.at(i).get_ls_id() == status_info.ls_id_) {
              ret = OB_SUCCESS;
              ls_info = ls_attr_array.at(i).get_ls_status();
            }
          }
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to find ls in attr", KR(ret), K(status_info), K(ls_attr_array));
          } else if (share::OB_LS_CREATING == ls_info) {
            //no need to update
          } else if (ls_info == status_info.status_) {
            //no need update
          } else if (OB_FAIL(status_op.update_ls_status_in_trans(
                  tenant_id, status_info.ls_id_, status_info.status_,
                  ls_info, share::NORMAL_SWITCHOVER_STATUS, trans))) {
            LOG_WARN("failed to update status", KR(ret), K(tenant_id), K(status_info), K(ls_info));
          } else {
            LOG_INFO("[RESTORE] update ls status", K(tenant_id), K(status_info), K(ls_info));
          }
        }
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", KR(ret), KR(tmp_ret));
    }
  }
  return ret;
}

int ObRestoreCommonUtil::try_update_tenant_role(common::ObMySQLProxy *sql_proxy,
                                                const uint64_t tenant_id,
                                                const share::SCN &restore_scn,
                                                const bool is_clone,
                                                bool &sync_satisfied)
{
  int ret = OB_SUCCESS;
  sync_satisfied = true;
  ObAllTenantInfo all_tenant_info;
  int64_t new_switch_ts = 0;
  bool need_update = false;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id)
                  || OB_ISNULL(sql_proxy)
                  || OB_ISNULL(GCTX.srv_rpc_proxy_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not user tenant or proxy is null", KR(ret), K(tenant_id),
                          KP(sql_proxy), KP(GCTX.srv_rpc_proxy_));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, sql_proxy,
          false/*for_update*/, all_tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id));
  } else if (!is_clone && all_tenant_info.is_restore()) {
    need_update = true;
  } else if (is_clone && all_tenant_info.is_clone()) {
    need_update = true;
  }

  if (OB_SUCC(ret) && need_update) {
    //update tenant role to standby tenant
    if (all_tenant_info.get_sync_scn() != restore_scn) {
      sync_satisfied = false;
      LOG_WARN("tenant sync scn not equal to restore scn", KR(ret),
                      K(all_tenant_info), K(restore_scn));
    } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role(
            tenant_id, sql_proxy, all_tenant_info.get_switchover_epoch(),
            share::STANDBY_TENANT_ROLE, all_tenant_info.get_switchover_status(),
            share::NORMAL_SWITCHOVER_STATUS, new_switch_ts))) {
      LOG_WARN("failed to update tenant role", KR(ret), K(tenant_id), K(all_tenant_info));
    } else {
      ObTenantRoleTransitionService role_transition_service(tenant_id, sql_proxy,
                                GCTX.srv_rpc_proxy_, obrpc::ObSwitchTenantArg::OpType::INVALID);
      (void)role_transition_service.broadcast_tenant_info(
            ObTenantRoleTransitionConstants::RESTORE_TO_STANDBY_LOG_MOD_STR);
    }
  }
  return ret;
}

int ObRestoreCommonUtil::process_schema(common::ObMySQLProxy *sql_proxy,
                                        const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy));
  } else {
    //reset schema status
    ObSchemaStatusProxy proxy(*sql_proxy);
    ObRefreshSchemaStatus schema_status(tenant_id, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
    if (OB_FAIL(proxy.init())) {
      LOG_WARN("failed to init schema proxy", KR(ret));
    } else if (OB_FAIL(proxy.set_tenant_schema_status(schema_status))) {
      LOG_WARN("failed to update schema status", KR(ret), K(schema_status));
    }
  }

  if (OB_SUCC(ret)) {
    obrpc::ObBroadcastSchemaArg arg;
    arg.tenant_id_ = tenant_id;
    if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rs_rpc_proxy_ or rs_mgr_is null", KR(ret), KP(GCTX.rs_rpc_proxy_), KP(GCTX.rs_mgr_));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to_rs(*GCTX.rs_mgr_).broadcast_schema(arg))) {
      LOG_WARN("failed to broadcast schema", KR(ret), K(arg));
    }
  }

  return ret;
}

int ObRestoreCommonUtil::check_tenant_is_existed(ObMultiVersionSchemaService *schema_service,
                                                 const uint64_t tenant_id,
                                                 bool &is_existed)
{
  int ret = OB_SUCCESS;
  is_existed = true;
  ObSchemaGetterGuard schema_guard;
  bool tenant_dropped = false;

  if (OB_INVALID_TENANT_ID == tenant_id) {
    //maybe failed to create tenant
    is_existed = false;
    LOG_INFO("tenant maybe failed to create", KR(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret));
  } else if (OB_SUCCESS != schema_guard.check_formal_guard()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("failed to check formal gurad", KR(ret));
  } else if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(tenant_id, tenant_dropped))) {
    LOG_WARN("failed to check tenant is beed dropped", KR(ret), K(tenant_id));
  } else if (tenant_dropped) {
    is_existed = false;
    LOG_INFO("restore tenant has been dropped", KR(ret), K(tenant_id));
  } else {
    //check restore tenant's meta tenant is valid to read
    const share::schema::ObTenantSchema *tenant_schema = NULL;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_FAIL(schema_guard.get_tenant_info(meta_tenant_id, tenant_schema))) {
      LOG_WARN("failed to get tenant info", KR(ret), K(meta_tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", KR(ret), K(meta_tenant_id));
    } else if (tenant_schema->is_normal()) {
      is_existed = true;
    } else {
      //other status cannot get result from meta
      is_existed = false;
      LOG_WARN("meta tenant of restore tenant not normal", KR(ret), KPC(tenant_schema));
    }
  }
  return ret;
}

int ObRestoreCommonUtil::set_tde_parameters(common::ObMySQLProxy *sql_proxy,
                                            obrpc::ObCommonRpcProxy *rpc_proxy,
                                            const uint64_t tenant_id,
                                            const ObString &tde_method,
                                            const ObString &kms_info)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  ObSqlString sql;
  int64_t affected_row = 0;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id)
                  || !ObTdeMethodUtil::is_valid(tde_method)
                  || NULL == sql_proxy
                  || NULL == rpc_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tde_method), KP(sql_proxy), KP(rpc_proxy));
  } else if (OB_FAIL(sql.assign_fmt("ALTER SYSTEM SET tde_method = '%.*s'",
                                                    tde_method.length(), tde_method.ptr()))) {
    LOG_WARN("failed to assign fmt", KR(ret), K(tde_method));
  } else if (OB_FAIL(sql_proxy->write(tenant_id, sql.ptr(), affected_row))) {
    LOG_WARN("failed to execute", KR(ret), K(tenant_id), K(sql));
  } else if (ObTdeMethodUtil::is_internal(tde_method)) {
    // do nothing
  } else if (FALSE_IT(sql.reset())) {
  } else if (OB_UNLIKELY(kms_info.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("kms_info should not be empty", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("ALTER SYSTEM SET external_kms_info= '%.*s'",
                                                    kms_info.length(), kms_info.ptr()))) {
    LOG_WARN("failed to assign fmt", KR(ret));
  } else if (OB_FAIL(sql_proxy->write(tenant_id, sql.ptr(), affected_row))) {
    LOG_WARN("failed to execute", KR(ret), K(tenant_id));
  }
  if (OB_SUCC(ret)) {
    const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
    obrpc::ObReloadMasterKeyArg arg;
    obrpc::ObReloadMasterKeyResult result;
    arg.tenant_id_ = tenant_id;
    if (OB_FAIL(rpc_proxy->timeout(DEFAULT_TIMEOUT).reload_master_key(arg, result))) {
      LOG_WARN("fail to reload master key", KR(ret), K(arg), K(DEFAULT_TIMEOUT));
    } else if (result.master_key_id_ > 0 ) {
      bool is_active = false;
      const int64_t SLEEP_US = 5 * 1000 * 1000L; // 5s
      const int64_t MAX_WAIT_US = 60 * 1000 * 1000L; // 60s
      const int64_t start = ObTimeUtility::current_time();
      char master_key[OB_MAX_MASTER_KEY_LENGTH] = {'\0'};
      int64_t master_key_len = 0;
      uint64_t master_key_id = 0;
      while (OB_SUCC(ret) && !is_active) {
        if (ObTimeUtility::current_time() - start > MAX_WAIT_US) {
          ret = OB_TIMEOUT;
          LOG_WARN("use too much time", KR(ret), "cost_us", ObTimeUtility::current_time() - start);
        } else if (OB_FAIL(ObMasterKeyGetter::get_active_master_key(tenant_id, master_key,
                                                                OB_MAX_MASTER_KEY_LENGTH,
                                                                master_key_len, master_key_id))) {
          if (OB_KEYSTORE_OPEN_NO_MASTER_KEY == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("master key is not active, need wait", K(tenant_id));
            usleep(SLEEP_US);
          } else {
            LOG_WARN("fail to get active master key", KR(ret), K(tenant_id));
          }
        } else {
          is_active = true;
        }
      }
    }
  }
#endif
  return ret;
}
