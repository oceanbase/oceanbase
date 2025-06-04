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
#define USING_LOG_PREFIX RS

#include "src/rootserver/ob_tenant_parallel_create_executor.h"

#include "rootserver/ob_root_service.h"
#include "rootserver/ob_tenant_ddl_service.h"
#include "rootserver/ob_ddl_service.h"
#include "share/ls/ob_ls_life_manager.h"
#include "share/ls/ob_ls_creator.h"
#include "share/backup/ob_backup_config.h"
#include "share/location_cache/ob_location_service.h"
#include "rootserver/restore/ob_tenant_clone_util.h"
#include "share/ob_primary_zone_util.h"
#include "rootserver/ob_tenant_thread_helper.h"
#include "share/ls/ob_ls_operator.h"
#include "share/inner_table/ob_sslog_table_schema.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_table_creator.h"

namespace oceanbase
{
using namespace obrpc;
using namespace share;
namespace rootserver
{

/* The reconstruction of zone_list follows the following rules:
 * 1. When creating tenant, the zone_list specified by the user will be ignored. Use the zone list of resource_pool.
 *  We still retain the zone_list column in the all_tenant table and the zone_list field in the tenantSchema.
 * 2. When creating table, the zone_list specified by the user will be ignored.
 *  If the create table does not specify locality, the locality of the table is not filled in,
 *  and the zone_list is not filled in.
 *  Both locality and zone_list are inherited from the tenant to which the table belongs.
 * 3. When creating table, the user specified zone_list and locality, the user-specified zone_list will be ignored;
 *  a zone list will be calculated based on the user-specified locality and the zone list of the resource pool,
 *  and the zone_list will be filled into the zone_list column of __all_table and in the zone_list field of TableSchema.
 * 4. When modifying locality, we are not allowed to modify F{3}@region_hz to F{2}@region_hz;
 *  because the semantics of this modification is not clear enough,
 *  it is impossible to clearly derive the change of zone
 */
int ObParallelCreateTenantExecutor::execute(obrpc::UInt64 &tenant_id)
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  TIMEGUARD_INIT(create_tenant, 15_s);
  FLOG_INFO("[CREATE_TENANT] start create tenant", K(create_tenant_arg_));
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("create tenant is not allowed in upgrade mode", KR(ret), K(GCONF.in_upgrade_mode()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "in upgrade mode, CREATE TENANT is");
    // 1. create tenant schema
  } else if (CLICK_FAIL(common_rpc_->to_rs(*rs_mgr_).timeout(ctx_.get_timeout())
        .create_tenant(create_tenant_arg_, create_tenant_schema_result_))) {
    LOG_WARN("failed to create tenant schema", KR(ret), K(create_tenant_arg_), K(ctx_));
  } else if (create_tenant_schema_result_.tenant_exist_) {
    if (!create_tenant_arg_.if_not_exist_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_exist without if_not_exist_, the rpc create_tenant_schema should fail",
          KR(ret), K(create_tenant_arg_), K(create_tenant_schema_result_));
    } else {
      LOG_INFO("tenant exists, not need to create tenant", KR(ret));
    }
  } else if (OB_FAIL(init_after_create_tenant_schema_())) {
    LOG_WARN("failed to init after create tenant schema", KR(ret));
    // 2. create tenant sys ls
  } else if (CLICK_FAIL(create_tenant_sys_ls_())) {
    LOG_WARN("failed to create tenant sys ls", KR(ret));
  } else {
    tenant_id = user_tenant_schema_.get_tenant_id();
    ObParallelCreateNormalTenantProxy proxy(*common_rpc_, &ObCommonRpcProxy::parallel_create_normal_tenant);
    // 3. call create normal tenant
    if (OB_FAIL(call_create_normal_tenant_(proxy))) {
      LOG_WARN("faled to async call create normal tenant", KR(ret));
    // 4. create user ls
    } else if (CLICK_FAIL(create_user_ls_(proxy))) {
      if (OB_CANCELED == ret) {
        LOG_WARN("previous error occured, create user ls is canceled", KR(ret));
      } else {
        LOG_WARN("failed to wait and init user tenant", KR(ret));
      }
    }
    // 5. wait all
    if (CLICK_TMP_FAIL(wait_all_(proxy, ret))) {
      LOG_WARN("failed to wait all", KR(ret), KR(tmp_ret));
      ret = (OB_SUCC(ret) || ret == OB_CANCELED) ? tmp_ret : ret;
    }
  }
  // 6. finish create tenant
  if (CLICK_TMP_FAIL(finish_create_tenant_(ret))) {
    LOG_WARN("failed to finish create tenant", KR(tmp_ret), KR(ret));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }
  FLOG_INFO("[CREATE_TENANT] finish create tenant", KR(ret), K(tenant_id), K(create_tenant_arg_),
      "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

bool ObParallelCreateTenantExecutor::async_rpc_has_error(ObParallelCreateNormalTenantProxy &proxy)
{
  return proxy.check_has_error_result();
}

int ObParallelCreateTenantExecutor::wait_all_(
    ObParallelCreateNormalTenantProxy &proxy,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  ObArray<int> return_ret_array;
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  DEBUG_SYNC(BEFORE_CHECK_CREATE_NORMAL_TENANT_RESULT);
  if (OB_FAIL(proxy.wait_all(return_ret_array))) {
    LOG_WARN("failed to wait all create normal tenant rpc", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(proxy.check_return_cnt(return_ret_array.count()))) {
    LOG_WARN("return cnt not match", KR(ret));
  } else {
    for (int64_t i = 0; i < return_ret_array.count(); i++) {
      int return_ret = return_ret_array.at(i);
      const ObAddr &addr = proxy.get_dests().at(i);
      if (OB_SUCCESS != return_ret) {
        ret = OB_FAIL(ret) ? ret :return_ret;
        LOG_WARN("rpc return error", KR(return_ret), K(addr));
      }
    } // end for
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ret_code)) {
    } else if (OB_FAIL(wait_ls_leader_(user_tenant_id, SYS_LS, true/*force_renew*/))) {
      LOG_WARN("failed to wait user sys ls exists in meta table", KR(ret), K(user_tenant_id));
      // To avoid load_sys_package and tenant DDL operations competing for DDL threads
      // wait for all load_sys_package tasks to complete before returning to the user.
      // ObCompatibilityMode::OCEANBASE_MODE means wait both mysql and oracle sys package
    } else if (OB_FAIL(ObLoadSysPackageTask::wait_sys_package_ready(*sql_proxy_, ctx_,
            ObCompatibilityMode::OCEANBASE_MODE))) {
      LOG_WARN("failed to wait sys package ready", KR(ret), K(ctx_));
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::construct_tenant_info_(const uint64_t tenant_id,
    ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  const SCN recovery_until_scn = get_recovery_until_scn_();
  const ObTenantRole tenant_role = create_tenant_arg_.get_tenant_role();
  if (is_user_tenant(tenant_id) && OB_FAIL(tenant_info.init(tenant_id, tenant_role,
            NORMAL_SWITCHOVER_STATUS, 0, SCN::base_scn(), SCN::base_scn(), SCN::base_scn(),
            recovery_until_scn))) {
    LOG_WARN("failed to init tenant_info", KR(ret), K(tenant_id), K(tenant_role), K(recovery_until_scn));
  } else if (is_meta_tenant(tenant_id) && OB_FAIL(tenant_info.init(tenant_id, PRIMARY_TENANT_ROLE))) {
    LOG_WARN("failed to init tenant_info", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::init_after_create_tenant_schema_()
{
  // create_tenant_schema_result_ should be set
  int ret = OB_SUCCESS;
  if (!create_tenant_schema_result_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create tenant schema result is invalid", KR(ret), K(create_tenant_schema_result_));
  } else {
    if (OB_FAIL(get_tenant_schema_from_inner_table_())) {
      LOG_WARN("failed to get tenant_schema", KR(ret));
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::create_tenant_sys_ls_()
{
  int ret = OB_SUCCESS;
  palf::PalfBaseInfo meta_palf_base_info;
  ObAllTenantInfo tenant_info;
  ObArray<share::ObResourcePoolName> pools;
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  const uint64_t meta_tenant_id = meta_tenant_schema_.get_tenant_id();
  if (OB_FAIL(construct_tenant_info_(meta_tenant_id, tenant_info))) {
    LOG_WARN("failed to get tenant_info", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(ObTenantDDLService::get_pools(create_tenant_arg_.pool_list_, pools))) {
    LOG_WARN("failed to init pools", KR(ret), K(create_tenant_arg_.pool_list_));
  } else if (GCTX.is_shared_storage_mode()) {
    FLOG_INFO("[CREATE_TENANT] create sslog log stream", K(meta_tenant_id));
    if (OB_FAIL(create_tenant_sys_ls_(meta_tenant_schema_, pools, false /*create_ls_with_palf*/,
          meta_palf_base_info, OB_INVALID_TENANT_ID/*source_tenant_id_to_use*/, tenant_info, true /*is_sslog*/))) {
      LOG_WARN("failed to create meta tenant sys ls", KR(ret), K(meta_tenant_schema_),
        K(pools), K(meta_palf_base_info), K(create_tenant_arg_));
    } else if (OB_FAIL(wait_ls_leader_(meta_tenant_id, SSLOG_LS))) {
      LOG_WARN("failed to wait ls leader", KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(create_sslog_tablet_())) {
      LOG_WARN("failed to create sslog tablet", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(create_tenant_sys_ls_(meta_tenant_schema_, pools, false /*create_ls_with_palf*/,
          meta_palf_base_info, create_tenant_arg_.source_tenant_id_, tenant_info))) {
    LOG_WARN("failed to create meta tenant sys ls", KR(ret), K(meta_tenant_schema_),
        K(pools), K(meta_palf_base_info), K(create_tenant_arg_));
  } else if (OB_FAIL(construct_tenant_info_(user_tenant_id, tenant_info))) {
    LOG_WARN("failed to get tenant_info", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(create_tenant_sys_ls_(user_tenant_schema_, pools, get_create_ls_with_palf_(),
            get_palf_base_info_(), create_tenant_arg_.source_tenant_id_, tenant_info))) {
    LOG_WARN("failed to create user tenant sys ls", KR(ret), K(user_tenant_schema_), K(pools),
        K(create_tenant_arg_));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::create_user_ls_(ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_CREATE_TENANT_FILL_USER_LS_INFO);
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  FLOG_INFO("[CREATE_TENANT] start to create user ls", K(user_tenant_id));
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (create_tenant_arg_.get_tenant_role().is_primary()) {
    // create primary tenant
    if (OB_FAIL(check_can_create_user_ls_(proxy))) {
      LOG_WARN("failed to check user tenant table writable", KR(ret), K(user_tenant_id));
    } else if (OB_FAIL(create_tenant_user_ls_(proxy))) {
      LOG_WARN("failed to create tenant user ls", KR(ret), K(user_tenant_id));
    }
  }
  FLOG_INFO("[CREATE_TENANT] finish create user ls", KR(ret), K(user_tenant_id));
  return ret;
}

int ObParallelCreateTenantExecutor::create_tenant_user_ls_(ObParallelCreateNormalTenantProxy &proxy)
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("variable is not init", KR(ret));
  } else {
    ObAddr leader;
    int64_t tmp_ret = OB_SUCCESS;
    //ignore failed
    const int64_t retry_interval_us = 200l * 1000l; // 200ms
    while(OB_SUCC(ret)) {
      const int64_t timeout = ctx_.get_timeout();
      if (ctx_.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("create user ls timeout", KR(ret));
      } else if (OB_TMP_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id, user_tenant_id, SYS_LS, FALSE, leader))) {
        LOG_WARN("failed to get leader", KR(ret), KR(tmp_ret), K(user_tenant_id));
      } else if (OB_TMP_FAIL(rpc_proxy_->to(leader).timeout(timeout)
            .notify_create_tenant_user_ls(user_tenant_id))) {
        LOG_WARN("failed to create tenant user ls", KR(ret), KR(tmp_ret), K(user_tenant_id), K(leader), K(ctx_));
      } else {
        break;
      }
      if (OB_FAIL(ret)) {
        // previous async rpc create_normal_tenant may have error, this function should fail fast
      } else if (async_rpc_has_error(proxy)) {
        ret = OB_CANCELED;
        LOG_WARN("create normal tenant RPC are returned, create user ls is canceled", KR(ret),
            K(user_tenant_id));
      } else {
        ob_usleep(retry_interval_us);
      }
    }
  }

  return ret;
}

int ObParallelCreateTenantExecutor::call_create_normal_tenant_(
    const ObTenantSchema &tenant_schema,
    const bool async_call,
    ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (OB_FAIL(async_call_create_normal_tenant_(tenant_schema, proxy))) {
    LOG_WARN("failed to call create normal tenant", KR(ret), K(tenant_schema));
  } else if (!async_call) {
    FLOG_INFO("[CREATE_TENANT] tenants will be created in sync mode", K(tenant_id), K(async_call));
    if (OB_FAIL(proxy.wait())) {
      LOG_WARN("failed to create tenant", KR(ret), K(tenant_schema));
    } else {
      proxy.reuse();
    }
  } else {
    FLOG_INFO("[CREATE_TENANT] tenants will be created in async mode", K(tenant_id), K(async_call));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::call_create_normal_tenant_(
    ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  const bool async_call = GCONF._enable_parallel_tenant_creation;
  if (OB_FAIL(call_create_normal_tenant_(meta_tenant_schema_, async_call, proxy))) {
    LOG_WARN("failed to call create normal tenant for meta tenant", KR(ret), K(meta_tenant_schema_),
        K(async_call));
  } else if (OB_FAIL(call_create_normal_tenant_(user_tenant_schema_, async_call, proxy))) {
    LOG_WARN("failed to call create normal tenant for user tenant", KR(ret), K(user_tenant_schema_),
        K(async_call));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::async_call_create_normal_tenant_(
    const ObTenantSchema &tenant_schema,
    ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  ObParallelCreateNormalTenantArg arg;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (is_user_tenant(tenant_id)) {
    DEBUG_SYNC(BEFORE_CREATE_USER_NORMAL_TENANT);
  } else if (is_meta_tenant(tenant_id)) {
    DEBUG_SYNC(BEFORE_CREATE_META_NORMAL_TENANT);
  }
  if (OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is NULL", KR(ret), KP(GCTX.rs_mgr_));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check_inner_stat_", KR(ret));
  } else if (!tenant_schema.is_creating()) {
    // non primary tenant do not need to create normal tenant
  } else if (OB_FAIL(wait_ls_leader_(tenant_id, SYS_LS))) {
    LOG_WARN("failed to wait ls leader", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(addr))) {
    LOG_WARN("failed to get rs addr", KR(ret));
  } else if (OB_FAIL(arg.init(create_tenant_arg_, tenant_id))) {
    LOG_WARN("failed to init ObParallelCreateNormalTenantArg", KR(ret), K(create_tenant_arg_),
        K(tenant_id));
  } else if (OB_FAIL(proxy.call(addr, ctx_.get_timeout(), OB_SYS_TENANT_ID, arg))) {
    LOG_WARN("failed to call parallel create normal tenant", KR(ret), K(addr), K(ctx_), K(arg));
  }
  LOG_INFO("call create_normal_tenant", KR(ret), K(arg));
  return ret;
}

int ObParallelCreateTenantExecutor::create_sslog_tablet_()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("start creating sslog table");
  const uint64_t tenant_id = meta_tenant_schema_.get_tenant_id();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is NULL", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    ObMySQLTransaction trans;
    ObTableSchema table_schema;
    ObTableCreator table_creator(tenant_id, SCN::base_scn(), trans);
    common::ObArray<const share::schema::ObTableSchema*> table_schema_ptrs;
    common::ObArray<share::ObLSID> ls_id_array;
    common::ObArray<bool> need_create_empty_majors;
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, tenant_id))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (OB_FAIL(table_creator.init(false))) {
      LOG_WARN("fail to init tablet creator", KR(ret));
    } else if (OB_FAIL(ObSSlogTableSchema::all_sslog_table_schema(table_schema))) {
      LOG_WARN("fail to get schema", KR(ret));
    } else if (OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(tenant_id, table_schema))) {
      LOG_WARN("fail to construct tenant space table", KR(ret), K(tenant_id));
    } else if (OB_FAIL(table_schema_ptrs.push_back(&table_schema))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(ls_id_array.push_back(SSLOG_LS))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(need_create_empty_majors.push_back(true))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(table_creator.add_create_tablets_of_tables_arg(
                                table_schema_ptrs,
                                ls_id_array,
                                DATA_CURRENT_VERSION,
                                need_create_empty_majors/*need_create_empty_major_sstable*/))) {
      LOG_WARN("fail to add create tablet arg", KR(ret));
    } else if (OB_FAIL(table_creator.execute())) {
      LOG_WARN("execute create partition failed", K(ret));
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      bool commit = OB_SUCC(ret);
      if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed", K(commit), K(temp_ret));
      }
    }
  }
  FLOG_INFO("finish creating sslog table", KR(ret));
  return ret;
}

int ObParallelCreateTenantExecutor::create_tenant_sys_ls_(
    const ObTenantSchema &tenant_schema,
    const ObIArray<share::ObResourcePoolName> &pool_list,
    const bool create_ls_with_palf,
    const palf::PalfBaseInfo &palf_base_info,
    const uint64_t source_tenant_id,
    const ObAllTenantInfo &tenant_info,
    const bool is_sslog)
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  const ObLSID ls_id = is_sslog ? SSLOG_LS : SYS_LS;
  FLOG_INFO("[CREATE_TENANT] STEP 2.1. start create sys log stream", K(tenant_schema), K(source_tenant_id), K(ls_id));
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  // meta tenant do not have to reference source tenant id
  const uint64_t source_tenant_id_to_use = is_user_tenant(tenant_id) ? source_tenant_id : OB_INVALID_TENANT_ID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (is_sys_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K(tenant_id));
  } else {
    ObArray<share::ObZoneReplicaAttrSet> locality;
    ObZone primary_zone;
    ObSqlString zone_priority;
    share::schema::ObSchemaGetterGuard schema_guard; // not used
    int64_t paxos_replica_num = OB_INVALID_ID;
    ObLSCreator ls_creator(*rpc_proxy_, tenant_id, ls_id, sql_proxy_);
    if (OB_FAIL(tenant_schema.get_zone_replica_attr_array(locality))) {
      LOG_WARN("fail to get tenant's locality", KR(ret), K(locality));
    } else if (OB_FAIL(tenant_schema.get_paxos_replica_num(schema_guard, paxos_replica_num))) {
      LOG_WARN("failed to get paxos replica num", KR(ret));
    } else if (OB_FAIL(ObTenantDDLService::get_tenant_zone_priority(tenant_schema, primary_zone,
            zone_priority))) {
      LOG_WARN("failed to get tenant zone priority", KR(ret), K(tenant_schema));
    } else if (OB_FAIL(ls_creator.create_tenant_sys_ls(
               primary_zone, locality, pool_list, paxos_replica_num,
               tenant_schema.get_compatibility_mode(), zone_priority.string(),
               create_ls_with_palf, palf_base_info, source_tenant_id_to_use, tenant_info))) {
      LOG_WARN("fail to create tenant sys ls", KR(ret), K(pool_list), K(palf_base_info),
               K(locality), K(paxos_replica_num), K(tenant_schema), K(zone_priority), K(source_tenant_id_to_use));
    }
  }
  if (is_sslog) {
  } else if (is_meta_tenant(tenant_id)) {
    DEBUG_SYNC(AFTER_CREATE_META_TENANT_SYS_LOGSTREAM);
  } else {
    DEBUG_SYNC(AFTER_CREATE_USER_TENANT_SYS_LOGSTREAM);
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2.1. finish create sys log stream", KR(ret), K(tenant_schema), K(ls_id),
           "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

int ObParallelCreateTenantExecutor::wait_ls_leader_(const uint64_t tenant_id, const ObLSID &ls_id, const bool force_renew)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t wait_leader = 0;
  // ignore LOG_USER_ERROR when trying to read from __all_ls_meta_table
  ObWarningBufferIgnoreScope ignore_errors_in_warning_buffer;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else {
    int64_t wait_leader_start = ObTimeUtility::current_time();
    bool is_cache_hit = false;
    ObLSLocation location;
    ObAddr leader;
    const uint64_t expire_renew_time = force_renew ? INT64_MAX : 0;
    const int64_t retry_interval_us = 200l * 1000l; // 200ms
    bool first_round = true;
    while (OB_SUCC(ret)) {
      if (ctx_.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait ls leader elect timeout", KR(ret), K(tenant_id), K(ctx_));
      } else if (!first_round && OB_TMP_FAIL(location_service_->renew_all_ls_locations_by_rpc())) {
        LOG_WARN("failed to renew all ls locations by rpc", KR(tmp_ret));
      } else if (OB_TMP_FAIL(location_service_->get(GCONF.cluster_id, tenant_id,
        ls_id, expire_renew_time, is_cache_hit, location))) {
        LOG_WARN("failed to get ls location", KR(tmp_ret), K(tenant_id));
      } else if (location.is_valid()) {
        if (OB_TMP_FAIL(location.get_leader(leader))) {
          LOG_WARN("failed to get leader from location cache", KR(ret), K(location));
        } else if (!leader.is_valid()) {
        } else {
          break;
        }
      }
      if (OB_SUCC(ret)) {
        ob_usleep(retry_interval_us);
      }
      first_round = false;
    }
    int64_t wait_leader_end = ObTimeUtility::current_time();
    wait_leader = wait_leader_end - wait_leader_start;
    LOG_INFO("wait tenant ls elect", KR(ret), K(tenant_id), K(leader), "cost", wait_leader);
  }
  return ret;
}

SCN ObParallelCreateTenantExecutor::get_recovery_until_scn_()
{
  SCN scn = SCN::max_scn();
  if (create_tenant_arg_.is_restore_tenant() || create_tenant_arg_.is_clone_tenant()) {
    scn = create_tenant_arg_.recovery_until_scn_;
  }
  return scn;
}

palf::PalfBaseInfo ObParallelCreateTenantExecutor::get_palf_base_info_()
{
  palf::PalfBaseInfo palf_base_info;
  if (create_tenant_arg_.is_restore_tenant() || create_tenant_arg_.is_clone_tenant()) {
    palf_base_info = create_tenant_arg_.palf_base_info_;
  }
  return palf_base_info;
}

bool ObParallelCreateTenantExecutor::get_create_ls_with_palf_()
{
  return create_tenant_arg_.is_restore_tenant() || create_tenant_arg_.is_clone_tenant();
}

int ObParallelCreateTenantExecutor::do_check_can_create_user_ls_(bool &can_create)
{
  int ret = OB_SUCCESS;
  can_create = false;
  ObLSAttrArray array;
  ObAllTenantInfo tenant_info;
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  ObLSAttrOperator ls_operator(user_tenant_id, sql_proxy_);
  ObTimeoutCtx ctx;
  const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
  if (OB_FAIL(ctx.set_timeout(default_timeout))) {
    /*
    Hear, we are checking whether it is possible to create a user LS, which is a separate thread from create_normal_tenant().
    At the end of each while loop, async_rpc_has_error() will be used to check if create_normal_tenant() returns an error code,
    so that when the create_normal_tenant() fails, the while loop can be exited.
    But if the while loop gets stuck(such as due to schema related ...) in the middle step and cannot detect the create_normal_tenant() error code,
    it will need to wait until timeout to exit, subsequent step drop tenant force will ultimately failed due to timeout.
    For example:
      Injecting ERRSIM_BROADCAST_SCHEMA into z2 (z1 is normal) when creating tenant and broadcast schema,
      due to schema related modifications in the __all_sslog_table, it is assumed that version 1 of the schema is also readable,
      however, if a inner sql request reaches z2, it will cause z2's machine to repeatedly refresh the schema,
      which has not yet been written into the inner table, the refresh in z2 will not be successful and will be stuck until timeout occurs.
    Here we manually set the ctx timeout for each round to ensure that it will not be hung.
    Note that the ctx timeout needs to be restored later.
    */
    LOG_WARN("failed to set default timeout ctx", KR(ret), K(default_timeout));
  } else if (OB_FAIL(ls_operator.get_all_ls_by_order(array))) {
    LOG_WARN("failed to get all ls", KR(ret), K(user_tenant_id));
  } else if (array.count() == 0) {
    LOG_INFO("sys ls is not in __all_ls, need wait", KR(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(user_tenant_id, sql_proxy_, false/*for_update*/, tenant_info))) {
    LOG_WARN("failed to get tenant info, need wait", KR(ret), K(user_tenant_id));
  } else {
    can_create = true;
  }
  return ret;
}

// wait meta tenant sys table tablet created
int ObParallelCreateTenantExecutor::check_can_create_user_ls_(ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  FLOG_INFO("[CREATE_TENANT] begin check can create user ls", KR(ret));
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else {
    const int64_t retry_interval_us = 200l * 1000l; // 200ms
    // ignore LOG_USER_ERROR when trying to read from __all_ls
    ObWarningBufferIgnoreScope ignore_errors_in_warning_buffer;
    while (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      bool can_create = false;
      if (ctx_.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait tenant table writable failed", KR(ret));
      } else if (OB_TMP_FAIL(do_check_can_create_user_ls_(can_create))) {
        LOG_WARN("failed to check can create ls", KR(tmp_ret), K(user_tenant_id));
      } else if (can_create) {
        LOG_INFO("meta tenant tablet is created, ready to write data", K(user_tenant_id));
        break;
      }
      if (OB_FAIL(ret)) {
        // previous async rpc create_normal_tenant may have error, this function should fail fast
      } else if (async_rpc_has_error(proxy)) {
        ret = OB_CANCELED;
        LOG_WARN("create normal tenant RPC are returned, no data in meta and user table, "
            "create user ls is canceled", KR(ret), K(user_tenant_id));
      } else {
        ob_usleep(retry_interval_us);
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] finish check create user ls", KR(ret), "timeout", ctx_.get_abs_timeout());
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_USER_CREATE_TENANT_END_FAIL);
ERRSIM_POINT_DEF(ERRSIM_META_CREATE_TENANT_END_FAIL);

int ObParallelCreateTenantExecutor::finish_create_tenant_(const int ret_code)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  const uint64_t meta_tenant_id = meta_tenant_schema_.get_tenant_id();
  const ObTenantRole tenant_role = create_tenant_arg_.get_tenant_role();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (!create_tenant_schema_result_.is_valid() || create_tenant_schema_result_.tenant_exist_) {
    // no need to finish
  } else {
    if (OB_SUCC(ret_code)) {
      ObCreateTenantEndArg create_tenant_end_arg;
      create_tenant_end_arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
      // finish create tenant
      if (OB_FAIL(create_tenant_end_arg.init(meta_tenant_id))) {
        LOG_WARN("failed to init create tenant end arg", KR(ret), K(meta_tenant_id));
      } else if (OB_FAIL(common_rpc_->to_rs(*rs_mgr_).timeout(ctx_.get_timeout())
            .create_tenant_end(create_tenant_end_arg))) {
        LOG_WARN("failed to create tenant end", KR(ret), K(create_tenant_end_arg), K(ctx_));
      } else if (OB_FAIL(ERRSIM_META_CREATE_TENANT_END_FAIL)) {
        LOG_WARN("ERRSIM_META_CREATE_TENANT_END_FAIL", KR(ret));
      } else if (OB_FAIL(create_tenant_end_arg.init(user_tenant_id))) {
        LOG_WARN("failed to init create tenant end arg", KR(ret), K(user_tenant_id));
      } else if (tenant_role.is_primary() && OB_FAIL(common_rpc_->to_rs(*rs_mgr_)
            .timeout(ctx_.get_timeout()).create_tenant_end(create_tenant_end_arg))) {
        LOG_WARN("failed to create tenant end", KR(ret), K(create_tenant_end_arg), K(ctx_));
      } else if (OB_FAIL(ERRSIM_USER_CREATE_TENANT_END_FAIL)) {
        LOG_WARN("ERRSIM_USER_CREATE_TENANT_END_FAIL", KR(ret));
      } else {
        LOG_INFO("successfully create tenant", KR(ret), K(user_tenant_id));
      }
    }
    if ((OB_FAIL(ret) || OB_FAIL(ret_code)) && tenant_role.is_primary()) {
      // failed to create tenant, so force drop tenant
      const ObString tenant_name = create_tenant_arg_.tenant_schema_.get_tenant_name();
      obrpc::ObDropTenantArg drop_tenant_arg;
      ObSqlString ddl_stmt;
      if (OB_TMP_FAIL(ObTenantDDLService::generate_drop_tenant_arg(user_tenant_id,
              tenant_name, ddl_stmt, drop_tenant_arg))) {
        LOG_WARN("failed to generate drop tenant arg", KR(tmp_ret), K(user_tenant_id), K(tenant_name));
      } else if (OB_TMP_FAIL(common_rpc_->to_rs(*rs_mgr_).timeout(ctx_.get_timeout())
            .drop_tenant(drop_tenant_arg))) {
        LOG_WARN("failed to create tenant end", KR(tmp_ret), K(create_tenant_arg_), K(ctx_));
      } else {
        LOG_INFO("failed to create tenant, drop tenant force", KR(tmp_ret), K(user_tenant_id));
      }
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::get_tenant_schema_from_inner_table_()
{
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = create_tenant_schema_result_.user_tenant_id_;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  int64_t version_in_inner_table = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = OB_SYS_TENANT_ID;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
          OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(user_tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(user_tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", KR(ret), KP(tenant_schema));
  } else if (OB_FAIL(user_tenant_schema_.assign(*tenant_schema))) {
    LOG_WARN("failed to assign user tenant schema", KR(ret), K(*tenant_schema));
  } else if (OB_FAIL(schema_guard.get_tenant_info(meta_tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(meta_tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", KR(ret), KP(tenant_schema));
  } else if (OB_FAIL(meta_tenant_schema_.assign(*tenant_schema))) {
    LOG_WARN("failed to assign meta tenant schema", KR(ret), K(*tenant_schema));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::init(
      obrpc::ObCreateTenantArg create_tenant_arg,
      obrpc::ObSrvRpcProxy *rpc_proxy,
      obrpc::ObCommonRpcProxy *common_rpc,
      common::ObMySQLProxy *sql_proxy,
      share::schema::ObMultiVersionSchemaService *schema_service,
      share::ObLSTableOperator *lst_operator,
      share::ObLocationService *location_service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_tenant_arg_.assign(create_tenant_arg))) {
    LOG_WARN("failed to assign create tenant arg", KR(ret), K(create_tenant_arg));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx_, GCONF._ob_ddl_timeout))) {
    LOG_WARN("fail to set default timeout", KR(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    common_rpc_ = common_rpc;
    sql_proxy_ = sql_proxy;
    schema_service_ = schema_service;
    lst_operator_ = lst_operator;
    location_service_ = location_service;
    rs_mgr_ = GCTX.rs_mgr_;
  }
  return ret;
}
int ObParallelCreateTenantExecutor::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (!create_tenant_arg_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create tenant arg is invalid", KR(ret), K(create_tenant_arg_));
  } else if (OB_ISNULL(rpc_proxy_) || OB_ISNULL(common_rpc_) || OB_ISNULL(sql_proxy_)
      || OB_ISNULL(schema_service_) || OB_ISNULL(lst_operator_) || OB_ISNULL(location_service_)
      || OB_ISNULL(rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", KR(ret), K(rpc_proxy_), K(common_rpc_), K(sql_proxy_), K(schema_service_),
        K(lst_operator_), K(location_service_), K(rs_mgr_));
  }
  return ret;
}
}
}
