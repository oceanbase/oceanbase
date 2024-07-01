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
#include "ob_common_ls_service.h"
#include "ob_ls_service_helper.h"
#include "ob_balance_ls_primary_zone.h"
#include "lib/profile/ob_trace_id.h"
#include "share/ob_errno.h"
#include "share/ob_max_id_fetcher.h"
#include "share/schema/ob_schema_struct.h"//ObTenantInfo
#include "share/ls/ob_ls_creator.h" //ObLSCreator
#include "share/ls/ob_ls_life_manager.h"//ObLSLifeAgentManager
#include "share/ob_primary_zone_util.h"//ObPrimaryZoneUtil
#include "share/ob_share_util.h"//ObShareUtil
#include "share/ob_tenant_info_proxy.h"//ObAllTenantInfo
#include "share/ob_common_rpc_proxy.h"//common_rpc_proxy
#include "observer/ob_server_struct.h"//GCTX
#include "logservice/palf/palf_base_info.h"//PalfBaseInfo

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
using namespace palf;
namespace rootserver
{
//////////////ObCommonLSService
#define RESTORE_EVENT_ADD(tenant_id)                                           \
  int ret_code = OB_SUCCESS;                                                   \
  switch (ret) {                                                               \
    case -ER_ACCESS_DENIED_ERROR:                                              \
      ret_code = OB_PASSWORD_WRONG;                                            \
    case -ER_CONNECT_FAILED:                                                   \
      ret_code = OB_CONNECT_ERROR;                                             \
    case OB_IN_STOP_STATE:                                                     \
      ret_code = OB_IN_STOP_STATE;                                             \
    default:                                                                   \
      ret_code = ret;                                                          \
  }                                                                            \
  ROOTSERVICE_EVENT_ADD("root_service", "update_primary_ip_list",              \
    "tenant_id", tenant_id, K(ret),                                            \
    "ob_error_name", ob_error_name(ret_code),                                  \
    "ob_error_str", ob_strerror(ret_code),                                     \
    "primary_user_tenant", user_and_tenant.ptr(),                              \
    "primary_ip_list", service_attr.addr_);                                    \

int ObCommonLSService::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("COMMONLSSe",
          lib::TGDefIDs::SimpleLSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("fail to start", KR(ret));
  } else {
    tenant_id_ = MTL_ID();
    inited_ = true;
    proxy_ = GCTX.sql_proxy_;
    primary_is_avaliable_ = true;
  }
  return ret;
}

void ObCommonLSService::destroy()
{
  ObTenantThreadHelper::destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  inited_ = false;
  proxy_ = NULL;
  restore_proxy_.destroy();
  primary_is_avaliable_ = false;
}

void ObCommonLSService::do_work()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_user_tenant(tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is user tenant", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(wait_tenant_schema_and_version_ready_(tenant_id_, DATA_VERSION_4_1_0_0))) {
    LOG_WARN("failed to wait tenant schema version ready", KR(ret), K(tenant_id_), K(DATA_CURRENT_VERSION));
  } else {
    int64_t idle_time_us = 1000 * 1000L;//1s
    share::schema::ObTenantSchema user_tenant_schema;
    int tmp_ret = OB_SUCCESS;
    while (!has_set_stop()) {
      ret = OB_SUCCESS;
      ObCurTraceId::init(GCONF.self_addr_);
      if (is_meta_tenant(tenant_id_)) {
        const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id_);
        if (OB_FAIL(check_can_do_recovery_(user_tenant_id))) {
          LOG_WARN("can not do recovery now", KR(ret), K(user_tenant_id));
        } else if (OB_FAIL(get_tenant_schema(user_tenant_id, user_tenant_schema))) {
          LOG_WARN("failed to get user tenant schema", KR(ret), K(user_tenant_id));
        } else if (user_tenant_schema.is_dropping()) {
          if (OB_TMP_FAIL(try_force_drop_tenant_(user_tenant_schema))) {
            LOG_WARN("failed to force drop tenant", KR(ret), KR(tmp_ret), K(user_tenant_id));
          }
        } else if (OB_TMP_FAIL(try_create_ls_(user_tenant_schema))) {
          LOG_WARN("failed to create ls", KR(ret), KR(tmp_ret), K(user_tenant_schema));
        }
        if (OB_SUCC(ret) && !user_tenant_schema.is_dropping()) {
          if (OB_TMP_FAIL(ObBalanceLSPrimaryZone::try_adjust_user_ls_primary_zone(user_tenant_schema))) {
            LOG_WARN("failed to adjust user tenant primary zone", KR(ret), KR(tmp_ret), K(user_tenant_schema));
          }
          if (OB_TMP_FAIL(try_modify_ls_unit_group_(user_tenant_schema))) {
            LOG_WARN("failed to modify ls unit group", KR(ret), KR(tmp_ret), K(user_tenant_schema));
          }
        }
        // update primary ip list in every 10s
        if (REACH_TENANT_TIME_INTERVAL(10 * 1000 * 1000)) {
          (void)try_update_primary_ip_list();
        }
      }

      if (OB_TMP_FAIL(ObBalanceLSPrimaryZone::try_update_sys_ls_primary_zone(tenant_id_))) {
        LOG_WARN("failed to update sys ls primary zone", KR(ret), KR(tmp_ret), K(tenant_id_));
      }

      user_tenant_schema.reset();
      LOG_INFO("[COMMON_LS_SERVICE] finish one round", KR(ret), KR(tmp_ret), K(idle_time_us));
      idle(idle_time_us);
    }  // end while
  }
}


int ObCommonLSService::try_create_ls_(const share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy  is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (!is_user_tenant(tenant_id) || !tenant_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", KR(ret), K(tenant_id), K(tenant_schema));
  } else {
    share::ObLSStatusInfoArray status_info_array;
    share::ObLSStatusOperator ls_op;
    ObLSRecoveryStat recovery_stat;
    ObLSRecoveryStatOperator ls_recovery_operator;
    palf::PalfBaseInfo palf_base_info;
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(ls_op.get_all_ls_status_by_order(
            tenant_id, status_info_array, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to get all ls status", KR(ret), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
      const ObLSStatusInfo &status_info = status_info_array.at(i);
      if (status_info.ls_is_creating()) {
        recovery_stat.reset();
        if (OB_FAIL(ls_recovery_operator.get_ls_recovery_stat(
                  tenant_id, status_info.ls_id_, false /*for_update*/,
                  recovery_stat, *GCTX.sql_proxy_))) {
          LOG_WARN("failed to get ls recovery stat", KR(ret), K(tenant_id),
                     K(status_info));
        } else if (OB_FAIL(do_create_user_ls(tenant_schema, status_info,
                                             recovery_stat.get_create_scn(),
                                             false, palf_base_info, OB_INVALID_TENANT_ID/*source_tenant_id*/))) {
          LOG_WARN("failed to create new ls", KR(ret), K(status_info),
                     K(recovery_stat));
        }
      }
    }  // end for
  }
  return ret;
}
//不管是主库还是备库都有概率存在一个日志流组内的日志流记录的unit_group不一致的情况
//所有的日志流都对齐日志流id最小的日志流,虽然不是最优，但是可以保证最终一致性
int ObCommonLSService::try_modify_ls_unit_group_(
    const share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (!is_user_tenant(tenant_id) || !tenant_schema.is_valid()
             || tenant_schema.is_dropping()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", KR(ret), K(tenant_id), K(tenant_schema));
  } else {
    ObTenantLSInfo tenant_info(GCTX.sql_proxy_, &tenant_schema, tenant_id);
    share::ObLSStatusOperator status_op;
    if (OB_FAIL(tenant_info.gather_stat())) {
      LOG_WARN("failed to gather stat", KR(ret));
    } else {
      ObLSGroupInfoArray &ls_group_array = tenant_info.get_ls_group_array();
      int64_t index = 0;//no used
      share::ObLSStatusInfo ls_status;
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_group_array.count(); ++i) {
        const uint64_t unit_group_id = ls_group_array.at(i).unit_group_id_;
        const uint64_t ls_group_id = ls_group_array.at(i).ls_group_id_;
        const ObArray<share::ObLSID> &ls_ids = ls_group_array.at(i).ls_ids_;
        for (int64_t j = 0; OB_SUCC(ret) && j < ls_ids.count(); ++j) {
          const share::ObLSID ls_id = ls_ids.at(j);
          if (OB_FAIL(tenant_info.get_ls_status_info(ls_id, ls_status, index))) {
            LOG_WARN("failed to get ls status info", KR(ret), K(ls_id));
          } else if (ls_status.unit_group_id_ != unit_group_id) {
            FLOG_INFO("ls group has different unit group id, need process", K(ls_status), K(unit_group_id));
            if (OB_FAIL(status_op.alter_unit_group_id(tenant_id, ls_id, ls_group_id,
                    ls_status.unit_group_id_, unit_group_id, *GCTX.sql_proxy_))) {
              LOG_WARN("failed to alter unit group", KR(ret), K(tenant_id), K(ls_id),
                  K(ls_group_id), K(unit_group_id), K(ls_status));

            }
          }
        }//end for j
      }//end for i
    }
  }
  return ret;
}

int ObCommonLSService::do_create_user_ls(
    const share::schema::ObTenantSchema &tenant_schema,
    const share::ObLSStatusInfo &info, const SCN &create_scn,
    bool create_with_palf, const palf::PalfBaseInfo &palf_base_info,
    const uint64_t source_tenant_id)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[COMMON_LS_SERVICE] start to create ls", K(info), K(create_scn));
  const int64_t start_time = ObTimeUtility::fast_current_time();
  if (OB_UNLIKELY(!info.is_valid() || !info.ls_is_creating())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid", KR(ret), K(info));
  } else {
    CK(OB_NOT_NULL(GCTX.sql_proxy_), OB_NOT_NULL(GCTX.srv_rpc_proxy_))
    common::ObArray<share::ObZoneReplicaAttrSet> locality_array;
    int64_t paxos_replica_num = 0;
    ObSchemaGetterGuard guard;//nothing
    if (FAILEDx(tenant_schema.get_zone_replica_attr_array(
                   locality_array))) {
      LOG_WARN("failed to get zone locality array", KR(ret));
    } else if (OB_FAIL(tenant_schema.get_paxos_replica_num(
                   guard, paxos_replica_num))) {
      LOG_WARN("failed to get paxos replica num", KR(ret));
    } else {
      ObLSCreator creator(*GCTX.srv_rpc_proxy_, info.tenant_id_,
                          info.ls_id_, GCTX.sql_proxy_);
      if (OB_FAIL(creator.create_user_ls(info, paxos_replica_num,
                                         locality_array, create_scn,
                                         tenant_schema.get_compatibility_mode(),
                                         create_with_palf,
                                         palf_base_info,
                                         source_tenant_id))) {
        LOG_WARN("failed to create user ls", KR(ret), K(info), K(locality_array), K(create_scn),
                                             K(palf_base_info), K(create_with_palf), K(source_tenant_id));
      }
    }
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  LOG_INFO("[COMMON_LS_SERVICE] end to create ls", KR(ret), K(info), K(cost));
  return ret;
}

int ObCommonLSService::try_force_drop_tenant_(
    const share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)
             || OB_ISNULL(GCTX.rs_rpc_proxy_)
             || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.rs_rpc_proxy_),
        KP(GCTX.schema_service_));
  } else if (!is_user_tenant(tenant_id) || !tenant_schema.is_valid()
             || !tenant_schema.is_dropping()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", KR(ret), K(tenant_id), K(tenant_schema));
  } else {
    ObLSStatusOperator op;
    share::ObLSStatusInfoArray ls_array;
    const int64_t start_time = ObTimeUtility::fast_current_time();
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.internal_sql_execute_timeout))) {
      LOG_WARN("fail to set timeout ctx", KR(ret), K(tenant_id));
    } else if (OB_FAIL(op.get_all_ls_status_by_order(tenant_id, ls_array, *GCTX.sql_proxy_))) {
      LOG_WARN("fail to get all ls status", KR(ret), K(tenant_id));
    } else if (ls_array.count() <= 0) {
      obrpc::ObDropTenantArg arg;
      arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
      arg.tenant_name_ = tenant_schema.get_tenant_name();
      arg.tenant_id_ = tenant_schema.get_tenant_id();
      arg.if_exist_ = true;
      arg.delay_to_drop_ = false;
      ObSqlString sql;
      const int64_t timeout_ts = ctx.get_timeout();
      if (OB_FAIL(sql.append_fmt("DROP TENANT IF EXISTS %s FORCE", arg.tenant_name_.ptr()))) {
        LOG_WARN("fail to generate sql", KR(ret), K(arg));
      } else if (FALSE_IT(arg.ddl_stmt_str_ = sql.string())) {
      } else if (OB_FAIL(GCTX.rs_rpc_proxy_->timeout(timeout_ts).drop_tenant(arg))) {
        LOG_WARN("fail to drop tenant", KR(ret), K(arg), K(timeout_ts));
      }
    } else {
      // tenant's logstream is still dropping, check next round
    }
    const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
    LOG_INFO("finish try drop tenant", KR(ret), K(tenant_id), K(ls_array),
        "timeout", ctx.get_timeout(), K(cost));
  }
  return ret;
}

void ObCommonLSService::try_update_primary_ip_list()
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", K_(tenant_id));
  } else if (!is_meta_tenant(tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", K_(tenant_id));
  } else {
    ObLogRestoreSourceMgr restore_source_mgr;
    ObLogRestoreSourceItem item;
    common::ObArray<common::ObAddr> primary_addrs;
    ObSqlString standby_source_value;
    ObRestoreSourceServiceAttr service_attr;
    ObMySQLTransaction trans;
    ObSqlString user_and_tenant;
    int64_t primary_cluster_id = 0;
    uint64_t primary_tenant_id = 0;
    char passwd[OB_MAX_PASSWORD_LENGTH + 1] = { 0 }; //unencrypted password
    uint64_t user_tenant_id = gen_user_tenant_id(tenant_id_);
    bool log_restore_source_exist = true;
    bool cluster_id_dup = false;

    if (OB_FAIL(restore_source_mgr.init(user_tenant_id, proxy_))) {
      LOG_WARN("fail to init restore_source_mgr", K(user_tenant_id));
    } else if (OB_FAIL(restore_source_mgr.get_source(item))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        log_restore_source_exist = false;
        if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_INFO("log restore source is empty, just skip", K(ret), K(user_tenant_id));
        }
      } else {
        LOG_WARN("get source failed", K(user_tenant_id), K(ret));
      }
    } else if (! need_update_ip_list_(item)) {
      if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000)) {
        LOG_INFO("log restore source not exists or the log restore source type is not service" , K(item));
      }
    } else if (OB_FAIL(get_restore_source_value_(item, standby_source_value))) {
      LOG_WARN("fail to get log_restore_source value", K(item));
    } else if (OB_FAIL(service_attr.parse_service_attr_from_str(standby_source_value))) {
      LOG_WARN("fail to parse service attr", K(item), K(standby_source_value));
    } else if (! service_attr.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("service attr is invalid", K(service_attr), K(user_tenant_id));
    } else if (OB_FAIL(service_attr.get_password(passwd, sizeof(passwd)))) {
      LOG_WARN("get servcie attr password failed", K(service_attr));
    } else if (OB_FAIL(service_attr.get_user_str_(user_and_tenant))) {
      LOG_WARN("get user str failed", K(service_attr.user_.user_name_), K(service_attr.user_.tenant_name_));
    } else if (!primary_is_avaliable_ && restore_proxy_.is_inited()) {
      LOG_WARN("primary is not avaliable, retry init restore proxy");
      restore_proxy_.destroy();
    } else if (!restore_proxy_.is_inited() && OB_FAIL(restore_proxy_.init(user_tenant_id/*standby*/,
        service_attr.addr_,
        user_and_tenant.ptr(),
        passwd,
        service_attr.user_.mode_ == ObCompatibilityMode::MYSQL_MODE ? OB_SYS_DATABASE_NAME : OB_ORA_SYS_SCHEMA_NAME))) {
      LOG_WARN("restore_proxy_ fail to connect to primary", K(user_tenant_id), K(service_attr.addr_), K(user_and_tenant));
    } else if (OB_FAIL(restore_proxy_.get_cluster_id(service_attr.user_.tenant_id_, primary_cluster_id))) {
      LOG_WARN("restore proxy fail to get primary cluster id", K(service_attr.user_.tenant_id_));
    } else if (OB_FAIL(restore_proxy_.get_tenant_id(service_attr.user_.tenant_name_, primary_tenant_id))) {
      LOG_WARN("restore proxy fail to get primary tenant id", K(service_attr.user_.tenant_name_));
    } else if (OB_FAIL(restore_proxy_.get_server_ip_list(service_attr.user_.tenant_id_, primary_addrs))) {
      LOG_WARN("restore proxy fail to get primary server ip list", K(service_attr.user_.tenant_id_));
    } else if (primary_addrs.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant ip list is empty", K(primary_addrs));
    } else if ((primary_cluster_id != service_attr.user_.cluster_id_) || (primary_tenant_id != service_attr.user_.tenant_id_)) {
      LOG_WARN("primary cluster_id or tenant_id has been changed",
      K(primary_cluster_id), K_(service_attr.user_.cluster_id), K(primary_tenant_id), K_(service_attr.user_.tenant_id), K(user_tenant_id));
    } else if (OB_FAIL(restore_proxy_.check_different_cluster_with_same_cluster_id(
                   service_attr.user_.cluster_id_, cluster_id_dup))) {
      LOG_WARN("fail to check different cluster with same cluster id", KR(ret),
               K(service_attr.user_.cluster_id_));
    } else if (cluster_id_dup) {
      ret = OB_OP_NOT_ALLOW;
      LOG_ERROR("different cluster with same cluster id is not allowed", KR(ret),
                K(service_attr.user_.cluster_id_));
    } else {
      // update primary state
      bool cur_primary_state = true;
      if (cur_primary_state != primary_is_avaliable_) {
        primary_is_avaliable_ = true;
        ROOTSERVICE_EVENT_ADD("root_service", "update_primary_ip_list",
          "tenant_id", user_tenant_id,
          "info", "primary connection recovery",
          "primary_user_tenant", user_and_tenant.ptr(),
          "primary_ip_list", primary_addrs);
      }
      // update primary ip list
      bool addr_is_same = false;
      addr_is_same = service_attr.compare_addr_(primary_addrs);
      if (!addr_is_same) {
        common::ObArray<common::ObAddr> tmp_addr;  // record old ip list
        if (OB_FAIL(tmp_addr.assign(service_attr.addr_))) {
          LOG_WARN("fail to assign service attr addr", K(service_attr.addr_));
        } else {
          service_attr.addr_.reset();
          ARRAY_FOREACH_N(primary_addrs, idx, cnt) {
            if (OB_FAIL(service_attr.addr_.push_back(primary_addrs.at(idx)))) {
              LOG_WARN("fail to push back primary addr", K(primary_addrs.at(idx)));
            }
          }
          LOG_INFO("primary ip list has changed", K(addr_is_same), K(primary_addrs), K_(service_attr.addr), K(user_tenant_id));
          if (OB_FAIL(ret)) {
            LOG_WARN("fail to update primary ip list", K(ret));
          } else if (OB_FAIL(do_update_restore_source_(service_attr, restore_source_mgr))) {
            LOG_WARN("fail to update restore source", K(service_attr));
          }
          ROOTSERVICE_EVENT_ADD("root_service", "update_primary_ip_list",
            "tenant_id", user_tenant_id,
            "info", "do update primary ip list",
            "primary_user_tenant", user_and_tenant.ptr(),
            "new_primary_ip_list", primary_addrs,
            "old_primary_ip_list", tmp_addr);
        }
      } else {
        LOG_TRACE("primary ip list has not changed", K(ret), K(user_and_tenant), K(addr_is_same));
      }
    }
    if (!log_restore_source_exist && OB_ENTRY_NOT_EXIST == ret) {
      LOG_TRACE("log_restore_source not exist", K(ret), K(user_tenant_id));
    } else if (OB_FAIL(ret)) {
      bool cur_primary_state = false;
      if (cur_primary_state != primary_is_avaliable_) {
        primary_is_avaliable_ = false;
        LOG_WARN("standby recovery ls service state changed");
        RESTORE_EVENT_ADD(user_tenant_id);
      }
    }
  }
}

bool ObCommonLSService::need_update_ip_list_(ObLogRestoreSourceItem &item)
{
  return item.is_valid() && ObLogRestoreSourceType::SERVICE == item.type_;
}

int ObCommonLSService::get_restore_source_value_(ObLogRestoreSourceItem &item, ObSqlString &standby_source_value)
{
  int ret = OB_SUCCESS;
  ObSqlString value;
  if (!item.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log restore source item is invalid");
  } else if (OB_FAIL(standby_source_value.assign(item.value_))) {
    LOG_WARN("fail to assign standby source value", K(item.value_));
  }
  return ret;
}

int ObCommonLSService::do_update_restore_source_(
  ObRestoreSourceServiceAttr &old_attr,
  ObLogRestoreSourceMgr &restore_source_mgr)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObLogRestoreSourceItem tmp_item;
  ObRestoreSourceServiceAttr tmp_service_attr;
  ObSqlString tmp_standby_source_value;
  char updated_value_str[OB_MAX_BACKUP_DEST_LENGTH + 1] = { 0 };

  if (OB_FAIL(old_attr.gen_service_attr_str(updated_value_str, sizeof(updated_value_str)))) {
    LOG_WARN("gen service_attr_str failed", K(updated_value_str), K(old_attr));
  } else if (OB_FAIL(trans.start(proxy_, tenant_id_))) {
    LOG_WARN("fail to start trans", K_(tenant_id));
  } else if (OB_FAIL(restore_source_mgr.get_source_for_update(tmp_item, trans))) {
    LOG_WARN("fail to get log restore source when double check" , K(tmp_item));
  } else if (ObLogRestoreSourceType::SERVICE != tmp_item.type_) {
    ret = OB_EAGAIN;
    LOG_WARN("log restore source type is not service", K_(tenant_id), K(tmp_item.type_));
  } else if (OB_FAIL(get_restore_source_value_(tmp_item, tmp_standby_source_value))) {
    LOG_WARN("fail to get service standby log restore source value", K(tmp_item));
  } else if (OB_FAIL(tmp_service_attr.parse_service_attr_from_str(tmp_standby_source_value))) {
    LOG_WARN("fail to parse service attr", K(tmp_item), K(tmp_standby_source_value));
  } else if (!tmp_service_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("service attr is invalid", K(tmp_service_attr));
  } else if (!(tmp_service_attr.user_ == old_attr.user_)) {
    ret = OB_EAGAIN;
    LOG_WARN("log restore source record may be modified", K(tmp_service_attr.user_), K(old_attr.user_));
  } else if (0 != STRCMP(tmp_service_attr.encrypt_passwd_, old_attr.encrypt_passwd_)) {
    ret = OB_EAGAIN;
    LOG_WARN("log restore source password may be modified", K_(tenant_id));
  } else if (OB_FAIL(update_source_inner_table_(updated_value_str, sizeof(updated_value_str), trans, tmp_item))) {
    LOG_WARN("fail to add service source", K(updated_value_str), K(tmp_item.until_scn_));
  }

  int temp_ret = OB_SUCCESS;
  if (trans.is_started()) {
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
      ret = (OB_SUCC(ret)) ? temp_ret : ret;
    }
  }
  return ret;
}

int ObCommonLSService::update_source_inner_table_(char *buf,
                                                    const int64_t buf_size,
                                                    ObMySQLTransaction &trans,
                                                    const ObLogRestoreSourceItem &item)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument when get source for update",K(buf_size));
  } else {
    uint64_t user_tenant_id = gen_user_tenant_id(tenant_id_);
    ObDMLSqlSplicer dml;
    ObSqlString sql;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, user_tenant_id))) {
      LOG_WARN("failed to add column", K_(tenant_id), K(item));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_LOG_RESTORE_SOURCE_ID, item.id_))) {
      LOG_WARN("failed to add column", K_(tenant_id), K(item));
    } else if (OB_FAIL(dml.add_column(OB_STR_LOG_RESTORE_SOURCE_VALUE, buf))) {
      LOG_WARN("failed to add column", K_(tenant_id), K(item));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_LOG_RESTORE_SOURCE_TNAME, sql))) {
      LOG_WARN("fill update source value sql failed", K(item));
    } else if (OB_FAIL(trans.write(tenant_id_, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to exec sql", K(sql), K(user_tenant_id));
    }
  }
  return ret;
}

}//end of rootserver
}
