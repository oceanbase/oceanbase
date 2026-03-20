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
#include "share/ls/ob_ls_creator.h" //ObLSCreator
#include "src/share/ob_common_rpc_proxy.h"
#include "share/ob_sync_standby_dest_parser.h"
#include "share/ob_sync_standby_dest_operator.h"
#include "share/ob_cluster_version.h"
#include "rootserver/standby/ob_protection_mode_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
using namespace palf;
namespace rootserver
{
//////////////ObCommonLSService
#define RESTORE_EVENT_ADD(tenant_id, target)                           \
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
  ROOTSERVICE_EVENT_ADD("root_service", "update_ip_list"                       \
    "tenant_id", tenant_id, K(ret),                                            \
    "ob_error_name", ob_error_name(ret_code),                                  \
    "ob_error_str", ob_strerror(ret_code),                                     \
    "change_target", target,                                                     \
    "user_tenant", user_and_tenant.ptr(),                                      \
    "ip_list", restore_source_attr.addr_);                                            \

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
  }
  return ret;
}

void ObCommonLSService::destroy()
{
  ObTenantThreadHelper::destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  inited_ = false;
  proxy_ = NULL;
  primary_restore_source_attr_updater_.reset();
  standby_restore_source_attr_updater_.reset();
}

ERRSIM_POINT_DEF(ERRSIM_DISCONNECT_RESTORE_SOURCE_ATTR)
ERRSIM_POINT_DEF(ERRSIM_STOP_COMMON_LS_SERVICE)
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
      DEBUG_SYNC(STOP_COMMON_LS_SERVICE);
      if (is_meta_tenant(tenant_id_) && OB_SUCC(ERRSIM_STOP_COMMON_LS_SERVICE)) {
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
          if (OB_TMP_FAIL(try_modify_ls_unit_group_or_unit_list_(user_tenant_schema))) {
            LOG_WARN("failed to modify ls unit group", KR(ret), KR(tmp_ret), K(user_tenant_schema));
          }
        }
        // update primary ip list in every 10s
        if (ERRSIM_DISCONNECT_RESTORE_SOURCE_ATTR || REACH_THREAD_TIME_INTERVAL(10 * 1000 * 1000)) {
          (void)try_update_ip_list();
        }
      }
      //系统日志流primary_zone的调整不受配置项的控制
      //系统日志流的个数不会发生变化，加上限制会导致升级case需要大量的修改
      if (OB_TMP_FAIL(ObBalanceLSPrimaryZone::try_update_sys_ls_primary_zone(tenant_id_))) {
        LOG_WARN("failed to update sys ls primary zone", KR(ret), KR(tmp_ret), K(tenant_id_));
      }

      user_tenant_schema.reset();
      LOG_INFO("[COMMON_LS_SERVICE] finish one round", KR(ret), KR(tmp_ret), K(idle_time_us),
          K(ERRSIM_STOP_COMMON_LS_SERVICE));
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
int ObCommonLSService::try_modify_ls_unit_group_or_unit_list_(
    const share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  uint64_t meta_tenant_data_version = 0;
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
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id), meta_tenant_data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
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
          } else if (!ObShareUtil::check_compat_version_for_hetero_zone(meta_tenant_data_version)) {
            // dat version below 4.4.2.0
            // check and update unit_group_id
            if (ls_status.unit_group_id_ != unit_group_id) {
              FLOG_INFO("ls group has different unit group id, need process", K(ls_status), K(unit_group_id));
              if (OB_FAIL(status_op.alter_unit_group_id(tenant_id, ls_id, ls_group_id,
                      ls_status.unit_group_id_, unit_group_id, *GCTX.sql_proxy_))) {
                LOG_WARN("failed to alter unit group", KR(ret), K(tenant_id), K(ls_id),
                    K(ls_group_id), K(unit_group_id), K(ls_status));
              }
            }
          } else {
            // data version up to 4.4.2.0
            // check and update unit_list
            if (ls_status.get_unit_list() != ls_group_array.at(i).unit_list_) {
              FLOG_INFO("ls group has different unit id list, need process",
                        K(ls_status), K(ls_group_array.at(i)));
              // use this function to update unit_list, not ls group id
              if (OB_FAIL(status_op.alter_ls_group_id(tenant_id, ls_id, ls_group_id/*old_one*/,
                              ls_group_id/*new_one*/, ls_status.get_unit_list()/*old_one*/,
                              ls_group_array.at(i).unit_list_/*new_one*/, *GCTX.sql_proxy_))) {
                LOG_WARN("fail to alter unit list", KR(ret), K(tenant_id), K(ls_id), K(ls_group_id),
                         K(ls_status), K(ls_group_array.at(i)));
              }
            }
          }
        }//end for j
      }//end for i
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_SKIP_CREATE_USER_LS)
int ObCommonLSService::do_create_user_ls(
    const share::schema::ObTenantSchema &tenant_schema,
    const share::ObLSStatusInfo &info, const SCN &create_scn,
    bool create_with_palf, const palf::PalfBaseInfo &palf_base_info,
    const uint64_t source_tenant_id)
{
  int ret = OB_SUCCESS;
  LOG_INFO("[COMMON_LS_SERVICE] start to create ls", K(info), K(create_scn));
  const int64_t start_time = ObTimeUtility::fast_current_time();
  if (OB_UNLIKELY(ERRSIM_SKIP_CREATE_USER_LS)) {
    LOG_INFO("errsim skip create user ls");
  } else if (OB_UNLIKELY(!info.is_valid() || !info.ls_is_creating())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("info not valid", KR(ret), K(info));
  } else {
    CK(OB_NOT_NULL(GCTX.sql_proxy_), OB_NOT_NULL(GCTX.srv_rpc_proxy_))
    common::ObArray<share::ObZoneReplicaAttrSet> locality_array;
    //由于日志流只能根据unit_list创建，所以paxos_replica_num设置为unit_list
    //内的paxos_replica_num的个数
    //unit_list可能不能和locality匹配，依赖后续的容灾操作把副本补完

    ObSchemaGetterGuard guard;//nothing
    if (FAILEDx(tenant_schema.get_zone_replica_attr_array(
                   locality_array))) {
      LOG_WARN("failed to get zone locality array", KR(ret));
    } else {
      ObLSCreator creator(*GCTX.srv_rpc_proxy_, info.tenant_id_,
                          info.ls_id_, GCTX.sql_proxy_);
      if (OB_FAIL(creator.create_user_ls(info,
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

ObRestoreSourceAttrUpdater::ObRestoreSourceAttrUpdater()
{
  reset();
}

ObRestoreSourceAttrUpdater::~ObRestoreSourceAttrUpdater()
{
  reset();
}

int ObRestoreSourceAttrUpdater::init(const uint64_t user_tenant_id, common::ObMySQLProxy *proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(proxy) || !is_valid_tenant_id(user_tenant_id) || !is_user_tenant(user_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("proxy is null or user tenant id is invalid", KR(ret), KP(proxy), K(user_tenant_id));
  } else {
    user_tenant_id_ = user_tenant_id;
    proxy_ = proxy;
    remote_is_avaliable_ = true;
    inited_ = true;
    remote_proxy_.destroy();
  }
  return ret;
}

void ObRestoreSourceAttrUpdater::reset()
{
  inited_ = false;
  user_tenant_id_ = OB_INVALID_TENANT_ID;
  proxy_ = NULL;
  remote_is_avaliable_ = false;
  remote_proxy_.destroy();
}
int ObRestoreSourceAttrUpdater::update_restore_source_attr_periodically()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy_));
  } else {
    ObRestoreSourceServiceAttr restore_source_attr;
    bool need_update = false;
    uint64_t target_tenant_id = OB_INVALID_TENANT_ID;
    int64_t target_cluster_id = OB_INVALID_CLUSTER_ID;
    ObSqlString user_and_tenant;
    ObArray<common::ObAddr> remote_ip_list;
    ObArray<int32_t> remote_svr_port_list;
    uint64_t user_data_version = OB_INVALID_VERSION;
    bool use_svr_port = false;
    bool internal_has_svr_port = false;
    if (OB_FAIL(fetch_current_restore_source_attr(restore_source_attr, need_update))) {
      LOG_WARN("fail to fetch current restore source attr", KR(ret), K(user_tenant_id_));
    } else if (!need_update) {
      LOG_INFO("restore source attr is not need update", KR(ret), K(user_tenant_id_));
    } else if (OB_FALSE_IT(internal_has_svr_port = restore_source_attr.has_svr_port())) {
      // record whether __all_log_restore_source already has svr_port before init_for_first_connection
    } else if (OB_FAIL(restore_source_attr.get_user_str_(user_and_tenant))) {
      LOG_WARN("fail to get user str", KR(ret), K(restore_source_attr));
    } else if ((!remote_is_avaliable_ || ERRSIM_DISCONNECT_RESTORE_SOURCE_ATTR) && remote_proxy_.is_inited()) {
      LOG_WARN("remote is not avaliable, retry init remote proxy", KR(ret), K(remote_is_avaliable_),
        K(ERRSIM_DISCONNECT_RESTORE_SOURCE_ATTR), K(user_tenant_id_));
      remote_proxy_.destroy();
    } else if (!remote_proxy_.is_inited() && OB_FAIL(restore_source_attr.init_proxy_utils(
        user_tenant_id_, remote_proxy_))) {
      LOG_WARN("fail to init for first connection", KR(ret), K(user_tenant_id_));
    } else if (OB_FAIL(restore_source_attr.check_tenant_not_changed(remote_proxy_, target_tenant_id, target_cluster_id))) {
      LOG_WARN("fail to check tenant not changed", KR(ret), K(user_tenant_id_));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(user_tenant_id_, user_data_version))) {
      LOG_WARN("failed to get user data version", KR(ret), K(user_tenant_id_));
    } else if (FALSE_IT(use_svr_port = (user_data_version >= standby::ObProtectionModeUtils::get_protection_mode_data_version()))) {
    } else if (use_svr_port && OB_FAIL(remote_proxy_.get_server_ip_and_svr_port_list(
        target_tenant_id, remote_ip_list, remote_svr_port_list))) {
      LOG_WARN("fail to get server ip and svr port list", KR(ret), K(user_tenant_id_));
    } else if (!use_svr_port && OB_FAIL(remote_proxy_.get_server_ip_list(target_tenant_id, remote_ip_list))) {
      LOG_WARN("fail to get server ip list", KR(ret), K(user_tenant_id_));
    } else if (remote_ip_list.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("remote ip list is empty", KR(ret), K(user_tenant_id_));
    } else {
      if (!remote_is_avaliable_) {
        remote_is_avaliable_ = true;
        ROOTSERVICE_EVENT_ADD("root_service", "update_ip_list",
          "tenant_id", user_tenant_id_,
          "info", "remote connection recovery",
          "user_tenant", user_and_tenant.ptr(),
          "ip_list", remote_ip_list,
          "change_target", get_change_target());
      }
      bool addr_is_same = restore_source_attr.compare_addr_(remote_ip_list);
      bool need_update = !addr_is_same
        || (use_svr_port && !internal_has_svr_port);
      if (need_update) {
        ObArray<ObRestoreSourceServiceAddr> previous_ip_list;
        LOG_INFO("remote ip list needs update", K(addr_is_same), K(use_svr_port),
            K_(restore_source_attr.addr), K(remote_ip_list), K(user_tenant_id_));
        if (OB_FAIL(restore_source_attr.get_service_addr_list(previous_ip_list))) {
          LOG_WARN("fail to get previous ip list", KR(ret), K(restore_source_attr));
        } else if (use_svr_port && OB_FAIL(restore_source_attr.set_sql_addr_and_svr_port_list(
              remote_ip_list, remote_svr_port_list))) {
            LOG_WARN("failed to set svr addr list", KR(ret), K(remote_ip_list));
        } else if (!use_svr_port && OB_FAIL(restore_source_attr.set_sql_addr_list(remote_ip_list))) {
          LOG_WARN("fail to set sql addr list", KR(ret), K(previous_ip_list));
        } else if (OB_FAIL(update_restore_source_attr(restore_source_attr))) {
          LOG_WARN("fail to update restore source", KR(ret), K(restore_source_attr));
        } else {
          ObArray<ObRestoreSourceServiceAddr> new_addr_list_with_port;
          if (OB_FAIL(restore_source_attr.get_service_addr_list(new_addr_list_with_port))) {
            LOG_WARN("failed to get service addr list", KR(ret), K(restore_source_attr));
          }
          LOG_INFO("remote ip list updated",
              KR(ret), K(user_tenant_id_), K(user_and_tenant), K(use_svr_port),
              K(remote_ip_list), K(remote_svr_port_list), K(previous_ip_list),
              K(new_addr_list_with_port));
          ROOTSERVICE_EVENT_ADD("root_service", "update_ip_list",
            "tenant_id", user_tenant_id_,
            "info", "do update ip list",
            "user_tenant", user_and_tenant.ptr(),
            "new_ip_list", new_addr_list_with_port,
            "old_ip_list", previous_ip_list,
            "change_target", get_change_target());
        }
      } else {
        LOG_TRACE("remote ip list has not changed", K(ret), K(user_and_tenant), K(addr_is_same));
      }
    }
    if (!need_update) {
    } else if (OB_FAIL(ret)) {
      if (remote_is_avaliable_) {
        remote_is_avaliable_ = false;
        LOG_WARN("remote connection state changed");
        RESTORE_EVENT_ADD(user_tenant_id_, get_change_target());
      }
    }
  }
  LOG_INFO("finish update ip list", KR(ret), K(user_tenant_id_), "target", get_change_target());
  return ret;
}

ObPrimaryRestoreSourceAttrUpdater::ObPrimaryRestoreSourceAttrUpdater() {}

ObPrimaryRestoreSourceAttrUpdater::~ObPrimaryRestoreSourceAttrUpdater() {}

int ObPrimaryRestoreSourceAttrUpdater::fetch_current_restore_source_attr(
  ObRestoreSourceServiceAttr &restore_source_attr, bool &need_update)
{
  int ret = OB_SUCCESS;
  ObLogRestoreSourceMgr restore_source_mgr;
  ObLogRestoreSourceItem item;
  ObSqlString restore_source_value;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(restore_source_mgr.init(user_tenant_id_, proxy_))) {
    LOG_WARN("fail to init restore_source_mgr", K(user_tenant_id_));
  } else if (OB_FAIL(restore_source_mgr.get_source(item))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      need_update = false;
      if (REACH_THREAD_TIME_INTERVAL(60 * 1000 * 1000)) {
        LOG_INFO("log restore source is empty, just skip", K(ret), K(user_tenant_id_));
      }
    } else {
      LOG_WARN("fail to get source", K(user_tenant_id_), K(ret));
    }
  } else if (!need_update_ip_list_(item)) {
    if (REACH_THREAD_TIME_INTERVAL(60 * 1000 * 1000)) {
      LOG_INFO("log restore source not exists or the log restore source type is not service" , K(item));
    }
  } else if (OB_FAIL(get_restore_source_value_(item, restore_source_value))) {
    LOG_WARN("fail to get restore source value", K(item));
  } else if (OB_FAIL(restore_source_attr.parse_service_attr_from_str(restore_source_value))) {
    LOG_WARN("fail to parse service attr", K(item), K(restore_source_value));
  } else if (!restore_source_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("service attr is invalid", K(restore_source_attr));
  } else {
    need_update = true;
  }
  return ret;
}

int ObPrimaryRestoreSourceAttrUpdater::update_restore_source_attr(const ObRestoreSourceServiceAttr &restore_source_attr)
{
  int ret = OB_SUCCESS;
  ObLogRestoreSourceMgr restore_source_mgr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (!restore_source_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("restore source attr is invalid", KR(ret), K(restore_source_attr));
  } else if (OB_FAIL(restore_source_mgr.init(user_tenant_id_, proxy_))) {
    LOG_WARN("fail to init restore_source_mgr", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(do_update_restore_source_(restore_source_attr, restore_source_mgr))) {
    LOG_WARN("fail to update restore source", KR(ret), K(user_tenant_id_), K(restore_source_attr));
  }
  return ret;
}

bool ObPrimaryRestoreSourceAttrUpdater::need_update_ip_list_(ObLogRestoreSourceItem &item)
{
  return item.is_valid() && ObLogRestoreSourceType::SERVICE == item.type_;
}

int ObPrimaryRestoreSourceAttrUpdater::do_update_restore_source_(
  const ObRestoreSourceServiceAttr &old_attr,
  ObLogRestoreSourceMgr &restore_source_mgr)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObLogRestoreSourceItem tmp_item;
  ObRestoreSourceServiceAttr tmp_service_attr;
  ObSqlString tmp_standby_source_value;
  char updated_value_str[OB_MAX_BACKUP_DEST_LENGTH + 1] = { 0 };
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id_);

  if (OB_FAIL(old_attr.gen_service_attr_str(updated_value_str, sizeof(updated_value_str)))) {
    LOG_WARN("gen service_attr_str failed", KR(ret), K(updated_value_str), K(old_attr));
  } else if (OB_FAIL(trans.start(proxy_, meta_tenant_id))) {
    LOG_WARN("fail to start trans", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(restore_source_mgr.get_source_for_update(tmp_item, trans))) {
    LOG_WARN("fail to get log restore source when double check" , KR(ret), K(tmp_item));
  } else if (ObLogRestoreSourceType::SERVICE != tmp_item.type_) {
    ret = OB_EAGAIN;
    LOG_WARN("log restore source type is not service", KR(ret), K(meta_tenant_id), K(tmp_item.type_));
  } else if (OB_FAIL(get_restore_source_value_(tmp_item, tmp_standby_source_value))) {
    LOG_WARN("fail to get service standby log restore source value", KR(ret), K(tmp_item));
  } else if (OB_FAIL(tmp_service_attr.parse_service_attr_from_str(tmp_standby_source_value))) {
    LOG_WARN("fail to parse service attr", KR(ret), K(tmp_item), K(tmp_standby_source_value));
  } else if (!tmp_service_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("service attr is invalid", K(tmp_service_attr));
  } else if (!(tmp_service_attr.user_ == old_attr.user_)) {
    ret = OB_EAGAIN;
    LOG_WARN("log restore source record may be modified", KR(ret), K(tmp_service_attr.user_), K(old_attr.user_));
  } else if (0 != STRCMP(tmp_service_attr.encrypt_passwd_, old_attr.encrypt_passwd_)) {
    ret = OB_EAGAIN;
    LOG_WARN("log restore source password may be modified", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(update_source_inner_table_(updated_value_str, sizeof(updated_value_str), trans, tmp_item))) {
    LOG_WARN("fail to add service source", KR(ret), K(updated_value_str), K(tmp_item.until_scn_));
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

int ObPrimaryRestoreSourceAttrUpdater::get_restore_source_value_(ObLogRestoreSourceItem &item, ObSqlString &standby_source_value)
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

int ObPrimaryRestoreSourceAttrUpdater::update_source_inner_table_(char *buf,
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
    uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id_);
    ObDMLSqlSplicer dml;
    ObSqlString sql;
    if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, user_tenant_id_))) {
      LOG_WARN("failed to add column", K(meta_tenant_id), K(item));
    } else if (OB_FAIL(dml.add_pk_column(OB_STR_LOG_RESTORE_SOURCE_ID, item.id_))) {
      LOG_WARN("failed to add column", K(meta_tenant_id), K(item));
    } else if (OB_FAIL(dml.add_column(OB_STR_LOG_RESTORE_SOURCE_VALUE, buf))) {
      LOG_WARN("failed to add column", K(meta_tenant_id), K(item));
    } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_LOG_RESTORE_SOURCE_TNAME, sql))) {
      LOG_WARN("fill update source value sql failed", K(item));
    } else if (OB_FAIL(trans.write(meta_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to exec sql", K(sql), K(meta_tenant_id));
    }
  }
  return ret;
}

ObStandbyRestoreSourceAttrUpdater::ObStandbyRestoreSourceAttrUpdater()
{
  net_timeout_ = 0;
  health_check_time_ = ObSyncStandbyDestStruct::OB_DEFAULT_HEALTH_CHECK_TIME;
}

ObStandbyRestoreSourceAttrUpdater::~ObStandbyRestoreSourceAttrUpdater()
{
  net_timeout_ = 0;
  health_check_time_ = ObSyncStandbyDestStruct::OB_DEFAULT_HEALTH_CHECK_TIME;
}

int ObStandbyRestoreSourceAttrUpdater::fetch_current_restore_source_attr(
  ObRestoreSourceServiceAttr &restore_source_attr, bool &need_update)
{
  int ret = OB_SUCCESS;
  ObSyncStandbyDestStruct standby_dest_struct;
  ObSyncStandbyDestOperator standby_dest_operator;
  bool is_empty = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(proxy_) || !is_valid_tenant_id(user_tenant_id_) || !is_user_tenant(user_tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("proxy is null or user tenant id is invalid", KR(ret), KP(proxy_), K(user_tenant_id_));
  } else if (OB_FAIL(standby_dest_operator.read_sync_standby_dest(*proxy_,
    gen_meta_tenant_id(user_tenant_id_), false/*for_update*/, is_empty, standby_dest_struct))) {
    LOG_WARN("fail to read sync standby dest", KR(ret), K(user_tenant_id_));
  } else if (is_empty) {
    need_update = false;
  } else if (!standby_dest_struct.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sync standby dest is invalid", K(standby_dest_struct));
  } else if (OB_FAIL(restore_source_attr.assign(standby_dest_struct.restore_source_service_attr_))) {
    LOG_WARN("fail to assign service attr", KR(ret), K(standby_dest_struct));
  } else {
    net_timeout_ = standby_dest_struct.net_timeout_;
    health_check_time_ = standby_dest_struct.health_check_time_;
    need_update = true;
  }
  return ret;
}

int ObStandbyRestoreSourceAttrUpdater::update_restore_source_attr(const ObRestoreSourceServiceAttr &restore_source_attr)
{
  int ret = OB_SUCCESS;
  ObSyncStandbyDestStruct standby_dest_struct;
  ObSyncStandbyDestOperator standby_dest_operator;
  ObMySQLTransaction trans;
  bool is_empty = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(proxy_) || !is_valid_tenant_id(user_tenant_id_)
      || !is_user_tenant(user_tenant_id_) || !restore_source_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(proxy_), K(user_tenant_id_), K(restore_source_attr));
  } else if (OB_FAIL(trans.start(proxy_, gen_meta_tenant_id(user_tenant_id_)))) {
    LOG_WARN("fail to start trans", KR(ret), K(user_tenant_id_));
  } else if (OB_FAIL(standby_dest_operator.read_sync_standby_dest(trans,
      gen_meta_tenant_id(user_tenant_id_), true/*for_update*/, is_empty, standby_dest_struct))) {
    LOG_WARN("fail to read sync standby dest", KR(ret), K(user_tenant_id_));
  } else if (is_empty || !standby_dest_struct.is_valid()) {
    ret = OB_NEED_RETRY;
    LOG_WARN("sync standby dest is not expected", KR(ret), K(user_tenant_id_), K(standby_dest_struct));
  } else if (!(restore_source_attr.user_ == standby_dest_struct.restore_source_service_attr_.user_)) {
    ret = OB_NEED_RETRY;
    LOG_WARN("log restore source record may be modified", KR(ret), K(restore_source_attr),
        K(standby_dest_struct));
  } else if (OB_FAIL(standby_dest_struct.restore_source_service_attr_.assign(restore_source_attr))) {
    LOG_WARN("fail to assign service attr", KR(ret), K(restore_source_attr));
  } else if (FALSE_IT(standby_dest_struct.net_timeout_ = net_timeout_)) {
  } else if (FALSE_IT(standby_dest_struct.health_check_time_ = health_check_time_)) {
  } else if (OB_FAIL(standby_dest_operator.write_sync_standby_dest(trans,
      gen_meta_tenant_id(user_tenant_id_), standby_dest_struct))) {
    LOG_WARN("fail to write sync standby dest", KR(ret), K(user_tenant_id_));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }
  return ret;
}

void ObCommonLSService::try_update_ip_list()
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", K_(tenant_id));
  } else if (!is_meta_tenant(tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", K_(tenant_id));
  } else {
    int tmp_ret = OB_SUCCESS;
    bool enabled = false;
    const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id_);
    if (!primary_restore_source_attr_updater_.is_inited() &&
        OB_TMP_FAIL(primary_restore_source_attr_updater_.init(user_tenant_id, proxy_))) {
      LOG_WARN("failed to init primary_restore_source_attr_updater_", KR(tmp_ret), K(user_tenant_id));
    } else if (OB_TMP_FAIL(primary_restore_source_attr_updater_.update_restore_source_attr_periodically())) {
      LOG_WARN("fail to update primary restore source attr", KR(tmp_ret), K(user_tenant_id));
    }
    if (OB_FAIL(standby::ObProtectionModeUtils::check_tenant_data_version_for_protection_mode(user_tenant_id, enabled))) {
      LOG_WARN("failed to check tenant enabled protection mode", KR(ret), K(user_tenant_id));
    } else if (!enabled) {
      if (REACH_TIME_INTERVAL(1_min)) {
        LOG_INFO("tenant protection mode is not enabled", KR(ret), K(user_tenant_id));
      }
    } else if (!standby_restore_source_attr_updater_.is_inited() &&
        OB_TMP_FAIL(standby_restore_source_attr_updater_.init(gen_user_tenant_id(tenant_id_), proxy_))) {
      LOG_WARN("failed to init standby_restore_source_attr_updater_", KR(tmp_ret), K(tenant_id_));
    } else if (OB_TMP_FAIL(standby_restore_source_attr_updater_.update_restore_source_attr_periodically())) {
      LOG_WARN("fail to update primary restore source attr", KR(tmp_ret), K(tenant_id_));
    }
  }
}

#undef RESTORE_EVENT_ADD

}//end of rootserver
}
