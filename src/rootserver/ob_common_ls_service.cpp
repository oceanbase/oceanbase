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
  }
  return ret;
}

void ObCommonLSService::destroy()
{
  ObTenantThreadHelper::destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  inited_ = false;
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
                                             false, palf_base_info))) {
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
    bool create_with_palf, const palf::PalfBaseInfo &palf_base_info)
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
                                         palf_base_info))) {
        LOG_WARN("failed to create user ls", KR(ret), K(info), K(locality_array), K(create_scn),
                                             K(palf_base_info), K(create_with_palf));
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

}//end of rootserver
}
