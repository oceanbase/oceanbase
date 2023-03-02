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
#include "rootserver/ob_tenant_info_loader.h"//get_tenant_info
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
  tenant_id_ = MTL_ID();
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("has inited", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::create("COMMONLSSe",
          lib::TGDefIDs::SimpleLSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("fail to start", KR(ret));
  } else {
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
  } else if (OB_FAIL(wait_tenant_schema_and_version_ready_(tenant_id_, DATA_VERSION_4_1_0_0))) {
    LOG_WARN("failed to wait tenant schema version ready", KR(ret), K(tenant_id_), K(DATA_CURRENT_VERSION));
  } else {
    int64_t idle_time_us = 100 * 1000L;
    share::schema::ObTenantSchema user_tenant_schema;
    while (!has_set_stop()) {
      ret = OB_SUCCESS;
      ObCurTraceId::init(GCONF.self_addr_);
      if (is_meta_tenant(tenant_id_)) {
        const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id_);
        if (OB_FAIL(get_tenant_schema(user_tenant_id, user_tenant_schema))) {
          LOG_WARN("failed to get user tenant schema", KR(ret), K(user_tenant_id));
        } else if (user_tenant_schema.is_dropping()) {
          if (OB_FAIL(try_force_drop_tenant_(user_tenant_schema))) {
            LOG_WARN("failed to force drop tenant", KR(ret), K(user_tenant_id));
          }
        } else if (OB_FAIL(try_create_ls_(user_tenant_schema))) {
          LOG_WARN("failed to create ls", KR(ret), K(user_tenant_schema));
        }
      }

      if (REACH_TENANT_TIME_INTERVAL(1 * 1000 * 1000)) {
        ret = OB_SUCCESS;//ignore error
        if (OB_FAIL(try_update_sys_ls_primary_zone_())) {
          LOG_WARN("failed to update sys ls primary zone", KR(ret));
        }
        if (is_meta_tenant(tenant_id_)) {
          if (FAILEDx(try_adjust_user_ls_primary_zone_(user_tenant_schema))) {
            LOG_WARN("failed to adjust user ls primary zone", KR(ret), K(user_tenant_schema));
          }
        }
      }

      user_tenant_schema.reset();
      idle(idle_time_us);
      LOG_INFO("[COMMON_LS_SERVICE] finish one round", KR(ret), K(idle_time_us));
    }  // end while
  }
}

int ObCommonLSService::try_update_sys_ls_primary_zone_()
{
  int ret = OB_SUCCESS;
  share::schema::ObTenantSchema tenant_schema;
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else if (OB_FAIL(get_tenant_schema(tenant_id_, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id_));
  } else {
    share::ObLSPrimaryZoneInfo primary_zone_info;
    ObArray<common::ObZone> primary_zone;
    share::ObLSStatusOperator status_op;
    ObZone new_primary_zone;
    ObSqlString new_zone_priority;
    if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_primary_zone_array(
            tenant_schema, primary_zone))) {
      LOG_WARN("failed to get tenant primary zone array", KR(ret), K(tenant_schema));
    } else if (OB_FAIL(status_op.get_ls_primary_zone_info(tenant_id_,
            SYS_LS, primary_zone_info, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to get ls primary_zone info", KR(ret), K(tenant_id_));
    } else if (OB_UNLIKELY(0 == primary_zone.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("primary zone is empty", KR(ret), K(tenant_schema));
    } else if (has_exist_in_array(primary_zone, primary_zone_info.get_primary_zone())) {
      if (OB_FAIL(new_primary_zone.assign(primary_zone_info.get_primary_zone()))) {
        LOG_WARN("failed to assign primary zone", KR(ret), K(primary_zone_info));
      }
    } else if (OB_FAIL(new_primary_zone.assign(primary_zone.at(0)))) {
      LOG_WARN("failed to assign primary zone", KR(ret), K(primary_zone));
    }
    if (OB_FAIL(ret)) {
    } else if (is_sys_tenant(tenant_id_)) {
      //sys tenant use tenant normalize primary zone
      if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_zone_priority(
              tenant_schema, new_zone_priority))) {
        LOG_WARN("failed to get tenant primary zone array", KR(ret), K(tenant_schema));
      }
    } else if (OB_FAIL(get_zone_priority(new_primary_zone,
            tenant_schema, new_zone_priority))) {
      LOG_WARN("failed to get normalize primary zone", KR(ret), K(new_primary_zone));
    }
    if (FAILEDx(try_update_ls_primary_zone_(
            primary_zone_info, new_primary_zone, new_zone_priority))) {
      LOG_WARN("failed to update ls primary zone", KR(ret), K(primary_zone_info),
          K(new_primary_zone), K(new_zone_priority));
    } else if (is_meta_tenant(tenant_id_)) {
      //user sys ls has same primary zone with meta sys ls
      share::ObLSPrimaryZoneInfo user_primary_zone_info;
      const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id_);
      if (OB_FAIL(status_op.get_ls_primary_zone_info(user_tenant_id, SYS_LS,
              user_primary_zone_info, *GCTX.sql_proxy_))) {
        LOG_WARN("failed to get ls primary_zone info", KR(ret), K(tenant_id_), K(user_tenant_id));
      } else if (OB_FAIL(try_update_ls_primary_zone_(user_primary_zone_info, new_primary_zone, new_zone_priority))) {
        LOG_WARN("failed to update ls primary zone", KR(ret), K(user_primary_zone_info),
            K(new_primary_zone), K(new_zone_priority));
      }
    }
  }
  return ret;
}

int ObCommonLSService::try_adjust_user_ls_primary_zone_(const share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  share::ObLSStatusOperator status_op;
  share::ObLSPrimaryZoneInfoArray info_array;
  ObArray<common::ObZone> primary_zone;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else if (!is_user_tenant(tenant_id) || !tenant_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant iS invalid", KR(ret), K(tenant_id), K(tenant_schema));
  } else if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_primary_zone_array(
                   tenant_schema, primary_zone))) {
    LOG_WARN("failed to get tenant primary zone array", KR(ret), K(tenant_schema));
  } else {
    if (OB_FAIL(status_op.get_tenant_primary_zone_info_array(
          tenant_id, info_array, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to get tenant primary zone info array", KR(ret), K(tenant_id));
    }
  }

  if (OB_SUCC(ret)) {
    uint64_t last_ls_group_id = OB_INVALID_ID;
    share::ObLSPrimaryZoneInfoArray tmp_info_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < info_array.count(); ++i) {
      //TODO ls group id maybe zero
      const ObLSPrimaryZoneInfo &info = info_array.at(i);
      if (!info.get_ls_id().is_sys_ls()) {
        if (OB_INVALID_ID == last_ls_group_id) {
          last_ls_group_id = info.get_ls_group_id();
        }
        if (last_ls_group_id != info.get_ls_group_id()) {
          //process the ls group
          if (OB_FAIL(adjust_primary_zone_by_ls_group_(primary_zone, tmp_info_array, tenant_schema))) {
            LOG_WARN("failed to update primary zone of each ls group", KR(ret),
                K(tmp_info_array), K(primary_zone), K(tenant_schema));
          } else {
            tmp_info_array.reset();
            last_ls_group_id = info.get_ls_group_id();
          }
        }
        if (FAILEDx(tmp_info_array.push_back(info))) {
          LOG_WARN("failed to push back primary info array", KR(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret) && 0 < tmp_info_array.count()) {
      if (OB_FAIL(adjust_primary_zone_by_ls_group_(primary_zone, tmp_info_array, tenant_schema))) {
        LOG_WARN("failed to update primary zone of each ls group", KR(ret),
            K(tmp_info_array), K(primary_zone), K(tenant_schema));
      }
    }
  }

  return ret;
}
//check every ls has right primary zone in ls group
//if primary_zone of ls not in primary zone, try to choose a right zone,
//if can not find a zone to modify, modify the ls to exist zone,
//load_balancer will choose the right ls to drop
//eg:
//if ls group has z1, z2 and primary zone is : z1. need modify z2 to z1
//if ls group has z1, z2 and primary zone is : z1, z3. modify z2 to z3;
int ObCommonLSService::adjust_primary_zone_by_ls_group_(
    const common::ObIArray<common::ObZone> &primary_zone_array,
    const share::ObLSPrimaryZoneInfoArray &primary_zone_infos,
    const share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  const int64_t ls_count = primary_zone_infos.count();
  const int64_t primary_zone_count = primary_zone_array.count();
  if (OB_UNLIKELY(0 == primary_zone_count
        || 0 == ls_count || !tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(primary_zone_infos),
        K(primary_zone_array), K(tenant_schema));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (1 == ls_count
      && primary_zone_infos.at(0).get_ls_id().is_sys_ls()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not be sys ls", KR(ret), K(ls_count), K(primary_zone_infos));
  } else {
    //Algorithm Description:
    //Assumption: We have 5 ls, 3 primary_zones(z1, z2, z3).
    //1. Set the primary zone of ls to  tenant's primary zone,
    //choose the least number of log streams on the zone is selected for all zones
    //2. After all the primary_zone of the ls are in the primary_zonw of the tenant,
    //choose the primary_zone with the most and least ls. Adjust a certain number of ls to the smallest zone without exceeding the balance
    //while guaranteeing that the number of the zone with the most is no less than the average.
    ObArray<ObZone> ls_primary_zone;//is match with primary_zone_infos
    ObSEArray<uint64_t, 3> count_group_by_zone;//ls count of each primary zone
    ObSqlString new_zone_priority;
    if (OB_FAIL(set_ls_to_primary_zone(primary_zone_array, primary_zone_infos, ls_primary_zone,
            count_group_by_zone))) {
      LOG_WARN("failed to set ls to primary zone", KR(ret), K(primary_zone_array), K(primary_zone_infos));
    } else if (OB_FAIL(balance_ls_primary_zone(primary_zone_array, ls_primary_zone, count_group_by_zone))) {
      LOG_WARN("failed to balance ls primary zone", KR(ret), K(ls_primary_zone), K(count_group_by_zone));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < ls_count; ++i) {
      const ObZone &new_primary_zone = ls_primary_zone.at(i);
      if (OB_FAIL(get_zone_priority(new_primary_zone, tenant_schema, new_zone_priority))) {
        LOG_WARN("failed to get normalize primary zone", KR(ret), K(new_primary_zone));
      }
      if (FAILEDx(try_update_ls_primary_zone_(primary_zone_infos.at(i), new_primary_zone, new_zone_priority))) {
        LOG_WARN("failed to update ls primary zone", KR(ret), "primary_zone_info", primary_zone_infos.at(i),
            K(new_primary_zone), K(new_zone_priority));
      }
    }
  }
  return ret;
}

int ObCommonLSService::set_ls_to_primary_zone(
    const common::ObIArray<ObZone> &primary_zone_array,
    const share::ObLSPrimaryZoneInfoArray &primary_zone_infos,
    common::ObIArray<common::ObZone> &ls_primary_zone,
    common::ObIArray<uint64_t> &count_group_by_zone)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == primary_zone_array.count()
        || 0 == primary_zone_infos.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(primary_zone_array), K(primary_zone_infos));
  } else {
    const int64_t primary_zone_count = primary_zone_array.count();
    const int64_t ls_count = primary_zone_infos.count();
    //ls may not in primary zone, record the index of primary_zone_infos not in primary zone
    ObSEArray<int64_t, 3> index_not_primary_zone;
    int64_t index = 0;
    ARRAY_FOREACH_X(primary_zone_array, idx, cnt, OB_SUCC(ret)) {
      if (OB_FAIL(count_group_by_zone.push_back(0))) {
        LOG_WARN("failed to push back", KR(ret), K(idx));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_count; ++i) {
      const ObZone &current_zone = primary_zone_infos.at(i).get_primary_zone();
      if (has_exist_in_array(primary_zone_array, current_zone, &index)) {
        count_group_by_zone.at(index)++;
      } else if (OB_FAIL(index_not_primary_zone.push_back(i))) {
        LOG_WARN("failed to push back", KR(ret), K(i), K(current_zone));
      }
      if (FAILEDx(ls_primary_zone.push_back(current_zone))) {
        LOG_WARN("failed to push back current zone", KR(ret), K(i), K(current_zone));
      }
    }
    //1. take all ls primary zone to tenant primary zone, choose the less primary zone count
    int64_t min_count = INT64_MAX;
    int64_t min_index = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_not_primary_zone.count(); ++i) {
      const int64_t ls_index = index_not_primary_zone.at(i);
      min_count = INT64_MAX;
      ARRAY_FOREACH_X(primary_zone_array, idx, cnt, OB_SUCC(ret)) {
        if (min_count > count_group_by_zone.at(idx)) {
          min_count = count_group_by_zone.at(idx);
          min_index = idx;
        }
      }//end for search min count
      if (OB_FAIL(ret)) {
      } else if (min_index >= primary_zone_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to found min count", KR(ret), K(min_index), K(primary_zone_array));
      } else if (OB_FAIL(ls_primary_zone.at(ls_index).assign(primary_zone_array.at(min_index)))) {
        LOG_WARN("failed to assign primary zone", KR(ret), K(min_index), K(min_count), K(ls_index));
      } else {
        count_group_by_zone.at(min_index)++;
      }
    }
  }
  return ret;

}

int ObCommonLSService::balance_ls_primary_zone(
    const common::ObIArray<common::ObZone> &primary_zone_array,
    common::ObIArray<common::ObZone> &ls_primary_zone,
    common::ObIArray<uint64_t> &count_group_by_zone)
{
  int ret = OB_SUCCESS;
  const int64_t ls_count = ls_primary_zone.count();
  const int64_t primary_zone_count = count_group_by_zone.count();
  if (OB_UNLIKELY(0 == primary_zone_count
        || 0 == ls_count
        || primary_zone_count != primary_zone_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(count_group_by_zone), K(ls_primary_zone));
  } else {
    int64_t max_count = -1, max_index = 0;
    int64_t min_count = INT64_MAX, min_index = 0;
    do {
      max_count = -1, max_index = 0;
      min_count = INT64_MAX, min_index = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < primary_zone_count; ++i) {
        const int64_t ls_count = count_group_by_zone.at(i);
        if (min_count > ls_count) {
          min_count = ls_count;
          min_index = i;
        }
        if (max_count < ls_count) {
          max_count = ls_count;
          max_index = i;
        }
      }//end for find min and max count
      if (OB_UNLIKELY(max_index >= primary_zone_count || min_index >= primary_zone_count
            || -1 == max_count || INT64_MAX == min_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get primary zone", KR(ret), K(max_index), K(min_index),
            K(min_count), K(max_count),
            K(primary_zone_array), K(primary_zone_count), K(ls_primary_zone));
      } else if (max_count - min_count > 1) {
        for (int64_t i = 0; OB_SUCC(ret) && i < ls_count; ++i) {
          if (ls_primary_zone.at(i) == primary_zone_array.at(max_index)) {
            if (OB_FAIL(ls_primary_zone.at(i).assign(primary_zone_array.at(min_index)))) {
              LOG_WARN("failed to push back ls primary zone", KR(ret), K(min_index));
            } else {
              count_group_by_zone.at(max_index)--;
              count_group_by_zone.at(min_index)++;
            }
            break;//only change one by one
          }
        }
      }
    } while (max_count - min_count > 1);
  }
  return ret;
}

int ObCommonLSService::try_update_ls_primary_zone_(
    const share::ObLSPrimaryZoneInfo &primary_zone_info,
    const common::ObZone &new_primary_zone,
    const common::ObSqlString &zone_priority)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!primary_zone_info.is_valid()
        || new_primary_zone.is_empty() || zone_priority.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("primary zone info is invalid", KR(ret), K(primary_zone_info),
        K(new_primary_zone), K(zone_priority));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (new_primary_zone != primary_zone_info.get_primary_zone()
      || zone_priority.string() != primary_zone_info.get_zone_priority_str()) {
    ObLSLifeAgentManager ls_life_agent(*GCTX.sql_proxy_);
    if (OB_FAIL(ls_life_agent.update_ls_primary_zone(primary_zone_info.get_tenant_id(),
            primary_zone_info.get_ls_id(),
            new_primary_zone, zone_priority.string()))) {
      LOG_WARN("failed to update ls primary zone", KR(ret), K(primary_zone_info),
          K(new_primary_zone), K(zone_priority));
    }
    LOG_INFO("[COMMON_LS_SERVICE] update ls primary zone", KR(ret), K(new_primary_zone),
        K(zone_priority), K(primary_zone_info));
  } else {
    //no need update
  }
  return ret;
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
      LOG_WARN("failed to update ls status", KR(ret), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
      const ObLSStatusInfo &status_info = status_info_array.at(i);
      if (status_info.ls_is_creating()) {
        recovery_stat.reset();
        if (OB_FAIL(check_can_create_ls_(tenant_schema))) {
          LOG_WARN("failed to wait can do recovery", KR(ret), K(tenant_schema));
        } else if (OB_FAIL(ls_recovery_operator.get_ls_recovery_stat(
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

int ObCommonLSService::check_can_create_ls_(const share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy  is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (!is_user_tenant(tenant_id) || !tenant_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", KR(ret), K(tenant_id), K(tenant_schema));
  } else if (tenant_schema.is_restore()) {
    //meta and user tenant unit must in the same observer
    MTL_SWITCH(tenant_id) {
      ObAllTenantInfo tenant_info;
      ObTenantInfoLoader* tenant_loader = MTL(rootserver::ObTenantInfoLoader*);
      if (OB_ISNULL(tenant_loader)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant report is null", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(tenant_loader->get_tenant_info(tenant_info))) {
        LOG_WARN("failed to get tenant info", KR(ret), K(tenant_id));
      } else if (OB_FAIL(check_can_do_recovery(tenant_info))) {
        LOG_WARN("failed to wait can do recovery", KR(ret), K(tenant_info), K(tenant_id));
      }
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
    LOG_INFO("try drop tenant", K(tenant_id));
    ObLSStatusOperator op;
    share::ObLSStatusInfoArray ls_array;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
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
  }
  return ret;
}


}//end of rootserver
}
