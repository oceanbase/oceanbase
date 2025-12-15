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
#include "ob_balance_ls_primary_zone.h"
#include "share/ls/ob_ls_life_manager.h"//ObLSLifeAgentManager
#include "share/ob_primary_zone_util.h"//ObPrimaryZoneUtil
#include "observer/ob_server_struct.h"//GCTX
#include "rootserver/ob_tenant_thread_helper.h"//get_zone_priority

namespace oceanbase
{
using namespace common;
using namespace share;
namespace rootserver
{

int ObBalanceLSPrimaryZone::try_adjust_user_ls_primary_zone(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  share::ObLSStatusOperator status_op;
  share::ObLSPrimaryZoneInfoArray info_array;
  ObArray<ObArray<ObZone>> multi_level_primary_zone_array;
  share::schema::ObTenantSchema tenant_schema;
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant is invalid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_multi_level_primary_zone_array(
                   tenant_schema, multi_level_primary_zone_array))) {
    LOG_WARN("failed to get tenant primary zone array", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(status_op.get_ls_primary_zone_info_by_order_ls_group(
          tenant_id, info_array, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to get ls primary zone info array", KR(ret), K(tenant_id));
  } else {
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
          if (OB_FAIL(adjust_primary_zone_by_ls_group_(multi_level_primary_zone_array, tmp_info_array))) {
            LOG_WARN("failed to update primary zone of each ls group", KR(ret),
                K(tmp_info_array), K(multi_level_primary_zone_array));
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
      if (OB_FAIL(adjust_primary_zone_by_ls_group_(multi_level_primary_zone_array, tmp_info_array))) {
        LOG_WARN("failed to update primary zone of each ls group", KR(ret),
            K(tmp_info_array), K(multi_level_primary_zone_array));
      }
    }
  }
  return ret;
}

// multi_level_primary_zone_array is parsed from tenant primary_zone str, according to which
//   we will decide each LS primary_zone_infos in same ls group, including primary_zone and zone_priority.
// * zone_priority is a string, which consists of all zones of all levels.
//   The order between levels keep unchanged, while order of zones inside each level differs.
// * primary_zone only consist first zone of first level in zone_priority.
// The rule is that: in each level, first zone of each ls are evenly distributed in each zone of the level.
// eg:
// tenant primary_zone str: z1,z2;z3,z4
// multi_level_primary_zone_array:
//   level 0: z1, z2
//   level 1: z3, z4
// After balance, we may have:
//   LS1: primary_zone: z1, zone_priority: z1;z2;z3;z4
//   LS2: primary_zone: z2, zone_priority: z2;z1;z4;z3
// In that case, for level 0, z1 and z2 both have 1 ls taking it as first zone.
//   for level 1, z3 and z4 both have 1 ls taking it as first zone.
int ObBalanceLSPrimaryZone::adjust_primary_zone_by_ls_group_(
    const common::ObIArray<common::ObArray<common::ObZone>> &multi_level_primary_zone_array,
    const ObIArray<ObLSPrimaryZoneInfo> &primary_zone_infos)
{
  int ret = OB_SUCCESS;
  const int64_t ls_count = primary_zone_infos.count();
  const int64_t level_count = multi_level_primary_zone_array.count();
  if (OB_UNLIKELY(0 == level_count || 0 == ls_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(primary_zone_infos),
        K(multi_level_primary_zone_array));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (1 == ls_count
      && primary_zone_infos.at(0).get_ls_id().is_sys_ls()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not be sys ls", KR(ret), K(ls_count), K(primary_zone_infos));
  } else {
    // Init arrays for storing balance result: primary_zone and zone_priority.
    // Following two arrays are match with primary_zone_infos
    ObArray<ObZone> new_primary_zone_array;
    ObArray<ObSqlString> new_zone_priority_array;
    ObSqlString empty_zone_priority;
    if (OB_FAIL(empty_zone_priority.assign(""))) {
      LOG_WARN("failed to assign", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_count; ++i) {
      if (OB_FAIL(new_zone_priority_array.push_back(empty_zone_priority))) {
        LOG_WARN("failed to push back", KR(ret), K(empty_zone_priority));
      }
    }

    // Balance first zone for each level
    for (int64_t level_idx = 0; OB_SUCC(ret) && level_idx < level_count; ++level_idx) {
      //Algorithm Description:
      //Assumption: We have 5 ls, and in this level there are 3 primary_zones(z1, z2, z3).
      //1. Set the primary zone of ls to one of tenant's primary zone of this level,
      //choose the least number of log streams on the zone is selected for all zones
      //2. After all the primary_zone of the ls are in the primary_zone of the tenant,
      //choose the primary_zone with the most and least ls. Adjust a certain number of ls to the smallest zone without exceeding the balance
      //while guaranteeing that the number of the zone with the most is no less than the average.
      const ObArray<ObZone> &primary_zone_array = multi_level_primary_zone_array.at(level_idx);
      ObArray<ObZone> ls_primary_zone;// first zone of each ls in this level
      ObSEArray<uint64_t, 3> count_group_by_zone;//ls count placing in first place of each zone
      if (OB_FAIL(set_ls_to_primary_zone_(primary_zone_array, primary_zone_infos, 0 == level_idx/*is_first_level*/,
            ls_primary_zone, count_group_by_zone))) {
        LOG_WARN("failed to set ls to primary zone", KR(ret), K(primary_zone_array), K(primary_zone_infos));
      } else if (OB_FAIL(balance_ls_primary_zone_(primary_zone_array, ls_primary_zone, count_group_by_zone))) {
        LOG_WARN("failed to balance ls primary zone", KR(ret), K(ls_primary_zone), K(count_group_by_zone));
      } else if (0 == level_idx && OB_FAIL(new_primary_zone_array.assign(ls_primary_zone))) {
        LOG_WARN("failed to assign primary zone", KR(ret), K(ls_primary_zone));
      }

      // update zone_priority for each ls, append level primary zone first, then other zones
      CK(new_zone_priority_array.count() == ls_count);
      CK(ls_primary_zone.count() == ls_count);
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_count; ++i) {
        ObSqlString &new_zone_priority = new_zone_priority_array.at(i);
        const char *separator_token = new_zone_priority.empty() ? "" : ";";
        if (OB_FAIL(new_zone_priority.append_fmt("%s%s", separator_token, ls_primary_zone.at(i).ptr()))) {
          LOG_WARN("failed to append first zone in level", KR(ret), K(ls_primary_zone.at(i)));
        } else {
          separator_token = ";";  // same level first and other zones separate by ";"
          for (int64_t j = 0; OB_SUCC(ret) && j < primary_zone_array.count(); ++j) {
            if (primary_zone_array.at(j) != ls_primary_zone.at(i)) {
              if (OB_FAIL(new_zone_priority.append_fmt("%s%s", separator_token, primary_zone_array.at(j).ptr()))) {
                LOG_WARN("failed to append other zone in level", KR(ret), K(ls_primary_zone.at(i)), K(primary_zone_array.at(j)));
              }
              separator_token = ",";  // same level other zones separate by ","
            }
          }
        }
      } // end for each ls
    } // end for each level

    // Update all ls primary_zone and zone_priority according to balance result
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_count; ++i) {
      const ObZone &new_primary_zone = new_primary_zone_array.at(i);
      const ObSqlString &new_zone_priority = new_zone_priority_array.at(i);
      if (OB_FAIL(try_update_ls_primary_zone(primary_zone_infos.at(i), new_primary_zone, new_zone_priority))) {
        LOG_WARN("failed to update ls primary zone", KR(ret), "primary_zone_info", primary_zone_infos.at(i),
            K(new_primary_zone), K(new_zone_priority));
      }
    }// end for each ls
  }
  return ret;
}

int ObBalanceLSPrimaryZone::set_ls_to_primary_zone_(
    const common::ObIArray<ObZone> &primary_zone_array,
    const ObIArray<ObLSPrimaryZoneInfo> &primary_zone_infos,
    const bool is_first_level,
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
    const ObZone empty_zone;
    // For first level, try to keep current primary_zone.
    // For other levels, just set empty zone first, and re-choose all ls primary_zone to min count zone later
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_count; ++i) {
      const ObZone &current_zone = is_first_level ? primary_zone_infos.at(i).get_primary_zone() : empty_zone;
      if (is_first_level && has_exist_in_array(primary_zone_array, current_zone, &index)) {
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

int ObBalanceLSPrimaryZone::balance_ls_primary_zone_(
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

int ObBalanceLSPrimaryZone::try_update_ls_primary_zone(
    const share::ObLSPrimaryZoneInfo &primary_zone_info,
    const common::ObZone &new_primary_zone,
    const common::ObSqlString &new_zone_priority)
{
  int ret = OB_SUCCESS;
  bool need_update = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(need_update_ls_primary_zone(
      primary_zone_info,
      new_primary_zone,
      new_zone_priority,
      need_update))) {
    LOG_WARN("fail to check need_update_ls_primary_zone", KR(ret), K(primary_zone_info),
        K(new_primary_zone), K(new_zone_priority));
  } else if (need_update) {
    ObLSLifeAgentManager ls_life_agent(*GCTX.sql_proxy_);
    if (OB_FAIL(ls_life_agent.update_ls_primary_zone(primary_zone_info.get_tenant_id(),
            primary_zone_info.get_ls_id(),
            new_primary_zone, new_zone_priority.string()))) {
      LOG_WARN("failed to update ls primary zone", KR(ret), K(primary_zone_info),
          K(new_primary_zone), K(new_zone_priority));
    }
    LOG_INFO("update ls primary zone", KR(ret), K(new_primary_zone),
        K(new_zone_priority), K(primary_zone_info));
  } else {
    //no need update
  }
  return ret;
}

int ObBalanceLSPrimaryZone::need_update_ls_primary_zone (
    const share::ObLSPrimaryZoneInfo &primary_zone_info,
    const common::ObZone &new_primary_zone,
    const common::ObSqlString &new_zone_priority,
    bool &need_update)
{
  int ret = OB_SUCCESS;
  need_update = false;
  if (OB_UNLIKELY(!primary_zone_info.is_valid()
        || new_primary_zone.is_empty() || new_zone_priority.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("primary zone info is invalid", KR(ret), K(primary_zone_info),
        K(new_primary_zone), K(new_zone_priority));
  } else if (new_primary_zone != primary_zone_info.get_primary_zone()
      || new_zone_priority.string() != primary_zone_info.get_zone_priority_str()) {
    need_update = true;
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_UPDATE_SYS_LS_PRIMARY_ZONE);
int ObBalanceLSPrimaryZone::try_update_sys_ls_primary_zone(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  share::ObLSPrimaryZoneInfo primary_zone_info;
  ObZone new_primary_zone;
  ObSqlString new_zone_priority;
  share::ObLSStatusOperator status_op;
  share::ObLSPrimaryZoneInfo sslog_primary_zone_info;
  if (OB_UNLIKELY(is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("user tenant no need update sys ls primary zone", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(ERRSIM_UPDATE_SYS_LS_PRIMARY_ZONE)) {
    LOG_WARN("ERRSIM_UPDATE_SYS_LS_PRIMARY_ZONE opened, do nothing", KR(ret), K(tenant_id));
  } else if (OB_FAIL(prepare_sys_ls_balance_primary_zone_info(
      tenant_id,
      primary_zone_info,
      new_primary_zone,
      new_zone_priority))) {
    LOG_WARN("fail to prepare sys_ls_balance_primary_zone_info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(try_update_ls_primary_zone(
          primary_zone_info, new_primary_zone, new_zone_priority))) {
    LOG_WARN("failed to update ls primary zone", KR(ret), K(primary_zone_info),
        K(new_primary_zone), K(new_zone_priority));
  } else if (is_meta_tenant(tenant_id)) {
    //user sys ls and meta sslog ls has same primary zone with meta sys ls
    share::ObLSPrimaryZoneInfo user_primary_zone_info;
    const uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
    if (OB_FAIL(status_op.get_ls_primary_zone_info(user_tenant_id, SYS_LS,
        user_primary_zone_info, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to get ls primary_zone info", KR(ret), K(tenant_id), K(user_tenant_id));
    } else if (OB_FAIL(try_update_ls_primary_zone(
        user_primary_zone_info, new_primary_zone,
        new_zone_priority))) {
      LOG_WARN("failed to update ls primary zone", KR(ret), K(user_primary_zone_info),
          K(new_primary_zone), K(new_zone_priority));
    }
  }
  if (OB_SUCC(ret) && GCTX.is_shared_storage_mode()) {
    // sys and meta tenant need update sslog primary zone info
    if (OB_FAIL(status_op.get_ls_primary_zone_info(tenant_id, SSLOG_LS, sslog_primary_zone_info, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to get ls primary_zone info", KR(ret), K(tenant_id));
    } else if (OB_FAIL(try_update_ls_primary_zone(sslog_primary_zone_info, new_primary_zone, new_zone_priority))) {
      LOG_WARN("failed to update ls primary zone", KR(ret), K(sslog_primary_zone_info), K(new_primary_zone), K(new_zone_priority));
    }
  }
  return ret;
}

int ObBalanceLSPrimaryZone::prepare_sys_ls_balance_primary_zone_info(
    const uint64_t tenant_id,
    share::ObLSPrimaryZoneInfo &primary_zone_info,
    common::ObZone &new_primary_zone,
    common::ObSqlString &new_zone_priority)
{
  int ret = OB_SUCCESS;
  share::schema::ObTenantSchema tenant_schema;
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", KR(ret), KP(GCTX.sql_proxy_), KP(GCTX.schema_service_));
  } else if (OB_UNLIKELY(is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only meta ans sys tenant are allowed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObTenantThreadHelper::get_tenant_schema(tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(tenant_id));
  } else {
    share::ObLSStatusOperator status_op;
    ObArray<common::ObZone> primary_zone;
    if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_primary_zone_array(
            tenant_schema, primary_zone))) {
      LOG_WARN("failed to get tenant primary zone array", KR(ret), K(tenant_schema));
    } else if (OB_FAIL(status_op.get_ls_primary_zone_info(tenant_id,
            SYS_LS, primary_zone_info, *GCTX.sql_proxy_))) {
      LOG_WARN("failed to get ls primary_zone info", KR(ret), K(tenant_id));
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
    } else if (is_sys_tenant(tenant_id)) {
      //sys tenant use tenant normalize primary zone
      if (OB_FAIL(ObPrimaryZoneUtil::get_tenant_zone_priority(
          tenant_schema, new_zone_priority))) {
        LOG_WARN("failed to get tenant primary zone array", KR(ret), K(tenant_schema));
      }
    } else if (OB_FAIL(ObTenantThreadHelper::get_zone_priority(new_primary_zone,
        tenant_schema, new_zone_priority))) {
      LOG_WARN("failed to get normalize primary zone", KR(ret), K(new_primary_zone));
    }
  }
  return ret;
}

int ObBalanceLSPrimaryZone::check_sys_ls_primary_zone_balanced(const uint64_t tenant_id, int &check_ret)
{
  int ret = OB_SUCCESS;
  share::ObLSPrimaryZoneInfo primary_zone_info;
  ObZone new_primary_zone;
  ObSqlString new_zone_priority;
  bool need_update = false;
  check_ret = OB_NEED_WAIT;
  if (OB_UNLIKELY(is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only meta ans sys tenant are allowed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(prepare_sys_ls_balance_primary_zone_info(
      tenant_id,
      primary_zone_info,
      new_primary_zone,
      new_zone_priority))) {
    LOG_WARN("fail to prepare sys_ls_balance_primary_zone_info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(need_update_ls_primary_zone(
      primary_zone_info,
      new_primary_zone,
      new_zone_priority,
      need_update))) {
    LOG_WARN("fail to check need_update_ls_primary_zone", KR(ret), K(primary_zone_info),
        K(new_primary_zone), K(new_zone_priority));
  } else if (!need_update) {
    check_ret = OB_SUCCESS;
  } else {
    check_ret = OB_NEED_WAIT;
  }
  return ret;
}
}//end of rootserver
}
