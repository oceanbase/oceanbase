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

#define USING_LOG_PREFIX RS_LB
#include "ob_server_balancer.h"
#include "ob_balance_info.h"
#include "lib/container/ob_array_iterator.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_file_system_router.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_unit_stat_manager.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_root_service.h"
#include "storage/ob_file_system_router.h"
#include "share/ob_all_server_tracer.h"
#include "share/ob_server_table_operator.h"
#include "rootserver/ob_heartbeat_service.h"
#include "share/ob_share_util.h" // ObShareUtil
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;
ObServerBalancer::ObServerBalancer()
    :inited_(false),
     schema_service_(NULL),
     unit_mgr_(NULL),
     zone_mgr_(NULL),
     server_mgr_(NULL),
     unit_stat_mgr_(),
     count_balance_strategy_(*this),
     inner_ttg_balance_strategy_(count_balance_strategy_),
     zone_disk_statistic_()
{}

ObServerBalancer::~ObServerBalancer() {}

int ObServerBalancer::init(
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObUnitManager &unit_manager,
    ObZoneManager &zone_mgr,
    ObServerManager &server_mgr,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(unit_stat_mgr_.init(sql_proxy))) {
    LOG_WARN("init unit_stat_mgr failed", KR(ret));
  } else {
    schema_service_ = &schema_service;
    unit_mgr_ = &unit_manager;
    zone_mgr_ = &zone_mgr;
    server_mgr_ = &server_mgr;
    inited_ = true;
  }
  return ret;
}

int ObServerBalancer::get_active_servers_info_and_resource_info_of_zone(
      const ObZone &zone,
      ObIArray<share::ObServerInfoInTable> &servers_info,
      ObIArray<obrpc::ObGetServerResourceInfoResult> &server_resources_info)
{
  int ret = OB_SUCCESS;
  servers_info.reset();
  server_resources_info.reset();
  ObServerResourceInfo resource_info_in_server_status;
  obrpc::ObGetServerResourceInfoResult resource_info_result;
  if (OB_FAIL(SVR_TRACER.get_active_servers_info(zone, servers_info))) {
    LOG_WARN("fail to execute get_active_servers_info", KR(ret), K(zone));
  } else if (OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_mgr_ is null", KR(ret), KP(server_mgr_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < servers_info.count(); i++) {
      const ObAddr &server = servers_info.at(i).get_server();
      resource_info_result.reset();
      resource_info_in_server_status.reset();
      if (OB_FAIL(server_mgr_->get_server_resource_info(server, resource_info_in_server_status))) {
        LOG_WARN("fail to get resource_info_in_server_status", KR(ret), K(server));
      } else if (OB_UNLIKELY(!resource_info_in_server_status.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid resource_info_in_server_status", KR(ret), K(server), K(resource_info_in_server_status));
      } else if (OB_FAIL(resource_info_result.init(server,resource_info_in_server_status))) {
        LOG_WARN("fail to init", KR(ret), K(server), K(resource_info_in_server_status));
      } else if (OB_FAIL(server_resources_info.push_back(resource_info_result))) {
        LOG_WARN("fail to push an element into server_resources_info", KR(ret), K(resource_info_result));
      }
    }
  }
  return ret;
}

int ObServerBalancer::tenant_group_balance()
{
  int ret = OB_SUCCESS;
  ObRootBalanceHelp::BalanceController balance_controller;
  ObString switch_config_str = GCONF.__balance_controller.str();
  {
    SpinWLockGuard guard(unit_mgr_->get_lock());  // lock!
    ObArray<ObZone> zones;
    if (!check_inner_stat()) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("fail to check inner stat", K(ret));
    } else if (OB_FAIL(unit_stat_mgr_.gather_stat())) {
      LOG_WARN("gather all tenant unit stat failed, refuse to do server_balance", K(ret));
      unit_stat_mgr_.reuse();
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ObRootBalanceHelp::parse_balance_info(
            switch_config_str, balance_controller))) {
      LOG_WARN("fail to parse balance switch", K(ret), "balance switch", switch_config_str);
    } else if (ObShareUtil::is_tenant_enable_rebalance(OB_SYS_TENANT_ID)
            || balance_controller.at(ObRootBalanceHelp::ENABLE_SERVER_BALANCE)) {
      if (OB_FAIL(zone_mgr_->get_zone(ObZoneStatus::ACTIVE, zones))) {
        LOG_WARN("get_zone failed", "status", ObZoneStatus::ACTIVE, K(ret));
      } else {
        LOG_INFO("start to do tenant group unit balance");
        // The server balance of all zones is completed independently
        for (int64_t i = 0; OB_SUCC(ret) && i < zones.count(); ++i) {
          bool can_execute_rebalance = false;
          if (OB_FAIL(check_can_execute_rebalance(zones.at(i), can_execute_rebalance))) {
            LOG_WARN("fail to check can execute rebalance", K(ret));
          } else if (!can_execute_rebalance) {
            // cannot do rebalance
          } else if (OB_FAIL(rebalance_servers_v2(zones.at(i)))) {
            LOG_WARN("failed to rebalance servers", "zone", zones.at(i), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::check_has_unit_in_migration(
    const common::ObIArray<ObUnitManager::ObUnitLoad> *unit_load_array,
    bool &has_unit_in_migration)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(nullptr == unit_load_array)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(unit_load_array));
  } else {
    has_unit_in_migration = false;
    for (int64_t i = 0;
         !has_unit_in_migration && OB_SUCC(ret) && i < unit_load_array->count();
         ++i) {
      const ObUnitManager::ObUnitLoad &unit_load = unit_load_array->at(i);
      if (OB_UNLIKELY(!unit_load.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid unit load", KR(ret), K(unit_load));
      } else if (unit_load.unit_->migrate_from_server_.is_valid()) {
        has_unit_in_migration = true;
      } else {
        // bypass
      }
    }
  }
  return ret;
}

int ObServerBalancer::balance_servers()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start do unit balance");
  ObRootBalanceHelp::BalanceController balance_controller;
  ObString switch_config_str = GCONF.__balance_controller.str();
  DEBUG_SYNC(START_UNIT_BALANCE);
  {
    SpinWLockGuard guard(unit_mgr_->get_lock());  // lock!
    const bool enable_sys_unit_standalone = GCONF.enable_sys_unit_standalone;
    if (!check_inner_stat()) {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("check_inner_stat failed", K(inited_), K(ret));
    } else if (OB_FAIL(ObRootBalanceHelp::parse_balance_info(switch_config_str, balance_controller))) {
      LOG_WARN("fail to parse balance switch", K(ret), "balance_swtich", switch_config_str);
    } else if (OB_FAIL(unit_stat_mgr_.gather_stat())) {
      LOG_WARN("gather all tenant unit stat failed, refuse to do balance for status change", K(ret));
      unit_stat_mgr_.reuse();
      ret = OB_SUCCESS;
    } else {
      // When the server status changes,
      // try to adjust the unit on the server without configuration item control
      if (GCONF.is_rereplication_enabled()
          || ObShareUtil::is_tenant_enable_rebalance(OB_SYS_TENANT_ID)
          || balance_controller.at(ObRootBalanceHelp::ENABLE_SERVER_BALANCE)) {
        if (OB_FAIL(distribute_for_server_status_change())) {
          LOG_WARN("fail to distribute for server status change", K(ret));
        }
      }

      if (OB_SUCC(ret) && enable_sys_unit_standalone) {
        if (OB_FAIL(distribute_for_standalone_sys_unit())) {
          LOG_WARN("fail to distribute for standalone sys unit");
        }
      }
    }
  }
  LOG_INFO("finish do unit balance", K(ret));
  return ret;
}

int ObServerBalancer::distribute_pool_for_standalone_sys_unit(
    const share::ObResourcePool &pool,
    const common::ObIArray<common::ObAddr> &sys_unit_server_array)
{
  int ret = OB_SUCCESS;
  const char *module = "UNIT_BALANCE_FOR_STANDALONE_SYS";
  share::ObUnitConfig *unit_config = nullptr;
  common::ObArray<share::ObUnit *> *pool_unit_array = nullptr;
  if (OB_UNLIKELY(OB_INVALID_ID == pool.resource_pool_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "pool_id", pool.resource_pool_id_);
  } else if (OB_UNLIKELY(nullptr == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit mgr ptr is nullptr", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_units_by_pool(pool.resource_pool_id_, pool_unit_array))) {
    LOG_WARN("fail to get units by pool", K(ret), "pool_id", pool.resource_pool_id_);
  } else if (OB_UNLIKELY(nullptr == pool_unit_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pool unit array", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_unit_config_by_id(pool.unit_config_id_, unit_config))) {
    LOG_WARN("fail to get unit config by pool name", K(pool));
  } else if (OB_UNLIKELY(nullptr == unit_config)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit config ptr is null", K(ret), K(pool));
  } else {
    ObServerInfoInTable server_info;
    ObUnitStat unit_stat;
    ObArray<ObUnitStat> in_migrate_unit_stat;
    common::ObArray<common::ObAddr> excluded_servers;
    common::ObAddr migrate_server;
    std::string resource_not_enough_reason;
    ObArray<ObServerInfoInTable> servers_info_of_zone;
    ObArray<ObServerInfoInTable> active_servers_info_of_zone;
    ObArray<obrpc::ObGetServerResourceInfoResult> active_servers_resource_info_of_zone;
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_unit_array->count(); ++i) {
      excluded_servers.reset();
      server_info.reset();
      unit_stat.reset();
      migrate_server.reset();
      servers_info_of_zone.reset();
      active_servers_resource_info_of_zone.reset();
      active_servers_info_of_zone.reset();
      share::ObUnit *unit = pool_unit_array->at(i);
      if (OB_UNLIKELY(nullptr == unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret));
      } else if (unit->migrate_from_server_.is_valid()) {
        // unit in migrate, bypass
      } else if (OB_FAIL(SVR_TRACER.get_server_info(unit->server_, server_info))) {
        LOG_WARN("fail to get server status", K(ret), "server", unit->server_);
      } else if (!server_info.is_active()) {
        // Only process servers that are active, skip non-active servers
        LOG_INFO("unit server status not active", K(ret), K(server_info), K(*unit));
      } else if (!has_exist_in_array(sys_unit_server_array, unit->server_)) {
        // bypass
      } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
              unit->unit_id_,
              unit->zone_,
              unit_stat))) {
        LOG_WARN("fail to locate unit", K(ret), "unit", *unit);
      } else if (OB_FAIL(SVR_TRACER.get_servers_info(unit->zone_, servers_info_of_zone))) {
        LOG_WARN("fail to servers_info_of_zone", KR(ret), K(unit->zone_));
      } else if (OB_FAIL(get_active_servers_info_and_resource_info_of_zone(
          unit->zone_,
          active_servers_info_of_zone,
          active_servers_resource_info_of_zone))) {
        LOG_WARN("fail to execute get_active_servers_info_and_resource_info_of_zone", KR(ret), K(unit->zone_));
      } else if (OB_FAIL(unit_mgr_->get_excluded_servers(
          *unit,
          unit_stat,
          module,
          servers_info_of_zone,
          active_servers_resource_info_of_zone,
          excluded_servers))) {
        LOG_WARN("fail to get exclude servers", K(ret), KPC(unit), K(servers_info_of_zone),
            K(active_servers_resource_info_of_zone));
      } else if (OB_FAIL(append(excluded_servers, sys_unit_server_array))) {
        LOG_WARN("fail tp append sys unit server array", K(ret));
      } else if (OB_FAIL(unit_mgr_->choose_server_for_unit(
          unit_config->unit_resource(), unit->zone_,
          excluded_servers,
          module,
          active_servers_info_of_zone,
          active_servers_resource_info_of_zone,
          migrate_server,
          resource_not_enough_reason))) {
        if (OB_ZONE_RESOURCE_NOT_ENOUGH == ret || OB_ZONE_SERVER_NOT_ENOUGH == ret) {
          LOG_WARN("has no place to migrate unit", K(module), KR(ret), "unit", *unit,
              K(excluded_servers), "resource_not_enough_reason", resource_not_enough_reason.c_str());
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to choose server for unit", KR(ret), K(unit), K(excluded_servers));
        }
      } else if (OB_FAIL(get_unit_resource_reservation(unit->unit_id_,
                                                       migrate_server,
                                                       in_migrate_unit_stat))) {
       LOG_WARN("fail to get unit resource reservation", K(ret), K(unit), K(migrate_server));
      } else if (OB_FAIL(try_migrate_unit(unit->unit_id_, pool.tenant_id_, unit_stat,
                                          in_migrate_unit_stat, migrate_server))) {
        LOG_WARN("fail to migrate unit", K(ret), K(unit), K(unit_stat), K(in_migrate_unit_stat));
      }
    }
  }
  return ret;
}

int ObServerBalancer::distribute_for_standalone_sys_unit()
{
  int ret = OB_SUCCESS;
  const bool enable_sys_unit_standalone = GCONF.enable_sys_unit_standalone;
  ObArray<ObAddr> sys_unit_server_array;
  const common::ObZone empty_zone; // means all zones
  LOG_INFO("start distribute for standalone sys unit");
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("fail to check inner stat", K(ret), K(inited_));
  } else if (!enable_sys_unit_standalone) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("sys unit standalone deployment is disabled", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_tenant_unit_servers(
          OB_SYS_TENANT_ID, empty_zone, sys_unit_server_array))) {
    LOG_WARN("fail to get tenant unit server array", K(ret));
  } else {
    ObHashMap<uint64_t, share::ObResourcePool *>::const_iterator iter = unit_mgr_->get_id_pool_map().begin();
    ObHashMap<uint64_t, share::ObResourcePool *>::const_iterator end = unit_mgr_->get_id_pool_map().end();
    for (; OB_SUCC(ret) && iter != end; ++iter) {
      if (OB_UNLIKELY(OB_INVALID_ID == iter->first)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool id unexpected", K(ret));
      } else if (OB_UNLIKELY(nullptr == iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("resource pool is null", K(ret));
      } else if (OB_SYS_TENANT_ID == iter->second->tenant_id_) {
        // no need to distribute for sys tenant
      } else if (OB_FAIL(distribute_pool_for_standalone_sys_unit(
              *iter->second, sys_unit_server_array))) {
        LOG_WARN("fail to distribute pool for standalone sys unit", K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::distribute_for_server_status_change()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start distribute for server status change");
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check_inner_stat failed", K_(inited),  K(ret));
  }

  ObHashMap<uint64_t, ObArray<share::ObResourcePool *> *>::const_iterator it =
      unit_mgr_->get_tenant_pools_map().begin();
  ObHashMap<uint64_t, ObArray<share::ObResourcePool *> *>::const_iterator it_end =
      unit_mgr_->get_tenant_pools_map().end();
  // Let the system tenant do it first
  for (; OB_SUCC(ret) && it != it_end; ++it) {
    if (OB_SYS_TENANT_ID != it->first) {
      // bypass, non sys tenant, do later
    } else if (NULL == it->second) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("it->second is null", KP(it->second), K(ret));
    } else if (OB_FAIL(distribute_by_tenant(it->first, *(it->second)))) {
      LOG_WARN("distribute by tenant failed", "tenant_id", it->first, K(ret));
    }
  }
  ObHashMap<uint64_t, ObArray<share::ObResourcePool *> *>::const_iterator it2 =
      unit_mgr_->get_tenant_pools_map().begin();
  ObHashMap<uint64_t, ObArray<share::ObResourcePool *> *>::const_iterator it2_end =
      unit_mgr_->get_tenant_pools_map().end();
  // other tenant
  for (; OB_SUCC(ret) && it2 != it2_end; ++it2) {
    if (OB_SYS_TENANT_ID == it2->first) {
      // bypass, sys tenant has been done before
    } else if (NULL == it2->second) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("it->second is null", KP(it2->second), K(ret));
    } else if (OB_FAIL(distribute_by_tenant(it2->first, *(it2->second)))) {
      LOG_WARN("distribute by tenant failed", "tenant_id", it2->first, K(ret));
    }
  }

  ObHashMap<uint64_t, share::ObResourcePool*>::const_iterator pool_it =
      unit_mgr_->get_id_pool_map().begin();
  ObHashMap<uint64_t, share::ObResourcePool*>::const_iterator pool_it_end =
      unit_mgr_->get_id_pool_map().end();
  for (; OB_SUCC(ret) && pool_it != pool_it_end; ++pool_it) {
    if (NULL == pool_it->second) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pool_it->second is null", KP(pool_it->second), K(ret));
    } else if (OB_INVALID_ID != pool_it->second->tenant_id_) {
      // bypass, already distribute in distribute by tenant
    } else if (OB_FAIL(distribute_by_pool(pool_it->second))) {
      LOG_WARN("distribute by pool failed", "pool", *(pool_it->second), K(ret));
    }
  }
  LOG_INFO("finish distribute for server status change", K(ret));
  return ret;
}

int ObServerBalancer::distribute_by_tenant(const uint64_t tenant_id,
                                        const ObArray<share::ObResourcePool *> &pools)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnitManager::ZoneUnit> zone_units;

  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check_inner_stat failed", K_(inited),  K(ret));
  } else if (OB_INVALID_ID == tenant_id || pools.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(pools), K(ret));
  } else if (OB_FAIL(unit_mgr_->get_zone_units(pools, zone_units))) {
    LOG_WARN("get_units_of_pools failed", K(pools), K(ret));
  } else {
    FOREACH_CNT_X(zone_unit, zone_units, OB_SUCCESS == ret) {
      if (OB_FAIL(distribute_zone_unit(*zone_unit))) {
        LOG_WARN("distribute zone unit failed", "zone_unit", *zone_unit, K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::distribute_by_pool(share::ObResourcePool *pool)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool *> pools;
  ObArray<ObUnitManager::ZoneUnit> zone_units;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check_inner_stat failed", K_(inited),  K(ret));
  } else if (NULL == pool) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pool is null", K(ret));
  } else if (OB_FAIL(pools.push_back(pool))) {
    LOG_WARN("push_back failed", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_zone_units(pools, zone_units))) {
    LOG_WARN("get_units_of_pools failed", K(pools), K(ret));
  } else {
    FOREACH_CNT_X(zone_unit, zone_units, OB_SUCCESS == ret) {
      if (OB_FAIL(distribute_zone_unit(*zone_unit))) {
        LOG_WARN("distribute zone unit failed", "zone_unit", *zone_unit, K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::distribute_zone_unit(const ObUnitManager::ZoneUnit &zone_unit)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check inner stat failed", K_(inited), K(ret));
  } else if (!zone_unit.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone_unit), K(ret));
  } else {
    ObServerInfoInTable server_info;
    FOREACH_CNT_X(unit_info, zone_unit.unit_infos_, OB_SUCCESS == ret) {
      server_info.reset();
      if (ObUnit::UNIT_STATUS_ACTIVE != unit_info->unit_.status_) {
        // ignore the unit that is in deleting
      } else if (OB_FAIL(SVR_TRACER.get_server_info(unit_info->unit_.server_, server_info))) {
        LOG_WARN("get_server_info failed", "server", unit_info->unit_.server_, KR(ret));
      } else if (server_info.is_active()) {
        if (OB_FAIL(distribute_for_active(server_info, *unit_info))) {
          LOG_WARN("distribute_for_active failed", K(server_info), "unit_info", *unit_info, K(ret));
        }
      } else if (server_info.is_permanent_offline() || server_info.is_deleting()) {
        if (OB_FAIL(distribute_for_permanent_offline_or_delete(server_info, *unit_info))) {
          LOG_WARN("distribute for permanent offline or delete failed",
                    K(server_info), "unit_info", *unit_info, KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::distribute_for_active(
    const ObServerInfoInTable &server_info,
    const ObUnitInfo &unit_info)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check inner stat failed", K_(inited), K(ret));
  } else if (!server_info.is_valid()
      || !server_info.is_active()
      || !unit_info.is_valid()
      || unit_info.unit_.server_ != server_info.get_server()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server_info), K(unit_info), K(ret));
  } else {
    //When the destination is blocked, cancel this migration
    //Temporary offline does not cancel the task, need to wait for permanent offline
    if ((server_info.is_migrate_in_blocked())
        && unit_info.unit_.migrate_from_server_.is_valid()) {
      LOG_INFO("find unit server active but can't migrate in, "
          "migrate_from_server is set", "unit", unit_info.unit_);
      if (OB_FAIL(distribute_for_migrate_in_blocked(unit_info))) {
        LOG_WARN("distribute for migrate in blocked failed", K(unit_info), K(ret));
      }
    }
  }
  return ret;
}

//When the migration destination is permanently offline,
//need to change to another destination
//Need to make sure that the member has been kicked out after being permanently offline
int ObServerBalancer::distribute_for_permanent_offline_or_delete(
    const share::ObServerInfoInTable &server_info,
    const ObUnitInfo &unit_info)
{
  int ret = OB_SUCCESS;
  const char *module = "UNIT_BALANCE_FOR_SERVER_PERMANENT_OFFLINE_OR_DELETE";

  LOG_INFO("find unit server permanent offline or delete, need distribute unit",
           K(module), "unit", unit_info.unit_, K(server_info));
  const bool enable_sys_unit_standalone = GCONF.enable_sys_unit_standalone;
  bool need_migrate_unit = false;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check inner stat failed", K_(inited), K(ret));
  } else if (!server_info.is_valid()
      || !unit_info.is_valid()
      || unit_info.unit_.server_ != server_info.get_server()
      || (!server_info.is_deleting() && !server_info.is_permanent_offline())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(server_info), K(unit_info), KR(ret));
  } else if (!unit_info.unit_.migrate_from_server_.is_valid()) {
    //The current unit is in a stable state, move it out
    need_migrate_unit = true;
    LOG_INFO("server is permanent offline or in deleting status, need migrate unit",
             K(unit_info), K(server_info));
  } else {
    //Currently moving in, try to cancel
    bool is_canceled = false;
    if (OB_FAIL(try_cancel_migrate_unit(unit_info.unit_, is_canceled))) {
      LOG_WARN("fail to cancel migrate unit", K(ret), K(unit_info));
    } else if (!is_canceled) {
      //If cancel fails, wait for the result of the check-in process
      //If the move-in process cannot be ended,
      //the delete server lasts for too long, and manual intervention should be required
      // ** FIXME (linqiucen): now we do not do the following commented process due to the deprecated variable with_partition
      // ** FIXME (linqiucen): in the future, we can do this process again by directly looking up the related table
      // if (!status.is_with_partition()) {
      //   //If there is no local replica, cancel this migration directly
      //   const ObUnitManager::EndMigrateOp op = ObUnitManager::ABORT;
      //   if (OB_FAIL(unit_mgr_->end_migrate_unit(unit_info.unit_.unit_id_, op))) {
      //     LOG_WARN("end_migrate_unit failed", "unit_id", unit_info.unit_.unit_id_, K(op), K(ret));
      //   } else {
      //     need_migrate_unit = true;
      //     LOG_INFO("unit has no partition, abort the migration",
      //              K(ret), K(unit_info), K(op), K(status));
      //   }
      // }
    } else {
      LOG_INFO("revert migrate unit success", K(ret), K(unit_info), K(server_info));
    }
  }
  ObUnitStat unit_stat;
  ObArray<ObAddr> excluded_servers;
  const ObZone zone = unit_info.unit_.zone_;
  ObAddr migrate_server;
  std::string resource_not_enough_reason;
  ObArray<ObServerInfoInTable> servers_info_of_zone;
  ObArray<ObServerInfoInTable> active_servers_info_of_zone;
  ObArray<obrpc::ObGetServerResourceInfoResult> active_servers_resource_info_of_zone;
  if (OB_FAIL(ret) || !need_migrate_unit) {
    //nothing todo
  } else if (OB_ISNULL(unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ is null", KR(ret), KP(unit_mgr_));
  } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
          unit_info.unit_.unit_id_,
          unit_info.unit_.zone_,
          unit_stat))) {
    LOG_WARN("fail to locate unit", K(ret), "unit", unit_info.unit_);
  } else if (OB_FAIL(SVR_TRACER.get_servers_info(unit_info.unit_.zone_, servers_info_of_zone))) {
    LOG_WARN("fail to servers_info_of_zone", KR(ret), K(unit_info.unit_.zone_));
  } else if (OB_FAIL(get_active_servers_info_and_resource_info_of_zone(
          unit_info.unit_.zone_,
          active_servers_info_of_zone,
          active_servers_resource_info_of_zone))) {
        LOG_WARN("fail to execute get_active_servers_info_and_resource_info_of_zone", KR(ret), K(unit_info.unit_.zone_));
  } else if (OB_FAIL(unit_mgr_->get_excluded_servers(
      unit_info.unit_,
      unit_stat,
      module,
      servers_info_of_zone,
      active_servers_resource_info_of_zone,
      excluded_servers))) {
    LOG_WARN("get_excluded_servers failed", "unit", unit_info.unit_, KR(ret), K(servers_info_of_zone),
        K(active_servers_resource_info_of_zone));
  } else if (OB_FAIL(unit_mgr_->choose_server_for_unit(
      unit_info.config_.unit_resource(),
      zone,
      excluded_servers,
      module,
      active_servers_info_of_zone,
      active_servers_resource_info_of_zone,
      migrate_server,
      resource_not_enough_reason))) {
    if (OB_ZONE_RESOURCE_NOT_ENOUGH == ret || OB_ZONE_SERVER_NOT_ENOUGH == ret) {
      LOG_WARN("has no place to migrate unit", K(module), KR(ret), K(zone), K(excluded_servers),
          K(unit_info), "resource_not_enough_reason", resource_not_enough_reason.c_str());
      // return success when resource not enough to go on next balance task
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("choose_unit_server failed", "config", unit_info.config_,
          K(zone), K(excluded_servers), K(ret));
    }
  } else {
    ObArray<ObUnitStat> in_migrate_unit_stat;
    if (OB_FAIL(get_unit_resource_reservation(unit_info.unit_.unit_id_,
        migrate_server,
        in_migrate_unit_stat))) {
      LOG_WARN("get_unit_resource_reservation failed", K(unit_info), K(ret));
    } else if (OB_FAIL(try_migrate_unit(unit_info.unit_.unit_id_,
        unit_info.pool_.tenant_id_,
        unit_stat,
        in_migrate_unit_stat,
        migrate_server))) {
      LOG_WARN("fail to try migrate unit", "unit", unit_info.unit_, K(migrate_server), K(ret));
    } else {
      LOG_INFO("migrate unit success", K(module), K(unit_info), K(server_info), "dest_server", migrate_server);
    }
  }
  return ret;
}

// NOTE:
//  When the unit migrates from A to B, if B is down or the disk is full,
//  two strategies will be tried in turn
//  1. A can move in, cancel this migration, and re-migrate the data back to A
//  2. A cannot move in, then choose a new machine C, and move unit to C
int ObServerBalancer::distribute_for_migrate_in_blocked(const ObUnitInfo &unit_info)
{
  int ret = OB_SUCCESS;
  ObServerInfoInTable server_info;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check inner stat failed", K_(inited), K(ret));
  } else if (!unit_info.is_valid() || !unit_info.unit_.migrate_from_server_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(unit_info), K(ret));
  } else if (OB_FAIL(SVR_TRACER.get_server_info(
              unit_info.unit_.migrate_from_server_, server_info))) {
    LOG_WARN("get_server_status failed",
             "server", unit_info.unit_.migrate_from_server_, K(ret));
  } else if (ObUnit::UNIT_STATUS_ACTIVE != unit_info.unit_.status_) {
    // ignore the unit which is in deleting
  } else {
    if (server_info.can_migrate_in()) {
      LOG_INFO("unit migrate_from_server can migrate in, "
          "migrate unit back to migrate_from_server", "unit", unit_info.unit_);
      const ObUnitManager::EndMigrateOp op = ObUnitManager::REVERSE;
      if (OB_FAIL(unit_mgr_->end_migrate_unit(unit_info.unit_.unit_id_, op))) {
        LOG_WARN("end_migrate_unit failed", "unit_id", unit_info.unit_.unit_id_, K(op), K(ret));
      }
    } else {
      //TODO: @wanhong.wwh need support when dest server is blocked and src server can not migrate in
      //nothing todo
      LOG_WARN("NOTICE: unit migration is hung. dest server is blocked "
          "and source server can not migrate in. NEED to be involved manually.",
               "unit", unit_info.unit_, "migrate_from_server", server_info);
    }

    /*
    {
      LOG_INFO("unit migrate_from_server can not migrate in, and unit_server is empty now"
          "migrate unit to new server", "unit", unit_info.unit_);
      ObAddr new_server;
      ObArray<ObAddr> excluded_servers;
      ObUnitStat unit_stat;
      ObArray<ObUnitStat> in_migrate_unit_stat;
      ObUnitManager::EndMigrateOp op = ObUnitManager::ABORT;
      if (OB_FAIL(unit_mgr_->end_migrate_unit(unit_info.unit_.unit_id_, op))) {
        LOG_WARN("fail to end migrate unit", K(ret), "unit", unit_info.unit_);
      } else if (OB_FAIL(unit_stat_mgr_->get_unit_stat(
              unit_info.unit_.unit_id_, unit_info.unit_.zone_, unit_stat))) {
        LOG_WARN("fail to locate unit", K(ret), "unit", unit_info.unit_);
      } else if (OB_FAIL(unit_mgr_->get_excluded_servers(unit_info.unit_, unit_stat, excluded_servers))) {
        LOG_WARN("get_excluded_servers failed", "unit", unit_info.unit_, K(ret));
      } else if (OB_FAIL(unit_mgr_->choose_server_for_unit(unit_info.config_, unit_info.unit_.zone_,
          excluded_servers, new_server))) {
        LOG_WARN("choose_unit_server failed", "config", unit_info.config_,
            "zone", unit_info.unit_.zone_, K(excluded_servers), K(ret));
      } else if (OB_FAIL(get_unit_resource_reservation(unit_info.unit_.unit_id_,
                                                       new_server,
                                                       in_migrate_unit_stat))) {
        LOG_WARN("get_unit_resource_reservation failed", K(unit_info), K(ret));
      } else if (OB_FAIL(try_migrate_unit(unit_info.unit_.unit_id_,
                                          unit_info.pool_.tenant_id_,
                                          unit_stat,
                                          in_migrate_unit_stat,
                                          new_server))) {
        LOG_WARN("fail to try migrate unit ", "unit", unit_info.unit_, K(new_server), K(ret));
      }
    }
    */
  }
  return ret;
}

int ObServerBalancer::get_unit_resource_reservation(uint64_t unit_id,
                                                    const ObAddr &new_server,
                                                    ObIArray<ObUnitStat> &in_migrate_unit_stat)
{
  int ret = OB_SUCCESS;
  UNUSED(unit_id);
  ObArray<ObUnitManager::ObUnitLoad> *unit_loads = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server balancer not init", K_(inited), K(ret));
  } else {
    if (OB_FAIL(unit_mgr_->get_loads_by_server(new_server, unit_loads))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get loads by server", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (NULL == unit_loads) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit loads ptr is null", K(ret));
    } else {
      // Statistics of the units that are being migrated to the new server
      for (int64_t i = 0; OB_SUCC(ret) && i < unit_loads->count(); ++i) {
        ObUnitStat in_mig_stat;
        const ObUnitManager::ObUnitLoad &load = unit_loads->at(i);
        if (!load.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit load is invalid", K(ret), K(load));
        } else if (new_server == load.unit_->migrate_from_server_) {
          // by pass
        } else if (!load.unit_->migrate_from_server_.is_valid()) {
          // by pass
        } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
                load.unit_->unit_id_, load.unit_->zone_, in_mig_stat))) {
          LOG_WARN("get unit stat fail", "unit_id", load.unit_->unit_id_, K(ret));
        } else if (OB_FAIL(in_migrate_unit_stat.push_back(in_mig_stat))) {
          LOG_WARN("push back fail", K(in_mig_stat), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::try_migrate_unit(const uint64_t unit_id,
                                       const uint64_t tenant_id,
                                       const ObUnitStat &unit_stat,
                                       const ObIArray<ObUnitStat> &migrating_unit_stat,
                                       const ObAddr &dst)
{
  int ret = OB_SUCCESS;
  ObServerResourceInfo dst_resource_info;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server balancer not init", K_(inited), K(ret));
  } else if (OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_mgr_ is null", KR(ret), KP(server_mgr_));
  } else if (OB_FAIL(server_mgr_->get_server_resource_info(dst, dst_resource_info))) {
    LOG_WARN("fail to get dst_resource_info", KR(ret), K(dst));
  } else {
    ret = unit_mgr_->try_migrate_unit(
        unit_id,
        tenant_id,
        unit_stat,
        migrating_unit_stat,
        dst,
        dst_resource_info);
  }
  return ret;
}

int ObServerBalancer::try_cancel_migrate_unit(const share::ObUnit &unit, bool &is_canceled)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("server balancer not init", K_(inited), K(ret));
  } else {
    ret = unit_mgr_->try_cancel_migrate_unit(unit, is_canceled);
  }
  return ret;
}

const double ObServerBalancer::CPU_EPSILON = 0.0001;
const double ObServerBalancer::EPSILON = 0.0000000001;

// the new version server balance
int ObServerBalancer::UnitMigrateStatCmp::get_ret() const
{
  return ret_;
}

bool ObServerBalancer::UnitMigrateStatCmp::operator()(
     const UnitMigrateStat &left,
     const UnitMigrateStat &right)
{
  bool bool_ret = false;
  if (OB_SUCCESS != ret_) {
    // do nothing
  } else if (left.arranged_pos_ < right.arranged_pos_) {
    bool_ret = true;
  } else if (right.arranged_pos_ < left.arranged_pos_) {
    bool_ret = false;
  } else {
    ret_ = OB_ERR_UNEXPECTED;
  }
  return bool_ret;
}

int ObServerBalancer::check_can_execute_rebalance(
    const common::ObZone &zone,
    bool &can_execute_rebalance)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(share::ObZoneInfo, zone_info) {
    common::ObArray<common::ObAddr> server_list;
    common::ObArray<uint64_t> unit_ids;
    if (OB_UNLIKELY(!inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_UNLIKELY(zone.is_empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(zone));
    } else if (OB_ISNULL(unit_mgr_) || OB_ISNULL(zone_mgr_) || OB_ISNULL(server_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit_mgr_, zone_mgr_ or server_mgr_ is null", KR(ret), KP(unit_mgr_), KP(zone_mgr_), KP(server_mgr_));
    } else if (OB_FAIL(zone_mgr_->get_zone(zone, zone_info))) {
      LOG_WARN("fail to get zone info", K(ret), K(zone));
    } else if (ObZoneStatus::ACTIVE != zone_info.status_) {
      can_execute_rebalance = false;
      LOG_INFO("cannot execute server rebalance: zone inactive", K(zone));
    } else if (OB_FAIL(SVR_TRACER.get_servers_of_zone(zone, server_list))) {
      LOG_WARN("fail to get servers of zone", K(ret), K(zone));
    } else if (OB_FAIL(unit_mgr_->inner_get_unit_ids(unit_ids))) {
      LOG_WARN("fail to get unit ids", K(ret));
    } else {
      can_execute_rebalance = true;
      share::ObUnitConfig sum_load;
      for (int64_t i = 0; can_execute_rebalance && OB_SUCC(ret) && i < server_list.count(); ++i) {
        const common::ObAddr &server = server_list.at(i);
        ObServerInfoInTable server_info;
        ObServerResourceInfo resource_info;
        ObArray<ObUnitManager::ObUnitLoad> *unit_loads = nullptr;
        sum_load.reset();
        if (OB_FAIL(unit_mgr_->get_loads_by_server(server, unit_loads))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("fail to get loads by server", K(ret));
          } else {
            ret = OB_SUCCESS; // unit_loads not exist, no load no this server
          }
        } else if (OB_UNLIKELY(nullptr == unit_loads)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit loads ptr is null", K(ret));
        } else if (OB_FAIL(unit_mgr_->calc_sum_load(unit_loads, sum_load))) {
          LOG_WARN("fail to calc sum load", K(ret));
        }
        if (OB_FAIL(ret)) {
          // failed
        } else if (OB_FAIL(SVR_TRACER.get_server_info(server, server_info))) {
          LOG_WARN("fail to get server status", K(ret));
        } else if (server_info.is_temporary_offline() || server_info.is_stopped()) {
          can_execute_rebalance = false;
          LOG_INFO("cannot execute server rebalance: Server status is not normal", K(zone), K(server_info));
        } else if (OB_FAIL(server_mgr_->get_server_resource_info(server_info.get_server(), resource_info))) {
          LOG_WARN("fail to execute get_server_resource_info", KR(ret), K(server_info.get_server()));
        } else if (fabs(resource_info.report_cpu_assigned_ - sum_load.min_cpu()) > CPU_EPSILON
            || fabs(resource_info.report_cpu_max_assigned_ - sum_load.max_cpu()) > CPU_EPSILON
            || resource_info.report_mem_assigned_ != sum_load.memory_size()) {
          can_execute_rebalance = false;
          LOG_INFO("cannot execute server rebalance: "
                   "Server resource_info and unit_manager sum_load not equal", K(zone), K(resource_info), K(sum_load));
        } else {} // no more to do
      }
      for (int64_t j = 0; can_execute_rebalance && OB_SUCC(ret) && j < unit_ids.count(); ++j) {
        ObUnit *unit = NULL;
        if (OB_FAIL(unit_mgr_->get_unit_by_id(unit_ids.at(j), unit))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unit_load is invalid", K(ret));
        } else if (NULL == unit) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit ptr is null", K(ret), KP(unit));
        } else if (ObUnit::UNIT_STATUS_DELETING == unit->status_) {
          can_execute_rebalance = false;
          LOG_INFO("cannot execute server rebalance: unit deleting", K(zone), "unit", *unit);
        } else if (unit->migrate_from_server_.is_valid()) {
          can_execute_rebalance = false;
          LOG_INFO("cannot execute server rebalance: unit migrating", K(zone), "unit", *unit);
        } else {} // unit in stable status
      }
    }
  }
  return ret;
}

int ObServerBalancer::rebalance_servers_v2(
    const common::ObZone &zone)
{
  int ret = OB_SUCCESS;
  common::ObArray<Matrix<uint64_t> > group_tenant_array;
  common::ObArray<uint64_t> standalone_tenant_array;
  common::ObArray<ObUnitManager::ObUnitLoad> not_grant_units;
  common::ObArray<ObUnitManager::ObUnitLoad> standalone_units;
  LOG_INFO("start to do tenant group unit balance", K(zone));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(generate_zone_server_disk_statistic(zone))) {
    LOG_WARN("fail to generate zone server disk statistic", K(ret), K(zone));
  } else if (OB_FAIL(generate_tenant_array(zone, group_tenant_array, standalone_tenant_array))) {
    LOG_WARN("fail to generate tenant array", K(ret));
  } else if (OB_FAIL(generate_standalone_units(zone, standalone_tenant_array, standalone_units))) {
    LOG_WARN("fail to generate standalone units", K(ret), K(zone));
  } else if (OB_FAIL(generate_not_grant_pool_units(zone, not_grant_units))) {
    LOG_WARN("fail to generate not grant pool units", K(ret), K(zone));
  } else if (OB_FAIL(do_rebalance_servers_v2(
          zone, group_tenant_array, standalone_units, not_grant_units))) {
    LOG_WARN("fail to rebalance servers", K(ret), K(zone));
  } else {} // no more to do
  return ret;
}

int ObServerBalancer::generate_single_tenant_group(
    Matrix<uint64_t> &tenant_id_matrix,
    const ObTenantGroupParser::TenantNameGroup &tenant_name_group)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(tenant_name_group.row_ <= 0 || tenant_name_group.column_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_name_group));
  } else if (OB_UNLIKELY(NULL == schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr is null", K(ret), KP(schema_service_));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret), KP(unit_mgr_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(tenant_id_matrix.init(tenant_name_group.row_, tenant_name_group.column_))) {
    LOG_WARN("fail to init tenant id matrix", K(ret));
  } else {
    for (int64_t row = 0; OB_SUCC(ret) && row < tenant_name_group.row_; ++row) {
      for (int64_t column = 0; OB_SUCC(ret) && column < tenant_name_group.column_; ++column) {
        const int64_t idx = row * tenant_name_group.column_ + column;
        const share::schema::ObTenantSchema *tenant_schema = NULL;
        if (idx >= tenant_name_group.tenants_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("idx value unexpected", K(ret), K(idx),
                   "tenants_count", tenant_name_group.tenants_.count());
        } else if (OB_FAIL(schema_guard.get_tenant_info(
                tenant_name_group.tenants_.at(idx), tenant_schema))) {
          LOG_WARN("fail to get tenant info", K(ret),
                   "tenant_name", tenant_name_group.tenants_.at(idx));
        } else if (OB_UNLIKELY(NULL == tenant_schema)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant schema ptr is null", K(ret), KP(tenant_schema));
        } else if (OB_UNLIKELY(OB_SYS_TENANT_ID == tenant_schema->get_tenant_id())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("sys tenant cannot be configured in tenant groups", K(ret));
        } else if (OB_FAIL(tenant_id_matrix.set(row, column, tenant_schema->get_tenant_id()))) {
          LOG_WARN("fail to set tenant id matrix", K(ret), K(row), K(column),
                   "tenant_id", tenant_schema->get_tenant_id());
        } else {} // next matrix cell
      }
    }
  }
  return ret;
}

int ObServerBalancer::generate_group_tenant_array(
    const common::ObIArray<ObTenantGroupParser::TenantNameGroup> &tenant_groups,
    common::ObIArray<Matrix<uint64_t> > &group_tenant_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    group_tenant_array.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_groups.count(); ++i) {
      Matrix<uint64_t> tenant_id_matrix;
      const ObTenantGroupParser::TenantNameGroup &tenant_name_group = tenant_groups.at(i);
      if (OB_FAIL(generate_single_tenant_group(tenant_id_matrix, tenant_name_group))) {
        LOG_WARN("fail to generate single tenant group", K(ret));
      } else if (OB_FAIL(group_tenant_array.push_back(tenant_id_matrix))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // go on next
    }
  }
  return ret;
}

bool ObServerBalancer::has_exist_in_group_tenant_array(
     const uint64_t tenant_id,
     const common::ObIArray<Matrix<uint64_t> > &group_tenant_array)
{
  bool exist = false;
  for (int64_t i = 0; !exist && i < group_tenant_array.count(); ++i) {
    const Matrix<uint64_t> &tenant_group = group_tenant_array.at(i);
    if (tenant_group.is_contain(tenant_id)) {
      exist = true;
    } else {} // not contain, go on next
  }
  return exist;
}

int ObServerBalancer::generate_standalone_tenant_array(
    const common::ObIArray<Matrix<uint64_t> > &group_tenant_array,
    common::ObIArray<uint64_t> &standalone_tenant_array)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  common::ObArray<uint64_t> tenant_ids;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service_ ptr is null", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", K(ret));
  } else {
    standalone_tenant_array.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      if (common::OB_SYS_TENANT_ID == tenant_ids.at(i)) {
        // System tenant unit scheduling special processing,
        // does not belong to any tenant group, nor is it standalone
      } else if (has_exist_in_group_tenant_array(tenant_ids.at(i), group_tenant_array)) {
        // Included in a tenant group, not standalone
      } else if (OB_FAIL(standalone_tenant_array.push_back(tenant_ids.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // next tenant id
    }
  }
  return ret;
}

int ObServerBalancer::generate_tenant_array(
    const common::ObZone &zone,
    common::ObIArray<Matrix<uint64_t> > &group_tenant_array,
    common::ObIArray<uint64_t> &standalone_tenant_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(generate_standalone_tenant_array(
          group_tenant_array, standalone_tenant_array))) {
    LOG_WARN("fail to generate standalone tenant array", K(ret));
  } else {} // no more to do
  LOG_INFO("generate tenant array finish", K(ret), K(zone),
           K(standalone_tenant_array), K(group_tenant_array));
  return ret;
}

int ObServerBalancer::fill_pool_units_in_zone(
    const common::ObZone &zone,
    share::ObResourcePool &pool,
    common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  } else {
    ObArray<ObUnit *> *units = NULL;
    ObUnitConfig *unit_config = NULL;
    if (!has_exist_in_array(pool.zone_list_, zone)) {
      // do not exist in pool zone list
    } else if (OB_FAIL(unit_mgr_->get_units_by_pool(pool.resource_pool_id_, units))) {
      LOG_WARN("fail to get units by pool", K(ret));
    } else if (OB_UNLIKELY(NULL == units)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("units ptr is null", K(ret), KP(units));
    } else if (OB_FAIL(unit_mgr_->get_unit_config_by_id(pool.unit_config_id_, unit_config))) {
      LOG_WARN("fail to get unit config by id", K(ret), "config_id", pool.unit_config_id_);
    } else if (OB_UNLIKELY(NULL == unit_config)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit_config ptr is null", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < units->count(); ++j) {
        ObUnitManager::ObUnitLoad unit_load;
        unit_load.unit_ = units->at(j);
        unit_load.unit_config_ = unit_config;
        unit_load.pool_ = &pool;
        if (NULL == units->at(j)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit ptr is null", K(ret));
        } else if (zone != units->at(j)->zone_) {
          // do not belong to this zone
        } else if (OB_FAIL(unit_loads.push_back(unit_load))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
    }
  }
  return ret;
}

int ObServerBalancer::generate_not_grant_pool_units(
    const common::ObZone &zone,
    common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  } else {
    not_grant_units.reset();
    ObHashMap<uint64_t, share::ObResourcePool *>::const_iterator iter;
    for (iter = unit_mgr_->id_pool_map_.begin();
         OB_SUCC(ret) && iter != unit_mgr_->id_pool_map_.end();
         ++iter) {
      share::ObResourcePool *pool = iter->second;
      if (NULL == pool) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pool is null", KP(pool), K(ret));
      } else if (OB_INVALID_ID != pool->tenant_id_) {
        // already grant to some tenant
      } else if (OB_FAIL(fill_pool_units_in_zone(zone, *pool, not_grant_units))) {
        LOG_WARN("fail to fill pool units in zone", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObServerBalancer::generate_standalone_units(
    const common::ObZone &zone,
    const common::ObIArray<uint64_t> &standalone_tenant_array,
    common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units)
{
   int ret = OB_SUCCESS;
   if (OB_UNLIKELY(!inited_)) {
     ret = OB_NOT_INIT;
     LOG_WARN("not init", K(ret));
   } else if (OB_UNLIKELY(zone.is_empty())) {
     ret = OB_INVALID_ARGUMENT;
     LOG_WARN("invalid argument", K(ret), K(zone));
   } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("unit_mgr_ ptr is null", K(ret));
   } else {
     standalone_units.reset();
     for (int64_t i = 0; OB_SUCC(ret) && i < standalone_tenant_array.count(); ++i) {
       common::ObArray<share::ObResourcePool *> *pools = NULL;
       const uint64_t tenant_id = standalone_tenant_array.at(i);
       if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("tenant id unexpected", K(ret), K(tenant_id));
       } else if (is_meta_tenant(tenant_id)) {
         // meta tenant has no unit, do not handle
       } else if (OB_FAIL(unit_mgr_->get_pools_by_tenant(tenant_id, pools))) {
         LOG_WARN("fail to get pools by tenant", K(ret), K(tenant_id));
       } else if (OB_UNLIKELY(NULL == pools)) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("pools ptr is null", K(ret));
       } else {
         for (int64_t j = 0; OB_SUCC(ret) && j < pools->count(); ++j) {
           share::ObResourcePool *pool = pools->at(j);
           if (NULL == pool) {
             ret = OB_ERR_UNEXPECTED;
             LOG_WARN("pool is null", K(ret), KP(pool));
           } else if (OB_FAIL(fill_pool_units_in_zone(zone, *pool, standalone_units))) {
             LOG_WARN("fail to fill pool units in zone", K(ret));
           } else {} // no more to do
         }
       }
     }
   }
   return ret;
}

int ObServerBalancer::generate_available_servers(
    const common::ObZone &zone,
    const bool excluded_sys_unit_server,
    common::ObIArray<common::ObAddr> &available_servers)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(share::ObZoneInfo, zone_info) {
    common::ObArray<common::ObAddr> server_list;
    ObArray<ObAddr> sys_unit_server_array;
    if (OB_UNLIKELY(!inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_UNLIKELY(zone.is_empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(zone));
    } else if (OB_ISNULL(zone_mgr_) || OB_ISNULL(unit_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone_mgr_ or unit_mgr_ is null", K(ret), KP(unit_mgr_), KP(zone_mgr_));
    } else if (OB_FAIL(zone_mgr_->get_zone(zone, zone_info))) {
      LOG_WARN("fail to get zone info", K(ret), K(zone));
    } else if (ObZoneStatus::ACTIVE != zone_info.status_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("zone is not in active", K(ret), K(zone_info));
    } else if (OB_FAIL(SVR_TRACER.get_servers_of_zone(zone, server_list))) {
      LOG_WARN("fail to get servers of zone", K(ret), K(zone));
    } else if (OB_FAIL(unit_mgr_->get_tenant_unit_servers(
            OB_SYS_TENANT_ID, zone, sys_unit_server_array))) {
      LOG_WARN("fail to get tenant unit server array", K(ret));
    } else {
      available_servers.reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
        share::ObServerInfoInTable server_info;
        if (OB_FAIL(SVR_TRACER.get_server_info(server_list.at(i), server_info))) {
          LOG_WARN("fail to get server status", K(ret));
        } else if (server_info.is_temporary_offline() || server_info.is_stopped()) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("server in zone is not stable, stop balance servers", K(ret), K(server_info),
                   "is_temporary_offline", server_info.is_temporary_offline(),
                   "is_stopped", server_info.is_stopped());
        } else if (excluded_sys_unit_server
                   && has_exist_in_array(sys_unit_server_array, server_list.at(i))) {
          // bypass
        } else if (server_info.is_active()) {
          if (OB_FAIL(available_servers.push_back(server_list.at(i)))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {}
        // Permanently offline and deleting servers are not available servers
      }
    }
    LOG_INFO("generate available servers finish", K(ret), K(zone), K(available_servers));
  }
  return ret;
}

int ObServerBalancer::check_need_balance_sys_tenant_units(
    const common::ObIArray<ObUnitManager::ObUnitLoad> &sys_tenant_units,
    const common::ObIArray<common::ObAddr> &available_servers,
    bool &need_balance)
{
  int ret = OB_SUCCESS;
  need_balance = false;
  for (int64_t i = 0; OB_SUCC(ret) && !need_balance && i < sys_tenant_units.count(); ++i) {
    const ObUnitManager::ObUnitLoad &unit_load = sys_tenant_units.at(i);
    if (!unit_load.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit load invalid", K(ret), K(unit_load));
    } else if (!has_exist_in_array(available_servers, unit_load.unit_->server_)) {
      need_balance = true;
      LOG_INFO("need to execute sys tenant units balance", "unit", *unit_load.unit_);
    } else {} // go on to check next
  }
  return ret;
}

int ObServerBalancer::pick_server_load(
    const common::ObAddr &server,
    ObIArray<ServerTotalLoad *> &server_load_ptrs_sorted,
    ServerTotalLoad *&target_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    bool find = false;
    for (int64_t i = 0; !find && i < server_load_ptrs_sorted.count() && OB_SUCC(ret); ++i) {
      ServerTotalLoad *ptr = server_load_ptrs_sorted.at(i);
      if (NULL == ptr) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ptr is null", K(ret));
      } else if (server != ptr->server_) {
        // go on to check next
      } else {
        target_ptr = ptr;
        find = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("server load not found", K(ret), K(server));
    }
  }
  return ret;
}

int ObServerBalancer::try_balance_single_unit_by_disk(
    const ObUnitManager::ObUnitLoad &unit_load,
    common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted,
    common::ObIArray<UnitMigrateStat> &task_array,
    const common::ObIArray<common::ObAddr> &excluded_servers,
    common::ObIArray<PoolOccupation> &pool_occupation,
    bool &do_balance)
{
  int ret = OB_SUCCESS;
  ObUnitStat unit_stat;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!unit_load.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit load", K(ret), K(unit_load));
  } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
          unit_load.unit_->unit_id_,
          unit_load.unit_->zone_,
          unit_stat))) {
    LOG_WARN("fail to get unit stat", K(ret), "unit", *unit_load.unit_);
  } else {
    do_balance = false;
    for (int64_t i = 0;
         !do_balance
          && OB_SUCC(ret)
          && i < zone_disk_statistic_.server_disk_statistic_array_.count();
         ++i) {
      ServerTotalLoad *server_load = NULL;
      ServerDiskStatistic &server_disk_statistic
          = zone_disk_statistic_.server_disk_statistic_array_.at(i);
      if (server_disk_statistic.wild_server_) {
        // Can't move unit to wild server
      } else if (unit_load.unit_->server_ == server_disk_statistic.server_) {
        // Can't move to oneself
      } else if (has_exist_in_array(excluded_servers, server_disk_statistic.server_)) {
        // In the excluded server, it cannot be used as the target machine
      } else if (OB_FAIL(pick_server_load(
              server_disk_statistic.server_, server_load_ptrs_sorted, server_load))) {
        LOG_WARN("fail to pick server load", K(ret));
      } else if (NULL == server_load) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server load ptr is null", K(ret), KP(server_load));
      } else {
        LoadSum this_load;
        bool enough = true;
        const bool mind_disk_waterlevel = false;
        UnitMigrateStat unit_migrate_stat;
        unit_migrate_stat.original_pos_ = unit_load.unit_->server_;
        unit_migrate_stat.arranged_pos_ = server_load->server_;
        unit_migrate_stat.unit_load_ = unit_load;
        if (OB_FAIL(this_load.append_load(unit_load))) {
          LOG_WARN("fail to append load", K(ret));
        } else if (OB_FAIL(check_single_server_resource_enough(
                this_load, unit_stat, *server_load, enough, mind_disk_waterlevel))) {
          LOG_WARN("fail to check single server resource enough", K(ret));
        } else if (!enough) {
          // no enough
        } else if (OB_FAIL(do_update_dst_server_load(
                unit_load, *server_load, unit_stat, pool_occupation))) {
          LOG_WARN("fail to append migrate unit task", K(ret));
        } else if (OB_FAIL(task_array.push_back(unit_migrate_stat))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          ServerTotalLoadCmp cmp;
          std::sort(server_load_ptrs_sorted.begin(), server_load_ptrs_sorted.end(), cmp);
          if (OB_FAIL(cmp.get_ret())) {
            LOG_WARN("fail to sort server loads", K(ret));
          }
          do_balance = true;
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::do_update_src_server_load(
    const ObUnitManager::ObUnitLoad &unit_load,
    ServerTotalLoad &this_server_load,
    const share::ObUnitStat &unit_stat)
{
  int ret = OB_SUCCESS;
  share::ObUnit *unit = NULL;
  share::ObUnitStat my_unit_stat = unit_stat;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!unit_load.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit load", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_unit_by_id(unit_stat.get_unit_id(), unit))) {
    LOG_WARN("fail to get unit by id", K(ret), "unit_id", unit_stat.get_unit_id());
  } else if (OB_UNLIKELY(NULL == unit)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit ptr is null", K(ret));
  } else if (common::REPLICA_TYPE_LOGONLY == unit->replica_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Replica type LOGONLY is unexpected", KR(ret), KP(unit));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(zone_disk_statistic_.reduce_server_disk_use(
          this_server_load.server_, my_unit_stat.get_required_size()))) {
    LOG_WARN("fail to reduce server disk use", K(ret));
  } else if (OB_FAIL(this_server_load.load_sum_.remove_load(unit_load))) {
    LOG_WARN("fail to remove load", K(ret));
  } else if (OB_FAIL(this_server_load.update_load_value())) {
    LOG_WARN("fail to update load value", K(ret));
  } else {} // no more to do
  return ret;
}

int ObServerBalancer::do_update_dst_server_load(
    const ObUnitManager::ObUnitLoad &unit_load,
    ServerTotalLoad &this_server_load,
    const share::ObUnitStat &unit_stat,
    common::ObIArray<PoolOccupation> &pool_occupation)
{
  int ret = OB_SUCCESS;
  share::ObUnit *unit = NULL;
  share::ObUnitStat my_unit_stat = unit_stat;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!unit_load.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit load", K(ret), K(unit_load));
  } else if (OB_FAIL(unit_mgr_->get_unit_by_id(unit_stat.get_unit_id(), unit))) {
    LOG_WARN("fail to get unit by id", K(ret), "unit_id", unit_stat.get_unit_id());
  } else if (OB_UNLIKELY(NULL == unit)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit ptr is null", K(ret));
  } else if (common::REPLICA_TYPE_LOGONLY == unit->replica_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Replica type LOGONLY is unexpected", KR(ret), KP(unit));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(pool_occupation.push_back(
          PoolOccupation(unit_load.pool_->tenant_id_,
                         unit_load.pool_->resource_pool_id_,
                         this_server_load.server_)))) {
    LOG_WARN("fail to push back", K(ret));
  } else if (OB_FAIL(zone_disk_statistic_.raise_server_disk_use(
          this_server_load.server_, my_unit_stat.get_required_size()))) {
    LOG_WARN("fail to raise server disk use", K(ret));
  } else if (OB_FAIL(this_server_load.load_sum_.append_load(unit_load))) {
    LOG_WARN("fail to append load", K(ret));
  } else if (OB_FAIL(this_server_load.update_load_value())) {
    LOG_WARN("fail to update load value", K(ret));
  } else {} // no more to do
  return ret;
}

int ObServerBalancer::try_balance_single_unit_by_cm(
    const ObUnitManager::ObUnitLoad &unit_load,
    common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted,
    common::ObIArray<UnitMigrateStat> &task_array,
    const common::ObIArray<common::ObAddr> &excluded_servers,
    common::ObIArray<PoolOccupation> &pool_occupation,
    bool &do_balance)
{
  int ret = OB_SUCCESS;
  ObUnitStat unit_stat;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!unit_load.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit_load));
  } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
          unit_load.unit_->unit_id_,
          unit_load.unit_->zone_,
          unit_stat))) {
    LOG_WARN("fail to get unit stat", K(ret), "unit", *unit_load.unit_);
  } else {
    do_balance = false;
    for (int64_t j = server_load_ptrs_sorted.count() - 1;
         !do_balance && OB_SUCC(ret) && j >= 0;
         --j) {
      ServerTotalLoad *server_load = server_load_ptrs_sorted.at(j);
      if (OB_UNLIKELY(NULL == server_load)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server load ptr is null", K(ret), KP(server_load));
      } else {
        LoadSum this_load;
        bool enough = true;
        UnitMigrateStat unit_migrate_stat;
        unit_migrate_stat.original_pos_ = unit_load.unit_->server_;
        unit_migrate_stat.arranged_pos_ = server_load->server_;
        unit_migrate_stat.unit_load_ = unit_load;
        if (server_load->wild_server_) {
          // Can't move unit to wild server
        } else if (server_load->server_ == unit_load.unit_->server_) {
          // Don't move to oneself
        } else if (has_exist_in_array(excluded_servers, server_load->server_)) {
          // it cannot be used as the target machine in the excluded server
        } else if (OB_FAIL(this_load.append_load(unit_load))) {
          LOG_WARN("fail to append load", K(ret));
        } else if (OB_FAIL(check_single_server_resource_enough(
                this_load, unit_stat, *server_load, enough))) {
          LOG_WARN("fail to check server resource", K(ret));
        } else if (!enough) {
          // The resources of this machine are not enough to support the move of this unit
        } else if (OB_FAIL(do_update_dst_server_load(
                unit_load, *server_load, unit_stat, pool_occupation))) {
          LOG_WARN("fail to append migrate unit task", K(ret));
        } else if (OB_FAIL(task_array.push_back(unit_migrate_stat))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          ServerTotalLoadCmp cmp;
          std::sort(server_load_ptrs_sorted.begin(), server_load_ptrs_sorted.end(), cmp);
          if (OB_FAIL(cmp.get_ret())) {
            LOG_WARN("fail to sort server loads", K(ret));
          }
          do_balance = true;
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::do_balance_sys_tenant_single_unit(
    const ObUnitManager::ObUnitLoad &unit_load,
    common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted,
    common::ObIArray<UnitMigrateStat> &task_array,
    common::ObIArray<PoolOccupation> &pool_occupation)
{
  int ret = OB_SUCCESS;
  bool do_balance = false;
  ObArray<common::ObAddr> excluded_servers;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!unit_load.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit_load));
  } else if (OB_FAIL(get_pool_occupation_excluded_dst_servers(
          unit_load, pool_occupation, excluded_servers))) {
    LOG_WARN("fail to get excluded servers", K(ret));
  } else if (OB_FAIL(try_balance_single_unit_by_cm(
          unit_load, server_load_ptrs_sorted, task_array,
          excluded_servers, pool_occupation, do_balance))) {
    LOG_WARN("fail to try balance sys single unit by cpu and memory", K(ret));
  } else if (do_balance) {
    // do execute balance according to cpu and memory, no more to do
  } else if (OB_FAIL(try_balance_single_unit_by_disk(
          unit_load, server_load_ptrs_sorted, task_array,
          excluded_servers, pool_occupation, do_balance))) {
    LOG_WARN("fail to do balance sys single unit by disk", K(ret));
  } else if (do_balance) {
    // good,
  } else {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_WARN("no available server to hold more unit", K(ret),
             "unit", *unit_load.unit_,
             "unit_config", *unit_load.unit_config_,
             "pool", *unit_load.pool_);
  }
  return ret;
}

int ObServerBalancer::do_balance_sys_tenant_units(
    const common::ObIArray<ObUnitManager::ObUnitLoad> &sys_tenant_units,
    const common::ObIArray<common::ObAddr> &available_servers,
    common::ObArray<ServerTotalLoad> &server_loads)
{
  int ret = OB_SUCCESS;
  ObArray<PoolOccupation> pool_occupation;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(generate_pool_occupation_array(sys_tenant_units, pool_occupation))) {
    LOG_WARN("fail to generate pool occupation array", K(ret));
  } else {
    ObArray<ServerTotalLoad *> server_load_ptrs_sorted;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      if (OB_FAIL(server_load_ptrs_sorted.push_back(&server_loads.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ServerTotalLoadCmp cmp;
      std::sort(server_load_ptrs_sorted.begin(), server_load_ptrs_sorted.end(), cmp);
      if (OB_FAIL(cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
    common::ObArray<UnitMigrateStat> task_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_tenant_units.count(); ++i) {
      const ObUnitManager::ObUnitLoad &unit_load = sys_tenant_units.at(i);
      if (OB_UNLIKELY(!unit_load.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid unit load", K(ret), K(unit_load));
      } else if (has_exist_in_array(available_servers, unit_load.unit_->server_)) {
        // skip in available server
      } else if (OB_FAIL(do_balance_sys_tenant_single_unit(
              unit_load, server_load_ptrs_sorted, task_array, pool_occupation))) {
        LOG_WARN("fail to do balance sys tenant single unit", K(ret));
      } else {} // no more to do
    }
    if (OB_FAIL(ret)) {
    } else if (task_array.count() <= 0) {
    } else if (OB_FAIL(check_and_do_migrate_unit_task_(task_array))) {
      LOG_WARN("fail to check and do migrate unit task", KR(ret), K(task_array));
    } else {} // no more to do
  }
  return ret;
}

int ObServerBalancer::try_balance_sys_tenant_units(
    const common::ObZone &zone,
    bool &do_execute)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObUnitManager::ObUnitLoad> sys_tenant_units;
  common::ObArray<common::ObAddr> available_servers;
  do_execute = false;
  bool need_balance = false;
  double g_res_weights[RES_MAX];
  ObArray<ServerTotalLoad> server_loads;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_FAIL(generate_available_servers(
          zone, false /* sys unit not excluded*/ , available_servers))) {
    LOG_WARN("fail to generate available servers", K(ret), K(zone));
  } else if (available_servers.count() <= 0) {
    LOG_INFO("no available servers, bypass", K(zone));
  } else if (OB_FAIL(unit_mgr_->get_tenant_zone_all_unit_loads(
          common::OB_SYS_TENANT_ID, zone, sys_tenant_units))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tenant zone unit loads", K(ret), K(zone));
    }
  } else if (OB_FAIL(check_need_balance_sys_tenant_units(
          sys_tenant_units, available_servers, need_balance))) {
    LOG_WARN("fail to check need balance sys tenant units", K(ret));
  } else if (!need_balance) {
    do_execute = false;
  } else if (OB_FAIL(calc_global_balance_resource_weights(
         zone, available_servers, g_res_weights, RES_MAX))) {
    LOG_WARN("fail to calc whole balance resource weights", K(ret));
  } else if (OB_FAIL(generate_complete_server_loads(
          zone, available_servers, g_res_weights, RES_MAX, server_loads))) {
    LOG_WARN("fail to generate complete server loads", K(ret));
  } else if (OB_FAIL(do_balance_sys_tenant_units(
          sys_tenant_units, available_servers, server_loads))) {
    LOG_WARN("fail to do balance sys tenant units", K(ret));
  } else {
    do_execute = true;
  }
  return ret;
}

int ObServerBalancer::try_balance_non_sys_tenant_units(
    const common::ObZone &zone,
    const common::ObIArray<Matrix<uint64_t> > &group_tenant_array,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units)
{
  int ret = OB_SUCCESS;
  ObArray<TenantGroupBalanceInfo> balance_info_array;
  ObArray<Matrix<uint64_t> > degraded_tenant_group_array;
  common::ObArray<common::ObAddr> available_servers;
  const bool enable_sys_unit_standalone = GCONF.enable_sys_unit_standalone;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone), K(available_servers));
  } else if (OB_FAIL(balance_info_array.reserve(2 * group_tenant_array.count()))) {
    LOG_WARN("ObArray fail to reserve", K(ret), "capacity", group_tenant_array.count());
  } else if (OB_FAIL(generate_tenant_balance_info_array(
          zone, group_tenant_array, degraded_tenant_group_array, balance_info_array))) {
    LOG_WARN("fail to generate tenant balance info array", K(ret));
  } else if (OB_FAIL(generate_available_servers(
          zone, enable_sys_unit_standalone, available_servers))) {
    LOG_WARN("fail to generate available servers", K(ret));
  } else if (available_servers.count() <= 0) {
    LOG_INFO("no available servers, bypass", K(zone));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < balance_info_array.count(); ++i) {
      TenantGroupBalanceInfo &balance_info = balance_info_array.at(i);
      if (OB_UNLIKELY(NULL == balance_info.tenant_id_matrix_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant id matrix in balance info is null", K(ret));
      } else if (OB_FAIL(generate_original_unit_matrix(
              *balance_info.tenant_id_matrix_, zone, balance_info.column_unit_num_array_,
              balance_info.unit_migrate_stat_matrix_))) {
        LOG_WARN("fail to generate original unit matrix", K(ret));
      } else if (OB_FAIL(single_unit_migrate_stat_matrix_balance(
              balance_info, available_servers))) {
        LOG_WARN("fail to do migrate stat matrix balance", K(ret));
      } else {
        LOG_INFO("finish single ttg balance",
                 "tenant_id_matrix", *balance_info.tenant_id_matrix_);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_amend_and_execute_unit_balance_task(
            zone, balance_info_array, standalone_units,
            not_grant_units, available_servers))) {
      LOG_WARN("fail to do global unit balance", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObServerBalancer::generate_tenant_balance_info_array(
    const common::ObZone &zone,
    const common::ObIArray<Matrix<uint64_t> > &group_tenant_array,
    common::ObIArray<Matrix<uint64_t> > &degraded_tenant_group_array,
    common::ObIArray<TenantGroupBalanceInfo> &balance_info_array)
{
  int ret = OB_SUCCESS;
  ObArray<const Matrix<uint64_t> *> unit_num_not_match_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < group_tenant_array.count(); ++i) {
      bool unit_num_match = false;
      TenantGroupBalanceInfo balance_info;
      balance_info.tenant_id_matrix_ = &group_tenant_array.at(i);
      if (OB_FAIL(check_and_get_tenant_matrix_unit_num(
              group_tenant_array.at(i), zone, unit_num_match,
              balance_info.column_unit_num_array_))) {
        LOG_WARN("fail to check and get tenant matrix unit num", K(ret));
      } else if (!unit_num_match) {
        // Suppose the tenant group has m rows and n columns.
        // When the unit num of the tenant group does not match,
        // the tenant group will degenerate into m tenant groups with 1 row and n columns.
        if (OB_FAIL(unit_num_not_match_array.push_back(&group_tenant_array.at(i)))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else if (balance_info.column_unit_num_array_.count() > 0) {
        if (OB_FAIL(balance_info_array.push_back(balance_info))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {} // no tenant of this group exists in this zone
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_generate_degraded_balance_info_array(
            zone, unit_num_not_match_array, degraded_tenant_group_array, balance_info_array))) {
      LOG_WARN("fail to try generate degraded balance info array", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObServerBalancer::do_degrade_tenant_group_matrix(
    const Matrix<uint64_t> &source_tenant_group,
    common::ObIArray<Matrix<uint64_t> > &degraded_tenant_group_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("start degrade tenant group matrix", K(source_tenant_group));
    for (int64_t i = 0; OB_SUCC(ret) && i < source_tenant_group.get_row_count(); ++i) {
      Matrix<uint64_t> tenant_id_vector;
      const int64_t VECTOR_ROW_CNT = 1;
      const int64_t VECTOR_ROW_IDX = 0;
      if (OB_FAIL(tenant_id_vector.init(VECTOR_ROW_CNT, source_tenant_group.get_column_count()))) {
        LOG_WARN("fail to init tenant id vector", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < source_tenant_group.get_column_count(); ++j) {
          uint64_t tenant_id = OB_INVALID_ID;
          if (OB_FAIL(source_tenant_group.get(i, j, tenant_id))) {
            LOG_WARN("fail to get tenant id", K(ret), K(i), K(j));
          } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
          } else if (OB_FAIL(tenant_id_vector.set(VECTOR_ROW_IDX, j, tenant_id))) {
            LOG_WARN("fail to set tenant id vector", K(ret), K(j), K(tenant_id));
          } else {} // no more to do
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(degraded_tenant_group_array.push_back(tenant_id_vector))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          LOG_INFO("output vector tenant group", K(i), "vector_ttg", tenant_id_vector);
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::try_generate_degraded_balance_info_array(
    const common::ObZone &zone,
    const common::ObIArray<const Matrix<uint64_t> *> &unit_num_not_match_array,
    common::ObIArray<Matrix<uint64_t> > &degraded_tenant_group_array,
    common::ObIArray<TenantGroupBalanceInfo> &balance_info_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_num_not_match_array.count(); ++i) {
      const Matrix<uint64_t> *this_tenant_group = unit_num_not_match_array.at(i);
      if (OB_UNLIKELY(NULL == this_tenant_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this tenant group ptr is null", K(ret));
      } else if (OB_FAIL(do_degrade_tenant_group_matrix(
              *this_tenant_group, degraded_tenant_group_array))) {
        LOG_WARN("fail to do degraded tenant group matrix", K(ret), "source", *this_tenant_group);
      } else {} // no more to do
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < degraded_tenant_group_array.count(); ++i) {
      bool unit_num_match = false;
      TenantGroupBalanceInfo balance_info;
      balance_info.tenant_id_matrix_ = &degraded_tenant_group_array.at(i);
      if (OB_FAIL(check_and_get_tenant_matrix_unit_num(
              degraded_tenant_group_array.at(i), zone, unit_num_match,
              balance_info.column_unit_num_array_))) {
        LOG_WARN("fail to check and get tenant matrix unit num", K(ret));
      } else if (!unit_num_match) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit num not match unexpected, since this tenant group is a vector",
                 K(ret), "vector tenant group", degraded_tenant_group_array.at(i));
      } else if (balance_info.column_unit_num_array_.count() > 0) {
        if (OB_FAIL(balance_info_array.push_back(balance_info))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {} // no tenant of this group exists in this zone
    }
  }
  return ret;
}

int ObServerBalancer::do_rebalance_servers_v2(
    const common::ObZone &zone,
    const common::ObIArray<Matrix<uint64_t> > &group_tenant_array,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units)
{
  int ret = OB_SUCCESS;
  bool do_sys_unit_balanced = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_FAIL(try_balance_sys_tenant_units(zone, do_sys_unit_balanced))) {
    LOG_WARN("fail to try balance sys tenant units", K(ret));
  } else if (do_sys_unit_balanced) {
    // bypass, first balance sys unit
  } else if (OB_FAIL(try_balance_non_sys_tenant_units(
          zone, group_tenant_array, standalone_units, not_grant_units))) {
    LOG_WARN("fail to try balance non sys tenant units", K(ret));
  }
  return ret;
}

int ObServerBalancer::try_amend_and_execute_unit_balance_task(
    const common::ObZone &zone,
    common::ObIArray<TenantGroupBalanceInfo> &balance_info_array,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
    const common::ObIArray<common::ObAddr> &available_servers)
{
  int ret = OB_SUCCESS;
  common::ObArray<TenantGroupBalanceInfo *> stable_tenant_group;
  common::ObArray<TenantGroupBalanceInfo *> unstable_tenant_group;
  bool do_amend_unstable_ttg = false;
  bool do_amend_stable_ttg = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(available_servers.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(divide_tenantgroup_balance_info(
          balance_info_array, stable_tenant_group, unstable_tenant_group))) {
    LOG_WARN("fail to divide tenantgroup balance info", K(ret));
  } else if (OB_FAIL(try_amend_and_execute_stable_tenantgroup(
          zone, unstable_tenant_group, stable_tenant_group, standalone_units,
          not_grant_units, available_servers, do_amend_stable_ttg))) {
    LOG_WARN("fail to amend and execute stable tenantgroup", K(ret));
  } else if (do_amend_stable_ttg) {
    // The stable tenant group has been adjusted between groups and no further operations will be performed
  } else {
    if (unstable_tenant_group.count() > 0) {
      if (OB_FAIL(try_amend_and_execute_unstable_tenantgroup(
              zone, unstable_tenant_group, stable_tenant_group, standalone_units,
              not_grant_units, available_servers, do_amend_unstable_ttg))) {
        LOG_WARN("fail to do amend and execute unit balance info", K(ret));
      }
    } else {} // do as follows

    if (OB_FAIL(ret)) {
    } else if (unstable_tenant_group.count() <= 0 || !do_amend_unstable_ttg) {
      bool do_balance_disk = false;
      if (OB_FAIL(try_balance_disk_by_stable_tenantgroup(
              zone, stable_tenant_group, available_servers, do_balance_disk))) {
        LOG_WARN("fail to do disk balance", K(ret));
      } else if (do_balance_disk) {
        // Disk adjustment is made, and no need to do non-tenantgroup balance
      } else if (OB_FAIL(do_non_tenantgroup_unit_balance_task(
              zone, standalone_units, not_grant_units, available_servers))) {
        LOG_WARN("fail to do non tenantgroup unit balance task", K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::generate_simple_ug_loads(
    common::ObIArray<SimpleUgLoad> &simple_ug_loads,
    TenantGroupBalanceInfo &balance_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (balance_info.unitgroup_load_array_.count()
             != balance_info.unit_migrate_stat_matrix_.get_column_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unitgroup load count and unit migrate stat matrix column not match", K(ret),
             "unitgroup_count", balance_info.unitgroup_load_array_.count(),
             "unit_matrix_column", balance_info.unit_migrate_stat_matrix_.get_column_count());
  } else if (balance_info.unit_migrate_stat_matrix_.get_row_count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit matrix row unexpected", K(ret),
             "unit_matrix_row", balance_info.unit_migrate_stat_matrix_.get_row_count());
  } else {
    simple_ug_loads.reset();
    for (int64_t column = 0;
         OB_SUCC(ret) && column < balance_info.unit_migrate_stat_matrix_.get_column_count();
         ++column) {
      UnitGroupLoad &unitgroup_load = balance_info.unitgroup_load_array_.at(column);
      UnitMigrateStat unit_migrate_stat;
      if (OB_FAIL(balance_info.unit_migrate_stat_matrix_.get(0, column, unit_migrate_stat))) {
        LOG_WARN("fail to get from matrix", K(ret));
      } else {
        SimpleUgLoad simple_ug_load;
        simple_ug_load.column_tenant_id_ = unitgroup_load.column_tenant_id_;
        simple_ug_load.original_server_ = unit_migrate_stat.original_pos_;
        simple_ug_load.arranged_server_ = unit_migrate_stat.arranged_pos_;
        simple_ug_load.load_sum_ = unitgroup_load.load_sum_;
        if (OB_FAIL(simple_ug_loads.push_back(simple_ug_load))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::get_ug_exchange_excluded_dst_servers(
    const int64_t ug_idx,
    const Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    common::ObIArray<common::ObAddr> &excluded_servers)
{
  int ret = OB_SUCCESS;
  excluded_servers.reset();
  common::ObArray<ObUnitManager::ObUnitLoad> zone_units;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(ug_idx >= unit_migrate_stat_matrix.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ug_idx),
             "unit migrate stat matrix column", unit_migrate_stat_matrix.get_column_count());
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  }
  for (int64_t row = 0; OB_SUCC(ret) && row < unit_migrate_stat_matrix.get_row_count(); ++row) {
    zone_units.reset();
    UnitMigrateStat unit_migrate_stat;
    if (OB_FAIL(unit_migrate_stat_matrix.get(row, ug_idx, unit_migrate_stat))) {
      LOG_WARN("fail to get unit migrate stat", K(ret));
    } else if (OB_FAIL(unit_mgr_->get_tenant_zone_all_unit_loads(
            unit_migrate_stat.tenant_id_, zone_disk_statistic_.zone_, zone_units))) {
      LOG_WARN("fail to get tenant zone all unit loads", K(ret), K(unit_migrate_stat));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_units.count(); ++i) {
        ObUnitManager::ObUnitLoad &unit_load = zone_units.at(i);
        if (!unit_load.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit load is invalid", K(ret), K(unit_load));
        }

        if (OB_FAIL(ret)) {
        } else if (has_exist_in_array(excluded_servers, unit_load.unit_->server_)) {
          // already exist in excluded servers
        } else if (OB_FAIL(excluded_servers.push_back(unit_load.unit_->server_))) {
          LOG_WARN("fail to push back", K(ret));
        }

        if (OB_FAIL(ret)) {
        } else if (!unit_load.unit_->migrate_from_server_.is_valid()) {
          // migrate from invalid, no need to put into excluded servers
        } else if (has_exist_in_array(excluded_servers, unit_load.unit_->migrate_from_server_)) {
          // already exist in excluded servers
        } else if (OB_FAIL(excluded_servers.push_back(unit_load.unit_->migrate_from_server_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::check_cm_resource_enough(
    const LoadSum &this_load,
    const ServerTotalLoad &server_load,
    bool &enough)
{
  int ret = OB_SUCCESS;
  double hard_limit = 0.0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_hard_limit(hard_limit))) {
    LOG_WARN("fail to get hard limit", K(ret));
  } else {
    enough = ((this_load.load_sum_.max_cpu() + server_load.load_sum_.load_sum_.max_cpu()
                <= server_load.resource_info_.cpu_ * hard_limit)
              && (this_load.load_sum_.min_cpu() + server_load.load_sum_.load_sum_.min_cpu()
                <= server_load.resource_info_.cpu_)
              && (static_cast<double>(this_load.load_sum_.memory_size())
                  + static_cast<double>(server_load.load_sum_.load_sum_.memory_size())
                <= static_cast<double>(server_load.resource_info_.mem_total_))
              && (static_cast<double>(this_load.load_sum_.log_disk_size())
                  + static_cast<double>(server_load.load_sum_.load_sum_.log_disk_size())
                <= static_cast<double>(server_load.resource_info_.log_disk_total_)));

    LOG_INFO("[SERVER_BALANCE] check_cm_resource_enough", K(enough), K(this_load.load_sum_),
        K(server_load.load_sum_.load_sum_), K(server_load.resource_info_), K(hard_limit));
  }
  return ret;
}

int ObServerBalancer::check_exchange_ug_make_sense(
    common::ObIArray<ServerTotalLoad> &server_loads,
    const int64_t left_idx,
    const int64_t right_idx,
    common::ObIArray<SimpleUgLoad> &simple_ug_loads,
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    bool &do_make_sense)
{
  int ret = OB_SUCCESS;
  double disk_waterlevel = 0.0;
  double disk_usage_limit = 0.0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (left_idx >= simple_ug_loads.count()
             || right_idx >= simple_ug_loads.count()
             || simple_ug_loads.count() != unit_migrate_stat_matrix.get_column_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(left_idx), K(right_idx),
             "ug_loads_count", simple_ug_loads.count(),
             "unit_matrix_column", unit_migrate_stat_matrix.get_column_count());
  } else if (OB_FAIL(get_server_balance_critical_disk_waterlevel(disk_waterlevel))) {
    LOG_WARN("fail to get disk waterlevel", K(ret));
  } else if (OB_FAIL(get_server_data_disk_usage_limit(disk_usage_limit))) {
    LOG_WARN("fail to get data disk usage limit", K(ret));
  } else {
    ObArray<ServerTotalLoad *> server_load_ptrs;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      if (OB_FAIL(server_load_ptrs.push_back(&server_loads.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    int64_t left_unit_use = 0;
    int64_t right_unit_use = 0;
    ServerDiskStatistic left_disk_statistic;
    ServerDiskStatistic right_disk_statistic;
    ServerTotalLoad *left_server_load = NULL;
    ServerTotalLoad *right_server_load = NULL;
    for (int64_t row = 0; OB_SUCC(ret) && row < unit_migrate_stat_matrix.get_row_count(); ++row) {
      UnitMigrateStat left_unit_migrate;
      ObUnitStat left_unit_stat;
      UnitMigrateStat right_unit_migrate;
      ObUnitStat right_unit_stat;
      if (OB_FAIL(unit_migrate_stat_matrix.get(row, left_idx, left_unit_migrate))) {
        LOG_WARN("fail to get from matrix", K(ret), K(row), "column", left_idx);
      } else if (OB_UNLIKELY(!left_unit_migrate.unit_load_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit load is invalid", K(ret));
      } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
              left_unit_migrate.unit_load_.unit_->unit_id_,
              left_unit_migrate.unit_load_.unit_->zone_,
              left_unit_stat))) {
        LOG_WARN("fail to get unit stat", K(ret));
      } else if (OB_FAIL(unit_migrate_stat_matrix.get(row, right_idx, right_unit_migrate))) {
        LOG_WARN("fail to get from matrix", K(ret), K(row), "column", right_idx);
      } else if (OB_UNLIKELY(!right_unit_migrate.unit_load_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit load is invalid", K(ret));
      } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
              right_unit_migrate.unit_load_.unit_->unit_id_,
              right_unit_migrate.unit_load_.unit_->zone_,
              right_unit_stat))) {
        LOG_WARN("fail to get unit stat", K(ret));
      } else {
        left_unit_use += left_unit_stat.get_required_size();
        right_unit_use += right_unit_stat.get_required_size();
      }
    }
    if (OB_SUCC(ret)) {
      bool enough = false;
      if (OB_FAIL(zone_disk_statistic_.get_server_disk_statistic(
              simple_ug_loads.at(left_idx).original_server_, left_disk_statistic))) {
        LOG_WARN("fail to get server disk statistic", K(ret));
      } else if (OB_FAIL(zone_disk_statistic_.get_server_disk_statistic(
              simple_ug_loads.at(right_idx).original_server_, right_disk_statistic))) {
        LOG_WARN("fail to get server disk statistic", K(ret));
      } else if (OB_FAIL(pick_server_load(
              simple_ug_loads.at(left_idx).original_server_,
              server_load_ptrs, left_server_load))) {
        LOG_WARN("fail to get server load", K(ret));
      } else if (OB_FAIL(pick_server_load(
              simple_ug_loads.at(right_idx).original_server_,
              server_load_ptrs, right_server_load))) {
        LOG_WARN("fail to get server load", K(ret));
      } else if (NULL == left_server_load || NULL == right_server_load) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left server or right server load ptr is null", K(ret));
      } else if (OB_FAIL(check_cm_resource_enough(
              simple_ug_loads.at(left_idx).load_sum_, *right_server_load, enough))) {
        LOG_WARN("fail to check cpu memory resource enough", K(ret));
      } else if (!enough) {
        do_make_sense = false;
      } else if (OB_FAIL(check_cm_resource_enough(
              simple_ug_loads.at(right_idx).load_sum_, *left_server_load, enough))) {
        LOG_WARN("fail to check cpu memory resource enough", K(ret));
      } else if (!enough) {
        do_make_sense = false;
      } else if (left_unit_use < right_unit_use) {
        do_make_sense = false; // Cannot lower the left disk after swap
      } else if (
          static_cast<double>(right_disk_statistic.disk_in_use_ - right_unit_use + left_unit_use)
          > disk_waterlevel * static_cast<double>(right_disk_statistic.disk_total_)) {
        do_make_sense = false; // The right disk exceeds the water mark after swapping
      } else if (
          static_cast<double>(right_disk_statistic.disk_in_use_ + left_unit_use)
          > disk_usage_limit * static_cast<double>(right_disk_statistic.disk_total_)) {
        do_make_sense = false; // Exceeded the upper limit of the right disk during the exchange
      } else if (
          static_cast<double>(left_disk_statistic.disk_in_use_ + right_unit_use)
          > disk_usage_limit * static_cast<double>(left_disk_statistic.disk_total_)) {
        do_make_sense = false;
      } else if (
          static_cast<double>(left_unit_use - right_unit_use)
          < static_cast<double>(left_disk_statistic.disk_total_) / static_cast<double>(200)) {
        do_make_sense = false; // The disk on the left side is not significantly reduced after swapping
      } else {
        do_make_sense = true;
      }
    }
  }
  return ret;
}

int ObServerBalancer::coordinate_unit_migrate_stat_matrix(
    const int64_t left_idx,
    const int64_t right_idx,
    common::ObIArray<SimpleUgLoad> &simple_ug_loads,
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(left_idx >= simple_ug_loads.count()
                         || right_idx >= simple_ug_loads.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(left_idx), K(right_idx),
             "simple ug loads count", simple_ug_loads.count());
  } else {
    SimpleUgLoad &left_ug = simple_ug_loads.at(left_idx);
    SimpleUgLoad &right_ug = simple_ug_loads.at(right_idx);
    for (int64_t row = 0; OB_SUCC(ret) && row < unit_migrate_stat_matrix.get_row_count(); ++row) {
      UnitMigrateStat left_unit_migrate;
      UnitMigrateStat right_unit_migrate;
      if (OB_FAIL(unit_migrate_stat_matrix.get(row, left_idx, left_unit_migrate))) {
        LOG_WARN("fail to get from matrix", K(ret), K(row), "column", left_idx);
      } else if (OB_FAIL(unit_migrate_stat_matrix.get(row, right_idx, right_unit_migrate))) {
        LOG_WARN("fail to get from matrix", K(ret), K(row), "column", right_idx);
      } else {
        left_unit_migrate.arranged_pos_ = right_ug.original_server_;
        right_unit_migrate.arranged_pos_ = left_ug.original_server_;
        if (OB_FAIL(unit_migrate_stat_matrix.set(row, left_idx, left_unit_migrate))) {
          LOG_WARN("fail to set", K(ret), K(row), "column", left_idx);
        } else if (OB_FAIL(unit_migrate_stat_matrix.set(row, right_idx, right_unit_migrate))) {
          LOG_WARN("fail to set", K(ret), K(row), "column", right_idx);
        } else {} // no more to do
      }
    }
    if (OB_SUCC(ret)) {
      left_ug.arranged_server_ = right_ug.original_server_;
      right_ug.arranged_server_ = left_ug.original_server_;
    }
  }
  return ret;
}

int ObServerBalancer::generate_exchange_ug_migrate_task(
    const int64_t left_idx,
    const int64_t right_idx,
    common::ObIArray<SimpleUgLoad> &simple_ug_loads,
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    common::ObIArray<UnitMigrateStat> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(left_idx >= unit_migrate_stat_matrix.get_column_count()
                         || right_idx >= unit_migrate_stat_matrix.get_column_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(left_idx), K(right_idx),
             "unit_matrix_column", unit_migrate_stat_matrix.get_column_count());
  } else {
    int64_t left_unit_use = 0;
    int64_t right_unit_use = 0;
    ServerDiskStatistic left_disk_statistic;
    ServerDiskStatistic right_disk_statistic;
    for (int64_t row = 0; OB_SUCC(ret) && row < unit_migrate_stat_matrix.get_row_count(); ++row) {
      UnitMigrateStat left_unit_migrate;
      ObUnitStat left_unit_stat;
      UnitMigrateStat right_unit_migrate;
      ObUnitStat right_unit_stat;
      if (OB_FAIL(unit_migrate_stat_matrix.get(row, left_idx, left_unit_migrate))) {
        LOG_WARN("fail to get from matrix", K(ret), K(row), "column", left_idx);
      } else if (OB_UNLIKELY(!left_unit_migrate.unit_load_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit load is invalid", K(ret));
      } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
              left_unit_migrate.unit_load_.unit_->unit_id_,
              left_unit_migrate.unit_load_.unit_->zone_,
              left_unit_stat))) {
        LOG_WARN("fail to get unit stat", K(ret));
      } else if (OB_FAIL(unit_migrate_stat_matrix.get(row, right_idx, right_unit_migrate))) {
        LOG_WARN("fail to get from matrix", K(ret), K(row), "column", right_idx);
      } else if (OB_UNLIKELY(!right_unit_migrate.unit_load_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit load is invalid", K(ret));
      } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
              right_unit_migrate.unit_load_.unit_->unit_id_,
              right_unit_migrate.unit_load_.unit_->zone_,
              right_unit_stat))) {
        LOG_WARN("fail to get unit stat", K(ret));
      } else if (OB_FAIL(task_array.push_back(left_unit_migrate))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(task_array.push_back(right_unit_migrate))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        left_unit_use += left_unit_stat.get_required_size();
        right_unit_use += right_unit_stat.get_required_size();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(zone_disk_statistic_.get_server_disk_statistic(
              simple_ug_loads.at(left_idx).original_server_, left_disk_statistic))) {
        LOG_WARN("fail to get server disk statistic", K(ret));
      } else if (OB_FAIL(zone_disk_statistic_.reduce_server_disk_use(
              simple_ug_loads.at(left_idx).original_server_, left_unit_use))) {
        LOG_WARN("fail to reduce server disk", K(ret));
      } else if (OB_FAIL(zone_disk_statistic_.raise_server_disk_use(
              simple_ug_loads.at(left_idx).original_server_, right_unit_use))) {
        LOG_WARN("fail to reduce server disk", K(ret));
      } else if (OB_FAIL(zone_disk_statistic_.get_server_disk_statistic(
              simple_ug_loads.at(right_idx).original_server_, right_disk_statistic))) {
        LOG_WARN("fail to get server disk statistic", K(ret));
      } else if (OB_FAIL(zone_disk_statistic_.reduce_server_disk_use(
              simple_ug_loads.at(right_idx).original_server_, right_unit_use))) {
        LOG_WARN("fail to reduce server disk", K(ret));
      } else if (OB_FAIL(zone_disk_statistic_.raise_server_disk_use(
              simple_ug_loads.at(right_idx).original_server_, left_unit_use))) {
        LOG_WARN("fail to reduce server disk", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObServerBalancer::try_balance_server_disk_onebyone(
    common::ObIArray<ServerTotalLoad> &server_loads,
    TenantGroupBalanceInfo &balance_info,
    common::ObIArray<SimpleUgLoad> &simple_ug_loads,
    const int64_t ug_idx,
    const common::ObIArray<common::ObAddr> &available_servers,
    const common::ObIArray<common::ObAddr> &disk_over_servers,
    common::ObIArray<UnitMigrateStat> &task_array,
    bool &do_balance_disk)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> left_excluded_servers;
  common::ObArray<common::ObAddr> right_excluded_servers;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(ug_idx >= simple_ug_loads.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ug_idx unexpected", K(ret), K(ug_idx), "ug_load count", simple_ug_loads.count());
  } else if (OB_FAIL(get_ug_exchange_excluded_dst_servers(
          ug_idx, balance_info.unit_migrate_stat_matrix_, left_excluded_servers))) {
    LOG_WARN("fail to get ug exchange dst servers", K(ret));
  } else {
    SimpleUgLoad &left_ug_load = simple_ug_loads.at(ug_idx);
    do_balance_disk = false;
    for (int64_t i = 0;
         !do_balance_disk
          && OB_SUCC(ret)
          && i < zone_disk_statistic_.server_disk_statistic_array_.count();
         ++i) {
      ServerDiskStatistic &disk_statistic
        = zone_disk_statistic_.server_disk_statistic_array_.at(i);
      if (has_exist_in_array(left_excluded_servers, disk_statistic.server_)) {
        // bypass, since in excluded
      } else if (has_exist_in_array(disk_over_servers, disk_statistic.server_)) {
        // bypass, since in disk over servers
      } else if (!has_exist_in_array(available_servers, disk_statistic.server_)) {
        // bypass, since not in available servers
      } else if (disk_statistic.wild_server_) {
        // bypass, wild server
      } else {
        for (int64_t j = 0; !do_balance_disk && OB_SUCC(ret) && j < simple_ug_loads.count(); ++j) {
          SimpleUgLoad &right_ug_load = simple_ug_loads.at(j);
          bool do_make_sense = false;
          if (left_ug_load.column_tenant_id_ == right_ug_load.column_tenant_id_) {
            // by pass, come from the same tenant column
          } else if (right_ug_load.original_server_ != right_ug_load.arranged_server_) {
            // already in migrating, ignore
          } else if (right_ug_load.original_server_ != disk_statistic.server_) {
            // not in this server
          } else if ((left_ug_load.load_sum_.load_sum_.min_cpu()
                          != right_ug_load.load_sum_.load_sum_.min_cpu())
                     || (left_ug_load.load_sum_.load_sum_.memory_size()
                          != right_ug_load.load_sum_.load_sum_.memory_size())) {
            // cpu and memory not match, exchange will violate the balance result
          } else if (OB_FAIL(get_ug_exchange_excluded_dst_servers(
                  j, balance_info.unit_migrate_stat_matrix_, right_excluded_servers))) {
            LOG_WARN("fail to get ug exchange dst servers", K(ret));
          } else if (has_exist_in_array(right_excluded_servers, left_ug_load.original_server_)) {
            // left ug server in right excluded servers
          } else if (OB_FAIL(check_exchange_ug_make_sense(
                  server_loads, ug_idx, j, simple_ug_loads,
                  balance_info.unit_migrate_stat_matrix_, do_make_sense))) {
            LOG_WARN("fail to check exchange ug make sence", K(ret));
          } else if (!do_make_sense) {
            // do not make sence
          } else if (OB_FAIL(coordinate_unit_migrate_stat_matrix(
                  ug_idx, j, simple_ug_loads, balance_info.unit_migrate_stat_matrix_))) {
            LOG_WARN("fail to coordinate unit migrate stat matrix", K(ret));
          } else if (OB_FAIL(generate_exchange_ug_migrate_task(
                  ug_idx, j, simple_ug_loads,
                  balance_info.unit_migrate_stat_matrix_, task_array))) {
            LOG_WARN("fail to generate exchange ug migrate task", K(ret));
          } else {
            do_balance_disk = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::try_balance_server_disk_by_ttg(
    common::ObIArray<ServerTotalLoad> &server_loads,
    const common::ObAddr &src_server,
    TenantGroupBalanceInfo &balance_info,
    const common::ObIArray<common::ObAddr> &available_servers,
    const common::ObIArray<common::ObAddr> &disk_over_servers,
    common::ObIArray<UnitMigrateStat> &task_array,
    bool &do_balance_disk)
{
  int ret = OB_SUCCESS;
  ObArray<SimpleUgLoad> simple_ug_loads;
  double disk_waterlevel = 0.0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!src_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_server));
  } else if (OB_FAIL(generate_simple_ug_loads(simple_ug_loads, balance_info))) {
    LOG_WARN("fail to generate simple ug loads", K(ret));
  } else if (OB_FAIL(get_server_balance_critical_disk_waterlevel(disk_waterlevel))) {
    LOG_WARN("fail to get disk waterlevel", K(ret));
  } else {
    do_balance_disk = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_ug_loads.count(); ++i) {
      bool my_do_balance_disk = false;
      SimpleUgLoad &simple_ug_load = simple_ug_loads.at(i);
      if (simple_ug_load.original_server_ != simple_ug_load.arranged_server_) {
        // bypass, since this has already been migrated by others
      } else if (src_server != simple_ug_load.original_server_) {
        // bypass, since server not match
      } else if (OB_FAIL(try_balance_server_disk_onebyone(
              server_loads, balance_info, simple_ug_loads, i, available_servers,
              disk_over_servers, task_array, my_do_balance_disk))) {
        LOG_WARN("fail to try balance server disk onebyone", K(ret));
      } else {
        do_balance_disk |= my_do_balance_disk;
      }
      if (OB_FAIL(ret)) {
      } else if (do_balance_disk) {
        break; // Performed a set of disk swaps
      }
    }
  }
  return ret;
}

int ObServerBalancer::try_balance_disk_by_stable_tenantgroup(
    const common::ObZone &zone,
    common::ObIArray<TenantGroupBalanceInfo *> &stable_ttg,
    const common::ObIArray<common::ObAddr> &available_servers,
    bool &do_balance_disk)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> disk_over_servers;
  common::ObArray<ServerTotalLoad> server_loads;
  double g_res_weights[RES_MAX];
  double disk_waterlevel = 0.0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!zone_disk_statistic_.over_disk_waterlevel()) {
    do_balance_disk = false; // The disk is within the water mark, no need to adjust
  } else if (stable_ttg.count() <= 0) {
    do_balance_disk = false; // stable ttg is empty, bypass
  } else if (OB_UNLIKELY(available_servers.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(available_servers));
  } else if (OB_FAIL(get_disk_over_available_servers(disk_over_servers))) {
    LOG_WARN("fail to get disk over servers", K(ret));
  } else if (OB_FAIL(get_server_balance_critical_disk_waterlevel(disk_waterlevel))) {
    LOG_WARN("fail to get disk waterlevel", K(ret));
  } else if (OB_FAIL(calc_global_balance_resource_weights(
          zone, available_servers, g_res_weights, RES_MAX))) {
    LOG_WARN("fail to calc whole balance resource weights", K(ret));
  } else if (OB_FAIL(generate_complete_server_loads(
          zone, available_servers, g_res_weights, RES_MAX, server_loads))) {
    LOG_WARN("fail to generate complete server loads", K(ret));
  } else {
    LOG_INFO("do balance disk by stable tenantgroup", K(ret), K(disk_over_servers));
    common::ObArray<UnitMigrateStat> task_array;
    do_balance_disk = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < disk_over_servers.count(); ++i) {
      common::ObAddr &src_server = disk_over_servers.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < stable_ttg.count(); ++j) {
        bool my_do_balance_disk = false;
        TenantGroupBalanceInfo *balance_info = stable_ttg.at(j);
        if (NULL == balance_info) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("balance info ptr is null", K(ret));
        } else if (OB_FAIL(try_balance_server_disk_by_ttg(
                server_loads, src_server, *balance_info, available_servers,
                disk_over_servers, task_array, my_do_balance_disk))) {
          LOG_WARN("fail to try balance server disk onebyone", K(ret));
        } else {
          do_balance_disk |= my_do_balance_disk;
        }
        if (OB_FAIL(ret)) {
        } else if (do_balance_disk) {
          break; // Exchange
        }
      }
    }
    if (OB_SUCC(ret) && task_array.count() > 0) {
      if (OB_FAIL(check_and_do_migrate_unit_task_(task_array))) {
        LOG_WARN("fail to check and do migrate unit task array", KR(ret), K(task_array));
      }
    }
  }
  return ret;
}

int ObServerBalancer::try_amend_and_execute_stable_tenantgroup(
    const common::ObZone &zone,
    const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
    const common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
    const common::ObIArray<common::ObAddr> &available_servers,
    bool &do_amend)
{
  int ret = OB_SUCCESS;
  TenantGroupBalanceInfo *info_may_amend = NULL;
  ObArray<TenantGroupBalanceInfo *> other_stable_ttg;
  const bool is_stable_ttg = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stable_tenant_group.count() <= 0) {
    do_amend = false;
  } else if (OB_FAIL(choose_balance_info_amend(
          stable_tenant_group, is_stable_ttg, info_may_amend, other_stable_ttg))) {
    LOG_WARN("fail to choose stable balance info amend", K(ret));
  } else if (NULL == info_may_amend) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(info_may_amend));
  } else if (OB_FAIL(try_amend_and_execute_tenantgroup(
          zone, info_may_amend, other_stable_ttg, unstable_tenant_group,
          standalone_units, not_grant_units, available_servers, do_amend))) {
    LOG_WARN("fail to amend and execute tenantgroup", K(ret));
  } else {} // no more to do
  return ret;
}

int ObServerBalancer::try_amend_and_execute_unstable_tenantgroup(
    const common::ObZone &zone,
    const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
    const common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
    const common::ObIArray<common::ObAddr> &available_servers,
    bool &do_amend)
{
  int ret = OB_SUCCESS;
  TenantGroupBalanceInfo *info_need_amend = NULL;
  ObArray<TenantGroupBalanceInfo *> other_unstable_ttg;
  const bool is_stable_ttg = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (unstable_tenant_group.count() <= 0) {
    do_amend = false;
  } else if (OB_FAIL(choose_balance_info_amend(
          unstable_tenant_group, is_stable_ttg, info_need_amend, other_unstable_ttg))) {
    LOG_WARN("fail to choose unstable balance info amend", K(ret));
  } else if (OB_UNLIKELY(NULL == info_need_amend)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(info_need_amend));
  } else if (OB_FAIL(try_amend_and_execute_tenantgroup(
          zone, info_need_amend, stable_tenant_group, other_unstable_ttg,
          standalone_units, not_grant_units, available_servers, do_amend))) {
    LOG_WARN("fail to amend and execute tenantgroup", K(ret));
  } else {} // no more to do
  return ret;
}

int ObServerBalancer::get_sys_tenant_unitgroup_loads(
    const common::ObZone &zone,
    common::ObIArray<UnitGroupLoad> &sys_tenant_ug_loads)
{
  int ret = OB_SUCCESS;
  sys_tenant_ug_loads.reset();
  common::ObArray<ObUnitManager::ObUnitLoad> unit_loads;
  common::ObReplicaType replica_type = common::REPLICA_TYPE_FULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_tenant_zone_unit_loads(
          common::OB_SYS_TENANT_ID, zone, replica_type, unit_loads))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tenant zone unit infos", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_loads.count(); ++i) {
      ObUnitManager::ObUnitLoad &unit_load = unit_loads.at(i);
      if (OB_UNLIKELY(!unit_load.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit load is valid", K(ret));
      } else {
        UnitGroupLoad ug_load;
        ug_load.column_tenant_id_ = common::OB_SYS_TENANT_ID;
        ug_load.start_column_idx_ = 0; // dummy for sys tenant
        ug_load.column_count_ = 1; // dummy for sys tenant
        ug_load.server_ = unit_load.unit_->server_;
        // assign server_load_ ptr in append\_alien\_ug\_loads()
        ug_load.server_load_ = NULL;
        if (OB_UNLIKELY(ug_load.unit_loads_.push_back(*unit_load.unit_config_))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (OB_FAIL(ug_load.sum_group_load())) {
          LOG_WARN("fail to sum group load", K(ret));
        } else if (OB_FAIL(sys_tenant_ug_loads.push_back(ug_load))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
    }
  }
  return ret;
}

int ObServerBalancer::try_amend_and_execute_tenantgroup(
    const common::ObZone &zone,
    TenantGroupBalanceInfo *info_amend,
    const common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
    const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
    const common::ObIArray<common::ObAddr> &available_servers,
    bool &do_amend)
{
  int ret = OB_SUCCESS;
  common::ObArray<UnitGroupLoad> sys_tenant_ug_loads;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == info_amend)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(get_sys_tenant_unitgroup_loads(zone, sys_tenant_ug_loads))) {
    LOG_WARN("fail to get sys tenant unitgroup_loads", K(ret));
  } else if (OB_FAIL(calc_inter_ttg_weights(
          info_amend, stable_tenant_group, sys_tenant_ug_loads,
          available_servers, info_amend->inter_weights_, RES_MAX))) {
    LOG_WARN("fail to calc inter ttg weights", K(ret));
  } else if (OB_FAIL(generate_inter_ttg_server_loads(
          info_amend, stable_tenant_group, sys_tenant_ug_loads, available_servers))) {
    LOG_WARN("fail to generate inter ttg server loads", K(ret));
  } else if (OB_FAIL(do_amend_inter_ttg_balance(
          info_amend, stable_tenant_group, info_amend->server_load_array_))) {
    LOG_WARN("fail to do amend inter ttg balance", K(ret));
  } else if (OB_FAIL(try_execute_unit_balance_task(
          zone, info_amend, standalone_units, not_grant_units,
          unstable_tenant_group, available_servers, do_amend))) {
    LOG_WARN("fail to do exuecte unit balance task", K(ret));
  } else {} // no more to do
  return ret;
}

int ObServerBalancer::calc_inter_ttg_weights(
    TenantGroupBalanceInfo *info_need_amend,
    const common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
    const common::ObIArray<UnitGroupLoad> &sys_tenant_ug_loads,
    const common::ObIArray<common::ObAddr> &available_servers,
    double *const resource_weights,
    const int64_t weights_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == resource_weights
        || RES_MAX != weights_count
        || NULL == info_need_amend)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(info_need_amend));
  } else if (OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_mgr_ is null", KR(ret), KP(server_mgr_));
  } else {
    LoadSum load_sum;
    for (int64_t i = 0;
         OB_SUCC(ret) && i < info_need_amend->unitgroup_load_array_.count();
         ++i) {
      const UnitGroupLoad &unitgroup_load = info_need_amend->unitgroup_load_array_.at(i);
      if (OB_UNLIKELY(!unitgroup_load.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unitgroup_load is invalid, unexpected", K(ret), K(unitgroup_load));
      } else if (OB_FAIL(load_sum.append_load(unitgroup_load.load_sum_))) {
        LOG_WARN("append load failed", K(ret));
      } else {} // no more to do
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_tenant_ug_loads.count(); ++i) {
      const UnitGroupLoad &unitgroup_load = sys_tenant_ug_loads.at(i);
      if (OB_UNLIKELY(!unitgroup_load.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unitgroup_load is invalid, unexpected", K(ret), K(unitgroup_load));
      } else if (OB_FAIL(load_sum.append_load(unitgroup_load.load_sum_))) {
        LOG_WARN("append load failed", K(ret));
      } else {} // no more to do
    }
    for (int64_t i = 0;
         OB_SUCC(ret) && i < stable_tenant_group.count();
         ++i) {
      const TenantGroupBalanceInfo *balance_info = stable_tenant_group.at(i);
      if (OB_UNLIKELY(NULL == balance_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("balance info ptr is null", K(ret));
      } else {
        for (int64_t j = 0;
             OB_SUCC(ret) && j < balance_info->unitgroup_load_array_.count();
             ++j) {
          const UnitGroupLoad &unitgroup_load = balance_info->unitgroup_load_array_.at(j);
          if (OB_UNLIKELY(!unitgroup_load.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit_load is invalid, unexpected", K(ret), K(unitgroup_load));
          } else if (OB_FAIL(load_sum.append_load(unitgroup_load.load_sum_))) {
            LOG_WARN("append load failed", K(ret));
          } else {} // no more to do
        }
      }
    }
    ResourceSum resource_sum;
    for (int64_t i = 0; OB_SUCC(ret) && i < available_servers.count(); ++i) {
      const common::ObAddr &server = available_servers.at(i);
      share::ObServerResourceInfo resource_info;
      if (OB_FAIL(server_mgr_->get_server_resource_info(server, resource_info))) {
        LOG_WARN("fail to get server resource_info", KR(ret), K(server));
      } else if (OB_FAIL(resource_sum.append_resource(resource_info))) {
        LOG_WARN("fail to append resource", K(ret));
      } else {} // no more to do
    }
    for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
      ObResourceType resource_type = static_cast<ObResourceType>(i);
      const double required = load_sum.get_required(resource_type);
      const double capacity = resource_sum.get_capacity(resource_type);
      if (required <= 0 || capacity <= 0) {
        resource_weights[resource_type] = 0.0;
      } else if (required >= capacity) {
        resource_weights[resource_type] = 1.0;
      } else {
        resource_weights[resource_type] = required / capacity;
      }
    }
    if (OB_SUCC(ret)) { // Weight normalization
      double sum = 0.0;
      const int64_t N = available_servers.count();
      for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
        resource_weights[i] /= static_cast<double>(N);
        sum += resource_weights[i];
        if (resource_weights[i] < 0 || resource_weights[i] > 1) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_ERROR("weight shall be in interval [0,1]", K(i), "w", resource_weights[i]);
        }
      }
      if (OB_SUCC(ret) && sum > 0) {
        for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
          resource_weights[i] /= sum;
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::append_alien_ug_loads(
    ServerLoad &server_load,
    const common::ObIArray<TenantGroupBalanceInfo *> &tenant_groups,
    common::ObIArray<UnitGroupLoad> &sys_tenant_ug_loads)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_tenant_ug_loads.count(); ++i) {
      UnitGroupLoad &unitgroup_load = sys_tenant_ug_loads.at(i);
      if (server_load.server_ != unitgroup_load.server_) {
        // not the same server
      } else if (FALSE_IT(unitgroup_load.server_load_ = &server_load)) {
        // will never be here
      } else if (OB_FAIL(server_load.alien_ug_loads_.push_back(&unitgroup_load))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_groups.count(); ++i) {
      TenantGroupBalanceInfo *balance_info = tenant_groups.at(i);
      if (OB_UNLIKELY(NULL == balance_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("balance info ptr is null", K(ret), KP(balance_info));
      } else {
        for (int64_t i = 0;
             OB_SUCC(ret) && i < balance_info->unitgroup_load_array_.count();
             ++i) {
          UnitGroupLoad &unitgroup_load = balance_info->unitgroup_load_array_.at(i);
          if (OB_UNLIKELY(!unitgroup_load.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unitgroup load is invalid, unexpected", K(ret), K(unitgroup_load));
          } else if (server_load.server_ != unitgroup_load.server_) {
            // not the same server
          } else if (OB_FAIL(server_load.alien_ug_loads_.push_back(&unitgroup_load))) {
            LOG_WARN("fail to push back", K(ret));
          } else {} // no more to do
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::generate_inter_ttg_server_loads(
    TenantGroupBalanceInfo *info_need_amend,
    const common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
    common::ObIArray<UnitGroupLoad> &sys_tenant_ug_loads,
    const common::ObIArray<common::ObAddr> &available_servers)
{
  int ret = OB_SUCCESS;
  UNUSED(available_servers);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == info_need_amend || available_servers.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(info_need_amend),
             "server_count", available_servers.count());
  } else {
    common::ObArray<ServerLoad> &target_server_loads = info_need_amend->server_load_array_;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_server_loads.count(); ++i) {
      ServerLoad &server_load = target_server_loads.at(i);
      if (!server_load.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should be a valid server load here", K(ret), K(server_load));
      } else {
        for (int32_t j = RES_CPU; j < RES_MAX; ++j) {
          server_load.inter_weights_[j] = info_need_amend->inter_weights_[j];
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < target_server_loads.count(); ++i) {
      target_server_loads.at(i).alien_ug_loads_.reset();
      if (OB_FAIL(append_alien_ug_loads(
              target_server_loads.at(i), stable_tenant_group, sys_tenant_ug_loads))) {
        LOG_WARN("fail to append alien ug loads", K(ret));
      } else if (OB_FAIL(target_server_loads.at(i).update_load_value())) {
        LOG_WARN("fail to update load value", K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::do_amend_inter_ttg_balance(
    TenantGroupBalanceInfo *balance_info,
    const common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
    common::ObIArray<ServerLoad> &server_loads)
{
  int ret = OB_SUCCESS;
  UNUSED(stable_tenant_group);
  common::ObArray<ServerLoad *> server_load_ptrs_sorted;
  common::ObArray<ServerLoad *> over_server_loads;
  common::ObArray<ServerLoad *> under_server_loads;
  double upper_lmt = 0.0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == balance_info
        || server_loads.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(balance_info),
             "server_load_count", server_loads.count());
  } else if (OB_FAIL(server_load_ptrs_sorted.reserve(server_loads.count()))) {
    LOG_WARN("fail to reserve", K(ret));
  } else if (OB_FAIL(over_server_loads.reserve(server_loads.count()))) {
    LOG_WARN("fail to reserve", K(ret));
  } else if (OB_FAIL(under_server_loads.reserve(server_loads.count()))) {
    LOG_WARN("fail to reserve", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      ServerLoad &server_load = server_loads.at(i);
      if (OB_FAIL(server_load_ptrs_sorted.push_back(&server_load))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      InterServerLoadCmp cmp;
      std::sort(server_load_ptrs_sorted.begin(), server_load_ptrs_sorted.end(), cmp);
      if (OB_FAIL(cmp.get_ret())) {
        LOG_WARN("fail to sort server load ptrs", K(ret));
      } else if (OB_FAIL(sort_server_loads_for_balance(
              server_load_ptrs_sorted, over_server_loads, under_server_loads, upper_lmt))) {
        LOG_WARN("fail to sort server loads for inter ttg balance", K(ret));
      } else if (OB_FAIL(make_inter_ttg_server_under_load(
              balance_info, over_server_loads, under_server_loads, upper_lmt))) {
        LOG_WARN("fail to make inter ttg server under load", K(ret));
      } else if (OB_FAIL(inner_ttg_balance_strategy_.coordinate_all_column_unit_group(
              balance_info->column_unit_num_array_, balance_info->unit_migrate_stat_matrix_,
              balance_info->unitgroup_load_array_))) {
        LOG_WARN("fail to coordinate column units", K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::sort_server_loads_for_balance(
    common::ObIArray<ServerLoad *> &complete_server_loads,
    common::ObIArray<ServerLoad *> &over_server_loads,
    common::ObIArray<ServerLoad *> &under_server_loads,
    double &upper_lmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(complete_server_loads.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "server_load_count", complete_server_loads.count());
  } else {
    over_server_loads.reset();
    under_server_loads.reset();
    double sum_load = 0.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < complete_server_loads.count(); ++i) {
      const ServerLoad *server_load = complete_server_loads.at(i);
      if (OB_UNLIKELY(NULL == server_load)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server load ptr is null", K(ret), KP(server_load));
      } else {
        sum_load += server_load->inter_ttg_load_value_;
      }
    }
    const int64_t cpu_mem_tolerance = GCONF.server_balance_cpu_mem_tolerance_percent;
    const double delta_percent = static_cast<double>(cpu_mem_tolerance) / static_cast<double>(100);
    const double average_load = sum_load / static_cast<double>(complete_server_loads.count());
    upper_lmt = average_load + delta_percent * average_load;
    for (int64_t i = 0; OB_SUCC(ret) && i < complete_server_loads.count(); ++i) {
      ServerLoad *server_load = complete_server_loads.at(i);
      if (OB_UNLIKELY(NULL == server_load)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server load ptr is null", K(ret), KP(server_load));
      } else if (server_load->inter_ttg_load_value_ >= upper_lmt) {
        if (OB_FAIL(over_server_loads.push_back(server_load))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        if (OB_FAIL(under_server_loads.push_back(server_load))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::make_inter_ttg_server_under_load(
    TenantGroupBalanceInfo *balance_info,
    common::ObIArray<ServerLoad *> &over_server_loads,
    common::ObIArray<ServerLoad *> &under_server_loads,
    const double inter_ttg_upper_lmt)
{
  int ret = OB_SUCCESS;
  double hard_limit = 0.0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == balance_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid balance info", K(ret), KP(balance_info));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_hard_limit(hard_limit))) {
    LOG_WARN("fail to get hard limit", K(ret));
  } else {
    double upper_limit = std::min(inter_ttg_upper_lmt, hard_limit);
    for (int64_t i = 0; OB_SUCC(ret) && i < over_server_loads.count(); ++i) {
      ServerLoad *over_server_load = over_server_loads.at(i);
      for (int64_t j = under_server_loads.count() - 1; OB_SUCC(ret) && j >= 0; --j) {
        ServerLoad *under_server_load = under_server_loads.at(j);
        if (OB_FAIL(amend_ug_inter_ttg_server_load(
                over_server_load, under_server_load, balance_info, upper_limit))) {
          LOG_WARN("fail to amend ug inter ttg server load", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::amend_ug_inter_ttg_server_load(
    ServerLoad *src_inter_load,
    ServerLoad *dst_inter_load,
    TenantGroupBalanceInfo *balance_info,
    const double inter_ttg_upper_lmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == src_inter_load
                         || NULL == dst_inter_load
                         || NULL == balance_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(src_inter_load),
             KP(dst_inter_load), KP(balance_info));
  } else if (OB_FAIL(inner_ttg_balance_strategy_.amend_ug_inter_ttg_server_load(
          src_inter_load, dst_inter_load, balance_info, inter_ttg_upper_lmt))) {
    LOG_WARN("fail to amend ug inter ttg server load", K(ret));
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::amend_ug_inter_ttg_server_load(
    ServerLoad *src_inter_load,
    ServerLoad *dst_inter_load,
    TenantGroupBalanceInfo *balance_info,
    const double inter_ttg_upper_lmt)
{
  int ret = OB_SUCCESS;
  UNUSED(inter_ttg_upper_lmt);
  if (OB_UNLIKELY(NULL == src_inter_load
                  || NULL == dst_inter_load
                  || NULL == balance_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(src_inter_load),
             KP(dst_inter_load), KP(balance_info));
  } else {
    ObIArray<UnitGroupLoad> &unitgroup_loads = balance_info->unitgroup_load_array_;
    if (unitgroup_loads.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit group load or server load empty", K(ret), K(unitgroup_loads));
    } else {
      if (src_inter_load->unitgroup_loads_.count() == dst_inter_load->unitgroup_loads_.count()) {
        if (OB_FAIL(try_exchange_ug_balance_inter_ttg_load(
                *src_inter_load, *dst_inter_load, balance_info,
                unitgroup_loads, inter_ttg_upper_lmt))) {
          LOG_WARN("fail to exchange ug balance inter ttg load", K(ret));
        }
      } else if (OB_FAIL(try_move_single_ug_balance_inter_ttg_load(
              *src_inter_load, *dst_inter_load, balance_info, inter_ttg_upper_lmt))) {
        LOG_WARN("fail to try move ug balance inter ttg load", K(ret));
      } else if (OB_FAIL(try_exchange_ug_balance_inter_ttg_load(
              *src_inter_load, *dst_inter_load, balance_info,
              unitgroup_loads, inter_ttg_upper_lmt))) {
        LOG_WARN("fail to try exchange ug balance inter ttg load", K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::try_exchange_ug_balance_inter_ttg_load(
    ServerLoad &src_inter_load,
    ServerLoad &dst_inter_load,
    TenantGroupBalanceInfo *balance_info,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    const double inter_ttg_upper_lmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == balance_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(balance_info));
  } else {
    int64_t max_times = src_inter_load.unitgroup_loads_.count()
                        + dst_inter_load.unitgroup_loads_.count();
    if (max_times >= INT32_MAX) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unitgroup loads cnt is so large", K(ret));
    } else {
      max_times = max_times * max_times;
    }
    int64_t times = 0;
    while (OB_SUCC(ret)
         && times < max_times
         && src_inter_load.inter_ttg_load_value_ - dst_inter_load.inter_ttg_load_value_ > EPSILON) {
      bool do_exchange = false;
      if (OB_FAIL(try_exchange_ug_balance_inter_ttg_load_foreach(
              src_inter_load, dst_inter_load, balance_info,
              unitgroup_loads, inter_ttg_upper_lmt, do_exchange))) {
        LOG_WARN("fail to exchange ug balance inter ttg load foreach", K(ret));
      } else if (!do_exchange) {
        break;
      } else {
        times++;
      }
    }
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::try_exchange_ug_balance_inter_ttg_load_foreach(
    ServerLoad &left_inter_load,
    ServerLoad &right_inter_load,
    TenantGroupBalanceInfo *balance_info,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    const double inter_ttg_upper_lmt,
    bool &do_exchange)
{
  int ret = OB_SUCCESS;
  do_exchange = false;
  if (OB_UNLIKELY(NULL == balance_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(balance_info));
  } else if (left_inter_load.inter_ttg_load_value_
             - right_inter_load.inter_ttg_load_value_ <= EPSILON) {
    // ignore, since this almost has no effect for load balance
  } else {
    for (int64_t i = 0;
         !do_exchange && OB_SUCC(ret) && i < left_inter_load.unitgroup_loads_.count();
         ++i) {
      common::ObArray<common::ObAddr> excluded_servers;
      UnitGroupLoad *left_ug = left_inter_load.unitgroup_loads_.at(i);
      if (OB_UNLIKELY(NULL == left_ug)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("src ug ptr is null", K(ret), KP(left_ug));
      } else if (left_inter_load.inter_ttg_load_value_ < inter_ttg_upper_lmt) {
        break;
      } else if (OB_FAIL(get_ug_balance_excluded_dst_servers(
              *left_ug, unitgroup_loads, excluded_servers))) {
        LOG_WARN("fail to get excluded server", K(ret));
      } else if (has_exist_in_array(excluded_servers, right_inter_load.server_)) {
        // ignore this
      } else {
        for (int64_t j = right_inter_load.unitgroup_loads_.count() - 1;
             !do_exchange && OB_SUCC(ret) && j >= 0;
             --j) {
          // When calculating ug load, the denominator used is right server load
          double left_ug_load_value = 0.0;
          double right_ug_load_value = 0.0;
          UnitGroupLoad *right_ug = right_inter_load.unitgroup_loads_.at(j);
          bool can_exchange = false;
          if (NULL == right_ug) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("src or dst ug ptr is null", K(ret), KP(right_ug));
          } else if (left_ug->column_tenant_id_ == right_ug->column_tenant_id_) {
            // ignore this
          } else if (OB_FAIL(get_ug_balance_excluded_dst_servers(
                  *right_ug, unitgroup_loads, excluded_servers))) {
            LOG_WARN("fail to get excluded servers", K(ret));
          } else if (has_exist_in_array(excluded_servers, left_inter_load.server_)) {
            // ignore this
          } else if (left_ug->get_inter_ttg_load_value(right_inter_load, left_ug_load_value)) {
            LOG_WARN("fail to get inter ttg load value", K(ret));
          } else if (right_ug->get_inter_ttg_load_value(right_inter_load, right_ug_load_value)) {
            LOG_WARN("fail to get inter ttg load value", K(ret));
          } else if (left_ug_load_value - right_ug_load_value <= EPSILON) {
            // ignore this, since this almost has no effect for load balance
          } else if (right_inter_load.inter_ttg_load_value_
                     + left_ug_load_value - right_ug_load_value >= inter_ttg_upper_lmt) {
            // ignore this
          } else if (OB_FAIL(check_can_exchange_ug_balance_inter_ttg_load(
                *left_ug, left_inter_load, *right_ug, right_inter_load, can_exchange))) {
            LOG_WARN("fail to check can exchange ug balance inter ttg load", K(ret));
          } else if (!can_exchange) {
            // ignore this
          } else if (OB_FAIL(exchange_ug_between_server_loads(
                *left_ug, i, left_inter_load, *right_ug, j, right_inter_load))) {
            LOG_WARN("fail to exchange ug between server loads", K(ret));
          } else if (OB_FAIL(coordinate_single_column_unit_group(
                  left_ug->start_column_idx_, left_ug->column_count_,
                  balance_info->unit_migrate_stat_matrix_,
                  balance_info->unitgroup_load_array_))) {
            LOG_WARN("fail to coordinate single column unit", K(ret));
          } else if (OB_FAIL(coordinate_single_column_unit_group(
                  right_ug->start_column_idx_, right_ug->column_count_,
                  balance_info->unit_migrate_stat_matrix_,
                  balance_info->unitgroup_load_array_))) {
            LOG_WARN("fail to coordinate single column unit", K(ret));
          } else {
            do_exchange = true;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      UnitGroupLoadCmp unitgroup_load_cmp(left_inter_load);
      std::sort(left_inter_load.unitgroup_loads_.begin(),
                left_inter_load.unitgroup_loads_.end(),
                unitgroup_load_cmp);
      if (OB_FAIL(unitgroup_load_cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      UnitGroupLoadCmp unitgroup_load_cmp(right_inter_load);
      std::sort(right_inter_load.unitgroup_loads_.begin(),
                right_inter_load.unitgroup_loads_.end(),
                unitgroup_load_cmp);
      if (OB_FAIL(unitgroup_load_cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::try_move_single_ug_balance_inter_ttg_load(
    ServerLoad &src_inter_load,
    ServerLoad &dst_inter_load,
    TenantGroupBalanceInfo *balance_info,
    const double inter_ttg_upper_lmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == balance_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(balance_info));
  } else {
    common::ObIArray<UnitGroupLoad> &unitgroup_loads = balance_info->unitgroup_load_array_;
    if (src_inter_load.unitgroup_loads_.count() <= dst_inter_load.unitgroup_loads_.count()) {
      // ignore since, src intra cnt is not greater than dst
    } else if (src_inter_load.inter_ttg_load_value_ <= dst_inter_load.inter_ttg_load_value_) {
      // ignore since src intra load is not greater than dst
    } else {
      ObArray<common::ObAddr> excluded_dst_servers;
      for (int64_t i = src_inter_load.unitgroup_loads_.count() - 1;
           OB_SUCC(ret) && i >= 0;
           --i) {
        UnitGroupLoad &ug_load = *src_inter_load.unitgroup_loads_.at(i);
        double ug_load_value = 0.0;
        bool can_move = false;
        excluded_dst_servers.reset();
        if (OB_FAIL(get_ug_balance_excluded_dst_servers(
                ug_load, unitgroup_loads, excluded_dst_servers))) {
          LOG_WARN("fail to get excluded server", K(ret));
        } else if (has_exist_in_array(excluded_dst_servers, dst_inter_load.server_)) {
          // in excluded servers, ignore
        } else if (OB_FAIL(ug_load.get_inter_ttg_load_value(dst_inter_load, ug_load_value))) {
          LOG_WARN("fail to get inter ttg load value", K(ret));
        } else if (dst_inter_load.inter_ttg_load_value_ + ug_load_value >= inter_ttg_upper_lmt) {
          // ignore, since this amendment will violate the balance in terms of inter ttg
        } else if (OB_FAIL(check_can_move_ug_balance_inter_ttg_load(
                src_inter_load, ug_load, dst_inter_load, can_move))) {
          LOG_WARN("fail to check can move ug balance inter ttg load", K(ret));
        } else if (!can_move) {
          // ignore
        } else if (OB_FAIL(move_ug_between_server_loads(
                src_inter_load, dst_inter_load, ug_load, i))) {
          LOG_WARN("fail to move ug between server loads", K(ret));
        } else if (OB_FAIL(coordinate_single_column_unit_group(
                ug_load.start_column_idx_, ug_load.column_count_,
                balance_info->unit_migrate_stat_matrix_,
                balance_info->unitgroup_load_array_))) {
          LOG_WARN("fail to coordinate single column unit", K(ret));
        } else {
          break;
        }
      }
      if (OB_SUCC(ret)) {
        UnitGroupLoadCmp unitgroup_load_cmp(src_inter_load);
        std::sort(src_inter_load.unitgroup_loads_.begin(),
                  src_inter_load.unitgroup_loads_.end(),
                  unitgroup_load_cmp);
        if (OB_FAIL(unitgroup_load_cmp.get_ret())) {
          LOG_WARN("fail to sort", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        UnitGroupLoadCmp unitgroup_load_cmp(dst_inter_load);
        std::sort(dst_inter_load.unitgroup_loads_.begin(),
                  dst_inter_load.unitgroup_loads_.end(),
                  unitgroup_load_cmp);
        if (OB_FAIL(unitgroup_load_cmp.get_ret())) {
          LOG_WARN("fail to sort", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::try_execute_unit_balance_task(
    const common::ObZone &zone,
    TenantGroupBalanceInfo *info_need_amend,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
    const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
    const common::ObIArray<common::ObAddr> &available_servers,
    bool &do_amend)
{
  int ret = OB_SUCCESS;
  common::ObArray<UnitMigrateStat *> task_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == info_need_amend)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (info_need_amend->is_stable()) {
    do_amend = false; // balance info is stable, no need to execute unit balance task
  } else if (info_need_amend->unit_migrate_stat_matrix_.get_element_count() <= 0
             || info_need_amend->unit_migrate_stat_matrix_.get_element_count() >= INT32_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(*info_need_amend));
  } else if (OB_FAIL(task_array.reserve(
          info_need_amend->unit_migrate_stat_matrix_.get_element_count()))) {
    LOG_WARN("fail to reserve", K(ret));
  } else if (OB_FAIL(generate_unit_balance_task(*info_need_amend, task_array))) {
    LOG_WARN("fail to generate unit balance task", K(ret));
  } else if (task_array.count() <= 0) {
    if (OB_FAIL(vacate_space_for_ttg_balance(
            zone, *info_need_amend, standalone_units, not_grant_units,
            unstable_tenant_group, available_servers, do_amend))) {
      LOG_WARN("fail to vacate space for ttg balance", K(ret));
    }
  } else if (OB_FAIL(do_migrate_unit_task(task_array))) {
    LOG_WARN("fail to do migrate unit task array", K(ret));
  } else {
    do_amend = true; // A migration task is executed
  }
  return ret;
}

int ObServerBalancer::do_migrate_unit_task(
    const common::ObIArray<UnitMigrateStat> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret), KP(unit_mgr_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array.count(); ++i) {
      ObUnitStat unit_stat;
      ObArray<ObUnitStat> in_migrate_unit_stat;
      const UnitMigrateStat &unit_migrate_stat = task_array.at(i);
      bool can_migrate_in = false;
      if (!unit_migrate_stat.unit_load_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(ret), "unit_load", unit_migrate_stat.unit_load_);
      } else if (OB_FAIL(SVR_TRACER.check_server_can_migrate_in(
              unit_migrate_stat.arranged_pos_, can_migrate_in))) {
        LOG_WARN("fail to check can migrate in", K(ret));
      } else if (!can_migrate_in) {
        // bypass
      } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
              unit_migrate_stat.unit_load_.unit_->unit_id_,
              unit_migrate_stat.unit_load_.unit_->zone_,
              unit_stat))) {
        LOG_WARN("fail to get unit stat", K(ret));
      } else if (OB_FAIL(get_unit_resource_reservation(
                  unit_migrate_stat.unit_load_.unit_->unit_id_, unit_migrate_stat.arranged_pos_,
                  in_migrate_unit_stat))) {
        LOG_WARN("fail to get unit resource reservation", K(ret));
      } else if (OB_FAIL(try_migrate_unit(
              unit_migrate_stat.unit_load_.unit_->unit_id_,
              unit_migrate_stat.unit_load_.pool_->tenant_id_, unit_stat,
              in_migrate_unit_stat, unit_migrate_stat.arranged_pos_))) {
        if (OB_OP_NOT_ALLOW == ret) {
          LOG_INFO("cannot migrate unit, server full",
                   "unit", *unit_migrate_stat.unit_load_.unit_,
                   "dst_server", unit_migrate_stat.arranged_pos_);
          ret = OB_SUCCESS; // ingore this unit
        } else {
          LOG_WARN("fail to migrate unit", K(ret),
                   "unit", *unit_migrate_stat.unit_load_.unit_,
                   "dst_server", unit_migrate_stat.arranged_pos_);
        }
      } else {} // no more to do
    }
  }
  return ret;
}

int ObServerBalancer::do_migrate_unit_task(
    const common::ObIArray<UnitMigrateStat *> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret), KP(unit_mgr_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_array.count(); ++i) {
      ObUnitStat unit_stat;
      ObArray<ObUnitStat> in_migrate_unit_stat;
      const UnitMigrateStat *unit_migrate_stat = task_array.at(i);
      bool can_migrate_in = false;
      if (OB_UNLIKELY(NULL == unit_migrate_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit migrate stat ptr is null", K(ret), KP(unit_migrate_stat));
      } else if (!unit_migrate_stat->unit_load_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(ret), "unit_load", unit_migrate_stat->unit_load_);
      } else if (OB_FAIL(SVR_TRACER.check_server_can_migrate_in(
          unit_migrate_stat->arranged_pos_,
          can_migrate_in))) {
        LOG_WARN("fail to check can migrate in", K(ret));
      } else if (!can_migrate_in) {
        // bypass
      } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
              unit_migrate_stat->unit_load_.unit_->unit_id_,
              unit_migrate_stat->unit_load_.unit_->zone_,
              unit_stat))) {
        LOG_WARN("fail to get unit stat", K(ret));
      } else if (OB_FAIL(get_unit_resource_reservation(
              unit_migrate_stat->unit_load_.unit_->unit_id_, unit_migrate_stat->arranged_pos_,
              in_migrate_unit_stat))) {
        LOG_WARN("fail to get unit resource reservation", K(ret));
      } else if (OB_FAIL(try_migrate_unit(
              unit_migrate_stat->unit_load_.unit_->unit_id_,
              unit_migrate_stat->unit_load_.pool_->tenant_id_, unit_stat,
              in_migrate_unit_stat, unit_migrate_stat->arranged_pos_))) {
        if (OB_OP_NOT_ALLOW == ret) {
          LOG_INFO("cannot migrate unit, server full",
                   "unit", *unit_migrate_stat->unit_load_.unit_,
                   "dst_server", unit_migrate_stat->arranged_pos_);
          ret = OB_SUCCESS; // ingore this unit
        } else {
          LOG_WARN("fail to migrate unit", K(ret),
                   "unit", *unit_migrate_stat->unit_load_.unit_,
                   "dst_server", unit_migrate_stat->arranged_pos_);
        }
      } else {} // no more to do
    }
  }
  return ret;
}

// 1. Try to perform all tasks in the matrix, if all tasks cannot be performed
// 2. Try to execute the first row of tasks in the matrix,
//    if the first row of tasks cannot be executed
// 3. Try to execute the first task in the matrix,
//    if the first one cannot be executed, give up
int ObServerBalancer::generate_unit_balance_task(
    TenantGroupBalanceInfo &balance_info,
    common::ObIArray<UnitMigrateStat *> &task_array)
{
  int ret = OB_SUCCESS;
  LOG_INFO("tenant group migrate unit stat matrix",
           "unit migrate stat matrix", balance_info.unit_migrate_stat_matrix_);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(try_generate_square_task_from_ttg_matrix(
          balance_info.unit_migrate_stat_matrix_, task_array))) {
    LOG_WARN("fail to generate balance task from ttg matrix", K(ret));
  } else if (task_array.count() > 0) {
    // good, has task generated
  } else if (OB_FAIL(try_generate_line_task_from_ttg_matrix(
          balance_info.unit_migrate_stat_matrix_, task_array))) {
    LOG_WARN("fail to generate balance task from ttg matrix", K(ret));
  } else if (task_array.count() > 0) {
    // good, has task generated
  } else if (OB_FAIL(try_generate_dot_task_from_ttg_matrix(
          balance_info.unit_migrate_stat_matrix_, task_array))) {
    LOG_WARN("fail to generate balance task from ttg matrix", K(ret));
  } else if (task_array.count() > 0) {
    // good, has task generated
  } else {
    // no task generated, sad
  }
  return ret;
}

int ObServerBalancer::check_unit_migrate_stat_unit_collide(
    UnitMigrateStat &unit_migrate_stat,
    ObUnitManager::ObUnitLoad &collide_unit,
    bool &tenant_unit_collide)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObUnitManager::ObUnitLoad> zone_units;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(common::OB_INVALID_ID == unit_migrate_stat.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit_migrate_stat));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_tenant_zone_all_unit_loads(
          unit_migrate_stat.tenant_id_, zone_disk_statistic_.zone_, zone_units))) {
    LOG_WARN("fail to get tenant zone all unit loads", K(ret), K(unit_migrate_stat));
  } else {
    tenant_unit_collide = false;
    for (int64_t i = 0; !tenant_unit_collide && OB_SUCC(ret) && i < zone_units.count(); ++i) {
      ObUnitManager::ObUnitLoad &unit_load = zone_units.at(i);
      if (!unit_load.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit load is invalid", K(ret), K(unit_load));
      } else if (unit_load.unit_->server_ == unit_migrate_stat.arranged_pos_) {
        tenant_unit_collide = true;
        collide_unit = unit_load;
      } else {} // good, go on to check next
    }
  }
  return ret;
}

int ObServerBalancer::accumulate_balance_task_loadsum(
    const UnitMigrateStat &unit_migrate_stat,
    common::ObIArray<ServerLoadSum> &server_load_sums)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!unit_migrate_stat.unit_load_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObUnitStat unit_stat;
    int64_t idx = 0;
    for (; idx < server_load_sums.count(); ++idx) {
      ServerLoadSum &server_load_sum = server_load_sums.at(idx);
      if (server_load_sum.server_ != unit_migrate_stat.arranged_pos_) {
        // next
      } else {
        break; // find one
      }
    }
    if (idx >= server_load_sums.count()) {
      ServerLoadSum server_load_sum;
      server_load_sum.server_ = unit_migrate_stat.arranged_pos_;
      if (OB_FAIL(server_load_sums.push_back(server_load_sum))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (idx >= server_load_sums.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server load sums count unexpecte", K(ret), K(idx));
    } else if (OB_FAIL(server_load_sums.at(idx).load_sum_.append_load(
            *unit_migrate_stat.unit_load_.unit_config_))) {
      LOG_WARN("fail to append load", K(ret));
    } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
            unit_migrate_stat.unit_load_.unit_->unit_id_,
            unit_migrate_stat.unit_load_.unit_->zone_,
            unit_stat))) {
      LOG_WARN("fail to get unit stat", K(ret));
    } else {
      server_load_sums.at(idx).disk_in_use_ += unit_stat.get_required_size();
    }
  }
  return ret;
}

int ObServerBalancer::check_servers_resource_enough(
    const common::ObIArray<ServerLoadSum> &server_load_sums,
    bool &enough)
{
  int ret = OB_SUCCESS;
  double hard_limit = 0.0;
  double disk_waterlevel = 0.0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(unit_mgr_) || OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ or server_mgr_is null", K(ret), KP(unit_mgr_), KP(server_mgr_));
  } else if (OB_FAIL(unit_mgr_->get_hard_limit(hard_limit))) {
    LOG_WARN("fail to hard limit", K(ret));
  } else if (OB_FAIL(get_server_balance_critical_disk_waterlevel(disk_waterlevel))) {
    LOG_WARN("fail to get critical disk waterlevel", K(ret));
  } else {
    enough = true;
    for (int64_t i = 0; OB_SUCC(ret) && enough && i < server_load_sums.count(); ++i) {
      ObArray<ObUnitManager::ObUnitLoad> *unit_loads = NULL;
      share::ObServerResourceInfo server_resource_info;
      const common::ObAddr &server = server_load_sums.at(i).server_;
      LoadSum load_sum = server_load_sums.at(i).load_sum_;
      int64_t disk_in_use = server_load_sums.at(i).disk_in_use_;
      ServerDiskStatistic disk_statistic;
      if (OB_FAIL(zone_disk_statistic_.get_server_disk_statistic(server, disk_statistic))) {
        LOG_WARN("fail to get disk statistic", K(ret), K(server));
      } else if (OB_FAIL(server_mgr_->get_server_resource_info(server, server_resource_info))) {
        // **TODO (linqiucen.lqc): temp.solution
        LOG_WARN("fail to get server resource info", KR(ret), K(server));
      } else if (OB_FAIL(unit_mgr_->get_loads_by_server(server, unit_loads))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get loads by server", K(ret));
        } else {
          ret = OB_SUCCESS;
          // unit_loads does not exist, there is no load on this server
        }
      } else if (NULL == unit_loads) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit loads ptr is null", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < unit_loads->count(); ++j) {
          const ObUnitManager::ObUnitLoad &load = unit_loads->at(j);
          if (!load.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("load is invalid", K(ret));
          } else if (OB_FAIL(load_sum.append_load(*load.unit_config_))) {
            LOG_WARN("fail to append", K(ret));
          } else {} // no more to do
        }
      }
      if (OB_SUCC(ret)) {
        if (load_sum.load_sum_.max_cpu()
                 > server_resource_info.cpu_ * hard_limit
            || load_sum.load_sum_.min_cpu()
                 > server_resource_info.cpu_
            || static_cast<double>(load_sum.load_sum_.memory_size())
                 > static_cast<double>(server_resource_info.mem_total_)
            || static_cast<double>(load_sum.load_sum_.log_disk_size())
                 > static_cast<double>(server_resource_info.log_disk_total_)
            || static_cast<double>(disk_in_use + disk_statistic.disk_in_use_)
                 > static_cast<double>(disk_statistic.disk_total_) * disk_waterlevel) {
          enough = false;
          LOG_INFO("resource not enough", K(load_sum), K(hard_limit), K(disk_waterlevel),
                   K(disk_in_use), K(disk_statistic),
                   "server_load_sum", server_load_sums.at(i));
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::try_generate_square_task_from_ttg_matrix(
    Matrix<UnitMigrateStat> &unit_migrate_stat_task,
    common::ObIArray<UnitMigrateStat *> &task_array)
{
  int ret = OB_SUCCESS;
  task_array.reset();
  common::ObArray<ServerLoadSum> server_load_sums;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObUnitManager::ObUnitLoad dummy_collide_unit;
    bool tenant_unit_collide = false;
    for (int64_t i = 0;
         !tenant_unit_collide && OB_SUCC(ret) && i < unit_migrate_stat_task.get_row_count();
         ++i) {
      for (int64_t j = 0;
           !tenant_unit_collide && OB_SUCC(ret) && j < unit_migrate_stat_task.get_column_count();
           ++j) {
        UnitMigrateStat *ptr = unit_migrate_stat_task.get(i, j);
        if (OB_UNLIKELY(NULL == ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit migrate stat ptr is null", K(ret));
        } else if (ptr->original_pos_ == ptr->arranged_pos_) {
          // ignore since this do not need to migrate
        } else if (OB_FAIL(check_unit_migrate_stat_unit_collide(
                *ptr, dummy_collide_unit, tenant_unit_collide))) {
          LOG_WARN("fail to check unit migrate stat unit collide", K(ret));
        } else if (tenant_unit_collide) {
          LOG_INFO("migrate unit collide", "migrate_unit", *ptr,
                   "collide_unit", dummy_collide_unit);
        } else if (OB_FAIL(accumulate_balance_task_loadsum(*ptr, server_load_sums))) {
          LOG_WARN("fail to accumulate balance task loadsum", K(ret));
        } else if (OB_FAIL(task_array.push_back(ptr))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
    }
    bool enough = true;
    if (OB_FAIL(ret)) {
      // previous procedure failed
    } else if (tenant_unit_collide) {
      task_array.reset();
      // Matrix task cannot be executed at the same time, clear task
    } else if (OB_FAIL(check_servers_resource_enough(server_load_sums, enough))) {
      LOG_WARN("fail to check server resource capacity", K(ret));
    } else if (enough) {
      // good, return the task array
    } else {
      LOG_INFO("server resource not enough");
      task_array.reset();
      // Matrix task cannot be executed at the same time, clear task
    }
  }
  return ret;
}

int ObServerBalancer::try_generate_line_task_from_ttg_matrix(
    Matrix<UnitMigrateStat> &unit_migrate_stat_task,
    common::ObIArray<UnitMigrateStat *> &task_array)
{
  int ret = OB_SUCCESS;
  task_array.reset();
  common::ObArray<ServerLoadSum> server_load_sums;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObUnitManager::ObUnitLoad dummy_collide_unit;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_migrate_stat_task.get_row_count(); ++i) {
      server_load_sums.reset();
      bool enough = true;
      bool tenant_unit_collide = false;
      for (int64_t j = 0;
           !tenant_unit_collide && OB_SUCC(ret) && j < unit_migrate_stat_task.get_column_count();
           ++j) {
        UnitMigrateStat *ptr = unit_migrate_stat_task.get(i, j);
        if (OB_UNLIKELY(NULL == ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit migrate stat ptr is null", K(ret));
        } else if (ptr->original_pos_ == ptr->arranged_pos_) {
          // ignore since this do not need to migrate
        } else if (OB_FAIL(check_unit_migrate_stat_unit_collide(
                *ptr, dummy_collide_unit, tenant_unit_collide))) {
          LOG_WARN("fail to check unit migrate stat unit collide", K(ret));
        } else if (tenant_unit_collide) {
          LOG_INFO("migrate unit collide", "migrate_unit", *ptr,
                   "collide_unit", dummy_collide_unit);
        } else if (OB_FAIL(accumulate_balance_task_loadsum(*ptr, server_load_sums))) {
          LOG_WARN("fail to accumulate balance task loadsum", K(ret));
        } else if (OB_FAIL(task_array.push_back(ptr))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
      if (OB_FAIL(ret)) {
      } else if (tenant_unit_collide) {
        task_array.reset();
        break;
      } else if (task_array.count() <= 0) {
        // do nothing
      } else if (OB_FAIL(check_servers_resource_enough(server_load_sums, enough))) {
        LOG_WARN("fail to check server resource capacity", K(ret));
      } else {
        if (enough) {
          // good, return the task array
        } else {
          LOG_INFO("server resource not enough");
          task_array.reset();
          // row task cannot be executed at the same time, clear task
        }
        break;
      }
    }
  }
  return ret;
}

int ObServerBalancer::try_generate_dot_task_from_migrate_stat(
    UnitMigrateStat &unit_migrate_stat,
    common::ObIArray<UnitMigrateStat *> &task_array)
{
  int ret = OB_SUCCESS;
  common::ObArray<ServerLoadSum> server_load_sums;
  ObUnitManager::ObUnitLoad dummy_collide_unit;
  bool tenant_unit_collide = false;
  bool enough = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (unit_migrate_stat.original_pos_ == unit_migrate_stat.arranged_pos_) {
    // ignore since this do not need to migrate
  } else if (OB_FAIL(check_unit_migrate_stat_unit_collide(
          unit_migrate_stat, dummy_collide_unit, tenant_unit_collide))) {
    LOG_WARN("fail to check unit migrate stat unit", K(ret));
  } else if (tenant_unit_collide) {
    LOG_INFO("migrate unit collide", K(unit_migrate_stat), K(dummy_collide_unit));
    task_array.reset();
  } else if (OB_FAIL(accumulate_balance_task_loadsum(unit_migrate_stat, server_load_sums))) {
    LOG_WARN("fail to accumulate balance task loadsum", K(ret));
  } else if (OB_FAIL(task_array.push_back(&unit_migrate_stat))) {
    LOG_WARN("fail to push back", K(ret));
  } else if (OB_FAIL(check_servers_resource_enough(server_load_sums, enough))) {
    LOG_WARN("fail to check server resource capacity", K(ret));
  } else if (enough) {
    // good
  } else {
    LOG_INFO("server resource not enough", K(server_load_sums));
    task_array.reset();
  }
  return ret;
}

int ObServerBalancer::try_generate_dot_task_from_ttg_matrix(
    Matrix<UnitMigrateStat> &unit_migrate_stat_task,
    common::ObIArray<UnitMigrateStat *> &task_array)
{
  int ret = OB_SUCCESS;
  task_array.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (unit_migrate_stat_task.get_row_count() <= 0
             || unit_migrate_stat_task.get_column_count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit migrate stat task array unexpected", K(ret), K(unit_migrate_stat_task));
  } else {
    bool first_row_stable = true;
    for (int64_t column = 0;
         first_row_stable && OB_SUCC(ret) && column < unit_migrate_stat_task.get_column_count();
         ++column) {
      UnitMigrateStat *ptr = unit_migrate_stat_task.get(0, column);
      if (OB_UNLIKELY(NULL == ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit migrate stat ptr is null", K(ret));
      } else if (ptr->original_pos_ == ptr->arranged_pos_) {
        // good, and bypass
      } else {
        first_row_stable = false;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (first_row_stable) {
      // The first line is the reference line, no migration is required, starting from the second line
      bool finish = false;
      for (int64_t i = 1;
           !finish && OB_SUCC(ret) && i < unit_migrate_stat_task.get_row_count();
           ++i) {
        for (int64_t j = 0;
             !finish && OB_SUCC(ret) && j < unit_migrate_stat_task.get_column_count();
             ++j) {
          UnitMigrateStat *ptr = unit_migrate_stat_task.get(i, j);
          if (OB_UNLIKELY(NULL == ptr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit migrate stat ptr is null", K(ret));
          } else if (OB_FAIL(try_generate_dot_task_from_migrate_stat(*ptr, task_array))) {
            LOG_WARN("fail to try generate dot task from migrate stat", K(ret));
          } else if (task_array.count() > 0) {
            finish = true;
          } else {} // not finish, go on next
        }
      }
    } else {
      bool finish = false;
      for (int64_t column = 0;
           !finish && OB_SUCC(ret) && column < unit_migrate_stat_task.get_column_count();
           ++column) {
        UnitMigrateStat *ptr = unit_migrate_stat_task.get(0, column);
        if (OB_UNLIKELY(NULL == ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit migrate stat ptr is null", K(ret));
        } else if (OB_FAIL(try_generate_dot_task_from_migrate_stat(*ptr, task_array))) {
          LOG_WARN("fail to try generate dot task from migrate stat", K(ret));
        } else if (task_array.count() > 0) {
          finish = true;
        } else {} // not finish, go on next
      }
    }
  }
  return ret;
}

int ObServerBalancer::vacate_space_for_ttg_balance(
    const common::ObZone &zone,
    TenantGroupBalanceInfo &balance_info,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
    const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
    const common::ObIArray<common::ObAddr> &available_servers,
    bool &do_amend)
{
  int ret = OB_SUCCESS;
  double g_res_weights[RES_MAX];
  ObArray<ServerTotalLoad> server_loads;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(calc_global_balance_resource_weights(
         zone, available_servers, g_res_weights, RES_MAX))) {
    LOG_WARN("fail to calc whole balance resource weights", K(ret));
  } else if (OB_FAIL(generate_complete_server_loads(
          zone, available_servers, g_res_weights, RES_MAX, server_loads))) {
    LOG_WARN("fail to generate complete server loads", K(ret));
  } else {
    bool find = false;
    Matrix<UnitMigrateStat> &migrate_matrix = balance_info.unit_migrate_stat_matrix_;
    for (int64_t i = 0;
         !find && OB_SUCC(ret) && i < migrate_matrix.get_row_count();
         ++i) {
      for (int64_t j = 0;
           !find && OB_SUCC(ret) && j < migrate_matrix.get_column_count();
           ++j) {
        UnitMigrateStat *unit_migrate_stat = migrate_matrix.get(i, j);
        bool tenant_unit_collide = false;
        ObUnitManager::ObUnitLoad collide_unit;
        if (OB_UNLIKELY(NULL == unit_migrate_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("migrate stat ptr is null", K(ret), KP(unit_migrate_stat));
        } else if (unit_migrate_stat->original_pos_ == unit_migrate_stat->arranged_pos_) {
          // ignore this, since this do not need to amend
        } else if (OB_FAIL(check_unit_migrate_stat_unit_collide(
                *unit_migrate_stat, collide_unit, tenant_unit_collide))) {
          LOG_WARN("fail to check unit migrate stat unit collide", K(ret));
        } else if (tenant_unit_collide) {
          if (OB_FAIL(do_migrate_out_collide_unit(
                  *unit_migrate_stat, collide_unit, server_loads, do_amend))) {
            LOG_WARN("fail to do migrate out collide unit", K(ret));
          } else {
            find = true;
          }
        } else {
          if (OB_FAIL(do_vacate_space_for_ttg_balance(
                  *unit_migrate_stat, standalone_units, not_grant_units,
                  unstable_tenant_group, server_loads, do_amend))) {
            LOG_WARN("fail to do vacate space for ttg balance", K(ret));
          } else {
            find = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::do_migrate_out_collide_unit(
    UnitMigrateStat &unit_migrate_stat,
    ObUnitManager::ObUnitLoad &collide_unit_load,
    common::ObArray<ServerTotalLoad> &server_loads,
    bool &do_amend)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObUnitManager::ObUnitLoad> zone_units;
  common::ObArray<PoolOccupation> pool_occupation;
  common::ObArray<ServerTotalLoad *> server_load_ptrs_sorted;
  ObArray<common::ObAddr> excluded_servers;
  ServerTotalLoad *server_load_to_vacate = NULL;
  ObUnitStat unit_stat;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(common::OB_INVALID_ID == unit_migrate_stat.tenant_id_
                         || !collide_unit_load.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit_migrate_stat), K(collide_unit_load));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
          collide_unit_load.unit_->unit_id_, collide_unit_load.unit_->zone_, unit_stat))) {
    LOG_WARN("fail to get unit stat", K(ret), K(collide_unit_load));
  } else if (OB_FAIL(unit_mgr_->get_tenant_zone_all_unit_loads(
          unit_migrate_stat.tenant_id_, zone_disk_statistic_.zone_, zone_units))) {
    LOG_WARN("fail to get tenant zone all unit loads", K(ret), K(unit_migrate_stat));
  } else if (OB_FAIL(generate_pool_occupation_array(zone_units, pool_occupation))) {
    LOG_WARN("fail to generate pool occupation", K(ret));
  } else if (OB_FAIL(pick_vacate_server_load(
          server_loads, server_load_ptrs_sorted, unit_migrate_stat, server_load_to_vacate))) {
    LOG_WARN("fail to pick vacate server load", K(ret));
  } else if (OB_UNLIKELY(NULL == server_load_to_vacate)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vacate server load ptr is null", K(ret));
  } else if (server_load_to_vacate->server_ != collide_unit_load.unit_->server_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server not match", K(ret),
             "left_server", server_load_to_vacate->server_,
             "right_server", collide_unit_load.unit_->server_);
  } else if (OB_FAIL(get_pool_occupation_excluded_dst_servers(
          collide_unit_load, pool_occupation, excluded_servers))) {
    LOG_WARN("fail to get excluded servers", K(ret));
  } else {
    do_amend = false;
    ObArray<UnitMigrateStat> task_array;
    for (int64_t j = server_load_ptrs_sorted.count() - 1; OB_SUCC(ret) && j >= 0; --j) {
      LoadSum this_load;
      ServerTotalLoad *server_load = server_load_ptrs_sorted.at(j);
      bool enough = false;
      if (OB_UNLIKELY(NULL == server_load)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server load ptr is null", K(ret));
      } else if (server_load->wild_server_) {
      } else if (server_load->server_ == collide_unit_load.unit_->server_) {
      } else if (has_exist_in_array(excluded_servers, server_load->server_)) {
      } else if (OB_FAIL(this_load.append_load(collide_unit_load))) {
        LOG_WARN("fail to append load", K(ret));
      } else if (OB_FAIL(check_single_server_resource_enough(
              this_load, unit_stat, *server_load, enough))) {
        LOG_WARN("fail to check server resource", K(ret));
      } else if (!enough) {
      } else {
        const common::ObAddr &server = server_load->server_;
        UnitMigrateStat unit_migrate_stat;
        unit_migrate_stat.original_pos_ = collide_unit_load.unit_->server_;
        unit_migrate_stat.arranged_pos_ = server;
        unit_migrate_stat.unit_load_ = collide_unit_load;
        if (OB_FAIL(do_update_src_server_load(
                collide_unit_load, *server_load_to_vacate, unit_stat))) {
          LOG_WARN("fail to update src server load", K(ret));
        } else if (OB_FAIL(do_update_dst_server_load(
                collide_unit_load, *server_load, unit_stat, pool_occupation))) {
          LOG_WARN("fail to do update dst server load", K(ret));
        } else if (OB_FAIL(task_array.push_back(unit_migrate_stat))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          ServerTotalLoadCmp cmp;
          std::sort(server_load_ptrs_sorted.begin(), server_load_ptrs_sorted.end(), cmp);
          if (OB_FAIL(cmp.get_ret())) {
            LOG_WARN("fail to sort server loads", K(ret));
          }
          break;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (task_array.count() <= 0) {
    } else if (OB_FAIL(check_and_do_migrate_unit_task_(task_array))) {
      LOG_WARN("fail to check and do migrate task", KR(ret), K(task_array));
    } else {
      do_amend = true;
    }
  }
  return ret;
}

int ObServerBalancer::pick_vacate_server_load(
    common::ObIArray<ServerTotalLoad> &server_loads,
    common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted,
    UnitMigrateStat &unit_migrate_stat,
    ServerTotalLoad *&vacate_server_load)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    vacate_server_load = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      ServerTotalLoad &server_load = server_loads.at(i);
      if (server_load.server_ == unit_migrate_stat.arranged_pos_ && !server_load.wild_server_) {
        vacate_server_load = &server_load;
        break;
      } else {} // go on next
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      ServerTotalLoad &server_load = server_loads.at(i);
      if (OB_FAIL(server_load_ptrs_sorted.push_back(&server_load))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ServerTotalLoadCmp cmp;
      if (FALSE_IT(std::sort(server_load_ptrs_sorted.begin(),
                             server_load_ptrs_sorted.end(),
                             cmp))) {
      } else if (OB_FAIL(cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::do_vacate_space_for_ttg_balance(
    UnitMigrateStat &unit_migrate_stat,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
    const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
    common::ObArray<ServerTotalLoad> &server_loads,
    bool &do_amend)
{
  int ret = OB_SUCCESS;
  double hard_limit = 0.0;
  ServerTotalLoad *server_load_to_vacate = NULL;
  common::ObArray<ServerTotalLoad *> server_load_ptrs_sorted;
  ObUnitStat unit_stat;
  ObUnitManager::ObUnitLoad &unit_load_to_vacate = unit_migrate_stat.unit_load_;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!unit_load_to_vacate.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit_load_to_vacate));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
  } else if (OB_FAIL(unit_mgr_->get_hard_limit(hard_limit))) {
    LOG_WARN("fail to get hard limit", K(ret));
  } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
          unit_load_to_vacate.unit_->unit_id_, unit_load_to_vacate.unit_->zone_, unit_stat))) {
    LOG_WARN("fail to get unit stat", K(ret), "unit", *unit_load_to_vacate.unit_);
  } else if (OB_FAIL(pick_vacate_server_load(
          server_loads, server_load_ptrs_sorted, unit_migrate_stat, server_load_to_vacate))) {
    LOG_WARN("fail to pick vacate server load", K(ret));
  } else if (OB_UNLIKELY(NULL == server_load_to_vacate)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vacate server load ptr is null", K(ret));
  } else {
    ObArray<UnitMigrateStat> task_array;
    bool vacate_enough = false;
    LoadSum load_to_vacate;
    if (OB_FAIL(load_to_vacate.append_load(unit_migrate_stat.unit_load_))) {
      LOG_WARN("fail to append load", K(ret));
    } else if (OB_FAIL(vacate_space_by_unit_array(
            load_to_vacate, unit_load_to_vacate, *server_load_to_vacate,
            task_array, not_grant_units, server_load_ptrs_sorted))) {
      LOG_WARN("fail to vacate space by not grant units", K(ret));
    } else if (OB_FAIL(check_single_server_resource_enough(
            load_to_vacate, unit_stat, *server_load_to_vacate, vacate_enough))) {
      LOG_WARN("fail to check single server resource enough", K(ret));
    } else if (vacate_enough) {
      // good, enough
    } else if (OB_FAIL(vacate_space_by_unit_array(
            load_to_vacate, unit_load_to_vacate, *server_load_to_vacate,
            task_array, standalone_units, server_load_ptrs_sorted))) {
      LOG_WARN("fail to vacate space by not grant units", K(ret));
    } else if (OB_FAIL(check_single_server_resource_enough(
            load_to_vacate, unit_stat, *server_load_to_vacate, vacate_enough))) {
      LOG_WARN("fail to check single server resource enough", K(ret));
    } else if (vacate_enough) {
      // good, enough
    } else if (OB_FAIL(vacate_space_by_tenantgroup(
            load_to_vacate, unit_load_to_vacate, *server_load_to_vacate,
            task_array, unstable_tenant_group, server_load_ptrs_sorted))) {
      LOG_WARN("fail to vacate space by tenantgroup", K(ret));
    } else if (OB_FAIL(check_single_server_resource_enough(
            load_to_vacate, unit_stat, *server_load_to_vacate, vacate_enough))) {
      LOG_WARN("fail to check single server resource enough", K(ret));
    } else if (vacate_enough) {
      // good, enough
    } else {
      do_amend = false; // resource not enough
    }
    if (OB_SUCC(ret) && vacate_enough) {
      if (OB_FAIL(check_and_do_migrate_unit_task_(task_array))) {
        LOG_WARN("fail to check and do migrate task", KR(ret), K(task_array));
      } else {
        do_amend = true;
      }
    }
  }
  return ret;
}

int ObServerBalancer::generate_pool_occupation_array(
    const common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads,
    common::ObIArray<PoolOccupation> &pool_occupation)
{
  int ret = OB_SUCCESS;
  pool_occupation.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < unit_loads.count(); ++i) {
    const ObUnitManager::ObUnitLoad &unit_load = unit_loads.at(i);
    if (OB_UNLIKELY(!unit_load.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit load unexpected", K(ret), K(unit_load));
    } else if (OB_FAIL(pool_occupation.push_back(
            PoolOccupation(unit_load.pool_->tenant_id_,
                           unit_load.pool_->resource_pool_id_,
                           unit_load.unit_->server_)))) {
      LOG_WARN("fail to push back", K(ret));
    } else {}
  }
  return ret;
}

int ObServerBalancer::vacate_space_by_tenantgroup(
    LoadSum &load_to_vacate,
    const ObUnitManager::ObUnitLoad &unit_load_to_vacate,
    ServerTotalLoad &server_load_to_vacate,
    common::ObIArray<UnitMigrateStat> &task_array,
    const common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group,
    common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted)
{
  int ret = OB_SUCCESS;
  ObUnitStat unit_stat;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!unit_load_to_vacate.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit_load_to_vacate));
  } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
          unit_load_to_vacate.unit_->unit_id_, unit_load_to_vacate.unit_->zone_, unit_stat))) {
    LOG_WARN("fail to get unit stat", K(ret), "unit", *unit_load_to_vacate.unit_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unstable_tenant_group.count(); ++i) {
      bool vacate_enough = false;
      common::ObArray<ObUnitManager::ObUnitLoad> unit_loads;
      TenantGroupBalanceInfo *ptr = unstable_tenant_group.at(i);
      if (OB_UNLIKELY(NULL == ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ptr is null", K(ret));
      } else if (OB_FAIL(ptr->get_all_unit_loads(unit_loads))) {
        LOG_WARN("fail to get all unit loads", K(ret));
      } else if (OB_FAIL(vacate_space_by_unit_array(
              load_to_vacate, unit_load_to_vacate, server_load_to_vacate,
              task_array, unit_loads, server_load_ptrs_sorted))) {
        LOG_WARN("fail to vacate space by unit array", K(ret));
      } else if (OB_FAIL(check_single_server_resource_enough(
              load_to_vacate, unit_stat, server_load_to_vacate, vacate_enough))) {
        LOG_WARN("fail to check single server resource enough", K(ret));
      } else if (vacate_enough) {
        break;
      } else {} // go on to the next
    }
  }
  return ret;
}

int ObServerBalancer::vacate_space_by_unit_array(
    LoadSum &load_to_vacate,
    const ObUnitManager::ObUnitLoad &unit_load_to_vacate,
    ServerTotalLoad &server_load_to_vacate,
    common::ObIArray<UnitMigrateStat> &task_array,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &units,
    common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted)
{
  int ret = OB_SUCCESS;
  ObUnitStat unit_stat;
  ObArray<PoolOccupation> pool_occupation;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!unit_load_to_vacate.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid unit load", K(ret), K(unit_load_to_vacate));
  } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
          unit_load_to_vacate.unit_->unit_id_, unit_load_to_vacate.unit_->zone_, unit_stat))) {
    LOG_WARN("fail to get unit stat", K(ret), "unit", *unit_load_to_vacate.unit_);
  } else if (OB_FAIL(generate_pool_occupation_array(units, pool_occupation))) {
    LOG_WARN("fail to generate pool occupation array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
      bool vacate_enough = false;
      const ObUnitManager::ObUnitLoad &unit_load = units.at(i);
      if (OB_UNLIKELY(!unit_load.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit load is invalid", K(ret), K(unit_load));
      } else if (unit_load.unit_->server_ != server_load_to_vacate.server_) {
        // unit is not belong to vacate, skip
      } else if (OB_FAIL(vacate_space_by_single_unit(
              server_load_to_vacate, unit_load,
              task_array, pool_occupation, server_load_ptrs_sorted))) {
        LOG_WARN("fail to vacate space by single unit", K(ret));
      } else if (OB_FAIL(check_single_server_resource_enough(
            load_to_vacate, unit_stat, server_load_to_vacate, vacate_enough))) {
        LOG_WARN("fail to check single server resource enough", K(ret));
      } else if (vacate_enough) {
        break;  // good, enough
      } else {} // go on
    }
  }
  return ret;
}

int ObServerBalancer::check_single_server_resource_enough(
    const LoadSum &this_load,
    const ObUnitStat &unit_stat,
    const ServerTotalLoad &server_load,
    bool &enough,
    const bool mind_disk_waterlevel)
{
  int ret = OB_SUCCESS;
  ServerDiskStatistic disk_statistic;
  double hard_limit = 0.0;
  double disk_waterlevel = 0.0;
  ObUnitStat my_unit_stat = unit_stat;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
  } else if (OB_FAIL(unit_mgr_->get_hard_limit(hard_limit))) {
    LOG_WARN("fail to get hard limit", K(ret));
  } else if (OB_FAIL(zone_disk_statistic_.get_server_disk_statistic(
          server_load.server_, disk_statistic))) {
    LOG_WARN("fail to get disk statistic", K(ret), "server", server_load.server_);
  } else if (mind_disk_waterlevel) {
    if (OB_FAIL(get_server_balance_critical_disk_waterlevel(disk_waterlevel))) {
      LOG_WARN("fail to get server balance critical disk waterlevle", K(ret));
    }
  } else {
    disk_waterlevel = 1.0;
  }
  if (OB_SUCC(ret)) {
    ObUnit *unit = NULL;
    if (OB_FAIL(unit_mgr_->get_unit_by_id(unit_stat.get_unit_id(), unit))) {
      LOG_WARN("fail to get unit by id", K(ret), "unit_id", unit_stat.get_unit_id());
    } else if (OB_UNLIKELY(NULL == unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit ptr is null", K(ret));
    } else if (common::REPLICA_TYPE_LOGONLY == unit->replica_type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Replica type LOGONLY is unexpected", KR(ret), KP(unit));
    }
  }
  if (OB_SUCC(ret)) {
    enough = ((this_load.load_sum_.max_cpu() + server_load.load_sum_.load_sum_.max_cpu()
                <= server_load.resource_info_.cpu_ * hard_limit)
              && (this_load.load_sum_.min_cpu() + server_load.load_sum_.load_sum_.min_cpu()
                <= server_load.resource_info_.cpu_)
              && (static_cast<double>(this_load.load_sum_.memory_size())
                  + static_cast<double>(server_load.load_sum_.load_sum_.memory_size())
                <= static_cast<double>(server_load.resource_info_.mem_total_))
              && (static_cast<double>(this_load.load_sum_.log_disk_size())
                  + static_cast<double>(server_load.load_sum_.load_sum_.log_disk_size())
                <= static_cast<double>(server_load.resource_info_.log_disk_total_))
              && (static_cast<double>(my_unit_stat.get_required_size() + disk_statistic.disk_in_use_)
                <= static_cast<double>(disk_statistic.disk_total_) * disk_waterlevel));
  }
  return ret;
}

int ObServerBalancer::vacate_space_by_single_unit(
    ServerTotalLoad &server_load_to_vacate,
    const ObUnitManager::ObUnitLoad &unit_load,
    common::ObIArray<UnitMigrateStat> &task_array,
    common::ObIArray<PoolOccupation> &pool_occupation,
    common::ObArray<ServerTotalLoad *> &server_load_ptrs_sorted)
{
  int ret = OB_SUCCESS;
  ObUnitStat unit_stat;
  ObArray<common::ObAddr> excluded_servers;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!unit_load.is_valid()
        || server_load_to_vacate.server_ != unit_load.unit_->server_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "unit_server", unit_load.unit_->server_,
             "server", server_load_to_vacate.server_);
  } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
          unit_load.unit_->unit_id_, unit_load.unit_->zone_, unit_stat))) {
    LOG_WARN("fail to get unit stat", K(ret), "unit", *unit_load.unit_);
  } else if (OB_FAIL(get_pool_occupation_excluded_dst_servers(
          unit_load, pool_occupation, excluded_servers))) {
    LOG_WARN("fail to get excluded servers", K(ret));
  } else {
    for (int64_t j = server_load_ptrs_sorted.count() - 1; OB_SUCC(ret) && j >= 0; --j) {
      LoadSum this_load;
      bool enough = true;
      ServerTotalLoad *server_load = server_load_ptrs_sorted.at(j);
      if (OB_UNLIKELY(NULL == server_load)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server load ptr is null", K(ret));
      } else if (server_load->wild_server_) {
      } else if (server_load->server_ == server_load_to_vacate.server_) {
      } else if (has_exist_in_array(excluded_servers, server_load->server_)) {
      } else if (OB_FAIL(this_load.append_load(unit_load))) {
        LOG_WARN("fail to append load", K(ret));
      } else if (OB_FAIL(check_single_server_resource_enough(
              this_load, unit_stat, *server_load_ptrs_sorted.at(j), enough))) {
        LOG_WARN("fail to check server resource", K(ret));
      } else if (!enough) {
      } else {
        const common::ObAddr &server = server_load->server_;
        UnitMigrateStat unit_migrate_stat;
        unit_migrate_stat.original_pos_ = unit_load.unit_->server_;
        unit_migrate_stat.arranged_pos_ = server;
        unit_migrate_stat.unit_load_ = unit_load;
        if (OB_FAIL(do_update_src_server_load(unit_load, server_load_to_vacate, unit_stat))) {
          LOG_WARN("fail to update src server load", K(ret));
        } else if (OB_FAIL(do_update_dst_server_load(
                unit_load, *server_load, unit_stat, pool_occupation))) {
          LOG_WARN("fail to do update dst server load", K(ret));
        } else if (OB_FAIL(task_array.push_back(unit_migrate_stat))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          ServerTotalLoadCmp cmp;
          std::sort(server_load_ptrs_sorted.begin(), server_load_ptrs_sorted.end(), cmp);
          if (OB_FAIL(cmp.get_ret())) {
            LOG_WARN("fail to sort server loads", K(ret));
          }
          break;
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::get_pool_occupation_excluded_dst_servers(
    const ObUnitManager::ObUnitLoad &this_load,
    const common::ObIArray<PoolOccupation> &pool_occupation,
    common::ObIArray<common::ObAddr> &excluded_servers)
{
  int ret = OB_SUCCESS;
  excluded_servers.reset();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!this_load.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(this_load));
  } else {
    // not grant to any tenant
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_occupation.count(); ++i) {
      const PoolOccupation &po = pool_occupation.at(i);
      if (common::OB_INVALID_ID == this_load.pool_->tenant_id_
          && this_load.pool_->resource_pool_id_ != po.resource_pool_id_) {
        // not grant to any tenant, compare resource pool id
      } else if (common::OB_INVALID_ID != this_load.pool_->tenant_id_
          && this_load.pool_->tenant_id_ != po.tenant_id_) {
        // already grant to any tenant, compare tenant id
      } else if (OB_FAIL(excluded_servers.push_back(po.server_))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObServerBalancer::generate_complete_server_loads(
    const common::ObZone &zone,
    const common::ObIArray<common::ObAddr> &available_servers,
    double *const resource_weights,
    const int64_t weights_count,
    common::ObArray<ServerTotalLoad> &server_loads)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> zone_servers;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == resource_weights
                         || RES_MAX != weights_count
                         || zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(resource_weights), K(weights_count));
  } else if (OB_ISNULL(unit_mgr_) || OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ or server_mgr_ is null", K(ret), KP(unit_mgr_), KP(server_mgr_));
  } else if (OB_FAIL(SVR_TRACER.get_servers_of_zone(zone, zone_servers))) {
    LOG_WARN("fail to get servers of zone", K(ret), K(zone));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_servers.count(); ++i) {
      const common::ObAddr &server = zone_servers.at(i);
      ServerTotalLoad server_load;
      server_load.server_ = server;
      share::ObServerResourceInfo server_resource_info;
      ObArray<ObUnitManager::ObUnitLoad> *unit_loads = NULL;
      LoadSum load_sum;
      server_load.wild_server_ = !has_exist_in_array(available_servers, server);
      if (OB_FAIL(server_mgr_->get_server_resource_info(server, server_resource_info))) {
        LOG_WARN("fail to get server status", K(ret), K(server));
      } else if (OB_FAIL(unit_mgr_->get_loads_by_server(server, unit_loads))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("get loads by server failed", K(ret), K(server));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_UNLIKELY(NULL == unit_loads)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit_loads is null", K(ret), KP(unit_loads));
      } else if (OB_FAIL(load_sum.append_load(*unit_loads))) {
        LOG_WARN("fail to append load", K(ret));
      }
      if (OB_SUCC(ret)) {
        for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
          server_load.resource_weights_[i] = resource_weights[i];
        }
        server_load.load_sum_ = load_sum;
        server_load.resource_info_ = server_resource_info;
        if (OB_FAIL(server_load.update_load_value())) {
          LOG_WARN("fail to update load value", K(ret));
        } else if (OB_FAIL(server_loads.push_back(server_load))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
      if (OB_SUCC(ret)) {
        ServerTotalLoadCmp cmp;
        std::sort(server_loads.begin(), server_loads.end(), cmp);
        if (OB_FAIL(cmp.get_ret())) {
          LOG_WARN("fail to sort", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) { // Defensive inspection
      for (int64_t i = 0; OB_SUCC(ret) && i < available_servers.count(); ++i) {
        if (!has_exist_in_array(zone_servers, available_servers.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("available server is not a member of zone servers", K(zone_servers),
                   "available server", available_servers.at(i));
        } else {} // good, in zone servers array
      }
    }
  }
  return ret;
}

int ObServerBalancer::divide_complete_server_loads_for_balance(
    const common::ObIArray<common::ObAddr> &available_servers,
    common::ObIArray<ServerTotalLoad> &server_loads,
    common::ObIArray<ServerTotalLoad *> &wild_server_loads,
    common::ObIArray<ServerTotalLoad *> &over_server_loads,
    common::ObIArray<ServerTotalLoad *> &under_server_loads,
    double &upper_lmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(server_loads.count() <= 0
                         || available_servers.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "server_load_count", server_loads.count(),
             "available_servers count", available_servers.count());
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret));
  } else {
    wild_server_loads.reset();
    over_server_loads.reset();
    under_server_loads.reset();
    double sum_load = 0.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      const ServerTotalLoad &server_load = server_loads.at(i);
      sum_load += server_load.inter_ttg_load_value_;
    }
    const int64_t cpu_mem_tolerance = GCONF.server_balance_cpu_mem_tolerance_percent;
    const double delta_percent = static_cast<double>(cpu_mem_tolerance) / static_cast<double>(100);
    const double average_load = sum_load / static_cast<double>(available_servers.count());
    upper_lmt = average_load + delta_percent * average_load;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      ServerTotalLoad &server_load = server_loads.at(i);
      if (server_load.wild_server_) {
        ObArray<ObUnitManager::ObUnitLoad> *loads = NULL;
        if (OB_FAIL(unit_mgr_->get_loads_by_server(server_load.server_, loads))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS; // good, has no load, no need to put into array
          } else {
            LOG_WARN("fail to get load by server", K(ret), "server", server_load.server_);
          }
        } else if (OB_UNLIKELY(NULL == loads)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("loads ptr is null", K(ret));
        } else if (OB_FAIL(wild_server_loads.push_back(&server_load))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else if (server_load.inter_ttg_load_value_ >= upper_lmt) {
        if (OB_FAIL(over_server_loads.push_back(&server_load))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        if (OB_FAIL(under_server_loads.push_back(&server_load))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }

    LOG_INFO("divide complete server loads for balance", KR(ret), K(server_loads),
        K(available_servers),
        K(wild_server_loads), K(over_server_loads), K(under_server_loads));
  }
  return ret;
}

int ObServerBalancer::generate_available_server_loads(
    ObIArray<ServerTotalLoad *> &over_server_loads,
    ObIArray<ServerTotalLoad *> &under_server_loads,
    ObIArray<ServerTotalLoad *> &available_server_loads)
{
  int ret = OB_SUCCESS;
  available_server_loads.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < over_server_loads.count(); ++i) {
    if (OB_FAIL(available_server_loads.push_back(over_server_loads.at(i)))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < under_server_loads.count(); ++i) {
    if (OB_FAIL(available_server_loads.push_back(under_server_loads.at(i)))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ObServerBalancer::make_single_wild_server_empty_by_units(
    common::ObIArray<PoolOccupation> &pool_occupation,
    ServerTotalLoad &wild_server_load,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &units,
    common::ObArray<ServerTotalLoad *> &available_server_loads,
    common::ObIArray<UnitMigrateStat> &task_array)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
    bool do_balance = false;
    ObArray<common::ObAddr> excluded_servers;
    LoadSum this_load;
    ServerTotalLoadCmp cmp;
    const ObUnitManager::ObUnitLoad &unit_load = units.at(i);
    if (OB_UNLIKELY(!unit_load.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit load is invalid", K(ret), K(unit_load));
    } else if (wild_server_load.server_ != unit_load.unit_->server_) {
      // not in this wild server, ignore
    } else if (OB_FAIL(get_pool_occupation_excluded_dst_servers(
            unit_load, pool_occupation, excluded_servers))) {
      LOG_WARN("fail to get excluded servers", K(ret));
    } else if (FALSE_IT(std::sort(available_server_loads.begin(),
                                  available_server_loads.end(),
                                  cmp))) {
      // sort cannot fail
    } else if (OB_FAIL(cmp.get_ret())) {
      LOG_WARN("fail to sort", K(ret));
    } else if (OB_FAIL(try_balance_single_unit_by_cm(
          unit_load, available_server_loads, task_array,
          excluded_servers, pool_occupation, do_balance))) {
      LOG_WARN("fail to try balance single unit by cmp and memory", K(ret));
    } else if (do_balance) {
      // do execute balance according to cpu and memory, no more to do
    } else if (OB_FAIL(try_balance_single_unit_by_disk(
            unit_load, available_server_loads, task_array,
            excluded_servers, pool_occupation, do_balance))) {
      LOG_WARN("fail to do balance single unit by disk", K(ret));
    } else if (do_balance) {
      // good
    } else {
      ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
      LOG_WARN("no available server to hold more unit", K(ret),
               "unit", *unit_load.unit_,
               "unit_config", *unit_load.unit_config_,
               "pool", *unit_load.pool_);
    }
  }
  return ret;
}

int ObServerBalancer::make_wild_servers_empty(
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
    common::ObIArray<ServerTotalLoad *> &wild_server_loads,
    common::ObIArray<ServerTotalLoad *> &over_server_loads,
    common::ObIArray<ServerTotalLoad *> &under_server_loads)
{
  int ret = OB_SUCCESS;
  ObArray<ServerTotalLoad *> available_server_loads;
  common::ObArray<UnitMigrateStat> task_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(generate_available_server_loads(
          over_server_loads, under_server_loads, available_server_loads))) {
    LOG_WARN("fail to generate available server loads", K(ret));
  } else if (OB_FAIL(make_wild_servers_empty_by_units(
          wild_server_loads, not_grant_units, available_server_loads, task_array))) {
    LOG_WARN("fail to make wild servers empty by units", K(ret));
  } else if (OB_FAIL(make_wild_servers_empty_by_units(
          wild_server_loads, standalone_units, available_server_loads, task_array))) {
    LOG_WARN("fail to make wild servers empty by units", K(ret));
  } else if (task_array.count() <= 0) {
    // no task generate
  } else if (OB_FAIL(check_and_do_migrate_unit_task_(task_array))) {
    LOG_WARN("fail to check and do migrate unit task", KR(ret), K(task_array));
  } else {} // no more to do
  return ret;
}

int ObServerBalancer::make_wild_servers_empty_by_units(
    common::ObIArray<ServerTotalLoad *> &wild_server_loads,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &units,
    common::ObArray<ServerTotalLoad *> &available_server_loads,
    common::ObIArray<UnitMigrateStat> &task_array)
{
  int ret = OB_SUCCESS;
  // pool_occupation is a placeholder for unit on the server
  common::ObArray<PoolOccupation> pool_occupation;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(generate_pool_occupation_array(units, pool_occupation))) {
    LOG_WARN("fail to generate pool occupation array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < wild_server_loads.count(); ++i) {
      ServerTotalLoad *wild_server_load = wild_server_loads.at(i);
      if (OB_UNLIKELY(NULL == wild_server_load)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wild server load ptr is null", K(ret), KP(wild_server_load));
      } else if (OB_FAIL(make_single_wild_server_empty_by_units(
              pool_occupation, *wild_server_load, units, available_server_loads, task_array))) {
        LOG_WARN("fail to make single wild server empty by units", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObServerBalancer::get_disk_over_available_servers(
    common::ObIArray<common::ObAddr> &disk_over_servers)
{
  int ret = OB_SUCCESS;
  double disk_waterlevel = 0.0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_server_balance_critical_disk_waterlevel(disk_waterlevel))) {
    LOG_WARN("fail to get critical disk waterlevel", K(ret));
  } else {
    disk_over_servers.reset();
    for (int64_t i = 0;
         OB_SUCC(ret) && i < zone_disk_statistic_.server_disk_statistic_array_.count(); ++i) {
      ServerDiskStatistic &disk_statistic
        = zone_disk_statistic_.server_disk_statistic_array_.at(i);
      if (static_cast<double>(disk_statistic.disk_in_use_)
                 > static_cast<double>(disk_statistic.disk_total_) * disk_waterlevel) {
        if (OB_FAIL(disk_over_servers.push_back(disk_statistic.server_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {} // still ok
    }
  }
  return ret;
}

int ObServerBalancer::make_non_ttg_balance_under_load_by_disk(
    common::ObIArray<PoolOccupation> &pool_occupation,
    common::ObIArray<UnitMigrateStat> &task_array,
    const ObUnitManager::ObUnitLoad &unit_load,
    ServerTotalLoad *src_server_load,
    const common::ObIArray<common::ObAddr> &disk_over_servers,
    common::ObIArray<ServerTotalLoad *> &available_server_loads)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> excluded_servers;
  double disk_waterlevel = 0.0;
  bool is_disk_over_waterlevel = true;
  ObUnitStat unit_stat;
  LoadSum this_load;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == src_server_load || !unit_load.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(src_server_load), K(unit_load));
  } else if (OB_FAIL(get_server_balance_critical_disk_waterlevel(disk_waterlevel))) {
    LOG_WARN("fail to get server balance critical disk waterlevel", K(ret));
  } else if (OB_FAIL(zone_disk_statistic_.check_server_over_disk_waterlevel(
          src_server_load->server_, disk_waterlevel, is_disk_over_waterlevel))) {
    LOG_WARN("fail to check disk over waterlevel", K(ret));
  } else if (!is_disk_over_waterlevel) {
    // do_nothing, it is already below the water mark, no need to migrate out unit
  } else if (OB_FAIL(get_pool_occupation_excluded_dst_servers(
          unit_load, pool_occupation, excluded_servers))) {
    LOG_WARN("fail to get excluded servers", K(ret));
  } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
          unit_load.unit_->unit_id_, unit_load.unit_->zone_, unit_stat))) {
    LOG_WARN("fail to get unit stat", K(ret), "unit", *unit_load.unit_);
  } else if (0 == unit_stat.get_required_size()) {
    // do nothing, skip unit with 0 data disk usage
  } else if (OB_FAIL(this_load.append_load(unit_load))) {
    LOG_WARN("fail to append load", K(ret));
  } else {
    for (int64_t i = 0;
         OB_SUCC(ret) && i < zone_disk_statistic_.server_disk_statistic_array_.count();
         ++i) {
      ServerDiskStatistic &disk_statistic = zone_disk_statistic_.server_disk_statistic_array_.at(i);
      bool enough = true;
      UnitMigrateStat unit_migrate;
      unit_migrate.unit_load_ = unit_load;
      unit_migrate.original_pos_ = src_server_load->server_;
      unit_migrate.arranged_pos_ = disk_statistic.server_;
      ServerTotalLoad *dst_server_load = NULL;
      if (disk_statistic.wild_server_) {
        // skip, wild server
      } else if (has_exist_in_array(excluded_servers, disk_statistic.server_)) {
        // skip, dst in excluded servers
      } else if (has_exist_in_array(disk_over_servers, disk_statistic.server_)) {
        // skip, dst in disk over servers
      } else if (OB_FAIL(pick_server_load(
              disk_statistic.server_, available_server_loads, dst_server_load))) {
        LOG_WARN("fail to pick server load", K(ret));
      } else if (OB_UNLIKELY(NULL == dst_server_load)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dst server load ptr is null", K(ret), KP(dst_server_load));
      } else if (OB_FAIL(check_single_server_resource_enough(
              this_load, unit_stat, *dst_server_load, enough))) {
        LOG_WARN("fail to check server resource enough", K(ret));
      } else if (!enough) {
        // resource not enough
      } else if (OB_FAIL(do_update_src_server_load(unit_load, *src_server_load, unit_stat))) {
        LOG_WARN("fail to do update src server load", K(ret));
      } else if (OB_FAIL(do_update_dst_server_load(
              unit_load, *dst_server_load, unit_stat, pool_occupation))) {
        LOG_WARN("fail to update dst server load", K(ret));
      } else if (OB_FAIL(task_array.push_back(unit_migrate))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        LOG_INFO("unit migration task generated for disk in use over waterlevel", KR(ret), K(unit_stat), K(unit_migrate));
        break;
        // unit migrate success
      }
    }
  }
  return ret;
}

int ObServerBalancer::make_available_servers_balance_by_disk(
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    common::ObIArray<ServerTotalLoad *> &available_server_loads,
    int64_t &task_count)
{
  int ret = OB_SUCCESS;
  ObArray<PoolOccupation> pool_occupation;
  common::ObArray<common::ObAddr> disk_over_servers;
  task_count = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_disk_over_available_servers(disk_over_servers))) {
    LOG_WARN("fail to get disk over servers", K(ret));
  } else if (OB_FAIL(generate_pool_occupation_array(standalone_units, pool_occupation))) {
    LOG_WARN("fail to generate pool occupation array", K(ret));
  } else {
    common::ObArray<const ObUnitManager::ObUnitLoad *> standalone_unit_ptrs;
    for (int64_t i = 0; OB_SUCC(ret) && i < standalone_units.count(); ++i) {
      if (OB_FAIL(standalone_unit_ptrs.push_back(&standalone_units.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      UnitLoadDiskCmp cmp(unit_stat_mgr_);
      std::sort(standalone_unit_ptrs.begin(), standalone_unit_ptrs.end(), cmp);
      if (OB_FAIL(cmp.get_ret())) {
        LOG_WARN("fail to get ret", K(ret));
      }
    }
    common::ObArray<UnitMigrateStat> task_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < disk_over_servers.count(); ++i) {
      common::ObAddr &src_server = disk_over_servers.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < standalone_unit_ptrs.count(); ++j) {
        const ObUnitManager::ObUnitLoad *unit_load = standalone_unit_ptrs.at(j);
        ServerTotalLoad *src_server_load = NULL;
        if (OB_UNLIKELY(NULL == unit_load)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit load ptr is null", K(ret));
        } else if (OB_UNLIKELY(!unit_load->is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit load is invalid", K(ret), K(*unit_load));
        } else if (src_server != unit_load->unit_->server_) {
          // no this server
        } else if (common::REPLICA_TYPE_LOGONLY == unit_load->unit_->replica_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Replica type LOGONLY is unexpected", KR(ret), KP(unit_load));
        } else if (OB_FAIL(pick_server_load(
                src_server, available_server_loads, src_server_load))) {
          LOG_WARN("fail to pick server load", K(ret));
        } else if (NULL == src_server_load) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dst server load ptr is null", K(ret));
        } else if (OB_FAIL(make_non_ttg_balance_under_load_by_disk(
                pool_occupation, task_array, *unit_load,
                src_server_load, disk_over_servers, available_server_loads))) {
          LOG_WARN("fail to make non ttg balance under load by disk", K(ret));
        } else {} // no more to do
      }
    }

    if (OB_SUCC(ret) && task_array.count() > 0) {
      task_count = task_array.count();
      if (OB_FAIL(check_and_do_migrate_unit_task_(task_array))) {
        LOG_WARN("fail to check and do migrate task", KR(ret), K(task_array));
      }
    }
  }
  return ret;
}

// DO NOT migrate unit for CREATING tenant
int ObServerBalancer::check_and_do_migrate_unit_task_(
    const common::ObIArray<UnitMigrateStat> &task_array)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  ObArray<UnitMigrateStat> new_task_array;

  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr is null", KR(ret), KP(schema_service_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get sys schema guard", KR(ret));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < task_array.count(); idx++) {
      const UnitMigrateStat &task = task_array.at(idx);
      const share::schema::ObSimpleTenantSchema *tenant_schema = NULL;
      bool can_do_unit_migrate = true;
      uint64_t tenant_id = task.unit_load_.get_tenant_id();

      if (OB_INVALID_ID == tenant_id || OB_INVALID_TENANT_ID == tenant_id) {
        LOG_INFO("[SERVER_BALANCE] [CHECK_CAN_DO] not-grant unit can always migrate", K(idx),
            K(task), K(lbt()));
        can_do_unit_migrate = true;
      } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        // tenant not exist, ignore this unit
        LOG_WARN("[SERVER_BALANCE] [CHECK_CAN_DO] tenant not exist, ignore this unit migrate task",
            K(idx), K(task));
        can_do_unit_migrate = false;
      } else if (tenant_schema->is_creating()) {
        LOG_INFO("[SERVER_BALANCE] [CHECK_CAN_DO] unit of creating tenant can not do migrate",
            K(idx), K(task), K(lbt()));
        can_do_unit_migrate = false;
      } else {
        LOG_INFO("[SERVER_BALANCE] [CHECK_CAN_DO] unit can do migrate", K(idx), K(task), K(lbt()));
        // other tenant unit can do migrate
        can_do_unit_migrate = true;
      }

      if (OB_SUCC(ret) && can_do_unit_migrate && OB_FAIL(new_task_array.push_back(task))) {
        LOG_WARN("push task into array fail", KR(ret), K(task));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (new_task_array.count() > 0 && OB_FAIL(do_migrate_unit_task(new_task_array))) {
      LOG_WARN("do migrate unit task fail", KR(ret), K(new_task_array), K(task_array));
    }
  }
  return ret;
}

int ObServerBalancer::make_available_servers_balance_by_cm(
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
    common::ObIArray<ServerTotalLoad *> &over_server_loads,
    common::ObIArray<ServerTotalLoad *> &under_server_loads,
    const double upper_lmt,
    double *const g_res_weights,
    const int64_t weights_count,
    int64_t &task_count)
{
  int ret = OB_SUCCESS;
  task_count = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    common::ObArray<UnitMigrateStat> task_array;
    if (OB_FAIL(do_non_ttg_unit_balance_by_cm(
            task_array, not_grant_units, over_server_loads, under_server_loads,
            upper_lmt, g_res_weights, weights_count))) {
      LOG_WARN("fail to do non ttg unit balance by units", K(ret));
    } else if (OB_FAIL(do_non_ttg_unit_balance_by_cm(
            task_array, standalone_units, over_server_loads, under_server_loads,
            upper_lmt, g_res_weights, weights_count))) {
      LOG_WARN("fail to do non ttg unit balance by units", K(ret));
    } else if (task_array.count() > 0) {
      task_count = task_array.count();

      if (OB_FAIL(check_and_do_migrate_unit_task_(task_array))) {
        LOG_WARN("fail to check and do migrate task", KR(ret), K(task_array));
      }
    } else {} // no more to do
  }
  return ret;
}

int ObServerBalancer::generate_sort_available_servers_disk_statistic(
    common::ObArray<ServerDiskStatistic> &server_disk_statistic_array,
    common::ObArray<ServerDiskStatistic *> &available_servers_disk_statistic)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    available_servers_disk_statistic.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < server_disk_statistic_array.count(); ++i) {
      ServerDiskStatistic &disk_statistic = server_disk_statistic_array.at(i);
      if (disk_statistic.wild_server_) {
        // a wild server, bypass
      } else if (OB_FAIL(available_servers_disk_statistic.push_back(&disk_statistic))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
    if (OB_SUCC(ret)) {
      ServerDiskPercentCmp cmp;
      std::sort(available_servers_disk_statistic.begin(),
                available_servers_disk_statistic.end(),
                cmp);
      if (OB_FAIL(cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::divide_available_servers_disk_statistic(
    common::ObIArray<ServerDiskStatistic *> &available_servers_disk_statistic,
    common::ObIArray<ServerDiskStatistic *> &over_disk_statistic,
    common::ObIArray<ServerDiskStatistic *> &under_disk_statistic,
    double &upper_lmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (available_servers_disk_statistic.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(available_servers_disk_statistic));
  } else {
    double total_value = 0.0;
    for (int64_t i = 0; OB_SUCC(ret) && i < available_servers_disk_statistic.count(); ++i) {
      ServerDiskStatistic *disk_statistic = available_servers_disk_statistic.at(i);
      if (OB_UNLIKELY(NULL == disk_statistic)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("disk statistic ptr is null", K(ret));
      } else {
        total_value += disk_statistic->get_disk_used_percent();
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t disk_tolerance = GCONF.server_balance_disk_tolerance_percent;
      const double delta = static_cast<double>(disk_tolerance) / static_cast<double>(100);
      const double average_value
          = total_value / static_cast<double>(available_servers_disk_statistic.count());
      upper_lmt = average_value + delta;
      for (int64_t i = 0; OB_SUCC(ret) && i < available_servers_disk_statistic.count(); ++i) {
        ServerDiskStatistic *disk_statistic = available_servers_disk_statistic.at(i);
        if (OB_UNLIKELY(NULL == disk_statistic)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("disk statistic ptr is null", K(ret));
        } else if (disk_statistic->get_disk_used_percent() >= upper_lmt) {
          if (OB_FAIL(over_disk_statistic.push_back(disk_statistic))) {
            LOG_WARN("fail to push back", K(ret));
          }
        } else {
          if (OB_FAIL(under_disk_statistic.push_back(disk_statistic))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::make_server_disk_underload_by_unit(
    ServerTotalLoad *src_server_load,
    const ObUnitManager::ObUnitLoad &unit_load,
    const double upper_lmt,
    common::ObIArray<PoolOccupation> &pool_occupation,
    common::ObIArray<ServerTotalLoad *> &available_server_loads,
    common::ObArray<ServerDiskStatistic *> &under_disk_statistic,
    common::ObIArray<UnitMigrateStat> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == src_server_load || !unit_load.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(src_server_load), K(unit_load));
  } else if (src_server_load->server_ != unit_load.unit_->server_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server not match", K(ret),
             "left_server", src_server_load->server_,
             "right_server", unit_load.unit_->server_);
  } else {
    ObArray<common::ObAddr> excluded_servers;
    ObUnitStat unit_stat;
    LoadSum this_load;
    if (OB_FAIL(get_pool_occupation_excluded_dst_servers(
            unit_load, pool_occupation, excluded_servers))) {
      LOG_WARN("fail to get excluded dst servers", K(ret));
    } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
            unit_load.unit_->unit_id_, unit_load.unit_->zone_, unit_stat))) {
      LOG_WARN("fail to get unit stat", K(ret), "unit", *unit_load.unit_);
    } else if (OB_FAIL(this_load.append_load(unit_load))) {
      LOG_WARN("fail to append load", K(ret));
    } else {
      for (int64_t j = under_disk_statistic.count() - 1; OB_SUCC(ret) && j >= 0; --j) {
        ServerTotalLoad *dst_server_load = NULL;
        ServerDiskStatistic *under_server = under_disk_statistic.at(j);
        const bool mind_disk_waterlevel = false;
        bool enough = true;
        if (OB_UNLIKELY(NULL == under_server)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("under server ptr is null", K(ret));
        } else if (has_exist_in_array(excluded_servers, under_server->server_)) {
          // in excluded server
        } else if (OB_FAIL(pick_server_load(
                under_server->server_, available_server_loads, dst_server_load))) {
          LOG_WARN("fail to pick server load", K(ret));
        } else if (NULL == dst_server_load) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dst server load ptr is null", K(ret));
        } else if (OB_FAIL(check_single_server_resource_enough(
                this_load, unit_stat, *dst_server_load, enough, mind_disk_waterlevel))) {
          LOG_WARN("fail to check server resource enough", K(ret));
        } else if (!enough) {
          // resource not enough
        } else if (under_server->get_disk_used_percent_if_add(unit_stat.get_required_size())
                   > upper_lmt) {
          // Exceeds the upper_lmt value
        } else {
          UnitMigrateStat unit_migrate;
          unit_migrate.unit_load_ = unit_load;
          unit_migrate.original_pos_ = src_server_load->server_;
          unit_migrate.arranged_pos_ = dst_server_load->server_;
          ServerDiskPercentCmp cmp;
          if (OB_FAIL(do_update_src_server_load(unit_load, *src_server_load, unit_stat))) {
            LOG_WARN("fail to do update srce server load", K(ret));
          } else if (OB_FAIL(do_update_dst_server_load(
                  unit_load, *dst_server_load, unit_stat, pool_occupation))) {
            LOG_WARN("fail to update dst server load", K(ret));
          } else if (OB_FAIL(task_array.push_back(unit_migrate))) {
            LOG_WARN("fail to push back", K(ret));
          } else if (FALSE_IT(std::sort(under_disk_statistic.begin(),
                                        under_disk_statistic.end(),
                                        cmp))) {
          } else if (OB_FAIL(cmp.get_ret())) {
            LOG_WARN("fail to sort", K(ret));
          } else {
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::make_server_disk_underload(
    ServerDiskStatistic &src_disk_statistic,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const double upper_lmt,
    common::ObIArray<PoolOccupation> &pool_occupation,
    common::ObIArray<ServerTotalLoad *> &available_server_loads,
    common::ObArray<ServerDiskStatistic *> &under_disk_statistic,
    common::ObIArray<UnitMigrateStat> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    common::ObArray<const ObUnitManager::ObUnitLoad *> standalone_unit_ptrs;
    for (int64_t i = 0; OB_SUCC(ret) && i < standalone_units.count(); ++i) {
      if (OB_FAIL(standalone_unit_ptrs.push_back(&standalone_units.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      UnitLoadDiskCmp cmp(unit_stat_mgr_);
      std::sort(standalone_unit_ptrs.begin(), standalone_unit_ptrs.end(), cmp);
      if (OB_FAIL(cmp.get_ret())) {
        LOG_WARN("fail to get ret", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < standalone_unit_ptrs.count(); ++i) {
      const ObUnitManager::ObUnitLoad *unit_load = standalone_unit_ptrs.at(i);
      ServerTotalLoad *src_server_load = NULL;
      if (src_disk_statistic.get_disk_used_percent() < upper_lmt) {
        break; // src already lower than limit
      } else if (NULL == unit_load) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit load ptr is null", K(ret));
      } else if (OB_FAIL(pick_server_load(
              src_disk_statistic.server_, available_server_loads, src_server_load))) {
        LOG_WARN("fail to pick server load", K(ret));
      } else if (OB_UNLIKELY(NULL == src_server_load)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("src server load ptr is null", K(ret));
      } else if (!unit_load->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit load is invalid", K(ret));
      } else if (unit_load->unit_->server_ != src_disk_statistic.server_) {
        // bypass
      } else if (OB_FAIL(make_server_disk_underload_by_unit(
              src_server_load, *unit_load, upper_lmt, pool_occupation,
              available_server_loads, under_disk_statistic, task_array))) {
        LOG_WARN("fail to make server disk underload by disk", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObServerBalancer::make_available_servers_disk_balance(
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    common::ObIArray<ServerTotalLoad *> &available_server_loads,
    int64_t &task_count)
{
  common::ObArray<ServerDiskStatistic *> available_servers_disk_statistic;
  common::ObArray<ServerDiskStatistic *> over_disk_statistic;
  common::ObArray<ServerDiskStatistic *> under_disk_statistic;
  common::ObArray<PoolOccupation> pool_occupation;
  int ret = OB_SUCCESS;
  double upper_limit = 0.0;
  task_count = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(generate_pool_occupation_array(standalone_units, pool_occupation))) {
    LOG_WARN("fail to generate pool occupation array", K(ret));
  } else if (OB_FAIL(generate_sort_available_servers_disk_statistic(
          zone_disk_statistic_.server_disk_statistic_array_, available_servers_disk_statistic))) {
    LOG_WARN("fail to generate sort available server disk statistic", K(ret));
  } else if (OB_FAIL(divide_available_servers_disk_statistic(
          available_servers_disk_statistic, over_disk_statistic,
          under_disk_statistic, upper_limit))) {
    LOG_WARN("fail to divide available servers disk statistic", K(ret));
  } else {
    common::ObArray<UnitMigrateStat> task_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < over_disk_statistic.count(); ++i) {
      ServerDiskStatistic *src_disk_statistic = over_disk_statistic.at(i);
      if (OB_UNLIKELY(NULL == src_disk_statistic)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("disk statistic ptr is null", K(ret));
      } else if (OB_FAIL(make_server_disk_underload(
              *src_disk_statistic, standalone_units, upper_limit, pool_occupation,
              available_server_loads, under_disk_statistic, task_array))) {
        LOG_WARN("fail to make server disk underload", K(ret));
      }
    }
    if (OB_SUCC(ret) && task_array.count() > 0) {
      task_count = task_array.count();
      if (OB_FAIL(check_and_do_migrate_unit_task_(task_array))) {
        LOG_WARN("fail to check and do migrate task", KR(ret), K(task_array));
      }
    }
  }
  return ret;
}

int ObServerBalancer::make_available_servers_balance(
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
    common::ObIArray<ServerTotalLoad *> &over_server_loads,
    common::ObIArray<ServerTotalLoad *> &under_server_loads,
    const double upper_lmt,
    double *const g_res_weights,
    const int64_t weights_count)
{
  int ret = OB_SUCCESS;
  double disk_waterlevel = 0.0;
  bool all_available_servers_disk_over = false;
  int64_t balance_task_count = 0;
  const char *balance_reason = "NONE";
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get_server_balance_critical_disk_waterlevel(disk_waterlevel))) {
    LOG_WARN("fail to get disk waterlevel", K(ret));
  } else if (OB_FAIL(zone_disk_statistic_.check_all_available_servers_over_disk_waterlevel(
          disk_waterlevel, all_available_servers_disk_over))) {
    LOG_WARN("fail to check all available servers over disk waterlevel", K(ret));
  } else if (all_available_servers_disk_over) {
    // The disk usage of all servers exceeds the warning water mark,
    // used a complete disk balancing strategy.
    // balance the disk usage when the cpu and memory can be accommodated.
    ObArray<ServerTotalLoad *> available_server_loads;
    if (OB_FAIL(generate_available_server_loads(
            over_server_loads, under_server_loads, available_server_loads))) {
      LOG_WARN("fail to generate available server loads", K(ret));
    } else if (OB_FAIL(make_available_servers_disk_balance(
            standalone_units, available_server_loads, balance_task_count))) {
      LOG_WARN("fail to make available servers disk balance", K(ret));
    } else {
      balance_reason = "disk_balance_as_all_server_disk_over";
    }
  } else if (zone_disk_statistic_.over_disk_waterlevel()) {
    // No need to deal with not grant, because they do not occupy disk
    ObArray<ServerTotalLoad *> available_server_loads;
    if (OB_FAIL(generate_available_server_loads(
            over_server_loads, under_server_loads, available_server_loads))) {
      LOG_WARN("fail to generate available server loads", K(ret));
    } else if (OB_FAIL(make_available_servers_balance_by_disk(
            standalone_units, available_server_loads, balance_task_count))) {
      LOG_WARN("fail to make available servers balance by disk", K(ret));
    } else {
      balance_reason = "disk_balance_as_over_disk_waterlevel";
    }
  } else {
    if (OB_FAIL(make_available_servers_balance_by_cm(
            standalone_units, not_grant_units, over_server_loads, under_server_loads,
            upper_lmt, g_res_weights, weights_count, balance_task_count))) {
      LOG_WARN("fail to make available servers balance by cpu and memory", K(ret));
    } else {
      balance_reason = "load_balance";
    }
  }

  LOG_INFO("finish zone servers balance", KR(ret), K(disk_waterlevel),
      K(balance_task_count), K(balance_reason),
      K(all_available_servers_disk_over),
      "zone_over_disk_waterlevel", zone_disk_statistic_.over_disk_waterlevel(),
      K(standalone_units.count()), K(not_grant_units.count()));
  return ret;
}

int ObServerBalancer::do_non_tenantgroup_unit_balance_task(
    const common::ObZone &zone,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &standalone_units,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &not_grant_units,
    const common::ObIArray<common::ObAddr> &available_servers)
{
  int ret = OB_SUCCESS;
  double g_res_weights[RES_MAX];
  ObArray<ServerTotalLoad> server_loads;
  common::ObArray<ServerTotalLoad *> wild_server_loads;
  common::ObArray<ServerTotalLoad *> over_server_loads;
  common::ObArray<ServerTotalLoad *> under_server_loads;
  double upper_lmt = 0.0;

  LOG_INFO("start do non-tenantgroup unit balance task", K(zone));

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(available_servers.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(calc_global_balance_resource_weights(
         zone, available_servers, g_res_weights, RES_MAX))) {
    LOG_WARN("fail to calc whole balance resource weights", K(ret));
  } else if (OB_FAIL(generate_complete_server_loads(
          zone, available_servers, g_res_weights, RES_MAX, server_loads))) {
    LOG_WARN("fail to generate complete server loads", K(ret));
  } else if (OB_FAIL(divide_complete_server_loads_for_balance(
          available_servers, server_loads, wild_server_loads,
          over_server_loads, under_server_loads, upper_lmt))) {
    LOG_WARN("fail to sort server loads for inter ttg balance", K(ret));
  } else if (wild_server_loads.count() > 0) {
    if (OB_FAIL(make_wild_servers_empty(
            standalone_units, not_grant_units, wild_server_loads,
            over_server_loads, under_server_loads))) {
      LOG_WARN("fail to make wild servers empty", K(ret));
    }
  } else {
    if (OB_FAIL(make_available_servers_balance(
            standalone_units, not_grant_units, over_server_loads,
            under_server_loads, upper_lmt, g_res_weights, RES_MAX))) {
      LOG_WARN("fail to make available servers balance", K(ret));
    }
  }

  LOG_INFO("finish do non-tenantgroup unit balance task", KR(ret), K(zone),
      K(upper_lmt),
      K(wild_server_loads.count()),
      K(over_server_loads.count()),
      K(under_server_loads.count()),
      K(standalone_units), K(not_grant_units), K(available_servers));
  return ret;
}

int ObServerBalancer::do_non_ttg_unit_balance_by_cm(
    common::ObIArray<UnitMigrateStat> &task_array,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &units,
    common::ObIArray<ServerTotalLoad *> &over_server_loads,
    common::ObIArray<ServerTotalLoad *> &under_server_loads,
    const double upper_lmt,
    double *const g_res_weights,
    const int64_t weights_count)
{
  int ret = OB_SUCCESS;
  common::ObArray<PoolOccupation> pool_occupation;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(generate_pool_occupation_array(units, pool_occupation))) {
    LOG_WARN("fail to generate pool occupation array", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < over_server_loads.count(); ++i) {
      ServerTotalLoad *src = over_server_loads.at(i);
      for (int64_t j = under_server_loads.count() - 1;
           OB_SUCC(ret) && j >= 0;
           --j) {
        ServerTotalLoad *dst = under_server_loads.at(j);
        if (OB_FAIL(make_non_ttg_balance_under_load_by_cm(
                pool_occupation, task_array, units, src, dst,
                upper_lmt, g_res_weights, weights_count))) {
          LOG_WARN("fail to make non ttg balance under load", K(ret));
        } else {} // no more to do
      }
    }
  }
  return ret;
}

int ObServerBalancer::make_non_ttg_balance_under_load_by_cm(
    common::ObIArray<PoolOccupation> &pool_occupation,
    common::ObIArray<UnitMigrateStat> &task_array,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &units,
    ServerTotalLoad *src_server_load,
    ServerTotalLoad *dst_server_load,
    const double upper_lmt,
    double *const g_res_weights,
    const int64_t weights_count)
{
  int ret = OB_SUCCESS;
  double hard_limit = 0.0;
  ObArray<common::ObAddr> excluded_servers;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == src_server_load || NULL == dst_server_load
             || NULL == g_res_weights || RES_MAX != weights_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(src_server_load), K(dst_server_load));
  } else if (OB_UNLIKELY(src_server_load->wild_server_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be here", K(ret), "wild_server", src_server_load->wild_server_);
  } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ ptr is null", K(ret), K(unit_mgr_));
  } else if (OB_FAIL(unit_mgr_->get_hard_limit(hard_limit))) {
    LOG_WARN("fail to get hard limit", K(ret));
  } else {
    double upper_limit = std::min(upper_lmt, hard_limit);
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
      LoadSum this_load;
      ObUnitStat unit_stat;
      const ObUnitManager::ObUnitLoad &unit_load = units.at(i);
      double dst_load_value = 0.0;
      UnitMigrateStat unit_migrate;
      unit_migrate.unit_load_ = unit_load;
      unit_migrate.original_pos_ = src_server_load->server_;
      unit_migrate.arranged_pos_ = dst_server_load->server_;
      bool enough = true;
      if (src_server_load->inter_ttg_load_value_ < upper_limit) {
        break;
      } else if (!unit_load.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit load is invalid", K(ret), K(unit_load));
      } else if (src_server_load->server_ != unit_load.unit_->server_) {
        // not in this src, ignore
      } else if (OB_FAIL(unit_stat_mgr_.get_unit_stat(
              unit_load.unit_->unit_id_, unit_load.unit_->zone_, unit_stat))) {
        LOG_WARN("fail to get unit stat", K(ret), "unit", *unit_load.unit_);
      } else if (OB_FAIL(get_pool_occupation_excluded_dst_servers(
              unit_load, pool_occupation, excluded_servers))) {
        LOG_WARN("fail to get excluded servers", K(ret));
      } else if (has_exist_in_array(excluded_servers, dst_server_load->server_)) {
        // in excluded server
      } else if (OB_FAIL(this_load.append_load(unit_load))) {
        LOG_WARN("fail to append load", K(ret));
      } else if (OB_FAIL(check_single_server_resource_enough(
              this_load, unit_stat, *dst_server_load, enough))) {
        LOG_WARN("fail to check server resource enough", K(ret));
      } else if (!enough) {
      } else if (OB_FAIL(this_load.calc_load_value(
              g_res_weights, weights_count, *dst_server_load, dst_load_value))) {
        LOG_WARN("fail to calc load value", K(ret));
      } else if (dst_server_load->inter_ttg_load_value_ + dst_load_value >= upper_limit) {
        // Unbalanced results
      } else if (OB_FAIL(do_update_src_server_load(unit_load, *src_server_load, unit_stat))) {
        LOG_WARN("fail to do update src server load", K(ret));
      } else if (OB_FAIL(do_update_dst_server_load(
              unit_load, *dst_server_load, unit_stat, pool_occupation))) {
        LOG_WARN("fail to update dst server load", K(ret));
      } else if (OB_FAIL(task_array.push_back(unit_migrate))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObServerBalancer::choose_balance_info_amend(
    const common::ObIArray<TenantGroupBalanceInfo *> &tenant_group,
    const bool is_stable_tenantgroup,
    TenantGroupBalanceInfo *&info_need_amend,
    common::ObIArray<TenantGroupBalanceInfo *> &other_ttg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (tenant_group.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant group count is zero", K(ret));
  } else {
    int64_t idx = -1;
    info_need_amend = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_group.count(); ++i) {
      uint64_t amend_id = UINT64_MAX;
      uint64_t this_id = UINT64_MAX;
      TenantGroupBalanceInfo *this_info = tenant_group.at(i);
      if (OB_UNLIKELY(NULL == this_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant group balance info ptr is null", K(ret), KP(this_info));
      } else if (NULL == info_need_amend) {
        info_need_amend = this_info;
        idx = i;
      } else if (OB_FAIL(info_need_amend->get_trick_id(amend_id))) {
        LOG_WARN("fail to get trick id", K(ret));
      } else if (OB_FAIL(this_info->get_trick_id(this_id))) {
        LOG_WARN("fail to get trick id", K(ret));
      } else if (UINT64_MAX == amend_id || UINT64_MAX == this_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid info id", K(ret), K(amend_id), K(this_id));
      } else if (!is_stable_tenantgroup) {
        if (amend_id > this_id) {
          info_need_amend = this_info;
          idx = i;
        }
      } else {
        if (amend_id < this_id) {
          info_need_amend = this_info;
          idx = i;
        }
      }
    }
    if (-1 == idx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get info", K(ret));
    } else {
      other_ttg.reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_group.count(); ++i) {
        if (idx == i) {
          // do nothing
        } else if (OB_FAIL(other_ttg.push_back(tenant_group.at(i)))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::divide_tenantgroup_balance_info(
    common::ObIArray<TenantGroupBalanceInfo> &balance_info_array,
    common::ObIArray<TenantGroupBalanceInfo *> &stable_tenant_group,
    common::ObIArray<TenantGroupBalanceInfo *> &unstable_tenant_group)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    stable_tenant_group.reset();
    unstable_tenant_group.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < balance_info_array.count(); ++i) {
      TenantGroupBalanceInfo &balance_info = balance_info_array.at(i);
      if (balance_info.is_stable()) {
        if (OB_FAIL(stable_tenant_group.push_back(&balance_info))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        if (OB_FAIL(unstable_tenant_group.push_back(&balance_info))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::calc_global_balance_resource_weights(
    const common::ObZone &zone,
    const common::ObIArray<common::ObAddr> &available_servers,
    double *const resource_weights,
    const int64_t weights_count)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObUnitManager::ObUnitLoad> zone_unit_loads;
  common::ObArray<common::ObAddr> zone_servers;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty()
                         || NULL == resource_weights
                         || RES_MAX != weights_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_ISNULL(unit_mgr_) || OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit_mgr_ or server_mgr_ is null", KR(ret), KP(unit_mgr_), KP(server_mgr_));
  } else if (OB_FAIL(SVR_TRACER.get_servers_of_zone(zone, zone_servers))) {
    LOG_WARN("fail to get zone servers", K(ret), K(zone));
  } else {
    LoadSum load_sum;
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_servers.count(); ++i) {
      ObArray<ObUnitManager::ObUnitLoad> *unit_loads = NULL;
      if (OB_FAIL(unit_mgr_->get_loads_by_server(zone_servers.at(i), unit_loads))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get loads by server", K(ret));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_UNLIKELY(NULL == unit_loads)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit_loads ptr is null", K(ret), KP(unit_loads));
      } else if (OB_FAIL(load_sum.append_load(*unit_loads))) {
        LOG_WARN("fail to append load", K(ret));
      } else {} // no more to do
    }
    ResourceSum resource_sum;
    for (int64_t i = 0; OB_SUCC(ret) && i < available_servers.count(); ++i) {
      const common::ObAddr &server = available_servers.at(i);
      share::ObServerResourceInfo resource_info;
      if (OB_FAIL(server_mgr_->get_server_resource_info(server, resource_info))) {
        LOG_WARN("fail to get resource_info", KR(ret), K(server));
      } else if (OB_FAIL(resource_sum.append_resource(resource_info))) {
        LOG_WARN("fail to append resource", K(ret));
      } else {} // no more to do
    }
    for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
      ObResourceType resource_type = static_cast<ObResourceType>(i);
      const double required = load_sum.get_required(resource_type);
      const double capacity = resource_sum.get_capacity(resource_type);
      if (required <= 0 || capacity <= 0) {
        resource_weights[resource_type] = 0.0;
      } else if (required >= capacity) {
        resource_weights[resource_type] = 1.0;
      } else {
        resource_weights[resource_type] = required / capacity;
      }
    }
    if (OB_SUCC(ret)) {
      double sum = 0.0;
      const int64_t N = available_servers.count();
      for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
        resource_weights[i] /= static_cast<double>(N);
        sum += resource_weights[i];
        if (resource_weights[i] < 0 || resource_weights[i] > 1) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_ERROR("weight shall be in interval [0,1]", K(i), "w", resource_weights[i]);
        }
      }
      if (OB_SUCC(ret) && sum > 0) {
        for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
          resource_weights[i] /= sum;
        }
      }
    }
  }

  LOG_INFO("finish calc global balance resource weights", KR(ret), K(zone),
      "cpu_weights", resource_weights[RES_CPU],
      "mem_weights", resource_weights[RES_MEM],
      "log_disk_weights", resource_weights[RES_LOG_DISK],
      K(available_servers));
  return ret;
}

int ObServerBalancer::check_and_get_tenant_matrix_unit_num(
    const Matrix<uint64_t> &tenant_id_matrix,
    const common::ObZone &zone,
    bool &unit_num_match,
    ObIArray<int64_t> &column_unit_num_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!tenant_id_matrix.is_valid() || zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id_matrix), K(zone));
  } else {
    column_unit_num_array.reset();
    unit_num_match = true;
    const int64_t row_count = tenant_id_matrix.get_row_count();
    const int64_t column_count = tenant_id_matrix.get_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && unit_num_match && i < column_count; ++i) {
      ObArray<uint64_t> tenant_column;
      for (int64_t j = 0; OB_SUCC(ret) && j < row_count; ++j) {
        uint64_t tenant_id = OB_INVALID_ID;
        if (OB_FAIL(tenant_id_matrix.get(j, i, tenant_id))) {
          LOG_WARN("fail to get element", K(ret));
        } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant id value unexpected", K(ret), K(tenant_id));
        } else if (OB_FAIL(tenant_column.push_back(tenant_id))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
      if (OB_SUCC(ret)) {
        int64_t unit_num = 0;
        if (OB_FAIL(check_and_get_tenant_column_unit_num(
                tenant_column, zone, unit_num_match, unit_num))) {
          LOG_WARN("fail to check and get tenant column unit num", K(ret));
        } else if (!unit_num_match || 0 == unit_num) {
          // unit num not match or unit num is zero on this zone
        } else if (OB_FAIL(column_unit_num_array.push_back(unit_num))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
    }
  }
  return ret;
}

int ObServerBalancer::check_and_get_tenant_column_unit_num(
    const ObIArray<uint64_t> &tenant_id_column,
    const common::ObZone &zone,
    bool &unit_num_match,
    int64_t &unit_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(tenant_id_column.count() <= 0 || zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else {
    unit_num_match = true;
    bool is_first = true;
    int64_t prev_unit_num = -1;
    for (int64_t i = 0; unit_num_match && OB_SUCC(ret) && i < tenant_id_column.count(); ++i) {
      int64_t this_unit_num = -1;
      const uint64_t tenant_id = tenant_id_column.at(i);
      if (OB_UNLIKELY(NULL == unit_mgr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
      } else if (OB_FAIL(unit_mgr_->inner_get_tenant_zone_full_unit_num(
              tenant_id, zone, this_unit_num))) {
        LOG_WARN("fail to get tenant zone unit num", K(ret));
      } else if (is_first || this_unit_num == prev_unit_num) {
        is_first = false;
        prev_unit_num = this_unit_num;
      } else {
        unit_num_match = false;
      }
    }
    if (OB_SUCC(ret) && unit_num_match) {
      unit_num = prev_unit_num;
    }
  }
  return ret;
}

int ObServerBalancer::generate_original_unit_matrix(
    const Matrix<uint64_t> &tenant_id_matrix,
    const common::ObZone &zone,
    const ObIArray<int64_t> &column_unit_num_array,
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else {
    const int64_t tenant_matrix_row_count = tenant_id_matrix.get_row_count();
    const int64_t tenant_matrix_column_count = tenant_id_matrix.get_column_count();
    const int64_t unit_matrix_row_count = tenant_id_matrix.get_row_count();
    int64_t unit_matrix_column_count = 0;
    for (int64_t i = 0; i < column_unit_num_array.count(); ++i) {
      unit_matrix_column_count += column_unit_num_array.at(i);
    }
    if (tenant_matrix_row_count <= 0 || tenant_matrix_row_count >= INT32_MAX
        || tenant_matrix_column_count <= 0 || tenant_matrix_column_count >= INT32_MAX) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant matrix row or column count unexpected", K(ret),
               K(tenant_matrix_row_count), K(tenant_matrix_column_count));
    } else if (unit_matrix_column_count <= 0 || unit_matrix_column_count >= INT32_MAX) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit matrix column count value unexpected", K(ret));
    } else if (OB_FAIL(unit_migrate_stat_matrix.init(
            unit_matrix_row_count, unit_matrix_column_count))) {
      LOG_WARN("fail to init unit migrate stat", K(ret));
    } else {
      common::ObArray<ObUnitManager::ObUnitLoad> unit_loads;
      const common::ObReplicaType replica_type = common::REPLICA_TYPE_FULL;
      for (int64_t row = 0; OB_SUCC(ret) && row < tenant_matrix_row_count; ++row) {
        int64_t acc_column_idx = 0;
        for (int64_t column = 0; OB_SUCC(ret) && column < tenant_matrix_column_count; ++column) {
          unit_loads.reset();
          uint64_t tenant_id = OB_INVALID_ID;
          if (OB_FAIL(tenant_id_matrix.get(row, column, tenant_id))) {
            LOG_WARN("fail to get tenant id", K(ret));
          } else if (OB_INVALID_ID == tenant_id) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant id unexpected", K(ret), K(tenant_id));
          } else if (OB_UNLIKELY(NULL == unit_mgr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit_mgr_ is null", K(ret), KP(unit_mgr_));
          } else if (OB_FAIL(unit_mgr_->get_tenant_zone_unit_loads(
                  tenant_id, zone, replica_type, unit_loads))) {
            if (OB_ENTRY_NOT_EXIST != ret) {
              LOG_WARN("fail to get tenant zone unit infos", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          } else if (OB_FAIL(fill_unit_migrate_stat_matrix(
                  unit_migrate_stat_matrix, tenant_id, row, acc_column_idx, unit_loads))) {
            LOG_WARN("fail to fill unit migrate stat matrix", K(ret));
          } else {
            acc_column_idx += unit_loads.count();
          }
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::fill_unit_migrate_stat_matrix(
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    const uint64_t tenant_id,
    const int64_t row,
    const int64_t start_column_id,
    const common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t unit_matrix_row_count = unit_migrate_stat_matrix.get_row_count();
    const int64_t unit_matrix_column_count = unit_migrate_stat_matrix.get_column_count();
    if (row >= unit_matrix_row_count
        || start_column_id >= unit_matrix_column_count
        || start_column_id + unit_loads.count() > unit_matrix_column_count) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(row), K(start_column_id), K(unit_loads),
               K(unit_matrix_row_count), K(unit_matrix_column_count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < unit_loads.count(); ++i) {
        const ObUnitManager::ObUnitLoad &unit_load = unit_loads.at(i);
        if (!unit_load.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit load invalid", K(ret), K(unit_load));
        } else {
          UnitMigrateStat stat;
          stat.tenant_id_ = tenant_id;
          stat.original_pos_ = unit_load.unit_->server_;
          stat.arranged_pos_ = unit_load.unit_->server_;
          stat.unit_load_ = unit_load;
          if (OB_FAIL(unit_migrate_stat_matrix.set(row, start_column_id + i, stat))) {
            LOG_WARN("fail to set", K(ret));
          } else {} // no more to do
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::single_unit_migrate_stat_matrix_balance(
    TenantGroupBalanceInfo &balance_info,
    const common::ObIArray<common::ObAddr> &available_servers)
{
  int ret = OB_SUCCESS;
  const ObIArray<int64_t> &column_unit_num_array = balance_info.column_unit_num_array_;
  Matrix<UnitMigrateStat> &unit_migrate_stat_matrix = balance_info.unit_migrate_stat_matrix_;
  ObIArray<UnitGroupLoad> &unitgroup_loads = balance_info.unitgroup_load_array_;
  ObIArray<ServerLoad> &server_loads = balance_info.server_load_array_;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (column_unit_num_array.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(balance_row_units(
          balance_info, column_unit_num_array, unit_migrate_stat_matrix,
          unitgroup_loads, server_loads, available_servers))) {
    LOG_WARN("fail to balance row units", K(ret));
  } else if (OB_FAIL(inner_ttg_balance_strategy_.coordinate_all_column_unit_group(
          column_unit_num_array, unit_migrate_stat_matrix, unitgroup_loads))) {
    LOG_WARN("fail to coordinate column units", K(ret));
  } else {} // no more to do
  return ret;
}

int ObServerBalancer::InnerTenantGroupBalanceStrategy::coordinate_all_column_unit_group(
    const common::ObIArray<int64_t> &column_unit_num_array,
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    const common::ObIArray<UnitGroupLoad> &unitgroup_loads)
{
  int ret = OB_SUCCESS;
  if (column_unit_num_array.count() <= 0
             || unit_migrate_stat_matrix.get_row_count() <= 0
             || unitgroup_loads.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_unit_num_array),
             K(unit_migrate_stat_matrix), K(unitgroup_loads));
  } else {
    int64_t accumulate_column_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_unit_num_array.count(); ++i) {
      if (OB_FAIL(coordinate_single_column_unit_group(
              accumulate_column_idx, column_unit_num_array.at(i),
              unit_migrate_stat_matrix, unitgroup_loads))) {
        LOG_WARN("fail to coordinate single column unit", K(ret));
      } else {
        accumulate_column_idx += column_unit_num_array.at(i);
      }
    }
  }
  return ret;
}

int ObServerBalancer::InnerTenantGroupBalanceStrategy::coordinate_single_column_unit_group(
    const int64_t start_column_idx,
    const int64_t this_column_count,
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    const common::ObIArray<UnitGroupLoad> &unitgroup_loads)
{
  int ret = OB_SUCCESS;
  if (start_column_idx < 0
      || this_column_count <= 0
      || start_column_idx + this_column_count > unit_migrate_stat_matrix.get_column_count()
      || unit_migrate_stat_matrix.get_row_count() <= 0
      || unit_migrate_stat_matrix.get_column_count() <= 0
      || unitgroup_loads.count() <= 0
      || unitgroup_loads.count() != unit_migrate_stat_matrix.get_column_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_column_idx), K(this_column_count),
             K(unit_migrate_stat_matrix), K(unitgroup_loads));
  } else {
    const int64_t end_column_idx = start_column_idx + this_column_count;
    ObArray<common::ObAddr> dest_server_array;
    for (int64_t i = start_column_idx; OB_SUCC(ret) && i < end_column_idx; ++i) {
      if (OB_FAIL(dest_server_array.push_back(unitgroup_loads.at(i).server_))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_coordinate_single_column_unit_group(
              start_column_idx, this_column_count,
              unit_migrate_stat_matrix, dest_server_array))) {
        LOG_WARN("fail to coordinate single column unit group", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObServerBalancer::InnerTenantGroupBalanceStrategy::do_coordinate_single_column_unit_group(
    const int64_t start_column_idx,
    const int64_t column_count,
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    const common::ObIArray<common::ObAddr> &dest_servers)
{
  int ret = OB_SUCCESS;
  if (start_column_idx < 0
      || column_count <= 0
      || dest_servers.count() <= 0
      || start_column_idx + column_count > unit_migrate_stat_matrix.get_column_count()
      || unit_migrate_stat_matrix.get_row_count() <= 0
      || unit_migrate_stat_matrix.get_column_count() <= 0
      || dest_servers.count() != column_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_column_idx), K(column_count),
             K(unit_migrate_stat_matrix), K(dest_servers));
  } else {
    for (int64_t row = 0;
         OB_SUCC(ret) && row < unit_migrate_stat_matrix.get_row_count();
         ++row) {
      if (OB_FAIL(do_coordinate_single_unit_row(
              row, start_column_idx, column_count,
              dest_servers, unit_migrate_stat_matrix))) {
        LOG_WARN("fail to coordinate single unit row", K(ret));
      }
    }
  }
  // After the above operation is over, the actual unit has been aggregated.
  // In order to be more tidy in the form,
  // the matrix is sorted vertically according to the server to facilitate the observation of information during debugging.
  UnitMigrateStatCmp cmp_operator;
  if (OB_FAIL(ret)) {
    // failed in the previous procedure
  } else if (OB_FAIL(unit_migrate_stat_matrix.sort_column_group(
            start_column_idx, column_count, cmp_operator))) {
    LOG_WARN("fail to sort unit group by arranged pos", K(ret));
  } else if (OB_FAIL(cmp_operator.get_ret())) {
    LOG_WARN("fail to sort column group", K(ret));
  } else {} // no more to do
  return ret;
}

int ObServerBalancer::InnerTenantGroupBalanceStrategy::do_coordinate_single_unit_row(
    const int64_t row,
    const int64_t start_column_idx,
    const int64_t column_count,
    const common::ObIArray<common::ObAddr> &candidate_servers,
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix)
{
  int ret = OB_SUCCESS;
  if (row < 0 || start_column_idx < 0 || column_count <= 0
      || start_column_idx + column_count > unit_migrate_stat_matrix.get_column_count()
      || unit_migrate_stat_matrix.get_row_count() <= 0
      || unit_migrate_stat_matrix.get_column_count() <= 0
      || row >= unit_migrate_stat_matrix.get_row_count()
      || candidate_servers.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row), K(start_column_idx), K(column_count),
             K(unit_migrate_stat_matrix), K(candidate_servers));
  } else {
    const int64_t end_column_idx = start_column_idx + column_count;
    ObArray<common::ObAddr> excluded_array;
    // Need to run twice, in the first pass, throw the ones that meet the requirements into the exclude array
    // The second pass will adjust those that do not meet the requirements
    for (int64_t column = start_column_idx; OB_SUCC(ret) && column < end_column_idx; ++column) {
      UnitMigrateStat unit_migrate_stat;
      if (OB_FAIL(unit_migrate_stat_matrix.get(row, column, unit_migrate_stat))) {
        LOG_WARN("fail to get unit migrate stat", K(ret));
      } else if (!has_exist_in_array(candidate_servers, unit_migrate_stat.original_pos_)) {
        // The current location is not in the candidate servers and needs to be re-allocated,
        // so the first pass will not be processed here
      } else {
        unit_migrate_stat.arranged_pos_ = unit_migrate_stat.original_pos_;
        if (OB_FAIL(unit_migrate_stat_matrix.set(row, column, unit_migrate_stat))) {
          LOG_WARN("fail to set unit migrate stat", K(ret));
        } else if (OB_FAIL(excluded_array.push_back(unit_migrate_stat.original_pos_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
    for (int64_t column = start_column_idx; OB_SUCC(ret) && column < end_column_idx; ++column) {
      UnitMigrateStat unit_migrate_stat;
      if (OB_FAIL(unit_migrate_stat_matrix.get(row, column, unit_migrate_stat))) {
        LOG_WARN("fail to get unit migrate stat", K(ret));
      } else if (has_exist_in_array(candidate_servers, unit_migrate_stat.original_pos_)) {
        // The current position is already in the candidate servers,
        // it has been processed in the first pass, skip it
      } else {
        // The current position is not in the candidate servers and needs to be re-allocated
        if (OB_FAIL(arrange_unit_migrate_stat_pos(
                candidate_servers, unit_migrate_stat, excluded_array))) {
          LOG_WARN("fail to arrange unit migrate stat pos", K(ret));
        } else if (OB_FAIL(unit_migrate_stat_matrix.set(row, column, unit_migrate_stat))) {
          LOG_WARN("fail to set unit migrate stat", K(ret));
        } else if (OB_FAIL(excluded_array.push_back(unit_migrate_stat.arranged_pos_))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // fine, no more to do
      }
    }
  }
  return ret;
}

int ObServerBalancer::InnerTenantGroupBalanceStrategy::arrange_unit_migrate_stat_pos(
    const common::ObIArray<common::ObAddr> &candidate_servers,
    UnitMigrateStat &unit_migrate_stat,
    const common::ObIArray<common::ObAddr> &excluded_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(candidate_servers.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(candidate_servers));
  } else if (has_exist_in_array(candidate_servers, unit_migrate_stat.original_pos_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit migrate stat already in candidate", K(ret),
             K(unit_migrate_stat), K(candidate_servers));
  } else {
    bool find = false;
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < candidate_servers.count(); ++i) {
      const common::ObAddr &this_candidate = candidate_servers.at(i);
      if (has_exist_in_array(excluded_array, this_candidate)) {
        // Cannot be used as a new pos in the excluded array
      } else {
        unit_migrate_stat.arranged_pos_ = this_candidate;
        find = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cannot find a new pos for unit migrate stat", K(ret),
               K(candidate_servers), K(excluded_array));
    } else { /* good */ }
  }
  return ret;
}

int ObServerBalancer::balance_row_units(
    TenantGroupBalanceInfo &balance_info,
    const common::ObIArray<int64_t> &column_unit_num_array,
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    common::ObIArray<ServerLoad> &server_loads,
    const common::ObIArray<common::ObAddr> &available_servers)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (column_unit_num_array.count() <= 0
             || unit_migrate_stat_matrix.get_row_count() <= 0
             || unit_migrate_stat_matrix.get_column_count() <= 0
             || available_servers.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_unit_num_array),
             K(available_servers), K(unit_migrate_stat_matrix));
  } else if (OB_FAIL(generate_unitgroup_and_server_load(
          unit_migrate_stat_matrix, column_unit_num_array, available_servers,
          unitgroup_loads, server_loads))) {
    LOG_WARN("fail to generate server and unitgroup load", K(ret));
  } else if (OB_FAIL(calc_row_balance_resource_weights(
          server_loads, unitgroup_loads, balance_info.intra_weights_, RES_MAX))) {
    LOG_WARN("fail to calc resource weights", K(ret));
  } else if (OB_FAIL(update_server_load_value(
          balance_info.intra_weights_, RES_MAX, server_loads))) {
    LOG_WARN("fail to update unitgroup and server load value", K(ret));
  } else if (OB_FAIL(do_balance_row_units(
          balance_info, unit_migrate_stat_matrix, unitgroup_loads, server_loads))) {
    LOG_WARN("fail to do balance row units", K(ret));
  } else {} // no more to do
  return ret;
}

int ObServerBalancer::generate_unitgroup_and_server_load(
    const Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    const common::ObIArray<int64_t> &column_unit_num_array,
    const common::ObIArray<common::ObAddr> &available_servers,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    common::ObIArray<ServerLoad> &server_loads)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (unit_migrate_stat_matrix.get_row_count() <= 0
             || unit_migrate_stat_matrix.get_column_count() <= 0
             || column_unit_num_array.count() <= 0
             || available_servers.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit_migrate_stat_matrix), K(available_servers));
  } else if (OB_FAIL(generate_unitgroup_load(
          unit_migrate_stat_matrix, column_unit_num_array, unitgroup_loads))) {
    LOG_WARN("fail to generate unitgroup load", K(ret));
  } else if (OB_FAIL(generate_server_load(available_servers, unitgroup_loads, server_loads))) {
    LOG_WARN("fail to generate server load");
  } else {} // finish
  return ret;
}

int ObServerBalancer::generate_unitgroup_load(
    const Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    const common::ObIArray<int64_t> &column_unit_num_array,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    unitgroup_loads.reset();
    // A separate unitgroup_loads is generated for each column of the unit matrix
    int64_t accumulate_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_unit_num_array.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < column_unit_num_array.at(i); ++j) {
        int64_t column = accumulate_idx + j;
        if (OB_FAIL(generate_column_unitgroup_load(
                column, unit_migrate_stat_matrix, accumulate_idx,
                column_unit_num_array.at(i), unitgroup_loads))) {
          LOG_WARN("fail to generate unit group load", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        accumulate_idx += column_unit_num_array.at(i);
      }
    }
  }
  return ret;
}

int ObServerBalancer::generate_column_unitgroup_load(
    const int64_t column,
    const Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    const int64_t start_column_idx,
    const int64_t column_count,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (column < 0 || start_column_idx < 0 || column_count <= 0
             || start_column_idx + column_count > unit_migrate_stat_matrix.get_column_count()
             || column >= unit_migrate_stat_matrix.get_column_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_column_idx), K(column),
             K(column_count), K(unit_migrate_stat_matrix));
  } else {
    UnitGroupLoad unitgroup_load;
    common::ObAddr server_addr;
    uint64_t column_tenant_id = OB_INVALID_ID;
    bool is_first = true;
    for (int64_t row = 0; row < unit_migrate_stat_matrix.get_row_count(); ++row) {
      UnitMigrateStat unit_migrate_stat;
      if (OB_FAIL(unit_migrate_stat_matrix.get(row, column, unit_migrate_stat))) {
        LOG_WARN("fail to get unit migrate stat matrix", K(ret));
      } else if (OB_FAIL(unitgroup_load.unit_loads_.push_back(
              *unit_migrate_stat.unit_load_.unit_config_))) {
        LOG_WARN("fail to append unit load", K(ret));
      } else {
        if (is_first) {
          column_tenant_id = unit_migrate_stat.tenant_id_;
          server_addr = unit_migrate_stat.arranged_pos_;
        }
        is_first = false;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!server_addr.is_valid() || OB_INVALID_ID == column_tenant_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("server addr unexpected", K(ret), K(column),
               K(server_addr), K(column_tenant_id), K(unit_migrate_stat_matrix));
    } else {
      unitgroup_load.server_ = server_addr;
      unitgroup_load.column_tenant_id_ = column_tenant_id;
      unitgroup_load.start_column_idx_ = start_column_idx;
      unitgroup_load.column_count_ = column_count;
      if (OB_FAIL(unitgroup_load.sum_group_load())) {
        LOG_WARN("fail to sum group load", K(ret));
      } else if (OB_FAIL(unitgroup_loads.push_back(unitgroup_load))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // good
    }
  }
  return ret;
}

// When balancing within a group,
// it is necessary to virtually set the resources (cpu, memory) of each server in the zone to the same value.
// Here is actually directly taking the resources of the first server
int ObServerBalancer::try_regulate_intra_ttg_resource_info(
    const share::ObServerResourceInfo &this_resource_info,
    share::ObServerResourceInfo &intra_ttg_resource_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!this_resource_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(this_resource_info));
  } else if (intra_ttg_resource_info.is_valid()) {
    // bypass
  } else {
    intra_ttg_resource_info = this_resource_info;
  }

  return ret;
}

int ObServerBalancer::generate_server_load(
    const common::ObIArray<common::ObAddr> &available_servers,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    common::ObIArray<ServerLoad> &server_loads)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (available_servers.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(available_servers));
  } else if (OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_mgr_ is null", KR(ret), KP(server_mgr_));
  } else {
    // Place the generated unitgroup load into the corresponding server load
    server_loads.reset();
    ServerLoad server_load;
    share::ObServerResourceInfo resource_info;
    share::ObServerResourceInfo intra_ttg_resource_info;
    // Pre-fill the server first, and fill in the server resource info
    for (int64_t i = 0; OB_SUCC(ret) && i < available_servers.count(); ++i) {
      server_load.reset();
      resource_info.reset();
      server_load.server_ = available_servers.at(i);
      if (OB_FAIL(server_mgr_->get_server_resource_info(server_load.server_, resource_info))) {
        LOG_WARN("fail to get server status", KR(ret), K(server_load.server_));
      } else if (OB_FAIL(try_regulate_intra_ttg_resource_info(
          resource_info,
          intra_ttg_resource_info))) {
        LOG_WARN("fail to try regulate intra resource info", K(ret));
      } else {
        server_load.resource_info_ = resource_info;
        if (OB_FAIL(server_loads.push_back(server_load))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
    }
    // Then fill in the intra ttg resource info of each server
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      ServerLoad &server_load = server_loads.at(i);
      if (OB_UNLIKELY(!intra_ttg_resource_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid intra ttg resource info", K(ret), K(intra_ttg_resource_info));
      } else {
        server_load.intra_ttg_resource_info_ = intra_ttg_resource_info;
      }
    }
    // Then fill the unitgroup load into the corresponding server load
    const int64_t server_load_count = server_loads.count();
    common::ObArray<UnitGroupLoad *> wild_ug_loads; // the ug not in available servers
    for (int64_t i = 0; OB_SUCC(ret) && i < unitgroup_loads.count(); ++i) {
      bool find = false;
      UnitGroupLoad &unitgroup_load = unitgroup_loads.at(i);
      for (int64_t j = 0; !find && OB_SUCC(ret) && j < server_load_count; ++j) {
        ServerLoad &server_load = server_loads.at(j);
        if (server_load.server_ != unitgroup_load.server_) {
          // go on to check next
        } // NOTE: Push in here is the pointer
        else if (OB_FAIL(server_load.unitgroup_loads_.push_back(&unitgroup_load))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          unitgroup_load.server_load_ = &server_load;
          find = true;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (server_load_count <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server load count unexpected", K(ret), K(server_load_count));
      } else if (!find) {
        if (OB_FAIL(wild_ug_loads.push_back(&unitgroup_load))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {} // find one, good
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < wild_ug_loads.count(); ++i) {
      UnitGroupLoad *ug_load = wild_ug_loads.at(i);
      if (OB_UNLIKELY(NULL == ug_load)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ug ptr is null", K(ret));
      } else if (OB_FAIL(arrange_server_load_for_wild_unitgroup(*ug_load, server_loads))) {
        LOG_WARN("cannot arrange server load for wild unitgroup", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObServerBalancer::arrange_server_load_for_wild_unitgroup(
    UnitGroupLoad &unitgroup_load,
    common::ObIArray<ServerLoad> &server_loads)
{
  int ret = OB_SUCCESS;
  bool find = false;
  for (int64_t i = 0; !find && OB_SUCC(ret) && i < server_loads.count(); ++i) {
    ServerLoad &server_load = server_loads.at(i);
    bool same_pool_found = false;
    for (int64_t j = 0;
         !same_pool_found && OB_SUCC(ret) && j < server_load.unitgroup_loads_.count();
         ++j) {
      UnitGroupLoad *this_ug = server_load.unitgroup_loads_.at(j);
      if (OB_UNLIKELY(NULL == this_ug)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ug ptr is null", K(ret));
      } else if (this_ug->column_tenant_id_ != unitgroup_load.column_tenant_id_) {
        // good, go on to check next
      } else {
        same_pool_found = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (same_pool_found) {
      // A ug from the same resource pool as unitgroup_load was found on the server,
      // and the server is not suitable
    } else if (OB_FAIL(server_load.unitgroup_loads_.push_back(&unitgroup_load))) {
      LOG_WARN("fail to push back", K(ret));
    } else {
      unitgroup_load.server_ = server_load.server_;
      unitgroup_load.server_load_ = &server_load;
      find = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!find) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_WARN("no available server to hold more unit",
             K(ret), "tenant_id", unitgroup_load.column_tenant_id_);
  }
  return ret;
}

int ObServerBalancer::calc_row_balance_resource_weights(
    const common::ObIArray<ServerLoad> &server_loads,
    const common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    double *const resource_weights,
    const int64_t weights_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(server_loads.count() <= 0
                         || unitgroup_loads.count() <= 0
                         || NULL == resource_weights
                         || RES_MAX != weights_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server_loads), K(unitgroup_loads));
  } else {
    for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
      ObResourceType resource_type = static_cast<ObResourceType>(i);
      double sum_capacity = 0.0;
      for (int64_t j = 0; j < server_loads.count(); ++j) {
        sum_capacity += server_loads.at(j).get_intra_ttg_resource_capacity(resource_type);
      }
      double sum_assigned = 0.0;
      for (int64_t j = 0; j < unitgroup_loads.count(); ++j) {
        sum_assigned += unitgroup_loads.at(j).load_sum_.get_required(resource_type);
      }
      if (sum_assigned <= 0 || sum_capacity <= 0) {
        resource_weights[resource_type] = 0.0;
      } else if (sum_assigned >= sum_capacity) {
        resource_weights[resource_type] = 1.0;
      } else {
        resource_weights[resource_type] = sum_assigned / sum_capacity;
      }
    }
    if (OB_SUCC(ret)) { // Weight normalization
      double sum = 0.0;
      const int64_t N = server_loads.count();
      for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
        resource_weights[i] /= static_cast<double>(N);
        sum += resource_weights[i];
        if (resource_weights[i] < 0 || resource_weights[i] > 1) {
          ret = common::OB_ERR_UNEXPECTED;
          LOG_ERROR("weight shall be in interval [0,1]", K(i), "w", resource_weights[i]);
        }
      }
      if (OB_SUCC(ret) && sum > 0) {
        for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
          resource_weights[i] /= sum;
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::update_server_load_value(
    const double *const resource_weights,
    const int64_t weights_count,
    common::ObIArray<ServerLoad> &server_loads)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (weights_count != RES_MAX || NULL == resource_weights) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(weights_count), KP(resource_weights));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      ServerLoad &server_load = server_loads.at(i);
      if (!server_load.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should be a valid server load here", K(ret), K(server_load));
      } else {
        for (int32_t j = RES_CPU; j < RES_MAX; ++j) {
          server_load.intra_weights_[j] = resource_weights[j];
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      ServerLoad &server_load = server_loads.at(i);
      if (!server_load.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should be a valid server load here", K(ret), K(server_load));
      } else if (OB_FAIL(server_load.update_load_value())) {
        LOG_WARN("server fails to update load value", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObServerBalancer::do_balance_row_units(
    TenantGroupBalanceInfo &balance_info,
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    common::ObIArray<ServerLoad> &server_loads)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(inner_ttg_balance_strategy_.do_balance_row_units(
          balance_info, unit_migrate_stat_matrix, unitgroup_loads, server_loads))) {
    LOG_WARN("fail to do balance row units", K(ret));
  } else {}
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::do_balance_row_units(
    TenantGroupBalanceInfo &balance_info,
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    common::ObIArray<ServerLoad> &server_loads)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(unitgroup_loads.count() <= 0 || server_loads.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server_loads), K(unitgroup_loads));
  } else if (OB_FAIL(make_count_balanced(
          balance_info, unit_migrate_stat_matrix, unitgroup_loads, server_loads))) {
    LOG_WARN("fail to make count balanced", K(ret));
  } else if (OB_FAIL(move_or_exchange_ug_balance_ttg_load(
          balance_info, unitgroup_loads, server_loads))) {
    LOG_WARN("fail to balance load", K(ret));
  } else {} // no more to do
  return ret;
}

// Continue to exchange the unitgroups on the maximum server load and the minimum server load
// until the exchange cannot reduce the difference between the two or the difference between the two is less than tolerance
int ObServerBalancer::CountBalanceStrategy::move_or_exchange_ug_balance_ttg_load(
    TenantGroupBalanceInfo &balance_info,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    common::ObIArray<ServerLoad> &server_loads)
{
  int ret = OB_SUCCESS;
  UNUSED(balance_info);
  if (OB_UNLIKELY(unitgroup_loads.count() <= 0 || server_loads.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unitgroup_loads), K(server_loads));
  } else {
    double sum_load = 0.0;
    ObArray<ServerLoad *> server_load_ptrs_sorted;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      if (OB_FAIL(server_load_ptrs_sorted.push_back(&server_loads.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      } else {
        sum_load += server_loads.at(i).intra_ttg_load_value_;
      }
    }
    if (OB_SUCC(ret)) {
      double tolerance = sum_load / 100;
      // Sort by intra load descending order
      IntraServerLoadCmp cmp;
      std::sort(server_load_ptrs_sorted.begin(),
                server_load_ptrs_sorted.end(),
                cmp);
      if (OB_FAIL(cmp.get_ret())) {
        LOG_WARN("fail to sorted", K(ret));
      } else if (OB_FAIL(do_move_or_exchange_ug_balance_ttg_load(
              server_load_ptrs_sorted, unitgroup_loads, tolerance))) {
        LOG_WARN("fail to exchange ug", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::do_move_or_exchange_ug_balance_ttg_load(
    common::ObArray<ServerLoad *> &server_load_ptrs_sorted,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    const double tolerance)
{
  int ret = OB_SUCCESS;
  if (server_load_ptrs_sorted.count() <= 0
      || unitgroup_loads.count() <= 0
      || unitgroup_loads.count() >= INT32_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server_load_ptrs_sorted), K(unitgroup_loads));
  } else {
    const int64_t max_times = unitgroup_loads.count() * unitgroup_loads.count();
    int64_t times = 0;
    bool do_operate = true;
    while (OB_SUCC(ret) && do_operate && times < max_times) {
      ServerLoad *max_load_server = NULL;
      ServerLoad *min_load_server = NULL;
      const int64_t server_cnt = server_load_ptrs_sorted.count();
      if (server_cnt <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server load array is empty", K(ret), K(server_cnt));
      } else if (NULL == (max_load_server = server_load_ptrs_sorted.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("load ptr is null", K(ret), KP(max_load_server));
      } else if (NULL == (min_load_server = server_load_ptrs_sorted.at(server_cnt - 1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("load ptr is null", K(ret), KP(min_load_server));
      } else if (max_load_server->intra_ttg_load_value_
                 < tolerance + min_load_server->intra_ttg_load_value_) {
        break; // load balanced
      } else if (max_load_server->unitgroup_loads_.count()
                 <= min_load_server->unitgroup_loads_.count()) {
        if (OB_FAIL(try_exchange_ug_balance_ttg_load_foreach(
              *max_load_server, *min_load_server, unitgroup_loads, do_operate))) {
          LOG_WARN("fail to exchange ug", K(ret));
        }
      } else if (max_load_server->unitgroup_loads_.count()
                 > min_load_server->unitgroup_loads_.count() + 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unitgroup load count unexpected", K(ret),
                 K(*max_load_server), K(*min_load_server));
      } else {
        if (OB_FAIL(try_move_ug_balance_ttg_load_foreach(
                *max_load_server, *min_load_server, unitgroup_loads, do_operate))) {
          LOG_WARN("fail to move ug", K(ret));
        } else if (do_operate) {
          // by pass
        } else if (OB_FAIL(try_exchange_ug_balance_ttg_load_foreach(
                *max_load_server, *min_load_server, unitgroup_loads, do_operate))) {
          LOG_WARN("fail to exchange ug", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (!do_operate) {
        // will leave the loop
      } else {
        times++;
        IntraServerLoadCmp cmp;
        std::sort(server_load_ptrs_sorted.begin(),
                  server_load_ptrs_sorted.end(),
                  cmp);
        if (OB_FAIL(cmp.get_ret())) {
          LOG_WARN("fail to sort", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::try_move_ug_balance_ttg_load_foreach(
    ServerLoad &max_server_load,
    ServerLoad &min_server_load,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    bool &do_move)
{
  int ret = OB_SUCCESS;
  do_move = false;
  if (max_server_load.unitgroup_loads_.count() <= min_server_load.unitgroup_loads_.count()) {
    // by pass
  } else {
    ObArray<common::ObAddr> excluded_dst_servers;
    for (int64_t i = max_server_load.unitgroup_loads_.count() - 1;
         !do_move && OB_SUCC(ret) && i >= 0;
         --i) {
      UnitGroupLoad &ug_load = *max_server_load.unitgroup_loads_.at(i);
      bool can_move = false;
      excluded_dst_servers.reset();
      if (OB_FAIL(get_ug_balance_excluded_dst_servers(
              ug_load, unitgroup_loads, excluded_dst_servers))) {
        LOG_WARN("fail to get excluded server", K(ret));
      } else if (has_exist_in_array(excluded_dst_servers, min_server_load.server_)) {
        // in excluded servers, ignore
      } else if (OB_FAIL(check_can_move_ug_balance_intra_ttg_load(
              max_server_load, ug_load, min_server_load, can_move))) {
        LOG_WARN("fail to check can move ug balance intra ttg load", K(ret));
      } else if (!can_move) {
        // ignore
      } else if (OB_FAIL(move_ug_between_server_loads(
              max_server_load, min_server_load, ug_load, i))) {
        LOG_WARN("fail to move ug between server loads", K(ret));
      } else {
        do_move = true;
      }
    }
    if (OB_SUCC(ret)) {
      UnitGroupLoadCmp unitgroup_load_cmp(max_server_load);
      std::sort(max_server_load.unitgroup_loads_.begin(),
                max_server_load.unitgroup_loads_.end(),
                unitgroup_load_cmp);
      if (OB_FAIL(unitgroup_load_cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      UnitGroupLoadCmp unitgroup_load_cmp(min_server_load);
      std::sort(min_server_load.unitgroup_loads_.begin(),
                min_server_load.unitgroup_loads_.end(),
                unitgroup_load_cmp);
      if (OB_FAIL(unitgroup_load_cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::try_exchange_ug_balance_ttg_load_foreach(
    ServerLoad &max_server_load,
    ServerLoad &min_server_load,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    bool &do_exchange)
{
  int ret = OB_SUCCESS;
  do_exchange = false;
  if (max_server_load.intra_ttg_load_value_ - min_server_load.intra_ttg_load_value_ < EPSILON) {
    do_exchange = false;
  } else if (max_server_load.unitgroup_loads_.count() <= 0
             || min_server_load.unitgroup_loads_.count() <= 0) {
    do_exchange = false;
  } else {
    for (int64_t i = 0;
         !do_exchange && OB_SUCC(ret) && i < max_server_load.unitgroup_loads_.count();
         ++i) {
      common::ObArray<common::ObAddr> excluded_servers;
      UnitGroupLoad *left_ug = max_server_load.unitgroup_loads_.at(i);
      double left_load = 0.0;
      if (OB_UNLIKELY(NULL == left_ug)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("src ug ptr is null", K(ret), KP(left_ug));
      } else if (OB_FAIL(get_ug_balance_excluded_dst_servers(
              *left_ug, unitgroup_loads, excluded_servers))) {
        LOG_WARN("fail to get excluded server", K(ret));
      } else if (has_exist_in_array(excluded_servers, min_server_load.server_)) {
        // ignore this
      } else if (OB_FAIL(left_ug->get_intra_ttg_load_value(max_server_load, left_load))) {
        LOG_WARN("fail to get intra ttg load value", K(ret));
      } else {
        for (int64_t j = min_server_load.unitgroup_loads_.count() - 1;
             !do_exchange && OB_SUCC(ret) && j >= 0;
             --j) {
          UnitGroupLoad *right_ug = min_server_load.unitgroup_loads_.at(j);
          // The calculation denominator of left load takes max_server_load,
          // so the calculation denominator of right load also takes max_server_load,
          // So that the comparison of the two values makes sense
          double right_load = 0.0;
          bool can_exchange = false;
          if (NULL == right_ug) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("src or dst ug ptr is null", K(ret), KP(right_ug));
          } else if (left_ug->column_tenant_id_ == right_ug->column_tenant_id_) {
            // ignore this,
          } else if (OB_FAIL(get_ug_balance_excluded_dst_servers(
                  *right_ug, unitgroup_loads, excluded_servers))) {
            LOG_WARN("fail to get excluded servers", K(ret));
          } else if (has_exist_in_array(excluded_servers, max_server_load.server_)) {
            // ignore this
          } else if (OB_FAIL(right_ug->get_intra_ttg_load_value(max_server_load, right_load))) {
            LOG_WARN("fail to get intra ttg load value", K(ret));
          } else if (left_load - right_load < EPSILON) {
            // When the left is smaller than the right or a little bigger than the right (EPSILON), no swap
          } else if (OB_FAIL(check_can_exchange_ug_balance_intra_ttg_load(
                *left_ug, max_server_load, *right_ug, min_server_load, can_exchange))) {
            LOG_WARN("fail to check can exchange ug balance ttg load", K(ret));
          } else if (!can_exchange) {
            // ignore this
          } else if (OB_FAIL(exchange_ug_between_server_loads(
                *left_ug, i, max_server_load, *right_ug, j, min_server_load))) {
            LOG_WARN("fail to exchange ug between server loads", K(ret));
          } else {
            do_exchange = true;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      UnitGroupLoadCmp unitgroup_load_cmp(max_server_load);
      std::sort(max_server_load.unitgroup_loads_.begin(),
                max_server_load.unitgroup_loads_.end(),
                unitgroup_load_cmp);
      if (OB_FAIL(unitgroup_load_cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      UnitGroupLoadCmp unitgroup_load_cmp(min_server_load);
      std::sort(min_server_load.unitgroup_loads_.begin(),
                min_server_load.unitgroup_loads_.end(),
                unitgroup_load_cmp);
      if (OB_FAIL(unitgroup_load_cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
  }
  return ret;
}

// Keep moving the unitgroup from the server load with the largest cnt to the server load with the smallest cnt,
// until the difference between the largest server load and the smallest server load is less than or equal to 1 unitgroup
int ObServerBalancer::CountBalanceStrategy::make_count_balanced(
    TenantGroupBalanceInfo &balance_info,
    Matrix<UnitMigrateStat> &unit_migrate_stat_matrix,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    common::ObIArray<ServerLoad> &server_loads)
{
  int ret = OB_SUCCESS;
  UNUSED(balance_info);
  UNUSED(unit_migrate_stat_matrix);
  if (OB_UNLIKELY(unitgroup_loads.count() <= 0 || server_loads.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server_loads), K(unitgroup_loads));
  } else {
    ObArray<ServerLoad *> server_cnt_ptrs_sorted;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      if (OB_FAIL(server_cnt_ptrs_sorted.push_back(&server_loads.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      // Sort by the number of unitgroups on the server in ascending order
      ServerLoadUgCntCmp cmp;
      std::sort(server_cnt_ptrs_sorted.begin(),
                server_cnt_ptrs_sorted.end(),
                cmp);
      if (OB_FAIL(cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      } else if (OB_FAIL(do_make_count_balanced(server_cnt_ptrs_sorted, unitgroup_loads))) {
        LOG_WARN("fail to make count balanced", K(ret));
      } else {} // no more to do
    }
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::do_make_count_balanced(
    common::ObArray<ServerLoad *> &server_cnt_ptrs_sorted,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads)
{
  int ret = OB_SUCCESS;
  if (server_cnt_ptrs_sorted.count() <= 0 || unitgroup_loads.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server_cnt_ptrs_sorted), K(unitgroup_loads));
  } else {
    int64_t times = 0;
    while (OB_SUCC(ret) && times < unitgroup_loads.count()) {
      ServerLoad *first = NULL;
      ServerLoad *last = NULL;
      const int64_t server_cnt = server_cnt_ptrs_sorted.count();
      if (server_cnt <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server load array is empty", K(ret));
      } else if (NULL == (first = server_cnt_ptrs_sorted.at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("load ptr is null", K(ret), KP(first));
      } else if (NULL == (last = server_cnt_ptrs_sorted.at(server_cnt - 1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("last ptr is null", K(ret), KP(last));
      } else if (last->unitgroup_loads_.count() - first->unitgroup_loads_.count() <= 1) {
        break;
      } else if (OB_FAIL(do_make_count_balanced_foreach(*last, *first, unitgroup_loads))) {
        LOG_WARN("fail to make count balanced foreach");
      } else {
        times++;
        ServerLoadUgCntCmp cmp;
        std::sort(server_cnt_ptrs_sorted.begin(),
                  server_cnt_ptrs_sorted.end(),
                  cmp);
        if (OB_FAIL(cmp.get_ret())) {
          LOG_WARN("fail to sort", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::do_make_count_balanced_foreach(
    ServerLoad &src_server_load,
    ServerLoad &dst_server_load,
    common::ObIArray<UnitGroupLoad> &unitgroup_loads)
{
  int ret = OB_SUCCESS;
  if (unitgroup_loads.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unitgroup_loads));
  } else {
    for (int64_t i = src_server_load.unitgroup_loads_.count() - 1;
         OB_SUCC(ret) && i >= 0;
         --i) {
      common::ObArray<common::ObAddr> excluded_servers;
      UnitGroupLoad *ug_load = src_server_load.unitgroup_loads_.at(i);
      if (src_server_load.unitgroup_loads_.count()
                 <= dst_server_load.unitgroup_loads_.count() + 1) {
        break;
      } else if (OB_UNLIKELY(NULL == ug_load)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ug_load ptr is null", K(ret), KP(ug_load));
      } else if (OB_FAIL(get_ug_balance_excluded_dst_servers(
              *ug_load, unitgroup_loads, excluded_servers))) {
        LOG_WARN("fail to get excluded servers", K(ret));
      } else if (has_exist_in_array(excluded_servers, dst_server_load.server_)) {
        // ignore this ug load, since it cannot move to dst server
      } else if (OB_FAIL(move_ug_between_server_loads(
              src_server_load, dst_server_load, *ug_load, i))) {
        LOG_WARN("fail to move ug", K(ret));
      } else {} // no more to do
    }
    if (OB_SUCC(ret)) {
      UnitGroupLoadCmp unitgroup_load_cmp(src_server_load);
      std::sort(src_server_load.unitgroup_loads_.begin(),
                src_server_load.unitgroup_loads_.end(),
                unitgroup_load_cmp);
      if (OB_FAIL(unitgroup_load_cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      UnitGroupLoadCmp unitgroup_load_cmp(dst_server_load);
      std::sort(dst_server_load.unitgroup_loads_.begin(),
                dst_server_load.unitgroup_loads_.end(),
                unitgroup_load_cmp);
      if (OB_FAIL(unitgroup_load_cmp.get_ret())) {
        LOG_WARN("fail to sort", K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::InnerTenantGroupBalanceStrategy::get_ug_balance_excluded_dst_servers(
    const UnitGroupLoad &unitgroup_load,
    const common::ObIArray<UnitGroupLoad> &unitgroup_loads,
    common::ObIArray<common::ObAddr> &excluded_servers)
{
  int ret = OB_SUCCESS;
  excluded_servers.reset();
  const uint64_t column_tenant_id = unitgroup_load.column_tenant_id_;
  for (int64_t i = 0; OB_SUCC(ret) && i < unitgroup_loads.count(); ++i) {
    const UnitGroupLoad &this_load = unitgroup_loads.at(i);
    if (column_tenant_id != this_load.column_tenant_id_) {
      // not from the same tenant column
    } else if (!has_exist_in_array(excluded_servers, this_load.server_)) {
      if (OB_FAIL(excluded_servers.push_back(this_load.server_))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObServerBalancer::InnerTenantGroupBalanceStrategy::move_ug_between_server_loads(
    ServerLoad &src_server_load,
    ServerLoad &dst_server_load,
    UnitGroupLoad &ug_load,
    const int64_t ug_idx)
{
  int ret = OB_SUCCESS;
  if (ug_idx < 0 || ug_idx >= src_server_load.unitgroup_loads_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ug_idx), K(src_server_load));
  } else if (&ug_load != src_server_load.unitgroup_loads_.at(ug_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ug load ptr and idx not match", K(ret), K(src_server_load), K(ug_load), K(ug_idx));
  } else {
    if (OB_FAIL(src_server_load.unitgroup_loads_.remove(ug_idx))) {
      LOG_WARN("remove failed", K(ret), K(ug_idx));
    } else if (OB_FAIL(dst_server_load.unitgroup_loads_.push_back(&ug_load))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(src_server_load.update_load_value())) {
      LOG_WARN("fail to update load value", K(ret));
    } else if (OB_FAIL(dst_server_load.update_load_value())) {
      LOG_WARN("fail to update load value", K(ret));
    } else {
      ug_load.server_ = dst_server_load.server_;
      ug_load.server_load_ = &dst_server_load;
    }
  }
  return ret;
}

int ObServerBalancer::InnerTenantGroupBalanceStrategy::exchange_ug_between_server_loads(
    UnitGroupLoad &left_ug,
    const int64_t left_idx,
    ServerLoad &left_server,
    UnitGroupLoad &right_ug,
    const int64_t right_idx,
    ServerLoad &right_server)
{
  int ret = OB_SUCCESS;
  if (left_idx < 0 || left_idx >= left_server.unitgroup_loads_.count()
      || right_idx < 0 || right_idx >= right_server.unitgroup_loads_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
            K(left_idx),
            "left_server_ug_cnt", left_server.unitgroup_loads_.count(),
            K(right_idx),
            "right_server_ug_cnt", right_server.unitgroup_loads_.count());
  } else if (&left_ug != left_server.unitgroup_loads_.at(left_idx)
             || &right_ug != right_server.unitgroup_loads_.at(right_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ug ptr and idx not match", K(ret), K(left_idx), K(right_idx));
  } else {
    if (OB_FAIL(left_server.unitgroup_loads_.remove(left_idx))) {
      LOG_WARN("fail to remove", K(ret));
    } else if (OB_FAIL(right_server.unitgroup_loads_.remove(right_idx))) {
      LOG_WARN("fail to remove", K(ret));
    } else if (OB_FAIL(left_server.unitgroup_loads_.push_back(&right_ug))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(right_server.unitgroup_loads_.push_back(&left_ug))) {
      LOG_WARN("fail to push back", K(ret));
    } else if (OB_FAIL(left_server.update_load_value())) {
      LOG_WARN("fail to update load value", K(ret));
    } else if (OB_FAIL(right_server.update_load_value())) {
      LOG_WARN("fail to update load value", K(ret));
    } else {
      left_ug.server_ = right_server.server_;
      left_ug.server_load_ = &right_server;
      right_ug.server_ = left_server.server_;
      right_ug.server_load_ = &left_server;
    }
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::check_can_move_ug_balance_intra_ttg_load(
    ServerLoad &left_server,
    UnitGroupLoad &ug_load,
    ServerLoad &right_server,
    bool &can_move)
{
  int ret = OB_SUCCESS;
  can_move = false;
  double left_before = left_server.intra_ttg_load_value_;
  double right_before = right_server.intra_ttg_load_value_;
  double ug_on_left_svr = 0.0; // ug's own load value of ug on the left server
  double ug_on_right_svr = 0.0; // Load value of ug itself after ug is moved to the right server
  double left_after = 0.0;
  double right_after = 0.0;
  if (OB_FAIL(ug_load.get_intra_ttg_load_value(left_server, ug_on_left_svr))) {
    LOG_WARN("fail to get left intra ttg load value on left", K(ret));
  } else if (OB_FAIL(ug_load.get_intra_ttg_load_value(right_server, ug_on_right_svr))) {
    LOG_WARN("fail to get left intra ttg load value on right", K(ret));
  } else {
    left_after = left_before - ug_on_left_svr;
    right_after = right_before + ug_on_right_svr;
    if (left_after > right_after) {
      can_move = (left_before - right_before > EPSILON)
                 && ((left_before - right_before) - (left_after - right_after) > EPSILON);
    } else {
      can_move = (left_before - right_before > EPSILON)
                 && ((left_before - right_before) - (right_after - left_after) > EPSILON);
    }
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::check_can_exchange_ug_balance_intra_ttg_load(
    UnitGroupLoad &left_ug,
    ServerLoad &left_server,
    UnitGroupLoad &right_ug,
    ServerLoad &right_server,
    bool &can_exchange)
{
  int ret = OB_SUCCESS;
  can_exchange = false;
  double left_before = left_server.intra_ttg_load_value_;
  double right_before = right_server.intra_ttg_load_value_;
  double left_ug_on_left_svr = 0.0; // Left ug's own load value of ug on the left server
  double left_ug_on_right_svr = 0.0; // The load value of ug itself after the left ug is moved to the right server
  double right_ug_on_right_svr = 0.0; // Right ug's own load value of ug on the right server
  double right_ug_on_left_svr = 0.0; // The load value of ug itself after the right ug is moved to the left server
  double left_after = 0.0;
  double right_after = 0.0;
  if (OB_FAIL(left_ug.get_intra_ttg_load_value(left_server, left_ug_on_left_svr))) {
    LOG_WARN("fail to get left intra ttg load value on left", K(ret));
  } else if (OB_FAIL(left_ug.get_intra_ttg_load_value(right_server, left_ug_on_right_svr))) {
    LOG_WARN("fail to get left intra ttg load value on right", K(ret));
  } else if (OB_FAIL(right_ug.get_intra_ttg_load_value(right_server, right_ug_on_right_svr))) {
    LOG_WARN("fail to get right intra ttg load value on right", K(ret));
  } else if (OB_FAIL(right_ug.get_intra_ttg_load_value(left_server, right_ug_on_left_svr))) {
    LOG_WARN("fail to get right intra ttg load value on left", K(ret));
  } else {
    left_after = left_before - left_ug_on_left_svr + right_ug_on_left_svr;
    right_after = right_before - right_ug_on_right_svr + left_ug_on_right_svr;
    if (left_after > right_after) {
      can_exchange = (left_before - right_before > EPSILON)
                     && ((left_before - right_before) - (left_after - right_after) > EPSILON);
    } else {
      can_exchange = (left_before - right_before > EPSILON)
                     && ((left_before - right_before) - (right_after - left_after) > EPSILON);
    }
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::check_can_move_ug_balance_inter_ttg_load(
    ServerLoad &left_server,
    UnitGroupLoad &ug_load,
    ServerLoad &right_server,
    bool &can_move)
{
  int ret = OB_SUCCESS;
  can_move = false;
  double left_before = left_server.intra_ttg_load_value_;
  double right_before = right_server.intra_ttg_load_value_;
  double ug_on_left_svr = 0.0; // ug's own load value of ug on the left server
  double ug_on_right_svr = 0.0; // Load value of ug itself after ug is moved to the right server
  double left_after = 0.0;
  double right_after = 0.0;
  if (OB_FAIL(ug_load.get_intra_ttg_load_value(left_server, ug_on_left_svr))) {
    LOG_WARN("fail to get left intra ttg load value on left", K(ret));
  } else if (OB_FAIL(ug_load.get_intra_ttg_load_value(right_server, ug_on_right_svr))) {
    LOG_WARN("fail to get left intra ttg load value on right", K(ret));
  } else {
    left_after = left_before - ug_on_left_svr;
    right_after = right_before + ug_on_right_svr;
    if (left_after > right_after) {
      can_move = (left_before - right_before > EPSILON)
                 && ((left_before - right_before) - (left_after - right_after) > -EPSILON);
    } else {
      can_move = (left_before - right_before > EPSILON)
                 && ((left_before - right_before) - (right_after - left_after) > -EPSILON);
    }
  }
  return ret;
}

int ObServerBalancer::CountBalanceStrategy::check_can_exchange_ug_balance_inter_ttg_load(
    UnitGroupLoad &left_ug,
    ServerLoad &left_server,
    UnitGroupLoad &right_ug,
    ServerLoad &right_server,
    bool &can_exchange)
{
  int ret = OB_SUCCESS;
  can_exchange = false;
  double left_before = left_server.intra_ttg_load_value_;
  double right_before = right_server.intra_ttg_load_value_;
  double left_ug_on_left_svr = 0.0;
  double left_ug_on_right_svr = 0.0;
  double right_ug_on_right_svr = 0.0;
  double right_ug_on_left_svr = 0.0;
  double left_after = 0.0;
  double right_after = 0.0;
  if (OB_FAIL(left_ug.get_intra_ttg_load_value(left_server, left_ug_on_left_svr))) {
    LOG_WARN("fail to get left intra ttg load value on left", K(ret));
  } else if (OB_FAIL(left_ug.get_intra_ttg_load_value(right_server, left_ug_on_right_svr))) {
    LOG_WARN("fail to get left intra ttg load value on right", K(ret));
  } else if (OB_FAIL(right_ug.get_intra_ttg_load_value(right_server, right_ug_on_right_svr))) {
    LOG_WARN("fail to get right intra ttg load value on right", K(ret));
  } else if (OB_FAIL(right_ug.get_intra_ttg_load_value(left_server, right_ug_on_left_svr))) {
    LOG_WARN("fail to get right intra ttg load value on left", K(ret));
  } else {
    left_after = left_before - left_ug_on_left_svr + right_ug_on_left_svr;
    right_after = right_before - right_ug_on_right_svr + left_ug_on_right_svr;
    if (left_after > right_after) {
      can_exchange = (left_before - right_before > EPSILON)
                     && ((left_before - right_before) - (left_after - right_after) > -EPSILON);
    } else {
      can_exchange = (left_before - right_before > EPSILON)
                     && ((left_before - right_before) - (right_after - left_after) > -EPSILON);
    }
  }
  return ret;
}

bool ObServerBalancer::UnitGroupLoad::is_valid() const
{
  bool bool_ret = OB_INVALID_ID != column_tenant_id_
                  && start_column_idx_ >= 0
                  && column_count_ > 0
                  && unit_loads_.count() > 0
                  && server_.is_valid()
                  && load_sum_.is_valid();
  for (int64_t i = 0; bool_ret && i < unit_loads_.count(); ++i) {
    bool_ret = unit_loads_.at(i).is_valid();
  }
  return bool_ret;
}

void ObServerBalancer::UnitGroupLoad::reset()
{
  column_tenant_id_ = OB_INVALID_ID;
  start_column_idx_ = -1;
  column_count_ = 0;
  server_.reset();
  server_load_ = NULL;
  unit_loads_.reset();
  load_sum_.reset();
}

int ObServerBalancer::UnitGroupLoad::sum_group_load()
{
  int ret = OB_SUCCESS;
  load_sum_.reset();
  for (int64_t j = 0; OB_SUCC(ret) && j < unit_loads_.count(); ++j) {
    const ObUnitConfig &unit_load = unit_loads_.at(j);
    if (!unit_load.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid unit load", K(ret), K(unit_load));
    } else if (OB_FAIL(load_sum_.append_load(unit_load))) {
      LOG_WARN("fail to append load", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObServerBalancer::UnitGroupLoad::get_intra_ttg_load_value(
    const ServerLoad &target_server_load,
    double &intra_ttg_load_value) const
{
  int ret = OB_SUCCESS;
  intra_ttg_load_value = 0.0;
  for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
    ObResourceType res_type = static_cast<ObResourceType>(i);
    if (target_server_load.get_intra_ttg_resource_capacity(res_type) <= 0
        || load_sum_.get_required(res_type) <= 0) {
      // has no effect on load value
    } else if (load_sum_.get_required(res_type) > target_server_load.get_intra_ttg_resource_capacity(res_type)) {
      intra_ttg_load_value += target_server_load.intra_weights_[i] * 1.0;
    } else {
      double quotient
          = load_sum_.get_required(res_type) / target_server_load.get_intra_ttg_resource_capacity(res_type);
      intra_ttg_load_value += target_server_load.intra_weights_[i] * quotient;
    }
  }
  return ret;
}

int ObServerBalancer::UnitGroupLoad::get_inter_ttg_load_value(
    const ServerLoad &target_server_load,
    double &inter_ttg_load_value) const
{
  int ret = OB_SUCCESS;
  inter_ttg_load_value = 0.0;
  for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
    ObResourceType res_type = static_cast<ObResourceType>(i);
    if (target_server_load.get_true_capacity(res_type) <= 0
        || load_sum_.get_required(res_type) <= 0) {
      // has no effect on load value
    } else if (load_sum_.get_required(res_type) > target_server_load.get_true_capacity(res_type)) {
      inter_ttg_load_value += target_server_load.inter_weights_[i] * 1.0;
    } else {
      double quotient
          = load_sum_.get_required(res_type) / target_server_load.get_true_capacity(res_type);
      inter_ttg_load_value += target_server_load.inter_weights_[i] * quotient;
    }
  }
  return ret;
}

int ObServerBalancer::UnitGroupLoadCmp::get_ret() const
{
  return ret_;
}

bool ObServerBalancer::UnitGroupLoadCmp::operator()(
     const UnitGroupLoad *left,
     const UnitGroupLoad *right)
{
  // Sort by load in ascending order
  bool bool_ret = false;
  double left_value = 0.0;
  double right_value = 0.0;
  int &ret = ret_;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // ignore
  } else if (OB_UNLIKELY(NULL == left || NULL == right)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret_), KP(left), K(right));
  } else if (OB_SUCCESS != (ret_ = left->get_intra_ttg_load_value(server_load_, left_value))) {
    LOG_WARN("fail to get left intra load value", K(ret_));
  } else if (OB_SUCCESS != (ret_ = right->get_intra_ttg_load_value(server_load_, right_value))) {
    LOG_WARN("fail to get right intra load value", K(ret_));
  } else if (left_value < right_value) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

bool ObServerBalancer::ServerLoad::is_valid() const
{
  // unitgroup loads can be empty, indicating that there is no load on the server, but server_ must be valid
  bool bool_ret = unitgroup_loads_.count() >= 0 && ServerResourceLoad::is_valid();
  for (int64_t i = 0; bool_ret && i < unitgroup_loads_.count(); ++i) {
    const UnitGroupLoad *ins = unitgroup_loads_.at(i);
    bool_ret = (NULL != ins && ins->is_valid());
  }
  return bool_ret;
}

double ObServerBalancer::ServerResourceLoad::get_true_capacity(const ObResourceType resource_type) const
{
  double ret = -1;
  switch (resource_type) {
  case RES_CPU:
    ret = resource_info_.cpu_;
    break;
  case RES_MEM:
    ret = static_cast<double>(resource_info_.mem_total_);
    break;
  case RES_LOG_DISK:
    ret = static_cast<double>(resource_info_.log_disk_total_);
    break;
  default:
    ret = -1;
    break;
  }
  return ret;
}

double ObServerBalancer::ServerLoad::get_intra_ttg_resource_capacity(
       const ObResourceType resource_type) const
{
  double ret = -1;
  switch (resource_type) {
  case RES_CPU:
    ret = intra_ttg_resource_info_.cpu_;
    break;
  case RES_MEM:
    ret = static_cast<double>(intra_ttg_resource_info_.mem_total_);
    break;
  case RES_LOG_DISK:
    ret = static_cast<double>(intra_ttg_resource_info_.log_disk_total_);
    break;
  default:
    ret = -1;
    break;
  }
  return ret;
}

int ObServerBalancer::ServerLoad::update_load_value()
{
  int ret = OB_SUCCESS;
  intra_ttg_load_value_ = 0.0;
  inter_ttg_load_value_ = 0.0;
  LoadSum intra_load_sum;
  LoadSum inter_load_sum;
  for (int64_t i = 0; OB_SUCC(ret) && i < unitgroup_loads_.count(); ++i) {
    const UnitGroupLoad *unitgroup_load = unitgroup_loads_.at(i);
    if (NULL == unitgroup_load) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unitgroup load null", K(ret), KP(unitgroup_load));
    } else if (OB_FAIL(intra_load_sum.append_load(unitgroup_load->load_sum_))) {
      LOG_WARN("fail to append intra load", K(ret));
    } else if (OB_FAIL(inter_load_sum.append_load(unitgroup_load->load_sum_))) {
      LOG_WARN("fail to append inter load", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < alien_ug_loads_.count(); ++i) {
    const UnitGroupLoad *unitgroup_load = alien_ug_loads_.at(i);
    if (NULL == unitgroup_load) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unitgroup load null", K(ret), KP(unitgroup_load));
    } else if (OB_FAIL(inter_load_sum.append_load(unitgroup_load->load_sum_))) {
      LOG_WARN("fail to append load", K(ret));
    }
  }
  for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
    ObResourceType resource_type = static_cast<ObResourceType>(i);
    const double required = intra_load_sum.get_required(resource_type);
    const double capacity = get_intra_ttg_resource_capacity(resource_type);
    if (required <= 0 || capacity <= 0) {
      // has on effect on load value
    } else if (required > capacity) {
      intra_ttg_load_value_ += intra_weights_[i] * 1.0;
    } else {
      intra_ttg_load_value_ += intra_weights_[i] * (required / capacity);
    }
  }
  for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
    ObResourceType resource_type = static_cast<ObResourceType>(i);
    const double required = inter_load_sum.get_required(resource_type);
    const double capacity = get_true_capacity(resource_type);
    if (required <= 0 || capacity <= 0) {
      // has on effect on load value
    } else if (required > capacity) {
      inter_ttg_load_value_ += inter_weights_[i] * 1.0;
    } else {
      inter_ttg_load_value_ += inter_weights_[i] * (required / capacity);
    }
  }
  return ret;
}

int ObServerBalancer::ServerTotalLoad::update_load_value()
{
  int ret = OB_SUCCESS;
  inter_ttg_load_value_ = 0.0;
  for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
    ObResourceType resource_type = static_cast<ObResourceType>(i);
    const double required = load_sum_.get_required(resource_type);
    const double capacity = get_true_capacity(resource_type);
    if (required <= 0 || capacity <= 0) {
      // has on effect on load value
    } else if (required > capacity) {
      inter_ttg_load_value_ += resource_weights_[i] * 1.0;
    } else {
      inter_ttg_load_value_ += resource_weights_[i] * (required / capacity);
    }
  }
  return ret;
}

bool ObServerBalancer::ServerTotalLoadCmp::operator()(
     const ServerTotalLoad *left,
     const ServerTotalLoad *right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // ignore
  } else if (OB_UNLIKELY(NULL == left || NULL == right)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN_RET(ret_, "invalid argument", K(ret_), KP(left), KP(right));
  } else if (left->wild_server_ && !left->wild_server_) {
    bool_ret = true;
  } else if (!left->wild_server_ && right->wild_server_) {
    bool_ret = false;
  } else {
    if (left->inter_ttg_load_value_ > right->inter_ttg_load_value_) {
      bool_ret = true;
    } else {
      bool_ret = false;
    }
  }
  return bool_ret;
}

int ObServerBalancer::ServerTotalLoadCmp::get_ret() const
{
  return ret_;
}

bool ObServerBalancer::ServerTotalLoadCmp::operator()(
     const ServerTotalLoad &left,
     const ServerTotalLoad &right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // ignore
  } else if (left.wild_server_ && !right.wild_server_) {
    bool_ret = true;
  } else if (!left.wild_server_ && right.wild_server_) {
    bool_ret = false;
  } else {
    if (left.inter_ttg_load_value_ > right.inter_ttg_load_value_) {
      bool_ret = true;
    } else {
      bool_ret = false;
    }
  }
  return bool_ret;
}

bool ObServerBalancer::IntraServerLoadCmp::operator()(
     const ServerLoad *left,
     const ServerLoad *right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // ignore
  } else if (OB_UNLIKELY(NULL == left || NULL == right)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN_RET(ret_, "invalid argument", K(ret_), KP(left), KP(right));
  } else if (left->intra_ttg_load_value_ > right->intra_ttg_load_value_) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

int ObServerBalancer::IntraServerLoadCmp::get_ret() const
{
  return ret_;
}

bool ObServerBalancer::IntraServerLoadCmp::operator()(
     const ServerLoad &left,
     const ServerLoad &right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // ignore
  } else if (left.intra_ttg_load_value_ > right.intra_ttg_load_value_) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

bool ObServerBalancer::InterServerLoadCmp::operator()(
     const ServerLoad *left,
     const ServerLoad *right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // ignore
  } else if (OB_UNLIKELY(NULL == left || NULL == right)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN_RET(ret_, "invalid argument", K(ret_), KP(left), KP(right));
  } else if (left->inter_ttg_load_value_ > right->inter_ttg_load_value_) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

int ObServerBalancer::InterServerLoadCmp::get_ret() const
{
  return ret_;
}

bool ObServerBalancer::InterServerLoadCmp::operator()(
     const ServerLoad &left,
     const ServerLoad &right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // ignore
  } else if (left.inter_ttg_load_value_ > right.inter_ttg_load_value_) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

bool ObServerBalancer::ServerLoadUgCntCmp::operator()(
     const ServerLoad *left,
     const ServerLoad *right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // ignore
  } else if (OB_UNLIKELY(NULL == left || NULL == right)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN_RET(ret_, "invalid argument", K(ret_), KP(left), KP(right));
  } else if (left->unitgroup_loads_.count() < right->unitgroup_loads_.count()) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

int ObServerBalancer::ServerLoadUgCntCmp::get_ret() const
{
  return ret_;
}

bool ObServerBalancer::LoadSum::is_valid() const
{
  return load_sum_.min_cpu() >= 0
         && load_sum_.max_cpu() >= load_sum_.min_cpu()
         && load_sum_.memory_size() >= 0
         && load_sum_.log_disk_size() >= 0;
}

void ObServerBalancer::LoadSum::reset()
{
  load_sum_.reset();
}

double ObServerBalancer::LoadSum::get_required(const ObResourceType resource_type) const
{
  double ret = -1;
  switch (resource_type) {
  case RES_CPU:
    ret = load_sum_.min_cpu();
    break;
  case RES_MEM:
    ret = static_cast<double>(load_sum_.memory_size());
    break;
  case RES_LOG_DISK:
    ret = static_cast<double>(load_sum_.log_disk_size());
    break;
  default:
    ret = -1;
    break;
  }
  return ret;
}

int ObServerBalancer::LoadSum::calc_load_value(
    double *const resource_weights,
    const int64_t weights_count,
    const ServerResourceLoad &server_resource,
    double &load_value)
{
  int ret = OB_SUCCESS;
  if (NULL == resource_weights || RES_MAX != weights_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    load_value = 0.0;
    for (int32_t i = RES_CPU; i < RES_MAX; ++i) {
      ObResourceType res_type = static_cast<ObResourceType>(i);
      if (server_resource.get_true_capacity(res_type) <= 0
          || get_required(res_type) <= 0) {
        // has no effect on load value
      } else if (get_required(res_type) > server_resource.get_true_capacity(res_type)) {
        load_value += resource_weights[i] * 1.0;
      } else {
        double quotient = get_required(res_type) / server_resource.get_true_capacity(res_type);
        load_value += resource_weights[i] * quotient;
      }
    }
  }
  return ret;
}

int ObServerBalancer::LoadSum::remove_load(
    const ObUnitManager::ObUnitLoad &unit_load)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!unit_load.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit_load));
  } else {
    load_sum_ -= *unit_load.unit_config_;
  }
  return ret;
}

int ObServerBalancer::LoadSum::append_load(
    const share::ObUnitConfig &load)
{
  int ret = OB_SUCCESS;
  load_sum_ += load;
  return ret;
}

int ObServerBalancer::LoadSum::append_load(
    const ObUnitManager::ObUnitLoad &unit_load)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!unit_load.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit_load));
  } else {
    load_sum_ += *unit_load.unit_config_;
  }
  return ret;
}

int ObServerBalancer::LoadSum::append_load(
    const common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < unit_loads.count(); ++i) {
    const ObUnitManager::ObUnitLoad &unit_load = unit_loads.at(i);
    if (OB_UNLIKELY(!unit_load.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(unit_load));
    } else {
      load_sum_ += *unit_load.unit_config_;
    }
  }
  return ret;
}

int ObServerBalancer::LoadSum::append_load(
    const ObServerBalancer::LoadSum &load)
{
  int ret = OB_SUCCESS;
  load_sum_ += load.load_sum_;
  return ret;
}

bool ObServerBalancer::ResourceSum::is_valid() const
{
  return resource_sum_.cpu_ > 0
         && resource_sum_.mem_total_ > 0
         && resource_sum_.disk_total_ > 0;
}

void ObServerBalancer::ResourceSum::reset()
{
  resource_sum_.reset();
}

double ObServerBalancer::ResourceSum::get_capacity(const ObResourceType resource_type) const
{
  double ret = -1;
  switch (resource_type) {
  case RES_CPU:
    ret = resource_sum_.cpu_;
    break;
  case RES_MEM:
    ret = static_cast<double>(resource_sum_.mem_total_);
    break;
  case RES_LOG_DISK:
    ret = static_cast<double>(resource_sum_.log_disk_total_);
    break;
  default:
    ret = -1;
    break;
  }
  return ret;
}

int ObServerBalancer::ResourceSum::append_resource(
    const share::ObServerResourceInfo &resource)
{
  int ret = OB_SUCCESS;
  resource_sum_.cpu_ += resource.cpu_;
  resource_sum_.mem_total_ += resource.mem_total_;
  resource_sum_.disk_total_ += resource.disk_total_;
  resource_sum_.log_disk_total_ += resource.log_disk_total_;
  return ret;
}

int ObServerBalancer::ResourceSum::append_resource(
    const ResourceSum &resource)
{
  int ret = OB_SUCCESS;
  resource_sum_.cpu_ += resource.resource_sum_.cpu_;
  resource_sum_.mem_total_ += resource.resource_sum_.mem_total_;
  resource_sum_.disk_total_ += resource.resource_sum_.disk_total_;
  resource_sum_.log_disk_total_ += resource.resource_sum_.log_disk_total_;
  return ret;
}

int ObServerBalancer::TenantGroupBalanceInfo::get_trick_id(
    uint64_t &trick_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == tenant_id_matrix_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant id matrix ptr is null", K(ret), KP(tenant_id_matrix_));
  } else if (tenant_id_matrix_->get_row_count() <= 0
             || tenant_id_matrix_->get_row_count() >= INT32_MAX
             || tenant_id_matrix_->get_column_count() <= 0
             || tenant_id_matrix_->get_column_count() >= INT32_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant id matrix size unexpected", K(ret),
             "row_count", tenant_id_matrix_->get_row_count(),
             "column_count", tenant_id_matrix_->get_column_count());
  } else {
    trick_id = UINT64_MAX;
    for (int64_t row = 0;
         OB_SUCC(ret) && row < tenant_id_matrix_->get_row_count();
         ++row) {
      for (int64_t column = 0;
           OB_SUCC(ret) && column < tenant_id_matrix_->get_column_count();
           ++column) {
        uint64_t my_trick_id = UINT64_MAX;
        if (OB_FAIL(tenant_id_matrix_->get(row, column, my_trick_id))) {
          LOG_WARN("fail to get from tenant id matrix", K(ret), K(row), K(column));
        } else if (my_trick_id < trick_id) {
          trick_id = my_trick_id;
        } else {} // next
      }
    }
  }
  return ret;
}

ObServerBalancer::ServerLoad *ObServerBalancer::TenantGroupBalanceInfo::get_server_load(
    const common::ObAddr &server)
{
  ServerLoad *ret_ptr = NULL;
  for (int64_t i = 0; NULL == ret_ptr && i < server_load_array_.count(); ++i) {
    ServerLoad &server_load = server_load_array_.at(i);
    if (server_load.server_ != server) {
      // go on to check next
    } else {
      ret_ptr = &server_load;
    }
  }
  return ret_ptr;
}

int ObServerBalancer::TenantGroupBalanceInfo::get_all_unit_loads(
    common::ObIArray<ObUnitManager::ObUnitLoad> &unit_loads)
{
  int ret = OB_SUCCESS;
  unit_loads.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < unit_migrate_stat_matrix_.get_row_count(); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < unit_migrate_stat_matrix_.get_column_count(); ++j) {
      UnitMigrateStat *unit_stat = unit_migrate_stat_matrix_.get(i, j);
      if (OB_UNLIKELY(NULL == unit_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit stat ptr is null", K(ret), KP(unit_stat));
      } else if (OB_FAIL(unit_loads.push_back(unit_stat->unit_load_))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // go on next
    }
  }
  return ret;
}

bool ObServerBalancer::TenantGroupBalanceInfo::is_stable() const
{
  bool stable = true;
  for (int64_t i = 0; stable && i < unit_migrate_stat_matrix_.get_row_count(); ++i) {
    for (int64_t j = 0; stable && j < unit_migrate_stat_matrix_.get_column_count(); ++j) {
      const UnitMigrateStat *unit_stat = unit_migrate_stat_matrix_.get(i, j);
      if (NULL == unit_stat) {
        stable = false;
      } else if (unit_stat->original_pos_ != unit_stat->arranged_pos_) {
        stable = false;
      } else {} // go on next
    }
  }
  return stable;
}

int ObServerBalancer::check_tenant_group_config_legality(
    common::ObIArray<ObTenantGroupParser::TenantNameGroup> &tenant_groups,
    bool &legal)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(unit_mgr_->get_lock());  // lock!
  legal = true;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service ptr is null", K(ret), KP(schema_service_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_groups.count() && legal; ++i) {
      const ObTenantGroupParser::TenantNameGroup &tenant_name_group = tenant_groups.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < tenant_name_group.tenants_.count(); ++j) {
        const share::schema::ObTenantSchema *tenant_schema = NULL;
        if (OB_FAIL(schema_guard.get_tenant_info(
                tenant_name_group.tenants_.at(j), tenant_schema))) {
          LOG_WARN("fail to get tenant info", K(ret));
        } else if (OB_UNLIKELY(NULL == tenant_schema)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant schema ptr is null", K(ret), KP(tenant_schema));
        } else if (OB_UNLIKELY(OB_SYS_TENANT_ID == tenant_schema->get_tenant_id())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("sys tenant cannot be configured in tenant groups", K(ret));
        } else {} // good and go on next
      }
    }
  }
  return ret;
}

int ObServerBalancer::get_server_data_disk_usage_limit(
    double &data_disk_usage_limit) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t value = GCONF.data_disk_usage_limit_percentage - 1;
    value = (value <= 0 ? 0 : value);
    data_disk_usage_limit = static_cast<double>(value) / 100;
  }
  return ret;
}

int ObServerBalancer::get_server_balance_critical_disk_waterlevel(
    double &server_balance_critical_disk_waterlevel) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int64_t value = GCONF.server_balance_critical_disk_waterlevel;
    server_balance_critical_disk_waterlevel = static_cast<double>(value) / 100;
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_SERVER_DISK_ASSIGN);

int ObServerBalancer::generate_zone_server_disk_statistic(
    const common::ObZone &zone)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObAddr> server_list;
  double disk_waterlevel = 0.0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else if (OB_ISNULL(server_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_mgr_ ptr is null", KR(ret), KP(server_mgr_));
  } else if (OB_FAIL(SVR_TRACER.get_servers_of_zone(zone, server_list))) {
    LOG_WARN("fail to get servers of zone", KR(ret), K(zone));
  } else if (OB_FAIL(get_server_balance_critical_disk_waterlevel(disk_waterlevel))) {
    LOG_WARN("fail to get server balance disk water level", K(ret));
  } else {
    zone_disk_statistic_.reset();
    zone_disk_statistic_.zone_ = zone;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_list.count(); ++i) {
      const common::ObAddr &server = server_list.at(i);
      share::ObServerResourceInfo server_resource_info;
      share::ObServerInfoInTable server_info;
      ServerDiskStatistic disk_statistic;
      if (OB_FAIL(SVR_TRACER.get_server_info(server, server_info))) {
        LOG_WARN("fail to get server info", KR(ret), K(server));
      } else if (server_info.is_temporary_offline() || server_info.is_stopped()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("server is not stable, stop balance servers in this zone",
            KR(ret), K(server), K(zone),
            "is_temporary_offline", server_info.is_temporary_offline(),
            "is_stopped", server_info.is_stopped());
      } else if (OB_FAIL(server_mgr_->get_server_resource_info(server, server_resource_info))) {
        LOG_WARN("fail to get server resource info", KR(ret), K(server));
      } else if (server_info.is_active()) {
        disk_statistic.server_ = server;
        disk_statistic.wild_server_ = false;
        if (OB_SUCC(ERRSIM_SERVER_DISK_ASSIGN)) {
          disk_statistic.disk_in_use_ = server_resource_info.disk_in_use_;
        } else {
          // ONLY FOR TEST, errsim triggered, make disk_in_use equal to (1GB * unit_num)
          ObArray<ObUnitManager::ObUnitLoad> *unit_loads_ptr;
          if (OB_FAIL(unit_mgr_->get_loads_by_server(server, unit_loads_ptr))) {
            if(OB_ENTRY_NOT_EXIST == ret) {
              ret = OB_SUCCESS; // disk_in_use is 0
            } else {
              LOG_WARN("fail to get_loads_by_server", KR(ret), K(server));
            }
          } else {
            disk_statistic.disk_in_use_ = unit_loads_ptr->count() * (1024 * 1024 * 1024);
          }
          LOG_ERROR("errsim triggered, assign server disk_in_use as unit count * 1GB", KR(ret), K(disk_statistic));
        }
        disk_statistic.disk_total_ = server_resource_info.disk_total_;
        if (static_cast<double>(disk_statistic.disk_in_use_)
            > disk_waterlevel * static_cast<double>(disk_statistic.disk_total_)) {
          zone_disk_statistic_.over_disk_waterlevel_ = true;
        }
      } else if (server_info.is_deleting() || server_info.is_permanent_offline()) {
        disk_statistic.server_ = server;
        disk_statistic.wild_server_ = true;
        disk_statistic.disk_in_use_ = server_resource_info.disk_in_use_;
        disk_statistic.disk_total_ = server_resource_info.disk_total_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknow server_info", K(ret), K(server_info));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(zone_disk_statistic_.append(disk_statistic))) {
        LOG_WARN("fail to append server statistic", K(ret));
      } else {} // no more to do
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("build zone disk statistic succeed", K(ret), K(zone_disk_statistic_));
    }
  }
  return ret;
}

bool ObServerBalancer::ServerDiskStatisticCmp::operator()(
     const ServerDiskStatistic &left,
     const ServerDiskStatistic &right)
{
  bool bool_ret = false;
  if (!left.wild_server_ && right.wild_server_) {
    bool_ret = true;
  } else if (left.wild_server_ && !right.wild_server_) {
    bool_ret = false;
  } else if (left.wild_server_ && right.wild_server_) {
    // whatever, I don't care the sequence of two wild servers
  } else {
    int64_t left_available = left.disk_total_ - left.disk_in_use_;
    int64_t right_available = right.disk_total_ - right.disk_in_use_;
    if (left_available > right_available) {
      bool_ret = true;
    } else {
      bool_ret = false;
    }
  }
  return bool_ret;
}

int ObServerBalancer::ZoneServerDiskStatistic::append(
    ServerDiskStatistic &server_disk_statistic)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(server_disk_statistic_array_.push_back(server_disk_statistic))) {
    LOG_WARN("fail to push back", K(ret));
  } else {
    ServerDiskStatisticCmp cmp;
    std::sort(server_disk_statistic_array_.begin(),
              server_disk_statistic_array_.end(),
              cmp);
  }
  return ret;
}

int ObServerBalancer::ZoneServerDiskStatistic::raise_server_disk_use(
    const common::ObAddr &server,
    const int64_t unit_required_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    bool find = false;
    for (int64_t i = 0;
         OB_SUCC(ret) && !find && i < server_disk_statistic_array_.count();
         ++i) {
      if (server_disk_statistic_array_.at(i).server_ != server) {
        // go on to check next
      } else {
        find = true;
        ServerDiskStatistic &server_disk_statistic = server_disk_statistic_array_.at(i);
        server_disk_statistic.disk_in_use_ += unit_required_size;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("server not exist", K(ret), K(server));
    } else {
      ServerDiskStatisticCmp cmp;
      std::sort(server_disk_statistic_array_.begin(),
                server_disk_statistic_array_.end(),
                cmp);
    }
  }
  return ret;
}

int ObServerBalancer::ZoneServerDiskStatistic::reduce_server_disk_use(
    const common::ObAddr &server,
    const int64_t unit_required_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    bool find = false;
    for (int64_t i = 0;
         OB_SUCC(ret) && !find && i < server_disk_statistic_array_.count();
         ++i) {
      if (server_disk_statistic_array_.at(i).server_ != server) {
        // go on to check next
      } else {
        find = true;
        ServerDiskStatistic &server_disk_statistic = server_disk_statistic_array_.at(i);
        server_disk_statistic.disk_in_use_ -= unit_required_size;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("server not exist", K(ret), K(server));
    } else {
      ServerDiskStatisticCmp cmp;
      std::sort(server_disk_statistic_array_.begin(),
                server_disk_statistic_array_.end(),
                cmp);
    }
  }
  return ret;
}

int ObServerBalancer::ZoneServerDiskStatistic::get_server_disk_statistic(
    const common::ObAddr &server,
    ServerDiskStatistic &server_disk_statistic)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    server_disk_statistic.reset();
    bool find = false;
    for (int64_t i = 0;
         OB_SUCC(ret) && !find && i < server_disk_statistic_array_.count();
         ++i) {
      if (server_disk_statistic_array_.at(i).server_ != server) {
        // go on to check next
      } else {
        find = true;
        server_disk_statistic = server_disk_statistic_array_.at(i);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObServerBalancer::ZoneServerDiskStatistic::check_server_over_disk_waterlevel(
    const common::ObAddr &server,
    const double disk_waterlevel,
    bool &disk_over_waterlevel)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    bool find = false;
    for (int64_t i = 0; !find && i < server_disk_statistic_array_.count(); ++i) {
      ServerDiskStatistic &disk_statistic = server_disk_statistic_array_.at(i);
      if (server != disk_statistic.server_) {
        // go on to check next
      } else {
        find = true;
        disk_over_waterlevel
            = (static_cast<double>(disk_statistic.disk_in_use_)
               > disk_waterlevel * static_cast<double>(disk_statistic.disk_total_));
      }
    }
    if (!find) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("server not exist", K(ret));
    }
  }
  return ret;
}

int ObServerBalancer::ZoneServerDiskStatistic::check_all_available_servers_over_disk_waterlevel(
    const double disk_waterlevel,
    bool &all_available_servers_over_disk_waterlevel)
{
  int ret = OB_SUCCESS;
  all_available_servers_over_disk_waterlevel = true;
  for (int64_t i = 0;
       all_available_servers_over_disk_waterlevel
       && OB_SUCC(ret)
       && i < server_disk_statistic_array_.count();
       ++i) {
    ServerDiskStatistic &disk_statistic = server_disk_statistic_array_.at(i);
    all_available_servers_over_disk_waterlevel
        = (static_cast<double>(disk_statistic.disk_in_use_)
           > disk_waterlevel * static_cast<double>(disk_statistic.disk_total_));

    LOG_INFO("check server over disk waterlevel",
        K_(zone), K(disk_statistic),
        "over_disk_waterlevel", all_available_servers_over_disk_waterlevel);
  }
  LOG_INFO("check all available servers over disk waterlevel",
      K_(zone),
      K(all_available_servers_over_disk_waterlevel),
      "zone_server_over_disk_waterlevel", over_disk_waterlevel_,
      K_(server_disk_statistic_array));
  return ret;
}

bool ObServerBalancer::ServerDiskPercentCmp::operator()(
     const ServerDiskStatistic *left,
     const ServerDiskStatistic *right)
{
  // Sort by the percentage of disk usage from highest to bottom
  bool bool_ret = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // ignore
  } else if (OB_UNLIKELY(NULL == left || NULL == right)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN_RET(ret_,"invalid argument", K(ret_), KP(left), KP(right));
  } else {
    double left_percent = left->get_disk_used_percent();
    double right_percent = right->get_disk_used_percent();
    if (left_percent > right_percent) {
      bool_ret = true;
    } else {
      bool_ret = false;
    }
  }
  return bool_ret;
}

bool ObServerBalancer::UnitLoadDiskCmp::operator()(
     const ObUnitManager::ObUnitLoad *left,
     const ObUnitManager::ObUnitLoad *right)
{
  bool bool_ret = false;
  ObUnitStat left_stat;
  ObUnitStat right_stat;
  int &ret = ret_;
  if (common::OB_SUCCESS != ret_) {
    // bypass
  } else if (NULL == left || NULL == right) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN("unit load ptr is null", K(ret_), KP(left), KP(right));
  } else if (!left->is_valid() || !right->is_valid()) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN("unit load unexpected", K(ret_), K(*left), K(*right));
  } else if (OB_SUCCESS != (ret_ = unit_stat_mgr_.get_unit_stat(
          left->unit_->unit_id_, left->unit_->zone_, left_stat))) {
    LOG_WARN("fail to get left unit stat", K(ret_));
  } else if (OB_SUCCESS != (ret_ = unit_stat_mgr_.get_unit_stat(
          right->unit_->unit_id_, right->unit_->zone_, right_stat))) {
    LOG_WARN("fail to get right unit stat", K(ret_));
  } else if (left_stat.get_required_size() > right_stat.get_required_size()) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

