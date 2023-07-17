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
#include "ob_migrate_unit_finish_checker.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "share/ls/ob_ls_status_operator.h"
#include "share/ls/ob_ls_table_operator.h"
#include "ob_unit_manager.h"
#include "ob_zone_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;

ObMigrateUnitFinishChecker::ObMigrateUnitFinishChecker(volatile bool &stop)
    : inited_(false),
      unit_mgr_(nullptr),
      zone_mgr_(nullptr),
      schema_service_(nullptr),
      sql_proxy_(nullptr),
      lst_operator_(nullptr),
      stop_(stop)
{
}

ObMigrateUnitFinishChecker::~ObMigrateUnitFinishChecker()
{
}

int ObMigrateUnitFinishChecker::check_stop() const
{
  int ret = OB_SUCCESS;
  if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("ObMigrateUnitFinishChecker stopped", KR(ret), K(stop_));
  }
  return ret;
}

int ObMigrateUnitFinishChecker::init(
    ObUnitManager &unit_mgr,
    ObZoneManager &zone_mgr,
    share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy,
    share::ObLSTableOperator &lst_operator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    unit_mgr_ = &unit_mgr;
    zone_mgr_ = &zone_mgr;
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    lst_operator_ = &lst_operator;
    inited_ = true;
  }
  return ret;
}

int ObMigrateUnitFinishChecker::check()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("start check unit migrate finish");
  ObArray<uint64_t> tenant_id_array;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("ObMigrateUnitFinishChecker stopped", KR(ret));
  } else if (OB_UNLIKELY(ObTenantUtils::get_tenant_ids(schema_service_, tenant_id_array))) {
    LOG_WARN("fail to get tenant id array", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_id_array.count(); ++i) {
      const uint64_t tenant_id = tenant_id_array.at(i);
      if (is_meta_tenant(tenant_id)) {
        // bypass
      } else {
        // check unit belongs to existed tenant
        if (OB_SUCCESS != (tmp_ret = try_check_migrate_unit_finish_by_tenant(tenant_id))) {
          LOG_WARN("fail to try check migrate unit finish by tenant", KR(tmp_ret), K(tenant_id));
        }
        // check unit not in locality but in zone list
        if (OB_SUCCESS != (tmp_ret = try_check_migrate_unit_finish_not_in_locality(tenant_id))) {
          LOG_WARN("fail to try check migrate unit finish not in locality", KR(tmp_ret), K(tenant_id));
        }
      }
    }
  }
  // check unit not in tenant
  if (OB_SUCCESS != (tmp_ret = try_check_migrate_unit_finish_not_in_tenant())) {
    LOG_WARN("fail to try check migrate unit finish not in tenant", KR(tmp_ret));
  }
  return ret;
}

int ObMigrateUnitFinishChecker::try_check_migrate_unit_finish_not_in_locality(
    const uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<common::ObZone> zone_list;
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("ObMigrateUnitFinishChecker stopped", KR(ret));
  } else if (OB_FAIL(unit_mgr_->get_tenant_pool_zone_list(tenant_id, zone_list))) {
    LOG_WARN("fail to get tenant pool zone list", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(unit_mgr_->finish_migrate_unit_not_in_locality(
             tenant_id, &schema_guard, zone_list))) {
    LOG_WARN("fail to finish migrat eunit not in locality", KR(ret), K(tenant_id), K(zone_list));
  }
  return ret;
}

int ObMigrateUnitFinishChecker::try_check_migrate_unit_finish_not_in_tenant()
{
  int ret = OB_SUCCESS;
  ObArray<share::ObResourcePool> pools;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("ObMigrateUnitFinishChecker stopped", KR(ret));
  } else if (OB_FAIL(unit_mgr_->get_pools(pools))) {
    LOG_WARN("fail to get pools", KR(ret));
  } else {
    FOREACH_CNT_X(pool, pools, OB_SUCC(ret)) {
      if (OB_FAIL(unit_mgr_->finish_migrate_unit_not_in_tenant(pool))) {
        LOG_WARN("fail to finish migrate unit not in tenant", KR(ret));
      }
      ret = OB_SUCCESS; //ignore ret 保证所有的pool都能运行
    }
  }
  return ret;
}

int ObMigrateUnitFinishChecker::try_check_migrate_unit_finish_by_tenant(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("ObMigrateUnitFinishChecker stopped", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    LOG_INFO("try check migrate unit finish by tenant", K(tenant_id));
    DRLSInfo dr_ls_info(gen_user_tenant_id(tenant_id),
                        unit_mgr_,
                        zone_mgr_,
                        schema_service_);
    ObLSStatusInfoArray ls_status_info_array;
    share::ObLSStatusOperator ls_status_operator;
    if (OB_FAIL(ls_status_operator.get_all_tenant_related_ls_status_info(
      *sql_proxy_, tenant_id, ls_status_info_array))) {
      LOG_WARN("fail to get all ls status", KR(ret), K(tenant_id));
    } else if (OB_FAIL(dr_ls_info.init())) {
      LOG_WARN("fail to init disaster log stream info", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_status_info_array.count(); ++i) {
        share::ObLSInfo ls_info;
        share::ObLSStatusInfo &ls_status_info = ls_status_info_array.at(i);
        if (OB_FAIL(check_stop())) {
          LOG_WARN("DRWorker stopped", KR(ret));
        } else if (OB_FAIL(lst_operator_->get(
                GCONF.cluster_id,
                ls_status_info.tenant_id_,
                ls_status_info.ls_id_,
                share::ObLSTable::COMPOSITE_MODE,
                ls_info))) {
          LOG_WARN("fail to get log stream info", KR(ret));
        } else if (OB_FAIL(dr_ls_info.build_disaster_ls_info(
                ls_info,
                ls_status_info,
                true/*filter_readonly_replicas_with_flag*/))) {
          LOG_WARN("fail to generate dr log stream info", KR(ret));
        } else if (OB_FAIL(statistic_migrate_unit_by_ls(
                dr_ls_info,
                ls_status_info))) {
          LOG_WARN("fail to try log stream disaster recovery", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(try_finish_migrate_unit(
              dr_ls_info.get_unit_stat_info_map()))) {
        LOG_WARN("fail to try finish migrate unit", KR(ret));
      }
    }
  }
  return ret;
}

int ObMigrateUnitFinishChecker::statistic_migrate_unit_by_ls(
    DRLSInfo &dr_ls_info,
    share::ObLSStatusInfo &ls_status_info)
{
  int ret = OB_SUCCESS;
  int64_t ls_replica_cnt = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("ObMigrateUnitFinishChecker stopped", KR(ret));
  } else if (OB_FAIL(dr_ls_info.get_replica_cnt(ls_replica_cnt))) {
    LOG_WARN("fail to get replica cnt", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_replica_cnt; ++i) {
      share::ObLSReplica *ls_replica = nullptr;
      DRServerStatInfo *server_stat_info = nullptr;
      DRUnitStatInfo *unit_stat_info = nullptr;
      DRUnitStatInfo *unit_in_group_stat_info = nullptr;
      if (OB_FAIL(dr_ls_info.get_replica_stat(
              i,
              ls_replica,
              server_stat_info,
              unit_stat_info,
              unit_in_group_stat_info))) {
        LOG_WARN("fail to get replica stat", KR(ret));
      } else if (OB_UNLIKELY(nullptr == ls_replica
                             || nullptr == server_stat_info
                             || nullptr == unit_stat_info
                             || nullptr == unit_in_group_stat_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica stat unexpected", KR(ret),
                 KP(ls_replica),
                 KP(server_stat_info),
                 KP(unit_stat_info),
                 KP(unit_in_group_stat_info));
      } else if (server_stat_info->get_server() != unit_stat_info->get_unit_info().unit_.server_
          && (ls_replica->is_in_service() || ls_status_info.ls_is_creating())) {
        unit_stat_info->inc_outside_replica_cnt();
        if (unit_stat_info->get_outside_replica_cnt() <= 2) { // print the first two outside replica
          LOG_INFO("outside replica", KPC(ls_replica), "unit", unit_stat_info->get_unit_info().unit_);
        }
      }
    }
  }
  return ret;
}

int ObMigrateUnitFinishChecker::try_finish_migrate_unit(
    const UnitStatInfoMap &unit_stat_info_map)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMigrateUnitFinishChecker not init", KR(ret));
  } else if (OB_UNLIKELY(nullptr == unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit mgr ptr is null", KR(ret), KP(unit_mgr_));
  } else {
    const UnitStatInfoMap::HashTable &inner_hash_table = unit_stat_info_map.get_hash_table();
    UnitStatInfoMap::HashTable::const_iterator iter = inner_hash_table.begin();
    for (; OB_SUCC(ret) && iter != inner_hash_table.end(); ++iter) {
      const DRUnitStatInfo &unit_stat_info = iter->v_;
      if (unit_stat_info.is_in_pool()
          && unit_stat_info.get_unit_info().unit_.migrate_from_server_.is_valid()
          && 0 == unit_stat_info.get_outside_replica_cnt()) {
        if (OB_FAIL(unit_mgr_->finish_migrate_unit(
                unit_stat_info.get_unit_info().unit_.unit_id_))) {
          LOG_WARN("fail to set unit migrate finish", KR(ret),
                   "unit_id", unit_stat_info.get_unit_info().unit_.unit_id_);
        }
      }
    }
  }
  return ret;
}
