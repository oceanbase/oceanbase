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

#include "ob_shrink_resource_pool_checker.h"
#include "lib/container/ob_array.h"
#include "ob_unit_manager.h"
#include "ob_root_utils.h"//get_tenant_id
#include "ob_disaster_recovery_info.h"//DRLSInfo
#include "share/ls/ob_ls_status_operator.h"//ObLSStatusInfo
#include "share/ls/ob_ls_table_operator.h"//ls_operator_ ->get
#include "share/ls/ob_ls_info.h"//ObLSInfo


namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver
{

int ObShrinkResourcePoolChecker::check_stop() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", KR(ret), K(is_inited_));
  } else if (is_stop_) {
    ret = OB_CANCELED;
    LOG_WARN("ObShrinkResourcePoolChecker stopped", KR(ret), K(is_stop_));
  } else {} // do nothing
  return ret;
}

int ObShrinkResourcePoolChecker::init(
    share::schema::ObMultiVersionSchemaService *schema_service,
    rootserver::ObUnitManager *unit_mgr,
    share::ObLSTableOperator &lst_operator,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObShrinkResourcePoolChecker not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(schema_service)
             || OB_ISNULL(unit_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(schema_service), KP(unit_mgr));
  } else {
    schema_service_ = schema_service;
    unit_mgr_ = unit_mgr;
    lst_operator_ = &lst_operator;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObShrinkResourcePoolChecker::check()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start check shrink resource pool");
  ObArray<uint64_t> tenant_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("ObShrinkResourcePoolChecker stop", KR(ret));
  } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tenant_ids))) {
    LOG_WARN("fail to get tenant id array", KR(ret));
  } else {
    DEBUG_SYNC(BEFORE_CHECK_SHRINK_RESOURCE_POOL);
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (!is_valid_tenant_id(tenant_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
      } else if (is_meta_tenant(tenant_id)) {
        //nothing TODO
      } else {
        LOG_INFO("start check shrink resource pool", K(tenant_id));
        if (OB_TMP_FAIL(check_shrink_resource_pool_finished_by_tenant_(tenant_id))) {
          LOG_WARN("fail to check shrink resource pool finish", KR(ret), KR(tmp_ret), K(tenant_id));
        } else {} // no more to do
      }
    }
  }
  LOG_INFO("finish check shrink resource pool", KR(ret));
  return ret;
}

int ObShrinkResourcePoolChecker::check_shrink_resource_pool_finished_by_tenant_(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> pool_ids;
  bool in_shrinking = true;
  bool is_finished = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("ObShrinkResourcePoolChecker stop", KR(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(sql_proxy_), KP(unit_mgr_));
  } else if (OB_FAIL(unit_mgr_->get_pool_ids_of_tenant(tenant_id, pool_ids))) {
    LOG_WARN("fail to get resource pools", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(0 == pool_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get tenant resource pool", KR(ret), K(tenant_id));
  } else if (OB_FAIL(unit_mgr_->check_pool_in_shrinking(pool_ids.at(0), in_shrinking))) {
    LOG_WARN("failed to check resource pool in shrink", KR(ret), K(pool_ids));
  } else if (!in_shrinking) {
    //nothing todo
  } else {
    //check shrink finish
    //get all unit and server
    //check ls not in the unit and ls_meta not in the server
    ObArray<share::ObUnit> units;
    ObArray<common::ObAddr> servers;
    ObArray<uint64_t> unit_ids;
    ObArray<uint64_t> unit_group_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
      units.reset();
      if (OB_FAIL(unit_mgr_->get_deleting_units_of_pool(pool_ids.at(i), units))) {
        LOG_WARN("failed to get deleting unit", KR(ret), K(i), K(pool_ids));
      } else if (OB_FAIL(extract_units_servers_and_ids_(units, servers, unit_ids, unit_group_ids))) {
        LOG_WARN("failed to extract units server and ids", KR(ret), K(units));
      }
    }//end for get all unit group, units, server
    if (FAILEDx(check_shrink_resource_pool_finished_by_ls_(tenant_id,
                servers, unit_ids, unit_group_ids, is_finished))) {
      LOG_WARN("failed to check shrink by ls", KR(ret), K(servers), K(unit_ids), K(unit_group_ids));
    }
    if (OB_SUCC(ret) && is_finished) {
      //commit finish of the tenant
      if (OB_FAIL(commit_tenant_shrink_resource_pool_(tenant_id))) {
        LOG_WARN("failed to shrink tenant resource pool", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObShrinkResourcePoolChecker::extract_units_servers_and_ids_(
    const ObIArray<share::ObUnit> &units,
    ObIArray<common::ObAddr> &servers,
    ObIArray<uint64_t> &unit_ids,
    ObIArray<uint64_t> &unit_group_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == units.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("units is empty", KR(ret), K(units));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); ++i) {
    if (OB_FAIL(servers.push_back(units.at(i).server_))) {
      LOG_WARN("fail to push back server", KR(ret), "server", units.at(i).server_);
    } else if (OB_FAIL(unit_ids.push_back(units.at(i).unit_id_))) {
      LOG_WARN("fail to push back unit id", KR(ret), "unit", units.at(i).unit_id_);
    } else if (!has_exist_in_array(unit_group_ids, units.at(i).unit_group_id_)) {
      if (OB_FAIL(unit_group_ids.push_back(units.at(i).unit_group_id_))) {
        LOG_WARN("failed to push back unit group id", KR(ret), K(i), K(units));
      }
    } else {} // no more to do
  }
  return ret;
}

int ObShrinkResourcePoolChecker::check_shrink_resource_pool_finished_by_ls_(
    const uint64_t tenant_id,
    const ObIArray<common::ObAddr> &servers,
    const ObIArray<uint64_t> &unit_ids,
    const ObIArray<uint64_t> &unit_group_ids,
    bool &is_finished)
{
  int ret = OB_SUCCESS;
  is_finished = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
             || 0 == servers.count() || 0 == unit_ids.count()
             || 0 == unit_group_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(servers), K(unit_ids), K(unit_group_ids));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("ObShrinkResourcePoolChecker stop", KR(ret));
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(unit_mgr_) || OB_ISNULL(lst_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(sql_proxy_), KP(unit_mgr_), KP(lst_operator_));
  } else {
    ObLSStatusInfoArray ls_status_array;
    ObLSStatusOperator ls_status_op;
    if (OB_FAIL(ls_status_op.get_all_tenant_related_ls_status_info(
                *sql_proxy_, tenant_id, ls_status_array))) {
      LOG_WARN("failed to get ls status array", KR(ret), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_status_array.count() && is_finished; ++i) {
      share::ObLSStatusInfo &ls_status_info = ls_status_array.at(i);
      share::ObLSInfo ls_info;
      int64_t ls_replica_cnt = 0;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("ObShrinkResourcePoolChecker stopped", KR(ret));
      } else if (has_exist_in_array(unit_group_ids, ls_status_info.unit_group_id_)) {
        is_finished = false;
        LOG_INFO("has ls in the unit group", KR(ret), K(ls_status_info));
      } else if (OB_FAIL(lst_operator_->get(
                GCONF.cluster_id,
                ls_status_info.tenant_id_,
                ls_status_info.ls_id_,
                share::ObLSTable::COMPOSITE_MODE,
                ls_info))) {
          LOG_WARN("fail to get ls info", KR(ret), K(ls_status_info));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < ls_info.get_replicas_cnt() && is_finished; ++j) {
          const share::ObLSReplica &ls_replica = ls_info.get_replicas().at(j);
          if (has_exist_in_array(servers, ls_replica.get_server())) {
            is_finished = false;
            LOG_INFO("has ls in the server", KR(ret), K(ls_replica));
          } else if (has_exist_in_array(unit_ids, ls_replica.get_unit_id())) {
            is_finished = false;
            LOG_INFO("has ls in the unit", KR(ret), K(ls_replica));
          }
        }//end for each ls replica
      }
    }//end for each ls
  }
  return ret;
}

int ObShrinkResourcePoolChecker::commit_tenant_shrink_resource_pool_(
      const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObShrinkResourcePoolChecker not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("ObShrinkResourcePoolChecker stop", KR(ret));
  } else if (OB_ISNULL(unit_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(unit_mgr_));
  } else if (OB_FAIL(unit_mgr_->commit_shrink_tenant_resource_pool(tenant_id))) {
    LOG_WARN("fail to shrink resource pool", KR(ret), K(tenant_id));
  } else {} // no more to do
  return ret;
}
} // end namespace rootserver
} // end oceanbase
