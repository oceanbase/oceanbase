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

#include "rootserver/ob_unit_stat_manager.h"
#include "rootserver/ob_balance_info.h"
#include "share/ob_unit_getter.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "rootserver/ob_unit_manager.h"
#include "storage/ob_file_system_router.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;

ObUnitStatManager::ObUnitStatManager()
  : inited_(false),
    schema_service_(NULL),
    unit_mgr_(NULL),
    unit_stat_getter_(),
    unit_stat_map_()
{
}

int ObUnitStatManager::init(
    share::schema::ObMultiVersionSchemaService &schema_service,
    ObUnitManager &unit_mgr,
    share::ObCheckStopProvider &check_stop_provider)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(inited_));
  } else if (OB_FAIL(unit_stat_getter_.init(check_stop_provider))) {
    LOG_WARN("init unit stat getter fail", KR(ret));
  } else if (OB_FAIL(unit_stat_map_.init(500000))) { /// FIXME: use more accurate CONSTANT
    LOG_WARN("fail init unit stat map", K(ret));
  } else {
    schema_service_ = &schema_service;
    unit_mgr_ = &unit_mgr;
    inited_ = true;
  }
  return ret;
}

void ObUnitStatManager::reuse()
{
  unit_stat_map_.reuse();
}

int ObUnitStatManager::gather_stat()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  ObArray<uint64_t> unit_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tenant_ids))) {
    LOG_WARN("get tenant ids fail", K(ret));
  } else {
    reuse();
    // init empty map with all unit ids
    if (OB_FAIL(unit_mgr_->get_unit_ids(unit_ids))) {
      LOG_WARN("fail get unit ids", K(ret));
    } else {
      ObUnitStatMap::Item *stat = NULL;
      FOREACH_CNT_X(unit_id, unit_ids, OB_SUCC(ret)) {
        if (OB_FAIL(unit_stat_map_.locate(*unit_id, stat))) {
          LOG_WARN("fail init stat for unit", K(*unit_id), K(ret));
        }
      }
    }

    // walk through all meta tables to gather unit stat data
    FOREACH_CNT_X(id, tenant_ids, OB_SUCC(ret)) {
      const uint64_t tenant_id = *id;
      if (OB_FAIL(unit_stat_getter_.get_unit_stat(tenant_id, unit_stat_map_))) {
        LOG_WARN("fail get unit stat", K(tenant_id), K(ret));
      }
    }
  }
  return ret;
}

int ObUnitStatManager::get_unit_stat(
    uint64_t unit_id,
    const common::ObZone &zone,
    share::ObUnitStat &unit_stat)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int tmp_ret = unit_stat_map_.get(unit_id, unit_stat);
    if (OB_SUCCESS == tmp_ret) {
      // good
    } else {
      ret = tmp_ret;
      LOG_WARN("fail to get unit stat", KR(ret), K(unit_id), K(zone));
    }
  }
  return ret;
}


