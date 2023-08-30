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
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;

ObUnitStatManager::ObUnitStatManager()
  : inited_(false),
    loaded_stats_(false),
    unit_stat_map_("UnitStatMgr")
{
}

int ObUnitStatManager::init(common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(inited_));
  } else if (OB_FAIL(unit_stat_map_.init(1000))) { /// FIXME: use more accurate CONSTANT
    LOG_WARN("fail init unit stat map", KR(ret));
  } else if (OB_FAIL(ut_operator_.init(sql_proxy))) {
    LOG_WARN("fail to init ut_operator_", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObUnitStatManager::reuse()
{
  unit_stat_map_.reuse();
  loaded_stats_ = false;
}

ERRSIM_POINT_DEF(ERRSIM_UNIT_DISK_ASSIGN);

int ObUnitStatManager::gather_stat()
{
  int ret = OB_SUCCESS;
  ObArray<share::ObUnitStat> unit_stats;
  ObArray<share::ObUnit> units;
  unit_stat_map_.reuse();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ut_operator_.get_unit_stats(unit_stats))) {
    LOG_WARN("get unit_stats failed", KR(ret));
  } else if (OB_FAIL(ut_operator_.get_units(units))) {
    LOG_WARN("get units failed", KR(ret));
  } else {
    ObUnitStatMap::Item *item = NULL;
    FOREACH_CNT_X(unit_stat, unit_stats, OB_SUCC(ret)) {
      if (OB_FAIL(unit_stat_map_.locate(unit_stat->get_unit_id(), item))) {
        LOG_WARN("fail to locate unit_stat", KR(ret), K(unit_stat->get_unit_id()));
      } else if (OB_FAIL(ERRSIM_UNIT_DISK_ASSIGN)) {
        if (OB_FAIL(item->v_.init(unit_stat->get_unit_id(),
                                  1024 * 1024 * 1024,  // assign 1 GB for test use only
                                  unit_stat->get_is_migrating()))) {
          LOG_WARN("fail to init unit_stat", KR(ret), K(unit_stat->get_unit_id()));
        } else {
          ret = OB_SUCCESS;
        }
        LOG_ERROR("errsim triggered, assign unit_stats' required size to 1GB.", KR(ret), KP(item));
      } else {
        item->v_.deep_copy(*unit_stat);
      }
    }
    // FIXME: (cangming.zl) temp workaround for problem of units on offline servers.
    FOREACH_CNT_X(unit, units, OB_SUCC(ret)) {
      share::ObUnitStat tmp_unit_stat;
      if (OB_SUCC(unit_stat_map_.get(unit->unit_id_, tmp_unit_stat))) {
        // do nothing, unit_stat of this unit is already set
      } else {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to try to get from unit_stat_map", KR(ret), K(unit->unit_id_));
        } else { /*OB_HASH_NOT_EXIST*/
          // Let required_size of unit_stat unable to get from __all_virtual_unit be 0
          ret = OB_SUCCESS;
          if (OB_FAIL(unit_stat_map_.locate(unit->unit_id_, item))) {
            LOG_WARN("fail to locate unit_stat", KR(ret), K(unit->unit_id_));
          } else {
            if (OB_ISNULL(item)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("item is nullptr", KR(ret));
            } else if (OB_FAIL(item->v_.init(unit->unit_id_,
                                      0 /*required_size*/,
                                      unit->migrate_from_server_.is_valid()/*is_migrating*/))) {
              LOG_WARN("fail to init unit_stat", KR(ret), K(unit->unit_id_));
            } else {
              LOG_INFO("unit not in unit_stat_map, set required_size as 0", KR(ret), K(unit->unit_id_));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    loaded_stats_ = true;
  } else {
    unit_stat_map_.reuse();
    loaded_stats_ = false;
  }
  return ret;
}

int ObUnitStatManager::get_unit_stat(
    uint64_t unit_id,
    const common::ObZone &zone,
    share::ObUnitStat &unit_stat)
{
  int ret = OB_SUCCESS;
  ObUnitStat tmp_unit_stat;
  if (!inited_ || !loaded_stats_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("not init or not loaded", KR(ret), K(inited_), K(loaded_stats_));
  } else if (OB_FAIL(unit_stat_map_.get(unit_id, tmp_unit_stat))) {
    LOG_WARN("fail to get unit stat", KR(ret), K(unit_id), K(zone));
  } else if (tmp_unit_stat.get_is_migrating()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("unit is migrating, disk statistic not accurate, not allowed to use", KR(ret), K(unit_id), K(zone));
  } else {
    unit_stat = tmp_unit_stat;
  }
  return ret;
}
