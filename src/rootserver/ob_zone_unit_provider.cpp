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
#include "ob_zone_unit_provider.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace rootserver
{

const ObUnitInfo *ObAliveZoneUnitAdaptor::at(int64_t idx) const
{
  ObUnitInfo *info = NULL;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == zu_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("unexpected null zu_. bad code.");
  } else if (OB_UNLIKELY(idx < 0 || idx >= zu_->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("unexpected idx", K(idx), "count", zu_->count(), K(ret));
  } else {
    info = zu_->at(idx);
  }
  return info;
}

int64_t ObAliveZoneUnitAdaptor::count() const
{
  int64_t cnt = 0;
  if (OB_UNLIKELY(NULL == zu_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected null zu_");
  } else {
    cnt = zu_->count();
  }
  return cnt;
}

int ObAliveZoneUnitAdaptor::get_target_unit_idx(
    const int64_t unit_offset,
    common::hash::ObHashSet<int64_t> &unit_set,
    const bool is_primary_partition,
    int64_t &unit_idx) const
{
  int ret = OB_SUCCESS;
  UNUSED(is_primary_partition);
  if (count() <= 0) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_WARN("no available unit to alloc replica", K(ret));
  } else {
    int64_t idx = unit_offset % count();
    const int64_t guard = idx;
    do {
      ret = unit_set.exist_refactored(at(idx)->unit_.unit_id_);
      if (OB_HASH_EXIST == ret) {
        idx++;
        idx %= count();
      }
    } while (OB_HASH_EXIST == ret && idx != guard);
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      unit_idx = idx;
    } else if (OB_HASH_EXIST == ret) {
      ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    } else {
      LOG_WARN("fail to alloc replica", K(ret));
    }
  }
  return ret;
}

int ObAliveZoneUnitAdaptor::update_tg_pg_count(
    const int64_t unit_idx,
    const bool is_primary_partition)
{
  UNUSED(unit_idx);
  UNUSED(is_primary_partition);
  return OB_SUCCESS;
}

bool ObAliveZoneUnitsProvider::UnitSortOp::operator()(
     share::ObUnitInfo *left,
     share::ObUnitInfo *right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(common::OB_SUCCESS != ret_)) {
    // jump out
  } else if (OB_UNLIKELY(nullptr == left || nullptr == right)) {
    ret_ = common::OB_ERR_UNEXPECTED;
    LOG_WARN_RET(ret_, "left or right ptr is null", K(ret_), KP(left), KP(right));
  } else if (left->unit_.unit_id_ < right->unit_.unit_id_) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

bool ObAliveZoneUnitsProvider::ZoneUnitSortOp::operator()(
     UnitPtrArray &left,
     UnitPtrArray &right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(common::OB_SUCCESS != ret_)) {
    // jump out
  } else if (left.count() <= 0 || right.count() <= 0) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN_RET(ret_, "left or right unit array empty", K(ret_),
             "left_count", left.count(), "right_count", right.count());
  } else if (nullptr == left.at(0) || nullptr == right.at(0)) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN_RET(ret_, "unit ptr is null", K(ret_), "left_ptr", left.at(0), "right_ptr", right.at(0));
  } else if (left.at(0)->unit_.zone_ < right.at(0)->unit_.zone_) {
    bool_ret = true;
  } else {
    bool_ret = false;
  }
  return bool_ret;
}

int ObAliveZoneUnitsProvider::init(
    const ZoneUnitPtrArray &all_zone_units)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    UnitPtrArray unit_ptr_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_zone_units.count(); ++i) {
      const UnitPtrArray &unit_array = all_zone_units.at(i);
      unit_ptr_array.reuse();
      for (int64_t j = 0; OB_SUCC(ret) && j < unit_array.count(); ++j) {
        share::ObUnitInfo *unit_info = unit_array.at(j);
        if (OB_UNLIKELY(nullptr == unit_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit info ptr is null", K(ret));
        } else if (OB_FAIL(unit_ptr_array.push_back(unit_info))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        UnitSortOp unit_sort_operator;
        lib::ob_sort(unit_ptr_array.begin(), unit_ptr_array.end(), unit_sort_operator);
        if (OB_FAIL(unit_sort_operator.get_ret())) {
          LOG_WARN("fail to sort unit in zone", K(ret));
        } else if (OB_FAIL(all_zone_unit_ptrs_.push_back(unit_ptr_array))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ZoneUnitSortOp zone_unit_sort_operator;
      lib::ob_sort(all_zone_unit_ptrs_.begin(), all_zone_unit_ptrs_.end(), zone_unit_sort_operator);
      if (OB_FAIL(zone_unit_sort_operator.get_ret())) {
        LOG_WARN("fail to sort zone unit", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  LOG_INFO("alive zone unit provider init", K(ret), K(all_zone_unit_ptrs_), K(all_zone_units));
  return ret;
}

int ObAliveZoneUnitsProvider::prepare_for_next_partition(
    const common::hash::ObHashSet<int64_t> &unit_set)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    available_zone_unit_ptrs_.reset();
    UnitPtrArray unit_ptr_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_zone_unit_ptrs_.count(); ++i) {
      unit_ptr_array.reuse();
      const UnitPtrArray &this_unit_ptr_array = all_zone_unit_ptrs_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < this_unit_ptr_array.count(); ++j) {
        share::ObUnitInfo *unit_info = this_unit_ptr_array.at(j);
        if (OB_UNLIKELY(nullptr == unit_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit info ptr is null", K(ret));
        } else {
          int tmp_ret = unit_set.exist_refactored(unit_info->unit_.unit_id_);
          if (OB_HASH_EXIST == tmp_ret) {
            // bypass
          } else if (OB_HASH_NOT_EXIST == tmp_ret) {
            if (OB_FAIL(unit_ptr_array.push_back(unit_info))) {
              LOG_WARN("fail to push back", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("check unit set exist failed", K(ret), K(tmp_ret));
          }
        }
      }
      if (OB_SUCC(ret) && unit_ptr_array.count() > 0) {
        if (OB_FAIL(available_zone_unit_ptrs_.push_back(unit_ptr_array))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  LOG_INFO("units prepare for next partition", K(ret), K(available_zone_unit_ptrs_));
  return ret;
}

int ObAliveZoneUnitsProvider::get_all_zone_units(
    ZoneUnitArray& zone_unit) const
{
  UNUSED(zone_unit);
  return OB_NOT_IMPLEMENT;
}

int ObAliveZoneUnitsProvider::get_all_ptr_zone_units(
    ZoneUnitPtrArray& zone_unit_ptr) const
{
  return zone_unit_ptr.assign(all_zone_unit_ptrs_);
}

int ObAliveZoneUnitsProvider::find_zone(
    const common::ObZone &zone,
    const ObZoneUnitAdaptor *&zua)
{
  int ret = OB_SUCCESS;
  zua = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    FOREACH_CNT_X(zu, available_zone_unit_ptrs_, OB_SUCCESS == ret) {
      if (zu->count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid zone unit count. should not be zero.", K(ret));
      } else if (zu->at(0)->unit_.zone_ == zone) {
        zone_unit_adaptor_.set_zone_unit(zu);
        zua = &zone_unit_adaptor_;
        break;
      }
    }
  }
  return ret;
}

const ObUnitInfo *ObAllZoneUnitAdaptor::at(int64_t idx) const
{
  ObUnitInfo *info = NULL;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == all_unit_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("unexpected null all_unit_. bad code.");
  } else if (OB_UNLIKELY(idx < 0 || idx >= all_unit_->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("unexpected idx", K(idx), "count", all_unit_->count(), K(ret));
  } else {
    info = &all_unit_->at(idx)->info_;
  }
  return info;
}

int64_t ObAllZoneUnitAdaptor::count() const
{
  int64_t cnt = 0;
  if (OB_UNLIKELY(NULL == all_unit_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected null all_unit_");
  } else {
    cnt = all_unit_->count();
  }
  return cnt;
}

int ObAllZoneUnitAdaptor::get_target_unit_idx(
    const int64_t unit_offset,
    common::hash::ObHashSet<int64_t> &unit_set,
    const bool is_primary_partition,
    int64_t &unit_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == all_unit_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("unexpected null all_unit_. bad code", K(ret));
  } else if (all_unit_->count() <= 0) {
    ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    LOG_WARN("no available unit to alloc replica, may be migrate blocked", K(ret));
  } else if (is_primary_partition) {
    unit_idx = -1;
    const int64_t start = unit_offset % all_unit_->count();
    const int64_t end = start + all_unit_->count();
    for (int64_t i = start; OB_SUCC(ret) && i < end; ++i) {
      const int64_t idx = i % all_unit_->count();
      const UnitStat *this_unit = all_unit_->at(idx);
      if (OB_UNLIKELY(nullptr == this_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit ptr is null", K(ret));
      } else if (nullptr == this_unit->server_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("this server ptr is null", K(ret));
      } else if (!this_unit->server_->active_
          || !this_unit->server_->online_
          || this_unit->server_->blocked_) {
        // bypass, since server not available
      } else if (OB_HASH_EXIST == unit_set.exist_refactored(this_unit->info_.unit_.unit_id_)) {
        // by pass
      } else if (-1 == unit_idx) {
        unit_idx = idx;
      } else if (all_unit_->at(unit_idx)->tg_pg_cnt_ > this_unit->tg_pg_cnt_) {
        unit_idx = idx;
      }
    }
    if (OB_SUCC(ret) && -1 == unit_idx) {
      ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    } 
  } else {
    int64_t idx = unit_offset % all_unit_->count();
    const int64_t guard = idx;
    do {
      ret = unit_set.exist_refactored(at(idx)->unit_.unit_id_);
      if (OB_HASH_EXIST == ret) {
        idx++;
        idx %= count();
      }
    } while (OB_HASH_EXIST == ret && idx != guard);
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      unit_idx = idx;
    } else if (OB_HASH_EXIST == ret) {
      ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
    } else {
      LOG_WARN("fail to alloc replica", K(ret));
    }
  }
  return ret;
}

int ObAllZoneUnitAdaptor::update_tg_pg_count(
    const int64_t unit_idx,
    const bool is_primary_partition)
{
  int ret = OB_SUCCESS;
  if (unit_idx >= count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit_idx));
  } else if (OB_UNLIKELY(nullptr == all_unit_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("unexpected null all_unit_. bad code", K(ret));
  } else if (is_primary_partition) {
    UnitStat *this_unit = const_cast<UnitStat *>(all_unit_->at(unit_idx));
    if (OB_UNLIKELY(nullptr == this_unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit ptr is null", K(ret));
    } else {
      ++this_unit->tg_pg_cnt_;
    }
  } else {
    // not primary partition, no need to update tg pg cnt
  }
  return ret;
}

bool ObZoneLogonlyUnitProvider::exist(const ObZone &zone, const uint64_t unit_id) const
{
  bool bret = false;
  FOREACH_CNT(zu, all_zone_units_) {
    if (zu->zone_ == zone) {
      FOREACH_CNT(us, zu->all_unit_) {
        if ((*us)->info_.unit_.replica_type_ == REPLICA_TYPE_LOGONLY
            && (*us)->info_.unit_.unit_id_ == unit_id) {
          bret = true;
          break;
        }
      }
    }
  }
  return bret;
}

int ObZoneLogonlyUnitProvider::get_all_zone_units(ZoneUnitArray& zone_unit) const
{
  return zone_unit.assign(all_zone_units_);
}

int ObZoneLogonlyUnitProvider::get_all_ptr_zone_units(ZoneUnitPtrArray& zone_unit) const
{
  UNUSED(zone_unit);
  return OB_NOT_IMPLEMENT;
}

int ObZoneLogonlyUnitProvider::find_zone(const common::ObZone &zone,
                                      const ObZoneUnitAdaptor *&zua)
{
  int ret = OB_SUCCESS;
  zua = NULL;
  FOREACH_CNT_X(zu, all_zone_units_, OB_SUCCESS == ret) {
    if (zu->zone_ == zone) {
      // construct atemporay ZoneUnit
      all_unit_.reuse();
      FOREACH_CNT_X(us, zu->all_unit_, OB_SUCC(ret)) {
        const ServerStat *server = (*us)->server_;
        if (OB_ISNULL(server)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (!server->can_migrate_in()) {
          // ignore
        } else if (REPLICA_TYPE_LOGONLY != (*us)->info_.unit_.replica_type_) {
          //nothing todo
        } else if (OB_FAIL(all_unit_.push_back(const_cast<UnitStat *>(*us)))) {
            LOG_WARN("fail add alive unit to zone_unit", K(ret));
        }
      }
      zone_unit_adaptor_.set_zone_unit(&all_unit_);
      zua = &zone_unit_adaptor_;
      break;
    }
  }
  return ret;
}

int ObZoneUnitsWithoutLogonlyProvider::get_all_zone_units(ZoneUnitArray& zone_unit) const
{
  return zone_unit.assign(all_zone_units_);
}

int ObZoneUnitsWithoutLogonlyProvider::get_all_ptr_zone_units(ZoneUnitPtrArray& zone_unit) const
{
  UNUSED(zone_unit);
  return OB_NOT_IMPLEMENT;
}

int ObZoneUnitsWithoutLogonlyProvider::find_zone(const common::ObZone &zone,
                                      const ObZoneUnitAdaptor *&zua)
{
  int ret = OB_SUCCESS;
  zua = NULL;
  FOREACH_CNT_X(zu, all_zone_units_, OB_SUCCESS == ret) {
    if (zu->zone_ == zone) {
      // construct a temporary ZoneUnit
      all_unit_.reuse();
      FOREACH_CNT_X(us, zu->all_unit_, OB_SUCC(ret)) {
        const ServerStat *server = (*us)->server_;
        if (OB_ISNULL(server)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (!server->can_migrate_in()) {
          // ignore
        } else if (REPLICA_TYPE_LOGONLY == (*us)->info_.unit_.replica_type_) {
          //nothing todo
        } else if (OB_FAIL(all_unit_.push_back(const_cast<UnitStat *>(*us)))) {
            LOG_WARN("fail add alive unit to zone_unit", K(ret));
        }
      }
      zone_unit_adaptor_.set_zone_unit(&all_unit_);
      zua = &zone_unit_adaptor_;
      break;
    }
  }
  return ret;
}

int ObAllZoneUnitsProvider::get_all_zone_units(ZoneUnitArray& zone_unit) const
{
  return zone_unit.assign(all_zone_units_);
}

int ObAllZoneUnitsProvider::get_all_ptr_zone_units(ZoneUnitPtrArray& zone_unit) const
{
  UNUSED(zone_unit);
  return OB_NOT_IMPLEMENT;
}

int ObAllZoneUnitsProvider::find_zone(const common::ObZone &zone,
                                      const ObZoneUnitAdaptor *&zua)
{
  int ret = OB_SUCCESS;
  zua = NULL;
  FOREACH_CNT_X(zu, all_zone_units_, OB_SUCCESS == ret) {
    if (zu->zone_ == zone) {
      // construct a temporary ZoneUnit
      all_unit_.reuse();
      FOREACH_CNT_X(us, zu->all_unit_, OB_SUCC(ret)) {
        const ServerStat *server = (*us)->server_;
        if (OB_ISNULL(server)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (!server->can_migrate_in()) {
          // ignore
        } else if (OB_FAIL(all_unit_.push_back(const_cast<UnitStat *>(*us)))) {
            LOG_WARN("fail add alive unit to zone_unit", K(ret));
        }
      }
      zone_unit_adaptor_.set_zone_unit(&all_unit_);
      zua = &zone_unit_adaptor_;
      break;
    }
  }
  return ret;
}


}/* ns rootserver*/
}/* ns oceanbase */
