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
#define USING_LOG_PREFIX BALANCE
#include "rootserver/ob_ls_balance_helper.h"
#include "rootserver/ob_ls_service_helper.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"//trans
#include "observer/ob_server_struct.h"//GCTX
#include "share/schema/ob_schema_getter_guard.h"//ObSchemaGetGuard
#include "share/schema/ob_multi_version_schema_service.h"//ObMultiSchemaService
#include "share/schema/ob_table_schema.h"//ObTableSchema
#include "share/ls/ob_ls_table_operator.h"  // ObLSTableOperator
#include "storage/ob_common_id_utils.h"     // ObCommonIDUtils
#define ISTAT(fmt, args...) FLOG_INFO("[LS_BALANCE] " fmt, ##args)
#define WSTAT(fmt, args...) FLOG_WARN("[LS_BALANCE] " fmt, ##args)

namespace oceanbase
{
using namespace share;
namespace rootserver
{
//////ObLSGroupStat
int ObLSGroupStat::init(const uint64_t lg_id, const ObUnitIDList &unit_list)
{
  reset();
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == lg_id || unit_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(lg_id), K(unit_list));
  } else if (OB_FAIL(current_unit_list_.assign(unit_list))) {
    LOG_WARN("failed to assign unit list", KR(ret), K(unit_list));
  } else {
    lg_id_ = lg_id;
  }
  return ret;
}
int ObLSGroupStat::assign(const ObLSGroupStat &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(current_unit_list_.assign(other.current_unit_list_))) {
      LOG_WARN("failed to assign unit list", KR(ret), K(other));
    } else if (OB_FAIL(ls_info_set_.add_members(other.ls_info_set_))) {
      LOG_WARN("failed to add members", KR(ret), K(other));
    } else if (OB_FAIL(ug_ids_.assign(other.ug_ids_))) {
      LOG_WARN("failed to assign ug id", KR(ret), K(other));
    } else if (OB_FAIL(target_unit_list_.assign(other.target_unit_list_))) {
      LOG_WARN("failed to assign unit list", KR(ret), K(other));
    } else {
      lg_id_ = other.lg_id_;
    }
  }
  return ret;
}

int ObLSGroupStat::add_ls_status(const int64_t ls_info_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 > ls_info_index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls index is invalid", KR(ret), K(ls_info_index));
  } else if (ls_info_set_.has_member(ls_info_index)) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("ls already exist", KR(ret), K(ls_info_index));
  } else if (OB_FAIL(ls_info_set_.add_member(ls_info_index))) {
    LOG_WARN("failed to add member", KR(ret), K(ls_info_index));
  }
  return ret;
}

int ObLSGroupStat::find_and_remove_ls(int64_t &ls_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls group is invalid", KR(ret), KPC(this));
  } else {
    int64_t start_pos = -1;
    if (OB_FAIL(ls_info_set_.find_next(start_pos, ls_index))) {
      LOG_WARN("failed to find next", KR(ret));
    } else if (OB_FAIL(ls_info_set_.del_member(ls_index))) {
      LOG_WARN("failed to del member", KR(ret), K(ls_index));
    }
  }
  return ret;
}

//////ObUnitLSStat
int ObUnitLSStat::init(share::ObUnit &unit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!unit.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit));
  } else {
    unit_ = &unit;
    unit_id_ = unit.unit_id_;
  }
  return ret;
}

int64_t ObUnitLSStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(unit), K_(unit_id));
  J_COMMA();
  J_NAME("ls_group_array");
  J_COLON();
  J_ARRAY_START();
  ARRAY_FOREACH_NORET(ls_group_info_, idx) {
    ObLSGroupStat* ls_group = ls_group_info_.at(idx);
    if (OB_NOT_NULL(ls_group)) {
      J_KV("ls_group_id", ls_group->lg_id_);
    } else {
      J_NAME("ls_group is null");
    }
    J_COMMA();
  }
  J_ARRAY_END();
  J_OBJ_END();
  return pos;
}
int ObUnitLSStat::add_ls_group(ObLSGroupStat &ls_group_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_group_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_group_info));
  } else if (has_exist_in_array(ls_group_info_, &ls_group_info)) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("ls group already exist", KR(ret), K(ls_group_info), K(ls_group_info_));
  } else if (OB_FAIL(ls_group_info_.push_back(&ls_group_info))) {
    LOG_WARN("failed to push back ls group info", KR(ret), K(ls_group_info_));
  }
  return ret;
}

int ObUnitLSStat::assign(const ObUnitLSStat &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else {
    reset();
    if (OB_FAIL(ls_group_info_.assign(other.ls_group_info_))) {
      LOG_WARN("failed to assign ls group info", KR(ret), K(other));
    } else {
      unit_ = other.unit_;
      unit_id_ = other.unit_id_;
      zone_stat_ = other.zone_stat_;
      unit_group_stat_ = other.unit_group_stat_;
    }
  }
  return ret;
}

//////end of ObUnitLSStat
/////ObUnitGroupStat
int ObUnitGroupStat::assign(const ObUnitGroupStat& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(ls_group_info_.assign(other.ls_group_info_))) {
      LOG_WARN("failed to assign lsgroup info", KR(ret), K(other));
    } else if (OB_FAIL(unit_info_.assign(other.unit_info_))) {
      LOG_WARN("failed to assign unit", KR(ret), K(other));
    } else {
      ug_id_ = other.ug_id_;
    }
  }
  return ret;
}
int64_t ObUnitGroupStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ug_id));
  J_COMMA();
  J_NAME("ls_group_array");
  J_COLON();
  J_ARRAY_START();
  ARRAY_FOREACH_NORET(ls_group_info_, idx) {
    ObLSGroupStat* ls_group = ls_group_info_.at(idx);
    if (OB_NOT_NULL(ls_group)) {
      J_KV("ls_group_id", ls_group->lg_id_);
    } else {
      J_NAME("ls_group is null");
    }
    J_COMMA();
  }
  J_ARRAY_END();
  J_COMMA();
  J_NAME("unit_list");
  J_COLON();
  J_ARRAY_START();
  ARRAY_FOREACH_NORET(unit_info_, idx) {
    ObUnitLSStat* unit = unit_info_.at(idx);
    if (OB_NOT_NULL(unit) && OB_NOT_NULL(unit->unit_)) {
      J_KV("unit_id", unit->unit_id_, "zone", unit->unit_->zone_);
    } else {
      J_NAME("unit is null");
    }
    if (unit_info_.count() - 1 > idx) {
      J_COMMA();
    }
  }
  J_ARRAY_END();
  J_OBJ_END();
  return pos;
}

int ObUnitGroupStat::init(ObUnitLSStat &unit_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!unit_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit group is invalid", KR(ret), K(unit_info));
  } else {
    reset();
    ug_id_ = unit_info.unit_->unit_group_id_;
  }
  return ret;
}

int ObUnitGroupStat::add_unit(ObUnitLSStat &unit)
{
  int ret = OB_SUCCESS;
  //一个unit只会属于一个unit_group，不会变化
  if (OB_UNLIKELY(!unit.is_valid() || ug_id_ != unit.unit_->unit_group_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit is invalid", KR(ret), K(unit));
  } else if (has_exist_in_array(unit_info_, &unit)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit already exist", KR(ret), K(unit), K(unit_info_));
  } else if (OB_FAIL(unit_info_.push_back(&unit))) {
    LOG_WARN("failed to push back unit", KR(ret), K(unit));
  }
  return ret;
}

int ObUnitGroupStat::construct_ls_group_info()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit is invalid", KR(ret), "this", *this);
  } else {
    //把所有的unit上面的日志流组都放进去，不会有重复
    ARRAY_FOREACH(unit_info_, idx) {
      ObUnitLSStat *unit_stat = unit_info_.at(idx);
      CK(OB_NOT_NULL(unit_stat), unit_stat->is_valid(), OB_NOT_NULL(unit_stat->zone_stat_));
      if (OB_SUCC(ret) && unit_stat->zone_stat_->is_in_locality_) {
        if (OB_FAIL(append_array_no_dup(ls_group_info_, unit_stat->ls_group_info_))) {
          LOG_WARN("failed to assign ls group info", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObUnitGroupStat::try_add_ls_group(ObLSGroupStat* lg_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit is invalid", KR(ret), "this", *this);
  } else if (OB_ISNULL(lg_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls group is null", KR(ret));
  } else if (has_exist_in_array(ls_group_info_, lg_info)) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("ls group already exist", KR(ret), KPC(lg_info), "this", *this);
  } else if (OB_FAIL(ls_group_info_.push_back(lg_info))) {
    LOG_WARN("failed to push back", KR(ret));
  }
  return ret;
}

int ObUnitGroupStat::get_unit_info_by_zone(const ObZone &zone, const ObUnitLSStat* &unit_info) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", KR(ret), K(zone));
  } else {
    ARRAY_FOREACH(unit_info_, idx) {
      const ObUnitLSStat *unit = unit_info_.at(idx);
      if (OB_ISNULL(unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit is null", KR(ret), K(unit_info_), K(idx));
      } else if (unit->unit_->zone_ == zone) {
        unit_info = unit;
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(unit_info)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to find unit by zone", KR(ret), K(zone), KPC(this));
    }
  }
  return ret;
}

int ObUnitGroupStat::get_zone_array(ObIArray<ObZone> &zone_array) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(this));
  } else {
    ARRAY_FOREACH(unit_info_, idx) {
      const ObUnitLSStat *unit = unit_info_.at(idx);
      CK(OB_NOT_NULL(unit), unit->is_valid(), OB_NOT_NULL(unit->unit_))
      if (OB_FAIL(ret)) {
      } else if (has_exist_in_array(zone_array, unit->unit_->zone_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone has multi unit", KR(ret), KPC(unit), K(zone_array), KPC(this));
      } else if (OB_FAIL(zone_array.push_back(unit->unit_->zone_))) {
        LOG_WARN("failed to push back", KR(ret), K(idx));
      }
    }
  }
  return ret;
}

int ObUnitGroupStat::get_zone_info_array(
    ObIArray<ObZoneLSStat*> &zone_info_array)
{
  int ret = OB_SUCCESS;
  zone_info_array.reset();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(this));
  } else {
    for (int64_t index = 0; index < unit_info_.count() && OB_SUCC(ret); ++index) {
      ObUnitLSStat* unit_ls_stat = unit_info_.at(index);
      if (OB_ISNULL(unit_ls_stat)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), KP(unit_ls_stat));
      } else {
        ObZoneLSStat* zone_stat = unit_ls_stat->zone_stat_;
        if (OB_ISNULL(zone_stat)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret), KP(zone_stat));
        } else if (has_exist_in_array(zone_info_array, zone_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("duplicate zones in one unit group unexpected", KR(ret), KPC(zone_stat));
        } else if (OB_FAIL(zone_info_array.push_back(zone_stat))) {
          LOG_WARN("fail to push back zone stat", KR(ret), KPC(zone_stat));
        }
      }
    }
  }
  return ret;
}

/////ObUnitGroupStat

//////ObZoneLSStat
int ObZoneLSStat::assign(const ObZoneLSStat &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    zone_ = other.zone_;
    is_balance_ = other.is_balance_;
    is_in_locality_ = other.is_in_locality_;
    gts_unit_ = other.gts_unit_;
    if (OB_FAIL(valid_unit_array_.assign(other.valid_unit_array_))) {
      LOG_WARN("failed to assign unit array", KR(ret), K(other));
    } else if (OB_FAIL(deleting_unit_array_.assign(other.deleting_unit_array_))) {
      LOG_WARN("failed to assign unit array", KR(ret), K(other));
    }
  }
  return ret;
}
int64_t ObZoneLSStat::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(zone), K_(is_balance), K_(is_in_locality), K_(gts_unit));
  J_COMMA();
  J_NAME("valid_unit_list");
  J_COLON();
  J_ARRAY_START();
  ARRAY_FOREACH_NORET(valid_unit_array_, idx) {
    ObUnitLSStat* unit = valid_unit_array_.at(idx);
    if (OB_NOT_NULL(unit) && OB_NOT_NULL(unit->unit_)) {
      J_KV("unit_id", unit->unit_id_, "unit_group_id", unit->unit_->unit_group_id_);
    } else {
      J_NAME("unit is null");
    }
    J_COMMA();
  }
  J_ARRAY_END();
  J_COMMA();
  J_NAME("deleting_unit_list");
  J_COLON();
  J_ARRAY_START();
  ARRAY_FOREACH_NORET(deleting_unit_array_, idx) {
    ObUnitLSStat* unit = deleting_unit_array_.at(idx);
    if (OB_NOT_NULL(unit) && OB_NOT_NULL(unit->unit_)) {
      J_KV("unit_id", unit->unit_id_, "unit_group_id", unit->unit_->unit_group_id_);
    } else {
      J_NAME("unit is null");
    }
    if (deleting_unit_array_.count() - 1 > idx) {
      J_COMMA();
    }
  }

  J_ARRAY_END();
  J_OBJ_END();
  return pos;
}

int ObZoneLSStat::add_unit_ls_info(ObUnitLSStat &unit_ls_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!unit_ls_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit ls info is invalid", KR(ret), K(unit_ls_stat));
  } else if (unit_ls_stat.is_deleting()) {
    if (OB_FAIL(deleting_unit_array_.push_back(&unit_ls_stat))) {
      LOG_WARN("failed to push back", KR(ret));
    }
  } else if (unit_ls_stat.is_sys_standalone_) {
    gts_unit_ = &unit_ls_stat;
  } else if (OB_FAIL(valid_unit_array_.push_back(&unit_ls_stat))) {
    LOG_WARN("failed to push back", KR(ret));
  }
  return ret;
}

int ObZoneLSStat::get_unit_info_by_ug_id(const uint64_t ug_id, ObUnitLSStat* &unit_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == ug_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit group is invalid", KR(ret), K(ug_id));
  }
  ARRAY_FOREACH(valid_unit_array_, idx) {
    ObUnitLSStat *unit = valid_unit_array_.at(idx);
    CK(OB_NOT_NULL(unit));
    if (OB_SUCC(ret) && unit->is_valid()) {
      if (ug_id == unit->unit_->unit_group_id_) {
        unit_info = unit;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(unit_info)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("unit group not exist in the zone", KR(ret), K(ug_id));
  }
  return ret;
}

int ObZoneLSStat::get_ug_array(ObUGArray &ug_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == valid_unit_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit array is invalid", KR(ret));
  } else {
    ARRAY_FOREACH(valid_unit_array_, idx) {
      ObUnitLSStat *unit = valid_unit_array_.at(idx);
      CK(OB_NOT_NULL(unit));
      CK(OB_NOT_NULL(unit->unit_group_stat_));
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ug_array.push_back(unit->unit_group_stat_))) {
        LOG_WARN("failed to push back", KR(ret));
      }
    }
  }
  return ret;
}
int ObZoneLSStat::get_min_unit_valid_for_normal_ls(ObUnitLSStat* &unit) const
{
  int ret = OB_SUCCESS;
  unit = NULL;
  if (OB_UNLIKELY(0 == valid_unit_array_.count() || !is_in_locality_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(this));
  } else {
    ARRAY_FOREACH(valid_unit_array_, idx) {
      ObUnitLSStat *const unit_ls = valid_unit_array_.at(idx);
      CK(OB_NOT_NULL(unit_ls));
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(unit) || unit->get_ls_group_count() > unit_ls->get_ls_group_count()) {
          unit = unit_ls;
        }
      }
    }
    LOG_INFO("find valid unit", KR(ret), K(unit));
  }
  return ret;
}


int ObZoneLSStat::set_is_balance()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || 0 == valid_unit_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit array is invalid", KR(ret), KPC(this));
  } else if (deleting_unit_array_.count() > 0) {
    is_balance_ = false;
    LOG_INFO("zone is not balance", K(zone_));
  } else {
    int64_t ls_group_cnt = -1;
    is_balance_ = true;
    ARRAY_FOREACH(valid_unit_array_, idx) {
      ObUnitLSStat *unit = valid_unit_array_.at(idx);
      CK(OB_NOT_NULL(unit))
      CK(OB_NOT_NULL(unit->unit_));
      if (OB_SUCC(ret)) {
        if (-1 == ls_group_cnt) {
          ls_group_cnt = unit->get_ls_group_count();
        }
        if (unit->unit_->status_ != ObUnit::UNIT_STATUS_ACTIVE
            || ls_group_cnt != unit->get_ls_group_count()
            || 0 == ls_group_cnt) {
          is_balance_ = false;
          LOG_INFO("zone is not balance", K(zone_), K(ls_group_cnt), KPC(unit));
          break;
        }
      }
    }
  }
  return ret;
}

bool ObZoneLSStat::is_all_unit_active() const
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || 0 == valid_unit_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit array is invalid", KR(ret), KPC(this));
  } else if (deleting_unit_array_.count() > 0) {
    bret = false;
  } else {
    bret = true;
    ARRAY_FOREACH_X(valid_unit_array_, idx, cnt, bret) {
      ObUnitLSStat *unit = valid_unit_array_.at(idx);
      CK(OB_NOT_NULL(unit), OB_NOT_NULL(unit->unit_))
      if (OB_FAIL(ret)) {
        bret = false;
      } else if (unit->unit_->status_ != ObUnit::UNIT_STATUS_ACTIVE) {
        bret = false;
      }
    }
  }
  return bret;
}

int ObZoneLSStat::calculate_variance_score(double &variance_score) const
{
  variance_score = 0.0;
  int ret = OB_SUCCESS;
  double average_ls_group_count = 0.0;
  if (OB_UNLIKELY(!is_valid())
      || OB_UNLIKELY(0 == valid_unit_array_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit array is invalid", KR(ret), KPC(this));
  } else {
    // 1. caculate average ls group count on this unit group
    for (int64_t index = 0; index < valid_unit_array_.count() && OB_SUCC(ret); ++index) {
      ObUnitLSStat *unit_ls_stat = valid_unit_array_.at(index);
      if (OB_ISNULL(unit_ls_stat)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), KP(unit_ls_stat));
      } else {
        average_ls_group_count += (static_cast<double>(unit_ls_stat->get_ls_group_count()) / valid_unit_array_.count());
      }
    }
    // 2. caculate variance score for this unit group
    for (int64_t index = 0; index < valid_unit_array_.count() && OB_SUCC(ret); ++index) {
      ObUnitLSStat *unit_ls_stat = valid_unit_array_.at(index);
      if (OB_ISNULL(unit_ls_stat)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), KP(unit_ls_stat));
      } else {
        variance_score += (static_cast<double>(unit_ls_stat->get_ls_group_count()) - average_ls_group_count)
                        * (static_cast<double>(unit_ls_stat->get_ls_group_count()) - average_ls_group_count);
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      variance_score = variance_score / static_cast<double>(valid_unit_array_.count());
    }
  }
  LOG_INFO("finish get variance score for unit group", KR(ret), K(variance_score),
           K(average_ls_group_count), K(valid_unit_array_));
  return ret;
}
//////end ObZoneLSStat

//////ChooseZoneCmp
bool ChooseZoneCmp::operator()(const ObZoneLSStat *left, const ObZoneLSStat *right)
{
  bool bret = true;
  int ret = OB_SUCCESS;  // dummy
  double left_variance_score = 0.0;
  double right_variance_score = 0.0;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN("zone stat is null", KR(ret_), KP(left), KP(right));
  } else if (OB_UNLIKELY(!left->is_valid() || !right->is_valid())) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN("zone stat is invalid", KR(ret_), K(left), K(right));
  } else if (left->is_in_locality_ != right->is_in_locality_) {
    if (left->is_in_locality_) {
      bret = true;
    } else {
      bret = false;
    }
  } else if (left->is_balance_ != right->is_balance_) {
    if (left->is_balance_) {
      bret = true;
    } else {
      bret = false;
    }
  } else if (left->is_all_unit_active() != right->is_all_unit_active()) {
    //两个zone都没有in_balance，检查是否有不可用的zone
    if (left->is_all_unit_active()) {
      bret = true;
    } else {
      bret = false;
    }
  } else if (OB_FAIL(left->calculate_variance_score(left_variance_score))) {
    ret_ = ret;
    LOG_WARN("fail to get variance score for valid unit array", KR(ret), KPC(left));
  } else if (OB_FAIL(right->calculate_variance_score(right_variance_score))) {
    ret_ = ret;
    LOG_WARN("fail to get variance score for valid unit array", KR(ret), KPC(right));
  } else if (fabs(left_variance_score - right_variance_score) > OB_DOUBLE_EPSINON) {
    if (left_variance_score < right_variance_score) {
      // left variance score is smaller means ls_group_count in left is more balanced
      bret = true;
    } else {
      bret = false;
    }
  }
  return bret;
}
//////end ChooseZoneCmp

//////ChooseUGCmp

bool ChooseUGCmp::operator()(const ObUnitLSStat *left, const ObUnitLSStat *right)
{
  bool bret = true;
  int ret = OB_SUCCESS;
  ChooseZoneCmp zone_cmp;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN("unit stat is null", KR(ret_), KP(left), KP(right));
  } else if (OB_UNLIKELY(!left->is_valid() || !right->is_valid())) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN("unit stat is invalid", KR(ret_), K(left), K(right));
  } else if (OB_ISNULL(left->zone_stat_) || OB_ISNULL(right->zone_stat_)) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN("zone stat is null", KR(ret_), KP(left->zone_stat_), KP(right->zone_stat_));
  } else if (left->is_valid_for_normal_ls() != right->is_valid_for_normal_ls()) {
    if (left->is_valid_for_normal_ls()) {
      bret = true;
    } else {
      bret = false;
    }
  } else {
    // check zone priority
    bret = zone_cmp(left->zone_stat_, right->zone_stat_);
    if (OB_FAIL(zone_cmp.get_ret())) {
      ret_ = ret;
      LOG_WARN("fail to compare zone", KR(ret), KPC(left), KPC(right));
    }
  }
  return bret;
}

//////end ChooseUGCmp


//////ObTenantLSBalanceInfo
int ObTenantLSBalanceInfo::init_tenant_ls_balance_info(
    const uint64_t tenant_id,
    const share::ObLSStatusInfoArray &status_array,
    const ObBalanceJobDesc &job_desc, const ObArray<ObUnit> &unit_array,
    const ObTenantRole &tenant_role)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else if (OB_UNLIKELY(0 >= status_array.count()
                         || 0 >= unit_array.count())
                         || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("status array is empty", KR(ret), K(status_array),
        K(unit_array), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    tenant_role_ = tenant_role;
    if (OB_FAIL(unit_info_.assign(unit_array))) {
      LOG_WARN("failed to assign unit array", KR(ret), K(unit_array));
    } else {
      //unit_info调整为按zone排序
      lib::ob_sort(unit_info_.begin(), unit_info_.end(), ObUnit::compare_with_zone);
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < status_array.count(); ++i) {
      const ObLSStatusInfo &ls_info = status_array.at(i);
      if (!ls_info.is_user_ls()) {
        if (OB_FAIL(sys_ls_info_.assign(ls_info))) {
          LOG_WARN("failed to assign", KR(ret), K(ls_info));
        }
      } else if (ls_info.is_duplicate_ls()) {
        if (OB_FAIL(duplicate_ls_info_.push_back(ls_info))) {
          LOG_WARN("failed to push back ls info", KR(ret), K(ls_info));
        }
      } else if (OB_FAIL(normal_ls_info_.push_back(ls_info))) {
        LOG_WARN("failed to push back ls info", KR(ret), K(ls_info));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(job_desc_.assign(job_desc))) {
      LOG_WARN("failed to assign job desc", KR(ret), K(job_desc));
    } else if (OB_FAIL(build_ls_group_array_())) {
      LOG_WARN("failed to build ls group array", KR(ret));
    } else if (OB_FAIL(build_unit_ls_array_())) {
      LOG_WARN("failed to build unit ls info", KR(ret));
    } else if (OB_FAIL(build_zone_ls_info_())) {
      LOG_WARN("failed to build zone info", KR(ret));
    } else {
      is_inited_ = true;
      ISTAT("init tenant ls balance info",
      K(job_desc_), K(normal_ls_info_), K(duplicate_ls_info_));
      ISTAT("other info",
      K(ls_group_array_), K(unit_array_), K(ug_array_), K(zone_array_), K(tenant_role_));
    }
  }
  return ret;
}

int ObTenantLSBalanceInfo::build_ls_group_array_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else if (normal_ls_info_.empty()) {
    //nothing todo
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < normal_ls_info_.count(); ++i) {
      ObLSGroupStat* lg_info = NULL;
      share::ObLSStatusInfo &ls_info = normal_ls_info_.at(i);
      if (OB_FAIL(get_ls_group_info(ls_info.ls_group_id_, lg_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          ObLSGroupStat new_lg_info;
          if (OB_FAIL(new_lg_info.init(ls_info.ls_group_id_, ls_info.unit_id_list_))) {
            LOG_WARN("failed to init lg info", KR(ret), K(ls_info));
          } else if (OB_FAIL(ls_group_array_.push_back(new_lg_info))) {
            LOG_WARN("failed to push back ls_group array", KR(ret), K(new_lg_info));
          } else {
            lg_info = &ls_group_array_.at(ls_group_array_.count() - 1);
          }
        } else {
          LOG_WARN("failed to get ls group info", KR(ret), K(ls_info));
        }
      }//end if
      if (OB_FAIL(ret)) {
        //nothing
      } else if (OB_ISNULL(lg_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("lg info is empty", KR(ret), K(ls_info));
      } else if (OB_FAIL(lg_info->add_ls_status(i))) {
        LOG_WARN("failed to add ls status", KR(ret), K(i), KP(lg_info));
      }
    }
  }
  return ret;
}

#define GET_FROM_ARRAY(ARRAY, member, result)\
  do {\
    bool found = false;\
    for (int64_t array_index = 0; OB_SUCC(ret) && array_index < ARRAY.count() && !found; ++array_index) {\
      if (member == ARRAY.at(array_index).member##_) {\
        result = &ARRAY.at(array_index);\
        found = true;\
      }\
    }\
    if (OB_SUCC(ret) && !found) {\
      ret = OB_ENTRY_NOT_EXIST;\
      LOG_WARN("failed to find member", KR(ret), K(member));\
    }\
  } while(0)

int ObTenantLSBalanceInfo::get_ls_group_info(const uint64_t lg_id,
    ObLSGroupStat* &lg_info)
{
  int ret = OB_SUCCESS;
  GET_FROM_ARRAY(ls_group_array_, lg_id, lg_info);
  return ret;
}


int ObTenantLSBalanceInfo::build_unit_ls_array_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else if (0 >= unit_info_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit info is empty", KR(ret), K(unit_info_));
  } else {
    //构造unit_array_
    ARRAY_FOREACH(unit_info_, idx) {
      ObUnit &unit = unit_info_.at(idx);
      ObUnitLSStat unit_ls_info;
      if (OB_FAIL(unit_ls_info.init(unit))) {
        LOG_WARN("failed to init unit", KR(ret), K(unit));
      } else if (OB_FAIL(unit_array_.push_back(unit_ls_info))) {
        LOG_WARN("failed to push back", KR(ret), K(unit_ls_info));
      }
    }

    ObUnitLSStat* unit_info = NULL;
    //构造ls_group和unit的关系
    ARRAY_FOREACH(ls_group_array_, ls_group_idx) {
      ObLSGroupStat &ls_group = ls_group_array_.at(ls_group_idx);
      ObUnitIDList &unit_list = ls_group.current_unit_list_;
      ARRAY_FOREACH(unit_list, idx) {
        unit_info = NULL;
        if (OB_FAIL(get_unit_ls_info(unit_list.at(idx).id(), unit_info))) {
          LOG_ERROR("failed to get unit ls info", KR(ret), K(idx), K(unit_list));
        } else if (OB_ISNULL(unit_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit info is empty", KR(ret), "unit_id", unit_list.at(idx));
        } else if (OB_FAIL(unit_info->add_ls_group(ls_group))) {
          LOG_WARN("failed to add ls status", KR(ret), K(ls_group_idx), K(idx), KPC(unit_info));
        }
      }//end for each unit
    }

    //标记sys_ls的unit_list的unit为sys_standalone
    if (job_desc_.get_enable_gts_standalone()) {
      ARRAY_FOREACH(sys_ls_info_.unit_id_list_, idx) {
        unit_info = NULL;
        if (OB_FAIL(get_unit_ls_info(sys_ls_info_.unit_id_list_.at(idx).id(), unit_info))) {
          LOG_ERROR("failed to get unit ls info", KR(ret), K(sys_ls_info_), K(idx));
        } else if (OB_ISNULL(unit_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit info is empty", KR(ret), "unit_id", sys_ls_info_.unit_id_list_.at(idx));
        } else {
          unit_info->is_sys_standalone_ = true;
        }
      }
    }
  }
  return ret;
}

int ObTenantLSBalanceInfo::get_unit_ls_info(const uint64_t unit_id, ObUnitLSStat* &unit_ls_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == unit_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_id));
  } else {
    GET_FROM_ARRAY(unit_array_, unit_id, unit_ls_info);
  }
  return ret;
}

int ObTenantLSBalanceInfo::get_ls_unit_list(const share::ObLSStatusInfo &ls_info, ObArray<ObUnit*> &unit_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls info is invalid", KR(ret), K(ls_info));
  } else {
    const ObUnitIDList &unit_id_list = ls_info.get_unit_list();
    if (0 != unit_id_list.count()) {
      ARRAY_FOREACH(unit_id_list, idx) {
        ObUnit* unit = NULL;
        uint64_t unit_id = unit_id_list.at(idx).id();
        GET_FROM_ARRAY(unit_info_, unit_id, unit);
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(unit)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit is null", KR(ret), K(unit_id), K(ls_info));
          } else if (OB_FAIL(unit_list.push_back(unit))) {
            LOG_WARN("failed to push back unit", KR(ret));
          }
        }
      }
    } else if (0 != ls_info.unit_group_id_) {
      //通过unit_group获取unit_list
      ARRAY_FOREACH(unit_info_, idx) {
        ObUnit &unit = unit_info_.at(idx);
        if (unit.unit_group_id_ == ls_info.unit_group_id_) {
          if (OB_FAIL(unit_list.push_back(&unit))) {
            LOG_WARN("failed to push back unit", KR(ret));
          }
        }
      }//end for array
    } else {
      //TODO
    }
  }
  //TODO是否需要检查unit_list可能会为空
  return ret;
}

int ObTenantLSBalanceInfo::get_or_create_unit_group_info_(ObUnitLSStat &unit_ls_info,
    ObUnitGroupStat* &ug_info)
{
  int ret = OB_SUCCESS;
  ug_info = NULL;
  if (OB_UNLIKELY(!unit_ls_info.is_valid() || unit_ls_info.is_deleting())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_ls_info));
  } else if (OB_FAIL(get_unit_group_info(unit_ls_info.unit_->unit_group_id_, ug_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get unit group info", KR(ret), K(unit_ls_info));
    } else {
      ObUnitGroupStat new_ug_info;
      if (OB_FAIL(new_ug_info.init(unit_ls_info))) {
        LOG_WARN("failed to init ug info", KR(ret), K(unit_ls_info));
      } else if (OB_FAIL(ug_array_.push_back(new_ug_info))) {
        LOG_WARN("failed to push back", KR(ret), K(new_ug_info));
      } else {
        ug_info = &ug_array_.at(ug_array_.count() - 1);
      }
    }
  }
  return ret;
}

int ObTenantLSBalanceInfo::get_ug_locality_unit_list(ObUnitGroupStat &ug_info,
    share::ObUnitIDList &unit_list)
{
  int ret = OB_SUCCESS;
  unit_list.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!ug_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit info is invalid", KR(ret), K(ug_info));
  } else {
    ARRAY_FOREACH(ug_info.unit_info_, idx) {
      ObUnitLSStat* unit = ug_info.unit_info_.at(idx);
      if (OB_ISNULL(unit) || !unit->is_valid()
          || OB_ISNULL(unit->zone_stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit is null", KR(ret), K(idx), K(unit));
      } else if (unit->zone_stat_->is_in_locality_) {
        if (OB_FAIL(unit_list.push_back(ObDisplayUnitID(unit->unit_id_)))) {
          LOG_WARN("failed to push back", KR(ret), KPC(unit));
        }
      }
    }
    LOG_INFO("get unit locality unit list", KR(ret), K(unit_list));
  }
  return ret;
}

int ObTenantLSBalanceInfo::get_unit_group_info(const uint64_t ug_id, ObUnitGroupStat* &ug_info)
{
  int ret = OB_SUCCESS;
  GET_FROM_ARRAY(ug_array_, ug_id, ug_info);
  return ret;
}

int ObTenantLSBalanceInfo::get_zone_info(const ObZone &zone, ObZoneLSStat* &zone_info)
{
  int ret = OB_SUCCESS;
  GET_FROM_ARRAY(zone_array_, zone, zone_info);
  return ret;
}

int ObTenantLSBalanceInfo::create_new_zone_info_(ObZone &zone)
{
  int ret = OB_SUCCESS;
  ObZoneLSStat *zone_info = NULL;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone));
  } else if (OB_FAIL(get_zone_info(zone, zone_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      bool in_locality = false;
      if (OB_FAIL(job_desc_.check_zone_in_locality(zone, in_locality))) {
        LOG_WARN("failed to check zone in locality", KR(ret), K(zone));
      } else {
        ObZoneLSStat zone_info(zone, in_locality);
        if (OB_FAIL(zone_array_.push_back(zone_info))) {
          LOG_WARN("failed to push back", KR(ret));
        }
      }
    } else {
      LOG_WARN("failed to get zone info", KR(ret));
    }
  } else {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("zone already exist", KR(ret), K(zone), K(zone_info));
  }
  return ret;
}

int ObTenantLSBalanceInfo::build_zone_ls_info_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret));
  } else if (OB_UNLIKELY(unit_array_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit array is empty", KR(ret), K(unit_array_));
  } else {
    ObZone last_zone;
    //unit_info是按照zone的顺序排列的
    ARRAY_FOREACH(unit_array_, idx) {
      ObUnitLSStat &unit_info = unit_array_.at(idx);
      CK(OB_NOT_NULL(unit_info.unit_))
      if (OB_SUCC(ret) && last_zone != unit_info.unit_->zone_) {
        last_zone = unit_info.unit_->zone_;
        if (OB_FAIL(create_new_zone_info_(last_zone))) {
          LOG_WARN("failed to create new zone", KR(ret), K(last_zone));
        }
      }
      if (OB_SUCC(ret)) {
        ObZoneLSStat &zone_info = zone_array_.at(zone_array_.count() - 1);
        unit_info.zone_stat_ = &zone_info;
        if (OB_FAIL(zone_info.add_unit_ls_info(unit_info))) {
          LOG_WARN("failed to add unit", KR(ret), K(unit_info));
        }
      }
      if (OB_SUCC(ret) && unit_info.unit_->status_ != ObUnit::UNIT_STATUS_DELETING) {
        //deleting状态的unit不计入内存的unit_group的范围
        ObUnitGroupStat *ug_info;
        if (OB_FAIL(get_or_create_unit_group_info_(unit_info, ug_info))) {
          LOG_WARN("failed to get or create unit group info", KR(ret), K(unit_info));
        } else if (OB_ISNULL(ug_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit group is null", KR(ret), K(unit_info));
        } else if (OB_FAIL(ug_info->add_unit(unit_info))) {
          LOG_WARN("failed to add unit", KR(ret), K(unit_info));
        } else {
          unit_info.unit_group_stat_ = ug_info;
        }
      }
    }//end for zone array

    //构造unit_group
    ARRAY_FOREACH(ug_array_, idx) {
      if (OB_FAIL(ug_array_.at(idx).construct_ls_group_info())) {
        LOG_WARN("failed to construct ls group info", KR(ret));
      }
    }
    //检查每个zone上都是active的unit，并且日志流组的个数都是均衡的，需要设置is_balance
    ARRAY_FOREACH(zone_array_, idx) {
      if (OB_FAIL(zone_array_.at(idx).set_is_balance())) {
        LOG_WARN("failed to set zone is balance", KR(ret));
      }
    }
  }
  return ret;
}


int ObTenantLSBalanceInfo::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id_) || !job_desc_.is_valid()
             || !sys_ls_info_.is_valid()
             || unit_info_.count() <= 0
             || ug_array_.count() <= 0
             || unit_array_.count() <= 0 || zone_array_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner stat error", KR(ret), K(tenant_id_),
    K(job_desc_), K(sys_ls_info_), K(normal_ls_info_), "unit count", unit_info_.count(),
    "ug_count", ug_array_.count(), "unit_ls_count", unit_array_.count(),
    "zone_count", zone_array_.count());
  }
  return ret;
}

int ObTenantLSBalanceInfo::check_unit_list_valid(const share::ObUnitIDList &unit_list,
                           bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(unit_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit list is empty", KR(ret), K(unit_list));
  } else {
    is_valid = true;
    ObUnitLSStat* unit_ls_info = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_list.count() && is_valid; ++i) {
      const uint64_t unit_id = unit_list.at(i).id();
      GET_FROM_ARRAY(unit_array_, unit_id, unit_ls_info);
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(unit_ls_info)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("unit not exist", KR(ret), K(unit_list), K(unit_id));
      } else if (!unit_ls_info->is_valid_for_normal_ls()) {
        is_valid = false;
        LOG_INFO("not vaild unit", KPC(unit_ls_info));
      }
    }//end for i
  }
  return ret;
}

int ObTenantLSBalanceInfo::split_hetero_zone_to_homo_unit_group(ObHeteroUGArray &hetero_ug_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else {
    int64_t unit_num = 0;
    hetero_ug_array.reset();
    FOREACH_CNT_X(zone_info, zone_array_, OB_SUCC(ret)) {
      if (OB_ISNULL(zone_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone info is empty", KR(ret));
      } else if (!zone_info->is_in_locality_) {
        //not in locality no need to check
      } else if (FALSE_IT(unit_num = zone_info->get_valid_unit_num())) {
      } else if (hetero_ug_array.empty()
                || unit_num != hetero_ug_array.at(0).count()) {
        if (hetero_ug_array.count() < 2) {
          ObUGArray ug_array;
          if (OB_FAIL(zone_info->get_ug_array(ug_array))) {
            LOG_WARN("failed to get ug array", KR(ret));
          } else if (OB_FAIL(hetero_ug_array.push_back(ug_array))) {
            LOG_WARN("failed to assign unit array", KR(ret));
          } else {
            LOG_INFO("push back homo ug", K(ug_array));
          }
        } else if (hetero_ug_array.count() == 2 && unit_num != hetero_ug_array.at(1).count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("has multi unit num", KR(ret), "zone", zone_info->zone_, K(unit_num),
              "ug1 count", hetero_ug_array.at(1).count());
        }
      }
    }//end for get homo ug array
    if (OB_SUCC(ret) && (hetero_ug_array.empty() || hetero_ug_array.count() > 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("has multi unit num", KR(ret), "ug array count", hetero_ug_array.count(), K(hetero_ug_array));
    }
  }
  return ret;
}

int ObTenantLSBalanceInfo::split_hetero_zone_to_homo_zone_stat(ObArray<ObArray<ObZoneLSStat*>> &hetero_zone_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else {
    ObHeteroUGArray hetero_ug_array;
    if (OB_FAIL(split_hetero_zone_to_homo_unit_group(hetero_ug_array))) {
      LOG_WARN("get hetero unit group array failed", KR(ret));
    } else if (hetero_ug_array.empty() || hetero_ug_array.count() > 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("has multi unit num", KR(ret), "ug array count", hetero_ug_array.count());
    } else {
      FOREACH_CNT_X(homo_ug_array, hetero_ug_array, OB_SUCC(ret)) {
        CK(!homo_ug_array->empty(), OB_NOT_NULL(homo_ug_array->at(0)));
        ObArray<ObZoneLSStat*> zone_stat_array;
        if (FAILEDx(homo_ug_array->at(0)->get_zone_info_array(zone_stat_array))) {
          LOG_WARN("get zone info array failed", KR(ret), KPC(homo_ug_array->at(0)));
        } else if (OB_FAIL(hetero_zone_array.push_back(zone_stat_array))) {
          LOG_WARN("push back failed", KR(ret));
        }
      }// end for each homo ug array
    }
  }
  return ret;
}

int ObTenantLSBalanceInfo::get_valid_unit_list(share::ObUnitIDList &unit_list)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else {
    //TODO根据当前unit_group分布中，各取一个日志流组个数最少的unit_group，组成最终的unit_list
    //异构zone可以组成两组同构zone
    ObHeteroUGArray hetero_ug_array;
    if (OB_FAIL(split_hetero_zone_to_homo_unit_group(hetero_ug_array))) {
      LOG_WARN("failed to get homo unit group", KR(ret));
    }

    //unit_list等于两组unit_group中各取一组unit_list
    FOREACH_CNT_X(ug_array, hetero_ug_array, OB_SUCC(ret)) {
      ObUnitGroupStat *ug_info = NULL;
      ObUnitIDList tmp_unit_list;
      if (OB_ISNULL(ug_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit array is empty", KR(ret));
      } else if (OB_FAIL(get_min_unit_group(*ug_array, ug_info))) {
        LOG_WARN("failed to get min unit list", KR(ret), KPC(ug_array));
      } else if (OB_ISNULL(ug_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ug info is null", KR(ret), KPC(ug_array));
      } else if (OB_FAIL(get_ug_locality_unit_list(*ug_info, tmp_unit_list))) {
        LOG_WARN("failed to get ug locality unit list", KR(ret), KPC(ug_info));
      } else if (OB_FAIL(append_array_no_dup(unit_list, tmp_unit_list))) {
        LOG_WARN("failed to append unit list", KR(ret), K(tmp_unit_list), K(unit_list));
      }
    }
  }
  return ret;
}

int ObTenantLSBalanceInfo::get_min_unit_group(common::ObIArray<ObUnitGroupStat*> &ug_array,
    ObUnitGroupStat* &ug_info)
{
  int ret = OB_SUCCESS;
  ug_info = NULL;
  if (OB_UNLIKELY(0 >= ug_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit group is empty", KR(ret), K(ug_array));
  } else {
    ARRAY_FOREACH(ug_array, idx) {
      ObUnitGroupStat* ug = ug_array.at(idx);
      if (OB_ISNULL(ug)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit group is unexpected", KR(ret), K(ug_array));
      } else if (OB_ISNULL(ug_info)
          || ug->get_ls_group_count() < ug_info->get_ls_group_count()) {
        ug_info = ug;
      }
    }//end if
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ug_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target ug is null", KR(ret), K(ug_array));
    } else {
      LOG_INFO("choose min unit group", KPC(ug_info));
    }
  }
  return ret;
}

int ObTenantLSBalanceInfo::get_max_unit_group(common::ObIArray<ObUnitGroupStat*> &ug_array,
    ObUnitGroupStat* &ug_info)
{
  int ret = OB_SUCCESS;
  ug_info = NULL;
  if (OB_UNLIKELY(0 >= ug_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit group is empty", KR(ret), K(ug_array));
  } else {
    ARRAY_FOREACH(ug_array, idx) {
      ObUnitGroupStat* ug = ug_array.at(idx);
      if (OB_ISNULL(ug)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit group is unexpected", KR(ret), K(ug_array));
      } else if (OB_ISNULL(ug_info)
          || ug->get_ls_group_count() > ug_info->get_ls_group_count()) {
        ug_info = ug;
      }
    }//end if
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ug_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target ug is null", KR(ret), K(ug_array));
    }
  }
  return ret;
}

int ObTenantLSBalanceInfo::get_unit_zone_info(const uint64_t unit_id, ObZoneLSStat* &zone_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat", KR(ret));
  } else if (OB_UNLIKELY(0 == unit_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit id is invalid", KR(ret), K(unit_id));
  } else {
    ObUnitLSStat *unit_stat = NULL;
    if (OB_FAIL(get_unit_ls_info(unit_id, unit_stat))) {
      LOG_WARN("failed to get unit ls info", KR(ret), K(unit_id));
    } else if (OB_ISNULL(unit_stat) || !unit_stat->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit stat is null", KR(ret), K(unit_id), K(unit_stat));
    } else if (OB_ISNULL(unit_stat->zone_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone stat is null", KR(ret), KPC(unit_stat));
    } else {
      zone_info = unit_stat->zone_stat_;
    }
  }
  return ret;
}

int ObTenantLSBalanceInfo::get_next_ls_status_info(
    const int64_t start_pos,
    ObLSGroupStat &lg_stat,
    int64_t &pos,
    ObLSStatusInfo *&status_info)
{
  int ret = OB_SUCCESS;
  status_info = nullptr;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!lg_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(lg_stat));
  } else if (OB_FAIL(lg_stat.ls_info_set_.find_next(start_pos, pos))) {
    LOG_WARN("fail to find next", KR(ret), K(pos), K(start_pos), K(lg_stat));
  } else if (pos < 0 || pos >= normal_ls_info_.size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pos", KR(ret), K(pos), K(start_pos), K(lg_stat));
  } else {
    status_info = &normal_ls_info_[pos];
  }
  return ret;
}

int ObTenantLSBalanceInfo::get_ls_status_in_lg(ObLSGroupStat &lg_stat,
    ObArray<ObLSStatusInfo*> &ls_status_array)
{
  int ret = OB_SUCCESS;
  //ls_status_array can not reset
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!lg_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(lg_stat));
  } else {
    int64_t start_pos = -1, pos = 0;
    const double src_factor = 1;
    ObLSStatusInfo *status_info = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < lg_stat.ls_count_in_group(); ++i) {
      if (OB_FAIL(get_next_ls_status_info(start_pos, lg_stat, pos, status_info))) {
        LOG_WARN("fail to get next ls status info", KR(ret), K(start_pos), K(lg_stat));
      } else if (OB_ISNULL(status_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("status_info is null", KR(ret), KP(status_info), K(start_pos), K(pos), K(lg_stat));
      } else if (OB_FAIL(ls_status_array.push_back(status_info))) {
        LOG_WARN("failed to get next ls status", KR(ret), K(i));
      }
      start_pos = pos;
    }
  }
  return ret;
}

int ObTenantLSBalanceInfo::get_all_ls_group(ObArray<ObLSGroupStat*> &ls_group_array)
{
  int ret = OB_SUCCESS;
  ls_group_array.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  }
  ARRAY_FOREACH(ls_group_array_, idx) {
    ObLSGroupStat& ls_group = ls_group_array_.at(idx);
    if (OB_FAIL(ls_group_array.push_back(&ls_group))) {
      LOG_WARN("failed to push back", KR(ret), K(idx));
    }
  }
  return ret;
}

//////end ObTenantLSBalanceInfo
///////ObLSBalanceStrategy
int ObLSBalanceStrategy::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_info_)
     || OB_ISNULL(sql_proxy_)
     || OB_ISNULL(job_) || OB_ISNULL(task_array_)
     || OB_ISNULL(lg_op_array_)
     || OB_ISNULL(unit_ug_op_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ptr is null", KR(ret), KP(tenant_info_), KP(sql_proxy_),
                            KP(job_), KP(task_array_), KP(lg_op_array_),
                            KP(unit_ug_op_array_));
  } else if (OB_FAIL(tenant_info_->check_inner_stat())) {
    LOG_WARN("failed to check inner stat", KR(ret), K(tenant_info_));
  }
  return ret;
}

int ObLSBalanceStrategy::generate_balance_job(const share::ObBalanceStrategy &balance_strategy, const bool only_job_strategy)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!balance_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("balance strategy is invalid", KR(ret), K(balance_strategy));
  } else {
    ObBalanceJobType job_type(ObBalanceJobType::BALANCE_JOB_LS);
    ObBalanceJobStatus job_status(ObBalanceJobStatus::BALANCE_JOB_STATUS_DOING);
    ObBalanceJobID job_id = specified_job_id_;
    ObString comment;
    const uint64_t tenant_id = tenant_info_->tenant_id_;
    if (job_id.is_valid()) {
    } else if (!tenant_info_->is_primary() || only_job_strategy) {
      job_id = 0;
    } else if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id, job_id))) {
      LOG_WARN("generate unique id for balance job fail", KR(ret), K(tenant_id));
    }

    if (FAILEDx(job_->init(tenant_id, job_id, job_type, job_status, comment, balance_strategy))) {
      LOG_WARN("failed to init job", KR(ret), K(tenant_id), K(job_id), K(job_type),
          K(job_status), K(balance_strategy));
    }
  }
  return ret;
}

int ObLSBalanceStrategy::construct_ls_part_info(const ObSplitLSParam &src_ls,
    const share::ObLSID &dest_ls_id,
    ObTransferPartList &part_list)
{
  int ret = OB_SUCCESS;
  ObLSID src_ls_id = src_ls.get_ls_id();
  const double factor = src_ls.get_current_factor();
  ObLSBalanceGroupInfo *src_ls_bg_info = NULL;
  ObLSBalanceGroupInfo *dst_ls_bg_info = NULL;
  part_list.reset();

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!src_ls.is_valid() || !src_ls_id.is_valid() || !dest_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src ls or dest ls is invalid", KR(ret), K(src_ls), K(src_ls_id), K(dest_ls_id));
  } else if (OB_FAIL(tenant_ls_bg_info_.get(src_ls_id, src_ls_bg_info))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      ISTAT("src ls is empty, no need to transfer out", KR(ret), K(src_ls_id));
    } else {
      LOG_WARN("get src ls balance group info fail", KR(ret), K(src_ls_id), K(src_ls));
    }
  } else if (OB_FAIL(tenant_ls_bg_info_.get_or_create(dest_ls_id, dst_ls_bg_info))) {
    LOG_WARN("get dest ls balance group info fail", KR(ret), K(dest_ls_id));
  } else if (OB_ISNULL(src_ls_bg_info) || OB_ISNULL(dst_ls_bg_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls balance group info", KR(ret), K(src_ls_bg_info), K(src_ls_id),
        K(dst_ls_bg_info), K(dest_ls_id));
  } else if (OB_FAIL(src_ls_bg_info->transfer_out_by_factor(*dst_ls_bg_info, factor, part_list))) {
    LOG_WARN("transfer out part list from LS balance group info fail", KR(ret), K(factor),
        KPC(src_ls_bg_info), KPC(dst_ls_bg_info), K(part_list));
  }
  return ret;
}

int ObLSBalanceStrategy::try_construct_job_unit_list_op(const share::ObBalanceStrategy &balance_strategy, const bool only_job_strategy)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(balance_strategy.has_balance_task())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can not has unit list change", KR(ret), K(balance_strategy));
  } else {
    ObArray<ObLSGroupStat> &ls_group_array = tenant_info_->ls_group_array_;
    FOREACH_CNT_X(ls_group, ls_group_array, OB_SUCC(ret)) {
      CK(OB_NOT_NULL(ls_group))
      if (OB_FAIL(ret)) {
      } else if (ls_group->need_change_unit_list()) {
        ObLSGroupUnitListOp lg_op;
        ObLSID ls_id;
        if (OB_FAIL(lg_op.init(ls_group->lg_id_, ls_id, ls_group->current_unit_list_,
                ls_group->target_unit_list_, ls_group->ls_count_in_group()))) {
          LOG_WARN("failed to init ls group op", KR(ret), KPC(ls_group));
        } else if (OB_FAIL(lg_op_array_->push_back(lg_op))) {
          LOG_WARN("failed to push back", KR(ret), K(lg_op));
        }
      }
    }
    if (OB_SUCC(ret) && lg_op_array_->count() > 0) {
      if (OB_FAIL(generate_balance_job(balance_strategy, only_job_strategy))) {
        LOG_WARN("failed to generate balance job", KR(ret), K(balance_strategy));
      }
    }
  }
  return ret;
}
int ObLSBalanceStrategy::construct_src_split_param_array(
    ObLSGroupStat &lg_stat,
    ObSplitLSParamArray &src_ls)
{
  int ret = OB_SUCCESS;
  //can not reset src ls
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!lg_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(lg_stat));
  } else {
    ObArray<ObLSStatusInfo*> ls_status_array;
    const double src_factor = 1;
    if (OB_FAIL(tenant_info_->get_ls_status_in_lg(lg_stat, ls_status_array))) {
      LOG_WARN("failed to get ls status", KR(ret), K(lg_stat));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_status_array.count(); ++i) {
      ObLSStatusInfo *status_info = ls_status_array.at(i);
      CK(OB_NOT_NULL(status_info))
      if (OB_SUCC(ret)) {
        ObSplitLSParam param(status_info, src_factor);
        if (OB_FAIL(src_ls.push_back(param))) {
          LOG_WARN("failed to push back param", KR(ret), K(param), K(i));
        }
      }
    }
  }
  return ret;
}

int ObLSBalanceStrategy::construct_expand_dest_param(
      const int64_t lack_ls_count,
      ObSplitLSParamArray &src_ls,
      ObIArray<ObSplitLSParamArray> &dest_array)
{
  int ret = OB_SUCCESS;
  dest_array.reset();
  if (OB_UNLIKELY(0 == lack_ls_count || 0 == src_ls.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(lack_ls_count), K(src_ls));
  } else {
    const double each_ls_target_factor = double(src_ls.count()) / (src_ls.count() + lack_ls_count);
    if (each_ls_target_factor <= OB_DOUBLE_EPSINON) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("too many lack ls count", KR(ret), K(each_ls_target_factor), K(lack_ls_count), K(src_ls));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < lack_ls_count; ++i) {
      double need_factor = each_ls_target_factor;
      ObSplitLSParamArray src_array;
      for (int64_t j = 0; OB_SUCC(ret) && j < src_ls.count() && need_factor > OB_DOUBLE_EPSINON; ++j) {
        ObSplitLSParam &param = src_ls.at(j);
        double get_factor = param.reduce_factor_for_dest(need_factor, each_ls_target_factor);
        if (get_factor > OB_DOUBLE_EPSINON) {
          ObSplitLSParam split_param(param.get_ls_info(), get_factor);
          need_factor -= get_factor;
          if (OB_FAIL(src_array.push_back(split_param))) {
            LOG_WARN("failed to push back split param", KR(ret), K(split_param));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(0 >= src_array.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("src array is empty", KR(ret), K(src_ls));
      } else if (OB_FAIL(dest_array.push_back(src_array))) {
        LOG_WARN("failed to push back src array", KR(ret), K(i), K(src_array));
      }
      LOG_INFO("construct expand dest param", KR(ret), K(lack_ls_count), K(src_ls), K(dest_array));
    }
  }
  return ret;
}

int ObLSBalanceStrategy::construct_shrink_src_param(
    const int64_t target_count,
    ObSplitLSParamArray &src_ls,
    ObIArray<ObSplitLSParamArray> &dest_split_array)
{
  int ret = OB_SUCCESS;
  dest_split_array.reset();
  if (OB_UNLIKELY(0 == target_count || 0 == src_ls.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(target_count), K(src_ls));
  } else {
    const double each_ls_target_factor = double(src_ls.count()) / (target_count);
    if (each_ls_target_factor <= OB_DOUBLE_EPSINON) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("too many ls", KR(ret), K(each_ls_target_factor), K(target_count), K(src_ls));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < target_count; ++i) {
      double need_factor = each_ls_target_factor;
      ObSplitLSParamArray src_array;
      for (int64_t j = 0; OB_SUCC(ret) && j < src_ls.count() && need_factor > OB_DOUBLE_EPSINON; ++j) {
        ObSplitLSParam &param = src_ls.at(j);
        double get_factor = param.reduce_enough_factor(need_factor);
        if (!(get_factor)) { // strictly equal to zero
          //empty
        } else if (OB_DOUBLE_EPSINON >= get_factor) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("factor is too small", KR(ret), K(need_factor), K(src_ls), K(src_array), K(dest_split_array));
        } else {
          need_factor -= get_factor;
          if (OB_DOUBLE_EPSINON >= param.get_current_factor()) {
            param.reduce_all();
            //for ex
            //if current ls is 3, need shrink to 2, first ls need transfer, second need merge
            get_factor = 1;
          }
          ObSplitLSParam split_param(param.get_ls_info(), get_factor);
          LOG_TRACE("split param", KR(ret), K(split_param), K(i), K(j));
          if (OB_FAIL(src_array.push_back(split_param))) {
            LOG_WARN("failed to push back split param", KR(ret), K(split_param));
          }
        }
      }//end for j
      if (FAILEDx(dest_split_array.push_back(src_array))) {
        LOG_WARN("failed to push back src array", KR(ret), K(i), K(src_array));
      }
      LOG_INFO("construct shrink src param", KR(ret), K(target_count), K(src_ls), K(dest_split_array));
    }
  }
  return ret;
}

///////end ObLSBalanceStrategy
///////ObDupLSBalance
int ObDupLSBalance::balance(const bool only_job_strategy)
{
  int ret = OB_SUCCESS;
  ObBalanceStrategy balance_strate(ObBalanceStrategy::LB_DUP_LS);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (!tenant_info_->job_desc_.get_enable_rebalance()) {
    LOG_INFO("tenant can not rebalance, no need do dup LS balance");
  } else if (tenant_info_->duplicate_ls_info_.empty()
      || (1 == tenant_info_->duplicate_ls_info_.count()
          && 0 == tenant_info_->duplicate_ls_info_.at(0).get_ls_group_id())) {
    //nothing todo
  } else if (OB_FAIL(generate_balance_job(balance_strate, only_job_strategy))) {
    LOG_WARN("failed to generate balance job", KR(ret), KPC(tenant_info_));
  } else if (only_job_strategy) {
    // skip
  } else if (1 == tenant_info_->duplicate_ls_info_.count()
            && 0 != tenant_info_->duplicate_ls_info_.at(0).get_ls_group_id()) {
    //need generate ls alter task
    ObLSStatusInfo &dup_ls = tenant_info_->duplicate_ls_info_.at(0);
    if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_alter_task(
      tenant_info_->tenant_id_, job_->get_job_id(), 0,
      dup_ls.get_ls_id(), balance_strate, *task_array_))) {
      LOG_WARN("failed to add ls alter task", KR(ret), K(dup_ls), K(tenant_info_->tenant_id_),
        K(job_), K(task_array_));
    }
  } else if (1 < tenant_info_->duplicate_ls_info_.count()) {
    //need shrink
    if (tenant_info_->job_desc_.get_enable_transfer()) {
      //可以走合并
      if (OB_FAIL(generate_shrink_task_())) {
        LOG_WARN("failed to generate shrink task", KR(ret), "duplicate ls", tenant_info_->duplicate_ls_info_);
      }
    } else {
      //如果没有开transfer，也是需要把dup上面的ls_group_id清理了掉，如果保持ls_group_id和unit_list
      //会导致机器宕机后，缺少F副本，因为F副本一定需要在unit_list的机器上
      ARRAY_FOREACH(tenant_info_->duplicate_ls_info_, idx) {
        ObLSStatusInfo &dup_ls = tenant_info_->duplicate_ls_info_.at(idx);
        if (dup_ls.get_ls_group_id() == 0) {
        } else if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_alter_task(
                tenant_info_->tenant_id_, job_->get_job_id(), 0,
                dup_ls.get_ls_id(), balance_strate, *task_array_))) {
          LOG_WARN("failed to add ls alter task", KR(ret), K(dup_ls), K(tenant_info_->tenant_id_),
              K(job_), K(task_array_));
        }
      }//end for dup
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not be here", KR(ret), KPC(tenant_info_));
  }
  return ret;
}

int ObDupLSBalance::generate_shrink_task_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (1 >= tenant_info_->duplicate_ls_info_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), K(tenant_info_->duplicate_ls_info_.count()));
  } else {
    const uint64_t tenant_id = tenant_info_->tenant_id_;
    ObLSStatusInfoArray &dup_ls_array = tenant_info_->duplicate_ls_info_;
    ObLSStatusInfo::Compare cmp;
    lib::ob_sort(dup_ls_array.begin(), dup_ls_array.end(), cmp);
    const ObLSID dest_ls_id = dup_ls_array.at(0).get_ls_id(); // smallest dup ls id
    uint64_t ls_group_id = 0;
    //获取一组有效的unit_list和ls_group
    if (OB_FAIL(get_ls_group_(ls_group_id))) {
      LOG_WARN("failed to get target unit list", KR(ret), KPC(tenant_info_));
    }
    //所有的duplicate_ls都修改成这样的unit_list和ls_group
    for (int64_t i = 0; OB_SUCC(ret) && i < dup_ls_array.count(); ++i) {
      ObLSStatusInfo &ls_info = dup_ls_array.at(i);
      if (ls_info.get_ls_group_id() != ls_group_id) {
        //生成ls_alter任务
        if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_alter_task(
          tenant_id, job_->get_job_id(), ls_group_id,
          ls_info.get_ls_id(), job_->get_balance_strategy(), *task_array_))) {
          LOG_WARN("failed to add ls alter task", KR(ret), K(ls_info),
                   K(tenant_id), K(job_), K(task_array_));
        }
      }
    }//end for alter each ls
    //生成merge操作
    for (int64_t i = 1; OB_SUCC(ret) && i < dup_ls_array.count(); ++i) {
      ObLSStatusInfo &ls_info = dup_ls_array.at(i);
      if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_merge_task(tenant_id, job_->get_job_id(),
      ls_group_id, ls_info.get_ls_id(), dest_ls_id, job_->get_balance_strategy(), *task_array_))) {
        LOG_WARN("failed to add ls merge task", KR(ret), K(ls_info), K(dest_ls_id),
                 K(tenant_id), K(job_), K(task_array_));
      }
    }
    //还回去duplicate应该有的unit_list和ls_group
    if (FAILEDx(ObLSBalanceTaskHelper::add_ls_alter_task(tenant_id, job_->get_job_id(),
    0, dest_ls_id, job_->get_balance_strategy(), *task_array_))) {
      LOG_WARN("failed to add ls alter task", KR(ret), K(dest_ls_id),
               K(tenant_id), K(job_), K(task_array_));
    }
  }
  return ret;
}

int ObDupLSBalance::get_ls_group_(uint64_t &ls_group_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else {
    ObLSStatusInfoArray &dup_ls_array = tenant_info_->duplicate_ls_info_;
    //使用现有的unit_list，获取现有的unit
    bool unit_list_valid = false;
    ObUnitIDList unit_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < dup_ls_array.count() && !unit_list_valid; ++i) {
      ObLSStatusInfo &ls_info = dup_ls_array.at(i);
      if (0 != ls_info.get_ls_group_id()) {
        //校验unit_group或者unit_list全是active
        //是否会存在ls_group有效，但是unit_group或者unit_list无效的场景？
        if (OB_FAIL(unit_list.assign(ls_info.get_unit_list()))) {
          LOG_WARN("failed to assign unit list", KR(ret), K(ls_info));
        } else if (unit_list.empty() && OB_FAIL(unit_list.push_back(ObDisplayUnitID(ls_info.unit_group_id_)))) {
          LOG_WARN("failed to assign unit list", KR(ret), K(ls_info));
        } else if (OB_FAIL(tenant_info_->check_unit_list_valid(unit_list, unit_list_valid))) {
          LOG_WARN("failed to check unit list valid", KR(ret), K(unit_list), K(ls_info));
        } else if (!unit_list_valid) {
          unit_list.reset();
        } else {
          ls_group_id = ls_info.get_ls_group_id();
        }
      }
    }//end for

    if (OB_SUCC(ret) && !unit_list_valid) {
      //从每个zone中选择一个unit构成unit_list
      if (OB_FAIL(tenant_info_->get_valid_unit_list(unit_list))) {
        LOG_WARN("failed to get valid unit list", KR(ret), KPC(tenant_info_));
      } else if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_group_id(sql_proxy_, tenant_info_->tenant_id_, ls_group_id))) {
        LOG_WARN("failed to fetch new ls group id", KR(ret), K(tenant_info_->tenant_id_));
      }
    }
  }
  return ret;
}
///////end ObDupLSBalance

////////////ObUnitGroupBalance
// check order:
// 1. if any unit is empty of ls_groups
// 2. if sys ls replica on any unit. LEADER and F replica has priority.
// 3. if any unit has less ls_groups
// 4. smaller unit id
bool ObUnitGroupBalance::ChooseGTSUnitComp::operator()(const ObUnitLSStat *left, const ObUnitLSStat *right)
{
  bool bret = false;
  bool priority_decided = false; // track if priority is already decided
  int ret = OB_SUCCESS; // dummy
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret_ = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", KR(ret_), KP(left), KP(right));
  } else if (OB_ISNULL(left->unit_) || OB_ISNULL(right->unit_)) {
    ret_ = OB_INNER_STAT_ERROR;
    LOG_WARN("unit is null", KR(ret_), KP(left->unit_), KP(right->unit_));
  } else if (OB_UNLIKELY(!sys_ls_info_.is_valid())) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN("sys_ls_info_ is invalid", KR(ret), K(sys_ls_info_));
  } else {
    // 1. check if any unit is empty of ls_group
    if ((left->get_ls_group_count() == 0) != (right->get_ls_group_count() == 0)) {
      priority_decided = true;
      if (left->get_ls_group_count() == 0) {
        bret = true;
      } else {
        bret = false;
      }
    // 2. check if any unit has sys_ls replica, LEADER and F replica has priority
    } else if (OB_FAIL(try_compare_by_sys_ls_replica(left, right, bret, priority_decided))) {
      ret_ = ret;
      LOG_WARN("failed to compare by sys ls replica", KR(ret), KPC(left), KPC(right));
    } else if (priority_decided) { // compare finish
    // 3. check if one unit has less ls_group
    } else if (left->get_ls_group_count() != right->get_ls_group_count()) {
      bret = left->get_ls_group_count() < right->get_ls_group_count();
    // 4. choose unit with smaller unit_id
    } else {
      bret = left->unit_->unit_id_ < right->unit_->unit_id_;
    }
  }
  return bret;
}

int ObUnitGroupBalance::ChooseGTSUnitComp::try_compare_by_sys_ls_replica(
    const ObUnitLSStat *left, const ObUnitLSStat *right, bool &bret, bool &priority_decided)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(left), OB_NOT_NULL(right));
  CK(OB_NOT_NULL(left->unit_), OB_NOT_NULL(right->unit_));
  if (OB_SUCC(ret)) {
    priority_decided = true;
    const ObLSReplica *replica_on_left = nullptr;
    const ObLSReplica *replica_on_right = nullptr;
    FOREACH_CNT_X(cur_replica, sys_ls_info_.get_replicas(), (nullptr == replica_on_left
        || nullptr == replica_on_right)) {
      if (cur_replica->get_server() == left->unit_->server_) {
        replica_on_left = cur_replica;
      }
      if (cur_replica->get_server() == right->unit_->server_) {
        replica_on_right = cur_replica;
      }
    }
    if (nullptr != replica_on_left && nullptr == replica_on_right) {
      bret = true;
    } else if (nullptr != replica_on_right && nullptr == replica_on_left) {
      bret = false;
    } else if (nullptr != replica_on_left && nullptr != replica_on_right) {
      if (replica_on_left->is_strong_leader() != replica_on_right->is_strong_leader()) {
        // one is leader and the other isn't
        bret = replica_on_left->is_strong_leader() ? true : false;
      } else if (replica_on_left->is_in_service() != replica_on_right->is_in_service()) {
        // one is in service and the other isn't
        bret = replica_on_left->is_in_service() ? true : false;
      } else if (replica_on_left->is_paxos_replica() != replica_on_right->is_paxos_replica()) {
        // one is F replica and the other isn't
        bret = replica_on_left->is_paxos_replica() ? true : false;
      } else {
        // both unit have sys ls replica, but can not decide priority
        priority_decided = false;
      }
    } else {
      // both unit have no sys ls replicas
      priority_decided = false;
    }
  }
  return ret;
}

int ObUnitGroupBalance::MaxWeightMatchHelper::init(const ObMatrix<int64_t> *weight)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(weight)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("weight is null", KR(ret));
  } else if (weight->get_row_count() != weight->get_column_count()
      || 0 == weight->get_row_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("weight shape not expected", KR(ret), KPC(weight));
  } else {
    node_n_ = weight->get_row_count();
    weight_ = weight;
    if (OB_FAIL(reset_all_())) {
      LOG_WARN("fail to reset all", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObUnitGroupBalance::MaxWeightMatchHelper::reset_all_()
{
  int ret = OB_SUCCESS;
  if (node_n_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("node_n_ not expected", KR(ret), K(node_n_));
  } else {
    lx_.reuse();
    ly_.reuse();
    vis_x_.reuse();
    vis_y_.reuse();
    match_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < node_n_; ++i) {
      if (OB_FAIL(lx_.push_back(0))) {
        LOG_WARN("lx_.push_back failed", KR(ret));
      } else if (OB_FAIL(ly_.push_back(0))) {
        LOG_WARN("ly_.push_back_failed", KR(ret));
      } else if (OB_FAIL(vis_x_.push_back(false))) {
        LOG_WARN("vis_x_.push_back_failed", KR(ret));
      } else if (OB_FAIL(vis_y_.push_back(false))) {
        LOG_WARN("vis_y_.push_back_failed", KR(ret));
      } else if (OB_FAIL(match_.push_back(-1))) {
        LOG_WARN("match_.push_back_failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObUnitGroupBalance::MaxWeightMatchHelper::reset_visited_()
{
  int ret = OB_SUCCESS;
  if (node_n_ <= 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("node_n_ not expected", KR(ret), K(node_n_));
  } else {
    vis_x_.reuse();
    vis_y_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < node_n_; i++) {
      if (OB_FAIL(vis_x_.push_back(false))) {
        LOG_WARN("vis_x_.push_back_failed", KR(ret));
      } else if (OB_FAIL(vis_y_.push_back(false))) {
        LOG_WARN("vis_y_.push_back_failed", KR(ret));
      }
    }
  }
  return ret;
}

int ObUnitGroupBalance::MaxWeightMatchHelper::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(weight_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("weight_ is null ptr", KR(ret), KP(weight_));
  } else {
    CK(weight_->get_column_count() == node_n_, weight_->get_row_count() == node_n_)
    CK(lx_.count() == node_n_, ly_.count() == node_n_)
    CK(vis_x_.count() == node_n_, vis_y_.count() == node_n_)
    CK(match_.count() == node_n_)
    if (OB_FAIL(ret)) {
      LOG_WARN("count not match", KR(ret), K(node_n_), KPC(weight_), K(lx_.count()), K(ly_.count()),
               K(vis_x_.count()), K(vis_y_.count()), K(match_.count()));
    }
  }
  return ret;
}

// search by DFS
int ObUnitGroupBalance::MaxWeightMatchHelper::search_path_(
    int64_t u, bool &found, char *buf, int64_t &buf_pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("info_buf is null ptr", KR(ret));
  } else if (u < 0 || u >= node_n_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(u));
  } else {
    vis_x_.at(u) = true;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(databuff_printf(buf, LOG_MSG_LEN, buf_pos, "X[%ld]", u))) {
      LOG_WARN("databuff_print fail", KR(tmp_ret));
    }
    for (int64_t v = 0; OB_SUCC(ret) && !found && v < node_n_; ++v) {
      int64_t weight_value = 0;
      if (vis_y_.at(v)) {
        // visited, skip
      } else if (OB_FAIL(weight_->get(u, v, weight_value))) {
        LOG_WARN("fail to get weight", KR(ret), K(u), K(v), KPC(weight_));
      } else if (lx_.at(u) + ly_.at(v) == weight_value) {
        vis_y_.at(v) = true;
        if (OB_TMP_FAIL(databuff_printf(buf, LOG_MSG_LEN, buf_pos, "->Y[%ld]->", v))) {
          LOG_WARN("databuff_print fail", KR(tmp_ret));
        }
        if (-1 == match_.at(v))  {
          found = true;
        } else if (OB_FAIL(search_path_(match_.at(v), found, buf, buf_pos))) {
          LOG_WARN("fail to search path", KR(ret), K(v), K(match_.at(v)));
        }
        if (OB_SUCC(ret) && found) {
          match_.at(v) = u;
        }
      }
    }
  }
  return ret;
}

// KM algorithm is a greedy algorithm, it will find a match with max weight sum
// Variables:
//  lx_[u] is label of x[u], ly_[v] is label of y[v]
//  match_[v] is index of x matched by y[v], -1 means no match
//  weight_[u][v] is weight of edge between x[u] and y[v]
//  vis_x_[u] and vis_y_[v] is visited flag of x[u] and y[v] in search path
int ObUnitGroupBalance::MaxWeightMatchHelper::do_match_by_KM_algorithm()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else {
    // Init label lx_[u] as max weight of weight[u][j] {j:0~N}
    for (int64_t i = 0; OB_SUCC(ret) && i < node_n_; ++i) {
      ly_.at(i) = 0;
      lx_.at(i) = -1;
      for (int64_t j = 0; OB_SUCC(ret) && j < node_n_; ++j) {
        int64_t weight_value = 0;
        if (OB_FAIL(weight_->get(i, j, weight_value))) {
          LOG_WARN("fail to get weight", KR(ret), K(i), K(j), KPC(weight_));
        } else {
          lx_.at(i) = MAX(lx_.at(i), weight_value);
        }
      }
    }
    LOG_INFO("[KM] init nodes label", KR(ret), K(lx_), K(ly_));
    // For each y[v], find a match path, if not found, decrease label of x[u]
    // and increase label of y[v] to find a new match path, until all x[u] have a match.
    char buf[LOG_MSG_LEN] = {'\0'};
    for (int64_t u = 0; OB_SUCC(ret) && u < node_n_; ++u) {
      bool found = false;
      while (OB_SUCC(ret) && !found) {
        memset(buf, 0, LOG_MSG_LEN);
        int64_t buf_pos = 0;
        if (OB_FAIL(reset_visited_())) {
          LOG_WARN("fail to reset visited", KR(ret));
        } else if (OB_FAIL(search_path_(u, found, buf, buf_pos))) {
          LOG_WARN("search path failed", KR(ret), K(u));
        } else if (found) {
          // search end
        } else {
          // update lx_ and ly_, prepare for next round search
          int64_t delta = INT64_MAX;
          for (int64_t i = 0; OB_SUCC(ret) && i < node_n_; ++i) {
            for (int64_t j = 0; OB_SUCC(ret) && j < node_n_; ++j) {
              if (vis_x_.at(i) && !vis_y_.at(j)) {
                int64_t weight_value = 0;
                if (OB_FAIL(weight_->get(i, j, weight_value))) {
                  LOG_WARN("fail to get weight", KR(ret), K(i), K(j), KPC(weight_));
                } else {
                  delta = MIN(delta, lx_.at(i) + ly_.at(j) - weight_value);
                }
              }
            }
          }
          for (int64_t i = 0; i < node_n_; ++i) {
            if (vis_x_.at(i)) {
              lx_.at(i) -= delta;
            }
            if (vis_y_.at(i)) {
              ly_.at(i) += delta;
            }
          }
        }
        LOG_INFO("[KM] do one round search path finished", KR(ret), K(u), K(found),
                 "search_track", ObString(buf), K(lx_), K(ly_), K(match_));
      }// end while
    }//end for x[u]
  }
  return ret;
}

int ObUnitGroupBalance::balance(const bool only_job_strategy)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (!tenant_info_->job_desc_.get_enable_rebalance()) {
    LOG_INFO("enable rebalance is off, can not do unit group balance");
  } else {
    // STEP 1: decide and update GTS standalone units
    if (tenant_info_->job_desc_.get_enable_gts_standalone()) {
      if (OB_FAIL(try_determine_and_set_gts_units_())) {
        LOG_WARN("failed to choose and update gts units", KR(ret));
      }
    } else {
      // if gts_standalone is disabled, sys_ls unit_list will be cleared
      if (OB_FAIL(try_clear_sys_ls_unit_list_())) {
        LOG_WARN("failed to clear sys ls unit list", KR(ret));
      }
    }

    // STEP 2: reorganize normal unit groups
    ObArray<ObArray<ObZoneLSStat*>> hetero_zones;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tenant_info_->split_hetero_zone_to_homo_zone_stat(hetero_zones))) {
      LOG_WARN("failed to get homo unit group", KR(ret));
    } else {
      // reorganize two set of homo ug separately
      ARRAY_FOREACH_X(hetero_zones, idx, cnt, OB_SUCC(ret)) {
        const ObArray<ObZoneLSStat*> &homo_zone_stats = hetero_zones.at(idx);
        if (OB_FAIL(reorganize_homo_zones_ug_(homo_zone_stats))) {
          LOG_WARN("failed to reorganize homo zones", KR(ret));
        }
      }
    }

    // STEP 3: record balance job
    if (OB_FAIL(ret)) {
    } else if (lg_op_array_->empty() && unit_ug_op_array_->empty()) {
      // skip
    } else {
      share::ObBalanceStrategy balance_strategy(share::ObBalanceStrategy::LB_UNIT_GROUP);
      if (OB_FAIL(generate_balance_job(balance_strategy, only_job_strategy))) {
        LOG_WARN("failed to generate balance job", KR(ret), K(balance_strategy));
      }
    }
  }
  return ret;
}

int ObUnitGroupBalance::try_determine_and_set_gts_units_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else {
    // check if need to set gts_unit, and if each zone unit_num > 2
    bool is_gts_units_all_set = true;
    ARRAY_FOREACH(tenant_info_->zone_array_, idx) {
      const ObZoneLSStat &zone_stat = tenant_info_->zone_array_.at(idx);
      if (!zone_stat.is_in_locality_) {
        // skip, ignore zone not in locality
      } else if (nullptr != zone_stat.gts_unit_) {
        // skip, gts_unit already set
      } else if (zone_stat.get_valid_unit_num() < 2) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone valid unit num not enough for gts_standalone", KR(ret), K(tenant_info_->zone_array_));
      } else {
        is_gts_units_all_set = false;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_gts_units_all_set) {
      LOG_INFO("sys_ls unit list already fully set, no need to determine gts standalone units", KR(ret),
              "sys_ls_unit_list", tenant_info_->sys_ls_info_.unit_id_list_, "zone_array", tenant_info_->zone_array_);
    } else {
      // get sys ls status info
      ObLSInfo sys_ls_info;
      if (OB_ISNULL(GCTX.lst_operator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("GCTX.lst_operator_ is null ptr", KR(ret));
      } else if (OB_FAIL(GCTX.lst_operator_->get(
          GCONF.cluster_id,
          tenant_info_->tenant_id_,
          SYS_LS,
          share::ObLSTable::DEFAULT_MODE,
          sys_ls_info))) {
        LOG_WARN("fail to get sys_ls info", KR(ret), "tenant_id", tenant_info_->tenant_id_);
      }
      // determine and set gts unit by each zone
      ObUnitIDList sys_ls_new_unit_list;
      ARRAY_FOREACH(tenant_info_->zone_array_, idx) {
        ObZoneLSStat &zone_stat = tenant_info_->zone_array_.at(idx);
        if (OB_FAIL(determine_and_set_zone_gts_unit_(sys_ls_info, zone_stat))) {
          LOG_WARN("fail to choose zone gts unit", KR(ret), K(sys_ls_info), K(zone_stat));
        } else if (OB_ISNULL(zone_stat.gts_unit_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("gts_unit_ is null after determine gts unit is unexpected", KR(ret), K(zone_stat));
        } else if (!zone_stat.is_in_locality_) {
          // zone not in locality, just mark a dummy gts_unit, no need to add to unit_list
        } else if (OB_FAIL(sys_ls_new_unit_list.push_back(ObDisplayUnitID(zone_stat.gts_unit_->unit_id_)))) {
          LOG_WARN("fail to push back", KR(ret), KPC(zone_stat.gts_unit_));
        }
      }
      // update sys ls unit list, use each sys_unit_ to construct new unit list
      if (OB_FAIL(ret)) {
      } else if (sys_ls_new_unit_list == tenant_info_->sys_ls_info_.unit_id_list_) {
        LOG_INFO("sys_ls unit_list not changed, skip", KR(ret), K(sys_ls_new_unit_list), K(tenant_info_->sys_ls_info_));
      } else {
        ObLSGroupUnitListOp lg_op;
        if (OB_FAIL(lg_op.init(tenant_info_->sys_ls_info_.get_ls_group_id(),
                               SYS_LS,
                               tenant_info_->sys_ls_info_.get_unit_list(),
                               sys_ls_new_unit_list, 1))) {
          LOG_WARN("failed to init ls group op", KR(ret), K(tenant_info_->sys_ls_info_), K(sys_ls_new_unit_list));
        } else if (OB_FAIL(lg_op_array_->push_back(lg_op))) {
          LOG_WARN("failed to push back", KR(ret), K(lg_op));
        }
        LOG_INFO("finish update gts units", KR(ret), K(lg_op));
      }
    }
  }
  return ret;
}

int ObUnitGroupBalance::try_clear_sys_ls_unit_list_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (tenant_info_->sys_ls_info_.get_unit_list().empty()) {
    // sys ls unit_list is already empty, nothing to do
  } else {
    ObLSGroupUnitListOp lg_op;
    if (OB_FAIL(lg_op.init(tenant_info_->sys_ls_info_.get_ls_group_id(),
                            SYS_LS,
                            tenant_info_->sys_ls_info_.get_unit_list(),
                            ObUnitIDList(), 1))) {
      LOG_WARN("failed to init ls group op", KR(ret), K(tenant_info_->sys_ls_info_));
    } else if (OB_FAIL(lg_op_array_->push_back(lg_op))) {
      LOG_WARN("failed to push back", KR(ret), K(lg_op));
    }
    LOG_INFO("finish clear sys ls unit_list", KR(ret), K(lg_op));
  }
  return ret;
}

int ObUnitGroupBalance::determine_and_set_zone_gts_unit_(const ObLSInfo &sys_ls_info, ObZoneLSStat &zone_stat)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (nullptr != zone_stat.gts_unit_) {
    LOG_INFO("zone gts_unit_ already set, should be unit in sys ls unit_list, keep using it", KR(ret), K(zone_stat),
             "sys_ls_unit_list", tenant_info_->sys_ls_info_.get_unit_list());
  } else {
    // zone gts_unit_ is null, need to determine
    ObIArray<ObUnitLSStat *> &valid_unit_array = zone_stat.valid_unit_array_;
    ObArray<ObUnitLSStat *> sorted_unit_array;
    ChooseGTSUnitComp cmp(sys_ls_info);
    if (OB_FAIL(sorted_unit_array.assign(valid_unit_array))) {
      LOG_WARN("assign unit stat array failed", KR(ret), K(valid_unit_array));
    } else if (FALSE_IT(lib::ob_sort(sorted_unit_array.begin(), sorted_unit_array.end(), cmp))) {
    } else if (OB_FAIL(cmp.get_ret())) {
      LOG_WARN("sort by choose gts unit cmp failed", KR(ret), K(sys_ls_info), K(zone_stat));
    } else {
      ObUnitLSStat* const determined_gts_unit = sorted_unit_array.at(0);
      // update zone_stat in memory
      int64_t idx_in_valid_array = -1;
      if (OB_ISNULL(determined_gts_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit_stat ptr is null", KR(ret), K(zone_stat));
      } else if (!has_exist_in_array(valid_unit_array, determined_gts_unit, &idx_in_valid_array)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("determined gts_unit not in valid_unit_array unexpected", KR(ret), KPC(determined_gts_unit));
      } else {
        CK(idx_in_valid_array >= 0 && idx_in_valid_array < valid_unit_array.count())
        if (FAILEDx(valid_unit_array.remove(idx_in_valid_array))) {
          LOG_WARN("failed to remove unit_stat in valid_unit_array_", KR(ret), K(idx_in_valid_array));
        } else {
          determined_gts_unit->is_sys_standalone_ = true;
          zone_stat.gts_unit_ = determined_gts_unit;
        }
        FLOG_INFO("choose zone GTS standalone unit", KR(ret), K(zone_stat), K(sys_ls_info), KPC(determined_gts_unit));
      }
    }
  }
  return ret;
}

// For a set of homo-zones, choose one zone as fixed ref-zone, then adjust ug_id of other zones one by one.
// For each of other zones, reorganize ug_id of zone to make more LS Groups distribute inside a UG with ref-zone.
// Use KM algorithm to match adjust-zone units with ref-zone units, then update ug_ids of adjust-zone
//   units to matched ref-zone units's ug_ids.
int ObUnitGroupBalance::reorganize_homo_zones_ug_(const common::ObIArray<ObZoneLSStat*> &homo_zone_stats)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (0 == homo_zone_stats.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid homo zone stats, array is empty", KR(ret));
  } else if (OB_ISNULL(homo_zone_stats.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("homo_zone_stats has null ptr", KR(ret));
  } else if (1 == homo_zone_stats.count()) {
    LOG_INFO("only one zone, no need to reorganize", KR(ret), KPC(homo_zone_stats.at(0)));
  } else {
    ObZoneLSStat *ref_zone = nullptr;
    // 1. sort zone stats with balanced zone ahead, pick 1st as ref_zone.
    ObArray<ObZoneLSStat *> sorted_zone_stats;
    ChooseZoneCmp zone_cmp;
    if (OB_FAIL(sorted_zone_stats.assign(homo_zone_stats))) {
      LOG_WARN("fail to assign sorted zone_stats", KR(ret));
    } else if (FALSE_IT(lib::ob_sort(sorted_zone_stats.begin(), sorted_zone_stats.end(), zone_cmp))) {
    } else if (OB_FAIL(zone_cmp.get_ret())) {
      LOG_WARN("fail to sort zone stats", KR(ret));
    } else {
      bool same_priority = zone_cmp(sorted_zone_stats.at(sorted_zone_stats.count() - 1), sorted_zone_stats.at(0));
      if (OB_FAIL(zone_cmp.get_ret())) {
        LOG_WARN("fail to cmp", KR(ret));
      } else if (same_priority) {
        // same priority, use first zone of original zone array as ref_zone
        ref_zone = homo_zone_stats.at(0);
      } else {
        ref_zone = sorted_zone_stats.at(0);
      }
      CK(OB_NOT_NULL(ref_zone));
    }
    // 2. reorganize ug of each of other zones refer to ref_zone
    for (int64_t i = 0; OB_SUCC(ret) && i < homo_zone_stats.count(); ++i) {
      const ObZoneLSStat *adjust_zone = homo_zone_stats.at(i);
      CK(OB_NOT_NULL(adjust_zone));
      if (adjust_zone != ref_zone && adjust_zone->is_in_locality_) {
        if (OB_FAIL(reorganize_zone_ug_to_ref_zone_(*ref_zone, *adjust_zone))) {
          LOG_WARN("fail to adjust zone ug to ref_zone", KR(ret), KPC(ref_zone), KPC(adjust_zone));
        }
      }
    }//end for each zone
  }
  return ret;
}

int ObUnitGroupBalance::reorganize_zone_ug_to_ref_zone_(const ObZoneLSStat &ref_zone, const ObZoneLSStat &adjust_zone)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (ref_zone.zone_ == adjust_zone.zone_
      || ref_zone.get_valid_unit_num() != adjust_zone.get_valid_unit_num()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ref zone and adjust zone should be different homo zones", KR(ret), K(ref_zone), K(adjust_zone));
  } else {
    // adjust GTS unit
    bool is_gts_unit_adjusted = false; // if gts unit is adjusted, valid units also have to adjust.
    if (tenant_info_->job_desc_.get_enable_gts_standalone()) {
      const ObUnitLSStat *ref_gts_unit = ref_zone.gts_unit_;
      const ObUnitLSStat *adjust_gts_unit = adjust_zone.gts_unit_;
      if (OB_ISNULL(ref_gts_unit) || OB_ISNULL(adjust_gts_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("gts_unit_ should not be nullptr", KR(ret), KP(ref_gts_unit), KP(adjust_gts_unit));
      } else if (OB_ISNULL(ref_gts_unit->unit_) || OB_ISNULL(adjust_gts_unit->unit_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit is nullptr", KR(ret), KPC(ref_gts_unit), KPC(adjust_gts_unit));
      } else if (ref_gts_unit->unit_->unit_group_id_ != adjust_gts_unit->unit_->unit_group_id_) {
        ObUnitUGOp unit_ug_op(adjust_gts_unit->unit_id_,
                              adjust_gts_unit->unit_->unit_group_id_,
                              ref_gts_unit->unit_->unit_group_id_);
        if (OB_FAIL(unit_ug_op_array_->push_back(unit_ug_op))) {
          LOG_WARN("fail to push back unit_ug_op", KR(ret), K(unit_ug_op));
        } else {
          is_gts_unit_adjusted = true;
        }
        LOG_INFO("finish adjust zone gts unit_group", KR(ret), K(ref_zone), K(adjust_zone), K(unit_ug_op));
      }
    }
    // adjust normal units
    const int64_t valid_unit_num = ref_zone.get_valid_unit_num();
    if (OB_FAIL(ret)) {
    } else if (!is_gts_unit_adjusted
        && (valid_unit_num == 1 || valid_unit_num > MAX_SUPPORTED_UNIT_NUM)) {
      LOG_INFO("gts unit not adjusted, and valid_unit_num is 1 or too many, skip ug reorganization",
               KR(ret), K(valid_unit_num));
    } else {
      // lg_count_matrix[i][j] is the count of LS Groups placing on both ref_zone_units[i] and adjust_zone_units[j]
      ObMatrix<int64_t> lg_count_matrix;
      // match_indices[i] is the index of ref_zone_unit matched by adjust_zone.units[i]
      ObArray<int64_t> match_indices;
      bool all_ls_in_unit_group = true;
      if (OB_FAIL(stat_ls_count_matrix_(ref_zone, adjust_zone, lg_count_matrix, all_ls_in_unit_group))) {
        LOG_WARN("fail to get ls count matrix", KR(ret), K(ref_zone), K(adjust_zone));
      } else if (!is_gts_unit_adjusted && all_ls_in_unit_group) {
        LOG_INFO("zone normal unit_group is already the most ideal distribution, no need to adjust",
                 KR(ret), K(ref_zone), K(adjust_zone), K(is_gts_unit_adjusted), K(all_ls_in_unit_group));
      } else if (OB_FAIL(compute_max_weight_match_(lg_count_matrix, match_indices))) {
        LOG_WARN("fail to compute max weight match", KR(ret), K(lg_count_matrix));
      } else if (OB_FAIL(compute_update_units_(ref_zone, adjust_zone, match_indices))) {
        LOG_WARN("fail to compute update units", KR(ret), K(ref_zone), K(adjust_zone));
      } else {
        LOG_INFO("finish adjust zone normal unit_groups", KR(ret), K(ref_zone), K(adjust_zone), K(lg_count_matrix),
                K(all_ls_in_unit_group), K(match_indices), KPC(unit_ug_op_array_));
      }
    }
  }
  return ret;
}

int ObUnitGroupBalance::stat_ls_count_matrix_(
    const ObZoneLSStat &ref_zone,
    const ObZoneLSStat &adjust_zone,
    ObMatrix<int64_t> &lg_count_matrix,
    bool &all_ls_in_unit_group)
{
  int ret = OB_SUCCESS;
  const int64_t unit_num = ref_zone.get_valid_unit_num();
  if (ref_zone.zone_ == adjust_zone.zone_
      || ref_zone.get_valid_unit_num() != adjust_zone.get_valid_unit_num()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ref zone and adjust zone should be different homo zones", KR(ret), K(ref_zone), K(adjust_zone));
  } else if (OB_FAIL(lg_count_matrix.init(unit_num, unit_num))) {
    LOG_WARN("fail to reserve ls count matrix", KR(ret), K(unit_num));
  } else {
    all_ls_in_unit_group = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_num; ++i) {
      const ObUnitLSStat *ref_unit = ref_zone.valid_unit_array_.at(i);
      CK(OB_NOT_NULL(ref_unit));
      for (int64_t j = 0; OB_SUCC(ret) && j < unit_num; ++j) {
        const ObUnitLSStat *adjust_unit = adjust_zone.valid_unit_array_.at(j);
        CK(OB_NOT_NULL(adjust_unit));
        // count how many LS Groups distribute on both ref_unit and adjust_unit
        int64_t lg_cnt = 0;
        FOREACH_CNT_X(ref_unit_lsg, ref_unit->ls_group_info_, OB_SUCC(ret)) {
          CK(OB_NOT_NULL(*ref_unit_lsg));
          if (OB_SUCC(ret) && has_exist_in_array(adjust_unit->ls_group_info_, *ref_unit_lsg)) {
            lg_cnt++;
          }
        }
        if (FAILEDx(lg_count_matrix.set(i, j, lg_cnt))) {
          LOG_WARN("failed to set lg_count_matrix element", KR(ret), K(i), K(j), K(lg_cnt));
        }
        // update all_ls_in_unit_group flag
        if (OB_SUCC(ret) && all_ls_in_unit_group) {
          CK(OB_NOT_NULL(ref_unit->unit_), OB_NOT_NULL(adjust_unit->unit_));
          if (OB_SUCC(ret)
              && ref_unit->unit_->unit_group_id_ != adjust_unit->unit_->unit_group_id_
              && lg_cnt > 0) {
            LOG_INFO("there are LS Groups distribute across unit groups, not all ls in unit_group",
                     KR(ret), KPC(ref_unit), KPC(adjust_unit), K(lg_cnt));
            all_ls_in_unit_group = false;
          }
        }
      }//end for adjust-unit
    }//end for ref-unit
  }
  return ret;
}

int ObUnitGroupBalance::compute_max_weight_match_(
    const ObMatrix<int64_t> &lg_count_matrix, ObArray<int64_t> &match_indices)
{
  int ret = OB_SUCCESS;
  if (lg_count_matrix.get_row_count() != lg_count_matrix.get_column_count()
      || 0 == lg_count_matrix.get_row_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lg_count_matrix size not expected", KR(ret), K(lg_count_matrix));
  } else {
    MaxWeightMatchHelper match_helper;
    if (OB_FAIL(match_helper.init(&lg_count_matrix))) {
      LOG_WARN("failed to init match_helper", KR(ret), K(lg_count_matrix));
    } else if (OB_FAIL(match_helper.do_match_by_KM_algorithm())) {
      LOG_WARN("failed to do Kuhn_Munkras", KR(ret));
    } else if (OB_FAIL(match_indices.assign(match_helper.get_match_result()))) {
      LOG_WARN("failed to get match result", KR(ret));
    }
  }
  return ret;
}

int ObUnitGroupBalance::compute_update_units_(
    const ObZoneLSStat &ref_zone,
    const ObZoneLSStat &adjust_zone,
    const ObArray<int64_t> &match_indices)
{
  int ret = OB_SUCCESS;
  const int64_t unit_num = ref_zone.get_valid_unit_num();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (unit_num != adjust_zone.get_valid_unit_num() || unit_num != match_indices.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_num), K(adjust_zone), K(ref_zone), K(match_indices));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_num; ++i) {
      int64_t match_index = match_indices.at(i);
      CK(match_index >= 0 && match_index < unit_num);
      const ObUnitLSStat *adjust_unit = adjust_zone.valid_unit_array_.at(i);
      const ObUnitLSStat *matched_ref_unit = ref_zone.valid_unit_array_.at(match_index);
      CK(OB_NOT_NULL(adjust_unit), OB_NOT_NULL(adjust_unit->unit_));
      CK(OB_NOT_NULL(matched_ref_unit), OB_NOT_NULL(matched_ref_unit->unit_));
      if (OB_FAIL(ret)) {
      } else if (adjust_unit->unit_->unit_group_id_ != matched_ref_unit->unit_->unit_group_id_) {
        // adjust_zone ug_id should update to that of matched ref_zone unit
        ObUnitUGOp unit_ug_op(adjust_unit->unit_->unit_id_,
                              adjust_unit->unit_->unit_group_id_,
                              matched_ref_unit->unit_->unit_group_id_);
        if (OB_FAIL(unit_ug_op_array_->push_back(unit_ug_op))) {
          LOG_WARN("push back unit ug op failed", KR(ret), K(unit_ug_op));
        }
      }
    }
  }
  return ret;
}
///////end ObUnitGroupBalance

/////ObLSGroupLocationBalance
int ObLSGroupLocationBalance::balance(const bool only_job_strategy)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (!tenant_info_->job_desc_.get_enable_rebalance()) {
    LOG_INFO("enable rebalance is off, can not do ls group location balance");
  } else {
    ObArray<ObLSGroupStat> &ls_group_array = tenant_info_->ls_group_array_;
    ObHeteroUGArray hetero_ug;
    if (OB_FAIL(tenant_info_->split_hetero_zone_to_homo_unit_group(hetero_ug))) {
      LOG_WARN("failed to get homo unit group", KR(ret));
    }
    FOREACH_CNT_X(ls_group, ls_group_array, OB_SUCC(ret)) {
      CK(OB_NOT_NULL(ls_group))
      ObArray<ObUnitLSStat*> unit_stat_array;
      if (FAILEDx(convert_unit_list_to_unit_stat_(*ls_group, unit_stat_array))) {
        LOG_WARN("failed to convert unit to unit group", KR(ret), KPC(ls_group));
      } else if (unit_stat_array.empty()) {
        //TODO,这个ls_group没有unit在locality中，不强制处理，预期不走到
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("the ls group has no valid unit on locality", KR(ret), KPC(ls_group));
      } else {
        //存在locality中的unit_list,可以处理
        ObArray<uint64_t> target_ug_ids;
        ARRAY_FOREACH(hetero_ug, idx) {
          ObUGArray &ug = hetero_ug.at(idx);
          uint64_t ug_id = 0;
          if (OB_FAIL(choose_unit_group_from_ug_array_(unit_stat_array, ug, ug_id))) {
            LOG_WARN("failed to choose unit group from ug", KR(ret), K(unit_stat_array));
          } else if (UINT64_MAX == ug_id) {
            LOG_INFO("not valid unit group, no need push back", KPC(ls_group), K(ug));
          } else if (OB_FAIL(target_ug_ids.push_back(ug_id))) {
            LOG_WARN("failed to push back", KR(ret), K(ug_id));
          }
        }

        if (FAILEDx(reorganize_new_unit_list_(target_ug_ids, unit_stat_array, *ls_group))) {
          LOG_WARN("failed to reorganize unit list", KR(ret), K(target_ug_ids), KPC(ls_group));
        } else if (ls_group->need_change_unit_list()) {
          LOG_INFO("ls group need change unit list", KPC(ls_group), K(target_ug_ids));
          if (OB_FAIL(try_fix_ug_ls_group_(*ls_group, target_ug_ids, hetero_ug))) {
            LOG_WARN("failed to fix ug ls group", KR(ret), KPC(ls_group), K(target_ug_ids));
          }
        } else {
          LOG_INFO("ls group no need change", KPC(ls_group), K(target_ug_ids));
        }
      }
    }
    if (OB_SUCC(ret)) {
      share::ObBalanceStrategy balance_strategy(share::ObBalanceStrategy::LB_LS_GROUP_LOCATION);
      if (OB_FAIL(try_construct_job_unit_list_op(balance_strategy, only_job_strategy))) {
        LOG_WARN("failed to construct job unit list", KR(ret), K(balance_strategy));
      }
    }
  }
  return ret;
}

int ObLSGroupLocationBalance::convert_unit_list_to_unit_stat_(
    ObLSGroupStat &ls_group, ObArray<ObUnitLSStat*> &unit_stat_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!ls_group.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls group is invalid", KR(ret), K(ls_group));
  } else {
    ObUnitIDList &unit_list = ls_group.current_unit_list_;
    ARRAY_FOREACH(unit_list, idx) {
      const uint64_t unit_id = unit_list.at(idx).id();
      ObUnitLSStat *unit_status = NULL;
      if (OB_FAIL(tenant_info_->get_unit_ls_info(unit_id, unit_status))) {
        LOG_WARN("failed to get unit ls info", KR(ret), K(unit_id));
      }
      CK(OB_NOT_NULL(unit_status), unit_status->is_valid(), OB_NOT_NULL(unit_status->unit_),
          OB_NOT_NULL(unit_status->zone_stat_))
      if (OB_FAIL(ret)) {
      } else if (!unit_status->zone_stat_->is_in_locality_) {
        LOG_INFO("zone is not in locality, no need check", K(ls_group), KPC(unit_status));
      } else if (OB_FAIL(unit_stat_array.push_back(unit_status))) {
        LOG_WARN("failed to push back", KR(ret));
      }
    }
  }
  return ret;
}

//从一组同构的unit_group中，挑选出一个unit_group
//例如2:2这一组unit_group中，unit_group1 : u1, u3 ;unit_group2 : u2, u4.
//日志流组的一组unit分别分布在u1和u4,不在同一个组unit_group内
//需要从两个unit_group中，这里会挑选出合适的unit_group；
//要么使用unit_group1，要么使用unit_group2
int ObLSGroupLocationBalance::choose_unit_group_from_ug_array_(
    const ObArray<ObUnitLSStat*> &unit_stat_array,
    ObUGArray &ug_array, uint64_t &unit_group_id)
{
  int ret = OB_SUCCESS;
  unit_group_id = 0;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(unit_stat_array.empty() || ug_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_stat_array), K(ug_array));
  } else {
    //检查unit_group_id是否在这个ug_array中存在多个，如果存在多个，则需要根据状态移除一个
    common::hash::ObHashMap<uint64_t, int64_t> ug_map;
    ObArray<const ObUnitLSStat*> dup_unit_group;
    if (OB_FAIL(get_unit_info_in_ug_array_(unit_stat_array, ug_array, ug_map, dup_unit_group))) {
      LOG_WARN("failed to get unit info in ug", KR(ret), K(unit_stat_array), K(ug_array));
    } else if (dup_unit_group.empty()) {
      unit_group_id = UINT64_MAX;
      LOG_INFO("ls group has no unit on unit_group", K(unit_stat_array), K(ug_array));
    } else if (1 == ug_map.size()) {
      unit_group_id = ug_map.begin()->first;
      LOG_INFO("only one unit group can be choose", K(unit_group_id), K(unit_stat_array));
    } else if (0 == ug_map.size()) {
      //当前日志流组在这个同构zone上有资源，但是都不在可用的unit上面，需要重新选择
      if (OB_FAIL(get_min_ug_in_ug_array_(ug_array, unit_group_id))) {
        LOG_WARN("failed to get min ug", KR(ret), K(ug_array));
      }
      LOG_INFO("the unit not in right unit group, choose new unit group", K(unit_group_id));
    } else {
      //这里进入的情况ug_map大于1，都要对zone_stat进行排序，找出第一优先级
      ChooseUGCmp ug_cmp;
      lib::ob_sort(dup_unit_group.begin(), dup_unit_group.end(), ug_cmp);
      bool equal_value = ug_cmp(dup_unit_group.at(dup_unit_group.count() - 1), dup_unit_group.at(0));
      if (OB_FAIL(ug_cmp.get_ret())) {
        LOG_WARN("failed to get ret", KR(ret));
      } else if (equal_value) {
        //如果当前所有的unit优先级都是一样的, 找出最多的
        //这个分支里面，所有unit肯定是都可以放普通日志流的
        int64_t max_num = 0;
        FOREACH_X(it, ug_map, OB_SUCC(ret)) {
          if (OB_SUCC(ret) && it->second > max_num) {
            max_num = it->second;
            unit_group_id = it->first;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (1 == max_num) {
          //找到这部分unit_group中ls_group最小的
          if (OB_FAIL(get_min_ug_in_unit_list_(dup_unit_group, unit_group_id))) {
            LOG_WARN("failed to get min ug", KR(ret), K(dup_unit_group));
          }
          LOG_INFO("all unit has same priority, choose min ug", K(unit_group_id), K(dup_unit_group));
        } else {
          LOG_INFO("all unit has same priority, choose more unit group", K(unit_group_id),
              K(max_num));
        }
      } else if (OB_ISNULL(dup_unit_group.at(0))
          || !dup_unit_group.at(0)->is_valid_for_normal_ls()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit is null", KR(ret), K(unit_stat_array));
      } else {
        CK(OB_NOT_NULL(dup_unit_group.at(0)->unit_))
        if (OB_SUCC(ret)) {
          unit_group_id = dup_unit_group.at(0)->unit_->unit_group_id_;
          LOG_INFO("choose unit group", K(unit_group_id), K(dup_unit_group));
        }
      }
    }
  }
  return ret;
}

int ObLSGroupLocationBalance::get_min_ug_in_ug_array_(
    ObUGArray &ug_array, uint64_t &unit_group_id)
{
  int ret = OB_SUCCESS;
  unit_group_id = 0;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(ug_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ug_array));
  } else {
    ObUnitGroupStat *ug_info = NULL;
    if (OB_FAIL(tenant_info_->get_min_unit_group(ug_array, ug_info))) {
      LOG_WARN("failed to get min unit", KR(ret), K(ug_array));
    } else if (OB_ISNULL(ug_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ug info is null", KR(ret), K(ug_array));
    } else {
      unit_group_id = ug_info->ug_id_;
    }
  }
  return ret;
}

//能进到这个函数之前，已经处理过了，没有一个unit时可以放普通日志流的情况，
//并且保证所有unit的优先级是一样的，那只有一个可能性，所有的unit都是可用的
int ObLSGroupLocationBalance::get_min_ug_in_unit_list_(
    const ObArray<const ObUnitLSStat*> &unit_stat_array, uint64_t &unit_group_id)
{
  int ret = OB_SUCCESS;
  unit_group_id = 0;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(unit_stat_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_stat_array));
  } else {
    int64_t max_ug = INT64_MAX;
    ARRAY_FOREACH(unit_stat_array, idx) {
      const ObUnitLSStat* unit = unit_stat_array.at(idx);
      CK(OB_NOT_NULL(unit), unit->is_valid(), unit->is_valid_for_normal_ls(),
          OB_NOT_NULL(unit->unit_group_stat_));
      if (OB_SUCC(ret) && unit->unit_group_stat_->get_ls_group_count() < max_ug) {
        unit_group_id = unit->unit_group_stat_->ug_id_;
        max_ug = unit->unit_group_stat_->get_ls_group_count();
      }
    }
  }
  return ret;

}
int ObLSGroupLocationBalance::get_unit_info_in_ug_array_(
    const ObArray<ObUnitLSStat*> &unit_stat_array,
    const ObUGArray &ug_array,
    common::hash::ObHashMap<uint64_t, int64_t> &ug_map,
    ObArray<const ObUnitLSStat*> &unit_in_ug)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> zone_array;
  ug_map.reuse();
  unit_in_ug.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(unit_stat_array.empty() || ug_array.empty()
        || OB_ISNULL(ug_array.at(0)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(unit_stat_array), K(ug_array));
  } else if (OB_FAIL(ug_array.at(0)->get_zone_array(zone_array))) {
    LOG_WARN("failed to get zone array", KR(ret), "unit group", ug_array.at(0));
  } else if (OB_FAIL(ug_map.create(10, "LSBalance"))) {
    LOG_WARN("failed to create hash map", KR(ret));
  } else {
    int64_t unit_group_count = 0;
    ARRAY_FOREACH(unit_stat_array, idx) {
      unit_group_count = 0;
      const ObUnitLSStat* unit = unit_stat_array.at(idx);
      CK(OB_NOT_NULL(unit), OB_NOT_NULL(unit->unit_))
      if (OB_SUCC(ret) && has_exist_in_array(zone_array, unit->unit_->zone_)) {
        if (OB_FAIL(unit_in_ug.push_back(unit))) {
          LOG_WARN("failed to push back", KR(ret), K(idx));
        }
      }
      if (OB_FAIL(ret) || !unit->is_valid_for_normal_ls()) {
        //unit 不可用
      } else if (OB_ISNULL(unit->unit_group_stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit is valid, but unit group is null", KR(ret), KPC(unit));
      } else if (has_exist_in_array(ug_array, unit->unit_group_stat_)) {
        ret = ug_map.get_refactored(unit->unit_group_stat_->ug_id_, unit_group_count);
        if (OB_HASH_NOT_EXIST == ret) {
          unit_group_count = 1;
          ret = OB_SUCCESS;
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed to get unit group count", KR(ret), KPC(unit));
        } else {
          unit_group_count++;
        }
        if (FAILEDx(ug_map.set_refactored(unit->unit_group_stat_->ug_id_, unit_group_count, 1))) {
          LOG_WARN("failed to set", KR(ret), KPC(unit));
        }
      }
    }//end for get multi dup
  }
  return ret;
}
//根据选择出来的unit_group构造出现的unit_list，
//这里需要注意的是，如果这个ls_group在这个zone上没有资源，不会给他加进去
//防止后续的均衡导致这个操作是浪费的。
//TODO如果这个zone已经不在locality了，还是会修改unit_list
int ObLSGroupLocationBalance::reorganize_new_unit_list_(
    const ObArray<uint64_t> &unit_group_array, ObArray<ObUnitLSStat*> &unit_stat_array,
    ObLSGroupStat &ls_group)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(unit_stat_array.empty() || unit_group_array.empty()
                         || !ls_group.is_valid() || unit_group_array.count() > 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_group), K(unit_stat_array), K(unit_group_array));
  } else {
    ls_group.target_unit_list_.reset();
    ARRAY_FOREACH(unit_stat_array, idx) {
      ObUnitLSStat* unit = unit_stat_array.at(idx);
      CK(OB_NOT_NULL(unit), OB_NOT_NULL(unit->unit_), OB_NOT_NULL(unit->zone_stat_))
      if (OB_SUCC(ret)) {
        if (unit->is_valid_for_normal_ls()
            && has_exist_in_array(unit_group_array, unit->unit_->unit_group_id_)) {
          //从3:3 变成3:2时，unit_group_id是可用的，但是unit不可用，需要使用新选择出来的unit group
          //当前unit是可用的，并且是指定的unit_group
          if (OB_FAIL(ls_group.target_unit_list_.push_back(ObDisplayUnitID(unit->unit_id_)))) {
            LOG_WARN("failed to push back", KR(ret), K(idx), KPC(unit));
          }
        } else {
          //拿到这个zone下指定unit_group的unit
          ObUnitLSStat *target_unit = NULL;
          ARRAY_FOREACH(unit_group_array, jdx) {
            const uint64_t ug_id = unit_group_array.at(jdx);
            if (OB_FAIL(unit->zone_stat_->get_unit_info_by_ug_id(ug_id, target_unit))) {
              if (OB_ENTRY_NOT_EXIST == ret) {
                //由于传进来的unit_group_array是包含两个同构zone的结果，所以某一个找不到是预期的
                LOG_INFO("ug id not in the zone", K(ug_id), KPC(unit->zone_stat_));
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to get unit by ug id", KR(ret), K(ug_id));
              }
            } else {
              break;
            }
          }//end for
          CK(OB_NOT_NULL(target_unit), OB_NOT_NULL(target_unit->unit_));
          if (FAILEDx(ls_group.target_unit_list_.push_back(ObDisplayUnitID(target_unit->unit_id_)))) {
            LOG_WARN("failed to push back", KR(ret), KPC(target_unit));
          }
        }
      }
    }
  }
  return ret;
}

int ObLSGroupLocationBalance::try_fix_ug_ls_group_(ObLSGroupStat &ls_group,
    const ObArray<uint64_t> &unit_group_id, ObHeteroUGArray &hetero_ug)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!ls_group.is_valid() || unit_group_id.empty()
        || hetero_ug.empty() || unit_group_id.count() > hetero_ug.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls group is invalid", KR(ret), K(ls_group), K(unit_group_id), K(hetero_ug));
  } else {
    ARRAY_FOREACH(unit_group_id, idx) {
      const uint64_t ug_id = unit_group_id.at(idx);
      ObUnitGroupStat *ug = NULL;
      ARRAY_FOREACH_X(hetero_ug, jdx, cnt, OB_ISNULL(ug)) {
        ObUGArray &ug_array = hetero_ug.at(jdx);
        //这个ug_id不在这个ug_array里面是预期的，在迭代完所以ug_array后再去判断是否找到
        ARRAY_FOREACH_X(ug_array, kdx, ug_cnt, OB_ISNULL(ug)) {
          ObUnitGroupStat *tmp_ug = ug_array.at(kdx);
          CK(OB_NOT_NULL(tmp_ug))
          if (OB_SUCC(ret) && tmp_ug->ug_id_ == ug_id) {
            ug = tmp_ug;
          }
        }
      }//end for each ug_array
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(ug)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit group can not found", KR(ret), K(ug_id), K(hetero_ug));
        } else if (OB_FAIL(ug->try_add_ls_group(&ls_group))) {
          if (OB_ENTRY_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to add ls group", KR(ret), K(ls_group));
          }
        } else {
          LOG_INFO("add ls to unit group", K(ug_id), K(ls_group));
        }
      }
    }
  }
  return ret;
}

////////////end ObLSGroupLocationBalance

/////////////begin ObLSGroupCountBalance
int ObLSGroupMatrixCell::assign(
    const ObLSGroupMatrixCell &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    if (OB_FAIL(inner_ls_group_stat_array_.assign(other.inner_ls_group_stat_array_))) {
      LOG_WARN("fail to assign inner_ls_group_stat_array_", KR(ret), K(other));
    } else {
      target_lg_cnt_ = other.target_lg_cnt_;
    }
  }
  return ret;
}

int ObLSGroupMatrixCell::init_ls_groups(ObIArray<ObLSGroupStat*> &ls_groups_to_add)
{
  int ret = OB_SUCCESS;
  target_lg_cnt_ = 0;
  if (OB_FAIL(add_ls_groups(ls_groups_to_add))) {
    LOG_WARN("failed to add ls groups", KR(ret), K(ls_groups_to_add));
  }
  return ret;
}

int ObLSGroupMatrixCell::add_ls_groups(ObIArray<ObLSGroupStat*> &ls_groups_to_add)
{
  int ret = OB_SUCCESS;
  for (int64_t index = 0; index < ls_groups_to_add.count() && OB_SUCC(ret); ++index) {
    ObLSGroupStat* ls_group_stat = ls_groups_to_add.at(index);
    if (OB_ISNULL(ls_group_stat)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), KP(ls_group_stat));
    } else if (has_exist_in_array(inner_ls_group_stat_array_, ls_group_stat)) {
      ret = OB_ENTRY_EXIST;
      LOG_WARN("ls group already exist", KR(ret), KPC(ls_group_stat), KPC(this));
    } else if (OB_FAIL(inner_ls_group_stat_array_.push_back(ls_group_stat))) {
      LOG_WARN("fail to push back ls group stat into array", KR(ret), KPC(ls_group_stat));
    }
  }
  if (OB_SUCC(ret)) {
    target_lg_cnt_ += ls_groups_to_add.count();
  }
  return ret;
}

int64_t ObLSGroupMatrixCell::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  bool need_append_comma = false;
  J_OBJ_START();
  J_NAME("ls_group_id_array");
  J_COLON();
  J_ARRAY_START();
  ARRAY_FOREACH_NORET(inner_ls_group_stat_array_, idx) {
    ObLSGroupStat* ls_group_stat = inner_ls_group_stat_array_.at(idx);
    if (need_append_comma) {
      J_COMMA();
    } else {
      need_append_comma = true;
    }
    if (OB_NOT_NULL(ls_group_stat)) {
      BUF_PRINTO(ls_group_stat->lg_id_);
    } else {
      J_NAME("nullptr");
    }
  }
  J_ARRAY_END();
  J_COMMA();
  J_KV(K_(target_lg_cnt));
  J_OBJ_END();
  return pos;
}

int ObLSGroupMatrixCell::get_ls_count(int64_t &ls_count) const
{
  int ret = OB_SUCCESS;
  ls_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < inner_ls_group_stat_array_.count(); ++i) {
    CK(OB_NOT_NULL(inner_ls_group_stat_array_.at(i)));
    if (OB_SUCC(ret)) {
      ls_count += inner_ls_group_stat_array_.at(i)->ls_count_in_group();
    }
  }
  return ret;
}

int ObLSGroupCountBalance::construct_ls_group_matrix_to_do_balance_(
    LSGroupMatrix &ls_group_matrix)
{
  int ret = OB_SUCCESS;
  ObHeteroUGArray hetero_ug_array;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(tenant_info_->split_hetero_zone_to_homo_unit_group(hetero_ug_array))) {
    LOG_WARN("fail to split hetero zone to homo unit group", KR(ret), KPC(tenant_info_));
  } else if (OB_UNLIKELY(1 != hetero_ug_array.count())
             && OB_UNLIKELY(2 != hetero_ug_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(hetero_ug_array));
  } else if (1 == hetero_ug_array.count()) {
    // is homo deployed
    // matrix should have only 1 row
    if (OB_FAIL(construct_ls_group_matrix_with_homo_deployed_(
                    hetero_ug_array, ls_group_matrix))) {
      LOG_WARN("fail to construct ls group matrix with homo-deployed zone",
               KR(ret), K(hetero_ug_array));
    }
  } else {
    // is hetero-deployed
    // matrix should in format with row_ug_array * column_ug_array
    // row_ug_array is more balanced
    if (OB_FAIL(construct_ls_group_matrix_with_hetero_deployed_(
                    hetero_ug_array, ls_group_matrix))) {
      LOG_WARN("fail to construct ls group matrix with hetero-deployed zone",
               KR(ret), K(hetero_ug_array));
    }
  }
  FLOG_INFO("ls group matrix is builded", KR(ret), K(ls_group_matrix), K(hetero_ug_array));
  return ret;
}

int ObLSGroupCountBalance::construct_ls_group_matrix_with_homo_deployed_(
    const ObHeteroUGArray &hetero_ug_array,
    LSGroupMatrix &ls_group_matrix)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(1 != hetero_ug_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(hetero_ug_array));
  } else if (OB_UNLIKELY(0 >= hetero_ug_array.at(0).count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(hetero_ug_array));
  } else if (OB_FAIL(ls_group_matrix.init(1/*row_count*/, hetero_ug_array.at(0).count()/*column_count*/))) {
    LOG_WARN("fail to init ls group matrix with homo deployed", KR(ret), K(hetero_ug_array));
  } else {
    ObLSGroupMatrixCell ls_group_matrix_cell;
    for (int64_t index = 0; index < hetero_ug_array.at(0).count() && OB_SUCC(ret); ++index) {
      ObUnitGroupStat *ug_stat = hetero_ug_array.at(0).at(index);
      ls_group_matrix_cell.reset();
      if (OB_ISNULL(ug_stat)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), KP(ug_stat));
      } else if (OB_FAIL(ls_group_matrix_cell.init_ls_groups(ug_stat->ls_group_info_))) {
        LOG_WARN("fail to add ls groups into cell", KR(ret), KPC(ug_stat));
      } else if (OB_FAIL(ls_group_matrix.set(0/*row*/, index/*column*/, ls_group_matrix_cell))) {
        // set() is to update cell, all cells is inited when init matrix
        LOG_WARN("fail to set cell into matrix", KR(ret), K(index), K(ls_group_matrix_cell));
      }
    }
    if (FAILEDx(build_matrix_for_orphans_ls_group_(ls_group_matrix))) {
      LOG_WARN("failed to build matrix for orphans ls group", KR(ret));
    }
  }
  return ret;
}

int ObLSGroupCountBalance::construct_ls_group_matrix_with_hetero_deployed_(
    const ObHeteroUGArray &hetero_ug_array,
    LSGroupMatrix &ls_group_matrix)
{
  int ret = OB_SUCCESS;
  ObUGArray row_ug_array; // more balanced
  ObUGArray column_ug_array;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(2 != hetero_ug_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(hetero_ug_array));
  } else if (OB_FAIL(choose_row_and_columns_for_matrix_(
                       hetero_ug_array,
                       row_ug_array,
                       column_ug_array))) {
    LOG_WARN("fail to decide row and column for matrix", KR(ret), K(hetero_ug_array));
  } else if (OB_FAIL(ls_group_matrix.init(row_ug_array.count()/*row_count*/, column_ug_array.count()/*column_count*/))) {
    LOG_WARN("fail to init ls group matrix with homo deployed", KR(ret), K(row_ug_array), K(column_ug_array));
  } else {
    ObLSGroupMatrixCell ls_group_matrix_cell;
    for (int64_t row_index = 0; row_index < row_ug_array.count() && OB_SUCC(ret); ++row_index) {
      ObUnitGroupStat *row_ug_stat = row_ug_array.at(row_index);
      CK(OB_NOT_NULL(row_ug_stat));
      for (int64_t column_index = 0; column_index < column_ug_array.count() && OB_SUCC(ret); ++column_index) {
        ObUnitGroupStat *column_ug_stat = column_ug_array.at(column_index);
        if (OB_ISNULL(row_ug_stat) || OB_ISNULL(column_ug_stat)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", KR(ret), KP(row_ug_stat), KP(column_ug_stat));
        } else if (OB_FAIL(build_matrix_cell_(*row_ug_stat, *column_ug_stat, ls_group_matrix_cell))) {
          LOG_WARN("fail to build matrix cell", KR(ret), KPC(row_ug_stat), KPC(column_ug_stat));
        } else if (OB_FAIL(ls_group_matrix.set(row_index/*row*/, column_index/*column*/, ls_group_matrix_cell))) {
          LOG_WARN("fail to set cell into matrix", KR(ret), K(row_index), K(column_index), K(ls_group_matrix_cell));
        }
      }//end for column
    }//end for matrix build
    if (FAILEDx(build_matrix_for_single_parent_ls_group_(true, row_ug_array, ls_group_matrix))) {
      LOG_WARN("failed to build matrix for row ug", KR(ret), K(row_ug_array));
    } else if (OB_FAIL(build_matrix_for_single_parent_ls_group_(false, column_ug_array, ls_group_matrix))) {
      LOG_WARN("failed to build matrix for column ug", KR(ret), K(column_ug_array));
    } else if (OB_FAIL(build_matrix_for_orphans_ls_group_(ls_group_matrix))) {
      LOG_WARN("failed to build matrix for orphans ls group", KR(ret));
    }
  }
  return ret;
}

//存在一些ls group，在行和列上是不全的，例如只存在在行上，不存在列上；或者只存在在列上，不存在在行上。对于这些ls group，我们要特殊处理, 我们称这种ls_group叫single_parent ls group。
// 目前的算法是把这些single_parent ls group找一个cell填充进去，比如对于只存在在某一行上的ls group，选择这一行上拥有最少ls group的cell放进去。
// TODO：目前选择single_parent ls group的cell算法仅仅考虑了ls group的数量，未来还应该考虑磁盘等
// 例如22的租户，增加一组locality44变成2244的部署，44上面是没有任何ls_group的
//但是不能让列空着，需要目前可以把ls_group放入每一行的第一个cell，其他的不管
int ObLSGroupCountBalance::build_matrix_for_single_parent_ls_group_(
      bool is_row_array,
      ObUGArray &ug_array,
      LSGroupMatrix &lg_matrix)
{
  int ret = OB_SUCCESS;
  int64_t col_cnt = lg_matrix.get_column_count();
  int64_t row_cnt = lg_matrix.get_row_count();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(ug_array.empty()
        || (is_row_array && ug_array.count() != row_cnt)
        || (!is_row_array && ug_array.count() != col_cnt))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ug array is not match with matrix", KR(ret), K(ug_array.count()),
      K(col_cnt), K(row_cnt), K(is_row_array));
  }
  ObLSGroupMatrixCell* min_cell = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < ug_array.count(); ++i) {
    ObUnitGroupStat *ug_stat = ug_array.at(i);
    CK(OB_NOT_NULL(ug_stat));
    min_cell = NULL;
    if (OB_SUCC(ret) && ug_stat->get_ls_group_count() > 0) {
      LOG_INFO("has single parent ls group in ug", KPC(ug_stat));
      //如果是行上的ug存在孤儿ls_group，需要在这一行上的所有列内找到最小的cell
      //如果是列上的ug，则需要在这一列上的所有行的cell内找到最小的cell
      int64_t max_cnt = is_row_array ? col_cnt : row_cnt;
      for (int64_t j = 0; OB_SUCC(ret) && j < max_cnt; ++j) {
        ObLSGroupMatrixCell *curr_cell = NULL;
        if (is_row_array) {
          curr_cell = lg_matrix.get(i, j);
        } else {
          curr_cell = lg_matrix.get(j, i);
        }
        CK(OB_NOT_NULL(curr_cell));
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(min_cell) ||
              min_cell->get_ls_group_count() > curr_cell->get_ls_group_count()) {
            min_cell = curr_cell;
          }
        }
      }//end for min_cell
      CK(OB_NOT_NULL(min_cell));
      if (FAILEDx(min_cell->add_ls_groups(ug_stat->ls_group_info_))) {
        LOG_WARN("failed to add ls groups", KR(ret), KPC(ug_stat), K(min_cell));
      } else {
        //清空所有
        LOG_INFO("put single parent ls group into cell", KPC(ug_stat), KPC(min_cell));
        ug_stat->ls_group_info_.reset();
      }
    }//end for process ls_group
  }//end for all ug
  return ret;
}
//如果有一些ls_group不在任何可用的unit_group中，也是需要把这个ls_group放在二维矩阵中的
//例如租户的locality是22，但是实际上资源部署是2244，可能存在一个ls_group他在22上是没有任何资源的
//只在44上存在，我们不会统计44这种unit_group，需要找个地方给这种孤儿ls_group。
//当前这种情况，如果这部分孤儿日志流要分裂的话，member_list没有办法对齐，这种
//日志流创建出来有问题，暂时不处理这种情况，如果存在这种情况先报错
int ObLSGroupCountBalance::build_matrix_for_orphans_ls_group_(
      LSGroupMatrix &lg_matrix)
{
  int ret = OB_SUCCESS;
  ObArray<ObLSGroupStat*> ls_group_array;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(tenant_info_->get_all_ls_group(ls_group_array))) {
    LOG_WARN("failed to assign ls group array", KR(ret));
  } else {
    //判断是否存在孤儿ls_group
    int64_t ls_group_index = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < lg_matrix.get_element_count(); ++i) {
      ObLSGroupMatrixCell* cell = lg_matrix.get(i);
      CK(OB_NOT_NULL(cell));
      for (int64_t j = 0; OB_SUCC(ret) && j < cell->get_ls_group_count(); ++j) {
        ObLSGroupStat* ls_group = cell->get_ls_groups().at(j);
        CK(OB_NOT_NULL(ls_group));
        if (OB_FAIL(ret)) {
        } else if (!has_exist_in_array(ls_group_array, ls_group, &ls_group_index)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls group not expected", KR(ret), KPC(ls_group), K(ls_group_array));
        } else if (OB_UNLIKELY(0 > ls_group_index || ls_group_index >= ls_group_array.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls group index is not expected", KR(ret), K(ls_group_index),
              KPC(ls_group), K(ls_group_array));
        } else if (OB_FAIL(ls_group_array.remove(ls_group_index))) {
          LOG_WARN("failed to remove ls group index", KR(ret), K(ls_group_index));
        }
      }
    }//end for check orphans ls group
    if (OB_SUCC(ret) && ls_group_array.count() > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("has ls group not in locality", KR(ret), K(ls_group_array));
    }
  }
  return ret;
}

int ObLSGroupCountBalance::choose_row_and_columns_for_matrix_(
    const ObHeteroUGArray &hetero_ug_array,
    ObUGArray &row_ug_array,
    ObUGArray &column_ug_array)
{
  int ret = OB_SUCCESS;
  column_ug_array.reset();
  row_ug_array.reset();
  ChooseZoneCmp zone_cmp;
  int64_t more_balanced_ug_array_index = 0;
  ObZoneLSStat* first_most_available_zone = NULL;
  ObZoneLSStat* second_most_available_zone = NULL;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(2 != hetero_ug_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(hetero_ug_array));
  } else if (OB_FAIL(find_most_available_zone_(
                         hetero_ug_array.at(0),
                         first_most_available_zone))) {
    LOG_WARN("fail to find most available zone", KR(ret), K(hetero_ug_array.at(0)));
  } else if (OB_FAIL(find_most_available_zone_(
                         hetero_ug_array.at(1),
                         second_most_available_zone))) {
    LOG_WARN("fail to find most available zone", KR(ret), K(hetero_ug_array.at(1)));
  } else if (OB_ISNULL(first_most_available_zone)
             || OB_ISNULL(second_most_available_zone)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(first_most_available_zone),
             KP(second_most_available_zone));
  } else {
    bool first_more_balanced = false;
    first_more_balanced = zone_cmp(first_most_available_zone, second_most_available_zone);
    if (OB_FAIL(zone_cmp.get_ret())) {
      LOG_WARN("fail to compare zone", KR(ret),
               KPC(first_most_available_zone), KPC(second_most_available_zone));
    } else if (first_more_balanced) {
      // first zone is more balanced
      more_balanced_ug_array_index = 0;
    } else {
      // second zone is more balanced
      more_balanced_ug_array_index = 1;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_ug_array.assign(hetero_ug_array.at(more_balanced_ug_array_index)))) {
    LOG_WARN("fail to assign row ug with hetero_ug_array",
             KR(ret), K(hetero_ug_array), K(more_balanced_ug_array_index));
  } else if (OB_FAIL(column_ug_array.assign(hetero_ug_array.at(1 - more_balanced_ug_array_index)))) {
    LOG_WARN("fail to assign column ug with hetero_ug_array",
             KR(ret), K(hetero_ug_array), K(more_balanced_ug_array_index));
  } else {
    FLOG_INFO("finish decide matrix row and column by checking zone priority",
              KR(ret), K(hetero_ug_array), KPC(first_most_available_zone),
              KPC(second_most_available_zone), K(more_balanced_ug_array_index));
  }
  return ret;
}

int ObLSGroupCountBalance::find_most_available_zone_(
    const ObUGArray &ug_array,
    ObZoneLSStat *&most_available_zone)
{
  int ret = OB_SUCCESS;
  most_available_zone = NULL;
  ObArray<ObZoneLSStat*> zone_stats;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(0 >= ug_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ug_array));
  } else {
    // every unit_group in array should have same zone_array, just pick first one
    ObUnitGroupStat *ug_stat = ug_array.at(0);
    if (OB_ISNULL(ug_stat)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), KP(ug_stat));
    } else if (OB_FAIL(ug_stat->get_zone_info_array(zone_stats))) {
      LOG_WARN("fail to get zone infos", KR(ret), K(ug_stat));
    } else if (OB_UNLIKELY(0 >= zone_stats.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(zone_stats));
    } else {
      ChooseZoneCmp zone_cmp;
      lib::ob_sort(zone_stats.begin(), zone_stats.end(), zone_cmp);
      if (OB_FAIL(zone_cmp.get_ret())) {
        LOG_WARN("failed to get ret", KR(ret));
      } else if (OB_ISNULL(zone_stats.at(0))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), KP(zone_stats.at(0)));
      } else {
        most_available_zone = zone_stats.at(0);
      }
    }
  }
  return ret;
}
//如果行列匹配上，就放入cell里面，然后把匹配上的lg删除掉
int ObLSGroupCountBalance::build_matrix_cell_(
    ObUnitGroupStat &row_ug_stat,
    ObUnitGroupStat &column_ug_stat,
    ObLSGroupMatrixCell &ls_group_matrix_cell)
{
  int ret = OB_SUCCESS;
  ls_group_matrix_cell.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!row_ug_stat.is_valid())
             || OB_UNLIKELY(!column_ug_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(row_ug_stat), K(column_ug_stat));
  } else {
    ObArray<ObLSGroupStat*> common_ls_group_stat_array;
    int64_t col_index = -1;
    for (int64_t row_index = row_ug_stat.ls_group_info_.count() - 1;
         row_index >= 0 && OB_SUCC(ret);
         --row_index) {
      ObLSGroupStat *ls_group_stat_in_row = row_ug_stat.ls_group_info_.at(row_index);
      if (OB_ISNULL(ls_group_stat_in_row)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), KP(ls_group_stat_in_row), K(row_ug_stat));
      } else if (has_exist_in_array(column_ug_stat.ls_group_info_, ls_group_stat_in_row, &col_index)) {
        if (OB_FAIL(common_ls_group_stat_array.push_back(ls_group_stat_in_row))) {
          LOG_WARN("fail to push ls group stat into cell", KR(ret), KPC(ls_group_stat_in_row));
        } else if (OB_FAIL(row_ug_stat.ls_group_info_.remove(row_index))) {
          LOG_WARN("failed to remove row index", KR(ret), K(row_index), K(col_index));
        } else if (OB_UNLIKELY(col_index < 0 || col_index >= column_ug_stat.ls_group_info_.count())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("col index in unexpected", KR(ret), K(col_index), K(column_ug_stat));
        } else if (OB_FAIL(column_ug_stat.ls_group_info_.remove(col_index))) {
          LOG_WARN("failed to remove col index", KR(ret), K(row_index), K(col_index));
        }
      }
    }//for
    if (FAILEDx(ls_group_matrix_cell.init_ls_groups(common_ls_group_stat_array))) {
      LOG_WARN("failed to init ls groups", KR(ret), K(common_ls_group_stat_array));
    }
  }
  return ret;
}

int ObLSGroupCountBalance::balance(const bool only_job_strategy)
{
  int ret = OB_SUCCESS;
  int64_t target_lg_cnt = 0;
  bool need_balance = true;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(check_can_balance_ls_group_(need_balance, target_lg_cnt))) {
    LOG_WARN("failed to check can balance in ls group", KR(ret), K(need_balance));
  } else if (!need_balance) {
  } else {
    LSGroupMatrix ls_group_matrix;
    share::ObBalanceStrategy balance_strate(share::ObBalanceStrategy::LB_LS_GROUP_COUNT);
    if (OB_FAIL(generate_balance_job(balance_strate, only_job_strategy))) {
      LOG_WARN("failed to generate balance job", KR(ret), K(balance_strate));
    } else if (only_job_strategy) {
    } else if (OB_FAIL(construct_ls_group_matrix_to_do_balance_(ls_group_matrix))) {
      LOG_WARN("fail to construct ls group matrix to do balance", KR(ret));
    } else {
      //构造以两组同构ug为行列的ls_group信息的二维数组
      const int64_t row_cnt = ls_group_matrix.get_row_count();
      const int64_t target_lg_count_each_ug = target_lg_cnt / row_cnt;
      const int64_t target_ls_count_each_ug = target_lg_count_each_ug *
        tenant_info_->job_desc_.get_ls_cnt_in_group();
      LOG_INFO("need balance", K(target_lg_cnt), K(row_cnt), K(target_lg_count_each_ug));
      if (tenant_info_->job_desc_.get_enable_transfer()) {
        ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
        if (OB_ISNULL(schema_service)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema service is null", KR(ret), K(schema_service));
        } else if (OB_FAIL(tenant_ls_bg_info_.init(tenant_info_->tenant_id_, target_ls_count_each_ug))) {
          LOG_WARN("init tenant LS balance group info fail", KR(ret), K(tenant_info_->tenant_id_),
              K(target_ls_count_each_ug));
        } else if (OB_FAIL(tenant_ls_bg_info_.build("LS_GROUP_COUNT_BALANCE",
                *sql_proxy_, *schema_service))) {
          LOG_WARN("build tenant all balance group info for all LS fail", KR(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
        if (OB_FAIL(balance_each_unit_group_by_row_(ls_group_matrix, i, target_lg_count_each_ug))) {
          LOG_WARN("failed to balance each unit group", KR(ret), K(i), K(target_lg_count_each_ug));
        }
      }
    }
  }
  return ret;
}

int ObLSGroupCountBalance::check_can_balance_ls_group_(
    bool &need_balance, int64_t &target_lg_cnt) const
{
  int ret = OB_SUCCESS;
  target_lg_cnt = 0;
  need_balance = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (!tenant_info_->is_primary()) {
    need_balance = false;
    if (REACH_TENANT_TIME_INTERVAL(100 * 1000 * 1000)) {
      LOG_INFO("not primary tenant, no need balance");
    }
  } else if (!tenant_info_->job_desc_.get_enable_rebalance()) {
    need_balance = false;
    LOG_INFO("tenant can not rebalance, no need do ls group count balance");
  } else if (OB_FAIL(tenant_info_->job_desc_.get_unit_lcm_count(target_lg_cnt))) {
    LOG_WARN("failed to get unit lcm count", KR(ret), "job_desc", tenant_info_->job_desc_);
  } else if (tenant_info_->ls_group_array_.count() == target_lg_cnt) {
    need_balance = false;
    LOG_INFO("ls group count is match with unit", K(target_lg_cnt));
  } else if (!tenant_info_->job_desc_.get_enable_transfer()) {
    //如果ls的总数小于target_lg_cnt，并且每个ls_group内只有一个日志流，则不能进行均衡
    if (tenant_info_->normal_ls_info_.count() >= target_lg_cnt) {
      need_balance = true;
    } else {
      //检查每个ls_group内是否有多余1条的日志流，如果有则也需要均衡，尽可能把日志流
      //平铺在各个组内. 可以证明一定能生成 alter 任务.
      ARRAY_FOREACH(tenant_info_->ls_group_array_, idx) {
        const ObLSGroupStat &lg_stat = tenant_info_->ls_group_array_.at(idx);
        if (lg_stat.ls_count_in_group() > 1) {
          need_balance = true;
          LOG_INFO("ls group has more than one ls, need balance", K(lg_stat));
          break;
        }
      }
    }
  } else {
    need_balance = true;
  }
  return ret;
}

int ObLSGroupCountBalance::balance_each_unit_group_by_row_(
    LSGroupMatrix &lg_matrix, const int64_t row_index, const int64_t target_lg_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(row_index < 0 || row_index >= lg_matrix.get_row_count()
        || target_lg_cnt < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row index is invalid", KR(ret), K(row_index), K(target_lg_cnt));
  } else {
    //计算这一行上所有列的ls_group数量的总和
    const int64_t col_cnt = lg_matrix.get_column_count();
    int64_t curr_lg_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      const ObLSGroupMatrixCell *cell = lg_matrix.get(row_index, i);
      CK(OB_NOT_NULL(cell))
      if (OB_SUCC(ret)) {
        curr_lg_cnt += cell->get_ls_group_count();
      }
    }//end for get all ls group count
    if (OB_SUCC(ret)) {
      if (target_lg_cnt == curr_lg_cnt) {
        //nothing todo
      } else if (target_lg_cnt > curr_lg_cnt) {
        //expand
        LOG_INFO("need expand count", K(row_index), K(target_lg_cnt), K(curr_lg_cnt));
        if (0 == curr_lg_cnt) {
          //如果这一行上是没有任何日志流的，只能走创建的逻辑
          if (OB_FAIL(expand_empty_row_by_create_(lg_matrix, row_index, target_lg_cnt))) {
            LOG_WARN("failed to expand ls by create", KR(ret), K(row_index));
          }
        } else if (OB_FAIL(expand_ls_group_cnt_(lg_matrix, row_index,
                target_lg_cnt, curr_lg_cnt))) {
          LOG_WARN("failed to expand", KR(ret), K(row_index), K(target_lg_cnt), K(curr_lg_cnt));
        }
      } else {
        //shrink
        LOG_INFO("need shrink count", K(row_index), K(target_lg_cnt), K(curr_lg_cnt));
        if (OB_FAIL(shrink_ls_group_cnt_(lg_matrix, row_index, target_lg_cnt, curr_lg_cnt))) {
          LOG_WARN("failed to shrink", KR(ret), K(row_index), K(target_lg_cnt), K(curr_lg_cnt));
        }
      }
    }
  }
  return ret;
}

//这一行上所有的cell都是空的，直接创建足够的日志流组
int ObLSGroupCountBalance::expand_empty_row_by_create_(LSGroupMatrix &lg_matrix,
    const int64_t row_index, const int64_t target_lg_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(row_index < 0 || row_index >= lg_matrix.get_row_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row index is invalid", KR(ret), K(row_index));
  } else {
    //直接去创建日志流，日志流创建的时候会找到最合适的位置创建
    uint64_t tenant_id = tenant_info_->tenant_id_;
    const share::ObBalanceJobID &job_id = job_->get_job_id();
    const share::ObBalanceStrategy balance_strategy = job_->get_balance_strategy();
    for (int64_t i = 0; OB_SUCC(ret) && i < target_lg_cnt; ++i) {
      uint64_t ls_group_id = 0;
      if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_group_id(sql_proxy_,
              tenant_info_->tenant_id_, ls_group_id))) {
        LOG_WARN("failed to fetch new ls group id", KR(ret), K(tenant_info_->tenant_id_));
      }
      LOG_INFO("ls group need create ls", K(ls_group_id));
      //创建1个日志流
      if (FAILEDx(ObLSBalanceTaskHelper::add_create_ls_task(tenant_id,
              job_id, ls_group_id, balance_strategy, sql_proxy_, *task_array_))) {
          LOG_WARN("failed to add create ls task", KR(ret), K(tenant_id), K(job_id),
              K(ls_group_id), K(balance_strategy));
      }
    }
  }
  return ret;
}

/*
 * 算法步骤：1. 确定在这行中，每个需要扩容出来的lg需要放在哪个单元格内；
 *           2. 找到每个单元格内需要扩容的lg，首先进行alter操作，由于单元格实际上unit_list都是相同的，所以只是做了一个ls_group_id的变更。
 *           3. 经过上述的操作，单元格内的日志流还是空的，则根据enable_transfer是否开启决定生成分裂合并操作，还是创建空日志流的操作
 * */
int ObLSGroupCountBalance::expand_ls_group_cnt_(
    LSGroupMatrix &lg_matrix, const int64_t row_index, const int64_t target_lg_cnt,
    const int64_t curr_lg_count)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = lg_matrix.get_column_count();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(row_index < 0 || row_index >= lg_matrix.get_row_count()
        || curr_lg_count <= 0
        || curr_lg_count >= target_lg_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(row_index), K(curr_lg_count), K(target_lg_cnt));
  } else if (OB_FAIL(set_cell_expand_lg_cnt_(lg_matrix, row_index, target_lg_cnt,
          curr_lg_count))) {
    LOG_WARN("failed to set cell expand lg cnt", KR(ret), K(lg_matrix), K(row_index),
        K(target_lg_cnt), K(curr_lg_count));
  }
  if (OB_SUCC(ret)) {
    const int64_t col_cnt = lg_matrix.get_column_count();
    for (int64_t j = 0; OB_SUCC(ret) && j < col_cnt; ++j) {
      ObLSGroupMatrixCell *cell = lg_matrix.get(row_index, j);
      CK(OB_NOT_NULL(cell))
      if (OB_SUCC(ret) && cell->get_expand_shrink_cnt() > 0) {
        //对于每个需要扩容出来的cell分别做下面的操作
        ObArray<ObLSGroupStat*> target_lg_array;
        ObArray<ObLSGroupStat> expand_lg_array;
        //构造出这个cell上所有的lg_array，target_lg_array 包含了现有的和即将扩容出来的lg的指针
        if (OB_FAIL(get_cell_expand_lg_(*cell, target_lg_array, expand_lg_array))) {
          LOG_WARN("failed to get cell expand lg", KR(ret), KPC(cell));
        }
        //在curr_lg_array范围内，把日志流从这个lg调整到另一个lg，保证最大最小不大于1
        else if (OB_FAIL(balance_ls_between_lgs_in_cell_(target_lg_array))) {
          LOG_WARN("failed to construct migrate task", KR(ret), K(target_lg_array));
        } else if (tenant_info_->job_desc_.get_enable_transfer()) {
          if (OB_FAIL(construct_expand_task_for_cell_emtpy_lg_(target_lg_array))) {
            LOG_WARN("failed to construct expand task", KR(ret), K(row_index), K(target_lg_array));
          }
        } else {
          //关掉transfer了，经过均衡了，没有什么可以做的了
        }
        LOG_INFO("after expand", K(target_lg_array));
      }
    }
  }
  return ret;
}
/*
 * 设置每个单元格应该扩容出来的个数，对于每个要扩容出来的lg选择出那个扩容后每个日志流
 * 组均值最大的单元格
 * 举例说明一个4*5的异构zone，4（1，2，3，4）作为行，5(a,b,c,d,e)作为列,二维矩阵的第一行的每个单元格内日志流组的数量如下：
 *   A B C D E
 *   1 2 0 0 0
 * 由于每一行应该有5个日志流组，我们现在的算法假设比较粗糙，假设每个日志流组内的日志流个数相同，每个日志流的大小相同。
 * 可以计算A这个cell下面占总比例的1/3,B这个cell占2/3
 * 对于第一个需要扩容出来的日志流组:
 *   如果要选择A这个cell，则扩容后每个日志流组的比例是 1/3 /(1 + 1) 等于1/6
 *   如果要选择B这个cell，则扩容后每个日志流组的比例是 2/3 /(2 + 1) 等于2/9
 *   综上，我们要选择一个扩容后比例最大的，选择B这个cell扩容。
 * 对于第二个需要扩容出来的日志流组:
 *   如果要选择A这个cell，则扩容后每个日志流组的比例是 1/3 /(1 + 1) 等于1/6
 *   如果要选择B这个cell，前面已经扩容过一个了，所以B当前的日志流组是3，则扩容后每个日志流组的比例是 2/3 /(3 + 1) 等于1/6。
 *   这个时候AB的扩容后比例是一样的，正常情况下是可以随机选择的，但是选择原始日志流最小的更加容易扩容，所以选择A。
 *
 * 另外，enable_transfer关闭的情况，如果cell只有单个ls的lsg，那么该cell无法扩容。
 * 这一行所有cell都不能扩容也是有可能的,跳过这行即可。
 * */
int ObLSGroupCountBalance::set_cell_expand_lg_cnt_(LSGroupMatrix &lg_matrix,
      const int64_t row_index, const int64_t target_lg_cnt, const int64_t curr_lg_count)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = lg_matrix.get_column_count();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(row_index < 0 || row_index >= lg_matrix.get_row_count()
        || curr_lg_count <= 0
        || curr_lg_count >= target_lg_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(row_index), K(curr_lg_count), K(target_lg_cnt));
  } else {
    bool no_more_cell_to_expand = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_lg_cnt - curr_lg_count && !no_more_cell_to_expand; ++i) {
      //对于每一个需要扩容的日志流组，从行上的每一列找到最大的比例分上去
      ObLSGroupMatrixCell* target_cell = NULL;
      double load_factor = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < col_cnt; ++j) {
        ObLSGroupMatrixCell *cell = lg_matrix.get(row_index, j);
        CK(OB_NOT_NULL(cell))
        if (OB_SUCC(ret)) {
          double cell_lg_cnt = cell->get_ls_group_count();
          int64_t cell_ls_count = 0;
          if (OB_FAIL(cell->get_ls_count(cell_ls_count))) {
            LOG_WARN("failed to get ls count", KR(ret), KPC(cell));
          } else if (cell_lg_cnt <= 0 ||
              (!tenant_info_->job_desc_.get_enable_transfer()
                && cell_ls_count < cell->get_target_lg_cnt() + 1)) {
            // this cell can not expand, continue
          } else {
            //计算这个cell如果新增一个日志流组，均值有多少，取一个均值最大的cell
            //假设每一个日志流组上tablet和磁盘是一样的，使用比值计算，TODO
            //当前cell的总数是cell_lg_cnt / curr_lg_count,
            double tmp_factor = ( cell_lg_cnt / curr_lg_count) / (cell->get_target_lg_cnt() + 1);
            if (tmp_factor > load_factor) {
              target_cell = cell;
              load_factor = tmp_factor;
            } else if (fabs(tmp_factor - load_factor) < OB_DOUBLE_EPSINON) {
              CK(OB_NOT_NULL(target_cell));
              if (OB_SUCC(ret)) {
                //如果计算出来的均值是一样的，则选择原始日志流组比较少的一个作为目标端
                target_cell = target_cell->get_ls_group_count() > cell_lg_cnt ?
                  cell : target_cell;
              }
            }
          }
        }
      }//end for get max factor
      if (OB_SUCC(ret)) {
        if (nullptr == target_cell) {
          if (!tenant_info_->job_desc_.get_enable_transfer()) {
            // 此row已经没有可expand的cell，则跳出循环
            no_more_cell_to_expand = true;
            LOG_INFO("no more cell can expand in this row", K(row_index), K(i), K(target_lg_cnt), K(curr_lg_count));
          } else {
            // 不合预期. 开启enable_transfer预期一定有非空的cell，一定可以选出expand的cell
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("no cell can expand, but transfer is enabled, unexpected", KR(ret), K(row_index));
          }
        } else {
          target_cell->inc_expand_cnt();
          LOG_INFO("the cell need expand", KPC(target_cell), K(row_index));
        }
      }
    }//end for construct
  }
  return ret;

}


/*
 * 算法描述：
 * 经过前面的操作，如果某一个cell上的日志流组内没有日志流，则对通过分裂其他日志流组内的日志流，在空日志流组内构造出新的日志流
 * 1. 首先检查是否存在空的日志流组。如果不存在结束。找到所有的空日志流组的个数
 * 2. 把非空日志流组内的日志流作为分裂源端。
 * 3. 走到这一步，所有的非空日志流组，只会有一条日志流（不然前面的步骤会保证迁移到空日志流中取。）对这些非空日志流组，也只通过分裂合并搞出一条日志流。
 * 4. 构造每个日志流组内一条日志流的任务
 *
 * */
int ObLSGroupCountBalance::construct_expand_task_for_cell_emtpy_lg_(ObIArray<ObLSGroupStat*> &curr_lg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(curr_lg.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls group is empty", KR(ret), K(curr_lg));
  } else {
    //把多出来的日志流给空ls_group分过去,从每个有lg
    //前面已经进行了cell内日志流个数的均衡，如果还存在空的ls_group,则走分裂合并的逻辑
    int64_t expand_ls_cnt = 0;
    ObSplitLSParamArray src_ls;
    ObArray<ObSplitLSParamArray> dest_split_array;
    ObArray<uint64_t> ls_group_ids;
    ARRAY_FOREACH(curr_lg, idx) {
      ObLSGroupStat *lg_stat = curr_lg.at(idx);
      CK(OB_NOT_NULL(lg_stat))
      if (OB_FAIL(ret)) {
      } else if (lg_stat->ls_count_in_group() == 0) {
        //日志流组内为空，是分裂的目的端
        if (OB_FAIL(ls_group_ids.push_back(lg_stat->lg_id_))) {
          LOG_WARN("failed to push back", KR(ret), KPC(lg_stat));
        }
      } else if (1 != lg_stat->ls_count_in_group() && !ls_group_ids.empty()) {
        //存在空日志流组的情况下，不可能存在一个日志流组内的日志流个数大于1
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls count in group can not more than one", KR(ret), K(curr_lg));
      //日志流组非空，则作为分裂源端
      } else if (OB_FAIL(construct_src_split_param_array(*lg_stat, src_ls))) {
        LOG_WARN("failed to construct src split param", KR(ret), KPC(lg_stat));
      }
    }//end for get src param and expand_ls_cnt
    if (OB_FAIL(ret)) {
    } else if (ls_group_ids.empty()) {
      //没有空lg是预期内的结果
      LOG_INFO("no need expand by transfer", K(curr_lg));
    } else if (FALSE_IT(expand_ls_cnt = ls_group_ids.count())) {
    } else if (OB_FAIL(construct_expand_dest_param(expand_ls_cnt, src_ls, dest_split_array))) {
      LOG_WARN("failed to construct dest split array", KR(ret), K(expand_ls_cnt), K(src_ls));
    } else if (OB_UNLIKELY(dest_split_array.count() != expand_ls_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dest split array not equal to expand ls count", KR(ret), K(expand_ls_cnt), K(src_ls), K(dest_split_array));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < expand_ls_cnt; ++i) {
      //获取新的ls_group_id
      uint64_t ls_group_id = ls_group_ids.at(i);
      LOG_INFO("expand in new ls group", K(ls_group_id), K(i));
      if (OB_FAIL(construct_ls_expand_task(ls_group_id, dest_split_array.at(i)))) {
        LOG_WARN("failed to construct ls expand task", KR(ret), K(ls_group_id),
            K(i), K(dest_split_array.at(i)));
      }
    } //end for all lg
  }
  return ret;
}

/*
 * 获取每个单元格内当前的日志流组和需要扩容出来的日志流组
 * curr_lg_array ：包括了当前日志流组的指针和需要扩容出来的日志流组指针。
 * expand_lg_array：需要扩容出来的日志流组
 * */
int ObLSGroupCountBalance::get_cell_expand_lg_(ObLSGroupMatrixCell &cell,
    ObIArray<ObLSGroupStat*> &curr_lg_array,
    ObArray<ObLSGroupStat> &expand_lg_array)
{
  int ret = OB_SUCCESS;
  curr_lg_array.reset();
  expand_lg_array.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(cell.get_expand_shrink_cnt() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cell no need expand cnt", KR(ret), K(cell));
  } else {
     int64_t expand_cnt = cell.get_expand_shrink_cnt();
     LOG_INFO("cell need expand", K(cell));
     if (OB_FAIL(curr_lg_array.assign(cell.get_ls_groups()))) {
       LOG_WARN("failed to assign", KR(ret), K(cell));
     } else if (curr_lg_array.empty() || OB_ISNULL(curr_lg_array.at(0))) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("expand source is empty", KR(ret), K(curr_lg_array));
     }
     //构造几个空ls_group放进去
     for (int64_t i = 0; OB_SUCC(ret) && i < expand_cnt; ++i) {
       //获取新的ls_group_id
       uint64_t ls_group_id = 0;
       ObLSGroupStat lg_stat;
       if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_group_id(sql_proxy_,
               tenant_info_->tenant_id_, ls_group_id))) {
         LOG_WARN("failed to fetch new ls group id", KR(ret), K(tenant_info_->tenant_id_));
         //扩容只存在于同cell内，所以扩容出来的lg和之前的lg有相同的unit_list
       } else if (OB_FAIL(lg_stat.init(ls_group_id, curr_lg_array.at(0)->current_unit_list_))) {
         LOG_WARN("failed to init lg stat", KR(ret), K(ls_group_id), K(curr_lg_array.at(0)));
       } else if (OB_FAIL(expand_lg_array.push_back(lg_stat))) {
         LOG_WARN("failed to push back", KR(ret), K(i), K(lg_stat));
       } else if (OB_FAIL(curr_lg_array.push_back(&expand_lg_array.at(expand_lg_array.count() - 1)))) {
         LOG_WARN("failed to push back to curr", KR(ret), K(expand_lg_array));
       } else {
         LOG_INFO("new ls group id", K(ls_group_id));
       }
     }//end for construct curr lg
  }
  return ret;

}


//每个某一个需要扩容出来的日志流来说，都需要在结束之后执行alter + merge任务
#define FINISH_LS_GROUP_OP \
  do {\
    if (FAILEDx(OB_FAIL(ObLSBalanceTaskHelper::add_ls_alter_task(\
              tenant_id, job_id, ls_group_id, dest_ls_id,\
              balance_strategy, *task_array_)))) {\
      LOG_WARN("failed to add alter task", KR(ret), K(tenant_id), K(job_id),\
          K(ls_group_id), K(dest_ls_id), K(balance_strategy));\
    } else if (dest_ls_id != target_ls_id) {\
      if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_merge_task(\
              tenant_id, job_id, ls_group_id, dest_ls_id, target_ls_id,\
              balance_strategy, *task_array_))) {\
        LOG_WARN("failed to add ls merge task", KR(ret), K(tenant_id), K(job_id),\
            K(ls_group_id), K(dest_ls_id), K(target_ls_id), K(balance_strategy));\
      }\
    }\
  } while(0)

/*
 * 对于每个需要扩容出来的日志流，构造一系列的split + alter + merge任务
 * 由于同cell内可能是有多个日志流的，举例说明：当前存在三个lg
 * lg1，lg2，lg3；1和2各有一个日志流，3为空，需要在3上扩容出来一个日志流。
 * 通过ObSplitLSParamArray生成的任务如下：lg1和lg2上各自分裂出1/3的日志流。
 * 构造出来的task 如下：
 * 1. 首先lg1上的日志流通过split操作分裂出新的日志流，这个日志流id也是最终放在lg3中id，称这个id为1003吧。
 * 2. 迭代到lg2上的时候，由于日志流组切换了，需要执行上面的FINISH_LS_GROUP_OP，只需要执行alter任务即可，把日志流1003的日志流组变更lg3
 * 3. 在变更完成后，对lg2上的日志流也生成了一个split任务，生成日志流1004；
 * 结束循环后，在执行上面的FINISH_LS_GROUP_OP，生成一个alter + merge任务
 * */
int ObLSGroupCountBalance::construct_ls_expand_task(const uint64_t ls_group_id,
     const ObSplitLSParamArray &dest_split_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check check_inner_stat", KR(ret));
  } else if (OB_UNLIKELY(!job_->is_valid() || dest_split_param.count() <= 0
        || 0 == ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_group_id), KPC(job_), K(dest_split_param));
  } else {
    uint64_t tenant_id = tenant_info_->tenant_id_;
    const share::ObBalanceJobID job_id = job_->get_job_id();
    const share::ObBalanceStrategy balance_strategy = job_->get_balance_strategy();
    ObTransferPartList part_list;
    //dest_split_param会涉及多个ls_group的，虽然我们外层保证了他们的member_list一定是
    //一样的，但是备库要感知这件事情，所以不能是一个split + transfer
    //正确的顺序应该是对于第一个ls_group，是split+  alter
    //剩下的ls_group是split+alter+merge到一个ls_group分裂出来的日志流上
    uint64_t last_lg_id = 0;
    ObLSID dest_ls_id;
    //最后要留下来的ls_id
    ObLSID target_ls_id;
    if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_id(sql_proxy_, tenant_id, target_ls_id))) {
      LOG_WARN("failed to fetch new ls id", KR(ret), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_split_param.count(); ++i) {
      const share::ObLSStatusInfo *src_ls = dest_split_param.at(i).get_ls_info();
      if (OB_ISNULL(src_ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("src ls is null", KR(ret), K(i), K(dest_split_param));
      } else if (OB_FAIL(construct_ls_part_info(dest_split_param.at(i), target_ls_id, part_list))) {
        LOG_WARN("failed to construct ls part info", KR(ret), KPC(src_ls));
      } else if (0 != last_lg_id && src_ls->ls_group_id_ != last_lg_id) {
        //生成alter,结束上一个日志流组的操作,如果不是第一个日志流，还是需要生成merge任务
        FINISH_LS_GROUP_OP;
        dest_ls_id.reset();
      }
      if (OB_SUCC(ret) && src_ls->ls_group_id_ != last_lg_id) {
        if (0 == i) {
          dest_ls_id = target_ls_id;
        }
        if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_split_task(
                GCTX.sql_proxy_, tenant_id, job_id, src_ls->ls_group_id_,
                src_ls->ls_id_, part_list, balance_strategy, dest_ls_id,
                *task_array_))) {
          LOG_WARN("failed to add ls split task failed", KR(ret), K(tenant_id), K(job_id),
              K(balance_strategy), KPC(src_ls), K(dest_ls_id), K(part_list));
        }
        last_lg_id = src_ls->ls_group_id_;
      } else {
        //先前的balance_ls_between_lgs_in_cell_算法保证了
        //每个日志流组内只会有一个日志流，所以不会存在这种情况
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not has more than one LS in group", KR(ret), K(i), K(dest_split_param));
      }
    }
    //最后还需要一个alter任务，把最后一批生成的日志流alter到目标ls_group_id
    FINISH_LS_GROUP_OP;
  }

  return ret;
}
#undef FINISH_LS_GROUP_OP
/*
 * curr_lg_array:包含了需要扩容出来的日志流和当前cell上全部的日志流
 * 为什么扩容要先执行alter任务：在同cell内，如果存在需要扩容出来的日志流组，执行做alter操作
 * 是没有任何代价的，只需要变更日志流的日志流组id即可，不需要有实际的迁移或者其他的操作。
 * 找到日志流个数最多的日志流组max_lg，找到日志流最少的日志流组min_lg
 * 如果max_lg内的日志流个数减去min_lg的日志流个数大于1，则调整max_lg内的一个日志流到min_lg内
 * */
int ObLSGroupCountBalance::balance_ls_between_lgs_in_cell_(ObArray<ObLSGroupStat*> &curr_lg_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check check_inner_stat", KR(ret));
  } else if (OB_UNLIKELY(curr_lg_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(curr_lg_array));
  } else {
    ObLSGroupStat* max_lg = NULL;
    ObLSGroupStat* min_lg = NULL;
    //找到max_lg和min_lg
    do {
      max_lg = NULL, min_lg = NULL;
      ARRAY_FOREACH(curr_lg_array, idx) {
        ObLSGroupStat *curr_lg = curr_lg_array.at(idx);
        CK(OB_NOT_NULL(curr_lg))
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(max_lg) || max_lg->ls_count_in_group() < curr_lg->ls_count_in_group()) {
            max_lg = curr_lg;
          }
          if (OB_ISNULL(min_lg) || min_lg->ls_count_in_group() > curr_lg->ls_count_in_group()) {
            min_lg = curr_lg;
          }
        }
      }//end for get min and max lg
      CK(OB_NOT_NULL(min_lg), OB_NOT_NULL(max_lg))
      if (OB_SUCC(ret)) {
        if (max_lg->ls_count_in_group() - min_lg->ls_count_in_group() < 2) {
          LOG_INFO("ls group is balance", KPC(max_lg), KPC(min_lg));
          break;
        } else {
          int64_t ls_index = -1;
          if (OB_FAIL(max_lg->find_and_remove_ls(ls_index))) {
            LOG_WARN("failed to find and remove ls", KR(ret), KPC(max_lg));
          } else if (OB_FAIL(min_lg->add_ls_status(ls_index))) {
            LOG_WARN("failed to add ls status", KR(ret), K(ls_index));
          }
          LOG_INFO("ls migrate to new ls group", KR(ret), K(ls_index), KPC(max_lg), KPC(min_lg));
        }
      }
    } while (OB_SUCC(ret));
    //结束上面的循环后，生成alter任务
    if (FAILEDx(generate_alter_task_for_each_lg_(curr_lg_array))) {
      LOG_WARN("failed to generate migrate task", KR(ret), K(curr_lg_array));
    }
  }
  return ret;
}

/*
 * 上面的逻辑存在把一个日志流从一个组调整到另外一个组的处理，保证日志流在各个日志流组内日志流个数相同。
 * 对于一个日志流组内日志流的ls_group_id信息和ObLSGroupStat不一致的，需要生成alter任务
 * */
int ObLSGroupCountBalance::generate_alter_task_for_each_lg_(ObArray<ObLSGroupStat*> &curr_lg_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check check_inner_stat", KR(ret));
  } else if (OB_UNLIKELY(curr_lg_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(curr_lg_array));
  }
  ARRAY_FOREACH(curr_lg_array, idx) {
    ObLSGroupStat* curr_lg = curr_lg_array.at(idx);
    CK(OB_NOT_NULL(curr_lg))
    if (OB_SUCC(ret)) {
      //如果日志流组的ls_group_id和日志流不匹配，则生成ls_alter任务
      ObArray<ObLSStatusInfo*> status_info_array;
      uint64_t tenant_id = tenant_info_->tenant_id_;
      const share::ObBalanceJobID job_id = job_->get_job_id();
      const share::ObBalanceStrategy balance_strategy = job_->get_balance_strategy();
      if (OB_FAIL(tenant_info_->get_ls_status_in_lg(*curr_lg, status_info_array))) {
        LOG_WARN("failed to get ls status in lg", KR(ret), KPC(curr_lg));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < status_info_array.count(); ++i) {
        ObLSStatusInfo* status_info = status_info_array.at(i);
        CK(OB_NOT_NULL(status_info));
        if (OB_FAIL(ret)) {
        } else if (curr_lg->lg_id_ != status_info->get_ls_group_id()) {
          if (OB_FAIL(OB_FAIL(ObLSBalanceTaskHelper::add_ls_alter_task(
                    tenant_id, job_id, curr_lg->lg_id_, status_info->get_ls_id(),
                    balance_strategy, *task_array_)))) {
            LOG_WARN("failed to add alter task", KR(ret), K(tenant_id), K(job_id),
                K(balance_strategy), KPC(curr_lg), KPC(status_info));
          } else {
            //后续还有分裂任务，如果这里不正确，会导致后续生成的任务也不正确
            status_info->ls_group_id_ = curr_lg->lg_id_;
          }
        }
      }//end for process each ls in group
    }
  }//process each ls group
  return ret;
}

/*
 * 算法描述：
 * 缩容的日志流组个数等于 ：curr_lg_cnt - target_lg_cnt
 * 缩容的日志流组的选择：
 * 1. 优选选择同cell缩容,同cell缩容时，选择缩容后每个日志流平均值最小的cell，和扩容相反
 * 2. 在所有的cell都最多只有1个cell的前提下，选择这一行上所有cell所在列上日志流组个数最多的cell缩容掉。
 *  举例说明一个4*6的异构zone，4（1，2，3，4）作为行，5(a,b,c,d,e,f)作为列,二维矩阵的第一行的每个单元格内日志流组的数量如下：
 *   A B C D E F
 *   1 2 3 1 1 0
 * 由于每一行应该有3个日志流组，我们现在的算法假设比较粗糙，假设每个日志流组内的日志流个数相同，每个日志流的大小相同，这一行当前有8个日志流，需要缩容掉5个日志流。
 * 可以计算A,DE这个cell下面占总比例的1/8,B这个cell占1/4,C占3/8
 * 第一个日志流组，可以在BC这两个cell中进行同cell缩容。
 *   如果要选择B这个cell，则缩容后每个日志流组的比例是 1/4 /(2 -1) 等于1/4
 *   如果要选择C这个cell，则缩容后每个日志流组的比例是 3/8 /(3-1) 等于3/16
 *   综上，我们要选择一个扩容后比例最小的，选择C这个cell扩容。
 * 对于第二个需要缩容出来的日志流组，还是可以在BC这两个cell中进行同cell缩容。
 *   如果要选择B这个cell，则缩容后每个日志流组的比例是 1/4 /(2 -1) 等于1/4
 *   如果要选择C这个cell，前面已经缩容过一个，C当前只有两个日志流组，
 *   则缩容后每个日志流组的比例是 3/8 /(2-1) 等于3/8
 *   综上，最小的是1/4,选择B进行缩容
 * 对于第三个需要缩容出来的日志流组，只有C可以进行同cell缩容。
 * 对于第四个，已经不能进行同cell缩容了，只能看，ABCDE这几列，谁的日志流组总数最多选择谁进行缩容，假设B是最多的，则可以把B上所有的日志流组都缩容掉。
 * 对于第五个，也是要去看ACDE这几列，谁的日志流组总数最多，假设选择E列。
 * 经过上述算法，这一行上，每一列的日志流组数量为
 * A B   C    D  E   F
 * 1 2-2 3-2  1  1-1 0
 * */
int ObLSGroupCountBalance::shrink_ls_group_cnt_(
    LSGroupMatrix &lg_matrix, const int64_t row_index, const int64_t target_lg_cnt,
    const int64_t curr_lg_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(row_index < 0 || row_index >= lg_matrix.get_row_count()
        || curr_lg_cnt < target_lg_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row index is invalid", KR(ret), K(row_index), K(target_lg_cnt), K(curr_lg_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < curr_lg_cnt - target_lg_cnt; ++i) {
      //对于每一个需要扩容的日志流组，从行上的每一列找到最大的比例分上去
      ObLSGroupMatrixCell* target_cell = NULL;
      double load_factor = DBL_MAX;
      const int64_t col_cnt = lg_matrix.get_column_count();
      for (int64_t j = 0; OB_SUCC(ret) && j < col_cnt; ++j) {
        ObLSGroupMatrixCell *cell = lg_matrix.get(row_index, j);
        CK(OB_NOT_NULL(cell))
        if (OB_SUCC(ret)) {
          int64_t cell_target_lg_cnt = cell->get_target_lg_cnt();
          if (cell_target_lg_cnt > 1) {
            //还有缩容的余地
            double cell_curr_lg_cnt = cell->get_ls_group_count();
            double tmp_factor = (cell_curr_lg_cnt / curr_lg_cnt) / (cell_target_lg_cnt - 1);
            LOG_INFO("curr factor", K(cell_curr_lg_cnt), K(tmp_factor), K(curr_lg_cnt));
            if (tmp_factor < load_factor) {
              target_cell = cell;
              load_factor = tmp_factor;
            }
          }
        }
      }//end for get min factor

      if (OB_SUCC(ret) && OB_ISNULL(target_cell)) {
        //如果没有同cell可以缩容，则只能走迁移的逻辑，找到这一行cell所在列lg总数，谁的lg最多选择哪个cell
        if (OB_FAIL(get_cell_with_max_lg_in_column_(lg_matrix, row_index, target_cell))) {
          LOG_WARN("failed to get max cell for shrink", KR(ret), K(row_index));
        }
      }
      CK(OB_NOT_NULL(target_cell))
      if (OB_SUCC(ret)) {
        target_cell->inc_shrink_cnt();
        LOG_INFO("cell need shrink", KPC(target_cell));
      }
    }//end for construct
    if (FAILEDx(construct_shrink_task_(lg_matrix, row_index))) {
      LOG_WARN("failed to construct shrink task", KR(ret), K(row_index));
    }
  }
  return ret;
}

/*
 * 选择一个cell，这个cell所在列日志流组的个数总数最大
 * */
int ObLSGroupCountBalance::get_cell_with_max_lg_in_column_(
    LSGroupMatrix &lg_matrix, const int64_t row_index, ObLSGroupMatrixCell* &cell)
{
  int ret = OB_SUCCESS;
  cell = NULL;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(row_index < 0 || row_index >= lg_matrix.get_row_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row index is invalid", KR(ret), K(row_index));
  } else {
    //如果没有同cell可以缩容，则只能走迁移的逻辑，找到这一行cell所在列lg总数，谁的lg最多选择哪个cell
    int64_t max_lg_count = -1;
    const int64_t row_cnt = lg_matrix.get_row_count();
    const int64_t col_cnt = lg_matrix.get_column_count();
    LOG_INFO("need shrink out cell", K(row_cnt), K(col_cnt));
    for (int64_t j = 0; OB_SUCC(ret) && j < col_cnt; ++j) {
      ObLSGroupMatrixCell *col_cell = lg_matrix.get(row_index, j);
      CK(OB_NOT_NULL(col_cell))
      if (OB_SUCC(ret)) {
        const int64_t cell_lg_cnt = col_cell->get_target_lg_cnt();
        if (cell_lg_cnt > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the cell has more than one ls group is unexpected", KR(ret),
              KPC(col_cell), K(row_index), K(j));
        } else if (0 == cell_lg_cnt) {
          LOG_INFO("cell is empty, can not shrink", K(row_index), K(j));
          continue;
        } else {
          //计算这一列上所有的lg_cnt
          int64_t col_lg_cnt = 0;
          for (int64_t k = 0; OB_SUCC(ret) && k < row_cnt; ++k) {
            ObLSGroupMatrixCell *row_cell = lg_matrix.get(k, j);
            CK(OB_NOT_NULL(row_cell))
            if (OB_SUCC(ret) && k != row_index) {
              //不确定是最终值，只能使用原始值，TODO
              col_lg_cnt += row_cell->get_ls_group_count();
            }
          }//end for get each column lg cnt
          if (OB_SUCC(ret) && col_lg_cnt > max_lg_count) {
            cell = col_cell;
            max_lg_count = col_lg_cnt;
          }
        }
      }
    }//end for get migrate cell
  }
  return ret;
}
/*
 * 算法描述：构造需要缩容的源端和缩容的目的端
 * 缩容实际上有两种
 * 1. 同cell缩容
 *    缩容条件是cell->get_expand_shrink_cnt() 大于0，并且当前cell上还有可以剩下的日志流组
 *    缩容源端：cell内记录的get_expand_shrink_cnt个数的日志流组
 *    缩容目的端：cell内当前日志流组减去get_expand_shrink_cnt个数的日志流组
 * 2. 跨cell缩容
 *    缩容条件：一个cell内存在日志流组，但是全部都被标记了缩容，即当前日志流组的个数等于get_expand_shrink_cnt
 *    缩容源端：一个cell内所有的日志流组都被缩容掉了，这里的日志流组就是缩容源端
 *    缩容目的端：每个cell内当前的日志流组迁移get_expand_shrink_cnt还剩下的日志流组
 *
 * 同cell缩容算法：
 *   1. 由于同cell不需要迁移，前通过alter操作补齐每个日志流组需要的日志流个数。
 *   2. 在每个日志流组内日志流个数已经足够多的情况下，每个需要留下来的日志流平分需要缩容掉的日志流。
 * 跨cell只有第二步，没有第一步
 * */
int ObLSGroupCountBalance::construct_shrink_task_(LSGroupMatrix &lg_matrix, const int64_t row_index)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(row_index < 0 || row_index >= lg_matrix.get_row_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row index is invalid", KR(ret), K(row_index));
  } else {
    const int64_t col_cnt = lg_matrix.get_column_count();
    //需要跨cell合并的目的端
    ObArray<ObLSGroupStat*> target_lg;
    //需要跨cell合并的日志流组
    ObArray<ObLSGroupStat*> shrink_lg;
    for (int64_t j = 0; OB_SUCC(ret) && j < col_cnt; ++j) {
      ObLSGroupMatrixCell *cell = lg_matrix.get(row_index, j);
      int64_t target_lg_cnt = 0;
      if (OB_ISNULL(cell)) {
      } else if (FALSE_IT(target_lg_cnt = cell->get_target_lg_cnt())) {
      } else if (0 == cell->get_ls_group_count()) {
        //cell内没有任何日志流组，不需要做操作
      } else if (0 == cell->get_expand_shrink_cnt()) {
        //cell内有日志流组，但是不需要缩容，这里面全都是跨缩容目的端
        if (OB_FAIL(append_array_no_dup(target_lg, cell->get_ls_groups()))) {
          LOG_WARN("failed to append array", KR(ret), KPC(cell));
        }
      } else if (0 == target_lg_cnt) {
        //cell 内有日志流组，需要缩容，并且缩容完了，这个cell都是跨缩容源端
        if (OB_FAIL(append_array_no_dup(shrink_lg, cell->get_ls_groups()))) {
          LOG_WARN("failed to append array", KR(ret), KPC(cell));
        }
      } else {
        //cell 内有日志流组，需要缩容，并没有缩容完，需要先进行同cell缩容
        //缩容完了后，剩下的日志流组是跨cell的源端
        ObArray<ObLSGroupStat*> &lg_array = cell->get_ls_groups();
        ObArray<ObLSGroupStat*> cell_target_lg;
        ObArray<ObLSGroupStat*> cell_shrink_lg;
        LOG_INFO("cell can shrink in cell", KPC(cell));
        for (int64_t i = 0; OB_SUCC(ret) && i < lg_array.count(); ++i) {
          ObLSGroupStat* lg = lg_array.at(i);
          CK(OB_NOT_NULL(lg))
          if (OB_FAIL(ret)) {
          } else if (i < target_lg_cnt) {
            if (OB_FAIL(cell_target_lg.push_back(lg))) {
              LOG_WARN("failed to push back lg", KR(ret), K(i), K(j));
            }
          } else if (OB_FAIL(cell_shrink_lg.push_back(lg))) {
            LOG_WARN("failed to push back", K(i), K(j));
          }
        }//end for get i
        if (FAILEDx(complete_missing_LS_in_lg_(cell_target_lg, cell_shrink_lg))) {
          LOG_WARN("failed to construct ls migrate task", KR(ret), K(cell_target_lg), K(cell_shrink_lg));
        } else if (cell_shrink_lg.empty()) {
          LOG_INFO("shrink ls is empty, no need shrink", KR(ret), K(cell_target_lg));
        } else if (OB_FAIL(construct_lg_shrink_task_(cell_target_lg, cell_shrink_lg))) {
          LOG_WARN("failed to construct lg shrink task", KR(ret), K(cell_target_lg));
        }
        if (FAILEDx(append_array_no_dup(target_lg, cell_target_lg))) {
          LOG_WARN("failed to append array", KR(ret), K(j));
        }
      }
    }
    if (OB_SUCC(ret) && shrink_lg.count() > 0) {
      LOG_INFO("need shrink out cell", K(shrink_lg));
      if (OB_FAIL(construct_lg_shrink_task_(target_lg, shrink_lg))) {
        LOG_WARN("failed to construct shrink task", KR(ret), K(target_lg), K(shrink_lg));
      }
    }
  }
  return ret;
}

/*
 * 每个留下来的日志流组内应该有primary_zone * ls_scale_out_factor个数的日志流，也就是job_desc_.get_ls_cnt_in_group();
 * 如果不够的话，从缩容源端直接alter过来，只有一个alter操作，没有实际的迁移.
 * 特别注意，如果shrink_lg空了，就从数组中移除，方便不进行下一步的均分操作
 * */
int ObLSGroupCountBalance::complete_missing_LS_in_lg_(
    ObArray<ObLSGroupStat*> &target_lg, ObArray<ObLSGroupStat*> &shrink_lg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(target_lg.empty() || shrink_lg.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row index is invalid", KR(ret), K(target_lg), K(shrink_lg));
  } else {
    //检查当前的target_lg有没有足够多的日志流，没有的话，先尝试补全这些日志流组
    //这种补全只在同cell发生
    bool is_finish = false;//代表shrink_lg肯定空了
    const int64_t ls_cnt_in_group = tenant_info_->job_desc_.get_ls_cnt_in_group();
    for (int64_t i = 0; OB_SUCC(ret) && i < target_lg.count() && !is_finish; ++i) {
      ObLSGroupStat* lg = target_lg.at(i);
      CK(OB_NOT_NULL(lg))
      if (OB_SUCC(ret) && lg->ls_count_in_group() < ls_cnt_in_group) {
        int64_t migrate_cnt = ls_cnt_in_group - lg->ls_count_in_group();
        do {
          ObLSGroupStat *target_lg = NULL;
          for (int64_t j = shrink_lg.count() - 1; OB_SUCC(ret) && j >= 0; --j) {
            ObLSGroupStat* tmp_lg = shrink_lg.at(j);
            CK(OB_NOT_NULL(tmp_lg))
            if (OB_FAIL(ret)) {
            } else if (tmp_lg->ls_count_in_group() == 0) {
              if (OB_FAIL(shrink_lg.remove(j))) {
                LOG_WARN("failed to remove shrink lg", KR(ret), K(j));
              }
            } else {
              target_lg = tmp_lg;
            }
          }//end for get target lg
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(target_lg)) {
            is_finish = true;
          } else {
            int64_t ls_index = -1;
            int64_t tmp_migrate_cnt = min(migrate_cnt, target_lg->ls_count_in_group());
            for (int64_t j = 0; OB_SUCC(ret) && j < tmp_migrate_cnt; ++j) {
              if (OB_FAIL(target_lg->find_and_remove_ls(ls_index))) {
                LOG_WARN("failed to find and remove ls", KR(ret), K(ls_index));
              } else if (OB_FAIL(lg->add_ls_status(ls_index))) {
                LOG_WARN("failed to add ls status", KR(ret), K(ls_index));
              }
              LOG_INFO("ls migrate to new ls group", KR(ret), K(ls_index), KPC(target_lg), KPC(lg));
            }//end for get target lg
            migrate_cnt -= tmp_migrate_cnt;
          }
          //shrink_lg非空，并且还有需要迁移的个数
        } while (OB_SUCC(ret) && !is_finish && migrate_cnt > 0);
      }
    }//end for each target lg
    if (FAILEDx(generate_alter_task_for_each_lg_(target_lg))) {
      LOG_WARN("failed to generate migrate task for lg", KR(ret), K(target_lg));
    } else if (1 == shrink_lg.count()) {
      //恰好target_lg缺的和shrink_lg内的日志流个数是一样的
      ObLSGroupStat* tmp_lg = shrink_lg.at(0);
      CK(OB_NOT_NULL(tmp_lg))
      if (OB_SUCC(ret) && 0 == tmp_lg->ls_count_in_group()) {
        if (OB_FAIL(shrink_lg.remove(0))) {
          LOG_WARN("failed to remove shrink lg", KR(ret));
        }
      }
    }
  }
  return ret;
}

/*
 * 均分缩容的时候需要参考transfer是否开启决定我们是通过分裂合并呢，还是只能通过alter把缩容的日志流组内的日志流
 * 均匀的分给剩下的日志流组
 * */
int ObLSGroupCountBalance::construct_lg_shrink_task_(
    ObArray<ObLSGroupStat*> &target_lg, ObArray<ObLSGroupStat*> &shrink_lg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(target_lg.empty() || shrink_lg.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row index is invalid", KR(ret), K(target_lg), K(shrink_lg));
  } else if (tenant_info_->job_desc_.get_enable_transfer()) {
    if (OB_FAIL(construct_lg_shrink_transfer_task_(target_lg, shrink_lg))) {
      LOG_WARN("failed to construnct shrink task", KR(ret), K(target_lg), K(shrink_lg));
    }
  } else if (OB_FAIL(generate_shrink_lg_disable_transfer_(target_lg, shrink_lg))) {
    LOG_WARN("failed to construnct shrink task", KR(ret), K(target_lg), K(shrink_lg));
  }
  return ret;
}
/*
 * 开启transfer，构造分裂源端和分裂目的端
 * 分裂源端：shrink_lg内所以得日志流组
 * 分裂目的端：target_lg内所有的日志流组
 * */
int ObLSGroupCountBalance::construct_lg_shrink_transfer_task_(
    ObArray<ObLSGroupStat*> &target_lg, ObArray<ObLSGroupStat*> &shrink_lg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(target_lg.empty() || shrink_lg.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row index is invalid", KR(ret), K(target_lg), K(shrink_lg));
  } else {
    ObSplitLSParamArray src_ls;
    ObArray<ObSplitLSParamArray> dest_ls;
    ObArray<ObLSStatusInfo*> ls_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_lg.count(); ++i) {
      ObLSGroupStat* lg = target_lg.at(i);
      CK(OB_NOT_NULL(lg))
      if (FAILEDx(tenant_info_->get_ls_status_in_lg(*lg, ls_array))) {
        LOG_WARN("failed to get ls status info", KR(ret), KPC(lg));
      }
    }//end for get target ls
    for (int64_t i = 0; OB_SUCC(ret) && i < shrink_lg.count(); ++i) {
      ObLSGroupStat* lg = shrink_lg.at(i);
      CK(OB_NOT_NULL(lg))
      if (FAILEDx(construct_src_split_param_array(*lg, src_ls))) {
        LOG_WARN("failed to construct src split param", KR(ret), KPC(lg));
      }
    }
    const int64_t target_ls_count = ls_array.count();
    if (FAILEDx(construct_shrink_src_param(target_ls_count, src_ls, dest_ls))) {
      LOG_WARN("failed to construct shrink src param", KR(ret), K(target_ls_count));
    } else if (OB_UNLIKELY(dest_ls.count() != target_ls_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("target_ls_cnt should be equal to the size of dest_ls", KR(ret),
          K(target_ls_count), K(dest_ls));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < target_ls_count; ++i) {
      ObLSStatusInfo *ls_status = ls_array.at(i);
      CK(OB_NOT_NULL(ls_status))
      //对于每个要留下的日志流，都要进行一系列的分裂合并操作
      if (FAILEDx(construct_ls_shrink_task_(dest_ls.at(i), *ls_status))) {
        LOG_WARN("failed to construct ls shrink task", KR(ret), KPC(ls_status), "dest ls", dest_ls.at(i));
      }
    }
  }
  return ret;
}

//对于每一个日志流组的最后一个日志流，都是alter + merge到对应的日志流上
#define FINISH_LS_GROUP_SHRINK \
  do {\
    if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_alter_task(tenant_id,\
            job_->get_job_id(), ls_group_id, last_ls_id, \
            job_->get_balance_strategy(), *task_array_))) {\
      LOG_WARN("failed to add ls alter task", KR(ret), K(tenant_id), KPC(job_),\
          K(ls_group_id), K(last_ls_id));\
    } else if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_merge_task(tenant_id,\
            job_->get_job_id(), ls_group_id, last_ls_id, dest_ls_id,\
            job_->get_balance_strategy(), *task_array_))) {\
      LOG_WARN("add ls merge task failed", KR(ret), K(tenant_id), KPC(job_),\
          K(ls_group_id), K(dest_ls_id), K(last_ls_id));\
    }\
  } while(0)

/*
 * 需要拿过来的日志流可能来自多个日志流组。
 * 同组内：1. 如果第一个日志流不需要全部过来，则生成split任务，否则直接merge任务
 *         2. 同组内的后面的日志流，如果需要全部过来，则merge到第一个日志流上，否则使用transfer。
 * 跨组的是，需要先把前一个组生成的日志流使用alter+merge操作先合并到目标日志流上，然后在开启本轮的处理。
 * for循环结束后也要先执行一个FINISH_LS_GROUP_SHRINK
 * */
int ObLSGroupCountBalance::construct_ls_shrink_task_(
    const ObSplitLSParamArray &src_split_param,
    const ObLSStatusInfo &ls_status_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("not init", KR(ret), KP(sql_proxy_));
  } else if (OB_UNLIKELY(!job_->is_valid() || src_split_param.count() <= 0 || !ls_status_info.is_valid())) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("error unexpected", KR(ret), K(job_), K(src_split_param), K(ls_status_info));
  } else {
    uint64_t tenant_id = tenant_info_->tenant_id_;
    //如果有一个lg内多个日志流都要生成分裂任务，则使用一个split + transfer
    //最后使用alter + merge任务
    //当前使用的ls_group_id和ls_Id
    uint64_t last_lg_id = 0;
    share::ObLSID last_ls_id;
    //最终大家的合并目的端
    const share::ObLSID &dest_ls_id = ls_status_info.ls_id_;
    //最终需要合并的日志流组
    const uint64_t ls_group_id = ls_status_info.ls_group_id_;
    ObTransferPartList part_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < src_split_param.count(); ++i) {
      part_list.reset();
      const ObSplitLSParam &param = src_split_param.at(i);
      const share::ObLSID &src_ls_id = param.get_ls_info()->ls_id_;
      const uint64_t source_lg_id = param.get_ls_info()->ls_group_id_;
      if (source_lg_id != last_lg_id && 0 != last_lg_id) {
        //先结束上一轮的alter 和merge任务
        FINISH_LS_GROUP_SHRINK;
      }
      if (OB_FAIL(ret)) {
      } else if (fabs(param.get_current_factor() - 1.0) >= OB_DOUBLE_EPSINON) {
        //需要拿过来的不等于1，则生成分裂任务，先准备好part_list
        if (OB_FAIL(construct_ls_part_info(param, dest_ls_id, part_list))) {
          LOG_WARN("failed to construct ls part list", KR(ret), K(param));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (source_lg_id != last_lg_id) {
        //这一组的第一个
        last_lg_id = source_lg_id;
        last_ls_id.reset();
        if (fabs(param.get_current_factor() - 1.0) < OB_DOUBLE_EPSINON) {
          //如果是这种情况就没必要分裂了,但是需要修改src_ls_id
          last_ls_id = src_ls_id;
        } else if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_split_task(
                GCTX.sql_proxy_, tenant_id, job_->get_job_id(), source_lg_id,
                src_ls_id, part_list,
                job_->get_balance_strategy(), last_ls_id, *task_array_))) {
          LOG_WARN("failed to add ls split task failed", KR(ret), K(tenant_id),
              KPC(job_), K(source_lg_id), K(part_list));
        }
      } else if (last_ls_id != src_ls_id) {
        if (fabs(param.get_current_factor() - 1.0) < OB_DOUBLE_EPSINON) {
          // need merge
          if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_merge_task(
                  tenant_id, job_->get_job_id(), source_lg_id, src_ls_id,
                  last_ls_id, job_->get_balance_strategy(), *task_array_))) {
            LOG_WARN("add ls merge task failed", KR(ret), K(tenant_id), KPC(job_),
                K(src_ls_id), K(last_ls_id), K(src_ls_id));
          }
        } else if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_transfer_task(
                tenant_id, job_->get_job_id(),
                source_lg_id, src_ls_id, last_ls_id, part_list,
                job_->get_balance_strategy(), *task_array_))) {
          LOG_WARN("failed to add ls transfer task", KR(ret), K(tenant_id), KPC(job_),
              K(source_lg_id), K(part_list));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can not has same ls", KR(ret), K(param), K(last_lg_id), K(last_ls_id));
      }
    }//end for
    //所有的都结束后
    FINISH_LS_GROUP_SHRINK;
  }
  return ret;
}
#undef FINISH_LS_GROUP_SHRINK

/*
 * 关闭transfer后，需要把shrink_lg内的日志流，均匀的分给剩下的日志流组
 * */
int ObLSGroupCountBalance::generate_shrink_lg_disable_transfer_(
    ObArray<ObLSGroupStat*> &target_lg, ObArray<ObLSGroupStat*> &shrink_lg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(target_lg.empty() || shrink_lg.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row index is invalid", KR(ret), K(target_lg), K(shrink_lg));
  } else {
    //给shrink_lg中的每个ls都在target_ls中找到一个最小日志流个数的lg塞进去
    const uint64_t tenant_id = tenant_info_->tenant_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < shrink_lg.count(); ++i) {
      ObLSGroupStat *lg = shrink_lg.at(i);
      CK(OB_NOT_NULL(lg))
      ObLSStatusInfo* ls_status = NULL;
      int64_t start_pos = -1, pos = -1;
      for (int64_t j = 0; OB_SUCC(ret) && j < lg->ls_count_in_group(); ++j) {
        ObLSGroupStat *tmp_target_lg = NULL;
        //获取一个日志流
        if (OB_FAIL(tenant_info_->get_next_ls_status_info(start_pos, *lg,
                pos, ls_status))) {
          LOG_WARN("failed to find next", KR(ret), K(start_pos));
        }
        CK(OB_NOT_NULL(ls_status))
        //找到日志流个数最小的日志流组
        for (int64_t k = 0; OB_SUCC(ret) && k < target_lg.count(); ++k) {
          ObLSGroupStat *tmp_lg = target_lg.at(k);
          CK(OB_NOT_NULL(tmp_lg))
          if (OB_SUCC(ret) && (NULL == tmp_target_lg
                || tmp_target_lg->ls_count_in_group() > tmp_lg->ls_count_in_group())) {
            tmp_target_lg = tmp_lg;
          }
        }
        CK(OB_NOT_NULL(tmp_target_lg))
        //放入目标日志流组，并且生成alter任务，这里没有修改内存中的ls_group_id，考虑到这里是最后一步操作了
        if (FAILEDx(tmp_target_lg->add_ls_status(pos))) {
          LOG_WARN("failed to add ls status", KR(ret), K(pos), K(j), K(i));
        } else if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_alter_task(tenant_id,
                job_->get_job_id(), tmp_target_lg->lg_id_, ls_status->ls_id_,
                job_->get_balance_strategy(), *task_array_))) {
          LOG_WARN("failed to add ls alter task", KR(ret), K(tenant_id), KPC(job_),
              KPC(tmp_target_lg), KPC(ls_status));
        }
        LOG_INFO("ls migrate to other ls group", K(pos), KPC(ls_status),
            KPC(lg), KPC(tmp_target_lg));
        start_pos = pos;
      }//end for each ls
    }//end for each lg
  }
  return ret;
}

////////end ObLSGroupCountBalance

////////////ObUnitListBalance
int ObUnitListBalance::balance(const bool only_job_strategy)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_FAIL(reorganize_sys_ls_unit_list_by_locality_())) {
    LOG_WARN("failed to reorganize sys ls unit list by locality", KR(ret));
  } else {
    ObArray<ObLSGroupStat> &ls_group_array = tenant_info_->ls_group_array_;
    if (!tenant_info_->job_desc_.get_enable_rebalance()) {
      FOREACH_CNT_X(ls_group, ls_group_array, OB_SUCC(ret)) {
        CK(OB_NOT_NULL(ls_group))
        if (FAILEDx(reorganize_unit_list_by_locality_(*ls_group))) {
          LOG_WARN("failed to reorganize unit locality", KR(ret), KPC(ls_group));
        }
      }
    } else {
      //在这个均衡之前已经保证，日志流组的个数和位置都满足预期，不会出现日志流组不在预定义的unit_group上面的情况
      ObHeteroUGArray hetero_ug;
      if (OB_FAIL(tenant_info_->split_hetero_zone_to_homo_unit_group(hetero_ug))) {
        LOG_WARN("failed to get homo unit group", KR(ret));
      }
      //1. 检查每个ls_group的unit_list中的unit是否还满足locality
      FOREACH_CNT_X(ls_group, ls_group_array, OB_SUCC(ret)) {
        if (OB_ISNULL(ls_group)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls group is null", KR(ret));
        } else if (OB_FAIL(construct_lg_unit_group_ids_(hetero_ug, *ls_group))) {
          LOG_WARN("failed to construct lg unit", KR(ret));
        }
      }
      //2. 根据enable_rebalance是否开启，检查每个ls_group是否均衡的分布在各个unit_group上面
      if (OB_SUCC(ret)) {
        FOREACH_CNT_X(ug_array, hetero_ug, OB_SUCC(ret)) {
          if (OB_ISNULL(ug_array)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ug group is null", KR(ret));
          } else if (OB_FAIL(balance_ls_unit_group_(*ug_array))) {
            LOG_WARN("failed to balance ls unit group", KR(ret), KPC(ug_array));
          }
        }
      }
      //3. 根据locality和unit_group构造出新的unit_list
      FOREACH_CNT_X(ls_group, ls_group_array, OB_SUCC(ret)) {
        if (OB_ISNULL(ls_group)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls group is null", KR(ret));
        } else if (OB_FAIL(construct_lg_unit_list_(*ls_group))) {
          LOG_WARN("failed to construct lg unit list", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      share::ObBalanceStrategy balance_strate(share::ObBalanceStrategy::LB_UNIT_LIST);
      if (OB_FAIL(try_construct_job_unit_list_op(balance_strate, only_job_strategy))) {
        LOG_WARN("failed to construct unit list op", KR(ret), K(balance_strate));
      }
    }
  }
  return ret;
}

// align sys ls unit_list according to locality when it's not empty.
// this operation is not affected by enable_rebalance.
int ObUnitListBalance::reorganize_sys_ls_unit_list_by_locality_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (tenant_info_->sys_ls_info_.get_unit_list().empty()) {
    LOG_INFO("sys_ls unit_list is empty, no need to reorganize", KR(ret));
  } else {
    const ObUnitIDList &curr_unit_list = tenant_info_->sys_ls_info_.get_unit_list();
    ObUnitIDList target_unit_list;
    // remove unit not in locality from unit_list
    ObArray<ObZone> valid_zone;
    if (OB_FAIL(filter_unit_list_in_locality_(curr_unit_list, target_unit_list, valid_zone))) {
      LOG_WARN("failed to filter unit list in locality", KR(ret), K(curr_unit_list));
    }
    // add unit to unit_list to cover all zone in locality
    ObLSInfo sys_ls_info;
    if (OB_ISNULL(GCTX.lst_operator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.lst_operator_ is null ptr", KR(ret));
    } else if (OB_FAIL(GCTX.lst_operator_->get(
        GCONF.cluster_id,
        tenant_info_->tenant_id_,
        SYS_LS,
        share::ObLSTable::DEFAULT_MODE,
        sys_ls_info))) {
      LOG_WARN("fail to get sys_ls info", KR(ret), "tenant_id", tenant_info_->tenant_id_);
    }
    ARRAY_FOREACH(tenant_info_->zone_array_, idx) {
      // if sys ls has replica in this zone, use unit of the replica, else use unit with least lsg
      const ObZoneLSStat &zone_stat = tenant_info_->zone_array_.at(idx);
      if (!zone_stat.is_in_locality_ || has_exist_in_array(valid_zone, zone_stat.zone_)) {
      } else {
        ObUnitLSStat *unit = NULL;
        FOREACH_CNT_X(replica, sys_ls_info.get_replicas(), OB_SUCC(ret) && nullptr == unit) {
          if (zone_stat.zone_ != replica->get_zone()) {
          } else if (OB_FAIL(tenant_info_->get_unit_ls_info(replica->get_unit_id(), unit))) {
            LOG_WARN("failed to get unit ls info", KR(ret), K(replica->get_unit_id()));
          }
        }
        if (OB_SUCC(ret) && nullptr == unit
            && OB_FAIL(zone_stat.get_min_unit_valid_for_normal_ls(unit))) {
          LOG_WARN("failed to get valid unit", KR(ret), K(zone_stat));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(unit)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit is invalid", KR(ret), K(unit));
        } else if (OB_FAIL(target_unit_list.push_back(ObDisplayUnitID(unit->unit_id_)))) {
          LOG_WARN("failed to push back", KR(ret));
        }
      }
    }//end for
    // update sys ls unit list
    if (OB_FAIL(ret)) {
    } else if (target_unit_list == tenant_info_->sys_ls_info_.unit_id_list_) {
      LOG_INFO("sys_ls unit_list not changed, skip", KR(ret), K(target_unit_list), K(tenant_info_->sys_ls_info_));
    } else {
      ObLSGroupUnitListOp lg_op;
      if (OB_FAIL(lg_op.init(tenant_info_->sys_ls_info_.get_ls_group_id(),
                             SYS_LS,
                             tenant_info_->sys_ls_info_.unit_id_list_,
                             target_unit_list, 1))) {
        LOG_WARN("failed to init ls group op", KR(ret), K(tenant_info_->sys_ls_info_), K(target_unit_list));
      } else if (OB_FAIL(lg_op_array_->push_back(lg_op))) {
        LOG_WARN("failed to push back", KR(ret), K(lg_op));
      }
    }
  }
  return ret;
}

int ObUnitListBalance::reorganize_unit_list_by_locality_(ObLSGroupStat &ls_group)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!ls_group.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_group));
  } else {
    ObUnitIDList &unit_list = ls_group.current_unit_list_;
    ObArray<ObZone> valid_zone;
    if (OB_FAIL(filter_unit_list_in_locality_(unit_list, ls_group.target_unit_list_, valid_zone))) {
      LOG_WARN("failed to filter unit list in locality", KR(ret), K(unit_list), K(ls_group));
    }

    ARRAY_FOREACH(tenant_info_->zone_array_, idx) {
      ObZoneLSStat &zone_stat = tenant_info_->zone_array_.at(idx);
      if (!zone_stat.is_in_locality_ || has_exist_in_array(valid_zone, zone_stat.zone_)) {
      } else {
        ObUnitLSStat *unit = NULL;
        if (OB_FAIL(get_valid_unit_by_inherit_(zone_stat, ls_group, unit))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            LOG_WARN("failed to get valid unit", KR(ret), K(zone_stat), K(ls_group));
          } else if (OB_FAIL(zone_stat.get_min_unit_valid_for_normal_ls(unit))) {
            LOG_WARN("failed to get valid unit", KR(ret), K(zone_stat));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(unit)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unit is invalid", KR(ret), K(unit));
        } else if (OB_FAIL(ls_group.target_unit_list_.push_back(ObDisplayUnitID(unit->unit_id_)))) {
          LOG_WARN("failed to push back", KR(ret));
        } else if (OB_FAIL(unit->add_ls_group(ls_group))) {
          LOG_WARN("failed to add ls group", KR(ret), K(ls_group));
        }
      }
    }//end for
  }
  return ret;
}

int ObUnitListBalance::filter_unit_list_in_locality_(
    const ObUnitIDList &unit_list, ObUnitIDList &tgt_unit_list, ObArray<ObZone> &tgt_zone_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(unit_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unit list is empty", KR(ret));
  } else {
    tgt_unit_list.reset();
    ARRAY_FOREACH(unit_list, idx) {
      ObZoneLSStat *zone_stat = NULL;
      uint64_t unit_id = unit_list.at(idx).id();
      if (OB_FAIL(tenant_info_->get_unit_zone_info(unit_id, zone_stat))) {
        LOG_WARN("failed to get unit ls info", KR(ret), K(unit_id));
      } else if (OB_ISNULL(zone_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone stat is null", KR(ret), K(unit_id));
      } else if (zone_stat->is_in_locality_) {
        if (OB_FAIL(tgt_unit_list.push_back(unit_list.at(idx)))) {
          LOG_WARN("failed to push back", KR(ret), K(idx));
        } else if (OB_FAIL(tgt_zone_list.push_back(zone_stat->zone_))) {
          LOG_WARN("failed to push back", KR(ret));
        }
      }
    }
  }
  return ret;
}

//检查这个日志流组在其他可用的组上都是相同的unit_group_id，如果是相同的，则继承这个unit_group_id
int ObUnitListBalance::get_valid_unit_by_inherit_(ObZoneLSStat &zone_stat,
                                                  const ObLSGroupStat &ls_group,
                                                  ObUnitLSStat* &unit)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!zone_stat.is_valid() || !zone_stat.is_in_locality_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(zone_stat));
  } else {
    //check has other zone which is balance and same unit_num
    const ObUnitIDList &valid_unit_list = ls_group.target_unit_list_;
    uint64_t target_unit_group = -1;
    ARRAY_FOREACH(valid_unit_list, jdx) {
      uint64_t unit_id = valid_unit_list.at(jdx).id();
      ObUnitLSStat *unit_stat = NULL;
      if (OB_FAIL(tenant_info_->get_unit_ls_info(unit_id, unit_stat))) {
        LOG_WARN("failed to get unit ls info", KR(ret), K(unit_id));
      } else if (OB_ISNULL(unit_stat) || !unit_stat->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit stat is null", KR(ret), K(jdx), K(unit_id), K(unit_stat));
      } else if (OB_ISNULL(unit_stat->zone_stat_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("zone stat is null", KR(ret), KPC(unit_stat));
      } else if (unit_stat->zone_stat_->get_valid_unit_num() == zone_stat.get_valid_unit_num()) {
        //走继承的逻辑，继承这个zone的这个unit_group_id
        if (-1 == target_unit_group) {
          target_unit_group = unit_stat->unit_->unit_group_id_;
        }
        if (target_unit_group != unit_stat->unit_->unit_group_id_) {
          target_unit_group = -1;
          break;
        }
      }
    }//end for

    if (OB_FAIL(ret)) {
    } else if (-1 == target_unit_group) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("can not find balance and homo zone", KR(ret), K(zone_stat), K(ls_group));
    } else if (OB_FAIL(zone_stat.get_unit_info_by_ug_id(target_unit_group, unit))) {
      LOG_WARN("failed to find unit by ug id", KR(ret), K(target_unit_group), K(zone_stat));
    }
  }
  return ret;
}

int ObUnitListBalance::construct_lg_unit_group_ids_(ObHeteroUGArray &hetero_ug, ObLSGroupStat &ls_group)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(hetero_ug.count() == 0 || hetero_ug.count() > 2 || !ls_group.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "count", hetero_ug.count(), K(ls_group));
  } else {
    ObUnitIDList &unit_list = ls_group.current_unit_list_;
    ARRAY_FOREACH(unit_list, idx) {
      ObUnitLSStat *unit_stat = NULL;
      uint64_t unit_id = unit_list.at(idx).id();
      if (OB_FAIL(tenant_info_->get_unit_ls_info(unit_id, unit_stat))) {
        LOG_WARN("failed to get unit ls info", KR(ret), K(unit_id));
      } else if (OB_ISNULL(unit_stat) || !unit_stat->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit stat is null", KR(ret), K(idx), K(unit_id), K(unit_stat));
      } else if (!has_exist_in_array(ls_group.ug_ids_, unit_stat->unit_->unit_group_id_)) {
        if (OB_FAIL(ls_group.ug_ids_.push_back(unit_stat->unit_->unit_group_id_))) {
          LOG_WARN("failed to push back", KR(ret), K(unit_stat));
        }
      }
    }//end for get current unit group
    if (OB_SUCC(ret)) {
      //检查每个homo的unit_group都有unit_group_id存在
      ARRAY_FOREACH(hetero_ug, idx) {
        ObUGArray &ug_array = hetero_ug.at(idx);
        bool found = false;
        ARRAY_FOREACH(ug_array, jdx) {
          ObUnitGroupStat *ug = ug_array.at(jdx);
          if (OB_ISNULL(ug)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unit stat is null", KR(ret), K(jdx));
          } else if (has_exist_in_array(ls_group.ug_ids_, ug->ug_id_)) {
            found = true;
            break;
          }
        }//end for check unit group
        if (OB_SUCC(ret) && !found) {
          ObUnitGroupStat* ug_info = NULL;
          if (OB_FAIL(tenant_info_->get_min_unit_group(ug_array, ug_info))) {
            LOG_WARN("failed to get min unit list", KR(ret));
          } else if (OB_FAIL(ls_group.ug_ids_.push_back(ug_info->ug_id_))) {
            LOG_WARN("failed to push back", KR(ret), "ug_id", ug_info->ug_id_);
          } else if (OB_FAIL(ug_info->ls_group_info_.push_back(&ls_group))) {
            LOG_WARN("failed to push back", KR(ret), K(idx));
          }
        }
      }//end for idx
    }
  }
  return ret;
}

int ObUnitListBalance::balance_ls_unit_group_(ObUGArray &ug_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(0 == ug_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "ug_count", ug_array.count());
  } else {
    bool is_balance = false;
    do {
      ObUnitGroupStat* min_ug = NULL;
      ObUnitGroupStat* max_ug = NULL;
      if (OB_FAIL(tenant_info_->get_min_unit_group(ug_array, min_ug))) {
        LOG_WARN("failed to get min unit group", KR(ret));
      } else if (OB_FAIL(tenant_info_->get_max_unit_group(ug_array, max_ug))) {
        LOG_WARN("failed to get max unit group", KR(ret));
      } else if (OB_ISNULL(min_ug) || OB_ISNULL(max_ug)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("min or max ug is null", KR(ret), KP(min_ug), KP(max_ug));
      } else if (max_ug->get_ls_group_count() - min_ug->get_ls_group_count() <= 1) {
        is_balance = true;
      } else {
        //随意挑出一个ls_group挪到min_ug中
        int64_t index = max_ug->get_ls_group_count() - 1;
        ObLSGroupStat* ls_group = max_ug->ls_group_info_.at(index);
        int64_t ls_group_index = -1;
        if (OB_ISNULL(ls_group)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls group is null", KR(ret), K(index));
          //移除unit_group_id中ls_group的定义
        } else if (OB_FAIL(max_ug->ls_group_info_.remove(index))) {
          LOG_WARN("failed to remove index", KR(ret), K(index));
        } else if (OB_FAIL(min_ug->ls_group_info_.push_back(ls_group))) {
          LOG_WARN("failed to push back", KR(ret), KPC(ls_group));
          //移除ls_group中unit_group_id的定义
        } else if (!has_exist_in_array(ls_group->ug_ids_, max_ug->ug_id_, &ls_group_index)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls group has no target ug", KR(ret), KPC(ls_group), KPC(max_ug));
        } else if (OB_FAIL(ls_group->ug_ids_.remove(ls_group_index))) {
          LOG_WARN("failed to remove", KR(ret), KPC(ls_group), KPC(max_ug));
        } else if (OB_FAIL(ls_group->ug_ids_.push_back(min_ug->ug_id_))) {
          LOG_WARN("failed to push back", KR(ret), KPC(ls_group));
        }
      }
    } while (OB_SUCC(ret) && !is_balance);
  }
  return ret;
}

int ObUnitListBalance::construct_lg_unit_list_(ObLSGroupStat &ls_group)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!ls_group.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_group));
  } else if (ls_group.ug_ids_.count() > 0) {
    //根据unit_group_id获取所有的unit，查看这个zone在不在locality中
    ls_group.target_unit_list_.reset();
    ARRAY_FOREACH(ls_group.ug_ids_, idx) {
      const uint64_t unit_group_id = ls_group.ug_ids_.at(idx);
      ObUnitGroupStat* ug_info = NULL;
      ObUnitIDList tmp_unit_list;
      if (OB_FAIL(tenant_info_->get_unit_group_info(unit_group_id, ug_info))) {
        LOG_WARN("failed to get unit group info", KR(ret), K(unit_group_id));
      } else if (OB_ISNULL(ug_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit group info is empty", KR(ret), K(unit_group_id));
      } else if (OB_FAIL(tenant_info_->get_ug_locality_unit_list(*ug_info, tmp_unit_list))) {
        LOG_WARN("failed to get ug locality unit list", KR(ret), KPC(ug_info));
      } else if (OB_FAIL(append_array_no_dup(ls_group.target_unit_list_, tmp_unit_list))) {
        LOG_WARN("failed to append", KR(ret), K(tmp_unit_list), "unit_list", ls_group.target_unit_list_);
      } else {
        LOG_INFO("modify ls group unit list", K(ls_group), K(idx), K(unit_group_id));
      }
    }
  } else {
    //在enable_rebalance关闭时，租户也需要处理locality变更，格局unit_list生成新的unit_list
  }
  return ret;
}
////////////end ObUnitListBalance

//////////////start ObLSCountBalance
int ObLSCountBalance::balance(const bool only_job_strategy)
{
  int ret = OB_SUCCESS;
  BalanceOp op = NO_OP;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (!tenant_info_->is_primary()) {
    if (REACH_TENANT_TIME_INTERVAL(100 * 1000 * 1000)) {
      LOG_INFO("not primary tenant, no need balance");
    }
  } else if (OB_FAIL(get_op_and_generate_job_(op, only_job_strategy))) {
    LOG_WARN("fail to execute get_op_and_generate_job_", KR(ret));
  } else if (NO_OP == op || only_job_strategy) {
    // do nothing
  } else if (OB_UNLIKELY(TRANSFER != op)) {
    //禁掉了match ls count的逻辑，后续如果做日志流在日志流组间均衡可以加回去
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op", KR(ret), K(op));
  } else if (TRANSFER == op) {
    ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
    const int64_t ls_cnt = tenant_info_->job_desc_.get_ls_cnt_in_group();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", KR(ret), K(schema_service));
    } else if (OB_FAIL(tenant_ls_bg_info_.init(tenant_info_->tenant_id_, ls_cnt))) {
      LOG_WARN("init tenant LS balance group info fail", KR(ret), K(tenant_info_->tenant_id_), K(ls_cnt));
    } else if (OB_FAIL(tenant_ls_bg_info_.build("LS_COUNT_BALANCE", *sql_proxy_, *schema_service))) {
      LOG_WARN("build tenant all balance group info for all LS fail", KR(ret));
    } else {
      const int64_t target_ls_cnt = tenant_info_->job_desc_.get_ls_cnt_in_group();
      for (int64_t i = 0; i < tenant_info_->ls_group_array_.size() && OB_SUCC(ret); ++i) {
        ObLSGroupStat &lg_stat = tenant_info_->ls_group_array_[i];
        const int64_t ls_cnt = lg_stat.ls_count_in_group();
        if (ls_cnt < target_ls_cnt && OB_FAIL(generate_expand_task_(lg_stat))) {
          LOG_WARN("fail to generate expand task", KR(ret), K(lg_stat), K(target_ls_cnt), K(lg_stat));
        } else if (ls_cnt > target_ls_cnt && OB_FAIL(generate_shrink_task_(lg_stat))) {
          LOG_WARN("fail to generate shrink task", KR(ret), K(ls_cnt), K(target_ls_cnt), K(lg_stat));
        } else if (ls_cnt == target_ls_cnt) {
          LOG_INFO("no need ls count balance", KR(ret), K(ls_cnt), K(target_ls_cnt), K(lg_stat));
        }
      }
    }
  }
  return ret;
}

//TODO 暂时先注释掉enable_transfer关闭时允许创建空日志流的逻辑
int ObLSCountBalance::get_op_and_generate_job_(BalanceOp &op, const bool only_job_strategy)
{
  int ret = OB_SUCCESS;
  op = NO_OP;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("inner stat error", KR(ret));
  } else if (!tenant_info_->job_desc_.get_enable_rebalance()
             || !tenant_info_->job_desc_.get_enable_transfer()) {
    // no need to do every thing
  } else {
    int64_t target_ls_cnt = tenant_info_->job_desc_.get_ls_cnt_in_group();
    int64_t ls_group_arr_size = tenant_info_->ls_group_array_.size();

    for (int64_t i = 0; i < ls_group_arr_size && NO_OP == op; ++i) {
      const int64_t ls_cnt = tenant_info_->ls_group_array_[i].ls_count_in_group();
      if (ls_cnt != target_ls_cnt) {
        op = TRANSFER;
      }
    }
    if (NO_OP != op) {
      share::ObBalanceStrategy balance_strategy;
      balance_strategy = ObBalanceStrategy::LB_LS_COUNT;
      if (OB_FAIL(generate_balance_job(balance_strategy, only_job_strategy))) {
        LOG_WARN("failed to generate balance job", KR(ret), K(balance_strategy));
      }
    }
  }
  return ret;
}

int ObLSCountBalance::generate_expand_task_(ObLSGroupStat &lg_stat)
{
  int ret = OB_SUCCESS;
  ObSplitLSParamArray src_ls;
  ObArray<ObSplitLSParamArray> dest_ls;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!lg_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(lg_stat));
  } else if (OB_FAIL(construct_src_split_param_array(lg_stat, src_ls))) {
    LOG_WARN("failed to construct src split param", KR(ret), K(lg_stat));
  }
  if (OB_SUCC(ret)) {
    const int64_t lack_ls_cnt = tenant_info_->job_desc_.get_ls_cnt_in_group() - lg_stat.ls_count_in_group();
    if (OB_FAIL(construct_expand_dest_param(lack_ls_cnt, src_ls, dest_ls))) {
      LOG_WARN("failed to construct expand dest param", KR(ret), K(lack_ls_cnt), K(src_ls));
    } else if (OB_UNLIKELY(dest_ls.count() != lack_ls_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lack_ls_cnt should be equal to the size of dest_ls", KR(ret), K(lack_ls_cnt), K(dest_ls), K(src_ls));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < lack_ls_cnt; ++j) {
      if (OB_FAIL(generate_ls_split_task_(dest_ls.at(j)))) {
        LOG_WARN("failed to generate ls info", KR(ret), "dest_ls_param", dest_ls.at(j));
      }
    }//end for j
  }
  return ret;
}
int ObLSCountBalance::generate_shrink_task_(ObLSGroupStat &lg_stat)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!lg_stat.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(lg_stat));
  } else {
    ObSplitLSParamArray src_ls;
    ObArray<ObSplitLSParamArray> dest_ls;
    const int64_t target_ls_cnt = tenant_info_->job_desc_.get_ls_cnt_in_group();
    int64_t start_pos = -1, pos = 0;
    const double src_factor = 1;
    ObLSStatusInfo *status_info = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < lg_stat.ls_count_in_group(); ++i) {
      if (OB_FAIL(tenant_info_->get_next_ls_status_info(start_pos, lg_stat, pos, status_info))) {
        LOG_WARN("fail to get next ls status info", KR(ret), K(start_pos), K(lg_stat));
      } else if (OB_ISNULL(status_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("status_info is null", KR(ret), KP(status_info), K(start_pos), K(pos), K(lg_stat));
      } else if (i < target_ls_cnt) { // redundant ls: i = target_ls_cnt ... end
        // skip
      } else {
        ObSplitLSParam param(status_info, src_factor);
        if (OB_FAIL(src_ls.push_back(param))) {
          LOG_WARN("failed to push back param", KR(ret), K(param), K(i));
        }
      }
      start_pos = pos;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(construct_shrink_src_param(target_ls_cnt, src_ls, dest_ls))) {
        LOG_WARN("failed to construct shrink src param", KR(ret), K(target_ls_cnt), K(src_ls));
      } else if (OB_UNLIKELY(dest_ls.count() != target_ls_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("target_ls_cnt should be equal to the size of dest_ls", KR(ret), K(target_ls_cnt), K(dest_ls), K(src_ls));
      }
      start_pos = -1;
      pos = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < target_ls_cnt; ++i) {
        if (OB_FAIL(tenant_info_->get_next_ls_status_info(start_pos, lg_stat, pos, status_info))) {
          LOG_WARN("fail to get next ls status info", KR(ret), K(start_pos), K(lg_stat));
        } else if (OB_ISNULL(status_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("status_info is null", KR(ret), KP(status_info), K(start_pos), K(pos), K(lg_stat));
        } else if (OB_FAIL(generate_shrink_task_for_each_ls_(dest_ls.at(i), *status_info))) {
          LOG_WARN("fail to execute generate_task_for_shrink", KR(ret), K(i), K(lg_stat),
              "dest_ls_param", dest_ls.at(i), KPC(status_info));
        }
        start_pos = pos;
      }
    }
  }
  return ret;
}

int ObLSCountBalance::generate_shrink_task_for_each_ls_(
    const ObSplitLSParamArray &src_split_param,
    const ObLSStatusInfo &ls_status_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("not init", KR(ret), KP(sql_proxy_));
  } else if (OB_UNLIKELY(!job_->is_valid() || src_split_param.count() <= 0 || !ls_status_info.is_valid())) {
    ret = OB_ERR_UNDEFINED;
    LOG_WARN("error unexpected", KR(ret), K(job_), K(src_split_param), K(ls_status_info));
  } else {
    uint64_t tenant_id = tenant_info_->tenant_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < src_split_param.count(); ++i) {
      const ObSplitLSParam &param = src_split_param.at(i);
      if (fabs(param.get_current_factor() - 1.0) < OB_DOUBLE_EPSINON) {
        // need merge
        const share::ObLSID &src_ls_id = param.get_ls_info()->ls_id_;
        const share::ObLSID &dest_ls_id = ls_status_info.ls_id_;
        const uint64_t ls_group_id = ls_status_info.ls_group_id_;
        if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_merge_task(
            tenant_id,
            job_->get_job_id(),
            ls_group_id,
            src_ls_id,
            dest_ls_id,
            job_->get_balance_strategy(),
            *task_array_))) {
          LOG_WARN("add ls merge task failed", KR(ret), K(tenant_id), K(job_),
              K(ls_group_id), K(dest_ls_id), K(src_ls_id));
        }
      } else if (OB_FAIL(generate_transfer_task_(param, ls_status_info))) {
        LOG_WARN("failed to generate transfer task", KR(ret), K(param), K(ls_status_info));
      }
    }//end for
  }
  return ret;
}

int ObLSCountBalance::generate_ls_split_task_(const ObSplitLSParamArray &dest_split_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check check_inner_stat", KR(ret));
  } else if (OB_UNLIKELY(!job_->is_valid() || dest_split_param.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(job_), K(dest_split_param));
  } else {
    uint64_t tenant_id = tenant_info_->tenant_id_;
    const share::ObBalanceJobID job_id = job_->get_job_id();
    const share::ObBalanceStrategy balance_strategy = job_->get_balance_strategy();
    ObTransferPartList part_list;
    ObLSID dest_ls_id;
    if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_id(GCTX.sql_proxy_, tenant_id, dest_ls_id))) {
      LOG_WARN("failed to fetch new ls id", KR(ret), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_split_param.count(); ++i) {
      const share::ObLSStatusInfo *src_ls = dest_split_param.at(i).get_ls_info();
      if (OB_ISNULL(src_ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("src ls is null", KR(ret), K(i), K(dest_split_param));
      } else if (OB_FAIL(construct_ls_part_info(dest_split_param.at(i), dest_ls_id, part_list))) {
        LOG_WARN("failed to construct ls part info", KR(ret), KPC(src_ls));
      } else if (0 == i && OB_FAIL(ObLSBalanceTaskHelper::add_ls_split_task(
          GCTX.sql_proxy_,
          tenant_id,
          job_id,
          src_ls->ls_group_id_,
          src_ls->ls_id_,
          part_list,
          balance_strategy,
          dest_ls_id,
          *task_array_))) {
          LOG_WARN("add ls split task failed", KR(ret), K(tenant_id), K(job_id), K(balance_strategy), KPC(src_ls), K(dest_ls_id), K(part_list));
      } else if (0 != i && OB_FAIL(ObLSBalanceTaskHelper::add_ls_transfer_task(
          tenant_id,
          job_id,
          src_ls->ls_group_id_,
          src_ls->ls_id_,
          dest_ls_id,
          part_list,
          balance_strategy,
          *task_array_))) {
        LOG_WARN("add ls transfer task failed", KR(ret), K(tenant_id), K(job_id), KPC(src_ls), K(dest_ls_id), K(part_list));
      }
    }
  }
  return ret;
}
int ObLSCountBalance::generate_transfer_task_(const ObSplitLSParam &param, const ObLSStatusInfo &ls_status_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check check_inner_stat", KR(ret));
  } else if (OB_UNLIKELY(!job_->is_valid() || !param.is_valid() || !ls_status_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(job_), K(param), K(!param.is_valid() || !ls_status_info.is_valid()));
  } else {
    uint64_t tenant_id = tenant_info_->tenant_id_;
    const share::ObBalanceJobID job_id = job_->get_job_id();
    const share::ObBalanceStrategy balance_strategy = job_->get_balance_strategy();
    ObTransferPartList part_list;
    if (OB_FAIL(construct_ls_part_info(param, ls_status_info.ls_id_, part_list))) {
      LOG_WARN("failed to construct ls part info", KR(ret), K(param), K(ls_status_info));
    } else if (OB_FAIL(ObLSBalanceTaskHelper::add_ls_transfer_task(
        tenant_id,
        job_id,
        ls_status_info.ls_group_id_,
        param.get_ls_info()->ls_id_,
        ls_status_info.ls_id_,
        part_list,
        balance_strategy,
        *task_array_))) {
      LOG_WARN("add ls transfer task failed", KR(ret), K(tenant_id), K(job_id), K(balance_strategy), K(part_list));
    }
  }
  return ret;
}

//////////////end ObLSCountBalance

//////////////ObLSBalanceTaskHelper

ObLSBalanceTaskHelper::ObLSBalanceTaskHelper(ObIAllocator &allocator) :
    inited_(false),
    sql_proxy_(NULL),
    job_(),
    task_array_(),
    lg_op_array_(),
    tenant_info_(allocator)
{
}

int ObLSBalanceTaskHelper::init(const uint64_t tenant_id,
           const share::ObLSStatusInfoArray &status_array,
           const ObBalanceJobDesc &job_desc, const ObArray<ObUnit> &unit_array,
           const ObTenantRole &tenant_role,
           ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == status_array.count() || !job_desc.is_valid()
                  || 0 >= unit_array.count() || OB_INVALID_TENANT_ID == tenant_id
                  || OB_ISNULL(sql_proxy))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(status_array), K(job_desc),
                                 K(unit_array), K(tenant_id), KP(sql_proxy));
  } else if (OB_FAIL(tenant_info_.init_tenant_ls_balance_info(tenant_id, status_array,
            job_desc, unit_array, tenant_role))) {
    LOG_WARN("failed to init tenant info", KR(ret), K(tenant_id), K(status_array),
        K(job_desc), K(tenant_role));
  } else {
    sql_proxy_ = sql_proxy;
    inited_ = true;
  }

  return ret;
}

int ObLSBalanceTaskHelper::check_need_ls_balance(bool &need_balance)
{
  int ret = OB_SUCCESS;
  need_balance = false;
  bool only_job_strategy = true;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(generate_ls_balance_task(only_job_strategy))) {
    LOG_WARN("failed to generate ls balance task", KR(ret));
  } else {
    need_balance = job_.is_valid();
  }
  return ret;
}


int ObLSBalanceTaskHelper::generate_ls_balance_task(
    const bool only_job_strategy,
    const ObBalanceJobID &specified_job_id/*= INVALID_ID*/)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy or schema service is null", KR(ret), K(sql_proxy_));
  } else {
    //TODO
    job_.reset();
    //1. process duplicate ls
    if (OB_SUCC(ret)) {
      ObDupLSBalance dup_ls(&tenant_info_, sql_proxy_, specified_job_id, &job_, &task_array_, &lg_op_array_, &unit_ug_op_array_);
      if (OB_FAIL(dup_ls.balance(only_job_strategy))) {
        LOG_WARN("failed to balance dupcate ls", KR(ret));
      }
    }
    //2. GTS standalone and unit group reorganize
    if (OB_SUCC(ret) && !job_.is_valid()) {
      ObUnitGroupBalance unit_group_balance(&tenant_info_, sql_proxy_, specified_job_id, &job_, &task_array_, &lg_op_array_, &unit_ug_op_array_);
      if (OB_FAIL(unit_group_balance.balance(only_job_strategy))) {
        LOG_WARN("failed to balance unit group", KR(ret));
      }
    }
    //3. check unit list match unit_groups
    if (OB_SUCC(ret) && !job_.is_valid()) {
      ObLSGroupLocationBalance lg_location_balance(&tenant_info_, sql_proxy_, specified_job_id, &job_, &task_array_, &lg_op_array_, &unit_ug_op_array_);
      if (OB_FAIL(lg_location_balance.balance(only_job_strategy))) {
        LOG_WARN("failed to balance ls group location", KR(ret));
      }
    }

    //4. ls group count balance
    if (OB_SUCC(ret) && !job_.is_valid()) {
      ObLSGroupCountBalance lg_cnt_balance(&tenant_info_, sql_proxy_, specified_job_id, &job_, &task_array_, &lg_op_array_, &unit_ug_op_array_);
      if (OB_FAIL(lg_cnt_balance.balance(only_job_strategy))) {
        LOG_WARN("failed to do ls group count balance", KR(ret));
      }
    }

    //5. ls group location balance
    // Do lsg location balance before ls-count balance to avoid creating too many LS on a few units.
    if (OB_SUCC(ret) && !job_.is_valid()) {
      ObUnitListBalance unit_list_balance(&tenant_info_, sql_proxy_, specified_job_id, &job_, &task_array_, &lg_op_array_, &unit_ug_op_array_);
      if (OB_FAIL(unit_list_balance.balance(only_job_strategy))) {
        LOG_WARN("failed to unit list balance", KR(ret));
      }
    }

    //6. ls count balance within ls group
    if (OB_SUCC(ret) && !job_.is_valid()) {
      ObLSCountBalance ls_count_balance(&tenant_info_, sql_proxy_, specified_job_id, &job_, &task_array_, &lg_op_array_, &unit_ug_op_array_);
      if (OB_FAIL(ls_count_balance.balance(only_job_strategy))) {
        LOG_WARN("failed to do ls count balance", KR(ret));
      }
    }
  }
  return ret;
}


#define GEN_BALANCE_TASK(task_type, ls_group_id, src_ls, dest_ls, part_list, balance_strategy) \
  do {                                                                                         \
    if (OB_SUCC(ret)) {                                                                        \
      ObBalanceTask task;                                                                      \
      ObBalanceTaskID task_id;                                                                 \
      if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id, task_id))) {                       \
        LOG_WARN("gen_unique_id", KR(ret), K(tenant_id));                                      \
      } else if (OB_FAIL(task.simple_init(tenant_id, balance_job_id, task_id,                  \
          task_type, ls_group_id, src_ls, dest_ls, part_list, balance_strategy))) {            \
        LOG_WARN("init task fail", KR(ret), K(tenant_id), K(balance_job_id), K(task_id),       \
            K(ls_group_id), K(src_ls), K(dest_ls), K(part_list), K(balance_strategy));         \
      } else if (OB_FAIL(task_array.push_back(task))) {                                        \
        LOG_WARN("push_back fail", KR(ret), K(task));                                          \
      } else {                                                                                 \
        LOG_INFO("gen balance task successfully", K(task));                                    \
      }                                                                                        \
    }                                                                                          \
  } while (0)

int ObLSBalanceTaskHelper::add_ls_alter_task(
    const uint64_t tenant_id,
    const share::ObBalanceJobID &balance_job_id,
    const uint64_t ls_group_id,
    const share::ObLSID &src_ls_id,
    const share::ObBalanceStrategy &balance_strategy,
    common::ObIArray<share::ObBalanceTask> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !balance_job_id.is_valid()
      || OB_INVALID_ID == ls_group_id
      || !src_ls_id.is_valid()
      || !balance_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(balance_job_id),
        K(ls_group_id), K(src_ls_id), K(balance_strategy));
  } else {
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_ALTER);
    ObTransferPartList empty_part_list;
    ObLSID dest_ls_id; // -1
    GEN_BALANCE_TASK(task_type, ls_group_id, src_ls_id, dest_ls_id, empty_part_list, balance_strategy);
  }
  return ret;
}

int ObLSBalanceTaskHelper::add_ls_transfer_task(
    const uint64_t tenant_id,
    const share::ObBalanceJobID &balance_job_id,
    const uint64_t ls_group_id,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    const share::ObTransferPartList &part_list,
    const share::ObBalanceStrategy &balance_strategy,
    common::ObIArray<share::ObBalanceTask> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !balance_job_id.is_valid()
      || OB_INVALID_ID == ls_group_id
      || !src_ls_id.is_valid()
      || !dest_ls_id.is_valid()
      || !balance_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(balance_job_id),
        K(ls_group_id), K(src_ls_id), K(dest_ls_id), K(part_list), K(balance_strategy));
  } else {
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_TRANSFER);
    GEN_BALANCE_TASK(task_type, ls_group_id, src_ls_id, dest_ls_id, part_list, balance_strategy);
  }
  return ret;
}

int ObLSBalanceTaskHelper::add_ls_split_task(
    ObMySQLProxy *sql_proxy,
    const uint64_t tenant_id,
    const share::ObBalanceJobID &balance_job_id,
    const uint64_t ls_group_id,
    const share::ObLSID &src_ls_id,
    const share::ObTransferPartList &part_list,
    const share::ObBalanceStrategy &balance_strategy,
    share::ObLSID &new_ls_id,
    common::ObIArray<share::ObBalanceTask> &task_array)
{
  int ret = OB_SUCCESS;
  // part_list may be empty when split a empty LS
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !balance_job_id.is_valid()
      || OB_INVALID_ID == ls_group_id
      || !src_ls_id.is_valid()
      || OB_ISNULL(sql_proxy)
      || !balance_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(balance_job_id), K(new_ls_id),
        K(ls_group_id), K(src_ls_id), K(part_list), KP(sql_proxy), K(balance_strategy));
  } else {
    if (!new_ls_id.is_valid()
        && OB_FAIL(ObLSServiceHelper::fetch_new_ls_id(GCTX.sql_proxy_, tenant_id, new_ls_id))) {
      LOG_WARN("failed to fetch new ls id", KR(ret), K(tenant_id));
    } else {
      ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_SPLIT);
      GEN_BALANCE_TASK(task_type, ls_group_id, src_ls_id, new_ls_id, part_list, balance_strategy);
    }
  }
  return ret;
}

int ObLSBalanceTaskHelper::add_ls_merge_task(
    const uint64_t tenant_id,
    const share::ObBalanceJobID &balance_job_id,
    const uint64_t ls_group_id,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    const share::ObBalanceStrategy &balance_strategy,
    common::ObIArray<share::ObBalanceTask> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !balance_job_id.is_valid()
      || OB_INVALID_ID == ls_group_id
      || !src_ls_id.is_valid()
      || !dest_ls_id.is_valid()
      || !balance_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(balance_job_id),
        K(ls_group_id), K(src_ls_id), K(dest_ls_id), K(balance_strategy));
  } else {
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_MERGE);
    ObTransferPartList empty_part_list;
    GEN_BALANCE_TASK(task_type, ls_group_id, src_ls_id, dest_ls_id, empty_part_list, balance_strategy);
  }
  return ret;
}

int ObLSBalanceTaskHelper::add_create_ls_task(
      const uint64_t tenant_id,
      const share::ObBalanceJobID &balance_job_id,
      const uint64_t ls_group_id,
      const share::ObBalanceStrategy &balance_strategy,
      ObMySQLProxy *sql_proxy,
      common::ObIArray<share::ObBalanceTask> &task_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy) || OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !balance_job_id.is_valid()
      || OB_INVALID_ID == ls_group_id
      || !balance_strategy.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(balance_job_id),
        K(ls_group_id), K(balance_strategy), KP(sql_proxy));
  } else {
    ObBalanceTaskType task_type(ObBalanceTaskType::BALANCE_TASK_CREATE);
    ObTransferPartList empty_part_list;
    ObLSID ls_id;
    if (OB_FAIL(ObLSServiceHelper::fetch_new_ls_id(sql_proxy, tenant_id, ls_id))) {
      LOG_WARN("failed to fetch new ls id", KR(ret), K(tenant_id));
    } else {
      GEN_BALANCE_TASK(task_type, ls_group_id, ls_id, ls_id, empty_part_list, balance_strategy);
    }
  }
  return ret;

}

// if ls_group_id of both src_ls and dest_ls are 0, choose other valid ls_group_id
int ObLSBalanceTaskHelper::choose_ls_group_id_for_transfer_between_dup_ls(
    const uint64_t src_ls_group_id,
    const uint64_t dest_ls_group_id,
    const uint64_t other_ls_group_id,
    uint64_t &chosen_ls_group_id)
{
  int ret = OB_SUCCESS;
  chosen_ls_group_id = OB_INVALID_ID;
  if (OB_UNLIKELY(OB_INVALID_ID == src_ls_group_id
      || OB_INVALID_ID == dest_ls_group_id
      || OB_INVALID_ID == other_ls_group_id
      || 0 == other_ls_group_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src_ls_group_id), K(dest_ls_group_id), K(other_ls_group_id));
  } else if (0 != dest_ls_group_id) {
    chosen_ls_group_id = dest_ls_group_id;
  } else if (0 != src_ls_group_id) {
    chosen_ls_group_id = src_ls_group_id;
  } else { // ls_group_id of both src_ls and dest_ls are 0, use a valid ls_group_id
    chosen_ls_group_id = other_ls_group_id;
  }
  LOG_INFO("choose ls_group_id for transfer between dup ls finshed", KR(ret),
      K(chosen_ls_group_id), K(src_ls_group_id), K(dest_ls_group_id), K(other_ls_group_id));
  return ret;
}

int ObLSBalanceTaskHelper::execute_job_without_task()
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::current_time();
  int64_t duration = 0;
  FLOG_INFO("begin to execute job without task", K(job_), K(start_time));
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!job_.is_valid()
      || job_.get_balance_strategy().has_balance_task()
      || (lg_op_array_.empty() && unit_ug_op_array_.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no need execute job without task", KR(ret), KPC(this));
  } else {
    //1. 对balance_job表加锁
    //2. 修改__all_ls_status表或者__all_unit表
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(update_ls_group_unit_list_in_table_())) {
      LOG_WARN("failed to update ls unit list in table", KR(ret), KPC(this));
    } else if (OB_FAIL(update_unit_ug_id_in_table_())) {
      LOG_WARN("failed to update unit ug id in table", KR(ret), KPC(this));
    }
    duration = ObTimeUtility::current_time() - start_time;
  }
  FLOG_INFO("finish to execute job without task", KR(ret), K(start_time), K(duration));
  return ret;
}

int ObLSBalanceTaskHelper::lock_balance_job_(
    const ObBalanceJob &balance_job,
    ObMySQLTransaction &job_trans)
{
  int ret = OB_SUCCESS;
  ObBalanceJob balance_job_in_table;
  int64_t start_time = 0, finish_time = 0;//no used
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(GCTX.sql_proxy_)
             || OB_UNLIKELY(!balance_job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.sql_proxy_), K(balance_job));
  } else if (tenant_info_.is_primary()) {
    if (OB_FAIL(job_trans.start(GCTX.sql_proxy_, balance_job.get_tenant_id()))) {
      LOG_WARN("failed to start job trans", KR(ret), K(balance_job));
    } else if (OB_FAIL(ObBalanceJobTableOperator::get_balance_job(
                           balance_job.get_tenant_id(), true/*for_update*/, job_trans,
                           balance_job_in_table, start_time, finish_time))) {
      LOG_WARN("failed to get balance job", KR(ret), K(balance_job));
    } else if (OB_UNLIKELY(balance_job.get_job_id() != balance_job_in_table.get_job_id())
               || OB_UNLIKELY(balance_job.get_balance_strategy() != balance_job_in_table.get_balance_strategy())) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("unmatched balance job", KR(ret), K(balance_job), K(balance_job_in_table));
    }
  }
  FLOG_INFO("finish lock line for balance job", KR(ret), K(balance_job), K_(tenant_info));
  return ret;
}

int ObLSBalanceTaskHelper::update_ls_group_unit_list_in_table_()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans_for_balance_job;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (lg_op_array_.empty()) {
    // No operations needed, skip update
    LOG_DEBUG("lg_op_array is empty", KR(ret), K(lg_op_array_));
  } else if (OB_FAIL(lock_balance_job_(job_, trans_for_balance_job))) {
    LOG_WARN("fail to lock balance job", KR(ret), K(job_));
  } else {
    const uint64_t tenant_id = job_.get_tenant_id();
    const uint64_t exec_tenant_id = ObLSLifeIAgent::get_exec_tenant_id(tenant_id);
    START_TRANSACTION(GCTX.sql_proxy_, exec_tenant_id);
    ObLSStatusOperator ls_op;
    ARRAY_FOREACH(lg_op_array_, idx) {
      const ObLSGroupUnitListOp &op = lg_op_array_.at(idx);
      if (op.get_ls_id().is_sys_ls()) {
        if (OB_FAIL(ls_op.alter_ls_group_id(
            tenant_id, op.get_ls_id(), op.get_ls_group_id(), op.get_ls_group_id(),
            op.get_current_unit_list(), op.get_target_unit_list(), trans))) {
          LOG_WARN("failed to alter sys ls unit list", KR(ret), K(tenant_id), K(op));
        }
      } else if (OB_FAIL(ls_op.alter_ls_group_unit_list(
          tenant_id,
          op.get_ls_group_id(),
          op.get_ls_count(),
          op.get_current_unit_list(),
          op.get_target_unit_list(),
          trans))) {
        LOG_WARN("failed to alter ls unit list", KR(ret), K(tenant_id), K(op));
      }
    }
    END_TRANSACTION(trans);
    LOG_INFO("update ls group unit list in table finished", KR(ret), K(lg_op_array_));
  }
  END_TRANSACTION(trans_for_balance_job);
  LOG_INFO("update ls group unit list in table by balance job finished", KR(ret), K(lg_op_array_));
  return ret;
}

int ObLSBalanceTaskHelper::update_unit_ug_id_in_table_()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans_for_balance_job;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (unit_ug_op_array_.empty()) {
    // No operations needed, skip update
    LOG_DEBUG("unit_ug_op_array is empty", KR(ret), K(unit_ug_op_array_));
  } else if (OB_FAIL(lock_balance_job_(job_, trans_for_balance_job))) {
    LOG_WARN("fail to lock balance job", KR(ret), K(job_));
  } else {
    const uint64_t tenant_id = job_.get_tenant_id();
    ObUnitTableTransaction unit_trans;
    if (OB_FAIL(unit_trans.start(GCTX.sql_proxy_, OB_SYS_TENANT_ID))) {
      LOG_WARN("failed to start unit trans", KR(ret));
    } else {
      // check if __all_unit has updated during doing balance job
      ObUnitTableOperator ut_operator;
      ObArray<ObUnit> tmp_unit_array;
      if (OB_FAIL(ut_operator.init(*GCTX.sql_proxy_))) {
        LOG_WARN("failed to init unit table operator", KR(ret));
      } else if (OB_FAIL(ut_operator.get_units_by_tenant(tenant_id, tmp_unit_array))) {
        LOG_WARN("failed to get units by tenant", KR(ret), K(tenant_id));
      } else if (OB_FAIL(check_unit_array_same(tmp_unit_array, get_unit_array()))) {
        LOG_WARN("check unit_array same failed", KR(ret), K(tmp_unit_array), KPC(this));
      }
      // do update ug_ids
      FOREACH_X(op, unit_ug_op_array_, OB_SUCC(ret)) {
        if (OB_FAIL(ut_operator.update_unit_ug_id(unit_trans, *op))) {
          LOG_WARN("failed to update unit ug id", KR(ret), KPC(op));
        }
      }
    }
    END_TRANSACTION(unit_trans);
    LOG_INFO("update unit ug id in table finished", KR(ret), K(unit_ug_op_array_));
    if (OB_SUCC(ret)) {
      ROOTSERVICE_EVENT_ADD("unit", "reallocate_ug",
          K(tenant_id),
          K_(unit_ug_op_array),
          "balancer_server", GCTX.self_addr(),
          "trace_id", *ObCurTraceId::get_trace_id());
    }
  }
  END_TRANSACTION(trans_for_balance_job);
  LOG_INFO("update unit ug id in table by balance job finished", KR(ret), K(unit_ug_op_array_));
  return ret;
}

int ObLSBalanceTaskHelper::check_unit_array_same(
    const ObIArray<ObUnit> &left_arr,
    const ObIArray<ObUnit> &right_arr)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> sorted_left_arr;
  ObArray<ObUnit> sorted_right_arr;
  if (left_arr.count() != right_arr.count()) {
    ret = OB_NEED_RETRY;
    LOG_WARN("unit array count change, need retry", KR(ret), K(left_arr), K(right_arr));
  } else if (OB_FAIL(sorted_left_arr.assign(left_arr))) {
    LOG_WARN("failed to assign left array", KR(ret));
  } else if (OB_FAIL(sorted_right_arr.assign(right_arr))) {
    LOG_WARN("failed to assign right array", KR(ret));
  } else {
    lib::ob_sort(sorted_left_arr.begin(), sorted_left_arr.end(), ObUnitIdCmp());
    lib::ob_sort(sorted_right_arr.begin(), sorted_right_arr.end(), ObUnitIdCmp());
    ARRAY_FOREACH(sorted_left_arr, idx) {
      if (!sorted_left_arr.at(idx).has_same_unit_group_info(sorted_right_arr.at(idx))) {
        ret = OB_NEED_RETRY;
        LOG_WARN("unit array info change, need retry", KR(ret), K(idx), K(sorted_left_arr), K(sorted_right_arr));
      }
    }
  }
  return ret;
}

#undef ISTAT
#undef WSTAT
#undef GET_FROM_ARRAY

}  // namespace rootserver
}
