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

#include "ob_zone_manager.h"

#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_zone_table_operation.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_zone_info.h"
#include "share/config/ob_server_config.h"
#include "ob_leader_coordinator.h"
#include "ob_rs_event_history_table_operator.h"
#include "share/schema/ob_table_iter.h"
#include "storage/ob_file_system_util.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_freeze_info_manager.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
using namespace observer;

namespace rootserver {

ObZoneManagerBase::ObZoneManagerBase()
    : lock_(ObLatchIds::ZONE_INFO_RW_LOCK),
      inited_(false),
      loaded_(false),
      zone_infos_(),
      zone_count_(0),
      cluster_create_ts_(OB_INVALID_TIMESTAMP),
      global_info_(),
      proxy_(NULL),
      leader_coordinator_(NULL)
{}

ObZoneManagerBase::~ObZoneManagerBase()
{}

void ObZoneManagerBase::reset()
{
  zone_count_ = 0;
  cluster_create_ts_ = OB_INVALID_TIMESTAMP;
  global_info_.reset();
}

int ObZoneManagerBase::init(ObMySQLProxy& proxy, ObILeaderCoordinator& leader_coordinator)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    proxy_ = &proxy;
    leader_coordinator_ = &leader_coordinator;
    inited_ = true;
    loaded_ = false;
  }
  return ret;
}

int ObZoneManagerBase::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (!inited_ || !loaded_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner_stat_error", K_(inited), K_(loaded), K(ret));
  }
  return ret;
}

int ObZoneManagerBase::get_zone_count(int64_t& zone_count) const
{
  int ret = OB_SUCCESS;
  zone_count = 0;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    zone_count = zone_count_;
  }
  return ret;
}

int ObZoneManagerBase::get_zone(const int64_t idx, ObZoneInfo& info) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  info.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(idx), K(ret));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    if (idx >= 0 && idx < zone_count_) {
      info = zone_infos_[idx];
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObZoneManagerBase::get_zone(ObZoneInfo& info) const
{
  return get_zone(info.zone_, info);
}

int ObZoneManagerBase::get_zone(const common::ObZone& zone, ObZoneInfo& info) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(zone), K(ret));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < zone_count_; ++i) {
      if (zone_infos_[i].zone_ == zone) {
        ret = OB_SUCCESS;
        info = zone_infos_[i];
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_all_region(common::ObIArray<common::ObRegion>& region_list) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else {
    region_list.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_count_; ++i) {
      common::ObRegion region;
      if (OB_FAIL(zone_infos_[i].get_region(region))) {
        LOG_WARN("fail to get region", K(ret));
      } else if (has_exist_in_array(region_list, region)) {
        // bypass
      } else if (OB_FAIL(region_list.push_back(region))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_region(const common::ObZone& zone, ObRegion& region) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(zone), K(ret));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < zone_count_; ++i) {
      if (zone_infos_[i].zone_ == zone) {
        if (OB_FAIL(zone_infos_[i].get_region(region))) {
          LOG_WARN("fail get region", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_zone_count(const ObRegion& region, int64_t& count) const
{
  int ret = OB_SUCCESS;
  count = 0;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(region), K(ret));
  } else {
    for (int64_t i = 0; i < zone_count_; ++i) {
      ObRegion zone_region(zone_infos_[i].region_.info_.ptr());
      if (zone_region == region) {
        count++;
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_zone(const ObRegion& region, ObIArray<ObZone>& zone_list) const
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone is empty", K(region), K(ret));
  } else {
    for (int64_t i = 0; i < zone_count_ && OB_SUCC(ret); ++i) {
      ObRegion zone_region(zone_infos_[i].region_.info_.ptr());
      if (zone_region == region) {
        if (OB_FAIL(zone_list.push_back(zone_infos_[i].zone_))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_zone(const share::ObZoneStatus::Status& status, common::ObIArray<ObZone>& zone_list) const
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (status < ObZoneStatus::INACTIVE || status >= ObZoneStatus::UNKNOWN) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", K(status), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_count_; ++i) {
      if (zone_infos_[i].status_ == status) {
        if (OB_FAIL(zone_list.push_back(zone_infos_[i].zone_))) {
          LOG_WARN("failed to add zone list", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_zone(ObIArray<ObZoneInfo>& infos) const
{
  int ret = OB_SUCCESS;
  infos.reset();
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_count_; ++i) {
      if (OB_FAIL(infos.push_back(zone_infos_[i]))) {
        LOG_WARN("failed to add zone info list", K(ret));
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_zone(ObIArray<ObZone>& zone_list) const
{
  int ret = OB_SUCCESS;
  zone_list.reset();
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_count_; ++i) {
      if (OB_FAIL(zone_list.push_back(zone_infos_[i].zone_))) {
        LOG_WARN("push back zone failed", K(ret));
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_active_zone(ObZoneInfo& info) const
{
  int ret = OB_SUCCESS;
  ObMalloc alloc(ObModIds::OB_TEMP_VARIABLES);
  ObPtrGuard<ObZoneInfo> tmp_info_guard(alloc);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid info", K(ret), K(info));
  } else if (OB_FAIL(tmp_info_guard.init())) {
    LOG_WARN("init temporary variable failed", K(ret));
  } else {
    ObZoneInfo& tmp_info = *tmp_info_guard.ptr();
    tmp_info.zone_ = info.zone_;
    ret = get_zone(tmp_info);
    if (OB_FAIL(get_zone(tmp_info))) {
      LOG_WARN("get_zone failed", "zone", tmp_info.zone_, K(ret));
    } else {
      if (ObZoneStatus::ACTIVE == tmp_info.status_) {
        info = tmp_info;
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_snapshot(share::ObGlobalInfo& global_info, common::ObIArray<share::ObZoneInfo>& infos) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  global_info.reset();
  infos.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    global_info = global_info_;
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_count_; ++i) {
      if (OB_FAIL(infos.push_back(zone_infos_[i]))) {
        LOG_WARN("push zone info to array failed", K(ret), "index", i);
      }
    }
  }
  return ret;
}

int64_t ObZoneManagerBase::get_cluster_create_timestamp() const
{
  return ATOMIC_LOAD(&cluster_create_ts_);
}

int ObZoneManagerBase::check_zone_exist(const common::ObZone& zone, bool& zone_exist) const
{
  int ret = OB_SUCCESS;
  zone_exist = false;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    for (int64_t i = 0; i < zone_count_; ++i) {
      if (zone == zone_infos_[i].zone_) {
        zone_exist = true;
        break;
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::check_zone_active(const common::ObZone& zone, bool& zone_active) const
{
  int ret = OB_SUCCESS;
  zone_active = false;
  bool find_zone = false;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    for (int64_t i = 0; i < zone_count_; ++i) {
      if (zone == zone_infos_[i].zone_) {
        find_zone = true;
        if (ObZoneStatus::ACTIVE == zone_infos_[i].status_) {
          zone_active = true;
        }
        break;
      }
    }
  }
  if (OB_SUCC(ret) && !find_zone) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to find zone", KR(ret), K(zone));
  }
  return ret;
}

int ObZoneManagerBase::add_zone(
    const ObZone& zone, const ObRegion& region, const ObIDC& idc, const ObZoneType& zone_type)
{
  int ret = OB_SUCCESS;
  int64_t index = OB_INVALID_INDEX;
  ObRegion my_region = region;
  ObIDC my_idc = idc;
  ObZoneType my_zone_type = zone_type;
  if (my_region.is_empty()) {
    my_region.assign(DEFAULT_REGION_NAME);  // compatible with ob1.2
  }
  if (my_idc.is_empty()) {
    my_idc.assign("");
  }
  if (my_zone_type < ObZoneType::ZONE_TYPE_READWRITE || my_zone_type >= ObZoneType::ZONE_TYPE_INVALID) {
    my_zone_type = ObZoneType::ZONE_TYPE_READWRITE;
  }
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone), K(ret));
  } else if (ObZoneType::ZONE_TYPE_READONLY == my_zone_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cant't set readonly zone since ver2.2", K(ret));
  } else if (my_region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(my_region), K(ret));
  } else if (zone_count_ >= common::MAX_ZONE_NUM) {
    ret = OB_OVER_ZONE_NUM_LIMIT;
    LOG_ERROR("only support MAX_ZONE_NUM zone", K(MAX_ZONE_NUM), K(ret));
  } else if (OB_SUCCESS == (ret = find_zone(zone, index))) {
    ret = OB_ENTRY_EXIST;
    LOG_ERROR("zone is already exist, cannot add", K(zone), K(ret));
  } else if (OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("find_zone failed", K(zone), K(ret));
  } else {
    ret = OB_SUCCESS;
    zone_infos_[zone_count_].reset();
    zone_infos_[zone_count_].zone_ = zone;
    zone_infos_[zone_count_].broadcast_version_.value_ = global_info_.global_broadcast_version_.value_;
    zone_infos_[zone_count_].merge_start_time_.value_ = ObTimeUtility::current_time();
    zone_infos_[zone_count_].last_merged_version_.value_ = global_info_.global_broadcast_version_.value_;
    zone_infos_[zone_count_].all_merged_version_.value_ = global_info_.global_broadcast_version_.value_;
    zone_infos_[zone_count_].last_merged_time_.value_ = ObTimeUtility::current_time();
    zone_infos_[zone_count_].zone_type_.value_ = my_zone_type;
    zone_infos_[zone_count_].storage_type_.value_ = ObZoneInfo::STORAGE_TYPE_LOCAL;
    ObMySQLTransaction trans;
    if (OB_FAIL(zone_infos_[zone_count_].region_.info_.assign(ObString(my_region.size(), my_region.ptr())))) {
      LOG_WARN("fail assign region info", K(ret), K(my_region));
    } else if (OB_FAIL(zone_infos_[zone_count_].idc_.info_.assign(ObString(my_idc.size(), my_idc.ptr())))) {
      LOG_WARN("fail to assign idc info", K(ret), K(my_idc));
    } else if (OB_FAIL(zone_infos_[zone_count_].zone_type_.info_.assign(ObString(zone_type_to_str(my_zone_type))))) {
      LOG_WARN("fail to assign zone_type info", K(ret), K(my_zone_type));
    } else if (OB_FAIL(zone_infos_[zone_count_].zone_type_.info_.assign(
                   ObString(ObZoneInfo::get_storage_type_str(ObZoneInfo::STORAGE_TYPE_LOCAL))))) {
      LOG_WARN("fail to assign zone_type info", K(ret), K(my_zone_type));
    } else if (OB_FAIL(trans.start(proxy_))) {
      LOG_WARN("start transaction failed", K(ret));
    } else {
      if (OB_FAIL(ObZoneTableOperation::insert_zone_info(trans, zone_infos_[zone_count_]))) {
        LOG_WARN("insert transaction failed", "zone_info", zone_infos_[zone_count_], K(ret));
      }
      int tmp_ret = trans.end(OB_SUCC(ret));
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(ret), K(tmp_ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("succeed to add new zone", "zone_info", zone_infos_[zone_count_]);
        ++zone_count_;
        ROOTSERVICE_EVENT_ADD("zone", "add_zone", K(zone));

        ObString zone_merge_order = GCONF.zone_merge_order.str();
        if (!zone_merge_order.empty()) {
          // LOG_USER_WARN("Please check new zone in zone_merge_order. You can show parameters like
          // 'zone_merge_order'");
          LOG_USER_WARN(OB_CHECK_ZONE_MERGE_ORDER);
        }
      }
    }
  }

  return ret;
}

int ObZoneManagerBase::delete_zone(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone), K(ret));
  } else if (OB_FAIL(find_zone(zone, idx))) {
    LOG_WARN("find_zone failed", K(zone), K(ret));
  } else if (OB_INVALID_INDEX == idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected idx", K(idx), K(ret));
  } else if (ObZoneStatus::INACTIVE != zone_infos_[idx].status_) {
    ret = OB_ZONE_STATUS_NOT_MATCH;
    LOG_WARN("zone is not inactive, can't delete it", K(ret));
  } else if (OB_FAIL(ObZoneTableOperation::remove_zone_info(*proxy_, zone))) {
    LOG_WARN("remove_zone_info failed", K(zone), K(ret));
  } else {
    for (int64_t i = idx; i < zone_count_ - 1; ++i) {
      zone_infos_[i] = zone_infos_[i + 1];
    }
    --zone_count_;
    LOG_INFO("succeed to delete zone", K(zone));
    ROOTSERVICE_EVENT_ADD("zone", "delete_zone", K(zone));
  }
  return ret;
}

int ObZoneManagerBase::start_zone(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone), K(ret));
  } else if (OB_FAIL(find_zone(zone, idx))) {
    LOG_WARN("find_zone failed", K(zone), K(ret));
  } else if (OB_INVALID_INDEX == idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected idx", K(idx), K(ret));
  } else if (ObZoneStatus::ACTIVE == zone_infos_[idx].status_) {
    ret = OB_ZONE_STATUS_NOT_MATCH;
    LOG_WARN("zone is already active, no need to start it again", K(zone), K(ret));
  } else if (ObZoneStatus::UNKNOWN == zone_infos_[idx].status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown status", K(ret));
  } else if (OB_FAIL(zone_infos_[idx].status_.update(
                 *proxy_, zone, ObZoneStatus::ACTIVE, ObZoneStatus::get_status_str(ObZoneStatus::ACTIVE)))) {
    LOG_WARN("failed to update zone status", K(zone), "status", ObZoneStatus::ACTIVE, K(ret));
  } else {
    leader_coordinator_->signal();
    LOG_INFO("succeed to start zone", K(zone));
    ROOTSERVICE_EVENT_ADD("zone", "start_zone", K(zone));
  }
  return ret;
}

int ObZoneManagerBase::stop_zone(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone), K(ret));
  } else if (OB_FAIL(find_zone(zone, idx))) {
    LOG_WARN("find_zone failed", K(zone), K(ret));
  } else if (OB_INVALID_INDEX == idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected idx", K(idx), K(ret));
  } else if (ObZoneStatus::INACTIVE == zone_infos_[idx].status_) {
    // make sure we can retry alter system stop zone until succ
    // so, ret is not OB_ZONE_STATUS_NOT_MATCH, but OB_SUCCESS
    ret = OB_SUCCESS;
    LOG_WARN("zone is inactive, no need to update its status", K(zone), K(ret));
  } else if (ObZoneStatus::UNKNOWN == zone_infos_[idx].status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown status", K(ret));
  } else {
    int64_t active_zone_cnt = 0;
    for (int64_t i = 0; i < zone_count_; ++i) {
      if (zone_infos_[i].status_ == ObZoneStatus::ACTIVE) {
        active_zone_cnt++;
      }
    }
    if (active_zone_cnt <= 1) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("try to stop the last active zone not allowed", K(ret), K(zone));
    } else if (OB_FAIL(zone_infos_[idx].status_.update(
                   *proxy_, zone, ObZoneStatus::INACTIVE, ObZoneStatus::get_status_str(ObZoneStatus::INACTIVE)))) {
      LOG_WARN("failed to update zone status", K(zone), "status", ObZoneStatus::INACTIVE, K(ret));
    } else {
      leader_coordinator_->signal();
      LOG_INFO("succeed to stop zone", K(zone));
      ROOTSERVICE_EVENT_ADD("zone", "stop_zone", K(zone));
    }
  }
  return ret;
}

int ObZoneManagerBase::alter_zone(const obrpc::ObAdminZoneArg& arg)
{
  int ret = OB_SUCCESS;
  int64_t index = OB_INVALID_INDEX;
  ObZone zone = arg.zone_;
  ObRegion my_region = arg.region_;
  ObIDC my_idc = arg.idc_;
  ObZoneType my_zone_type = arg.zone_type_;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1440) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("cannot alter zone during updating to 144", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "alter zone during upgrading to version 1.4.4");
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone), K(ret));
  } else {
    if (my_region.is_empty()) {
      my_region.assign(DEFAULT_REGION_NAME);
    }
    if (my_idc.is_empty()) {
      my_idc.assign("");
    }
    if (my_region.is_empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(my_region), K(ret));
    } else if (OB_FAIL(find_zone(zone, index))) {
      LOG_WARN("find_zone failed", K(zone), K(ret));
    } else if (index < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid index", K(ret), K(index));
    } else {
      ret = OB_SUCCESS;
      ObZoneItemTransUpdater updater;
      if (OB_FAIL(updater.start(*proxy_))) {
        LOG_WARN("start transaction failed", K(ret));
      } else {
        if (arg.alter_zone_options_.has_member(obrpc::ObAdminZoneArg::ALTER_ZONE_REGION)) {
          ObZoneInfoItem::Info my_region_info;
          if (OB_FAIL(my_region_info.assign(ObString(my_region.size(), my_region.ptr())))) {
            LOG_WARN("fail assign region info", K(ret), K(my_region));
          } else if (OB_FAIL(zone_infos_[index].region_.update(updater, zone, 0, my_region_info))) {
            LOG_WARN("failed to update region", K(ret), K(zone));
          } else {
          }  // no more to do
        }
        if (OB_SUCC(ret) && arg.alter_zone_options_.has_member(obrpc::ObAdminZoneArg::ALTER_ZONE_IDC)) {
          ObZoneInfoItem::Info my_idc_info;
          if (OB_FAIL(my_idc_info.assign(ObString(my_idc.size(), my_idc.ptr())))) {
            LOG_WARN("fail to assign idc info", K(ret), K(my_idc));
          } else if (OB_FAIL(zone_infos_[index].idc_.update(updater, zone, 0, my_idc_info))) {
            LOG_WARN("fail to update idc", K(ret), K(zone));
          } else {
          }  // no more to do
        }
        if (OB_SUCC(ret) && arg.alter_zone_options_.has_member(obrpc::ObAdminZoneArg::ALTER_ZONE_TYPE)) {
          ObZoneInfoItem::Info my_zone_type_info;
          if (my_zone_type < ObZoneType::ZONE_TYPE_READWRITE || my_zone_type >= ObZoneType::ZONE_TYPE_INVALID) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("zone type is invalid", K(ret), K(arg));
          } else if (ObZoneType::ZONE_TYPE_READONLY == my_zone_type) {
            // After version 2.2, support for read-only zone is no longer supported,
            // and relevant syntax needs to be banned
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("readonly zone after ver2.2 not supported", K(ret), K(zone));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "readonly zone after version 2.2");
          } else if (OB_FAIL(my_zone_type_info.assign(zone_type_to_str(my_zone_type)))) {
            LOG_WARN("fail to assign zone_type info", K(ret), K(my_zone_type));
          } else if (OB_FAIL(zone_infos_[index].zone_type_.update(updater, zone, my_zone_type, my_zone_type_info))) {
            LOG_WARN("fail to update zone_type", K(ret), K(zone), K(my_zone_type), K(my_zone_type_info));
          } else {
          }  // no more to do
        }
        int tmp_ret = updater.end(OB_SUCC(ret));
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
        }
      }
      if (OB_SUCC(ret)) {
        leader_coordinator_->signal();
        LOG_INFO("succeed to alter zone", "zone_info", zone_infos_[index]);
        ROOTSERVICE_EVENT_ADD("zone",
            "alter_zone",
            "zone",
            arg.zone_,
            "region",
            arg.region_,
            "idc",
            arg.idc_,
            "zone_type",
            arg.zone_type_);
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::reload()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObZone, MAX_ZONE_NUM> zone_list;
  HEAP_VAR(ObGlobalInfo, global_info)
  {
    ObMalloc alloc(ObModIds::OB_TEMP_VARIABLES);
    ObPtrGuard<ObZoneInfo, common::MAX_ZONE_NUM> tmp_zone_infos(alloc);
    // The last updated value is used by default
    int64_t cluster_create_ts = cluster_create_ts_;

    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(tmp_zone_infos.init())) {
      LOG_WARN("failed to alloc temp zone infos", K(ret));
    } else if (OB_FAIL(ObZoneTableOperation::load_global_info(*proxy_, global_info))) {
      LOG_WARN("failed to get global info", K(ret));
    } else if (OB_FAIL(ObZoneTableOperation::get_zone_list(*proxy_, zone_list))) {
      LOG_WARN("failed to get zone list", K(ret));
    }
    // only invalid create timestamp need to update.
    // need update only once after restart.
    else if (OB_INVALID_TIMESTAMP == cluster_create_ts &&
             OB_FAIL(ObClusterInfoProxy::load_cluster_create_timestamp(*proxy_, cluster_create_ts))) {
      LOG_WARN("failed to load cluster create timestamp", KR(ret), K(cluster_create_ts_));
    } else if (zone_list.count() > common::MAX_ZONE_NUM) {
      ret = OB_ERR_SYS;
      LOG_ERROR("the count of zone on __all_zone table is more than limit, cannot reload",
          "zone count",
          zone_list.count(),
          "zone count limit",
          common::MAX_ZONE_NUM,
          K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
        ObZoneInfo& info = tmp_zone_infos.ptr()[i];
        info.zone_ = zone_list[i];
        if (OB_FAIL(ObZoneTableOperation::load_zone_info(*proxy_, info))) {
          LOG_WARN("failed reload zone", "zone", zone_list[i], K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      reset();
      global_info_ = global_info;
      cluster_create_ts_ = cluster_create_ts;

      for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
        zone_infos_[zone_count_] = tmp_zone_infos.ptr()[i];
        ++zone_count_;
      }
    }

    if (OB_SUCC(ret)) {
      loaded_ = true;
      LOG_INFO("succeed to reload zone manager", "zone_manager_info", this, K(cluster_create_ts));
    } else {
      LOG_WARN("failed to reload zone manager", KR(ret), K(cluster_create_ts));
    }
  }
  return ret;
}

bool ObZoneManagerBase::is_stagger_merge() const
{
  return GCONF.enable_merge_by_turn;
}

int ObZoneManagerBase::is_merge_error(bool& is_merge_error) const
{
  int ret = OB_SUCCESS;
  is_merge_error = false;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    is_merge_error = global_info_.is_merge_error_;
  }
  return ret;
}

int ObZoneManagerBase::set_merge_error(const int64_t merge_error)
{
  int ret = OB_SUCCESS;
  ObZoneItemTransUpdater updater;
  ObZoneInfo::MergeStatus status = ObZoneInfo::MERGE_STATUS_MAX;
  if (0 == merge_error) {
    if (global_info_.last_merged_version_ == global_info_.global_broadcast_version_) {
      status = ObZoneInfo::MERGE_STATUS_IDLE;
    } else {
      status = ObZoneInfo::MERGE_STATUS_MERGING;
    }
  } else if (1 == merge_error) {
    status = ObZoneInfo::MERGE_STATUS_ERROR;
  }
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (0 != merge_error && 1 != merge_error) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(merge_error), K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else if (OB_FAIL(global_info_.is_merge_error_.update(updater, global_info_.zone_, merge_error))) {
    LOG_WARN("set merge error flag failed", K(ret), "zone", global_info_.zone_, K(merge_error));
  } else if (OB_FAIL(update_global_merge_status(status, updater))) {
    LOG_WARN("update merge status failed", K(ret));
  }
  int tmp_ret = updater.end(OB_SUCC(ret));
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  if (OB_SUCCESS == ret) {
    LOG_INFO("set merge error", K(ret), K(merge_error), K(global_info_.is_merge_error_));
    ROOTSERVICE_EVENT_ADD("daily_merge",
        "set_merge_error",
        "merge_version",
        global_info_.global_broadcast_version_.value_,
        K(merge_error));
  }
  return ret;
}

int ObZoneManagerBase::is_in_merge(bool& merge) const
{
  int ret = OB_SUCCESS;
  merge = false;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else {
    merge = global_info_.last_merged_version_ != global_info_.global_broadcast_version_;
  }
  return ret;
}

int ObZoneManagerBase::try_update_global_last_merged_version()
{
  int ret = OB_SUCCESS;
  ObZoneItemTransUpdater updater;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else {
    const int64_t cur_time = ObTimeUtility::current_time();
    const int64_t lease_info_version = std::max(global_info_.lease_info_version_ + 1, cur_time);
    bool need_update = false;
    if (global_info_.last_merged_version_ != global_info_.global_broadcast_version_) {
      bool merged = true;
      for (int64_t i = 0; OB_SUCC(ret) && merged && i < zone_count_; ++i) {
        if (zone_infos_[i].last_merged_version_ < global_info_.global_broadcast_version_) {
          LOG_INFO("zone not merged",
              "global_broadcast_version",
              global_info_.global_broadcast_version_.value_,
              "zone_info",
              zone_infos_[i]);
          merged = false;
        }
      }
      if (OB_SUCC(ret)) {
        need_update = merged;
      }
    }

    if (OB_SUCCESS == ret && need_update) {
      if (OB_FAIL(updater.start(*proxy_))) {
        LOG_WARN("start transaction failed", K(ret));
      } else {
        if (OB_FAIL(global_info_.last_merged_version_.update(
                updater, global_info_.zone_, global_info_.global_broadcast_version_.value_))) {
          LOG_WARN("update global last merged version failed",
              "last_merged_version",
              global_info_.global_broadcast_version_.value_,
              K(ret));
        } else if (OB_FAIL(global_info_.lease_info_version_.update(updater, global_info_.zone_, lease_info_version))) {
          LOG_WARN("update lease info version failed", K(ret), K(lease_info_version));
        } else if (OB_FAIL(update_global_merge_status(ObZoneInfo::MERGE_STATUS_IDLE, updater))) {
          LOG_WARN("update global merge status failed", "status", ObZoneInfo::MERGE_STATUS_IDLE, K(ret));
        }
        int tmp_ret = updater.end(OB_SUCC(ret));
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
        }
        if (OB_SUCC(ret)) {
          (void)ATOMIC_SET(&global_info_.lease_info_version_.value_, lease_info_version);
          LOG_INFO("update_global_last_merged_version", K(global_info_), K(lease_info_version));
        }
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::generate_next_global_broadcast_version()
{
  int ret = OB_SUCCESS;
  ObZoneItemTransUpdater updater;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (global_info_.last_merged_version_ < global_info_.global_broadcast_version_) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("not merged yet",
        "last_merged_version",
        global_info_.last_merged_version_.value_,
        "global_broadcast_version",
        global_info_.global_broadcast_version_.value_,
        K(ret));
  } else if (global_info_.last_merged_version_ > global_info_.global_broadcast_version_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("global_last_merged_version must not larger than global_broadcast_version",
        "last_merged_version",
        global_info_.last_merged_version_.value_,
        "global_broadcast_version",
        global_info_.global_broadcast_version_.value_,
        K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    if (global_info_.global_broadcast_version_ < global_info_.frozen_version_) {
      if (OB_FAIL(global_info_.global_broadcast_version_.update(
              updater, global_info_.zone_, global_info_.global_broadcast_version_ + 1))) {
        LOG_WARN("failed to set_global_broadcast_version",
            "global_broadcast_version",
            global_info_.global_broadcast_version_ + 1,
            K(ret));
      } else {
        LOG_INFO("update global broadcast version", "global_broadcast_version", global_info_.global_broadcast_version_);
      }

      if (OB_SUCC(ret)) {
        const ObZoneInfo::MergeStatus status = ObZoneInfo::MERGE_STATUS_MERGING;
        if (OB_FAIL(update_global_merge_status(status, updater))) {
          LOG_WARN("update global merge status failed", K(status), K(ret));
        }
      }
    }
  }

  int tmp_ret = updater.end(OB_SUCC(ret));
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}
int ObZoneManagerBase::clear_zone_merging(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  // const int64_t cur_time = ObTimeUtility::current_time();
  ObZoneItemTransUpdater updater;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone), K(ret));
  } else if (OB_FAIL(find_zone(zone, idx))) {
    LOG_ERROR("find_zone failed", K(zone), K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    const int64_t is_merging = 0;
    if (is_merging != zone_infos_[idx].is_merging_) {
      if (OB_FAIL(zone_infos_[idx].is_merging_.update(updater, zone, is_merging))) {
        LOG_WARN("update zone merge flag failed", K(is_merging), K(ret));
      } else {
        zone_infos_[idx].start_merge_fail_times_++;
      }
    }

    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }

  LOG_INFO("clear zone merging", K(ret), K(zone));
  return ret;
}

int ObZoneManagerBase::inc_start_merge_fail_times(const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone), K(ret));
  } else if (OB_FAIL(find_zone(zone, idx))) {
    LOG_ERROR("find_zone failed", K(zone), K(ret));
  } else {
    zone_infos_[idx].start_merge_fail_times_++;
  }
  return ret;
}

int ObZoneManagerBase::set_zone_merging(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  // const int64_t cur_time = ObTimeUtility::current_time();
  ObZoneItemTransUpdater updater;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone), K(ret));
  } else if (OB_FAIL(find_zone(zone, idx))) {
    LOG_ERROR("find_zone failed", K(zone), K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    const int64_t is_merging = 1;
    if (is_merging != zone_infos_[idx].is_merging_) {
      if (OB_FAIL(zone_infos_[idx].is_merging_.update(updater, zone, is_merging))) {
        LOG_WARN("update zone merge flag failed", K(is_merging), K(ret));
        //}  else if (OB_FAIL(zone_infos_[idx].merge_start_time_.update(updater, zone, cur_time))) {
        //  LOG_WARN("update merge start time failed", K(ret), K(zone), K(cur_time));
      }
    }

    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }

  LOG_INFO("set zone merging", K(ret), K(zone));
  return ret;
}

int ObZoneManagerBase::start_zone_merge(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  const int64_t cur_time = ObTimeUtility::current_time();
  const int64_t lease_info_version = std::max(global_info_.lease_info_version_ + 1, cur_time);
  ObZoneItemTransUpdater updater;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone), K(ret));
  } else if (OB_FAIL(find_zone(zone, idx))) {
    LOG_ERROR("find_zone failed", K(zone), K(ret));
  } else if (zone_infos_[idx].broadcast_version_ + 1 != global_info_.global_broadcast_version_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("broadcast_version must only increase by 1",
        "old broadcast_version",
        zone_infos_[idx].broadcast_version_,
        "new broadcast_version",
        global_info_.global_broadcast_version_,
        K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    const int64_t is_merging = 1;
    const ObZoneInfo::MergeStatus status = ObZoneInfo::MERGE_STATUS_MERGING;
    if (OB_FAIL(zone_infos_[idx].is_merging_.update(updater, zone, is_merging))) {
      LOG_WARN("update is merging failed", K(ret), K(zone), K(is_merging));
    } else if (OB_FAIL(
                   zone_infos_[idx].broadcast_version_.update(updater, zone, global_info_.global_broadcast_version_))) {
      LOG_WARN("update broadcast version failed",
          K(ret),
          K(zone),
          "broadcast_version",
          global_info_.global_broadcast_version_);
    } else if (OB_FAIL(zone_infos_[idx].merge_start_time_.update(updater, zone, cur_time))) {
      LOG_WARN("update merge start time failed", K(ret), K(zone), K(cur_time));
    } else if (OB_FAIL(global_info_.lease_info_version_.update(updater, global_info_.zone_, lease_info_version))) {
      LOG_WARN("update lease info version failed", K(ret), K(lease_info_version));
    } else if (OB_FAIL(zone_infos_[idx].merge_status_.update(
                   updater, zone, status, ObZoneInfo::get_merge_status_str(status)))) {
      LOG_WARN("update merge status failed", K(ret), K(zone), K(status));
    } else if (OB_FAIL(update_global_merge_status(updater))) {
      LOG_WARN("update global merge status failed", K(ret), K(zone), K(status));
    }

    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      (void)ATOMIC_SET(&global_info_.lease_info_version_.value_, lease_info_version);
      (void)ATOMIC_SET(&zone_infos_[idx].start_merge_fail_times_, 0);
    }
  }
  LOG_INFO("start zone merge", K(ret), K(zone), "broadcast_version", global_info_.global_broadcast_version_);
  return ret;
}

int ObZoneManagerBase::finish_zone_merge(
    const ObZone& zone, const int64_t merged_version, const int64_t all_merged_version)
{
  int ret = OB_SUCCESS;
  int64_t idx = OB_INVALID_INDEX;
  const int64_t cur_time = ObTimeUtility::current_time();
  const int64_t lease_info_version = std::max(global_info_.lease_info_version_ + 1, cur_time);
  ObZoneItemTransUpdater updater;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty() || merged_version <= 0 || all_merged_version <= 0 || all_merged_version > merged_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone), K(merged_version), K(all_merged_version));
  } else if (OB_FAIL(find_zone(zone, idx))) {
    LOG_ERROR("find_zone failed", K(zone), K(ret));
  } else if (merged_version != zone_infos_[idx].broadcast_version_ &&
             merged_version + 1 != zone_infos_[idx].broadcast_version_) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid merged version",
        "broadcast_version",
        zone_infos_[idx].broadcast_version_.value_,
        K(merged_version),
        K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    ObZoneInfo& info = zone_infos_[idx];
    ObZoneInfo::MergeStatus status = static_cast<ObZoneInfo::MergeStatus>(info.merge_status_.value_);
    if (merged_version != info.last_merged_version_) {
      status = ObZoneInfo::MERGE_STATUS_MOST_MERGED;
      const int64_t is_merging = 0;
      const int64_t is_merge_timeout = 0;
      if (OB_FAIL(info.is_merging_.update(updater, zone, is_merging))) {
        LOG_WARN("update is merging failed", K(ret), K(zone));
      } else if (OB_FAIL(info.last_merged_version_.update(updater, zone, merged_version))) {
        LOG_WARN("update last merged version failed", K(ret), K(zone), K(merged_version));
      } else if (OB_FAIL(info.last_merged_time_.update(updater, zone, cur_time))) {
        LOG_WARN("update last merged time failed", K(ret), K(zone), K(cur_time));
      } else if (OB_FAIL(info.is_merge_timeout_.update(updater, zone, is_merge_timeout))) {
        LOG_WARN("update is merge timeout failed", K(ret), K(zone), K(is_merge_timeout));
      }
    }
    if (OB_SUCC(ret)) {
      if (all_merged_version == info.broadcast_version_) {
        status = ObZoneInfo::MERGE_STATUS_IDLE;
      }
      if (all_merged_version > info.all_merged_version_) {
        if (OB_FAIL(info.all_merged_version_.update(updater, zone, all_merged_version))) {
          LOG_WARN("update all merged version failed", K(ret), K(zone), K(all_merged_version));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(global_info_.lease_info_version_.update(updater, global_info_.zone_, lease_info_version))) {
        LOG_WARN("update lease info version failed", K(ret), K(lease_info_version));
      } else if (OB_FAIL(info.merge_status_.update(updater, zone, status, ObZoneInfo::get_merge_status_str(status)))) {
        LOG_WARN("update merge status failed", K(ret), K(zone), K(status));
      } else if (OB_FAIL(update_global_merge_status(updater))) {
        LOG_WARN("update global merge status failed", K(ret), K(zone), K(status));
      }
    }

    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      (void)ATOMIC_SET(&global_info_.lease_info_version_.value_, lease_info_version);
    }
  }
  LOG_INFO("finish zone merge", K(ret), K(zone), K(merged_version), K(all_merged_version));
  return ret;
}

int ObZoneManagerBase::set_zone_merge_timeout(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  const int64_t is_merging = 0;
  const int64_t is_merge_timeout = 1;
  int64_t idx = OB_INVALID_INDEX;
  ObZoneItemTransUpdater updater;
  const ObZoneInfo::MergeStatus status = ObZoneInfo::MERGE_STATUS_TIMEOUT;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone), K(ret));
  } else if (OB_FAIL(find_zone(zone, idx))) {
    LOG_ERROR("find_zone failed", K(zone), K(ret));
  } else if (OB_INVALID_INDEX == idx) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid idx", K(idx), K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else if (OB_FAIL(zone_infos_[idx].is_merging_.update(updater, zone, is_merging))) {
    LOG_WARN("update is merging failed", K(ret), K(zone));
  } else if (OB_FAIL(zone_infos_[idx].is_merge_timeout_.update(updater, zone, is_merge_timeout))) {
    LOG_WARN("update is merge timeout failed", K(ret), K(zone), K(is_merge_timeout));
  } else if (OB_FAIL(zone_infos_[idx].merge_status_.update(
                 updater, zone, status, ObZoneInfo::get_merge_status_str(status)))) {
    LOG_WARN("update merge status failed", K(ret), K(zone), K(status));
  } else if (OB_FAIL(update_global_merge_status(updater))) {
    LOG_WARN("update global merge status failed", K(ret), K(zone), K(status));
  }

  int tmp_ret = updater.end(OB_SUCC(ret));
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }

  return ret;
}

int ObZoneManagerBase::update_privilege_version(const int64_t privilege_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (privilege_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid privilege_version", K(privilege_version), K(ret));
  } else if (OB_FAIL(update_value_with_lease(global_info_.zone_, global_info_.privilege_version_, privilege_version))) {
    LOG_WARN("update privilege version failed", K(ret), K(privilege_version));
  }
  return ret;
}

int ObZoneManagerBase::update_config_version(const int64_t config_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (config_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid config_version", K(config_version), K(ret));
  } else if (OB_FAIL(update_value_with_lease(global_info_.zone_, global_info_.config_version_, config_version))) {
    LOG_WARN("update config version failed", K(ret), K(config_version));
  }
  return ret;
}

int ObZoneManagerBase::set_frozen_info(const int64_t frozen_version, const int64_t frozen_time)
{
  int ret = OB_SUCCESS;
  ObZoneItemTransUpdater updater;
  if (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_2000) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("function should not be called when new major is on", K(ret));
  } else if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (frozen_version <= 0 || frozen_time <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(frozen_version), K(frozen_time), K(ret));
  } else if (global_info_.frozen_version_ >= frozen_version) {
    // nothing todo
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    const ObZone& zone = global_info_.zone_;
    if (OB_FAIL(global_info_.frozen_version_.update(updater, zone, frozen_version))) {
      LOG_WARN("update frozen version failed", K(ret), K(frozen_version));
    } else if (OB_FAIL(global_info_.frozen_time_.update(updater, zone, frozen_time))) {
      LOG_WARN("update frozen time failed", K(ret), K(frozen_time));
    }

    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
    LOG_WARN("finish set frozen info", K(ret), K(frozen_version), K(frozen_time));
  }
  return ret;
}

int ObZoneManagerBase::set_frozen_info(
    common::ObISQLClient& sql_client, const int64_t frozen_version, const int64_t frozen_time)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (frozen_version <= 0 || frozen_time <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(frozen_version), K(frozen_time), K(ret));
  } else {
    const ObZone& zone = global_info_.zone_;
    if (OB_FAIL(global_info_.frozen_version_.update(sql_client, zone, frozen_version))) {
      LOG_WARN("update frozen version failed", K(ret), K(frozen_version));
    } else if (OB_FAIL(global_info_.frozen_time_.update(sql_client, zone, frozen_time))) {
      LOG_WARN("update frozen time failed", K(ret), K(frozen_time));
    }
  }
  return ret;
}

int ObZoneManagerBase::get_frozen_info(int64_t& frozen_version, int64_t& frozen_time) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    frozen_version = global_info_.frozen_version_;
    frozen_time = global_info_.frozen_time_;
  }
  return ret;
}

int ObZoneManagerBase::set_try_frozen_version(const int64_t try_frozen_version)
{
  int ret = OB_SUCCESS;
  ObZoneItemTransUpdater updater;
  if (OB_FAIL(check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (try_frozen_version != global_info_.frozen_version_ + 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("try frozen version must be frozen version + 1",
        K(try_frozen_version),
        "frozen_version",
        global_info_.frozen_version_,
        K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    if (OB_FAIL(global_info_.try_frozen_version_.update(updater, global_info_.zone_, try_frozen_version))) {
      LOG_WARN("update try frozen version failed", K(ret), K(try_frozen_version));
    }

    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

// only record valid proposal frozen version
int ObZoneManagerBase::update_proposal_frozen_version(const int64_t proposal_frozen_version)
{
  int ret = OB_SUCCESS;
  ObZoneItemTransUpdater updater;
  if (OB_FAIL(check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (proposal_frozen_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("wrong proposal frozen version", K(proposal_frozen_version), K(ret));
  } else if (proposal_frozen_version == global_info_.proposal_frozen_version_ + 1) {
    if (OB_FAIL(update_value_with_lease(
            global_info_.zone_, global_info_.proposal_frozen_version_, proposal_frozen_version))) {
      LOG_WARN("update proposal_frozen_version failed", K(proposal_frozen_version), K(ret));
    }
  }
  return ret;
}

int ObZoneManagerBase::get_proposal_frozen_version(int64_t& proposal_frozen_version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    proposal_frozen_version = global_info_.proposal_frozen_version_;
  }
  return ret;
}

int ObZoneManagerBase::get_try_frozen_version(int64_t& frozen_version, int64_t& try_frozen_version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    frozen_version = global_info_.frozen_version_;
    try_frozen_version = global_info_.try_frozen_version_;
  }
  return ret;
}

int ObZoneManagerBase::get_cluster(ObFixedLengthString<MAX_ZONE_INFO_LENGTH>& cluster) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    cluster = global_info_.cluster_.info_;
  }
  return ret;
}

int ObZoneManagerBase::get_global_broadcast_version(int64_t& global_broadcast_version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    global_broadcast_version = global_info_.global_broadcast_version_;
  }
  return ret;
}

int ObZoneManagerBase::get_config_version(int64_t& config_version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    config_version = global_info_.config_version_;
  }
  return ret;
}

int ObZoneManagerBase::get_time_zone_info_version(int64_t& time_zone_info_version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    time_zone_info_version = global_info_.time_zone_info_version_;
  }
  return ret;
}

int ObZoneManagerBase::get_lease_info_version(int64_t& lease_info_version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    lease_info_version = global_info_.lease_info_version_;
  }
  return ret;
}

int ObZoneManagerBase::get_global_last_merged_version(int64_t& global_last_merged_version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    global_last_merged_version = global_info_.last_merged_version_;
  }
  return ret;
}

int ObZoneManagerBase::check_merge_order(const common::ObString& merge_order_str)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> merge_list;
  ObString list_str(merge_order_str.length(), merge_order_str.ptr());
  ObString zone_str;
  ObString zone;
  bool split_end = false;
  while (!split_end && OB_SUCCESS == ret) {
    zone_str = list_str.split_on(',');
    if (zone_str.empty() && NULL == zone_str.ptr()) {
      split_end = true;
      zone_str = list_str;
    }
    zone = zone_str.trim();
    if (!zone.empty()) {
      if (OB_FAIL(merge_list.push_back(zone))) {
        LOG_WARN("push back to array failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_merge_order(merge_list))) {
      LOG_WARN("fail to check merge list", K(ret));
    }
  }
  return ret;
}

int ObZoneManagerBase::check_merge_order(const ObIArray<ObZone>& merge_list)
{
  int ret = OB_SUCCESS;
  if (0 == merge_list.count()) {
    // nothing todo
  } else if (merge_list.count() != zone_count_) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_ERROR("invalid merge list count", K(ret), K(merge_list), K(zone_count_));
  } else {
    int64_t index = -1;
    for (int64_t i = 0; i < merge_list.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(find_zone(merge_list.at(i), index))) {
        LOG_WARN("fail to find zone", K(ret), "zone", merge_list.at(i));
      } else if (index < 0 || index >= zone_count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to find zone", K(ret), K(index), "zone", merge_list.at(i), K(zone_count_));
      }
    }
    for (int64_t i = 0; i < zone_count_ && OB_SUCC(ret); i++) {
      if (!has_exist_in_array(merge_list, zone_infos_[i].zone_)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_ERROR("fail to find zone", K(ret), "zone", zone_infos_[i].zone_, K(zone_count_));
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::update_value_with_lease(
    const common::ObZone& zone, share::ObZoneInfoItem& item, int64_t value, const char* info /* = NULL */)
{
  int ret = OB_SUCCESS;
  const int64_t cur_time = ObTimeUtility::current_time();
  const int64_t lease_info_version = std::max(global_info_.lease_info_version_ + 1, cur_time);
  ObZoneItemTransUpdater updater;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (!item.is_valid() || value < 0) {
    // empty zone means all zone, info can be null
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(item), K(value), K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    if (OB_FAIL(item.update(updater, zone, value, info))) {
      LOG_WARN("update item failed", K(ret), K(zone), K(value), K(info));
    } else if (OB_FAIL(global_info_.lease_info_version_.update(updater, global_info_.zone_, lease_info_version))) {
      LOG_WARN("update lease info version failed", K(ret), K(lease_info_version));
    }

    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      (void)ATOMIC_SET(&global_info_.lease_info_version_.value_, lease_info_version);
    }
  }
  return ret;
}

int ObZoneManagerBase::find_zone(const common::ObZone& zone, int64_t& idx) const
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_INDEX;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    ret = OB_ENTRY_NOT_EXIST;
    for (int64_t i = 0; i < zone_count_; ++i) {
      if (zone == zone_infos_[i].zone_) {
        idx = i;
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::suspend_merge(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  const bool suspend = true;
  // empty zone means all zone
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(suspend_or_resume_merge(zone, suspend))) {
    LOG_WARN("suspend_or_resume_merge failed", K(zone), K(suspend), K(ret));
  }
  return ret;
}

int ObZoneManagerBase::resume_merge(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  const bool suspend = false;
  // empty zone means all zone
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(suspend_or_resume_merge(zone, suspend))) {
    LOG_WARN("suspend_or_resume_merge failed", K(zone), K(suspend), K(ret));
  }
  return ret;
}

int ObZoneManagerBase::suspend_or_resume_merge(const ObZone& zone, const bool suspend)
{
  int ret = OB_SUCCESS;
  // empty zone means all zone
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    const int64_t cur_time = ObTimeUtility::current_time();
    const int64_t lease_info_version = std::max(global_info_.lease_info_version_ + 1, cur_time);
    ObZoneItemTransUpdater updater;
    if (OB_FAIL(updater.start(*proxy_))) {
      LOG_WARN("start transaction failed", K(ret));
    } else {
      bool exist = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_count_; ++i) {
        if (zone_infos_[i].zone_ == zone || zone.is_empty()) {
          exist = true;
          if (OB_FAIL(zone_infos_[i].suspend_merging_.update(updater, zone_infos_[i].zone_, suspend))) {
            LOG_WARN("update suspend merging failed", "zone", zone_infos_[i].zone_, K(suspend), K(ret));
          }
        }
      }

      if (OB_SUCC(ret) && !exist) {
        ret = OB_ZONE_INFO_NOT_EXIST;
        LOG_WARN("zone not exist", K(zone), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(global_info_.lease_info_version_.update(updater, global_info_.zone_, lease_info_version))) {
        LOG_WARN("update lease info version failed", K(ret), K(lease_info_version));
      }
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      (void)ATOMIC_SET(&global_info_.lease_info_version_.value_, lease_info_version);
    }
  }

  return ret;
}

int ObZoneManagerBase::update_global_merge_status(share::ObZoneItemTransUpdater& updater)
{
  int ret = OB_SUCCESS;
  // merge status will be detected if set to MAX
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner status failed", K(ret));
  } else {
    ObZoneInfo::MergeStatus status = ObZoneInfo::MERGE_STATUS_MAX;
    if (global_info_.global_broadcast_version_ != global_info_.last_merged_version_) {
      status = ObZoneInfo::MERGE_STATUS_MERGING;
    }
    const ObArrayHelper<ObZoneInfo> infos(zone_count_, zone_infos_, zone_count_);
    FOREACH_CNT(info, infos)
    {
      if (info->is_merge_timeout_) {
        status = ObZoneInfo::MERGE_STATUS_TIMEOUT;
      }
    }
    if (global_info_.is_merge_error_) {
      status = ObZoneInfo::MERGE_STATUS_ERROR;
    }
    if (ObZoneInfo::MERGE_STATUS_MAX == status) {
      status = static_cast<ObZoneInfo::MergeStatus>(global_info_.merge_status_.value_);
    }
    if (OB_FAIL(update_global_merge_status(status, updater))) {
      LOG_WARN("update global merge status failed", K(status), K(ret));
    }
  }
  return ret;
}

int ObZoneManagerBase::update_global_merge_status(const ObZoneInfo::MergeStatus status, ObZoneItemTransUpdater& updater)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner status failed", K(ret));
  } else if (status < ObZoneInfo::MERGE_STATUS_IDLE || status >= ObZoneInfo::MERGE_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid status", K(ret), K(status));
  } else if (global_info_.is_merge_error_ && status != ObZoneInfo::MERGE_STATUS_ERROR) {
    LOG_WARN("daily merge is error, can't change merge_status", K(global_info_.merge_status_));
  } else {
    if (global_info_.merge_status_.value_ != status) {
      ObZoneInfo::MergeStatus old_status = static_cast<ObZoneInfo::MergeStatus>(global_info_.merge_status_.value_);
      if (OB_FAIL(global_info_.merge_status_.update(
              updater, global_info_.zone_, status, ObZoneInfo::get_merge_status_str(status)))) {
        LOG_WARN("update merge status failed", K(ret), K(status));
      } else {
        LOG_INFO("update global merge status",
            "old_status",
            ObZoneInfo::get_merge_status_str(old_status),
            "new_status",
            ObZoneInfo::get_merge_status_str(status));
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::reset_global_merge_status()
{
  int ret = OB_SUCCESS;
  const ObZoneInfo::MergeStatus status = ObZoneInfo::MERGE_STATUS_IDLE;
  ObZoneItemTransUpdater updater;
  if (OB_FAIL(check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    // first reset to IDLE, then detect new global status.
    if (OB_FAIL(update_global_merge_status(status, updater))) {
      LOG_WARN("update global merge status failed", K(ret), K(status));
    } else if (update_global_merge_status(updater)) {
      LOG_WARN("update global merge status failed", K(ret));
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }

  return ret;
}

DEF_TO_STRING(ObZoneManagerBase)
{
  SpinRLockGuard guard(lock_);
  int64_t pos = 0;
  J_KV(K_(global_info), K_(zone_count), K_(cluster_create_ts));
  J_ARRAY_START();
  for (int64_t i = 0; i < zone_count_; ++i) {
    if (0 != i) {
      J_COMMA();
    }
    BUF_PRINTO(zone_infos_[i]);
  }
  J_ARRAY_END();
  return pos;
}

int ObZoneManagerBase::set_warm_up_start_time(const int64_t time_ts)
{
  int ret = OB_SUCCESS;
  ObZoneItemTransUpdater updater;
  const int64_t cur_time = ObTimeUtility::current_time();
  const int64_t lease_info_version = std::max(global_info_.lease_info_version_ + 1, cur_time);

  if (OB_FAIL(check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (time_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(time_ts));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    if (OB_FAIL(global_info_.warm_up_start_time_.update(updater, global_info_.zone_, time_ts))) {
      LOG_WARN("failed to update warm up start time", K(ret), K(time_ts));
    } else if (OB_FAIL(global_info_.lease_info_version_.update(updater, global_info_.zone_, lease_info_version))) {
      LOG_WARN("update lease info version failed", K(ret), K(lease_info_version));
    }

    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObZoneManagerBase::get_warm_up_start_time(int64_t& time_ts) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    time_ts = global_info_.warm_up_start_time_;
  }
  return ret;
}

// only used for copying data to/from shadow_
int ObZoneManagerBase::copy_infos(ObZoneManagerBase& dest, const ObZoneManagerBase& src)
{
  int ret = OB_SUCCESS;
  const int64_t count = src.zone_count_;
  if (0 > count || common::MAX_ZONE_NUM < count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid zone count", K(count), K(ret));
  } else {
    for (int64_t idx = 0; idx < count; ++idx) {
      dest.zone_infos_[idx] = src.zone_infos_[idx];
    }
    dest.global_info_ = src.global_info_;
    dest.cluster_create_ts_ = src.cluster_create_ts_;
    dest.zone_count_ = count;
    dest.inited_ = src.inited_;
    dest.loaded_ = src.loaded_;
  }
  return ret;
}

int ObZoneManagerBase::get_snapshot_gc_ts_in_memory(int64_t& gc_timestamp) const
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_2000) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("command not support now", K(ret));
  } else {
    SpinRLockGuard guard(lock_);
    if (OB_FAIL(check_inner_stat())) {
      LOG_WARN("fail to check inner stat", K(ret));
    } else if (!global_info_.is_valid()) {
      ret = OB_EAGAIN;
    } else {
      gc_timestamp = global_info_.snapshot_gc_ts_;
    }
  }
  return ret;
}
int ObZoneManagerBase::renew_snapshot_gc_ts()
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_2000) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("command not support now", K(ret));
  } else {
    int64_t gc_timestamp = ObTimeUtility::current_time();
    int64_t schema_version = OB_INVALID_VERSION;
    ObZoneItemTransUpdater updater;
    if (OB_FAIL(check_inner_stat())) {
      LOG_WARN("fail to check inner stat", K(ret));
    } else if (gc_timestamp <= global_info_.snapshot_gc_ts_) {
      // nothing todo
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_version(OB_SYS_TENANT_ID, schema_version))) {
      LOG_WARN("fail to get schema version", K(ret));
    } else {
      if (OB_FAIL(updater.start(*proxy_))) {
        LOG_WARN("start transaction failed", K(ret));
      } else if (OB_FAIL(global_info_.snapshot_gc_ts_.update(updater, global_info_.zone_, gc_timestamp))) {
        LOG_WARN("fail to update info", K(ret), K(gc_timestamp));
      } else if (OB_FAIL(global_info_.gc_schema_version_.update(updater, global_info_.zone_, schema_version))) {
        LOG_WARN("fail to update schema_version", K(ret), K(schema_version));
      }
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

///////////////////////////
///////////////////////////

int ObZoneManager::renew_snapshot_gc_ts()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.renew_snapshot_gc_ts();
    }
  }
  return ret;
}

ObZoneManager::ObZoneManager() : write_lock_(ObLatchIds::ZONE_MANAGER_LOCK), shadow_(), random_(71)
{}

ObZoneManager::~ObZoneManager()
{}

ObZoneManager::ObZoneManagerShadowGuard::ObZoneManagerShadowGuard(
    const common::SpinRWLock& lock, ObZoneManagerBase& zone_mgr, ObZoneManagerBase& shadow, int& ret)
    : lock_(const_cast<SpinRWLock&>(lock)), zone_mgr_(zone_mgr), shadow_(shadow), ret_(ret)
{
  SpinRLockGuard copy_guard(lock_);
  if (OB_UNLIKELY(OB_SUCCESS != (ret_ = ObZoneManager::copy_infos(shadow_, zone_mgr_)))) {
    LOG_WARN("copy to shadow_ failed", K(ret_));
  }
}

ObZoneManager::ObZoneManagerShadowGuard::~ObZoneManagerShadowGuard()
{
  SpinWLockGuard copy_guard(lock_);
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
  } else if (OB_UNLIKELY(OB_SUCCESS != (ret_ = ObZoneManager::copy_infos(zone_mgr_, shadow_)))) {
    LOG_WARN("copy from shadow_ failed", K(ret_));
  }
}

int ObZoneManager::init(ObMySQLProxy& proxy, ObILeaderCoordinator& leader_coordinator)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    // uninit proxy is used to prevent somebody from
    // trying to directly use the set-interfaces in ObZoneManagerBase
    // actually, all update operations are performed in shadow_
    if (OB_FAIL(ObZoneManagerBase::init(uninit_proxy_, leader_coordinator))) {
      LOG_WARN("init zone manager failed", K(ret));
    } else if (OB_FAIL(shadow_.init(proxy, leader_coordinator))) {
      LOG_WARN("init shadow_ failed", K(ret));
    }
  }
  return ret;
}

int ObZoneManager::reload()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.reload();
    }
  }
  return ret;
}

int ObZoneManager::add_zone(const common::ObZone& zone, const common::ObRegion& region, const common::ObIDC& idc,
    const common::ObZoneType& zone_type)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.add_zone(zone, region, idc, zone_type);
    }
  }
  return ret;
}

int ObZoneManager::delete_zone(const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.delete_zone(zone);
    }
  }
  return ret;
}

int ObZoneManager::start_zone(const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.start_zone(zone);
    }
  }
  return ret;
}

int ObZoneManager::stop_zone(const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.stop_zone(zone);
    }
  }
  return ret;
}

int ObZoneManager::alter_zone(const obrpc::ObAdminZoneArg& arg)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.alter_zone(arg);
    }
  }
  return ret;
}

int ObZoneManager::set_merge_error(const int64_t merge_error)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.set_merge_error(merge_error);
    }
  }
  return ret;
}

int ObZoneManager::try_update_global_last_merged_version()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.try_update_global_last_merged_version();
    }
  }
  return ret;
}

int ObZoneManager::generate_next_global_broadcast_version()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.generate_next_global_broadcast_version();
    }
  }
  return ret;
}

int ObZoneManager::set_zone_merging(const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.set_zone_merging(zone);
    }
  }
  return ret;
}

int ObZoneManager::inc_start_merge_fail_times(const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.inc_start_merge_fail_times(zone);
    }
  }
  return ret;
}

int ObZoneManager::start_zone_merge(const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.start_zone_merge(zone);
    }
  }
  return ret;
}

int ObZoneManager::finish_zone_merge(
    const common::ObZone& zone, const int64_t merged_version, const int64_t all_merged_version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.finish_zone_merge(zone, merged_version, all_merged_version);
    }
  }
  return ret;
}

int ObZoneManager::set_zone_merge_timeout(const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.set_zone_merge_timeout(zone);
    }
  }
  return ret;
}

int ObZoneManager::update_privilege_version(const int64_t privilege_version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.update_privilege_version(privilege_version);
    }
  }
  return ret;
}

int ObZoneManager::update_config_version(const int64_t config_version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.update_config_version(config_version);
    }
  }
  return ret;
}

int ObZoneManager::set_frozen_info(
    common::ObISQLClient& sql_client, const int64_t frozen_version, const int64_t frozen_time)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.set_frozen_info(sql_client, frozen_version, frozen_time);
    }
  }
  return ret;
}

int ObZoneManager::set_frozen_info(const int64_t frozen_version, const int64_t frozen_time)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.set_frozen_info(frozen_version, frozen_time);
    }
  }
  return ret;
}

int ObZoneManager::update_proposal_frozen_version(const int64_t proposal_frozen_version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.update_proposal_frozen_version(proposal_frozen_version);
    }
  }
  return ret;
}

int ObZoneManager::set_try_frozen_version(const int64_t try_frozen_version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.set_try_frozen_version(try_frozen_version);
    }
  }
  return ret;
}

int ObZoneManager::check_merge_order(const ObIArray<ObZone>& merge_order_list)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.check_merge_order(merge_order_list);
    }
  }
  return ret;
}

int ObZoneManager::check_merge_order(const common::ObString& merge_order_str)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.check_merge_order(merge_order_str);
    }
  }
  return ret;
}

int ObZoneManager::suspend_merge(const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.suspend_merge(zone);
    }
  }
  return ret;
}

int ObZoneManager::resume_merge(const common::ObZone& zone)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.resume_merge(zone);
    }
  }
  return ret;
}

int ObZoneManager::reset_global_merge_status()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.reset_global_merge_status();
    }
  }
  return ret;
}

int ObZoneManager::set_warm_up_start_time(const int64_t time_ts)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.set_warm_up_start_time(time_ts);
    }
  }
  return ret;
}
int ObZoneManager::clear_zone_merging(const ObZone& zone)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.clear_zone_merging(zone);
    }
  }
  return ret;
}

int ObZoneManager::set_storage_format_version(const int64_t version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(write_lock_);
  // destruct shadow_copy_guard before return
  // otherwise the ret_ in shadow_copy_guard will never be returned
  {
    ObZoneManagerShadowGuard shadow_copy_guard(lock_, *(static_cast<ObZoneManagerBase*>(this)), shadow_, ret);
    if (OB_SUCC(ret)) {
      ret = shadow_.set_storage_format_version(version);
    }
  }
  return ret;
}

int ObZoneManagerBase::construct_zone_region_list(
    common::ObIArray<ObZoneRegion>& zone_region_list, const common::ObIArray<common::ObZone>& zone_list)
{
  int ret = OB_SUCCESS;
  zone_region_list.reset();
  if (OB_FAIL(check_inner_stat())) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    common::ObArray<share::ObZoneInfo> zone_infos;
    if (OB_FAIL(get_zone(zone_infos))) {
      LOG_WARN("fail to get zone", K(ret));
    } else {
      ObZoneRegion zone_region;
      for (int64_t i = 0; i < zone_infos.count() && OB_SUCC(ret); ++i) {
        zone_region.reset();
        share::ObZoneInfo& zone_info = zone_infos.at(i);
        if (OB_FAIL(zone_region.zone_.assign(zone_info.zone_.ptr()))) {
          LOG_WARN("fail to assign zone", K(ret));
        } else if (OB_FAIL(zone_region.region_.assign(zone_info.region_.info_.ptr()))) {
          LOG_WARN("fail to assign region", K(ret));
        } else if (!has_exist_in_array(zone_list, zone_region.zone_)) {
          // this zone do not exist in my zone list, ignore it
        } else if (OB_FAIL(zone_region_list.push_back(zone_region))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
        }  // no more to do
      }
    }
  }
  return ret;
}

int ObZoneManagerBase::get_storage_format_version(int64_t& version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(lock_);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    version = global_info_.storage_format_version_;
    if (version < OB_STORAGE_FORMAT_VERSION_V3) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage format version should greater than or equal to 3", K(ret), K(version));
    }
  }
  return ret;
}

int ObZoneManagerBase::set_storage_format_version(const int64_t version)
{
  int ret = OB_SUCCESS;
  ObZoneItemTransUpdater updater;
  const int64_t old_version = global_info_.storage_format_version_;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (version <= OB_STORAGE_FORMAT_VERSION_INVALID || version >= OB_STORAGE_FORMAT_VERSION_MAX ||
             old_version < OB_STORAGE_FORMAT_VERSION_V3 || version != old_version + 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(version), K(old_version), K(ret));
  } else if (OB_FAIL(updater.start(*proxy_))) {
    LOG_WARN("start transaction failed", K(ret));
  } else {
    if (OB_FAIL(global_info_.storage_format_version_.update(updater, global_info_.zone_, version))) {
      LOG_WARN("set storage format version failed", K(ret), "zone", global_info_.zone_, K(version));
    }
    int tmp_ret = updater.end(OB_SUCC(ret));
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("set format version succeed", K(ret), K(version), K(global_info_.storage_format_version_));
    }
  }
  return ret;
}

}  // namespace rootserver
}  // namespace oceanbase
