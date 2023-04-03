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

#include "fake_zone_manager.h"
namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;

void FakeZoneManager::init_zone_manager(const int64_t version, int64_t zone_cnt)
{
  UNUSED(version);
  zone_count_ = zone_cnt;
  // global_info_.frozen_version_.value_ = version;
  // global_info_.try_frozen_version_.value_ = version;
  // global_info_.global_broadcast_version_.value_ = version;
  // global_info_.last_merged_version_.value_ = version;

  ObZoneInfo info;
  info.status_.value_ = ObZoneStatus::ACTIVE;
  // info.last_merged_time_.value_ = ::oceanbase::common::ObTimeUtility::current_time();
  // info.merge_start_time_.value_ = ::oceanbase::common::ObTimeUtility::current_time();
  // info.broadcast_version_.value_ = version;
  // info.last_merged_version_.value_ = version;
  // info.all_merged_version_.value_ = version;

  for (int64_t i = 0; i < zone_count_; ++i) {
    zone_infos_[i] = info;
    zone_infos_[i].zone_ = to_cstring(i + 1);
    zone_infos_[i].region_.info_ = to_cstring(i/2 + 10);
  }

  inited_ = true;
  loaded_ = true;
}

ObZoneInfo *FakeZoneManager::locate_zone(const ObZone &zone)
{

  ObZoneInfo *info = NULL;
  for (int64_t i = 0; i < zone_count_; ++i) {
    if (zone_infos_[i].zone_ == zone) {
      info = &zone_infos_[i];
    }
  }
  return info;
}

int FakeZoneManager::start_zone_merge(const ObZone &zone)
{
  ObZoneInfo *info = locate_zone(zone);
  if (info) {
    // info->broadcast_version_.value_++;
    // info->merge_start_time_.value_ = ::oceanbase::common::ObTimeUtility::current_time();
    // info->last_merged_time_.value_ = 0;
    return OB_SUCCESS;
  }
  return OB_ENTRY_NOT_EXIST;
}

int FakeZoneManager::finish_zone_merge(const ObZone &zone, const int64_t merged_version,
    const int64_t all_merged_version)
{
  UNUSED(merged_version);
  UNUSED(all_merged_version);
  ObZoneInfo *info = locate_zone(zone);
  if (info) {
    // info->last_merged_version_.value_ = merged_version;
    // info->last_merged_time_.value_ = ::oceanbase::common::ObTimeUtility::current_time();
    // info->all_merged_version_.value_ = all_merged_version;
    // info->is_merge_timeout_.value_ = 0;
    return OB_SUCCESS;
  }
  return OB_ENTRY_NOT_EXIST;
}

int FakeZoneManager::set_zone_merge_timeout(const ObZone &zone)
{
  ObZoneInfo *info = locate_zone(zone);
  if (info) {
    // info->is_merge_timeout_.value_ = 1;
    return OB_SUCCESS;
  }
  return OB_ENTRY_NOT_EXIST;
}

int FakeZoneManager::set_zone_merging(const ObZone &zone)
{
  int ret = OB_ENTRY_NOT_EXIST;
  ObZoneInfo *info = locate_zone(zone);
  if (info) {
    // info->is_merging_.value_ = 1;
    ret = OB_SUCCESS;
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
