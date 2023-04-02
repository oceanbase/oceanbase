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

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#define private public
#include "lib/stat/ob_session_stat.h"
#include "../share/schema/db_initializer.h"
#include "lib/time/ob_time_utility.h"
#include "lib/container/ob_array_iterator.h"
#include "rootserver/ob_zone_manager.h"
#include "share/ob_zone_table_operation.h"
#include "rootserver/ob_leader_coordinator.h"
#include "mock_leader_coordinate.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver
{

class TestZoneManager : public ::testing::Test
{
public:
  TestZoneManager() {}
  virtual ~TestZoneManager() {}
  virtual void SetUp();
  virtual void TearDown() {}
protected:
  virtual void check_info(const ObZoneInfo &l,
                          const ObZoneInfo &r);
  DBInitializer db_initer_;
  MockLeaderCoordinator leader_coordinator_;
  ObZoneManager zone_mgr_;
  ObGlobalInfo global_info_;
  ObArray<ObZoneInfo> zone_infos_;
};

void TestZoneManager::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, db_initer_.init());

  const bool only_core_tables = false;
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_system_table(only_core_tables));
  ASSERT_EQ(OB_SUCCESS, db_initer_.fill_sys_stat_table());
  ObGlobalInfo global_info;
  global_info_ = global_info;
  ASSERT_EQ(OB_SUCCESS, ObZoneTableOperation::insert_global_info(
      db_initer_.get_sql_proxy(), global_info));
  ObZone zones[] = { "zone1", "zone2" };
  ObZoneStatus::Status statuses[] = { ObZoneStatus::ACTIVE, ObZoneStatus::INACTIVE };
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(zones) / sizeof(ObZone)); ++i) {
    ObZoneInfo info;
    info.zone_ = zones[i];
    info.status_.value_ = statuses[i];
    info.status_.info_ = ObZoneStatus::get_status_str(statuses[i]);
    ASSERT_EQ(OB_SUCCESS, ObZoneTableOperation::insert_zone_info(
        db_initer_.get_sql_proxy(), info));
    ASSERT_EQ(OB_SUCCESS, zone_infos_.push_back(info));
  }
}

void TestZoneManager::check_info(const ObZoneInfo &l,
                                 const ObZoneInfo &r)
{
  ASSERT_EQ(l.zone_, r.zone_);
  ASSERT_EQ(l.status_.value_, r.status_.value_);
  ASSERT_EQ(l.broadcast_version_.value_, r.broadcast_version_.value_);
  ASSERT_EQ(l.last_merged_version_.value_, r.last_merged_version_.value_);
  ASSERT_EQ(l.last_merged_time_.value_, r.last_merged_time_.value_);
  ASSERT_EQ(l.merge_start_time_.value_, r.merge_start_time_.value_);
  ASSERT_EQ(l.is_merge_timeout_.value_, r.is_merge_timeout_.value_);
}

TEST_F(TestZoneManager, common)
{
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.init(db_initer_.get_sql_proxy(), leader_coordinator_));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.reload());
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.check_inner_stat());
  int64_t zone_count = 0;
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone_count(zone_count));
  ASSERT_EQ(2, zone_count);
  ObArray<ObZoneInfo> infos;
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone(infos));
  ASSERT_EQ(2, infos.count());

  ObZoneInfo info;
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone(0, info));
  check_info(zone_infos_[0], info);
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone(1, info));
  check_info(zone_infos_[1], info);

  info.reset();
  info.zone_ = "zone1";
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone(info));
  check_info(zone_infos_[0], info);

  info.reset();
  info.zone_ = "zone1";
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_active_zone(info));
  check_info(zone_infos_[0], info);
  info.reset();
  info.zone_ = "zone2";
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, zone_mgr_.get_active_zone(info));

  ObZoneStatus::Status status = ObZoneStatus::ACTIVE;
  ObArray<ObZone> active_zones;
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone(status, active_zones));
  ASSERT_EQ(1, active_zones.count());
  ASSERT_EQ(ObZone("zone1"), active_zones.at(0));

  ObGlobalInfo global_info;
  infos.reset();
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_snapshot(global_info, infos));
  ASSERT_EQ(2, infos.count());
  check_info(zone_infos_[0], infos[0]);
  check_info(zone_infos_[1], infos[1]);

  bool zone_active = false;
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.check_zone_active("zone1", zone_active));
  ASSERT_TRUE(zone_active);
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.check_zone_active("zone2", zone_active));
  ASSERT_FALSE(zone_active);
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.check_zone_active("zone3", zone_active));
  ASSERT_FALSE(zone_active);

  int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.update_privilege_version(now));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.update_config_version(now));
  int64_t config_version = 0;
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_config_version(config_version));
  ASSERT_EQ(now, config_version);

  LOG_INFO("mgr", K_(zone_mgr));
}

TEST_F(TestZoneManager, admin)
{

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.init(db_initer_.get_sql_proxy(), leader_coordinator_));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.reload());

  ObZone zone = "new zone";
  ObRegion region = "hangzhou";
  ObIDC idc = "idc";
  ObZoneType zone_type = ObZoneType::ZONE_TYPE_READWRITE;
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.add_zone(zone, region, idc, zone_type));
  ASSERT_EQ(OB_ENTRY_EXIST, zone_mgr_.add_zone(zone, region, idc, zone_type));
  ASSERT_EQ(OB_INVALID_ARGUMENT, zone_mgr_.add_zone("", region, idc, zone_type));
  int64_t zone_count = 0;
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone_count(zone_count));
  ASSERT_EQ(3, zone_count);
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.stop_zone(zone));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.start_zone(zone));
  ASSERT_EQ(OB_INVALID_ARGUMENT, zone_mgr_.start_zone(""));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, zone_mgr_.start_zone("zone3"));

  ASSERT_EQ(OB_ZONE_STATUS_NOT_MATCH, zone_mgr_.delete_zone(zone));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.stop_zone(zone));
  ASSERT_EQ(OB_INVALID_ARGUMENT, zone_mgr_.stop_zone(""));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, zone_mgr_.stop_zone("zone3"));

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.delete_zone(zone));
  ASSERT_EQ(OB_INVALID_ARGUMENT, zone_mgr_.delete_zone(""));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, zone_mgr_.delete_zone("zone3"));
}

TEST_F(TestZoneManager, merge)
{
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.init(db_initer_.get_sql_proxy(), leader_coordinator_));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.reload());

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.start_zone("zone2"));

  // increase frozne_version
  int64_t frozen_version = 0;
  int64_t try_frozen_version = 0;
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_try_frozen_version(frozen_version, try_frozen_version));
  ASSERT_EQ(frozen_version, try_frozen_version);
  try_frozen_version += 1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, zone_mgr_.set_try_frozen_version(9));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.set_try_frozen_version(try_frozen_version));
  int64_t frozen_time = ObTimeUtility::current_time();
  frozen_version = try_frozen_version;
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.set_frozen_info(frozen_version, frozen_time));
  int64_t got_frozen_version = 0;
  int64_t got_frozen_time = 0;
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_frozen_info(got_frozen_version, got_frozen_time));
  ASSERT_EQ(frozen_version, got_frozen_version);
  ASSERT_EQ(frozen_time, got_frozen_time);

  // merge
  //ObArray<ObZone> zones;
  //ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_merge_list(zones));
  //ASSERT_EQ(0, zones.count());
  //ASSERT_EQ(OB_SUCCESS, zones.push_back("zone2"));
  //ASSERT_EQ(OB_SUCCESS, zones.push_back("zone1"));
  //ASSERT_EQ(OB_SUCCESS, zone_mgr_.set_merge_list(zones));
  //zones.reuse();
  //ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_merge_list(zones));
  //ASSERT_EQ(2, zones.count());
  //ASSERT_STREQ("zone2", zones.at(0).ptr());
  //ASSERT_STREQ("zone1", zones.at(1).ptr());

  bool in_merge = false;
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.is_in_merge(in_merge));
  ASSERT_FALSE(in_merge);
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.generate_next_global_broadcast_version());
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.is_in_merge(in_merge));
  ASSERT_TRUE(in_merge);
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.try_update_global_last_merged_version());
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.is_in_merge(in_merge));
  ASSERT_TRUE(in_merge);

  ASSERT_NE(OB_SUCCESS, zone_mgr_.set_zone_merging(""));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.set_zone_merging("zone1"));
  {
    ObZoneInfo info;
    info.zone_ = "zone1";
    ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone(info));
    ASSERT_EQ(1, info.is_merging_);
  }

  ASSERT_NE(OB_SUCCESS, zone_mgr_.start_zone_merge(""));
  ASSERT_NE(OB_SUCCESS, zone_mgr_.start_zone_merge("zone_not_exit"));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.start_zone_merge("zone1"));
  ASSERT_NE(OB_SUCCESS, zone_mgr_.finish_zone_merge("", 2, 2));
  ASSERT_NE(OB_SUCCESS, zone_mgr_.finish_zone_merge("zone1", 20, 20));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.finish_zone_merge("zone1", 2, 2));
  { // finish merge zone will clear merging flag
    ObZoneInfo info;
    info.zone_ = "zone1";
    ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone(info));
    ASSERT_EQ(0, info.is_merging_);
  }
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.try_update_global_last_merged_version());
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.is_in_merge(in_merge));
  ASSERT_TRUE(in_merge);
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.start_zone_merge("zone2"));
  { // auto set merging flag when start zone merge
    ObZoneInfo info;
    info.zone_ = "zone2";
    ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone(info));
    ASSERT_EQ(1, info.is_merging_);
  }

  ASSERT_NE(OB_SUCCESS, zone_mgr_.set_zone_merge_timeout(""));
  ASSERT_NE(OB_SUCCESS, zone_mgr_.set_zone_merge_timeout("zone_not_exit"));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.set_zone_merge_timeout("zone2"));
  { // set merge timeout clear merging flag too
    ObZoneInfo info;
    info.zone_ = "zone2";
    ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone(info));
    ASSERT_EQ(0, info.is_merging_);
  }
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.finish_zone_merge("zone2", 2, 2));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.try_update_global_last_merged_version());
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.is_in_merge(in_merge));
  ASSERT_FALSE(in_merge);

  // suspend && resume
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.suspend_merge("zone1"));
  {
    ObZoneInfo info;
    info.zone_ = "zone1";
    ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone(info));
    ASSERT_EQ(1, info.suspend_merging_);

    ASSERT_EQ(OB_SUCCESS, zone_mgr_.resume_merge(""));
    ASSERT_EQ(OB_SUCCESS, zone_mgr_.get_zone(info));
    ASSERT_EQ(0, info.suspend_merging_);
  }
  ASSERT_EQ(OB_ZONE_INFO_NOT_EXIST, zone_mgr_.suspend_merge("not_exist_zone"));
}

TEST_F(TestZoneManager, global_merge_status)
{
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.init(db_initer_.get_sql_proxy(), leader_coordinator_));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.reload());

  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_IDLE, zone_mgr_.global_info_.merge_status_);
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.reset_global_merge_status());
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_IDLE, zone_mgr_.global_info_.merge_status_);

  // merge round:
  // zone1: merge error
  // zone2: merge timeout
  zone_mgr_.global_info_.try_frozen_version_.value_ = 2;
  zone_mgr_.global_info_.frozen_version_.value_ = 2;
  zone_mgr_.global_info_.global_broadcast_version_.value_ = 2;

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.reset_global_merge_status());
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_MERGING, zone_mgr_.global_info_.merge_status_);
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.start_zone_merge("zone1"));
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_MERGING, zone_mgr_.global_info_.merge_status_);
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_MERGING, zone_mgr_.zone_infos_[0].merge_status_);

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.set_merge_error(1));
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_ERROR, zone_mgr_.global_info_.merge_status_);

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.set_merge_error(0));
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_MERGING, zone_mgr_.global_info_.merge_status_);

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.finish_zone_merge("zone1", 2, 1));
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_MERGING, zone_mgr_.global_info_.merge_status_);
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_MOST_MERGED, zone_mgr_.zone_infos_[0].merge_status_);
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.finish_zone_merge("zone1", 2, 2));
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_IDLE, zone_mgr_.zone_infos_[0].merge_status_);

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.start_zone_merge("zone2"));
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_MERGING, zone_mgr_.global_info_.merge_status_);
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_MERGING, zone_mgr_.zone_infos_[1].merge_status_);
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.set_zone_merge_timeout("zone2"));
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_TIMEOUT, zone_mgr_.global_info_.merge_status_);
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_TIMEOUT, zone_mgr_.zone_infos_[1].merge_status_);

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.finish_zone_merge("zone2", 2, 2));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.try_update_global_last_merged_version());
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_INDEXING, zone_mgr_.global_info_.merge_status_);
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_IDLE, zone_mgr_.zone_infos_[1].merge_status_);

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.reset_global_merge_status());
  ASSERT_EQ(ObZoneInfo::MERGE_STATUS_IDLE, zone_mgr_.global_info_.merge_status_);
}

// BUG:
// increase all_merged_version before last_merged_version increased fail.
TEST_F(TestZoneManager, finish_zone_merge_bug)
{
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.init(db_initer_.get_sql_proxy(), leader_coordinator_));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.reload());

  // merge version 2
  zone_mgr_.global_info_.try_frozen_version_.value_ = 2;
  zone_mgr_.global_info_.frozen_version_.value_ = 2;
  zone_mgr_.global_info_.global_broadcast_version_.value_ = 2;

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.start_zone_merge("zone1"));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.start_zone_merge("zone2"));

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.finish_zone_merge("zone1", 2, 1));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.finish_zone_merge("zone2", 2, 1));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.try_update_global_last_merged_version());

  // merge version 3
  zone_mgr_.global_info_.try_frozen_version_.value_ = 3;
  zone_mgr_.global_info_.frozen_version_.value_ = 3;
  zone_mgr_.global_info_.global_broadcast_version_.value_ = 3;

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.start_zone_merge("zone1"));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.start_zone_merge("zone2"));

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.finish_zone_merge("zone1", 2, 2));

  ASSERT_EQ(OB_SUCCESS, zone_mgr_.finish_zone_merge("zone1", 3, 3));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.finish_zone_merge("zone2", 3, 3));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.try_update_global_last_merged_version());

  ASSERT_EQ(3L, zone_mgr_.global_info_.last_merged_version_.value_);
}

TEST_F(TestZoneManager, reload_all_zone_inactive)
{
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.init(db_initer_.get_sql_proxy(), leader_coordinator_));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.reload());
  ASSERT_EQ(OB_OP_NOT_ALLOW, zone_mgr_.stop_zone("zone1"));
  int64_t rows = 0;
  ASSERT_EQ(OB_SUCCESS, db_initer_.get_sql_proxy().write("update __all_zone set value = 1, info = 'INACTIVE' where name = 'status'", rows));
  ASSERT_EQ(OB_SUCCESS, zone_mgr_.reload());
}

TEST_F(TestZoneManager, check_merge_order)
{
  ObArray<ObZone> merge_list;
  //ObString list_str(merge_order_str.length(), merge_order_str.ptr());
  int ret = OB_SUCCESS;
  ObString list_str("zone1,,zone2,,");
  ObString zone_str;
  ObString zone;
  bool split_end = false;
  while (!split_end && OB_SUCCESS == ret) {
    zone_str = list_str.split_on(',');
    //if (zone_str.empty() && NULL == zone_str.ptr()) {
    if (zone_str.empty()) {
      split_end = true;
      zone_str = list_str;
    }
    zone = zone_str.trim();
    if (!zone.empty()) {
      if (OB_FAIL(merge_list.push_back(zone))) {
        LOG_WARN("push back to array failed", K(ret));
      } else {
        LOG_INFO("push back zone", K(zone));
      }
    }
  }

}

}//end namespace rootserver
}//end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
