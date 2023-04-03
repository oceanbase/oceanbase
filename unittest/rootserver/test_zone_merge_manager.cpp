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
#include "rootserver/freeze/ob_zone_merge_manager.h"
#include "share/ob_zone_merge_table_operator.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

class TestZoneMergeManager : public ::testing::Test
{
public:
  TestZoneMergeManager() {}
  virtual ~TestZoneMergeManager() {}
  virtual void SetUp();
  virtual void TearDown() {}
protected:
  virtual void check_info(const ObZoneMergeInfo &l,
                          const ObZoneMergeInfo &r);
  DBInitializer db_initer_;
  ObZoneMergeManager zone_merge_mgr_;
  ObGlobalMergeInfo global_merge_info_;
  ObArray<ObZoneMergeInfo> zone_merge_infos_;

  const static uint64_t DEFAULT_TENANT_ID = 1001;

private:
  void build_global_merge_info();
};

void TestZoneMergeManager::build_global_merge_info()
{
  global_merge_info_.tenant_id_ = DEFAULT_TENANT_ID;
  global_merge_info_.zone_ = "";
  global_merge_info_.try_frozen_version_.value_ = 1;
  global_merge_info_.frozen_version_.value_ = 1;
  global_merge_info_.frozen_time_.value_ = 99999;
  global_merge_info_.global_broadcast_version_.value_ = 1;
  global_merge_info_.last_merged_version_.value_ = 1;
  global_merge_info_.is_merge_error_.value_ = 0;
}

void TestZoneMergeManager::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, db_initer_.init());

  const bool only_core_tables = false;
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_system_table(only_core_tables));
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_tenant_space_tables(DEFAULT_TENANT_ID));

  build_global_merge_info();
  ASSERT_EQ(OB_SUCCESS, ObZoneMergeTableOperator::insert_global_merge_info(
      db_initer_.get_sql_proxy(), global_merge_info_));
  ObZone zones[] = { "zone1", "zone2" };
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(zones) / sizeof(ObZone)); ++i) {
    ObZoneMergeInfo info;
    info.tenant_id_ = DEFAULT_TENANT_ID;
    info.zone_ = zones[i];
    ASSERT_EQ(OB_SUCCESS, ObZoneMergeTableOperator::insert_zone_merge_info(
            db_initer_.get_sql_proxy(), info));
    ASSERT_EQ(OB_SUCCESS, zone_merge_infos_.push_back(info));
  }
}

void TestZoneMergeManager::check_info(
    const ObZoneMergeInfo &l,
    const ObZoneMergeInfo &r)
{
  ASSERT_EQ(l.tenant_id_, r.tenant_id_);
  ASSERT_EQ(l.zone_, r.zone_);
  ASSERT_EQ(l.broadcast_version_.value_, r.broadcast_version_.value_);
  ASSERT_EQ(l.last_merged_version_.value_, r.last_merged_version_.value_);
  ASSERT_EQ(l.last_merged_time_.value_, r.last_merged_time_.value_);
  ASSERT_EQ(l.merge_start_time_.value_, r.merge_start_time_.value_);
  ASSERT_EQ(l.is_merge_timeout_.value_, r.is_merge_timeout_.value_);
}

// TEST_F(TestZoneMergeManager, common)
// {
//   ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.init(DEFAULT_TENANT_ID, db_initer_.get_sql_proxy()));
//   ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.reload());
//   ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.check_inner_stat());
//   int64_t zone_count = zone_merge_mgr_.get_zone_count();
//   ASSERT_EQ(2, zone_count);
//   ObArray<ObZoneMergeInfo> infos;
//   ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_zone(infos));
//   ASSERT_EQ(2, infos.count());

//   ObZoneMergeInfo info;
//   ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_zone(0, info));
//   check_info(zone_merge_infos_[0], info);
//   ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_zone(1, info));
//   check_info(zone_merge_infos_[1], info);

//   info.reset();
//   info.zone_ = "zone1";
//   info.tenant_id_ = DEFAULT_TENANT_ID;
//   ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_zone(info));
//   check_info(zone_merge_infos_[0], info);

//   ObGlobalMergeInfo global_merge_info;
//   infos.reset();
//   ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_snapshot(global_merge_info, infos));
//   ASSERT_EQ(2, infos.count());
//   check_info(zone_merge_infos_[0], infos[0]);
//   check_info(zone_merge_infos_[1], infos[1]);
// }

TEST_F(TestZoneMergeManager, merge)
{
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.init(DEFAULT_TENANT_ID, db_initer_.get_sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.reload());

  // increase frozen_version
  int64_t frozen_time = 0;
  int64_t frozen_version = 0;
  int64_t try_frozen_version = 0;
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_global_frozen_info(frozen_version, frozen_time));
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_global_try_frozen_version(try_frozen_version));
  ASSERT_EQ(frozen_version, try_frozen_version);
  ASSERT_EQ(OB_INVALID_ARGUMENT, zone_merge_mgr_.set_global_try_frozen_version(9));
  try_frozen_version += 1;
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.set_global_try_frozen_version(try_frozen_version));
  frozen_time = ObTimeUtility::current_time();
  frozen_version = try_frozen_version;
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.set_global_frozen_info(frozen_version, frozen_time));
  int64_t got_frozen_version = 0;
  int64_t got_frozen_time = 0;
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_global_frozen_info(got_frozen_version, got_frozen_time));
  ASSERT_EQ(frozen_version, got_frozen_version);
  ASSERT_EQ(frozen_time, got_frozen_time);

  bool in_merge = false;
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.is_in_merge(in_merge));
  ASSERT_FALSE(in_merge);
  int64_t next_version = 0;
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.generate_next_global_broadcast_version(next_version));
  ASSERT_EQ(frozen_version, next_version);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.is_in_merge(in_merge));
  ASSERT_TRUE(in_merge);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.try_update_global_last_merged_version());
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.is_in_merge(in_merge));
  ASSERT_TRUE(in_merge);

  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.set_zone_merging("zone1"));
  {
    ObZoneMergeInfo info;
    info.zone_ = "zone1";
    info.tenant_id_ = DEFAULT_TENANT_ID;
    ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_zone(info));
    ASSERT_EQ(1, info.is_merging_);
  }

  ASSERT_NE(OB_SUCCESS, zone_merge_mgr_.set_zone_merging("")); // invalid argument
  ASSERT_NE(OB_SUCCESS, zone_merge_mgr_.start_zone_merge("zone_not_exit")); // OB_ENTRY_NOT_EXIST
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.start_zone_merge("zone1"));
  ASSERT_NE(OB_SUCCESS, zone_merge_mgr_.finish_zone_merge("", 2, 2));
  ASSERT_NE(OB_SUCCESS, zone_merge_mgr_.finish_zone_merge("zone1", 20, 20)); // last_merged_version not continue
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.finish_zone_merge("zone1", 2, 2));
  { // finish merge zone will clear merging flag
    ObZoneMergeInfo info;
    info.zone_ = "zone1";
    info.tenant_id_ = DEFAULT_TENANT_ID;
    ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_zone(info));
    ASSERT_EQ(0, info.is_merging_);
  }
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.try_update_global_last_merged_version());
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.is_in_merge(in_merge));
  ASSERT_TRUE(in_merge);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.start_zone_merge("zone2"));
  { // auto set merging flag when start zone merge
    ObZoneMergeInfo info;
    info.zone_ = "zone2";
    info.tenant_id_ = DEFAULT_TENANT_ID;
    ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_zone(info));
    ASSERT_EQ(1, info.is_merging_);
  }

  ASSERT_NE(OB_SUCCESS, zone_merge_mgr_.set_zone_merge_timeout(""));
  ASSERT_NE(OB_SUCCESS, zone_merge_mgr_.set_zone_merge_timeout("zone_not_exit"));
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.set_zone_merge_timeout("zone2"));
  { // set merge timeout clear merging flag too
    ObZoneMergeInfo info;
    info.zone_ = "zone2";
    info.tenant_id_ = DEFAULT_TENANT_ID;
    ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_zone(info));
    ASSERT_EQ(0, info.is_merging_);
  }
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.finish_zone_merge("zone2", 2, 2));
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.try_update_global_last_merged_version());
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.is_in_merge(in_merge));
  ASSERT_FALSE(in_merge); // all zone finish merge

  // suspend && resume
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.suspend_zone_merge("zone1"));
  {
    ObZoneMergeInfo info;
    info.zone_ = "zone1";
    info.tenant_id_ = DEFAULT_TENANT_ID;
    ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_zone(info));
    ASSERT_EQ(1, info.suspend_merging_);

    ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.resume_zone_merge("zone1"));
    ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_zone(info));
    ASSERT_EQ(0, info.suspend_merging_);
  }
  ASSERT_EQ(OB_ZONE_INFO_NOT_EXIST, zone_merge_mgr_.suspend_zone_merge("not_exist_zone"));
}

TEST_F(TestZoneMergeManager, global_merge_status)
{
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.init(DEFAULT_TENANT_ID, db_initer_.get_sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.reload());

  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_IDLE, zone_merge_mgr_.global_merge_info_.merge_status_);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.reset_global_merge_status());
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_IDLE, zone_merge_mgr_.global_merge_info_.merge_status_);

  zone_merge_mgr_.global_merge_info_.try_frozen_version_.value_ = 2;
  zone_merge_mgr_.global_merge_info_.frozen_version_.value_ = 2;
  zone_merge_mgr_.global_merge_info_.global_broadcast_version_.value_ = 2;

  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.reset_global_merge_status());
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_MERGING, zone_merge_mgr_.global_merge_info_.merge_status_);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.start_zone_merge("zone1"));
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_MERGING, zone_merge_mgr_.global_merge_info_.merge_status_);
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_MERGING, zone_merge_mgr_.zone_merge_infos_[0].merge_status_);

  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.set_merge_error(1));
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_ERROR, zone_merge_mgr_.global_merge_info_.merge_status_);

  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.set_merge_error(0));
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_MERGING, zone_merge_mgr_.global_merge_info_.merge_status_);

  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.finish_zone_merge("zone1", 2, 1));
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_MERGING, zone_merge_mgr_.global_merge_info_.merge_status_);
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_MOST_MERGED, zone_merge_mgr_.zone_merge_infos_[0].merge_status_);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.finish_zone_merge("zone1", 2, 2));
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_IDLE, zone_merge_mgr_.zone_merge_infos_[0].merge_status_);

  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.start_zone_merge("zone2"));
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_MERGING, zone_merge_mgr_.global_merge_info_.merge_status_);
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_MERGING, zone_merge_mgr_.zone_merge_infos_[1].merge_status_);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.set_zone_merge_timeout("zone2"));
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_TIMEOUT, zone_merge_mgr_.global_merge_info_.merge_status_);
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_TIMEOUT, zone_merge_mgr_.zone_merge_infos_[1].merge_status_);

  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.finish_zone_merge("zone2", 2, 2));
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.try_update_global_last_merged_version());
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_IDLE, zone_merge_mgr_.global_merge_info_.merge_status_);
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_IDLE, zone_merge_mgr_.zone_merge_infos_[1].merge_status_);

  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.reset_global_merge_status());
  ASSERT_EQ(ObZoneMergeInfo::MERGE_STATUS_IDLE, zone_merge_mgr_.global_merge_info_.merge_status_);
}

} // namespace rootserver
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_2200);
  return RUN_ALL_TESTS();
}