/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
#include "share/ob_global_merge_table_operator.h"

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
  global_merge_info_.is_merge_error_.value_ = 0;
  // NOTE: Old version-based fields (zone_, try_frozen_version_, frozen_version_,
  // frozen_time_, global_broadcast_version_, last_merged_version_) have been removed.
  // The new implementation uses SCN-based fields with default-constructed values.
}

void TestZoneMergeManager::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, db_initer_.init());

  const bool only_core_tables = false;
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_system_table(only_core_tables));
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_tenant_space_tables(DEFAULT_TENANT_ID));

  build_global_merge_info();
  ASSERT_EQ(OB_SUCCESS, ObGlobalMergeTableOperator::insert_global_merge_info(
      db_initer_.get_sql_proxy(), DEFAULT_TENANT_ID, global_merge_info_));
  ObZone zones[] = { "zone1", "zone2" };
  ObArray<ObZoneMergeInfo> infos_to_insert;
  for (int64_t i = 0; i < static_cast<int64_t>(sizeof(zones) / sizeof(ObZone)); ++i) {
    ObZoneMergeInfo info;
    info.tenant_id_ = DEFAULT_TENANT_ID;
    info.zone_ = zones[i];
    ASSERT_EQ(OB_SUCCESS, infos_to_insert.push_back(info));
    ASSERT_EQ(OB_SUCCESS, zone_merge_infos_.push_back(info));
  }
  ASSERT_EQ(OB_SUCCESS, ObZoneMergeTableOperator::insert_zone_merge_infos(
      db_initer_.get_sql_proxy(), DEFAULT_TENANT_ID, infos_to_insert));
}

void TestZoneMergeManager::check_info(
    const ObZoneMergeInfo &l,
    const ObZoneMergeInfo &r)
{
  ASSERT_EQ(l.tenant_id_, r.tenant_id_);
  ASSERT_EQ(l.zone_, r.zone_);
  ASSERT_EQ(l.broadcast_scn_.value_, r.broadcast_scn_.value_);
  ASSERT_EQ(l.last_merged_scn_.value_, r.last_merged_scn_.value_);
  ASSERT_EQ(l.last_merged_time_.value_, r.last_merged_time_.value_);
  ASSERT_EQ(l.merge_start_time_.value_, r.merge_start_time_.value_);
  // is_merge_timeout_ removed; merge timeout is no longer tracked per zone
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

TEST_F(TestZoneMergeManager, reload_and_get_status)
{
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.init(DEFAULT_TENANT_ID, db_initer_.get_sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.reload());

  // initial status should be IDLE
  ObGlobalMergeInfo::MergeStatus status = ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_MAX;
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_global_merge_status(status));
  ASSERT_EQ(ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_IDLE, status);

  // global snapshot: correct tenant_id, no merge error
  ObGlobalMergeInfo global_info;
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_snapshot(global_info));
  ASSERT_EQ(DEFAULT_TENANT_ID, global_info.tenant_id_);
  ASSERT_FALSE(global_info.is_merge_error());

  // default merge mode is MERGE_MODE_TENANT
  ObGlobalMergeInfo::MergeMode mode = ObGlobalMergeInfo::MergeMode::MERGE_MODE_MAX;
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.get_global_merge_mode(mode));
  ASSERT_EQ(ObGlobalMergeInfo::MergeMode::MERGE_MODE_TENANT, mode);

  // zone_merge_infos_ array is removed from ObZoneMergeManagerBase; zone rows are
  // accessed directly from the DB. Verify 2 rows exist as inserted in SetUp.
  ObArray<ObZoneMergeInfo> zone_infos;
  ASSERT_EQ(OB_SUCCESS, ObZoneMergeTableOperator::load_zone_merge_infos(
      db_initer_.get_sql_proxy(), DEFAULT_TENANT_ID, zone_infos));
  ASSERT_EQ(2, zone_infos.count());
}

TEST_F(TestZoneMergeManager, batch_update_all_zone_merge_info)
{
  // Mark all zones as MERGING in a single SQL (mirrors generate_next_global_broadcast_scn)
  ObZoneMergeInfo start_info;
  start_info.tenant_id_ = DEFAULT_TENANT_ID;
  start_info.is_merging_.set_val(1, true);
  start_info.set_merge_status(ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_MERGING, true);

  ASSERT_EQ(OB_SUCCESS, ObZoneMergeTableOperator::update_tenant_all_zone_merge_info(
      db_initer_.get_sql_proxy(), DEFAULT_TENANT_ID, start_info));

  // all zone rows should be updated
  ObArray<ObZoneMergeInfo> zone_infos;
  ASSERT_EQ(OB_SUCCESS, ObZoneMergeTableOperator::load_zone_merge_infos(
      db_initer_.get_sql_proxy(), DEFAULT_TENANT_ID, zone_infos));
  ASSERT_EQ(2, zone_infos.count());
  for (int64_t i = 0; i < zone_infos.count(); ++i) {
    ASSERT_EQ(1, zone_infos.at(i).is_merging_.get_value());
    ASSERT_EQ(ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_MERGING,
              zone_infos.at(i).merge_status());
  }

  // Mark all zones as IDLE in a single SQL (mirrors try_update_global_last_merged_scn)
  ObZoneMergeInfo finish_info;
  finish_info.tenant_id_ = DEFAULT_TENANT_ID;
  finish_info.is_merging_.set_val(0, true);
  finish_info.set_merge_status(ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_IDLE, true);

  ASSERT_EQ(OB_SUCCESS, ObZoneMergeTableOperator::update_tenant_all_zone_merge_info(
      db_initer_.get_sql_proxy(), DEFAULT_TENANT_ID, finish_info));

  zone_infos.reuse();
  ASSERT_EQ(OB_SUCCESS, ObZoneMergeTableOperator::load_zone_merge_infos(
      db_initer_.get_sql_proxy(), DEFAULT_TENANT_ID, zone_infos));
  ASSERT_EQ(2, zone_infos.count());
  for (int64_t i = 0; i < zone_infos.count(); ++i) {
    ASSERT_EQ(0, zone_infos.at(i).is_merging_.get_value());
    ASSERT_EQ(ObGlobalMergeInfo::MergeStatus::MERGE_STATUS_IDLE,
              zone_infos.at(i).merge_status());
  }
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