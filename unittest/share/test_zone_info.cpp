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

#define USING_LOG_PREFIX SHARE
#include "lib/stat/ob_session_stat.h"
#include "share/ob_zone_info.h"
#include "share/ob_zone_table_operation.h"
#include <gtest/gtest.h>
#include "schema/db_initializer.h"


namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share::schema;

class TestZoneInfo : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}

protected:
  DBInitializer db_initer_;
  ObGlobalInfo global_info_;
};

void TestZoneInfo::SetUp()
{
  int ret = db_initer_.init();

  ASSERT_EQ(OB_SUCCESS, ret);

  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_TRUE(global_info_.is_valid());

  ret = ObZoneTableOperation::insert_global_info(db_initer_.get_sql_proxy(), global_info_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestZoneInfo, create_global_info)
{
  LOG_INFO("zone info size", K(sizeof(ObGlobalInfo)), K(sizeof(ObZoneInfo)));
}

TEST_F(TestZoneInfo, zone_item_update)
{
  int ret = global_info_.lease_info_version_.update(
      db_initer_.get_sql_proxy(), global_info_.zone_, 1, "a");
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, global_info_.lease_info_version_.value_);
  ASSERT_EQ(1, global_info_.lease_info_version_);
  ASSERT_STREQ("a", global_info_.lease_info_version_.info_.ptr());

  // test copy construct
  ObGlobalInfo info2 = global_info_;
  ASSERT_EQ(1, info2.lease_info_version_.value_);
  ASSERT_EQ(1, info2.lease_info_version_);
  ASSERT_STREQ("a", info2.lease_info_version_.info_.ptr());

  global_info_.reset();

  ret = ObZoneTableOperation::load_global_info(db_initer_.get_sql_proxy(), global_info_);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(1, global_info_.lease_info_version_.value_);
  ASSERT_STREQ("a", global_info_.lease_info_version_.info_.ptr());

  // update in transaction
  ObZoneItemTransUpdater updater;
  ret = updater.start(db_initer_.get_sql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  ret = global_info_.frozen_version_.update(updater, global_info_.zone_, 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = global_info_.frozen_time_.update(updater, global_info_.zone_, now);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(2, global_info_.frozen_version_.value_);
  ASSERT_EQ(now, global_info_.frozen_time_.value_);

  ret = updater.end(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, global_info_.frozen_version_.value_);
  ASSERT_EQ(now, global_info_.frozen_time_.value_);

  ASSERT_EQ(OB_SUCCESS, updater.start(db_initer_.get_sql_proxy()));
  ASSERT_EQ(OB_SUCCESS, global_info_.frozen_version_.update(updater, global_info_.zone_, 3));
  ASSERT_EQ(OB_SUCCESS, global_info_.frozen_time_.update(updater, global_info_.zone_, now + 1));
  ASSERT_EQ(OB_SUCCESS, global_info_.frozen_version_.update(updater, global_info_.zone_, 4));

  ASSERT_EQ(4, global_info_.frozen_version_.value_);
  ASSERT_EQ(now + 1, global_info_.frozen_time_.value_);

  ret = updater.end(false);
  ASSERT_EQ(OB_SUCCESS, ret);

  // value rollbacked after transaction rollback.
  ASSERT_EQ(2, global_info_.frozen_version_.value_);
  ASSERT_EQ(now, global_info_.frozen_time_.value_);

  LOG_INFO("global_info", K_(global_info));
}

TEST_F(TestZoneInfo, zone_info)
{
  const char *zone = "ZJ.HZ.XH";
  ObZoneInfo zone_info;
  zone_info.zone_ = zone;
  zone_info.status_.value_ = ObZoneStatus::ACTIVE;
  ASSERT_STREQ("ACTIVE", zone_info.get_status_str());
  ASSERT_TRUE(zone_info.is_valid());
  int ret = ObZoneTableOperation::insert_zone_info(db_initer_.get_sql_proxy(), zone_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = zone_info.last_merged_time_.update(db_initer_.get_sql_proxy(), zone_info.zone_, 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, zone_info.last_merged_time_);
  zone_info.reset();
  zone_info.zone_ = zone;
  ret = ObZoneTableOperation::load_zone_info(db_initer_.get_sql_proxy(), zone_info);
  ASSERT_EQ(2, zone_info.last_merged_time_);

  LOG_INFO("zone_info", K(zone_info));

  ObArray<ObZone> zone_list;
  ASSERT_EQ(OB_SUCCESS, ObZoneTableOperation::get_zone_list(
  db_initer_.get_sql_proxy(), zone_list));
  ASSERT_EQ(1, zone_list.size());

  ASSERT_EQ(OB_SUCCESS, ObZoneTableOperation::remove_zone_info(
      db_initer_.get_sql_proxy(), zone));

  zone_list.reuse();
  ASSERT_EQ(OB_SUCCESS, ObZoneTableOperation::get_zone_list(
  db_initer_.get_sql_proxy(), zone_list));
  ASSERT_EQ(0, zone_list.size());
}

TEST_F(TestZoneInfo, get_lease_info)
{
  const char *zone = "ZJ.HZ.XH";
  ObZoneInfo zone_info;
  zone_info.zone_ = zone;
  zone_info.status_.value_ = ObZoneStatus::ACTIVE;
  int ret = ObZoneTableOperation::insert_zone_info(db_initer_.get_sql_proxy(), zone_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObZoneItemTransUpdater updater;
  updater.start(db_initer_.get_sql_proxy());
  const ObZone &gz = global_info_.zone_;
  global_info_.lease_info_version_.update(updater, gz, 4);
  global_info_.config_version_.update(updater, gz, 3);
  global_info_.frozen_version_.update(updater, gz, 2);
  global_info_.try_frozen_version_.update(updater, gz, 2);
  global_info_.global_broadcast_version_.update(updater, gz, 2);

  zone_info.broadcast_version_.update(updater, zone, 2);
  zone_info.last_merged_version_.update(updater, zone, 2);
  zone_info.suspend_merging_.update(updater, zone, 1);

  updater.end(true);

  ObZoneLeaseInfo lease_info;
  lease_info.zone_ = zone;
  ret = ObZoneTableOperation::get_zone_lease_info(db_initer_.get_sql_proxy(), lease_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, lease_info.lease_info_version_);
  ASSERT_EQ(3, lease_info.config_version_);
  ASSERT_EQ(2, lease_info.broadcast_version_);
  ASSERT_EQ(2, lease_info.last_merged_version_);
  ASSERT_TRUE(lease_info.suspend_merging_);
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_2200);
  return RUN_ALL_TESTS();
}
