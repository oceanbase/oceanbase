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

#include "share/ob_zone_merge_info.h"
#include "share/ob_zone_merge_table_operator.h"
#include "lib/stat/ob_session_stat.h"
#include <gtest/gtest.h>
#include "schema/db_initializer.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share::schema;

class TestZoneMergeInfo : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}

private:
  void build_global_merge_info();

protected:
  const static uint64_t DEFAULT_TENANT_ID = 5;
  DBInitializer db_initer_;
  ObGlobalMergeInfo global_merge_info_;
};

void TestZoneMergeInfo::build_global_merge_info()
{
  global_merge_info_.tenant_id_ = DEFAULT_TENANT_ID;
  global_merge_info_.zone_ = "";
  global_merge_info_.try_frozen_version_.value_ = 5;
  global_merge_info_.frozen_scn_.value_ = 4;
  global_merge_info_.frozen_time_.value_ = 99999;
  global_merge_info_.global_broadcast_scn_.value_ = 5;
  global_merge_info_.last_merged_scn_.value_ = 4;
  global_merge_info_.is_merge_error_.value_ = 0;
}

void TestZoneMergeInfo::SetUp()
{
  int ret = db_initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = db_initer_.create_tenant_space_tables(DEFAULT_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  build_global_merge_info();
  ASSERT_TRUE(global_merge_info_.is_valid());
  ret = ObZoneMergeTableOperator::insert_global_merge_info(db_initer_.get_sql_proxy(), global_merge_info_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestZoneMergeInfo, global_merge_info)
{
  ObGlobalMergeInfo tmp_merge_info;
  // get existed global_merge_info
  tmp_merge_info.tenant_id_ = global_merge_info_.tenant_id_;
  tmp_merge_info.zone_ = global_merge_info_.zone_;
  int ret = ObZoneMergeTableOperator::load_global_merge_info(db_initer_.get_sql_proxy(), tmp_merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(tmp_merge_info.frozen_time_.value_, global_merge_info_.frozen_time_.value_);

  // update global_merge_info in transaction
  const int64_t ori_frozen_version = global_merge_info_.frozen_scn_.value_;
  const int64_t ori_frozen_time = global_merge_info_.frozen_time_.value_;
  ObZoneMergeInfoItemTransUpdater updater;
  ret = updater.start(db_initer_.get_sql_proxy(), global_merge_info_.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t now = ::oceanbase::common::ObTimeUtility::current_time();
  ret = global_merge_info_.frozen_scn_.update(updater, global_merge_info_.tenant_id_,
        global_merge_info_.zone_, ori_frozen_version + 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = global_merge_info_.frozen_time_.update(updater, global_merge_info_.tenant_id_, 
        global_merge_info_.zone_, ori_frozen_time + 1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(ori_frozen_version + 1, global_merge_info_.frozen_scn_.value_);
  ASSERT_EQ(ori_frozen_time + 1, global_merge_info_.frozen_time_.value_);

  ret = updater.end(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  
  tmp_merge_info.reset();
  tmp_merge_info.tenant_id_ = global_merge_info_.tenant_id_;
  tmp_merge_info.zone_ = global_merge_info_.zone_;
  ret = ObZoneMergeTableOperator::load_global_merge_info(db_initer_.get_sql_proxy(), tmp_merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(tmp_merge_info.frozen_scn_.value_, global_merge_info_.frozen_scn_.value_);
  ASSERT_EQ(tmp_merge_info.frozen_time_.value_, global_merge_info_.frozen_time_.value_);
}

TEST_F(TestZoneMergeInfo, zone_merge_info)
{
  const char *zone = "ZJ.HZ.XH";
  ObZoneMergeInfo zone_merge_info;
  zone_merge_info.tenant_id_ = DEFAULT_TENANT_ID;
  zone_merge_info.zone_ = zone;
  zone_merge_info.last_merged_scn_.value_ = 100;
  int ret = ObZoneMergeTableOperator::insert_zone_merge_info(db_initer_.get_sql_proxy(), zone_merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = zone_merge_info.last_merged_scn_.update(db_initer_.get_sql_proxy(), zone_merge_info.tenant_id_,
        zone_merge_info.zone_, 101);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(101, zone_merge_info.last_merged_scn_);

  zone_merge_info.reset();
  zone_merge_info.tenant_id_ = DEFAULT_TENANT_ID;
  zone_merge_info.zone_ = zone;
  ASSERT_NE(101, zone_merge_info.last_merged_scn_);
  ret = ObZoneMergeTableOperator::load_zone_merge_info(db_initer_.get_sql_proxy(), zone_merge_info);
  ASSERT_EQ(101, zone_merge_info.last_merged_scn_);
}

TEST_F(TestZoneMergeInfo, batch_execute)
{
  // (1) batch load zone_merge_info
  ObZoneMergeInfo info1;
  const char *zone1 = "ZJ1";
  info1.tenant_id_ = OB_SYS_TENANT_ID;
  info1.zone_ = zone1;
  info1.last_merged_scn_.value_ = 101;
  int ret = ObZoneMergeTableOperator::insert_zone_merge_info(db_initer_.get_sql_proxy(), info1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObZoneMergeInfo info2;
  const char *zone2 = "ZJ2";
  info2.tenant_id_ = OB_SYS_TENANT_ID;
  info2.zone_ = zone2;
  info2.last_merged_scn_.value_ = 102;
  ret = ObZoneMergeTableOperator::insert_zone_merge_info(db_initer_.get_sql_proxy(), info2);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObZoneMergeInfo info3;
  const char *zone3 = "ZJ3";
  info3.tenant_id_ = OB_SYS_TENANT_ID;
  info3.zone_ = zone3;
  info3.last_merged_scn_.value_ = 103;
  ret = ObZoneMergeTableOperator::insert_zone_merge_info(db_initer_.get_sql_proxy(), info3);
  ASSERT_EQ(OB_SUCCESS, ret);

  info1.last_merged_scn_.value_ = 0;
  info2.last_merged_scn_.value_ = 0;
  info3.last_merged_scn_.value_ = 0;
  ObArray<ObZoneMergeInfo> infos;
  infos.push_back(info1);
  infos.push_back(info2);
  infos.push_back(info3);

  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_GT(100, infos.at(i).last_merged_scn_.value_);
  }
  ret = ObZoneMergeTableOperator::load_zone_merge_infos(db_initer_.get_sql_proxy(), infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_LT(100, infos.at(i).last_merged_scn_.value_);
  }

  // (2) create global_merge_info & zone_merge_info in one trans
  ObGlobalMergeInfo global_merge_info;
  global_merge_info = global_merge_info_;
  global_merge_info.tenant_id_ = OB_SYS_TENANT_ID;

  ObZoneMergeInfo info4;
  const char *zone4 = "ZJ4";
  info4.tenant_id_ = OB_SYS_TENANT_ID;
  info4.zone_ = zone4;
  info4.last_merged_scn_.value_ = 104;

  ObZoneMergeInfo info5;
  const char *zone5 = "ZJ5";
  info5.tenant_id_ = OB_SYS_TENANT_ID;
  info5.zone_ = zone5;
  info5.last_merged_scn_.value_ = 105;

  infos.reset();
  infos.push_back(info4);
  infos.push_back(info5);
  ret = ObZoneMergeTableOperator::insert_tenant_merge_info(db_initer_.get_sql_proxy(), global_merge_info, infos);
  ASSERT_EQ(OB_SUCCESS, ret);
  
  ObGlobalMergeInfo tmp_global_info;
  tmp_global_info.tenant_id_ = global_merge_info.tenant_id_;
  tmp_global_info.zone_ = global_merge_info.zone_;
  ret = ObZoneMergeTableOperator::load_global_merge_info(db_initer_.get_sql_proxy(), tmp_global_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(tmp_global_info.try_frozen_version_.value_, global_merge_info.try_frozen_version_.value_);

  ObZoneMergeInfo tmp_info4;
  tmp_info4.tenant_id_ = info4.tenant_id_;
  tmp_info4.zone_ = info4.zone_;
  ret = ObZoneMergeTableOperator::load_zone_merge_info(db_initer_.get_sql_proxy(), tmp_info4);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(tmp_info4.last_merged_scn_.value_, info4.last_merged_scn_.value_);

  // (3) batch delete by tenant_id
  ret = ObZoneMergeTableOperator::delete_tenant_merge_info(db_initer_.get_sql_proxy(), OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  tmp_global_info.reset();
  tmp_global_info.tenant_id_ = global_merge_info.tenant_id_;
  tmp_global_info.zone_ = global_merge_info.zone_;
  ret = ObZoneMergeTableOperator::load_global_merge_info(db_initer_.get_sql_proxy(), tmp_global_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(tmp_global_info.try_frozen_version_.value_, global_merge_info.try_frozen_version_.value_);
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
