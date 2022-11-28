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

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "ob_rs_test_utils.h"
#include "fake_zone_merge_manager.h"
#include "rootserver/freeze/ob_tenant_all_zone_merge_strategy.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"
#include "share/partition_table/fake_part_property_getter.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/string/ob_string.h"
#include "common/ob_zone_type.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace share;
using namespace share::schema;
using namespace share::host;
using namespace obrpc;
using ::testing::_;
namespace rootserver
{
class TestTenantAllZoneMergeStrategy :  public testing::Test
{
public:
  TestTenantAllZoneMergeStrategy() {}
  ~TestTenantAllZoneMergeStrategy() {}
  virtual void SetUp();
  virtual void TearDown() {}

  void gen_zone_merge_info(const common::ObZone &zone,
                           const int64_t frozen_version,
                           const int64_t broadcast_version,
                           ObZoneMergeInfo &zone_merge_info);
  void gen_global_merge_info(const int64_t global_broadcast_version,
                             ObGlobalMergeInfo &global_merge_info);
public:
  const static uint64_t CUR_TENANT_ID = 1100;
  const static int64_t ZONE_COUNT = 5;
  ObArray<ObZone> zone_list_;
  ObMySQLProxy sql_proxy_;
  FakeZoneMergeManager zone_merge_mgr_;
};

void TestTenantAllZoneMergeStrategy::gen_zone_merge_info(
    const common::ObZone &zone,
    const int64_t frozen_version,
    const int64_t broadcast_version,
    ObZoneMergeInfo &zone_merge_info)
{
  zone_merge_info.tenant_id_ = CUR_TENANT_ID;
  zone_merge_info.zone_ = zone;
  zone_merge_info.frozen_scn_.set_scn(frozen_version);
  zone_merge_info.broadcast_scn_.set_scn(broadcast_version);
}

void TestTenantAllZoneMergeStrategy::gen_global_merge_info(
    const int64_t global_broadcast_version,
    ObGlobalMergeInfo &global_merge_info)
{
  global_merge_info.tenant_id_ = CUR_TENANT_ID;
  global_merge_info.global_broadcast_scn_.set_scn(global_broadcast_version);
}

void TestTenantAllZoneMergeStrategy::SetUp()
{
  const uint64_t tenant_id = CUR_TENANT_ID;
  zone_merge_mgr_.init(tenant_id, sql_proxy_);
  zone_merge_mgr_.set_is_loaded(true);

  ObZoneMergeInfo zone_merge_info;
  gen_zone_merge_info("ZONE1", 1, 1, zone_merge_info);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.add_zone_merge_info(zone_merge_info));
  gen_zone_merge_info("ZONE2", 1, 1, zone_merge_info);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.add_zone_merge_info(zone_merge_info));
  gen_zone_merge_info("ZONE3", 1, 1, zone_merge_info);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.add_zone_merge_info(zone_merge_info));
  gen_zone_merge_info("ZONE4", 1, 1, zone_merge_info);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.add_zone_merge_info(zone_merge_info));
  gen_zone_merge_info("ZONE5", 1, 1, zone_merge_info);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.add_zone_merge_info(zone_merge_info));

  ObGlobalMergeInfo global_merge_info;
  gen_global_merge_info(2, global_merge_info);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.set_global_merge_info(global_merge_info));
}

TEST_F(TestTenantAllZoneMergeStrategy, get_next_zone)
{
  const uint64_t tenant_id = CUR_TENANT_ID;
  ObTenantAllZoneMergeStrategy merge_strategy;
  ASSERT_EQ(OB_SUCCESS, merge_strategy.init(tenant_id, &zone_merge_mgr_));
  
  ObArray<ObZone> to_merge;
  ASSERT_EQ(OB_SUCCESS, merge_strategy.get_next_zone(to_merge));
  ASSERT_EQ(5, to_merge.size());

  // filter ZONE4 cuz its broadcast_version is equal to global_broadcast_version, that means
  // ZONE4 is in merging, no need to start merge again
  ObZoneMergeInfo zone_merge_info;
  gen_zone_merge_info("ZONE4", 2, 2, zone_merge_info);
  ASSERT_EQ(OB_SUCCESS, zone_merge_mgr_.update_zone_merge_info(zone_merge_info));
  to_merge.reuse();
  ASSERT_EQ(OB_SUCCESS, merge_strategy.get_next_zone(to_merge));
  ASSERT_EQ(4, to_merge.size());
}

} //namespace rootserver
} //namespace oceanbase

int main(int argc, char **argv)
{
  init_oblog_for_rs_test("test_tenant_all_zone_merge_strategy");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
