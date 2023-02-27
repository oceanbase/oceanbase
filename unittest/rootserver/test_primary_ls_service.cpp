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
#define  private public
#include "rootserver/ob_common_ls_service.h"
namespace oceanbase {
using namespace common;
using namespace share;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace rootserver{
//multiclustermanage
class TestPrimaryLSService : public testing::Test
{
public:
  TestPrimaryLSService() {}
  virtual ~TestPrimaryLSService(){}
  virtual void SetUp() {};
  virtual void TearDown() {}
  virtual void TestBody() {}
}
;

TEST_F(TestPrimaryLSService, zone_balance)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> primary_zone_array;
  share::ObLSPrimaryZoneInfoArray primary_zone_infos;
  ObArray<ObZone> ls_primary_zone;
  ObSEArray<uint64_t, 3> count_group_by_zone;

  //case 1, set primary_zone to 'z1', ls to 'z2'
  ObZone z1("z1");
  ObZone z2("z2");
  ObZone z3("z3");
  ObZone z4("z4");
  ObZone z5("z5");
  ObLSPrimaryZoneInfo info;
  ObLSID ls_id(1001);
  ret = info.init(1002, 1001, ls_id, z2, z1.str());
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_array.push_back(z1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = ObCommonLSService::set_ls_to_primary_zone(primary_zone_array, primary_zone_infos, ls_primary_zone, count_group_by_zone);
  ASSERT_EQ(ret, OB_SUCCESS);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(z1, ls_primary_zone.at(0));
  ASSERT_EQ(1, count_group_by_zone.at(0));
  ret = ObCommonLSService::balance_ls_primary_zone(primary_zone_array, ls_primary_zone, count_group_by_zone);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(z1, ls_primary_zone.at(0));
  ASSERT_EQ(1, count_group_by_zone.at(0));

  //case2, primary zone is z1, z2, ls to z2, z2;
  ls_primary_zone.reset();
  count_group_by_zone.reset();
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_array.push_back(z2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = ObCommonLSService::set_ls_to_primary_zone(primary_zone_array, primary_zone_infos, ls_primary_zone, count_group_by_zone);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(z2, ls_primary_zone.at(0));
  ASSERT_EQ(z2, ls_primary_zone.at(1));
  ASSERT_EQ(0, count_group_by_zone.at(0));
  ASSERT_EQ(2, count_group_by_zone.at(1));
  ret = ObCommonLSService::balance_ls_primary_zone(primary_zone_array, ls_primary_zone, count_group_by_zone);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(z1, ls_primary_zone.at(0));
  ASSERT_EQ(z2, ls_primary_zone.at(1));
  ASSERT_EQ(1, count_group_by_zone.at(0));
  ASSERT_EQ(1, count_group_by_zone.at(1));
  
  //case 3 primary zone to z1, z3, ls to z2, z2;
  ls_primary_zone.reset();
  count_group_by_zone.reset();
  primary_zone_array.reset();
  ret = primary_zone_array.push_back(z1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_array.push_back(z3);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = ObCommonLSService::set_ls_to_primary_zone(primary_zone_array, primary_zone_infos, ls_primary_zone, count_group_by_zone);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(z1, ls_primary_zone.at(0));
  ASSERT_EQ(z3, ls_primary_zone.at(1));
  ASSERT_EQ(1, count_group_by_zone.at(0));
  ASSERT_EQ(1, count_group_by_zone.at(1));
  ret = ObCommonLSService::balance_ls_primary_zone(primary_zone_array, ls_primary_zone, count_group_by_zone);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(z1, ls_primary_zone.at(0));
  ASSERT_EQ(z3, ls_primary_zone.at(1));
  ASSERT_EQ(1, count_group_by_zone.at(0));
  ASSERT_EQ(1, count_group_by_zone.at(1)); 

  //case 3 primary zone to z1,z3,z2, ls to z2, z2;
  ls_primary_zone.reset();
  count_group_by_zone.reset();
  ret = primary_zone_array.push_back(z2);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = ObCommonLSService::set_ls_to_primary_zone(primary_zone_array, primary_zone_infos, ls_primary_zone, count_group_by_zone);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(z2, ls_primary_zone.at(0));
  ASSERT_EQ(z2, ls_primary_zone.at(1));
  ASSERT_EQ(0, count_group_by_zone.at(0));
  ASSERT_EQ(0, count_group_by_zone.at(1));
  ASSERT_EQ(2, count_group_by_zone.at(2));
  ret = ObCommonLSService::balance_ls_primary_zone(primary_zone_array, ls_primary_zone, count_group_by_zone);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(z1, ls_primary_zone.at(0));
  ASSERT_EQ(z2, ls_primary_zone.at(1));
  ASSERT_EQ(1, count_group_by_zone.at(0));
  ASSERT_EQ(0, count_group_by_zone.at(1)); 
  ASSERT_EQ(1, count_group_by_zone.at(2));

  //case 4 pirmary zone to  z1,z3,z2, ls to z1, z2, z2, z2
  ls_primary_zone.reset();
  count_group_by_zone.reset();
  ret = info.init(1002, 1001, ls_id, z2, z1.str());
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = info.init(1002, 1001, ls_id, z1, z1.str());
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = ObCommonLSService::set_ls_to_primary_zone(primary_zone_array, primary_zone_infos, ls_primary_zone, count_group_by_zone);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(z2, ls_primary_zone.at(0));
  ASSERT_EQ(z2, ls_primary_zone.at(1));
  ASSERT_EQ(z2, ls_primary_zone.at(2));
  ASSERT_EQ(z1, ls_primary_zone.at(3));
  ASSERT_EQ(1, count_group_by_zone.at(0));
  ASSERT_EQ(0, count_group_by_zone.at(1));
  ASSERT_EQ(3, count_group_by_zone.at(2));
  ret = ObCommonLSService::balance_ls_primary_zone(primary_zone_array, ls_primary_zone, count_group_by_zone);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(z3, ls_primary_zone.at(0));
  ASSERT_EQ(z2, ls_primary_zone.at(1));
  ASSERT_EQ(z2, ls_primary_zone.at(2));
  ASSERT_EQ(z1, ls_primary_zone.at(3));
  ASSERT_EQ(1, count_group_by_zone.at(0));
  ASSERT_EQ(1, count_group_by_zone.at(1)); 
  ASSERT_EQ(2, count_group_by_zone.at(2));

  //case 5 pirmary zone to  z1,z3,z2, ls to z1, z1, z1, z5, z5
  ls_primary_zone.reset();
  count_group_by_zone.reset();
  primary_zone_infos.reset();
  ret = info.init(1002, 1001, ls_id, z1, z1.str());
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = info.init(1002, 1001, ls_id, z5, z1.str());
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = ObCommonLSService::set_ls_to_primary_zone(primary_zone_array, primary_zone_infos, ls_primary_zone, count_group_by_zone);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(z1, ls_primary_zone.at(0));
  ASSERT_EQ(z1, ls_primary_zone.at(1));
  ASSERT_EQ(z1, ls_primary_zone.at(2));
  ASSERT_EQ(z3, ls_primary_zone.at(3));
  ASSERT_EQ(z2, ls_primary_zone.at(4));
  ASSERT_EQ(3, count_group_by_zone.at(0));
  ASSERT_EQ(1, count_group_by_zone.at(1));
  ASSERT_EQ(1, count_group_by_zone.at(2));
  ret = ObCommonLSService::balance_ls_primary_zone(primary_zone_array, ls_primary_zone, count_group_by_zone);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(z3, ls_primary_zone.at(0));
  ASSERT_EQ(z1, ls_primary_zone.at(1));
  ASSERT_EQ(z1, ls_primary_zone.at(2));
  ASSERT_EQ(z3, ls_primary_zone.at(3));
  ASSERT_EQ(z2, ls_primary_zone.at(4));
  ASSERT_EQ(2, count_group_by_zone.at(0));
  ASSERT_EQ(2, count_group_by_zone.at(1));
  ASSERT_EQ(1, count_group_by_zone.at(2));

  //case 6 pirmary zone to  z1,z3,z1, ls to z1, z1, z1,z1,z2,z2,z2,z2

  ls_primary_zone.reset();
  count_group_by_zone.reset();
  primary_zone_infos.reset();
  ret = info.init(1002, 1001, ls_id, z1, z1.str());
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);

  ret = info.init(1002, 1001, ls_id, z2, z1.str());
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = primary_zone_infos.push_back(info);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = ObCommonLSService::set_ls_to_primary_zone(primary_zone_array, primary_zone_infos, ls_primary_zone, count_group_by_zone);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(z1, ls_primary_zone.at(0));
  ASSERT_EQ(z1, ls_primary_zone.at(1));
  ASSERT_EQ(z1, ls_primary_zone.at(2));
  ASSERT_EQ(z1, ls_primary_zone.at(3));
  ASSERT_EQ(z2, ls_primary_zone.at(4));
  ASSERT_EQ(z2, ls_primary_zone.at(5));
  ASSERT_EQ(z2, ls_primary_zone.at(6));
  ASSERT_EQ(z2, ls_primary_zone.at(7));
  ASSERT_EQ(4, count_group_by_zone.at(0));
  ASSERT_EQ(0, count_group_by_zone.at(1));
  ASSERT_EQ(4, count_group_by_zone.at(2));
  ret = ObCommonLSService::balance_ls_primary_zone(primary_zone_array, ls_primary_zone, count_group_by_zone);
  LOG_INFO("set ls to primary zone", K(ls_primary_zone), K(count_group_by_zone));
  ASSERT_EQ(z3, ls_primary_zone.at(0));
  ASSERT_EQ(z1, ls_primary_zone.at(1));
  ASSERT_EQ(z1, ls_primary_zone.at(2));
  ASSERT_EQ(z1, ls_primary_zone.at(3));
  ASSERT_EQ(z3, ls_primary_zone.at(4));
  ASSERT_EQ(z2, ls_primary_zone.at(5));
  ASSERT_EQ(z2, ls_primary_zone.at(6));
  ASSERT_EQ(z2, ls_primary_zone.at(7));
  ASSERT_EQ(3, count_group_by_zone.at(0));
  ASSERT_EQ(2, count_group_by_zone.at(1));
  ASSERT_EQ(3, count_group_by_zone.at(2));


}
}
}
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#undef private
