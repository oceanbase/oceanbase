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

#define USING_LOG_PREFIX COMMON

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#include "share/ob_primary_zone_util.h"

using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
namespace share
{
using namespace schema;
class TestPrimaryZoneUtil : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
};

TEST_F(TestPrimaryZoneUtil, single_zone)
{
  ObString primary_zone("zone1");
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(primary_zone));
  ObPrimaryZoneUtil primary_zone_util(primary_zone);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init());
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(primary_zone == pz_str);
  ASSERT_EQ(1, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
}

TEST_F(TestPrimaryZoneUtil, single_zone1)
{
  ObString primary_zone("zone1;");
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ObPrimaryZoneUtil primary_zone_util(primary_zone);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init());
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1") == pz_str);
  ASSERT_EQ(1, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == pz_str);
  ASSERT_EQ(1, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
}

TEST_F(TestPrimaryZoneUtil, multiple_zone0)
{
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone2")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone3")));
  ObString primary_zone("zone1,zone2,zone3");
  ObPrimaryZoneUtil primary_zone_util(primary_zone);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init());
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1,zone2,zone3") == pz_str);
  ASSERT_EQ(3, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(1).score_);
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(2).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == "zone1");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(1).zone_ == "zone2");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(2).zone_ == "zone3");
  ASSERT_EQ(3, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
  ASSERT_TRUE(ObString("zone2") == primary_zone_util.full_zone_array_.at(1).zone_);
  ASSERT_TRUE(ObString("zone3") == primary_zone_util.full_zone_array_.at(2).zone_);
}

TEST_F(TestPrimaryZoneUtil, multiple_zone1)
{
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone2")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone3")));
  ObString primary_zone("zone1,  zone2,    zone3");
  ObPrimaryZoneUtil primary_zone_util(primary_zone);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init());
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1,zone2,zone3") == pz_str);
  ASSERT_EQ(3, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(1).score_);
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(2).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == "zone1");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(1).zone_ == "zone2");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(2).zone_ == "zone3");
  ASSERT_EQ(3, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
  ASSERT_TRUE(ObString("zone2") == primary_zone_util.full_zone_array_.at(1).zone_);
  ASSERT_TRUE(ObString("zone3") == primary_zone_util.full_zone_array_.at(2).zone_);
}

TEST_F(TestPrimaryZoneUtil, multiple_zone2)
{
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone2")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone3")));
  ObString primary_zone("zone1;zone2;zone3");
  ObPrimaryZoneUtil primary_zone_util(primary_zone);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init());
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1;zone2;zone3") == pz_str);
  ASSERT_EQ(3, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_EQ(1, primary_zone_util.zone_array_.at(1).score_);
  ASSERT_EQ(2, primary_zone_util.zone_array_.at(2).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == "zone1");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(1).zone_ == "zone2");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(2).zone_ == "zone3");
  ASSERT_EQ(3, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
  ASSERT_TRUE(ObString("zone2") == primary_zone_util.full_zone_array_.at(1).zone_);
  ASSERT_TRUE(ObString("zone3") == primary_zone_util.full_zone_array_.at(2).zone_);
}

TEST_F(TestPrimaryZoneUtil, multiple_zone3)
{
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone2")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone3")));
  ObString primary_zone("zone1;  zone2;    zone3;");
  ObPrimaryZoneUtil primary_zone_util(primary_zone);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init());
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1;zone2;zone3") == pz_str);
  ASSERT_EQ(3, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_EQ(1, primary_zone_util.zone_array_.at(1).score_);
  ASSERT_EQ(2, primary_zone_util.zone_array_.at(2).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == "zone1");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(1).zone_ == "zone2");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(2).zone_ == "zone3");
  ASSERT_EQ(3, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
  ASSERT_TRUE(ObString("zone2") == primary_zone_util.full_zone_array_.at(1).zone_);
  ASSERT_TRUE(ObString("zone3") == primary_zone_util.full_zone_array_.at(2).zone_);
}

TEST_F(TestPrimaryZoneUtil, multiple_zone4)
{
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone2")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone3")));
  ObString primary_zone("zone1,  zone2;    zone3;");
  ObPrimaryZoneUtil primary_zone_util(primary_zone);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init());
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1,zone2;zone3") == pz_str);
  ASSERT_EQ(3, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(1).score_);
  ASSERT_EQ(1, primary_zone_util.zone_array_.at(2).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == "zone1");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(1).zone_ == "zone2");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(2).zone_ == "zone3");
  ASSERT_EQ(3, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
  ASSERT_TRUE(ObString("zone2") == primary_zone_util.full_zone_array_.at(1).zone_);
  ASSERT_TRUE(ObString("zone3") == primary_zone_util.full_zone_array_.at(2).zone_);
}

TEST_F(TestPrimaryZoneUtil, multiple_zone5)
{
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone2")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone3")));
  ObString primary_zone("zone1;  zone2,    zone3;");
  ObPrimaryZoneUtil primary_zone_util(primary_zone);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init());
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1;zone2,zone3") == pz_str);
  ASSERT_EQ(3, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_EQ(1, primary_zone_util.zone_array_.at(1).score_);
  ASSERT_EQ(1, primary_zone_util.zone_array_.at(2).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == "zone1");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(1).zone_ == "zone2");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(2).zone_ == "zone3");
  ASSERT_EQ(3, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
  ASSERT_TRUE(ObString("zone2") == primary_zone_util.full_zone_array_.at(1).zone_);
  ASSERT_TRUE(ObString("zone3") == primary_zone_util.full_zone_array_.at(2).zone_);
}

TEST_F(TestPrimaryZoneUtil, multiple_zone6)
{
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone2")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone3")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone4")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone5")));

  ObArray<share::schema::ObZoneRegion> zone_region_list;
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone1", "HZ")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone2", "HZ")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone3", "SH")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone4", "SH")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone5", "SZ")));

  ObString primary_zone("zone1;zone2,zone3;");
  ObPrimaryZoneUtil primary_zone_util(primary_zone, &zone_region_list);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init(zone_list));
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1;zone2;zone3;zone4") == pz_str);
  ASSERT_EQ(3, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_EQ(1, primary_zone_util.zone_array_.at(1).score_);
  ASSERT_EQ(1, primary_zone_util.zone_array_.at(2).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == "zone1");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(1).zone_ == "zone2");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(2).zone_ == "zone3");
  ASSERT_EQ(4, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
  ASSERT_TRUE(ObString("zone2") == primary_zone_util.full_zone_array_.at(1).zone_);
  ASSERT_TRUE(ObString("zone3") == primary_zone_util.full_zone_array_.at(2).zone_);
  ASSERT_TRUE(ObString("zone4") == primary_zone_util.full_zone_array_.at(3).zone_);
}

TEST_F(TestPrimaryZoneUtil, multiple_zone7)
{
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone2")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone3")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone4")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone5")));

  ObArray<share::schema::ObZoneRegion> zone_region_list;
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone1", "HZ")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone2", "HZ")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone3", "SH")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone4", "SH")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone5", "SZ")));

  ObString primary_zone("zone1;zone3;");
  ObPrimaryZoneUtil primary_zone_util(primary_zone, &zone_region_list);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init(zone_list));
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1;zone2;zone3;zone4") == pz_str);
  ASSERT_EQ(2, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_EQ(1, primary_zone_util.zone_array_.at(1).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == "zone1");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(1).zone_ == "zone3");
  ASSERT_EQ(4, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
  ASSERT_TRUE(ObString("zone2") == primary_zone_util.full_zone_array_.at(1).zone_);
  ASSERT_TRUE(ObString("zone3") == primary_zone_util.full_zone_array_.at(2).zone_);
  ASSERT_TRUE(ObString("zone4") == primary_zone_util.full_zone_array_.at(3).zone_);
}

TEST_F(TestPrimaryZoneUtil, multiple_zone8)
{
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone2")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone3")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone4")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone5")));

  ObArray<share::schema::ObZoneRegion> zone_region_list;
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone1", "HZ")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone2", "HZ")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone3", "SH")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone4", "SH")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone5", "SZ")));

  ObString primary_zone("zone1,zone3;");
  ObPrimaryZoneUtil primary_zone_util(primary_zone, &zone_region_list);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init(zone_list));
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1,zone3;zone2,zone4") == pz_str);
  ASSERT_EQ(2, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(1).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == "zone1");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(1).zone_ == "zone3");
  ASSERT_EQ(4, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
  ASSERT_TRUE(ObString("zone3") == primary_zone_util.full_zone_array_.at(1).zone_);
  ASSERT_TRUE(ObString("zone2") == primary_zone_util.full_zone_array_.at(2).zone_);
  ASSERT_TRUE(ObString("zone4") == primary_zone_util.full_zone_array_.at(3).zone_);
}

TEST_F(TestPrimaryZoneUtil, multiple_zone9)
{
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone2")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone3")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone4")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone5")));

  ObArray<share::schema::ObZoneRegion> zone_region_list;
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone1", "HZ")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone2", "HZ")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone3", "SH")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone4", "SH")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone5", "SZ")));

  ObString primary_zone("zone1;zone3;zone5");
  ObPrimaryZoneUtil primary_zone_util(primary_zone, &zone_region_list);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init(zone_list));
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1;zone2;zone3;zone4;zone5") == pz_str);
  ASSERT_EQ(3, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_EQ(1, primary_zone_util.zone_array_.at(1).score_);
  ASSERT_EQ(2, primary_zone_util.zone_array_.at(2).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == "zone1");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(1).zone_ == "zone3");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(2).zone_ == "zone5");
  ASSERT_EQ(5, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
  ASSERT_TRUE(ObString("zone2") == primary_zone_util.full_zone_array_.at(1).zone_);
  ASSERT_TRUE(ObString("zone3") == primary_zone_util.full_zone_array_.at(2).zone_);
  ASSERT_TRUE(ObString("zone4") == primary_zone_util.full_zone_array_.at(3).zone_);
  ASSERT_TRUE(ObString("zone5") == primary_zone_util.full_zone_array_.at(4).zone_);
}

TEST_F(TestPrimaryZoneUtil, multiple_zone10)
{
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone2")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone3")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone4")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone5")));

  ObArray<share::schema::ObZoneRegion> zone_region_list;
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone1", "HZ")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone2", "HZ")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone3", "SH")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone4", "SH")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone5", "SZ")));

  ObString primary_zone("zone1;zone3,zone4;");
  ObPrimaryZoneUtil primary_zone_util(primary_zone, &zone_region_list);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init(zone_list));
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1;zone2;zone3,zone4") == pz_str);
  ASSERT_EQ(3, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_EQ(1, primary_zone_util.zone_array_.at(1).score_);
  ASSERT_EQ(1, primary_zone_util.zone_array_.at(2).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == "zone1");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(1).zone_ == "zone3");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(2).zone_ == "zone4");
  ASSERT_EQ(4, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
  ASSERT_TRUE(ObString("zone2") == primary_zone_util.full_zone_array_.at(1).zone_);
  ASSERT_TRUE(ObString("zone3") == primary_zone_util.full_zone_array_.at(2).zone_);
  ASSERT_TRUE(ObString("zone4") == primary_zone_util.full_zone_array_.at(3).zone_);
}

TEST_F(TestPrimaryZoneUtil, multiple_zone11)
{
  ObArray<ObString> zone_list;
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone1")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone2")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone3")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone4")));
  ASSERT_EQ(OB_SUCCESS, zone_list.push_back(ObString("zone5")));

  ObArray<share::schema::ObZoneRegion> zone_region_list;
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone1", "HZ")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone2", "HZ")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone3", "SH")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone4", "SH")));
  ASSERT_EQ(OB_SUCCESS, zone_region_list.push_back(ObZoneRegion("zone5", "SZ")));

  ObString primary_zone("zone1,zone2;zone4;");
  ObPrimaryZoneUtil primary_zone_util(primary_zone, &zone_region_list);
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.init(zone_list));
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.check_and_parse_primary_zone());
  char pz_str[MAX_ZONE_LENGTH];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, primary_zone_util.output_normalized_primary_zone(pz_str, MAX_ZONE_LENGTH, pos));
  ASSERT_TRUE(ObZone("zone1,zone2;zone4;zone3") == pz_str);
  ASSERT_EQ(3, primary_zone_util.zone_array_.count());
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(0).score_);
  ASSERT_EQ(0, primary_zone_util.zone_array_.at(1).score_);
  ASSERT_EQ(1, primary_zone_util.zone_array_.at(2).score_);
  ASSERT_TRUE(primary_zone_util.zone_array_.at(0).zone_ == "zone1");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(1).zone_ == "zone2");
  ASSERT_TRUE(primary_zone_util.zone_array_.at(2).zone_ == "zone4");
  ASSERT_EQ(4, primary_zone_util.full_zone_array_.count());
  ASSERT_TRUE(ObString("zone1") == primary_zone_util.full_zone_array_.at(0).zone_);
  ASSERT_TRUE(ObString("zone2") == primary_zone_util.full_zone_array_.at(1).zone_);
  ASSERT_TRUE(ObString("zone4") == primary_zone_util.full_zone_array_.at(2).zone_);
  ASSERT_TRUE(ObString("zone3") == primary_zone_util.full_zone_array_.at(3).zone_);
}
}
}

int main(int argc, char **argv)
{
  int ret = EXIT_SUCCESS;
  system("rm -f test_primary_zone_util.log");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_primary_zone_util.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);

  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
