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
#include "share/ob_locality_priority.h"
#include <gtest/gtest.h>

namespace oceanbase
{
namespace share
{
using namespace common;

class TestLocalityPriority : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  void get_primary_region_prioriry(const char *primary_zone,
      const common::ObIArray<ObLocalityRegion> &locality_region_array,
      common::ObIArray<ObLocalityRegion> &tenant_locality_region);
private:
  typedef common::ObFixedLengthString<common::MAX_ZONE_LENGTH> Zone;
protected:
  common::ObSEArray<ObLocalityRegion, 5> locality_region_array_;
};

void TestLocalityPriority::SetUp()
{
  locality_region_array_.reset();
  ObLocalityRegion region;
  region.region_ = "hz";
  Zone zone1 = "hz1";
  Zone zone2 = "hz2";
  region.zone_array_.push_back(zone1);
  region.zone_array_.push_back(zone2);
  locality_region_array_.push_back(region);

  region.reset();
  region.region_ = "sh";
  Zone zone3 = "sh1";
  Zone zone4 = "sh2";
  region.zone_array_.push_back(zone3);
  region.zone_array_.push_back(zone4);
  locality_region_array_.push_back(region);

  region.reset();
  region.region_ = "sz";
  Zone zone5 = "sz1";
  region.zone_array_.push_back(zone5);
  locality_region_array_.push_back(region);

  SHARE_LOG(INFO,"locality_region", K_(locality_region_array));
}

void TestLocalityPriority::TearDown()
{
  locality_region_array_.reset();
}

void TestLocalityPriority::get_primary_region_prioriry(const char *primary_zone,
      const common::ObIArray<ObLocalityRegion> &locality_region_array,
      common::ObIArray<ObLocalityRegion> &tenant_locality_region)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObLocalityPriority::get_primary_region_prioriry(primary_zone,
          locality_region_array, tenant_locality_region))) {
    SHARE_LOG(WARN, "ObLocalityPriority::get_primary_region_prioriry error", K(ret));
  } else {
    SHARE_LOG(INFO, "ObLocalityPriority::get_primary_region_prioriry success", K(tenant_locality_region));
  }
}

TEST_F(TestLocalityPriority, get_region_priority_primary_three_region)
{
  const char *tenant_primary_zone = "hz1,hz2;sh1,sh2;sz1";
  ObSEArray<ObLocalityRegion, 5> tenant_locality_region;
  get_primary_region_prioriry(tenant_primary_zone, locality_region_array_, tenant_locality_region);
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz3";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz4";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "sh1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 64);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "sh2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 64);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "sh3";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 64);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "sh100";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 64);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "hz1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 64);
  }

  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 128);
  }

  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 128);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz3";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 128);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "bj";
    locality_info.local_zone_ = "bj1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
}

TEST_F(TestLocalityPriority, get_region_priority_primary_one_region)
{
  const char *tenant_primary_zone = "hz1";
  ObSEArray<ObLocalityRegion, 5> tenant_locality_region;
  get_primary_region_prioriry(tenant_primary_zone, locality_region_array_, tenant_locality_region);

  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "sh1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "sh2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "bj";
    locality_info.local_zone_ = "bj1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
}

TEST_F(TestLocalityPriority, get_region_priority_primary_two_region)
{
  const char *tenant_primary_zone = "hz1;sh2";
  ObSEArray<ObLocalityRegion, 5> tenant_locality_region;
  get_primary_region_prioriry(tenant_primary_zone, locality_region_array_, tenant_locality_region);
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "sh1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 64);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "sh2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 64);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
}

TEST_F(TestLocalityPriority, get_region_priority_primary_same_region)
{
  const char *tenant_primary_zone = "hz1;hz2";
  ObSEArray<ObLocalityRegion, 5> tenant_locality_region;
  get_primary_region_prioriry(tenant_primary_zone, locality_region_array_, tenant_locality_region);
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "sh1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "sh2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
}

TEST_F(TestLocalityPriority, get_region_priority_primary_diff_region)
{
  const char *tenant_primary_zone = "  hz1  ,  sh1  ; hz2   ";
  ObSEArray<ObLocalityRegion, 5> tenant_locality_region;
  get_primary_region_prioriry(tenant_primary_zone, locality_region_array_, tenant_locality_region);
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "sh1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sh";
    locality_info.local_zone_ = "sh2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 0);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz1";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
}

TEST_F(TestLocalityPriority, get_region_priority_primary_one_empty)
{
  const char *tenant_primary_zone = "  ;  hz1";
  ObSEArray<ObLocalityRegion, 5> tenant_locality_region;
  get_primary_region_prioriry(tenant_primary_zone, locality_region_array_, tenant_locality_region);
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, 64);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
}

TEST_F(TestLocalityPriority, get_region_priority_primary_two_empty)
{
  const char *tenant_primary_zone = "  ;  ";
  ObSEArray<ObLocalityRegion, 5> tenant_locality_region;
  get_primary_region_prioriry(tenant_primary_zone, locality_region_array_, tenant_locality_region);
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
}

TEST_F(TestLocalityPriority, get_region_priority_primary_all_empty)
{
  const char *tenant_primary_zone = "      ";
  ObSEArray<ObLocalityRegion, 5> tenant_locality_region;
  get_primary_region_prioriry(tenant_primary_zone, locality_region_array_, tenant_locality_region);
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "hz";
    locality_info.local_zone_ = "hz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
  {
    ObLocalityInfo locality_info;
    locality_info.local_region_ = "sz";
    locality_info.local_zone_ = "sz2";
    uint64_t zone_priority = UINT64_MAX;
    ObLocalityPriority::get_region_priority(locality_info, tenant_locality_region, zone_priority);
    ASSERT_EQ(zone_priority, UINT64_MAX);
  }
}

TEST_F(TestLocalityPriority, get_region_priority_primary_NULL)
{
  const char *tenant_primary_zone = NULL;
  ObSEArray<ObLocalityRegion, 5> tenant_locality_region;
  int ret = ObLocalityPriority::get_primary_region_prioriry(tenant_primary_zone, locality_region_array_, tenant_locality_region);
  EXPECT_EQ(common::OB_INVALID_ARGUMENT, ret);
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
