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

#include <gtest/gtest.h>
#include <string>

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_region.h"
#include "common/ob_zone.h"
#include "logservice/logrouteservice/ob_server_priority.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace logservice;

// 未配置 zone_priority 时 get_priority 不会写入基准值，仅对命中 assign_region 的项做 priority--。
// 与 ob_log_all_svr_cache 等调用方约定：入参先置为「非首选」档位（数值较大），命中首选后减 1 得到更高优先级。
static constexpr FetchPriority UNCONFIGURED_FETCH_PRIORITY_BASE = 1;

// do not configure zone priority
TEST(TestObCdcLogFetchPriority, empty_or_whitespace_not_configured)
{
  ObCdcLogFetchPriority fp;
  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority(nullptr));
  EXPECT_FALSE(fp.is_configured());

  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority(""));
  EXPECT_FALSE(fp.is_configured());

  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority("   \t\n\r  "));
  EXPECT_FALSE(fp.is_configured());
}

// configure assign region but no configure zone
TEST(TestObCdcLogFetchPriority, unconfigured_uses_assign_region_case_insensitive)
{
  ObCdcLogFetchPriority fp;
  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority(""));

  ObRegion prefer;
  ASSERT_EQ(OB_SUCCESS, prefer.assign("HangZhou"));
  ASSERT_EQ(OB_SUCCESS, fp.set_assign_region(prefer));

  ObZone zone;
  ASSERT_EQ(OB_SUCCESS, zone.assign("any_zone"));

  ObRegion r_match;
  ASSERT_EQ(OB_SUCCESS, r_match.assign("HangZhou"));
  FetchPriority pr = UNCONFIGURED_FETCH_PRIORITY_BASE;
  ASSERT_EQ(OB_SUCCESS, fp.get_priority(zone, r_match, pr));
  EXPECT_EQ(UNCONFIGURED_FETCH_PRIORITY_BASE - 1, pr);

  ObRegion r_other;
  ASSERT_EQ(OB_SUCCESS, r_other.assign("ShenZhen"));
  pr = UNCONFIGURED_FETCH_PRIORITY_BASE;
  ASSERT_EQ(OB_SUCCESS, fp.get_priority(zone, r_other, pr));
  EXPECT_EQ(UNCONFIGURED_FETCH_PRIORITY_BASE, pr);

  ObRegion r_ci;
  ASSERT_EQ(OB_SUCCESS, r_ci.assign("hangzhou"));
  pr = UNCONFIGURED_FETCH_PRIORITY_BASE;
  ASSERT_EQ(OB_SUCCESS, fp.get_priority(zone, r_ci, pr));
  EXPECT_EQ(UNCONFIGURED_FETCH_PRIORITY_BASE - 1, pr);
}

// configure zone
TEST(TestObCdcLogFetchPriority, single_tier_and_default_level)
{
  ObCdcLogFetchPriority fp;
  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority("zone_a"));
  EXPECT_TRUE(fp.is_configured());
  EXPECT_EQ(3, fp.get_default_priority_level());

  ObZone z_a, z_b;
  ASSERT_EQ(OB_SUCCESS, z_a.assign("zone_a"));
  ASSERT_EQ(OB_SUCCESS, z_b.assign("zone_b"));

  EXPECT_EQ(1, fp.get_zone_priority(z_a));
  EXPECT_EQ(3, fp.get_zone_priority(z_b));
}

// configure multiple tiers
TEST(TestObCdcLogFetchPriority, multi_tier_semicolon)
{
  ObCdcLogFetchPriority fp;
  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority("z0;z1;z2"));
  EXPECT_EQ(7, fp.get_default_priority_level());

  ObZone z0, z1, z2, z9;
  ASSERT_EQ(OB_SUCCESS, z0.assign("z0"));
  ASSERT_EQ(OB_SUCCESS, z1.assign("z1"));
  ASSERT_EQ(OB_SUCCESS, z2.assign("z2"));
  ASSERT_EQ(OB_SUCCESS, z9.assign("z9"));

  EXPECT_EQ(1, fp.get_zone_priority(z0));
  EXPECT_EQ(3, fp.get_zone_priority(z1));
  EXPECT_EQ(5, fp.get_zone_priority(z2));
  EXPECT_EQ(7, fp.get_zone_priority(z9));
}

// configure same tier with comma and whitespace
TEST(TestObCdcLogFetchPriority, same_tier_comma_and_whitespace)
{
  ObCdcLogFetchPriority fp;
  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority("  z0a , z0b ; z2  "));
  EXPECT_EQ(5, fp.get_default_priority_level());

  ObZone z0a, z0b, z2, z9;
  ASSERT_EQ(OB_SUCCESS, z0a.assign("z0a"));
  ASSERT_EQ(OB_SUCCESS, z0b.assign("z0b"));
  ASSERT_EQ(OB_SUCCESS, z2.assign("z2"));
  ASSERT_EQ(OB_SUCCESS, z9.assign("z9"));

  EXPECT_EQ(1, fp.get_zone_priority(z0a));
  EXPECT_EQ(1, fp.get_zone_priority(z0b));
  EXPECT_EQ(3, fp.get_zone_priority(z2));
  EXPECT_EQ(5, fp.get_zone_priority(z9));
}

// configure zone with whitespace between ;
TEST(TestObCdcLogFetchPriority, zone_with_whitespace_between_tokens)
{
  ObCdcLogFetchPriority fp;
  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority("z0;;z1"));
  EXPECT_EQ(5, fp.get_default_priority_level());

  ObZone z0, z1;
  ASSERT_EQ(OB_SUCCESS, z0.assign("z0"));
  ASSERT_EQ(OB_SUCCESS, z1.assign("z1"));

  EXPECT_EQ(1, fp.get_zone_priority(z0));
  EXPECT_EQ(3, fp.get_zone_priority(z1));
}

// configure zone without zone info
TEST(TestObCdcLogFetchPriority, zone_without_zone_info)
{
  ObCdcLogFetchPriority fp;
  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority(";"));
  EXPECT_EQ(1, fp.get_default_priority_level());
  ObZone z1;
  ASSERT_EQ(OB_SUCCESS, z1.assign("z1"));
  EXPECT_EQ(1, fp.get_zone_priority(z1));
}

// 已配置 zone_priority 时仍以 get_zone_priority 为基准；若 region 命中 assign_region 再减 1（与 zone 分层叠加）。
TEST(TestObCdcLogFetchPriority, configured_get_priority_assign_region_adjusts)
{
  ObCdcLogFetchPriority fp;
  ObRegion prefer;
  ASSERT_EQ(OB_SUCCESS, prefer.assign("HangZhou"));
  ASSERT_EQ(OB_SUCCESS, fp.set_assign_region(prefer));
  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority("only_zone"));

  ObZone z;
  ASSERT_EQ(OB_SUCCESS, z.assign("only_zone"));

  ObRegion r_high, r_low;
  ASSERT_EQ(OB_SUCCESS, r_high.assign("HangZhou"));
  ASSERT_EQ(OB_SUCCESS, r_low.assign("ShenZhen"));

  const FetchPriority base = fp.get_zone_priority(z);

  FetchPriority p1 = REGION_PRIORITY_UNKNOWN;
  FetchPriority p2 = REGION_PRIORITY_UNKNOWN;
  ASSERT_EQ(OB_SUCCESS, fp.get_priority(z, r_high, p1));
  ASSERT_EQ(OB_SUCCESS, fp.get_priority(z, r_low, p2));
  EXPECT_EQ(base - 1, p1);
  EXPECT_EQ(base, p2);
}

// duplicate zone first tier wins
TEST(TestObCdcLogFetchPriority, duplicate_zone_first_tier_wins)
{
  ObCdcLogFetchPriority fp;
  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority("zdup;zother;zdup"));
  ObZone zdup;
  ASSERT_EQ(OB_SUCCESS, zdup.assign("zdup"));
  EXPECT_EQ(1, fp.get_zone_priority(zdup));
}

// clear config then region mode again
TEST(TestObCdcLogFetchPriority, clear_config_then_region_mode_again)
{
  ObCdcLogFetchPriority fp;
  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority("z0"));
  EXPECT_TRUE(fp.is_configured());

  ObZone z;
  ASSERT_EQ(OB_SUCCESS, z.assign("z0"));
  EXPECT_EQ(1, fp.get_zone_priority(z));

  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority(""));
  EXPECT_FALSE(fp.is_configured());

  ObRegion prefer;
  ASSERT_EQ(OB_SUCCESS, prefer.assign("sh"));
  ASSERT_EQ(OB_SUCCESS, fp.set_assign_region(prefer));

  ObRegion r_sh, r_sz;
  ASSERT_EQ(OB_SUCCESS, r_sh.assign("sh"));
  ASSERT_EQ(OB_SUCCESS, r_sz.assign("sz"));

  FetchPriority pr = UNCONFIGURED_FETCH_PRIORITY_BASE;
  ASSERT_EQ(OB_SUCCESS, fp.get_priority(z, r_sh, pr));
  EXPECT_EQ(UNCONFIGURED_FETCH_PRIORITY_BASE - 1, pr);
  pr = UNCONFIGURED_FETCH_PRIORITY_BASE;
  ASSERT_EQ(OB_SUCCESS, fp.get_priority(z, r_sz, pr));
  EXPECT_EQ(UNCONFIGURED_FETCH_PRIORITY_BASE, pr);
}

// zone name too long fails then recover
TEST(TestObCdcLogFetchPriority, zone_name_too_long_fails_then_recover)
{
  ObCdcLogFetchPriority fp;
  std::string too_long(static_cast<size_t>(MAX_ZONE_LENGTH) + 1, 'a');
  ASSERT_EQ(OB_SIZE_OVERFLOW, fp.init_zone_priority(too_long.c_str()));
  EXPECT_FALSE(fp.is_configured());

  ASSERT_EQ(OB_SUCCESS, fp.init_zone_priority("ok_zone"));
  EXPECT_TRUE(fp.is_configured());
  ObZone z;
  ASSERT_EQ(OB_SUCCESS, z.assign("ok_zone"));
  EXPECT_EQ(1, fp.get_zone_priority(z));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
