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

#include "gtest/gtest.h"
#define private public
#include "share/ob_tenant_role.h"
#undef private

#define ASSERT_SUCCESS(ret) ASSERT_EQ(OB_SUCCESS, (ret))

namespace oceanbase {
namespace share {

#define M_MPT ObProtectionMode::MAXIMUM_PROTECTION_MODE
#define M_MPF ObProtectionMode::MAXIMUM_PERFORMANCE_MODE
#define M_MA ObProtectionMode::MAXIMUM_AVAILABILITY_MODE
#define M_RE ObProtectionMode::RESYNCHRONIZATION_MODE
#define M_PRE_MPF ObProtectionMode::PRE_MAXIMUM_PERFORMANCE_MODE

#define L_MPF ObProtectionLevel::MAXIMUM_PERFORMANCE_LEVEL
#define L_MA ObProtectionLevel::MAXIMUM_AVAILABILITY_LEVEL
#define L_MPT ObProtectionLevel::MAXIMUM_PROTECTION_LEVEL
#define L_RE ObProtectionLevel::RESYNCHRONIZATION_LEVEL
#define L_PRE_MPF ObProtectionLevel::PRE_MAXIMUM_PERFORMANCE_LEVEL

#define STAT(mode, level) ({\
  ObProtectionStat tmp; \
  tmp.protection_mode_ = mode; \
  tmp.protection_level_ = level; \
  tmp.switchover_epoch_ = 0; \
  tmp; \
})

class TestProtectionStat : public ::testing::Test {
public:
  TestProtectionStat() {}
  ~TestProtectionStat() {}
  bool check_next_protection_stat(const ObProtectionStat &protection_stat, const ObProtectionStat &next_protection_stat)
  {
    ObProtectionStat tmp;
    return protection_stat.get_next_protection_stat(tmp) == OB_SUCCESS
      && tmp == next_protection_stat;
  }
  bool check_next_protection_stat(const ObProtectionStat &protection_stat,
    const ObProtectionMode::Mode &next_protection_mode, const ObProtectionStat &next_protection_stat,
    const bool fail = false)
  {
    ObProtectionStat tmp;
    ObProtectionLevel next_protection_level;
    bool success = (OB_SUCCESS == ObProtectionStat::get_next_protection_level(
      protection_stat.get_protection_mode(),
      protection_stat.get_protection_level(),
      ObProtectionMode(next_protection_mode),
      next_protection_level));
    return success != fail && (fail || next_protection_stat == STAT(next_protection_mode, next_protection_level));
  }
};

TEST_F(TestProtectionStat, check_stat_valid)
{
  ASSERT_TRUE(STAT(M_MPF, L_MPF).is_valid());
  ASSERT_FALSE(STAT(M_MPF, L_MA).is_valid());
  ASSERT_FALSE(STAT(M_MPF, L_MPT).is_valid());
  ASSERT_FALSE(STAT(M_MPF, L_RE).is_valid());
  ASSERT_TRUE(STAT(M_MPF, L_PRE_MPF).is_valid());

  ASSERT_TRUE(STAT(M_MPT, L_MPF).is_valid());
  ASSERT_FALSE(STAT(M_MPT, L_MA).is_valid());
  ASSERT_TRUE(STAT(M_MPT, L_MPT).is_valid());
  ASSERT_FALSE(STAT(M_MPT, L_RE).is_valid());
  ASSERT_FALSE(STAT(M_MPT, L_PRE_MPF).is_valid());

  ASSERT_TRUE(STAT(M_MA, L_MPF).is_valid());
  ASSERT_TRUE(STAT(M_MA, L_MA).is_valid());
  ASSERT_FALSE(STAT(M_MA, L_MPT).is_valid());
  ASSERT_TRUE(STAT(M_MA, L_RE).is_valid());
  ASSERT_TRUE(STAT(M_MA, L_PRE_MPF).is_valid());
}

TEST_F(TestProtectionStat, get_next_protection_stat_without_change_protection_mode)
{
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MPF, L_MPF), STAT(M_MPF, L_MPF)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MPF, L_PRE_MPF), STAT(M_MPF, L_MPF)));

  ASSERT_TRUE(check_next_protection_stat(STAT(M_MPT, L_MPF), STAT(M_MPT, L_MPT)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MPT, L_MPT), STAT(M_MPT, L_MPT)));

  ASSERT_TRUE(check_next_protection_stat(STAT(M_MA, L_MPF), STAT(M_MA, L_MA)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MA, L_MA), STAT(M_MA, L_PRE_MPF)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MA, L_PRE_MPF), STAT(M_MA, L_RE)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MA, L_RE), STAT(M_MA, L_MPF)));
}

TEST_F(TestProtectionStat, get_next_protection_stat_with_change_protection_mode)
{
  ObProtectionStat tmp;
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MPF, L_MPF), M_MPT, STAT(M_MPT, L_MPF)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MPF, L_MPF), M_MA, STAT(M_MA, L_RE)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MPF, L_PRE_MPF), M_MPT, tmp, true/*fail*/));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MPF, L_PRE_MPF), M_MA, STAT(M_MA, L_PRE_MPF)));

  ASSERT_TRUE(check_next_protection_stat(STAT(M_MPT, L_MPF), M_MPF, STAT(M_MPF, L_PRE_MPF)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MPT, L_MPF), M_MA, STAT(M_MA, L_MPF)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MPT, L_MPT), M_MPF, STAT(M_MPF, L_PRE_MPF)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MPT, L_MPT), M_MA, STAT(M_MA, L_MA)));

  ASSERT_TRUE(check_next_protection_stat(STAT(M_MA, L_MPF), M_MPF, STAT(M_MPF, L_PRE_MPF)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MA, L_MPF), M_MPT, STAT(M_MPT, L_MPF)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MA, L_MA), M_MPF, STAT(M_MPF, L_PRE_MPF)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MA, L_MA), M_MPT, STAT(M_MPT, L_MPT)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MA, L_PRE_MPF), M_MPF, STAT(M_MPF, L_PRE_MPF)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MA, L_PRE_MPF), M_MPT, tmp, true/*fail*/));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MA, L_RE), M_MPF, STAT(M_MPF, L_MPF)));
  ASSERT_TRUE(check_next_protection_stat(STAT(M_MA, L_RE), M_MPT, STAT(M_MPT, L_MPF)));
}

} // namespace test
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}