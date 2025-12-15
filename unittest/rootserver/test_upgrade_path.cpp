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
#include "share/ob_upgrade_utils.h"

using namespace oceanbase::share;

class TestUpgradePath : public ::testing::Test {
protected:
  void check_upgrade_path(const ObUpgradePath &path, const std::vector<std::pair<uint64_t, bool>> true_path)
  {
    ASSERT_TRUE(path.is_valid());
    std::vector<std::pair<uint64_t, bool>> path_in_vec;
    for (int64_t i = 0; i < path.count(); i++) {
      uint64_t version = OB_INVALID_VERSION;
      bool update = false;
      ASSERT_EQ(path.get_version(i, version, update), OB_SUCCESS);
      path_in_vec.push_back({version, update});
    }
    ASSERT_EQ(path_in_vec, true_path);
  }
};

TEST_F(TestUpgradePath, 42x) // 42x
{
  int ret = OB_SUCCESS;
  ObUpgradePath path;
  std::set<uint64_t> versions;
  // 421 10
  versions.insert(MOCK_DATA_VERSION_4_2_5_7);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(MOCK_DATA_VERSION_4_2_5_7, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_3_0_0, false},
    {DATA_VERSION_4_3_0_1, false},
    {DATA_VERSION_4_3_1_0, false},
    {DATA_VERSION_4_3_2_0, false},
    {DATA_VERSION_4_3_2_1, false},
    {DATA_VERSION_4_3_3_0, false},
    {DATA_VERSION_4_3_3_1, false},
    {DATA_VERSION_4_3_4_0, false},
    {DATA_VERSION_4_3_4_1, false},
    {DATA_VERSION_4_3_5_0, false},
    {DATA_VERSION_4_3_5_1, false},
    {DATA_VERSION_4_3_5_2, false},
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  ASSERT_EQ(versions.size(), ObUpgradeChecker::upgrade_versions[0].upgrade_path_num_);
}

TEST_F(TestUpgradePath, 43x) // 43x
{
  int ret = OB_SUCCESS;
  ObUpgradePath path;
  std::set<uint64_t> versions;
  // 430 00
  versions.insert(DATA_VERSION_4_3_0_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_3_0_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_3_0_1, true},
    {DATA_VERSION_4_3_1_0, true},
    {DATA_VERSION_4_3_2_0, true},
    {DATA_VERSION_4_3_2_1, true},
    {DATA_VERSION_4_3_3_0, true},
    {DATA_VERSION_4_3_3_1, true},
    {DATA_VERSION_4_3_4_0, true},
    {DATA_VERSION_4_3_4_1, true},
    {DATA_VERSION_4_3_5_0, true},
    {DATA_VERSION_4_3_5_1, true},
    {DATA_VERSION_4_3_5_2, true},
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_3_0_1);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_3_0_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_3_1_0, true},
    {DATA_VERSION_4_3_2_0, true},
    {DATA_VERSION_4_3_2_1, true},
    {DATA_VERSION_4_3_3_0, true},
    {DATA_VERSION_4_3_3_1, true},
    {DATA_VERSION_4_3_4_0, true},
    {DATA_VERSION_4_3_4_1, true},
    {DATA_VERSION_4_3_5_0, true},
    {DATA_VERSION_4_3_5_1, true},
    {DATA_VERSION_4_3_5_2, true},
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_3_1_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_3_1_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_3_2_0, true},
    {DATA_VERSION_4_3_2_1, true},
    {DATA_VERSION_4_3_3_0, true},
    {DATA_VERSION_4_3_3_1, true},
    {DATA_VERSION_4_3_4_0, true},
    {DATA_VERSION_4_3_4_1, true},
    {DATA_VERSION_4_3_5_0, true},
    {DATA_VERSION_4_3_5_1, true},
    {DATA_VERSION_4_3_5_2, true},
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_3_2_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_3_2_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_3_2_1, true},
    {DATA_VERSION_4_3_3_0, true},
    {DATA_VERSION_4_3_3_1, true},
    {DATA_VERSION_4_3_4_0, true},
    {DATA_VERSION_4_3_4_1, true},
    {DATA_VERSION_4_3_5_0, true},
    {DATA_VERSION_4_3_5_1, true},
    {DATA_VERSION_4_3_5_2, true},
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_3_2_1);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_3_2_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_3_3_0, true},
    {DATA_VERSION_4_3_3_1, true},
    {DATA_VERSION_4_3_4_0, true},
    {DATA_VERSION_4_3_4_1, true},
    {DATA_VERSION_4_3_5_0, true},
    {DATA_VERSION_4_3_5_1, true},
    {DATA_VERSION_4_3_5_2, true},
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_3_3_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_3_3_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_3_3_1, true},
    {DATA_VERSION_4_3_4_0, true},
    {DATA_VERSION_4_3_4_1, true},
    {DATA_VERSION_4_3_5_0, true},
    {DATA_VERSION_4_3_5_1, true},
    {DATA_VERSION_4_3_5_2, true},
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_3_3_1);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_3_3_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_3_4_0, true},
    {DATA_VERSION_4_3_4_1, true},
    {DATA_VERSION_4_3_5_0, true},
    {DATA_VERSION_4_3_5_1, true},
    {DATA_VERSION_4_3_5_2, true},
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_3_4_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_3_4_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_3_4_1, true},
    {DATA_VERSION_4_3_5_0, true},
    {DATA_VERSION_4_3_5_1, true},
    {DATA_VERSION_4_3_5_2, true},
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_3_4_1);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_3_4_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_3_5_0, true},
    {DATA_VERSION_4_3_5_1, true},
    {DATA_VERSION_4_3_5_2, true},
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_3_5_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_3_5_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_3_5_1, true},
    {DATA_VERSION_4_3_5_2, true},
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_3_5_1);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_3_5_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_3_5_2, true},
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_3_5_2);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_3_5_2, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_4_0_0, false},
    {MOCK_DATA_VERSION_4_4_0_1, false},
    {DATA_VERSION_4_4_1_0, false},
    {DATA_VERSION_4_4_2_0, true},
  });
  ASSERT_EQ(versions.size(), ObUpgradeChecker::upgrade_versions[1].upgrade_path_num_);
}

TEST_F(TestUpgradePath, 44x) // 44x
{
  int ret = OB_SUCCESS;
  std::set<uint64_t> versions;
  ObUpgradePath path;
  // 4220
  versions.insert(DATA_VERSION_4_4_0_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_4_0_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {MOCK_DATA_VERSION_4_4_0_1, true},
    {DATA_VERSION_4_4_1_0, true},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(MOCK_DATA_VERSION_4_4_0_1);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(MOCK_DATA_VERSION_4_4_0_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_4_1_0, true},
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_4_1_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_4_1_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_4_2_0, true},
  });
  versions.insert(DATA_VERSION_4_4_2_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_4_2_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
  });
  ASSERT_EQ(versions.size(), ObUpgradeChecker::upgrade_versions[2].upgrade_path_num_);
}

int main(int argc, char **argv)
{
  system("rm -f test_upgrade_path.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_upgrade_path.log", true);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
