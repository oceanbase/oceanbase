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

// 添加新版本时需要修改本文件
// 只需要修改last_lts和current_lts两个测试即可
// 下面分别讨论增加版本x时需要修改的点
// 1. 根据x是上一个LTS还是当前LTS，在对应位置增加
//  versions.insert(x); ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(x, path), OB_SUCCESS);
//  check_upgrade_path(path, {升级路径});
// 2. x是上一个LTS: 所有版本号小于x的版本，升级路径增加 {x, true}
// 3. x是当前LTS && x < CURRENT_LTS_ENTRY: last_lts中所有版本增加 {x, false}，current_lts中所有版本号小于x的版本增加{x, true}
// 4. x是当前LTS && x > CURRENT_LTS_ENTRY: 所有版本增加 {x, true}
// 5. x是当前LTS && 修改CURRENT_LTS_ENTRY为x: 所有last_lts中的版本升级路径里current_lts中的版本设置为false，增加{x, true}

#ifdef DATA_VERSION_4_2_5_2
#undef DATA_VERSION_4_2_5_2
#endif
#define DATA_VERSION_4_2_5_2 (cal_version(4, 2, 5, 2))

#ifndef DATA_VERSION_4_2_1_10
#define DATA_VERSION_4_2_1_10 (cal_version(4, 2, 1, 10))
#endif

#ifndef DATA_VERSION_4_2_1_11
#define DATA_VERSION_4_2_1_11 (cal_version(4, 2, 1, 11))
#endif

#ifndef DATA_VERSION_4_2_4_1
#define DATA_VERSION_4_2_4_1 (cal_version(4, 2, 4, 1))
#endif

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

  void SetUp() override {
    upgrade_path_last_standard = {
      { CALC_VERSION(4UL, 2UL, 1UL, 10UL), CALC_VERSION(4UL, 2UL, 5UL, 1UL) },  // 4.2.5.1
    };
    upgrade_path_current_standard = {
      CALC_VERSION(4UL, 2UL, 2UL, 0UL),  // 4.2.2.0
      CALC_VERSION(4UL, 2UL, 2UL, 1UL),  // 4.2.2.1
      CALC_VERSION(4UL, 2UL, 3UL, 0UL),  // 4.2.3.0
      CALC_VERSION(4UL, 2UL, 3UL, 1UL),  // 4.2.3.1
      CALC_VERSION(4UL, 2UL, 4UL, 0UL),  // 4.2.4.0
      CALC_VERSION(4UL, 2UL, 5UL, 0UL),  // 4.2.5.0
      CALC_VERSION(4UL, 2UL, 5UL, 1UL),  // 4.2.5.1
    };
  }


  bool check_in_current_version_list_(const uint64_t version)
  {
    for (auto x: upgrade_path_current) {
      if (version == x) {
        return true;
      }
    }
    return false;
  }

  bool check_in_last_version_list_(const uint64_t version)
  {
    for (auto x: upgrade_path_last) {
      if (version == x[0]) {
        return true;
      }
    }
    return false;
  }

  // code from ObUpgradeChecker
  int get_upgrade_path(const uint64_t version, ObUpgradePath &path)
  {
    int ret = OB_SUCCESS;
    path.reset();
    [&]() {
      if (check_in_last_version_list_(version)) {
        const uint64_t next_upgrade_version = upgrade_path_last.back()[1];
        for (int64_t i = 0; OB_SUCC(ret) && i < upgrade_path_last.size(); i++) {
          const uint64_t data_version = upgrade_path_last[i][0];
          if (data_version > version) {
            ASSERT_EQ(path.add_version(data_version, true /* update_current_data_version */), OB_SUCCESS);
          }
        }
        ASSERT_EQ(ObUpgradeChecker::add_upgrade_versions_(version, upgrade_path_current.data(), upgrade_path_current.size(),
              false /*force_update_current_version*/, next_upgrade_version, path), OB_SUCCESS);
      } else if (check_in_current_version_list_(version)) {
        ASSERT_EQ(ObUpgradeChecker::add_upgrade_versions_(version, upgrade_path_current.data(), upgrade_path_current.size(),
              true /*force_update_current_version*/, 0 /*next_upgrade_version*/, path), OB_SUCCESS);
      }
    } ();
    return OB_SUCCESS;
  }
protected:
  std::vector<std::array<uint64_t, 2>> upgrade_path_last_standard;
  std::vector<uint64_t> upgrade_path_current_standard;
  std::vector<std::array<uint64_t, 2>> upgrade_path_last;
  std::vector<uint64_t> upgrade_path_current;
};

TEST_F(TestUpgradePath, last_lts) // 421x
{
  int ret = OB_SUCCESS;
  ObUpgradePath path;
  std::set<uint64_t> versions;
  // 421 10
  versions.insert(DATA_VERSION_4_2_1_10);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_1_10, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_1_11, true},
    {DATA_VERSION_4_2_2_0, false},
    {DATA_VERSION_4_2_2_1, false},
    {DATA_VERSION_4_2_3_0, false},
    {DATA_VERSION_4_2_3_1, false},
    {DATA_VERSION_4_2_4_0, false},
    {DATA_VERSION_4_2_5_0, false},
    {DATA_VERSION_4_2_5_1, false},
    {DATA_VERSION_4_2_5_2, false},
    {DATA_VERSION_4_2_5_3, true},
    {DATA_VERSION_4_2_5_4, true},
    {DATA_VERSION_4_2_5_5, true},
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  // 421 11
  versions.insert(DATA_VERSION_4_2_1_11);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_1_11, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_2_0, false},
    {DATA_VERSION_4_2_2_1, false},
    {DATA_VERSION_4_2_3_0, false},
    {DATA_VERSION_4_2_3_1, false},
    {DATA_VERSION_4_2_4_0, false},
    {DATA_VERSION_4_2_5_0, false},
    {DATA_VERSION_4_2_5_1, false},
    {DATA_VERSION_4_2_5_2, false},
    {DATA_VERSION_4_2_5_3, true},
    {DATA_VERSION_4_2_5_4, true},
    {DATA_VERSION_4_2_5_5, true},
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  ASSERT_EQ(versions.size(), ObUpgradeChecker::get_upgrade_path_last_size_());
}

TEST_F(TestUpgradePath, current_lts) // 42x
{
  int ret = OB_SUCCESS;
  std::set<uint64_t> versions;
  ObUpgradePath path;
  // 4220
  versions.insert(DATA_VERSION_4_2_2_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_2_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_2_1, true},
    {DATA_VERSION_4_2_3_0, true},
    {DATA_VERSION_4_2_3_1, true},
    {DATA_VERSION_4_2_4_0, true},
    {DATA_VERSION_4_2_5_0, true},
    {DATA_VERSION_4_2_5_1, true},
    {DATA_VERSION_4_2_5_2, true},
    {DATA_VERSION_4_2_5_3, true},
    {DATA_VERSION_4_2_5_4, true},
    {DATA_VERSION_4_2_5_5, true},
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  // 4221
  versions.insert(DATA_VERSION_4_2_2_1);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_2_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_3_0, true},
    {DATA_VERSION_4_2_3_1, true},
    {DATA_VERSION_4_2_4_0, true},
    {DATA_VERSION_4_2_5_0, true},
    {DATA_VERSION_4_2_5_1, true},
    {DATA_VERSION_4_2_5_2, true},
    {DATA_VERSION_4_2_5_3, true},
    {DATA_VERSION_4_2_5_4, true},
    {DATA_VERSION_4_2_5_5, true},
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  // 4230
  versions.insert(DATA_VERSION_4_2_3_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_3_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_3_1, true},
    {DATA_VERSION_4_2_4_0, true},
    {DATA_VERSION_4_2_5_0, true},
    {DATA_VERSION_4_2_5_1, true},
    {DATA_VERSION_4_2_5_2, true},
    {DATA_VERSION_4_2_5_3, true},
    {DATA_VERSION_4_2_5_4, true},
    {DATA_VERSION_4_2_5_5, true},
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  // 4231
  versions.insert(DATA_VERSION_4_2_3_1);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_3_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_4_0, true},
    {DATA_VERSION_4_2_5_0, true},
    {DATA_VERSION_4_2_5_1, true},
    {DATA_VERSION_4_2_5_2, true},
    {DATA_VERSION_4_2_5_3, true},
    {DATA_VERSION_4_2_5_4, true},
    {DATA_VERSION_4_2_5_5, true},
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  // 4240
  versions.insert(DATA_VERSION_4_2_4_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_4_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_5_0, true},
    {DATA_VERSION_4_2_5_1, true},
    {DATA_VERSION_4_2_5_2, true},
    {DATA_VERSION_4_2_5_3, true},
    {DATA_VERSION_4_2_5_4, true},
    {DATA_VERSION_4_2_5_5, true},
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  // 4250
  versions.insert(DATA_VERSION_4_2_5_0);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_5_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_5_1, true},
    {DATA_VERSION_4_2_5_2, true},
    {DATA_VERSION_4_2_5_3, true},
    {DATA_VERSION_4_2_5_4, true},
    {DATA_VERSION_4_2_5_5, true},
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  // 4251
  versions.insert(DATA_VERSION_4_2_5_1);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_5_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_5_2, true},
    {DATA_VERSION_4_2_5_3, true},
    {DATA_VERSION_4_2_5_4, true},
    {DATA_VERSION_4_2_5_5, true},
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  // 4252
  versions.insert(DATA_VERSION_4_2_5_2);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_5_2, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_5_3, true},
    {DATA_VERSION_4_2_5_4, true},
    {DATA_VERSION_4_2_5_5, true},
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  //4253
  versions.insert(DATA_VERSION_4_2_5_3);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_5_3, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_5_4, true},
    {DATA_VERSION_4_2_5_5, true},
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  //4254
  versions.insert(DATA_VERSION_4_2_5_4);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_5_4, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_5_5, true},
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  //4255
  versions.insert(DATA_VERSION_4_2_5_5);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_5_5, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_5_6, true},
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  //4256
  versions.insert(DATA_VERSION_4_2_5_6);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_5_6, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_5_7, true},
    {DATA_VERSION_4_2_5_8, true},
  });
  //4257
  versions.insert(DATA_VERSION_4_2_5_7);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_5_7, path), OB_SUCCESS);
  check_upgrade_path(path, {
    {DATA_VERSION_4_2_5_8, true},
  });
  //4258
  versions.insert(DATA_VERSION_4_2_5_8);
  ASSERT_EQ(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_5_8, path), OB_SUCCESS);
  check_upgrade_path(path, {
  });
  ASSERT_EQ(versions.size(), ObUpgradeChecker::get_upgrade_path_current_size_());
}

TEST_F(TestUpgradePath, fail)
{
  int ret = OB_SUCCESS;
  ObUpgradePath path;
  ASSERT_NE(ObUpgradeChecker::get_upgrade_path(DATA_VERSION_4_2_0_0, path), OB_SUCCESS);
}

// 在已有的基础上增加421BP11
// 因为421BP11不能升级到4251，所以假设421BP11需要升级到4252
TEST_F(TestUpgradePath, 421BP11and4252)
{
  ObUpgradePath path;
  std::set<uint64_t> versions;
  upgrade_path_last = upgrade_path_last_standard;
  upgrade_path_current = upgrade_path_current_standard;

  upgrade_path_last = {
    { DATA_VERSION_4_2_1_10, DATA_VERSION_4_2_5_1 },
    { DATA_VERSION_4_2_1_11, DATA_VERSION_4_2_5_2 },
  };
  upgrade_path_current.push_back(DATA_VERSION_4_2_5_2);

  // 421 10
  versions.insert(DATA_VERSION_4_2_1_10);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_1_10, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_1_11, true},
      {DATA_VERSION_4_2_2_0, false},
      {DATA_VERSION_4_2_2_1, false},
      {DATA_VERSION_4_2_3_0, false},
      {DATA_VERSION_4_2_3_1, false},
      {DATA_VERSION_4_2_4_0, false},
      {DATA_VERSION_4_2_5_0, false},
      {DATA_VERSION_4_2_5_1, false},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 421 11
  versions.insert(DATA_VERSION_4_2_1_11);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_1_11, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_2_0, false},
      {DATA_VERSION_4_2_2_1, false},
      {DATA_VERSION_4_2_3_0, false},
      {DATA_VERSION_4_2_3_1, false},
      {DATA_VERSION_4_2_4_0, false},
      {DATA_VERSION_4_2_5_0, false},
      {DATA_VERSION_4_2_5_1, false},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4220
  versions.insert(DATA_VERSION_4_2_2_0);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_2_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_2_1, true},
      {DATA_VERSION_4_2_3_0, true},
      {DATA_VERSION_4_2_3_1, true},
      {DATA_VERSION_4_2_4_0, true},
      {DATA_VERSION_4_2_5_0, true},
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4221
  versions.insert(DATA_VERSION_4_2_2_1);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_2_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_3_0, true},
      {DATA_VERSION_4_2_3_1, true},
      {DATA_VERSION_4_2_4_0, true},
      {DATA_VERSION_4_2_5_0, true},
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4230
  versions.insert(DATA_VERSION_4_2_3_0);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_3_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_3_1, true},
      {DATA_VERSION_4_2_4_0, true},
      {DATA_VERSION_4_2_5_0, true},
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4231
  versions.insert(DATA_VERSION_4_2_3_1);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_3_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_4_0, true},
      {DATA_VERSION_4_2_5_0, true},
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4240
  versions.insert(DATA_VERSION_4_2_4_0);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_4_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_5_0, true},
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4250
  versions.insert(DATA_VERSION_4_2_5_0);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_5_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4251
  versions.insert(DATA_VERSION_4_2_5_1);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_5_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4252
  versions.insert(DATA_VERSION_4_2_5_2);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_5_2, path), OB_SUCCESS);
  check_upgrade_path(path, {
      });
  ASSERT_EQ(versions.size(), upgrade_path_last.size() + upgrade_path_current.size());
}

// 在已有的基础上增加4252
TEST_F(TestUpgradePath, 4252)
{
  ObUpgradePath path;
  std::set<uint64_t> versions;
  upgrade_path_last = upgrade_path_last_standard;
  upgrade_path_current = upgrade_path_current_standard;
  upgrade_path_current.push_back(DATA_VERSION_4_2_5_2);
  // 421 10
  versions.insert(DATA_VERSION_4_2_1_10);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_1_10, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_2_0, false},
      {DATA_VERSION_4_2_2_1, false},
      {DATA_VERSION_4_2_3_0, false},
      {DATA_VERSION_4_2_3_1, false},
      {DATA_VERSION_4_2_4_0, false},
      {DATA_VERSION_4_2_5_0, false},
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  versions.insert(DATA_VERSION_4_2_2_0);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_2_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_2_1, true},
      {DATA_VERSION_4_2_3_0, true},
      {DATA_VERSION_4_2_3_1, true},
      {DATA_VERSION_4_2_4_0, true},
      {DATA_VERSION_4_2_5_0, true},
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4221
  versions.insert(DATA_VERSION_4_2_2_1);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_2_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_3_0, true},
      {DATA_VERSION_4_2_3_1, true},
      {DATA_VERSION_4_2_4_0, true},
      {DATA_VERSION_4_2_5_0, true},
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4230
  versions.insert(DATA_VERSION_4_2_3_0);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_3_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_3_1, true},
      {DATA_VERSION_4_2_4_0, true},
      {DATA_VERSION_4_2_5_0, true},
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4231
  versions.insert(DATA_VERSION_4_2_3_1);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_3_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_4_0, true},
      {DATA_VERSION_4_2_5_0, true},
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4240
  versions.insert(DATA_VERSION_4_2_4_0);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_4_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_5_0, true},
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4250
  versions.insert(DATA_VERSION_4_2_5_0);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_5_0, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_5_1, true},
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4251
  versions.insert(DATA_VERSION_4_2_5_1);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_5_1, path), OB_SUCCESS);
  check_upgrade_path(path, {
      {DATA_VERSION_4_2_5_2, true},
      });
  // 4252
  versions.insert(DATA_VERSION_4_2_5_2);
  ASSERT_EQ(get_upgrade_path(DATA_VERSION_4_2_5_2, path), OB_SUCCESS);
  check_upgrade_path(path, {
      });
  ASSERT_EQ(versions.size(), upgrade_path_last.size() + upgrade_path_current.size());
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
