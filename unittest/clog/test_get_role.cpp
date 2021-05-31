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
#include "clog/ob_log_define.h"

namespace oceanbase {
using namespace clog;
using namespace common;
namespace unittest {
struct TestClass {
  union {
    int64_t tmp_val_;
    struct {
      common::ObRole role_;
      int16_t state_;
      bool is_removed_;
    };
  };

  int get_role_and_state() const
  {
    int ret = OB_SUCCESS;
    const int64_t tmp = ATOMIC_LOAD(&tmp_val_);
    if (tmp >> 48) {  // check is_removed_
      ret = OB_NOT_MASTER;
    } else if (((tmp & ((1ll << 48) - 1)) >> 32) != ACTIVE) {  // check state_
      ret = OB_NOT_MASTER;
    } else if ((tmp & ((1ll << 32) - 1)) != LEADER) {  // check role_
      ret = OB_NOT_MASTER;
    }

    const bool is_removed = static_cast<bool>(tmp >> 48);
    const int16_t state = static_cast<int16_t>((tmp & ((1ll << 48) - 1)) >> 32);
    const ObRole role = static_cast<ObRole>(tmp & ((1ll << 32) - 1));
    CLOG_LOG(INFO, "info", K(is_removed), K(state), K(role));
    return ret;
  }
};

TEST(test_get_role, test1)
{
  TestClass a;
  a.tmp_val_ = 0;
  a.role_ = LEADER;
  a.state_ = ACTIVE;
  a.is_removed_ = false;
  EXPECT_EQ(common::OB_SUCCESS, a.get_role_and_state());
  a.role_ = FOLLOWER;
  EXPECT_EQ(common::OB_NOT_MASTER, a.get_role_and_state());
  a.role_ = LEADER;
  a.state_ = RECONFIRM;
  EXPECT_EQ(common::OB_NOT_MASTER, a.get_role_and_state());
  a.state_ = ACTIVE;
  a.is_removed_ = true;
  EXPECT_EQ(common::OB_NOT_MASTER, a.get_role_and_state());
  a.is_removed_ = false;
  EXPECT_EQ(common::OB_SUCCESS, a.get_role_and_state());
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_get_role.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_get_role");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
