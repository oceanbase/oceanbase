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

#include "lib/thread/ob_thread_lease.h"
#include <gtest/gtest.h>

using namespace oceanbase::common;
class TestObThreadLease: public ::testing::Test
{
public:
  TestObThreadLease() {}
  virtual ~TestObThreadLease(){}
  virtual void SetUp()
  {
  }
  virtual void TearDown()
  {
  }

  static void SetUpTestCase()
  {
  }

  static void TearDownTestCase()
  {
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestObThreadLease);
protected:
  // function members
protected:
};

TEST_F(TestObThreadLease, smoke_test)
{
  ObThreadLease lease;

  ASSERT_EQ(ObThreadLease::IDLE, lease.value());
  ASSERT_EQ(true, lease.acquire());
  ASSERT_EQ(ObThreadLease::HANDLING, lease.value());
  ASSERT_EQ(true, lease.revoke());
  ASSERT_EQ(ObThreadLease::IDLE, lease.value());

  // 直接revoke，期望返回成功
  ASSERT_EQ(true, lease.revoke());
  ASSERT_EQ(ObThreadLease::IDLE, lease.value());
}

TEST_F(TestObThreadLease, simulate_multi_thread)
{
  ObThreadLease lease;
  ASSERT_EQ(ObThreadLease::IDLE, lease.value());

  for (int64_t i = 0; i < 100; i++) {
    ASSERT_EQ(true, lease.acquire());
    ASSERT_EQ(ObThreadLease::HANDLING, lease.value());

    // 再次acquire，期望失败，状态变更为READY
    ASSERT_EQ(false, lease.acquire());
    ASSERT_EQ(ObThreadLease::READY, lease.value());

    // 再次acquire，期望失败，状态变更为READY
    ASSERT_EQ(false, lease.acquire());
    ASSERT_EQ(ObThreadLease::READY, lease.value());

    // 再次acquire，期望失败，状态变更为READY
    ASSERT_EQ(false, lease.acquire());
    ASSERT_EQ(ObThreadLease::READY, lease.value());

    // revoke一次，期望失败，状态变更为HANDLING
    ASSERT_EQ(false, lease.revoke());
    ASSERT_EQ(ObThreadLease::HANDLING, lease.value());

    // 再次acquire，期望失败，状态变更为READY
    ASSERT_EQ(false, lease.acquire());
    ASSERT_EQ(ObThreadLease::READY, lease.value());

    // 再次acquire，期望失败，状态变更为READY
    ASSERT_EQ(false, lease.acquire());
    ASSERT_EQ(ObThreadLease::READY, lease.value());

    // revoke一次，期望失败，状态变更为HANDLING
    ASSERT_EQ(false, lease.revoke());
    ASSERT_EQ(ObThreadLease::HANDLING, lease.value());

    // 再次revoke，期望成功，状态变更为IDLE
    ASSERT_EQ(true, lease.revoke());
    ASSERT_EQ(ObThreadLease::IDLE, lease.value());
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

