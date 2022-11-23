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

#include "common/ob_timeout_ctx.h"

namespace oceanbase
{
namespace common
{

void process_func(const int64_t timeout_us)
{
  const ObTimeoutCtx *cc = &ObTimeoutCtx::get_ctx();
  ASSERT_TRUE(cc->is_timeout_set());
  ASSERT_TRUE(cc->get_timeout() > timeout_us - 10000);
  ASSERT_TRUE(cc->get_timeout() < timeout_us + 10000);

  ::usleep((int32_t)timeout_us + 10000);
  ASSERT_TRUE(cc->is_timeouted());
}

TEST(ObTimeoutCtx, all)
{
  const ObTimeoutCtx *cc = &ObTimeoutCtx::get_ctx();
  ASSERT_FALSE(cc->is_timeout_set());
  ASSERT_FALSE(cc->is_timeouted());
  {
    ObTimeoutCtx ctx;
    ASSERT_NE(OB_SUCCESS, ctx.set_timeout(-1));
    ASSERT_NE(OB_SUCCESS, ctx.set_abs_timeout(-1));

    ASSERT_EQ(OB_SUCCESS, ctx.set_timeout(20000));
    int64_t abs_timeout = ctx.get_abs_timeout();
    process_func(20000);

    {
      ObTimeoutCtx ctx2;
      ASSERT_TRUE(ctx2.is_timeout_set());
      ASSERT_TRUE(ctx2.is_timeouted());

      ASSERT_EQ(OB_SUCCESS, ctx2.set_abs_timeout(::oceanbase::common::ObTimeUtility::current_time() + 30000));
      process_func(30000);
    }
    ASSERT_EQ(abs_timeout, ObTimeoutCtx::get_ctx().get_abs_timeout());
  }
  cc = &ObTimeoutCtx::get_ctx();
  ASSERT_FALSE(cc->is_timeout_set());
}

} // end namespace common
} // end namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
