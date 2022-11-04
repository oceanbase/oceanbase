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
#include "lib/wait_event/ob_wait_event.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{
TEST(ObWaitEventDesc, normal)
{
  ObWaitEventDesc des1;
  ObWaitEventDesc des2;

  des1.reset();
  des1.wait_begin_time_ = 1;
  des2.reset();
  des2.wait_begin_time_ = 2;
  ASSERT_TRUE(des1 < des2);
  ASSERT_TRUE(des2 > des1);

  des1.add(des2);
  ASSERT_TRUE(des1 == des2);

  COMMON_LOG(INFO, "ObWaitEventDesc, ", K(des1));
}

TEST(ObWaitEventStat, normal)
{
  ObWaitEventStat stat1;
  ObWaitEventStat stat2;
  ObWaitEventStat stat3;

  stat1.reset();
  stat1.total_waits_ = 1;
  stat1.time_waited_ = 10;
  stat1.max_wait_ = 10;
  stat1.total_timeouts_ = 0;

  stat2.reset();
  stat2.total_waits_ = 2;
  stat2.time_waited_ = 10;
  stat2.max_wait_ = 6;
  stat2.total_timeouts_ = 0;

  stat3 = stat1;
  stat3.add(stat2);
  ASSERT_EQ(stat3.total_waits_, 3);
  ASSERT_EQ(stat3.time_waited_, 20);
  ASSERT_EQ(stat3.max_wait_, 10);
  ASSERT_EQ(stat3.total_timeouts_, 0);
}

}
}

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


