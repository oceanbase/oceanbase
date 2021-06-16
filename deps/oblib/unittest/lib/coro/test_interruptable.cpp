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
#include "lib/async/event_base.h"
#include "lib/coro/testing.h"

using namespace oceanbase::lib;

TEST(TestInterruptable, Main)
{
  class : public EventBase {
  public:
    using EventBase::signal;
    using EventBase::wait4event;
  } eb;
  eb.init();

  cotesting::FlexPool()
      .create(
          [&eb] {
            eb.signal();
            eb.signal();
          },
          1)
      .create(
          [&eb] {
            this_routine::usleep(10000);
            eb.wait4event(1000000);
            eb.wait4event(1000000);
          },
          1)
      .start();
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
