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

#include "test_lighty_queue.h"
#include <iostream>
#include <gtest/gtest.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/oblog/ob_log.h"
#include "lib/coro/testing.h"

using namespace oceanbase::common;
using namespace std;

int run()
{
  ObLightyQueue queue;
  queue.init(1<<16);

  cotesting::FlexPool pool([&queue] {
    for (int i = 0; i < 100; i++) {
      void *task = nullptr;
      queue.pop(task, 1000000);
      cout << (int64_t)task << endl;
    }
  }, 1);
  pool.start(false);
  ::usleep(1000000);
  cotesting::FlexPool([&queue] (){
    for (auto i = 0; i < 10; ++i) {
      queue.push((void*)1);
    }
  }, 10).start();
  pool.wait();

  return 0;
}

TEST(TestObLightyQueue, Main)
{
  run();
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
