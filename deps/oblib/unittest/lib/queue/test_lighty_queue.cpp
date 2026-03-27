/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "test_lighty_queue.h"
#include "deps/oblib/src/lib/queue/ob_lighty_queue.h"
#include <gtest/gtest.h>
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

#include <locale.h>
int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
