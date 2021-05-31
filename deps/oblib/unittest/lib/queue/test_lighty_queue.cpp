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

void Productor::run1()
{
  while (!has_set_stop()) {
    q_.push((void*)1);
  }
}

void Consumer::run1()
{
  int ret = oceanbase::common::OB_SUCCESS;
  while (!has_set_stop()) {
    void* data = NULL;
    if (!OB_FAIL(q_.pop(data, 1000000L))) {
      cout << "done" << endl;
      ATOMIC_INC(&cnt_);
    };
  }
}

int run()
{
  LightyQueue queue;
  queue.init(1 << 16);

  // queue.push((void*)1);
  cotesting::FlexPool pool(
      [&queue] {
        for (int i = 0; i < 100; i++) {
          void* task = nullptr;
          queue.pop(task, 1000000);
          cout << (int64_t)task << endl;
        }
      },
      1);
  pool.start(false);
  this_routine::usleep(1000000);
  cotesting::FlexPool([&queue](int64_t idx) { queue.push((void*)idx); }, 10, 9).start();
  pool.wait();
  // int p_th_cnt = 1;
  // int c_th_cnt = 1;

  // if (argc > 1) {
  //   p_th_cnt = atoi(argv[1]);
  // }

  // if (argc > 2) {
  //   c_th_cnt = atoi(argv[2]);
  // }

  // // Productor p(queue, p_th_cnt);
  // Consumer c(queue, c_th_cnt);

  // // LIB_LOG(INFO, "process begin", "cnt", c.get_cnt());
  // c.start();
  // queue.push((void*)1);
  // this_routine::usleep(1000);
  // c.stop();
  // c.wait();
  // p.start();

  // sleep(10);
  // p.stop();
  // c.stop();

  // p.wait();
  // c.wait();

  // LIB_LOG(INFO, "process done", "cnt", c.get_cnt());
  // cout << c.get_cnt() << endl;

  return 0;
}

TEST(TestLightyQueue, Main)
{
  run();
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
