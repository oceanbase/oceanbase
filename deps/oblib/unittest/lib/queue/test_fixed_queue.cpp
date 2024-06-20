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

#include "lib/queue/ob_fixed_queue.h"
#include <gtest/gtest.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/oblog/ob_log.h"
#include "lib/coro/testing.h"

using namespace oceanbase::common;
using namespace std;

int run()
{
  ObFixedQueue<void> queue;
  queue.init(64);

  cotesting::FlexPool pool([&queue] {
    for (int i = 0; i < 100; i++) {
      void *task = nullptr;
      queue.pop(task);
    //   cout << (int64_t)task << endl;
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
#define PUSH_CNT 800
#define PUSH_THREAD_CNT 2
#define POP_THREAD_CNT 2
#define ROUND_COUNT 5000
int run_order_test() {
  int failed_cnt = 0;
  ObOrderedFixedQueue<void> queue;
  // ObFixedQueue<void> queue;
  queue.init(4);
  srand(time(0));
  int64_t pop_cnt = 0;
  int64_t pop_index = 0;
  cotesting::FlexPool pool([&queue, &failed_cnt, &pop_cnt, &pop_index] {
    int ret = OB_SUCCESS;
    int64_t pop_thread_index = ATOMIC_FAA(&pop_index, 1);
    int64_t *output = (int64_t *)calloc(PUSH_THREAD_CNT * PUSH_CNT, sizeof(int64_t));
    int64_t *output_index = (int64_t *)calloc(PUSH_THREAD_CNT, sizeof(int64_t));
    int64_t start_pop_time = get_cur_ts();
    while (pop_cnt < PUSH_CNT * PUSH_THREAD_CNT) {
      void *task = nullptr;
      if (OB_FAIL(queue.pop(task))) {
        // printf("pop failed, ret = %d\n", ret);
      } else {
        ATOMIC_FAA(&pop_cnt, 1);
        int64_t value = (int64_t)task;
        int thread_index = value/1000;
        output[thread_index * PUSH_CNT + (output_index[thread_index]++)] = value;
        if (pop_thread_index == 0) {
          // fprintf(stdout, "%ld ", (int64_t)task);
        }
      }
      int64_t cur_time = get_cur_ts();
      if (cur_time - start_pop_time > 10 * 1000 * 1000) {
        fprintf(stderr, "pop cost too much time, cur_time=%ld, start_pop_time=%ld\n", cur_time, start_pop_time);
        ATOMIC_FAA(&failed_cnt, -10000);
        break;
      }
    }
    if (pop_thread_index == 0) {
      for (int i = 0; ATOMIC_LOAD(&failed_cnt) == 0 && i < PUSH_THREAD_CNT; i++) {
        for (int j = 0; j < PUSH_CNT; j++) {
          int64_t value = output[i * PUSH_CNT + j];
          if (value == 0) {
            break;
          }
          fprintf(stdout, "%ld ", value);
          if (j > 0 && value < output[i * PUSH_CNT + j - 1]) {
            fprintf(stdout, "%ld_ERROR ", value);
            ATOMIC_FAA(&failed_cnt, 1);
          }
        }
        fprintf(stdout, "\n");
      }
    }
    free(output);
    free(output_index);
  }, POP_THREAD_CNT);
  pool.start(false);
  int64_t index = 0;
  cotesting::FlexPool([&queue, &index] () {
    int64_t thread_index = ATOMIC_FAA(&index, 1);
    int64_t start_push_time = get_cur_ts();
    for (auto i = 0; i < PUSH_CNT; ++i) {
      int64_t value = thread_index * 1000 + i + 1;
      int ret = OB_SUCCESS;
      do {
        if (OB_FAIL(queue.push((void*)(value)))) {
          // printf("[%ld] push failed, ret = %d, count = %ld\n", thread_index, ret, queue.count_);
          usleep(rand() % 60);
        }
      } while (ret != OB_SUCCESS);
      int64_t cur_time = get_cur_ts();
      if (cur_time - start_push_time > 10 * 1000 * 1000) {
        fprintf(stderr, "push cost too much time, cur_time=%ld, start_push_time=%ld\n", cur_time, start_push_time);
        break;
      }
    }
  }, PUSH_THREAD_CNT).start();
  pool.wait();
  return failed_cnt;
}

TEST(TestObLightyQueue, Main)
{
  run();
}
TEST(TestObLightyQueue, OrderTest)
{
  for (int i = 0; i < ROUND_COUNT; i++) {
    printf("ROUND %d:\n", i);
    int ret = run_order_test();
    ASSERT_EQ(ret, 0);
  }
}

int main(int argc, char *argv[])
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
