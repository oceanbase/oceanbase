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

#include "lib/hash/ob_hashtable.h"
#include "lib/hash/ob_serialization.h"
#include "lib/allocator/ob_malloc.h"
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <ext/hash_map>
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

const int64_t ITEM_NUM = 1024 * 128;

struct data_t {
  uint32_t construct_flag;
  char data[1024];
  data_t()
  {
    construct_flag = 0xffffffff;
  };
};

typedef SimpleAllocer<data_t, 128> allocer_t;

template <class T>
void shuffle(T* array, const int64_t array_size)
{
  srand(static_cast<int32_t>(time(NULL)));
  for (int64_t i = 0; i < array_size; i++) {
    int64_t swap_pos = rand() % array_size;
    std::swap(array[i], array[swap_pos]);
  }
}

TEST(TestSimpleAllocer, allocate)
{
  allocer_t al;
  data_t* nil = NULL;

  data_t* store[ITEM_NUM];
  for (int64_t i = 0; i < ITEM_NUM; i++) {
    EXPECT_NE(nil, (store[i] = al.alloc()));
    EXPECT_EQ(0xffffffff, store[i]->construct_flag);
  }
  for (int64_t i = 0; i < ITEM_NUM; i++) {
    store[i]->construct_flag = 0x00000000;
    al.free(store[i]);
  }
  for (int64_t i = ITEM_NUM - 1; i >= 0; i--) {
    data_t* tmp = NULL;
    EXPECT_NE(nil, (tmp = al.alloc()));
    EXPECT_EQ(0xffffffff, tmp->construct_flag);
  }
}

TEST(TestSimpleAllocer, free)
{
  allocer_t al;

  data_t* store[ITEM_NUM];
  for (int64_t i = 0; i < ITEM_NUM; i++) {
    store[i] = al.alloc();
  }
  al.free(NULL);
  shuffle(store, ITEM_NUM);
  for (int64_t i = 0; i < ITEM_NUM; i++) {
    al.free(store[i]);
  }
  for (int64_t i = ITEM_NUM - 1; i >= 0; i--) {
    data_t* tmp = NULL;
    EXPECT_EQ(store[i], (tmp = al.alloc()));
  }
  for (int64_t i = 0; i < ITEM_NUM; i++) {
    al.free(store[i]);
    if (0 == i % 128) {
      al.gc();
    }
  }
}

TEST(TestSimpleAllocer, DISABLED_overflow)
{
  allocer_t al;
  data_t* nil = NULL;
  data_t* data[2];
  data_t tmp;

  EXPECT_NE(nil, (data[0] = al.alloc()));
  // this case is used to test overflow
  // copy more bytes than sizeof(data[0]). (actually safe)
  // but Coverity treat it as a potential bug
  memcpy(&tmp, data[0], sizeof(data_t) + sizeof(uint32_t));
  memset(data[0], -1, sizeof(data_t) + sizeof(uint32_t));
  al.free(data[0]);
  memcpy(data[0], &tmp, sizeof(data_t) + sizeof(uint32_t));
  al.free(data[0]);

  memset(data[0], -1, sizeof(data_t) + sizeof(uint32_t));
  EXPECT_EQ(nil, (data[1] = al.alloc()));
  memcpy(data[0], &tmp, sizeof(data_t) + sizeof(uint32_t));
  EXPECT_NE(nil, (data[1] = al.alloc()));
  EXPECT_EQ(data[0], data[1]);
}

int main(int argc, char** argv)
{
  ObLogger::get_logger().set_log_level("ERROR");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
