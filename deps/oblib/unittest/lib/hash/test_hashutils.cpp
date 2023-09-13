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

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#define private public
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashutils.h"
#undef private
#include "lib/allocator/ob_malloc.h"

#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;
using namespace hash;

class TestHashUtils: public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

struct MySimpleAllocer
{
public:
  explicit MySimpleAllocer()
  {
    used_cnt_ = 0;
  }
  void *alloc(const int64_t sz)
  {
    used_cnt_++;
    return ob_malloc(sz, attr_);
  }
  void free(void *p)
  {
    ob_free(p);
    used_cnt_--;
  }
  void set_attr(const ObMemAttr &attr) { attr_ = attr; }
  void set_label(const lib::ObLabel &label) { attr_.label_ = label; }
  int64_t used_cnt_;
  ObMemAttr attr_;
};

TEST_F(TestHashUtils, Basic)
{
  static constexpr int NODE_NUM = 2;
  using TestAlloc = hash::SimpleAllocer<int, NODE_NUM, SpinMutexDefendMode, MySimpleAllocer>;
  TestAlloc alloc;
  int *obj = alloc.alloc();
  ASSERT_TRUE(obj != NULL);
  int *obj2 = alloc.alloc();
  ASSERT_TRUE(obj2 != NULL);
  alloc.free(obj2);
  int *obj3 = alloc.alloc();
  ASSERT_EQ(obj3, obj2);
  alloc.free(obj3);
  int *obj4 = alloc.alloc();
  ASSERT_EQ(obj4, obj3);
  alloc.free(obj);
  alloc.free(obj4);
  ASSERT_EQ(0, alloc.allocer_.used_cnt_);
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
