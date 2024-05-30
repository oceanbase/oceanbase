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
#include <iostream>

#include "lib/alloc/ob_malloc_callback.h"
#include "lib/allocator/ob_malloc.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

class TestObMallocCallbackGuard : public ::testing::Test
{
};

class MallocCallback final : public ObMallocCallback
{
public:
  MallocCallback(int64_t& hold) : hold_(hold) {}
  virtual void operator()(const ObMemAttr& attr, int64_t used) override
  {
    UNUSED(attr);
    hold_ += used;
    std::cout << hold_ << " " << used << std::endl;
  }
private:
  int64_t& hold_;
};

TEST_F(TestObMallocCallbackGuard, AllocAndFree)
{
  int64_t hold = 0;
  MallocCallback cb(hold);
  ObMallocCallbackGuard guard(cb);
  auto *ptr = ob_malloc(2113, ObNewModIds::TEST);
  std::cout << "alloc" << std::endl;
  ASSERT_EQ(hold, 2113);
  ob_free(ptr);
  std::cout << "free" << std::endl << std::endl;
  ASSERT_EQ(hold, 0);
  {
    int64_t hold2 = 0;
    MallocCallback cb(hold2);
    ObMallocCallbackGuard guard(cb);
    auto *ptr = ob_malloc(2113, ObNewModIds::TEST);
    ASSERT_EQ(hold, 2113);
    ASSERT_EQ(hold2, 2113);
    std::cout << "alloc" << std::endl;
    ob_free(ptr);
    ASSERT_EQ(hold, 0);
    ASSERT_EQ(hold2, 0);
    std::cout << "free" << std::endl << std::endl;
  }
  ptr = ob_malloc(2113, ObNewModIds::TEST);
  ASSERT_EQ(hold, 2113);
  std::cout << "alloc" << std::endl;
  ob_free(ptr);
  std::cout << "free" << std::endl;
  ASSERT_EQ(hold, 0);
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
