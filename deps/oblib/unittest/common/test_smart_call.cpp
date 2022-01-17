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
#include <pthread.h>
#include <string>
#include "common/ob_smart_call.h"
#include "lib/coro/co_user_thread.h"

using namespace std;

namespace oceanbase {
namespace common {

int dec(int& i)
{
  if (i <= 0) {
    return OB_SUCCESS;
  } else {
    return dec(--i);
  }
}

TEST(sc, usability)
{
  int ret = OB_SUCCESS;
  // global function
  {
    int i = 10;
    ret = SMART_CALL(dec(i));
    EXPECT_EQ(ret, OB_SUCCESS);
    EXPECT_EQ(i, 0);
  }

  // member function
  {
    class Foo {
    public:
      int dec()
      {
        if (i_ <= 0) {
          return OB_SUCCESS;
        } else {
          --i_;
          return SMART_CALL(dec());
        }
      }
      int i_ = 10;
    };
    Foo foo;
    EXPECT_EQ(OB_SUCCESS, SMART_CALL(foo.dec()));
    EXPECT_EQ(foo.i_, 0);
  }

  // lambda && error code
  EXPECT_EQ(OB_ERR_UNEXPECTED, SMART_CALL([]() { return OB_ERR_UNEXPECTED; }()));
}

void* cur_stack_addr = nullptr;
size_t cur_stack_size = 0;
int stack_change_cnt = 0;
const int64_t s_size = STACK_PER_EXTEND;
int test(int& i, int once_invoke_hold)
{
  int ret = OB_SUCCESS;
  void* stack_addr = nullptr;
  size_t stack_size = 0;
  get_stackattr(stack_addr, stack_size);
  if (stack_addr != cur_stack_addr) {
    if (stack_size != STACK_PER_EXTEND) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      char tmp = '\0';
      bool is_overflow = false;
      if (&tmp < (char*)stack_addr || &tmp > (char*)stack_addr + stack_size) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(check_stack_overflow(is_overflow))) {
      } else if (is_overflow) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        stack_change_cnt += 1;
        cur_stack_addr = stack_addr;
        cur_stack_size = stack_size;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (i <= 0) {
      ret = OB_SUCCESS;
    } else {
      char buf[once_invoke_hold];
      MEMSET(buf, 0, once_invoke_hold);
      ret = SMART_CALL(test(--i, once_invoke_hold));
      void* stack_addr_after = nullptr;
      size_t stack_size_after = 0;
      get_stackattr(stack_addr_after, stack_size_after);
      if (stack_addr_after != stack_addr || stack_size_after != stack_size) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  return ret;
}

void* run(void*)
{
  int ret = OB_SUCCESS;
  // half, single, double
  for (int k = 0; k < 3; k++) {
    int i = s_size / STACK_RESERVED_SIZE * 0.5 * (1 << k);
    size_t stack_size = 0;
    get_stackattr(cur_stack_addr, stack_size);
    stack_change_cnt = 0;
    ret = test(i, STACK_RESERVED_SIZE);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(0, i);
    EXPECT_EQ(stack_change_cnt, k);
  }

  // total size overflow
  {
    int i = ALL_STACK_LIMIT / STACK_RESERVED_SIZE;
    size_t stack_size = 0;
    get_stackattr(cur_stack_addr, stack_size);
    stack_change_cnt = 0;
    ret = test(i, STACK_RESERVED_SIZE);
    EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
    EXPECT_EQ(1 + stack_change_cnt, 1 + (ALL_STACK_LIMIT - stack_size) / STACK_PER_EXTEND);
  }
  void* stack_addr = nullptr;
  size_t stack_size = 0;
  get_stackattr(stack_addr, stack_size);
  EXPECT_EQ(all_stack_size, stack_size);

  return nullptr;
}

TEST(sc, thread)
{
  pthread_t th;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, s_size);
  pthread_create(&th, &attr, oceanbase::common::run, nullptr);
  pthread_join(th, nullptr);
  pthread_attr_destroy(&attr);
}

TEST(sc, coro)
{
  lib::coro::config().stack_size_ = s_size;
  class : public lib::CoKThread {
    void run(int64_t) final
    {
      oceanbase::common::run(nullptr);
    }
  } th;
  th.start();
  th.wait();
}

}  // end namespace common
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
