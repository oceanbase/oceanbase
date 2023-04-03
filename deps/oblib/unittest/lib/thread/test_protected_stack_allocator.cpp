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
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/resource/ob_resource_mgr.h"
#include "lib/coro/testing.h"
#define private public
#include "lib/thread/protected_stack_allocator.h"

using namespace std;
using namespace oceanbase::lib;
using namespace oceanbase::common;

static constexpr int64_t DEFAULT_STACK_SIZE = (1 << 20) * 2; // 2M
static constexpr int64_t MAX_STACK_SIZE = (1 << 20) * 16; // 16M
using CPSA = ProtectedStackAllocator;
#define PAGE_SIZE CPSA::page_size()
#define ALLOC(size) g_stack_allocer.alloc(1, size)
#define DEALLOC(ptr) g_stack_allocer.dealloc(ptr)


//struct MhuGuard
//{
//  MhuGuard(CPSA::IMemHoldUpdater &mhu)
//  {
//    orig_mhu_ = CPSA::ref_mhu_;
//    CPSA::ref_mhu_ = &mhu;
//  }
//  ~MhuGuard() { CPSA::ref_mhu_ = orig_mhu_; }
//  CPSA::IMemHoldUpdater *orig_mhu_;
//};

class DoNothingMhu //: public CPSA::IMemHoldUpdater
{
public:
  virtual int update_hold(const uint64_t tenant_id ,const ssize_t size, bool &updated)
  {
    UNUSED(tenant_id);
    UNUSED(size);
    updated = true;
    return OB_SUCCESS;
  }
};

class TestHoldMhu //: public CPSA::IMemHoldUpdater
{
public:
  TestHoldMhu()
    : hold_(0), limit_(0)
  {}
  void reset()
  {
    hold_ = 0;
    limit_ = 0;
  }
  virtual int update_hold(const uint64_t tenant_id, const ssize_t size, bool &updated)
  {
    UNUSED(tenant_id);
    updated = false;
    if (hold_ + size <= limit_) {
      hold_ += size;
      updated = true;
    }
    return OB_SUCCESS;
  }
  int64_t hold_;
  int64_t limit_;

};

DoNothingMhu my_mhu;

void test_fixed_size(const int64_t size)
{
  cout << "size: " << size << endl;
  //MhuGuard guard(my_mhu);
  void *buf = ALLOC(size);
  ASSERT_NE(buf, nullptr);
  memset(buf, '1', size);
  char *str = new char[size];
  memset(str, '1', size);
  int cmp = memcmp(buf, str, size);
  EXPECT_EQ(cmp, 0);
  DEALLOC(buf);
  delete []str;
}

TEST(TestCPSA, Main)
{
  auto size = DEFAULT_STACK_SIZE;
  test_fixed_size(size);
  size = DEFAULT_STACK_SIZE - (4 << 10);
  test_fixed_size(size);
  size = DEFAULT_STACK_SIZE + (4 << 10);
  test_fixed_size(size);
  size = MAX_STACK_SIZE;
  test_fixed_size(size);
  size = PAGE_SIZE;
  test_fixed_size(size);
}

TEST(TestCPSA, EXCEPTION)
{
  //MhuGuard guard(my_mhu);
  // not page_size align is allowed
  ASSERT_NE(nullptr, ALLOC(DEFAULT_STACK_SIZE - 1));
  ASSERT_EQ(nullptr, ALLOC(-1));
  ASSERT_EQ(nullptr, ALLOC(0));
  ASSERT_EQ(nullptr, ALLOC(INT64_MAX));

  void *buf = nullptr;
  ASSERT_NE(nullptr, buf = ALLOC(-UINT64_MAX + DEFAULT_STACK_SIZE - 1));
  DEALLOC(buf);

  // nullptr ptr will be ignored
  DEALLOC(nullptr);
}

TEST(TestCPSA, DISABLED_HOLD_MOCK)
{
  TestHoldMhu mhu;
  //MhuGuard guard(mhu);

  mhu.limit_ = 1 << 20;
  ASSERT_EQ(nullptr, ALLOC(DEFAULT_STACK_SIZE));

  mhu.reset();
  mhu.limit_ = 1 << 30;
  mhu.hold_ = (1 << 30) - DEFAULT_STACK_SIZE;
  ASSERT_EQ(nullptr, ALLOC(DEFAULT_STACK_SIZE));

  mhu.reset();
  mhu.limit_ = 1 << 30;
  mhu.hold_ = (1 << 30) - DEFAULT_STACK_SIZE - PAGE_SIZE;
  void *buf = NULL;
  ASSERT_NE(nullptr, buf = ALLOC(DEFAULT_STACK_SIZE));
  ASSERT_EQ(nullptr, ALLOC(DEFAULT_STACK_SIZE));
  DEALLOC(buf);

  mhu.reset();
  int64_t size = 8 << 20;
  int64_t N = 100;
  mhu.limit_ = (size + PAGE_SIZE) * 100;
  vector<void *> bufs;
  for (int64_t i = 0; i < N; ++i) {
    void *buf = ALLOC(size);
    ASSERT_NE(nullptr, buf);
    memset(buf, '1', size);
    bufs.push_back(buf);
  }
  ASSERT_EQ(nullptr, ALLOC(size));
  for (int64_t i = 0; i < N; ++i) {
    DEALLOC(bufs[i]);
  }
  for (int64_t i = 0; i < N; ++i) {
    void *buf = ALLOC(size);
    ASSERT_NE(nullptr, buf);
    memset(buf, '1', size);
    DEALLOC(buf);
  }
}

using LimitFunc = std::function<void(ObTenantResourceMgrHandle &, const uint64_t)>;

void test_hold(LimitFunc func)
{
  const uint64_t tenant_id = 100;

  int64_t size = 8 << 20;
  const uint64_t limit = (size + PAGE_SIZE * 2) * 100;

  int ret = OB_SUCCESS;
  ObTenantResourceMgrHandle resource_handle;
  if (OB_FAIL(ObResourceMgr::get_instance().get_tenant_resource_mgr(
      tenant_id, resource_handle))) {
    LIB_LOG(ERROR, "get_tenant_resource_mgr failed", K(ret), K(tenant_id));
    return;
  } else {
    func(resource_handle, limit);
  }
  int64_t N = 100;
  vector<void *> bufs;
  for (int64_t i = 0; i < N; ++i) {
    void *buf = g_stack_allocer.alloc(tenant_id, size);
    ASSERT_NE(nullptr, buf);
    bufs.push_back(buf);
  }
  //ASSERT_EQ(nullptr, CPSA::alloc(tenant_id, size));
  for (int64_t i = 0; i < N; ++i) {
    DEALLOC(bufs[i]);
  }
  for (int64_t i = 0; i < N; ++i) {
    void *buf = g_stack_allocer.alloc(tenant_id, size);
    ASSERT_NE(nullptr, buf);
    DEALLOC(buf);
  }
}

TEST(TestCPSA, HOLD)
{
  test_hold(
    [](ObTenantResourceMgrHandle &resource_handle, const uint64_t limit)
            {
              resource_handle.get_memory_mgr()->set_limit(limit);
            }
    );
  test_hold(
    [](ObTenantResourceMgrHandle &resource_handle, const uint64_t limit)
            {
              resource_handle.get_memory_mgr()->set_ctx_limit(ObCtxIds::CO_STACK, limit);
            }
    );
}

TEST(TestCPSA, bench)
{
  auto begin_ts = ObTimeUtility::current_time();
  const auto CNT = 1000L * 10 ;
  for (auto i = 0; i < CNT; ++i) {
    auto buf = ALLOC(DEFAULT_STACK_SIZE);
    {
      int64_t  interval = 512 * (1 << 10);
      int64_t access_len = 1 << 10;
      for (int64_t j = 0; j < DEFAULT_STACK_SIZE; j+= interval) {
        memset((char *)buf + j, 'a', std::min(access_len, DEFAULT_STACK_SIZE - j + 1));
      }
    }
    if (nullptr == buf) {
      cout << i << endl;
      break;
    } else {
      DEALLOC(buf);
    }
  }
  auto end_ts = ObTimeUtility::current_time();
  cout << "alloc bench: " <<   CNT * 1000 / (end_ts - begin_ts) << "Kps" << endl;
}

#if 0
TEST(TestCPSA, ACCESS_GUARD)
{
  CPSA ca;
  void *buf = ca.alloc(DEFAULT_STACK_SIZE);
  ASSERT_NE(nullptr, buf);
  char *p = (char *) buf;
  cout << p[0] << endl;
  cout << p[-PAGE_SIZE - 1] << endl;
  cout << p[-1] << endl;
}
#endif

int main(int argc, char *argv[])
{
  OB_LOGGER.set_log_level(3);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
