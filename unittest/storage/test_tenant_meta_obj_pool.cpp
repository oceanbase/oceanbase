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

#define USING_LOG_PREFIX STORAGE

#define protected public
#define private public

#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tenant_meta_obj_pool.h"

namespace oceanbase
{
namespace storage
{

class TestTenantMetaObjPool : public ::testing::Test
{
public:
  TestTenantMetaObjPool();
  virtual ~TestTenantMetaObjPool() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;

  static const int64_t MAX_TEST_TABLET_CNT_IN_OBJ_POOL = 10000;

private:
  ObTenantMetaObjPool<ObTablet> obj_pool_;
  ObTenantMetaMemMgr t3m_;
};

TestTenantMetaObjPool::TestTenantMetaObjPool()
  : obj_pool_(common::OB_SERVER_TENANT_ID, MAX_TEST_TABLET_CNT_IN_OBJ_POOL, "TestPool", ObCtxIds::META_OBJ_CTX_ID),
    t3m_(common::OB_SERVER_TENANT_ID)
{
}

void TestTenantMetaObjPool::SetUp()
{
}

void TestTenantMetaObjPool::TearDown()
{
}

class TestConcurrentMetaObjPool : public share::ObThreadPool
{
public:
  TestConcurrentMetaObjPool(const int64_t thread_cnt, ObTenantMetaObjPool<ObTablet> &meta_obj_pool);
  virtual ~TestConcurrentMetaObjPool();
  virtual void run1();

  static const int64_t TABLET_CNT_PER_THREAD = 1000;

private:
  int64_t thread_cnt_;
  ObTenantMetaObjPool<ObTablet> &obj_pool_;
  common::ObSEArray<ObTablet *,64> obj_array_;
  common::SpinRWLock lock_;
};

TestConcurrentMetaObjPool::TestConcurrentMetaObjPool(const int64_t thread_cnt,
                                                     ObTenantMetaObjPool<ObTablet> &meta_obj_pool)
  : thread_cnt_(thread_cnt),
    obj_pool_(meta_obj_pool),
    obj_array_(),
    lock_()
{
  set_thread_count(static_cast<int32_t>(thread_cnt_));
}

TestConcurrentMetaObjPool::~TestConcurrentMetaObjPool()
{
  SpinWLockGuard guard(lock_);
  for (int64_t i = 0; i < obj_array_.count(); i++) {
    obj_pool_.release(obj_array_.at(i));
  }
  obj_array_.reset();
}

void TestConcurrentMetaObjPool::run1()
{
  int ret = OB_SUCCESS;
  int count = TABLET_CNT_PER_THREAD;
  while (count-- > 0) {
    ObTablet *tablet = nullptr;
    int ret = obj_pool_.acquire(tablet);
    ASSERT_EQ(common::OB_SUCCESS, ret);
    {
      SpinWLockGuard guard(lock_);
      ret = obj_array_.push_back(tablet);
      ASSERT_EQ(common::OB_SUCCESS, ret);
    }
  }
}

TEST_F(TestTenantMetaObjPool, test_acquire_and_release)
{
  ObTablet *tablet = nullptr;
  int ret = obj_pool_.acquire(tablet);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  ASSERT_TRUE(nullptr != tablet);
  ASSERT_EQ(1, obj_pool_.inner_used_num_);

  obj_pool_.release(tablet);
  ASSERT_EQ(0, obj_pool_.inner_used_num_);
}

TEST_F(TestTenantMetaObjPool, test_multi_acquire_and_release)
{
  const int64_t thread_cnt = 16;
  TestConcurrentMetaObjPool multi_thread(thread_cnt, obj_pool_);

  int ret = multi_thread.start();
  ASSERT_EQ(OB_SUCCESS, ret);
  multi_thread.wait();

  ASSERT_TRUE(thread_cnt * TestConcurrentMetaObjPool::TABLET_CNT_PER_THREAD > obj_pool_.inner_used_num_);
  ASSERT_EQ(1 * MAX_TEST_TABLET_CNT_IN_OBJ_POOL, obj_pool_.inner_used_num_);
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tenant_meta_obj_pool.log*");
  OB_LOGGER.set_file_name("test_tenant_meta_obj_pool.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

