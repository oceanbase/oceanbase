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

#include "lib/oblog/ob_log.h"
#include "lib/container/ob_se_array.h"
#include "common/storage/ob_io_device.h"
#include "share/ob_local_device.h"
#include "share/ob_thread_pool.h"

namespace oceanbase
{
namespace unittest
{

class TestIOCBPool : public ::testing::Test
{
public:
  TestIOCBPool() = default;
  virtual ~TestIOCBPool() = default;
  virtual void SetUp();
  virtual void TearDown();
private:
  ObIOCBPool<share::ObLocalIOCB> iocb_pool_;
  common::ObFIFOAllocator allocator_;
};

void TestIOCBPool::SetUp()
{
  const ObMemAttr mem_attr(OB_SYS_TENANT_ID, "test_iocb_pool");
  ASSERT_EQ(OB_SUCCESS, allocator_.init(lib::ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr));
  ASSERT_EQ(OB_SUCCESS, iocb_pool_.init(allocator_, share::ObLocalDevice::DEFUALT_PRE_ALLOCATED_IOCB_COUNT));
}

void TestIOCBPool::TearDown()
{
  iocb_pool_.reset();
  allocator_.reset();
}

TEST_F(TestIOCBPool, test_single_alloc_free)
{
  share::ObLocalIOCB *iocb1 = iocb_pool_.alloc();
  ASSERT_TRUE(nullptr != iocb1);
  share::ObLocalIOCB *iocb2 = iocb_pool_.alloc();
  ASSERT_TRUE(nullptr != iocb2);
  ASSERT_EQ(iocb_pool_.pre_allocated_size_, iocb_pool_.free_iocbs_.get_total() + 2);
  iocb_pool_.free(iocb1);
  iocb_pool_.free(iocb2);
  ASSERT_EQ(iocb_pool_.pre_allocated_size_, iocb_pool_.free_iocbs_.get_total());
  
  iocb1 = iocb_pool_.alloc();
  ASSERT_TRUE(nullptr != iocb1);
  iocb2 = iocb_pool_.alloc();
  ASSERT_TRUE(nullptr != iocb2);
  ASSERT_EQ(iocb_pool_.pre_allocated_size_, iocb_pool_.free_iocbs_.get_total() + 2);
  iocb_pool_.free(iocb1);
  iocb_pool_.free(iocb2);
 
  const int64_t iocb_count = iocb_pool_.pre_allocated_size_ + 100;
  share::ObLocalIOCB *iocb_ptrs_[iocb_count];
  for (int count = 0; count < iocb_count; ++count) {
    share::ObLocalIOCB *iocb = iocb_pool_.alloc();
    ASSERT_TRUE(nullptr != iocb);
    iocb_ptrs_[count] = iocb;
  }
  ASSERT_EQ(0, iocb_pool_.free_iocbs_.get_total());
  for (int64_t i = 0; i < iocb_count; ++i) {
    iocb_pool_.free(iocb_ptrs_[i]);
  }
  ASSERT_EQ(iocb_pool_.pre_allocated_size_, iocb_pool_.free_iocbs_.get_total());
}

class TestIOCBPoolStress : public share::ObThreadPool
{
public:
  explicit TestIOCBPoolStress(ObIOCBPool<share::ObLocalIOCB> &iocb_pool)
    : thread_cnt_(0), iocb_pool_(iocb_pool), iocb_ptrs_(), is_inited_(false) {}
  virtual ~TestIOCBPoolStress() = default;
  int init(const int64_t thread_cnt);
  virtual void run1();
private:
  static const int ALLOC_IOCB_COUNT_PER_THREAD = 1024;

  int64_t thread_cnt_;
  ObIOCBPool<share::ObLocalIOCB> &iocb_pool_;
  share::ObLocalIOCB* iocb_ptrs_[ALLOC_IOCB_COUNT_PER_THREAD];
  bool is_inited_ = false;
};

int TestIOCBPoolStress::init(const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (thread_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(thread_cnt));
  } else {
    thread_cnt_ = thread_cnt;
    is_inited_ = true;
  }
  return ret;
}

void TestIOCBPoolStress::run1()
{
  for (int count = 0; count < ALLOC_IOCB_COUNT_PER_THREAD; ++count) {
    share::ObLocalIOCB *iocb = iocb_pool_.alloc();
    ASSERT_TRUE(nullptr != iocb);
    iocb_ptrs_[count] = iocb;
  }
  for (int64_t i = 0; i < ALLOC_IOCB_COUNT_PER_THREAD; ++i) {
    iocb_pool_.free(iocb_ptrs_[i]);
  }
}

TEST_F(TestIOCBPool, test_multi_thread)
{
  int ret = OB_SUCCESS;
  TestIOCBPoolStress stress(iocb_pool_);
  const int thread_cnt = 64;

  ret = stress.init(thread_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = stress.start();
  ASSERT_EQ(OB_SUCCESS, ret);

  stress.wait();
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_iocb_pool.log*");
  OB_LOGGER.set_file_name("test_iocb_pool.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

