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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include <gtest/gtest.h>

#define protected public
#define private public
#include "test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheResize : public ::testing::Test
{
public:
  TestSSMicroCacheResize() {}
  virtual ~TestSSMicroCacheResize() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
public:
  struct TestSSMicroCacheResizeCtx
  {
  public:
    TestSSMicroCacheResizeCtx()
        : micro_block_info_arr_(), thread_num_(0) {}
    ~TestSSMicroCacheResizeCtx() { reset(); }
    void reset()
    {
      micro_block_info_arr_.destroy();
      thread_num_ = 0;
    }
  public:
    ObArray<TestSSCommonUtil::MicroBlockInfo> micro_block_info_arr_;
    int64_t thread_num_;
  };

public:
  class TestSSMicroCacheResizeThread : public Threads
  {
  public:
    enum class TestParallelType
    {
      GET_MICRO_BLOCK_AND_RESIZE_LARGER,
    };
  public:
    TestSSMicroCacheResizeThread(ObTenantBase *tenant_base, TestSSMicroCacheResizeCtx &ctx, TestParallelType type)
        : tenant_base_(tenant_base), ctx_(ctx), type_(type), fail_cnt_(0) {}
    void run(int64_t idx) final;
    int64_t get_fail_cnt() { return ATOMIC_LOAD(&fail_cnt_); }
  private:
    int parallel_get_micro_block_and_resize_larger(int64_t idx);

  private:
    ObTenantBase *tenant_base_;
    TestSSMicroCacheResizeCtx &ctx_;
    TestParallelType type_;
    int64_t fail_cnt_;
  };
};

void TestSSMicroCacheResize::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheResize::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheResize::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 27))); // 128MB
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
}

void TestSSMicroCacheResize::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSMicroCacheResize::TestSSMicroCacheResizeThread::run(int64_t idx)
{
  ObTenantEnv::set_tenant(tenant_base_);
  if (type_ == TestParallelType::GET_MICRO_BLOCK_AND_RESIZE_LARGER) {
    parallel_get_micro_block_and_resize_larger(idx);
  }
}

int TestSSMicroCacheResize::TestSSMicroCacheResizeThread::parallel_get_micro_block_and_resize_larger(int64_t idx)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObArenaAllocator allocator;
  char *read_buf = nullptr;
  if (OB_ISNULL(read_buf = static_cast<char*>(allocator.alloc(DEFAULT_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else {
    for (int64_t epoch = 0; OB_SUCC(ret) && epoch < 3; epoch++) {
      if (idx == 1 && epoch == 1) {
        if (OB_FAIL(micro_cache->resize_micro_cache_file_size(micro_cache->cache_file_size_ * 2))) {
          LOG_WARN("fail to resize file size", KR(ret));
        }
      }
      for (int64_t i = idx; OB_SUCC(ret) && i < ctx_.micro_block_info_arr_.count(); i += ctx_.thread_num_) {
        TestSSCommonUtil::MicroBlockInfo micro_info = ctx_.micro_block_info_arr_[i];
        if (OB_FAIL(TestSSCommonUtil::get_micro_block(micro_info, read_buf))) {
          LOG_WARN("fail to get micro_block", KR(ret), K(micro_info));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    ATOMIC_INC(&fail_cnt_);
  }
  return ret;
}

TEST_F(TestSSMicroCacheResize, test_basic_resize_cache_file)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_basic_resize_cache_file");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  const int64_t ori_total_phy_blk_cnt = phy_blk_mgr.blk_cnt_info_.total_blk_cnt_;
  const int64_t ori_arc_limit = micro_meta_mgr.arc_info_.limit_;
  const int64_t new_cache_file_size = micro_cache->cache_file_size_ * 2;
  ASSERT_EQ(OB_SUCCESS, micro_cache->resize_micro_cache_file_size(new_cache_file_size));
  ASSERT_EQ(new_cache_file_size, micro_cache->cache_file_size_);
  ASSERT_EQ(new_cache_file_size, phy_blk_mgr.super_block_.cache_file_size_);
  ASSERT_LT(ori_total_phy_blk_cnt, phy_blk_mgr.blk_cnt_info_.total_blk_cnt_);
  ASSERT_LT(ori_arc_limit, micro_meta_mgr.arc_info_.limit_);
}

/*
  resize cache_file_szie between begin_free_space_for_prewarm() and finish_free_space_for_prewarm()
*/
TEST_F(TestSSMicroCacheResize, test_resize_between_free_space_for_prewarm)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_resize_between_free_space_for_prewarm");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSARCInfo &arc_info = micro_cache->micro_meta_mgr_.arc_info_;

  const int64_t old_limit = arc_info.limit_;
  micro_cache->begin_free_space_for_prewarm();
  const int64_t prewarm_work_limit = static_cast<int64_t>((static_cast<double>(old_limit * SS_ARC_LIMIT_SHRINK_PCT) / 100.0));
  ASSERT_EQ(prewarm_work_limit, arc_info.work_limit_);

  const int64_t new_cache_file_size = micro_cache->phy_blk_mgr_.total_file_size_ * 2;
  ASSERT_EQ(OB_SUCCESS, micro_cache->resize_micro_cache_file_size(new_cache_file_size));
  const int64_t new_limit = arc_info.limit_;
  ASSERT_LE(old_limit * 2, new_limit);
  ASSERT_LT(prewarm_work_limit * 2, arc_info.work_limit_);

  micro_cache->finish_free_space_for_prewarm();
  ASSERT_EQ(new_limit, arc_info.work_limit_);
  ASSERT_EQ(new_limit, arc_info.limit_);
  const int64_t p = static_cast<int64_t>((static_cast<double>(new_limit * arc_info.DEF_ARC_P_OF_LIMIT_PCT) / 100.0));
  const int64_t max_p = new_limit * arc_info.MAX_ARC_P_OF_LIMIT_PCT / 100;
  const int64_t min_p = new_limit * arc_info.MIN_ARC_P_OF_LIMIT_PCT / 100;
  ASSERT_LE(abs(p - arc_info.p_), 5); // the conversion between 'int64_t' and 'double' causes some deviations
  ASSERT_EQ(max_p, arc_info.max_p_);
  ASSERT_EQ(min_p, arc_info.min_p_);
}

/*
  Multiple threads read micro blocks in parallel, and thread 0 will resize the file size from 128M to 256M. Check the
  data_blk_used_cnt exceeds total_data_blk_cnt.
*/
TEST_F(TestSSMicroCacheResize, test_get_micro_block_and_resize_larger)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_get_micro_block_and_resize_larger");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  const int64_t origin_data_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt();
  const int64_t total_macro_blk_cnt = origin_data_blk_cnt * 2;
  const int32_t block_size = phy_blk_mgr.block_size_;
  TestSSMicroCacheResizeCtx ctx;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::prepare_micro_blocks(total_macro_blk_cnt, block_size, ctx.micro_block_info_arr_, 1, true, 100 * 1024, 160 * 1024));

  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;

  ctx.thread_num_ = 4;
  TestSSMicroCacheResize::TestSSMicroCacheResizeThread threads(
    ObTenantEnv::get_tenant(), ctx, TestSSMicroCacheResizeThread::TestParallelType::GET_MICRO_BLOCK_AND_RESIZE_LARGER);
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  threads.set_thread_count(ctx.thread_num_);
  threads.start();
  threads.wait();
  ASSERT_EQ(0, threads.get_fail_cnt());
  ASSERT_LT(origin_data_blk_cnt, phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_resize.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_resize.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}