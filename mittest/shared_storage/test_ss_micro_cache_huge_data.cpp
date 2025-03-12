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

class TestSSMicroCacheHugeData : public ::testing::Test
{
public:
  TestSSMicroCacheHugeData() {}
  virtual ~TestSSMicroCacheHugeData() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

public:
  struct TestSSMicroCacheHugeDataCtx
  {
  public:
    ObSSMicroCache *micro_cache_;
    uint64_t tenant_id_;
    int64_t min_micro_size_;
    int64_t max_micro_size_;
    int64_t micro_cnt_;
    int64_t interval_us_;
    int64_t arc_limit_;

    TestSSMicroCacheHugeDataCtx() { reset(); }
    bool is_valid() const { return (nullptr != micro_cache_) && (min_micro_size_ > 0) && (max_micro_size_ >= min_micro_size_)
                                   && (micro_cnt_ > 0) && (arc_limit_ > 0); }
    void reset()
    {
      micro_cache_ = nullptr;
      tenant_id_ = OB_INVALID_TENANT_ID;
      min_micro_size_ = 0;
      max_micro_size_ = 0;
      micro_cnt_ = 0;
      interval_us_ = 0;
      arc_limit_ = 0;
    }

    TO_STRING_KV(KP_(micro_cache), K_(tenant_id), K_(min_micro_size), K_(max_micro_size), K_(micro_cnt), K_(interval_us),
      K_(arc_limit));
  };

  class TestSSMicroCacheHugeDataThread : public Threads
  {
  public:
    TestSSMicroCacheHugeDataThread(ObTenantBase *tenant_base, TestSSMicroCacheHugeDataCtx &ctx)
        : tenant_base_(tenant_base), ctx_(ctx), fail_cnt_(0) {}
    void run(int64_t idx) final;
    int64_t get_fail_cnt() { return ATOMIC_LOAD(&fail_cnt_); }
  private:
    int build_io_info(ObIOInfo &io_info, const uint64_t tenant_id, const ObSSMicroBlockCacheKey &micro_key,
                      const int32_t size, char *read_buf);
    int parallel_add_micro_block(int64_t idx);

  private:
    ObTenantBase *tenant_base_;
    TestSSMicroCacheHugeDataCtx &ctx_;
    int64_t fail_cnt_;
  };
};

void TestSSMicroCacheHugeData::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheHugeData::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheHugeData::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 32))); // 4096MB
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
}

void TestSSMicroCacheHugeData::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSMicroCacheHugeData::TestSSMicroCacheHugeDataThread::run(int64_t idx)
{
  ObTenantEnv::set_tenant(tenant_base_);
  parallel_add_micro_block(idx);
}

int TestSSMicroCacheHugeData::TestSSMicroCacheHugeDataThread::build_io_info(
    ObIOInfo &io_info,
    const uint64_t tenant_id,
    const ObSSMicroBlockCacheKey &micro_key,
    const int32_t size,
    char *read_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!micro_key.is_valid() || size <= 0 || tenant_id == OB_INVALID_TENANT_ID) || OB_ISNULL(read_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_key), K(read_buf), K(size), K(tenant_id));
  } else {
    io_info.tenant_id_ = tenant_id;
    io_info.offset_ = micro_key.micro_id_.offset_;
    io_info.size_ = size;
    io_info.flag_.set_wait_event(1);
    io_info.timeout_us_ = 5 * 1000 * 1000;
    io_info.buf_ = read_buf;
    io_info.user_data_buf_ = read_buf;
  }
  return ret;
}

int TestSSMicroCacheHugeData::TestSSMicroCacheHugeDataThread::parallel_add_micro_block(int64_t idx)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  char *read_buf = nullptr;
  if (OB_UNLIKELY(!ctx_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K_(ctx));
  } else {
    char *buf = nullptr;
    const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                   ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.micro_cnt_; ++i) {
      const int32_t micro_size = ObRandom::rand(ctx_.min_micro_size_, ctx_.max_micro_size_);
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id((idx + 1) * ctx_.micro_cnt_ + i);
      char c = macro_id.hash() % 26 + 'a';
      if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(micro_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", KR(ret), K(micro_size));
      } else {
        MEMSET(buf, c, micro_size);
        const int32_t offset = payload_offset;
        ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        ctx_.micro_cache_->add_micro_block_cache(micro_key, buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
        ObSSMicroCacheStat &cache_stat = ctx_.micro_cache_->cache_stat_;
        if (cache_stat.micro_stat().total_micro_size_ > ctx_.arc_limit_) {
          ob_usleep(ctx_.interval_us_);
        } else {
          ob_usleep(50);
        }
      }
      allocator.clear();
    }
  }

  if (OB_FAIL(ret)) {
    ATOMIC_INC(&fail_cnt_);
  }
  return ret;
}

TEST_F(TestSSMicroCacheHugeData, test_add_huge_micro_data)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager & phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  ObSSExecuteMicroCheckpointTask &micro_ckpt_task = micro_cache->task_runner_.micro_ckpt_task_;
  const int32_t block_size = phy_blk_mgr.block_size_;
  const int64_t total_data_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt();
  const int64_t exp_total_data_size = total_data_blk_cnt * block_size * 3;

  const int64_t thread_num = 4;
  TestSSMicroCacheHugeData::TestSSMicroCacheHugeDataCtx ctx;
  ctx.micro_cache_ = micro_cache;
  ctx.tenant_id_ = MTL_ID();
  ctx.min_micro_size_ = 1024;
  ctx.max_micro_size_ = 12 * 1024;
  const int32_t avg_micro_size = (ctx.min_micro_size_ + ctx.max_micro_size_) / 2;
  ctx.micro_cnt_ = exp_total_data_size / avg_micro_size / thread_num;
  ctx.interval_us_ = 50;
  ctx.arc_limit_ = micro_meta_mgr.arc_info_.limit_;
  int64_t start_us = ObTimeUtility::current_time();
  LOG_INFO("TEST: start case", K(ctx));

  TestSSMicroCacheHugeData::TestSSMicroCacheHugeDataThread threads(ObTenantEnv::get_tenant(), ctx);
  threads.set_thread_count(thread_num);
  threads.start();
  threads.wait();
  ASSERT_EQ(0, threads.get_fail_cnt());

  const int64_t cost_ms = (ObTimeUtility::current_time() - start_us) / 1000;
  LOG_INFO("TEST: finish case, check result", K(cost_ms), K(cache_stat), K(arc_info));

  ob_usleep(10 * 1000 * 1000);
  start_us = ObTimeUtility::current_time();
  const int64_t CASE_TIMEOUT_US = 60 * 1000 * 1000; // in case reorgan_task is still running, we wait more time
  while (cache_stat.mem_blk_stat().mem_blk_bg_used_cnt_ == cache_stat.mem_blk_stat().mem_blk_bg_max_cnt_
         && (ObTimeUtility::current_time() - start_us <= CASE_TIMEOUT_US)) {
    ob_usleep(1000 * 1000);
  }

  ASSERT_GT(cache_stat.mem_blk_stat().mem_blk_fg_max_cnt_, cache_stat.mem_blk_stat().mem_blk_fg_used_cnt_);
  ASSERT_GT(cache_stat.mem_blk_stat().mem_blk_bg_max_cnt_, cache_stat.mem_blk_stat().mem_blk_bg_used_cnt_);
  int64_t max_result_val = arc_info.limit_ * 105 / 100;
  int64_t min_result_val = arc_info.limit_ * 95 / 100;
  ASSERT_LT(min_result_val, cache_stat.micro_stat().valid_micro_size_);
  ASSERT_GT(max_result_val, cache_stat.micro_stat().valid_micro_size_);
  const int64_t ori_valid_size = arc_info.get_valid_size();

  LOG_INFO("TEST: start update arc", K(cache_stat), K(arc_info));
  micro_ckpt_task.ckpt_op_.enable_update_arc_limit_ = false;
  ob_usleep(1000);
  const int64_t tmp_arc_limit = arc_info.limit_ / 4;
  arc_info.update_arc_limit(tmp_arc_limit);
  ASSERT_EQ(tmp_arc_limit, arc_info.limit_);

  ob_usleep(10 * 1000 * 1000);
  LOG_INFO("TEST: finish update arc", K(cache_stat), K(arc_info));
  ASSERT_LT(arc_info.get_valid_size(), ori_valid_size);

}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_huge_data.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_huge_data.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
