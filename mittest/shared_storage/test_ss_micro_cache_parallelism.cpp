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

class TestSSMicroCacheParallelism : public ::testing::Test
{
public:
  TestSSMicroCacheParallelism() {}
  virtual ~TestSSMicroCacheParallelism() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

public:
  struct TestSSMicroCacheParallellismCtx
  {
  public:
    ObSSMicroCache *micro_cache_;
    uint64_t tenant_id_;
    int64_t min_micro_size_;
    int64_t max_micro_size_;
    int64_t micro_cnt_;
    int64_t interval_us_;
    int64_t arc_limit_;

    TestSSMicroCacheParallellismCtx() { reset(); }
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

  class TestSSMicroCacheParallellismThread : public Threads
  {
  public:
    TestSSMicroCacheParallellismThread(ObTenantBase *tenant_base, TestSSMicroCacheParallellismCtx &ctx)
        : tenant_base_(tenant_base), ctx_(ctx), fail_cnt_(0) {}
    void run(int64_t idx) final;
    int64_t get_fail_cnt() { return ATOMIC_LOAD(&fail_cnt_); }
  private:
    int build_io_info(ObIOInfo &io_info, const uint64_t tenant_id, const ObSSMicroBlockCacheKey &micro_key,
                      const int32_t size, char *read_buf);
    int parallel_add_micro_block(int64_t idx);

  private:
    ObTenantBase *tenant_base_;
    TestSSMicroCacheParallellismCtx &ctx_;
    int64_t fail_cnt_;
  };
};

void TestSSMicroCacheParallelism::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheParallelism::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheParallelism::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 32))); // 4096MB
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
}

void TestSSMicroCacheParallelism::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSMicroCacheParallelism::TestSSMicroCacheParallellismThread::run(int64_t idx)
{
  ObTenantEnv::set_tenant(tenant_base_);
  parallel_add_micro_block(idx);
}

int TestSSMicroCacheParallelism::TestSSMicroCacheParallellismThread::build_io_info(
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

int TestSSMicroCacheParallelism::TestSSMicroCacheParallellismThread::parallel_add_micro_block(int64_t idx)
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
    const int32_t micro_size = (ctx_.min_micro_size_ + ctx_.max_micro_size_) / 2;
    const int32_t block_size = 2 * 1024 * 1024;
    const int32_t avg_micro_cnt = block_size / micro_size;
    if (OB_UNLIKELY(avg_micro_cnt <= 1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(avg_micro_cnt), K(micro_size));
    } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(micro_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(micro_size));
    }
    int32_t tmp_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_.micro_cnt_; ++i) {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(1000 + i);
      char c = macro_id.hash() % 26 + 'a';
      MEMSET(buf, c, micro_size);
      const int32_t offset = payload_offset;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ret = ctx_.micro_cache_->add_micro_block_cache(micro_key, buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
      ++tmp_cnt;
      if (tmp_cnt >= avg_micro_cnt) {
        ob_usleep(100 * 1000);
        tmp_cnt = 0;
      }
    }
    allocator.clear();
  }

  if (OB_FAIL(ret)) {
    ATOMIC_INC(&fail_cnt_);
  }
  return ret;
}

TEST_F(TestSSMicroCacheParallelism, test_parallel_add_same_data)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager & phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  const int32_t block_size = phy_blk_mgr.block_size_;
  const int64_t total_data_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt();
  const int64_t exp_total_data_size = total_data_blk_cnt / 2 * block_size;

  const int64_t thread_num = 4;
  TestSSMicroCacheParallelism::TestSSMicroCacheParallellismCtx ctx;
  ctx.micro_cache_ = micro_cache;
  ctx.tenant_id_ = MTL_ID();
  ctx.min_micro_size_ = 8 * 1024;
  ctx.max_micro_size_ = 8 * 1024;
  const int32_t avg_micro_size = (ctx.min_micro_size_ + ctx.max_micro_size_) / 2;
  ctx.micro_cnt_ = exp_total_data_size / avg_micro_size / 4;
  ctx.interval_us_ = 50;
  ctx.arc_limit_ = micro_meta_mgr.arc_info_.limit_;
  int64_t start_us = ObTimeUtility::current_time();
  LOG_INFO("TEST: start case", K(ctx));

  TestSSMicroCacheParallelism::TestSSMicroCacheParallellismThread threads(ObTenantEnv::get_tenant(), ctx);
  threads.set_thread_count(thread_num);
  threads.start();
  threads.wait();
  ASSERT_EQ(0, threads.get_fail_cnt());
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_parallelism.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_parallelism.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
