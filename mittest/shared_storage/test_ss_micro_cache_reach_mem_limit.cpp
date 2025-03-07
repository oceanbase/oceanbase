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

class TestSSMicroCacheReachMemLimit : public ::testing::Test
{
public:
  TestSSMicroCacheReachMemLimit() {}
  virtual ~TestSSMicroCacheReachMemLimit() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
public:
  struct TestSSMicroCacheReachMemLimitCtx
  {
  public:
    TestSSMicroCacheReachMemLimitCtx()
        : macro_blk_cnt_(0), micro_blk_cnt_(0), thread_num_(0) {}
    ~TestSSMicroCacheReachMemLimitCtx() { reset(); }
    void reset()
    {
      macro_blk_cnt_ = 0;
      micro_blk_cnt_ = 0;
      thread_num_ = 0;
    }
  public:
    int64_t macro_blk_cnt_;
    int64_t micro_blk_cnt_;
    int64_t thread_num_;
  };

public:
  class TestSSMicroCacheReachMemLimitThread : public Threads
  {
  public:
    enum class TestParallelType
    {
      ADD_MICRO_BLOCK_REACH_MEM_LIMIT,
    };
  public:
    TestSSMicroCacheReachMemLimitThread(
        ObTenantBase *tenant_base, TestSSMicroCacheReachMemLimitCtx &ctx, TestParallelType type)
        : tenant_base_(tenant_base), ctx_(ctx), type_(type), fail_cnt_(0) {}
    void run(int64_t idx) final;
    int64_t get_fail_cnt() { return ATOMIC_LOAD(&fail_cnt_); }
  private:
    int parallel_add_micro_block_reach_mem_limit(int64_t idx);

  private:
    ObTenantBase *tenant_base_;
    TestSSMicroCacheReachMemLimitCtx &ctx_;
    TestParallelType type_;
    int64_t fail_cnt_;
  };
};

void TestSSMicroCacheReachMemLimit::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheReachMemLimit::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheReachMemLimit::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 30))); // 1G
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
}

void TestSSMicroCacheReachMemLimit::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSMicroCacheReachMemLimit::TestSSMicroCacheReachMemLimitThread::run(int64_t idx)
{
  ObTenantEnv::set_tenant(tenant_base_);
  if (type_ == TestParallelType::ADD_MICRO_BLOCK_REACH_MEM_LIMIT) {
    parallel_add_micro_block_reach_mem_limit(idx);
  }
}

int TestSSMicroCacheReachMemLimit::TestSSMicroCacheReachMemLimitThread::parallel_add_micro_block_reach_mem_limit(int64_t idx)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  if (OB_UNLIKELY(ctx_.macro_blk_cnt_ <= 0 || ctx_.micro_blk_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ctx", KR(ret), K_(ctx_.macro_blk_cnt), K_(ctx_.micro_blk_cnt));
  } else {
    ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
    ObSSARCInfo &arc_info = micro_cache->micro_meta_mgr_.arc_info_;
    const int64_t payload_offset =
        ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
    const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
    const int32_t micro_cnt = ctx_.micro_blk_cnt_;
    const int32_t micro_size = (DEFAULT_BLOCK_SIZE - payload_offset) / micro_cnt - micro_index_size;
    char *data_buf = nullptr;
    if (OB_ISNULL(data_buf = static_cast<char *>(allocator.alloc(micro_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(micro_size));
    } else {
      MEMSET(data_buf, 'a', micro_size);
      const int64_t timeout_us = 10 * 1000 * 1000;
      for (int64_t i = 1; OB_SUCC(ret) && i <= ctx_.macro_blk_cnt_; ++i) {
        const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(ctx_.macro_blk_cnt_ * idx + i);
        for (int64_t j = 0; OB_SUCC(ret) && j < micro_cnt; ++j) {
          const int32_t offset = payload_offset + j * micro_size;
          const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
          if (OB_FAIL(micro_cache->add_micro_block_cache(
                  micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
            // wait until arc_task delete some micro_mete.
            const int64_t start_us = ObTimeUtility::current_time();
            const int64_t exceed_cnt = arc_info.calc_exceed_micro_cnt_by_mem(arc_info.mem_limit_);
            const int64_t begin = ObTimeUtility::current_time();
            LOG_INFO("start time: wait arc_task to del micro_meta", K(idx), K(i), K(j), K(exceed_cnt));
            while (OB_SS_CACHE_REACH_MEM_LIMIT == ret) {
              ob_usleep(1000);
              ret = micro_cache->add_micro_block_cache(
                  micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
              if (ObTimeUtility::current_time() - start_us > timeout_us) {
                LOG_WARN("wait too long to add new micro block", KR(ret), K(idx), K(micro_key));
                break;
              }
            }
            const int64_t free_cnt = exceed_cnt - arc_info.calc_exceed_micro_cnt_by_mem(arc_info.mem_limit_);
            const int64_t cost_ms = (ObTimeUtility::current_time() - begin) / 1000;
            if (OB_FAIL(ret)) {
              LOG_WARN("fail to add micro_block, unexpected behavior", KR(ret), K(idx), K(i), K(j), K(micro_key));
            } else {
              LOG_INFO("finish time: wait arc_task to del micro_meta", K(idx), K(i), K(j), K(free_cnt), K(cost_ms));
            }
          }
        }
        // avoid exhausting mem_block
        if (FAILEDx(TestSSCommonUtil::wait_for_persist_task())) {
          LOG_WARN("fail to wait for persist_task", KR(ret), K(idx));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    ATOMIC_INC(&fail_cnt_);
  }
  return ret;
}

/*
  When total micro_meta reaches the memory usage limit, adding micro_block will return OB_SS_CACHE_REACH_MEM_LIMIT
*/
TEST_F(TestSSMicroCacheReachMemLimit, test_alloc_micro_meta_reach_mem_limit)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_alloc_micro_meta_reach_mem_limit");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->task_runner_.release_cache_task_.is_inited_ = false;
  ob_usleep(1000 * 1000);
  // reduce tenant memory to 32MB
  const int64_t origin_tenant_mem_size = MTL_MEM_SIZE();
  const int64_t new_tenant_mem_size = (1L << 25);
  share::ObTenantEnv::get_tenant()->set_unit_memory_size(new_tenant_mem_size);
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    tenant_config->_ss_micro_cache_memory_percentage = 1;
  }
  micro_cache->micro_meta_mgr_.update_cache_mem_limit_by_config();

  // add micro block until reach cache mem limit
  const int64_t cache_mem_limit = micro_cache->micro_meta_mgr_.get_cache_mem_limit();
  const int64_t max_micro_meta_cnt = cache_mem_limit / (SS_MICRO_META_POOL_ITEM_SIZE + SS_MICRO_META_MAP_ITEM_SIZE);
  const int64_t payload_offset =
      ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = 1024;
  const int32_t micro_size = (DEFAULT_BLOCK_SIZE - payload_offset) / micro_cnt - micro_index_size;
  const int64_t macro_block_cnt = max_micro_meta_cnt / micro_cnt + 1;

  char data_buf[micro_size];
  MEMSET(data_buf, 'a', micro_size);
  for (int64_t i = 1; i <= macro_block_cnt; ++i) {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_LE(micro_cache->cache_stat_.micro_stat().total_micro_cnt_, max_micro_meta_cnt);

  const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(8888888);
  const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, 1, micro_size);
  ASSERT_EQ(OB_SS_CACHE_REACH_MEM_LIMIT,
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE));

  // increase tenant micro_meta memory usage and we can continue to add micro meta
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    tenant_config->_ss_micro_cache_memory_percentage = 2;
  }
  micro_cache->micro_meta_mgr_.update_cache_mem_limit_by_config();
  ASSERT_EQ(OB_SUCCESS,
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE));

  // clear settings
  share::ObTenantEnv::get_tenant()->set_unit_memory_size(origin_tenant_mem_size);
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    tenant_config->_ss_micro_cache_memory_percentage = 20;
  }
}

/*
  This case covers two scenarios:
  1. When cache_mem_size reaches limit, arc_task will delete some micro_meta so that new micro_block can continue to be
     written into micro_cache.
  2. When adding micro blocks concurrently, thread A and B both meet cache_mem_limit. After they finish writing micro data into
     mem_block, A may alloc micro_meta successfully, but B fails. In this scenario, the micro_cnt of mem_block needs to rollback
     so that mem_block can be completed and persisted by persist_task. By calling wait_for_persist_task() we can check that
     all sealed_mem_blocks are completed and persisted.
*/
TEST_F(TestSSMicroCacheReachMemLimit, test_reach_cache_mem_limit)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_reach_cache_mem_limit");
  ObArenaAllocator allocator;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_EQ(OB_SUCCESS, micro_cache->resize_micro_cache_file_size(10 * (1L << 30)));
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    tenant_config->_ss_micro_cache_memory_percentage = 1;
  }
  micro_meta_mgr.update_cache_mem_limit_by_config();
  const int64_t cache_mem_limit = micro_meta_mgr.get_cache_mem_limit();
  const int64_t max_micro_meta_cnt = cache_mem_limit / (SS_MICRO_META_POOL_ITEM_SIZE + SS_MICRO_META_MAP_ITEM_SIZE);

  TestSSMicroCacheReachMemLimitCtx ctx;
  ctx.micro_blk_cnt_ = 1024;
  ctx.thread_num_ = 4;
  ctx.macro_blk_cnt_ = max_micro_meta_cnt / ctx.micro_blk_cnt_ / ctx.thread_num_;
  TestSSMicroCacheReachMemLimit::TestSSMicroCacheReachMemLimitThread threads(
      ObTenantEnv::get_tenant(), ctx, TestSSMicroCacheReachMemLimitThread::TestParallelType::ADD_MICRO_BLOCK_REACH_MEM_LIMIT);
  threads.set_thread_count(ctx.thread_num_);
  threads.start();
  threads.wait();
  ASSERT_EQ(0, threads.get_fail_cnt());

  // clear settings
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    tenant_config->_ss_micro_cache_memory_percentage = 20;
  }
  micro_meta_mgr.update_cache_mem_limit_by_config();
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache_reach_mem_limit.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_reach_mem_limit.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
