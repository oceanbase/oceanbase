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
#include "mittest/shared_storage/test_ss_common_util.h"
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

  int parallel_add_micro_block(const int64_t micro_cnt, const int64_t micro_size);
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

int TestSSMicroCacheParallelism::parallel_add_micro_block(const int64_t micro_cnt, const int64_t micro_size)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObArenaAllocator allocator;
  char *read_buf = nullptr;
  if (OB_UNLIKELY(micro_cnt <= 0 || micro_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(micro_cnt), K(micro_cnt));
  } else {
    char *buf = nullptr;
    const int64_t payload_offset = ObSSMemBlock::get_reserved_size();
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
    for (int64_t i = 0; OB_SUCC(ret) && i < micro_cnt; ++i) {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(1000 + i);
      char c = macro_id.hash() % 26 + 'a';
      MEMSET(buf, c, micro_size);
      const int32_t offset = payload_offset;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ret = micro_cache->add_micro_block_cache(micro_key, buf, micro_size,
                                               macro_id.second_id()/*effective_tablet_id*/,
                                               ObSSMicroCacheAccessType::COMMON_IO_TYPE);
      ++tmp_cnt;
      if (tmp_cnt >= avg_micro_cnt) {
        ob_usleep(100 * 1000);
        tmp_cnt = 0;
      }
    }
    allocator.clear();
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
  const int64_t total_data_blk_cnt = phy_blk_mgr.blk_cnt_info_.micro_data_blk_max_cnt();
  const int64_t exp_total_data_size = total_data_blk_cnt / 2 * block_size;

  const int64_t thread_num = 4;
  const int64_t micro_size = 8 * 1024;
  const int64_t micro_cnt = exp_total_data_size / micro_size / thread_num;

  ObTenantBase *tenant_base = MTL_CTX();
  auto test_func = [&](const int64_t idx) {
    ObTenantEnv::set_tenant(tenant_base);
    ASSERT_EQ(OB_SUCCESS, parallel_add_micro_block(micro_cnt, micro_size));
  };
  std::vector<std::thread> ths;
  for (int64_t i = 0; i < thread_num; ++i) {
    std::thread th(test_func, i);
    ths.push_back(std::move(th));
  }
  for (int64_t i = 0; i < thread_num; ++i) {
    ths[i].join();
  }
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
