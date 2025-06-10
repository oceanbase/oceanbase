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
#include "gtest/gtest.h"

#define private public
#define protected public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_reader.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_writer.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_range_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
class TestSSMicroCacheSmallFileSize : public ::testing::Test
{
public:
  TestSSMicroCacheSmallFileSize() {}
  virtual ~TestSSMicroCacheSmallFileSize() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void print_sparse_blk();
};

void TestSSMicroCacheSmallFileSize::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheSmallFileSize::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheSmallFileSize::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), 214748364)); // 204MB
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
}

void TestSSMicroCacheSmallFileSize::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSMicroCacheSmallFileSize::print_sparse_blk()
{
  int ret = OB_SUCCESS;
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObSSPhysicalBlockManager::SparsePhyBlockMap::BlurredIterator iter(phy_blk_mgr.sparse_blk_map_);
  iter.rewind();
  bool finish_iter = false;
  ObSSPhysicalBlock *phy_blk = nullptr;
  while (OB_SUCC(ret) && (!finish_iter)) {
    ObSSPhyBlockIdx blk_idx;
    bool is_exist = false;
    if (OB_FAIL(iter.next(blk_idx, is_exist))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        finish_iter = true;
      } else {
        LOG_WARN("fail to next phy_block", KR(ret));
      }
    } else if (OB_ISNULL(phy_blk = phy_blk_mgr.inner_get_phy_block_by_idx(blk_idx.blk_idx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block should not be null", KR(ret), K(blk_idx));
    } else {
      LOG_INFO("check sparse blk", K(blk_idx), KPC(phy_blk));
    }
  }
}

TEST_F(TestSSMicroCacheSmallFileSize, test_small_file_size)
{
  LOG_INFO("TEST_CASE: start test_small_file_size");
  int ret = OB_SUCCESS;
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tnt_file_mgr);
  tnt_file_mgr->persist_disk_space_task_.enable_adjust_size_ = false;
  ob_usleep(5 * 1000 * 1000);

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  const int64_t block_size = micro_cache->phy_blk_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSARCInfo &arc_info = micro_cache->micro_meta_mgr_.arc_info_;
  const int64_t min_micro_size = 8 * 1024; // 8KB
  const int64_t max_micro_size = 16 * 1024;

  const int64_t WRITE_BLK_CNT = 300;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(max_micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', max_micro_size);
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();

  int64_t prev_add_micro_cnt = 0;
  int64_t prev_reorgan_cnt = 0;
  for (int64_t round = 0; round < 5; ++round) {
    for (int64_t i = WRITE_BLK_CNT * round; i < WRITE_BLK_CNT * (round + 1); ++i) {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
      int32_t offset = payload_offset;
      do {
        const int32_t micro_size = ObRandom::rand(min_micro_size, max_micro_size);
        ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, macro_id.second_id(), ObSSMicroCacheAccessType::COMMON_IO_TYPE);
        offset += micro_size;
      } while (offset < block_size);
      ob_usleep((i / 50 + 1) * 5 * 1000);
    }
    const int64_t interval_s = ObRandom::rand(1, 10);
    ob_usleep(interval_s * 1000 * 1000L);
    const int64_t cur_add_micro_cnt = cache_stat.hit_stat().new_add_cnt_;
    const int64_t cur_reorgan_cnt = cache_stat.task_stat().reorgan_cnt_;
    LOG_INFO("check cache stat", K(round), K(prev_add_micro_cnt), K(prev_reorgan_cnt), K(cur_add_micro_cnt),
      K(cur_reorgan_cnt), K(cache_stat), K(arc_info));
    if (prev_add_micro_cnt >= cur_add_micro_cnt || prev_reorgan_cnt >= cur_reorgan_cnt) {
      ret = OB_ERR_UNEXPECTED;
      print_sparse_blk();
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    prev_add_micro_cnt = cur_add_micro_cnt;
    prev_reorgan_cnt = cur_reorgan_cnt;
  }

  const int64_t start_time_s = ObTimeUtility::current_time_s();
  bool exp_state = false;
  do {
    exp_state = (cache_stat.phy_blk_stat().sparse_blk_cnt_ < SS_MIN_REORGAN_BLK_CNT);
  } while (ObTimeUtility::current_time_s() - start_time_s < 180 && !exp_state);
  if (!exp_state) {
    print_sparse_blk();
    LOG_INFO("check cache stat 2", K(exp_state), K(cache_stat), K(arc_info));
  }
  ASSERT_EQ(true, exp_state);

  LOG_INFO("TEST_CASE: finish test_small_file_size");
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_small_file_size.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_small_file_size.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
