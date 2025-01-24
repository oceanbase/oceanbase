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
#include "test_ss_common_util.h"
#include "storage/shared_storage/ob_ss_micro_cache_io_helper.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_writer.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_reader.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

class TestSSMicroCacheCheckpoint : public ::testing::Test
{
public:
  TestSSMicroCacheCheckpoint() {}
  virtual ~TestSSMicroCacheCheckpoint() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestSSMicroCacheCheckpoint::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheCheckpoint::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheCheckpoint::SetUp()
{}

void TestSSMicroCacheCheckpoint::TearDown()
{}

TEST_F(TestSSMicroCacheCheckpoint, test_compress_micro_ckpt)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  const uint64_t tenant_id = MTL_ID();
  const int64_t block_size = micro_cache->phy_block_size_;
  ObSSMemDataManager &mem_data_mgr = micro_cache->mem_data_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;

  const int32_t micro_size = 8 * 1024;
  const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                 ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_cnt = (block_size - payload_offset) / (micro_size + micro_index_size);
  const int64_t total_data_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt();
  const int64_t macro_cnt = MIN(150, total_data_blk_cnt);

  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);
  const int64_t item_buf_size = sizeof(ObSSMicroBlockMeta) + 128;
  char *item_buf = static_cast<char *>(allocator.alloc(item_buf_size));
  ASSERT_NE(nullptr, item_buf);
  char *io_buf = static_cast<char *>(allocator.alloc(block_size));
  ASSERT_NE(nullptr, io_buf);

  // 1. gen micro_meta
  for (int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(2000 + i);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE);
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }

  // 2. gen micro_meta ckpt
  ObSSLinkedPhyBlockItemWriter item_writer;
  ObSSLinkedPhyBlockItemWriter item_comp_writer;
  ASSERT_EQ(OB_SUCCESS, item_writer.init(tenant_id, phy_blk_mgr, ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK));
  ASSERT_EQ(OB_SUCCESS, item_comp_writer.init(tenant_id, phy_blk_mgr, ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK, ObCompressorType::SNAPPY_COMPRESSOR));
  int64_t total_micro_item_cnt = 0;
  for (int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(2000 + i);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMicroBlockMetaHandle micro_handle;
      ret = micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_handle, false);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to get micro_meta", KR(ret), K(i), K(j), K(micro_key), K(cache_stat));
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(true, micro_handle.is_valid());
      if (micro_handle.get_ptr()->is_persisted()) {
        int64_t pos = 0;
        ASSERT_EQ(OB_SUCCESS, micro_handle()->serialize(item_buf, item_buf_size, pos));
        ASSERT_EQ(pos, micro_handle()->get_serialize_size());
        ASSERT_EQ(OB_SUCCESS, item_writer.write_item(item_buf, pos));
        ASSERT_EQ(OB_SUCCESS, item_comp_writer.write_item(item_buf, pos));
        ++total_micro_item_cnt;
      }
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
  ASSERT_EQ(OB_SUCCESS, item_writer.close());
  ASSERT_EQ(OB_SUCCESS, item_comp_writer.close());
  ObArray<int64_t> micro_ckpt_blk_list;
  ASSERT_EQ(OB_SUCCESS, item_writer.get_block_id_list(micro_ckpt_blk_list));
  ASSERT_LT(1, micro_ckpt_blk_list.count());
  int64_t entry_idx = 0;
  item_writer.get_entry_block(entry_idx);
  ObArray<int64_t> micro_comp_ckpt_blk_list;
  ASSERT_EQ(OB_SUCCESS, item_comp_writer.get_block_id_list(micro_comp_ckpt_blk_list));
  ASSERT_LE(micro_comp_ckpt_blk_list.count(), micro_ckpt_blk_list.count());

  // 3. get micro_meta ckpt data(one phy_block)
  ObSSPhysicalBlockHandle phy_blk_handle;
  const int64_t entry_blk_idx = micro_ckpt_blk_list.at(1);
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_block_handle(entry_blk_idx, phy_blk_handle));
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheIOHelper::read_block(entry_blk_idx * block_size, block_size,
            io_buf, phy_blk_handle));
  char *all_ckpt_item = nullptr;
  int32_t all_ckpt_item_len = 0;
  {
    int64_t pos = 0;
    ObSSPhyBlockCommonHeader common_header;
    ASSERT_EQ(OB_SUCCESS, common_header.deserialize(io_buf, block_size, pos));
    ObSSLinkedPhyBlockHeader linked_header;
    ASSERT_EQ(OB_SUCCESS, linked_header.deserialize(io_buf, block_size, pos));
    all_ckpt_item = io_buf + pos;
    all_ckpt_item_len = block_size - pos;
  }

  // 4. compress micro_meta ckpt data
  const uint8_t start_type = static_cast<uint8_t>(ObCompressorType::LZ4_COMPRESSOR);
  const uint8_t end_type = static_cast<uint8_t>(ObCompressorType::ZSTD_COMPRESSOR);
  for (uint8_t i = start_type; i <= end_type; ++i) {
    ObCompressorType comp_type = static_cast<ObCompressorType>(i);
    ObCompressor *compressor = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(comp_type, compressor));
    ASSERT_NE(nullptr, compressor);
    int64_t max_overflow_size = 0;
    ASSERT_EQ(OB_SUCCESS, compressor->get_max_overflow_size(all_ckpt_item_len, max_overflow_size));
    const int64_t out_io_buf_size = all_ckpt_item_len + max_overflow_size;
    char *out_io_buf = static_cast<char *>(allocator.alloc(out_io_buf_size));
    ASSERT_NE(nullptr, out_io_buf);
    int64_t compressed_len = 0;
    const int64_t start_us = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, compressor->compress(all_ckpt_item, all_ckpt_item_len, out_io_buf, out_io_buf_size, compressed_len));
    const int64_t cost_us = ObTimeUtility::current_time() - start_us;
    LOG_INFO("finish current round micro_meta ckpt compress", K(all_compressor_name[i]), K(all_ckpt_item_len), K(compressed_len), K(cost_us));
  }

  // 5. execute reading micro_meta ckpt
  ObSSLinkedPhyBlockItemReader item_reader;
  ASSERT_EQ(OB_SUCCESS, item_reader.init(micro_ckpt_blk_list.at(0), tenant_id, phy_blk_mgr));
  char *des_item_buf = nullptr;
  int64_t des_item_buf_len = 0;
  while (OB_SUCC(ret)) {
    ret = item_reader.get_next_item(des_item_buf, des_item_buf_len);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      break;
    } else {
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ASSERT_NE(nullptr, des_item_buf);
    ASSERT_NE(0, des_item_buf_len);
  }

  // 6. use checkpoint_op logic to gen micro_ckpt, mock micro_ckpt_block is not enough, we will allow this situation
  SSPhyBlockCntInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  ObSSExecuteMicroCheckpointTask &micro_ckpt_task = micro_cache->task_runner_.micro_ckpt_task_;
  const int64_t origin_micro_ckpt_blk_used_cnt = blk_cnt_info.meta_blk_.used_cnt_;
  // mock only one micro_ckpt_blk can be allocated.
  blk_cnt_info.meta_blk_.min_cnt_ = origin_micro_ckpt_blk_used_cnt + 1;
  blk_cnt_info.meta_blk_.max_cnt_ = origin_micro_ckpt_blk_used_cnt + 1;
  blk_cnt_info.meta_blk_.hold_cnt_ = origin_micro_ckpt_blk_used_cnt + 1;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.meta_blk_.hold_cnt_ + blk_cnt_info.data_blk_.hold_cnt_;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.get_ss_super_block(micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.prev_super_block_));
  ASSERT_EQ(OB_SUCCESS, micro_ckpt_task.ckpt_op_.gen_micro_meta_checkpoint());
  // actually we need more than 1 phy_block to store micro_ckpt, but here we just used 1 phy_block.
  ASSERT_EQ(blk_cnt_info.meta_blk_.used_cnt_, origin_micro_ckpt_blk_used_cnt + 1);

  allocator.clear();
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_checkpoint.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_checkpoint.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}