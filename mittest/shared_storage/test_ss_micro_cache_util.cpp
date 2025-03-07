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

#define private public
#define protected public
#include "test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_writer.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
class TestSSMicroCacheUtil : public ::testing::Test
{
public:
  TestSSMicroCacheUtil() {}
  virtual ~TestSSMicroCacheUtil() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

  void write_batch_macro_block_data(const int64_t micro_cnt, const int64_t macro_cnt);
};

void TestSSMicroCacheUtil::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCacheUtil::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCacheUtil::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 27)));
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
}

void TestSSMicroCacheUtil::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSMicroCacheUtil::write_batch_macro_block_data(const int64_t micro_cnt, const int64_t macro_cnt)
{
  ObArenaAllocator allocator;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);

  const int64_t payload_offset =
      ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_size = (DEFAULT_BLOCK_SIZE - payload_offset) / micro_cnt - micro_index_size;
  char *data_buf = reinterpret_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);

  MEMSET(data_buf, 'a', micro_size);
  for (int64_t i = 1; i <= macro_cnt; ++i) {
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i);
    for (int64_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ASSERT_EQ(OB_SUCCESS,
          micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
    }
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }
}


TEST_F(TestSSMicroCacheUtil, test_phy_block_common_header)
{
  ObArenaAllocator allocator;
  const int64_t buf_len = DEFAULT_BLOCK_SIZE;
  char *buf = reinterpret_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);

  ObSSPhyBlockCommonHeader hdr1;
  const int64_t common_hdr_size = ObSSPhyBlockCommonHeader::get_serialize_size();
  const int64_t payload_size = 1000;
  hdr1.set_payload_size(payload_size);
  hdr1.set_block_type(ObSSPhyBlockType::SS_CACHE_DATA_BLK);
  MEMSET(buf + common_hdr_size, 'c', hdr1.payload_size_);
  hdr1.calc_payload_checksum(buf + common_hdr_size, hdr1.payload_size_);
  ASSERT_EQ(true, hdr1.is_valid());

  ObSSPhyBlockCommonHeader hdr2;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, hdr1.serialize(buf, buf_len, pos));
  ASSERT_EQ(pos, common_hdr_size);
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::parse_phy_block_common_header(buf, buf_len, hdr2));
  ASSERT_EQ(hdr1.payload_size_, hdr2.payload_size_);
  ASSERT_EQ(hdr1.payload_checksum_, hdr2.payload_checksum_);
  ASSERT_EQ(hdr1.blk_type_, hdr2.blk_type_);
}

TEST_F(TestSSMicroCacheUtil, test_normal_phy_block_header)
{
  ObArenaAllocator allocator;
  const int64_t buf_len = DEFAULT_BLOCK_SIZE;
  char *buf = reinterpret_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);

  ObSSNormalPhyBlockHeader hdr1;
  hdr1.payload_size_ = 1000;
  hdr1.payload_offset_ = 1000;
  hdr1.micro_count_ = 11;
  hdr1.micro_index_offset_ = 77;
  hdr1.micro_index_size_ = 99;
  ASSERT_EQ(true, hdr1.is_valid());

  int64_t pos = 0;
  ObSSNormalPhyBlockHeader hdr2;
  ASSERT_EQ(OB_SUCCESS, hdr1.serialize(buf, buf_len, pos));
  ASSERT_EQ(pos, hdr1.get_serialize_size());
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::parse_normal_phy_block_header(buf, buf_len, hdr2));
  ASSERT_EQ(hdr1.payload_size_, hdr2.payload_size_);
  ASSERT_EQ(hdr1.payload_offset_, hdr2.payload_offset_);
  ASSERT_EQ(hdr1.micro_count_, hdr2.micro_count_);
  ASSERT_EQ(hdr1.micro_index_offset_, hdr2.micro_index_offset_);
  ASSERT_EQ(hdr1.micro_index_size_, hdr2.micro_index_size_);
  ASSERT_EQ(hdr1.payload_checksum_, hdr2.payload_checksum_);
}

TEST_F(TestSSMicroCacheUtil, test_parse_micro_block_indexs)
{
  ObArenaAllocator allocator;
  const int64_t buf_len = DEFAULT_BLOCK_SIZE;
  char *buf = reinterpret_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, buf);

  const int64_t cnt = 100;
  int64_t pos = 0;
  int64_t index_size = 0;
  ObSEArray<ObSSMicroBlockIndex, 64> micro_indexs;
  for (int64_t i = 0; i < cnt; ++i) {
    const int64_t offset = i + 100;
    const int64_t size = 10;
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, size);
    const ObSSMicroBlockIndex micro_index = ObSSMicroBlockIndex(micro_key, size);
    ASSERT_EQ(OB_SUCCESS, micro_indexs.push_back(micro_index));
    ASSERT_EQ(OB_SUCCESS, micro_index.serialize(buf, buf_len, pos));
    index_size += micro_index.get_serialize_size();
  }
  ASSERT_EQ(pos, index_size);

  ObSEArray<ObSSMicroBlockIndex, 64> tmp_micro_indexs;
  ASSERT_EQ(OB_SUCCESS,
      ObSSMicroCacheUtil::parse_micro_block_indexs(buf, index_size, tmp_micro_indexs));
  for (int64_t i = 0; i < cnt; ++i) {
    ASSERT_EQ(micro_indexs[i], tmp_micro_indexs[i]);
  }
}

TEST_F(TestSSMicroCacheUtil, test_parse_ss_super_block)
{
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache*)->phy_blk_mgr_;

  ObMemAttr attr(MTL_ID(), "test");
  const int64_t buf_len = DEFAULT_BLOCK_SIZE;
  char *dest_buf = reinterpret_cast<char *>(ob_malloc(buf_len, attr));
  char *src_buf = reinterpret_cast<char *>(ob_malloc_align(SS_MEM_BUF_ALIGNMENT, buf_len, attr));
  ASSERT_NE(nullptr, dest_buf);
  ASSERT_NE(nullptr, src_buf);

  const int64_t align_size = lib::align_up(phy_blk_mgr.super_block_.get_serialize_size(), SS_MEM_BUF_ALIGNMENT);
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, tnt_file_mgr->pread_cache_block(0, align_size, src_buf, read_size));
  ASSERT_EQ(read_size, align_size);

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::parse_ss_super_block(dest_buf, buf_len, src_buf, buf_len, pos));
  const char *file = "test_parse_ss_super_block";
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::dump_phy_block(src_buf, buf_len, file, true));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(file, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::unlink(file));

  ob_free(dest_buf);
  ob_free_align(src_buf);
}

TEST_F(TestSSMicroCacheUtil, test_parse_normal_phy_block)
{
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObMemAttr attr(MTL_ID(), "test");

  // 1. write micro block data
  const int64_t micro_cnt = 1024;
  const int64_t macro_cnt = 5;
  write_batch_macro_block_data(micro_cnt, macro_cnt);

  ObSSPhysicalBlockHandle phy_blk_handle;
  const int64_t phy_blk_idx = 3;
  phy_blk_mgr.get_block_handle(3, phy_blk_handle);
  ASSERT_EQ(ObSSPhyBlockType::SS_CACHE_DATA_BLK, phy_blk_handle()->get_block_type());

  // 2. dump normal_block
  const int64_t block_size = phy_blk_mgr.get_block_size();
  const int64_t src_buf_len = block_size;
  const int64_t dest_buf_len = block_size * 5;
  char *src_buf = reinterpret_cast<char *>(ob_malloc_align(SS_MEM_BUF_ALIGNMENT, src_buf_len, attr));
  char *dest_buf = reinterpret_cast<char *>(ob_malloc(dest_buf_len, attr));
  ASSERT_NE(nullptr, src_buf);
  ASSERT_NE(nullptr, dest_buf);

  const int64_t block_offset = phy_blk_mgr.get_block_size() * phy_blk_idx;
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, tnt_file_mgr->pread_cache_block(block_offset, block_size, src_buf, read_size));
  ASSERT_EQ(read_size, block_size);

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::parse_normal_phy_block(dest_buf, dest_buf_len, src_buf, src_buf_len, pos, false));
  const char *file = "test_parse_normal_phy_block";
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::dump_phy_block(src_buf, src_buf_len, file, false, false));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(file, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::unlink(file));

  ob_free(dest_buf);
  ob_free_align(src_buf);
}


TEST_F(TestSSMicroCacheUtil, test_parse_micro_ckpt_block)
{
  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager*);
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObMemAttr attr(MTL_ID(), "test");

  // 1. write micro block data and persist some phy_block
  const int64_t micro_cnt = 1024;
  const int64_t macro_cnt = 5;
  write_batch_macro_block_data(micro_cnt, macro_cnt);

  // 2. execute micro_meta_ckpt
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSExecuteMicroCheckpointTask &micro_ckpt_task = micro_cache->task_runner_.micro_ckpt_task_;
  micro_ckpt_task.ckpt_op_.micro_ckpt_ctx_.exe_round_ = ObSSExecuteMicroCheckpointOp::MICRO_META_CKPT_INTERVAL_ROUND - 2;
  usleep(5 * 1000 * 1000);
  ASSERT_EQ(1, cache_stat.task_stat().micro_ckpt_cnt_);
  ASSERT_LT(0, phy_blk_mgr.super_block_.micro_ckpt_entry_list_.count());

  // 3. dump micro_meta_ckpt block
  const int64_t block_size = phy_blk_mgr.get_block_size();
  const int64_t src_buf_len = block_size;
  const int64_t dest_buf_len = block_size * 10;
  char *src_buf = reinterpret_cast<char *>(ob_malloc_align(SS_MEM_BUF_ALIGNMENT, src_buf_len, attr));
  char *dest_buf = reinterpret_cast<char *>(ob_malloc(dest_buf_len, attr));
  ASSERT_NE(nullptr, src_buf);
  ASSERT_NE(nullptr, dest_buf);

  const int64_t phy_blk_idx = phy_blk_mgr.super_block_.micro_ckpt_entry_list_[0];
  const int64_t block_offset = phy_blk_mgr.get_block_size() * phy_blk_idx;
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, tnt_file_mgr->pread_cache_block(block_offset, block_size, src_buf, read_size));
  ASSERT_EQ(read_size, block_size);

  int64_t pos = 0;
  const bool is_micro_meta = true;
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::parse_checkpoint_block(dest_buf, dest_buf_len, src_buf, src_buf_len, pos, is_micro_meta));
  const char *file = "test_parse_ckpt_block";
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::dump_phy_block(src_buf, src_buf_len, file, false, is_micro_meta));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(file, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::unlink(file));

  ob_free(dest_buf);
  ob_free_align(src_buf);
}

}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_util.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_util.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}