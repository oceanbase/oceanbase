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
#include "lib/ob_errno.h"
#include "test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
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
{}

void TestSSMicroCacheUtil::TearDown()
{}

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
  ObArenaAllocator allocator;
  const int64_t buf_len = DEFAULT_BLOCK_SIZE;
  char *dest_buf = reinterpret_cast<char *>(allocator.alloc(buf_len));
  char *src_buf = reinterpret_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, dest_buf);
  ASSERT_NE(nullptr, src_buf);

  const int64_t cache_file_size = (1L << 32);
  const int64_t common_hdr_size = ObSSPhyBlockCommonHeader::get_serialize_size();
  ObSSMicroCacheSuperBlock super_block(cache_file_size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, super_block.serialize(src_buf + common_hdr_size, buf_len, pos));
  ASSERT_EQ(pos, super_block.get_serialize_size());

  ObSSPhyBlockCommonHeader common_hdr;
  const int64_t payload_size = super_block.get_serialize_size();
  common_hdr.set_payload_size(payload_size);
  common_hdr.set_block_type(ObSSPhyBlockType::SS_SUPER_BLK);
  common_hdr.calc_payload_checksum(src_buf + common_hdr_size, payload_size);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, common_hdr.serialize(src_buf, common_hdr_size, pos));
  ASSERT_EQ(pos, common_hdr.get_serialize_size());

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::parse_ss_super_block(dest_buf, buf_len, src_buf, buf_len, pos));
  const char *file = "test_parse_ss_super_block";
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::dump_phy_block(src_buf, buf_len, file, true));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(file, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::unlink(file));
}

TEST_F(TestSSMicroCacheUtil, test_parse_normal_phy_block)
{
  ObArenaAllocator allocator;
  const int64_t buf_len = DEFAULT_BLOCK_SIZE;
  char *dest_buf = reinterpret_cast<char *>(allocator.alloc(buf_len));
  char *src_buf = reinterpret_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, dest_buf);
  ASSERT_NE(nullptr, src_buf);

  const int64_t cnt = 100;
  const int64_t micro_block_size = 100;
  int64_t index_size = 0;
  ObSEArray<ObSSMicroBlockIndex, 64> micro_indexs;
  for (int64_t i = 0; i < cnt; ++i) {
    const int64_t offset = i + 100;
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
    const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_block_size);
    const ObSSMicroBlockIndex micro_index = ObSSMicroBlockIndex(micro_key, micro_block_size);
    ASSERT_EQ(OB_SUCCESS, micro_indexs.push_back(micro_index));
    index_size += micro_index.get_serialize_size();
  }

  const int64_t common_hdr_size = ObSSPhyBlockCommonHeader::get_serialize_size();
  const int64_t blk_hdr_size = ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int64_t reserved_size = common_hdr_size + blk_hdr_size;
  const int64_t data_size = cnt * micro_block_size;

  ObSSNormalPhyBlockHeader blk_hdr;
  blk_hdr.micro_count_ = cnt;
  blk_hdr.micro_index_offset_ = reserved_size + data_size;
  blk_hdr.micro_index_size_ = index_size;
  blk_hdr.payload_offset_ = reserved_size;
  blk_hdr.payload_size_ = data_size + index_size;
  MEMSET(src_buf + blk_hdr.payload_offset_, 'c', data_size);
  int64_t pos = blk_hdr.micro_index_offset_;
  for (int64_t i = 0; i < cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, micro_indexs[i].serialize(src_buf, buf_len, pos));
  }
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, blk_hdr.serialize(src_buf + common_hdr_size, common_hdr_size, pos));
  ASSERT_EQ(pos, blk_hdr.get_serialize_size());

  ObSSPhyBlockCommonHeader common_hdr;
  const int64_t payload_size = blk_hdr.payload_size_ + blk_hdr_size;
  common_hdr.set_payload_size(payload_size);
  common_hdr.set_block_type(ObSSPhyBlockType::SS_CACHE_DATA_BLK);
  common_hdr.calc_payload_checksum(src_buf + common_hdr_size, payload_size);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, common_hdr.serialize(src_buf, buf_len, pos));
  ASSERT_EQ(pos, common_hdr.get_serialize_size());

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::parse_normal_phy_block(dest_buf, buf_len, src_buf, buf_len, pos, false));
  const char *file = "test_parse_normal_phy_block";
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::dump_phy_block(src_buf, buf_len, file, false, false));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(file, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::unlink(file));
}


TEST_F(TestSSMicroCacheUtil, test_parse_ckpt_block)
{
  ObArenaAllocator allocator;
  const int64_t buf_len = DEFAULT_BLOCK_SIZE;
  char *dest_buf = reinterpret_cast<char *>(allocator.alloc(buf_len));
  char *src_buf = reinterpret_cast<char *>(allocator.alloc(buf_len));
  ASSERT_NE(nullptr, dest_buf);
  ASSERT_NE(nullptr, src_buf);

  const int64_t common_hdr_size = ObSSPhyBlockCommonHeader::get_serialize_size();
  const int64_t linked_hdr_size = ObSSLinkedPhyBlockHeader::get_fixed_serialize_size();
  // item_writer init and gen ckpt.
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  ObSSLinkedPhyBlockItemWriter item_writer;
  ASSERT_EQ(OB_SUCCESS, item_writer.init(OB_SERVER_TENANT_ID, phy_blk_mgr, ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK));
  const int64_t cnt = 100;
  for (int64_t i = 0; i < cnt; ++i) {
    const int64_t item_buf_size = sizeof(ObSSPhyBlockPersistInfo);
    char item_buf[item_buf_size];
    int64_t item_pos = 0;
    ObSSPhyBlockPersistInfo persist_info(i + 100, 1);
    ASSERT_EQ(OB_SUCCESS, persist_info.serialize(item_buf, item_buf_size, item_pos));
    ASSERT_EQ(OB_SUCCESS, item_writer.write_item(item_buf, item_pos));
  }
  if (item_writer.io_seg_buf_pos_ > 0) {
    ASSERT_EQ(OB_SUCCESS, item_writer.write_segment_());
  }
  MEMCPY(src_buf, item_writer.io_buf_, item_writer.io_buf_pos_);
  const int64_t io_buf_pos = item_writer.io_buf_pos_;
  const int64_t payload_offset = common_hdr_size + linked_hdr_size;

  ObSSLinkedPhyBlockHeader linked_hdr;
  linked_hdr.set_payload_size(static_cast<int32_t>(io_buf_pos - payload_offset));
  linked_hdr.item_count_ = cnt;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, linked_hdr.serialize(src_buf + common_hdr_size, linked_hdr_size, pos));
  ASSERT_EQ(pos, linked_hdr.get_serialize_size());

  ObSSPhyBlockCommonHeader common_hdr;
  common_hdr.set_payload_size(io_buf_pos - common_hdr_size);
  common_hdr.set_block_type(ObSSPhyBlockType::SS_PHY_BLOCK_CKPT_BLK);
  common_hdr.calc_payload_checksum(src_buf + common_hdr_size, common_hdr.payload_size_);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, common_hdr.serialize(src_buf, common_hdr_size, pos));
  ASSERT_EQ(pos, common_hdr.get_serialize_size());

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::parse_checkpoint_block(dest_buf, buf_len, src_buf, buf_len, pos, false));
  const char *file = "test_parse_ckpt_block";
  ASSERT_EQ(OB_SUCCESS, ObSSMicroCacheUtil::dump_phy_block(src_buf, buf_len, file, false, false));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(file, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::unlink(file));
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