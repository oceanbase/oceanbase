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

#define USING_LOG_PREFIX STORAGE
#include "gtest/gtest.h"
#include <thread>
#define private public
#define protected public

#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_struct.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

class TestSSMicroCacheCommonMeta : public ::testing::Test
{
public:
  TestSSMicroCacheCommonMeta();
  virtual ~TestSSMicroCacheCommonMeta();
  virtual void SetUp();
  virtual void TearDown();

private:
  static const int32_t BLOCK_SIZE = 2 * 1024 * 1024;
};

TestSSMicroCacheCommonMeta::TestSSMicroCacheCommonMeta()
{}

TestSSMicroCacheCommonMeta::~TestSSMicroCacheCommonMeta()
{}

void TestSSMicroCacheCommonMeta::SetUp()
{}

void TestSSMicroCacheCommonMeta::TearDown()
{}

struct TestSSPhysicalBlockHeader
{
public:
  int32_t magic_;
  int32_t payload_checksum_;
  int32_t payload_offset_;
  int32_t payload_size_;
  int32_t micro_count_;
  int32_t micro_index_offset_;
  int32_t micro_index_size_;
  int32_t a_;

  TestSSPhysicalBlockHeader()
    : magic_(0x000087AA), payload_checksum_(0), payload_offset_(-1), payload_size_(0),
      micro_count_(0), micro_index_offset_(-1), micro_index_size_(0), a_(-2)
  {}

  static const int64_t TEST_SS_PHYSICAL_BLOCK_HEADER_VERSION = 1;
  OB_UNIS_VERSION(TEST_SS_PHYSICAL_BLOCK_HEADER_VERSION);
};

OB_SERIALIZE_MEMBER(TestSSPhysicalBlockHeader, magic_, payload_checksum_, payload_offset_, payload_size_,
  micro_count_, micro_index_offset_, micro_index_size_, a_);

struct TestSSLinkedPhyBlockHeader
{
public:
  int32_t magic_;
  int32_t item_count_;
  int32_t payload_size_;
  uint32_t payload_checksum_;
  int64_t previous_phy_block_id_;
  int64_t a_;
  int64_t b_;

  TestSSLinkedPhyBlockHeader()
    : magic_(12345), item_count_(0), payload_size_(0), payload_checksum_(0),
      previous_phy_block_id_(-1), a_(-1), b_(-1)
  {}

  static const int64_t TEST_SS_LINKED_PHY_BLOCK_HEADER_VERSION = 1;
  OB_UNIS_VERSION(TEST_SS_LINKED_PHY_BLOCK_HEADER_VERSION);
};

OB_SERIALIZE_MEMBER(TestSSLinkedPhyBlockHeader, magic_, item_count_, payload_size_,
  payload_checksum_, previous_phy_block_id_, a_, b_);

TEST_F(TestSSMicroCacheCommonMeta, physical_block)
{
  ObSSPhysicalBlock phy_block;
  ASSERT_EQ(1, phy_block.get_ref_count());
  ASSERT_EQ(1, phy_block.reuse_version_);
  ASSERT_EQ(1, phy_block.gc_reuse_version_);
  ASSERT_EQ(0, phy_block.valid_len_);
  ASSERT_EQ(false, phy_block.is_sealed_);
  ASSERT_EQ(true, phy_block.is_free_);
  ASSERT_EQ(0, phy_block.alloc_time_us_);

  ASSERT_EQ(OB_SUCCESS, phy_block.set_first_used(ObSSPhyBlockType::SS_CACHE_DATA_BLK));
  ASSERT_EQ(2, phy_block.get_next_reuse_version());
  ASSERT_EQ(false, phy_block.can_reuse());
  phy_block.set_sealed(100/*valid_len*/);
  ASSERT_EQ(false, phy_block.can_reuse());
  phy_block.set_sealed(0/*valid_len*/);
  ASSERT_EQ(true, phy_block.can_reuse());
  phy_block.is_sealed_ = false;
  phy_block.alloc_time_us_ = ObTimeUtility::current_time() - PHY_BLK_MAX_REUSE_TIME - 10000000;
  ASSERT_EQ(true, phy_block.can_reuse());

  phy_block.set_reuse_info(SS_MAX_REUSE_VERSION/*reuse_version*/, 1/*gc_reuse_version*/);
  ObSSPhyBlockReuseInfo reuse_info;
  phy_block.get_reuse_info(reuse_info);
  ASSERT_EQ(true, reuse_info.reach_gc_reuse_version());
  phy_block.set_reuse_info(2/*reuse_version*/, 4/*gc_reuse_version*/);
  phy_block.get_reuse_info(reuse_info);
  ASSERT_EQ(false, reuse_info.reach_gc_reuse_version());
  phy_block.reuse_version_ = 3;
  phy_block.get_reuse_info(reuse_info);
  ASSERT_EQ(true, reuse_info.reach_gc_reuse_version());

  phy_block.reuse_version_ = 5;
  phy_block.update_gc_reuse_version(phy_block.reuse_version_);
  ASSERT_EQ(5, phy_block.gc_reuse_version_);
  phy_block.set_valid_len(100);
  ASSERT_EQ(100, phy_block.get_valid_len());
  phy_block.valid_len_ -= 50;
  ASSERT_EQ(50, phy_block.get_valid_len());

  phy_block.inc_ref_count();
  ASSERT_EQ(2, phy_block.get_ref_count());

  ASSERT_NE(OB_SUCCESS, phy_block.set_first_used(ObSSPhyBlockType::SS_CACHE_DATA_BLK));
  bool succ_free = false;
  phy_block.try_free(succ_free);
  ASSERT_EQ(false, succ_free);
  phy_block.dec_ref_count();
  ASSERT_EQ(1, phy_block.get_ref_count());
  phy_block.try_free(succ_free);
  ASSERT_EQ(false, succ_free);
  phy_block.valid_len_ -= 50;
  phy_block.try_free(succ_free);
  ASSERT_EQ(true, succ_free);

  phy_block.reuse();
  phy_block.inc_ref_count();
  phy_block.reset();
  ASSERT_NE(0, phy_block.block_state_);
  phy_block.dec_ref_count();
  ASSERT_EQ(0, phy_block.block_state_);
}

TEST_F(TestSSMicroCacheCommonMeta, physical_block_persist_info)
{
  ObSSPhyBlockPersistInfo persist_info;
  persist_info.blk_idx_ = 1001;
  persist_info.reuse_version_ = 1000300;

  const int64_t buf_size = sizeof(ObSSPhyBlockPersistInfo);
  char buf[buf_size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, persist_info.serialize(buf, buf_size, pos));
  ASSERT_EQ(pos, persist_info.get_serialize_size());
  ObSSPhyBlockPersistInfo tmp_persist_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_persist_info.deserialize(buf, buf_size, pos));
  ASSERT_EQ(persist_info.blk_idx_, tmp_persist_info.blk_idx_);
  ASSERT_EQ(persist_info.reuse_version_, tmp_persist_info.reuse_version_);
}

TEST_F(TestSSMicroCacheCommonMeta, physical_block_common_header)
{
  ObSSPhyBlockCommonHeader common_header;
  ASSERT_EQ(false, common_header.is_valid());
  ASSERT_LT(0, common_header.header_size_);
  ASSERT_EQ(ObSSPhyBlockCommonHeader::get_serialize_size(), common_header.header_size_);
  common_header.payload_size_ = 201;
  common_header.payload_checksum_ = 505;
  common_header.set_block_type(ObSSPhyBlockType::SS_CACHE_DATA_BLK);
  ASSERT_EQ(true, common_header.is_valid());

  const int64_t buf_size = sizeof(ObSSPhyBlockCommonHeader);
  char buf[buf_size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, common_header.serialize(buf, buf_size, pos));

  ObSSPhyBlockCommonHeader tmp_common_header;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_common_header.deserialize(buf, buf_size, pos));
  ASSERT_EQ(true, tmp_common_header.is_valid());
  ASSERT_EQ(true, tmp_common_header.is_cache_data_blk());
  ASSERT_EQ(common_header.payload_size_, tmp_common_header.payload_size_);
  ASSERT_EQ(common_header.payload_checksum_, tmp_common_header.payload_checksum_);
}

TEST_F(TestSSMicroCacheCommonMeta, physical_block_header)
{
  ObSSNormalPhyBlockHeader phy_blk_header;
  ASSERT_EQ(false, phy_blk_header.is_valid());
  phy_blk_header.payload_checksum_ = 201;
  phy_blk_header.payload_size_ = 100;
  phy_blk_header.payload_offset_ = 8;
  phy_blk_header.micro_count_ = 11;
  phy_blk_header.micro_index_offset_ = 77;
  phy_blk_header.micro_index_size_ = 99;
  ASSERT_EQ(true, phy_blk_header.is_valid());

  const int64_t buf_len = phy_blk_header.get_fixed_serialize_size();
  char buf[buf_len];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, phy_blk_header.serialize(buf, buf_len, pos));
  ASSERT_EQ(pos, phy_blk_header.get_serialize_size());

  ObSSNormalPhyBlockHeader tmp_header;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_header.deserialize(buf, buf_len, pos));
  ASSERT_EQ(phy_blk_header.magic_, tmp_header.magic_);
  ASSERT_EQ(phy_blk_header.payload_checksum_, tmp_header.payload_checksum_);
  ASSERT_EQ(phy_blk_header.payload_size_, tmp_header.payload_size_);
  ASSERT_EQ(phy_blk_header.micro_count_, tmp_header.micro_count_);
  ASSERT_EQ(phy_blk_header.micro_index_offset_, tmp_header.micro_index_offset_);
  ASSERT_EQ(phy_blk_header.micro_index_size_, tmp_header.micro_index_size_);
}

TEST_F(TestSSMicroCacheCommonMeta, physical_block_header_compat)
{
  ObSSNormalPhyBlockHeader header;
  header.micro_count_ = 1001;
  header.payload_size_ = 230;

  const int64_t buf_len = header.get_fixed_serialize_size();
  char buf[buf_len];
  memset(buf, '\0', buf_len);

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.serialize(buf, buf_len, pos));
  ASSERT_EQ(pos, header.get_serialize_size());
  ASSERT_LE(pos, buf_len);

  ObSSNormalPhyBlockHeader tmp_header;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_header.deserialize(buf, buf_len, pos));
  ASSERT_EQ(true, tmp_header.is_valid());
  ASSERT_EQ(pos, header.get_serialize_size());
  ASSERT_EQ(header.micro_count_, tmp_header.micro_count_);
  ASSERT_EQ(header.payload_size_, tmp_header.payload_size_);

  TestSSPhysicalBlockHeader compat_header;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, compat_header.deserialize(buf, buf_len, pos));
  ASSERT_EQ(header.micro_count_, compat_header.micro_count_);
  ASSERT_EQ(header.payload_size_, compat_header.payload_size_);
  ASSERT_EQ(-2, compat_header.a_);
  ASSERT_EQ(pos, header.get_serialize_size());
}

TEST_F(TestSSMicroCacheCommonMeta, physical_block_handle)
{
  ObSSPhysicalBlock phy_block1;
  phy_block1.reuse_version_ = 1;
  ObSSPhysicalBlockHandle handle1;
  handle1.set_ptr(&phy_block1);
  ASSERT_EQ(1, phy_block1.get_reuse_version());
  ASSERT_EQ(2, phy_block1.ref_cnt_);

  ObSSPhysicalBlock phy_block2;
  phy_block2.reuse_version_ = 2;
  ObSSPhysicalBlockHandle handle2;
  handle2.set_ptr(&phy_block2);
  ASSERT_EQ(2, phy_block2.get_reuse_version());
  ASSERT_EQ(2, phy_block2.ref_cnt_);

  {
    ObArray<ObSSPhysicalBlockHandle> handle_arr;
    // ObArray push_back handle will inc ref_cnt cuz 'assign'
    ASSERT_EQ(OB_SUCCESS, handle_arr.push_back(handle1));
    ASSERT_EQ(OB_SUCCESS, handle_arr.push_back(handle2));
    ASSERT_EQ(3, phy_block1.ref_cnt_);
    ASSERT_EQ(3, phy_block2.ref_cnt_);

    // So, don't use ObArray reuse if you want to destroy its handle.
    handle_arr.reuse();
    ASSERT_EQ(3, phy_block1.ref_cnt_);
    ASSERT_EQ(3, phy_block2.ref_cnt_);

    // ObArray reset can dec ref_cnt.
    handle_arr.reset();
    ASSERT_EQ(2, phy_block1.ref_cnt_);
    ASSERT_EQ(2, phy_block2.ref_cnt_);

    ASSERT_EQ(OB_SUCCESS, handle_arr.push_back(handle1));
    ASSERT_EQ(OB_SUCCESS, handle_arr.push_back(handle2));
    ASSERT_EQ(3, phy_block1.ref_cnt_);
    ASSERT_EQ(3, phy_block2.ref_cnt_);
  }

  {
    ObArray<ObSSPhysicalBlockHandle> handle_arr;
    ASSERT_EQ(OB_SUCCESS, handle_arr.push_back(handle1));
    ASSERT_EQ(OB_SUCCESS, handle_arr.push_back(handle2));
    ASSERT_EQ(3, phy_block1.ref_cnt_);
    ASSERT_EQ(3, phy_block2.ref_cnt_);

    // pop_back won't dec ref_cnt
    ObSSPhysicalBlockHandle tmp_handle2;
    ASSERT_EQ(OB_SUCCESS, handle_arr.pop_back(tmp_handle2));
    ASSERT_EQ(true, tmp_handle2.is_valid());
    ASSERT_EQ(4, phy_block2.ref_cnt_);

    ObSSPhysicalBlockHandle tmp_handle1;
    ASSERT_EQ(OB_SUCCESS, handle_arr.pop_back(tmp_handle1));
    ASSERT_EQ(true, tmp_handle1.is_valid());
    ASSERT_EQ(4, phy_block1.ref_cnt_);

    handle_arr.reset();
    ASSERT_EQ(3, phy_block1.ref_cnt_);
    ASSERT_EQ(3, phy_block2.ref_cnt_);
  }
  // When ObArray destroy, can dec ref_cnt
  ASSERT_EQ(2, phy_block1.ref_cnt_);
  ASSERT_EQ(2, phy_block2.ref_cnt_);

  ObSSPhysicalBlockHandle tmp_handle;
  ASSERT_EQ(OB_SUCCESS, tmp_handle.assign(handle2));
  ASSERT_EQ(3, phy_block2.ref_cnt_);
  tmp_handle.set_ptr(&phy_block1); // If a valid handle set_ptr, it will reset original ptr.
  ASSERT_EQ(3, phy_block1.ref_cnt_);
  ASSERT_EQ(2, phy_block2.ref_cnt_);
  tmp_handle.reset();
  ASSERT_EQ(2, phy_block1.ref_cnt_);

  handle1.reset();
  handle2.reset();
  ASSERT_EQ(1, phy_block1.ref_cnt_);
  ASSERT_EQ(1, phy_block2.ref_cnt_);
}

TEST_F(TestSSMicroCacheCommonMeta, ls_cache_info)
{
  ObSSTabletCacheInfo tablet_cache_info;
  ASSERT_EQ(false, tablet_cache_info.is_valid());
  ASSERT_EQ(0, tablet_cache_info.get_valid_size());
  ObTabletID tablet_id(101);
  tablet_cache_info.tablet_id_ = tablet_id;
  ASSERT_EQ(true, tablet_cache_info.is_valid());
  tablet_cache_info.add_micro_size(true, false, 100);
  tablet_cache_info.add_micro_size(false, false, 200);
  ASSERT_EQ(300, tablet_cache_info.get_valid_size());
  tablet_cache_info.add_micro_size(false, true, 300);
  ASSERT_EQ(300, tablet_cache_info.get_valid_size());

  ObSSTabletCacheInfo tmp_tablet_cache_info = tablet_cache_info;
  ASSERT_EQ(true, tmp_tablet_cache_info.is_valid());
  ASSERT_EQ(300, tmp_tablet_cache_info.get_valid_size());

  ObSSLSCacheInfo ls_cache_info;
  ASSERT_EQ(false, ls_cache_info.is_valid());
  ASSERT_EQ(0, ls_cache_info.get_valid_size());
  share::ObLSID ls_id(102);
  ls_cache_info.ls_id_ = ls_id;
  ASSERT_EQ(true, ls_cache_info.is_valid());
  ls_cache_info.t1_size_ = 200;
  ls_cache_info.t2_size_ = 300;
  ASSERT_EQ(500, ls_cache_info.get_valid_size());
}

TEST_F(TestSSMicroCacheCommonMeta, super_block)
{
  const int64_t file_size = 1 << 30;
  ObSSMicroCacheSuperBlock super_block(file_size);
  ASSERT_EQ(file_size, super_block.cache_file_size_);
  ASSERT_EQ(OB_SUCCESS, super_block.blk_ckpt_entry_list_.push_back(5));
  ASSERT_EQ(OB_SUCCESS, super_block.blk_ckpt_entry_list_.push_back(15));
  ASSERT_EQ(OB_SUCCESS, super_block.blk_ckpt_entry_list_.push_back(25));
  ASSERT_EQ(OB_SUCCESS, super_block.micro_ckpt_entry_list_.push_back(35));
  ASSERT_EQ(OB_SUCCESS, super_block.micro_ckpt_entry_list_.push_back(45));

  // serialize & deserialize
  const int64_t buf_len = 1024;
  char buf[buf_len];
  MEMSET(buf, '\0', buf_len);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, super_block.serialize(buf, buf_len, pos));
  ASSERT_NE(0, pos);
  const int64_t data_len = super_block.get_serialize_size();

  int64_t tmp_pos = 0;
  ObSSMicroCacheSuperBlock tmp_super_block;
  ASSERT_EQ(OB_SUCCESS, tmp_super_block.deserialize(buf, data_len, tmp_pos));
  ASSERT_EQ(file_size, tmp_super_block.cache_file_size_);
  ASSERT_EQ(pos, tmp_pos);
  ASSERT_EQ(3, tmp_super_block.blk_ckpt_entry_list_.count());
  ASSERT_EQ(25, tmp_super_block.blk_ckpt_entry_list_.at(2));
  ASSERT_EQ(2, tmp_super_block.micro_ckpt_entry_list_.count());
  ASSERT_EQ(45, tmp_super_block.micro_ckpt_entry_list_.at(1));

  tmp_super_block.clear_ckpt_entry_list();
  ASSERT_EQ(0, tmp_super_block.blk_ckpt_entry_list_.count());
  ASSERT_EQ(0, tmp_super_block.micro_ckpt_entry_list_.count());
}

TEST_F(TestSSMicroCacheCommonMeta, micro_cache_key)
{
  const int64_t buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;

  // 1. serialize and deserialize logic_macro_id
  ObLogicMacroBlockId logic_macro_id(32, 200, 300);
  ASSERT_EQ(true, logic_macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, logic_macro_id.serialize(buf, buf_size, pos));
  ObLogicMacroBlockId tmp_logic_macro_id;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_logic_macro_id.deserialize(buf, buf_size, pos));
  ASSERT_EQ(tmp_logic_macro_id, logic_macro_id);

  ObLogicMicroBlockId logic_micro_id;
  logic_micro_id.logic_macro_id_ = logic_macro_id;
  logic_micro_id.logic_macro_id_.info_ = 400;
  logic_micro_id.version_ = 1;
  logic_micro_id.offset_ = 123;
  ASSERT_EQ(true, logic_micro_id.is_valid());
  // 2. serialize and deserialize logic_micro_id
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, logic_micro_id.serialize(buf, buf_size, pos));
  ObLogicMicroBlockId tmp_micro_id;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_micro_id.deserialize(buf, buf_size, pos));
  ASSERT_EQ(tmp_micro_id, logic_micro_id);
  ASSERT_EQ(400, tmp_micro_id.logic_macro_id_.info_);
  ASSERT_EQ(1, tmp_micro_id.version_);
  ASSERT_EQ(123, tmp_micro_id.offset_);

  // 3. serialize and deserialize for LOGICAL mode
  ObSSMicroBlockCacheKey micro_key;
  micro_key.mode_ = ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE;
  micro_key.logic_micro_id_ = logic_micro_id;
  micro_key.micro_crc_ = 99;
  pos = 0;
  ASSERT_EQ(true, micro_key.is_valid());
  ASSERT_EQ(OB_SUCCESS, micro_key.serialize(buf, buf_size, pos));
  ObSSMicroBlockCacheKey tmp_micro_key;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_micro_key.deserialize(buf, buf_size, pos));
  ASSERT_EQ(true, tmp_micro_key.is_valid());
  ASSERT_EQ(micro_key, tmp_micro_key);
  ASSERT_EQ(400, tmp_micro_key.logic_micro_id_.logic_macro_id_.info_);
  ASSERT_EQ(1, tmp_micro_key.logic_micro_id_.version_);
  ASSERT_EQ(123, tmp_micro_key.logic_micro_id_.offset_);
  ASSERT_EQ(99, tmp_micro_key.micro_crc_);
  tmp_micro_key.reset();
  ASSERT_EQ(false, tmp_micro_key.is_valid());

  // 4. serialize and deserialize for PHYSICAL mode
  ObSSMicroBlockCacheKey micro_key2;
  micro_key2.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
  blocksstable::MacroBlockId macro_id(0, 201, 0);
  micro_key2.micro_id_.macro_id_ = macro_id;
  micro_key2.micro_id_.offset_ = 127;
  micro_key2.micro_id_.size_ = 231;

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, micro_key2.serialize(buf, buf_size, pos));

  ObSSMicroBlockCacheKey tmp_micro_key2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_micro_key2.deserialize(buf, buf_size, pos));
  ASSERT_EQ(micro_key2, tmp_micro_key2);
  ASSERT_EQ(231, tmp_micro_key2.micro_id_.size_);
  ASSERT_EQ(127, tmp_micro_key2.micro_id_.offset_);
}

TEST_F(TestSSMicroCacheCommonMeta, micro_block_index)
{
  ObSSPhyBlockIdxRange physical_block_range;
  ASSERT_FALSE(physical_block_range.is_valid());

  physical_block_range.start_blk_idx_ = 1;
  physical_block_range.end_blk_idx_ = 50;
  ASSERT_TRUE(physical_block_range.is_valid());

  const int64_t buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, physical_block_range.serialize(buf, buf_size, pos));

  ObSSPhyBlockIdxRange tmp_physical_block_range;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_physical_block_range.deserialize(buf, buf_size, pos));
  ASSERT_EQ(pos, tmp_physical_block_range.get_serialize_size());
  ASSERT_EQ(1, tmp_physical_block_range.start_blk_idx_);
  ASSERT_EQ(50, tmp_physical_block_range.end_blk_idx_);
}

TEST_F(TestSSMicroCacheCommonMeta, physical_block_range)
{
  const int64_t buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;

  ObLogicMacroBlockId logic_macro_id(32, 200, 300);
  ObLogicMicroBlockId logic_micro_id;
  logic_micro_id.logic_macro_id_ = logic_macro_id;
  logic_micro_id.logic_macro_id_.info_ = 400;
  logic_micro_id.version_ = 1;
  logic_micro_id.offset_ = 123;
  ObSSMicroBlockCacheKey micro_key;
  micro_key.mode_ = ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE;
  micro_key.logic_micro_id_ = logic_micro_id;
  micro_key.micro_crc_ = 99;
  ASSERT_EQ(true, micro_key.is_valid());

  ObSSMicroBlockIndex micro_index;
  micro_index.micro_key_ = micro_key;
  ASSERT_EQ(false, micro_index.is_valid());
  micro_index.size_ = 100;
  ASSERT_EQ(true, micro_index.is_valid());

  ASSERT_EQ(OB_SUCCESS, micro_index.serialize(buf, buf_size, pos));
  ASSERT_EQ(pos, micro_index.get_serialize_size());

  ObSSMicroBlockIndex tmp_micro_index;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_micro_index.deserialize(buf, buf_size, pos));
  ASSERT_EQ(pos, tmp_micro_index.get_serialize_size());
  ASSERT_EQ(micro_index, tmp_micro_index);
  ASSERT_EQ(100, tmp_micro_index.size_);
  ASSERT_EQ(99, tmp_micro_index.micro_key_.micro_crc_);
}

TEST_F(TestSSMicroCacheCommonMeta, micro_block_handle)
{
  const int32_t micro_size = 100;

  ObSSMicroBlockMeta micro_meta;
  micro_meta.first_val_ = 5;
  micro_meta.length_ = micro_size;
  micro_meta.access_time_ = 20;
  MacroBlockId macro_id(0, 200, 0);
  int32_t offset = 99;
  ObSSMicroBlockCacheKey micro_key;
  micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
  micro_key.micro_id_.macro_id_ = macro_id;
  micro_key.micro_id_.offset_ = offset;
  micro_key.micro_id_.macro_id_ = macro_id;
  micro_key.micro_id_.size_ = micro_size;
  micro_meta.micro_key_ = micro_key;
  ASSERT_EQ(true, micro_meta.is_valid_field());
  ASSERT_EQ(0, micro_meta.ref_cnt_);

  ObSSMicroBlockMetaHandle micro_handle;
  micro_handle.set_ptr(&micro_meta);
  ASSERT_EQ(true, micro_handle.is_valid());
  ASSERT_EQ(1, micro_meta.ref_cnt_);
  micro_meta.inc_ref_count();
  ASSERT_EQ(2, micro_meta.ref_cnt_);

  ObArray<ObSSMicroBlockMetaHandle> micro_handle_arr;
  ASSERT_EQ(OB_SUCCESS, micro_handle_arr.push_back(micro_handle));
  ASSERT_EQ(1, micro_handle_arr.count());
  ASSERT_EQ(3, micro_meta.ref_cnt_);

  micro_handle_arr.reuse();
  ASSERT_EQ(3, micro_meta.ref_cnt_);
  micro_handle_arr.reset();
  ASSERT_EQ(2, micro_meta.ref_cnt_);

  ObArray<ObSSMicroBlockMetaHandle> micro_handle_arr2;
  ASSERT_EQ(OB_SUCCESS, micro_handle_arr2.push_back(micro_handle));
  ASSERT_EQ(1, micro_handle_arr2.count());
  ASSERT_EQ(3, micro_meta.ref_cnt_);
  micro_handle_arr2.reset();
  ASSERT_EQ(2, micro_meta.ref_cnt_);
  micro_handle.reset();
  ASSERT_EQ(1, micro_meta.ref_cnt_);
}

TEST_F(TestSSMicroCacheCommonMeta, micro_meta_handle_arr)
{
  const int32_t micro_size = 100;

  MacroBlockId macro_id(0, 100, 0);
  ObSSMicroBlockCacheKey micro_key;
  micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
  micro_key.micro_id_.macro_id_ = macro_id;
  micro_key.micro_id_.offset_ = 100;
  micro_key.micro_id_.size_ = micro_size;

  ObSSMicroBlockMeta micro_meta;
  micro_meta.reuse_version_ = 1;
  micro_meta.length_ = micro_size;
  micro_meta.access_time_ = 12;
  micro_meta.ref_cnt_ = 3; // To avoid allocator free.

  ObArray<ObSSMicroBlockMetaHandle> micro_handle_arr;
  micro_meta.is_persisted_ = true;
  micro_key.micro_id_.offset_ = 100;
  micro_meta.micro_key_ = micro_key;
  ObSSMicroBlockMetaHandle micro_handle;
  micro_handle.set_ptr(&micro_meta);
  ASSERT_EQ(4, micro_meta.ref_cnt_);
  ASSERT_EQ(OB_SUCCESS, micro_handle_arr.push_back(micro_handle));
  ASSERT_EQ(1, micro_handle_arr.count());
  ASSERT_EQ(5, micro_handle_arr.at(0).get_ptr()->ref_cnt_);
}

TEST_F(TestSSMicroCacheCommonMeta, mem_block_base_info)
{
  const uint64_t tenant_id = OB_SERVER_TENANT_ID;
  MacroBlockId macro_id(0, 100, 0);
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);

  ObArenaAllocator allocator;
  ObSSMicroCacheStat cache_stat;
  ObSSMemBlockPool mem_blk_pool(cache_stat);
  ObSSMemBlock mem_block(mem_blk_pool);
  ASSERT_EQ(0, mem_block.reuse_version_);
  ASSERT_EQ(1, mem_block.ref_cnt_);
  ASSERT_EQ(false, mem_block.is_valid());

  const int32_t data_buf_size = BLOCK_SIZE;
  char *data_buf = static_cast<char *>(allocator.alloc(data_buf_size));
  const int32_t index_buf_size = BLOCK_SIZE / 2;
  char *index_buf = static_cast<char *>(allocator.alloc(index_buf_size));
  ASSERT_NE(nullptr, data_buf);
  ASSERT_NE(nullptr, index_buf);
  ASSERT_EQ(OB_SUCCESS, mem_block.init(tenant_id, data_buf, data_buf_size, index_buf, index_buf_size));
  ASSERT_EQ(1, mem_block.reuse_version_);
  ASSERT_EQ(1, mem_block.ref_cnt_);
  ASSERT_EQ(data_buf, mem_block.data_buf_);
  ASSERT_EQ(data_buf_size, mem_block.data_buf_size_);
  ASSERT_EQ(index_buf, mem_block.index_buf_);
  ASSERT_EQ(index_buf_size, mem_block.index_buf_size_);

  mem_block.add_valid_micro_block(50);
  mem_block.inc_handled_count();
  mem_block.add_valid_micro_block(100);
  mem_block.inc_handled_count();
  mem_block.micro_count_ = 3;
  ASSERT_EQ(150, mem_block.valid_val_);
  ASSERT_EQ(2, mem_block.valid_count_);
  ASSERT_EQ(2, mem_block.handled_count_);
  ASSERT_EQ(false, mem_block.is_completed());
  mem_block.add_valid_micro_block(50);
  mem_block.inc_handled_count();
  ASSERT_EQ(3, mem_block.valid_count_);
  ASSERT_EQ(3, mem_block.handled_count_);
  ASSERT_EQ(true, mem_block.is_completed());
  ASSERT_EQ(200, mem_block.valid_val_);

  int64_t cur_reuse_version = mem_block.reuse_version_;
  ASSERT_EQ(false, mem_block.is_reuse_version_match(cur_reuse_version + 1));
  ASSERT_EQ(true, mem_block.is_reuse_version_match(cur_reuse_version));
  mem_block.inc_ref_count();
  ASSERT_EQ(2, mem_block.ref_cnt_);
  bool succ_free = false;
  mem_block.try_free(succ_free);
  ASSERT_EQ(false, succ_free);
  ASSERT_EQ(1, mem_block.ref_cnt_);

  mem_block.inc_ref_count();
  ASSERT_EQ(2, mem_block.ref_cnt_);
  mem_block.reset();
  ASSERT_EQ(0, mem_block.ref_cnt_);
  ASSERT_EQ(false, mem_block.is_valid());

  mem_block.ref_cnt_ = 2; // avoid try_free
}

TEST_F(TestSSMicroCacheCommonMeta, linked_header_compat)
{
  ObSSLinkedPhyBlockHeader header;
  header.item_count_ = 91;
  header.payload_size_ = 1112;
  header.payload_checksum_ = 5;
  header.previous_phy_block_id_ = 15;

  const int64_t buf_len = header.get_fixed_serialize_size();
  char buf[buf_len];
  memset(buf, '\0', buf_len);

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.serialize(buf, buf_len, pos));
  ASSERT_EQ(pos, header.get_serialize_size());
  ASSERT_LE(pos, buf_len);

  ObSSLinkedPhyBlockHeader tmp_header;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_header.deserialize(buf, buf_len, pos));
  ASSERT_EQ(pos, header.get_serialize_size());
  ASSERT_EQ(header.item_count_, tmp_header.item_count_);
  ASSERT_EQ(header.payload_size_, tmp_header.payload_size_);
  ASSERT_EQ(header.payload_checksum_, tmp_header.payload_checksum_);
  ASSERT_EQ(header.previous_phy_block_id_, tmp_header.previous_phy_block_id_);

  TestSSLinkedPhyBlockHeader compat_header;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, compat_header.deserialize(buf, buf_len, pos));
  ASSERT_EQ(header.item_count_, compat_header.item_count_);
  ASSERT_EQ(header.payload_size_, compat_header.payload_size_);
  ASSERT_EQ(header.payload_checksum_, compat_header.payload_checksum_);
  ASSERT_EQ(header.previous_phy_block_id_, compat_header.previous_phy_block_id_);
  ASSERT_EQ(-1, compat_header.a_);
  ASSERT_EQ(-1, compat_header.b_);
  ASSERT_EQ(pos, header.get_serialize_size());
}

TEST_F(TestSSMicroCacheCommonMeta, ssmicro_cache_key)
{
  const int64_t buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;

  // 1. serialize and deserialize logic_macro_id
  ObLogicMacroBlockId logic_macro_id(32, 200, 300);
  ASSERT_EQ(OB_SUCCESS, logic_macro_id.serialize(buf, buf_size, pos));
  ObLogicMacroBlockId tmp_logic_macro_id;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_logic_macro_id.deserialize(buf, buf_size, pos));
  ASSERT_EQ(tmp_logic_macro_id, logic_macro_id);

  ObLogicMicroBlockId logic_micro_id;
  logic_micro_id.logic_macro_id_ = logic_macro_id;
  logic_micro_id.logic_macro_id_.info_ = 400;
  logic_micro_id.info_ = 1001;
  // 2. serialize and deserialize logic_micro_id
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, logic_micro_id.serialize(buf, buf_size, pos));
  ObLogicMicroBlockId tmp_micro_id;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_micro_id.deserialize(buf, buf_size, pos));
  ASSERT_EQ(tmp_micro_id, logic_micro_id);
  ASSERT_EQ(400, tmp_micro_id.logic_macro_id_.info_);
  ASSERT_EQ(1001, tmp_micro_id.info_);

  // 3. serialize and deserialize for LOGICAL mode
  ObSSMicroBlockCacheKey micro_key;
  micro_key.mode_ = ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE;
  micro_key.logic_micro_id_ = logic_micro_id;
  micro_key.micro_crc_ = 99;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, micro_key.serialize(buf, buf_size, pos));
  ObSSMicroBlockCacheKey tmp_micro_key;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_micro_key.deserialize(buf, buf_size, pos));
  ASSERT_EQ(micro_key, tmp_micro_key);
  ASSERT_EQ(400, tmp_micro_key.logic_micro_id_.logic_macro_id_.info_);
  ASSERT_EQ(1001, tmp_micro_key.logic_micro_id_.info_);
  ASSERT_EQ(99, tmp_micro_key.micro_crc_);

  // 4. serialize and deserialize for PHYSICAL mode
  ObSSMicroBlockCacheKey micro_key2;
  micro_key2.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
  blocksstable::MacroBlockId macro_id(0, 201, 0);
  micro_key2.micro_id_.macro_id_ = macro_id;
  micro_key2.micro_id_.offset_ = 127;
  micro_key2.micro_id_.size_ = 231;

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, micro_key2.serialize(buf, buf_size, pos));

  ObSSMicroBlockCacheKey tmp_micro_key2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_micro_key2.deserialize(buf, buf_size, pos));
  ASSERT_EQ(micro_key2, tmp_micro_key2);
  ASSERT_EQ(231, tmp_micro_key2.micro_id_.size_);
  ASSERT_EQ(127, tmp_micro_key2.micro_id_.offset_);
}

TEST_F(TestSSMicroCacheCommonMeta, micro_map)
{
  typedef ObLinearHashMap<const ObSSMicroBlockCacheKey *, ObSSMicroBlockMetaHandle> TmpMicroMap;
  TmpMicroMap tmp_micro_map;
  ASSERT_EQ(OB_SUCCESS, tmp_micro_map.init(ObMemAttr(1, "TmpMap")));
  ObSSMicroBlockMeta micro_meta;
  micro_meta.first_val_ = 1;
  micro_meta.length_ = 100;
  micro_meta.access_time_ = 1;
  micro_meta.ref_cnt_ = 10;

  ObSSMicroBlockCacheKey micro_key;
  micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
  blocksstable::MacroBlockId macro_id(0, 201, 0);
  micro_key.micro_id_.macro_id_ = macro_id;
  micro_key.micro_id_.offset_ = 127;
  micro_key.micro_id_.size_ = 231;
  micro_meta.micro_key_ = micro_key;

  ObSSMicroBlockMetaHandle micro_meta_handle;
  micro_meta_handle.set_ptr(&micro_meta);
  ASSERT_EQ(11, micro_meta.ref_cnt_);
  const ObSSMicroBlockCacheKey &cur_micro_key = micro_meta.get_micro_key();
  ASSERT_EQ(OB_SUCCESS, tmp_micro_map.insert(&cur_micro_key, micro_meta_handle));
  ASSERT_EQ(12, micro_meta.ref_cnt_);
  ASSERT_EQ(OB_SUCCESS, tmp_micro_map.erase(&cur_micro_key));
  ASSERT_EQ(11, micro_meta.ref_cnt_);
}

// When mem_block_pool destroy free_list, if the ref_cnt of mem_block exceeds 1,
// it will retry until ref_cnt = 1 and then destroy mem_block.*/
TEST_F(TestSSMicroCacheCommonMeta, mem_pool_destroy_free_list)
{
  ObSSMicroCacheStat cache_stat;
  ObSSMemBlockPool mem_blk_pool(cache_stat);
  const int64_t def_count = 10;
  const int64_t max_cnt = 30;
  const int64_t max_bg_mem_blk_cnt = 10;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.init(OB_SERVER_TENANT_ID, BLOCK_SIZE, def_count, max_cnt, max_bg_mem_blk_cnt));

  ObSSMemBlock *mem_block = nullptr;
  ASSERT_EQ(OB_SUCCESS, mem_blk_pool.alloc(mem_block, true));
  bool succ_free = false;
  ASSERT_EQ(OB_SUCCESS, mem_block->try_free(succ_free));
  ASSERT_EQ(true, succ_free);
  ObSSMemBlockHandle mem_blk_handle;
  mem_blk_handle.set_ptr(mem_block);
  ASSERT_EQ(2, mem_blk_handle.get_ptr()->ref_cnt_);

  const int64_t sleep_us = 1000 * 1000;
  const int64_t start_us = ObTimeUtility::current_time();
  std::thread t([&]() {
    ob_usleep(sleep_us);
    mem_blk_handle.reset();
  });
  mem_blk_pool.destroy_free_list();
  const int64_t end_us = ObTimeUtility::current_time();
  t.join();
  ASSERT_LE(sleep_us + start_us, end_us);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_micro_cache_common_meta.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache_common_meta.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}