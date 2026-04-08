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

#include "lib/ob_errno.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

class TestSSPhysicalBlockInfo : public ::testing::Test
{
public:
  TestSSPhysicalBlockInfo();
  virtual ~TestSSPhysicalBlockInfo();
  virtual void SetUp();
  virtual void TearDown();

private:
  static const int32_t BLOCK_SIZE = 2 * 1024 * 1024;
};

struct MockObSSPhyBlockCommonHeaderWithExtraField
{
public:
  MockObSSPhyBlockCommonHeaderWithExtraField()
  {
    reset();
  }

  void reset()
  {
    header_size_ = get_serialize_size();
    version_ = ObSSPhyBlockCommonHeader::SS_PHY_BLK_COMMON_HEADER_VERSION;
    magic_ = ObSSPhyBlockCommonHeader::SS_PHY_BLK_COMMON_HEADER_MAGIC;
    payload_size_ = 0;
    payload_checksum_ = 0;
    blk_type_ = static_cast<int32_t>(ObSSPhyBlockType::SS_INVALID_TYPE);
    reserved_ = 0;
    compat_extra_field_ = 0;
  }

  bool is_valid() const
  {
    return (header_size_ > 0
        && version_ == ObSSPhyBlockCommonHeader::SS_PHY_BLK_COMMON_HEADER_VERSION
        && magic_ == ObSSPhyBlockCommonHeader::SS_PHY_BLK_COMMON_HEADER_MAGIC
        && blk_type_ != static_cast<int32_t>(ObSSPhyBlockType::SS_INVALID_TYPE));
  }

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_UNLIKELY(pos + get_serialize_size() > buf_len)) {
      ret = OB_BUF_NOT_ENOUGH;
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      MockObSSPhyBlockCommonHeaderWithExtraField *header =
          reinterpret_cast<MockObSSPhyBlockCommonHeaderWithExtraField *>(buf + pos);
      header->header_size_ = header_size_;
      header->version_ = version_;
      header->magic_ = magic_;
      header->payload_size_ = payload_size_;
      header->payload_checksum_ = payload_checksum_;
      header->attr_ = attr_;
      header->compat_extra_field_ = compat_extra_field_;
      pos += header->header_size_;
    }
    return ret;
  }

  int64_t get_serialize_size() const
  {
    return sizeof(MockObSSPhyBlockCommonHeaderWithExtraField);
  }

public:
  int32_t header_size_;
  int32_t version_;
  int32_t magic_;
  int32_t payload_size_;
  uint32_t payload_checksum_;
  union {
    int32_t attr_;
    struct {
      int32_t blk_type_ : 8;
      int32_t reserved_ : 24;
    };
  };
  int32_t compat_extra_field_;
};

TestSSPhysicalBlockInfo::TestSSPhysicalBlockInfo()
{}

TestSSPhysicalBlockInfo::~TestSSPhysicalBlockInfo()
{}

void TestSSPhysicalBlockInfo::SetUp()
{}

void TestSSPhysicalBlockInfo::TearDown()
{}

TEST_F(TestSSPhysicalBlockInfo, phy_blk)
{
  ObSSPhysicalBlock phy_blk;
  ASSERT_EQ(true, phy_blk.is_empty());
  ASSERT_EQ(true, phy_blk.is_free());
  ASSERT_EQ(false, phy_blk.is_sealed());
  phy_blk.mark_used(ObSSPhyBlockType::SS_MICRO_DATA_BLK, false/*is_sealed*/);
  phy_blk.mark_sealed(100);
  ASSERT_EQ(true, phy_blk.is_valid_micro_data_block());
  ASSERT_EQ(100, phy_blk.get_valid_len());
}

TEST_F(TestSSPhysicalBlockInfo, phy_blk_handle)
{
  ObSSPhysicalBlock phy_block1;
  phy_block1.reuse_version_ = 1;
  ObSSPhyBlockHandle handle1;
  handle1.set_ptr(&phy_block1);
  ASSERT_EQ(1, phy_block1.get_reuse_version());
  ASSERT_EQ(2, phy_block1.ref_cnt_);

  ObSSPhysicalBlock phy_block2;
  phy_block2.reuse_version_ = 2;
  ObSSPhyBlockHandle handle2;
  handle2.set_ptr(&phy_block2);
  ASSERT_EQ(2, phy_block2.get_reuse_version());
  ASSERT_EQ(2, phy_block2.ref_cnt_);

  {
    ObArray<ObSSPhyBlockHandle> handle_arr;
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
    ObArray<ObSSPhyBlockHandle> handle_arr;
    ASSERT_EQ(OB_SUCCESS, handle_arr.push_back(handle1));
    ASSERT_EQ(OB_SUCCESS, handle_arr.push_back(handle2));
    ASSERT_EQ(3, phy_block1.ref_cnt_);
    ASSERT_EQ(3, phy_block2.ref_cnt_);

    // pop_back won't dec ref_cnt
    ObSSPhyBlockHandle tmp_handle2;
    ASSERT_EQ(OB_SUCCESS, handle_arr.pop_back(tmp_handle2));
    ASSERT_EQ(true, tmp_handle2.is_valid());
    ASSERT_EQ(4, phy_block2.ref_cnt_);

    ObSSPhyBlockHandle tmp_handle1;
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

  ObSSPhyBlockHandle tmp_handle;
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

TEST_F(TestSSPhysicalBlockInfo, phy_blk_idx_range)
{
  ObSSPhyBlockIdxRange idx_range;
  ASSERT_EQ(false, idx_range.is_valid());
  idx_range.start_blk_idx_ = 10;
  idx_range.end_blk_idx_ = 10;
  ASSERT_EQ(false, idx_range.is_valid());
  idx_range.end_blk_idx_ = 20;
  ASSERT_EQ(true, idx_range.is_valid());

  const int64_t buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, idx_range.serialize(buf, buf_size, pos));
  ASSERT_EQ(pos, idx_range.get_serialize_size());
  ObSSPhyBlockIdxRange tmp_idx_range;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_idx_range.deserialize(buf, buf_size, pos));
  ASSERT_EQ(true, tmp_idx_range.is_valid());
  ASSERT_EQ(idx_range.start_blk_idx_, tmp_idx_range.start_blk_idx_);
  ASSERT_EQ(idx_range.end_blk_idx_, tmp_idx_range.end_blk_idx_);
}

TEST_F(TestSSPhysicalBlockInfo, phy_blk_info_ckpt_item)
{
  ObSSPhyBlockInfoCkptItem ckpt_item;
  ckpt_item.blk_idx_ = 1;
  ckpt_item.reuse_version_ = 0;
  ASSERT_EQ(false, ckpt_item.is_valid());
  ckpt_item.blk_idx_ = 10;
  ckpt_item.reuse_version_ = 2;
  ASSERT_EQ(true, ckpt_item.is_valid());

  const int64_t buf_size = 1024;
  char buf[buf_size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ckpt_item.serialize(buf, buf_size, pos));
  ASSERT_EQ(pos, ckpt_item.get_serialize_size());
  ObSSPhyBlockInfoCkptItem tmp_ckpt_item;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_ckpt_item.deserialize(buf, buf_size, pos));
  ASSERT_EQ(true, tmp_ckpt_item.is_valid());
  ASSERT_EQ(ckpt_item.blk_idx_, tmp_ckpt_item.blk_idx_);
  ASSERT_EQ(ckpt_item.reuse_version_, tmp_ckpt_item.reuse_version_);
}

TEST_F(TestSSPhysicalBlockInfo, phy_blk_common_header)
{
  ObSSPhyBlockCommonHeader common_header;
  ASSERT_EQ(false, common_header.is_valid());
  ASSERT_LT(0, common_header.header_size_);
  ASSERT_EQ(common_header.get_serialize_size(), common_header.header_size_);
  common_header.payload_size_ = 201;
  common_header.payload_checksum_ = 505;
  common_header.set_block_type(ObSSPhyBlockType::SS_MICRO_DATA_BLK);
  ASSERT_EQ(true, common_header.is_valid());
  ASSERT_EQ(false, common_header.is_super_blk());
  ASSERT_EQ(true, common_header.is_micro_data_blk());

  const int64_t buf_size = sizeof(ObSSPhyBlockCommonHeader);
  char buf[buf_size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, common_header.serialize(buf, buf_size, pos));

  ObSSPhyBlockCommonHeader tmp_common_header;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_common_header.deserialize(buf, buf_size, pos));
  ASSERT_EQ(true, tmp_common_header.is_valid());
  ASSERT_EQ(true, tmp_common_header.is_micro_data_blk());
  ASSERT_EQ(common_header.payload_size_, tmp_common_header.payload_size_);
  ASSERT_EQ(common_header.payload_checksum_, tmp_common_header.payload_checksum_);
}

TEST_F(TestSSPhysicalBlockInfo, phy_blk_common_header_compatibility)
{
  const int64_t buf_size = 512;
  char buf[buf_size];
  MEMSET(buf, '\0', buf_size);

  MockObSSPhyBlockCommonHeaderWithExtraField new_header;
  new_header.payload_size_ = 4096;
  new_header.payload_checksum_ = 10086;
  new_header.blk_type_ = static_cast<int32_t>(ObSSPhyBlockType::SS_MICRO_DATA_BLK);
  new_header.compat_extra_field_ = 20260312;
  ASSERT_EQ(true, new_header.is_valid());

  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_header.serialize(buf, buf_size, pos));
  ASSERT_EQ(new_header.get_serialize_size(), pos);

  ObSSPhyBlockCommonHeader old_header;
  int64_t old_pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_header.deserialize(buf, pos, old_pos));
  ASSERT_EQ(true, old_header.is_valid());
  ASSERT_EQ(new_header.header_size_, old_header.header_size_);
  ASSERT_EQ(new_header.version_, old_header.version_);
  ASSERT_EQ(new_header.magic_, old_header.magic_);
  ASSERT_EQ(new_header.payload_size_, old_header.payload_size_);
  ASSERT_EQ(new_header.payload_checksum_, old_header.payload_checksum_);
  ASSERT_EQ(new_header.attr_, old_header.attr_);
  ASSERT_EQ(pos, old_pos);
}

TEST_F(TestSSPhysicalBlockInfo, micro_data_blk_header)
{
  ObSSMicroDataBlockHeader data_blk_header;
  ASSERT_EQ(false, data_blk_header.is_valid());
  data_blk_header.payload_size_ = 100;
  data_blk_header.payload_offset_ = 8;
  data_blk_header.micro_count_ = 11;
  data_blk_header.micro_index_offset_ = 77;
  data_blk_header.micro_index_size_ = 99;
  ASSERT_EQ(true, data_blk_header.is_valid());

  const int64_t buf_len = data_blk_header.get_serialize_size();
  char buf[buf_len];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, data_blk_header.serialize(buf, buf_len, pos));
  ASSERT_EQ(pos, data_blk_header.get_serialize_size());

  ObSSMicroDataBlockHeader tmp_header;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_header.deserialize(buf, buf_len, pos));
  ASSERT_EQ(data_blk_header.magic_, tmp_header.magic_);
  ASSERT_EQ(data_blk_header.payload_size_, tmp_header.payload_size_);
  ASSERT_EQ(data_blk_header.micro_count_, tmp_header.micro_count_);
  ASSERT_EQ(data_blk_header.micro_index_offset_, tmp_header.micro_index_offset_);
  ASSERT_EQ(data_blk_header.micro_index_size_, tmp_header.micro_index_size_);
}

TEST_F(TestSSPhysicalBlockInfo, ckpt_blk_header)
{
  ObSSCkptBlockHeader ckpt_blk_header;
  ASSERT_EQ(false, ckpt_blk_header.is_valid());
  ckpt_blk_header.item_count_ = 10;
  ckpt_blk_header.payload_size_ = 100;
  ckpt_blk_header.payload_checksum_ = 2001;
  ckpt_blk_header.prev_phy_blk_id_ = 11;
  ASSERT_EQ(true, ckpt_blk_header.is_valid());

  const int64_t buf_len = ckpt_blk_header.get_serialize_size();
  char buf[buf_len];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ckpt_blk_header.serialize(buf, buf_len, pos));
  ASSERT_EQ(pos, ckpt_blk_header.get_serialize_size());

  ObSSCkptBlockHeader tmp_header;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_header.deserialize(buf, buf_len, pos));
  ASSERT_EQ(ckpt_blk_header.magic_, tmp_header.magic_);
  ASSERT_EQ(ckpt_blk_header.version_, tmp_header.version_);
  ASSERT_EQ(ckpt_blk_header.item_count_, tmp_header.item_count_);
  ASSERT_EQ(ckpt_blk_header.payload_size_, tmp_header.payload_size_);
  ASSERT_EQ(ckpt_blk_header.payload_checksum_, tmp_header.payload_checksum_);
  ASSERT_EQ(ckpt_blk_header.prev_phy_blk_id_, tmp_header.prev_phy_blk_id_);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_physical_block_info.log*");
  OB_LOGGER.set_file_name("test_ss_physical_block_info.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}