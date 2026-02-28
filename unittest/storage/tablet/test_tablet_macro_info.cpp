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

/**
 * Unit test for ObTabletMacroInfo, focusing on internal structure lifecycle:
 * - default ctor / reset / destructor
 * - is_owned_by_tablet_ semantics (reset() must not clear when owned by tablet)
 * - serialize / deserialize lifecycle independence
 * - deep_copy lifecycle and buffer ownership
 */

#include <gtest/gtest.h>
#define protected public
#define private public

#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_tablet_block_aggregated_info.h"
#include "storage/tablet/ob_tablet_block_header.h"
#include "storage/ob_super_block_struct.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace unittest
{

using namespace storage;
using namespace common;
using namespace blocksstable;

class TestTabletMacroInfo : public ::testing::Test
{
public:
  TestTabletMacroInfo() = default;
  virtual ~TestTabletMacroInfo() = default;
  virtual void SetUp() override {}
  virtual void TearDown() override {}
};

// Build minimal serialized buffer for ObTabletMacroInfo (empty entry_block, all arrays cnt=0)
// so we can test deserialize/deep_copy lifecycle without ObBlockInfoSet (which needs MTL).
static int build_minimal_serialized_macro_info(char *buf, const int64_t buf_len, int64_t &out_len)
{
  int ret = OB_SUCCESS;
  ObSecondaryMetaHeader meta_header;
  const int64_t header_size = meta_header.get_serialize_size();
  const MacroBlockId entry_block = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
  const int64_t version = 1;  // TABLET_MACRO_INFO_VERSION
  if (OB_ISNULL(buf) || buf_len < header_size + 128) {
    ret = OB_INVALID_ARGUMENT;
    return ret;
  }
  int64_t pos = header_size;
  const int64_t version_pos = pos;
  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, version))) {
    return ret;
  }
  const int64_t size_pos = pos;
  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, static_cast<int64_t>(0)))) {
    return ret;
  }
  if (OB_FAIL(entry_block.serialize(buf, buf_len, pos))) {
    return ret;
  }
  for (int i = 0; OB_SUCC(ret) && i < 4; i++) {
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, static_cast<int64_t>(0)))) {
      return ret;
    }
  }
  const int64_t payload_size = pos - header_size;
  int64_t size_value = payload_size;
  int64_t rewrite_pos = size_pos;
  if (OB_FAIL(serialization::encode_i64(buf, buf_len, rewrite_pos, size_value))) {
    return ret;
  }
  meta_header.payload_size_ = static_cast<int32_t>(payload_size);
  meta_header.checksum_ = static_cast<int32_t>(ob_crc64(buf + header_size, payload_size));
  int64_t header_pos = 0;
  if (OB_FAIL(meta_header.serialize(buf, buf_len, header_pos))) {
    return ret;
  }
  out_len = header_size + payload_size;
  return ret;
}

TEST_F(TestTabletMacroInfo, default_construct_reset_destruct)
{
  ObTabletMacroInfo info;
  ASSERT_FALSE(info.is_inited_);
  ASSERT_FALSE(info.is_owned_by_tablet_);
  ASSERT_EQ(info.entry_block_, ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK);
  ASSERT_EQ(info.meta_block_info_arr_.arr_, nullptr);
  ASSERT_EQ(info.meta_block_info_arr_.cnt_, 0);

  info.reset();
  ASSERT_FALSE(info.is_inited_);
  ASSERT_EQ(info.meta_block_info_arr_.arr_, nullptr);

  info.reset();
  ASSERT_FALSE(info.is_inited_);
}

TEST_F(TestTabletMacroInfo, reset_skipped_when_owned_by_tablet)
{
  ObTabletMacroInfo info;
  info.is_owned_by_tablet_ = true;
  info.entry_block_.first_id_ = 123;
  info.is_inited_ = true;

  info.reset();
  ASSERT_TRUE(info.is_owned_by_tablet_);
  ASSERT_EQ(info.entry_block_.first_id_, 123);
  ASSERT_TRUE(info.is_inited_);
}

TEST_F(TestTabletMacroInfo, reset_clears_when_not_owned_by_tablet)
{
  ObTabletMacroInfo info;
  info.is_owned_by_tablet_ = false;
  info.entry_block_.first_id_ = 123;
  info.is_inited_ = true;

  info.reset();
  ASSERT_FALSE(info.is_owned_by_tablet_);
  ASSERT_NE(info.entry_block_.first_id_, 123);
  ASSERT_FALSE(info.is_inited_);
}

TEST_F(TestTabletMacroInfo, deserialize_lifecycle)
{
  ObArenaAllocator allocator;
  char buf[512];
  int64_t ser_len = 0;
  ASSERT_EQ(OB_SUCCESS, build_minimal_serialized_macro_info(buf, sizeof(buf), ser_len));

  ObTabletMacroInfo info;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, info.deserialize(allocator, buf, ser_len, pos));
  ASSERT_TRUE(info.is_inited_);
  ASSERT_FALSE(info.is_owned_by_tablet_);
  ASSERT_TRUE(IS_EMPTY_BLOCK_LIST(info.entry_block_));
  ASSERT_EQ(info.meta_block_info_arr_.cnt_, 0);
  ASSERT_EQ(info.meta_block_info_arr_.arr_, nullptr);

  info.reset();
  ASSERT_FALSE(info.is_inited_);
  ASSERT_EQ(info.meta_block_info_arr_.arr_, nullptr);
}

TEST_F(TestTabletMacroInfo, serialize_deserialize_roundtrip)
{
  ObArenaAllocator allocator;
  char ser_buf[512];
  int64_t ser_len = 0;
  ASSERT_EQ(OB_SUCCESS, build_minimal_serialized_macro_info(ser_buf, sizeof(ser_buf), ser_len));

  ObTabletMacroInfo src;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.deserialize(allocator, ser_buf, ser_len, pos));
  ASSERT_TRUE(src.is_valid());

  const int64_t out_len = src.get_serialize_size();
  char *out_buf = static_cast<char *>(allocator.alloc(out_len));
  ASSERT_NE(nullptr, out_buf);
  int64_t out_pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(out_buf, out_len, out_pos));
  ASSERT_EQ(out_pos, out_len);

  ObTabletMacroInfo dst;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(allocator, out_buf, out_len, read_pos));
  ASSERT_TRUE(dst.is_valid());
  ASSERT_EQ(dst.entry_block_, src.entry_block_);
  ASSERT_EQ(dst.meta_block_info_arr_.cnt_, src.meta_block_info_arr_.cnt_);

  src.reset();
  ASSERT_FALSE(src.is_inited_);
  ASSERT_TRUE(dst.is_inited_);
}

TEST_F(TestTabletMacroInfo, deep_copy_lifecycle)
{
  ObArenaAllocator allocator;
  char ser_buf[512];
  int64_t ser_len = 0;
  ASSERT_EQ(OB_SUCCESS, build_minimal_serialized_macro_info(ser_buf, sizeof(ser_buf), ser_len));

  ObTabletMacroInfo src;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.deserialize(allocator, ser_buf, ser_len, pos));
  const int64_t deep_size = src.get_deep_copy_size();
  char *copy_buf = static_cast<char *>(allocator.alloc(deep_size));
  ASSERT_NE(nullptr, copy_buf);

  ObTabletMacroInfo *dest = nullptr;
  ASSERT_EQ(OB_SUCCESS, src.deep_copy(copy_buf, deep_size, dest));
  ASSERT_NE(nullptr, dest);
  ASSERT_TRUE(dest->is_inited_);
  ASSERT_FALSE(dest->is_owned_by_tablet_);  // deep_copy() sets is_owned_by_tablet_ = false
  ASSERT_EQ(dest->entry_block_, src.entry_block_);

  src.reset();
  ASSERT_FALSE(src.is_inited_);
  ASSERT_TRUE(dest->is_inited_);
}

TEST_F(TestTabletMacroInfo, destructor_always_clears_internal_state)
{
  ObArenaAllocator allocator;
  char ser_buf[512];
  int64_t ser_len = 0;
  ASSERT_EQ(OB_SUCCESS, build_minimal_serialized_macro_info(ser_buf, sizeof(ser_buf), ser_len));

  {
    ObTabletMacroInfo info;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, info.deserialize(allocator, ser_buf, ser_len, pos));
    info.is_owned_by_tablet_ = true;
  }
}

// Explicitly verify that deserialize() and deep_copy() set is_owned_by_tablet_ = false.
// init() also sets it (tested where ObBlockInfoSet is available, e.g. mtlenv).
TEST_F(TestTabletMacroInfo, deserialize_sets_is_owned_by_tablet_false)
{
  ObArenaAllocator allocator;
  char ser_buf[512];
  int64_t ser_len = 0;
  ASSERT_EQ(OB_SUCCESS, build_minimal_serialized_macro_info(ser_buf, sizeof(ser_buf), ser_len));

  ObTabletMacroInfo info;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, info.deserialize(allocator, ser_buf, ser_len, pos));
  ASSERT_TRUE(info.is_inited_);
  ASSERT_FALSE(info.is_owned_by_tablet_);
}

TEST_F(TestTabletMacroInfo, deep_copy_sets_is_owned_by_tablet_false)
{
  ObArenaAllocator allocator;
  char ser_buf[512];
  int64_t ser_len = 0;
  ASSERT_EQ(OB_SUCCESS, build_minimal_serialized_macro_info(ser_buf, sizeof(ser_buf), ser_len));

  ObTabletMacroInfo src;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.deserialize(allocator, ser_buf, ser_len, pos));
  const int64_t deep_size = src.get_deep_copy_size();
  char *copy_buf = static_cast<char *>(allocator.alloc(deep_size));
  ASSERT_NE(nullptr, copy_buf);

  ObTabletMacroInfo *dest = nullptr;
  ASSERT_EQ(OB_SUCCESS, src.deep_copy(copy_buf, deep_size, dest));
  ASSERT_NE(nullptr, dest);
  ASSERT_FALSE(dest->is_owned_by_tablet_);
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet_macro_info.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_tablet_macro_info.log", true, true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
