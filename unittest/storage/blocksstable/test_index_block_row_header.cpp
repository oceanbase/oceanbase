// owner: zs475329
// owner group: storage

/**
 * Copyright (c) 2026 OceanBase
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

#include <gtest/gtest.h>
#define protected public
#define private public
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "common/ob_version_def.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

struct ObIndexBlockRowHeader_432
{
  union
  {
    uint64_t pack_;
    struct
    {
      uint64_t version_:8;                  // Version number of index block row header
      uint64_t row_store_type_:8;           // Row store type of next level micro block
      uint64_t compressor_type_:8;          // Compressor type for next micro block
      uint64_t is_data_index_:1;       // Whether this tree is built for data index
      uint64_t is_data_block_:1;            // Whether current row point to a data block directly
      uint64_t is_leaf_block_:1;             // Whether current row point to a leaf index block
      uint64_t is_major_node_:1;            // Whether this tree is located in a major sstable
      uint64_t is_pre_aggregated_:1;        // Whether data in children of this row were pre-aggregated
      uint64_t is_deleted_:1;               // Whether the microblock was pointed was deleted
      uint64_t contain_uncommitted_row_:1;  // Whether children of this row contains uncommitted row
      uint64_t is_macro_node_:1;            // Whether this row represent for macro block level meta
      uint64_t has_string_out_row_ : 1;     // Whether sub-tree of this node has string column out row as lob
      uint64_t all_lob_in_row_ : 1;         // Whether sub-tree of this node has out row lob column
      uint64_t reserved_:30;
    };
  };
  int64_t macro_id_first_id_; // Physical macro block id, set to default in leaf node
  int64_t macro_id_second_id_;
  int64_t macro_id_third_id_;
  int32_t block_offset_;                    // Offset of micro block in macro block
  int32_t block_size_;                      // Length of micro block data
  int64_t master_key_id_;                   // Master key id for encryption
  int64_t encrypt_id_;                      // Encryption id
  char encrypt_key_[share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH]; // Encrypt key 16 bytes
  uint64_t row_count_;                      // Row count of the blocks this row points to
  uint64_t schema_version_;                 // Schema version of the data block
  // TODO: fill block count correctly
  uint64_t macro_block_count_;              // Macro block count this index row covered
  uint64_t micro_block_count_;              // Micro block count this index row covered
};

class TestIndexBlockRowHeader : public ::testing::Test
{
public:
  TestIndexBlockRowHeader();
  ~TestIndexBlockRowHeader();
  virtual void SetUp();
  virtual void TearDown();
};

TestIndexBlockRowHeader::TestIndexBlockRowHeader()
{

}

TestIndexBlockRowHeader::~TestIndexBlockRowHeader()
{

}

void TestIndexBlockRowHeader::SetUp()
{

}

void TestIndexBlockRowHeader::TearDown()
{

}

// 构造一个合法的 V1 格式 header（无 logic_micro_id、无 shared_data_macro_id、macro_id_fourth_id 为 0）
void build_valid_v1_header(ObIndexBlockRowHeader &header)
{
  header.reset();
  header.version_ = ObIndexBlockRowHeader::INDEX_BLOCK_HEADER_V1;
  header.row_store_type_ = static_cast<uint8_t>(ObRowStoreType::FLAT_ROW_STORE);
  header.compressor_type_ = static_cast<uint8_t>(ObCompressorType::NONE_COMPRESSOR);
  header.is_data_index_ = 1;
  header.is_data_block_ = 1;
  header.is_leaf_block_ = 1;
  header.is_major_node_ = 1;
  header.is_macro_node_ = 0;
  header.is_pre_aggregated_ = 0;
  header.is_deleted_ = 0;
  header.contain_uncommitted_row_ = 0;
  header.has_string_out_row_ = 0;
  header.all_lob_in_row_ = 1;
  header.has_logic_micro_id_ = 0;
  header.has_shared_data_macro_id_ = 0;
  header.has_macro_block_bloom_filter_ = 0;
  header.macro_id_first_id_ = 1;
  header.macro_id_second_id_ = 2;
  header.macro_id_third_id_ = 3;
  header.block_offset_ = 100;
  header.block_size_ = 2000;
  header.row_count_ = 100;
  header.schema_version_ = 1;
  header.macro_block_count_ = 1;
  header.micro_block_count_ = 1;
}

// 构造一个合法的 V2 格式 header（可含 macro_id_fourth_id 等）
void build_valid_v2_header(ObIndexBlockRowHeader &header)
{
  build_valid_v1_header(header);
  header.version_ = ObIndexBlockRowHeader::INDEX_BLOCK_HEADER_V2;
  header.macro_id_fourth_id_ = 4;
  header.logic_micro_id_ = ObLogicMicroBlockId();
  header.data_checksum_ = 1234567890;
  header.has_logic_micro_id_ = 1;
}

TEST_F(TestIndexBlockRowHeader, serialize_deserialize_v1_format)
{
  ObIndexBlockRowHeader header;
  build_valid_v1_header(header);
  ASSERT_TRUE(header.is_valid());

  const int64_t data_version_old = DATA_VERSION_4_3_2_0; // < 4.3.3, using v1 format
  int64_t serialize_size = header.get_serialize_size(data_version_old);
  ASSERT_GT(serialize_size, 0);
  ASSERT_LT(serialize_size, static_cast<int64_t>(sizeof(ObIndexBlockRowHeader)));
  // 104 is the sizeof(ObIndexBlockRowHeader) in 4.3.2.0 version.
  const int64_t serialize_size_4320 = 104;
  ASSERT_EQ(serialize_size_4320, serialize_size);

  char buf[sizeof(ObIndexBlockRowHeader) * 2];
  int64_t pos = 0;
  int ret = header.serialize(buf, sizeof(buf), pos, data_version_old);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(serialize_size, pos);

  ObIndexBlockRowHeader des_header;
  int64_t read_pos = 0;
  ret = des_header.deserialize(buf, pos, read_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_pos, pos);
  ASSERT_TRUE(des_header.is_valid());
  ASSERT_EQ(des_header.version_, header.version_);
  ASSERT_EQ(des_header.macro_id_first_id_, header.macro_id_first_id_);
  ASSERT_EQ(des_header.macro_id_second_id_, header.macro_id_second_id_);
  ASSERT_EQ(des_header.macro_id_third_id_, header.macro_id_third_id_);
  ASSERT_EQ(des_header.block_offset_, header.block_offset_);
  ASSERT_EQ(des_header.block_size_, header.block_size_);
  ASSERT_EQ(des_header.row_count_, header.row_count_);
}

TEST_F(TestIndexBlockRowHeader, serialize_deserialize_v2_format)
{
  ObIndexBlockRowHeader header;
  build_valid_v2_header(header);
  ASSERT_TRUE(header.is_valid());

  const int64_t data_version_new = DATA_VERSION_4_3_3_0; // >= 4.3.3, using v2 format
  int64_t serialize_size = header.get_serialize_size(data_version_new);
  ASSERT_GT(serialize_size, 0);

  char buf[sizeof(ObIndexBlockRowHeader) * 2];
  int64_t pos = 0;
  int ret = header.serialize(buf, sizeof(buf), pos, data_version_new);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(serialize_size, pos);

  ObIndexBlockRowHeader des_header;
  int64_t read_pos = 0;
  ret = des_header.deserialize(buf, pos, read_pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(read_pos, pos);
  ASSERT_TRUE(des_header.is_valid());
  ASSERT_EQ(des_header.version_, header.version_);
  ASSERT_EQ(des_header.macro_id_fourth_id_, header.macro_id_fourth_id_);
}

TEST_F(TestIndexBlockRowHeader, get_serialize_size_read_vs_write)
{
  ObIndexBlockRowHeader v1_header;
  build_valid_v1_header(v1_header);
  int64_t size_read = v1_header.get_serialize_size();
  int64_t size_write_old = v1_header.get_serialize_size(DATA_VERSION_4_3_3_0 - 1);
  ASSERT_EQ(sizeof(ObIndexBlockRowHeader_432), size_write_old);
  int64_t size_write_new = v1_header.get_serialize_size(DATA_VERSION_4_3_3_0);
  ASSERT_EQ(size_read, size_write_old);
  ASSERT_LT(size_write_old, size_write_new);

  ObIndexBlockRowHeader v2_header;
  build_valid_v2_header(v2_header);
  size_read = v2_header.get_serialize_size();
  size_write_new = v2_header.get_serialize_size(DATA_VERSION_4_3_3_0);
  ASSERT_EQ(size_read, size_write_new);
}

TEST_F(TestIndexBlockRowHeader, deserialize_invalid_args)
{
  ObIndexBlockRowHeader header;
  build_valid_v1_header(header);
  char buf[512];
  int64_t pos = 0;
  header.serialize(buf, sizeof(buf), pos, DATA_VERSION_4_3_3_0 - 1);

  ObIndexBlockRowHeader des_header;
  int64_t read_pos = 0;
  ASSERT_NE(OB_SUCCESS, des_header.deserialize(nullptr, 100, read_pos));
  read_pos = 0;
  ASSERT_NE(OB_SUCCESS, des_header.deserialize(buf, 0, read_pos));
  read_pos = -1;
  ASSERT_NE(OB_SUCCESS, des_header.deserialize(buf, sizeof(buf), read_pos));
}

TEST_F(TestIndexBlockRowHeader, serialize_invalid_args)
{
  ObIndexBlockRowHeader header;
  build_valid_v1_header(header);
  const int64_t data_version = DATA_VERSION_4_3_3_0;
  int64_t pos = 0;

  ASSERT_NE(OB_SUCCESS, header.serialize(nullptr, 1024, pos, data_version));
  pos = 0;
  ASSERT_NE(OB_SUCCESS, header.serialize(nullptr, 0, pos, data_version));
  pos = -1;
  char buf[512];
  ASSERT_NE(OB_SUCCESS, header.serialize(buf, sizeof(buf), pos, data_version));
  pos = 0;
  ASSERT_NE(OB_SUCCESS, header.serialize(buf, sizeof(buf), pos, 0));
  ObIndexBlockRowHeader invalid_header;
  invalid_header.reset();
  invalid_header.version_ = 0xFF;
  pos = 0;
  ASSERT_NE(OB_SUCCESS, invalid_header.serialize(buf, sizeof(buf), pos, data_version));
}

TEST_F(TestIndexBlockRowHeader, roundtrip_v1_then_deserialize_with_v1_size)
{
  ObIndexBlockRowHeader header;
  build_valid_v1_header(header);
  const int64_t data_version_old = DATA_VERSION_4_3_3_0 - 1;
  char buf[512];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.serialize(buf, sizeof(buf), pos, data_version_old));

  ObIndexBlockRowHeader des_header;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, des_header.deserialize(buf, pos, read_pos));
  ASSERT_EQ(des_header.get_serialize_size(), header.get_serialize_size(data_version_old));
}

} // namespace blocksstable
} // namespace oceanbase


int main(int argc, char **argv)
{
  system("rm -f test_index_block_row_header.log*");
  OB_LOGGER.set_file_name("test_index_block_row_header.log", true, false);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
