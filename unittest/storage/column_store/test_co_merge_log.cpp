/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include <gmock/gmock.h>

#include "storage/column_store/ob_co_merge_log.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;

namespace compaction
{
namespace unittest
{

class TestCOMergeLogBuffer : public ObCOMergeLogBuffer
{
public:
  explicit TestCOMergeLogBuffer(ObIAllocator &allocator)
    : ObCOMergeLogBuffer(allocator)
  {}

  bool is_inited() const { return is_inited_; }
  int64_t capacity() const { return capacity_; }
  int64_t pos() const { return pos_; }
  const ObCOMergeLogBlock &current_block() const { return current_block_; }
  char *overflow_buffer() const { return overflow_buffer_; }
  int64_t overflow_buffer_size() const { return overflow_buffer_size_; }
  int alloc_overflow_buffer_for_test(const int64_t size)
  {
    return alloc_overflow_buffer(size);
  }
};

TEST(TestMergeLog, state_and_classification)
{
  ObMergeLog log;
  EXPECT_FALSE(log.is_valid());
  EXPECT_FALSE(log.is_range_mergelog());
  EXPECT_FALSE(log.is_delete_mergelog());
  EXPECT_EQ(ObMergeLog::INVALID, log.op_);
  EXPECT_EQ(-1, log.major_idx_);
  EXPECT_EQ(-1, log.row_id_);

  log.set_value(ObMergeLog::INSERT, -1, 100);
  EXPECT_TRUE(log.is_valid());
  EXPECT_FALSE(log.is_range_mergelog());
  EXPECT_FALSE(log.is_delete_mergelog());

  log.set_value(ObMergeLog::UPDATE, 1, 101);
  EXPECT_TRUE(log.is_valid());
  EXPECT_FALSE(log.is_range_mergelog());
  EXPECT_FALSE(log.is_delete_mergelog());

  log.set_value(ObMergeLog::DELETE, 1, 102);
  EXPECT_TRUE(log.is_valid());
  EXPECT_FALSE(log.is_range_mergelog());
  EXPECT_TRUE(log.is_delete_mergelog());

  log.set_value(ObMergeLog::REPLAY, 2, 103);
  EXPECT_TRUE(log.is_valid());
  EXPECT_TRUE(log.is_range_mergelog());
  EXPECT_FALSE(log.is_delete_mergelog());

  log.set_value(ObMergeLog::DELETE_RANGE, 2, 104);
  EXPECT_TRUE(log.is_valid());
  EXPECT_TRUE(log.is_range_mergelog());
  EXPECT_TRUE(log.is_delete_mergelog());

  EXPECT_STREQ("INSERT", ObMergeLog::get_op_type_str(ObMergeLog::INSERT));
  EXPECT_STREQ("DELETE_RANGE", ObMergeLog::get_op_type_str(ObMergeLog::DELETE_RANGE));
  EXPECT_STREQ("INVALID", ObMergeLog::get_op_type_str(ObMergeLog::INVALID));
  EXPECT_STREQ("INVALID", ObMergeLog::get_op_type_str(-1));

  log.reset();
  EXPECT_FALSE(log.is_valid());
  EXPECT_EQ(ObMergeLog::INVALID, log.op_);
  EXPECT_EQ(-1, log.major_idx_);
  EXPECT_EQ(-1, log.row_id_);
}

TEST(TestMergeLog, range_continuity)
{
  const ObMergeLog invalid;
  ObMergeLog replay(ObMergeLog::REPLAY, 3, 10);
  EXPECT_TRUE(replay.is_continuous(invalid));

  EXPECT_TRUE(ObMergeLog(ObMergeLog::REPLAY, 3, 10).is_continuous(replay));
  EXPECT_TRUE(ObMergeLog(ObMergeLog::REPLAY, 3, 11).is_continuous(replay));
  EXPECT_FALSE(ObMergeLog(ObMergeLog::REPLAY, 3, 9).is_continuous(replay));
  EXPECT_FALSE(ObMergeLog(ObMergeLog::REPLAY, 4, 11).is_continuous(replay));
  EXPECT_FALSE(ObMergeLog(ObMergeLog::DELETE_RANGE, 3, 11).is_continuous(replay));

  ObMergeLog delete_range(ObMergeLog::DELETE_RANGE, 5, 20);
  EXPECT_TRUE(delete_range.is_continuous(invalid));
  EXPECT_TRUE(ObMergeLog(ObMergeLog::DELETE_RANGE, 5, 21).is_continuous(delete_range));
  EXPECT_FALSE(ObMergeLog(ObMergeLog::INSERT, -1, 21).is_continuous(invalid));
}

TEST(TestMergeLog, serialization_round_trip)
{
  const ObMergeLog source(ObMergeLog::DELETE_RANGE, 7, INT64_MAX - 1);
  char buffer[128] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, source.serialize(buffer, sizeof(buffer), pos));
  ASSERT_EQ(source.get_serialize_size(), pos);

  ObMergeLog result;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, result.deserialize(buffer, pos, read_pos));
  EXPECT_EQ(pos, read_pos);
  EXPECT_TRUE(source == result);

  int64_t short_pos = 0;
  EXPECT_NE(OB_SUCCESS,
            source.serialize(buffer, source.get_serialize_size() - 1, short_pos));
  read_pos = 0;
  EXPECT_NE(OB_SUCCESS, result.deserialize(buffer, pos - 1, read_pos));
}

TEST(TestCOMergeProjector, reorder_columns_and_copy_row_flag)
{
  ObArenaAllocator allocator("TProjector");
  uint16_t column_idxs[] = {2, 0, 3};
  ObStorageColumnGroupSchema cg_schema(
      share::schema::NORMAL_COLUMN_GROUP,
      NONE_COMPRESSOR,
      FLAT_ROW_STORE,
      8 * 1024,
      3,
      0,
      0,
      ARRAYSIZEOF(column_idxs),
      column_idxs);
  ObCOMergeProjector projector;
  ASSERT_EQ(OB_SUCCESS, projector.init(cg_schema, allocator));
  EXPECT_EQ(ARRAYSIZEOF(column_idxs), projector.get_projector_count());
  ASSERT_NE(nullptr, projector.get_projector());
  EXPECT_EQ(2, projector.get_projector()[0]);
  EXPECT_EQ(0, projector.get_projector()[1]);
  EXPECT_EQ(3, projector.get_projector()[2]);

  ObDatumRow source;
  ASSERT_EQ(OB_SUCCESS, source.init(4));
  source.row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
  for (int64_t i = 0; i < source.count_; ++i) {
    source.storage_datums_[i].set_int(10 + i);
  }

  ObDatumRow result;
  ASSERT_EQ(OB_SUCCESS, result.init(ARRAYSIZEOF(column_idxs)));
  bool is_all_nop = true;
  ASSERT_EQ(OB_SUCCESS, projector.project(source, result, is_all_nop));
  EXPECT_FALSE(is_all_nop);
  EXPECT_TRUE(result.row_flag_.is_update());
  EXPECT_EQ(12, result.storage_datums_[0].get_int());
  EXPECT_EQ(10, result.storage_datums_[1].get_int());
  EXPECT_EQ(13, result.storage_datums_[2].get_int());

  ASSERT_EQ(OB_SUCCESS, projector.project(source));
  const ObDatumRow &internal_result = projector.get_project_row();
  EXPECT_TRUE(internal_result.row_flag_.is_update());
  EXPECT_EQ(12, internal_result.storage_datums_[0].get_int());
  EXPECT_EQ(10, internal_result.storage_datums_[1].get_int());
  EXPECT_EQ(13, internal_result.storage_datums_[2].get_int());
  EXPECT_EQ(OB_INIT_TWICE, projector.init(cg_schema, allocator));
}

TEST(TestCOMergeProjector, all_projected_columns_are_nop)
{
  ObArenaAllocator allocator("TProjectorNop");
  uint16_t column_idxs[] = {3, 1};
  ObStorageColumnGroupSchema cg_schema(
      share::schema::NORMAL_COLUMN_GROUP,
      NONE_COMPRESSOR,
      FLAT_ROW_STORE,
      8 * 1024,
      2,
      0,
      0,
      ARRAYSIZEOF(column_idxs),
      column_idxs);
  ObCOMergeProjector projector;
  ASSERT_EQ(OB_SUCCESS, projector.init(cg_schema, allocator));

  ObDatumRow source;
  ASSERT_EQ(OB_SUCCESS, source.init(4));
  source.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  source.storage_datums_[0].set_int(100);
  source.storage_datums_[1].set_nop();
  source.storage_datums_[2].set_int(200);
  source.storage_datums_[3].set_nop();

  ObDatumRow result;
  ASSERT_EQ(OB_SUCCESS, result.init(ARRAYSIZEOF(column_idxs)));
  bool is_all_nop = false;
  ASSERT_EQ(OB_SUCCESS, projector.project(source, result, is_all_nop));
  EXPECT_TRUE(is_all_nop);
  EXPECT_TRUE(result.row_flag_.is_insert());
  EXPECT_TRUE(result.storage_datums_[0].is_nop());
  EXPECT_TRUE(result.storage_datums_[1].is_nop());
}

TEST(TestCOMergeProjector, invalid_state_and_arguments)
{
  ObArenaAllocator allocator("TProjectorErr");
  ObCOMergeProjector projector;
  ObDatumRow source;
  ObDatumRow result;
  ASSERT_EQ(OB_SUCCESS, source.init(2));
  ASSERT_EQ(OB_SUCCESS, result.init(1));
  bool is_all_nop = false;

  EXPECT_EQ(OB_NOT_INIT, projector.project(source, result, is_all_nop));
  ObStorageColumnGroupSchema invalid_schema;
  EXPECT_EQ(OB_INVALID_ARGUMENT, projector.init(invalid_schema, allocator));

  uint16_t invalid_idx[] = {2};
  ObStorageColumnGroupSchema cg_schema(
      share::schema::SINGLE_COLUMN_GROUP,
      NONE_COMPRESSOR,
      FLAT_ROW_STORE,
      8 * 1024,
      1,
      0,
      0,
      ARRAYSIZEOF(invalid_idx),
      invalid_idx);
  ASSERT_EQ(OB_SUCCESS, projector.init(cg_schema, allocator));
  EXPECT_EQ(OB_ERR_UNEXPECTED, projector.project(source, result, is_all_nop));

  ObDatumRow invalid_source;
  EXPECT_EQ(OB_INVALID_ARGUMENT, projector.project(invalid_source, result, is_all_nop));
  ObDatumRow wrong_size_result;
  ASSERT_EQ(OB_SUCCESS, wrong_size_result.init(2));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            projector.project(source, wrong_size_result, is_all_nop));

  projector.reset();
  EXPECT_EQ(0, projector.get_projector_count());
  EXPECT_EQ(nullptr, projector.get_projector());
  EXPECT_EQ(OB_NOT_INIT, projector.project(source, result, is_all_nop));
}

TEST(TestCOMergeLogBuffer, init_and_reserve)
{
  ObArenaAllocator allocator("TLogBuffer");
  TestCOMergeLogBuffer buffer(allocator);
  EXPECT_EQ(OB_NOT_INIT, buffer.reserve(64));
  EXPECT_EQ(OB_INVALID_ARGUMENT, buffer.init(0));
  EXPECT_FALSE(buffer.is_inited());
  EXPECT_EQ(nullptr, buffer.data());

  ASSERT_EQ(OB_SUCCESS, buffer.init(32));
  ASSERT_TRUE(buffer.is_inited());
  ASSERT_NE(nullptr, buffer.data());
  EXPECT_EQ(32, buffer.capacity());
  for (int64_t i = 0; i < buffer.capacity(); ++i) {
    EXPECT_EQ(0, buffer.data()[i]);
  }

  buffer.data()[0] = 'x';
  char *const original_data = buffer.data();
  ASSERT_EQ(OB_SUCCESS, buffer.reserve(16));
  EXPECT_EQ(original_data, buffer.data());
  EXPECT_EQ('x', buffer.data()[0]);
  EXPECT_EQ(32, buffer.capacity());

  ASSERT_EQ(OB_SUCCESS, buffer.reserve(96));
  EXPECT_NE(original_data, buffer.data());
  EXPECT_EQ(96, buffer.capacity());
  for (int64_t i = 0; i < buffer.capacity(); ++i) {
    EXPECT_EQ(0, buffer.data()[i]);
  }
}

TEST(TestCOMergeLogBuffer, validate_block_header_magic)
{
  ObArenaAllocator allocator("TLogBlock");
  TestCOMergeLogBuffer buffer(allocator);
  const int64_t block_size = 128;
  EXPECT_EQ(OB_NOT_INIT, buffer.set_current_block(block_size));
  ASSERT_EQ(OB_SUCCESS, buffer.init(block_size));
  EXPECT_EQ(OB_INVALID_ARGUMENT, buffer.set_current_block(block_size + 1));
  EXPECT_EQ(OB_ERR_UNEXPECTED, buffer.set_current_block(block_size));

  ObCOMergeLogBlockHeader *header =
      reinterpret_cast<ObCOMergeLogBlockHeader *>(buffer.data());
  header->magic_num_ = ObCOMergeLogBlockHeader::MAGIC_NUM;
  header->piece_count_ = 2;
  header->length_ = 37;
  ASSERT_EQ(OB_SUCCESS, buffer.set_current_block(block_size));

  const ObCOMergeLogBlock &block = buffer.current_block();
  EXPECT_EQ(block_size, block.block_size_);
  EXPECT_EQ(header, block.header_);
  EXPECT_EQ(block_size - static_cast<int64_t>(sizeof(ObCOMergeLogBlockHeader)),
            block.block_max_writable_size());
  EXPECT_EQ(37, block.block_max_readable_size());

  ObCOMergeLogBlock empty_block;
  EXPECT_EQ(0, empty_block.block_max_readable_size());
}

TEST(TestCOMergeLogBuffer, overflow_buffer_and_reset)
{
  ObArenaAllocator allocator("TLogBufReset");
  TestCOMergeLogBuffer buffer(allocator);
  ASSERT_EQ(OB_SUCCESS, buffer.init(64));
  ASSERT_EQ(OB_SUCCESS, buffer.alloc_overflow_buffer_for_test(32));
  ASSERT_NE(nullptr, buffer.overflow_buffer());
  EXPECT_EQ(32, buffer.overflow_buffer_size());
  MEMSET(buffer.overflow_buffer(), 0x7f, buffer.overflow_buffer_size());

  char *const overflow_buffer = buffer.overflow_buffer();
  ASSERT_EQ(OB_SUCCESS, buffer.alloc_overflow_buffer_for_test(16));
  EXPECT_EQ(overflow_buffer, buffer.overflow_buffer());
  EXPECT_EQ(32, buffer.overflow_buffer_size());
  for (int64_t i = 0; i < buffer.overflow_buffer_size(); ++i) {
    EXPECT_EQ(0, buffer.overflow_buffer()[i]);
  }

  buffer.reset();
  EXPECT_FALSE(buffer.is_inited());
  EXPECT_EQ(nullptr, buffer.data());
  EXPECT_EQ(nullptr, buffer.overflow_buffer());
  EXPECT_EQ(0, buffer.overflow_buffer_size());
  EXPECT_EQ(OB_NOT_INIT, buffer.reserve(128));

  ASSERT_EQ(OB_SUCCESS, buffer.init(16));
  EXPECT_TRUE(buffer.is_inited());
  EXPECT_EQ(16, buffer.capacity());
}

} // namespace unittest
} // namespace compaction
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_co_merge_log.log*");
  OB_LOGGER.set_file_name("test_co_merge_log.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
