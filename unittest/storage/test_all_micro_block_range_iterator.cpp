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

#include <gtest/gtest.h>
#include "storage/ob_sstable_test.h"
#define private public
#include "storage/ob_all_micro_block_range_iterator.h"
#include "storage/ob_partition_component_factory.h"

namespace oceanbase {
using namespace storage;
using namespace blocksstable;
using namespace common;
namespace unittest {

class TestAllMicroBlockRangeIterator : public ObSSTableTest {
public:
  TestAllMicroBlockRangeIterator();
  virtual ~TestAllMicroBlockRangeIterator();
  void SetUp();
  void TearDown();

protected:
  static const int64_t ROW_CNT = 10000;
  ObPartitionComponentFactory cp_fty_;
};

TestAllMicroBlockRangeIterator::TestAllMicroBlockRangeIterator() : ObSSTableTest("ob_all_micro_block_range_iterator")
{}

TestAllMicroBlockRangeIterator::~TestAllMicroBlockRangeIterator()
{}

void TestAllMicroBlockRangeIterator::SetUp()
{
  ObPartitionMeta meta;
  meta.table_id_ = table_schema_.get_table_id();

  TestDataFilePrepare::SetUp();
  prepare_schema();
  ASSERT_EQ(OB_SUCCESS, sstable_.init(table_key_));
  ASSERT_EQ(OB_SUCCESS, sstable_.set_storage_file_handle(get_storage_file_handle()));
  prepare_data(ROW_CNT, sstable_);
  STORAGE_LOG(INFO, "macro block count", K(sstable_.get_macro_block_count()));

  srand(static_cast<uint32_t>(time(NULL)));
}

void TestAllMicroBlockRangeIterator::TearDown()
{
  ObSSTableTest::TearDown();
}

TEST_F(TestAllMicroBlockRangeIterator, test_init)
{
  ObTableHandle handle;
  const uint64_t index_id = table_schema_.get_table_id();
  ObAllMicroBlockRangeIterator iterator;
  ObExtStoreRange range;
  range.get_range().set_whole_range();
  ASSERT_NE(OB_SUCCESS, iterator.init(index_id, handle, range, false));
  ASSERT_EQ(OB_SUCCESS, handle.set_table(&sstable_));
  ASSERT_EQ(OB_SUCCESS, iterator.init(index_id, handle, range, false));
  ASSERT_NE(OB_SUCCESS, iterator.init(index_id, handle, range, false));
}

TEST_F(TestAllMicroBlockRangeIterator, test_range)
{
  ObTableHandle handle;
  const uint64_t index_id = table_schema_.get_table_id();
  ObExtStoreRange range;
  range.get_range().set_whole_range();
  ObAllMicroBlockRangeIterator iterator;
  const ObStoreRange* micro_range = nullptr;
  ObStoreRange pre_range;
  ObArenaAllocator allocator;
  ObStoreRow start_row;
  ObStoreRow end_row;
  ObObj cells[2][TEST_COLUMN_CNT];
  ObStoreRowkey end_key(cells[1], TEST_ROWKEY_COLUMN_CNT);
  ObStoreRowkey start_key(cells[0], TEST_ROWKEY_COLUMN_CNT);
  end_row.row_val_.assign(cells[1], TEST_COLUMN_CNT);
  start_row.row_val_.assign(cells[0], TEST_COLUMN_CNT);

  // whole range
  int64_t cnt = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row_cnt_, end_row));
  ASSERT_EQ(OB_SUCCESS, handle.set_table(&sstable_));
  ASSERT_EQ(OB_SUCCESS, iterator.init(index_id, handle, range, false));
  ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
  ASSERT_TRUE(NULL != micro_range);
  ++cnt;
  STORAGE_LOG(INFO, "debug: micro_range", K(*micro_range));
  ASSERT_TRUE(micro_range->get_start_key().is_min());
  while (micro_range->get_end_key().compare(end_key) < 0) {
    micro_range->deep_copy(allocator, pre_range);
    ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
    STORAGE_LOG(INFO, "debug: micro_range", K(*micro_range), K(cnt));
    ASSERT_TRUE(micro_range->get_start_key() == pre_range.get_end_key());
    ASSERT_FALSE(micro_range->get_border_flag().inclusive_start());
    ++cnt;
  }
  ASSERT_TRUE(micro_range->get_end_key().is_max());
  ASSERT_EQ(OB_ITER_END, iterator.get_next_range(micro_range));

  // 0 -> row_cnt_ - 1
  iterator.reset();
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(0, start_row));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row_cnt_ - 1, end_row));
  range.get_range().get_start_key() = start_key;
  range.get_range().get_end_key() = end_key;
  range.get_range().get_border_flag().set_inclusive_start();
  range.get_range().get_border_flag().set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, iterator.init(index_id, handle, range, false));
  ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
  ASSERT_TRUE(NULL != micro_range);
  ASSERT_TRUE(micro_range->get_start_key() == start_key);
  for (int64_t i = 1; i < cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
  }
  STORAGE_LOG(INFO, "endkey", K(micro_range->get_end_key()), K(end_key));
  ASSERT_TRUE(micro_range->get_end_key() == end_key);
  ASSERT_EQ(OB_ITER_END, iterator.get_next_range(micro_range));

  // reverse
  iterator.reset();
  ASSERT_EQ(OB_SUCCESS, iterator.init(index_id, handle, range, true));
  ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
  ASSERT_TRUE(NULL != micro_range);
  STORAGE_LOG(INFO, "endkey", K(*micro_range), K(end_key));
  ASSERT_TRUE(micro_range->get_end_key() == end_key);
  for (int64_t i = 1; i < cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
  }
  STORAGE_LOG(INFO, "micro_range = ", K(*micro_range), K(start_key), K(end_key));
  ASSERT_TRUE(micro_range->get_start_key() == start_key);
  ASSERT_EQ(OB_ITER_END, iterator.get_next_range(micro_range));
}

TEST_F(TestAllMicroBlockRangeIterator, test_border)
{
  ObTableHandle handle;
  const uint64_t index_id = table_schema_.get_table_id();
  ObExtStoreRange range;
  ObAllMicroBlockRangeIterator iterator;
  const ObStoreRange* micro_range = nullptr;
  ObStoreRow start_row;
  ObStoreRow end_row;
  ObObj cells[2][TEST_COLUMN_CNT];
  ObStoreRowkey end_key(cells[1], TEST_ROWKEY_COLUMN_CNT);
  ObStoreRowkey start_key(cells[0], TEST_ROWKEY_COLUMN_CNT);
  end_row.row_val_.assign(cells[1], TEST_COLUMN_CNT);
  start_row.row_val_.assign(cells[0], TEST_COLUMN_CNT);

  ObMacroBlockIterator macro_block_iterator;
  ObMacroBlockDesc desc;
  ASSERT_EQ(OB_SUCCESS, sstable_.scan_macro_block(macro_block_iterator, false));
  ASSERT_EQ(OB_SUCCESS, macro_block_iterator.get_next_macro_block(desc));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(desc.row_count_ - 1, start_row));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(desc.row_count_, end_row));
  range.get_range().get_start_key() = start_key;
  range.get_range().get_end_key() = end_key;
  range.get_range().get_border_flag().set_inclusive_start();
  range.get_range().get_border_flag().set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, handle.set_table(&sstable_));
  ASSERT_EQ(OB_SUCCESS, iterator.init(index_id, handle, range, false));
  ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
  ASSERT_TRUE(NULL != micro_range);
  STORAGE_LOG(INFO, "range = ", K(*micro_range), K(start_key));
  ASSERT_TRUE(micro_range->get_start_key() == start_key);
  ASSERT_TRUE(micro_range->get_border_flag().inclusive_start());
  ASSERT_TRUE(micro_range->get_end_key() == start_key);
  ASSERT_TRUE(micro_range->get_border_flag().inclusive_end());
  ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
  ASSERT_TRUE(micro_range->get_start_key() == start_key);
  ASSERT_FALSE(micro_range->get_border_flag().inclusive_start());
  ASSERT_TRUE(micro_range->get_end_key() == end_key);
  ASSERT_TRUE(micro_range->get_border_flag().inclusive_end());
  ASSERT_EQ(OB_ITER_END, iterator.get_next_range(micro_range));

  iterator.reset();
  range.get_range().get_border_flag().set_inclusive_start();
  range.get_range().get_border_flag().unset_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, iterator.init(index_id, handle, range, false));
  ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
  ASSERT_TRUE(NULL != micro_range);
  ASSERT_TRUE(micro_range->get_start_key() == start_key);
  ASSERT_TRUE(micro_range->get_border_flag().inclusive_start());
  ASSERT_TRUE(micro_range->get_end_key() == start_key);
  ASSERT_TRUE(micro_range->get_border_flag().inclusive_end());
  ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
  ASSERT_TRUE(micro_range->get_start_key() == start_key);
  ASSERT_FALSE(micro_range->get_border_flag().inclusive_start());
  ASSERT_TRUE(micro_range->get_end_key() == end_key);
  ASSERT_FALSE(micro_range->get_border_flag().inclusive_end());
  ASSERT_EQ(OB_ITER_END, iterator.get_next_range(micro_range));

  iterator.reset();
  range.get_range().get_border_flag().unset_inclusive_start();
  range.get_range().get_border_flag().set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, iterator.init(index_id, handle, range, false));
  ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
  ASSERT_TRUE(NULL != micro_range);
  ASSERT_TRUE(micro_range->get_start_key() == start_key);
  ASSERT_FALSE(micro_range->get_border_flag().inclusive_start());
  ASSERT_TRUE(micro_range->get_end_key() == end_key);
  ASSERT_TRUE(micro_range->get_border_flag().inclusive_end());
  ASSERT_EQ(OB_ITER_END, iterator.get_next_range(micro_range));

  iterator.reset();
  range.get_range().get_border_flag().unset_inclusive_start();
  range.get_range().get_border_flag().unset_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, iterator.init(index_id, handle, range, false));
  ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
  ASSERT_TRUE(NULL != micro_range);
  ASSERT_TRUE(micro_range->get_start_key() == start_key);
  ASSERT_FALSE(micro_range->get_border_flag().inclusive_start());
  ASSERT_TRUE(micro_range->get_end_key() == end_key);
  ASSERT_FALSE(micro_range->get_border_flag().inclusive_end());
  ASSERT_EQ(OB_ITER_END, iterator.get_next_range(micro_range));

  iterator.reset();
  range.get_range().get_start_key() = start_key;
  range.get_range().get_end_key() = end_key;
  range.get_range().get_border_flag().unset_inclusive_start();
  range.get_range().get_border_flag().unset_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, iterator.init(index_id, handle, range, false));
  ASSERT_EQ(OB_SUCCESS, iterator.get_next_range(micro_range));
  ASSERT_TRUE(NULL != micro_range);
  ASSERT_TRUE(micro_range->get_start_key() == start_key);
  ASSERT_FALSE(micro_range->get_border_flag().inclusive_start());
  ASSERT_TRUE(micro_range->get_end_key() == end_key);
  ASSERT_FALSE(micro_range->get_border_flag().inclusive_end());
  ASSERT_EQ(OB_ITER_END, iterator.get_next_range(micro_range));
}

TEST_F(TestAllMicroBlockRangeIterator, test_empty_range)
{
  ObTableHandle handle;
  const uint64_t index_id = table_schema_.get_table_id();
  ObExtStoreRange range;
  ObAllMicroBlockRangeIterator iterator;
  const ObStoreRange* micro_range = nullptr;
  ObStoreRow start_row;
  ObStoreRow end_row;
  ObObj cells[2][TEST_COLUMN_CNT];
  ObStoreRowkey end_key(cells[1], TEST_ROWKEY_COLUMN_CNT);
  ObStoreRowkey start_key(cells[0], TEST_ROWKEY_COLUMN_CNT);
  end_row.row_val_.assign(cells[1], TEST_COLUMN_CNT);
  start_row.row_val_.assign(cells[0], TEST_COLUMN_CNT);

  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row_cnt_, start_row));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row_cnt_ + 100, end_row));
  range.get_range().set_start_key(start_key);
  range.get_range().set_end_key(end_key);
  range.get_range().get_border_flag().set_inclusive_start();
  range.get_range().get_border_flag().set_inclusive_end();
  ASSERT_EQ(OB_SUCCESS, handle.set_table(&sstable_));
  ASSERT_EQ(OB_SUCCESS, iterator.init(index_id, handle, range, false));
  ASSERT_EQ(OB_ITER_END, iterator.get_next_range(micro_range));
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_all_micro_block_range_iterator.log*");
  OB_LOGGER.set_file_name("test_all_micro_block_range_iterator.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test_all_micro_block_range_iterator");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
