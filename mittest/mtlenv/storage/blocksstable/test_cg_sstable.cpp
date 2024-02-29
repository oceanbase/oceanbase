/**
 * Copyright (c) 2023 OceanBase
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
#define private public
#define protected public

#include "storage/blocksstable/index_block/ob_index_block_tree_cursor.h"
#include "storage/blocksstable/index_block/ob_index_block_dual_meta_iterator.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "ob_index_block_data_prepare.h"

namespace oceanbase
{
using namespace common;

namespace blocksstable
{
class TestCgSSTable : public TestIndexBlockDataPrepare
{
public:
  TestCgSSTable();
  virtual ~TestCgSSTable() {}
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();
};

TestCgSSTable::TestCgSSTable()
  : TestIndexBlockDataPrepare("Test index block tree cursor", ObMergeType::MAJOR_MERGE)
{
  is_cg_data_ = true;
}

void TestCgSSTable::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestCgSSTable::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestCgSSTable::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));

  if (is_cg_data_) {
    prepare_cg_data();
  } else {
    prepare_data();
  }
}

void TestCgSSTable::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

TEST_F(TestCgSSTable, test_cg_sstable)
{
  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *micro_reader;
  ASSERT_EQ(OB_SUCCESS, reader_helper.init(allocator_));
  ASSERT_EQ(OB_SUCCESS, reader_helper.get_reader(root_index_builder_->index_store_desc_.get_desc().get_row_store_type(), micro_reader));

  ObDatumRow row;
  OK(row.init(allocator_, 2));
  OK(micro_reader->init(root_block_data_buf_, nullptr));
  ObIndexBlockRowParser idx_row_parser;
  int64_t rows_cnt = -1;
  for (int64_t it = 0; it != micro_reader->row_count(); ++it) {
    idx_row_parser.reset();
    OK(micro_reader->get_row(it, row));
    OK(idx_row_parser.init(1, row));
    rows_cnt += idx_row_parser.header_->row_count_;
    ASSERT_EQ(rows_cnt, row.storage_datums_[0].get_int());
    ASSERT_EQ(rows_cnt, idx_row_parser.get_row_offset());
  }
}

TEST_F(TestCgSSTable, test_cg_macro_iter)
{
  sstable_.key_.table_type_ = ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
  const ObITableReadInfo *read_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(read_info));

  ObIndexBlockMacroIterator macro_iter;
  MacroBlockId macro_block_id;
  int64_t start_row_offset;
  int tmp_ret = OB_SUCCESS;
  int64_t cnt = 0;
  ObDatumRange iter_range;
  iter_range.set_whole_range();

  // reverse whole scan
  ASSERT_EQ(OB_SUCCESS, macro_iter.open(
      sstable_, iter_range, *read_info, allocator_, true, true));
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = macro_iter.get_next_macro_block(macro_block_id, start_row_offset);
    STORAGE_LOG(DEBUG, "Reverse get next macro block", K(tmp_ret), K(cnt),
        K(macro_block_id), K(macro_iter.micro_endkeys_.at(macro_iter.micro_endkeys_.count() - 1)));
    if (OB_SUCCESS == tmp_ret) {
      ++cnt;
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
  ASSERT_EQ(cnt, data_macro_block_cnt_);
}

TEST_F(TestCgSSTable, test_cg_index_tree_cursor)
{
  sstable_.key_.table_type_ = ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE;
  const ObITableReadInfo *read_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(read_info));
  ObIndexBlockTreeCursor tree_cursor;
  OK(tree_cursor.init(sstable_, allocator_, read_info));
  ObStorageDatum cg_rowkey[1];
  ObDatumRowkey rowkey;
  OK(rowkey.assign(cg_rowkey, 1));

  bool equal = false;
  bool is_beyond_range = false;

  // test invalid rowkey
  cg_rowkey[0].set_int(row_cnt_);
  OK(tree_cursor.pull_up_to_root());
  OK(tree_cursor.drill_down(rowkey, ObIndexBlockTreeCursor::MoveDepth::LEAF, true, equal, is_beyond_range));
  ASSERT_TRUE(is_beyond_range);

  int64_t start_row_offset = 0;
  bool is_macro_start = true;
  ObDatumRowkey endkey;
  const ObIndexBlockRowParser *parser = nullptr;
  for (int64_t i = 0; i < row_cnt_; ++i) {
    cg_rowkey[0].set_int(i);
    OK(tree_cursor.pull_up_to_root());
    OK(tree_cursor.drill_down(rowkey, ObIndexBlockTreeCursor::MoveDepth::LEAF, true, equal, is_beyond_range));
    ASSERT_FALSE(is_beyond_range);
    if (is_macro_start) {
      OK(tree_cursor.get_current_endkey(endkey));
      ASSERT_EQ(endkey.datums_[0].get_int(), 9);
      is_macro_start = false;
    }
    if ((i + 1) % 100 == 0) {
      OK(tree_cursor.get_idx_parser(parser));
      ASSERT_EQ(parser->get_row_offset(), 99);

      OK(tree_cursor.get_current_endkey(endkey));
      ASSERT_EQ(endkey.datums_[0].get_int(), 99);

      start_row_offset += 100;
      is_macro_start = true;
    }
  }

}
} // end blocksstable
} // end oceanbase


int main(int argc, char **argv)
{
  system("rm -f test_cg_sstable.log*");
  OB_LOGGER.set_file_name("test_cg_sstable.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
