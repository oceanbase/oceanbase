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
#define private public
#define protected public

#include "storage/blocksstable/ob_index_block_row_scanner.h"
#include "storage/blocksstable/ob_index_block_row_scanner.h"
#include "ob_index_block_data_prepare.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{
class TestIndexBlockRowScanner : public TestIndexBlockDataPrepare
{
public:
  TestIndexBlockRowScanner();
  virtual ~TestIndexBlockRowScanner();
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown();
protected:
  ObFixedArray<ObColDesc, common::ObIAllocator> full_index_cols_;
};

TestIndexBlockRowScanner::TestIndexBlockRowScanner()
  : TestIndexBlockDataPrepare("Test index block row scanner")
{
}

TestIndexBlockRowScanner::~TestIndexBlockRowScanner()
{
}

void TestIndexBlockRowScanner::SetUpTestCase()
{
  TestIndexBlockDataPrepare::SetUpTestCase();
}

void TestIndexBlockRowScanner::TearDownTestCase()
{
  TestIndexBlockDataPrepare::TearDownTestCase();
}

void TestIndexBlockRowScanner::SetUp()
{
  TestIndexBlockDataPrepare::SetUp();
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle_));
}

void TestIndexBlockRowScanner::TearDown()
{
  tablet_handle_.reset();
  TestIndexBlockDataPrepare::TearDown();
}

TEST_F(TestIndexBlockRowScanner, transform)
{
  ObIndexBlockDataTransformer index_block_transformer;
  const ObMicroBlockHeader *micro_header
      = reinterpret_cast<const ObMicroBlockHeader *>(root_block_data_buf_.get_buf());
  int64_t extra_size = ObIndexBlockDataTransformer::get_transformed_block_mem_size(
      root_block_data_buf_);
  char * extra_buf = reinterpret_cast<char *>(allocator_.alloc(extra_size));
  ASSERT_NE(nullptr, extra_buf);
  ASSERT_EQ(OB_SUCCESS, index_block_transformer.transform(root_block_data_buf_, extra_buf, extra_size));
  const ObIndexBlockDataHeader *idx_blk_header
      = reinterpret_cast<const ObIndexBlockDataHeader *>(extra_buf);
  for (int64_t i = 0; i < idx_blk_header->row_cnt_; ++i) {
    STORAGE_LOG(INFO, "Show transformed root block rowkey", K(idx_blk_header->rowkey_array_[i]));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(max_row_seed_, row));
  ObDatumRowkey last_rowkey;
  ASSERT_EQ(OB_SUCCESS, last_rowkey.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT));
  ObDatumRowkey index_rowkey;
  ASSERT_EQ(OB_SUCCESS, index_rowkey.assign(idx_blk_header->rowkey_array_[idx_blk_header->row_cnt_ - 1].datums_, TEST_ROWKEY_COLUMN_CNT));
  ASSERT_EQ(last_rowkey, index_rowkey);

  // Test update index block on deep copy
  const int64_t new_buf_size = root_block_data_buf_.get_buf_size() + extra_size;
  char *new_buf = reinterpret_cast<char *>(allocator_.alloc(new_buf_size));
  ASSERT_NE(nullptr, new_buf);
  char *new_extra_buf = new_buf + root_block_data_buf_.get_buf_size();
  MEMCPY(new_buf, root_block_data_buf_.get_buf(), root_block_data_buf_.get_buf_size());
  ASSERT_EQ(OB_SUCCESS, index_block_transformer.update_index_block(
      *idx_blk_header,
      new_buf,
      root_block_data_buf_.get_buf_size(),
      new_extra_buf,
      extra_size));

  const ObIndexBlockDataHeader *new_idx_blk_header = reinterpret_cast<const ObIndexBlockDataHeader *>(new_extra_buf);
  ASSERT_NE(nullptr, new_idx_blk_header);
  ASSERT_TRUE(new_idx_blk_header->is_valid());
  ASSERT_EQ(new_idx_blk_header->row_cnt_, idx_blk_header->row_cnt_);
  ASSERT_EQ(new_idx_blk_header->col_cnt_, idx_blk_header->col_cnt_);
  for (int64_t i = 0; i < idx_blk_header->row_cnt_; ++i) {
    ASSERT_EQ(new_idx_blk_header->rowkey_array_[i], idx_blk_header->rowkey_array_[i]);
  }

  // clear src block extra buf
  memset(extra_buf, 0, extra_size);
  allocator_.free(extra_buf);
  ObIndexBlockRowParser idx_row_parser;
  for (int64_t i = 0; i < new_idx_blk_header->row_cnt_; ++i) {
    const ObDatumRowkey &rowkey = new_idx_blk_header->rowkey_array_[i];
    for (int64_t j = 0; j < rowkey.get_datum_cnt(); ++j) {
      ASSERT_NE(nullptr, rowkey.datums_[j].ptr_);
    }
    const char *idx_data = nullptr;
    ASSERT_EQ(OB_SUCCESS, new_idx_blk_header->get_index_data(i, idx_data));
    ASSERT_EQ(OB_SUCCESS, idx_row_parser.init(idx_data));
  }

}

TEST_F(TestIndexBlockRowScanner, prefetch_and_scan)
{
  ObIndexMicroBlockCache &index_block_cache = OB_STORE_CACHE.get_index_block_cache();
  ObIndexBlockRowParser idx_row_parser;
  ObIndexBlockRowScanner idx_scanner;
  ObIndexBlockRowScanner raw_idx_scanner;
  ObArray<int32_t> agg_projector;
  ObArray<ObColumnSchemaV2> agg_column_schema;
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObIndexBlockDataTransformer transformer;
  const ObMicroBlockHeader *micro_header
      = reinterpret_cast<const ObMicroBlockHeader *>(root_block_data_buf_.get_buf());
  int64_t extra_size = ObIndexBlockDataTransformer::get_transformed_block_mem_size(
      root_block_data_buf_);
  char * extra_buf = reinterpret_cast<char *>(allocator_.alloc(extra_size));
  ASSERT_NE(nullptr, extra_buf);
  ASSERT_EQ(OB_SUCCESS, transformer.transform(root_block_data_buf_, extra_buf, extra_size));
  const ObIndexBlockDataHeader *root_blk_header
      = reinterpret_cast<const ObIndexBlockDataHeader *>(extra_buf);

  ASSERT_EQ(OB_SUCCESS, idx_scanner.init(
      agg_projector, agg_column_schema, tablet_handle_.get_obj()->get_rowkey_read_info().get_datum_utils(), allocator_, query_flag, 0));
  ASSERT_EQ(OB_SUCCESS, raw_idx_scanner.init(
      agg_projector, agg_column_schema, tablet_handle_.get_obj()->get_rowkey_read_info().get_datum_utils(), allocator_, query_flag, 0));

  ObMacroBlockHandle macro_handle;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  int64_t root_row_id = 0;
  const char *index_data_ptr;
  ASSERT_EQ(OB_SUCCESS, root_blk_header->get_index_data(root_row_id, index_data_ptr));
  ASSERT_EQ(OB_SUCCESS, idx_row_parser.init(index_data_ptr));
  ASSERT_EQ(OB_SUCCESS, idx_row_parser.get_header(idx_row_header));
  ObMicroIndexInfo idx_row;
  idx_row.endkey_ = &root_blk_header->rowkey_array_[root_row_id];
  idx_row.row_header_ = idx_row_header;
  ASSERT_EQ(OB_SUCCESS, index_block_cache.prefetch(
          table_schema_.get_tenant_id(),
          idx_row_header->get_macro_id(),
          idx_row,
          query_flag,
          macro_handle));

  ASSERT_EQ(OB_SUCCESS, macro_handle.wait(2000)); // Wait at most 2 sec
  const ObMicroBlockData *read_block
      = reinterpret_cast<const ObMicroBlockData *>(macro_handle.get_buffer());
  ASSERT_NE(nullptr, read_block);
  ObMicroBlockData raw_block;
  raw_block.buf_ = read_block->get_buf();
  raw_block.size_ = read_block->get_buf_size();
  raw_block.type_ = read_block->type_;

  ObMicroIndexInfo read_idx_info;
  ObMicroIndexInfo raw_read_idx_info;
  ASSERT_EQ(OB_SUCCESS, idx_scanner.open(
      ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
      *read_block,
      root_blk_header->rowkey_array_[root_row_id]));
  ASSERT_EQ(OB_SUCCESS, raw_idx_scanner.open(
      ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
      raw_block,
      root_blk_header->rowkey_array_[root_row_id]));
  ASSERT_EQ(idx_scanner.current_, raw_idx_scanner.current_);

  idx_scanner.reuse();
  raw_idx_scanner.reuse();
  ObDatumRange query_range;
  query_range.set_whole_range();

  ASSERT_EQ(OB_SUCCESS, idx_scanner.open(
      ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
      *read_block,
      query_range,
      0, true, true));
  ASSERT_EQ(OB_SUCCESS, raw_idx_scanner.open(
      ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
      raw_block,
      query_range,
      0, true, true));

  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    ASSERT_EQ(idx_scanner.current_, raw_idx_scanner.current_);
    tmp_ret = idx_scanner.get_next(read_idx_info);
    ASSERT_EQ(tmp_ret, raw_idx_scanner.get_next(raw_read_idx_info));
    ASSERT_EQ(read_idx_info.row_header_, raw_read_idx_info.row_header_);
    STORAGE_LOG(INFO, "Show read idx info", K(read_idx_info), K(raw_read_idx_info));
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
}

} // blocksstable
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_index_block_row_scanner.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_index_block_row_scanner.log", true, false);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
