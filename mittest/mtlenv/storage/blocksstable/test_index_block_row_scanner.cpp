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

#include "storage/blocksstable/index_block/ob_index_block_row_scanner.h"
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
  int prepare_tmp_rowkey(ObDatumRowkey &rowkey);
  ObFixedArray<ObColDesc, common::ObIAllocator> full_index_cols_;
};

TestIndexBlockRowScanner::TestIndexBlockRowScanner()
  : TestIndexBlockDataPrepare("Test index block row scanner", MAJOR_MERGE, true)
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

int TestIndexBlockRowScanner::prepare_tmp_rowkey(ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  // schema_rowkey + mvcc
  const int64_t index_rokwey_cnt = TEST_ROWKEY_COLUMN_CNT + 2;
  void *buf = allocator_.alloc(sizeof(ObStorageDatum) * index_rokwey_cnt);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObStorageDatum *datums = new (buf) ObStorageDatum[index_rokwey_cnt];
    ret = rowkey.assign(datums, index_rokwey_cnt);
  }
  return ret;
}

TEST_F(TestIndexBlockRowScanner, transform)
{
  ObIndexBlockDataTransformer index_block_transformer;
  char *allocated_buf = nullptr;
  ASSERT_EQ(OB_SUCCESS, index_block_transformer.transform(
      root_block_data_buf_, root_block_data_buf_, allocator_, allocated_buf));
  ASSERT_NE(nullptr, allocated_buf);
  const ObIndexBlockDataHeader *idx_blk_header
      = reinterpret_cast<const ObIndexBlockDataHeader *>(root_block_data_buf_.get_extra_buf());
  ASSERT_TRUE(idx_blk_header->is_valid());
  ObDatumRowkey tmp_rowkey;
  ASSERT_EQ(OB_SUCCESS, prepare_tmp_rowkey(tmp_rowkey));
  for (int64_t i = 0; i < idx_blk_header->row_cnt_; ++i) {
    ASSERT_EQ(OB_SUCCESS, idx_blk_header->rowkey_vector_->get_rowkey(i, tmp_rowkey));
    STORAGE_LOG(INFO, "Show transformed root block rowkey", K(tmp_rowkey));
  }
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, TEST_COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(max_row_seed_, row));
  ObDatumRowkey last_rowkey;
  ASSERT_EQ(OB_SUCCESS, last_rowkey.assign(row.storage_datums_, TEST_ROWKEY_COLUMN_CNT));
  ObDatumRowkey index_rowkey;
  ASSERT_EQ(OB_SUCCESS, idx_blk_header->rowkey_vector_->get_rowkey(idx_blk_header->row_cnt_ - 1, tmp_rowkey));
  ASSERT_EQ(OB_SUCCESS, index_rowkey.assign(tmp_rowkey.datums_, TEST_ROWKEY_COLUMN_CNT));
  ASSERT_EQ(last_rowkey, index_rowkey);

  // Test update index block on deep copy
  ObIndexBlockDataHeader new_idx_blk_header;
  const int64_t new_buf_size = root_block_data_buf_.get_buf_size() + root_block_data_buf_.get_extra_size();
  char *new_buf = reinterpret_cast<char *>(allocator_.alloc(new_buf_size));
  ASSERT_NE(nullptr, new_buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_idx_blk_header.deep_copy_transformed_index_block(
      *idx_blk_header, new_buf_size, new_buf, pos));


  ASSERT_TRUE(new_idx_blk_header.is_valid());
  ASSERT_EQ(new_idx_blk_header.row_cnt_, idx_blk_header->row_cnt_);
  ASSERT_EQ(new_idx_blk_header.col_cnt_, idx_blk_header->col_cnt_);
  ObDatumRowkey tmp_new_rowkey;
  ASSERT_EQ(OB_SUCCESS, prepare_tmp_rowkey(tmp_new_rowkey));
  for (int64_t i = 0; i < idx_blk_header->row_cnt_; ++i) {
    ASSERT_EQ(OB_SUCCESS, idx_blk_header->rowkey_vector_->get_rowkey(i, tmp_rowkey));
    ASSERT_EQ(OB_SUCCESS, new_idx_blk_header.rowkey_vector_->get_rowkey(i, tmp_new_rowkey));
    STORAGE_LOG(INFO, "cmp deep copy rowkey", K(i),
        K(tmp_new_rowkey), K(tmp_rowkey));
    ASSERT_EQ(tmp_new_rowkey, tmp_rowkey);
  }

  // clear src block extra buf
  memset(((char *)idx_blk_header), 0, idx_blk_header->data_buf_size_);
  allocator_.free((char *)idx_blk_header);
  ObIndexBlockRowParser idx_row_parser;
  for (int64_t i = 0; i < new_idx_blk_header.row_cnt_; ++i) {
    idx_row_parser.reset();
    ASSERT_EQ(OB_SUCCESS, new_idx_blk_header.rowkey_vector_->get_rowkey(i, tmp_new_rowkey));
    const ObDatumRowkey &rowkey = tmp_new_rowkey;
    for (int64_t j = 0; j < rowkey.get_datum_cnt(); ++j) {
      ASSERT_NE(nullptr, rowkey.datums_[j].ptr_);
    }
    const char *idx_data = nullptr;
    int64_t idx_len = 0;
    ASSERT_EQ(OB_SUCCESS, new_idx_blk_header.get_index_data(i, idx_data, idx_len));
    ASSERT_EQ(OB_SUCCESS, idx_row_parser.init(idx_data, idx_len));
  }

}

TEST_F(TestIndexBlockRowScanner, prefetch_and_scan)
{
  ObIndexMicroBlockCache &index_block_cache = OB_STORE_CACHE.get_index_block_cache();
  ObIndexBlockRowParser idx_row_parser;
  ObIndexBlockRowScanner idx_scanner;
  ObIndexBlockRowScanner raw_idx_scanner;
  ObQueryFlag query_flag;
  query_flag.set_use_block_cache();
  ObIndexBlockDataTransformer transformer;

  char *allocated_buf = nullptr;
  ASSERT_EQ(OB_SUCCESS, transformer.transform(
      root_block_data_buf_, root_block_data_buf_, allocator_, allocated_buf));
  ASSERT_NE(nullptr, allocated_buf);
  const ObIndexBlockDataHeader *root_blk_header
      = reinterpret_cast<const ObIndexBlockDataHeader *>(root_block_data_buf_.get_extra_buf());
  ASSERT_TRUE(root_blk_header->is_valid());

  ASSERT_EQ(OB_SUCCESS, idx_scanner.init(
          tablet_handle_.get_obj()->get_rowkey_read_info().get_datum_utils(), allocator_, query_flag, 0));
  ASSERT_EQ(OB_SUCCESS, raw_idx_scanner.init(
          tablet_handle_.get_obj()->get_rowkey_read_info().get_datum_utils(), allocator_, query_flag, 0));

  ObMacroBlockHandle macro_handle;
  const ObIndexBlockRowHeader *idx_row_header = nullptr;
  int64_t root_row_id = 0;
  const char *index_data_ptr;
  int64_t index_data_len = 0;
  ASSERT_EQ(OB_SUCCESS, root_blk_header->get_index_data(root_row_id, index_data_ptr, index_data_len));
  ASSERT_EQ(OB_SUCCESS, idx_row_parser.init(index_data_ptr, index_data_len));
  ASSERT_EQ(OB_SUCCESS, idx_row_parser.get_header(idx_row_header));
  ObDatumRowkey tmp_rowkey;
  ASSERT_EQ(OB_SUCCESS, prepare_tmp_rowkey(tmp_rowkey));
  ASSERT_EQ(OB_SUCCESS, root_blk_header->rowkey_vector_->get_rowkey(root_row_id, tmp_rowkey));
  ObMicroIndexInfo idx_row;
  idx_row.endkey_.set_compact_rowkey(&tmp_rowkey);
  idx_row.row_header_ = idx_row_header;
  ASSERT_EQ(OB_SUCCESS, index_block_cache.prefetch(
      table_schema_.get_tenant_id(),
      idx_row_header->get_macro_id(),
      idx_row,
      query_flag.is_use_block_cache(),
      macro_handle,
      &allocator_));

  ASSERT_EQ(OB_SUCCESS, macro_handle.wait());
  const ObMicroBlockData *read_block
      = &reinterpret_cast<const ObMicroBlockCacheValue *>(macro_handle.get_buffer())->get_block_data();
  ASSERT_NE(nullptr, read_block);

  ObMacroBlockHandle raw_block_macro_handle;
  ASSERT_EQ(OB_SUCCESS, index_block_cache.prefetch(
      table_schema_.get_tenant_id(),
      idx_row_header->get_macro_id(),
      idx_row,
      false /* disable use block cache */,
      raw_block_macro_handle,
      &allocator_));
  ASSERT_EQ(OB_SUCCESS, raw_block_macro_handle.wait());
  const ObMicroBlockData *raw_block
      = &reinterpret_cast<const ObMicroBlockCacheValue *>(raw_block_macro_handle.get_buffer())->get_block_data();

  ObMicroIndexInfo read_idx_info;
  ObMicroIndexInfo raw_read_idx_info;
  ASSERT_EQ(OB_SUCCESS, idx_scanner.open(
      ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
      *read_block,
      tmp_rowkey));
  ASSERT_EQ(OB_SUCCESS, raw_idx_scanner.open(
      ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID,
      *raw_block,
      tmp_rowkey));

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
      *raw_block,
      query_range,
      0, true, true));

  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS == tmp_ret) {
    tmp_ret = idx_scanner.get_next(read_idx_info);
    if (OB_SUCCESS == tmp_ret) {
      ASSERT_EQ(tmp_ret, raw_idx_scanner.get_next(raw_read_idx_info));
      ASSERT_EQ(read_idx_info.row_header_->get_macro_id(), raw_read_idx_info.row_header_->get_macro_id());
      ASSERT_EQ(read_idx_info.row_header_->get_block_offset(), raw_read_idx_info.row_header_->get_block_offset());
      ASSERT_EQ(read_idx_info.row_header_->get_block_size(), raw_read_idx_info.row_header_->get_block_size());
      ASSERT_EQ(read_idx_info.row_header_->pack_, raw_read_idx_info.row_header_->pack_);
    }
  }
  ASSERT_EQ(OB_ITER_END, tmp_ret);
}

} // blocksstable
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_index_block_row_scanner.log*");
  OB_LOGGER.set_file_name("test_index_block_row_scanner.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
