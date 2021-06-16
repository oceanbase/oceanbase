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
#include "ob_multi_version_sstable_test.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "ob_uncommitted_trans_test.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace unittest {

class TestMicroBlockRowScanner : public ObMultiVersionSSTableTest {
public:
  TestMicroBlockRowScanner() : ObMultiVersionSSTableTest("testmicroblockrowscanner")
  {}
  virtual ~TestMicroBlockRowScanner()
  {}

  virtual void SetUp()
  {
    ObMultiVersionSSTableTest::SetUp();
    store_ctx_.trans_table_guard_ = new transaction::ObTransStateTableGuard();
  }
  virtual void TearDown()
  {
    columns_.reset();
    param_.reset();
    context_.reset();
    allocator_.reuse();
    ObMultiVersionSSTableTest::TearDown();
    projector_.reuse();
    store_ctx_.reset();
  }

  void prepare_query_param(const ObVersionRange& version_range, const bool is_minor_merge, const bool is_reverse_scan);

  int build_macro_and_scan(const int64_t micro_cnt, const char** micro_data,
      ObMultiVersionMicroBlockMinorMergeRowScanner& m_scanner, ObMockIterator& scanner_iter);

  ObArray<ObColDesc> columns_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObArenaAllocator allocator_;
  ObArray<int32_t> projector_;
  ObStoreCtx store_ctx_;
  TestUncommittedMinorMergeScan test_trans_part_ctx_;
};

void TestMicroBlockRowScanner::prepare_query_param(
    const ObVersionRange& version_range, const bool is_minor_merge, const bool is_reverse_scan)
{
  columns_.reset();
  param_.reset();
  context_.reset();
  projector_.reset();
  block_cache_ws_.reset();

  ObQueryFlag query_flag;

  if (is_reverse_scan) {
    query_flag.scan_order_ = ObQueryFlag::Reverse;
  }
  query_flag.multi_version_minor_merge_ = is_minor_merge;

  ObColDesc col_desc;
  int multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int schema_rowkey_cnt = rowkey_cnt_ - multi_version_col_cnt;  // schema rowkey count
  int trans_version_col =
      ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(schema_rowkey_cnt, multi_version_col_cnt);
  int sql_no_col =
      ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(schema_rowkey_cnt, multi_version_col_cnt);
  for (int i = 0; i < column_cnt_; i++) {
    if (trans_version_col != i && sql_no_col != i) {
      col_desc.col_id_ = i + OB_APP_MIN_COLUMN_ID;
      col_desc.col_type_ = data_iter_[0].get_column_type()[i];
      OK(columns_.push_back(col_desc));
    }
  }
  for (int i = 0; i < column_cnt_ - multi_version_col_cnt; i++) {
    OK(projector_.push_back(i));
  }

  param_.table_id_ = combine_id(TENANT_ID, TABLE_ID);
  param_.schema_version_ = SCHEMA_VERSION;
  param_.rowkey_cnt_ = schema_rowkey_cnt;  // schema rowkey count
  param_.out_cols_ = &columns_;
  if (!is_minor_merge) {
    param_.projector_ = &projector_;
  } else {
    param_.projector_ = NULL;
  }

  OK(block_cache_ws_.init(TENANT_ID));
  context_.query_flag_ = query_flag;
  context_.store_ctx_ = &store_ctx_;
  context_.allocator_ = &allocator_;
  context_.stmt_allocator_ = &allocator_;
  context_.block_cache_ws_ = &block_cache_ws_;
  context_.trans_version_range_ = version_range;
  context_.read_out_type_ = FLAT_ROW_STORE;
  context_.is_inited_ = true;
  store_ctx_.trans_table_guard_->set_trans_state_table(&test_trans_part_ctx_);
}

int TestMicroBlockRowScanner::build_macro_and_scan(const int64_t micro_cnt, const char* micro_data[],
    ObMultiVersionMicroBlockMinorMergeRowScanner& m_scanner, ObMockIterator& scanner_iter)
{
  int ret = OB_SUCCESS;
  bool is_left_border = false;
  bool is_right_border = false;
  ObMockIterator micro_iter;
  ObStoreRowkey end_key;
  ObMicroBlockData block_data;
  ObMicroBlockData payload_data;
  const ObStoreRow* row = NULL;
  for (int64_t i = 0; i < micro_cnt; ++i) {
    ret = OB_SUCCESS;
    micro_iter.reset();
    if (OB_FAIL(micro_iter.from(micro_data[i]))) {
      STORAGE_LOG(WARN, "failed to get row from micro data", K(i));
    } else {
      build_micro_block_data(micro_iter, block_data, payload_data, end_key);
      if (0 == i) {
        is_left_border = true;
        is_right_border = false;
      } else if (micro_cnt - 1 == i) {
        is_left_border = false;
        is_right_border = true;
      } else {
        is_left_border = false;
        is_right_border = false;
      }
    }
    if (OB_SUCC(ret)) {
      MacroBlockId macro_id(0, 0, 1, ObStoreFileSystem::RESERVED_MACRO_BLOCK_INDEX);
      ObFullMacroBlockMeta full_meta;
      if (OB_FAIL(sstable_.get_meta(macro_id, full_meta))) {
        STORAGE_LOG(WARN, "failed to get meta", K(i));
      } else if (OB_FAIL(m_scanner.open(macro_id, full_meta, payload_data, is_left_border, is_right_border))) {
        STORAGE_LOG(WARN, "failed to open scanner", K(i));
      }
    }
    while (OB_SUCCESS == ret) {
      ret = m_scanner.get_next_row(row);
      if (OB_SUCCESS == ret) {
        scanner_iter.add_row(const_cast<ObStoreRow*>(row));
        STORAGE_LOG(WARN, "test", "this row", to_cstring(*row));
      } else if (OB_ITER_END != ret) {
        STORAGE_LOG(ERROR, "error", K(ret), KPC(row));
      }
    }
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  return ret;
}

TEST_F(TestMicroBlockRowScanner, test_minor_merge_sparse)
{
  GCONF._enable_sparse_row = true;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 3;
  const char* micro_data[micro_cnt];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0       2        2       EXIST   CF\n"
                  "1        var1  -2       0       2        NOP     EXIST   L\n"
                  "2        var2  -7       0       4        3       EXIST   CLF\n"
                  "3        var3  -8       0       7        2       EXIST   CF\n"
                  "3        var3  -5       0       7        1       EXIST   N\n"
                  "3        var3  -3       0       6        NOP     EXIST   N\n"
                  "3        var3  -2       0       5        NOP     EXIST   L\n"
                  "3        var4  -3       0       3        4       DELETE  CLF\n"
                  "4        var4  -9       0       7        NOP     EXIST   CF\n"
                  "4        var4  -7       0       6        5       EXIST   C\n"
                  "4        var4  -3       0       7        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -9       0       10       11      EXIST   CF\n"
                  "5        var5  -6       0       6        NOP     EXIST   N\n"
                  "5        var5  -4       0       7        4       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0       5        2       EXIST   N\n"
                  "5        var5  -2       0       5        1       EXIST   C\n"
                  "5        var5  -1       0       4        3       EXIST   L\n";

  prepare_data(micro_data, 3, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObVersionRange trans_version_range;
  ObMockIterator micro_iter;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  uint16_t result_col_id[] = {16, 17, 7, 8, 20, 21};

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8       0       2        2       EXIST   CF\n"
                        "1        var1  -2       0       2        NOP     EXIST   CL\n"
                        "2        var2  -7       0       4        3       EXIST   CLF\n"
                        "3        var3  -8       0       7        2       EXIST   CF\n"
                        "3        var3  -5       0       7        1       EXIST   CL\n"  // 5
                        "3        var4  -3       0       3        4       DELETE  CLF\n"
                        "4        var4  -9       0       7        NOP     EXIST   CF\n"
                        "4        var4  -7       0       6        5       EXIST   CL\n"
                        "5        var5  -9       0       10       11      EXIST   CF\n"
                        "5        var5  -6       0       6        4       EXIST   CL\n";  // 10

  range.set_whole_range();
  context_.read_out_type_ = SPARSE_ROW_STORE;
  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));
  int ret = OB_SUCCESS;
  const ObStoreRow* row = NULL;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1, '\\', result_col_id));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8       0       2        2       EXIST   CLF\n"
                        "2        var2  -7       0       4        3       EXIST   CLF\n"
                        "3        var3  -8       0       7        2       EXIST   CLF\n"
                        "3        var4  -3       0       3        4       DELETE  CLF\n"
                        "4        var4  -9       0       7        NOP     EXIST   CLF\n"
                        "5        var5  -9       0       10       11      EXIST   CLF\n";

  range.set_whole_range();
  context_.read_out_type_ = SPARSE_ROW_STORE;
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));
  ret = OB_SUCCESS;
  row = NULL;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2, '\\', result_col_id));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 0;
  prepare_query_param(trans_version_range, true, false);

  const char* result3 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8       0       2        2       EXIST   CF\n"
                        "1        var1  -2       0       2        NOP     EXIST   L\n"
                        "2        var2  -7       0       4        3       EXIST   CLF\n"
                        "3        var3  -8       0       7        2       EXIST   CF\n"
                        "3        var3  -5       0       7        1       EXIST   N\n"
                        "3        var3  -3       0       6        NOP     EXIST   N\n"
                        "3        var3  -2       0       5        NOP     EXIST   L\n"
                        "3        var4  -3       0       3        4       DELETE  CLF\n"
                        "4        var4  -9       0       7        NOP     EXIST   CF\n"
                        "4        var4  -7       0       6        5       EXIST   C\n"
                        "4        var4  -3       0       7        NOP     EXIST   L\n"
                        "5        var5  -9       0       10       11      EXIST   CF\n"
                        "5        var5  -6       0       6        NOP     EXIST   N\n"
                        "5        var5  -4       0       7        4       EXIST   N\n"
                        "5        var5  -3       0       5        2       EXIST   N\n"
                        "5        var5  -2       0       5        1       EXIST   C\n"
                        "5        var5  -1       0       4        3       EXIST   L\n";

  range.set_whole_range();
  context_.read_out_type_ = SPARSE_ROW_STORE;
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));
  ret = OB_SUCCESS;
  row = NULL;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3, '\\', result_col_id));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}
/*
TEST_F(TestMicroBlockRowScanner, single_micro)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8       0      NOP      2       EXIST   CF\n"
      "1        var1  -2       0      2        NOP     EXIST   L\n"
      "2        var2  -7       0      4        3       EXIST   CLF\n"
      "3        var3  -8       0      7        2       EXIST   CF\n"
      "3        var3  -5       0      7        1       EXIST   N\n"
      "3        var3  -3       0      6        NOP     EXIST   N\n"
      "3        var3  -2       0      5        NOP     EXIST   L\n"
      "3        var4  -3       0      3        4       DELETE  L\n"
      "4        var4  -9       0      7        NOP     EXIST   CF\n"
      "4        var4  -7       0      6        5       EXIST   C\n"
      "4        var4  -3       0      7        NOP     EXIST   L\n"
      "5        var5  -9       0      10       11      EXIST   CF\n"
      "5        var5  -6       0      6        NOP     EXIST   N\n"
      "5        var5  -4       0      7        4       EXIST   N\n"
      "5        var5  -3       0      5        2       EXIST   N\n"
      "5        var5  -2       0      5        1       EXIST   C\n"
      "5        var5  -1       0      4        3       EXIST   L\n";
  prepare_data(micro_data, 1, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockRowScanner m_scanner;
  ObMultiVersionMicroBlockRowScanner batch_scanner;
  ObVersionRange trans_version_range;
  ObMockIterator micro_iter;
  ObMockIterator res_iter;
  ObStoreRowkey end_key;
  ObMicroBlockData block_data;
  ObMicroBlockData payload_data;
  ObStoreRange range;

  // multi version forward scan
  trans_version_range.base_version_ = 2;
  trans_version_range.snapshot_version_ = 8;
  trans_version_range.multi_version_start_ = 2;
  prepare_query_param(trans_version_range, false, false);
  const char *result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  NOP      2       EXIST   CL\n"
      "2        var2  4        3       EXIST   CL\n"
      "3        var3  7        2       EXIST   C\n"
      "3        var4  3        4       DELETE  L\n"
      "4        var4  6        5       EXIST   CL\n"
      "5        var5  6        4       EXIST   CL\n";
  micro_iter.reset();
  res_iter.reset();
  OK(micro_iter.from(micro_data[0]));
  build_micro_block_data(micro_iter, block_data, payload_data, end_key);
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));
  OK(batch_scanner.init(param_, context_, &sstable_));
  OK(batch_scanner.set_range(range));

  MacroBlockId macro_id(0, 0, 1, ObStoreFileSystem::RESERVED_MACRO_BLOCK_INDEX);
  ObMacroBlockMetaHandle meta_handle;
  OK(ObMacroBlockMetaMgr::get_instance().get_meta(macro_id, meta_handle));
  const ObMacroBlockMeta *macro_meta = meta_handle.get_meta();
  OK(m_scanner.open(macro_id, *macro_meta, payload_data, true, true));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(m_scanner));

  OK(batch_scanner.open(macro_id, *macro_meta, payload_data, true, true));
  const ObStoreRow *batch_rows = NULL;
  int64_t batch_count = 0;
  ObMockIterator batch_iter;
  OK(batch_scanner.get_next_rows(batch_rows, batch_count));
  ASSERT_EQ(batch_count, res_iter.count());
  for (int64_t i = 0; i < batch_count; ++i) {
    OK(batch_iter.add_row(const_cast<ObStoreRow *>(batch_rows + i)));
  }
  res_iter.setup_start_cursor();
  ASSERT_TRUE(res_iter.equals(batch_iter));


  // multi version reverse scan
  m_scanner.reset();
  batch_scanner.reset();

  trans_version_range.base_version_ = 2;
  trans_version_range.snapshot_version_ = 8;
  trans_version_range.multi_version_start_ = 2;
  prepare_query_param(trans_version_range, false, true);
  const char *result2 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  6        4       EXIST   CL\n"
      "4        var4  6        5       EXIST   CL\n"
      "3        var4  3        4       DELETE  L\n"
      "3        var3  7        2       EXIST   C\n"
      "2        var2  4        3       EXIST   CL\n"
      "1        var1  NOP      2       EXIST   CL\n";
  micro_iter.reset();
  res_iter.reset();
  OK(micro_iter.from(micro_data[0]));
  build_micro_block_data(micro_iter, block_data, payload_data, end_key);
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));
  OK(m_scanner.open(macro_id, *macro_meta, payload_data, true, true));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(m_scanner));


  OK(batch_scanner.init(param_, context_, &sstable_));
  OK(batch_scanner.set_range(range));
  OK(batch_scanner.open(macro_id, *macro_meta, payload_data, true, true));
  batch_iter.reset();
  OK(batch_scanner.get_next_rows(batch_rows, batch_count));
  ASSERT_EQ(batch_count, res_iter.count());
  for (int64_t i = 0; i < batch_count; ++i) {
    OK(batch_iter.add_row(const_cast<ObStoreRow *>(batch_rows + i)));
  }
  res_iter.setup_start_cursor();
  ASSERT_TRUE(res_iter.equals(batch_iter));
}

TEST_F(TestMicroBlockRowScanner, multi_micro)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 7;
  const char *micro_data[micro_cnt];

  micro_data[0] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -8      0       NOP      2       EXIST   CF\n";
  micro_data[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -2       0      2        NOP     EXIST   L\n"
      "2        var2  -10      0      4        3       EXIST   CLF\n"
      "3        var3  -10      0      7        2       EXIST   C\n"
      "3        var3  -8       0      7        NOP     EXIST   N\n";
  micro_data[2] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "3        var3  -5       0      1        NOP     EXIST   N\n";
  micro_data[3] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "3        var3  -3       0      6        NOP     EXIST   L\n"
      "4        var4  -9       0      7        NOP     EXIST   CF\n";
  micro_data[4] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "4        var4  -7       0      6        5       EXIST   C\n"
      "4        var4  -3       0      5        NOP     EXIST   L\n"
      "4        var5  -4       0      NOP      NOP     DELETE  CF\n";

  micro_data[5] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "4        var5  -3       0      5        6       EXIST   L\n"
      "5        var5  -10      0      10       9       EXIST   CF\n"
      "5        var5  -6       0      6        NOP     EXIST   N\n"
      "5        var5  -4       0      7        NOP     EXIST   N\n";

  micro_data[6] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -3       0      5        2       EXIST   N\n"
      "5        var5  -2       0      5        1       EXIST   C\n"
      "5        var5  -1       0      4        3       EXIST   L\n";
  prepare_data(micro_data, 7, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockRowScanner m_scanner;
  ObMultiVersionMicroBlockRowScanner batch_scanner;
  ObVersionRange trans_version_range;
  ObMockIterator micro_iter;
  ObMockIterator res_iter;
  ObMockIterator scanner_iter;
  ObMockIterator batch_iter;
  ObStoreRowkey end_key;
  ObMicroBlockData block_data;
  ObMicroBlockData payload_data;
  ObStoreRange range;

  // multi version forward scan
  trans_version_range.base_version_ = 2;
  trans_version_range.snapshot_version_ = 8;
  trans_version_range.multi_version_start_ = 2;
  prepare_query_param(trans_version_range, false, false);
  const char *result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  NOP      2       EXIST   CL\n"
      "3        var3  7        NOP     EXIST   C\n"
      "4        var4  6        5       EXIST   CL\n"
      "4        var5  5        NOP     DELETE  CL\n"
      "5        var5  6        2       EXIST   CL\n";
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));
  OK(batch_scanner.init(param_, context_, &sstable_));
  OK(batch_scanner.set_range(range));
  int ret = OB_SUCCESS;
  const ObStoreRow *row = NULL;
  const ObStoreRow *batch_rows = NULL;
  int64_t batch_count = 0;
  bool is_left_border = false;
  bool is_right_border = false;
  for (int64_t i = 0; i < micro_cnt; ++i) {
    ret = OB_SUCCESS;
    micro_iter.reset();
    OK(micro_iter.from(micro_data[i]));
    build_micro_block_data(micro_iter, block_data, payload_data, end_key);
    if (0 == i) {
      is_left_border = true;
      is_right_border = false;
    } else if (micro_cnt - 1 == i) {
      is_left_border = false;
      is_right_border = true;
    } else {
      is_left_border = false;
      is_right_border = false;
    }
    MacroBlockId macro_id(0, 0, 1, ObStoreFileSystem::RESERVED_MACRO_BLOCK_INDEX);
    ObMacroBlockMetaHandle meta_handle;
    OK(ObMacroBlockMetaMgr::get_instance().get_meta(macro_id, meta_handle));
    const ObMacroBlockMeta *macro_meta = meta_handle.get_meta();

    OK(m_scanner.open(macro_id, *macro_meta, payload_data, is_left_border, is_right_border)) << "i: " << i;
    while (OB_SUCCESS == ret) {
      ret = m_scanner.get_next_row(row);
      if (OB_SUCCESS == ret) {
        ASSERT_TRUE(NULL != row) << "i: " << i;
        OK(scanner_iter.add_row(const_cast<ObStoreRow *>(row)));
        STORAGE_LOG(WARN, "test", "single row", to_cstring(*row));
      }
    }

    ret = OB_SUCCESS; //reset
    OK(batch_scanner.open(macro_id, *macro_meta, payload_data, is_left_border, is_right_border)) << "i: " << i;
    while (OB_SUCCESS == ret) {
      ret = batch_scanner.get_next_rows(batch_rows, batch_count);
      if (OB_SUCCESS == ret) {
        ASSERT_TRUE(NULL != batch_rows) << "i: " << i;
        ASSERT_TRUE(batch_count > 0);
        for (int64_t j = 0; j < batch_count; ++j) {
          OK(batch_iter.add_row(const_cast<ObStoreRow *>(batch_rows + j)));
          STORAGE_LOG(WARN, "test", "batch row", to_cstring(*(batch_rows + j)));
        }
      } else if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get batching rows", K(ret));
      }
    }
  }
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter));
  res_iter.setup_start_cursor();
  ASSERT_TRUE(res_iter.equals(batch_iter));


  // multi version reverse scan
  m_scanner.reset();
  batch_scanner.reset();
  scanner_iter.reset();
  batch_iter.reset();
  trans_version_range.base_version_ = 2;
  trans_version_range.snapshot_version_ = 8;
  trans_version_range.multi_version_start_ = 2;
  prepare_query_param(trans_version_range, false, true);
  const char *result2 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  6        2       EXIST   CL\n"
      "4        var5  5        NOP     DELETE  CL\n"
      "4        var4  6        5       EXIST   CL\n"
      "3        var3  7        NOP     EXIST   C\n"
      "1        var1  NOP      2       EXIST   CL\n";
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));
  OK(batch_scanner.init(param_, context_, &sstable_));
  OK(batch_scanner.set_range(range));
  for (int64_t i = micro_cnt - 1; i >= 0; --i) {
    ret = OB_SUCCESS;
    micro_iter.reset();
    OK(micro_iter.from(micro_data[i]));
    build_micro_block_data(micro_iter, block_data, payload_data, end_key);
    if (0 == i) {
      is_left_border = true;
      is_right_border = false;
    } else if (micro_cnt - 1 == i) {
      is_left_border = false;
      is_right_border = true;
    } else {
      is_left_border = false;
      is_right_border = false;
    }
    MacroBlockId macro_id(0, 0, 1, ObStoreFileSystem::RESERVED_MACRO_BLOCK_INDEX);
    ObMacroBlockMetaHandle meta_handle;
    OK(ObMacroBlockMetaMgr::get_instance().get_meta(macro_id, meta_handle));
    const ObMacroBlockMeta *macro_meta = meta_handle.get_meta();
    OK(m_scanner.open(macro_id, *macro_meta, payload_data, is_left_border, is_right_border)) << "i: " << i;
    while (OB_SUCCESS == ret) {
      ret = m_scanner.get_next_row(row);
      if (OB_SUCCESS == ret) {
        ASSERT_TRUE(NULL != row) << "i: " << i;
        OK(scanner_iter.add_row(const_cast<ObStoreRow *>(row)));
        STORAGE_LOG(WARN, "test", "single row", to_cstring(*row));
      }
    }

    ret = OB_SUCCESS;
    OK(batch_scanner.open(macro_id, *macro_meta, payload_data, is_left_border, is_right_border)) << "i: " << i;
    while (OB_SUCCESS == ret) {
      ret = batch_scanner.get_next_rows(batch_rows, batch_count);
      if (OB_SUCCESS == ret) {
        ASSERT_TRUE(NULL != batch_rows) << "i: " << i;
        ASSERT_TRUE(batch_count > 0);
        for (int64_t j = 0; j < batch_count; ++j) {
          OK(batch_iter.add_row(const_cast<ObStoreRow *>(batch_rows + j)));
          STORAGE_LOG(WARN, "test", "batch row", to_cstring(*(batch_rows + j)));
        }
      }
    }
  }
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter));
  res_iter.setup_start_cursor();
  ASSERT_TRUE(res_iter.equals(batch_iter));
}

*/
TEST_F(TestMicroBlockRowScanner, test_minor_merge_sparse2)
{
  GCONF._enable_sparse_row = true;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 5;
  const char* micro_data[micro_cnt];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0       NOP      2       EXIST   F\n"
                  "1        var1  -2       0       2        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2  -7       0       4        3       EXIST   CLF\n"
                  "3        var3  -8       0       7        2       EXIST   CF\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -5       0       NOP      1       EXIST   N\n"
                  "3        var3  -3       0       7        NOP     EXIST   L\n"
                  "4        var4  -9       0       7        NOP     EXIST   CF\n"
                  "4        var4  -7       0       6        5       EXIST   C\n";

  micro_data[3] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "4        var4  -3       0       7        NOP     EXIST   L\n"
                  "5        var5  -6       0       6        4       EXIST   CF\n"
                  "5        var5  -4       0       7        4       EXIST   N\n";

  micro_data[4] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0       5        2       EXIST   N\n"
                  "5        var5  -2       0       5        1       EXIST   C\n"
                  "5        var5  -1       0       4        3       EXIST   L\n"
                  "6        var6  -1       0       4        3       EXIST   CLF\n";
  prepare_data(micro_data, micro_cnt, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
  ObVersionRange trans_version_range;
  ObMockIterator micro_iter;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;

  // minor
  trans_version_range.base_version_ = 1;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8       0       2        2       EXIST   CF\n"
                        "1        var1  -2       0       2        NOP     EXIST   CL\n"
                        "2        var2  -7       0       4        3       EXIST   CLF\n"
                        "3        var3  -8       0       7        2       EXIST   CF\n"
                        "3        var3  -5       0       1        7       EXIST   CL\n"
                        "4        var4  -9       0       7        NOP     EXIST   CF\n"
                        "4        var4  -7       0       6        5       EXIST   CL\n"
                        "5        var5  -6       0       6        4       EXIST   CLF\n"
                        "6        var6  -1       0       4        3       EXIST   CLF\n";

  uint16_t result_col_id[] = {16,
      17,
      7,
      8,
      21,
      20,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,
      20,  // 5
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21};
  int64_t result_col_cnt[] = {6, 5, 6, 6, 6, 5, 6, 6, 6};
  range.set_whole_range();
  context_.read_out_type_ = SPARSE_ROW_STORE;
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1, '\\', result_col_id, result_col_cnt));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8       0       2        2       EXIST   CLF\n"
                        "2        var2  -7       0       4        3       EXIST   CLF\n"
                        "3        var3  -8       0       7        2       EXIST   CLF\n"
                        "4        var4  -9       0       7        NOP     EXIST   CLF\n"
                        "5        var5  -6       0       6        4       EXIST   CLF\n"
                        "6        var6  -1       0       4        3       EXIST   CLF\n";

  uint16_t result_col_id2[] = {16,
      17,
      7,
      8,
      21,
      20,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,  // 5
      16,
      17,
      7,
      8,
      20,
      21};
  int64_t result_col_cnt2[] = {6, 6, 6, 5, 6, 6};
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = INT64_MAX - 10;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  range.set_whole_range();
  context_.read_out_type_ = SPARSE_ROW_STORE;
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));
  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2, '\\', result_col_id2, result_col_cnt2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, minor_for_migration)
{
  GCONF._enable_sparse_row = true;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 3;
  const char* micro_data[micro_cnt];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0       NOP      2       EXIST   F\n"
                  "1        var1  -2       0       2        NOP     EXIST   L\n"
                  "2        var2  -7       0       4        3       EXIST   CLF\n"
                  "3        var3  -8       0       7        2       EXIST   CF\n"
                  "3        var3  -5       0       NOP      1       EXIST   N\n"  // 5
                  "3        var3  -3       0       7        NOP     EXIST   L\n"
                  "4        var4  -9       0       7        NOP     EXIST   F\n"
                  "4        var4  -7       0       6        5       EXIST   C\n"
                  "4        var4  -3       0       7        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6       0       6        2       EXIST   CF\n"  // 10
                  "5        var5  -4       0       7        2       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0       5        2       EXIST   N\n"
                  "5        var5  -2       0       5        1       EXIST   C\n"
                  "5        var5  -1       0       4        3       EXIST   L\n"
                  "6        var6  -10      0       10       10      EXIST   CF\n"
                  "6        var6  -6       0       6        NOP     EXIST   N\n"  // 15
                  "6        var6  -5       0       5        NOP     EXIST   N\n"
                  "6        var6  -4       0       4        NOP     EXIST   L\n"
                  "6        var7  -20      0       20       NOP     EXIST   CF\n"
                  "6        var7  -3       0       3        NOP     EXIST   L\n"
                  "7        var7  -10      0       10       10      EXIST   CF\n"  // 20
                  "7        var7  -6       0       6        NOP     EXIST   N\n"
                  "7        var7  -5       0       5        NOP     EXIST   N\n"
                  "7        var7  -4       0       4        NOP     EXIST   L\n";

  prepare_data(micro_data, micro_cnt, rowkey_cnt, 20, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
  ObVersionRange trans_version_range;
  ObMockIterator micro_iter;
  ObMockIterator res_iter;
  ObStoreRowkey end_key;
  ObMicroBlockData block_data;
  ObMicroBlockData payload_data;
  ObStoreRange range;
  ObMockIterator scanner_iter;

  // minor
  trans_version_range.snapshot_version_ = 8;
  trans_version_range.multi_version_start_ = 4;
  trans_version_range.base_version_ = 4;
  prepare_query_param(trans_version_range, true, false);

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  -8       0       2        2       EXIST   CF\n"
                        "1        var1  -2       0       2        NOP     EXIST   CL\n"
                        "2        var2  -7       0       4        3       EXIST   CLF\n"
                        "3        var3  -8       0       7        2       EXIST   CF\n"
                        "3        var3  -5       0       1        NOP     EXIST   N\n"  // 5
                        "3        var3  -3       0       7        NOP     EXIST   CL\n"
                        "4        var4  -9       0       7        5       EXIST   CF\n"
                        "4        var4  -7       0       6        5       EXIST   C\n"
                        "4        var4  -3       0       7        NOP     EXIST   CL\n"
                        "5        var5  -6       0       6        2       EXIST   CF\n"  // 10
                        "5        var5  -4       0       7        2       EXIST   CL\n"
                        "6        var6  -10      0       10       10      EXIST   CF\n"
                        "6        var6  -6       0       6        NOP     EXIST   N\n"
                        "6        var6  -5       0       5        NOP     EXIST   N\n"
                        "6        var6  -4       0       4        NOP     EXIST   CL\n"  // 15
                        "6        var7  -20      0       20       NOP     EXIST   CF\n"
                        "6        var7  -3       0       3        NOP     EXIST   CL\n"
                        "7        var7  -10      0       10       10      EXIST   CF\n"
                        "7        var7  -6       0       6        NOP     EXIST   N\n"
                        "7        var7  -5       0       5        NOP     EXIST   N\n"
                        "7        var7  -4       0       4        NOP     EXIST   CL\n";

  uint16_t result_col_id[] = {16,
      17,
      7,
      8,
      21,
      20,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      21,  // 5
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,  // 10
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,  // 15
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,
      21,
      16,
      17,
      7,
      8,
      20,
      16,
      17,
      7,
      8,
      20,  // 20
      16,
      17,
      7,
      8,
      20};
  int64_t result_col_cnt[] = {6, 5, 6, 6, 5, 5, 6, 6, 5, 6, 6, 6, 5, 5, 5, 5, 5, 6, 5, 5, 5};
  range.set_whole_range();
  context_.read_out_type_ = SPARSE_ROW_STORE;
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1, '\\', result_col_id, result_col_cnt));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  GCONF._enable_sparse_row = false;
  system("rm -f test_micro_block_row_scanner.log*");
  OB_LOGGER.set_file_name("test_micro_block_row_scanner.log");
  STORAGE_LOG(INFO, "begin unittest: test_micro_block_row_scanner");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
