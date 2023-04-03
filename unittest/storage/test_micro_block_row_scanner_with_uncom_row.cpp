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
#include "share/ob_local_device.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace unittest
{

class TestMicroBlockRowScanner : public ObMultiVersionSSTableTest
{
public:
  TestMicroBlockRowScanner() : ObMultiVersionSSTableTest("testmicroblockrowscanner_with_uncom_row") {}
  virtual ~TestMicroBlockRowScanner() {}

  virtual void SetUp()
  {
    ObMultiVersionSSTableTest::SetUp();
    store_ctx_.trans_table_guard_ = new ObTransStateTableGuard();
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

  void prepare_query_param(
      const ObVersionRange &version_range,
      const bool is_minor_merge,
      const bool is_reverse_scan);

  int build_macro_and_scan(
      const int64_t micro_cnt,
      const char *micro_data[],
      ObMultiVersionMicroBlockMinorMergeRowScanner &m_scanner,
      ObMockIterator &scanner_iter);

  ObArray<ObColDesc> columns_;
  ObArray<share::schema::ObColumnParam *> out_cols_param_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObArenaAllocator allocator_;
  ObArray<int32_t> projector_;
  ObStoreCtx store_ctx_;
  TestUncommittedMinorMergeScan test_trans_part_ctx_;
};

void TestMicroBlockRowScanner::prepare_query_param(
    const ObVersionRange &version_range,
    const bool is_minor_merge,
    const bool is_reverse_scan)
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
  query_flag.iter_uncommitted_row_ = true;

  ObColDesc col_desc;
  int multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int schema_rowkey_cnt = rowkey_cnt_ - multi_version_col_cnt; // schema rowkey count
  int trans_version_col = ObMultiVersionRowkeyHelpper::get_trans_version_col_store_index(
      schema_rowkey_cnt, multi_version_col_cnt);
  int sql_no_col = ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
      schema_rowkey_cnt, multi_version_col_cnt);
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
  param_.rowkey_cnt_ = schema_rowkey_cnt; // schema rowkey count
  param_.out_cols_ = &columns_;
  //jsut for test
  param_.out_cols_param_ = &out_cols_param_;
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

int TestMicroBlockRowScanner::build_macro_and_scan(
    const int64_t micro_cnt,
    const char *micro_data[],
    ObMultiVersionMicroBlockMinorMergeRowScanner &m_scanner,
    ObMockIterator &scanner_iter)
{
  int ret = OB_SUCCESS;
  bool is_left_border = false;
  bool is_right_border = false;
  ObMockIterator micro_iter;
  ObStoreRowkey end_key;
  ObMicroBlockData block_data;
  ObMicroBlockData payload_data;
  const ObStoreRow *row = NULL;
  for (int64_t i = 0; i < micro_cnt; ++i) {
    ret = OB_SUCCESS;
    micro_iter.reset();
    if(OB_FAIL(micro_iter.from(micro_data[i]))) {
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
      MacroBlockId macro_id(ObLocalDevice::RESERVED_BLOCK_INDEX, 0);
      ObFullMacroBlockMeta full_meta;
      if(OB_FAIL(sstable_.get_meta(macro_id, full_meta))) {
        STORAGE_LOG(WARN, "failed to get meta", K(i));
      } else if(OB_FAIL(m_scanner.open(macro_id, full_meta, payload_data, is_left_border, is_right_border))) {
        STORAGE_LOG(WARN, "failed to open scanner", K(i));
      }
    }
    while (OB_SUCCESS == ret) {
      ret = m_scanner.get_next_row(row);
      if (OB_SUCCESS == ret) {
        scanner_iter.add_row(const_cast<ObStoreRow *>(row));
        STORAGE_LOG(WARN, "test", "this row", to_cstring(*row));
      } else if (OB_ITER_END == ret) {
        STORAGE_LOG(ERROR, "error", K(ret), KPC(row));
      }
    }
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  return ret;
}

TEST_F(TestMicroBlockRowScanner, uncommitted_row_commit)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 3;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN      0      NOP      8       EXIST   U\n"
      "1        var1   MIN      0      2        NOP     EXIST   U\n"
      "1        var1   MIN      0      4        3       EXIST   U\n"
      "1        var1   MIN      0      7        6       EXIST   U\n"
      "1        var1   -3       0      9        5       EXIST   CF\n"
      "1        var1   -2       0      7        2       EXIST   L\n"
      "2        var2   -7       0      4        3       EXIST   CLF\n"
      "2        var3   -1       0      4        3       EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5   MIN     0       10       NOP     EXIST   U\n"
      "5        var5  -6       0       6        4       EXIST   CF\n"
      "5        var5  -4       0       7        4       EXIST   N\n";

  micro_data[2] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5  -3       0       5        2       EXIST   N\n"
      "5        var5  -2       0       5        1       EXIST   N\n"
      "5        var5  -1       0       4        3       EXIST   L\n";

  prepare_data(micro_data, 3, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;

  // make all trans commit
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::COMMIT, 12))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -12      0       2        8       EXIST   CF\n"
      "1        var1  -3       0       9        5       EXIST   CL\n"
      "2        var2  -7       0       4        3       EXIST   CLF\n"
      "2        var3  -1       0       4        3       EXIST   CLF\n"
      "5        var5  -12      0       10       4       EXIST   CF\n"
      "5        var5  -6       0       6        4       EXIST   CL\n";
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  const ObStoreRow *row = NULL;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // make all trans commit
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::COMMIT, 12))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -12      0       2        8       EXIST   CLF\n"
      "2        var2  -7       0       4        3       EXIST   CLF\n"
      "2        var3  -1       0       4        3       EXIST   CLF\n"
      "5        var5  -12      0       10       4       EXIST   CLF\n";

  range.set_whole_range();

  OK(m_scanner.set_range(range));

  row = NULL;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // make all trans commit
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::COMMIT, 12))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }
  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 0;
  prepare_query_param(trans_version_range, true, false);

  const char *result3 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -12      0       2        8       EXIST   CF\n"
      "1        var1   -3       0       9        5       EXIST   C\n"
      "1        var1   -2       0       7        2       EXIST   L\n"
      "2        var2   -7       0       4        3       EXIST   CLF\n"
      "2        var3   -1       0       4        3       EXIST   CLF\n"
      "5        var5   -12      0       10       4       EXIST   CF\n"
      "5        var5   -6       0       6        4       EXIST   C\n"
      "5        var5   -4       0       7        4       EXIST   N\n"
      "5        var5   -3       0       5        2       EXIST   N\n"
      "5        var5   -2       0       5        1       EXIST   N\n"
      "5        var5   -1       0       4        3       EXIST   L\n";

  range.set_whole_range();
  OK(m_scanner.set_range(range));
  ret = OB_SUCCESS;
  row = NULL;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, test_row_not_in_range)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 3;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN      -8     NOP      8       EXIST   U\n"
      "1        var1   MIN      -6     2        NOP     EXIST   U\n"
      "1        var1   MIN      -3     4        3       EXIST   U\n"
      "1        var1   MIN      -1     7        6       EXIST   U\n"
      "1        var1   -3       0      7        5       EXIST   CF\n"
      "1        var1   -2       0      7        2       EXIST   L\n"
      "2        var2   -7       0      4        3       EXIST   CLF\n"
      "2        var3   -1       0      4        3       EXIST   CLF\n";

  micro_data[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5   MIN     -2      10       11      EXIST   U\n"
      "5        var5  -6       0       6        4       EXIST   CF\n"
      "5        var5  -4       0       7        4       EXIST   L\n";

  micro_data[2] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "6        var6  -6       0       5        2       EXIST   CF\n"
      "6        var6  -2       0       5        1       EXIST   N\n"
      "6        var6  -1       0       4        3       EXIST   N\n"; // did't have Last Row

  prepare_data(micro_data, 3, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;

  // make all trans commit
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::RUNNING, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }
  // minor
  trans_version_range.base_version_ = 4;
  trans_version_range.snapshot_version_ = 8;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN      -8     NOP      8       EXIST   U\n"
      "1        var1   MIN      -6     2        NOP     EXIST   U\n"
      "1        var1   MIN      -3     4        3       EXIST   U\n"
      "1        var1   MIN      -1     7        6       EXIST   U\n"
      "1        var1   -3       0      7        5       EXIST   CLF\n"
      "2        var2   -7       0      4        3       EXIST   CLF\n"
      "2        var3   -1       0      4        3       EXIST   CLF\n"
      "5        var5   MIN      -2     10       11      EXIST   U\n"
      "5        var5   -6       0      6        4       EXIST   CLF\n"
      "6        var6   -6       0      5        2       EXIST   CF\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 7;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1  -12      -8      2        8       EXIST   CLF\n"
      "2        var2   -7       0      4        3       EXIST   CLF\n"
      "2        var3   -1       0      4        3       EXIST   CLF\n"
      "5        var5  -12      -2      10       11      EXIST   CLF\n"
      "6        var6   -6       0      5        2       EXIST   CF\n";

  range.set_whole_range();

  OK(m_scanner.set_range(range));
  // make all trans commit
  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::COMMIT, 12))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, test_committed_row_across_macro_block)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 3;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN      -28     NOP      18      EXIST   U\n"
      "1        var1   MIN      -26     NOP      NOP     EXIST   U\n"
      "1        var1   MIN      -23     NOP      3       EXIST   U\n"
      "1        var1   MIN      -21     NOP      6       EXIST   U\n";

  micro_data[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN      -19     NOP      9       EXIST   U\n"
      "1        var1   MIN      -18     NOP      NOP     EXIST   U\n"
      "1        var1   MIN      -15     5        3       EXIST   U\n"
      "1        var1   MIN      -13     7        16      EXIST   U\n";

  micro_data[2] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN      -8     2        8       EXIST   U\n"
      "1        var1   MIN      -6     1        NOP     EXIST   U\n"
      "1        var1   MIN      -3     4        13      EXIST   U\n"
      "1        var1   MIN      -1     17       7       EXIST   U\n"; // no Last row

  prepare_data(micro_data, 3, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;

  // make all trans commit
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::COMMIT, 29))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  // minor
  trans_version_range.base_version_ = 4;
  trans_version_range.snapshot_version_ = 38;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -29      -28    5        18      EXIST   N\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // make all trans commit
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  // minor
  trans_version_range.base_version_ = 4;
  trans_version_range.snapshot_version_ = 38;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n";

  range.set_whole_range();
  OK(m_scanner.set_range(range));

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, test_committed_row_across_macro_block2)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 2;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN      -28     NOP      18      EXIST   U\n"
      "1        var1   MIN      -26     NOP      NOP     EXIST   U\n"
      "1        var1   MIN      -23     NOP      3       EXIST   U\n"
      "1        var1   MIN      -21     NOP      6       EXIST   U\n";

  micro_data[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN      -19     NOP      9       EXIST   U\n"
      "1        var1   MIN      -18     NOP      NOP     EXIST   U\n";

  prepare_data(micro_data, 2, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;

  // make all trans commit
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::COMMIT, 29))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  // minor
  trans_version_range.base_version_ = 4;
  trans_version_range.snapshot_version_ = 8;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -29      -28    NOP     18      EXIST   N\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  const ObStoreRow *row = NULL;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}


TEST_F(TestMicroBlockRowScanner, all_uncommitted_row)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 3;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN      0      NOP      8       EXIST   U\n"
      "1        var1   MIN      0      2        NOP     EXIST   U\n"
      "1        var1   MIN      0      4        3       EXIST   U\n"
      "1        var1   -3       0      82       6       EXIST   CF\n";

  micro_data[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -1      0       82       8       EXIST   L\n"
      "2        var2   MIN     0       10       11      EXIST   LU\n"
      "3        var3   MIN     0       6        NOP     EXIST   U\n"
      "3        var3   -6      0       7        4       EXIST   CLF\n";

  micro_data[2] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5   MIN      0       5        2       EXIST   U\n"
      "5        var5   MIN      0       5        1       EXIST   U\n"
      "5        var5   -9       0       4        3       EXIST   CLF\n";

  prepare_data(micro_data, 3, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;

  // make all trans running
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::RUNNING))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN      0      NOP      8       EXIST   U\n"
      "1        var1   MIN      0      2        NOP     EXIST   U\n"
      "1        var1   MIN      0      4        3       EXIST   U\n"
      "1        var1   -3       0      82       6       EXIST   CLF\n"
      "2        var2   MIN      0      10       11      EXIST   LU\n"
      "3        var3   MIN      0      6        NOP     EXIST   U\n"
      "3        var3   -6       0      7        4       EXIST   CLF\n"
      "5        var5   MIN      0      5        2       EXIST   U\n"
      "5        var5   MIN      0      5        1       EXIST   U\n"
      "5        var5   -9       0      4        3       EXIST   CLF\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  const ObStoreRow *row = NULL;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();


  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 0;
  prepare_query_param(trans_version_range, true, false);

  const char *result3 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN      0      NOP      8       EXIST   U\n"
      "1        var1   MIN      0      2        NOP     EXIST   U\n"
      "1        var1   MIN      0      4        3       EXIST   U\n"
      "1        var1   -3       0      82       6       EXIST   CF\n"
      "1        var1   -1       0      82       8       EXIST   L\n"
      "2        var2   MIN      0      10       11      EXIST   LU\n"
      "3        var3   MIN      0      6        NOP     EXIST   U\n"
      "3        var3   -6       0      7        4       EXIST   CLF\n"
      "5        var5   MIN      0      5        2       EXIST   U\n"
      "5        var5   MIN      0      5        1       EXIST   U\n"
      "5        var5   -9       0      4        3       EXIST   CLF\n";

  range.set_whole_range();
  OK(m_scanner.set_range(range));

  ret = OB_SUCCESS;
  row = NULL;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

// sql sequence = 38 || 48 will set to be rollback
TEST_F(TestMicroBlockRowScanner, all_uncommitted_row_with_rollback_sql_sequence)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 3;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN    -48      NOP      8       EXIST   U\n"
      "1        var1   MIN    -40      2        NOP     EXIST   U\n"
      "1        var1   MIN    -38      4        3       EXIST   U\n"
      "1        var1   -3       0      NOP      6       EXIST   CF\n";

  micro_data[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -1      0       82       8       EXIST   L\n"
      "2        var2   MIN     -48     10       11      EXIST   LU\n"
      "3        var3   MIN     -38     6        NOP     EXIST   U\n"
      "3        var3   -6      0       7        4       EXIST   CLF\n";

  micro_data[2] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "5        var5   MIN     -38     5        2       EXIST   U\n"
      "5        var5   MIN     -8      5        NOP     EXIST   U\n"
      "5        var5   -9      0       4        3       EXIST   CLF\n";

  prepare_data(micro_data, 3, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;

  // make all trans running
  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::RUNNING))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN      -40    2        NOP     EXIST   U\n"
      "1        var1   -3       0      NOP      6       EXIST   CLF\n"
      "2        var2   MAGIC    MAGIC  NOP      NOP     EXIST   LM\n"
      "3        var3   -6       0      7        4       EXIST   CLF\n"
      "5        var5   MIN      -8     5        NOP     EXIST   U\n"
      "5        var5   -9       0      4        3       EXIST   CLF\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  const ObStoreRow *row = NULL;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();


  STORAGE_LOG(ERROR, "result two", K(ret));
  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 0;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -19      -40    2        6       EXIST   CF\n"
      "1        var1   -3       0      NOP      6       EXIST   C\n"
      "1        var1   -1       0      82       8       EXIST   L\n"
      "2        var2   MAGIC    MAGIC  NOP      NOP     EXIST   LM\n"
      "3        var3   -6       0      7        4       EXIST   CLF\n"
      "5        var5   -19      -8     5        3       EXIST   CF\n"
      "5        var5   -9       0      4        3       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.set_range(range));
  // make all trans running

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::COMMIT, 19))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ret = OB_SUCCESS;
  row = NULL;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, uncommitted_row_in_multi_micro_block)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 3;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -81      NOP      8       EXIST   U\n"
      "1        var1   MIN     -71      2        NOP     EXIST   U\n"
      "1        var1   MIN     -21      4        3       EXIST   U\n"
      "1        var1   MIN     -11      NOP      6       EXIST   U\n";

  micro_data[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -9      82       8       EXIST   U\n"
      "1        var1   MIN     -8      10       11      EXIST   U\n"
      "1        var1   MIN     -1      6        NOP     EXIST   U\n"
      "1        var1   -19     0       7        4       EXIST   CF\n";

  micro_data[2] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -17      0       5        2       EXIST   N\n"
      "1        var1   -8       0       5        1       EXIST   N\n"
      "1        var1   -4       0       4        3       EXIST   L\n";

  prepare_data(micro_data, 3, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;


  // make all trans running
  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::RUNNING))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -81     NOP      8       EXIST   U\n"
      "1        var1   MIN     -71     2        NOP     EXIST   U\n"
      "1        var1   MIN     -21     4        3       EXIST   U\n"
      "1        var1   MIN     -11     NOP      6       EXIST   U\n"
      "1        var1   MIN     -9      82       8       EXIST   U\n"
      "1        var1   MIN     -8      10       11      EXIST   U\n"
      "1        var1   MIN     -1      6        NOP     EXIST   U\n"
      "1        var1   -19     0       7        4       EXIST   CF\n"
      "1        var1   -17     0       5        2       EXIST   N\n"
      "1        var1   -8      0       5        1       EXIST   N\n"
      "1        var1   -4      0       4        3       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  const ObStoreRow *row = NULL;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -37     -81     2        8       EXIST   CF\n"
      "1        var1   -19     0       7        4       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.set_range(range));

  // make all trans commit
  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::COMMIT, 37))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 40;
  prepare_query_param(trans_version_range, true, false);

  const char *result3 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -37     -81     2        8       EXIST   CLF\n";

  range.set_whole_range();
  OK(m_scanner.set_range(range));

  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

// sql sequence 38 will be rollback
TEST_F(TestMicroBlockRowScanner, uncommitted_row_in_multi_micro_block2)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 4;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -39     NOP      8       EXIST   U\n";

  micro_data[1] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -38     2        NOP     EXIST   U\n"
      "1        var1   MIN     -19     4        3       EXIST   U\n"
      "1        var1   MIN     -17     NOP      6       EXIST   U\n"
      "1        var1   MIN     -9      82       8       EXIST   U\n"
      "1        var1   MIN     -7      10       11      EXIST   U\n"
      "1        var1   MIN     -1      6        NOP     EXIST   U\n";

  micro_data[2] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -19     0       7        4       EXIST   CF\n";

  micro_data[3] =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -17     0       5        2       EXIST   N\n"
      "1        var1   -8      0       5        1       EXIST   N\n"
      "1        var1   -4      0       4        3       EXIST   L\n";

  prepare_data(micro_data, 4, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;


  // make all trans running
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::RUNNING))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -39     NOP      8       EXIST   U\n"
      "1        var1   MIN     -19     4        3       EXIST   U\n"
      "1        var1   MIN     -17     NOP      6       EXIST   U\n"
      "1        var1   MIN     -9      82       8       EXIST   U\n"
      "1        var1   MIN     -7      10       11      EXIST   U\n"
      "1        var1   MIN     -1      6        NOP     EXIST   U\n"
      "1        var1   -19     0       7        4       EXIST   CF\n"
      "1        var1   -17     0       5        2       EXIST   N\n"
      "1        var1   -8      0       5        1       EXIST   N\n"
      "1        var1   -4      0       4        3       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));


  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -37     -39     4        8       EXIST   CF\n"
      "1        var1   -19     0       7        4       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  // make all trans commit
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::COMMIT, 37))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 40;
  prepare_query_param(trans_version_range, true, false);

  const char *result3 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -37    -39       4        8       EXIST   CLF\n";

  range.set_whole_range();
  OK(m_scanner.set_range(range));

  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, uncommitted_row_in_single_micro_block)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 1;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -28     NOP      8       EXIST   U\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U\n"
      "1        var1   MIN     -18     4        3       EXIST   LU\n"
      "1        var2   MIN     -14     NOP      6       EXIST   U\n"
      "1        var2   MIN     -8      82       8       EXIST   U\n"
      "1        var2   MIN     -5      10       11      EXIST   U\n"
      "1        var2   MIN     -3      6        NOP     EXIST   U\n"
      "1        var2   -19     0       7        4       EXIST   CLF\n"
      "7        var3   MIN     -2      NOP      2       EXIST   U\n"
      "7        var3   -8      0       5        1       EXIST   CF\n"
      "7        var3   -4      0       4        3       EXIST   L\n";

  prepare_data(micro_data, 1, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;


  // make all trans running
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::RUNNING))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -28     NOP      8       EXIST   U\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U\n"
      "1        var1   MIN     -18     4        3       EXIST   LU\n"
      "1        var2   MIN     -14     NOP      6       EXIST   U\n"
      "1        var2   MIN     -8      82       8       EXIST   U\n"
      "1        var2   MIN     -5      10       11      EXIST   U\n"
      "1        var2   MIN     -3      6        NOP     EXIST   U\n"
      "1        var2   -19     0       7        4       EXIST   CLF\n"
      "7        var3   MIN     -2      NOP      2       EXIST   U\n"
      "7        var3   -8      0       5        1       EXIST   CF\n"
      "7        var3   -4      0       4        3       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -37     -28     99       8       EXIST   CL\n"
      "1        var2   -37     -14     82       6       EXIST   CF\n"
      "1        var2   -19     0       7        4       EXIST   CL\n"
      "7        var3   -37     -2      5        2       EXIST   CF\n"
      "7        var3   -8      0       5        1       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.set_range(range));


  // make all trans commit
  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::COMMIT, 37))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 40;
  prepare_query_param(trans_version_range, true, false);

  const char *result3 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -37     -28     99       8       EXIST   CL\n"
      "1        var2   -37     -14     82       6       EXIST   CLF\n"
      "7        var3   -37     -2      5        2       EXIST   CLF\n";

  range.set_whole_range();
  OK(m_scanner.set_range(range));

  ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, abort_row_in_single_micro_block)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 1;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -28     NOP      8       EXIST   U\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U\n"
      "1        var1   MIN     -18     4        3       EXIST   LU\n"
      "2        var2   MIN     -14     NOP      6       EXIST   U\n"
      "2        var2   MIN     -8      82       8       EXIST   U\n"
      "2        var2   MIN     -5      10       11      EXIST   U\n"
      "2        var2   MIN     -3      6        NOP     EXIST   U\n"
      "2        var2   -19     0       7        4       EXIST   CLF\n"
      "7        var3   MIN     -2      NOP      2       EXIST   U\n"
      "7        var3   -8      0       5        1       EXIST   CF\n"
      "7        var3   -4      0       4        3       EXIST   L\n";

  prepare_data(micro_data, 1, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;


  // make all trans abort
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::ABORT))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n"
      "2        var2   -19     0       7        4       EXIST   CLF\n"
      "7        var3   -8      0       5        1       EXIST   CF\n"
      "7        var3   -4      0       4        3       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, abort_row_in_multi_micro_block)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 5;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -28     NOP      8       EXIST   U\n";
  micro_data[1] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U\n"
      "1        var1   MIN     -18     4        3       EXIST   LU\n"
      "1        var2   MIN     -14     NOP      6       EXIST   U\n"
      "1        var2   MIN     -8      82       8       EXIST   U\n";
  micro_data[2] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var2   MIN     -5      10       11      EXIST   U\n"
      "1        var2   MIN     -3      6        NOP     EXIST   U\n";
  micro_data[3] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var2   -19     0       7        4       EXIST   CLF\n"
      "7        var3   MIN     -2      NOP      2       EXIST   U\n"
      "7        var3   -8      0       5        1       EXIST   CF\n";
  micro_data[4] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "7        var3   -4      0       4        3       EXIST   L\n";

  prepare_data(micro_data, 5, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;


  // make all trans abort
  if (OB_FAIL(test_trans_part_ctx_.set_all_transaction_status(
      transaction::ObTransTableStatusType::ABORT))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n"
      "1        var2   -19     0       7        4       EXIST   CLF\n"
      "7        var3   -8      0       5        1       EXIST   CF\n"
      "7        var3   -4      0       4        3       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, mix_trans_row_in_single_micro_block)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 1;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN     -28     NOP      8       EXIST   U   trans_id_1\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U   trans_id_1\n"
      "1        var1   MIN     -18     4        3       EXIST   LU  trans_id_1\n" // first trans
      "1        var2   MIN     -14     NOP      6       EXIST   U   trans_id_2\n"
      "1        var2   MIN     -8      82       8       EXIST   U   trans_id_2\n"
      "1        var2   MIN     -5      10       11      EXIST   U   trans_id_2\n"
      "1        var2   MIN     -3      6        NOP     EXIST   U   trans_id_2\n" // second trans
      "1        var2   -19     0       7        4       EXIST   CLF trans_id_0\n"
      "7        var3   MIN     -2      NOP      2       EXIST   U   trans_id_3\n" // third trans
      "7        var3   -8      0       5        1       EXIST   CF  trans_id_0\n"
      "7        var3   -4      0       4        3       EXIST   L   trans_id_0\n";

  prepare_data(micro_data, 1, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -28     NOP      8       EXIST   U\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U\n"
      "1        var1   MIN     -18     4        3       EXIST   LU\n" // first trans
      "1        var2   -19     0       7        4       EXIST   CLF\n"
      "7        var3   -8      0       5        1       EXIST   CF\n"
      "7        var3   -4      0       4        3       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  transaction::ObTransTableStatusType status1[] = {
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT
  };
  int64_t commit_trans_version1[] = {INT64_MAX, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -28     NOP      8       EXIST   U\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U\n"
      "1        var1   MIN     -18     4        3       EXIST   LU\n" // first trans
      "1        var2   -29     -14     82       6       EXIST   CF\n" // second trans
      "1        var2   -19     0       7        4       EXIST   CL\n"
      "7        var3   -8      0       5        1       EXIST   CLF\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT
  };
  int64_t commit_trans_version2[] = {INT64_MAX, 29, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
    STORAGE_LOG(DEBUG, "add transaction status success", K(ret), K(i));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, mix_trans_row_in_single_micro_block2)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 1;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag   trans_id\n"
      "1        var1   MIN     -28     NOP      8       EXIST   U   trans_id_1\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U   trans_id_1\n"
      "1        var1   MIN     -18     4        3       EXIST   LU  trans_id_1\n" // first trans
      "1        var2   MIN     -14     NOP      6       EXIST   U   trans_id_2\n"
      "1        var2   MIN     -8      82       8       EXIST   U   trans_id_2\n"
      "1        var2   MIN     -5      10       11      EXIST   U   trans_id_2\n"
      "1        var2   MIN     -3      6        NOP     EXIST   U   trans_id_2\n" // second trans
      "1        var2   -19     0       7        4       EXIST   CLF trans_id_0\n"
      "7        var3   MIN     -2      NOP      2       EXIST   U   trans_id_3\n" // third trans
      "7        var3   -8      0       5        1       EXIST   CF  trans_id_0\n"
      "7        var3   -4      0       4        3       EXIST   L   trans_id_0\n";

  prepare_data(micro_data, 1, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -21     -28     99       8       EXIST   CL\n"// first trans
      "1        var2   -29     -14     82       6       EXIST   CF\n" // second trans
      "1        var2   -19     0       7        4       EXIST   CL\n"
      "7        var3   MIN     -2      NOP      2       EXIST   U\n" // third trans
      "7        var3   -8      0       5        1       EXIST   CF\n"
      "7        var3   -4      0       4        3       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));


  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING
  };
  int64_t commit_trans_version1[] = {21, 29, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n"
      "1        var2   -19     0       7        4       EXIST   CLF\n" // second trans
      "7        var3   MIN     -2      NOP      2       EXIST   U\n" // third trans
      "7        var3   -8      0       5        1       EXIST   CLF\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::RUNNING
  };
  int64_t commit_trans_version2[] = {INT64_MAX, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, mix_trans_row_in_multi_micro_block)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 5;
  const char *micro_data[micro_cnt];
  micro_data[0] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN     -28     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[1] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U   trans_id_1\n"
      "1        var1   MIN     -18     4        3       EXIST   LU  trans_id_1\n" // first trans
      "2        var2   MIN     -14     NOP      6       EXIST   U   trans_id_2\n";
  micro_data[2] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "2        var2   MIN     -8      82       8       EXIST   U   trans_id_2\n"
      "2        var2   MIN     -5      10       11      EXIST   U   trans_id_2\n"
      "2        var2   MIN     -3      6        NOP     EXIST   U   trans_id_2\n" // second trans
      "2        var2   -19     0       7        4       EXIST   CLF trans_id_0\n";
  micro_data[3] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "7        var3   MIN     -2      NOP      2       EXIST   U   trans_id_3\n" // third trans
      "7        var3   -8      0       5        1       EXIST   CF  trans_id_0\n";
  micro_data[4] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "7        var3   -4      0       4        3       EXIST   L   trans_id_0\n";

  prepare_data(micro_data, 5, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -82     -28     99       3       EXIST   CL\n"
      "2        var2   MIN     -14     NOP      6       EXIST   U\n"
      "2        var2   MIN     -8      82       8       EXIST   U\n"
      "2        var2   MIN     -5      10       11      EXIST   U\n"
      "2        var2   MIN     -3      6        NOP     EXIST   U\n" // second trans
      "2        var2   -19     0       7        4       EXIST   CLF\n"
      "7        var3   -22     -2      5        2       EXIST   CF\n" // third trans
      "7        var3   -8      0       5        1       EXIST   C\n"
      "7        var3   -4      0       4        3       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT
  };
  int64_t commit_trans_version1[] = {82, INT64_MAX, 22};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -82     -28     99       3       EXIST   CL\n" // first trans
      "2        var2   MIN     -14     NOP      6       EXIST   U\n"
      "2        var2   MIN     -8      82       8       EXIST   U\n"
      "2        var2   MIN     -5      10       11      EXIST   U\n"
      "2        var2   MIN     -3      6        NOP     EXIST   U\n" // second trans
      "2        var2   -19     0       7        4       EXIST   CLF\n"
      "7        var3   -22     -2      5        2       EXIST   CF\n" // third trans
      "7        var3   -8      0       5        1       EXIST   CL\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT
  };
  int64_t commit_trans_version2[] = {82, INT64_MAX, 22};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result3 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n"
      "2        var2   -42     -14     82       6       EXIST   CF\n"
      "2        var2   -19     0       7        4       EXIST   CL\n" // second trans
      "7        var3   MIN     -2      NOP      2       EXIST   U\n" // third trans
      "7        var3   -8      0       5        1       EXIST   CLF\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status3[] = {
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING
  };
  int64_t commit_trans_version3[] = {INT64_MAX, 42, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status3[i], commit_trans_version3[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
    STORAGE_LOG(INFO, "add transaction status success", K(ret), K(i));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(micro_cnt, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, mix_trans_row_in_multi_micro_block2)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 10;
  const char *micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN     -28     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN     -18     4        3       EXIST   LU  trans_id_1\n" // first trans
      "1        var2   MIN     -14     NOP      6       EXIST   U   trans_id_2\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var2   MIN     -8      82       8       EXIST   U   trans_id_2\n"
      "1        var2   MIN     -5      10       11      EXIST   U   trans_id_2\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var2   MIN     -3      6        NOP     EXIST   U   trans_id_2\n" // second trans
      "1        var2   -19     0       7        4       EXIST   CLF trans_id_0\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "7        var3   MIN     -2      NOP      2       EXIST   U   trans_id_3\n"; // third trans
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "7        var3   -8      0       5        1       EXIST   CF  trans_id_0\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "7        var3   -4      0       4        3       EXIST   L   trans_id_0\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator res_iter;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -82     -28     99       3       EXIST   CL\n"
      "1        var2   MIN     -14     NOP      6       EXIST   U\n"
      "1        var2   MIN     -8      82       8       EXIST   U\n"
      "1        var2   MIN     -5      10       11      EXIST   U\n"
      "1        var2   MIN     -3      6        NOP     EXIST   U\n" // second trans
      "1        var2   -19     0       7        4       EXIST   CLF\n"
      "7        var3   -22     -2      5        2       EXIST   CF\n" // third trans
      "7        var3   -8      0       5        1       EXIST   C\n"
      "7        var3   -4      0       4        3       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT
  };
  int64_t commit_trans_version1[] = {82, INT64_MAX, 22};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -82     -28     99       3       EXIST   CL\n" // first trans
      "1        var2   MIN     -14     NOP      6       EXIST   U\n"
      "1        var2   MIN     -8      82       8       EXIST   U\n"
      "1        var2   MIN     -5      10       11      EXIST   U\n"
      "1        var2   MIN     -3      6        NOP     EXIST   U\n" // second trans
      "1        var2   -19     0       7        4       EXIST   CLF\n"
      "7        var3   -22     -2      5        2       EXIST   CF\n" // third trans
      "7        var3   -8      0       5        1       EXIST   CL\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT
  };
  int64_t commit_trans_version2[] = {82, INT64_MAX, 22};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result3 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n"
      "1        var2   -42     -14     82       6       EXIST   CF\n"
      "1        var2   -19     0       7        4       EXIST   CL\n" // second trans
      "7        var3   MIN     -2      NOP      2       EXIST   U\n" // third trans
      "7        var3   -8      0       5        1       EXIST   CLF\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status3[] = {
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING
  };
  int64_t commit_trans_version3[] = {INT64_MAX, 42, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status3[i], commit_trans_version3[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, mix_trans_row_in_multi_micro_block3)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char *micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN     -28     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN     -18     4        NOP     EXIST   LU  trans_id_1\n" // first trans
      "2        var2   MIN     -14     NOP      6       EXIST   U   trans_id_2\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "2        var2   MIN     -8      82       8       EXIST   U   trans_id_2\n"
      "2        var2   MIN     -5      10       11      EXIST   U   trans_id_2\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "2        var2   MIN     -3      6        NOP     EXIST   U   trans_id_2\n" // second trans
      "2        var2   -19     0       7        4       EXIST   CLF trans_id_0\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "3        var3   MIN     -2      NOP      2       EXIST   U   trans_id_3\n"; // third trans
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "3        var3   -8      0       5        1       EXIST   CLF trans_id_0\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "4        var4   MIN     -8      NOP      9       EXIST   U  trans_id_4\n";  // forth trans

  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "4        var4   MIN     -2      NOP      NOP     EXIST   U  trans_id_4\n"
      "4        var4   -12     0       4        3       EXIST   CF trans_id_0\n"
      "4        var4   -8      0       NOP      9       EXIST   N  trans_id_0\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "4        var4   -6      0       NOP      NOP     EXIST   L  trans_id_0\n"
      "5        var5   MIN     -9      14       NOP     EXIST   U  trans_id_5\n";  // fifth trans
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "5        var5   MIN     -1      NOP      7       EXIST   LU  trans_id_5\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator micro_iter;
  ObMockIterator res_iter;
  ObStoreRowkey end_key;
  ObMicroBlockData block_data;
  ObMicroBlockData payload_data;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -82     -28     99       NOP     EXIST   CL\n" // first trans
      "2        var2   MIN     -14     NOP      6       EXIST   U\n"
      "2        var2   MIN     -8      82       8       EXIST   U\n"
      "2        var2   MIN     -5      10       11      EXIST   U\n"
      "2        var2   MIN     -3      6        NOP     EXIST   U\n" // second trans
      "2        var2   -19     0       7        4       EXIST   CLF\n"
      "3        var3   -22     -2      5        2       EXIST   CF\n" // third trans
      "3        var3   -8      0       5        1       EXIST   CL\n"
      "4        var4   -12     0       4        3       EXIST   CF\n" // forth trans
      "4        var4   -8      0       NOP      9       EXIST   N\n"
      "4        var4   -6      0       NOP      NOP     EXIST   CL\n"
      "5        var5   -19     -9      14       7       EXIST   CL\n";  // fifth trans

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT
  };
  int64_t commit_trans_version1[] = {82, INT64_MAX, 22, INT64_MAX, 19};
  for (int i = 0; OB_SUCC(ret) && i < 5; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -28     NOP      NOP     EXIST   U\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U\n"
      "1        var1   MIN     -18     4        NOP     EXIST   LU\n" // first trans
      "2        var2   -28     -14     82       6       EXIST   CF\n"// second trans
      "2        var2   -19     0       7        4       EXIST   CL\n"
      "3        var3   -8      0       5        1       EXIST   CLF\n" // third trans
      "4        var4   MIN     -8      NOP      9       EXIST   U\n"  // forth trans
      "4        var4   MIN     -2      NOP      NOP     EXIST   U\n"
      "4        var4   -12     0       4        3       EXIST   CLF\n"
      "5        var5   -71     -9      14       7       EXIST   CL\n";  // fifth trans

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
  };
  int64_t commit_trans_version2[] = {INT64_MAX, 28, INT64_MAX, INT64_MAX, 71};
  for (int i = 0; OB_SUCC(ret) && i < 5; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result3 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n"
      "2        var2   -21     -14     82       6       EXIST   CF\n"// second trans
      "2        var2   -19     0       7        4       EXIST   CL\n"
      "3        var3   MIN     -2      NOP      2       EXIST   U\n" // third trans
      "3        var3   -8      0       5        1       EXIST   CLF\n"
      "4        var4   -19     -8      4        9       EXIST   CLF\n"  // forth trans
      "5        var5   -20     -9      14       7       EXIST   CL\n";  // fifth trans

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status3[] = {
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
  };
  int64_t commit_trans_version3[] = {INT64_MAX, 21, INT64_MAX, 19, 20};
  for (int i = 0; OB_SUCC(ret) && i < 5; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status3[i], commit_trans_version3[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, mix_trans_row_without_last_row)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char *micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN     -28     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "1        var1   MIN     -18     4        NOP     EXIST   LU  trans_id_1\n" // first trans
      "2        var2   MIN     -14     NOP      6       EXIST   U   trans_id_2\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "2        var2   MIN     -8      82       8       EXIST   U   trans_id_2\n"
      "2        var2   MIN     -5      10       11      EXIST   U   trans_id_2\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "2        var2   MIN     -3      6        NOP     EXIST   U   trans_id_2\n" // second trans
      "2        var2   -19     0       7        4       EXIST   CLF trans_id_0\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "3        var3   MIN     -2      NOP      2       EXIST   U   trans_id_3\n"; // third trans
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "3        var3   -8      0       5        1       EXIST   CLF trans_id_0\n";
  micro_data[index++] =
      "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
      "4        var4   MIN     -38      NOP      9       EXIST   U  trans_id_4\n";  // forth trans

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner(allocator_);
  ObVersionRange trans_version_range;
  ObMockIterator micro_iter;
  ObMockIterator res_iter;
  ObStoreRowkey end_key;
  ObMicroBlockData block_data;
  ObMicroBlockData payload_data;
  ObStoreRange range;
  ObMockIterator scanner_iter;
  int ret = OB_SUCCESS;

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 7;
  prepare_query_param(trans_version_range, true, false);

  const char *result1 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   -82     -28     99       NOP     EXIST   CL\n" // first trans
      "2        var2   MIN     -14     NOP      6       EXIST   U\n"
      "2        var2   MIN     -8      82       8       EXIST   U\n"
      "2        var2   MIN     -5      10       11      EXIST   U\n"
      "2        var2   MIN     -3      6        NOP     EXIST   U\n" // second trans
      "2        var2   -19     0       7        4       EXIST   CLF\n"
      "3        var3   -22     -2      5        2       EXIST   CF\n" // third trans
      "3        var3   -8      0       5        1       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT
  };
  int64_t commit_trans_version1[] = {82, INT64_MAX, 22, 19};
  for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -28     NOP      NOP     EXIST   U\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U\n"
      "1        var1   MIN     -18     4        NOP     EXIST   LU\n" // first trans
      "2        var2   -28     -14     82       6       EXIST   CF\n"// second trans
      "2        var2   -19     0       7        4       EXIST   CL\n"
      "3        var3   -8      0       5        1       EXIST   CLF\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::RUNNING
  };
  int64_t commit_trans_version2[] = {INT64_MAX, 28, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char *result3 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n"
      "2        var2   -21     -14     82       6       EXIST   CF\n"// second trans
      "2        var2   -19     0       7        4       EXIST   CL\n"
      "3        var3   MIN     -2      NOP      2       EXIST   U\n" // third trans
      "3        var3   -8      0       5        1       EXIST   CLF\n";  // fifth trans

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status3[] = {
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::ABORT
  };
  int64_t commit_trans_version3[] = {INT64_MAX, 21, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 4; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
        status3[i], commit_trans_version3[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reuse();
  scanner_iter.reset();
}

}
}

int main(int argc, char **argv)
{
  GCONF._enable_sparse_row = false;
  system("rm -f test_micro_block_row_scanner_with_uncom_row.log*");
  OB_LOGGER.set_file_name("test_micro_block_row_scanner_with_uncom_row.log");
  STORAGE_LOG(INFO, "begin unittest: test_micro_block_row_scanner_with_uncom_row");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
