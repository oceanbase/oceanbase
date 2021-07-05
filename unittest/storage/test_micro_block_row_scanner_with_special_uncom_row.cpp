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
  TestMicroBlockRowScanner() : ObMultiVersionSSTableTest("testmicroblockrowscanner_with_special_uncom_row")
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
  int build_macro_and_scan(const int64_t micro_cnt, const char* micro_data[],
      ObMultiVersionMicroBlockMinorMergeRowScanner& m_scanner, ObMockIterator& scanner_iter);

  ObArray<ObColDesc> columns_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObArenaAllocator allocator_;
  ObArray<int32_t> projector_;
  memtable::ObMemtableCtxFactory f_;
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
  query_flag.iter_uncommitted_row_ = true;

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

  store_ctx_.mem_ctx_ = f_.alloc();
  store_ctx_.mem_ctx_->trans_begin();
  store_ctx_.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());

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
      } else if (OB_ITER_END == ret) {
        STORAGE_LOG(ERROR, "error", K(ret), KPC(row));
      }
    }
  }
  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  return ret;
}

TEST_F(TestMicroBlockRowScanner, multi_uncom_trans_in_multi_micro_block_1_commit)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MIN     -28     NOP      NOP     EXIST   U  trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MIN     -22     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MIN     -18     4        NOP     EXIST   LU  trans_id_1\n"   // first trans
                        "2        var2   MIN     -14     NOP      6       EXIST   U  trans_id_2\n"    // second trans
                        "2        var2   -19     -21     NOP      8       EXIST   U   trans_id_3\n";  // third trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -19     -18     67       NOP     EXIST   U   trans_id_3\n"
                        "2        var2   -19     -14     18       6       EXIST   LU  trans_id_3\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -82     -28     99       NOP     EXIST   CL\n"  // first trans
                        "2        var2   -32     -14     67       6       EXIST   C\n"
                        "2        var2   -19     -21     67       8       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT};
  int64_t commit_trans_version1[] = {82, 32, 19};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   MIN     -28     NOP      NOP     EXIST   U\n"
                        "1        var1   MIN     -22     99       NOP     EXIST   U\n"
                        "1        var1   MIN     -18     4        NOP     EXIST   LU\n"
                        "2        var2   -28     -14     NOP      6       EXIST   CL\n";  // first trans

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT};
  int64_t commit_trans_version2[] = {INT64_MAX, 28, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, multi_uncom_trans_in_multi_micro_block_1_abort)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MIN     -28     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MIN     -22     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MIN     -18     4        NOP     EXIST   LU  trans_id_1\n"  // first trans
                        "2        var2   MIN     -14     NOP      6       EXIST   U   trans_id_2\n"  // second trans
                        "2        var2   -19     -21     NOP      8       EXIST   U  trans_id_3\n";  // third trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -19     -18     67       NOP     EXIST   U   trans_id_3\n"
                        "2        var2   -19     -14     18       6       EXIST   LU  trans_id_3\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n"
                        "2        var2   -19     -21     67       8       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT};

  int64_t commit_trans_version1[] = {INT64_MAX, INT64_MAX, 19};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   MIN     -28     NOP      NOP     EXIST   U\n"
                        "1        var1   MIN     -22     99       NOP     EXIST   U\n"
                        "1        var1   MIN     -18     4        NOP     EXIST   LU\n"  // first trans
                        "2        var2   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT};
  int64_t commit_trans_version2[] = {INT64_MAX, 28, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, multi_uncom_trans_in_multi_micro_block_1_running)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MIN     -28     NOP      NOP     EXIST   U  trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MIN     -22     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MIN     -18     4        NOP     EXIST   LU  trans_id_1\n"   // first trans
                        "2        var2   MIN     -14     NOP      6       EXIST   U  trans_id_2\n"    // second trans
                        "2        var2   -19     -21     NOP      8       EXIST   U   trans_id_3\n";  // third trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -19     -18     67       NOP     EXIST   U   trans_id_3\n"
                        "2        var2   -19     -14     18       6       EXIST   LU  trans_id_3\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -82     -28     99       NOP     EXIST   CL\n"  // first trans
                        "2        var2   MIN     -14     NOP      6       EXIST   U\n"
                        "2        var2   -19     -21     67       8       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT};
  int64_t commit_trans_version1[] = {82, INT64_MAX, 19};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 =
      "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
      "1        var1   MIN     -28     NOP      NOP     EXIST   U\n"
      "1        var1   MIN     -22     99       NOP     EXIST   U\n"
      "1        var1   MIN     -18     4        NOP     EXIST   LU\n"   // first trans
      "2        var2   MIN     -14     NOP      6       EXIST   U\n"    // second trans
      "2        var2   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n";  // third trans(ONLY Last row is output)

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::ABORT};
  int64_t commit_trans_version2[] = {INT64_MAX, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, 3_uncom_trans_in_multi_micro_block_commit)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -28     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -22     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -18     4        NOP     EXIST   U   trans_id_1\n"  // first trans
                        "2        var2   MIN     -14     NOP      10      EXIST   U   trans_id_2\n"  // second trans
                        "2        var2   MIN     -1      18       NOP     EXIST   U   trans_id_2\n"
                        "2        var2   -19     -21     NOP      8       EXIST   U   trans_id_3\n";  // third trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -19     -18     67       NOP     EXIST   U   trans_id_3\n"
                        "2        var2   -19     -14     18       6       EXIST   LU  trans_id_3\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -82     -28     99       10      EXIST   C\n"  // first trans
                        "2        var2   -37     -14     18       10      EXIST   C\n"
                        "2        var2   -19     -21     67       8       EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT};
  int64_t commit_trans_version1[] = {82, 37, 19};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   MIN     -28     NOP      NOP     EXIST   U\n"
                        "2        var2   MIN     -22     99       NOP     EXIST   U\n"
                        "2        var2   MIN     -18     4        NOP     EXIST   U\n"  // first trans
                        "2        var2   -37     -14     18       10      EXIST   C\n"  // second trans
                        "2        var2   -19     -21     67       8       EXIST   CL\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT};
  int64_t commit_trans_version2[] = {INT64_MAX, 37, 19};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, 3_uncom_trans_in_multi_micro_block_abort)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -28     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -22     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -18     4        NOP     EXIST   U   trans_id_1\n"  // first trans
                        "2        var2   MIN     -14     NOP      10      EXIST   U   trans_id_2\n"  // second trans
                        "2        var2   MIN     -1      18       NOP     EXIST   U   trans_id_2\n"
                        "2        var2   -19     -21     NOP      8       EXIST   U   trans_id_3\n";  // third trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -19     -18     67       NOP     EXIST   U   trans_id_3\n"
                        "2        var2   -19     -14     18       6       EXIST   LU  trans_id_3\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -82     -28     99     NOP     EXIST   CL\n";  // first trans

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT};
  int64_t commit_trans_version1[] = {82, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   MIN     -28     NOP      NOP     EXIST   U\n"
                        "2        var2   MIN     -22     99       NOP     EXIST   U\n"
                        "2        var2   MIN     -18     4        NOP     EXIST   U\n"  // first trans
                        "2        var2   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT};
  int64_t commit_trans_version2[] = {INT64_MAX, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  const char* result3 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status3[] = {transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT};
  int64_t commit_trans_version3[] = {INT64_MAX, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status3[i], commit_trans_version3[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, multi_uncom_trans_in_multi_micro_block_abort)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MIN     -50     NOP      21      EXIST   U   trans_id_1\n"
                        "1        var1   MIN     -43     NOP      18      EXIST   U   trans_id_1\n"
                        "1        var1   MIN     -29     NOP      20      EXIST   U   trans_id_1\n";  // first trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   -82     -24     1        NOP     EXIST   U   trans_id_2\n"  // second trans
                        "1        var1   -82     -10     NOP      29      EXIST   LU  trans_id_2\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -28     NOP      NOP     EXIST   U   trans_id_3\n";  // third trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -22     99       NOP     EXIST   U   trans_id_3\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -18     4        NOP     EXIST   U   trans_id_3\n"
                        "2        var2   MIN     -14     NOP      10      EXIST   U   trans_id_4\n"  // forth trans
                        "2        var2   MIN     -1      18       NOP     EXIST   U   trans_id_4\n"
                        "2        var2   -19     -21     NOP      8       EXIST   LU   trans_id_5\n";  // fith trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "3        var3   MIN     -18     37       NOP     EXIST   U   trans_id_6\n"
                        "3        var3   MIN     -14     18       NOP     EXIST   U   trans_id_6\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "3        var3   MIN     -8      7        NOP     EXIST   U   trans_id_6\n"
                        "3        var3   -10     -14     18       NOP     EXIST   U   trans_id_7\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "3        var3   -10     -13     67       NOP     EXIST   U   trans_id_7\n"
                        "3        var3   -10     -9      18       72      EXIST   LU  trans_id_7\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -82     -24     1       29     EXIST   CL\n"
                        "2        var2   -19     -21     NOP     8      EXIST   L\n"
                        "3        var3   -10     -14     18      72     EXIST   CL\n";  // first trans

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT,  // 5
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT,
  };
  int64_t commit_trans_version1[] = {INT64_MAX, 82, INT64_MAX, INT64_MAX, 19, INT64_MAX, 10};
  for (int i = 0; OB_SUCC(ret) && i < 7; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   MAGIC   MAGIC   NOP     NOP     EXIST   LM\n"
                        "2        var2   MAGIC   MAGIC   NOP     NOP     EXIST   LM\n"
                        "3        var3   MAGIC   MAGIC   NOP     NOP     EXIST   LM\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT};
  int64_t commit_trans_version2[] = {INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 7; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  const char* result3 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   MAGIC   MAGIC   NOP     NOP      EXIST   LM\n"
                        "2        var2   -19     -21     NOP     8        EXIST   L\n"
                        "3        var3   MAGIC   MAGIC   NOP     NOP      EXIST   LM\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status3[] = {
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT,  // 5
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
  };
  int64_t commit_trans_version3[] = {INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX, 19, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 7; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status3[i], commit_trans_version3[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, multi_uncom_trans_in_multi_micro_block_running)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MIN     -50     NOP      21      EXIST   U   trans_id_1\n"
                        "1        var1   MIN     -43     NOP      18      EXIST   U   trans_id_1\n"
                        "1        var1   MIN     -29     NOP      20      EXIST   U   trans_id_1\n";  // first trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   -82     -24     1        NOP     EXIST   U   trans_id_2\n"  // second trans
                        "1        var1   -82     -10     NOP      29      EXIST   LU  trans_id_2\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -28     NOP      NOP     EXIST   U   trans_id_3\n";  // third trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -22     99       NOP     EXIST   U   trans_id_3\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -18     4        NOP     EXIST   U   trans_id_3\n"
                        "2        var2   MIN     -14     NOP      10      EXIST   U   trans_id_4\n"  // forth trans
                        "2        var2   MIN     -1      18       NOP     EXIST   U   trans_id_4\n"
                        "2        var2   -19     -21     NOP      8       EXIST   U   trans_id_5\n";  // fith trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -19     -17     12       NOP     EXIST   LU   trans_id_5\n"
                        "3        var3   MIN     -18     37       NOP     EXIST   U   trans_id_6\n"
                        "3        var3   MIN     -14     18       NOP     EXIST   U   trans_id_6\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "3        var3   MIN     -8      7        NOP     EXIST   U   trans_id_6\n"
                        "3        var3   -10     -14     18       NOP     EXIST   U   trans_id_7\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "3        var3   -10     -13     67       NOP     EXIST   U   trans_id_7\n"
                        "3        var3   -10     -9      18       72      EXIST   LU  trans_id_7\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   MIN     -50     NOP     21     EXIST   U\n"
                        "1        var1   MIN     -43     NOP     18     EXIST   U\n"
                        "1        var1   MIN     -29     NOP     20     EXIST   U\n"
                        "1        var1   -82     -24     1       29     EXIST   CL\n"
                        "2        var2   MIN     -28     NOP     NOP    EXIST   U\n"  // third trans
                        "2        var2   MIN     -22     99      NOP    EXIST   U\n"
                        "2        var2   MIN     -18     4       NOP    EXIST   U\n"
                        "2        var2   -30     -14     18      10     EXIST   C\n"  // forth trans
                        "2        var2   -19     -21     12      8      EXIST   CL\n"
                        "3        var3   MIN     -18     37      NOP    EXIST   U\n"
                        "3        var3   MIN     -14     18      NOP    EXIST   U\n"
                        "3        var3   MIN     -8      7       NOP    EXIST   U\n"
                        "3        var3   -10     -14     18      72     EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,  // 5
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
  };
  int64_t commit_trans_version1[] = {INT64_MAX, 82, INT64_MAX, 30, 19, INT64_MAX, 10};
  for (int i = 0; OB_SUCC(ret) && i < 7; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   MIN     -50     NOP     21     EXIST   U\n"
                        "1        var1   MIN     -43     NOP     18     EXIST   U\n"
                        "1        var1   MIN     -29     NOP     20     EXIST   U\n"
                        "1        var1   -82     -24     1       29     EXIST   CL\n"
                        "2        var2   MIN     -28     NOP     NOP    EXIST   U\n"  // third trans
                        "2        var2   MIN     -22     99      NOP    EXIST   U\n"
                        "2        var2   MIN     -18     4       NOP    EXIST   U\n"
                        "2        var2   MAGIC   MAGIC   NOP     NOP    EXIST   LM\n"
                        "3        var3   MIN     -18     37      NOP    EXIST   U\n"
                        "3        var3   MIN     -14     18      NOP    EXIST   U\n"
                        "3        var3   MIN     -8      7       NOP    EXIST   U\n"
                        "3        var3   -10     -14     18      72     EXIST   CL\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,  // 5
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::COMMIT,
  };
  int64_t commit_trans_version2[] = {INT64_MAX, 82, INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX, 10};
  for (int i = 0; OB_SUCC(ret) && i < 7; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  const char* result3 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   MIN     -50     NOP     21     EXIST   U\n"
                        "1        var1   MIN     -43     NOP     18     EXIST   U\n"
                        "1        var1   MIN     -29     NOP     20     EXIST   U\n"
                        "1        var1   MAGIC   MAGIC   NOP     NOP    EXIST   LM\n"
                        "2        var2   MIN     -28     NOP     NOP    EXIST   U\n"  // third trans
                        "2        var2   MIN     -22     99      NOP    EXIST   U\n"
                        "2        var2   MIN     -18     4       NOP    EXIST   U\n"
                        "2        var2   MAGIC   MAGIC   NOP     NOP    EXIST   LM\n"
                        "3        var3   MIN     -18     37      NOP    EXIST   U\n"
                        "3        var3   MIN     -14     18      NOP    EXIST   U\n"
                        "3        var3   MIN     -8      7       NOP    EXIST   U\n"
                        "3        var3   MAGIC   MAGIC   NOP     NOP    EXIST   LM\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status3[] = {
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,  // 5
      transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::ABORT,
  };
  int64_t commit_trans_version3[] = {INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 7; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status3[i], commit_trans_version3[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, multi_uncom_trans_in_multi_micro_block_commit)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MIN     -50     NOP      21      EXIST   U   trans_id_1\n"
                        "1        var1   MIN     -43     NOP      18      EXIST   U   trans_id_1\n"
                        "1        var1   MIN     -29     NOP      20      EXIST   U   trans_id_1\n";  // first trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   -82     -24     1        NOP     EXIST   U   trans_id_2\n"  // second trans
                        "1        var1   -82     -10     NOP      29      EXIST   LU  trans_id_2\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -28     NOP      NOP     EXIST   U   trans_id_3\n";  // third trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -22     99       NOP     EXIST   U   trans_id_3\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -18     4        NOP     EXIST   U   trans_id_3\n"
                        "2        var2   MIN     -14     NOP      10      EXIST   U   trans_id_4\n"  // forth trans
                        "2        var2   MIN     -1      18       NOP     EXIST   U   trans_id_4\n"
                        "2        var2   -19     -21     NOP      8       EXIST   U   trans_id_5\n";  // fith trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -19     -17     12       NOP     EXIST   LU   trans_id_5\n"
                        "3        var3   MIN     -18     37       NOP     EXIST   U   trans_id_6\n"
                        "3        var3   MIN     -14     18       NOP     EXIST   U   trans_id_6\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "3        var3   MIN     -8      7        NOP     EXIST   U   trans_id_6\n"
                        "3        var3   -10     -14     18       NOP     EXIST   U   trans_id_7\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "3        var3   -10     -13     67       NOP     EXIST   U   trans_id_7\n"
                        "3        var3   -10     -9      18       72      EXIST   LU  trans_id_7\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -108    -50     1       21     EXIST   C\n"
                        "1        var1   -82     -24     1       29     EXIST   CL\n"
                        "2        var2   -32     -28     99      10     EXIST   C\n"  // third trans
                        "2        var2   -30     -14     18      10     EXIST   C\n"  // forth trans
                        "2        var2   -19     -21     12      8      EXIST   CL\n"
                        "3        var3   -25     -18     37      72     EXIST   C\n"
                        "3        var3   -10     -14     18      72     EXIST   CL\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,  // 5
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
  };
  int64_t commit_trans_version1[] = {108, 82, 32, 30, 19, 25, 10};
  for (int i = 0; OB_SUCC(ret) && i < 7; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -108    -50     1       21     EXIST   C\n"
                        "1        var1   -82     -24     1       29     EXIST   CL\n"
                        "2        var2   -32     -28     99      NOP    EXIST   CL\n"  // third trans
                        "3        var3   -25     -18     37      NOP    EXIST   CL\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,  // 5
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT,
  };
  int64_t commit_trans_version2[] = {108, 82, 32, INT64_MAX, INT64_MAX, 25, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 7; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  const char* result3 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   -108    -50     NOP     21     EXIST   CL\n"
                        "2        var2   -32     -28     99      8      EXIST   C\n"  // third trans
                        "2        var2   -19     -21     12      8      EXIST   CL\n"
                        "3        var3   MAGIC   MAGIC   NOP     NOP    EXIST   LM\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status3[] = {
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT,  // 5
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::ABORT,
  };
  int64_t commit_trans_version3[] = {108, INT64_MAX, 32, INT64_MAX, 19, INT64_MAX, INT64_MAX};
  for (int i = 0; OB_SUCC(ret) && i < 7; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status3[i], commit_trans_version3[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, meet_rollback_sql_sequence_with_L)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -81     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -79     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -78     4        NOP     EXIST   U   trans_id_1\n"  // first trans
                        "2        var2   MIN2    -62     NOP      10      EXIST   U   trans_id_2\n"  // second trans
                        "2        var2   MIN2    -58     18       NOP     EXIST   U   trans_id_2\n"
                        "2        var2   -19     -51     NOP      8       EXIST   U   trans_id_3\n";  // third trans
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -19     -50     NOP      15      EXIST   U   trans_id_3\n"
                        "2        var2   -19     -38     18       6       EXIST   LU  trans_id_3\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -82     -81     99     8       EXIST   C\n"  // first trans
                        "2        var2   -19     -51     NOP    8       EXIST   L\n";

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status1[] = {transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT};
  int64_t commit_trans_version1[] = {82, INT64_MAX, 19};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status1[i], commit_trans_version1[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   MIN     -81     NOP      NOP     EXIST   U\n"
                        "2        var2   MIN     -79     99       NOP     EXIST   U\n"
                        "2        var2   MIN     -78     4        NOP     EXIST   U\n"  // first trans
                        "2        var2   -19     -51     NOP      8       EXIST   CL\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status2[] = {transaction::ObTransTableStatusType::RUNNING,
      transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT};
  int64_t commit_trans_version2[] = {INT64_MAX, INT64_MAX, 19};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status2[i], commit_trans_version2[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  const char* result3 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -39   -62     NOP      10      EXIST   C\n"
                        "2        var2   -19   -51     NOP      8       EXIST   L\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  transaction::ObTransTableStatusType status3[] = {transaction::ObTransTableStatusType::ABORT,
      transaction::ObTransTableStatusType::COMMIT,
      transaction::ObTransTableStatusType::COMMIT};
  int64_t commit_trans_version3[] = {INT64_MAX, 39, 19};
  for (int i = 0; OB_SUCC(ret) && i < 3; ++i) {
    if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(status3[i], commit_trans_version3[i]))) {
      STORAGE_LOG(ERROR, "add transaction status failed", K(ret), K(i));
    }
  }
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, meet_rollback_sql_sequence_with_L2)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -81     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -79     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -78     4        NOP     EXIST   U   trans_id_1\n"
                        "2        var2   MIN     -62     19       NOP     EXIST   U   trans_id_1\n"
                        "2        var2   MIN     -58     18       29      EXIST   U   trans_id_1\n"
                        "2        var2   MIN     -51     NOP      8       EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -28     -50     NOP      15      EXIST   U   trans_id_2\n"
                        "2        var2   -28     -38     18       6       EXIST   LU  trans_id_2\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   MIN     -81     NOP      NOP     EXIST   U\n"
                        "2        var2   MIN     -79     99       NOP     EXIST   U\n"
                        "2        var2   MIN     -78     4        NOP     EXIST   U\n"  // first trans
                        "2        var2   MIN     -62     19       NOP     EXIST   U\n"  // second trans
                        "2        var2   MIN     -51     NOP      8       EXIST   U\n"  // third trans
                        "2        var2   -28     -50     NOP      15      EXIST   CL\n";
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::RUNNING, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 28))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -29     -81     99       8     EXIST   C\n"
                        "2        var2   -28     -50     NOP      15    EXIST   L\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 29))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 28))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  const char* result3 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   MAGIC   MAGIC   NOP     NOP    EXIST   LM\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(
                 test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, meet_uncom_row_after_committed_row)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -81     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -79     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -78     4        NOP     EXIST   U   trans_id_1\n"
                        "2        var2   -48     -39     18       29      EXIST   U   trans_id_2\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MAGIC   MAGIC   NOP      NOP     EXIST   LM  trans_id_0\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -48     -39     18      29       EXIST   CL\n";
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 48))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -70     -81     99      NOP    EXIST   CL\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 70))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(
                 test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  const char* result3 = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -81     NOP      NOP     EXIST   U   trans_id_1\n"
                        "2        var2   -70     -79     99       NOP     EXIST   U   trans_id_1\n"
                        "2        var2   -70     -78     4        NOP     EXIST   U   trans_id_1\n"
                        "2        var2   MAGIC   MAGIC   NOP      NOP     EXIST   LM  trans_id_0\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::RUNNING, 70))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(
                 test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, meet_uncom_row_after_committed_row2)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -81     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -79     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -78     4        NOP     EXIST   U   trans_id_1\n"
                        "2        var2   -48     -39     18       29      EXIST   CF  trans_id_0\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MAGIC   MAGIC   NOP      NOP     EXIST   LM  trans_id_0\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -48     -39     18      29       EXIST   CLF\n";
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 28))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -70     -81     99       29    EXIST   CF\n"
                        "2        var2   -48     -39     18       29    EXIST   CL\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 70))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 28))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }
  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  const char* result3 = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -70     -81     NOP      NOP     EXIST   U \n"
                        "2        var2   -70     -79     99       NOP     EXIST   U \n"
                        "2        var2   -70     -78     4        NOP     EXIST   U \n"
                        "2        var2   -48     -39     18       29      EXIST   CLF \n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::RUNNING, 70))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 28))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, meet_uncom_row_after_committed_row3)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -81     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -79     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -78     4        NOP     EXIST   U   trans_id_1\n"
                        "2        var2   -48     -39     18       NOP     EXIST   CF   trans_id_0\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MAGIC   MAGIC   NOP      NOP     EXIST   LM  trans_id_0\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -48     -39     18     NOP       EXIST   CLF\n";
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(
                 test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -70     -81     99       NOP   EXIST   CF\n"
                        "2        var2   -48     -39     18       NOP   EXIST   CL\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 70))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(
                 test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  const char* result3 = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -81     NOP      NOP     EXIST   U   trans_id_1\n"
                        "2        var2   -70     -79     99       NOP     EXIST   U   trans_id_1\n"
                        "2        var2   -70     -78     4        NOP     EXIST   U   trans_id_1\n"
                        "2        var2   -48     -39     18       NOP     EXIST   CLF   trans_id_0\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::RUNNING, 70))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(
                 test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, meet_magic_row_after_committed_row4)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -81     NOP      NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -79     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   -70     -78     4        NOP     EXIST   U   trans_id_1\n"
                        "2        var2   -48     -39     18       NOP     EXIST   CF   trans_id_0\n";
  "2        var2   -48     -21     14       NOP     EXIST   N   trans_id_0\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MAGIC   MAGIC  NOP       NOP     EXIST   LM  trans_id_0\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -48     -39     18      NOP    EXIST   CLF\n";
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 28))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -70     -81     99       NOP    EXIST   CF\n"
                        "2        var2   -48     -39     18       NOP    EXIST   CL\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 70))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 28))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  const char* result3 = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2   -70     -81     NOP      NOP     EXIST   U \n"
                        "2        var2   -70     -79     99       NOP     EXIST   U \n"
                        "2        var2   -70     -78     4        NOP     EXIST   U \n"
                        "2        var2   -48     -39     18       NOP     EXIST   CLF \n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::RUNNING, 70))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 28))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, test_magic_row)
{
  GCONF._enable_sparse_row = false;
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 15;
  const char* micro_data[micro_cnt];
  int index = 0;
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM  trans_id_0\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -79     99       NOP     EXIST   U   trans_id_1\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "2        var2   MIN     -78     4        NOP     EXIST   U   trans_id_1\n"
                        "2        var2   -48     -39     18       NOP     EXIST   CF  trans_id_0\n"
                        "2        var2   MAGIC   MAGIC   NOP      NOP     EXIST   LM  trans_id_0\n";
  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "3        var3   MAGIC   MAGIC   NOP      NOP     EXIST   LM  trans_id_0\n"
                        "4        var4   MIN     -9      19       NOP     EXIST   U   trans_id_2\n"
                        "4        var4   MAGIC   MAGIC   NOP      NOP     EXIST   LM  trans_id_0\n";

  micro_data[index++] = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "5        var5   MIN    -3      NOP       NOP     DELETE  U   trans_id_3\n"
                        "5        var5   MIN    -1      NOP       10      EXIST   U   trans_id_3\n"
                        "5        var5   MAGIC   MAGIC  NOP       NOP     EXIST   LM  trans_id_0\n"
                        "6        var6   MAGIC   MAGIC  NOP       NOP     EXIST   LM  trans_id_0\n"
                        "7        var7   MAGIC   MAGIC  NOP       NOP     EXIST   LM  trans_id_0\n";

  prepare_data(micro_data, index, rowkey_cnt, 9, "none", FLAT_ROW_STORE, 0);

  ObMultiVersionMicroBlockMinorMergeRowScanner m_scanner;
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

  const char* result1 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                        "1        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM  trans_id_0\n"
                        "2        var2   MIN     -79     99       NOP     EXIST   U   trans_id_1\n"
                        "2        var2   MIN     -78     4        NOP     EXIST   U   trans_id_1\n"
                        "2        var2   -48     -39     18       NOP     EXIST   CLF trans_id_0\n"
                        "5        var5   -28     -3      NOP      NOP     DELETE  CL trans_id_0\n";
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::RUNNING, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(
                 test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 28))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 20;
  prepare_query_param(trans_version_range, true, false);

  const char* result2 = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   MAGIC   MAGIC   NOP      NOP    EXIST   LM\n"
                        "2        var2   -70     -79     99       NOP    EXIST   CF\n"
                        "2        var2   -48     -39     18       NOP    EXIST   CL\n"
                        "4        var4   MIN     -9      19       NOP    EXIST   U\n"
                        "4        var4   MAGIC   MAGIC   NOP      NOP    EXIST   LM\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 70))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(
                 transaction::ObTransTableStatusType::RUNNING, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(
                 test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();

  const char* result3 = "bigint   var   bigint  bigint  bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM\n"
                        "2        var2   -48     -39     18       NOP     EXIST   CLF\n";

  ret = OB_SUCCESS;
  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  test_trans_part_ctx_.clear_all();
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(
                 test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  } else if (OB_FAIL(
                 test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::ABORT, INT64_MAX))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  ASSERT_EQ(OB_SUCCESS, build_macro_and_scan(index, micro_data, m_scanner, scanner_iter));
  res_iter.reset();
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, minor_merge_lob_reuse_allocator)
{
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 1;
  const char* micro_data[micro_cnt];
  micro_data[0] = "bigint   var   bigint   bigint  bigint   lob   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -9       -9      1        1     NOP     EXIST   CF\n"
                  "1        var1  -8       -8      2        2     NOP     EXIST   N\n"
                  "1        var1  -7       -7      3        3     NOP     EXIST   N\n"
                  "1        var1  -6       -6      4        4     NOP     EXIST   N\n"
                  "1        var1  -5       -5      5        5     NOP     EXIST   N\n"
                  "1        var1  -4       -4      6        6     NOP     EXIST   N\n"
                  "1        var1  -3       -3      7        7     NOP     EXIST   N\n"
                  "1        var1  -2       -2      8        8     NOP     EXIST   N\n"
                  "1        var1  -1       -1      9        9     NOP     EXIST   L\n";
  prepare_data(micro_data, 1, rowkey_cnt, 9);

  ObMultiVersionMicroBlockRowScanner m_scanner;
  ObVersionRange trans_version_range;
  ObMockIterator micro_iter;
  ObMockIterator res_iter;
  ObStoreRowkey end_key;
  ObMicroBlockData block_data;
  ObMicroBlockData payload_data;
  ObStoreRange range;
  ObMockIterator scanner_iter;

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 5;
  trans_version_range.multi_version_start_ = 1;
  prepare_query_param(trans_version_range, true, false);

  range.set_whole_range();
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));
  int ret = OB_SUCCESS;
  const ObStoreRow* row = NULL;
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
    ObFullMacroBlockMeta full_meta;
    OK(sstable_.get_meta(macro_id, full_meta));
    OK(m_scanner.open(macro_id, full_meta, payload_data, is_left_border, is_right_border)) << "i: " << i;
    const int64_t lob_idx = 3;
    while (OB_SUCCESS == ret) {
      ret = m_scanner.get_next_row(row);
      if (OB_SUCCESS == ret) {
        ASSERT_TRUE(NULL != row) << "i: " << i;
        // OK(scanner_iter.add_row(const_cast<ObStoreRow *>(row)));
        STORAGE_LOG(INFO, "test", "this row", to_cstring(*row));
        ASSERT_EQ(row->row_val_.get_cell(lob_idx - 1).get_int(), 5);
        ObObj obj = row->row_val_.get_cell(lob_idx);
        const ObLobData* lob_data = obj.get_lob_value();
        STORAGE_LOG(INFO, "chaser debug", KPC(lob_data));
        ASSERT_EQ(lob_data->idx_cnt_, 5);
      } else if (OB_ITER_END != ret) {
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }
  }
  // res_iter.reset();
  // OK(res_iter.from(result1));
  // ASSERT_TRUE(res_iter.equals(scanner_iter, true));
  m_scanner.reset();
  // scanner_iter.reset();
}

TEST_F(TestMicroBlockRowScanner, test_bug)
{
  const int64_t rowkey_cnt = 4;
  const int64_t micro_cnt = 1;
  const char* micro_data[micro_cnt];
  micro_data[0] = "bigint   var   bigint   bigint  bigint   bigint  flag    multi_version_row_flag trans_id\n"
                  "0        var1  -1       -1      9        NOP     EXIST   L trans_id_0\n"
                  "1        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM trans_id_0\n"
                  "2        var1   MIN     -9      1        NOP     EXIST   U trans_id_1\n"
                  "2        var1   MIN     -8      NOP      1       EXIST   U trans_id_1\n"
                  "2        var1   MAGIC   MAGIC   NOP      NOP     EXIST   LM trans_id_0\n"
                  "3        var1  -1       -1      9        NOP     EXIST   L trans_id_0\n";
  prepare_data(micro_data, 1, rowkey_cnt, 9);

  ObMultiVersionMicroBlockRowScanner m_scanner;
  ObVersionRange trans_version_range;
  ObMockIterator micro_iter;
  ObMockIterator res_iter;
  ObMicroBlockData block_data;
  ObMicroBlockData payload_data;
  ObStoreRange range;
  ObMockIterator scanner_iter;

  // minor
  trans_version_range.base_version_ = 0;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  prepare_query_param(trans_version_range, true, false);

  const char var1[] = "var1";
  ObObj start_val[2];
  ObObj end_val[2];
  start_val[0].set_int(-1);
  start_val[1].set_varchar(var1, 4);
  start_val[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  end_val[0].set_int(2);
  end_val[1].set_varchar(var1, 4);
  end_val[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ObStoreRowkey start_key(start_val, 2);
  ObStoreRowkey end_key(end_val, 2);
  range.table_id_ = combine_id(1, 3001);
  range.start_key_ = start_key;
  range.end_key_ = end_key;
  range.set_right_closed();

  test_trans_part_ctx_.clear_all();
  int ret = OB_SUCCESS;
  if (OB_FAIL(test_trans_part_ctx_.add_transaction_status(transaction::ObTransTableStatusType::COMMIT, 10))) {
    STORAGE_LOG(ERROR, "add transaction status failed", K(ret));
  }

  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  const char* result1 = "bigint   var    bigint   bigint  flag\n"
                        "0        var1   9        NOP     EXIST\n"
                        "2        var1   1        1       EXIST\n";

  const ObStoreRow* row = NULL;
  bool is_left_border = true;
  bool is_right_border = true;
  for (int64_t i = 0; i < micro_cnt; ++i) {
    ret = OB_SUCCESS;
    micro_iter.reset();
    OK(micro_iter.from(micro_data[i]));
    build_micro_block_data(micro_iter, block_data, payload_data, end_key);
    MacroBlockId macro_id(0, 0, 1, ObStoreFileSystem::RESERVED_MACRO_BLOCK_INDEX);
    ObFullMacroBlockMeta full_meta;
    OK(sstable_.get_meta(macro_id, full_meta));
    const_cast<oceanbase::blocksstable::ObMacroBlockMetaV2*>(full_meta.meta_)->contain_uncommitted_row_ = true;
    OK(m_scanner.open(macro_id, full_meta, payload_data, is_left_border, is_right_border)) << "i: " << i;
    while (OB_SUCCESS == ret) {
      ret = m_scanner.get_next_row(row);
      if (OB_SUCCESS == ret) {
        ASSERT_TRUE(NULL != row) << "i: " << i;
        OK(scanner_iter.add_row(const_cast<ObStoreRow*>(row)));
        STORAGE_LOG(INFO, "test", "this row", to_cstring(*row));
      } else if (OB_ITER_END != ret) {
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }
  }
  res_iter.reset();
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(scanner_iter, false));
  scanner_iter.reset();

  m_scanner.reset();
  context_.query_flag_.scan_order_ = common::ObQueryFlag::Reverse;
  OK(m_scanner.init(param_, context_, &sstable_));
  OK(m_scanner.set_range(range));

  const char* result2 = "bigint   var    bigint   bigint  flag\n"
                        "2        var1   1        1       EXIST\n"
                        "0        var1   9        NOP     EXIST\n";

  for (int64_t i = 0; i < micro_cnt; ++i) {
    ret = OB_SUCCESS;
    micro_iter.reset();
    OK(micro_iter.from(micro_data[i]));
    build_micro_block_data(micro_iter, block_data, payload_data, end_key);
    MacroBlockId macro_id(0, 0, 1, ObStoreFileSystem::RESERVED_MACRO_BLOCK_INDEX);
    ObFullMacroBlockMeta full_meta;
    OK(sstable_.get_meta(macro_id, full_meta));
    const_cast<oceanbase::blocksstable::ObMacroBlockMetaV2*>(full_meta.meta_)->contain_uncommitted_row_ = true;
    OK(m_scanner.open(macro_id, full_meta, payload_data, is_left_border, is_right_border)) << "i: " << i;
    while (OB_SUCCESS == ret) {
      ret = m_scanner.get_next_row(row);
      if (OB_SUCCESS == ret) {
        ASSERT_TRUE(NULL != row) << "i: " << i;
        OK(scanner_iter.add_row(const_cast<ObStoreRow*>(row)));
        STORAGE_LOG(INFO, "test", "this row", to_cstring(*row));
      } else if (OB_ITER_END != ret) {
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }
  }
  res_iter.reset();
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(scanner_iter, false));
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  GCONF._enable_sparse_row = false;
  system("rm -f test_micro_block_row_scanner_with_special_uncom_row.log*");
  OB_LOGGER.set_file_name("test_micro_block_row_scanner_with_special_uncom_row.log");
  STORAGE_LOG(INFO, "begin unittest: test_micro_block_row_scanner_with_special_uncom_row");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
