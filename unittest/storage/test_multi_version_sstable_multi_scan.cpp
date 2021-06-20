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
#include "lib/alloc/alloc_func.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace unittest {

class TestMultiVersionSSTableMultiScan : public ObMultiVersionSSTableTest {
public:
  TestMultiVersionSSTableMultiScan() : ObMultiVersionSSTableTest("testmultiversionmultiscan")
  {}
  virtual ~TestMultiVersionSSTableMultiScan()
  {}

  virtual void SetUp()
  {
    ObMultiVersionSSTableTest::SetUp();
  }
  virtual void TearDown()
  {
    ObMultiVersionSSTableTest::TearDown();
    columns_.reset();
    param_.reset();
    context_.reset();
    allocator_.reuse();
    projector_.reuse();
    store_ctx_.reset();
  }

  void prepare_query_param(const ObVersionRange& trans_version_range, const bool is_reverse_scan = false);

  ObArray<ObColDesc> columns_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObArenaAllocator allocator_;
  ObArray<int32_t> projector_;
  ObStoreCtx store_ctx_;
};

void TestMultiVersionSSTableMultiScan::prepare_query_param(
    const ObVersionRange& trans_version_range, const bool is_reverse_scan)
{
  columns_.reset();
  param_.reset();
  context_.reset();
  block_cache_ws_.reset();
  projector_.reset();

  ObQueryFlag query_flag;

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

  if (is_reverse_scan) {
    query_flag.scan_order_ = ObQueryFlag::Reverse;
  }

  param_.table_id_ = combine_id(TENANT_ID, TABLE_ID);
  param_.projector_ = &projector_;
  param_.schema_version_ = SCHEMA_VERSION;
  param_.rowkey_cnt_ = schema_rowkey_cnt;  // schema rowkey count
  param_.out_cols_ = &columns_;

  OK(block_cache_ws_.init(TENANT_ID));
  context_.query_flag_ = query_flag;
  context_.store_ctx_ = &store_ctx_;
  context_.allocator_ = &allocator_;
  context_.stmt_allocator_ = &allocator_;
  context_.block_cache_ws_ = &block_cache_ws_;
  context_.trans_version_range_ = trans_version_range;
  context_.is_inited_ = true;
}

TEST_F(TestMultiVersionSSTableMultiScan, multi_scan_empty_sstable)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0     NOP      2       EXIST   C\n"
                  "1        var1  -2       0     2        NOP     EXIST   L\n"
                  "2        var2  -10      0     4        3       EXIST   CL\n"
                  "3        var3  -8       0     7        2       EXIST   C\n"
                  "3        var3  -5       0     NOP      1       EXIST   N\n"
                  "3        var3  -3       0     6        NOP     EXIST   L\n"
                  "4        var4  -9       0     7        NOP     EXIST   C\n"
                  "4        var4  -7       0     6        5       EXIST   C\n"
                  "4        var4  -3       0     5        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6       0     6        NOP     EXIST   N\n"
                  "5        var5  -4       0     7        4       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0     5        2       EXIST   N\n"
                  "5        var5  -2       0     5        1       EXIST   C\n"
                  "5        var5  -1       0     4        3       EXIST   L\n";

  prepare_empty_sstable(micro_data, rowkey_cnt);
  ObMockIterator res_iter;
  ObMockIterator rowkeys_iter;
  ObStoreRowIterator* multi_scanner = NULL;
  ObArray<ObExtStoreRange> ranges;

  const char* rowkeys_str = "bigint   var   flag\n"
                            "1        var1  EXIST\n"
                            "1        var2  EXIST\n"
                            "3        var2  EXIST\n"
                            "5        var4  EXIST\n"
                            "5        var5  EXIST\n";

  const char* result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    1\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n";

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range);

  OK(rowkeys_iter.from(rowkeys_str));
  const ObStoreRow* start = NULL;
  const ObStoreRow* end = NULL;
  ObExtStoreRange ext_range;
  // 1, 2-4, 4, 4-5
  OK(rowkeys_iter.get_row(0, start));
  OK(rowkeys_iter.get_row(1, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(2, start));
  make_range(start, start, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(3, start));
  make_range(start, start, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(3, start));
  OK(rowkeys_iter.get_row(4, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));

  res_iter.reset();
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*multi_scanner, false, true));
  multi_scanner->~ObStoreRowIterator();

  const char* reverse_result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
                                "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    1\n"
                                "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n";
  prepare_query_param(trans_version_range, true);

  res_iter.reset();
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(reverse_result1));
  ASSERT_TRUE(res_iter.equals(*multi_scanner, false, true));
  multi_scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSSTableMultiScan, empty_range_scan)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var8  -8       0     NOP      2       EXIST   CL\n"
                  "1        var9  -2       0     2        NOP     EXIST   CL\n"
                  "3        var8  -8       0     7        2       EXIST   CL\n"
                  "3        var9  -3       0     6        NOP     EXIST   CL\n"
                  "4        var7  -9       0     7        NOP     EXIST   CL\n"
                  "4        var8  -7       0     6        5       EXIST   CL\n"
                  "4        var9  -3       0     5        NOP     EXIST   CL\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6       0     6        NOP     EXIST   N\n"
                  "5        var5  -4       0     7        4       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0     5        2       EXIST   N\n"
                  "5        var5  -2       0     5        1       EXIST   C\n"
                  "5        var5  -1       0     4        3       EXIST   L\n";

  prepare_data(micro_data, 3, rowkey_cnt, 9);
  ObMockIterator rowkeys_iter;
  ObStoreRowIterator* multi_scanner = NULL;
  ObArray<ObExtStoreRange> ranges;

  const char* rowkeys_str = "bigint   var   flag\n"
                            "1        NULL  EXIST\n"
                            "1        var4  EXIST\n"
                            "3        NULL  EXIST\n"
                            "3        var5  EXIST\n"
                            "4        NULL  EXIST\n"
                            "4        var6  EXIST\n";

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 20;
  prepare_query_param(trans_version_range, true);

  OK(rowkeys_iter.from(rowkeys_str));
  const ObStoreRow* start = NULL;
  const ObStoreRow* end = NULL;
  ObExtStoreRange ext_range;

  OK(rowkeys_iter.get_row(0, start));
  OK(rowkeys_iter.get_row(1, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(2, start));
  OK(rowkeys_iter.get_row(3, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(4, start));
  OK(rowkeys_iter.get_row(5, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));

  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  const ObStoreRow* row = NULL;
  ASSERT_EQ(OB_ITER_END, multi_scanner->get_next_row(row));
  multi_scanner->~ObStoreRowIterator();

  prepare_query_param(trans_version_range);
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  ASSERT_EQ(OB_ITER_END, multi_scanner->get_next_row(row));
  multi_scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSSTableMultiScan, multi_scan_normal)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0     NOP      2       EXIST   C\n"
                  "1        var1  -2       0     2        NOP     EXIST   L\n"
                  "2        var2  -10      0     4        3       EXIST   CL\n"
                  "3        var3  -8       0     7        2       EXIST   C\n"
                  "3        var3  -5       0     NOP      1       EXIST   N\n"
                  "3        var3  -3       0     6        NOP     EXIST   L\n"
                  "3        var4  -3       0     6        NOP     DELETE  L\n"
                  "4        var4  -9       0     7        NOP     EXIST   C\n"
                  "4        var4  -7       0     6        5       EXIST   C\n"
                  "4        var4  -3       0     5        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6       0     6        NOP     EXIST   N\n"
                  "5        var5  -4       0     7        4       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0     5        2       EXIST   N\n"
                  "5        var5  -2       0     5        1       EXIST   C\n"
                  "5        var5  -1       0     4        3       EXIST   L\n";

  prepare_data(micro_data, 3, rowkey_cnt, 10);
  ObMockIterator res_iter;
  ObMockIterator rowkeys_iter;
  ObStoreRowIterator* multi_scanner = NULL;
  ObArray<ObExtStoreRange> ranges;

  const char* rowkeys_str = "bigint   var   flag\n"
                            "1        var1  EXIST\n"
                            "1        var2  EXIST\n"
                            "3        var2  EXIST\n"
                            "5        var4  EXIST\n"
                            "5        var5  EXIST\n";

  const char* result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
                        "1        var1  2        NOP     EXIST   N                        TRUE    0\n"
                        "3        var3  6        1       EXIST   N                        FALSE   1\n"
                        "3        var4  6        NOP     DELETE  N                        FALSE   1\n"
                        "4        var4  6        5       EXIST   N                        FALSE   1\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n"
                        "5        var5  6        4       EXIST   N                        FALSE   3\n";

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range);

  OK(rowkeys_iter.from(rowkeys_str));
  const ObStoreRow* start = NULL;
  const ObStoreRow* end = NULL;
  ObExtStoreRange ext_range;
  // 1, 2-4, 4, 4-5
  OK(rowkeys_iter.get_row(0, start));
  make_range(start, start, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(1, start));
  OK(rowkeys_iter.get_row(3, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(3, start));
  make_range(start, start, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(3, start));
  OK(rowkeys_iter.get_row(4, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));

  res_iter.reset();
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*multi_scanner, false, true));
  multi_scanner->~ObStoreRowIterator();

  // contain empty
  const char* result2 = "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    0\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n"
                        "5        var5  4        3       EXIST   N                        FALSE   3\n";
  trans_version_range.snapshot_version_ = 1;
  prepare_query_param(trans_version_range);

  res_iter.reset();
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*multi_scanner, false, true));
  multi_scanner->~ObStoreRowIterator();

  const char* reverse_result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
                                "1        var1  2        NOP     EXIST   N                        TRUE    0\n"
                                "4        var4  6        5       EXIST   N                        FALSE   1\n"
                                "3        var4  6        NOP     DELETE  N                        FALSE   1\n"
                                "3        var3  6        1       EXIST   N                        FALSE   1\n"
                                "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n"
                                "5        var5  6        4       EXIST   N                        FALSE   3\n";
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range, true);

  res_iter.reset();
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(reverse_result1));
  ASSERT_TRUE(res_iter.equals(*multi_scanner, false, true));
  multi_scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSSTableMultiScan, test_scan_index)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0     NOP      2       EXIST   C\n"
                  "1        var1  -2       0     2        NOP     EXIST   L\n"
                  "2        var2  -10      0     4        3       EXIST   CL\n"
                  "3        var3  -8       0     7        2       EXIST   C\n"
                  "3        var3  -5       0     NOP      1       EXIST   N\n"
                  "3        var3  -3       0     6        NOP     EXIST   L\n"
                  "3        var4  -3       0     6        NOP     DELETE  L\n"
                  "4        var4  -9       0     7        NOP     EXIST   C\n"
                  "4        var4  -7       0     6        5       EXIST   C\n"
                  "4        var4  -3       0     5        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6       0     6        NOP     EXIST   N\n"
                  "5        var5  -4       0     7        4       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0     5        2       EXIST   N\n"
                  "5        var5  -2       0     5        1       EXIST   C\n"
                  "5        var5  -1       0     4        3       EXIST   L\n";

  prepare_data(micro_data, 3, rowkey_cnt, 10);
  ObMockIterator res_iter;
  ObMockIterator rowkeys_iter;
  ObStoreRowIterator* multi_scanner = NULL;
  ObArray<ObExtStoreRange> ranges;

  const char* rowkeys_str = "bigint   var   flag\n"
                            "1        var1  EXIST\n"
                            "1        var2  EXIST\n"
                            "3        var7  EXIST\n"
                            "3        var8  EXIST\n"
                            "5        var5  EXIST\n"
                            "5        var6  EXIST\n";

  const char* reverse_result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
                                "1        var1  2        NOP     EXIST   N                        FALSE   0\n"
                                "5        var5  6        4       EXIST   N                        FALSE   2\n";

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range, true);

  OK(rowkeys_iter.from(rowkeys_str));
  const ObStoreRow* start = NULL;
  const ObStoreRow* end = NULL;
  ObExtStoreRange ext_range;

  OK(rowkeys_iter.get_row(0, start));
  OK(rowkeys_iter.get_row(1, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(2, start));
  OK(rowkeys_iter.get_row(3, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(4, start));
  OK(rowkeys_iter.get_row(5, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));

  res_iter.reset();
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(reverse_result1));
  ASSERT_TRUE(res_iter.equals(*multi_scanner, false, true));
  multi_scanner->~ObStoreRowIterator();

  const char* rowkeys_str1 = "bigint   var   flag\n"
                             "1        var1  EXIST\n"
                             "1        var2  EXIST\n"
                             "3        var7  EXIST\n"
                             "3        var8  EXIST\n"
                             "5        var5  EXIST\n"
                             "5        var6  EXIST\n";

  const char* result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
                        "1        var1  2        NOP     EXIST   N                        FALSE   0\n"
                        "5        var5  6        4       EXIST   N                        FALSE   2\n";

  prepare_query_param(trans_version_range, false);
  OK(rowkeys_iter.from(rowkeys_str1));

  ext_range.reset();
  ranges.reset();
  OK(rowkeys_iter.get_row(0, start));
  OK(rowkeys_iter.get_row(1, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(2, start));
  OK(rowkeys_iter.get_row(3, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(4, start));
  OK(rowkeys_iter.get_row(5, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));

  res_iter.reset();
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*multi_scanner, false, true));
  multi_scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSSTableMultiScan, span_macro_block)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[7];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -5       0     NOP      2       EXIST   C\n";
  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -2       0     2        NOP     EXIST   L\n"
                  "2        var2  -10      0     4        3       EXIST   CL\n"
                  "3        var3  -8       0     7        2       EXIST   C\n";
  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -5       0     NOP      1       EXIST   N\n";
  micro_data[3] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -3       0     6        NOP     EXIST   L\n"
                  "4        var4  -9       0     7        NOP     EXIST   C\n";
  micro_data[4] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "4        var4  -7       0     6        5       EXIST   C\n"
                  "4        var4  -3       0     5        NOP     EXIST   L\n";

  micro_data[5] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6       0     6        NOP     EXIST   N\n"
                  "5        var5  -4       0     7        4       EXIST   N\n";

  micro_data[6] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0     5        2       EXIST   N\n"
                  "5        var5  -2       0     5        1       EXIST   C\n"
                  "5        var5  -1       0     4        3       EXIST   L\n";

  prepare_data_start(micro_data, rowkey_cnt, 10, "none", FLAT_ROW_STORE, 0);
  for (int i = 0; i < 7; i++) {
    prepare_one_macro(&micro_data[i], 1);
  }
  prepare_data_end();

  ObMockIterator res_iter;
  ObMockIterator rowkeys_iter;
  ObStoreRowIterator* multi_scanner = NULL;
  ObArray<ObExtStoreRange> ranges;

  const char* rowkeys_str = "bigint   var   flag\n"
                            "1        var1  EXIST\n"
                            "1        var2  EXIST\n"
                            "3        var2  EXIST\n"
                            "5        var4  EXIST\n"
                            "5        var5  EXIST\n";

  const char* result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
                        "1        var1  2        NOP     EXIST   N                        TRUE    0\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n"
                        "5        var5  5        1       EXIST   N                        FALSE   3\n";

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 2;
  prepare_query_param(trans_version_range);

  OK(rowkeys_iter.from(rowkeys_str));
  const ObStoreRow* start = NULL;
  const ObStoreRow* end = NULL;
  ObExtStoreRange ext_range;
  // 1, 2-4, 4, 4-5
  OK(rowkeys_iter.get_row(0, start));
  make_range(start, start, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(1, start));
  OK(rowkeys_iter.get_row(3, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(3, start));
  make_range(start, start, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(3, start));
  OK(rowkeys_iter.get_row(4, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));

  res_iter.reset();
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*multi_scanner, false, true));
  multi_scanner->~ObStoreRowIterator();

  const char* reverse_result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
                                "1        var1  2        NOP     EXIST   N                        TRUE    0\n"
                                "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n"
                                "5        var5  5        1       EXIST   N                        FALSE   3\n";
  prepare_query_param(trans_version_range, true);

  res_iter.reset();
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(reverse_result1));
  ASSERT_TRUE(res_iter.equals(*multi_scanner, false, true));
  multi_scanner->~ObStoreRowIterator();
  //
  const char* result2 = "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
                        "1        var1  NOP      2       EXIST   N                        TRUE    0\n"
                        "3        var3  6        1       EXIST   N                        FALSE   1\n"
                        "4        var4  5        NOP     EXIST   N                        FALSE   1\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n"
                        "5        var5  7        4       EXIST   N                        FALSE   3\n";

  trans_version_range.snapshot_version_ = 5;
  prepare_query_param(trans_version_range);

  res_iter.reset();
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*multi_scanner, false, true));
  multi_scanner->~ObStoreRowIterator();

  const char* reverse_result2 = "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
                                "1        var1  NOP      2       EXIST   N                        TRUE    0\n"
                                "4        var4  5        NOP     EXIST   N                        FALSE   1\n"
                                "3        var3  6        1       EXIST   N                        FALSE   1\n"
                                "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n"
                                "5        var5  7        4       EXIST   N                        FALSE   3\n";
  prepare_query_param(trans_version_range, true);

  res_iter.reset();
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(reverse_result2));
  ASSERT_TRUE(res_iter.equals(*multi_scanner, false, true));
  multi_scanner->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSSTableMultiScan, test_skip_single_range)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0     NOP      2       EXIST   C\n"
                  "1        var1  -2       0     2        NOP     EXIST   L\n"
                  "2        var2  -10      0     4        3       EXIST   CL\n"
                  "3        var3  -8       0     7        2       EXIST   C\n"
                  "3        var3  -5       0     NOP      1       EXIST   N\n"
                  "3        var3  -3       0     6        NOP     EXIST   L\n"
                  "3        var4  -3       0     6        NOP     DELETE  L\n"
                  "4        var4  -9       0     7        NOP     EXIST   C\n"
                  "4        var4  -7       0     6        5       EXIST   C\n"
                  "4        var4  -3       0     5        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6       0     6        NOP     EXIST   N\n"
                  "5        var5  -4       0     7        4       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0     5        2       EXIST   N\n"
                  "5        var5  -2       0     5        1       EXIST   C\n"
                  "5        var5  -1       0     4        3       EXIST   L\n";

  prepare_data(micro_data, 3, rowkey_cnt, 10);
  ObMockIterator res_iter;
  ObMockIterator rowkeys_iter;
  ObStoreRowIterator* multi_scanner = NULL;
  ObArray<ObExtStoreRange> ranges;

  const char* rowkeys_str = "bigint   var   flag\n"
                            "1        var1  EXIST\n"
                            "1        var2  EXIST\n"
                            "3        var2  EXIST\n"
                            "5        var4  EXIST\n"
                            "5        var5  EXIST\n";

  /*const char *result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
      "1        var1  2        NOP     EXIST   N                        TRUE    0\n"
      "3        var3  6        1       EXIST   N                        FALSE   1\n"
      "3        var4  6        NOP     DELETE  N                        FALSE   1\n"
      "4        var4  6        5       EXIST   N                        FALSE   1\n"
      "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n"
      "5        var5  6        4       EXIST   N                        FALSE   3\n";*/
  const char* skip_result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
                             "1        var1  2        NOP     EXIST   N                        TRUE    0\n"
                             "3        var3  6        1       EXIST   N                        FALSE   1\n"
                             "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n"
                             "5        var5  6        4       EXIST   N                        FALSE   3\n";
  const char* gap_key = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "4        var5  6        5       EXIST   N\n";
  ObMockIterator gap_iter;
  ObArray<ObStoreRowkey*> gap_rowkeys;
  OK(gap_iter.from(gap_key));
  ASSERT_EQ(OB_SUCCESS, convert_iter_to_gapkeys(2, gap_iter, gap_rowkeys));

  ObArray<SkipInfo> skip_infos;
  SkipInfo info;
  info.start_key_ = 2;
  info.gap_key_ = 2;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));

  ObArray<int64_t> range_idx;
  ASSERT_EQ(OB_SUCCESS, range_idx.push_back(1));

  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range);

  OK(rowkeys_iter.from(rowkeys_str));
  const ObStoreRow* start = NULL;
  const ObStoreRow* end = NULL;
  ObExtStoreRange ext_range;
  // 1, 2-4, 4, 4-5
  OK(rowkeys_iter.get_row(0, start));
  make_range(start, start, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(1, start));
  OK(rowkeys_iter.get_row(3, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(3, start));
  make_range(start, start, ext_range);
  OK(ranges.push_back(ext_range));
  OK(rowkeys_iter.get_row(3, start));
  OK(rowkeys_iter.get_row(4, end));
  make_range(start, end, ext_range);
  OK(ranges.push_back(ext_range));

  STORAGE_LOG(INFO, "multi scan ranges", K(ranges));
  res_iter.reset();
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(skip_result1));
  ASSERT_EQ(OB_SUCCESS, compare_skip_iter(res_iter, multi_scanner, skip_infos, gap_rowkeys, range_idx));
  multi_scanner->~ObStoreRowIterator();

  /*const char *reverse_result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
      "5        var5  6        4       EXIST   N                        FALSE   3\n"
      "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n"
      "4        var4  6        5       EXIST   N                        FALSE   1\n"
      "3        var4  6        NOP     DELETE  N                        FALSE   1\n"
      "3        var3  6        1       EXIST   N                        FALSE   1\n"
      "1        var1  2        NOP     EXIST   N                        TRUE    0\n";*/
  const char* reverse_skip_result1 =
      "bigint   var   bigint   bigint  flag    multi_version_row_flag   is_get  scan_index\n"
      "1        var1  2        NOP     EXIST   N                        TRUE    0\n"
      "4        var4  6        5       EXIST   N                        FALSE   1\n"
      "3        var3  6        1       EXIST   N                        FALSE   1\n"
      "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N        TRUE    2\n"
      "5        var5  6        4       EXIST   N                        FALSE   3\n";
  const char* reverse_gap_key = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                                "3        var3  6        5       EXIST   N\n";
  trans_version_range.snapshot_version_ = 7;
  prepare_query_param(trans_version_range, true);

  skip_infos.reset();
  info.start_key_ = 2;
  info.gap_key_ = 2;
  ASSERT_EQ(OB_SUCCESS, skip_infos.push_back(info));
  res_iter.reset();
  OK(gap_iter.from(reverse_gap_key));
  ASSERT_EQ(OB_SUCCESS, convert_iter_to_gapkeys(2, gap_iter, gap_rowkeys));
  OK(sstable_.multi_scan(param_, context_, ranges, multi_scanner));
  OK(res_iter.from(reverse_skip_result1));
  ASSERT_EQ(OB_SUCCESS, compare_skip_iter(res_iter, multi_scanner, skip_infos, gap_rowkeys, range_idx));
  multi_scanner->~ObStoreRowIterator();
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  GCONF._enable_sparse_row = true;
  system("rm -rf test_multi_version_sstable_multi_scan.log");
  OB_LOGGER.set_file_name("test_multi_version_sstable_multi_scan.log");
  STORAGE_LOG(INFO, "begin unittest: test_multi_version_sstable_multi_scan");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
