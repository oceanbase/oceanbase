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

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace unittest {

class TestMultiVersionSparseSSTableMultiGet : public ObMultiVersionSSTableTest {
public:
  TestMultiVersionSparseSSTableMultiGet() : ObMultiVersionSSTableTest("testmultiversionsparsemultiget")
  {}
  virtual ~TestMultiVersionSparseSSTableMultiGet()
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

  void prepare_query_param(const ObVersionRange& trans_version_range);
  void prepare_rowkeys(const ObMockIterator& rowkeys_iter, ObArray<ObExtStoreRowkey>& rowkeys);

  ObArray<ObColDesc> columns_;
  ObTableIterParam param_;
  ObTableAccessContext context_;
  ObArenaAllocator allocator_;
  ObArray<int32_t> projector_;
  ObStoreCtx store_ctx_;
};

void TestMultiVersionSparseSSTableMultiGet::prepare_query_param(const ObVersionRange& trans_version_range)
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

void TestMultiVersionSparseSSTableMultiGet::prepare_rowkeys(
    const ObMockIterator& rowkeys_iter, ObArray<ObExtStoreRowkey>& rowkeys)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* row = NULL;
  ObExtStoreRowkey ext_rowkey;
  for (int i = 0; ret == OB_SUCCESS; i++) {
    ret = rowkeys_iter.get_row(i, row);
    if (OB_SUCCESS == ret) {
      ext_rowkey.reset();
      ASSERT_TRUE(NULL != row);
      ObSSTableTest::convert_rowkey(ObStoreRowkey(row->row_val_.cells_, rowkey_cnt_ - 2), ext_rowkey, allocator_);
      OK(rowkeys.push_back(ext_rowkey));
    }
  }
  ASSERT_TRUE(OB_ITER_END == ret);
}

TEST_F(TestMultiVersionSparseSSTableMultiGet, multi_get_on_empty_sstable)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0       2        NOP     EXIST   C\n"
                  "1        var1  -2       0       2        NOP     EXIST   L\n"
                  "2        var2  -10      0       4        3       EXIST   CL\n"
                  "3        var3  -8       0       7        2       EXIST   C\n"
                  "3        var3  -5       0       1        NOP     EXIST   N\n"  // 5
                  "3        var3  -3       0       6        NOP     EXIST   L\n"
                  "4        var4  -9       0       7        NOP     EXIST   C\n"
                  "4        var4  -7       0       6        5       EXIST   C\n"
                  "4        var4  -3       0       5        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6       0       6        NOP     EXIST   N\n"  // 10
                  "5        var5  -4       0       7        4       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0       5        2       EXIST   N\n"
                  "5        var5  -2       0       5        1       EXIST   C\n"
                  "5        var5  -1       0       4        3       EXIST   L\n";
  /*
    const char *input_column = "int int int int int\n"
                                "16  17  7  20\n"
                                "16  17  7  19\n"
                                "16  17  7  19  20\n"
                                "16  17  7  19  20\n"
                                "16  17  7  20\n" //5
                                "16  17  7  19\n"
                                "16  17  7  20\n"
                                "16  17  7  19  20\n"
                                "16  17  7  22\n"
                                "16  17  7  20\n" //10
                                "16  17  7  19\n"
                                "16  17  7  19  20\n"
                                "16  17  7  20  19\n"
                                "16  17  7  19  21\n";

    const int64_t col_cnt[] =
    {
        4, 4, 5, 5, 4,
        4, 4, 5, 4, 4,
        4, 5, 5, 5
    };*/
  prepare_empty_sstable(micro_data, rowkey_cnt);
  ObMockIterator res_iter;
  ObMockIterator rowkeys_iter;
  ObStoreRowIterator* multi_getter = NULL;
  ObArray<ObExtStoreRowkey> rowkeys;

  const char* rowkeys_str = "bigint   var   flag\n"
                            "1        var1  EXIST\n"
                            "2        var2  EXIST\n"
                            "3        var3  EXIST\n"
                            "4        var4  EXIST\n"
                            "5        var5  EXIST\n";

  const char* result1 = "bigint   var   bigint   bigint  flag                    multi_version_row_flag\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n";

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;

  prepare_query_param(trans_version_range);

  res_iter.reset();
  OK(rowkeys_iter.from(rowkeys_str));
  prepare_rowkeys(rowkeys_iter, rowkeys);
  OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*multi_getter));
  multi_getter->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableMultiGet, multi_get_normal)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0       2        NOP     EXIST   C\n"
                  "1        var1  -2       0       2        NOP     EXIST   L\n"
                  "2        var2  -10      0       4        3       EXIST   CL\n"
                  "3        var3  -8       0       7        2       EXIST   C\n"
                  "3        var3  -5       0       1        NOP     EXIST   N\n"  // 5
                  "3        var3  -3       0       6        NOP     EXIST   L\n"
                  "4        var4  -9       0       7        NOP     EXIST   C\n"
                  "4        var4  -7       0       6        5       EXIST   C\n"
                  "4        var4  -3       0       5        NOP     EXIST   L\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6       0       6        NOP     EXIST   N\n"  // 10
                  "5        var5  -4       0       7        4       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0       5        2       EXIST   N\n"
                  "5        var5  -2       0       5        1       EXIST   C\n"
                  "5        var5  -1       0       4        3       EXIST   L\n";

  const char* input_column = "int int int int int int\n"
                             "16  17  7  8   21\n"
                             "16  17  7  8   20\n"
                             "16  17  7  8   20  21\n"
                             "16  17  7  8   20  21\n"
                             "16  17  7  8   21\n"  // 5
                             "16  17  7  8   20\n"
                             "16  17  7  8   21\n"
                             "16  17  7  8   20  21\n"
                             "16  17  7  8   22\n"
                             "16  17  7  8   21\n"  // 10
                             "16  17  7  8   20  21\n"
                             "16  17  7  8   20  21\n"
                             "16  17  7  8   21  20\n"
                             "16  17  7  8   20  22\n";

  const int64_t col_cnt[] = {5, 5, 6, 6, 5, 5, 5, 6, 5, 5, 6, 6, 6, 6};

  prepare_data(micro_data, 3, rowkey_cnt, 9, "none", SPARSE_ROW_STORE, 0, input_column, col_cnt);
  // prepare_data(micro_data, 3, rowkey_cnt, 10);
  ObMockIterator res_iter;
  ObMockIterator rowkeys_iter;
  ObStoreRowIterator* multi_getter = NULL;
  ObArray<ObExtStoreRowkey> rowkeys;

  const char* rowkeys_str = "bigint   var   flag\n"
                            "1        var1  EXIST\n"
                            "2        var2  EXIST\n"
                            "3        var3  EXIST\n"
                            "3        var4  EXIST\n"
                            "4        var4  EXIST\n"
                            "5        var5  EXIST\n";

  const char* result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  2        NOP     EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "3        var3  6        1       EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST  N\n"
                        "4        var4  6        5       EXIST   N\n"
                        "5        var5  7        6       EXIST   N\n";

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  prepare_query_param(trans_version_range);

  res_iter.reset();
  OK(rowkeys_iter.from(rowkeys_str));
  prepare_rowkeys(rowkeys_iter, rowkeys);
  OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*multi_getter));
  multi_getter->~ObStoreRowIterator();

  // contain empty
  const char* result2 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  2        NOP     EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "5        var5  1        5       EXIST   N\n";
  trans_version_range.snapshot_version_ = 2;
  prepare_query_param(trans_version_range);

  res_iter.reset();
  OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*multi_getter));
  multi_getter->~ObStoreRowIterator();

  // contain empty
  const char* result3 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  NOP      2       EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "3        var3  7        2       EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "4        var4  6        5       EXIST   N\n"
                        "5        var5  7        6       EXIST   N\n";
  trans_version_range.snapshot_version_ = 8;
  trans_version_range.base_version_ = 2;
  prepare_query_param(trans_version_range);

  res_iter.reset();
  OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
  OK(res_iter.from(result3));
  ASSERT_TRUE(res_iter.equals(*multi_getter));
  multi_getter->~ObStoreRowIterator();

  // contain empty
  const char* result4 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "3        var3  NOP      1       EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "5        var5  NOP      6       EXIST   N\n";
  trans_version_range.snapshot_version_ = 6;
  trans_version_range.base_version_ = 4;
  prepare_query_param(trans_version_range);

  res_iter.reset();
  OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
  OK(res_iter.from(result4));
  ASSERT_TRUE(res_iter.equals(*multi_getter));
  multi_getter->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableMultiGet, multi_get_not_exist)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[3];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0     28       2       EXIST   C\n"
                  "1        var1  -2       0     2        NOP     EXIST   L\n"
                  "2        var2  -7       0     4        3       EXIST   CL\n"
                  "3        var3  -8       0     7        2       EXIST   C\n"
                  "3        var3  -5       0     81       21      EXIST   N\n"  // 5
                  "3        var3  -3       0     6        NOP     EXIST   L\n"
                  "3        var4  -3       0     6        7       DELETE  L\n"
                  "4        var4  -9       0     7        NOP     EXIST   C\n"
                  "4        var4  -7       0     6        5       EXIST   C\n"
                  "4        var4  -3       0     5        NOP     EXIST   L\n";  // 10

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6       0     6        NOP     EXIST   N\n"
                  "5        var5  -4       0     7        4       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0     5        2       EXIST   N\n"
                  "5        var5  -2       0     5        1       EXIST   C\n"
                  "5        var5  -1       0     4        3       EXIST   L\n";  // 15
  const char* input_column = "int int int int int int\n"
                             "16  17  7  8   21  20\n"
                             "16  17  7  8   20  NOP\n"
                             "16  17  7  8   20  21\n"
                             "16  17  7  8   21  20\n"
                             "16  17  7  8   21  20\n"  // 5
                             "16  17  7  8   20  NOP\n"
                             "16  17  7  8   21  22\n"
                             "16  17  7  8   20  NOP\n"
                             "16  17  7  8   22  21\n"
                             "16  17  7  8   21  NOP\n"  // 10
                             "16  17  7  8   21  NOP\n"
                             "16  17  7  8   21  22\n"
                             "16  17  7  8   21  20\n"
                             "16  17  7  8   21  20\n"
                             "16  17  7  8   20  22\n";  // 15

  const int64_t col_cnt[] = {6, 5, 6, 6, 6, 5, 6, 5, 6, 5, 5, 6, 6, 6, 6};

  prepare_data(micro_data, 3, rowkey_cnt, 10, "none", SPARSE_ROW_STORE, 0, input_column, col_cnt);
  ObMockIterator res_iter;
  ObMockIterator rowkeys_iter;
  ObStoreRowIterator* multi_getter = NULL;
  ObArray<ObExtStoreRowkey> rowkeys;

  const char* rowkeys_str = "bigint   var   flag\n"
                            "1        var1  EXIST\n"
                            "2        var2  EXIST\n"
                            "3        var3  EXIST\n"
                            "3        var4  EXIST\n"
                            "4        var4  EXIST\n"
                            "5        var5  EXIST\n";

  const char* result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "  1      var1  2        NOP    EXIST   N\n"
                        "  2      var2  4        3      EXIST   N\n"
                        "  3      var3  21       81     EXIST   N\n"
                        "  3      var4  NOP      6      DELETE  N\n"
                        "  4      var4  NOP      5      EXIST   N\n"
                        "  5      var5  2        6      EXIST   N\n";

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 7;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  prepare_query_param(trans_version_range);

  res_iter.reset();
  OK(rowkeys_iter.from(rowkeys_str));
  prepare_rowkeys(rowkeys_iter, rowkeys);
  OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*multi_getter));
  multi_getter->~ObStoreRowIterator();

  const char* rowkeys_str2 = "bigint   var   flag\n"
                             "1        var1  EXIST\n"
                             "2        var2  EXIST\n"
                             "3        var3  EXIST\n"
                             "3        var4  EXIST\n"
                             "4        var4  EXIST\n"
                             "5        var5  EXIST\n";

  const char* result2 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "NOP      NOP   NOP      NOP    OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP    OP_ROW_DOES_NOT_EXIST   N\n"
                        "  3      var3  21       81     EXIST   N\n"
                        "NOP      NOP   NOP      NOP    OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP    OP_ROW_DOES_NOT_EXIST   N\n"
                        "  5      var5  NOP      7      EXIST   N\n";

  trans_version_range.snapshot_version_ = 5;
  trans_version_range.base_version_ = 3;
  prepare_query_param(trans_version_range);

  res_iter.reset();
  OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*multi_getter));
  multi_getter->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableMultiGet, span_macro_block)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[7];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8       0      82       2       EXIST   C\n";
  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -2       0      2        NOP     EXIST   L\n"
                  "2        var2  -10      0      4        3       EXIST   CL\n"
                  "3        var3  -8       0      7        2       EXIST   C\n";
  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -5       0      NOP      1       EXIST   N\n";
  micro_data[3] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -3       0      6        NOP     EXIST   L\n"
                  "4        var4  -9       0      7        NOP     EXIST   C\n";
  micro_data[4] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "4        var4  -7       0      6        5       EXIST   C\n"
                  "4        var4  -3       0      5        NOP     EXIST   L\n";

  micro_data[5] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6       0      6        NOP     EXIST   N\n"
                  "5        var5  -4       0      7        4       EXIST   N\n";

  micro_data[6] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -3       0      5        2       EXIST   N\n"
                  "5        var5  -2       0      5        1       EXIST   C\n"
                  "5        var5  -1       0      4        3       EXIST   L\n";
  prepare_data_start(micro_data, rowkey_cnt, 10);
  for (int i = 0; i < 7; i++) {
    prepare_one_macro(&micro_data[i], 1);
  }
  prepare_data_end();

  ObMockIterator res_iter;
  ObMockIterator rowkeys_iter;
  ObStoreRowIterator* multi_getter = NULL;
  ObArray<ObExtStoreRowkey> rowkeys;

  const char* rowkeys_str1 = "bigint   var   flag\n"
                             "1        var1  EXIST\n"
                             "2        var2  EXIST\n"
                             "3        var3  EXIST\n"
                             "4        var4  EXIST\n"
                             "5        var5  EXIST\n";

  const char* result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  2        NOP     EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "5        var5  5        1       EXIST   N\n";

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 2;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  prepare_query_param(trans_version_range);

  res_iter.reset();
  OK(rowkeys_iter.from(rowkeys_str1));
  prepare_rowkeys(rowkeys_iter, rowkeys);
  OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*multi_getter));
  multi_getter->~ObStoreRowIterator();

  //
  const char* rowkeys_str2 = "bigint   var   flag\n"
                             "2        var2  EXIST\n"
                             "3        var3  EXIST\n"
                             "5        var5  EXIST\n";

  const char* result2 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "NOP      NOP   NOP      NOP     OP_ROW_DOES_NOT_EXIST   N\n"
                        "3        var3  6        1       EXIST   N\n"
                        "5        var5  7        4       EXIST   N\n";

  trans_version_range.snapshot_version_ = 5;
  prepare_query_param(trans_version_range);

  res_iter.reset();
  rowkeys.reset();
  OK(rowkeys_iter.from(rowkeys_str2));
  prepare_rowkeys(rowkeys_iter, rowkeys);
  OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
  OK(res_iter.from(result2));
  ASSERT_TRUE(res_iter.equals(*multi_getter));
  multi_getter->~ObStoreRowIterator();
}

TEST_F(TestMultiVersionSparseSSTableMultiGet, bug_14915264)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[16];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2  -100     0      4        3       EXIST   C\n"
                  "2        var2  -80      0      4        3       EXIST   N\n";

  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2  -78      0      NOP      2       EXIST   N\n"
                  "2        var2  -76      0      4        3       EXIST   N\n";

  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2  -68      0      NOP      2       EXIST   N\n"
                  "2        var2  -66      0      4        3       EXIST   N\n";

  micro_data[3] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2  -58      0      NOP      2       EXIST   N\n"
                  "2        var2  -56      0      4        3       EXIST   N\n";

  micro_data[4] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2  -48      0      NOP      2       EXIST   N\n"
                  "2        var2  -46      0      4        3       EXIST   N\n";
  micro_data[5] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2  -38      0      NOP      2       EXIST   N\n"
                  "2        var2  -36      0      4        3       EXIST   N\n";
  micro_data[6] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2  -28      0      NOP      2       EXIST   N\n"
                  "2        var2  -26      0      4        3       EXIST   C\n";
  micro_data[7] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2  -8       0      NOP      2       EXIST   N\n"
                  "2        var2  -6       0      4        3       EXIST   L\n";

  micro_data[8] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var3  -8       0      NOP      2       EXIST   CL\n"
                  "4        var4  -10      0      4        3       EXIST   CL\n";

  micro_data[9] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var4  -6       0      6        NOP     EXIST   C\n"
                  "5        var4  -4       0      7        4       EXIST   C\n";

  micro_data[10] = "bigint   var   bigint  bigint bigint   bigint  flag    multi_version_row_flag\n"
                   "5        var4  -3       0      5        2       EXIST   CL\n"
                   "5        var7  -2       0      5        1       EXIST   CL\n";

  prepare_data_start(micro_data, rowkey_cnt, 100);
  prepare_one_macro(micro_data, 3);
  prepare_one_macro(&micro_data[3], 5);
  prepare_one_macro(&micro_data[8], 1);
  prepare_one_macro(&micro_data[9], 2);
  prepare_data_end();
  ObMockIterator res_iter;
  ObMockIterator rowkeys_iter;
  ObStoreRowIterator* multi_getter = NULL;
  ObArray<ObExtStoreRowkey> rowkeys;

  const char* rowkeys_str = "bigint   var   flag\n"
                            "2        var2  EXIST\n"
                            "3        var3  EXIST\n"
                            "5        var4  EXIST\n";

  const char* result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "2        var2  4        3       EXIST   N\n"
                        "3        var3  NOP      2       EXIST   N\n"
                        "5        var4  6        NOP     EXIST   N\n";

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 200;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  prepare_query_param(trans_version_range);

  res_iter.reset();
  OK(rowkeys_iter.from(rowkeys_str));
  prepare_rowkeys(rowkeys_iter, rowkeys);
  OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*multi_getter));
  multi_getter->~ObStoreRowIterator();
}

// TEST_F(TestMultiVersionSparseSSTableMultiGet, test_bloom_filter)
//{
// const int64_t rowkey_cnt = 4;
// const char *micro_data[2];
// micro_data[0] =
//"bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
//"3        var2  -18      NOP      8       EXIST   C\n"
//"3        var2  -15      NOP      11      EXIST   N\n"
//"3        var2  -13      NOP      9       EXIST   N\n";
// micro_data[1] =
//"bigint   var   bigint   bigint   bigint  flag    multi_version_row_flag\n"
//"3        var6  -8       NOP      8       EXIST   N\n"
//"3        var6  -5       6        10      EXIST   N\n"
//"3        var6  -3       5        9       EXIST   L\n";

// prepare_data_start(micro_data, rowkey_cnt, 18);
// prepare_one_macro(micro_data, 1);
// prepare_one_macro(&micro_data[1], 1);
// prepare_data_end();

// ObMockIterator rowkeys_iter;
// ObStoreRowIterator *multi_getter = NULL;
// const ObStoreRow *row = NULL;
// ObArray<ObExtStoreRowkey> rowkeys;

//// not exist rowkey
// const char *not_exist_rowkeys =
//"bigint   var   flag    multi_version_row_flag\n"
//"2        var1  EXIST   N\n"
//"3        var3  EXIST   N\n"
//"2        var3  EXIST   N\n"
//"3        var4  EXIST   N\n";

// ObVersionRange version_range;
// version_range.snapshot_version_ = 16;
// version_range.base_version_ = 0;
// version_range.multi_version_start_ = 0;
// prepare_query_param(version_range);
// sstable_.key_.trans_version_range_.snapshot_version_ = 10;

// rowkeys_iter.reset();
// OK(rowkeys_iter.from(not_exist_rowkeys));
// prepare_rowkeys(rowkeys_iter, rowkeys);

// OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
// int tmp_ret = OB_SUCCESS;
// row = NULL;
// while (tmp_ret == OB_SUCCESS) {
// tmp_ret = multi_getter->get_next_row(row);
// if (NULL != row) {
// ASSERT_TRUE(ObActionFlag::OP_ROW_DOES_NOT_EXIST == row->flag_);
//}
//}
// ASSERT_EQ(OB_ITER_END, tmp_ret);
// multi_getter->~ObStoreRowIterator();

// const ObMacroBlockMeta *meta = NULL;
// get_macro_meta(1, 1, meta);
// ASSERT_EQ(2, meta->empty_read_cnt_[1]);
// get_macro_meta(2, 1, meta);
// ASSERT_EQ(2, meta->empty_read_cnt_[1]);

// sstable_.key_.trans_version_range_.snapshot_version_ = 20;
// OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
// tmp_ret = OB_SUCCESS;
// while (tmp_ret == OB_SUCCESS) {
// tmp_ret = multi_getter->get_next_row(row);
// if (NULL != row) {
// ASSERT_TRUE(ObActionFlag::OP_ROW_DOES_NOT_EXIST == row->flag_);
//}
//}
// ASSERT_EQ(OB_ITER_END, tmp_ret);
// multi_getter->~ObStoreRowIterator();

// get_macro_meta(1, 1, meta);
// ASSERT_EQ(2, meta->empty_read_cnt_[2]);
// get_macro_meta(2, 1, meta);
// ASSERT_EQ(2, meta->empty_read_cnt_[2]);
//}

TEST_F(TestMultiVersionSparseSSTableMultiGet, sync_io)
{
  const int64_t rowkey_cnt = 4;
  const char* micro_data[7];
  micro_data[0] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "1        var1  -8      0       NOP      2       EXIST   C\n"
                  "1        var1  -4      0       2        NOP     EXIST   L\n"
                  "1        var1  -2      0       2        NOP     EXIST   L\n"
                  "1        var2  -2      0       2        NOP     EXIST   CL\n";
  micro_data[1] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "2        var2  -10     0       4        3       EXIST   C\n"
                  "2        var2  -8      0       4        3       EXIST   N\n"
                  "2        var2  -4      0       4        3       EXIST   CL\n"
                  "3        var3  -8      0       7        2       EXIST   CL\n"
                  "3        var4  -8      0       7        2       EXIST   CL\n";
  micro_data[2] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var5  -5      0       NOP      1       EXIST   C\n"
                  "3        var5  -4      0       NOP      1       EXIST   N\n"
                  "3        var5  -3      0       NOP      1       EXIST   CL\n"
                  "3        var6  -5      0       NOP      1       EXIST   CL\n";
  micro_data[3] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "3        var7  -3      0       6        NOP     EXIST   C\n"
                  "3        var7  -2      0       6        NOP     EXIST   N\n"
                  "3        var7  -1      0       6        NOP     EXIST   CL\n"
                  "4        var4  -9      0       7        NOP     EXIST   CL\n"
                  "4        var5  -9      0       7        NOP     EXIST   CL\n";
  micro_data[4] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "4        var6  -7      0       6        5       EXIST   C\n"
                  "4        var6  -5      0       6        5       EXIST   N\n"
                  "4        var6  -3      0       6        5       EXIST   CL\n"
                  "4        var7  -3      0       5        NOP     EXIST   CL\n";

  micro_data[5] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var5  -6      0       6        NOP     EXIST   C\n"
                  "5        var5  -4      0       6        NOP     EXIST   N\n"
                  "5        var5  -2      0       6        NOP     EXIST   CL\n"
                  "5        var6  -4      0       7        4       EXIST   CL\n";

  micro_data[6] = "bigint   var   bigint bigint  bigint   bigint  flag    multi_version_row_flag\n"
                  "5        var7  -3      0       5        2       EXIST   C\n"
                  "5        var7  -2      0       5        2       EXIST   N\n"
                  "5        var7  -1      0       5        2       EXIST   CL\n"
                  "5        var8  -2      0       5        1       EXIST   CL\n"
                  "5        var9  -1      0       4        3       EXIST   CL\n";

  const int64_t max_merged_trans_version = 10;
  prepare_data_start(micro_data, rowkey_cnt, max_merged_trans_version);
  for (int i = 0; i < 7; i++) {
    prepare_one_macro(&micro_data[i], 1, max_merged_trans_version);
  }
  prepare_data_end();

  ObMockIterator res_iter;
  ObMockIterator rowkeys_iter;
  ObStoreRowIterator* multi_getter = NULL;
  ObArray<ObExtStoreRowkey> rowkeys;

  const char* rowkeys_str1 = "bigint   var   flag\n"
                             "1        var1  EXIST\n"
                             "2        var2  EXIST\n"
                             "3        var5  EXIST\n"
                             "3        var7  EXIST\n"
                             "4        var6  EXIST\n"
                             "3        var3  EXIST\n"
                             "5        var5  EXIST\n"
                             "3        var6  EXIST\n"
                             "5        var7  EXIST\n"
                             "4        var7  EXIST\n";

  const char* result1 = "bigint   var   bigint   bigint  flag    multi_version_row_flag\n"
                        "1        var1  NOP      2       EXIST   C\n"
                        "2        var2  4        3       EXIST   CL\n"
                        "3        var5  NOP      1       EXIST   CL\n"
                        "3        var7  6        NOP     EXIST   CL\n"
                        "4        var6  6        5       EXIST   CL\n"
                        "3        var3  7        2       EXIST   CL\n"
                        "5        var5  6        NOP     EXIST   CL\n"
                        "3        var6  NOP      1       EXIST   CL\n"
                        "5        var7  5        2       EXIST   CL\n"
                        "4        var7  5        NOP     EXIST   CL\n";

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 20;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  prepare_query_param(trans_version_range);

  res_iter.reset();
  OK(rowkeys_iter.from(rowkeys_str1));
  prepare_rowkeys(rowkeys_iter, rowkeys);
  OK(sstable_.multi_get(param_, context_, rowkeys, multi_getter));
  OK(res_iter.from(result1));
  ASSERT_TRUE(res_iter.equals(*multi_getter));
  multi_getter->~ObStoreRowIterator();
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  GCONF._enable_sparse_row = true;
  system("rm -rf test_multi_version_sparse_sstable_multi_get.log");
  OB_LOGGER.set_file_name("test_multi_version_sparse_sstable_multi_get.log");
  STORAGE_LOG(INFO, "begin unittest: test_multi_version_sparse_sstable_multi_get");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
