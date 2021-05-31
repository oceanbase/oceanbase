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

#include "storage/ob_row_fuse.h"
#include "lib/allocator/page_arena.h"
#include "mockcontainer/mock_ob_iterator.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>

namespace oceanbase {
using namespace common;
using namespace storage;

namespace unittest {
const static int64_t DEF_COL_NUM = 4;

void check_row_fuse(const char* input, const char* expect, const common::ObIArray<int32_t>* col_idxs = NULL,
    bool* column_changed = NULL, int64_t column_count = 0)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::OB_ST_TEMP);
  // const ObStoreRow *expect_row = NULL;
  const ObStoreRow* input_row = NULL;
  ObStoreRow* result = NULL;
  ObMockStoreRowIterator iter;
  ObMockStoreRowIterator expect_iter;
  if (OB_FAIL(malloc_store_row(allocator, OB_ROW_MAX_COLUMNS_COUNT, result))) {
    STORAGE_LOG(WARN, "no memory");
  } else {
    SCOPED_TRACE(expect);
    SCOPED_TRACE(input);
    result->row_val_.count_ = 0;
    result->flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
    ObNopPos nop_pos;
    bool final_result = false;

    // run row fuse
    ASSERT_EQ(OB_SUCCESS, iter.from(input));
    if (OB_SUCCESS != (ret = nop_pos.init(allocator, 256))) {
      STORAGE_LOG(WARN, "nop position initialize error", K(ret));
    }
    while (OB_SUCCESS == ret && !final_result) {
      if (OB_ITER_END != iter.get_next_row(input_row)) {
        ret = ObRowFuse::fuse_row(*input_row, *result, nop_pos, final_result, col_idxs, column_changed, column_count);
      } else {
        break;
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    // check result
    ASSERT_EQ(OB_SUCCESS, expect_iter.from(expect));
    // ASSERT_EQ(OB_SUCCESS, iter.get_next_row(expect_row));
    // ASSERT_TRUE(expect_iter.equals(*expect_row));
    ASSERT_TRUE(expect_iter.equals(*result));

    free_store_row(allocator, result);
  }
}

void check_simple_row_fuse(const char* input, const char* expect)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::OB_ST_TEMP);
  // const ObStoreRow *expect_row = NULL;
  const ObStoreRow* input_row = NULL;
  ObStoreRow* result = NULL;
  ObMockStoreRowIterator iter;
  ObMockStoreRowIterator expect_iter;
  if (OB_FAIL(malloc_store_row(allocator, OB_ROW_MAX_COLUMNS_COUNT, result))) {
    STORAGE_LOG(WARN, "no memory");
  } else {
    SCOPED_TRACE(expect);
    SCOPED_TRACE(input);
    result->row_val_.count_ = 0;
    result->flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
    ObNopPos nop_pos;
    bool final_result = false;

    // run row fuse
    ASSERT_EQ(OB_SUCCESS, iter.from(input));
    if (OB_SUCCESS != (ret = nop_pos.init(allocator, 256))) {
      STORAGE_LOG(WARN, "nop position initialize error", K(ret));
    }
    while (OB_SUCCESS == ret && !final_result) {
      if (OB_ITER_END != iter.get_next_row(input_row)) {
        ret = ObRowFuse::fuse_row(*input_row, *result, nop_pos, final_result);
      } else {
        break;
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    // check result
    ASSERT_EQ(OB_SUCCESS, expect_iter.from(expect));
    // ASSERT_EQ(OB_SUCCESS, iter.get_next_row(expect_row));
    // ASSERT_TRUE(expect_iter.equals(*expect_row));
    ASSERT_TRUE(expect_iter.equals(*result));

    free_store_row(allocator, result);
  }
}

void check_sparse_row_fuse(const char* input_row_ch, const char* input_column_id_ch, int* col_cnt,
    const char* expect_row_ch, const char* expect_column_id_ch)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::OB_ST_TEMP);
  // const ObStoreRow *expect_row = NULL;
  const ObStoreRow* input = NULL;
  const ObStoreRow* column_id = NULL;
  const ObStoreRow* expect = NULL;
  const ObStoreRow* expect_column_id = NULL;
  ObMockStoreRowIterator input_row_iter;
  ObMockStoreRowIterator input_column_id_iter;
  ObMockStoreRowIterator expect_iter;
  ObMockStoreRowIterator expect_column_id_expect_iter;

  SCOPED_TRACE(input_row_ch);
  SCOPED_TRACE(input_column_id_ch);
  SCOPED_TRACE(expect_row_ch);
  SCOPED_TRACE(expect_column_id_ch);

  bool final_result = false;
  ObStoreRow row;
  uint16_t column_id_array[OB_ROW_MAX_COLUMNS_COUNT];

  ObStoreRow result;
  uint16_t result_column_id_array[OB_ROW_MAX_COLUMNS_COUNT];
  result.column_ids_ = result_column_id_array;
  oceanbase::common::ObObj objs[OB_ROW_MAX_COLUMNS_COUNT];
  result.row_val_.cells_ = objs;
  result.row_val_.count_ = 0;
  result.capacity_ = OB_ROW_MAX_COLUMNS_COUNT;
  result.flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;

  // run row fuse
  ASSERT_EQ(OB_SUCCESS, input_row_iter.from(input_row_ch));
  ASSERT_EQ(OB_SUCCESS, input_column_id_iter.from(input_column_id_ch));
  int row_index = 0;
  ObFixedBitSet<OB_ALL_MAX_COLUMN_ID> bit_set;
  while (OB_SUCCESS == ret && !final_result) {
    if (OB_ITER_END != input_row_iter.get_next_row(input) &&
        OB_ITER_END != input_column_id_iter.get_next_row(column_id)) {
      row = *input;
      row.is_sparse_row_ = true;
      row.column_ids_ = column_id_array;
      for (int i = 0; i < col_cnt[row_index]; ++i) {
        row.column_ids_[i] = column_id->row_val_.cells_[i].get_int();
        STORAGE_LOG(INFO, "input row", K(row_index), K(i), K(column_id->row_val_.cells_[i]), K(row.column_ids_[i]));
      }
      row.row_val_.count_ = col_cnt[row_index];
      ret = ObRowFuse::fuse_sparse_row(row, result, bit_set, final_result);
      for (int i = 0; i < result.row_val_.count_; ++i) {
        STORAGE_LOG(INFO, "result row", K(row_index), K(i), K(result.row_val_.cells_[i]), K(result.column_ids_[i]));
      }
      row_index++;
    } else {
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 0; i < result.row_val_.count_; ++i) {
    STORAGE_LOG(INFO, "result", K(i), K(result.row_val_.cells_[i]), K(result.column_ids_[i]));
  }
  // check result
  ASSERT_EQ(OB_SUCCESS, expect_iter.from(expect_row_ch));
  expect_iter.get_next_row(expect);
  ASSERT_EQ(OB_SUCCESS, expect_column_id_expect_iter.from(expect_column_id_ch));
  expect_column_id_expect_iter.get_next_row(expect_column_id);
  for (int i = 0; i < expect->row_val_.count_; ++i) {
    ASSERT_TRUE(expect->row_val_.cells_[i] == result.row_val_.cells_[i]);
    ASSERT_TRUE(expect_column_id->row_val_.cells_[i].get_int() == result.column_ids_[i]);
  }
}

TEST(TestSparseRowFuse, test_sparse_row_fuse_same)
{
  const char* input = "int var  num  int int\n"
                      "1   var7  1    7   9\n"
                      "2   var2  6    4   7\n"
                      "3   var3 3.33  13 231\n"
                      "4   var4   5   2   4\n"
                      "5   var9 9.99  MAX  3\n";
  const char* input_column = "int int int int int\n"
                             "1  4  7  8  9\n"
                             "1  4  7  8  9\n"
                             "1  4  7  8  9\n"
                             "1  4  7  8  9\n"
                             "1  4  7  8  9\n";

  int col_cnt[] = {5, 5, 5, 5, 5};

  const char* expect = "int var  num  int int\n"
                       "1   var7  1    7   9\n";

  const char* expect_column = "int int  int  int int\n"
                              "1    4    7    8   9\n";
  check_sparse_row_fuse(input, input_column, col_cnt, expect, expect_column);
}

TEST(TestSparseRowFuse, test_sparse_row_fuse_1)
{
  const char* input = "int var  num  int int\n"
                      "1   var7  1    7   9\n"
                      "2   var2  6    4   7\n"
                      "3   var3  NOP NOP NOP\n"
                      "4   var4   5   4  NOP\n";
  const char* input_column = "int int  int  int int\n"
                             "1   2    7   8   9\n"
                             "1   4    7   8   9\n"
                             "6   10   NOP NOP NOP\n"
                             "1   14   7   9   NOP\n";
  int col_cnt[] = {5, 5, 2, 4};

  const char* expect = "int var  num  int int var  int  var  var\n"
                       "1   var7  1    7   9  var2  3  var3  var4\n";

  const char* expect_column = "int int  int  int int int int int int\n"
                              "1    2    7    8   9   4   6  10   14\n";
  check_sparse_row_fuse(input, input_column, col_cnt, expect, expect_column);
}

TEST(TestSparseRowFuse, test_sparse_row_fuse_2)
{
  const char* input = "int var  num  int int\n"
                      "1   NOP  NOP  NOP NOP\n"
                      "2   var2  6    4   7\n"
                      "3   var3 NOP  NOP NOP\n"
                      "4   var4   5   4  NOP\n";
  const char* input_column = "int int  int  int int\n"
                             "1 NOP NOP NOP NOP\n"
                             "1  4   7   8   9\n"
                             "6 10  NOP NOP NOP\n"
                             "1 14   7   9  NOP\n";
  int col_cnt[] = {1, 5, 2, 4};
  const char* expect = "int var  num  int int int var  var \n"
                       "1   var2  6    4   7   3  var3 var4 \n";

  const char* expect_column = "int int  int  int int int int int\n"
                              "1    4    7    8   9   6   10  14\n";
  check_sparse_row_fuse(input, input_column, col_cnt, expect, expect_column);
}

TEST(TestSparseRowFuse, test_sparse_row_fuse_3)
{
  const char* input = "int var  num  int int\n"
                      "1   NOP  NOP  NOP NOP\n"
                      "2   NOP  NOP  NOP NOP\n"
                      "3   var3 NOP  NOP NOP\n"
                      "4   var4   5   4  NOP\n";
  const char* input_column = "int int  int  int int\n"
                             "1 NOP  NOP  NOP NOP\n"
                             "5 NOP  NOP  NOP NOP\n"
                             "6  10  NOP  NOP NOP\n"
                             "3  14   7    9  NOP\n";
  int col_cnt[] = {1, 1, 2, 4};
  const char* expect = "int int  int  var  int var  num int\n"
                       "1    2    3   var3  4  var4  5   4 \n";

  const char* expect_column = "int int  int  int int int int int\n"
                              "1    5    6    10  3   14  7   9\n";
  check_sparse_row_fuse(input, input_column, col_cnt, expect, expect_column);
}

TEST(TestSparseRowFuse, test_sparse_row_fuse_delete_1)
{
  const char* input = "flag int var  num  int int\n"
                      "EXIST 1   var7  1    7   9\n"
                      "DELETE NOP NOP NOP NOP NOP\n"
                      "EXIST 3   var3  NOP NOP  NOP\n"
                      "EXIST 4   var4   5   4  NOP\n";
  const char* input_column = "int int  int  int int\n"
                             "1  2  7   8   9\n"
                             "NOP NOP NOP NOP NOP\n"
                             "6 10 NOP NOP NOP\n"
                             "1 14  7   9  NOP\n";
  int col_cnt[] = {5, 0, 2, 4};
  const char* expect = "flag  int var num int int\n"
                       "EXIST  1  var7 1   7   9\n";

  const char* expect_column = "int int int int int\n"
                              " 1   2   7   8   9\n";
  check_sparse_row_fuse(input, input_column, col_cnt, expect, expect_column);
}

TEST(TestSparseRowFuse, test_sparse_row_fuse_delete_2)
{
  const char* input = "flag int var  num  int int\n"
                      "EXIST 1   var7  1    7   9\n"
                      "EXIST 3   var3  NOP NOP  NOP\n"
                      "DELETE NOP NOP NOP NOP NOP\n"
                      "EXIST 4   var4   5   4  NOP\n";
  const char* input_column = "int int  int  int int\n"
                             "1  2  7   8   9\n"
                             "6 10 NOP NOP NOP\n"
                             "NOP NOP NOP NOP NOP\n"
                             "1 14  7   9  NOP\n";
  int col_cnt[] = {5, 2, 0, 4};
  const char* expect = "flag  int var num int int int var\n"
                       "EXIST  1  var7 1   7   9   3  var3\n";

  const char* expect_column = "int int int int int int int\n"
                              " 1   2   7   8   9  6  10\n";
  check_sparse_row_fuse(input, input_column, col_cnt, expect, expect_column);
}

TEST(ObRowFuseTest, test_fuse_nomal)
{
  const char* input = "int var  num  int int\n"
                      "1   NOP  NOP  NOP NOP\n"
                      "2   var2 NOP  NOP NULL\n"
                      "3   var3 3.33 NOP 5555\n"
                      "4   var4 NOP  NOP NOP\n"
                      "5   var9 9.99 MAX NULL\n";

  const char* expect = "int var  num  int int\n"
                       "1   var2 3.33 MAX NULL\n";
  check_row_fuse(input, expect);
  check_simple_row_fuse(input, expect);
}

TEST(ObRowFuseTest, test_fuse_delete)
{
  const char* input1 = "flag   int var  num  int int\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n";

  const char* expect1 = "flag   int var  num  int int\n"
                        "DELETE NOP NOP  NOP  NOP NOP\n";
  check_row_fuse(input1, expect1);
  check_simple_row_fuse(input1, expect1);

  const char* input2 = "flag   int var  num  int int\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n";

  const char* expect2 = "flag   int var  num  int int\n"
                        "EXIST  1   var2 NOP  NOP null\n";
  check_row_fuse(input2, expect2);
  check_simple_row_fuse(input2, expect2);

  const char* input3 = "flag   int var  num  int int\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n";

  const char* expect3 = "int var  num  int int\n"
                        "1   var2 3.33 MAX NULL\n";
  check_row_fuse(input3, expect3);
  check_simple_row_fuse(input3, expect3);
}

TEST(ObRowFuseTest, test_fuse_empty)
{
  const char* input1 = "flag   int var  num  int int\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n";

  const char* expect1 = "flag   int var  num  int int\n"
                        "DELETE NOP NOP  NOP  NOP NOP\n";
  check_row_fuse(input1, expect1);
  check_simple_row_fuse(input1, expect1);

  const char* input2 = "flag   int var  num  int int\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n";

  const char* expect2 = "flag   int var  num  int int\n"
                        "EXIST  1   var2 NOP  NOP NULL\n";
  check_row_fuse(input2, expect2);
  check_simple_row_fuse(input2, expect2);

  const char* input3 = "flag   int var  num  int int\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n";

  const char* expect3 = "flag  int var  num  int int\n"
                        "EXIST 1   var2 3.33 MAX NULL\n";
  check_row_fuse(input3, expect3);
  check_simple_row_fuse(input3, expect3);
}

TEST(ObRowFuseTest, test_with_idxs)
{
  common::ObArray<int32_t> col_idxs;
  col_idxs.push_back(1);
  col_idxs.push_back(3);
  col_idxs.push_back(4);
  bool column_changed[col_idxs.count()];
  memset(column_changed, 0, col_idxs.count() * sizeof(bool));

  const char* input1 = "flag   int var  num  int int\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n";

  const char* expect1 = "flag   int var  num  int int\n"
                        "DELETE NOP NOP  NOP  NOP NOP\n";

  check_row_fuse(input1, expect1, &col_idxs);

  const char* input2 = "flag   int var  num  int int\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n";

  const char* expect2 = "flag  var   int int\n"
                        "EXIST var2  NOP NULL\n";

  check_row_fuse(input2, expect2, &col_idxs, column_changed, col_idxs.count());
  for (int64_t i = 0; i < col_idxs.count(); i++) {
    if (1 == i) {
      ASSERT_EQ(false, column_changed[i]);
    } else {
      ASSERT_EQ(true, column_changed[i]);
    }
  }
  memset(column_changed, 0, col_idxs.count() * sizeof(bool));

  const char* input3 = "flag   int var  num  int int\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n";

  const char* expect3 = "flag  var  int int\n"
                        "EXIST var2 MAX NULL\n";
  check_row_fuse(input3, expect3, &col_idxs, column_changed, col_idxs.count());
  for (int64_t i = 0; i < col_idxs.count(); i++) {
    ASSERT_EQ(true, column_changed[i]);
  }

  memset(column_changed, 0, col_idxs.count() * sizeof(bool));

  const char* input4 = "from_base flag   int var  num  int int\n"
                       "FALSE     EXIST  1   NOP  NOP  NOP NOP\n"
                       "FALSE     EXIST  2   var2 NOP  NOP NULL\n"
                       "TRUE      EXIST  NOP NOP  NOP  MAX NOP\n";

  const char* expect4 = "flag  var  int int\n"
                        "EXIST var2 MAX NULL\n";
  check_row_fuse(input4, expect4, &col_idxs, column_changed, col_idxs.count());
  for (int64_t i = 0; i < col_idxs.count(); i++) {
    if (1 == i) {
      ASSERT_EQ(false, column_changed[i]);
    } else {
      ASSERT_EQ(true, column_changed[i]);
    }
  }
}

TEST(ObRowFuseTest, test_invalid_input_row)
{
  common::ObArenaAllocator allocator(ObModIds::OB_ST_TEMP);
  char* input_ptr = (char*)allocator.alloc(OB_ROW_MAX_COLUMNS_COUNT * sizeof(ObObj) + sizeof(ObStoreRow));
  char* result_ptr = (char*)allocator.alloc(OB_ROW_MAX_COLUMNS_COUNT * sizeof(ObObj) + sizeof(ObStoreRow));
  ObStoreRow* input_row = new (input_ptr) ObStoreRow;
  ObStoreRow* result = new (result_ptr) ObStoreRow;
  new (input_ptr + sizeof(ObStoreRow)) ObObj[OB_ROW_MAX_COLUMNS_COUNT];
  new (result_ptr + sizeof(ObStoreRow)) ObObj[OB_ROW_MAX_COLUMNS_COUNT];
  ASSERT_TRUE(NULL != input_row);
  ASSERT_TRUE(NULL != result);
  input_row->row_val_.cells_ = (ObObj*)(input_row + 1);
  input_row->row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
  input_row->flag_ = -1;  // set invalid flag

  result->row_val_.count_ = 0;
  result->flag_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  ObNopPos nop_pos;
  bool final_result = false;
  ASSERT_EQ(OB_SUCCESS, nop_pos.init(allocator, 256));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObRowFuse::fuse_row(*input_row, *result, nop_pos, final_result, NULL, NULL, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObRowFuse::fuse_row(*input_row, *result, nop_pos, final_result));
}

TEST(ObRowFuseTest, test_invalid_argument)
{
  common::ObArenaAllocator allocator(ObModIds::OB_ST_TEMP);
  char* input_ptr = (char*)allocator.alloc(DEF_COL_NUM * sizeof(ObObj) + sizeof(ObStoreRow));
  char* result_ptr = (char*)allocator.alloc(DEF_COL_NUM * sizeof(ObObj) + sizeof(ObStoreRow));
  ObStoreRow* input_row = new (input_ptr) ObStoreRow;
  ObStoreRow* result = new (result_ptr) ObStoreRow;
  new (input_ptr + sizeof(ObStoreRow)) ObObj[DEF_COL_NUM];
  new (result_ptr + sizeof(ObStoreRow)) ObObj[DEF_COL_NUM];

  ASSERT_TRUE(NULL != input_row);
  ASSERT_TRUE(NULL != result);

  input_row->row_val_.cells_ = (ObObj*)(input_row + 1);
  input_row->row_val_.count_ = DEF_COL_NUM;
  result->row_val_.cells_ = NULL;
  result->row_val_.count_ = 0;
  ObNopPos nop_pos;
  bool final_result = false;
  ASSERT_EQ(OB_SUCCESS, nop_pos.init(allocator, 256));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObRowFuse::fuse_row(*input_row, *result, nop_pos, final_result, NULL, NULL, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObRowFuse::fuse_row(*input_row, *result, nop_pos, final_result));
}

TEST(ObRowFuseTest, test_err_unexpected)
{
  common::ObArenaAllocator allocator(ObModIds::OB_ST_TEMP);
  char* input_ptr = (char*)allocator.alloc(DEF_COL_NUM * sizeof(ObObj) + sizeof(ObStoreRow));
  char* result_ptr = (char*)allocator.alloc(DEF_COL_NUM * sizeof(ObObj) + sizeof(ObStoreRow));
  ObStoreRow* input_row = new (input_ptr) ObStoreRow;
  ObStoreRow* result = new (result_ptr) ObStoreRow;
  new (input_ptr + sizeof(ObStoreRow)) ObObj[DEF_COL_NUM];
  new (result_ptr + sizeof(ObStoreRow)) ObObj[DEF_COL_NUM];

  ASSERT_TRUE(NULL != input_row);
  ASSERT_TRUE(NULL != result);
  input_row->row_val_.cells_ = (ObObj*)(input_row + 1);
  input_row->row_val_.count_ = DEF_COL_NUM;
  result->row_val_.cells_ = (ObObj*)(result + 1);
  result->row_val_.count_ = DEF_COL_NUM;

  ObNopPos nop_pos;
  ASSERT_EQ(OB_SUCCESS, nop_pos.init(allocator, 256));
}

TEST(ObRowFuseTest, test_nop_pos)
{
  ObNopPos nop_pos;
  common::ObArenaAllocator allocator(ObModIds::OB_ST_TEMP);
  // ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, nop_pos.init(LONG_MAX));
  ASSERT_EQ(OB_SUCCESS, nop_pos.init(allocator, 256));
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -rf test_row_fuse.log");
  OB_LOGGER.set_file_name("test_row_fuse.log");
  OB_LOGGER.set_log_level("WARN");
  CLOG_LOG(INFO, "begin unittest: test_row_fuse");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
