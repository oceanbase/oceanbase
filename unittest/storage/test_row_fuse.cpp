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
#include <gmock/gmock.h>
#define private public
#define protected public
#include "storage/ob_row_fuse.h"
#include "lib/allocator/page_arena.h"
#include "mockcontainer/mock_ob_iterator.h"


namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;

namespace unittest
{
const static int64_t DEF_COL_NUM = 4;

void check_row_fuse(const char *input, const char *expect)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("StTemp");
  //const ObStoreRow *expect_row = NULL;
  const ObStoreRow *input_row = NULL;
  ObStoreRow *result = NULL;
  ObMockStoreRowIterator iter;
  ObMockStoreRowIterator expect_iter;
  if (OB_FAIL(malloc_store_row(allocator, OB_ROW_MAX_COLUMNS_COUNT, result))) {
    STORAGE_LOG(WARN, "no memory");
  } else {
    SCOPED_TRACE(expect);
    SCOPED_TRACE(input);
    result->row_val_.count_ = 0;
    result->flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
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
        STORAGE_LOG(INFO, "fuse row", K(ret), KPC(input_row), KPC(result));
      } else {
        break;
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    // check result
    ASSERT_EQ(OB_SUCCESS, expect_iter.from(expect));
    //ASSERT_EQ(OB_SUCCESS, iter.get_next_row(expect_row));
    //ASSERT_TRUE(expect_iter.equals(*expect_row));
    STORAGE_LOG(INFO, "result row", K(ret), KPC(result));
    ASSERT_TRUE(expect_iter.equals(*result));

    free_store_row(allocator, result);
  }
}

void check_simple_row_fuse(const char *input, const char *expect)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("StTemp");
  //const ObStoreRow *expect_row = NULL;
  const ObStoreRow *input_row = NULL;
  ObStoreRow *result = NULL;
  ObMockStoreRowIterator iter;
  ObMockStoreRowIterator expect_iter;
  if (OB_FAIL(malloc_store_row(allocator, OB_ROW_MAX_COLUMNS_COUNT, result))) {
    STORAGE_LOG(WARN, "no memory");
  } else {
    SCOPED_TRACE(expect);
    SCOPED_TRACE(input);
    result->row_val_.count_ = 0;
    result->flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
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
    //ASSERT_EQ(OB_SUCCESS, iter.get_next_row(expect_row));
    //ASSERT_TRUE(expect_iter.equals(*expect_row));
    ASSERT_TRUE(expect_iter.equals(*result));

    free_store_row(allocator, result);
  }
}


TEST(ObRowFuseTest, test_fuse_nomal)
{
  const char *input = "int var  num  int int\n"
                      "1   NOP  NOP  NOP NOP\n"
                      "2   var2 NOP  NOP NULL\n"
                      "3   var3 3.33 NOP 5555\n"
                      "4   var4 NOP  NOP NOP\n"
                      "5   var9 9.99 MAX NULL\n";

  const char *expect = "int var  num  int int\n"
                       "1   var2 3.33 MAX NULL\n";
  check_row_fuse(input, expect);
  check_simple_row_fuse(input, expect);
}

TEST(ObRowFuseTest, test_fuse_delete)
{
  const char *input1 = "flag   int var  num  int int\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n";

  const char *expect1 = "flag   int var  num  int int\n"
                        "DELETE NOP NOP  NOP  NOP NOP\n";
  check_row_fuse(input1, expect1);
  check_simple_row_fuse(input1, expect1);

  const char *input2 = "flag   int var  num  int int\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n";

  const char *expect2 = "flag   int var  num  int int\n"
                        "EXIST  1   var2 NOP  NOP null\n";
  check_row_fuse(input2, expect2);
  check_simple_row_fuse(input2, expect2);

  const char *input3 = "flag   int var  num  int int\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n";

  const char *expect3 = "int var  num  int int\n"
                        "1   var2 3.33 MAX NULL\n";
  check_row_fuse(input3, expect3);
  check_simple_row_fuse(input3, expect3);
}

TEST(ObRowFuseTest, test_fuse_empty)
{
  const char *input1 = "flag   int var  num  int int\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "EMPTY  NOP NOP  NOP  NOP NOP\n"
                       "DELETE NOP NOP  NOP  NOP NOP\n"
                       "EXIST  1   NOP  NOP  NOP NOP\n"
                       "EXIST  2   var2 NOP  NOP NULL\n"
                       "EXIST  3   var3 3.33 NOP 5555\n"
                       "EXIST  4   var4 NOP  NOP NOP\n"
                       "EXIST  9   var9 9.99 MAX NULL\n";

  const char *expect1 = "flag   int var  num  int int\n"
                        "DELETE NOP NOP  NOP  NOP NOP\n";
  check_row_fuse(input1, expect1);
  check_simple_row_fuse(input1, expect1);

  const char *input2 = "flag   int var  num  int int\n"
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

  const char *expect2 = "flag   int var  num  int int\n"
                        "EXIST  1   var2 NOP  NOP NULL\n";
  check_row_fuse(input2, expect2);
  check_simple_row_fuse(input2, expect2);

  const char *input3 = "flag   int var  num  int int\n"
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

  const char *expect3 = "flag  int var  num  int int\n"
                        "EXIST 1   var2 3.33 MAX NULL\n";
  check_row_fuse(input3, expect3);
  check_simple_row_fuse(input3, expect3);
}

TEST(ObRowFuseTest, test_invalid_input_row)
{
  common::ObArenaAllocator allocator("StTemp");
  char *input_ptr = (char *)allocator.alloc(OB_ROW_MAX_COLUMNS_COUNT * sizeof(ObObj) + sizeof(ObStoreRow));
  char *result_ptr = (char *)allocator.alloc(OB_ROW_MAX_COLUMNS_COUNT * sizeof(ObObj) + sizeof(ObStoreRow));
  ObStoreRow *input_row = new (input_ptr) ObStoreRow;
  ObStoreRow *result = new (result_ptr) ObStoreRow;
  new (input_ptr + sizeof(ObStoreRow)) ObObj[OB_ROW_MAX_COLUMNS_COUNT];
  new (result_ptr + sizeof(ObStoreRow)) ObObj[OB_ROW_MAX_COLUMNS_COUNT];
  ASSERT_TRUE(NULL != input_row);
  ASSERT_TRUE(NULL != result);
  input_row->row_val_.cells_ = (ObObj *)(input_row + 1);
  input_row->row_val_.count_ = OB_ROW_MAX_COLUMNS_COUNT;
  input_row->flag_ = -1; // set invalid flag

  result->row_val_.count_ = 0;
  result->flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
  ObNopPos nop_pos;
  bool final_result = false;
  ASSERT_EQ(OB_SUCCESS, nop_pos.init(allocator, 256));
  ASSERT_EQ(OB_INVALID_ARGUMENT , ObRowFuse::fuse_row(*input_row, *result, nop_pos, final_result, NULL));
  ASSERT_EQ(OB_INVALID_ARGUMENT , ObRowFuse::fuse_row(*input_row, *result, nop_pos, final_result));
}

TEST(ObRowFuseTest, test_invalid_argument)
{
  common::ObArenaAllocator allocator("StTemp");
  char *input_ptr = (char *)allocator.alloc(DEF_COL_NUM * sizeof(ObObj) + sizeof(ObStoreRow));
  char *result_ptr = (char *)allocator.alloc(DEF_COL_NUM * sizeof(ObObj) + sizeof(ObStoreRow));
  ObStoreRow *input_row = new (input_ptr) ObStoreRow;
  ObStoreRow *result = new (result_ptr) ObStoreRow;
  new (input_ptr + sizeof(ObStoreRow)) ObObj[DEF_COL_NUM];
  new (result_ptr + sizeof(ObStoreRow)) ObObj[DEF_COL_NUM];

  ASSERT_TRUE(NULL != input_row);
  ASSERT_TRUE(NULL != result);

  input_row->row_val_.cells_ = (ObObj *)(input_row + 1);
  input_row->row_val_.count_ = DEF_COL_NUM;
  result->row_val_.cells_ = NULL;
  result->row_val_.count_ = 0;
  ObNopPos nop_pos;
  bool final_result = false;
  ASSERT_EQ(OB_SUCCESS, nop_pos.init(allocator, 256));
  ASSERT_EQ(OB_INVALID_ARGUMENT , ObRowFuse::fuse_row(*input_row, *result, nop_pos, final_result, NULL));
  ASSERT_EQ(OB_INVALID_ARGUMENT , ObRowFuse::fuse_row(*input_row, *result, nop_pos, final_result));
}

TEST(ObRowFuseTest, test_err_unexpected)
{
  common::ObArenaAllocator allocator("StTemp");
  char *input_ptr = (char *)allocator.alloc(DEF_COL_NUM * sizeof(ObObj) + sizeof(ObStoreRow));
  char *result_ptr = (char *)allocator.alloc(DEF_COL_NUM * sizeof(ObObj) + sizeof(ObStoreRow));
  ObStoreRow *input_row = new (input_ptr) ObStoreRow;
  ObStoreRow *result = new (result_ptr) ObStoreRow;
  new (input_ptr + sizeof(ObStoreRow)) ObObj[DEF_COL_NUM];
  new (result_ptr + sizeof(ObStoreRow)) ObObj[DEF_COL_NUM];

  ASSERT_TRUE(NULL != input_row);
  ASSERT_TRUE(NULL != result);
  input_row->row_val_.cells_ = (ObObj *)(input_row + 1);
  input_row->row_val_.count_ = DEF_COL_NUM;
  result->row_val_.cells_ = (ObObj *)(result + 1);
  result->row_val_.count_ = DEF_COL_NUM;

  ObNopPos nop_pos;
  ASSERT_EQ(OB_SUCCESS, nop_pos.init(allocator, 256));
}

TEST(ObRowFuseTest, test_nop_pos)
{
  ObNopPos nop_pos;
  common::ObArenaAllocator allocator("StTemp");
  // ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, nop_pos.init(LONG_MAX));
  ASSERT_EQ(OB_SUCCESS, nop_pos.init(allocator, 256));
}

void check_fuse_row_flag(
    const uint8_t old_flag,
    const uint8_t input_flag,
    const uint8_t expect_flag)
{
  ObStoreRow row;
  row.flag_ = old_flag;
  row.flag_.fuse_flag(ObDmlRowFlag(input_flag));
  if (expect_flag != row.flag_.whole_flag_) {
    STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "not equal", K(old_flag), K(input_flag),
        K(expect_flag), "fused_flag", row.flag_);
  }
}

TEST(ObRowFuseTest, test_row_flag_fuse)
{
  check_fuse_row_flag(DF_UPDATE, DF_INSERT, DF_INSERT);
  check_fuse_row_flag(DF_INSERT, DF_INSERT, DF_INSERT);
  check_fuse_row_flag(DF_LOCK, DF_INSERT, DF_INSERT);

  check_fuse_row_flag(DF_UPDATE, DF_UPDATE, DF_UPDATE);
  check_fuse_row_flag(DF_INSERT, DF_UPDATE, DF_INSERT);
  check_fuse_row_flag(DF_LOCK, DF_UPDATE, DF_LOCK);

  check_fuse_row_flag(DF_UPDATE, DF_DELETE, DF_UPDATE);
  check_fuse_row_flag(DF_INSERT, DF_DELETE, DF_INSERT);
  check_fuse_row_flag(DF_LOCK, DF_DELETE, DF_LOCK);

  check_fuse_row_flag(DF_UPDATE, DF_DELETE, DF_UPDATE);
  check_fuse_row_flag(DF_INSERT, DF_DELETE, DF_INSERT);
  check_fuse_row_flag(DF_LOCK, DF_DELETE, DF_LOCK);

  check_fuse_row_flag(DF_DELETE | ObDmlRowFlag::OB_FLAG_TYPE_MASK, DF_DELETE, DF_DELETE);
  check_fuse_row_flag(DF_DELETE, DF_DELETE | ObDmlRowFlag::OB_FLAG_TYPE_MASK, DF_DELETE);
  check_fuse_row_flag(DF_DELETE | ObDmlRowFlag::OB_FLAG_TYPE_MASK,
      DF_DELETE | ObDmlRowFlag::OB_FLAG_TYPE_MASK,
      DF_DELETE | ObDmlRowFlag::OB_FLAG_TYPE_MASK);
}


} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_row_fuse.log");
  OB_LOGGER.set_file_name("test_row_fuse.log");
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_row_fuse");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
