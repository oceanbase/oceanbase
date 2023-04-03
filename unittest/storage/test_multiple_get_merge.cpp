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
#include "storage/mock_ob_ss_store.h"
#include "storage/access/ob_multiple_get_merge.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/allocator/page_arena.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "mockcontainer/mock_ob_iterator.h"
#include "mockcontainer/mock_misc_for_ps_test.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace ::testing;

namespace unittest
{
class ObMultipleGetMergeTest : public ::testing::Test
{
public:

  const static int OUTPUT_COL_NUM = 4;
  const static int ITERATOR_NUM = 4;
  const static int ROWKEY_NUM = 1;
  const static int CELL_NUM = 4;
  const static int INVALID_ROWKEY_NUM = -1;
  const static int INVALID_CELL_NUM = -1;
  const static int INVALID_COL_INDEX = -1;

public:
  ObMultipleGetMergeTest() : allocator_(ObModIds::TEST) {}
  virtual ~ObMultipleGetMergeTest() {}
  void generate_query_rowkeys(ObIArray<ObStoreRowkey> &rowkeys);
  void prepare_input(const ObIArray<const char *> &inputs, ObArray<ObMockStoreRowIterator *> &iters);
  void prepare_store(const ObIArray<ObMockStoreRowIterator *> &iters);
  void check_result(const char *output, const ObIArray<ObStoreRowkey> &rowkeys);
  void SetUp()
  {
    for (int16_t i = 0; i < OUTPUT_COL_NUM; ++i) {
      ASSERT_EQ(OB_SUCCESS, out_idxs_.push_back(i));
    }
    main_table_param_.reset();
    ASSERT_EQ(OB_SUCCESS, main_table_param_.out_col_desc_param_.init());
    main_table_param_.iter_param_.rowkey_cnt_ = ROWKEY_NUM;
    main_table_param_.iter_param_.output_project_ = &out_idxs_;
    main_table_param_.iter_param_.out_cols_ = &main_table_param_.out_col_desc_param_.get_col_descs();
    ObColDesc col_desc;
    col_desc.col_id_ = 1;
    col_desc.col_type_ = ObObjMeta();
    ASSERT_EQ(OB_SUCCESS, main_table_param_.out_col_desc_param_.push_back(col_desc));

    main_table_ctx_.allocator_ = &allocator_;
    main_table_ctx_.stmt_allocator_ = &allocator_;
    main_table_ctx_.query_flag_ = ObQueryFlag();
    main_table_ctx_.store_ctx_ = &store_ctx_;
    main_table_ctx_.is_inited_ = true;
  }
  void TearDown()
  {
    out_idxs_.reset();
    allocator_.clear();
    for (int64_t i = 0; i < stores_.count(); ++i) {
      delete stores_.at(i);
    }
    stores_.reset();
  }
protected:
  ObArenaAllocator allocator_;
  ColIdxArray out_idxs_;
  ObStoreCtx store_ctx_;
  ObTableAccessParam main_table_param_;
  ObTableAccessContext main_table_ctx_;
  common::ObSEArray<ObIStore *, common::MAX_TABLE_CNT_IN_STORAGE> stores_;
  ObMultipleMerge::MergeIterators iters_;
  ObObj cells_[OUTPUT_COL_NUM];
};

// generate fake rowkeys
void ObMultipleGetMergeTest::generate_query_rowkeys(ObIArray<ObStoreRowkey> &rowkeys)
{
  ObStoreRowkey rowkey;
  cells_[0].set_int(1L);
  rowkey.assign(cells_, ROWKEY_NUM);
  ASSERT_EQ(OB_SUCCESS, rowkeys.push_back(rowkey));
}

void ObMultipleGetMergeTest::prepare_input(const ObIArray<const char *> &input, ObArray<ObMockStoreRowIterator *> &iters)
{
  for (int i = 0; i < input.count(); ++i) {
    ObMockStoreRowIterator *iter = new ObMockStoreRowIterator();
    iter->from(input.at(i));
    ASSERT_EQ(OB_SUCCESS, iters.push_back(iter));
  }
}

void ObMultipleGetMergeTest::check_result(const char *output, const ObIArray<ObStoreRowkey> &rowkeys)
{
  ObMockQueryRowIterator expect_iter;
  expect_iter.from(output);
  ObMultipleGetMerge merge_iter;
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(rowkeys));
  ASSERT_TRUE(expect_iter.equals(merge_iter));
  merge_iter.reset();
}

void ObMultipleGetMergeTest::prepare_store(const ObIArray<ObMockStoreRowIterator *> &iters)
{
  stores_.reset();
  for (int i = 0; i < iters.count(); ++i) {
    MockObSSStore *store = new MockObSSStore();
    EXPECT_CALL(*store, multi_get(_, _, _, _))
          .WillOnce(DoAll(SetArgReferee<3>(static_cast<ObStoreRowIterator *>(iters.at(i))),
                Return(OB_SUCCESS)));
    ASSERT_EQ(OB_SUCCESS, stores_.push_back(store));
  }
}

TEST_F(ObMultipleGetMergeTest, test_zero_rowkey)
{
  ObMultipleGetMerge merge_iter;
  ObArray<ObStoreRowkey> rowkeys;
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, merge_iter.open(rowkeys));
}

TEST_F(ObMultipleGetMergeTest, test_zero_store)
{
  ObMultipleGetMerge merge_iter;
  ObStoreRow *row = NULL;
  ObArray<ObStoreRowkey> rowkeys;
  generate_query_rowkeys(rowkeys);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(rowkeys));
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(row));
}

TEST_F(ObMultipleGetMergeTest, test_one_iterator_exist)
{
  ObMultipleGetMerge merge_iter;
  ObArray<ObStoreRowkey> rowkeys;
  generate_query_rowkeys(rowkeys);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input =
      "int int var  int flag  \n"
      "1   99  test  100 EXIST \n"
      "2   3  test  100 EXIST \n"
      "3   1  test  100 EXIST \n";
  const char *output =
      "int int var  int flag  \n"
      "1   99  test  100 EXIST \n"
      "2   3  test  100 EXIST \n"
      "3   1  test  100 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkeys);
}

TEST_F(ObMultipleGetMergeTest, test_one_iterator_not_exist)
{
  ObMultipleGetMerge merge_iter;
  ObArray<ObStoreRowkey> rowkeys;
  generate_query_rowkeys(rowkeys);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input =
      "int int var  int flag  \n"
      "NOP   NOP  NOP  NOP EMPTY \n"
      "NOP   NOP  NOP  NOP EMPTY \n"
      "NOP   NOP  NOP  NOP EMPTY \n";
  const char *output = NULL;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkeys);
}

TEST_F(ObMultipleGetMergeTest, test_one_iterator_delete)
{
  ObMultipleGetMerge merge_iter;
  ObArray<ObStoreRowkey> rowkeys;
  generate_query_rowkeys(rowkeys);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input =
      "int int var  int flag  \n"
      "1   2  test  100 DELETE \n"
      "2   2  var  200 DELETE \n"
      "3   2  test2  300 DELETE \n";
  const char *output = NULL;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkeys);
}

TEST_F(ObMultipleGetMergeTest, test_one_iterator_all)
{
  ObMultipleGetMerge merge_iter;
  ObArray<ObStoreRowkey> rowkeys;
  generate_query_rowkeys(rowkeys);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input =
      "int int var  int flag  \n"
      "1   2  test  100 DELETE \n"
      "2   2  var  200 EMPTY \n"
      "3   2  test2  300 EXIST \n";
  const char *output =
      "int int var  int flag  \n"
      "3   2  test2  300 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkeys);
}

// SSTable: exist
// Memtable: update
TEST_F(ObMultipleGetMergeTest, test_multiple_iterator_exist1)
{
  ObMultipleGetMerge merge_iter;
  ObArray<ObStoreRowkey> rowkeys;
  generate_query_rowkeys(rowkeys);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input1 =
      "int int var  int flag  \n"
      "1   99  test  100 EXIST \n"
      "2   99  test1  100 EXIST \n";
  const char *input2 =
      "int int var  int flag  \n"
      "1   100 TEST1  NOP EXIST \n"
      "2   98  NOP  102 EXIST \n";
  const char *output =
      "int int var  int flag  \n"
      "1   100  TEST1  100 EXIST \n"
      "2   98  test1  102 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkeys);
}

// SSTable: empty
// Memtable: insert
TEST_F(ObMultipleGetMergeTest, test_multiple_iterator_exist2)
{
  ObMultipleGetMerge merge_iter;
  ObArray<ObStoreRowkey> rowkeys;
  generate_query_rowkeys(rowkeys);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input1 =
      "int int var  int flag  \n"
      "NOP   NOP  NOP  NOP EMPTY \n"
      "NOP   NOP  NOP  NOP EMPTY \n";
  const char *input2 =
      "int int var  int flag  \n"
      "1   100 TEST1  101 EXIST \n"
      "2   98  VAR2  102 EXIST \n";
  const char *output =
      "int int var  int flag  \n"
      "1   100  TEST1  101 EXIST \n"
      "2   98  VAR2  102 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkeys);
}

// SSTable: exist
// Minor SSTable: update
// Memtable: update
TEST_F(ObMultipleGetMergeTest, test_multiple_iterator_exist3)
{
  ObMultipleGetMerge merge_iter;
  ObArray<ObStoreRowkey> rowkeys;
  generate_query_rowkeys(rowkeys);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input1 =
      "int int var  int flag  \n"
      "1   100  VAR2  100 EXIST \n"
      "2   102  VAR3  101 EXIST \n";
  const char *input2 =
      "int int var  int flag  \n"
      "1   NOP TEST1  NOP EXIST \n"
      "2   98  NOP  102 EXIST \n";
  const char *input3 =
      "int int var  int flag  \n"
      "1   NOP TEST11  101 EXIST \n"
      "2   NOP  NOP  103 EXIST \n";
  const char *output =
      "int int var  int flag  \n"
      "1   100  TEST11  101 EXIST \n"
      "2   98  VAR3  103 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input3));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkeys);
}

// SSTable: empty
// Minor SSTable: insert
// Memtable: update
TEST_F(ObMultipleGetMergeTest, test_multiple_iterator_exist4)
{
  ObMultipleGetMerge merge_iter;
  ObArray<ObStoreRowkey> rowkeys;
  generate_query_rowkeys(rowkeys);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input1 =
      "int int var  int flag  \n"
      "NOP NOP  NOP  NOP EMPTY \n"
      "NOP  NOP  NOP  NOP EMPTY \n";
  const char *input2 =
      "int int var  int flag  \n"
      "1   101 TEST1  102 EXIST \n"
      "2   98  TEST2  103 EXIST \n";
  const char *input3 =
      "int int var  int flag  \n"
      "1   NOP TEST11  101 EXIST \n"
      "2   97  NOP  110 EXIST \n";
  const char *output =
      "int int var  int flag  \n"
      "1   101  TEST11  101 EXIST \n"
      "2   97  TEST2  110 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input3));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkeys);
}

TEST_F(ObMultipleGetMergeTest, test_multiple_iterator_empty)
{
  ObMultipleGetMerge merge_iter;
  ObArray<ObStoreRowkey> rowkeys;
  generate_query_rowkeys(rowkeys);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input1 =
      "int int var  int flag  \n"
      "NOP NOP  NOP  NOP EMPTY \n"
      "NOP  NOP  NOP  NOP EMPTY \n";
  const char *input2 =
      "int int var  int flag  \n"
      "NOP  NOP NOP  NOP EMPTY \n"
      "NOP   NOP  NOP  NOP EMPTY \n";
  const char *output = NULL;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkeys);
}

TEST_F(ObMultipleGetMergeTest, test_multiple_iterator_delete)
{
  ObMultipleGetMerge merge_iter;
  ObArray<ObStoreRowkey> rowkeys;
  generate_query_rowkeys(rowkeys);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input1 =
      "int int var  int flag  \n"
      "1 100  test2  101 EMPTY \n"
      "2  101 test3  102 EMPTY \n";
  const char *input2 =
      "int int var  int flag  \n"
      "NOP  NOP NOP  NOP DELETE \n"
      "NOP   NOP  NOP  NOP DELETE \n";
  const char *output = NULL;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkeys);
}

TEST_F(ObMultipleGetMergeTest, test_estimate_row)
{
  int ret = OB_SUCCESS;
  ObQueryFlag query_flag;
  const uint64_t table_id = combine_id(1, 3000);
  ObStoreRowkey rowkey;
  ObArray<ObStoreRowkey> rowkeys;
  share::schema::ObColDesc col;
  ObArray<share::schema::ObColDesc> columns;
  ObPartitionEst cost;
  ObMultipleGetMerge merge_iter;
  ret = merge_iter.estimate_cost(query_flag, table_id, rowkeys, columns, stores_, cost);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = columns.push_back(col);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = rowkeys.push_back(rowkey);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObPartitionEst cost_metric1;
  ObPartitionEst cost_metric2;
  cost_metric1.logical_row_count_ = 10;
  cost_metric1.total_cost_ = 100;
  MockObSSStore *store1 = new MockObSSStore();
  //EXPECT_CALL(*store1, get_store_type())
    //.WillRepeatedly(Return(MAJOR_SSSTORE));
  EXPECT_CALL(*store1, estimate_get_cost(_, _, _, _, _))
    .WillRepeatedly(DoAll(SetArgReferee<4>(cost_metric1), Return(OB_SUCCESS)));
  stores_.reset();
  ret = stores_.push_back(store1);
  ASSERT_EQ(OB_SUCCESS, ret);
  cost_metric2.total_cost_ = 120;
  cost_metric2.logical_row_count_ = 11;
  MockObSSStore *store2 = new MockObSSStore();
  //EXPECT_CALL(*store2, get_store_type())
    //.WillRepeatedly(Return(MAJOR_SSSTORE));
  EXPECT_CALL(*store2, estimate_get_cost(_, _, _, _, _))
    .WillRepeatedly(DoAll(SetArgReferee<4>(cost_metric2), Return(OB_SUCCESS)));
  ret = stores_.push_back(store2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = merge_iter.estimate_cost(query_flag, table_id, rowkeys, columns, stores_, cost);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(11, cost.logical_row_count_);
  ASSERT_EQ(0, cost.physical_row_count_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_multiple_get_merge.log");
  OB_LOGGER.set_file_name("test_multiple_get_merge.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
