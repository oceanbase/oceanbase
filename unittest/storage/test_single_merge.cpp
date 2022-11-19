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
#include "storage/access/ob_single_merge.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/allocator/page_arena.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "mockcontainer/mock_ob_iterator.h"
#include "mockcontainer/mock_misc_for_ps_test.h"

namespace oceanbase
{
using namespace blocksstable;
namespace unittest
{

using namespace ::testing;
class ObSingleMergeTest : public ::testing::Test
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
  ObSingleMergeTest() : allocator_(ObModIds::OB_ST_TEMP) {}
  void generate_query_rowkey(const int64_t pk, ObStoreRowkey &rowkey);
  void prepare_input(const ObIArray<const char *> &inputs, ObArray<ObMockStoreRowIterator *> &iters);
  void prepare_store(const ObIArray<ObMockStoreRowIterator *> &iters);
  void check_result(const char *output, const ObStoreRowkey &rowkey);
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
    iters_.reset();
    for (int64_t i = 0; i < stores_.count(); ++i) {
      delete stores_.at(i);
    }
    stores_.reset();
  }

public:
  ObArenaAllocator allocator_;
  ColIdxArray out_idxs_;
  ObStoreCtx store_ctx_;
  ObTableAccessParam main_table_param_;
  ObTableAccessContext main_table_ctx_;
  common::ObSEArray<ObIStore *, common::MAX_TABLE_CNT_IN_STORAGE> stores_;
  ObMultipleMerge::MergeIterators iters_;
  ObObj cells_[OUTPUT_COL_NUM];
};

void ObSingleMergeTest::generate_query_rowkey(const int64_t pk, ObStoreRowkey &rowkey)
{
  cells_[0].set_int(pk);
  rowkey.assign(cells_, ROWKEY_NUM);
}

void ObSingleMergeTest::prepare_input(const ObIArray<const char *> &input, ObArray<ObMockStoreRowIterator *> &iters)
{
  for (int i = 0; i < input.count(); ++i) {
    ObMockStoreRowIterator *iter = new ObMockStoreRowIterator();
    iter->from(input.at(i));
    ASSERT_EQ(OB_SUCCESS, iters.push_back(iter));
  }
}

void ObSingleMergeTest::check_result(const char *output, const ObStoreRowkey &rowkey)
{
  ObMockQueryRowIterator expect_iter;
  expect_iter.from(output);
  ObSingleMerge merge_iter;
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(rowkey));
  ASSERT_TRUE(expect_iter.equals(merge_iter));
  merge_iter.reset();
}

void ObSingleMergeTest::prepare_store(const ObIArray<ObMockStoreRowIterator *> &iters)
{
  stores_.reset();
  for (int i = 0; i < iters.count(); ++i) {
    MockObSSStore *store = new MockObSSStore();
    EXPECT_CALL(*store, get(_, _, _, _))
          .WillRepeatedly(DoAll(SetArgReferee<3>(static_cast<ObStoreRowIterator *>(iters.at(i))),
                Return(OB_SUCCESS)));
    ASSERT_EQ(OB_SUCCESS, stores_.push_back(store));
  }
}

TEST_F(ObSingleMergeTest, test_rowkey_is_null)
{
  ObSingleMerge merge_iter;
  ObStoreRow *row = NULL;
  stores_.reset();
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(row));
  ASSERT_EQ(NULL, row);
}

TEST_F(ObSingleMergeTest, test_one_iterator_exist)
{
  ObStoreRowkey rowkey;
  generate_query_rowkey(1L, rowkey);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input =
      "int int var  int flag  \n"
      "1   99  test  100 EXIST \n";
  const char *output =
      "int int var  int flag  \n"
      "1   99  test  100 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkey);
}

TEST_F(ObSingleMergeTest, test_one_iterator_not_exist)
{
  ObStoreRowkey rowkey;
  generate_query_rowkey(1L, rowkey);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input =
      "int int var  int flag  \n"
      "1   99  test  100 EMPTY \n";
  const char *output = "\0";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkey);
}

TEST_F(ObSingleMergeTest, test_one_iterator_delete)
{
  ObStoreRowkey rowkey;
  generate_query_rowkey(1L, rowkey);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input =
      "int int var  int flag  \n"
      "1   99  test  100 DELETE \n";
  const char *output = "\0";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkey);
}

// SSTable: exist
// Memtable: update
TEST_F(ObSingleMergeTest, test_multiple_iterator_exist1)
{
  ObStoreRowkey rowkey;
  generate_query_rowkey(1L, rowkey);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input1 =
      "int int var  int flag  \n"
      "1   99  test  100 EXIST \n";
  const char *input2 =
      "int int var  int flag  \n"
      "1   100 TEST1  NOP EXIST \n";
  const char *output =
      "int int var  int flag  \n"
      "1   100 TEST1  100 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkey);
}

// SSTable: empty
// Memtable: insert
TEST_F(ObSingleMergeTest, test_multiple_iterator_exist2)
{
  ObStoreRowkey rowkey;
  generate_query_rowkey(1L, rowkey);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input1 =
      "int int var  int flag  \n"
      "NOP  NOP  NOP  NOP EXIST \n";
  const char *input2 =
      "int int var  int flag  \n"
      "1   100 TEST1  99 EXIST \n";
  const char *output =
      "int int var  int flag  \n"
      "1   100 TEST1  99 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkey);
}

// SSTable: exist
// Minor SSTable: update
// Memtable: update
TEST_F(ObSingleMergeTest, test_multiple_iterator_exist3)
{
  ObStoreRowkey rowkey;
  generate_query_rowkey(1L, rowkey);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input1 =
      "int int var  int flag  \n"
      "1  100  test  99 EXIST \n";
  const char *input2 =
      "int int var  int flag  \n"
      "1    99 TEST1  NOP EXIST \n";
  const char *input3 =
      "int int var  int flag  \n"
      "1   NOP TEST2  22 EXIST \n";
  const char *output =
      "int int var  int flag  \n"
      "1   99 TEST2  22 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input3));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkey);
}

// SSTable: empty
// Minor SSTable: insert
// Memtable: update
TEST_F(ObSingleMergeTest, test_multiple_iterator_exist4)
{
  ObStoreRowkey rowkey;
  generate_query_rowkey(1L, rowkey);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input1 =
      "int int var  int flag  \n"
      "NOP  NOP  NOP  NOP EMPTY \n";
  const char *input2 =
      "int int var  int flag  \n"
      "1    99 TEST1  2 EXIST \n";
  const char *input3 =
      "int int var  int flag  \n"
      "1   NOP TEST2  22 EXIST \n";
  const char *output =
      "int int var  int flag  \n"
      "1   99 TEST2  22 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input3));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkey);
}

TEST_F(ObSingleMergeTest, test_multiple_iterator_empty)
{
  ObStoreRowkey rowkey;
  generate_query_rowkey(1L, rowkey);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input1 =
      "int int var  int flag  \n"
      "NOP   NOP  NOP  NOP EMPTY \n";
  const char *input2 =
      "int int var  int flag  \n"
      "NOP   NOP NOP  NOP EMPTY \n";
  const char *output = NULL;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkey);
}

TEST_F(ObSingleMergeTest, test_multiple_iterator_delete)
{
  ObStoreRowkey rowkey;
  generate_query_rowkey(1L, rowkey);
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  const char *input1 =
      "int int var  int flag  \n"
      "100   100  test2  1 EMPTY \n";
  const char *input2 =
      "int int var  int flag  \n"
      "NOP   NOP NOP  NOP DELETE \n";
  const char *output = NULL;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output, rowkey);
}

TEST_F(ObSingleMergeTest, test_estimate_row)
{
  int ret = OB_SUCCESS;
  ObQueryFlag query_flag;
  const uint64_t table_id = combine_id(1, 3000);
  ObStoreRowkey rowkey;
  share::schema::ObColDesc col;
  ObArray<share::schema::ObColDesc> columns;
  ObPartitionEst cost;
  ObSingleMerge merge_iter;
  ret = merge_iter.estimate_cost(query_flag, table_id, rowkey, columns, stores_, cost);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = columns.push_back(col);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObPartitionEst cost_metric1;
  ObPartitionEst cost_metric2;
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
  MockObSSStore *store2 = new MockObSSStore();
  //EXPECT_CALL(*store2, get_store_type())
    //.WillRepeatedly(Return(MAJOR_SSSTORE));
  EXPECT_CALL(*store2, estimate_get_cost(_, _, _, _, _))
    .WillRepeatedly(DoAll(SetArgReferee<4>(cost_metric2), Return(OB_SUCCESS)));
  ret = stores_.push_back(store2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = merge_iter.estimate_cost(query_flag, table_id, rowkey, columns, stores_, cost);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, cost.logical_row_count_);
  ASSERT_EQ(0, cost.physical_row_count_);
  ASSERT_EQ(220, cost.total_cost_);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_single_merge.log");
  OB_LOGGER.set_file_name("test_single_merge.log");
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_single_merge");
  oceanbase::unittest::init_tenant_mgr();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
