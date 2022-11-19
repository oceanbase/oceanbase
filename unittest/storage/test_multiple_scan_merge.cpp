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
#include "storage/access/mock_ob_ss_store.h"
#include "storage/access/ob_multiple_scan_merge.h"
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
using namespace::testing;

namespace unittest
{
class ObMultipleScanMergeTest : public ::testing::Test
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
  ObMultipleScanMergeTest() : allocator_(ObModIds::OB_ST_TEMP) {}
  void prepare_input(const ObIArray<const char *> &inputs, ObArray<ObMockStoreRowIterator *> &iters);
  void prepare_store(const ObIArray<ObMockStoreRowIterator *> &iters);
  void check_result(const char *output);
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

    OB_STORE_CACHE.init(1,1,1,1,1, 10);
    main_table_ctx_.allocator_ = &allocator_;
    main_table_ctx_.stmt_allocator_ = &allocator_;
    main_table_ctx_.query_flag_ = ObQueryFlag();
    main_table_ctx_.query_flag_.query_stat_ = 1;
    main_table_ctx_.store_ctx_ = &store_ctx_;
    main_table_ctx_.is_inited_ = true;
    range_.set_whole_range();
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
    OB_STORE_CACHE.destroy();
  }

public:
  ObArenaAllocator allocator_;
  ColIdxArray out_idxs_;
  ObStoreCtx store_ctx_;
  ObTableAccessParam main_table_param_;
  ObTableAccessContext main_table_ctx_;
  common::ObSEArray<ObIStore *, common::MAX_TABLE_CNT_IN_STORAGE> stores_;
  ObStoreRange range_;
  ObMultipleMerge::MergeIterators iters_;
};

void ObMultipleScanMergeTest::prepare_input(const ObIArray<const char *> &input, ObArray<ObMockStoreRowIterator *> &iters)
{
  for (int i = 0; i < input.count(); ++i) {
    ObMockStoreRowIterator *iter = new ObMockStoreRowIterator();
    iter->from(input.at(i));
    ASSERT_EQ(OB_SUCCESS, iters.push_back(iter));
  }
}

void ObMultipleScanMergeTest::check_result(const char *output)
{
  ObMockQueryRowIterator expect_iter;
  expect_iter.from(output);
  ObMultipleScanMerge merge_iter;
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(range_));
  ASSERT_TRUE(expect_iter.equals(merge_iter));
  merge_iter.reset();
}

void ObMultipleScanMergeTest::prepare_store(const ObIArray<ObMockStoreRowIterator *> &iters)
{
  stores_.reset();
  for (int i = 0; i < iters.count(); ++i) {
    MockObSSStore *store = new MockObSSStore();
    EXPECT_CALL(*store, scan(_, _, _, _))
          .WillOnce(DoAll(SetArgReferee<3>(static_cast<ObStoreRowIterator *>(iters.at(i))),
                Return(OB_SUCCESS)));
    ASSERT_EQ(OB_SUCCESS, stores_.push_back(store));
  }
}

TEST_F(ObMultipleScanMergeTest, normal)
{
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  // first col is pk , others c1, c2, c3
  const char *input1 =
      "int int var int flag    \n"
      "6   288 var6 666 EXIST  \n"
      "7   381 var7 777 EXIST  \n"
      "8   88  var8 888 EXIST  \n";
  const char *input2 =
      "int int var  int flag   \n"
      "1   0   var1 111 EXIST  \n"
      "999 0   var9 999 EXIST  \n";
  const char *input3 =
      "int int var  int flag   \n"
      "2   281 NOP  NOP EXIST  \n"
      "3   388 var3 333 EXIST  \n"
      "4   481 NOP  NOP EXIST  \n"
      "9   99  var9 999 EXIST  \n";
  const char *input4 =
      "int int var int flag   \n"
      "1   100 NOP NOP EXIST  \n"
      "2   288 NOP NOP EXIST  \n"
      "3   381 NOP NOP EMPTY  \n"
      "4   88  NOP NOP EXIST  \n";
  const char *input5 =
      "int int var  int flag  \n"
      "1   99  NOP  NOP EXIST \n"
      "2   88  NOP  NOP DELETE\n"
      "4   488 var4 444 EXIST \n"
      "5   100 var5 555 EXIST \n";
  const char *expect =
      "int int var  int flag  \n"
      "1   99  var1 111 EXIST \n"
      "3   388 var3 333 EXIST \n"
      "4   488 var4 444 EXIST \n"
      "5   100 var5 555 EXIST  \n"
      "6   288 var6 666 EXIST  \n"
      "7   381 var7 777 EXIST  \n"
      "8   88  var8 888 EXIST  \n"
      "9   99  var9 999 EXIST  \n"
      "999 0   var9 999 EXIST \n";

  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input3));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input4));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input5));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(expect);


}

TEST_F(ObMultipleScanMergeTest, normal_reverse)
{
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  // first col is pk , others c1, c2, c3
  const char *input1 =
      "int int var  int flag   \n"
      "1   0   var1 111 EXIST  \n"
      "999 0   var9 999 EXIST  \n";
  const char *input2 =
      "int int var  int flag   \n"
      "2   281 NOP  NOP EXIST  \n"
      "3   388 var3 333 EXIST  \n"
      "4   481 NOP  NOP EXIST  \n";
  const char *input3 =
      "int int var int flag   \n"
      "1   100 NOP NOP EXIST  \n"
      "2   288 NOP NOP EXIST  \n"
      "3   381 NOP NOP EMPTY  \n"
      "4   88  NOP NOP EXIST  \n";
  const char *input4 =
      "int int var  int flag  \n"
      "1   99  NOP  NOP EXIST \n"
      "2   88  NOP  NOP DELETE\n"
      "4   488 var4 444 EXIST \n";

  const char *expect =
      "int int var  int flag  \n"
      "1   99  var1 111 EXIST \n"
      "3   388 var3 333 EXIST \n"
      "4   488 var4 444 EXIST \n"
      "999 0   var9 999 EXIST \n";

  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input3));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input4));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(expect);

}


TEST_F(ObMultipleScanMergeTest, test_iterator_rowkey_not_equal)
{
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  // first col is pk , others c1, c2, c3
  const char *input1 =
      "int int var  int flag  \n"
      "1   99  VAR   102 EXIST \n"
      "2   88  VAR2  100 DELETE\n"
      "10   488 var4 444 EXIST \n";

  const char *input2 =
      "int int var int flag   \n"
      "3   100 TEST 100 EXIST  \n"
      "4   288 TEST2 101 EXIST  \n"
      "5   381 TEST3 102 DELETE  \n"
      "6   88  TEST4 103 EXIST  \n";

  const char *output =
      "int int var  int flag  \n"
      "1   99  VAR 102 EXIST \n"
      "3   100 TEST 100 EXIST  \n"
      "4   288 TEST2 101 EXIST  \n"
      "6   88  TEST4 103 EXIST  \n"
      "10   488 var4 444 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output);
}

TEST_F(ObMultipleScanMergeTest, test_iterator_rowkey_equal_exist)
{
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  // first col is pk , others c1, c2, c3
  const char *input1 =
      "int int var  int flag  \n"
      "1   99  VAR   102 EXIST \n"
      "2   88  VAR2  100 EXIST\n"
      "10   488 var4 444 EXIST \n";

  const char *input2 =
      "int int var int flag   \n"
      "1   NOP NOP 100 EXIST  \n"
      "2   288 TEST2 101 EXIST  \n"
      "10   381 TEST3 NOP EXIST  \n";

  const char *output =
      "int int var  int flag  \n"
      "1   99  VAR 100 EXIST \n"
      "2   288 TEST2 101 EXIST  \n"
      "10   381 TEST3 444 EXIST \n";
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output);
}

TEST_F(ObMultipleScanMergeTest, test_iterator_rowkey_equal_not_exist)
{
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  // first col is pk , others c1, c2, c3
  const char *input1 =
      "int int var  int flag  \n"
      "1   99  VAR   102 EXIST \n"
      "2   88  VAR2  100 EXIST\n"
      "10   488 var4 444 EXIST \n";

  const char *input2 =
      "int int var int flag   \n"
      "1   NOP NOP 100 DELETE  \n"
      "2   288 TEST2 101 DELETE  \n"
      "10   381 TEST3 NOP DELETE  \n";

  const char *output = NULL;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output);
}

TEST_F(ObMultipleScanMergeTest, test_iterator_with_delete_row)
{
  ObMultipleScanMerge merge_iter;
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  ObStoreRow *row = NULL;
  // first col is pk , others c1, c2, c3
  const char *input1 =
      "int int var  int flag  \n"
      "1   99  VAR   102 DELETE \n"
      "2   88  VAR2  100 EXIST\n"
      "10   488 var4 444 EXIST \n";

  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  prepare_input(inputs, iters);
  prepare_store(iters);
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(range_));
  merge_iter.set_iter_del_row(true);
  ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(row));
  ASSERT_EQ(OB_ITER_END, merge_iter.get_next_row(row));
}

TEST_F(ObMultipleScanMergeTest, test_iterator_rowkey_equal_empty)
{
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  // first col is pk , others c1, c2, c3
  const char *input1 = NULL;
  const char *input2 = NULL;
  const char *output = NULL;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input1));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input2));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(output);
}

TEST_F(ObMultipleScanMergeTest, test_estimate_cost)
{
  int ret = OB_SUCCESS;
  ObQueryFlag query_flag;
  const uint64_t table_id = combine_id(1, 3000);
  share::schema::ObColDesc col;
  ObArray<share::schema::ObColDesc> columns;
  ObPartitionEst cost;
  ObMultipleScanMerge merge_iter;
  ObArray<ObEstRowCountRecord> records;
  ret = merge_iter.estimate_cost(query_flag, table_id, range_, columns, stores_, cost, records);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = columns.push_back(col);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObPartitionEst cost_metric1;
  ObPartitionEst cost_metric2;
  cost_metric1.logical_row_count_ = 10;
  cost_metric1.total_cost_ = 100;
  MockObSSStore *store1 = new MockObSSStore();
  //EXPECT_CALL(*store1, get_store_type())
    //.WillRepeatedly(Return(MAJOR_SSSTORE));
  EXPECT_CALL(*store1, estimate_scan_cost(_, _, _, _, _))
    .WillRepeatedly(DoAll(SetArgReferee<4>(cost_metric1), Return(OB_SUCCESS)));
  stores_.reset();
  ret = stores_.push_back(store1);
  ASSERT_EQ(OB_SUCCESS, ret);
  cost_metric2.total_cost_ = 120;
  cost_metric2.logical_row_count_ = 11;
  MockObSSStore *store2 = new MockObSSStore();
  //EXPECT_CALL(*store2, get_store_type())
    //.WillRepeatedly(Return(MAJOR_SSSTORE));
  EXPECT_CALL(*store2, estimate_scan_cost(_, _, _, _, _))
    .WillRepeatedly(DoAll(SetArgReferee<4>(cost_metric2), Return(OB_SUCCESS)));
  ret = stores_.push_back(store2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = merge_iter.estimate_cost(query_flag, table_id, range_, columns, stores_, cost, records);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(11, cost.logical_row_count_);
  ASSERT_EQ(0, cost.physical_row_count_);
}

TEST_F(ObMultipleScanMergeTest, test_reset)
{
  const char *input = "flag   int int var  int\n"
                      "EXIST  4   488 var4 444\n";

  const char *expect = "flag   int int var   int\n"
                       "EXIST  4   488 var4  444\n";

  ObMultipleScanMerge merge_iter;
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input));
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(expect);

  iters.reset();
  for (int64_t i = 0; i < stores_.count(); ++i) {
    delete stores_.at(i);
  }
  stores_.reset();
  prepare_input(inputs, iters);
  prepare_store(iters);
  check_result(expect);
}

TEST_F(ObMultipleScanMergeTest, test_init_twice)
{
  ObMultipleScanMerge merge_iter;
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_INIT_TWICE, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
}

// empty out idx_idxs is valid
TEST_F(ObMultipleScanMergeTest, test_init_empty_out_col)
{
  ObMultipleScanMerge merge_iter;
  ColIdxArray out_idxs;
  main_table_param_.iter_param_.output_project_ = &out_idxs;
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  main_table_param_.iter_param_.output_project_ = &out_idxs_;
}

// out idx_idxs contains invalid col index
TEST_F(ObMultipleScanMergeTest, test_init_invalid_out_col)
{
  const char *input = "flag   int int var  int\n"
      "EXIST  4   488 var4 444\n";

  ColIdxArray out_idxs;
  out_idxs.push_back(-1);
  main_table_param_.iter_param_.output_project_ = &out_idxs;
  ObArray<const char *> inputs;
  ObArray<ObMockStoreRowIterator *> iters;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(input));
  prepare_input(inputs, iters);
  prepare_store(iters);
  ObMultipleScanMerge merge_iter;
  ObStoreRow *row = NULL;
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(range_));
  ASSERT_EQ(OB_SUCCESS, merge_iter.get_next_row(row));
}

/*
TEST_F(ObMultipleScanMergeTest, test_default_value_row1)
{
  const char *input1 = "flag   int int var  int int\n"
                       "EXIST  4   388 var4 NOP NOP\n";
  const char *input2 = "flag   int int var  int int\n"
                       "EXIST  4   488 var4 NOP NOP\n";

  ObMultipleScanMerge merge_iter;
  ASSERT_EQ(OB_SUCCESS, merge_iter.init(main_table_param_, main_table_ctx_, stores_));
  ObMockStoreRowIterator iter1;
  ObMockStoreRowIterator iter2;
  ASSERT_EQ(OB_SUCCESS, iter1.from(input1));
  ASSERT_EQ(OB_SUCCESS, iter2.from(input2));
  ASSERT_EQ(OB_SUCCESS, merge_iter.add_iterator(iter1));
  ASSERT_EQ(OB_SUCCESS, merge_iter.add_iterator(iter2));
  ASSERT_EQ(OB_SUCCESS, merge_iter.open(range_));
  storage::ObStoreRow *row = NULL;
  ASSERT_EQ(OB_ERR_UNEXPECTED, merge_iter.get_next_row(row));
}

TEST_F(ObMultipleScanMergeTest, test_default_value_row2)
{
  const char *input1 = "flag   int int var  int int\n"
                       "EXIST  4   388 var4 NOP NOP\n";
  const char *input2 = "flag   int int var  int int\n"
                       "EXIST  4   488 var4 NOP NOP\n";
  const char *default_value_row = "flag  int  int\n"
                                  "EXIST 1    3  \n";
  ObMockStoreRowIterator default_value_iter;
  const storage::ObStoreRow *row;
  ASSERT_EQ(OB_SUCCESS, default_value_iter.from(default_value_row));
  ASSERT_EQ(OB_SUCCESS, default_value_iter.get_next_row(row));
  ObMockStoreRowIterator iter1;
  ObMockStoreRowIterator iter2;
  storage::ObStoreRow *r = NULL;
  ASSERT_EQ(OB_SUCCESS, iter1.from(input1));
  ASSERT_EQ(OB_SUCCESS, iter2.from(input2));
  ObQueryFlag flag;
  ObMultipleScanMerge merge_iter(allocator_, flag, CELL_NUM, ROWKEY_NUM, out_idxs_, row);
  ASSERT_EQ(OB_SUCCESS, merge_iter.add_iterator(iter1));
  ASSERT_EQ(OB_SUCCESS, merge_iter.add_iterator(iter2));
  ASSERT_EQ(OB_SUCCESS, merge_iter.init());
  ASSERT_EQ(OB_INVALID_ARGUMENT, merge_iter.get_next_row(r));
}

TEST_F(ObMultipleScanMergeTest, test_default_value_row3)
{
  const char *input1 = "flag   int int var  int int\n"
                       "EXIST  4   388 var4 NOP NOP\n";
  const char *input2 = "flag   int int var  int int\n"
                       "EXIST  4   488 var4 NOP NOP\n";
  const char *default_value_row = "flag  int  int var  int int\n"
                                  "EXIST 3    3   var4 4   NOP\n";
  ObQueryFlag flag;
  ObMockStoreRowIterator default_value_iter;
  const storage::ObStoreRow *row;
  ASSERT_EQ(OB_SUCCESS, default_value_iter.from(default_value_row));
  ASSERT_EQ(OB_SUCCESS, default_value_iter.get_next_row(row));
  ObMultipleScanMerge merge_iter(allocator_, flag, CELL_NUM, ROWKEY_NUM, out_idxs_, row);
  ObMockStoreRowIterator iter1;
  ObMockStoreRowIterator iter2;
  storage::ObStoreRow *r = NULL;
  ASSERT_EQ(OB_SUCCESS, iter1.from(input1));
  ASSERT_EQ(OB_SUCCESS, iter2.from(input2));
  ASSERT_EQ(OB_SUCCESS, merge_iter.add_iterator(iter1));
  ASSERT_EQ(OB_SUCCESS, merge_iter.add_iterator(iter2));
  ASSERT_EQ(OB_SUCCESS, merge_iter.init());
  ASSERT_EQ(OB_ERR_UNEXPECTED, merge_iter.get_next_row(r));
}
*/

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_multiple_scan_merge.log");
  OB_LOGGER.set_file_name("test_multiple_scan_merge.log");
  OB_LOGGER.set_log_level("WARN");
  CLOG_LOG(INFO, "begin unittest: test_multiple_scan_merge");
  oceanbase::unittest::init_tenant_mgr();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
