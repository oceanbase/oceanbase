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
#include "sql/ob_sql_init.h"
#include "sql/engine/set/ob_merge_union.h"
#include "sql/engine/table/ob_fake_table.h"
#include "sql/engine/recursive_cte/ob_recursive_cte_util.h"
#include "observer/ob_server.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;

class RecursiveCTETester : public ObRecursiveUnionAll
{
public:
  RecursiveCTETester() :ObRecursiveUnionAll(alloc_) {}
  virtual ~RecursiveCTETester() {}
};

class TestRecusiveUnionAll: public ::testing::Test
{
public:
  TestRecusiveUnionAll();
  virtual ~TestRecusiveUnionAll();
  virtual void SetUp();
  virtual void TearDown();
  void except_result_with_row_count(ObExecContext& _ctx, ObFakeTable& _result_table,
  		uint64_t _count, RecursiveCTETester& op,
			uint64_t start_idx,
			uint64_t end_idx,
			ObCollationType& collation) {
  	int ret = OB_SUCCESS;
    UNUSED(start_idx);
    UNUSED(end_idx);
    UNUSED(collation);
		if (OB_SUCC(ret)) {
			const ObNewRow *result_row = NULL;
			uint64_t j = 0;
			while(j < _count && (OB_SUCCESS == (ret = op.get_next_row(_ctx, result_row)))) {
				j++;
				const ObNewRow *except_row = NULL;
				//printf("row=%s\n", to_cstring(*result_row));
				ASSERT_EQ(OB_SUCCESS, _result_table.get_next_row(_ctx, except_row));
				//printf("except_row=%s\n", to_cstring(*except_row));
				ASSERT_TRUE(except_row->count_ == result_row->count_);
				/*for (int64_t i = start_idx; i < end_idx; ++i) {
					printf("index=%ld, cell=%s, respect_cell=%s\n", i, to_cstring(result_row->cells_[i]), to_cstring(except_row->cells_[i]));
					ASSERT_TRUE(0 == except_row->cells_[i].compare(result_row->cells_[i], collation));
				}*/
				result_row = NULL;
			}
		}
  }
protected:
private:
  // disallow copy
  TestRecusiveUnionAll(const TestRecusiveUnionAll &other);
  TestRecusiveUnionAll& operator=(const TestRecusiveUnionAll &other);
private:
  // data members
};
TestRecusiveUnionAll::TestRecusiveUnionAll()
{
}

TestRecusiveUnionAll::~TestRecusiveUnionAll()
{
}

void TestRecusiveUnionAll::SetUp()
{
}

void TestRecusiveUnionAll::TearDown()
{
}

TEST_F(TestRecusiveUnionAll, test_recursive_cte_muti_round_breadth)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  const ObNewRow* row = NULL;
  ObFakeTable &planA_output_table = TestRecursiveCTEFactory::get_planA_op_();
  ObFakeTable &planB_output_table = TestRecursiveCTEFactory::get_planB_op_();
  planB_output_table.set_no_rescan();
  ObFakeTable &result_table = TestRecursiveCTEFactory::get_result_table();
  RecursiveCTETester recursive_cte;
  recursive_cte.init_cte_pseudo_column();
  ObFakeCTETable fake_cte(alloc_);
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;
  fake_cte.add_column_involved_offset(0);
  fake_cte.add_column_involved_offset(1);
  fake_cte.add_column_involved_offset(2);
  fake_cte.add_column_involved_offset(3);
  fake_cte.add_column_involved_offset(4);

  TestRecursiveCTEFactory::init(ctx, &recursive_cte, &fake_cte, 5);

  ASSERT_EQ(OB_SUCCESS, recursive_cte.open(ctx));
  ASSERT_EQ(OB_SUCCESS, fake_cte.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));

  //第一轮不会产生数据，因为planb并不会被访问到，但是会有数据向fake cte table 输出，即A
  //下一论出来的数据则是A被拿去planb中查处来的数据
  ADD_ROW(planA_output_table, COL(1), COL(2), COL(1.0), COL("A"), COL(null));
  ADD_ROW(planA_output_table, COL(2), COL(3), COL(7), COL("B"), COL("oceanbase"));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("A"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("A"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1.0), COL("AA"), COL(null));
  ADD_ROW(planB_output_table, COL(2), COL(2), COL(4.0), COL("AB"), COL(null));

  ADD_ROW(result_table, COL(2), COL(3), COL(7), COL("B"), COL("oceanbase"));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AA"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1.0), COL("BA"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("AA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_TRUE(NULL == row);
  //EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AA"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1.0), COL("AAA"), COL(null));

  ADD_ROW(result_table, COL(2), COL(2), COL(4.0), COL("AB"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  //EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(2), COL(4.0), COL("AB"), COL(null));
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AAA"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("BA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_TRUE(NULL == row);
  //EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("BA"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("AAA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  //EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AAA"), COL(null));

  const ObNewRow *except_row = NULL;
  ASSERT_TRUE( OB_ITER_END == recursive_cte.get_next_row( ctx, except_row));

  ASSERT_EQ(OB_SUCCESS, recursive_cte.close(ctx));
  ASSERT_EQ(OB_SUCCESS, fake_cte.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestRecusiveUnionAll, test_recursive_cte_muti_round_depth)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  const ObNewRow* row = NULL;
  ObFakeTable &planA_output_table = TestRecursiveCTEFactory::get_planA_op_();
  ObFakeTable &planB_output_table = TestRecursiveCTEFactory::get_planB_op_();
  planB_output_table.set_no_rescan();
  ObFakeTable &result_table = TestRecursiveCTEFactory::get_result_table();
  RecursiveCTETester recursive_cte;
  recursive_cte.init_cte_pseudo_column();
  ObFakeCTETable fake_cte(alloc_);
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;
  fake_cte.add_column_involved_offset(0);
  fake_cte.add_column_involved_offset(1);
  fake_cte.add_column_involved_offset(2);
  fake_cte.add_column_involved_offset(3);
  fake_cte.add_column_involved_offset(4);

  TestRecursiveCTEFactory::init_depth(ctx, &recursive_cte, &fake_cte, 5);

  ASSERT_EQ(OB_SUCCESS, recursive_cte.open(ctx));
  ASSERT_EQ(OB_SUCCESS, fake_cte.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));

  //第一轮不会产生数据，因为planb并不会被访问到，但是会有数据向fake cte table 输出，即A
  //下一论出来的数据则是A被拿去planb中查处来的数据
  ADD_ROW(planA_output_table, COL(1), COL(2), COL(1.0), COL("A"), COL(null));
  ADD_ROW(planA_output_table, COL(2), COL(3), COL(7), COL("B"), COL("oceanbase"));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("A"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("A"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1.0), COL("AA"), COL(null));
  ADD_ROW(planB_output_table, COL(2), COL(2), COL(4.0), COL("AB"), COL(null));
  ADD_ROW(planB_output_table, COL(2), COL(2), COL(4.0), COL("AC"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("AA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AA"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1.0), COL("AAA"), COL(null));
  ADD_ROW(planB_output_table, COL(2), COL(2), COL(4.0), COL("AAB"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("AAA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AAA"), COL(null));

  ADD_ROW(result_table, COL(2), COL(2), COL(4.0), COL("AAB"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(2), COL(4.0), COL("AAB"), COL(null));

  ADD_ROW(result_table, COL(2), COL(2), COL(4.0), COL("AB"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(2), COL(4.0), COL("AB"), COL(null));

  ADD_ROW(result_table, COL(2), COL(2), COL(4.0), COL("AC"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(2), COL(4.0), COL("AC"), COL(null));

  ADD_ROW(result_table, COL(2), COL(3), COL(7), COL("B"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(3), COL(7), COL("B"), COL(null));

  const ObNewRow *except_row = NULL;
  ASSERT_TRUE( OB_ITER_END == recursive_cte.get_next_row( ctx, except_row));

  ASSERT_EQ(OB_SUCCESS, recursive_cte.close(ctx));
  ASSERT_EQ(OB_SUCCESS, fake_cte.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestRecusiveUnionAll, test_recursive_cte_muti_round_depth_search_by_col3)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  const ObNewRow* row = NULL;
  ObFakeTable &planA_output_table = TestRecursiveCTEFactory::get_planA_op_();
  ObFakeTable &planB_output_table = TestRecursiveCTEFactory::get_planB_op_();
  planB_output_table.set_no_rescan();
  ObFakeTable &result_table = TestRecursiveCTEFactory::get_result_table();
  RecursiveCTETester recursive_cte;
  recursive_cte.init_cte_pseudo_column();
  ObFakeCTETable fake_cte(alloc_);
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;
  fake_cte.add_column_involved_offset(0);
  fake_cte.add_column_involved_offset(1);
  fake_cte.add_column_involved_offset(2);
  fake_cte.add_column_involved_offset(3);
  fake_cte.add_column_involved_offset(4);

  TestRecursiveCTEFactory::init_depth_search_by_col3(ctx, &recursive_cte, &fake_cte, 5);

  ASSERT_EQ(OB_SUCCESS, recursive_cte.open(ctx));
  ASSERT_EQ(OB_SUCCESS, fake_cte.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));

  ADD_ROW(planA_output_table, COL(1), COL(2), COL(1.0), COL("A"), COL(null));
  ADD_ROW(planA_output_table, COL(2), COL(3), COL(7), COL("B"), COL("oceanbase"));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("A"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("A"), COL(null));
  //由于union all 自身关闭了排序逻辑，所以这里必须按序加入结果集
  ADD_ROW(planB_output_table, COL(2), COL(2), COL(4.0), COL("AA"), COL(null));
  ADD_ROW(planB_output_table, COL(2), COL(2), COL(4.0), COL("AB"), COL(null));
  ADD_ROW(planB_output_table, COL(2), COL(2), COL(4.0), COL("AC"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1.0), COL("AD"), COL(null));

  ADD_ROW(result_table, COL(2), COL(2), COL(4.0), COL("AA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(2), COL(4.0), COL("AA"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1.0), COL("AAA"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1.0), COL("AAB"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1.0), COL("AAC"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1.0), COL("AAD"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1.0), COL("AAE"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1.0), COL("AAF"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("AAA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AAA"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("AAB"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AAB"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("AAC"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AAC"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("AAD"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AAD"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("AAE"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AAE"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("AAF"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AAF"), COL(null));

  ADD_ROW(result_table, COL(2), COL(2), COL(4.0), COL("AB"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(2), COL(4.0), COL("AB"), COL(null));

  ADD_ROW(result_table, COL(2), COL(2), COL(4.0), COL("AC"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(2), COL(4.0), COL("AC"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1.0), COL("AD"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1.0), COL("AD"), COL(null));

  ADD_ROW(result_table, COL(2), COL(3), COL(7), COL("B"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(3), COL(7), COL("B"), COL(null));

  const ObNewRow *except_row = NULL;
  ASSERT_TRUE( OB_ITER_END == recursive_cte.get_next_row( ctx, except_row));

  ASSERT_EQ(OB_SUCCESS, recursive_cte.close(ctx));
  ASSERT_EQ(OB_SUCCESS, fake_cte.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

TEST_F(TestRecusiveUnionAll, test_recursive_cte_muti_round_breadth_search_by_col3_cycle_by_col2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  const ObNewRow* row = NULL;
  ObFakeTable &planA_output_table = TestRecursiveCTEFactory::get_planA_op_();
  ObFakeTable &planB_output_table = TestRecursiveCTEFactory::get_planB_op_();
  planB_output_table.set_no_rescan();
  ObFakeTable &result_table = TestRecursiveCTEFactory::get_result_table();
  RecursiveCTETester recursive_cte;
  recursive_cte.init_cte_pseudo_column();
  ObFakeCTETable fake_cte(alloc_);
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;
  fake_cte.add_column_involved_offset(0);
  fake_cte.add_column_involved_offset(1);
  fake_cte.add_column_involved_offset(2);
  fake_cte.add_column_involved_offset(3);
  fake_cte.add_column_involved_offset(4);

  TestRecursiveCTEFactory::init_breadth_search_by_col3_cyc_by_col2(ctx, &recursive_cte, &fake_cte, 5);

  ASSERT_EQ(OB_SUCCESS, recursive_cte.open(ctx));
  ASSERT_EQ(OB_SUCCESS, fake_cte.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));

  //
  // 下图展示了测试使用的图，其中AAA与先祖A在列2的值上发生了重复，是重复列。在oracle中测试，同一层的指定值重复也不算环
  //                  A （1）       B （2）
  //           AA（3）  AB（4）    BA（5）
  //     AAA（1）   ABA（5）       BAA (4)
  //
  //第一轮不会产生数据，因为planb并不会被访问到，但是会有数据向fake cte table 输出，即A
  //下一论出来的数据则是A被拿去planb中查处来的数据
  ADD_ROW(planA_output_table, COL(1), COL(2), COL(1), COL("A"), COL(null));
  ADD_ROW(planA_output_table, COL(2), COL(3), COL(2), COL("B"), COL("oceanbase"));

  ADD_ROW(result_table, COL(1), COL(2), COL(1), COL("A"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1), COL("A"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(3), COL("AA"), COL(null));
  ADD_ROW(planB_output_table, COL(2), COL(2), COL(4), COL("AB"), COL(null));

  ADD_ROW(result_table, COL(2), COL(3), COL(2), COL("B"), COL("oceanbase"));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  //EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(3), COL(2), COL("B"), COL("oceanbase"));
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(3), COL("AA"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(5), COL("BA"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(3), COL("AA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_TRUE(NULL == row);
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1), COL("AAA"), COL(null));

  ADD_ROW(result_table, COL(2), COL(2), COL(4), COL("AB"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  //EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(2), COL(4), COL("AB"), COL(null));
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(5), COL("BA"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(5), COL("ABA"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(5), COL("BA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_TRUE(NULL == row);
  //EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(5), COL("BA"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(4), COL("BAA"), COL(null));
  //AAA 列2值为1，与A列2值为1重复，A是AAA的先祖，所以AAA不会进入到fake table中，会直接出队
  ADD_ROW(result_table, COL(1), COL(2), COL(1), COL("AAA"), COL(null));
  ADD_ROW(result_table, COL(1), COL(2), COL(5), COL("ABA"), COL(null));
  except_result_with_row_count(ctx, result_table, 2, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(4), COL("BAA"), COL(null));
  //EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(5), COL("ABA"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(4), COL("BAA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_TRUE(NULL == row);
  //EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(4), COL("BAA"), COL(null));

  const ObNewRow *except_row = NULL;
  ASSERT_TRUE( OB_ITER_END == recursive_cte.get_next_row( ctx, except_row));

  ASSERT_EQ(OB_SUCCESS, recursive_cte.close(ctx));
  ASSERT_EQ(OB_SUCCESS, fake_cte.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}


TEST_F(TestRecusiveUnionAll, test_recursive_cte_muti_round_depth_search_by_col3_cycle_by_col2)
{
  int ret = OB_SUCCESS;
  ObExecContext ctx;
  const ObNewRow* row = NULL;
  ObFakeTable &planA_output_table = TestRecursiveCTEFactory::get_planA_op_();
  ObFakeTable &planB_output_table = TestRecursiveCTEFactory::get_planB_op_();
  planB_output_table.set_no_rescan();
  ObFakeTable &result_table = TestRecursiveCTEFactory::get_result_table();
  RecursiveCTETester recursive_cte;
  recursive_cte.init_cte_pseudo_column();
  ObFakeCTETable fake_cte(alloc_);
  ObCollationType agg_cs_type = CS_TYPE_UTF8MB4_BIN;
  fake_cte.add_column_involved_offset(0);
  fake_cte.add_column_involved_offset(1);
  fake_cte.add_column_involved_offset(2);
  fake_cte.add_column_involved_offset(3);
  fake_cte.add_column_involved_offset(4);

  TestRecursiveCTEFactory::init_depth_search_by_col3_cyc_by_col2(ctx, &recursive_cte, &fake_cte, 5);

  ASSERT_EQ(OB_SUCCESS, recursive_cte.open(ctx));
  ASSERT_EQ(OB_SUCCESS, fake_cte.open(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.open(ctx));

  //
  // 下图展示了测试使用的图，其中AAA与先祖A在列2的值上发生了重复，是重复列。在oracle中测试，同一层的指定值重复也不算环
  // 右图是节点在数组tree中的偏移量
  //                  A （1）       B （2）                  uint64      uint64
  //           AA（3）  AB（4）    BA（5）               0       0            1
  //     AAA（1）   ABA（5）       BAA (4)          2         3              4
  //
  // 第一轮不会产生数据，因为planb并不会被访问到，但是会有数据向fake cte table 输出，即A
  // 下一论出来的数据则是A被拿去planb中查处来的数据
  ADD_ROW(planA_output_table, COL(1), COL(2), COL(1), COL("A"), COL(null));
  ADD_ROW(planA_output_table, COL(2), COL(3), COL(2), COL("B"), COL("oceanbase"));

  ADD_ROW(result_table, COL(1), COL(2), COL(1), COL("A"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(1), COL("A"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(3), COL("AA"), COL(null));
  ADD_ROW(planB_output_table, COL(2), COL(2), COL(4), COL("AB"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(3), COL("AA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(3), COL("AA"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(1), COL("AAA"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(1), COL("AAA"), COL(null));//由于AAA列2的值为1，与栈顶节点"A"的值一样，因此AAA不会进入到fake cte table算子中
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  //AAA不会进入到fake table中，符合预期

  ADD_ROW(result_table, COL(2), COL(2), COL(4), COL("AB"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(2), COL(4), COL("AB"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(5), COL("ABA"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(5), COL("ABA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(5), COL("ABA"), COL(null));

  ADD_ROW(result_table, COL(2), COL(3), COL(2), COL("B"), COL("oceanbase"));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(2), COL(3), COL(2), COL("B"), COL("oceanbase"));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(5), COL("BA"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(5), COL("BA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(5), COL("BA"), COL(null));
  ADD_ROW(planB_output_table, COL(1), COL(2), COL(4), COL("BAA"), COL(null));

  ADD_ROW(result_table, COL(1), COL(2), COL(4), COL("BAA"), COL(null));
  except_result_with_row_count(ctx, result_table, 1, recursive_cte, 0, 4, agg_cs_type);//测试时，内存中的RS栈为 B BA BAA与预期符合
  row = NULL;
  fake_cte.get_next_row(ctx, row);
  ASSERT_FALSE(NULL == row);
  EXPECT_SAME_ROW((*row), result_table, 0, 4, agg_cs_type, COL(1), COL(2), COL(4), COL("BAA"), COL(null));

  const ObNewRow *except_row = NULL;
  ASSERT_TRUE( OB_ITER_END == recursive_cte.get_next_row( ctx, except_row));

  ASSERT_EQ(OB_SUCCESS, recursive_cte.close(ctx));
  ASSERT_EQ(OB_SUCCESS, fake_cte.close(ctx));
  ASSERT_EQ(OB_SUCCESS, result_table.close(ctx));
}

int main(int argc, char **argv)
{
  system("rm -f test_recursive_cte.log");
  init_global_memory_pool();
  init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}
