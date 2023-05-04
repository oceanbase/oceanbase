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

#define USING_LOG_PREFIX SQL_OPT
#include "observer/ob_server.h"
#include "election/ob_election_async_log.h"
#include "clog/ob_clog_history_reporter.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_opt_est_utils.h"
#include "observer/ob_req_time_service.h"
#include <test_optimizer_utils.h>

#define BUF_LEN 102400 // 100K
using std::cout;
using namespace oceanbase::lib;
using namespace oceanbase::json;
using oceanbase::sql::ObTableLocation;
using namespace oceanbase::sql;
using namespace oceanbase::observer;

namespace test
{
class TestOptimizer :public TestOptimizerUtils
{
public:
  TestOptimizer(){}
  ~TestOptimizer(){}
};
////// insert, update, delete, select for update
// TEST_F(TestOptimizer, ob_optimizer_hierarchical_query)
// {
//   const char* test_file = "./test_optimizer_hierarchical_query.sql";
//   const char* result_file = "./test_optimizer_hierarchical_query.result";
//   const char* tmp_file = "./test_optimizer_hierarchical_query.tmp";
//   ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file));
// }

//TEST_F(TestOptimizer, ob_optimizer_merg)
//{
//  const char* test_file = "./test_optimizer_merge.sql";
//  const char* result_file = "./test_optimizer_merge.result";
//  const char* tmp_file = "./test_optimizer_merge.tmp";
//  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file));
//}

TEST_F(TestOptimizer, ob_optimizer_dml)
{
  const char* test_file = "./test_optimizer_dml.sql";
  const char* result_file = "./test_optimizer_dml.result";
  const char* tmp_file = "./test_optimizer_dml.tmp";
  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file));
}

// select
TEST_F(TestOptimizer, ob_optimizer_select)
{
  const char* test_file = "./test_optimizer_select.sql";
  const char* result_file = "./test_optimizer_select.result";
  const char* tmp_file = "./test_optimizer_select.tmp";
  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file));
}


// explain format=json
TEST_F(TestOptimizer, ob_optimizer_explain_json)
{
  const char* test_file = "./test_optimizer_explain_json.sql";
  const char* result_file = "./test_optimizer_explain_json.result";
  const char* tmp_file = "./test_optimizer_explain_json.tmp";
  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file));
}

// explain extended
TEST_F(TestOptimizer, ob_optimizer_explain_extended)
{
  const char* test_file = "./test_optimizer_explain_extended.sql";
  const char* result_file = "./test_optimizer_explain_extended.result";
  const char* tmp_file = "./test_optimizer_explain_extended.tmp";
  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file));
}

// filter before index back extended
TEST_F(TestOptimizer, ob_optimizer_filter_before_indexback)
{
  const char* test_file = "./test_filter_before_indexback.sql";
  const char* result_file = "./test_filter_before_indexback.result";
  const char* tmp_file = "./test_filter_before_indexback.tmp";
  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file));
}

TEST_F(TestOptimizer, ob_default_stat_est)
{
  const char* test_file = "./test_optimizer_default_stat.sql";
  const char* result_file = "./test_optimizer_default_stat.result";
  const char* tmp_file = "./test_optimizer_default_stat.tmp";
  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file, true));
}

inline bool double_eq(ObObj a, double b, double eps) {
  return fabs(a.get_double() - b) <= eps;
}

TEST_F(TestOptimizer, string_scalar_convert)
{
  int ret = OB_SUCCESS;
  ObObj min, max, start, end;
  ObObj mins, maxs, starts, ends;
  //Standard case : number only, base = 10

  min.set_varchar("123400000");
  max.set_varchar("123500000");
  start.set_varchar("123450000");
  end.set_varchar("123470000");

  ASSERT_TRUE(OB_SUCC(ObOptEstObjToScalar::convert_objs_to_scalars(
      &min, &max, &start, &end, &mins, &maxs, &starts, &ends)));
  ASSERT_TRUE(double_eq(mins, 0.4, 0.0001));
  ASSERT_TRUE(double_eq(maxs, 0.5, 0.0001));
  ASSERT_TRUE(double_eq(starts, 0.45, 0.0001));
  ASSERT_TRUE(double_eq(ends, 0.47, 0.0001));


  min.set_varchar("123");
  max.set_varchar("123500000");
  start.set_varchar("123450000");
  end.set_varchar("123470000");

  ASSERT_TRUE(OB_SUCC(ObOptEstObjToScalar::convert_objs_to_scalars(
      &min, &max, &start, &end, &mins, &maxs, &starts, &ends)));
  ASSERT_TRUE(double_eq(mins, 0, 0.0001));
  ASSERT_TRUE(double_eq(maxs, 0.5, 0.0001));
  ASSERT_TRUE(double_eq(starts, 0.45, 0.0001));
  ASSERT_TRUE(double_eq(ends, 0.47, 0.0001));


  min.set_varchar("1");
  max.set_varchar("123500000");
  start.set_varchar("123450000");
  end.set_varchar("123470000");

  ASSERT_TRUE(OB_SUCC(ObOptEstObjToScalar::convert_objs_to_scalars(
      &min, &max, &start, &end, &mins, &maxs, &starts, &ends)));
  ASSERT_TRUE(double_eq(mins, 0, 0.0001));
  ASSERT_TRUE(double_eq(maxs, 0.235, 0.00001));
  ASSERT_TRUE(double_eq(starts, 0.2345, 0.00001));
  ASSERT_TRUE(double_eq(ends, 0.2347, 0.00001));


  min.set_varchar("123400000");
  max.set_varchar("123500000");
  start.set_varchar("120000");
  end.set_varchar("123470000");

  ASSERT_TRUE(OB_SUCC(ObOptEstObjToScalar::convert_objs_to_scalars(
      &min, &max, &start, &end, &mins, &maxs, &starts, &ends)));
  ASSERT_TRUE(double_eq(mins, 0.34, 0.0001));
  ASSERT_TRUE(double_eq(maxs, 0.35, 0.00001));
  ASSERT_TRUE(double_eq(starts, 0, 0.00001));
  ASSERT_TRUE(double_eq(ends, 0.347, 0.00001));

  // upper character : base = 26

  min.set_varchar("AAAA");
  max.set_varchar("KKKK");
  start.set_varchar("CCCC");
  end.set_varchar("FFFF");

  ASSERT_TRUE(OB_SUCC(ObOptEstObjToScalar::convert_objs_to_scalars(
      &min, &max, &start, &end, &mins, &maxs, &starts, &ends)));
  ASSERT_TRUE(double_eq(mins, 0, 0.00001));
  ASSERT_TRUE(double_eq(maxs, 0.4, 0.00001));
  ASSERT_TRUE(double_eq(starts, 0.08, 0.00001));
  ASSERT_TRUE(double_eq(ends, 0.2, 0.00001));

  // lower character : base = 26

  min.set_varchar("aaaa"); //a = 97
  max.set_varchar("kkkk"); //k = 107
  start.set_varchar("cccc"); //c = 99
  end.set_varchar("ffff");//f = 102

  ASSERT_TRUE(OB_SUCC(ObOptEstObjToScalar::convert_objs_to_scalars(
      &min, &max, &start, &end, &mins, &maxs, &starts, &ends)));
  ASSERT_TRUE(double_eq(mins, 0, 0.00001));
  ASSERT_TRUE(double_eq(maxs, 0.4, 0.00001));
  ASSERT_TRUE(double_eq(starts, 0.08, 0.00001));
  ASSERT_TRUE(double_eq(ends, 0.2, 0.00001));

  // lower character number mixed : base = 122 - 48 + 1 = 75

  min.set_varchar("a7aa"); //a = 97
  max.set_varchar("kk1k"); //k = 107
  start.set_varchar("c0cc"); //c = 99
  end.set_varchar("fff6");//f = 102

  ASSERT_TRUE(OB_SUCC(ObOptEstObjToScalar::convert_objs_to_scalars(
      &min, &max, &start, &end, &mins, &maxs, &starts, &ends)));
  ASSERT_TRUE(double_eq(mins, 0.654695, 0.00001));
  ASSERT_TRUE(double_eq(maxs, 0.797160, 0.00001));
  ASSERT_TRUE(double_eq(starts, 0.680123, 0.00001));
  ASSERT_TRUE(double_eq(ends, 0.729728, 0.00001));

  // other char number mixed, base = width([0-9] union [CHAR_OCCURED])
  start.set_varchar("123@4.56"); //. = 46 @ = 64 base = 19
  end.set_varchar("123123@4.56");
  ASSERT_TRUE(OB_SUCC(ObOptEstObjToScalar::convert_objs_to_scalars(
      NULL, NULL, &start, &end, NULL, NULL, &starts, &ends)));
  ASSERT_TRUE(double_eq(starts, 0.964046, 0.00001));
  ASSERT_TRUE(double_eq(ends, 0.169845, 0.00001));
}

TEST_F(TestOptimizer, sort_cost)
{
  int ret = OB_SUCCESS;
  //Sort cost test
  //default row-shape : 10 ints, sort 3 ints
  double rows = 1000.0;
  ObExprResType t_int;
  double cost = 0.0;
  t_int.set_int();
  ObSEArray<ObExprResType, 10> row_types;
  double width = 4 * 10;
  ObSEArray<ObExprResType, 3> sort_types;

  for (int64_t i = 0; i < 10; ++i) {
    row_types.push_back(t_int);
  }

  for (int64_t i = 0; i < 3; ++i) {
    sort_types.push_back(t_int);
  }
  const char* tmp_file = "./test_optimizer_sort_cost.tmp";
  const char* result_file = "./test_optimizer_sort_cost.result";
  ObSEArray<OrderItem, 1> order_items;
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  ObSortCostInfo cost_info(rows, width, 0, order_items, false);
  ret = ObOptEstCost::cost_sort(cost_info, cost, ObOptEstCost::VECTOR_MODEL);
  //add_cost=348.846000000000, get_cost=422.466000000000, real_sort_cost=262.178013484400
  ASSERT_TRUE(OB_SUCCESS == ret);
  of_result << cost << std::endl;
  //ASSERT_TRUE(1048 == cost);

  rows = 10000.0;
  cost_info.rows_ = rows;
  //add_cost=3398.460000000000, get_cost=4206.660000000000, real_sort_cost=3495.706846458661
  ret = ObOptEstCost::cost_sort(cost_info, cost, ObOptEstCost::VECTOR_MODEL);
  ASSERT_TRUE(OB_SUCCESS == ret);
  of_result << cost << std::endl;
  //ASSERT_TRUE(11115 == cost);

  rows = 100000.0;
  cost_info.rows_ = rows;
  //add_cost=33894.599999999999, get_cost=42048.599999999999, real_sort_cost=43696.335580733263
  ret = ObOptEstCost::cost_sort(cost_info, cost, ObOptEstCost::VECTOR_MODEL);
  ASSERT_TRUE(OB_SUCCESS == ret);
  of_result << cost << std::endl;
  //ASSERT_TRUE(119654 == cost);

  rows = 1000000.0;
  cost_info.rows_ = rows;
  //We see that even at rows = 1000000, row_store cost is still larger than real sort cost, even not
  //counting cost of cache/TLB miss
  //add_cost=338856.000000000000, get_cost=420467.999999999942, real_sort_cost=524356.026968799182
  ret = ObOptEstCost::cost_sort(cost_info, cost, ObOptEstCost::VECTOR_MODEL);
  ASSERT_TRUE(OB_SUCCESS == ret);
  of_result << cost << std::endl;
  //ASSERT_TRUE(1283695 == cost);



  //Test for mixed types
  ObExprResType t_vc;
  t_vc.set_varchar();
  t_vc.set_length(255);
  ObExprResType t_num;
  t_num.set_number();
  ObExprResType t_ts;
  t_ts.set_timestamp();

  row_types.push_back(t_vc);
  row_types.push_back(t_vc);
  row_types.push_back(t_vc);
  row_types.push_back(t_num);
  row_types.push_back(t_num);
  row_types.push_back(t_ts);
  row_types.push_back(t_ts);

  sort_types.reset();
  sort_types.push_back(t_vc);
  sort_types.push_back(t_num);
  sort_types.push_back(t_int);
  sort_types.push_back(t_ts);

  rows = 1000.0;
  width = 255 + 16 + 16;
  cost_info.rows_ = rows;
  cost_info.width_ = width;
  //add_cost=1792.210480000000, get_cost=699.636980000000, real_sort_cost=558.160103197957
  ret = ObOptEstCost::cost_sort(cost_info, cost, ObOptEstCost::VECTOR_MODEL);
  ASSERT_TRUE(OB_SUCCESS == ret);
  of_result << cost << std::endl;
  //ASSERT_TRUE(3065 == cost);

  rows = 10000.0;
  cost_info.rows_ = rows;
  //add_cost=17832.104800000001, get_cost=6978.369799999999, real_sort_cost=7442.134709306095
  ret = ObOptEstCost::cost_sort(cost_info, cost, ObOptEstCost::VECTOR_MODEL);
  ASSERT_TRUE(OB_SUCCESS == ret);
  of_result << cost << std::endl;
  //ASSERT_TRUE(32267 == cost);


  rows = 100000.0;
  //add_cost=178231.048000000039, get_cost=69765.697999999989, real_sort_cost=93026.683866326173
  cost_info.rows_ = rows;
  ret = ObOptEstCost::cost_sort(cost_info, cost, ObOptEstCost::VECTOR_MODEL);
  ASSERT_TRUE(OB_SUCCESS == ret);
  of_result << cost << std::endl;
  //ASSERT_TRUE(341038 == cost);

  rows = 1000000.0;
  cost_info.rows_ = rows;
  //add_cost=1782220.480000000214, get_cost=697638.979999999865, real_sort_cost=1116320.206395914080
  ret = ObOptEstCost::cost_sort(cost_info, cost, ObOptEstCost::VECTOR_MODEL);
  ASSERT_TRUE(OB_SUCCESS == ret);
  of_result << cost << std::endl;
  //ASSERT_TRUE(3596194 == cost);

  of_result.close();
  //std::cout << "diff -u " << tmp_file << " " << result_file << std::endl;
  TestSqlUtils::is_equal_content(tmp_file, result_file);
}


TEST_F(TestOptimizer, scan_cost) {
  ObCostTableScanInfo est_cost_info(OB_INVALID_ID, OB_INVALID_ID, OB_INVALID_ID);
  ObTableMetaInfo table_meta_info(OB_INVALID_ID);
  est_cost_info.table_meta_info_ = &table_meta_info;
  double STANDARD_SCHEMA_COL_COUNT = 50;
  est_cost_info.table_meta_info_->micro_block_size_ = 16384;
  est_cost_info.table_meta_info_->table_rowkey_count_ = 3;
  est_cost_info.table_meta_info_->table_row_count_ = 100000;
  est_cost_info.table_meta_info_->part_count_ = 1;
  est_cost_info.index_meta_info_.index_part_size_ = OB_EST_DEFAULT_DATA_SIZE;
  est_cost_info.index_meta_info_.index_micro_block_size_ = 16384;
  est_cost_info.batch_type_ = common::ObSimpleBatch::T_SCAN;
  bool index_back = false;
  int64_t column_count = 0;
  double access_row_count = 0.0;
  double cost = 0;
  double index_back_cost = 0;

  //Temporary:if a scan has a full open range(min to max), it will be given a small increase of
  //cost, leading optimizer to prefer access paths with ranges. 2 conditions to trigger.
  //condition 1 : scan without range
  est_cost_info.index_meta_info_.index_column_count_ = static_cast<int64_t>(STANDARD_SCHEMA_COL_COUNT);
  column_count = 10;
  access_row_count = 500.0;
  index_back = false;
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
 // ASSERT_TRUE(1266 == cost);

  est_cost_info.index_meta_info_.index_column_count_ = static_cast<int64_t>(STANDARD_SCHEMA_COL_COUNT);
  column_count = 10;
  access_row_count = 500.0;
  index_back = false;
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
//  ASSERT_TRUE(502 == cost);

  //condition 2 : scan more than 5 blocks
  est_cost_info.index_meta_info_.index_column_count_ = static_cast<int64_t>(STANDARD_SCHEMA_COL_COUNT);
  column_count = 10;
  access_row_count = 1000.0;
  index_back = false;
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
//  ASSERT_TRUE(1718 == cost);

  //the more rows, the bigger cost is
  access_row_count = 10000.0;
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
//  ASSERT_TRUE(9845 == cost);

  //the more rows, the bigger cost is
  access_row_count = 100000.0;
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
//  ASSERT_TRUE(97496 == cost);

  //the more columns, the bigger cost is
  column_count = 20;
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
//  ASSERT_TRUE(126709 == cost);

  //the wider the table is, the bigger cost is
  est_cost_info.index_meta_info_.index_column_count_ = 2 * static_cast<int64_t>(STANDARD_SCHEMA_COL_COUNT);
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
//  ASSERT_TRUE(182992 == cost);

  //scan index back
  index_back = true;
  est_cost_info.postfix_filter_sel_ = 0.1;
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
//  ASSERT_TRUE(283235 == cost);

  //get
  est_cost_info.batch_type_ = common::ObSimpleBatch::T_GET;
  est_cost_info.postfix_filter_sel_ = 1.0;
  est_cost_info.index_meta_info_.index_column_count_ = static_cast<int64_t>(STANDARD_SCHEMA_COL_COUNT);
  column_count = 10;
  access_row_count = 500.0;
  index_back = false;
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
//  ASSERT_TRUE(6751 == cost);

  //the more rows, the bigger cost is
  access_row_count = 1000;
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
//  ASSERT_TRUE(13503 == cost);

  //Compare of scan and get at 1000 rows
  //cost of get is roughly 10 times bigger than cost of scan at same row count
  est_cost_info.batch_type_ = common::ObSimpleBatch::T_SCAN;
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
//  ASSERT_TRUE(1718 == cost);
  est_cost_info.batch_type_ = common::ObSimpleBatch::T_GET;

  //the more columns, the bigger cost is
  column_count = 20;
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
 // ASSERT_TRUE(14041 == cost);

  //get index back
  index_back = true;
  ObOptEstCost::cost_table(est_cost_info, 1, access_row_count, access_row_count, cost, index_back_cost, ObOptEstCost::VECTOR_MODEL);
  //std::cout << type << ',' << access_row_count << ',' << column_count << ',' << cost << std::endl;
//  ASSERT_TRUE(27168 == cost);
}


//TEST_F(TestOptimizer, ob_optimizer_failed)
//{
//  const char* test_file = "./test_optimizer_failed.sql";
//  ASSERT_NO_FATAL_FAILURE(run_fail_test(test_file));
//}

// hint
TEST_F(TestOptimizer, ob_optimizer_hint)
{
  const char* test_file = "./test_optimizer_hint.sql";
  const char* result_file = "./test_optimizer_hint.result";
  const char* tmp_file = "./test_optimizer_hint.tmp";
  explain_type_ = EXPLAIN_OUTLINE;
  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file));
}

TEST_F(TestOptimizer, ob_optimizer_outline)
{
  const char* test_file = "./test_optimizer_outline.sql";
  const char* result_file = "./test_optimizer_outline.result";
  const char* tmp_file = "./test_optimizer_outline.tmp";
  explain_type_ = EXPLAIN_OUTLINE;
  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file));
}

TEST_F(TestOptimizer, ob_optimizer_used_hint)
{
  const char* test_file = "./test_optimizer_used_hint.sql";
  const char* result_file = "./test_optimizer_used_hint.result";
  const char* tmp_file = "./test_optimizer_used_hint.tmp";
  explain_type_ = EXPLAIN_EXTENDED;

  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file));
}

/*
todo(@ banliu.zyd): 功能还没有测全，暂时注释
TEST_F(TestOptimizer, ob_optimizer_hualong)
{
  const char* test_file = "./test_optimizer_hualong.sql";
  const char* result_file = "./test_optimizer_hualong.result";
  const char* tmp_file = "./test_optimizer_hualong.tmp";
  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file));
}

TEST_F(TestOptimizer, ob_optimizer_hualong_default)
{
  const char* test_file = "./test_optimizer_hualong.sql";
  const char* result_file = "./test_optimizer_hualong.result";
  const char* tmp_file = "./test_optimizer_hualong.tmp";
  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file, true));
}
*/


TEST_F(TestOptimizer, ob_optimizer_topk)
{
  const char* test_file = "./test_optimizer_topk.sql";
  const char* result_file = "./test_optimizer_topk.result";
  const char* tmp_file = "./test_optimizer_topk.tmp";
  explain_type_ = EXPLAIN_EXTENDED;

  ASSERT_NO_FATAL_FAILURE(run_test(test_file, result_file, tmp_file));
}
}
int main(int argc, char **argv)
{
  //char c;
  //std::cout<<"Press any key to continue..."<<std::endl;
  //std::cin>>c;
  set_memory_limit(10L << 30);
  system("rm -rf test_optimizer.log");
  observer::ObReqTimeGuard req_timeinfo_guard;
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_optimizer.log", true);
  ObServerOptions opts;
  opts.cluster_id_ = 1;
  opts.rs_list_ = "127.0.0.1:1";
  opts.zone_ = "test1";
  opts.data_dir_ = "/tmp";
  opts.mysql_port_ = 23000 + getpid() % 1000;
  opts.rpc_port_ = 24000 + getpid() % 1000;
  system("mkdir -p /tmp/sstable /tmp/clog /tmp/slog");
  GCONF.datafile_size = 41943040;
  //ObServer::get_instance().init(opts);
  oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc,argv);
  if(argc >= 2)
  {
    if (strcmp("DEBUG", argv[1]) == 0
        || strcmp("WARN", argv[1]) == 0)
    OB_LOGGER.set_log_level(argv[1]);
  }
  test::parse_cmd_line_param(argc, argv, test::clp);
  int ret = RUN_ALL_TESTS();
  OB_LOGGER.set_log_level("ERROR");
  return ret;
}
