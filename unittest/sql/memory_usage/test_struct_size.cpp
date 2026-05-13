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

#define USING_LOG_PREFIX SQL
#include <fstream>
#include <gtest/gtest.h>
#define private public
#define protected public
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dml/ob_insert_all_stmt.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/resolver/dml/ob_transpose_resolver.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_join_filter.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_update.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_function_table.h"
#include "sql/optimizer/ob_log_json_table.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/optimizer/ob_log_material.h"
#include "sql/optimizer/ob_log_window_function.h"
#include "sql/optimizer/ob_log_select_into.h"
#include "sql/optimizer/ob_log_topk.h"
#include "sql/optimizer/ob_log_count.h"
#include "sql/optimizer/ob_log_sequence.h"
#include "sql/optimizer/ob_log_merge.h"
#include "sql/optimizer/ob_log_granule_iterator.h"
#include "sql/optimizer/ob_log_monitoring_dump.h"
#include "sql/optimizer/ob_log_unpivot.h"
#include "sql/optimizer/ob_log_link_scan.h"
#include "sql/optimizer/ob_log_for_update.h"
#include "sql/optimizer/ob_log_temp_table_insert.h"
#include "sql/optimizer/ob_log_temp_table_access.h"
#include "sql/optimizer/ob_log_temp_table_transformation.h"
#include "sql/optimizer/ob_log_insert_all.h"
#include "sql/optimizer/ob_log_err_log.h"
#include "sql/optimizer/ob_log_stat_collector.h"
#include "sql/optimizer/ob_del_upd_log_plan.h"
#include "sql/optimizer/ob_log_link_dml.h"
#include "sql/optimizer/ob_log_values_table_access.h"
#include "sql/optimizer/ob_log_expand.h"
#include "sql/optimizer/ob_skyline_prunning.h"
#undef protected
#undef private
#include "sql/optimizer/ob_lake_table_partition_info.h"

using namespace oceanbase::obrpc;
using namespace oceanbase::sql;
namespace test
{

#define PRINT_HEADER(out) \
  out << "| class | sizeof(class) |" << std::endl; \
  out << "| --- | --- |" << std::endl;
#define PRINT_SIZE(out, type) out << "| " << #type << " | " << sizeof(type) << " |" << std::endl;
#define PRINT_MEMBER_SIZE(out, member) \
    out << "| " << #member << " | " << sizeof(obj.member) << " |" << std::endl; \
    total_size += sizeof(obj.member);

#define PRINT_MEMBER_FINISH(out, type) \
    out << "total_size = " << total_size << ", fragment = " \
        << static_cast<int64_t>(sizeof(type)) - total_size << std::endl; \
    out << std::endl;

class TestSQLCompile: public ::testing::Test
{
public:
  TestSQLCompile();
  virtual ~TestSQLCompile() {}
};

TestSQLCompile::TestSQLCompile()
{
}

void verify_results(const char* result_file, const char* tmp_file) {
  fprintf(stderr, "If tests failed, use `diff %s %s' to see the differences. \n", result_file, tmp_file);
  std::ifstream if_result(tmp_file);
  ASSERT_TRUE(if_result.is_open());
  std::istream_iterator<std::string> it_result(if_result);
  std::ifstream if_expected(result_file);
  ASSERT_TRUE(if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  ASSERT_TRUE(std::equal(it_result, std::istream_iterator<std::string>(), it_expected));
  std::remove(tmp_file);
}

TEST_F(TestSQLCompile, stmt_size)
{
  static const char* tmp_file = "./resolver/test_stmt_size.tmp";
  static const char* result_file = "./resolver/test_stmt_size.result";

  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  PRINT_HEADER(of_result);
  PRINT_SIZE(of_result, ObStmt)
  PRINT_SIZE(of_result, ObDMLStmt)
  PRINT_SIZE(of_result, ObSelectStmt)
  PRINT_SIZE(of_result, ObDelUpdStmt)
  PRINT_SIZE(of_result, ObInsertStmt)
  PRINT_SIZE(of_result, ObInsertAllStmt)
  PRINT_SIZE(of_result, ObUpdateStmt)
  PRINT_SIZE(of_result, ObDeleteStmt)
  PRINT_SIZE(of_result, ObMergeStmt)
  of_result.close();
  verify_results(result_file, tmp_file);
}

TEST_F(TestSQLCompile, stmt_member_size)
{
  static const char* tmp_file = "./resolver/test_stmt_member_size.tmp";
  static const char* result_file = "./resolver/test_stmt_member_size.result";

  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  int64_t total_size = 0;
  ObArenaAllocator allocator;

  {
    ObSelectStmt obj(allocator);
    // ObStmt
    PRINT_HEADER(of_result);
    PRINT_SIZE(of_result, ObStmt)
    of_result << "| vtable | 8 |" << std:: endl;
    total_size = 8;
    PRINT_MEMBER_SIZE(of_result, stmt_type_);
    PRINT_MEMBER_SIZE(of_result, query_ctx_);
    PRINT_MEMBER_SIZE(of_result, stmt_id_);
    PRINT_MEMBER_FINISH(of_result, ObStmt);
    // ObDMLStmt
    PRINT_HEADER(of_result);
    PRINT_SIZE(of_result, ObDMLStmt)
    PRINT_SIZE(of_result, ObStmt)
    total_size = sizeof(ObStmt);
    PRINT_MEMBER_SIZE(of_result, order_items_);
    PRINT_MEMBER_SIZE(of_result, limit_count_expr_);
    PRINT_MEMBER_SIZE(of_result, limit_offset_expr_);
    PRINT_MEMBER_SIZE(of_result, limit_percent_expr_);
    PRINT_MEMBER_SIZE(of_result, has_fetch_);
    PRINT_MEMBER_SIZE(of_result, is_fetch_with_ties_);
    PRINT_MEMBER_SIZE(of_result, from_items_);
    PRINT_MEMBER_SIZE(of_result, part_expr_items_);
    PRINT_MEMBER_SIZE(of_result, joined_tables_);
    PRINT_MEMBER_SIZE(of_result, stmt_hint_);
    PRINT_MEMBER_SIZE(of_result, semi_infos_);
    PRINT_MEMBER_SIZE(of_result, autoinc_params_);
    PRINT_MEMBER_SIZE(of_result, is_calc_found_rows_);
    PRINT_MEMBER_SIZE(of_result, has_top_limit_);
    PRINT_MEMBER_SIZE(of_result, is_contains_assignment_);
    PRINT_MEMBER_SIZE(of_result, affected_last_insert_id_);
    PRINT_MEMBER_SIZE(of_result, has_part_key_sequence_);
    PRINT_MEMBER_SIZE(of_result, nextval_sequence_ids_);
    PRINT_MEMBER_SIZE(of_result, currval_sequence_ids_);
    PRINT_MEMBER_SIZE(of_result, table_items_);
    PRINT_MEMBER_SIZE(of_result, column_items_);
    PRINT_MEMBER_SIZE(of_result, condition_exprs_);
    PRINT_MEMBER_SIZE(of_result, pseudo_column_like_exprs_);
    PRINT_MEMBER_SIZE(of_result, tables_hash_);
    PRINT_MEMBER_SIZE(of_result, subquery_exprs_);
    PRINT_MEMBER_SIZE(of_result, unpivot_item_);
    PRINT_MEMBER_SIZE(of_result, user_var_exprs_);
    PRINT_MEMBER_SIZE(of_result, check_constraint_items_);
    PRINT_MEMBER_SIZE(of_result, cte_definitions_);
    PRINT_MEMBER_SIZE(of_result, dblink_id_);
    PRINT_MEMBER_SIZE(of_result, is_reverse_link_);
    PRINT_MEMBER_SIZE(of_result, has_vec_approx_);
    PRINT_MEMBER_SIZE(of_result, match_exprs_);
    PRINT_MEMBER_SIZE(of_result, vector_index_query_param_);
    PRINT_MEMBER_FINISH(of_result, ObDMLStmt);
    // ObSelectStmt
    PRINT_HEADER(of_result);
    PRINT_SIZE(of_result, ObSelectStmt)
    PRINT_SIZE(of_result, ObDMLStmt)
    total_size = sizeof(ObDMLStmt);
    PRINT_MEMBER_SIZE(of_result, set_op_);
    PRINT_MEMBER_SIZE(of_result, is_recursive_cte_);
    PRINT_MEMBER_SIZE(of_result, is_breadth_search_);
    PRINT_MEMBER_SIZE(of_result, is_distinct_);
    PRINT_MEMBER_SIZE(of_result, is_nocycle_);
    PRINT_MEMBER_SIZE(of_result, search_by_items_);
    PRINT_MEMBER_SIZE(of_result, cycle_by_items_);
    PRINT_MEMBER_SIZE(of_result, cte_exprs_);
    PRINT_MEMBER_SIZE(of_result, select_items_);
    PRINT_MEMBER_SIZE(of_result, group_exprs_);
    PRINT_MEMBER_SIZE(of_result, rollup_exprs_);
    PRINT_MEMBER_SIZE(of_result, having_exprs_);
    PRINT_MEMBER_SIZE(of_result, agg_items_);
    PRINT_MEMBER_SIZE(of_result, win_func_exprs_);
    PRINT_MEMBER_SIZE(of_result, qualify_filters_);
    PRINT_MEMBER_SIZE(of_result, start_with_exprs_);
    PRINT_MEMBER_SIZE(of_result, connect_by_exprs_);
    PRINT_MEMBER_SIZE(of_result, connect_by_prior_exprs_);
    PRINT_MEMBER_SIZE(of_result, rollup_directions_);
    PRINT_MEMBER_SIZE(of_result, grouping_sets_items_);
    PRINT_MEMBER_SIZE(of_result, rollup_items_);
    PRINT_MEMBER_SIZE(of_result, cube_items_);
    PRINT_MEMBER_SIZE(of_result, for_update_columns_);
    PRINT_MEMBER_SIZE(of_result, is_set_distinct_);
    PRINT_MEMBER_SIZE(of_result, set_query_);
    PRINT_MEMBER_SIZE(of_result, for_update_dml_info_);
    PRINT_MEMBER_SIZE(of_result, show_stmt_ctx_);
    PRINT_MEMBER_SIZE(of_result, is_view_stmt_);
    PRINT_MEMBER_SIZE(of_result, view_ref_id_);
    PRINT_MEMBER_SIZE(of_result, select_type_);
    PRINT_MEMBER_SIZE(of_result, into_item_);
    PRINT_MEMBER_SIZE(of_result, is_match_topk_);
    PRINT_MEMBER_SIZE(of_result, children_swapped_);
    PRINT_MEMBER_SIZE(of_result, is_from_pivot_);
    PRINT_MEMBER_SIZE(of_result, check_option_);
    PRINT_MEMBER_SIZE(of_result, contain_ab_param_);
    PRINT_MEMBER_SIZE(of_result, is_order_siblings_);
    PRINT_MEMBER_SIZE(of_result, is_hierarchical_query_);
    PRINT_MEMBER_SIZE(of_result, has_prior_);
    PRINT_MEMBER_SIZE(of_result, has_reverse_link_);
    PRINT_MEMBER_SIZE(of_result, is_expanded_mview_);
    PRINT_MEMBER_SIZE(of_result, is_select_straight_join_);
    PRINT_MEMBER_SIZE(of_result, is_implicit_distinct_);
    PRINT_MEMBER_SIZE(of_result, is_oracle_compat_groupby_);
    PRINT_MEMBER_SIZE(of_result, is_recursive_union_branch_);
    PRINT_MEMBER_SIZE(of_result, for_update_cursor_table_id_);
    PRINT_MEMBER_FINISH(of_result, ObSelectStmt);
  }
  {
    ObDeleteStmt obj(allocator);
    // ObDelUpdStmt
    PRINT_HEADER(of_result);
    PRINT_SIZE(of_result, ObDelUpdStmt)
    PRINT_SIZE(of_result, ObDMLStmt)
    total_size = sizeof(ObDMLStmt);
    PRINT_MEMBER_SIZE(of_result, returning_exprs_);
    PRINT_MEMBER_SIZE(of_result, returning_into_exprs_);
    PRINT_MEMBER_SIZE(of_result, returning_strs_);
    PRINT_MEMBER_SIZE(of_result, returning_agg_items_);
    PRINT_MEMBER_SIZE(of_result, group_param_exprs_);
    PRINT_MEMBER_SIZE(of_result, ignore_);
    PRINT_MEMBER_SIZE(of_result, has_global_index_);
    PRINT_MEMBER_SIZE(of_result, error_log_info_);
    PRINT_MEMBER_SIZE(of_result, has_instead_of_trigger_);
    PRINT_MEMBER_SIZE(of_result, sharding_conditions_);
    PRINT_MEMBER_SIZE(of_result, ab_stmt_id_expr_);
    PRINT_MEMBER_SIZE(of_result, dml_source_from_join_);
    PRINT_MEMBER_SIZE(of_result, pdml_disabled_);
    PRINT_MEMBER_FINISH(of_result, ObDelUpdStmt);
    // ObDeleteStmt
    PRINT_HEADER(of_result);
    PRINT_SIZE(of_result, ObDeleteStmt)
    PRINT_SIZE(of_result, ObDelUpdStmt)
    total_size = sizeof(ObDelUpdStmt);
    PRINT_MEMBER_SIZE(of_result, table_info_);
    PRINT_MEMBER_FINISH(of_result, ObDeleteStmt);
  }

  {
    ObUpdateStmt obj(allocator);
    // ObUpdateStmt
    PRINT_HEADER(of_result);
    PRINT_SIZE(of_result, ObUpdateStmt)
    PRINT_SIZE(of_result, ObDelUpdStmt)
    total_size = sizeof(ObDelUpdStmt);
    PRINT_MEMBER_SIZE(of_result, table_info_);
    PRINT_MEMBER_FINISH(of_result, ObUpdateStmt);
  }

  {
    ObInsertStmt obj(allocator);
    // ObInsertStmt
    PRINT_HEADER(of_result);
    PRINT_SIZE(of_result, ObInsertStmt)
    PRINT_SIZE(of_result, ObDelUpdStmt)
    total_size = sizeof(ObDelUpdStmt);
    PRINT_MEMBER_SIZE(of_result, is_all_const_values_);
    PRINT_MEMBER_SIZE(of_result, table_info_)
    PRINT_MEMBER_FINISH(of_result, ObInsertStmt);
  }
  {
    ObInsertAllStmt obj(allocator);
    // ObInsertAllStmt
    PRINT_HEADER(of_result);
    PRINT_SIZE(of_result, ObInsertAllStmt)
    PRINT_SIZE(of_result, ObDelUpdStmt)
    total_size = sizeof(ObDelUpdStmt);
    PRINT_MEMBER_SIZE(of_result, is_multi_insert_first_);
    PRINT_MEMBER_SIZE(of_result, is_multi_condition_insert_);
    PRINT_MEMBER_SIZE(of_result, table_info_);
    PRINT_MEMBER_FINISH(of_result, ObInsertAllStmt);
  }
  {
    ObMergeStmt obj(allocator);
    // ObMergeStmt
    PRINT_HEADER(of_result);
    PRINT_SIZE(of_result, ObMergeStmt)
    PRINT_SIZE(of_result, ObDelUpdStmt)
    total_size = sizeof(ObDelUpdStmt);
    PRINT_MEMBER_SIZE(of_result, table_info_);
    PRINT_MEMBER_FINISH(of_result, ObMergeStmt);
  }

  of_result.close();
  verify_results(result_file, tmp_file);
}

TEST_F(TestSQLCompile, stmt_related_struct_size)
{
  static const char* tmp_file = "./resolver/test_stmt_related_struct_size.tmp";
  static const char* result_file = "./resolver/test_stmt_related_struct_size.result";

  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  PRINT_HEADER(of_result);
  PRINT_SIZE(of_result, TableItem)
  PRINT_SIZE(of_result, ObJtColBaseInfo)
  PRINT_SIZE(of_result, ObJsonTableDef)
  PRINT_SIZE(of_result, ObUnpivotItem)
  PRINT_SIZE(of_result, TransposeDef)
  PRINT_SIZE(of_result, ObValuesTableDef)
  PRINT_SIZE(of_result, ColumnItem)
  PRINT_SIZE(of_result, FromItem)
  PRINT_SIZE(of_result, SemiInfo)
  PRINT_SIZE(of_result, PartExprItem)
  PRINT_SIZE(of_result, CheckConstraintItem)
  PRINT_SIZE(of_result, SelectItem)
  PRINT_SIZE(of_result, ObSelectIntoItem)
  PRINT_SIZE(of_result, ObGroupbyExpr)
  PRINT_SIZE(of_result, ObRollupItem)
  PRINT_SIZE(of_result, ObCubeItem)
  PRINT_SIZE(of_result, ObGroupingSetsItem)
  PRINT_SIZE(of_result, ForUpdateDMLInfo)
  PRINT_SIZE(of_result, OrderItem)
  of_result.close();
  verify_results(result_file, tmp_file);
}

TEST_F(TestSQLCompile, join_order_size)
{
  static const char* tmp_file = "./optimizer/test_join_order_size.tmp";
  static const char* result_file = "./optimizer/test_join_order_size.result";

  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  PRINT_HEADER(of_result);
  PRINT_SIZE(of_result, ObJoinOrder)
  PRINT_SIZE(of_result, Path)
  PRINT_SIZE(of_result, AccessPath)
  PRINT_SIZE(of_result, JoinPath)
  PRINT_SIZE(of_result, SubQueryPath)
  PRINT_SIZE(of_result, FunctionTablePath)
  PRINT_SIZE(of_result, JsonTablePath)
  PRINT_SIZE(of_result, TempTablePath)
  PRINT_SIZE(of_result, CteTablePath)
  PRINT_SIZE(of_result, ValuesTablePath)
  of_result.close();
  verify_results(result_file, tmp_file);
}

TEST_F(TestSQLCompile, log_plan_size)
{
  static const char* tmp_file = "./optimizer/test_log_plan_size.tmp";
  static const char* result_file = "./optimizer/test_log_plan_size.result";

  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  PRINT_HEADER(of_result);
  PRINT_SIZE(of_result, ObLogPlan)
  PRINT_SIZE(of_result, ObLogicalOperator)
  PRINT_SIZE(of_result, ObLogGroupBy)
  PRINT_SIZE(of_result, ObLogSort)
  PRINT_SIZE(of_result, ObLogJoin)
  PRINT_SIZE(of_result, ObLogTableScan)
  PRINT_SIZE(of_result, ObLogLimit)
  PRINT_SIZE(of_result, ObLogExchange)
  PRINT_SIZE(of_result, ObLogSubPlanScan)
  PRINT_SIZE(of_result, ObLogSubPlanFilter)
  PRINT_SIZE(of_result, ObLogInsert)
  PRINT_SIZE(of_result, ObLogSet)
  PRINT_SIZE(of_result, ObLogUpdate)
  PRINT_SIZE(of_result, ObLogDelete)
  PRINT_SIZE(of_result, ObLogDistinct)
  PRINT_SIZE(of_result, ObLogExprValues)
  PRINT_SIZE(of_result, ObLogValues)
  PRINT_SIZE(of_result, ObLogMaterial)
  PRINT_SIZE(of_result, ObLogWindowFunction)
  PRINT_SIZE(of_result, ObLogSelectInto)
  PRINT_SIZE(of_result, ObLogTopk)
  PRINT_SIZE(of_result, ObLogCount)
  PRINT_SIZE(of_result, ObLogMerge)
  PRINT_SIZE(of_result, ObLogGranuleIterator)
  PRINT_SIZE(of_result, ObLogJoinFilter)
  PRINT_SIZE(of_result, ObLogSequence)
  PRINT_SIZE(of_result, ObLogMonitoringDump)
  PRINT_SIZE(of_result, ObLogFunctionTable)
  PRINT_SIZE(of_result, ObLogJsonTable)
  PRINT_SIZE(of_result, ObLogUnpivot)
  PRINT_SIZE(of_result, ObLogTempTableInsert)
  PRINT_SIZE(of_result, ObLogTempTableAccess)
  PRINT_SIZE(of_result, ObLogTempTableTransformation)
  PRINT_SIZE(of_result, ObLogLinkScan)
  PRINT_SIZE(of_result, ObLogLinkDml)
  PRINT_SIZE(of_result, ObLogForUpdate)
  PRINT_SIZE(of_result, ObLogInsertAll)
  PRINT_SIZE(of_result, ObLogErrLog)
  PRINT_SIZE(of_result, ObLogStatCollector)
  PRINT_SIZE(of_result, ObLogOptimizerStatsGathering)
  PRINT_SIZE(of_result, ObLogValuesTableAccess)
  PRINT_SIZE(of_result, ObLogExpand)
  of_result.close();
  verify_results(result_file, tmp_file);
}

TEST_F(TestSQLCompile, optimizer_related_struct_size)
{
  static const char* tmp_file = "./optimizer/test_optimizer_related_struct_size.tmp";
  static const char* result_file = "./optimizer/test_optimizer_related_struct_size.result";

  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());

  of_result << "# join order" << std::endl;
  PRINT_HEADER(of_result);
  PRINT_SIZE(of_result, BaseTableOptInfo)
  PRINT_SIZE(of_result, JoinFilterInfo)
  PRINT_SIZE(of_result, DomainIndexAccessInfo)
  PRINT_SIZE(of_result, ObIndexMergeNode)
  PRINT_SIZE(of_result, InnerPathInfo)
  PRINT_SIZE(of_result, ObConflictDetector)
  PRINT_SIZE(of_result, JoinInfo)
  PRINT_SIZE(of_result, IndexInfoEntry)
  PRINT_SIZE(of_result, OrderingInfo)
  PRINT_SIZE(of_result, QueryRangeInfo)
  PRINT_SIZE(of_result, ObSkylineDim)
  PRINT_SIZE(of_result, ObIndexBackDim)
  PRINT_SIZE(of_result, ObInterestOrderDim)
  PRINT_SIZE(of_result, ObQueryRangeDim)
  PRINT_SIZE(of_result, ObUniqueRangeDim)
  PRINT_SIZE(of_result, ObShardingInfoDim)

  of_result << std::endl;
  of_result << "# cost model" << std::endl;
  PRINT_HEADER(of_result);
  PRINT_SIZE(of_result, ObTableMetaInfo)
  PRINT_SIZE(of_result, ObIndexMetaInfo)
  PRINT_SIZE(of_result, ObCostTableScanInfo)
  PRINT_SIZE(of_result, ObDSFailTabInfo)
  PRINT_SIZE(of_result, ObDSTableParam)
  PRINT_SIZE(of_result, ObDSResultItem)
  PRINT_SIZE(of_result, ObDSStatItem)

  of_result << std::endl;
  of_result << "# log plan" << std::endl;
  PRINT_HEADER(of_result);
  PRINT_SIZE(of_result, OptTableMetas)
  PRINT_SIZE(of_result, OSGShareInfo)
  PRINT_SIZE(of_result, TableDependInfo)
  PRINT_SIZE(of_result, ObTopNFilterInfo)
  PRINT_SIZE(of_result, ObTextRetrievalInfo)
  PRINT_SIZE(of_result, ObRawFilterMonotonicity)
  PRINT_SIZE(of_result, ObRawAggrParamMonotonicity)
  PRINT_SIZE(of_result, ObVecIndexInfo)
  PRINT_SIZE(of_result, ObThreeStageAggrInfo)
  PRINT_SIZE(of_result, ObRollupAdaptiveInfo)
  PRINT_SIZE(of_result, ObHashRollupInfo)
  PRINT_SIZE(of_result, ObGroupingSetInfo)
  PRINT_SIZE(of_result, IndexDMLInfo)

  of_result << std::endl;
  of_result << "# location" << std::endl;
  PRINT_HEADER(of_result);
  PRINT_SIZE(of_result, ObShardingInfo)
  PRINT_SIZE(of_result, ObTablePartitionInfo)
  PRINT_SIZE(of_result, ObCandiTableLoc)
  PRINT_SIZE(of_result, ObCandiTabletLoc)
  PRINT_SIZE(of_result, ObOptTabletLoc)
  PRINT_SIZE(of_result, ObRoutePolicy::CandidateReplica)
  PRINT_SIZE(of_result, ObTableLocation)
  PRINT_SIZE(of_result, ObLakeTablePartitionInfo)

  of_result << std::endl;
  of_result << "# others" << std::endl;
  PRINT_HEADER(of_result);
  PRINT_SIZE(of_result, ObFdItem)
  PRINT_SIZE(of_result, ObTableFdItem)
  PRINT_SIZE(of_result, ObExprFdItem)
  PRINT_SIZE(of_result, PersistentEqualSets)
  PRINT_SIZE(of_result, EqualSet)
  of_result.close();
  verify_results(result_file, tmp_file);
}

}//end of namespace test

int main(int argc, char **argv)
{
  system("rm -rf test_struct_size.log*");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_struct_size.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
