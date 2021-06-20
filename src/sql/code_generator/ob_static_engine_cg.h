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

#ifndef OCEANBASE_SRC_OB_STATIC_ENGINE_CG_H_
#define OCEANBASE_SRC_OB_STATIC_ENGINE_CG_H_

#include "sql/code_generator/ob_code_generator_impl.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/sort/ob_sort_basic_info.h"

namespace oceanbase {
namespace sql {
class ObExpr;
class ObLogLimit;
class ObLimitSpec;
class ObLogDistinct;
class ObMergeDistinctSpec;
class ObHashDistinctSpec;
class ObLogMaterial;
class ObMaterialSpec;
class ObLogSort;
class ObSortSpec;
class ObLogSet;
class ObMergeSetSpec;
class ObMergeUnionSpec;
class ObMergeIntersectSpec;
class ObMergeExceptSpec;
class ObRecursiveUnionAllSpec;
class ObHashSetSpec;
class ObHashUnionSpec;
class ObHashIntersectSpec;
class ObHashExceptSpec;
class ObCountSpec;
class ObExprValuesSpec;
class ObTableMergeSpec;
class ObTableInsertSpec;
class ObTableUpdateSpec;
class ObTableUpdateReturningSpec;
class ObTableLockSpec;
class ObMultiPartLockSpec;
class ObTableInsertUpSpec;
class ObMultiTableInsertUpSpec;
class ObTableModifySpec;
class ObValuesSpec;
class ObLogTableScan;
class ObTableScanSpec;
class ObFakeCTETableSpec;
class ObHashJoinSpec;
class ObNestedLoopJoinSpec;
class ObBasicNestedLoopJoinSpec;
class ObMergeJoinSpec;
class ObJoinSpec;
class ObMonitoringDumpSpec;
class ObLogSequence;
class ObSequenceSpec;
class ObNLConnectBySpecBase;
class ObNLConnectBySpec;
class ObNLConnectByWithIndexSpec;
class ObGranuleIteratorSpec;
class ObPxReceiveSpec;
class ObPxTransmitSpec;
class ObPxFifoReceiveSpec;
class ObPxMSReceiveSpec;
class ObPxDistTransmitSpec;
class ObPxDistTransmitOp;
class ObPxRepartTransmitSpec;
class ObPxReduceTransmitSpec;
class ObPxFifoCoordSpec;
class ObPxMSCoordSpec;
class ObLogSubPlanFilter;
class ObSubPlanFilterSpec;
class ObLogSubPlanScan;
class ObSubPlanScanSpec;
class ObGroupBySpec;
class ObScalarAggregateSpec;
class ObMergeGroupBySpec;
class ObHashGroupBySpec;
class ObAggregateProcessor;
class ObAggrInfo;
class ObWindowFunctionSpec;
class WinFuncInfo;
template <int TYPE>
struct GenSpecHelper;
class ObTableLookupSpec;
class ObMultiPartTableScanSpec;
class ObMultiPartInsertSpec;
class ObMultiPartDeleteSpec;
class ObAppendSpec;
class ObTableRowStoreSpec;
class ObMultiPartUpdateSpec;
class ObMultiTableReplaceSpec;
class ObMultiTableMergeSpec;
class ObTableConflictRowFetcherSpec;
class ObRowSampleScanSpec;
class ObBlockSampleScanSpec;
class ObDirectReceiveSpec;
class ObDirectTransmitSpec;
class ObTableScanWithIndexBackSpec;
class ObPxMultiPartDeleteSpec;
class ObPxMultiPartInsertSpec;
class ObPxMultiPartUpdateSpec;
class ObTempTableAccessOpSpec;
class ObTempTableInsertOpSpec;
class ObTempTableTransformationOpSpec;

//
// code generator for static typing engine.
//
class ObStaticEngineCG : public ObCodeGeneratorImpl {
public:
  using ObCodeGeneratorImpl::ObCodeGeneratorImpl;
  template <int TYPE>
  friend class GenSpecHelper;

  ObStaticEngineCG(uint64_t min_cluster_version) : ObCodeGeneratorImpl(min_cluster_version)
  {}
  // generate physical plan
  int generate(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan) override;

  // Dangerous: there is a dragon!!!
  int generate_rt_expr(const ObRawExpr& raw_expr, ObExpr*& rt_expr);
  int generate_rt_exprs(const common::ObIArray<ObRawExpr*>& src, common::ObIArray<ObExpr*>& dst);

private:
  bool enable_pushdown_filter_to_storage(const ObLogTableScan& op);
  // Post order visit logic plan and generate operator specification.
  // %in_root_job indicate that the operator is executed in main execution thread,
  // not scheduled by distributed/remote execution or PX execution.
  int postorder_generate_op(
      ObLogicalOperator& op, ObOpSpec*& spec, const bool in_root_job, const bool is_subplan, bool& check_eval_once);
  int clear_all_exprs_specific_flag(const ObIArray<ObRawExpr*>& exprs, ObExprInfoFlag flag);
  int mark_expr_self_produced(ObRawExpr* expr);
  int mark_expr_self_produced(const ObIArray<ObRawExpr*>& exprs);
  int mark_expr_self_produced(const ObIArray<ObColumnRefRawExpr*>& exprs);
  int set_specific_flag_to_exprs(const ObIArray<ObRawExpr*>& exprs, ObExprInfoFlag flag);
  int check_expr_columnlized(const ObRawExpr* expr);
  int check_exprs_columnlized(ObLogicalOperator& op);

  // generate basic attributes (attributes of ObOpSpec class),
  int generate_spec_basic(ObLogicalOperator& op, ObOpSpec& spec, const bool check_eval_once);

  // Invoked after generate_spec() and generate_spec_basic(),
  // some operator need this phase to do some special generation.
  int generate_spec_final(ObLogicalOperator& op, ObOpSpec& spec);

  int generate_calc_exprs(const common::ObIArray<ObRawExpr*>& dep_exprs, const common::ObIArray<ObRawExpr*>& cur_exprs,
      common::ObIArray<ObExpr*>& calc_exprs, bool check_eval_once);

  /////////////////////////////////////////////////////////////////////////////////
  //
  // Code generator interface for all operator spec. When XXX operator is added,
  // you should implement the interface:
  //
  //    int generate_spec(ObLogXXX &op, ObXXXSepc &spec, const bool in_root_job);
  //
  // generate_spec() is called after generate_spec_basic() which generate attributes
  // of ObOpSpec class.
  //////////////////////////////////////////////////////////////////////////////////

  int generate_spec(ObLogLimit& op, ObLimitSpec& spec, const bool in_root_job);

  int generate_spec(ObLogDistinct& op, ObMergeDistinctSpec& spec, const bool in_root_job);
  int generate_spec(ObLogDistinct& op, ObHashDistinctSpec& spec, const bool in_root_job);

  int generate_spec(ObLogSet& op, ObHashUnionSpec& spec, const bool in_root_job);
  int generate_spec(ObLogSet& op, ObHashIntersectSpec& spec, const bool in_root_job);
  int generate_spec(ObLogSet& op, ObHashExceptSpec& spec, const bool in_root_job);
  int generate_hash_set_spec(ObLogSet& op, ObHashSetSpec& spec);

  int generate_spec(ObLogSet& op, ObMergeUnionSpec& spec, const bool in_root_job);
  int generate_spec(ObLogSet& op, ObMergeIntersectSpec& spec, const bool in_root_job);
  int generate_spec(ObLogSet& op, ObMergeExceptSpec& spec, const bool in_root_job);
  int generate_cte_pseudo_column_row_desc(ObLogSet& op, ObRecursiveUnionAllSpec& phy_set_op);
  int generate_spec(ObLogSet& op, ObRecursiveUnionAllSpec& spec, const bool in_root_job);
  int generate_merge_set_spec(ObLogSet& op, ObMergeSetSpec& spec);
  int generate_recursive_union_all_spec(ObLogSet& op, ObRecursiveUnionAllSpec& spec);

  int generate_spec(ObLogMaterial& op, ObMaterialSpec& spec, const bool in_root_job);

  int generate_spec(ObLogSort& op, ObSortSpec& spec, const bool in_root_job);

  int generate_spec(ObLogCount& op, ObCountSpec& spec, const bool in_root_job);

  int generate_spec(ObLogValues& op, ObValuesSpec& spec, const bool in_root_job);

  int generate_spec(ObLogSubPlanFilter& op, ObSubPlanFilterSpec& spec, const bool in_root_job);

  int generate_spec(ObLogSubPlanScan& op, ObSubPlanScanSpec& spec, const bool in_root_job);

  int generate_spec(ObLogTableScan& op, ObTableScanSpec& spec, const bool in_root_job);
  int generate_spec(ObLogTableScan& op, ObFakeCTETableSpec& spec, const bool in_root_job);

  int generate_spec(ObLogTempTableAccess& op, ObTempTableAccessOpSpec& spec, const bool in_root_job);
  int generate_spec(ObLogTempTableInsert& op, ObTempTableInsertOpSpec& spec, const bool in_root_job);
  int generate_spec(ObLogTempTableTransformation& op, ObTempTableTransformationOpSpec& spec, const bool in_root_job);

  int generate_spec(ObLogGroupBy& op, ObScalarAggregateSpec& spec, const bool in_root_job);
  int generate_spec(ObLogGroupBy& op, ObMergeGroupBySpec& spec, const bool in_root_job);
  int generate_spec(ObLogGroupBy& op, ObHashGroupBySpec& spec, const bool in_root_job);

  // generate normal table scan
  int generate_normal_tsc(ObLogTableScan& op, ObTableScanSpec& spec);
  int generate_real_virtual_table(ObLogTableScan& op, ObTableScanSpec& spec);
  int convert_table_param(ObLogTableScan& op, ObTableScanSpec& spec, const uint64_t table_id, const uint64_t index_id);
  int generate_tsc_filter(const ObLogTableScan& op, ObTableScanSpec& spec);

  int need_prior_exprs(
      common::ObIArray<ObExpr*>& self_output, common::ObIArray<ObExpr*>& left_output, bool& need_prior);
  int generate_param_spec(ObLogJoin& op, const common::ObIArray<std::pair<int64_t, ObRawExpr*>>& param_raw_exprs,
      ObFixedArray<ObDynamicParamSetter, ObIAllocator>& param_setter);
  int generate_pseudo_column_expr(ObLogJoin& op, ObNLConnectBySpecBase& spec);
  int generate_pump_exprs(ObLogJoin& op, ObNLConnectBySpecBase& spec);
  int get_connect_by_copy_expr(ObRawExpr& left_expr, ObRawExpr*& right_expr, common::ObIArray<ObRawExpr*>& right_exprs);
  int generate_spec(ObLogJoin& op, ObNLConnectByWithIndexSpec& spec, const bool in_root_job);
  int generate_spec(ObLogJoin& op, ObNLConnectBySpec& spec, const bool in_root_job);

  int generate_cte_table_spec(ObLogTableScan& op, ObFakeCTETableSpec& spec);

  int generate_spec(ObLogJoin& op, ObHashJoinSpec& spec, const bool in_root_job);
  // generate nested loop join
  int generate_spec(ObLogJoin& op, ObNestedLoopJoinSpec& spec, const bool in_root_job);
  // generate merge join
  int generate_spec(ObLogJoin& op, ObMergeJoinSpec& spec, const bool in_root_job);

  int generate_join_spec(ObLogJoin& op, ObJoinSpec& spec);

  int set_optimization_info(ObLogTableScan& op, ObTableScanSpec& spec);
  int set_partition_range_info(ObLogTableScan& op, ObTableScanSpec& spec);

  int generate_spec(ObLogExprValues& op, ObExprValuesSpec& spec, const bool in_root_job);

  int generate_spec(ObLogMerge& op, ObTableMergeSpec& spec, const bool in_root_job);
  int generate_spec(ObLogMerge& op, ObMultiTableMergeSpec& spec, const bool in_root_job);

  int generate_spec(ObLogInsert& op, ObTableInsertSpec& spec, const bool in_root_job);
  int generate_spec(ObLogInsert& op, ObTableInsertReturningSpec& spec, const bool in_root_job);
  int generate_spec(ObLogInsert& op, ObMultiPartInsertSpec& spec, const bool in_root_job);

  int generate_spec(ObLogicalOperator& op, ObAppendSpec& spec, const bool in_root_job);

  int generate_spec(ObLogicalOperator& op, ObTableRowStoreSpec& spec, const bool in_root_job);

  // for update && update returning.
  int generate_update(ObLogUpdate& op, ObTableUpdateSpec& spec);

  int generate_spec(ObLogUpdate& op, ObTableUpdateSpec& spec, const bool in_root_job);

  int generate_spec(ObLogUpdate& op, ObTableUpdateReturningSpec& spec, const bool in_root_job);

  int generate_spec(ObLogForUpdate& op, ObTableLockSpec& spec, const bool in_root_job);

  int generate_spec(ObLogForUpdate& op, ObMultiPartLockSpec& spec, const bool in_root_job);

  int generate_spec(ObLogInsert& op, ObTableInsertUpSpec& spec, const bool in_root_job);

  int generate_spec(ObLogInsert& op, ObMultiTableInsertUpSpec& spec, const bool in_root_job);

  int generate_spec(ObLogDelete& op, ObTableDeleteSpec& spec, const bool in_root_job);

  int generate_spec(ObLogDelete& op, ObTableDeleteReturningSpec& spec, const bool in_root_job);

  int generate_spec(ObLogDelete& op, ObMultiPartDeleteSpec& spec, const bool in_root_job);

  int generate_spec(ObLogInsert& op, ObTableReplaceSpec& spec, const bool in_root_job);

  int generate_spec(ObLogInsert& op, ObMultiTableReplaceSpec& spec, const bool in_root_job);

  int generate_spec(ObLogConflictRowFetcher& op, ObTableConflictRowFetcherSpec& spec, const bool in_root_job);

  int generate_spec(ObLogUpdate& op, ObMultiPartUpdateSpec& spec, const bool in_root_job);

  int generate_spec(ObLogTopk& op, ObTopKSpec& spec, const bool in_root_job);

  int generate_spec(ObLogSequence& op, ObSequenceSpec& spec, const bool in_root_job);

  int generate_spec(ObLogMonitoringDump& op, ObMonitoringDumpSpec& spec, const bool in_root_job);

  // px code gen
  int generate_spec(ObLogGranuleIterator& op, ObGranuleIteratorSpec& spec, const bool in_root_job);
  int generate_spec(ObLogExchange& op, ObPxFifoReceiveSpec& spec, const bool in_root_job);
  int generate_spec(ObLogExchange& op, ObPxMSReceiveSpec& spec, const bool in_root_job);
  int generate_spec(ObLogExchange& op, ObPxDistTransmitSpec& spec, const bool in_root_job);
  int generate_spec(ObLogExchange& op, ObPxRepartTransmitSpec& spec, const bool in_root_job);
  int generate_spec(ObLogExchange& op, ObPxReduceTransmitSpec& spec, const bool in_root_job);
  int generate_spec(ObLogExchange& op, ObPxFifoCoordSpec& spec, const bool in_root_job);
  int generate_spec(ObLogExchange& op, ObPxMSCoordSpec& spec, const bool in_root_job);

  // for remote execute
  int generate_spec(ObLogExchange& op, ObDirectTransmitSpec& spec, const bool in_root_job);
  int generate_spec(ObLogExchange& op, ObDirectReceiveSpec& spec, const bool in_root_job);

  int generate_spec(ObLogTableLookup& op, ObTableLookupSpec& spec, const bool in_root_job);

  int generate_spec(ObLogTableScan& op, ObMultiPartTableScanSpec& spec, const bool in_root_job);

  int generate_spec(ObLogWindowFunction& op, ObWindowFunctionSpec& spec, const bool in_root_job);

  int generate_spec(ObLogTableScan& op, ObRowSampleScanSpec& spec, const bool in_root_job);
  int generate_spec(ObLogTableScan& op, ObBlockSampleScanSpec& spec, const bool in_root_job);

  int generate_spec(ObLogTableScan& op, ObTableScanWithIndexBackSpec& spec, const bool in_root_job);
  // pdml code gen
  int generate_spec(ObLogDelete& op, ObPxMultiPartDeleteSpec& spec, const bool in_root_job);
  int generate_spec(ObLogInsert& op, ObPxMultiPartInsertSpec& spec, const bool in_root_job);
  int generate_spec(ObLogUpdate& op, ObPxMultiPartUpdateSpec& spec, const bool in_root_job);

private:
  int convert_global_index_merge_info(ObLogMerge& op, const TableColumns& table_columns,
      common::ObIArray<ObOpSpec*>& subplan_roots, ObTableDMLInfo& table_dml_info);
  int generate_merge_subplan_access_exprs(const bool has_update_clause, const ObAssignments& assigns,
      const ObIArray<ObColumnRefRawExpr*>& index_exprs, common::ObIArray<ObExpr*>& delete_access_exprs,
      common::ObIArray<ObExpr*>& update_insert_access_exprs);
  int generate_subplan_calc_exprs(const common::ObIArray<ObColumnRefRawExpr*>* table_columns,
      const IndexDMLInfo& index_dml_info, ObGlobalIndexDMLInfo& phy_dml_info);
  int convert_multi_table_insert_up_info(ObLogInsert& op, ObMultiTableInsertUpSpec& phy_op);
  int convert_duplicate_key_scan_info(ObLogicalOperator* log_scan_op,
      const common::ObIArray<ObRawExpr*>& table_column_exprs, ObRawExpr* calc_part_expr,
      ObUniqueIndexScanInfo& scan_info);
  int convert_duplicate_key_checker(ObLogDupKeyChecker& log_dupkey_checker, ObDuplicatedKeyChecker& phy_dupkey_checker);
  int convert_multi_table_replace_info(ObLogInsert& op, ObMultiTableReplaceSpec& phy_op);
  int convert_global_index_update_info(ObLogUpdate& op, const TableColumns& table_columns,
      common::ObIArray<ObOpSpec*>& subplan_roots, ObTableDMLInfo& table_dml_info);
  int convert_update_subplan(ObLogDelUpd& op, const IndexDMLInfo& index_dml_info, SeDMLSubPlan& dml_subplan);
  int convert_for_update_subplan(
      ObLogForUpdate& op, const uint64_t table_id, ObOpSpec*& subplan_root, ObTableDMLInfo& table_dml_info);
  int generate_basic_transmit_spec(ObLogExchange& op, ObPxTransmitSpec& spec, const bool in_root_job);
  int generate_basic_receive_spec(ObLogExchange& op, ObPxReceiveSpec& spec, const bool in_root_job);
  int calc_equal_cond_opposite(const ObLogJoin& op, const ObRawExpr& raw_expr, bool& is_opposite);
  int fill_sort_info(const ObIArray<OrderItem>& sort_keys, ObSortCollations& collations, ObIArray<ObExpr*>& sort_exprs);
  int fill_sort_funcs(const ObSortCollations& collations, ObSortFuncs& sort_funcs, const ObIArray<ObExpr*>& sort_exprs);
  int add_column_infos(
      ObLogicalOperator& log_op, ObTableModifySpec& phy_op, const common::ObIArray<ObColumnRefRawExpr*>& columns);
  int recursive_get_column_expr(const ObColumnRefRawExpr*& column, const TableItem& table_item);
  int add_column_conv_infos(ObTableModifySpec& phy_op, const common::ObIArray<ObColumnRefRawExpr*>& columns,
      const common::ObIArray<ObRawExpr*>& column_convert_exprs);
  int add_table_column_ids(ObLogicalOperator& op, ObTableModifySpec* phy_op,
      const common::ObIArray<ObColumnRefRawExpr*>& columns_ids, int64_t rowkey_cnt = 0);

  template <typename T>
  int convert_update_assignments(
      const common::ObIArray<ObColumnRefRawExpr*>& all_columns, const ObAssignments& assigns, T& spec);
  // build scan column ids for insert on duplicate update or merge into.
  // they need fetch row from storage if row need to update.
  // same with the build_update_source_row_desc
  template <typename OP_SPEC>
  int build_scan_column_ids(
      ObLogicalOperator& op, const common::ObIArray<ObColumnRefRawExpr*>& all_columns, OP_SPEC& spec);

  int convert_check_constraint(ObLogDelUpd& log_op, ObTableModifySpec& spec);

  int convert_foreign_keys(ObLogDelUpd& log_op, ObTableModifySpec& spec);
  int add_fk_arg_to_phy_op(ObForeignKeyArg& fk_arg, uint64_t name_table_id,
      const common::ObIArray<uint64_t>& name_column_ids, const common::ObIArray<uint64_t>& value_column_ids,
      share::schema::ObSchemaGetterGuard& schema_guard, ObTableModifySpec& spec);
  int convert_table_dml_param(ObLogDelUpd& log_op, ObTableModifySpec& phy_op);
  int fill_table_dml_param(
      share::schema::ObSchemaGetterGuard* guard, const uint64_t table_id, ObTableModifySpec& phy_op);
  int need_foreign_key_handle(const ObForeignKeyArg& fk_arg, const common::ObIArray<uint64_t>& value_column_ids,
      const ObTableModifySpec& spec, bool& need_handle);
  int fill_aggr_infos(ObLogGroupBy& op, ObGroupBySpec& spec, common::ObIArray<ObExpr*>* group_exprs = NULL,
      common::ObIArray<ObExpr*>* rollup_exprs = NULL);
  int fill_aggr_info(ObAggFunRawExpr& raw_expr, ObExpr& expr, ObAggrInfo& aggr_info);
  int extract_non_aggr_expr(ObExpr* input, const ObRawExpr* raw_input, common::ObIArray<ObExpr*>& exist_in_child,
      common::ObIArray<ObExpr*>& not_exist_in_aggr, common::ObIArray<ObExpr*>* not_exist_in_groupby,
      common::ObIArray<ObExpr*>* not_exist_in_rollup, common::ObIArray<ObExpr*>& output) const;

  int convert_insert_index_info(ObLogInsert& op, ObMultiPartInsertSpec& spec);
  int convert_global_index_delete_info(ObLogDelUpd& op, const TableColumns& table_columns,
      common::ObIArray<ObOpSpec*>& subplan_roots, ObTableDMLInfo& table_dml_info);
  int convert_insert_subplan(ObLogDelUpd& op, const IndexDMLInfo& index_dml_info, SeDMLSubPlan& dml_subplan);
  int convert_delete_subplan(ObLogDelUpd& op, const IndexDMLInfo& index_dml_info, SeDMLSubPlan& dml_subplan);
  int convert_common_dml_subplan(ObLogDelUpd& op, ObPhyOperatorType phy_op_type, ObIArray<ObExpr*>& access_columns,
      const IndexDMLInfo& index_dml_info, SeDMLSubPlan& dml_subplan);
  int generate_exprs_replace_spk(const ObIArray<ObColumnRefRawExpr*>& index_exprs, ObIArray<ObExpr*>& access_exprs);

  int generate_insert_subplan_access_exprs(const ObIArray<ObColumnRefRawExpr*>& index_exprs,
      const ObIArray<ObColumnRefRawExpr*>& base_table_columns, const ObIArray<ObRawExpr*>& conv_columns,
      ObIArray<ObExpr*>& access_exprs);
  int generate_pdml_insert_exprs(const ObIArray<ObColumnRefRawExpr*>& index_exprs,
      const ObIArray<ObRawExpr*>& index_dml_conv_columns, ObIArray<ObExpr*>& pdml_insert_exprs);
  int generate_pdml_update_exprs(
      const ObIArray<ObColumnRefRawExpr*>& index_exprs, const ObAssignments& assigns, ObPxMultiPartUpdateSpec& spec);
  int convert_index_values(uint64_t table_id, ObIArray<ObExpr*>& output_exprs, ObOpSpec*& trs_spec);

  int generate_delete_spec_common(ObLogDelete& op, ObTableDeleteSpec& spec, const bool in_root_job);

  int fill_wf_info(ObIArray<ObExpr*>& all_expr, ObWinFunRawExpr& win_expr, WinFuncInfo& wf_info);
  int fil_sort_info(const ObIArray<OrderItem>& sort_keys, ObIArray<ObExpr*>& all_exprs, ObIArray<ObExpr*>* sort_exprs,
      ObSortCollations& sort_collations, ObSortFuncs& sort_cmp_funcs, const ObItemType aggr_type = T_INVALID);
  int get_pdml_partition_id_column_idx(const ObIArray<ObExpr*>& dml_exprs, int64_t& idx);

  int do_gi_partition_pruning(ObLogJoin& op, ObBasicNestedLoopJoinSpec& spec);

  int generate_hash_func_exprs(const common::ObIArray<ObExchangeInfo::HashExpr>& hash_dist_exprs,
      ExprFixedArray& dist_exprs, common::ObHashFuncs& dist_hash_funcs);

private:
  // all exprs of current operator
  ObSEArray<ObRawExpr*, 8> cur_op_exprs_;
  // all self_produced exprs of current operator
  ObSEArray<ObRawExpr*, 8> cur_op_self_produced_exprs_;
  common::ObSEArray<uint64_t, 10> fake_cte_tables_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SRC_OB_STATIC_ENGINE_CG_H_
