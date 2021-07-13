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

#ifndef _OB_CODE_GENERATOR_IMPL_H
#define _OB_CODE_GENERATOR_IMPL_H 1

#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_log_sequence.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_for_update.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_log_update.h"
#include "sql/optimizer/ob_log_merge.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_function_table.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_material.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_window_function.h"
#include "sql/optimizer/ob_log_select_into.h"
#include "sql/optimizer/ob_log_topk.h"
#include "sql/optimizer/ob_log_append.h"
#include "sql/optimizer/ob_log_count.h"
#include "sql/optimizer/ob_log_granule_iterator.h"
#include "sql/optimizer/ob_log_table_lookup.h"
#include "sql/optimizer/ob_log_link.h"
#include "sql/optimizer/ob_log_conflict_row_fetcher.h"
#include "sql/optimizer/ob_log_monitoring_dump.h"
#include "sql/optimizer/ob_log_temp_table_access.h"
#include "sql/optimizer/ob_log_temp_table_insert.h"
#include "sql/optimizer/ob_log_temp_table_transformation.h"
#include "lib/hash/ob_hashmap.h"
#include "sql/code_generator/ob_expr_generator.h"
#include "sql/engine/basic/ob_values.h"
#include "sql/engine/dml/ob_table_insert_up.h"
#include "sql/engine/recursive_cte/ob_fake_cte_table.h"
#include "sql/engine/dml/ob_multi_part_insert.h"
#include "sql/engine/dml/ob_multi_part_update.h"
#include "sql/engine/dml/ob_multi_part_delete.h"
#include "sql/engine/dml/ob_multi_table_replace.h"
#include "sql/engine/dml/ob_multi_table_insert_up.h"
#include "ob_column_index_provider.h"
#include "sql/engine/pdml/ob_px_multi_part_modify.h"
#include "sql/optimizer/ob_log_unpivot.h"
#include "sql/engine/basic/ob_limit.h"
#include "sql/optimizer/ob_log_insert_all.h"
#include "sql/engine/dml/ob_table_insert_all.h"

namespace oceanbase {
namespace sql {
class ObFakeCTETable;
class ObSort;
class ObAppend;
class ObTableModify;
class ObTableScanWithIndexBack;
class ObJoin;
class ObTableScan;
class ObTableMerge;
class ObRecursiveUnionAll;
class ExprFunction;
class ObSQLSessionInfo;
class ObConnectBy;
class ObCodeGeneratorImpl {
public:
  explicit ObCodeGeneratorImpl(uint64_t min_cluster_version);
  virtual ~ObCodeGeneratorImpl()
  {}

  virtual int generate(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan);
  static int generate_calculable_exprs(const ObDMLStmt& stmt, ObPhysicalPlan& phy_plan);
  static int generate_calculable_exprs(const common::ObIArray<ObHiddenColumnItem>& calculable_exprs,
      const ObDMLStmt& stmt, ObPhysicalPlan& phy_plan, common::ObDList<ObSqlExpression>& pre_calc_exprs);

protected:
  // types and constants
  class ColumnIndexProviderImpl;
  typedef common::ObSEArray<std::pair<ObPhyOperator*, RowDesc*>, 2> PhyOpsDesc;

protected:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCodeGeneratorImpl);
  // function members
  int set_other_properties(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan);

  template <typename T>
  int create_phy_op_desc(
      ObPhyOperatorType type, T*& phy_op, RowDesc*& out_row_desc, PhyOpsDesc& out_ops, const uint64_t op_id);

  static int generate_expr(
      ObRawExpr& raw_expr, ObPhysicalPlan& phy_plan, const RowDesc& row_desc, ObSqlExpression& expr);
  int generate_expr(ObRawExpr& raw_expr, const RowDesc& row_desc, ObSqlExpression& expr);
  int generate_expr(ObRawExpr& raw_expr, ObIterExprOperator*& iter_expr);
  int generate_expr(
      ObRawExpr& raw_expr, const RowDesc& left_row_desc, const RowDesc& right_row_desc, ObSqlExpression& expr);
  int generate_expr(ObRawExpr& raw_expr, ObSqlExpression& expr, const RowDesc& row_desc, const int32_t* projector,
      int64_t projector_size);
  int generate_sql_expr(
      ObSqlExpressionFactory* factory, ObRawExpr* raw_expr, RowDesc& row_desc, ObSqlExpression*& expr);
  int add_filter(const common::ObIArray<ObRawExpr*>& filter_exprs, const RowDesc& row_desc, ObPhyOperator& phy_op,
      const bool startup = false);
  int add_compute(ObRawExpr& raw_expr, RowDesc& out_row_desc, RowDesc* extra_row_desc, ObPhyOperator& phy_op);
  int add_compute(const common::ObIArray<ObRawExpr*>& output_exprs, RowDesc& out_row_desc, RowDesc* extra_row_desc,
      ObPhyOperator& phy_op);
  template <typename T>
  int add_projector(const common::ObIArray<T>& output_exprs, RowDesc& out_row_desc, ObPhyOperator& phy_op);
  template <typename T>
  int generate_projector(const common::ObIArray<T*>& output_exprs, const RowDesc& out_row_desc, int32_t*& projector,
      int64_t& projector_size);
  int add_sort_column_for_set(ObSort& sort, RowDesc& sort_input_desc, ObPhyOperator& child_op);
  int add_column_infos(
      ObLogicalOperator& log_op, ObTableModify& phy_op, const common::ObIArray<ObColumnRefRawExpr*>& columns);
  int add_column_conv_infos(ObTableModify& phy_op, const common::ObIArray<ObColumnRefRawExpr*>& columns,
      const common::ObIArray<ObRawExpr*>& column_convert_exprs);
  int convert_common_parts(const ObLogicalOperator& op, RowDesc& out_row_desc, ObPhyOperator& phy_op);
  int copy_row_desc_by_projector(
      const RowDesc& input_row_desc, const int32_t* projector, int64_t projector_size, RowDesc& out_row_desc);
  int copy_row_desc(const RowDesc& input_row_desc, RowDesc& out_row_desc);
  template <typename T>
  static int create_expression(ObSqlExpressionFactory* factory, T*& expr);
  int postorder_visit(ObLogicalOperator& op, PhyOpsDesc& out_phy_ops, bool in_root_job);
  int convert(ObLogicalOperator& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops, bool in_root_job);
  int convert_limit(ObLogLimit& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int set_sort_columns_for_fetch(ObLimit* phy_op, RowDesc* input_row_desc, ObIArray<OrderItem>& expected_ordering);
  int convert_sequence(ObLogSequence& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_monitoring_dump(ObLogMonitoringDump& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_group_by(ObLogGroupBy& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_sort(ObLogSort& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_sort_for_set(PhyOpsDesc& child_ops);
  int convert_internal_sort(
      const common::ObIArray<ObRawExpr*>& sort_column, ObSort& sort, RowDesc& sort_row_desc, ObPhyOperator& child_op);
  int convert_table_scan(
      ObLogTableScan& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops, bool is_multi_partition_scan = false);
  int convert_temp_table(ObLogTempTableAccess& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_temp_table_insert(ObLogTempTableInsert& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_temp_table_transformation(
      ObLogTempTableTransformation& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_normal_table_scan(
      ObLogTableScan& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops, bool is_multi_partition_scan = false);
  int convert_table_scan_with_index_back(ObLogTableScan& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_cte_pump(ObLogTableScan& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_subplan_tree(ObLogicalOperator& subplan_root, ObPhyOperator*& phy_root);
  int construct_hash_elements_for_connect_by(ObLogJoin& op, const PhyOpsDesc& child_ops, ObConnectBy& phy_op);
  int convert_join(ObLogJoin& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_exchange(ObLogExchange& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops, bool in_root_job);
  int convert_distinct(ObLogDistinct& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_set(ObLogSet& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_subplan_scan(ObLogSubPlanScan& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_subplan_filter(ObLogSubPlanFilter& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_for_update(ObLogForUpdate& op, const PhyOpsDesc& childs_ops, PhyOpsDesc& out_ops);
  int convert_multi_table_for_update(ObLogForUpdate& op, const PhyOpsDesc& childs_ops, PhyOpsDesc& out_ops);
  int convert_for_update_subplan(ObLogForUpdate& op, const uint64_t table_id, RowDesc& row_desc,
      ObPhyOperator*& subplan_root, ObTableDMLInfo& table_dml_info);
  int convert_delete(ObLogDelete& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int handle_pdml_shadow_pk(const common::ObIArray<ObColumnRefRawExpr*>& index_dml_column_exprs, RowDesc* out_row_desc,
      ObPhyOperator* phy_op);
  int convert_pdml_delete(ObLogDelete& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int get_pdml_partition_id_column_idx(const RowDesc& row_desc, int64_t& idx);
  int convert_update(ObLogUpdate& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_pdml_update(ObLogUpdate& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_insert(ObLogInsert& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_pdml_insert(ObLogInsert& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int generate_pdml_insert_projector(const IndexDMLInfo& dml_index_info, const RowDesc& out_row_desc, ObLogInsert& op,
      int32_t*& projector, int64_t& projector_size);
  int generate_pdml_delete_projector(
      const IndexDMLInfo& dml_index_info, const RowDesc& out_row_desc, int32_t*& projector, int64_t& projector_size);
  int convert_merge(ObLogMerge& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int generate_update_info_for_merge(ObLogMerge& op, ObTableMerge& phy_op);
  int convert_exprs(const common::ObIArray<ObRawExpr*>* raw_exprs, RowDesc* row_desc, ExprFunction& function);
  int convert_exprs(const common::ObIArray<ObRawExpr*>* raw_exprs, RowDesc* left_row_desc, RowDesc* right_row_desc,
      ExprFunction& function);
  /**
   * @brief convert_exprs
   * Handle multiple table insert with multiple conditions, convert condition exprs to ObSqlExpression.
   */
  int convert_exprs(
      const common::ObIArray<ObRawExpr*>* raw_exprs, RowDesc* row_desc, ObMultiTableInsert& phy_op, int64_t idx);
  int generate_rowkey_desc(const ObLogMerge& log_op, const RowDesc& out_row_desc, ObTableMerge& phy_op);
  int convert_replace(ObLogInsert& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_conflict_row_fetcher(ObLogConflictRowFetcher& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_multi_table_replace_info(RowDesc& table_row_desc, ObLogInsert& op, ObMultiTableReplace& phy_op);
  int generate_multi_replace_row_desc(ObLogInsert& op, RowDesc& new_desc);
  int convert_duplicate_key_checker(RowDesc& insert_row_desc, RowDesc& table_row_desc,
      ObLogDupKeyChecker& log_dupkey_checker, ObDuplicatedKeyChecker& phy_dupkey_checker);
  int convert_duplicate_key_scan_info(
      RowDesc& row_desc, ObLogicalOperator* log_scan_op, ObUniqueIndexScanInfo& scan_info);
  int convert_replace_returning(ObLogInsert& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_insert_up(ObLogInsert& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int replace_insert_row_desc(ObLogInsert& op, RowDesc& row_desc);
  int convert_multi_table_insert_up_info(RowDesc& update_row_desc, ObLogInsert& op, ObMultiTableInsertUp& phy_op);
  int convert_insert_up_returning(ObLogInsert& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_select_into(ObLogSelectInto& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_link_scan(ObLogLink& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  template <class T>
  int build_update_source_row_desc(ObLogicalOperator& op, const common::ObIArray<ObColumnRefRawExpr*>& all_columns,
      T* phy_op, RowDesc& source_row_desc);
  int convert_generated_column(
      const common::ObIArray<ObColumnRefRawExpr*>& all_columns, ObPhyOperator& phy_op, RowDesc& row_desc);
  int convert_generated_column(const common::ObIArray<ObColumnRefRawExpr*>& all_columns, ObPhyOperator& phy_op,
      RowDesc& row_desc, InsertTableInfo*& insert_table_info);
  template <class ExprType, class OpType>
  int build_old_projector(const ObIArray<ExprType*>& old_columns, OpType* phy_op, RowDesc& row_desc);
  int generate_assign_expr_for_merge(
      ObLogMerge& op, ObTableMerge& phy_op, const RowDesc& out_row_desc, RowDesc& update_row_desc);

  template <typename T>
  int convert_expr_values(
      T& phy_op, RowDesc& out_row_desc, ObLogExprValues& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_expr_values(ObLogExprValues& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_values(ObLogValues& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_material(ObLogMaterial& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_window_function(ObLogWindowFunction& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_append(ObLogAppend& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int add_select_into(const ObLogPlan& log_plan, const PhyOpsDesc& child_ops, PhyOpsDesc& out_phy_ops);
  int create_identity_projector(ObPhyOperator& op, const int64_t projector_size);
  int copy_projector(ObPhyOperator& op, int64_t N, const int32_t* projector, int64_t projector_size);
  int add_table_column_ids(ObLogicalOperator& op, ObTableModify* phy_op,
      const common::ObIArray<ObColumnRefRawExpr*>& columns_ids, int64_t rowkey_cnt = 0);
  template <class UpdateOp>
  int convert_update_assignments(const common::ObIArray<ObColumnRefRawExpr*>& all_columns, const ObAssignments& assigns,
      const jit::expr::ObColumnIndexProvider& assign_expr_desc, const int32_t* old_projector,
      int64_t old_projector_size, UpdateOp& up_op);
  int convert_multi_table_update_column_info(
      const IndexDMLInfo& primary_index_info, const RowDesc& update_row_desc, ObAssignColumns& assign_columns);
  int convert_topk(ObLogTopk& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int generate_pump_row_desc(ObLogJoin& log_join, ObJoin* join, const std::pair<ObPhyOperator*, RowDesc*>& left_child,
      const std::pair<ObPhyOperator*, RowDesc*>& right_child);
  int generate_root_row_desc(ObJoin* join, const std::pair<ObPhyOperator*, RowDesc*>& left_child,
      const std::pair<ObPhyOperator*, RowDesc*>& right_child);
  int generate_pseudo_column_row_desc(ObLogJoin& op, ObJoin& join, RowDesc& out_row_desc);
  int generate_cte_pseudo_column_row_desc(ObLogSet& op, ObRecursiveUnionAll& set, RowDesc& out_row_desc);
  int convert_count(ObLogCount& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int classify_anti_monotone_filter_exprs(const common::ObIArray<ObRawExpr*>& input_filters,
      common::ObIArray<ObRawExpr*>& non_anti_monotone_filters, common::ObIArray<ObRawExpr*>& anti_monotone_filters);
  int add_connect_by_prior_exprs(ObLogJoin& op, const RowDesc& left_row_desc, ObJoin& join);
  int add_connect_by_root_exprs(ObLogJoin& log_join, RowDesc& row_desc, ObJoin& phy_join);
  int set_connect_by_ctx(const ObLogJoin& log_join, ObJoin& phy_join);
  int set_part_func(const ObIArray<ObRawExpr*>& part_func_exprs, const RowDesc* row_desc, ObSqlExpression* part_expr,
      ObSqlExpression* sub_part_expr);
  int convert_light_granule_iterator(ObLogGranuleIterator& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_granule_iterator(ObLogGranuleIterator& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int set_optimization_info(ObLogTableScan& log_ts, ObTableScan* phy_ts);
  template <typename T>
  int generate_returning_exprs(
      T* phy_op, const common::ObIArray<ObRawExpr*>* returning_exprs, RowDesc* returning_row_desc);
  int set_partition_range_info(ObLogTableScan& log_op, ObTableScan* phy_op);
  int convert_insert_index_info(ObLogInsert& op, ObMultiPartInsert& phy_op, const RowDesc& row_desc);
  int convert_insert_index_info(ObLogInsertAll& op, ObMultiTableInsert& phy_op, const RowDesc& row_desc);
  int convert_multi_table_update(ObLogUpdate& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_global_index_update_info(ObLogDelUpd& op, const TableColumns& table_columns,
      const ObAssignments& assignments, const RowDesc& row_desc, common::ObIArray<ObPhyOperator*>& subplan_roots,
      ObTableDMLInfo& table_dml_info);
  int convert_global_index_delete_info(ObLogDelUpd& op, const TableColumns& table_columns, RowDesc& row_desc,
      common::ObIArray<ObPhyOperator*>& subplan_roots, ObTableDMLInfo& table_dml_info);
  int convert_multi_table_delete(ObLogDelete& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int generate_index_location(
      const IndexDMLInfo& index_dml_info, ObLogDelUpd& op, RowDesc& row_desc, ObTableLocation*& index_location);
  int convert_table_lookup(ObLogTableLookup& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int generate_update_index_location(const IndexDMLInfo& index_info, ObLogDelUpd& op, const ObAssignments& assignments,
      const RowDesc& row_desc, ObTableLocation*& old_tbl_loc, ObTableLocation*& new_tbl_loc);
  template <class T>
  int construct_basic_row_desc(const common::ObIArray<T*>& basic_exprs, RowDesc& row_desc);
  int generate_update_new_row_desc(const ObAssignments& assignments, const RowDesc& row_desc, RowDesc& new_row_desc);
  /*
    int generate_insert_new_row_desc(const ObLogInsert &insert_op,
                                     RowDesc &extra_row_desc);
  */
  int generate_insert_new_row_desc(const ObIArray<ObColumnRefRawExpr*>* table_columns,
      const common::ObIArray<ObRawExpr*>& output_exprs, const RowDesc& orig_row_desc, RowDesc& extra_row_desc);
  template <typename T>
  int convert_common_dml_subplan(ObLogDelUpd& op, ObPhyOperatorType phy_op_type,
      const common::ObIArray<T*>& related_exprs, const IndexDMLInfo& index_dml_info, const RowDesc& table_row_desc,
      PhyOpsDesc& out_ops, DMLSubPlan& dml_subplan);
  int convert_insert_subplan(/*const*/ ObLogDelUpd& op,  // no const, see convert_foreign_keys.
      const IndexDMLInfo& index_dml_info, const RowDesc& table_row_desc, DMLSubPlan& dml_subplan);
  int convert_update_subplan(/*const*/ ObLogDelUpd& op,  // no const, see convert_foreign_keys.
      bool is_global_index, const IndexDMLInfo& index_dml_info, const RowDesc& table_row_desc, DMLSubPlan& dml_subplan);
  int convert_delete_subplan(/*const*/ ObLogDelUpd& op,  // no const, see convert_foreign_keys.
      const IndexDMLInfo& index_dml_info, const RowDesc& table_row_desc, DMLSubPlan& dml_subplan);
  template <typename T>
  int convert_index_values(uint64_t table_id, const common::ObIArray<T*>& output_exprs, const RowDesc& table_row_desc,
      PhyOpsDesc& out_ops, DMLSubPlan& dml_subplan);
  int fill_sort_columns(const ColumnIndexProviderImpl& idx_provider, const ObIArray<OrderItem>& sort_keys,
      ObPhyOperator* phy_op, ObSortableTrait& merge_receive);
  int convert_foreign_keys(ObLogDelUpd& log_op, ObTableModify& phy_op);
  int convert_check_constraint(ObLogDelUpd& log_op, ObTableModify& phy_op, RowDesc& out_row_desc);
  /**
   * @brief convert_check_constraint
   * Handle multiple table insert, convert check constraint exprs to ObSqlExpression.
   *
   */
  int convert_check_constraint(ObLogInsertAll& log_op, uint64_t index, ObMultiTableInsert& parent_phy_op,
      ObTableModify& phy_op, RowDesc& out_row_desc, InsertTableInfo*& insert_table_info);
  int add_fk_arg_to_phy_op(ObForeignKeyArg& fk_arg, uint64_t name_table_id,
      const common::ObIArray<uint64_t>& name_column_ids, const common::ObIArray<uint64_t>& value_column_ids,
      share::schema::ObSchemaGetterGuard& schema_guard, ObTableModify& phy_op);
  int need_foreign_key_handle(const ObForeignKeyArg& fk_arg, const common::ObIArray<uint64_t>& value_column_ids,
      const ObTableModify& phy_op, bool& need_handle);
  int convert_table_dml_param(ObLogDelUpd& log_op, ObTableModify& phy_op);
  int fill_table_dml_param(share::schema::ObSchemaGetterGuard* guard, const uint64_t table_id, ObTableModify& phy_op);
  // Get the column id of base table (%col->get_column_id() is column id of expanded view
  // for updatable view)
  int get_column_ref_base_cid(const ObLogicalOperator& op, ObColumnRefRawExpr* col, uint64_t& base_cid);
  int need_fire_update_event(const share::schema::ObTableSchema& table_schema, const common::ObString& update_events,
      const ObLogUpdate& log_update, const ObSQLSessionInfo& session, common::ObIAllocator& allocator, bool& need_fire);

  int convert_global_index_merge_info(ObLogMerge& op, const TableColumns& table_columns, RowDesc& row_desc,
      common::ObIArray<ObPhyOperator*>& subplan_roots, ObTableDMLInfo& table_dml_info);
  int generate_merge_index_location(
      const IndexDMLInfo& index_dml_info, ObLogMerge& op, RowDesc& insert_row_desc, TableLocationArray& new_tbl_loc);

  int generate_part_and_sort_columns(const ColumnIndexProviderImpl& idx_provider,
      const ObIArray<ObRawExpr*>& partition_exprs, const ObIArray<OrderItem>& order_items,
      common::ObIArray<ObColumnInfo>& partition_cols, common::ObIArray<ObSortColumn>& sort_cols);

  int add_window_function_info(const ColumnIndexProviderImpl& idx_provider, ObPhyOperator* phy_op,
      ObWinFunRawExpr* win_expr, const RowDesc& input_row_desc, RowDesc& output_row_desc);

  int generate_sql_expr_for_window_function(
      ObPhyOperator* phy_op, const RowDesc& input_row_desc, ObRawExpr*& raw_expr, ObSqlExpression*& sql_expr);

  int get_cell_idx(
      const ColumnIndexProviderImpl& idx_provider, const ObRawExpr* raw_expr, int64_t& cell_idx, bool& skip);
  int convert_unpivot(ObLogUnpivot& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);

  int convert_real_virtual_table(ObLogTableScan& op, ObTableScan& phy_op);
  int convert_table_param(ObLogTableScan& op, ObTableScan& phy_op, const uint64_t table_id, const uint64_t index_id);

  // detect physical operator type from logic operator.
  int get_phy_op_type(ObLogicalOperator& op, ObPhyOperatorType& type, const bool in_root_job);

  int set_need_check_pk_is_null(ObLogDelUpd& op, ObTableModify* phy_op);
  int recursive_get_column_expr(const ObColumnRefRawExpr*& column, const TableItem& table_item);
  int convert_multi_table_insert(ObLogInsertAll& op, const PhyOpsDesc& child_ops, PhyOpsDesc& out_ops);
  int convert_multi_insert_conditions(ObMultiTableInsert& phy_op, ObLogInsertAll& op, RowDesc* out_row_desc);

protected:
  ObPhysicalPlan* phy_plan_;
  common::ObSEArray<ObFakeCTETable*, 10> phy_cte_tables_;
  uint64_t cur_tbl_op_id_;
  uint64_t min_cluster_version_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_CODE_GENERATOR_IMPL_H */
