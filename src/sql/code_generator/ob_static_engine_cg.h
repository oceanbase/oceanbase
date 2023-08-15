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

#include "sql/optimizer/ob_logical_operator.h"
#include "sql/code_generator/ob_dml_cg_service.h"
#include "sql/code_generator/ob_tsc_cg_service.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/sort/ob_sort_basic_info.h"

namespace oceanbase
{
namespace sql
{
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
class ObTableLockSpec;
class ObTableInsertUpSpec;
class ObMultiTableInsertUpSpec;
class ObTableModifySpec;
class ObValuesSpec;
class ObLogTableScan;
class ObTableScanSpec;
class ObLogUnpivot;
class ObUnpivotSpec;
class ObFakeCTETableSpec;
class ObHashJoinSpec;
class ObNestedLoopJoinSpec;
class ObBasicNestedLoopJoinSpec;
class ObMergeJoinSpec;
class ObJoinSpec;
class ObMonitoringDumpSpec;
class ObLogSequence;
class ObSequenceSpec;
class ObJoinFilterSpec;
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
class ObPxCoordSpec;
class ObPxFifoCoordSpec;
class ObPxOrderedCoordSpec;
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
struct ObAggrInfo;
class ObWindowFunctionSpec;
class WinFuncInfo;
template <int TYPE>
    struct GenSpecHelper;
class ObTableRowStoreSpec;
//class ObMultiTableReplaceSpec;
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
class ObErrLogSpec;
class ObSelectIntoSpec;
class ObFunctionTableSpec;
class ObLinkDmlSpec;
class ObInsertAllTableInfo;
class ObTableInsertAllSpec;
class ObConflictCheckerCtdef;
struct ObRowkeyCstCtdef;
class ObUniqueIndexScanInfo;
class ObDuplicatedKeyChecker;
struct ObTableScanCtDef;
struct ObDASScanCtDef;
struct InsertAllTableInfo;
typedef common::ObList<uint64_t, common::ObIAllocator> DASTableIdList;
typedef common::ObSEArray<common::ObSEArray<int64_t, 8, common::ModulePageAllocator, true>,
                          1, common::ModulePageAllocator, true> RowParamMap;

enum JsonAraayaggCGOffset
{
  CG_JSON_ARRAYAGG_EXPR,
  CG_JSON_ARRAYAGG_FORMAT,
  CG_JSON_ARRAYAGG_ON_NULL,
  CG_JSON_ARRAYAGG_RETURNING,
  CG_JSON_ARRAYAGG_STRICT,
  CG_JSON_ARRAYAGG_MAX_IDX
};

enum JsonObjectaggCGOffset
{
  CG_JSON_OBJECTAGG_KEY,
  CG_JSON_OBJECTAGG_VALUE,
  CG_JSON_OBJECTAGG_FORMAT,
  CG_JSON_OBJECTAGG_ON_NULL,
  CG_JSON_OBJECTAGG_RETURNING,
  CG_JSON_OBJECTAGG_STRICT,
  CG_JSON_OBJECTAGG_UNIQUE_KEYS,
  CG_JSON_OBJECTAGG_MAX_IDX
};

//
// code generator for static typing engine.
//
class ObStaticEngineCG
{
  friend class ObDmlCgService;
  friend class ObTscCgService;
public:
  template <int TYPE>
  friend struct GenSpecHelper;

  ObStaticEngineCG(const uint64_t cur_cluster_version)
    : phy_plan_(NULL),
      opt_ctx_(nullptr),
      dml_cg_service_(*this),
      tsc_cg_service_(*this),
      cur_cluster_version_(cur_cluster_version)
  {
  }
  // generate physical plan
  int generate(const ObLogPlan &log_plan, ObPhysicalPlan &phy_plan);

  // !!! 注意: 下面两个接口仅用于初始化operator中各种表达式, 其他地方慎用， 如需使用
  // 请先理解这两个接口实际语义, 或者联系@升乐
  //
  // 接口语义：从raw expr中获取rt_expr，并将raw expr push到cur_op_exprs_中
  int generate_rt_expr(const ObRawExpr &raw_expr, ObExpr *&rt_expr);
  int generate_rt_exprs(const common::ObIArray<ObRawExpr *> &src, common::ObIArray<ObExpr *> &dst);
  // Mark support true if any operator (in the plan tree) supports vectorization
  static int check_vectorize_supported(bool &support, bool &stop_checking,
                                       double &scan_cardinality,
                                       ObLogicalOperator *op,
                                       bool is_root_job = true);
  inline static void exprs_not_support_vectorize(const ObIArray<ObRawExpr *> &exprs,
                                         bool &found);

  // detect physical operator type from logic operator.
  static int get_phy_op_type(ObLogicalOperator &op, ObPhyOperatorType &type,
                             const bool in_root_job);
  //set is json constraint type is strict or relax
  const static uint8_t IS_JSON_CONSTRAINT_RELAX = 1;
  const static uint8_t IS_JSON_CONSTRAINT_STRICT = 4;
private:
#ifdef OB_BUILD_TDE_SECURITY
  int init_encrypt_metas(
    const share::schema::ObTableSchema *table_schema,
    share::schema::ObSchemaGetterGuard *guard,
    ObIArray<transaction::ObEncryptMetaCache> &meta_array);

  int init_encrypt_table_meta(const share::schema::ObTableSchema *table_schema,
      share::schema::ObSchemaGetterGuard *guard,
      ObIArray<transaction::ObEncryptMetaCache>&meta_array);
#endif

  int classify_anti_monotone_filter_exprs(const common::ObIArray<ObRawExpr*> &input_filters,
                                          common::ObIArray<ObRawExpr*> &non_anti_monotone_filters,
                                          common::ObIArray<ObRawExpr*> &anti_monotone_filters);

  int set_other_properties(const ObLogPlan &log_plan, ObPhysicalPlan &phy_plan);

  // Post order visit logic plan and generate operator specification.
  // %in_root_job indicate that the operator is executed in main execution thread,
  // not scheduled by distributed/remote execution or PX execution.
  int postorder_generate_op(ObLogicalOperator &op,
                            ObOpSpec *&spec,
                            const bool in_root_job,
                            const bool is_subplan,
                            bool &check_eval_once,
                            const bool need_check_output_datum);
  int clear_all_exprs_specific_flag(const ObIArray<ObRawExpr *> &exprs, ObExprInfoFlag flag);
  int mark_expr_self_produced(ObRawExpr *expr);
  int mark_expr_self_produced(const ObIArray<ObRawExpr *> &exprs);
  int mark_expr_self_produced(const ObIArray<ObColumnRefRawExpr *> &exprs);
  int set_specific_flag_to_exprs(const ObIArray<ObRawExpr *> &exprs, ObExprInfoFlag flag);
  int check_expr_columnlized(const ObRawExpr *expr);
  int check_exprs_columnlized(ObLogicalOperator &op);

  // generate basic attributes (attributes of ObOpSpec class),
  int generate_spec_basic(ObLogicalOperator &op,
                          ObOpSpec &spec,
                          const bool check_eval_once,
                          const bool need_check_output_datum);

  // Invoked after generate_spec() and generate_spec_basic(),
  // some operator need this phase to do some special generation.
  int generate_spec_final(ObLogicalOperator &op, ObOpSpec &spec);

  int generate_calc_exprs(const common::ObIArray<ObRawExpr *> &dep_exprs,
                          const common::ObIArray<ObRawExpr *> &cur_exprs,
                          common::ObIArray<ObExpr *> &calc_exprs,
                          const log_op_def::ObLogOpType log_type,
                          bool check_eval_once,
                          bool need_flatten_gen_col = true);

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

  int generate_spec(ObLogLimit &op, ObLimitSpec &spec, const bool in_root_job);

  int generate_spec(ObLogDistinct &op, ObMergeDistinctSpec &spec, const bool in_root_job);
  int generate_spec(ObLogDistinct &op, ObHashDistinctSpec &spec, const bool in_root_job);

  int generate_spec(ObLogSet &op, ObHashUnionSpec &spec, const bool in_root_job);
  int generate_spec(ObLogSet &op, ObHashIntersectSpec &spec, const bool in_root_job);
  int generate_spec(ObLogSet &op, ObHashExceptSpec &spec, const bool in_root_job);
  int generate_hash_set_spec(ObLogSet &op, ObHashSetSpec &spec);

  int generate_spec(ObLogSet &op, ObMergeUnionSpec &spec, const bool in_root_job);
  int generate_spec(ObLogSet &op, ObMergeIntersectSpec &spec, const bool in_root_job);
  int generate_spec(ObLogSet &op, ObMergeExceptSpec &spec, const bool in_root_job);
  int generate_cte_pseudo_column_row_desc(ObLogSet &op, ObRecursiveUnionAllSpec &phy_set_op);
  int generate_spec(ObLogSet &op, ObRecursiveUnionAllSpec &spec, const bool in_root_job);
  int generate_merge_set_spec(ObLogSet &op, ObMergeSetSpec &spec);
  int generate_recursive_union_all_spec(ObLogSet &op, ObRecursiveUnionAllSpec &spec);

  int generate_spec(ObLogMaterial &op, ObMaterialSpec &spec, const bool in_root_job);

  int generate_spec(ObLogSort &op, ObSortSpec &spec, const bool in_root_job);

  int generate_spec(ObLogCount &op, ObCountSpec &spec, const bool in_root_job);

  int generate_spec(ObLogValues &op, ObValuesSpec &spec, const bool in_root_job);

  int generate_spec(ObLogSubPlanFilter &op, ObSubPlanFilterSpec &spec, const bool in_root_job);

  int generate_spec(ObLogSubPlanScan &op, ObSubPlanScanSpec &spec, const bool in_root_job);

  int generate_spec(ObLogErrLog &op, ObErrLogSpec &spec, const bool in_root_job);

  int generate_spec(ObLogTableScan &op, ObTableScanSpec &spec, const bool in_root_job);
  int generate_spec(ObLogTableScan &op, ObFakeCTETableSpec &spec, const bool in_root_job);

  int generate_spec(ObLogUnpivot &op, ObUnpivotSpec &spec, const bool in_root_job);

  int generate_spec(ObLogTempTableAccess &op, ObTempTableAccessOpSpec &spec, const bool in_root_job);
  int generate_spec(ObLogTempTableInsert &op, ObTempTableInsertOpSpec &spec, const bool in_root_job);
  int generate_spec(ObLogTempTableTransformation &op, ObTempTableTransformationOpSpec &spec, const bool in_root_job);

  int set_3stage_info(ObLogGroupBy &op, ObGroupBySpec &spec);
  int set_rollup_adaptive_info(ObLogGroupBy &op, ObMergeGroupBySpec &spec);
  int generate_spec(ObLogGroupBy &op, ObScalarAggregateSpec &spec, const bool in_root_job);
  int generate_spec(ObLogGroupBy &op, ObMergeGroupBySpec &spec, const bool in_root_job);
  int generate_spec(ObLogGroupBy &op, ObHashGroupBySpec &spec, const bool in_root_job);
  int generate_dist_aggr_distinct_columns(ObLogGroupBy &op, ObHashGroupBySpec &spec);
  int generate_dist_aggr_group(ObLogGroupBy &op, ObGroupBySpec &spec);

  // generate normal table scan
  int generate_normal_tsc(ObLogTableScan &op, ObTableScanSpec &spec);
  int need_prior_exprs(common::ObIArray<ObExpr*> &self_output,
      common::ObIArray<ObExpr*> &left_output,
      bool &need_prior);
  int generate_param_spec(const common::ObIArray<ObExecParamRawExpr *> &param_raw_exprs,
      ObFixedArray<ObDynamicParamSetter, ObIAllocator> &param_setter);
  int generate_pseudo_column_expr(ObLogJoin &op, ObNLConnectBySpecBase &spec);
  int generate_pump_exprs(ObLogJoin &op, ObNLConnectBySpecBase &spec);
  int get_connect_by_copy_expr(ObRawExpr &left_expr,
                               ObRawExpr *&right_expr,
                               common::ObIArray<ObRawExpr *> &right_exprs);
  int construct_hash_elements_for_connect_by(ObLogJoin &op, ObNLConnectBySpec &spec);
  int generate_spec(ObLogJoin &op, ObNLConnectByWithIndexSpec &spec, const bool in_root_job);
  int generate_spec(ObLogJoin &op, ObNLConnectBySpec &spec, const bool in_root_job);

  int generate_cte_table_spec(ObLogTableScan &op, ObFakeCTETableSpec &spec);

  int generate_spec(ObLogJoin &op, ObHashJoinSpec &spec, const bool in_root_job);
  // generate nested loop join
  int generate_spec(ObLogJoin &op, ObNestedLoopJoinSpec &spec, const bool in_root_job);
  // generate merge join
  int generate_spec(ObLogJoin &op, ObMergeJoinSpec &spec, const bool in_root_job);

  int generate_join_spec(ObLogJoin &op, ObJoinSpec &spec);

  int set_optimization_info(ObLogTableScan &op, ObTableScanSpec &spec);
  int set_partition_range_info(ObLogTableScan &op, ObTableScanSpec &spec);

  int generate_spec(ObLogExprValues &op, ObExprValuesSpec &spec, const bool in_root_job);

  int generate_merge_with_das(ObLogMerge &op, ObTableMergeSpec &spec, const bool in_root_job);

  int generate_spec(ObLogMerge &op, ObTableMergeSpec &spec, const bool in_root_job);

  int generate_spec(ObLogInsert &op, ObTableInsertSpec &spec, const bool in_root_job);

  int generate_spec(ObLogicalOperator &op, ObTableRowStoreSpec &spec, const bool in_root_job);

  // for update && update returning.
  int generate_update_with_das(ObLogUpdate &op, ObTableUpdateSpec &spec);

  int generate_spec(ObLogUpdate &op, ObTableUpdateSpec &spec, const bool in_root_job);

  int generate_spec(ObLogForUpdate &op, ObTableLockSpec &spec, const bool in_root_job);

  int generate_spec(ObLogInsert &op, ObTableInsertUpSpec &spec, const bool in_root_job);

  int generate_spec(ObLogDelete &op, ObTableDeleteSpec &spec, const bool in_root_job);

  int generate_spec(ObLogInsert &op, ObTableReplaceSpec &spec, const bool in_root_job);

  int generate_spec(ObLogTopk &op, ObTopKSpec &spec, const bool in_root_job);

  int generate_spec(ObLogSequence &op, ObSequenceSpec &spec, const bool in_root_job);

  int generate_spec(ObLogMonitoringDump &op, ObMonitoringDumpSpec &spec, const bool in_root_job);

  int generate_spec(ObLogJoinFilter &op, ObJoinFilterSpec &spec, const bool in_root_job);

  // px code gen
  int generate_spec(ObLogStatCollector &op, ObStatCollectorSpec &spec, const bool in_root_job);
  int generate_spec(ObLogGranuleIterator &op, ObGranuleIteratorSpec &spec, const bool in_root_job);
  int generate_dml_tsc_ids(const ObOpSpec &spec, const ObLogicalOperator &op,
                           ObIArray<int64_t> &dml_tsc_op_ids, ObIArray<int64_t> &dml_tsc_ref_ids);
  int generate_spec(ObLogExchange &op, ObPxFifoReceiveSpec &spec, const bool in_root_job);
  int generate_spec(ObLogExchange &op, ObPxMSReceiveSpec &spec, const bool in_root_job);
  int generate_spec(ObLogExchange &op, ObPxDistTransmitSpec &spec, const bool in_root_job);
  int generate_spec(ObLogExchange &op, ObPxRepartTransmitSpec &spec, const bool in_root_job);
  int generate_spec(ObLogExchange &op, ObPxReduceTransmitSpec &spec, const bool in_root_job);
  int generate_spec(ObLogExchange &op, ObPxFifoCoordSpec &spec, const bool in_root_job);
  int generate_spec(ObLogExchange &op, ObPxOrderedCoordSpec &spec, const bool in_root_job);
  int generate_spec(ObLogExchange &op, ObPxMSCoordSpec &spec, const bool in_root_job);
  int check_rollup_distributor(ObPxTransmitSpec *spec);

  // for remote execute
  int generate_spec(ObLogExchange &op, ObDirectTransmitSpec &spec, const bool in_root_job);
  int generate_spec(ObLogExchange &op, ObDirectReceiveSpec &spec, const bool in_root_job);

  int generate_spec(ObLogWindowFunction &op, ObWindowFunctionSpec &spec, const bool in_root_job);

  int generate_spec(ObLogTableScan &op, ObRowSampleScanSpec &spec, const bool in_root_job);
  int generate_spec(ObLogTableScan &op, ObBlockSampleScanSpec &spec, const bool in_root_job);

  int generate_spec(ObLogTableScan &op, ObTableScanWithIndexBackSpec &spec,
                                    const bool in_root_job);
  // pdml code gen
  int generate_spec(ObLogDelete &op, ObPxMultiPartDeleteSpec &spec, const bool in_root_job);
  int generate_spec(ObLogInsert &op, ObPxMultiPartInsertSpec &spec, const bool in_root_job);
  int generate_spec(ObLogUpdate &op, ObPxMultiPartUpdateSpec &spec, const bool in_root_job);
  int generate_spec(ObLogInsert &op, ObPxMultiPartSSTableInsertSpec &spec, const bool in_root_job);
  int generate_spec(ObLogSelectInto &op, ObSelectIntoSpec &spec, const bool in_root_job);
  int generate_spec(ObLogFunctionTable &op, ObFunctionTableSpec &spec, const bool in_root_job);
  int generate_spec(ObLogLinkScan &op, ObLinkScanSpec &spec, const bool in_root_job);
  int generate_spec(ObLogLinkDml &op, ObLinkDmlSpec &spec, const bool in_root_job);
  int generate_spec(ObLogInsertAll &op, ObTableInsertAllSpec &spec, const bool in_root_job);
  int generate_spec(ObLogJsonTable &op, ObJsonTableSpec &spec, const bool in_root_job);

  // online optimizer stats gathering
  int generate_spec(ObLogOptimizerStatsGathering &op, ObOptimizerStatsGatheringSpec &spec, const bool in_root_job);
private:
  int add_update_set(ObSubPlanFilterSpec &spec);
  int generate_basic_transmit_spec(
      ObLogExchange &op, ObPxTransmitSpec &spec, const bool in_root_job);
  int generate_basic_receive_spec(
      ObLogExchange &op, ObPxReceiveSpec &spec, const bool in_root_job);
  int init_recieve_dynamic_exprs(const ObIArray<ObExpr *> &child_outputs,
                                 ObPxReceiveSpec &spec);
  int calc_equal_cond_opposite(const ObLogJoin &op,
                               const ObRawExpr &raw_expr,
                               bool &is_opposite);
  int fill_sort_info(
    const ObIArray<OrderItem> &sort_keys,
    ObSortCollations &collations,
    ObIArray<ObExpr*> &sort_exprs);
  int fill_sort_funcs(
    const ObSortCollations &collations,
    ObSortFuncs &sort_funcs,
    const ObIArray<ObExpr*> &sort_exprs);
  int recursive_get_column_expr(const ObColumnRefRawExpr *&column, const TableItem &table_item);
  int fill_aggr_infos(ObLogGroupBy &op,
                      ObGroupBySpec &spec,
                      common::ObIArray<ObExpr *> *group_exprs = NULL,
                      common::ObIArray<ObExpr *> *rollup_exprs = NULL,
                      common::ObIArray<ObExpr *> *distinct_exprs = NULL);
  int fill_aggr_info(ObAggFunRawExpr &raw_expr, ObExpr &expr, ObAggrInfo &aggr_info,
                    common::ObIArray<ObExpr *> *group_exprs/*NULL*/,
                    common::ObIArray<ObExpr *> *rollup_exprs/*NULL*/);
  int extract_non_aggr_expr(ObExpr *input,
                            const ObRawExpr *raw_input,
                            common::ObIArray<ObExpr *> &exist_in_child,
                            common::ObIArray<ObExpr *> &not_exist_in_aggr,
                            common::ObIArray<ObExpr *> *not_exist_in_groupby,
                            common::ObIArray<ObExpr *> *not_exist_in_rollup,
                            common::ObIArray<ObExpr *> *not_exist_in_distinct,
                            common::ObIArray<ObExpr *> &output) const;
  int generate_insert_with_das(ObLogInsert &op, ObTableInsertSpec &spec);

  int generate_exprs_replace_spk(const ObIArray<ObColumnRefRawExpr*> &index_exprs,
                                 ObIArray<ObExpr *> &access_exprs);
  int convert_index_values(uint64_t table_id,
                           ObIArray<ObExpr*> &output_exprs,
                           ObOpSpec *&trs_spec);
  int generate_popular_values_hash(
      const common::ObHashFunc &hash_func,
      const ObIArray<common::ObObj> &popular_values_expr,
      common::ObFixedArray<uint64_t, common::ObIAllocator> &popular_values_hash);
  int generate_delete_with_das(ObLogDelete &op, ObTableDeleteSpec &spec);

  int fill_wf_info(ObIArray<ObExpr *> &all_expr, ObWinFunRawExpr &win_expr,
                   WinFuncInfo &wf_info, const bool can_push_down);
  int fil_sort_info(const ObIArray<OrderItem> &sort_keys,
                    ObIArray<ObExpr *> &all_exprs,
                    ObIArray<ObExpr *> *sort_exprs,
                    ObSortCollations &sort_collations,
                    ObSortFuncs &sort_cmp_funcs);
  int get_pdml_partition_id_column_idx(const ObIArray<ObExpr *> &dml_exprs,
                                       int64_t &idx);

  int do_gi_partition_pruning(
      ObLogJoin &op,
      ObBasicNestedLoopJoinSpec &spec);

  int generate_hash_func_exprs(
      const common::ObIArray<ObExchangeInfo::HashExpr> &hash_dist_exprs,
      ExprFixedArray &dist_exprs,
      common::ObHashFuncs &dist_hash_funcs);

  int generate_range_dist_spec(ObLogExchange &op,
      ObPxDistTransmitSpec &spec);

  int filter_sort_keys(
      ObLogExchange &op,
      const ObIArray<OrderItem> &old_sort_keys,
      ObIArray<OrderItem> &new_sort_keys);
  int generate_dynamic_sample_spec_if_need(
      ObLogExchange &op,
      ObPxCoordSpec &spec);

  int get_is_distributed(ObLogTempTableAccess &op, bool &is_distributed);

  int generate_top_fre_hist_expr_operator(ObAggFunRawExpr &raw_expr, ObAggrInfo &aggr_info);

  int generate_hybrid_hist_expr_operator(ObAggFunRawExpr &raw_expr, ObAggrInfo &aggr_info);

  int generate_insert_all_with_das(ObLogInsertAll &op, ObTableInsertAllSpec &spec);

  int generate_insert_all_table_info(const ObInsertAllTableInfo &insert_tbl_info,
                                     InsertAllTableInfo *&tbl_info);
  static int find_rownum_expr(bool &support, ObLogicalOperator *op);
  static int find_rownum_expr_recursively(bool &support,
                                           const ObRawExpr *raw_expr);
  inline static int find_rownum_expr(bool &support,
                              const common::ObIArray<ObRawExpr *> &exprs);
  int map_value_param_index(const ObInsertStmt *insert_stmt, RowParamMap &row_params_map);
  int add_output_datum_check_flag(ObOpSpec &spec);
  int generate_calc_part_id_expr(const ObRawExpr &src, const ObDASTableLocMeta *loc_meta, ObExpr *&dst);
  int check_only_one_unique_key(const ObLogPlan &log_plan, const ObTableSchema* table_schema, bool& only_one_unique_key);

  bool is_simple_aggr_expr(const ObItemType &expr_type) { return T_FUN_COUNT == expr_type
                                                                 || T_FUN_SUM == expr_type
                                                                 || T_FUN_MAX == expr_type
                                                                 || T_FUN_MIN == expr_type; }
  uint64_t get_cur_cluster_version() { return cur_cluster_version_; }
  int check_fk_nested_dup_del(const uint64_t table_id,
                              const uint64_t root_table_id,
                              DASTableIdList &parent_tables,
                              bool &is_dup);
  bool has_cycle_reference(DASTableIdList &parent_tables, const uint64_t table_id);

  void set_murmur_hash_func(ObHashFunc &hash_func, const ObExprBasicFuncs *basic_funcs_);

  int set_batch_exec_param(const ObIArray<ObExecParamRawExpr *> &exec_params,
                           const ObFixedArray<ObDynamicParamSetter, ObIAllocator>& setters);

  int check_window_functions_order(const ObIArray<ObWinFunRawExpr *> &winfunc_exprs);
private:
  struct BatchExecParamCache {
    BatchExecParamCache(ObExecParamRawExpr* expr, ObOpSpec* spec, bool is_left)
      : expr_(expr), spec_(spec), is_left_param_(is_left) {}

    BatchExecParamCache()
      : expr_(NULL), spec_(NULL), is_left_param_(true) {}

    BatchExecParamCache(const BatchExecParamCache& other)
    {
      expr_ = other.expr_;
      spec_ = other.spec_;
      is_left_param_ = other.is_left_param_;
    }

    TO_STRING_KV(K_(expr),
                 K_(is_left_param));

    ObExecParamRawExpr* expr_;
    ObOpSpec* spec_;
    bool is_left_param_;
  };

private:
  ObPhysicalPlan *phy_plan_;
  ObOptimizerContext *opt_ctx_;
  // all exprs of current operator
  ObSEArray<ObRawExpr *, 8> cur_op_exprs_;
  // all self_produced exprs of current operator
  ObSEArray<ObRawExpr *, 8> cur_op_self_produced_exprs_;
  //仅供递归cte使用，因为oracle的cte是不允许嵌套的，所以可以采用这种方式
  common::ObSEArray<uint64_t, 10> fake_cte_tables_;
  ObDmlCgService dml_cg_service_;
  ObTscCgService tsc_cg_service_;
  uint64_t cur_cluster_version_;
  common::ObSEArray<BatchExecParamCache, 8> batch_exec_param_caches_;

};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SRC_OB_STATIC_ENGINE_CG_H_
