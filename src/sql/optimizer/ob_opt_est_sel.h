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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_SEL_
#define OCEANBASE_SQL_OPTIMIZER_OB_OPT_EST_SEL_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_partition_key.h"
#include "common/object/ob_object.h"
#include "common/ob_range.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/optimizer/ob_opt_default_stat.h"
#include "sql/optimizer/ob_optimizer_context.h"

#define GROUP_PUSH_BACK(array0, item0, array1, item1, array2, item2) \
  if (OB_FAIL(array0.push_back(item0))) {                            \
    LOG_WARN("failed to push back item0", K(ret));                   \
  } else if (OB_FAIL(array1.push_back(item1))) {                     \
    LOG_WARN("failed to push back item1", K(ret));                   \
  } else if (OB_FAIL(array2.push_back(item2))) {                     \
    LOG_WARN("failed to push back item2", K(ret));                   \
  }

namespace oceanbase {
namespace share {
namespace schema {
class ObSchemaGetterGuard;
}
}  // namespace share
namespace common {
class ObStatManager;
class ObColumnStatValueHandle;
}  // namespace common
namespace sql {
class ObRawExpr;
class ObColumnRefRawExpr;
class ObDMLStmt;
class ObSQLSessionInfo;
class ObExecContext;
class ObLogicalOperator;
class ObJoinOrder;
struct ColumnItem;
struct RangeExprs;
struct ObExprSelPair;
enum ObEstimateType {
  OB_DEFAULT_STAT_EST,
  OB_CURRENT_STAT_EST,
};

class ObEstColumnStat {
public:
  int assign(const ObEstColumnStat& other);
  int init(const uint64_t column_id, const double rows, const int64_t ndv, const bool is_single_pkey);

  int update_column_static_with_rows(double old_rows, double new_rows);

  uint64_t get_column_id()
  {
    return column_id_;
  }
  void set_column_id(uint64_t column_id)
  {
    column_id_ = column_id;
  }
  double get_ndv()
  {
    return ndv_;
  }
  void set_ndv(double ndv)
  {
    ndv_ = ndv;
  }
  double get_origin_ndv()
  {
    return origin_ndv_;
  }
  void set_origin_ndv(double ndv)
  {
    origin_ndv_ = ndv;
  }

  TO_STRING_KV(K_(column_id), K_(ndv), K_(origin_ndv));

private:
  uint64_t column_id_;
  double ndv_;
  double origin_ndv_;
};

class ObEstTableStat {
public:
  int assign(const ObEstTableStat& other);

  int init(const uint64_t table_id, const ObPartitionKey& pkey, const int32_t rel_id, const double rows,
      common::ObStatManager& stat_manager, ObSqlSchemaGuard& schema_guard, common::ObIArray<int64_t>& all_used_part_id,
      const bool use_default_stat);

  // only base table join order and ObLogTableScan can enlarge
  int update_stat(const double rows, const bool can_reduce, const bool can_enlarge);

  int get_column(const uint64_t cid, ObEstColumnStat*& cstat);

  int32_t& get_rel_id()
  {
    return rel_id_;
  }
  void set_rel_id(const int32_t rel_id)
  {
    rel_id_ = rel_id;
  }

  uint64_t get_table_id()
  {
    return table_id_;
  }
  void set_table_id(const uint64_t& table_id)
  {
    table_id_ = table_id;
  }

  uint64_t get_ref_id()
  {
    return ref_id_;
  }
  void set_ref_id(const uint64_t& ref_id)
  {
    ref_id_ = ref_id;
  }

  int64_t get_part_id()
  {
    return part_id_;
  }
  void set_part_id(const int64_t& part_id)
  {
    part_id_ = part_id;
  }

  double get_rows()
  {
    return rows_;
  }
  void set_rows(double rows)
  {
    rows_ = rows;
  }

  double get_origin_rows()
  {
    return origin_rows_;
  }
  void set_origin_rows(double origin_rows)
  {
    origin_rows_ = origin_rows;
  }

  common::ObIArray<uint64_t>& get_all_used_parts()
  {
    return all_used_parts_;
  }
  common::ObIArray<uint64_t>& get_pkey_ids()
  {
    return pk_ids_;
  }
  common::ObIArray<ObEstColumnStat>& get_all_cstat()
  {
    return all_cstat_;
  }

  int compute_number_distinct(const char* llc_bitmap, int64_t& num_distinct);
  double select_alpha_value(const int64_t num_bucket);

  TO_STRING_KV(
      K_(table_id), K_(ref_id), K_(rel_id), K_(rows), K_(origin_rows), K_(all_used_parts), K_(pk_ids), K_(all_cstat));

private:
  uint64_t table_id_;
  uint64_t ref_id_;
  int64_t part_id_;  // best_partition id
  int32_t rel_id_;
  double rows_;         // table row after reduce/enlarge
  double origin_rows_;  // origin rows before reduce/enlarge
  // all used partition id after partition pruning
  common::ObSEArray<uint64_t, 64, common::ModulePageAllocator, true> all_used_parts_;
  common::ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> pk_ids_;
  common::ObSEArray<ObEstColumnStat, 32, common::ModulePageAllocator, true> all_cstat_;
};

class ObEstAllTableStat {
public:
  int assign(const ObEstAllTableStat& other);

  /**
   * init table column statistic, and table row count.
   */
  int add_table(const uint64_t table_id, const ObPartitionKey& pkey, const int32_t rel_id, const double rows,
      common::ObStatManager* stat_manager, ObSqlSchemaGuard* schema_guard, common::ObIArray<int64_t>& all_used_part_id,
      const bool use_default_stat);

  /**
   * update table colulmn statistic, and table row count.
   */
  int update_stat(const ObRelIds& rel_ids, const double rows, const EqualSets& equal_sets, const bool can_reduce,
      const bool can_enlarge);

  /**
   * merge child statistic.
   *  - join: merge left and right child.
   *  - spf: only merge first child.
   *  - union: no need to merge child statistic, generate new statistic instead.
   */
  int merge(const ObEstAllTableStat& other);

  /**
   * get distinct number of certain column
   */
  int get_distinct(const uint64_t alias_table_id, const uint64_t column_id, double& distinct_num);

  int get_origin_distinct(const uint64_t table_id, const uint64_t column_id, double& distinct_num);

  int get_rows(const uint64_t alias_table_id, double& rows, double& origin_rows);

  int get_part_id_by_table_id(const uint64_t& table_id, int64_t& part_id) const;

  int get_pkey_ids_by_table_id(const uint64_t table_id, ObIArray<uint64_t>& pkey_ids);

  common::ObIArray<ObEstTableStat>& get_all_tstat()
  {
    return all_tstat_;
  }

  int get_tstat_by_table_id(const uint64_t table_id, ObEstTableStat*& tstat);

  int get_cstat_by_table_id(const uint64_t table_id, const uint64_t column_id, ObEstColumnStat*& cstat);

  TO_STRING_KV(K_(all_tstat));

private:
  common::ObSEArray<ObEstTableStat, 8, common::ModulePageAllocator, true> all_tstat_;
};

struct ObEstRange {
  ObEstRange(const common::ObObj* startobj, const common::ObObj* endobj) : startobj_(startobj), endobj_(endobj)
  {}
  const common::ObObj* startobj_;
  const common::ObObj* endobj_;
};

struct ObEstColRangeInfo {
  ObEstColRangeInfo(double min, double max, const common::ObObj* startobj, const common::ObObj* endobj, double distinct,
      bool discrete, common::ObBorderFlag border_flag)
      : min_(min),
        max_(max),
        startobj_(startobj),
        endobj_(endobj),
        distinct_(distinct),
        discrete_(discrete),
        border_flag_(border_flag)
  {}
  double min_;
  double max_;
  const common::ObObj* startobj_;
  const common::ObObj* endobj_;
  double distinct_;
  bool discrete_;
  common::ObBorderFlag border_flag_;
};

class ObEstSelInfo {
public:
  ObEstSelInfo(ObOptimizerContext& ctx, ObDMLStmt* stmt, const ObLogicalOperator* op = NULL)
      : opt_ctx_(ctx), stmt_(stmt), op_(op), use_origin_stat_(false)
  {}

  ObOptimizerContext& get_opt_ctx()
  {
    return opt_ctx_;
  }
  const ObOptimizerContext& get_opt_ctx() const
  {
    return opt_ctx_;
  }
  const ObDMLStmt* get_stmt() const
  {
    return stmt_;
  }
  ObSQLSessionInfo* get_session_info()
  {
    return opt_ctx_.get_session_info();
  }
  const ObSQLSessionInfo* get_session_info() const
  {
    return opt_ctx_.get_session_info();
  }
  ObExecContext* get_exec_ctx()
  {
    return opt_ctx_.get_exec_ctx();
  }
  const ObExecContext* get_exec_ctx() const
  {
    return opt_ctx_.get_exec_ctx();
  }
  share::schema::ObSchemaGetterGuard* get_schema_guard()
  {
    return opt_ctx_.get_schema_guard();
  }
  const share::schema::ObSchemaGetterGuard* get_schema_guard() const
  {
    return opt_ctx_.get_schema_guard();
  }
  ObSqlSchemaGuard* get_sql_schema_guard() const
  {
    return opt_ctx_.get_sql_schema_guard();
  }
  common::ObStatManager* get_stat_manager()
  {
    return opt_ctx_.get_stat_manager();
  }
  const common::ObStatManager* get_stat_manager() const
  {
    return opt_ctx_.get_stat_manager();
  }
  common::ObOptStatManager* get_opt_stat_manager()
  {
    return opt_ctx_.get_opt_stat_manager();
  }
  const common::ObOptStatManager* get_opt_stat_manager() const
  {
    return opt_ctx_.get_opt_stat_manager();
  }
  common::ObIAllocator& get_allocator()
  {
    return opt_ctx_.get_allocator();
  }
  const common::ObIAllocator& get_allocator() const
  {
    return opt_ctx_.get_allocator();
  }
  const ParamStore* get_params() const
  {
    return opt_ctx_.get_params();
  }
  bool use_default_stat() const
  {
    return opt_ctx_.use_default_stat();
  }

  const ObLogicalOperator* get_logical_operator() const
  {
    return op_;
  }
  ObEstAllTableStat& get_table_stats()
  {
    return table_stats_;
  }
  const ObEstAllTableStat& get_table_stats() const
  {
    return table_stats_;
  }

  void reset_table_stats();
  int append_table_stats(const ObEstSelInfo* other);
  int update_table_stats(const ObLogicalOperator* child);
  int update_table_stats(const ObJoinOrder& join_order);

  int add_table_for_table_stat(const uint64_t table_id, const ObPartitionKey& pkey, const double rows,
      common::ObIArray<int64_t>& all_used_part_id);

  int rename_statics(const uint64_t subquery_id, const ObEstSelInfo& child_est_sel_info, const double& child_rows);
  void set_use_origin_stat(const bool use_origin)
  {
    use_origin_stat_ = use_origin;
  }
  bool get_use_origin_stat() const
  {
    return use_origin_stat_;
  }
  TO_STRING_KV(K_(table_stats));

private:
  ObOptimizerContext& opt_ctx_;
  ObDMLStmt* stmt_;
  // in join order, op_ is NULL
  // in logical operator, op_ point to related operator
  const ObLogicalOperator* op_;
  ObEstAllTableStat table_stats_;
  bool use_origin_stat_;
};
class ObOptEstSel {
public:
  static double DEFAULT_COLUMN_DISTINCT_RATIO;

  // calculate selectivity for predicates with conjective relationship
  static int calculate_selectivity(const ObEstSelInfo& est_sel_info, const common::ObIArray<ObRawExpr*>& quals,
      double& selectivity, common::ObIArray<ObExprSelPair>* all_predicate_sel, ObJoinType join_type = UNKNOWN_JOIN,
      const ObRelIds* left_rel_ids = NULL, const ObRelIds* right_rel_ids = NULL, const double left_row_count = -1.0,
      const double right_row_count = -1.0);

  // calculate selectivity for one predicate
  static int clause_selectivity(const ObEstSelInfo& est_sel_info, const ObRawExpr* qual, double& selectivity,
      common::ObIArray<ObExprSelPair>* all_predicate_sel, ObJoinType join_type = UNKNOWN_JOIN,
      const ObRelIds* left_rel_ids = NULL, const ObRelIds* right_rel_ids = NULL, const double left_row_count = -1.0,
      const double right_row_count = -1.0);

  static int get_single_newrange_selectivity(const ObEstSelInfo& est_sel_info,
      const common::ObIArray<ColumnItem>& range_columns, const common::ObNewRange& range, const ObEstimateType est_type,
      const bool is_like_sel, double& selectivity, bool single_value_only = false);

  // calculate distinct number of given exprs
  static int calculate_distinct(const double origin_rows, const ObEstSelInfo& est_sel_info,
      const common::ObIArray<ObRawExpr*>& exprs, double& rows, const bool use_est_origin_rows = false);

  static int filter_distinct_by_equal_set(const ObEstSelInfo& est_sel_info,
      const common::ObIArray<ObRawExpr*>& column_exprs, common::ObIArray<ObRawExpr*>& filtered_exprs);

  static double calc_ndv_by_sel(const double distinct_sel, const double null_sel, const double rows);

  static int get_var_distinct(const ObEstSelInfo& est_sel_info, const ObRawExpr& column_exprs, double& distinct);

  // deduce/enlarge ndv
  static double scale_distinct(double selected_rows, double rows, double ndv);

  static int calc_sel_for_equal_join_cond(const ObEstSelInfo& est_sel_info, const common::ObIArray<ObRawExpr*>& conds,
      double& left_selectivity, double& right_selectivity);

  static inline double revise_between_0_1(double num)
  {
    return num < 0 ? 0 : (num > 1 ? 1 : num);
  }

private:
  struct RangeSel {
    RangeSel() : var_(NULL), has_lt_(false), has_gt_(false), lt_sel_(0), gt_sel_(0)
    {}
    RangeSel(ObRawExpr* var, bool has_lt, bool has_gt, double lt_sel, double gt_sel)
        : var_(var), has_lt_(has_lt), has_gt_(has_gt), lt_sel_(lt_sel), gt_sel_(gt_sel)
    {}
    virtual ~RangeSel()
    {}
    int add_sel(bool lt, bool gt, double lt_sel, double gt_sel)
    {
      int ret = common::OB_SUCCESS;
      if (lt) {
        if (has_lt_) {
          lt_sel_ = lt_sel_ < lt_sel ? lt_sel_ : lt_sel;
        } else {
          has_lt_ = true;
          lt_sel_ = lt_sel;
        }
      }
      if (gt) {
        if (has_gt_) {
          gt_sel_ = gt_sel_ < gt_sel ? gt_sel_ : gt_sel;
        } else {
          has_gt_ = true;
          gt_sel_ = gt_sel;
        }
      }
      return ret;
    }

    TO_STRING_KV(K(var_), K(has_lt_), K(has_gt_), K(lt_sel_), K(gt_sel_));
    ObRawExpr* var_;
    bool has_lt_;
    bool has_gt_;
    double lt_sel_;
    double gt_sel_;
  };

private:
  // use real params to calc selectivity
  // @param in  plan the plan
  // @param in  qual the filter to calc selectivity, should be const or calculable expr
  // @param out selectivity
  // @return err code
  static int get_calculable_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity);

  // param:As some expr, query range can't calc range, then range will be (min, max).
  // no_whole_range representing that expr_sel should not use whole range to calc sel.
  static int get_column_range_sel(const ObEstSelInfo& est_sel_info, const ObColumnRefRawExpr& col_expr,
      const ObRawExpr& qual, const bool is_like_sel, double& selectivity, const bool no_whole_range = false);

  //@param: range_exprs, array of some simple filters with column and table id info
  // get the final ranges of range_exprs, calculate selectivity using the ranges
  static int get_range_exprs_sel(const ObEstSelInfo& est_sel_info, RangeExprs& range_exprs, double& selectivity);

  // 1. var = | <=> const, get_simple_predicate_sel
  // 2. func(var) = | <=> const,
  //       only simple op(+,-,*,/), get_simple_predicate_sel,
  //       mod(cnt_var, mod_num),  distinct_sel * mod_num
  //       else sqrt(distinct_sel)
  // 3. cnt(var) = |<=> cnt(var) get_cntcol_eq_cntcol_sel
  static int get_equal_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity,
      ObJoinType join_type = UNKNOWN_JOIN, const ObRelIds* left_rel_ids = NULL, const ObRelIds* right_rel_ids = NULL);

  static int get_equal_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& left_expr, const ObRawExpr& right_expr,
      const bool null_safe, double& selectivity, ObJoinType join_type = UNKNOWN_JOIN,
      const ObRelIds* left_rel_ids = NULL, const ObRelIds* right_rel_ids = NULL);

  // 1. var1(t1) = var1(t1), sel 1.0 - null_sel
  //   var1(t1) <=> var1(t1), sel 1.0
  //   cnt_var1(t1) = | <=> cnt_var2(t1), sel 0.005(DEFAULT_EQ_SEL)
  // 2. join condition
  //   var1(t1) = var2(t2), sel min(distinct_sel)
  //   var1(t1) <=> var2(t2), sel min(distinct_sel) + min(null_sel)
  //   others sel min(distinct_sel)
  static int get_cntcol_eq_cntcol_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& left_expr,
      const ObRawExpr& right_expr, bool null_safe, double& selectivity, ObJoinType join_type = UNKNOWN_JOIN,
      const ObRelIds* left_rel_ids = NULL, const ObRelIds* right_rel_ids = NULL);

  // c1 like 'xx%' -> use query range selectivity
  // c1 like '%xx' -> use DEFAULT_INEQ_SEL 1.0 / 3.0
  //'x' like c1   -> use query range selectivity
  static int get_like_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity);

  // c1 between $val1 and $val2     -> equal with [$val2 - $val1] range sel
  // c1 not between $val1 and $val2 -> equal with (min, $val1) or ($val2, max) range sel
  static int get_btw_or_not_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity);

  // @brief used for calculate column expr t1.c1 selectity
  // selectity = 1.0 - sel(t1.c1 = 0) - sel(t1.c1 is NULL)
  static int get_column_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity);

  // calculate selectivity for var=const or var is null, var can be column or func(column)
  // @param var
  // @return
  // distinct_sel:
  //   for column in current level query_block:(table_rows - null_num) / table_rows / distinct
  //   for others: EST_DEF_VAR_EQ_SEL
  // null_sel:
  //   for column in current level query_block:null_num/table_rows
  //   for others: EST_DEF_COL_NULL_RATIO
  static int get_var_basic_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& var, double* distinct_sel_ptr,
      double* null_sel_ptr, double* row_count_ptr = NULL, common::ObObj* min_obj_ptr = NULL,
      common::ObObj* max_obj_ptr = NULL, double* origin_row_count_ptr = NULL);

  static int get_var_basic_from_est_sel_info(const ObEstSelInfo& est_sel_info, const ObColumnRefRawExpr& col_var,
      double& distinct_num, double& row_count, double& origin_row_count);

  static int get_var_basic_from_statics(const ObEstSelInfo& est_sel_info, const ObColumnRefRawExpr& col_var,
      double& distinct_num, double& null_num, double& row_count, double& origin_row_count, common::ObObj*& min_obj,
      common::ObObj*& max_obj);

  static int get_var_basic_default(double& distinct_num, double& null_num, double& row_count, double& origin_row_count);

  // col RANGE_CMP const, column_range_sel
  // func(col) RANGE_CMP const, DEFAULT_INEQ_SEL
  // col1 RANGE_CMP col2, DEFAULT_INEQ_SEL
  static int get_range_cmp_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity);

  //  Get simple predicate selectivity
  //  (col) | (col +-* num) = const, sel = distinct_sel
  //  (col) | (col +-* num) = null, sel = 0
  //  (col) | (col +-* num) <=> const, sel = distinct_sel
  //  (col) | (col +-* num) <=> null, sel = null_sel
  //  multi_col | func(col) =|<=> null, sel DEFAULT_EQ_SEL 0.005
  // @param partition_id only used in base table
  static int get_simple_predicate_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& cnt_col_expr,
      const ObRawExpr* calculable_expr, const bool null_safe, double& selectivity);

  // cnt_col in (num1, num2, num3), sel: simple_predicate_sel * num_count
  // num in (num1, num2, num3) sel:if some num is list equal with num, 1.0,
  //                               else 0.0
  // cnt_col in (col, num2, num3) or num in (col1, col2, num)
  //   sel: If have equal, sel = 1.0
  //        Else sel = sum_sel(each_equal_condition)
  static int get_in_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity,
      ObJoinType join_type = UNKNOWN_JOIN, const ObRelIds* left_rel_ids = NULL, const ObRelIds* right_rel_ids = NULL);

  // not c1 in (a,b); not c1 > 100...
  // not op.
  // if can calculate null_sel, sel = 1.0 - null_sel - op_sel
  // else sel = 1.0 - op_sel
  static int get_not_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity,
      common::ObIArray<ObExprSelPair>* all_predicate_sel, ObJoinType join_type = UNKNOWN_JOIN,
      const ObRelIds* left_rel_ids = NULL, const ObRelIds* right_rel_ids = NULL, const double left_row_count = -1.0,
      const double right_row_count = -1.0);

  // col or (col +-* 2) != 1, 1.0 - distinct_sel - null_sel
  // col or (col +-* 2) != NULL -> 0.0
  // otherwise DEFAULT_SEL;
  // TODO consider how to combine with get_equal_sel
  static int get_ne_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity);

  // get var is[not] NULL\true\false selectivity
  // for var is column:
  //   var is NULL: selectivity = null_sel(get_var_basic_sel)
  //   var is true: selectivity = 1 - distinct_sel(var = 0) - null_sel
  //   var is false: selectivity = distinct_sel(var = 0)
  // others:
  //   DEFAULT_SEL
  // for var is not NULL\true\false: selectivity = 1.0 - is_sel
  static int get_is_or_not_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& var_expr, const ObRawExpr& value_expr,
      bool is_op, double& selectivity);

  // @param partition_id only used in base table
  static int calc_column_range_selectivity(const ObEstSelInfo& est_sel_info, const ObRawExpr& column_expr,
      const ObEstRange& range, const bool discrete_type, const common::ObBorderFlag border_flag,
      ObEstimateType est_type, bool& last_column, double& selectivity);

  // sel = revise_range_sel(($end - $start) / (max - min))
  static int do_calc_range_selectivity(const ObEstColRangeInfo& col_range_info, bool& scan, double& selectivity);

  static double calc_range_exceeds_limit_additional_selectivity(
      double min, double max, double end, double zero_sel_range_ratio = common::DEFAULT_RANGE_EXCEED_LIMIT_DECAY_RATIO);

  // column could be found in current level or not
  static int column_in_current_level_stmt(const ObDMLStmt* stmt, const ObRawExpr& expr, bool& is_in);

  static const TableItem* get_table_item_for_statics(const ObDMLStmt& stmt, uint64_t table_id);

  ///////////////Start normalize same column filters selectivity function/////
  static int add_range_sel(ObRawExpr* qual, double selectivity, common::ObIArray<RangeSel>& range_sels);
  static int find_range_sel(ObRawExpr* expr, common::ObIArray<RangeSel>& range_sels, int64_t& range_idx);
  ///////////////End normalize same column filters selectivity function/////

  // for discrete value, close range add 1.0 / distinct. Open range sub 1.0 / distinct
  // for continuous value, close range add 2.0 / distinct, with one inclusive add 1.0 /distinct
  static double revise_range_sel(
      double selectivity, double distinct, bool discrete, bool include_start, bool include_end);
  // TODO function not used, may deleted
  static double calc_single_value_exceeds_limit_selectivity(double min, double max, double value, double base_sel,
      double zero_sel_range_ratio = common::DEFAULT_RANGE_EXCEED_LIMIT_DECAY_RATIO);

  static int check_mutex_or(const ObRawExpr* ref_expr, int64_t index, bool& is_mutex);

  static int get_simple_eq_op_exprs(const ObRawExpr* qual, const ObRawExpr*& column, bool& is_simple);

  static int is_simple_join_condition(ObRawExpr& qual, const ObRelIds* left_rel_ids, const ObRelIds* right_rel_ids,
      bool& is_valid, ObIArray<ObRawExpr*>& join_conditions);

  static int get_equal_sel(const ObEstSelInfo& est_sel_info, ObIArray<ObRawExpr*>& quals, double& selectivity,
      ObJoinType join_type, const ObRelIds& left_rel_ids, const ObRelIds& right_rel_ids, const double left_rows,
      const double right_rows);

  static int get_cntcols_eq_cntcols_sel(const ObEstSelInfo& est_sel_info, const ObIArray<ObRawExpr*>& left_exprs,
      const ObIArray<ObRawExpr*>& right_exprs, const ObIArray<bool>& null_safes, double& selectivity,
      ObJoinType join_type, const double left_rows, const double right_rows);

  static int extract_join_exprs(ObIArray<ObRawExpr*>& quals, const ObRelIds& left_rel_ids,
      const ObRelIds& right_rel_ids, ObIArray<ObRawExpr*>& left_exprs, ObIArray<ObRawExpr*>& right_exprs,
      ObIArray<bool>& null_safes);

  static int get_agg_sel(const ObEstSelInfo& est_sel_info, const ObRawExpr& qual, double& selectivity,
      const double origen_rows, const double grouped_rows);

  static int get_agg_sel_with_minmax(const ObEstSelInfo& est_sel_info, const ObRawExpr& aggr_expr,
      const ObRawExpr* const_expr1, const ObRawExpr* const_expr2, const ObItemType type, double& selectivity,
      const double rows_per_group);

  static double get_agg_eq_sel(const ObObj& maxobj, const ObObj& minobj, const ObObj& constobj,
      const double distinct_sel, const double rows_per_group, const bool is_eq, const bool is_sum);

  static double get_agg_range_sel(const ObObj& maxobj, const ObObj& minobj, const ObObj& constobj,
      const double rows_per_group, const ObItemType type, const bool is_sum);

  static double get_agg_btw_sel(const ObObj& maxobj, const ObObj& minobj, const ObObj& constobj1,
      const ObObj& constobj2, const double rows_per_group, const ObItemType type, const bool is_sum);

  static int is_valid_agg_qual(const ObRawExpr& qual, bool& is_valid, const ObRawExpr*& aggr_expr,
      const ObRawExpr*& const_expr1, const ObRawExpr*& const_expr2);

  static int get_global_min_max(
      const ObEstSelInfo& est_sel_info, const ObColumnRefRawExpr& column_expr, ObObj*& minobj, ObObj*& maxobj);

  static int is_column_pkey(const ObEstSelInfo& est_sel_info, const ObColumnRefRawExpr& col_expr, bool& is_pkey);

  static int is_columns_contain_pkey(
      const ObEstSelInfo& est_sel_info, const ObIArray<ObRawExpr*>& col_exprs, bool& is_pkey, bool& is_union_pkey);

  static int is_columns_contain_pkey(const ObEstSelInfo& est_sel_info, const ObIArray<uint64_t>& col_ids,
      const uint64_t table_id, bool& is_pkey, bool& is_union_pkey);

  static int extract_column_ids(const ObIArray<ObRawExpr*>& col_exprs, ObIArray<uint64_t>& col_ids, uint64_t& table_id);

  static int is_valid_multi_join(ObIArray<ObRawExpr*>& quals, bool& is_valid);

  static inline bool is_number_euqal_to_zero(double num)
  {
    return fabs(num) < OB_DOUBLE_EPSINON;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObOptEstSel);
};
}  // namespace sql
}  // namespace oceanbase

#endif
