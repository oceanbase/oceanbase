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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_OPT_SELECTIVITY_
#define OCEANBASE_SQL_OPTIMIZER_OB_OPT_SELECTIVITY_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "common/object/ob_object.h"
#include "common/ob_range.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/optimizer/ob_opt_default_stat.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/optimizer/ob_dynamic_sampling.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace common
{
class ObOptStatManager;
class ObHistogram;
class ObOptColumnStatHandle;
}
namespace sql
{
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

class OptSelectivityCtx
{
 public:
  OptSelectivityCtx(ObOptimizerContext &ctx, const ObLogPlan *plan, const ObDMLStmt *stmt)
  : opt_ctx_(ctx),
    plan_(plan),
    stmt_(stmt),
    equal_sets_(NULL),
    join_type_(UNKNOWN_JOIN),
    left_rel_ids_(NULL),
    right_rel_ids_(NULL),
    row_count_1_(-1.0),
    row_count_2_(-1.0),
    current_rows_(-1.0)
  { }

  ObOptimizerContext &get_opt_ctx() { return opt_ctx_; }
  const ObOptimizerContext &get_opt_ctx() const { return opt_ctx_; }
  const ObDMLStmt *get_stmt() const { return stmt_; }
  const ObLogPlan *get_plan() const { return plan_; }
  
  const EqualSets *get_equal_sets() const { return equal_sets_; }
  void set_equal_sets(EqualSets *equal_sets) { equal_sets_ = equal_sets; } 

  ObSQLSessionInfo *get_session_info() { return opt_ctx_.get_session_info(); }
  ObSQLSessionInfo *get_session_info() const
  {
    return const_cast<ObSQLSessionInfo *>(opt_ctx_.get_session_info());
  }

  ObSqlSchemaGuard *get_sql_schema_guard() { return opt_ctx_.get_sql_schema_guard(); }

  common::ObOptStatManager *get_opt_stat_manager() { return opt_ctx_.get_opt_stat_manager(); }
  common::ObOptStatManager *get_opt_stat_manager() const
  {
    return const_cast<common::ObOptStatManager *>(opt_ctx_.get_opt_stat_manager());
  }
  common::ObIAllocator &get_allocator() { return opt_ctx_.get_allocator(); }
  common::ObIAllocator &get_allocator() const
  {
    return const_cast<common::ObIAllocator &>(opt_ctx_.get_allocator()); 
  }
  const ParamStore *get_params() const { return opt_ctx_.get_params(); }
  bool use_default_stat() const { return opt_ctx_.use_default_stat(); }

  ObJoinType get_join_type() const { return join_type_; }
  const ObRelIds *get_left_rel_ids() const { return left_rel_ids_; }
  const ObRelIds *get_right_rel_ids() const { return right_rel_ids_; }
  double get_row_count_1() const { return row_count_1_; }
  double get_row_count_2() const { return row_count_2_; }
  
  double get_current_rows() const { return current_rows_; }
  void set_current_rows(const double current_rows) { current_rows_ = current_rows; }

  void init_op_ctx(const EqualSets *equal_sets, const double current_rows)
  {
    equal_sets_ = equal_sets;
    current_rows_ = current_rows;
  }
  void init_row_count(const double row_count1, const double row_count2)
  {
    row_count_1_ = row_count1;
    row_count_2_ = row_count2;
  }

  void init_join_ctx(const ObJoinType join_type, const ObRelIds *left_rel_ids,
                     const ObRelIds *right_rel_ids, const double rc1, const double rc2,
                     const EqualSets *equal_sets = NULL)
  {
    join_type_ = join_type;
    left_rel_ids_ = left_rel_ids;
    right_rel_ids_ = right_rel_ids;
    row_count_1_ = rc1;
    row_count_2_ = rc2;
    current_rows_ = -1.0;
    equal_sets_ = equal_sets;
  }

  void clear_equal_sets() { equal_sets_ = NULL; }

  TO_STRING_KV(KP_(stmt), KP_(equal_sets), K_(join_type), KP_(left_rel_ids), KP_(right_rel_ids),
               K_(row_count_1), K_(row_count_2), K_(current_rows));

 private:
  ObOptimizerContext &opt_ctx_;
  const ObLogPlan *plan_;
  const ObDMLStmt *stmt_;
  const EqualSets *equal_sets_;
  ObJoinType join_type_;
  const ObRelIds *left_rel_ids_;
  const ObRelIds *right_rel_ids_;
  /**
   * when calculate join condition selectivity, 
   *    row_count_1_ represent left table row count
   *    row_count_2_ represent right table row count
   * when calculate having filter selectivity
   *    row_count_1_ represent row count before group by
   *    row_count_2_ represent row count after group by
   */
  double row_count_1_;
  double row_count_2_;
  double current_rows_;
};

class OptColumnMeta
{
public:
  OptColumnMeta() :
    column_id_(OB_INVALID_ID),
    ndv_(0),
    num_null_(0),
    avg_len_(0),
    hist_scale_(-1),
    min_max_inited_(false)
  {
    min_val_.set_min_value();
    max_val_.set_max_value();
  }
  int assign(const OptColumnMeta &other);
  void init(const uint64_t column_id, const double ndv, const double num_null, const double avg_len);

  uint64_t get_column_id() const { return column_id_; }
  void set_column_id(const uint64_t column_id) { column_id_ = column_id; }
  double get_ndv() const { return ndv_; }
  void set_ndv(const double ndv) { ndv_ = ndv; }
  double get_num_null() const { return num_null_; }
  void set_num_null(const double num_null) { num_null_ = num_null; }
  double get_avg_len() const { return avg_len_; }
  void set_avg_len(const double avg_len) { avg_len_ = avg_len; }
  double get_hist_scale() const { return hist_scale_; }
  void set_hist_scale(const double hist_scale) { hist_scale_ = hist_scale; }
  ObObj& get_min_value() { return min_val_; }
  const ObObj& get_min_value() const { return min_val_; }
  void set_min_value(const ObObj& min_val) { min_val_ = min_val; }
  ObObj& get_max_value() { return max_val_; }
  const ObObj& get_max_value() const { return max_val_; }
  void set_max_value(const ObObj& max_val) { max_val_ = max_val; }
  bool get_min_max_inited() const { return min_max_inited_; }
  void set_min_max_inited(const bool inited) { min_max_inited_ = inited; }

  TO_STRING_KV(K_(column_id), K_(ndv), K_(num_null), K_(avg_len), K_(hist_scale),
               K_(min_val), K_(max_val) , K_(min_max_inited));
private:
  uint64_t column_id_;
  double ndv_;
  double num_null_;
  double avg_len_;
  // the percentage of the histogram sample size that is available. For example, hist_scale = 0.5
  // means that current sample size of histogram is 50% of origin sample size.
  double hist_scale_; 
  ObObj min_val_;
  ObObj max_val_;
  bool min_max_inited_;
};

enum OptTableStatType {
  DEFAULT_TABLE_STAT = 0,    //default table stat.
  OPT_TABLE_STAT,            //optimizer gather table stat.
  OPT_TABLE_GLOBAL_STAT,     //optimizer gather table global stat when no table part stat.
  DS_TABLE_STAT              //dynamic sampling table stat
};

class OptTableMeta
{
public:
  OptTableMeta() :
    table_id_(OB_INVALID_ID),
    ref_table_id_(OB_INVALID_ID),
    table_type_(share::schema::MAX_TABLE_TYPE),
    rows_(0),
    stat_type_(OptTableStatType::DEFAULT_TABLE_STAT),
    last_analyzed_(0),
    all_used_parts_(),
    all_used_tablets_(),
    pk_ids_(),
    column_metas_(),
    ds_level_(ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING),
    all_used_global_parts_(),
    scale_ratio_(1.0)
  {}
  int assign(const OptTableMeta &other);

  int init(const uint64_t table_id,
           const uint64_t ref_table_id,
           const share::schema::ObTableType table_type,
           const int64_t rows,
           const OptTableStatType stat_type,
           ObSqlSchemaGuard &schema_guard,
           common::ObIArray<int64_t> &all_used_part_id,
           common::ObIArray<ObTabletID> &all_used_tablets,
           common::ObIArray<uint64_t> &column_ids,
           ObIArray<int64_t> &all_used_global_parts,
           const double scale_ratio,
           const OptSelectivityCtx &ctx);

  // int update_stat(const double rows, const bool can_reduce, const bool can_enlarge);

  int init_column_meta(const OptSelectivityCtx &ctx,
                       const uint64_t column_id,
                       OptColumnMeta &col_meta);

  int add_column_meta_no_dup(const uint64_t column_id, const OptSelectivityCtx &ctx);

  const OptColumnMeta* get_column_meta(const uint64_t column_id) const;

  uint64_t get_table_id() const { return table_id_; }
  void set_table_id(const uint64_t &table_id) { table_id_ = table_id; }
  uint64_t get_ref_table_id() const { return ref_table_id_; }
  void set_ref_table_id(const uint64_t &ref_table_id) { ref_table_id_ = ref_table_id; }
  double get_rows() const { return rows_; }
  void set_rows(const double rows) { rows_ = rows; }
  int64_t get_version() const { return last_analyzed_; }
  void set_version(const int64_t version) { last_analyzed_ = version; }
  const common::ObIArray<int64_t>& get_all_used_parts() const { return all_used_parts_; }
  common::ObIArray<int64_t> &get_all_used_parts() { return all_used_parts_; }
  const common::ObIArray<ObTabletID>& get_all_used_tablets() const { return all_used_tablets_; }
  common::ObIArray<ObTabletID> &get_all_used_tablets() { return all_used_tablets_; }
  const common::ObIArray<uint64_t>& get_pkey_ids() const { return pk_ids_; }
  common::ObIArray<OptColumnMeta>& get_column_metas() { return column_metas_; }
  const common::ObIArray<int64_t>& get_all_used_global_parts() const { return all_used_global_parts_; }
  common::ObIArray<int64_t> &get_all_used_global_parts() { return all_used_global_parts_; }
  double get_scale_ratio() const { return scale_ratio_; }
  void set_scale_ratio(const double scale_ratio) { scale_ratio_ = scale_ratio; }

  void set_ds_level(const int64_t ds_level) { ds_level_ = ds_level; }
  int64_t get_ds_level() const { return ds_level_; }
  bool use_default_stat() const { return stat_type_ == OptTableStatType::DEFAULT_TABLE_STAT; }
  bool use_opt_stat() const { return stat_type_ == OptTableStatType::OPT_TABLE_STAT ||
                                     stat_type_ == OptTableStatType::OPT_TABLE_GLOBAL_STAT; }
  bool use_opt_global_stat() const { return stat_type_ == OptTableStatType::OPT_TABLE_GLOBAL_STAT; }
  bool use_ds_stat() const { return stat_type_ == OptTableStatType::DS_TABLE_STAT; }
  void set_use_ds_stat() { stat_type_ = OptTableStatType::DS_TABLE_STAT; }

  share::schema::ObTableType get_table_type() const { return table_type_; }

  TO_STRING_KV(K_(table_id), K_(ref_table_id), K_(table_type), K_(rows), K_(stat_type), K_(ds_level),
               K_(all_used_parts), K_(all_used_tablets), K_(pk_ids), K_(column_metas),
               K_(all_used_global_parts), K_(scale_ratio));
private:
  uint64_t table_id_;
  uint64_t ref_table_id_;
  const share::schema::ObTableType table_type_;
  double rows_;
  OptTableStatType stat_type_;
  int64_t last_analyzed_;

  ObSEArray<int64_t, 64, common::ModulePageAllocator, true> all_used_parts_;
  ObSEArray<ObTabletID, 64, common::ModulePageAllocator, true> all_used_tablets_;
  ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> pk_ids_;
  ObSEArray<OptColumnMeta, 32, common::ModulePageAllocator, true> column_metas_;
  int64_t ds_level_;//dynamic sampling level
  ObSEArray<int64_t, 64, common::ModulePageAllocator, true> all_used_global_parts_;
  double scale_ratio_;
};

struct OptSelectivityDSParam {
  OptSelectivityDSParam() :
    table_meta_(NULL),
    quals_()
  {}
  TO_STRING_KV(KPC(table_meta_),
               K(quals_));
  const OptTableMeta *table_meta_;
  ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> quals_;
};

class OptTableMetas
{
public:
  int copy_table_meta_info(const OptTableMeta &src_meta, OptTableMeta *&dst_meta);
  int copy_table_meta_info(const OptTableMetas &table_metas, const uint64_t table_id);

  int add_base_table_meta_info(OptSelectivityCtx &ctx,
                               const uint64_t table_id,
                               const uint64_t ref_table_id,
                               const share::schema::ObTableType table_type,
                               const int64_t rows,
                               common::ObIArray<int64_t> &all_used_part_id,
                               common::ObIArray<ObTabletID> &all_used_tablets,
                               common::ObIArray<uint64_t> &column_ids,
                               const OptTableStatType stat_type,
                               ObIArray<int64_t> &all_used_global_parts,
                               const double scale_ratio,
                               int64_t last_analyzed);

  int add_set_child_stmt_meta_info(const ObDMLStmt *parent_stmt,
                                   const ObSelectStmt *child_stmt,
                                   const uint64_t table_id,
                                   const OptTableMetas &child_table_metas,
                                   const OptSelectivityCtx &child_ctx,
                                   const double child_rows);

  int add_generate_table_meta_info(const ObDMLStmt *parent_stmt,
                                   const ObSelectStmt *child_stmt,
                                   const uint64_t table_id,
                                   const OptTableMetas &child_table_metas,
                                   const OptSelectivityCtx &child_ctx,
                                   const double child_rows);

  int get_set_stmt_output_statistics(const ObSelectStmt &stmt,
                                     const OptTableMetas &child_table_metas,
                                     const int64_t idx,
                                     double &ndv,
                                     double &num_null,
                                     double &avg_len);

  common::ObIArray<OptTableMeta>& get_table_metas() { return table_metas_; }
  const OptTableMeta* get_table_meta_by_table_id(const uint64_t table_id) const;
  OptTableMeta* get_table_meta_by_table_id(const uint64_t table_id);
  const OptColumnMeta* get_column_meta_by_table_id(const uint64_t table_id,
                                                   const uint64_t column_id) const;

  int get_rows(const uint64_t table_id, double &rows);
  TO_STRING_KV(K_(table_metas));
private:
  common::ObSEArray<OptTableMeta, 16, common::ModulePageAllocator, true> table_metas_;
};

struct OptSelInfo
{
  OptSelInfo() :
    column_id_(0),
    selectivity_(1.0),
    equal_count_(0),
    range_selectivity_(1.0),
    has_range_exprs_(false) {}

  TO_STRING_KV(K_(column_id), K_(selectivity), K_(equal_count),
               K_(range_selectivity), K_(has_range_exprs));

  uint64_t column_id_;
  double selectivity_;
  uint64_t equal_count_;
  double range_selectivity_;
  bool has_range_exprs_;
};

struct ObEstColRangeInfo
{
  ObEstColRangeInfo(double min,
                    double max,
                    const common::ObObj *startobj,
                    const common::ObObj *endobj,
                    double distinct,
                    bool discrete,
                    common::ObBorderFlag border_flag)
      : min_(min), max_(max), startobj_(startobj), endobj_(endobj),
        distinct_(distinct), discrete_(discrete), border_flag_(border_flag)
  { }
  double min_;
  double max_;
  const common::ObObj *startobj_;
  const common::ObObj *endobj_;
  double distinct_;
  bool discrete_;
  common::ObBorderFlag border_flag_;
};


class ObOptSelectivity
{
public:
  // @brief 计算一组条件的选择率，条件之间是and关系，基于独立性假设
  static int calculate_selectivity(const OptTableMetas &table_metas,
                                   const OptSelectivityCtx &ctx,
                                   const common::ObIArray<ObRawExpr*> &quals,
                                   double &selectivity,
                                   common::ObIArray<ObExprSelPair> &all_predicate_sel);

  static int calculate_qual_selectivity(const OptTableMetas &table_metas,
                                        const OptSelectivityCtx &ctx,
                                        const ObRawExpr &qual,
                                        double &selectivity,
                                        ObIArray<ObExprSelPair> &all_predicate_sel);

  static int update_table_meta_info(const OptTableMetas &base_table_metas,
                                    OptTableMetas &update_table_metas,
                                    const OptSelectivityCtx &ctx,
                                    const uint64_t table_id,
                                    double filtered_rows,
                                    const common::ObIArray<ObRawExpr*> &quals,
                                    common::ObIArray<ObExprSelPair> &all_predicate_sel);

  static int calc_sel_for_equal_join_cond(const OptTableMetas &table_metas,
                                          const OptSelectivityCtx &ctx,
                                          const common::ObIArray<ObRawExpr*>& conds,
                                          const ObRelIds &left_ids,
                                          double &left_selectivity,
                                          double &right_selectivity);

  static int get_single_newrange_selectivity(const OptTableMetas &table_metas,
                                             const OptSelectivityCtx &ctx,
                                             const ObIArray<ColumnItem> &range_columns,
                                             const ObNewRange &range,
                                             double &selectivity);

  // @brief 计算一组变量的distinct
  static int calculate_distinct(const OptTableMetas &table_metas,
                                const OptSelectivityCtx &ctx,
                                const common::ObIArray<ObRawExpr*>& exprs,
                                const double origin_rows,
                                double &rows,
                                const bool need_refine = true);

  // ndv 按照行数进行缩放.
  static double scale_distinct(double selected_rows, double rows, double ndv);

  static inline double revise_between_0_1(double num)
  { return num < 0 ? 0 : (num > 1 ? 1 : num); }

private:
  static int check_qual_later_calculation(const OptTableMetas &table_metas,
                                          const OptSelectivityCtx &ctx,
                                          ObRawExpr &qual,
                                          ObIArray<ObExprSelPair> &all_pred_sel,
                                          ObIArray<ObRawExpr *> &join_conditions,
                                          ObIArray<RangeExprs> &range_conditions,
                                          bool &need_skip);
  
  static int is_simple_join_condition(ObRawExpr &qual,
                                      const ObRelIds *left_rel_ids,
                                      const ObRelIds *right_rel_ids,
                                      bool &is_valid,
                                      ObIArray<ObRawExpr *> &join_conditions);

  /**
   * calculate const or calculable expr selectivity.
   * e.g. `1`, `1 = 1`, `1 + 1`, `1 = 0`
   * if expr is always true, selectivity = 1.0
   * if expr is always false, selectivity = 0.0
   * if expr can't get actual value, like exec_param, selectivity = 0.5
   */
  static int get_const_sel(const OptSelectivityCtx &ctx,
                           const ObRawExpr &qual,
                           double &selectivity);

  /**
   * calculate column expr selectivity.
   * e.g. `c1`, `t1.c1`
   * selectity = 1.0 - sel(t1.c1 = 0) - sel(t1.c1 is NULL)
   */
  static int get_column_sel(const OptTableMetas &table_metas,
                            const OptSelectivityCtx &ctx,
                            const ObRawExpr &qual,
                            double &selectivity);

  //1. var = | <=> const, get_simple_predicate_sel
  //2. func(var) = | <=> const,
  //       only simple op(+,-,*,/), get_simple_predicate_sel,
  //       mod(cnt_var, mod_num),  distinct_sel * mod_num
  //       else sqrt(distinct_sel)
  //3. cnt(var) = |<=> cnt(var) get_cntcol_eq_cntcol_sel
  static int get_equal_sel(const OptTableMetas &table_metas,
                           const OptSelectivityCtx &ctx,
                           const ObRawExpr &qual,
                           double &selectivity);
  
  static int get_equal_sel(const OptTableMetas &table_metas,
                           const OptSelectivityCtx &ctx,
                           const ObRawExpr &left_expr,
                           const ObRawExpr &right_expr,
                           const bool null_safe,
                           double &selectivity);


  //  Get simple predicate selectivity
   //  (col) | (col +-* num) = const, sel = distinct_sel
   //  (col) | (col +-* num) = null, sel = 0
   //  (col) | (col +-* num) <=> const, sel = distinct_sel
   //  (col) | (col +-* num) <=> null, sel = null_sel
   //  multi_col | func(col) =|<=> null, sel DEFAULT_EQ_SEL 0.005
   // @param partition_id only used in base table
  /**
   * calculate equal predicate with format `contain_column_expr = not_contain_column_expr` by ndv
   * e.g. `c1 = 1`, `c1 + 1 = 2`, `c1 + c2 = 10`
   * if contain_column_expr contain not monotonic operator or has more than one column, 
   *    selectivity = DEFAULT_EQ_SEL
   * if contain_column_expr contain only one column and contain only monotonic operator,
   *    selectivity = 1 / ndv
   */
  static int get_simple_equal_sel(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  const ObRawExpr &cnt_col_expr,
                                  const ObRawExpr *calculable_expr,
                                  const bool null_safe,
                                  double &selectivity);

  static int get_cntcol_op_cntcol_sel(const OptTableMetas &table_metas,
                                      const OptSelectivityCtx &ctx,
                                      const ObRawExpr &input_left_expr,
                                      const ObRawExpr &input_right_expr,
                                      ObItemType op_type,
                                      double &selectivity);

  static int get_equal_sel(const OptTableMetas &table_metas,
                           const OptSelectivityCtx &ctx,
                           ObIArray<ObRawExpr *> &quals,
                           double &selectivity);

  static int extract_join_exprs(ObIArray<ObRawExpr *> &quals,
                                const ObRelIds &left_rel_ids,
                                const ObRelIds &right_rel_ids,
                                ObIArray<ObRawExpr *> &left_exprs,
                                ObIArray<ObRawExpr *> &right_exprs,
                                ObIArray<bool> &null_safes);

  static int get_cntcols_eq_cntcols_sel(const OptTableMetas &table_metas,
                                        const OptSelectivityCtx &ctx,
                                        const ObIArray<ObRawExpr *> &left_exprs,
                                        const ObIArray<ObRawExpr *> &right_exprs,
                                        const ObIArray<bool> &null_safes,
                                        double &selectivity);

  /**
   * calculate [not] in predicate selectivity
   * e.g. `c1 in (1, 2, 3)`, `1 in (c1, c2, c3)`
   * The most commonly format `column in (const1, const2, const3)`
   *    selectivity = sum(selectivity(column = const_i))
   * otherwise, `var in (var1, var2, var3)
   *    selectivity = sum(selectivity(var = var_i))
   * not_in_selectivity = 1.0 - in_selectivity
   */
  static int get_in_sel(const OptTableMetas &table_metas,
                        const OptSelectivityCtx &ctx,
                        const ObRawExpr &qual,
                        double &selectivity);

  // get var is[not] NULL\true\false selectivity
  // for var is column:
  //   var is NULL: selectivity = null_sel(get_var_basic_sel)
  //   var is true: selectivity = 1 - distinct_sel(var = 0) - null_sel
  //   var is false: selectivity = distinct_sel(var = 0)
  // others:
  //   DEFAULT_SEL
  // for var is not NULL\true\false: selectivity = 1.0 - is_sel
  /**
   * calculate is [not] predicate selectivity
   * e.g. `c1 is null`， `c1 is ture`(mysql only)
   */
  static int get_is_sel(const OptTableMetas &table_metas,
                        const OptSelectivityCtx &ctx,
                        const ObRawExpr &qual,
                        double &selectivity);

  //col RANGE_CMP const, column_range_sel
  //func(col) RANGE_CMP const, DEFAULT_INEQ_SEL
  //col1 RANGE_CMP col2, DEFAULT_INEQ_SEL
  static int get_range_cmp_sel(const OptTableMetas &table_metas,
                               const OptSelectivityCtx &ctx,
                               const ObRawExpr &qual,
                               double &selectivity);

  static int get_column_range_sel(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  const ObColumnRefRawExpr &col_expr,
                                  const ObRawExpr &qual,
                                  double &selectivity);

  //param:As some expr, query range can't calc range, then range will be (min, max).
  //no_whole_range representing that expr_sel should not use whole range to calc sel.
  //if OB_INVALID_ID == partition_id, then use part_id(0)
  static int get_column_range_sel(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  const ObColumnRefRawExpr &col_expr,
                                  const ObIArray<ObRawExpr* > &quals,
                                  double &selectivity);

  static int calc_column_range_selectivity(const OptTableMetas &table_metas,
                                           const OptSelectivityCtx &ctx,
                                           const ObRawExpr &column_expr,
                                           const ObObj &start_obj,
                                           const ObObj &end_obj,
                                           const bool discrete,
                                           const ObBorderFlag border_flag,
                                           bool &last_column,
                                           double &selectivity);

  static int do_calc_range_selectivity(const double min,
                                       const double max,
                                       const ObObj &scalar_start,
                                       const ObObj &scalar_end,
                                       const double ndv,
                                       const bool discrete,
                                       const ObBorderFlag &border_flag,
                                       bool &last_column,
                                       double &selectivity);
  
  //for discrete value, close range add 1.0 / distinct. Open range sub 1.0 / distinct
  //for continuous value, close range add 2.0 / distinct, with one inclusive add 1.0 /distinct
  static double revise_range_sel(double selectivity,
                                 double distinct,
                                 bool discrete,
                                 bool include_start,
                                 bool include_end);

  /**
   * calculate like predicate selectivity.
   * e.g. `c1 like 'xx%'`, `c1 like '%xx'`
   * c1 like 'xx%', use query range selectivity
   * c1 like '%xx', use DEFAULT_INEQ_SEL 1.0 / 3.0
   */
  static int get_like_sel(const OptTableMetas &table_metas,
                          const OptSelectivityCtx &ctx,
                          const ObRawExpr &qual,
                          double &selectivity,
                          bool &can_calc_sel);

  //c1 between $val1 and $val2     -> equal with [$val2 - $val1] range sel
  //c1 not between $val1 and $val2 -> equal with (min, $val1) or ($val2, max) range sel
  static int get_btw_sel(const OptTableMetas &table_metas,
                         const OptSelectivityCtx &ctx,
                         const ObRawExpr &qual,
                         double &selectivity);

  // not c1 in (a,b); not c1 > 100...
  // not op.
  // if can calculate null_sel, sel = 1.0 - null_sel - op_sel
  // else sel = 1.0 - op_sel
  static int get_not_sel(const OptTableMetas &table_metas,
                         const OptSelectivityCtx &ctx,
                         const ObRawExpr &qual,
                         double &selectivity,
                         common::ObIArray<ObExprSelPair> &all_predicate_sel);

  // col or (col +-* 2) != 1, 1.0 - distinct_sel - null_sel
  // col or (col +-* 2) != NULL -> 0.0
  // otherwise DEFAULT_SEL;
  static int get_ne_sel(const OptTableMetas &table_metas,
                        const OptSelectivityCtx &ctx,
                        const ObRawExpr &qual,
                        double &selectivity);

  static int get_ne_sel(const OptTableMetas &table_metas,
                        const OptSelectivityCtx &ctx,
                        const ObRawExpr &l_expr,
                        const ObRawExpr &r_expr,
                        double &selectivity);

  static int get_agg_sel(const OptTableMetas &table_metas,
                         const OptSelectivityCtx &ctx,
                         const ObRawExpr &qual,
                         double &selectivity);

  static int get_agg_sel_with_minmax(const OptTableMetas &table_metas,
                                     const OptSelectivityCtx &ctx,
                                     const ObRawExpr &aggr_expr,
                                     const ObRawExpr *const_expr1,
                                     const ObRawExpr *const_expr2,
                                     const ObItemType type,
                                     double &selectivity,
                                     const double rows_per_group);

  static double get_agg_eq_sel(const ObObj &maxobj,
                               const ObObj &minobj,
                               const ObObj &constobj,
                               const double distinct_sel,
                               const double rows_per_group,
                               const bool is_eq,
                               const bool is_sum);

  static double get_agg_range_sel(const ObObj &maxobj,
                                  const ObObj &minobj,
                                  const ObObj &constobj,
                                  const double rows_per_group,
                                  const ObItemType type,
                                  const bool is_sum);

  static double get_agg_btw_sel(const ObObj &maxobj,
                                const ObObj &minobj,
                                const ObObj &constobj1,
                                const ObObj &constobj2,
                                const double rows_per_group,
                                const ObItemType type,
                                const bool is_sum);  

  static int is_valid_agg_qual(const ObRawExpr &qual,
                               bool &is_valid,
                               const ObRawExpr *&aggr_expr,
                               const ObRawExpr *&const_expr1,
                               const ObRawExpr *&const_expr2);

  static int check_column_in_current_level_stmt(const ObDMLStmt *stmt,
                                                const ObRawExpr &expr);
  static int column_in_current_level_stmt(const ObDMLStmt *stmt,
                                          const ObRawExpr &expr,
                                          bool &is_in);

  static int get_column_basic_sel(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  const ObRawExpr &expr,
                                  double *distinct_sel_ptr = NULL,
                                  double *null_sel_ptr = NULL);

  static int get_column_ndv_and_nns(const OptTableMetas &table_metas,
                                    const OptSelectivityCtx &ctx,
                                    const ObRawExpr &expr,
                                    double *ndv_ptr,
                                    double *not_null_sel_ptr);
  static int get_column_ndv_and_nns_by_equal_set(const OptTableMetas &table_metas,
                                                 const OptSelectivityCtx &ctx,
                                                 const ObRawExpr *&expr,
                                                 double &ndv,
                                                 double &not_null_sel);

  
  static int get_column_min_max(const OptTableMetas &table_metas,
                                const OptSelectivityCtx &ctx,
                                const ObRawExpr &expr,  
                                ObObj &min_obj,
                                ObObj &max_obj);

  static int get_column_basic_info(const OptTableMetas &table_metas,
                                   const OptSelectivityCtx &ctx,
                                   const ObRawExpr &expr,
                                   double *ndv_ptr,
                                   double *num_null_ptr,
                                   double *row_count_ptr,
                                   double *avg_len_ptr);

  static int get_column_hist_scale(const OptTableMetas &table_metas,
                                   const OptSelectivityCtx &ctx,
                                   const ObRawExpr &expr,
                                   double &hist_scale);
  
  static int get_column_basic_from_meta(const OptTableMetas &table_metas,
                                        const ObColumnRefRawExpr &column_expr,
                                        bool &use_default,
                                        double &row_count,
                                        double &ndv,
                                        double &num_null,
                                        double &avg_len);

  static int get_var_basic_default(double &row_count,
                                   double &ndv,
                                   double &null_num,
                                   double &avg_len);

  static int get_histogram_by_column(const OptTableMetas &table_metas,
                                     const OptSelectivityCtx &ctx,
                                     uint64_t table_id,
                                     uint64_t column_id,
                                     ObOptColumnStatHandle &column_stat);

  static int get_compare_value(const OptSelectivityCtx &ctx,
                               const ObColumnRefRawExpr *col,
                               const ObRawExpr *calc_expr,
                               ObObj &expr_value,
                               bool &can_cmp);

  static int get_bucket_bound_idx(const ObHistogram &hist,
                                  const ObObj &value,
                                  int64_t &idx,
                                  bool &is_equal);

  static int get_equal_pred_sel(const ObHistogram &histogram,
                                const ObObj &value,
                                const double sample_size_scale,
                                double &density);

  static int get_range_sel_by_histogram(const common::ObHistogram &histogram,
                                        const ObQueryRangeArray &ranges,
                                        bool no_whole_range,
                                        const double sample_size_scale,
                                        double &selectivity);

  static int get_less_pred_sel(const ObHistogram &histogram,
                               const ObObj &maxv,
                               const bool inclusive,
                               double &density);

  static int get_greater_pred_sel(const ObHistogram &histogram,
                                  const ObObj &minv,
                                  const bool inclusive,
                                  double &density);

  static int get_range_pred_sel(const ObHistogram &histogram,
                                const ObObj &minv,
                                const bool min_inclusive,
                                const ObObj &maxv,
                                const bool max_inclusive,
                                double &density);

  static int get_column_query_range(const OptSelectivityCtx &ctx,
                                    const uint64_t table_id,
                                    const uint64_t column_id,
                                    const ObIArray<ObRawExpr *> &quals,
                                    ObIArray<ColumnItem> &column_items,
                                    ObQueryRange &query_range,
                                    ObQueryRangeArray &ranges);

  // @brief 检测OR中 expr 对于第 index 个子表达式的互斥性, 只检测 c1 = v 的情况,
  // 且只考虑本层.
  // @param ref_expr OR 表达式
  // @param index 需要检测的表达式在 ref_expr 子节点的下表
  // @param is_mutex 返回值, 是否互斥
  static int check_mutex_or(const ObRawExpr &qual, bool &is_mutex);

  // @breif 从某个表达式判断并抽取单列的等值条件中包含的列
  // @param qual 要抽取的表达式
  // @param column 返回值, 抽取的结果, 抽取失败则是 NULL
  static int get_simple_mutex_column(const ObRawExpr *qual, const ObRawExpr *&column);

  static int filter_column_by_equal_set(const OptTableMetas &table_metas,
                                        const OptSelectivityCtx &ctx,
                                        const common::ObIArray<ObRawExpr*> &column_exprs,
                                        common::ObIArray<ObRawExpr*> &filtered_exprs);
  static int filter_one_column_by_equal_set(const OptTableMetas &table_metas,
                                            const OptSelectivityCtx &ctx,
                                            const ObRawExpr *column_exprs,
                                            const ObRawExpr *&filtered_exprs);
  static int get_min_ndv_by_equal_set(const OptTableMetas &table_metas,
                                      const OptSelectivityCtx &ctx,
                                      const ObRawExpr *col_expr,
                                      bool &find,
                                      ObRawExpr *&expr,
                                      double &ndv);

  /**
  * 判断多列连接是否只涉及到两个表
  */
  static int is_valid_multi_join(ObIArray<ObRawExpr *> &quals,
                                 bool &is_valid);

  /**
   * 检查一组expr是否包含所在表的主键
   */
  static int is_columns_contain_pkey(const OptTableMetas &table_metas,
                                     const ObIArray<ObRawExpr *> &col_exprs,
                                     bool &is_pkey,
                                     bool &is_union_pkey);

  static int is_columns_contain_pkey(const OptTableMetas &table_metas,
                                     const ObIArray<uint64_t> &col_ids,
                                     const uint64_t table_id,
                                     bool &is_pkey,
                                     bool &is_union_pkey);

  /**
   * 从一组expr中提取column id, 并且检查column属于同一个表
   */
  static int extract_column_ids(const ObIArray<ObRawExpr *> &col_exprs,
                                ObIArray<uint64_t> &col_ids,
                                uint64_t &table_id);

  static int classify_quals(const ObIArray<ObRawExpr*> &quals,
                            ObIArray<ObExprSelPair> &all_predicate_sel,
                            ObIArray<OptSelInfo> &column_sel_infos);

  static int get_opt_sel_info(ObIArray<OptSelInfo> &column_sel_infos,
                              const uint64_t column_id,
                              OptSelInfo *&sel_info);

  static bool get_qual_selectivity(ObIArray<ObExprSelPair> &all_predicate_sel,
                                   const ObRawExpr *qual,
                                   double &selectivity);

  static int extract_equal_count(const ObRawExpr &qual, uint64_t &equal_count);

  static int get_join_pred_rows(const ObHistogram &left_histogram,
                                const ObHistogram &right_histogram,
                                const bool is_semi,
                                double &rows);

  static int calc_complex_predicates_selectivity_by_ds(const OptTableMetas &table_metas,
                                                       const OptSelectivityCtx &ctx,
                                                       const ObIArray<ObRawExpr*> &predicates,
                                                       ObIArray<ObExprSelPair> &all_predicate_sel);

  static int calc_selectivity_by_dynamic_sampling(const OptSelectivityCtx &ctx,
                                                  const OptSelectivityDSParam &ds_param,
                                                  ObIArray<ObExprSelPair> &all_predicate_sel);

  static int resursive_extract_valid_predicate_for_ds(const OptTableMetas &table_metas,
                                                      const OptSelectivityCtx &ctx,
                                                      const ObRawExpr *qual,
                                                      ObIArray<OptSelectivityDSParam> &ds_params);

  static int add_ds_result_items(const ObIArray<ObRawExpr*> &quals,
                                 const uint64_t ref_table_id,
                                 ObIArray<ObDSResultItem> &ds_result_items);

  static int add_ds_result_into_selectivity(const ObIArray<ObDSResultItem> &ds_result_items,
                                            const uint64_t ref_table_id,
                                            ObIArray<ObExprSelPair> &all_predicate_sel);

  static int add_valid_ds_qual(const ObRawExpr *qual,
                               const OptTableMetas &table_metas,
                               ObIArray<OptSelectivityDSParam> &ds_params);

  // static int calculate_join_selectivity_by_dynamic_sampling(const OptTableMetas &table_metas,
  //                                                           const OptSelectivityCtx &ctx,
  //                                                           const ObIArray<ObRawExpr*> &predicates,
  //                                                           double &selectivity,
  //                                                           bool &is_calculated);

  // static int collect_ds_join_param(const OptTableMetas &table_metas,
  //                                  const OptSelectivityCtx &ctx,
  //                                  const ObIArray<ObRawExpr*> &predicates,
  //                                  ObOptDSJoinParam &ds_join_param);

private:
  DISALLOW_COPY_AND_ASSIGN(ObOptSelectivity);
};
}
}

#endif
