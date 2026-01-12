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
class ObLakeColumnStat;
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
class AccessPath;
struct ColumnItem;
struct RangeExprs;
struct ObExprSelPair;

class ObEstCorrelationModel
{
public:
  static ObEstCorrelationModel &get_correlation_model(ObEstCorrelationType type);

  virtual double combine_filters_selectivity(ObIArray<double> &selectivities) const = 0;

  virtual double combine_ndvs(double rows, ObIArray<double> &ndvs) const = 0;

  virtual bool is_independent() const = 0;

protected:
  ObEstCorrelationModel() {}
  virtual ~ObEstCorrelationModel() = default;

private:
  DISALLOW_COPY_AND_ASSIGN(ObEstCorrelationModel);
};

class ObIndependentModel : public ObEstCorrelationModel
{
public:
  static ObEstCorrelationModel& get_model();

  virtual double combine_filters_selectivity(ObIArray<double> &selectivities) const override;

  virtual double combine_ndvs(double rows, ObIArray<double> &ndvs) const override;

  virtual bool is_independent() const override { return true; };

protected:
  ObIndependentModel() {}
  virtual ~ObIndependentModel() = default;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIndependentModel);
};

class ObPartialCorrelationModel : public ObEstCorrelationModel
{
public:
  static ObEstCorrelationModel& get_model();

  virtual double combine_filters_selectivity(ObIArray<double> &selectivities) const override;

  virtual double combine_ndvs(double rows, ObIArray<double> &ndvs) const override;

  virtual bool is_independent() const { return false; }

protected:
  ObPartialCorrelationModel() {}
  virtual ~ObPartialCorrelationModel() = default;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartialCorrelationModel);
};

class ObFullCorrelationModel : public ObEstCorrelationModel
{
public:
  static ObEstCorrelationModel& get_model();

  virtual double combine_filters_selectivity(ObIArray<double> &selectivities) const override;

  virtual double combine_ndvs(double rows, ObIArray<double> &ndvs) const override;

  virtual bool is_independent() const { return false; }

protected:
  ObFullCorrelationModel() {}
  virtual ~ObFullCorrelationModel() = default;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFullCorrelationModel);
};

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
    current_rows_(-1.0),
    ambient_card_(NULL),
    assumption_type_(UNKNOWN_JOIN)
  { }

  struct ExprDeduceInfo {
    ExprDeduceInfo() {}
    /*
     * antecedent exprs deduce consequence exprs
     * e.g. antecedent `(c1,c2) in ((1,1), (2,2))`
     *      =>
     *      consequence `c1 in (1,2)`, `c2 in (1,2)`
     */
    ObSEArray<ObRawExpr *, 1> antecedent_;
    ObSEArray<ObRawExpr *, 1> consequence_;

    DISABLE_COPY_ASSIGN(ExprDeduceInfo);
    void reuse() {
      antecedent_.reuse();
      consequence_.reuse();
    }

    int assign(const ExprDeduceInfo &other);
    int add_antecedent(ObIArray<ObRawExpr *> &exprs);
    int add_consequence(ObIArray<ObRawExpr *> &exprs);

    TO_STRING_KV(K_(antecedent), K_(consequence));
  };

  ObOptimizerContext &get_opt_ctx() const { return const_cast<ObOptimizerContext &>(opt_ctx_); }
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
  double get_left_row_count() const { return row_count_1_; }
  double get_right_row_count() const { return row_count_2_; }
  
  double get_current_rows() const { return current_rows_; }
  void set_current_rows(const double current_rows) { current_rows_ = current_rows; }

  const ObEstCorrelationModel &get_correlation_model() const
  {
    return ObEstCorrelationModel::get_correlation_model(opt_ctx_.get_correlation_type());
  }

  uint64_t get_compat_version() const {
    return OB_ISNULL(opt_ctx_.get_query_ctx()) ? 0 :
           opt_ctx_.get_query_ctx()->optimizer_features_enable_version_;
  }

  template<typename... Args>
  bool check_opt_compat_version(Args... args) const
  {
    return OB_ISNULL(get_opt_ctx().get_query_ctx()) ? false :
           get_opt_ctx().get_query_ctx()->check_opt_compat_version(args...);
  }

  void set_ambient_card(const ObIArray<double> *ambient_card) { ambient_card_ = ambient_card; }
  const ObIArray<double> *get_ambient_card() const { return ambient_card_; }
  int get_ambient_card(const uint64_t table_id, double &table_ambient_card) const;
  int get_ambient_card(const ObRelIds &rel_ids, double &table_ambient_card) const;
  void set_assumption_type(ObJoinType type) { assumption_type_ = type; }
  ObJoinType get_assumption_type() const {
    return UNKNOWN_JOIN == assumption_type_ ? join_type_ : assumption_type_;
  }
  const ObIArray<ExprDeduceInfo> &get_deduce_infos() const { return deduce_infos_; }

  void init_op_ctx(const EqualSets *equal_sets, const double current_rows,
                   const ObIArray<double> *ambient_card = NULL)
  {
    join_type_ = UNKNOWN_JOIN;
    left_rel_ids_ = NULL;
    right_rel_ids_ = NULL;
    equal_sets_ = equal_sets;
    current_rows_ = current_rows;
    ambient_card_ = ambient_card;
    deduce_infos_.reuse();
  }

  // child should be in the same query block
  void init_op_ctx(ObLogicalOperator *child);

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
    ambient_card_ = NULL;
    deduce_infos_.reuse();
  }

  void clear()
  {
    join_type_ = UNKNOWN_JOIN;
    left_rel_ids_ = NULL;
    right_rel_ids_ = NULL;
    equal_sets_ = NULL;
    current_rows_ = -1;
    ambient_card_ = NULL;
    deduce_infos_.reuse();
  }

  void init_row_count(const double row_count1, const double row_count2)
  {
    join_type_ = UNKNOWN_JOIN;
    left_rel_ids_ = NULL;
    right_rel_ids_ = NULL;
    row_count_1_ = row_count1;
    row_count_2_ = row_count2;
    ambient_card_ = NULL;
  }

  int init_deduce_infos(AccessPath *path);

  TO_STRING_KV(KP_(stmt), KP_(equal_sets), K_(join_type), KPC_(left_rel_ids), KPC_(right_rel_ids),
               K_(row_count_1), K_(row_count_2), K_(current_rows), KPC_(ambient_card), K_(deduce_infos));

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
  const ObIArray<double> *ambient_card_;

  /**
   * The join type which determines the estimation assumption.
   * Used to calculate the selectivity of ambient card.
  */
  ObJoinType assumption_type_;
  ObSEArray<ExprDeduceInfo, 1, common::ModulePageAllocator, true> deduce_infos_;
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
    min_max_inited_(false),
    cg_macro_blk_cnt_(0),
    cg_micro_blk_cnt_(0),
    cg_skip_rate_(0.0),
    base_ndv_(-1.0)
  {
    min_val_.set_min_value();
    max_val_.set_max_value();
  }
  int assign(const OptColumnMeta &other);
  void init(const uint64_t column_id,
            const double ndv,
            const double num_null,
            const double avg_len,
            const int64_t cg_macro_blk_cnt = 0,
            const int64_t cg_micro_blk_cnt = 0,
            const double cg_skip_rate = 0.0);

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
  int64_t get_cg_macro_blk_cnt() const { return cg_macro_blk_cnt_; }
  void set_cg_macro_blk_cnt(const int64_t cnt) { cg_macro_blk_cnt_ = cnt; }
  int64_t get_cg_micro_blk_cnt() const { return cg_micro_blk_cnt_; }
  void set_cg_micro_blk_cnt(const int64_t cnt) { cg_micro_blk_cnt_ = cnt; }
  double get_cg_skip_rate() const { return cg_skip_rate_; }
  void set_cg_skip_rate(const double skip_rate) { cg_skip_rate_ = skip_rate; }


  void set_default_meta(double rows)
  {
    ndv_ = std::min(rows, std::max(100.0, rows / 100.0));
    base_ndv_ = ndv_;
    num_null_ = rows * EST_DEF_COL_NULL_RATIO;
  }
  double get_base_ndv() const { return base_ndv_; }
  void set_base_ndv(double ndv) { base_ndv_ = ndv; }

  TO_STRING_KV(K_(column_id), K_(ndv), K_(num_null), K_(avg_len), K_(hist_scale),
               K_(min_val), K_(max_val) , K_(min_max_inited), K_(cg_macro_blk_cnt),
               K_(cg_micro_blk_cnt), K_(cg_skip_rate), K_(base_ndv));
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
  int64_t cg_macro_blk_cnt_;
  int64_t cg_micro_blk_cnt_;
  double cg_skip_rate_;
  double base_ndv_;
};

enum OptTableStatType {
  DEFAULT_TABLE_STAT = 0,    //default table stat.
  OPT_TABLE_STAT,            //optimizer gather table stat.
  OPT_TABLE_GLOBAL_STAT,     //optimizer gather table global stat when no table part stat.
  DS_TABLE_STAT              //dynamic sampling table stat
};

class OptDynamicExprMeta
{
public:
  OptDynamicExprMeta(): avg_len_(0) {}
  void set_expr(const ObRawExpr *expr) { expr_ = expr; }
  const ObRawExpr *get_expr() const { return expr_; }
  void set_avg_len(double avg_len) { avg_len_ = avg_len; }
  double get_avg_len() const { return avg_len_; }

  int assign(const OptDynamicExprMeta &other)
  {
    int ret = OB_SUCCESS;
    expr_ = other.expr_;
    avg_len_ = other.avg_len_;
    return ret;
  }

  TO_STRING_KV(KP_(expr), KPC_(expr), K_(avg_len));
private:
  const ObRawExpr *expr_;
  double avg_len_;
  DISALLOW_COPY_AND_ASSIGN(OptDynamicExprMeta);
};

class OptTableMeta
{
public:
  OptTableMeta() :
    table_id_(OB_INVALID_ID),
    ref_table_id_(OB_INVALID_ID),
    rows_(0),
    stat_type_(OptTableStatType::DEFAULT_TABLE_STAT),
    last_analyzed_(0),
    stat_locked_(false),
    all_used_parts_(),
    all_used_tablets_(),
    pk_ids_(),
    column_metas_(),
    ds_level_(ObDynamicSamplingLevel::NO_DYNAMIC_SAMPLING),
    scale_ratio_(1.0),
    distinct_rows_(0.0),
    table_partition_info_(NULL),
    base_meta_info_(NULL),
    real_rows_(-1.0),
    stale_stats_(false)
  {}
  int assign(const OptTableMeta &other);

  int init(const uint64_t table_id,
           const uint64_t ref_table_id,
           const int64_t rows,
           const OptTableStatType stat_type,
           const int64_t micro_block_count,
           ObSqlSchemaGuard &schema_guard,
           common::ObIArray<int64_t> &all_used_part_id,
           common::ObIArray<ObTabletID> &all_used_tablets,
           common::ObIArray<uint64_t> &column_ids,
           common::ObIArray<int64_t> &stat_part_id,
           common::ObIArray<int64_t> &hist_part_id,
           const double scale_ratio,
           const OptSelectivityCtx &ctx,
           const ObTablePartitionInfo *table_partition_info,
           const ObTableMetaInfo *base_meta_info);
  int init_lake_table(const uint64_t table_id,
                      const uint64_t ref_table_id,
                      const int64_t rows,
                      const int64_t last_analyzed,
                      const OptTableStatType stat_type,
                      ObIArray<uint64_t> &column_ids,
                      const ObIArray<common::ObLakeColumnStat*> &column_stats,
                      const OptSelectivityCtx &ctx,
                      const ObTableMetaInfo *base_meta_info);

  // int update_stat(const double rows, const bool can_reduce, const bool can_enlarge);
  int add_column_meta_no_dup(const ObIArray<uint64_t> &column_id, const OptSelectivityCtx &ctx);

  const OptColumnMeta* get_column_meta(const uint64_t column_id) const;
  OptColumnMeta* get_column_meta(const uint64_t column_id);
  uint64_t get_table_id() const { return table_id_; }
  void set_table_id(const uint64_t &table_id) { table_id_ = table_id; }
  uint64_t get_ref_table_id() const { return ref_table_id_; }
  void set_ref_table_id(const uint64_t &ref_table_id) { ref_table_id_ = ref_table_id; }
  double get_rows() const { return rows_; }
  void set_rows(const double rows) { rows_ = rows; }
  double get_base_rows() const { return base_rows_; }
  void set_base_rows(const double rows) { base_rows_ = rows; }
  int64_t get_version() const { return last_analyzed_; }
  void set_version(const int64_t version) { last_analyzed_ = version; }
  int64_t get_micro_block_count() const { return micro_block_count_; }
  const common::ObIArray<int64_t>& get_all_used_parts() const { return all_used_parts_; }
  common::ObIArray<int64_t> &get_all_used_parts() { return all_used_parts_; }
  const common::ObIArray<ObTabletID>& get_all_used_tablets() const { return all_used_tablets_; }
  common::ObIArray<ObTabletID> &get_all_used_tablets() { return all_used_tablets_; }
  const common::ObIArray<uint64_t>& get_pkey_ids() const { return pk_ids_; }
  common::ObIArray<OptColumnMeta>& get_column_metas() { return column_metas_; }
  const common::ObIArray<int64_t>& get_stat_parts() const { return stat_parts_; }
  const common::ObIArray<int64_t>& get_hist_parts() const { return hist_parts_; }
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
  bool is_stat_locked() const { return stat_locked_; }
  void set_stat_locked(bool locked) { stat_locked_ = locked; }
  double get_distinct_rows() const { return distinct_rows_; }
  void set_distinct_rows(double rows) { distinct_rows_ = rows; }
  bool is_opt_stat_expired() const { return stale_stats_; }
  void set_stale_stats(bool stale_stats) { stale_stats_ = stale_stats; }
  const ObTableMetaInfo *get_base_meta_info() const { return base_meta_info_; }


  // The ratio of the increase in the number of rows in the system table compared to the number of rows in the statistics.
  int get_increase_rows_ratio(ObOptimizerContext &ctx, double &increase_rows_ratio) const;
  void clear_base_table_info() {
    table_partition_info_ = NULL;
    base_meta_info_ = NULL;
    real_rows_ = -1.0;
  }

  static int refine_column_stat(const ObGlobalColumnStat &stat,
                                double rows,
                                OptColumnMeta &col_meta);

  TO_STRING_KV(K_(table_id), K_(ref_table_id), K_(rows), K_(stat_type), K_(ds_level),
               K_(all_used_parts), K_(all_used_tablets), K_(pk_ids), K_(column_metas),
               K_(scale_ratio), K_(stat_locked), K_(distinct_rows), K_(real_rows));

private:
  // TODO: make the column_metas to Array<OptColumnMeta*>
  int init_column_meta(const OptSelectivityCtx &ctx,
                       const ObIArray<uint64_t> &column_ids,
                       ObIArray<OptColumnMeta> &column_metas);

  int init_lake_column_meta(const OptSelectivityCtx &ctx,
                            const ObIArray<uint64_t> &column_ids,
                            const ObIArray<common::ObLakeColumnStat*> &column_stats,
                            ObIArray<OptColumnMeta> &column_metas);

  int refine_column_meta(const OptSelectivityCtx &ctx,
                         const uint64_t column_id,
                         const ObGlobalColumnStat &stat,
                         OptColumnMeta &col_meta);

private :
  uint64_t table_id_;
  uint64_t ref_table_id_;
  double rows_;
  OptTableStatType stat_type_;
  int64_t last_analyzed_;
  bool stat_locked_;

  int64_t micro_block_count_;

  ObSEArray<int64_t, 64, common::ModulePageAllocator, true> all_used_parts_;
  ObSEArray<ObTabletID, 64, common::ModulePageAllocator, true> all_used_tablets_;
  ObSEArray<uint64_t, 4, common::ModulePageAllocator, true> pk_ids_;
  ObSEArray<OptColumnMeta, 32, common::ModulePageAllocator, true> column_metas_;
  int64_t ds_level_;//dynamic sampling level
  ObSEArray<int64_t, 1, common::ModulePageAllocator, true> stat_parts_;
  ObSEArray<int64_t, 1, common::ModulePageAllocator, true> hist_parts_;
  double scale_ratio_;

  // only valid for child stmt meta of set distinct stmt
  double distinct_rows_;

  // only for base table
  const ObTablePartitionInfo *table_partition_info_;
  const ObTableMetaInfo *base_meta_info_;
  double real_rows_;
  //mark stat is expired
  bool stale_stats_;
  // rows without filters
  double base_rows_;
};

struct OptSelectivityDSParam {
  OptSelectivityDSParam() :
    table_meta_(NULL),
    quals_()
  {}
  TO_STRING_KV(KPC(table_meta_),
               K(quals_));
  DISABLE_COPY_ASSIGN(OptSelectivityDSParam);
  int assign(const OptSelectivityDSParam &other) {
    table_meta_ = other.table_meta_;
    return quals_.assign(other.quals_);
  }

  const OptTableMeta *table_meta_;
  ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> quals_;
};

enum class DistinctEstType
{
  BASE,     // estimate the ndv without any filters
  CURRENT,  // estimate current ndv according to current rows and ambient cardinality
};

class OptTableMetas
{
public:
  int copy_table_meta_info(const OptTableMeta &src_meta, OptTableMeta *&dst_meta);
  int copy_table_meta_info(const OptTableMetas &table_metas, const uint64_t table_id);

  int add_base_table_meta_info(OptSelectivityCtx &ctx,
                               const uint64_t table_id,
                               const uint64_t ref_table_id,
                               const int64_t rows,
                               const int64_t micro_block_count,
                               common::ObIArray<int64_t> &all_used_part_id,
                               common::ObIArray<ObTabletID> &all_used_tablets,
                               common::ObIArray<uint64_t> &column_ids,
                               const OptTableStatType stat_type,
                               common::ObIArray<int64_t> &stat_part_id,
                               common::ObIArray<int64_t> &hist_part_id,
                               const double scale_ratio,
                               int64_t last_analyzed,
                               bool is_stat_locked,
                               const ObTablePartitionInfo *table_partition_info,
                               const ObTableMetaInfo *base_meta_info,
                               bool stale_stats);

  int add_lake_table_meta_info(OptSelectivityCtx &ctx,
                               const uint64_t table_id,
                               const uint64_t ref_table_id,
                               const int64_t rows,
                               const int64_t last_analyzed,
                               ObIArray<uint64_t> &column_ids,
                               const ObIArray<ObLakeColumnStat*> &column_stats,
                               const OptTableStatType stat_type,
                               const ObTableMetaInfo *base_meta_info);

  int add_set_child_stmt_meta_info(const ObSelectStmt *parent_stmt,
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
  int add_values_table_meta_info(const ObDMLStmt *stmt,
                                 const uint64_t table_id,
                                 const OptSelectivityCtx &ctx,
                                 ObValuesTableDef *table_def);
  int get_set_stmt_output_statistics(const ObSelectStmt &stmt,
                                     const OptTableMetas &child_table_metas,
                                     const int64_t idx,
                                     double &ndv,
                                     double &num_null,
                                     double &avg_len);
  int get_set_stmt_output_ndv(const ObSelectStmt &stmt,
                              const OptTableMetas &child_table_metas,
                              double &ndv);

  common::ObIArray<OptTableMeta>& get_table_metas() { return table_metas_; }
  const common::ObIArray<OptTableMeta>& get_table_metas() const { return table_metas_; }
  const OptTableMeta* get_table_meta_by_table_id(const uint64_t table_id) const;
  OptTableMeta* get_table_meta_by_table_id(const uint64_t table_id);
  const OptColumnMeta* get_column_meta_by_table_id(const uint64_t table_id,
                                                   const uint64_t column_id) const;
  const OptDynamicExprMeta* get_dynamic_expr_meta(const ObRawExpr *expr) const;
  int add_dynamic_expr_meta(const OptDynamicExprMeta &dynamic_expr_meta) {
    return dynamic_expr_metas_.push_back(dynamic_expr_meta);
  }
  const ObIArray<OptDynamicExprMeta> &get_dynamic_expr_metas() const { return dynamic_expr_metas_; }

  double get_rows(const uint64_t table_id) const;
  double get_base_rows(const uint64_t table_id) const;
  TO_STRING_KV(K_(table_metas), K_(dynamic_expr_metas));
private:
  common::ObSEArray<OptTableMeta, 16, common::ModulePageAllocator, true> table_metas_;
  common::ObSEArray<OptDynamicExprMeta, 4, common::ModulePageAllocator, true> dynamic_expr_metas_;
};

struct OptSelInfo
{
  OptSelInfo() :
    column_id_(0),
    selectivity_(1.0),
    equal_count_(0),
    range_selectivity_(1.0),
    has_range_exprs_(false)
  {
    min_.set_min_value();
    max_.set_max_value();
  }

  TO_STRING_KV(K_(column_id), K_(selectivity), K_(equal_count),
               K_(range_selectivity), K_(has_range_exprs));
  DISABLE_COPY_ASSIGN(OptSelInfo);
  int assign(const OptSelInfo &other)
  {
    column_id_ = other.column_id_;
    selectivity_ = other.selectivity_;
    equal_count_ = other.equal_count_;
    range_selectivity_ = other.range_selectivity_;
    has_range_exprs_ = other.has_range_exprs_;
    min_ = other.min_;
    max_ = other.max_;
    return quals_.assign(other.quals_);
  }

  uint64_t column_id_;
  double selectivity_;
  uint64_t equal_count_;
  double range_selectivity_;
  bool has_range_exprs_;
  ObObj min_;
  ObObj max_;
  ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> quals_;
};

class ObSelEstimator;

struct OptDistinctHelper
{
public:
  OptDistinctHelper() {}

  TO_STRING_KV(K_(rel_id), K_(exprs));
  DISABLE_COPY_ASSIGN(OptDistinctHelper);
  int assign(const OptDistinctHelper &other) {
    rel_id_ = other.rel_id_;
    return exprs_.assign(other.exprs_);
  }

  ObRelIds rel_id_;
  ObSEArray<ObRawExpr *, 2> exprs_;
};

class ObOptSelectivity
{
public:
  static int calculate_selectivity(const OptTableMetas &table_metas,
                                   const OptSelectivityCtx &ctx,
                                   ObIArray<ObSelEstimator *> &sel_estimators,
                                   double &selectivity,
                                   common::ObIArray<ObExprSelPair> &all_predicate_sel,
                                   bool record_range_sel = false);

  static int calculate_selectivity(const OptTableMetas &table_metas,
                                   const OptSelectivityCtx &ctx,
                                   const common::ObIArray<ObRawExpr*> &quals,
                                   double &selectivity,
                                   common::ObIArray<ObExprSelPair> &all_predicate_sel);

  static int calculate_conditional_selectivity(const OptTableMetas &table_metas,
                                               const OptSelectivityCtx &ctx,
                                               common::ObIArray<ObRawExpr *> &total_filters,
                                               const common::ObIArray<ObRawExpr *> &append_filters,
                                               double &total_sel,
                                               double &conditional_sel,
                                               ObIArray<ObExprSelPair> &all_predicate_sel);

  static int calculate_join_selectivity(const OptTableMetas &table_metas,
                                        const OptSelectivityCtx &ctx,
                                        const common::ObIArray<ObRawExpr*> &quals,
                                        double &selectivity,
                                        common::ObIArray<ObExprSelPair> &all_predicate_sel,
                                        bool is_outerjoin_filter = false);

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

  static int calculate_table_ambient_cardinality(const OptTableMetas &table_metas,
                                                 const OptSelectivityCtx &ctx,
                                                 const ObRelIds &rel_id,
                                                 const double cur_rows,
                                                 double &table_ambient_card,
                                                 DistinctEstType est_type);

  static int calculate_distinct_in_single_table(const OptTableMetas &table_metas,
                                                const OptSelectivityCtx &ctx,
                                                const ObRelIds &rel_ids,
                                                const common::ObIArray<ObRawExpr*>& exprs,
                                                const double cur_rows,
                                                DistinctEstType est_type,
                                                double &rows);

  static int remove_dummy_distinct_exprs(ObIArray<OptDistinctHelper> &helpers,
                                         ObIArray<ObRawExpr *> &exprs);

  static int check_expr_in_distinct_helper(const ObRawExpr *expr,
                                           const ObIArray<OptDistinctHelper> &helpers,
                                           bool &is_dummy_expr);

  // @brief 计算一组变量的distinct
  static int calculate_distinct(const OptTableMetas &table_metas,
                                const OptSelectivityCtx &ctx,
                                const common::ObIArray<ObRawExpr*>& exprs,
                                const double origin_rows,
                                double &rows,
                                const bool need_refine = true,
                                DistinctEstType est_type = DistinctEstType::CURRENT);

  static int calculate_distinct(const OptTableMetas &table_metas,
                                const OptSelectivityCtx &ctx,
                                const ObRawExpr& expr,
                                const double origin_rows,
                                double &rows,
                                const bool need_refine = true,
                                DistinctEstType est_type = DistinctEstType::CURRENT);

  static double combine_two_ndvs(double ambient_card, double ndv1, double ndv2);

  static double combine_ndvs(double ambient_card, ObIArray<double> &ndvs);

  // ndv 按照行数进行缩放.
  static double scale_distinct(double selected_rows, double rows, double ndv);

  static inline double revise_between_0_1(double num)
  {
    bool ignore_inf_error = OB_SUCCESS == (OB_E(EventTable::EN_CHECK_OPERATOR_OUTPUT_ROWS) OB_SUCCESS);
    return std::isfinite(num) ? (num < 0 ? 0 : (num > 1 ? 1 : num)) :
           (ignore_inf_error ? 1.0 : num);
  }

  static inline double revise_ndv(double ndv) { return ndv < 1.0 ? 1.0 : ndv; }

  static int get_column_range_sel(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  const ObColumnRefRawExpr &col_expr,
                                  const ObRawExpr &qual,
                                  const bool need_out_of_bounds,
                                  double &selectivity);

  //param:As some expr, query range can't calc range, then range will be (min, max).
  //no_whole_range representing that expr_sel should not use whole range to calc sel.
  //if OB_INVALID_ID == partition_id, then use part_id(0)
  static int get_column_range_sel(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  const ObColumnRefRawExpr &col_expr,
                                  const ObIArray<ObRawExpr* > &quals,
                                  const bool need_out_of_bounds,
                                  double &selectivity);

  static int get_column_range_min_max(const OptSelectivityCtx &ctx,
                                      const ObColumnRefRawExpr *col_expr,
                                      const ObIArray<ObRawExpr* > &quals,
                                      ObObj &obj_min,
                                      ObObj &obj_max);

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

  static int refine_out_of_bounds_sel(const OptTableMetas &table_metas,
                                      const OptSelectivityCtx &ctx,
                                      const ObColumnRefRawExpr &col_expr,
                                      const ObQueryRangeArray &ranges,
                                      const ObObj &min_val,
                                      const ObObj &max_val,
                                      double &selectivity);

  static int get_single_range_out_of_bounds_sel(const ObObj &min_val,
                                                const ObObj &max_val,
                                                const ObObj &start_val,
                                                const ObObj &end_val,
                                                double &selectivity);

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
                                   double *avg_len_ptr,
                                   double *row_count_ptr,
                                   DistinctEstType est_type = DistinctEstType::CURRENT);

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
                                        double &avg_len,
                                        double &base_ndv);

  static int get_var_basic_default(double &row_count,
                                   double &ndv,
                                   double &null_num,
                                   double &avg_len,
                                   double &base_ndv);

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

  static int get_range_sel_by_histogram(const OptSelectivityCtx &ctx,
                                        const common::ObHistogram &histogram,
                                        const ObQueryRangeArray &ranges,
                                        bool no_whole_range,
                                        const double sample_size_scale,
                                        double &selectivity);

  static int get_less_pred_sel(const OptSelectivityCtx &ctx,
                               const ObHistogram &histogram,
                               const ObObj &maxv,
                               const bool inclusive,
                               double &density);

  static int get_greater_pred_sel(const OptSelectivityCtx &ctx,
                                  const ObHistogram &histogram,
                                  const ObObj &minv,
                                  const bool inclusive,
                                  double &density);

  static int get_range_pred_sel(const OptSelectivityCtx &ctx,
                                const ObHistogram &histogram,
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
                                    ObIAllocator &alloc,
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
                                        DistinctEstType est_type,
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
   * 检查一组expr是否包含所在表的主键
   */
  static int is_columns_contain_pkey(const OptTableMetas &table_metas,
                                     const ObIArray<ObRawExpr *> &exprs,
                                     bool &is_pkey,
                                     bool &is_union_pkey,
                                     uint64_t *table_id_ptr = NULL);

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

  static int classify_quals_deprecated(const OptSelectivityCtx &ctx,
                            const ObIArray<ObRawExpr*> &quals,
                            ObIArray<ObExprSelPair> &all_predicate_sel,
                            ObIArray<OptSelInfo> &column_sel_infos);

  static int classify_quals(const OptTableMetas &table_metas,
                            const OptSelectivityCtx &ctx,
                            const ObIArray<ObRawExpr*> &quals,
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
                                const ObJoinType join_type,
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


  static int get_column_min_max(ObRawExpr *expr, OptSelInfo &sel_info);

  static int calculate_special_ndv(const OptTableMetas &table_meta,
                                  const ObRawExpr* expr,
                                  const OptSelectivityCtx &ctx,
                                  double &special_ndv,
                                  const double origin_rows,
                                  DistinctEstType est_type);
  static int calculate_winfunc_ndv(const OptTableMetas &table_meta,
                                  const ObRawExpr* expr,
                                  const OptSelectivityCtx &ctx,
                                  double &special_ndv,
                                  const double origin_rows);
  static int calculate_expr_ndv(const ObIArray<ObRawExpr*>& exprs,
                                ObIArray<double>& expr_ndv,
                                const OptTableMetas &table_metas,
                                const OptSelectivityCtx &ctx,
                                const double origin_rows,
                                DistinctEstType est_type);
  static int check_is_special_distinct_expr(const OptSelectivityCtx &ctx,
                                            const ObRawExpr *expr,
                                            bool &is_special);
  static int classify_exprs(const OptSelectivityCtx &ctx,
                            const ObIArray<ObRawExpr*>& exprs,
                            ObIArray<OptDistinctHelper> &helpers,
                            ObIArray<ObRawExpr*>& special_exprs);
  static int classify_exprs(const OptSelectivityCtx &ctx,
                            ObRawExpr *expr,
                            ObIArray<OptDistinctHelper> &helpers,
                            ObIArray<ObRawExpr*>& special_exprs);
  static int add_expr_to_distinct_helper(ObIArray<OptDistinctHelper> &helpers,
                                         const ObRelIds &rel_id,
                                         ObRawExpr *expr);

  static int remove_ignorable_func_for_est_sel(const ObRawExpr *&expr);
  static int remove_ignorable_func_for_est_sel(ObRawExpr *&expr);
  static double get_set_stmt_output_count(double count1, double count2, ObSelectStmt::SetOperator set_type);

  static int calculate_expr_avg_len(const OptTableMetas &table_metas,
                                    const OptSelectivityCtx &ctx,
                                    const ObRawExpr *expr,
                                    double &avg_len);
  static int get_column_avg_len(const OptTableMetas &table_metas,
                                const OptSelectivityCtx &ctx,
                                const ObRawExpr *expr,
                                double &avg_len);
  static int calculate_substrb_info(const OptTableMetas &table_metas,
                                    const OptSelectivityCtx &ctx,
                                    const ObRawExpr *str_expr,
                                    const double substrb_len,
                                    const double cur_rows,
                                    double &ndv,
                                    double &nns,
                                    DistinctEstType est_type = DistinctEstType::CURRENT);
  static int calculate_expr_nns(const OptTableMetas &table_metas,
                                const OptSelectivityCtx &ctx,
                                const ObRawExpr *expr,
                                double &nns);
  static int calc_expr_min_max(const OptTableMetas &table_metas,
                               const OptSelectivityCtx &ctx,
                               const ObRawExpr *expr,
                               ObObj &min_value,
                               ObObj &max_value);
  static int calc_year_min_max(const OptTableMetas &table_metas,
                               const OptSelectivityCtx &ctx,
                               const ObRawExpr *expr,
                               int64_t &min_year,
                               int64_t &max_year,
                               bool &use_default);
  static int calc_const_numeric_value(const OptSelectivityCtx &ctx,
                                      const ObRawExpr *expr,
                                      double &value,
                                      bool &succ);
  static int convert_obj_to_expr_type(const OptSelectivityCtx &ctx,
                                      const ObRawExpr *expr,
                                      ObCastMode cast_mode,
                                      ObObj &obj);
  static bool is_dense_time_expr_type(ObItemType type)
  {
    return T_FUN_SYS_YEAR == type ||
           T_FUN_SYS_DAY == type ||
           T_FUN_SYS_DAY_OF_MONTH == type ||
           T_FUN_SYS_MONTH == type ||
           T_FUN_SYS_DAY_OF_YEAR == type ||
           T_FUN_SYS_WEEK_OF_YEAR == type ||
           T_FUN_SYS_WEEKDAY_OF_DATE == type ||
           T_FUN_SYS_YEARWEEK_OF_DATE == type ||
           T_FUN_SYS_DAY_OF_WEEK == type ||
           T_FUN_SYS_WEEK == type ||
           T_FUN_SYS_QUARTER == type ||
           T_FUN_SYS_HOUR == type ||
           T_FUN_SYS_MINUTE == type ||
           T_FUN_SYS_SECOND == type;
  }

  static double calc_equal_filter_sel(const OptSelectivityCtx &ctx,
                                      bool is_same_expr,
                                      ObItemType op_type,
                                      double left_ndv,
                                      double right_ndv,
                                      double left_nns,
                                      double right_nns);

  static double calc_equal_join_sel(const OptSelectivityCtx &ctx,
                                    ObItemType op_type,
                                    double left_ndv,
                                    double right_ndv,
                                    double left_nns,
                                    double right_nns,
                                    double left_base_ndv = -1.0,
                                    double right_base_ndv = -1.0);

  static int calc_expr_basic_info(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  const ObRawExpr *expr,
                                  double *ndv,
                                  double *nns = nullptr,
                                  double *base_ndv = nullptr);

private:
  DISALLOW_COPY_AND_ASSIGN(ObOptSelectivity);
};

class ObHistSelHelper
{
public:
  ObHistSelHelper():
    hist_scale_(-1),
    density_(0.0),
    column_expr_(NULL),
    is_valid_(false),
    table_meta_(NULL) {}

  virtual ~ObHistSelHelper() = default;

  int init(const OptTableMetas &table_metas,
           const OptSelectivityCtx &ctx,
           const ObColumnRefRawExpr &col);
  int get_sel(const OptSelectivityCtx &ctx,
              double &sel);

  bool is_valid() const { return is_valid_; }
  ObIArray<ObOptColumnStatHandle> &get_handlers() { return handlers_; }
  ObIArray<double> &get_part_rows() { return part_rows_; }
  double get_hist_scale() { return hist_scale_; }
  const ObColumnRefRawExpr *get_column_expr() { return column_expr_; }
  double get_density() const { return density_; }

protected:
  virtual int inner_get_sel(const OptSelectivityCtx &ctx,
                            const ObHistogram &histogram,
                            double &sel,
                            bool &is_rare_value)
  {
    return OB_ERR_UNEXPECTED;
  }

protected:
  ObSEArray<ObOptColumnStatHandle, 4> handlers_;
  ObSEArray<double, 4> part_rows_;
  double hist_scale_;
  double density_;
  const ObColumnRefRawExpr *column_expr_;
  bool is_valid_;
  const OptTableMeta *table_meta_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObHistSelHelper);
};

class ObHistEqualSelHelper: public ObHistSelHelper
{
public:
  ObHistEqualSelHelper() :
    is_neq_(false) {}
  virtual ~ObHistEqualSelHelper() = default;

  int set_compare_value(const OptSelectivityCtx &ctx,
                        const ObRawExpr *const_expr,
                        bool &is_valid) {
    return ObOptSelectivity::get_compare_value(
              ctx, column_expr_, const_expr, compare_value_, is_valid);
  }
  void set_compare_value(const ObObj &value) {
    compare_value_ = value;
  }
  void set_not_eq(bool neq) { is_neq_ = neq; }
protected:
  virtual int inner_get_sel(const OptSelectivityCtx &ctx,
                            const ObHistogram &histogram,
                            double &sel,
                            bool &is_rare_value) override;
  int refine_out_of_bounds_sel(const OptSelectivityCtx &ctx,
                               const ObObj &value,
                               double &sel,
                               bool &is_rare_value);
private:
  ObObj compare_value_;
  bool is_neq_;

  DISALLOW_COPY_AND_ASSIGN(ObHistEqualSelHelper);
};

class ObHistRangeSelHelper: public ObHistSelHelper
{
public:
  ObHistRangeSelHelper() = default;
  virtual ~ObHistRangeSelHelper() = default;

  int init(const OptTableMetas &table_metas,
           const OptSelectivityCtx &ctx,
           const ObColumnRefRawExpr &col);

  int set_ranges(const ObQueryRangeArray &ranges) {
    return ranges_.assign(ranges);
  }

  ObObj &get_min_value() { return hist_min_value_; }
  ObObj &get_max_value() { return hist_max_value_; }
protected:
  virtual int inner_get_sel(const OptSelectivityCtx &ctx,
                            const ObHistogram &histogram,
                            double &sel,
                            bool &is_rare_value) override;
private:
  ObQueryRangeArray ranges_;
  ObObj hist_min_value_;
  ObObj hist_max_value_;

  DISALLOW_COPY_AND_ASSIGN(ObHistRangeSelHelper);
};

}
}

#endif
