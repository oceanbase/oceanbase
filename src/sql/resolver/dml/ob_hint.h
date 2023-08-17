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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_HINT_
#define OCEANBASE_SQL_RESOLVER_DML_OB_HINT_
#include "lib/string/ob_string.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/container/ob_se_array.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/optimizer/ob_log_operator_factory.h"

namespace oceanbase
{
namespace sql
{
struct PlanText;
struct TableItem;

enum ObHintMergePolicy
{
  HINT_DOMINATED_EQUAL,
  LEFT_HINT_DOMINATED,
  RIGHT_HINT_DOMINATED
};

enum ObPlanCachePolicy
{
  OB_USE_PLAN_CACHE_INVALID = 0,//policy not set
  OB_USE_PLAN_CACHE_NONE,//do not use plan cache
  OB_USE_PLAN_CACHE_DEFAULT,//use plan cache
};

struct ObMonitorHint
{
  ObMonitorHint(): id_(0), flags_(0) {};
  ~ObMonitorHint() = default;

  static const int8_t OB_MONITOR_TRACING  = 0x1 << 1;
  static const int8_t OB_MONITOR_STAT     = 0x1 << 2;

  uint64_t id_;
  uint64_t flags_;
  TO_STRING_KV(K_(id), K_(flags));
};

struct ObDopHint
{
  ObDopHint(): dfo_(0), dop_(0) {};
  ~ObDopHint() = default;
  uint64_t dfo_; // dfo name, like 10001, 20000
  uint64_t dop_;
  TO_STRING_KV(K_(dfo), K_(dop));
};

// hint relate to optimizer statistics gathering.
struct ObOptimizerStatisticsGatheringHint
{
  ObOptimizerStatisticsGatheringHint(): flags_(0) {};
  ~ObOptimizerStatisticsGatheringHint() = default;

  static const int8_t OB_APPEND_HINT = 0x1 << 1;
  static const int8_t OB_OPT_STATS_GATHER = 0x1 << 2; // OPTIMIZER_STATISTICS_GATHERING
  static const int8_t OB_NO_OPT_STATS_GATHER = 0x1 << 3; // NO_OPTIMIZER_STATISTICS_GATHERING

  uint64_t flags_;
  TO_STRING_KV(K_(flags));
  int print_osg_hint(PlanText &plan_text) const;
};

struct ObOptParamHint
{
  ObOptParamHint() {};

  #define OPT_PARAM_TYPE_DEF(DEF)         \
    DEF(INVALID_OPT_PARAM_TYPE, = 0)      \
    DEF(HIDDEN_COLUMN_VISIBLE,)           \
    DEF(ROWSETS_ENABLED,)                 \
    DEF(ROWSETS_MAX_ROWS,)                \
    DEF(DDL_EXECUTION_ID,)                \
    DEF(DDL_TASK_ID,)                     \
    DEF(ENABLE_NEWSORT,)                  \
    DEF(USE_PART_SORT_MGB,)               \
    DEF(USE_DEFAULT_OPT_STAT,)            \
    DEF(ENABLE_IN_RANGE_OPTIMIZATION,)    \
    DEF(XSOLAPI_GENERATE_WITH_CLAUSE,)   \

  DECLARE_ENUM(OptParamType, opt_param, OPT_PARAM_TYPE_DEF, static);

  static bool is_param_val_valid(const OptParamType param_type, const ObObj &val);
  int merge_opt_param_hint(const ObOptParamHint &other);
  int add_opt_param_hint(const ObString &param_name, const ObObj &val)
  { return add_opt_param_hint(get_opt_param_value(param_name), val);  }
  int add_opt_param_hint(const OptParamType param_type, const ObObj &val);
  int get_opt_param(const OptParamType param_type, ObObj &val) const;
  int has_enable_opt_param(const OptParamType param_type, bool &enabled) const;
  int print_opt_param_hint(PlanText &plan_text) const;
  int get_bool_opt_param(const OptParamType param_type, bool &val, bool& is_exists) const;
  // if the corresponding opt_param is specified, the `val` will be overwritten
  int get_bool_opt_param(const OptParamType param_type, bool &val) const;
  int get_integer_opt_param(const OptParamType param_type, int64_t &val) const;
  int has_opt_param(const OptParamType param_type, bool &has_hint) const;
  bool empty() const { return param_types_.empty();  }
  void reset();
  TO_STRING_KV(K_(param_types), K_(param_vals));
  common::ObSEArray<OptParamType, 1, common::ModulePageAllocator, true> param_types_;
  common::ObSEArray<ObObj, 1, common::ModulePageAllocator, true> param_vals_;
};

class ObDDLSchemaVersionHint;

struct ObGlobalHint {
  ObGlobalHint() { reset(); }
  void reset();
  int assign(const ObGlobalHint &other);

  static const common::ObConsistencyLevel UNSET_CONSISTENCY = common::INVALID_CONSISTENCY;
  static const int64_t UNSET_QUERY_TIMEOUT = -1;
  static const int64_t UNSET_MAX_CONCURRENT = -1;
  static const uint64_t UNSET_OPT_FEATURES_VERSION = 0;
  static const uint64_t MIN_OUTLINE_ENABLE_VERSION = CLUSTER_VERSION_4_0_0_0;
  static const uint64_t CURRENT_OUTLINE_ENABLE_VERSION = CLUSTER_VERSION_4_0_0_0;
  static const int64_t DEFAULT_PARALLEL = 1;
  static const int64_t UNSET_PARALLEL = 0;
  static const int64_t SET_ENABLE_AUTO_DOP = -1;
  static const int64_t SET_ENABLE_MANUAL_DOP = -2;
  static const int64_t UNSET_DYNAMIC_SAMPLING = -1;

  int merge_global_hint(const ObGlobalHint &other);
  int merge_monitor_hints(const ObIArray<ObMonitorHint> &monitoring_ids);
  int merge_dop_hint(uint64_t dfo, uint64_t dop);
  int merge_dop_hint(const ObIArray<ObDopHint> &dop_hints);
  void merge_query_timeout_hint(int64_t hint_time);
  void reset_query_timeout_hint() { query_timeout_ = -1; }
  void merge_dblink_info_hint(int64_t tx_id, int64_t tm_sessid);
  void reset_dblink_info_hint();
  void merge_max_concurrent_hint(int64_t max_concurrent);
  void merge_parallel_hint(int64_t parallel);
  void merge_parallel_dml_hint(ObPDMLOption pdml_option);
  void merge_param_option_hint(ObParamOption opt);
  void merge_topk_hint(int64_t precision, int64_t sharding_minimum_row_count);
  void merge_plan_cache_hint(ObPlanCachePolicy policy);
  void merge_log_level_hint(const ObString &log_level);
  void merge_read_consistency_hint(ObConsistencyLevel read_consistency, int64_t frozen_version);
  void merge_opt_features_version_hint(uint64_t opt_features_version);
  void merge_osg_hint(int8_t flag);
  void merge_dynamic_sampling_hint(int64_t dynamic_sampling);

  bool has_hint_exclude_concurrent() const;
  int print_global_hint(PlanText &plan_text) const;
  int print_monitoring_hints(PlanText &plan_text) const;

  ObPDMLOption get_pdml_option() const { return pdml_option_; }
  ObParamOption get_param_option() const { return param_option_; }
  int64_t get_dblink_tx_id_hint() const { return tx_id_; }
  int64_t get_dblink_tm_sessid_hint() const { return tm_sessid_; }
  int64_t get_parallel_degree() const { return parallel_ >= DEFAULT_PARALLEL ? parallel_ : UNSET_PARALLEL; }
  bool has_parallel_degree() const { return parallel_ >= DEFAULT_PARALLEL; }
  bool has_parallel_hint() const { return UNSET_PARALLEL != parallel_; }
  bool enable_auto_dop() const { return SET_ENABLE_AUTO_DOP == parallel_; }
  bool enable_manual_dop() const { return SET_ENABLE_MANUAL_DOP == parallel_; }
  bool is_topk_specified() const { return topk_precision_ > 0 || sharding_minimum_row_count_ > 0; }
  bool has_valid_opt_features_version() const { return is_valid_opt_features_version(opt_features_version_); }
  static bool is_valid_opt_features_version(uint64_t version)
  { return MIN_OUTLINE_ENABLE_VERSION <= version && CLUSTER_CURRENT_VERSION >= version; }
  bool disable_query_transform() const { return disable_transform_; }
  bool disable_cost_based_transform() const { return disable_cost_based_transform_; }
  inline bool has_dbms_stats_hint() const { return has_dbms_stats_hint_; }
  inline void set_dbms_stats() { has_dbms_stats_hint_ = true; }
  bool get_flashback_read_tx_uncommitted() const { return flashback_read_tx_uncommitted_; }
  void set_flashback_read_tx_uncommitted(bool v) { flashback_read_tx_uncommitted_ = v; }
  bool has_append() const {
    return (osg_hint_.flags_ & ObOptimizerStatisticsGatheringHint::OB_APPEND_HINT) ? true : false;
  }
  void set_append(const bool enable_append)
  {
    if (enable_append) {
      merge_osg_hint(ObOptimizerStatisticsGatheringHint::OB_APPEND_HINT);
    }
  }


  // wether should generate optimizer_statistics_operator.
  bool should_generate_osg_operator () const {
    // TODO parallel hint.
    return (osg_hint_.flags_ & ObOptimizerStatisticsGatheringHint::OB_NO_OPT_STATS_GATHER)
           ? false
           : (((osg_hint_.flags_ & ObOptimizerStatisticsGatheringHint::OB_APPEND_HINT)
             || (osg_hint_.flags_ & ObOptimizerStatisticsGatheringHint::OB_OPT_STATS_GATHER)) ?
             true : false);
  };

  bool has_no_gather_opt_stat_hint() const {
    return (osg_hint_.flags_ & ObOptimizerStatisticsGatheringHint::OB_NO_OPT_STATS_GATHER) ? true : false;
  }

  bool has_gather_opt_stat_hint() const {
    return (osg_hint_.flags_ & ObOptimizerStatisticsGatheringHint::OB_OPT_STATS_GATHER) ? true : false;
  }
  int64_t get_dynamic_sampling() const { return dynamic_sampling_; }
  bool has_dynamic_sampling() const { return UNSET_DYNAMIC_SAMPLING != dynamic_sampling_; }

  TO_STRING_KV(K_(frozen_version),
               K_(topk_precision),
               K_(sharding_minimum_row_count),
               K_(query_timeout),
               K_(tx_id),
               K_(tm_sessid),
               K_(read_consistency),
               K_(plan_cache_policy),
               K_(force_trace_log),
               K_(max_concurrent),
               K_(enable_lock_early_release),
               K_(force_refresh_lc),
               K_(log_level),
               K_(parallel),
               K_(monitor),
               K_(pdml_option),
               K_(param_option),
               K_(monitoring_ids),
               K_(dops),
               K_(opt_features_version),
               K_(disable_transform),
               K_(disable_cost_based_transform),
               K_(enable_append),
               K_(opt_params),
               K_(ob_ddl_schema_versions),
               K_(osg_hint),
               K_(has_dbms_stats_hint),
               K_(dynamic_sampling));

  int64_t frozen_version_;
  int64_t topk_precision_;
  int64_t sharding_minimum_row_count_;
  int64_t query_timeout_;
  int64_t tx_id_;
  int64_t tm_sessid_;
  common::ObConsistencyLevel read_consistency_;
  ObPlanCachePolicy plan_cache_policy_;
  bool force_trace_log_;
  int64_t max_concurrent_;
  bool enable_lock_early_release_;
  bool force_refresh_lc_;
  common::ObString log_level_;
  int64_t parallel_;
  bool monitor_;
  ObPDMLOption pdml_option_;
  ObParamOption param_option_;
  common::ObSArray<ObMonitorHint> monitoring_ids_;
  common::ObSArray<ObDopHint> dops_;
  uint64_t opt_features_version_;
  bool disable_transform_;
  bool disable_cost_based_transform_;
  bool enable_append_;
  ObOptParamHint opt_params_;
  common::ObSArray<ObDDLSchemaVersionHint> ob_ddl_schema_versions_;
  ObOptimizerStatisticsGatheringHint osg_hint_;
  bool has_dbms_stats_hint_;
  bool flashback_read_tx_uncommitted_;
  int64_t dynamic_sampling_;
};

// used in physical plan
struct ObPhyPlanHint
{
  OB_UNIS_VERSION(1);
public:
  ObPhyPlanHint()
      : read_consistency_(common::INVALID_CONSISTENCY),
        query_timeout_(-1),
        plan_cache_policy_(OB_USE_PLAN_CACHE_INVALID),
        force_trace_log_(false),
        log_level_(),
        parallel_(-1),
        monitor_(false)
  {}

  ObPhyPlanHint(const ObGlobalHint &global_hint)
      : read_consistency_(global_hint.read_consistency_),
        query_timeout_(global_hint.query_timeout_),
        plan_cache_policy_(global_hint.plan_cache_policy_),
        force_trace_log_(global_hint.force_trace_log_),
        log_level_(global_hint.log_level_),
        parallel_(global_hint.parallel_),
        monitor_(global_hint.monitor_)
  {}

  int deep_copy(const ObPhyPlanHint &other, common::ObIAllocator &allocator);

  void reset();

  TO_STRING_KV(K_(read_consistency), K_(query_timeout), K_(plan_cache_policy),
               K_(force_trace_log), K_(log_level), K_(parallel), K_(monitor));

  common::ObConsistencyLevel read_consistency_;
  int64_t query_timeout_;
  ObPlanCachePolicy plan_cache_policy_;
  bool force_trace_log_;
  common::ObString log_level_;
  int64_t parallel_;
  bool monitor_;
};

struct ObTableInHint
{
  ObTableInHint() {}
  ObTableInHint(const common::ObString &qb_name,
                const common::ObString &db_name,
                const common::ObString &table_name)
      : qb_name_(qb_name), db_name_(db_name), table_name_(table_name)
  { }
  int assign(const ObTableInHint &other);
  bool is_match_table_item(ObCollationType cs_type, const TableItem &table_item) const;
  bool is_match_physical_table_item(ObCollationType cs_type, const TableItem &table_item) const;
  static bool is_match_table_item(ObCollationType cs_type,
                                  const ObIArray<ObTableInHint> &tables,
                                  const TableItem &table_item);
  static bool is_match_table_items(ObCollationType cs_type,
                                  const ObIArray<ObTableInHint> &tables,
                                  ObIArray<TableItem *> &table_items);
  int print_table_in_hint(PlanText &plan_text, bool ignore_qb_name = false) const;
  static int print_join_tables_in_hint(PlanText &plan_text,
                                       const ObIArray<ObTableInHint> &tables,
                                       bool ignore_qb_name = false);

  void reset() { qb_name_.reset(); db_name_.reset(); table_name_.reset(); }
  void set_table(const TableItem& table);

  bool equal(const ObTableInHint& other) const;

  DECLARE_TO_STRING;

  common::ObString qb_name_;
  common::ObString db_name_;
  common::ObString table_name_;
};

struct ObLeadingTable {
  ObLeadingTable() : table_(NULL), left_table_(NULL), right_table_(NULL) {}
  void reset() { table_ = NULL; left_table_ = NULL; right_table_ = NULL; }
  int assign(const ObLeadingTable &other);
  int print_leading_table(PlanText &plan_text) const;
  bool is_single_table() const { return NULL != table_ && NULL == left_table_ && NULL == right_table_; }
  bool is_join_table() const { return NULL == table_ && NULL != left_table_ && NULL != right_table_; }
  bool is_valid() const { return is_single_table() || is_join_table(); }
  int get_all_table_in_leading_table(ObIArray<ObTableInHint*> &all_tables);
  ObTableInHint *find_match_hint_table(ObCollationType cs_type, const TableItem &table_item);
  int deep_copy(ObIAllocator *allocator, const ObLeadingTable &other);

  DECLARE_TO_STRING;

  ObTableInHint *table_;
  ObLeadingTable *left_table_;
  ObLeadingTable *right_table_;
};

class ObHint
{
public:
  enum HintClass
    {
      HINT_INVALID_CLASS  = 0,
      // transform hint below
      HINT_TRANSFORM,   // normal transform hint
      HINT_VIEW_MERGE,
      HINT_OR_EXPAND,
      HINT_MATERIALIZE,
      HINT_SEMI_TO_INNER,
      HINT_COALESCE_SQ,
      HINT_COUNT_TO_EXISTS,
      HINT_LEFT_TO_ANTI,
      HINT_ELIMINATE_JOIN,
      HINT_GROUPBY_PLACEMENT,
      HINT_WIN_MAGIC,
      // optimize hint below
      HINT_OPTIMIZE,    // normal optimize hint
      HINT_ACCESS_PATH,
      HINT_JOIN_ORDER,
      HINT_JOIN_METHOD,
      HINT_TABLE_PARALLEL,
      HINT_PQ_SET,
      HINT_JOIN_FILTER,
      HINT_TABLE_DYNAMIC_SAMPLING
    };

  static const int64_t MAX_EXPR_STR_LENGTH_IN_HINT = 1024;

  ObHint(ObItemType hint_type = T_INVALID)
    : hint_class_(HINT_INVALID_CLASS),
      qb_name_(),
      orig_hint_(NULL) {
        hint_type_ = get_hint_type(hint_type);
        is_enable_hint_ = (hint_type_ == hint_type);
      }
  virtual ~ObHint() {}
  int assign(const ObHint &other);
  void set_orig_hint(const ObHint *hint) { orig_hint_ = NULL == hint ? NULL : hint->get_orig_hint(); }
  const ObHint *get_orig_hint() const { return NULL == orig_hint_ ? this : orig_hint_; }
  ObItemType get_hint_type() const {  return hint_type_; };
  static ObItemType get_hint_type(ObItemType type);
  const ObString &get_qb_name() const { return qb_name_; }
  void set_qb_name(const ObString &qb_name) { return qb_name_.assign_ptr(qb_name.ptr(), qb_name.length()); }
  const char* get_hint_name() const { return get_hint_name(hint_type_, is_enable_hint_); };
  static const char* get_hint_name(ObItemType type, bool is_enable_hint = true);
  void set_hint_class(HintClass hint_class) { hint_class_ = hint_class; }
  bool is_enable_hint() const { return is_enable_hint_; }
  bool is_disable_hint() const { return !is_enable_hint_; }
  int print_hint(PlanText &plan_text) const;
  virtual int merge_hint(const ObHint *cur_hint,
                         const ObHint *other,
                         ObHintMergePolicy policy,
                         ObIArray<ObItemType> &conflict_hints,
                         const ObHint *&final_hint) const;
  virtual int print_hint_desc(PlanText &plan_text) const;
  // hint contain table need override this function
  virtual int get_all_table_in_hint(ObIArray<ObTableInHint*> &all_tables) { return OB_SUCCESS; }
  int create_push_down_hint(ObIAllocator *allocator,
                            ObCollationType cs_type,
                            const TableItem &source_table,
                            const TableItem &target_table,
                            ObHint *&hint);
  int add_tables(ObIArray<ObTableInHint> &tables, ObIArray<ObTableInHint*> &tables_ptr);
  static int get_expr_str_in_hint(ObIAllocator &allocator, const ObRawExpr &expr, ObString &str);
  static bool is_expr_match_str(const ObRawExpr &expr, const ObString &str);
  bool is_transform_outline_hint() const {  return is_transform_hint() && (is_enable_hint() || is_materialize_hint()); };
  bool is_transform_hint() const { return hint_class_ >= HINT_TRANSFORM && hint_class_ < HINT_OPTIMIZE; }
  bool is_view_merge_hint() const { return HINT_VIEW_MERGE == hint_class_; }
  bool is_pred_deduce_hint() const { return T_PRED_DEDUCE == hint_type_; }
  bool is_unnest_hint() const { return T_UNNEST == hint_type_; }
  bool is_aggr_first_unnest_hint() const { return T_AGGR_FIRST_UNNEST == hint_type_; }
  bool is_join_first_unnest_hint() const { return T_JOIN_FIRST_UNNEST == hint_type_; }
  bool is_coalesce_sq_hint() const { return HINT_COALESCE_SQ == hint_class_; }
  bool is_materialize_hint() const { return HINT_MATERIALIZE == hint_class_; }
  bool is_semi_to_inner_hint() const { return HINT_SEMI_TO_INNER == hint_class_; }
  bool is_or_expand_hint() const { return HINT_OR_EXPAND == hint_class_; }
  bool is_count_to_exists_hint() const { return HINT_COUNT_TO_EXISTS == hint_class_; }
  bool is_left_to_anti_hint() const { return HINT_LEFT_TO_ANTI == hint_class_; }
  bool is_eliminate_join_hint() const { return HINT_ELIMINATE_JOIN == hint_class_; }
  bool is_groupby_placement_hint() const { return HINT_GROUPBY_PLACEMENT == hint_class_; }
  bool is_win_magic_hint() const { return HINT_WIN_MAGIC == hint_class_; }
  bool is_optimize_hint() const { return HINT_OPTIMIZE <= hint_class_; }
  bool is_access_path_hint() const { return HINT_ACCESS_PATH == hint_class_; }
  bool is_join_order_hint() const { return HINT_JOIN_ORDER == hint_class_; }
  bool is_join_hint() const { return HINT_JOIN_METHOD == hint_class_; }
  bool is_table_parallel_hint() const { return HINT_TABLE_PARALLEL == hint_class_; }
  bool is_join_filter_hint() const { return HINT_JOIN_FILTER == hint_class_; }
  bool is_project_prune_hint() const { return T_PROJECT_PRUNE == hint_type_; }
  bool is_table_dynamic_sampling_hint() const { return HINT_TABLE_DYNAMIC_SAMPLING == hint_class_; }

  VIRTUAL_TO_STRING_KV("hint_type", get_type_name(hint_type_),
                       K_(hint_class), K_(qb_name),
                       K_(orig_hint), K_(is_enable_hint));

private:
  // only used in create_push_down_hint
  int deep_copy_hint_contain_table(ObIAllocator *allocator, ObHint *&hint) const;

protected:
  ObItemType hint_type_;
  HintClass hint_class_;
  ObString qb_name_;
  const ObHint *orig_hint_;
  bool is_enable_hint_;
};

class ObTransHint : public ObHint
{
public:
  ObTransHint(ObItemType hint_type) : ObHint(hint_type) { set_hint_class(HINT_TRANSFORM); }
  int assign(const ObTransHint &other) { return ObHint::assign(other); }
  virtual ~ObTransHint() {}
  virtual bool is_explicit() const { return false; }
};

class ObOptHint : public ObHint
{
public:
  ObOptHint(ObItemType hint_type) : ObHint(hint_type) { set_hint_class(HINT_OPTIMIZE); }
  int assign(const ObOptHint &other) { return ObHint::assign(other); }
  virtual ~ObOptHint() {}
};

class ObViewMergeHint : public ObTransHint
{
public:
  ObViewMergeHint(ObItemType hint_type)
    : ObTransHint(hint_type),
      parent_qb_name_(),
      is_query_push_down_(false)
  {
    set_hint_class(HINT_VIEW_MERGE);
  }
  int assign(const ObViewMergeHint &other);
  virtual ~ObViewMergeHint() {}

  const ObString &get_parent_qb_name() const { return parent_qb_name_; }
  void set_parent_qb_name(const ObString &qb_name) { return parent_qb_name_.assign_ptr(qb_name.ptr(), qb_name.length()); }
  void set_is_used_query_push_down(bool is_true) { is_query_push_down_ = is_true; }
  virtual int print_hint_desc(PlanText &plan_text) const override;
  virtual bool is_explicit() const override { return !parent_qb_name_.empty(); }
  bool enable_no_view_merge() const { return is_disable_hint(); }
  bool enable_no_query_push_down() const { return is_disable_hint(); }
  bool enable_no_group_by_pull_up() const { return is_disable_hint(); }
  bool enable_view_merge(const ObString &parent_qb_name) const
  { return is_enable_hint() && !is_query_push_down_
           && (parent_qb_name_.empty() || 0 == parent_qb_name_.case_compare(parent_qb_name)); }
  bool enable_query_push_down(const ObString &parent_qb_name) const
  { return is_enable_hint() && is_query_push_down_
           && (parent_qb_name_.empty() || 0 == parent_qb_name_.case_compare(parent_qb_name)); }
  bool enable_group_by_pull_up(const ObString &parent_qb_name) const
  { return enable_view_merge(parent_qb_name); }

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(parent_qb_name),
                                         K_(is_query_push_down));
private:
  ObString parent_qb_name_;
  bool is_query_push_down_;
};

class ObOrExpandHint : public ObTransHint
{
public:
  ObOrExpandHint(ObItemType hint_type)
    : ObTransHint(hint_type),
      expand_cond_()
  {
    set_hint_class(HINT_OR_EXPAND);
  }
  int assign(const ObOrExpandHint &other);
  virtual ~ObOrExpandHint() {}

  virtual int print_hint_desc(PlanText &plan_text) const override;
  virtual bool is_explicit() const override { return !expand_cond_.empty(); }
  void set_expand_condition(const char *bytes, int32_t length) { expand_cond_.assign_ptr(bytes, length); }
  int set_expand_condition(ObIAllocator &allocator, const ObRawExpr &expr)
  { return get_expr_str_in_hint(allocator, expr, expand_cond_); }
  bool enable_use_concat(const ObRawExpr &expr) const
  { return is_enable_hint() && (expand_cond_.empty() || is_expr_match_str(expr, expand_cond_)); }

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(expand_cond));

private:
  ObString expand_cond_;
};
class ObCountToExistsHint : public ObTransHint
{
public:
  ObCountToExistsHint(ObItemType hint_type)
    : ObTransHint(hint_type),
      qb_name_list_()
  {
    set_hint_class(HINT_COUNT_TO_EXISTS);
  }
  int assign(const ObCountToExistsHint &other);
  virtual ~ObCountToExistsHint() {}
  virtual int print_hint_desc(PlanText &plan_text) const override;
  common::ObIArray<ObString> & get_qb_name_list() { return qb_name_list_; }
  const common::ObIArray<ObString> & get_qb_name_list() const { return qb_name_list_; }
  bool enable_count_to_exists(const ObString &qb_name) const;

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(qb_name_list));

private:
  common::ObSEArray<ObString, 4, common::ModulePageAllocator, true> qb_name_list_;
};

class ObLeftToAntiHint : public ObTransHint
{
public:
  // basic/generated table: size = 1
  // joined table: size > 1
  typedef ObSEArray<ObTableInHint, 4> single_or_joined_table;
  
  ObLeftToAntiHint(ObItemType hint_type)
    : ObTransHint(hint_type),
      table_list_()
  {
    set_hint_class(HINT_LEFT_TO_ANTI);
  }
  int assign(const ObLeftToAntiHint &other);
  virtual ~ObLeftToAntiHint() {}

  virtual int print_hint_desc(PlanText &plan_text) const override;
  virtual int get_all_table_in_hint(ObIArray<ObTableInHint*> &all_tables) override;
  common::ObIArray<single_or_joined_table> & get_tb_name_list() { return table_list_; }
  const common::ObIArray<single_or_joined_table> & get_tb_name_list() const { return table_list_; }
  bool enable_left_to_anti(ObCollationType cs_type, const TableItem &table) const;

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(table_list));

private:
  common::ObSEArray<single_or_joined_table, 4, common::ModulePageAllocator, true> table_list_;
};
class ObEliminateJoinHint : public ObTransHint
{
public:
  // basic/generated table: size = 1
  // joined table: size > 1
  typedef ObSEArray<ObTableInHint, 4> single_or_joined_table;
  
  ObEliminateJoinHint(ObItemType hint_type)
    : ObTransHint(hint_type),
      table_list_()
  {
    set_hint_class(HINT_ELIMINATE_JOIN);
  }
  int assign(const ObEliminateJoinHint &other);
  virtual ~ObEliminateJoinHint() {}

  virtual int print_hint_desc(PlanText &plan_text) const override;
  virtual int get_all_table_in_hint(ObIArray<ObTableInHint*> &all_tables) override;
  common::ObIArray<single_or_joined_table> & get_tb_name_list() { return table_list_; }
  const common::ObIArray<single_or_joined_table> & get_tb_name_list() const { return table_list_; }
  bool enable_eliminate_join(ObCollationType cs_type, const TableItem &table) const;

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(table_list));

private:
  common::ObSEArray<single_or_joined_table, 4, common::ModulePageAllocator, true> table_list_;
};

class ObGroupByPlacementHint : public ObTransHint
{
public:
  // basic/generated table: size = 1
  // joined table: size > 1
  typedef ObSEArray<ObTableInHint, 4> single_or_joined_table;
  
  ObGroupByPlacementHint(ObItemType hint_type)
    : ObTransHint(hint_type),
      table_list_()
  {
    set_hint_class(HINT_GROUPBY_PLACEMENT);
  }
  int assign(const ObGroupByPlacementHint &other);
  virtual ~ObGroupByPlacementHint() {}

  virtual int print_hint_desc(PlanText &plan_text) const override;
  virtual int get_all_table_in_hint(ObIArray<ObTableInHint*> &all_tables) override;
  common::ObIArray<single_or_joined_table> & get_tb_name_list() { return table_list_; }
  const common::ObIArray<single_or_joined_table> & get_tb_name_list() const { return table_list_; }
  bool enable_groupby_placement(ObCollationType cs_type, const TableItem &table) const;
  bool enable_groupby_placement(ObCollationType cs_type, const ObIArray<TableItem *> &tables) const;

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(table_list));

private:
  common::ObSEArray<single_or_joined_table, 4, common::ModulePageAllocator, true> table_list_;
};
class ObWinMagicHint : public ObTransHint
{
public:
  
  ObWinMagicHint(ObItemType hint_type)
    : ObTransHint(hint_type),
      table_list_()
  {
    set_hint_class(HINT_WIN_MAGIC);
  }
  int assign(const ObWinMagicHint &other);
  virtual ~ObWinMagicHint() {}
  virtual int print_hint_desc(PlanText &plan_text) const override;
  virtual int get_all_table_in_hint(ObIArray<ObTableInHint*> &all_tables) override;
  common::ObIArray<ObTableInHint> &get_tb_name_list() { return table_list_; }
  const common::ObIArray<ObTableInHint> &get_tb_name_list() const { return table_list_; }
  bool enable_win_magic(ObCollationType cs_type, const TableItem &table) const;

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(table_list));

private:
  common::ObSEArray<ObTableInHint, 4, common::ModulePageAllocator, true> table_list_;
};


struct QbNameList {
  int assign(const QbNameList& other);
  bool has_qb_name(const ObDMLStmt *stmt) const;
  bool has_qb_name(const ObString &qb_name) const;
  bool is_equal(const ObIArray<ObSelectStmt*> &stmts) const;
  bool is_equal(const ObIArray<ObString> &qb_name_list) const;
  bool is_subset(const ObIArray<ObSelectStmt*> &stmts) const;
  bool is_subset(const ObIArray<ObString> &qb_name_list) const;
  bool empty() const { return qb_names_.empty(); }
  TO_STRING_KV(K_(qb_names));
  common::ObSEArray<ObString, 4, common::ModulePageAllocator, true> qb_names_;
};

class ObMaterializeHint :  public ObTransHint
{
public:
  ObMaterializeHint(ObItemType hint_type)
    : ObTransHint(hint_type),
      qb_name_list_()
  {
    set_hint_class(HINT_MATERIALIZE);
  }
  int assign(const ObMaterializeHint &other);
  virtual ~ObMaterializeHint() {}

  virtual int print_hint_desc(PlanText &plan_text) const override;
  ObIArray<QbNameList> &get_qb_name_list() { return qb_name_list_; }
  bool has_qb_name_list() const { return !qb_name_list_.empty(); }
  int add_qb_name_list(const QbNameList& qb_names);
  int get_qb_name_list(const ObString& qb_name, QbNameList &qb_names) const;
  bool enable_materialize_subquery(const ObIArray<ObString> & subqueries) const;
  bool enable_materialize() const
  { return is_enable_hint() && qb_name_list_.empty(); }
  bool enable_inline() const { return is_disable_hint(); }

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(qb_name_list));

private:
  common::ObSEArray<QbNameList, 2, common::ModulePageAllocator, true> qb_name_list_;
};

class ObSemiToInnerHint : public ObTransHint
{
  public:
  ObSemiToInnerHint(ObItemType hint_type)
    : ObTransHint(hint_type)
  {
    set_hint_class(HINT_SEMI_TO_INNER);
  }
  int assign(const ObSemiToInnerHint &other);
  virtual ~ObSemiToInnerHint() {}

  virtual int print_hint_desc(PlanText &plan_text) const override;
  virtual int get_all_table_in_hint(ObIArray<ObTableInHint*> &all_tables) override { return add_tables(tables_, all_tables); }
  virtual bool is_explicit() const override { return !tables_.empty(); }
  ObIArray<ObTableInHint> &get_tables() { return tables_; }
  const ObIArray<ObTableInHint> &get_tables() const { return tables_; }
  bool enable_semi_to_inner(ObCollationType cs_type, const TableItem &table_item) const;

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(tables));

private:
  common::ObSEArray<ObTableInHint, 2, common::ModulePageAllocator, true> tables_;
};


class ObCoalesceSqHint : public ObTransHint
{
  public:
  ObCoalesceSqHint(ObItemType hint_type)
    : ObTransHint(hint_type),
      qb_name_list_()
  {
    set_hint_class(HINT_COALESCE_SQ);
  }
  int assign(const ObCoalesceSqHint &other);
  virtual ~ObCoalesceSqHint() {}

  virtual int print_hint_desc(PlanText &plan_text) const override;
  ObIArray<QbNameList> &get_qb_name_list() { return qb_name_list_; }
  int add_qb_name_list(const QbNameList& qb_names);
  int get_qb_name_list(const ObString& qb_name, QbNameList &qb_names) const;
  bool enable_coalesce_sq(const ObIArray<ObString> &subqueries) const
  { return is_enable_hint() && has_qb_name_list(subqueries); }

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(qb_name_list));

private:
  bool has_qb_name_list(const ObIArray<ObString> & qb_names) const;
  common::ObSEArray<QbNameList, 2, common::ModulePageAllocator, true> qb_name_list_;
};

class ObIndexHint : public ObOptHint
{
public:
  ObIndexHint(ObItemType hint_type)
    : ObOptHint(hint_type)
  {
    set_hint_class(HINT_ACCESS_PATH);
  }
  int assign(const ObIndexHint &other);
  virtual ~ObIndexHint() {}

  static const ObString PRIMARY_KEY;

  virtual int get_all_table_in_hint(ObIArray<ObTableInHint*> &all_tables) override { return all_tables.push_back(&table_); }
  virtual int print_hint_desc(PlanText &plan_text) const override;
  ObTableInHint &get_table() { return table_; }
  const ObTableInHint &get_table() const { return table_; }
  ObString &get_index_name() { return index_name_; }
  const ObString &get_index_name() const { return index_name_; }
  bool is_use_index_hint()  const { return T_NO_INDEX_HINT != get_hint_type(); }
  bool use_skip_scan()  const { return T_INDEX_SS_HINT == get_hint_type(); }

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(table), K_(index_name));

private:
  ObTableInHint table_;
  common::ObString index_name_;
};

class ObTableParallelHint : public ObOptHint
{
public:
  ObTableParallelHint(ObItemType hint_type = T_TABLE_PARALLEL)
    : ObOptHint(hint_type), parallel_(ObGlobalHint::UNSET_PARALLEL)
  {
    set_hint_class(HINT_TABLE_PARALLEL);
  }
  int assign(const ObTableParallelHint &other);
  virtual ~ObTableParallelHint() {}
  virtual int get_all_table_in_hint(ObIArray<ObTableInHint*> &all_tables) override { return all_tables.push_back(&table_); }
  virtual int print_hint_desc(PlanText &plan_text) const override;
  ObTableInHint &get_table() { return table_; }
  const ObTableInHint &get_table() const { return table_; }
  int64_t get_parallel() const { return parallel_; }
  void set_parallel(int64_t parallel) { parallel_ = parallel; }
  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(table), K_(table), K_(parallel));

private:
  ObTableInHint table_;
  int64_t parallel_;
};

class ObJoinHint : public ObOptHint
{
public:
  ObJoinHint(ObItemType hint_type = T_INVALID)
    : ObOptHint(hint_type),
      dist_algo_(DistAlgo::DIST_INVALID_METHOD)
  {
    set_hint_class(HINT_JOIN_METHOD);
  }
  int assign(const ObJoinHint &other);
  virtual ~ObJoinHint() {}
  virtual int get_all_table_in_hint(ObIArray<ObTableInHint*> &all_tables) override { return add_tables(tables_, all_tables); }
  virtual int print_hint_desc(PlanText &plan_text) const override;
  bool is_match_local_algo(JoinAlgo join_algo) const;
  const char *get_dist_algo_str() const { return get_dist_algo_str(dist_algo_); }
  static const char *get_dist_algo_str(DistAlgo dist_algo);
  static bool need_print_dist_algo(DistAlgo dist_algo) {  return  NULL != get_dist_algo_str(dist_algo); }

  ObIArray<ObTableInHint> &get_tables() { return tables_; }
  const ObIArray<ObTableInHint> &get_tables() const { return tables_; }
  DistAlgo get_dist_algo() const { return dist_algo_; }
  void set_dist_algo(DistAlgo dist_algo) { dist_algo_ = dist_algo; }

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(tables), K_(dist_algo));

private:
  common::ObSEArray<ObTableInHint, 4, common::ModulePageAllocator, true> tables_;
  DistAlgo dist_algo_;
};

class ObJoinFilterHint : public ObOptHint
{
public:
  ObJoinFilterHint(ObItemType hint_type = T_INVALID)
    : ObOptHint(hint_type)
  {
    set_hint_class(HINT_JOIN_FILTER);
  }
  int assign(const ObJoinFilterHint &other);
  virtual ~ObJoinFilterHint() {}
  virtual int get_all_table_in_hint(ObIArray<ObTableInHint*> &all_tables) override;
  virtual int print_hint_desc(PlanText &plan_text) const override;

  ObTableInHint &get_filter_table() { return filter_table_; }
  const ObTableInHint &get_filter_table() const { return filter_table_; }
  ObTableInHint &get_pushdown_filter_table() { return pushdown_filter_table_; }
  const ObTableInHint &get_pushdown_filter_table() const { return pushdown_filter_table_; }
  ObIArray<ObTableInHint> &get_left_tables() { return left_tables_; }
  const ObIArray<ObTableInHint> &get_left_tables() const { return left_tables_; }
  bool is_part_join_filter_hint() const { return T_PX_PART_JOIN_FILTER == hint_type_; }
  bool has_left_tables() const { return !left_tables_.empty(); }
  bool has_pushdown_filter_table() const { return !pushdown_filter_table_.table_name_.empty(); }

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(filter_table), K_(left_tables));

private:
  ObTableInHint filter_table_;
  common::ObSEArray<ObTableInHint, 1, common::ModulePageAllocator, true> left_tables_;
  ObTableInHint pushdown_filter_table_;
};

class ObPQSetHint : public ObOptHint
{
  public:
  ObPQSetHint(ObItemType hint_type = T_PQ_SET)
    : ObOptHint(hint_type),
      dist_methods_(),
      left_branch_()
  {
    set_hint_class(HINT_PQ_SET);
  }
  int assign(const ObPQSetHint &other);
  virtual ~ObPQSetHint() {}
  virtual int print_hint_desc(PlanText &plan_text) const override;
  static bool is_valid_dist_methods(const ObIArray<ObItemType> &dist_methods);
  static DistAlgo get_dist_algo(const ObIArray<ObItemType> &dist_methods,
                                int64_t &random_none_idx);
  static const char* get_dist_method_str(const ObItemType dist_method);

  const ObIArray<ObItemType> &get_dist_methods() const { return dist_methods_; }
  ObIArray<ObItemType> &get_dist_methods() { return dist_methods_; }
  int set_pq_set_hint(const DistAlgo dist_algo, const int64_t child_num, const int64_t random_none_idx);
  DistAlgo get_dist_algo(int64_t &random_none_idx) const { return get_dist_algo(dist_methods_, random_none_idx); }
  const ObString &get_left_branch() const { return left_branch_; }
  void set_left_branch(const ObString &left_branch) { return left_branch_.assign_ptr(left_branch.ptr(), left_branch.length()); }
  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(dist_methods), K_(left_branch));

private:
  common::ObSEArray<ObItemType, 2, common::ModulePageAllocator, true> dist_methods_;
  common::ObString left_branch_;  // qb_name for first branch of set, used for union distinct / intersect
};

class ObJoinOrderHint : public ObOptHint {
public:
  ObJoinOrderHint(ObItemType hint_type = T_LEADING)
    : ObOptHint(hint_type)
  {
    set_hint_class(HINT_JOIN_ORDER);
  }
  int assign(const ObJoinOrderHint &other);
  virtual ~ObJoinOrderHint() {}
  bool is_ordered_hint() const { return is_disable_hint(); }
  virtual int get_all_table_in_hint(ObIArray<ObTableInHint*> &all_tables) override { return table_.get_all_table_in_leading_table(all_tables); }
  virtual int print_hint_desc(PlanText &plan_text) const override;
  virtual int merge_hint(const ObHint *cur_hint,
                         const ObHint *other,
                         ObHintMergePolicy policy,
                         ObIArray<ObItemType> &conflict_hints,
                         const ObHint *&final_hint) const override;
  ObLeadingTable &get_table() { return table_; }
  const ObLeadingTable &get_table() const { return table_; }

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(table));

private:
  ObLeadingTable table_;
};

class ObWindowDistHint : public ObOptHint {
public:
  explicit ObWindowDistHint(ObItemType hint_type = T_PQ_DISTRIBUTE_WINDOW)
      : ObOptHint(hint_type)
  {
  }
  struct WinDistOption {
    WinDistOption() { reset();  }
    int assign(const WinDistOption& other);
    void reset();
    bool is_valid() const;
    int print_win_dist_option(PlanText &plan_text) const;

    WinDistAlgo algo_;
    common::ObSEArray<int64_t, 2, common::ModulePageAllocator, true> win_func_idxs_;
    bool use_hash_sort_;  // use hash sort for none/hash dist method
    bool is_push_down_;  // push down window function for hash dist method
    TO_STRING_KV(K_(algo), K_(win_func_idxs), K_(use_hash_sort), K_(is_push_down));
  };

  const ObIArray<WinDistOption> &get_win_dist_options() const { return win_dist_options_; }
  int set_win_dist_options(const ObIArray<WinDistOption> &win_dist_options) { return win_dist_options_.assign(win_dist_options); }
  int add_win_dist_option(const ObIArray<ObWinFunRawExpr*> &all_win_funcs,
                          const ObIArray<ObWinFunRawExpr*> &cur_win_funcs,
                          const WinDistAlgo algo,
                          const bool is_push_down,
                          const bool use_hash_sort);
  int add_win_dist_option(const ObIArray<int64_t> &win_func_idxs,
                          const WinDistAlgo algo,
                          const bool is_push_down,
                          const bool use_hash_sort);
  static const char* get_dist_algo_str(WinDistAlgo dist_algo);

  virtual int print_hint_desc(PlanText &plan_text) const override;

  INHERIT_TO_STRING_KV("hint", ObHint, K_(win_dist_options));
private:
  common::ObSEArray<WinDistOption, 2, common::ModulePageAllocator, true> win_dist_options_;
};

class ObAggHint : public ObOptHint
{
public:
  ObAggHint(ObItemType hint_type)
    : ObOptHint(hint_type),
      sort_method_valid_(false),
      use_partition_sort_(false)
  {
  }
  int assign(const ObAggHint &other);
  virtual ~ObAggHint() {}

  virtual int print_hint_desc(PlanText &plan_text) const override;
  void set_use_partition_sort(bool use_part_sort) { sort_method_valid_ = is_disable_hint(); use_partition_sort_ = sort_method_valid_ && use_part_sort; }
  void reset_use_partition_sort() { sort_method_valid_ = false; use_partition_sort_ = false; }
  bool force_partition_sort()  const { return is_disable_hint() && sort_method_valid_ && use_partition_sort_; }
  bool force_normal_sort()  const { return is_disable_hint() && sort_method_valid_ && !use_partition_sort_; }

  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(sort_method_valid), K_(use_partition_sort));

private:
  bool sort_method_valid_;
  bool use_partition_sort_;
};

struct ObDDLSchemaVersionHint
{
  ObDDLSchemaVersionHint() : schema_version_(0) {}

  TO_STRING_KV(K_(table), K_(schema_version));
  ObTableInHint table_;
  int64_t schema_version_;
};
class ObTableDynamicSamplingHint : public ObOptHint
{
public:
  ObTableDynamicSamplingHint(ObItemType hint_type = T_TABLE_DYNAMIC_SAMPLING)
    : ObOptHint(hint_type), dynamic_sampling_(ObGlobalHint::UNSET_DYNAMIC_SAMPLING), sample_block_cnt_(0)
  {
    set_hint_class(HINT_TABLE_DYNAMIC_SAMPLING);
  }
  int assign(const ObTableDynamicSamplingHint &other);
  virtual ~ObTableDynamicSamplingHint() {}
  virtual int get_all_table_in_hint(ObIArray<ObTableInHint*> &all_tables) override { return all_tables.push_back(&table_); }
  virtual int print_hint_desc(PlanText &plan_text) const override;
  ObTableInHint &get_table() { return table_; }
  const ObTableInHint &get_table() const { return table_; }
  int64_t get_dynamic_sampling() const { return dynamic_sampling_; }
  void set_dynamic_sampling(int64_t dynamic_sampling) { dynamic_sampling_ = dynamic_sampling; }
  int64_t get_sample_block_cnt() const { return sample_block_cnt_; }
  void set_sample_block_cnt(int64_t sample_block_cnt) { sample_block_cnt_ = sample_block_cnt; }
  INHERIT_TO_STRING_KV("ObHint", ObHint, K_(table), K_(dynamic_sampling), K_(sample_block_cnt));

private:
  ObTableInHint table_;
  int64_t dynamic_sampling_;
  int64_t sample_block_cnt_;
};

}
}

#endif
