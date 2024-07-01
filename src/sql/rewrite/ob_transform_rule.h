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

#ifndef _OCEANBASE_SQL_REWRITE_RULE_H
#define _OCEANBASE_SQL_REWRITE_RULE_H
#include "lib/container/ob_se_array.h"
#include "sql/resolver/dml/ob_raw_expr_sets.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/ob_optimizer_trace_impl.h"
#include "sql/ob_sql_context.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObAddr;
class ObOptStatManager;
}
namespace sql
{
class ObDMLStmt;
class ObSelectStmt;
class ObSQLSessionInfo;
class ObSchemaChecker;
class ObExecContext;
class ObRawExprFactory;
class ObStmtFactory;
class ObPhysicalPlan;
class ObCodeGeneratorImpl;
class ObLogPlan;
class StmtUniqueKeyProvider;

struct ObTransformerCtx
{
  ObTransformerCtx()
  : allocator_(NULL),
    session_info_(NULL),
    schema_checker_(NULL),
    exec_ctx_(NULL),
    expr_factory_(NULL),
    stmt_factory_(NULL),
    opt_stat_mgr_(NULL),
    sql_schema_guard_(NULL),
    self_addr_(NULL),
    phy_plan_(NULL),
    merged_version_(0),
    expr_constraints_(),
    plan_const_param_constraints_(),
    equal_param_constraints_(),
    is_set_stmt_oversize_(false),
    happened_cost_based_trans_(0),
    equal_sets_(),
    ignore_semi_infos_(),
    temp_table_ignore_stmts_(),
    eval_cost_(false),
    trans_list_loc_(0),
    src_qb_name_(),
    src_hash_val_(),
    outline_trans_hints_(),
    used_trans_hints_(),
    groupby_pushdown_stmts_(),
    is_spm_outline_(false),
    push_down_filters_(),
    in_accept_transform_(false),
    iteration_level_(0)
  { }
  virtual ~ObTransformerCtx() {}

  // used to get hash value to generate qb name.
  static const ObString SRC_STR_OR_EXPANSION_WHERE;
  static const ObString SRC_STR_OR_EXPANSION_INNER_JOIN;
  static const ObString SRC_STR_OR_EXPANSION_OUTER_JOIN;
  static const ObString SRC_STR_OR_EXPANSION_SEMI;
  static const ObString SRC_STR_CREATE_SIMPLE_VIEW;

  int add_src_hash_val(const ObString &src_str);
  int add_src_hash_val(uint64_t trans_type);
  const char* get_trans_type_string(uint64_t trans_type);
  int add_used_trans_hint(const ObHint *used_hint)
  {
    int ret = OB_SUCCESS;
    if (NULL != used_hint) {
      ret = add_var_to_array_no_dup(used_trans_hints_, used_hint->get_orig_hint());
    }
    return ret;
  }

  // for unittest
  void reset();
  bool is_valid();
  common::ObIAllocator *allocator_;
  ObSQLSessionInfo *session_info_;
  ObSchemaChecker *schema_checker_;
  ObExecContext *exec_ctx_;
  ObRawExprFactory *expr_factory_;
  ObStmtFactory *stmt_factory_;
  common::ObOptStatManager *opt_stat_mgr_;
  ObSqlSchemaGuard *sql_schema_guard_;
  common::ObAddr *self_addr_;

  /* member below is from phy_plan */
  ObPhysicalPlan *phy_plan_;

  int64_t merged_version_;
  ObSEArray<ObExprConstraint, 4, common::ModulePageAllocator, true> expr_constraints_;
  ObSEArray<ObPCConstParamInfo, 4, common::ModulePageAllocator, true> plan_const_param_constraints_;
  ObSEArray<ObPCParamEqualInfo, 4, common::ModulePageAllocator, true> equal_param_constraints_;
  bool is_set_stmt_oversize_;
  // record cost based transformers
  uint64_t happened_cost_based_trans_;
  EqualSets equal_sets_;
  //记录semi to inner改写中，代价竞争失败的semi info，避免下一轮迭代重复检查代价
  ObSEArray<uint64_t, 8, common::ModulePageAllocator, true> ignore_semi_infos_;
  ObSEArray<ObSelectStmt*, 8, common::ModulePageAllocator, true> temp_table_ignore_stmts_;
  bool eval_cost_;
  /* used for hint and outline below */
  int64_t trans_list_loc_;  // outline mode, used to keep transform happened ordering in query_hint.trans_list_
  ObString src_qb_name_;
  ObSEArray<uint32_t, 4> src_hash_val_;
  ObSEArray<const ObHint*, 8> outline_trans_hints_; // tranform hints to generate outline data
  ObSEArray<const ObHint*, 8> used_trans_hints_;
  ObSEArray<uint64_t, 4> groupby_pushdown_stmts_;
  /* end used for hint and outline below */
  bool is_spm_outline_;
  ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> push_down_filters_;
  bool in_accept_transform_;
  uint64_t iteration_level_;
};

enum TransMethod
{
  POST_ORDER = 0,
  PRE_ORDER = 1,
  ROOT_ONLY = 2,
  MAX_TRANSFORMATION_METHOD
};

enum TRANSFORM_TYPE {
  INVALID_TRANSFORM_TYPE     = 0,
  PRE_PROCESS                   ,
  POST_PROCESS                  ,
  SIMPLIFY_DISTINCT             ,
  SIMPLIFY_EXPR                 ,
  SIMPLIFY_GROUPBY              ,
  SIMPLIFY_LIMIT                ,
  SIMPLIFY_ORDERBY              ,
  SIMPLIFY_SUBQUERY             ,
  SIMPLIFY_WINFUNC              ,
  FASTMINMAX                    , // select min/max -> select order by limit 1
  ELIMINATE_OJ                  , // 外连接消除
  VIEW_MERGE                    , // 视图合并
  WHERE_SQ_PULL_UP              , // where子查询 -> semi join
  QUERY_PUSH_DOWN               , // push down limit
  AGGR_SUBQUERY                 , // JA类型子查询改写
  SIMPLIFY_SET                  , // set相关改写
  PROJECTION_PRUNING            , // project pruning
  OR_EXPANSION                  , // or expansion
  WIN_MAGIC                     ,
  JOIN_ELIMINATION              ,
  GROUPBY_PUSHDOWN              ,
  GROUPBY_PULLUP                ,
  SUBQUERY_COALESCE             ,
  PREDICATE_MOVE_AROUND         ,
  NL_FULL_OUTER_JOIN            ,
  SEMI_TO_INNER                 ,
  JOIN_LIMIT_PUSHDOWN           ,
  TEMP_TABLE_OPTIMIZATION       ,
  CONST_PROPAGATE               ,
  LEFT_JOIN_TO_ANTI             ,  // left join + is null -> anti-join
  COUNT_TO_EXISTS               ,
  SELECT_EXPR_PULLUP            ,
  PROCESS_DBLINK                ,
  DECORRELATE                   ,
  CONDITIONAL_AGGR_COALESCE     ,
  MV_REWRITE                    ,
  TRANSFORM_TYPE_COUNT_PLUS_ONE ,
};

struct ObParentDMLStmt
{
  ObParentDMLStmt() :
      pos_(0), stmt_(NULL)
  {
  }
  virtual ~ObParentDMLStmt() {}
  int64_t pos_;
  ObDMLStmt *stmt_;
  TO_STRING_KV(K_(pos),
               K_(stmt));
};

// use to keep view name/stmt id/qb name stable after copy stmt and try transform
struct ObTryTransHelper
{
  ObTryTransHelper() :
    available_tb_id_(0),
    subquery_count_(0),
    temp_table_count_(0),
    qb_name_sel_start_id_(0),
    qb_name_set_start_id_(0),
    qb_name_other_start_id_(0),
    unique_key_provider_(NULL)
  {}

  int fill_helper(const ObQueryCtx *query_ctx);
  int recover(ObQueryCtx *query_ctx);
  int is_filled() const { return !qb_name_counts_.empty(); }

  uint64_t available_tb_id_;
  int64_t subquery_count_;
  int64_t temp_table_count_;
  int64_t qb_name_sel_start_id_;
  int64_t qb_name_set_start_id_;
  int64_t qb_name_other_start_id_;
  ObSEArray<int64_t, 4, common::ModulePageAllocator, true> qb_name_counts_;
  StmtUniqueKeyProvider *unique_key_provider_;
};

// record context param values or array/list size
// param name in this structure is same as the name in origin contexts
struct ObEvalCostHelper
{
  int fill_helper(const ObPhysicalPlanCtx &phy_plan_ctx,
                  const ObQueryCtx &query_ctx,
                  const ObTransformerCtx &trans_ctx);
  int recover_context(ObPhysicalPlanCtx &phy_plan_ctx,
                      ObQueryCtx &query_ctx,
                      ObTransformerCtx &trans_ctx);

  // from query_ctx
  int64_t question_marks_count_;
  int64_t calculable_items_count_;
  ObTryTransHelper try_trans_helper_;

  // from transformer context
  bool eval_cost_;
  int64_t expr_constraints_count_;
  int64_t plan_const_param_constraints_count_;
  int64_t equal_param_constraints_count_;
  ObString src_qb_name_;
  int64_t outline_trans_hints_count_;
  int64_t used_trans_hints_count_;
};

class ObTransformRule
{
public:
  static const int64_t TRANSFORMER_DEFAULT_MAX_RECURSIVE_LEVEL = 150;
  static const uint64_t ALL_TRANSFORM_RULES = (1L << TRANSFORM_TYPE_COUNT_PLUS_ONE) - 1L;
  static const uint64_t ALL_HEURISTICS_RULES =
      (1L << SIMPLIFY_EXPR) |
      (1L << SIMPLIFY_DISTINCT) |
      (1L << SIMPLIFY_GROUPBY) |
      (1L << SIMPLIFY_WINFUNC) |
      (1L << SIMPLIFY_ORDERBY) |
      (1L << SIMPLIFY_LIMIT) |
      (1L << SIMPLIFY_SUBQUERY) |
      (1L << FASTMINMAX) |
      (1L << ELIMINATE_OJ) |
      (1L << VIEW_MERGE) |
      (1L << WHERE_SQ_PULL_UP) |
      (1L << QUERY_PUSH_DOWN) |
      (1L << SIMPLIFY_SET) |
      (1L << PROJECTION_PRUNING) |
      (1L << JOIN_ELIMINATION) |
      (1L << AGGR_SUBQUERY) |
      (1L << PREDICATE_MOVE_AROUND) |
      (1L << NL_FULL_OUTER_JOIN) |
      (1L << JOIN_LIMIT_PUSHDOWN) |
      (1L << CONST_PROPAGATE) |
      (1L << LEFT_JOIN_TO_ANTI) |
      (1L << COUNT_TO_EXISTS) |
      (1L << CONDITIONAL_AGGR_COALESCE) |
      (1L << SEMI_TO_INNER);
  static const uint64_t ALL_COST_BASED_RULES =
      (1L << OR_EXPANSION) |
      (1L << WIN_MAGIC) |
      (1L << GROUPBY_PUSHDOWN) |
      (1L << GROUPBY_PULLUP) |
      (1L << SUBQUERY_COALESCE) |
      (1L << SEMI_TO_INNER) |
      (1L << MV_REWRITE);

  ObTransformRule(ObTransformerCtx *ctx,
                  TransMethod transform_method,
                  ObItemType hint_type = T_INVALID)
    : ctx_(ctx),
      transform_method_(transform_method),
      hint_type_(hint_type),
      transformer_type_(INVALID_TRANSFORM_TYPE),
      trans_happened_(false),
      cost_based_trans_tried_(false),
      current_temp_table_(NULL),
      stmt_cost_(-1)
  {
  }
  virtual ~ObTransformRule()
  {
  }
  virtual int transform(ObDMLStmt *&stmt, uint64_t &transform_types);
  
  inline bool get_trans_happened() const
  {
    return trans_happened_;
  }
  inline void set_trans_happened()
  {
    trans_happened_ = true;
  }
  inline TransMethod get_transform_method()
  {
    return transform_method_;
  }
  inline void set_transform_method(TransMethod transform_method)
  {
    transform_method_ = transform_method;
  }
  inline void set_transformer_type(uint64_t transformer_type)
  {
    transformer_type_ = transformer_type;
  }
  inline uint64_t get_transformer_type() const
  {
    return transformer_type_;
  }
  static inline void add_trans_type(uint64_t &types, TRANSFORM_TYPE type)
  {
    types = (types | (1L << type));
  }
  int transform_self(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                     const int64_t current_level,
                     ObDMLStmt *&stmt);
protected:
  /*
   * This function tries to recursively transform all statements including its child statements
   * By default, there are two options, pre-order transformation and post-order transformation
   * If any rule want to change this behavior, rewrite this function
   */
  virtual int transform_stmt_recursively(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                         const int64_t current_level,
                                         ObDMLStmt *&stmt);
  /*
   * This function tries to transform one statement
   * parent_stmts: some rule may use parent statement to decide how to do rewriting, so keep it for use
   * Every rule should rewrite this function,
   * parameter stmt: input statement
   * parameter transformed_stmts: transformed statement
   */
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) = 0;

  virtual int transform_one_stmt_with_outline(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                              ObDMLStmt *&stmt,
                                              bool &trans_happened);

  /*
   * this function is for cost-based rules,
   * mainly used to check whether accept transformed stmt:
   *  1. force accept due to hint/rule;
   *  2. the transformed stmt has less cost than the origin stmt.
   */
  int accept_transform(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                       ObDMLStmt *&stmt,
                       ObDMLStmt *trans_stmt,
                       bool force_accept,
                       bool check_original_plan,
                       bool &trans_happened,
                       void *check_ctx = NULL);

  /*
   * after transforming a rule, this function try to figure out which rules to apply in the next iteration
   */
  virtual int adjust_transform_types(uint64_t &transform_types);

  /*
   * after transform happend, add a transform hint to transform context to generate outline data.
   */
  int add_transform_hint(ObDMLStmt &trans_stmt, void *trans_params = NULL);
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params);
  bool is_normal_disabled_transform(const ObDMLStmt &stmt);

  virtual int need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             const int64_t current_level,
                             const ObDMLStmt &stmt,
                             bool &need_trans);

  virtual int check_hint_status(const ObDMLStmt &stmt, bool &need_trans);

  const ObHint* get_hint(const ObStmtHint &hint) const
  { return hint.get_normal_hint(hint_type_); }
  ObItemType get_hint_type() const  { return hint_type_; }

  int deep_copy_temp_table(ObDMLStmt &stmt,
                           ObStmtFactory &stmt_factory,
                           ObRawExprFactory &expr_factory,
                           ObIArray<ObSelectStmt*> &old_temp_table_stmts,
                           ObIArray<ObSelectStmt*> &new_temp_table_stmts);

private:
  // pre-order transformation
  int transform_pre_order(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                          const int64_t current_level,
                          ObDMLStmt *&stmt);
  // post-order transformation
  int transform_post_order(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                           const int64_t current_level,
                           ObDMLStmt *&stmt);
  // root-only transformation
  int transform_root_only(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                          const int64_t current_level,
                          ObDMLStmt *&stmt);

  // transform non_set children statements
  int transform_children(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                         const int64_t current_level,
                         ObDMLStmt *&stmt);
  // transform temp table for root stmt
  int transform_temp_tables(ObIArray<ObParentDMLStmt> &parent_stmts,
                            const int64_t current_level,
                            ObDMLStmt *&stmt);
  int adjust_transformed_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                              ObDMLStmt *stmt,
                              ObDMLStmt *&orgin_stmt,
                              ObDMLStmt *&root_stmt);

  int evaluate_cost(common::ObIArray<ObParentDMLStmt> &parent_stms,
                    ObDMLStmt *&stmt,
                    bool is_trans_stmt,
                    double &plan_cost,
                    bool &is_expected,
                    void *check_ctx = NULL);

  int prepare_eval_cost_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             ObDMLStmt &stmt,
                             ObDMLStmt *&copied_stmt,
                             bool is_trans_stmt);

  int prepare_root_stmt_with_temp_table_filter(ObDMLStmt &root_stmt, ObDMLStmt *&root_stmt_with_filter);

  virtual int is_expected_plan(ObLogPlan *plan,
                               void *check_ctx,
                               bool is_trans_plan,
                               bool& is_valid);

  bool skip_move_trans_loc() const
  {
    return TEMP_TABLE_OPTIMIZATION == transformer_type_ // move trans loc in transform rule
           || POST_PROCESS == transformer_type_ // need not move trans loc
           || PRE_PROCESS == transformer_type_ // need not move trans loc
           || AGGR_SUBQUERY == transformer_type_; // move trans loc in transform rule
  }

  bool skip_adjust_qb_name() const   // qb name adjust by transform rule
  {
    return QUERY_PUSH_DOWN == transformer_type_
           || PROJECTION_PRUNING == transformer_type_
           || PREDICATE_MOVE_AROUND == transformer_type_;
  }

  DISALLOW_COPY_AND_ASSIGN(ObTransformRule);

protected:
  ObTransformerCtx *ctx_;
  TransMethod transform_method_;
  const ObItemType hint_type_;
  uint64_t transformer_type_;
  bool trans_happened_;
  bool cost_based_trans_tried_;
  ObDMLStmt::TempTableInfo *current_temp_table_;

private:
  double stmt_cost_;
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* _OCEANBASE_SQL_REWRITE_RULE_H */

