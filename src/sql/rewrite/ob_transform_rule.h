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
namespace oceanbase {
namespace common {
class ObIAllocator;
class ObAddr;
class ObStatManager;
class ObOptStatManager;
}  // namespace common
namespace share {
class ObIPartitionLocationCache;
}
namespace storage {
class ObPartitionService;
}
namespace sql {
class ObDMLStmt;
class ObSelectStmt;
class ObSQLSessionInfo;
class ObSchemaChecker;
class ObExecContext;
class ObRawExprFactory;
class ObStmtFactory;
class ObPhysicalPlan;
class ObCodeGeneratorImpl;

struct ObTransformerCtx {
  ObTransformerCtx()
      : allocator_(NULL),
        session_info_(NULL),
        schema_checker_(NULL),
        exec_ctx_(NULL),
        expr_factory_(NULL),
        stmt_factory_(NULL),
        partition_location_cache_(NULL),
        stat_mgr_(NULL),
        opt_stat_mgr_(NULL),
        partition_service_(NULL),
        sql_schema_guard_(NULL),
        self_addr_(NULL),
        phy_plan_(NULL),
        merged_version_(0),
        happened_cost_based_trans_(0),
        equal_sets_(),
        trans_happened_route_(),
        ignore_semi_infos_()
  {}
  virtual ~ObTransformerCtx()
  {}
  bool is_valid();
  common::ObIAllocator* allocator_;
  ObSQLSessionInfo* session_info_;
  ObSchemaChecker* schema_checker_;
  ObExecContext* exec_ctx_;
  ObRawExprFactory* expr_factory_;
  ObStmtFactory* stmt_factory_;
  share::ObIPartitionLocationCache* partition_location_cache_;
  common::ObStatManager* stat_mgr_;
  common::ObOptStatManager* opt_stat_mgr_;
  storage::ObPartitionService* partition_service_;
  ObSqlSchemaGuard* sql_schema_guard_;
  common::ObAddr* self_addr_;

  /* member below is from phy_plan */
  ObPhysicalPlan* phy_plan_;

  int64_t merged_version_;

  // record cost based transformers
  int64_t happened_cost_based_trans_;
  EqualSets equal_sets_;
  ObSEArray<uint64_t, 8, common::ModulePageAllocator, true> trans_happened_route_;
  ObSEArray<uint64_t, 8, common::ModulePageAllocator, true> ignore_semi_infos_;
};

enum TransMethod { POST_ORDER = 0, PRE_ORDER = 1, MAX_TRANSFORMATION_METHOD };

enum TRANSFORM_TYPE {
  INVALID_TRANSFORM_TYPE = 1 << 0,
  PRE_PROCESS = 1 << 1,
  POST_PROCESS = 1 << 2,
  SIMPLIFY = 1 << 3,
  ANYALL = 1 << 4,
  AGGR = 1 << 5,
  ELIMINATE_OJ = 1 << 6,
  VIEW_MERGE = 1 << 7,
  WHERE_SQ_PULL_UP = 1 << 8,
  QUERY_PUSH_DOWN = 1 << 9,
  AGGR_SUBQUERY = 1 << 10,
  SET_OP = 1 << 11,
  PROJECTION_PRUNING = 1 << 12,
  OR_EXPANSION = 1 << 13,
  WIN_MAGIC = 1 << 14,
  JOIN_ELIMINATION = 1 << 15,
  JOIN_AGGREGATION = 1 << 16,
  GROUPBY_PLACEMENT = 1 << 17,
  SUBQUERY_COALESCE = 1 << 18,
  WIN_GROUPBY = 1 << 19,
  PREDICATE_MOVE_AROUND = 1 << 20,
  NL_FULL_OUTER_JOIN = 1 << 21,
  SEMI_TO_INNER = 1 << 22,
  OUTERJOIN_LIMIT_PUSHDOWN = 1 << 23,
  TRANSFORM_TYPE_COUNT_PLUS_ONE = 1 << 24
};

struct ObParentDMLStmt {
  ObParentDMLStmt() : pos_(0), stmt_(NULL)
  {}
  virtual ~ObParentDMLStmt()
  {}
  int64_t pos_;
  ObDMLStmt* stmt_;
  TO_STRING_KV(K_(pos), K_(stmt));
};

class ObTransformRule {
public:
  static constexpr const double COST_BASE_TRANSFORM_THRESHOLD = 0.999;
  static const int64_t TRANSFORMER_DEFAULT_MAX_RECURSIVE_LEVEL = 150;
  static const uint64_t ALL_TRANSFORM_RULES = TRANSFORM_TYPE_COUNT_PLUS_ONE - 1;
  static const uint64_t ALL_HEURISTICS_RULES = SIMPLIFY | ANYALL | AGGR | ELIMINATE_OJ | VIEW_MERGE | WHERE_SQ_PULL_UP |
                                               QUERY_PUSH_DOWN | SET_OP | PROJECTION_PRUNING | JOIN_ELIMINATION |
                                               JOIN_AGGREGATION | WIN_GROUPBY | PREDICATE_MOVE_AROUND |
                                               NL_FULL_OUTER_JOIN | OUTERJOIN_LIMIT_PUSHDOWN;
  ObTransformRule(ObTransformerCtx* ctx, TransMethod transform_method)
      : ctx_(ctx),
        transform_method_(transform_method),
        transformer_type_(INVALID_TRANSFORM_TYPE),
        trans_happened_(false),
        cost_based_trans_tried_(false),
        stmt_cost_(-1)
  {}
  virtual ~ObTransformRule()
  {}
  virtual int transform(ObDMLStmt*& stmt, uint64_t& transform_types);

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

protected:
  /*
   * This function tries to recursively transform all statements including its child statements
   * By default, there are two options, pre-order transformation and post-order transformation
   * If any rule want to change this behavior, rewrite this function
   */
  virtual int transform_stmt_recursively(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, const int64_t current_level, ObDMLStmt*& stmt);
  /*
   * This function tries to transform one statement
   * parent_stmts: some rule may use parent statement to decide how to do rewriting, so keep it for use
   * Every rule should rewrite this function,
   * parameter stmt: input statement
   * parameter transformed_stmts: transformed statement
   */
  virtual int transform_one_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened) = 0;

  /*
   * this function is for cost-based rules,
   * mainly used to test whether the transformed stmt has a lower cost
   */
  int accept_transform(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, ObDMLStmt* trans_stmt, bool& trans_happened);

  /*
   * after transforming a rule, this function try to figure out which rules to apply in the next iteration
   */
  virtual int adjust_transform_types(uint64_t& transform_types);

private:
  // pre-order transformation
  int transform_pre_order(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, const int64_t current_level, ObDMLStmt*& stmt);
  // post-order transformation
  int transform_post_order(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, const int64_t current_level, ObDMLStmt*& stmt);

  int transform_self(common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt);
  // transform non_set children statements
  int transform_children(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, const int64_t current_level, ObDMLStmt*& stmt);
  int adjust_transformed_stmt(
      common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt* stmt, ObDMLStmt*& transformed_stmt);

  int evaluate_cost(common::ObIArray<ObParentDMLStmt>& parent_stms, ObDMLStmt*& stmt, double& plan_cost);

  virtual bool need_rewrite(const common::ObIArray<ObParentDMLStmt>& parent_stmts, const ObDMLStmt& stmt);

  bool is_view_stmt(const ObIArray<ObParentDMLStmt>& parents, const ObDMLStmt& stmt);

  bool is_large_stmt(const ObDMLStmt& stmt);

  DISALLOW_COPY_AND_ASSIGN(ObTransformRule);

protected:
  ObTransformerCtx* ctx_;
  TransMethod transform_method_;
  uint64_t transformer_type_;
  bool trans_happened_;
  bool cost_based_trans_tried_;

private:
  double stmt_cost_;
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* _OCEANBASE_SQL_REWRITE_RULE_H */
