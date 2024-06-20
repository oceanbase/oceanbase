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

#ifndef _OB_TRANSFORMER_IMPL_H
#define _OB_TRANSFORMER_IMPL_H 1

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "sql/parser/parse_node.h"
#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObDMLStmt;
class ObSelectStmt;

#define APPLY_RULE_IF_NEEDED(t, c)                                  \
  do {                                                              \
     if (OB_FAIL(ret)) {                                            \
     } else if (OB_FAIL(transform_one_rule<c>(stmt,                 \
                                              needed_types,         \
                                              t,                    \
                                              #c,                   \
                                              trans_happened))) {   \
       LOG_WARN("failed to transform one rule", K(ret));            \
     }                                                              \
  } while (0);

class ObTransformerImpl
{
  static const int64_t DEFAULT_ITERATION_COUNT = 10;
  static const int64_t MAX_RULE_COUNT = 64;
public:
  ObTransformerImpl(ObTransformerCtx *ctx)
    : ctx_(ctx),
      needed_transform_types_(ObTransformRule::ALL_TRANSFORM_RULES),
      max_iteration_count_(ObTransformerImpl::DEFAULT_ITERATION_COUNT)
  {
    memset(trans_count_, 0, sizeof(trans_count_));
  }
  virtual ~ObTransformerImpl()
  {
  }
  int transform(ObDMLStmt *&stmt);
  int do_transform(ObDMLStmt *&stmt);
  int do_transform_pre_precessing(ObDMLStmt *&stmt);
  int do_transform_dblink_write(ObDMLStmt *&stmt, bool &trans_happened);
  int do_transform_dblink_read(ObDMLStmt *&stmt);
  int transform_heuristic_rule(ObDMLStmt *&stmt);
  int transform_rule_set(ObDMLStmt *&stmt,
                         uint64_t needed_types,
                         int64_t iteration_count,
                         bool &trans_happened);
  int transform_rule_set_in_one_iteration(ObDMLStmt *&stmt,
                                          uint64_t needed_types,
                                          bool &trans_happened);
  int do_after_transform(ObDMLStmt *stmt);
  int get_all_stmts(ObDMLStmt *stmt,
                    ObIArray<ObDMLStmt*> &all_stmts);
  int add_param_and_expr_constraints(ObExecContext &exec_ctx,
                                     ObTransformerCtx &trans_ctx,
                                     ObDMLStmt &stmt);
  int add_all_rowkey_columns_to_stmt(ObDMLStmt *stmt);
  int add_all_rowkey_columns_to_stmt(const ObTableSchema &table_schema,
                                     const TableItem &table_item,
                                     ObRawExprFactory &expr_factory,
                                     ObDMLStmt &stmt,
                                     ObIArray<ColumnItem> &column_items);

  int add_trans_happended_hints(ObQueryCtx &query_ctx, ObTransformerCtx &trans_ctx);

  static inline bool is_type_needed(uint64_t needed_transform_types,
                                    TRANSFORM_TYPE type)
  {
    return (needed_transform_types & (1L << type)) != 0;
  }

  void clear_needed_types()
  {
    needed_transform_types_ = 0;
  }

  void add_needed_types(TRANSFORM_TYPE type)
  {
    needed_transform_types_ |= (1L << type);
  }

  inline int64_t get_max_iteration_count()
  {
    return max_iteration_count_;
  }
  inline void set_max_iteration_count(int64_t max_iteration_count)
  {
    max_iteration_count_ = max_iteration_count;
  }
  int get_cost_based_trans_happened(TRANSFORM_TYPE type, bool &trans_happened) const;

  int choose_rewrite_rules(ObDMLStmt *stmt, uint64_t &need_types);

  struct StmtFunc {
    StmtFunc () :
      contain_hie_query_(false),
      contain_sequence_(false),
      contain_for_update_(false),
      update_global_index_(false),
      contain_unpivot_query_(false),
      contain_enum_set_values_(false),
      contain_geometry_values_(false),
      contain_link_table_(false),
      contain_json_table_(false),
      contain_fulltext_search_(false),
      contain_dml_with_doc_id_(false)
    {}

    bool all_found() const {
      return contain_hie_query_ &&
          contain_sequence_ &&
          contain_for_update_ &&
          update_global_index_ &&
          contain_unpivot_query_ &&
          contain_enum_set_values_ &&
          contain_geometry_values_ &&
          contain_link_table_ &&
          contain_json_table_ &&
          contain_fulltext_search_ &&
          contain_dml_with_doc_id_;
    }

    bool contain_hie_query_;
    bool contain_sequence_;
    bool contain_for_update_;
    bool update_global_index_;
    bool contain_unpivot_query_;
    bool contain_enum_set_values_;
    bool contain_geometry_values_;
    bool contain_link_table_;
    bool contain_json_table_;
    bool contain_fulltext_search_;
    bool contain_dml_with_doc_id_;
  };
  static int check_stmt_functions(const ObDMLStmt *stmt, StmtFunc &func);
  int check_temp_table_functions(ObDMLStmt *stmt, StmtFunc &func);
  inline ObTransformerCtx *get_trans_ctx() { return ctx_; }
private:

  int collect_trans_stat(const ObTransformRule &rule);
  int get_stmt_trans_info(ObDMLStmt *stmt, bool is_root);
  void print_trans_stat();

  int finalize_exec_params(ObDMLStmt *stmt);

  int finalize_exec_params(ObDMLStmt *stmt, ObIArray<ObExecParamRawExpr*> & exec_params);
  /**
   * @brief adjust_global_dependency_tables
   * 为pl收集依赖表的schema version信息
   */
  int adjust_global_dependency_tables(ObDMLStmt *stmt);
  int verify_all_stmt_exprs(ObDMLStmt *stmt);
  int verify_stmt_exprs(ObDMLStmt *stmt);

  template<typename T>
  int transform_one_rule(ObDMLStmt *&stmt,
                        uint64_t needed_types,
                        TRANSFORM_TYPE type,
                        const char *rule_name,
                        bool &trans_happened);

  int transform_random_order(ObDMLStmt *&stmt,
                             ObQueryCtx *query_ctx,
                             uint64_t need_types,
                             int iter_count);

  static int get_random_order_array(uint64_t need_types,
                                    ObQueryCtx *query_ctx,
                                    ObArray<uint64_t> &need_types_array);

private:
  ObTransformerCtx *ctx_;
  uint64_t needed_transform_types_;
  int64_t max_iteration_count_;
  int64_t trans_count_[MAX_RULE_COUNT];
};

template<typename T>
int ObTransformerImpl::transform_one_rule(ObDMLStmt *&stmt,
                                          uint64_t needed_types,
                                          TRANSFORM_TYPE type,
                                          const char *rule_name,
                                          bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rule_name) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (is_type_needed(needed_types & needed_transform_types_,
                            type)) {
    SMART_VAR(T, trans, ctx_) {
      trans.set_transformer_type(type);
      OPT_TRACE_TITLE("start transform rule", rule_name);
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status fail", K(ret));
      } else if (OB_FAIL(trans.transform(stmt, needed_transform_types_))) {
        LOG_WARN("failed to transform a rewrite rule", "class", rule_name, K(ret), K(ctx_->outline_trans_hints_));
      } else if (OB_FAIL(collect_trans_stat(trans))) {
        LOG_WARN("failed to collect transform stat", K(ret));
      } else {
        trans_happened |= trans.get_trans_happened();
        OPT_TRACE_TIME_USED;
        OPT_TRACE_MEM_USED;
        LOG_TRACE("succeed to transform a rewrite rule", "class",
                  rule_name, K(trans.get_trans_happened()), K(ret));
      }
    }
  } else {
    LOG_TRACE("skip tranform a rewrite rule", "class", rule_name);
  }
  return ret;
}

}
}

#endif /* _OB_TRANSFORMER_IMPL_H */
