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

namespace oceanbase {
namespace sql {
class ObDMLStmt;
class ObSelectStmt;

#define APPLY_RULE_IF_NEEDED(t, c)                                                                              \
  do {                                                                                                          \
    if (OB_SUCC(ret) && ObTransformerImpl::is_type_needed(needed_types & needed_transform_types_, t)) {         \
      c trans(ctx_);                                                                                            \
      trans.set_transformer_type(t);                                                                            \
      if (OB_FAIL(THIS_WORKER.check_status())) {                                                                \
        LOG_WARN("check status fail", K(ret));                                                                  \
      } else if (OB_FAIL(trans.transform(stmt, needed_transform_types_))) {                                     \
        LOG_WARN("failed to transform a rewrite rule", "class", (#c), K(ret));                                  \
      } else if (OB_FAIL(collect_trans_stat(trans))) {                                                          \
        LOG_WARN("failed to collect transform stat", K(ret));                                                   \
      } else {                                                                                                  \
        trans_happened |= trans.get_trans_happened();                                                           \
        trans_happened_route |= trans.get_transformer_type();                                                   \
        LOG_TRACE("succeed to transform a rewrite rule", "class", (#c), K(trans.get_trans_happened()), K(ret)); \
      }                                                                                                         \
    }                                                                                                           \
  } while (0);

class ObTransformerImpl {
  static const int64_t DEFAULT_ITERATION_COUNT = 10;
  static const int64_t MAX_RULE_COUNT = 64;

public:
  ObTransformerImpl(ObTransformerCtx* ctx)
      : ctx_(ctx), needed_transform_types_(0), max_iteration_count_(ObTransformerImpl::DEFAULT_ITERATION_COUNT)
  {
    set_all_types();
    memset(trans_count_, 0, sizeof(trans_count_));
  }
  virtual ~ObTransformerImpl()
  {}
  int transform(ObDMLStmt*& stmt);
  int do_transform(ObDMLStmt*& stmt);
  int do_transform_pre_precessing(ObDMLStmt*& stmt);
  int do_transform_post_precessing(ObDMLStmt*& stmt);
  int transform_heuristic_rule(ObDMLStmt*& stmt);
  int transform_rule_set(ObDMLStmt*& stmt, uint64_t needed_types, int64_t iteration_count);
  int transform_rule_set_in_one_iteration(ObDMLStmt*& stmt, uint64_t needed_types, bool& trans_happened);
  int adjust_global_dependency_tables(ObDMLStmt* stmt);
  inline void set_all_types()
  {
    needed_transform_types_ = (TRANSFORM_TYPE_COUNT_PLUS_ONE - 1);
  }
  inline void clear_all_types()
  {
    needed_transform_types_ = 0;
  }
  inline void add_needed_type(TRANSFORM_TYPE type)
  {
    needed_transform_types_ |= type;
  }
  inline void remove_type(TRANSFORM_TYPE type)
  {
    needed_transform_types_ &= (~type);
  }
  inline bool is_type_needed(TRANSFORM_TYPE type)
  {
    return (needed_transform_types_ & type) != 0;
  }
  inline void set_needed_types(uint64_t needed_transfrom_types)
  {
    needed_transform_types_ = needed_transfrom_types;
  }
  inline int64_t get_max_iteration_count()
  {
    return max_iteration_count_;
  }
  inline void set_max_iteration_count(int64_t max_iteration_count)
  {
    max_iteration_count_ = max_iteration_count;
  }
  static inline bool is_type_needed(uint64_t needed_transform_types, TRANSFORM_TYPE type)
  {
    return (needed_transform_types & type) != 0;
  }
  int get_cost_based_trans_happened(TRANSFORM_TYPE type, bool& trans_happened) const;

  int choose_rewrite_rules(ObDMLStmt* stmt, uint64_t& need_types);

  struct StmtFunc {
    StmtFunc()
        : contain_hie_query_(false),
          contain_sequence_(false),
          contain_for_update_(false),
          contain_domain_index_(false),
          update_global_index_(false),
          contain_unpivot_query_(false)
    {}

    bool all_found() const
    {
      return contain_hie_query_ && contain_sequence_ && contain_for_update_ && contain_domain_index_ &&
             update_global_index_ && contain_unpivot_query_;
    }

    bool contain_hie_query_;
    bool contain_sequence_;
    bool contain_for_update_;
    bool contain_domain_index_;
    bool update_global_index_;
    bool contain_unpivot_query_;
  };
  int check_stmt_functions(ObDMLStmt* stmt, StmtFunc& func);
  inline ObTransformerCtx* get_trans_ctx()
  {
    return ctx_;
  }

private:
  int collect_trans_stat(const ObTransformRule& rule);

  void print_trans_stat();

private:
  ObTransformerCtx* ctx_;
  uint64_t needed_transform_types_;
  int64_t max_iteration_count_;
  int64_t trans_count_[MAX_RULE_COUNT];
};
}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_TRANSFORMER_IMPL_H */
