/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_SHARED_EXPR_RESOLVER_H
#define OB_SHARED_EXPR_RESOLVER_H

#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase {
namespace sql {

struct JoinedTable;
struct TableItem;
struct ObQuestionmarkEqualCtx : public ObExprEqualCheckContext
{
  ObQuestionmarkEqualCtx(bool need_check_deterministic = false):
    ObExprEqualCheckContext(need_check_deterministic)
  {
    override_const_compare_ = true;
  }

  bool compare_const(const ObConstRawExpr &left,
                     const ObConstRawExpr &right);

  ObSEArray<ObPCParamEqualInfo, 4> equal_pairs_;
};

struct ObRawExprEntry
{
  ObRawExprEntry()  :expr_(NULL), stmt_scope_(0), hash_code_(0)
  { }

  ObRawExprEntry(ObRawExpr *expr, uint64_t scope_id, uint64_t hash_code)
    : expr_(expr), stmt_scope_(scope_id), hash_code_(hash_code)
  {}

  bool compare(const ObRawExprEntry &node,
               ObQuestionmarkEqualCtx &cmp_ctx) const;

  TO_STRING_KV(K_(hash_code), K_(expr));

  ObRawExpr *expr_;
  uint64_t stmt_scope_;
  uint64_t hash_code_;
};

class ObSharedExprResolver
{
public:
  ObSharedExprResolver(ObQueryCtx *query_ctx)
    : allocator_("MergeSharedExpr"),
      scope_id_(0),
      query_ctx_(query_ctx)
  {}

  virtual ~ObSharedExprResolver();

  int get_shared_instance(ObRawExpr *expr,
                          ObRawExpr *&shared_expr,
                          bool &is_new);

  int add_new_instance(ObRawExprEntry &entry);

  uint64_t get_scope_id() const { return scope_id_; }

  void set_new_scope() { scope_id_ ++; }

  void revert_scope() { scope_id_ = 0; }

  uint64_t hash_expr_tree(ObRawExpr *expr, uint64_t hash_code, const bool is_root = true);

private:

  int inner_get_shared_expr(ObRawExprEntry &entry,
                            ObRawExpr *&new_expr);

private:
  typedef ObSEArray<ObRawExprEntry, 1> SharedExprs;

  ObArenaAllocator allocator_;
  hash::ObHashMap<uint64_t, SharedExprs *> shared_expr_map_;

  uint64_t scope_id_;

  ObQueryCtx *query_ctx_;
};

}
}
#endif // OB_SHARED_EXPR_RESOLVER_H
