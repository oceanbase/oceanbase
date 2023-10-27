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

#define USING_LOG_PREFIX SQL_RESV
#include "common/ob_smart_call.h"
#include "sql/resolver/expr/ob_shared_expr_resolver.h"
#include "sql/ob_sql_context.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log_print_kv.h"

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::lib;

bool ObQuestionmarkEqualCtx::compare_const(const ObConstRawExpr &left,
                                           const ObConstRawExpr &right)
{
  int &ret = err_code_;
  bool bret = false;
  if (left.get_expr_type() != right.get_expr_type() ||
      left.get_result_type() != right.get_result_type() ||
      (left.get_result_type().is_ext()
        && left.get_result_type().get_extend_type() > 0
        && left.get_result_type().get_extend_type() < T_EXT_SQL_ARRAY) ||
      left.get_result_type().is_user_defined_sql_type() ||
      OB_SUCCESS != err_code_) {
    // do nothing
  } else if (left.get_expr_type() != T_QUESTIONMARK) {
    bret = left.get_value().strict_equal(right.get_value());
  } else {
    bret = !left.get_result_type().get_param().is_null() &&
           !right.get_result_type().get_param().is_null() &&
        left.get_result_type().get_param().strict_equal(
          right.get_result_type().get_param());
    if (bret) {
      ObPCParamEqualInfo equal_info;
      equal_info.first_param_idx_ = left.get_value().get_unknown();
      equal_info.second_param_idx_ = right.get_value().get_unknown();
      if (equal_info.first_param_idx_ != equal_info.second_param_idx_) {
        if (OB_FAIL(equal_pairs_.push_back(equal_info))) {
          LOG_WARN("failed to push back equal_info", K(ret));
        }
      }
    }
  }
  return bret;
}

bool ObRawExprEntry::compare(const ObRawExprEntry &node,
                             ObQuestionmarkEqualCtx &cmp_ctx) const
{
  bool bret = false;
  if (NULL != expr_ && NULL != node.expr_ &&
      stmt_scope_ == node.stmt_scope_) {
    bret = (expr_ == node.expr_) ||
           (expr_->same_as(*node.expr_, &cmp_ctx));
    LOG_TRACE("compare expr entry",
              KPNAME(*expr_), KPNAME(*node.expr_), K(bret));
  }
  return bret;
}

uint64_t ObSharedExprResolver::hash_expr_tree(ObRawExpr *expr, uint64_t hash_code, const bool is_root)
{
  if (OB_LIKELY(NULL != expr)) {
    hash_code = common::do_hash(expr->get_expr_type(), hash_code);
    if (expr->get_expr_type() == T_QUESTIONMARK) {
      // do nothing
    } else if (expr->is_const_raw_expr()) {
      hash_code = common::do_hash(static_cast<ObConstRawExpr *>(expr)->get_value(),
                                  hash_code);
    } else if (expr->get_expr_type() == T_FUN_SYS_CAST) {
      hash_code = hash_expr_tree(expr->get_param_expr(0), hash_code, false);
    } else if (!is_root) {
      hash_code = common::do_hash((uintptr_t) (expr), hash_code);
    } else {
      for (int64_t i = 0; i < expr->get_param_count(); ++i) {
        ObRawExpr *param = expr->get_param_expr(i);
        if (NULL != param && (param->is_const_raw_expr() ||
                              param->get_expr_type() == T_FUN_SYS_CAST)) {
          hash_code = hash_expr_tree(param, hash_code, false);
        } else {
          hash_code = common::do_hash(((uintptr_t) (param)), hash_code);
        }
      }
    }
  }
  return hash_code;
}

ObSharedExprResolver::~ObSharedExprResolver()
{
  if (shared_expr_map_.created()) {
    for (hash::ObHashMap<uint64_t, SharedExprs *>::iterator it = shared_expr_map_.begin();
         it != shared_expr_map_.end(); it ++) {
      SharedExprs *shared_expr = it->second;
      shared_expr->~SharedExprs();
      it->second = NULL;
    }
  }
}

int ObSharedExprResolver::add_new_instance(ObRawExprEntry &entry)
{
  int ret = OB_SUCCESS;
  SharedExprs *shared_exprs = NULL;
  void *ptr = NULL;
  LOG_TRACE("add new shared expr", KPNAME(*entry.expr_), K(entry.hash_code_));
  if (OB_UNLIKELY(!shared_expr_map_.created()) &&
      OB_FAIL(shared_expr_map_.create(128, "MergeSharedExpr"))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else if (OB_FAIL(shared_expr_map_.get_refactored(entry.hash_code_,
                                                     shared_exprs))) {
    if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
      LOG_WARN("failed to get entry from map", K(ret));
    } else if (OB_ISNULL(ptr = allocator_.alloc(sizeof(SharedExprs)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      shared_exprs = new (ptr) SharedExprs();
      if (OB_FAIL(shared_expr_map_.set_refactored(entry.hash_code_,
                                                  shared_exprs))) {
        LOG_WARN("failed to add shared expr into map", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(shared_exprs->push_back(entry))) {
      LOG_WARN("failed to push back entry", K(ret));
    }
  }
  return ret;
}

int ObSharedExprResolver::get_shared_instance(ObRawExpr *expr,
                                              ObRawExpr *&shared_expr,
                                              bool &is_new)
{
  int ret = OB_SUCCESS;
  shared_expr = NULL;
  is_new = false;
  bool has_new_param = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (!expr->is_aggr_expr() && !expr->is_win_func_expr()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr *old_param_expr = expr->get_param_expr(i);
      ObRawExpr *new_param_expr = NULL;
      bool is_param_new = false;
      if (old_param_expr->get_expr_type() == T_QUESTIONMARK ||
          (expr->get_expr_type() == T_FUN_SYS_CAST && i == 1)) {
        // skip exec var and question mark
      } else if (OB_FAIL(SMART_CALL(get_shared_instance(old_param_expr,
                                                        new_param_expr,
                                                        is_param_new)))) {
        LOG_WARN("failed to get shared instance", K(ret));
      } else {
        expr->get_param_expr(i) = new_param_expr;
        has_new_param = has_new_param || is_param_new;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (expr->is_column_ref_expr() ||
        expr->is_aggr_expr() ||
        expr->is_win_func_expr() ||
        expr->is_query_ref_expr() ||
        expr->is_exec_param_expr() ||
        expr->is_pseudo_column_expr() ||
        expr->get_expr_type() == T_OP_ROW ||
        expr->get_expr_type() == T_QUESTIONMARK) {
      shared_expr = expr;
    } else {
      ObRawExprEntry entry(expr, get_scope_id(), hash_expr_tree(expr, get_scope_id()));
      ObRawExpr *new_expr = NULL;
      if (!has_new_param &&
          OB_FAIL(inner_get_shared_expr(entry, new_expr))) {
        LOG_WARN("failed to get shared expr entry", K(ret));
      } else if (NULL != new_expr) {
        shared_expr = new_expr;
      } else if (OB_FAIL(add_new_instance(entry))) {
        LOG_WARN("failed to add new instance", K(ret));
      } else {
        shared_expr = entry.expr_;
        is_new = true;
      }
    }
  }
  return ret;
}

int ObSharedExprResolver::inner_get_shared_expr(ObRawExprEntry &entry,
                                                ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  if (OB_LIKELY(shared_expr_map_.created())) {
    SharedExprs *shared_exprs = NULL;
    if (OB_FAIL(shared_expr_map_.get_refactored(entry.hash_code_,
                                                shared_exprs))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get entry from map", K(ret));
      }
    } else if (OB_ISNULL(shared_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shared expr is null", K(ret));
    } else {
      ObQuestionmarkEqualCtx cmp_ctx(/*need_check_deterministic = */true);
      for (int64_t i = 0; OB_SUCC(cmp_ctx.err_code_) && i < shared_exprs->count(); ++i) {
        if (shared_exprs->at(i).compare(entry, cmp_ctx)) {
          new_expr = shared_exprs->at(i).expr_;
          break;
        } else {
          cmp_ctx.equal_pairs_.reuse();
        }
      }
      if (OB_SUCCESS != cmp_ctx.err_code_) {
        ret = cmp_ctx.err_code_;
        LOG_WARN("compare expr failed", K(ret));
      } else if (NULL == new_expr || cmp_ctx.equal_pairs_.empty()) {
        // do nothing
      } else if (OB_ISNULL(query_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query ctx is not provide", K(ret));
      } else if (OB_FAIL(append(query_ctx_->all_equal_param_constraints_,
                                cmp_ctx.equal_pairs_))) {
        LOG_WARN("failed to add equal constraint", K(ret));
      }
    }
  }
  return ret;
}
