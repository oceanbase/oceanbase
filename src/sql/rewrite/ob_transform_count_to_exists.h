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

#ifndef _OB_TRANSFORM_COUNT_TO_EXISTS_H
#define _OB_TRANSFORM_COUNT_TO_EXISTS_H

#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase
{
namespace sql
{
class ObTransformCountToExists : public ObTransformRule
{
private:
  typedef enum TransType {
      TRANS_INVALID = -1,
      TRANS_EXISTS = 0,
      TRANS_NOT_EXISTS = 1
    } TransType;
  struct TransParam {
    TransParam()
      : subquery_expr_(NULL),
        const_expr_(NULL),
        count_param_(NULL),
        source_expr_(NULL),
        target_expr_(NULL),
        need_add_constraint_(false),
        trans_type_(TRANS_INVALID) {}
    virtual ~TransParam() {}
    ObQueryRefRawExpr *subquery_expr_;
    ObRawExpr *const_expr_;        // const expr within count(const)
    ObRawExpr *count_param_;          // expr within count(expr)
    ObRawExpr *source_expr_;       // cond expr that needs transformation
    ObRawExpr *target_expr_;       // target expr built for transformation
    bool need_add_constraint_;
    TransType trans_type_;

    void reset()
    {
      subquery_expr_ = NULL;
      const_expr_ = NULL;
      count_param_ = NULL;
      source_expr_ = NULL;
      target_expr_ = NULL;
      need_add_constraint_ = false;
      trans_type_ = TRANS_INVALID;
    }
    int assign(const TransParam &other);
    TO_STRING_KV(K(subquery_expr_),
                 K(const_expr_),
                 K(count_param_),
                 K(source_expr_),
                 K(target_expr_),
                 K(need_add_constraint_),
                 K(trans_type_));
  };
public:
  explicit ObTransformCountToExists(ObTransformerCtx *ctx) :
    ObTransformRule(ctx, TransMethod::POST_ORDER, T_COUNT_TO_EXISTS) {}
  virtual ~ObTransformCountToExists() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
private:
  int collect_trans_params(ObDMLStmt *stmt, const ObIArray<ObRawExpr *> &cond_exprs, ObIArray<TransParam> &trans_params);
  int get_trans_type(ObRawExpr *expr, ObRawExpr *&val_param, ObRawExpr *&subquery_param, TransType &trans_type);
  int check_trans_valid(ObDMLStmt *stmt, ObRawExpr *expr, TransParam &trans_param, bool &is_valid);
  int check_hint_valid(ObDMLStmt &stmt, ObSelectStmt &subquery, bool &is_valid);
  int check_sel_expr_valid(ObRawExpr *select_expr, ObRawExpr *&count_param, bool &is_sel_expr_valid);
  int check_value_zero(ObRawExpr *expr, bool &need_add_constraint, bool &is_valid);
  int do_transform(ObDMLStmt *stmt, const ObIArray<TransParam> &trans_params);
};
} /* namespace sql */
} /* namespace oceanbase */

#endif /* _OB_TRANSFORM_COUNT_TO_EXISTS_H */