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

#ifndef _OB_RAW_EXPR_TYPE_DEMOTION_H
#define _OB_RAW_EXPR_TYPE_DEMOTION_H 1

#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{

class ObRawExprTypeDemotion
{
public:
  /*
   * When comparing constants with columns, the range placement indicates the position of the
   * constant within the column range. Currently, we only have two types:
   * `rp_inside` and `rp_outside`. These represent whether the constant can be lossless converted
   * to the column type. In the future, we may have multiple range placements to support
   * condition rewriting, similar to MySQL's constant-folding-optimization behavior.
   */
  enum RangePlacement
  {
    RP_OUTSIDE = 0,
    RP_INSIDE
  };

public:
  // The Type Demotion for comparison types currently serves to optimize comparisons between
  // constants and columns by downgrading the constant's type to match the column's type.
  // This avoids adding a cast to the column, thereby enhancing query performance.
  ObRawExprTypeDemotion(const sql::ObSQLSessionInfo *session, ObRawExprFactory *expr_factory)
      : session_(session), expr_factory_(expr_factory), query_ctx_(NULL),
        is_batched_multi_stmt_(false)
  {}
  virtual ~ObRawExprTypeDemotion() {}
  int demote_type(ObOpRawExpr &expr);
  static bool need_constraint(const ObObjType from_type, const ObObjType &to_type);

private:
  int init_query_ctx_flags(bool &disabled);
  bool type_can_demote(const ObExprResType &from, const ObExprResType &to, const bool is_range);
  bool expr_can_demote(const ObConstRawExpr &from, const ObColumnRefRawExpr &to,
                       const bool is_range);
  int demote_type_common_comparison(ObOpRawExpr &expr);
  int extract_cmp_expr_pair(const ObRawExpr *left,
                            const ObRawExpr *right,
                            const ObColumnRefRawExpr *&column_ref,
                            const ObConstRawExpr *&const_value,
                            int64_t &constant_expr_idx);
  int try_demote_constant_type(const ObColumnRefRawExpr &column_ref,
                               const ObConstRawExpr &const_value,
                               const bool is_range_cmp,
                               ObOpRawExpr &op_expr,
                               int64_t replaced_expr_idx);
  int demote_type_in_or_not_in(ObOpRawExpr &expr);
  bool op_row_params_is_all_const(const ObOpRawExpr &op_row) const;
  int add_range_placement_constraint(const ObColumnRefRawExpr &column_ref,
                                     const ObConstRawExpr &const_expr,
                                     const RangePlacement rp);

private:
  const sql::ObSQLSessionInfo *session_;
  ObRawExprFactory *expr_factory_;
  ObQueryCtx *query_ctx_;
  bool is_batched_multi_stmt_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RAW_EXPR_TYPE_DEMOTION_H */
