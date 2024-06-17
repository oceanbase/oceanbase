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

#ifndef _OB_RAW_EXPR_WRAP_ENUM_SET_H
#define _OB_RAW_EXPR_WRAP_ENUM_SET_H 1
#include "sql/resolver/expr/ob_raw_expr.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase
{
namespace sql
{
class ObDMLStmt;
class ObSelectStmt;
class ObInsertStmt;
class ObSQLSessionInfo;

class ObRawExprWrapEnumSet: public ObRawExprVisitor
{
public:
  ObRawExprWrapEnumSet(ObRawExprFactory &expr_factory, ObSQLSessionInfo *my_session)
    : ObRawExprVisitor(),
      cur_stmt_(nullptr),
      expr_factory_(expr_factory),
      my_session_(my_session)
  {}
  int wrap_enum_set(ObDMLStmt &stmt);
  int analyze_all_expr(ObDMLStmt &stmt);
  int analyze_expr(ObRawExpr *expr);
  int visit(ObConstRawExpr &expr);
  int visit(ObExecParamRawExpr &expr);
  int visit(ObVarRawExpr &expr);
  int visit(ObOpPseudoColumnRawExpr &expr);
  int visit(ObQueryRefRawExpr &expr);
  int visit(ObColumnRefRawExpr &expr);
  int visit(ObOpRawExpr &expr);
  int visit(ObCaseOpRawExpr &expr);
  int visit(ObAggFunRawExpr &expr);
  int visit(ObSysFunRawExpr &expr);
  int visit(ObSetOpRawExpr &expr);
  int visit(ObAliasRefRawExpr &expr);
  int visit(ObWinFunRawExpr &expr);
  int visit(ObPseudoColumnRawExpr &expr);
  int visit(ObPlQueryRefRawExpr &expr);
  int visit(ObMatchFunRawExpr &expr);
  bool skip_child();
private:
  int visit_left_expr(ObOpRawExpr &expr, int64_t row_dimension,
                      const common::ObIArray<ObExprCalcType> &cmp_types);
  int check_and_wrap_left(ObRawExpr &expr, int64_t idx,
                          const common::ObIArray<ObExprCalcType> &cmp_types,
                          int64_t row_dimension,
                          ObSysFunRawExpr *&wrapped_expr) const;
  int visit_right_expr(ObRawExpr &expr, int64_t row_dimension,
                       const common::ObIArray<ObExprCalcType> &cmp_types,
                       const ObItemType &root_type);
  int wrap_type_to_str_if_necessary(ObRawExpr *expr,
                                    common::ObObjType calc_type,
                                    bool is_same_need,
                                    ObSysFunRawExpr *&wrapped_expr);
  int wrap_target_list(ObSelectStmt &stmt);
  int wrap_sub_select(ObInsertStmt &stmt);
  int wrap_value_vector(ObInsertStmt &stmt);
  int wrap_nullif_expr(ObSysFunRawExpr &expr);
  bool can_wrap_type_to_str(const ObRawExpr &expr) const;
  int visit_query_ref_expr(ObQueryRefRawExpr &expr,
                           const common::ObObjType dest_type,
                           const bool is_same_need);
  int wrap_param_expr(ObIArray<ObRawExpr*> &param_exprs, ObObjType dest_typ);
private:
  ObDMLStmt *cur_stmt_;
  ObRawExprFactory &expr_factory_;
  ObSQLSessionInfo *my_session_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_RAW_EXPR_WRAP_ENUM_SET_H */
