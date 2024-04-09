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

#ifndef OB_TRANSFORM_UDT_UTILS_H_
#define OB_TRANSFORM_UDT_UTILS_H_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{

namespace sql
{

class ObTransformUdtUtils
{
public:
  static int transform_query_udt_columns_exprs(ObTransformerCtx *ctx, const ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *stmt, bool &trans_happened);
  static int transform_udt_columns_constraint_exprs(ObTransformerCtx *ctx, ObDMLStmt *stmt, bool &trans_happened);
  static int transform_udt_dml_stmt(ObTransformerCtx *ctx, ObDMLStmt *stmt, bool &trans_happened);
private:
  static int transform_udt_column_conv_function(ObTransformerCtx *ctx, ObDMLStmt *stmt,
                                                ObIArray<ObColumnRefRawExpr*> &column_exprs,
                                                ObIArray<ObRawExpr*> &column_conv_exprs,
                                                ObColumnRefRawExpr &udt_col,
                                                ObIArray<ObColumnRefRawExpr *> &hidd_cols,
                                                ObRawExpr *value_expr = NULL);
  static int transform_sys_makexml(ObTransformerCtx *ctx, ObRawExpr *hidden_blob_expr, ObRawExpr *&new_expr);
  static int replace_udt_assignment_exprs(ObTransformerCtx *ctx, ObDMLStmt *stmt, ObDmlTableInfo &table_info,
                                          ObIArray<ObAssignment> &assignments, bool &trans_happened);
  static int set_hidd_col_not_null_attr(const ObColumnRefRawExpr &udt_col, ObIArray<ObColumnRefRawExpr *> &column_exprs);
  static int transform_returning_exprs(ObTransformerCtx *ctx, ObDelUpdStmt *stmt, ObInsertTableInfo *table_info);
  static bool is_in_values_desc(const uint64_t column_id, const ObInsertTableInfo &table_info, uint64_t &idx);
  static int transform_replace_udt_column_convert_value(ObDmlTableInfo &table_info,
                                                        ObIArray<ObColumnRefRawExpr*> &column_exprs,
                                                        uint64_t udt_set_id,
                                                        ObIArray<ObRawExpr*> &column_conv_exprs);
  static bool check_assign_value_from_same_table(const ObColumnRefRawExpr &udt_col, const ObRawExpr &udt_value, uint64_t &udt_set_id);
  static int transform_xml_value_expr_inner(ObTransformerCtx *ctx, ObDMLStmt *stmt, ObDmlTableInfo &table_info, ObRawExpr *&old_expr);
  static int transform_udt_hidden_column_conv_function_inner(ObTransformerCtx *ctx,
                                                             ObDMLStmt *stmt,
                                                             ObIArray<ObColumnRefRawExpr*> &column_exprs,
                                                             ObIArray<ObRawExpr*> &column_conv_exprs,
                                                             ObColumnRefRawExpr &udt_col,
                                                             ObColumnRefRawExpr &targe_col,
                                                             uint32_t attr_idx = 0,
                                                             int64_t schema_version = OB_INVALID_VERSION,
                                                             ObRawExpr *udt_value = NULL);
  static int transform_make_xml_binary(ObTransformerCtx *ctx, ObRawExpr *old_expr, ObRawExpr *&new_expr);
  static int transform_udt_assignments(ObTransformerCtx *ctx, ObDMLStmt *stmt, ObDmlTableInfo &table_info,
                                       ObColumnRefRawExpr &udt_col, ObRawExpr *udt_value,
                                       common::ObArray<ObColumnRefRawExpr*> &hidden_cols, ObIArray<ObAssignment> &assignments,
                                       ObAssignment &udt_assign);
  static int check_skip_child_select_view(const ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *stmt, bool &skip_for_view_table);
  static int get_update_generated_udt_in_parent_stmt(const ObIArray<ObParentDMLStmt> &parent_stmts, const ObDMLStmt *stmt,
                                                     ObIArray<ObColumnRefRawExpr*> &col_exprs);
  static int get_dml_view_col_exprs(const ObDMLStmt *stmt, ObIArray<ObColumnRefRawExpr*> &assign_col_exprs);
  static int create_udt_hidden_columns(ObTransformerCtx *ctx,
                                       ObDMLStmt *stmt,
                                       const ObColumnRefRawExpr &udt_expr,
                                       ObIArray<ObColumnRefRawExpr*> &col_exprs,
                                       bool &need_transform);

};

}
}

#endif /* OB_TRANSFORM_PRE_PROCESS_H_ */