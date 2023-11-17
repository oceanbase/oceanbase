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

#ifndef _OB_TRANSFORMDECORRELATE_H
#define _OB_TRANSFORMDECORRELATE_H

#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase {
namespace sql {

class ObTransformDecorrelate : public ObTransformRule
{
public:
  ObTransformDecorrelate(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_DECORRELATE)
  {}
  virtual ~ObTransformDecorrelate() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int transform_one_stmt_with_outline(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                              ObDMLStmt *&stmt,
                                              bool &trans_happened) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
  virtual int check_hint_status(const ObDMLStmt &stmt, bool &need_trans) override;
protected:

  int decorrelate_lateral_derived_table(ObDMLStmt *stmt,
                                        ObIArray<ObSelectStmt*> &decorrelate_stmts,
                                        bool &trans_happened);

  int decorrelate_aggr_lateral_derived_table(ObDMLStmt *stmt,
                                             ObIArray<ObSelectStmt*> &decorrelate_stmts,
                                             bool &trans_happened);

  int transform_lateral_inline_view(ObDMLStmt *parent_stmt,
                                    TableItem *table_item,
                                    ObIArray<ObSelectStmt*> &decorrelate_stmts,
                                    bool can_push_where,
                                    JoinedTable *joined_table,
                                    bool &trans_happened);

  int transform_joined_table(ObDMLStmt *parent_stmt,
                             JoinedTable *joined_table,
                             bool parent_can_push_where,
                             ObIArray<ObSelectStmt*> &decorrelate_stmts,
                             bool &trans_happened);

  int check_transform_validity(ObDMLStmt *stmt,
                               ObSelectStmt *ref_query,
                               TableItem *table_item,
                               bool can_push_where,
                               JoinedTable *joined_table,
                               bool &is_valid,
                               bool &need_create_spj);

  int check_lateral_inline_view_validity(TableItem *table_item,
                                         ObSelectStmt *ref_query,
                                         bool &is_valid);

  int check_hint_allowed_decorrelate(ObDMLStmt &stmt,
                                     ObDMLStmt &ref_query,
                                     bool &allowed);

  int do_transform_lateral_inline_view(ObDMLStmt *stmt,
                                       ObSelectStmt *ref_query,
                                       TableItem *table_item,
                                       bool can_push_where,
                                       JoinedTable *joined_table,
                                       bool need_create_spj);

  int transform_aggr_lateral_inline_view(ObDMLStmt *parent_stmt,
                                         TableItem *table_item,
                                         ObIArray<ObSelectStmt*> &decorrelate_stmts,
                                         ObIArray<FromItem> &from_item_list,
                                         ObIArray<JoinedTable*> &joined_table_list,
                                         bool &trans_happened);

  int check_transform_aggr_validity(ObDMLStmt *stmt,
                                    ObSelectStmt *ref_query,
                                    TableItem *table_item,
                                    ObIArray<ObRawExpr*> &pullup_conds,
                                    bool &is_valid);

  int check_transform_aggr_condition_validity(ObDMLStmt *stmt,
                                              ObSelectStmt *ref_query,
                                              TableItem *table_item,
                                              ObIArray<ObRawExpr*> &pullup_conds,
                                              bool &is_valid);

  int do_transform_aggr_lateral_inline_view(ObDMLStmt *stmt,
                                            ObSelectStmt *ref_query,
                                            TableItem *table_item,
                                            ObIArray<ObRawExpr*> &pullup_conds,
                                            ObIArray<FromItem> &from_item_list,
                                            ObIArray<JoinedTable*> &joined_table_list);

  int gather_select_item_null_propagate(ObSelectStmt *ref_query,
                                        ObIArray<bool> &is_null_prop);

  int transform_from_list(ObDMLStmt &stmt,
                          TableItem *view_table_item,
                          bool use_outer_join,
                          const ObIArray<ObRawExpr *> &joined_conds,
                          ObIArray<FromItem> &from_item_list,
                          ObIArray<JoinedTable*> &joined_table_list);

  bool is_valid_group_by(const ObSelectStmt &subquery);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransformDecorrelate);
};

}
}

#endif // _OB_TRANSFORMDECORRELATE_H