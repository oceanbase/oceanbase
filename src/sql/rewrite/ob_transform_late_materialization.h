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

#ifndef OB_TRANSFORM_LATE_MATERIALIZATION_H
#define OB_TRANSFORM_LATE_MATERIALIZATION_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "lib/container/ob_bit_set.h"

namespace oceanbase
{


namespace sql
{

class ObRawExpr;
class ObTransformLateMaterialization : public ObTransformRule
{
public:
  explicit ObTransformLateMaterialization(ObTransformerCtx *ctx) :
    ObTransformRule(ctx, TransMethod::POST_ORDER, T_USE_LATE_MATERIALIZATION) {}
  virtual ~ObTransformLateMaterialization() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;

private:
  struct ObCostBasedLateMaterializationCtx {
    ObCostBasedLateMaterializationCtx()
      : late_material_indexs_(),
        check_sort_indexs_(),
        late_table_id_(common::OB_INVALID_ID),
        base_index_(common::OB_INVALID_ID) { }
    ObSEArray<uint64_t, 4> late_material_indexs_;
    ObSEArray<uint64_t, 2> check_sort_indexs_;  // global index of partition table is ok even if there is no sort op
    uint64_t late_table_id_;
    uint64_t base_index_;

    TO_STRING_KV(K_(late_material_indexs),
                 K_(check_sort_indexs),
                 K_(late_table_id),
                 K_(base_index));
  };

  struct ObLateMaterializationInfo
  {
    ObLateMaterializationInfo()
      :
        candi_index_names_(),
        candi_indexs_(),
        project_col_in_view_() { }
    ObSEArray<ObString, 4> candi_index_names_;
    ObSEArray<uint64_t, 4> candi_indexs_; // late materialization index + some may late materialization index
    ObSEArray<uint64_t, 4> project_col_in_view_;
    TO_STRING_KV(K_(candi_index_names),
                 K_(candi_indexs),
                 K_(project_col_in_view));
  };
  int check_hint_validity(const ObDMLStmt &stmt, bool &force_trans, bool &force_no_trans);
  int check_stmt_need_late_materialization(const ObSelectStmt &stmt, const bool force_accept, bool &need);
  int generate_late_materialization_info(const ObSelectStmt &stmt,
                                         ObLateMaterializationInfo &info,
                                         ObCostBasedLateMaterializationCtx &check_ctx);
  int extract_transform_column_ids(const ObSelectStmt &select_stmt,
                                   const ObTableSchema &table_schema,
                                   ObIArray<uint64_t> &key_col_ids,
                                   ObIArray<uint64_t> &filter_col_ids,
                                   ObIArray<uint64_t> &orderby_col_ids,
                                   ObIArray<uint64_t> &select_col_ids);
  int get_accessible_index(const ObSelectStmt &select_stmt,
                           const TableItem &table_item,
                           ObIArray<const ObTableSchema *> &index_schemas);

  bool match_index_name(const ObIArray<const ObTableSchema *> &index_schemas,
                        const TableItem &table_item,
                        const ObIndexHint &index_hint,
                        const ObCollationType cs_type,
                        int64_t &id);
  int contain_enum_set_rowkeys(const ObRowkeyInfo &rowkey_info, bool &contain);
  int check_index_match_late_materialization(const uint64_t index_id,
                                             const ObIArray<uint64_t> &index_col_ids,
                                             const ObIArray<uint64_t> &key_col_ids,
                                             const ObIArray<uint64_t> &filter_col_ids,
                                             const ObIArray<uint64_t> &orderby_col_ids,
                                             const ObIArray<uint64_t> &select_col_ids,
                                             const uint64_t ref_table_id,
                                             ObCostBasedLateMaterializationCtx &check_ctx,
                                             bool &is_match);
  int generate_late_materialization_stmt(const ObLateMaterializationInfo &info,
                                         ObDMLStmt *stmt,
                                         ObDMLStmt *&trans_stmt);
  int generate_late_materialization_view(const ObIArray<uint64_t> &select_col_ids,
                                         ObSelectStmt *select_stmt,
                                         ObSelectStmt *&view_stmt,
                                         TableItem *&table_item);
  int extract_replace_column_exprs(const ObSelectStmt &select_stmt,
                                   const ObSelectStmt &view_stmt,
                                   const uint64_t table_id,
                                   const uint64_t view_id,
                                   ObIArray<ObRawExpr*> &old_col_exprs,
                                   ObIArray<ObRawExpr*> &new_col_exprs);
  int generate_pk_join_conditions(const uint64_t ref_table_id,
                                  const uint64_t table_id,
                                  const ObIArray<ObRawExpr*> &old_col_exprs,
                                  const ObIArray<ObRawExpr*> &new_col_exprs,
                                  ObSelectStmt &select_stmt);
  int generate_late_materialization_hint(const ObLateMaterializationInfo &info,
                                         const TableItem &table_item,
                                         const TableItem &view_table,
                                         ObSelectStmt &select_stmt,
                                         ObSelectStmt &view_stmt);
  int inner_accept_transform(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             ObDMLStmt *&stmt,
                             bool force_accept,
                             ObLateMaterializationInfo &info,
                             ObCostBasedLateMaterializationCtx &check_ctx,
                             bool &trans_happened);
  int check_transform_plan_expected(ObLogicalOperator* top,
                                    ObCostBasedLateMaterializationCtx &ctx,
                                    bool &is_valid);
  int get_index_of_base_stmt_path(ObLogicalOperator* top, ObCostBasedLateMaterializationCtx &ctx);
  int evaluate_stmt_cost(ObIArray<ObParentDMLStmt> &parent_stmts,
                         ObDMLStmt *&stmt,
                         bool is_trans_stmt,
                         double &plan_cost,
                         bool &is_expected,
                         ObCostBasedLateMaterializationCtx &check_ctx);
  virtual int is_expected_plan(ObLogPlan *plan, void *check_ctx, bool is_trans_plan, bool &is_valid) override;
};

}
}

#endif // OB_TRANSFORM_LATE_MATERIALIZATION_H