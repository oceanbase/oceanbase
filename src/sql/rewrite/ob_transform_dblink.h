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

#ifndef OB_TRANSFORM_DBLINK_H_
#define OB_TRANSFORM_DBLINK_H_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/rewrite/ob_union_find.h"

namespace oceanbase
{
namespace sql
{
struct ObAssignment;
class ObTransformDBlink: public ObTransformRule
{
public:
  ObTransformDBlink(ObTransformerCtx *ctx)
    :ObTransformRule(ctx, TransMethod::POST_ORDER),
     transform_for_write_(false) {}

  virtual ~ObTransformDBlink() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  inline void set_transform_for_write(bool transform_for_write) { transform_for_write_ = transform_for_write; }
private:
  struct LinkTableHelper {
    LinkTableHelper()
      :dblink_id_(OB_INVALID_ID),
      is_reverse_link_(false),
      parent_table_(NULL),
      parent_semi_info_(NULL)
      {}
    TO_STRING_KV(
      K_(dblink_id),
      K_(is_reverse_link),
      K_(parent_table),
      K_(parent_semi_info),
      K_(table_items),
      K_(conditions),
      K_(semi_infos)
    );
    int64_t dblink_id_;
    bool is_reverse_link_;
    JoinedTable *parent_table_;
    SemiInfo* parent_semi_info_;
    ObSEArray<TableItem*, 4> table_items_;
    ObSEArray<ObRawExpr*, 4> conditions_;
    ObSEArray<SemiInfo*, 4> semi_infos_;
  };

  int reverse_link_table(ObDMLStmt *stmt, bool &trans_happened);

  int check_dml_link_valid(ObDMLStmt *stmt, uint64_t target_dblink_id);

  int get_table_assigns(ObDMLStmt *stmt, ObIArray<ObAssignment> &assigns);

  int check_has_default_value(ObIArray<ObAssignment> &assigns, bool &has_default);

  int get_target_dblink_id(ObDMLStmt *stmt, uint64_t &dblink_id);

  int recursive_get_target_dblink_id(ObSelectStmt *stmt, uint64_t &dblink_id);

  int inner_reverse_link_table(ObDMLStmt *stmt, uint64_t target_dblink_id);

  int reverse_one_link_table(TableItem *table, uint64_t target_dblink_id);

  int reverse_link_tables(ObIArray<TableItem *> &tables, uint64_t target_dblink_id);

  int reverse_link_tables(ObDMLStmt &stmt,
                          ObIArray<TableItem *> &tables,
                          ObIArray<SemiInfo*> &semi_infos,
                          uint64_t target_dblink_id);

  int reverse_link_sequence(ObDMLStmt &stmt, uint64_t target_dblink_id);

  int reverse_link_table_for_temp_table(ObDMLStmt *root_stmt, uint64_t target_dblink_id);

  int pack_link_table(ObDMLStmt *stmt, bool &trans_happened);

  int collect_link_table(ObDMLStmt *stmt,
                         ObIArray<LinkTableHelper> &helpers,
                         uint64_t &dblink_id,
                         bool &is_reverse_link,
                         bool &all_table_from_one_dblink);

  int inner_collect_link_table(TableItem *table,
                               JoinedTable *parent_table,
                               ObIArray<LinkTableHelper> &helpers,
                               bool &all_table_from_one_dblink);

  int inner_collect_link_table(ObDMLStmt *stmt,
                               SemiInfo *semi_info,
                               ObIArray<LinkTableHelper> &helpers,
                               bool &all_table_from_one_dblink);

  int check_is_link_table(TableItem *table,
                          uint64_t &dblink_id,
                          bool &is_link_table,
                          bool &is_reverse_link);

  int check_is_link_semi_info(ObDMLStmt &stmt,
                              SemiInfo &semi_info,
                              uint64_t &dblink_id,
                              bool &is_link_semi_info,
                              bool &is_right_link_table,
                              bool &is_reverse_link);

  int add_link_table(TableItem *table,
                    uint64_t dblink_id,
                    bool is_reverse_link,
                    JoinedTable *parent_table,
                    SemiInfo* parent_semi_info,
                    ObIArray<LinkTableHelper> &helpers);

  int add_link_semi_info(SemiInfo *semi_info,
                         uint64_t dblink_id,
                         bool is_reverse_link,
                         ObIArray<LinkTableHelper> &helpers);

  int split_link_table_info(ObDMLStmt *stmt, ObIArray<LinkTableHelper> &helpers);

  int inner_split_link_table_info(ObDMLStmt *stmt,
                                  LinkTableHelper &helper,
                                  ObIArray<LinkTableHelper> &new_helpers);

  int connect_table(ObDMLStmt *stmt, ObRawExpr *expr, UnionFind &uf);

  int get_from_item_idx(ObDMLStmt *stmt, ObRawExpr *expr, ObIArray<int64_t> &idxs);

  int check_can_pushdown(ObDMLStmt *stmt, const LinkTableHelper &helper, bool &can_push);

  int collect_pushdown_conditions(ObDMLStmt *stmt, ObIArray<LinkTableHelper> &helpers);

  int has_none_pushdown_expr(ObIArray<ObRawExpr*> &exprs,
                             uint64_t dblink_id,
                             bool &has);

  int has_none_pushdown_expr(ObRawExpr* expr,
                             uint64_t dblink_id,
                             bool &has);

  int inner_pack_link_table(ObDMLStmt *stmt, LinkTableHelper &helper);

  int formalize_link_table(ObDMLStmt *stmt);

  int formalize_table_name(ObDMLStmt *stmt);

  int formalize_column_item(ObDMLStmt *stmt);

  int formalize_select_item(ObDMLStmt *stmt);

  int formalize_bool_select_expr(ObDMLStmt *stmt);

  int extract_limit(ObDMLStmt *stmt, ObDMLStmt *&dblink_stmt);

  virtual int need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             const int64_t current_level,
                             const ObDMLStmt &stmt,
                             bool &need_trans) override;

  int check_link_oracle(int64_t dblink_id, bool &link_oracle);

  int add_flashback_query_for_dblink(ObDMLStmt *stmt);

  int has_invalid_link_expr(ObDMLStmt &stmt, bool &has_invalid_expr);

  static int check_link_expr_valid(ObRawExpr *expr, bool &is_valid);

  DISALLOW_COPY_AND_ASSIGN(ObTransformDBlink);

private:
  bool transform_for_write_;
};

}
}

#endif /* OB_TRANSFORM_DBLINK_H_ */
