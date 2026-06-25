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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_UPDATE_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_UPDATE_RESOLVER_H_

#include "lib/hash/ob_placement_hashset.h"
#include "sql/resolver/dml/ob_del_upd_resolver.h"
#include "sql/resolver/dml/ob_update_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObUpdateResolver : public ObDelUpdResolver
{
public:
  static const int64_t WITH_MYSQL = 0;         /*10. with_clause node in mysql mode*/
  static const int64_t TABLE = 1;              /* 0. table node */
  static const int64_t UPDATE_LIST = 2;       /* 1. update list */
  static const int64_t WHERE = 3;              /* 2. where node */
  static const int64_t ORDER_BY = 4;        /* 3. order by node */
  static const int64_t LIMIT = 5;              /* 4. limit node */
  static const int64_t WHEN = 6;                /* 5. when node */
  static const int64_t HINT = 7;                /* 6. hint node */
  static const int64_t IGNORE = 8;               /*7. ignore node */
  static const int64_t RETURNING = 9;           /*8. returning node */
  static const int64_t ERRORLOGGING = 10;           /*9. error_logging node */

public:
  explicit ObUpdateResolver(ObResolverParams &params);
  virtual ~ObUpdateResolver();

  virtual int resolve(const ParseNode &parse_tree);
  inline ObUpdateStmt *get_update_stmt() { return static_cast<ObUpdateStmt*>(stmt_); }
private:
  int resolve_table_list(const ParseNode &parse_tree);
  int generate_update_table_info(ObTableAssignment &table_assign);
  int check_multi_update_table_conflict();
  int check_join_update_conflict();
  int is_join_table_update(const ObDMLStmt *stmt, bool &is_multi_table);
  int check_update_assign_duplicated(const ObUpdateStmt *update_stmt);
  int check_view_updatable();
  int try_expand_returning_exprs();
  int try_add_remove_const_expr_for_assignments();
  bool is_parent_col_self_ref_fk(uint64_t parent_col_id,
                                 const common::ObIArray<share::schema::ObForeignKeyInfo> &fk_infos);

  int check_safe_update_mode(ObUpdateStmt *update_stmt);
  int resolve_update_constraints();
  int generate_batched_stmt_info();
#ifdef OB_HOTSPOT_GROUP_COMMIT
  // Resolve primary key parameter positions for group commit
  int resolve_group_commit_key_param_infos();

  // Helper functions for resolve_group_commit_pk_param_infos
  int collect_key_sets(const share::schema::ObTableSchema *table_schema,
                       common::ObSEArray<common::ObSEArray<uint64_t, 4>, 4> &key_sets,
                       int &primary_key_index_in_key_sets);
  int build_col_to_param_map(ObUpdateStmt *update_stmt,
                              TableItem *target_table_item,
                              common::hash::ObHashMap<uint64_t, int64_t> &questionmark_col_to_param,
                              common::hash::ObHashMap<uint64_t, int64_t> &equal_col_to_param);
  int check_key_columns_updated(ObUpdateStmt *update_stmt,
                                 const share::schema::ObTableSchema *table_schema,
                                 TableItem *target_table_item,
                                 const common::hash::ObHashMap<uint64_t, int64_t> &questionmark_col_to_param,
                                 const common::ObSEArray<int64_t, 4> &matched_param_idx_result);
  int find_matched_key(const common::hash::ObHashMap<uint64_t, int64_t> &questionmark_col_to_param,
                       const common::hash::ObHashMap<uint64_t, int64_t> &equal_col_to_param,
                       const common::ObSEArray<common::ObSEArray<uint64_t, 4>, 4> &key_sets,
                       int primary_key_index_in_key_sets,
                       common::ObSEArray<int64_t, 4> &matched_param_idx_result);
#endif
};

} // namespace sql
} // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_DML_OB_UPDATE_RESOLVER_H_ */
