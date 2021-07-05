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
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/dml/ob_update_stmt.h"

namespace oceanbase {
namespace sql {
class ObUpdateResolver : public ObDMLResolver {
public:
  static const int64_t TABLE = 0;       /* 0. table node */
  static const int64_t UPDATE_LIST = 1; /* 1. update list */
  static const int64_t WHERE = 2;       /* 2. where node */
  static const int64_t ORDER_BY = 3;    /* 3. order by node */
  static const int64_t LIMIT = 4;       /* 4. limit node */
  static const int64_t WHEN = 5;        /* 5. when node */
  static const int64_t HINT = 6;        /* 6. hint node */
  static const int64_t IGNORE = 7;      /*7. ignore node */
  static const int64_t RETURNING = 8;   /*8. returning node */
public:
  explicit ObUpdateResolver(ObResolverParams& params);
  virtual ~ObUpdateResolver();

  virtual int resolve(const ParseNode& parse_tree);
  inline ObUpdateStmt* get_update_stmt()
  {
    return static_cast<ObUpdateStmt*>(stmt_);
  }

protected:
  /**
   *  For update stmt, we need to add the table to the from_item in order to reuse
   *  the same cost model to generate the access path like in the 'select' stmt case.
   *  It sounds weird though...
   */
  virtual int resolve_table_list(const ParseNode& parse_tree);

private:
  int add_related_columns_to_stmt();
  int resolve_cascade_updated_global_index(
      const ObTableAssignment& ta, common::ObIArray<uint64_t>& cascade_global_index);
  int resolve_multi_table_dml_info(const ObTableAssignment& ta, common::ObIArray<uint64_t>& global_index);
  int check_multi_update_key_conflict();

  int check_view_updatable();

  // following two funcs are for forigien key self reference(engine 3.0)
  // e.g.: update t1 set col = const_val;
  //       will add remove_const above const_val when col is parent column of a foreign key.
  //       see ObTableModifyOp::do_handle()
  int try_add_remove_const_expr(IndexDMLInfo& index_info);
  bool is_parent_col_self_ref_fk(
      uint64_t parent_col_id, const common::ObIArray<share::schema::ObForeignKeyInfo>& fk_infos);
  int try_add_rowid_column_to_stmt(const ObTableAssignment& tas);
  int is_multi_table_update(const ObDMLStmt* stmt, bool& is_multi_table);
  int check_safe_update_mode(ObUpdateStmt* update_stmt);

private:
  bool has_add_all_rowkey_;
  bool has_add_all_columns_;
  common::hash::ObPlacementHashSet<uint64_t> update_column_ids_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_DML_OB_UPDATE_RESOLVER_H_ */
