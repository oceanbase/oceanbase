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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_DELETE_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_DELETE_RESOLVER_H_
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
namespace oceanbase {
namespace sql {
class ObDeleteResolver : public ObDMLResolver {
public:
  // delete
  static const int64_t TABLE = 0;     /* table_node */
  static const int64_t WHERE = 1;     /* where */
  static const int64_t ORDER_BY = 2;  /* orderby */
  static const int64_t LIMIT = 3;     /* limit */
  static const int64_t WHEN = 4;      /* when */
  static const int64_t HINT = 5;      /* hint */
  static const int64_t RETURNING = 6; /* returning */

public:
  explicit ObDeleteResolver(ObResolverParams& params);
  virtual ~ObDeleteResolver();

  virtual int resolve(const ParseNode& parse_tree);
  /**
   *  For delete stmt(same as update), we need to add the table to the from_item
   *  in order to reuse the same cost model to generate the access path like in
   *  the 'select' stmt case.
   *
   *  It sounds weird though...
   */
  int resolve_table_list(const ParseNode& table_node, bool& is_multi_table_delete);
  ObDeleteStmt* get_delete_stmt()
  {
    return static_cast<ObDeleteStmt*>(stmt_);
  }

private:
  int add_related_columns_to_stmt(const TableItem& table_item);
  int add_index_related_column_to_stmt(
      const TableItem& table_item, common::ObIArray<ObColumnRefRawExpr*>& column_exprs);
  int find_delete_table_with_mysql_rule(
      const common::ObString& db_name, const common::ObString& table_name, TableItem*& table_item);
  int resolve_global_delete_index_info(const TableItem& table_item);

  int try_add_rowid_column_to_stmt(const TableItem& table_item);

  common::ObSEArray<const TableItem*, 4, common::ModulePageAllocator, true> delete_tables_;

  int check_view_deletable();
  int check_safe_update_mode(ObDeleteStmt* delete_stmt, bool is_multi_table_delete);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DML_OB_DELETE_RESOLVER_H_ */
