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
#include "sql/resolver/dml/ob_del_upd_resolver.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObDeleteResolver : public ObDelUpdResolver
{
public:
// delete
  static const int64_t WITH_MYSQL = 0;   /* with clause in mysql mode*/
  static const int64_t TABLE = 1;       /* table_node */
  static const int64_t WHERE = 2;            /* where */
  static const int64_t ORDER_BY = 3;       /* orderby */
  static const int64_t LIMIT = 4;            /* limit */
  static const int64_t WHEN = 5;              /* when */
  static const int64_t HINT = 6;              /* hint */
  static const int64_t RETURNING = 7;       /* returning */
  static const int64_t ERRORLOGGING = 8;     /* ERROR LOGGING*/

public:
  explicit ObDeleteResolver(ObResolverParams &params);
  virtual ~ObDeleteResolver();
  virtual int resolve(const ParseNode &parse_tree);
  ObDeleteStmt *get_delete_stmt() { return static_cast<ObDeleteStmt*>(stmt_); }
private:
  int resolve_table_list(const ParseNode &table_node, bool &is_multi_table_delete);
  int generate_delete_table_info(const TableItem &table_item);
  int check_multi_delete_table_conflict();
  //@TODO: 这里是mysql的delete table search规则，还需要加oracle的规则
  int find_delete_table_with_mysql_rule(const common::ObString &db_name,
                                        const common::ObString &table_name,
                                        TableItem *&table_item);
  int check_view_deletable();
  int check_safe_update_mode(ObDeleteStmt *delete_stmt, bool is_multi_table_delete);
  common::ObSEArray<TableItem*, 2, common::ModulePageAllocator, true> delete_tables_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DML_OB_DELETE_RESOLVER_H_ */
