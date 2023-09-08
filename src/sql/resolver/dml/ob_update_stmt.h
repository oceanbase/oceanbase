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

#ifndef OCEANBASE_SQL_OB_UPDATE_STMT_H
#define OCEANBASE_SQL_OB_UPDATE_STMT_H
#include "lib/utility/ob_print_utils.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/ob_sql_context.h"
namespace oceanbase
{
namespace sql
{
/**
 * UPDATE syntax from MySQL 5.7
 *
 * Single-table syntax:
 UPDATE [LOW_PRIORITY] [IGNORE] table_reference
 *   SET col_name1={expr1|DEFAULT} [, col_name2={expr2|DEFAULT}] ...
 *   [WHERE where_condition]
 *   [ORDER BY ...]
 *   [LIMIT row_count]
 *
 * Multiple-table syntax:
 *   UPDATE [LOW_PRIORITY] [IGNORE] table_references
 *   SET col_name1={expr1|DEFAULT} [, col_name2={expr2|DEFAULT}] ...
 *   [WHERE where_condition]
 */
class ObUpdateStmt : public ObDelUpdStmt
{
public:
  ObUpdateStmt();
  virtual ~ObUpdateStmt();
  int deep_copy_stmt_struct(ObIAllocator &allocator,
                            ObRawExprCopier &expr_copier,
                            const ObDMLStmt &other) override;
  int assign(const ObUpdateStmt &other);
  virtual int check_table_be_modified(uint64_t ref_table_id, bool& is_modified) const override;
  common::ObIArray<ObUpdateTableInfo*> &get_update_table_info() { return table_info_; }
  const common::ObIArray<ObUpdateTableInfo*> &get_update_table_info() const { return table_info_; }
  int get_assign_values(ObIArray<ObRawExpr *> &exprs, bool with_vector_assgin) const;
  int get_vector_assign_values(ObQueryRefRawExpr *query_ref,
                               ObIArray<ObRawExpr *> &assign_values) const;

  virtual uint64_t get_trigger_events() const override
  {
    return ObDmlEventType::DE_UPDATING;
  }
  int part_key_is_updated(bool &is_updated) const;
  virtual int get_assignments_exprs(ObIArray<ObRawExpr*> &exprs) const override;
  virtual int get_dml_table_infos(ObIArray<ObDmlTableInfo*>& dml_table_info) override;
  virtual int get_dml_table_infos(ObIArray<const ObDmlTableInfo*>& dml_table_info) const override;
  virtual int get_view_check_exprs(ObIArray<ObRawExpr*>& view_check_exprs) const override;
  virtual int64_t get_instead_of_trigger_column_count() const override;
  virtual int remove_table_item_dml_info(const TableItem* table) override;
  int remove_invalid_assignment();

  TO_STRING_KV(N_STMT_TYPE, stmt_type_,
      N_TABLE, table_items_,
      N_IS_IGNORE, ignore_,
      N_PARTITION_EXPR, part_expr_items_,
      N_COLUMN, column_items_,
      "update_tables", table_info_ ,
      N_WHERE, condition_exprs_,
      N_ORDER_BY, order_items_,
      N_LIMIT, limit_count_expr_,
      N_OFFSET, limit_offset_expr_,
      N_STMT_HINT, stmt_hint_,
      N_QUERY_CTX, query_ctx_);

private:
  common::ObSEArray<ObUpdateTableInfo*, 2, common::ModulePageAllocator, true> table_info_;
};
} //namespace sql
}//namespace oceanbase

#endif //OCEANBASE_SQL_OB_UPDATE_STMT_H
