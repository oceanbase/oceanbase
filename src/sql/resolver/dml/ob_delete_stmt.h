/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_DELETESTMT_H_
#define OCEANBASE_SQL_DELETESTMT_H_
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "sql/resolver/ob_sql_array.h"
#include "lib/string/ob_string.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{
namespace sql
{
/**
 * DELETE syntax from MySQL 5.7
 *
 * Single-Table Syntax:
 *   DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM tbl_name
 *   [PARTITION (partition_name,...)]
 *   [WHERE where_condition]
 *   [ORDER BY ...]
 *   [LIMIT row_count]
 *
 * Multiple-Table Syntax
 *   DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
 *   tbl_name[.*] [, tbl_name[.*]] ...
 *   FROM table_references
 *   [WHERE where_condition]
 *  Or:
 *   DELETE [LOW_PRIORITY] [QUICK] [IGNORE]
 *   FROM tbl_name[.*] [, tbl_name[.*]] ...
 *   USING table_references
 *   [WHERE where_condition]
 */
class ObDeleteStmt : public ObDelUpdStmt
{
public:
  ObDeleteStmt(ObIAllocator &allocator);
  virtual ~ObDeleteStmt();
  int deep_copy_stmt_struct(ObIAllocator &allocator,
                            ObRawExprCopier &expr_factory,
                            const ObDMLStmt &other) override;
  int assign(const ObDeleteStmt &other);
  virtual uint64_t get_trigger_events() const override
  {
    return ObDmlEventType::DE_DELETING;
  }
  virtual int check_table_be_modified(uint64_t ref_table_id, bool& is_modified) const override;
  int remove_delete_table_info(int64_t table_id);
  common::ObIArray<ObDeleteTableInfo*> &get_delete_table_info() { return table_info_; }
  const common::ObIArray<ObDeleteTableInfo*> &get_delete_table_info() const { return table_info_; }
  virtual int get_dml_table_infos(ObIArray<ObDmlTableInfo*>& dml_table_info) override;
  virtual int get_dml_table_infos(ObIArray<const ObDmlTableInfo*>& dml_table_info) const override;
  virtual int get_view_check_exprs(ObIArray<ObRawExpr*>& view_check_exprs) const override;
  virtual int64_t get_instead_of_trigger_column_count() const override;
  virtual int remove_table_item_dml_info(const TableItem* table) override;
  DECLARE_VIRTUAL_TO_STRING;
private:
  ObSqlArray<ObDeleteTableInfo*> table_info_;
};
}
}

#endif //OCEANBASE_SQL_DELETESTMT_H_
