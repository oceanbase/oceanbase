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

#ifndef OCEANBASE_SQL_DELETESTMT_H_
#define OCEANBASE_SQL_DELETESTMT_H_
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "lib/string/ob_string.h"
#include "sql/ob_sql_context.h"

namespace oceanbase {
namespace sql {
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
class ObDeleteStmt : public ObDelUpdStmt {
public:
  ObDeleteStmt();
  virtual ~ObDeleteStmt();
  int deep_copy_stmt_struct(
      ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& other) override;
  int assign(const ObDeleteStmt& other);
  DECLARE_VIRTUAL_TO_STRING;

private:
  /**
   * @note These fields are added for the compatiblity of MySQL syntax and are not
   * supported at the moment.
   */
  bool low_priority_;
  bool quick_;
  bool ignore_;
};
}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_DELETESTMT_H_
