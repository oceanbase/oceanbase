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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TABLE_COLUMNS_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TABLE_COLUMNS_
#include "lib/container/ob_se_array.h"
#include "share/ob_define.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_range.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {
namespace common {
class ObRowkeyInfo;
}
namespace sql {
class ObSQLSessionInfo;
class ObSelectStmt;
class ObRawExprFactory;
class ObStmtFactory;
struct SelectItem;
class ObRawExpr;
}  // namespace sql
namespace observer {
class ObTableColumns : public common::ObVirtualTableScannerIterator {
public:
  ObTableColumns();
  virtual ~ObTableColumns();
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  static int resolve_view_definition(common::ObIAllocator* allocator, sql::ObSQLSessionInfo* session,
      share::schema::ObSchemaGetterGuard* guard, const share::schema::ObTableSchema& table_schema,
      sql::ObSelectStmt*& select_stmt, sql::ObRawExprFactory& expr_factory, sql::ObStmtFactory& stmt_factory);

private:
  enum KeyType {
    KEY_TYPE_EMPTY = 0,
    KEY_TYPE_PRIMARY = 1,
    KEY_TYPE_UNIQUE = 2,
    KEY_TYPE_MULTIPLE = 3,
    KEY_TYPE_MAX = 4
  };
  struct ColumnAttributes {
    common::ObString field_;
    common::ObString type_;
    common::ObString null_;
    common::ObString key_;
    common::ObString default_;
    common::ObString extra_;
    common::ObString privileges_;
  };
  enum DESC_COLUMN { TABLE_ID = 16, FIELD, TYPE, COLLATION, NULLABLE, KEY, DEFAULT, EXTRA, PRIVILEGES, COMMENT };
  int calc_show_table_id(uint64_t& show_table_id);
  int fill_row_cells(
      const share::schema::ObTableSchema& table_schema, const share::schema::ObColumnSchemaV2& column_schema);
  int fill_row_cells(uint64_t table_id, const sql::ObSelectStmt* select_stmt, const sql::SelectItem& select_item);
  int deduce_column_attributes(
      const sql::ObSelectStmt* select_stmt, const sql::SelectItem& select_item, ColumnAttributes& column_attributes);
  int set_null_and_default_according_binary_expr(
      const sql::ObSelectStmt* select_stmt, const sql::ObRawExpr* expr, bool& nullable, bool& has_default);
  int get_key_type(const share::schema::ObTableSchema& table_schema,
      const share::schema::ObColumnSchemaV2& column_schema, KeyType& key_type) const;
  int is_primary_key(const share::schema::ObTableSchema& table_schema,
      const share::schema::ObColumnSchemaV2& column_schema, bool& is_pri) const;
  int is_unique_key(const share::schema::ObTableSchema& table_schema,
      const share::schema::ObColumnSchemaV2& column_schema, bool& is_uni) const;
  int is_multiple_key(const share::schema::ObTableSchema& table_schema,
      const share::schema::ObColumnSchemaV2& column_schema, bool& is_mul) const;
  /**
   * If ob_sql_type_str failed to call, and the error code returned is OB_SIZE_OVERFLOW.
   * realloc memory to the size of OB_MAX_EXTENDED_TYPE_INFO_LENGTH, then try again
   */
  int get_type_str(const ObObjMeta& obj_meta, const ObAccuracy& accuracy, const common::ObIArray<ObString>& type_info,
      const int16_t default_length_semantics, ObString& type_val);

private:
  char type_str_[common::OB_MAX_SYS_PARAM_NAME_LENGTH];
  char* column_type_str_;
  int64_t column_type_str_len_;
  DISALLOW_COPY_AND_ASSIGN(ObTableColumns);
};
}  // namespace observer
}  // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TABLE_COLUMNS_ */
