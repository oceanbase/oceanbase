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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_COLUMNS_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_COLUMNS_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace sql
{
class ObSelectStmt;
struct SelectItem;
}

namespace share
{
namespace schema
{
class ObDatabaseSchema;
class ObTableSchema;
class ObColumnSchemaV2;
}
}
namespace observer
{
class ObInfoSchemaColumnsTable : public common::ObVirtualTableScannerIterator
{
  static const int32_t COLUMNS_COLUMN_COUNT = 21;
  enum COLUMN_NAME {
    TABLE_SCHEMA = common::OB_APP_MIN_COLUMN_ID,
    TABLE_NAME,
    TABLE_CATALOG,
    COLUMN_NAME,
    ORDINAL_POSITION,
    COLUMN_DEFAULT,
    IS_NULLABLE,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    CHARACTER_OCTET_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE,
    DATETIME_PRECISION,
    CHARACTER_SET_NAME,
    COLLATION_NAME,
    COLUMN_TYPE,
    COLUMN_KEY,
    EXTRA,
    PRIVILEGES,
    COLUMN_COMMENT,
    GENERATION_EXPRESSION
  };
public:
  ObInfoSchemaColumnsTable();
  virtual ~ObInfoSchemaColumnsTable();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

  inline void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObInfoSchemaColumnsTable);
  int fill_row_cells(const common::ObString &database_name,
                     const share::schema::ObTableSchema *table_schema,
                     const share::schema::ObColumnSchemaV2 *column_schema,
                     const uint64_t ordinal_position);
  int fill_row_cells(const common::ObString &database_name,
                     const share::schema::ObTableSchema *table_schema,
                     const sql::ObSelectStmt *select_stmt,
                     const sql::SelectItem &select_item,
                     const uint64_t ordinal_position);
  int check_database_table_filter();
  /**
   * Iterate through all the tables and fill row cells,
   * if is_filter_table_schema is false, last_db_schema_idx
   * must be a valid value. else, use -1.
   */
  int iterate_table_schema_array(const bool is_filter_table_schema,
                                 const int64_t last_db_schema_idx);
  // Iterate through all the columns and fill cells
  int iterate_column_schema_array(
      const common::ObString &database_name,
      const share::schema::ObTableSchema &table_schema,
      const int64_t last_db_schema_idx,
      const int64_t last_table_idx,
      const bool is_filter_table_schema);
  /**
   * If ob_sql_type_str failed to call, and the error code returned is OB_SIZE_OVERFLOW.
   * realloc memory to the size of OB_MAX_EXTENDED_TYPE_INFO_LENGTH, then try again
   */
  int get_type_str(const ObObjMeta &obj_meta, const ObAccuracy &accuracy,
      const common::ObIArray<ObString> &type_info,
      const int16_t default_length_semantics, int64_t &pos,
      const uint64_t sub_type = static_cast<uint64_t>(common::ObGeoType::GEOMETRY));
  int fill_col_privs(
      ObSessionPrivInfo &session_priv,
      ObNeedPriv &need_priv, 
      ObPrivSet priv_set, 
      const char *priv_str,
      char* buf,
      const int64_t buf_len,
      int64_t &pos);
  inline int init_mem_context();
private:
  uint64_t tenant_id_;
  int64_t last_schema_idx_;
  int64_t last_table_idx_;
  int64_t last_column_idx_;
  bool has_more_;
  char *data_type_str_;
  char *column_type_str_;
  int64_t column_type_str_len_;
  bool is_filter_db_;
  int64_t last_filter_table_idx_;
  int64_t last_filter_column_idx_;
  common::ObSEArray<const share::schema::ObDatabaseSchema *, 8> database_schema_array_;
  common::ObSEArray<const share::schema::ObTableSchema *, 16> filter_table_schema_array_;
  ObArenaAllocator view_resolve_alloc_;
  lib::MemoryContext mem_context_;
  int64_t iter_cnt_;
};
} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_COLUMNS_
