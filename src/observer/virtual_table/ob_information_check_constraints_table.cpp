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

#include "ob_information_check_constraints_table.h"
#include "lib/container/ob_array_serialization.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_constraint.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace observer {

ObInfoSchemaCheckConstraintsTable::ObInfoSchemaCheckConstraintsTable()
    : ObVirtualTableScannerIterator(), tenant_id_(OB_INVALID_ID)
{}

ObInfoSchemaCheckConstraintsTable::~ObInfoSchemaCheckConstraintsTable()
{
  reset();
}

void ObInfoSchemaCheckConstraintsTable::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ObVirtualTableScannerIterator::reset();
}

int ObInfoSchemaCheckConstraintsTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(allocator_) || OB_ISNULL(schema_guard_) || OB_INVALID_ID == tenant_id_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN,
        "allocator or schema_guard is NULL or tenant_id is invalid!",
        K(ret),
        K_(schema_guard),
        K_(allocator),
        K_(tenant_id));
  }
  if (OB_SUCC(ret) && !start_to_read_) {
    ObSArray<const ObDatabaseSchema*> database_schemas;
    if (OB_FAIL(schema_guard_->get_database_schemas_in_tenant(tenant_id_, database_schemas))) {
      SERVER_LOG(WARN, "failed to get database schema of tenant", K(ret), K_(tenant_id));
    } else {
      ObObj *cells = NULL;
      const int64_t col_count = output_column_ids_.count();
      if (0 > col_count || col_count > CHECK_CONSTRAINTS_COLUMN_COUNT) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "column count error ", K(ret), K(col_count));
      } else if (OB_ISNULL((cells = cur_row_.cells_))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
      } else {
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < database_schemas.count(); ++i) {
        const ObDatabaseSchema *database_schema = database_schemas.at(i);
        if (OB_ISNULL(database_schema)) {
          ret = OB_ERR_BAD_DATABASE;
          SERVER_LOG(WARN, "database not exist", K(ret));
        } else if (database_schema->is_in_recyclebin()) {
          continue;
        } else if (database_schema->get_database_name_str() == OB_SYS_DATABASE_NAME ||
                   database_schema->get_database_name_str() == OB_INFORMATION_SCHEMA_NAME ||
                   database_schema->get_database_name_str() == OB_RECYCLEBIN_SCHEMA_NAME ||
                   ObString(OB_PUBLIC_SCHEMA_NAME) == database_schema->get_database_name_str()) {
          continue;
        } else if (OB_FAIL(add_check_constraints(*database_schema, cells, output_column_ids_.count()))) {
          SERVER_LOG(WARN, "failed to add table constraint of database schema!", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
  }

  if (OB_SUCC(ret) && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObInfoSchemaCheckConstraintsTable::add_check_constraints(
    const ObDatabaseSchema &database_schema, ObObj *cells, const int64_t col_count)
{
  int ret = OB_SUCCESS;
  ObSArray<const ObTableSchema*> table_schemas;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema guard should not be null", K(ret));
  } else if (OB_FAIL(schema_guard_->get_table_schemas_in_database(
                 tenant_id_, database_schema.get_database_id(), table_schemas))) {
    SERVER_LOG(WARN, "failed to get table schema in database", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObTableSchema *table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        SERVER_LOG(WARN, "table schema not exist", K(ret));
      } else if (table_schema->is_index_table() || table_schema->is_view_table() || table_schema->is_aux_lob_table()) {
        continue;
      } else if (!table_schema->is_mysql_tmp_table() && OB_FAIL(
                     add_check_constraints(*table_schema, database_schema.get_database_name_str(), cells, col_count))) {
        SERVER_LOG(WARN, "failed to add table constraint of table schema", K(ret), "table_schema", *table_schema);
      }
    }
  }
  return ret;
}

int ObInfoSchemaCheckConstraintsTable::add_check_constraints(
    const ObTableSchema &table_schema, const ObString &database_name, ObObj *cells, const int64_t col_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_guard_) || OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema guard or cells should not be null", K(ret));
  }

  for (ObTableSchema::const_constraint_iterator iter = table_schema.constraint_begin();
       OB_SUCC(ret) && iter != table_schema.constraint_end();
       ++iter) {
    if (OB_ISNULL(*iter)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "iter is NULL", K(ret));
    } else if ((*iter)->get_constraint_type() != CONSTRAINT_TYPE_CHECK) {
      continue;
    } else {
      const ObConstraint *constraint = *iter;
      if (col_count > reserved_column_cnt_) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cells count is error", K(ret), K(col_count), K_(reserved_column_cnt));
      }
      for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; ++cell_idx) {
        uint64_t col_id = output_column_ids_.at(cell_idx);
        switch (col_id) {
          case CONSTRAINT_CATALOG: {
            cells[cell_idx].set_varchar(ObString("def"));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case CONSTRAINT_SCHEMA: {
            cells[cell_idx].set_varchar(database_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case CONSTRAINT_NAME: {
            cells[cell_idx].set_varchar(constraint->get_constraint_name_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case CHECK_CLAUSE: {
            cells[cell_idx].set_varchar(constraint->get_check_expr());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
            break;
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(scanner_.add_row(cur_row_))) {
        SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
