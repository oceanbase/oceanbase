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

#include "ob_information_referential_constraints_table.h"
#include "lib/container/ob_array_serialization.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace observer
{

ObInfoSchemaReferentialConstraintsTable::ObInfoSchemaReferentialConstraintsTable()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID)
{
}

ObInfoSchemaReferentialConstraintsTable::~ObInfoSchemaReferentialConstraintsTable()
{
  reset();
}

void ObInfoSchemaReferentialConstraintsTable::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ObVirtualTableScannerIterator::reset();
}

int ObInfoSchemaReferentialConstraintsTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == allocator_ || NULL == schema_guard_ ||
                  OB_INVALID_ID == tenant_id_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator or schema_guard is NULL or tenant_id is invalid!",
               K_(schema_guard),
               K_(allocator),
               K_(tenant_id),
               K(ret));
  }
  if (OB_SUCCESS == ret && !start_to_read_) {
    ObSArray<const ObDatabaseSchema *> database_schemas;
    if (OB_FAIL(schema_guard_->get_database_schemas_in_tenant(tenant_id_,
                                                                database_schemas))) {
      SERVER_LOG(WARN, "failed to get database schema of tenant", K_(tenant_id));
    } else {
      ObObj *cells = NULL;
      const int64_t col_count = output_column_ids_.count();
      if (0 > col_count || col_count > REFERENTIAL_CONSTRAINTS_COLUMN_COUNT) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "column count error ", K(ret), K(col_count));
      } else if (NULL == (cells = cur_row_.cells_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
      } else {}
      for (int64_t i = 0; OB_SUCC(ret) && i < database_schemas.count(); ++i) {
        const ObDatabaseSchema* database_schema = database_schemas.at(i);
        if (OB_ISNULL(database_schema)) {
          ret = OB_ERR_BAD_DATABASE;
          SERVER_LOG(WARN, "database not exist", K(ret));
        } else if (database_schema->is_in_recyclebin()) {
          continue;
        } else if (database_schema->get_database_name_str() == OB_SYS_DATABASE_NAME
                   || database_schema->get_database_name_str() == OB_INFORMATION_SCHEMA_NAME
                   || database_schema->get_database_name_str() == OB_RECYCLEBIN_SCHEMA_NAME
                   || database_schema->get_database_name_str() == OB_PUBLIC_SCHEMA_NAME) {
          continue;
        } else if (OB_FAIL(add_fk_constraints_in_db(
                           *database_schema,
                           cells,
                           output_column_ids_.count()))) {
          SERVER_LOG(WARN, "failed to add fk constraint of database schema",
                     K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
  }

  if (OB_SUCCESS == ret && start_to_read_) {
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

int ObInfoSchemaReferentialConstraintsTable::add_fk_constraints_in_db(
    const share::schema::ObDatabaseSchema &database_schema,
    common::ObObj *cells,
    const int64_t col_count)
{
  int ret = OB_SUCCESS;
  ObSArray<const ObTableSchema *> table_schemas;
  bool priv_passed = true;

  if (OB_ISNULL(schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema guard should not be null", K(ret));
  } else if (OB_FAIL(schema_guard_->get_table_schemas_in_database(
                     tenant_id_,
                     database_schema.get_database_id(),
                     table_schemas))) {
    SERVER_LOG(WARN, "failed to get table schema in database", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObTableSchema *table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        SERVER_LOG(WARN, "table schema not exist", K(ret));
      } else if(table_schema->is_index_table() || table_schema->is_view_table() || table_schema->is_aux_lob_table()) {
        continue;
      } else if (OB_FAIL(check_priv("table_acc",
                                 database_schema.get_database_name_str(),
                                 table_schema->get_table_name_str(),
                                 tenant_id_,
                                 priv_passed))) {
        SERVER_LOG(WARN, "failed to check priv", K(ret), K(database_schema.get_database_name_str()),
        K(table_schema->get_table_name_str()));
      } else if (!priv_passed) {
        continue;
      } else if (OB_FAIL(add_fk_constraints_in_table(
                         *table_schema,
                         database_schema.get_database_name_str(),
                         cells,
                         col_count))){
        SERVER_LOG(WARN, "failed to add fk constraints of table schema",
                   "table_schema", *table_schema, K(ret));
      }
    }
  }

  return ret;
}

int ObInfoSchemaReferentialConstraintsTable::add_fk_constraints_in_table(
    const share::schema::ObTableSchema &table_schema,
    const common::ObString &database_name,
    common::ObObj *cells,
    const int64_t col_count)
{
  int ret = OB_SUCCESS;
  ObStringBuf allocator;
  ObString uk_index_name;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const ObIArray<ObForeignKeyInfo> &foreign_key_infos =
        table_schema.get_foreign_key_infos();

  if (OB_ISNULL(schema_guard_) || OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema guard or cells should not be null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count(); i++) {
    const ObForeignKeyInfo &foreign_key_info = foreign_key_infos.at(i);
    const ObSimpleTableSchemaV2 *parent_tbl_schema = NULL;
    const ObMockFKParentTableSchema *mock_fk_parent_table_schema = NULL;
    const ObSimpleDatabaseSchema *parent_db_schema = NULL;
    int64_t cell_idx = 0;
    allocator.reset();
    int64_t database_id = OB_INVALID_ID;
    ObString table_name;
    if (foreign_key_info.parent_table_id_ == table_schema.get_table_id()) {
      continue;
    } else if (col_count > reserved_column_cnt_) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cells count is error", K(ret), K(col_count),
                                               K_(reserved_column_cnt));
    } else if (!foreign_key_info.is_parent_table_mock_) {
      if (OB_FAIL(schema_guard_->get_simple_table_schema(
                  tenant_id,
                  foreign_key_info.parent_table_id_,
                  parent_tbl_schema))) {
        SERVER_LOG(WARN, "failed to get parent table schema", K(ret), K(tenant_id));
      } else if (OB_ISNULL(parent_tbl_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "parent_tbl_schema is null", K(ret));
      } else {
        database_id = parent_tbl_schema->get_database_id();
        table_name = parent_tbl_schema->get_table_name_str();
      }
    } else {
      if (OB_FAIL(schema_guard_->get_mock_fk_parent_table_schema_with_id(
          tenant_id_, foreign_key_info.parent_table_id_, mock_fk_parent_table_schema))) {
        SERVER_LOG(WARN, "failed to get parent table schema", K(ret));
      } else if (OB_ISNULL(mock_fk_parent_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "mock_fk_parent_table_schema is not existed", K(ret), K(tenant_id_), K(foreign_key_info));
      } else {
        database_id = mock_fk_parent_table_schema->get_database_id();
        table_name = mock_fk_parent_table_schema->get_mock_fk_parent_table_name();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema_guard_->get_database_schema(
        tenant_id, database_id,
        parent_db_schema))) {
      SERVER_LOG(WARN, "failed to get parent database schema", K(ret), K(tenant_id));
    } else if (OB_ISNULL(parent_db_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "parent_db_schema is null", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
        uint64_t col_id = output_column_ids_.at(j);
        switch(col_id) {
          case CONSTRAINT_CATALOG: {
            cells[cell_idx].set_varchar(ObString("def"));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                               ObCharset::get_default_charset()));
            break;
          }
          case CONSTRAINT_SCHEMA: {
            cells[cell_idx].set_varchar(database_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                               ObCharset::get_default_charset()));
            break;
          }
          case CONSTRAINT_NAME: {
            cells[cell_idx].set_varchar(foreign_key_info.foreign_key_name_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                               ObCharset::get_default_charset()));
            break;
          }
          case UNIQUE_CONSTRAINT_CATALOG: {
            cells[cell_idx].set_varchar(ObString("def"));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                               ObCharset::get_default_charset()));
            break;
          }
          case UNIQUE_CONSTRAINT_SCHEMA: {
            cells[cell_idx].set_varchar(parent_db_schema->get_database_name_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                               ObCharset::get_default_charset()));
            break;
          }
          case UNIQUE_CONSTRAINT_NAME: {
            if (CONSTRAINT_TYPE_PRIMARY_KEY == foreign_key_info.ref_cst_type_) {
              cells[cell_idx].set_varchar(ObString("PRIMARY"));
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                 ObCharset::get_default_charset()));
            } else if (CONSTRAINT_TYPE_UNIQUE_KEY == foreign_key_info.ref_cst_type_) {
              const ObSimpleTableSchemaV2 *uk_index_schema = NULL;
              if (OB_FAIL(schema_guard_->get_simple_table_schema(
                          tenant_id, foreign_key_info.ref_cst_id_, uk_index_schema))) {
                SERVER_LOG(WARN, "get uk_index_schema failed", K(ret), K(tenant_id));
                break;
              } else if (OB_ISNULL(uk_index_schema)) {
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "uk_index_schema is null");
                break;
              } else {
                uk_index_name.reset();
                // get the original short index name
                if (OB_FAIL(ObTableSchema::get_index_name(allocator,
                            uk_index_schema->get_table_id(),
                            uk_index_schema->get_table_name_str(),
                            uk_index_name))) {
                  SERVER_LOG(WARN, "error get index table name failed", K(ret));
                  break;
                } else {
                  cells[cell_idx].set_varchar(uk_index_name);
                  cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                     ObCharset::get_default_charset()));
                }
              }
            } else if (foreign_key_info.is_parent_table_mock_) {
              cells[cell_idx].set_varchar("NULL");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                                                   ObCharset::get_default_charset()));
            } else {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "foreign_key_info.ref_cst_type_ is invalid type",
                               K(foreign_key_info.ref_cst_type_));
              break;
            }
            break;
          }
          case MATCH_OPTION: {
            cells[cell_idx].set_varchar(ObString("NONE"));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                               ObCharset::get_default_charset()));
            break;
          }
          case UPDATE_RULE: {
            cells[cell_idx].set_varchar(ObString(foreign_key_info.get_update_action_str()));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                               ObCharset::get_default_charset()));
            break;
          }
          case DELETE_RULE: {
            cells[cell_idx].set_varchar(ObString(foreign_key_info.get_delete_action_str()));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                               ObCharset::get_default_charset()));
            break;
          }
          case TABLE_NAME: {
            cells[cell_idx].set_varchar(table_schema.get_table_name_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                               ObCharset::get_default_charset()));
            break;
          }
          case REFERENCED_TABLE_NAME: {
            cells[cell_idx].set_varchar(table_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                               ObCharset::get_default_charset()));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                      K(j), K(output_column_ids_), K(col_id));
            break;
          }
        }
        if (OB_SUCC(ret)) {
          cell_idx++;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(scanner_.add_row(cur_row_))) {
        SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
      }
    }
  }

  return ret;
}


}/* ns observer*/
}/* ns oceanbase */
