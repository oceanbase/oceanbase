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

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_show_create_table.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_printer.h"
#include "share/schema/ob_table_schema.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace observer {

ObShowCreateTable::ObShowCreateTable() : ObVirtualTableScannerIterator()
{}

ObShowCreateTable::~ObShowCreateTable()
{}

void ObShowCreateTable::reset()
{
  ObVirtualTableScannerIterator::reset();
}

int ObShowCreateTable::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret), K(schema_guard_));
  } else if (!start_to_read_) {
    const ObTableSchema* table_schema = NULL;
    uint64_t show_table_id = OB_INVALID_ID;
    if (OB_FAIL(calc_show_table_id(show_table_id))) {
      SERVER_LOG(WARN, "fail to calc show table id", K(ret), K(show_table_id));
    } else if (OB_UNLIKELY(OB_INVALID_ID == show_table_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "this table is used for show clause, can't be selected");
    } else if (OB_FAIL(schema_guard_->get_table_schema(show_table_id, table_schema))) {
      SERVER_LOG(WARN, "fail to get table schema", K(ret), K(show_table_id));
    } else if (OB_UNLIKELY(NULL == table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      SERVER_LOG(WARN, "fail to get table schema", K(ret), K(show_table_id));
    } else if (OB_SYS_TENANT_ID != table_schema->get_tenant_id() && table_schema->is_vir_table() &&
               is_restrict_access_virtual_table(table_schema->get_table_id())) {
      ret = OB_TABLE_NOT_EXIST;
      SERVER_LOG(WARN, "fail to get table schema", K(ret), K(show_table_id));
    } else {
      if (OB_FAIL(fill_row_cells(show_table_id, *table_schema))) {
        SERVER_LOG(WARN, "fail to fill row cells", K(ret), K(show_table_id), K(table_schema->get_table_name_str()));
      } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
        SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
      } else {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
  }
  if (OB_SUCCESS == ret && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObShowCreateTable::calc_show_table_id(uint64_t& show_table_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCCESS == ret && OB_INVALID_ID == show_table_id && i < key_ranges_.count(); ++i) {
    const ObRowkey& start_key = key_ranges_.at(i).start_key_;
    const ObRowkey& end_key = key_ranges_.at(i).end_key_;
    const ObObj* start_key_obj_ptr = start_key.get_obj_ptr();
    const ObObj* end_key_obj_ptr = end_key.get_obj_ptr();
    if (start_key.get_obj_cnt() > 0 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
      if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
      } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0] && ObIntType == start_key_obj_ptr[0].get_type()) {
        show_table_id = start_key_obj_ptr[0].get_int();
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObShowCreateTable::fill_row_cells(uint64_t show_table_id, const ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t cell_idx = 0;
  char* table_def_buf = NULL;
  int64_t table_def_buf_size = OB_MAX_VARCHAR_LENGTH;
  if (OB_UNLIKELY(NULL == schema_guard_ || NULL == session_ || NULL == allocator_ || NULL == cur_row_.cells_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(
        WARN, "data member isn't init", K(ret), K(schema_guard_), K(session_), K(allocator_), K(cur_row_.cells_));
  } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN,
        "cells count is less than output column count",
        K(ret),
        K(cur_row_.count_),
        K(output_column_ids_.count()));
  } else if (OB_UNLIKELY(NULL == (table_def_buf = static_cast<char*>(allocator_->alloc(table_def_buf_size))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "fail to alloc table_def_buf", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID: {
          // table_id
          cur_row_.cells_[cell_idx].set_int(show_table_id);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 1: {
          // table
          cur_row_.cells_[cell_idx].set_varchar(table_schema.get_table_name_str());
          cur_row_.cells_[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 2: {
          // create_table
          ObSchemaPrinter schema_printer(*schema_guard_);
          int64_t pos = 0;
          if (table_schema.is_view_table()) {
            if (OB_FAIL(
                    schema_printer.print_view_definiton(show_table_id, table_def_buf, OB_MAX_VARCHAR_LENGTH, pos))) {
              SERVER_LOG(WARN, "Generate view definition failed");
            }
          } else if (table_schema.is_index_table()) {
            if (OB_FAIL(schema_printer.print_index_table_definition(
                    show_table_id, table_def_buf, OB_MAX_VARCHAR_LENGTH, pos, TZ_INFO(session_), false))) {
              SERVER_LOG(WARN, "Generate index definition failed");
            }
          } else {
            const ObLengthSemantics default_length_semantics = session_->get_local_nls_length_semantics();
            // get auto_increment from auto_increment service, not from table option
            if (OB_FAIL(schema_printer.print_table_definition(show_table_id,
                    table_def_buf,
                    OB_MAX_VARCHAR_LENGTH,
                    pos,
                    TZ_INFO(session_),
                    default_length_semantics,
                    false))) {
              SERVER_LOG(WARN, "Generate table definition failed");
            }
          }
          if (OB_SUCC(ret)) {
            // This column type is changed from varchar to longtext in ver 3.1.0.
            // For compatibility, column type should be determined by schema before cluster is in upgrade mode.
            bool type_is_lob = true;
            if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100) {
              const ObColumnSchemaV2* column_schema = NULL;
              if (OB_ISNULL(table_schema_) || OB_ISNULL(column_schema = table_schema_->get_column_schema(col_id))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("table or column schema is null", K(ret), KP(table_schema_), KP(column_schema));
              } else {
                type_is_lob = column_schema->get_meta_type().is_lob();
              }
            }
            if (type_is_lob) {
              cur_row_.cells_[cell_idx].set_lob_value(ObLongTextType, table_def_buf, static_cast<int32_t>(pos));
            } else {
              ObString value_str(static_cast<int32_t>(pos), static_cast<int32_t>(pos), table_def_buf);
              cur_row_.cells_[cell_idx].set_varchar(value_str);
            }
            cur_row_.cells_[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 3: {
          ObCharsetType cs_client_type = table_schema.get_view_schema().get_character_set_client();
          // For compatibility, table schema may not record charset and collation.
          // In such situation, we use charset and collation from session.
          if (CHARSET_INVALID == cs_client_type && OB_FAIL(session_->get_character_set_client(cs_client_type))) {
            LOG_WARN("fail to get character_set_client", K(ret));
          } else {
            cur_row_.cells_[cell_idx].set_varchar(ObCharset::charset_name(cs_client_type));
            cur_row_.cells_[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 4: {
          ObCollationType coll_connection_type = table_schema.get_view_schema().get_collation_connection();
          if (CS_TYPE_INVALID == coll_connection_type &&
              OB_FAIL(session_->get_collation_connection(coll_connection_type))) {
            LOG_WARN("fail to get coll_connection_type", K(ret));
          } else {
            cur_row_.cells_[cell_idx].set_varchar(ObCharset::collation_name(coll_connection_type));
            cur_row_.cells_[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(i), K(output_column_ids_), K(col_id));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
