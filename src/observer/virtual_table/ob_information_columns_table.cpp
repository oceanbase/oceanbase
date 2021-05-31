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

#include "observer/virtual_table/ob_information_columns_table.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;

namespace observer {

ObInfoSchemaColumnsTable::ObInfoSchemaColumnsTable()
    : ObVirtualTableScannerIterator(),
      tenant_id_(OB_INVALID_ID),
      last_schema_idx_(-1),
      last_table_idx_(-1),
      last_column_idx_(-1),
      has_more_(false),
      data_type_str_(NULL),
      column_type_str_(NULL),
      column_type_str_len_(-1),
      is_filter_db_(false),
      last_filter_table_idx_(-1),
      last_filter_column_idx_(-1),
      database_schema_array_(),
      filter_table_schema_array_()
{}

ObInfoSchemaColumnsTable::~ObInfoSchemaColumnsTable()
{
  reset();
}

void ObInfoSchemaColumnsTable::reset()
{
  ObVirtualTableScannerIterator::reset();
  tenant_id_ = OB_INVALID_ID;
}

int ObInfoSchemaColumnsTable::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_) || OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or schema_guard_ is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "tenant id is invalid_id", K(ret), K(tenant_id_));
  } else {
    if (!start_to_read_) {
      void* tmp_ptr = NULL;
      // If there is no db filter (is_filter_db_: false), At most one loop
      // in check_database_table_filter: start_key=MIN,MIN, end_key=MAX,MAX
      if (!is_filter_db_ && OB_FAIL(check_database_table_filter())) {
        SERVER_LOG(WARN, "fail to check database and table filter", K(ret));
        // When db_name is specified, there is no need to traverse all the database_schema_array of the tenant directly
      } else if (!is_filter_db_ &&
                 OB_FAIL(schema_guard_->get_database_schemas_in_tenant(tenant_id_, database_schema_array_))) {
        SERVER_LOG(WARN, "fail to get database schemas in tenant", K(ret), K(tenant_id_));
      } else if (OB_UNLIKELY(NULL == (tmp_ptr = static_cast<char*>(allocator_->alloc(OB_MAX_SYS_PARAM_NAME_LENGTH))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(ERROR, "fail to alloc memory", K(ret));
      } else if (FALSE_IT(data_type_str_ = static_cast<char*>(tmp_ptr))) {
      } else if (OB_UNLIKELY(NULL == (tmp_ptr = static_cast<char*>(allocator_->alloc(OB_MAX_SYS_PARAM_NAME_LENGTH))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(ERROR, "fail to alloc memory", K(ret));
      } else {
        column_type_str_ = static_cast<char*>(tmp_ptr);
        column_type_str_len_ = OB_MAX_SYS_PARAM_NAME_LENGTH;
      }

      // Traverse in two parts: database_schema_array + table_schema_array
      //
      // 1. Scan database_schema_array
      int64_t i = 0;
      if (last_schema_idx_ != -1) {
        i = last_schema_idx_;
        last_schema_idx_ = -1;
      }
      bool is_filter_table_schema = false;
      for (; OB_SUCC(ret) && i < database_schema_array_.count() && !has_more_; ++i) {
        const ObDatabaseSchema* database_schema = database_schema_array_.at(i);
        if (OB_ISNULL(database_schema)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "database_schema is NULL", K(ret));
        } else if (database_schema->is_in_recyclebin() ||
                   ObString(OB_RECYCLEBIN_SCHEMA_NAME) == database_schema->get_database_name_str() ||
                   ObSchemaUtils::is_public_database(database_schema->get_database_name_str(), lib::is_oracle_mode())) {
          continue;
        } else if (OB_FAIL(iterate_table_schema_array(is_filter_table_schema, i))) {
          SERVER_LOG(WARN, "fail to iterate all table schema. ", K(ret));
        }
      }  // end for database_schema_array_ loop

      // 2. Scan table_schema_array
      // After scanning database_schema_array, continue to scan filter_table_schema_array
      if (database_schema_array_.count() == i) {
        is_filter_table_schema = true;
        if (OB_FAIL(iterate_table_schema_array(is_filter_table_schema, -1))) {
          SERVER_LOG(WARN, "fail to iterate all table schema. ", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }

    if (OB_SUCCESS == ret && start_to_read_) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        } else if (has_more_) {
          SERVER_LOG(INFO, "continue to fetch reset rows", K(ret));
          has_more_ = false;
          start_to_read_ = false;
          scanner_.reset();
          ret = inner_get_next_row(row);
        }
      } else {
        row = &cur_row_;
      }
    }
  }

  return ret;
}

int ObInfoSchemaColumnsTable::iterate_table_schema_array(
    const bool is_filter_table_schema, const int64_t last_db_schema_idx)
{
  int ret = OB_SUCCESS;
  const ObDatabaseSchema* database_schema = NULL;
  ObArray<const ObTableSchema*> table_schema_array;
  int64_t table_schema_array_size = 0;
  // Handling table_schema_array pagination
  int64_t i = 0;
  if (!is_filter_table_schema && OB_UNLIKELY(last_db_schema_idx < 0)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "last_db_schema_idx should be greater than or equal to 0", K(ret));
  } else if (is_filter_table_schema) {
    table_schema_array_size = filter_table_schema_array_.count();
    if (last_filter_table_idx_ != -1) {
      i = last_filter_table_idx_;
      last_filter_table_idx_ = -1;
    }
  } else {
    database_schema = database_schema_array_.at(last_db_schema_idx);
    const uint64_t database_id = database_schema->get_database_id();
    // get all tables
    if (OB_FAIL(schema_guard_->get_table_schemas_in_database(tenant_id_, database_id, table_schema_array))) {
      SERVER_LOG(WARN, "fail to get table schemas in database", K(ret), K(tenant_id_), K(database_id));
    } else {
      table_schema_array_size = table_schema_array.count();
    }
    if (last_table_idx_ != -1) {
      i = last_table_idx_;
      last_table_idx_ = -1;
    }
  }
  for (; OB_SUCC(ret) && i < table_schema_array_size && !has_more_; ++i) {
    const ObTableSchema* table_schema = NULL;
    if (is_filter_table_schema) {
      table_schema = filter_table_schema_array_.at(i);
    } else {
      table_schema = table_schema_array.at(i);
    }
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "table_schema should not be NULL", K(ret));
    } else {
      // Do not display index table
      if (table_schema->is_aux_table() || table_schema->is_in_recyclebin() ||
          (table_schema->is_view_table()  // isn't materialized view, continue
              && !table_schema->is_materialized_view())) {
        continue;
      } else if (is_filter_table_schema && OB_FAIL(schema_guard_->get_database_schema(
                                               tenant_id_, table_schema->get_database_id(), database_schema))) {
        SERVER_LOG(WARN, "fail to get database schema", K(ret));
      } else if (OB_ISNULL(database_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "database schema is null", K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(iterate_column_schema_array(database_schema->get_database_name_str(),
                              *table_schema,
                              last_db_schema_idx,
                              i,
                              is_filter_table_schema))) {
        SERVER_LOG(ERROR, "fail to iterate all table columns. ", K(ret));
      }
    }
  }
  return ret;
}

int ObInfoSchemaColumnsTable::iterate_column_schema_array(const ObString& database_name,
    const share::schema::ObTableSchema& table_schema, const int64_t last_db_schema_idx, const int64_t last_table_idx,
    const bool is_filter_table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t ordinal_position = 0;
  // Log the logical order of the columns
  uint64_t logical_index = 0;
  if (last_column_idx_ != -1) {
    logical_index = last_column_idx_;
    last_column_idx_ = -1;
  }
  ObColumnIterByPrevNextID iter(table_schema);
  const ObColumnSchemaV2* column_schema = NULL;
  for (int j = 0; OB_SUCC(ret) && j < logical_index && OB_SUCC(iter.next(column_schema)); j++) {
    // do nothing
  }
  while (OB_SUCC(ret) && OB_SUCC(iter.next(column_schema)) && !has_more_) {
    ++logical_index;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "column_schema is NULL", K(ret));
    } else {
      // Do not show hidden pk
      if (column_schema->is_hidden()) {
        continue;
      }
      // use const_column_iterator, if it's index table
      // so should use the physical position
      if (table_schema.is_index_table()) {
        ordinal_position = column_schema->get_column_id() - 15;
      } else {
        ordinal_position = logical_index;
      }
      if (OB_FAIL(fill_row_cells(database_name, &table_schema, column_schema, ordinal_position))) {
        SERVER_LOG(WARN, "failed to fill row cells", K(ret));
      } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
        if (OB_SIZE_OVERFLOW == ret) {
          if (is_filter_table_schema) {
            last_filter_table_idx_ = last_table_idx;
            last_filter_column_idx_ = logical_index;
          } else {
            last_schema_idx_ = last_db_schema_idx;
            last_table_idx_ = last_table_idx;
            last_column_idx_ = logical_index;
          }
          has_more_ = true;
          ret = OB_SUCCESS;
        } else {
          SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    } else {
      SERVER_LOG(WARN, "fail to iterate all table columns. iter quit. ", K(ret));
    }
  }
  return ret;
}

// Filtering strategy:
// If the key_ranges_ extracts the db_name:
//      directly traverses the current database_schema_array, all the database_schema_arrays
//      of the tenant are no longer obtained from the schema guard; regardless of
//      whether it is a valid db_name, is_filter_db_ is set to true
// If the table_name is extracted:
//      the current table_schema_array is directly traversed, all the table schemas
//      will not be obtained from the database_schema_arrays
int ObInfoSchemaColumnsTable::check_database_table_filter()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges_.count(); ++i) {
    const ObRowkey& start_key = key_ranges_.at(i).start_key_;
    const ObRowkey& end_key = key_ranges_.at(i).end_key_;
    const ObObj* start_key_obj_ptr = start_key.get_obj_ptr();
    const ObObj* end_key_obj_ptr = end_key.get_obj_ptr();
    if (2 != start_key.get_obj_cnt() || 2 != end_key.get_obj_cnt()) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN,
          "unexpected # of rowkey columns",
          K(ret),
          "size of start key",
          start_key.get_obj_cnt(),
          "size of end key",
          end_key.get_obj_cnt());
    } else if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
    } else if (start_key_obj_ptr[0].is_varchar_or_char() && end_key_obj_ptr[0].is_varchar_or_char() &&
               start_key_obj_ptr[0] == end_key_obj_ptr[0]) {
      // Indicates that at least db_name is specified, and the filter condition
      // is db_name + table_name, so there is no need to obtain all database_schema under the tenant
      is_filter_db_ = true;
      ObString database_name = start_key_obj_ptr[0].get_varchar();
      const ObDatabaseSchema* filter_database_schema = NULL;
      if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_, database_name, filter_database_schema))) {
        SERVER_LOG(WARN, "fail to get database schema", K(ret), K(tenant_id_), K(database_name));
      } else if (NULL == filter_database_schema) {
      } else if (start_key_obj_ptr[1].is_varchar_or_char() && end_key_obj_ptr[1].is_varchar_or_char() &&
                 start_key_obj_ptr[1] == end_key_obj_ptr[1]) {
        // Specify db_name and tbl_name at the same time
        const ObTableSchema* filter_table_schema = NULL;
        ObString table_name = start_key_obj_ptr[1].get_varchar();
        if (OB_FAIL(schema_guard_->get_table_schema(tenant_id_,
                filter_database_schema->get_database_id(),
                table_name,
                false /*is_index*/,
                filter_table_schema))) {
          SERVER_LOG(WARN, "fail to get table", K(ret), K(tenant_id_), K(database_name), K(table_name));
        } else if (NULL == filter_table_schema) {
        } else if (OB_FAIL(filter_table_schema_array_.push_back(filter_table_schema))) {
          SERVER_LOG(WARN, "push_back failed", K(filter_table_schema->get_table_name()));
        }
        // At this time, only db_name is specified, and the db push_back is directly entered into
        // filter_database_schema_array
      } else if (OB_FAIL(database_schema_array_.push_back(filter_database_schema))) {
        SERVER_LOG(WARN, "push_back failed", K(filter_database_schema->get_database_name()));
      }
    }
  }
  return ret;
}

int ObInfoSchemaColumnsTable::get_type_str(const ObObjMeta& obj_meta, const ObAccuracy& accuracy,
    const common::ObIArray<ObString>& type_info, const int16_t default_length_semantics, int64_t& pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ob_sql_type_str(
          obj_meta, accuracy, type_info, default_length_semantics, column_type_str_, column_type_str_len_, pos))) {
    if (OB_MAX_SYS_PARAM_NAME_LENGTH == column_type_str_len_ && OB_SIZE_OVERFLOW == ret) {
      if (OB_UNLIKELY(
              NULL == (column_type_str_ = static_cast<char*>(allocator_->realloc(
                           column_type_str_, OB_MAX_SYS_PARAM_NAME_LENGTH, OB_MAX_EXTENDED_TYPE_INFO_LENGTH))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(ERROR, "fail to alloc memory", K(ret));
      } else {
        pos = 0;
        column_type_str_len_ = OB_MAX_EXTENDED_TYPE_INFO_LENGTH;
        ret = ob_sql_type_str(
            obj_meta, accuracy, type_info, default_length_semantics, column_type_str_, column_type_str_len_, pos);
      }
    }
  }
  return ret;
}

int ObInfoSchemaColumnsTable::fill_row_cells(const ObString& database_name, const ObTableSchema* table_schema,
    const ObColumnSchemaV2* column_schema, const uint64_t ordinal_position)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ or session_ is NULL", K(ret), K(allocator_), K(session_));
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(column_schema) || OB_ISNULL(column_type_str_) ||
             OB_ISNULL(data_type_str_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "table_schema or column_schema is NULL", K(ret));
  } else {
    ObObj* cells = NULL;
    const int64_t col_count = output_column_ids_.count();
    if (OB_ISNULL(cells = cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
    } else if (OB_UNLIKELY(col_count < 1 || col_count > COLUMNS_COLUMN_COUNT)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "column count error ", K(ret), K(col_count));
    } else if (OB_UNLIKELY(col_count > reserved_column_cnt_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cells count error", K(ret), K(col_count), K(reserved_column_cnt_), K(database_name));
    } else {
      const ObDataTypeCastParams dtc_params = sql::ObBasicSessionInfo::create_dtc_params(session_);
      ObCastCtx cast_ctx(allocator_, &dtc_params, CM_NONE, ObCharset::get_system_collation());
      ObObj casted_cell;
      uint64_t cell_idx = 0;
      const int64_t col_count = output_column_ids_.count();
      for (int64_t k = 0; OB_SUCC(ret) && k < col_count; ++k) {
        uint64_t col_id = output_column_ids_.at(k);
        switch (col_id) {
          case TABLE_CATALOG: {
            cells[cell_idx].set_varchar(ObString::make_string("def"));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case TABLE_SCHEMA: {
            cells[cell_idx].set_varchar(database_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case TABLE_NAME: {
            cells[cell_idx].set_varchar(table_schema->get_table_name_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case COLUMN_NAME: {
            cells[cell_idx].set_varchar(column_schema->get_column_name_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case ORDINAL_POSITION: {
            cells[cell_idx].set_uint64(ordinal_position);
            break;
          }
          case COLUMN_DEFAULT: {
            casted_cell.reset();
            const ObObj* res_cell = NULL;
            ObObj def_obj = column_schema->get_cur_default_value();
            if (IS_DEFAULT_NOW_OBJ(def_obj)) {
              ObObj def_now_obj;
              def_now_obj.set_varchar(ObString::make_string(N_UPPERCASE_CUR_TIMESTAMP));
              cells[cell_idx] = def_now_obj;
            } else if (def_obj.is_bit() || ob_is_enum_or_set_type(def_obj.get_type())) {
              char* buf = NULL;
              int64_t buf_len = number::ObNumber::MAX_PRINTABLE_SIZE;
              int64_t pos = 0;
              if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(allocator_->alloc(buf_len))))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                SERVER_LOG(ERROR, "fail to allocate memory", K(ret));
              } else if (def_obj.is_bit()) {
                if (OB_FAIL(def_obj.print_varchar_literal(buf, buf_len, pos, TZ_INFO(session_)))) {
                  SERVER_LOG(WARN, "fail to print varchar literal", K(ret), K(def_obj), K(buf_len), K(pos), K(buf));
                } else {
                  cells[cell_idx].set_varchar(ObString(static_cast<int32_t>(pos), buf));
                }
              } else {
                if (OB_FAIL(
                        def_obj.print_plain_str_literal(column_schema->get_extended_type_info(), buf, buf_len, pos))) {
                  SERVER_LOG(
                      WARN, "fail to print plain str literal", KPC(column_schema), K(buf), K(buf_len), K(pos), K(ret));
                } else {
                  cells[cell_idx].set_varchar(ObString(static_cast<int32_t>(pos), buf));
                }
              }
            } else {
              if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, def_obj, casted_cell, res_cell))) {
                SERVER_LOG(WARN, "failed to cast to ObVarcharType object", K(ret), K(def_obj));
              } else if (OB_ISNULL(res_cell)) {
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(ERROR, "succ to cast to ObVarcharType, but res_cell is NULL", K(ret), K(def_obj));
              } else {
                cells[cell_idx] = *res_cell;
              }
            }

            if (OB_SUCC(ret)) {
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          case IS_NULLABLE: {
            ObString nullable_val = ObString::make_string(column_schema->is_nullable() ? "YES" : "NO");
            cells[cell_idx].set_varchar(nullable_val);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case DATA_TYPE: {
            if (OB_FAIL(ob_sql_type_str(data_type_str_,
                    column_type_str_len_,
                    column_schema->get_data_type(),
                    column_schema->get_collation_type()))) {
              SERVER_LOG(WARN, "fail to get data type str", K(ret), K(column_schema->get_data_type()));
            } else {
              ObString type_val(column_type_str_len_, static_cast<int32_t>(strlen(data_type_str_)), data_type_str_);
              cells[cell_idx].set_varchar(type_val);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          case CHARACTER_MAXIMUM_LENGTH: {
            if (ob_is_string_type(column_schema->get_data_type())) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(column_schema->get_data_length()));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
          case CHARACTER_OCTET_LENGTH: {
            if (ob_is_string_tc(column_schema->get_data_type())) {
              ObCollationType coll = column_schema->get_collation_type();
              int64_t mbmaxlen = 0;
              if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(coll, mbmaxlen))) {
                SERVER_LOG(WARN, "failed to get mbmaxlen", K(ret), K(coll));
              } else {
                cells[cell_idx].set_uint64(static_cast<uint64_t>(mbmaxlen * column_schema->get_data_length()));
              }
            } else if (ob_is_text_tc(column_schema->get_data_type())) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(column_schema->get_data_length()));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
          case NUMERIC_PRECISION: {
            ObPrecision precision = column_schema->get_data_precision();
            // for float(xx), precision==x, scale=-1
            if (!share::is_oracle_mode() && column_schema->get_data_scale() < 0) {
              // mysql float(xx)'s NUMERIC_PRECISION is always 12 from Field.field_length
              // mysql double(xx)'s NUMERIC_PRECISION is always 22  from Field.field_length
              // as ob does not have Field.field_length, we set hard code here for compat
              if (ob_is_float_type(column_schema->get_data_type())) {
                precision = MAX_FLOAT_STR_LENGTH;
              } else if (ob_is_double_type(column_schema->get_data_type())) {
                precision = MAX_DOUBLE_STR_LENGTH;
              }
            }
            if (ob_is_numeric_type(column_schema->get_data_type()) && precision >= 0) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(precision));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
          case NUMERIC_SCALE: {
            ObScale scale = column_schema->get_data_scale();
            if (ob_is_numeric_type(column_schema->get_data_type()) && scale >= 0) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(column_schema->get_data_scale()));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
          case DATETIME_PRECISION: {
            if (ob_is_datetime_tc(column_schema->get_data_type()) || ob_is_time_tc(column_schema->get_data_type())) {
              cells[cell_idx].set_uint64(static_cast<uint64_t>(column_schema->get_data_scale()));
            } else {
              cells[cell_idx].reset();
            }
            break;
          }
          case CHARACTER_SET_NAME: {
            cells[cell_idx].set_varchar(common::ObCharset::charset_name(column_schema->get_charset_type()));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case COLLATION_NAME: {
            cells[cell_idx].set_varchar(common::ObCharset::collation_name(column_schema->get_collation_type()));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case COLUMN_TYPE: {
            int64_t pos = 0;
            const ObLengthSemantics default_length_semantics = session_->get_local_nls_length_semantics();
            if (OB_FAIL(get_type_str(column_schema->get_meta_type(),
                    column_schema->get_accuracy(),
                    column_schema->get_extended_type_info(),
                    default_length_semantics,
                    pos))) {
              SERVER_LOG(WARN, "fail to get column type str", K(ret), K(column_schema->get_data_type()));
            } else if (column_schema->is_zero_fill()) {
              // zerofill, only for int, float, decimal
              if (OB_FAIL(databuff_printf(column_type_str_, column_type_str_len_, pos, " zerofill"))) {
                SERVER_LOG(WARN, "fail to print zerofill", K(ret));
              }
            }
            if (OB_SUCC(ret)) {
              ObString type_val(column_type_str_len_, static_cast<int32_t>(strlen(column_type_str_)), column_type_str_);
              cells[cell_idx].set_varchar(type_val);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          case COLUMN_KEY: {
            if (column_schema->is_original_rowkey_column()) {
              cells[cell_idx].set_varchar("PRI");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            } else {
              // TODO: if (column_schema->is_index_column())
              cells[cell_idx].set_varchar("");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          case EXTRA: {
            // TODO
            ObString extra = ObString::make_string("");
            // auto_increment and on update current_timestamp will not appear on the same column at the same time
            if (column_schema->is_autoincrement()) {
              extra = ObString::make_string("auto_increment");
            } else if (column_schema->is_on_update_current_timestamp()) {
              extra = ObString::make_string("on update current_timestamp");
            }
            cells[cell_idx].set_varchar(extra);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case PRIVILEGES: {
            break;
          }
          case COLUMN_COMMENT: {
            cells[cell_idx].set_varchar(column_schema->get_comment_str());
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case GENERATION_EXPRESSION: {
            if ((1 == (column_schema->get_column_flags() & VIRTUAL_GENERATED_COLUMN_FLAG)) ||
                (2 == (column_schema->get_column_flags() & STORED_GENERATED_COLUMN_FLAG))) {
              cells[cell_idx].set_varchar(column_schema->get_orig_default_value().get_string());
            } else {
              cells[cell_idx].set_varchar("");
            }
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
            break;
          }
        }
        if (OB_SUCC(ret)) {
          ++cell_idx;
        }
      }
    }
  }

  return ret;
}

}  // namespace observer
}  // namespace oceanbase
