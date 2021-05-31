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
#include "observer/virtual_table/ob_show_create_database.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_printer.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace observer {

ObShowCreateDatabase::ObShowCreateDatabase() : ObVirtualTableScannerIterator()
{}

ObShowCreateDatabase::~ObShowCreateDatabase()
{}

void ObShowCreateDatabase::reset()
{
  ObVirtualTableScannerIterator::reset();
}

int ObShowCreateDatabase::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member is NULL", K(ret), K(allocator_), K(schema_guard_));
  } else {
    if (!start_to_read_) {
      const ObDatabaseSchema* db_schema = NULL;
      uint64_t show_database_id = OB_INVALID_ID;
      if (OB_FAIL(calc_show_database_id(show_database_id))) {
        LOG_WARN("fail to calc show database id", K(ret));
      } else if (OB_UNLIKELY(OB_INVALID_ID == show_database_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "this table is used for show clause, can't be selected");
      } else if (OB_FAIL(schema_guard_->get_database_schema(show_database_id, db_schema))) {
        LOG_WARN("failed to get database_schema", K(ret), K(show_database_id));
      } else if (OB_UNLIKELY(NULL == db_schema)) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("db_schema is null", K(ret), K(show_database_id));
      } else {
        if (OB_FAIL(fill_row_cells(show_database_id, db_schema->get_database_name_str()))) {
          LOG_WARN("fail to fill row cells", K(ret), K(show_database_id), K(db_schema->get_database_name_str()));
        } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
          LOG_WARN("fail to add row", K(ret), K(cur_row_));
        } else {
          scanner_it_ = scanner_.begin();
          start_to_read_ = true;
        }
      }
    }
    if (OB_LIKELY(OB_SUCCESS == ret && start_to_read_)) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }
  return ret;
}

int ObShowCreateDatabase::calc_show_database_id(uint64_t& show_database_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCCESS == ret && OB_INVALID_ID == show_database_id && i < key_ranges_.count(); ++i) {
    ObRowkey start_key = key_ranges_.at(i).start_key_;
    ObRowkey end_key = key_ranges_.at(i).end_key_;
    const ObObj* start_key_obj_ptr = start_key.get_obj_ptr();
    const ObObj* end_key_obj_ptr = end_key.get_obj_ptr();
    if (start_key.get_obj_cnt() > 0 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
      if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
      } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0] && ObIntType == start_key_obj_ptr[0].get_type()) {
        show_database_id = start_key_obj_ptr[0].get_int();
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObShowCreateDatabase::fill_row_cells(uint64_t show_database_id, const ObString& database_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row_.cells_) || OB_ISNULL(schema_guard_) || OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("class isn't inited", K(cur_row_.cells_), K(schema_guard_), K(allocator_));
  } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN,
        "cur row cell count is less than output coumn",
        K(ret),
        K(cur_row_.count_),
        K(output_column_ids_.count()));
  } else {
    uint64_t cell_idx = 0;
    char* db_def_buf = NULL;
    int64_t db_def_buf_size = OB_MAX_VARCHAR_LENGTH;
    if (OB_UNLIKELY(NULL == (db_def_buf = static_cast<char*>(allocator_->alloc(db_def_buf_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID: {
          // database_id
          cur_row_.cells_[cell_idx].set_int(show_database_id);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 1: {
          // database_name
          cur_row_.cells_[cell_idx].set_varchar(database_name);
          cur_row_.cells_[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 2: {
          // create_database
          ObSchemaPrinter schema_printer(*schema_guard_);
          int64_t pos = 0;
          if (OB_FAIL(
                  schema_printer.print_database_definiton(show_database_id, false, db_def_buf, db_def_buf_size, pos))) {
            LOG_WARN("Generate database definition failed");
          } else {
            ObString value_str(static_cast<int32_t>(db_def_buf_size), static_cast<int32_t>(pos), db_def_buf);
            cur_row_.cells_[cell_idx].set_varchar(value_str);
            cur_row_.cells_[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 3: {
          // create_database_with_if_not_exists
          ObSchemaPrinter schema_printer(*schema_guard_);
          int64_t pos = 0;
          if (OB_FAIL(
                  schema_printer.print_database_definiton(show_database_id, true, db_def_buf, db_def_buf_size, pos))) {
            LOG_WARN("Generate database definition failed");
          } else {
            ObString value_str(static_cast<int32_t>(db_def_buf_size), static_cast<int32_t>(pos), db_def_buf);
            cur_row_.cells_[cell_idx].set_varchar(value_str);
            cur_row_.cells_[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", K(ret), K(cell_idx), K(i), K(output_column_ids_), K(col_id));
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
