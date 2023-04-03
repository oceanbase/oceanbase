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
#include "observer/virtual_table/ob_show_create_tablegroup.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_printer.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

ObShowCreateTablegroup::ObShowCreateTablegroup()
    : ObVirtualTableScannerIterator()
{
}

ObShowCreateTablegroup::~ObShowCreateTablegroup()
{
}

void ObShowCreateTablegroup::reset()
{
  ObVirtualTableScannerIterator::reset();
}

int ObShowCreateTablegroup::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member is NULL", K(ret), K(allocator_), K(schema_guard_));
  } else {
    if (!start_to_read_) {
      const ObTablegroupSchema *tg_schema = NULL;
      uint64_t show_tablegroup_id = OB_INVALID_ID;
      if (OB_FAIL(calc_show_tablegroup_id(show_tablegroup_id))) {
        LOG_WARN("fail to calc show tablegroup id", K(ret));
      } else if (OB_UNLIKELY(OB_INVALID_ID == show_tablegroup_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "this table is used for show clause, can't be selected");
      } else if (OB_UNLIKELY(NULL == (tg_schema = schema_guard_->get_tablegroup_schema(
                 effective_tenant_id_, show_tablegroup_id)))) {
        ret = OB_TABLEGROUP_NOT_EXIST;
        LOG_WARN("fail to get tablegroup schema", K(ret), K_(effective_tenant_id), K(show_tablegroup_id));
      } else {
        if (OB_FAIL(fill_row_cells(show_tablegroup_id, tg_schema->get_tablegroup_name_str()))) {
          LOG_WARN("fail to fill row cells", K(ret),
                     K(show_tablegroup_id), K(tg_schema->get_tablegroup_name_str()));
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

int ObShowCreateTablegroup::calc_show_tablegroup_id(uint64_t &show_tablegroup_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0;
       OB_SUCCESS == ret && OB_INVALID_ID == show_tablegroup_id && i < key_ranges_.count(); ++i) {
    ObRowkey start_key = key_ranges_.at(i).start_key_;
    ObRowkey end_key = key_ranges_.at(i).end_key_;
    const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
    const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
    if (start_key.get_obj_cnt() > 0 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
      if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
      } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0]
                 && ObIntType == start_key_obj_ptr[0].get_type()) {
        show_tablegroup_id = start_key_obj_ptr[0].get_int();
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObShowCreateTablegroup::fill_row_cells(uint64_t show_tablegroup_id,
                                         const ObString &tablegroup_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row_.cells_)
      || OB_ISNULL(schema_guard_)
      || OB_ISNULL(allocator_)
      || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("class isn't inited", K(cur_row_.cells_), K(schema_guard_), K(allocator_), K(session_), K(ret));
  } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN,
               "cur row cell count is less than output coumn",
               K(ret),
               K(cur_row_.count_),
               K(output_column_ids_.count()));
  } else {
    uint64_t cell_idx = 0;
    char *db_def_buf = NULL;
    int64_t db_def_buf_size = OB_MAX_VARCHAR_LENGTH;
    if (OB_UNLIKELY(NULL == (db_def_buf = static_cast<char *>(allocator_->alloc(db_def_buf_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("no memory");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
        case OB_APP_MIN_COLUMN_ID: {
          // tablegroup_id
          cur_row_.cells_[cell_idx].set_int(show_tablegroup_id);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 1: {
          // tablegroup_name
          cur_row_.cells_[cell_idx].set_varchar(tablegroup_name);
          cur_row_.cells_[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 2: {
          // create_tablegroup
          ObSchemaPrinter schema_printer(*schema_guard_);
          int64_t pos = 0;
          if (OB_FAIL(schema_printer.print_tablegroup_definition(effective_tenant_id_,
                                                                 show_tablegroup_id,
                                                                 db_def_buf,
                                                                 db_def_buf_size,
                                                                 pos,
                                                                 false,
                                                                 TZ_INFO(session_)))) {
            LOG_WARN("Generate tablegroup definition failed",
                     KR(ret), K(effective_tenant_id_), K(show_tablegroup_id));
          } else {
            const ObColumnSchemaV2 *column_schema = NULL;
            if (OB_ISNULL(table_schema_) ||
                OB_ISNULL(column_schema = table_schema_->get_column_schema(col_id))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("table or column schema is null", K(ret), KP(table_schema_), KP(column_schema));
            } else {
              const bool type_is_lob = column_schema->get_meta_type().is_lob();
              // for compatibility
              if (type_is_lob) {
                cur_row_.cells_[cell_idx].set_lob_value(ObLongTextType, db_def_buf, static_cast<int32_t>(pos));
              } else {
                ObString value_str(static_cast<int32_t>(db_def_buf_size), static_cast<int32_t>(pos), db_def_buf);
                cur_row_.cells_[cell_idx].set_varchar(value_str);
              }
              cur_row_.cells_[cell_idx].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", K(ret), K(cell_idx),
                     K(i), K(output_column_ids_), K(col_id));
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

}/* ns observer*/
}/* ns oceanbase */
