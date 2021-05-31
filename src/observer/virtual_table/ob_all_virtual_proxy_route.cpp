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

#include "ob_all_virtual_proxy_route.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace observer {

ObAllVirtualProxyRoute::ObAllVirtualProxyRoute()
    : ObAllVirtualProxyBaseIterator(), is_iter_end_(false), sql_string_(), tenant_name_(), database_name_()
{}

ObAllVirtualProxyRoute::~ObAllVirtualProxyRoute()
{}

int ObAllVirtualProxyRoute::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K_(schema_guard), K(ret));
  } else if (key_ranges_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "only one sql can be specified");
  } else {
    ObRowkey start_key = key_ranges_.at(0).start_key_;
    ObRowkey end_key = key_ranges_.at(0).end_key_;
    const ObObj* start_key_obj_ptr = start_key.get_obj_ptr();
    ;
    const ObObj* end_key_obj_ptr = end_key.get_obj_ptr();

    if (start_key.get_obj_cnt() < ROW_KEY_COUNT || end_key.get_obj_cnt() < ROW_KEY_COUNT ||
        OB_ISNULL(start_key_obj_ptr) ||
        OB_ISNULL(end_key_obj_ptr)) {  // not compare start_key and end_key as they are varchar
      ret = OB_ERR_UNEXPECTED;
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "sql_string, tenant_name, table_name, should all be specified");
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < ROW_KEY_COUNT); ++i) {
        if (!start_key_obj_ptr[i].is_varchar_or_char() || !end_key_obj_ptr[i].is_varchar_or_char()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_USER_ERROR(OB_ERR_UNEXPECTED, "sql_string, tenant_name, table_name, should all be specified");
        }
        switch (i) {
          case SQL_STRING_IDX:
            sql_string_ = start_key_obj_ptr[i].get_varchar();
            break;
          case TENANT_NAME_IDX:
            tenant_name_ = start_key_obj_ptr[i].get_varchar();
            break;
          case DATABASE_NAME_IDX:
            database_name_ = start_key_obj_ptr[i].get_varchar();
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid key", K(i), K(ret));
            break;
        }
      }
    }
  }
  return ret;
}

int ObAllVirtualProxyRoute::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema guard is NULL", K(ret));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else {
    ret = fill_cells();
    is_iter_end_ = true;
  }
  return ret;
}

int ObAllVirtualProxyRoute::fill_cells()
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj* cells = cur_row_.cells_;
  ObCollationType coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());
  if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case SQL_STRING:
        cells[i].set_varchar(sql_string_);
        cells[i].set_collation_type(coll_type);
        break;
      case TENANT_NAME:
        cells[i].set_varchar(tenant_name_);
        cells[i].set_collation_type(coll_type);
        break;
      case DATABASE_NAME:
        cells[i].set_varchar(database_name_);
        cells[i].set_collation_type(coll_type);
        break;
      case TABLE_NAME:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case TABLE_ID:
        cells[i].set_int(0);
        break;
      case CALCULATOR_BIN:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case RESULT_STATUS:
        cells[i].set_int(0);
        break;

      case SPARE1:
        cells[i].set_int(0);
        break;
      case SPARE2:
        cells[i].set_int(0);
        break;
      case SPARE3:
        cells[i].set_int(0);
        break;
      case SPARE4:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case SPARE5:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      case SPARE6:
        cells[i].set_varchar("");
        cells[i].set_collation_type(coll_type);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(i), K(ret));
        break;
    }
  }
  return ret;
}

}  // end of namespace observer
}  // end of namespace oceanbase
