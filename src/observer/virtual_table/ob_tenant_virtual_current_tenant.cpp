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

#include "observer/virtual_table/ob_tenant_virtual_current_tenant.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_printer.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

ObTenantVirtualCurrentTenant::ObTenantVirtualCurrentTenant()
    : ObVirtualTableScannerIterator(),
      sql_proxy_(NULL)
{
}

ObTenantVirtualCurrentTenant::~ObTenantVirtualCurrentTenant()
{
}

void ObTenantVirtualCurrentTenant::reset()
{
  sql_proxy_ = NULL;
  ObVirtualTableScannerIterator::reset();
}

int ObTenantVirtualCurrentTenant::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  uint64_t show_tenant_id = OB_INVALID_ID;
  const int64_t col_count = output_column_ids_.count();
  int64_t pos = 0;
  if (OB_UNLIKELY(NULL == allocator_
                  || NULL == schema_guard_
                  || NULL == session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN,
               "data member is not init", K(ret), K(allocator_), K(schema_guard_), K(session_));
  } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN,
                   "cells count is less than output column count",
                   K(ret),
                   K(cur_row_.count_),
                   K(output_column_ids_.count()));
  } else {
    if (!start_to_read_) {
      // get tenant id
      for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_ID == show_tenant_id && i < key_ranges_.count(); ++i) {
        const ObRowkey &start_key = key_ranges_.at(i).start_key_;
        const ObRowkey &end_key = key_ranges_.at(i).end_key_;
        const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
        const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
        if (start_key.get_obj_cnt() > 0 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
          if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
          } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0]
                     && ObIntType == start_key_obj_ptr[0].get_type()) {
            show_tenant_id = start_key_obj_ptr[0].get_int();
          } else {/*do nothing*/}
        }
      }
      // fill scanner
      if (OB_SUCC(ret)) {
        ObObj *cells = NULL;
        char *create_stmt = NULL;
        if (OB_UNLIKELY(NULL == (create_stmt = static_cast<char *>(allocator_->alloc(OB_MAX_VARCHAR_LENGTH))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SERVER_LOG(ERROR, "fail to alloc memory", K(ret));
        } else if (OB_UNLIKELY(OB_INVALID_ID == show_tenant_id)) {
          // FIXME(tingshuai.yts):暂时定为显示该错误信息，只有直接查询该虚拟表才可能出现
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "select a table which is used for show clause");
        } else if (OB_FAIL(schema_guard_->get_tenant_info(show_tenant_id, tenant_schema))) {
          SERVER_LOG(WARN, "get tenant info failed", K(show_tenant_id), K(ret));
        } else if (OB_ISNULL(tenant_schema)) {
          ret = OB_TENANT_NOT_EXIST;
          SERVER_LOG(WARN, "Unknow tenant", K(ret), K(show_tenant_id));
        } else if (OB_ISNULL(cells = cur_row_.cells_)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
        } else {
          uint64_t cell_idx = 0;
          ObSchemaPrinter schema_printer(*schema_guard_);
          for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
            uint64_t col_id = output_column_ids_.at(i);
            switch(col_id) {//tenant_name
              case OB_APP_MIN_COLUMN_ID: {
                cells[cell_idx].set_int(static_cast<int64_t>(show_tenant_id));
                break;
              }
              case OB_APP_MIN_COLUMN_ID + 1: {
                cells[cell_idx].set_varchar(ObString::make_string(tenant_schema->get_tenant_name()));
                cells[cell_idx].set_collation_type(
                    ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              }
              case OB_APP_MIN_COLUMN_ID + 2: {//create_stmt
                if (OB_FAIL(schema_printer.print_tenant_definition(show_tenant_id,
                                                                    sql_proxy_,
                                                                    create_stmt,
                                                                    OB_MAX_VARCHAR_LENGTH,
                                                                    pos,
                                                                    false/*is_agent_mode*/))) {
                  SERVER_LOG(WARN, "print tenant definition fail", K(ret), K(show_tenant_id));
                } else {
                  cells[cell_idx].set_varchar(ObString::make_string(create_stmt));
                  cells[cell_idx].set_collation_type(
                      ObCharset::get_default_collation(ObCharset::get_default_charset()));
                }
                break;
              }
              default: {
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                           K(i), K(output_column_ids_), K(col_id));
                break;
              }
            } //switch
            if (OB_SUCC(ret)) {
              cell_idx++;
            }
          } // for
          if (OB_UNLIKELY(OB_SUCCESS == ret && OB_SUCCESS != (ret = scanner_.add_row(cur_row_)))) {
            SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
          } else {
            scanner_it_ = scanner_.begin();
            start_to_read_ = true;
          }
        }
      }
    } // if (!start_to_read_)
    if (OB_SUCCESS == ret && start_to_read_) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }
  return ret;
}
} // observer
} // namespace
