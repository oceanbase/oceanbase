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
#include "observer/virtual_table/ob_tenant_virtual_privilege.h"
#include "share/inner_table/ob_inner_table_schema_constants.h" // source of data
#include "lib/charset/ob_charset.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{

int ObTenantVirtualPrivilege::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    if (OB_FAIL(fill_scanner())) {
      LOG_WARN("fail to fill scanner", K(ret));
    } else {
      start_to_read_ = true;
    }
  }

  if (OB_SUCC(ret) && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObTenantVirtualPrivilege::fill_scanner()
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator is NULL", K(ret));  
  } else if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell is NULL", K(ret));
  } 

  int64_t row_count = sizeof(share::all_privileges) / sizeof(share::PrivilegeRow);
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count-1; i++) {
    //last row is NULL;
    int cell_idx = 0;
    share::PrivilegeRow privilege_row = share::all_privileges[i];
    for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
      int64_t col_id = output_column_ids_.at(j);
      switch(col_id) {
        case PRIVILEGE_COL: {
          cells[cell_idx].set_varchar(
            ObString::make_string(privilege_row.privilege_));
          cells[cell_idx].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case CONTEXT_COL: {
          cells[cell_idx].set_varchar(
            ObString::make_string(privilege_row.context_));
          cells[cell_idx].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case COMMENT_COL: {
          cells[cell_idx].set_varchar(
            ObString::make_string(privilege_row.comment_));
          cells[cell_idx].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid column id", K(ret), K(cell_idx), K(j), K(output_column_ids_), K(col_id));
          break;
        }
      }   
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    }//end for
    if (OB_SUCC(ret) && OB_FAIL(scanner_.add_row(cur_row_))) {
      LOG_WARN("fail to add row", K(ret), K(cur_row_));
    }
  }
  if (OB_SUCC(ret)) {
    scanner_it_ = scanner_.begin();
    start_to_read_ = true;
  }
  return ret;
}

}
}