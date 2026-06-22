/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_tenant_virtual_charset.h"
#include "share/ob_compatibility_control.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{

int ObTenantVirtualCharset::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    if (OB_FAIL(fill_scanner())) {
      SERVER_LOG(WARN, "fail to fill scanner", K(ret));
    } else {
      start_to_read_ = true;
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

int ObTenantVirtualCharset::fill_scanner()
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  const ObCharsetWrapper *charset_wrap_arr = NULL;
  int64_t charset_wrap_arr_len = 0;
  ObCharsetCompatType charset_compat_type = CHARSET_COMPAT_MYSQL57;
  ObCharset::get_charset_wrap_arr(charset_wrap_arr, charset_wrap_arr_len);
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator is NULL", K(ret));
  } else if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell is NULL", K(ret));
  } else if (OB_ISNULL(charset_wrap_arr) ||
      OB_UNLIKELY(ObCharset::VALID_CHARSET_TYPES != charset_wrap_arr_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("charset wrap array is NULL or charset_wrap_arr_len is not CHARSET_WRAPPER_COUNT",
              K(ret), K(charset_wrap_arr), K(charset_wrap_arr_len));
  } else if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(session_->get_charset_compat_type(charset_compat_type))) {
    LOG_WARN("failed to get charset compat type", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < charset_wrap_arr_len; ++i) {
    int cell_idx = 0;
    ObCharsetWrapper charset_wrap = charset_wrap_arr[i];
    for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
      int64_t col_id = output_column_ids_.at(j);
      switch(col_id) {
        case CHARSET: {
          cells[cell_idx].set_varchar(
              ObString::make_string(ObCharset::charset_name(charset_wrap.charset_)));
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case DESCRIPTION: {
          cells[cell_idx].set_varchar(
              ObString::make_string(charset_wrap.description_));
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case DEFAULT_COLLATION: {
          cells[cell_idx].set_varchar(
              ObString::make_string(ObCharset::collation_name(
                  ObCharset::get_default_collation(
                    charset_wrap.charset_, charset_compat_type))));
          cells[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case MAX_LENGTH: {
          cells[cell_idx].set_int(charset_wrap.maxlen_);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(ERROR, "invalid column id", K(ret), K(cell_idx), K(j),
                     K(output_column_ids_), K(col_id));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    }//for
    if (OB_SUCCESS == ret && OB_FAIL(scanner_.add_row(cur_row_))) {
      LOG_WARN("fail to add row", K(ret), K(cur_row_));
    }
  }//for
  if (OB_SUCC(ret)) {
    scanner_it_ = scanner_.begin();
    start_to_read_ = true;
  }
  return ret;
}

}
}
