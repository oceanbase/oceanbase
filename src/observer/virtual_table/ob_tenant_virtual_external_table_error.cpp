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
#include "observer/virtual_table/ob_tenant_virtual_external_table_error.h"
// #include "common/ob_external_error_buffer.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{

int ObTenantVirtualExternalTableError::inner_get_next_row(common::ObNewRow *&row)
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

int ObTenantVirtualExternalTableError::fill_scanner()
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  common::ObExternalErrorBuffer ext_ = session_->get_external_error_buffer();
  common::ObExternalErrorBuffer::ExternalErrorWrapper ext_err_wrap{ext_.get_err_desc(), ext_.get_err_file_name(), ext_.get_err_data(),ext_.get_err_row(),ext_.get_err_column_name()};
  int64_t charset_wrap_arr_len = 0;
  common::ObExternalErrorBuffer::ExternalErrorWrapper tmp = ext_err_wrap;
  LOG_INFO("external static check 2",K(tmp.error_description),K(tmp.error_file_name),K(tmp.error_row_number),K(tmp.error_column_name),K(tmp.error_data));
  LOG_INFO("external table filename",
             K(ext_err_wrap.error_file_name));
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator is NULL", K(ret));
  } else if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell is NULL", K(ret));
  }
  // } else if (OB_ISNULL(ext_err_wrap)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_ERROR("external table error wrapper is NULL",
  //             K(ret), K(ext_err_wrap));
  // } 
  if (OB_SUCC(ret)){
    int cell_idx = 0;
    for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
    int64_t col_id = output_column_ids_.at(j);
    switch(col_id) {
      LOG_INFO("in col ids", K(col_id));
        case ERROR_DESCRIPTION: {
            cells[cell_idx].set_varchar(
                ext_err_wrap.error_description);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
        }
        case ERROR_FILE_NAME: {
            LOG_INFO("setting error filename", K(ext_err_wrap.error_file_name));
            cells[cell_idx].set_varchar(
                ext_err_wrap.error_file_name);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
        }
        case ERROR_DATA: {
            cells[cell_idx].set_varchar(
                ext_err_wrap.error_data);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
        }
        case ERROR_ROW_NUMBER: {
            cells[cell_idx].set_int(ext_err_wrap.error_row_number);
            break;
        }
        case ERROR_COLUMN_NAME: {
            cells[cell_idx].set_varchar(
                ext_err_wrap.error_column_name);
            cells[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
        }
        default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(ERROR, "invalid column id", K(ret), K(cell_idx), K(j),
                        K(output_column_ids_), K(col_id));
            break;
        }
    }
    LOG_INFO("cell info",K(cells[cell_idx]), K(cur_row_));
    if (OB_SUCC(ret)) {
      cell_idx++;
    }
    
    }//for

    
    if (OB_SUCCESS == ret && OB_FAIL(scanner_.add_row(cur_row_))) {
        LOG_WARN("fail to add row", K(ret), K(cur_row_));
    }
    LOG_INFO("scanner", K(scanner_));
  }
  if (OB_SUCC(ret)) {
    scanner_it_ = scanner_.begin();
    start_to_read_ = true;
  }
  return ret;
}

}
}
