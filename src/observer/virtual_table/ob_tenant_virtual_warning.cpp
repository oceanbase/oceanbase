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

#include "lib/oblog/ob_warning_buffer.h"
#include "observer/virtual_table/ob_tenant_virtual_warning.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace observer
{
const char *const ObTenantVirtualWarning::SHOW_WARNING_STR = "Warning";
const char *const ObTenantVirtualWarning::SHOW_NOTE_STR = "Note";
const char *const ObTenantVirtualWarning::SHOW_ERROR_STR = "Error";

ObTenantVirtualWarning::ObTenantVirtualWarning()
    : ObVirtualTableScannerIterator()
{
}

ObTenantVirtualWarning::~ObTenantVirtualWarning()
{
}

void ObTenantVirtualWarning::reset()
{
  ObVirtualTableScannerIterator::reset();
}

int ObTenantVirtualWarning::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!start_to_read_) {
    if (OB_FAIL(fill_scanner())) {
      SERVER_LOG(WARN, "fail to fill scanner", K(ret));
    } else {/*do nothing*/}
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

int ObTenantVirtualWarning::fill_scanner()
{
  int ret = OB_SUCCESS;
  ObObj *cells = NULL;
  if (OB_UNLIKELY(NULL == allocator_ || NULL == session_ || NULL == cur_row_.cells_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN,
               "parameter or data member is not init",
               K(ret),
               K(allocator_),
               K(session_),
               K(cur_row_.cells_));
  } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN,
               "cells count is less than output column count",
               K(ret),
               K(cur_row_.count_),
               K(output_column_ids_.count()));
  } else {
    // be compatible with mysql, show error firs, then warnings;
    cells = cur_row_.cells_;
    const common::ObWarningBuffer &warn_buff = (NULL == session_->get_pl_context()) ?
                                                session_->get_show_warnings_buffer() :
                                                session_->get_warnings_buffer();
    uint32_t warn_buff_count = warn_buff.get_readable_warning_count();
    // fill error row(jus one row)
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(NULL == warn_buff.get_err_msg())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "err msg from warn buff is NULL", K(ret));
      } else if (0 != strlen(warn_buff.get_err_msg()) || OB_MAX_ERROR_CODE != warn_buff.get_err_code()) {
        uint64_t cell_idx = 0;
        for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count() ; ++j) {
          uint64_t col_id = output_column_ids_.at(j);
          switch(col_id) {
            case OB_APP_MIN_COLUMN_ID: {// level
              cells[cell_idx].set_varchar(ObString::make_string(SHOW_ERROR_STR));
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 1: {// code
              cells[cell_idx].set_int(ob_errpkt_errno(warn_buff.get_err_code(), lib::is_oracle_mode()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 2: {// message
              cells[cell_idx].set_varchar(ObString::make_string(warn_buff.get_err_msg()));
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 3: {// origin code
              cells[cell_idx].set_int(warn_buff.get_err_code());
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 4: {// sqlstate
              cells[cell_idx].set_varchar(ObString::make_string(warn_buff.get_sql_state()));
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
              //no need, delete later @hualong
              //case OB_APP_MIN_COLUMN_ID + 3: {// type
              //  cells[cell_idx].set_varchar(ObString::make_string("error"));
              //  break;
              // }
            default: {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(j),
                         K(output_column_ids_), K(col_id));
              break;
            }
          }
          if (OB_SUCC(ret)) {
            cell_idx++;
          }
        }
        if (OB_UNLIKELY(OB_SUCCESS == ret && OB_SUCCESS != (ret = scanner_.add_row(cur_row_)))) {
          SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
        }
      }
    }

    // fill warning rows
    for(uint32_t i = 0; OB_SUCC(ret) && i < warn_buff_count; ++i) {
      uint64_t cell_idx = 0;
      const ObWarningBuffer::WarningItem *item = warn_buff.get_warning_item(i);
      if (OB_LIKELY(NULL != item)) {
        for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
          uint64_t col_id = output_column_ids_.at(j);
          switch(col_id) {
            case OB_APP_MIN_COLUMN_ID: {// level
              if (ObLogger::USER_WARN == item->log_level_) {
                cells[cell_idx].set_varchar(ObString::make_string(SHOW_WARNING_STR));
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              } else if (ObLogger::USER_NOTE == item->log_level_) {
                cells[cell_idx].set_varchar(ObString::make_string(SHOW_NOTE_STR));
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              } else {
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "unknown warning type");
              }
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 1: {// code
              cells[cell_idx].set_int(ob_errpkt_errno(item->code_, lib::is_oracle_mode()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 2: {// message
              cells[cell_idx].set_varchar(ObString::make_string(item->msg_));
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 3: {// origin code
              cells[cell_idx].set_int(item->code_);
              break;
            }
            case OB_APP_MIN_COLUMN_ID + 4: {// sqlstate
              cells[cell_idx].set_varchar(ObString::make_string(item->sql_state_));
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
              //no need, delete later @hualong
              // case OB_APP_MIN_COLUMN_ID + 3: {// type
              //   cells[cell_idx].set_varchar(ObString::make_string("warning"));
              //   break;
              // }
            default: {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(j),
                         K(output_column_ids_), K(col_id));
              break;
            }
          }
          if (OB_SUCC(ret)) {
            cell_idx++;
          }
        }
        if (OB_UNLIKELY(OB_SUCCESS == ret && OB_SUCCESS != (ret = scanner_.add_row(cur_row_)))) {
          SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
        }
      }
    }


    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  return ret;
}


}/* ns observer*/
}/* ns oceanbase */
