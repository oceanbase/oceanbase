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

#include "ob_information_session_variables_table.h"

#include "share/schema/ob_schema_struct.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace sql;

namespace observer
{

ObInfoSchemaSessionVariablesTable::ObInfoSchemaSessionVariablesTable() :
    ObVirtualTableScannerIterator(),
    sys_variable_schema_(NULL)
{
}

ObInfoSchemaSessionVariablesTable::~ObInfoSchemaSessionVariablesTable()
{
  reset();
}

void ObInfoSchemaSessionVariablesTable::reset()
{
  ObVirtualTableScannerIterator::reset();
  sys_variable_schema_ = NULL;
}

int ObInfoSchemaSessionVariablesTable::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_) || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator or session_ is NULL", K(ret));
  } else if (OB_ISNULL(sys_variable_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tenant schema should not be NULL", K(ret));
  } else {
    if (!start_to_read_) {
      ObObj *cells = NULL;
      const int64_t col_count = output_column_ids_.count();
      if (OB_ISNULL(cells = cur_row_.cells_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
      } else if (OB_UNLIKELY(col_count < 0 ||
                             col_count > SESSION_VARIABLES_COLUMN_COUNT)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "column count error ", K(ret), K(col_count));
      } else if (OB_UNLIKELY(col_count > reserved_column_cnt_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cells count error", K(ret), K(col_count),
                   K(reserved_column_cnt_));
      } else {
        const ObBasicSysVar *sys_var = NULL;
        ObString sys_var_show_str;
        for (int64_t i = 0; OB_SUCC(ret) && i < session_->get_sys_var_count(); ++i) {
          if (NULL == (sys_var = session_->get_sys_var(i))) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "sys var is NULL", K(ret), K(i));
          } else if (sys_var->is_invisible()) {
            // invisible, skip
          } else {
            int64_t cell_idx = 0;
            uint64_t col_id = OB_INVALID_ID;
            for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
              col_id = output_column_ids_.at(j);
              switch (col_id) {
              case VARIABLE_NAME: {
                  cells[cell_idx].set_varchar(sys_var->get_name());
                  cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                    ObCharset::get_default_charset()));
                  break;
                }
              case VARIABLE_VALUE: {
                  //deal with read_only
                  if (SYS_VAR_READ_ONLY == sys_var->get_type()) {
                    //replace with tenant schema
                    if (sys_variable_schema_->is_read_only()) {
                      cells[cell_idx].set_varchar("ON");
                    } else {
                      cells[cell_idx].set_varchar("OFF");
                    }
                  } else {
                    sys_var_show_str.reset();
                    if (OB_FAIL(sys_var->to_show_str(*allocator_, *session_, sys_var_show_str))) {
                      SERVER_LOG(WARN, "fail to convert to show string", K(ret), K(*sys_var));
                    } else {
                      cells[cell_idx].set_varchar(sys_var_show_str);
                    }
                  }
                  if (OB_SUCC(ret)) {
                    cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                      ObCharset::get_default_charset()));
                  }
                  break;
                }
              default: {
                  ret = OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(j),
                             K(output_column_ids_), K(col_id));
                  break;
                }
              }
              if (OB_SUCC(ret)) {
                ++cell_idx;
              }
            }
            if (OB_SUCCESS == ret && OB_FAIL(scanner_.add_row(cur_row_))) {
              SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
            }
          }
        }
        if (OB_SUCC(ret)) {
          scanner_it_ = scanner_.begin();
          start_to_read_ = true;
        }
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
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase
