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

#include "observer/virtual_table/ob_session_variables.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/plan_cache/ob_plan_cache.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
namespace oceanbase
{
namespace observer
{

ObSessionVariables::ObSessionVariables()
    : ObVirtualTableScannerIterator(),
      sys_variable_schema_(NULL)
{
}

ObSessionVariables::~ObSessionVariables()
{
}

void ObSessionVariables::reset()
{
  session_ = NULL;
  ObVirtualTableScannerIterator::reset();
}

int ObSessionVariables::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)
      || OB_ISNULL(session_)
      || OB_ISNULL(sys_variable_schema_)
      || OB_ISNULL(cur_row_.cells_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(
        WARN, "data member is NULL", K(ret), K(allocator_), K(session_), K_(sys_variable_schema), K(cur_row_.cells_));
  } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN,
                   "cells count is less than output column count",
                   K(ret),
                   K(cur_row_.count_),
                   K(output_column_ids_.count()));
  } else {
    if (!start_to_read_) {
      ObObj *cells = cur_row_.cells_;
      ObString sys_var_show_str;
      for (int64_t i = 0; OB_SUCC(ret) && i < session_->get_sys_var_count(); ++i) {
        const ObBasicSysVar *sys_var = NULL;

        if (OB_UNLIKELY(NULL == (sys_var = session_->get_sys_var(i)))) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "sys var is NULL", K(ret), K(i));
        } else if (sys_var->is_invisible()) {
          // invisible, skip
        } else if (sys_var->is_oracle_only() && !session_->is_oracle_compatible()) {
          //skip oracle variable in mysql mode
        } else if (sys_var->is_mysql_only() && session_->is_oracle_compatible()) {
          //skip mysql variable in oracle mode
        } else {
          uint64_t cell_idx = 0;
          for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
            uint64_t col_id = output_column_ids_.at(j);
            switch(col_id) {
              case OB_APP_MIN_COLUMN_ID: {
                cells[cell_idx].set_varchar(sys_var->get_name());
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                break;
              }
              case OB_APP_MIN_COLUMN_ID + 1: {
                //deal with read_only
                if (share::SYS_VAR_READ_ONLY == sys_var->get_type()) {
                  //replace with tenant schema
                  if (sys_variable_schema_->is_read_only()) {
                    cells[cell_idx].set_varchar("ON");
                  } else {
                    cells[cell_idx].set_varchar("OFF");
                  }
                } else if (share::SYS_VAR_OB_PLAN_CACHE_PERCENTAGE == sys_var->get_type()
                           || share::SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE == sys_var->get_type()
                           || share::SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE == sys_var->get_type()) {
                  if (OB_FAIL(set_pc_conf(sys_var, cells[cell_idx]))) {
                    SERVER_LOG(WARN, "fail to set plan cache conf", K(ret), K(*sys_var));
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
                  cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                }
                break;
              }
              default: {
                ret = OB_ERR_UNEXPECTED;
                SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(j), K(output_column_ids_), K(col_id));
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
    if (OB_LIKELY(OB_SUCCESS == ret && start_to_read_)) {
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

int ObSessionVariables::set_pc_conf(const ObBasicSysVar *sys_var, ObObj &cell)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sys_var) || OB_ISNULL(allocator_) || OB_ISNULL(session_)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(sys_var), K(session_), K(ret));
  } else {
    char *buff = NULL;
    int64_t pos = 0;
    ObPlanCache *pc = session_->get_plan_cache();
    if (OB_ISNULL(pc)) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid argument", K(pc), K(ret));
    } else if (NULL == (buff = static_cast<char *>(allocator_->alloc(OB_MAX_CONFIG_VALUE_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "fail to alloc mem for plan cache conf", K(ret));
    } else if (share::SYS_VAR_OB_PLAN_CACHE_PERCENTAGE == sys_var->get_type()) {
      if (OB_FAIL(databuff_printf(buff, OB_MAX_CONFIG_VALUE_LEN, pos, "%ld", pc->get_mem_limit_pct()))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fail to transform plan cache conf from int to varchar", K(ret));
      } else {
        cell.set_varchar(buff, static_cast<int>(pos));
      }
    } else if (share::SYS_VAR_OB_PLAN_CACHE_EVICT_HIGH_PERCENTAGE == sys_var->get_type()) {
      if (OB_FAIL(databuff_printf(buff, OB_MAX_CONFIG_VALUE_LEN, pos, "%ld", pc->get_mem_high_pct()))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fail to transform plan cache conf from int to varchar", K(ret));
      } else {
        cell.set_varchar(buff, static_cast<int>(pos));
      }
    } else if (share::SYS_VAR_OB_PLAN_CACHE_EVICT_LOW_PERCENTAGE == sys_var->get_type()) {
      if (OB_FAIL(databuff_printf(buff, OB_MAX_CONFIG_VALUE_LEN, pos, "%ld", pc->get_mem_low_pct()))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fail to transform plan cache conf from int to varchar", K(ret));
      } else {
        cell.set_varchar(buff, static_cast<int>(pos));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid sys var type", K(ret));
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
