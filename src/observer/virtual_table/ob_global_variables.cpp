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

#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_schema_struct.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "observer/virtual_table/ob_global_variables.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/system_variable/ob_system_variable_factory.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace observer
{

ObGlobalVariables::ObGlobalVariables()
    : ObVirtualTableScannerIterator(),
      sql_proxy_(NULL),
      sys_variable_schema_(NULL)
{
}

ObGlobalVariables::~ObGlobalVariables()
{
}

void ObGlobalVariables::reset()
{
  sql_proxy_ = NULL;
  sys_variable_schema_ = NULL;
  ObVirtualTableScannerIterator::reset();
}

int ObGlobalVariables::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)
      || OB_ISNULL(session_)
      || OB_ISNULL(sys_variable_schema_)
      || OB_ISNULL(cur_row_.cells_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "some data member is NULL", K(ret), K(allocator_), K(session_),
               K(sys_variable_schema_), K(sql_proxy_), K(cur_row_.cells_));
  } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cells count is less than output column count",
                   K(ret), K(cur_row_.count_), K(output_column_ids_.count()));
  } else {
    if (!start_to_read_) {
      const int64_t col_count = output_column_ids_.count();
      ObObj *cells = cur_row_.cells_;
      ObSysVarFactory sys_var_fac;
      for (int64_t i = 0; OB_SUCC(ret) && i < sys_variable_schema_->get_sysvar_count(); ++i) {
        const ObSysVarSchema *sysvar_schema = sys_variable_schema_->get_sysvar_schema(i);
        if (sysvar_schema != NULL) {
          ObSysVarClassType var_id = ObSysVarFactory::find_sys_var_id_by_name(sysvar_schema->get_name(), true);
          ObBasicSysVar *sysvar = NULL;
          ObObj value;
          const common::ObDataTypeCastParams dtc_params
                    = ObBasicSessionInfo::create_dtc_params(session_);
          if (SYS_VAR_INVALID == var_id) {
            SERVER_LOG(INFO, "system variable maybe come from diff version", K(*sysvar_schema));
          } else if (OB_FAIL(sys_var_fac.create_sys_var(var_id, sysvar)) || OB_ISNULL(sysvar)) {
            ret = COVER_SUCC(OB_ERR_UNEXPECTED);
            SERVER_LOG(WARN, "create system variable failed", K(ret), K(var_id));
          } else if (OB_FAIL(sysvar_schema->get_value(allocator_, dtc_params, value))) {
            SERVER_LOG(WARN, "get value of sysvar schema failed", K(ret));
          } else {
            sysvar->set_value(value);
            sysvar->set_data_type(sysvar_schema->get_data_type());
            sysvar->set_flags(sysvar_schema->get_flags());
          }
          if (OB_FAIL(ret) || OB_ISNULL(sysvar)) {
            //do nothing
          } else if (sysvar->is_invisible()) {
            //is invisible, skip it
          } else if (!sysvar->is_global_scope()) {
            //is global, skip it
          } else if (sysvar->is_oracle_only() && !session_->is_oracle_compatible()) {
            //is oracle only, skip it
          } else if (sysvar->is_mysql_only() && session_->is_oracle_compatible()) {
            //is mysql only, skip it
          } else {
            uint64_t cell_idx = 0;
            for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
              uint64_t col_id = output_column_ids_.at(j);
              switch(col_id) {
                case OB_APP_MIN_COLUMN_ID: {
                  cells[cell_idx].set_varchar(sysvar->get_name());
                  cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                          ObCharset::get_default_charset()));
                  break;
                }
                case OB_APP_MIN_COLUMN_ID + 1: {
                  //deal with read_only
                  if (sysvar->get_type() == SYS_VAR_READ_ONLY) {
                    //replace with tenant schema
                    if (sys_variable_schema_->is_read_only()) {
                      cells[cell_idx].set_varchar("ON");
                    } else {
                      cells[cell_idx].set_varchar("OFF");
                    }
                  } else if (share::SYS_VAR_VERSION == sysvar->get_type()
                             && 0 == sysvar->get_value().get_string_len()) {
                    cells[cell_idx].set_varchar(ObSpecialSysVarValues::version_);
                  } else {
                    ObString show_str;
                    if (OB_FAIL(sysvar->to_show_str(*allocator_, *session_, show_str))) {
                      SERVER_LOG(WARN, "convert to show str failed", K(ret));
                    } else {
                      cells[cell_idx].set_varchar(show_str);
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
                cell_idx++;
              }
            }
            if (OB_SUCC(ret) && OB_FAIL(scanner_.add_row(cur_row_))) {
              SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
    if (OB_LIKELY(OB_SUCC(ret) && start_to_read_)) {
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

}/* ns observer*/
}/* ns oceanbase */
