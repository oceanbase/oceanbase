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
#include "observer/virtual_table/ob_all_virtual_sys_variable_default_value.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/system_variable/ob_system_variable_factory.h"
#include "share/schema/ob_schema_getter_guard.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace observer
{

ObSysVarDefaultValue::ObSysVarDefaultValue()
    : ObVirtualTableScannerIterator()
{
}

ObSysVarDefaultValue::~ObSysVarDefaultValue()
{
}

void ObSysVarDefaultValue::reset()
{
  ObVirtualTableScannerIterator::reset();
}

int ObSysVarDefaultValue::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!((GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_2_0 &&
         GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0) ||
         GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_3_1_0)) {
    ret = OB_NOT_SUPPORTED;
    SERVER_LOG(WARN,"__all_virtual_sys_variable_default_value is only supported\
                     when cluster_version in the range [422,430) or [431,~)", K(ret));
  } else if (OB_ISNULL(cur_row_.cells_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "some data member is NULL", K(ret), K(cur_row_.cells_));
  } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells count is less than output column count",
                K(ret), K(cur_row_.count_), K(output_column_ids_.count()));
  } else if (start_to_read_) {
    //do nothing
  } else if (OB_FAIL(add_row())) {
    SERVER_LOG(WARN, "fail to add row", K(ret));
  } else {
    scanner_it_ = scanner_.begin();
    start_to_read_ = true;
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
  return ret;
}

int ObSysVarDefaultValue::add_row()
{
  int ret = OB_SUCCESS;
  ObObj *cells = nullptr;
  if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else {
    for (int64_t store_idx = 0; OB_SUCC(ret) && store_idx < ObSysVarFactory::ALL_SYS_VARS_COUNT; ++store_idx) {
      if (store_idx < 0 || store_idx >= ObSysVarFactory::ALL_SYS_VARS_COUNT) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected store idx", K(ret), K(store_idx));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < output_column_ids_.count(); ++j) {
          uint64_t col_id = output_column_ids_.at(j);
          switch(col_id) {
            case NAME: {
              cells[j].set_varchar(ObSysVariables::get_name(store_idx));
              cells[j].set_collation_type(ObSysVariables::get_default_sysvar_collation());
              break;
            }
            case DEFAULT_VALUE: {
              cells[j].set_varchar(ObSysVariables::get_value(store_idx));
              cells[j].set_collation_type(ObSysVariables::get_default_sysvar_collation());
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "invalid column id", K(ret), K(j),
                          K(output_column_ids_), K(col_id));
              break;
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(scanner_.add_row(cur_row_))) {
          SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
        }
      }
    }
  }
  return ret;
}

}
}
