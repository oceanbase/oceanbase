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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_all_ddl_operation_schema_info.h"
#include "share/inner_table/ob_inner_table_schema.h"                // ObInnerTableSchema
#include "share/schema/ob_column_schema.h"                          // ObColumnSchemaV2

namespace oceanbase
{
namespace libobcdc
{
ObLogAllDdlOperationSchemaInfo::ObLogAllDdlOperationSchemaInfo() :
  all_ddl_operation_table_schema_(),
  col_des_array_()
{
}

int ObLogAllDdlOperationSchemaInfo::init()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(share::ObInnerTableSchema::all_ddl_operation_schema(all_ddl_operation_table_schema_))) {
    LOG_ERROR("ObInnerTableSchema all_ddl_operation_table_schema failed", KR(ret));
  } else if (OB_FAIL(all_ddl_operation_table_schema_.get_column_ids(col_des_array_))) {
    LOG_ERROR("all_ddl_operation_table_schema get_column_ids failed", KR(ret));
  } else {}

  return ret;
}

void ObLogAllDdlOperationSchemaInfo::reset()
{
  all_ddl_operation_table_schema_.reset();
  col_des_array_.reset();
}

void ObLogAllDdlOperationSchemaInfo::print()
{
  int ret = OB_SUCCESS;

  for (int64_t idx = 0; OB_SUCC(ret) && idx < col_des_array_.count(); ++idx) {
    const uint64_t column_id = col_des_array_[idx].col_id_;
    const share::schema::ObColumnSchemaV2 *column_table_schema = NULL;

    if (OB_ISNULL(column_table_schema = all_ddl_operation_table_schema_.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("column_table_schema is null", KR(ret), K(column_id), KPC(column_table_schema));
    } else {
      LOG_INFO("ALL_DDL_OPERATION_TABLE", K(idx), K(column_id), "name", column_table_schema->get_column_name());
    }
  } // for
}

} // namespace libobcdc
} // namespace oceanbase
