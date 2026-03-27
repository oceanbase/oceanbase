/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_ALL_DDL_OPERATION_SCHEMA_INFO_H_
#define OCEANBASE_LOG_ALL_DDL_OPERATION_SCHEMA_INFO_H_

#include "share/schema/ob_table_schema.h"           // TableSchema
#include "share/schema/ob_table_param.h"            // ObColDesc

namespace oceanbase
{
namespace libobcdc
{
class ObLogAllDdlOperationSchemaInfo
{
public:
  ObLogAllDdlOperationSchemaInfo();
  ~ObLogAllDdlOperationSchemaInfo() { reset (); }
  int init();
  void reset();
  const share::schema::ObTableSchema &get_table_schema() const { return all_ddl_operation_table_schema_; }
  const ObArray<share::schema::ObColDesc> &get_cols_des_array() const { return col_des_array_; }

  // For test or debug
  void print();

private:
  share::schema::ObTableSchema all_ddl_operation_table_schema_;
  ObArray<share::schema::ObColDesc> col_des_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogAllDdlOperationSchemaInfo);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
