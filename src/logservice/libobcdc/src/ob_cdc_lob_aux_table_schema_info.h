/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIBOBCDC_LOB_AUX_TABLE_SCHEMA_INFO_H_
#define OCEANBASE_LIBOBCDC_LOB_AUX_TABLE_SCHEMA_INFO_H_

#include "share/schema/ob_table_schema.h"           // TableSchema
#include "share/schema/ob_table_param.h"            // ObColDesc

namespace oceanbase
{
namespace libobcdc
{
class ObCDCLobAuxTableSchemaInfo
{
public:
  ObCDCLobAuxTableSchemaInfo();
  ~ObCDCLobAuxTableSchemaInfo() { reset (); }
  int init();
  void reset();
  const share::schema::ObTableSchema &get_table_schema() const { return table_schema_; }
  const ObArray<share::schema::ObColDesc> &get_cols_des_array() const { return col_des_array_; }

  // For test or debug
  void print();

private:
  share::schema::ObTableSchema table_schema_;
  ObArray<share::schema::ObColDesc> col_des_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCDCLobAuxTableSchemaInfo);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
