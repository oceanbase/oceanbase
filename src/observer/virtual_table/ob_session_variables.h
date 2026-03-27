/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SESSION_VARIABLES_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SESSION_VARIABLES_
#include "share/ob_virtual_table_scanner_iterator.h"
namespace oceanbase
{
namespace share
{
class ObBasicSysVar;
namespace schema
{
class ObTenantSchema;
}
}
namespace sql
{
class ObSQLSessionInfo;
}
namespace common
{
class ObObj;
}
namespace observer
{
class ObSessionVariables : public common::ObVirtualTableScannerIterator
{
public:
  ObSessionVariables();
  virtual ~ObSessionVariables();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_sys_variable_schema(const share::schema::ObSysVariableSchema *&sys_variable_schema)
  {
    sys_variable_schema_ = sys_variable_schema;
  }
private:
  int set_pc_conf(const share::ObBasicSysVar *sys_var, common::ObObj &cell);
private:
  const share::schema::ObSysVariableSchema *sys_variable_schema_;
  DISALLOW_COPY_AND_ASSIGN(ObSessionVariables);
};
}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SESSION_VARIABLES_ */
