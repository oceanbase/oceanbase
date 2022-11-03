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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_SESSION_VARIABLES_TABLE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_SESSION_VARIABLES_TABLE_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTenantSchema;
}
}
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObInfoSchemaSessionVariablesTable : public common::ObVirtualTableScannerIterator
{
  static const int32_t SESSION_VARIABLES_COLUMN_COUNT = 2;
  enum COLUMN_NAME {
    VARIABLE_NAME = common::OB_APP_MIN_COLUMN_ID,
    VARIABLE_VALUE,
  };

public:
  ObInfoSchemaSessionVariablesTable();
  virtual ~ObInfoSchemaSessionVariablesTable();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

  inline void set_sys_variable_schema(const share::schema::ObSysVariableSchema *&sys_variable_schema)
  {
    sys_variable_schema_ = sys_variable_schema;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObInfoSchemaSessionVariablesTable);
private:
  const share::schema::ObSysVariableSchema *sys_variable_schema_;
};
}
}
#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SESSION_VARIABLES_
