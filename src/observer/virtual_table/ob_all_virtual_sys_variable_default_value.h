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

#ifndef _OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_H_
#define _OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_H_
#include "share/ob_virtual_table_scanner_iterator.h"
namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
namespace schema
{
class ObTenantSchema;
class ObSysVariableSchema;
}
}
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObSysVarDefaultValue : public common::ObVirtualTableScannerIterator
{
public:
  ObSysVarDefaultValue();
  virtual ~ObSysVarDefaultValue();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  int add_row();
private:
  enum COLUMN_NAME
  {
    NAME = common::OB_APP_MIN_COLUMN_ID,
    DEFAULT_VALUE
  };
  DISALLOW_COPY_AND_ASSIGN(ObSysVarDefaultValue);
};
}
}
#endif /* _OB_ALL_VIRTUAL_SYS_VARIABLE_DEFAULT_VALUE_H_*/
