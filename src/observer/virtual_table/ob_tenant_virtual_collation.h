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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_COLLATION_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_COLLATION_
#include "share/ob_virtual_table_scanner_iterator.h"
namespace oceanbase
{
namespace observer
{
class ObTenantVirtualCollation:public common::ObVirtualTableScannerIterator
{
public:
  ObTenantVirtualCollation()
  {
  }
  ~ObTenantVirtualCollation()
  {
    reset();
  }
  virtual void reset() override
  {
    ObVirtualTableScannerIterator::reset();
  }
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  ObTenantVirtualCollation(const ObTenantVirtualCollation &other)=delete;
  ObTenantVirtualCollation &operator=(const ObTenantVirtualCollation &other)=delete;
  enum COLLATION_COLUMN
  {
    COLLATION_TYPE = common::OB_APP_MIN_COLUMN_ID,
    COLLATION,
    CHARSET,
    ID,
    IS_DEFAULT,
    IS_COMPILED,
    SORTLEN
  };
  int fill_scanner();
};
}
}
#endif
