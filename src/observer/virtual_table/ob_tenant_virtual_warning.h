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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_WARNING_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_WARNING_
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_range.h"
namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace observer
{
class ObTenantVirtualWarning : public common::ObVirtualTableScannerIterator
{
public:
  ObTenantVirtualWarning();
  virtual ~ObTenantVirtualWarning();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  int fill_scanner();
private:
  static const char* const SHOW_WARNING_STR;
  static const char* const SHOW_NOTE_STR;
  static const char* const SHOW_ERROR_STR;
  DISALLOW_COPY_AND_ASSIGN(ObTenantVirtualWarning);
};
}//observer
}//oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_WARNING_ */
