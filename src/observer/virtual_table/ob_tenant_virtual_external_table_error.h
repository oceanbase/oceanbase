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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_EXTERNAL_TABLE_ERROR_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_EXTERNAL_TABLE_ERROR_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/ob_external_error_buffer.h"
#include "sql/session/ob_sql_session_info.h"
namespace oceanbase
{
namespace observer
{
class ObTenantVirtualExternalTableError:public common::ObVirtualTableScannerIterator
{
public:
  ObTenantVirtualExternalTableError(sql::ObSQLSessionInfo *session):session_(session)
  {
  }
  ~ObTenantVirtualExternalTableError()
  {
    reset();
  }
  virtual void reset() override
  {
    ObVirtualTableScannerIterator::reset();
  }
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  ObTenantVirtualExternalTableError(const ObTenantVirtualExternalTableError &other)=delete;
  ObTenantVirtualExternalTableError &operator=(const ObTenantVirtualExternalTableError &other)=delete;
  enum EXTERNAL_ERROR_COLUMN
  {
    ERROR_FILE_NAME = common::OB_APP_MIN_COLUMN_ID,
    ERROR_COLUMN_NAME,
    ERROR_ROW_NUMBER,
    ERROR_DATA,
    ERROR_DESCRIPTION
  };
  int fill_scanner();
  sql::ObSQLSessionInfo *session_;
};
}
}
#endif
