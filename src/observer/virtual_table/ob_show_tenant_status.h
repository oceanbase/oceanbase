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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SHOW_TENANT_STATUS_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SHOW_TENANT_STATUS_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace share
{
namespace schema
{
class ObTenantSchema;
}
}
namespace observer
{

class ObShowTenantStatus : public common::ObVirtualTableScannerIterator
{
public:
  ObShowTenantStatus();
  virtual ~ObShowTenantStatus();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

  int add_tenant_status(const common::ObAddr &server_addr,
                        const share::schema::ObTenantSchema &tenant_schema,
                        const share::schema::ObSysVariableSchema &sys_variable_schema,
                        common::ObObj *cells,
                        const int64_t col_count);
  int add_all_tenant_status();
private:
  uint64_t tenant_id_;
private:
  enum TENANT_STATUS_COLUMN
  {
    TENANT_NAME = common::OB_APP_MIN_COLUMN_ID,
    HOST,
    PORT,
    READ_ONLY,
    MAX_TENANT_STATUS_COLUMN
  };
  static const int64_t TENANT_STATUS_COLUMN_COUNT = 4;
  DISALLOW_COPY_AND_ASSIGN(ObShowTenantStatus);
};

}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_SHOW_TENANT_STATUS_ */
