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

#ifndef OCEANBASE_OBSERVER_ALL_VIRTUAL_AUDIT_OPERATION_
#define OCEANBASE_OBSERVER_ALL_VIRTUAL_AUDIT_OPERATION_

#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualAuditOperationTable : public common::ObVirtualTableScannerIterator
{
#ifndef OB_BUILD_AUDIT_SECURITY
public:
  ObAllVirtualAuditOperationTable() {}
  virtual ~ObAllVirtualAuditOperationTable() {}
  virtual int inner_get_next_row(common::ObNewRow *&row) { return OB_ITER_END; }
#else
  static const int32_t AUDIT_OPERATION_COLUMN_COUNT = 3;
  enum COLUMN_NAME {
    OPERATION_TYPE = common::OB_APP_MIN_COLUMN_ID,
    OPERATION_NAME,
    AUDIT_TYPE_NAME,
  };

public:
  ObAllVirtualAuditOperationTable();
  virtual ~ObAllVirtualAuditOperationTable();

  virtual int inner_get_next_row(common::ObNewRow *&row);
#endif

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualAuditOperationTable);
};
} // namespace observer
} // namespace oceanbase
#endif // OCEANBASE_OBSERVER_ALL_VIRTUAL_AUDIT_OPERATION_
