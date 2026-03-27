/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_CTX_MEMORY_INFO_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_CTX_MEMORY_INFO_H_

#include "lib/container/ob_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualTenantCtxMemoryInfo : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualTenantCtxMemoryInfo();
  virtual ~ObAllVirtualTenantCtxMemoryInfo();
  virtual int inner_open();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int add_row(uint64_t tenant_id, int64_t ctx_id, int64_t hold, int64_t used, int64_t limit);
private:
  enum CACHE_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    CTX_ID,
    CTX_NAME,
    HOLD,
    USED,
    LIMIT,
  };
  uint64_t tenant_ids_[OB_MAX_SERVER_TENANT_CNT];
  char ip_buf_[common::OB_IP_STR_BUFF];
  bool has_start_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantCtxMemoryInfo);
};
}
}

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_CTX_MEMORY_INFO_H_
