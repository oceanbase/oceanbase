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
