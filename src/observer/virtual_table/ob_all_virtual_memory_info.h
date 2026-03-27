/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MEMORY_INFO_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MEMORY_INFO_H_

#include "lib/container/ob_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualMemoryInfo : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualMemoryInfo();
  virtual ~ObAllVirtualMemoryInfo();
  virtual int inner_open();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  enum CACHE_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    CTX_ID,
    LABEL,
    CTX_NAME,
    MOD_TYPE,
    MOD_ID,
    MOD_NAME,
    ZONE,
    HOLD,
    USED,
    COUNT,
    ALLOC_COUNT,
    FREE_COUNT,
  };
  uint64_t tenant_ids_[OB_MAX_SERVER_TENANT_CNT];
  char ip_buf_[common::OB_IP_STR_BUFF];
  int64_t col_count_;
  bool has_start_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualMemoryInfo);
};
}
}

#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_MEMORY_INFO_H_
