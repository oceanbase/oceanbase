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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_DISK_INFO_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_DISK_INFO_H_

#include "lib/container/ob_array.h"
#include "share/ob_virtual_table_iterator.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
namespace common {
class ObTenantManager;
}
namespace observer {
class ObAllVirtualTenantDiskStat : public common::ObVirtualTableIterator {
public:
  ObAllVirtualTenantDiskStat();
  virtual ~ObAllVirtualTenantDiskStat();
  virtual int inner_open();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow*& row);

private:
  enum CACHE_COLUMN {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    ZONE,
    BLOCK_TYPE,
    BLOCK_SIZE,
  };
  int fill_tenant_ids();
  common::ObArray<uint64_t> tenant_ids_;
  common::ObString ipstr_;
  int64_t tenant_ids_index_;
  int64_t col_count_;
  int8_t attr_;
  bool has_start_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantDiskStat);
};
}  // namespace observer
}  // namespace oceanbase

#endif  // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_DISK_INFO_H_
