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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TRANS_TABLE_STATUS_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TRANS_TABLE_STATUS_H_

#include "lib/container/ob_array.h"
#include "share/ob_virtual_table_iterator.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_partition_store.h"
#include "storage/ob_partition_meta_redo_module.h"

namespace oceanbase {
namespace common {
class ObTenantManager;
}
namespace observer {

class ObAllVirtualTransTableStatus : public common::ObVirtualTableIterator {
public:
  ObAllVirtualTransTableStatus();
  virtual ~ObAllVirtualTransTableStatus();
  virtual int init();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow*& row);

private:
  enum CACHE_COLUMN {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    ZONE,
    TABLE_ID,
    PARTITION_ID,
    END_LOG_ID,
    TRANS_CNT
  };
  common::ObString ipstr_;
  int64_t index_;
  storage::ObIPartitionArrayGuard partitions_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTransTableStatus);
};
}  // namespace observer
}  // namespace oceanbase

#endif  // OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TRANS_TABLE_STATUS_H_
