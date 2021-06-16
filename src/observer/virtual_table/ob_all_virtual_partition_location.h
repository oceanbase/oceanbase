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

#ifndef OCEANBASE_OBSERVER_ALL_VIRTUAL_PARTITION_LOCATION_
#define OCEANBASE_OBSERVER_ALL_VIRTUAL_PARTITION_LOCATION_

#include "common/ob_range.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_virtual_table_iterator.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_table_operator.h"

namespace oceanbase {
namespace share {
class ObPartitionInfo;
class ObPartitionTableOperator;
}  // namespace share
namespace observer {
class ObAllVirtualPartitionLocation : public common::ObVirtualTableIterator {
  enum ALL_VIRTUAL_PARTITION_LOCATION_COLUMNS {
    TENANT_ID = oceanbase::common::OB_APP_MIN_COLUMN_ID,
    TABLE_ID,
    PARTITION_ID,
    SVR_IP,
    SVR_PORT,
    UNIT_ID,
    PARTITION_CNT,
    ZONE,
    ROLE,
    MEMBER_LIST,
    REPLICA_TYPE,
    STATUS,
    DATA_VERSION,
  };

public:
  ObAllVirtualPartitionLocation();
  virtual ~ObAllVirtualPartitionLocation();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow*& row);

  int init(share::ObPartitionTableOperator& pt_operator);

private:
  int get_query_key(uint64_t& tenant_id, uint64_t& table_id, int64_t& partition_id);
  int fill_row(const share::ObPartitionReplica& replica, common::ObNewRow*& row);

private:
  bool inited_;
  ObArenaAllocator arena_allocator_;
  share::ObPartitionTableOperator* pt_operator_;
  share::ObPartitionInfo partition_info_;
  int64_t next_replica_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPartitionLocation);
};
}  // end of namespace observer
}  // end of namespace oceanbase
#endif /* OCEANBASE_OBSERVER_ALL_VIRTUAL_PARTITION_LOCATION_ */
