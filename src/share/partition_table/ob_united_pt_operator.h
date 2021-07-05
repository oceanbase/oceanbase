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

#ifndef OCEANBASE_PARTITION_TABLE_OB_UNITED_PT_OPERATOR_H
#define OCEANBASE_PARTITION_TABLE_OB_UNITED_PT_OPERATOR_H
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_remote_partition_table_operator.h"
namespace oceanbase {
namespace share {
class ObPartitionInfo;
class ObUnitedPtOperator {
public:
  ObUnitedPtOperator();
  virtual ~ObUnitedPtOperator()
  {}
  int init(ObPartitionTableOperator* pt_operator, ObRemotePartitionTableOperator* remote_pt_operator);
  // specify a specific  table_id/partition_id to get partition info
  // when on standby cluster, return the partition infos both on primary and standby clusters.
  // when on primary cluster, return only the partition infos of the primary cluster, remote_partition is reset
  virtual int united_get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& partition_info);

private:
  // specify a specific  table_id/partition_id to get partition info
  // when on standby cluster, return the partition infos both on primary and standby clusters.
  // when on primary cluster, return only the partition infos of the primary cluster, remote_partition is reset
  int get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& local_partition,
      ObPartitionInfo& remote_partition);

private:
  bool inited_;
  ObPartitionTableOperator* pt_operator_;
  ObRemotePartitionTableOperator* remote_pt_operator_;
};
}  // namespace share
}  // namespace oceanbase
#endif
