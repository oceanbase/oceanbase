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

#ifndef OCEANBASE_SQL_OB_PHY_TABLE_LOCATION_
#define OCEANBASE_SQL_OB_PHY_TABLE_LOCATION_

#include "common/ob_range.h"
#include "share/partition_table/ob_partition_location.h"

namespace oceanbase
{
namespace sql
{
typedef common::ObSEArray<share::ObPartitionLocation, 1> ObPartitionLocationSEArray;
typedef common::ObSEArray<share::ObPartitionReplicaLocation, 1> ObPartitionReplicaLocationSEArray;

enum class ObDuplicateType : int64_t
{
  NOT_DUPLICATE = 0, //非复制表
  DUPLICATE,         //复制表, 可以选择任意副本
  DUPLICATE_IN_DML,  //被DML更改的复制表, 此时只能选leader副本
};

class ObPhyTableLocation final
{
  OB_UNIS_VERSION(1);
public:
  ObPhyTableLocation();
  int assign(const ObPhyTableLocation &other);

  TO_STRING_KV(K_(table_location_key),
               K_(ref_table_id),
               K_(partition_location_list),
               K_(duplicate_type));
private:
  int try_build_location_idx_map();
private:
  /* 用于表ID(可能是generated alias id)寻址location */
  uint64_t table_location_key_;
  /* 用于获取实际的物理表ID */
  uint64_t ref_table_id_;

  // The following two list has one element for each partition
  ObPartitionReplicaLocationSEArray partition_location_list_;
  ObDuplicateType duplicate_type_;

  // for lookup performance
  static const int FAST_LOOKUP_LOC_IDX_SIZE_THRES = 3;
  common::hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> location_idx_map_;
};


}
}
#endif /* OCEANBASE_SQL_OB_PHY_TABLE_LOCATION_ */
