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

#include "lib/container/ob_array_serialization.h"  // ObSArray
#include "lib/utility/ob_unify_serialize.h"        // OB_UNIS_VERSION
#include "common/ob_partition_key.h"               // ObPartitionKey

#ifndef OCEANBASE_SHARE_OB_REPLICA_WRS_INFO_H_
#define OCEANBASE_SHARE_OB_REPLICA_WRS_INFO_H_

namespace oceanbase {
namespace share {

struct ObReplicaWrsInfo {
  OB_UNIS_VERSION(1);

public:
  ObReplicaWrsInfo();
  ObReplicaWrsInfo(const common::ObPartitionKey& pkey, const int64_t wrs, const int32_t pstate,
      const int32_t replica_type, const int32_t replica_status);

  TO_STRING_KV(K_(pkey), K_(weak_read_timestamp), K_(part_state), K_(replica_type), K_(replica_status));

public:
  common::ObPartitionKey pkey_;
  int64_t weak_read_timestamp_;

  // PartitionState, used to identify whether it is a readable and valid replica
  int32_t part_state_;      // ObPartitionState
  int32_t replica_type_;    // common::ObReplicaType
  int32_t replica_status_;  // share::ObReplicaStatus
};

typedef common::ObSArray<ObReplicaWrsInfo> ObReplicaWrsInfoList;
}  // namespace share
}  // namespace oceanbase
#endif
