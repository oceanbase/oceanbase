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

#include "ob_replica_wrs_info.h"

namespace oceanbase {
using namespace common;
namespace share {

OB_SERIALIZE_MEMBER(ObReplicaWrsInfo, pkey_, weak_read_timestamp_, part_state_, replica_type_, replica_status_);

ObReplicaWrsInfo::ObReplicaWrsInfo()
    : pkey_(),
      weak_read_timestamp_(0),
      part_state_(1),     // storage::F_WORKING
      replica_type_(0),   // common::REPLICA_TYPE_FULL
      replica_status_(0)  // REPLICA_STATUS_NORMAL
{}

ObReplicaWrsInfo::ObReplicaWrsInfo(const common::ObPartitionKey& pkey, const int64_t wrs, const int32_t pstate,
    const int32_t replica_type, const int32_t replica_status)
    : pkey_(pkey),
      weak_read_timestamp_(wrs),
      part_state_(pstate),
      replica_type_(replica_type),
      replica_status_(replica_status)
{}

}  // namespace share
}  // namespace oceanbase
