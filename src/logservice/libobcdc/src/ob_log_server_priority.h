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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_SERVER_PRIORITY_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_SERVER_PRIORITY_H_

#include "share/ob_define.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace libobcdc
{
// region priority
// The smaller the value, the higher the priority
enum RegionPriority
{
  REGION_PRIORITY_UNKNOWN = -1,
  REGION_PRIORITY_HIGH = 0,
  REGION_PRIORITY_LOW = 1,
  REGION_PRIORITY_MAX
};
const char *print_region_priority(RegionPriority type);

// Replica type priority
// The smaller the value, the higher the priority
//
// The priority is from highest to lowest as follows: L > R > F > OTHER
// where F is a fully functional copy, R is a read-only copy, L is a logged copy
// OTHER is the other type of replica, with the lowest default level
enum ReplicaPriority
{
  REPLICA_PRIORITY_UNKNOWN = -1,
  REPLICA_PRIORITY_LOGONLY = 0,
  REPLICA_PRIORITY_READONLY = 1,
  REPLICA_PRIORITY_FULL = 2,
  REPLICA_PRIORITY_OTHER = 3,
  REPLICA_PRIORITY_MAX
};
const char *print_replica_priority(ReplicaPriority type);

// Get replica priority based on replica type
int get_replica_priority(const common::ObReplicaType type,
    ReplicaPriority &priority);
}
}

#endif
