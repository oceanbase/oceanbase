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

#include "ob_server_priority.h"

namespace oceanbase
{
namespace logservice
{
using namespace oceanbase::common;

const char *print_region_priority(RegionPriority type)
{
  const char *str = nullptr;
  switch (type) {
    case REGION_PRIORITY_UNKNOWN:
      str = "UNKNOWN";
      break;
    case REGION_PRIORITY_LOW:
      str = "LOW";
      break;
    case REGION_PRIORITY_HIGH:
      str = "HIGH";
      break;
    default:
      str = "INVALID";
      break;
  }
  return str;
}

const char *print_replica_priority(ReplicaPriority type)
{
  const char *str = nullptr;
  switch (type) {
    case REPLICA_PRIORITY_UNKNOWN:
      str = "UNKNOWN";
      break;
    case REPLICA_PRIORITY_OTHER:
      str = "OTHER_REPLICA";
      break;
    case REPLICA_PRIORITY_FULL:
      str = "FULL";
      break;
    case REPLICA_PRIORITY_READONLY:
      str = "READ_ONLY";
      break;
    case REPLICA_PRIORITY_LOGONLY:
      str = "LOG_ONLY";
      break;
    default:
      str = "INVALID";
      break;
  }
  return str;
}

int get_replica_priority(const common::ObReplicaType type,
    ReplicaPriority &priority)
{
  int ret = OB_SUCCESS;
  priority = REPLICA_PRIORITY_UNKNOWN;

  if (!ObReplicaTypeCheck::is_replica_type_valid(type)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    switch(type) {
      case REPLICA_TYPE_FULL: {
        priority = REPLICA_PRIORITY_FULL;
        break;
      }
      case REPLICA_TYPE_READONLY: {
        priority = REPLICA_PRIORITY_READONLY;
        break;
      }
      case REPLICA_TYPE_LOGONLY: {
        priority = REPLICA_PRIORITY_LOGONLY;
        break;
      }
      default: {
        priority = REPLICA_PRIORITY_OTHER;
        break;
      }
    };
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase

