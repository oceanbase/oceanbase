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

#define USING_LOG_PREFIX COMMON

#include "share/ob_cluster_type.h"
#include "lib/ob_define.h"

namespace oceanbase {
namespace common {

// The standard view value
static const char* cluster_type_strs[] = {"INVALID", "PRIMARY", "PHYSICAL STANDBY"};
static const char* cluster_status_strs[] = {"INVALID", "VALID", "DISABLED", "REGISTERED"};

static const char* cluster_protection_mode_strs[] = {
    "MAXIMUM PERFORMANCE", "MAXIMUM AVAILABILITY", "MAXIMUM PROTECTION"};
static const char* cluster_protection_level_strs[] = {
    "MAXIMUM PERFORMANCE", "MAXIMUM AVAILABILITY", "MAXIMUM PROTECTION", "RESYNCHRONIZATION"};

const char* cluster_type_to_str(ObClusterType type)
{
  const char* type_str = "UNKNOWN";
  if (OB_UNLIKELY(type < INVALID_CLUSTER_TYPE) || OB_UNLIKELY(type > STANDBY_CLUSTER)) {
    LOG_ERROR("fatal error, unknown cluster type", K(type));
  } else {
    type_str = cluster_type_strs[type];
  }
  return type_str;
}
const char* cluster_status_to_str(ObClusterStatus status)
{
  const char* status_str = "UNKNOWN";
  if (OB_UNLIKELY(status < INVALID_CLUSTER_STATUS) || OB_UNLIKELY(status > REGISTERED)) {
    LOG_ERROR("fatal error, unknown cluster type", K(status));
  } else {
    status_str = cluster_status_strs[status];
  }
  return status_str;
}
const char* cluster_protection_mode_to_str(ObProtectionMode mode)
{
  const char* mode_str = "UNKNOWN";
  if (OB_UNLIKELY(mode < MAXIMUM_PERFORMANCE_MODE) || OB_UNLIKELY(mode > MAXIMUM_PROTECTION_MODE)) {
    LOG_ERROR("fatal error, unknown cluster protect mode", K(mode));
  } else {
    mode_str = cluster_protection_mode_strs[mode];
  }
  return mode_str;
}
const char* cluster_protection_level_to_str(ObProtectionLevel level)
{
  const char* level_str = "UNKNOWN";
  if (OB_UNLIKELY(level > RESYNCHRONIZATION_LEVEL) || OB_UNLIKELY(level < MAXIMUM_PERFORMANCE_LEVEL)) {
    LOG_ERROR("fatal error, unknown cluster level", K(level));
  } else {
    level_str = cluster_protection_level_strs[level];
  }
  return level_str;
}

}  // namespace common
}  // end namespace oceanbase
