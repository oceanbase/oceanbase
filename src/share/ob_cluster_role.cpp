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

#include "share/ob_cluster_role.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{

// The standard view value
static const char *cluster_role_strs[] = { "INVALID", "PRIMARY", "PHYSICAL STANDBY" };
static const char *cluster_status_strs[] = { "INVALID", "VALID", "DISABLED", "REGISTERED",
                                             "DISABLED WITH READ ONLY"};

static const char *cluster_protection_mode_strs[] = { "INVALID PROTECTION MODE", 
                                                      "MAXIMUM PERFORMANCE", 
                                                      "MAXIMUM AVAILABILITY", 
                                                      "MAXIMUM PROTECTION"};
static const char *cluster_protection_level_strs[] = { "INVALID PROTECTION LEVEL", 
                                                       "MAXIMUM PERFORMANCE", 
                                                       "MAXIMUM AVAILABILITY", 
                                                       "MAXIMUM PROTECTION",
                                                       "RESYNCHRONIZATION", 
                                                       "MAXIMUM PERFORMANCE", 
                                                       "MAXIMUM PERFORMANCE"};
const char *cluster_role_to_str(ObClusterRole type)
{
  const char *type_str = "UNKNOWN";
  if (OB_UNLIKELY(type < INVALID_CLUSTER_ROLE) || OB_UNLIKELY(type > STANDBY_CLUSTER)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown cluster type", K(type));
  } else {
    type_str = cluster_role_strs[type];
  }
  return type_str;
}
const char *cluster_status_to_str(ObClusterStatus status)
{
  const char *status_str = "UNKNOWN";
  if (OB_UNLIKELY(status < INVALID_CLUSTER_STATUS) || OB_UNLIKELY(status >= MAX_CLUSTER_STATUS)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown cluster type", K(status));
  } else {
    status_str = cluster_status_strs[status];
  }
  return status_str;
}
const char *cluster_protection_mode_to_str(ObProtectionMode mode)
{
  STATIC_ASSERT(ARRAYSIZEOF(cluster_protection_mode_strs) == PROTECTION_MODE_MAX,
                "type string array size mismatch with enum ObProtectionMode count");

  const char *mode_str = "UNKNOWN";
  if (OB_UNLIKELY(mode < INVALID_PROTECTION_MODE) || OB_UNLIKELY(mode > MAXIMUM_PROTECTION_MODE)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown cluster protect mode", K(mode));
  } else {
    mode_str = cluster_protection_mode_strs[mode];
  }
  return mode_str;
}
const char *cluster_protection_level_to_str(ObProtectionLevel level)
{

  STATIC_ASSERT(ARRAYSIZEOF(cluster_protection_level_strs) == PROTECTION_LEVEL_MAX,
                "type string array size mismatch with enum ObProtectionLevel count");

  const char *level_str = "UNKNOWN";
  if (OB_UNLIKELY(level > MPF_TO_MA_MPT_LEVEL) || OB_UNLIKELY(level < INVALID_PROTECTION_LEVEL)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown cluster level", K(level));
  } else {
    level_str = cluster_protection_level_strs[level];
  }
  return level_str;
}

ObProtectionMode str_to_cluster_protection_mode(const common::ObString &str)
{
  ObProtectionMode mode = INVALID_PROTECTION_MODE;
  for (int64_t i = 0; i < ARRAYSIZEOF(cluster_protection_mode_strs); ++i) {
    if (0 == str.compare(cluster_protection_mode_strs[i])) {
      mode = static_cast<ObProtectionMode>(i);
      break;
    }
  }
  return mode;
}

ObProtectionLevel str_to_cluster_protection_level(const common::ObString &str)
{
  ObProtectionLevel level = INVALID_PROTECTION_LEVEL;
  for (int64_t i = 0; i < ARRAYSIZEOF(cluster_protection_level_strs); ++i) {
    if (0 == str.compare(cluster_protection_level_strs[i])) {
      level = static_cast<ObProtectionLevel>(i);
      break;
    }
  }
  return level;
}
bool need_sync_to_standby_level(const ObProtectionLevel level)
{
  return common::MAXIMUM_PROTECTION_LEVEL == level
         || common::MAXIMUM_AVAILABILITY_LEVEL == level
         || common::MPF_TO_MA_MPT_LEVEL == level
         || common::MPF_TO_MPT_LEVEL == level;
}
bool is_sync_level_on_standby(const ObProtectionLevel standby_level)
{
  return common::MAXIMUM_AVAILABILITY_LEVEL == standby_level
         || common::MAXIMUM_PROTECTION_LEVEL == standby_level
         || common::RESYNCHRONIZATION_LEVEL == standby_level;
}

bool is_steady_protection_level(const ObProtectionLevel level)
{
  return common::MAXIMUM_PROTECTION_LEVEL == level
         || common::MAXIMUM_AVAILABILITY_LEVEL == level 
         || common::MAXIMUM_PERFORMANCE_LEVEL == level
         || common::RESYNCHRONIZATION_LEVEL == level;
}

bool has_sync_standby_mode(const ObProtectionMode mode)
{
  return common::MAXIMUM_PROTECTION_MODE == mode
         || common::MAXIMUM_AVAILABILITY_MODE == mode;
}

/**
 * @description:
 *  Check if the protection level is the MAXIMUM PROTECTION or MAXIMUM AVAILABILITY protection level
 * @param[in] level protection level
 */
bool mpt_or_ma_protection_level(const ObProtectionLevel level)
{
  return common::MAXIMUM_PROTECTION_LEVEL == level
         || common::MAXIMUM_AVAILABILITY_LEVEL == level;
}
bool is_disabled_cluster(const ObClusterStatus status)
{
  return common::CLUSTER_DISABLED == status
         || common::CLUSTER_DISABLED_WITH_READONLY == status;
}

bool is_cluster_protection_mode_level_match(
    const ObProtectionMode mode, const ObProtectionLevel level)
{
  bool bret = true;
  if (common::INVALID_PROTECTION_MODE == mode
      || common::INVALID_PROTECTION_LEVEL == level) {
    bret = false;
  } else if (common::MAXIMUM_PERFORMANCE_MODE == mode
      && common::MAXIMUM_PERFORMANCE_LEVEL != level) {
    //if mode is mpf, level must be mpf.
    //regardless of the primary and standby cluster
    bret = false;
  } else if (common::MAXIMUM_PROTECTION_MODE == mode
             && common::MAXIMUM_AVAILABILITY_LEVEL == level) {
    bret = false;
  } else if (common::MAXIMUM_AVAILABILITY_MODE == mode
             && common::MAXIMUM_PROTECTION_LEVEL == level) {
    bret = false;
  }
  return bret;
}

}
}//end namespace oceanbase
