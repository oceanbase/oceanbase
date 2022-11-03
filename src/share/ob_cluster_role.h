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

#ifndef OCEANBASE_COMMON_OB_CLUSTER_ROLE_H_
#define OCEANBASE_COMMON_OB_CLUSTER_ROLE_H_
#include <pthread.h>
#include "lib/string/ob_string.h"
namespace oceanbase
{
namespace common
{
enum ObClusterRole
{
  INVALID_CLUSTER_ROLE = 0,
  PRIMARY_CLUSTER = 1,
  STANDBY_CLUSTER = 2,
};

enum ObClusterStatus
{
  INVALID_CLUSTER_STATUS = 0,
  CLUSTER_VALID = 1,
  CLUSTER_DISABLED,
  REGISTERED,
  CLUSTER_DISABLED_WITH_READONLY,
  MAX_CLUSTER_STATUS,
};

enum ObProtectionMode
{
  INVALID_PROTECTION_MODE = 0,
  MAXIMUM_PERFORMANCE_MODE = 1,
  MAXIMUM_AVAILABILITY_MODE = 2,
  MAXIMUM_PROTECTION_MODE = 3,
  PROTECTION_MODE_MAX
};

enum ObProtectionLevel
{
  INVALID_PROTECTION_LEVEL = 0,
  MAXIMUM_PERFORMANCE_LEVEL = 1,
  MAXIMUM_AVAILABILITY_LEVEL = 2,
  MAXIMUM_PROTECTION_LEVEL = 3,
  RESYNCHRONIZATION_LEVEL = 4,
  MPF_TO_MPT_LEVEL = 5,// mpf->mpt middle state, used under mpf mode
  MPF_TO_MA_MPT_LEVEL = 6,// mpf->ma middle state, mpf->mpt middle state, used under ma mode
  PROTECTION_LEVEL_MAX
};
const char *cluster_role_to_str(ObClusterRole type);
const char *cluster_status_to_str(ObClusterStatus type);
const char *cluster_protection_mode_to_str(ObProtectionMode mode);
const char *cluster_protection_level_to_str(ObProtectionLevel level);
ObProtectionMode str_to_cluster_protection_mode(const common::ObString &str);
ObProtectionLevel str_to_cluster_protection_level(const common::ObString &str);
bool is_disabled_cluster(const ObClusterStatus status);

// Check primary protectioni level whether need sync-transport clog to standby
// ex. MA, MPT, and other middle level
bool need_sync_to_standby_level(const ObProtectionLevel level);

// Check standby protection level whether only receive sync-transported clog
// ex. MA, MPT, RESYNC
bool is_sync_level_on_standby(const ObProtectionLevel standby_level);

// Whether in steady state.
// Only steady state can change protection mode, switchover and so on.
bool is_steady_protection_level(const ObProtectionLevel level);

// The protection mode which need SYNC mode standby
// MPT or MA mode
bool has_sync_standby_mode(const ObProtectionMode mode);

//Check if the protection level is the MAXIMUM PROTECTION or MAXIMUM AVAILABILITY protection level
bool mpt_or_ma_protection_level(const ObProtectionLevel level);

// Updating mode and level is not atomic, to check whether mode and level can match
bool is_cluster_protection_mode_level_match(const ObProtectionMode mode, const ObProtectionLevel level);
}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_CLUSTER_ROLE_H_
