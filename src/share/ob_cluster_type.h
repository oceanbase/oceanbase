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

#ifndef OCEANBASE_COMMON_OB_CLUSTER_TYPE_H_
#define OCEANBASE_COMMON_OB_CLUSTER_TYPE_H_
#include <pthread.h>
#include "lib/string/ob_string.h"
namespace oceanbase {
namespace common {
enum ObClusterType {
  INVALID_CLUSTER_TYPE = 0,
  PRIMARY_CLUSTER = 1,
  STANDBY_CLUSTER = 2,
};

enum ObClusterStatus {
  INVALID_CLUSTER_STATUS = 0,
  CLUSTER_VALID = 1,
  CLUSTER_DISABLED,
  REGISTERED,
};

enum ObProtectionMode {
  MAXIMUM_PERFORMANCE_MODE = 0,
  MAXIMUM_AVAILABILITY_MODE = 1,
  MAXIMUM_PROTECTION_MODE = 2,
};

enum ObProtectionLevel {
  MAXIMUM_PERFORMANCE_LEVEL = 0,
  MAXIMUM_AVAILABILITY_LEVEL = 1,
  MAXIMUM_PROTECTION_LEVEL = 2,
  RESYNCHRONIZATION_LEVEL = 3,
};
const char* cluster_type_to_str(ObClusterType type);
const char* cluster_status_to_str(ObClusterStatus type);
const char* cluster_protection_mode_to_str(ObProtectionMode mode);
const char* cluster_protection_level_to_str(ObProtectionLevel level);
}  // end namespace common
}  // end namespace oceanbase

#endif  // OCEANBASE_COMMON_OB_CLUSTER_TYPE_H_
