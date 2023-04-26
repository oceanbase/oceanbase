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

#define USING_LOG_PREFIX SHARE

#include "lib/ob_name_id_def.h" //OB_ID
#include "share/ob_tenant_switchover_status.h"
#include "lib/utility/ob_print_utils.h" //TO_STRING_KV
#include "lib/trace/ob_trace_event.h"

using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase {
namespace share {

static const char* TENANT_SWITCHOVER_ARRAY[] = 
{
  "INVALID",
  "NORMAL",
  "SWITCHING TO PRIMARY",
  "PREPARE FLASHBACK",
  "FLASHBACK",
  "PREPARE SWITCHING TO STANDBY",
  "SWITCHING TO STANDBY",
  "PREPARE SWITCHING TO PRIMARY",
};

OB_SERIALIZE_MEMBER(ObTenantSwitchoverStatus, value_);
DEFINE_TO_YSON_KV(ObTenantSwitchoverStatus,
                  OB_ID(value), value_);

const char* ObTenantSwitchoverStatus::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(TENANT_SWITCHOVER_ARRAY) == MAX_STATUS, "array size mismatch");
  const char *type_str = "UNKNOWN";
  if (OB_UNLIKELY(value_ >= ARRAYSIZEOF(TENANT_SWITCHOVER_ARRAY)
                  || value_ < INVALID_STATUS)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown switchover status", K_(value));
  } else {
    type_str = TENANT_SWITCHOVER_ARRAY[value_];
  }
  return type_str;
}

ObTenantSwitchoverStatus::ObTenantSwitchoverStatus(const ObString &str)
{
  value_ = INVALID_STATUS;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(TENANT_SWITCHOVER_ARRAY); i++) {
      if (0 == str.case_compare(TENANT_SWITCHOVER_ARRAY[i])) {
        value_ = static_cast<ObTenantSwitchoverStatus::Status>(i);
        break;
      }
    }
  }

  if (INVALID_STATUS == value_) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid switchover status", K_(value), K(str));
  }
}

}  // share
}  // oceanbase
