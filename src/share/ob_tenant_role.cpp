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

#include "share/ob_tenant_role.h"

using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase {
namespace share {

static const char* TENANT_ROLE_ARRAY[] = 
{
  "INVALID",
  "PRIMARY",
  "STANDBY",
  "RESTORE",
};

OB_SERIALIZE_MEMBER(ObTenantRole, value_);

const char* ObTenantRole::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(TENANT_ROLE_ARRAY) == MAX_TENANT, "array size mismatch");
  const char *type_str = "UNKNOWN";
  if (OB_UNLIKELY(value_ >= ARRAYSIZEOF(TENANT_ROLE_ARRAY)
                  || value_ < INVALID_TENANT)) {
    LOG_ERROR("fatal error, unknown tenant role", K_(value));
  } else {
    type_str = TENANT_ROLE_ARRAY[value_];
  }
  return type_str;
}

ObTenantRole::ObTenantRole(const ObString &str)
{
  value_ = INVALID_TENANT;
  if (str.empty()) {
  } else {
    for (int64_t i = 0; i < ARRAYSIZEOF(TENANT_ROLE_ARRAY); i++) {
      if (0 == str.case_compare(TENANT_ROLE_ARRAY[i])) {
        value_ = i;
        break;
      }
    }
  }

  if (INVALID_TENANT == value_) {
    LOG_WARN("invalid tenant role", K_(value), K(str));
  }
}

}  // share
}  // oceanbase
