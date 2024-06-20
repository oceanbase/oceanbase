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
#include "share/ob_tenant_role.h"
#include "lib/utility/ob_print_utils.h" //TO_STRING_KV
#include "lib/trace/ob_trace_event.h"

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
  "CLONE",
};

OB_SERIALIZE_MEMBER(ObTenantRole, value_);
DEFINE_TO_YSON_KV(ObTenantRole,
                  OB_ID(value), value_);

const char* ObTenantRole::to_str() const
{
  STATIC_ASSERT(ARRAYSIZEOF(TENANT_ROLE_ARRAY) == MAX_TENANT, "array size mismatch");
  const char *type_str = "UNKNOWN";
  if (OB_UNLIKELY(value_ >= ARRAYSIZEOF(TENANT_ROLE_ARRAY)
                  || value_ < INVALID_TENANT)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "fatal error, unknown tenant role", K_(value));
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
        value_ = static_cast<ObTenantRole::Role>(i);
        break;
      }
    }
  }

  if (INVALID_TENANT == value_) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid tenant role", K_(value), K(str));
  }
}

#define GEN_IS_TENANT_ROLE(TENANT_ROLE_VALUE, TENANT_ROLE) \
  bool is_##TENANT_ROLE##_tenant(const ObTenantRole::Role value) { return TENANT_ROLE_VALUE == value; }

GEN_IS_TENANT_ROLE(ObTenantRole::Role::INVALID_TENANT, invalid)
GEN_IS_TENANT_ROLE(ObTenantRole::Role::PRIMARY_TENANT, primary)
GEN_IS_TENANT_ROLE(ObTenantRole::Role::STANDBY_TENANT, standby)
GEN_IS_TENANT_ROLE(ObTenantRole::Role::RESTORE_TENANT, restore)
GEN_IS_TENANT_ROLE(ObTenantRole::Role::CLONE_TENANT, clone)
#undef GEN_IS_TENANT_ROLE


}  // share
}  // oceanbase
