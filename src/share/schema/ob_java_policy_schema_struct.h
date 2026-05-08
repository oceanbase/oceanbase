/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_JAVA_POLICY_SCHEMA_STRUCT_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_JAVA_POLICY_SCHEMA_STRUCT_H_

#include <cstdint>

#include "share/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

struct ObTenantJavaPolicyId
{

public:
  ObTenantJavaPolicyId()
    : tenant_id_(common::OB_INVALID_ID), java_policy_id_(common::OB_INVALID_ID)
  {  }

  ObTenantJavaPolicyId(const uint64_t tenant_id, const uint64_t java_policy_id)
    : tenant_id_(tenant_id), java_policy_id_(java_policy_id)
  {  }

  bool operator==(const ObTenantJavaPolicyId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (java_policy_id_ == rhs.java_policy_id_);
  }

  bool operator!=(const ObTenantJavaPolicyId &rhs) const
  {
    return !(*this == rhs);
  }

  bool operator<(const ObTenantJavaPolicyId &rhs) const
  {
    bool ret = false;
    if (tenant_id_ != rhs.tenant_id_) {
      ret = (tenant_id_ < rhs.tenant_id_);
    } else {
      ret = (java_policy_id_ < rhs.java_policy_id_);
    }
    return ret;
  }



  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = common::murmurhash(&java_policy_id_, sizeof(java_policy_id_), hash_val);
    return hash_val;
  }

  bool is_valid() const
  {
    return (common::OB_INVALID_ID != tenant_id_) && (common::OB_INVALID_ID != java_policy_id_);
  }

  TO_STRING_KV(K_(tenant_id), K_(java_policy_id));

  uint64_t tenant_id_;
  uint64_t java_policy_id_;
};


} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SRC_SHARE_SCHEMA_OB_JAVA_POLICY_SCHEMA_STRUCT_H_
