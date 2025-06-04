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

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_EXTERNAL_RESOURCE_SCHEMA_STRUCT_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_EXTERNAL_RESOURCE_SCHEMA_STRUCT_H_

#include <cstdint>

#include "share/ob_define.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

struct ObTenantExternalResourceId
{
  OB_UNIS_VERSION(1);

public:
  ObTenantExternalResourceId()
    : tenant_id_(common::OB_INVALID_ID), external_resource_id_(common::OB_INVALID_ID)
  {  }

  ObTenantExternalResourceId(const uint64_t tenant_id, const uint64_t external_resource_id)
    : tenant_id_(tenant_id), external_resource_id_(external_resource_id)
  {  }

  bool operator==(const ObTenantExternalResourceId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_) && (external_resource_id_ == rhs.external_resource_id_);
  }

  bool operator!=(const ObTenantExternalResourceId &rhs) const
  {
    return !(*this == rhs);
  }

  bool operator<(const ObTenantExternalResourceId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;

    if (tenant_id_ == rhs.tenant_id_) {
      bret = external_resource_id_ < rhs.external_resource_id_;
    }

    return bret;
  }

  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;

    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_ret);
    hash_ret = common::murmurhash(&external_resource_id_, sizeof(external_resource_id_), hash_ret);

    return hash_ret;
  }

  bool is_valid() const
  {
    return is_valid_tenant_id(tenant_id_) && (external_resource_id_ != common::OB_INVALID_ID);
  }
  TO_STRING_KV(K_(tenant_id), K_(external_resource_id));
  uint64_t tenant_id_;
  uint64_t external_resource_id_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SRC_SHARE_SCHEMA_OB_EXTERNAL_RESOURCE_SCHEMA_STRUCT_H_
