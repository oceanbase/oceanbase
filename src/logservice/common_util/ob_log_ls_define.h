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

#ifndef OCEANBASE_LOGSERVICE_COMMON_LS_DEFINE_H_
#define OCEANBASE_LOGSERVICE_COMMON_LS_DEFINE_H_

#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace logservice
{
class TenantLSID
{
public:
  TenantLSID() { reset(); }
  TenantLSID(const uint64_t tenant_id, const share::ObLSID &ls_id)
    : tenant_id_(tenant_id), ls_id_(ls_id)
  {}
  ~TenantLSID() { reset(); }

  void reset()
  {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    ls_id_.reset();
  }

  bool is_valid() const {
    return ls_id_.is_valid_with_tenant(tenant_id_);
  }

  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_val);

    return hash_val;
  }

  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  bool is_sys_log_stream() const { return ls_id_.is_sys_ls(); }

  int compare(const TenantLSID &other) const;
  bool operator==(const TenantLSID &other) const
  {
    return (tenant_id_ == other.tenant_id_) &&(ls_id_ == other.ls_id_);
  }
  bool operator!=(const TenantLSID &other) const { return !operator==(other); }
  bool operator<(const TenantLSID &other) const { return -1 == compare(other); }
  TenantLSID &operator=(const TenantLSID &other);

  int64_t get_tenant_id() const { return tenant_id_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }

  TO_STRING_KV(K_(tenant_id),
      K_(ls_id));

private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

} // namespace logservice
} // namespace oceanbase

#endif
