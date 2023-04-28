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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_route_key.h"
#include "lib/ob_define.h"    // OB_INVALID_CLUSTER_ID, OB_INVALID_TENANT_ID

namespace oceanbase
{
namespace logservice
{
ObLSRouterKey::ObLSRouterKey()
    : cluster_id_(OB_INVALID_CLUSTER_ID),
      tenant_id_(OB_INVALID_TENANT_ID),
      ls_id_()
{
}

ObLSRouterKey::ObLSRouterKey(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
    : cluster_id_(cluster_id), tenant_id_(tenant_id), ls_id_(ls_id)
{
}

void ObLSRouterKey::reset()
{
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
}

bool ObLSRouterKey::operator ==(const ObLSRouterKey &other_key) const
{
  return cluster_id_ == other_key.cluster_id_
      && tenant_id_ == other_key.tenant_id_
      && ls_id_ == other_key.ls_id_;
}

bool ObLSRouterKey::operator !=(const ObLSRouterKey &other) const
{
  return !(*this == other);
}

bool ObLSRouterKey::is_valid() const
{
  return OB_INVALID_CLUSTER_ID != cluster_id_
      && OB_INVALID_TENANT_ID != tenant_id_
      && ls_id_.is_valid();
}

uint64_t ObLSRouterKey::hash() const
{
  uint64_t hash_val = 0;

  hash_val = murmurhash(&cluster_id_, sizeof(cluster_id_), hash_val);
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = murmurhash(&ls_id_, sizeof(ls_id_), hash_val);

  return hash_val;
}

int ObLSRouterKey::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

int ObLSRouterKey::deep_copy(char *buf,
    const int64_t buf_len,
    ObLSRouterKey *&key) const
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), "size", size());
  } else {
    ObLSRouterKey *pkey = new (buf) ObLSRouterKey();
    *pkey = *this;
    key = pkey;
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase

