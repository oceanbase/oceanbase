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
 *
 * definition of PartTransID(identify a PartTrans)
 */

#include "ob_cdc_define.h"

namespace oceanbase
{
namespace libobcdc
{

TenantTransID::TenantTransID(const uint64_t tenant_id, const transaction::ObTransID tx_id) :
    tenant_id_(tenant_id), trans_id_(tx_id)
{}

TenantTransID::TenantTransID()
{
  reset();
}

TenantTransID::~TenantTransID()
{
  reset();
}

void TenantTransID::reset()
{
  tenant_id_ = common::OB_INVALID_TENANT_ID;
  trans_id_ = -1;
}

int TenantTransID::compare(const TenantTransID &other) const
{
  int cmp_ret = 0;

  if (tenant_id_ > other.tenant_id_) {
    cmp_ret = 1;
  } else if (tenant_id_ < other.tenant_id_) {
    cmp_ret = -1;
  } else if (trans_id_ > other.trans_id_) {
    cmp_ret = 1;
  } else if (trans_id_ < other.trans_id_) {
    cmp_ret = -1;
  } else {
    cmp_ret = 0;
  }

  return cmp_ret;
}

bool TenantTransID::operator==(const TenantTransID &other) const
{
  return (tenant_id_ == other.get_tenant_id()) && (trans_id_ == other.get_tx_id());
}

uint64_t TenantTransID::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = common::murmurhash(&trans_id_, sizeof(trans_id_), hash_val);
  return hash_val;
}

bool TenantTransID::is_valid() const
{
  return tenant_id_ != common::OB_INVALID_TENANT_ID && trans_id_ > 0;
}

PartTransID::PartTransID(const logservice::TenantLSID &tls_id, const transaction::ObTransID &trans_id) :
    tls_id_(tls_id), trans_id_(trans_id)
{}

PartTransID::~PartTransID()
{
  reset();
}

void PartTransID::reset()
{
  tls_id_.reset();
  trans_id_ = -1;
}

bool PartTransID::operator==(const PartTransID &part_trans_id) const
{
  return trans_id_ == part_trans_id.trans_id_ && tls_id_ == part_trans_id.get_tls_id();
}

uint64_t PartTransID::hash() const
{
  uint64_t hash_val = 0;
  uint64_t tls_hv = tls_id_.hash();

  hash_val = murmurhash(&hash_val, sizeof(hash_val), tls_hv);
  hash_val = murmurhash(&hash_val, sizeof(hash_val), trans_id_);

  return hash_val;
}

int PartTransID::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

} // end namespace libobcdc
} // end namespace oceanbase
