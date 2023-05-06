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
 * definition of PartTransID(identify a PartTrans) and TenantTransID
 */

#ifndef  OCEANBASE_LIBOBCDC_DEFINE_H_
#define  OCEANBASE_LIBOBCDC_DEFINE_H_

#include "logservice/common_util/ob_log_ls_define.h"
#include "storage/tx/ob_tx_log.h"             // ObTransID

namespace oceanbase
{
namespace libobcdc
{
// TransID with Tenant Info(ObTransID is Unique in a Tenant but may not unique amone different tenants)
struct TenantTransID
{
  uint64_t                tenant_id_;
  transaction::ObTransID  trans_id_;    // trans_id(int64_t)

  TenantTransID(const uint64_t tenant_id, const transaction::ObTransID tx_id);
  TenantTransID();
  ~TenantTransID();

  void reset();
  int compare(const TenantTransID &other) const;
  bool operator==(const TenantTransID &other) const;
  uint64_t hash() const;

  bool is_valid() const;
  uint64_t get_tenant_id() const { return tenant_id_; }
  const transaction::ObTransID &get_tx_id() const { return trans_id_; }

  TO_STRING_KV(K_(tenant_id), K_(trans_id));
};

// TenentLSID + ObTransID: Uniquely identifies a LS-Trans
struct PartTransID
{
  // TenentLSID
  logservice::TenantLSID       tls_id_;
  // transaction::ObTransID(int64_t)
  transaction::ObTransID       trans_id_;

  PartTransID(const logservice::TenantLSID &tls_id, const transaction::ObTransID &trans_id);
  ~PartTransID();
  bool operator==(const PartTransID &part_trans_id) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  void reset();

  bool is_valid() const { return tls_id_.is_valid() && trans_id_ > 0; }
  bool is_sys_ls() const { return tls_id_.is_sys_log_stream(); }
  const logservice::TenantLSID &get_tls_id() const { return tls_id_; };
  uint64_t get_tenant_id() const { return tls_id_.get_tenant_id(); };
  const share::ObLSID &get_ls_id() const { return tls_id_.get_ls_id(); };
  const transaction::ObTransID &get_tx_id() const { return trans_id_; };

  TO_STRING_KV(K_(tls_id), K_(trans_id));
};

} // end namespace libobcdc
} // end namespace oceanbase

#endif
