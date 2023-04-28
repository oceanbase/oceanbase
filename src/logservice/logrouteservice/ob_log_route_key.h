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

#ifndef OCEANBASE_LOG_ROUTE_KEY_H_
#define OCEANBASE_LOG_ROUTE_KEY_H_

#include "share/ob_ls_id.h"       // ObLSID

namespace oceanbase
{
namespace logservice
{
class ObLSRouterKey
{
public:
  ObLSRouterKey();
  ObLSRouterKey(
      const int64_t cluster_id,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id);
  virtual ~ObLSRouterKey() {}
  void reset();
  virtual bool operator ==(const ObLSRouterKey &other) const;
  virtual bool operator !=(const ObLSRouterKey &other) const;
  virtual bool is_valid() const;
  virtual uint64_t hash() const;
  virtual int hash(uint64_t &hash_val) const;
  virtual int64_t size() const { return sizeof(*this); }
  virtual int deep_copy(char *buf, const int64_t buf_len, ObLSRouterKey *&key) const;
  inline int64_t get_cluster_id() const { return cluster_id_; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline share::ObLSID get_ls_id() const { return ls_id_; }

  TO_STRING_KV(K_(cluster_id),
      K_(tenant_id),
      K_(ls_id));
private:
  int64_t cluster_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

} // namespace logservice
} // namespace oceanbase

#endif

