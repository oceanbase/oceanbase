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

#ifndef OCEANBASE_SHARE_OB_TENANT_MEMSTORE_INFO_OPERATOR_H_
#define OCEANBASE_SHARE_OB_TENANT_MEMSTORE_INFO_OPERATOR_H_

#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class ObResourcePool;
class ObTenantMemstoreInfoOperator
{
public:
  struct TenantServerMemInfo {
    TenantServerMemInfo()
      : tenant_id_(common::OB_INVALID_ID), server_(), active_memstore_used_(0),
        total_memstore_used_(0), major_freeze_trigger_(0), memstore_limit_(0) {}

    TO_STRING_KV(K_(tenant_id), K_(server), K_(active_memstore_used),
        K_(total_memstore_used), K_(major_freeze_trigger), K_(memstore_limit));

    bool is_valid() const;
    void reset() { *this = TenantServerMemInfo(); }

    uint64_t tenant_id_;
    common::ObAddr server_;
    int64_t active_memstore_used_;
    int64_t total_memstore_used_;
    int64_t major_freeze_trigger_;
    int64_t memstore_limit_;
  };

  ObTenantMemstoreInfoOperator(common::ObMySQLProxy &proxy) : proxy_(proxy) {}

  int get(const uint64_t tenant_id,
          const common::ObIArray<common::ObAddr> &servers,
          common::ObIArray<TenantServerMemInfo> &mem_infos);
private:
  common::ObMySQLProxy &proxy_;
};
}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_TENANT_MEMSTORE_INFO_OPERATOR_H_
