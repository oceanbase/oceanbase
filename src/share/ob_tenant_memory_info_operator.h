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

#include "lib/container/ob_iarray.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{
namespace obrpc
{
class ObSrvRpcProxy;
}
namespace share
{
class ObTenantMemoryInfoOperator
{
public:
  struct TenantMenstoreInfo {
    OB_UNIS_VERSION(1);
  public:
    TenantMenstoreInfo() :
      total_memstore_used_(0),
      memstore_limit_(0) {}
    TO_STRING_KV(K_(total_memstore_used),
                 K_(memstore_limit));
    int64_t total_memstore_used_;
    int64_t memstore_limit_;
  };
  struct TenantVectorMemInfo {
    OB_UNIS_VERSION(1);
  public:
    TenantVectorMemInfo() :
      raw_malloc_size_(0),
      index_metadata_size_(0),
      vector_mem_hold_(0),
      vector_mem_used_(0),
      vector_mem_limit_(0) {}
    TO_STRING_KV(K_(raw_malloc_size),
                 K_(index_metadata_size),
                 K_(vector_mem_hold),
                 K_(vector_mem_used),
                 K_(vector_mem_limit));
    int64_t raw_malloc_size_;
    int64_t index_metadata_size_;
    int64_t vector_mem_hold_;
    int64_t vector_mem_used_;
    int64_t vector_mem_limit_;
  };
  struct TenantServerMemoryInfo {
    TenantServerMemoryInfo()
      : tenant_id_(OB_INVALID_TENANT_ID), server_(),
        menstore_info_(), vector_mem_info_() {}
    TO_STRING_KV(K_(tenant_id), K_(server), K_(menstore_info), K_(vector_mem_info));
    uint64_t tenant_id_;
    ObAddr server_;
    TenantMenstoreInfo menstore_info_;
    TenantVectorMemInfo vector_mem_info_;
  };

  ObTenantMemoryInfoOperator(obrpc::ObSrvRpcProxy &rpc_proxy, const uint64_t tenant_id)
    : rpc_proxy_(rpc_proxy), tenant_id_(tenant_id), is_oracle_mode_(false) {}
  int init();
  int get(const ObIArray<ObAddr> &servers,
          ObIArray<TenantServerMemoryInfo> &mem_infos);
  bool is_oracle_mode() const { return is_oracle_mode_; }

private:
obrpc::ObSrvRpcProxy &rpc_proxy_;
  uint64_t tenant_id_;
  bool is_oracle_mode_;
};
}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_TENANT_MEMSTORE_INFO_OPERATOR_H_
