/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_SRV_MEM_INFO_H_
#define OCEANBASE_OBSERVER_OB_SRV_MEM_INFO_H_

#include "share/ob_rpc_struct.h"

int get_tenant_memory_info(const oceanbase::obrpc::ObGetTenantMemoryInfoArg &arg, oceanbase::obrpc::ObGetTenantMemoryInfoResult &result);
int get_tenant_memstore_info(const uint64_t tenant_id, oceanbase::share::TenantMemstoreInfo &memstore_info);
int get_tenant_vector_mem_info(const uint64_t tenant_id, oceanbase::share::TenantVectorMemInfo &vector_mem_info);

#endif // OCEANBASE_OBSERVER_OB_SRV_MEM_INFO_H_
