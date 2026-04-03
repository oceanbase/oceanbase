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

#ifndef OCEANBASE_OBSERVER_OB_SRV_MEM_INFO_H_
#define OCEANBASE_OBSERVER_OB_SRV_MEM_INFO_H_

#include "share/ob_rpc_struct.h"

int get_tenant_memory_info(const oceanbase::obrpc::ObGetTenantMemoryInfoArg &arg, oceanbase::obrpc::ObGetTenantMemoryInfoResult &result);
int get_tenant_memstore_info(const uint64_t tenant_id, oceanbase::share::TenantMemstoreInfo &memstore_info);
int get_tenant_vector_mem_info(const uint64_t tenant_id, oceanbase::share::TenantVectorMemInfo &vector_mem_info);

#endif // OCEANBASE_OBSERVER_OB_SRV_MEM_INFO_H_
