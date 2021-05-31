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

#ifndef _ALLOC_FUNC_H_
#define _ALLOC_FUNC_H_

#include <stdint.h>

namespace oceanbase {
namespace common {
struct ObModItem;
}  // end of namespace common

namespace lib {
// statistic relating
void set_memory_limit(int64_t bytes);
int64_t get_memory_limit();
int64_t get_memory_hold();
int64_t get_memory_used();
int64_t get_memory_avail();

void set_tenant_memory_limit(uint64_t tenant_id, int64_t bytes);
int64_t get_tenant_memory_limit(uint64_t tenant_id);
int64_t get_tenant_memory_hold(uint64_t tenant_id);
int64_t get_tenant_memory_hold(const uint64_t tenant_id, const uint64_t ctx_id);
void get_tenant_mod_memory(uint64_t tenant_id, int mod_id, common::ObModItem& ObModItem);
void ob_set_reserved_memory(const int64_t bytes);
void ob_set_urgent_memory(const int64_t bytes);
int64_t ob_get_reserved_urgent_memory();

// Set Work Area memory limit for specified tenant.
// ms_pctg: percentage limitation of tenant memory can be used by MemStore
// pc_pctg: percentage limitation of tenant memory can be used by Plan Cache
// wa_pctg: percentage limitation of tenant memory can be used by Work Area
int set_wa_limit(uint64_t tenand_id, int64_t ms_pctg, int64_t pc_pctg, int64_t wa_pctg);
int set_wa_limit(uint64_t tenand_id, int64_t wa_pctg);

}  // end of namespace lib
}  // end of namespace oceanbase

#endif /* _ALLOC_FUNC_H_ */
