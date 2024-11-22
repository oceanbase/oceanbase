/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_DIAGNOSTIC_INFO_UTIL_H_
#define OB_DIAGNOSTIC_INFO_UTIL_H_

#include "lib/stat/ob_diagnostic_info_container.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_di_cache.h"

namespace oceanbase
{
namespace common
{

extern void lib_release_tenant(void *ptr);
extern int64_t get_mtl_id();
extern ObDiagnosticInfoContainer *get_di_container();
extern uint64_t lib_get_cpu_khz();
extern void lib_mtl_switch(int64_t tenant_id, std::function<void(int)> fn);
extern int64_t lib_mtl_cpu_count();

#define LIB_MTL_ID() get_mtl_id()

#define MTL_DI_CONTAINER() get_di_container()

}  // namespace common
}  // namespace oceanbase

#endif /* OB_DIAGNOSTIC_INFO_UTIL_H_ */