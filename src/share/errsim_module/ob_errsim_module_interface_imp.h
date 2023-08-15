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

#ifndef SRC_SHARE_ERRSIM_MODULE_OB_TENANT_ERRSIM_MODULE_INTERFACE_IMP_H_
#define SRC_SHARE_ERRSIM_MODULE_OB_TENANT_ERRSIM_MODULE_INTERFACE_IMP_H_

#include "ob_tenant_errsim_module_mgr.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "common/errsim_module/ob_tenant_errsim_event.h"


namespace oceanbase
{
namespace common
{

int build_tenant_errsim_moulde(
    const uint64_t tenant_id,
    const int64_t config_version,
    const common::ObArray<ObFixedLengthString<ObErrsimModuleTypeHelper::MAX_TYPE_NAME_LENGTH>> &module_array,
    const int64_t percentage);
bool is_errsim_module(
    const uint64_t tenant_id,
    const ObErrsimModuleType::TYPE &type);
int add_tenant_errsim_event(
    const uint64_t tenant_id,
    const ObTenantErrsimEvent &event);


} // namespace common
} // namespace oceanbase
#endif
