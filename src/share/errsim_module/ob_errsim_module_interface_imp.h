/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
