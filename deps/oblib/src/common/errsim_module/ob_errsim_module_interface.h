/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_ERRSIM_MODULE_OB_ERRSIM_MODULE_INTERFACE_H_
#define OCEANBASE_COMMON_ERRSIM_MODULE_OB_ERRSIM_MODULE_INTERFACE_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "ob_errsim_module_type.h"
#include "lib/container/ob_array.h"
#include "ob_tenant_errsim_event.h"

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

#endif // OCEANBASE_COMMON_ERRSIM_MODULE_OB_ERRSIM_MODULE_INTERFACE_H_
