/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OMT_OB_TENANT_ERRSIM_H_
#define OCEANBASE_OBSERVER_OMT_OB_TENANT_ERRSIM_H_

#include <stdint.h>

namespace oceanbase
{
namespace omt
{
// When EN_TENANT_RAND_PUSH_GROUP is non-zero and effective_tenant_id == -EN_TENANT_RAND_PUSH_GROUP
// (same condition as ObTenant::recv_request random push group), SQL layer should not force
// OB_NEED_SWITCH_CONSUMER_GROUP retries (worker group may intentionally differ from mapping).
bool ob_tenant_errsim_rand_push_group_active_for_tenant(const uint64_t effective_tenant_id);
} // namespace omt
} // namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OMT_OB_TENANT_ERRSIM_H_ */
