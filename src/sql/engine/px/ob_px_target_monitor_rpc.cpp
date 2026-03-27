/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_px_target_monitor_rpc.h"

namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER(ObPxRpcAddrTarget, addr_, target_);
OB_SERIALIZE_MEMBER(ObPxRpcFetchStatArgs, tenant_id_, follower_version_, addr_target_array_, need_refresh_all_, addr_);
OB_SERIALIZE_MEMBER(ObPxRpcFetchStatResponse, status_, tenant_id_, leader_version_, addr_target_array_);

}
}
