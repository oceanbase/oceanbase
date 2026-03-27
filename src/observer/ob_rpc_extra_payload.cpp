/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "ob_rpc_extra_payload.h"
#include "share/ob_debug_sync.h"

namespace oceanbase
{
namespace observer
{

int64_t ObRpcExtraPayload::get_serialize_size() const
{
  return GDS.rpc_spread_actions().get_serialize_size();
}

int ObRpcExtraPayload::serialize(SERIAL_PARAMS) const
{
  return GDS.rpc_spread_actions().serialize(buf, buf_len, pos);
}

int ObRpcExtraPayload::deserialize(DESERIAL_PARAMS)
{
  return GDS.rpc_spread_actions().deserialize(buf, data_len, pos);
}

} // end namespace server
} // end namespace oceanbase
