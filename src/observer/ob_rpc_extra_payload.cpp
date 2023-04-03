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
