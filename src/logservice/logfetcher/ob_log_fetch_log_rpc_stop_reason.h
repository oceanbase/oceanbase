/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OB_LOG_FETCH_LOG_RPC_STOP_REASON_H_
#define OCEANBASE_OB_LOG_FETCH_LOG_RPC_STOP_REASON_H_

namespace oceanbase
{
namespace logfetcher
{


// RPC stop reason
enum class RpcStopReason
{
  INVALID_REASON = -1,
  REACH_MAX_LOG = 0,        // Reach maximum log
  REACH_UPPER_LIMIT = 1,    // Reach progress limit
  FETCH_NO_LOG = 2,         // Fetched 0 logs
  FETCH_LOG_FAIL = 3,       // Fetch log failure
  REACH_MAX_RPC_RESULT = 4, // The number of RPC results reaches the upper limit
  FORCE_STOP_RPC = 5,       // Exnernal forced stop of RPC
  RESULT_NOT_READABLE = 6,
  RPC_PROTO_NOT_MATCH = 7,
};
const char *print_rpc_stop_reason(const RpcStopReason reason);


}
}

#endif