/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_OB_CDC_DEFINE_
#define OCEANBASE_LOGSERVICE_OB_CDC_DEFINE_

#include <cstdint>
namespace oceanbase
{
namespace cdc
{
typedef int64_t ObLogRpcIDType;
static const int64_t OB_LOG_INVALID_RPC_ID = 0;

// Critical value of delay time
// Delay greater than or equal to this value is considered backward
static const int64_t LS_FALL_BEHIND_THRESHOLD_TIME = 3 * 1000 * 1000;

static const int64_t FETCH_LOG_WARN_THRESHOLD = 1 * 1000 * 1000; // 1 second

// a small engouh timestamp, used to avoid misuse time_interval and timestamp
static const int64_t BASE_DEADLINE = 1000000000000000; // 2001-09-09
} // namespace cdc
} // namespace oceanbase

#endif
