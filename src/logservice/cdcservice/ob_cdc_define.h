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
