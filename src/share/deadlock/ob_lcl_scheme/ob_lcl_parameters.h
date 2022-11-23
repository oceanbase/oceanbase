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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_LCL_SCHEME_OB_LCL_PARAMETERS_H
#define OCEANBASE_SHARE_DEADLOCK_OB_LCL_SCHEME_OB_LCL_PARAMETERS_H
#include <stdint.h>

namespace oceanbase
{
namespace share
{
namespace detector
{

constexpr const int64_t MAX_CLOCK_DIFF = 50L * 1000L;// 50ms
constexpr const int64_t MAX_MSG_DELAY = 40L * 1000L;// 40ms
constexpr const int64_t SEND_MSG_LIMIT_INTERVAL = 10L * 1000L;// 10ms
constexpr const int64_t MIN_CYCLY_SIZE_SUPPORTED = 10;// 10 node
constexpr const int64_t RANDOM_DELAY_RANGE = 100L * 1000L;// 100ms
constexpr const int64_t TIMER_SCHEDULE_RESERVE_TIME = 1L * 1000L;// 1ms
constexpr const int64_t PHASE_TIME = 2 * MAX_CLOCK_DIFF +
                                     RANDOM_DELAY_RANGE + 
                                     (MAX_MSG_DELAY + SEND_MSG_LIMIT_INTERVAL) * 
                                     MIN_CYCLY_SIZE_SUPPORTED;// 700ms
constexpr const int64_t PERIOD = 2 * PHASE_TIME;// 1.4s
constexpr const int64_t COMMON_BLOCK_SIZE = 3;// assume element number in block list not more than 3
                                              // in most scenarios
}// namespace detector
}// namespace share
}// namespace oceanbase
#endif