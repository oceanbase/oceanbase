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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_PARAMETERS_
#define OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_PARAMETERS_
#include <stdint.h>

namespace oceanbase
{
namespace share
{
namespace detector
{
// UserBinaryKey inner buffer size limit
constexpr const int64_t BUFFER_LIMIT_SIZE = 128;
// common used within deadlock module
// special value indicate invalid value within deadlock module
constexpr const int64_t INVALID_VALUE = INT64_MAX;
// used within ObDeadLockDetectorMgr
// timeWheel polling granularity, set to 10ms
constexpr const int64_t TIME_WHEEL_PRECISION_US = 10 * 1000;
constexpr const int TIMER_THREAD_COUNT = 1;// timeWheel thread number
constexpr const char *DETECTOR_TIMER_NAME = "DetectorTimer";// timeWheel thread name
// used within ObDeadLockRpc
constexpr const int64_t OB_DETECTOR_RPC_TIMEOUT = 5 * 1000 * 1000;// timeout time for rpc, set to 5s
// used within ObDetectUserReportInfo
// 3 extra columns in inner table that user could describe more things
constexpr const int64_t EXTRA_INFO_COLUMNS = 3;
// used within ObDetectUserReportInfo
// the length of string showed in inner table should less than 65536
constexpr const int64_t STR_LEN_LIMIT = 65536;
// used within ObDeadLockDetector
// at lest 1s between two collect info process in one detector
constexpr const int64_t COLLECT_INFO_INTERVAL = 1000 * 1000;
// used within ObDeadLockInnerTableServic::ObDeadLockEventHistoryTableOperator::async_delete
// remain last 7 days event records
constexpr const int64_t REMAIN_RECORD_DURATION = 7L * 24L * 60L * 60L * 1000L * 1000L;
// used when report deadlock inner info
struct ROLE
{
  // the detector who find there is a deadlock exist, starting collect info phase
  static constexpr const char *EXECUTOR = "executor";
  // the detectors who tansfer message between executor and victim
  static constexpr const char *WITNESS = "witness";
  static constexpr const char *VICTIM = "victim";// the detector who finally killed
  static constexpr const char *SUICIDE = "executor-victim";// the executor who suicide
};
// detector priority range, lower priority range means higher possiblity to be killed
enum class PRIORITY_RANGE
{
  EXTREMELY_LOW = -2,
  LOW = -1,
  NORMAL = 0,
  HIGH = 1,
  EXTREMELY_HIGH = 2
};
}// namespace detector
}// namespace share
}// namespace oceanbase
#endif