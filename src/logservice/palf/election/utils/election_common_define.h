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

#ifndef LOGSERVICE_PALF_ELECTION_UTILS_OB_ELECTION_COMMON_DEFINE_H
#define LOGSERVICE_PALF_ELECTION_UTILS_OB_ELECTION_COMMON_DEFINE_H

// this file should only include in .cpp file,
// or may cause MACRO pollution
#include "lib/oblog/ob_log_module.h"
#include "share/ob_occam_time_guard.h"
#include <algorithm>

/* to do bin.lb: to be removed
#define LOG_PHASE(level, phase, info, args...) \
do {\
  if (phase == LogPhase::NONE) {\
    ELECT_LOG(level, info, ##args, PRINT_WRAPPER);\
  } else {\
    char joined_info[256] = {0};\
    int64_t pos = 0;\
    switch (phase) {\
    case LogPhase::INIT:\
      oceanbase::common::databuff_printf(joined_info, 256, pos, "[INIT]%s", info);\
      break;\
    case LogPhase::DESTROY:\
      oceanbase::common::databuff_printf(joined_info, 256, pos, "[DESTROY]%s", info);\
      break;\
    case LogPhase::ELECT_LEADER:\
      oceanbase::common::databuff_printf(joined_info, 256, pos, "[ELECT_LEADER]%s", info);\
      break;\
    case LogPhase::RENEW_LEASE:\
      oceanbase::common::databuff_printf(joined_info, 256, pos, "[RENEW_LEASE]%s", info);\
      break;\
    case LogPhase::CHANGE_LEADER:\
      oceanbase::common::databuff_printf(joined_info, 256, pos, "[CHANGE_LEADER]%s", info);\
      break;\
    case LogPhase::EVENT:\
      oceanbase::common::databuff_printf(joined_info, 256, pos, "[EVENT]%s", info);\
      break;\
    case LogPhase::SET_MEMBER:\
      oceanbase::common::databuff_printf(joined_info, 256, pos, "[SET_MEMBER]%s", info);\
      break;\
    default:\
      oceanbase::common::databuff_printf(joined_info, 256, pos, "[UNKNOWN]%s", info);\
      break;\
    }\
    ELECT_LOG(level, joined_info, ##args, PRINT_WRAPPER);\
  }\
} while(0)
*/

#define LOG_PHASE(level, phase, info, args...) ELECT_LOG(level, "[" #phase "]" info, ##args, PRINT_WRAPPER)
#define LOG_PHASE_RET(level, errcode, phase, info, args...) ELECT_LOG_RET(level, errcode, "[" #phase "]" info, ##args, PRINT_WRAPPER)

#define LOG_INIT(level, info, args...) LOG_PHASE(level, LogPhase::INIT, info, ##args)
#define LOG_DESTROY(level, info, args...) LOG_PHASE(level, LogPhase::DESTROY, info, ##args)
#define LOG_ELECT_LEADER(level, info, args...) LOG_PHASE(level, LogPhase::ELECT_LEADER, info, ##args)
#define LOG_RENEW_LEASE(level, info, args...) LOG_PHASE(level, LogPhase::RENEW_LEASE, info, ##args)
#define LOG_CHANGE_LEADER(level, info, args...) LOG_PHASE(level, LogPhase::CHANGE_LEADER, info, ##args)
#define LOG_EVENT(level, info, args...) LOG_PHASE(level, LogPhase::EVENT, info, ##args)
#define LOG_SET_MEMBER(level, info, args...) LOG_PHASE(level, LogPhase::SET_MEMBER, info, ##args)
#define LOG_NONE(level, info, args...) ELECT_LOG(level, info, ##args, PRINT_WRAPPER)

#define LOG_INIT_RET(level, errcode, args...) { int ret = errcode; LOG_INIT(level, ##args); }
#define LOG_DESTROY_RET(level, errcode, args...) { int ret = errcode; LOG_DESTROY(level, ##args); }
#define LOG_RENEW_LEASE_RET(level, errcode, args...) { int ret = errcode; LOG_RENEW_LEASE(level, ##args); }
#define LOG_CHANGE_LEADER_RET(level, errcode, args...) { int ret = errcode; LOG_CHANGE_LEADER(level, ##args); }
#define LOG_EVENT_RET(level, errcode, args...) { int ret = errcode; LOG_EVENT(level, ##args); }
#define LOG_SET_MEMBER_RET(level, errcode, args...) { int ret = errcode; LOG_SET_MEMBER(level, ##args); }
#define LOG_NONE_RET(level, errcode, args...) { int ret = errcode; LOG_NONE(level, ##args); }

#define ELECT_TIME_GUARD(func_cost_threshold) TIMEGUARD_INIT(ELECT, func_cost_threshold, 10_s)

namespace oceanbase
{
namespace palf
{
namespace election
{

enum class LogPhase
{
  NONE = 0,
  INIT = 1,
  DESTROY = 2,
  ELECT_LEADER = 3,
  RENEW_LEASE = 4,
  CHANGE_LEADER = 5,
  EVENT = 6,
  SET_MEMBER = 7,
};

// inner priority seed define, bigger means lower priority
enum class PRIORITY_SEED_BIT : uint64_t
{
  DEFAULT_SEED = (1ULL << 12),
  SEED_IN_REBUILD_PHASE_BIT = (1ULL << 32),
  SEED_NOT_NORMOL_REPLICA_BIT = (1ULL << 48),
};

constexpr int64_t MSG_DELAY_WARN_THRESHOLD = 200_ms;
constexpr int64_t MAX_LEASE_TIME = 10_s;
constexpr int64_t PRIORITY_BUFFER_SIZE = 512;
constexpr int64_t INVALID_VALUE = -1;// 所有int64_t变量的初始默认无效值
constexpr int64_t CACHE_EXPIRATION_TIME = 5_s;
extern int64_t MAX_TST; // 最大单程消息延迟，暂设为1s，在单测中会将其调低，日后可改为配置项，现阶段先用全局变量代替
inline int64_t CALCULATE_RENEW_LEASE_INTERVAL() { return  std::min<int64_t>(0.5 * MAX_TST, 500_ms); }// 续约周期固定为消息延迟的一半，最大不超过500ms
inline int64_t CALCULATE_TIME_WINDOW_SPAN_TS() { return  2 * MAX_TST; }// 时间窗口的长度，为两个最大单程消息延迟， 默认为2s
inline int64_t CALCULATE_MAX_ELECT_COST_TIME() { return  10 * MAX_TST; }// 一次选举可能出现的最大耗时设置，默认为10s
inline int64_t CALCULATE_LEASE_INTERVAL() { return 4 * MAX_TST; }// 4个消息延迟，默认是4s
inline int64_t CALCULATE_TRIGGER_ELECT_WATER_MARK() { return std::min<int64_t>(MAX_TST, 1_s); }// 触发无主选举的Lease剩余水位线，1个最大消息延迟，最大不超过1s

}// namespace election
}// namespace palf
}// namesapce oceanbase

#endif
