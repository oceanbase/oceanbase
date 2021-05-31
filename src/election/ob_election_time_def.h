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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_TIME_DEF_
#define OCEANBASE_ELECTION_OB_ELECTION_TIME_DEF_

#include <stdint.h>

namespace oceanbase {
namespace election {
#define T_DIFF 100000
#define T_ST 200000
#define T_ELECT_DIFF_ROUNDS 6
#define T_ELECT_ST_ROUNDS 3
#define T_ELECT_BUFFER 200000
#define T_SLAVE_LEASE_MORE 1000000
#define T_LEADER_LEASE_EXTENDS 4
#define T_LEADER_LEASE_EXTENDS_V2 7
#define T_CYCLE_EXTEND_MORE 1
#define T_CENTRALIZED_VOTE_EXTENDS 2
#define T_CENTRALIZED_VOTE_EXTENDS_V2 4
#define T_CENTRALIZED_VOTE_PERIOD_EG 1  // election group renew lease period
#define T_LEADER_LEASE_MORE 400000

#define T_ELECT (T_DIFF * T_ELECT_DIFF_ROUNDS + T_ST * T_ELECT_ST_ROUNDS)         // 1200
#define T_ELECT2 (T_ELECT + T_ELECT_BUFFER)                                       // 1400
#define T_LEADER_LEASE (T_LEADER_LEASE_EXTENDS * T_ELECT2 + T_LEADER_LEASE_MORE)  // 4 * 1400 + 400
#define T_SLAVE_LEASE (T_LEADER_LEASE_EXTENDS * T_ELECT2 + T_SLAVE_LEASE_MORE)    // 5600 + 1000
#define T_CYCLE_EXTENDS (T_LEADER_LEASE_EXTENDS + T_CYCLE_EXTEND_MORE)            // 5
#define T_CYCLE_EXTENDS_V2 (T_LEADER_LEASE_EXTENDS_V2 + T_CYCLE_EXTEND_MORE)      // 8
#define T_CYCLE_V1 (T_CYCLE_EXTENDS * T_ELECT2)                                   // 5 * 1400 = 7S
#define T_CYCLE_V2 (T_CYCLE_EXTENDS_V2 * T_ELECT2)                                // 8 * 1400 = 11.2S

#if T_SLAVE_LEASE_MORE <= T_LEADER_LEASE_MORE
#error "T_SLAVE_LEASE <= T_LEADER_LEASE"
#endif

#if T_CYCLE_V1 <= T_SLAVE_LEASE
#error "T_CYCLE_V1 <= T_SLAVE_LEASE"
#endif
#if (T_CYCLE_EXTENDS_V2 * T_ELECT2) <= (T_LEADER_LEASE_EXTENDS_V2 * T_ELECT2 + T_SLAVE_LEASE_MORE)
#error "T_CYCLE_V2 <= T_SLAVE_LEASE_V2"
#endif

#if T_SLAVE_LEASE_MORE > T_ELECT2
#error "T_SLAVE_LEASE_MORE > T_ELECT2"
#endif

#if T_CENTRALIZED_VOTE_EXTENDS >= T_LEADER_LEASE_EXTENDS
#error "T_CENTRALIZED_VOTE_EXTENDS >= T_LEADER_LEASE_EXTENDS"
#endif
#if T_CENTRALIZED_VOTE_EXTENDS_V2 >= T_LEADER_LEASE_EXTENDS_V2
#error "T_CENTRALIZED_VOTE_EXTENDS_V2 >= T_LEADER_LEASE_EXTENDS_V2"
#endif

#define T1_TIMESTAMP(t1) ((t1) + T_DIFF * 0 + T_ST * 0)
#define T2_TIMESTAMP(t1) ((t1) + T_DIFF * 2 + T_ST * 1)
#define T3_TIMESTAMP(t1) ((t1) + T_DIFF * 4 + T_ST * 2)
#define T4_TIMESTAMP(t1) ((t1) + T_DIFF * 6 + T_ST * 3)

#define IN_T0_RANGE(ts, t1) (((ts) >= (t1 - T_DIFF * 2)) && ((ts) <= ((t1) + T_DIFF * 6 + T_ST * 3 + T_ELECT_BUFFER)))
#define IN_T1_RANGE(ts, t1) (((ts) >= T1_TIMESTAMP(t1) - T_DIFF * 2) && ((ts) <= T1_TIMESTAMP(t1) + T_DIFF * 2 + T_ST))
#define IN_T2_RANGE(ts, t1) (((ts) >= T2_TIMESTAMP(t1) - T_DIFF * 2) && ((ts) <= T2_TIMESTAMP(t1) + T_DIFF * 2 + T_ST))
#define IN_T3_RANGE(ts, t1) (((ts) >= T3_TIMESTAMP(t1) - T_DIFF * 2) && ((ts) <= T3_TIMESTAMP(t1) + T_DIFF * 2 + T_ST))
#define IN_T4_RANGE(ts, t1) (((ts) >= T4_TIMESTAMP(t1) - T_DIFF * 2) && ((ts) <= T4_TIMESTAMP(t1) + T_DIFF * 2 + T_ST))

// timer task run time diff
//#define T_TIMER_DIFF T_DIFF
#define T_DEVOTE_TIMER_DIFF (2 * (T_DIFF))
#define T_VOTE_TIMER_DIFF (6 * (T_DIFF) + 3 * T_ST)
#define T_CHANGE_LEADER_DIFF (T_ST * 5)  // change leader task delay no more than 1s
#define T_CHANGE_LEADER_TIMER_GAP 5000

// const variable
static const int32_t OB_ELECTION_120_LEASE_TIME = (T_LEADER_LEASE_EXTENDS * T_ELECT2);
static const int32_t OB_ELECTION_130_LEASE_TIME = (T_LEADER_LEASE_EXTENDS_V2 * T_ELECT2);
static const int64_t OB_ELECTION_HASH_TABLE_NUM = 100;  // mod 100
static const int64_t OB_ELECTION_HASH_TIME_US = 10000;  // 10ms
static const int64_t EXPIRED_TIME_FOR_WARNING = 2 * T_CYCLE_V1;
// reserve 100ms to avoid timer start failed
const int64_t RESERVED_TIME_US = 100 * 1000;
}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_TIME_DEF_
