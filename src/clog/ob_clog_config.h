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

#ifndef OCEANBASE_CLOG_OB_CLOG_CONFIG_H_
#define OCEANBASE_CLOG_OB_CLOG_CONFIG_H_

#include <stdint.h>
#include "share/ob_define.h"

namespace oceanbase {
namespace clog {
const int64_t CLOG_FILE_SIZE = 1 << 26;  // 64M
// ==== perf monitor ====
// whether open clog operation performance monitor, default value is false
// if set true, it will monitor time consumption of following operations:
// 1) batch_buffer module,waiting available buffer block.
// 2) batch_buffer module,copying data to batch_buffer.
// 3) batch_buffer module,submiting task to handler(ObLogWriter/ObRpcPostHandler).
// 4) direct_reader module,reading data directly from file.
// 5) direct_reader module,reading data from log_cache or file.
// 6) log_writer module,submiting flush task.
const bool ENABLE_CLOG_PERF = true;
// WARN time threshold for clog operation(default 10ms), need set ENABLE_CLOG_PERF true.
const int64_t CLOG_PERF_WARN_THRESHOLD = 10000;
// whether open single clog trace_profile monitor, default false
const bool ENABLE_CLOG_TRACE_PROFILE = false;
// WARN time threshold for clog trace_profile(defalut 10ms), need set ENABLE_CLOG_TRACE_PROFILE true.
const int64_t CLOG_TRACE_PROFILE_WARN_THRESHOLD = 10000;

// ==== IO ====
const bool ENABLE_CLOG_CACHE = true;
const bool ENABLE_ILOG_CACHE = true;
const int64_t DEFAULT_READ_TIMEOUT = 1 * 1000 * 1000;  // 1s
// WARN time threshold for calling transaction on_success
const int64_t TRANSACTION_ON_SUCC_CB_TIME_THRESHOLD = 20 * 1000;

// ==== thread pool ====
// thread count of ObFetchLogEngine
const int64_t CLOG_FETCH_LOG_THREAD_COUNT = 8;
const int64_t MINI_MODE_CLOG_FETCH_LOG_THREAD_COUNT = 1;
const int64_t CLOG_FETCH_LOG_TASK_QUEUE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE * 32;
// thread count for flush cb
const int64_t CLOG_CB_THREAD_COUNT = 32;
const int64_t CLOG_CB_TASK_QUEUE_SIZE = (1LL << 20);
// thread count for member change cb
const int64_t CLOG_SP_CB_THREAD_COUNT = 4;
const int64_t CLOG_SP_CB_TASK_QUEUE_SIZE = (1LL << 20);
STATIC_ASSERT(CLOG_FETCH_LOG_TASK_QUEUE_SIZE >= 6000, "the limit num is too small");

// ==== clog file collect ====
// clog file collect interval, default 1s
const int64_t CLOG_FILE_COLLECT_CHECK_INTERVAL = 1l * 1000l * 1000l;  // 1s
// the num of days the observer promises to keep clog.
// Warning if clog is collected within this interval.
const int64_t CLOG_RESERVED_TIME_FOR_LIBOBLOG_SEC = 3 * 24 * 3600;

// ==== swtch leader ====
// max num of clog the dst server may lag behind when allowing switch leader, default 5000
const uint64_t CLOG_SWITCH_LEADER_ALLOW_THRESHOLD = 5000;

// ==== loop interval ====
// state change check interval
const int32_t CLOG_STATE_DRIVER_INTERVAL = 50 * 1000;
const int32_t CLOG_LOG_DELAY_STATICS_INTERVAL = 3 * 1000 * 1000;
const int32_t CLOG_CHECK_REPLICA_STATE_INTERVAL = 2 * 1000 * 1000;
const int64_t CLOG_CHECK_PARENT_INTERVAL = 3 * 1000 * 1000;
const int32_t CLOG_PARENT_INVALID_ALERT_TIME = 5 * 60 * 1000 * 1000;
const int64_t CLOG_BROADCAST_INTERVAL = 10 * 60 * 1000 * 1000;            // 10min
const int64_t CLOG_BROADCAST_MAX_INTERVAL = 3 * CLOG_BROADCAST_INTERVAL;  // 30min
const int32_t CLOG_NET_LOG_BUF_MGR_RUN_INTERVAL = 10 * 1000;
const int64_t CLOG_CHECK_RELEASE_AGGRE_BUFFER_INTERVAL = 1 * 1000 * 1000;
// replay driver interval
const int32_t CLOG_REPLAY_DRIVER_LOWER_INTERVAL = 500;
const int32_t CLOG_REPLAY_DRIVER_UPPER_INTERVAL = 500 * 1000;
// time threshold the ReplayDriver exec try_replay,default 500ms
const int32_t CLOG_REPLAY_DRIVER_RUN_THRESHOLD = 500 * 1000;
const int32_t CLOG_MAX_REPLAY_TIMEOUT = 500 * 1000;
// logonly replica checkpoint interval
const int32_t CLOG_CHECKPOINT_LOG_REPLICA_INTERVAL = 30 * 60 * 1000 * 1000;  // 30min
// time interval to check if need revoke when own clog disk is full and the num of disk full member is not reach
// majority.
const int32_t CLOG_CHECK_CLOG_DISK_FULL_INTERVAL = 10 * 1000 * 1000;  // 10s

// ==== fetch log ====
// time interval the follower try fetch log from leader/parent when local max_confirmed_log_id is not changed during
// this time.
const int64_t CLOG_FETCH_LOG_INTERVAL_LOWER_BOUND = 4 * 1000 * 1000;
const int64_t CLOG_FETCH_LOG_INTERVAL_UPPER_BOUND = 16 * CLOG_FETCH_LOG_INTERVAL_LOWER_BOUND;
const int64_t CLOG_FAKE_PUSH_LOG_INTERVAL = 4 * 1000 * 1000;
const int64_t CLOG_LEADER_KEEPALIVE_INTERVAL = 98 * 1000;
const int64_t CLOG_CHECK_STATE_INTERVAL = 10 * 1000;
const uint64_t MAX_DELIVER_CNT = 10;
// max time a synced replica is allowed to lag behind current time.
const int64_t MAX_SYNC_FALLBACK_INTERVAL = 20 * 1000 * 1000;
// ==== replica cascading ====
// max transport delay time for a cascading msg,
// calculated by max single trip rpc time(200ms) + queue time in both server(2*100ms) + max clock diff time in both
// server(2*100ms)
const int64_t CASCAD_MSG_DELAY_TIME_THRESHOLD = 200 * 1000 + 4 * 100 * 1000;
// time inteval for assigning new parent when current parent is invalid
const int64_t CLOG_CASCADING_PARENT_INVALID_DETECT_THRESHOLD = 500 * 1000;
// time interval for checking child state
const int64_t CLOG_CASCADING_CHECK_CHILD_INTERVAL = 1 * 60 * 1000 * 1000;
const int64_t CLOG_FETCH_LOG_TASK_QUEUE_TIMEOUT = 3 * 1000 * 1000;
const int64_t CLOG_FETCH_LOG_NETWORK_THRESHOLD = 120 * 1024 * 1024;  // 120M
// time used for smoothly switching parent
const int64_t REREGISTER_PERIOD = 10 * 1000 * 1000;

// ==== timeout ====
// clog sync timeout in leader active state, default 10s
// if one clog reaches timeout, leader may revoke.
const int64_t CLOG_LEADER_ACTIVE_SYNC_TIMEOUT = 10 * 1000 * 1000;
// valid time for fake ack msg from follower
const int64_t FAKE_ACK_MSG_VALID_TIME = CLOG_LEADER_ACTIVE_SYNC_TIMEOUT;
// clog sync timeout in leader reconfirm state, default 10s
// if one clog reaches timeout, leader will revoke.
const int64_t CLOG_LEADER_RECONFIRM_SYNC_TIMEOUT = 10 * 1000 * 1000;
// time interval for requesting max_log_id during leader reconfirming.
const int64_t CLOG_RECONFIRM_FETCH_MAX_LOG_ID_INTERVAL = 2 * 1000 * 1000;
const int64_t CLOG_AIO_WRITE_TIMEOUT = 30 * 1000 * 1000;  // 30s
const int64_t RECONFIRM_LOG_ARRAY_LENGTH = 128;           // log array size for reconfirm

const uint64_t DEFAULT_LEADER_MAX_UNCONFIRMED_LOG_COUNT = 500;
// max num of logs for single fetch log request
const uint64_t CLOG_ST_FETCH_LOG_COUNT = 250;
// refresh interval for sliding window's cached tenant config clog_max_unconfirmed_log_count
const int64_t TENANT_CONFIG_REFRESH_INTERVAL = 10 * 1000 * 1000;
const int64_t CLOG_APPEND_MAX_UNCONFIRMED_THRESHOLD = 10000;
// renew interval for location cache in clog module
const int64_t RENEW_LOCATION_TIME_INTERVAL = 5 * 1000 * 1000;
// time interval that primary cluster renew standby's location
const int64_t PRIMARY_RENEW_LOCATION_TIME_INTERVAL = 2 * 1000 * 1000;
// When server restarts, if the file_id corresponding to the current appending log and the file_id corresponding to the
// log that has just popped out are greater than the following diff (default 200), server will try to pop logs
// immediately (submit the log to replay_engine). In the case of a large number of partitions and small machine memory,
// the max default value may cause LogTask memory to burst, so we set the default value to 10.
const int64_t CLOG_APPEND_MAX_FILE_DIFF = 10;

// ==== group commit ====
const int64_t CLOG_DISK_BUFFER_COUNT = 64;
const int64_t CLOG_DISK_BUFFER_SIZE = common::OB_MAX_LOG_BUFFER_SIZE;

// ==== IlogStorage ====
// max file count the ilog_cache can cache
const int64_t CURSOR_CACHE_HOLD_FILE_COUNT_LIMIT = 100;

// === LogService ===
// max active stream count
const uint64_t ACTIVE_STREAM_COUNT_LIMIT = 10000;
#define DEBUG_CHECK_READER false

// #define USE_HISTOGRAM true

// max num of partitions the batch submit can support
const int64_t MAX_BATCH_SUBMIT_CNT = 512;

// the num of threads used for scanning clog files during starting
const int64_t SCAN_THREAD_CNT = 4;
const int64_t MINI_MODE_SCAN_THREAD_CNT = 1;
// the num of threads used for locating file range by log_id
const int64_t FILE_RANGE_THREAD_CNT = 16;
const int64_t MINI_MODE_FILE_RANGE_THREAD_CNT = 2;

// clog hot_cache
const int64_t HOT_CACHE_LOWER_HARD_LIMIT = 1L << 28;  // hot_cache memory's lower bound
const int64_t HOT_CACHE_UPPER_HARD_LIMIT = 1L << 30;  // hot_cache memory's upper bound
const int64_t HOT_CACHE_MEM_PERCENT = 5;              // the percentage of server tenant's memory that hot_cache can use

// aggregate commit
const int64_t AGGRE_BUFFER_SIZE = 64 * 1024;
const int64_t AGGRE_BUFFER_FLAG = AGGRE_BUFFER_SIZE - 1;
const int64_t AGGRE_BUFFER_LIMIT = AGGRE_BUFFER_SIZE - 1024;
const int64_t AGGRE_LOG_RESERVED_SIZE = 12;  // 8 for submit_timestamp, 4 for next_log offset
const int64_t AGGRE_BUFFER_FLUSH_INTERVAL_US = 1 * 1000;

// max wait time for submitting log to log_writer queue
const uint64_t DEFAULT_CLOG_APPEND_TIMEOUT_US = 365ull * 24 * 3600 * 1000 * 1000;
// the length of log_writer queue, greater than (_ob_clog_disk_buffer_cnt + 1)
const uint64_t DEFAULT_WRITER_MAX_BUFFER_ITEM_CNT = 4 * 1024;
// the buffer size of membership log
const int64_t MS_LOG_BUFFER_SIZE = 2048;
// max wait time for flush log to stable storage
const uint64_t DEFAULT_CLOG_FLUSH_TIMEOUT_US = 15 * 1000 * 1000;
}  // namespace clog
}  // namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_CLOG_CONFIG_H_
