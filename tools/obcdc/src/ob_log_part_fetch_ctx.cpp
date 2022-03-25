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

#define USING_LOG_PREFIX  OBLOG_FETCHER

#include "ob_log_part_fetch_ctx.h"

#include "lib/hash_func/murmur_hash.h"        // murmurhash
#include "storage/ob_storage_log_type.h"      // ObStorageLogType

#include "ob_log_utils.h"                     // get_timestamp
#include "ob_log_fetcher_heartbeat_worker.h"  // IObLogFetcherHeartbeatWorker
#include "ob_log_config.h"                    // ObLogConfig
#include "ob_log_part_fetch_mgr.h"            // IObLogPartFetchMgr
#include "ob_log_trace_id.h"                  // ObLogTraceIdGuard

#define STAT(level, fmt, args...) OBLOG_FETCHER_LOG(level, "[STAT] [FETCH_CTX] " fmt, ##args)
#define _STAT(level, fmt, args...) _OBLOG_FETCHER_LOG(level, "[STAT] [FETCH_CTX] " fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define DSTAT(fmt, args...) STAT(DEBUG, fmt, ##args)

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

namespace oceanbase
{
namespace liboblog
{

/////////////////////////////// PartFetchCtx /////////////////////////////////

// Defining global class variables
int64_t PartFetchCtx::g_svr_list_update_interval =
    ObLogConfig::default_svr_list_update_interval_sec * _SEC_;
int64_t PartFetchCtx::g_leader_info_update_interval =
    ObLogConfig::default_leader_info_update_interval_sec * _SEC_;
int64_t PartFetchCtx::g_heartbeat_interval = ObLogConfig::default_heartbeat_interval_sec * _SEC_;
int64_t PartFetchCtx::g_blacklist_history_clear_interval=
    ObLogConfig::default_blacklist_history_clear_interval_min * _MIN_;

PartFetchCtx::PartFetchCtx()
{
  reset();
}

PartFetchCtx::~PartFetchCtx()
{
  reset();
}

void PartFetchCtx::configure(const ObLogConfig &config)
{
  // update global class variables
  int64_t svr_list_update_interval_sec = config.svr_list_update_interval_sec;
  int64_t leader_info_update_interval_sec = config.leader_info_update_interval_sec;
  int64_t heartbeat_interval_sec = config.heartbeat_interval_sec;
  int64_t blacklist_history_clear_interval_min = config.blacklist_history_clear_interval_min;

  ATOMIC_STORE(&g_svr_list_update_interval, svr_list_update_interval_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(svr_list_update_interval_sec));

  ATOMIC_STORE(&g_leader_info_update_interval, leader_info_update_interval_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(leader_info_update_interval_sec));

  ATOMIC_STORE(&g_heartbeat_interval, heartbeat_interval_sec * _SEC_);
  LOG_INFO("[CONFIG]", K(heartbeat_interval_sec));

  ATOMIC_STORE(&g_blacklist_history_clear_interval, blacklist_history_clear_interval_min * _MIN_);
  LOG_INFO("[CONFIG]", K(blacklist_history_clear_interval_min));
}

void PartFetchCtx::reset()
{
  // Note: The default stream type for setting partitions is hot stream
  stype_ = FETCH_STREAM_TYPE_HOT;
  state_ = STATE_NORMAL;
  discarded_ = false;
  pkey_.reset();
  serve_info_.reset();
  progress_id_ = -1;
  part_fetch_mgr_ = NULL;
  part_trans_resolver_ = NULL;
  last_sync_progress_ = OB_INVALID_TIMESTAMP;
  progress_.reset();
  fetch_info_.reset();
  blacklist_.reset();
  svr_list_[0].reset();
  svr_list_[1].reset();
  cur_svr_list_idx_ = 0;
  svr_list_need_update_ = true;
  svr_list_last_update_tstamp_ = OB_INVALID_TIMESTAMP;
  svr_find_req_.reset();
  start_log_id_locate_req_.reset();
  leader_.reset();
  has_leader_ = false;
  leader_last_update_tstamp_ = OB_INVALID_TIMESTAMP;
  leader_find_req_.reset();
  heartbeat_req_.reset();
  heartbeat_last_update_tstamp_ = OB_INVALID_TIMESTAMP;
  split_dest_array_.reset();
  FetchTaskListNode::reset();
}

int PartFetchCtx::init(const common::ObPartitionKey &pkey,
    const int64_t start_tstamp,
    const int64_t start_log_id,
    const int64_t progress_id,
    IObLogPartTransResolver &part_trans_resolver,
    IObLogPartFetchMgr &part_fetch_mgr)
{
  int ret = OB_SUCCESS;
  // If the start log ID is 1, the service is started from creation
  bool start_serve_from_create = (1 == start_log_id);

  reset();

  pkey_ = pkey;
  serve_info_.reset(start_serve_from_create, start_tstamp);
  progress_.reset(start_log_id, start_tstamp);
  progress_id_ = progress_id;
  part_fetch_mgr_ = &part_fetch_mgr;
  part_trans_resolver_ = &part_trans_resolver;

  // Default is DDL stream type if it is a DDL partition, otherwise it is a hot stream
  if (is_ddl_table(pkey.get_table_id())) {
    stype_ = FETCH_STREAM_TYPE_DDL;
  } else {
    stype_ = FETCH_STREAM_TYPE_HOT;
  }

  return ret;
}

int PartFetchCtx::dispatch_heartbeat_if_need_()
{
  int ret = OB_SUCCESS;
  // Get current progress
  int64_t cur_progress = get_progress();
  if (OB_ISNULL(part_trans_resolver_)) {
    LOG_ERROR("invalid part_trans_resolver_", K(part_trans_resolver_));
    ret = OB_NOT_INIT;
  }
  // heartbeats are sent down only if progress updated
  else if (cur_progress != last_sync_progress_) {
    LOG_DEBUG("partition progress updated. generate HEARTBEAT task", K_(pkey),
        "last_sync_progress", TS_TO_STR(last_sync_progress_),
        "cur_progress", TS_TO_STR(cur_progress));

    if (OB_FAIL(part_trans_resolver_->heartbeat(pkey_, cur_progress))) {
      LOG_ERROR("generate HEARTBEAT task fail", KR(ret), K(pkey_), K(cur_progress));
    } else {
      last_sync_progress_ = cur_progress;
    }
  }
  return ret;
}

int PartFetchCtx::sync(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  int64_t pending_trans_count = 0;
  // Heartbeat issued according to conditions
  if (OB_FAIL(dispatch_heartbeat_if_need_())) {
    LOG_ERROR("dispatch_heartbeat_if_need_ fail", KR(ret));
  } else {
    ret = dispatch_(stop_flag, pending_trans_count);
  }
  return ret;
}

int PartFetchCtx::dispatch_(volatile bool &stop_flag, int64_t &pending_trans_count)
{
  int ret = OB_SUCCESS;
  // get current state
  int cur_state = state_;

  // If in a waiting state, no task is issued
  if (is_in_wait_state(cur_state)) {
    LOG_DEBUG("part is in wait state, can not dispatch trans task", K_(pkey),
        "cur_state", print_state(cur_state));
  } else if (OB_ISNULL(part_trans_resolver_)) {
    LOG_ERROR("invalid part trans resolver", K(part_trans_resolver_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(part_trans_resolver_->dispatch(stop_flag, pending_trans_count))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("resolver dispatch fail", KR(ret));
    }
  } else {
    LOG_DEBUG("dispatch trans task success", K_(pkey), K(pending_trans_count),
        "state", print_state(state_));
  }
  return ret;
}

int PartFetchCtx::read_log(const clog::ObLogEntry &log_entry,
    IObLogPartTransResolver::ObLogMissingInfo &missing,
    TransStatInfo &tsi,
    const bool need_filter_pg_no_missing_redo_trans,
    const IObLogPartTransResolver::ObAggreLogIndexArray &log_indexs,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const clog::ObLogEntryHeader &header = log_entry.get_header();
  const ObPartitionKey &pkey = header.get_partition_key();
  ObStorageLogType log_type = storage::OB_LOG_UNKNOWN;

  if (OB_ISNULL(part_trans_resolver_)) {
    LOG_ERROR("invalid part trans resolver", K(part_trans_resolver_));
    ret = OB_INVALID_ERROR;
  }
  // Verify log validity
  else if (OB_UNLIKELY(pkey != pkey_)) {
    LOG_ERROR("invalid log entry which pkey does not match", K(pkey_), K(log_entry));
    ret = OB_INVALID_ARGUMENT;
  }
  // Verifying log continuity
  else if (OB_UNLIKELY(progress_.get_next_log_id() != header.get_log_id())) {
    LOG_ERROR("log not sync", "next_log_id", progress_.get_next_log_id(),
        "cur_log_id", header.get_log_id(), K(log_entry));
    ret = OB_LOG_NOT_SYNC;
  }
  // Parsing logs and returning log types
  else if (OB_FAIL(part_trans_resolver_->read(log_entry, missing, tsi, serve_info_, log_type,
          need_filter_pg_no_missing_redo_trans, log_indexs))) {
    if (OB_ITEM_NOT_SETTED == ret) {
      // Missing redo log
    } else {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("resolve log entry fail", KR(ret), K(log_entry), K(serve_info_), K(log_type),
            K(need_filter_pg_no_missing_redo_trans), K(log_indexs));
      }
    }
  }
  // Handling split source partition logs
  else if (OB_LOG_SPLIT_SOURCE_PARTITION == log_type
      && OB_FAIL(handle_split_src_log_(log_entry, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_split_src_log_ fail", KR(ret), K(log_type), K(log_entry));
    }
  }
  // Processing split target partition logs
  else if (OB_LOG_SPLIT_DEST_PARTITION == log_type && OB_FAIL(handle_split_dst_log_(log_entry))) {
    LOG_ERROR("handle_split_dst_log_ fail", KR(ret), K(log_type), K(log_entry));
  }
  // Processing OFFLINE logs
  else if ((storage::ObStorageLogTypeChecker::is_offline_partition_log(log_type))
      && OB_FAIL(handle_offline_partition_log_(log_entry, stop_flag))) {
    LOG_ERROR("handle_offline_partition_log_ fail", KR(ret), K(log_type), K(log_entry));
  } else {
    uint64_t next_log_id = header.get_log_id() + 1;
    int64_t log_tstamp = header.get_submit_timestamp();

    // Advancing zoning progress
    if (OB_FAIL(progress_.update_log_progress(next_log_id, log_tstamp))) {
      LOG_ERROR("update log progress fail", KR(ret), K(next_log_id), K(log_tstamp), K(progress_));
    }

    LOG_DEBUG("read log and update progress success", K_(pkey), K(log_entry), K_(progress));
  }
  return ret;
}

int PartFetchCtx::handle_offline_partition_log_(const clog::ObLogEntry &log_entry,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const uint64_t log_id = log_entry.get_header().get_log_id();
  const int64_t tstamp = log_entry.get_header().get_submit_timestamp();

  ISTAT("[HANDLE_OFFLINE_LOG] begin", K_(pkey), "state", print_state(state_), K(log_id), K(tstamp));

  // For OFFLINE logs, only tasks in NORMAL state are processed
  // Tasks in other states will be deleted by other scenarios responsible for the partition
  //
  // Ensure that the discard recycling mechanism.
  //
  // 1. STATE_NORMAL: discard will be set when OFFLINE logging or partition deletion DDL is encountered
  // 2. STATE_WAIT: Deleting a partition DDL sets the discard
  // 3. STATE_SPLIT_IN_PROCESS: discard is set when a split is completed, and discard is also set when a partition DDL is deleted
  // 4. STATE_WAIT_AND_SPLIT_IN_PROCESS: Discard is set when splitting is complete and discard is set when partition DDL is deleted
  // 5. STATE_SPLIT_DONE: Discard will be set when the split is complete, and discard will be set when the partition DDL is deleted
  //
  // Note: Mechanically, we have to take precautions in many ways and cannot rely on one mechanism to guarantee partition recovery.
  // There are two scenarios in which partitions need to be reclaimed.
  // 1. partition deletion by DDL: this includes deleting tables, deleting partitions, deleting DBs, deleting tenants, etc. This scenario relies on DDL deletion to be sufficient
  // The observer ensures that the partition is not iterated over in the schema after the DDL is deleted
  //
  // 2. Partition completion: this scenario must wait for all logs to be collected before recycling
  // One is to receive the split completion log; the other is to receive the OFFLINE log.
  // The reason for the scenario that relies on the OFFLINE log is that the start timestamp may be located after the split completes and before OFFLINE,
  // so that the split completion log is not received. Therefore, the OFFLINE log has to be handled, and when the state is always STATE_NORMAL, discard is set if the OFFLINE log is encountered.
  //
  // It is worth noting that partitions can be deleted by DDL when they are in the middle of a split. The scenario is that when a DB is deleted or a tenant is deleted,
  // all partitions under the DB and tenant will be traversed, in which case the split source partition will also be deleted by DDL.
  // fetcher DEAD POOL has to handle this concurrency scenario.
  if (STATE_NORMAL == state_) {
    int64_t pending_trans_count = 0;
    // First ensure that all tasks in the queue are dispatched
    if (OB_FAIL(dispatch_(stop_flag, pending_trans_count))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("dispatch task fail", KR(ret), K(pkey_));
      }
    }
    // Check if there are pending transactions to be output
    else if (OB_UNLIKELY(pending_trans_count > 0)) {
      LOG_ERROR("there are still pending trans after dispatch when processing offline log, unexcept error",
          K(pending_trans_count), K(pkey_), K(state_));
      ret = OB_INVALID_DATA;
    } else {
      // Finally mark the partition as ready for deletion
      // Note: there is a concurrency situation here, after a successful setup, it may be dropped into the DEAD POOL for recycling by other threads immediately
      // Since all data is already output here, it doesn't matter if it goes to the DEAD POOL
      set_discarded();
    }
  }

  ISTAT("[HANDLE_OFFLINE_LOG] end", KR(ret), K_(pkey), "state", print_state(state_),
      K(log_id), K(tstamp));

  return ret;
}

// Processing of source partition split end logs
int PartFetchCtx::handle_split_src_log_(const clog::ObLogEntry &log_entry, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_header().get_data_len();
  ObStorageLogType log_type = storage::OB_LOG_UNKNOWN;
  ObPartitionSplitSourceLog split_src_log;
  bool split_in_process = false;

  // Parsing log headers: log types
  if (OB_FAIL(decode_storage_log_type(log_entry, pos, log_type))) {
    LOG_ERROR("decode_storage_log_type fail", KR(ret), K(log_entry), K(pos));
  } else if (OB_UNLIKELY(OB_LOG_SPLIT_SOURCE_PARTITION != log_type)) {
    LOG_ERROR("invalid log type which is not OB_LOG_SPLIT_SOURCE_PARTITION", K(log_type),
        K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  }
  // Deserialising source partition split logs
  else if (OB_FAIL(split_src_log.deserialize(buf, len, pos))) {
    LOG_ERROR("deserialize split source partition log fail", KR(ret), K(buf), K(len), K(pos),
        K(log_entry), K(log_type));
  }
  // Verify that the source partition is self
  else if (OB_UNLIKELY(split_src_log.get_spp().get_source_pkey() != pkey_)) {
    LOG_ERROR("unexcepted error, split source partition is not self", K_(pkey), K(split_src_log));
    ret = OB_ERR_UNEXPECTED;
  }
  // Populate an array of split target partitions
  // Target partition must be populated first, subsequent state transitions will depend on this array and there are concurrency scenarios
  else if (OB_FAIL(split_dest_array_.assign(split_src_log.get_spp().get_dest_array()))) {
    LOG_ERROR("assign split dest array fail", KR(ret), K(split_src_log), K(split_dest_array_));
  }
  // Prepare to split, switch state to STATE_SPLIT_IN_PROCESS or STATE_WAIT_AND_SPLIT_IN_PROCESS
  else if (OB_FAIL(prepare_split_(split_src_log, split_in_process))) {
    LOG_ERROR("prepare split fail", KR(ret), K(split_src_log));
  } else if (! split_in_process) {
    // The splitting operation does not continue and is taken over by other threads
    // Thereafter, the object data structure cannot be referenced again and a concurrency scenario exists
  }
  // Handle the split task
  // There will be no multi-threaded operation here, only one thread will handle the splitting
  else if (OB_FAIL(process_split_(stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("process_split_ fail", KR(ret), K(split_src_log));
    }
  } else {
    // success
  }

  return ret;
}

int PartFetchCtx::prepare_split_(const ObPartitionSplitSourceLog &split_src_log,
    bool &split_in_process)
{
  int ret = OB_SUCCESS;
  bool done = false;
  FetchState cur_state = state_;
  FetchState new_state = state_;
  // The reason for referencing the partition in the log here is to prevent the current object's memory from being reclaimed in a concurrency scenario
  const ObPartitionKey &pkey = split_src_log.get_spp().get_source_pkey();

  split_in_process = false;

  // Check status
  if (OB_UNLIKELY(STATE_NORMAL != cur_state && STATE_WAIT != cur_state)) {
    LOG_ERROR("state not match which can not prepare split", K(cur_state), K(pkey_));
    ret = OB_STATE_NOT_MATCH;
  } else {
    ISTAT("[SPLIT] [SPLIT_SRC] prepare split begin", K(pkey), "state", print_state(cur_state));

    while (! done) {
      switch (cur_state) {
        case STATE_NORMAL:
          // STATE_NORMAL -> STATE_SPLIT_IN_PROCESS
          new_state = STATE_SPLIT_IN_PROCESS;
          cur_state = ATOMIC_CAS(&state_, STATE_NORMAL, new_state);
          done = (STATE_NORMAL == cur_state);
          // Successful conversions toSTATE_SPLIT_IN_PROCESS，Only then can we continue to process the split
          if (done) {
            split_in_process = true;
          }
          break;
        case STATE_WAIT:
          // STATE_WAIT -> STATE_WAIT_AND_SPLIT_IN_PROCESS
          // Note that after the conversion to this state, no further reference can be made to the object's data content, and there may be concurrency scenarios where
          // Other threads will continue to advance the state and then the data structure will be reset or destructured
          new_state = STATE_WAIT_AND_SPLIT_IN_PROCESS;
          cur_state = ATOMIC_CAS(&state_, STATE_WAIT, new_state);
          done = (STATE_WAIT == cur_state);
          break;
          // Other states considered successful
        default:
          done = true;
          break;
      }
    }

    // The object data structure is no longer referenced here
    ISTAT("[SPLIT] [SPLIT_SRC] prepare split done", K(pkey),
        "old_state", print_state(cur_state),
        "new_state", print_state(new_state));
  }
  return ret;
}

int PartFetchCtx::process_split_(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  int64_t pending_trans_count = 0;
  ISTAT("[SPLIT] [SPLIT_SRC] process begin", K_(pkey), "state", print_state(state_),
      K_(discarded));
  // The required status is STATE_SPLIT_IN_PROCESS
  if (OB_UNLIKELY(STATE_SPLIT_IN_PROCESS != state_)) {
    LOG_ERROR("state not match which is not STATE_SPLIT_IN_PROCESS", K(state_), K(pkey_));
    ret = OB_STATE_NOT_MATCH;
  }
  // Firstly, the backlog of service tasks is issued
  else if (OB_FAIL(dispatch_(stop_flag, pending_trans_count))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("dispatch trans task fail", KR(ret));
    }
  }
  // Check if there are pending transactions to be output
  else if (OB_UNLIKELY(pending_trans_count > 0)) {
    LOG_ERROR("there are still pending trans after dispatch when processing split, unexcept error",
        K(pending_trans_count), K(pkey_), K(state_));
    ret = OB_INVALID_DATA;
  }
  // Then mark this partition splitting process complete
  else if (OB_FAIL(mark_split_done_())) {
    LOG_ERROR("mark split done fail", KR(ret), K(pkey_));
  }
  // At this point, all the transactions for this partition have been output, and the following processes the split target partition
  else if (OB_FAIL(process_split_dest_part_(stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("process_split_dest_part_ fail", KR(ret), K(pkey_));
    }
  } else {
    // Success, mark this partition as ready for deletion
    // Note: there is a concurrency situation here, after a successful setup, it may be immediately dropped into the DEAD POOL by other threads for recycling
    // Since all data is already output here, it does not matter if it goes to the DEAD POOL
    set_discarded();

    ISTAT("[SPLIT] [SPLIT_SRC] process done", K_(pkey), "state", print_state(state_),
        K_(discarded));
  }
  return ret;
}

int PartFetchCtx::mark_split_done_()
{
  int ret = OB_SUCCESS;
  int old_state = ATOMIC_CAS(&state_, STATE_SPLIT_IN_PROCESS, STATE_SPLIT_DONE);
  if (OB_UNLIKELY(STATE_SPLIT_IN_PROCESS != old_state)) {
    LOG_ERROR("state not match which is not STATE_SPLIT_IN_PROCESS", K(old_state), K(state_),
        K(pkey_));
    ret = OB_STATE_NOT_MATCH;
  } else {
    // success
    ISTAT("[SPLIT] mark split done", K_(pkey), "state", print_state(state_));
  }
  return ret;
}

int PartFetchCtx::process_split_dest_part_(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_fetch_mgr_)) {
    LOG_ERROR("invalid part fetch mgr", K(part_fetch_mgr_));
    ret = OB_NOT_INIT;
  }
  // Verify that the target split partition array is valid
  else if (OB_UNLIKELY(split_dest_array_.count() <= 0)) {
    LOG_ERROR("split dest array is invalid", K(split_dest_array_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t dst_part_count = split_dest_array_.count();
    ISTAT("[SPLIT] [SPLIT_SRC] process dest partitions", "count", dst_part_count,
        "src_part", pkey_);

    for (int64_t index = 0; OB_SUCCESS == ret && index < dst_part_count; index++) {
      const ObPartitionKey &pkey = split_dest_array_.at(index);
      // Notify target partition that source partition split processing is complete
      if (OB_FAIL(part_fetch_mgr_->activate_split_dest_part(pkey, stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("notify_src_part_split_done fail", KR(ret), K(pkey));
        }
      }
    }
  }
  return ret;
}

int PartFetchCtx::handle_when_src_split_done(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  // Is the old state marked WAIT
  // i.e. ：STATE_WAIT or STATE_WAIT_AND_SPLIT_IN_PROCESS
  bool old_state_mark_wait = false;
  // Has the old state started to split
  bool old_state_in_split = false;
  int64_t pending_trans_count = 0;

  ISTAT("[SPLIT] [ACTIVATE_DEST] handle begin", K_(pkey), "state", print_state(state_),
      K_(discarded));

  // Clear the WAIT wait state out
  if (OB_FAIL(clear_wait_state_(old_state_mark_wait, old_state_in_split))) {
    LOG_ERROR("clear_wait_state_ fail", KR(ret), K(pkey_));
  }
  // If previously entered into a waiting state, the stacked task is sent down
  else if (old_state_mark_wait && OB_FAIL(dispatch_(stop_flag, pending_trans_count))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("dispatch fail", KR(ret), K(pkey_));
    }
  }
  // If a split has been entered previously, the split is processed
  else if (old_state_in_split && OB_FAIL(process_split_(stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("process_split_ fail", KR(ret), K(pkey_));
    }
  }

  ISTAT("[SPLIT] [ACTIVATE_DEST] handle done", KR(ret), K_(pkey),
      "state", print_state(state_), K(old_state_mark_wait), K(old_state_in_split),
      K(pending_trans_count), K_(discarded));
  return ret;
}

// STATE_WAIT -> STATE_NORMAL
// STATE_WAIT_AND_SPLIT_IN_PROCESS -> STATE_SPLIT_IN_PROCESS
int PartFetchCtx::clear_wait_state_(bool &old_state_mark_wait, bool &old_state_in_split)
{
  int ret = OB_SUCCESS;
  bool done = false;
  FetchState cur_state = state_;
  FetchState new_state = state_;

  old_state_mark_wait = false;
  old_state_in_split = false;

  while (! done) {
    switch (cur_state) {
      case STATE_WAIT:
        // STATE_WAIT -> STATE_NORMAL
        // Clear wait status if currently waiting
        new_state = STATE_NORMAL;
        cur_state = ATOMIC_CAS(&state_, STATE_WAIT, new_state);
        done = (STATE_WAIT == cur_state);
        if (done) {
          old_state_mark_wait = true;
          old_state_in_split = false;   // The old state did not enter the split state
        }
        break;

        // STATE_WAIT_AND_SPLIT_IN_PROCESS -> STATE_SPLIT_IN_PROCESS
        // If the partition is in the wait state and has entered the split state, clear the wait state only and then enter the split state
      case STATE_WAIT_AND_SPLIT_IN_PROCESS:
        new_state = STATE_SPLIT_IN_PROCESS;
        cur_state = ATOMIC_CAS(&state_, STATE_WAIT_AND_SPLIT_IN_PROCESS, new_state);
        done = (STATE_WAIT_AND_SPLIT_IN_PROCESS == cur_state);
        if (done) {
          old_state_mark_wait = true;
          old_state_in_split = true;    // The old state has gone into schism
        }
        break;

        // Other states are not in the wait state and do not need to be processed
        // Since the source partition has already been split, the target partition will not enter the wait state subsequently
      default:
        done = true;
        old_state_in_split = false;
        old_state_mark_wait = false;
        break;
    }
  }

  ISTAT("[SPLIT] clear wait state", K_(pkey), "old_state", print_state(cur_state),
      "new_state", print_state(new_state), K(old_state_mark_wait), K(old_state_in_split));

  return ret;
}

// Process the target partition split log
// This partition is the target partition of the split, depending on the status of the source partition it is decided whether to wait or not
int PartFetchCtx::handle_split_dst_log_(const clog::ObLogEntry &log_entry)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_header().get_data_len();
  ObStorageLogType log_type = storage::OB_LOG_UNKNOWN;
  ObPartitionSplitDestLog split_dst_log;

  // Parsing log headers: log types
  if (OB_FAIL(decode_storage_log_type(log_entry, pos, log_type))) {
    LOG_ERROR("decode_storage_log_type fail", KR(ret), K(log_entry), K(pos));
  } else if (OB_UNLIKELY(OB_LOG_SPLIT_DEST_PARTITION != log_type)) {
    LOG_ERROR("invalid log type which is not OB_LOG_SPLIT_DEST_PARTITION", K(log_type),
        K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  }
  // Deserialising source partition split logs
  else if (OB_FAIL(split_dst_log.deserialize(buf, len, pos))) {
    LOG_ERROR("deserialize split dest partition log fail", KR(ret), K(buf), K(len), K(pos),
        K(log_entry), K(log_type));
  } else {
    const ObSplitPartitionPair &spp = split_dst_log.get_spp();
    bool src_part_split_done = false;

    ISTAT("[SPLIT] [SPLIT_DEST] process begin", K_(pkey), "state", print_state(state_),
        "src_pkey", spp.get_source_pkey(), "dst_pkey_count", spp.get_dest_array().count(),
        K_(discarded));

    // First into a waiting state
    if (OB_FAIL(mark_state_wait_from_normal_())) {
      LOG_ERROR("mark_state_wait_from_normal_ fail", KR(ret));
    }
    // Check if the source partition is split
    else if (OB_FAIL(check_src_part_split_state_(split_dst_log, src_part_split_done))) {
      LOG_ERROR("check_src_part_split_state_ fail", KR(ret), K(split_dst_log));
    } else if (! src_part_split_done) {
      // If the source partition does not finish splitting, it is no longer processed
      // Hand over to the source partition processing thread to continue advancing state
    }
    // If the source partition split is complete, mark the status NORMAL
    else if (OB_FAIL(mark_state_normal_from_wait_())) {
      LOG_ERROR("mark_state_normal_from_wait_ fail", KR(ret), K(pkey_));
    } else {
      // success
    }

    ISTAT("[SPLIT] [SPLIT_DEST] process done", K_(pkey), "state", print_state(state_),
        K(src_part_split_done), K_(discarded),
        "src_pkey", spp.get_source_pkey(), "dst_pkey_count", spp.get_dest_array().count());
  }

  return ret;
}

// convert state：STATE_NORMAL -> STATE_WAIT
int PartFetchCtx::mark_state_wait_from_normal_()
{
  int ret = OB_SUCCESS;
  FetchState new_state = STATE_WAIT;
  FetchState old_state = ATOMIC_CAS(&state_, STATE_NORMAL, new_state);

  // Requires current state to be STATE_NORMAL
  // Note: With future support for splitting no-kill transactions, the state here may not be equal to NORMAL and may enter WATI state earlier
  if (OB_UNLIKELY(STATE_NORMAL != old_state)) {
    LOG_ERROR("state not match which is not STATE_NORMAL", K(old_state), K(state_));
    ret = OB_STATE_NOT_MATCH;
  } else {
    ISTAT("[SPLIT] mark state wait", K_(pkey), "old_state", print_state(old_state),
        "new_state", print_state(new_state));
  }
  return ret;
}

int PartFetchCtx::check_src_part_split_state_(const ObPartitionSplitDestLog &split_dst_log,
    bool &src_part_split_done)
{
  int ret = OB_SUCCESS;
  src_part_split_done = false;
  const ObPartitionKey &src_pkey = split_dst_log.get_spp().get_source_pkey();
  uint64_t src_log_id = split_dst_log.get_source_log_id();
  int64_t src_log_ts = split_dst_log.get_source_log_ts();

  if (OB_ISNULL(part_fetch_mgr_)) {
    LOG_ERROR("invalid part fetch mgr", K(part_fetch_mgr_));
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(part_fetch_mgr_->check_part_split_state(src_pkey, src_log_id, src_log_ts,
      src_part_split_done))) {
    LOG_ERROR("check_part_split_state fail", KR(ret), K(src_pkey), K(pkey_), K(split_dst_log));
  } else {
    // success
  }

  return ret;
}

// State transition: STATE_WAIT -> STATE_NORMAL
// Note that.
// 1. this function needs to take concurrency into account, the source partition will also come over to concurrent settings
// 2. The wait state can only be STATE_WAIT, not STATE_WAIT_AND_SPLIT_IN_PROCESS, because this function is called
// when the split target partition log is received, and it is not possible to move on to the next round of splits
int PartFetchCtx::mark_state_normal_from_wait_()
{
  int ret = OB_SUCCESS;
  FetchState new_state = STATE_NORMAL;
  FetchState old_state = ATOMIC_CAS(&state_, STATE_WAIT, new_state);

  // Requires the current state to be STATE_WAIT or STATE_NORMAL
  // If it is STATE_NORMAL, the source partition is concurrently set
  if (OB_UNLIKELY(STATE_WAIT != old_state && STATE_NORMAL != old_state)) {
    LOG_ERROR("state not match which is not STATE_WAIT or STATE_NORMAL", K(old_state),
        K(state_), K(pkey_));
    ret = OB_STATE_NOT_MATCH;
  } else {
    ISTAT("[SPLIT] mark state normal", K_(pkey), "old_state", print_state(old_state),
        "new_state", print_state(new_state));
  }
  return ret;
}

int PartFetchCtx::read_missing_redo(const clog::ObLogEntry &log_entry,
    const IObLogPartTransResolver::ObTransIDArray &missing_log_trans_id_array)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(part_trans_resolver_)) {
    LOG_ERROR("invalid part trans resolver", K(part_trans_resolver_));
    ret = OB_INVALID_ERROR;
  }
  // Verify log validity
  else if (OB_UNLIKELY(log_entry.get_header().get_partition_key() != pkey_)) {
    LOG_ERROR("invalid log entry which pkey does not match", K(pkey_), K(log_entry));
    ret = OB_INVALID_ARGUMENT;
  }
  // Parsing missing logs
  else if (OB_FAIL(part_trans_resolver_->read_missing_redo(log_entry, missing_log_trans_id_array))) {
    LOG_ERROR("resolver read missing redo fail", KR(ret), K(log_entry), K(missing_log_trans_id_array));
  } else {
    // success
  }

  return ret;
}

// Note: this function is called concurrently with handle_when_src_split_done()
// part_trans_resolver ensures that the underlying data operations are mutually exclusive
//
// The scenario is that the split target partition is deleted early by the DDL while the split source partition is responsible for advancing the state of the split target partition
//
// This function is called by the DEAD POOL and will clean up all unpublished tasks and then downlink them
// handle_when_src_split_done() is called by the split source partition and requires it not to produce new tasks, otherwise the downline task would not be the last one.
//
// The implementation guarantees that tasks will only be produced when the log is read.
int PartFetchCtx::offline(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ISTAT("[OFFLINE_PART] begin", K_(pkey), "state", print_state(state_), K_(discarded));
  if (OB_ISNULL(part_trans_resolver_)) {
    LOG_ERROR("invalid part trans resolver", K(part_trans_resolver_));
    ret = OB_INVALID_ERROR;
  }
  // 发出“分区结束”任务
  else if (OB_FAIL(part_trans_resolver_->offline(stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("delete partition by part trans resolver fail", KR(ret));
    }
  } else {
    // 成功
    ISTAT("[OFFLINE_PART] end", K_(pkey), "state", print_state(state_), K_(discarded));
  }
  return ret;
}

bool PartFetchCtx::need_update_svr_list() const
{
  bool bool_ret = false;
  int64_t cur_time = get_timestamp();
  int64_t update_delta_time = cur_time - svr_list_last_update_tstamp_;
  int64_t avail_svr_count = get_cur_svr_list().count();
  int64_t svr_list_update_interval = ATOMIC_LOAD(&g_svr_list_update_interval);

  // If it has never been updated, it must be updated
  if (OB_INVALID_TIMESTAMP == svr_list_last_update_tstamp_) {
    bool_ret = true;
  }
  // If no server is available, or if a proactive update is requested, an update is required
  else if (avail_svr_count <= 0 || svr_list_need_update_) {
    bool_ret = true;
  }
  // Periodic updates are required if not updated for a period of time
  else if (update_delta_time >= svr_list_update_interval) {
    bool_ret = true;
  }

  LOG_DEBUG("need_update_svr_list", K(bool_ret), K(pkey_), K(svr_list_need_update_),
      K(update_delta_time), K(svr_list_update_interval), K(avail_svr_count),
      K(has_leader_), K(leader_));
  return bool_ret;
}

bool PartFetchCtx::need_update_leader_info() const
{
  bool bool_ret = false;
  int64_t cur_time = get_timestamp();
  int64_t update_delta_time = cur_time - leader_last_update_tstamp_;
  int64_t leader_info_update_interval = ATOMIC_LOAD(&g_leader_info_update_interval);

  // No mains need to be updated
  if (! has_leader_) {
    bool_ret = true;
  }
  // Periodic update of leader information
  else if (OB_INVALID_TIMESTAMP == leader_last_update_tstamp_
      || update_delta_time >= leader_info_update_interval) {
    bool_ret = true;
  }

  LOG_DEBUG("need_update_leader_info", K(bool_ret), K(pkey_), K(update_delta_time),
      K(leader_info_update_interval), K(has_leader_), K(leader_));

  return bool_ret;
}

bool PartFetchCtx::need_locate_start_log_id() const
{
  return OB_INVALID_ID == progress_.get_next_log_id();
}

int PartFetchCtx::update_svr_list(IObLogSvrFinder &svr_finder, const bool need_print_info)
{
  int ret = OB_SUCCESS;
  int64_t cur_time = get_timestamp();
  int64_t update_delta_time = cur_time - svr_list_last_update_tstamp_;
  int64_t svr_list_update_min_interval = DEFAULT_SVR_LIST_UPDATE_MIN_INTERVAL;
  int64_t start_tstamp = serve_info_.start_serve_timestamp_;

  if (OB_INVALID_TIMESTAMP != svr_list_last_update_tstamp_
      && svr_find_req_.is_state_idle()
      && update_delta_time < svr_list_update_min_interval) {
    // Check that the minimum update interval is met
    LOG_DEBUG("svr list is updated too often", K_(pkey), K(update_delta_time),
        K(svr_list_update_min_interval), "svr_list", get_cur_svr_list());
  } else {
    int state = svr_find_req_.get_state();

    // If in IDLE state, initiate an asynchronous request to update the server list
    if (SvrFindReq::IDLE == state) {
      // Prepare alternate objects for requesting server lists
      PartSvrList &req_svr_list = get_bak_svr_list();
      req_svr_list.reset();

      // Preparing the request structure
      uint64_t next_log_id = progress_.get_next_log_id();
      if (OB_INVALID_ID == next_log_id) {
        svr_find_req_.reset_for_req_by_tstamp(req_svr_list, pkey_, start_tstamp);
      } else {
        svr_find_req_.reset_for_req_by_log_id(req_svr_list, pkey_, next_log_id);
      }

      if (OB_FAIL(svr_finder.async_svr_find_req(&svr_find_req_))) {
        LOG_ERROR("launch async svr find request fail", KR(ret), K(svr_find_req_));
      }

      LOG_DEBUG("begin to update server list", KR(ret), K_(pkey), K(next_log_id));
    } else if (SvrFindReq::REQ == state) {
      // On request
    } else if (SvrFindReq::DONE == state) {
      svr_list_last_update_tstamp_ = get_timestamp();

      // Set the Trace ID to be used during execution
      ObLogTraceIdGuard guard(svr_find_req_.trace_id_);

      if (OB_SUCCESS != svr_find_req_.get_err_code()) {
        // Failed to print WARN
        LOG_WARN("update server list", "err", svr_find_req_.get_err_code(),
            "mysql_err", svr_find_req_.get_mysql_err_code(),
            K_(pkey), "next_log_id", progress_.get_next_log_id(), K_(serve_info),
            "svr_list", get_cur_svr_list());
      } else {
        // Update server list successfully, switch server list for atomic: switch from standby to official
        switch_svr_list();

        // Withdraw the server list update flag
        mark_svr_list_update_flag(false);

        if (need_print_info) {
          LOG_INFO("update server list", "err", svr_find_req_.get_err_code(),
              "mysql_err", svr_find_req_.get_mysql_err_code(),
              K_(pkey), "next_log_id", progress_.get_next_log_id(), K_(serve_info),
              "svr_list", get_cur_svr_list());
        } else {
          LOG_DEBUG("update server list", "err", svr_find_req_.get_err_code(),
              "mysql_err", svr_find_req_.get_mysql_err_code(),
              K_(pkey), "next_log_id", progress_.get_next_log_id(), K_(serve_info),
              "svr_list", get_cur_svr_list());
        }
      }

      // Finally reset the request structure anyway
      svr_find_req_.reset();
    } else {
      LOG_ERROR("invalid svr finder request", K(state), K(svr_find_req_));
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

int PartFetchCtx::update_leader_info(IObLogSvrFinder &svr_finder)
{
  int ret = OB_SUCCESS;
  int64_t cur_time = get_timestamp();
  int64_t update_delta_time = cur_time - leader_last_update_tstamp_;
  int64_t leader_info_update_min_interval = DEFAULT_LEADER_INFO_UPDATE_MIN_INTERVAL;

  if (OB_INVALID_TIMESTAMP != leader_last_update_tstamp_
      && leader_find_req_.is_state_idle()
      && update_delta_time < leader_info_update_min_interval) {
    // Check minimum update interval
    LOG_DEBUG("leader info is updated too often", K_(pkey), K(update_delta_time),
        K(leader_info_update_min_interval), K_(has_leader), K_(leader));
  } else {
    int state = leader_find_req_.get_state();

    if (LeaderFindReq::IDLE == state) {
      leader_find_req_.reset(pkey_);

      if (OB_FAIL(svr_finder.async_leader_find_req(&leader_find_req_))) {
        LOG_ERROR("launch leader find request fail", KR(ret), K(leader_find_req_));
      }

      LOG_DEBUG("begin to update leader info", KR(ret), K_(pkey));
    } else if (LeaderFindReq::REQ == state) {
      // On request
    } else if (LeaderFindReq::DONE == state) {
      leader_last_update_tstamp_ = get_timestamp();
      bool leader_changed = false;

      // Set the Trace ID to be used during execution
      ObLogTraceIdGuard guard(leader_find_req_.trace_id_);

      // Only process the result of a request if it is successful
      if (OB_SUCCESS == leader_find_req_.get_err_code()) {
        if (leader_find_req_.has_leader_ != has_leader_
            || leader_ != leader_find_req_.leader_) {
          leader_changed = true;
        }

        has_leader_ = leader_find_req_.has_leader_;
        leader_ = leader_find_req_.leader_;
      }

      // No leader or failed to request a leader, print WARN
      if (OB_SUCCESS != leader_find_req_.get_err_code() || ! has_leader_) {
        LOG_WARN("update leader info", "err", leader_find_req_.get_err_code(),
            "mysql_err", leader_find_req_.get_mysql_err_code(),
            K_(pkey), "has_leader", leader_find_req_.has_leader_,
            "leader", leader_find_req_.leader_);
      }
      // Print INFO when leader information changes
      else if (leader_changed) {
        LOG_INFO("update leader info", "err", leader_find_req_.get_err_code(),
            "mysql_err", leader_find_req_.get_mysql_err_code(),
            K_(pkey), "has_leader", leader_find_req_.has_leader_,
            "leader", leader_find_req_.leader_);
      } else {
        // In other cases, print DEBUG
        LOG_DEBUG("update leader info", "err", leader_find_req_.get_err_code(),
            "mysql_err", leader_find_req_.get_mysql_err_code(),
            K_(pkey), "has_leader", leader_find_req_.has_leader_,
            "leader", leader_find_req_.leader_);
      }

      // Finally reset the request anyway
      leader_find_req_.reset();
    } else {
      LOG_ERROR("invalid leader find request", K(state), K(leader_find_req_));
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

bool PartFetchCtx::need_heartbeat(const int64_t upper_limit)
{
  return progress_.need_heartbeat(upper_limit, ATOMIC_LOAD(&g_heartbeat_interval));
}

int PartFetchCtx::locate_start_log_id(IObLogStartLogIdLocator &start_log_id_locator)
{
  int ret = OB_SUCCESS;
  int state = start_log_id_locate_req_.get_state();
  int64_t start_tstamp = serve_info_.start_serve_timestamp_;

  if (StartLogIdLocateReq::IDLE == state) {
    start_log_id_locate_req_.reset(pkey_, start_tstamp);

    PartSvrList &cur_svr_list = get_cur_svr_list();

    if (cur_svr_list.count() <= 0) {
      LOG_INFO("server list is empty for locating start log id, mark for updating server list");
      mark_svr_list_update_flag(true);
    } else if (OB_FAIL(init_locate_req_svr_list_(start_log_id_locate_req_, cur_svr_list))) {
      LOG_ERROR("init_locate_req_svr_list_ fail", KR(ret), K(cur_svr_list));
    } else if (OB_FAIL(start_log_id_locator.async_start_log_id_req(&start_log_id_locate_req_))) {
      LOG_ERROR("launch async start log id request fail", KR(ret), K(start_log_id_locate_req_));
    } else {
      LOG_INFO("start log id locate request launched", K_(pkey),
          "start_tstamp", TS_TO_STR(start_tstamp),
          "svr_cnt", start_log_id_locate_req_.svr_list_.count(),
          "svr_list", start_log_id_locate_req_.svr_list_);
    }
  } else if (StartLogIdLocateReq::REQ == state) {
    // On request
  } else if (StartLogIdLocateReq::DONE == state) {
    uint64_t start_log_id = OB_INVALID_ID;
    ObAddr locate_svr;

    if (! start_log_id_locate_req_.get_result(start_log_id, locate_svr)) {
      LOG_ERROR("start log id locate fail", K_(start_log_id_locate_req));
    } else {
      progress_.set_next_log_id(start_log_id);
      LOG_INFO("start log id located", K_(pkey), K(start_log_id),
          "start_tstamp", TS_TO_STR(start_tstamp), K(locate_svr),
          "svr_cnt", start_log_id_locate_req_.svr_list_.count(),
          "svr_list", start_log_id_locate_req_.svr_list_);
    }

    // Reset the location request, whether successful or not
    start_log_id_locate_req_.reset();
  } else {
    LOG_ERROR("unknown start log id locator request state", K(state),
        K(start_log_id_locate_req_));
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}


int PartFetchCtx::update_heartbeat_info(IObLogFetcherHeartbeatWorker &heartbeater,
    IObLogSvrFinder &svr_finder)
{
  int ret = OB_SUCCESS;
  int64_t cur_time = get_timestamp();
  int64_t update_delta_time = cur_time - heartbeat_last_update_tstamp_;
  int64_t heartbeat_min_interval = ATOMIC_LOAD(&g_heartbeat_interval);

  if (OB_INVALID_TIMESTAMP != heartbeat_last_update_tstamp_
      && heartbeat_req_.is_state_idle()
      && update_delta_time < heartbeat_min_interval) {
    // Check minimum update interval to avoid frequent updates
    LOG_DEBUG("heartbeat is updated too often", K_(pkey), K(update_delta_time),
        K(heartbeat_min_interval), K_(progress));
  } else {
    int state = heartbeat_req_.get_state();

    // Idle state, ready to initiate a heartbeat
    if (HeartbeatRequest::IDLE == state) {
      if (OB_FAIL(launch_heartbeat_request_(heartbeater, svr_finder))) {
        LOG_ERROR("launch heartbeat request fail", KR(ret), K_(pkey));
      }
    } else if (HeartbeatRequest::REQ == state) {
      // Request in progress
    } else if (HeartbeatRequest::DONE == state) {
      // When the heartbeat request is complete, update the time of the last request
      heartbeat_last_update_tstamp_ = cur_time;

      // Request completed, heartbeat processed
      if (OB_FAIL(handle_heartbeat_request_(svr_finder))) {
        LOG_ERROR("handle heartbeat request fail", KR(ret), K_(pkey));
      }

      // Reset the heartbeat request anyway
      heartbeat_req_.reset();
    }
  }
  return ret;
}

int PartFetchCtx::launch_heartbeat_request_(IObLogFetcherHeartbeatWorker &heartbeater,
    IObLogSvrFinder &svr_finder)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! heartbeat_req_.is_state_idle())) {
    LOG_ERROR("heartbeat request state is not IDLE", K(heartbeat_req_));
    ret = OB_INVALID_ERROR;
  } else {
    // If there is no leader, first update the leader information
    if (! has_leader_) {
      if (OB_FAIL(update_leader_info(svr_finder))) {
        LOG_ERROR("update leader info fail", KR(ret), K(pkey_));
      }
    }

    if (OB_SUCCESS == ret) {
      if (! has_leader_) {
        LOG_DEBUG("partition has no leader, can not request heartbeat", K_(pkey));

        // If the leader query request has completed and there is still no leader, print the WARN message
        if (leader_find_req_.is_state_idle()) {
          LOG_WARN("partition has no leader, can not request heartbeat", K_(pkey),
              "leader_last_update_time", TS_TO_STR(leader_last_update_tstamp_));
        }
      } else {
        // leader is valid and initiates a heartbeat request to the leader
        heartbeat_req_.reset(pkey_, progress_.get_next_log_id(), leader_);

        LOG_DEBUG("launch heartbeat", K_(pkey), K_(leader), K_(progress));

        if (OB_FAIL(heartbeater.async_heartbeat_req(&heartbeat_req_))) {
          LOG_ERROR("launch async heartbeat request fail", KR(ret), K(heartbeat_req_));
        }
      }
    }
  }
  return ret;
}

int PartFetchCtx::handle_heartbeat_request_(IObLogSvrFinder &svr_finder)
{
  int ret = OB_SUCCESS;

  // Use the Trace ID from the request process
  ObLogTraceIdGuard guard(heartbeat_req_.trace_id_);

  // Requires a status of DONE
  if (OB_UNLIKELY(! heartbeat_req_.is_state_done())) {
    LOG_ERROR("heartbeat request state is not DONE", K(heartbeat_req_));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(part_trans_resolver_)) {
    LOG_ERROR("invalid part trans resolver", K(part_trans_resolver_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool need_update_leader_info = false;
    const HeartbeatResponse &resp = heartbeat_req_.get_resp();

    LOG_DEBUG("handle heartbeat", K_(pkey), K(resp), K_(leader));

    if (OB_SUCCESS != resp.rpc_err_ || OB_SUCCESS != resp.svr_err_) {
      LOG_ERROR("request heartbeat fail, rpc or server error", "rpc_err", resp.rpc_err_,
          "svr_err", resp.svr_err_, "svr", heartbeat_req_.svr_, "pkey", heartbeat_req_.pkey_);
      need_update_leader_info = true;
    } else {
      if (OB_SUCCESS != resp.partition_err_) {
        // The requested server is not the master
        LOG_INFO("heartbeat server is not master", K_(heartbeat_req));
        need_update_leader_info = true;
      }

      // Only handle cases where the primary or backup returns successful results
      if (OB_SUCCESS == resp.partition_err_ || OB_NOT_MASTER == resp.partition_err_) {
        if (OB_INVALID_ID == resp.next_served_log_id_ ||
            OB_INVALID_TIMESTAMP == resp.next_served_tstamp_) {
          LOG_ERROR("heartbeat result is invalid, this maybe server's BUG", K_(heartbeat_req));
        } else {
          // Update Heartbeat Progress
          progress_.update_heartbeat_progress(resp.next_served_log_id_,
              resp.next_served_tstamp_);
        }
      }
    }

    // If the leader information needs to be updated, the update task is launched immediately
    if (OB_SUCCESS == ret && need_update_leader_info) {
      reset_leader_info();

      if (OB_FAIL(update_leader_info(svr_finder))) {
        LOG_ERROR("update_leader_info fail", KR(ret), K_(pkey));
      }
    }
  }
  return ret;
}

int PartFetchCtx::update_log_heartbeat(const uint64_t next_log_id,
    const int64_t log_progress)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_trans_resolver_)) {
    LOG_ERROR("invalid part trans resolver", K(part_trans_resolver_));
    ret = OB_INVALID_ERROR;
  } else {
    int64_t old_progress = progress_.get_progress();

    // Update Log Heartbeat Progress
    if (OB_FAIL(progress_.update_log_heartbeat(next_log_id, log_progress))) {
      LOG_ERROR("PartProgress update log heartbeat fail", KR(ret), K(next_log_id), K(log_progress),
          K(progress_), K(pkey_));
    } else {
      int64_t new_progress = progress_.get_progress();
      bool progress_updated = false;

      // Check if progress is up to date
      if (OB_INVALID_TIMESTAMP != new_progress &&
          (OB_INVALID_TIMESTAMP == old_progress || new_progress > old_progress)) {
        progress_updated = true;
      } else {
        progress_updated = false;
      }

      LOG_DEBUG("update log heartbeat", K_(pkey), K(next_log_id), K(log_progress),
          K(old_progress), K(new_progress), K(progress_updated), K_(progress));
    }
  }
  return ret;
}

int PartFetchCtx::next_server(common::ObAddr &svr)
{
  int ret = OB_SUCCESS;
  PartSvrList &svr_list = get_cur_svr_list();
  uint64_t next_log_id = progress_.get_next_log_id();
  IBlackList::BLSvrArray wash_svr_array;
  wash_svr_array.reset();

  // Note: The blacklist must be cleansed before the next available server can be retrieved from the server list.
  if (OB_FAIL(blacklist_.do_white_washing(wash_svr_array))) {
    LOG_ERROR("blacklist do while washing fail", KR(ret), K(pkey_), K(blacklist_));
  } else {
    if (wash_svr_array.count() > 0) {
      ISTAT("[BLACK_LIST] [WASH]", KR(ret), K_(pkey),
          "wash_svr_cnt", wash_svr_array.count(), K(wash_svr_array));
    }

    if (OB_FAIL(svr_list.next_server(next_log_id, blacklist_, svr))) {
      if (OB_ITER_END != ret) {
        LOG_ERROR("get next server from svr_list fail", KR(ret), K(next_log_id), K(svr), K(svr_list));
      }
    } else {
      // Get server success
    }
  }

  if (OB_ITER_END == ret) {
    // If the server is exhausted, ask to update the server list
    mark_svr_list_update_flag(true);
  }

  return ret;
}

int PartFetchCtx::init_locate_req_svr_list_(StartLogIdLocateReq &req, PartSvrList &svr_list)
{
  int ret = OB_SUCCESS;
  // Locate start log ids preferably from the server with the latest logs to avoid inaccurate history tables that can lead to too many locate fallbacks.
  if (OB_FAIL(svr_list.get_server_array_for_locate_start_log_id(req.svr_list_))) {
    LOG_ERROR("get_server_array_for_locate_start_log_id fail", KR(ret), K(svr_list), K(req));
  }
  return ret;
}

PartSvrList &PartFetchCtx::get_cur_svr_list()
{
  return svr_list_[(ATOMIC_LOAD(&cur_svr_list_idx_)) % 2];
}

const PartSvrList &PartFetchCtx::get_cur_svr_list() const
{
  return svr_list_[(ATOMIC_LOAD(&cur_svr_list_idx_)) % 2];
}

PartSvrList &PartFetchCtx::get_bak_svr_list()
{
  return svr_list_[(ATOMIC_LOAD(&cur_svr_list_idx_) + 1) % 2];
}

void PartFetchCtx::switch_svr_list()
{
  ATOMIC_INC(&cur_svr_list_idx_);
}

void PartFetchCtx::mark_svr_list_update_flag(const bool need_update)
{
  ATOMIC_STORE(&svr_list_need_update_, need_update);
}

uint64_t PartFetchCtx::hash() const
{
  // hash by "PKEY + next_log_id"
  uint64_t next_log_id = progress_.get_next_log_id();
  return murmurhash(&next_log_id, sizeof(next_log_id), pkey_.hash());
}

// Timeout conditions: (both satisfied)
// 1. the progress is not updated for a long time on the target server
// 2. progress is less than upper limit
int PartFetchCtx::check_fetch_timeout(const common::ObAddr &svr,
    const int64_t upper_limit,
    const int64_t fetcher_resume_tstamp,
    bool &is_fetch_timeout,                       // Is the log fetch timeout
    bool &is_fetch_timeout_on_lagged_replica)     // Is the log fetch timeout on a lagged replica
{
  int ret = OB_SUCCESS;
  int64_t cur_time = get_timestamp();
  int64_t svr_start_fetch_tstamp = OB_INVALID_TIMESTAMP;
  // Partition timeout, after which time progress is not updated, it is considered to be a log fetch timeout
  const int64_t partition_timeout = TCONF.partition_timeout_sec * _SEC_;
  // Timeout period for partitions on lagging replica, compared to normal timeout period
  const int64_t partition_timeout_for_lagged_replica = TCONF.partition_timeout_sec_for_lagged_replica * _SEC_;

  is_fetch_timeout = false;
  is_fetch_timeout_on_lagged_replica = false;

  // Get the starting log time on the current server
  if (OB_FAIL(fetch_info_.get_cur_svr_start_fetch_tstamp(svr, svr_start_fetch_tstamp))) {
    LOG_ERROR("get_cur_svr_start_fetch_tstamp fail", KR(ret), K(svr), K(fetch_info_));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == svr_start_fetch_tstamp)) {
    LOG_ERROR("invalid start fetch tstamp", K(svr_start_fetch_tstamp), K(fetch_info_));
    ret = OB_INVALID_ERROR;
  } else {
    // Get the current progress and when the progress was last updated
    int64_t cur_progress = progress_.get_progress();
    int64_t progress_last_update_tstamp = progress_.get_touch_tstamp();
    int64_t next_served_log_id = progress_.get_next_served_log_id();
    int64_t hb_touch_tstamp = progress_.get_hb_touch_tstamp();
    int64_t next_log_id = progress_.get_next_log_id();

    if (OB_INVALID_TIMESTAMP != cur_progress && cur_progress >= upper_limit) {
      is_fetch_timeout = false;
      is_fetch_timeout_on_lagged_replica = false;
    } else {
      // Consider the time at which logs start to be fetched on the server as a lower bound for progress updates
      // Ensure that the partition stays on a server for a certain period of time
      int64_t last_update_tstamp = OB_INVALID_TIMESTAMP;
      if ((OB_INVALID_TIMESTAMP == progress_last_update_tstamp)) {
        last_update_tstamp = svr_start_fetch_tstamp;
      } else {
        last_update_tstamp = std::max(progress_last_update_tstamp, svr_start_fetch_tstamp);
      }

      if (OB_INVALID_TIMESTAMP != fetcher_resume_tstamp) {
        // After a fetcher pause and restart, the fetcher pause time is also counted as partition fetch time,
        // a misjudgement may occur, resulting in a large number of partition timeouts being dispatched
        last_update_tstamp = std::max(last_update_tstamp, fetcher_resume_tstamp);
      }

      // Progress update interval
      const int64_t progress_update_interval = (cur_time - last_update_tstamp);
      // Heartbeat progress update interval
      const int64_t hb_progress_update_interval =
          OB_INVALID_TIMESTAMP != hb_touch_tstamp ? (cur_time - hb_touch_tstamp) : 0;

      // long periods of non-updating progress and progress timeouts, where it is no longer necessary to determine if the machine is behind in its backup
      if (progress_update_interval >= partition_timeout) {
        is_fetch_timeout = true;
        is_fetch_timeout_on_lagged_replica = false;
      } else {
        // Before the progress timeout, verify that the server fetching the logs is a lagged replica, and if the logs are not fetched on the lagged replica for some time, then it is also considered a progress timeout
        // Generally, the timeout for a lagged copy is less than the progress timeout
        // partition_timeout_for_lagged_replica < partition_timeout
        //
        // How to define a long timeout for fetching logs on a lagged replica?
        // 1. lagged replica: the next log does exist, but this server can't fetch it, indicating that this server is most likely behind the replica
        // 2. When to start counting the timeout: from the time liboblog confirms the existence of the next log
        //
        // next_served_log_id: the log ID of the next service in the Leader, if the next log is smaller than it, the next log exists
        // hb_touch_tstamp: Leader heartbeat update time, i.e. the time to confirm the log ID of the largest service
        if (OB_INVALID_ID != next_served_log_id
            && next_log_id < next_served_log_id   // Next log exists
            && progress_update_interval >= partition_timeout_for_lagged_replica   // Progress update time over lagging replica configuration item
            && hb_progress_update_interval >= partition_timeout_for_lagged_replica) { // Heartbeat progress update time over lagging replica configuration item
          is_fetch_timeout = true;
          is_fetch_timeout_on_lagged_replica = true;
        }
      }

      if (is_fetch_timeout) {
        LOG_INFO("[CHECK_PROGRESS_TIMEOUT]", K_(pkey), K(svr),
            K(is_fetch_timeout), K(is_fetch_timeout_on_lagged_replica),
            K(progress_update_interval), K(hb_progress_update_interval),
            K(progress_),
            "update_limit", TS_TO_STR(upper_limit),
            "last_update_tstamp", TS_TO_STR(last_update_tstamp),
            "svr_start_fetch_tstamp", TS_TO_STR(svr_start_fetch_tstamp));
      } else {
        LOG_DEBUG("[CHECK_PROGRESS_TIMEOUT]", K_(pkey), K(svr),
            K(is_fetch_timeout), K(is_fetch_timeout_on_lagged_replica),
            K(progress_update_interval), K(hb_progress_update_interval),
            K(progress_),
            "update_limit", TS_TO_STR(upper_limit),
            "last_update_tstamp", TS_TO_STR(last_update_tstamp),
            "svr_start_fetch_tstamp", TS_TO_STR(svr_start_fetch_tstamp));
      }
    }
  }
  return ret;
}

int PartFetchCtx::get_dispatch_progress(int64_t &dispatch_progress, PartTransDispatchInfo &dispatch_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part_trans_resolver_)) {
    LOG_ERROR("invalid part trans resolver", K(part_trans_resolver_));
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(part_trans_resolver_->get_dispatch_progress(dispatch_progress,
      dispatch_info))) {
    LOG_ERROR("get_dispatch_progress from part trans resolver fail", KR(ret), K(pkey_));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == dispatch_progress)) {
    LOG_ERROR("dispatch_progress is invalid", K(dispatch_progress), K(pkey_), K(dispatch_info));
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

bool PartFetchCtx::is_in_use() const
{
  // As long as there is an asynchronous request in progress, it is considered to be "in use"
  return svr_find_req_.is_state_req()
      || start_log_id_locate_req_.is_state_req()
      || leader_find_req_.is_state_req()
      || heartbeat_req_.is_state_req();
}

void PartFetchCtx::print_dispatch_info() const
{
  PartProgress cur_progress;
  progress_.atomic_copy(cur_progress);

  int64_t progress = cur_progress.get_progress();

  _ISTAT("[DISPATCH_FETCH_TASK] PKEY=%s TO=%s FROM=%s REASON=\"%s\" "
      "DELAY=%s PROGRESS=%s DISCARDED=%d",
      to_cstring(pkey_), to_cstring(fetch_info_.cur_mod_),
      to_cstring(fetch_info_.out_mod_), fetch_info_.out_reason_,
      TS_TO_DELAY(progress),
      to_cstring(cur_progress),
      discarded_);
}

void PartFetchCtx::dispatch_in_idle_pool()
{
  fetch_info_.dispatch_in_idle_pool();
  print_dispatch_info();
}

void PartFetchCtx::dispatch_in_fetch_stream(const common::ObAddr &svr, FetchStream &fs)
{
  fetch_info_.dispatch_in_fetch_stream(svr, fs);
  print_dispatch_info();
}

void PartFetchCtx::dispatch_in_dead_pool()
{
  fetch_info_.dispatch_in_dead_pool();
  print_dispatch_info();
}

int PartFetchCtx::get_cur_svr_start_fetch_tstamp(const common::ObAddr &svr,
      int64_t &svr_start_fetch_tstamp) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(fetch_info_.get_cur_svr_start_fetch_tstamp(svr, svr_start_fetch_tstamp))) {
    LOG_ERROR("get_cur_svr_start_fetch_tstamp fail", KR(ret), K(fetch_info_),
        K(svr), K(svr_start_fetch_tstamp));
  }

  return ret;
}

int PartFetchCtx::add_into_blacklist(const common::ObAddr &svr,
    const int64_t svr_service_time,
    int64_t &survival_time)
{
  int ret = OB_SUCCESS;
  int64_t blacklist_history_clear_interval = ATOMIC_LOAD(&g_blacklist_history_clear_interval);

  // Cyclical cleaning blacklist history
  if (REACH_TIME_INTERVAL(blacklist_history_clear_interval)) {
    IBlackList::SvrHistoryArray clear_svr_array;
    clear_svr_array.reset();
    if (OB_FAIL(clear_blacklist_history_(clear_svr_array))) {
      LOG_ERROR("blacklist remove history error", KR(ret), K(pkey_));
    } else {
      if (clear_svr_array.count() > 0) {
        // Print the number of blacklisted servers and the servers
        ISTAT("[BLACK_LIST] [CLEAR]", KR(ret), K(pkey_),
            "clear_svr_cnt", clear_svr_array.count(), K(clear_svr_array));
      }
    }
  }

  if (OB_SUCCESS == ret) {
    if (OB_FAIL(blacklist_.add(svr, svr_service_time, survival_time))) {
      LOG_ERROR("blacklist add error", KR(ret), K(pkey_), K(svr),
          "svr_service_time", TVAL_TO_STR(svr_service_time),
          "survival_time", TVAL_TO_STR(survival_time));
    } else {
      ISTAT("[BLACK_LIST] [ADD]", K_(pkey), K(svr),
          "svr_service_time", TVAL_TO_STR(svr_service_time),
          "survival_time", TVAL_TO_STR(survival_time),
          "blacklist_cnt", blacklist_.count(), K_(blacklist));
    }
  }

  return ret;
}

int PartFetchCtx::clear_blacklist_history_(IBlackList::SvrHistoryArray &clear_svr_array)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(blacklist_.clear_overdue_history(clear_svr_array))) {
  } else {
    // succ
  }

  return ret;
}

bool PartFetchCtx::need_switch_server(const common::ObAddr &cur_svr)
{
  bool bool_ret = false;
  PartSvrList &svr_list = get_cur_svr_list();
  uint64_t next_log_id = progress_.get_next_log_id();

  bool_ret = svr_list.need_switch_server(next_log_id, blacklist_, pkey_, cur_svr);

  return bool_ret;
}

bool PartFetchCtx::is_split_done(const uint64_t split_log_id, const int64_t split_log_ts) const
{
  // If the start timestamp is greater than the split log timestamp, the split is complete
  bool split_done = (split_log_ts < serve_info_.start_serve_timestamp_);
  int cur_state = state_;

  // Otherwise the current state prevails
  if (! split_done) {
    split_done = (STATE_SPLIT_DONE == cur_state);
  }

  ISTAT("[SPLIT] [CHECK_STATE]", K_(pkey), K(split_done),
      "cur_state", print_state(cur_state),
      K(split_done), K(split_log_id), K(split_log_ts), K(serve_info_),
      K_(discarded));
  return split_done;
}

/////////////////////////////////// PartProgress ///////////////////////////////////

void PartFetchCtx::PartProgress::reset()
{
  progress_ = OB_INVALID_TIMESTAMP;
  touch_tstamp_ = OB_INVALID_TIMESTAMP;
  is_log_progress_ = false;

  next_log_id_ = OB_INVALID_ID;
  log_progress_ = OB_INVALID_TIMESTAMP;
  log_touch_tstamp_ = OB_INVALID_TIMESTAMP;

  next_served_log_id_ = OB_INVALID_ID;
  next_served_tstamp_ = OB_INVALID_TIMESTAMP;
  hb_touch_tstamp_ = OB_INVALID_TIMESTAMP;
}

// start_log_id refers to the start log id, which may not be valid
// start_tstamp refers to the partition start timestamp, not the start_log_id log timestamp
//
// Therefore, this function sets start_tstamp to the current progress progress_, but does not update to log_progress_
// log_progress_ only indicates log progress
void PartFetchCtx::PartProgress::reset(const uint64_t start_log_id, const int64_t start_tstamp)
{
  reset();

  // Update next_log_id
  // but does not update the log progress
  next_log_id_ = start_log_id;

  // Set start-up timestamp to global progress
  progress_ = start_tstamp;
  touch_tstamp_ = get_timestamp();
  is_log_progress_ = true;
}

// If the progress is greater than the upper limit, the touch timestamp of the corresponding progress is updated
// NOTE: The purpose of this function is to prevent the touch timestamp from not being updated for a long time if the progress
// is greater than the upper limit, which could lead to a false detection of a progress timeout if the upper limit suddenly increases.
void PartFetchCtx::PartProgress::update_touch_tstamp_if_progress_beyond_upper_limit(const int64_t upper_limit)
{
  ObByteLockGuard guard(lock_);

  if (OB_INVALID_TIMESTAMP != progress_
      && OB_INVALID_TIMESTAMP != upper_limit
      && progress_ >= upper_limit) {
    touch_tstamp_ = get_timestamp();
  }
}

// Two scenarios for updating heartbeats.
// 1. the heartbeat message is invalid
// 2. The log is not behind the heartbeat, and the progress is less than the upper limit, and the progress update times out
bool PartFetchCtx::PartProgress::need_heartbeat(const int64_t upper_limit,
    const int64_t hb_interval) const
{
  ObByteLockGuard guard(lock_);

  // Is the progress update timed out
  bool is_progress_timeout = false;
  if (OB_INVALID_TIMESTAMP == touch_tstamp_) {
    is_progress_timeout = true;
  } else {
    // If progress has not been updated for a period of time, it is suspected that a heartbeat will need to be used to update progress
    is_progress_timeout = ((get_timestamp() - touch_tstamp_) >= hb_interval);
  }

  // Is the log lagging behind the heartbeat
  bool log_fall_behind_hb = (OB_INVALID_ID == next_log_id_ || next_log_id_ < next_served_log_id_);

  return OB_INVALID_TIMESTAMP == next_served_tstamp_
      || OB_INVALID_ID == next_served_log_id_
      || OB_INVALID_TIMESTAMP == progress_
      || ((! log_fall_behind_hb) && progress_ < upper_limit && is_progress_timeout);
}

void PartFetchCtx::PartProgress::update_heartbeat_progress(const uint64_t ns_log_id,
    const int64_t ns_tstamp)
{
  ObByteLockGuard guard(lock_);

  if (OB_INVALID_ID != ns_log_id && OB_INVALID_TIMESTAMP != ns_tstamp) {
    bool touched = false;

    if (OB_INVALID_ID == next_served_log_id_ || ns_log_id > next_served_log_id_) {
      next_served_log_id_ = ns_log_id;
      touched = true;
    }

    if (OB_INVALID_TIMESTAMP == next_served_tstamp_ || ns_tstamp > next_served_tstamp_) {
      next_served_tstamp_ = ns_tstamp;
      touched = true;
    }

    if (touched) {
      hb_touch_tstamp_ = get_timestamp();

      // Update global progress
      update_progress_();
    }
  }
}

int PartFetchCtx::PartProgress::update_log_progress(const uint64_t new_next_log_id,
    const int64_t new_log_progress)
{
  ObByteLockGuard guard(lock_);

  int ret = OB_SUCCESS;
  // Require next_log_id to be valid
  if (OB_UNLIKELY(OB_INVALID_ID == next_log_id_)) {
    LOG_ERROR("invalid next_log_id", K(next_log_id_), K(log_progress_), K(progress_));
    ret = OB_INVALID_ERROR;
  }
  // Verifying log continuity
  else if (OB_UNLIKELY((next_log_id_ + 1) != new_next_log_id)) {
    LOG_ERROR("log not sync", K(next_log_id_), K(new_next_log_id));
    ret = OB_LOG_NOT_SYNC;
  } else {
    next_log_id_ = new_next_log_id;

    // Update log progress if it is invalid, or if log progress has been updated
    if (OB_INVALID_TIMESTAMP == log_progress_ ||
        (OB_INVALID_TIMESTAMP != new_log_progress && new_log_progress > log_progress_)) {
      log_progress_ = new_log_progress;
    }

    log_touch_tstamp_ = get_timestamp();

    // Update global progress
    update_progress_();

    // Log progress update, forcing the update timestamp of the global progress to be updated
    // NOTE: The reason is: the log progress is updated, indicating that the log was fetched and that the progress was updated anyway
    //
    // 1. normally, if the log progress is updated, the global progress is also updated, because the global progress is equal to the log progress
    // 2. At startup, there is a log rollback and the global progress is equal to the startup timestamp and cannot be rolled back,
    // so the log progress is less than the global progress and the update of the log progress does not drive the global progress update,
    // but the partition does pull the log, in which case the global progress should be considered to be forcibly updated
    // and therefore the "update timestamp of global progress" needs to be updated
    touch_tstamp_ = log_touch_tstamp_;
  }
  return ret;
}

int PartFetchCtx::PartProgress::update_log_heartbeat(const uint64_t next_log_id,
    const int64_t new_log_progress)
{
  ObByteLockGuard guard(lock_);

  int ret = OB_SUCCESS;

  // Request the next log to be valid
  if (OB_UNLIKELY(OB_INVALID_ID == next_log_id_)) {
    LOG_ERROR("invalid log progress", K(next_log_id_), K(log_progress_));
    ret = OB_INVALID_ERROR;
  }
  // Require log id match
  else if (OB_UNLIKELY(next_log_id != next_log_id_)) {
    LOG_ERROR("next log id does not match, log not sync", K(next_log_id_), K(next_log_id));
    ret = OB_LOG_NOT_SYNC;
  } else if (OB_INVALID_TIMESTAMP == log_progress_ ||
      (OB_INVALID_TIMESTAMP != new_log_progress && new_log_progress > log_progress_)) {
    // Update the touch timestamp only after the progress has actually been updated
    log_progress_ = new_log_progress;
    log_touch_tstamp_ = get_timestamp();

    // Update global progress
    update_progress_();

    // Log progress update, force update of global progress update timestamp
    touch_tstamp_ = log_touch_tstamp_;
  }

  return ret;
}

void PartFetchCtx::PartProgress::update_progress_()
{
  int64_t new_progress = OB_INVALID_TIMESTAMP;
  bool new_is_log_progress = false;
  bool new_touch_tstamp = OB_INVALID_TIMESTAMP;

  // If the heartbeat progress is ahead of the log stream progress in the case of fetching the latest log, then the heartbeat progress is used
  // Note: requires next_served_log_id == next_log_id, i.e. the log streams are consistent
  if (OB_INVALID_ID != next_log_id_
      && OB_INVALID_TIMESTAMP != next_served_tstamp_
      && OB_INVALID_ID != next_served_log_id_
      && next_log_id_ == next_served_log_id_) {
    if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == log_progress_) || next_served_tstamp_ > log_progress_) {
      new_progress = next_served_tstamp_;
      new_is_log_progress = false;
      new_touch_tstamp = hb_touch_tstamp_;
    } else {
      new_progress = log_progress_;
      new_is_log_progress = true;
      new_touch_tstamp = log_touch_tstamp_;
    }
  } else {
    // Default is log stream progress
    new_progress = log_progress_;
    new_is_log_progress = true;
    new_touch_tstamp = log_touch_tstamp_;
  }

  // At this point, the new progress value has been confirmed and updated to the global progress if it is ahead of the global progress
  if (OB_INVALID_TIMESTAMP != new_progress && new_progress > progress_) {
    progress_ = new_progress;
    is_log_progress_ = new_is_log_progress;

    // If the update timestamp is not updated, it is updated to the current timestamp
    // indicates that the global progress has just been updated
    if (new_touch_tstamp <= touch_tstamp_) {
      touch_tstamp_ = get_timestamp();
    } else {
      touch_tstamp_ = new_touch_tstamp;
    }
  }
}

void PartFetchCtx::PartProgress::atomic_copy(PartProgress &prog) const
{
  // protected by lock
  ObByteLockGuard guard(lock_);

  prog.progress_ = progress_;
  prog.touch_tstamp_ = touch_tstamp_;
  prog.is_log_progress_ = is_log_progress_;

  prog.next_log_id_ = next_log_id_;
  prog.log_progress_ = log_progress_;
  prog.log_touch_tstamp_ = log_touch_tstamp_;

  prog.next_served_log_id_ = next_served_log_id_;
  prog.next_served_tstamp_ = next_served_tstamp_;
  prog.hb_touch_tstamp_ = hb_touch_tstamp_;
}

///////////////////////////////// FetchModule /////////////////////////////////
void PartFetchCtx::FetchModule::reset()
{
  module_ = FETCH_MODULE_NONE;
  svr_.reset();
  fetch_stream_ = NULL;
  start_fetch_tstamp_ = OB_INVALID_TIMESTAMP;
}

void PartFetchCtx::FetchModule::reset_to_idle_pool()
{
  reset();
  module_ = FETCH_MODULE_IDLE_POOL;
}

void PartFetchCtx::FetchModule::reset_to_fetch_stream(const common::ObAddr &svr, FetchStream &fs)
{
  module_ = FETCH_MODULE_FETCH_STREAM;
  svr_ = svr;
  fetch_stream_ = &fs;
  start_fetch_tstamp_ = get_timestamp();
}

void PartFetchCtx::FetchModule::reset_to_dead_pool()
{
  reset();
  module_ = FETCH_MODULE_DEAD_POOL;
}

int64_t PartFetchCtx::FetchModule::to_string(char *buffer, const int64_t size) const
{
  int64_t pos = 0;

  switch (module_) {
    case FETCH_MODULE_NONE: {
      (void)databuff_printf(buffer, size, pos, "NONE");
      break;
    }

    case FETCH_MODULE_IDLE_POOL: {
      (void)databuff_printf(buffer, size, pos, "IDLE_POOL");
      break;
    }

    case FETCH_MODULE_FETCH_STREAM: {
      (void)databuff_printf(buffer, size, pos, "[%s](%p)",
          to_cstring(svr_), fetch_stream_);
      break;
    }

    case FETCH_MODULE_DEAD_POOL: {
      (void)databuff_printf(buffer, size, pos, "DEAD_POOL");
      break;
    }

    default: {
      // Invalid module
      (void)databuff_printf(buffer, size, pos, "INVALID");
      break;
    }
  }

  return pos;
}

///////////////////////////////// FetchInfo /////////////////////////////////
void PartFetchCtx::FetchInfo::reset()
{
  cur_mod_.reset();
  out_mod_.reset();
  out_reason_ = "NONE";
}

void PartFetchCtx::FetchInfo::dispatch_in_idle_pool()
{
  cur_mod_.reset_to_idle_pool();
}

void PartFetchCtx::FetchInfo::dispatch_in_fetch_stream(const common::ObAddr &svr, FetchStream &fs)
{
  cur_mod_.reset_to_fetch_stream(svr, fs);
}

void PartFetchCtx::FetchInfo::dispatch_in_dead_pool()
{
  cur_mod_.reset_to_dead_pool();
}

void PartFetchCtx::FetchInfo::dispatch_out(const char *reason)
{
  out_mod_ = cur_mod_;
  cur_mod_.reset();
  out_reason_ = reason;
}

int PartFetchCtx::FetchInfo::get_cur_svr_start_fetch_tstamp(const common::ObAddr &svr,
    int64_t &svr_start_fetch_ts) const
{
  int ret = OB_SUCCESS;

  svr_start_fetch_ts = OB_INVALID_TIMESTAMP;

  // Requires that the FetchStream module is currently in progress
  if (OB_UNLIKELY(FetchModule::FETCH_MODULE_FETCH_STREAM != cur_mod_.module_)) {
    LOG_ERROR("current module is not FetchStream", K(cur_mod_));
    ret = OB_INVALID_ERROR;
  }
  // verify server
  else if (OB_UNLIKELY(svr != cur_mod_.svr_)) {
    LOG_ERROR("server does not match", K(svr), K(cur_mod_));
    ret = OB_INVALID_ARGUMENT;
  } else {
    svr_start_fetch_ts = cur_mod_.start_fetch_tstamp_;
  }

  return ret;
}

//////////////////////////////////////// PartFetchInfoForPrint //////////////////////////////////

PartFetchInfoForPrint::PartFetchInfoForPrint() :
    tps_(0),
    is_discarded_(false),
    pkey_(),
    progress_(),
    fetch_mod_(),
    dispatch_progress_(OB_INVALID_TIMESTAMP),
    dispatch_info_()
{
}

int PartFetchInfoForPrint::init(PartFetchCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctx.get_dispatch_progress(dispatch_progress_, dispatch_info_))) {
    LOG_ERROR("get_dispatch_progress from PartFetchCtx fail", KR(ret), K(ctx));
  } else {
    tps_ = ctx.get_tps();
    is_discarded_ = ctx.is_discarded();
    pkey_ = ctx.get_pkey();
    ctx.get_progress_struct(progress_);
    fetch_mod_ = ctx.get_cur_fetch_module();
  }
  return ret;
}

void PartFetchInfoForPrint::print_fetch_progress(const char *description,
    const int64_t idx,
    const int64_t array_cnt,
    const int64_t cur_time) const
{
  _LOG_INFO("[STAT] %s idx=%ld/%ld pkey=%s mod=%s "
      "discarded=%d delay=%s tps=%.2lf progress=%s",
      description, idx, array_cnt, to_cstring(pkey_),
      to_cstring(fetch_mod_),
      is_discarded_, TVAL_TO_STR(cur_time - progress_.get_progress()),
      tps_, to_cstring(progress_));
}

void PartFetchInfoForPrint::print_dispatch_progress(const char *description,
    const int64_t idx,
    const int64_t array_cnt,
    const int64_t cur_time) const
{
  _LOG_INFO("[STAT] %s idx=%ld/%ld pkey=%s delay=%s pending_task(queue/total)=%ld/%ld "
      "dispatch_progress=%s last_dispatch_log_id=%lu next_task=%s "
      "next_trans(log_id=%lu,committed=%d,ready_to_commit=%d,global_version=%s) checkpoint=%s",
      description, idx, array_cnt, to_cstring(pkey_),
      TVAL_TO_STR(cur_time - dispatch_progress_),
      dispatch_info_.task_count_in_queue_,
      dispatch_info_.pending_task_count_,
      TS_TO_STR(dispatch_progress_),
      dispatch_info_.last_dispatch_log_id_,
      dispatch_info_.next_task_type_,
      dispatch_info_.next_trans_log_id_,
      dispatch_info_.next_trans_committed_,
      dispatch_info_.next_trans_ready_to_commit_,
      TS_TO_STR(dispatch_info_.next_trans_global_version_),
      TS_TO_STR(dispatch_info_.current_checkpoint_));
}

}
}
