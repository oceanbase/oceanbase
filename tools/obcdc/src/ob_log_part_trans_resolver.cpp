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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_log_part_trans_resolver.h"

#include "share/ob_define.h"                // OB_SERVER_TENANT_ID
#include "share/ob_errno.h"                 // OB_SUCCESS
#include "lib/utility/ob_macro_utils.h"     // OB_UNLIKELY
#include "lib/oblog/ob_log_module.h"        // LOG_ERROR

#include "ob_log_fetch_stat_info.h"         // TransStatInfo
#include "ob_log_part_serve_info.h"         // PartServeInfo
#include "ob_log_cluster_id_filter.h"       // IObLogClusterIDFilter
#include "ob_log_config.h"                  // TCONF
#include "ob_log_instance.h"                // TCTX
#include "ob_log_dml_parser.h"              // IObLogDmlParser

using namespace oceanbase::common;
using namespace oceanbase::clog;
using namespace oceanbase::storage;
using namespace oceanbase::transaction;
namespace oceanbase
{
namespace liboblog
{
bool IObLogPartTransResolver::test_mode_on = false;
bool IObLogPartTransResolver::test_checkpoint_mode_on = false;
int64_t IObLogPartTransResolver::test_mode_ignore_redo_count = 0;

IObLogPartTransResolver::ObLogMissingInfo::ObLogMissingInfo()
{
  reset();
}

int IObLogPartTransResolver::ObLogMissingInfo::sort_and_unique_missing_log_ids()
{
  int ret = OB_SUCCESS;

  // sort missing log
  std::sort(missing_log_ids_.begin(), missing_log_ids_.end());
  LOG_INFO("[UNIQUE] [MISSING_LOG] [BEGIN]", K(missing_log_ids_));

  // unique
  int64_t cur_idx = 0;
  while (OB_SUCC(ret) && cur_idx < missing_log_ids_.count()) {
    const int64_t cur_log_id = missing_log_ids_.at(cur_idx);

    bool has_done = false;

    int64_t check_log_id_idx = cur_idx + 1;
    while(OB_SUCC(ret) && ! has_done && check_log_id_idx < missing_log_ids_.count()) {
      if (cur_log_id == missing_log_ids_.at(check_log_id_idx)) {
        if (OB_FAIL(missing_log_ids_.remove(check_log_id_idx))) {
          LOG_ERROR("missing_log_ids_ remove fail", KR(ret), K(check_log_id_idx), K(missing_log_ids_));
        } else {
          check_log_id_idx = cur_idx + 1;
        }
      } else {
        has_done = true;
      }
    } // while
    cur_idx += 1;
  }

  LOG_INFO("[UNIQUE] [MISSING_LOG] [END]", KR(ret), K(missing_log_ids_));

  return ret;
}

int IObLogPartTransResolver::ObLogMissingInfo::push_back_missing_log_id(const uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(missing_log_ids_.push_back(log_id))) {
    LOG_ERROR("missing_log_ids_ push_back fail", KR(ret), K(log_id));
  }

  return ret;
}

int IObLogPartTransResolver::ObLogMissingInfo::push_back_trans_id(const transaction::ObTransID &trans_id)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(missing_trans_ids_.push_back(trans_id))) {
    LOG_ERROR("missing_trans_ids_ push_back fail", KR(ret), K(trans_id));
  }

  return ret;
}

int IObLogPartTransResolver::ObLogMissingInfo::push_back_log_index(const int64_t log_index)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(log_index < 0)) {
    LOG_ERROR("invalid argument", K(log_index));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(log_indexs_.push_back(log_index))) {
    LOG_ERROR("log_indexs_ push_back fail", KR(ret), K(log_index));
  }

  return ret;
}

ObLogPartTransResolver::ObLogPartTransResolver(const char* pkey_str,
    TaskPool &task_pool,
    PartTransTaskMap &task_map,
    TransCommitMap &trans_commit_map,
    IObLogFetcherDispatcher &dispatcher,
    IObLogClusterIDFilter &cluster_id_filter) :
    offlined_(false),
    pkey_(),
    first_log_ts_(OB_INVALID_TIMESTAMP),
    part_trans_dispatcher_(pkey_str, task_pool, task_map, trans_commit_map, dispatcher),
    cluster_id_filter_(cluster_id_filter),
    start_global_trans_version_(OB_INVALID_TIMESTAMP)
{}

ObLogPartTransResolver::~ObLogPartTransResolver()
{
}

int ObLogPartTransResolver::init(const ObPartitionKey& pkey,
    const int64_t start_tstamp,
    const int64_t start_global_trans_version)
{
  pkey_ = pkey;
  start_global_trans_version_ = start_global_trans_version;
  first_log_ts_ = OB_INVALID_TIMESTAMP;

  return part_trans_dispatcher_.init(pkey, start_tstamp);
}

int ObLogPartTransResolver::read(const clog::ObLogEntry& log_entry,
    ObLogMissingInfo &missing_info,
    TransStatInfo &tsi,
    const PartServeInfo &serve_info,
    ObStorageLogType &log_type,
    const bool need_filter_pg_no_missing_redo_trans,
    const IObLogPartTransResolver::ObAggreLogIndexArray &log_indexs)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  bool is_log_filtered = false;
  bool is_log_aggre = false;
  bool is_barrier_log = false;
  int64_t begin_time = get_timestamp();
  const int64_t tstamp = log_entry.get_header().get_submit_timestamp();
  // Record before processing
  if (OB_INVALID_TIMESTAMP == first_log_ts_) {
    first_log_ts_ = tstamp;
  }

  missing_info.reset();
  log_type = storage::OB_LOG_UNKNOWN;

  if (OB_UNLIKELY(offlined_)) {
    LOG_ERROR("partition has been offlined", K(offlined_), K(pkey_));
    ret = OB_ERR_UNEXPECTED;
  }
  // Parsing the header of a transaction log
  else if (OB_FAIL(decode_trans_log_header_(log_entry, pos, is_log_filtered, is_log_aggre, is_barrier_log, log_type))) {
    LOG_ERROR("decode trans log header fail", KR(ret), K(log_entry), K(pos));
  } else if (! is_log_filtered) {
    // Non-transaction logs or checkpoint logs are always treated as unknown log types
    if (OB_FAIL(read_unknown_log_(log_entry))) {
      LOG_ERROR("read NON-SUBMIT type log entry fail", KR(ret), K(log_entry));
    }
  // Non-PG aggregation log
  } else if (! is_log_aggre) {
    ObLogAggreTransLog aggre_trans_log;
    ObLogEntryWrapper log_entry_wrapper(/*is_log_aggre*/false, log_entry, aggre_trans_log);
    const int64_t log_entry_index = 0;

    if (is_barrier_log) {
      // TODO
    } else if (OB_FAIL(read_log_(log_entry_wrapper, log_entry_index, begin_time, pos, missing_info, tsi, serve_info, log_type))) {
      if (OB_ITEM_NOT_SETTED != ret) {
        LOG_ERROR("read_log_ fail", KR(ret), K(log_entry_wrapper), K(log_entry_index), K(begin_time), K(pos), K(serve_info),
            K(serve_info), K(log_type));
      }
    }
  // PG aggregation log
  } else {
    ObTransIDArray missing_log_trans_id_array;
    const bool is_read_missing_log = false;

    if (OB_FAIL(parse_and_read_aggre_log_(log_entry, begin_time, missing_info, tsi, serve_info,
            is_read_missing_log, missing_log_trans_id_array, need_filter_pg_no_missing_redo_trans,
            log_indexs))) {
      if (OB_ITEM_NOT_SETTED != ret) {
        LOG_ERROR("parse_and_read_aggre_log_ fail", KR(ret), K(log_entry), K(missing_log_trans_id_array), K(begin_time), K(serve_info),
            K(is_read_missing_log), K(missing_log_trans_id_array), K(need_filter_pg_no_missing_redo_trans),
            K(log_indexs));
      }
    }
  }

  // Missing log arrays are guaranteed to be ordered and free of duplicates
  if (OB_ITEM_NOT_SETTED == ret) {
    LOG_INFO("need to read missing log", K(log_entry), K(missing_info));

    // in case of overwrite error code OB_ITEM_NOT_SETTED
    int tmp_ret = OB_SUCCESS;
    if (missing_info.get_missing_log_count() <= 0) {
      LOG_ERROR("missing log count should be greater than 0", "log_cnt", missing_info.get_missing_log_count());
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = missing_info.sort_and_unique_missing_log_ids()))) {
      LOG_ERROR("missing_info sort_and_unique_missing_log_ids fail", KR(ret), KR(tmp_ret), K(missing_info));
      ret = tmp_ret;
    }
  }

  return ret;
}

int ObLogPartTransResolver::read_log_(const liboblog::ObLogEntryWrapper &log_entry,
    const int64_t log_entry_index,
    const int64_t begin_time,
    int64_t &pos,
    ObLogMissingInfo &missing,
    TransStatInfo &tsi,
    const PartServeInfo &serve_info,
    storage::ObStorageLogType &log_type)
{
  int ret = OB_SUCCESS;
  int hit_count = 0;
  ObTransIDArray missing_log_trans_id_array;

  // Single multi-partition CHECKPOINT log, one log_entry
  if (storage::ObStorageLogTypeChecker::is_checkpoint_log(log_type)) {
    ++hit_count;
    if (OB_FAIL(read_checkpoint_log_(log_entry, pos))) {
      LOG_ERROR("read_checkpoint_log_ fail", KR(ret), K(log_type), K(log_entry), K(pos));
    }
  } else {
    tsi.decode_header_time_ += get_timestamp() - begin_time;

    LOG_DEBUG("read trans log", K_(pkey), K(log_entry), K(log_type));

    // First process the OB_LOG_MUTATOR log
    // The Mutator log only logs mutator information, and may contain sequences as follows:
    // 1. mutator_log, mutator_log ...... mutator abort log
    // 2. mutator_log, mutator_log ...... redo, prepare commit/abort
    // 3. mutator_log, mutator_log ...... sp_read, sp_commit/abort
    if (OB_SUCCESS == ret && (log_type & storage::OB_LOG_MUTATOR)) {
      ++hit_count;
      if (OB_FAIL(read_mutator_(log_entry, pos, tsi, missing_log_trans_id_array))) {
        if (OB_ENTRY_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_DEBUG("mutator log has been read multiple times", K(log_type), K(log_entry));
        } else {
          LOG_ERROR("read mutator log fail", KR(ret), K(log_type), K(log_entry));
        }
      }
    }

    // mutator abort log
    if (OB_SUCCESS == ret && (log_type & storage::OB_LOG_MUTATOR_ABORT)) {
      ++hit_count;
      ret = read_mutator_abort_(log_entry, pos);
    }

    // REDO/PREPARE/COMMIT/CLEAR logs of various types may be in one log body
    // REDO logs are processed first
    if (OB_SUCCESS == ret && (log_type & storage::OB_LOG_TRANS_REDO)) {
      ++hit_count;
      ret = read_redo_(log_entry, pos, tsi, missing_log_trans_id_array);

      // Handling duplicate redo logs
      if (OB_ENTRY_EXIST == ret) {
        bool with_prepare = (log_type & storage::OB_LOG_TRANS_PREPARE);
        if (with_prepare) {
          // When the redo log and the prepare log are in one log body, since the prepare log may be read multiple times
          // the redo log may be duplicated, allowing this to exist
          ret = OB_SUCCESS;
          LOG_INFO("redo log has been read multiple times which should have missing log",
              K(log_type), K(log_entry));
        } else {
          LOG_ERROR("redo log has been read multiple times", KR(ret), K(log_type), K(log_entry));
        }
      }
    }

    // PREPARE log
    bool with_prepare_and_served = false;
    if (OB_SUCCESS == ret && (log_type & storage::OB_LOG_TRANS_PREPARE)) {
      ++hit_count;
      bool with_redo = (log_type & storage::OB_LOG_TRANS_REDO);
      ret = read_prepare_(log_entry, log_entry_index, missing, pos, with_redo, tsi, serve_info,
          with_prepare_and_served);
    }

    // Commit log
    if (OB_SUCCESS == ret && (log_type & storage::OB_LOG_TRANS_COMMIT)) {
      ++hit_count;
      bool with_prepare = (log_type & storage::OB_LOG_TRANS_PREPARE);
      // Filter the commit log if the commit log and the prepare log are together and the prepare log is not served
      if (with_prepare && ! with_prepare_and_served) {
        // filter commit log
      } else {
        ret = read_commit_(log_entry, serve_info, pos, with_prepare, tsi);
      }
    }

    // Abort log
    if (OB_SUCCESS == ret && (log_type & storage::OB_LOG_TRANS_ABORT)) {
      ++hit_count;
      ret = read_abort_(log_entry, pos);
    }

    // Clear log
    if (OB_SUCCESS == ret && (log_type & storage::OB_LOG_TRANS_CLEAR)) {
      ++hit_count;
      // ignore
      tsi.clear_cnt_++;
      tsi.clear_size_ += log_entry.get_header().get_data_len();
    }

    // single partition REDO log
    if (OB_SUCCESS == ret && (log_type & storage::OB_LOG_SP_TRANS_REDO)) {
      ++hit_count;
      ret = read_sp_trans_redo_(log_entry, pos, tsi, missing_log_trans_id_array);
    }

    // single partition COMMIT log
    if (OB_SUCCESS == ret && (log_type & storage::OB_LOG_SP_TRANS_COMMIT)) {
      ++hit_count;
      ret = read_sp_trans_commit_(log_entry, log_entry_index, missing, pos, tsi, serve_info);
    }

    // single partition ABORT log
    if (OB_SUCCESS == ret && (log_type & storage::OB_LOG_SP_TRANS_ABORT)) {
      ++hit_count;
      ret = read_sp_trans_abort_(log_entry, pos);
    }

    if (OB_SUCCESS == ret && (storage::OB_LOG_SP_ELR_TRANS_COMMIT == log_type)) {
      ++hit_count;
      ret = read_sp_trans_commit_(log_entry, log_entry_index, missing, pos, tsi, serve_info, true/*is_sp_elr_trans*/);
    }
  }

  if (OB_SUCC(ret)) {
    if (hit_count <=0
        && OB_LOG_TRANS_STATE != log_type) {
      LOG_ERROR("Log entry is trans log type, but not handled, not supported.", K(hit_count), K(log_entry), K(pos), K(log_type));
      ret = OB_NOT_SUPPORTED;
    }
  }

  return ret;
}

int ObLogPartTransResolver::read_missing_redo(const clog::ObLogEntry &log_entry,
    const ObTransIDArray &missing_log_trans_id_array)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  bool is_trans_log = false;
  bool is_log_aggre = false;
  bool is_barrier_log = false;
  ObStorageLogType log_type = storage::OB_LOG_UNKNOWN;
  TransStatInfo tsi;

  if (OB_UNLIKELY(offlined_)) {
    LOG_ERROR("partition has been offlined", K(offlined_), K(pkey_));
    ret = OB_ERR_UNEXPECTED;
  }
  // Parsing the header of a transaction log
  else if (OB_FAIL(decode_trans_log_header_(log_entry, pos, is_trans_log, is_log_aggre, is_barrier_log, log_type))) {
    LOG_ERROR("decode trans log header fail", KR(ret), K(log_entry), K(pos));
  } else if (! is_trans_log) {
    LOG_ERROR("invalid missing redo log which is not trans log", K(log_type), K(log_entry));
    ret = OB_INVALID_ARGUMENT;
  } else if (! is_log_aggre) {
    // Non-aggregated log handling
    ObLogAggreTransLog aggre_trans_log;
    ObLogEntryWrapper log_entry_wrapper(/*is_log_aggre*/false, log_entry, aggre_trans_log);

    if (OB_FAIL(read_missing_redo_(log_entry_wrapper, missing_log_trans_id_array, pos, tsi, log_type))) {
      LOG_ERROR("read_missing_redo_ fail", KR(ret), K(log_entry_wrapper), K(missing_log_trans_id_array), K(pos),
          K(tsi), K(log_type));
    }
  } else {
    // aggregated log handling
    int64_t begin_time = 0;
    ObLogMissingInfo missing_info;
    PartServeInfo serve_info;
    const bool is_read_missing_log = true;
    const bool need_filter_pg_no_missing_redo_trans = false;
    ObAggreLogIndexArray log_indexs;

    if (OB_FAIL(parse_and_read_aggre_log_(log_entry, begin_time, missing_info, tsi, serve_info,
            is_read_missing_log, missing_log_trans_id_array, need_filter_pg_no_missing_redo_trans,
            log_indexs))) {
      LOG_ERROR("parse_and_read_aggre_log_ fail", KR(ret), K(log_entry), K(begin_time), K(serve_info),
          K(is_read_missing_log), K(missing_log_trans_id_array), K(need_filter_pg_no_missing_redo_trans),
          K(log_indexs));
    }
  }

  return ret;
}

int ObLogPartTransResolver::read_missing_redo_(const liboblog::ObLogEntryWrapper &log_entry,
    const ObTransIDArray &missing_log_trans_id_array,
    int64_t &pos,
    TransStatInfo &tsi,
    storage::ObStorageLogType &log_type)
{
  int ret = OB_SUCCESS;
  const bool is_pg_aggre_log = log_entry.is_pg_aggre_log();

  // Mutor log
  if (log_type & storage::OB_LOG_MUTATOR) {
    if (OB_FAIL(read_mutator_(log_entry, pos, tsi, missing_log_trans_id_array, true))) {
      LOG_ERROR("read mutator missing log fail", KR(ret), K(pos), K(log_entry), K(missing_log_trans_id_array));
    }
  }
  // normal REDO log
  else if (log_type & storage::OB_LOG_TRANS_REDO) {
    if (OB_FAIL(read_redo_(log_entry, pos, tsi, missing_log_trans_id_array, true))) {
      LOG_ERROR("read redo missing log fail", KR(ret), K(pos), K(log_entry), K(missing_log_trans_id_array));
    }
  }
  // Single partition transaction REDO log
  else if (log_type & storage::OB_LOG_SP_TRANS_REDO) {
    if (OB_FAIL(read_sp_trans_redo_(log_entry, pos, tsi, missing_log_trans_id_array, true))) {
      LOG_ERROR("read sp trans redo fail", KR(ret), K(pos), K(log_entry), K(missing_log_trans_id_array));
    }
  }
  // The COMMIT log of the single partition, REDO is in the COMMIT log
  // You need to deserialize the log first and read the redo inside
  else if (log_type & storage::OB_LOG_SP_TRANS_COMMIT) {
    ObSpTransCommitLog commit_log;
    int64_t after_decode_time = 0;
    bool with_redo = false;
    if (OB_FAIL(deserialize_sp_commit_and_parse_redo_(log_entry, pos, commit_log, after_decode_time,
        with_redo, missing_log_trans_id_array, true))) {
      LOG_ERROR("deserialize_sp_commit_and_parse_redo_ fail", KR(ret), K(log_entry), K(pos),
          K(commit_log), K(missing_log_trans_id_array));
    }
  }
  else {
    if (! is_pg_aggre_log) {
      // Non-PG, other logs: exceptions, requirement to be REDO logs
      LOG_ERROR("invalid missing redo log which is not REDO log", K(log_type), K(log_entry));
      ret = OB_INVALID_ARGUMENT;
    } else {
      // PG scenario read missing redo, all other log types ignored
      LOG_INFO("redo PG missing log, ignore log type which is not REDO log", K(log_type), K(log_entry));
    }
  }

  return ret;
}

int ObLogPartTransResolver::filter_pg_log_based_on_trans_id_(const ObTransIDArray &missing_log_trans_id_array,
    const ObTransID &log_trans_id,
    const bool is_pg_missing_log,
    bool &is_filter)
{
  int ret = OB_SUCCESS;
  // No filtering by default
  is_filter = false;
  bool has_find = false;

  // filter by trans_id for pg redo missing log
  if (is_pg_missing_log) {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < missing_log_trans_id_array.count() && ! has_find; ++idx) {
      const ObTransID &trans_id = missing_log_trans_id_array.at(idx);

      if (log_trans_id == trans_id) {
        is_filter = false;
        has_find = true;
      }
    } // for

    if (OB_SUCC(ret)) {
      if (! has_find) {
        is_filter = true;
      }
    }
  } else {
    // do nothing
  }

  if (is_pg_missing_log) {
    LOG_INFO("filter_pg_log_based_on_trans_id_", K(log_trans_id), K(missing_log_trans_id_array), K(is_pg_missing_log),
        K(is_filter), K(has_find));
  }

  return ret;
}

// Support for multi-threaded calls
// Issuance policy:
// DML/DDL downlisting of partitioned transaction tasks
int ObLogPartTransResolver::dispatch(volatile bool &stop_flag, int64_t &pending_task_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(offlined_)) {
    // No flush after partition deletion, handling concurrent scenarios
    LOG_INFO("partition has been offlined, need not flush", K(offlined_), K(pkey_));
    ret = OB_SUCCESS;
  } else if (OB_FAIL(part_trans_dispatcher_.dispatch_part_trans(stop_flag, pending_task_count))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("part trans dispatch fail", KR(ret), K(pkey_), K(part_trans_dispatcher_));
    }
  } else {
    // success
  }

  return ret;
}

int ObLogPartTransResolver::offline(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  LOG_INFO("[PART_TRANS_RESOLVER] offline", KPC(this));

  if (OB_UNLIKELY(offlined_)) {
    LOG_ERROR("partition has been offlined", K(offlined_), K(pkey_));
    ret = OB_ERR_UNEXPECTED;
  }
  // First clear all ready and unready tasks to ensure memory reclamation
  // This operation is mutually exclusive with the dispatch operation
  else if (OB_FAIL(part_trans_dispatcher_.clean_task(pkey_))) {
    LOG_ERROR("clean task fail", KR(ret), K(pkey_));
  }
  // dispatch offline partition task
  else if (OB_FAIL(part_trans_dispatcher_.dispatch_offline_partition_task(pkey_, stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("dispatch offline task fail", KR(ret), K(pkey_));
    }
  } else {
    offlined_ = true;
  }

  return ret;
}

int ObLogPartTransResolver::decode_trans_log_header_(const clog::ObLogEntry& log_entry,
    int64_t &pos,
    bool &is_log_filtered,
    bool &is_log_aggre,
    bool &is_barrier_log,
    ObStorageLogType &log_type)
{
  int ret = OB_SUCCESS;
  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_header().get_data_len();
  ObLogType log_entry_log_type = log_entry.get_header().get_log_type();
  const uint64_t log_id = log_entry.get_header().get_log_id();

  log_type = storage::OB_LOG_UNKNOWN;
  is_log_filtered = false;
  is_log_aggre = false;
  is_barrier_log = false;

  if (OB_ISNULL(buf) || OB_UNLIKELY(len < 0)) {
    LOG_ERROR("invalid log entry", KR(ret), K(buf), K(len), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  }
  // check is barrier log or not
  else if (is_barrier_log_(log_id, log_entry)) {
    is_log_filtered = true;
    // Non-aggregated logs
    is_log_aggre = false;
    // barrer日志
    is_barrier_log = true;
  }
  // Only parses SUBMIT type CLOG logs
  else if (clog::OB_LOG_SUBMIT != log_entry_log_type
      && clog::OB_LOG_AGGRE != log_entry_log_type) {
    is_log_filtered = false;
    log_type = storage::OB_LOG_UNKNOWN;
  } else {
    if (clog::OB_LOG_AGGRE == log_entry_log_type) {
      is_log_filtered = true;
      is_log_aggre = true;
    } else {
      if (OB_FAIL(decode_storage_log_type(log_entry, pos, log_type))) {
        LOG_ERROR("decode_storage_log_type fail", KR(ret), K(log_entry), K(pos), K(log_type));
      } else {
        // Only transaction logs or checkpoint logs are handled.
        bool is_trans_log = storage::ObStorageLogTypeChecker::is_trans_log(log_type);
        bool is_checkpoint_log = storage::ObStorageLogTypeChecker::is_checkpoint_log(log_type);
        is_log_filtered = is_trans_log || is_checkpoint_log;

        if (is_trans_log) {
          // 1. only transaction logs need to ignore the reserve field
          // 2. checkpoint logs do not have a reverse field and do not need to be ignored
          int64_t reserve = 0;
          if (OB_FAIL(serialization::decode_i64(buf, len, pos, &reserve))) {
            LOG_ERROR("decode reserve field fail", KR(ret), K(pkey_), K(buf), K(len),
                K(pos), K(log_type), K(is_trans_log), K(log_entry));
          }
        }
      }
    }
  }

  return ret;
}

// TODO
// The first log of the new partition is treated as a barrier log
bool ObLogPartTransResolver::is_barrier_log_(const uint64_t log_id,
    const clog::ObLogEntry& log_entry)
{
  bool bool_ret = false;
  UNUSED(log_entry);

  bool_ret = (1 == log_id);

  return bool_ret;
}

// Get the task, and if it doesn't exist, create a new one
int ObLogPartTransResolver::obtain_task_(const PartTransID &part_trans_id, PartTransTask*& task)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(part_trans_dispatcher_.get_task(part_trans_id, task))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;

      // Assign a new task
      if (OB_FAIL(part_trans_dispatcher_.alloc_task(part_trans_id, task))) {
        LOG_ERROR("alloc task fail", KR(ret), K(part_trans_id));
      } else {
        // success
      }
    } else {
      LOG_ERROR("get task fail", KR(ret), K(part_trans_id));
    }
  } else {
    // success
  }

  return ret;
}

int ObLogPartTransResolver::parse_and_read_aggre_log_(const clog::ObLogEntry &log_entry,
    const int64_t begin_time,
    ObLogMissingInfo &missing,
    TransStatInfo &tsi,
    const PartServeInfo &serve_info,
    const bool is_read_missing_log,
    const ObTransIDArray &missing_log_trans_id_array,
    const bool need_filter_pg_no_missing_redo_trans,
    const IObLogPartTransResolver::ObAggreLogIndexArray &log_indexs)
{
  int ret = OB_SUCCESS;

  const char *log_buf = log_entry.get_buf();
  const int64_t log_buf_len = log_entry.get_header().get_data_len();
  int32_t next_log_offset = 0;
  int64_t log_entry_index = -1;

  while (OB_SUCC(ret) && next_log_offset < log_buf_len) {
    int64_t pos = next_log_offset;
    int64_t submit_timestamp = OB_INVALID_TIMESTAMP;
    int64_t log_type_in_buf = 0;
    int64_t trans_id_inc = 0;

    if (OB_FAIL(serialization::decode_i32(log_buf, log_buf_len, pos, &next_log_offset))) {
      LOG_ERROR("serialization decode_i32 failed", KR(ret), K(log_buf_len), K(pos));
    } else if (OB_FAIL(serialization::decode_i64(log_buf, log_buf_len, pos, &submit_timestamp))) {
      LOG_ERROR("serialization::decode_i64 failed", KR(ret), K(log_buf_len), K(pos));
    } else if (OB_FAIL(serialization::decode_i64(log_buf, log_buf_len, pos, &log_type_in_buf))) {
      LOG_ERROR("serialization::decode_i64 failed", KR(ret), K(log_buf_len), K(pos));
    } else if (OB_FAIL(serialization::decode_i64(log_buf, log_buf_len, pos, &trans_id_inc))) {
      LOG_ERROR("serialization::decode_i64 failed", KR(ret), K(log_buf_len), K(pos));
    } else {
      const char *buf = log_buf + pos;
      const int64_t buf_len = next_log_offset - pos;

      ObLogAggreTransLog aggre_trans_log;
      aggre_trans_log.reset(next_log_offset,
          submit_timestamp,
          static_cast<ObStorageLogType>(log_type_in_buf),
          trans_id_inc,
          buf,
          buf_len);

      // Process each aggregated log
      ObLogEntryWrapper log_entry_wrapper(/*is_log_aggre*/true, log_entry, aggre_trans_log);
      // Inc log_entry_index
      ++log_entry_index;
      int64_t aggre_trans_log_pos = 0;
      // Default not filter
      bool is_filter = false;

      if (! is_read_missing_log) {
        if (need_filter_pg_no_missing_redo_trans) {
          if (OB_FAIL(filter_pg_no_missing_redo_trans_(log_indexs, log_entry_index, is_filter))) {
            LOG_ERROR("filter_pg_no_missing_redo_trans_ fail", KR(ret), K(log_indexs), K(log_entry_index), K(is_filter));
          }
        } else {
          is_filter = false;
        }

        if (is_filter) {
          if (OB_UNLIKELY(false == need_filter_pg_no_missing_redo_trans)) {
            LOG_ERROR("need_filter_pg_no_missing_redo_trans should be true", K(need_filter_pg_no_missing_redo_trans),
                K(is_filter), K(log_entry_index), K(log_entry_wrapper), K(log_entry), K(log_indexs));
            ret = OB_ERR_UNEXPECTED;
          } else {
            LOG_INFO("filter pg no missing redo trans", K(log_entry_index), K(log_entry_wrapper), K(log_entry),
                K(log_indexs), K(is_filter));
          }
        } else {
          if (OB_FAIL(read_log_(log_entry_wrapper, log_entry_index, begin_time, aggre_trans_log_pos, missing, tsi, serve_info,
                  aggre_trans_log.log_type_))) {
            if (OB_ITEM_NOT_SETTED != ret) {
              LOG_ERROR("read_log_ fail", KR(ret), K(log_entry_wrapper), K(log_entry_index), K(begin_time),
                  K(aggre_trans_log_pos), K(serve_info), K(serve_info), "log_type", aggre_trans_log.log_type_);
            }
          }
        }
      } else {
        // Read missing log for PG
        if (OB_FAIL(read_missing_redo_(log_entry_wrapper, missing_log_trans_id_array, aggre_trans_log_pos, tsi, aggre_trans_log.log_type_))) {
          LOG_ERROR("read_missing_redo_ fail", KR(ret), K(log_entry_wrapper), K(missing_log_trans_id_array),
              K(aggre_trans_log_pos), K(tsi), "log_type", aggre_trans_log.log_type_);
        }
      }
    }
  } // while

  return ret;
}

int ObLogPartTransResolver::filter_pg_no_missing_redo_trans_(const ObAggreLogIndexArray &log_indexs,
    const int64_t cur_log_index,
    bool &is_filter)
{
  int ret = OB_SUCCESS;
  // No filtering by default
  is_filter = false;
  bool has_find = false;

  for (int64_t idx = 0; OB_SUCC(ret) && idx < log_indexs.count() && ! has_find; ++idx) {
    const int64_t log_index = log_indexs.at(idx);

    if (cur_log_index == log_index) {
        is_filter = false;
        has_find = true;
    }
  } // for

  if (OB_SUCC(ret)) {
    if (! has_find) {
      is_filter = true;
    }
  }

  LOG_INFO("filter_pg_no_missing_redo_trans_", K(log_indexs), K(cur_log_index), K(is_filter), K(has_find));

  return ret;
}

int ObLogPartTransResolver::read_mutator_(const liboblog::ObLogEntryWrapper &log_entry,
    int64_t &pos,
    TransStatInfo &tsi,
    const ObTransIDArray &missing_log_trans_id_array,
    const bool is_missing_log)
{
  int ret = OB_SUCCESS;
  transaction::ObTransMutatorLog log;
  const uint64_t log_id = log_entry.get_header().get_log_id();
  const int64_t tstamp = log_entry.get_submit_timestamp();
  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_buf_len();
  int64_t start_time = get_timestamp();
  const bool is_pg_aggre_log = log_entry.is_pg_aggre_log();
  const uint64_t real_tenant_id = log_entry.get_header().get_partition_key().get_tenant_id();

  if (OB_ISNULL(buf) || OB_UNLIKELY(len < 0)) {
    LOG_ERROR("invalid log entry", K(buf), K(len), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  // Initialize the mutator in the mutator log, requiring that no data be copied during deserialization
  } else if (OB_FAIL(log.get_mutator().init())) {
    LOG_ERROR("mutator log init for deserialize fail", KR(ret), K(pkey_), K(log_entry));
  } else if (OB_FAIL(log.deserialize(buf, len, pos))) {
    LOG_ERROR("deserialize redo log fail", KR(ret), K(pkey_), K(buf), K(len), K(pos),
        K(log), K(log_entry));
  } else {
    int64_t after_decode_time = get_timestamp();

    if (OB_FAIL(parse_redo_log_(log_entry.get_log_offset(), log.get_mutator(), log.get_trans_id(), log_id, tstamp,
        log.get_log_no(), false, is_missing_log, is_pg_aggre_log, missing_log_trans_id_array))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_ERROR("parse redo log fail", KR(ret), K(log), K(log_id), K(tstamp), K(log_entry),
            K(is_missing_log), K(is_pg_aggre_log), K(missing_log_trans_id_array));
      }
    } else {
      int64_t end_time = get_timestamp();

      tsi.redo_size_ += len;
      tsi.redo_cnt_++;
      tsi.read_redo_decode_time_ += after_decode_time - start_time;
      tsi.read_redo_parse_time_ += end_time - after_decode_time;
      tsi.read_redo_time_ += end_time - start_time;
    }
  }

  return ret;
}

int ObLogPartTransResolver::read_redo_(const liboblog::ObLogEntryWrapper &log_entry,
    int64_t &pos,
    TransStatInfo &tsi,
    const ObTransIDArray &missing_log_trans_id_array,
    const bool is_missing_log)
{
  int ret = OB_SUCCESS;
  transaction::ObTransRedoLogHelper helper;
  transaction::ObTransRedoLog log(helper);
  const uint64_t log_id = log_entry.get_header().get_log_id();
  const int64_t tstamp = log_entry.get_submit_timestamp();
  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_buf_len();
  int64_t start_time = get_timestamp();
  const bool is_pg_aggre_log = log_entry.is_pg_aggre_log();
  const uint64_t real_tenant_id = log_entry.get_header().get_partition_key().get_tenant_id();

  if (OB_ISNULL(buf) || OB_UNLIKELY(len < 0)) {
    LOG_ERROR("invalid log entry", K(buf), K(len), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  }
  // Initialize the mutator in the redo log, requiring that no data be copied during deserialization
  else if (OB_FAIL(log.init())) {
    LOG_ERROR("redo log init for deserialize fail", KR(ret), K(pkey_), K(log_entry));
  } else if (OB_FAIL(log.deserialize(buf, len, pos))) {
    LOG_ERROR("deserialize redo log fail", KR(ret), K(pkey_), K(buf), K(len), K(pos),
        K(log), K(log_entry));
  } else {
    int64_t after_decode_time = get_timestamp();

    if (is_last_redo_with_empty_content_(log)) {
      // filter the last redo if it is empty
      // currently the last relog log of xa trans or trans of replicate table may be empty, should ignore this empty redo
      LOG_DEBUG("filter empty redo log for xa trans or trans of replicate table", "trans_id", log.get_trans_id(),
          "partition", log.get_partition(), K(log_id), "log_mutator", log.get_mutator());
    } else if (OB_FAIL(parse_redo_log_(log_entry.get_log_offset(), log.get_mutator(), log.get_trans_id(), log_id, tstamp,
        log.get_log_no(), false, is_missing_log, is_pg_aggre_log, missing_log_trans_id_array))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_ERROR("parse redo log fail", KR(ret), K(log), K(log_id), K(tstamp), K(log_entry),
            K(is_missing_log), K(is_pg_aggre_log), K(missing_log_trans_id_array));
      }
    } else {
      int64_t end_time = get_timestamp();

      tsi.redo_size_ += len;
      tsi.redo_cnt_++;
      tsi.read_redo_decode_time_ += after_decode_time - start_time;
      tsi.read_redo_parse_time_ += end_time - after_decode_time;
      tsi.read_redo_time_ += end_time - start_time;
    }
  }

  return ret;
}

int ObLogPartTransResolver::parse_redo_log_(const int32_t log_offset,
    const transaction::ObTransMutator &log_mutator,
    const transaction::ObTransID &trans_id,
    const uint64_t log_id,
    const int64_t tstamp,
    const int64_t log_no,
    const bool is_sp_trans,
    const bool parse_missing_log,
    const bool is_pg_aggre_log,
    const ObTransIDArray &missing_log_trans_id_array)
{
  int ret = OB_SUCCESS;
  const char *data = log_mutator.get_data();
  const int64_t data_len = log_mutator.get_position();
  PartTransTask *task = NULL;
  PartTransID part_trans_id(trans_id, pkey_);
  bool is_filter = false;
  const bool is_pg_missing_log = parse_missing_log && is_pg_aggre_log;

  if (OB_FAIL(filter_pg_log_based_on_trans_id_(missing_log_trans_id_array, trans_id, is_pg_missing_log,
          is_filter))) {
    LOG_ERROR("filter_pg_log_based_on_trans_id_ fail", KR(ret), K(missing_log_trans_id_array),
        K(trans_id), K(is_pg_missing_log), K(parse_missing_log), K(is_filter), K(log_id));
  } else if (is_filter) {
    LOG_INFO("filter pg trans log, which is not missing", K(trans_id), K(log_id),
        K(is_pg_missing_log), K(parse_missing_log));
  // Ignore the first few redo logs in test mode and in the case of non-missing log parsing
  } else if (OB_UNLIKELY(! parse_missing_log && test_mode_on && test_mode_ignore_redo_count > log_no)) {
    LOG_WARN("[TEST_MODE] ignore redo log", K(log_no), K(log_id), K_(pkey), K(tstamp), K(trans_id),
        K(is_sp_trans), K(parse_missing_log), K(test_mode_on), K(test_mode_ignore_redo_count));
  } else {
    // Request data valid
    if (OB_UNLIKELY(data_len <= 0)) {
      LOG_ERROR("invalid mutator data length", K(data_len), K(log_mutator));
      ret = OB_INVALID_DATA;
    }
    // Get a Partition Trans Task
    else if (OB_FAIL(part_trans_dispatcher_.get_task(part_trans_id, task))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("get task fail", KR(ret), K(part_trans_id));
      } else {
        // For reading missing log scenarios, the task must exist
        if (OB_UNLIKELY(parse_missing_log)) {
          LOG_ERROR("task does not exist while reading missing log",
              KR(ret), K(trans_id), K(pkey_), K(log_id), K(tstamp));
        } else {
          // For normal read log scenarios, when the task does not exist, dynamically create one
          ret = OB_SUCCESS;
          if (OB_FAIL(part_trans_dispatcher_.alloc_task(part_trans_id, task))) {
            LOG_ERROR("alloc task fail", KR(ret), K(part_trans_id), K(pkey_), K(parse_missing_log));
          }
        }
      }
    }

    // Push redo logs to partition task
    if (OB_SUCCESS == ret) {
      bool need_dispatch_row_data = false;
      ObLogEntryTask *redo_log_entry_task = NULL;

      if (OB_ISNULL(task)) {
        LOG_ERROR("invalid task", K(task));
        ret = OB_INVALID_ERROR;
      } else if (OB_FAIL(task->push_redo_log(pkey_, trans_id, log_no, log_id, log_offset, tstamp, data, data_len,
              need_dispatch_row_data, redo_log_entry_task))) {
        if (OB_ENTRY_EXIST == ret) {
          // redo log duplication
        }
        // Due to missing logs, the current logs cannot be pushed in and are handled in separate cases
        else if (OB_LOG_MISSING == ret) {
          LOG_WARN("[MISSING_LOG] redo log is missing, current log can not be pushed",
              K(pkey_), K(log_no), K(log_id), K(tstamp), K(trans_id), K(parse_missing_log));
          // If a missing log scenario exists during the reading of the missing log, a bug exists and an unpredictable error is returned here
          if (parse_missing_log) {
            LOG_ERROR("this should not happen while parsing missing log, unexcepted error",
                K(pkey_), K(log_no), K(log_id), K(tstamp), K(trans_id), K(parse_missing_log));
            ret = OB_ERR_UNEXPECTED;
          } else {
            // In a normal read log scenario, this log is simply ignored, and the missing logs will definitely be found when the prepare log is processed later
            LOG_WARN("[MISSING_LOG] ignore current redo log while previous redo is missing",
                K(pkey_), K(log_no), K(log_id), K(tstamp), K(trans_id), K(parse_missing_log));
            ret = OB_SUCCESS;
          }
        } else {
          LOG_ERROR("push redo log fail", KR(ret), K(pkey_), K(task), K(log_id),
              K(tstamp), K(log_no));
        }
      } else if (need_dispatch_row_data) {
        if (OB_FAIL(dispatch_log_entry_task_(redo_log_entry_task))) {
          LOG_ERROR("dispatch_log_entry_task_ fail", KR(ret), K(redo_log_entry_task));
        }
      } else {
        // do nothing
      }

      LOG_DEBUG("read redo log", K(is_sp_trans), K(parse_missing_log), K_(pkey), K(log_no),
          K(log_id), K(tstamp), K(data_len), K(trans_id), "is_aggre_log", is_pg_aggre_log);
    }
  }

  return ret;
}

// Read the PREPARE log to check if the redo log is missing
// If some redo logs are missing, return OB_ITEM_NOT_SETTED
int ObLogPartTransResolver::read_prepare_(const liboblog::ObLogEntryWrapper &log_entry,
    const int64_t log_entry_index,
    ObLogMissingInfo &missing,
    int64_t &pos,
    const bool with_redo,
    TransStatInfo &tsi,
    const PartServeInfo &serve_info,
    bool &is_prepare_log_served)
{
  int ret = OB_SUCCESS;

  const uint64_t log_id = log_entry.get_header().get_log_id();
  const int64_t tstamp = log_entry.get_submit_timestamp();
  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_buf_len();
  const common::ObVersion freeze_version = log_entry.get_header().get_freeze_version();
  int64_t start_time = get_timestamp();
  // Whether the transaction is marked as batch commit in CLOG
  bool is_batch_committed = log_entry.is_batch_committed();
  // close batch commit for test mode
  if (test_checkpoint_mode_on) {
    is_batch_committed = false;
  }
  transaction::ObTransPrepareLogHelper helper;
  transaction::ObTransPrepareLog log(helper);

  if (OB_ISNULL(buf)) {
    LOG_ERROR("invalid log buf", K(buf), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(len < 0)) {
    LOG_ERROR("invalid log len", K(len), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(log.deserialize(buf, len, pos))) {
    LOG_ERROR("deserialize prepare log fail", KR(ret), K(pkey_), K(buf), K(len), K(pos));
  } else {
    int64_t after_decode_time = get_timestamp();
    BatchCommitTransInfo batch_commit_trans_info;
    batch_commit_trans_info.init_for_prepare_log(log.is_batch_commit_trans(), is_batch_committed);
    const transaction::ObElrTransInfoArray &elt_trans_info_array = log.get_prev_trans_arr();

    if (OB_FAIL(parse_prepare_log_(log.get_trans_id(),
        log.get_redo_log_ids(),
        with_redo,
        log.get_cluster_id(),
        freeze_version,
        log_id,
        tstamp,
        missing,
        log_entry_index,
        false,
        log.get_app_trace_id_str(),
        log.get_app_trace_info(),
        serve_info,
        is_prepare_log_served,
        batch_commit_trans_info,
        elt_trans_info_array,
        log.get_checkpoint(),
        log.get_commit_version(),
        &log.get_partition_log_info_arr()))) {
      if (OB_ITEM_NOT_SETTED == ret) {
        // missing log
      } else {
        LOG_ERROR("parse prepare log fail", KR(ret), K(log), K(with_redo), K(log_entry),
            K(serve_info), K(is_batch_committed));
      }
    } else {
      int64_t end_time = get_timestamp();
      int64_t decode_time = after_decode_time - start_time;
      int64_t parse_time = end_time - after_decode_time;

      tsi.prepare_cnt_++;
      tsi.prepare_size_ += len;
      tsi.prepare_with_redo_cnt_ += with_redo ? 1 : 0;
      tsi.read_prepare_time_ += end_time - start_time;
      tsi.read_prepare_decode_time_ += decode_time;
      tsi.read_prepare_parse_time_ += parse_time;
    }
  }

  return ret;
}

int ObLogPartTransResolver::check_part_trans_served_(const int64_t trans_log_tstamp,
    const uint64_t log_id,
    const uint64_t cluster_id,
    const PartServeInfo &serve_info,
    bool &is_served)
{
  int ret = OB_SUCCESS;
  bool is_trans_log_served = serve_info.is_served(trans_log_tstamp);
  bool is_cluster_id_served = false;

  if (OB_FAIL(cluster_id_filter_.check_is_served(cluster_id, is_cluster_id_served))) {
    LOG_ERROR("check cluster id served fail", KR(ret), K(cluster_id));
  } else {
    is_served = is_trans_log_served && is_cluster_id_served;

    // The info log is only output when the prepare log is not served, assuming there are not many of these logs, otherwise the DEBUG log is output
    if (! is_trans_log_served) {
      LOG_INFO("[STAT] [FETCHER] [PART_TRANS_NOT_SERVE]", K(is_trans_log_served),
          K(is_cluster_id_served), K(log_id),
          "trans_log_tstamp", TS_TO_STR(trans_log_tstamp),
          K(cluster_id), K_(pkey), K(serve_info));
    } else if (! is_cluster_id_served) {
      LOG_DEBUG("[STAT] [FETCHER] [PART_TRANS_NOT_SERVE]", K(is_trans_log_served),
          K(is_cluster_id_served), K(log_id),
          "trans_log_tstamp", TS_TO_STR(trans_log_tstamp),
          K(cluster_id), K_(pkey), K(serve_info));
    }
  }
  return ret;
}

// DDL partitions can be cleaned up directly
// DML needs to mark the PartTransTask as unserved
// and ensure that parse_commit_log_ is processed correctly PartTransTask::is_served_state
int ObLogPartTransResolver::handle_when_part_trans_not_served_(const PartTransID &part_trans_id,
    const uint64_t log_id,
    const int64_t tstamp)
{
  int ret = OB_SUCCESS;
  const bool is_ddl_part = is_ddl_partition(pkey_);

  if (OB_FAIL(part_trans_dispatcher_.remove_task(is_ddl_part, part_trans_id))) {
    LOG_ERROR("remove task from part trans dispatcher fail", KR(ret), K(is_ddl_part), K(part_trans_id),
        K(log_id), K(tstamp));
  }

  return ret;
}

// is_batch_commit_trans: whether the transaction is a batch commit optimization, i.e. the prepare log contains commit version information, batch commit
//
// trans_is_batch_committed: whether the transaction was batch-committed successfully
// observer will mark committed in ilog for transactions that have been batch committed, liboblog will record in CLOG via RPC whether
// this transaction has been batch committed, optimising the transaction process and avoiding a backlog of transactions
//
// Note: the observer does not guarantee that each transaction's prepare log has a batch commit marker, which can be lost due to master cutting or log loss
int ObLogPartTransResolver::parse_prepare_log_(const transaction::ObTransID &trans_id,
    const transaction::ObRedoLogIdArray &all_redos,
    const bool with_redo,
    const uint64_t cluster_id,
    const common::ObVersion freeze_version,
    const uint64_t log_id,
    const int64_t tstamp,
    ObLogMissingInfo &missing_info,
    const int64_t log_entry_index,
    const bool is_sp_trans,
    const ObString &trace_id,
    const ObString &trace_info,
    const PartServeInfo &serve_info,
    bool &is_prepare_log_served,
    const BatchCommitTransInfo &batch_commit_trans_info,
    const transaction::ObElrTransInfoArray &elt_trans_info_array,
    const int64_t checkpoint,
    const int64_t commit_version,
    const transaction::PartitionLogInfoArray *prepare_log_info)
{
  int ret = OB_SUCCESS;
  PartTransTask *task = NULL;
  PartTransID part_trans_id(trans_id, pkey_);

  is_prepare_log_served = false;

  // Check if partitioned transaction is serviced
  if (OB_FAIL(check_part_trans_served_(tstamp, log_id, cluster_id, serve_info, is_prepare_log_served))) {
    LOG_ERROR("check_part_trans_served_ fail", KR(ret), K(tstamp), K(cluster_id), K(serve_info));
  } else if (! is_prepare_log_served) {
    // If a partitioned transaction is not in service, the information already available for that partitioned transaction is deleted directly
    if (OB_FAIL(handle_when_part_trans_not_served_(part_trans_id, log_id, tstamp))) {
      LOG_ERROR("handle_when_part_trans_not_served_ fail", KR(ret), K(part_trans_id), K(log_id),
          K(tstamp));
    }
  } else if (OB_FAIL(obtain_task_(part_trans_id, task))) {
    LOG_ERROR("obtain task fail", KR(ret), K(pkey_), K(part_trans_id));
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid task", K(task));
    ret = OB_ERR_UNEXPECTED;
  } else {
    const SortedRedoLogList &sorted_redos = task->get_sorted_redo_list();

    // Verifying the integrity of REDO logs
    if (OB_FAIL(check_redo_log_list_(all_redos, log_id, sorted_redos, with_redo, missing_info))) {
      LOG_ERROR("check_redo_log_list_ fail", KR(ret), K(all_redos), K(sorted_redos), K(with_redo),
          K(missing_info));
    } else {
      bool is_redo_log_complete = (missing_info.get_missing_log_count() <= 0);

      if (! is_redo_log_complete) {
        // Record the trans_id corresponding to the missing redo log
        if (OB_FAIL(missing_info.push_back_trans_id(trans_id))) {
          LOG_ERROR("missing_info push_back_trans_id fail", KR(ret), K(trans_id));
        } else if (OB_FAIL(missing_info.push_back_log_index(log_entry_index))) {
          LOG_ERROR("missing_info push_back_log_index fail", KR(ret), K(log_entry_index));
        } else {
          ret = OB_ITEM_NOT_SETTED;
        }
        // Missing redo log scenario
        LOG_INFO("[MISSING_LOG] partition detect missing log", K_(pkey),
            "prepare_log_id", log_id, "missing_count", missing_info.get_missing_log_count(),
            K(missing_info), K(trans_id), K(tstamp), K(with_redo));
      } else {
        // Prepare logs are only set when REDO logs are complete
        if (OB_FAIL(prepare_normal_trans_task_(*task, tstamp, trans_id, log_id, cluster_id,
            freeze_version, trace_id, trace_info, batch_commit_trans_info, elt_trans_info_array,
            commit_version, checkpoint, prepare_log_info))) {
          LOG_ERROR("prepare_normal_trans_task_ fail", KR(ret), KPC(task), K(tstamp), K(trans_id),
              K(log_id), K(cluster_id), K(freeze_version), K(trace_id), K(trace_info),
              K(batch_commit_trans_info), K(elt_trans_info_array), K(commit_version),
              K(checkpoint), KPC(prepare_log_info));
        } else {
          LOG_DEBUG("read prepare log", K(is_sp_trans), K_(pkey), K(log_id), K(tstamp),
              K(cluster_id), K(freeze_version), K(trans_id), K(trace_id),
              K(batch_commit_trans_info), K(elt_trans_info_array), K(commit_version),
              K(checkpoint), KPC(prepare_log_info));
        }
      }
    }
  }

  return ret;
}

/*
 There are two bugs in transactions, both of which are transaction logging exceptions that when encountered will cause liboblog to check for incorrect status and exit abnormally:
 The first problem: a single transaction may log prepare twice on a partition, and the log sequence is not as expected
 Second problem: a single transaction may log prepare+abort again on a partition after commit [because of a statement rollback scenario, the transaction state machine is incorrect].

 The server needs to fix this problem and liboblog needs to have the means to support exception log recovery:
 The more difficult part of the problem is that when a second prepare log is encountered, it cannot be simply filtered because it is impossible to distinguish between two prepares or a prepare+abort after a commit, using the following ideas.
 (1) support configuration items to filter prepare logs, encounter support for filtering redundant prepare
 (2) in order to distinguish between the above two cases, if the transaction has been committed off, again abort, abort will be filtered out to avoid modifying the state and losing data

  For commit + prepare + abort logs, the following is a demonstration of the correctness of the processing
  FetchStream will only dispatch a task after each Fetch Result is processed
  1. when commit + prepare + abort are processed in the same Fetch result, after filtering out prepare, the fetch partition transaction is in the committed state, and abort is filtered at this point
  2. When commit is processed first and the partitioned transaction task has been dispatched successfully, the next processing of dirty data [prepare+abort] will end up aborting
  3. When commit is processed first and the partitioned transaction task is not sent successfully, the next processing of dirty data can be filtered out
*/
int ObLogPartTransResolver::prepare_normal_trans_task_(PartTransTask &task,
    const int64_t tstamp,
    const transaction::ObTransID &trans_id,
    const uint64_t log_id,
    const uint64_t cluster_id,
    const common::ObVersion freeze_version,
    const common::ObString &trace_id,
    const common::ObString &trace_info,
    const BatchCommitTransInfo &batch_commit_trans_info,
    const transaction::ObElrTransInfoArray &elt_trans_info_array,
    const int64_t commit_version,
    const int64_t checkpoint,
    const transaction::PartitionLogInfoArray *prepare_log_info)
{
  int ret = OB_SUCCESS;
  const bool skip_abnormal_trans_log = (TCONF.skip_abnormal_trans_log != 0);
  // default need handle
  bool need_handle_prepare_task = true;

  if (skip_abnormal_trans_log) {
    // Already advanced via the prepare log, filter the second prepare log at this point
    if (task.is_dml_trans() || task.is_ddl_trans()) {
      // No handling required
      need_handle_prepare_task = false;
      LOG_ERROR("skip abnormal prepare log", K(task), K(trans_id), K(log_id), K(tstamp));
    }
  }

  if (! need_handle_prepare_task) {
    // do nothing
  } else if (OB_FAIL(task.prepare(pkey_, tstamp, trans_id, log_id, cluster_id, freeze_version, trace_id, trace_info,
          elt_trans_info_array))) {
    LOG_ERROR("prepare normal trans fail", KR(ret), K(pkey_), K(trans_id), K(log_id), K(tstamp),
        K(cluster_id), K(freeze_version), K(task), K(trace_id), K(trace_info), K(elt_trans_info_array));
    // Update if checkpoint is valid, push partition level checkpoint
  } else if (OB_INVALID_VERSION != checkpoint && OB_FAIL(part_trans_dispatcher_.update_checkpoint(checkpoint))) {
    LOG_ERROR("part_trans_dispatcher update checkpoint fail", KR(ret), K(pkey_), K(trans_id), K(log_id),
        K(tstamp), K(cluster_id), K(freeze_version), K(task), K(trace_id),
        K(batch_commit_trans_info), K(commit_version), K(checkpoint),
        KPC(prepare_log_info));
  } else {
    // 1. batch commit transactions
    // 2. early unlock scenario, no precommit here for single machine single partition transaction [no prepart_log_info], precommit inside parse_commit_log_
    if (batch_commit_trans_info.is_batch_commit_trans_) {
      if (OB_ISNULL(prepare_log_info)) {
        LOG_ERROR("prepare_log_info is null", KPC(prepare_log_info));
        ret = OB_ERR_UNEXPECTED;
      } else {
        // Whether it is pre-committed or not, if the transaction is not committed in bulk
        bool is_ready_to_commit = (! batch_commit_trans_info.trans_is_batch_committed_);

        // This is not a commit log carrying a prepare log scenario, but a prepare log triggering a commit scenario by itself,
        // and there is no scenario where the number of participants is uncertain
        bool commit_log_with_prepare = false;

        // Execute the commit action
        if (OB_FAIL(commit_task_(task, commit_version, *prepare_log_info,
                log_id, tstamp, trans_id, commit_log_with_prepare, is_ready_to_commit))) {
          LOG_ERROR("commit task fail where prepare task", KR(ret), K(pkey_), K(task),
              K(commit_version), KPC(prepare_log_info), K(log_id), K(tstamp), K(trans_id),
              K(commit_log_with_prepare), K(is_ready_to_commit));
        }
      }
    }

    if (OB_SUCC(ret)) {
      // Preparing tasks to be placed in the dispatcher
      if (OB_FAIL(part_trans_dispatcher_.prepare_task(task))) {
        LOG_ERROR("dispatcher prepare task fail", KR(ret), K(task));
      } else {
        // succ
      }
    }
  }

  return ret;
}

int ObLogPartTransResolver::dispatch_log_entry_task_(ObLogEntryTask *log_entry_task)
{
  int ret = OB_SUCCESS;
  IObLogDmlParser *dml_parser = TCTX.dml_parser_;
// TODO
  bool stop_flag = false;

  if (OB_ISNULL(log_entry_task)) {
    LOG_ERROR("log_entry_task is NULL", K(log_entry_task));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(dml_parser)) {
    LOG_ERROR("dml_parser is NULL", K(dml_parser));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // succ
    RETRY_FUNC(stop_flag, *dml_parser, push, *log_entry_task, DATA_OP_TIMEOUT);
  }

  return ret;
}

/// Check that the redo log list is complete
/// Note that.
/// 1. when the prepare log shares a log with the last redo, all_redos does not contain the last redo log
/// 2. sorted_redos is a list of existing redo logs, sorted from smallest to largest, which ensures that
/// in the case of missing redo logs, the missing redo log ID must be smaller than the existing redo log ID because the partition logs are read in order
///
/// @param all_redos              all redo log IDs, not including the last redo log ID if prepare_with_redo is true
/// @param prepare_log_id         prepare log ID
/// @param sorted_redos           list of currently fetched redo logs, sorted from smallest to largest
/// @param prepare_with_redo      whether the prepare log carries the last redo log
/// @param missing                returns the missing logs
int ObLogPartTransResolver::check_redo_log_list_(
    const transaction::ObRedoLogIdArray& all_redos,
    const uint64_t prepare_log_id,
    const SortedRedoLogList &sorted_redos,
    const bool prepare_with_redo,
    ObLogMissingInfo &missing_info)
{
  int ret = OB_SUCCESS;
  bool has_missing = false;

  missing_info.reset();

  if (prepare_with_redo) {
    has_missing = (sorted_redos.log_num_ < (all_redos.count() + 1));
  } else {
    has_missing = (sorted_redos.log_num_ < all_redos.count());
  }

  // Most scenarios will not have misses, so put the time-consuming operations in the scenarios with misses
  if (has_missing) {
    // For scenarios where the last REDO and PREPARE are together:
    // 1. For non-LOB scenarios, the last REDO must exist
    // 2. For LOB scenarios, the last REDO may be half of the LOB data and it may be ignored
    //
    // So, when looking for a missing log, consider the last REDO
    ObRedoLogIdArray actual_all_redos(all_redos);
    if (prepare_with_redo) {
      if (OB_FAIL(actual_all_redos.push_back(prepare_log_id))) {
        LOG_ERROR("push prepare_log_id into all redos fail", KR(ret), K(actual_all_redos),
            K(prepare_log_id));
      }
    }

    if (OB_SUCCESS == ret) {
      // Iterate through actual_all_redos, all redo logs with a smaller ID than the first redo log in sorted_redos are the missing logs
      // Assume here that actual_all_redos is not sorted, so traverse all logs
      for (int64_t idx = 0; OB_SUCCESS == ret && idx < actual_all_redos.count(); idx++) {
        uint64_t curr_id = static_cast<uint64_t>(actual_all_redos.at(idx));

        if (OB_ISNULL(sorted_redos.head_) || curr_id < sorted_redos.head_->start_log_id_) {
          if (OB_FAIL(missing_info.push_back_missing_log_id(curr_id))) {
            LOG_ERROR("push log id into missing fail", KR(ret), K(curr_id), K(missing_info));
          }
        }
      }
    }
    LOG_DEBUG("check_redo_log_list_", K(sorted_redos), K(all_redos), K(missing_info),
        K(sorted_redos.head_), K(sorted_redos.tail_),
        KPC(sorted_redos.head_), KPC(sorted_redos.tail_));
  }

  return ret;
}

int ObLogPartTransResolver::read_commit_(const liboblog::ObLogEntryWrapper &log_entry,
    const PartServeInfo &serve_info,
    int64_t &pos,
    bool with_prepare,
    TransStatInfo &tsi)
{
  int ret = OB_SUCCESS;

  const uint64_t log_id = log_entry.get_header().get_log_id();
  const int64_t tstamp = log_entry.get_submit_timestamp();
  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_buf_len();
  int64_t start_time = get_timestamp();

  PartitionLogInfoArray partition_log_info_arr;
  transaction::ObTransCommitLog log(partition_log_info_arr);
  if (OB_ISNULL(buf)) {
    LOG_ERROR("invalid log buf", K(buf), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(len < 0)) {
    LOG_ERROR("invalid log len", K(len), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(log.deserialize(buf, len, pos))) {
    LOG_ERROR("deserialize commit log fail", KR(ret), K(pkey_), K(buf), K(len), K(pos));
  } else {
    int64_t after_decode_time = get_timestamp();

    if (OB_FAIL(parse_commit_log_(false/*is_ready_to_commit*/,
        with_prepare,
        log.get_partition_log_info_array(),
        log.get_trans_id(),
        log.get_global_trans_version(),
        log_id,
        tstamp,
        log.get_cluster_id(),
        false,
        serve_info))) {
      LOG_ERROR("parse commit log fail", KR(ret), K(with_prepare), K(log), K(log_id), K(tstamp),
          K(serve_info));
    } else {
      int64_t end_time = get_timestamp();
      int64_t read_commit_time = end_time - start_time;

      tsi.commit_cnt_++;
      tsi.commit_size_ += len;
      tsi.commit_with_prepare_cnt_ += with_prepare ? 1 : 0;
      tsi.read_commit_time_ += read_commit_time;
      tsi.read_commit_decode_time_ += after_decode_time - start_time;
      tsi.read_commit_parse_time_ += end_time - after_decode_time;
    }
  }

  return ret;
}

// commit_log_with_prepare: Whether the commit log contains a prepare, which determines whether to build an array of participants
// is_ready_to_commit: whether to commit or not
int ObLogPartTransResolver::commit_task_(PartTransTask &task,
    const int64_t global_trans_version,
    const transaction::PartitionLogInfoArray &prepare_log_info,
    const uint64_t log_id,
    const int64_t tstamp,
    const transaction::ObTransID &trans_id,
    const bool commit_log_with_prepare,
    const bool is_ready_to_commit)
{
  int ret = OB_SUCCESS;
  const transaction::PartitionLogInfoArray *pid_arr = NULL;
  transaction::PartitionLogInfoArray with_prepare_pid_arr;

  // If the commit log contains prepare, then it is a single transaction with only itself as a participant
  // Since the participants do not know the prepare log ID when they write the log, the participants array is empty and needs to be constructed manually
  if (commit_log_with_prepare) {
    transaction::ObPartitionLogInfo pid(pkey_, log_id, tstamp);

    if (OB_UNLIKELY(prepare_log_info.count() > 0)) {
      LOG_ERROR("invalid prepare log info which should be empty. "
          "because prepare and commit log is combined",
          K(pkey_), K(log_id), K(commit_log_with_prepare), K(prepare_log_info),
          K(trans_id), K(tstamp));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(with_prepare_pid_arr.push_back(pid))) {
      LOG_ERROR("push back partition log info fail", KR(ret), K(pid), K(with_prepare_pid_arr));
    } else {
      pid_arr = &with_prepare_pid_arr;
    }
  } else {
    pid_arr = &(prepare_log_info);
  }

  if (OB_SUCCESS == ret) {
    TransCommitInfo trans_commit_info(log_id, tstamp);

    // Set to a partitioned transaction context
    if (OB_ISNULL(pid_arr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("PartitionLogInfoArray for commit_task should not be null", KR(ret), K_(pkey), K(task));
    } else if (OB_FAIL(task.commit(global_trans_version, *pid_arr, is_ready_to_commit, first_log_ts_,
            trans_id, trans_commit_info, part_trans_dispatcher_))) {
      LOG_ERROR("commit normal trans fail", KR(ret), K(pkey_), K(task), K(log_id),
          K(tstamp), K(global_trans_version), KPC(pid_arr), K(is_ready_to_commit),
          K(first_log_ts_), K(trans_id), K(trans_commit_info));
    } else { /* success */ }
  }

  return ret;
}

int ObLogPartTransResolver::parse_commit_log_(const bool is_ready_to_commit,
    const bool with_prepare,
    const transaction::PartitionLogInfoArray &prepare_log_info,
    const transaction::ObTransID &trans_id,
    const int64_t global_trans_version,
    const uint64_t log_id,
    const int64_t tstamp,
    const uint64_t cluster_id,
    const bool is_sp_trans,
    const PartServeInfo &serve_info)
{
  int ret = OB_SUCCESS;
  PartTransTask *task = NULL;
  PartTransID part_trans_id(trans_id, pkey_);

  if (OB_FAIL(part_trans_dispatcher_.get_task(part_trans_id, task))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // The transaction context does not exist, in two cases.
      // 1. it can happen when the commit log is a separate log
      // i.e. this log is after the liboblog start point, but the prepare log is before the start point
      // 2. prepare log processing determines that it is not served, and removes the partition transaction task, see: handle_when_part_trans_not_served_
      if (with_prepare) {
        LOG_ERROR("trans task does not exist", KR(ret), K(part_trans_id), K(log_id), K(pkey_),
            K(with_prepare));
      } else {
        // Normal, skip this log
        // 1. In backup and recovery mode: check the log
        // 2. start_global_trans_version is not set, no need check, skip this log
        if (OB_INVALID_TIMESTAMP == start_global_trans_version_) {
          // do nothing
          ret = OB_SUCCESS;
        } else {
          // check the trans
          bool is_served = false;
          if (OB_FAIL(check_part_trans_served_(tstamp, log_id, cluster_id, serve_info, is_served))) {
            LOG_ERROR("check_part_trans_served_ fail", KR(ret), K(tstamp), K(cluster_id), K(serve_info),
                K(is_served));
          } else if (! is_served) {
            // No service, no processing required
            ret = OB_SUCCESS;
          } else {
            // served -> may not really served, because it is determined by the timestamp of the commit log, here need to check
            // If the current commmit log global trans version is greater than the start global trans ersion, an error should be reported and the larger timestamp should be rolled back
            if (global_trans_version > start_global_trans_version_) {
              LOG_ERROR("commit log global_trans_version is greater then start_global_trans_version, "
                  "maybe have lost trans",
                  K(is_sp_trans), K_(pkey), K(log_id), K(tstamp), K(trans_id),
                  K(with_prepare), K(prepare_log_info), K(global_trans_version),
                  K(start_global_trans_version_));
              ret = OB_LOG_MISSING;
            } else {
              // Checked successfully, can ignore commit log
              ret = OB_SUCCESS;
            }
          }
        }

        if (OB_SUCC(ret)) {
          LOG_INFO("ignore commit log which is single trans log after start log "
              "whose prepare log id is little than start log id",
              K_(pkey), K(log_id), K(tstamp), K(trans_id), K(with_prepare), K(prepare_log_info),
              K(is_sp_trans));
        }
      }
    } else {
      LOG_ERROR("get task from part trans dispatcher fail", KR(ret), K(pkey_), K(trans_id));
    }
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid task", KR(ret), K(trans_id), K(task));
    ret = OB_ERR_UNEXPECTED;
  }
  // 1. Execute the commit task action
  // 2. Early unlocking scenario where single partition transaction is pre-committed
  else if (OB_FAIL(commit_task_(*task, global_trans_version, prepare_log_info, log_id, tstamp,
      trans_id, with_prepare, is_ready_to_commit))) {
    LOG_ERROR("commit_task_ fail", KR(ret), KPC(task), K(global_trans_version), K(prepare_log_info),
        K(log_id), K(tstamp), K(trans_id), K(with_prepare), K(is_ready_to_commit));
  } else {
    // After commit, the task cannot be accessed again, as it may be concurrently flushed to downstream
    LOG_DEBUG("read commit log", K(is_sp_trans), K(is_ready_to_commit), K_(pkey), K(log_id), K(tstamp), K(trans_id),
        K(with_prepare), K(prepare_log_info), K(global_trans_version));
  }
  return ret;
}

int ObLogPartTransResolver::read_mutator_abort_(const liboblog::ObLogEntryWrapper &log_entry,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_buf_len();
  const int64_t tstamp = log_entry.get_submit_timestamp();
  const uint64_t log_id = log_entry.get_header().get_log_id();
  transaction::ObTransMutatorAbortLog log;

  if (OB_ISNULL(buf)) {
    LOG_ERROR("invalid log buf", K(buf), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(len < 0)) {
    LOG_ERROR("invalid log len", K(len), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(log.deserialize(buf, len, pos))) {
    LOG_ERROR("deserialize abort log fail", KR(ret), K(pkey_), K(buf), K(len), K(pos));
  } else {
    if (OB_FAIL(parse_abort_log_(log.get_trans_id(), log_id, tstamp))) {
      LOG_ERROR("parse abort log fail", KR(ret), K(log), K(log_id), K(tstamp));
    } else {
      // succ
    }
    LOG_DEBUG("read mutator abort log", K_(pkey), K(tstamp), K(log_id), K(log));
  }

  return ret;
}

int ObLogPartTransResolver::read_abort_(const liboblog::ObLogEntryWrapper &log_entry,
    int64_t &pos)
{
  int ret = OB_SUCCESS;

  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_buf_len();
  const int64_t tstamp = log_entry.get_submit_timestamp();
  const uint64_t log_id = log_entry.get_header().get_log_id();

  transaction::ObTransAbortLog log;
  if (OB_ISNULL(buf)) {
    LOG_ERROR("invalid log buf", K(buf), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(len < 0)) {
    LOG_ERROR("invalid log len", K(len), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(log.deserialize(buf, len, pos))) {
    LOG_ERROR("deserialize abort log fail", KR(ret), K(pkey_), K(buf), K(len), K(pos));
  } else {
    if (OB_FAIL(parse_abort_log_(log.get_trans_id(), log_id, tstamp))) {
      LOG_ERROR("parse abort log fail", KR(ret), K(log), K(log_id), K(tstamp));
    } else {
      // success
    }
    LOG_DEBUG("read abort log", K_(pkey), K(tstamp), K(log_id));
  }

  return ret;
}

int ObLogPartTransResolver::parse_abort_log_(const transaction::ObTransID &trans_id,
    const uint64_t log_id,
    const int64_t tstamp)
{
  int ret = OB_SUCCESS;
  PartTransID part_trans_id(trans_id, pkey_);
  const bool is_ddl_part = is_ddl_partition(pkey_);
  const bool skip_abnormal_trans_log = (TCONF.skip_abnormal_trans_log != 0);
  // Default requires handling
  bool need_handle_abort_log = true;

  if (skip_abnormal_trans_log) {
    PartTransTask *task = NULL;
    if (OB_FAIL(part_trans_dispatcher_.get_task(part_trans_id, task))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("get task from part trans dispatcher fail", KR(ret), K(pkey_), K(trans_id));
      }
    } else {
      if (OB_ISNULL(task)) {
        LOG_ERROR("task is NULL", K(task));
        ret = OB_ERR_UNEXPECTED;
      } else if (task->is_trans_committed()) {
        need_handle_abort_log = false;
        LOG_ERROR("task is_trans_committed, skip abnormal abort log", KPC(task), K(trans_id), K(log_id), K(tstamp));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (! need_handle_abort_log) {
    // do nothing
  }
  // Delete partitioned transaction tasks directly from dispatcher
  else if (OB_FAIL(part_trans_dispatcher_.remove_task(is_ddl_part, part_trans_id))) {
    LOG_ERROR("remove task from dispatcher fail", KR(ret), K(is_ddl_part), K(part_trans_id), K(log_id), K(tstamp),
        K(part_trans_dispatcher_));
  }

  return ret;
}

int ObLogPartTransResolver::read_sp_trans_redo_(const liboblog::ObLogEntryWrapper &log_entry,
    int64_t &pos,
    TransStatInfo &tsi,
    const ObTransIDArray &missing_log_trans_id_array,
    const bool is_missing_log)
{
  int ret = OB_SUCCESS;

  const uint64_t log_id = log_entry.get_header().get_log_id();
  const int64_t tstamp = log_entry.get_submit_timestamp();
  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_buf_len();
  int64_t start_time = get_timestamp();
  const bool is_pg_aggre_log = log_entry.is_pg_aggre_log();
  const uint64_t real_tenant_id = log_entry.get_header().get_partition_key().get_tenant_id();

  transaction::ObSpTransRedoLog log;
  if (OB_ISNULL(buf)) {
    LOG_ERROR("invalid log buf", K(buf), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(len < 0)) {
    LOG_ERROR("invalid log len", K(len), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  }
  // Initialize the mutator in the redo log, requiring that no data be copied during deserialization
  else if (OB_FAIL(log.get_mutator().init())) {
    LOG_ERROR("sp redo log init for deserialize fail", KR(ret), K(pkey_), K(log_entry));
  } else if (OB_FAIL(log.deserialize(buf, len, pos))) {
    LOG_ERROR("deserialize sp trans redo log fail", KR(ret), K(pkey_), K(buf), K(len), K(pos),
        K(log), K(log_entry));
  } else {
    int64_t after_decode_time = get_timestamp();

    if (OB_FAIL(parse_redo_log_(log_entry.get_log_offset(),
        log.get_mutator(),
        log.get_trans_id(),
        log_id,
        tstamp,
        log.get_log_no(),
        true,
        is_missing_log,
        is_pg_aggre_log,
        missing_log_trans_id_array))) {
      // Note: The OB_ENTRY_EXIST case is not handled here and must not be repeated
      LOG_ERROR("parse sp redo log fail", KR(ret), K(log), K(log_id), K(tstamp), K(log_entry),
          K(is_missing_log), K(is_pg_aggre_log), K(missing_log_trans_id_array));
    } else {
      int64_t end_time = get_timestamp();

      tsi.sp_redo_cnt_++;
      tsi.sp_redo_size_ += len;
      tsi.read_sp_redo_time_ += end_time - start_time;
      tsi.read_sp_redo_decode_time_ += after_decode_time - start_time;
      tsi.read_sp_redo_parse_time_ += end_time - after_decode_time;
    }
  }

  return ret;
}

int ObLogPartTransResolver::deserialize_sp_commit_and_parse_redo_(const liboblog::ObLogEntryWrapper &log_entry,
    int64_t &pos,
    transaction::ObSpTransCommitLog &commit_log,
    int64_t &after_decode_time,
    bool &with_redo,
    const ObTransIDArray &missing_log_trans_id_array,
    const bool is_missing_log)
{
  int ret = OB_SUCCESS;
  const uint64_t log_id = log_entry.get_header().get_log_id();
  const int64_t tstamp = log_entry.get_submit_timestamp();
  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_buf_len();
  const bool is_pg_aggre_log = log_entry.is_pg_aggre_log();
  const uint64_t real_tenant_id = log_entry.get_header().get_partition_key().get_tenant_id();

  if (OB_ISNULL(buf)) {
    LOG_ERROR("invalid log buf", K(buf), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(len < 0)) {
    LOG_ERROR("invalid log len", K(len), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  }
  // Initialize the mutator in the redo log, requiring that no data be copied during deserialization
  else if (OB_FAIL(commit_log.get_mutator().init())) {
    LOG_ERROR("sp commit log init for deserialize fail", KR(ret), K(pkey_), K(log_entry));
  } else if (OB_FAIL(commit_log.deserialize(buf, len, pos))) {
    LOG_ERROR("deserialize sp trans commit log fail", KR(ret), K(pkey_), K(buf), K(len), K(pos),
        K(commit_log), K(log_entry));
  } else {
    after_decode_time = get_timestamp();
    const transaction::ObTransID &trans_id = commit_log.get_trans_id();

    // If the redo log is not empty, parse the redo log
    with_redo = (! commit_log.is_empty_redo_log());
    if (! with_redo) {
      // empty redo log
    } else {
      if (OB_FAIL(parse_redo_log_(log_entry.get_log_offset(), commit_log.get_mutator(), trans_id, log_id, tstamp,
          commit_log.get_log_no(), true, is_missing_log, is_pg_aggre_log, missing_log_trans_id_array))) {
        if (OB_ENTRY_EXIST == ret) {
          // duplicate redo logs, probably dealing with missing log scenarios
          // This error is simply ignored to facilitate subsequent processing of prepare and commit
          LOG_WARN("redo log has been read mutiple times, must have missing log",
              K(commit_log), K(log_entry), K(is_missing_log), K(is_pg_aggre_log), K(missing_log_trans_id_array));
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("parse sp redo log in commit log fail", KR(ret), K(commit_log),
              K(commit_log), K(log_entry));
        }
      }
    }
  }
  return ret;
}

int ObLogPartTransResolver::read_sp_trans_commit_(const liboblog::ObLogEntryWrapper &log_entry,
    const int64_t log_entry_index,
    ObLogMissingInfo &missing,
    int64_t &pos,
    TransStatInfo &tsi,
    const PartServeInfo &serve_info,
    const bool is_sp_elr_trans)
{
  int ret = OB_SUCCESS;
  const uint64_t log_id = log_entry.get_header().get_log_id();
  const int64_t tstamp = log_entry.get_submit_timestamp();
  const int64_t len = log_entry.get_buf_len();
  const common::ObVersion freeze_version = log_entry.get_header().get_freeze_version();
  int64_t start_time = get_timestamp();
  int64_t after_decode_time = 0;
  bool with_redo = false;
  ObTransIDArray missing_log_trans_id_array;

  transaction::ObSpTransCommitLog commit_log;

  if (OB_FAIL(deserialize_sp_commit_and_parse_redo_(log_entry, pos, commit_log, after_decode_time,
      with_redo, missing_log_trans_id_array))) {
    LOG_ERROR("deserialize_sp_commit_and_parse_redo_ fail", KR(ret), K(log_entry), K(pos),
        K(commit_log), K(missing_log_trans_id_array));
  } else {
    const transaction::ObTransID &trans_id = commit_log.get_trans_id();
    const transaction::ObElrTransInfoArray &elt_trans_info_array = commit_log.get_prev_trans_arr();
    bool is_prepare_log_served = false;
    // Early unlocking scenario:  single partition transaction on single machine, essentially requires parsing of sp_commit logs, but similar to batch commit transaction, is pre-committed
    BatchCommitTransInfo batch_commit_trans_info;
    batch_commit_trans_info.init_for_sp_elr_trans(is_sp_elr_trans);

    // prepare part trans
    if (OB_SUCCESS == ret) {
      if (OB_FAIL(parse_prepare_log_(trans_id,
          commit_log.get_redo_log_ids(),
          with_redo,
          commit_log.get_cluster_id(),
          freeze_version,
          log_id,
          tstamp,
          missing,
          log_entry_index,
          true,
          commit_log.get_app_trace_id_str(),
          commit_log.get_app_trace_info(),
          serve_info,
          is_prepare_log_served,
          batch_commit_trans_info,
          elt_trans_info_array,
          commit_log.get_checkpoint()))) {
        if (OB_ITEM_NOT_SETTED == ret) {
          // missing log
        } else {
          LOG_ERROR("parse sp prepare log fail", KR(ret), K(commit_log), K(log_id), K(tstamp),
              K(missing), K(batch_commit_trans_info));
        }
      }
    }

    // commit partitioned transactions
    // Parsing the commit log if only the partitioned transaction is served
    if (OB_SUCCESS == ret && is_prepare_log_served) {
      transaction::PartitionLogInfoArray prepare_log_info;
      bool with_prepare = true;
      if (OB_FAIL(parse_commit_log_(is_sp_elr_trans/*is_ready_to_commit*/,
          with_prepare,
          prepare_log_info,
          trans_id,
          tstamp,
          log_id,
          tstamp,
          commit_log.get_cluster_id(),
          true,
          serve_info))) {
        LOG_ERROR("parse sp commit log fail", KR(ret), K(commit_log), K(log_id), K(tstamp), K(serve_info));
      }
    }

    int64_t end_time = get_timestamp();

    if (OB_SUCCESS == ret) {
      tsi.sp_commit_cnt_++;
      tsi.sp_commit_size_ += len;
      tsi.sp_commit_with_redo_cnt_ += with_redo ?  1 : 0;
      tsi.read_sp_redo_time_ += end_time - start_time;
      tsi.read_sp_commit_decode_time_ += after_decode_time - start_time;
      tsi.read_sp_commit_parse_time_ += end_time - after_decode_time;
    }
  }

  return ret;
}

int ObLogPartTransResolver::read_sp_trans_abort_(const liboblog::ObLogEntryWrapper &log_entry,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t tstamp = log_entry.get_submit_timestamp();
  const uint64_t log_id = log_entry.get_header().get_log_id();
  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_buf_len();
  // Default trace_id is empty
  ObString trace_id;

  transaction::ObSpTransAbortLog log;
  if (OB_ISNULL(buf)) {
    LOG_ERROR("invalid log buf", K(buf), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(len < 0)) {
    LOG_ERROR("invalid log len", K(len), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(log.deserialize(buf, len, pos))) {
    LOG_ERROR("deserialize abort log fail", KR(ret), K(pkey_), K(buf), K(len), K(pos));
  } else {
    const transaction::ObTransID &trans_id = log.get_trans_id();

    if (OB_FAIL(parse_abort_log_(trans_id, log_id, tstamp))) {
      LOG_ERROR("parse sp abort log fail", KR(ret), K(log), K(log_id), K(tstamp));
    } else {
      // 成功
    }

    LOG_DEBUG("read sp trans abort log", K_(pkey), K(tstamp), K(log));
  }
  return ret;
}

int ObLogPartTransResolver::read_checkpoint_log_(const liboblog::ObLogEntryWrapper &log_entry,
    int64_t &pos)
{
  int ret = OB_SUCCESS;

  const uint64_t log_id = log_entry.get_header().get_log_id();
  const int64_t tstamp = log_entry.get_submit_timestamp();
  const char *buf = log_entry.get_buf();
  const int64_t len = log_entry.get_buf_len();
  const bool is_ddl_part = is_ddl_partition(pkey_);
  transaction::ObCheckpointLog log;

  if (OB_ISNULL(buf)) {
    LOG_ERROR("invalid log buf", K(buf), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(len < 0)) {
    LOG_ERROR("invalid log len", K(len), K(log_entry));
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(log.deserialize(buf, len, pos))) {
    LOG_ERROR("deserialize checkpoint log fail", KR(ret), K(pkey_), K(buf), K(len), K(pos));
  } else {
    const int64_t checkpoint = log.get_checkpoint();

    // ignoring checkpoint logs in test mode
    if (test_checkpoint_mode_on) {
      LOG_INFO("read checkpoint log, but ignore", K_(pkey), K(tstamp), K(log), K(test_checkpoint_mode_on));
    } else {
      // Advance partition checkpoint based on checkpoint logs
      if (OB_FAIL(part_trans_dispatcher_.update_checkpoint(checkpoint))) {
        LOG_ERROR("part_trans_dispatcher update checkpoint fail", KR(ret), K(pkey_), K(log), K(log_id),
            K(tstamp), K(checkpoint));
      }

      LOG_DEBUG("read checkpoint log", K_(pkey), K(is_ddl_part), K(tstamp), K(log), K(checkpoint));
    }
  }

  return ret;
}

// Converting unknown logs to heartbeats
int ObLogPartTransResolver::read_unknown_log_(const clog::ObLogEntry &log_entry)
{
  int ret = OB_SUCCESS;
  UNUSED(log_entry);
  return ret;
}

bool ObLogPartTransResolver::is_last_redo_with_empty_content_(const transaction::ObTransRedoLog &redo_log) const
{
  const int64_t length = redo_log.get_mutator().get_position();
  return 0 == length && redo_log.is_last();
}

}
}
