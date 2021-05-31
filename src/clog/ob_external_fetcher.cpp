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

#define USING_LOG_PREFIX EXTLOG

#include "ob_external_fetcher.h"
#include <string.h>
#include "lib/allocator/ob_qsync.h"
#include "lib/stat/ob_diagnose_info.h"
#include "storage/ob_partition_service.h"

#include "ob_log_engine.h"
#include "ob_partition_log_service.h"
#include "ob_external_log_service_monitor.h"
#include "ob_log_line_cache.h"
#include "ob_log_compress.h"

namespace oceanbase {
using namespace common;
using namespace clog;
using namespace storage;
using namespace obrpc;
namespace logservice {
int ObExtLogFetcher::init(
    ObLogLineCache& line_cache, ObILogEngine* log_engine, ObPartitionService* partition_service, const ObAddr& self)
{
  int ret = OB_SUCCESS;
  const uint64_t STREAM_MAP_TENANT_ID = OB_SERVER_TENANT_ID;
  const uint64_t STREAM_ALLOCATOR_TENANT_ID = OB_SERVER_TENANT_ID;
  const char* STREAM_MAP_LABEL = ObModIds::OB_EXT_STREAM_MAP;
  const char* STREAM_ALLOCATOR_LABEL = ObModIds::OB_EXT_STREAM_ALLOCATOR;
  UNUSED(STREAM_ALLOCATOR_TENANT_ID);
  if (OB_UNLIKELY(NULL == log_engine) || OB_UNLIKELY(NULL == partition_service) || OB_UNLIKELY(!self.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init error", K(ret), KP(log_engine));
  } else if (OB_FAIL(ObLogFetcherImpl::init(line_cache, log_engine))) {
    LOG_WARN("ObLogFetcherImpl init error", K(ret), KP(log_engine));
  } else if (OB_FAIL(stream_map_.init(STREAM_MAP_LABEL, STREAM_MAP_TENANT_ID))) {
    LOG_WARN("stream_map_ init error", K(ret));
  } else if (OB_FAIL(stream_allocator_.init(global_default_allocator, OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    LOG_WARN("stream_allocator_ init error", K(ret));
  } else {
    stream_allocator_.set_label(STREAM_ALLOCATOR_LABEL);
    self_ = self;
    partition_service_ = partition_service;
    CLOG_LOG(
        INFO, "ObExtLogFetcher init success", K(self_), KP(log_engine_), KP(partition_service_), K(skip_hotcache_));
  }
  if (OB_FAIL(ret)) {
    is_inited_ = false;
  }
  return ret;
}

// Generate a globally unique tag
int ObExtLogFetcher::generate_stream_seq(ObStreamSeq& stream_seq)
{
  int ret = OB_SUCCESS;
  int64_t next_ts = OB_INVALID_TIMESTAMP;
  while (true) {
    int64_t now = ObTimeUtility::current_time();
    int64_t cur_ts = ATOMIC_LOAD(&cur_ts_);
    next_ts = (now > cur_ts) ? now : (cur_ts + 1);
    if (ATOMIC_BCAS(&cur_ts_, cur_ts, next_ts)) {
      break;
    } else {
      PAUSE();
    }
  }
  stream_seq.self_ = self_;
  stream_seq.seq_ts_ = next_ts;
  return ret;
}

int ObExtLogFetcher::alloc_stream_mem(const ObStreamSeq& seq, const ObLogOpenStreamReq& req, char*& ret_buf)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  const int64_t pkey_count = req.get_params().count();
  int64_t size = 0;
  size += static_cast<int64_t>(sizeof(ObStream));
  size += static_cast<int64_t>(sizeof(ObStreamItem) * pkey_count);
  do {
    ObSpinLockGuard lock_guard(stream_allocator_lock_);
    buf = reinterpret_cast<char*>(stream_allocator_.alloc(size));
  } while (false);
  if (OB_UNLIKELY(NULL == buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc stream mem error", K(ret), K(seq), K(size));
  } else {
    ret_buf = buf;
  }
  return ret;
}

void ObExtLogFetcher::free_stream_mem(ObStream* stream)
{
  if (NULL != stream) {
    // deconstruct Stream first
    stream->~ObStream();

    ObSpinLockGuard lock_guard(stream_allocator_lock_);
    stream_allocator_.free(stream);
    stream = NULL;
  }
}

// Perform open flow operation
int ObExtLogFetcher::do_open_stream(
    const ObLogOpenStreamReq& req, const ObAddr& liboblog_addr, ObLogOpenStreamResp& resp)
{
  int ret = OB_SUCCESS;
  ObStreamSeq stream_seq;
  char* buf = NULL;
  ObStream* stream = NULL;
  if (OB_FAIL(generate_stream_seq(stream_seq))) {
    LOG_WARN("generate_stream_seq error", K(ret));
  } else if (OB_FAIL(alloc_stream_mem(stream_seq, req, buf))) {
    LOG_WARN("alloc stream mem error", K(ret), K(req), KP(buf));
  } else {
    stream = new (buf) ObStream();
    ObStream::LiboblogInstanceId liboblog_instance_id(liboblog_addr, req.get_liboblog_pid());
    if (OB_FAIL(stream->init(req.get_params(), req.get_stream_lifetime(), liboblog_instance_id))) {
      LOG_WARN("stream init error", K(ret), K(req));
    } else if (OB_FAIL(stream_map_.insert(stream_seq, stream))) {
      LOG_WARN("stream map insert error", K(ret), K(stream_seq), KP(stream));
    } else {
      LOG_TRACE("insert new stream success", K(stream_seq), KP(stream));
    }
    if (OB_FAIL(ret)) {
      free_stream_mem(stream);
      stream = NULL;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(resp.set_stream_seq(stream_seq))) {
      LOG_WARN("resp set_stream_seq error", K(ret), K(stream_seq));
    }
  }
  return ret;
}

bool ObExtLogFetcher::ExpiredStreamMarker::operator()(const obrpc::ObStreamSeq& stream_seq, ObStream* stream)
{
  if (NULL != stream) {
    stream->mark_expired();
    LOG_INFO("mark stale stream expired success", K(stream_seq), KP(stream));
  }

  return true;
}

// Delete stale streams
// liboblog will merge and split stream, then the original stream is no longer useful, and logservice is notified to
// delete
void ObExtLogFetcher::mark_stream_expired(const ObStreamSeq& stale_seq)
{
  int ret = OB_SUCCESS;
  if (stale_seq.is_valid()) {
    ExpiredStreamMarker expired_stream_marker;

    // mark stream expired
    // under lock protection, no additional guard protection is needed here
    if (OB_FAIL(stream_map_.operate(stale_seq, expired_stream_marker))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // expected
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("stream map mark stale stream error", K(ret), K(stale_seq));
      }
    }
  } else {
    // invalid stale_seq means no need wash
    // do nothing
  }
}

int ObExtLogFetcher::open_stream(const ObLogOpenStreamReq& req, const ObAddr& liboblog_addr, ObLogOpenStreamResp& resp)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("open stream req is invalid", K(ret), K(req));
  } else {
    if (OB_UNLIKELY(stream_map_.count() > ACTIVE_STREAM_COUNT_LIMIT)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("too many stream, can not open new stream", K(ret), K(req));
    } else if (OB_FAIL(do_open_stream(req, liboblog_addr, resp))) {
      LOG_WARN("do open_stream error", K(ret), K(req), K(liboblog_addr), K(resp));
    }

    // mark expired stream
    mark_stream_expired(req.get_stale_stream());
  }
  resp.set_debug_err(ret);
  if (OB_SUCC(ret)) {
    resp.set_err(OB_SUCCESS);
    LOG_INFO("ObExtLogFetcher open_stream success", K(liboblog_addr), K(req), K(resp));
  } else {
    // OB_SIZE_OVERFLOW refuse open stream, this error code is currently not used,
    // Will be used after adding performance monitoring later
    // (return when the server's service capacity is insufficient).
    resp.set_err(OB_ERR_SYS);
    LOG_WARN("ObExtLogFetcher open_stream error", K(liboblog_addr), K(req), K(resp));
  }
  return ret;
}

bool ObExtLogFetcher::ExpiredStreamPicker::operator()(const ObStreamSeq& stream_seq, ObStream* stream)
{
  int ret = OB_SUCCESS;
  bool need_erase = false;

  if (NULL != stream && stream->is_expired()) {
    SeqStreamPair pair;
    pair.seq_ = stream_seq;
    pair.stream_ = stream;
    if (OB_FAIL(retired_arr_.push_back(pair))) {
      LOG_WARN("push back stream into array fail", K(ret), K(pair));
    } else {
      LOG_INFO("catch an expired stream", K(pair));
      // Only insert successfully can erase.
      need_erase = true;
    }
  }

  return need_erase;
}

int ObExtLogFetcher::wash()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    // delete expired streams directly
    ExpiredStreamPicker expired_stream_picker;
    if (OB_FAIL(stream_map_.remove_if(expired_stream_picker))) {
      LOG_WARN("stream_map apply expired_stream_picker error", K(ret));
    } else {
      RetiredStreamArray& retired_arr = expired_stream_picker.get_retired_arr();
      const int64_t count = retired_arr.count();
      if (0 == count) {
        LOG_TRACE("[FETCH_LOG_STREAM] Wash Stream: no expired stream picked");
      } else {
        // wait for QSync before reclaiming memory
        WaitQuiescent(get_stream_qs());

        SeqStreamPair pair;
        for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
          pair = retired_arr[i];
          if (OB_LIKELY(NULL != pair.stream_)) {
            free_stream_mem(pair.stream_);
            pair.stream_ = NULL;
          }
        }  // end for
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("[FETCH_LOG_STREAM] Wash Stream: wash expired stream success", K(count), K(retired_arr));
      }
    }
  }
  return ret;
}

// log_id wantted does not exist on this server, feed this information back to liboblog,
// liboblog needs to change search server.
int ObExtLogFetcher::handle_log_not_exist(
    const ObPartitionKey& pkey, const uint64_t next_log_id, ObLogStreamFetchLogResp& resp)
{
  int ret = OB_SUCCESS;
  ObLogStreamFetchLogResp::FeedbackPartition fbp;
  fbp.pkey_ = pkey;
  fbp.feedback_type_ = ObLogStreamFetchLogResp::LOG_NOT_IN_THIS_SERVER;
  if (OB_FAIL(resp.append_feedback(fbp))) {
    LOG_WARN("handle log_not_exist append_feedback error", K(ret), K(fbp));
  } else {
    LOG_INFO("handle log_not_exist append_feedback success", K(fbp), K(next_log_id));
  }
  return ret;
}

int ObExtLogFetcher::after_partition_fetch_log(ObStreamItem& stream_item, const uint64_t beyond_upper_log_id,
    const int64_t beyond_upper_log_ts, const int64_t fetched_log_count, ObLogStreamFetchLogResp& resp)
{
  int ret = OB_SUCCESS;
  // 1. dealing with heartbeat hollow
  if ((0 == fetched_log_count) && OB_INVALID_ID != beyond_upper_log_id) {
    // log hole problem:
    // When liboblog side coordinates in unison, if the difference between the slowest pkey's
    // adjacent log timestamps is greater than the expected log time interval length of liboblog,
    // It may cause the slowest pkey to be unable to advance since upper_limit_ts limit of the expected log time
    // interval. At this time, the next log timestamp -1 is returned to liboblog to advance the progress of this pkey.
    ObLogStreamFetchLogResp::FetchLogHeartbeatItem hbp;
    hbp.pkey_ = stream_item.pkey_;
    hbp.next_log_id_ = beyond_upper_log_id;
    hbp.heartbeat_ts_ = beyond_upper_log_ts - 1;
    if (OB_FAIL(resp.append_hb(hbp))) {
      LOG_WARN("resp append fetch_log heartbeat error", K(ret), K(hbp));
    } else {
      LOG_TRACE("resp append fetch_log heartbeat success", K(hbp));
      // update and log progress
      stream_item.fetch_progress_ts_ = beyond_upper_log_ts - 1;
    }
  }
  return ret;
}

// Get single log entry
int ObExtLogFetcher::partition_fetch_log_entry_(const ObLogCursorExt& cursor_ext, const ObPartitionKey& pkey,
    const int64_t end_tstamp, ObReadCost& read_cost, ObLogStreamFetchLogResp& resp, bool& fetch_log_from_hot_cache,
    int64_t& log_entry_size)
{
  int ret = OB_SUCCESS;
  int64_t remain_size = 0;
  char* remain_buf = resp.get_remain_buf(remain_size);
  ret = fetch_decrypted_log_entry_(
      pkey, cursor_ext, remain_buf, remain_size, end_tstamp, read_cost, fetch_log_from_hot_cache, log_entry_size);
  return ret;
}

// fill clog directly into resp.buf_.
// If it is found that there is a *batch commit* mark in ilog, modify the serialized
// content of clog and mark batch commit in the submit timestamp.
// After receiving this mark, liboblog clears the mark first,
// and then verifies the checksum, otherwise the checksum cannot pass.
int ObExtLogFetcher::prefill_resp_with_clog_entry(const ObLogCursorExt& cursor_ext, const ObPartitionKey& pkey,
    const int64_t end_tstamp, ObReadCost& read_cost, ObLogStreamFetchLogResp& resp, bool& fetch_log_from_hot_cache,
    int64_t& log_entry_size)
{
  int ret = OB_SUCCESS;
  int64_t remain_size = 0;
  char* remain_buf = resp.get_remain_buf(remain_size);
  if (OB_FAIL(partition_fetch_log_entry_(
          cursor_ext, pkey, end_tstamp, read_cost, resp, fetch_log_from_hot_cache, log_entry_size))) {
    LOG_WARN("partition_fetch_log_entry_ fail", K(ret), K(cursor_ext), K(pkey), K(read_cost), K(resp), K(end_tstamp));
  } else {
    // If ilog marks the log as batch commit, set clog to batch commit
    // Since the log content is the content after serialization,
    // the serialized content should be modified at the specified location
    if (cursor_ext.is_batch_committed()) {
      ObLogEntryHeader header;
      // set submit_timestamp
      header.set_submit_timestamp(cursor_ext.get_submit_timestamp());
      // set batch commit mark
      header.set_trans_batch_commit_flag();

      // find the serialized position of submit_timestamp
      int64_t submit_ts_serialize_pos = header.get_submit_ts_serialize_pos();
      char* submit_ts_buf = remain_buf + submit_ts_serialize_pos;
      int64_t submit_ts_buf_size = remain_size - submit_ts_serialize_pos;

      // modify the serialized content of submit_timestamp
      int64_t serialize_pos = 0;
      if (OB_FAIL(header.serialize_submit_timestamp(submit_ts_buf, submit_ts_buf_size, serialize_pos))) {
        LOG_WARN("header serialize_submit_timestamp fail",
            K(ret),
            K(header),
            K(submit_ts_buf_size),
            K(submit_ts_buf),
            K(serialize_pos));
      }
    }
  }
  return ret;
}

void ObExtLogFetcher::check_next_cursor_(const ObStreamItem& stream_item, const ObLogCursorExt& next_cursor,
    ObLogStreamFetchLogResp& resp, const int64_t start_log_tstamp, const bool fetch_log_from_hot_cache,
    const int64_t fetched_log_count, FetchRunTime& frt, bool& part_fetch_stopped, const char*& part_stop_reason,
    uint64_t& beyond_upper_log_id, int64_t& beyond_upper_log_ts, bool& reach_upper_limit)
{
  int64_t submit_ts = next_cursor.get_submit_timestamp();
  bool buf_full = (!resp.has_enough_buffer(next_cursor.get_size()));

  if (buf_full) {
    // This log may not have the desired timestamp, but it is still regarded as buf_full.
    handle_buffer_full_(frt, part_stop_reason);
  } else if (submit_ts > frt.upper_limit_ts_) {
    // beyond the upper bound, the partition stops fetching logs
    reach_upper_limit = true;
    part_fetch_stopped = true;
    part_stop_reason = "ReachUpperLimit";
    beyond_upper_log_id = stream_item.next_log_id_;
    beyond_upper_log_ts = submit_ts;
    LOG_TRACE("partition reach upper limit", K(stream_item), K(submit_ts), K(frt));
  } else if (!fetch_log_from_hot_cache && OB_INVALID_TIMESTAMP != start_log_tstamp) {
    // If the log is not currently fetched from the hot cache, and the initial cursor is valid,
    // then verify whether the range of log fetching exceeds limit,
    // the purpose is to ensure that each partition "goes in step" to avoid data skew.
    int64_t log_time_interval = submit_ts - start_log_tstamp;

    if (log_time_interval > MAX_LOG_TIME_INTERVAL_PER_ROUND_PER_PART) {
      // The range of fetching logs exceeds limit
      part_fetch_stopped = true;
      part_stop_reason = "ReachMaxLogTimeIntervalPerRound";
      LOG_TRACE("partition reach max log time interval per round",
          K(log_time_interval),
          K(fetched_log_count),
          K(stream_item),
          K(next_cursor),
          K(start_log_tstamp));
    }
  }
}

void ObExtLogFetcher::handle_buffer_full_(FetchRunTime& frt, const char*& part_stop_reason)
{
  frt.stop("BufferFull");
  part_stop_reason = "BufferFull";
}

int ObExtLogFetcher::get_next_cursor_(ObStreamItem& stream_item, FetchRunTime& frt, const ObLogCursorExt*& next_cursor)
{
  UNUSED(frt);
  int ret = OB_SUCCESS;
  const ObPartitionKey& pkey = stream_item.pkey_;

  if (OB_FAIL(stream_item.get_next_cursor(next_cursor))) {
    if (OB_CURSOR_NOT_EXIST != ret) {
      LOG_WARN("get_next_cursor from stream_item fail", K(ret), K(stream_item));
    } else {
      // The cursor cached in the stream item is used up, visit the cursor cache here to get the cursor.
      ret = OB_SUCCESS;

      ObGetCursorResult result;
      // Prepare a result, store query result based on the memory of stream_item.
      stream_item.prepare_get_cursor_result(result);

      if (OB_SUCC(log_engine_->get_cursor_batch(pkey, stream_item.next_log_id_, result))) {
        // query success
        stream_item.update_cursor_array(result.ret_len_);

        // get cursor again
        if (OB_FAIL(stream_item.get_next_cursor(next_cursor))) {
          LOG_WARN(
              "get_next_cursor from stream_item fail after get_cursor_batch succ", K(ret), K(stream_item), K(result));
        }
      } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
        // reach the upper bound
      } else if (OB_CURSOR_NOT_EXIST == ret) {
        // next log does not exist
      } else if (OB_NEED_RETRY == ret) {
        // need retry
        LOG_WARN("get cursor need retry", K(ret), K(stream_item), K(result));
      } else {
        LOG_WARN("log_engine get_cursor_batch fail", K(ret), K(stream_item), K(result));
      }
    }
  }
  return ret;
}

int ObExtLogFetcher::partition_fetch_log(ObStreamItem& stream_item, FetchRunTime& frt, ObLogStreamFetchLogResp& resp,
    const int64_t end_tstamp, bool& reach_upper_limit, bool& reach_max_log_id, int64_t& log_count)
{
  int ret = OB_SUCCESS;
  int csr_ret = OB_SUCCESS;
  bool part_fetch_stopped = false;
  const char* part_stop_reason = "NONE";
  bool fetch_log_from_hot_cache = true;  // Whether to fetch logs from Hot Cache, the default is true.
  int64_t start_log_tstamp = OB_INVALID_TIMESTAMP;
  const ObPartitionKey& pkey = stream_item.pkey_;
  uint64_t beyond_upper_log_id = OB_INVALID_ID;
  int64_t beyond_upper_log_ts = OB_INVALID_TIMESTAMP;

  // Note: After the optimization of step by step, the count of logs fetched in each round of
  // each partition will be "suddenly reduced". It is possible to get only a few logs per round,
  // so other overheads will increase significantly.
  // In order to optimize performance, during the log fetching process, the current timestamp is
  // no longer used to evaluate the log fetching time, and the log reading time monitoring is temporarily removed.
  while (OB_SUCCESS == ret && !part_fetch_stopped && !frt.is_stopped()) {
    const ObLogCursorExt* next_cursor = NULL;

    if (is_time_up_(log_count, end_tstamp)) {  // time up, stop fetching logs globally
      frt.stop("TimeUP");
      part_stop_reason = "TimeUP";
      LOG_INFO("fetch log quit in time", K(end_tstamp), K(stream_item), K(frt), K(log_count));
    }
    // get the next cursor
    else if (OB_FAIL(get_next_cursor_(stream_item, frt, next_cursor))) {
      csr_ret = ret;
      if (OB_ERR_OUT_OF_UPPER_BOUND == ret || OB_CURSOR_NOT_EXIST == ret || OB_NEED_RETRY == ret) {
        // expected error code, quit cycle
        // If it is greater than the upper bound, considered that the maximum log ID is reached.
        reach_max_log_id = (OB_ERR_OUT_OF_UPPER_BOUND == ret);
      } else {
        LOG_WARN("get_next_cursor_ fail", K(ret), K(stream_item), K(frt), KPC(next_cursor));
      }
    } else if (OB_ISNULL(next_cursor)) {
      LOG_WARN("invalid next_cursor", K(next_cursor), K(stream_item), K(frt));
      ret = OB_ERR_UNEXPECTED;
    } else {
      // Check the next cursor to determine whether need to continue fetching logs.
      check_next_cursor_(stream_item,
          *next_cursor,
          resp,
          start_log_tstamp,
          fetch_log_from_hot_cache,
          log_count,
          frt,
          part_fetch_stopped,
          part_stop_reason,
          beyond_upper_log_id,
          beyond_upper_log_ts,
          reach_upper_limit);

      int64_t log_entry_size = 0;
      if (frt.is_stopped() || part_fetch_stopped) {
        // stop fetching log
      }
      // get single log entry
      else if (OB_FAIL(prefill_resp_with_clog_entry(
                   *next_cursor, pkey, end_tstamp, frt.read_cost_, resp, fetch_log_from_hot_cache, log_entry_size))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          handle_buffer_full_(frt, part_stop_reason);
          ret = OB_SUCCESS;
        } else {
          LOG_WARN(
              "fill clog by cursor fail", K(ret), K(stream_item), KPC(next_cursor), K(frt), K(end_tstamp), K(resp));
        }
      }
      // log fetched successfully, point to the next log
      else if (OB_FAIL(stream_item.next_log_fetched(next_cursor->get_submit_timestamp()))) {
        LOG_WARN("stream_item move to next log fail", K(ret), K(stream_item), KPC(next_cursor));
      } else {
        log_count++;
        resp.clog_entry_filled(log_entry_size);  // modify the buf pointer of resp
        if (OB_INVALID_TIMESTAMP == start_log_tstamp) {
          start_log_tstamp = next_cursor->get_submit_timestamp();
        }

        LOG_TRACE("partition fetch a log", K(log_count), K(stream_item), KPC(next_cursor), K(frt));
      }
    }
  }

  if (OB_SUCCESS == ret) {
    // do nothing
  } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret || OB_NEED_RETRY == ret || OB_TIMEOUT == ret) {
    // obtaining the cursor reaches the upper bound, obtaining the cursor needs retry, log fetching timeout
    ret = OB_SUCCESS;
  } else if (OB_CURSOR_NOT_EXIST == ret) {
    // log not exists
    ret = OB_SUCCESS;
    if (OB_FAIL(handle_log_not_exist(pkey, stream_item.next_log_id_, resp))) {
      LOG_WARN("handle log_not_exist error", K(ret), K(stream_item));
    }
  } else {
    // other error code
  }

  if (OB_SUCCESS == ret) {
    if (OB_FAIL(after_partition_fetch_log(stream_item, beyond_upper_log_id, beyond_upper_log_ts, log_count, resp))) {
      LOG_WARN("after partition fetch log error",
          K(ret),
          K(stream_item),
          K(log_count),
          K(beyond_upper_log_id),
          K(beyond_upper_log_ts));
    }
  }

  LOG_TRACE("partition fetch log done",
      K(ret),
      K(csr_ret),
      K(part_fetch_stopped),
      K(part_stop_reason),
      K(log_count),
      K(stream_item),
      K(frt),
      K(resp),
      K(fetch_log_from_hot_cache));
  return ret;
}

int ObExtLogFetcher::update_traffic_controller(ObReadCost& read_cost, ObIlogStorageQueryCost& csr_cost)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_cost.is_valid()) || OB_UNLIKELY(!csr_cost.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CSR_LOG(ERROR, "invalid cost", K(read_cost), K(csr_cost));
  } else {
    traffic_controller_.add_read_clog_disk_count(read_cost.read_disk_count_);
    traffic_controller_.add_read_ilog_disk_count(csr_cost.read_ilog_disk_count_);
    traffic_controller_.add_read_info_block_disk_count(csr_cost.read_info_block_disk_count_);

    LOG_TRACE("update traffic controller", K(read_cost), K(csr_cost), K(traffic_controller_));
  }
  return ret;
}

void ObExtLogFetcher::handle_when_need_not_fetch_(const bool reach_upper_limit, const bool reach_max_log_id,
    const bool status_changed, const int64_t fetched_log_count, ObStreamItem& stream_item, FetchRunTime& frt,
    ObStreamItemArray& invain_pkeys)
{
  bool push_into_feedback_check_array = false;
  bool is_stream_fall_behind = frt.is_stream_fall_behind();

  // Only when the status changes to "no need to fetch logs" will updated stats to avoid repeated statistics
  if (status_changed) {
    if (reach_upper_limit) {
      frt.fetch_status_.reach_upper_limit_ts_pkey_count_++;
    } else if (reach_max_log_id) {
      frt.fetch_status_.reach_max_log_id_pkey_count_++;

      // Put the partitions which reach the maximum log ID but the progress is behind the upper limit
      // into the feedback check list.
      // Because we cannot determine whether this partition is a backward standby or a normal partition,
      // we can only poll and check these partitions.
      // Optimize:
      // 1. No feedback is needed for real-time log fetching case, because no partitions fall behind.
      // 2. TODO: Each check, update the progress value of the corresponding partition,
      //          so we can filter the "normal service" partitions with a high probability.
      //
      // Only meet the following conditions to join the feedback list:
      if (fetched_log_count <= 0       // 1. No logs fetched in this round of RPC
          && frt.feedback_enabled_     // 2. need feedback
          && is_stream_fall_behind) {  // 3. Check only when the overall progress is behind,
                                       //    optimize the real-time log fetching case
        (void)invain_pkeys.push_back(&stream_item);
        push_into_feedback_check_array = true;
      }
    }

    LOG_TRACE("partition need not fetch",
        K(stream_item),
        K(status_changed),
        K(fetched_log_count),
        K(push_into_feedback_check_array),
        K(is_stream_fall_behind),
        "upper_limit_ts",
        frt.upper_limit_ts_,
        "feedback_enabled",
        frt.feedback_enabled_,
        "reach_upper_limit_ts_pkey_count",
        frt.fetch_status_.reach_upper_limit_ts_pkey_count_,
        "reach_max_log_id_pkey_count",
        frt.fetch_status_.reach_max_log_id_pkey_count_);
  }
}

int ObExtLogFetcher::do_fetch_log(const ObLogStreamFetchLogReq& req, FetchRunTime& frt, ObStream& stream,
    ObLogStreamFetchLogResp& resp, ObStreamItemArray& invain_pkeys)
{
  int ret = OB_SUCCESS;
  int64_t touched_pkey_count = 0;
  int64_t scan_round_count = 0;       // epoch of fetching
  int64_t need_fetch_pkey_count = 0;  // the actual count of partitions that need to fetch logs
  int64_t part_count = stream.get_item_count();
  storage::ObPartitionService* ps = partition_service_;
  int64_t end_tstamp = frt.rpc_deadline_ - RPC_QIT_RESERVED_TIME;

  if (OB_ISNULL(ps)) {
    ret = OB_NOT_INIT;
  }

  // Support cyclic scanning multiple rounds
  // Stop condition:
  // 1. time up, timeout
  // 2. buffer is full
  // 3. all partitions do not need to fetch logs, reach upper limit or max log id
  // 4. scan all partitions but no log fetched
  while (OB_SUCCESS == ret && !frt.is_stopped()) {
    int64_t index = 0;
    ObStreamItem* item_iter = NULL;
    int64_t stop_fetch_part_cnt = 0;  // count of partitions that stop fetching, each round of independent statistics
    int64_t before_scan_log_num = resp.get_log_num();
    int64_t need_fetch_part_count_per_round = 0;  // count of partitions that need to fetch logs in this round

    // update fetching rounds
    scan_round_count++;

    // Scan all partitions in each round, in which the iterator loop process unconditionally points to the next element,
    // so the bad partition can be skipped in extreme cases.
    for (index = 0, item_iter = stream.cur_item(); index < part_count && OB_SUCC(ret) && !frt.is_stopped();
         index++, item_iter = stream.next_item()) {
      if (OB_ISNULL(item_iter)) {
        LOG_WARN("invalid stream item", K(ret), K(stream), K(index), K(req));
        ret = OB_INVALID_ERROR;
      } else {
        bool need_fetch = true;
        bool reach_upper_limit = false;
        bool reach_max_log_id = false;
        bool status_changed = false;
        int64_t fetched_log_count = 0;  // count of log fetched

        // Check whether the partition needs to continue fetching, the function supports repeated checks.
        item_iter->check_need_fetch_status(
            frt.rpc_id_, frt.upper_limit_ts_, *ps, status_changed, need_fetch, reach_upper_limit, reach_max_log_id);

        if (need_fetch) {
          need_fetch_part_count_per_round++;

          // execute specific logging logic
          if (OB_FAIL(partition_fetch_log(
                  *item_iter, frt, resp, end_tstamp, reach_upper_limit, reach_max_log_id, fetched_log_count))) {
            LOG_WARN("partition_fetch_log error", K(ret), K(*item_iter), K(frt));
          } else {
            // after fetching the log, check again whether it is necessary to continue fetching
            if (reach_upper_limit || reach_max_log_id) {
              need_fetch = false;
              // FIXME: Here simulates the performance of check_need_fetch_status() function in StreamItem,
              // instead of calling again directly, intend to reduce the overhead of obtaining the maximum log ID.
              // status_changed indicates whether it is the first time that the status changes to be
              // not need to be fetched, here must be true.
              // Note: The status in StreamItem still as needs to fetch logs,
              // if there is the next round of fetching logs, its status will definitely change.
              status_changed = true;
            }
          }
        }

        if (OB_SUCCESS == ret) {
          // comprehensively consider the results of the two checks and handle stop fetching situation
          if (!need_fetch) {
            stop_fetch_part_cnt++;  // increase the count of stop fetching partitions in this round
            // update monitoring items, increase feedback partition array
            handle_when_need_not_fetch_(
                reach_upper_limit, reach_max_log_id, status_changed, fetched_log_count, *item_iter, frt, invain_pkeys);
          }
        }
      }
    }

    if (OB_SUCCESS == ret) {
      int64_t scan_part_count = index;
      int64_t fetch_log_cnt_in_scan = resp.get_log_num() - before_scan_log_num;
      // count of partitions for global update is set as the count of partitions with the most access in each round
      touched_pkey_count = std::max(touched_pkey_count, scan_part_count);
      // count of partitions that need fetching is set as the maximum number of partitions that need fetching in each
      // round
      need_fetch_pkey_count = std::max(need_fetch_pkey_count, need_fetch_part_count_per_round);

      // if still not over, decide whether to perform the next scanning round
      if (!frt.is_stopped()) {
        // all partition finished, stop scanning
        if (stop_fetch_part_cnt >= part_count) {
          frt.stop("AllPartStopFetch");
        } else if (0 >= fetch_log_cnt_in_scan) {
          // no log fetched in this round, stop scanning
          frt.stop("AllPartFetchNoLog");
        } else if (ObTimeUtility::current_time() > end_tstamp) {
          // time up, quit
          frt.stop("TimeUP");
        }
      }

      LOG_TRACE("fetch_log one round finish",
          K(scan_round_count),
          K(scan_part_count),
          K(fetch_log_cnt_in_scan),
          K(need_fetch_part_count_per_round),
          K(stop_fetch_part_cnt),
          "is_stopped",
          frt.is_stopped(),
          "stop_reason",
          frt.stop_reason_,
          "liboblog_instance_id",
          stream.get_liboblog_instance_id(),
          "stream_seq",
          req.get_stream_seq());
    }
  }

  // update statistics
  if (OB_SUCC(ret)) {
    frt.fetch_status_.touched_pkey_count_ = touched_pkey_count;
    frt.fetch_status_.need_fetch_pkey_count_ = need_fetch_pkey_count;
    frt.fetch_status_.scan_round_count_ = scan_round_count;
    resp.set_fetch_status(frt.fetch_status_);
    update_monitor(frt.fetch_status_, part_count, frt.read_cost_);
  } else {
    // In the case of failure to fetch the log, clear the progress timestamps of all partitions,
    // avoid the inconsistency between the progress of each partition on server and on liboblog.
    LOG_WARN("fetch log fail, clear all partitions progress timestamp",
        K(ret),
        "liboblog_instance_id",
        stream.get_liboblog_instance_id(),
        K(part_count),
        K(req),
        K(resp));
    stream.clear_progress_ts();
  }

  LOG_TRACE("do_fetch_log done", K(ret), K(frt), K(stream), K(req), K(resp));
  return ret;
}

// If liboblog pulls logs on a backward standby server, should feed this information back to liboblog,
// then liboblog will change accessed server.
bool ObExtLogFetcher::is_lag_follower(const ObPartitionKey& pkey, const int64_t sync_ts)
{
  bool is_lag = false;
  if (0 == sync_ts) {
    // corner case, partition is just created, do not try sync timestamp even
    LOG_INFO("partition is just created", K(pkey), K(sync_ts));
  } else {
    is_lag = ((ObTimeUtility::current_time() - sync_ts) > SYNC_TIMEOUT);
  }
  return is_lag;
}

// Check whether is backward standby server
int ObExtLogFetcher::check_lag_follower(const ObStreamSeq& fetch_log_stream_seq, const ObStreamItem& stream_item,
    ObIPartitionLogService& pls, ObLogStreamFetchLogResp& resp)
{
  int ret = OB_SUCCESS;
  ObRole role = INVALID_ROLE;
  int64_t leader_epoch = OB_INVALID_TIMESTAMP;
  const ObPartitionKey& pkey = stream_item.pkey_;
  const uint64_t next_log_id = stream_item.next_log_id_;
  uint64_t last_slide_log_id = pls.get_last_slide_log_id();

  if (OB_FAIL(pls.get_role_and_leader_epoch(role, leader_epoch))) {
    LOG_WARN("get_role_and_leader_epoch fail", K(ret));
  } else if (OB_UNLIKELY(INVALID_ROLE == role) || OB_UNLIKELY(LEADER == role && OB_INVALID_TIMESTAMP == leader_epoch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid role info", K(ret), K(pkey), K(role), K(leader_epoch));
  } else if (OB_UNLIKELY(OB_INVALID_ID == last_slide_log_id)) {
    // The partition has just been deleted, it is basically impossible to happen,
    // the following checks will not be performed to make RPC fail.
    ret = OB_PARTITION_NOT_EXIST;
    LOG_WARN("check lag follower error, last_slide_log_id is invalid", K(ret), K(pkey), K(last_slide_log_id));
  }
  // If is standby replica, and the next log taken is larger than the latest log,
  // check whether it is a backward replica, if it is backward, add it to the backward replica list.
  else if (next_log_id > last_slide_log_id && FOLLOWER == role) {
    bool is_sync = false;
    if (OB_FAIL(pls.is_in_sync(is_sync))) {
      if (OB_LOG_NOT_SYNC == ret) {
        // In the case where an out-of-sync error code is returned, considered as out of sync
        is_sync = false;
        ret = OB_SUCCESS;
      }
    }

    // Add self server to the backward replica list
    if (OB_SUCCESS == ret && !is_sync) {
      ObLogStreamFetchLogResp::FeedbackPartition fbp;
      fbp.pkey_ = pkey;
      fbp.feedback_type_ = ObLogStreamFetchLogResp::LAGGED_FOLLOWER;
      if (OB_FAIL(resp.append_feedback(fbp))) {
        LOG_WARN("append_feedback error", K(ret), K(fbp));
      } else {
        LOG_INFO("catch a lag follower", K(last_slide_log_id), K(stream_item), K(fetch_log_stream_seq));
      }
    }
  }
  return ret;
}

// Check whether the partition is still in service at the current moment
// > partition is garbage cleaned   =>  not serving
// > partition is invalid           =>  not serving
// > partition is offlined          =>  not serving
int ObExtLogFetcher::is_partition_serving(
    const ObPartitionKey& pkey, ObIPartitionGroup& part, ObIPartitionLogService& pls, bool& is_serving)
{
  int ret = OB_SUCCESS;
  bool offline = false;
  is_serving = false;
  if (!(part.is_valid())) {
    // partition is invalid, consider as unserviceable
  } else if (OB_FAIL(pls.is_offline(offline))) {
    LOG_WARN("check if partition is offline error", K(ret), K(pkey));
  } else if (offline) {
    LOG_INFO("check partition serving, partition is offline", K(pkey), K(offline), K(is_serving));
  } else {
    is_serving = true;
  }
  return ret;
}

// partition not serviceable
int ObExtLogFetcher::handle_partition_not_serving_(const ObStreamItem& stream_item, ObLogStreamFetchLogResp& resp)
{
  int ret = OB_SUCCESS;
  // Since discovered no more logs in the partition at first, and then judge is_serving,
  // Therefore, there may actually be a small amount of logs on the partition that can be fetched locally,
  // but report liboblog to change server in advance.
  // The probability of such situation is very small, while changing machine is also expected,
  // and the missing logs must can be pulled from other replica.
  // Therefore, such corner case is not handled.
  ObLogStreamFetchLogResp::FeedbackPartition fbp;
  fbp.pkey_ = stream_item.pkey_;
  fbp.feedback_type_ = ObLogStreamFetchLogResp::PARTITION_OFFLINED;
  if (OB_FAIL(resp.append_feedback(fbp))) {
    LOG_WARN("not serving partition, append_feedback error", K(ret), K(fbp), K(resp), K(stream_item));
  } else {
    LOG_TRACE("not serving partition, append_feedback success", K(fbp), K(stream_item));
  }
  return ret;
}

// feedback is used to return some useful information to liboblog
int ObExtLogFetcher::feedback(
    const ObLogStreamFetchLogReq& req, const ObStreamItemArray& invain_pkeys, ObLogStreamFetchLogResp& resp)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t count = invain_pkeys.count();

  if (OB_ISNULL(partition_service_)) {
    ret = OB_NOT_INIT;
  }

  // TODO: In the feedback process, the progress of the partitions in each
  // feedback list is updated with the latest service information,
  // Optimize the cold partition scenario, otherwise, in fetching historical log case,
  // each feedback will accidentally damage the cold partition of the normal service.
  // Only by using the heartbeat progress to update the progress of the cold partition,
  // can avoid being added to the feedback list.
  for (int64_t i = 0; i < count && OB_SUCC(ret); i++) {
    ObStreamItem* stream_item = invain_pkeys[i];
    ObIPartitionGroupGuard guard;
    ObIPartitionGroup* part = NULL;
    ObIPartitionLogService* pls = NULL;
    bool is_serving = false;

    if (OB_ISNULL(stream_item)) {
      // skip
    } else {
      const ObPartitionKey& pkey = stream_item->pkey_;

      if (OB_SUCCESS != partition_service_->get_partition(pkey, guard) ||
          NULL == (part = guard.get_partition_group()) ||
          NULL == (pls = guard.get_partition_group()->get_log_service())) {
        is_serving = false;
      } else if (OB_FAIL(is_partition_serving(pkey, *part, *pls, is_serving))) {
        LOG_WARN("check partition is serving fail", K(ret), K(pkey), K(is_serving));
      }

      if (OB_SUCCESS == ret) {
        if (!is_serving) {
          LOG_INFO(
              "partition is not serving, feedback PARTITION_OFFLINED info to fetch stream", KPC(stream_item), K(req));
          if (OB_FAIL(handle_partition_not_serving_(*stream_item, resp))) {
            LOG_WARN("handle_partition_not_serving_ fail", K(ret), KPC(stream_item), K(resp));
          }
        }
        // In the case of partition servicing, continue to check whether it is a backward standby machine
        else if (OB_FAIL(check_lag_follower(req.get_stream_seq(), *stream_item, *pls, resp))) {
          LOG_WARN("check lag follower error", K(ret), KP(stream_item), K(req));
        }
      }
    }
  }

  int64_t end_time = ObTimeUtility::current_time();
  ObExtLogServiceMonitor::feedback_count();
  ObExtLogServiceMonitor::feedback_pkey_count(count);
  ObExtLogServiceMonitor::feedback_time(end_time - start_time);
  return ret;
}

int ObExtLogFetcher::fetch_log(const ObLogStreamFetchLogReq& req, ObLogStreamFetchLogResp& resp)
{
  int ret = OB_SUCCESS;
  FetchRunTime frt;
  ObStream* stream = NULL;
  ObStreamItemArray invain_pkeys;
  const ObStreamSeq& stream_seq = req.get_stream_seq();
  const int64_t cur_tstamp = ObTimeUtility::current_time();

  // Generate this RPC ID using the current timestamp directly.
  ObLogRpcIDType rpc_id = cur_tstamp;

  if (IS_NOT_INIT || OB_ISNULL(log_engine_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fetch_log req", K(ret), K(req));
  } else if (OB_FAIL(frt.init(rpc_id, cur_tstamp, req))) {
    LOG_WARN("fetch runtime init error", K(ret), K(rpc_id), K(req));
  } else {
    // get stream use guard protection
    CriticalGuard(get_stream_qs());

    if (OB_FAIL(stream_map_.get(stream_seq, stream))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_STREAM_NOT_EXIST;
        LOG_WARN("stream not exist", K(ret), K(stream_seq), K(req));
      } else {
        LOG_WARN("stream_map_ get error", K(ret), K(stream_seq));
      }
    } else if (OB_ISNULL(stream)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null stream from map", K(ret), KP(stream), K(stream_seq));
    } else {
      // ensure only one liboblog enters the processing
      if (stream->start_process(req.get_upper_limit_ts())) {
        if (OB_FAIL(do_fetch_log(req, frt, *stream, resp, invain_pkeys))) {
          LOG_WARN("do fetch log error", K(ret), K(req), K(*stream));
        } else if (req.is_feedback_enabled() && OB_FAIL(feedback(req, invain_pkeys, resp))) {
          LOG_WARN("feedback error", K(ret), K(req), K(*stream));
        }
        stream->finish_process();
      } else {
        // liboblog guarantee this
        ret = OB_STREAM_BUSY;
        LOG_WARN("this stream already performed fetching", K(ret), K(stream_seq), K(*stream));
      }
    }
  }

  // set debug error
  resp.set_debug_err(ret);
  if (OB_SUCC(ret) || OB_STREAM_NOT_EXIST == ret || OB_STREAM_BUSY == ret) {
    // do nothing
  } else {
    LOG_WARN("fetch log error", K(ret), K(req));
    ret = OB_ERR_SYS;
  }
  if (OB_SUCC(ret)) {
    const int64_t fetch_log_size = resp.get_pos();
    const int64_t fetch_log_count = resp.get_log_num();
    ObExtLogServiceMonitor::fetch_size(fetch_log_size);
    EVENT_ADD(CLOG_EXTLOG_FETCH_LOG_SIZE, fetch_log_size);
    ObExtLogServiceMonitor::fetch_log_count(fetch_log_count);
    EVENT_ADD(CLOG_EXTLOG_FETCH_LOG_COUNT, fetch_log_count);
    traffic_controller_.add_traffic_size(fetch_log_size);
  }

  resp.set_err(ret);
  return ret;
}

// for debug
bool ObExtLogFetcher::DebugPrinter::operator()(const ObStreamSeq& seq, ObStream* stream)
{
  bool bret = true;
  if (OB_UNLIKELY(!seq.is_valid()) || OB_ISNULL(stream)) {
    LOG_WARN("invalid item", K(seq), KP(stream));
    bret = false;
  } else {
    stream->print_basic_info(count_, seq);
    LOG_TRACE("[FETCH_LOG_STREAM]", "print_idx", count_, "StreamSeq", seq, "Stream", *stream);
    count_++;
  }
  return bret;
}

void ObExtLogFetcher::print_all_stream()
{
  int64_t stream_count = stream_map_.count();
  LOG_INFO("[FETCH_LOG_STREAM] Print All Stream", K(stream_count));
  DebugPrinter printer;

  // for_each has lock protection inside, no guard protection is needed
  stream_map_.for_each(printer);
}

/////////////////////// FetchRunTime ///////////////////////
FetchRunTime::FetchRunTime()
    : rpc_id_(OB_LOG_INVALID_RPC_ID),
      rpc_start_tstamp_(0),
      upper_limit_ts_(0),
      step_per_round_(0),
      rpc_deadline_(0),
      feedback_enabled_(false),
      stop_(false),
      stop_reason_("NONE"),
      read_cost_(),
      csr_cost_(),
      fetch_status_()
{}

FetchRunTime::~FetchRunTime()
{
  // for performance, no longer resetting each variable here
}

int FetchRunTime::init(const ObLogRpcIDType rpc_id, const int64_t rpc_start_tstamp, const ObLogStreamFetchLogReq& req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_LOG_INVALID_RPC_ID == rpc_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("init fetch_runtime error", K(ret), K(rpc_id), K(req));
  } else {
    rpc_id_ = rpc_id;

    upper_limit_ts_ = req.get_upper_limit_ts();
    step_per_round_ = req.get_log_cnt_per_part_per_round();
    rpc_deadline_ = THIS_WORKER.get_timeout_ts();
    rpc_start_tstamp_ = rpc_start_tstamp;
    feedback_enabled_ = req.is_feedback_enabled();

    stop_ = false;
    stop_reason_ = "NONE";

    fetch_status_.reset();
    read_cost_.reset();
    csr_cost_.reset();
  }

  return ret;
}

}  // namespace logservice
}  // namespace oceanbase
