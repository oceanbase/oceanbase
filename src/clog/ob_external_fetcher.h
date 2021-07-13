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

#ifndef OCEANBASE_CLOG_OB_EXTERNAL_FETCHER_
#define OCEANBASE_CLOG_OB_EXTERNAL_FETCHER_

#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/allocator/ob_qsync.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "share/ob_worker.h"
#include "common/ob_role.h"
#include "ob_external_log_service_monitor.h"
#include "ob_log_define.h"
#include "ob_log_fetcher_impl.h"
#include "ob_log_reader_interface.h"
#include "ob_external_stream.h"              // ObStream, ObStreamItem
#include "ob_external_traffic_controller.h"  // ObExtTrafficController
#include "ob_log_line_cache.h"               // ObLogLineCache

namespace oceanbase {
namespace storage {
class ObIPartitionGroup;
}
namespace clog {
class ObILogEngine;
class ObIPartitionLogService;
}  // namespace clog
namespace logservice {

/*
 * According to the previous discussion, logservice pulls logs using logical sites.
 * libobolog organizes partitions into streams, sends a set of partitions and their start_log_id to logserviceo,
 * Logservice registers these partitions as a stream and assigns a globally unique StreamSeq back to liboblog,
 * then liboblog can use this StreamSeq to pull logs directly.
 * Which log is pulled next is recorded on the Stream maintained by logservice,
 * and the liboblog side has verification to ensure that the log will not be missed.
 *
 * In addition to returning logs, logservice also returns feedback,
 * Information including log non-existence, backward standby machine, partition offline, etc. must be fed back to
 * liboblog, and liboblog end will change machines according to its own strategy
 *
 * Log hole problem:
 * When the liboblog side coordinates in unison, if the slowest pkey adjacent log timestamps
 * is greater than the expected log time interval length of liboblog,
 * may cause the slowest pkey to be unable to advance because of the
 * upper_limit_ts limit of the expected log time interval.
 * At this time, the next log timestamp -1 should be returned to liboblog to advance the progress of the pkey.
 */

////////////////////////// ObExtLogFetcher ///////////////////////////
struct FetchRunTime;
class ObExtLogFetcher : public ObLogFetcherImpl {
public:
  static const int64_t SYNC_TIMEOUT = 5 * 1000 * 1000;  // 5 second
  // When fetch log finds that the remaining time is less than RPC_QIT_RESERVED_TIME,
  // exit immediately to avoid timeout
  static const int64_t RPC_QIT_RESERVED_TIME = 5 * 1000 * 1000;  // 5 second

  // In each epoch, the maximum amount of logs that each partition can take,
  // in units of time length.
  // For example, each round of each partition can take up to 1 second of the log amount.
  //
  // refer to the data access time setting of Line Cache,
  // ensure that all partitions go in step, so Line Cache can be eliminated as soon as possible
  static const int64_t MAX_LOG_TIME_INTERVAL_PER_ROUND_PER_PART = clog::ObLogLineCache::DATA_ACCESS_TIME_US / 2;

  // get_cursor_batch internally retries 10 times, guarantee to return in a short time
  static const int64_t GET_CURSOR_RETRY_LIMIT = 10;

public:
  ObExtLogFetcher()
      : cur_ts_(0),
        self_(),
        partition_service_(NULL),
        stream_map_(),
        stream_allocator_(),
        stream_allocator_lock_(),
        traffic_controller_()
  {}
  ~ObExtLogFetcher()
  {
    destroy();
  }
  int init(clog::ObLogLineCache& line_cache, clog::ObILogEngine* log_engine,
      storage::ObPartitionService* partition_service, const common::ObAddr& self);
  void destroy()
  {
    if (is_inited_) {
      is_inited_ = false;
      skip_hotcache_ = false;
      DestroyFunctor df(stream_allocator_);
      stream_map_.for_each(df);
      (void)stream_map_.destroy();
      stream_allocator_.reset();
      line_cache_ = NULL;
      log_engine_ = NULL;
      partition_service_ = NULL;
      traffic_controller_.reset();
      self_.reset();
    }
  }
  int open_stream(const obrpc::ObLogOpenStreamReq& req, const common::ObAddr& addr, obrpc::ObLogOpenStreamResp& resp);
  int fetch_log(const obrpc::ObLogStreamFetchLogReq& req, obrpc::ObLogStreamFetchLogResp& resp);

  // called in timer task
  int wash();
  void print_all_stream();

public:
  // pick out expired streams
  class ExpiredStreamPicker {
  public:
    ExpiredStreamPicker() : retired_arr_()
    {}
    bool operator()(const obrpc::ObStreamSeq& steam_seq, ObStream* stream);
    RetiredStreamArray& get_retired_arr()
    {
      return retired_arr_;
    }

  private:
    RetiredStreamArray retired_arr_;
  };

  // Pick out expired functor
  struct ExpiredStreamMarker {
    bool operator()(const obrpc::ObStreamSeq& steam_seq, ObStream* stream);
  };

private:
  inline static clog::ObReadParam cursor2param(const clog::ObLogCursorExt& cursor_ext)
  {
    const int64_t DEFAULT_READ_TIMEOUT = 10 * 1000;  // 10 ms
    clog::ObReadParam param;
    param.file_id_ = cursor_ext.get_file_id();
    param.offset_ = cursor_ext.get_offset();
    param.read_len_ = cursor_ext.get_size();
    param.timeout_ = DEFAULT_READ_TIMEOUT;
    return param;
  }

private:
  inline static bool is_invalid_submit_timestamp(const int64_t timestamp)
  {
    return 0 == timestamp || common::OB_INVALID_TIMESTAMP == timestamp;
  }

private:
  int generate_stream_seq(obrpc::ObStreamSeq& stream_seq);
  int alloc_stream_mem(const obrpc::ObStreamSeq& seq, const obrpc::ObLogOpenStreamReq& req, char*& ret_buf);
  void free_stream_mem(ObStream* stream);
  int do_open_stream(
      const obrpc::ObLogOpenStreamReq& req, const common::ObAddr& addr, obrpc::ObLogOpenStreamResp& resp);
  void mark_stream_expired(const obrpc::ObStreamSeq& seq);
  int handle_log_not_exist(
      const common::ObPartitionKey& pkey, const uint64_t next_log_id, obrpc::ObLogStreamFetchLogResp& resp);
  int after_partition_fetch_log(ObStreamItem& stream_item, const uint64_t beyond_upper_log_id,
      const int64_t beyond_upper_log_ts, const int64_t fetched_log_count, obrpc::ObLogStreamFetchLogResp& resp);
  int prefill_resp_with_clog_entry(const clog::ObLogCursorExt& cursor_ext, const common::ObPartitionKey& pkey,
      const int64_t end_tstamp, clog::ObReadCost& read_cost, obrpc::ObLogStreamFetchLogResp& resp,
      bool& fetch_log_from_hot_cache, int64_t& log_entry_size);
  int partition_fetch_log_entry_(const clog::ObLogCursorExt& cursor_ext, const common::ObPartitionKey& pkey,
      const int64_t end_tstamp, clog::ObReadCost& read_cost, obrpc::ObLogStreamFetchLogResp& resp,
      bool& fetch_log_from_hot_cache, int64_t& log_entry_size);
  int get_next_cursor_(ObStreamItem& stream_item, FetchRunTime& frt, const clog::ObLogCursorExt*& next_cursor);
  int partition_fetch_log(ObStreamItem& stream_item, FetchRunTime& frt, obrpc::ObLogStreamFetchLogResp& resp,
      const int64_t end_tstamp, bool& reach_upper_limit, bool& reach_max_log_id, int64_t& fetched_log_count);
  void check_next_cursor_(const ObStreamItem& stream_item, const clog::ObLogCursorExt& next_cursor,
      obrpc::ObLogStreamFetchLogResp& resp, const int64_t start_log_tstamp, const bool fetch_log_from_hot_cache,
      const int64_t fetched_log_count, FetchRunTime& frt, bool& part_fetch_stopped, const char*& part_stop_reason,
      uint64_t& beyond_upper_log_id, int64_t& beyond_upper_log_ts, bool& reach_upper_limit);
  void handle_buffer_full_(FetchRunTime& frt, const char*& part_stop_reason);
  int update_traffic_controller(clog::ObReadCost& read_cost, clog::ObIlogStorageQueryCost& csr_cost);
  inline void update_monitor(
      const obrpc::ObFetchStatus& fetch_status, const int64_t item_count, const clog::ObReadCost& read_cost)
  {
    ObExtLogServiceMonitor::round_rate(100 * fetch_status.touched_pkey_count_ / item_count);
    ObExtLogServiceMonitor::total_fetch_pkey_count(fetch_status.touched_pkey_count_);
    ObExtLogServiceMonitor::reach_upper_ts_pkey_count(fetch_status.reach_upper_limit_ts_pkey_count_);
    ObExtLogServiceMonitor::reach_max_log_pkey_count(fetch_status.reach_max_log_id_pkey_count_);
    ObExtLogServiceMonitor::need_fetch_pkey_count(fetch_status.need_fetch_pkey_count_);
    ObExtLogServiceMonitor::scan_round_count(fetch_status.scan_round_count_);
    ObExtLogServiceMonitor::read_disk_count(read_cost.read_disk_count_);
  }
  void handle_when_need_not_fetch_(const bool reach_upper_limit, const bool reach_max_log_id, const bool status_changed,
      const int64_t fetched_log_count, ObStreamItem& stream_item, FetchRunTime& frt, ObStreamItemArray& invain_pkeys);
  int do_fetch_log(const obrpc::ObLogStreamFetchLogReq& req, FetchRunTime& fetch_runtime, ObStream& stream,
      obrpc::ObLogStreamFetchLogResp& resp, ObStreamItemArray& invain_pkeys);
  bool is_lag_follower(const common::ObPartitionKey& pkey, const int64_t sync_ts);
  int check_lag_follower(const obrpc::ObStreamSeq& fetch_log_stream_seq, const ObStreamItem& stream_item,
      clog::ObIPartitionLogService& pls, obrpc::ObLogStreamFetchLogResp& resp);
  int is_partition_serving(const ObPartitionKey& pkey, storage::ObIPartitionGroup& part,
      clog::ObIPartitionLogService& pls, bool& is_serving);
  int handle_partition_not_serving_(const ObStreamItem& stream_item, obrpc::ObLogStreamFetchLogResp& resp);
  int feedback(const obrpc::ObLogStreamFetchLogReq& req, const ObStreamItemArray& invain_pkeys,
      obrpc::ObLogStreamFetchLogResp& resp);

  // check whether has reached time limit
  inline bool is_time_up_(const int64_t log_count, const int64_t end_tstamp)
  {
    // every batch of logs, check whether has timed out
    static const int64_t CHECK_TIMEOUT_LOG_COUNT = 100;
    static const int64_t CHECK_TIMEOUT_LOG_INDEX = 10;

    bool ret = false;
    if (((log_count % CHECK_TIMEOUT_LOG_COUNT) == CHECK_TIMEOUT_LOG_INDEX)) {
      int64_t cur_time = ObTimeUtility::current_time();
      ret = cur_time > end_tstamp;
    }
    return ret;
  }

private:
  class DebugPrinter {
  public:
    DebugPrinter() : count_(0)
    {}
    bool operator()(const obrpc::ObStreamSeq& seq, ObStream* stream);

  private:
    int64_t count_;
  };
  class DestroyFunctor {
  public:
    explicit DestroyFunctor(common::ObFIFOAllocator& fifo_allocator) : fifo_allocator_(fifo_allocator)
    {}
    bool operator()(const obrpc::ObStreamSeq& seq, ObStream* stream)
    {
      UNUSED(seq);
      if (NULL != stream) {
        fifo_allocator_.free(stream);
        stream = NULL;
      }
      return true;
    }

  private:
    common::ObFIFOAllocator& fifo_allocator_;
  };

private:
  int64_t cur_ts_;
  common::ObAddr self_;
  storage::ObPartitionService* partition_service_;
  common::ObLinearHashMap<obrpc::ObStreamSeq, ObStream*> stream_map_;
  common::ObFIFOAllocator stream_allocator_;
  common::ObSpinLock stream_allocator_lock_;

  // TODO: flow control currently does not work. refactor the flow control module later
  //       to make flow control take effect
  ObExtTrafficController traffic_controller_;
};

// some parameters and status during Fetch execution
struct FetchRunTime {
  // critical value of delay time
  // DELAY greater than or equal to this value is considered backward
  static const int64_t STREAM_FALL_BEHIND_THRESHOLD_TIME = 3 * 1000 * 1000;

  // in params: config related
  // The unique identifier of a round of RPC, currently uses a timestamp to distinguish
  ObLogRpcIDType rpc_id_;
  int64_t rpc_start_tstamp_;
  int64_t upper_limit_ts_;
  int64_t step_per_round_;
  int64_t rpc_deadline_;
  bool feedback_enabled_;

  // out params: control flow related
  bool stop_;
  const char* stop_reason_;

  // the following are frequently changed
  clog::ObReadCost read_cost_ CACHE_ALIGNED;
  clog::ObIlogStorageQueryCost csr_cost_ CACHE_ALIGNED;
  obrpc::ObFetchStatus fetch_status_ CACHE_ALIGNED;

public:
  FetchRunTime();
  ~FetchRunTime();
  int init(const ObLogRpcIDType rpc_id, const int64_t cur_tstamp, const obrpc::ObLogStreamFetchLogReq& req);

  inline bool is_stopped() const
  {
    return stop_;
  }

  inline void stop(const char* stop_reason)
  {
    stop_ = true;
    stop_reason_ = stop_reason;
  }

  inline bool is_stream_fall_behind() const
  {
    int64_t delay_time = rpc_start_tstamp_ - upper_limit_ts_;
    return delay_time > STREAM_FALL_BEHIND_THRESHOLD_TIME;
  }

  TO_STRING_KV(K(rpc_id_), K(rpc_start_tstamp_), K(upper_limit_ts_), K(step_per_round_), K(rpc_deadline_),
      K(feedback_enabled_), K(stop_), K(stop_reason_), K(read_cost_), K(csr_cost_), K(fetch_status_));
};

}  // namespace logservice
}  // namespace oceanbase

#endif
