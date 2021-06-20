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

#ifndef OCEANBASE_CLOG_OB_EXTERNAL_STREAM_
#define OCEANBASE_CLOG_OB_EXTERNAL_STREAM_

#include "lib/allocator/ob_qsync.h"
#include "ob_log_define.h"
#include "ob_log_external_rpc.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace logservice {
// get log status
// describe whether need to continue fetching logs
struct NeedFetchStatus {
  // Whether need to continuing fetch logs is determined based on
  // whether reach upper limit and reach max log id.
  bool need_fetch_;
  // whether reach upper limit
  bool reach_upper_limit_;
  // whether reach max log id
  bool reach_max_log_id_;

  // need fetch log by default
  NeedFetchStatus()
  {
    reset();
  }
  void reset()
  {
    // need fetch log by default
    need_fetch_ = true;
    reach_upper_limit_ = false;
    reach_max_log_id_ = false;
  }
  void reset(const bool need_fetch, const bool reach_upper_limit, const bool reach_max_log_id)
  {
    need_fetch_ = need_fetch;
    reach_upper_limit_ = reach_upper_limit;
    reach_max_log_id_ = reach_max_log_id;
  }

  TO_STRING_KV(K(need_fetch_), K(reach_upper_limit_), K(reach_max_log_id_));
};

typedef int64_t ObLogRpcIDType;
static const int64_t OB_LOG_INVALID_RPC_ID = 0;

// Information about a partition in Stream
struct ObStreamItem {
  static const int64_t CACHED_CURSOR_COUNT = 128;
  //************* member variables *************
  common::ObPartitionKey pkey_;
  uint64_t next_log_id_;        // next log
  int64_t fetch_progress_ts_;   // get log progress timestamp, log update, or heartbeat update
  uint64_t last_slide_log_id_;  // record the maximum log id
  // the most recent RPC of the partition fetch log, which means the last RPC update of the data of the partition
  ObLogRpcIDType fetch_rpc_id_;
  NeedFetchStatus need_fetch_status_;  // get log status

  // next_cursor_ and next_log_id_ one-to-one correspondence
  int64_t next_cursor_;
  int64_t cursor_array_size_;
  clog::ObLogCursorExt cursor_array_[CACHED_CURSOR_COUNT];

  ObStreamItem()
      : pkey_(),
        next_log_id_(common::OB_INVALID_ID),
        fetch_progress_ts_(0),
        last_slide_log_id_(common::OB_INVALID_ID),
        fetch_rpc_id_(OB_LOG_INVALID_RPC_ID),
        need_fetch_status_(),
        next_cursor_(0),
        cursor_array_size_(0)
  {}
  ObStreamItem(const common::ObPartitionKey& pkey, const uint64_t start_log_id)
      : pkey_(pkey),
        next_log_id_(start_log_id),
        fetch_progress_ts_(0),
        last_slide_log_id_(common::OB_INVALID_ID),
        fetch_rpc_id_(OB_LOG_INVALID_RPC_ID),
        need_fetch_status_(),
        next_cursor_(0),
        cursor_array_size_(0)
  {}
  bool is_valid() const
  {
    return pkey_.is_valid() && common::OB_INVALID_ID != next_log_id_;
  }

  inline int get_next_cursor(const clog::ObLogCursorExt*& cursor)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(next_cursor_ >= cursor_array_size_)) {
      ret = common::OB_CURSOR_NOT_EXIST;
    } else {
      cursor = cursor_array_ + next_cursor_;
    }
    return ret;
  }
  inline void prepare_get_cursor_result(clog::ObGetCursorResult& result)
  {
    result.csr_arr_ = cursor_array_;
    result.arr_len_ = CACHED_CURSOR_COUNT;
    result.ret_len_ = 0;
  }
  inline int update_cursor_array(const int64_t valid_array_size)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(valid_array_size > CACHED_CURSOR_COUNT) || OB_UNLIKELY(valid_array_size <= 0)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      next_cursor_ = 0;
      cursor_array_size_ = valid_array_size;
    }
    return ret;
  }
  // move to the next log
  // ensure atomicity between multiple variables
  inline int next_log_fetched(const int64_t log_submit_ts)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(next_cursor_ >= cursor_array_size_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      next_cursor_++;
      next_log_id_++;
      fetch_progress_ts_ = log_submit_ts;
    }
    return ret;
  }
  inline void update_progress_tstamp(const int64_t tstamp)
  {
    fetch_progress_ts_ = tstamp;
  }

  // Check whether need to continue fetching logs based on current status.
  int check_need_fetch_status(const ObLogRpcIDType fetch_rpc_id, const int64_t upper_limit_ts,
      storage::ObPartitionService& part_service, bool& status_changed, bool& need_fetch, bool& reach_upper_limit,
      bool& reach_max_log_id);

  TO_STRING_KV(K(pkey_), K(next_log_id_), K(fetch_progress_ts_), K(last_slide_log_id_), K(fetch_rpc_id_),
      K(need_fetch_status_), K(next_cursor_), K(cursor_array_size_));

private:
  uint64_t get_last_slide_log_id_(const ObPartitionKey& pkey, storage::ObPartitionService& part_service);
};

typedef common::ObSEArray<ObStreamItem*, 64> ObStreamItemArray;

class ObStream {
public:
  ObStream()
      : processing_(false),
        liboblog_instance_id_(),
        lifetime_(0),
        deadline_ts_(0),
        item_count_(0),
        rr_pointer_(0),
        upper_limit_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObStream()
  {
    destroy();
  }
  struct LiboblogInstanceId;
  int init(const obrpc::ObLogOpenStreamReq::ParamArray& params, const int64_t lifetime,
      const LiboblogInstanceId& liboblog_instance_id);
  void destroy();
  inline bool is_expired() const
  {
    return common::ObTimeUtility::current_time() > ATOMIC_LOAD(&deadline_ts_);
  }
  // mark expired
  void mark_expired()
  {
    ATOMIC_STORE(&deadline_ts_, 0);
  }
  void keep_alive()  // extend one cycle
  {
    ATOMIC_STORE(&deadline_ts_, common::ObTimeUtility::current_time() + lifetime_);
  }
  ObStreamItem* cur_item();
  ObStreamItem* next_item();

  // return whether the opening is successful
  // true: preempt the stream successfully
  // false: preempt failed
  bool start_process(const int64_t upper_limit_ts)
  {
    bool succ = ATOMIC_BCAS(&processing_, false, true);
    if (succ) {
      // extend cycle
      keep_alive();
      update_upper_limit(upper_limit_ts);
    }
    return succ;
  }
  void finish_process()
  {
    (void)ATOMIC_STORE(&processing_, false);
  }

  // update upper limit
  void update_upper_limit(const int64_t upper_limit)
  {
    ATOMIC_STORE(&upper_limit_ts_, upper_limit);
  }
  int64_t get_upper_limit() const
  {
    return ATOMIC_LOAD(&upper_limit_ts_);
  }

  // reset progress information of each partition
  void clear_progress_ts();

  // for monitor
  int64_t get_item_count() const
  {
    return item_count_;
  }
  int64_t get_rr_pointer() const
  {
    return rr_pointer_;
  }
  const LiboblogInstanceId& get_liboblog_instance_id() const
  {
    return liboblog_instance_id_;
  }

  DECLARE_TO_STRING;
  void print_basic_info(const int64_t stream_idx, const obrpc::ObStreamSeq& stream_seq) const;

public:
  // maximum number of files to be cached
  static const int64_t MAX_CACHED_FILE_COUNT = 10;

public:
  struct LiboblogInstanceId {
    common::ObAddr addr_;
    uint64_t peer_pid_;

    LiboblogInstanceId()
    {
      addr_.reset();
      peer_pid_ = 0;
    }
    LiboblogInstanceId(const common::ObAddr& addr, const uint64_t his_pid) : addr_(addr), peer_pid_(his_pid)
    {}

    void reset()
    {
      addr_.reset();
      peer_pid_ = 0;
    }

    TO_STRING_KV(K(addr_), K(peer_pid_));
  };

private:
  static const bool PRINT_STREAM_VERBOSE = false;

private:
  // The Stream information maintained by the server does not allow concurrent modification. Use this mark to check.
  bool processing_;
  // LiboblogInstanceId is only used for debugging, since may be multiple liboblogs pulling the same cluster.
  LiboblogInstanceId liboblog_instance_id_;
  // Each time a stream is requested, the life of the stream will be extended to a life_time_
  // (the interval between two accesses of the stream may be different, keep flexible).
  int64_t lifetime_;
  // If this stream is not used until deadline_ts_, it will be washed off by the scheduled task.
  int64_t deadline_ts_;
  // partition count in a stream
  int64_t item_count_;

  // rotate partition pointer
  int64_t rr_pointer_;

  // upper limit of current stream
  int64_t upper_limit_ts_;

  // must the last element, followed by the partition information
  ObStreamItem items_[0];
};

// Protect the Stream object being used from being washed.
static inline common::ObQSync& get_stream_qs()
{
  static common::ObQSync stream_qs;
  return stream_qs;
}

struct SeqStreamPair {
  obrpc::ObStreamSeq seq_;
  ObStream* stream_;
  TO_STRING_KV(K(seq_), KP(stream_));
};

typedef common::ObSEArray<SeqStreamPair, 16> RetiredStreamArray;

}  // namespace logservice
}  // namespace oceanbase

#endif
