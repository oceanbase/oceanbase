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

#ifndef OCEANBASE_CLOG_OB_LOG_EXTERNAL_EXECUTOR_V2_
#define OCEANBASE_CLOG_OB_LOG_EXTERNAL_EXECUTOR_V2_

#include "share/ob_worker.h"
#include "ob_info_block_handler.h"
#include "ob_i_log_engine.h"
#include "ob_log_define.h"
#include "ob_log_external_rpc.h"

namespace oceanbase {
namespace clog {
class ObILogEngine;
class ObIndexInfoBlockHandler;
}  // namespace clog
namespace extlog {
class ObExtRpcQit;

//----  structures searching  -----

// Locate log_id based on timestamp, the information recorded by each partition during disk scan
struct SearchByTs {
  SearchByTs()
  {
    reset();
  }

  // Liboblog start time
  int64_t start_ts_;

  // per-pkey err code
  int err_;
  // Only determine the result based on InfoBlock, avoid scanning the disk and return a small safe value
  uint64_t start_log_id_;
  // log_ts corresponding to log of start_log_id_
  int64_t start_log_ts_;

  // Liboblog startup timestamp start_ts is very small,
  // and the smallest log timestamp of this machine is larger than start_ts_
  // In order not to leak the log at this time, a safe start_log_id cannot be given
  // The error code of this partition is OB_ERR_LOWER_BOUND, and start_log_id_ is the smallest known log_id
  uint64_t min_greater_log_id_;

  void init()
  {
    reset();
  }
  void reset()
  {
    start_ts_ = common::OB_INVALID_TIMESTAMP;
    err_ = common::OB_NOT_INIT;
    start_log_id_ = common::OB_INVALID_ID;
    start_log_ts_ = common::OB_INVALID_TIMESTAMP;
    min_greater_log_id_ = common::OB_INVALID_ID;
  }
  TO_STRING_KV(K(start_ts_), K(err_), K(start_log_id_), K(start_log_ts_), K(min_greater_log_id_));
};

// Locating offset based on log_id, the information recorded by each partition during the scanning process
struct SearchById {
  SearchById()
  {
    reset();
  }

  // requested log_id
  uint64_t start_log_id_;

  // per-pkey err code
  int err_;
  // Only determine the result based on InfoBlock, avoid scanning the disk. offset is 0
  clog::file_id_t res_file_id_;

  void init()
  {
    reset();
  }
  void reset()
  {
    start_log_id_ = common::OB_INVALID_ID;
    err_ = common::OB_NOT_INIT;
    res_file_id_ = common::OB_INVALID_FILE_ID;
  }
  TO_STRING_KV(K(start_log_id_), K(err_), K(res_file_id_));
};

enum SearchStatusType { SST_INVALID = -1, SST_BY_TS = 0, SST_BY_ID = 1 };

struct SearchStatus {
public:
  SearchStatusType type_;
  SearchByTs search_by_ts_;
  SearchById search_by_id_;
  bool finished_;

public:
  SearchStatus() : type_(SST_INVALID), search_by_ts_(), search_by_id_(), finished_(false)
  {}
  int init(const SearchStatusType type)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(SST_BY_ID != type) && OB_UNLIKELY(SST_BY_TS != type)) {
      ret = common::OB_INVALID_ARGUMENT;
      EXTLOG_LOG(ERROR, "construct SearchStatus error, unknown type", K(type));
    } else {
      type_ = type;
      if (SST_BY_TS == type_) {
        search_by_ts_.reset();
      } else if (SST_BY_ID == type_) {
        search_by_id_.reset();
      }
      finished_ = false;
    }
    return ret;
  }
  SearchByTs& get_by_ts_status()
  {
    return search_by_ts_;
  }
  SearchById& get_by_id_status()
  {
    return search_by_id_;
  }
  int set_start_ts(const int64_t start_ts)
  {
    int ret = common::OB_SUCCESS;
    if (start_ts <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "set start timestamp error", K(ret), K(start_ts));
    } else {
      search_by_ts_.start_ts_ = start_ts;
    }
    return ret;
  }
  int64_t get_start_ts() const
  {
    return search_by_ts_.start_ts_;
  }
  int set_start_log_id(const uint64_t start_log_id)
  {
    int ret = common::OB_SUCCESS;
    if (start_log_id <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "set start timestamp error", K(ret), K(start_log_id));
    } else {
      search_by_id_.start_log_id_ = start_log_id;
    }
    return ret;
  }
  uint64_t get_start_log_id() const
  {
    return search_by_id_.start_log_id_;
  }
  void set_finished()
  {
    finished_ = true;
  }
  bool is_finished() const
  {
    return finished_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(SearchStatus);
};

typedef common::ObLinearHashMap<common::ObPartitionKey, SearchStatus*> SearchMap;

struct SearchParam {
public:
  SearchStatusType type_;
  common::PageArena<>* map_allocator_;
  common::PageArena<>* info_allocator_;

  SearchParam() : type_(SST_INVALID), map_allocator_(NULL), info_allocator_(NULL)
  {}
  int init(const SearchStatusType type, common::PageArena<>* ma, common::PageArena<>* ia)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(SST_BY_ID != type && SST_BY_TS != type) || OB_UNLIKELY(NULL == ma) || OB_UNLIKELY(NULL == ia)) {
      ret = common::OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument", K(ret), K(type), KP(ma), KP(ia));
    } else {
      type_ = type;
      map_allocator_ = ma;
      info_allocator_ = ia;
    }
    return ret;
  }
  bool is_valid()
  {
    return (NULL != map_allocator_) && (NULL != info_allocator_) && (SST_BY_TS == type_ || SST_BY_ID == type_);
  }
  bool is_by_ts() const
  {
    return SST_BY_TS == type_;
  }
  bool is_by_id() const
  {
    return SST_BY_ID == type_;
  }
  TO_STRING_KV(K(type_), KP(map_allocator_), KP(info_allocator_));

private:
  DISALLOW_COPY_AND_ASSIGN(SearchParam);
};

// Record information related to positioning progress
struct Progress {
  // The total number of partitions and the number of completed partitions
  int64_t total_;
  int64_t finished_;

  // file id range
  clog::file_id_t cur_file_id_;
  clog::file_id_t min_file_id_;
  clog::file_id_t max_file_id_;

  // In this positioning rpc request, the maximum value of the breakpoint of each partition
  clog::file_id_t start_file_id_;

  // Exit the breakpoint during this execution
  clog::file_id_t break_file_id_;

  Progress()
  {
    reset();
  }
  void reset()
  {
    total_ = 0;
    finished_ = 0;
    cur_file_id_ = common::OB_INVALID_FILE_ID;
    min_file_id_ = common::OB_INVALID_FILE_ID;
    max_file_id_ = common::OB_INVALID_FILE_ID;
    start_file_id_ = common::OB_INVALID_FILE_ID;
    break_file_id_ = common::OB_INVALID_FILE_ID;
  }
  int set_total(const int64_t total)
  {
    int ret = common::OB_SUCCESS;
    if (total <= 0) {
      ret = common::OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "progress set total error", K(ret), K(total));
    } else {
      total_ = total;
    }
    return ret;
  }
  void clear_finished()
  {
    finished_ = 0;
  }
  void finish_one()
  {
    finished_++;
    if (finished_ > total_) {
      CLOG_LOG(ERROR, "finished overflow", K(finished_), K(total_));
    }
  }
  int set_cur_file_id(const clog::file_id_t cur)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(common::OB_INVALID_FILE_ID == cur)) {
      ret = common::OB_INVALID_ARGUMENT;
      EXTLOG_LOG(WARN, "progress set cur_file_id error", K(ret), K(cur));
    } else {
      cur_file_id_ = cur;
    }
    return ret;
  }
  int set_file_id_range(const clog::file_id_t min, const clog::file_id_t max)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(common::OB_INVALID_FILE_ID == min) || OB_UNLIKELY(common::OB_INVALID_FILE_ID == max) ||
        OB_UNLIKELY(min > max)) {
      ret = common::OB_INVALID_ARGUMENT;
      EXTLOG_LOG(WARN, "set file id range error", K(ret), K(min), K(max));
    } else {
      min_file_id_ = min;
      max_file_id_ = max;
    }
    return ret;
  }
  // When the parameter max_break_file_id is an illegal value,
  // it means that there is no breakpoint information, and start_file_id_ starts from max_file_id
  int set_max_break_file_id(const clog::file_id_t max_break_file_id)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(common::OB_INVALID_FILE_ID == min_file_id_) ||
        OB_UNLIKELY(common::OB_INVALID_FILE_ID == max_file_id_)) {
      ret = common::OB_NOT_INIT;
      EXTLOG_LOG(WARN, "should init min/max file id before set max_break_file_id", K(ret));
    } else {
      if (common::OB_INVALID_FILE_ID == max_break_file_id) {
        start_file_id_ = max_file_id_;
      } else if (max_break_file_id > max_file_id_) {
        // The given breakpoint is larger than the current largest file,
        // it may be that fetcher asked the wrong machine (the breakpoint information is server local)
        EXTLOG_LOG(ERROR, "too large max_break_file_id", K(max_break_file_id));
        start_file_id_ = max_file_id_;
      } else {
        start_file_id_ = max_break_file_id;
        if (max_break_file_id < min_file_id_) {
          // Continue the subsequent process and finish after_scan
          EXTLOG_LOG(WARN, "max_break_file_id is too small");
        }
      }
    }
    return ret;
  }
  clog::file_id_t get_start_file_id() const
  {
    if (OB_UNLIKELY(common::OB_INVALID_FILE_ID == start_file_id_)) {
      EXTLOG_LOG(ERROR, "start_file_id_ not inited");
    }
    return start_file_id_;
  }
  bool all_finished()
  {
    return finished_ == total_;
  }
  bool is_last_file()
  {
    return cur_file_id_ == max_file_id_;
  }
  int set_break_file_id(const clog::file_id_t break_file_id)
  {
    int ret = common::OB_SUCCESS;
    if (common::OB_INVALID_FILE_ID == break_file_id) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      break_file_id_ = break_file_id;
    }
    return ret;
  }
  clog::file_id_t get_break_file_id() const
  {
    return break_file_id_;
  }
  TO_STRING_KV(
      K(total_), K(finished_), K(min_file_id_), K(max_file_id_), K(cur_file_id_), K(start_file_id_), K(break_file_id_));

private:
  DISALLOW_COPY_AND_ASSIGN(Progress);
};

// Only supports positioning with breakpoints
class ObLogExternalExecutorWithBreakpoint {
public:
  struct Config {
    const char* ilog_dir_;
    uint64_t read_timeout_;

    Config()
    {
      ilog_dir_ = NULL;
      read_timeout_ = 0;
    }
    bool is_valid() const
    {
      return (NULL != ilog_dir_) && (0 < read_timeout_);
    }
    TO_STRING_KV(K(ilog_dir_), K(read_timeout_));
  };

public:
  ObLogExternalExecutorWithBreakpoint() : is_inited_(false), log_engine_(NULL), config_(), partition_service_(NULL)
  {}
  ~ObLogExternalExecutorWithBreakpoint()
  {}
  int init(const ObLogExternalExecutorWithBreakpoint::Config& config, clog::ObILogEngine* log_engine,
      storage::ObPartitionService* partition_service);

public:
  int req_start_log_id_by_ts_with_breakpoint(const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req_msg,
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& result);
  int req_start_pos_by_log_id_with_breakpoint(const obrpc::ObLogReqStartPosByLogIdRequestWithBreakpoint& req_msg,
      obrpc::ObLogReqStartPosByLogIdResponseWithBreakpoint& result);

private:
  inline int64_t get_rpc_deadline()
  {
    return THIS_WORKER.get_timeout_ts();
  }
  int init_search_status(
      SearchStatus* search_status, const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint::Param& param);
  int init_search_status(
      SearchStatus* search_status, const obrpc::ObLogReqStartPosByLogIdRequestWithBreakpoint::Param& param);
  int get_last_slide_log(const common::ObPartitionKey& pkey, uint64_t& last_log_id, int64_t& last_ts);
  template <class ReqMsg>
  int build_search_map(const ReqMsg& req_msg, SearchParam& search_param, SearchMap& search_map, Progress& progress);
  int process_info_entry(const common::ObPartitionKey& pkey, const uint64_t min_log_id, const int64_t submit_timestamp,
      SearchParam& search_param, SearchStatus* search_status, Progress& progress);
  int scan_cur_info_block(SearchMap& search_map, SearchParam& search_param, Progress& progress);
  int after_scan(SearchMap& search_map, SearchParam& search_param, Progress& progress);
  int progress_set_file_id_range(Progress& progress);
  int do_hurry_quit(SearchMap& search_map, SearchParam& search_param, Progress& progress, const ObExtRpcQit& qit);
  int scan_info_blocks(SearchMap& search_map, SearchParam& search_param, Progress& progress, const ObExtRpcQit& qit);
  int get_last_log_id_and_ts(const common::ObPartitionKey& pkey, uint64_t& last_log_id, int64_t& last_ts);
  int progress_set_file_range(Progress& progress);
  int ts_handle_cold_pkeys_by_sw(const common::ObPartitionKey& pkey, SearchStatus* search_status, Progress& progress);
  int get_sw_max_log_id(const common::ObPartitionKey& pkey, uint64_t& sw_max_log_id);
  int id_handle_cold_pkeys_by_sw(const common::ObPartitionKey& pkey, SearchStatus* search_status, Progress& progress);
  int handle_cold_pkeys_by_sw(SearchMap& search_map, SearchParam& search_param, Progress& progress);
  int ts_handle_cold_pkeys_by_last_info(
      const common::ObPartitionKey& pkey, SearchStatus* search_status, Progress& progress);
  int id_handle_cold_pkeys_by_last_info(
      const common::ObPartitionKey& pkey, SearchStatus* search_status, Progress& progress);
  int handle_cold_pkeys_by_last_info(SearchMap& search_map, SearchParam& search_param, Progress& progress);
  int handle_cold_pkeys(SearchMap& search_map, SearchParam& search_param, Progress& progress);
  int ts_build_result(const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req, SearchMap& search_map,
      const Progress& progress, obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& resp);
  int id_build_result(const obrpc::ObLogReqStartPosByLogIdRequestWithBreakpoint& req_msg, SearchMap& search_map,
      const Progress& progress, obrpc::ObLogReqStartPosByLogIdResponseWithBreakpoint& resp);
  int do_req_start_log_id_by_ts(const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req_msg,
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& response, const ObExtRpcQit& qit);
  int do_req_start_log_id_by_ts_qit(const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req_msg,
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& resp);
  int do_req_start_pos_by_log_id(const obrpc::ObLogReqStartPosByLogIdRequestWithBreakpoint& req_msg,
      obrpc::ObLogReqStartPosByLogIdResponseWithBreakpoint& response, const ObExtRpcQit& qit);
  int do_req_start_pos_by_log_id_qit(const obrpc::ObLogReqStartPosByLogIdRequestWithBreakpoint& req_msg,
      obrpc::ObLogReqStartPosByLogIdResponseWithBreakpoint& response);
  int get_min_log_id_and_ts_from_index_info_block_map_(clog::IndexInfoBlockMap& index_info_block_map,
      const common::ObPartitionKey& partition_key, uint64_t& min_log_id, int64_t& min_submit_timestamp) const;

private:
  static const bool FORCE_RPINT_STATISTIC = true;

private:
  bool is_inited_;
  clog::ObILogEngine* log_engine_;
  Config config_;
  storage::ObPartitionService* partition_service_;
  DISALLOW_COPY_AND_ASSIGN(ObLogExternalExecutorWithBreakpoint);
};
}  // namespace extlog
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_LOG_EXTERNAL_EXECUTOR_V2_
