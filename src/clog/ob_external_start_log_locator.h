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

#ifndef OCEANBASE_CLOG_OB_EXTERNAL_START_LOG_LOCATOR_
#define OCEANBASE_CLOG_OB_EXTERNAL_START_LOG_LOCATOR_

#include "share/ob_worker.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "clog/ob_log_external_rpc.h"
#include "clog/ob_info_block_handler.h"
#include "clog/ob_log_file_pool.h"

namespace oceanbase {
namespace clog {
class ObILogEngine;
}

namespace storage {
class ObPartitionService;
}

namespace logservice {

typedef obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint ObLocateByTsReq;
typedef obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint ObLocateByTsResp;

/*
 * ObExtStartLogLocator is used to serve liboblog positioning requirements
 *
 * 1 Consider the situation before splitting user and system logs:
 *  1.1 According to the agreement between liboblog-server,
 *      liboblog sends observer positioning request ObLocateByTsReq,
 *      The request contains a set of partition_key and start_timestamp
 *      (it can also be designed to have only one start_timestamp, But not flexible enough),
 *      logservice determines the start_log_id of each partition by querying the
 *      InfoBlock of each ilog file in reverse order.
 *  1.2 The core logic is as follows:
 *      First, build a search_map of the execution process based on the information in the request:
 *      partition_key => search_status.
 *      search_status records the status of scanning the current ilog file in the process of
 *      scanning InfoBlock from back to front.
 *      See the code for the definition of SearchStatus, which has detailed comments.
 *      Every time an ilog file InfoBlock is scanned,
 *      it is judged for the partition that has not been positioned at the moment.
 *      If the min_log_ts of the partition in this InfoBlock is less than start_timestamp,
 *      the partition is safe enough to pull logs from the corresponding min_log_id.
 *      Otherwise continue to scan the previous ilog file.
 *
 * 2 Support breakpoint
 *    Since there may be too many ilog files to be scanned
 *    (for example, liboblog locates the wrong machine, or liboblog startup time is too old),
 *    The entire process will take too long due to more disk reads, and RPC will time out.
 *    To solve this problem, logservice supports to exit in time before the scan is about to time out.
 *    ObExtRpcQit will record information related to timely exit,
 *    such as the timeout deadline and the time reserved in advance.
 *    In the scanning process, after scanning a file every time,
 *    it is judged according to the information of ObExtRpcQit whether to exit in time.
 *    If exit in time, mark the result of the partition that has not been positioned as UNFINISH,
 *    and bring the ilog file id that ended last time. The liboblog side will re-send the request,
 *    and logservice will scan forward after the last ilog file id which recorded.
 *
 * 3. handle cold partition
 *  3.1 Considering no write on the partition, if use current timestamp to locate the starting start_log_id,
 *      In fact, the log_id of the next log can be returned. For specific logic,
 *      please refer to handle_cold_pkey_by_sw, which has detailed notes.
 *  3.2 Another case is according to the InfoBlock of the last file (this InfoBlock has not been written to disk,
 *      only in memory), refer to handle_cold_pkey_by_last_info, which has detailed notes.
 */

// Qit for quit in time
class ObExtRpcQit {
public:
  ObExtRpcQit() : deadline_(common::OB_INVALID_TIMESTAMP)
  {}
  int init(const int64_t deadline);
  // check should_hurry_quit when start to perform a time-consuming operation
  bool should_hurry_quit() const;
  TO_STRING_KV(K(deadline_));

private:
  static const int64_t RESERVED_INTERVAL = 1 * 1000 * 1000;  // 1 second
  int64_t deadline_;
};

// Locate log_id based on the timestamp, and record the information of
// each partition during the scanning process.
struct SearchStatus {
  explicit SearchStatus(const int64_t start_ts)
      : start_ts_(start_ts),
        err_(common::OB_SUCCESS),
        start_log_id_(common::OB_INVALID_ID),
        start_log_ts_(common::OB_INVALID_TIMESTAMP),
        min_greater_log_id_(common::OB_INVALID_ID),
        min_greater_log_ts_(common::OB_INVALID_TIMESTAMP),
        finished_(false)
  {}

  // liboblog startup timestamp
  const int64_t start_ts_;

  // per-pkey err code
  int err_;

  // Starting log id and timestamp of successful positioning
  uint64_t start_log_id_;
  int64_t start_log_ts_;

  // liboblog startup timestamp start_ts is very small,
  // even the smallest log timestamp of this server is larger than start_ts_.
  // At this time, in order not to leak the log, a safe start_log_id cannot be given.
  // The error code of this partition is OB_ERR_OUT_OF_LOWER_BOUND,
  // and start_log_id_ is the smallest known log_id.
  uint64_t min_greater_log_id_;
  int64_t min_greater_log_ts_;

  // Mark of the pkey has completed positioning.
  bool finished_;

  // Mark as has been positioned.
  void mark_finish(const int error_num, const uint64_t start_log_id, const int64_t start_log_ts)
  {
    err_ = error_num;
    start_log_id_ = start_log_id;
    start_log_ts_ = start_log_ts;
    finished_ = true;
  }

  bool is_finished() const
  {
    return finished_;
  }

  void update_min_greater_log(const uint64_t log_id, const int64_t log_ts)
  {
    if (common::OB_INVALID_ID == min_greater_log_id_ || min_greater_log_id_ > log_id) {
      min_greater_log_id_ = log_id;
      min_greater_log_ts_ = log_ts;
    }
  }

  TO_STRING_KV(K_(start_ts), K_(err), K_(start_log_id), K_(start_log_ts), K_(min_greater_log_id),
      K_(min_greater_log_ts), K_(finished));

private:
  DISALLOW_COPY_AND_ASSIGN(SearchStatus);
};

typedef common::ObLinearHashMap<common::ObPartitionKey, SearchStatus*> SearchMap;

// SearchParam is to simplify the code structure and avoid passing too many parameters.
// SearchParam mainly encapsulated some distributors.
struct SearchParam {
public:
  common::PageArena<>* map_allocator_;

  SearchParam() : map_allocator_(NULL)
  {}
  int init(common::PageArena<>* ma)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(ma)) {
      ret = common::OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument", K(ret), KP(ma));
    } else {
      map_allocator_ = ma;
    }
    return ret;
  }
  bool is_valid()
  {
    return (NULL != map_allocator_);
  }
  TO_STRING_KV(KP(map_allocator_));

private:
  DISALLOW_COPY_AND_ASSIGN(SearchParam);
};

class ObExtStartLogLocatorForDir {
public:
  ObExtStartLogLocatorForDir() : is_inited_(false), partition_service_(NULL), log_engine_(NULL)
  {}
  ~ObExtStartLogLocatorForDir()
  {
    destroy();
  }
  int init(storage::ObPartitionService* partition_service, clog::ObILogEngine* log_engine);
  void destroy();
  int locator_req(const ObLocateByTsReq& req_msg, ObLocateByTsResp& result, bool& is_hurry_quit);

private:
  inline int64_t get_rpc_deadline() const
  {
    return THIS_WORKER.get_timeout_ts();
  }
  int build_search_map(const ObLocateByTsReq& req_msg, SearchParam& search_param, SearchMap& search_map);
  int get_last_slide_log(const common::ObPartitionKey& pkey, uint64_t& last_log_id, int64_t& last_ts);
  int handle_cold_pkey_by_sw_(const common::ObPartitionKey& pkey, SearchStatus& search_status);
  int locate_from_last_file_(const common::ObPartitionKey& pkey, const int64_t start_ts, const uint64_t min_log_id,
      SearchStatus& search_status);
  int locate_pkey_from_flying_ilog_(const common::ObPartitionKey& pkey, SearchStatus& search_status);
  int locate_pkey_from_ilog_storage_(const common::ObPartitionKey& pkey, SearchStatus& search_status);
  int handle_when_locate_out_of_lower_bound_(const ObPartitionKey& pkey, const SearchStatus& search_status,
      uint64_t& target_log_id, int64_t& target_log_timestamp, int& locate_err);
  int build_search_result(const ObLocateByTsReq& req, SearchMap& search_map, ObLocateByTsResp& resp);
  int do_locate_partition_(const common::ObPartitionKey& pkey, SearchStatus& search_status);
  int do_locator_req(
      const ObLocateByTsReq& req_msg, ObLocateByTsResp& response, const ObExtRpcQit& qit, bool& is_hurry_quit);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExtStartLogLocatorForDir);

private:
  bool is_inited_;
  storage::ObPartitionService* partition_service_;
  clog::ObILogEngine* log_engine_;
};

class ObExtStartLogLocator {
public:
  ObExtStartLogLocator() : is_inited_(false), partition_service_(NULL), log_engine_(NULL)
  {}

  ~ObExtStartLogLocator()
  {
    destroy();
  }
  void destroy()
  {
    is_inited_ = false;
    partition_service_ = NULL;
    log_engine_ = NULL;
  }
  int init(storage::ObPartitionService* partition_service, clog::ObILogEngine* log_engine);
  int req_start_log_id_by_ts_with_breakpoint(const ObLocateByTsReq& req_msg, ObLocateByTsResp& result);

private:
  int do_req_start_log_id(const ObLocateByTsReq& req, ObLocateByTsResp& resp);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExtStartLogLocator);

private:
  bool is_inited_;
  storage::ObPartitionService* partition_service_;
  clog::ObILogEngine* log_engine_;
};

}  // namespace logservice
}  // namespace oceanbase

#endif
