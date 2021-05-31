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

#include "ob_external_start_log_locator.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_pg_storage.h"
#include "clog/ob_log_file_pool.h"
#include "clog/ob_log_reader_interface.h"
#include "clog/ob_i_log_engine.h"
#include "clog/ob_log_reader_interface.h"
#include "clog/ob_partition_log_service.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace clog;
using namespace storage;
namespace logservice {

int ObExtRpcQit::init(const int64_t deadline)
{
  int ret = OB_SUCCESS;
  // a small engouh timestamp, used to avoid misuse time_interval and timestamp
  const int64_t BASE_DEADLINE = 1000000000000000;  // 2001-09-09
  if (OB_UNLIKELY(deadline <= BASE_DEADLINE)) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "invalid deadline", K(ret), K(deadline));
  } else {
    deadline_ = deadline;
  }
  return ret;
}

bool ObExtRpcQit::should_hurry_quit() const
{
  bool bool_ret = false;

  if (OB_LIKELY(common::OB_INVALID_TIMESTAMP != deadline_)) {
    int64_t now = ObTimeUtility::current_time();
    bool_ret = (now > deadline_ - RESERVED_INTERVAL);
  }

  return bool_ret;
}

int ObExtStartLogLocatorForDir::init(ObPartitionService* partition_service, ObILogEngine* log_engine)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service) || OB_ISNULL(log_engine)) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "ObExtStartLogLocatorForDir init error", K(ret), KP(partition_service), KP(log_engine));
  } else {
    partition_service_ = partition_service;
    log_engine_ = log_engine;
    is_inited_ = true;
  }
  return ret;
}

void ObExtStartLogLocatorForDir::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    partition_service_ = NULL;
    log_engine_ = NULL;
  }
}

int ObExtStartLogLocatorForDir::build_search_map(
    const ObLocateByTsReq& req_msg, SearchParam& search_param, SearchMap& search_map)
{
  int ret = OB_SUCCESS;
  const ObLocateByTsReq::ParamArray& params = req_msg.get_params();
  const int64_t count = params.count();

  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    const ObLocateByTsReq::Param& param = params[i];
    const ObPartitionKey& pkey = param.pkey_;
    char* buf = static_cast<char*>(search_param.map_allocator_->alloc(sizeof(SearchStatus)));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      EXTLOG_LOG(ERROR, "log external executor alloc memory error", K(ret));
    } else {
      SearchStatus* search_status = new (buf) SearchStatus(param.start_tstamp_);
      if (OB_FAIL(search_map.insert(pkey, search_status))) {
        EXTLOG_LOG(WARN, "search map insert error", K(ret), K(pkey));
      } else {
        EXTLOG_LOG(TRACE, "search map insert success", K(pkey), KPC(search_status));
      }
    }
  }  // end for

  return ret;
}

int ObExtStartLogLocatorForDir::get_last_slide_log(const ObPartitionKey& pkey, uint64_t& last_log_id, int64_t& last_ts)
{
  int ret = OB_SUCCESS;
  ObIPartitionLogService* pls = NULL;
  ObIPartitionGroupGuard guard;
  uint64_t start_log_id = OB_INVALID_ID;
  uint64_t end_log_id = OB_INVALID_ID;
  int64_t start_ts = OB_INVALID_TIMESTAMP;
  int64_t end_ts = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(partition_service_->get_partition(pkey, guard)) || NULL == guard.get_partition_group() ||
      NULL == (pls = guard.get_partition_group()->get_log_service())) {
    ret = OB_PARTITION_NOT_EXIST;
  } else if (!(guard.get_partition_group()->is_valid())) {
    ret = OB_INVALID_PARTITION;
  } else if (guard.get_partition_group()->get_pg_storage().is_restore()) {
    ret = OB_STATE_NOT_MATCH;
    EXTLOG_LOG(ERROR, "partition group is not in normal status", KR(ret), K(pkey));
  } else {
    if (OB_FAIL(pls->get_log_id_range(start_log_id, start_ts, end_log_id, end_ts))) {
      EXTLOG_LOG(WARN, "get log id range from pls error", K(ret), K(pkey));
    } else {
      last_log_id = end_log_id;
      last_ts = end_ts;
      if (OB_UNLIKELY(OB_INVALID_ID == last_log_id) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == last_ts)) {
        // sw has initialized last_slide to last_replay_log_id/ts
        ret = OB_ERR_UNEXPECTED;
        EXTLOG_LOG(ERROR, "get invalid last log id or ts", K(ret), K(last_log_id), K(last_ts));
      }
    }
  }
  return ret;
}

int ObExtStartLogLocatorForDir::handle_cold_pkey_by_sw_(const ObPartitionKey& pkey, SearchStatus& search_status)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = search_status.start_ts_;
  uint64_t last_slide_log_id = OB_INVALID_ID;
  int64_t last_slide_log_ts = OB_INVALID_TIMESTAMP;
  if (OB_FAIL(get_last_slide_log(pkey, last_slide_log_id, last_slide_log_ts))) {
    if (OB_PARTITION_NOT_EXIST == ret || OB_INVALID_PARTITION == ret) {
      EXTLOG_LOG(INFO, "get_last_slide_log partition not exist or invalid", K(ret), K(pkey));
      // partition not exists, not cold partition. Confirm by scanning InfoBlock
      ret = OB_SUCCESS;
    } else {
      EXTLOG_LOG(WARN, "get_last_slide_log error", K(ret), K(pkey));
    }
  } else if (last_slide_log_ts <= start_ts) {
    // If the target timestamp is greater than the maximum log timestamp,
    // it is safe to return the next log as the startup log.
    // BAD CASE is: if visited server is a backward backup machine, there will be many rollbacks
    //
    // analysis of several situations:
    // Note 1:
    // liboblog will pull logs from start_log_id (including this start_log_id),
    // last_slide_log_id is a safe result for liboblog, actually, even last_slide_log_id + 1 is safe as well.
    // Here should return last_slide_log_id + 1 as result.
    // Because for cold partitions, pulling the last_slide_log_id log may trigger IlogStorage to load older files.
    // If there are more cold partitions, it may cause serious IlogStorage pollution and poor locality.
    //
    // Note 2:
    // liboblog should ask the master first, or should ask all copies,
    // Otherwise, if you ask about a backward standby machine,
    // it may cause the given security log_id to be too small.
    //
    // Note 3:
    // For the following Corner Case, the code logic is also applicable:
    // The partition has just been created, and no logs have been slid out.
    // At this time, last_slide_log_id = 0, last_slide_log_ts = 0.
    uint64_t start_log_id = last_slide_log_id + 1;
    int64_t start_log_ts = last_slide_log_ts;

    search_status.mark_finish(OB_SUCCESS, start_log_id, start_log_ts);

    EXTLOG_LOG(INFO,
        "cold pkey locate success, last_slide_log_id is safe",
        K(pkey),
        K(search_status),
        K(last_slide_log_id),
        K(last_slide_log_ts));
  } else {
    // The maximum log timestamp of the partition is greater than the target timestamp, set the upper bound value
    search_status.update_min_greater_log(last_slide_log_id, last_slide_log_ts);

    EXTLOG_LOG(TRACE,
        "not cold pkey, last_slide_log_ts > start_ts",
        K(pkey),
        K(start_ts),
        K(last_slide_log_ts),
        K(last_slide_log_id),
        K(search_status));
  }
  return ret;
}

int ObExtStartLogLocatorForDir::locate_from_last_file_(
    const ObPartitionKey& pkey, const int64_t start_ts, const uint64_t min_log_id, SearchStatus& search_status)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!pkey.is_valid()) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == start_ts) ||
      OB_UNLIKELY(OB_INVALID_ID == min_log_id)) {
    EXTLOG_LOG(WARN, "invalid argument", K(pkey), K(start_ts), K(min_log_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool finished = false;

    // Polling all logs of log_index, find the first log greater than or equal to start_ts
    for (uint64_t cur_log_id = min_log_id; OB_SUCCESS == ret && !finished; cur_log_id++) {
      ObLogCursorExt cursor;

      if (OB_FAIL(log_engine_->get_cursor(pkey, cur_log_id, cursor))) {
        if (OB_ERR_OUT_OF_LOWER_BOUND == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret || OB_PARTITION_NOT_EXIST == ret) {
          EXTLOG_LOG(TRACE, "get_cursor out of range", K(ret), K(pkey), K(cur_log_id), K(min_log_id), K(start_ts));
        } else {
          EXTLOG_LOG(WARN, "pls get_cursor fail", K(ret), K(pkey), K(cur_log_id));
        }
      } else {
        // Find the first log greater than or equal to the start timestamp
        if (cursor.get_submit_timestamp() >= start_ts) {
          search_status.mark_finish(OB_SUCCESS, cur_log_id, cursor.get_submit_timestamp());
          finished = true;
        }
      }
    }
  }

  return ret;
}

// Locate the log id from the last ILOG
//
// If a file is cut during execution, new flying InfoBlock can be treated as an old file simply.
// When locating from IlogStorage later, the target file will be scanned, so it is safe not to process it here.
int ObExtStartLogLocatorForDir::locate_pkey_from_flying_ilog_(const ObPartitionKey& pkey, SearchStatus& search_status)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = search_status.start_ts_;
  uint64_t info_min_log_id = OB_INVALID_ID;
  int64_t info_min_ts = OB_INVALID_TIMESTAMP;

  if (OB_ISNULL(log_engine_)) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(ERROR, "not init", K(ret), KP(log_engine_));
  } else {
    if (OB_FAIL(log_engine_->get_ilog_memstore_min_log_id_and_ts(pkey, info_min_log_id, info_min_ts))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        // partition not exist, expected
        ret = OB_SUCCESS;
        EXTLOG_LOG(INFO, "get flying ilog info block info fail, partition not exist", K(pkey));
      } else {
        EXTLOG_LOG(WARN, "get_min_log_id from flying info block fail", K(ret), K(pkey));
      }
    } else if (OB_UNLIKELY(OB_INVALID_ID == info_min_log_id) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == info_min_ts)) {
      // return value invalid
      EXTLOG_LOG(WARN, "invalid flying ilog info block info", K(pkey), K(info_min_log_id), K(info_min_ts));
      ret = OB_INVALID_DATA;
    } else {
      if (start_ts >= info_min_ts) {
        // If the start timestamp falls in the last ilog file, query last_file to locate the log accurately
        if (OB_FAIL(locate_from_last_file_(pkey, start_ts, info_min_log_id, search_status))) {
          EXTLOG_LOG(
              WARN, "locate_from_last_file_ fail", K(ret), K(pkey), K(info_min_log_id), K(start_ts), K(info_min_ts));

          // If the partition does not exist or the log does not exist, the return is OB_SUCCESS,
          // and it is required to locate from IlogStorage
          if (OB_PARTITION_NOT_EXIST == ret || OB_INVALID_PARTITION == ret || OB_ERR_OUT_OF_LOWER_BOUND == ret ||
              OB_ERR_OUT_OF_UPPER_BOUND == ret) {
            ret = OB_SUCCESS;
          }
        } else {
          EXTLOG_LOG(INFO,
              "locate start log id from log index success",
              K(pkey),
              K(search_status),
              K(info_min_log_id),
              K(info_min_ts));
        }
      } else {
        // Keep the smallest log information greater than or equal to the target timestamp
        // If all InfoBlocks are scanned forward and there is no log of this pkey,
        // return this log and mark OB_ERR_OUT_OF_LOWER_BOUND at well.
        search_status.update_min_greater_log(info_min_log_id, info_min_ts);
        EXTLOG_LOG(
            TRACE, "find min greater log from flying ilog", K(pkey), K(start_ts), K(info_min_ts), K(info_min_log_id));
      }
    }
  }
  return ret;
}

int ObExtStartLogLocatorForDir::locate_pkey_from_ilog_storage_(const ObPartitionKey& pkey, SearchStatus& search_status)
{
  int ret = OB_SUCCESS;
  int locate_err = OB_SUCCESS;
  uint64_t target_log_id = OB_INVALID_ID;
  int64_t target_log_timestamp = OB_INVALID_TIMESTAMP;
  const int64_t SLEEP_FOR_RETRY = 10 * 1000;  // 10ms
  const int64_t RETRY_CNT = 10;
  int64_t start_ts = search_status.start_ts_;

  if (OB_ISNULL(log_engine_)) {
    ret = OB_NOT_INIT;
  } else {
    int64_t retry_times = 0;
    int64_t start_time = ObTimeUtility::current_time();

    // locate from IlogStorage
    // If OB_NEED_RETRY is returned, it will retry locally,
    // if the fixed number of retry is still invalid, return OB_EXT_HANDLE_UNFINISH.
    do {
      ret = log_engine_->locate_by_timestamp(pkey, start_ts, target_log_id, target_log_timestamp);

      if (OB_NEED_RETRY == ret) {
        EXTLOG_LOG(WARN,
            "cursor cache locate_by_timestamp need retry",
            K(ret),
            K(pkey),
            K(start_ts),
            K(target_log_id),
            K(target_log_timestamp),
            K(retry_times));
        usleep(SLEEP_FOR_RETRY);
      }
    } while (OB_NEED_RETRY == ret && ++retry_times < RETRY_CNT);

    int64_t end_time = ObTimeUtility::current_time();

    // set positioning results
    locate_err = ret;

    // handle error code
    if (OB_SUCCESS == ret) {
      EXTLOG_LOG(INFO,
          "locate_by_timestamp from cursor cache success",
          K(pkey),
          K(start_ts),
          K(target_log_id),
          K(target_log_timestamp),
          K(retry_times),
          "locate_time",
          end_time - start_time);
    } else {
      EXTLOG_LOG(WARN,
          "locate_by_timestamp from cursor cache fail",
          K(ret),
          K(pkey),
          K(start_ts),
          K(target_log_id),
          K(target_log_timestamp),
          K(retry_times),
          K(search_status));

      // Continue to deal with cases OB_ERR_OUT_OF_UPPER_BOUND
      // Compare the upper bound value and min_greater_log_id, if it is continuous,
      // It means that the log falls in the last ilog. In this case, min_greater_log_id is returned.
      if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
        if (target_log_id + 1 == search_status.min_greater_log_id_) {
          // update positioning results
          target_log_id = search_status.min_greater_log_id_;
          target_log_timestamp = search_status.min_greater_log_ts_;
          locate_err = OB_SUCCESS;

          EXTLOG_LOG(INFO,
              "locate_by_timestamp success, use first log of flying ilog",
              K(pkey),
              K(start_ts),
              K(target_log_id),
              K(target_log_timestamp),
              K(retry_times),
              K(search_status));
        }
      }
      // partition not exist, continue processing
      else if (OB_PARTITION_NOT_EXIST == ret) {
        // last ilog not served yet, return NO_LOG
        if (OB_INVALID_ID == search_status.min_greater_log_id_) {
          locate_err = OB_NO_LOG;
        } else {
          // last ilog service, return OB_ERR_OUT_OF_LOWER_BOUND
          target_log_id = search_status.min_greater_log_id_;
          target_log_timestamp = search_status.min_greater_log_ts_;
          locate_err = OB_ERR_OUT_OF_LOWER_BOUND;
        }
      }
      // partition needs to be retried will return OB_EXT_HANDLE_UNFINISH status,
      // and the client will continue to retry next loop
      else if (OB_NEED_RETRY == ret) {
        locate_err = OB_EXT_HANDLE_UNFINISH;
      } else {
        // other error codes remain unchanged
      }

      // all error code expected should return OB_SUCCESS
      if (OB_ERR_OUT_OF_LOWER_BOUND == ret || OB_ERR_OUT_OF_UPPER_BOUND == ret || OB_PARTITION_NOT_EXIST == ret ||
          OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
      }
    }

    // Dealing with OB_ERR_OUT_OF_LOWER_BOUND
    if (OB_SUCCESS == ret && OB_ERR_OUT_OF_LOWER_BOUND == locate_err) {
      // Whether the processing is successful or not, should return OB_SUCCESS
      //
      // NOTE: the following functions will modify the positioning results:
      // target_log_id/target_log_timestamp/locate_err
      int tmp_ret =
          handle_when_locate_out_of_lower_bound_(pkey, search_status, target_log_id, target_log_timestamp, locate_err);

      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        EXTLOG_LOG(WARN,
            "handle_when_locate_out_of_lower_bound_ fail",
            K(tmp_ret),
            K(pkey),
            K(locate_err),
            K(target_log_timestamp),
            K(target_log_id));
      }
    }

    // Finally, set ending regardless of success
    search_status.mark_finish(locate_err, target_log_id, target_log_timestamp);
  }

  return ret;
}

// When locating based on the timestamp to be OB_ERR_OUT_OF_LOWER_BOUND,
// it means that all logs less than target_log_id will be recycled.
// In order to reduce the probability of positioning return failure,
// the log information recorded in storage info is verified.
// If the log recorded in storage info is continuous with the lower bound log,
// and the location timestamp falls between the two logs, means the positioning is successful.
int ObExtStartLogLocatorForDir::handle_when_locate_out_of_lower_bound_(const ObPartitionKey& pkey,
    const SearchStatus& search_status, uint64_t& target_log_id, int64_t& target_log_timestamp, int& locate_err)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard guard;
  ObIPartitionGroup* partition = NULL;
  int64_t start_ts = search_status.start_ts_;
  ObBaseStorageInfo clog_info;
  int old_locate_err = locate_err;

  if (OB_UNLIKELY(OB_ERR_OUT_OF_LOWER_BOUND != locate_err) || OB_UNLIKELY(OB_INVALID_ID == target_log_id) ||
      OB_UNLIKELY(OB_INVALID_TIMESTAMP == target_log_timestamp) || OB_UNLIKELY(start_ts > target_log_timestamp)) {
    EXTLOG_LOG(
        ERROR, "invalid argument", K(pkey), K(locate_err), K(target_log_id), K(target_log_timestamp), K(search_status));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(partition_service_)) {
    EXTLOG_LOG(WARN, "partition service is NULL, not init", K(partition_service_));
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(partition_service_->get_partition(pkey, guard)) ||
             (NULL == (partition = guard.get_partition_group()))) {
    // partition not exists, expected, not process.
    ret = OB_SUCCESS;
  } else if (OB_FAIL(partition->get_saved_clog_info(clog_info))) {
    EXTLOG_LOG(WARN, "get_saved_clog_info fail", K(ret), K(pkey));
  } else {
    int64_t base_log_tstamp = clog_info.get_submit_timestamp();
    uint64_t base_log_id = clog_info.get_last_replay_log_id();

    // Check whether the lower bound log of the location is continuous with that recorded in the storage info.
    if (base_log_id == (target_log_id - 1)) {
      // Check whether the location timestamp falls in the middle of the two logs.
      // clog_info < start_ts < target_log_timestamp
      if (start_ts > base_log_tstamp) {
        locate_err = OB_SUCCESS;
      }
      // If the timestamp is exactly equal to the log timestamp in storage info,
      // the log in storage info is returned.
      else if (start_ts == base_log_tstamp) {
        locate_err = OB_SUCCESS;
        target_log_id = base_log_id;
        target_log_timestamp = base_log_tstamp;
      }
    }

    EXTLOG_LOG(INFO,
        "locate_by_timestamp handle OUT_OF_LOWER_BOUND case",
        K(locate_err),
        K(old_locate_err),
        K(pkey),
        K(target_log_id),
        K(target_log_timestamp),
        K(start_ts),
        K(clog_info));
  }
  return ret;
}

int ObExtStartLogLocatorForDir::build_search_result(
    const ObLocateByTsReq& req, SearchMap& search_map, ObLocateByTsResp& resp)
{
  int ret = OB_SUCCESS;
  const ObLocateByTsReq::ParamArray& params = req.get_params();
  const int64_t count = params.count();
  ObLocateByTsResp::Result result;

  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    const ObPartitionKey& pkey = params[i].pkey_;
    SearchStatus* search_status = NULL;

    if (OB_FAIL(search_map.get(pkey, search_status))) {
      EXTLOG_LOG(WARN, "search map get error", K(ret), K(pkey), KP(search_status));
    } else if (OB_ISNULL(search_status)) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "search map get null value", K(ret), K(pkey));
    } else {
      result.reset(search_status->err_, search_status->start_log_id_, search_status->start_log_ts_);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(resp.append_result(result))) {
          EXTLOG_LOG(WARN, "locate by ts resp append result error", K(ret), K(pkey), K(result));
        } else {
          EXTLOG_LOG(INFO, "locate_by_timestamp append result success", K(pkey), K(result));
        }
      }
    }
  }  // end for

  if (OB_SUCC(ret)) {
    EXTLOG_LOG(INFO, "locate_by_timestamp build result success", K(count));
  }
  return ret;
}

int ObExtStartLogLocatorForDir::do_locate_partition_(const common::ObPartitionKey& pkey, SearchStatus& search_status)
{
  int ret = OB_SUCCESS;
  int64_t begin_ts = ObTimeUtility::current_time();

  // step 1: prioritize cold partitions based on the largest log
  // These cold partitions do not write logs for a long time,
  // and the target timestamp is likely to be greater than the last log timestamp
  // In this case, return to the next service log.
  if (OB_FAIL(handle_cold_pkey_by_sw_(pkey, search_status))) {
    EXTLOG_LOG(WARN, "handle_cold_pkey_by_sw_ fail", K(ret), K(pkey), K(search_status));
    search_status.mark_finish(ret, OB_INVALID_ID, OB_INVALID_TIMESTAMP);
  }
  // step 2: locate from last ilog
  else if (!search_status.is_finished() && OB_FAIL(locate_pkey_from_flying_ilog_(pkey, search_status))) {
    EXTLOG_LOG(WARN, "locate_pkey_from_flying_ilog_ fail", K(ret), K(pkey), K(search_status));
  }
  // step 3: locate from IlogStorage
  else if (!search_status.is_finished() && OB_FAIL(locate_pkey_from_ilog_storage_(pkey, search_status))) {
    EXTLOG_LOG(WARN, "locate_pkey_from_ilog_storage_ fail", K(ret), K(pkey), K(search_status));
  }
  // status must be marked as complete finally
  else if (OB_UNLIKELY(!search_status.is_finished())) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(WARN,
        "locate_by_timestamp unexpected error, partition does not mark finished",
        K(ret),
        K(pkey),
        K(search_status));
  } else {
    // success
  }

  int64_t end_ts = ObTimeUtility::current_time();
  EXTLOG_LOG(TRACE,
      "partition locate_by_timestamp finish",
      K(ret),
      K(pkey),
      K(search_status),
      "locate_time",
      end_ts - begin_ts);

  return ret;
}

int ObExtStartLogLocatorForDir::do_locator_req(
    const ObLocateByTsReq& req_msg, ObLocateByTsResp& resp, const ObExtRpcQit& qit, bool& is_hurry_quit)
{
  int ret = OB_SUCCESS;
  SearchMap search_map;
  PageArena<> map_allocator;
  SearchParam search_param;

  search_map.init(ObModIds::OB_CLOG_EXT_RPC);

  is_hurry_quit = false;

  // prepare data
  if (OB_FAIL(search_param.init(&map_allocator))) {
    EXTLOG_LOG(WARN, "search param init error", K(ret));
  } else if (OB_FAIL(build_search_map(req_msg, search_param, search_map))) {
    EXTLOG_LOG(WARN, "build search map error", K(ret), K(req_msg));
  } else {
    SearchMap::BlurredIterator iter(search_map);
    ObPartitionKey pkey;
    SearchStatus* search_status = NULL;

    while (OB_SUCCESS == ret && OB_SUCCESS == (ret = iter.next(pkey, search_status))) {
      if (OB_ISNULL(search_status)) {
        ret = OB_ERR_UNEXPECTED;
        EXTLOG_LOG(ERROR, "get null search status", K(ret), K(pkey), K(search_status));
      } else {
        // If exit halfway, mark every subsequent partition with unfinished error code
        if (OB_UNLIKELY(is_hurry_quit = qit.should_hurry_quit())) {
          search_status->mark_finish(OB_EXT_HANDLE_UNFINISH, OB_INVALID_ID, OB_INVALID_TIMESTAMP);
          EXTLOG_LOG(
              INFO, "partition locate_by_timestamp mark unfinish", K(ret), K(pkey), K(search_status), K(is_hurry_quit));
        }
        // locate a partition, and set the result regardless of success or not
        else if (OB_FAIL(do_locate_partition_(pkey, *search_status))) {
          EXTLOG_LOG(WARN, "do_locate_partition_ fail", K(ret), K(pkey), KPC(search_status));
        } else {
          // success
        }
      }
    }  // end while

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  // build result
  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_search_result(req_msg, search_map, resp))) {
      EXTLOG_LOG(WARN, "build result error", K(ret), K(req_msg));
    }
  }

  (void)search_map.destroy();
  map_allocator.free();
  return ret;
}

int ObExtStartLogLocatorForDir::locator_req(const ObLocateByTsReq& req_msg, ObLocateByTsResp& resp, bool& is_hurry_quit)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "log external executor not init", K(ret), K(req_msg));
  } else if (OB_UNLIKELY(!req_msg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "ObLocateByTsReq is not valid", K(ret), K(req_msg));
  } else {
    ObExtRpcQit qit;
    if (OB_FAIL(qit.init(get_rpc_deadline()))) {
      EXTLOG_LOG(WARN, "init qit error", K(ret));
    } else if (OB_FAIL(do_locator_req(req_msg, resp, qit, is_hurry_quit))) {
      EXTLOG_LOG(WARN, "do locator request error", K(ret), K(req_msg), K(resp), K(qit), K(is_hurry_quit));
    } else {
      EXTLOG_LOG(TRACE, "do locator success", K(req_msg), K(resp), K(qit), K(is_hurry_quit));
    }
  }

  if (OB_SUCC(ret)) {
    resp.set_err(OB_SUCCESS);
  } else {
    resp.set_err(OB_ERR_SYS);
  }
  return ret;
}

int ObExtStartLogLocator::init(ObPartitionService* partition_service, clog::ObILogEngine* log_engine)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service) || OB_ISNULL(log_engine)) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "invalid partition_service_ or log_engine", K(ret), KP(partition_service), KP(log_engine));
  } else {
    partition_service_ = partition_service;
    log_engine_ = log_engine;
    is_inited_ = true;
  }
  return ret;
}

int ObExtStartLogLocator::do_req_start_log_id(const ObLocateByTsReq& req, ObLocateByTsResp& resp)
{
  int ret = OB_SUCCESS;
  ObExtStartLogLocatorForDir locator_impl;

  bool is_hurry_quit = false;
  if (0 == req.get_params().count()) {
    EXTLOG_LOG(INFO, "no partition in request", K(req));
  } else if (OB_FAIL(locator_impl.init(partition_service_, log_engine_))) {
    EXTLOG_LOG(WARN, "locator_impl init error", K(ret));
  } else if (OB_FAIL(locator_impl.locator_req(req, resp, is_hurry_quit))) {
    EXTLOG_LOG(WARN, "locator req error", K(ret), K(req), K(resp));
  } else {
    locator_impl.destroy();
    EXTLOG_LOG(INFO, "locator req success", K(req), K(resp), K(is_hurry_quit));
  }
  return ret;
}

int ObExtStartLogLocator::req_start_log_id_by_ts_with_breakpoint(
    const ObLocateByTsReq& req_msg, ObLocateByTsResp& result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "log external executor not init", K(ret), K(req_msg));
  } else if (OB_UNLIKELY(!req_msg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "ObLocateByTsReq is not valid", K(ret), K(req_msg));
  } else {
    if (OB_FAIL(do_req_start_log_id(req_msg, result))) {
      EXTLOG_LOG(WARN, "do req_start_log_id error", K(ret), K(req_msg), K(result));
    } else {
      EXTLOG_LOG(INFO, "do req_start_log_id success", K(ret), K(req_msg), K(result));
    }
  }
  if (OB_SUCC(ret)) {
    result.set_err(OB_SUCCESS);
  } else {
    result.set_err(OB_ERR_SYS);
  }
  return ret;
}

}  // namespace logservice
}  // namespace oceanbase
