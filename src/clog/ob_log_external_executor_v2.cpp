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

#include "storage/ob_partition_service.h"
#include "ob_log_external_executor_v2.h"
#include "ob_log_define.h"
#include "ob_log_external_monitor.h"
#include "ob_log_external_qit.h"
#include "ob_partition_log_service.h"

namespace oceanbase {
using namespace common;
using namespace storage;
using namespace obrpc;
using namespace clog;
namespace extlog {

int ObLogExternalExecutorWithBreakpoint::init(const ObLogExternalExecutorWithBreakpoint::Config& config,
    ObILogEngine* log_engine, ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!config.is_valid()) || OB_UNLIKELY(NULL == log_engine) ||
             OB_UNLIKELY(NULL == partition_service)) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(
        WARN, "ObLogExternalExecutorWithBreakpoint init error, invalid argument", K(ret), K(config), KP(log_engine));
  } else {
    config_ = config;
    log_engine_ = log_engine;
    partition_service_ = partition_service;
    ObExtReqStatistic::reset_all();
    is_inited_ = true;
    EXTLOG_LOG(INFO, "log external executor init success", K(config), KP(log_engine_));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::init_search_status(
    SearchStatus* search_status, const ObLogReqStartLogIdByTsRequestWithBreakpoint::Param& param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(search_status)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_FAIL(search_status->init(SST_BY_TS))) {
      EXTLOG_LOG(WARN, "search status init error", K(ret));
    } else if (OB_FAIL(search_status->set_start_ts(param.start_tstamp_))) {
      EXTLOG_LOG(WARN, "search status set start_ts error", K(ret), "start_ts", param.start_tstamp_);
    } else {
      // always set min_greater_log_id_ no matter it is invalid or not
      search_status->search_by_ts_.min_greater_log_id_ = param.break_info_.min_greater_log_id_;

      if (OB_INVALID_ID != param.break_info_.min_greater_log_id_) {
        EXTLOG_LOG(TRACE, "min_greater_log_id_", K(param));
      }
    }
    EXTLOG_LOG(TRACE, "init by_ts search_status", K(ret), K(param));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::init_search_status(
    SearchStatus* search_status, const ObLogReqStartPosByLogIdRequestWithBreakpoint::Param& param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(search_status)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_FAIL(search_status->init(SST_BY_ID))) {
      EXTLOG_LOG(WARN, "search status init error", K(ret));
    } else if (OB_FAIL(search_status->set_start_log_id(param.start_log_id_))) {
      EXTLOG_LOG(WARN, "search status set start_ts error", K(ret), "start_log_id", param.start_log_id_);
    }
    EXTLOG_LOG(TRACE, "init by_id search_status", K(ret), K(param));
  }
  return ret;
}

template <class ReqMsg>
int ObLogExternalExecutorWithBreakpoint::build_search_map(
    const ReqMsg& req_msg, SearchParam& search_param, SearchMap& search_map, Progress& progress)
{
  int ret = OB_SUCCESS;
  const typename ReqMsg::ParamArray& params = req_msg.get_params();
  int64_t count = params.count();
  file_id_t max_break_file_id = OB_INVALID_FILE_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    const typename ReqMsg::Param& param = params[i];
    const ObPartitionKey& pkey = param.pkey_;
    char* buf = static_cast<char*>(search_param.map_allocator_->alloc(sizeof(SearchStatus)));
    if (OB_UNLIKELY(NULL == buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      EXTLOG_LOG(ERROR, "log external executor alloc memory error", K(ret));
    } else {
      SearchStatus* search_status = new (buf) SearchStatus();
      if (OB_FAIL(init_search_status(search_status, params[i]))) {
        EXTLOG_LOG(WARN, "init search status error", K(ret), K(i), "param", params[i]);
      } else if (OB_FAIL(search_map.insert(pkey, search_status))) {
        EXTLOG_LOG(ERROR, "search map insert error", K(ret), K(pkey));
      } else {
        EXTLOG_LOG(TRACE, "search map insert success", K(pkey));
      }
    }
  }  // end for

  // Determine the breakpoint, take max
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    const typename ReqMsg::Param& param = params[i];
    const file_id_t break_file_id = param.break_info_.break_file_id_;
    if (OB_INVALID_FILE_ID == break_file_id) {
      max_break_file_id = OB_INVALID_FILE_ID;
      break;
    } else if (OB_INVALID_FILE_ID == max_break_file_id || max_break_file_id < break_file_id) {
      max_break_file_id = break_file_id;
    } else {
      // do nothing
    }
  }
  if (OB_SUCC(ret)) {
    EXTLOG_LOG(INFO, "get max_break_file_id", K(max_break_file_id));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(progress_set_file_id_range(progress))) {
      EXTLOG_LOG(WARN, "progress set file_id_rangeerror", K(ret), K(progress));
    } else if (OB_FAIL(progress.set_max_break_file_id(max_break_file_id))) {
      EXTLOG_LOG(WARN, "progress set max_file_id error", K(ret), K(progress), K(max_break_file_id));
    } else if (OB_FAIL(progress.set_total(count))) {
      EXTLOG_LOG(WARN, "progress set total error", K(ret), K(progress), K(count));
    } else {
      EXTLOG_LOG(INFO, "progress init success", K(progress), K(max_break_file_id));
    }
  }

  if (OB_SUCC(ret)) {
    EXTLOG_LOG(INFO, "build search_map success", K(progress));
  } else {
    EXTLOG_LOG(WARN, "build search_map error", K(ret), K(progress));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::after_scan(
    SearchMap& search_map, SearchParam& search_param, Progress& progress)
{
  int ret = OB_SUCCESS;
  SearchMap::BlurredIterator iter(search_map);
  ObPartitionKey pkey;
  SearchStatus* search_status = NULL;
  while (OB_SUCC(ret) && OB_SUCC(iter.next(pkey, search_status)) && !progress.all_finished()) {
    if (OB_ISNULL(search_status)) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "get null search_status");
    } else if (!search_status->is_finished()) {
      if (search_param.is_by_ts()) {
        if (OB_INVALID_ID == search_status->search_by_ts_.min_greater_log_id_) {
          search_status->search_by_ts_.err_ = OB_NO_LOG;
        } else {
          search_status->search_by_ts_.err_ = OB_ERR_OUT_OF_LOWER_BOUND;
          search_status->search_by_ts_.start_log_id_ = search_status->search_by_ts_.min_greater_log_id_;
        }
        search_status->set_finished();
        progress.finish_one();
        EXTLOG_LOG(TRACE,
            "ts after_scan",
            K(pkey),
            "err_code",
            search_status->search_by_ts_.err_,
            "start_log_id",
            search_status->search_by_ts_.start_log_id_);
      } else if (search_param.is_by_id()) {
        search_status->search_by_id_.err_ = OB_ERR_OUT_OF_LOWER_BOUND;
        search_status->search_by_id_.res_file_id_ = progress.cur_file_id_;
        search_status->set_finished();
        progress.finish_one();
        EXTLOG_LOG(TRACE,
            "ts after_scan",
            K(pkey),
            "err_code",
            search_status->search_by_ts_.err_,
            "start_log_id",
            search_status->search_by_ts_.start_log_id_);
      } else {
        ret = OB_ERR_UNEXPECTED;
        EXTLOG_LOG(ERROR, "unknown type", K(ret), K(search_param));
      }
    } else {
      // already finished
      // do nothing
    }
  }  // end while
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    EXTLOG_LOG(INFO, "after_scan success");
  } else {
    EXTLOG_LOG(WARN, "after_scan error", K(ret));
  }
  return ret;
}

/*
// for test
bool mock_quit(const file_id_t cur, const file_id_t start)
{
  bool q = false;
  q = (start - cur == 2);
  EXTLOG_LOG(INFO, "quit?", K(cur), K(start), K(q));
  return q;
}
*/

int ObLogExternalExecutorWithBreakpoint::progress_set_file_id_range(Progress& progress)
{
  int ret = OB_SUCCESS;
  file_id_t min_file_id = OB_INVALID_FILE_ID;
  file_id_t max_file_id = OB_INVALID_FILE_ID;
  if (OB_FAIL(log_engine_->get_ilog_file_id_range(min_file_id, max_file_id))) {
    EXTLOG_LOG(WARN, "get file id range error", K(ret), K(min_file_id), K(max_file_id));
  } else if (OB_INVALID_FILE_ID == min_file_id || OB_INVALID_FILE_ID == max_file_id) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(WARN, "min or max file id is not valid", K(ret), K(min_file_id), K(max_file_id));
  } else if (OB_FAIL(progress.set_file_id_range(min_file_id, max_file_id))) {
    EXTLOG_LOG(WARN, "progress set_file_id_range error", K(ret), K(min_file_id), K(max_file_id));
  } else {
    // do nothing
  }
  if (OB_SUCC(ret)) {
    EXTLOG_LOG(INFO, "progress set_file_id_range success", K(progress));
  } else {
    EXTLOG_LOG(WARN, "progress set_file_id_range error", K(ret), K(progress));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::process_info_entry(const ObPartitionKey& pkey, const uint64_t min_log_id,
    const int64_t submit_timestamp, SearchParam& search_param, SearchStatus* search_status, Progress& progress)
{
  EXTLOG_LOG(TRACE, "process info entry begin", K(min_log_id), K(submit_timestamp), K(progress));
  int ret = OB_SUCCESS;
  const uint64_t log_id_in_info = min_log_id;
  const int64_t ts_in_info = submit_timestamp;
  if (OB_ISNULL(search_status)) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "search status is null", K(ret));
  } else if (search_param.is_by_ts()) {
    const uint64_t ts_in_req = search_status->get_start_ts();
    if (ts_in_info <= ts_in_req) {
      search_status->search_by_ts_.err_ = OB_SUCCESS;
      search_status->search_by_ts_.start_log_id_ = log_id_in_info;
      search_status->search_by_ts_.start_log_ts_ = ts_in_info;
      search_status->set_finished();
      progress.finish_one();
      EXTLOG_LOG(INFO, "search by ts, record safe log id and ts", K(min_log_id), K(submit_timestamp), K(progress));
    } else {
      search_status->search_by_ts_.min_greater_log_id_ = log_id_in_info;
      EXTLOG_LOG(TRACE, "record min_greater_log_id", K(pkey), K(log_id_in_info));
    }
  } else if (search_param.is_by_id()) {
    const int64_t log_id_in_req = search_status->get_start_log_id();
    if (log_id_in_info <= log_id_in_req) {
      search_status->search_by_id_.err_ = OB_SUCCESS;
      search_status->search_by_id_.res_file_id_ = progress.cur_file_id_;
      search_status->set_finished();
      progress.finish_one();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "invalid search type", K(ret), K(search_param));
  }
  if (OB_SUCC(ret)) {
    EXTLOG_LOG(TRACE, "process info entry finish succes", K(min_log_id), K(submit_timestamp), K(progress));
  } else {
    EXTLOG_LOG(WARN, "process info entry finish error", K(ret), K(min_log_id), K(submit_timestamp), K(progress));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::scan_cur_info_block(
    SearchMap& search_map, SearchParam& search_param, Progress& progress)
{
  int ret = OB_SUCCESS;
  IndexInfoBlockMap index_info_block_map;
  if (OB_FAIL(log_engine_->get_index_info_block_map(progress.cur_file_id_, index_info_block_map))) {
    EXTLOG_LOG(WARN, "get_index_info_block_map failed", K(search_param), K(progress));
  } else {
    SearchMap::BlurredIterator iter(search_map);
    ObPartitionKey pkey;
    SearchStatus* search_status = NULL;
    while (OB_SUCCESS == ret && OB_SUCCESS == (ret = iter.next(pkey, search_status))) {
      if (OB_ISNULL(search_status)) {
        ret = OB_ERR_UNEXPECTED;
        EXTLOG_LOG(ERROR, "search status in search map is null", K(ret));
      } else if (!search_status->is_finished()) {
        uint64_t min_log_id = OB_INVALID_ID;
        int64_t submit_timestamp = OB_INVALID_TIMESTAMP;
        if (OB_SUCC(get_min_log_id_and_ts_from_index_info_block_map_(
                index_info_block_map, pkey, min_log_id, submit_timestamp))) {
          if (OB_FAIL(process_info_entry(pkey, min_log_id, submit_timestamp, search_param, search_status, progress))) {
            EXTLOG_LOG(WARN,
                "process info entry error",
                K(ret),
                K(min_log_id),
                K(submit_timestamp),
                K(search_param),
                K(progress));
          }
        } else if (OB_PARTITION_NOT_EXIST == ret) {
          // Currently InfoBlock has no record of pkey, continue
          ret = OB_SUCCESS;
        } else {
          EXTLOG_LOG(ERROR, "info hash get error", K(ret), K(pkey));
        }
      } else {
        EXTLOG_LOG(TRACE, "this partition already finished", K(pkey));
      }
    }  // end while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret)) {
    EXTLOG_LOG(INFO, "scan cur info block success", K(progress));
  } else {
    EXTLOG_LOG(WARN, "scan cur info block error", K(ret), K(progress));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::do_hurry_quit(
    SearchMap& search_map, SearchParam& search_param, Progress& progress, const ObExtRpcQit& qit)
{
  int ret = OB_SUCCESS;
  SearchMap::BlurredIterator iter(search_map);
  ObPartitionKey pkey;
  SearchStatus* search_status = NULL;
  while (OB_SUCC(ret) && OB_SUCC(iter.next(pkey, search_status)) && !progress.all_finished()) {
    if (OB_ISNULL(search_status)) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "get null search status from map", K(ret), K(pkey));
    } else if (!search_status->is_finished()) {
      if (search_param.is_by_ts()) {
        search_status->search_by_ts_.err_ = OB_EXT_HANDLE_UNFINISH;
        search_status->set_finished();
        progress.finish_one();
      } else if (search_param.is_by_id()) {
        // search by id
        search_status->search_by_id_.err_ = OB_EXT_HANDLE_UNFINISH;
        search_status->set_finished();
        progress.finish_one();
      } else {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      EXTLOG_LOG(INFO, "this pkey already finished when hurry quit", K(pkey), K(qit));
    }
  }  // end while
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::scan_info_blocks(
    SearchMap& search_map, SearchParam& search_param, Progress& progress, const ObExtRpcQit& qit)
{
  EXTLOG_LOG(INFO, "scan info blocks begin", K(progress), K(qit));
  int ret = OB_SUCCESS;
  file_id_t start_file_id = progress.get_start_file_id();
  file_id_t min_file_id = progress.min_file_id_;
  file_id_t cur_file_id = OB_INVALID_FILE_ID;
  bool is_hurry_quit = false;

  if (OB_INVALID_FILE_ID == start_file_id) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "invalid start file id to scan info blocks", K(ret), K(progress));
  } else {
    EXTLOG_LOG(INFO, "scan info block from start_log_id", K(start_file_id), K(progress), K(qit));
  }

  for (cur_file_id = start_file_id;
       OB_SUCC(ret) && cur_file_id >= min_file_id && !progress.all_finished() && !is_hurry_quit;
       cur_file_id--) {
    progress.set_cur_file_id(cur_file_id);
    if (OB_FAIL(scan_cur_info_block(search_map, search_param, progress))) {
      EXTLOG_LOG(WARN, "test current file error", K(ret), K(progress));
    } else if (!progress.all_finished()) {
      EXTLOG_LOG(TRACE, "test should hurry quit", K(qit), K(progress));
      if (OB_FAIL(qit.should_hurry_quit(is_hurry_quit))) {
        EXTLOG_LOG(WARN, "qit check should_hurry_quit error", K(ret), K(qit));
      } else if (is_hurry_quit) {
        // } else if (is_hurry_quit || (is_hurry_quit = mock_quit(cur_file_id, start_file_id))) {
        if (OB_FAIL(do_hurry_quit(search_map, search_param, progress, qit))) {
          EXTLOG_LOG(WARN, "do_hurry_quit error", K(ret), K(qit));
        } else {
          EXTLOG_LOG(INFO, "do_hurry_quit success", K(qit), K(progress));
        }
      }
    }
  }  // end for
  if (OB_SUCC(ret) && is_hurry_quit) {
    const file_id_t break_file_id = cur_file_id + 1;
    if (OB_FAIL(progress.set_break_file_id(break_file_id))) {
      EXTLOG_LOG(WARN, "progress set break_file_id error", K(break_file_id));
    }
  }

  if (OB_SUCC(ret) && !progress.all_finished() && !is_hurry_quit) {
    // End the scan normally and process the unfinished pkey
    if (OB_FAIL(after_scan(search_map, search_param, progress))) {
      EXTLOG_LOG(WARN, "after scan finish", K(ret), K(progress));
    }
  }
  if (OB_SUCC(ret)) {
    EXTLOG_LOG(INFO, "scan info blocks finish success", K(progress), K(is_hurry_quit));
  } else {
    EXTLOG_LOG(WARN, "scan info blocks finish error", K(ret), K(progress), K(is_hurry_quit));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::get_last_slide_log(
    const ObPartitionKey& pkey, uint64_t& last_log_id, int64_t& last_ts)
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
  } else {
    if (OB_FAIL(pls->get_log_id_range(start_log_id, start_ts, end_log_id, end_ts))) {
      EXTLOG_LOG(WARN, "get log id range from pls error", K(ret), K(pkey));
    } else {
      last_log_id = end_log_id;
      last_ts = end_ts;
      if (OB_INVALID_ID == last_log_id || OB_INVALID_TIMESTAMP == last_ts) {
        ret = OB_ERR_UNEXPECTED;
        EXTLOG_LOG(ERROR, "get invalid last log id or ts", K(ret), K(last_log_id), K(last_ts));
      }
    }
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::ts_handle_cold_pkeys_by_sw(
    const ObPartitionKey& pkey, SearchStatus* search_status, Progress& progress)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(search_status)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const int64_t start_ts = search_status->get_start_ts();
    uint64_t last_slide_log_id = OB_INVALID_ID;
    int64_t last_slide_log_ts = OB_INVALID_TIMESTAMP;
    if (OB_FAIL(get_last_slide_log(pkey, last_slide_log_id, last_slide_log_ts))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        // The partition does not exist and is not a cold partition. Confirm by scanning InfoBlock
        ret = OB_SUCCESS;
        EXTLOG_LOG(TRACE, "get_last_slide_log partition not exist", K(ret), K(pkey));
      } else {
        EXTLOG_LOG(WARN, "get_last_slide_log error", K(ret), K(pkey));
      }
    } else if (OB_UNLIKELY(OB_INVALID_ID == last_slide_log_id) ||
               OB_UNLIKELY(OB_INVALID_TIMESTAMP == last_slide_log_ts)) {
      // sw has initialized last_slide to last_replay_log_id/ts
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(
          ERROR, "sw maintain wrong last_slide_log_id/ts", K(ret), K(pkey), K(last_slide_log_id), K(last_slide_log_ts));
    } else if (0 == last_slide_log_id) {
      // The log_id of the last log that slide out of the sliding window is 0,
      // indicating that no log slide out (last_replay_log_id=0)
      if (0 != last_slide_log_ts) {
        ret = OB_ERR_UNEXPECTED;
        EXTLOG_LOG(ERROR, "last_slide info corrupted", K(ret), K(last_slide_log_ts), K(last_slide_log_id));
      } else {
        search_status->search_by_ts_.err_ = OB_SUCCESS;
        search_status->search_by_ts_.start_log_id_ = 1;
        search_status->search_by_ts_.start_log_ts_ = 0;  // for debug
        search_status->set_finished();
        progress.finish_one();
      }
    } else if (last_slide_log_ts < start_ts) {
      EXTLOG_LOG(TRACE, "handle cold pkey", K(pkey), K(last_slide_log_id), K(last_slide_log_ts));
      // The timestamp of this log is smaller than the timestamp of liboblog startup, it is safe to return this log_id,
      // liboblog should ask the leader first, or should ask all replicas,
      // Otherwise, if you ask about a lagged machine, it may cause the given security log_id to be too small,
      // resulting in too much fallback when positioning the location later.
      search_status->search_by_ts_.err_ = OB_SUCCESS;
      search_status->search_by_ts_.start_log_id_ = last_slide_log_id;
      search_status->search_by_ts_.start_log_ts_ = last_slide_log_ts;
      search_status->set_finished();
      progress.finish_one();
    } else {
      // can not handle this pkey now
      // handle it by scanning info blocks
      EXTLOG_LOG(TRACE, "not cold pkey", K(pkey), K(start_ts), K(last_slide_log_ts));
    }
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::get_sw_max_log_id(const ObPartitionKey& pkey, uint64_t& sw_max_log_id)
{
  int ret = OB_SUCCESS;
  ObIPartitionLogService* pls = NULL;
  ObIPartitionGroupGuard guard;
  if (OB_FAIL(partition_service_->get_partition(pkey, guard)) || NULL == guard.get_partition_group() ||
      NULL == (pls = guard.get_partition_group()->get_log_service())) {
    ret = OB_PARTITION_NOT_EXIST;
  } else if (!(guard.get_partition_group()->is_valid())) {
    ret = OB_INVALID_PARTITION;
  } else {
    if (OB_FAIL(pls->get_sw_max_log_id(sw_max_log_id))) {
      EXTLOG_LOG(WARN, "pls get sw_max_log_id error", K(ret));
    } else if (OB_INVALID_ID == sw_max_log_id) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "invalid sw_max_log_id", K(ret), K(pkey), K(sw_max_log_id));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::id_handle_cold_pkeys_by_sw(
    const ObPartitionKey& pkey, SearchStatus* search_status, Progress& progress)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(search_status)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const uint64_t req_start_log_id = search_status->get_start_log_id();
    uint64_t sw_max_log_id = OB_INVALID_ID;
    if (OB_FAIL(get_sw_max_log_id(pkey, sw_max_log_id))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        EXTLOG_LOG(WARN, "get sw_max_log_id error", K(ret), K(pkey), K(req_start_log_id), K(progress));
      }
    } else if (req_start_log_id >= sw_max_log_id + 1) {
      // When get_sw_max_log_id, the clog corresponding to req_start_log_id has not been submitted to the sliding window
      // It is safe to return the max_file_id retrieved in advance
      search_status->search_by_id_.err_ = OB_SUCCESS;
      search_status->search_by_id_.res_file_id_ = progress.max_file_id_;
      search_status->set_finished();
      progress.finish_one();
      EXTLOG_LOG(INFO, "cold partition", K(pkey), K(req_start_log_id), K(sw_max_log_id), K(progress));
      if (req_start_log_id > sw_max_log_id + 1) {
        EXTLOG_LOG(
            INFO, "liboblog is faster than this server", K(pkey), K(req_start_log_id), K(sw_max_log_id), K(progress));
      }
    } else {
      // can not determine
    }
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::handle_cold_pkeys_by_sw(
    SearchMap& search_map, SearchParam& search_param, Progress& progress)
{
  int ret = OB_SUCCESS;
  SearchMap::BlurredIterator iter(search_map);
  ObPartitionKey pkey;
  SearchStatus* search_status = NULL;
  while (OB_SUCCESS == ret && OB_SUCCESS == (ret = iter.next(pkey, search_status)) && !progress.all_finished()) {
    EXTLOG_LOG(TRACE, "iterate search_map:", K(pkey));
    if (OB_ISNULL(search_status)) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "get null search status", K(ret), K(pkey));
    } else if (search_status->is_finished()) {
      // continue, next
    } else {
      if (search_param.is_by_ts()) {
        if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == search_status->get_start_ts())) {
          ret = OB_ERR_UNEXPECTED;
          EXTLOG_LOG(
              ERROR, "invalid start_ts in search map", K(ret), K(pkey), "search_by_ts", search_status->search_by_ts_);
        } else if (OB_FAIL(ts_handle_cold_pkeys_by_sw(pkey, search_status, progress))) {
          EXTLOG_LOG(WARN, "handle cold pkeys by sw error", K(ret), K(pkey), K(progress));
        } else {
          // do nothing
        }
      } else if (search_param.is_by_id()) {
        if (OB_UNLIKELY(OB_INVALID_ID == search_status->get_start_log_id())) {
          ret = OB_ERR_UNEXPECTED;
          EXTLOG_LOG(ERROR,
              "invalid start_log_id in search map",
              K(ret),
              K(pkey),
              "search_by_id",
              search_status->search_by_id_);
        } else if (OB_FAIL(id_handle_cold_pkeys_by_sw(pkey, search_status, progress))) {
          EXTLOG_LOG(WARN, "handle cold pkeys by sw error", K(ret), K(pkey), K(progress));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        EXTLOG_LOG(ERROR, "unexpect search type");
      }
    }
  }  // end while
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCCESS == ret) {
    EXTLOG_LOG(INFO, "handle cold pkeys by_sw success", K(ret), K(progress));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::ts_handle_cold_pkeys_by_last_info(
    const ObPartitionKey& pkey, SearchStatus* search_status, Progress& progress)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = search_status->get_start_ts();
  uint64_t info_min_log_id = OB_INVALID_ID;
  int64_t info_min_ts = OB_INVALID_TIMESTAMP;
  if (OB_ISNULL(log_engine_) || OB_ISNULL(search_status)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "log_engine_ or search_satus is null", K(ret), KP(log_engine_), KP(search_status));
  } else {
    if (OB_FAIL(log_engine_->get_ilog_memstore_min_log_id_and_ts(pkey, info_min_log_id, info_min_ts))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        EXTLOG_LOG(TRACE, "get info min log id, pkey not exist", K(ret), K(pkey));
      } else {
        EXTLOG_LOG(WARN, "get info min log id error", K(ret), K(pkey));
      }
    } else if (OB_UNLIKELY(OB_INVALID_ID == info_min_log_id) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == info_min_ts) ||
               OB_UNLIKELY(0 == info_min_ts)) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "get invalid info_min_log_id", K(ret), K(pkey), K(info_min_log_id), K(info_min_ts));
    } else {
      // get info entry success
      EXTLOG_LOG(TRACE, "get info entry success", K(pkey), K(info_min_log_id), K(info_min_ts));
      if (start_ts > info_min_ts) {
        // The timestamp of info_min_ts is smaller than that of liboblog. It is safe to return this log_id.
        search_status->search_by_ts_.err_ = OB_SUCCESS;
        search_status->search_by_ts_.start_log_id_ = info_min_log_id;
        search_status->search_by_ts_.start_log_ts_ = info_min_ts;
        search_status->set_finished();
        progress.finish_one();
        EXTLOG_LOG(INFO,
            "ts handle cold pkey, log lies in last file",
            K(pkey),
            K(start_ts),
            K(info_min_log_id),
            K(info_min_ts));
      } else {
        // If all InfoBlocks are scanned backward, but there is no log of this pkey
        // Then return this log and mark OB_ERR_OUT_OF_LOWER_BOUND at the same time
        search_status->search_by_ts_.min_greater_log_id_ = info_min_log_id;
        EXTLOG_LOG(TRACE, "ts handle cold pkey, log lies in prev files", K(pkey), K(start_ts), K(info_min_ts));
      }
    }
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::id_handle_cold_pkeys_by_last_info(
    const ObPartitionKey& pkey, SearchStatus* search_status, Progress& progress)
{
  int ret = OB_SUCCESS;
  const uint64_t start_log_id = search_status->get_start_log_id();
  uint64_t info_min_log_id = OB_INVALID_ID;
  int64_t info_min_ts = OB_INVALID_TIMESTAMP;
  if (OB_ISNULL(log_engine_) || OB_ISNULL(search_status)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(ERROR, "log_engine_ or search_satus is null", K(ret), KP(log_engine_), KP(search_status));
  } else {
    if (OB_FAIL(log_engine_->get_ilog_memstore_min_log_id_and_ts(pkey, info_min_log_id, info_min_ts))) {
      if (OB_PARTITION_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        EXTLOG_LOG(TRACE, "get info min log id, pkey not exist", K(ret), K(pkey));
      } else {
        EXTLOG_LOG(WARN, "get info min log id error", K(ret), K(pkey));
      }
    } else if (OB_UNLIKELY(OB_INVALID_ID == info_min_log_id) || OB_UNLIKELY(OB_INVALID_TIMESTAMP == info_min_ts) ||
               OB_UNLIKELY(0 == info_min_ts)) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "get invalid info_min_log_id", K(ret), K(pkey), K(info_min_log_id), K(info_min_ts));
    } else {
      // get info entry success
      EXTLOG_LOG(TRACE, "get info entry success", K(pkey), K(info_min_log_id), K(info_min_ts));
      if (start_log_id >= info_min_log_id) {
        search_status->search_by_id_.err_ = OB_SUCCESS;
        // If log file is swithed, the return progress.max_file_id_ may be too small, which is safe
        search_status->search_by_id_.res_file_id_ = progress.max_file_id_;
        search_status->set_finished();
        progress.finish_one();
        EXTLOG_LOG(INFO, "id handle cold pkey, log lies in last file", K(pkey), K(start_log_id), K(info_min_log_id));
      } else {
        // in previous file
        EXTLOG_LOG(TRACE, "id handle cold pkey, log lies in prev files", K(pkey), K(start_log_id), K(info_min_log_id));
      }
    }
  }
  return ret;
}

// Judge cold partition by last_info_block
// If log file is switched during execution, you can simply treat the new flying infoblock as an old file
// When scanning InfoBlock in the next step, always try to read the last file.
// So even if the file is switched, the information of progress.max_file_id_ will not be skipped by mistake
// In this way, the given start_log_id may be too small, which is safe.
int ObLogExternalExecutorWithBreakpoint::handle_cold_pkeys_by_last_info(
    SearchMap& search_map, SearchParam& search_param, Progress& progress)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(log_engine_)) {
    ret = OB_ERR_UNEXPECTED;
    EXTLOG_LOG(WARN, "log engine is null", K(ret));
  } else {
    SearchMap::BlurredIterator iter(search_map);
    ObPartitionKey pkey;
    SearchStatus* search_status = NULL;
    while (OB_SUCCESS == ret && OB_SUCCESS == (ret = iter.next(pkey, search_status)) && !progress.all_finished()) {
      if (OB_UNLIKELY(!pkey.is_valid()) || OB_UNLIKELY(NULL == search_status)) {
        ret = OB_ERR_UNEXPECTED;
        EXTLOG_LOG(ERROR, "get invalid item from search status", K(ret), K(pkey), KP(search_status));
      } else if (search_status->is_finished()) {
        // continue
      } else {
        if (search_param.is_by_ts()) {
          if (OB_UNLIKELY(OB_INVALID_TIMESTAMP == search_status->get_start_ts())) {
            ret = OB_ERR_UNEXPECTED;
            EXTLOG_LOG(ERROR, "invalid start_ts", K(ret), K(pkey));
          } else if (OB_FAIL(ts_handle_cold_pkeys_by_last_info(pkey, search_status, progress))) {
            EXTLOG_LOG(ERROR, "ts_handle_cold_pkeys_by_last_info error");
          }
        } else if (search_param.is_by_id()) {
          if (OB_UNLIKELY(OB_INVALID_ID == search_status->get_start_log_id())) {
            ret = OB_ERR_UNEXPECTED;
            EXTLOG_LOG(ERROR, "invalid start_ts", K(ret), K(pkey));
          } else if (OB_FAIL(id_handle_cold_pkeys_by_last_info(pkey, search_status, progress))) {
            EXTLOG_LOG(ERROR, "id_handle_cold_pkeys_by_last_info error");
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          EXTLOG_LOG(ERROR, "unknown search type", K(ret), K(search_param));
        }
        if (OB_PARTITION_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        }
      }
    }  // end while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::handle_cold_pkeys(
    SearchMap& search_map, SearchParam& search_param, Progress& progress)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(handle_cold_pkeys_by_sw(search_map, search_param, progress))) {
    EXTLOG_LOG(WARN, "handle cold pkeys by sw error", K(ret), K(progress));
  } else if (OB_FAIL(handle_cold_pkeys_by_last_info(search_map, search_param, progress))) {
    EXTLOG_LOG(WARN, "handle cold pkeys by last info error", K(ret), K(progress));
  } else {
    EXTLOG_LOG(INFO, "handle cold pkeys success", K(progress));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::ts_build_result(const ObLogReqStartLogIdByTsRequestWithBreakpoint& req,
    SearchMap& search_map, const Progress& progress, ObLogReqStartLogIdByTsResponseWithBreakpoint& resp)
{
  int ret = OB_SUCCESS;
  const ObLogReqStartLogIdByTsRequestWithBreakpoint::ParamArray& params = req.get_params();
  int64_t count = params.count();
  const file_id_t break_file_id = progress.get_break_file_id();
  ObLogReqStartLogIdByTsResponseWithBreakpoint::Result result;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    const ObPartitionKey& pkey = params[i].pkey_;
    SearchStatus* search_status = NULL;
    if (OB_FAIL(search_map.get(pkey, search_status))) {
      EXTLOG_LOG(WARN, "search map get error", K(ret), K(pkey), KP(search_status));
    } else if (OB_ISNULL(search_status)) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "search map get null value", K(ret), K(pkey));
    } else {
      result.reset();
      const int err_code = search_status->search_by_ts_.err_;
      if (OB_EXT_HANDLE_UNFINISH == err_code) {
        if (OB_INVALID_FILE_ID == break_file_id) {
          ret = OB_ERR_UNEXPECTED;
          EXTLOG_LOG(WARN, "unknown break_file_id", K(ret), K(progress));
        } else {
          result.err_ = search_status->search_by_ts_.err_;
          result.break_info_.break_file_id_ = break_file_id;
          result.break_info_.min_greater_log_id_ = search_status->search_by_ts_.min_greater_log_id_;
          EXTLOG_LOG(INFO, "ts_build_result, unfinished pkey", K(pkey), K(result));
        }
      } else {
        result.err_ = search_status->search_by_ts_.err_;
        result.start_log_id_ = search_status->search_by_ts_.start_log_id_;
        result.start_log_ts_ = search_status->search_by_ts_.start_log_ts_;
      }
      if (OB_SUCC(ret) && OB_FAIL(resp.append_result(result))) {
        EXTLOG_LOG(WARN, "by ts response append result error", K(ret), K(pkey), K(result));
      } else {
        EXTLOG_LOG(INFO, "by ts response append result success", K(pkey), K(result));
      }
    }
  }
  if (OB_SUCC(ret)) {
    EXTLOG_LOG(INFO, "ts build result success", K(params), K(progress));
  } else {
    EXTLOG_LOG(WARN, "ts build result error", K(ret), K(params), K(progress));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::do_req_start_log_id_by_ts(
    const ObLogReqStartLogIdByTsRequestWithBreakpoint& req_msg, ObLogReqStartLogIdByTsResponseWithBreakpoint& response,
    const ObExtRpcQit& qit)
{
  int ret = OB_SUCCESS;
  const SearchStatusType type = SST_BY_TS;
  SearchMap search_map;
  search_map.init(ObModIds::OB_CLOG_EXT_RPC);
  Progress progress;
  PageArena<> map_allocator;
  PageArena<> info_allocator;
  SearchParam search_param;
  ObExtReqStatistic::ts_inc_req_count();
  // step 1: prepare
  if (OB_FAIL(search_param.init(type, &map_allocator, &info_allocator))) {
    EXTLOG_LOG(WARN, "search param init error", K(ret), K(type));
  } else if (OB_FAIL(build_search_map(req_msg, search_param, search_map, progress))) {
    EXTLOG_LOG(WARN, "build search map error", K(ret), K(req_msg));
  }
  // step 2: handle cold pkeys
  if (OB_SUCC(ret)) {
    if (OB_FAIL(handle_cold_pkeys(search_map, search_param, progress))) {
      EXTLOG_LOG(WARN, "handle cold pkeys error", K(ret), K(progress));
    } else {
      EXTLOG_LOG(INFO, "handle cold pkeys success", K(ret), K(progress), K(qit));
    }
  }
  // step 3: scan info blocks
  if (OB_SUCC(ret) && !progress.all_finished()) {
    if (OB_FAIL(scan_info_blocks(search_map, search_param, progress, qit))) {
      EXTLOG_LOG(WARN, "scan info block error", K(ret), K(search_param), K(req_msg));
    } else {
      EXTLOG_LOG(INFO, "scan info block success", K(ret), K(progress), K(qit));
    }
  }
  // step 4: build result
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ts_build_result(req_msg, search_map, progress, response))) {
      EXTLOG_LOG(WARN, "build result error", K(ret), K(req_msg));
    }
  } else {
    ObExtReqStatistic::ts_inc_err();
    // double check execute error or scan disk error
    EXTLOG_LOG(WARN, "do req start log id by ts error", K(ret), K(req_msg));
  }
  if (OB_SUCC(ret)) {
    response.set_err(OB_SUCCESS);
  } else {
    response.set_err(OB_ERR_SYS);
  }
  (void)search_map.destroy();
  map_allocator.free();
  info_allocator.free();
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::do_req_start_log_id_by_ts_qit(
    const ObLogReqStartLogIdByTsRequestWithBreakpoint& req_msg, ObLogReqStartLogIdByTsResponseWithBreakpoint& resp)
{
  int ret = OB_SUCCESS;
  const ObLogReqStartLogIdByTsRequestWithBreakpoint::ParamArray& params = req_msg.get_params();
  int64_t count = params.count();
  ObLogReqStartLogIdByTsResponseWithBreakpoint::Result result;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    result.reset();
    result.err_ = OB_EXT_HANDLE_UNFINISH;
    // result.break_info_ keeps illegal value
    if (OB_FAIL(resp.append_result(result))) {
      EXTLOG_LOG(WARN, "by_ts response append result error", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    EXTLOG_LOG(INFO, "req start log id by ts quit in time success", K(req_msg));
  } else {
    EXTLOG_LOG(WARN, "req start log id by ts quit in time err", K(ret), K(req_msg));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::req_start_log_id_by_ts_with_breakpoint(
    const ObLogReqStartLogIdByTsRequestWithBreakpoint& req_msg, ObLogReqStartLogIdByTsResponseWithBreakpoint& response)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "log external executor not init", K(ret), K(req_msg));
  } else if (OB_UNLIKELY(!req_msg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "ObLogReqStartLogIdByTsRequestWithBreakpoint is not valid", K(ret), K(req_msg));
  } else {
    ObExtRpcQit qit;
    bool is_enough = false;
    if (OB_FAIL(qit.init(ObExtRpcQit::RPC_LOCATE_BY_TS, get_rpc_deadline()))) {
      EXTLOG_LOG(WARN, "init qit error", K(ret));
    } else if (OB_FAIL(qit.enough_time_to_handle(is_enough))) {
      EXTLOG_LOG(WARN, "qit check enough time to handle error", K(ret));
    } else {
      const int64_t start_ts = ObTimeUtility::current_time();
      if (is_enough) {
        ret = do_req_start_log_id_by_ts(req_msg, response, qit);
      } else {
        ret = do_req_start_log_id_by_ts_qit(req_msg, response);
      }
      const int64_t end_ts = ObTimeUtility::current_time();
      if (OB_SUCC(ret)) {
        ObExtReqStatistic::ts_inc_perf_total(end_ts - start_ts);
        EXTLOG_LOG(INFO, "req start log id by ts success", K(ret), K(req_msg), K(response));
      } else {
        EXTLOG_LOG(WARN, "req start log id by ts error", K(ret), K(req_msg), K(response));
      }
    }
  }
  ObExtReqStatistic::print_statistic_info(FORCE_RPINT_STATISTIC);
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::id_build_result(const ObLogReqStartPosByLogIdRequestWithBreakpoint& req_msg,
    SearchMap& search_map, const Progress& progress, ObLogReqStartPosByLogIdResponseWithBreakpoint& resp)
{
  int ret = OB_SUCCESS;
  const ObLogReqStartPosByLogIdRequestWithBreakpoint::ParamArray& params = req_msg.get_params();
  int64_t count = params.count();
  const file_id_t break_file_id = progress.get_break_file_id();
  ObLogReqStartPosByLogIdResponseWithBreakpoint::Result result;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    const ObPartitionKey& pkey = params[i].pkey_;
    SearchStatus* search_status = NULL;
    if (OB_FAIL(search_map.get(pkey, search_status))) {
      EXTLOG_LOG(WARN, "search map get error", K(ret), K(pkey), KP(search_status));
    } else if (OB_ISNULL(search_status)) {
      ret = OB_ERR_UNEXPECTED;
      EXTLOG_LOG(ERROR, "search map get null value", K(ret), K(pkey));
    } else {
      result.reset();
      const int err_code = search_status->search_by_id_.err_;
      if (OB_EXT_HANDLE_UNFINISH == err_code) {
        if (OB_INVALID_FILE_ID == break_file_id) {
          ret = OB_ERR_UNEXPECTED;
          EXTLOG_LOG(WARN, "unknown break_file_id", K(ret), K(progress));
        } else {
          result.err_ = err_code;
          result.break_info_.break_file_id_ = break_file_id;
        }
      } else {
        result.err_ = search_status->search_by_id_.err_;
        result.file_id_ = search_status->search_by_id_.res_file_id_;
        result.offset_ = 0;
      }
      if (OB_SUCC(ret) && OB_FAIL(resp.append_result(result))) {
        EXTLOG_LOG(WARN, "by id response append result error", K(ret), K(pkey), K(result));
      } else {
        EXTLOG_LOG(TRACE, "by id response append result success", K(pkey), K(result));
      }
    }
  }
  if (OB_SUCC(ret)) {
    EXTLOG_LOG(INFO, "id build result success", K(params));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::do_req_start_pos_by_log_id(
    const ObLogReqStartPosByLogIdRequestWithBreakpoint& req_msg,
    ObLogReqStartPosByLogIdResponseWithBreakpoint& response, const ObExtRpcQit& qit)
{
  int ret = OB_SUCCESS;
  const SearchStatusType type = SST_BY_ID;
  SearchMap search_map;
  search_map.init(ObModIds::OB_CLOG_EXT_RPC);
  Progress progress;
  PageArena<> map_allocator;
  PageArena<> info_allocator;
  SearchParam search_param;
  ObExtReqStatistic::id_inc_req_count();
  // step 1, Initialize the search map. Get max_file_id (handling the problem of switch files during execution)
  if (OB_FAIL(search_param.init(type, &map_allocator, &info_allocator))) {
    EXTLOG_LOG(WARN, "search param init error", K(ret), K(type));
  } else if (OB_FAIL(build_search_map(req_msg, search_param, search_map, progress))) {
    EXTLOG_LOG(WARN, "build search map error", K(ret), K(req_msg));
  }
  // step 2: handle cold partitions.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(handle_cold_pkeys(search_map, search_param, progress))) {
      EXTLOG_LOG(WARN, "handle cold pkeys error", K(ret), K(progress));
    } else {
      EXTLOG_LOG(INFO, "handle cold pkeys success", K(ret), K(progress), K(qit));
    }
  }
  // step 3: scan
  if (OB_SUCC(ret) && !progress.all_finished()) {
    if (OB_FAIL(scan_info_blocks(search_map, search_param, progress, qit))) {
      EXTLOG_LOG(WARN, "scan info block error", K(ret), K(search_param), K(req_msg));
    } else {
      EXTLOG_LOG(INFO, "scan info block success", K(progress), K(qit));
    }
  }
  // step 4: build result
  if (OB_SUCC(ret)) {
    if (OB_FAIL(id_build_result(req_msg, search_map, progress, response))) {
      EXTLOG_LOG(WARN, "build result error", K(ret), K(req_msg));
    } else {
      EXTLOG_LOG(INFO, "build result succes", K(ret), K(req_msg));
    }
  }
  if (OB_SUCC(ret)) {
    response.set_err(OB_SUCCESS);
  } else {
    response.set_err(OB_ERR_SYS);
    ObExtReqStatistic::id_inc_err();
  }
  (void)search_map.destroy();
  map_allocator.free();
  info_allocator.free();
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::do_req_start_pos_by_log_id_qit(
    const ObLogReqStartPosByLogIdRequestWithBreakpoint& req_msg,
    ObLogReqStartPosByLogIdResponseWithBreakpoint& response)
{
  int ret = OB_SUCCESS;
  const ObLogReqStartPosByLogIdRequestWithBreakpoint::ParamArray& params = req_msg.get_params();
  const int64_t partition_count = params.count();
  ObLogReqStartPosByLogIdResponseWithBreakpoint::Result result;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_count; i++) {
    result.reset();
    result.err_ = OB_EXT_HANDLE_UNFINISH;
    if (OB_FAIL(response.append_result(result))) {
      EXTLOG_LOG(WARN, "by_id response append result error", K(ret), K(result));
    }
  }
  if (OB_SUCCESS == ret) {
    EXTLOG_LOG(INFO, "req start pos by log_id quit in time success", K(req_msg));
  }
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::req_start_pos_by_log_id_with_breakpoint(
    const ObLogReqStartPosByLogIdRequestWithBreakpoint& req_msg,
    ObLogReqStartPosByLogIdResponseWithBreakpoint& response)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    EXTLOG_LOG(WARN, "log external executor not init", K(ret), K(req_msg));
  } else if (!req_msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "req_start_pos_by_log_id_with_breakpoint error", K(ret), K(req_msg));
  } else {
    ObExtRpcQit qit;
    bool is_enough = false;
    if (OB_FAIL(qit.init(ObExtRpcQit::RPC_LOCATE_BY_ID, get_rpc_deadline()))) {
      EXTLOG_LOG(WARN, "init qit error", K(ret));
    } else if (OB_FAIL(qit.enough_time_to_handle(is_enough))) {
      EXTLOG_LOG(WARN, "qit check enough time to handle error", K(ret));
    } else {
      const int64_t start_ts = ObTimeUtility::current_time();
      if (is_enough) {
        ret = do_req_start_pos_by_log_id(req_msg, response, qit);
      } else {
        // Time is running out, do nothing, exit immediately
        ret = do_req_start_pos_by_log_id_qit(req_msg, response);
      }
      const int64_t end_ts = ObTimeUtility::current_time();
      if (OB_SUCC(ret)) {
        ObExtReqStatistic::id_inc_perf_total(end_ts - start_ts);
        EXTLOG_LOG(INFO, "req_start_pos_by_log_id_with_breakpoint success", K(ret), K(req_msg), K(response));
      } else {
        EXTLOG_LOG(WARN, "req_start_pos_by_log_id_with_breakpoint error", K(ret), K(req_msg), K(response));
      }
    }
  }
  ObExtReqStatistic::print_statistic_info(FORCE_RPINT_STATISTIC);
  return ret;
}

int ObLogExternalExecutorWithBreakpoint::get_min_log_id_and_ts_from_index_info_block_map_(
    IndexInfoBlockMap& index_info_block_map, const common::ObPartitionKey& partition_key, uint64_t& min_log_id,
    int64_t& min_submit_timestamp) const
{
  int ret = OB_SUCCESS;
  IndexInfoBlockEntry entry;
  if (OB_FAIL(index_info_block_map.get(partition_key, entry)) && OB_ENTRY_NOT_EXIST != ret) {
    EXTLOG_LOG(WARN, "index_info_block_map get failed", K(ret), K(partition_key));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_PARTITION_NOT_EXIST;
  } else {
    min_log_id = entry.min_log_id_;
    min_submit_timestamp = entry.min_log_timestamp_;
  }
  return ret;
}
}  // namespace extlog
}  // namespace oceanbase
