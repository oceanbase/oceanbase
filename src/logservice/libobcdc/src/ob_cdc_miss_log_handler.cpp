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
 *
 * OBCDC header file
 * This file defines interface of OBCDC
 */

#define USING_LOG_PREFIX OBLOG_FETCHER


#include "ob_cdc_miss_log_handler.h"
#include "ob_log_config.h"
#include "ob_log_utils.h"

namespace oceanbase
{
namespace libobcdc
{
int64_t ObCDCMissLogHandler::g_rpc_timeout = TCONF.fetch_log_rpc_timeout_sec * _SEC_;
const int64_t ObCDCMissLogHandler::RETRY_LOG_PRINT_INTERVAL = 30 * _SEC_;
const int64_t ObCDCMissLogHandler::RETRY_TIMEOUT = 1 * _HOUR_;
const int64_t ObCDCMissLogHandler::MAX_RPC_TIMEOUT = 5 * _MIN_;

MissLogTask::MissLogTask(
    const common::ObAddr &orig_svr,
    IObLogRpc &rpc,
    LSFetchCtx &ls_fetch_ctx,
    IObCDCPartTransResolver::MissingLogInfo &missing_info,
    logfetcher::TransStatInfo &tsi)
    : svr_(), rpc_(rpc), ls_fetch_ctx_(ls_fetch_ctx), missing_info_(missing_info), need_change_server_(false), tsi_(tsi)
{
  svr_ = orig_svr;
}

void MissLogTask::reset()
{
  // only server is init by MissLogTask
  svr_.reset();
  need_change_server_ = false;
}

int MissLogTask::try_change_server(const int64_t timeout, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const int64_t end_time = get_timestamp() + timeout;

  if (OB_UNLIKELY(need_change_server_)) {
    common::ObAddr req_svr;
    int64_t retry_times = 0;
    bool is_timeout = false;
    const static int64_t RETRY_LOG_PRINT_INTERVAL = 30 * _SEC_;
    do {
      if (OB_FAIL(ls_fetch_ctx_.next_server(req_svr))) {
        LOG_WARN("get next_server failed", KR(ret), K_(ls_fetch_ctx));
      } else if (OB_UNLIKELY(!req_svr.is_valid())) {
        ret = OB_NEED_RETRY;
        LOG_WARN("next_server is not valid, need_retry", KR(ret), K(req_svr), K_(ls_fetch_ctx));
      } else {
        ObCStringHelper helper;
        _LOG_INFO("[MISS_LOG][CHANGE_SVR][FROM=%s][TO=%s][PART_TRANS_ID=%s]",
            helper.convert(svr_), helper.convert(req_svr), helper.convert(get_part_trans_id()));
        svr_ = req_svr;
        need_change_server_ = false;
      }

      if (OB_FAIL(ret)) {
        is_timeout = get_timestamp() >= end_time;
        if (is_timeout) {
          LOG_ERROR("[MISS_LOG][NEXT_SERVER]RETRY_GET_NEXT_SERVER TIMEOUT", KR(ret), K(timeout), KPC(this));
          ret = OB_TIMEOUT;
        } else {
          ++retry_times;
          if (TC_REACH_TIME_INTERVAL(RETRY_LOG_PRINT_INTERVAL)) {
            LOG_WARN("[MISS_LOG][NEXT_SERVER]RETRY_GET_NEXT_SERVER",
                "tls_id", ls_fetch_ctx_.get_tls_id(), K(retry_times),
                "end_time", TS_TO_STR(end_time));
          }
          ob_usleep(10 * _MSEC_);
        }
      }
    } while (OB_FAIL(ret) && ! is_timeout && ! stop_flag);

    if (OB_UNLIKELY(stop_flag)) {
      ret = OB_IN_STOP_STATE;
      LOG_WARN("handle miss log task stop", KR(ret), KPC(this));
    }
  }

  return ret;
}

ObCDCMissLogHandler& ObCDCMissLogHandler::get_instance()
{
  static ObCDCMissLogHandler instance;
  return instance;
}

void ObCDCMissLogHandler::configure(const ObLogConfig &config)
{
  g_rpc_timeout = config.fetch_log_rpc_timeout_sec * _SEC_;
  LOG_INFO("[MISS_LOG][CONFIG]", "rpc_timeout", TVAL_TO_STR(g_rpc_timeout));
}

// TODO add metrics
int ObCDCMissLogHandler::handle_log_miss(
    const common::ObAddr &cur_svr,
    IObLogRpc *rpc,
    LSFetchCtx &ls_fetch_ctx,
    IObCDCPartTransResolver::MissingLogInfo &missing_info,
    logfetcher::TransStatInfo &tsi,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(rpc)
      || OB_UNLIKELY(missing_info.is_empty()
      || ! cur_svr.is_valid()
      || missing_info.get_part_trans_id().get_tls_id() != ls_fetch_ctx.get_tls_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args to handle miss log", KR(ret), K(cur_svr), K(missing_info), K(rpc));
  } else {
    MissLogTask misslog_task(cur_svr, *rpc, ls_fetch_ctx, missing_info, tsi);

    if (OB_FAIL(handle_miss_log_task_(misslog_task, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        int debug_err = ret;
        // overwrite ret
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("handle_miss_log_task_ failed", KR(ret), KR(debug_err), K(misslog_task), K(stop_flag));
      }
    }
  }

  return ret;
}

int ObCDCMissLogHandler::handle_miss_log_task_(MissLogTask &misslog_task, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObTraceIdGuard guard(*common::ObCurTraceId::get_trace_id());
  const int64_t start_ts = get_timestamp();
  misslog_task.missing_info_.set_resolving_miss_log();
  FetchLogSRpc *fetch_log_srpc = nullptr;

  if (OB_FAIL(alloc_fetch_log_srpc_(misslog_task.get_tenant_id(), fetch_log_srpc))) {
    LOG_ERROR("alloc fetch_log_srpc fail", KR(ret));
  } else if (OB_ISNULL(fetch_log_srpc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid fetch_log_srpc", KR(ret), K(misslog_task));
  } else if (OB_FAIL(handle_miss_record_or_state_log_(
      misslog_task,
      *fetch_log_srpc,
      stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_miss_record_or_state_log_ failed", KR(ret), K(misslog_task));
    }
  } else if (OB_FAIL(handle_miss_redo_log_(
      misslog_task,
      *fetch_log_srpc,
      stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("handle_miss_redo_log_ failed", KR(ret), K(misslog_task));
    }
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  } else {
    const int64_t cost_time = get_timestamp() - start_ts;
    LOG_INFO("[MISS_LOG][HANDLE_DONE]", KR(ret),
        K(cost_time), "cost_time", TVAL_TO_STR(cost_time),
        K(misslog_task));
  }

  if (OB_NOT_NULL(fetch_log_srpc)) {
    free_fetch_log_srpc_(fetch_log_srpc);
    fetch_log_srpc = nullptr;
  }
  return ret;
}

int ObCDCMissLogHandler::handle_miss_record_or_state_log_(
    MissLogTask &misslog_task,
    FetchLogSRpc &fetch_log_srpc,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (misslog_task.missing_info_.has_miss_record_or_state_log()) {
    int64_t miss_record_cnt = 1;
    ObArrayImpl<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> batched_misslog_lsn_arr;
    palf::LSN misslog_lsn;
    const int64_t start_ts = get_timestamp();
    LOG_INFO("[MISS_LOG][FETCH_RECORD_OR_STATE][BEGIN]", KR(ret), "part_trans_id", misslog_task.get_part_trans_id());

    while (OB_SUCC(ret) && ! stop_flag && misslog_task.missing_info_.has_miss_record_or_state_log()) {
      misslog_lsn.reset();
      batched_misslog_lsn_arr.reset();
      ObCdcLSFetchMissLogReq::MissLogParam param;

      if (OB_FAIL(misslog_task.missing_info_.get_miss_record_or_state_log_lsn(misslog_lsn))) {
        LOG_ERROR("get_miss_record_or_state_log_lsn failed", KR(ret), K(misslog_task), K(misslog_lsn));
      } else {
        param.miss_lsn_ = misslog_lsn;

        if (OB_FAIL(batched_misslog_lsn_arr.push_back(param))) {
          LOG_ERROR("push_back miss_record_or_state_log_lsn into batched_misslog_lsn_arr failed", KR(ret), K(param));
        } else if (OB_FAIL(fetch_miss_log_with_retry_(
            misslog_task,
            batched_misslog_lsn_arr,
            fetch_log_srpc,
            stop_flag))) {
          if (OB_IN_STOP_STATE != ret) {
            LOG_ERROR("fetch_miss_log_with_retry_ failed", KR(ret), K(batched_misslog_lsn_arr), K(misslog_task));
          }
        } else {
          const obrpc::ObRpcResultCode &rcode = fetch_log_srpc.get_result_code();
          const obrpc::ObCdcLSFetchLogResp &resp = fetch_log_srpc.get_resp();

          if (OB_FAIL(rcode.rcode_) || OB_FAIL(resp.get_err())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("fetch log fail on rpc", KR(ret), K(rcode), K(resp), K(batched_misslog_lsn_arr));
          } else if (resp.get_log_num() < 1) {
            LOG_INFO("fetch_miss_log_rpc doesn't fetch log, retry", K(misslog_lsn));
          } else if (OB_UNLIKELY(resp.get_log_num() > 1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("expect only one misslog while fetching miss_record_or_state_log", K(resp));
          } else if (OB_UNLIKELY(resp.get_next_miss_lsn() != misslog_lsn)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("fetched log not match miss_log_lsn", KR(ret), K(misslog_lsn), K(resp));
          } else {
            misslog_task.missing_info_.reset_miss_record_or_state_log_lsn();
            palf::LogEntry miss_log_entry;
            miss_log_entry.reset();
            const char *buf = resp.get_log_entry_buf();
            const int64_t len = resp.get_pos();
            int64_t pos = 0;

            if (OB_FAIL(miss_log_entry.deserialize(buf, len, pos))) {
              LOG_ERROR("deserialize log_entry of miss_record_or_state_log failed", KR(ret), K(misslog_lsn), KP(buf), K(len), K(pos));
            } else if (OB_FAIL(misslog_task.ls_fetch_ctx_.read_miss_tx_log(miss_log_entry, misslog_lsn, misslog_task.tsi_, misslog_task.missing_info_))) {
              if (OB_ITEM_NOT_SETTED == ret) {
                miss_record_cnt ++;
                ret = OB_SUCCESS;
                LOG_INFO("[MISS_LOG][FETCH_RECORD_OR_STATE] found new miss_record_or_state_log",
                    "tls_id", misslog_task.ls_fetch_ctx_.get_tls_id(), K(misslog_lsn), K(miss_record_cnt), K(misslog_task));
              } else {
                LOG_ERROR("[MISS_LOG][FETCH_RECORD_OR_STATE] read miss_log failed", KR(ret), K(miss_log_entry), K(misslog_lsn), K(misslog_task));
              }
            }
          }
        }
      }
    }

    if (stop_flag) {
      ret = OB_IN_STOP_STATE;
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("[MISS_LOG][FETCH_RECORD_OR_STATE][END]",
          "part_trans_id", misslog_task.missing_info_.get_part_trans_id(),
          K(miss_record_cnt),
          "miss_redo_cnt", misslog_task.missing_info_.get_miss_redo_lsn_arr().count(),
          "cost", get_timestamp() - start_ts);
    }
  }

  return ret;
}

int ObCDCMissLogHandler::handle_miss_redo_log_(
    MissLogTask &misslog_task,
    FetchLogSRpc &fetch_log_srpc,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(misslog_task.missing_info_.sort_and_unique_missing_log_lsn())) {
    LOG_ERROR("sort_and_unique_missing_log_lsn failed", KR(ret), K(misslog_task));
  } else {
    const int64_t total_misslog_cnt = misslog_task.missing_info_.get_total_misslog_cnt();
    int64_t fetched_missing_log_cnt = 0;
    ObArrayImpl<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> batched_misslog_lsn_arr;
    const int64_t start_ts = get_timestamp();
    LOG_INFO("[MISS_LOG][FETCH_REDO][BEGIN]", KR(ret),
        "part_trans_id", misslog_task.get_part_trans_id(), K(total_misslog_cnt));

    while (OB_SUCC(ret) && ! stop_flag && fetched_missing_log_cnt < total_misslog_cnt) {
      batched_misslog_lsn_arr.reset();

      if (OB_FAIL(build_batch_misslog_lsn_arr_(
          fetched_missing_log_cnt,
          misslog_task.missing_info_,
          batched_misslog_lsn_arr))) {
        LOG_ERROR("build_batch_misslog_lsn_arr_ failed", KR(ret),
            K(fetched_missing_log_cnt), K(misslog_task));
      } else if (OB_FAIL(fetch_miss_log_with_retry_(
          misslog_task,
          batched_misslog_lsn_arr,
          fetch_log_srpc,
          stop_flag))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("fetch_miss_log_with_retry_ failed", KR(ret), K(batched_misslog_lsn_arr), K(misslog_task));
        }
      } else {
        const obrpc::ObRpcResultCode &rcode = fetch_log_srpc.get_result_code();
        const obrpc::ObCdcLSFetchLogResp &resp = fetch_log_srpc.get_resp();

        if (OB_FAIL(rcode.rcode_) || OB_FAIL(resp.get_err())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("fetch log fail on rpc", KR(ret), K(rcode), K(resp), K(batched_misslog_lsn_arr));
        } else {
          // check next_miss_lsn
          bool is_next_miss_lsn_match = false;
          palf::LSN next_miss_lsn = resp.get_next_miss_lsn();
          const int64_t batch_cnt = batched_misslog_lsn_arr.count();
          const int64_t resp_log_cnt = resp.get_log_num();

          if (batch_cnt == resp_log_cnt) {
            is_next_miss_lsn_match = (batched_misslog_lsn_arr.at(batch_cnt - 1).miss_lsn_ == next_miss_lsn);
          } else if (batch_cnt > resp_log_cnt) {
            is_next_miss_lsn_match = (batched_misslog_lsn_arr.at(resp_log_cnt).miss_lsn_ == next_miss_lsn);
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("too many misslog fetched", KR(ret), K(next_miss_lsn), K(batch_cnt),
                K(resp_log_cnt),K(resp), K(misslog_task));
          }

          if (OB_SUCC(ret)) {
            if (!is_next_miss_lsn_match) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("misslog fetched is not match batched_misslog_lsn_arr requested", KR(ret),
                  K(next_miss_lsn), K(batch_cnt), K(resp_log_cnt), K(batched_misslog_lsn_arr), K(resp), K(misslog_task));
            } else if (OB_FAIL(read_batch_misslog_(
                misslog_task.ls_fetch_ctx_,
                resp,
                fetched_missing_log_cnt,
                misslog_task.tsi_,
                misslog_task.missing_info_))) {
              // expected no misslog found while resolving normal log.
              LOG_ERROR("read_batch_misslog failed", KR(ret), K(fetched_missing_log_cnt), K(misslog_task));
            }
          }
        }
      }
    }
    LOG_INFO("[MISS_LOG][FETCH_REDO][END]", KR(ret),
        "part_trans_id", misslog_task.missing_info_.get_part_trans_id(),
        K(total_misslog_cnt),
        "cost", get_timestamp() - start_ts);
  }

  return ret;
}

// split all miss_logs by batch
int ObCDCMissLogHandler::build_batch_misslog_lsn_arr_(
    const int64_t fetched_log_idx,
    IObCDCPartTransResolver::MissingLogInfo &missing_log_info,
    ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &batched_misslog_lsn_arr)
{
  int ret = OB_SUCCESS;

  int64_t batched_cnt = 0;
  static int64_t MAX_MISSLOG_CNT_PER_RPC= 1024;
  const ObLogLSNArray &miss_redo_or_state_log_arr = missing_log_info.get_miss_redo_lsn_arr();
  const int64_t miss_log_cnt = miss_redo_or_state_log_arr.count();

  if (OB_UNLIKELY(0 < batched_misslog_lsn_arr.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid batched_misslog_lsn_arr", KR(ret), K(batched_misslog_lsn_arr));
  } else {
    batched_misslog_lsn_arr.reset();

    // fetched_log_idx is log_count that already fetched after last batch rpc
    // for miss_redo_or_state_log_arr with 100 miss_log and MAX_MISSLOG_CNT_PER_RPC = 10
    // fetched_log_idx start from 0, if fetched 8 miss_log in one rpc, then fetched_log_idx is 8,
    // and for next batch, miss_redo_or_state_log_arr.at(8) is the 9th miss_log as expected.
    for (int idx = fetched_log_idx; OB_SUCC(ret) && batched_cnt < MAX_MISSLOG_CNT_PER_RPC && idx < miss_log_cnt; idx++) {
      const palf::LSN &lsn = miss_redo_or_state_log_arr.at(idx);
      ObCdcLSFetchMissLogReq::MissLogParam param;
      param.miss_lsn_ = lsn;
      if (OB_FAIL(batched_misslog_lsn_arr.push_back(param))) {
        LOG_ERROR("push_back missing_log lsn into batched_misslog_lsn_arr failed", KR(ret), K(idx),
            K(fetched_log_idx), K(miss_redo_or_state_log_arr), K(batched_misslog_lsn_arr), K(param));
      } else {
        batched_cnt++;
      }
    }
  }
  ObCStringHelper helper;
  _LOG_INFO("[MISS_LOG][BATCH_MISSLOG][PART_TRANS_ID=%s][PROGRESS=%ld/%ld][BATCH_SIZE=%ld]",
        helper.convert(missing_log_info.get_part_trans_id()),
        fetched_log_idx,
        miss_log_cnt,
        batched_misslog_lsn_arr.count());

  return ret;
}

// read batched misslog
int ObCDCMissLogHandler::read_batch_misslog_(
    LSFetchCtx &ls_fetch_ctx,
    const obrpc::ObCdcLSFetchLogResp &resp,
    int64_t &fetched_missing_log_cnt,
    logfetcher::TransStatInfo &tsi,
    IObCDCPartTransResolver::MissingLogInfo &missing_info)
{
  int ret = OB_SUCCESS;
  LOG_INFO("read_batch_misslog_ begin", "tls_id", ls_fetch_ctx.get_tls_id(), K(resp), K(fetched_missing_log_cnt));

  const int64_t total_misslog_cnt = missing_info.get_total_misslog_cnt();
  const char *buf = resp.get_log_entry_buf();
  const int64_t len = resp.get_pos();
  int64_t pos = 0;
  const int64_t log_cnt = resp.get_log_num();
  const ObLogLSNArray &org_misslog_arr = missing_info.get_miss_redo_lsn_arr();
  int64_t start_ts = get_timestamp();

  if (OB_UNLIKELY(log_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("expected valid log count from FetchLogSRpc for misslog", KR(ret), K(resp));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < log_cnt; idx++) {
      if (fetched_missing_log_cnt >= total_misslog_cnt) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fetched_missing_log_cnt is more than total_misslog_cnt", KR(ret),
            K(fetched_missing_log_cnt), K(missing_info), K(idx), K(resp));
      } else {
        palf::LSN misslog_lsn;
        palf::LogEntry miss_log_entry;
        misslog_lsn.reset();
        miss_log_entry.reset();
        IObCDCPartTransResolver::MissingLogInfo tmp_miss_info;
        tmp_miss_info.set_resolving_miss_log();

        if (org_misslog_arr.count() == fetched_missing_log_cnt) {
          // TODO check it!
          // already consume the all miss_redo_log, but still exist one miss_record_log.
          // lsn record_log is the last miss_log to fetch.
          if (OB_FAIL(missing_info.get_miss_record_or_state_log_lsn(misslog_lsn))) {
            if (OB_ENTRY_NOT_EXIST == ret) {
              LOG_ERROR("expect valid miss-record_log_lsn", KR(ret), K(missing_info), K(fetched_missing_log_cnt), K(ls_fetch_ctx));
            } else {
              LOG_ERROR("get_miss_record_or_state_log_lsn failed", KR(ret), K(missing_info), K(fetched_missing_log_cnt), K(ls_fetch_ctx));
            }
          }
        } else if (OB_FAIL(org_misslog_arr.at(fetched_missing_log_cnt, misslog_lsn))) {
          LOG_ERROR("get misslog_lsn fail", KR(ret), K(fetched_missing_log_cnt),
              K(idx), K(org_misslog_arr), K(resp));
        }

        if (FAILEDx(miss_log_entry.deserialize(buf, len, pos))) {
          LOG_ERROR("deserialize miss_log_entry fail", KR(ret), K(len), K(pos));
        } else if (OB_FAIL(ls_fetch_ctx.read_miss_tx_log(miss_log_entry, misslog_lsn, tsi, tmp_miss_info))) {
          LOG_ERROR("read_miss_log fail", KR(ret), K(miss_log_entry),
              K(misslog_lsn), K(fetched_missing_log_cnt), K(idx), K(tmp_miss_info));
        } else {
          fetched_missing_log_cnt++;
          // update last misslog submit ts to orig missing_info
          missing_info.set_last_misslog_progress(tmp_miss_info.get_last_misslog_progress());
        }
      }
    }
  }

  int64_t read_batch_missing_cost = get_timestamp() - start_ts;
  const int64_t handle_miss_progress = missing_info.get_last_misslog_progress();
  ObCStringHelper helper;
  _LOG_INFO("[MISS_LOG][READ_MISSLOG][PART_TRANS_ID=%s][COST=%ld][PROGRESS_CNT=%ld/%ld][PROGRESS_SCN=%ld(%s)]",
      helper.convert(missing_info.get_part_trans_id()),
      read_batch_missing_cost,
      fetched_missing_log_cnt,
      org_misslog_arr.count(),
      handle_miss_progress,
      NTS_TO_STR(handle_miss_progress));

  return ret;
}

int ObCDCMissLogHandler::alloc_fetch_log_srpc_(const uint64_t tenant_id, FetchLogSRpc *&fetch_log_srpc)
{
  int ret = OB_SUCCESS;
  void *buf = ob_cdc_malloc(sizeof(FetchLogSRpc), ObModIds::OB_LOG_FETCH_LOG_SRPC, tenant_id);

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for FetchLogSRpc fail", KR(ret), K(tenant_id), K(sizeof(FetchLogSRpc)));
  } else if (OB_ISNULL(fetch_log_srpc = new(buf) FetchLogSRpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("construct fetch miss log srpc fail", KR(ret), K(tenant_id), K(buf));
  } else {
    // success
  }

  return ret;
}

void ObCDCMissLogHandler::free_fetch_log_srpc_(FetchLogSRpc *fetch_log_srpc)
{
  if (OB_NOT_NULL(fetch_log_srpc)) {
    fetch_log_srpc->~FetchLogSRpc();
    ob_cdc_free(fetch_log_srpc);
    fetch_log_srpc = nullptr;
  }
}

int ObCDCMissLogHandler::fetch_miss_log_with_retry_(
    MissLogTask &misslog_task,
    const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
    FetchLogSRpc &fetch_srpc,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  int64_t fetch_log_timeout = g_rpc_timeout; // default fetch_log_timeoout to user_config;

  const int64_t start_ts = get_timestamp();
  const int64_t end_ts = start_ts + RETRY_TIMEOUT;
  int64_t cur_ts = start_ts;
  int64_t try_cnt = 0;
  bool rpc_succ = false;
  bool rpc_fetch_no_log = false;
  const bool test_mode_misslog_errsim = (1 == TCONF.test_mode_on && 1 == TCONF.test_fetch_missing_errsim);
  int test_mode_fail_max_cnt = test_mode_misslog_errsim ? 2 : 0;
  int test_mode_fail_cnt = 0;

  while (! stop_flag && ! rpc_succ && cur_ts < end_ts) {
    bool has_valid_feedback = false;
    try_cnt ++;
    if (OB_FAIL(fetch_miss_log_(
        miss_log_array,
        fetch_log_timeout,
        misslog_task,
        fetch_srpc,
        stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("fetch_miss_log failed", KR(ret), K(misslog_task));
      }
    } else {
      const obrpc::ObRpcResultCode &rcode = fetch_srpc.get_result_code();
      const obrpc::ObCdcLSFetchLogResp &resp = fetch_srpc.get_resp();

      if (OB_SUCCESS != rcode.rcode_ || OB_SUCCESS != resp.get_err()) {
        if (OB_TIMEOUT == rcode.rcode_ || OB_TIMEOUT == resp.get_err() || OB_TIMEOUT == resp.get_debug_err()) {
          ret = OB_TIMEOUT;
        } else {
          if (OB_SUCCESS != rcode.rcode_) {
            ret = rcode.rcode_;
          } else if (OB_SUCCESS != resp.get_err()) {
            ret = resp.get_err();
          } else if (OB_SUCCESS != resp.get_debug_err()) {
            ret = resp.get_debug_err();
          }

          if (OB_ERR_OUT_OF_LOWER_BOUND == ret) {
            rpc_fetch_no_log = true;
            // exit if fatal error
            LOG_ERROR("[MISS_LOG] fetch log fail on rpc, log has been recycled", KR(ret), K(rpc_fetch_no_log), K(rcode), K(resp), K(misslog_task));
          } else {
            LOG_WARN("[MISS_LOG] fetch log fail on rpc", KR(ret), K(rpc_fetch_no_log), K(rcode), K(resp), K(misslog_task));
          }
        }
      } else if (OB_UNLIKELY(test_mode_fail_cnt++ < test_mode_fail_max_cnt)) {
        ret = OB_TIMEOUT;
        LOG_INFO("mock fetch log fail in test mode", KR(ret), K(test_mode_fail_cnt), K(test_mode_fail_max_cnt), "end_time", TS_TO_STR(end_ts));
      } else if (FALSE_IT(check_feedback_(resp, has_valid_feedback, rpc_fetch_no_log))) {
      } else if (OB_UNLIKELY(resp.get_log_num() <= 0)) {
        LOG_WARN("[MISS_LOG][EMPTY_RPC_RESPONSE], need retry", K(resp), K(misslog_task));
      } else {
        rpc_succ = true;
      }

      if (OB_TIMEOUT == ret) {
        ObCStringHelper helper;
        // allow adjust max_rpc_timeout by change config in libobcdc.conf while fetch_log_timeout * 2 is larger than MAX_RPC_TIMEOUT
        const int64_t max_rpc_timeout = std::max(MAX_RPC_TIMEOUT, g_rpc_timeout);
        const int64_t new_fetch_log_timeout = std::min(max_rpc_timeout, fetch_log_timeout * 2);
        _LOG_INFO("[MISS_LOG][FETCH_TIMEOUT][ADJUST_FETCH_MISSLOG_TIMEOUT][FROM=%s][TO=%s][rpc_rcode=%s][rpc_response=%s]",
            TVAL_TO_STR(fetch_log_timeout), TVAL_TO_STR(new_fetch_log_timeout), helper.convert(rcode), helper.convert(resp));
        fetch_log_timeout = new_fetch_log_timeout;
      }
    }

    if (OB_UNLIKELY(! rpc_succ || has_valid_feedback)) {
      const uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
      // cdcservice not support fetch_miss_log on server didn't fetch_log(normal mode) for the ls(in current cdc progress) before.
      misslog_task.need_change_server_ = (cluster_version >= MOCK_CLUSTER_VERSION_4_2_1_7  && cluster_version < CLUSTER_VERSION_4_2_2_0)
          || (cluster_version >= MOCK_CLUSTER_VERSION_4_2_5_0 && cluster_version < CLUSTER_VERSION_4_3_0_0)
          || cluster_version >= CLUSTER_VERSION_4_3_4_0;

      if (rpc_fetch_no_log) {
        LOG_WARN("[MISS_LOG][FETCH_NO_LOG]", KR(ret), K(misslog_task), "rpc_response", fetch_srpc.get_resp(), "rpc_request", fetch_srpc.get_req());
      }
    }


    cur_ts = get_timestamp();
  } // end while

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  ObCStringHelper helper;
  _LOG_INFO("[MISS_LOG][FETCH_MISSLOG][ret=%d][PART_TRANS_ID=%s][COST=%ld][LOG_CNT=%ld][FETCH_STATUS=%s][FETCH_ROUND=%ld]", ret,
      helper.convert(misslog_task.get_part_trans_id()),
      cur_ts - start_ts,
      miss_log_array.count(),
      helper.convert(fetch_srpc.get_resp().get_fetch_status()),
      try_cnt);

  return ret;
}

int ObCDCMissLogHandler::fetch_miss_log_(
    const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
    const int64_t timeout,
    MissLogTask &misslog_task,
    FetchLogSRpc &fetch_srpc,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  const ClientFetchingMode fetching_mode = misslog_task.ls_fetch_ctx_.get_fetching_mode();
  const int64_t last_handle_progress = misslog_task.get_handle_progress();
  LSFetchCtx &ls_fetch_ctx = misslog_task.ls_fetch_ctx_;

  if (OB_UNLIKELY(! is_fetching_mode_valid(fetching_mode))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fetching mode is not valid", KR(ret), K(fetching_mode));
  } else if (is_integrated_fetching_mode(fetching_mode)) {
    if (OB_FAIL(misslog_task.try_change_server(timeout, stop_flag))) {
      if (OB_IN_STOP_STATE != ret) {
        LOG_ERROR("get fetch_miss_log server failed", KR(ret), K(ls_fetch_ctx));
      }
    } else if (OB_FAIL(fetch_srpc.fetch_log(
        misslog_task.rpc_,
        ls_fetch_ctx.get_tls_id().get_tenant_id(),
        ls_fetch_ctx.get_tls_id().get_ls_id(),
        miss_log_array,
        misslog_task.svr_,
        timeout,
        last_handle_progress))) {
      LOG_ERROR("fetch_misslog_rpc exec failed", KR(ret), K(miss_log_array), K(last_handle_progress), K(misslog_task));
      ret = OB_NEED_RETRY;
    } else {
      // succ
    }
  } else if (is_direct_fetching_mode(fetching_mode)) {
    // mock FetchLogSRpc here
    if (OB_FAIL(fetch_miss_log_direct_(miss_log_array, timeout, fetch_srpc, ls_fetch_ctx))) {
      LOG_ERROR("fetch missing log direct failed", KR(ret), K(miss_log_array), K(misslog_task));
      // rewrite ret code to make sure that cdc wouldn't exit because fetch_missing_log_direct_ failed.
      ret = OB_NEED_RETRY;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid fetching mode", KR(ret), K(fetching_mode), K(misslog_task));
  }

  return ret;
}

int ObCDCMissLogHandler::fetch_miss_log_direct_(
    const ObIArray<obrpc::ObCdcLSFetchMissLogReq::MissLogParam> &miss_log_array,
    const int64_t timeout,
    FetchLogSRpc &fetch_log_srpc,
    LSFetchCtx &ls_fetch_ctx)
{
  int ret = OB_SUCCESS;
  ObCdcLSFetchLogResp *resp = NULL;
  LSFetchCtxGetSourceFunctor get_source_func(ls_fetch_ctx);
  LSFetchCtxUpdateSourceFunctor update_source_func(ls_fetch_ctx);
  const int64_t current_progress = ls_fetch_ctx.get_progress();
  const int64_t tenant_id = ls_fetch_ctx.get_tls_id().get_tenant_id();
  const ObLSID &ls_id = ls_fetch_ctx.get_tls_id().get_ls_id();
  archive::LargeBufferPool *buffer_pool = NULL;
  logservice::ObLogExternalStorageHandler *log_ext_handler = NULL;
  ObRpcResultCode rcode;
  SCN cur_scn;
  const int64_t start_fetch_ts = get_timestamp();
  const int64_t time_upper_limit = start_fetch_ts + timeout;
  bool stop_fetch = false;
  bool is_timeout = false;

  if (OB_ISNULL(resp = OB_NEW(ObCdcLSFetchLogResp, "FetchMissResp"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc ObCdcLSFetchLogResp failed", KR(ret));
  } else if (OB_FAIL(cur_scn.convert_from_ts(current_progress/1000L))) {
    LOG_ERROR("convert log progress to scn failed", KR(ret), K(current_progress));
  } else if (OB_FAIL(ls_fetch_ctx.get_large_buffer_pool(buffer_pool))) {
    LOG_ERROR("get large buffer pool when fetching missing log failed", KR(ret), K(ls_fetch_ctx));
  } else if (OB_FAIL(ls_fetch_ctx.get_log_ext_handler(log_ext_handler))) {
    LOG_ERROR("get log ext handler when fetching missing log failed", KR(ret), K(ls_fetch_ctx));
  } else {
    int64_t fetched_cnt = 0;
    const int64_t arr_cnt = miss_log_array.count();
    resp->set_next_miss_lsn(miss_log_array.at(0).miss_lsn_);
    while (OB_SUCC(ret) && !stop_fetch) {
      bool retry_on_err = false;
      while (OB_SUCC(ret) && fetched_cnt < arr_cnt && !is_timeout) {
        const int64_t start_fetch_entry_ts = get_timestamp();
        const ObCdcLSFetchMissLogReq::MissLogParam &param = miss_log_array.at(fetched_cnt);
        const LSN &missing_lsn = param.miss_lsn_;
        const char *buf;
        int64_t buf_size = 0;
        palf::LogEntry log_entry;
        palf::LSN lsn;
        logservice::ObRemoteLogpEntryIterator entry_iter(get_source_func, update_source_func);
        resp->set_next_miss_lsn(missing_lsn);

        if (get_timestamp() > time_upper_limit) {
          is_timeout = true;
        } else if (OB_FAIL(entry_iter.init(tenant_id, ls_id, cur_scn, missing_lsn,
            LSN(palf::LOG_MAX_LSN_VAL), buffer_pool, log_ext_handler, archive::ARCHIVE_FILE_DATA_BUF_SIZE))) {
          LOG_WARN("remote entry iter init failed", KR(ret));
        } else if (OB_FAIL(entry_iter.next(log_entry, lsn, buf, buf_size))) {
          retry_on_err =true;
          LOG_WARN("log entry iter failed to iterate", KR(ret));
        } else {
          if (OB_UNLIKELY(missing_lsn != lsn)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("direct fetch missing_lsn not match", KR(ret), K(tenant_id), K(ls_id),
                K(missing_lsn), K(lsn));
          } else {
            const int64_t entry_size = log_entry.get_serialize_size();
            int64_t pos = 0;
            resp->inc_log_fetch_time(get_timestamp() - start_fetch_entry_ts);

            if (! resp->has_enough_buffer(entry_size)) {
              ret = OB_BUF_NOT_ENOUGH;
            } else {
              int64_t remain_size = 0;
              char *remain_buf = resp->get_remain_buf(remain_size);
              if (OB_ISNULL(remain_buf)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("remain buffer is null", KR(ret));
              }
              else if (OB_FAIL(log_entry.serialize(remain_buf, remain_size, pos))) {
                LOG_WARN("missing LogEntry serialize failed", KR(ret), K(remain_size),
                    K(pos), K(missing_lsn), K(log_entry));
              }
              else {
                // TODO: there is an issue in entry_iter.next(), the returned buffer is not as expected
                // MEMCPY(remain_buf, buf, buf_size);
                resp->log_entry_filled(entry_size);
                fetched_cnt++;
              }
            }
          }
        }
      }// while

      if (OB_SUCC(ret)) {
        stop_fetch = true;
      } else if (OB_BUF_NOT_ENOUGH == ret) {
        stop_fetch = true;
        ret = OB_SUCCESS;
      } else if (retry_on_err) {
        ret = OB_SUCCESS;
      }
    } // while
    resp->set_l2s_net_time(0);
    resp->set_svr_queue_time(0);
    resp->set_process_time(get_timestamp() - start_fetch_ts);
  }
  // regard resp not null as sending rpc successfully
  if (OB_NOT_NULL(resp)) {
    resp->set_debug_err(ret);
    if (OB_FAIL(ret)) {
      resp->set_err(OB_ERR_SYS);
    } else {
      resp->set_err(OB_SUCCESS);
    }
    ret = OB_SUCCESS;
    rcode.rcode_ = OB_SUCCESS;
  } else {
    rcode.rcode_ = ret;
    sprintf(rcode.msg_, "failed to allocate fetchlogresp");
  }
  fetch_log_srpc.set_resp(rcode, resp);

  if (OB_NOT_NULL(resp)) {
    OB_DELETE(ObCdcLSFetchLogResp, "FetchMissResp", resp);
    resp = NULL;
  }

  return ret;
}

void ObCDCMissLogHandler::check_feedback_(
    const obrpc::ObCdcLSFetchLogResp &resp,
    bool &has_valid_feedback,
    bool &rpc_fetch_no_log)
{
  const obrpc::FeedbackType& feedback = resp.get_feedback_type();
  has_valid_feedback = (feedback != obrpc::FeedbackType::INVALID_FEEDBACK);
  rpc_fetch_no_log = (obrpc::FeedbackType::LOG_NOT_IN_THIS_SERVER == feedback);

  if (has_valid_feedback) {
    const char *rpc_feedback_info = nullptr;
    switch (feedback) {
      case obrpc::FeedbackType::LAGGED_FOLLOWER:
        rpc_feedback_info = "fetching log on lagged follower";
        break;
      case obrpc::FeedbackType::LOG_NOT_IN_THIS_SERVER:
        rpc_feedback_info = "log not in this server, may be recycled";
        break;
      case obrpc::FeedbackType::LS_OFFLINED:
        rpc_feedback_info = "fetching log on offline logstream";
        break;
      case obrpc::FeedbackType::ARCHIVE_ITER_END_BUT_LS_NOT_EXIST_IN_PALF:
        rpc_feedback_info = "archive log iter end but no valid palf on this server";
        break;
      default:
        rpc_feedback_info = "unknown feedback type";
        break;
    }
    LOG_INFO("[MISS_LOG][RPC_FEEDBACK]", K(rpc_fetch_no_log), K(resp), KCSTRING(rpc_feedback_info));
  }
}

} // namespace libobcdc
} // namespace oceanbase
