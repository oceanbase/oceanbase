/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_keep_alive_ls_handler.h"
#include "logservice/ob_log_handler.h"
#include "storage/tx/ob_ts_mgr.h"

#define USING_LOG_PREFIX TRANS

namespace oceanbase
{

using namespace logservice;
using namespace share;

namespace transaction
{

OB_SERIALIZE_MEMBER(ObKeepAliveLogBody, compat_bit_, min_start_scn_, min_start_status_);

int64_t ObKeepAliveLogBody::get_max_serialize_size()
{
  SCN scn = SCN::max_scn();
  ObKeepAliveLogBody max_log_body(INT64_MAX, scn, MinStartScnStatus::MAX);
  return max_log_body.get_serialize_size();
}

int ObKeepAliveLSHandler::init(const int64_t tenant_id, const ObLSID &ls_id,
                               logservice::ObLogHandler *log_handler_ptr)
{
  int ret = OB_SUCCESS;
  logservice::ObLogBaseHeader base_header(ObLogBaseType::KEEP_ALIVE_LOG_BASE_TYPE,
                                          ObReplayBarrierType::NO_NEED_BARRIER,INT64_MAX);
  submit_buf_len_ = base_header.get_serialize_size() + ObKeepAliveLogBody::get_max_serialize_size();
  submit_buf_pos_ = 0;

  if (OB_ISNULL(log_handler_ptr) || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_NOT_NULL(log_handler_ptr_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(submit_buf_ =
                       static_cast<char *>(ob_malloc(submit_buf_len_, ObMemAttr(tenant_id, "KeepAliveBuf"))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "[Keep Alive] submit_buf alloc failed", K(ret), KP(submit_buf_),
              K(base_header));
  } else {
    ls_id_ = ls_id;
    is_busy_ = false;
    is_master_ = false;
    is_stopped_ = false;
    log_handler_ptr_ = log_handler_ptr;
    stat_info_.reset();
  }

  return ret;
}

void ObKeepAliveLSHandler::stop()
{
  ATOMIC_STORE(&is_stopped_, true);
}

bool ObKeepAliveLSHandler::check_safe_destory()
{
  return !is_busy();
}

void ObKeepAliveLSHandler::destroy() { reset(); }

void ObKeepAliveLSHandler::reset()
{
  if (OB_NOT_NULL(submit_buf_)) {
    ob_free(submit_buf_);
  }
  submit_buf_ = nullptr;
  submit_buf_len_ = 0;
  submit_buf_pos_ = 0;
  log_handler_ptr_ = nullptr;
  is_busy_ = false;
  is_master_ = false;
  is_stopped_ = false;
  sys_ls_end_scn_.set_min();
  ls_id_.reset();
  tmp_keep_alive_info_.reset();
  durable_keep_alive_info_.reset();
  stat_info_.reset();
}

void ObKeepAliveLSHandler::clear_keep_alive_smaller_scn_info()
{
  SpinWLockGuard guard(lock_);
  FLOG_INFO("[Keep Alive] clear keep alive ls info", K(ls_id_), K(tmp_keep_alive_info_), K(durable_keep_alive_info_));
  tmp_keep_alive_info_.reset();
  durable_keep_alive_info_.reset();
}

int ObKeepAliveLSHandler::try_submit_log(const SCN &min_start_scn, MinStartScnStatus min_start_status)
{
  int ret = OB_SUCCESS;
  palf::LSN lsn;
  SCN scn = SCN::min_scn();
  share::SCN ref_scn;

  SpinWLockGuard guard(lock_);

  if (OB_ISNULL(log_handler_ptr_)) {
    stat_info_.other_error_cnt += 1;
    ret = OB_NOT_INIT;
  } else if (!ATOMIC_LOAD(&is_master_)) {
    stat_info_.not_master_cnt += 1;
    // ret = OB_NOT_MASTER;
  } else if (ATOMIC_LOAD(&is_busy_)) {
    stat_info_.cb_busy_cnt += 1;
    // ret = OB_TX_NOLOGCB;
  } else if (!check_gts_and_get_ref_scn_(ref_scn) && min_start_status == MinStartScnStatus::UNKOWN) {
    stat_info_.near_to_gts_cnt += 1;
    // ret = OB_OP_NOT_ALLOW;
  } else {
    ATOMIC_STORE(&is_busy_, true);
    if (ATOMIC_LOAD(&is_stopped_)) {
      ATOMIC_STORE(&is_busy_, false);
      TRANS_LOG(INFO, "ls hash stopped", K(ret));
    } else if (OB_FAIL(serialize_keep_alive_log_(min_start_scn, min_start_status))) {
      ATOMIC_STORE(&is_busy_, false);
      TRANS_LOG(WARN, "[Keep Alive] serialize keep alive log failed", K(ret), K(ls_id_));
    } else if (OB_FAIL(log_handler_ptr_->append(submit_buf_, submit_buf_pos_, ref_scn, true,
                                                false/*allow_compression*/, this, lsn, scn))) {
      stat_info_.other_error_cnt += 1;
      ATOMIC_STORE(&is_busy_, false);
      TRANS_LOG(WARN, "[Keep Alive] submit keep alive log failed", K(ret), K(ls_id_));
    } else {
      stat_info_.submit_succ_cnt += 1;
      tmp_keep_alive_info_.loop_job_succ_scn_ = scn;
      tmp_keep_alive_info_.lsn_ = lsn;
      tmp_keep_alive_info_.min_start_status_ = min_start_status;
      tmp_keep_alive_info_.min_start_scn_ = min_start_scn;
      TRANS_LOG(DEBUG, "[Keep Alive] submit keep alive log success", K(ret), K(ls_id_),
                K(tmp_keep_alive_info_), K(min_start_scn), K(min_start_status));
    }
  }

  return ret;
}

int ObKeepAliveLSHandler::on_success()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(lock_);

  durable_keep_alive_info_.replace(tmp_keep_alive_info_);
  stat_info_.stat_keepalive_info_ = durable_keep_alive_info_;


  ATOMIC_STORE(&is_busy_,false);

  return ret;
}

int ObKeepAliveLSHandler::on_failure()
{
  int ret = OB_SUCCESS;

  ATOMIC_STORE(&is_busy_,false);

  return ret;
}

int ObKeepAliveLSHandler::replay(const void *buffer,
                                 const int64_t nbytes,
                                 const palf::LSN &lsn,
                                 const SCN &scn)
{
  int ret = OB_SUCCESS;

  logservice::ObLogBaseHeader base_header;
  ObKeepAliveLogBody log_body;

  int64_t pos = 0;
  if (OB_FAIL(base_header.deserialize(static_cast<const char *>(buffer), nbytes, pos))) {
    TRANS_LOG(WARN, "[Keep Alive] deserialize base header error", K(ret), K(nbytes), K(pos));
  } else if (OB_FAIL(log_body.deserialize(static_cast<const char *>(buffer), nbytes, pos))) {
    TRANS_LOG(WARN, "[Keep Alive] deserialize log body error", K(ret), K(nbytes), K(pos));
  } else {
    SpinWLockGuard guard(lock_);
    tmp_keep_alive_info_.loop_job_succ_scn_ = scn;
    tmp_keep_alive_info_.lsn_ = lsn;
    tmp_keep_alive_info_.min_start_scn_ = log_body.get_min_start_scn();
    tmp_keep_alive_info_.min_start_status_ = log_body.get_min_start_status();
    durable_keep_alive_info_.replace(tmp_keep_alive_info_);
    stat_info_.stat_keepalive_info_ = durable_keep_alive_info_;
  }

  if (OB_SUCC(ret)) {
    TRANS_LOG(DEBUG, "[Keep Alive] replay keep alive log success", K(ret), K(base_header),
              K(log_body));
  }

  return ret;
}

void ObKeepAliveLSHandler::print_stat_info()
{
  SpinRLockGuard guard(lock_);
  ObCStringHelper helper;
  TRANS_LOG(INFO, "[Keep Alive Stat] [WRS] LS Keep Alive Info",
                  "tenant_id",          MTL_ID(),
                  "LS_ID",              ls_id_,
                  "cb_busy_cnt",        stat_info_.cb_busy_cnt,
                  "Not_Master_Cnt",     stat_info_.not_master_cnt,
                  "Near_To_GTS_Cnt",    stat_info_.near_to_gts_cnt,
                  "Other_Error_Cnt",    stat_info_.other_error_cnt,
                  "Submit_Succ_Cnt",    stat_info_.submit_succ_cnt,
                  "last_scn",           helper.convert(stat_info_.stat_keepalive_info_.loop_job_succ_scn_),
                  "last_lsn",           stat_info_.stat_keepalive_info_.lsn_,
                  "min_start_scn",      helper.convert(stat_info_.stat_keepalive_info_.min_start_scn_),
                  "min_start_status",   stat_info_.stat_keepalive_info_.min_start_status_,
                  "sys_ls_end_scn",     sys_ls_end_scn_);
  stat_info_.clear_cnt();
}

void ObKeepAliveLSHandler::get_min_start_scn(SCN &min_start_scn,
                                             SCN &keep_alive_scn,
                                             MinStartScnStatus &status)
{
  SpinRLockGuard guard(lock_);

  min_start_scn = durable_keep_alive_info_.min_start_scn_;
  keep_alive_scn = durable_keep_alive_info_.loop_job_succ_scn_;
  status = durable_keep_alive_info_.min_start_status_;
}


bool ObKeepAliveLSHandler::check_gts_and_get_ref_scn_(share::SCN &ref_scn) const
{
  bool need_submit = true;
  SCN gts;
  SCN max_scn;
  SCN end_scn;
  int ret = OB_SUCCESS;
  SCN sys_ls_end_scn = sys_ls_end_scn_.atomic_load();
  const int64_t current_ts = ObClockGenerator::getClock();
  SCN current_ts_scn;

  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  const int64_t KEEPALIVE_INTERVAL = tenant_config.is_valid() ?
      tenant_config->_keepalive_interval : KEEP_ALIVE_GTS_INTERVAL;
  const uint64_t gts_tenant_id = is_tenant_sslog_ls(MTL_ID(), ls_id_) ? get_sslog_gts_tenant_id(MTL_ID()) : MTL_ID();
  if (OB_ISNULL(log_handler_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), KP(log_handler_ptr_));
  } else if (OB_FAIL(OB_TS_MGR.get_gts(gts_tenant_id, nullptr, gts))) {
    TRANS_LOG(WARN, "get gts error", K(ret), K(ls_id_), K(gts_tenant_id));
  } else if (OB_FAIL(log_handler_ptr_->get_max_scn(max_scn))) {
    TRANS_LOG(WARN, "get max log_ts failed", K(ret));
  } else if (OB_FAIL(log_handler_ptr_->get_end_scn(end_scn))) {
    TRANS_LOG(WARN, "get end scn failed", K(ret));
  } else if (OB_FAIL(current_ts_scn.convert_from_ts(current_ts))) {
    TRANS_LOG(WARN, "convert current time to scn failed", KR(ret), K(current_ts));
  } else if (end_scn.is_valid_and_not_min()
      && sys_ls_end_scn.is_valid_and_not_min()
      && end_scn < sys_ls_end_scn) {
    need_submit = true;
  } else if (SCN::max(gts, current_ts_scn).convert_to_ts() - max_scn.convert_to_ts() >= KEEPALIVE_INTERVAL/2) {
    // Submit keep alive log if the time difference between the last log and GTS or current time exceeds KEEPALIVE_INTERVAL/2.
    //
    // The purpose is to ensure that the LS log SCN does not lag behind GTS or current time by more than KEEPALIVE_INTERVAL.
    //
    // Here, we use KEEPALIVE_INTERVAL/2 because the thread scheduling period is KEEPALIVE_INTERVAL. This ensures that
    // we submit a keep alive log approximately every KEEPALIVE_INTERVAL. Otherwise, the LS's weak read snapshot version
    // would lag behind by 2 * KEEPALIVE_INTERVAL
    //
    // For example, if KEEPALIVE_INTERVAL is 100ms and the thread scheduling period is 100ms, when the time difference
    // between the last log and GTS or current time is exactly 99ms, we would need to wait until the difference reaches
    // 199ms before submitting the keep alive log
    //
    // Why consider both GTS and current time: GTS is not updated in real-time, but periodically. This means that
    // we cannot guarantee submitting a keep alive log every KEEPALIVE_INTERVAL, resulting in the server-level weak
    // read snapshot version lagging behind.
    need_submit = true;
  } else {
    need_submit = false;
  }

  if (OB_SUCC(ret) && need_submit) {
    ref_scn = get_ref_scn_(gts, current_ts_scn);
  }

  WRS_LOG(TRACE, "[WRS] need submit keep alive log", K(need_submit),
      "tenant_id", MTL_ID(), "ls_id", ls_id_,
      K(gts), K(current_ts_scn), K(ref_scn),
      "max_scn_delta_with_gts", gts.convert_to_ts(true) - max_scn.convert_to_ts(true),
      "max_scn_delta_with_current_ts", current_ts - max_scn.convert_to_ts(true),
      K(end_scn), K(sys_ls_end_scn), KR(ret));

  return need_submit;
}

share::SCN ObKeepAliveLSHandler::get_ref_scn_(const SCN &gts, const SCN &current_ts_scn) const
{
  // if current time is greater than gts scn, use current time as ref scn
  // keep alive log will keep sync with gts or current time
  SCN ref_scn = share::SCN::max(gts, current_ts_scn);
  SCN sys_ls_end_scn = sys_ls_end_scn_.atomic_load();

  if (ref_scn.is_valid_and_not_min() && sys_ls_end_scn.is_valid_and_not_min()) {
    ref_scn = sys_ls_end_scn > ref_scn ? sys_ls_end_scn : ref_scn;
  }

  return ref_scn;
}

int ObKeepAliveLSHandler::serialize_keep_alive_log_(const SCN &min_start_scn, MinStartScnStatus status)
{
  int ret = OB_SUCCESS;

  const int64_t replay_hint = ls_id_.hash();
  logservice::ObLogBaseHeader base_header(ObLogBaseType::KEEP_ALIVE_LOG_BASE_TYPE,
                                          ObReplayBarrierType::NO_NEED_BARRIER, replay_hint);
  ObKeepAliveLogBody log_body(1, min_start_scn, status);

  if (OB_ISNULL(submit_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[Keep Alive] invalid submit buf", K(ret), KP(submit_buf_), K(submit_buf_len_),
              K(submit_buf_pos_));
  } else if (OB_FALSE_IT(submit_buf_pos_ = 0)) {
  } else if (OB_FAIL(base_header.serialize(submit_buf_, submit_buf_len_, submit_buf_pos_))) {
    TRANS_LOG(WARN, "[Keep Alive] serialize base header error", K(ret),
              K(base_header.get_serialize_size()), K(submit_buf_len_), K(submit_buf_pos_));
  } else if (OB_FAIL(log_body.serialize(submit_buf_, submit_buf_len_, submit_buf_pos_))) {
    TRANS_LOG(WARN, "[Keep Alive] serialize keep alive log body failed", K(ret),
              K(log_body.get_serialize_size()), K(submit_buf_len_), K(submit_buf_pos_));
  }

  TRANS_LOG(DEBUG, "[Keep Alive] serialize keep alive log", K(ret), K(ls_id_), K(log_body));
  return ret;
}

} // namespace transaction
} // namespace oceanbase
