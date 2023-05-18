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

#include "storage/tx/ob_tx_ls_log_writer.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx/ob_trans_id_service.h"
#include "logservice/ob_log_base_header.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "lib/stat/ob_latch_define.h"

namespace oceanbase
{

using namespace logservice;
using namespace palf;
using namespace share;

namespace transaction
{

//****************** ObTxLsLogLimit ***********************

int64_t ObTxLSLogLimit::LOG_BUF_SIZE = 0;

uint16_t ObTxLSLogLimit::get_max_parallel(const ObTxLogType &type)
{
  uint16_t tmp_para = 0;
  switch (type) {
  case ObTxLogType::TX_START_WORKING_LOG: {
    tmp_para = START_WORKING_MAX_PARALLEL;
    break;
  }
  default: {
    tmp_para = 0;
    break;
  }
  }
  return tmp_para;
}

uint16_t ObTxLSLogLimit::get_sum_parallel()
{
  uint16_t tmp_para = 0;
  tmp_para = START_WORKING_MAX_PARALLEL;
  return tmp_para;
}

void ObTxLSLogLimit::decide_log_buf_size()
{
  LOG_BUF_SIZE = 0;
  ObLogBaseHeader base_header;
  ObTxLogHeader tx_header;
  ObTxLogBlockHeader block_header(UINT64_MAX, INT64_MAX, INT64_MAX, common::ObAddr());
  ObTxStartWorkingLog sw_log(INT_MAX64);

  // block_header.before_serialize();
  // sw_log.before_serialize();
  int64_t max_body_size = 0;
  LOG_BUF_SIZE = base_header.get_serialize_size() + tx_header.get_serialize_size() + block_header.get_serialize_size();
  max_body_size = OB_MAX(max_body_size, sw_log.get_serialize_size());

  LOG_BUF_SIZE = LOG_BUF_SIZE + max_body_size;
}

//********************************************************

//****************** ObTxLSLogCb *************************
ObTxLSLogCb::ObTxLSLogCb(ObTxLSLogWriter *base)
{
  log_buf_ = nullptr;
  reset();
  base_wr_ = base;
}

ObTxLSLogCb::~ObTxLSLogCb()
{
  ObTxBaseLogCb::reset();
  reset();
}

void ObTxLSLogCb::reuse()
{
  ObTxBaseLogCb::reuse();
  type_ = ObTxLogType::UNKNOWN;
  pos_ = 0;
}

void ObTxLSLogCb::reset()
{
  reuse();
  if (nullptr != log_buf_) {
    ob_free(log_buf_);
  }
  log_buf_ = nullptr;
}

int ObTxLSLogCb::occupy_by(const ObTxLogType &log_type)
{
  int ret = OB_SUCCESS;
  if (!ObTxLogTypeChecker::is_ls_log(log_type) || OB_ISNULL(base_wr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[TxLsLogWriter] invalid arguments", KR(ret), K(log_type), KP(base_wr_));
  } else if (ObTxLogType::UNKNOWN != type_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "[TxLsLogWriter] cb has been used", KR(ret));
  } else {
    type_ = log_type;
  }
  return ret;
}

int ObTxLSLogCb::on_success()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(base_wr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[TxLsLogWriter] invalid arguments", KP(base_wr_));
  } else if (OB_FAIL(base_wr_->on_success(this))) {
    TRANS_LOG(WARN, "[TxLsLogWriter] on_success failed", KR(ret), K(log_ts_), K(type_));
  }
  return ret;
}

int ObTxLSLogCb::on_failure()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(base_wr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[TxLsLogWriter] invalid arguments", KP(base_wr_));
  } else if (base_wr_->on_failure(this)) {
    TRANS_LOG(WARN, "[TxLsLogWriter] on_failure failed", KR(ret), K(log_ts_), K(type_));
  }
  return ret;
}

int ObTxLSLogCb::alloc_log_buf_()
{
  int ret = OB_SUCCESS;

  ObMemAttr attr(base_wr_->get_tenant_id(), "TxLSLogBuf");
  SET_USE_500(attr);
  if (0 == ObTxLSLogLimit::LOG_BUF_SIZE || nullptr != log_buf_) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[TxLsLogWriter] invalid arguments", KR(ret), K(ObTxLSLogLimit::LOG_BUF_SIZE), KP(log_buf_));
  } else if (nullptr
             == (log_buf_ = (char *)ob_malloc(ObTxLSLogLimit::LOG_BUF_SIZE, attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "[TxLsLogWriter] allocate memory failed", KR(ret), K(ObTxLSLogLimit::LOG_BUF_SIZE));
  }
  return ret;
}
//********************************************************

//******************** ObTxLSLogWriter *******************
ObTxLSLogWriter::ObTxLSLogWriter() : cbs_lock_(common::ObLatchIds::TX_LS_LOG_WRITER_LOCK) { reset(); }

ObTxLSLogWriter::~ObTxLSLogWriter() { reset(); }

int ObTxLSLogWriter::init(const int64_t tenant_id,
                          const ObLSID &ls_id,
                          ObITxLogAdapter * adapter,
                          ObLSTxCtxMgr *ctx_mgr)
  {
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(tx_log_adapter_) || OB_NOT_NULL(ctx_mgr_)) {
    ret = OB_INIT_TWICE;
  } else if (!ls_id.is_valid() || OB_ISNULL(adapter) || OB_ISNULL(ctx_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[TxLsLogWriter] invalid arguments", K(ls_id), KP(adapter), KP(ctx_mgr));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    tenant_id_ = tenant_id;
    ctx_mgr_ = ctx_mgr;
    tx_log_adapter_ = adapter;
    ObTxLSLogLimit::decide_log_buf_size();
    while (free_cbs_.get_size() < DEFAULT_LOG_CB_CNT && OB_SUCC(ret)) {
      if (OB_FAIL(append_free_log_cb_())) {
        TRANS_LOG(WARN, "init free log cb error", KR(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ATOMIC_STORE(&is_stopped_, false);
    TRANS_LOG(DEBUG, "[TxLsLogWriter] init success", K(ret), K(ls_id_));
  } else {
    TRANS_LOG(WARN, "[TxLsLogWriter] init failed", K(ret), K(ls_id_));
  }
  return ret;
}

int ObTxLSLogWriter::stop()
{
  int ret = OB_SUCCESS;

  ATOMIC_STORE(&is_stopped_, true);

  return ret;
}

int ObTxLSLogWriter::wait()
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard cbs_guard(cbs_lock_);
  if (!keep_alive_cbs_.is_empty() || !start_working_cbs_.is_empty()) {
    ret = OB_EAGAIN;
  }
  return ret;
}


void ObTxLSLogWriter::reset()
{
  destroy();
  ATOMIC_STORE(&is_stopped_, true);
  keep_alive_cbs_.reset();
  start_working_cbs_.reset();
  free_cbs_.reset();
  tenant_id_ = OB_SERVER_TENANT_ID;
  ls_id_.reset();
  ctx_mgr_ = nullptr;
  tx_log_adapter_ = nullptr;
}

void ObTxLSLogWriter::destroy()
{
  destroy_cbs_(keep_alive_cbs_);
  destroy_cbs_(start_working_cbs_);
  destroy_cbs_(free_cbs_);
}

int ObTxLSLogWriter::submit_start_working_log(const int64_t &leader_epoch, SCN &log_ts)
{
  int ret = OB_SUCCESS;

  ObTxStartWorkingLog sw_log(leader_epoch);
  if (OB_FAIL(submit_ls_log_(sw_log, logservice::ObReplayBarrierType::STRICT_BARRIER, false, log_ts))) {
    TRANS_LOG(WARN, "[TxLsLogWriter] submit start working log failed", KR(ret));
  }

  return ret;
}

int ObTxLSLogWriter::on_success(ObTxLSLogCb *cb)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard cb_guard(cbs_lock_);

  const ObTxLogType log_type = cb->get_log_type();

  switch (log_type) {
  case ObTxLogType::TX_START_WORKING_LOG: {
    if (OB_FAIL(ctx_mgr_->on_start_working_log_cb_succ(cb->get_log_ts()))) {
      TRANS_LOG(WARN, "start working log callback failed", KR(ret));
    }
    break;
  }
  // TODO, other types
  default: {
    TRANS_LOG(WARN, "unknown log type", K(log_type));
  }
  }
  return_log_cb_(cb);
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[TxLsLogWriter] on success", KR(ret), K(cb->get_log_type()),
              K(cb->get_log_ts()));
  }
  return ret;
}

int ObTxLSLogWriter::on_failure(ObTxLSLogCb *cb)
{
  int ret = OB_SUCCESS;

  ObSpinLockGuard cb_guard(cbs_lock_);

  const ObTxLogType log_type = cb->get_log_type();

  switch (log_type) {
  case ObTxLogType::TX_START_WORKING_LOG: {
    if (OB_FAIL(ctx_mgr_->on_start_working_log_cb_fail())) {
      TRANS_LOG(WARN, "start working log callback failed", KR(ret));
    }
    break;
  }
  // TODO, other types
  default: {
    TRANS_LOG(WARN, "unknown log type", K(log_type));
  }
  }
  TRANS_LOG(INFO, "[TxLsLogWriter] on failure", KR(ret), K(cb->get_log_type()),
          K(cb->get_log_ts()));
  return_log_cb_(cb);
  return ret;
}

int ObTxLSLogWriter::get_log_cb_(const ObTxLogType &log_type, ObTxLSLogCb *&cb)
{
  int ret = OB_SUCCESS;
  ObDList<ObTxLSLogCb> *tmp_cbs = nullptr;

  if (nullptr == (tmp_cbs = get_target_cbs_(log_type))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[TxLsLogWriter] INVALID LOG TYPE", K(log_type));
  } else if (reach_parallel_cbs_limit_(log_type, tmp_cbs->get_size())) {
    ret = OB_TX_NOLOGCB;
    TRANS_LOG(INFO, "[TxLsLogWriter] reach max parallel limit, need retry", KR(ret), K(log_type),
              K(tmp_cbs->get_size()));
  } else if (!enough_log_cb_() && (OB_FAIL(append_free_log_cb_()))) {
    TRANS_LOG(WARN, "[TxLsLogWriter] append free log_cb error", KR(ret));
  } else {
    // move from free_cbs_
    cb = free_cbs_.remove_first();
    if (OB_FAIL(cb->occupy_by(log_type))) {
      TRANS_LOG(ERROR, "[TxLsLogWriter] occupy by ls log_cb failed", KR(ret), K(log_type), K(*cb));
      free_cbs_.add_last(cb);
    } else {
      tmp_cbs->add_last(cb);
    }
  }

  return ret;
}

int ObTxLSLogWriter::return_log_cb_(ObTxLSLogCb *cb)
{
  int ret = OB_SUCCESS;

  ObDList<ObTxLSLogCb> *tmp_cbs = nullptr;

  if (nullptr == cb || nullptr == (tmp_cbs = get_target_cbs_(cb->get_log_type()))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[TxLsLogWriter] INVALID LOG TYPE", K(cb->get_log_type()));
  } else {
    tmp_cbs->remove(cb);
    cb->reuse();
    free_cbs_.add_first(cb);

    TRANS_LOG(DEBUG, "[TxLsLogWriter] success return log_cb", K(cb->get_log_type()),
              K(tmp_cbs->get_size()), K(free_cbs_.get_size()));
  }
  return ret;
}

bool ObTxLSLogWriter::enough_log_cb_() { return free_cbs_.is_empty(); }

int ObTxLSLogWriter::append_free_log_cb_()
{
  int ret = OB_SUCCESS;
  char *cb_buf = nullptr;
  ObTxLSLogCb *tmp_cb = nullptr;
  if (nullptr
      == (cb_buf = (char *)ob_malloc(sizeof(ObTxLSLogCb) * APPEND_LOG_CB_CNT,
                                     ObMemAttr(tenant_id_, "ObTxLSLogCb")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    for (int i = 0; i < APPEND_LOG_CB_CNT; i++) {
      TRANS_LOG(DEBUG, "[TxLsLogWriter] append_free_log_cb", K(ret), K(free_cbs_.get_size()), K(i));
      tmp_cb = new (cb_buf + i * sizeof(ObTxLSLogCb)) ObTxLSLogCb(this);
      free_cbs_.add_last(tmp_cb);
    }
  }

  return ret;
}

bool ObTxLSLogWriter::reach_parallel_cbs_limit_(const ObTxLogType &log_type, const int32_t &cbs_cnt)
{
  bool res = true;
  if(cbs_cnt < ObTxLSLogLimit::get_max_parallel(log_type))
  {
    res = false;
  }

  return res;
}
ObDList<ObTxLSLogCb> *ObTxLSLogWriter::get_target_cbs_(const ObTxLogType &type)
{
  ObDList<ObTxLSLogCb> *ptr = nullptr;
  switch (type) {
  case ObTxLogType::TX_START_WORKING_LOG: {
    ptr = &start_working_cbs_;
    break;
  }
  default: {
    ptr = nullptr;
    break;
  }
  }
  return ptr;
}

int64_t ObTxLSLogWriter::generate_replay_hint_(const ObTxLogType &type)
{
  int64_t hint = 0;
  const int64_t id = ls_id_.id();
  int64_t log_type = static_cast<int64_t>(type);
  hint = murmurhash(&id, sizeof(id), hint);
  hint = murmurhash(&log_type, sizeof(log_type), hint);
  return std::abs(hint);
}

void ObTxLSLogWriter::destroy_cbs_(common::ObDList<ObTxLSLogCb> &cbs)
{
  ObTxLSLogCb *cb_ptr = nullptr;
  while (!cbs.is_empty()) {
    if (OB_NOT_NULL(cb_ptr = cbs.remove_first())) {
      cb_ptr->~ObTxLSLogCb();
      ob_free(cb_ptr);
    }
  }
}

//********************************************************
} // namespace transaction
} // namespace oceanbase
