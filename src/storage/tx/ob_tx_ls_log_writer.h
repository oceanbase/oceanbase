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

#ifndef OCEANBASE_TRANSACTION_OB_TX_LOGSTREAM_LOG_WRITER
#define OCEANBASE_TRANSACTION_OB_TX_LOGSTREAM_LOG_WRITER

#include "lib/list/ob_dlink_node.h"
#include "lib/lock/ob_spin_lock.h"
#include "logservice/palf/palf_callback.h"
#include "storage/tx/ob_tx_log.h"
#include "storage/tx/ob_tx_log_adapter.h"
#include "logservice/ob_log_base_header.h"

namespace oceanbase {

namespace share
{
class ObLSID;
}

namespace logservice
{
class ObLogBaseHeader;
class ObLogHandler;
class AppendCb;
}

namespace palf
{
class LSN;
} // namespace palf

namespace transaction
{
class ObLSTxCtxMgr;
class ObITxLogAdapter;

class ObTxLSLogLimit
{
public:
  static const uint16_t START_WORKING_MAX_PARALLEL = 1;

  static uint16_t get_max_parallel(const ObTxLogType &type);
  static uint16_t get_sum_parallel();

private:
  ObTxLSLogLimit();
  DISALLOW_COPY_AND_ASSIGN(ObTxLSLogLimit);
};

// Per-cb inline buffer cap for LS control logs. This is NOT a process-wide
// mutable size: serialize always uses the buffer owned by the cb.
// Bounds are derived from the log types themselves; if a log outgrows them,
// serialize falls back to an exactly-sized heap alloc with a WDIAG.
struct ObTxLSCtrlLogBufSize
{
  static const int64_t BASE_HEADER_SIZE = 16;   // ObLogBaseHeader: i16+i16+i32+i64
  static const int64_t TX_HEADER_SIZE = 8;      // ObTxLogHeader: i64
  static const int64_t BLOCK_HEADER_SIZE = 128; // ObTxLogBlockHeader (UNIS + ObAddr)
  static const int64_t INLINE_SIZE = BASE_HEADER_SIZE
                                   + TX_HEADER_SIZE
                                   + BLOCK_HEADER_SIZE
                                   + ObTxStartWorkingLog::MAX_SERIALIZE_SIZE;
};

class ObTxLSLogWriter;

// used by one thread
class ObTxLSLogCb : public ObTxBaseLogCb,
                    public common::ObDLinkBase<ObTxLSLogCb>
{
public:
  ObTxLSLogCb(ObTxLSLogWriter *base);
  ~ObTxLSLogCb();
  void reuse();
  void reset();

  int occupy_by(const ObTxLogType & log_type);
  template <typename T>
  int serialize_ls_log(T &ls_log,
                       int64_t replay_hint,
                       const enum logservice::ObReplayBarrierType barrier_type = logservice::ObReplayBarrierType::NO_NEED_BARRIER);
  char *get_log_buf() { return log_buf_; }
  int64_t get_log_pos() { return pos_; }
  const ObTxLogType &get_log_type() { return type_; }
public:
  int on_success();
  int on_failure();
  const char *get_cb_name() const override { return "TxLSLogCb"; }

public:
  INHERIT_TO_STRING_KV("ObTxBaseLogCb", ObTxBaseLogCb, K(type_), KP(log_buf_), K(pos_), KP(base_wr_));

  static const int64_t INLINE_LOG_BUF_SIZE = ObTxLSCtrlLogBufSize::INLINE_SIZE;

private:
  OB_NOINLINE int ensure_log_buf_(const int64_t need_size);
  template <typename T>
  int serialize_payload_(T &ls_log,
                         logservice::ObLogBaseHeader &base_header,
                         ObTxLogBlockHeader &block_header,
                         ObTxLogHeader &tx_header);

private:
  ObTxLogType type_; // Unkown == unused, not init
  char inline_log_buf_[INLINE_LOG_BUF_SIZE];
  char *log_buf_;
  int64_t log_buf_len_;
  int64_t pos_;
  ObTxLSLogWriter *base_wr_;
};

template <typename T>
int ObTxLSLogCb::serialize_ls_log(T &ls_log,
                                  int64_t replay_hint,
                                  const enum logservice::ObReplayBarrierType barrier_type)
{
  int ret = OB_SUCCESS;
  if (!ObTxLogTypeChecker::is_ls_log(T::LOG_TYPE) || replay_hint < 0 || T::LOG_TYPE != type_
      || OB_ISNULL(base_wr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "[TxLsLogWriter] invalid arguments", KR(ret), K(T::LOG_TYPE), K(replay_hint),
              KP(base_wr_));
  } else {
    logservice::ObLogBaseHeader base_header(logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE,
                                            barrier_type, replay_hint);
    ObTransID fake_tx_id(0); // fake a invalid tx_id
    ObTxLogBlockHeader block_header(1, 1, 1, fake_tx_id, ObAddr());
    ObTxLogHeader tx_header(T::LOG_TYPE);
    if (OB_FAIL(block_header.before_serialize())) {
      TRANS_LOG(WARN, "[TxLsLogWriter] before serialize block header error", KR(ret),
                K(block_header));
    } else {
      const int64_t need_size = base_header.get_serialize_size()
                              + block_header.get_serialize_size()
                              + tx_header.get_serialize_size()
                              + ls_log.get_serialize_size();
      if (OB_FAIL(ensure_log_buf_(need_size))) {
        TRANS_LOG(WARN, "[TxLsLogWriter] ensure log buf error", KR(ret), K(T::LOG_TYPE), K(need_size));
      } else if (OB_FAIL(serialize_payload_(ls_log, base_header, block_header, tx_header))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          const int64_t grow_to = MAX(need_size * 2, log_buf_len_ * 2);
          TRANS_LOG(WARN, "[TxLsLogWriter] ls log serialize size underestimated, retry with larger buf",
                    K(type_), "inline_buf_size", (int64_t)INLINE_LOG_BUF_SIZE,
                    "required_size", grow_to, K(log_buf_len_));
          if (OB_FAIL(ensure_log_buf_(grow_to))) {
            TRANS_LOG(WARN, "[TxLsLogWriter] ensure log buf error", KR(ret), K(T::LOG_TYPE), K(grow_to));
          } else if (OB_FAIL(serialize_payload_(ls_log, base_header, block_header, tx_header))) {
            TRANS_LOG(WARN, "[TxLsLogWriter] serialize ls_log error after grow", KR(ret), KP(log_buf_),
                      K(pos_), K(log_buf_len_));
          }
        } else {
          TRANS_LOG(WARN, "[TxLsLogWriter] serialize ls_log error", KR(ret), KP(log_buf_), K(pos_));
        }
      }
    }
  }
  return ret;
}

template <typename T>
int ObTxLSLogCb::serialize_payload_(T &ls_log,
                                    logservice::ObLogBaseHeader &base_header,
                                    ObTxLogBlockHeader &block_header,
                                    ObTxLogHeader &tx_header)
{
  int ret = OB_SUCCESS;
  pos_ = 0;
  if (OB_FAIL(base_header.serialize(log_buf_, log_buf_len_, pos_))) {
  } else if (OB_FAIL(block_header.serialize(log_buf_, log_buf_len_, pos_))) {
  } else if (OB_FAIL(tx_header.serialize(log_buf_, log_buf_len_, pos_))) {
  } else if (OB_FAIL(ls_log.serialize(log_buf_, log_buf_len_, pos_))) {
  }
  return ret;
}

class ObTxLSLogWriter
{
public:
  const static uint8_t DEFAULT_LOG_CB_CNT = 1;
  const static uint8_t APPEND_LOG_CB_CNT = 1;

  typedef common::ObSEArray<ObTxLSLogCb *, DEFAULT_LOG_CB_CNT, TransModulePageAllocator>
      TxLsLogCbArray;

public:
  ObTxLSLogWriter();
  ~ObTxLSLogWriter();
  int init(const int64_t tenant_id,
           const share::ObLSID &ls_id,
           ObITxLogAdapter *adapter,
           ObLSTxCtxMgr *ctx_mgr);
  int stop();
  //TODO wait until log_cb finished
  int wait();
  void reset();
  void destroy();

public:
  int submit_start_working_log(const int64_t &leader_epoch, share::SCN &log_ts);

  int64_t get_tenant_id() const { return tenant_id_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
public:
  int on_success(ObTxLSLogCb *cb);
  int on_failure(ObTxLSLogCb *cb);

private:
  template <typename T>
  int submit_ls_log_(T &ls_log,
                     const enum logservice::ObReplayBarrierType barrier_type,
                     bool need_nonblock,
                     share::SCN &log_ts);
  int get_log_cb_(const ObTxLogType &log_type, ObTxLSLogCb *&cb);
  int return_log_cb_(ObTxLSLogCb *cb);
  int append_free_log_cb_();

  bool reach_parallel_cbs_limit_(const ObTxLogType &log_type, const int32_t &cbs_cnt);

  common::ObDList<ObTxLSLogCb> *get_target_cbs_(const ObTxLogType &log_type);
  int64_t generate_replay_hint_(const ObTxLogType &type);

  void destroy_cbs_(common::ObDList<ObTxLSLogCb> &cbs);

private:
  bool is_stopped_;

  mutable common::ObSpinLock cbs_lock_;
  common::ObDList<ObTxLSLogCb> free_cbs_;
  common::ObDList<ObTxLSLogCb> keep_alive_cbs_;
  common::ObDList<ObTxLSLogCb> start_working_cbs_;

  int64_t tenant_id_;
  share::ObLSID ls_id_;
  ObLSTxCtxMgr *ctx_mgr_;
  ObITxLogAdapter *tx_log_adapter_;
};

template <typename T>
int ObTxLSLogWriter::submit_ls_log_(T &ls_log,
                                    const enum logservice::ObReplayBarrierType barrier_type,
                                    bool need_nonblock,
                                    share::SCN &log_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSpinLockGuard cb_guard(cbs_lock_);
  ObTxLSLogCb *cb = nullptr;
  int64_t replay_hint = generate_replay_hint_(T::LOG_TYPE);
  const bool skip_pre_async_wait = (ObTxLogType::TX_START_WORKING_LOG == T::LOG_TYPE);
  if (OB_FAIL(get_log_cb_(T::LOG_TYPE, cb))) {
    if (OB_TX_NOLOGCB == ret) {
      ret = OB_EAGAIN;
    } else {
      TRANS_LOG(WARN, "[TxLsLogWriter] can not get log cb", KR(ret), K(T::LOG_TYPE), KP(cb));
    }
  } else if (OB_ISNULL(cb)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "[TxLsLogWriter] get null log cb with success ret", KR(ret), K(T::LOG_TYPE));
  } else if (OB_FAIL(cb->serialize_ls_log(ls_log, replay_hint, barrier_type))) {
    TRANS_LOG(WARN, "[TxLsLogWriter] serialize ls log error", KR(ret), K(T::LOG_TYPE), KP(cb));
    if (OB_TMP_FAIL(return_log_cb_(cb))) {
      TRANS_LOG(ERROR, "[TxLsLogWriter] return log cb failed after serialize fail",
                KR(tmp_ret), K(T::LOG_TYPE), KP(cb));
    }
    cb = nullptr;
  } else if (OB_FAIL(tx_log_adapter_->submit_log(cb->get_log_buf(),
                                                 cb->get_log_pos(),
                                                 share::SCN::min_scn(),
                                                 cb,
                                                 need_nonblock,
                                                 ObITxLogAdapter::DEFAULT_RETRY_TIMEOUT_US,
                                                 skip_pre_async_wait))) {
    TRANS_LOG(WARN, "[TxLsLogWriter] submit ls log failed", KR(ret), K(T::LOG_TYPE), K(ls_id_), KPC(cb));
    if (OB_TMP_FAIL(return_log_cb_(cb))) {
      TRANS_LOG(ERROR, "[TxLsLogWriter] return log cb failed after submit fail",
                KR(tmp_ret), K(T::LOG_TYPE), KP(cb));
    }
    cb = nullptr;
  } else {
    log_ts = cb->get_log_ts();
    TRANS_LOG(INFO, "[TxLsLogWriter] submit ls log success", K(ret), K(T::LOG_TYPE), K(ls_id_), KPC(cb));
  }
  return ret;
}

} // namespace transaction
} // namespace oceanbase
#endif
