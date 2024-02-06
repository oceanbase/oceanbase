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

public:
  static int64_t LOG_BUF_SIZE;
  static void decide_log_buf_size();

private:
  ObTxLSLogLimit();
  DISALLOW_COPY_AND_ASSIGN(ObTxLSLogLimit);
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

public:
  INHERIT_TO_STRING_KV("ObTxBaseLogCb", ObTxBaseLogCb, K(type_), KP(log_buf_), K(pos_), KP(base_wr_));

private:
  bool need_alloc_buf_() { return nullptr == log_buf_; }
  OB_NOINLINE int alloc_log_buf_();

private:
  ObTxLogType type_; // Unkown == unused, not init
  char *log_buf_;
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
  } else if (need_alloc_buf_() && OB_FAIL(alloc_log_buf_())) {
    TRANS_LOG(WARN, "alloc log buf failed", KR(ret));
  } else {
    logservice::ObLogBaseHeader base_header(logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE,
                                            barrier_type, replay_hint);
    ObTxLogBlockHeader block_header;
    ObTxLogHeader tx_header(T::LOG_TYPE);
    // if (OB_FAIL(block_header.before_serialize())) {
    //   TRANS_LOG(WARN, "[TxLsLogWriter] before serialize block header error", KR(ret),
    //             K(block_header));
    // } else if (OB_FAIL(ls_log.before_serialize())) {
    //   TRANS_LOG(WARN, "[TxLsLogWriter] before serialize block header error", KR(ret), K(ls_log));
    // } else
    if (OB_FAIL(base_header.serialize(log_buf_, ObTxLSLogLimit::LOG_BUF_SIZE, pos_))) {
      TRANS_LOG(WARN, "[TxLsLogWriter] serialize base header error", KR(ret), KP(log_buf_),
                K(pos_));
    } else if (OB_FAIL(block_header.serialize(log_buf_, ObTxLSLogLimit::LOG_BUF_SIZE, pos_))) {
      TRANS_LOG(WARN, "[TxLsLogWriter] serialize block header error", KR(ret), KP(log_buf_),
                K(pos_));
    } else if (OB_FAIL(tx_header.serialize(log_buf_, ObTxLSLogLimit::LOG_BUF_SIZE, pos_))) {
      TRANS_LOG(WARN, "[TxLsLogWriter] serialize tx header error", KR(ret), KP(log_buf_), K(pos_));
    } else if (OB_FAIL(ls_log.serialize(log_buf_, ObTxLSLogLimit::LOG_BUF_SIZE, pos_))) {
      TRANS_LOG(WARN, "[TxLsLogWriter] serialize ls_log error", KR(ret), KP(log_buf_), K(pos_));
    }
  }
  return ret;
}

class ObTxLSLogWriter
{
public:
  const static uint8_t DEFAULT_LOG_CB_CNT = 5;
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
public:
  int on_success(ObTxLSLogCb *cb);
  int on_failure(ObTxLSLogCb *cb);

  // TODO RoleChangeSubHandler
  void switch_to_follower_forcedly();
  int switch_to_leader();
  int switch_to_follower_gracefully();
  int resume_leader();

private:
  template <typename T>
  int submit_ls_log_(T &ls_log,
                     const enum logservice::ObReplayBarrierType barrier_type,
                     bool need_nonblock,
                     share::SCN &log_ts);
  int get_log_cb_(const ObTxLogType &log_type, ObTxLSLogCb *&cb);
  int return_log_cb_(ObTxLSLogCb *cb);
  bool enough_log_cb_();
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
  ObSpinLockGuard cb_guard(cbs_lock_);
  ObTxLSLogCb *cb = nullptr;
  int64_t replay_hint = generate_replay_hint_(T::LOG_TYPE);
  if (OB_FAIL(get_log_cb_(T::LOG_TYPE, cb))) {
    if (OB_TX_NOLOGCB == ret) {
      ret = OB_EAGAIN;
    } else {
      TRANS_LOG(WARN, "[TxLsLogWriter] can not get log cb", KR(ret), K(T::LOG_TYPE), KP(cb));
    }
  } else if (nullptr == cb || OB_FAIL(cb->serialize_ls_log(ls_log, replay_hint, barrier_type))) {
    TRANS_LOG(WARN, "[TxLsLogWriter] serialize ls log error", KR(ret), K(T::LOG_TYPE), KP(cb));
  } else if (OB_FAIL(tx_log_adapter_->submit_log(cb->get_log_buf(), cb->get_log_pos(), share::SCN::min_scn(), cb, need_nonblock))) {
    return_log_cb_(cb);
    cb = nullptr;
    TRANS_LOG(WARN, "[TxLsLogWriter] submit ls log failed", KR(ret), K(T::LOG_TYPE), K(ls_id_));
  } else {
    log_ts = cb->get_log_ts();
    TRANS_LOG(INFO, "[TxLsLogWriter] submit ls log success", K(ret), K(T::LOG_TYPE), K(ls_id_));
  }
  return ret;
}

} // namespace transaction
} // namespace oceanbase
#endif
