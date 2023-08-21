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

#include "ob_xa_define.h"
#include "ob_xa_ctx.h"
#include "ob_xa_ctx_mgr.h"

namespace oceanbase
{

using namespace common;

namespace transaction
{

const int64_t LOCK_FORMAT_ID = -2;
const int64_t XA_INNER_TABLE_TIMEOUT = 10 * 1000 * 1000;

const bool ENABLE_NEW_XA = true;

OB_SERIALIZE_MEMBER(ObXATransID, gtrid_str_, bqual_str_, format_id_);

ObXATransID::ObXATransID(const ObXATransID &xid)
{
  format_id_ = xid.format_id_;
  gtrid_str_.reset();
  bqual_str_.reset();
  gtrid_str_.assign_buffer(gtrid_buf_, sizeof(gtrid_buf_));
  bqual_str_.assign_buffer(bqual_buf_, sizeof(bqual_buf_));
  gtrid_str_.write(xid.gtrid_str_.ptr(), xid.gtrid_str_.length());
  bqual_str_.write(xid.bqual_str_.ptr(), xid.bqual_str_.length());
  g_hv_ = xid.g_hv_;
  b_hv_ = xid.b_hv_;
}

void ObXATransID::reset()
{
  gtrid_str_.reset();
  (void)gtrid_str_.assign_buffer(gtrid_buf_, sizeof(gtrid_buf_));
  bqual_str_.reset();
  (void)bqual_str_.assign_buffer(bqual_buf_, sizeof(bqual_buf_));
  format_id_ = 1;
  g_hv_ = 0;
  b_hv_ = 0;
}

int ObXATransID::set(const ObString &gtrid_str,
                       const ObString &bqual_str,
                       const int64_t format_id)
{
  int ret = OB_SUCCESS;
  if (0 > gtrid_str.length() || 0 > bqual_str.length()
      || MAX_GTRID_LENGTH < gtrid_str.length()
      || MAX_BQUAL_LENGTH < bqual_str.length()) {
    TRANS_LOG(WARN, "invalid arguments", K(gtrid_str), K(bqual_str), K(format_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    format_id_ = format_id;
    gtrid_str_.reset();
    bqual_str_.reset();
    gtrid_str_.assign_buffer(gtrid_buf_, sizeof(gtrid_buf_));
    bqual_str_.assign_buffer(bqual_buf_, sizeof(bqual_buf_));
    gtrid_str_.write(gtrid_str.ptr(), gtrid_str.length());
    bqual_str_.write(bqual_str.ptr(), bqual_str.length());
    g_hv_ = murmurhash(gtrid_str.ptr(), gtrid_str.length(), 0) % HASH_SIZE;
    b_hv_ = murmurhash(bqual_str.ptr(), bqual_str.length(), 0) % HASH_SIZE;
  }
  return ret;
}

int ObXATransID::set(const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  if (!xid.is_valid()) {
    TRANS_LOG(WARN, "invalid xid", K(xid));
    ret = OB_INVALID_ARGUMENT;
  } else {
    format_id_ = xid.format_id_;
    gtrid_str_.reset();
    bqual_str_.reset();
    gtrid_str_.assign_buffer(gtrid_buf_, sizeof(gtrid_buf_));
    bqual_str_.assign_buffer(bqual_buf_, sizeof(bqual_buf_));
    gtrid_str_.write(xid.gtrid_str_.ptr(), xid.gtrid_str_.length());
    bqual_str_.write(xid.bqual_str_.ptr(), xid.bqual_str_.length());
    g_hv_ = xid.g_hv_;
    b_hv_ = xid.b_hv_;
  }
  return ret;
}

bool ObXATransID::empty() const
{
  return gtrid_str_.empty();
}

bool ObXATransID::is_valid() const
{
  return 0 <= gtrid_str_.length() && 0 <= bqual_str_.length()
         && MAX_GTRID_LENGTH >= gtrid_str_.length()
         && MAX_BQUAL_LENGTH >= bqual_str_.length();
}

ObXATransID &ObXATransID::operator=(const ObXATransID &xid)
{
  if (this != &xid) {
    format_id_ = xid.format_id_;
    gtrid_str_.reset();
    bqual_str_.reset();
    gtrid_str_.assign_buffer(gtrid_buf_, sizeof(gtrid_buf_));
    bqual_str_.assign_buffer(bqual_buf_, sizeof(bqual_buf_));
    gtrid_str_.write(xid.gtrid_str_.ptr(), xid.gtrid_str_.length());
    bqual_str_.write(xid.bqual_str_.ptr(), xid.bqual_str_.length());
  }
  return *this;
}

bool ObXATransID::operator==(const ObXATransID &xid) const
{
  return gtrid_str_ == xid.gtrid_str_
         && bqual_str_ == xid.bqual_str_
         && format_id_ == xid.format_id_;
}

bool ObXATransID::operator!=(const ObXATransID &xid) const
{
  return gtrid_str_ != xid.gtrid_str_;
         //|| bqual_str_ != xid.bqual_str_
         //|| format_id_ != xid.format_id_;
}

int32_t ObXATransID::to_full_xid_string(char *buffer,
                                        const int64_t capacity) const
{
  int64_t count = 0;
  if (NULL == buffer || 0 >= capacity) {
    TRANS_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid augumnets", KP(buffer), K(capacity));
  } else if (gtrid_str_.empty()) {
    TRANS_LOG_RET(WARN, OB_INVALID_ARGUMENT, "xid is empty");
  } else if ((count = snprintf(buffer, capacity, "%.*s%.*s",
                               gtrid_str_.length(), gtrid_str_.ptr(),
                               bqual_str_.length(), bqual_str_.ptr())) <= 0
             || count >= capacity ) {
    TRANS_LOG_RET(WARN, OB_BUF_NOT_ENOUGH, "buffer is not enough", K(count), K(capacity), K(gtrid_str_.length()),
        K(bqual_str_.length()));
  } else {
    TRANS_LOG(DEBUG, "generate buffer success", K(count), K(capacity), K(gtrid_str_.length()),
        K(bqual_str_.length()));
  }
  return count;
}

int ObXATransID::to_yson(char *buf, const int64_t buf_len, int64_t &pos) const
{
  return oceanbase::yson::databuff_encode_elements(buf, buf_len, pos,
                                                   OB_ID(gtrid), g_hv_,
                                                   OB_ID(bqual), b_hv_);
}

bool ObXATransID::all_equal_to(const ObXATransID &other) const
{
  return gtrid_str_ == other.gtrid_str_ &&
         bqual_str_ == other.bqual_str_ &&
         format_id_ == other.format_id_;
}

bool ObXATransID::gtrid_equal_to(const ObXATransID &other) const
{
  return gtrid_str_ == other.gtrid_str_;
}

bool ObXATransState::can_convert(const int32_t src_state, const int32_t dst_state)
{
  bool ret_bool = true;
  if (src_state != dst_state) {
    switch (dst_state) {
      case NON_EXISTING:
        break;
      case ACTIVE: {
        if (NON_EXISTING != src_state && IDLE != src_state) {
          ret_bool = false;
        }
        break;
      }
      case IDLE: {
        if (ACTIVE != src_state) {
          ret_bool = false;
        }
        break;
      }
      case PREPARED: {
        if (IDLE != src_state) {
          ret_bool = false;
        }
        break;
      }
      case COMMITTED: {
        if (IDLE != src_state && PREPARED != src_state) {
          ret_bool = false;
        }
        break;
      }
      case ROLLBACKED: {
        if (IDLE != src_state && PREPARED != src_state) {
          ret_bool = false;
        }
        break;
      }
      default: {
        ret_bool = false;
      }
    }
  }
  return ret_bool;
}

bool ObXAFlag::is_valid(const int64_t flag, const int64_t xa_req_type)
{
  bool ret_bool = true;

  switch (xa_req_type) {
    case ObXAReqType::XA_START: {
      if (((flag & OBTMRESUME) && ((flag & OBTMJOIN) || (flag & OBLOOSELY)))
         || ((flag & OBLOOSELY) && (flag & OBTMJOIN))) {
        ret_bool = false;
      } else {
        const bool is_resumejoin = flag & (OBTMRESUME | OBTMJOIN);
        if (!is_resumejoin) {
          const int64_t mask = OBLOOSELY | OBTMREADONLY | OBTMSERIALIZABLE;
          if (mask != (flag | mask)) {
            ret_bool = false;
          } else {
            ret_bool = true;
          }
        } else {
          if ((flag & OBTMJOIN) && (flag & OBTMRESUME)) {
            ret_bool = false;
          } else {
            ret_bool = true;
          }
        }
      }
      break;
    }
    case ObXAReqType::XA_END: {
      const int64_t mask = 0x00000000FFFFFFFF;
      if ((flag & mask) != OBTMSUSPEND && (flag & mask) != OBTMSUCCESS && (flag & mask) != OBTMFAIL) {
        ret_bool = false;
      } else {
        ret_bool = true;
      }
      break;
    }
    case ObXAReqType::XA_PREPARE: {
      // oracle would not carry flag in a xa prepare req
      ret_bool = true;
      TRANS_LOG(INFO, "no need to check flag for xa prepare", K(xa_req_type), K(flag));
      break;
    }
    case ObXAReqType::XA_COMMIT: {
      // noflags or onephase
      if (flag != OBTMNOFLAGS && flag != OBTMONEPHASE) {
        ret_bool = false;
      } else {
        ret_bool = true;
      }
      break;
    }
    case ObXAReqType::XA_ROLLBACK: {
      // oracle would not carry flag in a xa rollback req
      ret_bool = true;
      TRANS_LOG(INFO, "no need to check flag for xa rollback", K(xa_req_type), K(flag));
      break;
    }
    default: {
      ret_bool = false;
      TRANS_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid xa request type", K(xa_req_type), K(flag));
    }
  }
  TRANS_LOG(INFO, "check xa flag", K(ret_bool), K(xa_req_type), KPHEX(&flag, sizeof(int64_t)));

  return ret_bool;
}

bool ObXAFlag::is_valid_inner_flag(const int64_t flag)
{
  bool ret_bool = true;
  if ((flag & OBTMSUSPEND) && (flag & OBTMSUCCESS)) {
    ret_bool = false;
  } else if (!(flag & OBTMSUSPEND) && !(flag & OBTMSUCCESS)) {
    ret_bool = false;
  } else {
    ret_bool = true;
  }
  TRANS_LOG(INFO, "check xa inner flag", K(ret_bool), KPHEX(&flag, sizeof(int64_t)));
  return ret_bool;
}

bool ObXAFlag::is_tmnoflags(const int64_t flag, const int64_t xa_req_type)
{
  bool ret_bool = true;
  if (ObXAReqType::XA_START == xa_req_type) {
    const int64_t mask = OBLOOSELY | OBTMREADONLY | OBTMSERIALIZABLE;
    ret_bool = ((mask | flag) == mask);
  } else {
    ret_bool = (OBTMNOFLAGS == flag);
  }
  TRANS_LOG(INFO, "check tmnoflags", K(ret_bool), K(xa_req_type), KPHEX(&flag, sizeof(int64_t)));
  return ret_bool;
}

int ObXABranchInfo::init(const ObXATransID &xid,
                         const int64_t state,
                         const int64_t timeout_seconds,
                         const int64_t abs_expired_time,
                         const common::ObAddr &addr,
                         const int64_t unrespond_msg_cnt,
                         const int64_t last_hb_ts,
                         const int64_t end_flag)
{
  int ret = OB_SUCCESS;

  // NOTE that branch adding happens during xa start
  if (!xid.is_valid() || !ObXATransState::is_valid(state) ||
      timeout_seconds < 0 || !addr.is_valid() ||
      unrespond_msg_cnt < 0 || last_hb_ts < 0 ||
      !ObXAFlag::is_valid(end_flag, ObXAReqType::XA_START)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(xid), K(state), K(timeout_seconds),
              K(addr), K(unrespond_msg_cnt), K(last_hb_ts), K(end_flag));
  } else {
    xid_ = xid;
    state_ = state;
    timeout_seconds_ = timeout_seconds;
    abs_expired_time_ = abs_expired_time;
    addr_ = addr;
    unrespond_msg_cnt_ = unrespond_msg_cnt;
    last_hb_ts_ = last_hb_ts;
    end_flag_ = end_flag;
  }

  return ret;
}

int ObXATimeoutTask::init(ObXACtx *ctx)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    TRANS_LOG(WARN, "ObTransTimeoutTask inited twice");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(ctx)) {
    TRANS_LOG(WARN, "invalid argument", KP(ctx));
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_inited_ = true;
    ctx_ = ctx;
  }

  return ret;
}

void ObXATimeoutTask::destroy()
{
  ObITransTimer *timer = NULL;
  if (is_inited_) {
    is_inited_ = false;
    if (NULL != ctx_ && NULL != (timer = ctx_->get_timer())) {
      (void)timer->unregister_timeout_task(*this);
    }
  }
}

void ObXATimeoutTask::reset()
{
  ObTimeWheelTask::reset();
  ObITimeoutTask::reset();
  is_inited_ = false;
  ctx_ = NULL;
}

uint64_t ObXATimeoutTask::hash() const
{
  uint64_t hv = 0;
  if (NULL == ctx_) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "ctx is null, unexpected error", KP_(ctx));
  } else {
    const ObTransID &trans_id = ctx_->get_trans_id();
    if (!trans_id.is_valid()) {
      TRANS_LOG_RET(WARN, OB_INVALID_ARGUMENT, "ObTransID is invalid", K(trans_id));
    } else {
      hv = trans_id.hash();
    }
  }
  return hv;
}

void ObXATimeoutTask::runTimerTask()
{
  int tmp_ret = OB_SUCCESS;
  ObXACtxMgr *xa_ctx_mgr = NULL;

  if (!is_inited_) {
    TRANS_LOG_RET(WARN, OB_NOT_INIT, "ObTransTimeoutTask not inited");
  } else if (NULL == ctx_) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "ctx is null, unexpected error", KP_(ctx));
    tmp_ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_SUCCESS != (tmp_ret = ctx_->handle_timeout(delay_))) {
      TRANS_LOG_RET(WARN, tmp_ret, "handle timeout error", "ret", tmp_ret, "context", *ctx_);
    }
    //dec the ctx ref
    if (NULL == (xa_ctx_mgr = ctx_->get_xa_ctx_mgr())) {
      TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "get partition mgr error", "context", *ctx_);
    } else {
      (void)xa_ctx_mgr->revert_xa_ctx(ctx_);
    }
  }
}

void ObXAStatistics::reset()
{
  ATOMIC_STORE(&total_standby_clearup_count_, 0);
  ATOMIC_STORE(&total_success_xa_start_, 0);
  ATOMIC_STORE(&total_failure_xa_start_, 0);
  ATOMIC_STORE(&total_success_xa_prepare_, 0);
  ATOMIC_STORE(&total_failure_xa_prepare_, 0);
  ATOMIC_STORE(&total_success_xa_1pc_commit_, 0);
  ATOMIC_STORE(&total_failure_xa_1pc_commit_, 0);
  ATOMIC_STORE(&total_success_xa_2pc_commit_, 0);
  ATOMIC_STORE(&total_failure_xa_2pc_commit_, 0);
  ATOMIC_STORE(&total_xa_rollback_, 0);
  ATOMIC_STORE(&total_success_dblink_promotion_, 0);
  ATOMIC_STORE(&total_failure_dblink_promotion_, 0);
  ATOMIC_STORE(&total_success_dblink_, 0);
  ATOMIC_STORE(&total_failure_dblink_, 0);
}

void ObXAStatistics::print_statistics(int64_t cur_ts)
{
  const int64_t last_stat_ts = ATOMIC_LOAD(&last_stat_ts_);
  if (cur_ts - last_stat_ts >= STAT_INTERVAL) {
    if (ATOMIC_BCAS(&last_stat_ts_, last_stat_ts, cur_ts)) {
      TRANS_LOG(INFO, "xa statistics",
                      "total_active_xa_ctx_count", ATOMIC_LOAD(&total_active_xa_ctx_count_),
                      "total_standby_clearup_count", ATOMIC_LOAD(&total_standby_clearup_count_),
                      "total_success_xa_start", ATOMIC_LOAD(&total_success_xa_start_),
                      "total_failure_xa_start", ATOMIC_LOAD(&total_failure_xa_start_),
                      "total_success_xa_prepare", ATOMIC_LOAD(&total_success_xa_prepare_),
                      "total_failure_xa_prepare", ATOMIC_LOAD(&total_failure_xa_prepare_),
                      "total_success_xa_1pc_commit", ATOMIC_LOAD(&total_success_xa_1pc_commit_),
                      "total_failure_xa_1pc_commit", ATOMIC_LOAD(&total_failure_xa_1pc_commit_),
                      "total_success_xa_2pc_commit", ATOMIC_LOAD(&total_success_xa_2pc_commit_),
                      "total_failure_xa_2pc_commit", ATOMIC_LOAD(&total_failure_xa_2pc_commit_),
                      "total_failure_xa_rollback", ATOMIC_LOAD(&total_xa_rollback_),
                      "total_success_dblink_promotion", ATOMIC_LOAD(&total_success_dblink_promotion_),
                      "total_failure_dblink_promotion", ATOMIC_LOAD(&total_failure_dblink_promotion_),
                      "total_success_dblink", ATOMIC_LOAD(&total_success_dblink_),
                      "total_failure_dblink", ATOMIC_LOAD(&total_failure_dblink_));
      reset();
    }
  }
}

}//transaction

}//oceanbase
