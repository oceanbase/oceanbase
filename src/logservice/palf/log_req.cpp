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

#include "log_req.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/serialization.h"
#include "log_writer_utils.h"
#include "logservice/palf/lsn.h"
#include "log_meta_info.h"

namespace oceanbase
{
using namespace common;
namespace palf
{
// ==================== LogPushReq start ========================
LogPushReq::LogPushReq()
    : push_log_type_(PUSH_LOG),
      msg_proposal_id_(INVALID_PROPOSAL_ID),
      prev_log_proposal_id_(INVALID_PROPOSAL_ID),
      prev_lsn_(),
      curr_lsn_(),
      write_buf_()
{
}

LogPushReq::LogPushReq(const PushLogType push_log_type,
                       const int64_t &msg_proposal_id,
                       const int64_t &prev_log_proposal_id,
                       const LSN &prev_lsn,
                       const LSN &curr_lsn,
                       const LogWriteBuf &write_buf)
    : push_log_type_(push_log_type),
      msg_proposal_id_(msg_proposal_id),
      prev_log_proposal_id_(prev_log_proposal_id),
      prev_lsn_(prev_lsn),
      curr_lsn_(curr_lsn),
      write_buf_(write_buf)
{
}

LogPushReq::~LogPushReq()
{
  reset();
}

bool LogPushReq::is_valid() const
{
  // NB: prev_lsn may be invalid
  return INVALID_PROPOSAL_ID != msg_proposal_id_
         && true == curr_lsn_.is_valid()
         && true == write_buf_.is_valid();
}

void LogPushReq::reset()
{
  push_log_type_ = PUSH_LOG;
  msg_proposal_id_ = INVALID_PROPOSAL_ID;
  prev_log_proposal_id_ = INVALID_PROPOSAL_ID;
  prev_lsn_.reset();
  curr_lsn_.reset();
  write_buf_.reset();
}

OB_DEF_SERIALIZE(LogPushReq)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, push_log_type_))
             || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, msg_proposal_id_))
             || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, prev_log_proposal_id_))
             || OB_FAIL(prev_lsn_.serialize(buf, buf_len, new_pos))
             || OB_FAIL(curr_lsn_.serialize(buf, buf_len, new_pos))
             || OB_FAIL(write_buf_.serialize(buf, buf_len, new_pos))) {
    PALF_LOG(ERROR, "LogPushReq serialize failed", K(ret), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_DESERIALIZE(LogPushReq)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || pos < 0 || pos > data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &push_log_type_))
             || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, reinterpret_cast<int64_t*>(&msg_proposal_id_)))
             || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, reinterpret_cast<int64_t*>(&prev_log_proposal_id_)))
             || OB_FAIL(prev_lsn_.deserialize(buf, data_len, new_pos))
             || OB_FAIL(curr_lsn_.deserialize(buf, data_len, new_pos))
             || OB_FAIL(write_buf_.deserialize(buf, data_len, new_pos))) {
    PALF_LOG(ERROR, "LogPushReq serialize failed", K(ret), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(LogPushReq)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(push_log_type_);
  size += serialization::encoded_length_i64(msg_proposal_id_);
  size += serialization::encoded_length_i64(prev_log_proposal_id_);
  size += prev_lsn_.get_serialize_size();
  size += curr_lsn_.get_serialize_size();
  size += write_buf_.get_serialize_size();
  return size;
}
// ================== LogPushReq end =========================

// ================== LogPushResp start ======================
LogPushResp::LogPushResp()
    : msg_proposal_id_(INVALID_PROPOSAL_ID),
      lsn_()
{
}

LogPushResp::LogPushResp(const int64_t &msg_proposal_id,
                             const LSN &lsn)
    : msg_proposal_id_(msg_proposal_id),
      lsn_(lsn)
{
}

LogPushResp::~LogPushResp()
{
  reset();
}

bool LogPushResp::is_valid() const
{
  return INVALID_PROPOSAL_ID != msg_proposal_id_
         && true == lsn_.is_valid();
}

void LogPushResp::reset()
{
  msg_proposal_id_ = INVALID_PROPOSAL_ID;
  lsn_.reset();
}

OB_SERIALIZE_MEMBER(LogPushResp, msg_proposal_id_, lsn_);
// ================= LogPushResp end =======================

// ================= LogFetchReq start =====================
LogFetchReq::LogFetchReq()
    : fetch_type_(FETCH_LOG_FOLLOWER),
      msg_proposal_id_(INVALID_PROPOSAL_ID),
      prev_lsn_(),
      lsn_(),
      fetch_log_size_(0),
      fetch_log_count_(0)
{
}

LogFetchReq::LogFetchReq(const FetchLogType fetch_type,
                         const int64_t msg_proposal_id,
                         const LSN &prev_lsn,
                         const LSN &lsn,
                         const int64_t fetch_log_size,
                         const int64_t fetch_log_count,
                         const int64_t accepted_mode_pid)
    : fetch_type_(fetch_type),
      msg_proposal_id_(msg_proposal_id),
      prev_lsn_(prev_lsn),
      lsn_(lsn),
      fetch_log_size_(fetch_log_size),
      fetch_log_count_(fetch_log_count),
      accepted_mode_pid_(accepted_mode_pid)
{
}

LogFetchReq::~LogFetchReq()
{
  reset();
}

bool LogFetchReq::is_valid() const
{
  return INVALID_PROPOSAL_ID != msg_proposal_id_
          && true == lsn_.is_valid();
}

void LogFetchReq::reset()
{
  fetch_type_ = FETCH_LOG_FOLLOWER;
  msg_proposal_id_ = INVALID_PROPOSAL_ID;
  prev_lsn_.reset();
  lsn_.reset();
  fetch_log_size_ = 0;
  fetch_log_count_ = 0;
  accepted_mode_pid_ = INVALID_PROPOSAL_ID;
}

OB_SERIALIZE_MEMBER(LogFetchReq, fetch_type_, msg_proposal_id_, prev_lsn_, lsn_, fetch_log_size_,
    fetch_log_count_, accepted_mode_pid_);
// ================= LogFetchReq end ======================

NotifyRebuildReq::NotifyRebuildReq()
    : base_lsn_(), base_prev_log_info_()
{
}

NotifyRebuildReq::NotifyRebuildReq(const LSN &base_lsn,
    const LogInfo &base_prev_log_info)
    : base_lsn_(base_lsn), base_prev_log_info_(base_prev_log_info)
{
}

NotifyRebuildReq::~NotifyRebuildReq()
{
  reset();
}

bool NotifyRebuildReq::is_valid() const
{
    return true == base_lsn_.is_valid() && true == base_prev_log_info_.is_valid();
}

void NotifyRebuildReq::reset()
{
  base_lsn_.reset();
  base_prev_log_info_.reset();
}

OB_SERIALIZE_MEMBER(NotifyRebuildReq, base_lsn_, base_prev_log_info_);

NotifyFetchLogReq::NotifyFetchLogReq()
{}

NotifyFetchLogReq::~NotifyFetchLogReq()
{}

bool NotifyFetchLogReq::is_valid() const
{
  return true;
}

OB_SERIALIZE_MEMBER(NotifyFetchLogReq);

// ================= LogPrepareReq start =================
LogPrepareReq::LogPrepareReq()
    : log_proposal_id_(INVALID_PROPOSAL_ID)
{
}

LogPrepareReq::LogPrepareReq(const int64_t &log_proposal_id)
    : log_proposal_id_(log_proposal_id)
{
}

LogPrepareReq::LogPrepareReq(const LogPrepareReq &req)
{
  log_proposal_id_ = req.log_proposal_id_;
}


LogPrepareReq::~LogPrepareReq()
{
  reset();
}

bool LogPrepareReq::is_valid() const
{
  return INVALID_PROPOSAL_ID != log_proposal_id_;
}

void LogPrepareReq::reset()
{
  log_proposal_id_ = INVALID_PROPOSAL_ID;
}

OB_SERIALIZE_MEMBER(LogPrepareReq, log_proposal_id_);
// ================= LogPrepareReq end ===================

// ================= LogPrepareResp start ================
LogPrepareResp::LogPrepareResp()
  : msg_proposal_id_(INVALID_PROPOSAL_ID),
    vote_granted_(false),
    log_proposal_id_(INVALID_PROPOSAL_ID),
    max_flushed_lsn_(),
    log_mode_meta_(),
    committed_end_lsn_()
{
}

LogPrepareResp::LogPrepareResp(
    const int64_t &msg_proposal_id,
    const bool vote_granted,
    const int64_t &log_proposal_id,
    const LSN &max_flushed_lsn,
    const LogModeMeta &mode_meta,
    const LSN &committed_end_lsn)
  : msg_proposal_id_(msg_proposal_id),
    vote_granted_(vote_granted),
    log_proposal_id_(log_proposal_id),
    max_flushed_lsn_(max_flushed_lsn),
    log_mode_meta_(mode_meta),
    committed_end_lsn_(committed_end_lsn)
{
}

LogPrepareResp::~LogPrepareResp()
{
  reset();
}

bool LogPrepareResp::is_valid() const
{
  return INVALID_PROPOSAL_ID != msg_proposal_id_
         && INVALID_PROPOSAL_ID != log_proposal_id_
         && true == max_flushed_lsn_.is_valid()
         && true == log_mode_meta_.is_valid();
}

void LogPrepareResp::reset()
{
  msg_proposal_id_ = INVALID_PROPOSAL_ID;
  vote_granted_ = false;
  log_proposal_id_ = INVALID_PROPOSAL_ID;
  max_flushed_lsn_.reset();
  log_mode_meta_.reset();
  committed_end_lsn_.reset();
}

OB_SERIALIZE_MEMBER(LogPrepareResp, msg_proposal_id_, vote_granted_, log_proposal_id_, \
    max_flushed_lsn_, log_mode_meta_, committed_end_lsn_);

LogChangeConfigMetaReq::LogChangeConfigMetaReq()
  : msg_proposal_id_(INVALID_PROPOSAL_ID),
    prev_log_proposal_id_(INVALID_PROPOSAL_ID),
    prev_lsn_(),
    prev_mode_pid_(INVALID_PROPOSAL_ID),
    meta_()
{
}

LogChangeConfigMetaReq::LogChangeConfigMetaReq(
    const int64_t &msg_proposal_id,
    const int64_t &prev_log_proposal_id,
    const LSN &prev_lsn,
    const int64_t &prev_mode_pid,
    const LogConfigMeta &meta)
  : msg_proposal_id_(msg_proposal_id),
    prev_log_proposal_id_(prev_log_proposal_id),
    prev_lsn_(prev_lsn),
    prev_mode_pid_(prev_mode_pid),
    meta_(meta)
{
}

LogChangeConfigMetaReq::~LogChangeConfigMetaReq()
{
  reset();
}

bool LogChangeConfigMetaReq::is_valid() const
{
  return INVALID_PROPOSAL_ID != msg_proposal_id_
//         && INVALID_PROPOSAL_ID != prev_log_proposal_id_  // 对于日志流无写入的场景，prev_log是无效的
//         && prev_lsn_.is_valid()
         && meta_.is_valid();
}

void LogChangeConfigMetaReq::reset()
{
  msg_proposal_id_ = INVALID_PROPOSAL_ID;
  prev_log_proposal_id_ = INVALID_PROPOSAL_ID;
  prev_lsn_.reset();
  prev_mode_pid_ = INVALID_PROPOSAL_ID;
  meta_.reset();
}

OB_SERIALIZE_MEMBER(LogChangeConfigMetaReq, msg_proposal_id_, prev_log_proposal_id_, prev_lsn_,
    prev_mode_pid_, meta_);

LogChangeConfigMetaResp::LogChangeConfigMetaResp()
  : proposal_id_(INVALID_PROPOSAL_ID),
    config_version_()
{
}

LogChangeConfigMetaResp::LogChangeConfigMetaResp(const int64_t proposal_id,
                                     const LogConfigVersion &config_version)
  : proposal_id_(proposal_id),
    config_version_(config_version)
{
}

LogChangeConfigMetaResp::~LogChangeConfigMetaResp()
{
  reset();
}

bool LogChangeConfigMetaResp::is_valid() const
{
  return INVALID_PROPOSAL_ID != proposal_id_ && config_version_.is_valid();
}

void LogChangeConfigMetaResp::reset()
{
  proposal_id_ = INVALID_PROPOSAL_ID;
  config_version_.reset();
}

OB_SERIALIZE_MEMBER(LogChangeConfigMetaResp, proposal_id_, config_version_);

// ========== LogChangeModeMetaReq start=========
LogChangeModeMetaReq::LogChangeModeMetaReq()
  : msg_proposal_id_(INVALID_PROPOSAL_ID),
    meta_(),
    is_applied_mode_meta_(false)
{
}

LogChangeModeMetaReq::LogChangeModeMetaReq(
    const int64_t &msg_proposal_id,
    const LogModeMeta &meta,
    const bool is_applied_mode_meta)
  : msg_proposal_id_(msg_proposal_id),
    meta_(meta),
    is_applied_mode_meta_(is_applied_mode_meta)
{
}

LogChangeModeMetaReq::~LogChangeModeMetaReq()
{
  reset();
}

bool LogChangeModeMetaReq::is_valid() const
{
  return INVALID_PROPOSAL_ID != msg_proposal_id_
         && meta_.is_valid();
}

void LogChangeModeMetaReq::reset()
{
  msg_proposal_id_ = INVALID_PROPOSAL_ID;
  meta_.reset();
  is_applied_mode_meta_ = false;
}

OB_SERIALIZE_MEMBER(LogChangeModeMetaReq, msg_proposal_id_, meta_, is_applied_mode_meta_);
// ========== LogChangeModeMetaReq end=========

// ========== LogChangeModeMetaResp start=========
LogChangeModeMetaResp::LogChangeModeMetaResp() : msg_proposal_id_(INVALID_PROPOSAL_ID)
{
}

LogChangeModeMetaResp::LogChangeModeMetaResp(const int64_t msg_proposal_id)
  : msg_proposal_id_(msg_proposal_id)
{
}

LogChangeModeMetaResp::~LogChangeModeMetaResp()
{
  reset();
}

bool LogChangeModeMetaResp::is_valid() const
{
  return INVALID_PROPOSAL_ID != msg_proposal_id_;
}

void LogChangeModeMetaResp::reset()
{
  msg_proposal_id_ = INVALID_PROPOSAL_ID;
}

OB_SERIALIZE_MEMBER(LogChangeModeMetaResp, msg_proposal_id_);
// ========== LogChangeModeMetaResp end=========

// ================= LogGetMCStReq start ================
LogGetMCStReq::LogGetMCStReq()
  : config_version_(), need_purge_throttling_(false)
{
}

LogGetMCStReq::LogGetMCStReq(const LogConfigVersion &config_version,
                             const bool need_purge_throttling)
  : config_version_(config_version), need_purge_throttling_(need_purge_throttling)
{
}

LogGetMCStReq::~LogGetMCStReq()
{
  reset();
}

bool LogGetMCStReq::is_valid() const
{
  return config_version_.is_valid();
}

void LogGetMCStReq::reset()
{
  config_version_.reset();
  need_purge_throttling_ = false;
}

OB_SERIALIZE_MEMBER(LogGetMCStReq, config_version_, need_purge_throttling_);
// ================= LogGetMCStReq end ================

// ================= LogGetMCStResp start ================
LogGetMCStResp::LogGetMCStResp()
  : msg_proposal_id_(INVALID_PROPOSAL_ID),
    max_flushed_end_lsn_(),
    is_normal_replica_(false),
    need_update_config_meta_(false),
    last_slide_log_id_(INT64_MAX)
{
}

LogGetMCStResp::LogGetMCStResp(const int64_t &msg_proposal_id,
                               const LSN &max_flushed_end_lsn,
                               const bool is_normal_replica,
                               const bool need_update_config_meta,
                               const int64_t last_slide_log_id)
  : msg_proposal_id_(msg_proposal_id),
    max_flushed_end_lsn_(max_flushed_end_lsn),
    is_normal_replica_(is_normal_replica),
    need_update_config_meta_(need_update_config_meta),
    last_slide_log_id_(last_slide_log_id)
{
}

LogGetMCStResp::~LogGetMCStResp()
{
  reset();
}

bool LogGetMCStResp::is_valid() const
{
  return INVALID_PROPOSAL_ID != msg_proposal_id_ && max_flushed_end_lsn_.is_valid();
}

void LogGetMCStResp::reset()
{
  msg_proposal_id_ = INVALID_PROPOSAL_ID;
  max_flushed_end_lsn_.reset();
  is_normal_replica_ = false;
  need_update_config_meta_ = false;
  last_slide_log_id_ = INT64_MAX;
}

OB_SERIALIZE_MEMBER(LogGetMCStResp, msg_proposal_id_, max_flushed_end_lsn_, \
    is_normal_replica_, need_update_config_meta_, last_slide_log_id_);
// ================= LogGetMCStResp end ================

// ============= LogLearnerReq begin ==============
LogLearnerReq::LogLearnerReq(): sender_(), req_type_(INVALID_LEARNER_REQ_TYPE)
{
}

LogLearnerReq::LogLearnerReq(const LogLearner &sender,
                             const LogLearnerReqType req_type)
    : sender_(sender),
    req_type_(req_type)
{
}

LogLearnerReq::~LogLearnerReq()
{
  reset();
}

bool LogLearnerReq::is_valid() const
{
  bool bool_ret = false;
  if (OB_UNLIKELY(req_type_ == INVALID_LEARNER_REQ_TYPE)) {
    // pass
  } else {
    bool_ret = sender_.is_valid();
  }
  return bool_ret;
}

void LogLearnerReq::reset()
{
  sender_.reset();
  req_type_ = INVALID_LEARNER_REQ_TYPE;
}

OB_SERIALIZE_MEMBER(LogLearnerReq, sender_, req_type_);
// ============= LogLearnerReq end ==============

// ============= LogRegisterParentReq begin ==============
LogRegisterParentReq::LogRegisterParentReq() : child_(), is_to_leader_(false)
{
}

LogRegisterParentReq::LogRegisterParentReq(const LogLearner &child,
                                           const bool is_to_leader) : child_(child), is_to_leader_(is_to_leader)
{
}

LogRegisterParentReq::~LogRegisterParentReq()
{
  reset();
}

bool LogRegisterParentReq::is_valid() const
{
  return child_.is_valid();
}

void LogRegisterParentReq::reset()
{
  child_.reset();
  is_to_leader_ = false;
}
OB_SERIALIZE_MEMBER(LogRegisterParentReq, child_, is_to_leader_);
// ============= LogRegisterParentReq end ==============

// ============= LogRegisterParentResp begin ==============
LogRegisterParentResp::LogRegisterParentResp()
    : parent_(),
    candidate_list_(),
    reg_ret_(RegisterReturn::INVALID_REG_RET)
{
}

LogRegisterParentResp::LogRegisterParentResp(const LogLearner &parent,
                                             const LogCandidateList &candidate_list,
                                             const RegisterReturn reg_ret)
    : parent_(parent),
    candidate_list_(candidate_list),
    reg_ret_(reg_ret)
{
}

LogRegisterParentResp::~LogRegisterParentResp()
{
  reset();
}

bool LogRegisterParentResp::is_valid() const
{
  return parent_.is_valid();
}

void LogRegisterParentResp::reset()
{
  parent_.reset();
  candidate_list_.reset();
  reg_ret_ = RegisterReturn::INVALID_REG_RET;
}

OB_SERIALIZE_MEMBER(LogRegisterParentResp, parent_, candidate_list_, reg_ret_);
// ============= LogRegisterParentResp end ==============

// ============= CommittedInfo begin ==============
CommittedInfo::CommittedInfo()
    : msg_proposal_id_(INVALID_PROPOSAL_ID),
      prev_log_id_(OB_INVALID_LOG_ID),
      prev_log_proposal_id_(INVALID_PROPOSAL_ID),
      committed_end_lsn_()
{
}

CommittedInfo::CommittedInfo(const int64_t &msg_proposal_id,
                             const int64_t prev_log_id,
                             const int64_t &prev_log_proposal_id,
                             const LSN &committed_end_lsn)
    : msg_proposal_id_(msg_proposal_id),
      prev_log_id_(prev_log_id),
      prev_log_proposal_id_(prev_log_proposal_id),
      committed_end_lsn_(committed_end_lsn)
{
}

CommittedInfo::~CommittedInfo()
{
  reset();
}

bool CommittedInfo::is_valid() const
{
  // NB: prev_lsn may be invalid
  return INVALID_PROPOSAL_ID != msg_proposal_id_
    && OB_INVALID_LOG_ID != prev_log_id_
    && INVALID_PROPOSAL_ID != prev_log_proposal_id_
    && true == committed_end_lsn_.is_valid();
}

void CommittedInfo::reset()
{
  msg_proposal_id_ = INVALID_PROPOSAL_ID;
  prev_log_id_ = OB_INVALID_LOG_ID;
  prev_log_proposal_id_ = INVALID_PROPOSAL_ID;
  committed_end_lsn_.reset();
}

OB_SERIALIZE_MEMBER(CommittedInfo, msg_proposal_id_, prev_log_id_, \
    prev_log_proposal_id_, committed_end_lsn_);
// ============= CommittedInfo end ==============

// ================= LogGetStatReq start ================
LogGetStatReq::LogGetStatReq()
  : get_type_(LogGetStatType::INVALID_SYNC_GET_TYPE)
{
}

LogGetStatReq::LogGetStatReq(const LogGetStatType get_type)
  : get_type_(get_type)
{
}

LogGetStatReq::~LogGetStatReq()
{
  reset();
}

bool LogGetStatReq::is_valid() const
{
  return get_type_ != LogGetStatType::INVALID_SYNC_GET_TYPE;
}

void LogGetStatReq::reset()
{
  get_type_ = LogGetStatType::INVALID_SYNC_GET_TYPE;
}

OB_SERIALIZE_MEMBER(LogGetStatReq, get_type_);
// ================= LogGetStatReq end ================

// ================= LogGetStatResp start ================
LogGetStatResp::LogGetStatResp()
  : max_scn_(), end_lsn_()
{
}

LogGetStatResp::LogGetStatResp(const share::SCN &max_scn, const LSN &end_lsn)
  : max_scn_(max_scn), end_lsn_(end_lsn)
{
}

LogGetStatResp::~LogGetStatResp()
{
  reset();
}

bool LogGetStatResp::is_valid() const
{
  return true;
}

void LogGetStatResp::reset()
{
  max_scn_.reset();
  end_lsn_.reset();
}

OB_SERIALIZE_MEMBER(LogGetStatResp, max_scn_, end_lsn_);
// ================= LogGetStatResp end ================

#ifdef OB_BUILD_ARBITRATION
LogGetArbMemberInfoReq::LogGetArbMemberInfoReq()
  : palf_id_(-1)
{}

LogGetArbMemberInfoReq::LogGetArbMemberInfoReq(const int64_t palf_id)
  : palf_id_(palf_id)
{}

LogGetArbMemberInfoReq::~LogGetArbMemberInfoReq()
{
  reset();
}

bool LogGetArbMemberInfoReq::is_valid() const
{
  return is_valid_palf_id(palf_id_);
}

void LogGetArbMemberInfoReq::reset()
{
  palf_id_ = -1;
}

OB_SERIALIZE_MEMBER(LogGetArbMemberInfoReq, palf_id_);

LogGetArbMemberInfoResp::LogGetArbMemberInfoResp()
  : arb_member_info_()
{}

LogGetArbMemberInfoResp::~LogGetArbMemberInfoResp()
{
  reset();
}

bool LogGetArbMemberInfoResp::is_valid() const
{
  return arb_member_info_.is_valid();
}

void LogGetArbMemberInfoResp::reset()
{
  arb_member_info_.reset();
}

OB_SERIALIZE_MEMBER(LogGetArbMemberInfoResp, arb_member_info_);

ArbMemberInfo::ArbMemberInfo()
   :  palf_id_(INVALID_PALF_ID),
      arb_server_(),
      log_proposal_id_(INVALID_PROPOSAL_ID),
      config_version_(),
      mode_version_(INVALID_PROPOSAL_ID),
      access_mode_(AccessMode::INVALID_ACCESS_MODE),
      paxos_member_list_(),
      paxos_replica_num_(-1),
      arbitration_member_(),
      degraded_list_() { }

bool ArbMemberInfo::is_valid() const
{
  return (arb_server_.is_valid() && palf_id_ != INVALID_PALF_ID);
}

void ArbMemberInfo::reset()
{
  palf_id_ = INVALID_PALF_ID;
  arb_server_.reset();
  log_proposal_id_ = INVALID_PROPOSAL_ID;
  config_version_.reset();
  mode_version_ = INVALID_PROPOSAL_ID;
  access_mode_ = AccessMode::INVALID_ACCESS_MODE;
  paxos_member_list_.reset();
  paxos_replica_num_ = -1;
  arbitration_member_.reset();
  degraded_list_.reset();
}

OB_SERIALIZE_MEMBER(ArbMemberInfo, palf_id_, arb_server_, log_proposal_id_,
    config_version_, mode_version_, access_mode_, paxos_member_list_,
    paxos_replica_num_, arbitration_member_, degraded_list_);
#endif

LogBatchFetchResp::LogBatchFetchResp()
    : msg_proposal_id_(INVALID_PROPOSAL_ID),
      prev_log_proposal_id_(INVALID_PROPOSAL_ID),
      prev_lsn_(),
      curr_lsn_(),
      write_buf_()
{
}

LogBatchFetchResp::LogBatchFetchResp(const int64_t msg_proposal_id,
                                     const int64_t prev_log_proposal_id,
                                     const LSN &prev_lsn,
                                     const LSN &curr_lsn,
                                     const LogWriteBuf &write_buf)
    : msg_proposal_id_(msg_proposal_id),
      prev_log_proposal_id_(prev_log_proposal_id),
      prev_lsn_(prev_lsn),
      curr_lsn_(curr_lsn),
      write_buf_(write_buf)
{
}

LogBatchFetchResp::~LogBatchFetchResp()
{
  reset();
}

bool LogBatchFetchResp::is_valid() const
{
  return INVALID_PROPOSAL_ID != msg_proposal_id_
      && true == curr_lsn_.is_valid()
      && true == write_buf_.is_valid();
}

void LogBatchFetchResp::reset()
{
  msg_proposal_id_ = INVALID_PROPOSAL_ID;
  prev_log_proposal_id_ = INVALID_PROPOSAL_ID;
  prev_lsn_.reset();
  curr_lsn_.reset();
  write_buf_.reset();
}

OB_DEF_SERIALIZE(LogBatchFetchResp)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, msg_proposal_id_))
             || OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, prev_log_proposal_id_))
             || OB_FAIL(prev_lsn_.serialize(buf, buf_len, new_pos))
             || OB_FAIL(curr_lsn_.serialize(buf, buf_len, new_pos))
             || OB_FAIL(write_buf_.serialize(buf, buf_len, new_pos))) {
    PALF_LOG(ERROR, "LogBatchFetchResp serialize failed", K(ret), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_DESERIALIZE(LogBatchFetchResp)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || pos < 0 || pos > data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos,
                                               reinterpret_cast<int64_t *>(&msg_proposal_id_)))
             || OB_FAIL(serialization::decode_i64(buf, data_len, new_pos,
                                               reinterpret_cast<int64_t *>(&prev_log_proposal_id_)))
             || OB_FAIL(prev_lsn_.deserialize(buf, data_len, new_pos))
             || OB_FAIL(curr_lsn_.deserialize(buf, data_len, new_pos))
             || OB_FAIL(write_buf_.deserialize(buf, data_len, new_pos))) {
    PALF_LOG(ERROR, "LogBatchFetchResp deserialize failed", K(ret), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(LogBatchFetchResp)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(msg_proposal_id_);
  size += serialization::encoded_length_i64(prev_log_proposal_id_);
  size += prev_lsn_.get_serialize_size();
  size += curr_lsn_.get_serialize_size();
  size += write_buf_.get_serialize_size();
  return size;
}

} // end namespace palf
} // end namespace oceanbase
