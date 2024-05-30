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

#include "ob_log_rpc_req.h"
#include "logservice/palf/log_define.h"

namespace oceanbase
{
using namespace share;
namespace logservice
{

// ============= LogConfigChangeCmd begin ===========
LogConfigChangeCmd::LogConfigChangeCmd()
  : src_(),
    palf_id_(-1),
    added_member_(),
    removed_member_(),
    curr_member_list_(),
    curr_replica_num_(0),
    new_replica_num_(0),
    cmd_type_(INVALID_CONFIG_CHANGE_CMD),
    timeout_us_(0),
    lock_owner_(palf::OB_INVALID_CONFIG_CHANGE_LOCK_OWNER),
    config_version_(),
    added_list_(),
    removed_list_() { }

LogConfigChangeCmd::LogConfigChangeCmd(
    const common::ObAddr &src,
    const int64_t palf_id,
    const common::ObMember &added_member,
    const common::ObMember &removed_member,
    const int64_t new_replica_num,
    const LogConfigChangeCmdType cmd_type,
    const int64_t timeout_us)
  : src_(src),
    palf_id_(palf_id),
    added_member_(added_member),
    removed_member_(removed_member),
    curr_member_list_(),
    curr_replica_num_(),
    new_replica_num_(new_replica_num),
    cmd_type_(cmd_type),
    timeout_us_(timeout_us),
    lock_owner_(palf::OB_INVALID_CONFIG_CHANGE_LOCK_OWNER),
    config_version_(),
    added_list_(),
    removed_list_() { }

LogConfigChangeCmd::LogConfigChangeCmd(
    const common::ObAddr &src,
    const int64_t palf_id,
    const common::ObMemberList &member_list,
    const int64_t curr_replica_num,
    const int64_t new_replica_num,
    const LogConfigChangeCmdType cmd_type,
    const int64_t timeout_us)
  : src_(src),
    palf_id_(palf_id),
    added_member_(),
    removed_member_(),
    curr_member_list_(member_list),
    curr_replica_num_(curr_replica_num),
    new_replica_num_(new_replica_num),
    cmd_type_(cmd_type),
    timeout_us_(timeout_us),
    lock_owner_(palf::OB_INVALID_CONFIG_CHANGE_LOCK_OWNER),
    config_version_(),
    added_list_(),
    removed_list_() { }

LogConfigChangeCmd::LogConfigChangeCmd(const common::ObAddr &src,
                                       const int64_t palf_id,
                                       const int64_t lock_owner,
                                       const LogConfigChangeCmdType cmd_type,
                                       const int64_t timeout_us)
    : src_(src),
      palf_id_(palf_id),
      added_member_(),
      removed_member_(),
      curr_member_list_(),
      curr_replica_num_(0),
      new_replica_num_(0),
      cmd_type_(cmd_type),
      timeout_us_(timeout_us),
      lock_owner_(lock_owner),
      config_version_(),
      added_list_(),
      removed_list_() { }

LogConfigChangeCmd::LogConfigChangeCmd(
    const common::ObAddr &src,
    const int64_t palf_id,
    const common::ObMemberList &added_list,
    const common::ObMemberList &removed_list,
    const LogConfigChangeCmdType cmd_type,
    const int64_t timeout_us)
  : src_(src),
    palf_id_(palf_id),
    added_member_(),
    removed_member_(),
    curr_member_list_(),
    curr_replica_num_(0),
    new_replica_num_(0),
    cmd_type_(cmd_type),
    timeout_us_(timeout_us),
    lock_owner_(palf::OB_INVALID_CONFIG_CHANGE_LOCK_OWNER),
    config_version_(),
    added_list_(added_list),
    removed_list_(removed_list) { }

LogConfigChangeCmd::~LogConfigChangeCmd()
{
  reset();
}

void LogConfigChangeCmd::in_leader(const palf::LogConfigVersion &config_version)
{
  config_version_ = config_version;
}

bool LogConfigChangeCmd::is_valid() const
{
  bool bool_ret = false;
  bool_ret = (src_.is_valid() && palf_id_ >= 0 && timeout_us_ > 0 &&                            \
      cmd_type_ != INVALID_CONFIG_CHANGE_CMD)? true: false;
  bool_ret = bool_ret && ((is_add_member_list() || ADD_LEARNER_CMD == cmd_type_ ||              \
      SWITCH_TO_LEARNER_CMD == cmd_type_)? added_member_.is_valid(): true);
  bool_ret = bool_ret && ((is_remove_member_list() || REMOVE_LEARNER_CMD == cmd_type_ ||        \
      SWITCH_TO_ACCEPTOR_CMD == cmd_type_)? removed_member_.is_valid(): true);
  bool_ret = bool_ret && ((is_set_new_replica_num())? is_valid_replica_num(new_replica_num_): true);
  bool_ret = bool_ret && ((CHANGE_REPLICA_NUM_CMD == cmd_type_)? curr_member_list_.is_valid()    \
      && is_valid_replica_num(curr_replica_num_) && is_valid_replica_num(new_replica_num_): true);\
  bool_ret = bool_ret && ((TRY_LOCK_CONFIG_CHANGE_CMD == cmd_type_ || UNLOCK_CONFIG_CHANGE_CMD == cmd_type_) ? \
      (palf::OB_INVALID_CONFIG_CHANGE_LOCK_OWNER != lock_owner_) : true);
  bool_ret = bool_ret && ((REPLACE_LEARNERS_CMD == cmd_type_)? (added_list_.is_valid()    \
      && removed_list_.is_valid()): true);
  return bool_ret;
}

bool LogConfigChangeCmd::is_remove_member_list() const
{
  return REMOVE_MEMBER_CMD == cmd_type_
#ifdef OB_BUILD_ARBITRATION
         || REMOVE_ARB_MEMBER_CMD == cmd_type_
#endif
         || REPLACE_MEMBER_CMD == cmd_type_
         || SWITCH_TO_LEARNER_CMD == cmd_type_
         || REPLACE_MEMBER_WITH_LEARNER_CMD == cmd_type_;
}

bool LogConfigChangeCmd::is_add_member_list() const
{
  return ADD_MEMBER_CMD == cmd_type_
#ifdef OB_BUILD_ARBITRATION
        || ADD_ARB_MEMBER_CMD == cmd_type_
#endif
        || REPLACE_MEMBER_CMD == cmd_type_
        || SWITCH_TO_ACCEPTOR_CMD == cmd_type_
        || REPLACE_MEMBER_WITH_LEARNER_CMD == cmd_type_;
}

bool LogConfigChangeCmd::is_set_new_replica_num() const
{
  return ADD_MEMBER_CMD == cmd_type_
        || REMOVE_MEMBER_CMD == cmd_type_
        || SWITCH_TO_LEARNER_CMD == cmd_type_
        || SWITCH_TO_ACCEPTOR_CMD == cmd_type_;
}

void LogConfigChangeCmd::reset()
{
  src_.reset();
  palf_id_ = -1;
  added_member_.reset();
  removed_member_.reset();
  curr_member_list_.reset();
  curr_replica_num_ = 0;
  new_replica_num_ = 0;
  cmd_type_ = INVALID_CONFIG_CHANGE_CMD;
  timeout_us_ = 0;
  lock_owner_ = palf::OB_INVALID_CONFIG_CHANGE_LOCK_OWNER;
  config_version_.reset();
  added_list_.reset();
  removed_list_.reset();
}

OB_SERIALIZE_MEMBER(LogConfigChangeCmd, src_, palf_id_, added_member_, removed_member_,
curr_member_list_, curr_replica_num_, new_replica_num_, cmd_type_, timeout_us_, lock_owner_,
config_version_, added_list_, removed_list_, new_member_list_);
// ============= LogConfigChangeCmd end =============

// ============= LogConfigChangeCmdResp begin ===========
LogConfigChangeCmdResp::LogConfigChangeCmdResp()
{
  reset();
}


LogConfigChangeCmdResp::~LogConfigChangeCmdResp()
{
  reset();
}

bool LogConfigChangeCmdResp::is_valid() const
{
  return ret_ != OB_MAX_ERROR_CODE;
}

void LogConfigChangeCmdResp::reset()
{
  ret_ = OB_MAX_ERROR_CODE;
  lock_owner_ = palf::OB_INVALID_CONFIG_CHANGE_LOCK_OWNER;
  is_locked_ = false;
}

OB_SERIALIZE_MEMBER(LogConfigChangeCmdResp, ret_, lock_owner_, is_locked_);
// ============= LogConfigChangeCmdResp end =============

// ============= LogGetPalfStatReq begin ===========
LogGetPalfStatReq::LogGetPalfStatReq(
    const common::ObAddr &src_addr,
    const int64_t palf_id,
    const bool is_to_leader)
  : src_(src_addr),
    palf_id_(palf_id),
    is_to_leader_(is_to_leader) { }

LogGetPalfStatReq::~LogGetPalfStatReq()
{
  reset();
}

bool LogGetPalfStatReq::is_valid() const
{
  return src_.is_valid() && palf_id_ >= 0;
}

void LogGetPalfStatReq::reset()
{
  src_.reset();
  palf_id_ = -1;
}
OB_SERIALIZE_MEMBER(LogGetPalfStatReq, src_, palf_id_, is_to_leader_);
// ============= LogGetPalfStatReq end =============

// ============= LogGetPalfStatResp begin ===========
LogGetPalfStatResp::LogGetPalfStatResp(
    const palf::PalfStat &palf_stat)
  : palf_stat_(palf_stat) { }

LogGetPalfStatResp::~LogGetPalfStatResp()
{
  reset();
}

bool LogGetPalfStatResp::is_valid() const
{
  return palf_stat_.is_valid();
}

void LogGetPalfStatResp::reset()
{
  palf_stat_.reset();
}

OB_SERIALIZE_MEMBER(LogGetPalfStatResp, palf_stat_);
// ============= LogGetPalfStatResp end =============

// ============= LogServerProbeMsg start =============
LogServerProbeMsg::LogServerProbeMsg()
    : src_(),
      palf_id_(-1),
      req_id_(-1),
      msg_type_(PROBE_REQ),
      server_status_(-1) { }


LogServerProbeMsg::LogServerProbeMsg(
    const common::ObAddr &src,
    const int64_t palf_id,
    const int64_t req_id,
    const LogServerProbeType msg_type,
    const int64_t status)
    : src_(src),
      palf_id_(palf_id),
      req_id_(req_id),
      msg_type_(msg_type),
      server_status_(status) { }

LogServerProbeMsg::~LogServerProbeMsg()
{
  reset();
}

bool LogServerProbeMsg::is_valid() const
{
  return src_.is_valid() && -1 != palf_id_ && req_id_ != -1 && server_status_ != -1;
}

void LogServerProbeMsg::reset()
{
  src_.reset();
  palf_id_ = -1;
  req_id_ = -1;
  msg_type_ = PROBE_REQ;
  server_status_ = -1;
}

OB_SERIALIZE_MEMBER(LogServerProbeMsg, src_, palf_id_, req_id_, msg_type_, server_status_);
// ============= LogServerProbeMsg end =============

// ============= LogChangeAccessModeCmd start =============
LogChangeAccessModeCmd::LogChangeAccessModeCmd()
    : src_(),
      ls_id_(-1),
      mode_version_(palf::INVALID_PROPOSAL_ID),
      access_mode_(palf::AccessMode::INVALID_ACCESS_MODE),
      ref_scn_() { }


LogChangeAccessModeCmd::LogChangeAccessModeCmd(
    const common::ObAddr &src,
    const int64_t ls_id,
    const int64_t mode_version,
    const palf::AccessMode &access_mode,
    const SCN &ref_scn)
    : src_(src),
      ls_id_(ls_id),
      mode_version_(mode_version),
      access_mode_(access_mode),
      ref_scn_(ref_scn) { }

bool LogChangeAccessModeCmd::is_valid() const
{
  return src_.is_valid() &&
         -1 != ls_id_ &&
         palf::INVALID_PROPOSAL_ID != mode_version_ &&
         palf::AccessMode::INVALID_ACCESS_MODE != access_mode_ &&
         ref_scn_.is_valid();
}

void LogChangeAccessModeCmd::reset()
{
  src_.reset();
  ls_id_ = -1;
  mode_version_ = palf::INVALID_PROPOSAL_ID;
  access_mode_ = palf::AccessMode::INVALID_ACCESS_MODE;
  ref_scn_.reset();
}

OB_SERIALIZE_MEMBER(LogChangeAccessModeCmd, src_, ls_id_, mode_version_, access_mode_, ref_scn_);
// ============= LogChangeAccessModeCmd end =============

// ============= LogFlashbackMsg start =============
LogFlashbackMsg::LogFlashbackMsg()
    : src_tenant_id_(OB_INVALID_TENANT_ID),
      src_(),
      ls_id_(-1),
      mode_version_(palf::INVALID_PROPOSAL_ID),
      flashback_scn_(),
      is_flashback_req_(false) { }

LogFlashbackMsg::LogFlashbackMsg(
    const uint64_t src_tenant_id,
    const common::ObAddr &src,
    const int64_t ls_id,
    const int64_t mode_version,
    const SCN &flashback_scn,
    const bool is_flashback_req)
    : src_tenant_id_(src_tenant_id),
      src_(src),
      ls_id_(ls_id),
      mode_version_(mode_version),
      flashback_scn_(flashback_scn),
      is_flashback_req_(is_flashback_req) { }

bool LogFlashbackMsg::is_valid() const
{
  return is_valid_tenant_id(src_tenant_id_) &&
         src_.is_valid() &&
         -1 != ls_id_ &&
         palf::INVALID_PROPOSAL_ID != mode_version_ &&
         flashback_scn_.is_valid();
}

void LogFlashbackMsg::reset()
{
  src_tenant_id_ = OB_INVALID_TENANT_ID;
  src_.reset();
  ls_id_ = -1;
  mode_version_ = palf::INVALID_PROPOSAL_ID;
  flashback_scn_.reset();
  is_flashback_req_ = false;
}

OB_SERIALIZE_MEMBER(LogFlashbackMsg, src_tenant_id_, src_, ls_id_,
    mode_version_, flashback_scn_, is_flashback_req_);
// ============= LogFlashbackMsg end =============

} // end namespace logservice
}// end namespace oceanbase
