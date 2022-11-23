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

namespace oceanbase
{
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
    timeout_ns_(0) { }

LogConfigChangeCmd::LogConfigChangeCmd(
    const common::ObAddr &src,
    const int64_t palf_id,
    const common::ObMember &added_member,
    const common::ObMember &removed_member,
    const int64_t new_replica_num,
    const LogConfigChangeCmdType cmd_type,
    const int64_t timeout_ns)
  : src_(src),
    palf_id_(palf_id),
    added_member_(added_member),
    removed_member_(removed_member),
    curr_member_list_(),
    curr_replica_num_(),
    new_replica_num_(new_replica_num),
    cmd_type_(cmd_type),
    timeout_ns_(timeout_ns) { }

LogConfigChangeCmd::LogConfigChangeCmd(
    const common::ObAddr &src,
    const int64_t palf_id,
    const common::ObMemberList &member_list,
    const int64_t curr_replica_num,
    const int64_t new_replica_num,
    const LogConfigChangeCmdType cmd_type,
    const int64_t timeout_ns)
  : src_(src),
    palf_id_(palf_id),
    added_member_(),
    removed_member_(),
    curr_member_list_(member_list),
    curr_replica_num_(curr_replica_num),
    new_replica_num_(new_replica_num),
    cmd_type_(cmd_type),
    timeout_ns_(timeout_ns) { }

LogConfigChangeCmd::~LogConfigChangeCmd()
{
  reset();
}

bool LogConfigChangeCmd::is_valid() const
{
  bool bool_ret = false;
  bool_ret = (src_.is_valid() && palf_id_ >= 0 && timeout_ns_ > 0 &&                            \
      cmd_type_ != INVALID_CONFIG_CHANGE_CMD)? true: false;
  bool_ret = bool_ret && ((is_add_member_list() || ADD_LEARNER_CMD == cmd_type_ ||              \
      SWITCH_TO_LEARNER_CMD == cmd_type_)? added_member_.is_valid(): true);
  bool_ret = bool_ret && ((is_remove_member_list() || REMOVE_LEARNER_CMD == cmd_type_ ||        \
      SWITCH_TO_ACCEPTOR_CMD == cmd_type_)? removed_member_.is_valid(): true);
  bool_ret = bool_ret && ((ADD_MEMBER_CMD == cmd_type_ || REMOVE_MEMBER_CMD == cmd_type_ ||     \
      ADD_ARB_MEMBER_CMD == cmd_type_ || REMOVE_ARB_MEMBER_CMD == cmd_type_)?                   \
      is_valid_replica_num(new_replica_num_): true);
  bool_ret = bool_ret && ((CHANGE_REPLICA_NUM_CMD == cmd_type_)? curr_member_list_.is_valid()    \
      && is_valid_replica_num(curr_replica_num_) && is_valid_replica_num(new_replica_num_): true);
  return bool_ret;
}

bool LogConfigChangeCmd::is_remove_member_list() const
{
  return REMOVE_MEMBER_CMD == cmd_type_ || REMOVE_ARB_MEMBER_CMD == cmd_type_ || REPLACE_MEMBER_CMD == cmd_type_ ||
         REPLACE_ARB_MEMBER_CMD == cmd_type_ || SWITCH_TO_LEARNER_CMD == cmd_type_;
}

bool LogConfigChangeCmd::is_add_member_list() const
{
  return ADD_MEMBER_CMD == cmd_type_ || ADD_ARB_MEMBER_CMD == cmd_type_ || REPLACE_MEMBER_CMD == cmd_type_ ||
         REPLACE_ARB_MEMBER_CMD == cmd_type_ || SWITCH_TO_ACCEPTOR_CMD == cmd_type_;
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
  timeout_ns_ = 0;
}

OB_SERIALIZE_MEMBER(LogConfigChangeCmd, src_, palf_id_, added_member_, removed_member_,
curr_member_list_, curr_replica_num_, new_replica_num_, cmd_type_, timeout_ns_);
// ============= LogConfigChangeCmd end =============

// ============= LogConfigChangeCmdResp begin ===========
LogConfigChangeCmdResp::LogConfigChangeCmdResp()
  : ret_(OB_MAX_ERROR_CODE) { }

LogConfigChangeCmdResp::LogConfigChangeCmdResp(
    const int ret)
    : ret_(ret) { }

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
}
OB_SERIALIZE_MEMBER(LogConfigChangeCmdResp, ret_);
// ============= LogConfigChangeCmdResp end =============

// ============= LogGetPalfStatReq begin ===========
LogGetPalfStatReq::LogGetPalfStatReq(
    const common::ObAddr &src_addr,
    const int64_t palf_id)
  : src_(src_addr), palf_id_(palf_id) { }

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
OB_SERIALIZE_MEMBER(LogGetPalfStatReq, src_, palf_id_);
// ============= LogGetPalfStatReq end =============

// ============= LogGetPalfStatResp begin ===========
LogGetPalfStatResp::LogGetPalfStatResp(
    const int64_t max_ts_ns)
  : max_ts_ns_(max_ts_ns) { }

LogGetPalfStatResp::~LogGetPalfStatResp()
{
  reset();
}

bool LogGetPalfStatResp::is_valid() const
{
  return max_ts_ns_ != OB_INVALID_TIMESTAMP;
}

void LogGetPalfStatResp::reset()
{
  max_ts_ns_ = OB_INVALID_TIMESTAMP;
}
OB_SERIALIZE_MEMBER(LogGetPalfStatResp, max_ts_ns_);
// ============= LogGetPalfStatResp end =============
} // end namespace logservice
}// end namespace oceanbase