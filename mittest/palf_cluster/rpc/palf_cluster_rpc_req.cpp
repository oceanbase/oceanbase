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

#include "palf_cluster_rpc_req.h"
#include "logservice/palf/log_define.h"

namespace oceanbase
{
using namespace share;
namespace palfcluster
{

// ============= LogCreateReplicaCmd start =============
LogCreateReplicaCmd::LogCreateReplicaCmd()
    : src_(),
      ls_id_(-1),
      member_list_(),
      replica_num_(-1),
      leader_idx_(-1) { }

LogCreateReplicaCmd::LogCreateReplicaCmd(
    const common::ObAddr &src,
    const int64_t ls_id,
    const common::ObMemberList &member_list,
    const int64_t replica_num,
    const int64_t leader_idx)
    : src_(src),
      ls_id_(ls_id),
      member_list_(member_list),
      replica_num_(replica_num),
      leader_idx_(leader_idx) { }

bool LogCreateReplicaCmd::is_valid() const
{
  return src_.is_valid() &&
         -1 != ls_id_ &&
         member_list_.is_valid() &&
         replica_num_ > 0 &&
         replica_num_ <= common::OB_MAX_MEMBER_NUMBER;
}

void LogCreateReplicaCmd::reset()
{
  src_.reset();
  ls_id_ = -1;
  member_list_.reset();
  replica_num_ = -1;
  leader_idx_ = -1;
}

OB_SERIALIZE_MEMBER(LogCreateReplicaCmd, src_, ls_id_,
    member_list_, replica_num_, leader_idx_);
// ============= LogCreateReplicaCmd end =============

// ============= SubmitLogCmd start =============
SubmitLogCmd::SubmitLogCmd()
    : src_(),
      ls_id_(-1),
      client_id_(-1),
      log_buf_()
    { }

SubmitLogCmd::SubmitLogCmd(
    const common::ObAddr &src,
    const int64_t ls_id,
    const int64_t client_id,
    const palf::LogWriteBuf &log_buf)
    : src_(src),
      ls_id_(ls_id),
      client_id_(client_id),
      log_buf_(log_buf)
    { }

bool SubmitLogCmd::is_valid() const
{
  return src_.is_valid() &&
         -1 != ls_id_ &&
         log_buf_.is_valid();
}

void SubmitLogCmd::reset()
{
  src_.reset();
  ls_id_ = -1;
  client_id_ = -1;
  log_buf_.reset();
}

OB_SERIALIZE_MEMBER(SubmitLogCmd, src_, ls_id_, client_id_, log_buf_);
// ============= SubmitLogCmd end =============


// ============= SubmitLogCmdResp start =============
SubmitLogCmdResp::SubmitLogCmdResp()
    : src_(),
      ls_id_(-1),
      client_id_(-1)
    { }

SubmitLogCmdResp::SubmitLogCmdResp(
    const common::ObAddr &src,
    const int64_t ls_id,
    const int64_t client_id)
    : src_(src),
      ls_id_(ls_id),
      client_id_(client_id)
    { }

bool SubmitLogCmdResp::is_valid() const
{
  return src_.is_valid() &&
         -1 != ls_id_;
}

void SubmitLogCmdResp::reset()
{
  src_.reset();
  ls_id_ = -1;
  client_id_ = -1;
}

OB_SERIALIZE_MEMBER(SubmitLogCmdResp, src_, ls_id_, client_id_);
// ============= SubmitLogCmdResp end =============
} // end namespace logservice
}// end namespace oceanbase
