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

#include "ob_log_type.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_serialization_helper.h"

namespace oceanbase {
using namespace common;
namespace clog {
int ObConfirmedInfo::init(const int64_t data_checksum, const int64_t epoch_id, const int64_t accum_checksum)
{
  int ret = OB_SUCCESS;
  data_checksum_ = data_checksum;
  epoch_id_ = epoch_id;
  accum_checksum_ = accum_checksum;
  return ret;
}

// used for RPC
OB_SERIALIZE_MEMBER(ObConfirmedInfo, data_checksum_, epoch_id_, accum_checksum_);

ObMembershipLog::ObMembershipLog()
    : version_(MS_LOG_VERSION),
      replica_num_(0),
      timestamp_(OB_INVALID_TIMESTAMP),
      member_list_(),
      prev_member_list_(),
      type_(0),
      cluster_id_(OB_INVALID_CLUSTER_ID)
{
  member_list_.reset();
  prev_member_list_.reset();
}

ObMembershipLog::~ObMembershipLog()
{}

int64_t ObMembershipLog::get_replica_num() const
{
  return replica_num_;
}

int64_t ObMembershipLog::get_timestamp() const
{
  return timestamp_;
}

const ObMemberList& ObMembershipLog::get_member_list() const
{
  return member_list_;
}

const ObMemberList& ObMembershipLog::get_prev_member_list() const
{
  return prev_member_list_;
}

int64_t ObMembershipLog::get_type() const
{
  return type_;
}

int64_t ObMembershipLog::get_cluster_id() const
{
  return cluster_id_;
}

void ObMembershipLog::set_replica_num(const int64_t replica_num)
{
  replica_num_ = replica_num;
}

void ObMembershipLog::set_timestamp(const int64_t timestamp)
{
  timestamp_ = timestamp;
}

void ObMembershipLog::set_member_list(const ObMemberList& member_list)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = member_list_.deep_copy(member_list))) {
    CLOG_LOG(ERROR, "set_member_list failed", K(tmp_ret), K(member_list));
  }
}

void ObMembershipLog::set_prev_member_list(const ObMemberList& prev_member_list)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = prev_member_list_.deep_copy(prev_member_list))) {
    CLOG_LOG(ERROR, "set_prev_member_list failed", K(tmp_ret), K(prev_member_list));
  }
}

void ObMembershipLog::set_type(const int64_t type)
{
  type_ = type;
}

void ObMembershipLog::set_cluster_id(const int64_t cluster_id)
{
  cluster_id_ = cluster_id;
}

int64_t ObMembershipLog::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "replica_num:%ld,", replica_num_);
  databuff_printf(buf, buf_len, pos, "timetamp:%ld,", timestamp_);
  databuff_printf(buf, buf_len, pos, "member_list:%s,", to_cstring(&member_list_));
  databuff_printf(buf, buf_len, pos, "prev_member_list:%s,", to_cstring(&prev_member_list_));
  databuff_printf(buf, buf_len, pos, "type:%ld", type_);
  databuff_printf(buf, buf_len, pos, "cluster_id:%ld", cluster_id_);
  return pos;
}

OB_SERIALIZE_MEMBER(
    ObMembershipLog, version_, replica_num_, timestamp_, member_list_, prev_member_list_, type_, cluster_id_);

ObRenewMembershipLog::ObRenewMembershipLog()
    : replica_num_(0),
      timestamp_(OB_INVALID_TIMESTAMP),
      member_list_(),
      prev_member_list_(),
      type_(0),
      cluster_id_(OB_INVALID_CLUSTER_ID),
      ms_proposal_id_(),
      barrier_log_id_(OB_INVALID_ID)
{
  member_list_.reset();
  prev_member_list_.reset();
}

ObRenewMembershipLog::~ObRenewMembershipLog()
{}

int64_t ObRenewMembershipLog::get_replica_num() const
{
  return replica_num_;
}

int64_t ObRenewMembershipLog::get_timestamp() const
{
  return timestamp_;
}

const ObMemberList& ObRenewMembershipLog::get_member_list() const
{
  return member_list_;
}

const ObMemberList& ObRenewMembershipLog::get_prev_member_list() const
{
  return prev_member_list_;
}

int64_t ObRenewMembershipLog::get_type() const
{
  return type_;
}

int64_t ObRenewMembershipLog::get_cluster_id() const
{
  return cluster_id_;
}

common::ObProposalID ObRenewMembershipLog::get_ms_proposal_id() const
{
  return ms_proposal_id_;
}

uint64_t ObRenewMembershipLog::get_barrier_log_id() const
{
  return barrier_log_id_;
}

void ObRenewMembershipLog::set_ms_proposal_id(const common::ObProposalID& ms_proposal_id)
{
  ms_proposal_id_ = ms_proposal_id;
}

void ObRenewMembershipLog::set_replica_num(const int64_t replica_num)
{
  replica_num_ = replica_num;
}

void ObRenewMembershipLog::set_timestamp(const int64_t timestamp)
{
  timestamp_ = timestamp;
}

void ObRenewMembershipLog::set_member_list(const ObMemberList& member_list)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = member_list_.deep_copy(member_list))) {
    CLOG_LOG(ERROR, "set_member_list failed", K(tmp_ret), K(member_list));
  }
}

void ObRenewMembershipLog::set_prev_member_list(const ObMemberList& prev_member_list)
{
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = prev_member_list_.deep_copy(prev_member_list))) {
    CLOG_LOG(ERROR, "set_prev_member_list failed", K(tmp_ret), K(prev_member_list));
  }
}

void ObRenewMembershipLog::set_type(const int64_t type)
{
  type_ = type;
}

void ObRenewMembershipLog::set_cluster_id(const int64_t cluster_id)
{
  cluster_id_ = cluster_id;
}

void ObRenewMembershipLog::set_barrier_log_id(const uint64_t barrier_log_id)
{
  barrier_log_id_ = barrier_log_id;
}

int64_t ObRenewMembershipLog::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "replica_num:%ld,", replica_num_);
  databuff_printf(buf, buf_len, pos, "timetamp:%ld,", timestamp_);
  databuff_printf(buf, buf_len, pos, "member_list:%s,", to_cstring(&member_list_));
  databuff_printf(buf, buf_len, pos, "prev_member_list:%s,", to_cstring(&prev_member_list_));
  databuff_printf(buf, buf_len, pos, "type:%ld", type_);
  databuff_printf(buf, buf_len, pos, "cluster_id:%ld", cluster_id_);
  databuff_printf(buf, buf_len, pos, "proposal_id:%s", to_cstring(ms_proposal_id_));
  databuff_printf(buf, buf_len, pos, "barrier_log_id:%ld", barrier_log_id_);
  return pos;
}

OB_SERIALIZE_MEMBER(ObRenewMembershipLog, replica_num_, timestamp_, member_list_, prev_member_list_, type_, cluster_id_,
    ms_proposal_id_, barrier_log_id_);

OB_SERIALIZE_MEMBER(ObNopLog, version_);
OB_SERIALIZE_MEMBER(ObPreparedLog, version_);
OB_SERIALIZE_MEMBER(ObLogArchiveInnerLog, version_, checkpoint_ts_, round_start_ts_, round_snapshot_version_);
}  // namespace clog
}  // namespace oceanbase
