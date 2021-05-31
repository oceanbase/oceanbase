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

#include "ob_election_msg.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::election;

OB_SERIALIZE_MEMBER((ObElectionMsgBuffer, ObDataBuffer));
OB_SERIALIZE_MEMBER(ObElectionMsg, msg_type_, send_timestamp_, reserved_, sender_);
OB_SERIALIZE_MEMBER((ObElectionVoteMsg, ObElectionMsg), T1_timestamp_);
OB_SERIALIZE_MEMBER((ObElectionMsgDEPrepare, ObElectionVoteMsg), priority_, lease_time_);
OB_SERIALIZE_MEMBER((ObElectionMsgDEVote, ObElectionVoteMsg), vote_leader_);
OB_SERIALIZE_MEMBER((ObElectionMsgDESuccess, ObElectionVoteMsg), leader_, lease_time_);
OB_SERIALIZE_MEMBER((ObElectionMsgPrepare, ObElectionVoteMsg), cur_leader_, new_leader_, lease_time_);
OB_SERIALIZE_MEMBER((ObElectionMsgVote, ObElectionVoteMsg), priority_, cur_leader_, new_leader_);
OB_SERIALIZE_MEMBER(
    (ObElectionMsgSuccess, ObElectionVoteMsg), cur_leader_, new_leader_, lease_time_, last_gts_, last_leader_epoch_);
OB_SERIALIZE_MEMBER((ObElectionMsgEGPrepare, ObElectionVoteMsg), cur_leader_, new_leader_, lease_time_);
OB_SERIALIZE_MEMBER((ObElectionMsgEGVote, ObElectionVoteMsg), priority_, cur_leader_, new_leader_);
OB_SERIALIZE_MEMBER((ObElectionMsgEGSuccess, ObElectionVoteMsg), cur_leader_, new_leader_, is_all_part_merged_in_,
    majority_part_idx_array_, lease_time_);
OB_SERIALIZE_MEMBER((ObElectionQueryLeader, ObElectionMsg));
OB_SERIALIZE_MEMBER((ObElectionQueryLeaderResponse, ObElectionMsg), leader_, epoch_, t1_, lease_time_);

// election base message
bool ObElectionMsg::is_valid() const
{
  return (msg_type_ >= OB_ELECTION_DEVOTE_PREPARE && msg_type_ <= OB_ELECTION_EG_DESTROY) ||
         (msg_type_ >= OB_ELECTION_QUERY_LEADER && msg_type_ <= OB_ELECTION_NOTIFY_MEMBER_LIST);
}

int64_t ObElectionMsg::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    databuff_printf(
        buf, buf_len, pos, "[type=%d, send_ts=%ld, sender=%s", msg_type_, send_timestamp_, to_cstring(sender_));
  }

  return pos;
}

// election vote message
bool ObElectionVoteMsg::is_valid() const
{
  bool flag = ObElectionMsg::is_valid();

  if (flag) {
    flag = (T1_timestamp_ > 0 && T1_timestamp_ % T_ELECT2 == 0);
  }

  return flag;
}

int64_t ObElectionVoteMsg::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    pos = ObElectionMsg::to_string(buf, buf_len);
    databuff_printf(buf, buf_len, pos, ", T1=%ld", T1_timestamp_);
  }

  return pos;
}

// election message: OB_ELECTION_DEVOTE_PREPARE
ObElectionMsgDEPrepare::ObElectionMsgDEPrepare(const ObElectionPriority& priority, const int64_t t1,
    const int64_t send_ts, const ObAddr& sender, const int64_t lease_time)
    : ObElectionVoteMsg(OB_ELECTION_DEVOTE_PREPARE, send_ts, sender, t1), priority_(priority), lease_time_(lease_time)
{}

int64_t ObElectionMsgDEPrepare::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    pos = ObElectionVoteMsg::to_string(buf, buf_len);
    databuff_printf(buf, buf_len, pos, ", priority=%s lease_time=%ld]", to_cstring(priority_), lease_time_);
  }

  return pos;
}

// election message: OB_ELECTION_DEVOTE_VOTE
ObElectionMsgDEVote::ObElectionMsgDEVote(
    const ObAddr& leader, const int64_t t1, const int64_t send_ts, const ObAddr& sender)
    : ObElectionVoteMsg(OB_ELECTION_DEVOTE_VOTE, send_ts, sender, t1), vote_leader_(leader)
{}

int64_t ObElectionMsgDEVote::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    pos = ObElectionVoteMsg::to_string(buf, buf_len);
    databuff_printf(buf, buf_len, pos, ", vote_leader=%s]", to_cstring(vote_leader_));
  }

  return pos;
}

// election message: OB_ELECTION_DEVOTE_SUCCESS
ObElectionMsgDESuccess::ObElectionMsgDESuccess(
    const ObAddr& leader, const int64_t t1, const int64_t send_ts, const ObAddr& sender, const int64_t lease_time)
    : ObElectionVoteMsg(OB_ELECTION_DEVOTE_SUCCESS, send_ts, sender, t1), leader_(leader), lease_time_(lease_time)
{}

int64_t ObElectionMsgDESuccess::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    pos = ObElectionVoteMsg::to_string(buf, buf_len);
    databuff_printf(buf, buf_len, pos, ", leader=%s, lease_time=%ld]", to_cstring(leader_), lease_time_);
  }

  return pos;
}

// election message: OB_ELECTION_VOTE_PREPARE
ObElectionMsgPrepare::ObElectionMsgPrepare(const ObAddr& cur_leader, const ObAddr& new_leader, const int64_t t1,
    const int64_t send_ts, const ObAddr& sender, const int64_t lease_time)
    : ObElectionVoteMsg(OB_ELECTION_VOTE_PREPARE, send_ts, sender, t1),
      cur_leader_(cur_leader),
      new_leader_(new_leader),
      lease_time_(lease_time)
{}

bool ObElectionMsgPrepare::is_valid() const
{
  return (ObElectionVoteMsg::is_valid() && cur_leader_.is_valid() && new_leader_.is_valid() && lease_time_ >= 0);
}

int64_t ObElectionMsgPrepare::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    pos = ObElectionVoteMsg::to_string(buf, buf_len);
    databuff_printf(buf,
        buf_len,
        pos,
        ", cur_leader=%s, new_leader=%s, lease_time=%ld]",
        to_cstring(cur_leader_),
        to_cstring(new_leader_),
        lease_time_);
  }

  return pos;
}

// election message: OB_ELECTION_VOTE_VOTE
ObElectionMsgVote::ObElectionMsgVote(const ObElectionPriority& priority, const ObAddr& cur_leader,
    const ObAddr& new_leader, const int64_t t1, const int64_t send_ts, const ObAddr& sender)
    : ObElectionVoteMsg(OB_ELECTION_VOTE_VOTE, send_ts, sender, t1),
      priority_(priority),
      cur_leader_(cur_leader),
      new_leader_(new_leader)
{}

bool ObElectionMsgVote::is_valid() const
{
  return (
      ObElectionVoteMsg::is_valid() && (cur_leader_.is_valid()) && (new_leader_.is_valid()) && (priority_.is_valid()));
}

int64_t ObElectionMsgVote::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    pos = ObElectionVoteMsg::to_string(buf, buf_len);
    databuff_printf(buf,
        buf_len,
        pos,
        ", priority=%s, cur_leader=%s, new_leader=%s]",
        to_cstring(priority_),
        to_cstring(cur_leader_),
        to_cstring(new_leader_));
  }

  return pos;
}

// election message: OB_ELECTION_VOTE_SUCCESS
ObElectionMsgSuccess::ObElectionMsgSuccess(const ObAddr& cur_leader, const ObAddr& new_leader, const int64_t t1,
    const int64_t send_ts, const ObAddr& sender, const int64_t lease_time, const int64_t last_gts,
    const int64_t last_leader_epoch)
    : ObElectionVoteMsg(OB_ELECTION_VOTE_SUCCESS, send_ts, sender, t1),
      cur_leader_(cur_leader),
      new_leader_(new_leader),
      lease_time_(lease_time),
      last_gts_(last_gts),
      last_leader_epoch_(last_leader_epoch)
{}

bool ObElectionMsgSuccess::is_valid() const
{
  return (ObElectionVoteMsg::is_valid() && cur_leader_.is_valid() && new_leader_.is_valid() && lease_time_ >= 0);
}

int64_t ObElectionMsgSuccess::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    pos = ObElectionVoteMsg::to_string(buf, buf_len);
    databuff_printf(buf,
        buf_len,
        pos,
        ", cur_leader=%s, new_leader=%s, lease_time=%ld, last_gts=%ld]",
        to_cstring(cur_leader_),
        to_cstring(new_leader_),
        lease_time_,
        last_gts_);
  }

  return pos;
}

// election query leader
ObElectionQueryLeader::ObElectionQueryLeader(const int64_t send_ts, const ObAddr& sender)
{
  msg_type_ = OB_ELECTION_QUERY_LEADER;
  send_timestamp_ = send_ts;
  sender_ = sender;
}

bool ObElectionQueryLeader::is_valid() const
{
  return ObElectionMsg::is_valid();
}

int64_t ObElectionQueryLeader::to_string(char* buf, const int64_t buf_len) const
{
  return ObElectionMsg::to_string(buf, buf_len);
}

ObElectionQueryLeaderResponse::ObElectionQueryLeaderResponse(const int64_t send_ts, const ObAddr& sender,
    const ObAddr& leader, const int64_t epoch, const int64_t t1, const int64_t lease_time)
{
  msg_type_ = OB_ELECTION_QUERY_LEADER_RESPONSE;
  send_timestamp_ = send_ts;
  sender_ = sender;
  leader_ = leader;
  epoch_ = epoch;
  t1_ = t1;
  lease_time_ = lease_time;
}

bool ObElectionQueryLeaderResponse::is_valid() const
{
  return (ObElectionMsg::is_valid() && leader_.is_valid() && t1_ > 0 && lease_time_ >= 0);
}

int64_t ObElectionQueryLeaderResponse::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    pos = ObElectionMsg::to_string(buf, buf_len);
    databuff_printf(buf,
        buf_len,
        pos,
        ", leader=%s, epoch=%ld, T1=%ld lease_time=%ld]",
        to_cstring(leader_),
        epoch_,
        t1_,
        lease_time_);
  }

  return pos;
}

// election message: OB_ELECTION_EG_VOTE_PREPARE
ObElectionMsgEGPrepare::ObElectionMsgEGPrepare(const ObAddr& cur_leader, const ObAddr& new_leader, const int64_t t1,
    const int64_t send_ts, const ObAddr& sender, const int64_t lease_time)
    : ObElectionVoteMsg(OB_ELECTION_EG_VOTE_PREPARE, send_ts, sender, t1),
      cur_leader_(cur_leader),
      new_leader_(new_leader),
      lease_time_(lease_time)
{}

bool ObElectionMsgEGPrepare::is_valid() const
{
  return (ObElectionVoteMsg::is_valid() && cur_leader_.is_valid() && new_leader_.is_valid() && lease_time_ >= 0);
}

int64_t ObElectionMsgEGPrepare::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    pos = ObElectionVoteMsg::to_string(buf, buf_len);
    databuff_printf(buf,
        buf_len,
        pos,
        ", cur_leader=%s, new_leader=%s, lease_time=%ld]",
        to_cstring(cur_leader_),
        to_cstring(new_leader_),
        lease_time_);
  }

  return pos;
}

// election message: OB_ELECTION_EG_VOTE_VOTE
ObElectionMsgEGVote::ObElectionMsgEGVote(const ObElectionGroupPriority& priority, const ObAddr& cur_leader,
    const ObAddr& new_leader, const int64_t t1, const int64_t send_ts, const ObAddr& sender)
    : ObElectionVoteMsg(OB_ELECTION_EG_VOTE_VOTE, send_ts, sender, t1),
      priority_(priority),
      cur_leader_(cur_leader),
      new_leader_(new_leader)
{}

ObElectionMsgEGVote::ObElectionMsgEGVote(const ObElectionMsgEGVote& msg)
    : ObElectionVoteMsg(OB_ELECTION_EG_VOTE_VOTE, msg.get_send_timestamp(), msg.get_sender(), msg.get_T1_timestamp())
{
  priority_ = msg.get_priority();
  cur_leader_ = msg.get_cur_leader();
  new_leader_ = msg.get_new_leader();
}

bool ObElectionMsgEGVote::is_valid() const
{
  return (
      ObElectionVoteMsg::is_valid() && (priority_.is_valid()) && (cur_leader_.is_valid()) && (new_leader_.is_valid()));
}

int64_t ObElectionMsgEGVote::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    pos = ObElectionVoteMsg::to_string(buf, buf_len);
    databuff_printf(buf,
        buf_len,
        pos,
        ", priority=%s, cur_leader=%s, new_leader=%s]",
        to_cstring(priority_),
        to_cstring(cur_leader_),
        to_cstring(new_leader_));
  }

  return pos;
}

ObElectionMsgEGVote4Store::ObElectionMsgEGVote4Store(const ObElectionMsgEGVote& eg_vote_msg)
    : ObElectionMsgEGVote(eg_vote_msg), eg_version_(0)
{}

ObElectionMsgEGVote4Store::ObElectionMsgEGVote4Store(
    const ObElectionMsgEGVote& eg_vote_msg, const int64_t eg_version, const common::ObPartitionArray& vote_array)
    : ObElectionMsgEGVote(eg_vote_msg), eg_version_(eg_version), vote_part_array_(vote_array)
{}

// election message: OB_ELECTION_EG_VOTE_SUCCESS
ObElectionMsgEGSuccess::ObElectionMsgEGSuccess(const ObAddr& cur_leader, const ObAddr& new_leader, const int64_t t1,
    const bool is_all_part_merged_in, const ObPartIdxArray& majority_part_idx_array, const int64_t send_ts,
    const ObAddr& sender, const int64_t lease_time)
    : ObElectionVoteMsg(OB_ELECTION_EG_VOTE_SUCCESS, send_ts, sender, t1),
      cur_leader_(cur_leader),
      new_leader_(new_leader),
      is_all_part_merged_in_(is_all_part_merged_in),
      majority_part_idx_array_(majority_part_idx_array),
      lease_time_(lease_time)
{}

bool ObElectionMsgEGSuccess::is_valid() const
{
  return (ObElectionVoteMsg::is_valid() && cur_leader_.is_valid() && new_leader_.is_valid() && lease_time_ >= 0);
}

int64_t ObElectionMsgEGSuccess::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    pos = ObElectionVoteMsg::to_string(buf, buf_len);
    databuff_printf(buf,
        buf_len,
        pos,
        ", cur_leader=%s, new_leader=%s, lease_time=%ld]",
        to_cstring(cur_leader_),
        to_cstring(new_leader_),
        lease_time_);
  }

  return pos;
}

int ObEGBatchReq::fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  filled_size = 0;

  if (OB_FAIL(common::serialization::encode_i64(buf, size, pos, eg_version_))) {
  } else if (OB_FAIL(common::serialization::encode_i64(buf, size, pos, buf_ser_size_))) {
  } else {
    MEMCPY(buf + pos, array_buf_, buf_ser_size_);
    pos += buf_ser_size_;
    if (OB_SUCC(eg_msg_->fill_buffer(buf + pos, size - pos, filled_size))) {
      filled_size += pos;
    }
  }

  return ret;
}

int64_t ObEGBatchReq::get_req_size() const
{
  int64_t req_size = 0;
  req_size = common::serialization::encoded_length_i64(eg_version_) +
             common::serialization::encoded_length_i64(buf_ser_size_) + buf_ser_size_ + eg_msg_->get_serialize_size();
  return req_size;
}

void ObEGBatchReq::reset()
{
  eg_version_ = 0;
  array_buf_ = NULL;
  buf_ser_size_ = 0;
  eg_msg_ = NULL;
}
