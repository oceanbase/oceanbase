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

#include "storage/ob_base_storage_info.h"

namespace oceanbase {
namespace common {
ObBaseStorageInfo::ObBaseStorageInfo()
    : version_(STORAGE_INFO_VERSION),
      epoch_id_(0),
      proposal_id_(),
      last_replay_log_id_(0),
      last_submit_timestamp_(0),
      accumulate_checksum_(0),
      replica_num_(0),
      membership_timestamp_(0),
      membership_log_id_(0),
      curr_member_list_(),
      ms_proposal_id_()
{}

ObBaseStorageInfo::~ObBaseStorageInfo()
{}

int ObBaseStorageInfo::init(const int64_t epoch_id, const ObProposalID& proposal_id, const uint64_t last_replay_log_id,
    const int64_t last_submit_timestamp, const int64_t accumulate_checksum, const int64_t replica_num,
    const int64_t membership_timestamp, const uint64_t membership_log_id, const common::ObMemberList& curr_member_list,
    const ObProposalID& ms_proposal_id)
{
  int ret = OB_SUCCESS;
  if (last_submit_timestamp < 0 || replica_num <= 0 || !curr_member_list.is_valid()) {
    COMMON_LOG(WARN,
        "invalid arguments",
        K(epoch_id),
        K(proposal_id),
        K(last_replay_log_id),
        K(last_submit_timestamp),
        K(accumulate_checksum),
        K(replica_num),
        K(membership_timestamp),
        K(membership_log_id),
        K(curr_member_list));
    ret = OB_INVALID_ARGUMENT;
  } else {
    version_ = STORAGE_INFO_VERSION;
    epoch_id_ = epoch_id;
    proposal_id_ = proposal_id;
    last_replay_log_id_ = last_replay_log_id;
    last_submit_timestamp_ = last_submit_timestamp;
    accumulate_checksum_ = accumulate_checksum;
    replica_num_ = replica_num;
    membership_timestamp_ = membership_timestamp;
    membership_log_id_ = membership_log_id;
    ms_proposal_id_ = ms_proposal_id;
    if (OB_SUCCESS != (ret = curr_member_list_.deep_copy(curr_member_list))) {
      COMMON_LOG(WARN, "fail to deep_copy member list", K(ret));
    }
  }
  return ret;
}

int ObBaseStorageInfo::init(const int64_t replica_num, const common::ObMemberList& member_list,
    const int64_t last_submit_timestamp, const uint64_t last_replay_log_id, const bool skip_mlist_check)
{
  int ret = OB_SUCCESS;
  if (replica_num <= 0 || replica_num > OB_MAX_MEMBER_NUMBER || (!skip_mlist_check && !member_list.is_valid()) ||
      last_submit_timestamp < 0 || OB_INVALID_ID == last_replay_log_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid argument",
        K(ret),
        K(replica_num),
        K(member_list),
        K(last_submit_timestamp),
        K(last_replay_log_id));
  } else if (!skip_mlist_check && OB_FAIL(curr_member_list_.deep_copy(member_list))) {
    CLOG_LOG(WARN, "deep_copy failed", K(ret), K(member_list));
  } else {
    version_ = STORAGE_INFO_VERSION;
    epoch_id_ = 0;
    proposal_id_.reset();
    last_replay_log_id_ = last_replay_log_id;
    last_submit_timestamp_ = last_submit_timestamp;
    accumulate_checksum_ = 0;
    replica_num_ = replica_num;
    membership_timestamp_ = 0;
    membership_log_id_ = 0;
  }
  return ret;
}

bool ObBaseStorageInfo::is_valid() const
{
  return STORAGE_INFO_VERSION == version_ &&
         epoch_id_ >= 0
         //&& proposal_id_.is_valid()
         //&& last_replay_log_id_ >= 0
         && last_submit_timestamp_ >= 0 && accumulate_checksum_ >= 0 && replica_num_ > 0 && membership_timestamp_ >= 0;
  //&& membership_log_id_ >= 0;
}

void ObBaseStorageInfo::reset()
{
  version_ = STORAGE_INFO_VERSION;
  epoch_id_ = 0;
  proposal_id_.reset();
  ms_proposal_id_.reset();
  last_replay_log_id_ = 0;
  last_submit_timestamp_ = 0;
  accumulate_checksum_ = 0;
  replica_num_ = 0;
  membership_timestamp_ = 0;
  membership_log_id_ = 0;
  curr_member_list_.reset();
}

int64_t ObBaseStorageInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  common::databuff_printf(buf, buf_len, pos, "epoch_id = %ld,", epoch_id_);
  common::databuff_printf(buf, buf_len, pos, "proposal_id = %s,", to_cstring(proposal_id_));
  common::databuff_printf(buf, buf_len, pos, "last_replay_log_id = %lu,", last_replay_log_id_);
  common::databuff_printf(buf, buf_len, pos, "last_submit_timestamp = %ld,", last_submit_timestamp_);
  common::databuff_printf(buf, buf_len, pos, "accumulate_checksum = %ld,", accumulate_checksum_);
  common::databuff_printf(buf, buf_len, pos, "replica_num = %ld,", replica_num_);
  common::databuff_printf(buf, buf_len, pos, "membership_timestamp = %ld,", membership_timestamp_);
  common::databuff_printf(buf, buf_len, pos, "membership_log_id_ = %lu,", membership_log_id_);
  common::databuff_printf(buf, buf_len, pos, "curr_member_list = %s", to_cstring(curr_member_list_));
  common::databuff_printf(buf, buf_len, pos, "proposal_id = %s,", to_cstring(ms_proposal_id_));
  return pos;
}

int ObBaseStorageInfo::deep_copy(const ObBaseStorageInfo& base_storage_info)
{
  int ret = OB_SUCCESS;
  if (!base_storage_info.is_valid()) {
    COMMON_LOG(WARN, "invalid arguments", K(base_storage_info));
    ret = OB_INVALID_ARGUMENT;
  } else {
    version_ = base_storage_info.version_;
    epoch_id_ = base_storage_info.epoch_id_;
    proposal_id_ = base_storage_info.proposal_id_;
    last_replay_log_id_ = base_storage_info.last_replay_log_id_;
    last_submit_timestamp_ = base_storage_info.last_submit_timestamp_;
    accumulate_checksum_ = base_storage_info.accumulate_checksum_;
    replica_num_ = base_storage_info.replica_num_;
    membership_timestamp_ = base_storage_info.membership_timestamp_;
    membership_log_id_ = base_storage_info.membership_log_id_;
    ms_proposal_id_ = base_storage_info.get_ms_proposal_id();
    ret = curr_member_list_.deep_copy(base_storage_info.curr_member_list_);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObBaseStorageInfo, version_, epoch_id_, proposal_id_, last_replay_log_id_, last_submit_timestamp_,
    accumulate_checksum_, replica_num_, membership_timestamp_, membership_log_id_, curr_member_list_, ms_proposal_id_);

int ObBaseStorageInfo::try_update_member_list(const uint64_t ms_log_id, const int64_t mc_timestamp,
    const int64_t replica_num, const ObMemberList& mlist, const common::ObProposalID& ms_proposal_id)
{
  int ret = OB_SUCCESS;
  if (mc_timestamp == membership_timestamp_) {
    ret = OB_ENTRY_EXIST;
  } else if (mc_timestamp > membership_timestamp_) {
    membership_log_id_ = ms_log_id;
    membership_timestamp_ = mc_timestamp;
    replica_num_ = replica_num;
    curr_member_list_ = mlist;
    ms_proposal_id_ = ms_proposal_id;
    COMMON_LOG(INFO,
        "ObBaseStorageInfo update member list success",
        K(ms_log_id),
        K(mc_timestamp),
        K(replica_num),
        K(mlist),
        K(ms_proposal_id));
  }
  return ret;
}

int ObBaseStorageInfo::standby_force_update_member_list(const uint64_t ms_log_id, const int64_t mc_timestamp,
    const int64_t replica_num, const ObMemberList& mlist, const common::ObProposalID& ms_proposal_id)
{
  int ret = OB_SUCCESS;
  membership_log_id_ = ms_log_id;
  membership_timestamp_ = mc_timestamp;
  replica_num_ = replica_num;
  curr_member_list_ = mlist;
  ms_proposal_id_ = ms_proposal_id;
  COMMON_LOG(INFO,
      "standby force update member list success",
      K(ms_log_id),
      K(mc_timestamp),
      K(replica_num),
      K(mlist),
      K(ms_proposal_id));
  return ret;
}

}  // namespace common
}  // namespace oceanbase
