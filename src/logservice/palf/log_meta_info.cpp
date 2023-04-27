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

#include "log_meta_info.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/serialization.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace palf
{
using namespace common;
using namespace share;

LogVotedFor::LogVotedFor()
{
  reset();
}

LogVotedFor::LogVotedFor(const ObAddr &addr)
{
  if (addr.using_ipv4()) {
    voted_for_[0] = addr.get_ipv4();
    voted_for_[1] = addr.get_port();
  } else {
    voted_for_[0] = addr.get_ipv6_high();
    voted_for_[1] = addr.get_ipv6_low();
    voted_for_[2] = addr.get_port();
  }
}

LogVotedFor::~LogVotedFor()
{
  reset();
}

LogVotedFor::LogVotedFor(const LogVotedFor &voted_for)
{
  MEMCPY(voted_for_, voted_for.voted_for_, COUNT*serialization::encoded_length_i64(voted_for_[0]));
}

LogVotedFor::LogVotedFor(LogVotedFor &&voted_for)
{
  MEMCPY(voted_for_, voted_for.voted_for_, COUNT*serialization::encoded_length_i64(voted_for_[0]));
}

LogVotedFor & LogVotedFor::operator=(const LogVotedFor &voted_for)
{
  MEMCPY(voted_for_, voted_for.voted_for_, COUNT*serialization::encoded_length_i64(voted_for_[0]));
  return *this;
}

bool LogVotedFor::operator==(const LogVotedFor &voted_for)
{
  return 0 == MEMCMP(voted_for_, voted_for.voted_for_, COUNT*serialization::encoded_length_i64(voted_for_[0]));
}

void LogVotedFor::reset()
{
  MEMSET(voted_for_, 0, COUNT*serialization::encoded_length_i64(voted_for_[0]));
}

int64_t LogVotedFor::to_string(char *buf, const int64_t buf_len)const
{
  ObAddr addr(voted_for_[0], voted_for_[1]);
  // nowdays, just print IPV4
  return addr.to_string(buf, buf_len);
}

DEFINE_SERIALIZE(LogVotedFor)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (buf_len - new_pos < get_serialize_size()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    for (int64_t i = 0; i < COUNT && OB_SUCC(ret); i++) {
      ret = serialization::encode_i64(buf, buf_len, new_pos, voted_for_[i]); 
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogVotedFor)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || data_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (data_len - new_pos < get_serialize_size()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    for (int64_t i = 0; i < COUNT && OB_SUCC(ret); i++) {
      ret = serialization::decode_i64(buf, data_len, new_pos, &voted_for_[i]); 
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogVotedFor) 
{
  int64_t size = COUNT * serialization::encoded_length_i64(voted_for_[0]);
  return size;
}

LogPrepareMeta::LogPrepareMeta() : version_(-1), voted_for_(), log_proposal_id_(INVALID_PROPOSAL_ID)
{}

LogPrepareMeta::~LogPrepareMeta()
{
  reset();
}

int LogPrepareMeta::generate(const LogVotedFor &voted_for, const int64_t &log_proposal_id)
{
  int ret = OB_SUCCESS;
  if (INVALID_PROPOSAL_ID == log_proposal_id) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    version_ = LOG_PREPARE_VERSION;
    voted_for_ = voted_for;
    log_proposal_id_ = log_proposal_id;
  }
  return ret;
}

bool LogPrepareMeta::is_valid() const
{
  // voted_for is unused
  return version_ == LOG_PREPARE_VERSION && INVALID_PROPOSAL_ID != log_proposal_id_;
}

void LogPrepareMeta::reset()
{
  log_proposal_id_ = INVALID_PROPOSAL_ID;
  voted_for_.reset();
  version_ = -1;
}

void LogPrepareMeta::operator=(const LogPrepareMeta &log_prepare_meta)
{
  this->version_ = log_prepare_meta.version_;
  this->voted_for_ = log_prepare_meta.voted_for_;
  this->log_proposal_id_ = log_prepare_meta.log_proposal_id_;
}

DEFINE_SERIALIZE(LogPrepareMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (buf_len - new_pos < get_serialize_size()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, version_)) ||
             OB_FAIL(voted_for_.serialize(buf, buf_len, new_pos)) || 
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, log_proposal_id_))) {
    PALF_LOG(ERROR, "LogPrepareMeta serialize failed", K(ret), K(new_pos));
  } else {
    PALF_LOG(TRACE, "LogPreareMeta serialize", K(*this), K(buf + pos), KP(buf), K(pos), K(new_pos));
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogPrepareMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf || data_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (data_len - new_pos < get_serialize_size()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &version_)) ||
             OB_FAIL(voted_for_.deserialize(buf, data_len, new_pos)) ||
             OB_FAIL(
                 serialization::decode_i64(buf, data_len, new_pos, reinterpret_cast<int64_t *>(&log_proposal_id_)))) {
    PALF_LOG(ERROR, "LogPrepareMeta deserialize failed", K(ret), K(new_pos));
  } else {
    PALF_LOG(TRACE, "LogPreareMeta deserialize", K(*this), K(buf + pos), KP(buf), K(pos), K(new_pos));
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogPrepareMeta)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(version_);
  size += voted_for_.get_serialize_size();
  size += serialization::encoded_length_i64(log_proposal_id_);
  return size;
}

LogConfigVersion::LogConfigVersion() : proposal_id_(INVALID_PROPOSAL_ID), config_seq_(-1)
{}

LogConfigVersion::~LogConfigVersion()
{
  reset();
}

void LogConfigVersion::reset()
{
  proposal_id_ = INVALID_PROPOSAL_ID;
  config_seq_ = -1;
}

bool LogConfigVersion::is_valid() const
{
  return proposal_id_ != INVALID_PROPOSAL_ID && config_seq_ >= 0;
}

int LogConfigVersion::generate(const int64_t proposal_id, const int64_t config_seq)
{
  int ret = OB_SUCCESS;
  if (proposal_id == INVALID_PROPOSAL_ID || config_seq < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    proposal_id_ = proposal_id;
    config_seq_ = config_seq;
  }
  return ret;
}

int LogConfigVersion::inc_update_version(const int64_t proposal_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(INVALID_PROPOSAL_ID == proposal_id_ || config_seq_ < 0)) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogConfigVersion not init", K(ret), K(proposal_id_), K(config_seq_));
  } else if (OB_UNLIKELY(INVALID_PROPOSAL_ID == proposal_id || proposal_id < proposal_id_)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(proposal_id), K(proposal_id_));
  } else {
    proposal_id_ = proposal_id;
    config_seq_ += 1;
  }
  return ret;
}

void LogConfigVersion::operator=(const LogConfigVersion &config_version)
{
  proposal_id_ = config_version.proposal_id_;
  config_seq_ = config_version.config_seq_;
}

bool LogConfigVersion::operator==(const LogConfigVersion &config_version) const
{
  return proposal_id_ == config_version.proposal_id_ && config_seq_ == config_version.config_seq_;
}

bool LogConfigVersion::operator!=(const LogConfigVersion &config_version) const
{
  return !(*this == config_version);
}

bool LogConfigVersion::operator>(const LogConfigVersion &config_version) const
{
  const bool self_valid = is_valid();
  const bool arg_valid = config_version.is_valid();
  bool bool_ret = false;
  const bool cmp_ret = (proposal_id_ > config_version.proposal_id_ ) ||
      (proposal_id_ == config_version.proposal_id_ && config_seq_ > config_version.config_seq_);
  if (OB_LIKELY(self_valid && arg_valid)) {
    bool_ret = cmp_ret;
  } else if (!self_valid) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool LogConfigVersion::operator<(const LogConfigVersion &config_version) const
{
  const bool self_valid = is_valid();
  const bool arg_valid = config_version.is_valid();
  bool bool_ret = false;
  const bool cmp_ret = (proposal_id_ < config_version.proposal_id_) ||
      (proposal_id_ == config_version.proposal_id_ && config_seq_ < config_version.config_seq_);
  if (OB_LIKELY(self_valid && arg_valid)) {
    bool_ret = cmp_ret;
  } else if (!arg_valid) {
    bool_ret = false;
  } else {
    bool_ret = true;
  }
  return bool_ret;
}

bool LogConfigVersion::operator>=(const LogConfigVersion &config_version) const
{
  return (*this > config_version) || (*this == config_version);
}

bool LogConfigVersion::operator<=(const LogConfigVersion &config_version) const
{
  return (*this < config_version) || (*this == config_version);
}

int64_t LogConfigVersion::to_string(char *buf, const int64_t buf_len)
{
  int64_t pos = 0;
  if (OB_SUCCESS != databuff_print_obj(buf, buf_len, pos, proposal_id_)) {
    PALF_LOG_RET(WARN, OB_ERR_UNEXPECTED, "databuff_print_obj failed", K(pos));
  } else if (OB_SUCCESS != databuff_print_obj(buf, buf_len, pos, config_seq_)) {
    PALF_LOG_RET(WARN, OB_ERR_UNEXPECTED, "databuff_print_obj failed", K(pos));
  } else {
  }
  return pos;
}

DEFINE_SERIALIZE(LogConfigVersion)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, proposal_id_)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, config_seq_))) {
    PALF_LOG(ERROR, "LogConfigVersion serialize failed", K(ret), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogConfigVersion)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &proposal_id_)) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &config_seq_))) {
    PALF_LOG(ERROR, "LogConfigVersion deserialize failed", K(ret), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogConfigVersion)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(proposal_id_);
  size += serialization::encoded_length_i64(config_seq_);
  return size;
}

LogConfigInfo::LogConfigInfo()
    : log_sync_memberlist_(),
      log_sync_replica_num_(0),
      arbitration_member_(),
      learnerlist_(),
      degraded_learnerlist_(),
      config_version_()
{}

LogConfigInfo::~LogConfigInfo()
{
  reset();
}

bool LogConfigInfo::is_valid() const
{
  const bool is_arb_in_log_sync = log_sync_memberlist_.contains(arbitration_member_.get_server());
  return true == log_sync_memberlist_.is_valid() &&
         false == is_arb_in_log_sync &&
         0 < log_sync_replica_num_ &&
         common::OB_MAX_MEMBER_NUMBER >= log_sync_replica_num_ &&
         config_version_.is_valid();
}

void LogConfigInfo::reset()
{
  log_sync_memberlist_.reset();
  log_sync_replica_num_ = 0;
  arbitration_member_.reset();
  learnerlist_.reset();
  degraded_learnerlist_.reset();
  config_version_.reset();
}

int LogConfigInfo::generate(const ObMemberList &memberlist,
                            const int64_t replica_num,
                            const common::GlobalLearnerList &learnerlist,
                            const LogConfigVersion &config_version)
{
  int ret = OB_SUCCESS;
  if (false == memberlist.is_valid() || false == config_version.is_valid() ||
      0 >= replica_num || OB_MAX_MEMBER_NUMBER < replica_num) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    log_sync_memberlist_ = memberlist;
    log_sync_replica_num_ = replica_num;
    learnerlist_ = learnerlist;
    config_version_ = config_version;
  }
  return ret;
}

int LogConfigInfo::get_expected_paxos_memberlist(common::ObMemberList &paxos_memberlist,
                                                 int64_t &paxos_replica_num) const
{
  int ret = OB_SUCCESS;
  if (false == log_sync_memberlist_.is_valid() ||
      0 >= log_sync_replica_num_ ||
      common::OB_MAX_MEMBER_NUMBER < log_sync_replica_num_) {
    // memberlist may be empty when bootstraping cluster, just return empty memberlist
    paxos_memberlist.reset();
    paxos_replica_num = 0;
  } else if (OB_UNLIKELY(degraded_learnerlist_.is_valid())) {
    paxos_memberlist = log_sync_memberlist_;
    paxos_replica_num = log_sync_replica_num_;
    common::ObMember tmp_member;
    const int64_t degraded_count = degraded_learnerlist_.get_member_number();
    for (int64_t i = 0; i < degraded_count && OB_SUCC(ret); ++i) {
      if (OB_FAIL(degraded_learnerlist_.get_member_by_index(i, tmp_member))) {
        PALF_LOG(WARN, "get_member_by_index failed", KR(ret), K(i), K(degraded_learnerlist_));
      } else if (OB_FAIL(paxos_memberlist.add_member(tmp_member))) {
        PALF_LOG(WARN, "add_member failed", KR(ret), K(paxos_memberlist), K(tmp_member));
      } else {
        paxos_replica_num++;
      }
    }
  } else {
    paxos_memberlist = log_sync_memberlist_;
    paxos_replica_num = log_sync_replica_num_;
  }
  return ret;
}

// generate paxos memberlist including arbitration replica
int LogConfigInfo::convert_to_complete_config(common::ObMemberList &alive_paxos_memberlist,
                                              int64_t &alive_paxos_replica_num,
                                              GlobalLearnerList &all_learners) const
{
  int ret = OB_SUCCESS;
  if (false == log_sync_memberlist_.is_valid() ||
      0 >= log_sync_replica_num_ ||
      common::OB_MAX_MEMBER_NUMBER < log_sync_replica_num_) {
    ret = OB_NOT_INIT;
    PALF_LOG(WARN, "LogConfigInfo not init", KR(ret), K_(log_sync_memberlist), K_(log_sync_replica_num));
  } else if (OB_FAIL(all_learners.deep_copy(learnerlist_))) {
  } else if (OB_FAIL(all_learners.append(degraded_learnerlist_))) {
  } else if (OB_UNLIKELY(true == arbitration_member_.is_valid())) {
    alive_paxos_memberlist = log_sync_memberlist_;
    if (OB_FAIL(alive_paxos_memberlist.add_member(arbitration_member_))) {
      PALF_LOG(WARN, "add_member failed", KR(ret), K(alive_paxos_memberlist), K(arbitration_member_));
    } else {
      alive_paxos_replica_num = log_sync_replica_num_ + 1;
    }
  } else {
    alive_paxos_memberlist = log_sync_memberlist_;
    alive_paxos_replica_num = log_sync_replica_num_;
  }
  return ret;
}

void LogConfigInfo::operator=(const LogConfigInfo &config_info)
{
  log_sync_memberlist_ = config_info.log_sync_memberlist_;
  log_sync_replica_num_ = config_info.log_sync_replica_num_;
  arbitration_member_ = config_info.arbitration_member_;
  learnerlist_.deep_copy(config_info.learnerlist_);
  degraded_learnerlist_.deep_copy(config_info.degraded_learnerlist_);
  config_version_ = config_info.config_version_;
  PALF_LOG(TRACE, "LogConfigInfo operator =", KPC(this), K(config_info));
}

bool LogConfigInfo::operator==(const LogConfigInfo &config_info) const
{
  return true == log_sync_memberlist_.member_addr_equal(config_info.log_sync_memberlist_) &&
         log_sync_replica_num_ == config_info.log_sync_replica_num_ &&
         true == learnerlist_.learner_addr_equal(config_info.learnerlist_) &&
         true == degraded_learnerlist_.learner_addr_equal(config_info.degraded_learnerlist_) &&
         config_version_ == config_info.config_version_;
}

DEFINE_SERIALIZE(LogConfigInfo)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(log_sync_memberlist_.serialize(buf, buf_len, new_pos)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, log_sync_replica_num_)) ||
             OB_FAIL(arbitration_member_.serialize(buf, buf_len, new_pos)) ||
             OB_FAIL(learnerlist_.serialize(buf, buf_len, new_pos)) ||
             OB_FAIL(degraded_learnerlist_.serialize(buf, buf_len, new_pos)) ||
             OB_FAIL(config_version_.serialize(buf, buf_len, new_pos))) {
    PALF_LOG(ERROR, "LogConfigInfo serialize failed", K(ret), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogConfigInfo)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(log_sync_memberlist_.deserialize(buf, data_len, new_pos)) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &log_sync_replica_num_)) ||
             OB_FAIL(arbitration_member_.deserialize(buf, data_len, new_pos)) ||
             OB_FAIL(learnerlist_.deserialize(buf, data_len, new_pos)) ||
             OB_FAIL(degraded_learnerlist_.deserialize(buf, data_len, new_pos)) ||
             OB_FAIL(config_version_.deserialize(buf, data_len, new_pos))) {
    PALF_LOG(ERROR, "LogConfigInfo deserialize failed", K(ret), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogConfigInfo)
{
  int64_t size = 0;
  size += log_sync_memberlist_.get_serialize_size();
  size += serialization::encoded_length_i64(log_sync_replica_num_);
  size += arbitration_member_.get_serialize_size();
  size += learnerlist_.get_serialize_size();
  size += degraded_learnerlist_.get_serialize_size();
  size += config_version_.get_serialize_size();
  return size;
}

LogConfigMeta::LogConfigMeta()
  : version_(-1),
    proposal_id_(INVALID_PROPOSAL_ID),
    prev_(),
    curr_(),
    prev_log_proposal_id_(INVALID_PROPOSAL_ID),
    prev_lsn_(),
    prev_mode_pid_(INVALID_PROPOSAL_ID)
{}

LogConfigMeta::~LogConfigMeta()
{
  reset();
}

int LogConfigMeta::generate_for_default(
    const int64_t proposal_id,
    const LogConfigInfo &prev_config_info,
    const LogConfigInfo &curr_config_info)
{
  int ret = OB_SUCCESS;
  if (INVALID_PROPOSAL_ID == proposal_id) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    // Note: we generate a default META_VERSION rather than according to DATA_VERSION,
    //       because DATA_VERSION of the tenant may be empty it the server is just added
    //       to the cluster. It's fine because real LogConfigMeta will be set by the
    //       set_initial_member_list interface.
    version_ = LOG_CONFIG_META_VERSION_INC;
    proposal_id_ = proposal_id;
    prev_ = prev_config_info;
    curr_ = curr_config_info;
  }
  return ret;
}

int LogConfigMeta::generate(
    const int64_t proposal_id,
    const LogConfigInfo &prev_config_info,
    const LogConfigInfo &curr_config_info,
    const int64_t prev_log_proposal_id,
    const LSN &prev_lsn,
    const int64_t prev_mode_pid)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  if (INVALID_PROPOSAL_ID == proposal_id || false == curr_config_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), tenant_data_version))) {
    PALF_LOG(WARN, "get tenant data version failed", K(ret));
  } else {
    const bool is_cluster_already_4100 = (tenant_data_version >= DATA_VERSION_4_1_0_0);
    version_ = (is_cluster_already_4100)? LOG_CONFIG_META_VERSION_INC: LOG_CONFIG_META_VERSION;
    proposal_id_ = proposal_id;
    prev_ = prev_config_info;
    curr_ = curr_config_info;
    if (is_cluster_already_4100) {
      prev_log_proposal_id_ = prev_log_proposal_id;
      prev_lsn_ = prev_lsn;
      prev_mode_pid_ = prev_mode_pid;
    }
  }
  return ret;
}

bool LogConfigMeta::is_valid() const
{
  // NB: prev_config_info is invalid before change config
  return (LOG_CONFIG_META_VERSION == version_ || LOG_CONFIG_META_VERSION_INC == version_)
      && proposal_id_ != INVALID_PROPOSAL_ID;
}

void LogConfigMeta::reset()
{
  proposal_id_ = INVALID_PROPOSAL_ID;
  curr_.reset();
  prev_.reset();
  version_ = -1;
  prev_log_proposal_id_ = INVALID_PROPOSAL_ID;
  prev_lsn_.reset();
  prev_mode_pid_ = INVALID_PROPOSAL_ID;
}

void LogConfigMeta::operator=(const LogConfigMeta &log_config_meta)
{
  this->version_ = log_config_meta.version_;
  this->proposal_id_ = log_config_meta.proposal_id_;
  this->prev_ = log_config_meta.prev_;
  this->curr_ = log_config_meta.curr_;
  this->prev_log_proposal_id_ = log_config_meta.prev_log_proposal_id_;
  this->prev_lsn_ = log_config_meta.prev_lsn_;
  this->prev_mode_pid_ = log_config_meta.prev_mode_pid_;
}

DEFINE_SERIALIZE(LogConfigMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (buf_len - new_pos < get_serialize_size()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, version_)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, proposal_id_)) ||
             OB_FAIL(prev_.serialize(buf, buf_len, new_pos)) || OB_FAIL(curr_.serialize(buf, buf_len, new_pos))) {
    PALF_LOG(ERROR, "LogConfigMeta serialize failed", K(ret), K(new_pos));
  } else if (LOG_CONFIG_META_VERSION_INC == version_) {
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, prev_log_proposal_id_)) ||
        OB_FAIL(prev_lsn_.serialize(buf, buf_len, new_pos)) ||
        OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, prev_mode_pid_))) {
      PALF_LOG(ERROR, "LogConfigMeta Version 2 serialize failed", K(ret), K(new_pos));
    } else {
      PALF_LOG(TRACE, "LogConfigMeta Version 2 serialize", K(*this), K(buf + pos), KP(buf), K(pos), K(new_pos));
      pos = new_pos;
    }
  } else {
    PALF_LOG(TRACE, "LogConfigMeta serialize", K(*this), K(buf + pos), KP(buf), K(pos), K(new_pos));
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogConfigMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= data_len) {
    ret = OB_INVALID_ARGUMENT;
    //  TODO: ObAddr's serialized size is variable, replace it later.
    //  } else if (data_len - new_pos < get_serialize_size()) {
    //    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &version_)) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &proposal_id_)) ||
             OB_FAIL(prev_.deserialize(buf, data_len, new_pos)) || OB_FAIL(curr_.deserialize(buf, data_len, new_pos))) {
    PALF_LOG(ERROR, "LogConfigMeta deserialize failed", K(ret), K(new_pos));
  } else if (LOG_CONFIG_META_VERSION_INC == version_) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &prev_log_proposal_id_)) ||
        OB_FAIL(prev_lsn_.deserialize(buf, data_len, new_pos)) ||
        OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &prev_mode_pid_))) {
      PALF_LOG(ERROR, "LogConfigMeta Version 2 deserialize failed", K(ret), K(new_pos));
    } else {
      PALF_LOG(TRACE, "LogConfigMeta Version 2 deserialize", K(*this), K(buf + pos), KP(buf), K(pos), K(new_pos));
      pos = new_pos;
    }
  } else {
    PALF_LOG(TRACE, "LogConfigMeta deserialize", K(*this), K(buf + pos), KP(buf), K(pos), K(new_pos));
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogConfigMeta)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(version_);
  size += serialization::encoded_length_i64(proposal_id_);
  size += prev_.get_serialize_size();
  size += curr_.get_serialize_size();
  if (LOG_CONFIG_META_VERSION_INC == version_) {
    size += serialization::encoded_length_i64(prev_log_proposal_id_);
    size += prev_lsn_.get_serialize_size();
    size += serialization::encoded_length_i64(prev_mode_pid_);
  }
  return size;
}

LogModeMeta::LogModeMeta()
    : version_(-1),
      proposal_id_(INVALID_PROPOSAL_ID),
      mode_version_(INVALID_PROPOSAL_ID),
      access_mode_(AccessMode::INVALID_ACCESS_MODE),
      ref_scn_()
{}

LogModeMeta::~LogModeMeta()
{
  reset();
}

int LogModeMeta::generate(const int64_t proposal_id,
                          const int64_t mode_version,
                          const AccessMode &access_mode,
                          const SCN &ref_scn)
{
  int ret = OB_SUCCESS;
  if (INVALID_PROPOSAL_ID == mode_version ||
      INVALID_PROPOSAL_ID == proposal_id ||
      false == is_valid_access_mode(access_mode) ||
      !ref_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    version_ = LOG_MODE_META_VERSION;
    proposal_id_ = proposal_id;
    mode_version_ = mode_version;
    access_mode_ = access_mode;
    ref_scn_ = ref_scn;
  }
  return ret;
}

bool LogModeMeta::is_valid() const
{
  return LOG_MODE_META_VERSION == version_ &&
         INVALID_PROPOSAL_ID != proposal_id_ &&
         INVALID_PROPOSAL_ID != mode_version_ &&
         is_valid_access_mode(access_mode_) &&
         ref_scn_.is_valid();
}

void LogModeMeta::reset()
{
  version_ = -1;
  proposal_id_ = INVALID_PROPOSAL_ID;
  mode_version_ = INVALID_PROPOSAL_ID;
  access_mode_ = AccessMode::INVALID_ACCESS_MODE;
  ref_scn_.reset();
}

void LogModeMeta::operator=(const LogModeMeta &mode_meta)
{
  this->version_ = mode_meta.version_;
  this->proposal_id_ = mode_meta.proposal_id_;
  this->mode_version_ = mode_meta.mode_version_;
  this->access_mode_ = mode_meta.access_mode_;
  this->ref_scn_ = mode_meta.ref_scn_;
}

DEFINE_SERIALIZE(LogModeMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (buf_len - new_pos < get_serialize_size()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, version_)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, proposal_id_)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, mode_version_)) ||
             OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, static_cast<int64_t>(access_mode_))) ||
             OB_FAIL(ref_scn_.fixed_serialize(buf, buf_len, new_pos))) {
    PALF_LOG(ERROR, "LogModeMeta serialize failed", K(ret), K(new_pos));
  } else {
    PALF_LOG(TRACE, "LogModeMeta serialize", K(*this), K(buf + pos), KP(buf), K(pos), K(new_pos));
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogModeMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || 0 >= data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &version_)) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &proposal_id_)) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &mode_version_)) ||
             OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, reinterpret_cast<int64_t *>(&access_mode_))) ||
             OB_FAIL(ref_scn_.fixed_deserialize(buf, data_len, new_pos))) {
    PALF_LOG(ERROR, "LogModeMeta deserialize failed", K(ret), K(new_pos));
  } else {
    PALF_LOG(TRACE, "LogModeMeta deserialize", K(*this), K(buf + pos), KP(buf), K(pos), K(new_pos));
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogModeMeta)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(version_);
  size += serialization::encoded_length_i64(proposal_id_);
  size += serialization::encoded_length_i64(mode_version_);
  size += serialization::encoded_length_i64(static_cast<int64_t>(access_mode_));
  size += ref_scn_.get_fixed_serialize_size();
  return size;
}

LogSnapshotMeta::LogSnapshotMeta() : version_(-1), base_lsn_(), prev_log_info_()
{}

LogSnapshotMeta::~LogSnapshotMeta()
{
  reset();
}

int LogSnapshotMeta::generate(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  if (false == lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    version_ = LOG_SNAPSHOT_META_VERSION;
    base_lsn_ = lsn;
    prev_log_info_.reset();
  }
  return ret;
}

int LogSnapshotMeta::generate(const LSN &lsn, const LogInfo &prev_log_info)
{
  int ret = OB_SUCCESS;
  if (false == lsn.is_valid() || false == prev_log_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    version_ = LOG_SNAPSHOT_META_VERSION;
    base_lsn_ = lsn;
    prev_log_info_ = prev_log_info;
  }
  return ret;
}

bool LogSnapshotMeta::is_valid() const
{
  return true == base_lsn_.is_valid() && LOG_SNAPSHOT_META_VERSION == version_;
}

void LogSnapshotMeta::reset()
{
  base_lsn_.reset();
  prev_log_info_.reset();
  version_ = -1;
}

int LogSnapshotMeta::get_prev_log_info(LogInfo &log_info) const
{
  int ret = OB_SUCCESS;
  log_info.reset();
  if (!prev_log_info_.is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    log_info = prev_log_info_;
  }
  return ret;
}

void LogSnapshotMeta::operator=(const LogSnapshotMeta &log_snapshot_meta)
{
  this->version_ = log_snapshot_meta.version_;
  this->base_lsn_ = log_snapshot_meta.base_lsn_;
  this->prev_log_info_ = log_snapshot_meta.prev_log_info_;
}

DEFINE_SERIALIZE(LogSnapshotMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (buf_len - new_pos < get_serialize_size()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, version_)) ||
             OB_FAIL(base_lsn_.serialize(buf, buf_len, new_pos)) ||
             OB_FAIL(prev_log_info_.serialize(buf, buf_len, new_pos))) {
    PALF_LOG(ERROR, "LogSnapshotMeta serialize failed", K(ret), K(new_pos));
  } else {
    PALF_LOG(TRACE, "LogSnapshotMeta serialize", K(*this), K(buf + pos), KP(buf), K(pos), K(new_pos));
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogSnapshotMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (data_len - new_pos < get_serialize_size()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &version_)) ||
             OB_FAIL(base_lsn_.deserialize(buf, data_len, new_pos)) ||
             OB_FAIL(prev_log_info_.deserialize(buf, data_len, new_pos))) {
    PALF_LOG(ERROR, "LogSnapshotMeta deserialize failed", K(ret), K(new_pos));
  } else {
    PALF_LOG(TRACE, "LogSnapshotMeta deserialize", K(*this), K(buf + pos), KP(buf), K(pos), K(new_pos));
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(LogSnapshotMeta)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(version_);
  size += base_lsn_.get_serialize_size();
  size += prev_log_info_.get_serialize_size();
  return size;
}

int LogReplicaPropertyMeta::generate(const bool allow_vote, const LogReplicaType replica_type)
{
  int ret = OB_SUCCESS;
  version_ = LOG_REPLICA_PROPERTY_META_VERSION;
  allow_vote_ = allow_vote;
  replica_type_ = replica_type;
  return ret;
}

bool LogReplicaPropertyMeta::is_valid() const
{
  return LOG_REPLICA_PROPERTY_META_VERSION == version_;
}

void LogReplicaPropertyMeta::reset()
{
  allow_vote_ = false;
  replica_type_ = LogReplicaType::INVALID_REPLICA;
  version_ = -1;
}

void LogReplicaPropertyMeta::operator=(const LogReplicaPropertyMeta &replica_meta)
{
  this->version_ = replica_meta.version_;
  this->allow_vote_ = replica_meta.allow_vote_;
  this->replica_type_ = replica_meta.replica_type_;
}

DEFINE_SERIALIZE(LogReplicaPropertyMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (buf_len - new_pos < get_serialize_size()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, version_))
            || OB_FAIL(serialization::encode_bool(buf, buf_len, new_pos, allow_vote_))
            || OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, replica_type_))) {
    PALF_LOG(ERROR, "LogReplicaPropertyMeta serialize failed", K(ret), K(new_pos));
  } else {
    PALF_LOG(TRACE, "LogReplicaPropertyMeta serialize", K(*this), K(buf+pos), KP(buf), K(pos), K(new_pos));
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(LogReplicaPropertyMeta)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (data_len - new_pos < get_serialize_size()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &version_))
            || OB_FAIL(serialization::decode_bool(buf, data_len, new_pos, &allow_vote_))
            || OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, reinterpret_cast<int32_t*>(&replica_type_)))) {
    PALF_LOG(ERROR, "LogReplicaPropertyMeta deserialize failed", K(ret), K(new_pos));
  } else {
    PALF_LOG(TRACE, "LogReplicaPropertyMeta deserialize", K(*this), K(buf+pos), KP(buf), K(pos), K(new_pos));
    pos = new_pos;
  }
  return ret;
}


DEFINE_GET_SERIALIZE_SIZE(LogReplicaPropertyMeta)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(version_);
  size += serialization::encoded_length_bool(allow_vote_);
  size += serialization::encoded_length_i32(replica_type_);
  return size;
}
} // end namespace palf
} // end namespace oceanbase
