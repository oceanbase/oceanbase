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

#include "ob_election_msg_pool.h"
#include "ob_election_group.h"
#include "ob_election.h"
#include "ob_election_info.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::election;

/***********************[define for ObElectionVoteMsgPool::MsgRecorder]*******************/

void ObElectionVoteMsgPool::MsgRecorder::reset()
{
  cur_idx_ = 0;
  record_T1_timestamp_ = OB_INVALID_TIMESTAMP;
  new_leader_.reset();
}

// @param [in] T1_timestamp election's T1
// @return void
void ObElectionVoteMsgPool::MsgRecorder::correct_recordT1_if_necessary(int64_t T1_timestamp)
{
  if (record_T1_timestamp_ > T1_timestamp) {
    void* p_this = this;
    ELECT_ASYNC_LOG(
        WARN, "record t1 is corrected", KP(p_this), K_(partition), K_(record_T1_timestamp), K(T1_timestamp));
    reset();
    record_T1_timestamp_ = T1_timestamp;
  } else {
    // do nothing
  }
}

// record messages, check if someone reach majority
// @param [in] msg message for recording
// @return error code
int ObElectionVoteMsgPool::MsgRecorder::record_msg(const ObElectionVoteMsg& msg)
{
  int ret = OB_SUCCESS;
  int64_t msg_T1 = msg.get_T1_timestamp();
  int64_t election_current_ts = 0;
  int64_t election_t1 = 0;

  if (OB_UNLIKELY(nullptr == election_)) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(ERROR,
        "election_ is NULL, probably not inited",
        K_(partition),
        K(msg),
        K_(record_T1_timestamp),
        KP_(election),
        K(ret));
  } else if (OB_UNLIKELY(msg_T1 > record_T1_timestamp_)) {
    reset();
    record_T1_timestamp_ = msg_T1;
  } else if (OB_UNLIKELY(msg_T1 < record_T1_timestamp_)) {
    ret = OB_ELECTION_WARN_MESSAGE_NOT_INTIME;
    ELECT_ASYNC_LOG(WARN, "msg is too old", K_(partition), K(msg), K_(record_T1_timestamp), K(ret));
  } else {
    // ok,continue
  }

  if (OB_LIKELY(OB_SUCCESS == ret)) {
    if (msg.get_msg_type() == ObElectionMsgType::OB_ELECTION_VOTE_VOTE) {
      const ObElectionMsgVote& casted_msg = static_cast<const ObElectionMsgVote&>(msg);
      if (OB_UNLIKELY(!new_leader_.is_valid())) {
        new_leader_ = casted_msg.get_new_leader();
      } else if (OB_UNLIKELY(casted_msg.get_new_leader() != new_leader_)) {
        ELECT_ASYNC_LOG(
            WARN, "two different new leader whith same T1 timestamp", K_(partition), K(msg), K_(new_leader), K(ret));
        reset();
        record_T1_timestamp_ = msg_T1;
        new_leader_ = casted_msg.get_new_leader();
      } else {
        // do nothing, continue
      }
    }

    if (OB_LIKELY(ret == OB_SUCCESS)) {
      if (OB_UNLIKELY(cur_idx_ >= OB_MAX_ELECTION_MSG_COUNT)) {
        // do nothing
      } else {
        const common::ObAddr& sender = msg.get_sender();
        for (int i = 0; i < cur_idx_; ++i) {
          if (OB_UNLIKELY(record_member_[i] == sender)) {
            ret = OB_ELECTION_ERROR_DUPLICATED_MSG;
            ELECT_ASYNC_LOG(WARN, "msg is duplicated", K_(partition), K(msg), K(sender), K(ret));
            break;
          }
        }

        if (OB_LIKELY(ret == OB_SUCCESS)) {
          record_member_[cur_idx_++] = sender;
        }
      }
    }
  }
  return ret;
}

bool ObElectionVoteMsgPool::MsgRecorder::reach_majority(int replica_num) const
{
  return cur_idx_ >= (replica_num / 2 + 1);
}

int64_t ObElectionVoteMsgPool::MsgRecorder::get_record_T1_timestamp() const
{
  return record_T1_timestamp_;
}

int ObElectionVoteMsgPool::MsgRecorder::size() const
{
  return cur_idx_;
}

const ObAddr& ObElectionVoteMsgPool::MsgRecorder::get_new_leader() const
{
  return new_leader_;
}

/***********************[define for ObElectionVoteMsgPool]*******************/

int ObElectionVoteMsgPool::init(const ObAddr& self, ObIElection* election)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    ELECT_ASYNC_LOG(ERROR, "init twice", KP(this), K_(partition));
  } else if (!self.is_valid() || nullptr == election) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(ERROR, "init argument is not valid", K_(partition), K(self), K(election), KP(this));
  } else {
    self_ = self;
    election_ = election;
    ObElectionInfo election_info;
    int temp_ret;
    if (OB_SUCCESS != (temp_ret = election->get_election_info(election_info))) {
      partition_ = election_info.get_partition();
      ELECT_ASYNC_LOG(ERROR, "get election info failed", K(self), K(election), KP(this), K_(partition));
    } else {
      partition_ = election_info.get_partition();
    }
    is_inited_ = true;
    void* p_vote_recorder = &vote_recorder_;
    void* p_devote_recorder = &devote_recorder_;
    void* p_deprepare_recorder = &deprepare_recorder_;
    ELECT_ASYNC_LOG(INFO,
        "init success",
        KP(p_vote_recorder),
        KP(p_devote_recorder),
        KP(p_deprepare_recorder),
        K_(partition),
        K(self),
        K(election),
        KP(this));
  }

  return ret;
}

void ObElectionVoteMsgPool::reset()
{
  deprepare_recorder_.reset();
  devote_recorder_.reset();
  vote_recorder_.reset();
  cached_devote_prepare_msg_.reset();
  cached_vote_prepare_msg_.reset();
  cached_new_leader_vote_msg_.reset();
  self_.reset();
  partition_.reset();
  election_ = nullptr;
  is_inited_ = false;
}

// @param [in] T1_timestamp election's current T1
// @return void
void ObElectionVoteMsgPool::correct_recordT1_if_necessary(int64_t T1_timestamp)
{
  deprepare_recorder_.correct_recordT1_if_necessary(T1_timestamp);
  devote_recorder_.correct_recordT1_if_necessary(T1_timestamp);
  vote_recorder_.correct_recordT1_if_necessary(T1_timestamp);
}

// record messages, in [decentralized prepare/decentralizes voting/centralized prepare/centralized voting]
// @param [in] msg messgae for recording
// @return error code
int ObElectionVoteMsgPool::store(const ObElectionVoteMsg& msg)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(ERROR, "not init yet", K_(partition), KP(this));
  } else if (OB_UNLIKELY(!msg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(ERROR, "invalid argument", K_(partition), K(msg), K(ret));
  } else {
    const int msg_type = msg.get_msg_type();
    switch (msg_type) {
      case ObElectionMsgType::OB_ELECTION_DEVOTE_PREPARE: {
        const ObElectionMsgDEPrepare& casted_msg = static_cast<const ObElectionMsgDEPrepare&>(msg);
        if (OB_UNLIKELY(!casted_msg.get_priority().is_candidate())) {
          ret = OB_ELECTION_WARN_NOT_CANDIDATE;
          ELECT_ASYNC_LOG(ERROR, "deprepare msg sender is not candidiate", K_(partition), K(msg), K(ret));
        } else if (OB_UNLIKELY(OB_FAIL(deprepare_recorder_.record_msg(msg)))) {
          ELECT_ASYNC_LOG(WARN, "fail to store election msg", K_(partition), K(msg), K(ret));
        } else if (OB_UNLIKELY(casted_msg.get_T1_timestamp() > cached_devote_prepare_msg_.get_T1_timestamp())) {
          cached_devote_prepare_msg_ = casted_msg;
          if (REACH_TIME_INTERVAL(1000 * 1000)) {
            ELECT_ASYNC_LOG(
                DEBUG, "local cached DEVOTE msg is too old, replaced by new one", K_(partition), K(msg), K(ret));
          }
        } else if (OB_LIKELY(casted_msg.get_T1_timestamp() == cached_devote_prepare_msg_.get_T1_timestamp())) {
          if (casted_msg.get_priority().compare_with_accurate_logid(cached_devote_prepare_msg_.get_priority()) > 0 ||
              (casted_msg.get_priority().compare_with_accurate_logid(cached_devote_prepare_msg_.get_priority()) == 0 &&
                  casted_msg.get_sender() > cached_devote_prepare_msg_.get_sender())) {
            cached_devote_prepare_msg_ = casted_msg;
          } else {
            // do nothing
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          ELECT_ASYNC_LOG(ERROR,
              "depaepare_msg's T1_timestamp is smaller than cached_devote_prepare_msg_, but recorded success",
              K(msg),
              K_(cached_devote_prepare_msg),
              K(ret),
              K_(partition));
        }
        break;
      }
      case ObElectionMsgType::OB_ELECTION_DEVOTE_VOTE: {
        if (OB_UNLIKELY(static_cast<const ObElectionMsgDEVote&>(msg).get_vote_leader() != self_)) {
          ret = OB_ERR_UNEXPECTED;
          ELECT_ASYNC_LOG(ERROR, "receive devote msg vote for others", K_(partition), K(msg), K_(self), K(ret));
        } else if (OB_UNLIKELY(OB_FAIL(devote_recorder_.record_msg(msg)))) {
          ELECT_ASYNC_LOG(WARN, "fail to store election msg", K_(partition), K(msg), K(ret));
        } else {
          // do nothing
        }
        break;
      }
      case ObElectionMsgType::OB_ELECTION_VOTE_PREPARE: {
        const ObElectionMsgPrepare& prepare_msg = static_cast<const ObElectionMsgPrepare&>(msg);
        if (OB_LIKELY(prepare_msg.get_T1_timestamp() > cached_vote_prepare_msg_.get_T1_timestamp())) {
          cached_vote_prepare_msg_ = prepare_msg;
        } else if (prepare_msg.get_T1_timestamp() == cached_vote_prepare_msg_.get_T1_timestamp()) {
          ret = OB_ELECTION_ERROR_DUPLICATED_MSG;
          ELECT_ASYNC_LOG(ERROR,
              "receive vote prepare msg with same T1_timestamp more than one time",
              K_(cached_vote_prepare_msg),
              K_(self),
              K(ret),
              K_(partition));
        }
        break;
      }
      case ObElectionMsgType::OB_ELECTION_VOTE_VOTE: {
        const ObElectionMsgVote& casted_msg = static_cast<const ObElectionMsgVote&>(msg);
        if (OB_UNLIKELY(casted_msg.get_cur_leader() != self_)) {
          ret = OB_ERR_UNEXPECTED;
          ELECT_ASYNC_LOG(ERROR, "receive devote msg vote for others", K_(partition), K(msg), K_(self), K(ret));
        } else if (OB_UNLIKELY(OB_FAIL(vote_recorder_.record_msg(msg)))) {
          ELECT_ASYNC_LOG(WARN, "record msg failed", K_(partition), K(msg), K_(self), K(ret));
        } else if (OB_UNLIKELY(casted_msg.get_new_leader() != casted_msg.get_cur_leader() &&
                               casted_msg.get_sender() == casted_msg.get_new_leader() &&
                               casted_msg.get_T1_timestamp() > cached_new_leader_vote_msg_.get_T1_timestamp())) {
          cached_new_leader_vote_msg_ = casted_msg;
        } else {
          // do nothing
        }
        break;
      }
      default:  // reject
      {
        ret = OB_ERR_UNEXPECTED;
        ELECT_ASYNC_LOG(ERROR, "unexpected msg type", K_(partition), K(msg), K(ret));
        break;
      }
    }
  }

  return ret;
}

// get candidate need vote for, in gt2_task
// @param [out] server server vote for, has highest priority
// @param [out] priority priority of that server
// @param [in] replica_num
// @param [in] t1
// @param [out] lease_time
// @return error code
int ObElectionVoteMsgPool::get_decentralized_candidate(
    ObAddr& server, ObElectionPriority& priority, const int64_t replica_num, const int64_t t1, int64_t& lease_time)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(ERROR, "not init yet", K_(partition), KP(this));
  } else if (OB_UNLIKELY(0 >= replica_num || 0 >= t1)) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(ERROR, "invalid argument", K_(partition), K(replica_num), K(t1));
  } else if (OB_UNLIKELY(t1 != deprepare_recorder_.get_record_T1_timestamp())) {
    ret = OB_ELECTION_WARN_T1_NOT_MATCH;
    ELECT_ASYNC_LOG(
        WARN, "record member t1 timestamp not match", K_(partition), K(t1), K_(cached_devote_prepare_msg), K(ret));
  } else if (OB_UNLIKELY(t1 != cached_devote_prepare_msg_.get_T1_timestamp())) {
    ret = OB_ELECTION_WARN_T1_NOT_MATCH;
    ELECT_ASYNC_LOG(WARN,
        "cached devote prepare message t1 timestamp not match",
        K_(partition),
        K(t1),
        K_(cached_devote_prepare_msg),
        K(ret));
    // } else if (!deprepare_recorder_.reach_majority(replica_num)) {// need at least majority's record
    //   ret = OB_ELECTION_WARN_NOT_REACH_MAJORITY;
    //   ELECT_ASYNC_LOG(WARN, "devote prepare msg not reach majority", K_(partition), K(t1),
    //   K_(cached_devote_prepare_msg), K(ret));
  } else if (OB_UNLIKELY((!cached_devote_prepare_msg_.get_priority().is_valid()) ||
                         (!cached_devote_prepare_msg_.get_sender().is_valid()) ||
                         (cached_devote_prepare_msg_.get_lease_time() == 0))) {  // no cached request
    ret = OB_ELECTION_WARN_NO_PREPARE_MESSAGE;
    ELECT_ASYNC_LOG(
        WARN, "cached devote prepare msg not valid", K_(partition), K(t1), K_(cached_devote_prepare_msg), K(ret));
  } else {
    server = cached_devote_prepare_msg_.get_sender();
    priority = cached_devote_prepare_msg_.get_priority();
    lease_time = cached_devote_prepare_msg_.get_lease_time();
  }

  return ret;
}

// count votes in process_devote_prepare_
// @param [out] new_leader
// @param [out] ticket
// @param [in] replica_num
// @param [in] t1]
// @return error code
int ObElectionVoteMsgPool::check_decentralized_majority(
    common::ObAddr& new_leader, int64_t& ticket, const int64_t replica_num, const int64_t t1)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(ERROR, "not init yet", K_(partition), KP(this));
  } else if (OB_UNLIKELY(0 >= replica_num || 0 >= t1)) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(ERROR, "invalid argument", K_(partition), K(replica_num), K(t1), K(ret));
  } else if (OB_UNLIKELY(t1 != devote_recorder_.get_record_T1_timestamp())) {
    ret = OB_ELECTION_WARN_T1_NOT_MATCH;
    int64_t record_t1 = devote_recorder_.get_record_T1_timestamp();
    ELECT_ASYNC_LOG(WARN, "message t1 timestamp not match", K_(partition), K(t1), K(record_t1), K(ret));
  } else if (OB_UNLIKELY(!devote_recorder_.reach_majority(replica_num))) {
    ret = OB_ELECTION_WARN_NOT_REACH_MAJORITY;
    int64_t ticket = devote_recorder_.size();
    ELECT_ASYNC_LOG(WARN,
        "devote_recorder_ not reach majority",
        K_(partition),
        K(t1),
        K(ret),
        K(ticket),
        K(cached_devote_prepare_msg_));
  } else {
    new_leader = self_;
    ticket = devote_recorder_.size();
  }

  return ret;
}

// get highest priority candidate in process_vote_prepare_
// @param [out] cur_leader
// @param [out] new_leader
// @param [in] t1
// @return error code
int ObElectionVoteMsgPool::get_centralized_candidate(ObAddr& cur_leader, ObAddr& new_leader, const int64_t t1)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(ERROR, "not init yet", K_(partition), KP(this));
  } else if (OB_UNLIKELY(0 >= t1)) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K_(partition), K(t1), K(ret));
  } else if (OB_UNLIKELY(t1 != cached_vote_prepare_msg_.get_T1_timestamp())) {  // not match
    ret = OB_ELECTION_WARN_T1_NOT_MATCH;
    ELECT_ASYNC_LOG(WARN, "message t1 timestamp not match", K_(partition), K(t1), K_(cached_vote_prepare_msg), K(ret));
  } else {  // same t1
    cur_leader = cached_vote_prepare_msg_.get_cur_leader();
    new_leader = cached_vote_prepare_msg_.get_new_leader();
  }

  return ret;
}

// count votes in process_vote_prepare_
// @param [out] cur_leader
// @param [out] new_leader
// @param [out] priority new leader's priority, needed in change leader process only
// @param [in] replica_num for counting majority
// @param [in] t1 t1 of request, if not match cached records, return error
// @return error code
int ObElectionVoteMsgPool::check_centralized_majority(
    ObAddr& cur_leader, ObAddr& new_leader, ObElectionPriority& priority, const int64_t replica_num, const int64_t t1)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    ELECT_ASYNC_LOG(ERROR, "not init yet", K_(partition), KP(this));
  } else if (OB_UNLIKELY(0 >= replica_num || 0 >= t1)) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K_(partition), K(replica_num), K(t1), K(ret));
  } else if (OB_UNLIKELY(t1 != vote_recorder_.get_record_T1_timestamp())) {
    ret = OB_ELECTION_WARN_T1_NOT_MATCH;
    int64_t record_t1 = vote_recorder_.get_record_T1_timestamp();
    ELECT_ASYNC_LOG(WARN, "message t1 timestamp not match", K_(partition), K(t1), K(record_t1), K(ret));
  } else if (!vote_recorder_.reach_majority(replica_num)) {
    ret = OB_ELECTION_WARN_NOT_REACH_MAJORITY;
    // ELECT_ASYNC_LOG(WARN, "devote prepare msg not reach majority", K_(partition), K(t1), K(ret));
  } else {
    if (OB_UNLIKELY(vote_recorder_.get_new_leader() != self_)) {  // change leader
      if (OB_UNLIKELY(cached_new_leader_vote_msg_.get_new_leader() != vote_recorder_.get_new_leader() ||
                      t1 != cached_new_leader_vote_msg_.get_T1_timestamp())) {
        ret = OB_ELECTION_WAIT_LEADER_MESSAGE;
        const ObAddr& vote_recorder_leader = vote_recorder_.get_new_leader();
        ELECT_ASYNC_LOG(WARN,
            "waitting for new leader message",
            K_(partition),
            K_(cached_new_leader_vote_msg),
            K(vote_recorder_leader),
            K(t1),
            K(ret));
      } else {
        cur_leader = self_;
        new_leader = vote_recorder_.get_new_leader();
        priority = cached_new_leader_vote_msg_.get_priority();
      }
    } else {
      cur_leader = self_;
      new_leader = self_;
    }
  }

  return ret;
}
