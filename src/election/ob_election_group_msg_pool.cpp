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

#include "ob_election_group.h"
#include <stdint.h>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::election;

void ObEGVoteMsgPool::reset()
{
  ObLockGuard<ObSpinLock> lock_guard(lock_);
  eg_prepare_array_.reset();
  eg_vote_array_.reset();
}

template <typename T>
int ObEGVoteMsgPool::do_store_(const T& msg, ObElectionVoteMsgArray<T>& array)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  T tmp_msg;

  if (!msg.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(msg));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !is_exist && i < array.get_msg_count(); ++i) {
      if (OB_FAIL(array.get_msg(i, tmp_msg))) {
        ELECT_ASYNC_LOG(WARN, "get message error", K(ret), K(i), K(tmp_msg));
      } else if (tmp_msg.get_sender() == msg.get_sender()) {
        is_exist = true;
        if (OB_SUCCESS != (ret = array.set_msg(i, msg))) {
          ELECT_ASYNC_LOG(WARN, "set msg error", K(ret), K(array), K(i), K(msg));
        }
      } else {
        // do nothing
      }
    }
    if (OB_SUCCESS == ret && !is_exist) {
      if (OB_SUCCESS != (ret = array.push_msg(msg))) {
        ELECT_ASYNC_LOG(WARN, "push msg error", K(ret), K(array), K(msg));
      } else {
        ELECT_ASYNC_LOG(DEBUG, "push msg success", K(array), K(msg));
      }
    }
  }

  return ret;
}

int ObEGVoteMsgPool::check_eg_centralized_majority(common::ObAddr& cur_leader, common::ObAddr& new_leader,
    const bool is_all_part_merged_in, bool& is_eg_majority, ObPartStateArray& part_state_array,
    const common::ObPartitionArray& partition_array, const int64_t self_version, const int64_t replica_num,
    const int64_t t1, const uint64_t eg_hash, const int64_t cur_ts)
{
  int ret = OB_SUCCESS;

  ObLockGuard<ObSpinLock> lock_guard(lock_);

  if (self_version < 0 || 0 >= replica_num || 0 >= t1 || partition_array.count() != part_state_array.count()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN,
        "invalid argument",
        K(ret),
        K(self_version),
        K(replica_num),
        K(t1),
        K(eg_hash),
        "part_stat cnt",
        part_state_array.count(),
        "partition cnt",
        partition_array.count());
  } else {
    const int64_t msg_count = eg_vote_array_.get_msg_count();
    if (msg_count <= replica_num / 2) {
      ret = OB_ELECTION_WARN_NOT_REACH_MAJORITY;
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        ELECT_ASYNC_LOG(DEBUG, "not majority msg_count", K(ret), K(msg_count), K(replica_num));
      }
    } else {
      ret = get_eg_centralized_majority_(cur_leader,
          new_leader,
          is_all_part_merged_in,
          is_eg_majority,
          part_state_array,
          partition_array,
          self_version,
          replica_num,
          t1,
          eg_hash,
          cur_ts);
    }
  }

  return ret;
}

int ObEGVoteMsgPool::store(const ObElectionMsg& msg)
{
  int ret = OB_SUCCESS;

  if (!msg.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(msg));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const int msg_type = msg.get_msg_type();
    if (msg_type == OB_ELECTION_EG_VOTE_PREPARE) {
      ret = store_(static_cast<const ObElectionMsgEGPrepare&>(msg));
    } else if (msg_type == OB_ELECTION_EG_VOTE_VOTE) {
      ret = store_(static_cast<const ObElectionMsgEGVote4Store&>(msg));
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

int ObEGVoteMsgPool::store_(const ObElectionMsgEGPrepare& msg)
{
  int ret = OB_SUCCESS;

  ObLockGuard<ObSpinLock> lock_guard(lock_);
  if (!msg.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(msg));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = do_store_<ObElectionMsgEGPrepare>(msg, eg_prepare_array_);
  }

  return ret;
}

int ObEGVoteMsgPool::store_(const ObElectionMsgEGVote4Store& msg)
{
  int ret = OB_SUCCESS;

  ObLockGuard<ObSpinLock> lock_guard(lock_);
  if (!msg.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(msg));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = do_store_<ObElectionMsgEGVote4Store>(msg, eg_vote_array_);
  }

  return ret;
}

int ObEGVoteMsgPool::get_eg_centralized_candidate(ObAddr& cur_leader, ObAddr& new_leader, const int64_t t1)
{
  int ret = OB_SUCCESS;
  int count = 0;
  ObAddr msg_cur_leader;
  ObAddr msg_new_leader;
  ObElectionMsgEGPrepare msg;

  ObLockGuard<ObSpinLock> lock_guard(lock_);

  if (0 >= t1) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(t1));
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < eg_prepare_array_.get_msg_count(); ++i) {
      if (OB_FAIL(eg_prepare_array_.get_msg(i, msg))) {
        ELECT_ASYNC_LOG(WARN, "get message error", K(ret), K(i), K(msg));
      } else if (msg.get_T1_timestamp() != t1) {
        ELECT_ASYNC_LOG(WARN, "message t1 timestamp not match", K(t1), K(msg));
        // yes, continue
      } else {
        ++count;
        msg_cur_leader = msg.get_cur_leader();
        msg_new_leader = msg.get_new_leader();
      }
    }
    if (count > 1) {
      ret = OB_ELECTION_ERROR_MULTI_PREPARE_MESSAGE;
      ELECT_ASYNC_LOG(ERROR, "vote prepare messages more than one", K_(eg_prepare_array));
    } else if (count == 1 && msg_cur_leader.is_valid() && msg_new_leader.is_valid()) {
      cur_leader = msg_cur_leader;
      new_leader = msg_new_leader;
    } else {
      ret = OB_ELECTION_WARN_NO_PREPARE_MESSAGE;
    }
    eg_prepare_array_.reset();
  }

  return ret;
}

bool ObEGVoteMsgPool::is_pkey_exist_(
    const common::ObPartitionKey& pkey, const common::ObPartitionArray& partition_array, int64_t& idx) const
{
  bool bool_ret = false;
  idx = -1;
  int64_t count = partition_array.count();
  for (int64_t i = 0; !bool_ret && i < count; ++i) {
    if (pkey == partition_array.at(i)) {
      idx = i;
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObEGVoteMsgPool::count_ticket_(const common::ObPartitionArray& msg_part_array,
    const common::ObPartitionArray& partition_array, ObPartStateArray& part_state_array)
{
  int ret = OB_SUCCESS;

  if (msg_part_array.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    common::ObPartitionKey tmp_pkey;
    int64_t tmp_idx = -1;
    const int64_t part_cnt = msg_part_array.count();
    for (int64_t i = 0; i < part_cnt; ++i) {
      tmp_pkey = msg_part_array.at(i);
      if (is_pkey_exist_(tmp_pkey, partition_array, tmp_idx)) {
        part_state_array.at(tmp_idx).vote_cnt_++;
      }
    }
  }

  return ret;
}

int ObEGVoteMsgPool::process_vote_cnt_(const int64_t eg_vote_cnt, const int64_t replica_num,
    ObPartStateArray& part_state_array, bool& is_eg_majority, const int64_t cur_ts)
{
  int ret = OB_SUCCESS;

  if (part_state_array.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    is_eg_majority = true;
    const int64_t part_cnt = part_state_array.count();
    for (int64_t i = 0; i < part_cnt; ++i) {
      part_state_array.at(i).vote_cnt_ = static_cast<int32_t>(eg_vote_cnt) + part_state_array.at(i).vote_cnt_;
      // if partition's election's lease is expired, drop the vote, cause this may exposed to others already
      if (part_state_array.at(i).vote_cnt_ <= replica_num / 2 || part_state_array.at(i).lease_end_ < cur_ts) {
        is_eg_majority = false;
      }
    }
  }

  return ret;
}

int ObEGVoteMsgPool::get_eg_centralized_majority_(common::ObAddr& cur_leader, common::ObAddr& new_leader,
    const bool is_all_part_merged_in, bool& is_eg_majority, ObPartStateArray& part_state_array,
    const common::ObPartitionArray& partition_array, const int64_t self_version, const int64_t replica_num,
    const int64_t t1, const uint64_t hash, const int64_t cur_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObElectionMsgEGVote4Store msg;
  common::ObAddr tmp_cur_leader;
  common::ObAddr tmp_new_leader;

  int64_t msg_cnt = eg_vote_array_.get_msg_count();
  for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < msg_cnt; ++i) {
    if (OB_SUCCESS != (tmp_ret = eg_vote_array_.get_msg(i, msg))) {
      ELECT_ASYNC_LOG(WARN, "get message error", K(tmp_ret), K(i), K(msg));
    } else if (msg.get_T1_timestamp() != t1) {
      ELECT_ASYNC_LOG(WARN, "message t1 timestamp not match", K(i), K(hash), K(t1), K(msg), K(msg_cnt));
      // yes, continue
    } else {
      int64_t ticket = 0;       // totle ticket
      int64_t eg_vote_cnt = 0;  // the ticket in same version
      bool is_leader_sender = false;
      tmp_cur_leader = msg.get_cur_leader();
      tmp_new_leader = msg.get_new_leader();
      for (int64_t j = 0; OB_SUCCESS == tmp_ret && j < msg_cnt; ++j) {
        if (OB_SUCCESS != (tmp_ret = eg_vote_array_.get_msg(j, msg))) {
          ELECT_ASYNC_LOG(WARN, "get message error", K(tmp_ret), K(i), K(msg));
        } else if (msg.get_T1_timestamp() != t1) {
          ELECT_ASYNC_LOG(WARN, "message t1 timestamp not match", K(t1), K(msg));
          // yes, continue
        } else if (msg.get_new_leader() == tmp_new_leader && msg.get_cur_leader() == tmp_cur_leader) {
          ++ticket;
          if (tmp_new_leader == msg.get_sender()) {
            is_leader_sender = true;
          }
          if (self_version == msg.get_eg_version()) {
            // same version, count ticket overall
            ++eg_vote_cnt;
          } else {
            // different version, count ticket for each election
            count_ticket_(msg.get_vote_part_array(), partition_array, part_state_array);
          }
        } else {
          // do nothing
        }
      }
      if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
      } else if (ticket <= replica_num / 2) {
        ret = OB_ELECTION_WARN_NOT_REACH_MAJORITY;
      } else if (!is_leader_sender) {
        ret = OB_ELECTION_WAIT_LEADER_MESSAGE;
      } else {
        if (eg_vote_cnt > replica_num / 2) {
          if (false == is_all_part_merged_in) {  // first merge-in, need check lease of every election
            process_vote_cnt_(eg_vote_cnt, replica_num, part_state_array, is_eg_majority, cur_ts);
          } else {  // already in merge-in state, no need to check lease of singal election, cause that lease is not
                    // maintained anymore
            is_eg_majority = true;
          }
        } else {
          process_vote_cnt_(eg_vote_cnt, replica_num, part_state_array, is_eg_majority, cur_ts);
        }
        cur_leader = tmp_cur_leader;
        new_leader = tmp_new_leader;
        eg_vote_array_.reset();
        ret = OB_SUCCESS;
        break;
      }
    }
  }

  return ret;
}
