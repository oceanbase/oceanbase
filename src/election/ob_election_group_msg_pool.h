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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_GROUP_MSG_POOL_
#define OCEANBASE_ELECTION_OB_ELECTION_GROUP_MSG_POOL_

#include <stdint.h>
#include "lib/net/ob_addr.h"
#include "lib/list/ob_list.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_print_utils.h"
#include "common/data_buffer.h"
#include "ob_election_base.h"
#include "ob_election_msg.h"
#include "ob_election_time_def.h"
#include "ob_election_async_log.h"
#include "ob_election_group_id.h"

namespace oceanbase {
namespace election {
class ObElection;

template <class T>
class ObElectionVoteMsgArray {
public:
  static const int64_t OB_MAX_ELECTION_MSG_COUNT = 2 * common::OB_MAX_MEMBER_NUMBER;

public:
  ObElectionVoteMsgArray()
  {
    reset();
  }
  ~ObElectionVoteMsgArray()
  {
    destroy();
  }
  void reset()
  {
    msg_count_ = 0;
  }
  void destroy()
  {
    reset();
  }

public:
  int set_msg(const int64_t idx, const T& msg);
  int push_msg(const T& msg);
  int get_msg(const int64_t idx, T& msg) const;
  int64_t get_msg_count() const
  {
    return msg_count_;
  }
  int64_t to_string(char* buf, const int64_t buf_len) const;

private:
  T msg_array_[OB_MAX_ELECTION_MSG_COUNT];
  int64_t msg_count_;
};

template <class T>
int ObElectionVoteMsgArray<T>::set_msg(const int64_t idx, const T& msg)
{
  int ret = common::OB_SUCCESS;

  if (idx < 0 || OB_MAX_ELECTION_MSG_COUNT <= idx || !msg.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(idx), K(msg));
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    msg_array_[idx] = msg;
  }

  return ret;
}

template <class T>
int ObElectionVoteMsgArray<T>::push_msg(const T& msg)
{
  int ret = common::OB_SUCCESS;

  if (!msg.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(msg));
    ret = common::OB_INVALID_ARGUMENT;
  } else if (msg_count_ + 1 > OB_MAX_ELECTION_MSG_COUNT) {
    ELECT_ASYNC_LOG(WARN, "push msg error", K_(msg_count), K(msg));
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    msg_array_[msg_count_++] = msg;
  }

  return ret;
}

template <class T>
int ObElectionVoteMsgArray<T>::get_msg(const int64_t idx, T& msg) const
{
  int ret = common::OB_SUCCESS;

  if (idx < 0 || idx >= OB_MAX_ELECTION_MSG_COUNT) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(idx));
    ret = common::OB_INVALID_ARGUMENT;
  } else if (!msg_array_[idx].is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid msg", "msg", msg_array_[idx]);
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    msg = msg_array_[idx];
  }

  return ret;
}

template <class T>
int64_t ObElectionVoteMsgArray<T>::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  J_ARRAY_START();
  common::databuff_printf(buf, buf_len, pos, "\"msg_count\":%ld ", msg_count_);
  for (int64_t i = 0; i < msg_count_ - 1; ++i) {
    BUF_PRINTO(msg_array_[i]);
    J_COMMA();
  }
  if (0 < msg_count_) {
    BUF_PRINTO(msg_array_[msg_count_ - 1]);
  }
  J_ARRAY_END();

  return pos;
}

class ObEGVoteMsgPool {
public:
  ObEGVoteMsgPool() : lock_(common::ObLatchIds::ELECTION_MSG_LOCK)
  {
    reset();
  }
  virtual ~ObEGVoteMsgPool()
  {
    destroy();
  }
  void reset();
  void destroy()
  {
    reset();
  }
  int store(const ObElectionMsg& msg);
  int check_eg_centralized_majority(common::ObAddr& cur_leader, common::ObAddr& new_leader,
      const bool is_all_part_merged_in, bool& is_eg_majority, ObPartStateArray& part_state_array,
      const common::ObPartitionArray& partition_array, const int64_t self_version, const int64_t replica_num,
      const int64_t t1, const uint64_t eg_hash, const int64_t cur_ts);
  int get_eg_centralized_candidate(common::ObAddr& cur_leader, common::ObAddr& new_leader, const int64_t t1);

public:
  ObElectionVoteMsgArray<ObElectionMsgEGPrepare>& get_eg_prepare_msg_array()
  {
    return eg_prepare_array_;
  }
  ObElectionVoteMsgArray<ObElectionMsgEGVote4Store>& get_eg_vote_msg_array()
  {
    return eg_vote_array_;
  }
  TO_STRING_KV(K_(eg_prepare_array), K_(eg_vote_array));

private:
  int get_eg_centralized_majority_(common::ObAddr& cur_leader, common::ObAddr& new_leader,
      const bool is_all_part_merged_in, bool& is_eg_majority, ObPartStateArray& part_state_array,
      const common::ObPartitionArray& partition_array, const int64_t self_version, const int64_t replica_num,
      const int64_t t1, const uint64_t hash, const int64_t cur_ts);
  bool is_pkey_exist_(
      const common::ObPartitionKey& pkey, const common::ObPartitionArray& partition_array, int64_t& idx) const;
  int count_ticket_(const common::ObPartitionArray& msg_part_array, const common::ObPartitionArray& partition_array,
      ObPartStateArray& part_state_array);
  int process_vote_cnt_(const int64_t eg_vote_cnt, const int64_t replica_num, ObPartStateArray& part_state_array,
      bool& is_eg_majority, const int64_t cur_ts);

private:
  int store_(const ObElectionMsgEGPrepare& msg);
  int store_(const ObElectionMsgEGVote4Store& msg);

private:
  template <typename T>
  int do_store_(const T& msg, ObElectionVoteMsgArray<T>& array);

private:
  ObElectionVoteMsgArray<ObElectionMsgEGPrepare> eg_prepare_array_;
  ObElectionVoteMsgArray<ObElectionMsgEGVote4Store> eg_vote_array_;
  mutable common::ObSpinLock lock_;
};

}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_GROUP_MSG_POOL_
