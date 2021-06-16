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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_MSG_
#define OCEANBASE_ELECTION_OB_ELECTION_MSG_

#include <stdint.h>
#include "lib/net/ob_addr.h"
#include "share/ob_define.h"
#include "share/rpc/ob_batch_proxy.h"
#include "common/ob_member_list.h"
#include "common/data_buffer.h"
#include "lib/list/ob_list.h"
#include "ob_election_base.h"
#include "ob_election_time_def.h"
#include "ob_election_priority.h"
#include "ob_election_group_priority.h"
#include "common/ob_partition_key.h"
#include "ob_election_group_id.h"
#include "ob_election_part_array_buf.h"

namespace oceanbase {
namespace common {
class ObAddr;
class ObSpinLock;
}  // namespace common
namespace election {
class ObElectionPriority;

// election message type, contains vote and admin message
enum ObElectionMsgType {
  UNKNOWN_TYPE = -1,
  OB_ELECTION_DEVOTE_PREPARE = 0,
  OB_ELECTION_DEVOTE_VOTE,
  OB_ELECTION_DEVOTE_SUCCESS,
  OB_ELECTION_VOTE_PREPARE,
  OB_ELECTION_VOTE_VOTE,
  OB_ELECTION_VOTE_SUCCESS,
  OB_ELECTION_EG_VOTE_PREPARE,
  OB_ELECTION_EG_VOTE_VOTE,
  OB_ELECTION_EG_VOTE_SUCCESS,
  OB_ELECTION_EG_DESTROY,

  OB_ELECTION_QUERY_LEADER = 100,
  OB_ELECTION_QUERY_LEADER_RESPONSE = 101,
  OB_ELECTION_NOTIFY_MEMBER_LIST = 102,
};

class ObElectionMsgTypeChecker {
private:
  ObElectionMsgTypeChecker()
  {}
  ~ObElectionMsgTypeChecker()
  {}

public:
  static bool is_valid_msg_type(const int msg_type)
  {
    return ((OB_ELECTION_DEVOTE_PREPARE <= msg_type && OB_ELECTION_EG_DESTROY >= msg_type) ||
            (OB_ELECTION_QUERY_LEADER <= msg_type && OB_ELECTION_NOTIFY_MEMBER_LIST >= msg_type));
  }
  static bool is_partition_msg(const int msg_type)
  {
    return ((OB_ELECTION_DEVOTE_PREPARE <= msg_type && OB_ELECTION_VOTE_SUCCESS >= msg_type) ||
            (OB_ELECTION_QUERY_LEADER <= msg_type && OB_ELECTION_NOTIFY_MEMBER_LIST >= msg_type));
  }
  static bool is_election_group_msg(const int msg_type)
  {
    return (OB_ELECTION_EG_VOTE_PREPARE <= msg_type && OB_ELECTION_EG_DESTROY >= msg_type);
  }
};

class ObElectionMsgBuffer : public common::ObDataBuffer {
  OB_UNIS_VERSION_V(1);

public:
  ObElectionMsgBuffer()
  {
    (void)set_data(buf_, MAX_MSGBUF_SIZE);
  }
  ~ObElectionMsgBuffer()
  {}

  TO_STRING_KV(K_(capacity), K_(position), K_(limit));

public:
  static const int64_t MAX_MSGBUF_SIZE = 8192 - 256;

private:
  char buf_[MAX_MSGBUF_SIZE];
};

// election base message
class ObElectionMsg : public obrpc::ObIFill {
  OB_UNIS_VERSION_V(1);

public:
  ObElectionMsg() : msg_type_(UNKNOWN_TYPE), send_timestamp_(0), reserved_(0)
  {}
  ObElectionMsg(const int msg_type, const int64_t send_ts, const common::ObAddr& sender)
      : msg_type_(msg_type), send_timestamp_(send_ts), reserved_(0), sender_(sender)
  {}
  ~ObElectionMsg()
  {}
  int get_msg_type() const
  {
    return msg_type_;
  }
  int64_t get_send_timestamp() const
  {
    return send_timestamp_;
  }
  /**********should removed after a barrier version bigger than 3.1**********/
  void set_send_timestamp(int64_t send_timestamp)
  {
    send_timestamp_ = send_timestamp;
  }
  /***************************************************************/
  const common::ObAddr& get_sender() const
  {
    return sender_;
  }
  virtual bool is_valid() const;
  void reset()
  {
    msg_type_ = ObElectionMsgType::UNKNOWN_TYPE;
    send_timestamp_ = 0;
    reserved_ = 0;
    sender_.reset();
  }

  virtual int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const
  {
    filled_size = 0;
    return serialize(buf, size, filled_size);
  }
  virtual int64_t get_req_size() const
  {
    return get_serialize_size();
  }
  virtual int64_t to_string(char* buf, const int64_t buf_len) const;
  VIRTUAL_TO_YSON_KV(Y_(msg_type), Y_(send_timestamp), Y_(sender));

protected:
  int msg_type_;
  int64_t send_timestamp_;
  int64_t reserved_;
  common::ObAddr sender_;
};

// election vote message
class ObElectionVoteMsg : public ObElectionMsg {
  OB_UNIS_VERSION_V(1);

public:
  ObElectionVoteMsg() : T1_timestamp_(0)
  {}
  ObElectionVoteMsg(const int msg_type, const int64_t send_ts, const common::ObAddr& sender, const int64_t t1)
      : ObElectionMsg(msg_type, send_ts, sender), T1_timestamp_(t1)
  {}
  ~ObElectionVoteMsg()
  {}

public:
  int64_t get_T1_timestamp() const
  {
    return T1_timestamp_;
  }
  virtual bool is_valid() const;
  void reset()
  {
    ObElectionMsg::reset();
    T1_timestamp_ = 0;
  }

  virtual int64_t to_string(char* buf, const int64_t buf_len) const;
  VIRTUAL_TO_YSON_KV(Y_(msg_type), Y_(send_timestamp), Y_(sender), Y_(T1_timestamp));

protected:
  int64_t T1_timestamp_;
};

// election message: OB_ELECTION_DEVOET_PREPARE
class ObElectionMsgDEPrepare : public ObElectionVoteMsg {
  OB_UNIS_VERSION(1);

public:
  ObElectionMsgDEPrepare() : lease_time_(0)
  {}
  ObElectionMsgDEPrepare(const ObElectionPriority& priority, const int64_t t1, const int64_t send_ts,
      const common::ObAddr& sender, const int64_t lease_time);
  ~ObElectionMsgDEPrepare()
  {}
  ObElectionMsgDEPrepare& operator=(const ObElectionMsgDEPrepare& other) = default;

  // bool intime(const int64_t ts, const int64_t t1) const {return IN_T1_RANGE(ts, t1); }
  const ObElectionPriority& get_priority() const
  {
    return priority_;
  }
  int64_t get_lease_time() const
  {
    return lease_time_;
  }
  bool is_valid() const
  {
    return ObElectionVoteMsg::is_valid() && priority_.is_valid();
  }
  void reset()
  {
    ObElectionVoteMsg::reset();
    priority_.reset();
    lease_time_ = 0;
  }

  int64_t to_string(char* buf, const int64_t buf_len) const;
  TO_YSON_KV(Y_(msg_type), Y_(send_timestamp), Y_(sender), Y_(T1_timestamp), Y_(lease_time), Y_(priority));

private:
  ObElectionPriority priority_;
  int64_t lease_time_;
};

// election message: OB_ELECTION_DEVOTE_VOTE
class ObElectionMsgDEVote : public ObElectionVoteMsg {
  OB_UNIS_VERSION(1);

public:
  ObElectionMsgDEVote()
  {}
  ObElectionMsgDEVote(
      const common::ObAddr& leader, const int64_t t1, const int64_t send_ts, const common::ObAddr& sender);
  ~ObElectionMsgDEVote()
  {}
  // bool intime(const int64_t ts, const int64_t t1) const {return IN_T2_RANGE(ts, t1); }

  const common::ObAddr& get_vote_leader() const
  {
    return vote_leader_;
  }
  bool is_valid() const
  {
    return (ObElectionVoteMsg::is_valid() && (vote_leader_.is_valid()));
  }

  int64_t to_string(char* buf, const int64_t buf_len) const;
  TO_YSON_KV(Y_(msg_type), Y_(send_timestamp), Y_(sender), Y_(T1_timestamp), Y_(vote_leader));

private:
  common::ObAddr vote_leader_;
};

// election message: OB_ELECTION_DEVOTE_SUCCESS
class ObElectionMsgDESuccess : public ObElectionVoteMsg {
  OB_UNIS_VERSION(1);

public:
  ObElectionMsgDESuccess() : lease_time_(0)
  {}
  ObElectionMsgDESuccess(const common::ObAddr& leader, const int64_t t1, const int64_t send_ts,
      const common::ObAddr& sender, const int64_t lease_time);
  ~ObElectionMsgDESuccess()
  {}
  // bool intime(const int64_t ts, const int64_t t1) const { return IN_T3_RANGE(ts, t1);}
  const common::ObAddr& get_leader() const
  {
    return leader_;
  }
  int64_t get_lease_time() const
  {
    return lease_time_;
  }
  bool is_valid() const
  {
    return (ObElectionVoteMsg::is_valid() && (leader_.is_valid()));
  }

  int64_t to_string(char* buf, const int64_t buf_len) const;
  TO_YSON_KV(Y_(msg_type), Y_(send_timestamp), Y_(sender), Y_(lease_time), Y_(T1_timestamp), Y_(leader));

private:
  common::ObAddr leader_;
  int64_t lease_time_;
};

// election message: OB_ELECTION_VOTE_PREPARE
class ObElectionMsgPrepare : public ObElectionVoteMsg {
  OB_UNIS_VERSION(1);

public:
  ObElectionMsgPrepare() : lease_time_(0)
  {}
  ObElectionMsgPrepare(const common::ObAddr& cur_leader, const common::ObAddr& new_leader, const int64_t t1,
      const int64_t send_ts, const common::ObAddr& sender, const int64_t lease_time);
  ~ObElectionMsgPrepare()
  {}
  ObElectionMsgPrepare& operator=(const ObElectionMsgPrepare& other) = default;
  // bool intime(const int64_t ts, const int64_t t1) const
  //{ return check_upgrade_env() ? IN_T1_RANGE(ts, t1) : IN_T0_RANGE(ts, t1);}
  const common::ObAddr& get_cur_leader() const
  {
    return cur_leader_;
  }
  const common::ObAddr& get_new_leader() const
  {
    return new_leader_;
  }
  int64_t get_lease_time() const
  {
    return lease_time_;
  }
  bool is_valid() const;
  void reset()
  {
    ObElectionVoteMsg::reset();
    cur_leader_.reset();
    new_leader_.reset();
    lease_time_ = 0;
  }

  int64_t to_string(char* buf, const int64_t buf_len) const;
  TO_YSON_KV(
      Y_(msg_type), Y_(send_timestamp), Y_(sender), Y_(T1_timestamp), Y_(cur_leader), Y_(new_leader), Y_(lease_time));

private:
  common::ObAddr cur_leader_;
  common::ObAddr new_leader_;
  int64_t lease_time_;
};

// election message: OB_ELECTION_VOTE_VOTE
class ObElectionMsgVote : public ObElectionVoteMsg {
  OB_UNIS_VERSION(1);

public:
  ObElectionMsgVote()
  {}
  ObElectionMsgVote(const ObElectionPriority& priority, const common::ObAddr& cur_leader,
      const common::ObAddr& new_leader, const int64_t t1, const int64_t send_ts, const common::ObAddr& sender);
  ~ObElectionMsgVote()
  {}
  ObElectionMsgVote& operator=(const ObElectionMsgVote& other) = default;
  // bool intime(const int64_t ts, const int64_t t1) const
  //{ return check_upgrade_env() ? IN_T2_RANGE(ts, t1) : IN_T0_RANGE(ts, t1);}
  const ObElectionPriority& get_priority() const
  {
    return priority_;
  }
  const common::ObAddr& get_cur_leader() const
  {
    return cur_leader_;
  }
  const common::ObAddr& get_new_leader() const
  {
    return new_leader_;
  }
  bool is_valid() const;
  void reset()
  {
    ObElectionVoteMsg::reset();
    priority_.reset();
    cur_leader_.reset();
    new_leader_.reset();
  }

  int64_t to_string(char* buf, const int64_t buf_len) const;
  TO_YSON_KV(
      Y_(msg_type), Y_(send_timestamp), Y_(sender), Y_(T1_timestamp), Y_(priority), Y_(cur_leader), Y_(new_leader));

private:
  ObElectionPriority priority_;
  common::ObAddr cur_leader_;
  common::ObAddr new_leader_;
};

// election message: OB_ELECTION_VOTE_SUCCESS
class ObElectionMsgSuccess : public ObElectionVoteMsg {
  OB_UNIS_VERSION(1);

public:
  ObElectionMsgSuccess()
      : lease_time_(0), last_gts_(common::OB_INVALID_TIMESTAMP), last_leader_epoch_(common::OB_INVALID_TIMESTAMP)
  {}
  ObElectionMsgSuccess(const common::ObAddr& cur_leader, const common::ObAddr& new_leader, const int64_t t1,
      const int64_t send_ts, const common::ObAddr& sender, const int64_t lease_time, const int64_t last_gts,
      const int64_t last_leader_epoch);
  ~ObElectionMsgSuccess()
  {}
  // bool intime(const int64_t ts, const int64_t t1) const
  //{ return check_upgrade_env() ? IN_T3_RANGE(ts, t1) : IN_T0_RANGE(ts, t1);}
  const common::ObAddr& get_cur_leader() const
  {
    return cur_leader_;
  }
  const common::ObAddr& get_new_leader() const
  {
    return new_leader_;
  }
  int64_t get_lease_time() const
  {
    return lease_time_;
  }
  int64_t get_last_gts() const
  {
    return last_gts_;
  }
  int64_t get_last_leader_epoch() const
  {
    return last_leader_epoch_;
  }
  bool is_valid() const;

  int64_t to_string(char* buf, const int64_t buf_len) const;
  TO_YSON_KV(
      Y_(msg_type), Y_(send_timestamp), Y_(sender), Y_(T1_timestamp), Y_(cur_leader), Y_(new_leader), Y_(lease_time));

private:
  common::ObAddr cur_leader_;
  common::ObAddr new_leader_;
  int64_t lease_time_;
  int64_t last_gts_;
  int64_t last_leader_epoch_;
};

// election query leader
class ObElectionQueryLeader : public ObElectionMsg {
  OB_UNIS_VERSION_V(1);

public:
  ObElectionQueryLeader()
  {}
  ObElectionQueryLeader(const int64_t send_ts, const common::ObAddr& sender);
  ~ObElectionQueryLeader()
  {}

public:
  bool is_valid() const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
  TO_YSON_KV(Y_(msg_type), Y_(send_timestamp), Y_(sender));
};

// election query leader response
class ObElectionQueryLeaderResponse : public ObElectionMsg {
  OB_UNIS_VERSION_V(1);

public:
  ObElectionQueryLeaderResponse() : epoch_(0), t1_(0), lease_time_(0)
  {}
  ObElectionQueryLeaderResponse(const int64_t send_ts, const common::ObAddr& sender, const common::ObAddr& leader,
      const int64_t epoch, const int64_t t1, const int64_t lease_time);
  ~ObElectionQueryLeaderResponse()
  {}

public:
  const common::ObAddr& get_leader() const
  {
    return leader_;
  }
  int64_t get_epoch() const
  {
    return epoch_;
  }
  int64_t get_t1() const
  {
    return t1_;
  }
  int64_t get_lease_time() const
  {
    return lease_time_;
  }
  bool is_valid() const;

  int64_t to_string(char* buf, const int64_t buf_len) const;
  TO_YSON_KV(Y_(msg_type), Y_(send_timestamp), Y_(sender), Y_(leader), Y_(epoch), Y_(t1), Y_(lease_time));

private:
  common::ObAddr leader_;
  int64_t epoch_;
  int64_t t1_;
  int64_t lease_time_;
};

// election message: OB_ELECTION_EG_VOTE_PREPARE
class ObElectionMsgEGPrepare : public ObElectionVoteMsg {
  OB_UNIS_VERSION(1);

public:
  ObElectionMsgEGPrepare() : lease_time_(0)
  {}
  ObElectionMsgEGPrepare(const common::ObAddr& cur_leader, const common::ObAddr& new_leader, const int64_t t1,
      const int64_t send_ts, const common::ObAddr& sender, const int64_t lease_time);
  ~ObElectionMsgEGPrepare()
  {}
  const common::ObAddr& get_cur_leader() const
  {
    return cur_leader_;
  }
  const common::ObAddr& get_new_leader() const
  {
    return new_leader_;
  }
  int64_t get_lease_time() const
  {
    return lease_time_;
  }
  bool is_valid() const;

  int64_t to_string(char* buf, const int64_t buf_len) const;
  TO_YSON_KV(
      Y_(msg_type), Y_(send_timestamp), Y_(sender), Y_(T1_timestamp), Y_(cur_leader), Y_(new_leader), Y_(lease_time));

private:
  common::ObAddr cur_leader_;
  common::ObAddr new_leader_;
  int64_t lease_time_;
};

// election message: OB_ELECTION_EG_VOTE_VOTE
// vote message of election group
class ObElectionMsgEGVote : public ObElectionVoteMsg {
  OB_UNIS_VERSION(1);

public:
  ObElectionMsgEGVote()
  {}
  ObElectionMsgEGVote(const ObElectionMsgEGVote& msg);
  ObElectionMsgEGVote(const ObElectionGroupPriority& priority, const common::ObAddr& cur_leader,
      const common::ObAddr& new_leader, const int64_t t1, const int64_t send_ts, const common::ObAddr& sender);
  ~ObElectionMsgEGVote()
  {}
  const ObElectionGroupPriority& get_priority() const
  {
    return priority_;
  }
  const common::ObAddr& get_cur_leader() const
  {
    return cur_leader_;
  }
  const common::ObAddr& get_new_leader() const
  {
    return new_leader_;
  }
  virtual bool is_valid() const;
  virtual int64_t to_string(char* buf, const int64_t buf_len) const;
  TO_YSON_KV(
      Y_(msg_type), Y_(send_timestamp), Y_(sender), Y_(T1_timestamp), Y_(priority), Y_(cur_leader), Y_(new_leader));

protected:
  ObElectionGroupPriority priority_;
  common::ObAddr cur_leader_;
  common::ObAddr new_leader_;
};

// stored vote messages on leader
class ObElectionMsgEGVote4Store : public ObElectionMsgEGVote {
public:
  ObElectionMsgEGVote4Store() : eg_version_(0)
  {}
  ObElectionMsgEGVote4Store(const ObElectionMsgEGVote& eg_vote_msg);
  ObElectionMsgEGVote4Store(
      const ObElectionMsgEGVote& eg_vote_msg, const int64_t eg_version, const common::ObPartitionArray& vote_array);
  ~ObElectionMsgEGVote4Store()
  {}
  int64_t get_eg_version() const
  {
    return eg_version_;
  }
  void set_eg_version(const int64_t eg_version)
  {
    eg_version_ = eg_version;
  }
  const common::ObPartitionArray& get_vote_part_array() const
  {
    return vote_part_array_;
  }
  void set_vote_part_array(const common::ObPartitionArray& vote_array)
  {
    vote_part_array_ = vote_array;
  }

  TO_YSON_KV(Y_(msg_type), Y_(send_timestamp), Y_(sender), Y_(T1_timestamp), OB_ID(priority), get_priority(),
      OB_ID(cur_leader), get_cur_leader(), OB_ID(new_leader), get_new_leader(), Y_(eg_version));

protected:
  int64_t eg_version_;
  common::ObPartitionArray vote_part_array_;
};

// election message: OB_ELECTION_EG_VOTE_SUCCESS
class ObElectionMsgEGSuccess : public ObElectionVoteMsg {
  OB_UNIS_VERSION(1);

public:
  ObElectionMsgEGSuccess() : is_all_part_merged_in_(false), lease_time_(0)
  {}
  ObElectionMsgEGSuccess(const common::ObAddr& cur_leader, const common::ObAddr& new_leader, const int64_t t1,
      const bool is_all_part_merged_in, const ObPartIdxArray& majority_part_idx_array, const int64_t send_ts,
      const common::ObAddr& sender, const int64_t lease_time);
  ~ObElectionMsgEGSuccess()
  {}
  const common::ObAddr& get_cur_leader() const
  {
    return cur_leader_;
  }
  const common::ObAddr& get_new_leader() const
  {
    return new_leader_;
  }
  bool is_all_part_merged_in() const
  {
    return is_all_part_merged_in_;
  }
  const ObPartIdxArray& get_majority_part_idx_array() const
  {
    return majority_part_idx_array_;
  }
  int64_t get_lease_time() const
  {
    return lease_time_;
  }
  bool is_valid() const;

  int64_t to_string(char* buf, const int64_t buf_len) const;
  TO_YSON_KV(
      Y_(msg_type), Y_(send_timestamp), Y_(sender), Y_(T1_timestamp), Y_(cur_leader), Y_(new_leader), Y_(lease_time));

private:
  common::ObAddr cur_leader_;
  common::ObAddr new_leader_;
  bool is_all_part_merged_in_;
  ObPartIdxArray majority_part_idx_array_;
  int64_t lease_time_;
};

class ObEGBatchReq : public obrpc::ObIFill {
public:
  ObEGBatchReq()
  {
    reset();
  }
  ObEGBatchReq(const ObPartArrayBuffer* array_buf, const ObElectionMsg* eg_msg)
      : eg_version_(array_buf->get_eg_version()),
        array_buf_(array_buf->get_data_buf()),
        buf_ser_size_(array_buf->get_serialize_size()),
        eg_msg_(eg_msg)
  {}
  ~ObEGBatchReq()
  {
    reset();
  }
  void reset();
  virtual int fill_buffer(char* buf, int64_t size, int64_t& filled_size) const;
  virtual int64_t get_req_size() const;

private:
  int64_t eg_version_;
  const char* array_buf_;
  int64_t buf_ser_size_;
  const ObElectionMsg* eg_msg_;
};

}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_MSG_
