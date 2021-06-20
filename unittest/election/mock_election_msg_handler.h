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

#ifndef _MOCK_ELECTION_MSG_HANDLER_H_
#define _MOCK_ELECTION_MSG_HANDLER_H_

#include "loss.h"
#include "outage.h"
#include "election/ob_election.h"
#include "election/ob_election_msg_handler.h"

namespace oceanbase {
namespace tests {
namespace election {
using namespace oceanbase::common;
using namespace oceanbase::election;

class MockObElectionMsgHandler : public ObElectionMsgHandler {
public:
  MockObElectionMsgHandler();
  ~MockObElectionMsgHandler();
  virtual int init(ObElection* e);
  virtual void destroy(void);

public:
  //////handlers for vote message
  virtual int handle_devote_prepare(const ObElectionMsgDEPrepare& msg, ObDataBuffer& out_buff);
  virtual int handle_devote_vote(const ObElectionMsgDEVote& msg, ObDataBuffer& out_buff);
  virtual int handle_devote_success(const ObElectionMsgDESuccess& msg, ObDataBuffer& out_buff);

  virtual int handle_vote_prepare(const ObElectionMsgPrepare& msg, ObDataBuffer& out_buff);
  virtual int handle_vote_vote(const ObElectionMsgVote& msg, ObDataBuffer& out_buff);
  virtual int handle_vote_success(const ObElectionMsgSuccess& msg, ObDataBuffer& out_buff);

public:
  void set_outage_for(ObElectionVoteMsgType type, int64_t start, int64_t end);
  void set_outage_all(int64_t start, int64_t end);
  void reset_outage_for(ObElectionVoteMsgType type);
  void reset_outage_all(void);

  void set_loss_all(int64_t loss_rate, int64_t start = INT64_MIN, int64_t end = INT64_MAX);
  void reset_loss_all(void);

private:
  bool inited_;
  Outage outage_map_[MAX_VOTE_MSG_TYPE];
  Loss loss_map_[MAX_VOTE_MSG_TYPE];
};
}  // namespace election
}  // namespace tests
}  // namespace oceanbase

#endif
