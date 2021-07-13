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

#ifndef __OB_TEST_ELECTION_MOCK_HANDLE_PACKET_H_
#define __OB_TEST_ELECTION_MOCK_HANDLE_PACKET_H_

#include "common/data_buffer.h"
#include "election/ob_election_server.h"
#include "election/ob_election_msg.h"
#include "outage.h"

namespace oceanbase {
namespace tests {
namespace election {
using namespace oceanbase::common;
using namespace oceanbase::election;

class MockObElection : public ObElection {
public:
  MockObElection();

public:
  virtual int handle_packet(const int64_t packet_code, ObDataBuffer& in_buff, ObDataBuffer& out_buff);

  int set_outage_for(ObElectionVoteMsgType type, int64_t start_time = 0, int64_t end_time = INT64_MAX);

  int set_outage_all(int64_t start_time = 0, int64_t end_time = INT64_MAX);

private:
  Outage outage_map_[MAX_VOTE_MSG_TYPE];
};
}  // namespace election
}  // namespace tests
}  // namespace oceanbase
#endif
