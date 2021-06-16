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

#ifndef _OB_ELECTION_TESTER_
#define _OB_ELECTION_TESTER_

#include "lib/allocator/page_arena.h"
#include "common/ob_server.h"
#include "common/ob_array_helper.h"
#include "election/ob_election_server.h"
#include "election/ob_election_rpc.h"
#include "mock_election_rpc.h"
#include "mock_election_clock.h"

#define NOW (::oceanbase::common::ObTimeUtility::current_time() / 1000)
#define NEXT_T_CYCLE(ts) (ts) - ((ts) % (T_CYCLE)) + (T_CYCLE)
#define NEXT_T_ELECT2(ts) (ts) - ((ts) % (T_ELECT2)) + (T_ELECT2)
#define DECENTRALIZED_VOTE_START ((NOW) + (T_CYCLE) + (T_CYCLE)-1) / (T_CYCLE) * (T_CYCLE)

namespace oceanbase {
namespace tests {
namespace election {
using namespace oceanbase::common;
using namespace oceanbase::election;

/*usage:

  ObElectionTesterCluster cluster;
  ObElectionTester t1, t2, t3;

  t1.set_candidate()
    .set_packet_loss(DECENTRALIZED_DO_VOTE, ObElectionTester::IN, INT_MIN, INT_MAX)
    .clock_skew(timeout)
    .add_to(cluster);
  t2.set_candidate()
    .set_packet_loss(DECENTRALIZED_DO_VOTE, ObElectionTester::IN, INT_MIN, INT_MAX)
    .clock_skew(timeout)
    .add_to(cluster);
  t3.set_candidate()
    .set_packet_loss(DECENTRALIZED_DO_VOTE, ObElectionTester::IN, INT_MIN, INT_MAX)
    .clock_skew(timeout)
    .add_to(cluster);

  cluster.init(dev_name).start();
  sleep(3);
  t1.restart();
  sleep(4);

  ASSERT(t1.has_leader() && t1.get_current_leader() == t1)
  */
class ObElectionTesterCluster;
class MockClockSkew;
class MockClockSkewRandom;
class MockObElectionRpcStub;
class ObElectionTester {
  friend class ObElectionTesterCluster;

public:
  enum InitialState { NORMAL_MODE = 0, RANDOM_MOCK, MOCK };
  enum NetWorkDirection { INVALID = 0, IN, OUT, IN_OUT };

public:
  ObElectionTester();
  ~ObElectionTester();

  inline ObElectionTester& set_candidate();
  inline bool is_candidate() const;
  void destroy(void);
  void create(void);

public:
  ObElectionTester& set_packet_loss_all(NetWorkDirection direction, int64_t start, int64_t end);
  ObElectionTester& set_packet_loss(ObElectionVoteMsgType type, NetWorkDirection direction, int64_t start, int64_t end);
  ObElectionTester& set_random_packet_loss(
      NetWorkDirection direction, int64_t loss_rate, int64_t start = INT64_MIN, int64_t end = INT64_MAX);

  ObElectionTester& clock_skew(int64_t ts);
  ObElectionTester& clock_skew_random(int64_t t1, int64_t t2);

  int start(void);
  void stop(void);
  void restart(void);

  ObElectionTester* get_current_leader(void) const;
  ObElectionTester& add_to(ObElectionTesterCluster& cluster);
  const ObServer& get_self() const;

public:
  void reset_rpc(void);
  void reset_clock(void);
  void reset_msg_handler(void);
  int get_port(void) const;
  void set_port(const int32_t port);

  bool has_leader() const;
  bool is_real_leader() const;
  bool operator==(const ObServer& server) const;

private:
  int init(const char* dev_name, const ObList<ObServer, common::ObIAllocator>& voters,
      const ObList<ObServer, common::ObIAllocator>& candidates, const ObList<ObServer, common::ObIAllocator>& watchers);

  MockClockSkew* clock_skew_;
  MockClockSkewRandom* clock_skew_random_;

  MockObElectionRpcStub* rpc_loss_;
  MockObElectionRpcStub* rpc_random_loss_;

  MockObElectionMsgHandler* msg_handler_loss_;
  MockObElectionMsgHandler* msg_handler_random_loss_;

private:
  ObElectionServer* server_;          // election server wrapped by this class
  ObElectionTesterCluster* cluster_;  // cluster this tester belongs to
  InitialState skew_stat_;
  InitialState rpc_recv_stat_;
  InitialState rpc_send_stat_;

private:
  bool inited_;  // we need two step of init
  int port_;
  ObElectionMemberType server_role_;
  obsys::CRWLock mutex_;
};

class ObElectionTesterCluster {
  friend class ObElectionTester;

public:
  ObElectionTesterCluster();
  ObElectionTesterCluster& init();
  ObElectionTesterCluster& init(const char* dev_name);
  void start(void);
  void stop(void);
  inline int64_t get_server_count() const;
  bool have_leader_agreement(void) const;
  bool have_leader_agreement(const ObElectionTester* leader) const;
  ObElectionTester* get_tester(const ObServer& server);
  ObElectionTester* get_one_slave(void) const;

private:
  static int port;
  static const char* devname;
  // calc servers and push them into three list according to server role
  int calculate_roles();
  int remove(const ObElectionTester* tester);
  bool add(ObElectionTester* tester);

private:
  static const int MAX_TESTER = 64;
  ObElectionTester* vtester_[MAX_TESTER];
  ObArrayHelper<ObElectionTester*> array_helper_;
  common::ObArenaAllocator allocator_;
  ObList<ObServer, common::ObIAllocator> voters_;
  ObList<ObServer, common::ObIAllocator> candidates_;
  ObList<ObServer, common::ObIAllocator> watchers_;
  uint32_t local_ip_;
  const char* dev_name_;
  bool inited_;  // we need two step of init
};
inline int64_t ObElectionTesterCluster::get_server_count() const
{
  return array_helper_.get_array_index();
}

inline ObElectionTester& ObElectionTester::set_candidate()
{
  server_role_ = MEMBER_TYPE_CANDIDATE;
  return *this;
}

inline bool ObElectionTester::is_candidate() const
{
  return server_role_ == MEMBER_TYPE_CANDIDATE;
}
}  // namespace election
}  // namespace tests
}  // namespace oceanbase

#endif
