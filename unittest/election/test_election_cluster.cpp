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

#include "test_election_cluster.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/ob_cluster_version.h"
#include "election/ob_election_async_log.h"
#include <cstdio>

namespace oceanbase {
namespace unittest {
const static char* LOCAL_IP = "127.0.0.1";
const static int32_t RPC_PORT = 34506;
static int64_t membership_version = 0;

int ObTestElectionCluster::start_one_(const int64_t idx, const char* ip, const int32_t rpc_port)
{
  int ret = OB_SUCCESS;

  if (idx < 0 || idx >= MAX_OB_TRANS_SERVICE_NUM || NULL == ip) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(idx), KP(ip));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObAddr addr;

    if (!addr.set_ip_addr(ip, rpc_port)) {
      ELECT_ASYNC_LOG(WARN, "set ipv4 addr error", K(ret), K(ip), K(rpc_port));
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_SUCCESS != (ret = election_mgr_[idx].init(addr, &batch_rpc_, &eg_cb_))) {
      ELECT_ASYNC_LOG(WARN, "election mgr init error", K(ret), K(idx));
    } else if (OB_SUCCESS != (ret = election_rpc_[idx].init(&election_mgr_[idx], addr))) {
      ELECT_ASYNC_LOG(WARN, "election rpc init error", K(ret));
    } else if (OB_SUCCESS != (ret = election_mgr_[idx].set_election_rpc(this))) {
      ELECT_ASYNC_LOG(WARN, "election mgr set rpc error", K(ret));
    } else if (OB_SUCCESS != (ret = election_mgr_[idx].start())) {
      ELECT_ASYNC_LOG(WARN, "election mgr start error", K(ret));
    } else {
      election_mgr_[idx].idx_ = idx;
      election_mgr_[idx].addr_ = addr;
      ELECT_ASYNC_LOG(INFO, "election mgr start success", K(idx), K(addr));
    }
  }

  return ret;
}

int ObTestElectionCluster::init()
{
  int ret = OB_SUCCESS;

  ip_ = LOCAL_IP;
  base_rpc_port_ = RPC_PORT;
  is_force_leader_ = false;

  return ret;
}

int ObTestElectionCluster::start()
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_OB_TRANS_SERVICE_NUM; ++i) {
    if (OB_SUCCESS != (ret = start_one_(i, ip_, base_rpc_port_ + (int32_t)i))) {
      ELECT_ASYNC_LOG(WARN, "start one election mgr error", K(ret), K(i));
    } else {
      ELECT_ASYNC_LOG(INFO, "start one election mgr success", K(i));
    }
  }

  if (OB_SUCC(ret)) {
    inited_ = true;
  }

  return ret;
}

void ObTestElectionCluster::destroy()
{
  ELECT_ASYNC_LOG(INFO, "ObTestElectionCluster destroy");
  for (int64_t i = 0; i < MAX_OB_TRANS_SERVICE_NUM; ++i) {
    election_rpc_[i].destroy();
    election_mgr_[i].destroy();
  }
  inited_ = false;
}

int ObTestElectionCluster::post_election_msg(
    const ObAddr& server, const ObPartitionKey& partition, const election::ObElectionMsg& msg)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ELECT_ASYNC_LOG(WARN, "not init");
    ret = OB_NOT_INIT;
  } else if (!server.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(ret), K(server), K(msg));
  } else {
    if (!is_force_leader_) {
      for (int32_t i = 0; i < MAX_ELECTION_MSG_COUNT; ++i) {
        if (msg.get_msg_type() == i && rpc_loss_[i].times_-- > 0) {
          ELECT_ASYNC_LOG(INFO, "message loss", "msg_type", i);
          ret = OB_RPC_POST_ERROR;
          break;
        }
      }
    }

    if (OB_SUCC(ret)) {
      const int32_t idx = server.get_port() - base_rpc_port_;
      if (idx < 0 || idx > MAX_OB_TRANS_SERVICE_NUM) {
        ret = OB_ERR_UNEXPECTED;
        ELECT_ASYNC_LOG(WARN, "server address error", K(idx), K(server), K(base_rpc_port_));
      } else {
        ElectionRpcTask* rpc_task = ElectionRpcTaskFactory::alloc();
        if (NULL == rpc_task) {
          ELECT_ASYNC_LOG(WARN, "alloc election rpc task error");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          ObElectionMsgBuffer msgbuf;

          if (OB_SUCCESS !=
              (ret = serialization::encode_i32(
                   msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), msg.get_msg_type()))) {
            ELECT_ASYNC_LOG(WARN, "serialize msg type error", K(ret), "msg type", msg.get_msg_type());
          } else if (OB_SUCCESS !=
                     (ret = partition.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position()))) {
            ELECT_ASYNC_LOG(WARN, "serialize partition error", K(ret), K(partition));
          } else if (OB_SUCCESS != msg.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())) {
            ELECT_ASYNC_LOG(WARN, "serialize msg error", K(ret), K(msg));
          } else {
            rpc_task->server_ = server;
            rpc_task->msgbuf_ = msgbuf;
            rpc_task->partition_ = partition;
            rpc_task->timestamp_ = ObClockGenerator::getClock();

            if (OB_SUCCESS != (ret = election_rpc_[idx].push(rpc_task))) {
              ELECT_ASYNC_LOG(WARN, "push election rpc task error", K(ret), K(msg));
              ElectionRpcTaskFactory::release(rpc_task);
            } else {
              ELECT_ASYNC_LOG(INFO, "election msg", K(idx), K(server), K(msg), K(partition));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObTestElectionCluster::post_election_group_msg(const common::ObAddr& server, const ObElectionGroupId& eg_id,
    const ObPartArrayBuffer& part_array_buf, const ObElectionMsg& msg)
{
  UNUSED(server);
  UNUSED(eg_id);
  UNUSED(part_array_buf);
  UNUSED(msg);
  return OB_SUCCESS;
}

int ObTestElectionCluster::add_partition(const ObPartitionKey& partition, const ObAddr& leader)
{
  int ret = OB_SUCCESS;
  int64_t leader_epoch = 0;
  ObIElection* unused = NULL;

  if (!inited_) {
    ELECT_ASYNC_LOG(WARN, "not init");
    ret = OB_NOT_INIT;
  } else if (!partition.is_valid() || !leader.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition), K(leader));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObAddr addr;
    ObMemberList mlist;

    for (int64_t i = 0; OB_SUCC(ret) && i < OB_REPLICA_NUM; ++i) {
      if (!addr.set_ip_addr(ip_, base_rpc_port_ + (int32_t)i)) {
        ELECT_ASYNC_LOG(WARN, "set ipv4 addr error", K_(ip), "port", base_rpc_port_ + i);
        ret = OB_ERR_UNEXPECTED;
      } else {
        if (OB_SUCCESS != (ret = mlist.add_server(addr))) {
          ELECT_ASYNC_LOG(WARN, "mlist add server error", K(ret), K(i), K(mlist), K(addr));
        } else if (OB_SUCCESS !=
                   (ret = election_mgr_[i].add_partition(partition, OB_REPLICA_NUM, &election_cb_[i], unused))) {
          ELECT_ASYNC_LOG(WARN, "election_mgr add partition error", K(ret), K(i));
        } else {
          ELECT_ASYNC_LOG(INFO, "election_mgr add partition success", K(i), K(partition), K(addr));
        }
      }
    }

    int64_t cur_ts = ObClockGenerator::getClock() - T_ELECT2;
    for (int64_t i = 0; OB_SUCC(ret) && i < OB_REPLICA_NUM; ++i) {
      if (OB_SUCCESS != (election_mgr_[i].set_candidate(partition, OB_REPLICA_NUM, mlist, ++membership_version))) {
        ELECT_ASYNC_LOG(WARN, "set candidate error", K(ret), K(i), K(partition), K(mlist));
      } else if (OB_SUCCESS != (ret = election_mgr_[i].start_partition(partition, leader, cur_ts, leader_epoch))) {
        ELECT_ASYNC_LOG(WARN, "election_mgr start partition error", K(ret), K(i), K(partition), K(leader));
      } else {
        ELECT_ASYNC_LOG(INFO, "election_mgr start partition success", K(i), K(partition), K(mlist), K(leader));
      }
    }
  }

  return ret;
}

int ObTestElectionCluster::remove_partition(const ObPartitionKey& partition)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ELECT_ASYNC_LOG(WARN, "not init");
    ret = OB_NOT_INIT;
  } else if (!partition.is_valid()) {
    ELECT_ASYNC_LOG(WARN, "invalid argument", K(partition));
  } else {
    for (int64_t i = 0; i < OB_REPLICA_NUM; ++i) {
      if (OB_SUCCESS != (ret = election_mgr_[i].remove_partition(partition))) {
        ELECT_ASYNC_LOG(WARN, "remove partition error", K(ret), K(i), K(partition));
      } else {
        ELECT_ASYNC_LOG(INFO, "remove partition success", K(i), K(partition));
      }
    }
  }

  return ret;
}

void TestObElectionCluster::SetUp()
{
  oceanbase::election::ObAsyncLog::getLogger().init("test_election_cluster.log", OB_LOG_LEVEL_INFO, true);
  test_partition_.init(combine_id(1, 3001), 0, 1);
  EXPECT_TRUE(leader_.set_ip_addr(LOCAL_IP, RPC_PORT));
  for (int64_t i = 0; i < OB_REPLICA_NUM; ++i) {
    ObElectionPriority priority(true, OB_REPLICA_NUM, ObVersion(10), OB_REPLICA_NUM - i);
    cluster_.election_cb_[i].init(priority);
  }

  ObElectionPriority priority(true, 1, ObVersion(1), 1);
  cluster_.election_cb_[3].init(priority);

  EXPECT_EQ(OB_SUCCESS, cluster_.init());
  EXPECT_EQ(OB_SUCCESS, cluster_.start());
  EXPECT_EQ(OB_SUCCESS, cluster_.add_partition(test_partition_, leader_));
}

void TestObElectionCluster::TearTown()
{
  cluster_.remove_partition(test_partition_);
  cluster_.destroy();
}

TEST_F(TestObElectionCluster, test_get_leader)
{
  ObAddr addr;
  ObAddr previous_leader;
  int64_t leader_epoch = 0;
  bool unused_bool;
  ELECT_ASYNC_LOG(INFO, "test_get_leader");

  for (int64_t i = 0; i < OB_REPLICA_NUM; ++i) {
    addr.reset();
    EXPECT_EQ(OB_SUCCESS,
        cluster_.election_mgr_[i].get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
    EXPECT_EQ(leader_, addr);
  }
}

TEST_F(TestObElectionCluster, change_leader_not_exist)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  ObAddr next_leader;
  ObAddr previous_leader;
  int64_t leader_epoch = 0;
  bool unused_bool;
  ELECT_ASYNC_LOG(INFO, "change_leader_not_exist");
  sleep(5);

  EXPECT_EQ(OB_SUCCESS,
      cluster_.election_mgr_[0].get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
  EXPECT_EQ(leader_, addr);

  const int32_t idx = addr.get_port() - cluster_.base_rpc_port_;

  EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[1].stop_partition(test_partition_));

  EXPECT_TRUE(next_leader.set_ip_addr(LOCAL_IP, RPC_PORT + 1));
  const int32_t next_idx = 1;
  int64_t old_membership_version = cluster_.election_cb_[next_idx].get_priority().get_membership_version();
  int64_t old_data_version = cluster_.election_cb_[next_idx].get_priority().get_data_version();
  ObTsWindows ts_windows;
  cluster_.election_cb_[next_idx].priority_.set_membership_version(
      cluster_.election_cb_[idx].get_priority().get_membership_version() + 1);
  cluster_.election_cb_[next_idx].priority_.set_data_version(
      cluster_.election_cb_[idx].get_priority().get_data_version());
  if (OB_SUCCESS != (ret = cluster_.election_mgr_[idx].change_leader_async(test_partition_, next_leader, ts_windows))) {
    ELECT_ASYNC_LOG(WARN, "change leader error", K(ret), K_(test_partition), K(next_leader));
  } else {
    sleep(8);
  }
  //  EXPECT_EQ(OB_ELECTION_WARN_INVALID_LEADER, cluster_.election_mgr_[0].get_leader(test_partition_, addr,
  //  previous_leader, leader_epoch, unused_bool));
  cluster_.election_cb_[next_idx].priority_.set_membership_version(old_membership_version);
  cluster_.election_cb_[next_idx].priority_.set_data_version(ObVersion(old_data_version));
}

TEST_F(TestObElectionCluster, change_leader)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  ObAddr next_leader;
  ObAddr previous_leader;
  ObTsWindows ts_windows;
  int64_t leader_epoch = 0;
  bool unused_bool;
  ELECT_ASYNC_LOG(INFO, "change_leader");
  sleep(3);

  EXPECT_EQ(OB_SUCCESS,
      cluster_.election_mgr_[0].get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
  EXPECT_EQ(leader_, addr);

  const int32_t idx = addr.get_port() - cluster_.base_rpc_port_;
  EXPECT_TRUE(next_leader.set_ip_addr(LOCAL_IP, RPC_PORT + 1));
  if (OB_SUCCESS != (ret = cluster_.election_mgr_[idx].change_leader_async(test_partition_, next_leader, ts_windows))) {
    ELECT_ASYNC_LOG(WARN, "change leader error", K(ret), K_(test_partition), K(next_leader));
  } else {
    sleep(3);
  }
  EXPECT_EQ(OB_SUCCESS,
      cluster_.election_mgr_[0].get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
  EXPECT_EQ(addr, next_leader);
}

TEST_F(TestObElectionCluster, test_vote_vote_leader_candidate_false)
{
  ObAddr addr;
  ObAddr previous_leader;
  int64_t leader_epoch = 0;
  bool unused_bool;
  ELECT_ASYNC_LOG(INFO, "test_vote_vote_leader_candidate_false");

  EXPECT_EQ(OB_SUCCESS,
      cluster_.election_mgr_[0].get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
  EXPECT_EQ(leader_, addr);
  const int32_t idx = addr.get_port() - cluster_.base_rpc_port_;
  // printf("idx=%d\n", idx);

  // set leader priority candidate is false
  // cluster_.election_cb_[idx].priority_.set_candidate(false);

  sleep(5);
  EXPECT_EQ(OB_SUCCESS,
      cluster_.election_mgr_[idx].get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
  EXPECT_EQ(leader_, addr);

  cluster_.election_cb_[idx].priority_.set_candidate(true);
}

//  A(A, B, C)
//  B(A, B, C)
//  C(A, B, C)
//  D(A, B, C)
TEST_F(TestObElectionCluster, test_query_not_in_member)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  ObAddr previous_leader;
  ObMemberList mlist;
  int64_t leader_epoch = 0;
  ObIElection* unused = NULL;
  ELECT_ASYNC_LOG(INFO, "test_query_not_in_member");
  ObIElectionMgr* query = &cluster_.election_mgr_[3];
  ObIElectionCallback* query_election_cb = &cluster_.election_cb_[3];

  for (int32_t i = 0; OB_SUCC(ret) && i < OB_REPLICA_NUM; ++i) {
    if (!(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + i))) {
      ELECT_ASYNC_LOG(WARN, "set addr error", "ip", cluster_.ip_, "port", cluster_.base_rpc_port_ + i);
    } else {
      if (OB_SUCCESS != (ret = mlist.add_server(addr))) {
        ELECT_ASYNC_LOG(WARN, "mlist add server error", K(ret), K(mlist), K(addr));
      } else {
        ELECT_ASYNC_LOG(INFO, "mlist add server", K(mlist), K(addr));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = query->add_partition(test_partition_, OB_REPLICA_NUM, query_election_cb, unused))) {
      ELECT_ASYNC_LOG(WARN, "add partition error", K(ret), K_(test_partition));
    } else if (OB_SUCCESS !=
               (ret = query->set_candidate(test_partition_, OB_REPLICA_NUM, mlist, ++membership_version))) {
      ELECT_ASYNC_LOG(WARN, "set candidate error", K(ret), K_(test_partition), K(mlist));
    } else if (OB_SUCCESS != (ret = query->start_partition(test_partition_))) {
      ELECT_ASYNC_LOG(WARN, "start candidate error", K(ret), K_(test_partition));
    } else {
      ELECT_ASYNC_LOG(INFO, "query start partition success", K_(test_partition), K(mlist));
    }
  }

  sleep(10);
  bool unused_bool;
  for (int64_t i = 0; i < 10; ++i) {
    addr.reset();
    EXPECT_EQ(OB_SUCCESS, query->get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
    EXPECT_EQ(leader_, addr);
    sleep(1);
  }
  EXPECT_EQ(OB_SUCCESS, query->remove_partition(test_partition_));
}

// A(A, B, C)
// B(A, B, C)
// C(A, B, C)
// D(B, C, D)
TEST_F(TestObElectionCluster, test_query_not_in_leader_member_and_without_leader)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  ObMemberList mlist;
  ObAddr previous_leader;
  int64_t leader_epoch = 0;
  bool unused_bool;
  ObIElection* unused = NULL;
  ELECT_ASYNC_LOG(INFO, "test_query_not_in_leader_member_and_without_leader");

  ObIElectionMgr* query = &cluster_.election_mgr_[3];
  ObIElectionCallback* query_election_cb = &cluster_.election_cb_[3];

  for (int32_t i = 1; OB_SUCC(ret) && i < MAX_OB_TRANS_SERVICE_NUM; ++i) {
    if (!(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + i))) {
      ELECT_ASYNC_LOG(WARN, "set addr error", "ip", cluster_.ip_, "port", cluster_.base_rpc_port_ + i);
    } else {
      EXPECT_EQ(OB_SUCCESS, mlist.add_server(addr));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = query->add_partition(test_partition_, OB_REPLICA_NUM, query_election_cb, unused))) {
      ELECT_ASYNC_LOG(WARN, "add partition error", K(ret), K_(test_partition));
    } else if (OB_SUCCESS !=
               (ret = query->set_candidate(test_partition_, OB_REPLICA_NUM, mlist, ++membership_version))) {
      ELECT_ASYNC_LOG(WARN, "set candidate error", K(ret), K_(test_partition), K(mlist));
    } else if (OB_SUCCESS != (ret = query->start_partition(test_partition_))) {
      ELECT_ASYNC_LOG(WARN, "start candidate error", K(ret), K_(test_partition));
    } else {
      ELECT_ASYNC_LOG(INFO, "query start partition success", K_(test_partition), K(mlist));
    }
  }

  sleep(10);
  for (int64_t i = 0; i < 10; ++i) {
    addr.reset();
    EXPECT_EQ(OB_SUCCESS, query->get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
    EXPECT_EQ(leader_, addr);
    sleep(1);
  }
  EXPECT_EQ(OB_SUCCESS, query->remove_partition(test_partition_));
}

// A(A, B, C)
// B(A, B, C)
// C(A, B, C)
// D(A, B, D)

TEST_F(TestObElectionCluster, test_query_leader_and_with_leader_and_self)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  ObAddr previous_leader;
  ObMemberList mlist;
  int64_t leader_epoch = 0;
  ObIElection* unused = NULL;
  ELECT_ASYNC_LOG(INFO, "test_query_leader_and_with_leader_and_self");

  ObIElectionMgr* query = &cluster_.election_mgr_[3];
  ObIElectionCallback* query_election_cb = &cluster_.election_cb_[3];

  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_OB_TRANS_SERVICE_NUM; ++i) {
    if (i == 2) {  // skip
      continue;
    }
    if (!(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + (int32_t)i))) {
      ELECT_ASYNC_LOG(WARN, "set addr error", "ip", cluster_.ip_, "port", cluster_.base_rpc_port_ + i);
    } else {
      if (OB_SUCCESS != (ret = mlist.add_server(addr))) {
        ELECT_ASYNC_LOG(WARN, "mlist add server error", K(ret), K(mlist), K(addr));
      } else {
        ELECT_ASYNC_LOG(INFO, "mlist add server", K(mlist), K(addr));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = query->add_partition(test_partition_, OB_REPLICA_NUM, query_election_cb, unused))) {
      ELECT_ASYNC_LOG(WARN, "add partition error", K(ret), K_(test_partition));
    } else if (OB_SUCCESS !=
               (ret = query->set_candidate(test_partition_, OB_REPLICA_NUM, mlist, ++membership_version))) {
      ELECT_ASYNC_LOG(WARN, "set candidate error", K(ret), K_(test_partition), K(mlist));
    } else if (OB_SUCCESS != (ret = query->start_partition(test_partition_))) {
      ELECT_ASYNC_LOG(WARN, "start candidate error", K(ret), K_(test_partition));
    } else {
      ELECT_ASYNC_LOG(INFO, "query start partition success", K_(test_partition), K(mlist));
    }
  }

  sleep(10);
  bool unused_bool;
  for (int64_t i = 0; i < 10; ++i) {
    addr.reset();
    EXPECT_EQ(OB_SUCCESS, query->get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
    EXPECT_EQ(leader_, addr);
    sleep(1);
  }
  EXPECT_EQ(OB_SUCCESS, query->remove_partition(test_partition_));
}

// A(A, B, C)
// B(A, B, C)
// C(A, B, C)
// D(C, D)

TEST_F(TestObElectionCluster, test_query_leader_and_without_leader)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  ObAddr previous_leader;
  ObMemberList mlist;
  int64_t leader_epoch = 0;
  ObIElection* unused = NULL;
  ELECT_ASYNC_LOG(INFO, "test_query_leader_and_without_leader");

  ObIElectionMgr* query = &cluster_.election_mgr_[3];
  ObIElectionCallback* query_election_cb = &cluster_.election_cb_[3];

  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_OB_TRANS_SERVICE_NUM; ++i) {
    if (i == 0 || i == 1) {  // skip
      continue;
    }
    if (!(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + (int32_t)i))) {
      ELECT_ASYNC_LOG(WARN, "set addr error", "ip", cluster_.ip_, "port", cluster_.base_rpc_port_ + i);
    } else {
      if (OB_SUCCESS != (ret = mlist.add_server(addr))) {
        ELECT_ASYNC_LOG(WARN, "mlist add server error", K(ret), K(mlist), K(addr));
      } else {
        ELECT_ASYNC_LOG(INFO, "mlist add server", K(mlist), K(addr));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = query->add_partition(test_partition_, OB_REPLICA_NUM, query_election_cb, unused))) {
      ELECT_ASYNC_LOG(WARN, "add partition error", K(ret), K_(test_partition));
    } else if (OB_SUCCESS !=
               (ret = query->set_candidate(test_partition_, OB_REPLICA_NUM, mlist, ++membership_version))) {
      ELECT_ASYNC_LOG(WARN, "set candidate error", K(ret), K_(test_partition), K(mlist));
    } else if (OB_SUCCESS != (ret = query->start_partition(test_partition_))) {
      ELECT_ASYNC_LOG(WARN, "start candidate error", K(ret), K_(test_partition));
    } else {
      ELECT_ASYNC_LOG(INFO, "query start partition success", K_(test_partition), K(mlist));
    }
  }

  sleep(10);
  bool unused_bool;
  for (int64_t i = 0; i < 10; ++i) {
    addr.reset();
    EXPECT_EQ(OB_SUCCESS, query->get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
    EXPECT_EQ(leader_, addr);
    sleep(1);
  }
  EXPECT_EQ(OB_SUCCESS, query->remove_partition(test_partition_));
}

// A(A, B, C)
// B(A, B, C)
// C(A, B, C)
// D(B, C)

TEST_F(TestObElectionCluster, test_query_leader_and_without_leader_and_self)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  ObAddr previous_leader;
  ObMemberList mlist;
  int64_t leader_epoch = 0;
  ObIElection* unused = NULL;
  ObIElectionMgr* query = &cluster_.election_mgr_[3];
  ELECT_ASYNC_LOG(INFO, "test_query_leader_and_without_leader_and_self");

  ObIElectionCallback* query_election_cb = &cluster_.election_cb_[3];
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_OB_TRANS_SERVICE_NUM; ++i) {
    if (i == 0 || i == 3) {  // skip
      continue;
    }
    if (!(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + (int32_t)i))) {
      ELECT_ASYNC_LOG(WARN, "set addr error", "ip", cluster_.ip_, "port", cluster_.base_rpc_port_ + i);
    } else {
      if (OB_SUCCESS != (ret = mlist.add_server(addr))) {
        ELECT_ASYNC_LOG(WARN, "mlist add server error", K(ret), K(mlist), K(addr));
      } else {
        ELECT_ASYNC_LOG(INFO, "mlist add server", K(mlist), K(addr));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = query->add_partition(test_partition_, OB_REPLICA_NUM, query_election_cb, unused))) {
      ELECT_ASYNC_LOG(WARN, "add partition error", K(ret), K_(test_partition));
    } else if (OB_SUCCESS !=
               (ret = query->set_candidate(test_partition_, OB_REPLICA_NUM, mlist, ++membership_version))) {
      ELECT_ASYNC_LOG(WARN, "set candidate error", K(ret), K_(test_partition), K(mlist));
    } else if (OB_SUCCESS != (ret = query->start_partition(test_partition_))) {
      ELECT_ASYNC_LOG(WARN, "start candidate error", K(ret), K_(test_partition));
    } else {
      ELECT_ASYNC_LOG(INFO, "query start partition success", K_(test_partition), K(mlist));
    }
  }

  sleep(10);
  bool unused_bool;
  for (int64_t i = 0; i < 10; ++i) {
    addr.reset();
    EXPECT_EQ(OB_SUCCESS, query->get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
    EXPECT_EQ(leader_, addr);
    sleep(1);
  }
  EXPECT_EQ(OB_SUCCESS, query->remove_partition(test_partition_));
}

// A(A, B, C)
// B(A, B, C)
// C(A, B, C)
// D(A, B, C)

TEST_F(TestObElectionCluster, test_query_leader_and_without_self)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  ObAddr previous_leader;
  ObMemberList mlist;
  int64_t leader_epoch = 0;
  ObIElection* unused = NULL;
  ELECT_ASYNC_LOG(INFO, "test_query_leader_and_without_self");

  ObIElectionMgr* query = &cluster_.election_mgr_[3];
  ObIElectionCallback* query_election_cb = &cluster_.election_cb_[3];

  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_OB_TRANS_SERVICE_NUM; ++i) {
    if (i == 2) {  // skip
      continue;
    }
    if (!(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + (int32_t)i))) {
      ELECT_ASYNC_LOG(WARN, "set addr error", "ip", cluster_.ip_, "port", cluster_.base_rpc_port_ + i);
    } else {
      if (OB_SUCCESS != (ret = mlist.add_server(addr))) {
        ELECT_ASYNC_LOG(WARN, "mlist add server error", K(ret), K(mlist), K(addr));
      } else {
        ELECT_ASYNC_LOG(INFO, "mlist add server", K(mlist), K(addr));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = query->add_partition(test_partition_, OB_REPLICA_NUM, query_election_cb, unused))) {
      ELECT_ASYNC_LOG(WARN, "add partition error", K(ret), K_(test_partition));
    } else if (OB_SUCCESS !=
               (ret = query->set_candidate(test_partition_, OB_REPLICA_NUM, mlist, ++membership_version))) {
      ELECT_ASYNC_LOG(WARN, "set candidate error", K(ret), K_(test_partition), K(mlist));
    } else if (OB_SUCCESS != (ret = query->start_partition(test_partition_))) {
      ELECT_ASYNC_LOG(WARN, "start candidate error", K(ret), K_(test_partition));
    } else {
      ELECT_ASYNC_LOG(INFO, "query start partition success", K_(test_partition), K(mlist));
    }
  }

  sleep(10);
  bool unused_bool;
  for (int64_t i = 0; i < 10; ++i) {
    addr.reset();
    EXPECT_EQ(OB_SUCCESS, query->get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
    EXPECT_EQ(leader_, addr);
    sleep(1);
  }
  EXPECT_EQ(OB_SUCCESS, query->remove_partition(test_partition_));
}

// A(A, D)
// D(B, C)

TEST_F(TestObElectionCluster, query_vote_leader_no_query)
{
  int ret = OB_SUCCESS;
  ObAddr leader;
  ObAddr addr;
  ObAddr next_leader;
  ObAddr previous_leader;
  int64_t leader_epoch = 0;
  const int32_t idx = 0;
  ObIElection* unused = NULL;
  ELECT_ASYNC_LOG(INFO, "query_vote_leader_no_query");

  for (int64_t i = 0; OB_SUCC(ret) && i < OB_REPLICA_NUM; ++i) {
    if (i != idx) {
      EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[i].remove_partition(test_partition_));
    }
  }

  sleep(10);
  bool unused_bool;
  EXPECT_EQ(OB_ELECTION_WARN_INVALID_LEADER,
      cluster_.election_mgr_[idx].get_leader(test_partition_, leader, previous_leader, leader_epoch, unused_bool));

  ObMemberList mlist;
  ObIElectionMgr* tmp_replica = &cluster_.election_mgr_[3];

  EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + idx));
  EXPECT_EQ(OB_SUCCESS, mlist.add_server(addr));
  EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + 3));
  EXPECT_EQ(OB_SUCCESS, mlist.add_server(addr));
  cluster_.election_mgr_[idx].set_candidate(test_partition_, OB_REPLICA_NUM, mlist, ++membership_version);

  mlist.reset();
  EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + 1));
  mlist.add_server(addr);
  EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + 2));
  mlist.add_server(addr);

  ObElectionPriority priority(false, 1, ObVersion(1), 1);
  cluster_.election_cb_[3].init(priority);
  ObIElectionCallback* election_cb = &cluster_.election_cb_[3];

  if (OB_SUCCESS != (ret = tmp_replica->add_partition(test_partition_, OB_REPLICA_NUM, election_cb, unused))) {
    ELECT_ASYNC_LOG(WARN, "add partition error", K(ret), K_(test_partition));
  } else if (OB_SUCCESS !=
             (ret = tmp_replica->set_candidate(test_partition_, OB_REPLICA_NUM, mlist, ++membership_version))) {
    ELECT_ASYNC_LOG(WARN, "set candidate error", K(ret), K_(test_partition), K(mlist));
  } else if (OB_SUCCESS != (ret = tmp_replica->start_partition(test_partition_))) {
    ELECT_ASYNC_LOG(WARN, "start candidate error", K(ret), K_(test_partition));
  } else {
    ELECT_ASYNC_LOG(INFO, "query start partition success", K_(test_partition), K(mlist));
  }

  sleep(10);
  leader.reset();
  EXPECT_EQ(OB_SUCCESS, tmp_replica->get_leader(test_partition_, leader, previous_leader, leader_epoch, unused_bool));
  leader.reset();
  EXPECT_EQ(OB_SUCCESS,
      cluster_.election_mgr_[idx].get_leader(test_partition_, leader, previous_leader, leader_epoch, unused_bool));
  EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + idx));
  EXPECT_EQ(leader, addr);

  sleep(15);
  for (int64_t i = 0; i < 10; ++i) {
    leader.reset();
    EXPECT_EQ(OB_SUCCESS,
        cluster_.election_mgr_[idx].get_leader(test_partition_, leader, previous_leader, leader_epoch, unused_bool));
    leader.reset();
    EXPECT_EQ(OB_SUCCESS, tmp_replica->get_leader(test_partition_, leader, previous_leader, leader_epoch, unused_bool));
    EXPECT_EQ(leader, addr);
    sleep(1);
  }

  EXPECT_EQ(OB_SUCCESS, tmp_replica->remove_partition(test_partition_));
}

// A(A, B)
// B(A, B)
// D(B, C)

// A (A, D)
// D (B, C)

TEST_F(TestObElectionCluster, query_vote_leader_need_query_to_without_query)
{
  int ret = OB_SUCCESS;
  ObAddr leader;
  ObAddr addr;
  ObAddr next_leader;
  ObAddr previous_leader;
  int64_t leader_epoch = 0;
  const int32_t idx = 0;
  ObIElection* unused = NULL;
  ELECT_ASYNC_LOG(INFO, "query_vote_leader_need_query_to_without_query");

  EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[2].remove_partition(test_partition_));

  ObIElectionMgr* tmp_replica = &cluster_.election_mgr_[3];
  ObMemberList mlist;
  EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + idx));
  EXPECT_EQ(OB_SUCCESS, mlist.add_server(addr));
  EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + 1));
  EXPECT_EQ(OB_SUCCESS, mlist.add_server(addr));
  cluster_.election_mgr_[idx].set_candidate(test_partition_, OB_REPLICA_NUM, mlist, ++membership_version);
  cluster_.election_mgr_[idx + 1].set_candidate(test_partition_, OB_REPLICA_NUM, mlist, ++membership_version);

  mlist.reset();
  EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + 1));
  mlist.add_server(addr);
  EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + 2));
  mlist.add_server(addr);

  ObElectionPriority priority(false, 1, ObVersion(1), 1);
  cluster_.election_cb_[3].init(priority);
  ObIElectionCallback* election_cb = &cluster_.election_cb_[3];

  if (OB_SUCCESS != (ret = tmp_replica->add_partition(test_partition_, OB_REPLICA_NUM, election_cb, unused))) {
    ELECT_ASYNC_LOG(WARN, "add partition error", K(ret), K_(test_partition));
  } else if (OB_SUCCESS !=
             (ret = tmp_replica->set_candidate(test_partition_, OB_REPLICA_NUM, mlist, ++membership_version))) {
    ELECT_ASYNC_LOG(WARN, "set candidate error", K(ret), K_(test_partition), K(mlist));
  } else if (OB_SUCCESS != (ret = tmp_replica->start_partition(test_partition_))) {
    ELECT_ASYNC_LOG(WARN, "start candidate error", K(ret), K_(test_partition));
  } else {
    ELECT_ASYNC_LOG(INFO, "query start partition success", K_(test_partition), K(mlist));
  }

  sleep(10);
  leader.reset();
  bool unused_bool;
  EXPECT_EQ(OB_SUCCESS,
      cluster_.election_mgr_[idx].get_leader(test_partition_, leader, previous_leader, leader_epoch, unused_bool));
  EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + idx));
  EXPECT_EQ(leader, addr);

  sleep(15);
  for (int64_t i = 0; i < 10; ++i) {
    leader.reset();
    EXPECT_EQ(OB_SUCCESS,
        cluster_.election_mgr_[idx].get_leader(test_partition_, leader, previous_leader, leader_epoch, unused_bool));
    EXPECT_EQ(OB_SUCCESS, tmp_replica->get_leader(test_partition_, leader, previous_leader, leader_epoch, unused_bool));
    EXPECT_EQ(leader, addr);
    sleep(1);
  }

  EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[1].remove_partition(test_partition_));

  sleep(10);
  EXPECT_EQ(OB_ELECTION_WARN_INVALID_LEADER,
      cluster_.election_mgr_[idx].get_leader(test_partition_, leader, previous_leader, leader_epoch, unused_bool));

  mlist.reset();
  EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + idx));
  EXPECT_EQ(OB_SUCCESS, mlist.add_server(addr));
  EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + 3));
  EXPECT_EQ(OB_SUCCESS, mlist.add_server(addr));
  cluster_.election_mgr_[idx].set_candidate(test_partition_, OB_REPLICA_NUM, mlist, ++membership_version);

  sleep(15);
  for (int64_t i = 0; i < 1; ++i) {
    leader.reset();
    EXPECT_EQ(OB_SUCCESS,
        cluster_.election_mgr_[idx].get_leader(test_partition_, leader, previous_leader, leader_epoch, unused_bool));
    EXPECT_TRUE(addr.set_ip_addr(cluster_.ip_, cluster_.base_rpc_port_ + idx));
    EXPECT_EQ(leader, addr);
    sleep(1);
  }
  EXPECT_EQ(OB_SUCCESS, tmp_replica->get_leader(test_partition_, leader, previous_leader, leader_epoch, unused_bool));
  EXPECT_EQ(OB_SUCCESS, tmp_replica->remove_partition(test_partition_));
  EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[idx].remove_partition(test_partition_));
}

// TEST_F(TestObElectionCluster, send_vote_vote_message_loss)
//{
// int ret = OB_SUCCESS;
// ObAddr addr;
// ObAddr next_leader;
// int64_t leader_epoch = 0;

// ELECT_ASYNC_LOG(INFO, "send_vote_vote_message_loss");
// EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[0].get_leader(test_partition_, addr, leader_epoch));
// EXPECT_EQ(leader_, addr);

// const int32_t idx = addr.get_port() - cluster_.base_rpc_port_;

// EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[2].stop_partition(test_partition_));

// cluster_.rpc_loss_[OB_ELECTION_VOTE_VOTE].times_ = 1;
// EXPECT_TRUE(next_leader.set_ip_addr(LOCAL_IP, RPC_PORT + 1));
// int32_t next_idx = next_leader.get_port() - cluster_.base_rpc_port_;
// int64_t old_membership_version = cluster_.election_cb_[next_idx].get_priority().get_membership_version();
// int64_t old_data_version = cluster_.election_cb_[next_idx].get_priority().get_data_version();
// cluster_.election_cb_[next_idx].priority_.set_membership_version(cluster_.election_cb_[idx].get_priority().get_membership_version()
// + 1);
// cluster_.election_cb_[next_idx].priority_.set_data_version(cluster_.election_cb_[idx].get_priority().get_data_version());
// if (OB_SUCCESS != (ret = cluster_.election_mgr_[idx].change_leader_async(test_partition_, next_leader))) {
// ELECT_ASYNC_LOG(WARN, "change leader error", K(ret), K_(test_partition), K(next_leader));
//} else {
// sleep(6);
//}
// EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[0].get_leader(test_partition_, addr, leader_epoch));
// EXPECT_EQ(next_leader, addr);
// EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[next_idx].leader_revoke(test_partition_));
// EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[2].start_partition(test_partition_));
// cluster_.election_cb_[next_idx].priority_.set_membership_version(old_membership_version);
// cluster_.election_cb_[next_idx].priority_.set_data_version(ObVersion(old_data_version));
//}

TEST_F(TestObElectionCluster, test_get_valid_candidate)
{
  ObAddr addr;
  ObAddr previous_leader;
  int64_t leader_epoch = 0;
  bool unused_bool;

  ELECT_ASYNC_LOG(INFO, "test_get_valid_candiadte");

  sleep(5);
  EXPECT_EQ(OB_SUCCESS,
      cluster_.election_mgr_[0].get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
  EXPECT_EQ(leader_, addr);

  const int32_t idx = addr.get_port() - cluster_.base_rpc_port_;
  ObMemberList valid_list;
  ObMemberList curr_list;
  EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[idx].get_curr_candidate(test_partition_, curr_list));
  EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[idx].get_valid_candidate(test_partition_, valid_list));

  EXPECT_EQ(OB_REPLICA_NUM, valid_list.get_member_number());
  EXPECT_EQ(valid_list.get_member_number(), curr_list.get_member_number());
  for (int64_t index = 0; index < curr_list.get_member_number(); ++index) {
    ObAddr addr;
    EXPECT_EQ(OB_SUCCESS, curr_list.get_server_by_index(index, addr));
    EXPECT_TRUE(valid_list.contains(addr));
  }
}

TEST_F(TestObElectionCluster, force_leader)
{
  ObAddr addr;
  ObAddr previous_leader;
  int64_t leader_epoch = 0;
  bool unused_bool;
  EXPECT_EQ(OB_SUCCESS,
      cluster_.election_mgr_[0].get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));
  EXPECT_EQ(leader_, addr);
  const int32_t idx = addr.get_port() - cluster_.base_rpc_port_;
  const uint32_t revoke_type = ObElection::RevokeType::LEASE_EXPIRED;
  EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[idx].leader_revoke(test_partition_, revoke_type));
  cluster_.rpc_loss_[OB_ELECTION_DEVOTE_PREPARE].times_ = 1000;
  cluster_.rpc_loss_[OB_ELECTION_DEVOTE_VOTE].times_ = 1000;
  cluster_.rpc_loss_[OB_ELECTION_DEVOTE_SUCCESS].times_ = 1000;
  sleep(10);
  cluster_.is_force_leader_ = true;
  EXPECT_EQ(OB_SUCCESS, cluster_.election_mgr_[2].force_leader_async(test_partition_));
  sleep(15);
  EXPECT_EQ(OB_SUCCESS,
      cluster_.election_mgr_[0].get_leader(test_partition_, addr, previous_leader, leader_epoch, unused_bool));

  //  EXPECT_EQ(addr, cluster_.election_mgr_[2].addr_);
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  int ret = -1;

  oceanbase::election::ObAsyncLog::getLogger().init("test_election_cluster.log", OB_LOG_LEVEL_INFO, true);
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_1500);
  EXPECT_EQ(OB_SUCCESS, ObTenantMutilAllocatorMgr::get_instance().init());

  if (OB_FAIL(oceanbase::common::ObClockGenerator::init())) {
    ELECT_ASYNC_LOG(WARN, "clock generator init error.", K(ret));
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }

  return ret;
}
