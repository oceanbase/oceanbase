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


#include "lib/list/ob_dlist.h"
#include "logservice/palf/election/interface/election_msg_handler.h"
#include <algorithm>
#include <chrono>
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "logservice/palf/election/interface/election.h"
#include "logservice/palf/log_meta_info.h"
#define UNITTEST
#include "logservice/palf/election/utils/election_common_define.h"
#include "logservice/palf/election/algorithm/election_impl.h"
#include "logservice/leader_coordinator/election_priority_impl/election_priority_impl.h"
#include "share/ob_occam_timer.h"
#include "share/rc/ob_tenant_base.h"
#include "mock_logservice_container/mock_election_user.h"
#include <iostream>
#include <vector>

using namespace oceanbase::obrpc;
using namespace std;

#define SUCC_(stmt) ASSERT_EQ((stmt), OB_SUCCESS)
#define FAIL_(stmt) ASSERT_EQ((stmt), OB_FAIL)
#define TRUE_(stmt) ASSERT_EQ((stmt), true)
#define FALSE_(stmt) ASSERT_EQ((stmt), false)

namespace oceanbase {
namespace palf
{
namespace election
{
int EventRecorder::report_event_(ElectionEventType, const common::ObString &)
{
  return OB_SUCCESS;
}
}
}
namespace unittest {

using namespace common;
using namespace palf::election;
using namespace std;
using namespace logservice::coordinator;

int64_t MSG_DELAY = 1_ms;
std::atomic_int leader_takeover_times(0);
std::atomic_int leader_revoke_times(0);
std::atomic_int devote_to_be_leader_count(0);
std::atomic_int lease_expired_to_be_follower_count(0);
std::atomic_int change_leader_to_be_leader_count(0);
std::atomic_int change_leader_to_be_follower_count(0);
std::atomic_int stop_to_be_follower_count(0);
MockNetService GlobalNetService;
ObOccamThreadPool thread_pool;
ObOccamTimer timer;

void reset_global_status()
{
  leader_takeover_times = 0;
  leader_revoke_times = 0;
  devote_to_be_leader_count = 0;
  lease_expired_to_be_follower_count = 0;
  change_leader_to_be_leader_count = 0;
  change_leader_to_be_follower_count = 0;
  stop_to_be_follower_count = 0;
}

class TestElection : public ::testing::Test {
public:
  TestElection() {}
  ~TestElection() {}
  virtual void SetUp() {
    MAX_TST = 750_ms;
    share::ObTenantSwitchGuard guard;
    guard.switch_to(OB_SYS_TENANT_ID);
    GlobalNetService.clear();
    reset_global_status();
    new(&thread_pool) ObOccamThreadPool();
    new(&timer) ObOccamTimer();
    thread_pool.init_and_start(5);
    timer.init_and_start(thread_pool, 1_ms, "timer");
  }
  virtual void TearDown() { timer.~ObOccamTimer(); thread_pool.~ObOccamThreadPool(); }
};

template <typename TAKEOVER_OP>
vector<ElectionImpl *> create_election_group(const int election_num, const vector<uint64_t> &priority_seed, TAKEOVER_OP &&op)
{
  if (priority_seed.size() != election_num) {
    abort();
  }
  vector<ElectionImpl *> v;

  MemberList member_list;
  ObArray<ObAddr> addr_list;
  static int port = 1;
  for (int i = 0; i < election_num; ++i)
    addr_list.push_back(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", port + i));
  palf::LogConfigVersion version;
  version.proposal_id_ = 1;
  version.config_seq_ = 1;
  member_list.set_new_member_list(addr_list, version, election_num);

  int ret = OB_SUCCESS;
  for (int i = 0; i < election_num; ++i) {
    ElectionImpl *election = new ElectionImpl();
    election->self_addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", port + i);
    v.push_back(election);
  }
  for (auto &election_1 : v) {
    for (auto &election_2 : v) {
      GlobalNetService.connect(election_1, election_2);
    }
  }
  int index = 0;
  for (auto &election : v) {
    ret = election->init_and_start(
      1,
      &timer,
      &GlobalNetService,
      ObAddr(ObAddr::VER::IPV4, "127.0.0.1", port + index),
      priority_seed[index],
      1,
      [&election](const int64_t, const ObAddr &dest_addr) {
        return THREAD_POOL.commit_task_ignore_ret([&election, dest_addr]() {
          election->change_leader_to(dest_addr);
        });
      },
      [op, ret](Election *election, ObRole before, ObRole after, RoleChangeReason reason) {
        if (before == ObRole::FOLLOWER && after == ObRole::LEADER) {
          ELECT_LOG(INFO, "i become LEADER", K(obj_to_string(reason)), KPC(election));
          op();
        } else if (before == ObRole::LEADER && after == ObRole::FOLLOWER) {
          ELECT_LOG(INFO, "i become FOLLOWER", K(obj_to_string(reason)), KPC(election));
        } else {
          ELECT_LOG(ERROR, "i don't know why call me");
        }
        switch (reason) {
          case RoleChangeReason::DevoteToBeLeader:
            devote_to_be_leader_count++;
            leader_takeover_times++;
            break;
          case RoleChangeReason::LeaseExpiredToRevoke:
            lease_expired_to_be_follower_count++;
            leader_revoke_times++;
            break;
          case RoleChangeReason::ChangeLeaderToBeLeader:
            change_leader_to_be_leader_count++;
            leader_takeover_times++;
            break;
          case RoleChangeReason::ChangeLeaderToRevoke:
            change_leader_to_be_follower_count++;
            leader_revoke_times++;
            break;
          case RoleChangeReason::StopToRevoke:
            stop_to_be_follower_count++;
            leader_revoke_times++;
            break;
          default:
            ELECT_LOG(ERROR, "should not go here");
        }
      }
    );
    index++;
    OB_ASSERT(ret == OB_SUCCESS);
    ret = election->set_memberlist(member_list);
    OB_ASSERT(ret == OB_SUCCESS);
  }
  port += election_num;
  return v;
}

TEST_F(TestElection, 3replica_disconnect_leader) {
  auto election_list = create_election_group(3, {0, 0, 0}, [](){});
  this_thread::sleep_for(chrono::seconds(7));

  ELECT_LOG(INFO, "disconnect leader");
  GlobalNetService.disconnect_two_side(election_list[0], election_list[1]);
  GlobalNetService.disconnect_two_side(election_list[0], election_list[2]);

  this_thread::sleep_for(chrono::seconds(14));

  for (auto iter = election_list.rbegin(); iter != election_list.rend(); ++iter)
    (*iter)->stop();
  this_thread::sleep_for(chrono::seconds(2));
  for (auto &election_ : election_list)
    delete election_;

  ASSERT_EQ(leader_takeover_times, 2);
  ASSERT_EQ(leader_revoke_times, 2);
  ASSERT_EQ(devote_to_be_leader_count, 2);
  ASSERT_EQ(lease_expired_to_be_follower_count, 1);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 1);
}

void calculate_and_print_max_min_average(const vector<int64_t> &leader_takeover_time,
                                         const vector<int64_t> &disconnect_leader_time,
                                         int64_t &min,
                                         int64_t &max,
                                         int64_t &average)
{
  vector<int64_t> diffs;
  int group_size = leader_takeover_time.size();
  for (int i = 0; i < group_size; ++i) {
    diffs.push_back(leader_takeover_time[i] - disconnect_leader_time[i]);
  }
  int64_t min_idx = 0, max_idx = 0;
  for (int i = 0; i < group_size; ++i) {
    if (diffs[i] > max) {
      max = diffs[i];
      max_idx = i;
    }
    if (diffs[i] < min) {
      min = diffs[i];
      min_idx = i;
    }
    average += diffs[i];
  }
  average /= group_size;
  ELECT_LOG(INFO, "no leader info summary", K(min), K(min_idx), K(max), K(max_idx), K(average));
}

// TEST_F(TestElection, test_no_leader_time_with_400ms_msg_delay) {
//   MAX_TST = 1_s;
//   MSG_DELAY = 900_ms;
//   vector<vector<ElectionImpl *>> all_election_group;
//   vector<int64_t> disconnect_leader_time;
//   vector<int64_t> leader_takeover_time;
//   int64_t group_size = 50;
//   disconnect_leader_time.resize(group_size);
//   leader_takeover_time.resize(group_size);
//   for (int i = 0; i < group_size; ++i) {// 创建多个paxos组，并行测试以节约时间
//     int64_t *leader_takeover_ts = &leader_takeover_time[i];
//     auto op = [leader_takeover_ts](){ *leader_takeover_ts = get_monotonic_ts(); };
//     all_election_group.push_back(create_election_group(3, op));
//   }

//   this_thread::sleep_for(chrono::seconds(10));// 等待选出所有group的第一任Leader
//   ASSERT_EQ(leader_takeover_times, group_size);
//   for (int i = 0; i < group_size; ++i) {// 将这些group的leader网络断联
//     this_thread::sleep_for(std::chrono::microseconds(MAX_TST / group_size));
//     ELECT_LOG(INFO, "disconnect leader", K(i), KPC(all_election_group[i][0]));
//     disconnect_leader_time[i] = get_monotonic_ts();
//     GlobalNetService.disconnect_two_side(all_election_group[i][0], all_election_group[i][1]);
//     GlobalNetService.disconnect_two_side(all_election_group[i][0], all_election_group[i][2]);
//   }

//   this_thread::sleep_for(chrono::seconds(10));// 等待选出所有group的新Leader

//   for (auto &group : all_election_group)
//     for (auto &election : group)
//       election->stop();
//   this_thread::sleep_for(chrono::seconds(5));// 等待网络中的消息处理完毕
//   for (auto &group : all_election_group)
//     for (auto &election : group)
//       delete election;
    
//   ASSERT_EQ(leader_takeover_times, 2 * group_size);
//   ASSERT_EQ(leader_revoke_times, 2 * group_size);
//   ASSERT_EQ(devote_to_be_leader_count, 2 * group_size);
//   ASSERT_EQ(lease_expired_to_be_follower_count, 1 * group_size);
//   ASSERT_EQ(change_leader_to_be_leader_count, 0);
//   ASSERT_EQ(change_leader_to_be_follower_count, 0);
//   ASSERT_EQ(stop_to_be_follower_count, 1 * group_size);

//   int64_t min = INT64_MAX, max = 0, average = 0;
//   calculate_and_print_max_min_average(leader_takeover_time, disconnect_leader_time,
//                                       min, max, average);
//   // see:
//   int64_t expected_min = 4 * MAX_TST + 3 * MSG_DELAY - (0.5 * MAX_TST - MSG_DELAY);
//   int64_t expected_max = 4 * (MAX_TST + MSG_DELAY);
//   int64_t expected_average = 4 * MAX_TST + 3.5 * MSG_DELAY - 0.5 * (0.5 * MAX_TST - MSG_DELAY);
//   // 计算偏差率
//   double min_diff_percentage = std::abs(expected_min - min) * 100.0 / expected_min;
//   double max_diff_percentage = std::abs(expected_max - max) * 100.0 / expected_max;
//   double average_diff_percentage = std::abs(expected_average - average) * 100.0 / expected_average;
//   char print_buf[1024] = {0};
//   int64_t pos = 0;
//   databuff_printf(print_buf, 1024, pos, "min_diff_percentage=%.2f%%, max_diff_percentage=%.2f%%, average_diff_percentage=%.2f%%", min_diff_percentage, max_diff_percentage, average_diff_percentage);
//   ELECT_LOG(INFO, "diff percentage summary", K(print_buf));
//   // 测试值与理论计算公式的预期值偏差在5%以内(实际上误差在0.5%以内，但是为了单测稳定把阈值调大)
//   ASSERT_NEAR(min, expected_min, expected_min * 0.05);
//   ASSERT_NEAR(max, expected_max, expected_max * 0.05);
//   ASSERT_NEAR(average, expected_average, expected_average * 0.05);
//   MSG_DELAY = 1_ms;
// }

// TEST_F(TestElection, test_no_leader_time_with_1ms_msg_delay) {
//   MAX_TST = 1_s;
//   MSG_DELAY = 1_ms;
//   vector<vector<ElectionImpl *>> all_election_group;
//   vector<int64_t> disconnect_leader_time;
//   vector<int64_t> leader_takeover_time;
//   int64_t group_size = 50;
//   disconnect_leader_time.resize(group_size);
//   leader_takeover_time.resize(group_size);
//   for (int i = 0; i < group_size; ++i) {// 创建多个paxos组，并行测试以节约时间
//     int64_t *leader_takeover_ts = &leader_takeover_time[i];
//     auto op = [leader_takeover_ts](){ *leader_takeover_ts = get_monotonic_ts(); };
//     all_election_group.push_back(create_election_group(3, op));
//   }

//   this_thread::sleep_for(chrono::seconds(10));// 等待选出所有group的第一任Leader
//   for (int i = 0; i < group_size; ++i) {// 将这些group的leader网络断联
//     this_thread::sleep_for(std::chrono::microseconds(MAX_TST / group_size));
//     ELECT_LOG(INFO, "disconnect leader", K(i), KPC(all_election_group[i][0]));
//     disconnect_leader_time[i] = get_monotonic_ts();
//     GlobalNetService.disconnect_two_side(all_election_group[i][0], all_election_group[i][1]);
//     GlobalNetService.disconnect_two_side(all_election_group[i][0], all_election_group[i][2]);
//   }

//   this_thread::sleep_for(chrono::seconds(5));// 等待选出所有group的新Leader

//   for (auto &group : all_election_group)
//     for (auto &election : group)
//       election->stop();
//   this_thread::sleep_for(chrono::seconds(1));// 等待网络中的消息处理完毕
//   for (auto &group : all_election_group)
//     for (auto &election : group)
//       delete election;
    
//   ASSERT_EQ(leader_takeover_times, 2 * group_size);
//   ASSERT_EQ(leader_revoke_times, 2 * group_size);
//   ASSERT_EQ(devote_to_be_leader_count, 2 * group_size);
//   ASSERT_EQ(lease_expired_to_be_follower_count, 1 * group_size);
//   ASSERT_EQ(change_leader_to_be_leader_count, 0);
//   ASSERT_EQ(change_leader_to_be_follower_count, 0);
//   ASSERT_EQ(stop_to_be_follower_count, 1 * group_size);

//   int64_t min = INT64_MAX, max = 0, average = 0;
//   calculate_and_print_max_min_average(leader_takeover_time, disconnect_leader_time,
//                                       min, max, average);
//   // see:
//   int64_t expected_min = 4 * MAX_TST + 3 * MSG_DELAY - (0.5 * MAX_TST - MSG_DELAY);
//   int64_t expected_max = 4 * (MAX_TST + MSG_DELAY);
//   int64_t expected_average = 4 * MAX_TST + 3.5 * MSG_DELAY - 0.5 * (0.5 * MAX_TST - MSG_DELAY);
//   // 计算偏差率
//   double min_diff_percentage = std::abs(expected_min - min) * 100.0 / expected_min;
//   double max_diff_percentage = std::abs(expected_max - max) * 100.0 / expected_max;
//   double average_diff_percentage = std::abs(expected_average - average) * 100.0 / expected_average;
//   char print_buf[1024] = {0};
//   int64_t pos = 0;
//   databuff_printf(print_buf, 1024, pos, "min_diff_percentage=%.2f%%, max_diff_percentage=%.2f%%, average_diff_percentage=%.2f%%", min_diff_percentage, max_diff_percentage, average_diff_percentage);
//   ELECT_LOG(INFO, "diff percentage summary", K(print_buf));
//   // 测试值与理论计算公式的预期值偏差在5%以内(实际上误差在0.5%以内，但是为了单测稳定把阈值调大)
//   ASSERT_NEAR(min, expected_min, expected_min * 0.05);
//   ASSERT_NEAR(max, expected_max, expected_max * 0.05);
//   ASSERT_NEAR(average, expected_average, expected_average * 0.05);
//   MSG_DELAY = 1_ms;
// }

TEST_F(TestElection, 1replica_elect_leader) {
  auto election_list = create_election_group(1, {0}, [](){});

  this_thread::sleep_for(chrono::seconds(7));

  for (auto &election_ : election_list)
    election_->stop();
  this_thread::sleep_for(chrono::seconds(2));
  for (auto &election_ : election_list)
    delete election_;

  ASSERT_EQ(leader_takeover_times, 1);
  ASSERT_EQ(leader_revoke_times, 1);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 1);
}

TEST_F(TestElection, change_leader) {
  auto election_list = create_election_group(3, {0, 0, 0}, [](){});
  
  this_thread::sleep_for(chrono::seconds(7));

  ASSERT_EQ(election_list[0]->change_leader_to(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 2)), OB_SUCCESS);
  ELECT_LOG(INFO, "call change leader");

  this_thread::sleep_for(chrono::seconds(1));

  for (auto iter = election_list.rbegin(); iter != election_list.rend(); ++iter)
    (*iter)->stop();
  this_thread::sleep_for(chrono::seconds(2));
  for (auto &election_ : election_list)
    delete election_;

  ASSERT_EQ(leader_takeover_times, 2);
  ASSERT_EQ(leader_revoke_times, 2);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 1);
  ASSERT_EQ(change_leader_to_be_follower_count, 1);
  ASSERT_EQ(stop_to_be_follower_count, 1);
}

TEST_F(TestElection, config_vresion_not_same_and_wont_split_vote) {
  auto election_list = create_election_group(3, {0, 0, 0}, [](){});

  MemberList member_list;
  ObArray<ObAddr> addr_list;
  int port = 1;
  for (int i = 0; i < 3; ++i)
    addr_list.push_back(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", port + i));

  for (int i = 0; i < 3; ++i) {
    palf::LogConfigVersion version;
    OB_ASSERT(OB_SUCCESS == version.generate(2, i));
    member_list.set_new_member_list(addr_list, version, 3);
    int ret = election_list[i]->set_memberlist(member_list);
    OB_ASSERT(OB_SUCCESS == ret);
  }

  this_thread::sleep_for(chrono::seconds(10));

  for (auto iter = election_list.rbegin(); iter != election_list.rend(); ++iter)
    (*iter)->stop();
  this_thread::sleep_for(chrono::seconds(2));
  for (auto &election_ : election_list)
    delete election_;

  ASSERT_EQ(leader_takeover_times, 1);
  ASSERT_EQ(leader_revoke_times, 1);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 1);
}

TEST_F(TestElection, change_member_concurrent_with_change_leader) {
  auto election_list = create_election_group(3, {0, 0, 0}, [](){});

  this_thread::sleep_for(chrono::seconds(7));

  ASSERT_EQ(election_list[0]->change_leader_to(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", 200)), OB_SUCCESS);
  ELECT_LOG(INFO, "call change leader, but dest addr not in member list");

  this_thread::sleep_for(chrono::seconds(1));
  for (auto iter = election_list.rbegin(); iter != election_list.rend(); ++iter)
    (*iter)->stop();
  this_thread::sleep_for(chrono::seconds(2));
  for (auto &election_ : election_list)
    delete election_;
  ASSERT_EQ(leader_takeover_times, 2);
  ASSERT_EQ(leader_revoke_times, 2);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 1);
  ASSERT_EQ(change_leader_to_be_follower_count, 1);
  ASSERT_EQ(stop_to_be_follower_count, 1);
}

TEST_F(TestElection, not_allowed_smallest_ip_be_leader) {
  auto election_list = create_election_group(3, {1, 0, 0}, [](){});

  for (auto &election_1 : election_list) {
    for (auto &election_2 : election_list) {
      GlobalNetService.connect(election_1, election_2);
    }
  }
  this_thread::sleep_for(chrono::seconds(7));
  ObRole role;
  int64_t _;
  election_list[1]->get_role(role, _);
  ASSERT_EQ(role, ObRole::LEADER);
  for (auto iter = election_list.rbegin(); iter != election_list.rend(); ++iter)
    (*iter)->stop();
  this_thread::sleep_for(chrono::seconds(2));
  for (auto &election_ : election_list)
    delete election_;

  ASSERT_EQ(leader_takeover_times, 1);
  ASSERT_EQ(leader_revoke_times, 1);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 1);
}

TEST_F(TestElection, inner_priority_seed_not_valid_when_membership_version_not_equal) {
  auto election_list = create_election_group(3, {0, 1, 2}, [](){});
  MemberList member_list;
  ObArray<ObAddr> addr_list;
  int port = 1;
  for (int i = 0; i < 3; ++i)
    addr_list.push_back(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", port + i));

  for (int i = 0; i < 3; ++i) {
    palf::LogConfigVersion version;
    OB_ASSERT(OB_SUCCESS == version.generate(2, i));
    member_list.set_new_member_list(addr_list, version, 3);
    int ret = election_list[i]->set_memberlist(member_list);
    OB_ASSERT(OB_SUCCESS == ret);
  }
  for (auto &election_1 : election_list) {
    for (auto &election_2 : election_list) {
      GlobalNetService.connect(election_1, election_2);
    }
  }
  this_thread::sleep_for(chrono::seconds(7));
  ObRole role;
  int64_t _;
  election_list[2]->get_role(role, _);
  ASSERT_EQ(role, ObRole::LEADER);
  for (auto iter = election_list.rbegin(); iter != election_list.rend(); ++iter)
    (*iter)->stop();
  this_thread::sleep_for(chrono::seconds(2));
  for (auto &election_ : election_list)
    delete election_;

  ASSERT_EQ(leader_takeover_times, 1);
  ASSERT_EQ(leader_revoke_times, 1);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 1);
}

TEST_F(TestElection, inner_priority_seed_valid_when_membership_version_equal) {
  auto election_list = create_election_group(3, {3, 2, 1}, [](){});
  for (auto &election_1 : election_list) {
    for (auto &election_2 : election_list) {
      GlobalNetService.connect(election_1, election_2);
    }
  }
  this_thread::sleep_for(chrono::seconds(7));
  ObRole role;
  int64_t _;
  election_list[2]->get_role(role, _);
  ASSERT_EQ(role, ObRole::LEADER);
  for (auto iter = election_list.rbegin(); iter != election_list.rend(); ++iter)
    (*iter)->stop();
  this_thread::sleep_for(chrono::seconds(2));
  for (auto &election_ : election_list)
    delete election_;

  ASSERT_EQ(leader_takeover_times, 1);
  ASSERT_EQ(leader_revoke_times, 1);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 1);
}

TEST_F(TestElection, set_inner_priority_seed) {
  auto election_list = create_election_group(3, {(uint64_t)PRIORITY_SEED_BIT::DEFAULT_SEED, (uint64_t)PRIORITY_SEED_BIT::DEFAULT_SEED, (uint64_t)PRIORITY_SEED_BIT::DEFAULT_SEED}, [](){});
  for (auto &election_1 : election_list) {
    for (auto &election_2 : election_list) {
      GlobalNetService.connect(election_1, election_2);
    }
  }
  this_thread::sleep_for(chrono::seconds(7));
  ObRole role;
  int64_t _;
  election_list[0]->get_role(role, _);
  ASSERT_EQ(role, ObRole::LEADER);

  ASSERT_EQ(OB_SUCCESS, election_list[0]->add_inner_priority_seed_bit(PRIORITY_SEED_BIT::SEED_IN_REBUILD_PHASE_BIT));
  this_thread::sleep_for(chrono::seconds(3));
  election_list[1]->get_role(role, _);
  ASSERT_EQ(role, ObRole::LEADER);

  for (auto iter = election_list.rbegin(); iter != election_list.rend(); ++iter)
    (*iter)->stop();
  this_thread::sleep_for(chrono::seconds(2));
  for (auto &election_ : election_list)
    delete election_;

  ASSERT_EQ(leader_takeover_times, 2);
  ASSERT_EQ(leader_revoke_times, 2);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 1);
  ASSERT_EQ(change_leader_to_be_follower_count, 1);
  ASSERT_EQ(stop_to_be_follower_count, 1);
}

TEST_F(TestElection, advance_ballot) {
  // 创建paxos group
  auto election_list = create_election_group(3, {(uint64_t)PRIORITY_SEED_BIT::DEFAULT_SEED, (uint64_t)PRIORITY_SEED_BIT::DEFAULT_SEED, (uint64_t)PRIORITY_SEED_BIT::DEFAULT_SEED}, [](){});
  // 建立网络连接
  for (auto &election_1 : election_list) {
    for (auto &election_2 : election_list) {
      GlobalNetService.connect(election_1, election_2);
    }
  }

  this_thread::sleep_for(chrono::seconds(7));// 等待第一轮选举结果，为election[0]
  ObRole role;
  int64_t old_epoch;
  election_list[0]->get_role(role, old_epoch);
  ASSERT_EQ(role, ObRole::LEADER);

  // 推大leader的ballot number之后，leader将会重新进行paxos两阶段确定新的epoch值
  ASSERT_EQ(OB_SUCCESS, election_list[0]->change_leader_to(election_list[0]->get_self_addr()));
  this_thread::sleep_for(chrono::seconds(5));// wait for advance prepare_success_ballot
  int64_t new_epoch;
  election_list[0]->get_role(role, new_epoch);
  ASSERT_EQ(role, ObRole::LEADER);
  ASSERT_EQ(old_epoch + 1, new_epoch);

  // 析构+清理动作
  for (auto iter = election_list.rbegin(); iter != election_list.rend(); ++iter)
    (*iter)->stop();
  this_thread::sleep_for(chrono::seconds(2));
  for (auto &election_ : election_list)
    delete election_;

  // 测试过程中发生的事件数量断言
  ASSERT_EQ(leader_takeover_times, 2);
  ASSERT_EQ(leader_revoke_times, 2);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 1);
  ASSERT_EQ(change_leader_to_be_follower_count, 1);
  ASSERT_EQ(stop_to_be_follower_count, 1);
}

TEST_F(TestElection, temporarily_downgrade_protocol_priority) {
  // 创建paxos group，初始场景下，election[0]的优先级为默认，election[1]/[2]的优先级被默认降低
  auto election_list = create_election_group(3, {(uint64_t)PRIORITY_SEED_BIT::DEFAULT_SEED,
                                                 (uint64_t)PRIORITY_SEED_BIT::DEFAULT_SEED | (uint64_t)PRIORITY_SEED_BIT::TEST_BIT,
                                                 (uint64_t)PRIORITY_SEED_BIT::DEFAULT_SEED | (uint64_t)PRIORITY_SEED_BIT::TEST_BIT}, [](){});
  // 建立网络连接
  for (auto &election_1 : election_list) {
    for (auto &election_2 : election_list) {
      GlobalNetService.connect(election_1, election_2);
    }
  }
  this_thread::sleep_for(chrono::seconds(7));// 等待第一轮选举结果，为election[0]
  ObRole role;
  int64_t _;
  election_list[0]->get_role(role, _);
  ASSERT_EQ(role, ObRole::LEADER);

  // 临时降低election[0]的优先级2s，此时发生切主：election[0] -> election[1]
  ASSERT_EQ(OB_SUCCESS, election_list[0]->temporarily_downgrade_protocol_priority(2_s, "test"));
  this_thread::sleep_for(chrono::seconds(2));
  election_list[1]->get_role(role, _);
  ASSERT_EQ(role, ObRole::LEADER);

  // 等待2s后，election[0]的优先级恢复，此时发生切主：election[1] -> election[0]
  this_thread::sleep_for(chrono::seconds(2));
  election_list[0]->get_role(role, _);
  ASSERT_EQ(role, ObRole::LEADER);

  // 析构+清理动作
  for (auto iter = election_list.rbegin(); iter != election_list.rend(); ++iter)
    (*iter)->stop();
  this_thread::sleep_for(chrono::seconds(2));
  for (auto &election_ : election_list)
    delete election_;

  // 测试过程中发生的事件数量断言
  ASSERT_EQ(leader_takeover_times, 3);
  ASSERT_EQ(leader_revoke_times, 3);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 2);
  ASSERT_EQ(change_leader_to_be_follower_count, 2);
  ASSERT_EQ(stop_to_be_follower_count, 1);
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_ob_election.log");
  oceanbase::palf::election::GLOBAL_INIT_ELECTION_MODULE();
  oceanbase::unittest::MockNetService::init();
  oceanbase::palf::election::INIT_TS = 0;
  oceanbase::common::ObClockGenerator::init();
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  oceanbase::share::ObTenantBase tenant_base(OB_SYS_TENANT_ID);
  tenant_base.init();
  oceanbase::share::ObTenantEnv::set_tenant(&tenant_base);
  logger.set_file_name("test_ob_election.log", false);
  logger.set_log_level(OB_LOG_LEVEL_TRACE);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
