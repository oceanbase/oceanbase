// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "logservice/leader_coordinator/ob_failure_detector.h"
#include "common/ob_role.h"
#include "lib/list/ob_dlist.h"
#include "logservice/leader_coordinator/failure_event.h"
#include "logservice/palf/election/interface/election_msg_handler.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include "logservice/palf/election/interface/election.h"
#include "logservice/palf/log_meta_info.h"
#define UNITTEST
#include "logservice/palf/election/utils/election_common_define.h"
#include "logservice/palf/election/algorithm/election_impl.h"
#include "logservice/leader_coordinator/election_priority_impl/election_priority_impl.h"
#include "share/ob_occam_timer.h"
#include "share/rc/ob_tenant_base.h"
#include "mock_logservice_container/mock_election_user.h"
#include "observer/ob_server.h"
#include <iostream>
#include <vector>

using namespace oceanbase::obrpc;
using namespace std;

#define SUCC_(stmt) ASSERT_EQ((stmt), OB_SUCCESS)
#define FAIL_(stmt) ASSERT_EQ((stmt), OB_FAIL)
#define TRUE_(stmt) ASSERT_EQ((stmt), true)
#define FALSE_(stmt) ASSERT_EQ((stmt), false)

namespace oceanbase
{
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
namespace logservice
{
namespace coordinator
{
int PriorityV0::refresh_(const share::ObLSID &)
{
  return OB_SUCCESS;
}
int PriorityV1::refresh_(const share::ObLSID &)
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
std::atomic_bool change_leader_from_prepare_change_leader_cb(false);
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

class TestElectionWithPriority : public ::testing::Test {
public:
  TestElectionWithPriority() {}
  ~TestElectionWithPriority() {}
  virtual void SetUp() {
    MAX_TST = 100_ms;
    change_leader_from_prepare_change_leader_cb = false;
    oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_0_0_0;
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
vector<ElectionImpl *> create_election_group(vector<ElectionPriorityImpl> &v_pri, TAKEOVER_OP &&op, const vector<int> &v_port = {})
{
  vector<ElectionImpl *> v;
  int election_num = v_pri.size();

  MemberList member_list;
  ObArray<ObAddr> addr_list;
  static int port = 1;
  if (v_port.empty())
    for (int i = 0; i < election_num; ++i)
      addr_list.push_back(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", port + i));
  else
    for (int port : v_port)
      addr_list.push_back(ObAddr(ObAddr::VER::IPV4, "127.0.0.1", port));
  palf::LogConfigVersion version;
  version.proposal_id_ = 1;
  version.config_seq_ = 1;
  member_list.set_new_member_list(addr_list, version, election_num);

  int ret = OB_SUCCESS;
  for (int i = 0; i < election_num; ++i) {
    ElectionImpl *election = new ElectionImpl();
    election->self_addr_ = ObAddr(ObAddr::VER::IPV4, "127.0.0.1", addr_list[i].port_);
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
      election->self_addr_,
      true,
      1,
      [election](int64_t, const ObAddr &dest_addr) {
        change_leader_from_prepare_change_leader_cb = true;
        THREAD_POOL.commit_task_ignore_ret([election, dest_addr]() { election->change_leader_to(dest_addr); });
        return OB_SUCCESS;
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
    assert(ret == OB_SUCCESS);
    election->set_priority(&v_pri[index]);
    ret = election->set_memberlist(member_list);
    assert(ret == OB_SUCCESS);
    index++;
  }
  port += election_num;
  return v;
}

void init_pri(ElectionPriorityImpl &pri)
{
  pri.priority_tuple_.element<1>().is_valid_ = true;
  pri.priority_tuple_.element<1>().is_primary_region_ = true;
  pri.priority_tuple_.element<1>().scn_.set_base();
  pri.priority_tuple_.element<1>().zone_priority_ = 0;
}

TEST_F(TestElectionWithPriority, elect_leader_because_primary_region_and_change_leader_bacause_zone_priority) {
  vector<ElectionPriorityImpl> v_pri(3);
  for (auto &pri : v_pri)
    init_pri(pri);
  v_pri[0].priority_tuple_.element<1>().is_primary_region_ = false;
  auto election_group = create_election_group(v_pri, [](){});
  this_thread::sleep_for(chrono::seconds(5));// 等待选出第一任Leader
  ASSERT_EQ(election_group[1]->proposer_.role_, ObRole::LEADER);
  ASSERT_EQ(leader_takeover_times, 1);
  ASSERT_EQ(leader_revoke_times, 0);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 0);

  v_pri[1].priority_tuple_.element<1>().zone_priority_ = 1;// 1的zone priority会降低，Leader自动切换至2
  this_thread::sleep_for(chrono::seconds(1));// 等待选出第二任Leader

  ASSERT_EQ(election_group[2]->proposer_.role_, ObRole::LEADER);
  ASSERT_EQ(leader_takeover_times, 2);
  ASSERT_EQ(leader_revoke_times, 1);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 1);
  ASSERT_EQ(change_leader_to_be_follower_count, 1);
  ASSERT_EQ(stop_to_be_follower_count, 0);

  for (auto &election : election_group)
    election->stop();
  this_thread::sleep_for(chrono::seconds(1));
  for (auto &election : election_group)
    delete election;
  
  ASSERT_EQ(leader_takeover_times, 2);
  ASSERT_EQ(leader_revoke_times, 2);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 1);
  ASSERT_EQ(change_leader_to_be_follower_count, 1);
  ASSERT_EQ(stop_to_be_follower_count, 1);
  ASSERT_EQ(change_leader_from_prepare_change_leader_cb, true);
}

TEST_F(TestElectionWithPriority, not_change_leader_because_follower_memership_version_not_update_enough_and_change_leader_later_when_follwer_membership_version_update_endough) {
  vector<ElectionPriorityImpl> v_pri(3);
  for (auto &pri : v_pri)
    init_pri(pri);
  auto election_group = create_election_group(v_pri, [](){});
  // 0的IP-PORT最小，但因为membership version的原因，1会当选Leader
  election_group[1]->proposer_.memberlist_with_states_.p_impl_->member_list_.membership_version_.config_seq_ += 1;
  this_thread::sleep_for(chrono::seconds(5));// 等待选出第一任Leader
  ASSERT_EQ(election_group[1]->proposer_.role_, ObRole::LEADER);
  ASSERT_EQ(leader_takeover_times, 1);
  ASSERT_EQ(leader_revoke_times, 0);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 0);
  // 0的membership version追上来了，但是并不会触发切主，尽管它的IP-PORT更小，但是优先级字段同Leader相同
  election_group[0]->proposer_.memberlist_with_states_.p_impl_->member_list_.membership_version_.config_seq_ += 1;
  this_thread::sleep_for(chrono::seconds(1));// 等待切主
  ASSERT_EQ(election_group[1]->proposer_.role_, ObRole::LEADER);
  ASSERT_EQ(leader_takeover_times, 1);
  ASSERT_EQ(leader_revoke_times, 0);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 0);
  // 1的membership version再加1，同时让2的log_ts超过比较阈值
  election_group[1]->proposer_.memberlist_with_states_.p_impl_->member_list_.membership_version_.config_seq_ += 1;
  v_pri[2].priority_tuple_.element<1>().scn_.convert_for_logservice(100 * 1000 * 1000 * 1000L);// 此时并不会触发切主
  this_thread::sleep_for(chrono::seconds(1));// 等待切主
  ASSERT_EQ(election_group[1]->proposer_.role_, ObRole::LEADER);
  ASSERT_EQ(leader_takeover_times, 1);
  ASSERT_EQ(leader_revoke_times, 0);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 0);
  // 2的membership version追上Leader后触发切主
  election_group[2]->proposer_.memberlist_with_states_.p_impl_->member_list_.membership_version_.config_seq_ += 2;
  this_thread::sleep_for(chrono::seconds(1));// 等待切主
  ASSERT_EQ(election_group[2]->proposer_.role_, ObRole::LEADER);
  ASSERT_EQ(leader_takeover_times, 2);
  ASSERT_EQ(leader_revoke_times, 1);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 1);
  ASSERT_EQ(change_leader_to_be_follower_count, 1);
  ASSERT_EQ(stop_to_be_follower_count, 0);

  for (auto &election : election_group)
    election->stop();
  this_thread::sleep_for(chrono::seconds(1));
  for (auto &election : election_group)
    delete election;
  
  ASSERT_EQ(leader_takeover_times, 2);
  ASSERT_EQ(leader_revoke_times, 2);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 1);
  ASSERT_EQ(change_leader_to_be_follower_count, 1);
  ASSERT_EQ(stop_to_be_follower_count, 1);
  ASSERT_EQ(change_leader_from_prepare_change_leader_cb, true);
}

TEST_F(TestElectionWithPriority, meet_fatal_failure)
{
  vector<ElectionPriorityImpl> v_pri(3);
  for (auto &pri : v_pri)
    init_pri(pri);
  v_pri[0].priority_tuple_.element<1>().zone_priority_ = 0;
  v_pri[1].priority_tuple_.element<1>().zone_priority_ = 1;
  v_pri[2].priority_tuple_.element<1>().zone_priority_ = 2;
  auto election_group = create_election_group(v_pri, [](){});
  this_thread::sleep_for(chrono::seconds(5));// 等待选出第一任Leader
  ASSERT_EQ(election_group[0]->proposer_.role_, ObRole::LEADER);
  ASSERT_EQ(leader_takeover_times, 1);
  ASSERT_EQ(leader_revoke_times, 0);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 0);

  FailureEvent event(FailureType::PROCESS_HANG, FailureModule::LOG, FailureLevel::FATAL);
  event.set_info("test");
  ASSERT_EQ(OB_SUCCESS, v_pri[0].priority_tuple_.element<1>().fatal_failures_.push_back(event));
  ELECT_LOG(INFO, "add fatal failure");
  this_thread::sleep_for(chrono::seconds(5));// 等待切主
  ASSERT_EQ(election_group[1]->proposer_.role_, ObRole::LEADER);
  ASSERT_EQ(leader_takeover_times, 2);
  ASSERT_EQ(leader_revoke_times, 1);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 1);
  ASSERT_EQ(change_leader_to_be_follower_count, 1);
  ASSERT_EQ(stop_to_be_follower_count, 0);

  for (auto &election : election_group)
    election->stop();
  this_thread::sleep_for(chrono::seconds(1));
  for (auto &election : election_group)
    delete election;
  ASSERT_EQ(change_leader_from_prepare_change_leader_cb, false);
}
}
}


namespace oceanbase
{
namespace palf
{
namespace election
{
uint64_t ElectionImpl::get_ls_biggest_min_cluster_version_ever_seen_() const// 让port=5555的副本认为自己是A副本
{
  #define PRINT_WRAPPER K(*this)
  int ret = OB_SUCCESS;
  uint64_t ls_biggest_min_cluster_version_ever_seen = 0;
  //if (observer::ObServer::get_instance().is_arbitration_mode()) {
  if (observer::ObServer::get_instance().is_arbitration_mode() || self_addr_.port_ == 5555) {
    if (CLUSTER_CURRENT_VERSION < ls_biggest_min_cluster_version_ever_seen_.version_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_NONE(ERROR, "ls_biggest_min_cluster_version_ever_seen_ less than arb binary version");
    } else if (ls_biggest_min_cluster_version_ever_seen_.version_ == 0) {
      LOG_NONE(WARN, "ls_biggest_min_cluster_version_ever_seen_ not setted yet");
    }
    ls_biggest_min_cluster_version_ever_seen = ls_biggest_min_cluster_version_ever_seen_.version_;
  } else {
    ls_biggest_min_cluster_version_ever_seen = std::max(GET_MIN_CLUSTER_VERSION(),
                                                        ls_biggest_min_cluster_version_ever_seen_.version_);
  }
  return ls_biggest_min_cluster_version_ever_seen;
  #undef PRINT_WRAPPER
}
}
}
namespace unittest
{
TEST_F(TestElectionWithPriority, arb_server_split_vote_cause_not_set_priority)// 复现仲裁bug场景
{
  // oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_3_2_3_0;// 此时采用V0版本的优先级逻辑比较，投给IP
  vector<ElectionPriorityImpl> v_pri(4);
  for (auto &pri : v_pri)
    init_pri(pri);
  v_pri[0].priority_tuple_.element<1>().zone_priority_ = 3;//F
  v_pri[1].priority_tuple_.element<1>().zone_priority_ = 2;//F
  v_pri[2].priority_tuple_.element<1>().zone_priority_ = 1;//F
  v_pri[3].priority_tuple_.element<1>().zone_priority_ = 0;//A
  auto election_group = create_election_group(v_pri, [](){}, {1,2,3,5555/*仲裁*/});
  election_group[0]->stop();// kill掉一个，还有2F1A
  election_group[3]->set_inner_priority_seed(0ULL |  static_cast<uint64_t>(palf::election::PRIORITY_SEED_BIT::SEED_NOT_NORMOL_REPLICA_BIT));
  election_group[3]->reset_priority();// 移除优先级，模拟A副本，会投票给IP最小的副本
  this_thread::sleep_for(chrono::seconds(5));// 等待选出第一任Leader
  ASSERT_EQ(election_group[1]->proposer_.role_, ObRole::FOLLOWER);
  ASSERT_EQ(election_group[2]->proposer_.role_, ObRole::FOLLOWER);
  ASSERT_EQ(election_group[3]->proposer_.role_, ObRole::FOLLOWER);
  ASSERT_EQ(leader_takeover_times, 0);
  ASSERT_EQ(leader_revoke_times, 0);
  ASSERT_EQ(devote_to_be_leader_count, 0);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 0);
  for (auto &election : election_group)
    election->stop();
    this_thread::sleep_for(chrono::seconds(1));
  for (auto &election : election_group)
    delete election;
}
TEST_F(TestElectionWithPriority, arb_server_won_t_split_vote_cause_set_priority)// 测试修复后的行为
{
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_3_2_3_0;// 此时采用V0版本的优先级逻辑比较，投给port_number_较大的副本
  vector<ElectionPriorityImpl> v_pri(4);
  for (auto &pri : v_pri)
    init_pri(pri);
  // 优先级V0
  v_pri[0].priority_tuple_.element<0>().port_number_ = 3;//F
  v_pri[1].priority_tuple_.element<0>().port_number_ = 2;//F
  v_pri[2].priority_tuple_.element<0>().port_number_ = 1;//F
  v_pri[3].priority_tuple_.element<0>().port_number_ = 0;//A
  // 优先级V1
  v_pri[0].priority_tuple_.element<1>().zone_priority_ = 3;//F
  v_pri[1].priority_tuple_.element<1>().zone_priority_ = 2;//F
  v_pri[2].priority_tuple_.element<1>().zone_priority_ = 1;//F
  v_pri[3].priority_tuple_.element<1>().zone_priority_ = 0;//A
  auto election_group = create_election_group(v_pri, [](){}, {1,2,3,5555/*仲裁*/});
  election_group[0]->stop();// kill掉一个，还有2F1A
  election_group[3]->set_inner_priority_seed(0ULL |  static_cast<uint64_t>(palf::election::PRIORITY_SEED_BIT::SEED_NOT_NORMOL_REPLICA_BIT));
  this_thread::sleep_for(chrono::seconds(5));// 等待选出第一任Leader
  ASSERT_EQ(election_group[1]->proposer_.role_, ObRole::LEADER);
  ASSERT_EQ(election_group[2]->proposer_.role_, ObRole::FOLLOWER);
  ASSERT_EQ(election_group[3]->proposer_.role_, ObRole::FOLLOWER);
  ASSERT_EQ(leader_takeover_times, 1);
  ASSERT_EQ(leader_revoke_times, 0);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 0);
  ASSERT_EQ(change_leader_to_be_follower_count, 0);
  ASSERT_EQ(stop_to_be_follower_count, 0);

  // 升级版本号
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_VERSION_4_2_0_0;// 此时将根据V1版本的优先级，将leader切换至election 2
  this_thread::sleep_for(chrono::seconds(2));// 等待执行切主
  ASSERT_EQ(election_group[1]->proposer_.role_, ObRole::FOLLOWER);
  ASSERT_EQ(election_group[2]->proposer_.role_, ObRole::LEADER);
  ASSERT_EQ(election_group[3]->proposer_.role_, ObRole::FOLLOWER);
  ASSERT_EQ(leader_takeover_times, 2);
  ASSERT_EQ(leader_revoke_times, 1);
  ASSERT_EQ(devote_to_be_leader_count, 1);
  ASSERT_EQ(lease_expired_to_be_follower_count, 0);
  ASSERT_EQ(change_leader_to_be_leader_count, 1);
  ASSERT_EQ(change_leader_to_be_follower_count, 1);
  ASSERT_EQ(stop_to_be_follower_count, 0);

  // 给leader断网，触发无主选举
  GlobalNetService.disconnect_two_side(election_group[2], election_group[1]);
  GlobalNetService.disconnect_two_side(election_group[2], election_group[3]);
  this_thread::sleep_for(chrono::seconds(5));// 等待leader卸任

  // 恢复leader的网络，预期无主时，就算是仲裁副本也正确采用了V1版本的优先级
  GlobalNetService.connect_two_side(election_group[2], election_group[1]);
  GlobalNetService.connect_two_side(election_group[2], election_group[3]);
  this_thread::sleep_for(chrono::seconds(5));// 等待无主选举

  ASSERT_EQ(election_group[1]->proposer_.role_, ObRole::FOLLOWER);
  ASSERT_EQ(election_group[2]->proposer_.role_, ObRole::LEADER);
  ASSERT_EQ(election_group[3]->proposer_.role_, ObRole::FOLLOWER);
  ASSERT_EQ(leader_takeover_times, 3);
  ASSERT_EQ(leader_revoke_times, 2);
  ASSERT_EQ(devote_to_be_leader_count, 2);
  ASSERT_EQ(lease_expired_to_be_follower_count, 1);
  ASSERT_EQ(change_leader_to_be_leader_count, 1);
  ASSERT_EQ(change_leader_to_be_follower_count, 1);
  ASSERT_EQ(stop_to_be_follower_count, 0);

  for (auto &election : election_group)
    election->stop();
    this_thread::sleep_for(chrono::seconds(1));
  for (auto &election : election_group)
    delete election;
}
}
}



int main(int argc, char **argv)
{
  system("rm -rf test_ob_election_with_priority.log");
  oceanbase::palf::election::GLOBAL_INIT_ELECTION_MODULE();
  oceanbase::unittest::MockNetService::init();
  oceanbase::palf::election::INIT_TS = 0;
  oceanbase::common::ObClockGenerator::init();
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  oceanbase::share::ObTenantBase tenant_base(OB_SYS_TENANT_ID);
  tenant_base.init();
  oceanbase::share::ObTenantEnv::set_tenant(&tenant_base);
  logger.set_file_name("test_ob_election_with_priority.log", false);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
