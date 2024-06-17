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

#include <cstdio>
#include <gtest/gtest.h>
#include <signal.h>
#include <thread>
#define private public
#include "ob_simple_log_cluster_env.h"
#include "ob_simple_log_cluster_testbase.h"
#include "ob_simple_log_server.h"
#include "common/ob_member_list.h"
#include "common/ob_role.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "logservice/ob_log_handler.h"
#include "logservice/palf/lsn.h"
#include "share/scn.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/palf_handle.h"
#include "logservice/palf/palf_base_info.h"
#include "logservice/palf/palf_handle_impl_guard.h"
#include "logservice/palf/palf_iterator.h"
#include "logservice/palf/palf_options.h"
#include "logservice/palf_handle_guard.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_service.h"
#undef private

namespace oceanbase
{
using namespace logservice;
using namespace palf;
namespace unittest
{

void init_gtest_output(std::string &gtest_log_name)
{
  // 判断是否处于Farm中
  char *mit_network_start_port_env = getenv("mit_network_start_port");
  char *mit_network_port_num_env = getenv("mit_network_port_num");
  if (mit_network_start_port_env != nullptr && mit_network_port_num_env != nullptr) {
    std::string gtest_file_name = gtest_log_name;
    int fd = open(gtest_file_name.c_str(), O_RDWR|O_CREAT, 0666);
    if (fd == 0) {
      ob_abort();
    }
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
  }
}

PalfHandleImplGuard::PalfHandleImplGuard() : palf_id_(),
                                             palf_handle_impl_(NULL),
                                             palf_env_impl_(NULL)
{
}

PalfHandleImplGuard::~PalfHandleImplGuard()
{
  reset();
}

bool PalfHandleImplGuard::is_valid() const
{
  return true == is_valid_palf_id(palf_id_) && NULL != palf_handle_impl_ && NULL != palf_env_impl_;
}

void PalfHandleImplGuard::reset()
{
  if (NULL != palf_handle_impl_ && NULL != palf_env_impl_) {
    palf_env_impl_->revert_palf_handle_impl(palf_handle_impl_);
  }
  palf_handle_impl_ = NULL;
  palf_env_impl_ = NULL;
  palf_id_ = -1;
};

PalfHandleLiteGuard::PalfHandleLiteGuard() : palf_id_(),
                                             palf_handle_lite_(NULL),
                                             palf_env_lite_(NULL)
{
}

PalfHandleLiteGuard::~PalfHandleLiteGuard()
{
  reset();
}

bool PalfHandleLiteGuard::is_valid() const
{
  return true == is_valid_palf_id(palf_id_) && NULL != palf_handle_lite_ && NULL != palf_env_lite_;
}

void PalfHandleLiteGuard::reset()
{
  if (NULL != palf_handle_lite_ && NULL != palf_env_lite_) {
    palf_env_lite_->revert_palf_handle_impl(palf_handle_lite_);
  }
  palf_handle_lite_ = NULL;
  palf_env_lite_ = NULL;
  palf_id_ = -1;
};

int generate_data(char *&buf, const int buf_len, int &real_data_size, const int wanted_data_size = 0)
{
  int ret = OB_SUCCESS;
  //random from NO_NEED_BARRIER(3), PRE_BARRIER(2), FULL_BARRIER(1)
  int i = rand() % 3 + 1;
  enum ObReplayBarrierType barrier_type;
  switch (i)
  {
  case 1 :
    barrier_type = ObReplayBarrierType::STRICT_BARRIER;
    break;
  case 2:
    barrier_type = ObReplayBarrierType::PRE_BARRIER;
    break;
  case 3:
    barrier_type = ObReplayBarrierType::NO_NEED_BARRIER;
    break;
  default:
    barrier_type = ObReplayBarrierType::STRICT_BARRIER;
    break;
  }
  logservice::ObLogBaseHeader header(logservice::TRANS_SERVICE_LOG_BASE_TYPE, barrier_type, rand());
  const int header_size = header.get_serialize_size();
  constexpr int MAX_SIZE = 100 * 1024;
  constexpr int UNIT_STR_LENGTH = 2 * 1024;
  int64_t pos = 0;
  ObTimeGuard guard("generate_data", 0);
  int payload_size = 0;
  if (buf_len < wanted_data_size) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(header.serialize(buf, buf_len, pos))) {
    CLOG_LOG(ERROR, "header serialize failed", K(header), KP(buf), K(buf_len), K(pos));
  } else if (wanted_data_size <= 0) {
    // generate random length data
    payload_size = std::min(std::max(rand() % MAX_SIZE, 20 * 1024), buf_len - header_size);
  } else {
    // generate data which length is wanted_data_size
    payload_size = wanted_data_size - header_size;
  }
  if (OB_SUCC(ret)) {
    real_data_size = payload_size + header_size;
    // generate random data as 1KB unit
    std::mt19937 generator{std::random_device{}()};
    std::uniform_int_distribution<int> distribution{'a', 'z'};
    std::string rand_str_unit(UNIT_STR_LENGTH, '\0');
    for(auto& dis: rand_str_unit) {
      dis = distribution(generator);
    }
    guard.click("random");
    int64_t remain_size = payload_size;
    for (int64_t cur_round_size = 0; remain_size > 0; remain_size -= cur_round_size) {
      cur_round_size = MIN(remain_size, UNIT_STR_LENGTH);
      MEMCPY(buf+pos, rand_str_unit.c_str(), cur_round_size);
      pos+=cur_round_size;
    }
    guard.click("fillbuf");
    CLOG_LOG(INFO, "generate_data finish", K(ret), K(payload_size), K(wanted_data_size), K(real_data_size), K(pos), KP(buf), K(guard));
  }
  return ret;
}

int64_t ObSimpleLogClusterTestEnv::palf_id_ = 1;

ObSimpleLogClusterTestEnv::ObSimpleLogClusterTestEnv()
  : ObSimpleLogClusterTestBase(), prev_leader_idx_(0)
{
}

ObSimpleLogClusterTestEnv::~ObSimpleLogClusterTestEnv()
{
}

void ObSimpleLogClusterTestEnv::SetUp()
{
  CLOG_LOG(INFO, "ObSimpleLogClusterTestEnv start delete instance", K(palf_id_));
  delete_paxos_group(palf_id_);
}

void ObSimpleLogClusterTestEnv::TearDown()
{
  CLOG_LOG(INFO, "ObSimpleLogClusterTestEnv end delete instance", K(palf_id_));
  delete_paxos_group(palf_id_);
}


int ObSimpleLogClusterTestEnv::delete_paxos_group(const int64_t id)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  for (auto svr : get_cluster()) {
    ObTenantEnv::set_tenant(svr->get_tenant_base());
    auto palf_env = svr->get_palf_env();
    if (OB_ISNULL(palf_env)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "unexpected error", K(ret), KP(svr), K(svr->is_arb_server()));
    } else if (OB_FAIL(palf_env->remove_palf_handle_impl(id))) {
      CLOG_LOG(WARN, "remove_ls failed", K(ret));
      break;
    }
    svr->remove_mock_election(id);
    svr->revert_palf_env(palf_env);
  }
  return ret;
}
int ObSimpleLogClusterTestEnv::create_paxos_group(const int64_t id, int64_t &leader_idx, PalfHandleImplGuard &leader)
{
  PalfBaseInfo palf_base_info;
  palf_base_info.generate_by_default();
  const bool with_mock_election = false;
  return create_paxos_group(id, palf_base_info, NULL, leader_idx, with_mock_election, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group(const int64_t id, palf::PalfLocationCacheCb *loc_cb, int64_t &leader_idx, PalfHandleImplGuard &leader)
{
  PalfBaseInfo palf_base_info;
  palf_base_info.generate_by_default();
  const bool with_mock_election = false;
  return create_paxos_group(id, palf_base_info, loc_cb, leader_idx, with_mock_election, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group(const int64_t id, const share::SCN &create_scn, int64_t &leader_idx, PalfHandleImplGuard &leader)
{
  int ret = OB_SUCCESS;
  PalfBaseInfo palf_base_info;
  palf_base_info.generate_by_default();
  palf_base_info.prev_log_info_.scn_ = create_scn;
  const bool with_mock_election = false;
  return create_paxos_group(id, palf_base_info, NULL, leader_idx, with_mock_election, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group(const int64_t id, const LSN &lsn, int64_t &leader_idx, PalfHandleImplGuard &leader)
{
  int ret = OB_SUCCESS;
  PalfBaseInfo palf_base_info;
  palf_base_info.generate_by_default();
  palf_base_info.curr_lsn_ = lsn;
  const bool with_mock_election = false;
  return create_paxos_group(id, palf_base_info, NULL, leader_idx, with_mock_election, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group(const int64_t id, const PalfBaseInfo &palf_base_info, int64_t &leader_idx, PalfHandleImplGuard &leader)
{
  const bool with_mock_election = false;
  return create_paxos_group(id, palf_base_info, NULL, leader_idx, with_mock_election, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group(const int64_t id,
                                                  const PalfBaseInfo &palf_base_info,
                                                  palf::PalfLocationCacheCb *loc_cb,
                                                  int64_t &leader_idx,
                                                  const bool with_mock_election,
                                                  PalfHandleImplGuard &leader)
{
  int ret = OB_SUCCESS;
  for (auto svr : get_cluster()) {
    ObTenantEnv::set_tenant(svr->get_tenant_base());
    IPalfHandleImpl* handle = NULL;
    MockElection *mock_election = NULL;
    share::ObLSID sid(id);
    if (svr->get_palf_env() == NULL) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "svr is null", KPC(svr));
    } else if (OB_FAIL(svr->get_palf_env()->create_palf_handle_impl(id, palf::AccessMode::APPEND, palf_base_info, handle))) {
      CLOG_LOG(WARN, "create_palf_handle_impl failed", K(ret), K(id), KPC(svr));
      break;
    } else {
      if (with_mock_election) {
        if (OB_FAIL(svr->create_mock_election(id, mock_election))) {
          CLOG_LOG(WARN, "create_mock_election failed", K(ret), K(id), KPC(svr));
          break;
        } else {
          common::ObAddr leader_addr;
          const ObMemberList &member_list = get_member_list();
          member_list.get_server_by_index(0, leader_addr);
          PalfHandleImpl *palf_handle_impl = dynamic_cast<PalfHandleImpl*>(handle);
          mock_election->set_leader(leader_addr, 1);
          palf_handle_impl->state_mgr_.election_ = mock_election;
          palf_handle_impl->config_mgr_.election_ = mock_election;
        }
      }
      handle->set_location_cache_cb(loc_cb);
      handle->set_locality_cb(get_cluster()[0]->get_locality_manager());
      const ObMemberList &member_list = get_member_list();
      GlobalLearnerList learner_list;
      handle->set_initial_member_list(member_list, member_list.get_member_number(), learner_list);
      CLOG_LOG(INFO, "set_initial_member_list success", K(id), "addr", svr->get_addr(), K(member_list));
    }
    if (NULL != handle) {
      svr->get_palf_env()->revert_palf_handle_impl(handle);
    }
  }
  if (OB_SUCC(ret)) {
    ret = get_leader(id, leader, leader_idx);
    ObTenantEnv::set_tenant(get_cluster()[leader_idx]->get_tenant_base());
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::create_paxos_group_with_mock_election(
    const int64_t id,
    int64_t &leader_idx,
    PalfHandleImplGuard &leader)
{
  PalfBaseInfo palf_base_info;
  palf_base_info.generate_by_default();
  const bool with_mock_election = true;
  return create_paxos_group(id, palf_base_info, NULL, leader_idx, with_mock_election, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group_with_arb(
    const int64_t id,
    int64_t &arb_replica_idx,
    int64_t &leader_idx,
    PalfHandleImplGuard &leader)
{
  const bool with_mock_election = false;
  return create_paxos_group_with_arb(id, NULL, arb_replica_idx, leader_idx, with_mock_election, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group_with_arb_mock_election(
    const int64_t id,
    int64_t &arb_replica_idx,
    int64_t &leader_idx,
    PalfHandleImplGuard &leader)
{
  const bool with_mock_election = true;
  return create_paxos_group_with_arb(id, NULL, arb_replica_idx, leader_idx, with_mock_election, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group_with_arb(
    const int64_t id,
    palf::PalfLocationCacheCb *loc_cb,
    int64_t &arb_replica_idx,
    int64_t &leader_idx,
    const bool with_mock_election,
    PalfHandleImplGuard &leader)
{
  // if member_cnt_ is 3, arb_replica_idx should be 0,1,2
  const ObMemberList member_list = get_member_list();
  const int64_t member_cnt = get_member_cnt();
  ObMember arb_member;
  return create_paxos_group_with_arb(id, loc_cb, member_list, member_cnt, arb_member, arb_replica_idx, leader_idx, with_mock_election, leader);
}

// member_list and member_cnt contain arb member
int ObSimpleLogClusterTestEnv::create_paxos_group_with_arb(
    const int64_t id,
    palf::PalfLocationCacheCb *loc_cb,
    ObMemberList member_list,
    int64_t member_cnt,
    ObMember arb_member,
    int64_t &arb_replica_idx,
    int64_t &leader_idx,
    const bool with_mock_election,
    PalfHandleImplGuard &leader)
{
  int ret = OB_SUCCESS;
  PalfBaseInfo palf_base_info;
  palf_base_info.generate_by_default();
  ObMember arb_replica = arb_member;
  GlobalLearnerList learner_list;
  arb_replica_idx = 2;
  for (int i = 0; i < get_cluster().size(); i++) {
    auto svr = get_cluster()[i];
    if (svr->is_arb_server()) {
      arb_replica_idx = i;
    }
  }
  if (-1 == arb_replica_idx) {
    ret = OB_NOT_SUPPORTED;
    PALF_LOG(ERROR, "there is not any arb server");
  } else if (false == arb_replica.is_valid() &&
      OB_FAIL(member_list.get_member_by_index(arb_replica_idx, arb_replica))) {
    PALF_LOG(ERROR, "get_member_by_index failed", K(ret), K(arb_replica_idx));
  } else {
    member_list.remove_member(arb_replica);
    for (auto svr : get_cluster()) {
      ObTenantEnv::set_tenant(svr->get_tenant_base());
      IPalfHandleImpl *handle = NULL;
      MockElection *mock_election = NULL;
      share::ObLSID sid(id);
      if (svr->get_palf_env() == NULL) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "svr is null", KPC(svr));
        break;
      } else if (OB_FAIL(svr->get_palf_env()->create_palf_handle_impl(id, palf::AccessMode::APPEND, palf_base_info, handle))) {
        CLOG_LOG(WARN, "create_palf_handle_impl failed", K(ret), K(id), KPC(svr));
      } else if (!svr->is_arb_server() && OB_FAIL(handle->set_initial_member_list(member_list, arb_replica, member_cnt-1, learner_list))) {
        CLOG_LOG(ERROR, "set_initial_member_list failed", K(ret), K(id), KPC(svr));
      } else {
        common::ObAddr leader_addr;
        member_list.get_server_by_index(0, leader_addr);
        if (with_mock_election) {
          if (OB_FAIL(svr->create_mock_election(id, mock_election))) {
            CLOG_LOG(WARN, "create_mock_election failed", K(ret), K(id), KPC(svr));
            break;
          } else if (false == svr->is_arb_server()) {
            PalfHandleImpl *palf_handle_impl = dynamic_cast<PalfHandleImpl*>(handle);
            mock_election->set_leader(leader_addr, 1);
            palf_handle_impl->state_mgr_.election_ = mock_election;
            palf_handle_impl->config_mgr_.election_ = mock_election;
          } else {
            palflite::PalfHandleLite *palf_handle_lite = dynamic_cast<palflite::PalfHandleLite*>(handle);
            mock_election->set_leader(leader_addr, 1);
            palf_handle_lite->state_mgr_.election_ = mock_election;
            palf_handle_lite->config_mgr_.election_ = mock_election;
          }
        }
        handle->set_location_cache_cb(loc_cb);
        handle->set_locality_cb(get_cluster()[0]->get_locality_manager());
        CLOG_LOG(INFO, "set_initial_member_list success", K(member_list));
      }
      if (NULL == svr->get_palf_env()) {
        ret = OB_ERR_UNEXPECTED;
        ObSimpleLogServer *log_p = dynamic_cast<ObSimpleLogServer*>(svr);
        ObSimpleArbServer *arb_p = dynamic_cast<ObSimpleArbServer*>(svr);
        CLOG_LOG(ERROR, "palf env is nullptr", K(ret), K(svr->is_arb_server()), KPC(svr), KP(log_p), K(arb_p), KPC(log_p), KPC(arb_p));
      } else {
      }
      if (NULL != handle) {
        svr->get_palf_env()->revert_palf_handle_impl(handle);
      }
    }
    if (OB_SUCC(ret)) {
      ret = get_leader(id, leader, leader_idx);
      ObTenantEnv::set_tenant(get_cluster()[leader_idx]->get_tenant_base());
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::update_disk_options(const int64_t server_id,
                                                   const int64_t recycle_threshold,
                                                   const int64_t write_stop_threshold)
{
  int ret = OB_SUCCESS;
  const int64_t MB = 1024 * 1024;
  PalfOptions opts;
  auto cluster = get_cluster();
  if (server_id >= 0 && server_id < cluster.size()) {
    ObTenantEnv::set_tenant(cluster[server_id]->get_tenant_base());
    auto srv = cluster[server_id];
    if  (true == srv->is_arb_server()) {
      ret = OB_NOT_SUPPORTED;
    } else {
      auto palf_env_impl = dynamic_cast<PalfEnvImpl*>(srv->get_palf_env());
      if (OB_FAIL(palf_env_impl->get_options(opts))) {
        PALF_LOG(ERROR, "get_optiosn failed", K(ret), K(server_id));
      } else {
       // palf_env_impl->disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_threshold_
       //   = recycle_threshold;
       // palf_env_impl->disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_limit_threshold_
       //   = write_stop_threshold;
        opts.disk_options_.log_disk_utilization_threshold_ = recycle_threshold;
        opts.disk_options_.log_disk_utilization_limit_threshold_ = write_stop_threshold;
        ret = srv->update_disk_opts(opts.disk_options_);
      }
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::update_disk_options(const int64_t server_id, const int64_t file_block_num)
{
  int ret = OB_SUCCESS;
  const int64_t MB = 1024 * 1024;
  PalfOptions opts;
  auto cluster = get_cluster();
  if (server_id >= 0 && server_id < cluster.size()) {
    ObTenantEnv::set_tenant(cluster[server_id]->get_tenant_base());
    auto srv = cluster[server_id];
    if  (true == srv->is_arb_server()) {
      ret = OB_NOT_SUPPORTED;
    } else {
      auto palf_env_impl = dynamic_cast<PalfEnvImpl*>(srv->get_palf_env());
      if (OB_FAIL(palf_env_impl->get_options(opts))) {
        PALF_LOG(ERROR, "get_optiosn failed", K(ret), K(server_id));
      } else {
        opts.disk_options_.log_disk_usage_limit_size_ = file_block_num * PALF_PHY_BLOCK_SIZE;
        ret = srv->update_disk_opts(opts.disk_options_);
      }
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::update_disk_options(const int64_t log_block_num)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  for (int64_t server_id = 0; server_id < cluster.size(); server_id++) {
    ret = update_disk_options(server_id, log_block_num);
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::get_disk_options(const int64_t server_id,
                                                PalfDiskOptions &opts)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  if (server_id >= 0 && server_id < cluster.size()) {
    ObTenantEnv::set_tenant(cluster[server_id]->get_tenant_base());
    auto srv = cluster[server_id];
    if (true == srv->is_arb_server()) {
      ret = OB_NOT_SUPPORTED;
    } else {
      ret = srv->get_disk_opts(opts);
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::restart_paxos_groups()
{
  int ret = OB_SUCCESS;
  int64_t node_idx = get_node_idx_base();
  int64_t finished_cnt = 0;
  auto func = [&finished_cnt, this](const int64_t node_idx, ObISimpleLogServer *svr) -> int{
    int ret = OB_SUCCESS;
    ObTenantEnv::set_tenant(svr->get_tenant_base());
    if (OB_FAIL(svr->simple_restart(get_test_name(), node_idx))) {
      PALF_LOG(WARN, "simple_restart failed", K(ret), K(node_idx));
    } else {
      ATOMIC_INC(&finished_cnt);
      PALF_LOG(INFO, "restart_paxos_groups success", K(node_idx), K(svr->get_addr()), KPC(svr));
    }
    return ret;
  };
  std::vector<std::thread> threads;
  for (auto svr : get_cluster()) {
    threads.emplace_back(std::thread(func, node_idx, svr));
    node_idx+=2;
  }
  for (auto &thread: threads) {
    thread.join();
  }
  if (finished_cnt != get_cluster().size()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "restart failed", K(ret), K(finished_cnt));
  } else {
    ObTenantEnv::set_tenant(get_cluster()[finished_cnt-1]->get_tenant_base());
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::restart_server(const int64_t server_id)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  if (server_id >= 0 && server_id < cluster.size()) {
    const auto svr = cluster[server_id];
    ObTenantEnv::set_tenant(svr->get_tenant_base());
    if (OB_FAIL(svr->simple_restart(get_test_name(), get_node_idx_base() + server_id * 2))) {
      PALF_LOG(WARN, "simple_restart failed", K(ret), K(server_id));
    } else {
      PALF_LOG(INFO, "restart_paxos_groups success", K(svr->get_addr()));
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::remove_dir()
{
  int ret = OB_SUCCESS;
  for (auto svr : get_cluster()) {
    char cmd[1024]; snprintf(cmd, 1024, "rm -rf %s/*", svr->get_clog_dir().c_str());
    system(cmd);
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::get_log_pool(const int64_t leader_idx, ObServerLogBlockMgr *&pool)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  pool = dynamic_cast<ObServerLogBlockMgr*>(cluster[leader_idx]->get_block_pool());
  return ret;
}

int ObSimpleLogClusterTestEnv::get_leader(const int64_t id, PalfHandleImplGuard &leader, int64_t &leader_idx)
{
  int ret = OB_SUCCESS;
  leader.reset();
  leader_idx = 0;
  do {
    auto cluster = get_cluster();
    for (int i = 0; i < cluster.size(); i++) {
      IPalfHandleImpl *ipalf_handle_impl = NULL;
      auto svr = cluster[i];
      bool is_pending_state = false;
      ObRole role = LEADER;
      int64_t epoch;
      if (false == svr->is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "svr is invalid", K(ret), KPC(svr));
      } else if (OB_FAIL(svr->get_palf_env()->get_palf_handle_impl(id, ipalf_handle_impl))) {
        PALF_LOG(WARN, "create_palf_handle_impl failed", K(ret), K(id), KPC(svr));
      } else if (OB_FAIL(ipalf_handle_impl->get_role(role, epoch, is_pending_state))) {
        PALF_LOG(WARN, "get_role failed", K(ret));
      } else if (role == LEADER
                  && false == is_pending_state) {
        leader_idx = i;
        leader.palf_handle_impl_ = dynamic_cast<PalfHandleImpl *>(ipalf_handle_impl);
        leader.palf_id_ = id;
        leader.palf_env_impl_ = dynamic_cast<PalfEnvImpl*>(svr->get_palf_env());
        PALF_LOG(INFO, "get_leader is", K(svr->get_addr()), K(leader_idx), K(epoch));
        ObTenantEnv::set_tenant(cluster[leader_idx]->get_tenant_base());
        break;
      }
      svr->get_palf_env()->revert_palf_handle_impl(ipalf_handle_impl);
    }
    if (NULL== leader.palf_handle_impl_) {
      ret = OB_ENTRY_NOT_EXIST;
      PALF_LOG(WARN, "leader not exist", K(ret), K(id));
      sleep(1);
    }
  } while (OB_ENTRY_NOT_EXIST == ret);
  if (OB_SUCC(ret) && disable_hot_cache()) {
    leader.get_palf_handle_impl()->log_engine_.log_storage_.log_cache_->hot_cache_.reset();
  }
  PALF_LOG(INFO, "get_leader finished", K(ret), K(id), K(leader_idx));
  return ret;
}

int ObSimpleLogClusterTestEnv::get_cluster_palf_handle_guard(const int64_t palf_id, std::vector<PalfHandleImplGuard*> &palf_list)
{
  int ret = OB_SUCCESS;
  for (auto svr : get_cluster()) {
    PALF_LOG(INFO, "current", KPC(svr));
    IPalfHandleImpl *ipalf_handle_impl = NULL;
    auto guard = OB_NEW(PalfHandleImplGuard, "TestLogCluster");
    if (svr->is_arb_server()) {
      palf_list.push_back(guard);
    } else if (OB_FAIL(svr->get_palf_env()->get_palf_handle_impl(palf_id, ipalf_handle_impl))) {
      PALF_LOG(WARN, "open palf failed", K(ret), K(palf_id), KPC(svr));
    } else {
      guard->palf_handle_impl_ = dynamic_cast<PalfHandleImpl*>(ipalf_handle_impl);
      guard->palf_env_impl_ = dynamic_cast<PalfEnvImpl*>(svr->get_palf_env());
      guard->palf_id_ = palf_id;
      palf_list.push_back(guard);
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::get_arb_member_guard(const int64_t palf_id, PalfHandleLiteGuard &guard)
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (auto svr : get_cluster()) {
    PALF_LOG(INFO, "current", KPC(svr));
    IPalfHandleImpl *ipalf_handle_impl = NULL;
    palflite::PalfEnvLite *palf_env = NULL;
    ObSimpleArbServer *arb_svr = dynamic_cast<ObSimpleArbServer*>(svr);
    if (svr->is_arb_server()) {
      palf_env = dynamic_cast<palflite::PalfEnvLite*>(arb_svr->get_palf_env());
      if (OB_FAIL(palf_env->get_palf_handle_impl(palf_id, ipalf_handle_impl))) {
        PALF_LOG(WARN, "open palf failed", K(ret), K(palf_id), KPC(svr));
      } else {
        guard.palf_handle_lite_ = dynamic_cast<palflite::PalfHandleLite*>(ipalf_handle_impl);
        guard.palf_env_lite_ = palf_env;
        guard.palf_id_ = palf_id;
        ret = OB_SUCCESS;
      }
      if (OB_NOT_NULL(palf_env)) {
        arb_svr->revert_palf_env(palf_env);
      }
      break;
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::revert_cluster_palf_handle_guard(std::vector<PalfHandleImplGuard*> &palf_list)
{
  int ret = OB_SUCCESS;
  for (auto palf_handle_guard : palf_list) {
    OB_DELETE(PalfHandleImplGuard, "TestLogCluster", palf_handle_guard);
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::get_palf_handle_guard(const std::vector<PalfHandleImplGuard*> &palf_list,
                                                     const common::ObAddr &server,
                                                     PalfHandleImplGuard &palf_handle)
{
  int ret = OB_SUCCESS;
  int64_t palf_id = INVALID_PALF_ID;;
  PalfEnvImpl *palf_env = NULL;
  for (auto handle: palf_list) {
    if (OB_ISNULL(handle->palf_handle_impl_)) {
    } else if (handle->palf_handle_impl_->self_ == server) {
      palf_id = handle->palf_handle_impl_->palf_id_;
      palf_env = handle->palf_env_impl_;
      break;
    }
  }
  if (false == is_valid_palf_id(palf_id)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    IPalfHandleImpl *ipalf_handle_impl = NULL;
    ret = palf_env->get_palf_handle_impl(palf_id, ipalf_handle_impl);
    palf_handle.palf_env_impl_= palf_env;
    palf_handle.palf_handle_impl_ = dynamic_cast<PalfHandleImpl*>(ipalf_handle_impl);
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::switch_leader(const int64_t id, const int64_t new_leader_idx, PalfHandleImplGuard &leader)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  int64_t old_leader_idx = 0;
  common::ObAddr leader_addr = cluster[new_leader_idx]->get_addr();
  PalfHandleImplGuard old_leader, new_leader;
  std::vector<PalfHandleImplGuard*> palf_list;

  CLOG_LOG(INFO, "begin to switch_leader");
  if (OB_FAIL(get_leader(id, old_leader, old_leader_idx))) {
    PALF_LOG(WARN, "get_leader failed", K(ret), K(id));
  } else {
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
    EXPECT_EQ(OB_SUCCESS, get_palf_handle_guard(palf_list, leader_addr, new_leader));
    while (old_leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.config_version_ >
          new_leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.config_version_) {
      ::usleep(500);
    }
    new_leader.reset();
    EXPECT_EQ(OB_SUCCESS, revert_cluster_palf_handle_guard(palf_list));
    ObTenantEnv::set_tenant(cluster[old_leader_idx]->get_tenant_base());
    old_leader.palf_handle_impl_->change_leader_to(leader_addr);
    CLOG_LOG(INFO, "switch_leader success", K(ret), "prev_leader:", cluster[prev_leader_idx_]->get_addr(), "new_leader:", cluster[new_leader_idx]->get_addr(),
             "old_leader:", cluster[old_leader_idx]->get_addr(), K(old_leader_idx), K(new_leader_idx));
    prev_leader_idx_ = new_leader_idx;
    // 确保election已经切主成功.
    do {
      ret = get_leader(id, leader, old_leader_idx);
    } while (old_leader_idx != new_leader_idx);
  }
  if (OB_SUCC(ret)) {
    ObTenantEnv::set_tenant(cluster[new_leader_idx]->get_tenant_base());
  }
  return ret;
}

void ObSimpleLogClusterTestEnv::set_need_drop_packet(const int64_t id, const bool need_drop_packet)
{
  auto cluster = get_cluster();
  cluster[id]->set_need_drop_packet(need_drop_packet);
}

int ObSimpleLogClusterTestEnv::check_replica_sync(const int64_t id, PalfHandleImplGuard *pf1, PalfHandleImplGuard *pf2, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  ObAddr addr1 = pf1->palf_handle_impl_->self_;
  ObAddr addr2 = pf2->palf_handle_impl_->self_;

  const int64_t start_ts = ObTimeUtility::current_time();
  LSN max_lsn_1, max_lsn_2;
  LSN end_lsn_1, end_lsn_2;
  if (NULL == pf1 | NULL == pf2) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(ERROR, "invalid argumemt", KP(pf1), KP(pf2));
  } else {
    while (OB_SUCC(ret)) {
      max_lsn_1 = pf1->palf_handle_impl_->get_max_lsn();
      max_lsn_2 = pf2->palf_handle_impl_->get_max_lsn();
      end_lsn_1 = pf1->palf_handle_impl_->get_end_lsn();
      end_lsn_2 = pf2->palf_handle_impl_->get_end_lsn();
      if (!end_lsn_1.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "end_lsn_1 is invalid", K(addr1), K(addr2), K(max_lsn_1), K(end_lsn_1), K(end_lsn_2));
      } else if (end_lsn_1 < max_lsn_1) {
        usleep(1 * 1000);
      } else if (max_lsn_2 < end_lsn_1) {
        // 目前用max_lsn与leader的end_lsn比较
        usleep(1 * 1000);
      } else if (max_lsn_2 > end_lsn_1) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "max_lsn is larger than leader's end_lsn", K(addr1), K(addr2), K(max_lsn_1), K(max_lsn_2), K(end_lsn_1), K(end_lsn_2));
        break;
      } else {
        SERVER_LOG(INFO, "log is sync", K(addr1), K(addr2), K(max_lsn_1), K(max_lsn_2), K(end_lsn_1), K(end_lsn_2));
        break;
      }
      if (OB_SUCC(ret) && timeout_us > 0 && ObTimeUtility::current_time() - start_ts >= timeout_us) {
        ret = OB_TIMEOUT;
        SERVER_LOG(INFO, "wait log sync timeout", K(ret), K(timeout_us), K(addr1), K(addr2), K(end_lsn_1), K(end_lsn_2));
        break;
      }
    };
  }

  SERVER_LOG(INFO, "check_replica_sync finished", K(ret), K(addr1), K(addr2), K(max_lsn_1), K(end_lsn_1), K(max_lsn_2), K(end_lsn_2), K(timeout_us));
  return ret;
}

// Bidirectional network isolation
void ObSimpleLogClusterTestEnv::block_all_net(const int64_t id)
{
  auto cluster = get_cluster();
  const ObAddr this_addr = cluster[id]->get_addr();
  for (auto server : cluster) {
    if (this_addr != server->get_addr()) {
      cluster[id]->block_net(server->get_addr());
      server->block_net(this_addr);
    }
  }
  SERVER_LOG(INFO, "block_net success", K(this_addr));
}

void ObSimpleLogClusterTestEnv::unblock_all_net(const int64_t id)
{
  auto cluster = get_cluster();
  const ObAddr this_addr = cluster[id]->get_addr();
  for (auto server : cluster) {
    if (this_addr != server->get_addr()) {
      cluster[id]->unblock_net(server->get_addr());
      server->unblock_net(this_addr);
    }
  }
  SERVER_LOG(INFO, "unblock_net success", K(this_addr));
}

void ObSimpleLogClusterTestEnv::block_net(const int64_t id1, const int64_t id2, const bool is_single_direction)
{
  auto cluster = get_cluster();
  ObAddr addr1 = cluster[id1]->get_addr();
  ObAddr addr2 = cluster[id2]->get_addr();
  cluster[id2]->block_net(addr1);
  if (!is_single_direction) {
    cluster[id1]->block_net(addr2);
  }
  SERVER_LOG(INFO, "block_net success", K(addr1), K(addr2), K(is_single_direction));
}

void ObSimpleLogClusterTestEnv::unblock_net(const int64_t id1, const int64_t id2)
{
  auto cluster = get_cluster();
  ObAddr addr1 = cluster[id1]->get_addr();
  ObAddr addr2 = cluster[id2]->get_addr();
  cluster[id1]->unblock_net(addr2);
  cluster[id2]->unblock_net(addr1);
  SERVER_LOG(INFO, "unblock_net success", K(addr1), K(addr2));
}

void ObSimpleLogClusterTestEnv::block_pcode(const int64_t id1, const ObRpcPacketCode &pcode)
{
  auto cluster = get_cluster();
  ObAddr addr1 = cluster[id1]->get_addr();
  cluster[id1]->block_pcode(pcode);
  SERVER_LOG(INFO, "block_pcode success", K(addr1), K(pcode));
}

void ObSimpleLogClusterTestEnv::unblock_pcode(const int64_t id1, const ObRpcPacketCode &pcode)
{
  auto cluster = get_cluster();
  ObAddr addr1 = cluster[id1]->get_addr();
  cluster[id1]->unblock_pcode(pcode);
  SERVER_LOG(INFO, "unblock_pcode success", K(addr1), K(pcode));
}

// set rpc loss by rate from id1 to id2
void ObSimpleLogClusterTestEnv::set_rpc_loss(const int64_t id1, const int64_t id2, const int loss_rate)
{
  auto cluster = get_cluster();
  ObAddr addr1 = cluster[id1]->get_addr();
  ObAddr addr2 = cluster[id2]->get_addr();
  cluster[id2]->set_rpc_loss(addr1, loss_rate);
  SERVER_LOG(INFO, "set_rpc_loss success", K(addr1), K(addr2), K(loss_rate));
}

void ObSimpleLogClusterTestEnv::reset_rpc_loss(const int64_t id1, const int64_t id2)
{
  auto cluster = get_cluster();
  ObAddr addr1 = cluster[id1]->get_addr();
  ObAddr addr2 = cluster[id2]->get_addr();
  cluster[id2]->reset_rpc_loss(addr1);
  SERVER_LOG(INFO, "reset_rpc_loss success", K(addr1), K(addr2));
}

int ObSimpleLogClusterTestEnv::submit_log(PalfHandleImplGuard &leader, int count, int id)
{
  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_array;
  return submit_log_impl(leader, count, id, 0, lsn_array, scn_array);
}

int ObSimpleLogClusterTestEnv::submit_log(PalfHandleImplGuard &leader, int count, int id, int data_len)
{
  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_array;
  return submit_log_impl(leader, count, id, data_len, lsn_array, scn_array);
}

int ObSimpleLogClusterTestEnv::submit_log(PalfHandleImplGuard &leader, int count, int id, std::vector<LSN> &lsn_array, std::vector<SCN> &scn_array)
{
  return submit_log_impl(leader, count, id, 0, lsn_array, scn_array);
}

int ObSimpleLogClusterTestEnv::submit_log(PalfHandleImplGuard &leader, int count, int data_len, int id, std::vector<LSN> &lsn_array, std::vector<SCN> &scn_array)
{
  return submit_log_impl(leader, count, id, data_len, lsn_array, scn_array);
}

int ObSimpleLogClusterTestEnv::submit_log(PalfHandleImplGuard &leader, LSN &lsn, share::SCN &scn)
{
  const int64_t ref_ts = ObTimeUtility::current_time_ns();
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(ref_ts);
  return submit_log(leader, ref_scn, lsn, scn);
}

int ObSimpleLogClusterTestEnv::submit_log(PalfHandleImplGuard &leader, const share::SCN &ref_scn, LSN &lsn, share::SCN &scn)
{
  int ret = OB_SUCCESS;
  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_array;
  if (OB_FAIL(submit_log_impl(leader, 1, 1, 0, ref_scn, lsn_array, scn_array))) {
    //do nothing
  } else {
    lsn = lsn_array[0];
    scn = scn_array[0];
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::submit_log_impl(PalfHandleImplGuard &leader,
                                               const int64_t count,
                                               const int64_t id,
                                               const int64_t data_len,
                                               std::vector<LSN> &lsn_array,
                                               std::vector<SCN> &scn_array)
{
  const int64_t ref_ts = ObTimeUtility::current_time_ns();
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(ref_ts);
  return submit_log_impl(leader, count, id, data_len, ref_scn, lsn_array, scn_array);
}

// params[in] wanted_data_size: if equals to 0, it will generate data randomly
int ObSimpleLogClusterTestEnv::submit_log_impl(PalfHandleImplGuard &leader,
                                               const int64_t count,
                                               const int64_t id,
                                               const int64_t data_len,
                                               const share::SCN &ref_scn,
                                               std::vector<LSN> &lsn_array,
                                               std::vector<SCN> &scn_array)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int buf_len = (data_len <= 0) ? MAX_LOG_BODY_SIZE: data_len;
  int real_log_data_size = 0;
  PalfAppendOptions opts;
  ObRole role;
  while (buf == NULL) {
    buf = static_cast<char *>(ob_malloc(buf_len, ObNewModIds::TEST));
  }
  bool state;
  if (OB_FAIL(generate_data(buf, buf_len, real_log_data_size, data_len))) {
    PALF_LOG(ERROR, "generate_data failed", KP(buf), K(buf_len), K(real_log_data_size));
  } else if (OB_FAIL(leader.palf_handle_impl_->get_role(role, opts.proposal_id, state))) {
    PALF_LOG(WARN, "get_fole failed");
  } else if (LEADER != role) {
    ret = OB_STATE_NOT_MATCH;
    PALF_LOG(WARN, "self is not leader, can't submit log", K(ret), K(role));
  } else {
    for (int i  = 0; i < count && OB_SUCC(ret); i++) {
      do {
        usleep(10);
        LSN lsn;
        share::SCN scn;
        ret = leader.palf_handle_impl_->submit_log(opts, buf, real_log_data_size, ref_scn, lsn, scn);
        if (OB_SUCC(ret) && lsn.is_valid()) {
          lsn_array.push_back(lsn);
          scn_array.push_back(scn);
          usleep(200);
          PALF_LOG(INFO, "submit_log success", KR(ret), K(id), K(lsn), K(scn));
        } else {
          if (REACH_TIME_INTERVAL(100 * 1000)) {
            PALF_LOG(WARN, "append failed", KR(ret), K(i));
          }
        }
      } while (OB_EAGAIN == ret || common::OB_ERR_OUT_OF_UPPER_BOUND == ret);
      if (OB_FAIL(ret)) {
        PALF_LOG(WARN, "submit_log failed", KR(ret), K(i));
      }
    }
  }
  if (OB_NOT_NULL(buf)) {
    ob_free(buf);
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::raw_write(PalfHandleImplGuard &leader,
                                         const LSN lsn,
                                         const char *buf,
                                         const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int real_log_data_size = 0;
  PalfAppendOptions opts;
  ObRole role;
  bool state;
  if (OB_FAIL(leader.palf_handle_impl_->get_role(role, opts.proposal_id, state))) {
    PALF_LOG(WARN, "get_fole failed");
  } else {
    do {
      usleep(10);
      ret = (leader.palf_handle_impl_)->submit_group_log(opts, lsn, buf, buf_len);
      if (OB_SUCC(ret) || OB_ERR_OUT_OF_LOWER_BOUND == ret) {
        PALF_LOG(INFO, "raw_write success", KR(ret), K(lsn));
        ret = OB_SUCCESS;
      } else {
        if (REACH_TIME_INTERVAL(100 * 1000)) {
          PALF_LOG(WARN, "raw_write failed", KR(ret));
        }
      }
    } while (OB_EAGAIN == ret || common::OB_ERR_OUT_OF_UPPER_BOUND == ret);
    if (OB_FAIL(ret)) {
      PALF_LOG(WARN, "raw_write failed", KR(ret));
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::change_access_mode_to_raw_write(PalfHandleImplGuard &leader)
{
  int ret = OB_SUCCESS;
  palf::AccessMode new_access_mode = palf::AccessMode::RAW_WRITE;
  palf::AccessMode curr_access_mode;
  int64_t proposal_id, mode_version;
  ObRole unused_role;
  bool state;
  const int64_t timeout_ns = 10 * 1000 * 1000 * 1000l;
  if (OB_FAIL(leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode))) {
    PALF_LOG(WARN, "get_access_mode failed", K(ret));
  } else if (OB_FAIL(leader.palf_handle_impl_->get_role(unused_role, proposal_id, state))) {
    PALF_LOG(WARN, "get_access_mode failed", K(ret));
  } else if (OB_FAIL(leader.palf_handle_impl_->change_access_mode(proposal_id, mode_version, new_access_mode, share::SCN::min_scn()))) {
    PALF_LOG(WARN, "change_access_mode failed", K(ret));
  } else {
    PALF_LOG(INFO, "change_access_mode to raw_write success", K(ret));
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::change_access_mode_to_append(PalfHandleImplGuard &leader)
{
  int ret = OB_SUCCESS;
  palf::AccessMode new_access_mode = palf::AccessMode::APPEND;
  palf::AccessMode curr_access_mode;
  int64_t proposal_id, mode_version;
  ObRole unused_role;
  bool state;
  const int64_t timeout_ns = 10 * 1000 * 1000 * 1000l;
  if (OB_FAIL(leader.palf_handle_impl_->get_access_mode(mode_version, curr_access_mode))) {
    PALF_LOG(WARN, "get_access_mode failed", K(ret));
  } else if (OB_FAIL(leader.palf_handle_impl_->get_role(unused_role, proposal_id, state))) {
    PALF_LOG(WARN, "get_access_mode failed", K(ret));
  } else if (OB_FAIL(leader.palf_handle_impl_->change_access_mode(proposal_id, mode_version, new_access_mode, SCN::min_scn()))) {
    PALF_LOG(WARN, "change_access_mode failed", K(ret));
  } else {
    PALF_LOG(INFO, "change_access_mode to raw_write success", K(ret));
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::read_log(PalfHandleImplGuard &leader)
{
  return read_log(leader, LSN(PALF_INITIAL_LSN_VAL));
}

int ObSimpleLogClusterTestEnv::read_log(PalfHandleImplGuard &leader, const LSN &lsn)
{
  int ret = OB_SUCCESS;
  PalfBufferIterator iterator(leader.palf_id_);
  if (OB_FAIL(leader.palf_handle_impl_->alloc_palf_buffer_iterator(lsn, iterator))) {
  } else {
    while (OB_SUCCESS == ret) {
      const char *buf;
      int64_t buf_len = 0;
      share::SCN scn = share::SCN::min_scn();
      LSN log_offset;
      bool is_raw_write = false;
      if (OB_FAIL(iterator.next())) {
      } else if (OB_FAIL(iterator.get_entry(buf, buf_len, scn, log_offset, is_raw_write))) {
      } else {
        PALF_LOG(TRACE, "print log entry", K(is_raw_write));
      }
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::read_group_log(PalfHandleImplGuard &leader, LSN lsn)
{
  int ret = OB_SUCCESS;
  PalfGroupBufferIterator iterator(leader.palf_id_);
  if (OB_FAIL(leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(lsn, iterator))) {
  } else {
    LogGroupEntry entry;
    while (OB_SUCCESS == ret) {
      LSN log_offset;
      if (OB_FAIL(iterator.next())) {
      } else if (OB_FAIL(iterator.get_entry(entry, log_offset))) {
      } else if (iterator.iterator_impl_.curr_entry_is_raw_write_ != entry.get_header().is_raw_write()) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "raw write not match, unexpected error", K(ret), K(iterator));
      } else {
        PALF_LOG(TRACE, "print log group entry", K(entry));
      }
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::read_and_submit_group_log(PalfHandleImplGuard &leader,
                                                         PalfHandleImplGuard &leader_raw_write,
                                                         const LSN &start_lsn)
{
  int ret = OB_SUCCESS;
  PalfGroupBufferIterator iterator(leader.palf_id_);
  if (OB_FAIL(leader.palf_handle_impl_->alloc_palf_group_buffer_iterator(LSN(start_lsn), iterator))) {
  } else {
    LogGroupEntry entry;
    while (OB_SUCCESS == ret) {
      LSN lsn;
      if (OB_FAIL(iterator.next())) {
        PALF_LOG(WARN, "iterator next failed", K(ret), K(iterator));
      } else if (OB_FAIL(iterator.get_entry(entry, lsn))) {
        PALF_LOG(WARN, "iterator get_entry failed", K(ret), K(iterator));
      } else if (OB_FAIL(raw_write(leader_raw_write, lsn,
              entry.get_data_buf()-entry.get_header().get_serialize_size(), entry.get_serialize_size()))) {
        PALF_LOG(WARN, "raw_write failed", K(ret), K(iterator), K(entry), K(lsn));
      }
    }
    if (OB_ITER_END == ret) {
      wait_until_has_committed(leader_raw_write, LSN(leader.palf_handle_impl_->get_end_lsn()));
      PalfBufferIterator iterator_raw_write(leader_raw_write.palf_id_);
      if (OB_FAIL(leader_raw_write.palf_handle_impl_->alloc_palf_buffer_iterator(LSN(0), iterator_raw_write))) {
        PALF_LOG(WARN, "leader seek failed", K(ret), K(iterator_raw_write));
      } else {
        while (OB_SUCCESS == ret) {
          LSN lsn;
          const char *buffer = NULL;
          bool is_raw_write = false;
          int64_t nbytes = 0;
          share::SCN scn;
          if (OB_FAIL(iterator_raw_write.next())) {
            PALF_LOG(WARN, "iterator next failed", K(ret), K(iterator_raw_write));
          } else if (OB_FAIL(iterator_raw_write.get_entry(buffer, nbytes, scn, lsn, is_raw_write))) {
            PALF_LOG(WARN, "iterator get_entry failed", K(ret), K(iterator_raw_write), K(is_raw_write));
          } else if (lsn >= start_lsn && is_raw_write != true) {
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(ERROR, "iterator get_entry failed, is_raw_write must be true", K(ret), K(iterator_raw_write), K(is_raw_write),
                     K(lsn), K(start_lsn));
          }
        }
      }
    }
  }
  return ret;
}

int ping_buf(const block_id_t min_block_id, const block_id_t max_block_id, char *&buf, LogEngine *log_engine)
{
  int ret = OB_SUCCESS;
  int64_t size = PALF_BLOCK_SIZE;
  buf = nullptr;
  buf = reinterpret_cast<char *>(ob_malloc_align(4 * 1024, size, "Dummy"));
  OB_ASSERT(nullptr != buf);
  memset(buf, 0, size);
  PALF_LOG(INFO, "runlin trace ping buf after memset");
  int64_t cursor = 0;
  for (block_id_t block_id = min_block_id; block_id <= min_block_id && OB_SUCC(ret); block_id++) {
    ReadBuf read_buf(buf + cursor, PALF_BLOCK_SIZE);
    int64_t out_read_size = 0;
    if (OB_FAIL(
            log_engine->read_log(LSN(block_id * PALF_BLOCK_SIZE), PALF_BLOCK_SIZE, read_buf, out_read_size))) {
    } else {
      cursor += PALF_BLOCK_SIZE;
      PALF_LOG(INFO, "runlin trace ping buf read block_id", K(block_id));
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::read_log_from_memory(PalfHandleImplGuard &leader)
{
  int ret = OB_SUCCESS;
  LSN lsn(0);
  MemPalfBufferIterator iterator(leader.palf_id_);
  MemoryStorage mem_storage;
  char *buf = nullptr;
  block_id_t min_block_id, max_block_id;
  LogEngine *log_engine = &leader.palf_handle_impl_->log_engine_;
  if (OB_FAIL(log_engine->get_block_id_range(min_block_id, max_block_id))) {
    PALF_LOG(WARN, "get_block_id_range failed", K(ret));
  } else {
    auto func = [&leader](){return leader.palf_handle_impl_->get_end_lsn();};
    if (OB_FAIL(ping_buf(min_block_id, max_block_id, buf, log_engine))) {
      PALF_LOG(ERROR, "ping_buf failed", K(ret));
    } else if (OB_FAIL(mem_storage.init(LSN(min_block_id * PALF_BLOCK_SIZE)))) {
      PALF_LOG(ERROR, "mem_storage init failed", K(ret));
    } else if (OB_FAIL(mem_storage.append(buf, PALF_BLOCK_SIZE))) {
      PALF_LOG(ERROR, "append failed", K(ret), KP(buf), K(max_block_id), K(min_block_id));
    } else if (OB_FAIL(iterator.init(LSN(min_block_id * PALF_BLOCK_SIZE), func, &mem_storage))) {
      PALF_LOG(ERROR, "init failed", K(ret), KP(buf));
    } else {
      LogEntry entry;
      LSN lsn;
      while (OB_SUCC(iterator.next())) {
        if (OB_FAIL(iterator.get_entry(entry, lsn))) {
          PALF_LOG(WARN, "get_entry failed", K(ret));
        }
      }
    }
    if (NULL != buf) {
      ob_free_align(buf);
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::advance_base_info(const int64_t id, const PalfBaseInfo &base_info)
{
  int ret = OB_SUCCESS;
  for (auto svr : get_cluster()) {
    ObTenantEnv::set_tenant(svr->get_tenant_base());
    IPalfHandleImplGuard guard;
    share::ObLSID sid(id);
    if (svr->is_arb_server()) {
    } else if (svr->get_palf_env() == NULL) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "svr is null", KPC(svr));
    } else if (OB_FAIL(svr->get_palf_env()->get_palf_handle_impl(id, guard))) {
      CLOG_LOG(WARN, "open palf failed", K(ret), K(id), KPC(svr));
    } else if (OB_FAIL(guard.palf_handle_impl_->disable_sync())) {
      CLOG_LOG(WARN, "disable_sync failed", K(ret), K(id), KPC(svr));
    } else if (OB_FAIL(guard.palf_handle_impl_->advance_base_info(base_info, true))) {
      CLOG_LOG(WARN, "advance_base_info failed", K(ret));
    } else if (OB_FAIL(guard.palf_handle_impl_->enable_sync())) {
      CLOG_LOG(WARN, "enable_sync failed", K(ret), K(id), KPC(svr));
    } else {
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::get_palf_env(const int64_t server_idx,
                                            PalfEnv *&palf_env)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  if (server_idx >= cluster.size()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ret = cluster[server_idx]->get_palf_env(palf_env);
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::wait_until_has_committed(PalfHandleImplGuard &leader, const LSN &lsn)
{
  int ret = OB_SUCCESS;
  LSN end_lsn;
  SCN end_scn;
  while (OB_SUCC(ret)) {
    end_lsn = leader.palf_handle_impl_->get_end_lsn();
    end_scn = leader.palf_handle_impl_->get_end_scn();
    auto &sw = leader.palf_handle_impl_->sw_;
    CLOG_LOG(INFO, "runlin trace wait_until_has_committed", K(end_lsn), K(lsn), K(end_scn), K(sw));
    if (end_lsn < lsn) {
      usleep(1000);
    } else {
      break;
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::wait_lsn_until_slide(const LSN &lsn, PalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;

  int64_t print_log_time = OB_INVALID_TIMESTAMP;
  LSN last_slide_end_lsn = guard.palf_handle_impl_->sw_.last_slide_end_lsn_;
  while (lsn > last_slide_end_lsn) {
    usleep(1*1000);
    if (palf_reach_time_interval(1*1000*1000, print_log_time)) {
      PALF_LOG(WARN, "wait_lsn_until_slide", K(last_slide_end_lsn), K(lsn));
    }
    last_slide_end_lsn = guard.palf_handle_impl_->sw_.last_slide_end_lsn_;
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::wait_lsn_until_flushed(const LSN &lsn, PalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;

  int64_t print_log_time = OB_INVALID_TIMESTAMP;
  LSN max_flushed_end_lsn = guard.palf_handle_impl_->sw_.max_flushed_end_lsn_;
  while (lsn > max_flushed_end_lsn) {
    usleep(5*1000);
    if (palf_reach_time_interval(1*1000*1000, print_log_time)) {
      PALF_LOG(WARN, "wait_lsn_until_flushed", K(ret), K(max_flushed_end_lsn), K(lsn));
    }
    max_flushed_end_lsn = guard.palf_handle_impl_->sw_.max_flushed_end_lsn_;
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::wait_lsn_until_submitted(const LSN &lsn, PalfHandleImplGuard &guard)
{
  int ret = OB_SUCCESS;

  int64_t print_log_time = OB_INVALID_TIMESTAMP;
  LSN max_submit_end_lsn = guard.palf_handle_impl_->sw_.last_submit_end_lsn_;
  while (lsn > max_submit_end_lsn) {
    usleep(5 * 1000L);
    if (palf_reach_time_interval(1 * 1000 * 1000L, print_log_time)) {
      PALF_LOG(WARN, "wait_lsn_until_submitted", K(ret), K(max_submit_end_lsn), K(lsn));
    }
    max_submit_end_lsn = guard.palf_handle_impl_->sw_.last_submit_end_lsn_;
  }
  return ret;
}

void ObSimpleLogClusterTestEnv::wait_all_replcias_log_sync(const int64_t palf_id)
{
  std::vector<PalfHandleImplGuard*> palf_list;
  PalfHandleImplGuard leader;
  int64_t leader_idx;
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(palf_id, palf_list));
  EXPECT_EQ(OB_SUCCESS, get_leader(palf_id, leader, leader_idx));
  const LSN &end_lsn = leader.palf_handle_impl_->get_end_lsn();
  bool need_check = true;
  while (need_check) {
    need_check = false;
    for (auto srv: palf_list) {
      LSN max_flushed_end_lsn;
      srv->palf_handle_impl_->sw_.get_max_flushed_end_lsn(max_flushed_end_lsn);
      if (end_lsn != max_flushed_end_lsn) {
        need_check = true;
        break;
      }
    }
    ::usleep(1000);
  }
  revert_cluster_palf_handle_guard(palf_list);
}

int ObSimpleLogClusterTestEnv::get_middle_scn(const int64_t log_num, PalfHandleImplGuard &leader, SCN &mid_scn, LogEntryHeader &log_entry_header)
{
	int ret = OB_SUCCESS;
	PalfBufferIterator iterator(leader.palf_id_);
	LSN init_lsn(PALF_INITIAL_LSN_VAL);
	if (OB_FAIL(leader.palf_handle_impl_->alloc_palf_buffer_iterator(init_lsn, iterator))) {
		PALF_LOG(ERROR, "seek failed", K(ret), K(iterator));
	} else {
		LogEntry entry;
		LSN lsn;
		int i = 0;
		while (i < log_num && OB_SUCC(ret) && OB_SUCC(iterator.next())) {
			if (OB_FAIL(iterator.get_entry(entry, lsn))) {
			} else {
				mid_scn = entry.get_scn();
			}
			i++;
		};
    if (OB_SUCCESS == ret) {
      log_entry_header = entry.get_header();
    }
	PALF_LOG(INFO, "runlin trace entry", K(i), K(log_num), K(entry));
	}
	return ret;
}

void ObSimpleLogClusterTestEnv::switch_append_to_raw_write(PalfHandleImplGuard &leader, int64_t &mode_version)
{
  AccessMode access_mode = AccessMode::INVALID_ACCESS_MODE;
  int64_t proposal_id = INVALID_PROPOSAL_ID;
  ObRole unused_role;
  bool state;
  leader.palf_handle_impl_->get_role(unused_role, proposal_id, state);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, access_mode));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(proposal_id, mode_version, AccessMode::RAW_WRITE, SCN::min_scn()));
  do {
    usleep(50);
    leader.palf_handle_impl_->get_access_mode(mode_version, access_mode);
  } while(access_mode != AccessMode::RAW_WRITE);
}

void ObSimpleLogClusterTestEnv::switch_append_to_flashback(PalfHandleImplGuard &leader, int64_t &mode_version)
{
  AccessMode access_mode = AccessMode::INVALID_ACCESS_MODE;
  int64_t proposal_id = INVALID_PROPOSAL_ID;
  ObRole unused_role;
  bool state;
  leader.palf_handle_impl_->get_role(unused_role, proposal_id, state);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, access_mode));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(proposal_id, mode_version, AccessMode::RAW_WRITE, SCN::min_scn()));
  do {
    usleep(50);
    leader.palf_handle_impl_->get_access_mode(mode_version, access_mode);
  } while(access_mode != AccessMode::RAW_WRITE);
  leader.palf_handle_impl_->get_role(unused_role, proposal_id, state);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(proposal_id, mode_version, AccessMode::FLASHBACK, SCN::min_scn()));
  do {
    usleep(50);
    leader.palf_handle_impl_->get_access_mode(mode_version, access_mode);
  } while(access_mode != AccessMode::FLASHBACK);
}

void ObSimpleLogClusterTestEnv::switch_flashback_to_append(PalfHandleImplGuard &leader, int64_t &mode_version)
{
  AccessMode access_mode = AccessMode::INVALID_ACCESS_MODE;
  int64_t proposal_id = INVALID_PROPOSAL_ID;
  ObRole unused_role;
  bool state;
  leader.palf_handle_impl_->get_role(unused_role, proposal_id, state);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_access_mode(mode_version, access_mode));
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->change_access_mode(proposal_id, mode_version, AccessMode::APPEND, SCN::min_scn()));
  do {
    usleep(50);
    leader.palf_handle_impl_->get_access_mode(mode_version, access_mode);
  } while(access_mode != AccessMode::APPEND);
}

void ObSimpleLogClusterTestEnv::set_disk_options_for_throttling(PalfEnvImpl &palf_env_impl)
{
  const int64_t MB = 1024 * 1024L;
  const int64_t total_disk_size = 400 * MB;
  const int64_t utilization_limit_threshold = 95;
  const int64_t utilization_threshold = 94;
  const int64_t throttling_percentage = 100;
  const int64_t unrecyclable_size = 0;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_throttling_percentage_ = throttling_percentage;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_usage_limit_size_ = total_disk_size;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_threshold_ = utilization_threshold;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_.log_disk_utilization_limit_threshold_ = utilization_limit_threshold;
  palf_env_impl.disk_options_wrapper_.disk_opts_for_recycling_blocks_ = palf_env_impl.disk_options_wrapper_.disk_opts_for_stopping_writing_;
  palf_env_impl.disk_options_wrapper_.set_cur_unrecyclable_log_disk_size(unrecyclable_size);
}

bool ObSimpleLogClusterTestEnv::is_degraded(const PalfHandleImplGuard &leader,
                                            const int64_t degraded_server_idx)
{
  bool has_degraded = false;
  while (!has_degraded) {
    common::GlobalLearnerList degraded_learner_list;
    leader.palf_handle_impl_->config_mgr_.get_degraded_learner_list(degraded_learner_list);
    has_degraded = degraded_learner_list.contains(get_cluster()[degraded_server_idx]->get_addr());
    sleep(1);
    PALF_LOG(INFO, "wait degrade");
  }
  return has_degraded;
}

bool ObSimpleLogClusterTestEnv::is_upgraded(PalfHandleImplGuard &leader, const int64_t palf_id)
{
  bool has_upgraded = false;
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, palf_id));
  while (!has_upgraded) {
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 1, palf_id));
    common::GlobalLearnerList degraded_learner_list;
    leader.palf_handle_impl_->config_mgr_.get_degraded_learner_list(degraded_learner_list);
    const bool is_config_change_done = (LogConfigMgr::ConfigChangeState::INIT == leader.palf_handle_impl_->config_mgr_.state_);
    has_upgraded = (0 == degraded_learner_list.get_member_number() && is_config_change_done);
    sleep(1);
    PALF_LOG(INFO, "wait upgrade", K(palf_id));
  }
  return has_upgraded;
}

int ObSimpleLogClusterTestEnv::wait_until_disk_space_to(const int64_t server_id,
                                                        const int64_t expect_log_disk_space)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  if (server_id >= 0 && server_id < cluster.size()) {
    ObTenantEnv::set_tenant(cluster[server_id]->get_tenant_base());
    auto srv = cluster[server_id];
    if  (true == srv->is_arb_server()) {
      ret = OB_NOT_SUPPORTED;
    } else {
      auto palf_env_impl = dynamic_cast<PalfEnvImpl*>(srv->get_palf_env());
      int64_t used_log_disk_space = INT64_MAX;
      int64_t total_log_disk_space = 0;
      while (used_log_disk_space >= expect_log_disk_space) {
        if (OB_FAIL(palf_env_impl->get_disk_usage(used_log_disk_space, total_log_disk_space))) {
          PALF_LOG(WARN, "get_disk_usage failed", K(used_log_disk_space), K(total_log_disk_space));
        } else {
          usleep(10*1000);
          PALF_LOG(INFO, "disk_space is not enough", K(used_log_disk_space), K(expect_log_disk_space));
        }
      }
      PALF_LOG(INFO, "wait_until_disk_space_to success", K(used_log_disk_space), K(expect_log_disk_space));
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::update_server_log_disk(const int64_t log_disk_size)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  for (auto srv : cluster) {
    srv->update_server_log_disk(log_disk_size);
  }
  return ret;
}

} // end namespace unittest
} // end namespace oceanbase
