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
  constexpr int UNIT_STR_LENGTH = 1 * 1024;
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
    std::string rand_str(payload_size, '\0');
    int cnt = 0;
    int char_idx = 0;
    for (auto& dis: rand_str) {
      dis = rand_str_unit[cnt++ % UNIT_STR_LENGTH];
    }
    memcpy(buf+pos, rand_str.c_str(), payload_size);
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
    ObTenantEnv::set_tenant(svr->tenant_base_);
    auto ls_id = ObLSID(id);
    if (OB_FAIL(svr->get_palf_env()->remove(id))) {
      CLOG_LOG(WARN, "remove_ls failed", K(ret));
      break;
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::create_paxos_group(const int64_t id, int64_t &leader_idx, PalfHandleGuard &leader)
{
  PalfBaseInfo palf_base_info;
  palf_base_info.generate_by_default();
  return create_paxos_group(id, palf_base_info, NULL, leader_idx, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group(const int64_t id, palf::PalfLocationCacheCb *loc_cb, int64_t &leader_idx, PalfHandleGuard &leader)
{
  PalfBaseInfo palf_base_info;
  palf_base_info.generate_by_default();
  return create_paxos_group(id, palf_base_info, loc_cb, leader_idx, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group(const int64_t id, const share::SCN &create_scn, int64_t &leader_idx, PalfHandleGuard &leader)
{
  int ret = OB_SUCCESS;
  PalfBaseInfo palf_base_info;
  palf_base_info.generate_by_default();
  palf_base_info.prev_log_info_.scn_ = create_scn;
  return create_paxos_group(id, palf_base_info, NULL, leader_idx, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group(const int64_t id, const LSN &lsn, int64_t &leader_idx, PalfHandleGuard &leader)
{
  int ret = OB_SUCCESS;
  PalfBaseInfo palf_base_info;
  palf_base_info.generate_by_default();
  palf_base_info.curr_lsn_ = lsn;
  return create_paxos_group(id, palf_base_info, NULL, leader_idx, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group(const int64_t id, const PalfBaseInfo &palf_base_info, int64_t &leader_idx, PalfHandleGuard &leader)
{
  return create_paxos_group(id, palf_base_info, NULL, leader_idx, leader);
}

int ObSimpleLogClusterTestEnv::create_paxos_group(const int64_t id, const PalfBaseInfo &palf_base_info, palf::PalfLocationCacheCb *loc_cb, int64_t &leader_idx, PalfHandleGuard &leader)
{
  int ret = OB_SUCCESS;
  for (auto svr : get_cluster()) {
    ObTenantEnv::set_tenant(svr->tenant_base_);
    PalfHandle handle;
    share::ObLSID sid(id);
    if (svr->get_palf_env() == NULL) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "svr is null", KPC(svr));
    } else if (OB_FAIL(svr->get_palf_env()->create(id, palf::AccessMode::APPEND, palf_base_info, handle))) {
      CLOG_LOG(WARN, "create_palf_handle_impl failed", K(ret), K(id), KPC(svr));
      break;
    } else {
      handle.set_location_cache_cb(loc_cb);
      const ObMemberList &member_list = get_member_list();
      handle.set_initial_member_list(member_list, member_list.get_member_number());
      handle.set_paxos_member_region_map(get_member_region_map());
      CLOG_LOG(INFO, "set_initial_member_list success", K(id), "addr", svr->get_addr(), K(member_list));
    }
    svr->get_palf_env()->close(handle);
  }
  if (OB_SUCC(ret)) {
    ret = get_leader(id, leader, leader_idx);
    ObTenantEnv::set_tenant(get_cluster()[leader_idx]->tenant_base_);
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::update_server_log_disk_size(const int64_t new_size)
{
  int ret = OB_SUCCESS;
  for (auto svr : get_cluster()) {
    ObTenantEnv::set_tenant(svr->tenant_base_);
    if (OB_FAIL(svr->log_block_pool_.resize(new_size))) {
      PALF_LOG(ERROR, "resize failed", K(ret), K(new_size));
      break;
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::update_disk_options(const int64_t server_id, const int64_t file_block_num)
{
  int ret = OB_SUCCESS;
  const int64_t MB = 1024 * 1024;
  PalfDiskOptions opts;
  opts.log_disk_usage_limit_size_ = file_block_num * PALF_PHY_BLOCK_SIZE;
  opts.log_disk_utilization_threshold_ = 80;
  opts.log_disk_utilization_limit_threshold_ = 95;
  auto cluster = get_cluster();
  if (server_id >= 0 && server_id < cluster.size()) {
    ObTenantEnv::set_tenant(cluster[server_id]->tenant_base_);
    ret = cluster[server_id]->get_palf_env()->palf_env_impl_.update_disk_options(opts);
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::restart_paxos_groups()
{
  int ret = OB_SUCCESS;
  int64_t node_idx = get_node_idx_base();
  for (auto svr : get_cluster()) {
    ObTenantEnv::set_tenant(svr->tenant_base_);
    if (OB_FAIL(svr->simple_restart(get_test_name(), node_idx))) {
      PALF_LOG(WARN, "simple_restart failed", K(ret), K(node_idx));
      break;
    } else {
      node_idx++;
      PALF_LOG(INFO, "restart_paxos_groups success", K(svr->get_addr()), KPC(svr));
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::restart_server(const int64_t server_id)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  if (server_id >= 0 && server_id < cluster.size()) {
    const auto svr = cluster[server_id];
    ObTenantEnv::set_tenant(svr->tenant_base_);
    if (OB_FAIL(svr->simple_restart(get_test_name(), get_node_idx_base() + server_id))) {
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

int ObSimpleLogClusterTestEnv::get_leader(const int64_t id, PalfHandleGuard &leader, int64_t &leader_idx)
{
  int ret = OB_SUCCESS;
  leader_idx = 0;
  do {
    auto cluster = get_cluster();
    for (int i = 0; i < cluster.size(); i++) {
      auto svr = cluster[i];
      PalfHandle handle;
      bool is_pending_state = false;
      ObRole role = LEADER;
      int64_t epoch;
      if (false == svr->is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "svr is invalid", K(ret), KPC(svr));
      } else if (OB_FAIL(svr->get_palf_env()->open(id, handle))) {
        PALF_LOG(WARN, "create_palf_handle_impl failed", K(ret), K(id), KPC(svr));
      } else if (OB_FAIL(handle.get_role(role, epoch, is_pending_state))) {
        PALF_LOG(WARN, "get_role failed", K(ret));
      } else if (role == LEADER
                  && false == is_pending_state
                  && !FALSE_IT(leader.set(handle, svr->get_palf_env()))) {
        leader_idx = i;
        PALF_LOG(INFO, "get_leader is", K(svr->get_addr()), K(leader_idx), K(epoch), KP(leader.palf_env_));
        break;
      }
      svr->get_palf_env()->close(handle);
    }
    if (false == leader.get_palf_handle()->is_valid()) {
      ret = OB_ENTRY_NOT_EXIST;
      PALF_LOG(WARN, "leader not exist", K(ret), K(id));
      sleep(1);
    }
  } while (OB_ENTRY_NOT_EXIST == ret);
  PALF_LOG(INFO, "get_leader finished", K(ret), K(id), K(leader_idx));
  return ret;
}

int ObSimpleLogClusterTestEnv::get_cluster_palf_handle_guard(const int64_t palf_id, std::vector<PalfHandleGuard*> &palf_list)
{
  int ret = OB_SUCCESS;
  for (auto svr : get_cluster()) {
    PalfHandle handle;
    auto guard  = OB_NEW(PalfHandleGuard, "TestLogCluster");
    if (OB_FAIL(svr->get_palf_env()->open(palf_id, handle))) {
      PALF_LOG(WARN, "open palf failed", K(ret), K(palf_id), KPC(svr));
    } else {
      guard->set(handle, svr->get_palf_env());
      palf_list.push_back(guard);
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::revert_cluster_palf_handle_guard(std::vector<PalfHandleGuard*> &palf_list)
{
  int ret = OB_SUCCESS;
  for (auto palf_handle_guard : palf_list) {
    palf_handle_guard->~PalfHandleGuard();
    OB_DELETE(PalfHandleGuard, "TestLogCluster", palf_handle_guard);
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::get_palf_handle_guard(const std::vector<PalfHandleGuard*> &palf_list,
                                                     const common::ObAddr &server,
                                                     PalfHandleGuard &palf_handle)
{
  int ret = OB_SUCCESS;
  int64_t palf_id = INVALID_PALF_ID;;
  PalfEnv *palf_env = NULL;
  for (auto handle: palf_list) {
    if (handle->palf_handle_.palf_handle_impl_->self_ == server) {
      palf_id = handle->palf_handle_.palf_handle_impl_->palf_id_;
      palf_env = handle->palf_env_;
      break;
    }
  }
  if (false == is_valid_palf_id(palf_id)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    PalfHandle handle;
    ret = palf_env->open(palf_id, handle);
    palf_handle.set(handle, palf_env);
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::switch_leader(const int64_t id, const int64_t new_leader_idx, PalfHandleGuard &leader)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  int64_t old_leader_idx = 0;
  common::ObAddr leader_addr = cluster[new_leader_idx]->get_addr();
  PalfHandleGuard old_leader;
  if (OB_FAIL(get_leader(id, old_leader, old_leader_idx))) {
    PALF_LOG(WARN, "get_leader failed", K(ret), K(id));
  } else {
    ObTenantEnv::set_tenant(cluster[old_leader_idx]->tenant_base_);
    old_leader.change_leader_to(leader_addr);
    CLOG_LOG(INFO, "switch_leader success", K(ret), "prev_leader:", cluster[prev_leader_idx_]->get_addr(), "new_leader:", cluster[new_leader_idx]->get_addr(),
             "old_leader:", cluster[old_leader_idx]->get_addr());
    prev_leader_idx_ = new_leader_idx;
    // 确保election已经切主成功.
    sleep(2);
    ret = get_leader(id, leader, old_leader_idx);
  }
  return ret;
}

void ObSimpleLogClusterTestEnv::set_need_drop_packet(const int64_t id, const bool need_drop_packet)
{
  auto cluster = get_cluster();
  cluster[id]->set_need_drop_packet(need_drop_packet);
}

int ObSimpleLogClusterTestEnv::check_replica_sync(const int64_t id, PalfHandleGuard *pf1, PalfHandleGuard *pf2, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  auto cluster = get_cluster();
  ObAddr addr1 = pf1->palf_handle_.palf_handle_impl_->self_;
  ObAddr addr2 = pf2->palf_handle_.palf_handle_impl_->self_;

  const int64_t start_ts = ObTimeUtility::current_time();
  LSN max_lsn_1, max_lsn_2;
  LSN end_lsn_1, end_lsn_2;
  if (NULL == pf1 | NULL == pf2) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(ERROR, "invalid argumemt", KP(pf1), KP(pf2));
  } else {
    while (OB_SUCC(ret)) {
      pf1->palf_handle_.get_max_lsn(max_lsn_1);
      pf2->palf_handle_.get_max_lsn(max_lsn_2);
      pf1->palf_handle_.get_end_lsn(end_lsn_1);
      pf2->palf_handle_.get_end_lsn(end_lsn_2);
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

int ObSimpleLogClusterTestEnv::submit_log(PalfHandleGuard &leader, int count, int id)
{
  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_array;
  return submit_log_impl(leader, count, id, 0, lsn_array, scn_array);
}

int ObSimpleLogClusterTestEnv::submit_log(PalfHandleGuard &leader, int count, int id, int data_len)
{
  std::vector<LSN> lsn_array;
  std::vector<SCN> scn_array;
  return submit_log_impl(leader, count, id, data_len, lsn_array, scn_array);
}

int ObSimpleLogClusterTestEnv::submit_log(PalfHandleGuard &leader, int count, int id, std::vector<LSN> &lsn_array, std::vector<SCN> &scn_array)
{
  return submit_log_impl(leader, count, id, 0, lsn_array, scn_array);
}

int ObSimpleLogClusterTestEnv::submit_log(PalfHandleGuard &leader, int count, int data_len, int id, std::vector<LSN> &lsn_array, std::vector<SCN> &scn_array)
{
  return submit_log_impl(leader, count, id, data_len, lsn_array, scn_array);
}

int ObSimpleLogClusterTestEnv::submit_log(PalfHandleGuard &leader, LSN &lsn, share::SCN &scn)
{
  const int64_t ref_ts = ObTimeUtility::current_time_ns();
  share::SCN ref_scn;
  ref_scn.convert_for_logservice(ref_ts);
  return submit_log(leader, ref_scn, lsn, scn);
}

int ObSimpleLogClusterTestEnv::submit_log(PalfHandleGuard &leader, const share::SCN &ref_scn, LSN &lsn, share::SCN &scn)
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

int ObSimpleLogClusterTestEnv::submit_log_impl(PalfHandleGuard &leader,
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
int ObSimpleLogClusterTestEnv::submit_log_impl(PalfHandleGuard &leader,
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
    buf = static_cast<char *>(ob_malloc(buf_len));
  }
  if (OB_FAIL(generate_data(buf, buf_len, real_log_data_size, data_len))) {
    PALF_LOG(ERROR, "generate_data failed", KP(buf), K(buf_len), K(real_log_data_size));
  } else if (OB_FAIL(leader.get_role(role, opts.proposal_id))) {
    PALF_LOG(WARN, "get_fole failed");
  } else {
    for (int i  = 0; i < count && OB_SUCC(ret); i++) {
      do {
        usleep(10);
        LSN lsn;
        share::SCN scn;
        ret = leader.append(opts, buf, real_log_data_size, ref_scn, lsn, scn);
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

int ObSimpleLogClusterTestEnv::raw_write(PalfHandleGuard &leader,
                                         const LSN lsn,
                                         const char *buf,
                                         const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int real_log_data_size = 0;
  PalfAppendOptions opts;
  ObRole role;
  if (OB_FAIL(leader.get_role(role, opts.proposal_id))) {
    PALF_LOG(WARN, "get_fole failed");
  } else {
    do {
      usleep(10);
      ret = (leader.get_palf_handle())->raw_write(opts, lsn, buf, buf_len);
      if (OB_SUCC(ret)) {
        PALF_LOG(INFO, "raw_write success", KR(ret), K(lsn));
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

int ObSimpleLogClusterTestEnv::change_access_mode_to_raw_write(PalfHandleGuard &leader)
{
  int ret = OB_SUCCESS;
  palf::AccessMode new_access_mode = palf::AccessMode::RAW_WRITE;
  palf::AccessMode curr_access_mode;
  int64_t proposal_id, mode_version;
  ObRole unused_role;
  const int64_t timeout_ns = 10 * 1000 * 1000 * 1000l;
  if (OB_FAIL(leader.get_access_mode(mode_version, curr_access_mode))) {
    PALF_LOG(WARN, "get_access_mode failed", K(ret));
  } else if (OB_FAIL(leader.get_role(unused_role, proposal_id))) {
    PALF_LOG(WARN, "get_access_mode failed", K(ret));
  } else if (OB_FAIL(leader.change_access_mode(proposal_id, mode_version, new_access_mode, share::SCN::min_scn()))) {
    PALF_LOG(WARN, "change_access_mode failed", K(ret));
  } else {
    PALF_LOG(INFO, "change_access_mode to raw_write success", K(ret));
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::read_log(PalfHandleGuard &leader)
{
  return read_log(leader, LSN(PALF_INITIAL_LSN_VAL));
}

int ObSimpleLogClusterTestEnv::read_log(PalfHandleGuard &leader, const LSN &lsn)
{
  int ret = OB_SUCCESS;
  PalfBufferIterator iterator;
  if (OB_FAIL(leader.seek(lsn, iterator))) {
  } else {
    while (OB_SUCCESS == ret) {
      const char *buf;
      int64_t buf_len = 0;
      share::SCN scn = share::SCN::min_scn();
      LSN log_offset;
      bool is_raw_write = false;
      if (OB_FAIL(iterator.next())) {
      } else if (OB_FAIL(iterator.get_entry(buf, buf_len, scn, log_offset, is_raw_write))) {
      } else if (true == is_raw_write){
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(ERROR, "the log mustn't be raw write", K(ret), K(iterator), K(log_offset));
      }
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::read_group_log(PalfHandleGuard &leader, LSN lsn)
{
  int ret = OB_SUCCESS;
  PalfGroupBufferIterator iterator;
  if (OB_FAIL(leader.seek(lsn, iterator))) {
  } else {
    LogGroupEntry entry;
    while (OB_SUCCESS == ret) {
      LSN log_offset;
      if (OB_FAIL(iterator.next())) {
      } else if (OB_FAIL(iterator.get_entry(entry, log_offset))) {
      } else {}
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::read_and_submit_group_log(PalfHandleGuard &leader, PalfHandleGuard &leader_raw_write)
{
  int ret = OB_SUCCESS;
  PalfGroupBufferIterator iterator;
  if (OB_FAIL(leader.seek(LSN(0), iterator))) {
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
      wait_until_has_committed(leader_raw_write, LSN(leader.palf_handle_.palf_handle_impl_->get_end_lsn()));
      PalfBufferIterator iterator_raw_write;
      if (OB_FAIL(leader_raw_write.seek(LSN(0), iterator_raw_write))) {
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
          } else if (false == is_raw_write) {
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(ERROR, "the log is must be raw_write", K(ret), K(is_raw_write), K(iterator_raw_write));
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
  int64_t size = (max_block_id - min_block_id) * PALF_BLOCK_SIZE;
  buf = nullptr;
  buf = reinterpret_cast<char *>(ob_malloc_align(4 * 1024, size, "Dummy"));
  OB_ASSERT(nullptr != buf);
  memset(buf, 0, size);
  PALF_LOG(INFO, "runlin trace ping buf after memset");
  int64_t cursor = 0;
  for (block_id_t block_id = min_block_id; block_id < max_block_id && OB_SUCC(ret); block_id++) {
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

int ObSimpleLogClusterTestEnv::read_log_from_memory(PalfHandleGuard &leader)
{
  int ret = OB_SUCCESS;
  LSN lsn(0);
  MemPalfBufferIterator iterator;
  MemoryStorage mem_storage;
  char *buf = nullptr;
  block_id_t min_block_id, max_block_id;
  LogEngine *log_engine = &leader.palf_handle_.palf_handle_impl_->log_engine_;
  if (OB_FAIL(log_engine->get_block_id_range(min_block_id, max_block_id))) {
    PALF_LOG(WARN, "get_block_id_range failed", K(ret));
  } else {
    auto func = [&leader](){return leader.palf_handle_.palf_handle_impl_->get_end_lsn();};
    if (OB_FAIL(ping_buf(min_block_id, max_block_id, buf, log_engine))) {
      PALF_LOG(ERROR, "ping_buf failed", K(ret));
    } else if (OB_FAIL(mem_storage.init(LSN(min_block_id * PALF_BLOCK_SIZE)))) {
      PALF_LOG(ERROR, "mem_storage init failed", K(ret));
    } else if (OB_FAIL(mem_storage.append(buf, PALF_BLOCK_SIZE * (max_block_id - min_block_id)))) {
      PALF_LOG(ERROR, "append failed", K(ret));
    } else if (OB_FAIL(iterator.init(LSN(min_block_id * PALF_BLOCK_SIZE), &mem_storage, func))) {
      PALF_LOG(ERROR, "init failed", K(ret));
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
    ObTenantEnv::set_tenant(svr->tenant_base_);
    PalfHandle handle;
    share::ObLSID sid(id);
    if (svr->get_palf_env() == NULL) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "svr is null", KPC(svr));
    } else if (OB_FAIL(svr->get_palf_env()->open(id, handle))) {
      CLOG_LOG(WARN, "open palf failed", K(ret), K(id), KPC(svr));
    } else if (OB_FAIL(handle.disable_sync())) {
      CLOG_LOG(WARN, "disable_sync failed", K(ret), K(id), KPC(svr));
    } else if (OB_FAIL(handle.advance_base_info(base_info, true))) {
      CLOG_LOG(WARN, "advance_base_info failed", K(ret), K(handle));
    } else if (OB_FAIL(handle.enable_sync())) {
      CLOG_LOG(WARN, "enable_sync failed", K(ret), K(id), KPC(svr));
    } else {
    }
    svr->get_palf_env()->close(handle);
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
    palf_env = cluster[server_idx]->get_palf_env();
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::wait_until_has_committed(PalfHandleGuard &leader, const LSN &lsn)
{
  int ret = OB_SUCCESS;
  LSN end_lsn;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(leader.get_end_lsn(end_lsn))) {
    } else if (end_lsn < lsn) {
      usleep(1000);
    } else {
      break;
    }
  }
  return ret;
}

int ObSimpleLogClusterTestEnv::wait_lsn_until_flushed(const LSN &lsn, PalfHandleGuard &guard)
{
  int ret = OB_SUCCESS;

  int64_t print_log_time = OB_INVALID_TIMESTAMP;
  LSN max_flushed_end_lsn = guard.palf_handle_.palf_handle_impl_->sw_.max_flushed_end_lsn_;
  while (lsn > max_flushed_end_lsn) {
    usleep(5*1000);
    if (palf_reach_time_interval(1*1000*1000, print_log_time)) {
      PALF_LOG(WARN, "wait_lsn_until_flushed", K(ret), K(max_flushed_end_lsn), K(lsn));
    }
    max_flushed_end_lsn = guard.palf_handle_.palf_handle_impl_->sw_.max_flushed_end_lsn_;
  }
  return ret;
}

} // end namespace unittest
} // end namespace oceanbase
