// owner: zjf225077
// owner group: log

// Copyright (c) 2021 OceanBase
// SPDX-License-Identifier: Apache-2.0
#include <unistd.h>
#include <string>

#include "prometheus/gauge.h"
#include "prometheus/registry.h"
#include "prometheus/text_serializer.h"

#ifdef ERRSIM
#include "lib/utility/ob_tracepoint.h"
#endif
#include "mittest/ob_mittest_utils.h"

#define private public
#include "env/ob_simple_log_cluster_env.h"
#undef private

const std::string TEST_NAME = "single_arb_server";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
namespace unittest
{

class TestObSimpleMutilArbServer : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleMutilArbServer() :  ObSimpleLogClusterTestEnv()
  {}
#ifdef ERRSIM
protected:
  void expect_ls_metrics_disabled_(const int64_t ls_id);
#endif
};

#ifdef ERRSIM
void TestObSimpleMutilArbServer::expect_ls_metrics_disabled_(const int64_t ls_id)
{
  PalfHandleLiteGuard guard;
  ASSERT_EQ(OB_SUCCESS, get_arb_member_guard(ls_id, guard));
  ASSERT_TRUE(guard.is_valid());
  EXPECT_EQ(nullptr, guard.palf_handle_lite_->election_counter_.impl_);
  EXPECT_EQ(nullptr, guard.palf_handle_lite_->config_version_proposal_id_gauge_.impl_);
  EXPECT_EQ(nullptr, guard.palf_handle_lite_->config_version_config_seq_gauge_.impl_);
  EXPECT_EQ(nullptr, guard.palf_handle_lite_->mode_version_gauge_.impl_);
  EXPECT_EQ(nullptr, guard.palf_handle_lite_->ls_has_leader_gauge_.impl_);
}
#endif

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
bool ObSimpleLogClusterTestBase::need_add_arb_server_ = true;
bool ObSimpleLogClusterTestBase::need_shared_storage_ = false;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;

bool check_dir_exist(const char *base_dir, const int64_t id)
{
  char dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  snprintf(dir, OB_MAX_FILE_NAME_LENGTH, "%s/tenant_%ld", base_dir, id);
  int ret = OB_SUCCESS;
  bool result = false;
  if (OB_FAIL(FileDirectoryUtils::is_exists(dir, result))) {
    CLOG_LOG(WARN, "dir is not exist", K(ret), K(errno), K(dir), K(dir));
  }
  return result;
}

TEST(ObArbPrometheus, metrics_port_conflict_does_not_block_monitor_init)
{
  arbserver::ObArbMonitor monitor;
  int reserved_fd = -1;
  int occupied_fd = -1;
  const int64_t port = get_rpc_port(reserved_fd);
  DEFER(
    if (0 <= occupied_fd) {
      (void) ::close(occupied_fd);
    }
    if (0 <= reserved_fd) {
      (void) ::close(reserved_fd);
    }
  );
  ASSERT_GT(port, 0);
  ASSERT_EQ(0, listen_occupy_port(port, occupied_fd));

  EXPECT_EQ(OB_SUCCESS, monitor.init(port));
  EXPECT_EQ(nullptr, monitor.exposer_.get());
  EXPECT_FALSE(monitor.get_registry().is_valid());
  EXPECT_EQ(OB_SUCCESS, monitor.update_system_metrics("/tmp"));
  monitor.destroy();
}

TEST(ObArbPrometheus, disabled_registry_does_not_block_rpc_metrics)
{
  arbserver::ObArbMetricRegistry disabled_registry;
  arbserver::ObArbMetricRegistry another_registry;
  arbserver::ObArbSrvRpcXlator rpc_xlator;
  ASSERT_EQ(OB_SUCCESS, rpc_xlator.init());
  EXPECT_EQ(OB_SUCCESS, rpc_xlator.set_registry(disabled_registry));
  EXPECT_EQ(OB_INIT_TWICE, rpc_xlator.set_registry(another_registry));
  rpc_xlator.destroy();
}

TEST_F(TestObSimpleMutilArbServer, rpc_metrics_follow_registry_after_restart)
{
  const char *const RPC_PROCESS_METRIC_HELP =
      "# HELP arb_rpc_process_latency_seconds ";
  const char *const RPC_TRANSPORT_METRIC_HELP =
      "# HELP arb_rpc_transport_latency_seconds ";
  ObISimpleLogServer *server = get_cluster()[0];
  ObSimpleArbServer *arb_server = nullptr;
  prometheus::TextSerializer serializer;
  std::string metrics_text;

  SET_CASE_LOG_FILE(TEST_NAME, "rpc_metrics_follow_registry_after_restart");
  ASSERT_NE(nullptr, server);
  ASSERT_TRUE(server->is_arb_server());
  arb_server = static_cast<ObSimpleArbServer *>(server);

  ASSERT_TRUE(arb_server->palf_env_mgr_.get_arb_monitor()->get_registry().is_valid());
  metrics_text = serializer.Serialize(
      arb_server->palf_env_mgr_.get_arb_monitor()->get_registry().impl_->Collect());
  ASSERT_NE(std::string::npos, metrics_text.find(RPC_PROCESS_METRIC_HELP));
  ASSERT_NE(std::string::npos, metrics_text.find(RPC_TRANSPORT_METRIC_HELP));

  ASSERT_EQ(OB_SUCCESS, restart_server(0));
  ASSERT_TRUE(arb_server->palf_env_mgr_.get_arb_monitor()->get_registry().is_valid());
  metrics_text = serializer.Serialize(
      arb_server->palf_env_mgr_.get_arb_monitor()->get_registry().impl_->Collect());
  EXPECT_NE(std::string::npos, metrics_text.find(RPC_PROCESS_METRIC_HELP));
  EXPECT_NE(std::string::npos, metrics_text.find(RPC_TRANSPORT_METRIC_HELP));
}

TEST(ObArbPrometheus, disable_monitoring_releases_port)
{
  arbserver::ObArbMonitor monitor;
  int reserved_fd = -1;
  int rebound_fd = -1;
  const int64_t port = get_rpc_port(reserved_fd);
  DEFER(
    if (0 <= rebound_fd) {
      (void) ::close(rebound_fd);
    }
    if (0 <= reserved_fd) {
      (void) ::close(reserved_fd);
    }
  );
  ASSERT_GT(port, 0);
  ASSERT_EQ(OB_SUCCESS, monitor.init(port));
  ASSERT_NE(nullptr, monitor.exposer_.get());
  monitor.disable_monitoring();
  monitor.disable_monitoring();
  EXPECT_EQ(nullptr, monitor.exposer_.get());
  EXPECT_FALSE(monitor.get_registry().is_valid());
  EXPECT_EQ(OB_SUCCESS, monitor.update_system_metrics("/tmp"));
  EXPECT_EQ(0, listen_occupy_port(port, rebound_fd));
  monitor.destroy();
}

#ifdef ERRSIM
TEST_F(TestObSimpleMutilArbServer, metric_add_exception_does_not_block_ls_creation)
{
  const int64_t cluster_id = ObSimpleArbServer::cluster_id_;
  const uint64_t tenant_id = ObISimpleLogServer::DEFAULT_TENANT_ID;
  const int64_t first_ls_id = 81001;
  const int64_t second_ls_id = 81002;
  const palflite::PalfEnvKey env_key(cluster_id, tenant_id);
  const std::string rolled_back_series =
      "arb_ls_election_count{cluster_id=\"" + std::to_string(cluster_id)
      + "\",ls_id=\"" + std::to_string(first_ls_id)
      + "\",tenant_id=\"" + std::to_string(tenant_id) + "\"}";
  ObISimpleLogServer *server = get_cluster()[0];
  ObSimpleArbServer *arb_server = nullptr;
  palflite::PalfEnvLiteMgr *palf_env_mgr = nullptr;
  arbserver::ObArbMetricRegistry *registry = nullptr;
  prometheus::TextSerializer serializer;
  std::string metrics_text;
  common::EventItem errsim_item;
  bool is_event_set = false;
  bool is_first_arbitration_created = false;
  bool is_second_arbitration_created = false;
  bool original_metric_registration_state = false;

  DEFER({
    common::EventItem clear_item;
    if (is_event_set) {
      EXPECT_EQ(OB_SUCCESS,
          common::EventTable::instance().set_event(
              "ERRSIM_ARB_GAUGE_FAMILY_ADD_EXCEPTION", clear_item));
    }
    if (is_second_arbitration_created) {
      EXPECT_EQ(OB_SUCCESS, palf_env_mgr->delete_arbitration_instance(
          env_key, arb_server->self_, second_ls_id));
    }
    if (is_first_arbitration_created) {
      EXPECT_EQ(OB_SUCCESS, palf_env_mgr->delete_arbitration_instance(
          env_key, arb_server->self_, first_ls_id));
    }
    if (nullptr != palf_env_mgr) {
      ATOMIC_STORE(&palf_env_mgr->allow_ls_metric_registration_,
                   original_metric_registration_state);
      EXPECT_EQ(original_metric_registration_state,
                palf_env_mgr->can_register_ls_metrics());
    }
  });

  SET_CASE_LOG_FILE(TEST_NAME, "metric_add_exception_does_not_block_ls_creation");
  ASSERT_NE(nullptr, server);
  ASSERT_TRUE(server->is_arb_server());
  arb_server = static_cast<ObSimpleArbServer *>(server);
  palf_env_mgr = &arb_server->palf_env_mgr_;
  original_metric_registration_state =
      palf_env_mgr->can_register_ls_metrics();
  ASSERT_TRUE(original_metric_registration_state);

  errsim_item.error_code_ = OB_ERR_UNEXPECTED;
  errsim_item.occur_ = 1;
  errsim_item.trigger_freq_ = 0;
  errsim_item.cond_ = cluster_id;
  ASSERT_EQ(OB_SUCCESS,
      common::EventTable::instance().set_event(
          "ERRSIM_ARB_GAUGE_FAMILY_ADD_EXCEPTION", errsim_item));
  is_event_set = true;
  ASSERT_EQ(OB_SUCCESS, palf_env_mgr->create_arbitration_instance(
      env_key, arb_server->self_, first_ls_id,
      ObTenantRole(ObTenantRole::PRIMARY_TENANT)));
  is_first_arbitration_created = true;
  EXPECT_FALSE(palf_env_mgr->can_register_ls_metrics());
  expect_ls_metrics_disabled_(first_ls_id);

  registry = &palf_env_mgr->get_arb_monitor()->get_registry();
  ASSERT_TRUE(registry->is_valid());
  ASSERT_NE(nullptr, registry->impl_.get());
  EXPECT_NE(nullptr, palf_env_mgr->get_ls_election_count_family().impl_);
  metrics_text = serializer.Serialize(registry->impl_->Collect());
  EXPECT_NE(std::string::npos, metrics_text.find("# HELP arb_cluster_count "));
  EXPECT_EQ(std::string::npos, metrics_text.find(rolled_back_series));

  ASSERT_EQ(OB_SUCCESS, palf_env_mgr->create_arbitration_instance(
      env_key, arb_server->self_, second_ls_id,
      ObTenantRole(ObTenantRole::PRIMARY_TENANT)));
  is_second_arbitration_created = true;
  expect_ls_metrics_disabled_(second_ls_id);
}
#endif

TEST_F(TestObSimpleMutilArbServer, null_palf_env_is_rejected_by_load)
{
  ObISimpleLogServer *server = get_cluster()[0];
  ObSimpleArbServer *arb_server = nullptr;
  PalfEnvLiteGuard env_guard;
  palflite::PalfEnvLite *palf_env = nullptr;
  palflite::PalfHandleLite palf_handle;
  bool is_integrity = true;
  int64_t loaded_palf_id = palf::INVALID_PALF_ID;

  SET_CASE_LOG_FILE(TEST_NAME, "null_palf_env_is_rejected_by_load");
  ASSERT_NE(nullptr, server);
  ASSERT_TRUE(server->is_arb_server());
  arb_server = static_cast<ObSimpleArbServer *>(server);
  ASSERT_EQ(OB_SUCCESS, arb_server->get_palf_env_lite(
      ObISimpleLogServer::DEFAULT_TENANT_ID, env_guard));
  ASSERT_NE(nullptr, env_guard.palf_env_lite_);
  palf_env = env_guard.palf_env_lite_;

  EXPECT_EQ(OB_INVALID_ARGUMENT,
      palf_handle.load(82002, palf_env->log_dir_, palf_env->log_alloc_mgr_,
          palf_env->log_block_pool_, &palf_env->log_rpc_, &palf_env->log_io_worker_,
          nullptr, palf_env->self_, &palf_env->election_timer_, 1,
          arb_server->palf_env_mgr_.get_io_adapter(), is_integrity));
  EXPECT_EQ(OB_NOT_INIT, palf_handle.get_palf_id(loaded_palf_id));
}

TEST_F(TestObSimpleMutilArbServer, create_mutil_tenant)
{
  SET_CASE_LOG_FILE(TEST_NAME, "create_mutil_tenant");
  OB_LOGGER.set_log_level("TRACE");
  ObISimpleLogServer *iserver = get_cluster()[0];
  EXPECT_EQ(true, iserver->is_arb_server());
  ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(iserver);
  palflite::PalfEnvLiteMgr *palf_env_mgr = &arb_server->palf_env_mgr_;
  int64_t cluster_id = 1;
  arbserver::GCMsgEpoch epoch = arbserver::GCMsgEpoch(1, 1);
  EXPECT_EQ(OB_ARBITRATION_SERVICE_ALREADY_EXIST, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_id, "arbserver_test", epoch));
  sleep(2);
  CLOG_LOG(INFO, "one tenant");
  sleep(2);
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->create_palf_env_lite(palflite::PalfEnvKey(cluster_id, 1)));
  sleep(2);
  CLOG_LOG(INFO, "two tenant");
  sleep(2);
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->create_palf_env_lite(palflite::PalfEnvKey(cluster_id, 2)));
  sleep(2);
  CLOG_LOG(INFO, "three tenant");
  sleep(2);
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->create_palf_env_lite(palflite::PalfEnvKey(cluster_id, 3)));
  sleep(2);
  CLOG_LOG(INFO, "four tenant");
  sleep(2);
  {
    PalfBaseInfo info_2; info_2.generate_by_default();
    AccessMode mode(palf::AccessMode::APPEND);
    IPalfHandleImpl *ipalf_handle_impl_2 = NULL;
    palflite::PalfEnvLite *palf_env_lite_2 = NULL;
    EXPECT_EQ(OB_SUCCESS, palf_env_mgr->palf_env_lite_map_.get(palflite::PalfEnvKey(cluster_id, 2), palf_env_lite_2));
    EXPECT_EQ(OB_SUCCESS, palf_env_lite_2->create_palf_handle_impl(1, mode, info_2,
        palf::LogReplicaType::ARBITRATION_REPLICA, ipalf_handle_impl_2));
    palf_env_lite_2->revert_palf_handle_impl(ipalf_handle_impl_2);
    palf_env_mgr->revert_palf_env_lite(palf_env_lite_2);
    CLOG_LOG(INFO, "revert_palf_env_lite2");
  }
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_palf_env_lite(palflite::PalfEnvKey(cluster_id, 1)));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_palf_env_lite(palflite::PalfEnvKey(cluster_id, 2)));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_palf_env_lite(palflite::PalfEnvKey(cluster_id, 3)));
  EXPECT_EQ(false, check_dir_exist(palf_env_mgr->base_dir_, 1));
  EXPECT_EQ(false, check_dir_exist(palf_env_mgr->base_dir_, 2));
  EXPECT_EQ(false, check_dir_exist(palf_env_mgr->base_dir_, 3));
  CLOG_LOG(INFO, "before restart_paxos_groups1");
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  auto &map = palf_env_mgr->cluster_meta_info_map_;
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->create_palf_env_lite(palflite::PalfEnvKey(cluster_id, 1)));
  palflite::ClusterMetaInfo info;
  EXPECT_EQ(OB_SUCCESS, map.get_refactored(cluster_id, info));
  EXPECT_EQ(2, info.tenant_count_);
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->create_palf_env_lite(palflite::PalfEnvKey(cluster_id, 2)));
  EXPECT_EQ(OB_SUCCESS, map.get_refactored(cluster_id, info));
  EXPECT_EQ(3, info.tenant_count_);
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->create_palf_env_lite(palflite::PalfEnvKey(cluster_id, 3)));
  EXPECT_EQ(OB_SUCCESS, map.get_refactored(cluster_id, info));
  EXPECT_EQ(4, info.tenant_count_);
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->create_arbitration_instance(
        palflite::PalfEnvKey(cluster_id, 1), arb_server->self_, 1001,
        ObTenantRole(ObTenantRole::PRIMARY_TENANT)));
  auto update_meta = [](palflite::PalfEnvLiteMgr::MapPair &pair) -> void{
    auto &info = pair.second;
    info.epoch_ = arbserver::GCMsgEpoch(100, 100);
  };
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->update_cluster_meta_info_(cluster_id, update_meta));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->get_cluster_meta_info_(cluster_id, info));
  EXPECT_EQ(arbserver::GCMsgEpoch(100, 100), info.epoch_);
  CLOG_LOG(INFO, "before restart_paxos_groups2");
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  {
    palflite::PalfEnvLite *palf_env_lite_2 = NULL;
    EXPECT_EQ(OB_SUCCESS, palf_env_mgr->palf_env_lite_map_.get(palflite::PalfEnvKey(cluster_id, 2), palf_env_lite_2));
    palf_env_mgr->revert_palf_env_lite(palf_env_lite_2);
  }
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_palf_env_lite(palflite::PalfEnvKey(cluster_id, 1)));
  EXPECT_EQ(OB_SUCCESS, map.get_refactored(cluster_id, info));
  EXPECT_EQ(3, info.tenant_count_);
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_palf_env_lite(palflite::PalfEnvKey(cluster_id, 2)));
  EXPECT_EQ(OB_SUCCESS, map.get_refactored(cluster_id, info));
  EXPECT_EQ(2, info.tenant_count_);
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_palf_env_lite(palflite::PalfEnvKey(cluster_id, 3)));
  EXPECT_EQ(OB_SUCCESS, map.get_refactored(cluster_id, info));
  EXPECT_EQ(1, info.tenant_count_);
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, palf_env_mgr->remove_palf_env_lite(palflite::PalfEnvKey(cluster_id, 1001)));
  CLOG_LOG(INFO, "end restart_paxos_groups");
}

TEST_F(TestObSimpleMutilArbServer, out_interface)
{
  SET_CASE_LOG_FILE(TEST_NAME, "out_interface");
  OB_LOGGER.set_log_level("TRACE");
  ObISimpleLogServer *iserver = get_cluster()[0];
  EXPECT_EQ(true, iserver->is_arb_server());
  ObMember member(iserver->get_addr(), 100);
  ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(iserver);
  ObTenantRole tenant_role(ObTenantRole::PRIMARY_TENANT);
  int64_t cluster_id = 1;
  GlobalLearnerList learner_list;
  arbserver::GCMsgEpoch epoch = arbserver::GCMsgEpoch(1, 1);
  EXPECT_EQ(OB_ARBITRATION_SERVICE_ALREADY_EXIST, arb_server->palf_env_mgr_.add_cluster(
            iserver->get_addr(), cluster_id, "arbserver_test", epoch));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, arb_server->palf_env_mgr_.set_initial_member_list(
        palflite::PalfEnvKey(cluster_id, 1), arb_server->self_,
        1000, get_member_list(), member, get_member_cnt(), learner_list));
  EXPECT_EQ(OB_SUCCESS, arb_server->palf_env_mgr_.create_arbitration_instance(
        palflite::PalfEnvKey(cluster_id, 1), arb_server->self_,
        1000, tenant_role));
  EXPECT_EQ(OB_SUCCESS, arb_server->palf_env_mgr_.create_arbitration_instance(
        palflite::PalfEnvKey(cluster_id, 1), arb_server->self_,
        1000, tenant_role));
  EXPECT_EQ(OB_NOT_SUPPORTED, arb_server->palf_env_mgr_.set_initial_member_list(
        palflite::PalfEnvKey(cluster_id, 1), arb_server->self_,
        1000, get_member_list(), member, get_member_cnt(), learner_list));
  ObMemberList member_list = get_member_list();
  member_list.add_server(arb_server->self_);
  EXPECT_EQ(OB_NOT_SUPPORTED, arb_server->palf_env_mgr_.set_initial_member_list(
        palflite::PalfEnvKey(cluster_id, 1), arb_server->self_,
        1000, member_list, member, get_member_cnt(), learner_list));
  EXPECT_EQ(OB_SUCCESS, arb_server->palf_env_mgr_.delete_arbitration_instance(
        palflite::PalfEnvKey(cluster_id, 1), arb_server->self_, 1000));
  palflite::PalfEnvLite *palf_env_lite = NULL;
  EXPECT_EQ(OB_SUCCESS, arb_server->palf_env_mgr_.get_palf_env_lite(
        palflite::PalfEnvKey(cluster_id, ObISimpleLogServer::DEFAULT_TENANT_ID), palf_env_lite));
  arb_server->palf_env_mgr_.revert_palf_env_lite(palf_env_lite);
  EXPECT_EQ(OB_SUCCESS, arb_server->palf_env_mgr_.remove_palf_env_lite(
        palflite::PalfEnvKey(cluster_id, ObISimpleLogServer::DEFAULT_TENANT_ID)));
  EXPECT_EQ(OB_SUCCESS, arb_server->palf_env_mgr_.delete_arbitration_instance(
        palflite::PalfEnvKey(cluster_id, 1), arb_server->self_, 1000));
  CLOG_LOG(INFO, "end test out_interface");
}

TEST_F(TestObSimpleMutilArbServer, gc_tenant_updates_arb_ls_count)
{
  SET_CASE_LOG_FILE(TEST_NAME, "gc_tenant_updates_arb_ls_count");
  ObISimpleLogServer *iserver = get_cluster()[0];
  ASSERT_TRUE(iserver->is_arb_server());
  ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(iserver);
  ASSERT_NE(nullptr, arb_server);
  palflite::PalfEnvLiteMgr *palf_env_mgr = &arb_server->palf_env_mgr_;
  ASSERT_NE(nullptr, palf_env_mgr->ls_count_.impl_);

  const double baseline_ls_count = palf_env_mgr->ls_count_.impl_->Value();
  const int64_t cluster_id = 2001;
  const uint64_t removed_tenant_id = 3001;
  const uint64_t retained_tenant_id = 3002;
  const char *cluster_name = "arb_ls_count_test";
  const arbserver::GCMsgEpoch initial_epoch(1, 1);
  const arbserver::GCMsgEpoch gc_epoch(2, 1);
  const ObTenantRole tenant_role(ObTenantRole::PRIMARY_TENANT);
  const int64_t removed_ls_ids[] = {1, 1001, 1002};
  const palflite::PalfEnvKey removed_tenant_key(cluster_id, removed_tenant_id);
  const palflite::PalfEnvKey retained_tenant_key(cluster_id, retained_tenant_id);

  ASSERT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(
      arb_server->self_, cluster_id, cluster_name, initial_epoch));
  for (const int64_t ls_id : removed_ls_ids) {
    ASSERT_EQ(OB_SUCCESS, palf_env_mgr->create_arbitration_instance(
        removed_tenant_key, arb_server->self_, ls_id, tenant_role));
  }
  ASSERT_EQ(OB_SUCCESS, palf_env_mgr->create_arbitration_instance(
      retained_tenant_key, arb_server->self_, 1, tenant_role));
  EXPECT_DOUBLE_EQ(baseline_ls_count + ARRAYSIZEOF(removed_ls_ids) + 1,
                   palf_env_mgr->ls_count_.impl_->Value());

  arbserver::TenantLSIDS retained_tenant_ls_ids;
  ASSERT_EQ(OB_SUCCESS, retained_tenant_ls_ids.push_back(ObLSID(1)));
  retained_tenant_ls_ids.set_max_ls_id(
      arbserver::TenantLSID(retained_tenant_id, ObLSID(1)));
  arbserver::TenantLSIDSArray gc_ls_ids;
  ASSERT_EQ(OB_SUCCESS, gc_ls_ids.push_back(retained_tenant_ls_ids));
  gc_ls_ids.set_max_tenant_id(retained_tenant_id);

  ASSERT_EQ(OB_SUCCESS, palf_env_mgr->handle_gc_message(
      gc_epoch, arb_server->self_, cluster_id, gc_ls_ids));
  EXPECT_DOUBLE_EQ(baseline_ls_count + 1, palf_env_mgr->ls_count_.impl_->Value());
  ASSERT_EQ(OB_SUCCESS, palf_env_mgr->handle_gc_message(
      gc_epoch, arb_server->self_, cluster_id, gc_ls_ids));
  EXPECT_DOUBLE_EQ(baseline_ls_count + 1, palf_env_mgr->ls_count_.impl_->Value());

  ASSERT_EQ(OB_SUCCESS, palf_env_mgr->remove_cluster(
      arb_server->self_, cluster_id, cluster_name, gc_epoch));
  EXPECT_DOUBLE_EQ(baseline_ls_count, palf_env_mgr->ls_count_.impl_->Value());
}

TEST_F(TestObSimpleMutilArbServer, create_mutil_cluster)
{
  SET_CASE_LOG_FILE(TEST_NAME, "create_mutil_cluster");
  OB_LOGGER.set_log_level("TRACE");
  ObISimpleLogServer *iserver = get_cluster()[0];
  EXPECT_EQ(true, iserver->is_arb_server());
  ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(iserver);
  palflite::PalfEnvLiteMgr *palf_env_mgr = &arb_server->palf_env_mgr_;
  std::vector<int64_t> cluster_ids = {2, 3, 4, 5, 6, 7};
  arbserver::GCMsgEpoch epoch = arbserver::GCMsgEpoch(1, 1);

  // test add tenant without cluster, generate placeholder
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->create_palf_env_lite(palflite::PalfEnvKey(cluster_ids[0], 1)));
  EXPECT_TRUE(palf_env_mgr->is_cluster_placeholder_exists(cluster_ids[0]));
  EXPECT_EQ(OB_ARBITRATION_SERVICE_ALREADY_EXIST, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[0], "arbserver_test", epoch));
  EXPECT_EQ(OB_ARBITRATION_SERVICE_ALREADY_EXIST, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[0], "", epoch));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_cluster(iserver->get_addr(), cluster_ids[0], "", epoch));

  // test add cluster and restart
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[0], "arbserver_test", epoch));
  // duplicate add_cluster
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[0], "arbserver_test", epoch));
  // wrong add_cluster
  EXPECT_EQ(OB_ARBITRATION_SERVICE_ALREADY_EXIST, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[0], "arbserver_test1", epoch));
  EXPECT_EQ(OB_ARBITRATION_SERVICE_ALREADY_EXIST, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[0], "arbserver_test", arbserver::GCMsgEpoch(1, 0)));

  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[1], "arbserver_test", epoch));
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  // re-create cluster, cluster_name match, return OB_SUCCESS
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[0], "arbserver_test", epoch));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[1], "arbserver_test", epoch));
  // re-create cluster, cluster_name do not match, return OB_SUCCESS
  EXPECT_EQ(OB_ARBITRATION_SERVICE_ALREADY_EXIST, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[0], "arbserver_test1", epoch));
  EXPECT_EQ(OB_ARBITRATION_SERVICE_ALREADY_EXIST, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[1], "arbserver_test1", epoch));

  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[2], "arbserver_test", epoch));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[3], "arbserver_test", epoch));

  // empty cluster_name
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[4], "", epoch));
  EXPECT_EQ(OB_ARBITRATION_SERVICE_ALREADY_EXIST, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[4], "test", epoch));

  // long cluster_name
  char *long_cluster_name = new char[OB_MAX_CLUSTER_NAME_LENGTH + 1];
  MEMSET(long_cluster_name, '\0', OB_MAX_CLUSTER_NAME_LENGTH + 1);
  MEMSET(long_cluster_name, 'a', OB_MAX_CLUSTER_NAME_LENGTH);
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[5], long_cluster_name, epoch));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[5], long_cluster_name, epoch));
  EXPECT_TRUE(palf_env_mgr->is_cluster_placeholder_exists(cluster_ids[0]));
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
  EXPECT_TRUE(palf_env_mgr->is_cluster_placeholder_exists(cluster_ids[0]));
  palflite::ClusterMetaInfo long_cluster_meta_info;
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->get_cluster_meta_info_(cluster_ids[5], long_cluster_meta_info));
  EXPECT_EQ(0, strcmp(long_cluster_name, long_cluster_meta_info.cluster_name_));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_cluster(iserver->get_addr(), cluster_ids[5], long_cluster_name, epoch));

  // test remove_cluster
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_cluster(iserver->get_addr(), cluster_ids[0], "arbserver_test", epoch));
  // duplicate remove_cluster
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_cluster(iserver->get_addr(), cluster_ids[0], "arbserver_test", epoch));

  // remove_cluster with wrong cluster_name, still success
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_cluster(iserver->get_addr(), cluster_ids[1], "arbserver_test1", epoch));

  // improve epoch and remove cluster
  arbserver::GCMsgEpoch improved_epoch = arbserver::GCMsgEpoch(2, 1);
  arbserver::TenantLSIDSArray ls_ids;
  ls_ids.set_max_tenant_id(UINT64_MAX);
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->handle_gc_message_(improved_epoch, iserver->get_addr(), cluster_ids[2], ls_ids));
  EXPECT_EQ(OB_OP_NOT_ALLOW, palf_env_mgr->remove_cluster(iserver->get_addr(), cluster_ids[2], "arbserver_test", epoch));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_cluster(iserver->get_addr(), cluster_ids[2], "arbserver_test", improved_epoch));

  // normal remove_cluster
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_cluster(iserver->get_addr(), cluster_ids[1], "arbserver_test", epoch));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_cluster(iserver->get_addr(), cluster_ids[3], "arbserver_test", epoch));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_cluster(iserver->get_addr(), cluster_ids[4], "", epoch));

  // test upgrade arbserver, cluster 2 has a tenant and cluster 3 do not
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[0], "arbserver_test", epoch));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->add_cluster(iserver->get_addr(), cluster_ids[1], "arbserver_test", epoch));
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->create_palf_env_lite(palflite::PalfEnvKey(cluster_ids[0], 1)));
  EXPECT_TRUE(palf_env_mgr->is_cluster_placeholder_exists(cluster_ids[0]));
  EXPECT_TRUE(palf_env_mgr->is_cluster_placeholder_exists(cluster_ids[1]));

  std::string cluster_dir1 = palf_env_mgr->base_dir_;
  cluster_dir1 += "/cluster_2_clustername_arbserver_test";
  cluster_dir1 = "rm -rf " + cluster_dir1;
  std::string cluster_dir2 = palf_env_mgr->base_dir_;
  cluster_dir2 += "/cluster_3_clustername_arbserver_test";
  cluster_dir2 = "rm -rf " + cluster_dir2;
  SERVER_LOG(INFO, "delete cluster placeholder manually", K(cluster_dir1.c_str()), K(cluster_dir2.c_str()));

  system(cluster_dir1.c_str());
  system(cluster_dir2.c_str());
  EXPECT_FALSE(palf_env_mgr->is_cluster_placeholder_exists(cluster_ids[0]));
  EXPECT_FALSE(palf_env_mgr->is_cluster_placeholder_exists(cluster_ids[1]));

  // restart and regenerate cluster placeholder dir for cluster 2
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());

  EXPECT_TRUE(palf_env_mgr->is_cluster_placeholder_exists(cluster_ids[0]));
  EXPECT_FALSE(palf_env_mgr->is_cluster_placeholder_exists(cluster_ids[1]));

  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->remove_cluster(iserver->get_addr(), cluster_ids[0], "arbserver_test", epoch));
  EXPECT_FALSE(palf_env_mgr->is_cluster_placeholder_exists(cluster_ids[0]));
}

TEST_F(TestObSimpleMutilArbServer, restart_arb)
{
  SET_CASE_LOG_FILE(TEST_NAME, "restart_arb");
  OB_LOGGER.set_log_level("TRACE");
  ObISimpleLogServer *iserver = get_cluster()[0];
  EXPECT_EQ(true, iserver->is_arb_server());
  ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(iserver);
  palflite::PalfEnvLiteMgr *palf_env_mgr = &arb_server->palf_env_mgr_;
  std::vector<int64_t> cluster_ids = {2, 3, 4, 5, 6, 7};
  arbserver::GCMsgEpoch epoch = arbserver::GCMsgEpoch(1, 1);

  // test add tenant without cluster, generate placeholder
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->create_palf_env_lite(palflite::PalfEnvKey(cluster_ids[0], 1)));
  EXPECT_TRUE(palf_env_mgr->is_cluster_placeholder_exists(cluster_ids[0]));

  palflite::PalfEnvLite *palf_env_lite = NULL;
  IPalfHandleImpl *ipalf_handle_impl = NULL;
  {
    PalfBaseInfo info; info.generate_by_default();
    AccessMode mode(palf::AccessMode::APPEND);
    EXPECT_EQ(OB_SUCCESS, palf_env_mgr->get_palf_env_lite(palflite::PalfEnvKey(cluster_ids[0], 1), palf_env_lite));
    EXPECT_EQ(OB_SUCCESS, palf_env_lite->create_palf_handle_impl(1, mode, info,
        palf::LogReplicaType::ARBITRATION_REPLICA, ipalf_handle_impl));
    palflite::PalfHandleLite *palf_handle_lite = dynamic_cast<palflite::PalfHandleLite*>(ipalf_handle_impl);
    ASSERT_NE(nullptr, palf_handle_lite);
    LogEngine *log_engine = &palf_handle_lite->log_engine_;
    LogMeta log_meta = log_engine->log_meta_;
    int count = (2 * 1024 * 1024 - log_engine->log_meta_storage_.log_tail_.val_) / 4096;
    while (count > 0) {
      EXPECT_EQ(OB_SUCCESS, log_engine->append_log_meta_(log_meta));
      count --;
    }
    while (log_engine->log_meta_storage_.log_tail_ != LSN(2*1024*1024)) {
      sleep(1);
    }
    sleep(1);
    EXPECT_EQ(2*1024*1024, log_engine->log_meta_storage_.log_tail_);
    EXPECT_EQ(OB_SUCCESS, log_engine->log_meta_storage_.block_mgr_.switch_next_block(1));
  }
  palf_env_lite->revert_palf_handle_impl(ipalf_handle_impl);
  palf_env_mgr->revert_palf_env_lite(palf_env_lite);
  EXPECT_EQ(OB_SUCCESS, restart_server(0));
}

TEST_F(TestObSimpleMutilArbServer, multi_thread)
{
  SET_CASE_LOG_FILE(TEST_NAME, "restart_arb");
  OB_LOGGER.set_log_level("TRACE");
  ObISimpleLogServer *iserver = get_cluster()[0];
  EXPECT_EQ(true, iserver->is_arb_server());
  ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(iserver);
  palflite::PalfEnvLiteMgr *palf_env_mgr = &arb_server->palf_env_mgr_;
  ASSERT_NE(nullptr, palf_env_mgr->ls_count_.impl_);
  const double baseline_ls_count = palf_env_mgr->ls_count_.impl_->Value();
  int64_t cluster_id = 100;
  arbserver::GCMsgEpoch epoch = arbserver::GCMsgEpoch(1, 1);

  // test add tenant without cluster, generate placeholder
  EXPECT_EQ(OB_SUCCESS, palf_env_mgr->create_palf_env_lite(palflite::PalfEnvKey(cluster_id, 1)));
  EXPECT_TRUE(palf_env_mgr->is_cluster_placeholder_exists(cluster_id));

  std::vector<int64_t> ls_ids = {1001, 1002, 1003, 1004, 1005, 1006, 1007};
  int64_t create_success_count = 0;
  auto create_func = [&]() {
    for (auto ls_id : ls_ids) {
      int ret = palf_env_mgr->create_arbitration_instance(
            palflite::PalfEnvKey(cluster_id, 1), arb_server->self_, 1001,
            ObTenantRole(ObTenantRole::PRIMARY_TENANT));
      if (OB_SUCCESS == ret) {
        ATOMIC_INC(&create_success_count);
      }
      if (OB_SUCCESS != ret) {
        ASSERT_EQ(false, true);
      } else {
      }
    }
  };
  int64_t remove_success_count = 0;
  auto remove_func = [&] () {
    for (auto ls_id : ls_ids) {
      int ret = arb_server->palf_env_mgr_.delete_arbitration_instance(
        palflite::PalfEnvKey(cluster_id, 1), arb_server->self_, ls_id);
      if (OB_SUCCESS == ret) {
        ATOMIC_INC(&remove_success_count);
      }
      if (OB_SUCCESS != ret) {
        ASSERT_EQ(false, true);
      } else {
      }
    }
  };
  int64_t thread_count = 8;
  std::vector<std::thread> create_threads;
  create_threads.reserve(thread_count);
  for (int i = 0; i < thread_count; i++) {
    create_threads.emplace_back(std::thread(create_func));
  }
  for (int i = 0; i < thread_count; i++) {
    create_threads[i].join();
  }
  ASSERT_EQ(thread_count*ls_ids.size(), create_success_count);
  EXPECT_DOUBLE_EQ(baseline_ls_count + 1, palf_env_mgr->ls_count_.impl_->Value());
  std::vector<std::thread> remove_threads;
  remove_threads.reserve(thread_count);
  for (int i = 0; i < thread_count; i++) {
    remove_threads.emplace_back(std::thread(remove_func));
  }
  for (int i = 0; i < thread_count; i++) {
    remove_threads[i].join();
  }
  ASSERT_EQ(thread_count*ls_ids.size(), remove_success_count);
  EXPECT_DOUBLE_EQ(baseline_ls_count, palf_env_mgr->ls_count_.impl_->Value());
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  GCONF.prometheus_metrics_port = 0;  // disable exposer in mittest
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
