// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include <cstdio>
#include <gtest/gtest.h>
#include <signal.h>
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
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 1;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 1;
bool ObSimpleLogClusterTestBase::need_add_arb_server_ = true;
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
    EXPECT_EQ(OB_SUCCESS, palf_env_lite_2->create_palf_handle_impl(1, mode, info_2, ipalf_handle_impl_2));
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
        palflite::PalfEnvKey(cluster_id, OB_SERVER_TENANT_ID), palf_env_lite));
  arb_server->palf_env_mgr_.revert_palf_env_lite(palf_env_lite);
  EXPECT_EQ(OB_SUCCESS, arb_server->palf_env_mgr_.remove_palf_env_lite(
        palflite::PalfEnvKey(cluster_id, OB_SERVER_TENANT_ID)));
  EXPECT_EQ(OB_SUCCESS, arb_server->palf_env_mgr_.delete_arbitration_instance(
        palflite::PalfEnvKey(cluster_id, 1), arb_server->self_, 1000));
  CLOG_LOG(INFO, "end test out_interface");
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
    EXPECT_EQ(OB_SUCCESS, palf_env_lite->create_palf_handle_impl(1, mode, info, ipalf_handle_impl));
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

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
