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
#define protected public
#include "env/ob_simple_log_cluster_env.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#undef private
#undef protected

const std::string TEST_NAME = "mutil_arb_server";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace palflite;
using namespace palf;
using namespace arbserver;
namespace unittest
{

class TestObSimpleMutilArbServer : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleMutilArbServer() :  ObSimpleLogClusterTestEnv()
  {}
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 3;
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

int create_palf_handle_impl(palflite::PalfEnvLite *palf_env_lite, int64_t palf_id)
{
  int ret = OB_SUCCESS;
  IPalfHandleImpl *ipalf_handle_impl = NULL;

  AccessMode access_mode(AccessMode::APPEND);
  PalfBaseInfo info;
  info.generate_by_default();
  if (OB_FAIL(palf_env_lite->create_palf_handle_impl(palf_id, access_mode, info, ipalf_handle_impl))) {
    CLOG_LOG(WARN, "create_palf_handle_impl failed", K(ret), KPC(palf_env_lite));
  }
  if (ipalf_handle_impl != NULL) {
    palf_env_lite->revert_palf_handle_impl(ipalf_handle_impl);
  }
  return ret;
}

TEST_F(TestObSimpleMutilArbServer, create_mutil_tenant)
{
  SET_CASE_LOG_FILE(TEST_NAME, "create_mutil_tenant");
  OB_LOGGER.set_log_level("TRACE");
  CLOG_LOG(INFO, "begin create_mutil_tenant");
  sleep(2);
  ObISimpleLogServer *iserver_arb = get_cluster()[2];
  ObISimpleLogServer *iserver_log = get_cluster()[0];
  EXPECT_EQ(true, iserver_arb->is_arb_server());
  EXPECT_EQ(false, iserver_log->is_arb_server());
  ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(iserver_arb);
  ObSimpleLogServer *log_server = dynamic_cast<ObSimpleLogServer*>(iserver_log);
  ObSrvRpcProxy &rpc_proxy = log_server->srv_proxy_;
  ObAddr dst_addr = iserver_arb->get_addr();
  // 验证建立arb replica副本
  obrpc::ObCreateArbArg arg;
  obrpc::ObCreateArbResult result;
  ObRpcNetHandler::CLUSTER_ID = 1;
  int64_t cluster_id = ObRpcNetHandler::CLUSTER_ID;
  EXPECT_EQ(OB_SUCCESS, arg.init(1001, ObLSID(1), ObTenantRole(ObTenantRole::PRIMARY_TENANT)));
  EXPECT_EQ(OB_SUCCESS, rpc_proxy.to(dst_addr).create_arb(arg, result));
  palflite::PalfEnvLite *palf_env_lite = NULL;
  palf::IPalfHandleImplGuard guard;
  EXPECT_EQ(OB_SUCCESS, arb_server->palf_env_mgr_.get_palf_env_lite(
        palflite::PalfEnvKey(cluster_id, 1001), palf_env_lite));
  EXPECT_EQ(OB_SUCCESS, palf_env_lite->get_palf_handle_impl(1, guard));
  guard.reset();

  // 验证设置成员列表
  obrpc::ObSetMemberListArgV2 memberlist_arg;
  obrpc::ObSetMemberListArgV2 memberlist_result;
  const ObMemberList member_list = get_arb_member_list();
  const ObMember arb_member = get_arb_member();
  const GlobalLearnerList learner_list;
  EXPECT_EQ(true, arb_member.is_valid());
  rootserver::ObSetMemberListProxy proxy(rpc_proxy,
                                         &obrpc::ObSrvRpcProxy::set_member_list);
  EXPECT_EQ(OB_SUCCESS,
      memberlist_arg.init(
        1001,
        ObLSID(1),
        2,
        member_list,
        arb_member,
        learner_list));
  proxy.call(dst_addr, 1000*1000, cluster_id, 1001, memberlist_arg);
  proxy.wait();

  obrpc::ObDeleteArbArg delete_arg;
  obrpc::ObDeleteArbResult delete_result;
  EXPECT_EQ(OB_SUCCESS, delete_arg.init(1001, ObLSID(1)));
EXPECT_EQ(OB_SUCCESS, rpc_proxy.to(dst_addr).delete_arb(delete_arg, delete_result));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, palf_env_lite->get_palf_handle_impl(1, guard));
  guard.reset();
  arb_server->palf_env_mgr_.revert_palf_env_lite(palf_env_lite);

  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
}

TEST_F(TestObSimpleMutilArbServer, test_gc)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_gc");
  OB_LOGGER.set_log_level("TRACE");
  ObISimpleLogServer *iserver_arb = get_cluster()[2];
  ObISimpleLogServer *iserver_log = get_cluster()[0];
  ObSimpleArbServer *arb_server = dynamic_cast<ObSimpleArbServer*>(iserver_arb);
  ObSimpleLogServer *log_server = dynamic_cast<ObSimpleLogServer*>(iserver_log);
  ObSrvRpcProxy &rpc_proxy = log_server->srv_proxy_;
  ObAddr dst_addr = iserver_arb->get_addr();
  // 验证GC
  {
    palflite::PalfEnvLiteMgr *mgr = &arb_server->palf_env_mgr_;
    auto create_clusters = [&mgr, log_server](const std::vector<int64_t> &cluster_ids) -> int {
      int ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < cluster_ids.size(); i++) {
        auto cluster_id = cluster_ids[i];
        std::string name_base = "name_base";
        name_base += std::to_string(cluster_id);
        arbserver::GCMsgEpoch epoch(1, 1);
        EXPECT_EQ(OB_SUCCESS, mgr->add_cluster(log_server->get_addr(),         \
                                               cluster_id,                     \
                                               ObString(name_base.c_str()),    \
                                               epoch));
      }
      return OB_SUCCESS;
    };
    auto create_tenant_and_ls = [&mgr]
                                  (const std::vector<int64_t> &cluster_ids,
                                   const std::vector<uint64_t> &tenant_ids,
                                   const std::vector<int64_t> &ls_ids) -> int {
      int ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < cluster_ids.size(); i++) {
        auto cluster_id = cluster_ids[i];

        for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); i++) {
          auto tenant_id = tenant_ids[i];
          auto malloc = ObMallocAllocator::get_instance();
          if (NULL == malloc->get_tenant_ctx_allocator(tenant_id, 0)) {
            malloc->create_and_add_tenant_allocator(tenant_id);
          }
          PalfEnvKey palf_env_key(cluster_id, tenant_id);
          PalfEnvLite *palf_env_lite = NULL;

          if (OB_FAIL(mgr->create_palf_env_lite(palf_env_key))) {
            CLOG_LOG(WARN, "create_palf_env_lite failed", K(ret), K(palf_env_key));
          } else if (OB_FAIL(mgr->get_palf_env_lite(palf_env_key, palf_env_lite))) {
            CLOG_LOG(WARN, "get_palf_env_lite failed", K(ret), K(palf_env_key));
          }

          for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids.size(); i++) {
            auto ls_id = ls_ids[i];
            if (OB_FAIL(create_palf_handle_impl(palf_env_lite, ls_id))) {
              CLOG_LOG(WARN, "create_palf_handle_impl failed", K(ret), K(palf_env_key));
            }
          }

          if (NULL != palf_env_lite) {
            mgr->revert_palf_env_lite(palf_env_lite);
          }
        }
      }
      return OB_SUCCESS;
    };
    auto check_tenant_and_ls = [&mgr]
                                  (const std::vector<int64_t> &cluster_ids,
                                   const std::vector<uint64_t> &tenant_ids,
                                   const std::vector<int64_t> &ls_ids) -> bool {
      int ret = OB_SUCCESS;
      bool bool_ret = true;
      for (int64_t i = 0; i < cluster_ids.size(); i++) {
        auto cluster_id = cluster_ids[i];

        for (int64_t i = 0; i < tenant_ids.size(); i++) {
          auto tenant_id = tenant_ids[i];
          PalfEnvKey palf_env_key(cluster_id, tenant_id);
          PalfEnvLite *palf_env_lite;

          if (OB_FAIL(mgr->get_palf_env_lite(palf_env_key, palf_env_lite))) {
            CLOG_LOG(WARN, "get_palf_env_lite failed", K(ret), K(palf_env_key));
          }

          bool_ret = (OB_SUCCESS == ret);
          for (int64_t i = 0; i < ls_ids.size(); i++) {
            auto ls_id = ls_ids[i];
            IPalfHandleImplGuard guard;
            if (OB_FAIL(palf_env_lite->get_palf_handle_impl(ls_id, guard))) {
              CLOG_LOG(WARN, "create_palf_handle_impl failed", K(ret), K(palf_env_key));
            }
            bool_ret = (OB_SUCCESS == ret);
          CLOG_LOG(INFO, "current trace", K(palf_env_key), K(ls_id), K(ret), K(bool_ret));
          }

          if (NULL != palf_env_lite) {
            mgr->revert_palf_env_lite(palf_env_lite);
          }
        }
      }
      return bool_ret;
    };

    auto check_tenant = [&mgr] (const std::vector<int64_t> &cluster_ids,
                                const std::vector<uint64_t> &tenant_ids,
                                const GCMsgEpoch &epoch) -> bool {
      int ret = OB_SUCCESS;
      bool bool_ret = true;
      ClusterMetaInfo info;
      for (int64_t i = 0; i < cluster_ids.size(); i++) {
        auto cluster_id = cluster_ids[i];

        if (!epoch.is_valid()) {
        } else if (OB_FAIL(mgr->get_cluster_meta_info_(cluster_id, info))) {
          CLOG_LOG(WARN, "get_cluster_meta_info_ failed", KR(ret), K(cluster_id));
          return false;
        } else if (!(info.epoch_ == epoch)) {
          CLOG_LOG(WARN, "epoch is not same", KR(ret), K(cluster_id), K(info), K(epoch));
          return false;
        }
        for (int64_t i = 0; i < tenant_ids.size(); i++) {
          auto tenant_id = tenant_ids[i];
          PalfEnvKey palf_env_key(cluster_id, tenant_id);
          PalfEnvLite *palf_env_lite = NULL;

          if (OB_FAIL(mgr->get_palf_env_lite(palf_env_key, palf_env_lite))) {
            CLOG_LOG(WARN, "get_palf_env_lite failed", K(ret), K(palf_env_key));
          }

          bool_ret = (OB_SUCCESS == ret);
          CLOG_LOG(INFO, "current trace", K(palf_env_key), K(ret), K(bool_ret));

          if (NULL != palf_env_lite) {
            mgr->revert_palf_env_lite(palf_env_lite);
          }
        }
      }
      return bool_ret;
    };

    std::vector<int64_t> cluster_ids = {1000, 1001, 1002, 1003};
    std::vector<uint64_t> tenant_ids = {1000, 1001, 1002, 1003, 1004, 1007};
    std::vector<int64_t> ls_ids = {1000, 1001, 1002, 1003, 1004, 1007, 1008};

    EXPECT_EQ(OB_SUCCESS, create_clusters(cluster_ids));
    EXPECT_EQ(OB_SUCCESS, create_tenant_and_ls(cluster_ids, tenant_ids, ls_ids));
    EXPECT_TRUE(check_tenant_and_ls(cluster_ids, tenant_ids, ls_ids));

    std::vector<int64_t> not_existing_cluster_ids = {1004};

    ClusterMetaInfo tmp_info;
    EXPECT_EQ(OB_HASH_NOT_EXIST, mgr->get_cluster_meta_info_(1004, tmp_info));

    std::vector<int64_t> gc_cluster_ids = {1000, 1002};
    std::vector<uint64_t> gc_tenant_ids = {1000, 1001, 1002, 1004};
    std::vector<int64_t> max_ls_id = {1007, 1007, 1007, 1007};
    std::vector<int64_t> gc_ls_ids = {1000, 1001, 1003, 1004};

    auto create_tenant_ls_id_array = [&gc_tenant_ids, &max_ls_id, &gc_ls_ids](
        arbserver::TenantLSIDSArray &array) -> int{
      for (int i = 0; i < gc_tenant_ids.size(); i++) {
        auto tenant_id = gc_tenant_ids[i];
        auto tmp_max_ls_id = ObLSID(max_ls_id[i]);
        arbserver::TenantLSIDS tenant_ls_ids;
        for (auto ls_id : gc_ls_ids) {
          tenant_ls_ids.push_back(ObLSID(ls_id));
        }
        arbserver::TenantLSID id(tenant_id, tmp_max_ls_id);
        tenant_ls_ids.set_max_ls_id(id);
        array.push_back(tenant_ls_ids);
      }
      return OB_SUCCESS;
    };

    arbserver::GCMsgEpoch epoch(100, 1000);
    // 执行GC动作
    {
      uint64_t max_tenant_id = 1006;
      // 只操作了1000 1002 cluster，预期1001 1003 cluster的日志流全部存在
      // 预期1003租户会被直接删除完毕
      // 预期1000 1001 1002 1004租户的1002 1007日志流会被删除
      arbserver::TenantLSIDSArray array;
      EXPECT_EQ(OB_SUCCESS, create_tenant_ls_id_array(array));
      for (auto cluster_id : gc_cluster_ids) {
        ObArbGCNotifyArg arg;
        ObArbGCNotifyResult result;
        //ObRpcNetHandler::CLUSTER_ID = cluster_id;
        rpc_proxy.src_cluster_id_ = cluster_id;
        array.set_max_tenant_id(max_tenant_id);
        EXPECT_EQ(OB_SUCCESS, arg.init(epoch, array));
        EXPECT_EQ(OB_SUCCESS, rpc_proxy.to(dst_addr).arb_gc_notify(arg, result));
      }
    }

    std::vector<uint64_t> not_exist_tenant_ids = {1003};
    std::vector<int64_t> no_gc_cluster_ids = {1001, 1003};
    // 已经gc的cluster中，1003 租户不存在
    CLOG_LOG(INFO, "first check");
    EXPECT_EQ(false, check_tenant(gc_cluster_ids, not_exist_tenant_ids, epoch));
    // 未gc的cluster中，1003 租户存在
    CLOG_LOG(INFO, "second check");
    EXPECT_EQ(true, check_tenant(no_gc_cluster_ids, not_exist_tenant_ids, GCMsgEpoch(palf::INVALID_PROPOSAL_ID, -1)));
    // 已经gc的cluster中，gc_tenant_ids中租户存在
    CLOG_LOG(INFO, "third check");
    EXPECT_EQ(true, check_tenant(gc_cluster_ids, gc_tenant_ids, epoch));

    // 已经gc的cluster中，1002以及1007日志流不存在
    std::vector<int64_t> not_exist_ls = {1002, 1007};
    CLOG_LOG(INFO, "fourth check");
    EXPECT_EQ(false, check_tenant_and_ls(gc_cluster_ids, gc_tenant_ids, not_exist_ls));
    // 已经gc的cluster中，其他日志流存在
    CLOG_LOG(INFO, "five check");
    EXPECT_EQ(true, check_tenant_and_ls(gc_cluster_ids, gc_tenant_ids, gc_ls_ids));

    // delete all tenants of following clusters
    arbserver::GCMsgEpoch delete_epoch(101, 1000);
    std::vector<int64_t> full_gc_cluster_ids = {1000, 1001, 1002};
    std::vector<int64_t> no_full_gc_cluster_ids = {1003};
    for (int i = 0; i < full_gc_cluster_ids.size(); i++) {
      EXPECT_TRUE(mgr->is_cluster_placeholder_exists(full_gc_cluster_ids[i]));
      EXPECT_EQ(OB_SUCCESS, mgr->remove_cluster(log_server->get_addr(), full_gc_cluster_ids[i], "test", delete_epoch));
      EXPECT_FALSE(mgr->is_cluster_placeholder_exists(full_gc_cluster_ids[i]));
    }
    for (int i = 0; i < no_full_gc_cluster_ids.size(); i++) {
      EXPECT_TRUE(mgr->is_cluster_placeholder_exists(no_full_gc_cluster_ids[i]));
    }

  }
  sleep(3);
  EXPECT_EQ(OB_SUCCESS, restart_paxos_groups());
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
