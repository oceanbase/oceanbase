// owner: yunlong.cb
// owner group: log

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

#define private public
#include "env/ob_simple_log_cluster_env.h"
#undef private

const std::string TEST_NAME = "test_fast_rebuild";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
namespace logservice
{
int LogRequestHandler::handle_acquire_log_rebuild_info_msg_(const LogAcquireRebuildInfoMsg &req)
{
  int ret = common::OB_SUCCESS;
  const int64_t palf_id = req.palf_id_;
  palf::PalfHandleGuard palf_handle_guard;
  if (false == req.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "Invalid argument!!!", K(ret), K(req));
  } else if (OB_FAIL(get_palf_handle_guard_(palf_id, palf_handle_guard))) {
    CLOG_LOG(WARN, "get_palf_handle_guard_ failed", K(ret), K(palf_id));
  } else {
    logservice::ObLogHandler log_handler;
    log_handler.is_inited_ = true;
    log_handler.palf_handle_.palf_handle_impl_ = palf_handle_guard.palf_handle_.palf_handle_impl_;
    logservice::ObLogService *log_srv = MTL(logservice::ObLogService*);
    ObLogFastRebuildEngine *fast_rebuild_engine = log_srv->get_shared_log_service()->get_log_fast_rebuild_engine();
    log_handler.rpc_proxy_ = log_srv->get_rpc_proxy();
    log_handler.self_ = log_srv->self_;
    log_handler.id_ = palf_id;
    unittest::MockLocCB loc_cb;
    // mock storage rebuild cb
    ObLogRebuildCbAdapter rebuild_cb;

    if (OB_FAIL(log_handler.rebuild_cb_adapter_.init(log_srv->self_, palf_id, fast_rebuild_engine, log_srv->get_rpc_proxy(), &loc_cb))) {
      CLOG_LOG(WARN, "rebuild_cb_adapter_ init failed", K(ret), K(palf_id));
    } else if (OB_FAIL(log_handler.rebuild_cb_adapter_.register_rebuild_cb(&rebuild_cb))) {
      CLOG_LOG(WARN, "register_rebuild_cb failed", K(ret), K(palf_id));
    } else if (OB_FAIL(log_handler.handle_acquire_log_rebuild_info_msg(req))) {
      CLOG_LOG(WARN, "handle_acquire_log_rebuild_info_msg failed", K(ret), K(palf_id), K(req));
    } else {
      CLOG_LOG(INFO, "handle_acquire_log_rebuild_info_msg success", K(ret), K(req));
    }

    log_handler.is_inited_ = false;
    log_handler.palf_handle_.palf_handle_impl_ = NULL;
  }
  return ret;
}
};
using namespace logservice;
using namespace palf;
namespace unittest
{
class TestObSimpleLogClusterRebuild : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterRebuild() : ObSimpleLogClusterTestEnv()
  {}
public:
  class TestRebuildCbImpl : public PalfRebuildCb, public share::ObThreadPool
  {
  public:
    TestRebuildCbImpl():
        test_base_(NULL),
        server_idx_(-1),
        rebuild_palf_id_(-1),
        rebuild_lsn_(),
        allow_rebuild_(false),
        is_inited_(false) {}
    virtual ~TestRebuildCbImpl() { destroy(); }
  public:
    int init(TestObSimpleLogClusterRebuild *ptr, const int64_t server_idx)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(ptr) || server_idx < 0 || server_idx >= ptr->get_member_cnt()) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(ERROR, "invalid_argument", KP(ptr), K(server_idx));
      } else {
        test_base_ = ptr;
        server_idx_ = server_idx;
        is_inited_ = true;
      }
      return ret;
    }

    int start()
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
      } else if (OB_FAIL(share::ObThreadPool::start())) {
        PALF_LOG(ERROR, "RebuildCB thread failed to start");
      } else {
        PALF_LOG(INFO, "RebuildCB start success", K(ret));
      }
      return ret;
    }

    void destroy()
    {
      is_inited_ = false;
      stop();
      wait();
      server_idx_ = -1;
      rebuild_palf_id_ = -1;
      rebuild_lsn_.reset();
      allow_rebuild_ = false;
      test_base_ = NULL;
    }

    int on_rebuild(const int64_t id, const LSN &lsn)
    {
      int ret = OB_SUCCESS;
      if (!is_inited_) {
        ret = OB_NOT_INIT;
      } else if (id < 0 || !lsn.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        SERVER_LOG(ERROR, "invalid rebuild argument", K(server_idx_), K(id), K(lsn));
      } else {
        rebuild_palf_id_ = id;
        rebuild_lsn_ = lsn;
        SERVER_LOG(INFO, "on_rebuild success", K(server_idx_), K(id), K(lsn));
      }
      return ret;
    }

    bool is_rebuilding(const int64_t id) const { return false; }

    void run1()
    {
      share::ObTenantBase tenant_base(OB_SYS_TENANT_ID);
      tenant_base.init();
      ObTenantEnv::set_tenant(&tenant_base);
      lib::set_thread_name("RebuildCB");
      while (!has_set_stop()) {
        if (true == allow_rebuild_ && rebuild_palf_id_ != -1 && rebuild_lsn_.is_valid()) {
          PalfHandleImplGuard leader;
          PalfHandleImplGuard *rebuild_palf;
          int64_t leader_idx;
          PalfBaseInfo rebuild_base_info;
          std::vector<PalfHandleImplGuard*> palf_list;
          EXPECT_EQ(OB_SUCCESS, test_base_->get_cluster_palf_handle_guard(rebuild_palf_id_, palf_list));
          rebuild_palf = palf_list[server_idx_];
          EXPECT_EQ(OB_SUCCESS, test_base_->get_leader(test_base_->palf_id_, leader, leader_idx));
          EXPECT_EQ(OB_SUCCESS, rebuild_palf->palf_handle_impl_->disable_sync());
          LSN base_lsn;
          EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_lsn(base_lsn));
          EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_base_info(base_lsn, rebuild_base_info));
          EXPECT_EQ(OB_SUCCESS, rebuild_palf->palf_handle_impl_->advance_base_info(rebuild_base_info, true));
          EXPECT_EQ(OB_SUCCESS, rebuild_palf->palf_handle_impl_->enable_sync());
          rebuild_palf = NULL;
          test_base_->revert_cluster_palf_handle_guard(palf_list);
          rebuild_palf_id_ = -1;
          rebuild_lsn_.reset();
        }
        SERVER_LOG(INFO, "RebuildCB", K(server_idx_));
        usleep(500 * 1000);
      }
    }
  public:
    TestObSimpleLogClusterRebuild *test_base_;
    int64_t server_idx_;
    int64_t rebuild_palf_id_;
    LSN rebuild_lsn_;
    bool allow_rebuild_;
    bool is_inited_;
  };
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 3;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;
bool ObSimpleLogClusterTestBase::need_add_arb_server_  = false;
bool ObSimpleLogClusterTestBase::need_shared_storage_ = true;

TEST_F(TestObSimpleLogClusterRebuild, test_fast_rebuild)
{
  SET_CASE_LOG_FILE(TEST_NAME, "test_fast_rebuild");
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  constexpr int64_t MB = 1024 * 1024;
  PALF_LOG(INFO, "start test_fast_rebuild", K(id));
  int64_t leader_idx = 0;
  unittest::PalfHandleImplGuard leader;
  unittest::PalfHandleImplGuard *rebuild_server = NULL;
  std::vector<PalfHandleImplGuard*> palf_list;
  int64_t follower_idx;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group(id, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  follower_idx = (leader_idx + 1) % 3;
  TestRebuildCbImpl rebuild_cb;
  rebuild_cb.allow_rebuild_ = true;
  logservice::ObLogRebuildCbAdapter rebuild_cb_adapter;
  EXPECT_EQ(OB_SUCCESS, rebuild_cb.init(this, follower_idx));
  EXPECT_EQ(OB_SUCCESS, rebuild_cb.start());
  // reigster rebuild cb
  rebuild_server = palf_list[follower_idx];
  const common::ObAddr rebuild_server_addr = get_cluster()[follower_idx]->get_addr();
  logservice::ObLogService *rebuild_log_srv = &(dynamic_cast<ObSimpleLogServer*>(get_cluster()[follower_idx])->log_service_);
  logservice::ObLogFastRebuildEngine *fast_rebuild_engine = (rebuild_log_srv->get_shared_log_service()->get_log_fast_rebuild_engine());
  obrpc::ObLogServiceRpcProxy *rpc_proxy = rebuild_log_srv->get_rpc_proxy();
  MockLocCB loc_cb;
  ObSimpleLogServer *log_server = dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx]);
  loc_cb.leader_ = log_server->get_addr();
  EXPECT_EQ(OB_SUCCESS, rebuild_cb_adapter.init(rebuild_server_addr, id, fast_rebuild_engine, rpc_proxy, &loc_cb));
  EXPECT_EQ(OB_SUCCESS, rebuild_cb_adapter.register_rebuild_cb(&rebuild_cb));
  PalfRebuildCbNode rebuild_node(&rebuild_cb_adapter);
  EXPECT_EQ(OB_SUCCESS, rebuild_server->palf_handle_impl_->register_rebuild_cb(&rebuild_node));
  // set fast rebuild threshold
  // the follower is empty
  block_net(leader_idx, follower_idx);
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 64*3, leader_idx, MB));
  // recycle one block
  LSN expected_base_lsn = LSN(3*PALF_BLOCK_SIZE);
  EXPECT_UNTIL_EQ(expected_base_lsn, leader.palf_handle_impl_->log_engine_.base_lsn_for_block_gc_);
  EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->delete_block(0));
  update_disk_options(leader_idx, 8);
  sleep(1);
  block_id_t leader_min_block_id, follower_min_block_id, unused_id;
  share::SCN min_scn;
  while (true) {
    EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->get_min_block_info_for_gc(leader_min_block_id, min_scn));
    if (leader_min_block_id >= 1) {
      break;
    } else {
      sleep(2);
      PALF_LOG(INFO, "wait gc", K(leader_min_block_id));
    }
  }
  // follower_idx will trigger rebuild when fetching log
  unblock_net(leader_idx, follower_idx);
  // check log sync and wait for rebuilding
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 100, id, 6 * 1024));
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn(), leader.palf_handle_impl_->get_end_lsn());
  EXPECT_UNTIL_EQ(leader.palf_handle_impl_->get_max_lsn(), rebuild_server->palf_handle_impl_->get_max_lsn());

  LSN leader_end_lsn, rebuild_server_end_lsn;
  leader_end_lsn = leader.palf_handle_impl_->get_end_lsn();
  rebuild_server_end_lsn = rebuild_server->palf_handle_impl_->get_end_lsn();
  PALF_LOG(INFO, "rebuild result", K(leader_end_lsn), K(rebuild_server_end_lsn));

  // check block_id
  EXPECT_EQ(OB_SUCCESS, submit_log(leader, 10, id, 1 * 1024));

  // check base_lsn
  EXPECT_EQ(LSN(lsn_2_block(leader_end_lsn, PALF_BLOCK_SIZE) * PALF_BLOCK_SIZE), \
      rebuild_server->palf_handle_impl_->log_engine_.get_log_meta().get_log_snapshot_meta().base_lsn_);
  EXPECT_EQ(expected_base_lsn, rebuild_server->palf_handle_impl_->log_engine_.base_lsn_for_block_gc_);

  leader.reset();
  rebuild_server->reset();
  revert_cluster_palf_handle_guard(palf_list);
  EXPECT_EQ(OB_SUCCESS, delete_paxos_group(id));
  PALF_LOG(INFO, "end test_fast_rebuild", K(id));
}
} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
