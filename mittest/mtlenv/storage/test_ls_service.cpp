// owner: cxf262476
// owner group: transaction

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

#define USING_LOG_PREFIX STORAGETEST

#define protected public
#define private public
#include "src/storage/slog/ob_storage_log_item.h"
#include "storage/ob_storage_rpc.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/test_tablet_helper.h"
#include "deps/oblib/src/lib/ob_define.h"
#include "deps/oblib/src/lib/ob_replica_define.h"
#include "lib/utility/ob_defer.h"

namespace oceanbase
{
using namespace common;
using namespace rpc;
using namespace rpc::frame;
using namespace share;
namespace storage
{
class TestLSService : public ::testing::Test
{
public:
  TestLSService() = default;
  virtual ~TestLSService() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }
private:
  common::ObArenaAllocator allocator_;
};

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace std;
using namespace oceanbase::obsys;

void TestLSService::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
}

void TestLSService::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestLSService, basic)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObCreateLSArg arg;
  ObLSService* ls_svr = MTL(ObLSService*);
  bool exist = false;
  ObLSID id_100(100);
  ObLSID id_101(101);
  ObLSID id_102(102);
  ObLSID exist_id = id_100;
  ObLSID not_exist_id = id_101;
  ObLSHandle handle;
  ObLS *ls = NULL;
  common::ObSharedGuard<ObLSIterator> iter;
  // TEST_F(ObLSServiceTest, create_ls)
  LOG_INFO("create_ls begin");
  // 1. create 100
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_100, arg));
  LOG_INFO("create_ls", K(arg), K(id_100));
  EXPECT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));

  // 2. left 101
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_101, arg));
  LOG_INFO("create_ls", K(arg), K(id_101));

  // 3. create 102
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_102, arg));
  LOG_INFO("create_ls", K(arg), K(id_102));
  EXPECT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));

  // TEST_F(ObLSServiceTest, get_ls)
  LOG_INFO("get_ls begin");
  // 1. exist get
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(exist_id, handle, ObLSGetMod::STORAGE_MOD));
  EXPECT_EQ(exist_id, handle.get_ls()->get_ls_id());

  // 2. not exist get
  EXPECT_EQ(OB_LS_NOT_EXIST, ls_svr->get_ls(not_exist_id, handle, ObLSGetMod::STORAGE_MOD));

  // TEST_F(ObLSServiceTest, check_ls_exist)
  LOG_INFO("check_ls_exist begin");
  // 1. exist check
  EXPECT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(exist_id, exist));
  EXPECT_TRUE(exist);
  // 2. not exist check
  EXPECT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(not_exist_id, exist));
  EXPECT_FALSE(exist);

  // TEST_F(ObLSServiceTest, get_ls_iter)
  LOG_INFO("get_ls_iter begin");
  // 1. create iter
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls_iter(iter, ObLSGetMod::STORAGE_MOD));

  // 2. iter LS
  EXPECT_EQ(OB_SUCCESS, iter->get_next(ls));
  ObLSID first_id = ls->get_ls_id();

  EXPECT_EQ(OB_SUCCESS, iter->get_next(ls));
  ObLSID second_id = ls->get_ls_id();

  // 3. iter end
  EXPECT_EQ(OB_ITER_END, iter->get_next(ls));

  // 4. test the result.
  EXPECT_TRUE(((first_id == id_100) || (first_id == id_102)));
  EXPECT_TRUE(((second_id == id_100) || (second_id == id_102)));
  EXPECT_NE(first_id, second_id);

  // TEST_F(ObLSServiceTest, remove_ls)
  LOG_INFO("remove_ls begin");
  // 1. remove not exist.
  EXPECT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(not_exist_id, exist));
  EXPECT_FALSE(exist);
  EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(not_exist_id));

  // 2. remove exist.
  EXPECT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_100, exist));
  EXPECT_TRUE(exist);
  EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_100));
  EXPECT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_100, exist));
  EXPECT_FALSE(exist);

  EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_102));

  // 3. check empty iter.
  iter.reset();
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls_iter(iter, ObLSGetMod::STORAGE_MOD));
  EXPECT_EQ(OB_ITER_END, iter->get_next(ls));
}

TEST_F(TestLSService, permanent_remove_marks_log_gc_without_offline_scn)
{
  ObLSService *ls_svr = MTL(ObLSService*);
  const ObLSID ls_id(120);
  ObCreateLSArg arg;
  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  auto *apply_status = ls->get_log_handler()->apply_status_;
  ASSERT_NE(nullptr, apply_status);
  SCN offline_scn;
  ASSERT_EQ(OB_SUCCESS, ls->get_offline_scn(offline_scn));
  ASSERT_FALSE(offline_scn.is_valid());
  ASSERT_FALSE(apply_status->is_ls_gc_state());

  // A temporary offline must retain the strong-sync durability contract.
  ASSERT_EQ(OB_SUCCESS, ls->ls_tx_svr_.offline());
  EXPECT_FALSE(apply_status->is_ls_gc_state());
  ASSERT_EQ(OB_SUCCESS, ls->offline(false /*remove_from_disk*/));
  EXPECT_FALSE(apply_status->is_ls_gc_state());

  // A permanent removal must not depend on having replayed OFFLINE_LS first.
  ASSERT_EQ(OB_SUCCESS, ls->offline(true /*remove_from_disk*/));
  EXPECT_TRUE(apply_status->is_ls_gc_state());
  ASSERT_EQ(OB_SUCCESS, ls->get_offline_scn(offline_scn));
  EXPECT_FALSE(offline_scn.is_valid());
  handle.reset();
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
}

TEST_F(TestLSService, permanent_remove_waits_for_active_tx_before_gc)
{
  using namespace transaction;
  using namespace logservice;
  ObLSService *ls_svr = MTL(ObLSService*);
  const ObLSID ls_id(121);
  ObCreateLSArg arg;
  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ObLSTxService &tx_service = ls->ls_tx_svr_;
  ObLSTxCtxMgr *mgr = tx_service.mgr_;
  ASSERT_NE(nullptr, mgr);
  ObPartTransCtx *ctx = nullptr;
  bool existed = false;
  const ObTxCreateArg tx_arg(true /*for_replay*/, PartCtxSource::REPLAY,
      MTL_ID(), ObTransID(121), ls_id, 1, GET_MIN_CLUSTER_VERSION(), 0, 0, 0,
      MockTenantModuleEnv::get_instance().self_addr_,
      ObTimeUtility::current_time() + 60 * 1000 * 1000, MTL(ObTransService*));
  ASSERT_EQ(OB_SUCCESS, tx_service.create_tx_ctx(tx_arg, existed, ctx));
  ASSERT_NE(nullptr, ctx);
  ASSERT_EQ(1, mgr->get_active_tx_count());

  class ExitCheckingCb : public AppendCb
  {
  public:
    explicit ExitCheckingCb(ObPartTransCtx *ctx) : ctx_(ctx), result_(0) {}
    int on_success() override
    {
      bool exited = false;
      {
        CtxLockGuard guard(ctx_->lock_);
        exited = ctx_->is_exiting() && ctx_->sub_state_.is_force_abort();
      }
      ATOMIC_STORE(&result_, exited ? 1 : -1);
      return OB_SUCCESS;
    }
    int on_failure() override { ATOMIC_STORE(&result_, -2); return OB_SUCCESS; }
    const char *get_cb_name() const override { return "ExitCheckingCb"; }
    ObPartTransCtx *ctx_;
    int64_t result_;
  } cb(ctx);
  ObApplyStatus *apply = ls->get_log_handler()->apply_status_;
  ASSERT_NE(nullptr, apply);
  ASSERT_EQ(OB_SUCCESS, apply->unregister_file_size_cb());
  ASSERT_EQ(OB_SUCCESS, apply->switch_to_leader(1, palf::SyncMode::SYNC));
  SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_for_logservice(1));
  ASSERT_EQ(OB_SUCCESS, apply->update_palf_committed_end_lsn(palf::LSN(10), scn, 1));
  cb.__set_lsn(palf::LSN(0));
  cb.__set_scn(scn);
  ASSERT_EQ(OB_SUCCESS, apply->push_append_cb(&cb));
  const auto transport_gc = [ls, &ls_id]() {
    ObTpStatusGuard guard;
    const int ret = ls->get_log_handler()->transport_service_->get_transport_status(ls_id, guard);
    EXPECT_EQ(OB_SUCCESS, ret);
    return OB_SUCCESS == ret && ATOMIC_LOAD(&guard.get_transport_status()->is_ls_gc_state_);
  };
  // Run one offline attempt so its retry boundary can be inspected. A readonly
  // request delays transaction termination while the SYNC callback is parked.
  mgr->inc_total_active_readonly_request_count();
  const int64_t start_ts = ObTimeUtility::current_time();
  const auto offline_once = [ls, start_ts](const bool remove_from_disk) {
    ObLSLockGuard guard(ls, ls->lock_, 0, LSLOCKALL);
    return ls->offline_(start_ts, remove_from_disk);
  };
  EXPECT_EQ(OB_EAGAIN, offline_once(true));
  EXPECT_FALSE(apply->is_ls_gc_state());
  EXPECT_FALSE(transport_gc());
  EXPECT_EQ(1, mgr->get_active_tx_count());
  EXPECT_EQ(0, ATOMIC_LOAD(&cb.result_));
  mgr->dec_total_active_readonly_request_count();

  // kill_all_tx's traversal can report success even if a context could not be
  // killed. Model that failure with an uninitialized context and require retry.
  ctx->is_inited_ = false;
  EXPECT_EQ(OB_EAGAIN, offline_once(true));
  EXPECT_FALSE(apply->is_ls_gc_state());
  EXPECT_FALSE(transport_gc());
  EXPECT_EQ(1, mgr->get_active_tx_count());
  EXPECT_EQ(0, ATOMIC_LOAD(&cb.result_));
  ctx->is_inited_ = true;

  // A temporary offline kills transactions but must not bypass standby ACKs.
  EXPECT_EQ(OB_EAGAIN, offline_once(false));
  EXPECT_EQ(0, mgr->get_active_tx_count());
  EXPECT_FALSE(apply->is_ls_gc_state());
  EXPECT_FALSE(transport_gc());
  EXPECT_EQ(0, ATOMIC_LOAD(&cb.result_));

  // Our retained context reference must not prevent GC publication once the
  // transaction has exited; it must still prevent offline from finishing.
  EXPECT_EQ(OB_EAGAIN, offline_once(true));
  EXPECT_EQ(0, mgr->get_active_tx_count());
  EXPECT_EQ(1, mgr->get_tx_ctx_count());
  EXPECT_TRUE(apply->is_ls_gc_state());
  EXPECT_TRUE(transport_gc());
  const int64_t deadline = ObTimeUtility::current_time() + 5 * 1000 * 1000;
  while (0 == ATOMIC_LOAD(&cb.result_) && ObTimeUtility::current_time() < deadline) {
    ob_usleep(1000);
  }
  EXPECT_EQ(1, ATOMIC_LOAD(&cb.result_));
  EXPECT_EQ(OB_SUCCESS, apply->switch_to_follower());
  {
    // On timeout, drain the pending callback before releasing either its
    // context or its stack storage. Exclude the apply worker during cleanup.
    ObApplyStatus::WLockGuard guard(apply->lock_);
    if (0 == ATOMIC_LOAD(&cb.result_)) {
      EXPECT_EQ(OB_SUCCESS, apply->handle_drop_cb());
    }
  }
  EXPECT_EQ(OB_SUCCESS, tx_service.revert_tx_ctx(ctx));
  ctx = nullptr;
  EXPECT_EQ(OB_SUCCESS, ls->offline(true /*remove_from_disk*/));
  handle.reset();
  EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
}

TEST_F(TestLSService, permanent_remove_drains_real_commit_callback)
{
  using namespace transaction;
  using namespace logservice;
  ObLSService *ls_svr = MTL(ObLSService*);
  const ObLSID ls_id(123);
  ObCreateLSArg arg;
  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ObLSTxService &tx_service = ls->ls_tx_svr_;
  ObLSTxCtxMgr *mgr = tx_service.mgr_;
  ObApplyStatus *apply = ls->get_log_handler()->apply_status_;
  ObPartTransCtx *ctx = nullptr;
  ObApplyServiceQueueTask *queue = nullptr;
  bool owns_queue_lease = false;
  bool removed = false;
  const auto release_queue = [&]() {
    if (owns_queue_lease) {
      while (!queue->revoke_lease()) {}
      owns_queue_lease = false;
    }
  };
  DEFER(
    release_queue();
    if (nullptr != ctx) {
      EXPECT_EQ(OB_SUCCESS, tx_service.revert_tx_ctx(ctx));
      ctx = nullptr;
    }
    if (!removed) {
      // Release test-owned resources before teardown, including on assertion failure.
      EXPECT_EQ(OB_SUCCESS, ls->offline(true));
      handle.reset();
      EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
    }
  );
  const auto wait_until = [](auto predicate) {
    const int64_t deadline = ObTimeUtility::current_time() + 10 * 1000 * 1000;
    while (ObTimeUtility::current_time() < deadline) {
      if (predicate()) {
        return true;
      }
      ob_usleep(1000);
    }
    return false;
  };
  ObMemberList members;
  ASSERT_EQ(OB_SUCCESS, members.add_server(MockTenantModuleEnv::get_instance().self_addr_));
  ASSERT_EQ(OB_SUCCESS, ls->set_initial_member_list(members, 1, GlobalLearnerList()));
  int64_t proposal_id = 0;
  ASSERT_TRUE(wait_until([&]() {
    ObRole role = INVALID_ROLE;
    return OB_SUCCESS == ls->get_log_handler()->get_role(role, proposal_id)
        && LEADER == role && mgr->is_master_();
  }));

  // Use the real transaction log producer, PALF append and ObTxLogCb. Only the
  // standby confirmation is withheld; no mock on_success replaces the tx code.
  ASSERT_EQ(OB_SUCCESS, apply->switch_sync_mode(proposal_id, palf::SyncMode::SYNC));
  const ObTxCreateArg tx_arg(false /*for_replay*/, PartCtxSource::MVCC_WRITE,
      MTL_ID(), ObTransID(123), ls_id, 1, GET_MIN_CLUSTER_VERSION(), 0, 0, 0,
      MockTenantModuleEnv::get_instance().self_addr_,
      ObTimeUtility::current_time() + 60 * 1000 * 1000, MTL(ObTransService*));
  bool existed = false;
  ASSERT_EQ(OB_SUCCESS, tx_service.create_tx_ctx(tx_arg, existed, ctx));
  ASSERT_NE(nullptr, ctx);
  ObTxLogCb *cb = nullptr;
  {
    CtxLockGuard guard(ctx->lock_);
    ctx->part_trans_action_ = ObPartTransAction::COMMIT;
    ASSERT_EQ(OB_SUCCESS, ctx->submit_log_impl_(ObTxLogType::TX_COMMIT_LOG));
    ASSERT_EQ(1, ctx->busy_cbs_.get_size());
    cb = ctx->busy_cbs_.get_first();
    ASSERT_TRUE(is_contain(cb->get_cb_arg_array(), ObTxLogType::TX_COMMIT_LOG));
  }
  const palf::LSN cb_lsn = cb->__get_lsn();
  const SCN cb_scn = cb->__get_scn();
  const int64_t queue_idx = cb_scn.get_val_for_logservice() & (APPLY_TASK_QUEUE_SIZE - 1);
  queue = &apply->cb_queues_[queue_idx];
  ASSERT_TRUE(wait_until([&]() {
    return ATOMIC_LOAD(&apply->palf_committed_end_lsn_.val_) > cb_lsn.val_
        && queue->get_total_submit_cb_cnt() > queue->get_total_apply_cb_cnt();
  }));
  ASSERT_TRUE(wait_until([&]() { return owns_queue_lease = queue->acquire_lease(); }));
  const int64_t applied_before = queue->get_total_apply_cb_cnt();
  const int64_t standby_before = ATOMIC_LOAD(&apply->standby_committed_end_lsn_.val_);
  bool timeslice_out = false;
  ASSERT_EQ(OB_SUCCESS, apply->try_handle_cb_queue(queue, timeslice_out));
  EXPECT_EQ(applied_before, queue->get_total_apply_cb_cnt());
  EXPECT_EQ(AppendCb::__get_member_address(cb), queue->top());
  EXPECT_EQ(1, mgr->get_active_tx_count());
  EXPECT_FALSE(ctx->is_exiting());
  EXPECT_FALSE(apply->is_ls_gc_state());
  EXPECT_LE(standby_before, cb_lsn.val_);

  const auto transport_gc = [&]() {
    ObTpStatusGuard guard;
    const int ret = ls->get_log_handler()->transport_service_->get_transport_status(ls_id, guard);
    EXPECT_EQ(OB_SUCCESS, ret);
    return OB_SUCCESS == ret && ATOMIC_LOAD(&guard.get_transport_status()->is_ls_gc_state_);
  };
  const int64_t start_ts = ObTimeUtility::current_time();
  const auto offline_once = [&]() {
    ObLSLockGuard guard(ls, ls->lock_, 0, LSLOCKALL);
    return ls->offline_(start_ts, true /*remove_from_disk*/);
  };
  mgr->inc_total_active_readonly_request_count();
  EXPECT_EQ(OB_EAGAIN, offline_once());
  EXPECT_EQ(1, mgr->get_active_tx_count());
  EXPECT_FALSE(apply->is_ls_gc_state());
  EXPECT_FALSE(transport_gc());
  mgr->dec_total_active_readonly_request_count();

  // DROP TENANT's LS removal calls offline(true). Hold the queue lease, not a
  // ctx/Apply lock, so force-kill and GC publication can finish independently.
  EXPECT_EQ(OB_EAGAIN, offline_once());
  EXPECT_EQ(0, mgr->get_active_tx_count());
  EXPECT_EQ(1, mgr->get_tx_ctx_count());
  EXPECT_TRUE(apply->is_ls_gc_state());
  EXPECT_TRUE(transport_gc());
  {
    CtxLockGuard guard(ctx->lock_);
    EXPECT_TRUE(ctx->is_exiting());
    EXPECT_TRUE(ctx->sub_state_.is_force_abort());
    EXPECT_EQ(2, ctx->get_ref()); // the test's handle plus the real pending log cb
    EXPECT_EQ(1, ctx->busy_cbs_.get_size());
    EXPECT_FALSE(cb->is_callbacked());
    EXPECT_EQ(ObTxData::ABORT, ctx->ctx_tx_data_.get_state());
  }
  EXPECT_EQ(applied_before, queue->get_total_apply_cb_cnt());
  EXPECT_EQ(OB_SUCCESS, tx_service.revert_tx_ctx(ctx));
  ctx = nullptr;
  // Only the actual log callback now owns the context. This excludes the
  // retained test reference that masked callback lifetime in ExitCheckingCb.
  EXPECT_EQ(1, mgr->get_tx_ctx_count());
  EXPECT_EQ(OB_EAGAIN, offline_once());

  release_queue();
  EXPECT_EQ(OB_SUCCESS, ls->get_log_handler()->mark_ls_gc_state());
  ASSERT_TRUE(wait_until([&]() {
    return queue->get_total_apply_cb_cnt() == applied_before + 1
        && mgr->get_tx_ctx_count() == 0;
  }));
  EXPECT_EQ(standby_before, ATOMIC_LOAD(&apply->standby_committed_end_lsn_.val_));
  EXPECT_EQ(OB_SUCCESS, ls->offline(true));
  handle.reset();
  EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
  removed = true;
  ObLSHandle removed_handle;
  EXPECT_EQ(OB_LS_NOT_EXIST, ls_svr->get_ls(ls_id, removed_handle, ObLSGetMod::STORAGE_MOD));
}

int gen_create_logonly_ls_arg(const int64_t tenant_id, const share::ObLSID &ls_id,
                             obrpc::ObCreateLSArg &arg);

TEST_F(TestLSService, permanent_remove_marks_logonly_gc)
{
  ObLSService *ls_svr = MTL(ObLSService*);
  const ObLSID ls_id(122);
  ObCreateLSArg arg;
  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, gen_create_logonly_ls_arg(MTL_ID(), ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_TRUE(ls->is_logonly_replica());
  auto *apply = ls->get_log_handler()->apply_status_;
  ASSERT_NE(nullptr, apply);
  EXPECT_EQ(OB_SUCCESS, ls->offline(false /*remove_from_disk*/));
  EXPECT_FALSE(apply->is_ls_gc_state());
  EXPECT_EQ(OB_SUCCESS, ls->offline(true /*remove_from_disk*/));
  EXPECT_TRUE(apply->is_ls_gc_state());
  handle.reset();
  EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
}

TEST_F(TestLSService, tablet_test)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObCreateLSArg arg;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle handle;
  ObLS *ls = NULL;
  ObLSID ls_id(103);
  ObTabletID tablet_id(1001);
  obrpc::ObBatchCreateTabletArg create_tablet_arg;
  ObMemberList member_list;
  int64_t paxos_replica_num = 1;
  int64_t leader_epoch = 0;
  ObTabletHandle tablet_handle;
  ObTablet *tablet =NULL;
  (void) member_list.add_server(MockTenantModuleEnv::get_instance().self_addr_);
  // TEST_F(TestLSService, create_tablet)

  // create ls
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  GlobalLearnerList learner_list;
  ASSERT_EQ(OB_SUCCESS, ls->set_initial_member_list(member_list,
                                                    paxos_replica_num,
                                                    learner_list));

  for (int i=0;i<15;i++) {
    ObRole role;
    int64_t proposal_id = 0;
    ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
    if (role == ObRole::LEADER) {
      break;
    }
    ::sleep(1);
  }

  // 1. create a tablet
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(handle, tablet_id, table_schema, allocator_));

  // 2. test tablet
  LOG_INFO("TestLSService::tablet_test 2.");
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  ASSERT_EQ(tablet_id, tablet->get_tablet_meta().tablet_id_);

  // 3. remove tablet
  LOG_INFO("TestLSService::tablet_test 3.");
  obrpc::ObBatchRemoveTabletArg remove_tablet_arg;
  remove_tablet_arg.id_ = ls_id;
  remove_tablet_arg.tablet_ids_.push_back(tablet_id);
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::remove_tablet(handle, tablet_id));

  EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id));
}

TEST_F(TestLSService, ls_safe_destroy)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObCreateLSArg arg;
  ObLSService* ls_svr = MTL(ObLSService*);
  bool exist = false;
  bool waiting = false;
  ObLSID id_104(104);

  ObLSHandle handle;
  ObLS *ls = NULL;

  // 1. TEST CREATE WHILE WAITING DESTROY
  // 1.1. CREATE LS
  // 1.2. REMOVE LS BUT KEEP LS HANDLE
  // 1.3. CREATE LS AGAIN

  // 1. create ls
  LOG_INFO("TestLSService::ls_safe_destroy 1.1");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_104, arg));
  LOG_INFO("create_ls", K(arg), K(id_104));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));

  // 2. hold the ls with ls handle
  LOG_INFO("TestLSService::ls_safe_destroy 1.2");
  LOG_INFO("get_ls begin");
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(id_104, handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(id_104, handle.get_ls()->get_ls_id());

  // 3. remove ls
  LOG_INFO("TestLSService::ls_safe_destroy 1.3");
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_104));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_104, exist));
  ASSERT_FALSE(exist);

  // 4. create again and check waiting = true
  LOG_INFO("TestLSService::ls_safe_destroy 1.4");
  LOG_INFO("create_ls", K(arg), K(id_104));
  ASSERT_EQ(OB_LS_WAITING_SAFE_DESTROY, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_104, waiting));
  ASSERT_TRUE(waiting);

  // 5. release ls and check waiting = false
  LOG_INFO("TestLSService::ls_safe_destroy 1.5");
  handle.reset();
  int cnt = 0;
  while (cnt++ < 20) {
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_104, waiting));
    if (waiting) {
      ::sleep(1);
    } else {
      break;
    }
  }
  ASSERT_FALSE(waiting);

  // 6. recreate success
  LOG_INFO("TestLSService::ls_safe_destroy 1.6");
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));

  // 7. remove ls
  LOG_INFO("TestLSService::ls_safe_destroy 1.7");
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_104));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_104, exist));
  ASSERT_FALSE(exist);
}

TEST_F(TestLSService, create_and_clean)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObCreateLSArg arg;
  ObLSService* ls_svr = MTL(ObLSService*);
  bool exist = false;
  bool waiting = false;
  ObLSID id_105(105);
  int64_t MAX_CREATE_STEP = 100;
  int cnt = 0;

  LOG_INFO("TestLSService::create_and_clean");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_105, arg));
  for (int64_t i = 1; i < MAX_CREATE_STEP; i++) {
    LOG_INFO("create ls break point", K(i));
    ls_svr->break_point = i;
    if (OB_FAIL(ls_svr->create_ls(arg)) && OB_BREAK_BY_TEST == ret) {
      // check exist
      ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_105, exist));
      ASSERT_FALSE(exist);
      // wait safe destroy
      cnt = 0;
      while (cnt++ < 20) {
        ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_105, waiting));
        if (waiting) {
          ::sleep(1);
        } else {
          break;
        }
      }
      ASSERT_FALSE(waiting);
    } else if (OB_FAIL(ret)) {
      LOG_WARN("create failed but not break by test", K(ret), K(id_105));
    } else {
      // create success and finish the break test
      ls_svr->break_point = 0;
      ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_105, exist));
      ASSERT_TRUE(exist);
      ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_105));
      ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_105, exist));
      ASSERT_FALSE(exist);
      break;
    }
  }
  cnt = 0;
  while (cnt++ < 20) {
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_105, waiting));
    if (waiting) {
      ::sleep(1);
    } else {
      break;
    }
  }
  ASSERT_FALSE(waiting);
}

// Helper function to generate create LS arg with logonly replica type
int gen_create_logonly_ls_arg(const int64_t tenant_id,
                              const share::ObLSID &ls_id,
                              obrpc::ObCreateLSArg &arg)
{
  int ret = OB_SUCCESS;
  ObReplicaType replica_type = REPLICA_TYPE_LOGONLY;  // L副本类型
  ObReplicaProperty property;
  share::ObAllTenantInfo tenant_info;
  const share::SCN create_scn = share::SCN::base_scn();
  arg.reset();
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
  palf::PalfBaseInfo palf_base_info;
  ObMajorMVMergeInfo major_merge_info;

  if (OB_FAIL(tenant_info.init(tenant_id, share::PRIMARY_TENANT_ROLE))) {
    STORAGE_LOG(WARN, "failed to init tenant info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(arg.init(tenant_id, ls_id, replica_type, property, tenant_info, create_scn, compat_mode, false, palf_base_info, major_merge_info))) {
    STORAGE_LOG(WARN, "failed to init arg", KR(ret), K(tenant_id), K(ls_id), K(tenant_info), K(create_scn), K(compat_mode), K(palf_base_info), K(major_merge_info));
  }
  return ret;
}

TEST_F(TestLSService, advance_checkpoint_rpc_rejects_logonly)
{
  const uint64_t tenant_id = MTL_ID();
  ObLSService *ls_service = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_service);
  ls_service->break_point = 0;
  for (const bool logonly : {false, true}) {
    const ObLSID ls_id(logonly ? 202 : 201);
    ObCreateLSArg create_arg;
    ASSERT_EQ(OB_SUCCESS, logonly ? gen_create_logonly_ls_arg(tenant_id, ls_id, create_arg)
                                 : gen_create_ls_arg(tenant_id, ls_id, create_arg));
    ASSERT_EQ(OB_SUCCESS, ls_service->create_ls(create_arg));
    ObLSHandle handle;
    ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ObLS *ls = handle.get_ls();
    ASSERT_NE(nullptr, ls);
    ASSERT_EQ(logonly, ls->is_logonly_replica());
    ObLSMigrationHandler *handler = ls->get_ls_migration_handler();
    ASSERT_NE(nullptr, handler);
    const SCN previous_scn = handler->advance_checkpoint_scn_;

    ObAdvanceSrcLSCheckpointP processor;
    processor.arg_.tenant_id_ = tenant_id;
    processor.arg_.ls_id_ = ls_id;
    processor.arg_.recycle_scn_ = SCN::base_scn();
    EXPECT_EQ(logonly ? OB_STATE_NOT_MATCH : OB_SUCCESS, processor.process());
    EXPECT_EQ(logonly ? previous_scn : SCN::base_scn(), handler->advance_checkpoint_scn_);

    handle.reset();
    ASSERT_EQ(OB_SUCCESS, ls_service->remove_ls(ls_id));
  }
}

TEST_F(TestLSService, test_remove_ls)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObCreateLSArg arg;
  ObLSService* ls_svr = MTL(ObLSService*);
  bool exist = false;
  bool waiting = false;
  bool need_break = false;
  ObLSID id_105(105);
  int64_t MAX_CREATE_STEP = 100;
  int cnt = 0;

  LOG_INFO("TestLSService::test_remove_ls");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_105, arg));
  for (int64_t i = 1; i < MAX_CREATE_STEP; i++) {
    ls_svr->break_point = 0;
    ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
    // create success and remove ls break test
    LOG_INFO("remove ls break point", K(i));
    ls_svr->break_point = i;
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_105, exist));
    ASSERT_TRUE(exist);
    cnt = 0;
    while (OB_FAIL(ls_svr->remove_ls(id_105)) && OB_BREAK_BY_TEST == ret) {
      ls_svr->break_point = 0;
      cnt++;
      LOG_WARN("remove_ls failed, retry", K(ret), K(cnt));
    }
    // remove ls does not failed, the test should break
    if (cnt == 0) {
      need_break = true;
    }
    // wait safe destroy
    cnt = 0;
    while (cnt++ < 20) {
      ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_105, waiting));
      if (waiting) {
        ::sleep(1);
      } else {
        break;
      }
    }
    ASSERT_FALSE(waiting);
    if (need_break) {
      break;
    }
  }
}

TEST_F(TestLSService, create_logonly_ls)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  ObCreateLSArg arg;
  ObLSService* ls_svr = MTL(ObLSService*);
  bool exist = false;
  ObLSID id_200(200);  // 使用不同的LS ID避免冲突
  ObLSHandle handle;
  ObLS *ls = NULL;
  handle.reset();
  int64_t cnt = 0;
  bool waiting = false;

  LOG_INFO("create_logonly_ls begin");
  ls_svr->break_point = 0;

  // 1. create logonly LS
  ASSERT_EQ(OB_SUCCESS, gen_create_logonly_ls_arg(tenant_id, id_200, arg));
  LOG_INFO("create_logonly_ls", K(arg), K(id_200));

  int64_t MAX_CREATE_STEP = 100;
  for (int64_t i = 1; i < MAX_CREATE_STEP; i++) {
    LOG_INFO("create ls break point", K(i));
    ls_svr->break_point = i;
    if (OB_FAIL(ls_svr->create_ls(arg)) && OB_BREAK_BY_TEST == ret) {
      // check exist
      ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_200, exist));
      ASSERT_FALSE(exist);
      // wait safe destroy
      cnt = 0;
      while (cnt++ < 20) {
        ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_200, waiting));
        if (waiting) {
          ::sleep(1);
        } else {
          break;
        }
      }
      ASSERT_FALSE(waiting);
    } else if (OB_FAIL(ret)) {
      LOG_WARN("create failed but not break by test", K(ret), K(id_200));
    } else {
      // create success and finish the break test
      ls_svr->break_point = 0;
      ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_200, exist));
      ASSERT_TRUE(exist);

      EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(id_200, handle, ObLSGetMod::STORAGE_MOD));
      EXPECT_EQ(id_200, handle.get_ls()->get_ls_id());

      ObReplicaType replica_type = handle.get_ls()->get_replica_type();
      EXPECT_EQ(REPLICA_TYPE_LOGONLY, replica_type);
      LOG_INFO("logonly ls created successfully", K(replica_type), K(id_200));

      ObReplicaType meta_replica_type = handle.get_ls()->get_ls_meta().get_replica_type();
      EXPECT_EQ(REPLICA_TYPE_LOGONLY, meta_replica_type);

      EXPECT_TRUE(ObReplicaTypeCheck::is_log_replica(replica_type));
      EXPECT_FALSE(ObReplicaTypeCheck::is_full_replica(replica_type));
      EXPECT_FALSE(ObReplicaTypeCheck::is_readonly_replica(replica_type));

      ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_200));
      ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_200, exist));
      ASSERT_FALSE(exist);
      break;
    }
  }
  handle.reset();
  cnt = 0;
  while (cnt++ < 20) {
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_200, waiting));
    if (waiting) {
      ::sleep(1);
    } else {
      break;
    }
  }
  ASSERT_FALSE(waiting);

  LOG_INFO("create_logonly_ls end");
}

static bool wait_ls_iter_cnt(ObLSService *ls_svr, const int64_t expected_iter_cnt)
{
  const int64_t timeout_us = 10 * 1000 * 1000;
  const int64_t start_time = ObTimeUtil::current_time();
  int64_t current_iter_cnt = ATOMIC_LOAD(&ls_svr->iter_cnt_);
  while (expected_iter_cnt != current_iter_cnt
         && ObTimeUtil::current_time() - start_time < timeout_us) {
    ob_usleep(1000);
    current_iter_cnt = ATOMIC_LOAD(&ls_svr->iter_cnt_);
  }
  if (expected_iter_cnt != current_iter_cnt) {
    int ret = OB_TIMEOUT;
    LOG_WARN("wait ls iter count timeout", K(ret), K(expected_iter_cnt), K(current_iter_cnt), K(timeout_us));
  }
  return expected_iter_cnt == current_iter_cnt;
}

TEST_F(TestLSService, check_ls_iter_cnt)
{
  int ret = OB_SUCCESS;
  int64_t start_time = 0;
  int64_t end_time = 0;
  ObLS *ls = NULL;
  ObLSService* ls_svr = MTL(ObLSService*);
  common::ObSharedGuard<ObLSIterator> guard;
  // Wait for unrelated background iterators to drain before checking for leaks.
  ASSERT_TRUE(wait_ls_iter_cnt(ls_svr, 0));
  EventItem item;
  item.trigger_freq_ = 1;
  item.error_code_ = 4013;
  ASSERT_EQ(OB_SUCCESS, EventTable::instance().set_event("ALLOC_LS_ITER_GUARD_FAIL", item));

  LOG_INFO("TestLSService::check_ls_iter_cnt");
  // 1. get ls iter 100 times.
  for (int i = 0; i < 100; i++) {
    ret = ls_svr->get_ls_iter(guard, ObLSGetMod::OBSERVER_MOD);
    EXPECT_EQ(4013, ret);
    if (OB_FAIL(ret)) {
      LOG_WARN("get ls iter failed");
    }
  }
  // Disable the global tracepoint before any fatal assertion to avoid leaking
  // error injection into background workers and test teardown.
  EventItem disable_item;
  EXPECT_EQ(OB_SUCCESS, EventTable::instance().set_event("ALLOC_LS_ITER_GUARD_FAIL", disable_item));
  // 2. injected failures must eventually release every iterator they allocate.
  ASSERT_TRUE(wait_ls_iter_cnt(ls_svr, 0));

  // 3. get success 100 times.
  for (int i = 0; i < 100; i++) {
    ret = ls_svr->get_ls_iter(guard, ObLSGetMod::OBSERVER_MOD);
    EXPECT_EQ(OB_SUCCESS, ret);
    if (OB_FAIL(ret)) {
      LOG_WARN("get ls iter failed again");
    }
  }
  ASSERT_NE(nullptr, guard.get_ptr());
  // 4. replacing the test-owned guard eventually leaves exactly one iterator.
  ASSERT_TRUE(wait_ls_iter_cnt(ls_svr, 1));

  // 5. iter the ls.
  ObLSIterator *iter = NULL;
  if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is NULL", K(ret), K(iter));
  } else {
    LOG_INFO("iter cnt", K(ls_svr->iter_cnt_));
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next log stream failed", K(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls in null");
      } else {
        LOG_INFO("there is a ls", KPC(ls));
      }
    }
  }

  // 6. check ls service is empty
  ASSERT_EQ(true, ls_svr->is_empty());

  // 7. stop ls service
  ls_svr->stop();
  ASSERT_EQ(OB_NOT_RUNNING, ls_svr->get_ls_iter(guard, ObLSGetMod::OBSERVER_MOD));

  ls_svr->is_inited_ = false;
  // 8. get iter failed.
  ASSERT_EQ(OB_NOT_INIT, ls_svr->get_ls_iter(guard, ObLSGetMod::OBSERVER_MOD));

  // 9. destroy immediately
  guard.reset();
  start_time = ObTimeUtil::current_time();
  ls_svr->wait();
  ls_svr->destroy();
  end_time = ObTimeUtil::current_time();
  ASSERT_TRUE(end_time - start_time <= 60 * 1000 * 1000);
}


} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ls_service.log*");
  OB_LOGGER.set_file_name("test_ls_service.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
