// owner: gengli.wzy
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

#include <gtest/gtest.h>

#define protected public
#define private public
#define UNITTEST

#include <iostream>
#include <thread>

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/init_basic_struct.h"
#include "storage/test_tablet_helper.h"
#include "storage/test_dml_common.h"

#include "storage/ls/ob_ls.h"
#include "storage/compaction/ob_tablet_merge_task.h"

#undef private
#undef protected

int64_t TEST_TENANT_ID = 0;
const ObLSID TEST_LS_ID(1001);
const int64_t START_TABLE_ID = 500001;
const int64_t START_TABLET_ID = 200001;
int64_t TEST_TABLET_COUNT = 1000;

namespace oceanbase
{

namespace storage
{

class TestTabletFreeze : public ::testing::Test
{
public:
  TestTabletFreeze() : ls_(nullptr) {}
  virtual ~TestTabletFreeze() {}

  virtual void SetUp() override { ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited()); }
  virtual void TearDown() override {}
  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
  }
  static void TearDownTestCase() { MockTenantModuleEnv::get_instance().destroy(); }

  void frequently_tablet_freeze();

public:
  void create_ls(const ObLSID ls_id, ObLS *&ls);
  void create_tablets(const ObLSID ls_id);
  void create_freeze_test_tablet(const ObLSID &ls_id, const ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  void prepare_valid_weak_read_scn();
  void wait_async_freeze_finished();
  void loop_tablet_freeze(const bool is_sync);
  void loop_ls_freeze(const bool is_sync);

public:
  common::ObArenaAllocator allocator_;
  ObLS *ls_;
};

void TestTabletFreeze::create_ls(const ObLSID ls_id, ObLS *&ls)
{
  uint64_t tenant_id = MTL_ID();
  ObCreateLSArg arg;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle handle;
  obrpc::ObBatchCreateTabletArg create_tablet_arg;
  ObMemberList member_list;
  int64_t paxos_replica_num = 1;
  int64_t leader_epoch = 0;
  ObTabletHandle tablet_handle;
  ObTablet *tablet =NULL;
  (void)member_list.add_server(MockTenantModuleEnv::get_instance().self_addr_);

  // create ls
  ASSERT_EQ(OB_SUCCESS, storage::gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD, ObLSAccessAttr::DISABLE_LOGONLY));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  GlobalLearnerList learner_list;
  ASSERT_EQ(OB_SUCCESS, ls->set_initial_member_list(member_list,
                                                    paxos_replica_num,
                                                    learner_list));

  bool is_leader = false;
  for (int i = 0; i < 15; i++) {
    ObRole role;
    int64_t proposal_id = 0;
    ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
    if (role == ObRole::LEADER) {
      is_leader = true;
      break;
    }
    ::sleep(1);
  }
  ASSERT_TRUE(is_leader);
}

void TestTabletFreeze::create_tablets(const ObLSID ls_id){

  // 1. create a tablet
  ObLSHandle handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD, ObLSAccessAttr::DISABLE_LOGONLY));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  share::schema::ObTableSchema table_schema;
  const int64_t start_time = ObClockGenerator::getClock();
  for (int i = 1; i < TEST_TABLET_COUNT; i++) {
    ObSqlString str;

    int64_t table_id = START_TABLE_ID + i;
    ObTabletID tablet_id = ObTabletID(START_TABLET_ID + i);
    str.assign_fmt("test_table_%ld", table_id);
    ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
    ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(handle, tablet_id, table_schema, allocator_));

    // 2. test tablet
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    STORAGE_LOG(INFO, "TestLSService::tablet_test 2.");
    ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
    tablet = tablet_handle.get_obj();
    ASSERT_NE(nullptr, tablet);
    ASSERT_EQ(tablet_id, tablet->get_tablet_meta().tablet_id_);
  }
  const int64_t spend_time_ms = (ObClockGenerator::getClock() - start_time) / 1000;
  fprintf(stdout, "create tablets finish. spend_time_ms = %ld\n", spend_time_ms);
}

void TestTabletFreeze::prepare_valid_weak_read_scn()
{
  share::SCN max_decided_scn;
  bool weak_read_scn_ready = false;
  for (int64_t i = 0; i < 60 && !weak_read_scn_ready; i++) {
    ASSERT_EQ(OB_SUCCESS, ls_->get_max_decided_scn(max_decided_scn));
    if (max_decided_scn > share::ObScnRange::MIN_SCN && !max_decided_scn.is_max()) {
      ls_->get_ls_wrs_handler()->ls_weak_read_ts_ = max_decided_scn;
      weak_read_scn_ready = true;
    } else {
      ::sleep(1);
    }
  }
  ASSERT_TRUE(weak_read_scn_ready);
}

void TestTabletFreeze::wait_async_freeze_finished()
{
  ObFreezer *freezer = ls_->get_freezer();
  ASSERT_NE(nullptr, freezer);

  bool async_freeze_finished = false;
  int64_t stable_times = 0;
  int64_t max_retry_times = 120;
  while (max_retry_times-- > 0) {
    const bool async_tablet_task_running = ATOMIC_LOAD(&freezer->is_async_tablet_freeze_task_existing_);
    const bool async_ls_task_running = ATOMIC_LOAD(&freezer->is_async_ls_freeze_task_existing_);
    const int64_t high_priority_freeze_cnt = ATOMIC_LOAD(&freezer->high_priority_freeze_cnt_);
    const int64_t low_priority_freeze_cnt = ATOMIC_LOAD(&freezer->low_priority_freeze_cnt_);
    if (!async_tablet_task_running
        && !async_ls_task_running
        && 0 == high_priority_freeze_cnt
        && 0 == low_priority_freeze_cnt
        && freezer->get_async_freeze_tablets().empty()) {
      if (++stable_times >= 2) {
        async_freeze_finished = true;
        break;
      }
    } else {
      stable_times = 0;
    }

    if (0 == max_retry_times % 10) {
      STORAGE_LOG(INFO,
                  "wait async freeze finish",
                  K(async_tablet_task_running),
                  K(async_ls_task_running),
                  K(high_priority_freeze_cnt),
                  K(low_priority_freeze_cnt));
    }
    ::sleep(1);
  }
  ASSERT_TRUE(async_freeze_finished);
}

void TestTabletFreeze::loop_tablet_freeze(const bool is_sync)
{
  const int64_t ASYNC_FREEZE_CNT = 1000;
  const int64_t start_time = ObClockGenerator::getClock();

  for (int64_t i = 1; i < TEST_TABLET_COUNT; i++) {
    ObTabletID tablet_id(START_TABLET_ID + i);
    STORAGE_LOG(INFO, "commit async tablet freeze", K(tablet_id));
    (void)ls_->tablet_freeze(tablet_id, false /* need_rewrite_meta */, is_sync, 0);
  }

  const int64_t end_time = ObClockGenerator::getClock();
  const int64_t spend_time_seconds = (end_time - start_time) / 1000 / 1000;
  STORAGE_LOG(INFO, "tablet freeze thread finish task", K(is_sync), K(spend_time_seconds), KTIME(start_time), KTIME(end_time));
  fprintf(stdout, "tablet freeze finish, is_sync = %d, spend_time_seconds = %ld\n", is_sync, spend_time_seconds);
}

void TestTabletFreeze::loop_ls_freeze(const bool is_sync)
{
  const int64_t ASYNC_FREEZE_CNT = 1000;
  const int64_t start_time = ObClockGenerator::getClock();

  for (int64_t i = 0; i <= 1000; i++) {
    ObTabletID tablet_id(START_TABLET_ID + i);
    STORAGE_LOG(INFO, "commit async tablet freeze", K(tablet_id));
    (void)ls_->logstream_freeze(checkpoint::INVALID_TRACE_ID, is_sync, 0);
  }

  const int64_t end_time = ObClockGenerator::getClock();
  const int64_t spend_time_seconds = (end_time - start_time) / 1000 / 1000;
  STORAGE_LOG(
      INFO, "ls freeze thread finish task", K(is_sync), K(spend_time_seconds), KTIME(start_time), KTIME(end_time));
  fprintf(stdout, "ls freeze finish, is_sync = %d, spend_time_seconds = %ld\n", is_sync, spend_time_seconds);
}

void TestTabletFreeze::frequently_tablet_freeze()
{
  create_ls(TEST_LS_ID, ls_);
  create_tablets(TEST_LS_ID);
  prepare_valid_weak_read_scn();

  const int64_t ASYNC_TABLET_FREEZE_THREAD = 5;
  const int64_t SYNC_TABLET_FREEZE_THREAD = 2;
  const int64_t ASYNC_LS_FREEZE_THREAD = 1;
  const int64_t SYNC_LS_FREEZE_THREAD = 1;
  std::vector<std::thread> alloc_threads;

  // async tablet freeze
  for (int64_t i = 0; i < ASYNC_TABLET_FREEZE_THREAD; i++) {
    alloc_threads.push_back(std::thread([this]() {
      int ret = OB_SUCCESS;
      MTL_SWITCH(TEST_TENANT_ID) { this->loop_tablet_freeze(false /*is_sync*/); };
    }));
  }

  // sync tablet freeze
  for (int64_t i = 0; i < SYNC_TABLET_FREEZE_THREAD; i++) {
    alloc_threads.push_back(std::thread([this]() {
      int ret = OB_SUCCESS;
      MTL_SWITCH(TEST_TENANT_ID) { this->loop_tablet_freeze(true /*is_sync*/); };
    }));
  }

  // sync logstream freeze
  alloc_threads.push_back(std::thread([this]() {
    int ret = OB_SUCCESS;
    MTL_SWITCH(TEST_TENANT_ID) { this->loop_ls_freeze(true /*is_sync*/); };
  }));

  // async logstream freeze
  alloc_threads.push_back(std::thread([this]() {
    int ret = OB_SUCCESS;
    MTL_SWITCH(TEST_TENANT_ID) { this->loop_ls_freeze(false /*is_sync*/); };
  }));

  for (int i = 0;
       i < ASYNC_TABLET_FREEZE_THREAD + SYNC_TABLET_FREEZE_THREAD + ASYNC_LS_FREEZE_THREAD + SYNC_LS_FREEZE_THREAD;
       i++) {
    alloc_threads[i].join();
  }

  wait_async_freeze_finished();

  int ret = OB_SUCCESS;
  MTL_SWITCH(TEST_TENANT_ID) {
    MTL(ObTenantFreezer*)->stop();
    MTL(ObTenantFreezer*)->wait();
  };
}

void TestTabletFreeze::create_freeze_test_tablet(
    const ObLSID &ls_id, const ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  create_ls(ls_id, ls_);
  ASSERT_NE(nullptr, ls_);
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD, ObLSAccessAttr::DISABLE_LOGONLY));
  share::schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, START_TABLE_ID));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_,
      ObTabletStatus::NORMAL, share::SCN::base_scn(), tablet_handle));
  prepare_valid_weak_read_scn();
}

TEST_F(TestTabletFreeze, rewrite_snapshot_without_memtable)
{
  const ObLSID ls_id(1002);
  const ObTabletID tablet_id(START_TABLET_ID);
  ObTabletHandle old_handle;
  create_freeze_test_tablet(ls_id, tablet_id, old_handle);
  ASSERT_TRUE(old_handle.is_valid());
  ObTablet *old_tablet = old_handle.get_obj();
  ObTabletMemtableMgr *memtable_mgr = static_cast<ObTabletMemtableMgr *>(old_tablet->get_memtable_mgr());
  ASSERT_NE(nullptr, memtable_mgr);
  ASSERT_FALSE(memtable_mgr->has_memtable());
  const int64_t old_snapshot = old_tablet->get_snapshot_version();
  const SCN old_checkpoint = old_tablet->get_clog_checkpoint_scn();
  // A snapshot rewrite must not acknowledge the previous medium compaction's checksum check.
  compaction::ObExtraMediumInfo &extra_info = old_tablet->mds_data_.extra_medium_info_;
  extra_info.last_compaction_type_ = compaction::ObMediumCompactionInfo::MAJOR_COMPACTION;
  extra_info.last_medium_scn_ = 1;
  extra_info.wait_check_flag_ = true;

  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<ObTableHandleV2, 1> frozen_handles;
  ObSEArray<ObTabletID, 1> failed_tablets;
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id));
  ObFreezer *freezer = ls_->get_freezer();
  ASSERT_NE(nullptr, freezer);
  const SCN snapshot = ls_->get_ls_wrs_handler()->get_ls_weak_read_ts();
  ASSERT_EQ(OB_SUCCESS, freezer->tablet_freeze(checkpoint::INVALID_TRACE_ID, tablet_ids,
      true /*need_rewrite_meta*/, frozen_handles, failed_tablets));
  // No frozen handles means tablet_freeze_ cannot enter submit_log_if_needed_.
  EXPECT_TRUE(frozen_handles.empty());
  EXPECT_TRUE(failed_tablets.empty());
  EXPECT_FALSE(memtable_mgr->has_memtable());
  ObTabletHandle new_handle;
  ASSERT_EQ(OB_SUCCESS, ls_->get_tablet(tablet_id, new_handle));
  const ObTablet *new_tablet = new_handle.get_obj();
  ASSERT_NE(nullptr, new_tablet);
  EXPECT_EQ(snapshot.get_val_for_tx(), new_tablet->get_snapshot_version());
  EXPECT_GT(new_tablet->get_snapshot_version(), old_snapshot);
  EXPECT_EQ(old_checkpoint, new_tablet->get_clog_checkpoint_scn());
  EXPECT_EQ(old_snapshot, old_tablet->get_snapshot_version());
  EXPECT_EQ(extra_info.info_, new_tablet->mds_data_.extra_medium_info_.info_);
  EXPECT_EQ(extra_info.last_medium_scn_, new_tablet->mds_data_.extra_medium_info_.last_medium_scn_);
  EXPECT_TRUE(new_tablet->get_tablet_addr().is_disked());

  const int64_t new_snapshot = new_tablet->get_snapshot_version();
  ASSERT_EQ(OB_SUCCESS, ls_->get_tablet_svr()->update_tablet_snapshot_version(tablet_id, old_snapshot));
  ASSERT_EQ(OB_SUCCESS, ls_->get_tablet(tablet_id, new_handle));
  EXPECT_EQ(new_snapshot, new_handle.get_obj()->get_snapshot_version());
}

TEST_F(TestTabletFreeze, rewrite_snapshot_waits_for_mini_dag)
{
  const ObLSID ls_id(1003);
  const ObTabletID tablet_id(START_TABLET_ID);
  ObTabletHandle tablet_handle;
  create_freeze_test_tablet(ls_id, tablet_id, tablet_handle);
  ASSERT_TRUE(tablet_handle.is_valid());
  const int64_t old_snapshot = tablet_handle.get_obj()->get_snapshot_version();
  compaction::ObTabletMergeDagParam param(MINI_MERGE, ls_id, tablet_id);
  param.for_diagnose_ = true;
  compaction::ObTabletMiniMergeDag mini_dag;
  ASSERT_EQ(OB_SUCCESS, mini_dag.init_by_param(&param));
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_NE(nullptr, scheduler);
  // Register only the DAG identity, so a background worker cannot finish it before the check.
  {
    ObThreadCondGuard guard(scheduler->scheduler_sync_);
    ASSERT_EQ(OB_SUCCESS, scheduler->dag_map_.set_refactored(&mini_dag, &mini_dag));
  }
  const SCN snapshot = ls_->get_ls_wrs_handler()->get_ls_weak_read_ts();
  const int ret = ls_->get_freezer()->handle_no_active_memtable_(tablet_id, tablet_handle.get_obj(), snapshot);
  {
    ObThreadCondGuard guard(scheduler->scheduler_sync_);
    EXPECT_EQ(OB_SUCCESS, scheduler->dag_map_.erase_refactored(&mini_dag));
  }
  EXPECT_EQ(OB_EAGAIN, ret);
  ObTabletHandle current_handle;
  ASSERT_EQ(OB_SUCCESS, ls_->get_tablet(tablet_id, current_handle));
  EXPECT_EQ(old_snapshot, current_handle.get_obj()->get_snapshot_version());
}

TEST_F(TestTabletFreeze, rewrite_snapshot_waits_for_frozen_memtable)
{
  const ObLSID ls_id(1004);
  const ObTabletID tablet_id(START_TABLET_ID);
  ObTabletHandle tablet_handle;
  create_freeze_test_tablet(ls_id, tablet_id, tablet_handle);
  ASSERT_TRUE(tablet_handle.is_valid());
  ObTablet *tablet = tablet_handle.get_obj();
  const int64_t old_snapshot = tablet->get_snapshot_version();
  ASSERT_EQ(OB_SUCCESS, ls_->get_tablet_svr()->create_memtable(tablet_id, 0));
  ObTabletMemtableMgr *memtable_mgr = static_cast<ObTabletMemtableMgr *>(tablet->get_memtable_mgr());
  ASSERT_NE(nullptr, memtable_mgr);
  ObTableHandleV2 handle;
  ASSERT_EQ(OB_SUCCESS, memtable_mgr->set_is_tablet_freeze_for_active_memtable(handle, checkpoint::INVALID_TRACE_ID));
  ASSERT_TRUE(memtable_mgr->has_memtable());
  EXPECT_FALSE(memtable_mgr->has_active_memtable());
  const SCN snapshot = ls_->get_ls_wrs_handler()->get_ls_weak_read_ts();
  EXPECT_EQ(OB_SUCCESS, ls_->get_freezer()->handle_no_active_memtable_(tablet_id, tablet, snapshot));
  memtable::ObMemtable *memtable = nullptr;
  ASSERT_EQ(OB_SUCCESS, handle.get_data_memtable(memtable));
  ASSERT_NE(nullptr, memtable);
  memtable->is_tablet_freeze_ = false; // restore the synthetic frozen state before fixture teardown
  ObTabletHandle current_handle;
  ASSERT_EQ(OB_SUCCESS, ls_->get_tablet(tablet_id, current_handle));
  EXPECT_EQ(old_snapshot, current_handle.get_obj()->get_snapshot_version());
}

TEST_F(TestTabletFreeze, rewrite_snapshot_respects_transfer_scn)
{
  const ObLSID ls_id(1005);
  const ObTabletID tablet_id(START_TABLET_ID);
  ObTabletHandle tablet_handle;
  create_freeze_test_tablet(ls_id, tablet_id, tablet_handle);
  ASSERT_TRUE(tablet_handle.is_valid());
  ObTablet *tablet = tablet_handle.get_obj();
  ObFreezer *freezer = ls_->get_freezer();
  ASSERT_NE(nullptr, freezer);
  const SCN snapshot = SCN::plus(SCN::base_scn(), 200);
  SCN real_snapshot;
  ObTabletCreateDeleteMdsUserData user_data;
  user_data.tablet_status_ = ObTabletStatus::TRANSFER_OUT;
  user_data.data_type_ = ObTabletMdsUserDataType::START_TRANSFER_OUT;
  user_data.transfer_ls_id_ = ObLSID(1006);
  user_data.create_commit_scn_ = SCN::base_scn();
  user_data.create_commit_version_ = user_data.create_commit_scn_.get_val_for_tx();
  user_data.transfer_scn_ = SCN::min_scn();
  mds::MdsCtx ctx(mds::MdsWriter(transaction::ObTransID(123)));
  ASSERT_EQ(OB_SUCCESS, tablet->set(user_data, ctx));
  ASSERT_EQ(OB_SUCCESS, freezer->decide_real_snapshot_version_(tablet_id, tablet, snapshot, real_snapshot));
  EXPECT_EQ(snapshot, real_snapshot); // transfer state exists but its SCN has not been decided yet
  ctx.on_abort(SCN::plus(SCN::base_scn(), 1));

  user_data.transfer_scn_ = SCN::plus(SCN::base_scn(), 100);
  mds::MdsCtx committed_ctx(mds::MdsWriter(transaction::ObTransID(124)));
  ASSERT_EQ(OB_SUCCESS, tablet->set(user_data, committed_ctx));
  committed_ctx.single_log_commit(user_data.transfer_scn_, user_data.transfer_scn_);
  ASSERT_EQ(OB_SUCCESS, freezer->decide_real_snapshot_version_(tablet_id, tablet, snapshot, real_snapshot));
  EXPECT_EQ(user_data.transfer_scn_, real_snapshot);
  ASSERT_EQ(OB_SUCCESS, freezer->handle_no_active_memtable_(tablet_id, tablet, snapshot));
  ObTabletHandle current_handle;
  ASSERT_EQ(OB_SUCCESS, ls_->get_tablet_svr()->get_tablet(tablet_id, current_handle,
      ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
  EXPECT_EQ(user_data.transfer_scn_.get_val_for_tx(), current_handle.get_obj()->get_snapshot_version());
}

TEST_F(TestTabletFreeze, frequently_async_freeze)
{
  TEST_TENANT_ID = MTL_ID();
  (void)frequently_tablet_freeze();
}


}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 1;
  system("rm -f test_tablet_freeze.log*");
  system("rm -fr run_*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_tablet_freeze.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);

  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();

  return ret;
}
