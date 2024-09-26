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
  TestTabletFreeze() {}
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
}

void TestTabletFreeze::create_tablets(const ObLSID ls_id){

  // 1. create a tablet
  ObLSHandle handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
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

void TestTabletFreeze::loop_tablet_freeze(const bool is_sync)
{
  const int64_t ASYNC_FREEZE_CNT = 1000;
  const int64_t start_time = ObClockGenerator::getClock();

  for (int64_t i = 0; i <= TEST_TABLET_COUNT; i++) {
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

  bool async_freeze_finished = false;
  int64_t max_retry_times = 20;
  while (max_retry_times-- > 0) {
    if (ls_->get_freezer()->get_async_freeze_tablets().empty()) {
      async_freeze_finished = true;
      break;
    }
    ::sleep(1);
  }
  ASSERT_EQ(true, async_freeze_finished);

  int ret = OB_SUCCESS;
  MTL_SWITCH(TEST_TENANT_ID) {
    MTL(ObTenantFreezer*)->stop();
    MTL(ObTenantFreezer*)->wait();
  };
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
