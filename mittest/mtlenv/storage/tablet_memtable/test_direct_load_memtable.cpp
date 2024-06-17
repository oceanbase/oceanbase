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

#include "storage/ls/ob_ls.h"

#undef private
#undef protected

int64_t TEST_TENANT_ID = 0;
const ObLSID TEST_LS_ID(1001);
const int64_t TEST_TABLE_ID = 500001;
const int64_t TEST_TABLET_ID = 200001;

namespace oceanbase
{

namespace storage
{

class TestDirectLoadPlusOffline : public ::testing::Test
{
public:
  TestDirectLoadPlusOffline() {}
  virtual ~TestDirectLoadPlusOffline() {}

  virtual void SetUp() override { ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited()); }
  virtual void TearDown() override {}
  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
  }
  static void TearDownTestCase() { MockTenantModuleEnv::get_instance().destroy(); }

  void offline_ls_with_active_direct_load_memtable();

public:
  void create_ls(const ObLSID ls_id, ObLS *&ls);
  void create_tablets(const ObLSID ls_id);

public:
  common::ObArenaAllocator allocator_;
  ObLS *ls_;
};

void TestDirectLoadPlusOffline::create_ls(const ObLSID ls_id, ObLS *&ls)
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

void TestDirectLoadPlusOffline::create_tablets(const ObLSID ls_id){

  // 1. create a tablet
  ObLSHandle handle;
  ObLSService *ls_svr = MTL(ObLSService *);
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  share::schema::ObTableSchema table_schema;
  const int64_t start_time = ObClockGenerator::getClock();
  ObSqlString str;

  ObTabletID tablet_id = ObTabletID(TEST_TABLET_ID);
  str.assign_fmt("test_table_%ld", TEST_TABLE_ID);
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, TEST_TABLE_ID));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(handle, tablet_id, table_schema, allocator_));

  // 2. test tablet
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  STORAGE_LOG(INFO, "TestLSService::tablet_test 2.");
  ASSERT_EQ(OB_SUCCESS, ls->get_tablet(tablet_id, tablet_handle));
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  ASSERT_EQ(tablet_id, tablet->get_tablet_meta().tablet_id_);
  const int64_t spend_time_ms = (ObClockGenerator::getClock() - start_time) / 1000;
}

void TestDirectLoadPlusOffline::offline_ls_with_active_direct_load_memtable()
{
  int ret = OB_SUCCESS;
  create_ls(TEST_LS_ID, ls_);
  create_tablets(TEST_LS_ID);

  ASSERT_EQ(OB_SUCCESS,
            ls_->get_tablet_svr()->create_memtable(ObTabletID(TEST_TABLET_ID),
                                                   0 /* schema version */,
                                                   true /* for_direct_load */,
                                                   false /*for_replay*/,
                                                   SCN::min_scn() /*clog_checkpoint*/));

  ASSERT_EQ(OB_SUCCESS, ls_->offline());
}

TEST_F(TestDirectLoadPlusOffline, offline_ls_with_active_direct_load_memtable)
{
  TEST_TENANT_ID = MTL_ID();
  offline_ls_with_active_direct_load_memtable();
}


}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 1;
  system("rm -f test_direct_load_plus_offline.log*");
  system("rm -fr run_*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_direct_load_plus_offline.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);

  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();

  return ret;
}
