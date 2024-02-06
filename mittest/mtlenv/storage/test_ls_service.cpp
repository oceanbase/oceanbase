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
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>
#include <gmock/gmock.h>
#define USING_LOG_PREFIX STORAGETEST

#define protected public
#define private public
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/slog/ob_storage_logger.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/init_basic_struct.h"
#include "storage/test_tablet_helper.h"

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
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
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
  EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(not_exist_id, true));

  // 2. remove exist.
  EXPECT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_100, exist));
  EXPECT_TRUE(exist);
  EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_100, true));
  EXPECT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_100, exist));
  EXPECT_FALSE(exist);

  EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_102, true));

  // 3. check empty iter.
  iter.reset();
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls_iter(iter, ObLSGetMod::STORAGE_MOD));
  EXPECT_EQ(OB_ITER_END, iter->get_next(ls));
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

  EXPECT_EQ(OB_SUCCESS, ls_svr->remove_ls(ls_id, true));
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
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_104, true));
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
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_104, true));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_104, exist));
  ASSERT_FALSE(exist);
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
