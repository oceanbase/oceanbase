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

TEST_F(TestLSService, check_ls_iter_cnt)
{
  int ret = OB_SUCCESS;
  int64_t start_time = 0;
  int64_t end_time = 0;
  EventItem item;
  item.trigger_freq_ = 1;
  item.error_code_ = 4013;
  EventTable::instance().set_event("ALLOC_LS_ITER_GUARD_FAIL", item);

  LOG_INFO("TestLSService::check_ls_iter_cnt");
  ObLS *ls = NULL;
  ObLSService* ls_svr = MTL(ObLSService*);
  common::ObSharedGuard<ObLSIterator> guard;
  // 1. get ls iter 100 times.
  for (int i = 0; i < 100; i++) {
    if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::OBSERVER_MOD))) {
      LOG_WARN("get ls iter failed");
    }
  }
  // 2. check the iter cnt, it should smaller than 100.
  ASSERT_EQ(0, ls_svr->iter_cnt_);

  // 3. get success 100 times.
  item.trigger_freq_ = 0;
  EventTable::instance().set_event("ALLOC_LS_ITER_GUARD_FAIL", item);
  for (int i = 0; i < 100; i++) {
    if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::OBSERVER_MOD))) {
      LOG_WARN("get ls iter failed again");
    }
  }
  // 4. check the iter cnt, it should smaller than 100.
  ASSERT_EQ(1, ls_svr->iter_cnt_);

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
