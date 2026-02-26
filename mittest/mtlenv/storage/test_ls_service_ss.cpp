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
#ifdef OB_BUILD_SHARED_STORAGE

#define protected public
#define private public
#include "src/storage/slog/ob_storage_log_item.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/test_tablet_helper.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
using namespace common;
using namespace rpc;
using namespace rpc::frame;
using namespace share;
namespace storage
{
int ObLSService::check_sslog_ls_exist()
{
  int ret = OB_SUCCESS;
  return ret;
}

// Test class for shared storage mode
class TestLSServiceSS : public ::testing::Test
{
public:
  TestLSServiceSS() = default;
  virtual ~TestLSServiceSS() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }
private:
  common::ObArenaAllocator allocator_;
};

void TestLSServiceSS::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
}

void TestLSServiceSS::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestLSServiceSS, test_remove_ls_parameter_merge_ss)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t cnt = 0;
  ObCreateLSArg arg;
  ObLSService* ls_svr = MTL(ObLSService*);
  bool exist = false;
  bool waiting = false;
  ObLSID id_200(200);
  ObLSID id_201(201);
  ObLSHandle handle;
  ObLS *ls = nullptr;

  LOG_INFO("TestLSServiceSS::test_remove_ls_parameter_merge_ss");

  // Test purpose: Verify that remove_ls_ function signature works correctly
  // in shared storage mode, where delete_ls uses update_tenant_ls_item instead of writing slog

  // 1. Test with remove_from_disk = true
  LOG_INFO("TestLSServiceSS::test_remove_ls_parameter_merge_ss: test remove_from_disk = true");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_200, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_200, exist));
  ASSERT_TRUE(exist);

  // Use public remove_ls interface which internally calls remove_ls_ with remove_from_disk = true
  // In shared storage mode, this will call update_tenant_ls_item instead of writing slog
  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_200));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_200, exist));
  ASSERT_FALSE(exist);
  cnt = 0;
  while (cnt++ < 20) {
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_200, waiting));
    if (waiting) {
      ::sleep(1);
    } else {
      break;
    }
  }

  // 2. Test direct call to remove_ls_ with remove_from_disk = true
  LOG_INFO("TestLSServiceSS::test_remove_ls_parameter_merge_ss: test direct call with remove_from_disk = true");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_201, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(id_201, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);

  // Prepare LS for removal (similar to safe_remove_ls_)
  ASSERT_EQ(OB_SUCCESS, ls->offline(true));
  ASSERT_EQ(OB_SUCCESS, ls->stop());
  ls->wait();

  ObLSLockGuard lock_ls(ls);
  ASSERT_EQ(OB_SUCCESS, ls->set_remove_state(true));
  ASSERT_EQ(OB_SUCCESS, ls->prepare_for_safe_destroy());

  // In shared storage mode, remove_ls_ will call write_tablet_id_set_to_pending_free
  // and then use update_tenant_ls_item instead of writing slog
  ls_svr->remove_ls_(ls, true/*remove_from_disk*/);
  lock_ls.~ObLSLockGuard();
  handle.reset();

  // Verify LS is removed
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_201, exist));
  ASSERT_FALSE(exist);
  cnt = 0;
  while (cnt++ < 20) {
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_201, waiting));
    if (waiting) {
      ::sleep(1);
    } else {
      break;
    }
  }

  LOG_INFO("TestLSServiceSS::test_remove_ls_parameter_merge_ss: parameter merge verified in shared storage mode");
}

TEST_F(TestLSServiceSS, test_remove_ls_retry_after_write_slog_ss)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t cnt = 0;
  ObCreateLSArg arg;
  ObLSService* ls_svr = MTL(ObLSService*);
  bool exist = false;
  bool waiting = false;
  ObLSID id_202(202);
  ObLSHandle handle;
  ObLS *ls = nullptr;

  LOG_INFO("TestLSServiceSS::test_remove_ls_retry_after_write_slog_ss");

  // Test scenario: In shared storage mode, verify that remove_ls_ handles
  // update_tenant_ls_item correctly and retry mechanism works

  // 1. Create a LS for testing
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_202, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(id_202, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);

  // 2. Prepare LS for removal
  ASSERT_EQ(OB_SUCCESS, ls->offline(true));
  ASSERT_EQ(OB_SUCCESS, ls->stop());
  ls->wait();

  ObLSLockGuard lock_ls(ls);
  ASSERT_EQ(OB_SUCCESS, ls->set_remove_state(true));

  // 3. In shared storage mode, write_tablet_id_set_to_pending_free is called first
  ASSERT_EQ(OB_SUCCESS, ls->write_tablet_id_set_to_pending_free());
  ASSERT_EQ(OB_SUCCESS, ls->prepare_for_safe_destroy());

  // 4. Manually update tenant ls item to simulate the scenario where
  // update_tenant_ls_item is called successfully but subsequent steps might fail
  const share::ObLSID &ls_id = ls->get_ls_id();
  ObTenantStorageMetaService *meta_service = MTL(ObTenantStorageMetaService*);
  ASSERT_NE(nullptr, meta_service);
  // In shared storage mode, delete_ls calls update_tenant_ls_item
  ASSERT_EQ(OB_SUCCESS, meta_service->delete_ls(ls_id, ls->get_ls_epoch()));
  ASSERT_EQ(OB_SUCCESS, TENANT_STORAGE_META_SERVICE.delete_ls_item(ls_id, ls->get_ls_epoch()));

  // Verify LS still exists in map (item updated but remove_ls_ not completed yet)
  lock_ls.~ObLSLockGuard();
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_202, exist));
  ASSERT_TRUE(exist);

  // 5. Now call remove_ls_ which should handle the case where update_tenant_ls_item is already done
  // The remove_ls_ function will check and proceed with remaining steps
  ObLSLockGuard lock_ls2(ls);
  ASSERT_EQ(OB_SUCCESS, ls->write_tablet_id_set_to_pending_free());
  ASSERT_EQ(OB_SUCCESS, ls->prepare_for_safe_destroy());
  ls_svr->remove_ls_(ls, true/*remove_from_disk*/);
  lock_ls2.~ObLSLockGuard();
  handle.reset();

  // 6. Verify LS is removed from map
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_202, exist));
  ASSERT_FALSE(exist);
  cnt = 0;
  while (cnt++ < 20) {
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_202, waiting));
    if (waiting) {
      ::sleep(1);
    } else {
      break;
    }
  }

  LOG_INFO("TestLSServiceSS::test_remove_ls_retry_after_write_slog_ss: retry after update tenant ls item verified");
}

TEST_F(TestLSServiceSS, test_replay_remove_ls_idempotent_ss)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  int64_t cnt = 0;
  ObCreateLSArg arg;
  ObLSService* ls_svr = MTL(ObLSService*);
  bool exist = false;
  bool waiting = false;
  ObLSID id_203(203);
  ObLSHandle handle;
  ObLS *ls = nullptr;

  LOG_INFO("TestLSServiceSS::test_replay_remove_ls_idempotent_ss");

  // Test scenario: In shared storage mode, simulate restart and replay remove ls slog multiple times
  // The replay should be idempotent - safe to replay the same remove operation multiple times

  // 1. Create a LS
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, id_203, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_203, exist));
  ASSERT_TRUE(exist);

  // 2. Get LS and set it to remove state (simulating normal remove process)
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(id_203, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);

  ObLSLockGuard lock_ls(ls);
  // Set remove state without writing slog (simulating replay scenario)
  // In shared storage mode, replay_remove_ls also sets remove state
  ASSERT_EQ(OB_SUCCESS, ls->set_remove_state(false /*write_slog*/));
  lock_ls.~ObLSLockGuard();
  handle.reset();

  // 3. First replay remove ls (normal case)
  ASSERT_EQ(OB_SUCCESS, ls_svr->replay_remove_ls(id_203));

  // 4. Replay the same remove ls again (simulating duplicate replay after restart)
  // This should be safe - replay_remove_ls should handle the case where LS doesn't exist
  ASSERT_EQ(OB_SUCCESS, ls_svr->replay_remove_ls(id_203));

  ASSERT_EQ(OB_SUCCESS, ls_svr->remove_ls(id_203));
  // Verify LS still doesn't exist (idempotent)
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_203, exist));
  ASSERT_FALSE(exist);

  cnt = 0;
  while (cnt++ < 20) {
    ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_waiting_safe_destroy(id_203, waiting));
    if (waiting) {
      ::sleep(1);
    } else {
      break;
    }
  }

  LOG_INFO("TestLSServiceSS::test_replay_remove_ls_idempotent_ss: idempotent replay verified in shared storage mode");

  // 6. Test replay remove for non-existent LS (edge case)
  ObLSID id_204(204);
  ASSERT_EQ(OB_SUCCESS, ls_svr->replay_remove_ls(id_204));
  ASSERT_EQ(OB_SUCCESS, ls_svr->check_ls_exist(id_204, exist));
  ASSERT_FALSE(exist);

  LOG_INFO("TestLSServiceSS::test_replay_remove_ls_idempotent_ss: all scenarios verified in shared storage mode");
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ls_service_ss.log*");
  OB_LOGGER.set_file_name("test_ls_service_ss.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#endif // OB_BUILD_SHARED_STORAGE
