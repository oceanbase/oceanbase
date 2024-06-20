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
#define USING_LOG_PREFIX TABLELOCK
#include <gtest/gtest.h>
#define protected public
#define private public

#include "storage/ls/ob_ls.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "mtlenv/tablelock/table_lock_common_env.h"
#include "mtlenv/tablelock/table_lock_tx_common_env.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/init_basic_struct.h"
#include "storage/tablelock/ob_lock_memtable.h"

namespace oceanbase
{
namespace storage
{
using namespace checkpoint;
}
namespace transaction
{
namespace tablelock
{
class TestLockMemtableCheckpoint : public ::testing::Test
{
public:
  TestLockMemtableCheckpoint()
    : fake_t3m_(common::OB_SERVER_TENANT_ID),
      ls_id_(ObLSID(100)),
      ls_handle_()
  {
    LOG_INFO("construct TestLockMemtableCheckpoint");
  }
  ~TestLockMemtableCheckpoint() = default;

  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp() override
  {
    ObCreateLSArg arg;
    ObLSService *ls_svr = MTL(ObLSService*);

    ASSERT_NE(nullptr, ls_svr);
    ASSERT_EQ(OB_SUCCESS,
              storage::gen_create_ls_arg(OB_SYS_TENANT_ID, ls_id_, arg));
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->create_ls(arg));
    ASSERT_EQ(OB_SUCCESS,
              ls_svr->get_ls(ls_id_, ls_handle_, ObLSGetMod::TABLELOCK_MOD));
    ASSERT_NE(nullptr, ls_ = ls_handle_.get_ls());
    ASSERT_EQ(OB_SUCCESS, ls_->get_lock_table()->get_lock_memtable(table_handle_));
    ASSERT_EQ(OB_SUCCESS, table_handle_.get_lock_memtable(memtable_));
    LOG_INFO("set up success");
  }
  void TearDown() override
  {
    LOG_INFO("tear down success");
  }

private:
  ObLockMemtable *memtable_;
  ObTableHandleV2 table_handle_;
  ObTenantMetaMemMgr fake_t3m_;
  ObFreezer freezer_;
  ObLSID ls_id_;
  ObLS *ls_;
  ObLSHandle ls_handle_;

  ObArenaAllocator allocator_;
};

void TestLockMemtableCheckpoint::SetUpTestCase()
{
  LOG_INFO("SetUpTestCase");
  init_default_lock_test_value();
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
}

void TestLockMemtableCheckpoint::TearDownTestCase()
{
  LOG_INFO("TearDownTestCase");
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestLockMemtableCheckpoint, replay_disorder)
{
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder");
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  int ret = OB_SUCCESS;
  ObCreateLSArg arg;

  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_base();
  commit_scn.set_base();

  // 1. get ls checkpoint
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 1");
  ObCheckpointExecutor *checkpoint_executor = ls_->get_checkpoint_executor();
  ObCommonCheckpoint *checkpoint =
      dynamic_cast<ObLSTxService *>(
          checkpoint_executor
              ->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
          ->common_checkpoints_[ObCommonCheckpointType::LOCK_MEMTABLE_TYPE];

  // 2.recover unlock op and lock op
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 2");
  ret = memtable_->recover_obj_lock(DEFAULT_OUT_TRANS_UNLOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = memtable_->recover_obj_lock(DEFAULT_OUT_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. update lock status disorder
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 3");
  commit_version.val_ = 3;
  commit_scn.val_ = 3;
  ret = memtable_->update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  commit_version.val_ = 2;
  commit_scn.val_ = 2;
  ret = memtable_->update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. check checkpoint
  // The rec_scn should be equal with the smaller commit_scn
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 4");
  ASSERT_EQ(commit_scn.val_, memtable_->get_rec_scn().val_);
  share::SCN rec_scn = checkpoint->get_rec_scn();
  ASSERT_EQ(commit_scn.val_, rec_scn.val_);

  // 5. flush and get a previous commit log
  // You will find the log about disordered replay in the log file.
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 5");
  ret = memtable_->recover_obj_lock(DEFAULT_OUT_TRANS_UNLOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, checkpoint->flush(share::SCN::max_scn(), 0));
  commit_version.val_ = 1;
  commit_scn.val_ = 1;
  ret = memtable_->update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 6. check checkpoint
  // The rec_scn should be equal with the smaller commit_scn
  // during flushing (i.e. it's get from pre_rec_scn)
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 6");
  ASSERT_EQ(commit_scn.val_, memtable_->get_rec_scn().val_);
  rec_scn = checkpoint->get_rec_scn();
  ASSERT_EQ(commit_scn.val_, rec_scn.val_);

  // 7. get a commit log with a commit_scn which
  // is larger than freeze_scn during flushing
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 7");
  ret = memtable_->recover_obj_lock(DEFAULT_OUT_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  commit_version.val_ = 4;
  commit_scn.val_ = 4;
  ret = memtable_->update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);

  // 8. check checkpoint
  // The rec_scn should still be equal with the smaller
  // commit_scn during flushing (i.e. it's get from pre_rec_scn)
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 8");
  ASSERT_EQ(1, memtable_->get_rec_scn().val_);
  rec_scn = checkpoint->get_rec_scn();
  ASSERT_EQ(1, rec_scn.val_);

  // 9. flush finish
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 9");
  ret = memtable_->on_memtable_flushed();
  ASSERT_EQ(OB_SUCCESS, ret);

  // 10. check checkpoint
  // The rec_scn should be equal with the latest commit_scn
  // which got during previous flushing (i.e. it's get from rec_scn)
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 10");
  ASSERT_EQ(commit_scn.val_, memtable_->get_rec_scn().val_);
  rec_scn = checkpoint->get_rec_scn();
  ASSERT_EQ(commit_scn.val_, rec_scn.val_);

  // 11. clean up
  LOG_INFO("TestLockMemtableCheckpoint::replay_disorder 11");
  table_handle_.reset();
  ls_handle_.reset();
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id_));
}
}  // namespace tablelock
}  // namespace transaction
}  // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_lock_memtable_checkpoint.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
