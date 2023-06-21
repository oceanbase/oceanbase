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
#define UNITTEST

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/tablelock/ob_obj_lock.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "table_lock_common_env.h"
#include "table_lock_tx_common_env.h"

namespace oceanbase
{
namespace transaction
{
namespace tablelock
{

class TestObjLockMap : public MockTxEnv,
                       public ::testing::Test
{
public:
  TestObjLockMap()
    : MockTxEnv(MTL_ID(), ObLSID(1)),
      fake_t3m_(common::OB_SERVER_TENANT_ID)
  {}
  ~TestObjLockMap() {}

  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp() override
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
    // mock sequence no
    ObClockGenerator::init();
    create_memtable();
    LOG_INFO("set up success");
  }
  virtual void TearDown() override
  {
    ObClockGenerator::destroy();
    LOG_INFO("tear down success");
  }
public:
  void create_memtable()
  {
    ObITable::TableKey table_key;
    table_key.table_type_ = ObITable::LOCK_MEMTABLE;
    table_key.tablet_id_ = LS_LOCK_TABLET;
    table_key.scn_range_.start_scn_.convert_for_gts(1); // fake
    table_key.scn_range_.end_scn_.convert_for_gts(2); // fake

    ASSERT_EQ(OB_SUCCESS, memtable_.init(table_key, ls_id_, &freezer_));
    ASSERT_EQ(OB_SUCCESS, handle_.set_table(&memtable_, &fake_t3m_, ObITable::LOCK_MEMTABLE));
  }

  void start_tx(const ObTransID &tx_id, MyTxCtx &my_ctx)
  {
    MockTxEnv::start_tx(tx_id, handle_, my_ctx);
  }
  void get_store_ctx(MyTxCtx &my_ctx, ObStoreCtx &store_ctx)
  {
    MockTxEnv::get_store_ctx(my_ctx, &tx_table_, store_ctx);
  }
private:
  ObLockMemtable memtable_;
  ObTableHandleV2 handle_;
  storage::ObTenantMetaMemMgr fake_t3m_;
  ObFreezer freezer_;
  ObTxTable tx_table_;

  ObOBJLockMap lock_map_;
};

void TestObjLockMap::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  init_default_lock_test_value();
}

void TestObjLockMap::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestObjLockMap, lock)
{
  // only need check create obj lock and remove obj lock
  // the lock operation is dealt by obj lock.
  // LOCK
  // 1. LOCK
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = 0;
  ObOBJLock *obj_lock = NULL;
  ObTxIDSet conflict_tx_set;
  unsigned char lock_mode_in_same_trans = 0x0;
  ObLockParam param;

  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;
  // 1.1 in trans try lock
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);

  lock_map_.init();
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = lock_map_.lock(param,
                       store_ctx,
                       DEFAULT_IN_TRANS_LOCK_OP,
                       lock_mode_in_same_trans,
                       conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = lock_map_.get_obj_lock_with_ref_(DEFAULT_TABLET_LOCK_ID,
                                         obj_lock);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(obj_lock->row_exclusive_, 1);
  // 1.2 lock and wait
  is_try_lock = false;
  expired_time = 0;
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = lock_map_.lock(param,
                       store_ctx,
                       DEFAULT_IN_TRANS_LOCK_OP,
                       lock_mode_in_same_trans,
                       conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(obj_lock->row_exclusive_, 2);

  is_try_lock = false;
  expired_time = ObClockGenerator::getClock() + 1000;
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = lock_map_.lock(param,
                       store_ctx,
                       DEFAULT_IN_TRANS_LOCK_OP,
                       lock_mode_in_same_trans,
                       conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(obj_lock->row_exclusive_, 3);
  // 1.3 remove lock
  for (int i=3; i >0; i--) {
    lock_map_.remove_lock_record(DEFAULT_IN_TRANS_LOCK_OP);
    ASSERT_EQ(obj_lock->row_exclusive_, i - 1);
  }
  lock_map_.lock_map_.revert(obj_lock);
  obj_lock = NULL;
  // 1.4 obj lock release check
  ret = lock_map_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);

  lock_map_.reset();
}

TEST_F(TestObjLockMap, unlock)
{
  // only need check create obj lock and remove obj lock
  // the lock operation is dealt by obj lock.
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = 0;
  ObOBJLock *obj_lock = NULL;
  share::SCN min_commited_scn;
  share::SCN flushed_scn;
  ObTxIDSet conflict_tx_set;
  unsigned char lock_mode_in_same_trans = 0x0;
  ObLockParam param;

  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;
  min_commited_scn.set_min();
  flushed_scn.set_min();
  // 1.1 unlock not exist out trans lock
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);
  lock_map_.init();
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = lock_map_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  // 1.2 lock
  ret = lock_map_.lock(param,
                       store_ctx,
                       DEFAULT_OUT_TRANS_LOCK_OP,
                       lock_mode_in_same_trans,
                       conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = lock_map_.get_min_ddl_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 1.3 unlock not complete lock
  ret = lock_map_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_COMPLETED, ret);
  // 1.4 commit out trans lock
  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_base();
  commit_scn.set_base();
  ret = lock_map_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = lock_map_.get_min_ddl_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  // 1.5 unlock success.
  ret = lock_map_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.6 unlock again
  ret = lock_map_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_UNLOCK_CONFLICT, ret);
  // 1.7 unlock commit
  ret = lock_map_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = lock_map_.get_min_ddl_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 1.8 obj lock release check
  ret = lock_map_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);

  lock_map_.reset();
}

TEST_F(TestObjLockMap, recover)
{
  // only need check create obj lock and remove obj lock
  // the lock operation is dealt by obj lock.
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = 0;
  ObOBJLock *obj_lock = NULL;
  share::SCN min_commited_scn;
  share::SCN flushed_scn;
  min_commited_scn.set_min();
  flushed_scn.set_min();
  lock_map_.init();
  // 1 recover in trans lock
  // 1.1 recover
  ret = lock_map_.recover_obj_lock(DEFAULT_IN_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check exist
  ret = lock_map_.get_obj_lock_with_ref_(DEFAULT_TABLET_LOCK_ID,
                                         obj_lock);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(obj_lock->row_exclusive_, 1);
  // 1.3 commit
  lock_map_.remove_lock_record(DEFAULT_IN_TRANS_LOCK_OP);
  // 1.4 check exist
  ret = lock_map_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);

  // 2 recover out trans lock
  // 2.1 recover
  ret = lock_map_.recover_obj_lock(DEFAULT_OUT_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 check exist
  ret = lock_map_.get_obj_lock_with_ref_(DEFAULT_TABLE_LOCK_ID,
                                         obj_lock);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = lock_map_.get_min_ddl_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 2.3 commit
  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_base();
  commit_scn.set_base();
  ret = lock_map_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = lock_map_.get_min_ddl_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  // 2.4 unlock
  ret = lock_map_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.5 unlock commit
  ret = lock_map_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.6 check exist
  min_commited_scn = lock_map_.get_min_ddl_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  ret = lock_map_.unlock(DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);

  lock_map_.reset();
}

} // tablelock
} // transaction
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_obj_lock_map.log*");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_obj_lock_map.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObLogger::get_logger().set_enable_async_log(false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
