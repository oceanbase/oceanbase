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
#include "storage/tablelock/ob_lock_memtable.h"
#include "table_lock_common_env.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "table_lock_tx_common_env.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace memtable;
using namespace storage;
using namespace transaction;
using namespace blocksstable;

namespace storage {
int ObTxTable::online()
{
  ATOMIC_INC(&epoch_);
  ATOMIC_STORE(&state_, TxTableState::ONLINE);
  return OB_SUCCESS;
}
}  // namespace storage

namespace memtable
{
int ObMvccWriteGuard::write_auth(ObStoreCtx &) { return OB_SUCCESS; }

ObMvccWriteGuard::~ObMvccWriteGuard() {}
}

namespace transaction
{
namespace tablelock
{

class TestLockMemtable : public MockTxEnv,
                         public ::testing::Test
{
public:
  TestLockMemtable()
    : MockTxEnv(MTL_ID(), ObLSID(1)),
      fake_t3m_(common::OB_SERVER_TENANT_ID)
  {}
  ~TestLockMemtable() {}

  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp() override
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
    // mock sequence no
    ObClockGenerator::init();
    create_memtable();
    tx_table_.online();
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
};

void TestLockMemtable::SetUpTestCase()
{
  LOG_INFO("SetUpTestCase");
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  init_default_lock_test_value();
}

void TestLockMemtable::TearDownTestCase()
{
  LOG_INFO("TearDownTestCase");
  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestLockMemtable, lock)
{
  LOG_INFO("TestLockMemtable::lock");
  // 1. IN TRANS LOCK
  // 2. OUT TRANS LOCK
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  ObLockParam param;
  ObMemtableCtx *mem_ctx = NULL;
  bool lock_exist = false;
  ObOBJLock *obj_lock = NULL;
  share::SCN min_commited_scn;
  share::SCN flushed_scn;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};

  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;
  ObStoreCtx unlock_store_ctx;
  min_commited_scn.set_min();
  flushed_scn.set_min();
  // 1.1 get store ctx
  LOG_INFO("TestLockMemtable::lock 1.1");
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);
  default_ctx.tx_ctx_.change_to_leader();
  // 1.2 lock
  LOG_INFO("TestLockMemtable::lock 1.2");
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = memtable_.lock(param,
                       store_ctx,
                       DEFAULT_IN_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.3 check lock exist at memctx.
  LOG_INFO("TestLockMemtable::lock 1.3");
  mem_ctx = store_ctx.mvcc_acc_ctx_.mem_ctx_;
  ret = mem_ctx->check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, true);
  // 1.4 check lock at lock map
  LOG_INFO("TestLockMemtable::lock 1.4");
  ret = memtable_.obj_lock_map_.get_obj_lock_with_ref_(DEFAULT_TABLET_LOCK_ID,
                                                       obj_lock);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(obj_lock->row_exclusive_, 1);
  // 1.5 remove lock op at memtable.
  LOG_INFO("TestLockMemtable::lock 1.5");
  memtable_.remove_lock_record(DEFAULT_IN_TRANS_LOCK_OP);
  // 1.6 check again
  LOG_INFO("TestLockMemtable::lock 1.6");
  ret = mem_ctx->check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, true);
  // 1.7 remove lock op at memctx.
  LOG_INFO("TestLockMemtable::lock 1.7");
  mem_ctx->lock_mem_ctx_.remove_lock_record(DEFAULT_IN_TRANS_LOCK_OP);
  ret = mem_ctx->check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, false);

  // 2.1 lock
  LOG_INFO("TestLockMemtable::lock 2.1");
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = memtable_.lock(param,
                       store_ctx,
                       DEFAULT_OUT_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 check lock exist at memctx.
  LOG_INFO("TestLockMemtable::lock 2.2");
  mem_ctx = store_ctx.mvcc_acc_ctx_.mem_ctx_;
  ret = mem_ctx->check_lock_exist(DEFAULT_OUT_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, true);
  // 2.3 unlock not complete lock
  LOG_INFO("TestLockMemtable::lock 2.3");
  MyTxCtx ctx2;
  start_tx(TRANS_ID2, ctx2);
  get_store_ctx(ctx2, unlock_store_ctx);
  ctx2.tx_ctx_.change_to_leader();
  ret = memtable_.unlock(unlock_store_ctx,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_COMPLETED, ret);
  // 2.4 commit out trans lock
  LOG_INFO("TestLockMemtable::lock 2.4");
  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_base();
  commit_scn.set_base();
  ret = memtable_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = memtable_.obj_lock_map_.get_min_ddl_committed_scn(
                            flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  memtable_.obj_lock_map_.print();
  // 2.5 unlock complete lock
  LOG_INFO("TestLockMemtable::lock 2.5");
  ret = memtable_.unlock(unlock_store_ctx,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_SUCCESS, ret);
  memtable_.obj_lock_map_.print();
  // 2.6 unlock commit
  LOG_INFO("TestLockMemtable::lock 2.6");
  ret = memtable_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  memtable_.obj_lock_map_.print();
  min_commited_scn = memtable_.obj_lock_map_.get_min_ddl_committed_scn(
                            flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 2.7 check unlock with the same trans: should be succeed.
  LOG_INFO("TestLockMemtable::lock 2.7");
  ret = memtable_.unlock(unlock_store_ctx,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2.8 check with another trans: should be OB_OBJ_LOCK_NOT_EXIST
  LOG_INFO("TestLockMemtable::lock 2.8");
  MyTxCtx ctx3;
  ObStoreCtx unlock_store_ctx3;
  start_tx(TRANS_ID3, ctx3);
  get_store_ctx(ctx3, unlock_store_ctx3);
  ctx3.tx_ctx_.change_to_leader();
  ret = memtable_.unlock(unlock_store_ctx3,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
}


TEST_F(TestLockMemtable, replay)
{
  LOG_INFO("TestLockMemtable::replay");
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  share::SCN min_commited_scn;
  share::SCN flushed_scn;
  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;
  ObStoreCtx unlock_store_ctx;
  ObMemtableCtx *mem_ctx = nullptr;
  bool lock_exist = false;
  ObOBJLock *obj_lock = NULL;
  min_commited_scn.set_min();
  flushed_scn.set_min();

  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);
  default_ctx.tx_ctx_.change_to_leader();
  store_ctx.mvcc_acc_ctx_.type_ = ObMvccAccessCtx::T::REPLAY;
  mem_ctx = store_ctx.mvcc_acc_ctx_.mem_ctx_;
  // 1. REPLAY ROW
  // 1.1 replay row
  LOG_INFO("TestLockMemtable::replay 1.1");
  ObStoreRowkey rowkey;
  uint64_t table_id = 0;
  uint64_t table_version = 1;
  ObMutatorTableLock table_lock(table_id,
                                rowkey,
                                table_version,
                                DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                DEFAULT_IN_TRANS_LOCK_OP.lock_seq_no_,
                                DEFAULT_IN_TRANS_LOCK_OP.create_timestamp_,
                                DEFAULT_IN_TRANS_LOCK_OP.create_schema_version_);
  ObMemtableMutatorIterator mmi;
  mmi.table_lock_ = table_lock;
  share::SCN log_scn = share::SCN::base_scn();
  ret = memtable_.replay_row(store_ctx,
                             log_scn,
                             &mmi);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check exist at memctx
  LOG_INFO("TestLockMemtable::replay 1.2");
  ret = mem_ctx->check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, true);
  // 1.3 check lock at lock map
  LOG_INFO("TestLockMemtable::replay 1.3");
  ret = memtable_.obj_lock_map_.get_obj_lock_with_ref_(DEFAULT_TABLET_LOCK_ID,
                                                       obj_lock);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(obj_lock->row_exclusive_, 1);
  // 1.4 remove lock op at memtable.
  LOG_INFO("TestLockMemtable::replay 1.4");
  memtable_.remove_lock_record(DEFAULT_IN_TRANS_LOCK_OP);
  // 1.5 check again
  LOG_INFO("TestLockMemtable::replay 1.5");
  ret = mem_ctx->check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, true);
  // 1.6 remove lock op at memctx.
  LOG_INFO("TestLockMemtable::replay 1.6");
  mem_ctx->lock_mem_ctx_.remove_lock_record(DEFAULT_IN_TRANS_LOCK_OP);
  ret = mem_ctx->check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, false);
  // 2. REPLAY LOCK
  // 2.1 replay lock
  share::SCN scn;
  scn.set_base();
  LOG_INFO("TestLockMemtable::replay 2.1");
  ret = memtable_.replay_lock(mem_ctx,
                              DEFAULT_OUT_TRANS_LOCK_OP,
                              scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("TestLockMemtable::replay 2.2");
  ret = mem_ctx->check_lock_exist(DEFAULT_OUT_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, true);
  // 2.3 unlock not complete lock
  LOG_INFO("TestLockMemtable::replay 2.3");
  MyTxCtx ctx2;
  start_tx(TRANS_ID2, ctx2);
  get_store_ctx(ctx2, unlock_store_ctx);
  ctx2.tx_ctx_.change_to_leader();
  ret = memtable_.unlock(unlock_store_ctx,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_COMPLETED, ret);
  // 2.4 commit out trans lock
  LOG_INFO("TestLockMemtable::replay 2.4");
  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_base();
  commit_scn.set_base();
  ret = memtable_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = memtable_.obj_lock_map_.get_min_ddl_committed_scn(
                            flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  memtable_.obj_lock_map_.print();
  // 2.5 unlock complete lock
  LOG_INFO("TestLockMemtable::replay 2.5");
  ret = memtable_.unlock(unlock_store_ctx,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_SUCCESS, ret);
  memtable_.obj_lock_map_.print();
  // 2.6 unlock commit
  LOG_INFO("TestLockMemtable::replay 2.6");
  ret = memtable_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  memtable_.obj_lock_map_.print();
  min_commited_scn = memtable_.obj_lock_map_.get_min_ddl_committed_scn(
                            flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
  // 2.7 check unlock
  LOG_INFO("TestLockMemtable::replay 2.7");
  ret = memtable_.unlock(unlock_store_ctx,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.8 check with another trans: should be OB_OBJ_LOCK_NOT_EXIST
  LOG_INFO("TestLockMemtable::replay 2.8");
  MyTxCtx ctx3;
  ObStoreCtx unlock_store_ctx3;
  start_tx(TRANS_ID3, ctx3);
  get_store_ctx(ctx3, unlock_store_ctx3);
  ctx3.tx_ctx_.change_to_leader();
  ret = memtable_.unlock(unlock_store_ctx3,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
}

TEST_F(TestLockMemtable, recover)
{
  LOG_INFO("TestLockMemtable::recover");
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  share::SCN min_commited_scn;
  share::SCN flushed_scn;
  ObOBJLock *obj_lock = NULL;
  min_commited_scn.set_min();
  flushed_scn.set_min();
  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_base();
  commit_scn.set_base();
  MyTxCtx ctx2;
  ObStoreCtx unlock_store_ctx;
  // 1. recover in trans lock
  // 1.1 recover
  LOG_INFO("TestLockMemtable::recover 1.1");
  ret = memtable_.recover_obj_lock(DEFAULT_IN_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check exist
  LOG_INFO("TestLockMemtable::recover 1.2");
  ret = memtable_.obj_lock_map_.get_obj_lock_with_ref_(DEFAULT_TABLET_LOCK_ID,
                                                       obj_lock);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(obj_lock->row_exclusive_, 1);
  // 1.3 commit
  LOG_INFO("TestLockMemtable::recover 1.3");
  memtable_.remove_lock_record(DEFAULT_IN_TRANS_LOCK_OP);
  // 1.4 check exist
  // We move obj lock garbage collect process to gc thread
  // which is backend, so you can still see obj lock here.
  // See ObOBJLockGarbageCollector for details.
  //
  // LOG_INFO("TestLockMemtable::recover 1.4");
  // ret = memtable_.obj_lock_map_.get_obj_lock_with_ref_(DEFAULT_TABLET_LOCK_ID,
  //                                                      obj_lock);
  // ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);

  // 2. recover out trans lock
  // 2.1 recover
  LOG_INFO("TestLockMemtable::recover 2.1");
  ret = memtable_.recover_obj_lock(DEFAULT_OUT_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 commit
  LOG_INFO("TestLockMemtable::recover 2.2");
  ret = memtable_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.3 unlock
  LOG_INFO("TestLockMemtable::recover 2.3");
  start_tx(TRANS_ID2, ctx2);
  get_store_ctx(ctx2, unlock_store_ctx);
  ctx2.tx_ctx_.change_to_leader();
  ret = memtable_.unlock(unlock_store_ctx,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.4 unlock commit
  LOG_INFO("TestLockMemtable::recover 2.4");
  ret = memtable_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.5 check with unlock with the same trans: should be succeed.
  LOG_INFO("TestLockMemtable::recover 2.5");
  ret = memtable_.unlock(unlock_store_ctx,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.6 check with another trans: should be OB_OBJ_LOCK_NOT_EXIST
  LOG_INFO("TestLockMemtable::recover 2.6");
  MyTxCtx ctx3;
  ObStoreCtx unlock_store_ctx3;
  start_tx(TRANS_ID3, ctx3);
  get_store_ctx(ctx3, unlock_store_ctx3);
  ctx3.tx_ctx_.change_to_leader();
  ret = memtable_.unlock(unlock_store_ctx3,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
}

TEST_F(TestLockMemtable, pre_check_lock)
{
  LOG_INFO("TestLockMemtable::pre_check_lock");
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  bool lock_exist = false;
  int64_t expired_time = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;
  ObOBJLock *obj_lock = NULL;
  ObMemtableCtx *mem_ctx = NULL;
  ObTxIDSet conflict_tx_set;
  ObLockParam param;
  // 1.1 get store ctx
  LOG_INFO("TestLockMemtable::pre_check_lock 1.1");
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);
  default_ctx.tx_ctx_.change_to_leader();
  mem_ctx = store_ctx.mvcc_acc_ctx_.mem_ctx_;
  // 1.2 check before lock
  LOG_INFO("TestLockMemtable::pre_check_lock 1.2");
  ret = memtable_.check_lock_conflict(mem_ctx,
                                      DEFAULT_IN_TRANS_LOCK_OP,
                                      conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.3 lock
  LOG_INFO("TestLockMemtable::pre_check_lock 1.3");
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = memtable_.lock(param,
                       store_ctx,
                       DEFAULT_IN_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.4 pre_check_lock exist return OB_SUCCESS
  LOG_INFO("TestLockMemtable::pre_check_lock 1.4");
  ret = memtable_.check_lock_conflict(mem_ctx,
                                      DEFAULT_IN_TRANS_LOCK_OP,
                                      conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.5 check allow lock
  LOG_INFO("TestLockMemtable::pre_check_lock 1.5");
  ObTableLockOp lock_op = DEFAULT_IN_TRANS_LOCK_OP;
  lock_op.lock_mode_ = ROW_SHARE;
  ret = memtable_.check_lock_conflict(mem_ctx,
                                      lock_op,
                                      conflict_tx_set);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.6 remove lock op at memtable.
  LOG_INFO("TestLockMemtable::pre_check_lock 1.6");
  memtable_.remove_lock_record(DEFAULT_IN_TRANS_LOCK_OP);
  // 1.7 check again
  LOG_INFO("TestLockMemtable::pre_check_lock 1.7");
  ret = mem_ctx->check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, true);
  // 1.8 remove lock op at memctx.
  LOG_INFO("TestLockMemtable::pre_check_lock 1.8");
  mem_ctx->lock_mem_ctx_.remove_lock_record(DEFAULT_IN_TRANS_LOCK_OP);
  ret = mem_ctx->check_lock_exist(DEFAULT_IN_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_IN_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_IN_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, false);
}

TEST_F(TestLockMemtable, lock_twice_out)
{
  LOG_INFO("TestLockMemtable::lock_twice_out");
  // 1. LOCK TWICE: OUT DOING CONFLICT
  // 2. LOCK TWICE: OUT COMPLETE SUCCESS
  // 3. LOCK TWICE: LOCK, UNLOCK DOING CONFLICT
  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  bool is_exact = true;
  ObTxIDSet conflict_tx_set;
  int64_t conflict_modes = 0;
  ObLockParam param;

  ObTableLockOp lock_first = DEFAULT_OUT_TRANS_LOCK_OP;
  ObTableLockOp lock_second = DEFAULT_OUT_TRANS_LOCK_OP;

  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;
  MyTxCtx ctx2;
  ObStoreCtx store_ctx2;

  // 1.1 lock first
  LOG_INFO("TestLockMemtable::lock_twice_out 1.1");
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);
  default_ctx.tx_ctx_.change_to_leader();
  start_tx(TRANS_ID2, ctx2);
  get_store_ctx(ctx2, store_ctx2);
  ctx2.tx_ctx_.change_to_leader();
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = memtable_.lock(param,
                       store_ctx,
                       lock_first);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 lock second
  LOG_INFO("TestLockMemtable::lock_twice_out 1.2");
  ret = memtable_.lock(param,
                       store_ctx2,
                       lock_second);
  ASSERT_EQ(OB_ERR_EXCLUSIVE_LOCK_CONFLICT, ret);

  // 2.1 update to complete
  LOG_INFO("TestLockMemtable::lock_twice_out 2.1");
  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_base();
  commit_scn.set_base();
  ret = memtable_.update_lock_status(lock_first,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 lock again
  LOG_INFO("TestLockMemtable::lock_twice_out 2.2");
  ret = memtable_.lock(param,
                       store_ctx2,
                       lock_second);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3.1 unlock doing
  LOG_INFO("TestLockMemtable::lock_twice_out 3.1");
  MyTxCtx ctx3;
  ObStoreCtx unlock_store_ctx;
  start_tx(TRANS_ID3, ctx3);
  get_store_ctx(ctx3, unlock_store_ctx);
  ctx3.tx_ctx_.change_to_leader();
  ret = memtable_.unlock(unlock_store_ctx,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 3.2 lock again conflict
  LOG_INFO("TestLockMemtable::lock_twice_out 3.2");
  ret = memtable_.lock(param,
                       store_ctx2,
                       lock_second);
  ASSERT_EQ(OB_ERR_EXCLUSIVE_LOCK_CONFLICT, ret);
  // clean: unlock complete.
  LOG_INFO("TestLockMemtable::lock_twice_out clean");
  share::SCN min_commited_scn;
  share::SCN flushed_scn;
  min_commited_scn.set_min();
  flushed_scn.set_min();
  ret = memtable_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = memtable_.obj_lock_map_.get_min_ddl_committed_scn(
      flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
}

TEST_F(TestLockMemtable, out_trans_multi_source)
{
  LOG_INFO("TestLockMemtable::out_trans_multi_source");
  // 1. NOT REGISTER(replay)
  // 2. REGISTER (not replay)
  // 3. NOTIFY (do nothing: REGISTER_SUCC/ON_REDO/ON_PREPARE/ON_COMMIT/ON_ABORT)
  // 4. NOTIFY (replay lock: TX_END)
  // 5. NOTIFY SUCCESS (replay unlock: TX_END before lock committed)
  // 6. UNLOCK COMMITTED BEFORE LOCK COMMITTED
  // 7. UNLOCK COMMITTED AFTER LOCK COMMITTED
  int ret = OB_SUCCESS;
  ObTxBufferNodeArray mds_array;
  ObTxBufferNodeArray mds_array_unlock;
  bool is_replay = false;
  bool lock_exist = false;
  bool is_commit = false;
  share::SCN scn = share::SCN::min_scn();
  share::SCN commit_version = share::SCN::min_scn();
  share::SCN commit_scn = share::SCN::min_scn();
  ObMemtableCtx *mem_ctx = NULL;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};
  ObTableLockOp lock_op = DEFAULT_OUT_TRANS_LOCK_OP;
  ObTableLockOp unlock_op = DEFAULT_OUT_TRANS_UNLOCK_OP;
  unlock_op.create_trans_id_ = TRANS_ID2;
  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;
  MyTxCtx ctx2;
  ObStoreCtx store_ctx2;
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  start_tx(TRANS_ID2, ctx2);
  get_store_ctx(default_ctx, store_ctx);
  get_store_ctx(ctx2, store_ctx2);
  default_ctx.tx_ctx_.change_to_leader();
  ctx2.tx_ctx_.change_to_leader();
  // 1.1 replay register
  LOG_INFO("TestLockMemtable::out_trans_multi_source 1.1");
  is_replay = true;
  mem_ctx = store_ctx.mvcc_acc_ctx_.mem_ctx_;
  // should not call register:
  // ret = mem_ctx->register_multi_source_data_if_need_(lock_op);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check exist at memctx
  LOG_INFO("TestLockMemtable::out_trans_multi_source 1.2");
  ret = mem_ctx->check_lock_exist(lock_op.lock_id_,
                                  lock_op.owner_id_,
                                  lock_op.lock_mode_,
                                  lock_op.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);
  // 1.3 check exist at multi source
  LOG_INFO("TestLockMemtable::out_trans_multi_source 1.3");
  mds_array.reset();
  ret = default_ctx.tx_ctx_.gen_total_mds_array_(mds_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(mds_array.count(), 0);
  // 2.1 register (not replay)
  LOG_INFO("TestLockMemtable::out_trans_multi_source 2.1");
  ret = mem_ctx->register_multi_source_data_if_need_(lock_op);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 check exist at memctx
  LOG_INFO("TestLockMemtable::out_trans_multi_source 2.2");
  ret = mem_ctx->check_lock_exist(lock_op.lock_id_,
                                  lock_op.owner_id_,
                                  lock_op.lock_mode_,
                                  lock_op.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);
  // 2.3 check exist at multi source
  LOG_INFO("TestLockMemtable::out_trans_multi_source 2.3");
  mds_array.reset();
  ret = default_ctx.tx_ctx_.gen_total_mds_array_(mds_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(mds_array.count(), 1);
  // 3.1 notify REGISTER_SUCC/ON_REDO/ON_PREPARE
  LOG_INFO("TestLockMemtable::out_trans_multi_source 3.1");
  is_replay = false;
  ret = default_ctx.tx_ctx_.notify_data_source_(NotifyType::REGISTER_SUCC, scn, is_replay, mds_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = default_ctx.tx_ctx_.notify_data_source_(NotifyType::ON_REDO, scn, is_replay, mds_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = default_ctx.tx_ctx_.notify_data_source_(NotifyType::ON_PREPARE, scn, is_replay, mds_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx->check_lock_exist(lock_op.lock_id_,
                                  lock_op.owner_id_,
                                  lock_op.lock_mode_,
                                  lock_op.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);
  // 3.2 notify ON_COMMIT/ON_ABORT
  LOG_INFO("TestLockMemtable::out_trans_multi_source 3.2");
  ret = default_ctx.tx_ctx_.notify_data_source_(NotifyType::ON_COMMIT, scn, is_replay, mds_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx->check_lock_exist(lock_op.lock_id_,
                                  lock_op.owner_id_,
                                  lock_op.lock_mode_,
                                  lock_op.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);
  ret = default_ctx.tx_ctx_.notify_data_source_(NotifyType::ON_ABORT, scn, is_replay, mds_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx->check_lock_exist(lock_op.lock_id_,
                                  lock_op.owner_id_,
                                  lock_op.lock_mode_,
                                  lock_op.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, false);

  // 4.1 notify TX_END
  LOG_INFO("TestLockMemtable::out_trans_multi_source 4.1");
  is_replay = true;
  scn = share::SCN::base_scn();
  ret = default_ctx.tx_ctx_.notify_data_source_(NotifyType::TX_END, scn, is_replay, mds_array);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = mem_ctx->check_lock_exist(lock_op.lock_id_,
                                  lock_op.owner_id_,
                                  lock_op.lock_mode_,
                                  lock_op.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(lock_exist, true);

  // 5 NOTIFY UNLOCK
  // 5.1 register unlock op
  LOG_INFO("TestLockMemtable::out_trans_multi_source 5.1");
  mem_ctx = store_ctx2.mvcc_acc_ctx_.mem_ctx_;
  ret = mem_ctx->register_multi_source_data_if_need_(unlock_op);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ctx2.tx_ctx_.gen_total_mds_array_(mds_array_unlock);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(mds_array_unlock.count(), 1);
  // 5.2 replay unlock op
  LOG_INFO("TestLockMemtable::out_trans_multi_source 5.2");
  scn = share::SCN::plus(share::SCN::min_scn(), 2);
  is_replay = true;
  ret = ctx2.tx_ctx_.notify_data_source_(NotifyType::TX_END, scn , is_replay, mds_array_unlock);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 6.1 commit unlock op before lock committed
  LOG_INFO("TestLockMemtable::out_trans_multi_source 6.1");
  is_commit = true;
  commit_version = share::SCN::plus(share::SCN::min_scn(), 2);
  commit_scn = share::SCN::plus(share::SCN::min_scn(), 2);
  mem_ctx = store_ctx2.mvcc_acc_ctx_.mem_ctx_;
  ret = mem_ctx->clear_table_lock_(is_commit,
                                   commit_version,
                                   commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 6.2 check lock op exist(not committed lock op exist)
  commit_version.set_base();
  commit_scn.set_base();
  ret = memtable_.update_lock_status(lock_op,
                                     commit_version,
                                     commit_scn,
                                     DEFAULT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 7 UNLOCK COMMITTED AFTER LOCK COMMITTED
  // 7.1 commi lock op
  LOG_INFO("TestLockMemtable::out_trans_multi_source 7.1");
  is_commit = true;
  commit_version.set_base();
  commit_scn.set_base();
  mem_ctx = store_ctx.mvcc_acc_ctx_.mem_ctx_;
  ret = mem_ctx->clear_table_lock_(is_commit,
                                   commit_version,
                                   commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 7.2 check lock op exist
  LOG_INFO("TestLockMemtable::out_trans_multi_source 7.2");
  commit_version.set_base();
  commit_scn.set_base();
  ret = memtable_.update_lock_status(lock_op,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 7.3 commit unlock op after lock committed
  LOG_INFO("TestLockMemtable::out_trans_multi_source 7.3");
  scn = share::SCN::plus(share::SCN::min_scn(), 2);
  is_replay = true;
  ret = ctx2.tx_ctx_.notify_data_source_(NotifyType::TX_END, scn, is_replay, mds_array_unlock);
  ASSERT_EQ(OB_SUCCESS, ret);
  is_commit = true;
  commit_version = share::SCN::plus(share::SCN::min_scn(), 2);
  commit_scn = share::SCN::plus(share::SCN::min_scn(), 2);
  mem_ctx = store_ctx2.mvcc_acc_ctx_.mem_ctx_;
  ret = mem_ctx->clear_table_lock_(is_commit,
                                   commit_version,
                                   commit_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 7.4 check lock op exist
  LOG_INFO("TestLockMemtable::out_trans_multi_source 7.4");
  commit_version.set_base();
  commit_scn.set_base();
  ret = memtable_.update_lock_status(lock_op,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  // 7.5 check unlock op exist
  LOG_INFO("TestLockMemtable::out_trans_multi_source 7.5");
  commit_version = share::SCN::plus(share::SCN::min_scn(), 2);
  commit_scn = share::SCN::plus(share::SCN::min_scn(), 2);
  ret = memtable_.update_lock_status(unlock_op,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);

  // 8 COMPACT
  // This case is invalid after ObOBJLockGarbageCollector
  // starts using. In short, when switch to leader, gc thread
  // will try to execute a force compact, to avoid this case.
  // We don't need to compact residual unlock op anymore.
  // 8.1 create an unlock op and committed it.
  // LOG_INFO("TestLockMemtable::out_trans_multi_source 8.1");
  // scn = share::SCN::plus(share::SCN::min_scn(), 2);
  // is_replay = true;
  // ret = ctx2.tx_ctx_.notify_data_source_(NotifyType::TX_END, scn, is_replay, mds_array_unlock);
  // ASSERT_EQ(OB_SUCCESS, ret);
  // memtable_.obj_lock_map_.print();
  // 8.2 commit the unlock op
  // LOG_INFO("TestLockMemtable::out_trans_multi_source 8.2");
  // is_commit = true;
  // commit_version = share::SCN::plus(share::SCN::min_scn(), 2);
  // commit_scn = share::SCN::plus(share::SCN::min_scn(), 2);
  // mem_ctx = store_ctx2.mvcc_acc_ctx_.mem_ctx_;
  // ret = mem_ctx->clear_table_lock_(is_commit,
  //                                  commit_version,
  //                                  commit_scn);
  // ASSERT_EQ(OB_SUCCESS, ret);
  // memtable_.obj_lock_map_.print();
  // 8.3 compact unlock op if lock conflict occur.
  // LOG_INFO("TestLockMemtable::out_trans_multi_source 8.3");
  // ObTableLockOp conflict_lock_op = lock_op;
  // conflict_lock_op.lock_mode_ = DEFAULT_COFLICT_LOCK_MODE;
  // ObLockParam param;
  // bool is_try_lock = true;
  // int64_t expired_time = 0;
  // param.is_try_lock_ = is_try_lock;
  // param.expired_time_ = expired_time;
  // ret = memtable_.lock(param,
  //                      store_ctx,
  //                      conflict_lock_op);
  // ASSERT_EQ(OB_SUCCESS, ret);
  // 8.4 check unlock op exist
  // LOG_INFO("TestLockMemtable::out_trans_multi_source 8.4");
  // commit_version = share::SCN::plus(share::SCN::min_scn(), 2);
  // commit_scn = share::SCN::plus(share::SCN::min_scn(), 2);
  // ret = memtable_.update_lock_status(unlock_op,
  //                                    commit_version,
  //                                    commit_scn,
  //                                    COMMIT_LOCK_OP_STATUS);
  // ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
}

TEST_F(TestLockMemtable, test_lock_retry)
{
  LOG_INFO("TestLockMemtable::test_lock_retry");
  // 1. LOCK PART CTX
  // 2. DO OBJ LOCK
  // 3. CHECK:
  //    the lock at map;
  //    the lock at lock mem ctx;
  //    the lock at part ctx;
  // 4. UNLOCK PART CTX
  // 5. DO OBJ LOCK AGAIN
  // 6. CHECK

  // ctx.lock_.lock()
  // CtxLockGuard guard(lock_, need_lock);

  int ret = OB_SUCCESS;
  bool is_try_lock = true;
  int64_t expired_time = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  ObLockParam param;
  ObOBJLock *obj_lock = NULL;
  ObMemtableCtx *mem_ctx = NULL;
  bool lock_exist = false;
  share::SCN min_commited_scn;
  share::SCN flushed_scn;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};

  ObTableLockOp lock_op = DEFAULT_OUT_TRANS_LOCK_OP;
  min_commited_scn.set_min();
  flushed_scn.set_min();

  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;
  ObStoreCtx unlock_store_ctx;
  // 1. lock part ctx.
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);
  default_ctx.tx_ctx_.change_to_leader();
  ASSERT_EQ(OB_SUCCESS, default_ctx.tx_ctx_.lock_.lock());

  // 2. do obj lock
  LOG_INFO("TestLockMemtable::test_lock_retry 2 do obj lock");
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = memtable_.lock(param,
                       store_ctx,
                       lock_op);
  ASSERT_EQ(OB_TIMEOUT, ret);

  // 3. check
  // 3.1 check lock at map
  LOG_INFO("TestLockMemtable::test_lock_retry 3.1 check lock at map");
  ret = memtable_.obj_lock_map_.get_obj_lock_with_ref_(DEFAULT_TABLE_LOCK_ID,
                                                       obj_lock);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, obj_lock->size_without_lock());

  // 3.2 check lock at mem ctx
  LOG_INFO("TestLockMemtable::test_lock_retry 3.2 check lock at mem ctx");
  mem_ctx = store_ctx.mvcc_acc_ctx_.mem_ctx_;
  ret = mem_ctx->check_lock_exist(DEFAULT_OUT_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, false);

  // 3.3 check lock at part ctx
  LOG_INFO("TestLockMemtable::test_lock_retry 3.3 check lock at part ctx");
  ASSERT_EQ(default_ctx.tx_ctx_.mds_cache_.mds_list_.empty(), true);

  // 4. unlock part ctx
  default_ctx.tx_ctx_.lock_.unlock();

  // 5. do obj lock again.
  LOG_INFO("TestLockMemtable::test_lock_retry 5 do lock again");
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  ret = memtable_.lock(param,
                       store_ctx,
                       lock_op);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 6. check
  // 6.1 check lock at map
  LOG_INFO("TestLockMemtable::test_lock_retry 6.1 check lock at map");
  ret = memtable_.obj_lock_map_.get_obj_lock_with_ref_(DEFAULT_TABLE_LOCK_ID,
                                                       obj_lock);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, obj_lock->size_without_lock());
  obj_lock->print();

  // 6.2 check lock at mem ctx
  LOG_INFO("TestLockMemtable::test_lock_retry 6.2 check lock at mem ctx");
  mem_ctx = store_ctx.mvcc_acc_ctx_.mem_ctx_;
  ret = mem_ctx->check_lock_exist(DEFAULT_OUT_TRANS_LOCK_OP.lock_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.owner_id_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.lock_mode_,
                                  DEFAULT_OUT_TRANS_LOCK_OP.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, true);

  // 6.3 check lock at part ctx
  LOG_INFO("TestLockMemtable::test_lock_retry 6.3 check lock at part ctx");
  ASSERT_EQ(default_ctx.tx_ctx_.mds_cache_.mds_list_.empty(), false);

  // 7. clean
  // 7.1 commit out trans lock
  LOG_INFO("TestLockMemtable::test_lock_retry 7.1");
  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_base();
  commit_scn.set_base();
  ret = memtable_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = memtable_.obj_lock_map_.get_min_ddl_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  memtable_.obj_lock_map_.print();

  // 7.2 unlock complete lock
  MyTxCtx ctx2;
  start_tx(TRANS_ID2, ctx2);
  get_store_ctx(ctx2, unlock_store_ctx);
  ctx2.tx_ctx_.change_to_leader();
  LOG_INFO("TestLockMemtable::test_lock_retry 7.2");
  expired_time = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  ret = memtable_.unlock(unlock_store_ctx,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_SUCCESS, ret);
  memtable_.obj_lock_map_.print();
  // 7.3 unlock commit
  LOG_INFO("TestLockMemtable::test_lock_retry 7.3");
  ret = memtable_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  memtable_.obj_lock_map_.print();
  min_commited_scn = memtable_.obj_lock_map_.get_min_ddl_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());

  // 7.4 check
  LOG_INFO("TestLockMemtable::test_lock_retry 7.4");
  MyTxCtx ctx3;
  ObStoreCtx unlock_store_ctx3;
  start_tx(TRANS_ID3, ctx3);
  get_store_ctx(ctx3, unlock_store_ctx3);
  ctx3.tx_ctx_.change_to_leader();
  expired_time = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  ret = memtable_.unlock(unlock_store_ctx3,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
}

TEST_F(TestLockMemtable, test_lock_retry_lock_conflict)
{
  LOG_INFO("TestLockMemtable::test_lock_retry_lock_conflict");
  // 1. LOCK
  // 2. LOCK AGAIN
  // 3. UNLOCK
  // 4. LOCK AGAIN
  // 5. CHECK:
  //    the lock at map;
  //    the lock at lock mem ctx;
  //    the lock at part ctx;
  // 6. CLEAN

  int ret = OB_SUCCESS;
  bool is_try_lock = false;
  int64_t expired_time = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  ObLockParam param;
  ObOBJLock *obj_lock = NULL;
  ObMemtableCtx *mem_ctx = NULL;
  bool lock_exist = false;
  share::SCN min_commited_scn;
  share::SCN flushed_scn;
  uint64_t lock_mode_cnt_in_same_trans[TABLE_LOCK_MODE_COUNT] = {0, 0, 0, 0, 0};

  ObTableLockOp lock_first = DEFAULT_OUT_TRANS_LOCK_OP; // RX, owner 0
  ObTableLockOp lock_second = DEFAULT_OUT_TRANS_LOCK_OP;
  lock_second.lock_mode_ = DEFAULT_COFLICT_LOCK_MODE; // X
  lock_second.owner_id_ = CONFLICT_OWNER_ID; // owner 1

  MyTxCtx default_ctx;
  ObStoreCtx store_ctx;
  MyTxCtx ctx2;
  ObStoreCtx store_ctx2;
  MyTxCtx ctx3;
  ObStoreCtx unlock_store_ctx;

  // 1. lock first.
  start_tx(DEFAULT_TRANS_ID, default_ctx);
  get_store_ctx(default_ctx, store_ctx);
  default_ctx.tx_ctx_.change_to_leader();
  start_tx(TRANS_ID2, ctx2);
  get_store_ctx(ctx2, store_ctx2);
  ctx2.tx_ctx_.change_to_leader();
  start_tx(TRANS_ID3, ctx3);
  get_store_ctx(ctx3, unlock_store_ctx);
  ctx3.tx_ctx_.change_to_leader();

  LOG_INFO("TestLockMemtable::test_lock_retry_lock_conflict 1 lock first");
  param.is_try_lock_ = is_try_lock;
  param.expired_time_ = expired_time;
  ret = memtable_.lock(param,
                       store_ctx,
                       lock_first);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. lock second, must conflict and retry until timeout
  LOG_INFO("TestLockMemtable::test_lock_retry_lock_conflict 2 lock second");
  ret = memtable_.lock(param,
                       store_ctx2,
                       lock_second);
  ASSERT_EQ(OB_ERR_EXCLUSIVE_LOCK_CONFLICT, ret);

  // 3. unlock
  // 3.1 commit out trans lock
  LOG_INFO("TestLockMemtable::test_lock_retry_lock_conflict 3.1");
  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_base();
  commit_scn.set_base();
  ret = memtable_.update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = memtable_.obj_lock_map_.get_min_ddl_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  memtable_.obj_lock_map_.print();

  // 3.2 unlock complete lock
  LOG_INFO("TestLockMemtable::test_lock_retry_lock_conflict 3.2");
  expired_time = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  ret = memtable_.unlock(unlock_store_ctx,
                         DEFAULT_OUT_TRANS_UNLOCK_OP,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_SUCCESS, ret);
  memtable_.obj_lock_map_.print();
  // 3.3 unlock commit
  LOG_INFO("TestLockMemtable::test_lock_retry_lock_conflict 3.3");
  ret = memtable_.update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  memtable_.obj_lock_map_.print();
  min_commited_scn = memtable_.obj_lock_map_.get_min_ddl_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());

  // 4. lock again
  LOG_INFO("TestLockMemtable::test_lock_retry_lock_conflict 4 lock again");
  param.expired_time_ = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  ret = memtable_.lock(param,
                       store_ctx2,
                       lock_second);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. check
  // 5.1 check lock at map
  LOG_INFO("TestLockMemtable::test_lock_retry_lock_conflict 5.1 check lock at map");
  ret = memtable_.obj_lock_map_.get_obj_lock_with_ref_(DEFAULT_TABLE_LOCK_ID,
                                                       obj_lock);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, obj_lock->size_without_lock());

  // 5.2 check lock at mem ctx
  LOG_INFO("TestLockMemtable::test_lock_retry_lock_conflict 5.2 check lock at mem ctx");
  mem_ctx = store_ctx2.mvcc_acc_ctx_.mem_ctx_;
  ret = mem_ctx->check_lock_exist(lock_second.lock_id_,
                                  lock_second.owner_id_,
                                  lock_second.lock_mode_,
                                  lock_second.op_type_,
                                  lock_exist,
                                  lock_mode_cnt_in_same_trans);
  ASSERT_EQ(lock_exist, true);

  // 6. clean
  // 6.1 commit out trans lock
  LOG_INFO("TestLockMemtable::test_lock_retry_lock_conflict 6.1");
  commit_version.set_base();
  commit_scn.set_base();
  ret = memtable_.update_lock_status(lock_second,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  min_commited_scn = memtable_.obj_lock_map_.get_min_ddl_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, commit_scn);
  memtable_.obj_lock_map_.print();

  // 6.2 unlock complete lock
  LOG_INFO("TestLockMemtable::test_lock_retry_lock_conflict 6.2");
  expired_time = ObClockGenerator::getClock() + 1 * 1000 * 1000;
  ObTableLockOp unlock_op = DEFAULT_OUT_TRANS_UNLOCK_OP;
  unlock_op.owner_id_ = lock_second.owner_id_;
  unlock_op.lock_mode_ = lock_second.lock_mode_;
  ret = memtable_.unlock(unlock_store_ctx,
                         unlock_op,
                         is_try_lock,
                         expired_time);
  ASSERT_EQ(OB_SUCCESS, ret);
  memtable_.obj_lock_map_.print();
  // 6.3 unlock commit
  LOG_INFO("TestLockMemtable::test_lock_retry_lock_conflict 6.3");
  ret = memtable_.update_lock_status(unlock_op,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  memtable_.obj_lock_map_.print();
  min_commited_scn = memtable_.obj_lock_map_.get_min_ddl_committed_scn(flushed_scn);
  ASSERT_EQ(min_commited_scn, share::SCN::max_scn());
}

} // tablelock
} // transaction
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_lock_memtable.log*");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_lock_memtable.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  oceanbase::common::ObLogger::get_logger().set_enable_async_log(false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
