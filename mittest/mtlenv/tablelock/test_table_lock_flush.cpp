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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define protected public
#define private public
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/tablelock/ob_table_lock_common.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "storage/tablelock/ob_table_lock_iterator.h"
#include "storage/tx_table/ob_tx_table_iterator.h"
#include "storage/init_basic_struct.h"
#include "storage/ob_i_table.h"
#include "lib/utility/ob_defer.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;
using namespace oceanbase::palf;

namespace oceanbase
{
using namespace transaction;
namespace storage
{

class TestTableLockFlush : public ::testing::Test
{
public:
  TestTableLockFlush();
  virtual ~TestTableLockFlush() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;
  static void SetUpTestCase();
  static void TearDownTestCase();

private:
  int64_t tenant_id_;
};

class TestFreezer : public ObFreezer
{
public:
  TestFreezer() : max_consequent_callbacked_scn_() {
    max_consequent_callbacked_scn_.set_base();
  }
  virtual int get_max_consequent_callbacked_scn(share::SCN &max_consequent_callbacked_scn) override
  {
    max_consequent_callbacked_scn = max_consequent_callbacked_scn_;
    return OB_SUCCESS;
  }

  share::SCN max_consequent_callbacked_scn_;
};

TestTableLockFlush::TestTableLockFlush()
{}

void TestTableLockFlush::SetUp()
{
  ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  tenant_id_ = MTL_ID();
}
void TestTableLockFlush::TearDown()
{}
void TestTableLockFlush::SetUpTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ObServerCheckpointSlogHandler::get_instance().is_started_ = true;
}

void TestTableLockFlush::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

static void mock_tx_desc(transaction::ObTxDesc &tx)
{
  tx.state_ = transaction::ObTxDesc::State::IDLE;
  tx.tx_id_ = transaction::ObTransID(1001);
  tx.op_sn_ = 1;
}

static void mock_part_ctx(transaction::ObPartTransCtx &part_ctx)
{
  part_ctx.default_init_();
  part_ctx.is_exiting_ = false;
  part_ctx.exec_info_.state_ = transaction::ObTxState::INIT;
  part_ctx.part_trans_action_ = transaction::ObPartTransAction::UNKNOWN;
  part_ctx.role_state_ = transaction::TxCtxRoleState::LEADER;
  part_ctx.last_op_sn_ = 0;
  part_ctx.mt_ctx_.ctx_ = &part_ctx;
  part_ctx.mt_ctx_.ctx_cb_allocator_.init(MTL_ID());
  part_ctx.ls_id_ = 1; // fake ls id
}

static void mock_store_ctx(ObStoreCtx &store_ctx,
                           ObPartTransCtx &part_ctx,
                           ObTxDesc &tx_desc,
                           ObLS *ls)
{
  store_ctx.ls_ = ls;
  store_ctx.ls_id_ = ls->get_ls_id();
  ObTxTableGuard tx_guard;
  tx_guard.tx_table_ = (ObTxTable*)0x01;
  ObTxSnapshot snapshot;
  concurrent_control::ObWriteFlag write_flag;
  snapshot.version_ = share::SCN::base_scn();
  store_ctx.mvcc_acc_ctx_.init_write(part_ctx,
                                     part_ctx.mt_ctx_,
                                     tx_desc.tx_id_,
                                     ObTxSEQ(19999, 0),
                                     tx_desc,
                                     tx_guard,
                                     snapshot,
                                     50000,
                                     10000,
                                     write_flag);
}

TEST_F(TestTableLockFlush, checkpoint)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObCreateLSArg arg;
  ObLockParam param;
  ObLSHandle handle;
  ObLSID ls_id(110);
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id_, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->create_ls(arg));
  EXPECT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);

  ObStoreCtx ob_store_ctx;
  transaction::ObTxDesc txDesc;
  mock_tx_desc(txDesc);
  transaction::ObPartTransCtx part_ctx;
  mock_part_ctx(part_ctx);
  mock_store_ctx(ob_store_ctx, part_ctx, txDesc, ls);
  auto &mem_ctx = part_ctx.mt_ctx_;
  // table_lock_op1
  transaction::tablelock::ObTableLockOp table_lock_op1;
  ObLockID lock_id1;
  lock_id1.obj_type_ = ObLockOBJType::OBJ_TYPE_TABLE;
  lock_id1.obj_id_ = 1000;
  table_lock_op1.lock_id_ = lock_id1;
  table_lock_op1.lock_mode_ = SHARE_ROW_EXCLUSIVE;
  table_lock_op1.create_trans_id_ = 2;
  table_lock_op1.op_type_ = OUT_TRANS_LOCK;
  table_lock_op1.lock_op_status_ = LOCK_OP_DOING;
  share::SCN table_lock_op1_commit_version = share::SCN::plus(share::SCN::min_scn(), 10);
  share::SCN table_lock_op1_commit_scn = share::SCN::plus(share::SCN::min_scn(), 10);

  ObTabletID tablet_id(LS_LOCK_TABLET);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ObLockMemtableMgr memtable_mgr;
  TestFreezer freezer;
  freezer.max_consequent_callbacked_scn_ = share::SCN::plus(share::SCN::min_scn(), 11);
  freezer.ls_tx_svr_ = ls->get_tx_svr();
  ASSERT_EQ(OB_SUCCESS, memtable_mgr.init(tablet_id, ls->get_ls_id(), &freezer, t3m));
  ASSERT_EQ(OB_SUCCESS, memtable_mgr.create_memtable(SCN::base_scn(), 1));
  ASSERT_EQ(1, memtable_mgr.get_memtable_count_());
  ObTableHandleV2 handle1;
  ASSERT_EQ(OB_SUCCESS, ls->lock_table_.get_lock_memtable(handle1));
  ObLockMemtable *memtable = nullptr;
  EXPECT_EQ(OB_SUCCESS, handle1.get_lock_memtable(memtable));
  mem_ctx.set_trans_ctx(&part_ctx);
  mem_ctx.lock_mem_ctx_.memtable_handle_ = handle1;
  memtable->freezer_ = &freezer;

  ASSERT_EQ(share::SCN::max_scn(), memtable->get_rec_scn());
  ASSERT_EQ(OB_SUCCESS, memtable->lock(param, ob_store_ctx, table_lock_op1));
  ASSERT_EQ(OB_SUCCESS, memtable->update_lock_status(table_lock_op1,
                                                     table_lock_op1_commit_version,
                                                     table_lock_op1_commit_scn,
                                                     LOCK_OP_COMPLETE));
  ASSERT_EQ(table_lock_op1_commit_scn, memtable->get_rec_scn());
  ASSERT_EQ(OB_SUCCESS, memtable->flush(share::SCN::plus(share::SCN::min_scn(), 12)));
  ASSERT_EQ(OB_SUCCESS, memtable->on_memtable_flushed());
  ASSERT_EQ(share::SCN::max_scn(), memtable->get_rec_scn());

  // table_lock_op2
  transaction::tablelock::ObTableLockOp table_lock_op2;
  ObLockID lock_id2;
  lock_id2.obj_type_ = ObLockOBJType::OBJ_TYPE_TABLET;
  lock_id2.obj_id_ = 1001;
  table_lock_op2.lock_id_ = lock_id2;
  table_lock_op2.lock_mode_ = SHARE_ROW_EXCLUSIVE;
  table_lock_op2.create_trans_id_ = 3;
  table_lock_op2.op_type_ = OUT_TRANS_UNLOCK;
  table_lock_op2.lock_op_status_ = LOCK_OP_DOING;
  share::SCN table_lock_op2_commit_version = share::SCN::plus(share::SCN::min_scn(), 12);
  share::SCN table_lock_op2_commit_scn = share::SCN::plus(share::SCN::min_scn(), 12);
  ASSERT_EQ(OB_SUCCESS, memtable->lock(param, ob_store_ctx, table_lock_op2));
  ASSERT_EQ(OB_SUCCESS, memtable->update_lock_status(table_lock_op2,
                                                     table_lock_op2_commit_version,
                                                     table_lock_op2_commit_scn,
                                                     LOCK_OP_COMPLETE));

  ASSERT_EQ(table_lock_op2_commit_scn, memtable->get_rec_scn());
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id));
}

TEST_F(TestTableLockFlush, restore_tablelock_memtable)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  // create ls
  ObCreateLSArg arg;
  ObLockParam param;
  ObLSHandle handle;
  ObLSID ls_id(111);
  LOG_INFO("create_ls");
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id_, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->create_ls(arg));
  EXPECT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  // create table lock memtable

  transaction::ObTxDesc txDesc;
  mock_tx_desc(txDesc);
  transaction::ObPartTransCtx part_ctx;
  mock_part_ctx(part_ctx);
  ObStoreCtx ob_store_ctx;
  mock_store_ctx(ob_store_ctx, part_ctx, txDesc, ls);
  auto &mem_ctx = part_ctx.mt_ctx_;

  LOG_INFO("create_memtable");
  ObTabletID tablet_id(LS_LOCK_TABLET);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr *);
  ObLockMemtableMgr memtable_mgr;
  share::SCN max_consequent_callbacked_scn = share::SCN::plus(share::SCN::min_scn(), 13);
  TestFreezer freezer;
  freezer.max_consequent_callbacked_scn_ = max_consequent_callbacked_scn;
  freezer.ls_tx_svr_ = ls->get_tx_svr();
  ASSERT_EQ(OB_SUCCESS, memtable_mgr.init(tablet_id, ls->get_ls_id(), &freezer, t3m));
  ASSERT_EQ(OB_SUCCESS, memtable_mgr.create_memtable(SCN::base_scn(), 1));
  ASSERT_EQ(1, memtable_mgr.get_memtable_count_());

  ObTableHandleV2 handle1;
  ASSERT_EQ(OB_SUCCESS, ls->lock_table_.get_lock_memtable(handle1));
  ObLockMemtable *memtable = nullptr;
  EXPECT_EQ(OB_SUCCESS, handle1.get_lock_memtable(memtable));
  mem_ctx.set_trans_ctx(&part_ctx);
  mem_ctx.lock_mem_ctx_.memtable_handle_ = handle1;
  memtable->freezer_ = &freezer;

  // table_lock_op1 DDL
  transaction::tablelock::ObTableLockOp table_lock_op1;
  ObLockID lock_id1;
  lock_id1.obj_type_ = ObLockOBJType::OBJ_TYPE_TABLE;
  lock_id1.obj_id_ = 1002;
  table_lock_op1.owner_id_ = 1;
  table_lock_op1.lock_id_ = lock_id1;
  table_lock_op1.lock_mode_ = SHARE_ROW_EXCLUSIVE;
  table_lock_op1.create_trans_id_ = 2;
  table_lock_op1.op_type_ = OUT_TRANS_LOCK;
  table_lock_op1.lock_op_status_ = LOCK_OP_DOING;
  share::SCN table_lock_op1_commit_version = share::SCN::plus(share::SCN::min_scn(), 10);
  share::SCN table_lock_op1_commit_scn = share::SCN::plus(share::SCN::min_scn(), 10);

  // table_lock_op2 DDL
  transaction::tablelock::ObTableLockOp table_lock_op2;
  ObLockID lock_id2;
  lock_id2.obj_type_ = ObLockOBJType::OBJ_TYPE_TABLE;
  lock_id2.obj_id_ = 1002;
  table_lock_op1.owner_id_ = 1;
  table_lock_op2.lock_id_ = lock_id2;
  table_lock_op2.lock_mode_ = ROW_SHARE;
  table_lock_op2.create_trans_id_ = 3;
  table_lock_op2.op_type_ = OUT_TRANS_UNLOCK;
  table_lock_op2.lock_op_status_ = LOCK_OP_DOING;
  share::SCN table_lock_op2_commit_version = share::SCN::plus(share::SCN::min_scn(), 15);
  share::SCN table_lock_op2_commit_scn = share::SCN::plus(share::SCN::min_scn(), 15);

  LOG_INFO("lock");
  ASSERT_EQ(share::SCN::max_scn(), memtable->get_rec_scn());
  ASSERT_EQ(OB_SUCCESS, memtable->lock(param, ob_store_ctx, table_lock_op1));
  ASSERT_EQ(OB_SUCCESS, memtable->update_lock_status(table_lock_op1,
                                                     table_lock_op1_commit_version,
                                                     table_lock_op1_commit_scn,
                                                     LOCK_OP_COMPLETE));
  ASSERT_EQ(table_lock_op1_commit_scn, memtable->get_rec_scn());
  ASSERT_EQ(OB_SUCCESS, memtable->lock(param, ob_store_ctx, table_lock_op2));
  ASSERT_EQ(OB_SUCCESS, memtable->update_lock_status(table_lock_op2,
                                                     table_lock_op2_commit_version,
                                                     table_lock_op2_commit_scn,
                                                     LOCK_OP_COMPLETE));
  ASSERT_EQ(table_lock_op1_commit_scn, memtable->get_rec_scn());

  LOG_INFO("memtable flush");
  // add flush task and confirm freeze_scn
  ASSERT_EQ(OB_SUCCESS, memtable->flush(share::SCN::plus(share::SCN::min_scn(), 12)));
  // get table lock store info
  ObSEArray<ObTableLockOp, ObTableLockScanIterator::TABLE_LOCK_ARRAY_LENGTH> table_lock_store_info;
  EXPECT_EQ(OB_SUCCESS, memtable->get_table_lock_store_info(table_lock_store_info));
  ASSERT_EQ(1, table_lock_store_info.count());
  ASSERT_EQ(OB_SUCCESS, memtable->on_memtable_flushed());
  ASSERT_EQ(max_consequent_callbacked_scn, memtable->get_rec_scn());
  table_lock_store_info.reset();
  max_consequent_callbacked_scn = share::SCN::plus(share::SCN::min_scn(), 16);
  freezer.max_consequent_callbacked_scn_ = max_consequent_callbacked_scn;

  LOG_INFO("memtable flush again");
  ASSERT_EQ(OB_EAGAIN, memtable->flush(share::SCN::max_scn()));
  EXPECT_EQ(OB_SUCCESS, memtable->get_table_lock_store_info(table_lock_store_info));
  // only lock op can be dumped
  ASSERT_EQ(1, table_lock_store_info.count());
  ASSERT_EQ(OB_SUCCESS, memtable->on_memtable_flushed());
  ASSERT_EQ(share::SCN::max_scn(), memtable->get_rec_scn());

  // memtable reset to del the lock
  memtable->obj_lock_map_.reset();
  memtable->obj_lock_map_.init();
  freezer.max_consequent_callbacked_scn_ = max_consequent_callbacked_scn;

  LOG_INFO("recover obj lock");
  ASSERT_EQ(OB_SUCCESS, memtable->recover_obj_lock(table_lock_store_info[0]));
  memtable->set_flushed_scn(freezer.max_consequent_callbacked_scn_);
  ASSERT_EQ(share::SCN::max_scn(), memtable->get_rec_scn());

  LOG_INFO("remove ls");
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id));
}

TEST_F(TestTableLockFlush, restore_tx_ctx)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  // create ls
  ObLockParam param;
  ObCreateLSArg arg;
  ObLSHandle handle;
  ObLSID ls_id(113);
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id_, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->create_ls(arg));
  EXPECT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);

  // construct ObPartTransCtx
  transaction::ObTxDesc txDesc;
  mock_tx_desc(txDesc);
  transaction::ObPartTransCtx ctx1;
  mock_part_ctx(ctx1);
  ObStoreCtx ob_store_ctx;
  mock_store_ctx(ob_store_ctx, ctx1, txDesc, ls);
  ob_store_ctx.ls_ = ls;
  ob_store_ctx.ls_id_ = ls->get_ls_id();
  ob_store_ctx.mvcc_acc_ctx_.tx_table_guards_.tx_table_guard_.tx_table_ = (ObTxTable*)0x1;
  ob_store_ctx.mvcc_acc_ctx_.tx_table_guards_.tx_table_guard_.epoch_ = ObTxTable::INVALID_READ_EPOCH;
  ob_store_ctx.mvcc_acc_ctx_.abs_lock_timeout_ = 5000;
  ob_store_ctx.mvcc_acc_ctx_.tx_ctx_ = &ctx1;
  ob_store_ctx.mvcc_acc_ctx_.tx_desc_ = &txDesc;
  ob_store_ctx.mvcc_acc_ctx_.mem_ctx_ = &ctx1.mt_ctx_;
  auto id1 = ctx1.trans_id_ = txDesc.tx_id_;
  ObTableHandleV2 handle1;
  ASSERT_EQ(OB_SUCCESS, ls->lock_table_.get_lock_memtable(handle1));
  ObLockMemtable *memtable = nullptr;
  EXPECT_EQ(OB_SUCCESS, handle1.get_lock_memtable(memtable));
  ctx1.mt_ctx_.lock_mem_ctx_.memtable_handle_ = handle1;
  ctx1.trans_id_ = id1;
  ctx1.exec_info_.max_applying_log_ts_.set_base();
  ctx1.is_inited_ = true;

  // table_lock_op1 DML
  transaction::tablelock::ObTableLockOp table_lock_op1;
  ObLockID lock_id1;
  lock_id1.obj_type_ = ObLockOBJType::OBJ_TYPE_TABLE;
  lock_id1.obj_id_ = 1005;
  table_lock_op1.lock_id_ = lock_id1;
  table_lock_op1.lock_mode_ = SHARE_ROW_EXCLUSIVE;
  table_lock_op1.create_trans_id_ = 2;
  table_lock_op1.op_type_ = IN_TRANS_DML_LOCK;
  table_lock_op1.lock_op_status_ = LOCK_OP_DOING;
  table_lock_op1.commit_version_ = share::SCN::plus(share::SCN::min_scn(), 10);
  // add lock
  ASSERT_EQ(OB_SUCCESS, memtable->lock(param, ob_store_ctx, table_lock_op1));
  ctx1.mt_ctx_.lock_mem_ctx_.lock_list_.get_first()->set_logged();

  ObLSTxCtxMgr *ls_tx_ctx_mgr = ls->get_tx_svr()->mgr_;
  LOG_INFO("before insert into tx map", K(ls_tx_ctx_mgr->ls_tx_ctx_map_.count()));
  EXPECT_EQ(OB_SUCCESS, ls_tx_ctx_mgr->ls_tx_ctx_map_.insert_and_get(id1, &ctx1, NULL));
  DEFER(ls_tx_ctx_mgr->ls_tx_ctx_map_.revert(&ctx1));
  LOG_INFO("after insert into tx map", K(ctx1), K(ls_tx_ctx_mgr->ls_tx_ctx_map_.count()));

  // get lock_table store info in tx_ctx_table
  ObPartTransCtx* tx_ctx = nullptr;
  ObTxCtxTableInfo ctx_info;
  transaction::ObLSTxCtxIterator ls_tx_ctx_iter_;
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_iter_.set_ready(ls_tx_ctx_mgr));
  ASSERT_EQ(OB_SUCCESS, ls_tx_ctx_iter_.get_next_tx_ctx(tx_ctx));
  tx_ctx->exec_info_.max_applying_log_ts_ = 1;
  ASSERT_EQ(OB_SUCCESS, tx_ctx->get_tx_ctx_table_info(ctx_info));
  ASSERT_EQ(1, ctx_info.table_lock_info_.table_lock_ops_.count());
  ASSERT_EQ(share::SCN::plus(share::SCN::min_scn(), 10),
            ctx_info.table_lock_info_.table_lock_ops_[0].commit_version_);
  ASSERT_EQ(OB_SUCCESS, ctx1.mt_ctx_.recover_from_table_lock_durable_info(
                          ctx_info.table_lock_info_));
  // check lock_mem_ctx
  ASSERT_EQ(share::SCN::plus(share::SCN::min_scn(), 10),
            ctx1.mt_ctx_.lock_mem_ctx_.lock_list_.get_first()->lock_op_.commit_version_);
  // check lock_memtable
  ObOBJLock *obj_lock = NULL;
  ASSERT_EQ(OB_SUCCESS, memtable->obj_lock_map_.lock_map_.get(lock_id1, obj_lock));
  ASSERT_EQ(share::SCN::plus(share::SCN::min_scn(), 10),
            obj_lock->map_[3]->get_first()->lock_op_.commit_version_);
  DEFER(ls_tx_ctx_mgr->ls_tx_ctx_map_.del(id1, &ctx1));
  ctx1.is_inited_ = false;
  LOG_INFO("after delete from tx map", KP(tx_ctx), KPC(tx_ctx), KP(&ctx1), K(ctx1), K(ls_tx_ctx_mgr->ls_tx_ctx_map_.count()));
  ls_tx_ctx_mgr->ls_tx_ctx_map_.reset();

  handle.reset();
  LOG_INFO("restore_tx_ctx remove_ls");
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id));
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_table_lock_flush.log*");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_table_lock_flush.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
