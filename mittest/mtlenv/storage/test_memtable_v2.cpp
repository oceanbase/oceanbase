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
#define private public
#define protected public
#include "storage/memtable/ob_memtable.h"
#include "share/rc/ob_tenant_base.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/tx/ob_mock_tx_ctx.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#include "storage/init_basic_struct.h"
#include "share/ob_master_key_getter.h"

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
int ObMvccWriteGuard::write_auth(storage::ObStoreCtx &) { return OB_SUCCESS; }

ObMvccWriteGuard::~ObMvccWriteGuard() {}

void *ObMemtableCtx::alloc_mvcc_row_callback()
{
  void* ret = NULL;
  if (OB_ISNULL(ret = std::malloc(sizeof(ObMvccRowCallback)))) {
    TRANS_LOG_RET(ERROR, OB_ALLOCATE_MEMORY_FAILED, "callback alloc error, no memory", K(*this));
  } else {
    ATOMIC_FAA(&callback_mem_used_, sizeof(ObMvccRowCallback));
    ATOMIC_INC(&callback_alloc_count_);
  }
  return ret;
}

void ObMemtableCtx::free_mvcc_row_callback(ObITransCallback *cb)
{
  if (OB_ISNULL(cb)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "cb is null, unexpected error", KP(cb), K(*this));
  } else {
    ATOMIC_INC(&callback_free_count_);
    std::free(cb);
    cb = NULL;
  }
}

int ObMvccRow::check_double_insert_(const share::SCN ,
                                    ObMvccTransNode &,
                                    ObMvccTransNode *)
{
  return OB_SUCCESS;
}

// int ObTxEndFunctor::operator()(ObITransCallback *callback)
// {
//   int ret = OB_SUCCESS;

//   if (NULL == callback) {
//     ret = OB_ERR_UNEXPECTED;
//     TRANS_LOG(ERROR, "unexpected callback", KP(callback));
//   } else if (is_commit_
//              && OB_FAIL(callback->trans_commit())) {
//     TRANS_LOG(ERROR, "trans commit failed", KPC(callback));
//   } else if (!is_commit_
//              && OB_FAIL(callback->trans_abort())) {
//     TRANS_LOG(ERROR, "trans abort failed", KPC(callback));
//   } else {
//     need_remove_callback_ = true;
//   }

//   return ret;
// }

} // namespace memtable

namespace unittest
{
int64_t TENANT_ID = 1;
int64_t LSID = 1001;

class TestMemtableV2 : public ::testing::Test
{
public:
  TestMemtableV2()
    : ls_id_(LSID),
      tablet_id_(50001),
      tenant_id_(TENANT_ID),
      rowkey_cnt_(1),
      value_cnt_(1),
      encrypt_index_(500006), /*table_id*/
      iter_param_(),
      columns_(),
      allocator_(),
      allocator2_(),
      read_info_(),
      trans_version_range_(),
      query_flag_(),
      encrypt_meta_(NULL)
    {
      columns_.reset();
      read_info_.reset();
      query_flag_.use_row_cache_ = ObQueryFlag::DoNotUseCache;
      query_flag_.set_not_use_fuse_row_cache();
    }

private:
  static ObTxTable tx_table_;
  static bool is_sstable_contains_lock_;
public:
  virtual void SetUp() override
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
    // mock sequence no
    ObClockGenerator::init();
    // mock tx table
    ObTxPalfParam palf_param((logservice::ObLogHandler *)(0x01), (ObDupTableLSHandler *)(0x02));
    EXPECT_EQ(OB_SUCCESS,
              ls_tx_ctx_mgr_.init(tenant_id_, /*tenant_id*/
                                  ls_id_,
                                  &tx_table_,
                                  (ObLockTable*)(0x01),
                                  (ObITsMgr *)(0x01),
                                  (ObTransService *)(0x01),
                                  &palf_param,
                                  nullptr));
    EXPECT_EQ(OB_SUCCESS, tx_table_.tx_ctx_table_.init(ls_id_));
    tx_table_.online();
    tx_table_.is_inited_ = true;
    tx_table_.ls_ = &ls_;

    // mock columns
    EXPECT_EQ(OB_SUCCESS, mock_col_desc());

    // mock iterator parameter
    EXPECT_EQ(OB_SUCCESS, mock_iter_param());

    // mock trans version range
    EXPECT_EQ(OB_SUCCESS, mock_trans_version_range());

    // is_sstable_contain_lock
    is_sstable_contains_lock_ = false;

    // mock master key getter
    ObMasterKeyGetter::instance().init(NULL);

    const testing::TestInfo* const test_info =
      testing::UnitTest::GetInstance()->current_test_info();
    const char * test_name = test_info->name();
    _TRANS_LOG(INFO, ">>> setup success : %s", test_name);
  }

  virtual void TearDown() override
  {
    // reset iterator parameter
    reset_iter_param();
    // reset columns
    columns_.reset();
    // reset tx table
    ls_tx_ctx_mgr_.reset();
    ls_tx_ctx_mgr_.ls_tx_ctx_map_.reset();
    // reset sequence no
    ObClockGenerator::destroy();
    // reset trans version range
    trans_version_range_.reset();
    // reset allocator
    allocator_.reset();
    allocator2_.reset();

    ObMasterKeyGetter::instance().stop();
    TRANS_LOG(INFO, "teardown success");
  }

  static void SetUpTestCase()
  {
    TRANS_LOG(INFO, "SetUpTestCase");
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    ObServerCheckpointSlogHandler::get_instance().is_started_ = true;

    // create ls
    ObCreateLSArg arg;
    EXPECT_EQ(OB_SUCCESS, gen_create_ls_arg(TENANT_ID,
                                            ObLSID(LSID),
                                            arg));
    ObLSService* ls_svr = MTL(ObLSService*);
    EXPECT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  }
  static void TearDownTestCase()
  {
    // remove ls
    ObLSID ls_id(1001);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id));

    MockTenantModuleEnv::get_instance().destroy();
    TRANS_LOG(INFO, "TearDownTestCase");
  }
public:
  ObMemtable *create_memtable()
  {
    int ret = OB_SUCCESS;
    ObITable::TableKey table_key;
    table_key.table_type_ = ObITable::DATA_MEMTABLE;
    table_key.tablet_id_ = ObTabletID(tablet_id_.id());
    table_key.scn_range_.start_scn_.convert_for_tx(1);
    table_key.scn_range_.end_scn_.set_max();
    ObLSService* ls_svr = MTL(ObLSService*);
    ObLSHandle ls_handle;
    EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id_,
                                         ls_handle,
                                         ObLSGetMod::STORAGE_MOD));

    ObMemtable *memtable = new ObMemtable();
    ObFreezer *freezer = new ObFreezer;
    freezer->ls_ = ls_handle.get_ls();
    ObTabletMemtableMgr *memtable_mgr = new ObTabletMemtableMgr;
    int64_t schema_version  = 1;
    uint32_t freeze_clock = 0;

    EXPECT_EQ(OB_SUCCESS, memtable->init(table_key,
                                         ls_handle,
                                         freezer,
                                         memtable_mgr,
                                         schema_version,
                                         freeze_clock));

    return memtable;
  }

  void rollback_to_txn(ObStoreCtx *store_ctx,
                       const ObTxSEQ from,
                       const ObTxSEQ to)
  {
    ObUndoAction undo(from, to);
    ObPartTransCtx *tx_ctx = store_ctx->mvcc_acc_ctx_.tx_ctx_;
    ObTxDataGuard tx_data_guard;
    EXPECT_EQ(OB_SUCCESS, tx_ctx->ls_tx_ctx_mgr_->get_tx_table()->alloc_tx_data(tx_data_guard));
    ObUndoStatusNode *undo_node = NULL;
    EXPECT_EQ(OB_SUCCESS, tx_ctx->insert_undo_action_to_tx_table_(undo, tx_data_guard, undo_node, SCN::min_scn()));
    ObMemtableCtx *mt_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
    ObTxCallbackList &cb_list = mt_ctx->trans_mgr_.callback_list_;
    for (ObMvccRowCallback *iter = (ObMvccRowCallback *)(cb_list.get_guard()->get_next());
         iter != (ObMvccRowCallback *)(cb_list.get_guard());
         iter = (ObMvccRowCallback *)(iter->get_next())) {
      if (iter->seq_no_ > to) {
        iter->tnode_->set_delayed_cleanout(true);
      }
    }
  }

  void fast_commit_txn(ObStoreCtx *store_ctx)
  {
    ObMemtableCtx *mt_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
    ObTxCallbackList &cb_list = mt_ctx->trans_mgr_.callback_list_;
    ObMvccRowCallback *next = NULL;
    for (ObMvccRowCallback *iter = (ObMvccRowCallback *)(cb_list.get_guard()->get_next());
         iter != (ObMvccRowCallback *)(cb_list.get_guard()); iter = next) {
      next = (ObMvccRowCallback *)(iter->get_next());
      iter->tnode_->set_delayed_cleanout(true);
      iter->remove();
    }
  }

  int flush_txn_log(ObStoreCtx *store_ctx, const int64_t scn, const int cnt, ObCallbackScope &scope)
  {
    ObMemtableCtx *mt_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
    ObTxCallbackList &cb_list = mt_ctx->trans_mgr_.callback_list_;
    ObMvccRowCallback *next = NULL;
    ObIMemtable* mt;
    share::SCN scn_x;
    int ret = scn_x.convert_for_tx(scn);
    for (ObMvccRowCallback *iter = (ObMvccRowCallback *)(cb_list.get_log_cursor());
         iter != (ObMvccRowCallback *)(cb_list.get_guard()); iter = next) {
      next = (ObMvccRowCallback *)(iter->get_next());
      iter->log_submitted_cb(scn_x, mt);
      if (!*scope.start_) { scope.start_ = iter; }
      scope.end_ = iter;
      scope.host_ = &cb_list;
      scope.cnt_ +=1;
      if (scope.cnt_ == cnt) {
        break;
      }
    }
    cb_list.submit_log_succ(scope);
    return ret;
  }

  int sync_txn_log_fail(ObStoreCtx *store_ctx,
                        ObCallbackScope &scope,
                        const int64_t palf_applied_scn,
                        int64_t &removed_cnt)
  {
    share::SCN scn_x;
    int ret = OB_SUCCESS;
    if (OB_SUCC(scn_x.convert_for_tx(palf_applied_scn))) {
      ObMemtableCtx *mt_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
      ObTxCallbackList &cb_list = mt_ctx->trans_mgr_.callback_list_;
      ret = cb_list.sync_log_fail(scope, scn_x, removed_cnt);
    }
    return ret;
  }

  void prepare_txn(ObStoreCtx *store_ctx,
                   const int64_t prepare_version)
  {
    share::SCN prepare_scn;
    prepare_scn.convert_for_tx(prepare_version);
    ObPartTransCtx *tx_ctx = store_ctx->mvcc_acc_ctx_.tx_ctx_;
    ObMemtableCtx *mt_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
    tx_ctx->exec_info_.state_ = ObTxState::PREPARE;
    tx_ctx->exec_info_.prepare_version_ = prepare_scn;
    mt_ctx->trans_version_ = prepare_scn;
  }

  void commit_txn(ObStoreCtx *store_ctx,
                  const int64_t commit_version,
                  const bool need_write_back = false)
  {
    share::SCN commit_scn;
    commit_scn.convert_for_tx(commit_version);
    ObIMemtable *mt;
    ObPartTransCtx *tx_ctx = store_ctx->mvcc_acc_ctx_.tx_ctx_;
    ObMemtableCtx *mt_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
    tx_ctx->exec_info_.state_ = ObTxState::COMMIT;
    tx_ctx->ctx_tx_data_.set_commit_version(commit_scn);
    tx_ctx->ctx_tx_data_.set_state(ObTxData::COMMIT);
    if (need_write_back) {
      // flush log for all txNode
      {
        ObTxCallbackList &cb_list = mt_ctx->trans_mgr_.callback_list_;
        for (ObMvccRowCallback *iter = (ObMvccRowCallback *)(cb_list.get_guard()->get_next());
             iter != (ObMvccRowCallback *)(cb_list.get_guard());
             iter = (ObMvccRowCallback *)(iter->get_next())) {
          iter->log_submitted_cb(share::SCN::minus(commit_scn, 10), mt);
        }
      }
      EXPECT_EQ(OB_SUCCESS, mt_ctx->trans_end(true, /*commit*/
                                              commit_scn,
                                              commit_scn /*commit log ts*/));
    } else {
      ObTxCallbackList &cb_list = mt_ctx->trans_mgr_.callback_list_;
      for (ObMvccRowCallback *iter = (ObMvccRowCallback *)(cb_list.get_guard()->get_next());
           iter != (ObMvccRowCallback *)(cb_list.get_guard());
           iter = (ObMvccRowCallback *)(iter->get_next())) {
        iter->tnode_->set_delayed_cleanout(true);
      }
    }
  }

  void abort_txn(ObStoreCtx *store_ctx,
                 const bool need_write_back = false)
  {
    ObPartTransCtx *tx_ctx = store_ctx->mvcc_acc_ctx_.tx_ctx_;
    ObMemtableCtx *mt_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
    tx_ctx->exec_info_.state_ = ObTxState::ABORT;
    tx_ctx->ctx_tx_data_.set_state(ObTxData::ABORT);

    if (need_write_back) {
      EXPECT_EQ(OB_SUCCESS, mt_ctx->trans_end(false, /*commit*/
                                              share::SCN::min_scn()      /*commit version*/,
                                              share::SCN::max_scn()));
    } else {
      ObTxCallbackList &cb_list = mt_ctx->trans_mgr_.callback_list_;
      for (ObMvccRowCallback *iter = (ObMvccRowCallback *)(cb_list.get_guard()->get_next());
           iter != (ObMvccRowCallback *)(cb_list.get_guard());
           iter = (ObMvccRowCallback *)(iter->get_next())) {
        iter->tnode_->set_delayed_cleanout(true);
      }
    }
  }
  ObStoreCtx *start_tx(const ObTransID &tx_id, const bool for_replay = false)
  {
    ObTxDesc *tx_desc = new ObTxDesc();
    tx_desc->state_ = ObTxDesc::State::ACTIVE;
    tx_desc->tx_id_ = tx_id;
    tx_desc->isolation_ = ObTxIsolationLevel::RC; // used by write conflict error resolve
    ObStoreCtx *store_ctx = new ObStoreCtx;
    MockObTxCtx *tx_ctx = new MockObTxCtx;
    ObTxData *tx_data = new ObTxData;
    tx_data->reset();
    tx_data->tx_id_ = tx_id;
    tx_ctx->init(ls_id_,
                 tx_id,
                 &ls_tx_ctx_mgr_,
                 tx_data, // ObTxData
                 NULL);   // mailbox_mgr
    store_ctx->mvcc_acc_ctx_.abs_lock_timeout_ts_ = 0; // nowait
    store_ctx->mvcc_acc_ctx_.tx_desc_ = tx_desc;
    store_ctx->mvcc_acc_ctx_.tx_id_ = tx_id;
    store_ctx->mvcc_acc_ctx_.tx_ctx_ = tx_ctx;
    store_ctx->mvcc_acc_ctx_.mem_ctx_ = &(tx_ctx->mt_ctx_);
    store_ctx->mvcc_acc_ctx_.mem_ctx_->set_trans_ctx(tx_ctx);
    tx_ctx->mt_ctx_.log_gen_.set(&(tx_ctx->mt_ctx_.trans_mgr_),
                                 &(tx_ctx->mt_ctx_));
    store_ctx->mvcc_acc_ctx_.snapshot_.tx_id_ = tx_id;
    store_ctx->mvcc_acc_ctx_.tx_table_guards_.tx_table_guard_.init(&tx_table_);
    if (for_replay) {
      store_ctx->mvcc_acc_ctx_.mem_ctx_->commit_to_replay();
    }

    if (ObTransID(READ_TX_ID) != tx_id) {
      EXPECT_EQ(OB_SUCCESS, ls_tx_ctx_mgr_.ls_tx_ctx_map_.insert_and_get(tx_id, tx_ctx, NULL));
    }

    return store_ctx;
  }

  void start_stmt(ObStoreCtx *store_ctx,
                  const share::SCN snapshot_scn,
                  const int64_t expire_time = 10000000000)
  {
    ObSequence::inc();
    store_ctx->mvcc_acc_ctx_.type_ = ObMvccAccessCtx::T::WRITE;
    store_ctx->mvcc_acc_ctx_.snapshot_.tx_id_ = store_ctx->mvcc_acc_ctx_.tx_id_;
    store_ctx->mvcc_acc_ctx_.snapshot_.version_ = snapshot_scn;
    store_ctx->mvcc_acc_ctx_.snapshot_.scn_ = ObTxSEQ(ObSequence::get_max_seq_no());
    const int64_t abs_expire_time = expire_time + ::oceanbase::common::ObTimeUtility::current_time();
    store_ctx->mvcc_acc_ctx_.abs_lock_timeout_ts_ = abs_expire_time;
    store_ctx->mvcc_acc_ctx_.tx_scn_ = ObTxSEQ(ObSequence::inc_and_get_max_seq_no());
  }
  void start_pdml_stmt(ObStoreCtx *store_ctx,
                       const share::SCN snapshot_scn,
                       const ObTxSEQ read_seq_no,
                       const int64_t expire_time = 10000000000)
  {
    ObSequence::inc();
    store_ctx->mvcc_acc_ctx_.type_ = ObMvccAccessCtx::T::WRITE;
    store_ctx->mvcc_acc_ctx_.snapshot_.tx_id_ = store_ctx->mvcc_acc_ctx_.tx_id_;
    store_ctx->mvcc_acc_ctx_.snapshot_.version_ = snapshot_scn;
    store_ctx->mvcc_acc_ctx_.snapshot_.scn_ = read_seq_no;
    const int64_t abs_expire_time = expire_time + ::oceanbase::common::ObTimeUtility::current_time();
    store_ctx->mvcc_acc_ctx_.abs_lock_timeout_ts_ = abs_expire_time;
    store_ctx->mvcc_acc_ctx_.tx_scn_ = ObTxSEQ(ObSequence::inc_and_get_max_seq_no());
  }
  void print_callback(ObStoreCtx *wtx)
  {
    TRANS_LOG(INFO, "========== START PRINT CALLBACK ===========", K(wtx->mvcc_acc_ctx_.tx_id_));
    wtx->mvcc_acc_ctx_.mem_ctx_->print_callbacks();
    TRANS_LOG(INFO, "=========== END PRINT CALLBACK ============", K(wtx->mvcc_acc_ctx_.tx_id_));
  }

  void write_tx(ObStoreCtx *wtx,
                ObMemtable *memtable,
                const int64_t snapshot,
                const ObStoreRow &write_row,
                const int expect_ret = OB_SUCCESS,
                const int64_t expire_time = 10000000000)
  {
    int ret = OB_SUCCESS;
    TRANS_LOG(INFO, "====================== start write tx =====================",
              K(wtx->mvcc_acc_ctx_.tx_id_), K(*wtx), K(snapshot), K(expire_time), K(write_row));

    share::SCN snapshot_scn;
    snapshot_scn.convert_for_tx(snapshot);
    start_stmt(wtx, snapshot_scn, expire_time);

    ObTableAccessContext context;
    ObVersionRange trans_version_range;
    const bool read_latest = true;
    ObQueryFlag query_flag;

    trans_version_range.base_version_ = 0;
    trans_version_range.multi_version_start_ = 0;
    trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
    query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
    query_flag.read_latest_ = read_latest & ObQueryFlag::OBSF_MASK_READ_LATEST;

    if (OB_FAIL(context.init(query_flag, *wtx, allocator_, trans_version_range))) {
      TRANS_LOG(WARN, "Fail to init access context", K(ret));
    }
    ret = memtable->set(iter_param_, context, columns_, write_row, encrypt_meta_, false);
    if (ret == -5024) {
      TRANS_LOG(ERROR, "nima", K(ret), K(write_row));
    }
    EXPECT_EQ(expect_ret, ret);
    TRANS_LOG(INFO, "======================= end write tx ======================",
              K(ret), K(wtx->mvcc_acc_ctx_.tx_id_), K(*wtx), K(snapshot), K(expire_time), K(write_row));
  }

  void lock_tx(ObStoreCtx *ltx,
               ObMemtable *memtable,
               const int64_t snapshot,
               const ObDatumRowkey &rowkey,
               const int expect_ret = OB_SUCCESS,
               const int64_t expire_time = 10000000000)
   {
     int ret = OB_SUCCESS;
     TRANS_LOG(INFO, "====================== start lock tx =====================",
               K(ltx->mvcc_acc_ctx_.tx_id_), K(*ltx), K(snapshot), K(expire_time), K(rowkey));

     share::SCN snapshot_scn;
     snapshot_scn.convert_for_tx(snapshot);
     start_stmt(ltx, snapshot_scn, expire_time);

     ObTableAccessContext context;
     ObVersionRange trans_version_range;
     const bool read_latest = true;
     ObQueryFlag query_flag;

     trans_version_range.base_version_ = 0;
     trans_version_range.multi_version_start_ = 0;
     trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
     query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
     query_flag.read_latest_ = read_latest & ObQueryFlag::OBSF_MASK_READ_LATEST;

     if (OB_FAIL(context.init(query_flag, *ltx, allocator_, trans_version_range))) {
       TRANS_LOG(WARN, "Fail to init access context", K(ret));
     }

     EXPECT_EQ(expect_ret, (ret = memtable->lock(iter_param_, context, rowkey)));

     TRANS_LOG(INFO, "======================= end lock tx ======================",
               K(ret), K(ltx->mvcc_acc_ctx_.tx_id_), K(*ltx), K(snapshot), K(expire_time), K(rowkey));
   }

  void write_no_value_tx(ObStoreCtx *ltx,
                         ObMemtable *memtable,
                         const int64_t snapshot,
                         const ObDatumRowkey &rowkey,
                         const int expect_ret = OB_SUCCESS,
                         const int64_t expire_time = 10000000000)
    {
      int ret = OB_SUCCESS;
      TRANS_LOG(INFO, "====================== start lock tx =====================",
                K(ltx->mvcc_acc_ctx_.tx_id_), K(*ltx), K(snapshot), K(expire_time), K(rowkey));

      share::SCN snapshot_scn;
      snapshot_scn.convert_for_tx(snapshot);
      start_stmt(ltx, snapshot_scn, expire_time);

      ObTableAccessContext context;
      ObVersionRange trans_version_range;
      const bool read_latest = true;
      ObQueryFlag query_flag;

      trans_version_range.base_version_ = 0;
      trans_version_range.multi_version_start_ = 0;
      trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
      query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
      query_flag.read_latest_ = read_latest & ObQueryFlag::OBSF_MASK_READ_LATEST;

      if (OB_FAIL(context.init(query_flag, *ltx, allocator_, trans_version_range))) {
        TRANS_LOG(WARN, "Fail to init access context", K(ret));
      }

      EXPECT_EQ(expect_ret, (ret = memtable->lock(iter_param_, context, rowkey)));
      ObMvccTransNode *node = get_tx_last_tnode(ltx);
      ((ObMemtableDataHeader *)(node->buf_))->dml_flag_ = blocksstable::ObDmlFlag::DF_INSERT;


      TRANS_LOG(INFO, "mock tnode with no value", KPC(node));
      TRANS_LOG(INFO, "======================= end lock tx ======================",
                K(ret), K(ltx->mvcc_acc_ctx_.tx_id_), K(*ltx), K(snapshot), K(expire_time), K(rowkey));
    }


  bool is_write_set_empty(ObStoreCtx *ctx)
  {
    ObMemtableCtx *mem_ctx = ctx->mvcc_acc_ctx_.mem_ctx_;
    ObTxCallbackList &callback_list = mem_ctx->trans_mgr_.callback_list_;
    return 0 == callback_list.length_;
  }

  ObMvccRowCallback *get_tx_last_cb(ObStoreCtx *store_ctx)
  {
    ObMemtableCtx *mem_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
    ObTxCallbackList &callback_list = mem_ctx->trans_mgr_.callback_list_;
    return (ObMvccRowCallback *)callback_list.get_tail();
  }

  ObMvccRowCallback *get_tx_first_cb(ObStoreCtx *store_ctx)
  {
    ObMemtableCtx *mem_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
    ObTxCallbackList &callback_list = mem_ctx->trans_mgr_.callback_list_;
    return (ObMvccRowCallback *)callback_list.get_guard()->next_;
  }

  ObMvccTransNode *get_tx_last_tnode(ObStoreCtx *store_ctx)
  {
    return get_tx_last_cb(store_ctx)->tnode_;
  }

  ObMvccTransNode *get_tx_first_tnode(ObStoreCtx *store_ctx)
  {
    return get_tx_first_cb(store_ctx)->tnode_;
  }

  ObMvccRow *get_tx_last_mvcc_row(ObStoreCtx *store_ctx)
  {
    return &(get_tx_last_cb(store_ctx)->value_);
  }

  ObMvccRow *get_tx_first_mvcc_row(ObStoreCtx *store_ctx)
  {
    return &(get_tx_first_cb(store_ctx)->value_);
  }

  void read_row(ObMemtable *memtable,
                const ObDatumRowkey &rowkey,
                const int64_t snapshot,
                int64_t k,
                int64_t v,
                const bool exist = true,
                const int expect_ret = OB_SUCCESS,
                const int64_t expire_time = 10000000000)
  {
    ObTransID read_tx_id = ObTransID(READ_TX_ID);
    ObStoreCtx *rtx = start_tx(read_tx_id);

    read_row(rtx,
             memtable,
             rowkey,
             snapshot,
             k,
             v,
             exist,
             expect_ret,
             expire_time);
  }

  void read_row(ObStoreCtx *rtx,
                ObMemtable *memtable,
                const ObDatumRowkey &rowkey,
                const int64_t snapshot,
                int64_t k,
                int64_t v,
                const bool exist = true,
                const int expect_ret = OB_SUCCESS,
                const int64_t expire_time = 10000000000)
  {
    int ret = OB_SUCCESS;
    ObDatumRow read_row;
    ObTableAccessContext access_context;

    TRANS_LOG(INFO, "====================== start read row =====================",
              K(rtx->mvcc_acc_ctx_.tx_id_), K(*rtx), K(snapshot), K(expire_time));
    share::SCN snapshot_scn;
    snapshot_scn.convert_for_tx(snapshot);
    start_stmt(rtx, snapshot_scn, expire_time);

    EXPECT_EQ(OB_SUCCESS, access_context.init(query_flag_,
                                              *rtx,
                                              allocator_,
                                              trans_version_range_));

    EXPECT_EQ(expect_ret, (ret = memtable->get(iter_param_,
                                               access_context,
                                               rowkey,
                                               read_row)));

    if (OB_SUCC(ret)) {
      if (!exist) {
        EXPECT_EQ(true, read_row.row_flag_.is_not_exist());
      } else {
        EXPECT_EQ(true, read_row.row_flag_.is_exist());
        ObStorageDatum *cells = read_row.storage_datums_;
        int64_t count = read_row.count_;
        EXPECT_EQ(rowkey_cnt_ + value_cnt_, count);
        for (int64_t i = 0; i < count; i++) {
          int64_t row_v;
          row_v = cells[i].get_int();
          if (i == 0) {
            EXPECT_EQ(k, row_v);
          } else if (i == 1) {
            EXPECT_EQ(v, row_v);
          } else {
            ob_abort();
          }
        }
      }
    }

    TRANS_LOG(INFO, "read row success", K(ret), K(read_row));
    TRANS_LOG(INFO, "====================== end read row =====================",
              K(rtx->mvcc_acc_ctx_.tx_id_), K(*rtx), K(snapshot), K(expire_time));
  }

  void compact_row(ObMvccRow *row,
                   ObMemtable *memtable,
                   int64_t snapshot_version,
                   const bool for_replay)
  {
    ASSERT_NE(NULL, (long)row);
    TRANS_LOG(INFO, "====================== start compact row =====================",
              K(*row), K(snapshot_version));
    share::SCN snapshot_scn;
    snapshot_scn.convert_for_tx(snapshot_version);
    EXPECT_EQ(OB_SUCCESS, row->row_compact(memtable,
                                           snapshot_scn,
                                           &allocator2_));
    TRANS_LOG(INFO, "====================== end compact row =====================",
              K(*row), K(snapshot_version));
  }

  int mock_col_desc()
  {
    share::schema::ObColDesc col_desc;
    col_desc.col_id_ = OB_APP_MIN_COLUMN_ID;
    col_desc.col_type_.set_type(ObIntType);
    col_desc.col_type_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    columns_.push_back(col_desc);

    share::schema::ObColDesc col_desc2;
    col_desc2.col_id_ = OB_APP_MIN_COLUMN_ID + 1;
    col_desc2.col_type_.set_type(ObIntType);
    col_desc2.col_type_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    columns_.push_back(col_desc2);

    return OB_SUCCESS;
  }

  int mock_row(const int64_t key,
               const int64_t value,
               ObDatumRowkey &rowkey,
               ObStoreRow &row)
  {
    rowkey_datums_[0].set_int(key);
    rowkey_datums_[1].set_int(value);
    rowkey.assign(rowkey_datums_, 1);

    ObObj *obj = new ObObj[2];
    obj[0].set_int(key);
    obj[1].set_int(value);

    row.row_val_.cells_ = obj;
    row.row_val_.count_ = 2;
    row.row_val_.projector_ = NULL;
    row.flag_.set_flag(ObDmlFlag::DF_INSERT);
    rowkey.store_rowkey_.assign(obj, 1);

    return OB_SUCCESS;
  }

  int mock_delete(const int64_t key,
                  ObDatumRowkey &rowkey,
                  ObStoreRow &row)
  {
    rowkey_datums_[0].set_int(key);
    rowkey.assign(rowkey_datums_, 1);

    ObObj *obj = new ObObj[1];
    obj[0].set_int(key);

    row.row_val_.cells_ = obj;
    row.row_val_.count_ = 2;
    row.row_val_.projector_ = NULL;
    row.flag_.set_flag(ObDmlFlag::DF_DELETE);
    rowkey.store_rowkey_.assign(obj, 1);

    return OB_SUCCESS;
  }

  int mock_row(const int64_t key,
               const int64_t value,
               ObStoreRowkey &rowkey,
               ObStoreRow &row)
  {
    ObObj *obj = new ObObj[2];
    obj[0].set_int(key);
    obj[1].set_int(value);

    rowkey.assign(obj, 1);

    row.row_val_.cells_ = obj;
    row.row_val_.count_ = 2;
    row.row_val_.projector_ = NULL;
    row.flag_.set_flag(ObDmlFlag::DF_INSERT);

    return OB_SUCCESS;
  }

  void mock_replay_iterator(ObStoreCtx *store_ctx,
                            ObMemtableMutatorIterator &mmi)
  {
    mmi.reset();
    int64_t serialize_pos = 0;
    int64_t deserialize_pos = 0;
    ObCLogEncryptInfo encrypt_info;
    ObRedoLogSubmitHelper helper;
    ObIMemtableCtx *mem_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
    char *redo_log_buffer = new char[REDO_BUFFER_SIZE];
    encrypt_info.init();
    ObTxFillRedoCtx ctx;
    ctx.buf_ = redo_log_buffer;
    ctx.buf_len_ = REDO_BUFFER_SIZE;
    ctx.buf_pos_ = serialize_pos;
    ctx.helper_ = &helper;
    ctx.fill_count_ = 0;
    EXPECT_EQ(OB_SUCCESS, mem_ctx->fill_redo_log(ctx));

    EXPECT_EQ(OB_SUCCESS, mmi.deserialize(redo_log_buffer,
                                          ctx.buf_pos_,
                                          deserialize_pos,
                                          encrypt_info));
  }

  void serialize_encrypted_redo_log(ObStoreCtx *store_ctx,
                                    char *redo_log_buffer)
  {
    int64_t mutator_size = 0;
    int64_t pos = 0;
    ObRedoLogSubmitHelper helper;
    ObPartTransCtx *tx_ctx = store_ctx->mvcc_acc_ctx_.tx_ctx_;
    ObTxRedoLog redo_log(1000 /*fake cluster_version_*/);

    redo_log.set_mutator_buf(redo_log_buffer);
    redo_log.set_mutator_size(REDO_BUFFER_SIZE, false /*after_fill*/);

    ObIMemtableCtx *mem_ctx = store_ctx->mvcc_acc_ctx_.mem_ctx_;
    ObTxFillRedoCtx ctx;
    ctx.buf_ = redo_log.get_mutator_buf();
    ctx.buf_len_ = redo_log.get_mutator_size();
    ctx.buf_pos_ = mutator_size;
    ctx.helper_ = &helper;
    ctx.fill_count_ = 0;
    EXPECT_EQ(OB_SUCCESS, mem_ctx->fill_redo_log(ctx));

    redo_log.set_mutator_size(ctx.buf_pos_, true /*after_fill*/);
    EXPECT_EQ(OB_SUCCESS, redo_log.serialize(redo_log_buffer, REDO_BUFFER_SIZE, pos));
  }

  void deserialize_redo_log_extract_encryption(char *redo_log_buffer,
                                               ObTxRedoLog &redo_log,
                                               ObMemtableMutatorIterator &mmi,
                                               ObCLogEncryptInfo &encrypt_info)
  {
    int64_t pos = 0;
    EXPECT_EQ(OB_SUCCESS, redo_log.deserialize(redo_log_buffer, REDO_BUFFER_SIZE, pos));

    //mock replay iterator
    //deserialize encrypt info
    mmi.reset();
    pos = 0;
    EXPECT_EQ(OB_SUCCESS, mmi.deserialize(redo_log.get_replay_mutator_buf(),
                                          redo_log.get_mutator_size(),
                                          pos,
                                          encrypt_info));
    EXPECT_EQ(redo_log.get_mutator_size(), pos);

    //decrypt table key
    ObTxEncryptMap *encrypt_map = encrypt_info.encrypt_map_;
    char decrypted_table_key[OB_ENCRYPTED_TABLE_KEY_LEN] = {0};
    int64_t out_len = 0;
    if (OB_NOT_NULL(encrypt_map) && encrypt_map->begin() != encrypt_map->end()) {
      int64_t master_key_len = 0;
      ObEncryptMeta &meta = encrypt_map->begin()->meta_;
      meta.tenant_id_ = 1004;
      EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::get_master_key(meta.tenant_id_,
                                                              meta.master_key_version_,
                                                              meta.master_key_.ptr(),
                                                              OB_MAX_MASTER_KEY_LENGTH,
                                                              master_key_len));
      meta.master_key_.get_content().set_length(master_key_len);
      TRANS_LOG(INFO, "deserialized master key", K(meta.master_key_));
      EXPECT_EQ(OB_SUCCESS, ObBlockCipher::decrypt(meta.master_key_.ptr(),
                                                   meta.master_key_.size(),
                                                   meta.encrypted_table_key_.ptr(),
                                                   meta.encrypted_table_key_.size(),
                                                   OB_ENCRYPTED_TABLE_KEY_LEN,
                                                   NULL, 0, NULL, 0, NULL, 0,
                                                   static_cast<ObCipherOpMode>(meta.encrypt_algorithm_),
                                                   decrypted_table_key,
                                                   out_len));
      meta.table_key_.set_content(decrypted_table_key, out_len);
      TRANS_LOG(INFO, "deserialized table_key", K(meta.table_key_));
      EXPECT_EQ(true, meta.is_valid());
    } else {
      ob_abort();
    }

  }

  void replay_tx(ObStoreCtx *store_ctx,
                 ObMemtable *memtable,
                 const int64_t replay_log_ts,
                 ObMemtableMutatorIterator &mmi)
  {
    int ret = OB_SUCCESS;
    bool can_continue = true;
    ObEncryptRowBuf unused_row_buf;
    transaction::ObCLogEncryptInfo unused_encrypt_info;
    unused_encrypt_info.init();
    while (can_continue) {
      if (OB_FAIL(mmi.iterate_next_row(unused_row_buf, unused_encrypt_info))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(ERROR, "get row head failed", K(ret));
        }
        can_continue = false;
      } else {
        TRANS_LOG(INFO, "TEST_MEMTABLE V2: replay row",
                  K(*store_ctx));
        share::SCN replay_scn;
        replay_scn.convert_for_tx(replay_log_ts);
        EXPECT_EQ(OB_SUCCESS, memtable->replay_row(*store_ctx,
                                                   replay_scn,
                                                   &mmi));
      }
    }
  }

  void replay_tx_with_encryption(ObStoreCtx *store_ctx,
                 ObMemtable *memtable,
                 const int64_t replay_log_ts,
                 ObMemtableMutatorIterator &mmi,
                 const ObCLogEncryptInfo &encrypt_info)
  {
    int ret = OB_SUCCESS;
    bool can_continue = true;
    ObEncryptRowBuf row_buf;
    while (can_continue) {
      if (OB_FAIL(mmi.iterate_next_row(row_buf, encrypt_info))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(ERROR, "get row head failed", K(ret));
        }
        can_continue = false;
      } else {
        TRANS_LOG(INFO, "TEST_MEMTABLE V2: replay row",
                  K(*store_ctx));
        share::SCN replay_scn;
        replay_scn.convert_for_tx(replay_log_ts);
        EXPECT_EQ(OB_SUCCESS, memtable->replay_row(*store_ctx,
                                                   replay_scn,
                                                   &mmi));
      }
    }
  }

  int mock_iter_param()
  {
	int ret = OB_SUCCESS;
    // iter_param_.rowkey_cnt_ = rowkey_cnt_;
    iter_param_.tablet_id_ = tablet_id_;
    iter_param_.table_id_ = tablet_id_.id();
    read_info_.init(allocator_, 16000, rowkey_cnt_, lib::is_oracle_mode(), columns_, nullptr/*storage_cols_index*/);
    iter_param_.read_info_ = &read_info_;

    return ret;
  }

  void reset_iter_param()
  {
    iter_param_.reset();
    read_info_.reset();
  }

  int mock_trans_version_range()
  {
    trans_version_range_.base_version_ = 0;
    trans_version_range_.multi_version_start_ = 0;
    trans_version_range_.snapshot_version_ = INT64_MAX - 2;
    return OB_SUCCESS;
  }

  void verify_cb(ObMvccRowCallback *cb,
                 const ObMemtable *mt,
                 const ObTxSEQ seq_no,
                 const int64_t k,
                 const bool is_link = true,
                 const bool need_fill_redo = true,
                 const int64_t log_ts = INT64_MAX)
  {
    ASSERT_NE(NULL, (long)cb);
    TRANS_LOG(INFO, "=============== VERIFY TRANS CALLBACK START ===============", K(*cb));

    share::SCN scn;
    scn.convert_for_tx(log_ts);
    EXPECT_EQ(mt, cb->memtable_);
    EXPECT_EQ(scn, cb->scn_);
    EXPECT_EQ(is_link, cb->is_link_);
    EXPECT_EQ(need_fill_redo, cb->need_submit_log_);
    EXPECT_EQ(seq_no, cb->seq_no_);
    ObStoreRowkey *rowkey = cb->key_.rowkey_;
    ObObj *key = rowkey->get_obj_ptr();
    int64_t row_k;
    key->get_int(row_k);
    EXPECT_EQ(k, row_k);

    TRANS_LOG(INFO, "=============== VERIFY TRANS CALLBACK END ===============", K(*cb));
  }

  void verify_tnode(const ObMvccTransNode *tnode,
                    const ObMvccTransNode *prev,
                    const ObMvccTransNode *next,
                    const ObMemtable *mt,
                    const ObTransID &tx_id,
                    const int64_t trans_version,
                    const ObTxSEQ seq_no,
                    const uint32_t modify_count,
                    const uint8_t tnode_flag,
                    const ObDmlFlag dml_flag,
                    const int64_t k,
                    const int64_t v,
                    const int64_t log_ts = INT64_MAX,
                    const uint8_t ndt_type = NDT_NORMAL)
  {
    ASSERT_NE(NULL, (long)tnode);
    TRANS_LOG(INFO, "=============== VERIFY TRANS NODE START ===============", K(*tnode), K(log_ts));

    share::SCN scn;
    scn.convert_for_tx(log_ts);
    EXPECT_EQ(tx_id, tnode->tx_id_);
    EXPECT_EQ(trans_version, tnode->trans_version_.get_val_for_tx());
    EXPECT_EQ(scn, tnode->scn_);
    EXPECT_EQ(seq_no, tnode->seq_no_);
    EXPECT_EQ(prev, tnode->prev_);
    EXPECT_EQ(next, tnode->next_);
    EXPECT_EQ(modify_count, tnode->modify_count_);
    // EXPECT_EQ(0, tnode->acc_checksum_);
    EXPECT_EQ(mt->get_timestamp(), tnode->version_);
    EXPECT_EQ(ndt_type, tnode->type_);
    EXPECT_EQ(tnode_flag, tnode->flag_);

    int ret = OB_SUCCESS;
    const ObMemtableDataHeader *mtd = reinterpret_cast<const ObMemtableDataHeader *>(tnode->buf_);
    ObArenaAllocator allocator;
    ObDatumRow datum_row;
    ObRowReader row_reader;
    const blocksstable::ObRowHeader *row_header = nullptr;
    if (OB_FAIL(row_reader.read_row_header(mtd->buf_, mtd->buf_len_, row_header))) {
      CLOG_LOG(WARN, "Failed to read row header", K(ret));
    } else if (OB_FAIL(datum_row.init(allocator, row_header->get_column_count()))) {
      CLOG_LOG(WARN, "Failed to init datum row", K(ret));
    } else if (OB_FAIL(row_reader.read_row(mtd->buf_, mtd->buf_len_, nullptr, datum_row))) {
      CLOG_LOG(WARN, "Failed to read datum row", K(ret));
    } else {
      EXPECT_EQ(dml_flag, mtd->dml_flag_);
      TRANS_LOG(INFO, "TEST_MEMTABLE_V2 row: ", K(*tnode), K(mtd));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_row.get_column_count(); i++) {
      int64_t row_v = datum_row.storage_datums_[i].get_int();
      if (i == 0) {
        EXPECT_EQ(k, row_v);
      } else {
        EXPECT_EQ(v, row_v);
      }
      TRANS_LOG(INFO, "    TEST_MEMTABLE_V2 column: ", K(i), K(datum_row.storage_datums_[i]));
    }

    TRANS_LOG(INFO, "=============== VERIFY TRANS NODE END ===============", K(*tnode));
  }

  void verify_wtx(ObStoreCtx *wtx,
                  ObMemtable *wmt,
                  ObMvccTransNode *prev,
                  ObMvccTransNode *next,
                  ObTxSEQ seq_no,
                  uint32_t modify_count,
                  int64_t k,
                  int64_t v)
  {
    ObMvccRowCallback *cb = get_tx_last_cb(wtx);
    ObMvccTransNode *tnode = cb->tnode_;

    EXPECT_NE(NULL, (long)cb);
    EXPECT_NE(NULL, (long)tnode);

    TRANS_LOG(INFO, "=============== VERIFY TRANS CALLBACK ===============", K(*cb));

    EXPECT_EQ(wmt, cb->memtable_);
    EXPECT_EQ(share::SCN::max_scn(), cb->scn_);
    EXPECT_EQ(true, cb->is_link_);
    EXPECT_EQ(true, cb->need_submit_log_);
    EXPECT_EQ(seq_no, cb->seq_no_);
    ObStoreRowkey *rowkey = cb->key_.rowkey_;
    ObObj *key = rowkey->get_obj_ptr();
    int64_t row_k;
    key->get_int(row_k);
    EXPECT_EQ(k, row_k);

    TRANS_LOG(INFO, "=============== VERIFY TRANS CALLBACK ===============", K(*cb));

    TRANS_LOG(INFO, "=============== VERIFY TRANS NODE START ===============", K(*tnode));

    EXPECT_EQ(wtx->mvcc_acc_ctx_.tx_id_, tnode->tx_id_);
    EXPECT_EQ(share::SCN::max_scn(), tnode->trans_version_);
    EXPECT_EQ(share::SCN::max_scn(), tnode->scn_);
    EXPECT_EQ(seq_no, tnode->seq_no_);
    EXPECT_EQ(prev, tnode->prev_);
    EXPECT_EQ(next, tnode->next_);
    EXPECT_EQ(modify_count, tnode->modify_count_);
    EXPECT_EQ(0, tnode->acc_checksum_);
    EXPECT_EQ(wmt->get_timestamp(), tnode->version_);
    EXPECT_EQ(NDT_NORMAL, tnode->type_);
    EXPECT_EQ(ObMvccTransNode::F_INIT, tnode->flag_);

    int ret = OB_SUCCESS;
    const ObMemtableDataHeader *mtd = reinterpret_cast<const ObMemtableDataHeader *>(tnode->buf_);
    ObArenaAllocator allocator;
    ObDatumRow datum_row;
    ObRowReader row_reader;
    const blocksstable::ObRowHeader *row_header = nullptr;
    if (OB_FAIL(row_reader.read_row_header(mtd->buf_, mtd->buf_len_, row_header))) {
      CLOG_LOG(WARN, "Failed to read row header", K(ret));
    } else if (OB_FAIL(datum_row.init(allocator, row_header->get_column_count()))) {
      CLOG_LOG(WARN, "Failed to init datum row", K(ret));
    } else if (OB_FAIL(row_reader.read_row(mtd->buf_, mtd->buf_len_, nullptr, datum_row))) {
      CLOG_LOG(WARN, "Failed to read datum row", K(ret));
    } else {
      EXPECT_EQ(ObDmlFlag::DF_INSERT, mtd->dml_flag_);
      TRANS_LOG(INFO, "TEST_MEMTABLE_V2 row: ", K(*tnode), K(mtd));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_row.get_column_count(); i++) {
      int64_t row_v = datum_row.storage_datums_[i].get_int();
      if (i == 0) {
        EXPECT_EQ(k, row_v);
      } else {
        EXPECT_EQ(v, row_v);
      }
      TRANS_LOG(INFO, "    TEST_MEMTABLE_V2 column: ", K(i), K(datum_row.storage_datums_[i]));
    }

    TRANS_LOG(INFO, "=============== VERIFY TRANS NODE END ===============", K(*tnode));
  }

  void verify_mvcc_row(ObMvccRow *row,
                       const int8_t first_dml,
                       const int8_t last_dml,
                       const ObMvccTransNode *list_head,
                       const int64_t max_trans_version,
                       /*const int64_t max_elr_trans_version,*/
                       const int64_t total_trans_node_cnt,
                       const uint8_t flag = ObMvccRow::F_HASH_INDEX | ObMvccRow::F_BTREE_INDEX)
  {
    TRANS_LOG(INFO, "=============== VERIFY MVCC ROW START ===============", K(*row));
    EXPECT_EQ(flag, row->flag_);
    EXPECT_EQ(first_dml, row->first_dml_flag_);
    EXPECT_EQ(last_dml, row->last_dml_flag_);
    EXPECT_EQ(list_head, row->list_head_);
    EXPECT_EQ(total_trans_node_cnt, row->total_trans_node_cnt_);
    TRANS_LOG(INFO, "=============== VERIFY MVCC ROW END ===============", K(*row));
  }

  void create_and_store_encrypt_info(ObSerializeEncryptMeta &encrypt_meta)
  {
    //create fake master key
    char cur_master_key[] = "abcdef";
    uint64_t tenant_id = 1004;
    uint64_t master_key_id = 1;
    EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().set_master_key(tenant_id, master_key_id, cur_master_key, strlen(cur_master_key)));

    TRANS_LOG(INFO, "create and set fake master key success", K(cur_master_key));

    //create encrypted_table_key
    char origin_table_key[OB_ORIGINAL_TABLE_KEY_LEN] = {0};
    char encrypt_table_key[OB_ENCRYPTED_TABLE_KEY_LEN] = {0};

    int64_t encrypt_out_len = 0;
    int algorithm = ObCipherOpMode::ob_invalid_mode + 3;
    EXPECT_EQ(OB_SUCCESS, ObKeyGenerator::generate_encrypt_key(origin_table_key, OB_ORIGINAL_TABLE_KEY_LEN));
    EXPECT_EQ(OB_SUCCESS, ObBlockCipher::encrypt(cur_master_key, strlen(cur_master_key),
                                                 origin_table_key, OB_ORIGINAL_TABLE_KEY_LEN,
                                                 OB_ENCRYPTED_TABLE_KEY_LEN, NULL, 0, NULL, 0, 0,
                                                 static_cast<ObCipherOpMode>(algorithm),
                                                 encrypt_table_key, encrypt_out_len, NULL));
    encrypt_table_key[encrypt_out_len] = '\0';
    EXPECT_STRNE(origin_table_key, encrypt_table_key);

    TRANS_LOG(INFO, "create encrypted table key success", K(origin_table_key), K(encrypt_table_key));

    //create encrypt_meta
    char master_key[OB_MAX_MASTER_KEY_LENGTH] = {0};
    int64_t master_key_len = 0;
    EXPECT_EQ(OB_SUCCESS, ObMasterKeyGetter::get_master_key(tenant_id, master_key_id, master_key, OB_MAX_MASTER_KEY_LENGTH, master_key_len));
    EXPECT_STREQ(master_key, cur_master_key);

    encrypt_meta.master_key_.set_content(master_key, master_key_len);
    encrypt_meta.table_key_.set_content(origin_table_key, OB_ORIGINAL_TABLE_KEY_LEN);
    encrypt_meta.encrypted_table_key_.set_content(encrypt_table_key, OB_ENCRYPTED_TABLE_KEY_LEN);
    encrypt_meta.tenant_id_ = tenant_id;
    encrypt_meta.master_key_version_ = master_key_id;
    encrypt_meta.encrypt_algorithm_ = algorithm;

    TRANS_LOG(INFO, "create encrypt_meta success", K(encrypt_meta));

    //store encrypt-info
    encrypt_meta_ = &encrypt_meta;
    TRANS_LOG(INFO, "store encrypt meta success");
  }

private:
  static const int64_t READ_TX_ID = 987654321;
  static const int64_t UNUSED_VALUE = -1;
  static const int64_t REDO_BUFFER_SIZE = 2L * 1024L * 1024L;
private:
  static ObLSTxCtxMgr ls_tx_ctx_mgr_;
  static ObLS ls_;
  const ObLSID ls_id_;
  const ObTabletID tablet_id_;
  const int64_t tenant_id_;
  const int64_t rowkey_cnt_;
  const int64_t value_cnt_;
  uint64_t encrypt_index_;

  ObTableIterParam iter_param_;
  ObSEArray<share::schema::ObColDesc, 2> columns_;
  ObStorageDatum rowkey_datums_[2];
  ObArenaAllocator allocator_;
  ObArenaAllocator allocator2_;
  ObTableReadInfo read_info_;
  ObVersionRange trans_version_range_;
  ObQueryFlag query_flag_;
  char redo_log_buffer_[REDO_BUFFER_SIZE];
  ObSerializeEncryptMeta *encrypt_meta_;
};

ObLSTxCtxMgr TestMemtableV2::ls_tx_ctx_mgr_;
ObTxTable TestMemtableV2::tx_table_;
ObLS TestMemtableV2::ls_;
bool TestMemtableV2::is_sstable_contains_lock_;


TEST_F(TestMemtableV2, test_write_read_conflict)
{
  ObMemtable *memtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: write row into memtable");
  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));

  ObTransID write_tx_id = ObTransID(1);
  ObStoreCtx *wtx = start_tx(write_tx_id);
  write_tx(wtx,
           memtable,
           1000, /*snapshot version*/
           write_row);
  const auto wtx_seq_no = ObTxSEQ(ObSequence::get_max_seq_no());

  verify_cb(get_tx_last_cb(wtx),
            memtable,
            wtx_seq_no,
            1,   /*key*/
            true /*is_link*/);
  verify_tnode(get_tx_last_tnode(wtx),
               NULL,      /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               wtx_seq_no,
               0,         /*modify_count*/
               ObMvccTransNode::F_INIT,
               DF_INSERT,
               1,         /*key*/
               2          /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(wtx),
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  get_tx_last_tnode(wtx),
                  0, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);
  read_row(wtx,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           2     /*value*/);

  TRANS_LOG(INFO, "######## CASE2: read row(running during txn) from memtable with empty result");
  read_row(memtable,
           rowkey,
           1800,   /*snapshot version*/
           1,      /*key*/
           2,      /*value*/
           false   /*exist*/);

  TRANS_LOG(INFO, "######## CASE3: read row(prepare during txn) from memtable with lock for read skipping");
  prepare_txn(wtx, 1500/*prepare_version*/);
  read_row(memtable,
           rowkey,
           1200,   /*snapshot version*/
           1,      /*key*/
           2,      /*value*/
           false   /*exist*/);

  TRANS_LOG(INFO, "######## CASE4: read row(prepare during txn) from memtable with lock for read blocking");
  read_row(memtable,
           rowkey,
           1800,   /*snapshot version*/
           1,      /*key*/
           2,      /*value*/
           false,  /*exist*/
           OB_ERR_SHARED_LOCK_CONFLICT,
           1000000 /*expire_time*/);

  TRANS_LOG(INFO, "######## CASE5: read row(commit during txn) from memtable with lock for read success");
  commit_txn(wtx,
             2000,/*commit_version*/
             false/*need_write_back*/);
  read_row(memtable,
           rowkey,
           3000,   /*snapshot version*/
           1,      /*key*/
           2       /*value*/);

  verify_cb(get_tx_last_cb(wtx),
            memtable,
            wtx_seq_no,
            1,   /*key*/
            true /*is_link*/);
  verify_tnode(get_tx_last_tnode(wtx),
               NULL,      /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               2000, /*trans_version*/
               wtx_seq_no,
               0,         /*modify_count*/
               ObMvccTransNode::F_COMMITTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               2          /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(wtx),
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  get_tx_last_tnode(wtx),
                  2000, /*max_trans_version*/
                  1     /*total_trans_node_cnt*/);

  TRANS_LOG(INFO, "######## CASE6: read row(WRRITTEN during txn) from memtable with lock for read success");
  ObMvccTransNode *tmp_node = get_tx_last_tnode(wtx);
  ObMvccRow *tmp_row = get_tx_last_mvcc_row(wtx);
  commit_txn(wtx,
             2000,/*commit_version*/
             true /*need_write_back*/);
  read_row(memtable,
           rowkey,
           3000,   /*snapshot version*/
           1,      /*key*/
           2       /*value*/);
  verify_tnode(tmp_node,
               NULL,      /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               2000, /*trans_version*/
               wtx_seq_no,
               0,         /*modify_count*/
               ObMvccTransNode::F_COMMITTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               2          /*value*/,
               2000 - 10);
  verify_mvcc_row(tmp_row,
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  tmp_node,
                  2000, /*max_trans_version*/
                  1     /*total_trans_node_cnt*/);
  memtable->destroy();
}

TEST_F(TestMemtableV2, test_tx_abort)
{
  ObMemtable *memtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: write row into memtable");
  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));

  ObTransID write_tx_id = ObTransID(1);
  ObStoreCtx *wtx = start_tx(write_tx_id);
  write_tx(wtx,
           memtable,
           1000, /*snapshot version*/
           write_row);
  const auto wtx_seq_no = ObTxSEQ(ObSequence::get_max_seq_no());

  verify_cb(get_tx_last_cb(wtx),
            memtable,
            wtx_seq_no,
            1,   /*key*/
            true /*is_link*/);
  verify_tnode(get_tx_last_tnode(wtx),
               NULL,      /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               wtx_seq_no,
               0,         /*modify_count*/
               ObMvccTransNode::F_INIT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               2          /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(wtx),
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  get_tx_last_tnode(wtx),
                  0, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);
  read_row(wtx,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           2     /*value*/);

  TRANS_LOG(INFO, "######## CASE2: read row(abort during txn) from memtable with lock for read no data");
  abort_txn(wtx, false/*need_write_back*/);
  read_row(memtable,
           rowkey,
           3000,   /*snapshot version*/
           -1,     /*key*/
           -1,     /*value*/
           false   /*exist*/);
  verify_cb(get_tx_last_cb(wtx),
            memtable,
            wtx_seq_no,
            1,    /*key*/
            true  /*is_link*/);
  verify_tnode(get_tx_last_tnode(wtx),
               NULL,      /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               wtx_seq_no,
               0,         /*modify_count*/
               ObMvccTransNode::F_ABORTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               2          /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(wtx),
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  NULL,
                  0,    /*max_trans_version*/
                  0     /*total_trans_node_cnt*/);

  TRANS_LOG(INFO, "######## CASE3: read row(WRRITTEN during txn) from memtable with lock for read success");
  ObMvccRow *tmp_row = get_tx_last_mvcc_row(wtx);
  ObMvccTransNode *tmp_node = get_tx_last_tnode(wtx);
  abort_txn(wtx, true /*need_write_back*/);
  read_row(memtable,
           rowkey,
           3000,   /*snapshot version*/
           -1,     /*key*/
           -1,     /*value*/
           false   /*exist*/);
  verify_tnode(tmp_node,
               NULL,      /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               wtx_seq_no,
               0,         /*modify_count*/
               ObMvccTransNode::F_ABORTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               2          /*value*/);
  verify_mvcc_row(tmp_row,
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  NULL, /*list_head*/
                  0,    /*max_trans_version*/
                  0     /*total_trans_node_cnt*/);
  memtable->destroy();
}

TEST_F(TestMemtableV2, test_write_write_conflict)
{
  ObMemtable *memtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: txn1 write row into memtable");
  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));

  ObTransID write_tx_id = ObTransID(1);
  ObStoreCtx *wtx = start_tx(write_tx_id);
  write_tx(wtx,
           memtable,
           1000, /*snapshot version*/
           write_row);

  verify_cb(get_tx_last_cb(wtx),
            memtable,
            ObTxSEQ(ObSequence::get_max_seq_no()),
            1,   /*key*/
            true /*is_link*/);
  verify_tnode(get_tx_last_tnode(wtx),
               NULL,      /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               ObTxSEQ(ObSequence::get_max_seq_no()),
               0,         /*modify_count*/
               ObMvccTransNode::F_INIT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               2          /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(wtx),
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  get_tx_last_tnode(wtx),
                  0, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);
  read_row(wtx,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           2     /*value*/);

  TRANS_LOG(INFO, "######## CASE2: txn2 write row into memtable, lock for write failed");
  ObDatumRowkey rowkey2;
  ObStoreRow write_row2;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 3, /*value*/
                                 rowkey2,
                                 write_row2));

  ObTransID write_tx_id2 = ObTransID(2);
  ObStoreCtx *wtx2 = start_tx(write_tx_id2);
  write_tx(wtx2,
           memtable,
           1200, /*snapshot version*/
           write_row2,
           OB_TRY_LOCK_ROW_CONFLICT);
  read_row(wtx,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           2     /*value*/);

  TRANS_LOG(INFO, "######## CASE3: txn1 write row into memtable, lock for write succeed");
  ObDatumRowkey rowkey3;
  ObStoreRow write_row3;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 4, /*value*/
                                 rowkey3,
                                 write_row3));
  ObMvccTransNode *tmp_node = get_tx_last_tnode(wtx);

  write_tx(wtx,
           memtable,
           1200, /*snapshot version*/
           write_row3);
  ObMvccTransNode *wtx_case3_tnode = get_tx_last_tnode(wtx);

  verify_cb(get_tx_last_cb(wtx),
            memtable,
            ObTxSEQ(ObSequence::get_max_seq_no()),
            1,   /*key*/
            true /*is_link*/);
  verify_tnode(get_tx_last_tnode(wtx),
               tmp_node,  /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               ObTxSEQ(ObSequence::get_max_seq_no()),
               1,         /*modify_count*/
               ObMvccTransNode::F_INIT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               4          /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(wtx),
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  get_tx_last_tnode(wtx),
                  0, /*max_trans_version*/
                  2  /*total_trans_node_cnt*/);
  read_row(wtx,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           4     /*value*/);

  TRANS_LOG(INFO, "######## CASE4: txn2 write row(prepare during txn) into memtable, lock for write failed");
  prepare_txn(wtx, 1500/*prepare_version*/);

  write_tx(wtx2,
           memtable,
           1200, /*snapshot version*/
           write_row2,
           OB_TRY_LOCK_ROW_CONFLICT);
  read_row(wtx,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           4     /*value*/);

  TRANS_LOG(INFO, "######## CASE5: txn2 write row(commit during txn) into memtable, lock for write encounters tsc");
  commit_txn(wtx,
             2000,/*commit_version*/
             false/*need_write_back*/);
  write_tx(wtx2,
           memtable,
           1800, /*snapshot version*/
           write_row2,
           OB_TRANSACTION_SET_VIOLATION);
  read_row(wtx,
           memtable,
           rowkey,
           3000, /*snapshot version*/
           1,    /*key*/
           4     /*value*/);

  TRANS_LOG(INFO, "######## CASE6: txn2 write row(commit during txn) into memtable, lock for write succeed");
  write_tx(wtx2,
           memtable,
           2100, /*snapshot version*/
           write_row2);
  const auto wtx2_seq_no = ObTxSEQ(ObSequence::get_max_seq_no());

  verify_cb(get_tx_last_cb(wtx2),
            memtable,
            wtx2_seq_no,
            1,   /*key*/
            true /*is_link*/);
  verify_tnode(get_tx_last_tnode(wtx2),
               get_tx_last_cb(wtx)->tnode_, /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx2->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               wtx2_seq_no,
               2,         /*modify_count*/
               ObMvccTransNode::F_INIT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               3          /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(wtx2),
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  get_tx_last_tnode(wtx2),
                  2000, /*max_trans_version*/
                  3     /*total_trans_node_cnt*/);
  read_row(wtx2,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           3     /*value*/);
  ObMvccTransNode *wtx_last_tnode = get_tx_last_tnode(wtx);
  commit_txn(wtx,
             2000,/*commit_version*/
             true/*need_write_back*/);

  TRANS_LOG(INFO, "######## CASE7: txn2 abort, undo mvcc row");
  abort_txn(wtx2, false/*need_write_back*/);
  read_row(memtable,
           rowkey,
           3000,   /*snapshot version*/
           1,      /*key*/
           4       /*value*/);
  verify_tnode(get_tx_last_tnode(wtx2),
               wtx_last_tnode, /*prev tnode*/
               NULL,           /*next tnode*/
               memtable,
               wtx2->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               wtx2_seq_no,
               2,         /*modify_count*/
               ObMvccTransNode::F_ABORTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               3          /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(wtx2),
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  wtx_case3_tnode, /*list_head*/
                  2000, /*max_trans_version*/
                  2     /*total_trans_node_cnt*/);
  memtable->destroy();
}

TEST_F(TestMemtableV2, test_lock)
{
  ObMemtable *memtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: txn1 lock row in memtable");
  ObDatumRowkey rowkey;
  ObStoreRow tmp_row;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 tmp_row));

  ObTransID lock_tx_id = ObTransID(1);
  ObStoreCtx *ltx = start_tx(lock_tx_id);
  lock_tx(ltx,
          memtable,
          1000, /*snapshot version*/
          rowkey);
  const auto wtx_seq_no = ObTxSEQ(ObSequence::get_max_seq_no());

  verify_cb(get_tx_last_cb(ltx),
            memtable,
            wtx_seq_no,
            1,   /*key*/
            true /*is_link*/);
  verify_tnode(get_tx_last_tnode(ltx),
               NULL,        /*prev tnode*/
               NULL,        /*next tnode*/
               memtable,
               ltx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX,   /*trans_version*/
               wtx_seq_no,
               0,           /*modify_count*/
               ObMvccTransNode::F_INIT,
               ObDmlFlag::DF_LOCK,
               1,           /*key*/
               UNUSED_VALUE /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(ltx),
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  get_tx_last_tnode(ltx),
                  0, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);
  read_row(ltx,
           memtable,
           rowkey,
           1200,         /*snapshot version*/
           1,            /*key*/
           UNUSED_VALUE, /*value*/
           false   /*exist*/);

  TRANS_LOG(INFO, "######## CASE2: other txn read row in memtable with no data");
  read_row(memtable,
           rowkey,
           1200,   /*snapshot version*/
           -1,     /*key*/
           -1,     /*value*/
           false   /*exist*/);

  TRANS_LOG(INFO, "######## CASE3: txn2 write row in memtable with lock for write failed");
  ObDatumRowkey rowkey2;
  ObStoreRow write_row2;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 3, /*value*/
                                 rowkey2,
                                 write_row2));

  ObTransID write_tx_id2 = ObTransID(2);
  ObStoreCtx *wtx2 = start_tx(write_tx_id2);
  write_tx(wtx2,
           memtable,
           1200, /*snapshot version*/
           write_row2,
           OB_TRY_LOCK_ROW_CONFLICT);

  TRANS_LOG(INFO, "######## CASE4: txn2 lock row in memtable with lock for write failed");
  lock_tx(wtx2,
          memtable,
          1200, /*snapshot version*/
          rowkey,
          OB_TRY_LOCK_ROW_CONFLICT);

  TRANS_LOG(INFO, "######## CASE5: txn1 lock row in memtable with no new lock");
  lock_tx(ltx,
          memtable,
          1200, /*snapshot version*/
          rowkey);
  verify_cb(get_tx_last_cb(ltx),
            memtable,
            wtx_seq_no,
            1,   /*key*/
            true /*is_link*/);
  verify_tnode(get_tx_last_tnode(ltx),
               NULL,        /*prev tnode*/
               NULL,        /*next tnode*/
               memtable,
               ltx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX,   /*trans_version*/
               wtx_seq_no,
               0,           /*modify_count*/
               ObMvccTransNode::F_INIT,
               ObDmlFlag::DF_LOCK,
               1,           /*key*/
               UNUSED_VALUE /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(ltx),
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  get_tx_last_tnode(ltx),
                  0, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);
  read_row(ltx,
           memtable,
           rowkey,
           1200,         /*snapshot version*/
           1,            /*key*/
           UNUSED_VALUE, /*value*/
           false   /*exist*/);

  TRANS_LOG(INFO, "######## CASE6: txn1 commit, and txn2 lock row in memtable succeed");
  commit_txn(ltx,
             2000,/*commit_version*/
             false/*need_write_back*/);
  lock_tx(wtx2,
          memtable,
          2500, /*snapshot version*/
          rowkey);
  const auto wtx2_seq_no = ObTxSEQ(ObSequence::get_max_seq_no());
  verify_cb(get_tx_last_cb(wtx2),
            memtable,
            wtx2_seq_no,
            1,   /*key*/
            true /*is_link*/);
  verify_tnode(get_tx_last_tnode(ltx),
               NULL, /*prev tnode*/
               NULL, /*next tnode*/
               memtable,
               ltx->mvcc_acc_ctx_.tx_id_,
               2000, /*trans_version*/
               wtx_seq_no,
               0,    /*modify_count*/
               ObMvccTransNode::F_COMMITTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_LOCK,
               1,           /*key*/
               UNUSED_VALUE /*value*/);
  verify_tnode(get_tx_last_tnode(wtx2),
               NULL,        /*prev tnode*/
               NULL,        /*next tnode*/
               memtable,
               wtx2->mvcc_acc_ctx_.tx_id_,
               INT64_MAX,   /*trans_version*/
               wtx2_seq_no,
               0,           /*modify_count*/
               ObMvccTransNode::F_INIT,
               ObDmlFlag::DF_LOCK,
               1,           /*key*/
               UNUSED_VALUE /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(wtx2),
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  get_tx_last_tnode(wtx2),
                  2000, /*max_trans_version*/
                  1     /*total_trans_node_cnt*/);
  read_row(wtx2,
           memtable,
           rowkey,
           3000,         /*snapshot version*/
           1,            /*key*/
           UNUSED_VALUE, /*value*/
           false          /*exist*/);
  read_row(memtable,
           rowkey,
           3000,         /*snapshot version*/
           1,            /*key*/
           UNUSED_VALUE, /*value*/
           false         /*exist*/);

  TRANS_LOG(INFO, "######## CASE7: txn2 abort, and txn3 lock row in memtable succeed");
  abort_txn(wtx2,
            false/*need_write_back*/);
  ObTransID write_tx_id3 = ObTransID(3);
  ObStoreCtx *wtx3 = start_tx(write_tx_id3);
  ObDatumRowkey rowkey3;
  ObStoreRow write_row3;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 4, /*value*/
                                 rowkey3,
                                 write_row3));
  write_tx(wtx3,
           memtable,
           4500, /*snapshot version*/
           write_row3);
  const auto wtx3_seq_no = ObTxSEQ(ObSequence::get_max_seq_no());
  verify_cb(get_tx_last_cb(wtx3),
            memtable,
            wtx3_seq_no,
            1,   /*key*/
            true /*is_link*/);
  verify_tnode(get_tx_last_tnode(wtx2),
               NULL,                    /*prev tnode*/
               NULL,                    /*next tnode*/
               memtable,
               wtx2->mvcc_acc_ctx_.tx_id_,
               INT64_MAX,               /*trans_version*/
               wtx2_seq_no,
               0,                       /*modify_count*/
               ObMvccTransNode::F_ABORTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_LOCK,
               1,           /*key*/
               UNUSED_VALUE /*value*/);
  verify_tnode(get_tx_last_tnode(wtx3),
               NULL,                    /*prev tnode*/
               NULL,                    /*next tnode*/
               memtable,
               wtx3->mvcc_acc_ctx_.tx_id_,
               INT64_MAX,               /*trans_version*/
               wtx3_seq_no,
               0,                       /*modify_count*/
               ObMvccTransNode::F_INIT,
               ObDmlFlag::DF_INSERT,
               1, /*key*/
               4  /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(wtx3),
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  get_tx_last_tnode(wtx3),
                  2000, /*max_trans_version*/
                  1     /*total_trans_node_cnt*/);
  read_row(wtx3,
           memtable,
           rowkey,
           4500, /*snapshot version*/
           1,    /*key*/
           4     /*value*/);
  read_row(memtable,
           rowkey,
           3000,         /*snapshot version*/
           1,            /*key*/
           UNUSED_VALUE, /*value*/
           false         /*exist*/);

  ObMvccRow *wtx3_last_row = get_tx_last_mvcc_row(wtx3);
  ObMvccTransNode *wtx3_last_tnode = get_tx_last_tnode(wtx3);
  commit_txn(wtx3,
             5000,/*commit_version*/
             true/*need_write_back*/);
  verify_mvcc_row(wtx3_last_row,
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  wtx3_last_tnode,
                  5000, /*max_trans_version*/
                  1     /*total_trans_node_cnt*/);

  read_row(memtable,
           rowkey,
           6000, /*snapshot version*/
           1,    /*key*/
           4     /*value*/);
  memtable->destroy();
}

TEST_F(TestMemtableV2, test_sstable_lock)
{
  ObMemtable *memtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: write row into memtable failed because of sstable lock");
  is_sstable_contains_lock_ = true;

  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));

  ObTransID write_tx_id = ObTransID(1);
  ObStoreCtx *wtx = start_tx(write_tx_id);
  write_tx(wtx,
           memtable,
           1000, /*snapshot version*/
           write_row,
           OB_TRY_LOCK_ROW_CONFLICT);
  EXPECT_EQ(true, is_write_set_empty(wtx));
  read_row(wtx,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           0,    /*value*/
           false /*exist*/);

  is_sstable_contains_lock_ = false;
  memtable->destroy();
}

TEST_F(TestMemtableV2, test_rollback_to)
{
  ObMemtable *memtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: write row into memtable");
  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  ObStoreRow write_row2;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 3, /*value*/
                                 rowkey,
                                 write_row2));


  ObTransID write_tx_id = ObTransID(1);
  ObStoreCtx *wtx = start_tx(write_tx_id);
  write_tx(wtx,
           memtable,
           1000, /*snapshot version*/
           write_row);
  const auto wtx_seq_no1 = ObTxSEQ(ObSequence::get_max_seq_no());
  write_tx(wtx,
           memtable,
           1000, /*snapshot version*/
           write_row2);
  const auto wtx_seq_no2 = ObTxSEQ(ObSequence::get_max_seq_no());

  print_callback(wtx);

  verify_cb(get_tx_last_cb(wtx),
            memtable,
            wtx_seq_no2,
            1,   /*key*/
            true /*is_link*/);
  verify_tnode(get_tx_last_tnode(wtx),
               get_tx_first_tnode(wtx), /*prev tnode*/
               NULL,                    /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               wtx_seq_no2,
               1,         /*modify_count*/
               ObMvccTransNode::F_INIT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               3          /*value*/);
  verify_mvcc_row(get_tx_last_mvcc_row(wtx),
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  get_tx_last_tnode(wtx),
                  0, /*max_trans_version*/
                  2  /*total_trans_node_cnt*/);
  read_row(wtx,
           memtable,
           rowkey,
           2000, /*snapshot version*/
           1,    /*key*/
           3     /*value*/);

  TRANS_LOG(INFO, "######## CASE2: rollback the last tnode, and write write conflict");
  rollback_to_txn(wtx,
                  wtx_seq_no2,    /*from*/
                  wtx_seq_no1 + 1 /*to*/);

  ObDatumRowkey rowkey3;
  ObStoreRow write_row3;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 4, /*value*/
                                 rowkey3,
                                 write_row3));

  ObTransID write_tx_id2 = ObTransID(2);
  ObStoreCtx *wtx2 = start_tx(write_tx_id2);
  write_tx(wtx2,
           memtable,
           3000, /*snapshot version*/
           write_row3,
           OB_TRY_LOCK_ROW_CONFLICT);
  read_row(wtx,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           2     /*value*/);
  memtable->destroy();
}

TEST_F(TestMemtableV2, test_replay)
{
  ObMemtable *lmemtable = create_memtable();
  ObMemtable *fmemtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: txn1 and txn3 write row in lmemtable");
  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  ObDatumRowkey rowkey2;
  ObStoreRow write_row2;
  ObDatumRowkey rowkey3;
  ObStoreRow write_row3;

  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 3, /*value*/
                                 rowkey2,
                                 write_row2));
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 4, /*value*/
                                 rowkey3,
                                 write_row3));

  ObTransID write_tx_id3 = ObTransID(3);
  ObStoreCtx *wtx3 = start_tx(write_tx_id3);
  write_tx(wtx3,
           lmemtable,
           500, /*snapshot version*/
           write_row3);
  const auto wtx3_seq_no1 = ObTxSEQ(ObSequence::get_max_seq_no());
  commit_txn(wtx3,
             800,/*commit_version*/
             false/*need_write_back*/);

  ObTransID write_tx_id = ObTransID(1);
  ObStoreCtx *wtx = start_tx(write_tx_id);
  write_tx(wtx,
           lmemtable,
           1000, /*snapshot version*/
           write_row);
  const auto wtx_seq_no1 = ObTxSEQ(ObSequence::get_max_seq_no());
  write_tx(wtx,
           lmemtable,
           1200, /*snapshot version*/
           write_row2);
  const auto wtx_seq_no2 = ObTxSEQ(ObSequence::get_max_seq_no());

  ObMemtableMutatorIterator mmi;
  mock_replay_iterator(wtx, mmi);

  commit_txn(wtx,
             2000,/*commit_version*/
             false/*need_write_back*/);

  TRANS_LOG(INFO, "######## CASE2: txn2 replay row in fmemtable");

  ObTransID replay_tx_id = ObTransID(2);
  ObStoreCtx *ptx = start_tx(replay_tx_id, true);
  replay_tx(ptx,
            fmemtable,
            1300, /*replay_scn*/
            mmi);
  read_row(ptx,
           fmemtable,
           rowkey,
           1500, /*snapshot version*/
           1,    /*key*/
           3     /*value*/);
  ObMvccRowCallback *first_cb = (ObMvccRowCallback *)(get_tx_last_cb(ptx)->prev_);
  verify_cb(get_tx_last_cb(ptx),
            fmemtable,
            wtx_seq_no2,
            1,    /*key*/
            true, /*is_link*/
            false,/*need_fill_redo*/
            1300  /*scn*/);
  verify_cb(first_cb,
            fmemtable,
            wtx_seq_no1,
            1,    /*key*/
            true, /*is_link*/
            false,/*need_fill_redo*/
            1300  /*scn*/);
  verify_tnode(get_tx_last_tnode(ptx),
               first_cb->tnode_, /*prev tnode*/
               NULL,             /*next tnode*/
               lmemtable,
               ptx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               wtx_seq_no2,
               2,         /*modify_count*/
               ObMvccTransNode::F_INIT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               3,         /*value*/
               1300       /*scn*/);
  verify_tnode(first_cb->tnode_,
               NULL,                   /*prev tnode*/
               get_tx_last_tnode(ptx), /*next tnode*/
               lmemtable,
               ptx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               wtx_seq_no1,
               1,         /*modify_count*/
               ObMvccTransNode::F_INIT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               2,         /*value*/
               1300       /*scn*/);
  verify_mvcc_row(get_tx_last_mvcc_row(ptx),
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  get_tx_last_tnode(ptx),
                  0, /*max_trans_version*/
                  2  /*total_trans_node_cnt*/);

  TRANS_LOG(INFO, "######## CASE3: txn4 replay row in fmemtable in reverse order");

  ObMemtableMutatorIterator mmi3;
  mock_replay_iterator(wtx3, mmi3);

  ObTransID replay_tx_id4 = ObTransID(4);
  ObStoreCtx *ptx4 = start_tx(replay_tx_id4, true/*for_replay*/);
  replay_tx(ptx4,
            fmemtable,
            800, /*replay_scn*/
            mmi3);

  read_row(ptx,
           fmemtable,
           rowkey,
           1500, /*snapshot version*/
           1,    /*key*/
           3     /*value*/);

  commit_txn(ptx,
             2000,/*commit_version*/
             false/*need_write_back*/);
  commit_txn(ptx4,
             800,/*commit_version*/
             false/*need_write_back*/);

  read_row(fmemtable,
           rowkey,
           3000,   /*snapshot version*/
           1,      /*key*/
           3       /*value*/);

  read_row(fmemtable,
           rowkey,
           900,   /*snapshot version*/
           1,      /*key*/
           4      /*value*/);

  verify_cb(get_tx_last_cb(ptx4),
            fmemtable,
            wtx3_seq_no1,
            1,    /*key*/
            true, /*is_link*/
            false,/*need_fill_redo*/
            800  /*scn*/);
  verify_tnode(get_tx_last_tnode(ptx4),
               NULL,                    /*prev tnode*/
               get_tx_first_tnode(ptx), /*next tnode*/
               lmemtable,
               ptx4->mvcc_acc_ctx_.tx_id_,
               800, /*trans_version*/
               wtx3_seq_no1,
               0,         /*modify_count*/
               ObMvccTransNode::F_COMMITTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               4,         /*value*/
               800        /*scn*/);
  verify_mvcc_row(get_tx_last_mvcc_row(ptx4),
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  get_tx_last_tnode(ptx),
                  2000, /*max_trans_version*/
                  3     /*total_trans_node_cnt*/);
  lmemtable->destroy();
  fmemtable->destroy();
}

// TEST_F(TestMemtableV2, test_replay_with_clog_encryption)
// {
//   ObMemtable *lmemtable = create_memtable();
//   ObMemtable *fmemtable = create_memtable();

//   TRANS_LOG(INFO, "######## CASE1: txn1 write row in lmemtable");
//   ObDatumRowkey rowkey;
//   ObStoreRow write_row;
//   ObDatumRowkey rowkey2;
//   ObStoreRow write_row2;

//   EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
//                                  2, /*value*/
//                                  rowkey,
//                                  write_row));
//   EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
//                                  3, /*value*/
//                                  rowkey2,
//                                  write_row2));

//   ObTransID write_tx_id = ObTransID(1);
//   ObStoreCtx *wtx = start_tx(write_tx_id);

//   //create encrypt_meta and store encrypt_meta in encrypt_meta_
//   ObSerializeEncryptMeta encrypt_meta;
//   create_and_store_encrypt_info(encrypt_meta);

//   //encrypt_meta_ will be added to memtable during ObMemtable::set
//   write_tx(wtx,
//            lmemtable,
//            1000, /*snapshot version*/
//            write_row);
//   const auto wtx_seq_no1 = ObTxSEQ(ObSequence::get_max_seq_no());
//   write_tx(wtx,
//            lmemtable,
//            1200, /*snapshot version*/
//            write_row2);
//   const auto wtx_seq_no2 = ObTxSEQ(ObSequence::get_max_seq_no());

//   //use encrypt_meta_ in memtable during submitting log
//   char *redo_log_buffer = new char[REDO_BUFFER_SIZE];
//   serialize_encrypted_redo_log(wtx, redo_log_buffer);

//   commit_txn(wtx,
//              2000,/*commit_version*/
//              false/*need_write_back*/);

//   TRANS_LOG(INFO, "######## CASE2: txn2 replay row in fmemtable");

//   ObTxRedoLogTempRef temp_ref;
//   ObTxRedoLog redo_log(temp_ref);

//   //get encrypt_info during deserializing log
//   ObMemtableMutatorIterator mmi;
//   ObCLogEncryptInfo encrypt_info;
//   encrypt_info.init();
//   deserialize_redo_log_extract_encryption(redo_log_buffer, redo_log, mmi, encrypt_info);

//   ObTransID replay_tx_id = ObTransID(2);
//   ObStoreCtx *ptx = start_tx(replay_tx_id, true);
//   replay_tx_with_encryption(ptx,
//                             fmemtable,
//                             1300, /*replay_log_ts*/
//                             mmi,
//                             encrypt_info);
//   read_row(ptx,
//            fmemtable,
//            rowkey,
//            1500, /*snapshot version*/
//            1,    /*key*/
//            3     /*value*/);

//   ObMvccRowCallback *first_cb = (ObMvccRowCallback *)(get_tx_last_cb(ptx)->prev_);
//   verify_cb(get_tx_last_cb(ptx),
//             fmemtable,
//             wtx_seq_no2,
//             1,    /*key*/
//             true, /*is_link*/
//             false,/*need_fill_redo*/
//             1300  /*log_ts*/);
//   verify_cb(first_cb,
//             fmemtable,
//             wtx_seq_no1,
//             1,    /*key*/
//             true, /*is_link*/
//             false,/*need_fill_redo*/
//             1300  /*log_ts*/);
//   verify_tnode(get_tx_last_tnode(ptx),
//                first_cb->tnode_, /*prev tnode*/
//                NULL,             /*next tnode*/
//                lmemtable,
//                ptx->mvcc_acc_ctx_.tx_id_,
//                INT64_MAX, /*trans_version*/
//                wtx_seq_no2,
//                1,         /*modify_count*/
//                ObMvccTransNode::F_INIT,
//                ObDmlFlag::DF_INSERT,
//                1,         /*key*/
//                3,         /*value*/
//                1300       /*log_ts*/);
//   verify_tnode(first_cb->tnode_,
//                NULL,                   /*prev tnode*/
//                get_tx_last_tnode(ptx), /*next tnode*/
//                lmemtable,
//                ptx->mvcc_acc_ctx_.tx_id_,
//                INT64_MAX, /*trans_version*/
//                wtx_seq_no1,
//                0,         /*modify_count*/
//                ObMvccTransNode::F_INIT,
//                ObDmlFlag::DF_INSERT,
//                1,         /*key*/
//                2,         /*value*/
//                1300       /*log_ts*/);
//   verify_mvcc_row(get_tx_last_mvcc_row(ptx),
//                   ObDmlFlag::DF_NOT_EXIST,
//                   ObDmlFlag::DF_NOT_EXIST,
//                   get_tx_last_tnode(ptx),
//                   0, /*max_trans_version*/
//                   2  /*total_trans_node_cnt*/);
//   commit_txn(ptx,
//              2500,/*commit_version*/
//              false/*need_write_back*/);
//   read_row(fmemtable,
//            rowkey,
//            3000,   /*snapshot version*/
//            1,      /*key*/
//            3       /*value*/);

//   //release resources
//   delete[] redo_log_buffer;
//   encrypt_meta_ = NULL;
//   lmemtable->destroy();
//   fmemtable->destroy();
// }

TEST_F(TestMemtableV2, test_compact)
{
  ObMemtable *lmemtable = create_memtable();
  ObMemtable *fmemtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: txn1 and txn3 write row in lmemtable");
  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  ObDatumRowkey rowkey2;
  ObStoreRow write_row2;
  ObDatumRowkey rowkey3;
  ObStoreRow write_row3;

  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 3, /*value*/
                                 rowkey2,
                                 write_row2));
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 4, /*value*/
                                 rowkey3,
                                 write_row3));

  ObTransID write_tx_id = ObTransID(1);
  ObStoreCtx *wtx = start_tx(write_tx_id);
  write_tx(wtx,
           lmemtable,
           1000, /*snapshot version*/
           write_row);
  const auto wtx_seq_no1 = ObTxSEQ(ObSequence::get_max_seq_no());
  write_tx(wtx,
           lmemtable,
           1200, /*snapshot version*/
           write_row2);
  const auto wtx_seq_no2 = ObTxSEQ(ObSequence::get_max_seq_no());

  ObMemtableMutatorIterator mmi;
  mock_replay_iterator(wtx, mmi);

  print_callback(wtx);

  commit_txn(wtx,
             2000,/*commit_version*/
             false/*need_write_back*/);

  ObTransID write_tx_id3 = ObTransID(3);
  ObStoreCtx *wtx3 = start_tx(write_tx_id3);
  write_tx(wtx3,
           lmemtable,
           2500, /*snapshot version*/
           write_row3);
  const auto wtx3_seq_no1 = ObTxSEQ(ObSequence::get_max_seq_no());
  commit_txn(wtx3,
             3000,/*commit_version*/
             false/*need_write_back*/);

  ObMemtableMutatorIterator mmi3;
  mock_replay_iterator(wtx3, mmi3);

  TRANS_LOG(INFO, "######## CASE2: txn2 replay row in fmemtable");

  ObTransID replay_tx_id = ObTransID(2);
  ObStoreCtx *ptx = start_tx(replay_tx_id, true/*for_replay*/);
  replay_tx(ptx,
            fmemtable,
            2000, /*replay_scn*/
            mmi);
  read_row(ptx,
           fmemtable,
           rowkey,
           2400, /*snnapshot version*/
           1,    /*keny*/
           3     /*value*/);

  ObMvccRow *row = get_tx_last_mvcc_row(ptx);
  ObMvccTransNode *ptx_first_tnode = get_tx_first_tnode(ptx);
  ObMvccTransNode *ptx_last_tnode = get_tx_last_tnode(ptx);

  commit_txn(ptx,
             2000,/*commit_version*/
             true/*need_write_back*/);

  compact_row(row, fmemtable, 2400, true/*for_replay*/);

  verify_tnode(ptx_last_tnode,
               ptx_first_tnode, /*prev tnode*/
               row->latest_compact_node_, /*next tnode*/
               lmemtable,
               ptx->mvcc_acc_ctx_.tx_id_,
               2000,        /*trans_version*/
               wtx_seq_no2,
               1,         /*modify_count*/
               ObMvccTransNode::F_COMMITTED,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               3,         /*value*/
               2000       /*scn*/);
  verify_tnode(row->latest_compact_node_,
               ptx_last_tnode,/*prev tnode*/
               NULL, /*next tnode*/
               lmemtable,
               ptx->mvcc_acc_ctx_.tx_id_,
               2000, /*trans_version*/
               wtx_seq_no2,
               1,         /*modify_count*/
               ObMvccTransNode::F_COMMITTED,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               3,         /*value*/
               2000,      /*scn*/
               NDT_COMPACT);
  verify_mvcc_row(row,
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  row->latest_compact_node_, /*list_head*/
                  2000, /*max_trans_version*/
                  2  /*total_trans_node_cnt*/);

  TRANS_LOG(INFO, "######## CASE2: txn4 replay uncommitted row in lmemtable and compacted");

  ObTransID replay_tx_id4 = ObTransID(4);
  ObStoreCtx *ptx4 = start_tx(replay_tx_id4, true/*for_replay*/);
  replay_tx(ptx4,
            fmemtable,
            3000, /*replay_scn*/
            mmi3);

  read_row(fmemtable,
           rowkey,
           2900, /*snapshot version*/
           1,    /*key*/
           3     /*value*/);

  compact_row(row, fmemtable, 2500, true/*for_replay*/);

  verify_cb(get_tx_last_cb(ptx4),
            fmemtable,
            wtx3_seq_no1,
            1,    /*key*/
            true, /*is_link*/
            false,/*need_fill_redo*/
            3000  /*scn*/);
  verify_tnode(get_tx_last_tnode(ptx4),
               row->latest_compact_node_, /*prev tnode*/
               NULL,                      /*next tnode*/
               lmemtable,
               ptx4->mvcc_acc_ctx_.tx_id_,
               INT64_MAX,                 /*trans_version*/
               wtx3_seq_no1,
               2,                         /*modify_count*/
               ObMvccTransNode::F_INIT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               4,         /*value*/
               3000       /*scn*/);
  verify_tnode(row->latest_compact_node_,
               ptx_last_tnode,          /*prev tnode*/
               get_tx_last_tnode(ptx4), /*next tnode*/
               lmemtable,
               ptx->mvcc_acc_ctx_.tx_id_,
               2000,                    /*trans_version*/
               wtx_seq_no2,
               1,                       /*modify_count*/
               ObMvccTransNode::F_COMMITTED,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               3,         /*value*/
               2000,      /*scn*/
               NDT_COMPACT);
  verify_mvcc_row(get_tx_last_mvcc_row(ptx4),
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  get_tx_last_tnode(ptx4),
                  2000, /*max_trans_version*/
                  3     /*total_trans_node_cnt*/);

  TRANS_LOG(INFO, "######## CASE3: commit txn4 in lmemtable and compacted");

  ObMvccTransNode *prev_compact_node = row->latest_compact_node_;
  ObMvccRowCallback *ptx4_cb = get_tx_last_cb(ptx4);
  ObMvccTransNode *ptx4_tnode = get_tx_last_tnode(ptx4);
  commit_txn(ptx4,
             3000,/*commit_version*/
             true/*need_write_back*/);

  read_row(fmemtable,
           rowkey,
           3500, /*snapshot version*/
           1,    /*key*/
           4     /*value*/);

  compact_row(row, fmemtable, 4000, true/*for_replay*/);

  verify_tnode(ptx4_tnode,
               prev_compact_node, /*prev tnode*/
               row->latest_compact_node_, /*next tnode*/
               lmemtable,
               ptx4->mvcc_acc_ctx_.tx_id_,
               3000,                 /*trans_version*/
               wtx3_seq_no1,
               2,                         /*modify_count*/
               ObMvccTransNode::F_COMMITTED,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               4,         /*value*/
               3000       /*scn*/);
  verify_tnode(row->latest_compact_node_,
               ptx4_tnode, /*prev tnode*/
               NULL, /*next tnode*/
               lmemtable,
               ptx4->mvcc_acc_ctx_.tx_id_,
               3000,                    /*trans_version*/
               wtx3_seq_no1,
               2,                       /*modify_count*/
               ObMvccTransNode::F_COMMITTED,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               4,         /*value*/
               3000,      /*scn*/
               NDT_COMPACT);
  verify_mvcc_row(row,
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  row->latest_compact_node_,
                  3000, /*max_trans_version*/
                  3     /*total_trans_node_cnt*/);
  lmemtable->destroy();
  fmemtable->destroy();
}

TEST_F(TestMemtableV2, test_compact_v2)
{
  ObMemtable *memtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: txn1 write two rows and txn3 write a row in memtable");
  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  ObDatumRowkey rowkey2;
  ObStoreRow write_row2;
  ObDatumRowkey rowkey3;
  ObStoreRow write_row3;

  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 3, /*value*/
                                 rowkey2,
                                 write_row2));
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 4, /*value*/
                                 rowkey3,
                                 write_row3));

  ObTransID write_tx_id = ObTransID(1);
  ObStoreCtx *wtx = start_tx(write_tx_id);

  lock_tx(wtx,
          memtable,
          1000, /*snapshot version*/
          rowkey);
  const auto wtx_seq_no1 = ObTxSEQ(ObSequence::get_max_seq_no());
  ObMvccTransNode *wtx_first_tnode = get_tx_last_tnode(wtx);
  ObMvccRow *row = get_tx_last_mvcc_row(wtx);

  write_tx(wtx,
           memtable,
           1200, /*snapshot version*/
           write_row);
  const auto wtx_seq_no2 = ObTxSEQ(ObSequence::get_max_seq_no());
  ObMvccTransNode *wtx_second_tnode = get_tx_last_tnode(wtx);

  write_tx(wtx,
           memtable,
           1300, /*snapshot version*/
           write_row2);
  const auto wtx_seq_no3 = ObTxSEQ(ObSequence::get_max_seq_no());
  ObMvccTransNode *wtx_third_tnode = get_tx_last_tnode(wtx);

  print_callback(wtx);

  commit_txn(wtx,
             2000,/*commit_version*/
             false/*need_write_back*/);

  ObTransID write_tx_id3 = ObTransID(3);
  ObStoreCtx *wtx3 = start_tx(write_tx_id3);
  write_tx(wtx3,
           memtable,
           2500, /*snapshot version*/
           write_row3);
  const auto wtx3_seq_no1 = ObTxSEQ(ObSequence::get_max_seq_no());
  ObMvccTransNode *wtx3_first_tnode = get_tx_last_tnode(wtx3);
  commit_txn(wtx3,
             3000,/*commit_version*/
             false/*need_write_back*/);

  verify_tnode(wtx_first_tnode,
               NULL, /*prev tnode*/
               wtx_second_tnode, /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX,        /*trans_version*/
               wtx_seq_no1,
               0,         /*modify_count*/
               ObMvccTransNode::F_INIT | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_LOCK,
               1,         /*key*/
               UNUSED_VALUE /*value*/);

  verify_tnode(wtx_second_tnode,
               wtx_first_tnode,  /*prev tnode*/
               wtx_third_tnode, /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX,        /*trans_version*/
               wtx_seq_no2,
               1,         /*modify_count*/
               ObMvccTransNode::F_INIT | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               2 /*value*/);

  verify_mvcc_row(row,
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  wtx3_first_tnode,
                  2000, /*max_trans_version*/
                  4  /*total_trans_node_cnt*/);

  compact_row(row, memtable, 2400, false/*for_replay*/);

  verify_tnode(wtx_first_tnode,
               NULL, /*prev tnode*/
               wtx_second_tnode, /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               2000,        /*trans_version*/
               wtx_seq_no1,
               0,         /*modify_count*/
               ObMvccTransNode::F_COMMITTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_LOCK,
               1,         /*key*/
               UNUSED_VALUE /*value*/);

  verify_tnode(wtx_second_tnode,
               NULL,  /*prev tnode*/
               wtx_third_tnode, /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               2000,        /*trans_version*/
               wtx_seq_no2,
               1,         /*modify_count*/
               ObMvccTransNode::F_COMMITTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               2 /*value*/);

  verify_tnode(wtx_third_tnode,
               wtx_second_tnode,  /*prev tnode*/
               row->latest_compact_node_, /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               2000,        /*trans_version*/
               wtx_seq_no3,
               2,         /*modify_count*/
               ObMvccTransNode::F_COMMITTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               3 /*value*/);

  verify_tnode(row->latest_compact_node_,
               wtx_third_tnode,  /*prev tnode*/
               wtx3_first_tnode, /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               2000,        /*trans_version*/
               wtx_seq_no3,
               2,         /*modify_count*/
               ObMvccTransNode::F_COMMITTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               ObDmlFlag::DF_INSERT,
               1,         /*key*/
               3, /*value*/
               INT64_MAX,
               NDT_COMPACT);

  verify_mvcc_row(row,
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  wtx3_first_tnode,
                  2000, /*max_trans_version*/
                  3  /*total_trans_node_cnt*/);
  memtable->destroy();
}

TEST_F(TestMemtableV2, test_compact_v3)
{
  ObMemtable *lmemtable = create_memtable();
  ObMemtable *fmemtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: txn1 write two row and txn2 write row in lmemtable");
  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  ObDatumRowkey rowkey2;
  ObStoreRow write_row2;
  ObDatumRowkey rowkey3;
  ObStoreRow write_row3;
  ObDatumRowkey rowkey4;
  ObStoreRow write_row4;

  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 3, /*value*/
                                 rowkey2,
                                 write_row2));
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 4, /*value*/
                                 rowkey3,
                                 write_row3));
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 5, /*value*/
                                 rowkey4,
                                 write_row4));

  ObTransID write_tx_id = ObTransID(1);
  ObStoreCtx *wtx = start_tx(write_tx_id);
  write_tx(wtx,
           lmemtable,
           1000, /*snapshot version*/
           write_row);
  write_tx(wtx,
           lmemtable,
           1200, /*snapshot version*/
           write_row2);

  ObMemtableMutatorIterator mmi;
  mock_replay_iterator(wtx, mmi);

  abort_txn(wtx,
            false/*need_write_back*/);

  ObTransID write_tx_id2 = ObTransID(2);
  ObStoreCtx *wtx2 = start_tx(write_tx_id2);
  lock_tx(wtx2,
          lmemtable,
          2200, /*snapshot version*/
          rowkey3);
  print_callback(wtx2);

  ObMemtableMutatorIterator mmi2;
  mock_replay_iterator(wtx2, mmi2);
  commit_txn(wtx2,
             3000,/*commit_version*/
             false/*need_write_back*/);


  ObTransID write_tx_id5 = ObTransID(5);
  ObStoreCtx *wtx5 = start_tx(write_tx_id5);
  lock_tx(wtx5,
          lmemtable,
          3200, /*snapshot version*/
          rowkey4);
  ObMemtableMutatorIterator mmi3;
  mock_replay_iterator(wtx5, mmi3);

  TRANS_LOG(INFO, "######## CASE2: txn3, txn4 replay rows in fmemtable in reverse order");

  ObTransID replay_tx_id3 = ObTransID(3);
  ObStoreCtx *ptx3 = start_tx(replay_tx_id3, true/*for_replay*/);
  ObTransID replay_tx_id4 = ObTransID(4);
  ObStoreCtx *ptx4 = start_tx(replay_tx_id4, true/*for_replay*/);
  ObTransID replay_tx_id6 = ObTransID(6);
  ObStoreCtx *ptx6 = start_tx(replay_tx_id6, true/*for_replay*/);

  replay_tx(ptx6,
            fmemtable,
            3500, /*replay_scn*/
            mmi3);
  replay_tx(ptx3,
            fmemtable,
            3000, /*replay_scn*/
            mmi2);
  replay_tx(ptx4,
            fmemtable,
            2000, /*replay_scn*/
            mmi);

  ObMvccTransNode *node =  get_tx_last_tnode(ptx3);
  ((ObMemtableDataHeader *)(node->buf_))->dml_flag_ = blocksstable::ObDmlFlag::DF_INSERT;
  ObMvccTransNode *node2 = get_tx_last_tnode(ptx6);
  ((ObMemtableDataHeader *)(node2->buf_))->dml_flag_ = blocksstable::ObDmlFlag::DF_INSERT;

  ObMvccRow *row = get_tx_last_mvcc_row(ptx3);
  row->print_row();

  commit_txn(ptx3,
             3000,/*commit_version*/
             true/*need_write_back*/);
  commit_txn(ptx6,
             3600,/*commit_version*/
             true/*need_write_back*/);
  abort_txn(ptx4,
            false/*need_write_back*/);

  ((ObMemtableDataHeader *)(node->buf_))->dml_flag_ = blocksstable::ObDmlFlag::DF_LOCK;
  ((ObMemtableDataHeader *)(node2->buf_))->dml_flag_ = blocksstable::ObDmlFlag::DF_LOCK;

  row->print_row();
  compact_row(row, fmemtable, 4000, true/*for_replay*/);
  row->print_row();
  ObMvccTransNode *compact_node = row->latest_compact_node_;
  EXPECT_EQ(NULL, compact_node);

  verify_mvcc_row(row,
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  node2,
                  3600, /*max_trans_version*/
                  2  /*total_trans_node_cnt*/);

  read_row(fmemtable,
           rowkey,
           4500, /*snapshot version*/
           1,    /*key*/
           4,    /*value*/
           false /*exist*/);
  lmemtable->destroy();
  fmemtable->destroy();
}

TEST_F(TestMemtableV2, test_dml_flag)
{
  ObMemtable *lmemtable = create_memtable();
  ObMemtable *fmemtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: txns write and row in lmemtable, test its dml flag");
  ObDatumRowkey rowkey;
  ObStoreRow write_row1;
  ObDatumRowkey rowkey2;
  ObStoreRow write_row2;
  ObDatumRowkey rowkey3;
  ObStoreRow write_row3;

  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row1));
  EXPECT_EQ(OB_SUCCESS, mock_delete(1, /*key*/
                                    rowkey2,
                                    write_row2));
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 4, /*value*/
                                 rowkey3,
                                 write_row3));

  ObTransID write_tx_id1 = ObTransID(1);
  ObStoreCtx *wtx1 = start_tx(write_tx_id1);
  write_tx(wtx1,
           lmemtable,
           400, /*snapshot version*/
           write_row1);
  const auto wtx1_seq_no1 = ObTxSEQ(ObSequence::get_max_seq_no());
  ObMvccRow *wtx1_row = get_tx_last_mvcc_row(wtx1);
  ObMvccTransNode *wtx1_tnode1 = get_tx_last_tnode(wtx1);

  verify_mvcc_row(wtx1_row,
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  wtx1_tnode1,
                  0, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);

  ObMemtableMutatorIterator mmi1;
  mock_replay_iterator(wtx1, mmi1);

  commit_txn(wtx1,
             800,/*commit_version*/
             true/*need_write_back*/);

  verify_mvcc_row(wtx1_row,
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  wtx1_tnode1,
                  800, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);

  ObTransID write_tx_id2 = ObTransID(2);
  ObStoreCtx *wtx2 = start_tx(write_tx_id2);
  lock_tx(wtx2,
          lmemtable,
          1000, /*snapshot version*/
          rowkey);
  const auto wtx2_seq_no1 = ObTxSEQ(ObSequence::get_max_seq_no());

  ObMemtableMutatorIterator mmi2;
  mock_replay_iterator(wtx2, mmi2);

  commit_txn(wtx2,
             1200,/*commit_version*/
             true/*need_write_back*/);

  verify_mvcc_row(wtx1_row,
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  wtx1_tnode1,
                  1200, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);

  ObTransID write_tx_id3 = ObTransID(3);
  ObStoreCtx *wtx3 = start_tx(write_tx_id3);
  write_tx(wtx3,
           lmemtable,
           1400, /*snapshot version*/
           write_row2);
  const auto wtx3_seq_no1 = ObTxSEQ(ObSequence::get_max_seq_no());
  ObMvccTransNode *wtx3_tnode1 = get_tx_last_tnode(wtx3);

  ObMemtableMutatorIterator mmi3;
  mock_replay_iterator(wtx3, mmi3);

  commit_txn(wtx3,
             1600,/*commit_version*/
             true/*need_write_back*/);

  verify_mvcc_row(wtx1_row,
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_DELETE,
                  wtx3_tnode1,
                  1600, /*max_trans_version*/
                  2  /*total_trans_node_cnt*/);

  TRANS_LOG(INFO, "######## CASE2: txns replay row in fmemtable and test dml flag");

  ObTransID replay_tx_id1 = ObTransID(4);
  ObStoreCtx *ptx1 = start_tx(replay_tx_id1, true);
  ObTransID replay_tx_id2 = ObTransID(5);
  ObStoreCtx *ptx2 = start_tx(replay_tx_id2, true);
  ObTransID replay_tx_id3 = ObTransID(6);
  ObStoreCtx *ptx3 = start_tx(replay_tx_id3, true);

  replay_tx(ptx2,
            fmemtable,
            1200, /*replay_scn*/
            mmi2);
  ObMvccRow *ptx2_row = get_tx_last_mvcc_row(ptx2);
  ObMvccTransNode *ptx2_tnode1 = get_tx_last_tnode(ptx2);
  commit_txn(ptx2,
             1200,/*commit_version*/
             true/*need_write_back*/);

  verify_mvcc_row(ptx2_row,
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  NULL,
                  1200, /*max_trans_version*/
                  0  /*total_trans_node_cnt*/);

  replay_tx(ptx3,
            fmemtable,
            1600, /*replay_scn*/
            mmi3);
  ObMvccRow *ptx3_row = get_tx_last_mvcc_row(ptx3);
  ObMvccTransNode *ptx3_tnode1 = get_tx_last_tnode(ptx3);
  commit_txn(ptx3,
             1600,/*commit_version*/
             true/*need_write_back*/);

  verify_mvcc_row(ptx3_row,
                  ObDmlFlag::DF_DELETE,
                  ObDmlFlag::DF_DELETE,
                  ptx3_tnode1,
                  1600, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);

  replay_tx(ptx1,
            fmemtable,
            800, /*replay_scn*/
            mmi1);
  ObMvccRow *ptx1_row = get_tx_last_mvcc_row(ptx1);
  ObMvccTransNode *ptx1_tnode1 = get_tx_last_tnode(ptx1);
  commit_txn(ptx1,
             800,/*commit_version*/
             true/*need_write_back*/);

  verify_mvcc_row(ptx1_row,
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_DELETE,
                  ptx3_tnode1,
                  1600, /*max_trans_version*/
                  2  /*total_trans_node_cnt*/);
  lmemtable->destroy();
  fmemtable->destroy();
}

TEST_F(TestMemtableV2, test_fast_commit)
{
  ObMemtable *memtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: write row into memtable and fast commit, then check result is ok");
  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));

  ObTransID write_tx_id = ObTransID(1);
  ObTransID write_tx_id2 = ObTransID(2);
  ObStoreCtx *wtx = start_tx(write_tx_id);
  ObStoreCtx *wtx2 = start_tx(write_tx_id2);
  write_tx(wtx,
           memtable,
           1000, /*snapshot version*/
           write_row);
  const auto wtx_seq_no = ObTxSEQ(ObSequence::get_max_seq_no());
  ObMvccRowCallback *wtx_cb = (ObMvccRowCallback *)(get_tx_last_cb(wtx));
  ObMvccRow *wtx_row = get_tx_last_mvcc_row(wtx);
  ObMvccTransNode *wtx_tnode = get_tx_last_tnode(wtx);

  read_row(wtx,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           2     /*value*/);

  fast_commit_txn(wtx);

  read_row(wtx,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           2     /*value*/);

  read_row(wtx2,
           memtable,
           rowkey,
           1100, /*snapshot version*/
           1,    /*key*/
           2,    /*value*/
           false /*exist*/);

  prepare_txn(wtx, 1200 /*prepare_version*/);

  read_row(memtable,
           rowkey,
           1800,   /*snapshot version*/
           1,      /*key*/
           2,      /*value*/
           false,  /*exist*/
           OB_ERR_SHARED_LOCK_CONFLICT,
           1000000 /*expire_time*/);

  verify_cb(wtx_cb,
            memtable,
            wtx_seq_no,
            1,   /*key*/
            true /*is_link*/);

  verify_tnode(wtx_tnode,
               NULL,      /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               wtx_seq_no,
               0,         /*modify_count*/
               ObMvccTransNode::F_DELAYED_CLEANOUT,
               DF_INSERT,
               1,         /*key*/
               2          /*value*/);
  verify_mvcc_row(wtx_row,
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  wtx_tnode,
                  0, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);

  commit_txn(wtx, 2000 /*commit_version*/);

  read_row(wtx,
           memtable,
           rowkey,
           3000, /*snapshot version*/
           1,    /*key*/
           2     /*value*/);

  verify_cb(wtx_cb,
            memtable,
            wtx_seq_no,
            1,   /*key*/
            true /*is_link*/);
  verify_tnode(wtx_tnode,
               NULL,      /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               2000, /*trans_version*/
               wtx_seq_no,
               0,         /*modify_count*/
               ObMvccTransNode::F_COMMITTED | ObMvccTransNode::F_DELAYED_CLEANOUT,
               DF_INSERT,
               1,         /*key*/
               2          /*value*/);
  verify_mvcc_row(wtx_row,
                  ObDmlFlag::DF_INSERT,
                  ObDmlFlag::DF_INSERT,
                  wtx_tnode,
                  2000, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);
  memtable->destroy();
}

TEST_F(TestMemtableV2, test_fast_commit_with_no_delay_cleanout)
{
  ObMemtable *memtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: write row into memtable and not fast commit, then check result is ok");
  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));

  ObTransID write_tx_id = ObTransID(1);
  ObTransID write_tx_id2 = ObTransID(2);
  ObStoreCtx *wtx = start_tx(write_tx_id);
  ObStoreCtx *wtx2 = start_tx(write_tx_id2);
  write_tx(wtx,
           memtable,
           1000, /*snapshot version*/
           write_row);
  const auto wtx_seq_no = ObTxSEQ(ObSequence::get_max_seq_no());
  ObMvccRowCallback *wtx_cb = (ObMvccRowCallback *)(get_tx_last_cb(wtx));
  ObMvccRow *wtx_row = get_tx_last_mvcc_row(wtx);
  ObMvccTransNode *wtx_tnode = get_tx_last_tnode(wtx);

  read_row(wtx,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           2     /*value*/);

  read_row(wtx,
           memtable,
           rowkey,
           1000, /*snapshot version*/
           1,    /*key*/
           2     /*value*/);

  read_row(wtx2,
           memtable,
           rowkey,
           1100, /*snapshot version*/
           1,    /*key*/
           2,    /*value*/
           false /*exist*/);

  prepare_txn(wtx, 1200 /*prepare_version*/);

  read_row(memtable,
           rowkey,
           1800,   /*snapshot version*/
           1,      /*key*/
           2,      /*value*/
           false,  /*exist*/
           OB_ERR_SHARED_LOCK_CONFLICT,
           1000000 /*expire_time*/);

  verify_cb(wtx_cb,
            memtable,
            wtx_seq_no,
            1,   /*key*/
            true /*is_link*/);

  verify_tnode(wtx_tnode,
               NULL,      /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               wtx_seq_no,
               0,         /*modify_count*/
               ObMvccTransNode::F_INIT,
               DF_INSERT,
               1,         /*key*/
               2          /*value*/);
  verify_mvcc_row(wtx_row,
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  wtx_tnode,
                  0, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);

  // NB: we use the following code to mock the concurrency between tx_end and
  // delay cleanout
  ObPartTransCtx *tx_ctx = wtx->mvcc_acc_ctx_.tx_ctx_;
  ObMemtableCtx *mt_ctx = wtx->mvcc_acc_ctx_.mem_ctx_;
  tx_ctx->exec_info_.state_ = ObTxState::COMMIT;
  share::SCN commit_scn;
  commit_scn.convert_for_tx(2000);
  tx_ctx->ctx_tx_data_.set_commit_version(commit_scn);
  tx_ctx->ctx_tx_data_.set_state(ObTxData::COMMIT);

  read_row(wtx,
           memtable,
           rowkey,
           3000, /*snapshot version*/
           1,    /*key*/
           2     /*value*/);

  verify_cb(wtx_cb,
            memtable,
            wtx_seq_no,
            1,   /*key*/
            true /*is_link*/);

  verify_tnode(wtx_tnode,
               NULL,      /*prev tnode*/
               NULL,      /*next tnode*/
               memtable,
               wtx->mvcc_acc_ctx_.tx_id_,
               INT64_MAX, /*trans_version*/
               wtx_seq_no,
               0,         /*modify_count*/
               ObMvccTransNode::F_INIT,
               DF_INSERT,
               1,         /*key*/
               2          /*value*/);
  verify_mvcc_row(wtx_row,
                  ObDmlFlag::DF_NOT_EXIST,
                  ObDmlFlag::DF_NOT_EXIST,
                  wtx_tnode,
                  INT64_MAX, /*max_trans_version*/
                  1  /*total_trans_node_cnt*/);
  memtable->destroy();
}

TEST_F(TestMemtableV2, test_seq_set_violation)
{
  int ret = OB_SUCCESS;
  ObMemtable *memtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: write row into memtable");
  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  ObStoreRow write_row2;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 3, /*value*/
                                 rowkey,
                                 write_row2));

  ObTransID write_tx_id = ObTransID(1);
  ObStoreCtx *wtx = start_tx(write_tx_id);

  ObTxSEQ read_seq_no = ObTxSEQ(ObSequence::get_max_seq_no());
  share::SCN scn_3000;
  scn_3000.convert_for_tx(3000);
  start_pdml_stmt(wtx, scn_3000, read_seq_no, 1000000000/*expire_time*/);

  ObTableAccessContext context;
  ObVersionRange trans_version_range;
  const bool read_latest = true;
  ObQueryFlag query_flag;

  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
  query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
  query_flag.read_latest_ = read_latest & ObQueryFlag::OBSF_MASK_READ_LATEST;

  if (OB_FAIL(context.init(query_flag, *wtx, allocator_, trans_version_range))) {
    TRANS_LOG(WARN, "Fail to init access context", K(ret));
  }

  EXPECT_EQ(OB_SUCCESS, (ret = memtable->set(iter_param_,
                                             context,
                                             columns_,
                                             write_row,
                                             encrypt_meta_,
                                             false)));

  start_pdml_stmt(wtx, scn_3000, read_seq_no, 1000000000/*expire_time*/);
  EXPECT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, (ret = memtable->set(iter_param_,
                                                               context,
                                                               columns_,
                                                               write_row,
                                                               encrypt_meta_,
                                                               false)));
  memtable->destroy();
}

TEST_F(TestMemtableV2, test_parallel_lock_with_same_txn)
{
  int ret = OB_SUCCESS;
  ObMemtable *memtable = create_memtable();

  TRANS_LOG(INFO, "######## CASE1: lock row into memtable parallelly");
  ObDatumRowkey rowkey;
  ObStoreRow write_row;
  EXPECT_EQ(OB_SUCCESS, mock_row(1, /*key*/
                                 2, /*value*/
                                 rowkey,
                                 write_row));

  ObTransID write_tx_id = ObTransID(1);
  ObStoreCtx *wtx = start_tx(write_tx_id);
  share::SCN scn_1000;
  scn_1000.convert_for_tx(1000);

  // Step1: prepare the global sequence
  ObSequence::inc();
  ObTxSEQ read_seq_no = ObTxSEQ(ObSequence::get_max_seq_no());

  // Step2: init the mvcc acc ctx
  wtx->mvcc_acc_ctx_.type_ = ObMvccAccessCtx::T::WRITE;
  wtx->mvcc_acc_ctx_.snapshot_.tx_id_ = wtx->mvcc_acc_ctx_.tx_id_;
  wtx->mvcc_acc_ctx_.snapshot_.version_ = scn_1000;
  wtx->mvcc_acc_ctx_.snapshot_.scn_ = read_seq_no;
  const int64_t abs_expire_time = 10000000000 + ::oceanbase::common::ObTimeUtility::current_time();
  wtx->mvcc_acc_ctx_.abs_lock_timeout_ts_ = abs_expire_time;
  wtx->mvcc_acc_ctx_.tx_scn_ = ObTxSEQ(ObSequence::inc_and_get_max_seq_no());

  ObTableAccessContext context;
  ObVersionRange trans_version_range;
  const bool read_latest = true;
  ObQueryFlag query_flag;

  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = EXIST_READ_SNAPSHOT_VERSION;
  query_flag.use_row_cache_ = ObQueryFlag::DoNotUseCache;
  query_flag.read_latest_ = read_latest & ObQueryFlag::OBSF_MASK_READ_LATEST;

  if (OB_FAIL(context.init(query_flag, *wtx, allocator_, trans_version_range))) {
    TRANS_LOG(WARN, "Fail to init access context", K(ret));
  }

  // Step3: lock for the first time
  EXPECT_EQ(OB_SUCCESS, (ret = memtable->lock(iter_param_,
                                              context,
                                              rowkey)));

  // Step4: lock for the second time
  wtx->mvcc_acc_ctx_.tx_scn_ = ObTxSEQ(ObSequence::inc_and_get_max_seq_no());
  EXPECT_EQ(OB_SUCCESS, (ret = memtable->lock(iter_param_,
                                              context,
                                              rowkey)));
  memtable->destroy();
}

TEST_F(TestMemtableV2, test_sync_log_fail_on_frozen_memtable)
{
  int ret = OB_SUCCESS;
  ObMemtable *memtable = create_memtable();

  TRANS_LOG(INFO, "######## start two txn and write some rows");
  ObDatumRowkey rowkey;
  ObTransID txid_1 = ObTransID(1);
  ObStoreCtx *tx_1 = start_tx(txid_1);
  int i = 1;
  for (; i <= 10; i++) {
    ObStoreRow row1;
    EXPECT_EQ(OB_SUCCESS, mock_row(i, i*2, rowkey, row1));
    write_tx(tx_1, memtable, 10000, row1);
  }
  ObTransID txid_2 = ObTransID(2);
  ObStoreCtx *tx_2 = start_tx(txid_2);
  for (; i <= 20; i++) {
    ObStoreRow row1;
    EXPECT_EQ(OB_SUCCESS, mock_row(i, i*4, rowkey, row1));
    write_tx(tx_2, memtable, 10000, row1);
  }
  TRANS_LOG(INFO, "######## frozen memtable");
  memtable->set_is_tablet_freeze();
  EXPECT_TRUE(memtable->is_frozen_memtable());
  TRANS_LOG(INFO, "######## submit log for frozen memtable");
  ObCallbackScope scope_tx_1_1;
  EXPECT_EQ(OB_SUCCESS, flush_txn_log(tx_1, 100, 5, scope_tx_1_1));
  EXPECT_EQ(5, scope_tx_1_1.cnt_);
  ObCallbackScope scope_tx_2_1;
  EXPECT_EQ(OB_SUCCESS, flush_txn_log(tx_2, 200, 5, scope_tx_2_1));
  EXPECT_EQ(5, scope_tx_2_1.cnt_);
  ObCallbackScope scope_tx_1_2;
  EXPECT_EQ(OB_SUCCESS, flush_txn_log(tx_1, 300, 5, scope_tx_1_2));
  EXPECT_EQ(5, scope_tx_1_2.cnt_);
  ObCallbackScope scope_tx_2_2;
  EXPECT_EQ(OB_SUCCESS, flush_txn_log(tx_2, 400, 5, scope_tx_2_2));
  EXPECT_EQ(5, scope_tx_2_2.cnt_);
  EXPECT_EQ(400, memtable->get_max_end_scn().get_val_for_tx());
  EXPECT_EQ(100, memtable->get_rec_scn().get_val_for_tx());
  TRANS_LOG(INFO, "######## sync log fail, adjust memtable's right boundary");
  int64_t removed_cnt_tx_1;
  int64_t palf_applied_scn = 250;
  EXPECT_EQ(OB_SUCCESS, sync_txn_log_fail(tx_1, scope_tx_1_2, palf_applied_scn, removed_cnt_tx_1));
  EXPECT_EQ(250, memtable->get_max_end_scn().get_val_for_tx());
  EXPECT_EQ(100, memtable->get_rec_scn().get_val_for_tx());
  EXPECT_TRUE(memtable->get_end_scn().is_max());
  int64_t removed_cnt_tx_2;
  EXPECT_EQ(OB_SUCCESS, sync_txn_log_fail(tx_2, scope_tx_2_2, palf_applied_scn, removed_cnt_tx_2));
  EXPECT_EQ(removed_cnt_tx_1, 5);
  EXPECT_EQ(removed_cnt_tx_2, 5);
  EXPECT_EQ(250, memtable->get_max_end_scn().get_val_for_tx());
  EXPECT_EQ(100, memtable->get_rec_scn().get_val_for_tx());
  EXPECT_TRUE(memtable->get_end_scn().is_max());
  TRANS_LOG(INFO, "######## all log sync fail, memtable is empty, adjust memtable's right boundary");
  palf_applied_scn = 50;
  EXPECT_EQ(OB_SUCCESS, sync_txn_log_fail(tx_1, scope_tx_1_1, palf_applied_scn, removed_cnt_tx_1));
  EXPECT_EQ(50, memtable->get_max_end_scn().get_val_for_tx());
  EXPECT_EQ(100, memtable->get_rec_scn().get_val_for_tx());
  EXPECT_TRUE(memtable->get_end_scn().is_max());
  EXPECT_EQ(OB_SUCCESS, sync_txn_log_fail(tx_2, scope_tx_2_1, palf_applied_scn, removed_cnt_tx_2));
  EXPECT_EQ(removed_cnt_tx_1, 5);
  EXPECT_EQ(removed_cnt_tx_2, 5);
  EXPECT_EQ(50, memtable->get_max_end_scn().get_val_for_tx());
  EXPECT_EQ(100, memtable->get_rec_scn().get_val_for_tx());
  EXPECT_TRUE(memtable->get_end_scn().is_max());
  abort_txn(tx_1, true);
  abort_txn(tx_2, true);
  memtable->destroy();
}


} // namespace unittest

namespace storage
{
int ObTxDataTable::alloc_undo_status_node(ObUndoStatusNode *&undo_status_node)
{
  int ret = OB_SUCCESS;

  undo_status_node = new ObUndoStatusNode();

  return ret;
}

int ObTxCtxTable::acquire_ref_(const ObLSID& ls_id)
{
  int ret = OB_SUCCESS;

  ls_tx_ctx_mgr_ = &unittest::TestMemtableV2::ls_tx_ctx_mgr_;
  TRANS_LOG(INFO, "[TX_CTX_TABLE] tx ctx table acquire ref", K(ls_id), K(this));

  return ret;
}

int ObTxCtxTable::release_ref_()
{
  int ret = OB_SUCCESS;

  ls_tx_ctx_mgr_ = NULL;
  TRANS_LOG(INFO, "[TX_CTX_TABLE] tx ctx table release ref", K(this));

  return ret;
}
void ObITabletMemtable::unset_logging_blocked_for_active_memtable_()
{
}
} // namespace storage

namespace memtable{

int ObMemtable::lock_row_on_frozen_stores_(
    const storage::ObTableIterParam &,
    const ObTxNodeArg &,
    const ObMemtableKey *,
    const bool check_exist,
    storage::ObTableAccessContext &,
    ObMvccRow *,
    ObMvccWriteResult &)
{
  if (unittest::TestMemtableV2::is_sstable_contains_lock_) {
    return OB_TRY_LOCK_ROW_CONFLICT;
  } else {
    return OB_SUCCESS;
  }
}
}

namespace transaction
{
int ObLSTxCtxMgr::init(const int64_t tenant_id,
                       const ObLSID &ls_id,
                       ObTxTable *tx_table,
                       ObLockTable *lock_table,
                       ObITsMgr *ts_mgr,
                       ObTransService *txs,
                       ObITxLogParam * param,
                       ObITxLogAdapter * log_adapter)
{
  int ret = OB_SUCCESS;

  UNUSED(log_adapter);
  if (is_inited_) {
    TRANS_LOG(WARN, "ObLSTxCtxMgr inited twice");
    ret = OB_INIT_TWICE;
  } else {
    if (OB_FAIL(ls_tx_ctx_map_.init(lib::ObMemAttr(tenant_id, "LSTxCtxMgr")))) {
      TRANS_LOG(WARN, "ls_tx_ctx_map_ init fail", KR(ret));
    } else if (OB_FAIL(tx_ls_state_mgr_.init(ls_id))) {
      TRANS_LOG(WARN, "init tx_ls_state_mgr_ failed", KR(ret));
    } else if (OB_FAIL(tx_ls_state_mgr_.switch_tx_ls_state(ObTxLSStateMgr::TxLSAction::START))) {
      TRANS_LOG(WARN, "start ls_tx_ctx_mgr failed",K(ret),K(tx_ls_state_mgr_));
    } else {
      is_inited_ = true;
      tenant_id_ = tenant_id;
      ls_id_ = ls_id;
      tx_table_ = tx_table;
      lock_table_ = lock_table;
      txs_ = txs;
      ts_mgr_ = ts_mgr;
      TRANS_LOG(INFO, "ObLSTxCtxMgr inited success", KP(this), K(ls_id));
    }
  }

  return ret;
}
} // namespace transaction

} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_memtable.log*");
  OB_LOGGER.set_file_name("test_memtable.log", true, false,
                          "test_memtable.log",
                          "test_memtable.log",
                          "test_memtable.log");
  OB_LOGGER.set_log_level("INFO");
  STORAGE_LOG(INFO, "begin unittest: test simple memtable");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
