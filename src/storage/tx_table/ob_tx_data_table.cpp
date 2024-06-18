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

#include "storage/tx_table/ob_tx_data_table.h"

#include "lib/lock/ob_tc_rwlock.h"
#include "lib/time/ob_time_utility.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx/ob_tx_data_functor.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx/ob_tx_data_define.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/tx_table/ob_tx_ctx_table.h"
#include "storage/tx_table/ob_tx_table_define.h"
#include "storage/tx_table/ob_tx_data_cache.h"
#include "storage/tablet/ob_tablet.h"

#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{

using namespace oceanbase::transaction;
using namespace oceanbase::share;

namespace storage
{

int ObTxDataTable::init(ObLS *ls, ObTxCtxTable *tx_ctx_table)
{
  int ret = OB_SUCCESS;
  STATIC_ASSERT(sizeof(ObTxData) <= TX_DATA_SLICE_SIZE, "Size of ObTxData Overflow.");
  STATIC_ASSERT(sizeof(ObUndoAction) == UNDO_ACTION_SZIE, "Size of ObUndoAction Overflow.");
  STATIC_ASSERT(sizeof(ObUndoStatusNode) <= TX_DATA_SLICE_SIZE, "Size of ObUndoStatusNode Overflow");

  ObMemtableMgrHandle memtable_mgr_handle;
  if (OB_ISNULL(ls) || OB_ISNULL(tx_ctx_table)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "ls tablet service or tx ctx table is nullptr", KR(ret));
  } else if (OB_ISNULL(tx_data_allocator_ = &MTL(ObSharedMemAllocMgr*)->tx_data_allocator())) {
    ret = OB_ERR_UNEXPECTED;;
    STORAGE_LOG(WARN, "unexpected nullptr of mtl object", KR(ret), KP(tx_data_allocator_));
  } else if (FALSE_IT(ls_tablet_svr_ = ls->get_tablet_svr())) {
  } else if (OB_FAIL(ls_tablet_svr_->get_tx_data_memtable_mgr(memtable_mgr_handle))) {
    STORAGE_LOG(WARN, "get tx data memtable mgr fail.", KR(ret), K(tablet_id_));
  } else if (OB_FAIL(init_arena_allocator_())) {
    STORAGE_LOG(ERROR, "slice_allocator_ init fail");
  } else if (OB_FAIL(init_tx_data_read_schema_())) {
    STORAGE_LOG(WARN, "init tx data read ctx failed.", KR(ret), K(tablet_id_));
  } else {
    calc_upper_trans_version_cache_.commit_versions_.array_.set_attr(
      ObMemAttr(ls->get_tenant_id(), "CommitVersions"));

    ls_ = ls;
    ls_id_ = ls->get_ls_id();
    memtable_mgr_ = static_cast<ObTxDataMemtableMgr *>(memtable_mgr_handle.get_memtable_mgr());
    tx_ctx_table_ = tx_ctx_table;
    tablet_id_ = LS_TX_DATA_TABLET;
    calc_upper_trans_is_disabled_ = false;
    latest_transfer_scn_.reset();

    is_inited_ = true;
    FLOG_INFO("tx data table init success", K(sizeof(ObTxData)), K(sizeof(ObTxDataLinkNode)), KPC(this));
  }
  return ret;
}

int ObTxDataTable::init_arena_allocator_()
{
  ObMemAttr mem_attr;
  mem_attr.label_ = "TX_DATA_ARENA";
  mem_attr.tenant_id_ = MTL_ID();
  mem_attr.ctx_id_ = ObCtxIds::TX_DATA_TABLE;
  arena_allocator_.set_attr(mem_attr);
  return OB_SUCCESS;
}

int ObTxDataTable::init_tx_data_read_schema_()
{
  int ret = OB_SUCCESS;

  ObTableIterParam &iter_param = read_schema_.iter_param_;
  ObTableReadInfo &read_info = read_schema_.read_info_;
  ObRowkeyReadInfo &full_read_info = read_schema_.full_read_info_;
  common::ObSEArray<share::schema::ObColDesc, 5> columns;

  share::schema::ObColDesc key;
  key.col_id_ = TX_ID;
  key.col_type_.set_int();
  key.col_order_ = ObOrderType::ASC;

  share::schema::ObColDesc idx;
  idx.col_id_ = IDX;
  idx.col_type_.set_int();
  idx.col_order_ = ObOrderType::ASC;

  share::schema::ObColDesc trans_col;
  trans_col.col_id_ = common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID;
  trans_col.col_type_.set_int();
  trans_col.col_order_ = ObOrderType::ASC;

  share::schema::ObColDesc sequence_col;
  sequence_col.col_id_ = common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID;
  sequence_col.col_type_.set_int();
  sequence_col.col_order_ = ObOrderType::ASC;

  share::schema::ObColDesc total_row_cnt;
  total_row_cnt.col_id_ = TOTAL_ROW_CNT;
  total_row_cnt.col_type_.set_int();
  total_row_cnt.col_order_ = ObOrderType::ASC;

  share::schema::ObColDesc end_ts;
  end_ts.col_id_ = END_LOG_TS;
  end_ts.col_type_.set_int();
  end_ts.col_order_ = ObOrderType::ASC;

  share::schema::ObColDesc value;
  value.col_id_ = VALUE;
  value.col_type_.set_binary();
  value.col_order_ = ObOrderType::ASC;

  iter_param.table_id_ = 1;
  iter_param.tablet_id_ = LS_TX_DATA_TABLET;

  if (OB_FAIL(columns.push_back(key))) {
    STORAGE_LOG(WARN, "failed to push back key", KR(ret), K(key));
  } else if (OB_FAIL(columns.push_back(idx))) {
    STORAGE_LOG(WARN, "failed to push back idx", KR(ret), K(idx));
  } else if (OB_FAIL(columns.push_back(total_row_cnt))) {
    STORAGE_LOG(WARN, "failed to push back total row cnt", KR(ret), K(total_row_cnt));
  } else if (OB_FAIL(columns.push_back(end_ts))) {
    STORAGE_LOG(WARN, "failed to push back end_ts", KR(ret), K(end_ts));
  } else if (OB_FAIL(columns.push_back(value))) {
    STORAGE_LOG(WARN, "failed to push back value", KR(ret), K(value));
  } else if (OB_FAIL(read_info.init(arena_allocator_, LS_TX_DATA_SCHEMA_COLUMN_CNT,
                                    LS_TX_DATA_SCHEMA_ROWKEY_CNT, lib::is_oracle_mode(),
                                    columns, nullptr/*storage_cols_index*/))) {
    STORAGE_LOG(WARN, "Fail to init read_info", K(ret));
  } else if (OB_FAIL(full_read_info.init(arena_allocator_, LS_TX_DATA_SCHEMA_COLUMN_CNT,
                                         LS_TX_DATA_SCHEMA_ROWKEY_CNT, lib::is_oracle_mode(),
                                         columns))) {
    STORAGE_LOG(WARN, "Fail to init read_info", K(ret));
  } else {
    read_schema_.iter_param_.read_info_ = &read_info;
  }

  return ret;
}

int ObTxDataTable::start()
{
  int ret = OB_SUCCESS;
  is_started_ = true;
  return ret;
}

void ObTxDataTable::stop()
{
  is_started_ = false;
}

void ObTxDataTable::reset()
{
  tablet_id_ = 0;
  ls_ = nullptr;
  ls_tablet_svr_ = nullptr;
  memtable_mgr_ = nullptr;
  tx_ctx_table_ = nullptr;
  calc_upper_trans_version_cache_.reset();
  memtables_cache_.reuse();
  calc_upper_trans_is_disabled_ = false;
  latest_transfer_scn_.reset();
  is_started_ = false;
  is_inited_ = false;
}

int ObTxDataTable::prepare_for_safe_destroy()
{
  int ret = clean_memtables_cache_();
  LOG_INFO("tx data table prepare for safe destroy", KR(ret), K(ls_id_));
  return ret;
}

int ObTxDataTable::offline()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data table is not inited", KR(ret), KPC(this));
  } else if (OB_FAIL(get_memtable_mgr_()->offline()) && OB_NOT_INIT != ret) {
    STORAGE_LOG(WARN, "release memtables failed", KR(ret));
  } else if (OB_FAIL(clean_memtables_cache_())) {
    STORAGE_LOG(WARN, "clean memtables cache failed", KR(ret), KPC(this));
  } else {
    is_started_ = false;
    calc_upper_trans_version_cache_.reset();
  }

  return ret;
}

int ObTxDataTable::online()
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  ObTablet *tablet;
  ObLSTabletService *ls_tablet_svr = ls_->get_tablet_svr();

  if (OB_ISNULL(ls_tablet_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get ls tablet svr failed", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->get_tablet(LS_TX_DATA_TABLET,
                                               handle))) {
    LOG_WARN("get tablet failed", K(ret));
  } else if (FALSE_IT(tablet = handle.get_obj())) {
  } else if (OB_FAIL(ls_tablet_svr->create_memtable(
                 LS_TX_DATA_TABLET, 0 /* schema_version */, false /* for_inc_direct_load */, false /*for_replay*/))) {
    LOG_WARN("failed to create memtable", K(ret));
  } else {
    // load tx data table succeed
    {
      TCWLockGuard lock_guard(calc_upper_trans_version_cache_.lock_);
      calc_upper_trans_version_cache_.reset();
    }
    latest_transfer_scn_.reset();
    ATOMIC_STORE(&calc_upper_trans_is_disabled_, false);
    is_started_ = true;
  }

  return ret;
}

int ObTxDataTable::clean_memtables_cache_()
{
  int ret = OB_SUCCESS;
  TCWLockGuard guard(memtables_cache_.lock_);
  memtables_cache_.reuse();
  return ret;
}

void ObTxDataTable::destroy() { reset(); }

int ObTxDataTable::alloc_tx_data(ObTxDataGuard &tx_data_guard,
                                 const bool enable_throttle,
                                 const int64_t abs_expire_time)
{
  int ret = OB_SUCCESS;
  void *slice_ptr = nullptr;

  if (OB_ISNULL(slice_ptr = tx_data_allocator_->alloc(enable_throttle, abs_expire_time))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "allocate memory from slice_allocator fail.", KR(ret), KP(this), K(tablet_id_));
  } else {
    ObTxData *tx_data = new (slice_ptr) ObTxData();
    tx_data->tx_data_allocator_ = tx_data_allocator_;
    tx_data->op_allocator_ = &MTL(share::ObSharedMemAllocMgr*)->tx_data_op_allocator();
    tx_data_guard.init(tx_data);
  }
  return ret;
}

int ObTxDataTable::alloc_undo_status_node(ObUndoStatusNode *&undo_status_node)
{
  int ret = OB_SUCCESS;
  void *slice_ptr = nullptr;
  const int64_t abs_expire_time = THIS_WORKER.get_timeout_ts();

  if (OB_ISNULL(slice_ptr = tx_data_allocator_->alloc(true, abs_expire_time))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "allocate memory fail.", KR(ret), KP(this), K(tablet_id_), K(abs_expire_time));
  } else {
    undo_status_node = new (slice_ptr) ObUndoStatusNode();
  }
  return ret;
}

int ObTxDataTable::free_undo_status_node(ObUndoStatusNode *&undo_status_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(undo_status_node)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "trying to free nullptr", KR(ret), K(tablet_id_));
  } else {
    tx_data_allocator_->free(undo_status_node);
  }
  return ret;
}

void ObTxDataTable::free_undo_status_list_(ObUndoStatusNode *node_ptr)
{
  ObUndoStatusNode *node_to_free = nullptr;
  while (nullptr != node_ptr) {
    node_to_free = node_ptr;
    node_ptr = node_ptr->next_;
    tx_data_allocator_->free(reinterpret_cast<void *>(node_to_free));
  }
}

int ObTxDataTable::insert(ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;
  ObTxDataMemtableWriteGuard write_guard;
  ObTransID tx_id = tx_data->tx_id_;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data table is not init.", KR(ret), KP(this), KPC(tx_data), K(tablet_id_));
  } else if (OB_ISNULL(tx_data)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(ERROR, "trying to insert a null tx data.", KP(this), K(tablet_id_));
  } else if (!tx_data->is_valid_in_tx_data_table()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "trying to insert an invalid tx data into tx data table", KR(ret),
                KP(tx_data), KPC(tx_data));
  } else if (OB_FAIL(get_memtable_mgr_()->get_all_memtables_for_write(write_guard))) {
    STORAGE_LOG(WARN, "get all memtables for write fail.", KR(ret), KPC(get_memtable_mgr_()));
  } else if (OB_FAIL(insert_(tx_data, write_guard))) {
    STORAGE_LOG(WARN, "insert tx data failed.", KR(ret), KPC(tx_data), KP(this), K(tablet_id_));
  } else {
    // successfully insert
    // TODO : @gengli do not dec ref and set nullptr after insert
  }

  return ret;
}

// In order to support the commit log without undo actions, the tx data related to a single
// transaction may be inserted multiple times. For more details, see
//
int ObTxDataTable::insert_(ObTxData *&tx_data, ObTxDataMemtableWriteGuard &write_guard)
{
  int ret = OB_SUCCESS;
  bool inserted = false;
  ObTxDataMemtable *tx_data_memtable = nullptr;
  ObTableHandleV2 (&memtable_handles)[MAX_TX_DATA_MEMTABLE_CNT] = write_guard.handles_;

  for (int i = write_guard.size_ - 1; OB_SUCC(ret) && OB_NOT_NULL(tx_data) && !inserted && i >= 0; i--) {
    tx_data_memtable = nullptr;
    if (OB_FAIL(memtable_handles[i].get_tx_data_memtable(tx_data_memtable))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "get tx data memtable from table handles fail.", KR(ret), KP(this),
                  K(tablet_id_), K(memtable_handles[i]));
    } else if (OB_ISNULL(tx_data_memtable)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "tx data memtable is nullptr.", KR(ret), KP(this), K(tablet_id_),
                  K(memtable_handles[i]));
    } else if (tx_data_memtable->get_start_scn() < tx_data->end_scn_
               && tx_data_memtable->get_end_scn() >= tx_data->end_scn_) {
      if (OB_FAIL(tx_data_memtable->insert(tx_data))) {
        STORAGE_LOG(WARN,
                    "insert tx data into tx data memtable failed",
                    KR(ret),
                    K(ls_id_),
                    KPC(tx_data),
                    KPC(tx_data_memtable));
      } else {
        inserted = true;
      }
    } else {
      // should not insert into this memtable
      STORAGE_LOG(DEBUG, "skip this tx data memtable", KPC(tx_data), KPC(tx_data_memtable));
    }
  }

  // If this tx data can not be inserted into all memtables, check if it should be filtered.
  // We use the start log ts of the first memtable as the filtering time stamp
  if (OB_SUCC(ret) && !inserted && OB_NOT_NULL(tx_data_memtable)) {
    SCN clog_checkpoint_scn = tx_data_memtable->get_key().get_start_scn();
    if (tx_data->end_scn_ <= clog_checkpoint_scn) {
      // Filter this tx data. The part trans ctx need to handle this error code because the memory
      // of tx data need to be freed.
      STORAGE_LOG(DEBUG, "This tx data is filtered.", K(clog_checkpoint_scn), KPC(tx_data));
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "cannot find the correct tx data memtable to insert", KR(ret),
                  KPC(tx_data), K(clog_checkpoint_scn), K(memtable_handles));
    }
  }

  return ret;
}

int ObTxDataTable::check_with_tx_data(const ObTransID tx_id,
                                      ObITxDataCheckFunctor &fn,
                                      ObTxDataGuard &tx_data_guard,
                                      SCN &recycled_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data table is not init.", KR(ret), KP(this), K(tx_id));
  } else if (OB_SUCC(check_tx_data_in_memtable_(tx_id, fn, tx_data_guard))) {
    // successfully do check function in memtable, check done
    STORAGE_LOG(DEBUG, "tx data table check with tx memtable data succeed", K(tx_id), K(fn));
  } else if (OB_TRANS_CTX_NOT_EXIST == ret && OB_SUCC(check_tx_data_in_sstable_(tx_id, fn, tx_data_guard, recycled_scn))) {
    // successfully do check function in sstable
    STORAGE_LOG(DEBUG, "tx data table check with tx sstable data succeed", K(tx_id), K(fn));
  } else {
    STORAGE_LOG(WARN, "check something in tx data fail.", KR(ret), K(tx_id), KP(this),
                K(tablet_id_));
  }

  if (OB_SUCC(ret) &&
      OB_NOT_NULL(tx_data_guard.tx_data()) &&
      !fn.may_exist_undecided_state_in_tx_data_table() &&
      (ObTxData::RUNNING == tx_data_guard.tx_data()->state_)) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "read a running state tx data from tx data table, need retry", KR(ret), K(tx_data_guard));
  }
  return ret;
}

int ObTxDataTable::check_tx_data_in_memtable_(const ObTransID tx_id, ObITxDataCheckFunctor &fn, ObTxDataGuard &tx_data_guard)
{
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    if (OB_FAIL(check_tx_data_with_cache_once_(tx_id, fn, tx_data_guard))) {
      if (OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(update_memtables_cache())) {
          STORAGE_LOG(WARN, "update memtables cache failed", KR(ret));
        } else {
          // do check_tx_data_with_cache_once_ again
        }
      } else if (OB_TRANS_CTX_NOT_EXIST == ret) {
        // need check tx data in sstable
      } else {
        STORAGE_LOG(WARN, "check tx data with cache failed", KR(ret));
      }
    } else {
      // check tx data with cache succeed
      break;
    }
  }

  return ret;
}

int ObTxDataTable::check_tx_data_with_cache_once_(const transaction::ObTransID tx_id, ObITxDataCheckFunctor &fn, ObTxDataGuard &tx_data_guard)
{
  int ret = OB_SUCCESS;

  bool find = false;

  if (OB_FAIL(get_tx_data_from_cache_(tx_id, tx_data_guard, find))) {
    if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "get tx data from cache failed", KR(ret));
    }
  } else {
    if (find) {
      if (ObTxData::RUNNING == tx_data_guard.tx_data()->state_ &&
          !fn.may_exist_undecided_state_in_tx_data_table()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR,
                    "read a running state tx data from tx data table",
                    KR(ret),
                    K(tx_data_guard),
                    KPC(tx_data_guard.tx_data()));
      } else {
        EVENT_INC(ObStatEventIds::TX_DATA_READ_TX_DATA_MEMTABLE_COUNT);
        ret = fn(*tx_data_guard.tx_data());
      }
    } else {
      int64_t memtable_head = -1;
      int64_t memtable_tail = -1;
      if (OB_FAIL(get_memtable_mgr_()->get_memtable_range(memtable_head, memtable_tail))) {
        STORAGE_LOG(WARN, "get memtable range failed", KR(ret));
      } else if (memtable_head != memtables_cache_.memtable_head_ || memtable_tail != memtables_cache_.memtable_tail_) {
        ret = OB_EAGAIN;
      } else {
        ret = OB_TRANS_CTX_NOT_EXIST;
      }
    }
  }
  return ret;
}

int ObTxDataTable::get_tx_data_from_cache_(const transaction::ObTransID tx_id, ObTxDataGuard &tx_data_guard, bool &find)
{
  int ret = OB_SUCCESS;

  TCRLockGuard guard(memtables_cache_.lock_);
  ObTableHdlArray &memtable_handles = memtables_cache_.memtable_handles_;
  int memtable_handle_tail = memtable_handles.count() - 1;

  for (int i = memtable_handle_tail ; OB_SUCC(ret) && !find && i >= 0; i--) {
    ObTxDataMemtable *tx_data_memtable = nullptr;
    if (OB_FAIL(memtable_handles.at(i).get_tx_data_memtable(tx_data_memtable))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "get tx data memtable from table handles fail.", KR(ret), K(tx_id), K(memtable_handles.at(i)));
    } else if (OB_ISNULL(tx_data_memtable)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "tx data memtable is nullptr.", KR(ret), K(tx_id));
    } else if (i == memtable_handle_tail && ObTxDataMemtable::State::ACTIVE != tx_data_memtable->get_state()) {
      // the latest memtable is not active, update memtable handles cache
      ret = OB_EAGAIN;
    } else {
      int tmp_ret = tx_data_memtable->get_tx_data(tx_id, tx_data_guard);
      if (OB_SUCCESS == tmp_ret) {
        find = true;
      } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
        // search tx_id in next memtable
      } else {
        STORAGE_LOG(WARN,
            "get_tx_data fail.",
            KR(tmp_ret),
            KP(this),
            K(tablet_id_),
            KP(tx_data_memtable),
            KPC(tx_data_memtable));
      }
    }
  }

  return ret;
}

int ObTxDataTable::update_memtables_cache()
{
  int ret = OB_SUCCESS;
  bool need_update = true;

  // lock for updating memtables cache
  TCWLockGuard guard(memtables_cache_.lock_);
  if (OB_FAIL(check_need_update_memtables_cache_(need_update))) {
    STORAGE_LOG(WARN, "check if memtable handles need update failed.", KR(ret));
  } else if (!need_update) {
    // do not need update cache, skip update
  } else if (FALSE_IT(memtables_cache_.reuse())) {
  } else if (OB_FAIL(get_memtable_mgr_()->get_all_memtables_with_range(memtables_cache_.memtable_handles_,
                                                                       memtables_cache_.memtable_head_,
                                                                       memtables_cache_.memtable_tail_))) {
    STORAGE_LOG(WARN, "get all memtables with range failed.", KR(ret), KPC(this), KPC(get_memtable_mgr_()));
  }

  return ret;
}

int ObTxDataTable::check_need_update_memtables_cache_(bool &need_update)
{
  int ret = OB_SUCCESS;
  int64_t memtable_head = -1;
  int64_t memtable_tail = -1;
  if (OB_FAIL(get_memtable_mgr_()->get_memtable_range(memtable_head, memtable_tail))) {
    STORAGE_LOG(WARN, "get memtable range failed.", KR(ret));
  } else if (memtables_cache_.memtable_head_ == memtable_head && memtables_cache_.memtable_tail_ == memtable_tail) {
    // cache already up to date, skip update
    need_update = false;
  }

  return ret;
}

// To avoid memtables_cache_ being holded by tx data table for a long time, tx_data_memtable_mgr will call this function
// after periodically flushing.
void ObTxDataTable::reuse_memtable_handles_cache()
{
  TCWLockGuard guard(memtables_cache_.lock_);
  memtables_cache_.reuse();
}

int ObTxDataTable::get_tx_data_in_memtables_cache_(const ObTransID tx_id,
                                                   ObTableHandleV2 &src_memtable_handle,
                                                   ObTxDataGuard &tx_data_guard,
                                                   bool &find)
{
  int ret = OB_SUCCESS;
  // lock for reading memtables cache
  TCRLockGuard guard(memtables_cache_.lock_);
  ObTxDataMemtable *tx_data_memtable = nullptr;
  ObTableHdlArray &memtable_handles = memtables_cache_.memtable_handles_;

  for (int i = memtable_handles.count() - 1; OB_SUCC(ret) && !find && i >= 0; i--) {
    tx_data_memtable = nullptr;
    if (OB_FAIL(memtable_handles.at(i).get_tx_data_memtable(tx_data_memtable))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "get tx data memtable from table handles fail.", KR(ret), K(tx_id), K(memtable_handles.at(i)));
    } else if (OB_ISNULL(tx_data_memtable)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "tx data memtable is nullptr.", KR(ret), K(tx_id));
    } else if (ObTxDataMemtable::State::RELEASED == tx_data_memtable->get_state()) {
      // skip get tx data in this tx data memtable
    } else {
      int tmp_ret = tx_data_memtable->get_tx_data(tx_id, tx_data_guard);
      if (OB_SUCCESS == tmp_ret) {
        find = true;
        src_memtable_handle = memtable_handles.at(i);
      } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
        // search tx_id in next memtable
      } else {
        STORAGE_LOG(WARN,
            "get_tx_data fail.",
            KR(tmp_ret),
            KP(this),
            K(tablet_id_),
            KP(tx_data_memtable),
            KPC(tx_data_memtable));
      }
    }
  }

  return ret;
}

int ObTxDataTable::check_tx_data_in_sstable_(const ObTransID tx_id,
                                             ObITxDataCheckFunctor &fn,
                                             ObTxDataGuard &tx_data_guard,
                                             SCN &recycled_scn)
{
  int ret = OB_SUCCESS;
  tx_data_guard.reset();

  if (OB_FAIL(alloc_tx_data(tx_data_guard, false/* enable_throttle */))) {
    STORAGE_LOG(WARN, "allocate tx data to read from sstable failed", KR(ret), K(tx_data_guard));
  } else if (OB_ISNULL(tx_data_guard.tx_data())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx data is unexpected null", KR(ret), K(tx_data_guard));
  } else if (OB_FAIL(get_tx_data_in_sstable_(tx_id, *tx_data_guard.tx_data(), recycled_scn))) {
    STORAGE_LOG(WARN, "get tx data from sstable failed.", KR(ret), K(tx_id));
  } else {
    EVENT_INC(ObStatEventIds::TX_DATA_READ_TX_DATA_SSTABLE_COUNT);
    if (OB_FAIL(fn(*tx_data_guard.tx_data()))) {
      STORAGE_LOG(WARN, "check tx data in sstable failed.", KR(ret), KP(this),
                  K(tablet_id_));
    }
  }

  if (OB_FAIL(ret)) {
    tx_data_guard.reset();
  }
  return ret;
}

int ObTxDataTable::get_tx_data_in_sstable_(const transaction::ObTransID tx_id, ObTxData &tx_data, share::SCN &recycled_scn)
{
  int ret = OB_SUCCESS;
  const ObTableIterParam &iter_param = read_schema_.iter_param_;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObTabletHandle tablet_handle;

  if (FALSE_IT(tx_data.tx_id_ = tx_id)) {
  } else if (OB_FAIL(ls_tablet_svr_->get_tablet(tablet_id_, tablet_handle))) {
    STORAGE_LOG(WARN, "get tablet from ls tablet service fail.", KR(ret), KP(this), K(tablet_id_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle), K(tablet_id_));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &sstables = table_store_wrapper.get_member()->get_minor_sstables();
    ObTxDataSingleRowGetter getter(iter_param, sstables, *tx_data_allocator_, recycled_scn);
    if (OB_FAIL(getter.init(tx_id))) {
      STORAGE_LOG(WARN, "init ObTxDataSingleRowGetter fail.", KR(ret), KP(this), K(tablet_id_));
    } else if (OB_FAIL(getter.get_next_row(tx_data))) {
      if (OB_ITER_END == ret) {
        ret = OB_TRANS_CTX_NOT_EXIST;
      }
      STORAGE_LOG(WARN, "get tx data in sstable failed.", KR(ret), KP(this), K(tablet_id_));
    } else {
      // get tx data from sstable succeed.
    }
  }
  return ret;
}

int ObTxDataTable::get_recycle_scn(SCN &recycle_scn)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObLSTabletIterator iterator(ObMDSGetTabletMode::READ_READABLE_COMMITED);
  ObMigrationStatus migration_status;
  share::ObLSRestoreStatus restore_status;
  ObTimeGuard tg("get recycle scn", 10L * 1000L * 1000L /* 10 seconds */);
  SCN min_end_scn = SCN::max_scn();
  SCN min_end_scn_from_old_tablets = SCN::max_scn();
  SCN min_end_scn_from_latest_tablets = SCN::max_scn();

  // set recycle_scn = SCN::min_scn() as default which means clear nothing
  recycle_scn.set_min();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data table has not been inited", KR(ret));
  } else if (OB_FAIL(ls_->get_migration_status(migration_status))) {
    STORAGE_LOG(WARN, "get migration status failed", KR(ret), "ls_id", ls_->get_ls_id());
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status) {
    recycle_scn.set_min();
    STORAGE_LOG(INFO, "logstream is in migration state. skip recycle tx data", "ls_id", ls_->get_ls_id());
  } else if (OB_FAIL(ls_->get_restore_status(restore_status))) {
    STORAGE_LOG(WARN, "get restore status failed", KR(ret), "ls_id", ls_->get_ls_id());
  } else if (ObLSRestoreStatus::NONE != restore_status) {
    recycle_scn.set_min();
    STORAGE_LOG(INFO, "logstream is in restore state. skip recycle tx data", "ls_id", ls_->get_ls_id());
  } else if (FALSE_IT(tg.click("iterate tablets start"))) {
  } else if (OB_FAIL(ls_tablet_svr_->get_ls_min_end_scn(min_end_scn_from_latest_tablets,
                                                        min_end_scn_from_old_tablets))) {
    STORAGE_LOG(WARN, "fail to get ls min end log ts", KR(ret));
  } else if (FALSE_IT(tg.click("iterate tablets finish"))) {
  } else {
    min_end_scn = std::min(min_end_scn_from_old_tablets, min_end_scn_from_latest_tablets);
    if (!min_end_scn.is_max()) {
      recycle_scn = min_end_scn;
      //Regardless of whether the primary or standby tenant is unified, refer to GTS.
      //If the tenant role in memory is deferred,
      //it may cause the standby tenant to commit and recycle when the primary is switched to standby.
      SCN snapshot_version;
      MonotonicTs unused_ts(0);
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(OB_TS_MGR.get_gts(MTL_ID(), MonotonicTs(1), NULL, snapshot_version, unused_ts))) {
        LOG_WARN("failed to get snapshot version", K(tmp_ret), K(MTL_ID()));
        // recycle nothing this time
        recycle_scn.set_min();
      } else {
        recycle_scn = std::min(recycle_scn, snapshot_version);
      }
    }
  }

  FLOG_INFO("get tx data recycle scn finish.",
            KR(ret),
            "ls_id", ls_->get_ls_id(),
            K(recycle_scn),
            K(min_end_scn_from_old_tablets),
            K(min_end_scn_from_latest_tablets));

  return ret;
}

// This task is used to do freeze by tx data table itself. The following conditions are set to
// trigger self freeze:
// 1. Tx data memtable uses more than TX_DATA_FREEZE_TRIGGER_MAX_PERCENTAGE% memory in total tenant
// memory.
// 2. The tenant remain memory is than (1 - freeze_trigger_percentage) and the tx data uses more
// than TX_DATA_FREEZE_TRIGGER_MIN_PERCENTAGE% memory in total tenant memory.
int ObTxDataTable::self_freeze_task()
{
  int ret = OB_SUCCESS;

  STORAGE_LOG(DEBUG, "start tx data table self freeze task", K(get_ls_id()));

  if (OB_FAIL(memtable_mgr_->flush(SCN::max_scn(), checkpoint::INVALID_TRACE_ID, true))) {
    share::ObLSID ls_id = get_ls_id();
    STORAGE_LOG(WARN, "self freeze of tx data memtable failed.", KR(ret), K(ls_id), KPC(memtable_mgr_));
  }

  STORAGE_LOG(DEBUG, "finish tx data table self freeze task", KR(ret), K(get_ls_id()));
  return ret;
}

// The main steps in calculating upper_trans_version. For more details, see :
//
int ObTxDataTable::get_upper_trans_version_before_given_scn(const SCN sstable_end_scn, SCN &upper_trans_version)
{
  int ret = OB_SUCCESS;
  bool skip_calc = false;
  upper_trans_version.set_max();

  STORAGE_LOG(DEBUG, "start get upper trans version", K(get_ls_id()));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The tx data table is not inited.", KR(ret));
  } else if (ATOMIC_LOAD(&calc_upper_trans_is_disabled_)) {
    skip_calc = true;
  } else if (true == (skip_calc = skip_this_sstable_end_scn_(sstable_end_scn))) {
    // there is a start_scn of running transactions is smaller than the sstable_end_scn
  } else {
    TCWLockGuard lock_guard(calc_upper_trans_version_cache_.lock_);
    if (OB_FAIL(update_cache_if_needed_(skip_calc))) {
      STORAGE_LOG(WARN, "update cache failed.", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (skip_calc) {
  } else {
    TCRLockGuard lock_guard(calc_upper_trans_version_cache_.lock_);
    if (0 == calc_upper_trans_version_cache_.commit_versions_.array_.count()) {
      ret = OB_EAGAIN;
      STORAGE_LOG(WARN, "empty commit versions. may be a concurrent transfer.", K(calc_upper_trans_version_cache_));
    } else if (!calc_upper_trans_version_cache_.commit_versions_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid cache for upper trans version calculation. ", KR(ret));
    } else if (OB_FAIL(calc_upper_trans_scn_(sstable_end_scn, upper_trans_version))) {
      STORAGE_LOG(WARN, "calc upper trans version failed", KR(ret), "ls_id", get_ls_id());
    } else {
      FLOG_INFO("get upper trans version finish.",
                KR(ret),
                K(skip_calc),
                K(ls_id_),
                K(sstable_end_scn),
                K(upper_trans_version));
    }
  }

  return ret;
}

int ObTxDataTable::DEBUG_slowly_calc_upper_trans_version_(const SCN &sstable_end_scn,
                                                          SCN &tmp_upper_trans_version)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator allocator;

  ObStoreCtx store_ctx;
  ObTableAccessContext access_context;

  ObQueryFlag query_flag(ObQueryFlag::Forward, false, /*is daily merge scan*/
                         false,                       /*is read multiple macro block*/
                         false, /*sys task scan, read one macro block in single io*/
                         false, /*is full row scan?*/
                         false, false);
  query_flag.disable_cache();

  common::ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;

  if (OB_FAIL(access_context.init(query_flag, store_ctx, allocator, trans_version_range))) {
    STORAGE_LOG(WARN, "failed to init access context", KR(ret));
  } else if (OB_FAIL(DEBUG_calc_with_all_sstables_(access_context, sstable_end_scn,
                                             tmp_upper_trans_version))) {
    STORAGE_LOG(WARN, "calculation upper trans version failed.", KR(ret));
  } else {
  }

  return ret;
}

int ObTxDataTable::DEBUG_calc_with_all_sstables_(ObTableAccessContext &access_context,
                                           const SCN &sstable_end_scn,
                                           SCN &tmp_upper_trans_version)
{
  int ret = OB_SUCCESS;

  const ObTableIterParam &iter_param = read_schema_.iter_param_;
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObStoreRowIterator *row_iter = nullptr;

  if (OB_FAIL(ls_tablet_svr_->get_tablet(tablet_id_, tablet_handle))) {
    STORAGE_LOG(WARN, "get tablet from ls tablet service failed.", KR(ret));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle), K(tablet_id_));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &sstables = table_store_wrapper.get_member()->get_minor_sstables();
    blocksstable::ObDatumRange whole_range;
    whole_range.set_whole_range();

    for (int i = 0; OB_SUCC(ret) && i < sstables.count(); i++) {
      if (OB_FAIL(
            sstables[i]->scan(read_schema_.iter_param_, access_context, whole_range, row_iter))) {
        STORAGE_LOG(WARN, "scan tx data sstable failed.", KR(ret), KPC(sstables[i]));
      } else if (OB_ISNULL(row_iter)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "row iter is unexpected nullptr", KR(ret), KPC(sstables[i]));
      } else if (OB_FAIL(
                   DEBUG_calc_with_row_iter_(row_iter, sstable_end_scn, tmp_upper_trans_version))) {
        STORAGE_LOG(WARN, "calculation upper trans version with row iter failed", KR(ret),
                    KPC(sstables[i]));
      } else if (OB_NOT_NULL(row_iter)) {
        row_iter->~ObStoreRowIterator();
        row_iter = nullptr;
      }
    }
  }

  return ret;
}

int ObTxDataTable::DEBUG_calc_with_row_iter_(ObStoreRowIterator *row_iter,
                                       const SCN &sstable_end_scn,
                                       SCN &tmp_upper_trans_version)
{
  int ret = OB_SUCCESS;
  ObTxData tx_data;
  const blocksstable::ObDatumRow *row = nullptr;

  while (OB_SUCC(ret)) {
    tx_data.reset();
    row = nullptr;
    if (OB_FAIL(row_iter->get_next_row(row))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "get next row from tx data sstable failed.", KR(ret), KPC(this));
      }
    } else if (INT64_MAX == row->storage_datums_[TX_DATA_ID_COLUMN].get_int()) {
      // skip
    } else {
      int64_t pos = 0;
      const ObString &str = row->storage_datums_[TX_DATA_VAL_COLUMN].get_string();

      if (OB_FAIL(tx_data.deserialize(str.ptr(), str.length(), pos, *tx_data_allocator_))) {
        STORAGE_LOG(WARN, "deserialize tx data from store row fail.", KR(ret), K(*row), KPHEX(str.ptr(), str.length()));
      } else if (tx_data.start_scn_ <= sstable_end_scn
                 && tx_data.commit_version_ > tmp_upper_trans_version) {
        // FLOG_INFO("update tmp upper trans version", K(tmp_upper_trans_version), K(tx_data));
        tmp_upper_trans_version = tx_data.commit_version_;
      }
    }
    if (tx_data.op_guard_.is_valid() && OB_NOT_NULL(tx_data.op_guard_->get_undo_status_list().head_)) {
      free_undo_status_list_(tx_data.op_guard_->get_undo_status_list().head_);
      tx_data.op_guard_->get_undo_status_list().head_ = nullptr;
    }

  }

  if (OB_ITER_END == ret) {
    // iterate end
    ret = OB_SUCCESS;
  }
  return ret;
}

bool ObTxDataTable::skip_this_sstable_end_scn_(const SCN &sstable_end_scn)
{
  int ret = OB_SUCCESS;
  bool need_skip = false;
  SCN max_decided_scn = SCN::min_scn();
  SCN min_start_scn_in_ctx = SCN::min_scn();
  SCN effective_scn = SCN::min_scn();
  SCN min_start_scn_in_tx_data_memtable = SCN::max_scn();

  // make sure the max decided log ts is greater than sstable_end_scn
  if (OB_FAIL(ls_->get_max_decided_scn(max_decided_scn))) {
    need_skip = true;
    STORAGE_LOG(WARN, "get max decided log ts failed", KR(ret), K(ls_id_).id());
  }

  // check if the min_start_scn_in_ctx is larger than sstable_end_scn
  if (need_skip) {
  } else if (OB_FAIL(check_min_start_in_ctx_(
                 sstable_end_scn, max_decided_scn, min_start_scn_in_ctx, effective_scn, need_skip))) {
    need_skip = true;
    STORAGE_LOG(WARN, "check min start in ctx failed", KR(ret), KP(this), K(sstable_end_scn));
  }

  if (need_skip) {
  } else if (OB_FAIL(check_min_start_in_tx_data_(sstable_end_scn, min_start_scn_in_tx_data_memtable, need_skip))) {
    need_skip = true;
    STORAGE_LOG(WARN, "check min start in tx data failed", KR(ret), KP(this), K(sstable_end_scn));
  }

  if (!need_skip) {
    STORAGE_LOG(INFO,
                "do calculate upper trans version.",
                K(need_skip),
                K(sstable_end_scn),
                K(max_decided_scn),
                K(min_start_scn_in_ctx),
                K(effective_scn),
                K(min_start_scn_in_tx_data_memtable));
  } else {
    STORAGE_LOG(TRACE,
                "do calculate upper trans version.",
                K(need_skip),
                K(sstable_end_scn),
                K(max_decided_scn),
                K(min_start_scn_in_ctx),
                K(effective_scn),
                K(min_start_scn_in_tx_data_memtable));
  }

  return need_skip;
}

int ObTxDataTable::check_min_start_in_ctx_(const SCN &sstable_end_scn,
                                           const SCN &max_decided_scn,
                                           SCN &min_start_scn,
                                           SCN &effective_scn,
                                           bool &need_skip)
{
  int ret = OB_SUCCESS;
  min_start_scn.set_min();
  effective_scn.set_min();

  if (OB_FAIL(ls_->get_uncommitted_tx_min_start_scn(min_start_scn, effective_scn))) {
    need_skip = true;
    STORAGE_LOG(DEBUG, "get uncommited tx min_start_scn failed", KR(ret), K(sstable_end_scn), K(max_decided_scn));
  } else if (min_start_scn <= sstable_end_scn || max_decided_scn <= effective_scn ||
             (latest_transfer_scn_.is_valid() && effective_scn < latest_transfer_scn_)) {
    need_skip = true;
    STORAGE_LOG(DEBUG,
                "skip calculate upper_trans_version",
                K(sstable_end_scn),
                K(max_decided_scn),
                K(min_start_scn),
                K(effective_scn),
                K(latest_transfer_scn_),
                K(need_skip));
  } else {
    // there is no ctx whose start_scn less than sstable_end_scn
  }

  return ret;
}

int ObTxDataTable::check_min_start_in_tx_data_(const SCN &sstable_end_scn,
                                               SCN &min_start_scn_in_tx_data_memtable,
                                               bool &need_skip)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_memtables_cache())) {
    STORAGE_LOG(WARN, "update memtables fail.", KR(ret));
    // something wrong happend, skip calculation
    need_skip = true;
  } else {
    TCRLockGuard guard(memtables_cache_.lock_);
    ObTxDataMemtable *tx_data_memtable = nullptr;
    ObTableHdlArray &memtable_handles = memtables_cache_.memtable_handles_;
    for (int i = memtable_handles.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      tx_data_memtable = nullptr;
      if (OB_FAIL(memtable_handles.at(i).get_tx_data_memtable(tx_data_memtable))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR,
                    "get tx data memtable from table handles fail.",
                    KR(ret),
                    KP(this),
                    K(tablet_id_),
                    K(memtable_handles.at(i)));
      } else if (OB_ISNULL(tx_data_memtable)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "tx data memtable is nullptr.", KR(ret), KP(this), K(tablet_id_), K(memtable_handles.at(i)));
      } else if (FALSE_IT(min_start_scn_in_tx_data_memtable =
                              std::min(min_start_scn_in_tx_data_memtable, tx_data_memtable->get_min_start_scn()))) {
      } else if (sstable_end_scn >= min_start_scn_in_tx_data_memtable) {
        // there is a min_start_scn in tx_data_memtable less than sstable_end_scn, skip this calculation
        need_skip = true;
        STORAGE_LOG(DEBUG, "skip calculate upper_trans_version", K(ret), K(sstable_end_scn), KPC(tx_data_memtable));
        break;
      }
    }
  }

  return ret;
}

int ObTxDataTable::deep_copy_tx_data(const ObTxDataGuard &in_tx_data, ObTxDataGuard &out_tx_data)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(in_tx_data);
  UNUSED(out_tx_data);
  return ret;
}

int ObTxDataTable::update_cache_if_needed_(bool &skip_calc)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(ls_tablet_svr_->get_tablet(LS_TX_DATA_TABLET, tablet_handle))) {
    STORAGE_LOG(WARN, "get tablet from ls tablet service failed.", KR(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    ObITable *table =table_store_wrapper.get_member()->get_minor_sstables().get_boundary_table(true /*is_last*/);

    if (nullptr == table) {
      skip_calc = true;
    } else if (!calc_upper_trans_version_cache_.is_inited_ ||
               calc_upper_trans_version_cache_.cache_version_ < table->get_end_scn()) {
      ret = update_calc_upper_trans_version_cache_(table);
    } else if (calc_upper_trans_version_cache_.cache_version_ > table->get_end_scn()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR,
                  "the end scn of the latest sstable is unexpected smaller",
                  KR(ret),
                  KPC(tablet_handle.get_obj()),
                  KPC(table));
    }
  }

  return ret;
}

int ObTxDataTable::update_calc_upper_trans_version_cache_(ObITable *table)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(DEBUG, "update calc upper trans version cache once.");
  const ObTableIterParam &iter_param = read_schema_.iter_param_;
  ObTabletHandle tablet_handle;

  if (OB_FAIL(ls_tablet_svr_->get_tablet(tablet_id_, tablet_handle))) {
    STORAGE_LOG(WARN, "get tablet from ls tablet service fail.", KR(ret), KP(this), K(tablet_id_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle), K(tablet_id_));
  } else {
    ObStorageMetaHandle sstable_handle;
    ObSSTable *sstable = static_cast<ObSSTable *>(table);
    if (sstable->is_loaded()) {
    } else if (OB_FAIL(ObTabletTableStore::load_sstable(sstable->get_addr(), sstable->is_co_sstable(), sstable_handle))) {
      STORAGE_LOG(WARN, "fail to load sstable", K(ret), KPC(sstable));
    } else if (OB_FAIL(sstable_handle.get_sstable(sstable))) {
      STORAGE_LOG(WARN, "fail to get sstable", K(ret), K(sstable_handle));
    }
    if (OB_SUCC(ret)) {
      ObCommitVersionsGetter getter(iter_param, sstable);
      if (OB_FAIL(getter.get_next_row(calc_upper_trans_version_cache_.commit_versions_))) {
        STORAGE_LOG(WARN, "update calc_upper_trans_trans_version_cache failed.", KR(ret), KPC(sstable));
      } else {
        calc_upper_trans_version_cache_.is_inited_ = true;
        calc_upper_trans_version_cache_.cache_version_ = table->get_end_scn();
        // update calc_upper_trans_scn_cache succeed.
      }
    }
  }
  return ret;
}

int ObTxDataTable::calc_upper_trans_scn_(const SCN sstable_end_scn, SCN &upper_trans_version)
{
  int ret = OB_SUCCESS;

  const ObIArray<ObCommitVersionsArray::Node> &array = calc_upper_trans_version_cache_.commit_versions_.array_;
  int l = 0;
  int r = array.count() - 1;

  // Binary find the first start_scn that is greater than or equal to sstable_end_scn
  while (l < r) {
    int mid = (l + r) >> 1;
    if (array.at(mid).start_scn_ < sstable_end_scn) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }

  // Check if the start_scn is greater than or equal to the sstable_end_scn. If not, delay the
  // upper_trans_version calculation to the next time.
  if (0 == array.count() || !array.at(l).commit_version_.is_valid()) {
    upper_trans_version.set_max();
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected array count or commit version", KR(ret), K(array.count()), K(array.at(l)));
  } else {
    upper_trans_version = array.at(l).commit_version_;
  }

  STORAGE_LOG(INFO,
              "calculate upper trans version finish",
              K(sstable_end_scn),
              K(upper_trans_version),
              K(calc_upper_trans_version_cache_),
              K(ls_id_),
              "array_count", array.count(),
              "chose_idx", l);

  return ret;
}

int ObTxDataTable::supplement_tx_op_if_exist(ObTxData *tx_data)
{
  int ret = OB_SUCCESS;
  ObTxData tx_data_from_sstable;
  tx_data_from_sstable.reset();
  SCN unused_scn;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data table is not init", KR(ret), KP(this));
  } else if (OB_ISNULL(tx_data)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "tx data is nullptr", KR(ret), KP(this));
  } else if (FALSE_IT(tx_data_from_sstable.tx_data_allocator_ = &MTL(share::ObSharedMemAllocMgr*)->tx_data_allocator())) {
  } else if (FALSE_IT(tx_data_from_sstable.op_allocator_ = &MTL(share::ObSharedMemAllocMgr*)->tx_data_op_allocator())) {
  } else if (OB_FAIL(get_tx_data_in_sstable_(tx_data->tx_id_, tx_data_from_sstable, unused_scn))) {
    if (ret == OB_TRANS_CTX_NOT_EXIST) {
      // This transaction does not have undo actions
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "get tx data from sstable failed.", KR(ret));
    }
  } else if (FALSE_IT(*tx_data = tx_data_from_sstable)) {
  }
  tx_data_from_sstable.tx_data_allocator_ = nullptr;
  tx_data_from_sstable.reset();
  return ret;
}

int ObTxDataTable::get_start_tx_scn(SCN &start_tx_scn)
{
  int ret = OB_SUCCESS;
  start_tx_scn.set_max();
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTableMetaHandle meta_handle;
  ObSSTable *oldest_minor_sstable = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data table is not init", KR(ret), KPC(this));
  } else if (OB_FAIL(ls_tablet_svr_->get_tablet(tablet_id_, tablet_handle))) {
    STORAGE_LOG(WARN, "get tablet from ls tablet service failed.", KR(ret));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tablet is nullptr.", KR(ret), KP(this), K(tablet_id_));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_ISNULL(oldest_minor_sstable = static_cast<ObSSTable *>(
      table_store_wrapper.get_member()->get_minor_sstables().get_boundary_table(false /*is_last*/)))) {
    start_tx_scn.set_max();
    STORAGE_LOG(INFO, "this logstream do not have tx data sstable", K(start_tx_scn), K(get_ls_id()), KPC(tablet));
  } else if (OB_FAIL(oldest_minor_sstable->get_meta(meta_handle))) {
    LOG_WARN("fail to get sstable meta", K(ret));
  } else if (FALSE_IT(start_tx_scn = meta_handle.get_sstable_meta().get_filled_tx_scn())) {
  } else if (start_tx_scn.is_max()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "start_tx_scn is unexpected INT64_MAX", KR(ret), KPC(tablet), KPC(oldest_minor_sstable));
  } else {
    FLOG_INFO("get start tx scn done", KR(ret), K(start_tx_scn), KPC(oldest_minor_sstable));
  }
  return ret;
}

void ObTxDataTable::disable_upper_trans_calculation()
{
  ATOMIC_STORE(&calc_upper_trans_is_disabled_, true);
  {
    TCWLockGuard lock_guard(calc_upper_trans_version_cache_.lock_);
    calc_upper_trans_version_cache_.reset();
  }
}

void ObTxDataTable::enable_upper_trans_calculation(const share::SCN latest_transfer_scn)
{
  {
    TCWLockGuard lock_guard(calc_upper_trans_version_cache_.lock_);
    calc_upper_trans_version_cache_.reset();
  }
  if (latest_transfer_scn_.is_valid()) {
    latest_transfer_scn_ = SCN::max(latest_transfer_scn, latest_transfer_scn_);
  } else {
    latest_transfer_scn_ = latest_transfer_scn;
  }
  ATOMIC_STORE(&calc_upper_trans_is_disabled_, false);
}

int ObTxDataTable::dump_single_tx_data_2_text(const int64_t tx_id_int, FILE *fd)
{
  int ret = OB_SUCCESS;
  const ObTransID tx_id(tx_id_int);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data table is not init.", KR(ret), KP(this), K(tx_id));
  } else if (OB_SUCC(dump_tx_data_in_memtable_2_text_(tx_id, fd))) {
    // successfully do check function in memtable, check done
    STORAGE_LOG(DEBUG, "tx data table check with tx memtable data succeed", K(tx_id));
  } else if (OB_TRANS_CTX_NOT_EXIST == ret && OB_SUCC(dump_tx_data_in_sstable_2_text_(tx_id, fd))) {
    // successfully do check function in sstable
    STORAGE_LOG(DEBUG, "tx data table check with tx sstable data succeed", K(tx_id));
  } else {
    STORAGE_LOG(WARN, "check something in tx data fail.", KR(ret), K(tx_id), KP(this),
                K(tablet_id_));
  }

  return ret;
}

int ObTxDataTable::dump_tx_data_in_memtable_2_text_(const ObTransID tx_id, FILE *fd)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_memtables_cache())) {
    STORAGE_LOG(WARN, "get all memtables fail.", KR(ret), K(tx_id));
  } else {
    ObTableHandleV2 src_memtable_handle;
    ObTxDataGuard tx_data_guard;
    tx_data_guard.reset();
    bool find = false;

    if (OB_FAIL(get_tx_data_in_memtables_cache_(tx_id, src_memtable_handle, tx_data_guard, find))) {
      STORAGE_LOG(INFO, "get tx data in memtables cache failed.", KR(ret), K(tx_id));
    } else if (find) {
      fprintf(fd, "********** Tx Data MemTable ***********\n\n");
      tx_data_guard.tx_data()->dump_2_text(fd);
      fprintf(fd, "\n********** Tx Data MemTable ***********\n");
    } else {
      ret = OB_TRANS_CTX_NOT_EXIST;
    }
  }
  return ret;
}

int ObTxDataTable::dump_tx_data_in_sstable_2_text_(const ObTransID tx_id, FILE *fd)
{
  int ret = OB_SUCCESS;
  ObTxData tx_data;
  tx_data.reset();
  SCN unused_scn;

  if (OB_FAIL(get_tx_data_in_sstable_(tx_id, tx_data, unused_scn))) {
    STORAGE_LOG(WARN, "get tx data from sstable failed.", KR(ret), K(tx_id));
  } else {
    fprintf(fd, "********** Tx Data SSTable ***********\n\n");
    tx_data.dump_2_text(fd);
    fprintf(fd, "\n********** Tx Data SSTable ***********\n");
  }
  return ret;
}

share::ObLSID ObTxDataTable::get_ls_id() { return ls_id_; }

}  // namespace storage

}  // namespace oceanbase

#undef USING_LOG_PREFIX
