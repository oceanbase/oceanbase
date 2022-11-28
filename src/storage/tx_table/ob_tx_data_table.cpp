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
#include "share/rc/ob_tenant_base.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx/ob_tx_data_functor.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "storage/tx_table/ob_tx_ctx_table.h"
#include "storage/tx_table/ob_tx_table_define.h"

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

  STATIC_ASSERT(sizeof(ObTxData) <= TX_DATA_SIZE, "Size of ObTxData Overflow.");
  STATIC_ASSERT(sizeof(TxDataHashNode) <= TX_DATA_HASH_NODE_SIZE,
                "Size of TxDataHashNode Overflow.");
  STATIC_ASSERT(sizeof(ObTxDataSortListNode) <= TX_DATA_SORT_LIST_NODE_SIZE,
                "Size of ObTxDataSortListNode Overflow.");
  STATIC_ASSERT(TX_DATA_SIZE + TX_DATA_HASH_NODE_SIZE + TX_DATA_SORT_LIST_NODE_SIZE
                  <= TX_DATA_SLICE_SIZE,
                "Size of ObTxDataSortListNode Overflow.");

  mem_attr_.label_ = "TX_DATA_TABLE";
  mem_attr_.tenant_id_ = MTL_ID();
  mem_attr_.ctx_id_ = ObCtxIds::DEFAULT_CTX_ID;
  ObMemtableMgrHandle memtable_mgr_handle;
  if (OB_ISNULL(ls) || OB_ISNULL(tx_ctx_table)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "ls tablet service or tx ctx table is nullptr", KR(ret));
  } else if (OB_FAIL(slice_allocator_.init(TX_DATA_SLICE_SIZE, OB_MALLOC_NORMAL_BLOCK_SIZE,
                                           common::default_blk_alloc, mem_attr_))) {
    STORAGE_LOG(ERROR, "slice_allocator_ init fail");
  } else if (FALSE_IT(ls_tablet_svr_ = ls->get_tablet_svr())) {
  } else if (OB_FAIL(ls_tablet_svr_->get_tx_data_memtable_mgr(memtable_mgr_handle))) {
    STORAGE_LOG(WARN, "get tx data memtable mgr fail.", KR(ret), K(tablet_id_));
  } else if (FALSE_IT(arena_allocator_.set_attr(mem_attr_))) {
  } else if (OB_FAIL(init_tx_data_read_schema_())) {
    STORAGE_LOG(WARN, "init tx data read ctx failed.", KR(ret), K(tablet_id_));
  } else {
    slice_allocator_.set_nway(ObTxDataTable::TX_DATA_MAX_CONCURRENCY);

    ls_ = ls;
    memtable_mgr_ = static_cast<ObTxDataMemtableMgr *>(memtable_mgr_handle.get_memtable_mgr());
    memtable_mgr_->set_slice_allocator(&slice_allocator_);
    tx_ctx_table_ = tx_ctx_table;
    tablet_id_ = LS_TX_DATA_TABLET;

    is_inited_ = true;
  }
  return ret;
}

int ObTxDataTable::init_tx_data_read_schema_()
{
  int ret = OB_SUCCESS;

  auto &iter_param = read_schema_.iter_param_;
  auto &read_info = read_schema_.read_info_;
  auto &full_read_info = read_schema_.full_read_info_;
  common::ObSEArray<share::schema::ObColDesc, 5> columns;
  common::ObSEArray<share::schema::ObColDesc, 7> full_columns;

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
  } else if (OB_FAIL(full_columns.push_back(key))) {
    STORAGE_LOG(WARN, "failed to push back key", KR(ret), K(key));
  } else if (OB_FAIL(full_columns.push_back(idx))) {
    STORAGE_LOG(WARN, "failed to push back idx", KR(ret), K(idx));
  } else if (OB_FAIL(full_columns.push_back(trans_col))) {
    STORAGE_LOG(WARN, "failed to push back trans col", KR(ret), K(key));
  } else if (OB_FAIL(full_columns.push_back(sequence_col))) {
    STORAGE_LOG(WARN, "failed to push back sequence col", KR(ret), K(key));
  } else if (OB_FAIL(full_columns.push_back(total_row_cnt))) {
    STORAGE_LOG(WARN, "failed to push back total row cnt", KR(ret), K(total_row_cnt));
  } else if (OB_FAIL(full_columns.push_back(end_ts))) {
    STORAGE_LOG(WARN, "failed to push back end ts", KR(ret), K(end_ts));
  } else if (OB_FAIL(full_columns.push_back(value))) {
    STORAGE_LOG(WARN, "failed to push back value", KR(ret), K(value));
  } else if (OB_FAIL(read_info.init(arena_allocator_, LS_TX_DATA_SCHEMA_COLUMN_CNT,
                                    LS_TX_DATA_SCHEMA_ROWKEY_CNT, lib::is_oracle_mode(),
                                    columns))) {
    STORAGE_LOG(WARN, "Fail to init read_info", K(ret));
  } else if (OB_FAIL(full_read_info.init(arena_allocator_, LS_TX_DATA_SCHEMA_COLUMN_CNT,
                                         LS_TX_DATA_SCHEMA_ROWKEY_CNT, lib::is_oracle_mode(),
                                         full_columns, true))) {
    STORAGE_LOG(WARN, "Fail to init read_info", K(ret));
  } else {
    read_schema_.iter_param_.read_info_ = &read_info;
    read_schema_.iter_param_.full_read_info_ = &full_read_info;
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
  min_start_scn_in_ctx_.reset();
  last_update_ts_ = 0;
  tablet_id_ = 0;
  ls_ = nullptr;
  ls_tablet_svr_ = nullptr;
  memtable_mgr_ = nullptr;
  tx_ctx_table_ = nullptr;
  memtables_cache_.reuse();
  slice_allocator_.purge_extra_cached_block(0);
  is_started_ = false;
  is_inited_ = false;
}

int ObTxDataTable::prepare_for_safe_destroy()
{
  return clean_memtables_cache_();
}

int ObTxDataTable::offline()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data table is not inited", KR(ret), KPC(this));
  } else if (get_memtable_mgr_()->offline()) {
    STORAGE_LOG(WARN, "release memtables failed", KR(ret));
  } else if (OB_FAIL(clean_memtables_cache_())) {
    STORAGE_LOG(WARN, "clean memtables cache failed", KR(ret), KPC(this));
  } else {
    last_update_ts_ = 0;
    min_start_scn_in_ctx_.set_min();
    calc_upper_trans_version_cache_.reset();
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

int ObTxDataTable::alloc_tx_data(ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;
  void *slice_ptr = nullptr;
  if (OB_ISNULL(slice_ptr = slice_allocator_.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "allocate memory from slice_allocator fail.", KR(ret), KP(this),
                K(tablet_id_));
  } else {
    tx_data = ObTxData::get_tx_data_by_hash_node(reinterpret_cast<TxDataHashNode *>(slice_ptr));
    // construct ObTxData()
    new (tx_data) ObTxData();
  }
  return ret;
}

int ObTxDataTable::deep_copy_tx_data(ObTxData *in_tx_data, ObTxData *&out_tx_data)
{
  int ret = OB_SUCCESS;
  void *slice_ptr = nullptr;
  if (OB_ISNULL(slice_ptr = slice_allocator_.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "allocate memory from slice_allocator fail.", KR(ret), KP(this),
                K(tablet_id_));
  } else {
    out_tx_data = ObTxData::get_tx_data_by_hash_node(reinterpret_cast<TxDataHashNode *>(slice_ptr));
    new (out_tx_data) ObTxData(*in_tx_data);
    out_tx_data->undo_status_list_.head_ = nullptr;
    if (OB_FAIL(deep_copy_undo_status_list_(in_tx_data->undo_status_list_,
                                            out_tx_data->undo_status_list_))) {
      STORAGE_LOG(WARN, "deep copy undo status list failed.");
    } else {
      // deep copy succeed.
    }
  }
  return ret;
}

int ObTxDataTable::deep_copy_undo_status_list_(ObUndoStatusList &in_list,
                                               ObUndoStatusList &out_list)
{
  int ret = OB_SUCCESS;
  ObUndoStatusNode *cur_in_node = in_list.head_;
  ObUndoStatusNode *pre_node = nullptr;
  ObUndoStatusNode *new_node = nullptr;

  while (OB_SUCC(ret) && nullptr != cur_in_node) {
    if (OB_FAIL(alloc_undo_status_node(new_node))) {
      STORAGE_LOG(WARN, "alloc undo status node failed.", KR(ret));
    } else {
      *new_node = *cur_in_node;
      if (nullptr == pre_node) {
        out_list.head_ = new_node;
      } else {
        pre_node->next_ = new_node;
      }
      pre_node = new_node;
      cur_in_node = cur_in_node->next_;
    }
  }

  return ret;
}

int ObTxDataTable::alloc_undo_status_node(ObUndoStatusNode *&undo_status_node)
{
  int ret = OB_SUCCESS;
  void *slice_ptr = nullptr;
  if (OB_ISNULL(slice_ptr = slice_allocator_.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "allocate memory fail.", KR(ret), KP(this), K(tablet_id_));
  } else {
    undo_status_node = new (slice_ptr) ObUndoStatusNode();
  }
  return ret;
}

void ObTxDataTable::free_tx_data(ObTxData *tx_data)
{
  if (OB_NOT_NULL(tx_data)) {
    free_undo_status_list_(tx_data->undo_status_list_.head_);
    // The memory of tx data belongs to a slice of memory allocated by slice allocator. And the
    // start of the slice memory is hash node.
    slice_allocator_.free(ObTxData::get_hash_node_by_tx_data(tx_data));
  }
}

int ObTxDataTable::free_undo_status_node(ObUndoStatusNode *&undo_status_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(undo_status_node)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "trying to free nullptr", KR(ret), K(tablet_id_));
  } else {
    slice_allocator_.free(reinterpret_cast<void *>(undo_status_node));
  }
  return ret;
}

void ObTxDataTable::free_undo_status_list_(ObUndoStatusNode *node_ptr)
{
  ObUndoStatusNode *node_to_free = nullptr;
  while (nullptr != node_ptr) {
    node_to_free = node_ptr;
    node_ptr = node_ptr->next_;
    slice_allocator_.free(reinterpret_cast<void *>(node_to_free));
  }
}

int ObTxDataTable::insert(ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard tg("tx_data_table::insert", 100 * 1000);
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
  } else if (FALSE_IT(tg.click())) {
    // do nothing
  } else if (OB_FAIL(get_memtable_mgr_()->get_all_memtables_for_write(write_guard))) {
    STORAGE_LOG(WARN, "get all memtables for write fail.", KR(ret), KPC(get_memtable_mgr_()));
  } else if (FALSE_IT(tg.click())) {
    // do nothing
  } else if (OB_FAIL(insert_(tx_data, write_guard))) {
    STORAGE_LOG(WARN, "insert tx data failed.", KR(ret), KPC(tx_data), KP(this), K(tablet_id_));
  } else {
    // STORAGE_LOG(DEBUG, "insert tx data succeed.", KPC(tx_data));
    // successfully insert
  }
  if (tg.get_diff() > 100000) {
    STORAGE_LOG(INFO, "ObTxDataTable insert cost too much time", K(tx_id), K(tg));
  }

  return ret;
}

// In order to support the commit log without undo actions, the tx data related to a single
// transaction may be inserted multiple times. For more details, see
// https://yuque.antfin.com/ob/transaction/cdn5ez
int ObTxDataTable::insert_(ObTxData *&tx_data, ObTxDataMemtableWriteGuard &write_guard)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard tg("tx_data_table::insert_", 100 * 1000);
  ObTxDataMemtable *tx_data_memtable = nullptr;
  ObTableHandleV2 (&memtable_handles)[MAX_TX_DATA_MEMTABLE_CNT] = write_guard.handles_;

  for (int i = write_guard.size_ - 1; OB_SUCC(ret) && OB_NOT_NULL(tx_data) && i >= 0; i--) {
    tx_data_memtable = nullptr;
    if (OB_FAIL(memtable_handles[i].get_tx_data_memtable(tx_data_memtable))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "get tx data memtable from table handles fail.", KR(ret), KP(this),
                  K(tablet_id_), K(memtable_handles[i]));
    } else if (OB_ISNULL(tx_data_memtable)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "tx data memtable is nullptr.", KR(ret), KP(this), K(tablet_id_),
                  K(memtable_handles[i]));
    } else if (OB_UNLIKELY(ObTxDataMemtable::State::RELEASED == tx_data_memtable->get_state())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR,
                  "trying to insert a tx data into a tx data memtable in frozen/dumped state.",
                  KR(ret), KP(this), K(tablet_id_), KP(tx_data_memtable), KPC(tx_data_memtable));
    } else if (FALSE_IT(tg.click())) {
      // do nothing
    } else if (tx_data_memtable->get_start_scn() < tx_data->end_scn_
               && tx_data_memtable->get_end_scn() >= tx_data->end_scn_) {
      tg.click();
      ret = insert_into_memtable_(tx_data_memtable, tx_data);
    } else {
      // should not insert into this memtable
    }
  }
  tg.click();

  // If this tx data can not be inserted into all memtables, check if it should be filtered.
  // We use the start scn of the first memtable as the filtering time stamp
  if (OB_SUCC(ret) && OB_NOT_NULL(tx_data) && OB_NOT_NULL(tx_data_memtable)) {
    SCN clog_checkpoint_scn = tx_data_memtable->get_key().get_start_scn();
    if (tx_data->end_scn_ <= clog_checkpoint_scn) {
      // Filter this tx data. The part trans ctx need to handle this error code because the memory
      // of tx data need to be freed.
      STORAGE_LOG(INFO, "This tx data is filtered.", K(clog_checkpoint_scn), KPC(tx_data));
      free_tx_data(tx_data);
      tx_data = nullptr;
      tg.click();

    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "cannot find the correct tx data memtable to insert", KR(ret),
                  KPC(tx_data), K(memtable_handles));
    }
  }
  if (tg.get_diff() > 100000) {
    STORAGE_LOG(INFO, "ObTxDataTable insert_ cost too much time", K(tg));
  }

  return ret;
}

// A tx data with lesser end_scn may be inserted after a tx data with greater end_scn due to
// the out-of-order callback of log. This function will retain the newer tx data and delete the
// older one.
int ObTxDataTable::insert_into_memtable_(ObTxDataMemtable *tx_data_memtable, ObTxData *&tx_data)
{
  common::ObTimeGuard tg("tx_data_table::insert_into_memtable", 100 * 1000);
  int ret = OB_SUCCESS;
  bool need_insert = true;

  if (OB_UNLIKELY(tx_data_memtable->contain_tx_data(tx_data->tx_id_))) {
    tg.click();
    // check and insert
    ObTxData *existed_tx_data = nullptr;
    if (OB_FAIL(tx_data_memtable->get_tx_data(tx_data->tx_id_, existed_tx_data))) {
      STORAGE_LOG(WARN, "get tx data from tx data memtable failed.", KR(ret), KPC(tx_data),
                  KPC(tx_data_memtable));
    } else if (OB_ISNULL(existed_tx_data)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "existed tx data is unexpected nullptr", KR(ret), KPC(tx_data),
                  KPC(tx_data_memtable));
    } else if (existed_tx_data->end_scn_ < tx_data->end_scn_) {
      tg.click();
      if (OB_FAIL(tx_data_memtable->remove(tx_data->tx_id_))) {
        STORAGE_LOG(ERROR, "remove tx data from tx data memtable failed.", KR(ret), KPC(tx_data),
                    KPC(tx_data_memtable));
      } else {
        existed_tx_data->is_in_tx_data_table_ = false;

        // successfully remove
      }
    } else {
      // The end_scn of tx data in memtable is greater than or equal to the end_scn of current
      // tx data, which means the tx data waiting to insert is an older one. So we set need_insert =
      // false to skip inserting this tx data
      need_insert = false;
      tx_data_memtable->revert_tx_data(existed_tx_data);
    }
  }
  tg.click();

  // insert or free according to the need_insert flag
  if (OB_SUCC(ret)) {
    if (OB_LIKELY(need_insert)) {
      tg.click();
      if (OB_FAIL(tx_data_memtable->insert(tx_data))) {
        STORAGE_LOG(WARN, "insert tx data into tx data memtable failed.", KR(ret),
                    KPC(tx_data_memtable), KPC(tx_data));
      } else {
        tx_data = nullptr;
      }
      tg.click();
    } else {
      free_tx_data(tx_data);
      tx_data = nullptr;
    }
  }
  if (tg.get_diff() > 100000) {
    STORAGE_LOG(INFO, "ObTxDataTable insert_info_memtable cost too much time", K(tg));
  }

  return ret;
}

int ObTxDataTable::check_with_tx_data(const ObTransID tx_id, ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data table is not init.", KR(ret), KP(this), K(tx_id));
  } else if (OB_SUCC(check_tx_data_in_memtable_(tx_id, fn))) {
    // successfully do check function in memtable, check done
    STORAGE_LOG(DEBUG, "tx data table check with tx memtable data succeed", K(tx_id), K(fn));
  } else if (OB_TRANS_CTX_NOT_EXIST == ret && OB_SUCC(check_tx_data_in_sstable_(tx_id, fn))) {
    // successfully do check function in sstable
    STORAGE_LOG(DEBUG, "tx data table check with tx sstable data succeed", K(tx_id), K(fn));
  } else {
    STORAGE_LOG(WARN, "check something in tx data fail.", KR(ret), K(tx_id), KP(this),
                K(tablet_id_));
  }

  return ret;
}

int ObTxDataTable::check_tx_data_in_memtable_(const ObTransID tx_id, ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(update_memtables_cache())) {
    STORAGE_LOG(WARN, "get all memtables fail.", KR(ret), K(tx_id));
  } else {
    // look for the tx data
    // construct memtable_handle before tx_data_guard to destruct it latter
    ObTableHandleV2 src_memtable_handle;
    ObTxDataGuard tx_data_guard;
    bool find = false;

    if (OB_FAIL(get_tx_data_in_memtables_cache_(tx_id, src_memtable_handle, tx_data_guard, find))) {
      STORAGE_LOG(INFO, "get tx data in memtables cache failed.", KR(ret), K(tx_id));
    } else if (find) {
      // do function if find tx data in memtable
      if (OB_FAIL(fn(tx_data_guard.tx_data()))) {
        STORAGE_LOG(WARN, "do data check function fail.", KR(ret), KP(this), K(tablet_id_), K(tx_data_guard.tx_data()));
      } else {
        // do data check function success
      }
    } else {
      ret = OB_TRANS_CTX_NOT_EXIST;
    }
  }

  return ret;
}

int ObTxDataTable::update_memtables_cache()
{
  int ret = OB_SUCCESS;
  bool need_update = true;
  if (OB_FAIL(check_need_update_memtables_cache_(need_update))) {
    STORAGE_LOG(WARN, "check if memtable handles need update failed.", KR(ret));
  } else if (need_update) {
    bool make_sure_need_update = true;
    // lock for updating memtables cache
    TCWLockGuard guard(memtables_cache_.lock_);
    if (OB_FAIL(check_need_update_memtables_cache_(make_sure_need_update))) {
      STORAGE_LOG(WARN, "check if memtable handles need update failed.", KR(ret));
    } else if (!make_sure_need_update) {
      // do not need update cache, skip update
    } else if (FALSE_IT(memtables_cache_.reuse())) {
    } else if (OB_FAIL(get_memtable_mgr_()->get_all_memtables_with_range(memtables_cache_.memtable_handles_,
                   memtables_cache_.memtable_head_,
                   memtables_cache_.memtable_tail_))) {
      STORAGE_LOG(WARN, "get all memtables with range failed.", KR(ret), KPC(this), KPC(get_memtable_mgr_()));
    }
  }

  return ret;
}

int ObTxDataTable::check_need_update_memtables_cache_(bool &need_update)
{
  int ret = OB_SUCCESS;
  int64_t memtable_head = -1;
  int64_t memtable_tail = -1;
  if (!is_started_) {
    ret = OB_NOT_RUNNING;
    need_update = false;
    STORAGE_LOG(WARN, "tx data memtable has stopped", KR(ret), KPC(this), K(need_update));
  } else if (OB_FAIL(get_memtable_mgr_()->get_memtable_range(memtable_head, memtable_tail))) {
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

// For ease of understanding, this function can be regarded as the following steps:
// 1. Try to get tx data from sstable cache if we want to use the cache.
// 2. If we 1) dont want to get tx data from cache; 2) get from cache failed; 3) get an tx data with
// running state from cachce, trying to get tx data from sstable.
// 3. Call functor with tx data.
// 4. Free or revert tx data according to where it is from.
int ObTxDataTable::check_tx_data_in_sstable_(const ObTransID tx_id, ObITxDataCheckFunctor &fn)
{
  int ret = OB_SUCCESS;
  ObTxData *tx_data = nullptr;

  if (OB_FAIL(get_tx_data_in_sstable_(tx_id, tx_data))) {
    STORAGE_LOG(WARN, "get tx data from sstable failed.", KR(ret), K(tx_id));
  } else if (OB_ISNULL(tx_data)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected nullptr of tx data", KR(ret), K(tx_id));
  } else if (OB_FAIL(fn(*tx_data))) {
    STORAGE_LOG(WARN, "check tx data in sstable failed.", KR(ret), KP(this), K(tablet_id_));
  }

  // free tx data after using it
  if (OB_NOT_NULL(tx_data)) {
    free_tx_data(tx_data);
  }
  return ret;
}

int ObTxDataTable::get_tx_data_in_sstable_(const transaction::ObTransID tx_id, ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;
  tx_data = nullptr;
  ObTableIterParam iter_param = read_schema_.iter_param_;
  ObTabletHandle &tablet_handle = iter_param.tablet_handle_;

  if (tablet_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tablet handle should be empty", KR(ret), K(tablet_handle));
  } else if (OB_FAIL(alloc_tx_data(tx_data))) {
    STORAGE_LOG(WARN, "allocate tx data from tx data table failed.");
  } else if (FALSE_IT(tx_data->tx_id_ = tx_id)) {
  } else if (OB_FAIL(ls_tablet_svr_->get_tablet(tablet_id_, tablet_handle))) {
    STORAGE_LOG(WARN, "get tablet from ls tablet service fail.", KR(ret), KP(this), K(tablet_id_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle), K(tablet_id_));
  } else {
    ObTxDataSingleRowGetter getter(iter_param, slice_allocator_);
    if (OB_FAIL(getter.init(tx_id))) {
      STORAGE_LOG(WARN, "init ObTxDataSingleRowGetter fail.", KR(ret), KP(this), K(tablet_id_));
    } else if (OB_FAIL(getter.get_next_row(*tx_data))) {
      if (OB_ITER_END == ret) {
        ret = OB_TRANS_CTX_NOT_EXIST;
      }
      STORAGE_LOG(WARN, "get tx data in sstable failed.", KR(ret), KP(this), K(tablet_id_));
    } else {
      // get tx data from sstable succeed.
    }
  }

  // If get tx data in sstable failed, free the tx data and reset it.
  if (OB_FAIL(ret) && (nullptr != tx_data)) {
    free_tx_data(tx_data);
    tx_data = nullptr;
  }
  return ret;
}

int ObTxDataTable::get_recycle_scn(SCN &recycle_scn)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObLSTabletIterator iterator;
  ObMigrationStatus migration_status;
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
    STORAGE_LOG(INFO, "logstream is not in a normal state. skip recycle tx data", "ls_id", ls_->get_ls_id());
  } else if (OB_FAIL(get_ls_min_end_scn_in_latest_tablets_(min_end_scn_from_latest_tablets))) {
    // get_ls_min_end_log_ts_in_latest_tablets must before get_ls_min_end_log_ts_in_old_tablets
    STORAGE_LOG(WARN, "fail to get ls min end log ts in all of latest tablets", KR(ret));
  } else if (OB_FAIL(ls_tablet_svr_->get_ls_min_end_scn_in_old_tablets(min_end_scn_from_old_tablets))) {
    STORAGE_LOG(WARN, "fail to get ls min end log ts in all of old tablets", KR(ret));
  } else {
    min_end_scn = std::min(min_end_scn_from_old_tablets, min_end_scn_from_latest_tablets);
    if (!min_end_scn.is_max()) {
      recycle_scn = min_end_scn;
    }
  }

  FLOG_INFO("get tx data recycle ts finish.",
            KR(ret),
            "ls_id", ls_->get_ls_id(),
            K(recycle_scn),
            K(min_end_scn_from_old_tablets),
            K(min_end_scn_from_latest_tablets));

  return ret;
}

int ObTxDataTable::get_ls_min_end_scn_in_latest_tablets_(SCN &min_end_scn)
{
  int ret = OB_SUCCESS;
  ObLSTabletIterator iterator(ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US);
  ObTabletHandle tablet_handle;
  min_end_scn.set_max();

  if (OB_FAIL(ls_tablet_svr_->build_tablet_iter(iterator))) {
    STORAGE_LOG(WARN, "build ls table iter failed.", KR(ret));
  } else {
    while (OB_SUCC(iterator.get_next_tablet(tablet_handle))) {
      SCN end_scn_from_single_tablet = SCN::max_scn();
      if (OB_FAIL(get_min_end_scn_from_single_tablet_(tablet_handle, end_scn_from_single_tablet))) {
        STORAGE_LOG(WARN, "get min end_scn from a single tablet failed.", KR(ret));
      } else if (end_scn_from_single_tablet < min_end_scn) {
        min_end_scn = end_scn_from_single_tablet;
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTxDataTable::get_min_end_scn_from_single_tablet_(ObTabletHandle &tablet_handle,
                                                      SCN &end_scn)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tablet is nullptr.", KR(ret), KP(this), K(tablet_id_));
  } else if (tablet->get_tablet_meta().tablet_id_.is_ls_inner_tablet()) {
    // skip inner tablet
  } else {
    end_scn = tablet->get_tablet_meta().clog_checkpoint_scn_;
    ObTabletTableStore &table_store = tablet->get_table_store();
    ObITable *first_minor_mini_sstable
      = table_store.get_minor_sstables().get_boundary_table(false /*is_last*/);

    if (OB_NOT_NULL(first_minor_mini_sstable)) {
      end_scn = first_minor_mini_sstable->get_end_scn();
    }
  }
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

  STORAGE_LOG(INFO, "start tx data table self freeze task", K(get_ls_id()));

  if (OB_FAIL(memtable_mgr_->flush(SCN::max_scn(), true))) {
    share::ObLSID ls_id = get_ls_id();
    STORAGE_LOG(WARN, "self freeze of tx data memtable failed.", KR(ret), K(ls_id), KPC(memtable_mgr_));
  }

  STORAGE_LOG(INFO, "finish tx data table self freeze task", KR(ret), K(get_ls_id()));
  return ret;
}

// The main steps in calculating upper_trans_version. For more details, see :
// https://yuque.antfin-inc.com/ob/transaction/lurtok
int ObTxDataTable::get_upper_trans_version_before_given_scn(const SCN sstable_end_scn, SCN &upper_trans_version)
{
  int ret = OB_SUCCESS;
  bool skip_calc = false;
  upper_trans_version.set_max();

  STORAGE_LOG(DEBUG, "start get upper trans version", K(get_ls_id()));

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The tx data table is not inited.", KR(ret));
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
  } else if (0 == calc_upper_trans_version_cache_.commit_scns_.array_.count()) {
    STORAGE_LOG(ERROR, "Unexpected empty array.", K(calc_upper_trans_version_cache_));
  } else {
    TCRLockGuard lock_guard(calc_upper_trans_version_cache_.lock_);
    if (!calc_upper_trans_version_cache_.commit_scns_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "invalid cache for upper trans version calculation", KR(ret));
    } else if (OB_FAIL(calc_upper_trans_scn_(sstable_end_scn, upper_trans_version))) {
      STORAGE_LOG(WARN, "calc upper trans version failed", KR(ret), "ls_id", get_ls_id());
    } else {
      FLOG_INFO("get upper trans version finish.",
                KR(ret),
                K(skip_calc),
                "ls_id", get_ls_id(),
                K(sstable_end_scn),
                K(upper_trans_version));
    }
  }
  return ret;
}

// This function is implemented for two reasons : accuracy and performance of upper_trans_version
// calculation.
//
// 1. Accuracy :
// If there are some running transactions with start_scn which is less than or equal to
// sstable_end_scn, we skip this upper_trans_version calculation because it is currently
// undetermined.
//
// 2. Performance :
// If there are some commited transactions with start_scn which is less than or equal to
// sstable_end_scn stil in tx data memtable, we skip this upper_trans_version calculation because
// calculating upper_trans_version in tx data memtable is very slow.
bool ObTxDataTable::skip_this_sstable_end_scn_(SCN sstable_end_scn)
{
  int ret = OB_SUCCESS;
  bool need_skip = false;
  int64_t cur_ts = common::ObTimeUtility::fast_current_time();
  int64_t tmp_update_ts = ATOMIC_LOAD(&last_update_ts_);
  SCN min_start_scn_in_tx_data_memtable = SCN::max_scn();
  SCN max_decided_scn = SCN::min_scn();

  // make sure the max decided log ts is greater than sstable_end_log_ts
  if (OB_FAIL(ls_->get_max_decided_scn(max_decided_scn))) {
    STORAGE_LOG(WARN, "get max decided log ts failed", KR(ret), "ls_id", get_ls_id().id());
  } else if (max_decided_scn < sstable_end_scn) {
    need_skip = true;
    STORAGE_LOG(INFO, "skip calc upper trans version once", K(max_decided_scn), K(sstable_end_scn));
  }

  // If the min_start_scn_in_ctx has not been updated for more than 30 seconds,
  if (need_skip) {
  } else if (cur_ts - tmp_update_ts > 30_s
      && tmp_update_ts == ATOMIC_CAS(&last_update_ts_, tmp_update_ts, cur_ts)) {
    // update last_update_min_start_scn
    if (OB_FAIL(tx_ctx_table_->get_min_start_scn(min_start_scn_in_ctx_))) {
      STORAGE_LOG(WARN, "get min start scn from tx ctx table failed.", KR(ret));
    }
  }

  if (need_skip) {
  } else if (OB_FAIL(ret)) {
    need_skip = true;
  } else if (min_start_scn_in_ctx_ <= sstable_end_scn) {
    // skip this sstable end scn calculation
    need_skip = true;
  } else if (OB_FAIL(update_memtables_cache())) {
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
        STORAGE_LOG(ERROR, "get tx data memtable from table handles fail.", KR(ret), KP(this),
                    K(tablet_id_), K(memtable_handles.at(i)));
      } else if (OB_ISNULL(tx_data_memtable)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "tx data memtable is nullptr.", KR(ret), KP(this), K(tablet_id_),
                    K(memtable_handles.at(i)));
      } else if (FALSE_IT(min_start_scn_in_tx_data_memtable
                          = std::min(min_start_scn_in_tx_data_memtable,
                                     tx_data_memtable->get_min_start_scn()))) {
      } else if (sstable_end_scn >= min_start_scn_in_tx_data_memtable) {
        // there is a min_start_scn in tx_data_memtable less than sstable_end_scn, skip this
        // calculation
        need_skip = true;
        break;
      }
    }

    if (!need_skip) {
      STORAGE_LOG(INFO, "check skip upper trans version calculation finish.", K(need_skip),
              K(sstable_end_scn), K(min_start_scn_in_ctx_),
              K(min_start_scn_in_tx_data_memtable));
    }
  }


  return need_skip;
}

int ObTxDataTable::update_cache_if_needed_(bool &skip_calc)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;

  if (OB_FAIL(ls_tablet_svr_->get_tablet(LS_TX_DATA_TABLET, tablet_handle))) {
    STORAGE_LOG(WARN, "get tablet from ls tablet service failed.", KR(ret));
  } else {
    ObITable *table =
        tablet_handle.get_obj()->get_table_store().get_minor_sstables().get_boundary_table(true /*is_last*/);

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
  ObTableIterParam iter_param = read_schema_.iter_param_;
  ObTabletHandle &tablet_handle = iter_param.tablet_handle_;

  if (tablet_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tablet handle should be empty", KR(ret), K(tablet_handle));
  } else if (OB_FAIL(ls_tablet_svr_->get_tablet(tablet_id_, tablet_handle))) {
    STORAGE_LOG(WARN, "get tablet from ls tablet service fail.", KR(ret), KP(this), K(tablet_id_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", KR(ret), K(tablet_handle), K(tablet_id_));
  } else {
    ObCommitVersionsGetter getter(iter_param, table);
    if (OB_FAIL(getter.get_next_row(calc_upper_trans_version_cache_.commit_scns_))) {
      STORAGE_LOG(WARN, "update calc_upper_trans_trans_version_cache failed.", KR(ret), KPC(table));
    } else {
      calc_upper_trans_version_cache_.is_inited_ = true;
      calc_upper_trans_version_cache_.cache_version_ = table->get_end_scn();
      // update calc_upper_trans_scn_cache succeed.
    }
  }
  return ret;
}

int ObTxDataTable::calc_upper_trans_scn_(const SCN sstable_end_scn, SCN &upper_trans_version)
{
  int ret = OB_SUCCESS;

  const auto &array = calc_upper_trans_version_cache_.commit_scns_.array_;
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
              "ls_id", get_ls_id(),
              "array_count", array.count(),
              "chose_idx", l);
  return ret;
}

int ObTxDataTable::supplement_undo_actions_if_exist(ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;
  ObTxData *tx_data_from_sstable = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data table is not init", KR(ret), KP(this));
  } else if (OB_ISNULL(tx_data)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "tx data is nullptr", KR(ret), KP(this));
  } else if (OB_FAIL(get_tx_data_in_sstable_(tx_data->tx_id_, tx_data_from_sstable))) {
    if (ret == OB_TRANS_CTX_NOT_EXIST) {
      // This transaction does not have undo actions
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "get tx data from sstable failed.", KR(ret));
    }
  } else if (OB_ISNULL(tx_data_from_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected nullptr of tx data", KR(ret), KPC(tx_data));
  } else {
    // assign and reset to avoid deep copy
    tx_data->undo_status_list_ = tx_data_from_sstable->undo_status_list_;
    tx_data_from_sstable->undo_status_list_.reset();
  }

  if (OB_NOT_NULL(tx_data_from_sstable)) {
    free_tx_data(tx_data_from_sstable);
  }
  return ret;
}

int ObTxDataTable::get_start_tx_scn(SCN &start_tx_scn)
{
  int ret = OB_SUCCESS;
  start_tx_scn.set_max();
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObSSTable *oldest_minor_sstable = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "tx data table is not init", KR(ret), KPC(this));
  } else if (OB_FAIL(ls_tablet_svr_->get_tablet(tablet_id_, tablet_handle))) {
    STORAGE_LOG(WARN, "get tablet from ls tablet service failed.", KR(ret));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tablet is nullptr.", KR(ret), KP(this), K(tablet_id_));
  } else if (OB_ISNULL(oldest_minor_sstable =
      (ObSSTable *)tablet->get_table_store().get_minor_sstables().get_boundary_table(false /*is_last*/))) {
    start_tx_scn.set_max();
    STORAGE_LOG(INFO, "this logstream do not have tx data sstable", K(start_tx_scn), K(get_ls_id()), KPC(tablet));
  } else if (FALSE_IT(start_tx_scn = oldest_minor_sstable->get_filled_tx_scn())) {
  } else if (start_tx_scn.is_max()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "start_tx_scn is unexpected INT64_MAX", KR(ret), KPC(tablet), KPC(oldest_minor_sstable));
  } else {
    FLOG_INFO("get start tx scn done", KR(ret), K(start_tx_scn), KPC(oldest_minor_sstable));
  }
  return ret;
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
    bool find = false;

    if (OB_FAIL(get_tx_data_in_memtables_cache_(tx_id, src_memtable_handle, tx_data_guard, find))) {
      STORAGE_LOG(INFO, "get tx data in memtables cache failed.", KR(ret), K(tx_id));
    } else if (find) {
      fprintf(fd, "********** Tx Data MemTable ***********\n\n");
      tx_data_guard.tx_data().dump_2_text(fd);
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
  ObTxData *tx_data = nullptr;

  if (OB_FAIL(get_tx_data_in_sstable_(tx_id, tx_data))) {
    STORAGE_LOG(WARN, "get tx data from sstable failed.", KR(ret), K(tx_id));
  } else if (OB_ISNULL(tx_data)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected nullptr of tx data", KR(ret), K(tx_id));
  } else {
    fprintf(fd, "********** Tx Data SSTable ***********\n\n");
    tx_data->dump_2_text(fd);
    fprintf(fd, "\n********** Tx Data SSTable ***********\n");
  }

  // free tx data after using it
  if (OB_NOT_NULL(tx_data)) {
    free_tx_data(tx_data);
  }

  return ret;
}

share::ObLSID ObTxDataTable::get_ls_id() { return ls_->get_ls_id(); }

bool CleanTxDataSSTableCacheFunctor::operator()(const transaction::ObTransID &key,
                                                ObTxData *tx_data)
{
  int ret = OB_SUCCESS;
  int64_t *latest_used_ts = ObTxData::get_latest_used_ts_by_tx_data(tx_data);
  if ((*latest_used_ts < clean_ts_) && (OB_FAIL(sstable_cache_.del(key)))) {
    STORAGE_LOG(WARN, "delete tx data from tx data sstable cache failed.", KR(ret), KPC(tx_data));
  }
  return true;
}

}  // namespace storage

}  // namespace oceanbase

#undef USING_LOG_PREFIX
