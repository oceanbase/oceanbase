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

#include "storage/tx_table/ob_tx_data_memtable.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/ls/ob_freezer.h"
#include "storage/tx/ob_tx_data_functor.h"
#include "storage/tx_table/ob_tx_data_memtable_mgr.h"
#include "storage/tx_table/ob_tx_data_table.h"
#include "storage/tx_table/ob_tx_table_iterator.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::transaction;

int ObTxDataMemtable::init(const ObITable::TableKey &table_key,
                           SliceAllocator *slice_allocator,
                           ObTxDataMemtableMgr *memtable_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init tx data memtable twice", KR(ret));
  } else if (OB_ISNULL(slice_allocator) || OB_ISNULL(memtable_mgr)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "the slice_allocator or arena_allocator is nullptr", KR(ret));
  } else if (OB_FAIL(ObITable::init(table_key))) {
    STORAGE_LOG(WARN, "ObITable::init fail");
  } else {
    is_iterating_ = false;
    min_tx_log_ts_ = INT64_MAX;
    max_tx_log_ts_ = 0;
    min_start_log_ts_ = INT64_MAX;
    inserted_cnt_ = 0;
    deleted_cnt_ = 0;
    write_ref_ = 0;
    occupied_size_ = 0;
    last_insert_ts_ = 0;
    state_ = ObTxDataMemtable::State::ACTIVE;
    sort_list_head_.reset();
    slice_allocator_ = slice_allocator;
    memtable_mgr_ = memtable_mgr;

    TxDataHashMapAllocHandle tx_data_alloc_handle(slice_allocator_);
    ObMemAttr attr;
    attr.tenant_id_ = MTL_ID();
    attr.label_ = "TX_DATA_TABLE";
    attr.ctx_id_ = ObCtxIds::DEFAULT_CTX_ID;
    void *data_map_ptr = ob_malloc(sizeof(*tx_data_map_), attr);
    if (OB_ISNULL(data_map_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "allocate memory of tx_data_map_ failed", KR(ret));
    } else {
      tx_data_map_ = new (data_map_ptr) TxDataMap(tx_data_alloc_handle, 64 << 10, 2 << 20);
      if (OB_FAIL(tx_data_map_->init(attr.label_, attr.tenant_id_))) {
        STORAGE_LOG(WARN, "tx_data_map_ init failed", KR(ret));
      } else {
        is_inited_ = true;
      }
    }
  }

  return ret;
}

void ObTxDataMemtable::reset()
{
  key_.reset();
  is_iterating_ = false;
  has_constructed_list_ = false;
  min_tx_log_ts_ = INT64_MAX;
  max_tx_log_ts_ = 0;
  min_start_log_ts_ = INT64_MAX;
  inserted_cnt_ = 0;
  deleted_cnt_ = 0;
  write_ref_ = 0;
  occupied_size_ = 0;
  last_insert_ts_ = 0;
  state_ = ObTxDataMemtable::State::INVALID;
  sort_list_head_.reset();
  reset_thread_local_list_();
  if (OB_NOT_NULL(tx_data_map_)) {
    tx_data_map_->destroy();
    ob_free(tx_data_map_);
    tx_data_map_ = nullptr;
  }
  slice_allocator_ = nullptr;
  memtable_mgr_ = nullptr;
  freezer_ = nullptr;
  ObITable::reset();
  is_inited_ = false;
}

void ObTxDataMemtable::reset_thread_local_list_()
{
  for (int i = 0; i < MAX_TX_DATA_TABLE_CONCURRENCY; i++) {
    ObTxDataSortListNode *cur_node = local_sort_list_head_[i].next_;
    while (OB_NOT_NULL(cur_node)) {
      ObTxData *tx_data = ObTxData::get_tx_data_by_sort_list_node(cur_node);
      cur_node = cur_node->next_;
      if (false == tx_data->is_in_tx_data_table_) {
        if (OB_ISNULL(tx_data_map_)) {
          STORAGE_LOG(ERROR, "tx_data_map is unexpected nullptr", KP(tx_data_map_), KPC(tx_data));
        } else {
          tx_data_map_->revert(tx_data);
        }
      }
    }
    local_sort_list_head_[i].reset();
  }
}

int ObTxDataMemtable::insert(ObTxData *tx_data)
{
  common::ObTimeGuard tg("tx_data_memtable::insert", 100 * 1000);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx data memtable is not init");
  } else if (OB_ISNULL(tx_data)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tx data is nullptr", KR(ret));
  } else if (OB_ISNULL(tx_data_map_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected null value of tx_data_map_", KR(ret));
  } else if (true == tx_data->is_in_tx_data_table_) {
    STORAGE_LOG(ERROR, "insert an exist tx data", KP(tx_data), KPC(tx_data), KR(ret),
                KP(tx_data_map_));
  } else if (OB_FAIL(tx_data_map_->insert_and_get(tx_data->tx_id_, tx_data))) {
    // insert_and_get fail
    if (ret == OB_ENTRY_EXIST) {
      STORAGE_LOG(ERROR, "insert an exist tx data", KP(tx_data), KPC(tx_data), KR(ret),
                  KP(tx_data_map_));
    } else {
      STORAGE_LOG(ERROR, "insert the tx data into tx_data_map_ fail.", KP(tx_data), KPC(tx_data),
                  KR(ret), KP(tx_data_map_));
    }
  } else {
    tg.click();
    // insert_and_get success
    tx_data->is_in_tx_data_table_ = true;
    common::inc_update(&max_tx_log_ts_, tx_data->end_log_ts_);
    common::dec_update(&min_tx_log_ts_, tx_data->end_log_ts_);
    common::dec_update(&min_start_log_ts_, tx_data->start_log_ts_);
    ATOMIC_INC(&inserted_cnt_);
    tg.click();

    int thread_idx = ::get_itid() % MAX_TX_DATA_TABLE_CONCURRENCY;
    ObTxDataSortListNode *cur_node = ObTxData::get_sort_list_node_by_tx_data(tx_data);
    tg.click();
    while (true) {
      ObTxDataSortListNode *last_node = ATOMIC_LOAD(&local_sort_list_head_[thread_idx].next_);
      cur_node->next_ = last_node;
      if (last_node == ATOMIC_CAS(&local_sort_list_head_[thread_idx].next_, last_node, cur_node)) {
        break;
      }
    }
    tg.click();

    // Note : a tx data may be deleted from memtable in ObTxDataTable::insert_into_memtable_ but the
    // occupied_size would not be reduced because the memory will not be freed until freeze done.
    int64_t tx_data_size = TX_DATA_SLICE_SIZE * (1LL + tx_data->undo_status_list_.undo_node_cnt_);
    ATOMIC_FAA(&occupied_size_, tx_data_size);

    // TODO : @gengli remove this after tx data memtable flush stable
    common::inc_update(&last_insert_ts_, ObTimeUtil::current_time_ns());

    tx_data_map_->revert(tx_data);
    tg.click();
  }
  if (tg.get_diff() > 100000) {
    STORAGE_LOG(INFO, "tx data memtable insert cost too much time", K(tg));
  }

  return ret;
}

int ObTxDataMemtable::get_tx_data(const ObTransID &tx_id, ObTxDataGuard &tx_data_guard)
{
  int ret = OB_SUCCESS;
  ObTxData *tx_data = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx data memtable is not init");
  } else if (OB_FAIL(tx_data_map_->get(tx_id, tx_data))) {
    // This tx data is not in this tx data memtable
  } else if (OB_FAIL(tx_data_guard.init(tx_data, tx_data_map_))) {
    STORAGE_LOG(WARN, "init tx data guard fail.", KR(ret));
  } else {
    // get tx data success
  }
  return ret;
}

int ObTxDataMemtable::get_tx_data(const transaction::ObTransID &tx_id, ObTxData *&tx_data)
{
  int ret = OB_SUCCESS;
  tx_data = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx data memtable is not init");
  } else if (OB_FAIL(tx_data_map_->get(tx_id, tx_data))) {
    // This tx data is not in this tx data memtable
  } else {
    // get tx data success
  }
  return ret;
}

int ObTxDataMemtable::prepare_tx_data_list()
{
  int ret = OB_SUCCESS;

  if (ObTxDataMemtable::State::FROZEN != state_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Trying to dump a non-frozen tx data memtable.", KR(ret), KP(this));
  } else if (!can_iterate()) {
    ret = OB_STATE_NOT_MATCH;
    STORAGE_LOG(
      ERROR,
      "trying to construct a tx data memtable scan iterator while another iterator is existed.",
      KR(ret), KPC(this));
  } else {
    if (!has_constructed_list_) {
      if (OB_FAIL(construct_list_for_sort_())) {
        STORAGE_LOG(WARN, "construct list for sort failed.", KR(ret));
      } else {
        has_constructed_list_ = true;
      }
    } else {
      // construct scan iterator for this tx data memtable twice or more
      STORAGE_LOG(INFO, "construct tx data memtable scan iterator more than once", KPC(this));
    }

    // sort list with merge sort
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(do_sort_by_tx_id_())) {
      STORAGE_LOG(ERROR, "prepare dump fail when do sort", KR(ret), KPC(this));
    }
  }

  if (OB_FAIL(ret)) {		
    reset_is_iterating();		
  }
  return ret;
}

// We need construct a list to hold the tx datas and sort them. There are already some thread local
// list exist. Here we just iterate them and concat them.
//
// It is not strange except one thing : There are some tx datas deleted in the link_hash_map because
// of the transaction rollback. But, it is difficult to delete them in thread local list at the same
// time deleted in link hash map. So we mark them by is_in_tx_data_table filed and revert it now to
// free its memory.
int ObTxDataMemtable::construct_list_for_sort_()
{
  int ret = OB_SUCCESS;
  int64_t start_construct_ts = ObTimeUtil::current_time_ns();

  ObTxDataSortListNode *pre_node = &sort_list_head_;
  ObTxDataSortListNode *cur_node = nullptr;

  int64_t sort_list_node_cnt = 0;
  int64_t skip_list_node_cnt = 0;
  for (int i = 0; i < MAX_TX_DATA_TABLE_CONCURRENCY; i++) {
    cur_node = local_sort_list_head_[i].next_;
    local_sort_list_head_[i].reset();
    while (OB_NOT_NULL(cur_node)) {
      ObTxData *tx_data = ObTxData::get_tx_data_by_sort_list_node(cur_node);

      if (false == tx_data->is_in_tx_data_table_) {
        cur_node = cur_node->next_;
        // TODO : @gengli remove log info after stable
        // STORAGE_LOG(INFO, "skip one tx data", KPC(tx_data), KP(this), K(freezer_->get_ls_id()));
        skip_list_node_cnt++;
        // revert must behind move pointer
        tx_data_map_->revert(tx_data);
      } else {
        pre_node->next_ = cur_node;
        pre_node = cur_node;
        cur_node = cur_node->next_;
        sort_list_node_cnt++;
      }

    }
  }

  pre_node->next_ = nullptr;

  if (start_construct_ts <= last_insert_ts_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "Tx data is inserted after flushing is running.", KR(ret), K(start_construct_ts), K(last_insert_ts_), KPC(this));
  } else {
    bool node_cnt_correct = (skip_list_node_cnt == deleted_cnt_) 
                          && (skip_list_node_cnt + sort_list_node_cnt == inserted_cnt_) 
                          && (sort_list_node_cnt == tx_data_map_->count()) 
                          && (inserted_cnt_ - deleted_cnt_ == tx_data_map_->count());

    if (!node_cnt_correct) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR,
        "sort list count is not equal to inserted tx data count",
        KR(ret),
        K(inserted_cnt_),
        K(deleted_cnt_),
        K(skip_list_node_cnt),
        K(sort_list_node_cnt),
        K(tx_data_map_->count()),
        KPC(this));
    }
  }

  return ret;
}

int ObTxDataMemtable::prepare_commit_version_list()
{
  int ret = OB_SUCCESS;

  if (ObTxDataMemtable::State::FROZEN != state_) {
    if (ObTxDataMemtable::State::RELEASED == state_) {
      ret = OB_STATE_NOT_MATCH;
      STORAGE_LOG(
          WARN, "tx data memtable has been released. A concurrent logstream removing may happend", KR(ret), KP(this));
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "Trying to dump a non-frozen tx data memtable.", KR(ret), KP(this));
    }
  } else if (OB_FAIL(do_sort_by_start_log_ts_())) {
    STORAGE_LOG(ERROR, "prepare dump fail when do sort", KR(ret));
  }

  return ret;
}

int ObTxDataMemtable::scan(const ObTableIterParam &param,
                           ObTableAccessContext &context,
                           const blocksstable::ObDatumRange &range,
                           ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  UNUSED(range);
  ObTxDataMemtableScanIterator *scan_iter_ptr = nullptr;
  void *scan_iter_buff = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ob tx data memtable is not inited.", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid param", KR(ret), K(param), K(context));
  } else if (OB_UNLIKELY(!param.is_multi_version_minor_merge_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ObTxDataMemtable only support scan for minor merge", KR(ret), K(param));
  } else if (OB_ISNULL(scan_iter_buff
                       = context.stmt_allocator_->alloc(sizeof(ObTxDataMemtableScanIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "construct ObTxDataMemtableScanIterator fail", "scan_iter_buffer",
                scan_iter_buff, "scan_iter_ptr", scan_iter_ptr, KR(ret));
  } else if (FALSE_IT(scan_iter_ptr = new (scan_iter_buff) ObTxDataMemtableScanIterator(
                          memtable_mgr_->get_tx_data_table()->get_read_schema().iter_param_))) {
  } else if (OB_FAIL(scan_iter_ptr->init(this))) {
    STORAGE_LOG(WARN, "init scan_iter_ptr fail.", KR(ret));
  } else {
    // tx data memtable scan iterator init success
    row_iter = scan_iter_ptr;
  }
  return ret;
}



int ObTxDataMemtable::set_freezer(ObFreezer *handler)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(handler)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "freeze handler is null", K(ret));
  } else {
    freezer_ = handler;
  }

  return ret;
}

bool ObTxDataMemtable::can_be_minor_merged()
{
  return ready_for_flush();
}

bool ObTxDataMemtable::contain_tx_data(transaction::ObTransID tx_id)
{
  bool bool_ret = OB_ENTRY_EXIST == tx_data_map_->contains_key(tx_id);
  return bool_ret;
}

int ObTxDataMemtable::remove(transaction::ObTransID tx_id)
{
  ATOMIC_INC(&deleted_cnt_);
  // TODO : @gengli remove log info after stable
  STORAGE_LOG(INFO, "remove one tx data", K(tx_id), KP(this), K(freezer_->get_ls_id()));

  return tx_data_map_->del(tx_id);
}

bool ObTxDataMemtable::ready_for_flush()
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  int64_t max_consequent_callbacked_log_ts = 0;
  if (OB_UNLIKELY(ObTxDataMemtable::State::RELEASED == state_
                  || ObTxDataMemtable::State::ACTIVE == state_
                  || ObTxDataMemtable::State::INVALID == state_)) {
    STORAGE_LOG(WARN, "call ready_for_flush() function on an incorrect state tx data memtable.",
                KPC(this));
  } else if (ObTxDataMemtable::State::FROZEN == state_) {
    bool_ret = true;
    STORAGE_LOG(INFO, "memtable is frozen yet.", KP(this));
  } else if (OB_FAIL(freezer_->get_max_consequent_callbacked_log_ts(max_consequent_callbacked_log_ts))) {
    STORAGE_LOG(WARN, "get_max_consequent_callbacked_log_ts failed", K(ret), K(freezer_->get_ls_id()));
  } else if (max_consequent_callbacked_log_ts >= key_.log_ts_range_.end_log_ts_) {
    state_ = ObTxDataMemtable::State::FROZEN;
    set_snapshot_version(min_tx_log_ts_);
    bool_ret = true;
  } else {
    int64_t freeze_ts = key_.log_ts_range_.end_log_ts_;
    STORAGE_LOG(INFO, "tx data metmable is not ready for flush",
                K(max_consequent_callbacked_log_ts), K(freeze_ts));
  }

  return bool_ret;
}

int ObTxDataMemtable::flush()
{
  int ret = OB_SUCCESS;
  compaction::ObTabletMergeDagParam param;
  param.ls_id_ = freezer_->get_ls_id();
  param.tablet_id_ = key_.tablet_id_;
  param.merge_type_ = MINI_MERGE;
  param.merge_version_ = ObVersion::MIN_VERSION;
  if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tx_table_merge_dag(param))) {
    if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
      STORAGE_LOG(WARN, "failed to schedule tablet merge dag", K(ret));
    }
  } else {
    STORAGE_LOG(INFO, "schedule flush tx data memtable task done", KR(ret), K(param), KPC(this));
  }
  return ret;
}

int64_t get_tx_id_(const ObTxData &tx_data) { return tx_data.tx_id_.get_id(); }

int ObTxDataMemtable::do_sort_by_tx_id_()
{
  int ret = OB_SUCCESS;
  // TODO : optimize merge sort with multiple threads
  merge_sort_(&get_tx_id_, sort_list_head_.next_);
  // sort_list_head_.next_ = quick_sort_(&get_tx_id_, sort_list_head_.next_);
  return ret;
}

int64_t get_start_ts_(const ObTxData &tx_data) { return tx_data.start_log_ts_; }

int ObTxDataMemtable::do_sort_by_start_log_ts_()
{
  int ret = OB_SUCCESS;
  merge_sort_(&get_start_ts_, sort_list_head_.next_);
  // sort_list_head_.next_ = quick_sort_(&get_start_ts_, sort_list_head_.next_);
  return ret;
}

int ObTxDataMemtable::DEBUG_check_sort_result_(int64_t (*get_key)(const ObTxData &))
{
  int ret = OB_SUCCESS;

  ObTxDataSortListNode *cur_node = sort_list_head_.next_;
  ObTxData *pre_tx_data = nullptr;
  int64_t pre_key = -1;

  STORAGE_LOG(INFO, "start check sort result", KPC(this));

  while (OB_SUCC(ret) && OB_NOT_NULL(cur_node)) {
    ObTxData *tx_data = ObTxData::get_tx_data_by_sort_list_node(cur_node);
    cur_node = cur_node->next_;
    int64_t cur_key = get_key(*tx_data);
    if (cur_key < pre_key) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "check sort result failed", KR(ret), KPC(pre_tx_data), KPC(tx_data));
    } else {
      pre_key = cur_key;
      pre_tx_data = tx_data;
    }
  }

  STORAGE_LOG(INFO, "finish check sort result", KR(ret), KPC(this));
  return ret;
}

/*
 * This quick sort based on linked list is a litte bit diffrent to normal quick sort. It uses the
 * key of head node to split the whole list to a small_list and a large_list. Then it connects
 * small_list, head_node and large_list.
 *
 * For example : First, we get a list like this:
 * ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐
 * │      │    │      │    │      │    │      │    │      │    │      │
 * │   4  ├───►│   8  ├───►│   6  ├───►│   2  ├───►│   7  ├───►│   1  ├───►nullptr
 * │      │    │      │    │      │    │      │    │      │    │      │
 * └──────┘    └──────┘    └──────┘    └──────┘    └──────┘    └──────┘
 *    ▲
 *    │
 *    │
 *   head
 *
 *  It is splited by the first node (key = 4):
 * ┌──────────────┐     ┌──────┐    ┌──────┐
 * │              │     │      │    │      │
 * │  small_head  ├────►│   2  ├───►│   1  ├───►nullptr
 * │              │     │      │    │      │
 * └──────────────┘     └──────┘    └──────┘
 *                                     ▲
 *                                     │
 *                                     │
 * ┌──────┐                         small_ptr
 * │      │
 * │   4  │────►nullptr
 * │      │
 * └──────┘
 *    ▲
 *    │
 *    │
 *   head
 *
 * ┌──────────────┐     ┌──────┐    ┌──────┐    ┌──────┐
 * │              │     │      │    │      │    │      │
 * │  large_head  ├────►│   8  ├───►│   6  ├───►│   7  ├───►nullptr
 * │              │     │      │    │      │    │      │
 * └──────────────┘     └──────┘    └──────┘    └──────┘
 *                                                 ▲
 *                                                 │
 *                                                 │
 *                                              large_ptr
 */
ObTxDataSortListNode *ObTxDataMemtable::quick_sort_(int64_t (*get_key)(const ObTxData &), ObTxDataSortListNode *head)
{
  if (OB_ISNULL(head) || OB_ISNULL(head->next_)) {
    return head;
  }

  ObTxDataSortListNode small_head;
  ObTxDataSortListNode large_head;
  ObTxDataSortListNode *small_ptr = &small_head;;
  ObTxDataSortListNode *large_ptr = &large_head;;
  ObTxDataSortListNode *node_ptr = head->next_;
  int64_t mid_key = get_key(*ObTxData::get_tx_data_by_sort_list_node(head));

  while (OB_NOT_NULL(node_ptr)) {
    ObTxDataSortListNode *cur_node = node_ptr;
    node_ptr = node_ptr->next_;
    int64_t cur_key = get_key(*ObTxData::get_tx_data_by_sort_list_node(cur_node));
    if (cur_key < mid_key) {
      small_ptr->next_ = cur_node;
      small_ptr = cur_node;
    } else {
      large_ptr->next_ = cur_node;
      large_ptr = cur_node;
    }
  }

  small_ptr->next_ = head;
  head->next_ = nullptr;
  large_ptr->next_ = nullptr;

  small_head.next_ = quick_sort_(get_key, small_head.next_);
  large_head.next_ = quick_sort_(get_key, large_head.next_);

  head->next_ = large_head.next_;

  return small_head.next_;
}

void ObTxDataMemtable::merge_sort_(int64_t (*get_key)(const ObTxData &), ObTxDataSortListNode *&head)
{
  ObTxDataSortListNode *left_list = nullptr;
  ObTxDataSortListNode *right_list = nullptr;


  if (OB_ISNULL(head) || OB_ISNULL(head->next_)) {
    return;
  }

  split_list_(head, left_list, right_list);

  // TODO : a non-recursive implementation
  merge_sort_(get_key, left_list);
  merge_sort_(get_key, right_list);

  head = merge_sorted_list_(get_key, left_list, right_list);
}

ObTxDataSortListNode *ObTxDataMemtable::merge_sorted_list_(int64_t (*get_key)(const ObTxData &),
                                                           ObTxDataSortListNode *left_list,
                                                           ObTxDataSortListNode *right_list)
{
  ObTxDataSortListNode res;
  ObTxDataSortListNode *insert_pos = &res;
  bool is_first_loop = true;
  ObTransID left_key = INT64_MAX;
  ObTransID right_key = INT64_MAX;

  while (nullptr != left_list && nullptr != right_list) {
    if (OB_UNLIKELY(is_first_loop)) {
      left_key = get_key(*ObTxData::get_tx_data_by_sort_list_node(left_list));
      right_key = get_key(*ObTxData::get_tx_data_by_sort_list_node(right_list));
      is_first_loop = false;
    }

    if (cmp_key_(left_key, right_key) < 0) {
      insert_pos->next_ = left_list;
      left_list = left_list->next_;
      if (OB_NOT_NULL(left_list)) {
        left_key = get_key(*ObTxData::get_tx_data_by_sort_list_node(left_list));
      }
    } else {
      insert_pos->next_ = right_list;
      right_list = right_list->next_;
      if (OB_NOT_NULL(right_list)) {
        right_key = get_key(*ObTxData::get_tx_data_by_sort_list_node(right_list));
      }
    }

    insert_pos = insert_pos->next_;
  }

  if (OB_ISNULL(left_list)) {
    insert_pos->next_ = right_list;
  } else {
    insert_pos->next_ = left_list;
  }

  return res.next_;
}

void ObTxDataMemtable::split_list_(ObTxDataSortListNode *head,
                                   ObTxDataSortListNode *&left_list,
                                   ObTxDataSortListNode *&right_list)
{
  ObTxDataSortListNode *slow = head;
  ObTxDataSortListNode *fast = head->next_;

  while (nullptr != fast) {
    fast = fast->next_;
    if (nullptr != fast) {
      slow = slow->next_;
      fast = fast->next_;
    }
  }

  left_list = head;
  right_list = slow->next_;
  slow->next_ = nullptr;
}

int ObTxDataMemtable::cmp_key_(const int64_t &lhs, const int64_t &rhs)
{
  int int_ret = 0;
  if (lhs < rhs) {
    int_ret = -1;
  } else if (lhs == rhs) {
    int_ret = 0;
  } else {
    int_ret = 1;
  }
  return int_ret;
}

int ObTxDataMemtable::dump2text(const char *fname)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "start dump tx data memtable");
  char real_fname[OB_MAX_FILE_NAME_LENGTH];
  FILE *fd = NULL;

  STORAGE_LOG(INFO, "dump2text", K_(key));
  if (OB_ISNULL(fname)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "fanme is NULL");
  } else if (snprintf(real_fname, sizeof(real_fname), "%s.%ld", fname,
                      ::oceanbase::common::ObTimeUtility::current_time()) >= (int64_t)sizeof(real_fname)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "fname too long", K(fname));
  } else if (NULL == (fd = fopen(real_fname, "w"))) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN, "open file fail:", K(fname));
  } else {
    auto ls_id = freezer_->get_ls_id().id();
    auto tenant_id = MTL_ID();
    fprintf(fd, "tenant_id=%ld ls_id=%ld\n", tenant_id, ls_id);
    fprintf(fd,
        "memtable: key=%s is_inited=%d is_iterating=%d has_constructed_list=%d min_tx_log_ts=%ld max_tx_log_ts=%ld "
        "min_start_log_ts=%ld inserted_cnt=%ld deleted_cnt=%ld write_ref=%ld occupied_size=%ld last_insert_ts=%ld "
        "state=%d\n",
        S(key_),
        is_inited_,
        is_iterating_,
        has_constructed_list_,
        min_tx_log_ts_,
        max_tx_log_ts_,
        min_start_log_ts_,
        inserted_cnt_,
        deleted_cnt_,
        write_ref_,
        occupied_size_,
        last_insert_ts_,
        state_);
    fprintf(fd, "tx_data_count=%ld \n", tx_data_map_->count());
    DumpTxDataMemtableFunctor fn(fd);
    tx_data_map_->for_each(fn);
  }
  if (NULL != fd) {
    fprintf(fd, "end of tx data memtable\n");
    fclose(fd);
    fd = NULL;
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "dump_tx_data_memtable fail", K(fname), K(ret));
  }
  return ret;
}

void ObTxDataMemtable::DEBUG_dump_sort_list_node_2_text(const char *fname)
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "start dump tx data sort list for debug");
  char real_fname[OB_MAX_FILE_NAME_LENGTH];
  FILE *fd = NULL;

  STORAGE_LOG(INFO, "dump2text for debug", K_(key));
  if (OB_ISNULL(fname)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "fanme is NULL");
  } else if (snprintf(real_fname, sizeof(real_fname), "%s.%ld", fname,
                      ::oceanbase::common::ObTimeUtility::current_time()) >= (int64_t)sizeof(real_fname)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "fname too long", K(fname));
  } else if (NULL == (fd = fopen(real_fname, "w"))) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN, "open file fail:", K(fname));
  } else {
    auto ls_id = freezer_->get_ls_id().id();
    auto tenant_id = MTL_ID();
    fprintf(fd, "tenant_id=%ld ls_id=%ld\n", tenant_id, ls_id);
    fprintf(fd,
        "memtable: key=%s is_inited=%d is_iterating=%d has_constructed_list=%d min_tx_log_ts=%ld max_tx_log_ts=%ld "
        "min_start_log_ts=%ld inserted_cnt=%ld deleted_cnt=%ld write_ref=%ld occupied_size=%ld last_insert_ts=%ld "
        "state=%d\n",
        S(key_),
        is_inited_,
        is_iterating_,
        has_constructed_list_,
        min_tx_log_ts_,
        max_tx_log_ts_,
        min_start_log_ts_,
        inserted_cnt_,
        deleted_cnt_,
        write_ref_,
        occupied_size_,
        last_insert_ts_,
        state_);
    fprintf(fd, "tx_data_count=%ld \n", tx_data_map_->count());
    DumpTxDataMemtableFunctor fn(fd);

    auto node = sort_list_head_.next_;
    transaction::ObTransID unuse_key = 1;
    while (OB_NOT_NULL(node)) {
      auto tx_data = ObTxData::get_tx_data_by_sort_list_node(node);
      node = node->next_;

      fn(unuse_key, tx_data);
    }
  }

  if (NULL != fd) {
    fprintf(fd, "end of tx data memtable\n");
    fclose(fd);
    fd = NULL;
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "dump_tx_data_memtable fail", K(fname), K(ret));
  }
}

const char *ObTxDataMemtable::get_state_string()
{
  STATIC_ASSERT(int(State::ACTIVE) == 0, "Invalid State Enum");
  STATIC_ASSERT(int(State::FREEZING) == 1, "Invalid State Enum");
  STATIC_ASSERT(int(State::FROZEN) == 2, "Invalid State Enum");
  STATIC_ASSERT(int(State::RELEASED) == 3, "Invalid State Enum");
  STATIC_ASSERT(int(State::INVALID) == 4, "Invalid State Enum");
  STATIC_ASSERT(int(State::STATE_CNT) == 5, "Invalid State Enum");
  const static int cnt = int(State::STATE_CNT);
  const static char STATE_TO_CHAR[cnt][10] = {"ACTIVE", "FREEZING", "FROZEN", "RELEASED", "INVALID"};
  return STATE_TO_CHAR[int(state_)];
}

// ********************* Derived functions which are not supported *******************

int ObTxDataMemtable::get(const storage::ObTableIterParam &param,
                          storage::ObTableAccessContext &context,
                          const blocksstable::ObDatumRowkey &rowkey,
                          ObStoreRowIterator *&row_iter)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(param);
  UNUSED(context);
  UNUSED(rowkey);
  UNUSED(row_iter);
  return ret;
}

int ObTxDataMemtable::multi_get(const ObTableIterParam &param,
                                ObTableAccessContext &context,
                                const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys,
                                ObStoreRowIterator *&row_iter)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(param);
  UNUSED(context);
  UNUSED(rowkeys);
  UNUSED(row_iter);
  return ret;
}

int ObTxDataMemtable::multi_scan(const ObTableIterParam &param,
                                 ObTableAccessContext &context,
                                 const common::ObIArray<blocksstable::ObDatumRange> &ranges,
                                 ObStoreRowIterator *&row_iter)

{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(param);
  UNUSED(context);
  UNUSED(ranges);
  UNUSED(row_iter);
  return ret;
}

int ObTxDataMemtable::get_frozen_schema_version(int64_t &schema_version) const
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(schema_version);
  return ret;
}

int ObTxDataMemtable::get(const storage::ObTableIterParam &param,
                          storage::ObTableAccessContext &context,
                          const blocksstable::ObDatumRowkey &rowkey,
                          blocksstable::ObDatumRow &row)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(param);
  UNUSED(context);
  UNUSED(rowkey);
  UNUSED(row);
  return ret;
}

int ObTxDataMemtable::set(storage::ObStoreCtx &ctx,
                          const uint64_t table_id,
                          const storage::ObTableReadInfo &read_info,
                          const common::ObIArray<share::schema::ObColDesc> &columns,
                          const storage::ObStoreRow &row)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(read_info);
  UNUSED(columns);
  UNUSED(row);
  return ret;
}

int ObTxDataMemtable::lock(storage::ObStoreCtx &ctx,
                           const uint64_t table_id,
                           const storage::ObTableReadInfo &read_info,
                           common::ObNewRowIterator &row_iter)
{

  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(read_info);
  UNUSED(row_iter);
  return ret;
}

int ObTxDataMemtable::lock(storage::ObStoreCtx &ctx,
                           const uint64_t table_id,
                           const storage::ObTableReadInfo &read_info,
                           const common::ObNewRow &row)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(read_info);
  UNUSED(row);
  return ret;
}

int ObTxDataMemtable::lock(storage::ObStoreCtx &ctx,
                           const uint64_t table_id,
                           const storage::ObTableReadInfo &read_info,
                           const blocksstable::ObDatumRowkey &rowkey)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(ctx);
  UNUSED(table_id);
  UNUSED(read_info);
  UNUSED(rowkey);
  return ret;
}




}  // namespace storage

}  // namespace oceanbase
