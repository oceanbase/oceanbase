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
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx/ob_tx_data_functor.h"
#include "storage/tx_table/ob_tx_data_memtable_mgr.h"
#include "storage/tx_table/ob_tx_data_table.h"
#include "storage/tx_table/ob_tx_table_iterator.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"

namespace oceanbase
{

using namespace transaction;
using namespace share;

namespace storage
{


int64_t ObTxDataMemtable::PERIODICAL_SELECT_INTERVAL_NS = 1000LL * 1000LL * 1000LL;

int ObTxDataMemtable::init(const ObITable::TableKey &table_key,
                           ObTxDataMemtableMgr *memtable_mgr,
                           storage::ObFreezer *freezer,
                           const int64_t buckets_cnt)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init tx data memtable twice", KR(ret), K(table_key), KPC(memtable_mgr));
  } else if (OB_ISNULL(memtable_mgr)) {
    ret = OB_ERR_NULL_VALUE;
    STORAGE_LOG(WARN, "the tx_data_allocator is nullptr", KR(ret), K(table_key), KPC(memtable_mgr));
  } else if (OB_FAIL(ObITable::init(table_key))) {
    STORAGE_LOG(WARN, "ObITable::init fail", KR(ret), K(table_key), KPC(memtable_mgr));
  } else if (FALSE_IT(init_arena_allocator_())) {
  } else if (OB_FAIL(init_tx_data_map_(buckets_cnt))) {
    STORAGE_LOG(WARN, "init tx data map failed.", KR(ret), K(table_key), KPC(memtable_mgr));
  } else if (OB_FAIL(buf_.reserve(common::OB_MAX_VARCHAR_LENGTH))) {
    STORAGE_LOG(WARN, "reserve space for tx data memtable failed.", KR(ret), K(table_key), KPC(memtable_mgr));
  } else if (OB_FAIL(set_freezer(freezer))) {
    STORAGE_LOG(WARN, "fail to set freezer", K(ret), KP(freezer));
  } else {
    for (int i = 0; i < MAX_TX_DATA_TABLE_CONCURRENCY; i++) {
      min_tx_scn_[i] = SCN::max_scn();
      min_start_scn_[i] = SCN::max_scn();
      occupied_size_[i] = 0;
      total_undo_node_cnt_[i] = 0;
    }
    ls_id_ = freezer_->get_ls_id();
    construct_list_done_ = false;
    pre_process_done_ = false;
    do_recycle_ = false;
    max_tx_scn_.set_min();
    inserted_cnt_ = 0;
    deleted_cnt_ = 0;
    write_ref_ = 0;
    last_insert_ts_ = 0;
    stat_change_ts_.reset();
    state_ = ObTxDataMemtable::State::ACTIVE;
    sort_list_head_.reset();
    memtable_mgr_ = memtable_mgr;
    row_key_array_.reuse();

    DEBUG_last_start_scn_ = SCN::min_scn();
    is_inited_ = true;
  }

  return ret;
}

int ObTxDataMemtable::init_tx_data_map_(const int64_t buckets_cnt)
{
  int ret = OB_SUCCESS;

  void *data_map_ptr = arena_allocator_.alloc(sizeof(*tx_data_map_));
  if (OB_ISNULL(data_map_ptr)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "allocate memory of tx_data_map_ failed", KR(ret));
  } else {
    int64_t real_buckets_cnt = buckets_cnt;
    if (real_buckets_cnt < ObTxDataHashMap::MIN_BUCKETS_CNT) {
      real_buckets_cnt = ObTxDataHashMap::MIN_BUCKETS_CNT;
    } else if (real_buckets_cnt > ObTxDataHashMap::MAX_BUCKETS_CNT) {
      real_buckets_cnt = ObTxDataHashMap::MAX_BUCKETS_CNT;
    }
    tx_data_map_ = new (data_map_ptr) TxDataMap(arena_allocator_, real_buckets_cnt);
    if (OB_FAIL(tx_data_map_->init())) {
      STORAGE_LOG(WARN, "tx_data_map_ init failed", KR(ret));
    }
  }
  return ret;
}

void ObTxDataMemtable::init_arena_allocator_()
{
  ObMemAttr attr;
  attr.tenant_id_ = MTL_ID();
  attr.label_ = "MEMTABLE_ARENA";
  attr.ctx_id_ = ObCtxIds::TX_DATA_TABLE;
  arena_allocator_.set_attr(attr);
}

void ObTxDataMemtable::reset()
{
  for (int i = 0; i < MAX_TX_DATA_TABLE_CONCURRENCY; i++) {
    min_tx_scn_[i] = SCN::max_scn();
    min_start_scn_[i] = SCN::max_scn();
    occupied_size_[i] = 0;
    total_undo_node_cnt_[i] = 0;
  }
  construct_list_done_ = false;
  pre_process_done_ = false;
  do_recycle_ = false;
  key_.reset();
  max_tx_scn_.set_min();
  inserted_cnt_ = 0;
  deleted_cnt_ = 0;
  write_ref_ = 0;
  last_insert_ts_ = 0;
  state_ = ObTxDataMemtable::State::INVALID;
  sort_list_head_.reset();
  if (OB_NOT_NULL(tx_data_map_)) {
    tx_data_map_->destroy();
    arena_allocator_.free(tx_data_map_);
    tx_data_map_ = nullptr;
  }
  memtable_mgr_ = nullptr;
  buf_.reset();
  arena_allocator_.reset();
  row_key_array_.reuse();
  freezer_ = nullptr;
  ObITable::reset();
  DEBUG_last_start_scn_ = SCN::min_scn();
  stat_change_ts_.reset();
  is_inited_ = false;
  reset_trace_id();
}

int ObTxDataMemtable::insert(ObTxData *tx_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx data memtable is not init");
  } else if (OB_UNLIKELY(ObTxDataMemtable::State::FROZEN <= get_state())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR,
                "trying to insert a tx data into a tx data memtable in frozen/dumped state.",
                KR(ret),
                KP(this),
                KPC(this));
  } else if (OB_ISNULL(tx_data)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "tx data is nullptr", KR(ret));
  } else if (OB_ISNULL(tx_data_map_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected null value of tx_data_map_", KR(ret));
  } else if (OB_FAIL(tx_data_map_->insert(tx_data->tx_id_, tx_data))) {
    STORAGE_LOG(ERROR, "insert the tx data into tx_data_map_ fail.", KP(tx_data), KPC(tx_data),
                  KR(ret), KP(tx_data_map_));
  } else {
    // insert_and_get success
    max_tx_scn_.inc_update(tx_data->end_scn_);
    atomic_update_(tx_data);
    ATOMIC_INC(&inserted_cnt_);
    if (OB_UNLIKELY(tx_data->op_guard_.is_valid() && tx_data->op_guard_->get_undo_status_list().undo_node_cnt_ >= 10)) {
      if (tx_data->op_guard_->get_undo_status_list().undo_node_cnt_ == 10 || tx_data->op_guard_->get_undo_status_list().undo_node_cnt_ % 100 == 0) {
        STORAGE_LOG(INFO,
                    "attention! this tx write too many rollback to savepoint log",
                    "ls_id", get_ls_id(),
                    "tx_id", tx_data->tx_id_,
                    "state", ObTxData::get_state_string(tx_data->state_),
                    "undo_node_cnt", tx_data->op_guard_->get_undo_status_list().undo_node_cnt_,
                    "newest_undo_node", tx_data->op_guard_->get_undo_status_list().head_,
                    K(tx_data->start_scn_),
                    K(tx_data->end_scn_));
      }
    }
  }

  return ret;
}

void ObTxDataMemtable::atomic_update_(ObTxData *tx_data)
{
  int64_t thread_idx = common::get_itid() & MAX_CONCURRENCY_MOD_MASK;
  min_tx_scn_[thread_idx].dec_update(tx_data->end_scn_);
  min_start_scn_[thread_idx].dec_update(tx_data->start_scn_);
  int64_t tx_data_size = 0;
  int64_t count = 0;
  if (tx_data->state_ == ObTxCommitData::RUNNING) {
    tx_data_size = TX_DATA_SLICE_SIZE;
  } else if (!tx_data->op_guard_.is_valid()) {
    tx_data_size = TX_DATA_SLICE_SIZE;
  } else {
    count = tx_data->op_guard_->get_undo_status_list().undo_node_cnt_;
    int64_t tx_op_size = tx_data->op_guard_->get_tx_op_size();
    tx_data_size = TX_DATA_SLICE_SIZE + TX_DATA_SLICE_SIZE +
                   count * TX_DATA_SLICE_SIZE + // undo status list
                   tx_op_size; // tx_op
  }
  ATOMIC_FAA(&occupied_size_[thread_idx], tx_data_size);
  ATOMIC_FAA(&total_undo_node_cnt_[thread_idx], count);
}

int ObTxDataMemtable::get_tx_data(const ObTransID &tx_id, ObTxDataGuard &tx_data_guard)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tx data memtable is not init");
  } else if (OB_FAIL(tx_data_map_->get(tx_id, tx_data_guard))) {
    // This tx data is not in this tx data memtable
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "get tx data from tx data map failed.", KR(ret), KPC(this));
    }
  } else {
    // get tx data success
  }
  return ret;
}

int ObTxDataMemtable::pre_process_for_merge()
{
  int ret = OB_SUCCESS;
  ObTxDataGuard fake_tx_data_guard;
  ObTimeGuard tg("pre process for tx datas merge", 0);
  fake_tx_data_guard.reset();
  bool inserted_fake = false;

  if (State::FROZEN != state_) {
    // only do pre process for frozen tx data memtable
  } else if (pre_process_done_) {
    STORAGE_LOG(INFO, "call pre process more than once. skip pre process.");
  } else if (OB_FAIL(memtable_mgr_->get_tx_data_table()->alloc_tx_data(fake_tx_data_guard, false /* enable_throttle */))) {
    STORAGE_LOG(WARN, "allocate tx data from tx data table failed.", KR(ret), KPC(this));
  } else if (OB_FAIL(prepare_tx_data_list())) {
    STORAGE_LOG(WARN, "prepare tx data list failed.", KR(ret), KPC(this));
  } else if (OB_FAIL(do_sort_by_start_scn_())) {
    STORAGE_LOG(WARN, "do sort by start log ts failed.", KR(ret), KPC(this));
  } else if (OB_FAIL(pre_process_commit_version_row_(fake_tx_data_guard.tx_data()))) {
    STORAGE_LOG(WARN, "process commit version row failed.", KR(ret), KPC(this));
  } else if (FALSE_IT(tg.click("finish process commit version"))) {
  } else if (OB_FAIL(insert_fake_tx_data_to_list_and_map_(fake_tx_data_guard.tx_data()))) {
    STORAGE_LOG(WARN, "insert fake tx data to list and map failed.", KR(ret), KPC(this));
  } else if (OB_FAIL(do_sort_by_tx_id_())) {
    STORAGE_LOG(WARN, "do sort by tx id failed.", KR(ret), KPC(this));
  } else {
    pre_process_done_ = true;
    tg.click("finish pre process");
  }

  STORAGE_LOG(INFO,
              "[TX DATA MERGE]pre process for merge done",
              KR(ret),
              K(get_ls_id()),
              KP(this),
              K(tg),
              K(fake_tx_data_guard),
              KPC(this));
  return ret;
}

int ObTxDataMemtable::prepare_tx_data_list()
{
  int ret = OB_SUCCESS;

  if (ObTxDataMemtable::State::FROZEN != state_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Trying to dump a non-frozen tx data memtable.", KR(ret), KP(this));
  } else if (construct_list_done_) {
    STORAGE_LOG(INFO, "construct list more than once. skip this time", KP(this));
  } else if (OB_FAIL(construct_list_for_sort_())) {
    STORAGE_LOG(WARN, "construct list for sort failed.", KR(ret));
  } else {
    construct_list_done_ = true;
  }

  return ret;
}

// We need construct a list to hold the tx datas and sort them. Here we iterate the tx_data_hash_map and concat them.
int ObTxDataMemtable::construct_list_for_sort_()
{
  int ret = OB_SUCCESS;
  TxDataMap::Iterator iter(*tx_data_map_);

  ObTxDataGuard tx_data_guard;
  tx_data_guard.reset();
  ObTxDataLinkNode *cur_node = &sort_list_head_;
  int64_t DEBUG_iter_cnt = 0;
  while (OB_SUCC(iter.get_next(tx_data_guard))) {
    cur_node->next_ = tx_data_guard.tx_data();
    cur_node = &(tx_data_guard.tx_data()->sort_list_node_);
    DEBUG_iter_cnt++;
  }

  if (OB_ITER_END == ret) {
    cur_node->next_ = nullptr;
    ret = OB_SUCCESS;
  } else {
    STORAGE_LOG(WARN, "construct list for sort failed", KR(ret), KPC(this));
  }

  return ret;
}

// This function is called after sorting tx_data by start_log_ts and the following steps is
// executed:
// 1. Select (start_log_ts, commit_version) point per second and push them into an array.
// 2. Read (start_log_ts, commit_version) array from the latest tx data sstable.
// 3. Get the recycle_scn to filtrate the point which is not needed any more.
// 4. Merge the arrays above. This procedure should filtrate the points are not needed and keep the
// commit versions monotonically increasing.
// 5. Serialize the merged array into one sstable row.
int ObTxDataMemtable::pre_process_commit_version_row_(ObTxData *fake_tx_data)
{
  int ret = OB_SUCCESS;

  SCN recycle_scn = SCN::min_scn();
  do_recycle_ = true;
  ObCommitVersionsArray cur_commit_versions;
  ObCommitVersionsArray past_commit_versions;
  ObCommitVersionsArray merged_commit_versions;

  int64_t current_time = ObClockGenerator::getClock();
  int64_t prev_recycle_time = memtable_mgr_->get_mini_merge_recycle_commit_versions_ts();
  if (current_time - prev_recycle_time < MINI_RECYCLE_COMMIT_VERSIONS_INTERVAL_US) {
    // tx data mini merge do not recycle commit versions array every time
    do_recycle_ = false;
  } else {
    do_recycle_ = true;
  }
  STORAGE_LOG(
      INFO, "pre-process commit versions row", K(get_ls_id()), K(do_recycle_), K(current_time), K(prev_recycle_time));

  if (OB_FAIL(fill_in_cur_commit_versions_(cur_commit_versions)/*step 1*/)) {
    STORAGE_LOG(WARN, "periodical select commit version failed.", KR(ret));
  } else if (OB_FAIL(get_past_commit_versions_(past_commit_versions)/*step 2*/)) {
    STORAGE_LOG(WARN, "get past commit versions failed.", KR(ret));
  } else if (do_recycle_ && OB_FAIL(memtable_mgr_->get_tx_data_table()->get_recycle_scn(recycle_scn) /*step 3*/)) {
    STORAGE_LOG(WARN, "get recycle ts failed.", KR(ret));
  } else if (OB_FAIL(merge_cur_and_past_commit_verisons_(recycle_scn, cur_commit_versions,/*step 4*/
                                                         past_commit_versions,
                                                         merged_commit_versions))) {
    STORAGE_LOG(WARN, "merge current and past commit versions failed.", KR(ret));
  } else if (!merged_commit_versions.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR,
        "invalid merged commit versions",
        KR(ret),
        K(cur_commit_versions),
        K(past_commit_versions),
        K(merged_commit_versions));
  } else {
    int64_t pos = 0;
    int64_t serialize_size = merged_commit_versions.get_serialize_size();

    if (serialize_size > common::OB_MAX_VARCHAR_LENGTH) {
      // TODO : @gengli multiple rows
      int ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "serialize data is too large", KR(ret), K(serialize_size));
    } else if (OB_FAIL(buf_.reserve(serialize_size))) {
      STORAGE_LOG(WARN, "Failed to reserve local buffer", KR(ret), K(serialize_size), K(merged_commit_versions));
    } else if (OB_FAIL(merged_commit_versions.serialize(buf_.get_ptr(), serialize_size, pos))){
      STORAGE_LOG(WARN, "serialize merged commit versions failed", KR(ret), K(merged_commit_versions));
    } else {
      // pre_processs commit version row done.
      // Here we use commit_version_ and start_log_ts as two int64_t
      fake_tx_data->tx_id_ = INT64_MAX;
      fake_tx_data->commit_version_.convert_for_tx(serialize_size);
      fake_tx_data->start_scn_.convert_for_tx((int64_t)buf_.get_ptr());
    }
  }

  return ret;
}

int ObTxDataMemtable::fill_in_cur_commit_versions_(ObCommitVersionsArray &cur_commit_versions)
{
  int ret = OB_SUCCESS;
  ObCommitVersionsArray::Node node;
  ProcessCommitVersionData process_data(sort_list_head_.next_, SCN::min_scn(), SCN::min_scn());
  DEBUG_last_start_scn_ = SCN::min_scn();

  while (OB_SUCC(periodical_get_next_commit_version_(process_data, node))) {
    cur_commit_versions.array_.push_back(node);
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected error occurs when periodical select commit version", KR(ret),
                KPC(this));
  }

  return ret;
}

int ObTxDataMemtable::periodical_get_next_commit_version_(ProcessCommitVersionData &process_data,
                                                          ObCommitVersionsArray::Node &node)
{
  int ret = OB_SUCCESS;
  ObTxData *tx_data = nullptr;
  SCN &cur_max_commit_version = process_data.cur_max_commit_version_;
  SCN &pre_start_scn = process_data.pre_start_scn_;

  while (OB_SUCC(ret) && nullptr != process_data.cur_tx_data_) {
    ObTxData *tmp_tx_data = process_data.cur_tx_data_;
    process_data.cur_tx_data_ = process_data.cur_tx_data_->sort_list_node_.next_;
    process_data.DEBUG_iter_commit_scn_cnt_++;

    // avoid rollback or abort transaction influencing commit versions array
    if (ObTxData::COMMIT != tmp_tx_data->state_) {
      continue;
    } else {
      tx_data = tmp_tx_data;
    }

    if (process_data.DEBUG_last_start_scn_ > tx_data->start_scn_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected start log ts order", K(DEBUG_last_start_scn_), KPC(tx_data));
      break;
    } else {
      process_data.DEBUG_last_start_scn_ = tx_data->start_scn_;
    }

    // update pre_commit_version
    if (tx_data->commit_version_ > cur_max_commit_version) {
      cur_max_commit_version = tx_data->commit_version_;
    }

    // If this tx data is the first tx data in sorted list or its start_log_ts is 1_s larger than
    // the pre_start_scn, we use this start_log_ts to calculate upper_trans_version
    if (SCN::min_scn() == pre_start_scn ||
        tx_data->start_scn_ >= SCN::plus(pre_start_scn, PERIODICAL_SELECT_INTERVAL_NS/*1s*/)) {
      pre_start_scn = tx_data->start_scn_;
      break;
    }
  }

  if (nullptr != tx_data) {
    node.start_scn_ = tx_data->start_scn_;
    // use cur_max_commit_version_ to keep the commit versions monotonically increasing
    node.commit_version_ = cur_max_commit_version;
    tx_data = nullptr;
  } else if (nullptr == process_data.cur_tx_data_) {
    ret = OB_ITER_END;
  }

  return ret;
}

int ObTxDataMemtable::get_past_commit_versions_(ObCommitVersionsArray &past_commit_versions)
{
  int ret = OB_SUCCESS;
  ObLSTabletService *tablet_svr = get_tx_data_memtable_mgr()->get_ls_tablet_svr();
  const ObTableIterParam &iter_param = get_tx_data_memtable_mgr()->get_tx_data_table()->get_read_schema().iter_param_;
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;

  if (OB_ISNULL(tablet_svr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "tablet svr is nullptr", KR(ret), KPC(this));
  } else if (OB_FAIL(tablet_svr->get_tablet(LS_TX_DATA_TABLET, tablet_handle))) {
    STORAGE_LOG(WARN, "get tablet from ls tablet service failed.", KR(ret));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid tablet handle", KR(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(wrapper))) {
    STORAGE_LOG(WARN, "get table store fail", KR(ret), K(tablet_handle));
  } else {
    ObSSTable *sstable = static_cast<ObSSTable *>(wrapper.get_member()->get_minor_sstables().get_boundary_table(true));
    if (OB_NOT_NULL(sstable)) {
      ObStorageMetaHandle sstable_handle;
      ObSSTable *tmp_sstable = nullptr;
      if (sstable->is_loaded()) {
        tmp_sstable = sstable;
      } else if (OB_FAIL(ObTabletTableStore::load_sstable(sstable->get_addr(), sstable->is_co_sstable(), sstable_handle))) {
        STORAGE_LOG(WARN, "fail to load sstable", K(ret), KPC(sstable));
      } else if (OB_FAIL(sstable_handle.get_sstable(tmp_sstable))) {
        STORAGE_LOG(WARN, "fail to get sstable", K(ret), K(sstable_handle));
      }
      if (OB_SUCC(ret)) {
        ObCommitVersionsGetter getter(iter_param, tmp_sstable);
        if (OB_FAIL(getter.get_next_row(past_commit_versions))) {
          STORAGE_LOG(WARN, "get commit versions from tx data sstable failed.", KR(ret));
        }
      }
    } else {
      STORAGE_LOG(DEBUG, "There is no tx data sstable yet", KR(ret), KPC(sstable));
    }
  }

  return ret;
}

int ObTxDataMemtable::merge_cur_and_past_commit_verisons_(const SCN recycle_scn,
                                                          ObCommitVersionsArray &cur_commit_versions,
                                                          ObCommitVersionsArray &past_commit_versions,
                                                          ObCommitVersionsArray &merged_commit_versions)
{
  int ret = OB_SUCCESS;
  ObIArray<ObCommitVersionsArray::Node> &cur_arr = cur_commit_versions.array_;
  ObIArray<ObCommitVersionsArray::Node> &past_arr = past_commit_versions.array_;
  ObIArray<ObCommitVersionsArray::Node> &merged_arr = merged_commit_versions.array_;

  int64_t cur_size = cur_commit_versions.get_serialize_size();
  int64_t past_size = past_commit_versions.get_serialize_size();
  int64_t step_len = 1;
  if (cur_size + past_size > common::OB_MAX_VARCHAR_LENGTH) {
    STORAGE_LOG(INFO,
                "Too Much Pre-Process Data to Desirialize",
                K(recycle_scn),
                K(past_size),
                K(cur_size),
                "past_array_count", past_commit_versions.array_.count(),
                "cur_array_count", cur_commit_versions.array_.count());
    step_len = step_len + ((cur_size + past_size) / OB_MAX_VARCHAR_LENGTH);
  }

  // here we merge the past commit versions and current commit versions. To keep merged array correct, the node in past
  // array whose start_scn is larger than the minimum start_scn in current array will be dropped. The reason is in this
  // issue:
  SCN cur_min_start_scn = cur_arr.count() > 0 ? cur_arr.at(0).start_scn_ : SCN::max_scn();
  SCN max_commit_version = SCN::min_scn();
  if (OB_FAIL(merge_pre_process_node_(
          step_len, cur_min_start_scn, recycle_scn, past_arr, max_commit_version, merged_arr))) {
    STORAGE_LOG(WARN, "merge past commit versions failed.", KR(ret), K(past_arr), KPC(this));
  } else if (OB_FAIL(merge_pre_process_node_(
                 step_len, SCN::max_scn() /*start_scn_limit*/, recycle_scn, cur_arr, max_commit_version, merged_arr))) {
    STORAGE_LOG(WARN, "merge current commit versions failed.", KR(ret), K(cur_arr), KPC(this));
  } else if (0 == merged_arr.count()) {
    if (OB_FAIL(merged_arr.push_back(ObCommitVersionsArray::Node(SCN::max_scn(), SCN::max_scn())))) {
      STORAGE_LOG(WARN, "push back commit version node failed.", KR(ret), KPC(this));
    } else {
      STORAGE_LOG(INFO, "push back an INT64_MAX node for upper trans version calculation", K(merged_arr));
    }
  }

  STORAGE_LOG(INFO,
              "genenrate commit versions array finish.",
              K(recycle_scn),
              K(step_len),
              "past_array_count", past_commit_versions.array_.count(),
              "cur_array_count", cur_commit_versions.array_.count(),
              "merged_array_count", merged_commit_versions.array_.count());

  return ret;
}

int ObTxDataMemtable::merge_pre_process_node_(const int64_t step_len,
                                              const SCN start_scn_limit,
                                              const SCN recycle_scn,
                                              const ObIArray<ObCommitVersionsArray::Node> &data_arr,
                                              SCN &max_commit_version,
                                              ObIArray<ObCommitVersionsArray::Node> &merged_arr)
{
  int ret = OB_SUCCESS;
  int64_t arr_len = data_arr.count();
  if (arr_len <= 0) {
    // skip push back
  } else {
    // push back pre-process node except the last one
    int64_t i = 0;
    for (; OB_SUCC(ret) && i < arr_len - 1; i += step_len) {
      if (data_arr.at(i).start_scn_ >= start_scn_limit) {
        break;
      }
      max_commit_version = std::max(max_commit_version, data_arr.at(i).commit_version_);
      ObCommitVersionsArray::Node new_node(data_arr.at(i).start_scn_, max_commit_version);
      if (new_node.commit_version_ <= recycle_scn) {
        // this tx data should be recycled
        // do nothing
      } else if (OB_FAIL(merged_arr.push_back(new_node))) {
        STORAGE_LOG(WARN, "push back commit version node failed.", KR(ret), KPC(this));
      }
    }

    // push back the last pre-process node
    max_commit_version = std::max(max_commit_version, data_arr.at(arr_len - 1).commit_version_);
    if (OB_SUCC(ret) && data_arr.at(arr_len - 1).start_scn_ < start_scn_limit) {
      ObCommitVersionsArray::Node new_node(data_arr.at(arr_len - 1).start_scn_, max_commit_version);
      if (OB_FAIL(merged_arr.push_back(new_node))) {
        STORAGE_LOG(WARN, "push back commit version node failed.", KR(ret), KPC(this));
      }
    }

  }
  return ret;
}

int ObTxDataMemtable::insert_fake_tx_data_to_list_and_map_(ObTxData *fake_tx_data)
{
  int ret = OB_SUCCESS;

  // insert fake tx data into link hash map to release its memory after flushing
  if (OB_FAIL(tx_data_map_->insert(fake_tx_data->tx_id_.get_id(), fake_tx_data))) {
    STORAGE_LOG(WARN, "insert fake tx data into tx data map failed.", KR(ret), KPC(this));
  } else {
    fake_tx_data->sort_list_node_.next_ = sort_list_head_.next_;
    sort_list_head_.next_ = fake_tx_data;
    ATOMIC_INC(&inserted_cnt_);
  }

  return ret;
}

int64_t ObTxDataMemtable::get_occupied_size() const
{
  int64_t res = 0;
  res += (get_buckets_cnt() * sizeof(ObTxDataHashMap::ObTxDataHashHeader));
  for (int i = 0; i < MAX_TX_DATA_TABLE_CONCURRENCY; i++) {
    res += occupied_size_[i];
  }
  return res;
}

int64_t ObTxDataMemtable::get_total_undo_node_cnt() const
{
  int64_t res = 0;
  for (int i = 0; i < MAX_TX_DATA_TABLE_CONCURRENCY; i++) {
    res += total_undo_node_cnt_[i];
  }
  return res;
}

int ObTxDataMemtable::estimate_phy_size(const ObStoreRowkey *start_key,
                                        const ObStoreRowkey *end_key,
                                        int64_t &total_bytes,
                                        int64_t &total_rows)
{
  int ret = OB_SUCCESS;
  total_bytes = 0;
  for (int i = 0; i < MAX_TX_DATA_TABLE_CONCURRENCY; i++) {
    total_bytes += occupied_size_[i];
  }
  total_rows = inserted_cnt_ - deleted_cnt_;
  return ret;
}

int ObTxDataMemtable::get_split_ranges(const ObStoreRange &input_range,
                                       const int64_t part_cnt,
                                       common::ObIArray<common::ObStoreRange> &range_array)
{
  UNUSED(input_range);
  int ret = OB_SUCCESS;

  if (!pre_process_done_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "need do pre process before flushing tx data table", KR(ret), KPC(this));
  } else if (OB_FAIL(prepare_array_space_(part_cnt))) {
    STORAGE_LOG(WARN, "prepare array space failed", KR(ret), KPC(this));
  } else if (OB_FAIL(uniq_tx_id_())) {
    STORAGE_LOG(WARN, "uniq tx id failed", KR(ret), KPC(this));
  } else if (FALSE_IT(row_key_array_.at(0).assign(0))) {
  } else if (OB_FAIL(push_range_bounds_(part_cnt))) {
    STORAGE_LOG(WARN, "push range bounds failed", KR(ret), KPC(this));
  } else if (FALSE_IT(row_key_array_.at(part_cnt).assign(INT64_MAX))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt; i++) {
      ObStoreRange merge_range;
      merge_range.set_start_key(row_key_array_.at(i).get_rowkey());
      merge_range.set_end_key(row_key_array_.at(i + 1).get_rowkey());
      merge_range.set_left_open();
      merge_range.set_right_closed();
      if (OB_FAIL(range_array.push_back(merge_range))) {
        STORAGE_LOG(WARN, "Failed to push back the merge range to array", KR(ret), K(merge_range));
      }
    }
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    // reset ret code and use input_range as output result
    ret = OB_SUCCESS;
    if (OB_FAIL(range_array.push_back(input_range))) {
      STORAGE_LOG(WARN, "Failed to push back the merge range to array", KR(ret), K(input_range));
    }
  }

  STORAGE_LOG(INFO, "generate range bounds for parallel dump tx data memtable:", K(ret), K(row_key_array_), K(tx_id_2_range_));
  return ret;
}

int ObTxDataMemtable::prepare_array_space_(const int64_t part_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tx_id_2_range_.reserve(part_cnt + 1))) {
    STORAGE_LOG(WARN, "reserve space for tx id to count array failed.", KR(ret), KPC(this));
  } else if (OB_FAIL(row_key_array_.reserve(part_cnt + 1))) {
    STORAGE_LOG(WARN, "reserve space for fake row key array failed.", KR(ret), KPC(this));
  } else if (FALSE_IT(row_key_array_.reuse())) {
  } else if (FALSE_IT(tx_id_2_range_.reuse())) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i <= part_cnt; i++) {
      if (OB_FAIL(row_key_array_.push_back(TxDataFakeRowKey()))) {
        STORAGE_LOG(WARN, "push back tx data fake row key failed", KR(ret), K(row_key_array_));
      }
    }
  }
  return ret;
}

int ObTxDataMemtable::uniq_tx_id_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sort_list_head_.next_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected null sort list", KR(ret), KPC(this));
  } else {
    ObTxDataLinkNode *cur_node = &(sort_list_head_.next_->sort_list_node_);
    ObTxDataLinkNode *pre_node = &sort_list_head_;

    // iterate the whole sort list
    while (OB_NOT_NULL(cur_node->next_)) {
      ObTxData *cur_tx_data = ObTxData::get_tx_data_by_sort_list_node(cur_node);
      ObTxData *next_tx_data = cur_node->next_;

      if (OB_LIKELY(cur_tx_data->tx_id_ != next_tx_data->tx_id_)) {
        // for most case, the transaction do not have rollback to savepoint operation
        pre_node = cur_node;
        cur_node = &(cur_node->next_->sort_list_node_);
      } else {
        ATOMIC_INC(&deleted_cnt_);

        // delete some tx data if they have the same tx id
        if (cur_tx_data->end_scn_ > next_tx_data->end_scn_) {
          cur_tx_data->sort_list_node_.next_ = next_tx_data->sort_list_node_.next_;
        } else {
          pre_node->next_ = next_tx_data;
          cur_node = &(next_tx_data->sort_list_node_);
        }
      }
    }
  }
  return ret;
}

int ObTxDataMemtable::push_range_bounds_(const int64_t part_cnt)
{
  int ret = OB_SUCCESS;
  int64_t tx_data_cnt_to_flush = inserted_cnt_ - deleted_cnt_;
  if (tx_data_cnt_to_flush < part_cnt) {
    ret = OB_ENTRY_NOT_EXIST;
    STORAGE_LOG(WARN, "range too small, not enough rows ro split", KR(ret), K(part_cnt), KPC(this));
  } else {
    int64_t data_cnt_in_one_range = tx_data_cnt_to_flush / part_cnt;
    int64_t pre_range_tail_tx_id = 0;
    int64_t last_tx_data_count = tx_data_cnt_to_flush;
    ObTxDataLinkNode *pre_range_tail_node = &sort_list_head_;
    ObTxDataLinkNode *cur_node = &sort_list_head_;

    // iterate to find the bounds of the ranges
    for (int64_t range_idx = 1; OB_SUCC(ret) && range_idx < part_cnt; range_idx++) {
      for (int64_t data_cnt = 0; data_cnt < data_cnt_in_one_range; data_cnt++) {
        if (OB_NOT_NULL(cur_node->next_)) {
          cur_node = &(cur_node->next_->sort_list_node_);
        } else {
          // this break should not be executed
          break;
        }
      }

      // here we find the last node in a single range
      ObTxData *tx_data = ObTxData::get_tx_data_by_sort_list_node(cur_node);
      const int64_t tx_id = tx_data->tx_id_.get_id();
      row_key_array_.at(range_idx).assign(tx_id);

      // push back tx data count to tx_id_2_cnt_ array
      if (OB_FAIL(tx_id_2_range_.push_back(
              TxId2Range(transaction::ObTransID(pre_range_tail_tx_id), data_cnt_in_one_range, pre_range_tail_node)))) {
        STORAGE_LOG(WARN,
                    "push back tx id to count pair failed.",
                    KR(ret),
                    K(pre_range_tail_tx_id),
                    K(data_cnt_in_one_range),
                    K(part_cnt),
                    KPC(this));
      } else {
        pre_range_tail_tx_id = tx_id;
        pre_range_tail_node = cur_node;
        last_tx_data_count = last_tx_data_count - data_cnt_in_one_range;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tx_id_2_range_.push_back(TxId2Range(
                   transaction::ObTransID(pre_range_tail_tx_id), last_tx_data_count, pre_range_tail_node)))) {
      STORAGE_LOG(WARN, "push back tx id to count pair failed.", KR(ret));
    }
  }
  return ret;
}

int ObTxDataMemtable::get_iter_start_and_count(const transaction::ObTransID &tx_id, ObTxDataLinkNode *&start_node, int64_t &iterate_row_cnt)
{
  int ret = OB_SUCCESS;
  iterate_row_cnt = -1;
  for (int i = 0; i < tx_id_2_range_.count(); i++) {
    if (tx_id_2_range_.at(i).tx_id_ == tx_id) {
      start_node = tx_id_2_range_.at(i).sort_list_node_;
      iterate_row_cnt = tx_id_2_range_.at(i).tx_data_count_;
      break;
    }
  }

  if (-1 == iterate_row_cnt) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "this tx id is not in tx_id_2_cnt_ array.", KR(ret), K(tx_id), K(tx_id_2_range_));
  }
  return ret;
}

int ObTxDataMemtable::scan(const ObTableIterParam &param,
                           ObTableAccessContext &context,
                           const blocksstable::ObDatumRange &range,
                           ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;

  ObTxDataMemtableScanIterator *scan_iter_ptr = nullptr;
  void *scan_iter_buff = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ob tx data memtable is not inited.", KR(ret), KPC(this));
  } else if (!pre_process_done_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "need do pre process before flushing tx data table", KR(ret), KPC(this));
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
                          memtable_mgr_->get_tx_data_table()->get_read_schema().iter_param_, range))) {
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

bool ObTxDataMemtable::ready_for_flush()
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  SCN max_consequent_callbacked_scn = SCN::min_scn();
  if (OB_UNLIKELY(ObTxDataMemtable::State::RELEASED == state_
                  || ObTxDataMemtable::State::ACTIVE == state_
                  || ObTxDataMemtable::State::INVALID == state_)) {
    STORAGE_LOG(WARN, "call ready_for_flush() function on an incorrect state tx data memtable.",
                KPC(this));
  } else if (ObTxDataMemtable::State::FROZEN == state_) {
    bool_ret = true;
    STORAGE_LOG(INFO, "memtable is frozen yet.", KP(this));
  } else if (OB_FAIL(freezer_->get_max_consequent_callbacked_scn(max_consequent_callbacked_scn))) {
    STORAGE_LOG(WARN, "get_max_consequent_callbacked_scn failed", K(ret), K(get_ls_id()));
  } else if (max_consequent_callbacked_scn >= key_.scn_range_.end_scn_) {
    state_ = ObTxDataMemtable::State::FROZEN;
    STORAGE_LOG(INFO, "[TX DATA MERGE]tx data memtable is frozen", K(get_ls_id()), KP(this));
    set_snapshot_version(get_min_tx_scn());
    bool_ret = true;
    stat_change_ts_.ready_for_flush_time_ = ObTimeUtil::fast_current_time();
  } else if (REACH_TIME_INTERVAL(1 * 1000 * 1000 /* one second */)) {
    const SCN &freeze_scn = key_.scn_range_.end_scn_;
    if (ObTimeUtil::fast_current_time() - stat_change_ts_.frozen_time_ > 60 * 1000 * 1000 /* 60 seconds */) {
      STORAGE_LOG(WARN,
                  "tx data metmable is not ready for flush",
                  K(max_consequent_callbacked_scn),
                  K(freeze_scn),
                  K(stat_change_ts_));
    } else {
      STORAGE_LOG(INFO,
                  "tx data metmable is not ready for flush",
                  K(max_consequent_callbacked_scn),
                  K(freeze_scn),
                  K(stat_change_ts_));
    }
  }

  return bool_ret;
}

int ObTxDataMemtable::flush(const int64_t trace_id)
{
  int ret = OB_SUCCESS;
  compaction::ObTabletMergeDagParam param;
  param.ls_id_ = freezer_->get_ls_id();
  param.tablet_id_ = key_.tablet_id_;
  param.merge_type_ = compaction::MINI_MERGE;
  param.merge_version_ = ObVersionRange::MIN_VERSION;
  set_trace_id(trace_id);
  if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_tx_table_merge_dag(param, true /* is_emergency */))) {
    if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
      STORAGE_LOG(WARN, "failed to schedule tablet merge dag", K(ret));
    }
  } else {
    REPORT_CHECKPOINT_DIAGNOSE_INFO(update_schedule_dag_info, this, get_rec_scn(),
        get_start_scn(), get_end_scn());
    stat_change_ts_.create_flush_dag_time_ = ObTimeUtil::fast_current_time();
    STORAGE_LOG(INFO,
                "[TX DATA MERGE]schedule flush tx data memtable task done",
                KR(ret),
                K(get_ls_id()),
                KP(this),
                K(param),
                KPC(this));
  }
  return ret;
}

int64_t get_tx_id_(const ObTxData &tx_data) { return tx_data.tx_id_.get_id(); }

int ObTxDataMemtable::do_sort_by_tx_id_()
{
  int ret = OB_SUCCESS;
  // TODO : optimize merge sort with multiple threads
  merge_sort_(&get_tx_id_, sort_list_head_.next_);

  return ret;
}

int64_t get_start_ts_(const ObTxData &tx_data)
{
  return tx_data.start_scn_.get_val_for_tx(true/*ignore_invalid_scn*/);
}

int ObTxDataMemtable::do_sort_by_start_scn_()
{
  int ret = OB_SUCCESS;
  merge_sort_(&get_start_ts_, sort_list_head_.next_);
  // sort_list_head_.next_ = quick_sort_(&get_start_ts_, sort_list_head_.next_);

  return ret;
}

void ObTxDataMemtable::merge_sort_(int64_t (*get_key)(const ObTxData &), ObTxData *&head)
{
  ObTxData *left_list = nullptr;
  ObTxData *right_list = nullptr;

  if (OB_ISNULL(head) || OB_ISNULL(head->sort_list_node_.next_)) {
    return;
  }

  split_list_(head, left_list, right_list);

  // TODO : a non-recursive implementation
  merge_sort_(get_key, left_list);
  merge_sort_(get_key, right_list);

  head = merge_sorted_list_(get_key, left_list, right_list);
}

ObTxData *ObTxDataMemtable::merge_sorted_list_(int64_t (*get_key)(const ObTxData &),
                                                           ObTxData *left_list,
                                                           ObTxData *right_list)
{
  ObTxData dummy_head;
  ObTxData *insert_pos = &dummy_head;
  bool is_first_loop = true;
  ObTransID left_key = INT64_MAX;
  ObTransID right_key = INT64_MAX;

  while (nullptr != left_list && nullptr != right_list) {
    if (OB_UNLIKELY(is_first_loop)) {
      left_key = get_key(*left_list);
      right_key = get_key(*right_list);
      is_first_loop = false;
    }

    if (cmp_key_(left_key, right_key) < 0) {
      insert_pos->sort_list_node_.next_ = left_list;
      left_list = left_list->sort_list_node_.next_;
      if (OB_NOT_NULL(left_list)) {
        left_key = get_key(*left_list);
      }
    } else {
      insert_pos->sort_list_node_.next_ = right_list;
      right_list = right_list->sort_list_node_.next_;
      if (OB_NOT_NULL(right_list)) {
        right_key = get_key(*right_list);
      }
    }

    insert_pos = insert_pos->sort_list_node_.next_;
  }

  if (OB_ISNULL(left_list)) {
    insert_pos->sort_list_node_.next_ = right_list;
  } else {
    insert_pos->sort_list_node_.next_ = left_list;
  }

  return dummy_head.sort_list_node_.next_;
}

void ObTxDataMemtable::split_list_(ObTxData *head, ObTxData *&left_list, ObTxData *&right_list)
{
  ObTxData *slow = head;
  ObTxData *fast = head->sort_list_node_.next_;

  while (nullptr != fast) {
    fast = fast->sort_list_node_.next_;
    if (nullptr != fast) {
      slow = slow->sort_list_node_.next_;
      fast = fast->sort_list_node_.next_;
    }
  }

  left_list = head;
  right_list = slow->sort_list_node_.next_;
  slow->sort_list_node_.next_ = nullptr;
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

share::ObLSID ObTxDataMemtable::get_ls_id() const
{
  return OB_ISNULL(freezer_) ? share::ObLSID(ObLSID::INVALID_LS_ID) : freezer_->get_ls_id();
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
    int64_t ls_id = freezer_->get_ls_id().id();
    int64_t tenant_id = MTL_ID();
    fprintf(fd, "tenant_id=%ld ls_id=%ld\n", tenant_id, ls_id);
    fprintf(fd,
        "memtable: key=%s is_inited=%d construct_list_done=%d pre_process_done=%d do_recycle_=%d min_tx_log_ts=%s max_tx_log_ts=%s "
        "min_start_log_ts=%s inserted_cnt=%ld deleted_cnt=%ld write_ref=%ld occupied_size=%ld total_undo_node_cnt=%ld last_insert_ts=%ld "
        "state=%d\n",
        S(key_),
        is_inited_,
        construct_list_done_,
        pre_process_done_,
        do_recycle_,
        to_cstring(get_min_tx_scn()),
        to_cstring(max_tx_scn_),
        to_cstring(get_min_start_scn()),
        inserted_cnt_,
        deleted_cnt_,
        write_ref_,
        get_occupied_size(),
        get_total_undo_node_cnt(),
        last_insert_ts_,
        state_);
    fprintf(fd, "tx_data_count=%ld \n", tx_data_map_->count());
    DumpTxDataMemtableFunctor fn(fd);
    // tx_data_map_->for_each(fn);
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

int ObTxDataMemtable::DEBUG_try_calc_upper_and_check_(ObCommitVersionsArray &merged_commit_versions)
{
  int ret = OB_SUCCESS;

  ObTxData *cur_node = get_sorted_list_head()->next_;
  int64_t DEBUG_iter_cnt = 0;
  while (OB_SUCC(ret) && OB_NOT_NULL(cur_node)) {
    DEBUG_iter_cnt++;
    ObTxData *tx_data = cur_node;
    cur_node = cur_node->sort_list_node_.next_;

    if (ObTxData::COMMIT != tx_data->state_) {
      continue;
    }

    SCN upper_trans_version = SCN::min_scn();
    if (OB_FAIL(DEBUG_fake_calc_upper_trans_version(tx_data->start_scn_, upper_trans_version, merged_commit_versions))) {
      STORAGE_LOG(ERROR, "invalid upper trans version", KR(ret));
    } else if (upper_trans_version < tx_data->commit_version_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "invalid upper trans version", KR(ret), K(upper_trans_version), KPC(tx_data));
    }

    if (OB_FAIL(ret)) {
      DEBUG_print_start_scn_list_("start_scn_list");
      DEBUG_print_merged_commit_versions_(merged_commit_versions);
    }
  }
  if (OB_SUCC(ret) && DEBUG_iter_cnt != inserted_cnt_ - deleted_cnt_) {
    ret = OB_SUCCESS;
    STORAGE_LOG(ERROR, "invalid iter cnt", KR(ret), K(DEBUG_iter_cnt), K(inserted_cnt_), K(deleted_cnt_));
  }
  return ret;
}

int ObTxDataMemtable::DEBUG_fake_calc_upper_trans_version(const SCN sstable_end_scn,
                                                          SCN &upper_trans_version,
                                                          ObCommitVersionsArray &merged_commit_versions)
{
  int ret = OB_SUCCESS;

  ObIArray<ObCommitVersionsArray::Node> &array = merged_commit_versions.array_;
  int l = 0;
  int r = array.count() - 1;

  // Binary find the first start_log_ts that is greater than or equal to sstable_end_scn
  while (l < r) {
    int mid = (l + r) >> 1;
    if (array.at(mid).start_scn_ < sstable_end_scn) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }

  // Check if the start_log_ts is greater than or equal to the sstable_end_scn. If not, delay the
  // upper_trans_version calculation to the next time.
  if (0 == array.count() || !array.at(l).commit_version_.is_valid()) {
    upper_trans_version = SCN::max_scn();
    ret = OB_ERR_UNDEFINED;
    STORAGE_LOG(WARN, "unexpected array count or commit version", K(array.count()), K(array.at(l)));
  } else {
    upper_trans_version = array.at(l).commit_version_;
  }

  return ret;
}

void ObTxDataMemtable::DEBUG_print_start_scn_list_(const char* fname)
{
  int ret = OB_SUCCESS;
  const char *real_fname = fname;
  FILE *fd = NULL;

  if (NULL == (fd = fopen(real_fname, "w"))) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN, "open file fail:", K(real_fname));
  } else {
    int64_t tenant_id = MTL_ID();
    fprintf(fd, "tenant_id=%ld \n", tenant_id);
    ObTxData *cur_node = get_sorted_list_head()->next_;
    while (OB_NOT_NULL(cur_node)) {
      ObTxData *tx_data = cur_node;
      cur_node = cur_node->sort_list_node_.next_;

      fprintf(fd,
              "ObTxData : tx_id=%-19ld state=%-8s start_scn=%-19s "
              "end_scn=%-19s "
              "commit_version=%-19s\n",
              tx_data->tx_id_.get_id(),
              ObTxData::get_state_string(tx_data->state_),
              to_cstring(tx_data->start_scn_),
              to_cstring(tx_data->end_scn_),
              to_cstring(tx_data->commit_version_));
    }
  }

  if (NULL != fd) {
    fprintf(fd, "end of start log ts list\n");
    fclose(fd);
    fd = NULL;
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "dump start scn list fail", K(real_fname), K(ret));
  }
}

void ObTxDataMemtable::DEBUG_print_merged_commit_versions_(ObCommitVersionsArray &merged_commit_versions)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObCommitVersionsArray::Node> &array = merged_commit_versions.array_;
  const char *real_fname = "merge_commit_versions";
  FILE *fd = NULL;

  if (NULL == (fd = fopen(real_fname, "w"))) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN, "open file fail:", K(real_fname));
  } else {
    int64_t tenant_id = MTL_ID();
    fprintf(fd, "tenant_id=%ld \n", tenant_id);
    for (int i = 0; i < array.count(); i++) {
      fprintf(fd,
              "start_scn=%-19s "
              "commit_version=%-19s\n",
              to_cstring(array.at(i).start_scn_),
              to_cstring(array.at(i).commit_version_));
    }
  }

  if (NULL != fd) {
    fprintf(fd, "end of commit versions array\n");
    fclose(fd);
    fd = NULL;
  }
  if (OB_FAIL(ret)) {
    STORAGE_LOG(WARN, "dump commit versions fail", K(real_fname), K(ret));
  }
}

void ObTxDataMemtable::TEST_reset_tx_data_map_()
{
  int ret = OB_SUCCESS;
  tx_data_map_ = nullptr;
  init_tx_data_map_(ObTxDataHashMap::DEFAULT_BUCKETS_CNT);
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


}  // namespace storage

}  // namespace oceanbase
