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

#ifndef OCEANBASE_STORAGE_OB_TX_DATA_MEMTABLE
#define OCEANBASE_STORAGE_OB_TX_DATA_MEMTABLE

#include "storage/memtable/ob_memtable_interface.h"
#include "storage/tx/ob_tx_data_define.h"

namespace oceanbase
{
namespace storage
{
class ObLSTabletService;
class ObTxDataMemtableMgr;
class ObFreezer;
class ObTxDataTable;

// The basic unit that manages tx_data
class ObTxDataMemtable : public memtable::ObIMemtable
{
private:
  using SliceAllocator = ObSliceAlloc;
  static const int MAX_TX_DATA_TABLE_CONCURRENCY = 64;

public:
  // active   : freeze_ts is not set
  // freezing : freeze_ts is set, tx data is incomplete
  // frozen   : tx data is complete and sorted
  // dumped   : the memtable has been dumped
  enum class State
  {
    ACTIVE = 0,
    FREEZING = 1,
    FROZEN = 2,
    RELEASED = 3,
    INVALID = 4,
    STATE_CNT
  };

  // temporary logic to do self-freeze
  static const int64_t TX_DATA_MEMTABLE_SELF_FREEZE_THRESHOLD_CNT = 2000000;

public:  // ObTxDataMemtable
  ObTxDataMemtable()
    : ObIMemtable(),
      is_inited_(false),
      is_iterating_(false),
      has_constructed_list_(false),
      min_tx_log_ts_(0),
      max_tx_log_ts_(0),
      min_start_log_ts_(0),
      inserted_cnt_(0),
      deleted_cnt_(0),
      write_ref_(0),
      occupied_size_(0),
      last_insert_ts_(0),
      state_(ObTxDataMemtable::State::INVALID),
      sort_list_head_(),
      tx_data_map_(nullptr),
      slice_allocator_(nullptr),
      memtable_mgr_(nullptr),
      freezer_(nullptr)
  {
    reset_thread_local_list_();
  }
  ~ObTxDataMemtable() { reset(); }
  void reset();
  int init(const ObITable::TableKey &table_key,
           SliceAllocator *slice_allocator,
           ObTxDataMemtableMgr *memtable_mgr);

  /**
   * @brief Insert the tx data into this tx data memtable
   *
   * @param[in] tx_data the tx data waiting to be inserted
   */
  int insert(ObTxData *tx_data);

  /**
   * @brief Get the tx data with transaction id
   *
   * @param[in] tx_id the transaction id of tx data which is waiting to get
   * @param[out] tx_data_guard the tx data guard contains the tx data
   */
  int get_tx_data(const transaction::ObTransID &tx_id, ObTxDataGuard &tx_data_guard);

  /**
   * @brief Get the tx data without guard
   */
  int get_tx_data(const transaction::ObTransID &tx_id, ObTxData *&tx_data);

  void revert_tx_data(ObTxData *tx_data) { tx_data_map_->revert(tx_data); }

  /**
   * @brief This function is used by ObTxDataMemtableScanIterator and it will do the following
   * operations:
   * 1. Iterate the link hash map of tx data memtable to construct a tx data list
   * 2. Sort the list by trans_id of tx data for dumping
   */
  int prepare_tx_data_list();

  /**
   * @brief This function is used by ObTxDataMemtableScanIterator after all tx data is dumped. It
   * performs sorting similarly to prepare_tx_data_list() function by start_log_ts of tx data
   *
   */
  int prepare_commit_version_list();

  /**
   * @brief Check if this tx data memtable can be minor merge
   * See more details at ready_for_flush() function.
   */
  bool can_be_minor_merged() override;

  /**
   * @brief check if this tx data memtable contains a tx data
   *
   * @param tx_id the tx_id of tx data
   */
  bool contain_tx_data(transaction::ObTransID tx_id);

  bool can_iterate()
  {
    bool bool_ret = (false == ATOMIC_CAS(&is_iterating_, false, true));
    return bool_ret;
  }

  /**
   * @brief delete a tx data from tx data memtable
   *
   * @param tx_id the tx_id of tx data
   */
  int remove(transaction::ObTransID tx_id);

  bool need_self_freeze();

  /**
   * @brief dump tx data memtable to file
   *
   * @param[in] fname
   */
  int dump2text(const char *fname);

  void DEBUG_dump_sort_list_node_2_text(const char *fname);

  INHERIT_TO_STRING_KV("ObITable",
                       ObITable,
                       KP(this),
                       K_(is_inited),
                       K_(is_iterating),
                       K_(has_constructed_list),
                       K_(min_tx_log_ts),
                       K_(max_tx_log_ts),
                       K_(min_start_log_ts),
                       K_(snapshot_version),
                       K_(inserted_cnt),
                       K_(write_ref),
                       K_(occupied_size),
                       K_(state),
                       KP_(tx_data_map),
                       KP_(memtable_mgr));


public: /* derived from ObITable */

  virtual bool is_active_memtable() const override { return ObTxDataMemtable::State::ACTIVE == state_; }

  /**
   * @brief Scan this memtable to flush the tx data
   *
   * @param[in] param the param to iterate this memtable
   * @param[in] context the access context
   * @param[in] key_range not used
   * @param[out] row_iter the iterator which is used to iterate this memtable
   */
  virtual int scan(const ObTableIterParam &param,
                   ObTableAccessContext &context,
                   const blocksstable::ObDatumRange &key_range,
                   ObStoreRowIterator *&row_iter) override;
  // not supported
  virtual int get(const storage::ObTableIterParam &param,
                  storage::ObTableAccessContext &context,
                  const blocksstable::ObDatumRowkey &rowkey,
                  ObStoreRowIterator *&row_iter) override;
  // not supported
  virtual int multi_get(const ObTableIterParam &param,
                        ObTableAccessContext &context,
                        const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys,
                        ObStoreRowIterator *&row_iter) override;
  // not supported
  virtual int multi_scan(const ObTableIterParam &param,
                         ObTableAccessContext &context,
                         const common::ObIArray<blocksstable::ObDatumRange> &ranges,
                         ObStoreRowIterator *&row_iter) override;
  // not supported
  virtual int get_frozen_schema_version(int64_t &schema_version) const override;
  // check if this memtable is frozen
  virtual bool is_frozen_memtable() const { return ObTxDataMemtable::State::FROZEN == state_; }

public: /* derived from ObIMemtable */
  virtual int64_t get_occupied_size() const { return occupied_size_; }

  // not supported
  virtual int get(const storage::ObTableIterParam &param,
                  storage::ObTableAccessContext &context,
                  const blocksstable::ObDatumRowkey &rowkey,
                  blocksstable::ObDatumRow &row) override;
  // not supported
  virtual int set(storage::ObStoreCtx &ctx,
                  const uint64_t table_id,
                  const storage::ObTableReadInfo &read_info,
                  const common::ObIArray<share::schema::ObColDesc> &columns,
                  const storage::ObStoreRow &row) override;
  // not supported
  virtual int lock(storage::ObStoreCtx &ctx,
                   const uint64_t table_id,
                   const storage::ObTableReadInfo &read_info,
                   common::ObNewRowIterator &row_iter) override;
  // not supported
  virtual int lock(storage::ObStoreCtx &ctx,
                   const uint64_t table_id,
                   const storage::ObTableReadInfo &read_info,
                   const common::ObNewRow &row) override;
  // not supported
  virtual int lock(storage::ObStoreCtx &ctx,
                   const uint64_t table_id,
                   const storage::ObTableReadInfo &read_info,
                   const blocksstable::ObDatumRowkey &rowkey) override;

public:  // checkpoint
  int64_t get_rec_log_ts();

  // int freeze();

  int flush();
  
  /**
   * @brief Because of the random order of clog callbacks, the tx data in a freezing tx data
   * memtable may not completed. We must wait until the max_consequent_callbacked_log_ts is larger
   * than the end_log_ts of tx data memtable which means this memtable is now completed.
   */
  bool ready_for_flush();

public:  // getter && setter
  int64_t get_min_start_log_ts() { return ATOMIC_LOAD(&min_start_log_ts_); }
  int64_t get_tx_data_count() { return tx_data_map_->count(); }
  int64_t size() { return get_tx_data_count(); }
  int64_t get_inserted_count() { return inserted_cnt_; }
  int64_t get_deleted_count() { return deleted_cnt_; }
  int64_t inc_write_ref() { return ATOMIC_AAF(&write_ref_, 1); }
  int64_t dec_write_ref() { return ATOMIC_AAF(&write_ref_, -1); }
  int64_t get_write_ref() const override { return ATOMIC_LOAD(&write_ref_); }
  ObTxDataMemtable::State get_state() { return state_; }
  ObTxDataSortListNode *get_sorted_list_head() { return &sort_list_head_; }
  const char* get_state_string();
  ObTxDataMemtableMgr *get_tx_data_memtable_mgr() { return memtable_mgr_; }

  int64_t get_min_tx_log_ts() { return min_tx_log_ts_; }
  int64_t get_max_tx_log_ts() { return max_tx_log_ts_; }
  int set_freezer(ObFreezer *handler);
  void set_start_log_ts(const int64_t start_log_ts) {key_.log_ts_range_.start_log_ts_ = start_log_ts;}
  void set_end_log_ts() { key_.log_ts_range_.end_log_ts_ = max_tx_log_ts_; }
  void set_state(const ObTxDataMemtable::State &state) { state_ = state; }
  void set_has_constructed_list(bool val) { has_constructed_list_ = val; }
  void reset_is_iterating() { ATOMIC_STORE(&is_iterating_, false); }

private:  // ObTxDataMemtable
  int do_sort_by_tx_id_();
  void merge_sort_(int64_t (*get_key)(const ObTxData &), ObTxDataSortListNode *&head);

  ObTxDataSortListNode *quick_sort_(int64_t (*get_key)(const ObTxData &), ObTxDataSortListNode *head);
  ObTxDataSortListNode *merge_sorted_list_(int64_t (*get_key)(const ObTxData &),
                                           ObTxDataSortListNode *left_list,
                                           ObTxDataSortListNode *right_list);
  void split_list_(ObTxDataSortListNode *head,
                   ObTxDataSortListNode *&left_list,
                   ObTxDataSortListNode *&right_list);

  int do_sort_by_start_log_ts_();

  int cmp_key_(const int64_t &lhs, const int64_t &rhs);

  int DEBUG_check_sort_result_(int64_t (*get_key)(const ObTxData &));

  int construct_list_for_sort_();
  void reset_thread_local_list_();


private:  // ObTxDataMemtable
  bool is_inited_;
  bool is_iterating_;
  bool has_constructed_list_;

  // the minimum log ts of commit_log_ts in this tx data memtable
  int64_t min_tx_log_ts_;

  // the maximum log ts in this tx data memtable
  int64_t max_tx_log_ts_;

  // the minimum start log ts in this tx data memtable
  int64_t min_start_log_ts_;

  int64_t inserted_cnt_;

  int64_t deleted_cnt_;

  int64_t write_ref_;

  int64_t occupied_size_;

  int64_t last_insert_ts_;

  // the state of tx data memtable can be one of 4 kinds of state :
  // active, freezing, frozen, dumped
  ObTxDataMemtable::State state_;

  // the head node of sorted list which is used before dump
  ObTxDataSortListNode sort_list_head_;
  // use thread local list instead of foreach of link hash map can speed up constructing list for
  // sort.
  ObTxDataSortListNode local_sort_list_head_[MAX_TX_DATA_TABLE_CONCURRENCY];

  // the hash map sotres tx data
  TxDataMap *tx_data_map_;

  // the link hash map of tx data need the slice allocator of tx data table to construct because the
  // destruct of link hash map will free all tx data
  SliceAllocator *slice_allocator_;

  // used for freeze
  ObTxDataMemtableMgr *memtable_mgr_;

  // used for checkpoint executor
  storage::ObFreezer *freezer_;
};

class IterateTxDataMapForSortFunctor
{
public:
  explicit IterateTxDataMapForSortFunctor(ObTxDataSortListNode *head) : pre_sort_list_node_(head) {}

  bool operator()(const transaction::ObTransID &key, ObTxData *tx_data)
  {
    UNUSED(key);
    ObTxDataSortListNode *sort_list_node = ObTxData::get_sort_list_node_by_tx_data(tx_data);
    pre_sort_list_node_->next_ = sort_list_node;
    pre_sort_list_node_ = sort_list_node;

    return true;
  }

private:
  ObTxDataSortListNode *pre_sort_list_node_;
};

class DumpTxDataMemtableFunctor
{
public:
  explicit DumpTxDataMemtableFunctor(FILE *fd) : fd_(fd) {}

  bool operator()(const transaction::ObTransID &key, ObTxData *tx_data) {
    UNUSED(key);
    // printf basic info
    fprintf(fd_,
            "ObTxData : tx_id=%-19ld is_in_memtable=%-3d state=%-8s start_log_ts=%-19ld "
            "end_log_ts=%-19ld "
            "commit_version=%-19ld ",
            tx_data->tx_id_.get_id(), tx_data->is_in_tx_data_table_,
            ObTxData::get_state_string(tx_data->state_), tx_data->start_log_ts_,
            tx_data->end_log_ts_, tx_data->commit_version_);

    // printf undo status list
    fprintf(fd_, "Undo Actions : {");
    ObUndoStatusNode *cur_node = tx_data->undo_status_list_.head_;
    while (OB_NOT_NULL(cur_node))
    {
      for (int i = 0; i < cur_node->size_; i++) {
        fprintf(fd_, "(from:%ld,to:%ld)", cur_node->undo_actions_[i].undo_from_, cur_node->undo_actions_[i].undo_to_);
      }
      cur_node = cur_node->next_;
    }
    fprintf(fd_, "}\n");

    return true;
  }

private:
  FILE *fd_;
};


OB_INLINE int64_t ObTxDataMemtable::get_rec_log_ts()
{
  // TODO : @gengli
  // rec_log_ts changes constantly. The rec_log_ts obtained by checkpoint mgr
  // may be greater than the actual checkpoint of tx_data_memtable because the
  // callback functions are not sequential. The checkpoint is determined both on
  // the max-sequential callback point of the log and the rec_log_ts.
  return min_tx_log_ts_;
}

}  // namespace storage

}  // namespace oceanbase

#endif
