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

#include "share/scn.h"
#include "storage/checkpoint/ob_freeze_checkpoint.h"
#include "storage/memtable/ob_memtable_interface.h"
#include "storage/tx/ob_tx_data_define.h"
#include "storage/tx_table/ob_tx_table_define.h"
#include "storage/tx_table/tx_table_local_buffer.h"
#include "storage/tx/ob_tx_data_op.h"

namespace oceanbase
{

namespace blocksstable
{
struct ObDatumRowkey;
struct ObDatumRange;
}

namespace storage
{
class ObLSTabletService;
class ObTxDataMemtableMgr;
class ObFreezer;
class ObTxDataTable;


// The basic unit that manages tx_data
class ObTxDataMemtable : public ObIMemtable
{
private:
  // The MINI_MERGE of TxDataTable uses recycle_scn to recycle the pre-processed commit versions. But it no need be
  // recycled very quickly, due to the algorithm choses one point each second. Theoretically, a single column in sstable
  // can hold at least 65536(1MB/16) commit version pair, which means 65536 seconds. So we only iterate all tablets once
  // an hour.
  const static int64_t MINI_RECYCLE_COMMIT_VERSIONS_INTERVAL_US = 1L * 60L * 60L * 1000L * 1000L; // an hour

  static int64_t PERIODICAL_SELECT_INTERVAL_NS;

  struct ProcessCommitVersionData
  {
    ProcessCommitVersionData(ObTxData *start_tx_data,
                             share::SCN cur_max_commit_version,
                             share::SCN pre_start_scn)
      : cur_tx_data_(start_tx_data),
        cur_max_commit_version_(cur_max_commit_version),
        pre_start_scn_(pre_start_scn) {}

    ObTxData *cur_tx_data_;
    share::SCN cur_max_commit_version_;
    share::SCN pre_start_scn_;
    // TODO : @gengli remove these variables
    int64_t DEBUG_iter_commit_scn_cnt_;
    share::SCN DEBUG_last_start_scn_;
  };

  struct TxDataFakeRowKey
  {
  public:
    // default constructer for ObSEArray
    TxDataFakeRowKey()
    {
      MEMSET(obj_array_, 0, sizeof(obj_array_));
      rowkey_.reset();
    }

    TxDataFakeRowKey(const int64_t tx_id)
    {
      for (int64_t i = 0; i < OBJ_CNT; i++) {
        obj_array_[i].set_int(tx_id);
      }
      rowkey_.assign(obj_array_, OBJ_CNT);
      rowkey_.set_min();
    }
    ~TxDataFakeRowKey() {}

    TO_STRING_KV(K_(rowkey), K_(obj_array));

    void assign(const int64_t tx_id)
    {
      for (int64_t i = 0; i < OBJ_CNT; i++) {
        obj_array_[i].set_int(tx_id);
      }
      rowkey_.assign(obj_array_, OBJ_CNT);
    }

    const common::ObStoreRowkey &get_rowkey() const { return rowkey_; }

    const transaction::ObTransID get_tx_id() { return transaction::ObTransID(obj_array_[0].get_int()); }

  private:
    static const int64_t OBJ_CNT = 1;

  public:
    common::ObStoreRowkey rowkey_;
    ObObj obj_array_[OBJ_CNT];
  };

  struct TxId2Range {
    TxId2Range() : tx_id_(0), tx_data_count_(0), sort_list_node_(nullptr) {}
    TxId2Range(const transaction::ObTransID tx_id, const int64_t tx_data_count, ObTxDataLinkNode *sort_list_node)
        : tx_id_(tx_id), tx_data_count_(tx_data_count), sort_list_node_(sort_list_node) {}
    transaction::ObTransID tx_id_;
    int64_t tx_data_count_;
    ObTxDataLinkNode *sort_list_node_;

    TO_STRING_KV(K_(tx_id), K_(tx_data_count), KP_(sort_list_node));
  };

  struct StateChangeTime {
    void reset() { memset(this, 0, sizeof(*this)); }
    int64_t frozen_time_;
    int64_t ready_for_flush_time_;
    int64_t create_flush_dag_time_;
    int64_t release_time_;

    TO_STRING_KV(K_(frozen_time), K_(ready_for_flush_time), K_(create_flush_dag_time), K_(release_time));
  };

  using SliceAllocator = ObSliceAlloc;
  static const int MAX_TX_DATA_TABLE_CONCURRENCY = 1 << 6; // 64
  static const int MAX_CONCURRENCY_MOD_MASK = MAX_TX_DATA_TABLE_CONCURRENCY - 1;

public:
  // active   : freeze_ts is not set
  // freezing : freeze_ts is set, tx data is incomplete
  // frozen   : tx data is complete and sorted
  // dumped   : the memtable has been dumped
  enum class State : int32_t
  {
    ACTIVE = 0,
    FREEZING = 1,
    FROZEN = 2,
    RELEASED = 3,
    INVALID = 4,
    STATE_CNT
  };

public:  // ObTxDataMemtable
  ObTxDataMemtable()
    : ObIMemtable(),
      is_inited_(false),
      is_iterating_(false),
      construct_list_done_(false),
      pre_process_done_(false),
      do_recycle_(false),
      max_tx_scn_(),
      inserted_cnt_(0),
      deleted_cnt_(0),
      write_ref_(0),
      occupied_size_(),
      total_undo_node_cnt_(),
      last_insert_ts_(0),
      state_(ObTxDataMemtable::State::INVALID),
      arena_allocator_(),
      sort_list_head_(),
      tx_data_map_(nullptr),
      memtable_mgr_(nullptr),
      freezer_(nullptr),
      buf_(arena_allocator_),
      row_key_array_() {}
  ~ObTxDataMemtable() { reset(); }
  void reset();
  int init(const ObITable::TableKey &table_key,
           ObTxDataMemtableMgr *memtable_mgr,
           storage::ObFreezer *freezer,
           const int64_t buckets_cnt);
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

  // void revert_tx_data(ObTxData *tx_data) { tx_data_map_->revert(tx_data); }

  /**
   * @brief This function is used by ObTxDataMemtableScanIterator and it will do the following
   * operations:
   * 1. Iterate the link hash map of tx data memtable to construct a tx data list
   * 2. Sort the list by trans_id of tx data for dumping
   */
  int prepare_tx_data_list();

  /**
   * @brief Check if this tx data memtable can be minor merge
   * See more details at ready_for_flush() function.
   */
  bool can_be_minor_merged() override;

  /**
   * @brief delete a tx data from tx data memtable
   *
   * @param tx_id the tx_id of tx data
   */
  int remove(transaction::ObTransID tx_id);

  int pre_process_for_merge();

  int get_iter_start_and_count(const transaction::ObTransID &tx_id, ObTxDataLinkNode *&start_node, int64_t &iterate_row_cnt);

  share::ObLSID get_ls_id() const;
  int64_t get_total_undo_node_cnt() const;

  /**
   * @brief dump tx data memtable to file
   *
   * @param[in] fname
   */
  int dump2text(const char *fname);

  INHERIT_TO_STRING_KV("ObITable",
                       ObITable,
                       K_(is_inited),
                       K_(is_iterating),
                       K_(pre_process_done),
                       K_(construct_list_done),
                       "min_tx_scn", get_min_tx_scn(),
                       K_(max_tx_scn),
                       "min_start_scn", get_min_start_scn(),
                       K_(snapshot_version),
                       K_(inserted_cnt),
                       K_(deleted_cnt),
                       K_(write_ref),
                       "occupy_size", get_occupied_size(),
                       "total_undo_node_cnt", get_total_undo_node_cnt(),
                       K_(state),
                       K_(stat_change_ts),
                       KP_(tx_data_map),
                       KP_(memtable_mgr),
                       K_(commit_versions_serialize_size),
                       K_(row_key_array),
                       K_(tx_id_2_range));


public: /* derived from ObITable */

  virtual bool is_active_memtable() override { return ObTxDataMemtable::State::ACTIVE == state_; }

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
  virtual bool is_frozen_memtable() { return ObTxDataMemtable::State::FROZEN == state_; }

public: /* derived from ObIMemtable */
  virtual int64_t get_occupied_size() const;
  virtual int estimate_phy_size(const ObStoreRowkey *start_key,
                                const ObStoreRowkey *end_key,
                                int64_t &total_bytes,
                                int64_t &total_rows) override;

  virtual int get_split_ranges(const ObStoreRange &input_range,
                               const int64_t part_cnt,
                               common::ObIArray<common::ObStoreRange> &range_array) override;
  // not supported
  virtual int get(const storage::ObTableIterParam &param,
                  storage::ObTableAccessContext &context,
                  const blocksstable::ObDatumRowkey &rowkey,
                  blocksstable::ObDatumRow &row) override;
  virtual ObTabletID get_tablet_id() const { return LS_TX_DATA_TABLET; }

public:  // checkpoint
  share::SCN get_rec_scn()
  {
    return get_min_tx_scn();
  }

  int flush(const int64_t trace_id);
  /**
   * @brief Because of the random order of clog callbacks, the tx data in a freezing tx data
   * memtable may not completed. We must wait until the max_consequent_callbacked_scn is larger
   * than the end_scn of tx data memtable which means this memtable is now completed.
   */
  bool ready_for_flush();

public:  // getter && setter
  bool do_recycled() { return do_recycle_; }
  int64_t get_tx_data_count() { return tx_data_map_->count(); }
  int64_t size() { return get_tx_data_count(); }
  int64_t get_inserted_count() { return inserted_cnt_; }
  int64_t get_deleted_count() { return deleted_cnt_; }
  int64_t inc_write_ref() { return ATOMIC_AAF(&write_ref_, 1); }
  int64_t dec_write_ref() { return ATOMIC_AAF(&write_ref_, -1); }
  int64_t get_write_ref() const override { return ATOMIC_LOAD(&write_ref_); }
  int64_t get_buckets_cnt() const { return OB_NOT_NULL(tx_data_map_) ? tx_data_map_->get_buckets_cnt() : 0; }
  ObTxDataMemtable::State get_state() { return state_; }
  ObTxDataLinkNode *get_sorted_list_head() { return &sort_list_head_; }
  const char* get_state_string();
  ObTxDataMemtableMgr *get_tx_data_memtable_mgr() { return memtable_mgr_; }

  share::SCN get_min_tx_scn() const
  {
    share::SCN res = share::SCN::max_scn();
    for (int i = 0; i < MAX_TX_DATA_TABLE_CONCURRENCY; i++) {
      share::SCN min_tx_scn = min_tx_scn_[i].atomic_load();
      if (min_tx_scn < res) {
        res = min_tx_scn;
      }
    }
    return res;
  }

  share::SCN get_min_start_scn() const
  {
    share::SCN res = share::SCN::max_scn();
    for (int i = 0; i < MAX_TX_DATA_TABLE_CONCURRENCY; i++) {
      share::SCN min_start_scn = min_start_scn_[i].atomic_load();
      if (min_start_scn < res) {
        res = min_start_scn;
      }
    }
    return res;
  }
  share::SCN get_max_tx_scn() { return max_tx_scn_; }

  int set_freezer(ObFreezer *handler);
  void set_start_scn(const share::SCN start_scn) {key_.scn_range_.start_scn_ = start_scn; }
  void set_end_scn() { key_.scn_range_.end_scn_ = max_tx_scn_; }
  void set_state(const ObTxDataMemtable::State &state) { state_ = state; }
  void reset_is_iterating() { ATOMIC_STORE(&is_iterating_, false); }
  void set_freeze_time() { stat_change_ts_.frozen_time_ = ObTimeUtil::fast_current_time(); }
  void set_release_time() { stat_change_ts_.release_time_ = ObTimeUtil::fast_current_time(); }

  share::SCN get_end_scn() { return key_.scn_range_.end_scn_;}

  double load_factory() { return OB_ISNULL(tx_data_map_) ? 0 : tx_data_map_->load_factory(); }

private:  // ObTxDataMemtable
  void atomic_update_(ObTxData *tx_data);
  int do_sort_by_tx_id_();

  int do_sort_by_start_scn_();

  int cmp_key_(const int64_t &lhs, const int64_t &rhs);

  int construct_list_for_sort_();

  int init_tx_data_map_(const int64_t buckets_cnt);

  int pre_process_commit_version_row_(ObTxData *fake_tx_data);

  int insert_fake_tx_data_to_list_and_map_(ObTxData *fake_tx_data);


  int fill_in_cur_commit_versions_(ObCommitVersionsArray &cur_commit_versions);

  int periodical_get_next_commit_version_(ProcessCommitVersionData &process_data,
                                          ObCommitVersionsArray::Node &node);

  int get_past_commit_versions_(ObCommitVersionsArray &past_commit_versions);

  int merge_cur_and_past_commit_verisons_(const share::SCN recycle_scn,
                                          ObCommitVersionsArray &cur_commit_versions,
                                          ObCommitVersionsArray &past_commit_versions,
                                          ObCommitVersionsArray &merged_commit_versions);


  int push_range_bounds_(const int64_t part_cnt);

  int prepare_array_space_(const int64_t part_cnt);

  int uniq_tx_id_();

  void init_arena_allocator_();

  void merge_sort_(int64_t (*get_key)(const ObTxData &), ObTxData *&head);

  ObTxData *merge_sorted_list_(int64_t (*get_key)(const ObTxData &),
                                           ObTxData *left_list,
                                           ObTxData *right_list);
  void split_list_(ObTxData *head,
                   ObTxData *&left_list,
                   ObTxData *&right_list);

  int merge_pre_process_node_(const int64_t step_len,
                              const share::SCN start_scn_limit,
                              const share::SCN recycle_scn,
                              const ObIArray<ObCommitVersionsArray::Node> &data_arr,
                              share::SCN &max_commit_version,
                              ObIArray<ObCommitVersionsArray::Node> &merged_arr);

  int DEBUG_try_calc_upper_and_check_(ObCommitVersionsArray &merged_commit_versions);
  int DEBUG_fake_calc_upper_trans_version(const share::SCN sstable_end_scn,
                                          share::SCN &upper_trans_version,
                                          ObCommitVersionsArray &merged_commit_versions);
  void DEBUG_print_start_scn_list_(const char* fname);
  void DEBUG_print_merged_commit_versions_(ObCommitVersionsArray &merged_commit_versions);
  void TEST_reset_tx_data_map_();

private:  // ObTxDataMemtable
  bool is_inited_;
  bool is_iterating_;
  bool construct_list_done_;
  bool pre_process_done_;
  bool do_recycle_;

  // the maximum scn in this tx data memtable
  share::SCN max_tx_scn_;
  // the minimum log ts of commit_log_ts in this tx data memtable
  share::SCN min_tx_scn_[MAX_TX_DATA_TABLE_CONCURRENCY];
  // the minimum start log ts in this tx data memtable
  share::SCN min_start_scn_[MAX_TX_DATA_TABLE_CONCURRENCY];

  int64_t inserted_cnt_;

  int64_t deleted_cnt_;

  int64_t write_ref_;

  int64_t occupied_size_[MAX_TX_DATA_TABLE_CONCURRENCY];
  int64_t total_undo_node_cnt_[MAX_TX_DATA_TABLE_CONCURRENCY];

  int64_t last_insert_ts_;
  StateChangeTime stat_change_ts_;

  // the state of tx data memtable can be one of 4 kinds of state :
  // active, freezing, frozen, dumped
  ObTxDataMemtable::State state_;

  ObArenaAllocator arena_allocator_;

  // the head node of sorted list which is used before dump
  ObTxDataLinkNode sort_list_head_;

  // the hash map sotres tx data
  TxDataMap *tx_data_map_;

  // used for freeze
  ObTxDataMemtableMgr *memtable_mgr_;

  // used for checkpoint executor
  storage::ObFreezer *freezer_;

  int64_t commit_versions_serialize_size_;
  ObTxLocalBuffer buf_;

  // provide ObStoreRowkey for get_split_range() function
  ObSEArray<TxDataFakeRowKey, 8> row_key_array_;

  // When parallel dump is enabled, the tx data sort list is splited to multiple ranges. This array map the start tx_id
  // of the range to a tx data count in the range. Then the ObTxDataMemtableScanIterator can detect how many tx data
  // need to be dumped.
  ObSEArray<TxId2Range, 8> tx_id_2_range_;
  share::SCN DEBUG_last_start_scn_;
};

class DumpTxDataMemtableFunctor
{
public:
  explicit DumpTxDataMemtableFunctor(FILE *fd) : fd_(fd) {}

  bool operator()(ObTxData *tx_data) {
    // printf basic info
    fprintf(fd_,
            "ObTxData : tx_id=%-19ld state=%-8s start_scn=%-19s "
            "end_scn=%-19s "
            "commit_version=%-19s ",
            tx_data->tx_id_.get_id(),
            ObTxData::get_state_string(tx_data->state_),
            to_cstring(tx_data->start_scn_),
            to_cstring(tx_data->end_scn_),
            to_cstring(tx_data->commit_version_));

    // printf undo status list
    fprintf(fd_, "Undo Actions [from, to): {");
    ObUndoStatusNode *cur_node = NULL;
    if (tx_data->op_guard_.is_valid()) {
      cur_node = tx_data->op_guard_->get_undo_status_list().head_;
    }
    while (OB_NOT_NULL(cur_node))
    {
      for (int i = 0; i < cur_node->size_; i++) {
        fprintf(fd_, "[%ld, %ld)",
                cur_node->undo_actions_[i].undo_from_.cast_to_int(),
                cur_node->undo_actions_[i].undo_to_.cast_to_int());
      }
      cur_node = cur_node->next_;
    }
    fprintf(fd_, "}\n");

    return true;
  }

private:
  FILE *fd_;
};

}  // namespace storage

}  // namespace oceanbase

#endif
