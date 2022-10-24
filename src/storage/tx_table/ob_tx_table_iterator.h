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

#ifndef OCEANBASE_STORAGE_OB_TX_TABLE_ITERATOR
#define OCEANBASE_STORAGE_OB_TX_TABLE_ITERATOR

#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/tx_table/ob_tx_table_define.h"
#include "storage/tx/ob_trans_ctx_mgr.h"
#include "storage/tx_table/tx_table_local_buffer.h"

namespace oceanbase
{

namespace blocksstable {
struct ObDatumRow;
};

namespace transaction
{
  class ObLSTxCtxIterator;
}
namespace storage
{
class ObTxDataMemtable;
class ObTxDataSortListNode;
class ObSSTableArray;
struct TxDataReadSchema;


enum TX_DATA_SSTABLE_COL_IDX : int64_t
{
  TX_DATA_ID_COLUMN = 0,
  TX_DATA_IDX_COLUMN,
  TX_DATA_TOTAL_ROW_CNT_COLUMN,
  TX_DATA_END_TS_COLUMN,
  TX_DATA_VAL_COLUMN,
  TX_DATA_MAX_COLUMN
};

enum TX_CTX_SSTABLE_COL_IDX : int64_t
{
  TX_CTX_TABLE_KEY_COLUMN = 0,
  TX_CTX_TABLE_META_COLUMN,
  TX_CTX_TABLE_VAL_COLUMN,
  TX_CTX_TABLE_MAX_COLUMN
};

/**
 * @brief Using for dump tx data memtable and pre-process result to calculate upper_trans_version
 *
 * This iterator is a little more complicated. It dumps not only tx datas but also pre-process
 * result to calculate upper_trans_version. The sstables it iterates produce are looked like the
 * following table:
 *
 * │  Column 0  │        Hidden Column       │  Column 1  │             Column 2             │
 * ├────────────┼──────────────┬─────────────┼────────────┼──────────────────────────────────┤
 * │    Key     │ TransVersion │ SqlSequence │  EndLogTs  │               Value              │
 * ├────────────┼──────────────┼─────────────┼────────────┼──────────────────────────────────┤
 * │ trans_id_1 │     -1       │             │ end_log_ts │     serialization of tx_data1    │
 * │ trans_id_2 │     -1       │             │ end_log_ts │     serialization of tx_data2    │
 * │ trans_id_3 │     -1       │             │ end_log_ts │     serialization of tx_data3    │
 * │    ...     │    ....      │             │    ...     │                ...               │
 * │ INT64_MAX  │     -1       │             │ INT64_MAX  │ serialization of pre-process data│
 * └────────────┴──────────────┴─────────────┴────────────┴──────────────────────────────────┘
 *
 * This iterator will execute twice sorting and once reading from old tx data sstable.
 * For more details, see https://yuque.antfin-inc.com/ob/transaction/lurtok
 *
 */


class ObTxDataMemtableScanIterator : public memtable::ObIMemtableIterator
{
private:
  static const int64_t BUF_LENGTH = 1024;
  static int64_t PERIODICAL_SELECT_INTERVAL_NS;

public:
  ObTxDataMemtableScanIterator(const ObTableIterParam &iter_param)
    : is_inited_(false),
      iter_param_(iter_param),
      dump_tx_data_done_(false),
      cur_max_commit_version_(0),
      pre_start_log_ts_(0),
      tx_data_row_cnt_(0),
      pre_tx_data_(nullptr),
      arena_allocator_(),
      cur_node_(nullptr),
      row_(),
      buf_(arena_allocator_),
      tx_data_memtable_(nullptr),
      key_datum_(),
      DEBUG_iter_commit_ts_cnt_(0) {}

  ~ObTxDataMemtableScanIterator() { reset(); }

  /**
   * @brief This init() function may cost more than 1 second because of the following
   * reasons:
   * 1. The tx data memtable uses a link hash map to do insert and read operation. So it does not
   * have an ordered data structure.
   * 2. The scann iterator need ordered access to tx data memtable.
   *
   * So this init() function will do sorting function of tx data memtable, this may cost more than 1
   * second.
   *
   * @param[in] tx_data_memtable the tx data memtable to scan
   */
  virtual int init(ObTxDataMemtable *tx_data_memtable);

  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&row);
  virtual void reset();
  virtual void reuse();

private:
  int get_next_tx_data_row_(const blocksstable::ObDatumRow *&row);

  int get_next_commit_version_row_(const blocksstable::ObDatumRow *&row);

  int prepare_commit_version_list_();

  int periodical_get_next_commit_version_(ObCommitVersionsArray::Node &node);

  int fill_in_cur_commit_versions_(ObCommitVersionsArray &cur_commit_versions);

  int get_past_commit_versions_(ObCommitVersionsArray &past_commit_versions);

  int deserialize_commit_versions_array_from_row_(const blocksstable::ObDatumRow *row, ObCommitVersionsArray &past_commit_versions);


  int merge_cur_and_past_commit_verisons_(const int64_t recycle_ts,
                                          ObCommitVersionsArray &cur_commit_versions,
                                          ObCommitVersionsArray &past_commit_versions,
                                          ObCommitVersionsArray &merged_commit_versions);

  /**
   * @brief get node from data_arr and push_back it to merged_arr
   *
   * @param[in] step_len to control serialize size of commit versions array
   * @param[in] start_scn_limit to drop some nodes in past commit versions array
   * @param[in] recycle_ts to recycle some nodes
   * @param[in] data_arr the source data to be merged
   * @param[out] max_commit_version to record current max commit version
   * @param[out] merged_arr the target array to be dumped
   */
  int merge_pre_process_node_(const int64_t step_len,
                              const int64_t start_scn_limit,
                              const int64_t recycle_ts,
                              const ObIArray<ObCommitVersionsArray::Node> &data_arr,
                              int64_t &max_commit_version,
                              ObIArray<ObCommitVersionsArray::Node> &merged_arr);

  int set_row_with_merged_commit_versions_(ObCommitVersionsArray &merged_commit_versions,
                                           const blocksstable::ObDatumRow *&row);

  int DEBUG_check_past_and_cur_arr(ObCommitVersionsArray &cur_commit_versions,
                                   ObCommitVersionsArray &past_commit_versions);

  int DEBUG_try_calc_upper_and_check_(ObCommitVersionsArray &merged_commit_versions);

  int DEBUG_fake_calc_upper_trans_version(const int64_t sstable_end_log_ts,
                                          int64_t &upper_trans_version,
                                          ObCommitVersionsArray &merged_commit_versions);

  void DEBUG_print_start_scn_list_();
  void DEBUG_print_merged_commit_versions_(ObCommitVersionsArray &merged_commit_versions);

private:
  bool is_inited_;
  const ObTableIterParam &iter_param_;
  bool dump_tx_data_done_;
  int64_t cur_max_commit_version_;
  int64_t pre_start_log_ts_;
  int64_t tx_data_row_cnt_;
  ObTxData *pre_tx_data_;
  ObArenaAllocator arena_allocator_;
  ObTxDataSortListNode *cur_node_;
  blocksstable::ObDatumRow row_;
  ObTxLocalBuffer buf_;
  ObTxDataMemtable *tx_data_memtable_;
  blocksstable::ObStorageDatum key_datum_;
  int64_t DEBUG_iter_commit_ts_cnt_;
  int64_t DEBUG_last_start_log_ts_;
};

/**
 * @brief Using for read tx data from sstables.
 */
class ObTxDataSingleRowGetter
{
  using SliceAllocator = ObSliceAlloc;
public:
  ObTxDataSingleRowGetter(const ObTableIterParam & iter_param,
                          SliceAllocator &slice_allocator)
      : iter_param_(iter_param), slice_allocator_(slice_allocator), key_datums_() {}
  virtual ~ObTxDataSingleRowGetter() {}

  /**
   * @brief Init this tx data single row getter
   *
   * @param[in] tx_id using to find the tx data
   */
  int init(const transaction::ObTransID &tx_id);
  int get_next_row(ObTxData &tx_data);

private:
  int get_next_row_(ObSSTableArray &sstables, ObTxData &tx_data);
  int get_row_from_sstables_(blocksstable::ObDatumRowkey &row_key,
                             ObSSTableArray &sstables,
                             const ObTableIterParam &iter_param,
                             ObTableAccessContext &access_context,
                             ObTxData &tx_data);
  int deserialize_tx_data_from_store_row_(const blocksstable::ObDatumRow *row, ObTxData &tx_data);

private:
  const ObTableIterParam & iter_param_;
  SliceAllocator &slice_allocator_;
  transaction::ObTransID tx_id_;
  ObArenaAllocator arena_allocator_;
  blocksstable::ObStorageDatum key_datums_[2];
};

/**
 * @brief Using for read commit versions from sstables.
 */
class ObCommitVersionsGetter {
public:
  ObCommitVersionsGetter(const ObTableIterParam &iter_param, ObITable *table)
      : iter_param_(iter_param), table_(table), key_datums_() {}
  virtual ~ObCommitVersionsGetter() {}

  int get_next_row(ObCommitVersionsArray &commit_versions);

private:
  const ObTableIterParam &iter_param_;
  ObArenaAllocator arena_allocator_;
  ObITable *table_;
  blocksstable::ObStorageDatum key_datums_[2];
};

// ObTxCtxMemtableScanIterator is the iterator for tx ctx table merge process
class ObTxCtxMemtableScanIterator : public memtable::ObIMemtableIterator
{
public:
  ObTxCtxMemtableScanIterator()
    : allocator_(),
      row_(),
      has_unmerged_buf_(false),
      unmerged_buf_start_pos_(0),
      prev_meta_(),
      meta_buf_(allocator_),
      buf_(allocator_),
      idx_(0),
      ls_tx_ctx_iter_(),
      MAX_VALUE_LENGTH_(MAX_TX_CTX_TABLE_VALUE_LENGTH),
      is_inited_(false) {}

  ~ObTxCtxMemtableScanIterator() {}

  // TODO(handora.qc): ObTableIterParam, ObTableAccessContext
  virtual int init(ObTxCtxMemtable *tx_ctx_memtable_);

  virtual int inner_get_next_row(const blocksstable::ObDatumRow *&row);
  virtual void reset();
  virtual void reuse();

  // =================== ONLY USED FOR TEST =================
  int TEST_set_max_value_length(int64_t max_value_length)
  {
    int ret = OB_SUCCESS;
    if (max_value_length > MAX_TX_CTX_TABLE_VALUE_LENGTH) {
      ret = OB_ERROR_OUT_OF_RANGE;
    } else {
      *(const_cast<int64_t*>(&MAX_VALUE_LENGTH_)) = max_value_length;
    }
    return ret;
  }
private:
  int get_next_tx_ctx_table_info_(transaction::ObPartTransCtx *&tx_ctx,
                                  ObTxCtxTableInfo &ctx_info);
private:
  const static int64_t TX_CTX_META_BUF_LENGTH = 256;
  const static int64_t TX_CTX_BUF_LENGTH = 1000;
  ObArenaAllocator allocator_;
  blocksstable::ObDatumRow row_;
  bool has_unmerged_buf_;
  int64_t unmerged_buf_start_pos_;
  ObTxCtxTableMeta prev_meta_;
  ObTxLocalBuffer meta_buf_;
  ObTxLocalBuffer buf_;
  int64_t idx_;
  transaction::ObLSTxCtxIterator ls_tx_ctx_iter_;
  const int64_t MAX_VALUE_LENGTH_;
  bool is_inited_;

};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_TX_DATA_TABLE
