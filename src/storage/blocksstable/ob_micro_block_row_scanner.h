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

#ifndef OB_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_ROW_SCANNER_H_
#define OB_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_ROW_SCANNER_H_

#include "ob_row_queue.h"
#include "storage/ob_row_fuse.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/blocksstable/encoding/ob_micro_block_decoder.h"
#include "storage/access/ob_index_sstable_estimator.h"

namespace oceanbase
{
namespace storage
{
struct ObTableIterParam;
struct ObTableAccessContext;
struct PushdownFilterInfo;
class ObBlockRowStore;
class ObTableStoreStat;
}
namespace blocksstable
{
struct ObMicroIndexInfo;

class ObIMicroBlockRowScanner {
public:
  ObIMicroBlockRowScanner(common::ObIAllocator &allocator);
  virtual ~ObIMicroBlockRowScanner();
  virtual void reuse();
  virtual int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable);
  OB_INLINE bool is_valid() const { return is_inited_ && nullptr != range_; }
  virtual int switch_context(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable);
  virtual int set_range(const ObDatumRange &range);
  virtual int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &block_data,
      const bool is_left_border,
      const bool is_right_border);
  virtual int get_next_row(const ObDatumRow *&row);
  virtual int get_next_rows();
  virtual int apply_blockscan(
      storage::ObBlockRowStore *block_row_store,
      storage::ObTableStoreStat &table_store_stat);
  int filter_pushdown_filter(
      sql::ObPushdownFilterExecutor *parent,
      sql::ObPushdownFilterExecutor *filter,
      storage::PushdownFilterInfo &filter_info,
      common::ObBitmap &bitmap);

protected:
  virtual int inner_get_next_row(const ObDatumRow *&row);
  int inner_get_row_header(const ObRowHeader *&row_header);
  int set_reader(const ObRowStoreType store_type);
  int set_base_scan_param(const bool is_left_bound_block,
                          const bool is_right_bound_block);
  int end_of_block() const;
  int locate_range_pos(
      const bool is_left_bound_block,
      const bool is_right_bound_block,
      int64_t &begin,
      int64_t &end);
  int fuse_row(
      const ObDatumRow &former,
      ObDatumRow &result,
      storage::ObNopPos &nop_pos,
      bool &final_result,
      common::ObIAllocator *allocator = nullptr);
  OB_INLINE bool is_row_empty(const ObDatumRow &row) const
  { return row.row_flag_.is_not_exist(); }
private:
  int inner_get_next_row_blockscan(const ObDatumRow *&row);

protected:
  bool is_inited_;
  bool use_fuse_row_cache_;
  bool reverse_scan_;
  bool is_left_border_;
  bool is_right_border_;
  int64_t current_;         // current cursor
  int64_t start_;           // start of scan, inclusive.
  int64_t last_;            // end of scan, inclusive.
  int64_t step_;
  ObDatumRow row_;
  MacroBlockId macro_id_;
  const ObITableReadInfo *read_info_;
  const ObDatumRange *range_;
  const blocksstable::ObSSTable *sstable_;
  ObIMicroBlockReader *reader_;
  ObMicroBlockReader *flat_reader_;
  ObMicroBlockDecoder *decoder_;
  const storage::ObTableIterParam *param_;
  storage::ObTableAccessContext *context_;
  ObIAllocator &allocator_;
  bool can_ignore_multi_version_;
  storage::ObBlockRowStore *block_row_store_;
  storage::ObTxTableGuards tx_table_guard_;
};

// major sstable micro block scanner for query and merge
class ObMicroBlockRowScanner : public ObIMicroBlockRowScanner
{
public:
  ObMicroBlockRowScanner(common::ObIAllocator &allocator)
    : ObIMicroBlockRowScanner(allocator)
  {}
  virtual ~ObMicroBlockRowScanner() {}
  virtual int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable) override final;
  virtual int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &block_data,
      const bool is_left_border,
      const bool is_right_border) override final;
  int estimate_row_count(
      const ObITableReadInfo &column_info,
      const ObMicroBlockData &block_data,
      const ObDatumRange &range,
      bool consider_multi_version,
      ObPartitionEst &est);
};

/*
 multi version sstable micro block format

 note : C means compacted row (from C row to L row)
        F means first row
        L means last row

 caution:
   1. C row may contain nop column
   2. The first row must have C flag, but may have F flag
   3. The last row must have L flag
   4. The C row may not continuous
   5. multi version row of same rowkey may spread across multiple micro block

 examples:
--------------------------------------------------
index | rowkey | version | flag | c1 | c2 | c3
--------------------------------------------------
0     | 1      | -4      |      | x  |    |
1     | 1      | -2      | L    |    | x  |
--------------------------------------------------
2     | 2      | -6      | CF   |    | x  | x
3     | 2      | -5      |      |    | x  |
4     | 2      | -4      | C    |    | x  | x
5     | 2      | -2      | L    |    |    | x
--------------------------------------------------
6     | 3      | -6      | CF   | x  | x  | x
7     | 3      | -5      |      |    | x  | x
8     | 3      | -4      | L    |    |    | x
--------------------------------------------------
9     | 4      | -5      | CFL  | x  | x  |
--------------------------------------------------
10    | 5      | -3      | CL   |    |    | x
--------------------------------------------------
11    | 6      | -7      | CF   |    | x  | x
12    | 6      | -6      |      |    | x  |
--------------------------------------------------
*/

// multi version sstable micro block scanner for query and major merge
class ObMultiVersionMicroBlockRowScanner : public ObIMicroBlockRowScanner
{
public:
  ObMultiVersionMicroBlockRowScanner(common::ObIAllocator &allocator)
      : ObIMicroBlockRowScanner(allocator),
        cell_allocator_(common::ObModIds::OB_SSTABLE_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        reserved_pos_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
        finish_scanning_cur_rowkey_(true),
        is_last_multi_version_row_(true),
        trans_version_col_idx_(-1),
        sql_sequence_col_idx_(-1),
        cell_cnt_(0),
        read_row_direct_flag_(false)
  {}
  virtual ~ObMultiVersionMicroBlockRowScanner() {}
  void reuse() override;
  virtual int switch_context(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable) override;
  virtual int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable) override final;
  virtual int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &block_data,
      const bool is_left_border,
      const bool is_right_border) override final;
protected:
  virtual int inner_get_next_row(const ObDatumRow *&row) override;
  virtual void inner_reset();
private:
  OB_INLINE int inner_get_next_row_impl(const ObDatumRow *&ret_row);
  void reuse_cur_micro_row();
  void reuse_prev_micro_row();
  int locate_cursor_to_read(bool &found_first_row);
  int inner_inner_get_next_row(
      const ObDatumRow *&ret_row,
      bool &version_fit,
      bool &final_result,
      bool &have_uncommited_row);
  int inner_get_next_row_directly(
      const ObDatumRow *&ret_row,
      bool &version_fit,
      bool &final_result);
  int cache_cur_micro_row(const bool found_first_row, const bool final_result);
  int do_compact(const ObDatumRow *src_row, ObDatumRow &dest_row, bool &final_result);
  int lock_for_read(
      const transaction::ObLockForReadArg &lock_for_read_arg,
      bool &can_read,
      int64_t &trans_version,
      bool &is_determined_state);
  // The store_rowkey is a decoration of the ObObj pointer,
  // and it will be destroyed when the life cycle of the rowkey_helper is end.
  // So we have to send it into the function to avoid this situation.
  int get_store_rowkey(ObStoreRowkey &store_rowkey, ObDatumRowkeyHelper &rowkey_helper);
private:
  ObDatumRow prev_micro_row_;
  storage::ObNopPos nop_pos_;
  common::ObArenaAllocator cell_allocator_;
  int64_t reserved_pos_;
  // Use shallow_copy to directly quote the original data of the microblock when compacting,
  // only at the moment (when the dump row format is flat) there is no risk
  // TRUE:it means that the compacted result of the current rowkey has been obtained
  // (not necessarily reading the L mark, it may have been fuse to no nop column)
  bool finish_scanning_cur_rowkey_;
  // TRUE: meet Last Flag of current rowkey
  bool is_last_multi_version_row_;
  ObDatumRow tmp_row_;
  int64_t trans_version_col_idx_;
  int64_t sql_sequence_col_idx_;
  int64_t cell_cnt_;
  transaction::ObTransID trans_id_;
  common::ObVersionRange version_range_;
  bool read_row_direct_flag_;
};

// multi version sstable micro block scanner for minor merge
class ObMultiVersionMicroBlockMinorMergeRowScanner : public ObIMicroBlockRowScanner
{
public:
  ObMultiVersionMicroBlockMinorMergeRowScanner(common::ObIAllocator &allocator)
      : ObIMicroBlockRowScanner(allocator),
      trans_version_col_idx_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
      sql_sequence_col_idx_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
      row_allocator_("MergeRowQueue", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      row_queue_(),
      is_last_multi_version_row_(false),
      is_row_queue_ready_(false),
      scan_state_(SCAN_START),
      committed_trans_version_(INT64_MAX),
      last_trans_state_(INT64_MAX),
      read_trans_id_(),
      last_trans_id_(),
      first_rowkey_flag_(true),
      have_output_row_flag_(false)
  {
    for (int i = 0; i < COMPACT_MAX_ROW; ++i) {
      nop_pos_[i] = NULL;
    }
  }
  virtual ~ObMultiVersionMicroBlockMinorMergeRowScanner()
  {}

  virtual int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable) override final;
  virtual int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &block_data,
      const bool is_left_border,
      const bool is_right_border) override final;
  void reuse() override;
  virtual int apply_blockscan(
      storage::ObBlockRowStore *block_row_store,
      storage::ObTableStoreStat &table_store_stat) override final
  {
    UNUSEDx(block_row_store, table_store_stat);
    return OB_NOT_SUPPORTED;
  }
  virtual int get_next_rows() override
  { return OB_NOT_SUPPORTED; }
  int get_first_row_mvcc_info(bool &is_first_row, bool &is_shadow_row) const;
  TO_STRING_KV(K_(macro_id), K_(is_last_multi_version_row), K_(is_row_queue_ready),
               K_(row_queue), K_(start), K_(current), K_(last),
               K_(scan_state), K_(committed_trans_version));
protected:
  virtual int inner_get_next_row(const ObDatumRow *&row) override;
private:
  enum ScanState{
    SCAN_START = 0,
    GET_RUNNING_TRANS_ROW = 1,
    PREPARE_COMMITTED_ROW_QUEUE = 2,
    FILTER_ABORT_TRANS_ROW = 3,
    COMPACT_COMMIT_TRANS_ROW = 4,
    GET_ROW_FROM_ROW_QUEUE = 5,
    LOCATE_LAST_COMMITTED_ROW = 6,
  };
private:
  int init_row_queue(const int64_t row_col_cnt);
  int locate_last_committed_row();
  void clear_row_queue_status();
  int compact_first_row();
  int compact_last_row();
  int compact_row(
      const ObDatumRow &former,
      const int64_t row_compact_info_index,
      ObDatumRow &result);
  int find_uncommitted_row();
  int filter_abort_trans_row(const ObDatumRow *&row);
  int get_running_trans_row(const ObDatumRow *&row);
  int compact_commit_trans_row(const ObDatumRow *&row);
  int judge_trans_state(
      const int64_t state,
      const int64_t commit_trans_version);
  void clear_scan_status();
  int prepare_committed_row_queue(const ObDatumRow *&row);
  int get_row_from_row_queue(const ObDatumRow *&row);
  int check_curr_row_can_read(const transaction::ObTransID &trans_id, const transaction::ObTxSEQ &sql_seq, bool &can_read);
  int compact_trans_row_into_row_queue();
  int set_trans_version_for_uncommitted_row(ObDatumRow &row);
  int get_trans_state(const transaction::ObTransID &trans_id,
                       int64_t &state,
                       int64_t &commit_trans_version);
  int read_committed_row(bool &add_row_queue_flag, const ObDatumRow *&row);
  int read_uncommitted_row(bool &can_read, const ObDatumRow *&row);
  int add_row_into_row_queue(const ObDatumRow *&row);
  int meet_uncommitted_last_row(const bool can_read, const ObDatumRow *&row);
  int filter_unneeded_row(
      bool &add_row_queue_flag,
      bool &is_tombstone_row_flag);
  int compact_shadow_row_to_last();

private:
  enum RowCompactInfoIndex{
    COMPACT_FIRST_ROW = 0,
    COMPACT_LAST_ROW = 1,
    COMPACT_MAX_ROW,
  };
  // multi version
  int64_t trans_version_col_idx_;
  int64_t sql_sequence_col_idx_;
  common::ObArenaAllocator row_allocator_;
  ObRowQueue row_queue_;
  storage::ObNopPos *nop_pos_[COMPACT_MAX_ROW];
  bool is_last_multi_version_row_;
  bool is_row_queue_ready_;
  ScanState scan_state_;
  int64_t committed_trans_version_;
  int64_t last_trans_state_;
  transaction::ObTransID read_trans_id_;
  transaction::ObTransID last_trans_id_;
  bool first_rowkey_flag_;
  bool have_output_row_flag_;
};

}
}
#endif //OB_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_ROW_SCANNER_H_
