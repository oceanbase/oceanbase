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

#ifndef OB_MICRO_BLOCK_ROW_SCANNER_H_
#define OB_MICRO_BLOCK_ROW_SCANNER_H_

#include "lib/container/ob_raw_se_array.h"
#include "ob_row_queue.h"
#include "storage/ob_sstable.h"
#include "storage/ob_row_fuse.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_sparse_micro_block_reader.h"
#include "storage/blocksstable/ob_lob_data_reader.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace storage {
class ObTableIterParam;
class ObTableAccessContext;
}  // namespace storage
namespace blocksstable {

class ObColumnMap;

class ObIMicroBlockRowScanner {
public:
  ObIMicroBlockRowScanner()
      : param_(NULL),
        context_(NULL),
        range_(NULL),
        sstable_(NULL),
        reader_(NULL),
        current_(ObIMicroBlockReader::INVALID_ROW_INDEX),
        start_(ObIMicroBlockReader::INVALID_ROW_INDEX),
        last_(ObIMicroBlockReader::INVALID_ROW_INDEX),
        reverse_scan_(false),
        is_left_border_(false),
        is_right_border_(false),
        step_(1),
        has_lob_column_(false),
        lob_reader_(),
        macro_id_(),
        is_inited_(false)
  {}
  virtual ~ObIMicroBlockRowScanner()
  {}
  virtual int init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const storage::ObSSTable* sstable);
  virtual int set_range(const common::ObStoreRange& range);
  virtual int open(const MacroBlockId& macro_id, const ObFullMacroBlockMeta& macro_meta,
      const ObMicroBlockData& block_data, const bool is_left_border, const bool is_right_border);
  virtual int get_next_row(const storage::ObStoreRow*& row);
  virtual int get_next_rows(const storage::ObStoreRow*& rows, int64_t& count);
  virtual void reset();
  virtual void rescan();
  int alloc_row(ObIAllocator& allocator, const int64_t cell_cnt, storage::ObStoreRow& row);
  virtual int get_cur_micro_row_count(int64_t& row_count) const;

protected:
  virtual int inner_get_next_row(const storage::ObStoreRow*& row) = 0;
  virtual int inner_get_next_rows(const storage::ObStoreRow*& rows, int64_t& count) = 0;
  int set_reader(const ObRowStoreType store_type);
  int set_base_scan_param(const bool is_left_bound_block, const bool is_right_bound_block);
  OB_INLINE int end_of_block() const;
  int locate_range_pos(const bool is_left_bound_block, const bool is_right_bound_block, int64_t& begin, int64_t& end);
  int fuse_row(const storage::ObStoreRow& former, storage::ObStoreRow& result, storage::ObNopPos& nop_pos,
      bool& final_result, storage::ObObjDeepCopy* obj_copy = nullptr);
  int fuse_sparse_row(const storage::ObStoreRow& former, storage::ObStoreRow& result,
      ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>& bit_set, bool& final_result, storage::ObObjDeepCopy* obj_copy = nullptr);
  virtual int add_lob_reader(const storage::ObTableIterParam& access_param, storage::ObTableAccessContext& access_ctx,
      const storage::ObSSTable& sstable);
  virtual int read_lob_columns(const storage::ObStoreRow* store_row);
  OB_INLINE bool has_lob_column()
  {
    return has_lob_column_;
  }
  OB_INLINE bool is_row_empty(const storage::ObStoreRow& row) const
  {
    return common::ObActionFlag::OP_ROW_DOES_NOT_EXIST == row.flag_;
  }

protected:
  // assigned in init func
  const storage::ObTableIterParam* param_;
  storage::ObTableAccessContext* context_;
  const common::ObStoreRange* range_;
  const storage::ObSSTable* sstable_;
  ObMacroBlockMetaHandle meta_handle_;
  // for scan
  ObColumnMap column_map_;
  ObIMicroBlockReader* reader_;
  ObMicroBlockReader flat_reader_;
  ObSparseMicroBlockReader sparse_reader_;
  int64_t current_;  // current cursor
  int64_t start_;    // start of scan, inclusive.
  int64_t last_;     // end of scan, inclusive.
  bool reverse_scan_;
  bool is_left_border_;
  bool is_right_border_;
  int64_t step_;
  bool has_lob_column_;
  ObLobDataReader lob_reader_;
  MacroBlockId macro_id_;
  bool is_inited_;
};

// major sstable micro block scanner for query and merge
class ObMicroBlockRowScanner : public ObIMicroBlockRowScanner {
public:
  ObMicroBlockRowScanner()
  {}
  virtual ~ObMicroBlockRowScanner()
  {}
  virtual int init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const storage::ObSSTable* sstable) override;
  virtual int open(const MacroBlockId& macro_id, const ObFullMacroBlockMeta& macro_meta,
      const ObMicroBlockData& block_data, const bool is_left_border, const bool is_right_border) override;
  void reset();

protected:
  virtual int inner_get_next_row(const storage::ObStoreRow*& row) override;
  virtual int inner_get_next_rows(const storage::ObStoreRow*& rows, int64_t& count) override;

protected:
  storage::ObStoreRow rows_[ObIMicroBlockReader::OB_MAX_BATCH_ROW_COUNT];
  char obj_buf_[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(ObObj) * ObIMicroBlockReader::OB_MAX_BATCH_ROW_COUNT];
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
class ObMultiVersionMicroBlockRowScanner : public ObIMicroBlockRowScanner {
public:
  ObMultiVersionMicroBlockRowScanner()
      : cell_allocator_(common::ObModIds::OB_SSTABLE_READER),
        reserved_pos_(ObIMicroBlockReader::INVALID_ROW_INDEX),
        finish_scanning_cur_rowkey_(true),
        is_last_multi_version_row_(true),
        trans_version_col_idx_(-1),
        sql_sequence_col_idx_(-1),
        cell_cnt_(0),
        read_row_direct_flag_(false)
  {}
  virtual ~ObMultiVersionMicroBlockRowScanner()
  {}
  virtual int init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const storage::ObSSTable* sstable) override;
  virtual int open(const MacroBlockId& macro_id, const ObFullMacroBlockMeta& macro_meta,
      const ObMicroBlockData& block_data, const bool is_left_border, const bool is_right_border) override;
  void reset();
  void rescan() override;

protected:
  virtual int inner_get_next_row(const storage::ObStoreRow*& row) override;
  virtual int inner_get_next_rows(const storage::ObStoreRow*& rows, int64_t& count) override;

private:
  OB_INLINE void inner_reset();
  OB_INLINE int inner_get_next_row_impl(const storage::ObStoreRow*& ret_row);
  void reuse_cur_micro_row();
  void reuse_prev_micro_row();
  int locate_cursor_to_read(bool& found_first_row);
  int inner_inner_get_next_row(const storage::ObStoreRow*& ret_row, bool& version_fit, bool& final_result);
  int inner_get_next_row_directly(const storage::ObStoreRow*& ret_row, bool& version_fit, bool& final_result);
  int cache_cur_micro_row(const bool found_first_row, const bool final_result);
  int do_compact(const storage::ObStoreRow* src_row, storage::ObStoreRow& dest_row, bool& final_result);
  int lock_for_read(const transaction::ObLockForReadArg& lock_for_read_arg, bool& can_read, int64_t& trans_version,
      bool& is_determined_state);

private:
  storage::ObStoreRow prev_micro_row_;
  storage::ObStoreRow cur_micro_row_;
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
  storage::ObStoreRow tmp_row_;
  char tmp_row_obj_buf_[common::OB_ROW_MAX_COLUMNS_COUNT * sizeof(ObObj)];
  int64_t trans_version_col_idx_;
  int64_t sql_sequence_col_idx_;
  int64_t cell_cnt_;
  transaction::ObTransID trans_id_;
  common::ObVersionRange version_range_;
  bool read_row_direct_flag_;
};

// multi version sstable micro block scanner for minor merge
class ObMultiVersionMicroBlockMinorMergeRowScanner : public ObIMicroBlockRowScanner {
public:
  ObMultiVersionMicroBlockMinorMergeRowScanner();
  virtual ~ObMultiVersionMicroBlockMinorMergeRowScanner();

  virtual int init(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const storage::ObSSTable* sstable) override;
  virtual int open(const MacroBlockId& macro_id, const ObFullMacroBlockMeta& macro_meta,
      const ObMicroBlockData& block_data, const bool is_left_border, const bool is_right_border) override;
  void reset();
  void rescan() override
  {
    reset();
  }
  TO_STRING_KV(K_(macro_id), K_(is_last_multi_version_row), K_(is_row_queue_ready), "row_queue_count",
      row_queue_.count(), K_(start), K_(current), K_(last));

protected:
  virtual int inner_get_next_row(const storage::ObStoreRow*& row) override;
  virtual int inner_get_next_rows(const storage::ObStoreRow*& rows, int64_t& count) override;

private:
  enum ScanState {
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
  void init_multi_version_rowkey_info(const int multi_version_rowkey_type, int64_t& expect_multi_version_col_cnt);
  int locate_last_committed_row();
  void clear_row_queue_status();
  int compact_first_row();
  int compact_last_row();
  int compact_row(const storage::ObStoreRow& former, const int64_t row_compact_info_index, storage::ObStoreRow& result);
  int compact_trans_row_to_one();
  int add_sql_sequence_col(storage::ObStoreRow& row);
  int find_uncommitted_row();
  int filter_abort_trans_row(const storage::ObStoreRow*& row);
  int get_running_trans_row(const storage::ObStoreRow*& row);
  int compact_commit_trans_row(const storage::ObStoreRow*& row);
  int judge_trans_status(const transaction::ObTransTableStatusType status, const int64_t commit_trans_version);
  void clear_scan_status();
  int prepare_committed_row_queue(const storage::ObStoreRow*& row);
  int get_row_from_row_queue(const storage::ObStoreRow*& row);
  int check_curr_row_can_read(const transaction::ObTransID& trans_id, const int64_t sql_seq, bool& can_read);
  bool check_meet_another_trans(const transaction::ObTransID& trans_id);
  int compact_trans_row_into_row_queue();
  int set_trans_version_for_uncommitted_row(storage::ObStoreRow& row);
  int get_trans_status(const transaction::ObTransID& trans_id, transaction::ObTransTableStatusType& status,
      int64_t& commit_trans_version);
  int read_committed_row(bool& add_row_queue_flag, const storage::ObStoreRow*& row);
  int read_uncommitted_row(bool& can_read, const storage::ObStoreRow*& row);
  int add_row_into_row_queue(const storage::ObStoreRow*& row);
  int meet_uncommitted_last_row(const bool can_read, const storage::ObStoreRow*& row);
  void complete_row_queue();
  int filter_unneeded_row(bool& add_row_queue_flag, bool& is_magic_row_flag);

private:
  enum RowCompactInfoIndex {
    RNPI_FIRST_ROW = 0,
    RNPI_TRANS_COMPACT_ROW = 1,
    RNPI_LAST_ROW = 2,
    RNPI_MAX,
  };
  // multi version
  int64_t trans_version_col_idx_;
  int64_t sql_sequence_col_store_idx_;
  int64_t sql_sequence_col_idx_;
  common::ObArenaAllocator row_allocator_;
  common::ObArenaAllocator allocator_;
  ObRowQueue row_queue_;
  storage::ObNopPos* nop_pos_[RNPI_MAX];
  ObFixedBitSet<OB_ALL_MAX_COLUMN_ID>* bit_set_[RNPI_MAX];
  storage::ObObjDeepCopy obj_copy_;
  storage::ObStoreRow row_;
  ObObj obj_buf_[common::OB_ROW_MAX_COLUMNS_COUNT];
  uint16_t col_id_buf_[common::OB_ROW_MAX_COLUMNS_COUNT];
  bool is_last_multi_version_row_;
  bool is_row_queue_ready_;
  bool add_sql_sequence_col_flag_;  // old-version sstable should add sql_sequence col
  ScanState scan_state_;
  int64_t committed_trans_version_;
  bool not_first_trans_flag_;
  transaction::ObTransID read_trans_id_;
  transaction::ObTransID last_trans_id_;
  bool first_rowkey_flag_;
  bool have_output_row_flag_;
  bool is_first_row_filtered_;  // the flag indicate that if the sstable cut the first row
};

OB_INLINE int ObIMicroBlockRowScanner::end_of_block() const
{
  int ret = common::OB_SUCCESS;
  if (ObIMicroBlockReader::INVALID_ROW_INDEX == current_) {
    ret = common::OB_ITER_END;
  } else {
    if (!reverse_scan_) {
      if (current_ > last_) {
        ret = common::OB_ITER_END;  // forward scan to border
      }
    } else {  // reverse_scan_
      if (current_ < last_ || current_ > start_) {
        ret = common::OB_ITER_END;  // reverse scan to border
      }
    }
  }
  return ret;
}

}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_MICRO_BLOCK_ROW_SCANNER_H_ */
