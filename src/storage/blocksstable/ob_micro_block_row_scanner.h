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
#include "storage/blocksstable/cs_encoding/ob_micro_block_cs_decoder.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/column_store/ob_cg_bitmap.h"

namespace oceanbase
{
namespace memtable {
class ObMemtableBlockReader;
}
namespace storage
{
struct ObTableIterParam;
struct ObTableAccessContext;
struct ObRowSampleFilter;
class ObBlockRowStore;
class ObTableScanStoreStat;
class ObAggGroupBase;
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
  OB_INLINE int64_t get_data_length() const
  { return nullptr == reader_ ? 0 : reader_->original_data_length(); }
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
  int open_column_block(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &block_data,
      const ObCSRange &range);
  virtual int get_next_row(const ObDatumRow *&row);
  virtual int get_next_rows();
  virtual int apply_filter(const bool can_blockscan);
  virtual int set_ignore_shadow_row() { return OB_NOT_SUPPORTED;}
  int end_of_block() const;
  OB_INLINE int get_access_cnt() const { return reverse_scan_ ? (current_ - last_ + 1) : (last_ - current_ + 1);}
  int filter_micro_block_in_blockscan(sql::PushdownFilterInfo &pd_filter_info);
  int filter_micro_block_in_cg(
      sql::ObPushdownFilterExecutor *parent,
      sql::PushdownFilterInfo &pd_filter_info,
      const ObCGBitmap *parent_bitmap,
      const ObCSRowId micro_start_id,
      int64_t &access_count);
  int filter_pushdown_filter(
      sql::ObPushdownFilterExecutor *parent,
      sql::ObPushdownFilterExecutor *filter,
      sql::PushdownFilterInfo &filter_info,
      const bool can_use_vectorize,
      common::ObBitmap &bitmap);
  virtual int get_next_rows(
      const common::ObIArray<int32_t> &cols_projector,
      const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
      const int32_t *row_ids,
      const char **cell_datas,
      const int64_t row_cap,
      common::ObIArray<ObSqlDatumInfo> &datums,
      const int64_t datum_offset,
      uint32_t *len_array,
      const bool is_padding_mode,
      const bool init_vector_header = true);
  int advance_to_border(
      const ObDatumRowkey &rowkey,
      int64_t &start_offset,
      int64_t &end_offset);
  int check_can_group_by(
      const int32_t group_by_col,
      int64_t &row_cnt,
      int64_t &read_cnt,
      int64_t &distinct_cnt,
      bool &can_group_by) const;
  int read_distinct(
      const int32_t group_by_col,
      const char **cell_datas,
      const bool is_padding_mode,
      storage::ObGroupByCellBase &group_by_cell) const;
  int read_reference(
      const int32_t group_by_col,
      const int32_t *row_ids,
      const int64_t row_cap,
      storage::ObGroupByCellBase &group_by_cell) const;
  OB_INLINE void reserve_reader_memory(bool reserve)
  {
    if (nullptr != reader_) {
      reader_->reserve_reader_memory(reserve);
    }
  }

  int get_rows_for_old_format(
      const common::ObIArray<int32_t> &col_offsets,
      const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
      const int32_t *row_ids,
      const int64_t row_cap,
      const int64_t vector_offset,
      const char **cell_datas,
      sql::ObExprPtrIArray &exprs,
      common::ObIArray<ObSqlDatumInfo> &datum_infos,
      const common::ObIArray<blocksstable::ObStorageDatum> *default_datums,
      const bool is_padding_mode);
  int get_rows_for_rich_format(
      const common::ObIArray<int32_t> &col_offsets,
      const common::ObIArray<const share::schema::ObColumnParam *> &col_params,
      const int32_t *row_ids,
      const int64_t row_cap,
      const int64_t vector_offset,
      const char **cell_datas,
      uint32_t *len_array,
      sql::ObExprPtrIArray &exprs,
      const common::ObIArray<blocksstable::ObStorageDatum> *default_datums,
      const bool is_padding_mode,
      const bool need_init_vector = true);
  OB_INLINE int64_t get_current_pos() const
  { return current_; }
  OB_INLINE int64_t get_last_pos() const
  { return last_; }
  OB_INLINE ObIMicroBlockReader *get_reader() const
  { return reader_; }
  int get_next_border_rows(const ObDatumRowkey &rowkey);
  OB_INLINE bool can_blockscan() const
  { return can_blockscan_ && can_ignore_multi_version_; }
  OB_INLINE bool is_filter_applied() const
  { return is_filter_applied_; }
  void reset_blockscan()
  { can_blockscan_ = false; is_filter_applied_ = false; }
  int check_and_revert_non_border_rowkey(
      const ObDatumRowkey &border_rowkey,
      const ObDatumRow &deleted_row,
      ObCSRowId &co_current);
  int get_next_skip_row(const ObDatumRow *&row);
  int skip_to_range(
      const int64_t begin,
      const int64_t end,
      const ObDatumRange &range,
      const bool is_left_border,
      const bool is_right_border,
      int64_t &skip_row_idx,
      bool &has_data,
      bool &range_finished);
  int compare_rowkey(const ObDatumRowkey &rowkey, const bool is_cmp_end, int32_t &cmp_ret) const;
  OB_INLINE void skip_to_end()
  { current_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX; }
  VIRTUAL_TO_STRING_KV(K_(is_left_border), K_(is_right_border), K_(can_ignore_multi_version), K_(use_private_bitmap),
                       K_(can_blockscan), K_(is_filter_applied), K_(current), K_(start), K_(last), K_(reserved_pos), K_(step),
                       K_(macro_id));
protected:
  virtual int inner_get_next_row(const ObDatumRow *&row);
  int set_reader(const ObRowStoreType store_type);
  int set_base_scan_param(const bool is_left_bound_block,
                          const bool is_right_bound_block);
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
  int get_filter_result(ObFilterResult &res);
  bool is_di_bitmap_valid() const;
  int init_bitmap(ObCGBitmap *&bitmap, bool is_all_true);
  int inner_get_next_row_blockscan(const ObDatumRow *&row);
private:
  int apply_black_filter_batch(
      sql::ObPushdownFilterExecutor *parent,
      sql::ObBlackFilterExecutor &filter,
      sql::PushdownFilterInfo &pd_filter_info,
      common::ObBitmap &result_bitmap);
protected:
  bool is_inited_;
  bool reverse_scan_;
  bool is_left_border_;
  bool is_right_border_;
  bool can_ignore_multi_version_;
  bool can_blockscan_;
  bool is_filter_applied_;
  bool use_private_bitmap_;
  int64_t current_;         // current cursor
  int64_t start_;           // start of scan, inclusive.
  int64_t last_;            // end of scan, inclusive.
  int64_t reserved_pos_;
  int64_t step_;
  ObDatumRow row_;
  MacroBlockId macro_id_;
  const ObITableReadInfo *read_info_;
  const ObDatumRange *range_;
  const blocksstable::ObSSTable *sstable_;
  ObIMicroBlockReader *reader_;
  ObMicroBlockReaderHelper reader_helper_;
  memtable::ObMemtableBlockReader *memtable_reader_;
  const storage::ObTableIterParam *param_;
  storage::ObTableAccessContext *context_;
  ObIAllocator &allocator_;
  storage::ObBlockRowStore *block_row_store_;
  ObCGBitmap *di_bitmap_;
  ObCGBitmap *filter_bitmap_;
};

// tablet split ddl task scan bared row without multi-merge.
class ObMicroBlockRowDirectScanner final : public ObIMicroBlockRowScanner
{
public:
  ObMicroBlockRowDirectScanner(common::ObIAllocator &allocator)
    : ObIMicroBlockRowScanner(allocator)
  {}
  virtual ~ObMicroBlockRowDirectScanner() {}
  virtual int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable) override final;
  virtual int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &block_data,
      const bool is_left_border,
      const bool is_right_border) override final;
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
        finish_scanning_cur_rowkey_(true),
        is_last_multi_version_row_(true),
        read_row_direct_flag_(false),
        cell_allocator_(common::ObModIds::OB_SSTABLE_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        trans_version_col_idx_(-1),
        sql_sequence_col_idx_(-1),
        cell_cnt_(0)
  {}
  virtual ~ObMultiVersionMicroBlockRowScanner() {}
  virtual void reuse() override;
  virtual int switch_context(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable) override;
  virtual int init(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObSSTable *sstable) override;
  virtual int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &block_data,
      const bool is_left_border,
      const bool is_right_border) override;
  INHERIT_TO_STRING_KV("ObMultiVersionMicroBlockRowScanner",
                       ObIMicroBlockRowScanner, K_(read_row_direct_flag),
                       K_(version_range), K_(is_last_multi_version_row),
                       K_(finish_scanning_cur_rowkey));

protected:
  virtual int inner_get_next_row(const ObDatumRow *&row) override;
  int check_trans_version(
      bool &final_result,
      bool &version_fit,
      bool &have_uncommited_row,
      int64_t &trans_version,
      int64_t &sql_sequence,
      const int64_t index,
      const ObRowHeader *&row_header);
  int check_foreign_key(
      const int64_t trans_version,
      const int64_t sql_sequence,
      const ObRowHeader *row_header);
  void reuse_cur_micro_row();
private:
  void reuse_prev_micro_row();
  void inner_reset();
  int locate_cursor_to_read(bool &found_first_row);
  int inner_get_next_row_impl(const ObDatumRow *&ret_row);
  int inner_inner_get_next_row(
      const ObDatumRow *&ret_row,
      bool &version_fit,
      bool &final_result,
      bool &have_uncommitted_row);
  int inner_get_next_row_directly(
      const ObDatumRow *&ret_row,
      bool &version_fit,
      bool &final_result);
  int cache_cur_micro_row(const bool found_first_row, const bool final_result);
  int do_compact(const ObDatumRow *src_row, ObDatumRow &dest_row, bool &final_result);
  int lock_for_read(
      const transaction::ObLockForReadArg &lock_for_read_arg,
      bool &can_read,
      int64_t &trans_version);
  // The store_rowkey is a decoration of the ObObj pointer,
  // and it will be destroyed when the life cycle of the rowkey_helper is end.
  // So we have to send it into the function to avoid this situation.
  int get_store_rowkey(ObStoreRowkey &store_rowkey, ObDatumRowkeyHelper &rowkey_helper);
protected:
  // Use shallow_copy to directly quote the original data of the microblock when compacting,
  // only at the moment (when the dump row format is flat) there is no risk
  // TRUE:it means that the compacted result of the current rowkey has been obtained
  // (not necessarily reading the L mark, it may have been fuse to no nop column)
  bool finish_scanning_cur_rowkey_;
  // TRUE: meet Last Flag of current rowkey
  bool is_last_multi_version_row_;
  bool read_row_direct_flag_;
  ObDatumRow prev_micro_row_;
  ObDatumRow tmp_row_;
private:
  storage::ObNopPos nop_pos_;
  common::ObArenaAllocator cell_allocator_;
  int64_t trans_version_col_idx_;
  int64_t sql_sequence_col_idx_;
  int64_t cell_cnt_;
  common::ObVersionRange version_range_;
};

// multi version sstable micro block scanner for mow tables
class ObMultiVersionDIMicroBlockRowScanner final : public ObMultiVersionMicroBlockRowScanner
{
public:
  ObMultiVersionDIMicroBlockRowScanner(common::ObIAllocator &allocator)
      : ObMultiVersionMicroBlockRowScanner(allocator),
        is_prev_micro_row_valid_(false),
        found_first_di_row_(false)
  {}
  virtual ~ObMultiVersionDIMicroBlockRowScanner() {}
  virtual void reuse() override final;
  virtual int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &block_data,
      const bool is_left_border,
      const bool is_right_border) override final;
  INHERIT_TO_STRING_KV("ObMultiVersionDIMicroBlockRowScanner", ObMultiVersionMicroBlockRowScanner,
      K_(is_prev_micro_row_valid), K_(found_first_di_row));
protected:
  virtual int inner_get_next_row(const ObDatumRow *&row) override final;
private:
  int inner_get_next_di_row(const ObDatumRow *&row);
  int inner_get_next_compact_di_row(const ObDatumRow *&ret_row);
  int inner_get_next_header_info(
    int64_t &index,
    bool &version_fit,
    bool &final_result,
    ObDmlRowFlag &row_flag);
  int locate_next_rowkey(int64_t &index);
  int preprocess_di_rows();
  int compact_rows_of_same_rowkey(
      const int64_t insert_idx,
      const int64_t delete_idx,
      const ObDatumRow *&ret_row);
  int check_meet_next_rowkey(
      const int64_t start,
      const int64_t end,
      bool &meet_next_rowkey) const;
  int try_cache_unfinished_row(
      const int64_t insert_idx,
      const int64_t delete_idx);
  int get_trans_version(
      int64_t &trans_version,
      const int64_t index);
  int set_row_trans_col(
      const int64_t trans_version,
      ObDatumRow &row);

private:
  bool is_prev_micro_row_valid_;
  bool found_first_di_row_;
};

// multi version sstable micro block scanner for minor merge
class ObMultiVersionMicroBlockMinorMergeRowScanner : public ObIMicroBlockRowScanner
{
public:
  ObMultiVersionMicroBlockMinorMergeRowScanner(common::ObIAllocator &allocator)
      : ObIMicroBlockRowScanner(allocator),
      trans_version_col_idx_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
      sql_sequence_col_idx_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
      committed_trans_version_(INT64_MAX),
      last_trans_state_(INT64_MAX)
  {}
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
  virtual int apply_filter(const bool can_blockscan) override final
  {
    UNUSED(can_blockscan);
    return OB_NOT_SUPPORTED;
  }
  virtual int get_next_rows() override
  { return OB_NOT_SUPPORTED; }
  int get_first_row_mvcc_info(bool &is_first_row, bool &is_shadow_row) const;
  TO_STRING_KV(K_(macro_id), K_(start), K_(current), K_(last));
protected:
  virtual int inner_get_next_row(const ObDatumRow *&row) override;
private:
  int get_trans_state(
    const transaction::ObTransID &read_trans_id,
    int64_t &state,
    bool &can_read);
  int64_t get_trans_state_from_cache(
    const transaction::ObTransID &read_trans_id,
    const transaction::ObTxSEQ &sql_seq,
    bool &can_read);
  int get_trans_state_from_tx_table(
    const transaction::ObTransID &read_trans_id,
    const transaction::ObTxSEQ &sql_seq,
    int64_t &state,
    bool &can_read);
  int check_row_trans_state(bool &skip_curr_row);
private:
  // multi version
  int64_t trans_version_col_idx_;
  int64_t sql_sequence_col_idx_;
  int64_t committed_trans_version_;
  int64_t last_trans_state_;
};

}
}
#endif //OB_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_ROW_SCANNER_H_
