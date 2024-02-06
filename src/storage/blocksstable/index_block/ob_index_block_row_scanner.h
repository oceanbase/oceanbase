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

#ifndef OB_INDEX_BLOCK_ROW_SCANNER_H_
#define OB_INDEX_BLOCK_ROW_SCANNER_H_

#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_micro_block_reader_helper.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/column_store/ob_column_store_util.h"
#include "ob_index_block_row_struct.h"


namespace oceanbase
{
namespace storage
{
struct ObTableIterParam;
struct ObTableAccessContext;
class ObBlockMetaTree;
class ObRowsInfo;
}
namespace blocksstable
{
class ObSSTable;
// Memory structure of Index micro block.
// This struct won't hold extra memory, lifetime security need to be ensured by caller
struct ObIndexBlockDataHeader
{
  OB_INLINE bool is_valid() const
  {
    return row_cnt_ >= 0
        && col_cnt_ > 0
        && nullptr != rowkey_array_
        && nullptr != datum_array_;
  }
  int get_index_data(const int64_t row_idx, const char *&index_ptr, int64_t &index_len) const;

  int deep_copy_transformed_index_block(
      const ObIndexBlockDataHeader &header,
      const int64_t buf_size,
      char *buf,
      int64_t &pos);

  int64_t row_cnt_;
  int64_t col_cnt_;
  // Array of rowkeys in index block
  const ObDatumRowkey *rowkey_array_;
  // Array of deserialzed Object array
  ObStorageDatum *datum_array_;
  const char *data_buf_;
  int64_t data_buf_size_;

  TO_STRING_KV(
      K_(row_cnt), K_(col_cnt),
      // "Rowkeys:", common::ObArrayWrap<ObDatumRowkey>(rowkey_array_, row_cnt_)
      // output first only
      KPC_(rowkey_array)
      );
};

class ObIndexBlockDataTransformer
{
public:
  ObIndexBlockDataTransformer();
  virtual ~ObIndexBlockDataTransformer();
  int transform(
      const ObMicroBlockData &raw_data,
      ObMicroBlockData &transformed_data,
      ObIAllocator &allocator,
      char *&allocated_buf);

  // For micro header bug in version before 4.3, when root block serialized in sstable meta,
  // data length related fileds was lefted to be filled
int fix_micro_header_and_transform(
    const ObMicroBlockData &raw_data,
    ObMicroBlockData &transformed_data,
    ObIAllocator &allocator,
    char *&allocated_buf);
  static int get_transformed_upper_mem_size(const char *raw_block_data, int64_t &mem_limit);
private:
  int get_reader(const ObRowStoreType store_type, ObIMicroBlockReader *&micro_reader);
private:
  ObArenaAllocator allocator_;
  ObMicroBlockReaderHelper micro_reader_helper_;
};

class ObIndexBlockRowScanner
{
public:
  ObIndexBlockRowScanner();
  virtual ~ObIndexBlockRowScanner();
  void reuse();
  void reset();

  int init(
      const ObIArray<int32_t> &agg_projector,
      const ObIArray<share::schema::ObColumnSchemaV2> &agg_column_schema,
      const ObStorageDatumUtils &datum_utils,
      ObIAllocator &allocator,
      const common::ObQueryFlag &query_flag,
      const int64_t nested_offset,
      const bool is_normal_cg = false);
  int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &idx_block_data,
      const ObDatumRowkey &rowkey,
      const int64_t range_idx = 0,
      const ObMicroIndexInfo *idx_info = nullptr);
  int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &idx_block_data,
      const ObDatumRange &range,
      const int64_t range_idx,
      const bool is_left_border,
      const bool is_right_border,
      const ObMicroIndexInfo *idx_info = nullptr);
  int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &idx_block_data,
      const ObRowsInfo *rows_info,
      const int64_t rowkey_begin_idx,
      const int64_t rowkey_end_idx);
  int get_next(
      ObMicroIndexInfo &idx_block_row,
      const bool is_multi_check = false);
  bool end_of_block() const;
  int get_index_row_count(int64_t &index_row_count) const;
  int check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan);
  int locate_range(const ObDatumRange &range, const bool is_left_border, const bool is_right_border);
  int advance_to_border(
      const ObDatumRowkey &rowkey,
      const int32_t range_idx,
      ObCSRange &cs_range);
  int find_out_rows(
      const int32_t range_idx,
      int64_t &found_idx);
  int find_out_rows_from_start_to_end(
      const int32_t range_idx,
      const ObCSRowId start_row_id,
      bool &is_certain,
      int64_t &found_idx);
  bool is_in_border();
  inline const ObDatumRowkey &get_end_key() const
  {
    return idx_data_header_->rowkey_array_[idx_data_header_->row_cnt_ - 1];
  }
  OB_INLINE bool is_valid() const { return is_inited_; }
  void switch_context(const ObSSTable &sstable, const ObStorageDatumUtils &datum_utils);
  TO_STRING_KV(K_(current), K_(start), K_(end), K_(step),
               K_(range_idx), K_(is_get), K_(is_reverse_scan),
               K_(is_left_border), K_(is_right_border),
               K_(rowkey_begin_idx), K_(rowkey_end_idx),
               K_(is_inited), K_(index_format), K_(macro_id), KPC_(idx_data_header), KPC_(datum_utils),
               K_(is_normal_cg), K_(parent_row_range), K_(filter_constant_type));
private:
  int init_by_micro_data(const ObMicroBlockData &idx_block_data);
  int locate_key(const ObDatumRowkey &rowkey);
  int init_datum_row();
  int read_curr_idx_row(const ObIndexBlockRowHeader *&idx_row_header, const ObDatumRowkey *&endkey);
  int get_cur_row_id_range(ObCSRange &cs_range);
  int get_idx_row_header_in_target_idx(
      const ObIndexBlockRowHeader *&idx_row_header,
      const int64_t idx);
  int advance_to_border(
      const ObDatumRowkey &rowkey,
      const int64_t limit_idx,
      ObCSRange &cs_range);
  int get_next_idx_row(ObMicroIndexInfo &idx_block_row);
  void skip_index_rows();
  int find_rowkeys_belong_to_same_idx_row(int64_t &rowkey_idx);
  int skip_to_next_valid_position(ObMicroIndexInfo &idx_block_row);
private:
  union {
    const ObDatumRowkey *rowkey_;
    const ObDatumRange *range_;
    const ObRowsInfo *rows_info_;
    const void *query_range_;
  };
  enum IndexFormat {
    INVALID = 0,
    RAW_DATA,
    TRANSFORMED,
    BLOCK_TREE
  };
  const ObIArray<int32_t> *agg_projector_;
  const ObIArray<share::schema::ObColumnSchemaV2> *agg_column_schema_;
  const ObIndexBlockDataHeader *idx_data_header_;
  MacroBlockId macro_id_;
  ObIAllocator *allocator_;
  ObMicroBlockReaderHelper micro_reader_helper_;
  ObIMicroBlockReader *micro_reader_;
  storage::ObBlockMetaTree *block_meta_tree_;
  ObDatumRow *datum_row_;
  ObDatumRowkey endkey_;
  ObIndexBlockRowParser idx_row_parser_;
  const ObStorageDatumUtils *datum_utils_;
  int64_t current_;
  int64_t start_;               // inclusive
  int64_t end_;                 // inclusive
  int64_t step_;
  int64_t range_idx_;
  int64_t nested_offset_;
  int64_t rowkey_begin_idx_;
  int64_t rowkey_end_idx_;
  IndexFormat index_format_;
  ObCSRange parent_row_range_;
  bool is_get_;
  bool is_reverse_scan_;
  bool is_left_border_;
  bool is_right_border_;
  bool is_inited_;
  bool is_normal_cg_;
  sql::ObBoolMaskType filter_constant_type_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif
