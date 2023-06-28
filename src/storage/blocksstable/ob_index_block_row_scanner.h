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
#include "ob_block_sstable_struct.h"
#include "ob_micro_block_reader_helper.h"
#include "ob_index_block_row_struct.h"
#include "ob_datum_range.h"

namespace oceanbase
{
namespace storage
{
struct ObTableIterParam;
struct ObTableAccessContext;
class ObBlockMetaTree;
}
namespace blocksstable
{
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
  int get_index_data(const int64_t row_idx, const char *&index_ptr) const;

  int64_t row_cnt_;
  int64_t col_cnt_;
  // Array of rowkeys in index block
  const ObDatumRowkey *rowkey_array_;
  // Array of deserialzed Object array
  ObStorageDatum *datum_array_;
  TO_STRING_KV(
      K_(row_cnt), K_(col_cnt),
      "Rowkeys:", common::ObArrayWrap<ObDatumRowkey>(rowkey_array_, row_cnt_));
};

class ObIndexBlockDataTransformer
{
public:
  ObIndexBlockDataTransformer();
  virtual ~ObIndexBlockDataTransformer();
  int transform(
      const ObMicroBlockData &block_data,
      char *transform_buf,
      int64_t buf_len);
  int update_index_block(
      const ObIndexBlockDataHeader &src_idx_header,
      const char *micro_data,
      const int64_t micro_data_size,
      char *transform_buf,
      int64_t buf_len);
  static int64_t get_transformed_block_mem_size(const int64_t row_cnt, const int64_t idx_col_cnt);
  static int64_t get_transformed_block_mem_size(const ObMicroBlockData &block_data);
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
      const int64_t nested_offset);
  int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &idx_block_data,
      const ObDatumRowkey &rowkey,
      const int64_t range_idx = 0);
  int open(
      const MacroBlockId &macro_id,
      const ObMicroBlockData &idx_block_data,
      const ObDatumRange &range,
      const int64_t range_idx,
      const bool is_left_border,
      const bool is_right_border);
  int get_next(ObMicroIndexInfo &idx_block_row);
  bool end_of_block() const;
  int get_index_row_count(int64_t &index_row_count) const;
  int check_blockscan(const ObDatumRowkey &rowkey, bool &can_blockscan);
  OB_INLINE bool is_valid() const { return is_inited_; }
  void switch_context(const ObSSTable &sstable, const ObStorageDatumUtils &datum_utils);
  TO_STRING_KV(K_(current), K_(start), K_(end), K_(step),
               K_(range_idx), K_(is_get), K_(is_reverse_scan),
               K_(is_left_border), K_(is_right_border),
               K_(is_inited), K_(index_format), K_(macro_id), KPC_(idx_data_header), KPC_(datum_utils));
private:
  int init_by_micro_data(const ObMicroBlockData &idx_block_data);
  int locate_key(const ObDatumRowkey &rowkey);
  int locate_range(const ObDatumRange &range, const bool is_left_border, const bool is_right_border);
  int init_datum_row();
  int read_curr_idx_row(const ObIndexBlockRowHeader *&idx_row_header, const ObDatumRowkey *&endkey);
private:
  union {
    const ObDatumRowkey *rowkey_;
    const ObDatumRange *range_;
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
  IndexFormat index_format_;
  bool is_get_;
  bool is_reverse_scan_;
  bool is_left_border_;
  bool is_right_border_;
  bool is_inited_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif
