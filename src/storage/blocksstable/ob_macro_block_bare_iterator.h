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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_BARE_ITERATOR_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_BARE_ITERATOR_H_

#include "ob_macro_block_reader.h"
#include "ob_micro_block_reader_helper.h"


namespace oceanbase
{
namespace blocksstable
{

// Iterate micro block data in macro block without index
class ObMicroBlockBareIterator
{
public:
  ObMicroBlockBareIterator();
  virtual ~ObMicroBlockBareIterator();
  void reset();
  void reuse();

  // whole macro block scan
  int open(
      const char *macro_block_buf,
      const int64_t macro_block_buf_size,
      const bool need_check_data_integrity = false,
      const bool need_deserialize = true);
  // scan by range
  int open(
      const char *macro_block_buf,
      const int64_t macro_block_buf_size,
      const ObDatumRange &range,
      const ObITableReadInfo &rowkey_read_info,
      const bool is_left_border,
      const bool is_right_border);
  int get_next_micro_block_data(ObMicroBlockData &micro_block);
  int get_macro_block_header(ObSSTableMacroBlockHeader &macro_header);
  int get_micro_block_count(int64_t &micro_block_count);
  int get_index_block(ObMicroBlockData &micro_block, const bool is_macro_meta_block = false);
  int get_macro_meta_block(ObMicroBlockData &micro_block);
  bool is_left_border() const { return iter_idx_ == begin_idx_; }
  bool is_right_border() const { return iter_idx_ == end_idx_; }
  TO_STRING_KV(KP_(macro_block_buf), K_(macro_block_buf_size), K_(common_header),
      K_(macro_block_header), K_(begin_idx), K_(end_idx), K_(iter_idx), K_(read_pos),
      K_(need_deserialize), K_(is_inited));
private:
  int check_macro_block_data_integrity(const char *payload_buf, const int64_t payload_size);
  int locate_range(
      const ObDatumRange &range,
      const ObITableReadInfo &index_read_info,
      const bool is_left_border,
      const bool is_right_border);
  int set_reader(const ObRowStoreType store_type);
private:
  ObArenaAllocator allocator_;
  const char *macro_block_buf_;
  int64_t macro_block_buf_size_;
  ObMacroBlockReader macro_reader_;
  ObMacroBlockCommonHeader common_header_;
  ObSSTableMacroBlockHeader macro_block_header_;
  ObIMicroBlockReader *reader_;
  ObMicroBlockReaderHelper micro_reader_helper_;
  int64_t begin_idx_;
  int64_t end_idx_;
  int64_t iter_idx_;
  int64_t read_pos_;
  bool need_deserialize_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockBareIterator);
};

// Iterate all rows in macro block
class ObMacroBlockRowBareIterator
{
public:
  ObMacroBlockRowBareIterator(common::ObIAllocator &allocator);
  virtual ~ObMacroBlockRowBareIterator();

  void reset();
  int open(
      const char *macro_block_buf,
      const int64_t macro_block_buf_size,
      const bool need_check_integrity = false);
  int get_next_row(const ObDatumRow *&row);
  // switch to iterate leaf index block in data macro block
  int open_leaf_index_micro_block(const bool is_macro_meta = false);
  int open_next_micro_block();

  int get_macro_block_header(ObSSTableMacroBlockHeader &macro_header);
  int get_curr_micro_block_data(const ObMicroBlockData *&block_data);
  int get_curr_micro_block_row_cnt(int64_t &row_count);
  OB_INLINE const common::ObIArray<share::schema::ObColDesc> &get_rowkey_column_descs() const
  { return col_read_info_.get_columns_desc(); }
  int get_column_checksums(const int64_t *&column_checksums);
private:
  int init_micro_reader(const ObRowStoreType store_type);
private:
  ObDatumRow row_;
  ObMicroBlockBareIterator micro_iter_;
  const ObObjMeta *column_types_;
  const int64_t *column_checksums_;
  ObRowkeyReadInfo col_read_info_;
  common::ObIAllocator *allocator_;
  ObIMicroBlockReader *micro_reader_;
  ObMicroBlockData curr_micro_block_data_;
  int64_t curr_block_row_idx_;
  int64_t curr_block_row_cnt_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockRowBareIterator);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_BARE_ITERATOR_H_
