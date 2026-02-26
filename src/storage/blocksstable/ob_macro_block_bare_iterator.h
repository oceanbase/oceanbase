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
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"


namespace oceanbase
{
namespace blocksstable
{

// Iterate micro block data in macro block without index
class ObMicroBlockBareIterator
{
public:
  ObMicroBlockBareIterator(const uint64_t tenant_id = MTL_ID());
  virtual ~ObMicroBlockBareIterator();
  void reset();
  void reuse();

  // whole macro block scan, used for:
  //     1.index macro block
  //     2.data macro block without n-1 index micro block
  //     3.data macro block with n-1 index micro block, but can not get micro_index_data
  int open(
      const char *macro_block_buf,
      const int64_t macro_block_buf_size,
      const bool need_check_data_integrity = false,
      const bool need_deserialize = true);

  // whole macro block scan, used for data macro block with n-1 index micro block
  //      Only after calling this interface can the get_next function retrieve micro_index_data.
  int open_for_whole_range(
      const char *macro_block_buf,
      const int64_t macro_block_buf_size,
      const bool need_deserialize);

  // scan by range, used for data macro block with n-1 index micro block
  int open(
      const char *macro_block_buf,
      const int64_t macro_block_buf_size,
      const ObDatumRange &range,
      const ObITableReadInfo &rowkey_read_info,
      const bool is_left_border,
      const bool is_right_border,
      const bool need_deserialize = true);
  ObRowStoreType get_row_type() { return ObRowStoreType(macro_block_header_.fixed_header_.row_store_type_); }
  int set_end_iter_idx(const bool is_start);
  int get_curr_start_row_offset(int64_t &row_offset);
  int get_next_micro_block_data(ObMicroBlockData &micro_block);
  int get_next_micro_block_data_and_offset(ObMicroBlockData &micro_block, int64_t &offset);
  // get index micro block when whole macro block scan
  int get_next_micro_block_desc(ObMicroBlockDesc &micro_block_desc,
                                const ObDataStoreDesc &data_store_desc,
                                ObIAllocator &allocator,
                                const bool need_check_sum);
  // get uncompressed data micro block and raw micro block filled in macro block when whole macro block scan
  int get_next_micro_block_desc(ObMicroBlockDesc &uncompressed_micro_block_desc,
                                ObMicroBlockDesc &micro_block_desc,
                                ObIAllocator &allocator);
  // get data micro block and micro_index_data when scan by range or whole macro block scan
  int get_next_micro_block_desc(
      ObMicroBlockDesc &micro_block_desc,
      ObMicroIndexData &micro_index_data,
      ObIAllocator &rowkey_allocator);
  int get_macro_block_header(ObSSTableMacroBlockHeader &macro_header);
  int get_macro_meta(ObDataMacroBlockMeta *&macro_meta, ObIAllocator &allocator);
  int get_micro_block_count(int64_t &micro_block_count);
  int get_index_block(ObMicroBlockData &micro_block, const bool force_deserialize, const bool is_macro_meta_block = false);
  int get_macro_meta_block(ObMicroBlockData &micro_block);
  bool is_left_border() const { return iter_idx_ == begin_idx_; }
  bool is_right_border() const { return iter_idx_ == end_idx_; }
  TO_STRING_KV(KP_(macro_block_buf), K_(macro_block_buf_size), K_(common_header),
      K_(macro_block_header), K_(begin_idx), K_(end_idx), K_(iter_idx), K_(read_pos),
      K_(need_deserialize), K_(is_inited));
protected:
  int check_macro_block_data_integrity(const char *payload_buf, const int64_t payload_size);
  int locate_range(
      const ObDatumRange &range,
      const ObITableReadInfo &index_read_info,
      const bool is_left_border,
      const bool is_right_border);
  int set_reader(const ObRowStoreType store_type);
private:
  OB_INLINE bool check_column_checksums(const ObMicroBlockHeader *header) const
  {
    return !header->has_column_checksum_ || nullptr != header->column_checksums_;
  }
  int generate_uncompressed_micro_block(
    const ObDatumRowkey &rowkey,
    const ObMicroBlockHeader *header,
    const ObMicroBlockData &micro_block,
    ObIAllocator &allocator,
    ObMicroBlockDesc &micro_block_desc);
  int generate_micro_block(
    const ObDatumRowkey &rowkey,
    const ObMicroBlockHeader *header,
    int64_t block_offset,
    const char *micro_buf,
    ObIAllocator &allocator,
    ObMicroBlockDesc &micro_block_desc);
  int generate_basic_micro_block_desc(
    const ObDatumRowkey &rowkey,
    const ObMicroBlockHeader *header,
    ObIAllocator &allocator,
    ObMicroBlockDesc &micro_block_desc);
  int get_micro_block_header(const char *buf, int64_t buf_len, ObMicroBlockHeader *&header) const;
protected:
  ObArenaAllocator allocator_;
  const char *macro_block_buf_;
  int64_t macro_block_buf_size_;
  ObMacroBlockReader macro_reader_;
  ObMacroBlockReader index_reader_;
  ObMacroBlockCommonHeader common_header_;
  ObSSTableMacroBlockHeader macro_block_header_;
  ObIMicroBlockReader *reader_;
  ObMicroBlockReaderHelper micro_reader_helper_;
  int64_t index_rowkey_cnt_;
  int64_t column_cnt_;
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
  ObMacroBlockRowBareIterator(common::ObIAllocator &allocator, const uint64_t tenant_id = MTL_ID());
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
  { return rowkey_descs_; }
  int get_column_checksums(const int64_t *&column_checksums);
private:
  ObDatumRow row_;
  ObMicroBlockBareIterator micro_iter_;
  const ObObjMeta *column_types_;
  const int64_t *column_checksums_;
  common::ObFixedArray<share::schema::ObColDesc, common::ObIAllocator> rowkey_descs_;
  common::ObIAllocator *allocator_;
  ObIMicroBlockReader *micro_reader_;
  ObMicroBlockData curr_micro_block_data_;
  int64_t curr_block_row_idx_;
  int64_t curr_block_row_cnt_;
  ObMicroBlockReaderHelper reader_helper_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlockRowBareIterator);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_BARE_ITERATOR_H_
