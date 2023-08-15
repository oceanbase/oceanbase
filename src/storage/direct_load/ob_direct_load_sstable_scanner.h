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
#pragma once

#include "ob_direct_load_tmp_file.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/direct_load/ob_direct_load_external_row.h"
#include "storage/direct_load/ob_direct_load_rowkey_iterator.h"
#include "storage/direct_load/ob_direct_load_sstable_builder.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"

namespace oceanbase
{
namespace storage
{
struct ObDirectLoadIndexBlockMeta
{
public:
  ObDirectLoadIndexBlockMeta() : row_count_(0) {}
  TO_STRING_KV(K_(row_count), K(rowkey_column_count_), K(end_key_));
public:
  int64_t row_count_;
  int64_t rowkey_column_count_;
  blocksstable::ObDatumRowkey end_key_;
};

class ObDirectLoadSSTableScanner
{
public:
  ObDirectLoadSSTableScanner();
  virtual ~ObDirectLoadSSTableScanner();
  int init(ObDirectLoadSSTable *sstable, const ObDirectLoadTableDataDesc &table_data_desc,
           const blocksstable::ObDatumRange &range,
           const blocksstable::ObStorageDatumUtils *datum_utils);
  int get_next_row(const blocksstable::ObDatumRow *&datum_row);
  int get_next_row(const ObDirectLoadExternalRow *&external_row);
  TO_STRING_KV(KP(buf_), K(buf_pos_), K(buf_size_), K(curr_idx_), K(start_idx_), K(end_idx_),
               K(offset_));
private:
  int get_start_key(int64_t idx, blocksstable::ObDatumRowkey &startkey);
  int inner_open();
  int locate_next_buffer();
  int change_buffer();
  int compare(blocksstable::ObDatumRowkey cmp_key, const ObDirectLoadExternalRow &external_row,
              int &cmp_ret);
  int inner_get_next_row(const ObDirectLoadExternalRow *&external_row);
  int read_buffer(uint64_t offset, uint64_t size);
  int get_next_buffer();
  int get_large_buffer(int64_t buf_size);
  void assign(const int64_t buf_cap, char *buf);
  int locate_lower_bound(const blocksstable::ObDatumRowkey &rowkey, int64_t &logic_id);
  int locate_upper_bound(const blocksstable::ObDatumRowkey &rowkey, int64_t &logic_id);
private:
  int64_t buf_pos_;
  int64_t buf_size_;
  int64_t sstable_data_block_size_;
  int64_t rowkey_column_num_;
  int64_t column_count_;
  char *buf_;
  char *large_buf_;
  int64_t io_timeout_ms_;
  int64_t curr_idx_;
  int64_t start_idx_;
  int64_t end_idx_;
  int64_t start_fragment_idx_;
  int64_t end_fragment_idx_;
  int64_t curr_fragment_idx_;
  uint64_t locate_start_offset_;
  uint64_t locate_end_offset_;
  bool found_start_;
  bool found_end_;
  uint64_t offset_;
  blocksstable::ObDatumRow datum_row_;
  ObDirectLoadTmpFileIOHandle file_io_handle_;
  ObDirectLoadSSTable *sstable_;
  const blocksstable::ObDatumRange *query_range_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  common::ObArenaAllocator allocator_;
  ObDirectLoadDataBlockReader2 data_block_reader_;
  ObDirectLoadIndexBlockReader index_block_reader_;
  ObDirectLoadSSTableFragmentOperator fragment_operator_;
  bool is_inited_;
};

class ObDirectLoadIndexBlockMetaIterator
{
public:
  ObDirectLoadIndexBlockMetaIterator();
  ~ObDirectLoadIndexBlockMetaIterator();
public:
  int init(ObDirectLoadSSTable *sstable);
  int get_next(ObDirectLoadIndexBlockMeta &index_meta);
private:
  int64_t get_total_block_count() const { return total_index_block_count_; }
  void assign(const int64_t buf_pos, const int64_t buf_cap, char *buf);
  int get_end_key(ObDirectLoadExternalRow &row, ObDirectLoadIndexBlockMeta &index_block_meta);
  int get_row(int64_t idx, ObDirectLoadIndexBlockMeta &meta);
  int read_buffer(uint64_t offset, uint64_t size);
  TO_STRING_KV(KP(buf_), K(buf_pos_), K(buf_size_), K(curr_fragment_idx_), K(curr_block_idx_),
               K(total_index_block_count_), K(index_item_num_per_block_));
private:
  int64_t buf_pos_;
  int64_t buf_size_;
  int64_t io_timeout_ms_;
  char *buf_;
  ObDirectLoadExternalRow row_;
  int64_t curr_fragment_idx_;
  int64_t curr_block_idx_;
  int64_t rowkey_column_count_;
  int64_t total_index_block_count_;
  int64_t index_item_num_per_block_;
  ObDirectLoadTmpFileIOHandle file_io_handle_;
  ObDirectLoadSSTable *sstable_;
  common::ObArenaAllocator allocator_;
  ObDirectLoadIndexBlockReader index_block_reader_;
  ObDirectLoadSSTableFragmentOperator fragment_operator_;
  bool is_inited_;
};

class ObDirectLoadIndexBlockEndKeyIterator : public ObIDirectLoadDatumRowkeyIterator
{
public:
  ObDirectLoadIndexBlockEndKeyIterator() : index_block_meta_iter_(nullptr), is_inited_(false) {}
  virtual ~ObDirectLoadIndexBlockEndKeyIterator();
  int init(ObDirectLoadIndexBlockMetaIterator *index_block_meta_iter);
  int get_next_rowkey(const blocksstable::ObDatumRowkey *&rowkey) override;
private:
  ObDirectLoadIndexBlockMetaIterator *index_block_meta_iter_;
  blocksstable::ObDatumRowkey rowkey_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase