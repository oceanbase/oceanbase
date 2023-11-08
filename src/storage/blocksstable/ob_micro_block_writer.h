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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_MICRO_BLOCK_WRITER_H_
#include "ob_block_sstable_struct.h"
#include "ob_row_writer.h"
#include "ob_imicro_block_writer.h"

namespace oceanbase
{
namespace common
{
class ObNewRow;
}
namespace blocksstable
{
// memory
//  |- row data buffer
//        |- ObMicroBlockHeader
//        |- row data
//  |- row index buffer
//        |- ObRowIndex
//  |- row hash index builder(optional)
//
// build output
//  |- compressed data
//        |- ObMicroBlockHeader
//        |- row data
//        |- RowIndex
//        |- RowHashIndex(optional)
class ObMicroBlockWriter : public ObIMicroBlockWriter
{
  static const int64_t INDEX_ENTRY_SIZE = sizeof(int32_t);
  static const int64_t DEFAULT_DATA_BUFFER_SIZE = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  static const int64_t DEFAULT_INDEX_BUFFER_SIZE = 2 * 1024;
  static const int64_t MIN_RESERVED_SIZE = 1024; //1KB;
public:
  ObMicroBlockWriter();
  virtual ~ObMicroBlockWriter();
  int init(
      const int64_t micro_block_size_limit,
      const int64_t rowkey_column_count,
      const int64_t column_count = 0,
      const common::ObIArray<share::schema::ObColDesc> *col_desc_array = nullptr,
      const bool is_major = false);

  virtual int append_row(const ObDatumRow &row);
  virtual int build_block(char *&buf, int64_t &size);
  virtual void reuse();

  virtual int64_t get_block_size() const override;
  virtual int64_t get_row_count() const override;
  virtual int64_t get_data_size() const override;
  virtual int64_t get_column_count() const override;
  virtual int64_t get_original_size() const override;
  virtual int append_hash_index(ObMicroBlockHashIndexBuilder& hash_index_builder);
  virtual bool has_enough_space_for_hash_index(const int64_t hash_index_size) const;
  void reset();
private:
  int inner_init();
  inline int64_t get_index_size() const;
  inline int64_t get_future_block_size(const int64_t row_length) const;
  int try_to_append_row(const int64_t &row_length);
  int check_input_param(
      const int64_t macro_block_size,
      const int64_t column_count,
      const int64_t rowkey_column_count);
  int finish_row(const int64_t length);
  int reserve_header(
      const int64_t column_count,
      const int64_t rowkey_column_count,
      const bool need_calc_column_chksum);
  bool is_exceed_limit(const int64_t row_length);
  int64_t get_data_base_offset() const;
  int64_t get_index_base_offset() const;
  int process_out_row_columns(const ObDatumRow &row);
private:
  int64_t micro_block_size_limit_;
  int64_t column_count_;
  ObRowWriter row_writer_;
  int64_t rowkey_column_count_;
  ObSelfBufferWriter data_buffer_;
  ObSelfBufferWriter index_buffer_;
  const common::ObIArray<share::schema::ObColDesc> *col_desc_array_;
  bool is_major_;
  bool is_inited_;
};

inline int64_t ObMicroBlockWriter::get_block_size() const
{
  return get_data_size() + get_index_size();
}
inline int64_t ObMicroBlockWriter::get_row_count() const
{
  return NULL == header_ ? 0 : header_->row_count_;
}
inline int64_t ObMicroBlockWriter::get_data_size() const
{
  int64_t data_size = data_buffer_.length();
  if (data_size == 0) { // lazy allocate
    data_size = get_data_base_offset();
  }
  return data_size;
}
inline int64_t ObMicroBlockWriter::get_column_count() const
{
  return column_count_;
}
inline int64_t ObMicroBlockWriter::get_index_size() const
{
  int64_t index_size = index_buffer_.length();
  if (index_size == 0) { // lazy allocate
    index_size = get_index_base_offset();
  }
  return index_size;
}
inline int64_t ObMicroBlockWriter::get_future_block_size(const int64_t row_length) const {
  return get_data_size() + row_length
             + get_index_size() + INDEX_ENTRY_SIZE;
}

inline int64_t ObMicroBlockWriter::get_data_base_offset() const
{
  return ObMicroBlockHeader::get_serialize_size(column_count_, is_major_);
}

inline int64_t ObMicroBlockWriter::get_index_base_offset() const
{
  return sizeof(int32_t);
}

inline int64_t ObMicroBlockWriter::get_original_size() const
{
  int64_t original_size = 0;
  if (OB_NOT_NULL(header_)) {
    original_size = data_buffer_.pos() - header_->header_size_;
  }
  return original_size;
}

}//end namespace blocksstable
}//end namespace oceanbase
#endif
