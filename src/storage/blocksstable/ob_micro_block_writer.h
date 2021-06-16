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
#include "ob_data_buffer.h"
#include "ob_row_writer.h"
#include "ob_imicro_block_writer.h"

namespace oceanbase {
namespace common {
class ObNewRow;
}
namespace storage {
class ObStoreRow;
}
namespace blocksstable {
// memory
//  |- row data buffer
//        |- ObMicroBlockHeader
//        |- row data
//  |- row index buffer
//        |- ObRowIndex
//
// build output
//  |- compressed data
//        |- ObMicroBlockHeader
//        |- row data
//        |- RowIndex
class ObMicroBlockWriter : public ObIMicroBlockWriter {
  static const int64_t INDEX_ENTRY_SIZE = sizeof(int32_t);
  static const int64_t DEFAULT_DATA_BUFFER_SIZE = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  static const int64_t DEFAULT_INDEX_BUFFER_SIZE = 2 * 1024;
  static const int64_t MIN_RESERVED_SIZE = 1024;  // 1KB;
public:
  ObMicroBlockWriter();
  virtual ~ObMicroBlockWriter();
  int init(const int64_t micro_block_size_limit, const int64_t rowkey_column_count, const int64_t column_count = 0,
      const common::ObRowStoreType row_store_type = common::FLAT_ROW_STORE);
  virtual int append_row(const storage::ObStoreRow& row) override;
  virtual int build_block(char*& buf, int64_t& size) override;
  virtual void reuse() override;

  virtual int64_t get_block_size() const override;
  virtual int64_t get_row_count() const override;
  virtual int64_t get_data_size() const override;
  virtual int64_t get_column_count() const override;
  virtual common::ObString get_last_rowkey() const override;
  void reset();

private:
  inline int64_t get_index_size() const;
  int check_input_param(const int64_t macro_block_size, const int64_t column_count, const int64_t rowkey_column_count,
      const ObRowStoreType row_store_type);
  int finish_row(const int64_t length);
  int reserve_header(const int64_t column_count);
  bool is_exceed_limit(const int64_t row_length, const int64_t rowkey_length);

private:
  int64_t micro_block_size_limit_;
  int64_t column_count_;
  ObRowWriter row_writer_;
  int64_t rowkey_column_count_;
  ObMicroBlockHeader* header_;
  ObPosition last_rowkey_pos_;
  ObSelfBufferWriter data_buffer_;
  ObSelfBufferWriter index_buffer_;
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
  return data_buffer_.length();
}
inline int64_t ObMicroBlockWriter::get_column_count() const
{
  return header_->column_count_;
}
inline common::ObString ObMicroBlockWriter::get_last_rowkey() const
{
  common::ObString rowkey(0, last_rowkey_pos_.length_, data_buffer_.data() + last_rowkey_pos_.offset_);
  return rowkey;
}
inline int64_t ObMicroBlockWriter::get_index_size() const
{
  return index_buffer_.length();
}

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
