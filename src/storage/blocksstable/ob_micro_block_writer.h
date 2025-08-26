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
#include "ob_micro_block_checksum_helper.h"

namespace oceanbase
{
namespace common
{
class ObNewRow;
}
namespace blocksstable
{

template<bool EnableNewFlatFormat>
class ObMicroBufferFlatWriter : public ObMicroBufferWriter
{
public:
  ObMicroBufferFlatWriter() = default;

  virtual ~ObMicroBufferFlatWriter() = default;

  int write_row(const ObDatumRow &row,
                const int64_t rowkey_cnt,
                const ObIArray<ObColDesc> *col_descs,
                int64_t &len);

private:
  typename std::conditional<EnableNewFlatFormat, ObRowWriter, ObRowWriterV0>::type row_writer_;
};

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
template<bool EnableNewFlatFormat = true>
class ObMicroBlockWriter : public ObIMicroBlockWriter
{
  static const int64_t INDEX_ENTRY_SIZE = sizeof(int32_t);
  static const int64_t DEFAULT_DATA_BUFFER_SIZE = common::OB_DEFAULT_MACRO_BLOCK_SIZE;
  static const int64_t DEFAULT_INDEX_BUFFER_SIZE = 4 * 1024;
  static const int64_t MIN_RESERVED_SIZE = 1024; //1KB;
public:
  ObMicroBlockWriter();
  virtual ~ObMicroBlockWriter();
  int init(const ObDataStoreDesc *data_store_desc);

  int init(const int64_t micro_block_size_limit,
           const int64_t rowkey_column_count,
           const int64_t column_count);

  virtual int append_row(const ObDatumRow &row);
  virtual int build_block(char *&buf, int64_t &size);
  virtual void reuse();

  virtual int64_t get_block_size() const override;
  virtual int64_t get_row_count() const override;
  virtual int64_t get_column_count() const override;
  virtual int64_t get_original_size() const override;
  virtual int append_hash_index(ObMicroBlockHashIndexBuilder& hash_index_builder);
  void reset();
private:
  int inner_init();
  inline int64_t get_index_size() const;
  inline int64_t get_data_size() const;
  inline int64_t get_future_block_size() const;
  int try_to_append_row();
  int finish_row();
  int reserve_header(
      const int64_t column_count,
      const int64_t rowkey_column_count,
      const bool need_calc_column_chksum);
  bool is_exceed_limit();
  int64_t get_data_base_offset() const;
  int64_t get_index_base_offset() const;
  int process_out_row_columns(const ObDatumRow &row);

  int init_hash_index_builder(const ObDataStoreDesc *data_store_desc = nullptr);

  int append_row_to_hash_index(const ObDatumRow &row);

  int build_hash_index_block();

private:
  int64_t micro_block_size_limit_;
  int64_t column_count_;
  int64_t rowkey_column_count_;
  const common::ObIArray<share::schema::ObColDesc> *col_desc_array_;
  int64_t row_count_;
  ObMicroBufferFlatWriter<EnableNewFlatFormat> data_buffer_;
  ObMicroBufferWriter index_buffer_;
  ObMicroBlockHashIndexBuilder hash_index_builder_;
  bool reuse_hash_index_builder_;
  bool is_major_;
  bool is_inited_;
};


template<bool EnableNewFlatFormat>
inline int64_t ObMicroBlockWriter<EnableNewFlatFormat>::get_block_size() const
{
  return get_data_size() + get_index_size();
}

template<bool EnableNewFlatFormat>
inline int64_t ObMicroBlockWriter<EnableNewFlatFormat>::get_row_count() const
{
  return row_count_;
}

template<bool EnableNewFlatFormat>
inline int64_t ObMicroBlockWriter<EnableNewFlatFormat>::get_data_size() const
{
  int64_t data_size = data_buffer_.length();
  if (data_size == 0) { // lazy allocate
    data_size = get_data_base_offset();
  }
  return data_size;
}

template<bool EnableNewFlatFormat>
inline int64_t ObMicroBlockWriter<EnableNewFlatFormat>::get_column_count() const
{
  return column_count_;
}

template<bool EnableNewFlatFormat>
inline int64_t ObMicroBlockWriter<EnableNewFlatFormat>::get_index_size() const
{
  int64_t index_size = index_buffer_.length();
  if (index_size == 0) { // lazy allocate
    index_size = get_index_base_offset();
  }
  return index_size;
}

template<bool EnableNewFlatFormat>
inline int64_t ObMicroBlockWriter<EnableNewFlatFormat>::get_future_block_size() const
{
  int64_t hash_index_size = 0;
  if (hash_index_builder_.is_valid()) {
    hash_index_size = hash_index_builder_.estimate_size(/* plus_one = */ true);
  }
  return get_data_size() + get_index_size() + INDEX_ENTRY_SIZE + hash_index_size;
}

template<bool EnableNewFlatFormat>
inline int64_t ObMicroBlockWriter<EnableNewFlatFormat>::get_data_base_offset() const
{
  return ObMicroBlockHeader::get_serialize_size(column_count_, is_major_);
}

template<bool EnableNewFlatFormat>
inline int64_t ObMicroBlockWriter<EnableNewFlatFormat>::get_index_base_offset() const
{
  return sizeof(int32_t);
}

template<bool EnableNewFlatFormat>
inline int64_t ObMicroBlockWriter<EnableNewFlatFormat>::get_original_size() const
{
  int64_t original_size = 0;
  if (data_buffer_.length() > 0) {
    original_size = data_buffer_.length() - ObMicroBlockHeader::get_serialize_size(column_count_, is_major_);
  }
  return original_size;
}

}//end namespace blocksstable
}//end namespace oceanbase
#endif
