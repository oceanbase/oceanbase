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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_ROW_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_ROW_WRITER_H_
#include "share/ob_define.h"
#include "storage/ob_i_store.h"
#include "common/object/ob_obj_type.h"

namespace oceanbase {
namespace common {
class ObOTimestampData;
class ObObj;
class ObNewRow;
}  // namespace common
namespace storage {
class ObStoreRow;
}
namespace blocksstable {
class ObRowHeader;

class ObRowWriter {
public:
  ObRowWriter();
  virtual ~ObRowWriter();
  int write(const common::ObNewRow& row, char* buf, const int64_t buf_size, const common::ObRowStoreType row_store_type,
      int64_t& pos);
  int write(const int64_t rowkey_column_count, const storage::ObStoreRow& row, char* buf, const int64_t buf_size,
      int64_t& pos, int64_t& rowkey_start_pos, int64_t& rowkey_end_pos, const bool only_row_key = false);
  int write_flat_row(const int64_t rowkey_column_count, const storage::ObStoreRow& row, char* buf,
      const int64_t buf_size, int64_t& pos, int64_t& rowkey_start_pos, int64_t& rowkey_end_pos,
      const bool only_row_key = false);
  int write_sparse_row(const int64_t rowkey_column_count, const storage::ObStoreRow& row, char* buf,
      const int64_t buf_size, int64_t& pos, int64_t& rowkey_start_pos, int64_t& rowkey_end_pos,
      const bool only_row_key = false);

private:
  struct NumberAllocator {
    char* buf_;
    int64_t buf_size_;
    int64_t& pos_;
    NumberAllocator(char* buf, const int64_t buf_size, int64_t& pos);
    char* alloc(const int64_t size)
    {
      char* ret = NULL;
      if (buf_size_ >= pos_ + size) {
        ret = buf_ + pos_;
        pos_ += size;
      }
      return ret;
    }
    char* alloc(const int64_t size, const lib::ObMemAttr& attr)
    {
      UNUSED(attr);
      return alloc(size);
    }
  };

private:
  inline int write_oracle_timestamp(
      const common::ObOTimestampData& ot_data, const common::ObOTimestampMetaAttrType otmat);
  int write_text_store(const common::ObObj& obj);
  int append_column(const common::ObObj& obj);
  int init_common(char* buf, const int64_t buf_size, const int64_t pos);
  int init_store_row(const storage::ObStoreRow& row, const int64_t rowkey_column_count);
  int append_row_header(const storage::ObStoreRow& row);
  int append_store_row(const int64_t rowkey_column_count, const storage::ObStoreRow& row, const bool only_row_key,
      int64_t& rowkey_start_pos, int64_t& rowkey_length);
  int append_sparse_store_row(const int64_t rowkey_column_count, const storage::ObStoreRow& row,
      const bool only_row_key, int64_t& rowkey_start_pos, int64_t& rowkey_length);
  int append_new_row(const common::ObNewRow& row);
  int append_sparse_new_row(const common::ObNewRow& row);
  int append_column_index();
  int append_column_ids(const storage::ObStoreRow& row);
  template <class T>
  OB_INLINE int append(const T& value);
  OB_INLINE int get_int_byte(const int64_t int_value, int64_t& bytes) const;
  OB_INLINE int write_int(const int64_t value);
  OB_INLINE int write_number(const common::number::ObNumber& number);
  OB_INLINE int write_char(const common::ObObj& obj, const storage::ObDataStoreType& meta_type,
      const common::ObString& char_value, const int64_t max_length);

private:
  char* buf_;
  int64_t buf_size_;
  int64_t start_pos_;
  int64_t pos_;
  ObRowHeader* row_header_;
  int64_t column_index_count_;
  common::number::ObNumber tmp_number_;
  uint16_t column_ids_[common::OB_ROW_MAX_COLUMNS_COUNT];  // for sparse row
  int8_t column_indexs_8_[common::OB_ROW_MAX_COLUMNS_COUNT];
  int16_t column_indexs_16_[common::OB_ROW_MAX_COLUMNS_COUNT];
  int32_t column_indexs_32_[common::OB_ROW_MAX_COLUMNS_COUNT];
  DISALLOW_COPY_AND_ASSIGN(ObRowWriter);
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
