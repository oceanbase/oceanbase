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

#ifndef OB_STORAGE_ACCESS_TABLE_READ_INFO_H_
#define OB_STORAGE_ACCESS_TABLE_READ_INFO_H_

#include "lib/container/ob_fixed_array.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObColumnParam;
class ObColDesc;
}
}
using namespace share::schema;
namespace storage {
class ObStorageSchema;

typedef common::ObFixedArray<ObColumnParam *, common::ObIAllocator> Columns;
typedef common::ObFixedArray<int32_t, common::ObIAllocator> ColumnsIndex;
typedef common::ObFixedArray<ObColDesc, common::ObIAllocator> ColDescArray;

class ObTableReadInfo
{
public:
  ObTableReadInfo()
    : allocator_(nullptr),
      schema_column_count_(0),
      schema_rowkey_cnt_(0),
      rowkey_cnt_(0),
      trans_col_index_(OB_INVALID_INDEX),
      group_idx_col_index_(OB_INVALID_INDEX),
      seq_read_column_count_(0),
      max_col_index_(-1),
      is_oracle_mode_(false),
      cols_param_(),
      cols_desc_(),
      cols_index_(),
      memtable_cols_index_(),
      datum_utils_(),
      index_read_info_(nullptr)
  {}
  virtual ~ObTableReadInfo();
  void reset();
  /*
   * schema_rowkey_cnt: schema row key count
   * cols_desc: access col descs
   * is_multi_version_full: input is full multi version column descs, extra rowkeys included
   * storage_cols_index: access column store index in storage file row
   * cols_param: access column params
   * index_read_info: pointer to index block read info if this is a full read info
   */
  int init(
      common::ObIAllocator &allocator,
      const int64_t schema_column_count,
      const int64_t schema_rowkey_cnt,
      const bool is_oracle_mode,
      const common::ObIArray<ObColDesc> &cols_desc,
      const bool is_multi_version_full = false,
      const common::ObIArray<int32_t> *storage_cols_index = nullptr,
      const common::ObIArray<ObColumnParam *> *cols_param = nullptr,
      const bool is_index_read_info = false);
  bool is_valid() const
  {
    return schema_rowkey_cnt_ <= seq_read_column_count_
        && seq_read_column_count_ <= cols_desc_.count()
        && 0 < cols_desc_.count()
        && 0 < schema_column_count_
        && datum_utils_.is_valid()
        && cols_desc_.count() == cols_index_.count();
  }
  bool is_valid_full_read_info() const
  {
    return is_valid()
        && nullptr != index_read_info_
        && index_read_info_->is_valid();
  }
  OB_INLINE bool is_oracle_mode() const { return is_oracle_mode_; }
  OB_INLINE int64_t get_schema_column_count() const
  { return schema_column_count_; }
  OB_INLINE int64_t get_trans_col_index() const
  { return trans_col_index_; }
  OB_INLINE int64_t get_group_idx_col_index() const
  { return group_idx_col_index_; }
  OB_INLINE int64_t get_schema_rowkey_count() const
  { return schema_rowkey_cnt_; }
  OB_INLINE int64_t get_rowkey_count() const
  { return rowkey_cnt_; }
  OB_INLINE int64_t get_seq_read_column_count() const
  { return seq_read_column_count_; }
  OB_INLINE const common::ObIArray<ObColumnParam *> &get_columns() const
  { return cols_param_; }
  OB_INLINE const common::ObIArray<ObColDesc> &get_columns_desc() const
  { return cols_desc_; }
  OB_INLINE int64_t get_request_count() const
  { return cols_desc_.count(); }
  OB_INLINE const common::ObIArray<int32_t> &get_columns_index() const
  { return cols_index_; }
  OB_INLINE const common::ObIArray<int32_t> &get_memtable_columns_index() const
  { return memtable_cols_index_; }
  OB_INLINE const blocksstable::ObStorageDatumUtils &get_datum_utils() const { return datum_utils_; }
  OB_INLINE const ObTableReadInfo *get_index_read_info() const { return index_read_info_; }
  OB_INLINE int64_t get_max_col_index() const { return max_col_index_; }
  int assign(common::ObIAllocator &allocator, const ObTableReadInfo &read_info);
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int serialize(
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const;
  int64_t get_serialize_size() const;

  DECLARE_TO_STRING;

private:
  int build_index_read_info(
      common::ObIAllocator &allocator,
      const int64_t schema_rowkey_cnt,
      const bool is_oracle_mode,
      const common::ObIArray<ObColDesc> &cols_desc);
  DISALLOW_COPY_AND_ASSIGN(ObTableReadInfo);

private:
  ObIAllocator *allocator_;
  // distinguish schema changed by schema column count
  int64_t schema_column_count_;
  int64_t schema_rowkey_cnt_;
  int64_t rowkey_cnt_;
  int64_t trans_col_index_;
  int64_t group_idx_col_index_;
  // the count of common prefix between request columns and store columns
  int64_t seq_read_column_count_;
  int64_t max_col_index_;
  bool is_oracle_mode_;
  Columns cols_param_;
  ColDescArray cols_desc_; // used in storage layer, won't serialize
  ColumnsIndex cols_index_; // there is no multi verison rowkey col in memtable, we need another col idx array
  ColumnsIndex memtable_cols_index_;
  blocksstable::ObStorageDatumUtils datum_utils_;
  ObTableReadInfo *index_read_info_; // no need to serialize
};

}
}
#endif //OB_STORAGE_ACCESS_TABLE_READ_INFO_H_
