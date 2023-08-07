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

#include "storage/meta_mem/ob_fixed_meta_obj_array.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObColumnParam;
class ObColDesc;
}
}
namespace blocksstable{
struct ObDataStoreDesc;
}
using namespace share::schema;
namespace storage {
class ObStorageSchema;

typedef ObFixedMetaObjArray<ObColumnParam *> Columns;
typedef ObFixedMetaObjArray<int32_t> ColumnsIndex;
typedef ObFixedMetaObjArray<ObColDesc> ColDescArray;
/*
  ObITableReadInfo: basic interface
  --ObReadInfoStruct: basic struct, implement get* func
  ----ObTableReadInfo:  for access, may not get all column
  ----ObRowkeyReadInfo: for compaction, get all column | for mid-index, get rowkey
*/
struct ObColumnIndexArray
{
public:
  ObColumnIndexArray(const bool rowkey_mode = false, const bool for_memtable = false)
      : version_(COLUMN_INDEX_ARRAY_VERSION),
        rowkey_mode_(rowkey_mode),
        for_memtable_(for_memtable),
        reserved_(0),
        schema_rowkey_cnt_(0),
        column_cnt_(0),
        array_() {}
  ~ObColumnIndexArray() { reset(); }
  void reset()
  {
    schema_rowkey_cnt_ = 0;
    column_cnt_ = 0;
    array_.reset();
  }
  bool is_valid() const
  {
    return (rowkey_mode_ && column_cnt_ > schema_rowkey_cnt_ && schema_rowkey_cnt_ > 0)
      || (!rowkey_mode_ && array_.count() > 0);
  }
  int init(
    const int64_t count,
    const int64_t schema_rowkey_cnt,
    ObIAllocator &allocator);

  int32_t at(int64_t idx) const;
  OB_INLINE int64_t count() const
  {
    int64_t ret_count = 0;
    if (rowkey_mode_) {
      ret_count = column_cnt_;
    } else {
      ret_count = array_.count();
    }
    return ret_count;
  }

  int64_t get_deep_copy_size() const;
  int deep_copy(
    char *dst_buf,
    const int64_t buf_size,
    int64_t &pos,
    ObColumnIndexArray &dst_array) const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos, common::ObIAllocator &allocator);
  int64_t get_serialize_size() const;
  DECLARE_TO_STRING;

  const static uint8_t COLUMN_INDEX_ARRAY_VERSION = 1;
  uint8_t version_;
  bool rowkey_mode_;
  bool for_memtable_;
  uint8_t reserved_;
  uint32_t schema_rowkey_cnt_;
  uint32_t column_cnt_;
  ObFixedMetaObjArray<int32_t> array_;
};
class ObITableReadInfo
{
public:
  ObITableReadInfo() = default;
  virtual ~ObITableReadInfo() = default;
  virtual bool is_oracle_mode() const = 0;
  virtual int64_t get_schema_column_count() const = 0;
  virtual int64_t get_seq_read_column_count() const = 0;
  virtual int64_t get_request_count() const = 0;
  virtual int64_t get_schema_rowkey_count() const = 0;
  virtual int64_t get_rowkey_count() const = 0;
  virtual int64_t get_group_idx_col_index() const = 0;
  virtual int64_t get_max_col_index() const = 0;
  virtual int64_t get_trans_col_index() const = 0;
  virtual const common::ObIArray<ObColDesc> &get_columns_desc() const = 0;
  virtual const ObColumnIndexArray &get_columns_index() const = 0;
  virtual const ObColumnIndexArray &get_memtable_columns_index() const = 0;
  virtual const blocksstable::ObStorageDatumUtils &get_datum_utils() const = 0;
  virtual const common::ObIArray<ObColumnParam *> *get_columns() const = 0;
  virtual bool is_valid() const = 0;
  virtual void reset() = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObReadInfoStruct : public ObITableReadInfo
{
public:
  ObReadInfoStruct(const bool rowkey_mode = false)
    : ObITableReadInfo(),
      is_inited_(false),
      is_oracle_mode_(false),
      allocator_(nullptr),
      schema_column_count_(0),
      compat_version_(READ_INFO_VERSION_V1),
      reserved_(0),
      schema_rowkey_cnt_(0),
      rowkey_cnt_(0),
      cols_desc_(),
      cols_index_(rowkey_mode, false/*for_memtable*/),
      memtable_cols_index_(rowkey_mode, true/*for_memtable*/),
      datum_utils_()
  {}
  virtual ~ObReadInfoStruct() { reset(); }
  virtual bool is_valid() const override
  {
    return is_inited_
        && schema_rowkey_cnt_ <= cols_desc_.count()
        && 0 < cols_desc_.count()
        && 0 < cols_index_.count()
        && schema_rowkey_cnt_ <= schema_column_count_
        && datum_utils_.is_valid();
  }
  virtual void reset() override;

  OB_INLINE virtual bool is_oracle_mode() const override { return is_oracle_mode_; }
  OB_INLINE virtual int64_t get_schema_column_count() const override { return schema_column_count_; }
  OB_INLINE virtual int64_t get_schema_rowkey_count() const override { return schema_rowkey_cnt_; }
  OB_INLINE virtual int64_t get_rowkey_count() const override { return rowkey_cnt_; }
  OB_INLINE virtual const common::ObIArray<ObColDesc> &get_columns_desc() const override
  { return cols_desc_; }
  OB_INLINE virtual int64_t get_request_count() const override
  { return cols_desc_.count(); }
  OB_INLINE virtual const ObColumnIndexArray &get_columns_index() const override
  { return cols_index_; }
  OB_INLINE virtual const ObColumnIndexArray &get_memtable_columns_index() const override
  { return memtable_cols_index_; }
  OB_INLINE virtual const blocksstable::ObStorageDatumUtils &get_datum_utils() const override { return datum_utils_; }
  OB_INLINE virtual int64_t get_group_idx_col_index() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise group idx col index");
    return OB_INVALID_INDEX;
  }
  OB_INLINE virtual int64_t get_max_col_index() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise max col index");
    return OB_INVALID_INDEX;
  }
  OB_INLINE virtual int64_t get_trans_col_index() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise trans col index");
    return OB_INVALID_INDEX;
  }
  OB_INLINE virtual int64_t get_seq_read_column_count() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise seq read column count");
    return OB_INVALID_INDEX;
  }
  OB_INLINE virtual const common::ObIArray<ObColumnParam *> *get_columns() const override
  {
    OB_ASSERT_MSG(false, "ObReadInfoStruct dose not promise columns array");
    return nullptr;
  }
  DECLARE_TO_STRING;
  void init_basic_info(const int64_t schema_column_count,
                       const int64_t schema_rowkey_cnt,
                       const bool is_oracle_mode);
  int prepare_arrays(common::ObIAllocator &allocator,
                     const common::ObIArray<ObColDesc> &cols_desc,
                     const int64_t col_cnt);
  int init_compat_version();
protected:
  const int64_t READ_INFO_VERSION_V0 = 0;
  const int64_t READ_INFO_VERSION_V1 = 1;
  bool is_inited_;
  bool is_oracle_mode_;
  ObIAllocator *allocator_;
  // distinguish schema changed by schema column count
  union {
    uint64_t info_;
    struct {
      uint32_t schema_column_count_;
      uint16_t compat_version_;
      uint16_t reserved_;
    };
  };
  int64_t schema_rowkey_cnt_;
  int64_t rowkey_cnt_;
  ColDescArray cols_desc_; // used in storage layer, won't serialize
  ObColumnIndexArray cols_index_; // col index in sstable
  ObColumnIndexArray memtable_cols_index_; // there is no multi verison rowkey col in memtable
  blocksstable::ObStorageDatumUtils datum_utils_;
};

class ObTableReadInfo : public ObReadInfoStruct
{
public:
  ObTableReadInfo();
  virtual ~ObTableReadInfo();
  virtual void reset() override;
  /*
   * schema_rowkey_cnt: schema row key count
   * cols_desc: access col descs
   * storage_cols_index: access column store index in storage file row
   * cols_param: access column params
   */
  virtual int init(
      common::ObIAllocator &allocator,
      const int64_t schema_column_count,
      const int64_t schema_rowkey_cnt,
      const bool is_oracle_mode,
      const common::ObIArray<ObColDesc> &cols_desc,
      const common::ObIArray<int32_t> *storage_cols_index,
      const common::ObIArray<ObColumnParam *> *cols_param = nullptr);
  virtual OB_INLINE bool is_valid() const override
  {
    return ObReadInfoStruct::is_valid()
        && cols_desc_.count() == cols_index_.count()
        && schema_rowkey_cnt_ <= seq_read_column_count_
        && seq_read_column_count_ <= cols_desc_.count();
  }
  OB_INLINE virtual int64_t get_trans_col_index() const override
  { return trans_col_index_; }
  OB_INLINE int64_t get_group_idx_col_index() const
  { return group_idx_col_index_; }
  OB_INLINE int64_t get_seq_read_column_count() const
  { return seq_read_column_count_; }
  virtual const common::ObIArray<ObColumnParam *> *get_columns() const
  { return &cols_param_; }

  // this func only called in query
  OB_INLINE virtual int64_t get_request_count() const override
  { return cols_desc_.count(); }
  OB_INLINE virtual int64_t get_max_col_index() const override { return max_col_index_; }
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
  DISALLOW_COPY_AND_ASSIGN(ObTableReadInfo);
  int init_datum_utils(common::ObIAllocator &allocator);

private:
  // distinguish schema changed by schema column count
  int64_t trans_col_index_;
  int64_t group_idx_col_index_;
  // the count of common prefix between request columns and store columns
  int64_t seq_read_column_count_;
  int64_t max_col_index_;
  Columns cols_param_;
};

class ObRowkeyReadInfo final : public ObReadInfoStruct
{
public:
  ObRowkeyReadInfo();
  virtual ~ObRowkeyReadInfo() {}

  int init(
      common::ObIAllocator &allocator,
      const int64_t schema_column_count,
      const int64_t schema_rowkey_cnt,
      const bool is_oracle_mode,
      const common::ObIArray<ObColDesc> &rowkey_col_descs);
  int init(common::ObIAllocator &allocator,
           const blocksstable::ObDataStoreDesc &data_store_desc);

  OB_INLINE virtual int64_t get_seq_read_column_count() const override
  { return get_request_count(); }
  OB_INLINE virtual int64_t get_trans_col_index() const override
  { return schema_rowkey_cnt_; }
  virtual int64_t get_request_count() const override;
  int deep_copy(char *buf, const int64_t buf_len, ObRowkeyReadInfo *&value) const;
  int64_t get_deep_copy_size() const;
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
  DISALLOW_COPY_AND_ASSIGN(ObRowkeyReadInfo);
};
}
}
#endif //OB_STORAGE_ACCESS_TABLE_READ_INFO_H_
