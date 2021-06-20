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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_ROW_CACHE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_ROW_CACHE_H_
#include "ob_block_sstable_struct.h"
#include "lib/container/ob_vector.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/ob_i_table.h"

namespace oceanbase {
namespace storage {
class ObSSTable;
}
namespace blocksstable {
class ObRowCacheKey : public common::ObIKVCacheKey {
public:
  ObRowCacheKey();
  ObRowCacheKey(const uint64_t table_id, const int64_t file_id, const common::ObStoreRowkey& row_key,
      const int64_t data_version, const storage::ObITable::TableType table_type);
  virtual ~ObRowCacheKey();
  virtual bool operator==(const ObIKVCacheKey& other) const;
  virtual uint64_t get_tenant_id() const;
  virtual uint64_t hash() const;
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const;
  storage::ObITable::TableType get_table_type() const
  {
    return table_type_;
  }
  int64_t get_data_version() const
  {
    return data_version_;
  }
  bool is_valid() const;
  TO_STRING_KV(K_(table_id), K_(file_id), K_(rowkey_size), K_(data_version), K_(table_type), K_(rowkey));

private:
  uint64_t table_id_;
  int64_t file_id_;
  int64_t rowkey_size_;
  int64_t data_version_;
  storage::ObITable::TableType table_type_;
  common::ObStoreRowkey rowkey_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRowCacheKey);
};

class ObRowCacheValue : public common::ObIKVCacheValue {
public:
  ObRowCacheValue();
  virtual ~ObRowCacheValue();
  int init(const ObFullMacroBlockMeta& macro_meta, const storage::ObStoreRow* row, const MacroBlockId& block_id);
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const;
  inline void set_row_not_exist()
  {
    obj_array_ = NULL;
    size_ = 0;
  }
  inline bool is_row_not_exist() const
  {
    return 0 == size_;
  }
  inline int64_t get_buf_size() const
  {
    return size_;
  }
  inline common::ObObj* get_obj_array() const
  {
    return obj_array_;
  }
  inline int64_t get_column_cnt() const
  {
    return column_cnt_;
  }
  inline int64_t get_schema_version() const
  {
    return schema_version_;
  }
  inline int64_t get_flag() const
  {
    return flag_;
  }
  inline MacroBlockId get_block_id() const
  {
    return block_id_;
  }
  inline storage::ObStoreRowDml get_row_dml() const
  {
    return row_dml_;
  }
  inline storage::ObRowDml get_dml() const
  {
    return row_dml_.get_dml();
  }
  inline storage::ObRowDml get_first_dml() const
  {
    return row_dml_.get_first_dml();
  }
  inline bool is_valid() const
  {
    return (NULL == obj_array_ && 0 == size_) || (NULL != obj_array_ && size_ > 0 && block_id_.is_valid());
  }
  inline const uint16_t* get_column_ids() const
  {
    return column_ids_;
  }
  TO_STRING_KV(KP_(obj_array), K_(size), K_(block_id), KP_(column_ids));

private:
  common::ObObj* obj_array_;
  uint16_t* column_ids_;
  int64_t size_;
  int64_t column_cnt_;
  int64_t schema_version_;
  int64_t flag_;
  MacroBlockId block_id_;
  storage::ObStoreRowDml row_dml_;
};

struct ObRowValueHandle {
  ObRowCacheValue* row_value_;
  common::ObKVCacheHandle handle_;
  ObRowValueHandle() : row_value_(NULL), handle_()
  {}
  virtual ~ObRowValueHandle()
  {}
  inline bool is_valid() const
  {
    return NULL != row_value_ && row_value_->is_valid() && handle_.is_valid();
  }
  inline void reset()
  {
    row_value_ = NULL;
    handle_.reset();
  }
  TO_STRING_KV(KP(row_value_), K(handle_));
};

class ObRowCache : public common::ObKVCache<ObRowCacheKey, ObRowCacheValue> {
public:
  ObRowCache();
  virtual ~ObRowCache();
  int get_row(const ObRowCacheKey& key, ObRowValueHandle& handle);
  int put_row(const ObRowCacheKey& key, const ObRowCacheValue& value);
  DISALLOW_COPY_AND_ASSIGN(ObRowCache);
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
