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
#include "storage/ob_i_table.h"
#include "ob_datum_rowkey.h"

namespace oceanbase
{
namespace blocksstable
{
class ObRowCacheKey : public common::ObIKVCacheKey
{
public:
  //only used in deep copy
  ObRowCacheKey();
  //read / put should always use this constructor
  ObRowCacheKey(const uint64_t tenant_id,
                const ObTabletID &table_id,
                const ObDatumRowkey &row_key,
                const ObStorageDatumUtils &datum_utils,
                const int64_t data_version,
                const storage::ObITable::TableType table_type);
  virtual ~ObRowCacheKey();
  virtual int equal(const ObIKVCacheKey &other, bool &equal) const override;
  virtual int hash(uint64_t &hash_value) const override;
  virtual uint64_t get_tenant_id() const;
  virtual int64_t size() const;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const;
  storage::ObITable::TableType get_table_type() const {return table_type_;}
  int64_t get_data_version() const {return data_version_;}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(tablet_id), K_(data_version), K_(table_type), K_(rowkey), KPC_(datum_utils));
private:
  int64_t rowkey_size_;
  int64_t tenant_id_;
  ObTabletID tablet_id_;
  int64_t data_version_;
  storage::ObITable::TableType table_type_;
  ObDatumRowkey rowkey_;
  const ObStorageDatumUtils *datum_utils_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRowCacheKey);
};

class ObRowCacheValue : public common::ObIKVCacheValue
{
public:
  ObRowCacheValue();
  virtual ~ObRowCacheValue();
  int init(const int64_t start_log_ts,
           const ObDatumRow &row);
  virtual int64_t size() const;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const;
  inline void set_row_not_exist() { datums_ = nullptr; size_ = 0; }
  inline bool is_row_not_exist() const { return 0 == size_; }
  inline int64_t get_buf_size() const { return size_; }
  inline ObStorageDatum *get_datums() const { return datums_; }
  inline int64_t get_column_cnt() const { return column_cnt_; }
  inline int64_t get_start_log_ts() const { return start_log_ts_; }
  inline ObDmlRowFlag get_flag() const { return flag_; }
  inline MacroBlockId get_block_id() const { return block_id_; }
  inline bool is_valid() const { return (NULL == datums_ && 0 == size_) || (NULL != datums_ && size_ > 0); }
  TO_STRING_KV(KP_(datums), K_(size), K_(flag), K_(size), K_(column_cnt), K_(start_log_ts), K_(block_id));
private:
  ObStorageDatum *datums_;
  ObDmlRowFlag flag_;
  int64_t size_;
  int64_t column_cnt_;
  int64_t start_log_ts_;
  MacroBlockId block_id_;
};


struct ObRowValueHandle
{
  ObRowCacheValue *row_value_;
  common::ObKVCacheHandle handle_;
  ObRowValueHandle() : row_value_(NULL), handle_() {}
  virtual ~ObRowValueHandle() {}
  inline bool is_valid() const { return NULL != row_value_ && row_value_->is_valid() && handle_.is_valid(); }
  inline void reset() { row_value_ = NULL; handle_.reset(); }
  TO_STRING_KV(KP(row_value_), K(handle_));
};

class ObRowCache : public common::ObKVCache<ObRowCacheKey, ObRowCacheValue>
{
public:
  ObRowCache();
  virtual ~ObRowCache();
  int get_row(const ObRowCacheKey &key, ObRowValueHandle &handle);
  int put_row(const ObRowCacheKey &key, const ObRowCacheValue &value);
  DISALLOW_COPY_AND_ASSIGN(ObRowCache);
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif
