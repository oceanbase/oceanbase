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

#ifndef OCEANBASE_STORAGE_FUSE_ROW_CACHE_H_
#define OCEANBASE_STORAGE_FUSE_ROW_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "storage/ob_i_store.h"
#include "ob_datum_rowkey.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObFuseRowCacheKeyBase final
{
public:
  ObFuseRowCacheKeyBase();
  ObFuseRowCacheKeyBase(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      const ObDatumRowkey &rowkey,
      const int64_t schema_column_count,
      const ObStorageDatumUtils &datum_utils);
  int hash(uint64_t &hash_value) const;
  int equal(const ObFuseRowCacheKeyBase &other, bool &equal) const;
  int deep_copy(char *buf, const int64_t buf_len, ObFuseRowCacheKeyBase &dest) const;
  OB_INLINE bool is_valid() const
  {
    return OB_LIKELY(tenant_id_ != 0 && tablet_id_.is_valid() && rowkey_size_ > 0 && rowkey_.is_valid() && schema_column_count_ >= 0);
  }
  OB_INLINE int64_t rowkey_size() const { return rowkey_size_; }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  TO_STRING_KV(K_(tenant_id), K_(tablet_id), K_(rowkey_size), K_(rowkey), K_(schema_column_count), KPC_(datum_utils));
private:
  uint64_t tenant_id_;
  ObTabletID tablet_id_;
  int64_t rowkey_size_;
  ObDatumRowkey rowkey_;
  int64_t schema_column_count_;
  const ObStorageDatumUtils *datum_utils_;
  DISALLOW_COPY_AND_ASSIGN(ObFuseRowCacheKeyBase);
};

class ObFuseRowCacheKey : public common::ObIKVCacheKey
{
public:
  ObFuseRowCacheKey();
  ObFuseRowCacheKey(
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      const ObDatumRowkey &rowkey,
      const int64_t schema_column_count,
      const ObStorageDatumUtils &datum_utils);
  virtual ~ObFuseRowCacheKey() = default;
  virtual int equal(const ObIKVCacheKey &other, bool &equal) const override;
  virtual int hash(uint64_t &hash_value) const override;
  virtual uint64_t get_tenant_id() const override;
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  bool is_valid() const;
  TO_STRING_KV(K_(base));
private:
  ObFuseRowCacheKeyBase base_;
  DISALLOW_COPY_AND_ASSIGN(ObFuseRowCacheKey);
};

class ObFuseRowCacheValue : public common::ObIKVCacheValue
{
public:
  ObFuseRowCacheValue();
  virtual ~ObFuseRowCacheValue() = default;
  int init(const blocksstable::ObDatumRow &row, const int64_t read_snapshot_version);
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  bool is_valid() const { return (nullptr != datums_ && 0 != column_cnt_) || (nullptr == datums_ && 0 == column_cnt_); }
  OB_INLINE ObStorageDatum *get_datums() const { return datums_; }
  OB_INLINE int64_t get_column_cnt() const { return column_cnt_; }
  OB_INLINE int64_t get_read_snapshot_version() const { return read_snapshot_version_; }
  ObDmlRowFlag get_flag() const { return flag_; }
  TO_STRING_KV(KP_(datums), K_(size), K_(column_cnt), K_(read_snapshot_version), K_(flag));
private:
  ObStorageDatum *datums_;
  int64_t size_;
  int32_t column_cnt_;
  int64_t read_snapshot_version_;
  ObDmlRowFlag flag_;
};

struct ObFuseRowValueHandle
{
  ObFuseRowValueHandle()
    : value_(nullptr), handle_()
  {}
  ~ObFuseRowValueHandle() = default;
  bool is_valid() const { return nullptr != value_ && value_->is_valid() && handle_.is_valid(); }
  void reset()
  {
    value_ = nullptr;
    handle_.reset();
  }
  TO_STRING_KV(KP_(value), K_(handle));
  ObFuseRowCacheValue *value_;
  common::ObKVCacheHandle handle_;
};

class ObFuseRowCache : public common::ObKVCache<ObFuseRowCacheKey, ObFuseRowCacheValue>
{
public:
  ObFuseRowCache() = default;
  virtual ~ObFuseRowCache() = default;
  int get_row(const ObFuseRowCacheKey &key, ObFuseRowValueHandle &handle);
  int put_row(const ObFuseRowCacheKey &key, const ObFuseRowCacheValue &value);
private:
  DISALLOW_COPY_AND_ASSIGN(ObFuseRowCache);
};

class ObMultiVersionFuseRowCacheKey : public common::ObIKVCacheKey
{
public:
  ObMultiVersionFuseRowCacheKey();
  ObMultiVersionFuseRowCacheKey(
      const int64_t begin_version,
      const int64_t end_version,
      const uint64_t tenant_id,
      const ObTabletID &tablet_id,
      const ObDatumRowkey &rowkey,
      const int64_t schema_column_count,
      const ObStorageDatumUtils &datum_utils);
  virtual ~ObMultiVersionFuseRowCacheKey() = default;
  virtual int equal(const ObIKVCacheKey &other, bool &equal) const override;
  virtual int hash(uint64_t &hash_value) const override;
  virtual uint64_t get_tenant_id() const override;
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;
  bool is_valid() const;
  TO_STRING_KV(K_(base), K_(begin_version), K_(end_version));
private:
  ObFuseRowCacheKeyBase base_;
  // (begin_version_, end_version]
  int64_t begin_version_;
  int64_t end_version_;
  DISALLOW_COPY_AND_ASSIGN(ObMultiVersionFuseRowCacheKey);
};

class ObMultiVersionFuseRowCache : public common::ObKVCache<ObMultiVersionFuseRowCacheKey, ObFuseRowCacheValue>
{
public:
  ObMultiVersionFuseRowCache() = default;
  virtual ~ObMultiVersionFuseRowCache() = default;
  int get_row(const ObMultiVersionFuseRowCacheKey &key, ObFuseRowValueHandle &handle);
  int put_row(const ObMultiVersionFuseRowCacheKey &key, const ObFuseRowCacheValue &value);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMultiVersionFuseRowCache);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_FUSE_ROW_CACHE_H_
