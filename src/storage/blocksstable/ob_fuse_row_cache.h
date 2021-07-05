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

namespace oceanbase {
namespace blocksstable {

class ObFuseRowCacheKey : public common::ObIKVCacheKey {
public:
  ObFuseRowCacheKey();
  ObFuseRowCacheKey(const uint64_t table_id, const ObStoreRowkey& rowkey);
  virtual ~ObFuseRowCacheKey() = default;
  virtual bool operator==(const ObIKVCacheKey& other) const override;
  virtual uint64_t get_tenant_id() const override;
  virtual uint64_t hash() const override;
  virtual int64_t size() const override;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const override;
  bool is_valid() const;
  TO_STRING_KV(K_(table_id), K_(rowkey_size), K_(rowkey));

private:
  uint64_t table_id_;
  int64_t rowkey_size_;
  common::ObStoreRowkey rowkey_;
  DISALLOW_COPY_AND_ASSIGN(ObFuseRowCacheKey);
};

class ObFuseRowCacheValue : public common::ObIKVCacheValue {
public:
  ObFuseRowCacheValue();
  virtual ~ObFuseRowCacheValue() = default;
  int init(const storage::ObStoreRow& row, const int64_t schema_version, const int64_t snapshot_version,
      const int64_t partition_id, const int64_t sstable_end_log_ts, const storage::ObFastQueryContext& fq_ctx);
  virtual int64_t size() const override;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const override;
  bool is_valid() const
  {
    return (nullptr != obj_array_ && 0 != column_cnt_) || (nullptr == obj_array_ && 0 == column_cnt_);
  }
  common::ObObj* get_obj_ptr() const
  {
    return obj_array_;
  }
  int64_t get_obj_cnt() const
  {
    return column_cnt_;
  }
  int64_t get_flag() const
  {
    return flag_;
  }
  int64_t get_snapshot_version() const
  {
    return snapshot_version_;
  }
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  int64_t get_partition_id() const
  {
    return partition_id_;
  }
  void set_snapshot_version(const int64_t snapshot_version);
  int64_t get_sstable_end_log_ts() const
  {
    return sstable_end_log_ts_;
  }
  const storage::ObFastQueryContext* get_fq_ctx() const
  {
    return &fq_ctx_;
  }
  TO_STRING_KV(KP_(obj_array), K_(size), K_(column_cnt), K_(schema_version), K_(flag), K_(snapshot_version),
      K_(partition_id), K_(sstable_end_log_ts), K(fq_ctx_));

private:
  common::ObObj* obj_array_;
  int64_t size_;
  int32_t column_cnt_;
  int32_t flag_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t partition_id_;
  int64_t sstable_end_log_ts_;
  storage::ObFastQueryContext fq_ctx_;
};

struct ObFuseRowValueHandle {
  ObFuseRowValueHandle() : value_(nullptr), handle_()
  {}
  ~ObFuseRowValueHandle() = default;
  bool is_valid() const
  {
    return nullptr != value_ && value_->is_valid() && handle_.is_valid();
  }
  void reset()
  {
    value_ = nullptr;
    handle_.reset();
  }
  TO_STRING_KV(KP_(value), K_(handle));
  ObFuseRowCacheValue* value_;
  common::ObKVCacheHandle handle_;
};

class ObFuseRowCache : public common::ObKVCache<ObFuseRowCacheKey, ObFuseRowCacheValue> {
public:
  ObFuseRowCache() = default;
  virtual ~ObFuseRowCache() = default;
  int get_row(const ObFuseRowCacheKey& key, const int64_t partition_id, ObFuseRowValueHandle& handle);
  int put_row(const ObFuseRowCacheKey& key, const ObFuseRowCacheValue& value);

private:
  DISALLOW_COPY_AND_ASSIGN(ObFuseRowCache);
};

}  // namespace blocksstable
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_FUSE_ROW_CACHE_H_
