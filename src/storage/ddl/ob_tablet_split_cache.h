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

#ifndef OCEANBASE_STORAGE_DDL_TABLET_SPLIT_CACHE_H_
#define OCEANBASE_STORAGE_DDL_TABLET_SPLIT_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/ob_storage_struct.h"

namespace oceanbase
{
namespace storage
{
class ObTabletSplitCacheKey : public common::ObIKVCacheKey
{
public:
  //only used in deep copy
  ObTabletSplitCacheKey();
  //read / put should always use this constructor
  ObTabletSplitCacheKey(const uint64_t tenant_id,
                const ObTabletID &table_id,
                const uint8_t bucket_id);
  virtual ~ObTabletSplitCacheKey() = default;
  virtual int equal(const ObIKVCacheKey &other, bool &equal) const override;
  virtual int hash(uint64_t &hash_value) const override;
  virtual uint64_t get_tenant_id() const override { return tenant_id_; }
  virtual int64_t size() const override { return sizeof(*this); }
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const;
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(tablet_id), K_(bucket_id));
private:
  int64_t tenant_id_;
  ObTabletID tablet_id_;
  uint8_t bucket_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitCacheKey);
};

class ObTabletSplitCacheValue : public common::ObIKVCacheValue
{
public:
  ObTabletSplitCacheValue();
  virtual ~ObTabletSplitCacheValue();
  int init(const ObTabletSplitTscInfo &split_info);
  int deep_copy(ObTabletSplitTscInfo &split_info, ObIAllocator &allocator) const;
  bool is_valid() const;
  virtual int64_t size() const override { return sizeof(*this) + deep_copy_size_; };
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const;
  TO_STRING_KV(K_(deep_copy_size), K_(split_cnt), K_(split_type), K_(partkey_is_rowkey_prefix), K_(start_partkey), K_(end_partkey));
private:
  int64_t deep_copy_size_;
  int64_t split_cnt_;
  ObTabletSplitType split_type_;
  bool partkey_is_rowkey_prefix_;
  blocksstable::ObDatumRowkey start_partkey_;
  blocksstable::ObDatumRowkey end_partkey_;
};

struct ObTabletSplitCacheValueHandle
{
  ObTabletSplitCacheValue *row_value_;
  common::ObKVCacheHandle handle_;
  ObTabletSplitCacheValueHandle() : row_value_(NULL), handle_() {}
  void move_from(ObTabletSplitCacheValueHandle &other)
  {
    this->row_value_ = other.row_value_;
    this->handle_.move_from(other.handle_);
    other.reset();
  }
  int assign(const ObTabletSplitCacheValueHandle &other)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(this->handle_.assign(other.handle_))) {
      COMMON_LOG(WARN, "failed to assign handle", K(ret));
      this->row_value_ = nullptr;
    } else {
      this->row_value_ = other.row_value_;
    }
    return ret;
  }
  virtual ~ObTabletSplitCacheValueHandle() {}
  inline bool is_valid() const { return NULL != row_value_ && row_value_->is_valid() && handle_.is_valid(); }
  inline void reset() { row_value_ = NULL; handle_.reset(); }
  TO_STRING_KV(KP(row_value_), K(handle_));
};

class ObTabletSplitCache : public common::ObKVCache<ObTabletSplitCacheKey, ObTabletSplitCacheValue>
{
public:
  ObTabletSplitCache();
  virtual ~ObTabletSplitCache();
  int get_split_cache(const ObTabletSplitCacheKey &key, ObTabletSplitCacheValueHandle &handle);
  int put_split_cache(const ObTabletSplitCacheKey &key, const ObTabletSplitCacheValue &value);
  DISALLOW_COPY_AND_ASSIGN(ObTabletSplitCache);
};

}//end namespace storage
}//end namespace oceanbase
#endif

