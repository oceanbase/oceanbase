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

#pragma once

#include "share/cache/ob_kv_storecache.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_tx_data_define.h"

#define OB_TX_DATA_KV_CACHE oceanbase::storage::ObTxDataKVCache::get_instance()

namespace oceanbase {
namespace storage {

#define DELETE_COPY_CONSTRUCTOR(TypeName) \
  TypeName(const TypeName &) = delete;    \
  TypeName &operator=(const TypeName &) = delete;

class ObTxDataCacheKey : public common::ObIKVCacheKey {
public:
  ObTxDataCacheKey() : tenant_id_(0), ls_id_(0), tx_id_(0) {}
  ObTxDataCacheKey(int64_t tenant_id, share::ObLSID ls_id, transaction::ObTransID tx_id)
      : tenant_id_(tenant_id), ls_id_(ls_id), tx_id_(tx_id) {}
  ~ObTxDataCacheKey() {}

  bool is_valid() const
  {
    bool is_valid = common::is_valid_tenant_id(tenant_id_) && ls_id_.is_valid() && tx_id_.is_valid();
    return is_valid;
  }

  share::ObLSID get_ls_id() const { return ls_id_; }

  transaction::ObTransID get_tx_id() const { return tx_id_; }

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tx_id));

public:  // derived from ObIKVCacheKey
  virtual bool operator==(const ObIKVCacheKey &other) const
  {
    const ObTxDataCacheKey &rhs = static_cast<const ObTxDataCacheKey&>(other);
    bool equal = (tenant_id_ == rhs.get_tenant_id() && ls_id_ == rhs.get_ls_id() && tx_id_ == rhs.get_tx_id());
    return equal;
  }

  virtual uint64_t hash() const
  {
    uint64_t hash_code = 0;
    hash_code = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_code);
    hash_code = murmurhash(&ls_id_, sizeof(ls_id_), hash_code);
    hash_code = murmurhash(&tx_id_, sizeof(tx_id_), hash_code);
    return hash_code;
  }

  virtual uint64_t get_tenant_id() const { return tenant_id_; }

  virtual int64_t size() const { return sizeof(*this); }

  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", KR(ret), K(buf_len), K(size()));
    } else {
      ObTxDataCacheKey *new_key = new (buf) ObTxDataCacheKey(tenant_id_, ls_id_, tx_id_);
      if (OB_ISNULL(new_key)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "new key ptr is null", KR(ret), KPC(this));
      } else {
        key = new_key;
      }
    }
    return ret;
  }

private:
  int64_t tenant_id_;
  share::ObLSID ls_id_;
  transaction::ObTransID tx_id_;
};

class ObTxDataCacheValue : public common::ObIKVCacheValue {
public:
  ObTxDataCacheValue() : is_inited_(false), tx_data_(nullptr), undo_node_array_(nullptr), mtl_alloc_buf_(nullptr) {}
  ~ObTxDataCacheValue();

  int init(const ObTxData &tx_data);

  int init(const ObTxDataCacheValue &tx_data_cache_val, void *buf, const int64_t buf_len);

  bool is_valid() const { return is_inited_; }

  const ObTxData *get_tx_data() const { return tx_data_; }

  void destroy();

  TO_STRING_KV(K_(is_inited), KP_(tx_data), KPC_(tx_data), KP_(mtl_alloc_buf), KP(&reserved_buf_));

public:  // derived from ObIKVCacheValue
  virtual int64_t size() const { return (IS_INIT && OB_NOT_NULL(tx_data_)) ? sizeof(*this) + tx_data_->size_need_cache() : 0; }

  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const;

private:
  int inner_deep_copy_(void *buf, const ObTxData &rhs);
  int inner_deep_copy_undo_status_(const ObTxData &rhs);

private:
  bool is_inited_;
  storage::ObTxData *tx_data_;
  storage::ObUndoStatusNode *undo_node_array_;
  void *mtl_alloc_buf_;
  storage::ObTxData reserved_buf_;
};

struct ObTxDataValueHandle {
  const ObTxDataCacheValue *value_;
  common::ObKVCacheHandle handle_;

  ObTxDataValueHandle() : value_(nullptr), handle_() {}
  ObTxDataValueHandle(const ObTxDataValueHandle &rhs) { *this = rhs; }
  ObTxDataValueHandle &operator=(const ObTxDataValueHandle &rhs)
  {
    value_ = rhs.value_;
    handle_ = rhs.handle_;
    return *this;
  }
  ~ObTxDataValueHandle() {}

  OB_INLINE bool is_valid() const { return OB_NOT_NULL(value_) && value_->is_valid() && handle_.is_valid(); }

  TO_STRING_KV(KP_(value), KPC_(value))
};

class ObTxDataKVCache : public common::ObKVCache<ObTxDataCacheKey, ObTxDataCacheValue> {
public:
  static ObTxDataKVCache &get_instance()
  {
    static ObTxDataKVCache instance;
    return instance;
  }

public:
  ObTxDataKVCache() {}
  DELETE_COPY_CONSTRUCTOR(ObTxDataKVCache);
  ~ObTxDataKVCache() {}

  int get_row(const ObTxDataCacheKey &key, ObTxDataValueHandle &val_handle);
  int put_row(const ObTxDataCacheKey &key, const ObTxDataCacheValue &value);
};


#undef DELETE_COPY_CONSTRUCTOR
}  // namespace storage
}  // namespace oceanbase
