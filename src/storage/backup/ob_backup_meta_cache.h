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

#ifndef STORAGE_LOG_STREAM_BACKUP_META_CACHE_H_
#define STORAGE_LOG_STREAM_BACKUP_META_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "share/ob_ls_id.h"
#include "storage/backup/ob_backup_data_struct.h"

#define OB_BACKUP_META_CACHE oceanbase::backup::ObBackupMetaKVCache::get_instance()

namespace oceanbase {
namespace backup {

class ObBackupMetaCacheKey : public common::ObIKVCacheKey {
public:
  ObBackupMetaCacheKey();
  ObBackupMetaCacheKey(const uint64_t tenant_id, const ObBackupMetaIndex &meta_index);
  virtual ~ObBackupMetaCacheKey();
  virtual bool operator==(const ObIKVCacheKey &other) const override;
  virtual uint64_t get_tenant_id() const override;
  virtual uint64_t hash() const override;
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;

  TO_STRING_KV(K_(meta_index));

private:
  uint64_t tenant_id_;
  ObBackupMetaIndex meta_index_;
};

class ObBackupMetaCacheValue : public common::ObIKVCacheValue {
public:
  ObBackupMetaCacheValue();
  ObBackupMetaCacheValue(const char *buf, const int64_t size);
  virtual ~ObBackupMetaCacheValue();
  const char *buf() const
  {
    return buf_;
  }
  int64_t len() const
  {
    return len_;
  }
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  bool is_valid() const;
  TO_STRING_KV(KP_(buf), K_(len));

private:
  const char *buf_;
  int64_t len_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaCacheValue);
};

class ObBackupMetaKVCache : public common::ObKVCache<ObBackupMetaCacheKey, ObBackupMetaCacheValue> {
public:
  static ObBackupMetaKVCache &get_instance();
  int init();
  void destroy();
  bool is_inited() const;

private:
  ObBackupMetaKVCache();
  virtual ~ObBackupMetaKVCache();

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaKVCache);
};

class ObBackupMetaCacheReader final {
public:
  ObBackupMetaCacheReader();
  ~ObBackupMetaCacheReader();
  int init(const uint64_t tenant_id, const common::ObString &path, const share::ObBackupStorageInfo *storage_info,
      const common::ObStorageIdMod &mod, ObBackupMetaKVCache &cache);
  int fetch_block(const ObBackupMetaIndex &meta_index, common::ObIAllocator &allocator,
      ObKVCacheHandle &handle, blocksstable::ObBufferReader &buffer_reader);

private:
  int get_backup_meta_cache_key_(const ObBackupMetaIndex &meta_index, ObBackupMetaCacheKey &cache_key) const;
  int do_on_cache_miss_(const ObBackupMetaIndex &meta_index, common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer_reader);
  int put_block_to_cache_(const ObBackupMetaIndex &meta_index, const blocksstable::ObBufferReader &buffer);
  int fetch_index_block_from_dest_(const common::ObString &path, const share::ObBackupStorageInfo *storage_info,
      const int64_t offset, const int64_t length, common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObString path_;
  const share::ObBackupStorageInfo *storage_info_;
  common::ObStorageIdMod mod_;
  ObBackupMetaKVCache *meta_kv_cache_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupMetaCacheReader);
};

}  // namespace backup
}  // namespace oceanbase

#endif