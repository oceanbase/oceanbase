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

#ifndef STORAGE_LOG_STREAM_BACKUP_INDEX_CACHE_H_
#define STORAGE_LOG_STREAM_BACKUP_INDEX_CACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "share/ob_ls_id.h"
#include "storage/backup/ob_backup_data_struct.h"

#define OB_BACKUP_INDEX_CACHE oceanbase::backup::ObBackupIndexKVCache::get_instance()

namespace oceanbase {
namespace backup {

struct ObBackupBlockDesc {
  ObBackupBlockDesc();
  int set(const int64_t turn_id, const int64_t retry_id, const ObBackupFileType &file_type, const int64_t file_id,
      const int64_t offset, const int64_t length);
  bool is_valid() const;
  bool operator==(const ObBackupBlockDesc &other) const;
  TO_STRING_KV(K_(turn_id), K_(retry_id), K_(file_type), K_(file_id), K_(offset), K_(length));
  int64_t turn_id_;
  int64_t retry_id_;
  ObBackupFileType file_type_;
  int64_t file_id_;
  int64_t offset_;
  int64_t length_;
};

class ObBackupIndexCacheKey : public common::ObIKVCacheKey {
public:
  ObBackupIndexCacheKey();
  ObBackupIndexCacheKey(const ObBackupRestoreMode &mode, const uint64_t tenant_id, const int64_t backup_set_id,
      const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type, const ObBackupBlockDesc &block_desc);
  virtual ~ObBackupIndexCacheKey();
  virtual bool operator==(const ObIKVCacheKey &other) const override;
  int set(const ObBackupRestoreMode &mode, const uint64_t tenant_id, const int64_t backup_set_id,
      const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type, const ObBackupBlockDesc &block_desc);
  virtual uint64_t get_tenant_id() const override;
  virtual uint64_t hash() const override;
  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;

  TO_STRING_KV(K_(mode), K_(tenant_id), K_(backup_set_id), K_(ls_id), K_(backup_data_type), K_(block_desc));

private:
  ObBackupRestoreMode mode_;
  uint64_t tenant_id_;
  int64_t backup_set_id_;
  share::ObLSID ls_id_;
  share::ObBackupDataType backup_data_type_;
  ObBackupBlockDesc block_desc_;
};

class ObBackupIndexCacheValue : public common::ObIKVCacheValue {
public:
  ObBackupIndexCacheValue();
  ObBackupIndexCacheValue(const char *buf, const int64_t size);
  virtual ~ObBackupIndexCacheValue();
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
  DISALLOW_COPY_AND_ASSIGN(ObBackupIndexCacheValue);
};

class ObBackupIndexKVCache : public common::ObKVCache<ObBackupIndexCacheKey, ObBackupIndexCacheValue> {
public:
  static ObBackupIndexKVCache &get_instance();
  int init();
  void destroy();
  bool is_inited() const;

private:
  ObBackupIndexKVCache();
  virtual ~ObBackupIndexKVCache();

private:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupIndexKVCache);
};

}  // namespace backup
}  // namespace oceanbase

#endif