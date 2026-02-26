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

#define USING_LOG_PREFIX STORAGE

#include "storage/backup/ob_backup_meta_cache.h"
#include "ob_backup_restore_util.h"

using namespace oceanbase::share;

namespace oceanbase {
namespace backup {

/* ObBackupMetaCacheKey */

ObBackupMetaCacheKey::ObBackupMetaCacheKey()
    : tenant_id_(), meta_index_() {}

ObBackupMetaCacheKey::ObBackupMetaCacheKey(const uint64_t tenant_id, const ObBackupMetaIndex &meta_index)
    : tenant_id_(tenant_id), meta_index_(meta_index) {}

ObBackupMetaCacheKey::~ObBackupMetaCacheKey()
{}

bool ObBackupMetaCacheKey::operator==(const ObIKVCacheKey &other) const
{
  const ObBackupMetaCacheKey &other_key = reinterpret_cast<const ObBackupMetaCacheKey &>(other);
  return tenant_id_ == other_key.tenant_id_ && meta_index_ == other_key.meta_index_;
}

uint64_t ObBackupMetaCacheKey::get_tenant_id() const
{
  return tenant_id_;
}

uint64_t ObBackupMetaCacheKey::hash() const
{
  uint64_t hash_code = 0;
  hash_code = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_code);
  hash_code = meta_index_.calc_hash(hash_code);
  return hash_code;
}

int64_t ObBackupMetaCacheKey::size() const
{
  return sizeof(*this);
}

int ObBackupMetaCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(buf_len));
  } else {
    key = new (buf) ObBackupMetaCacheKey(tenant_id_, meta_index_);
  }
  return ret;
}

/* ObBackupMetaCacheValue */

ObBackupMetaCacheValue::ObBackupMetaCacheValue() : buf_(NULL), len_(0)
{}

ObBackupMetaCacheValue::ObBackupMetaCacheValue(const char *buf, const int64_t len) : buf_(buf), len_(len)
{}

ObBackupMetaCacheValue::~ObBackupMetaCacheValue()
{}

int64_t ObBackupMetaCacheValue::size() const
{
  return sizeof(*this) + len_;
}

int ObBackupMetaCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), "request_size", size());
  } else if (OB_ISNULL(buf_) || len_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf or len is not expected", K(ret), KP_(buf), K_(len));
  } else {
    ObBackupMetaCacheValue *value_ptr = new (buf) ObBackupMetaCacheValue;
    value_ptr->buf_ = buf + sizeof(*this);
    value_ptr->len_ = len_;
    MEMCPY(const_cast<char *>(value_ptr->buf_), buf_, len_);
    value = value_ptr;
  }
  return ret;
}

/* ObBackupMetaKVCache */

ObBackupMetaKVCache::ObBackupMetaKVCache() : is_inited_(false)
{}

ObBackupMetaKVCache::~ObBackupMetaKVCache()
{}

ObBackupMetaKVCache &ObBackupMetaKVCache::get_instance()
{
  static ObBackupMetaKVCache instance_;
  return instance_;
}

int ObBackupMetaKVCache::init()
{
  int ret = OB_SUCCESS;
  const char *cache_name = "BACKUP_META_CACHE";
  const int64_t priority = 1;
  if (OB_SUCCESS != (ret = ObKVCache<ObBackupMetaCacheKey, ObBackupMetaCacheValue>::init(cache_name, priority))) {
    LOG_WARN("failed to init ObKVCache", K(ret), K(cache_name), K(priority));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObBackupMetaKVCache::destroy()
{
  ObKVCache<ObBackupMetaCacheKey, ObBackupMetaCacheValue>::destroy();
}

bool ObBackupMetaKVCache::is_inited() const
{
  return is_inited_;
}

/* ObBackupMetaCacheReader */

ObBackupMetaCacheReader::ObBackupMetaCacheReader() : is_inited_(false), tenant_id_(), path_(), storage_info_(NULL), mod_(), meta_kv_cache_(NULL) {}

ObBackupMetaCacheReader::~ObBackupMetaCacheReader() {}

int ObBackupMetaCacheReader::init(const uint64_t tenant_id, const common::ObString &path, const share::ObBackupStorageInfo *storage_info,
    const common::ObStorageIdMod &mod, ObBackupMetaKVCache &cache)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("meta cache reader init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(tenant_id), K(path));
  } else {
    tenant_id_ = tenant_id;
    path_ = path;
    storage_info_ = storage_info;
    mod_ = mod;
    meta_kv_cache_ = &cache;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupMetaCacheReader::fetch_block(const ObBackupMetaIndex &meta_index, common::ObIAllocator &allocator,
    ObKVCacheHandle &handle, blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  ObBackupMetaCacheKey key;
  const ObBackupMetaCacheValue *pvalue = NULL;
  if (!meta_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(meta_index));
  } else if (OB_ISNULL(meta_kv_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta cache should not be null", K(ret));
  } else if (OB_FAIL(get_backup_meta_cache_key_(meta_index, key))) {
    LOG_WARN("failed to get backup index cache key", K(ret), K(meta_index));
  } else if (OB_FAIL(meta_kv_cache_->get(key, pvalue, handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      EVENT_INC(ObStatEventIds::BACKUP_META_CACHE_MISS);
      ret = OB_SUCCESS;
      if (OB_FAIL(do_on_cache_miss_(meta_index, allocator, buffer_reader))) {
        LOG_WARN("failed to do on cache miss", K(ret), K(meta_index));
      } else {
        const int64_t hit_cnt = meta_kv_cache_->get_hit_cnt();
        const int64_t miss_cnt = meta_kv_cache_->get_miss_cnt();
        LOG_DEBUG("do on cache miss", K(meta_index), K(hit_cnt), K(miss_cnt));
      }
    } else {
      LOG_WARN("failed to get value from kv cache", K(ret), K(key));
    }
  } else if (OB_ISNULL(pvalue)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache value should not be null", K(ret));
  } else {
    buffer_reader.assign(pvalue->buf(), pvalue->len());
    EVENT_INC(ObStatEventIds::BACKUP_META_CACHE_HIT);
  }
  return ret;
}

int ObBackupMetaCacheReader::get_backup_meta_cache_key_(const ObBackupMetaIndex &meta_index, ObBackupMetaCacheKey &cache_key) const
{
  int ret = OB_SUCCESS;
  if (!meta_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(meta_index));
  } else {
    cache_key = ObBackupMetaCacheKey(tenant_id_, meta_index);
  }
  return ret;
}

int ObBackupMetaCacheReader::do_on_cache_miss_(const ObBackupMetaIndex &meta_index, common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer_reader)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fetch_index_block_from_dest_(path_, storage_info_, meta_index.offset_, meta_index.length_, allocator, buffer_reader))) {
    LOG_WARN("failed to fetch index block from dest", K(ret), K(path_), K(meta_index));
  } else if (OB_FAIL(put_block_to_cache_(meta_index, buffer_reader))) {
    LOG_WARN("failed to put block to cache", K(ret), K(meta_index));
  }
  return ret;
}

int ObBackupMetaCacheReader::put_block_to_cache_(const ObBackupMetaIndex &meta_index, const blocksstable::ObBufferReader &buffer)
{
  int ret = OB_SUCCESS;
  ObBackupMetaCacheKey key;
  ObBackupMetaCacheValue value(buffer.data(), buffer.capacity());
  if (!meta_index.is_valid() || !buffer.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(meta_index));
  } else if (OB_ISNULL(meta_kv_cache_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index cache should not be null", K(ret));
  } else if (OB_FAIL(get_backup_meta_cache_key_(meta_index, key))) {
    LOG_WARN("failed to get backup meta cache key", K(ret), K(meta_index));
  } else if (OB_FAIL(meta_kv_cache_->put(key, value))) {
    LOG_WARN("failed to put to kv cache", K(ret), K(key), K(value));
  }
  return ret;
}

int ObBackupMetaCacheReader::fetch_index_block_from_dest_(const common::ObString &path,
    const share::ObBackupStorageInfo *storage_info, const int64_t offset, const int64_t length,
    common::ObIAllocator &allocator, blocksstable::ObBufferReader &buffer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(offset), K(length));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(length));
  } else if (OB_FAIL(ObLSBackupRestoreUtil::pread_file(path, storage_info, mod_, offset, length, buf))) {
    LOG_WARN("failed to pread file", K(ret), K(path), KP(storage_info), K(offset), K(length));
  } else {
    buffer.assign(buf, length);
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase
