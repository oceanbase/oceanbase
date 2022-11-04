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

#include "storage/backup/ob_backup_index_cache.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::share;

namespace oceanbase {
namespace backup {

/* ObBackupBlockDesc */

ObBackupBlockDesc::ObBackupBlockDesc()
    : turn_id_(-1), retry_id_(-1), file_type_(BACKUP_FILE_TYPE_MAX), file_id_(-1), offset_(-1), length_(-1)
{}

int ObBackupBlockDesc::set(const int64_t turn_id, const int64_t retry_id, const ObBackupFileType &file_type,
    const int64_t file_id, const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (turn_id <= 0 || retry_id < 0 || file_type < BACKUP_DATA_FILE || file_type >= BACKUP_FILE_TYPE_MAX ||
      file_id < 0 || offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(turn_id), K(retry_id), K(file_id), K(offset), K(length));
  } else {
    turn_id_ = turn_id;
    retry_id_ = retry_id;
    file_type_ = file_type;
    file_id_ = file_id;
    offset_ = offset;
    length_ = length;
  }
  return ret;
}

bool ObBackupBlockDesc::is_valid() const
{
  return turn_id_ > 0 && retry_id_ >= 0 && file_type_ >= BACKUP_DATA_FILE && file_type_ < BACKUP_FILE_TYPE_MAX &&
         file_id_ >= 0 && offset_ >= 0 && length_ > 0;
}

bool ObBackupBlockDesc::operator==(const ObBackupBlockDesc &other) const
{
  return turn_id_ == other.turn_id_ && retry_id_ == other.retry_id_ && file_type_ == other.file_type_ &&
         file_id_ == other.file_id_ && offset_ == other.offset_ && length_ == other.length_;
}

/* ObBackupIndexCacheKey */

ObBackupIndexCacheKey::ObBackupIndexCacheKey()
    : mode_(), tenant_id_(), backup_set_id_(), ls_id_(), backup_data_type_(), block_desc_()
{}

ObBackupIndexCacheKey::ObBackupIndexCacheKey(const ObBackupRestoreMode &mode, const uint64_t tenant_id,
    const int64_t backup_set_id, const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
    const ObBackupBlockDesc &block_desc)
    : mode_(mode),
      tenant_id_(tenant_id),
      backup_set_id_(backup_set_id),
      ls_id_(ls_id),
      backup_data_type_(backup_data_type),
      block_desc_(block_desc)
{}

ObBackupIndexCacheKey::~ObBackupIndexCacheKey()
{}

int ObBackupIndexCacheKey::set(const ObBackupRestoreMode &mode, const uint64_t tenant_id, const int64_t backup_set_id,
    const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type, const ObBackupBlockDesc &block_desc)
{
  int ret = OB_SUCCESS;
  if (mode < BACKUP_MODE || mode >= MAX_MODE || OB_INVALID_ID == tenant_id || !block_desc.is_valid() ||
      !backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(mode), K(tenant_id), K(backup_set_id), K(block_desc));
  } else {
    mode_ = mode;
    tenant_id_ = tenant_id;
    backup_set_id_ = backup_set_id;
    ls_id_ = ls_id;
    backup_data_type_ = backup_data_type;
    block_desc_ = block_desc;
  }
  return ret;
}

bool ObBackupIndexCacheKey::operator==(const ObIKVCacheKey &other) const
{
  const ObBackupIndexCacheKey &other_key = reinterpret_cast<const ObBackupIndexCacheKey &>(other);
  return mode_ == other_key.mode_ && tenant_id_ == other_key.tenant_id_ && backup_set_id_ == other_key.backup_set_id_ &&
         ls_id_ == other_key.ls_id_ && backup_data_type_.type_ == other_key.backup_data_type_.type_ &&
         block_desc_ == other_key.block_desc_;
}

uint64_t ObBackupIndexCacheKey::get_tenant_id() const
{
  return tenant_id_;
}

uint64_t ObBackupIndexCacheKey::hash() const
{
  return murmurhash(this, sizeof(ObBackupIndexCacheKey), 0);
}

int64_t ObBackupIndexCacheKey::size() const
{
  return sizeof(*this);
}

int ObBackupIndexCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(buf_len));
  } else {
    key = new (buf) ObBackupIndexCacheKey(mode_, tenant_id_, backup_set_id_, ls_id_, backup_data_type_, block_desc_);
  }
  return ret;
}

/* ObBackupIndexCacheValue */

ObBackupIndexCacheValue::ObBackupIndexCacheValue() : buf_(NULL), len_(0)
{}

ObBackupIndexCacheValue::ObBackupIndexCacheValue(const char *buf, const int64_t len) : buf_(buf), len_(len)
{}

ObBackupIndexCacheValue::~ObBackupIndexCacheValue()
{}

int64_t ObBackupIndexCacheValue::size() const
{
  return sizeof(*this) + len_;
}

int ObBackupIndexCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(buf), K(buf_len), "request_size", size());
  } else {
    ObBackupIndexCacheValue *value_ptr = new (buf) ObBackupIndexCacheValue;
    value_ptr->buf_ = buf + sizeof(*this);
    value_ptr->len_ = len_;
    if (NULL != buf_) {
      MEMCPY(const_cast<char *>(value_ptr->buf_), buf_, len_);
    }
    value = value_ptr;
  }
  return ret;
}

/* ObBackupIndexKVCache */

ObBackupIndexKVCache::ObBackupIndexKVCache() : is_inited_(false)
{}

ObBackupIndexKVCache::~ObBackupIndexKVCache()
{}

ObBackupIndexKVCache &ObBackupIndexKVCache::get_instance()
{
  static ObBackupIndexKVCache instance_;
  return instance_;
}

int ObBackupIndexKVCache::init()
{
  int ret = OB_SUCCESS;
  const char *cache_name = "BACKUP_INDEX_CACHE";
  const int64_t priority = 1;
  if (OB_SUCCESS != (ret = ObKVCache<ObBackupIndexCacheKey, ObBackupIndexCacheValue>::init(cache_name, priority))) {
    LOG_WARN("failed to init ObKVCache", K(ret), K(cache_name), K(priority));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObBackupIndexKVCache::destroy()
{
  ObKVCache<ObBackupIndexCacheKey, ObBackupIndexCacheValue>::destroy();
}

bool ObBackupIndexKVCache::is_inited() const
{
  return is_inited_;
}

}  // namespace backup
}  // namespace oceanbase
