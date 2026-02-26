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

#include "lib/encrypt/ob_caching_sha2_cache.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{

// ========== ObCachingSha2Digest Implementation ==========

ObCachingSha2Digest::ObCachingSha2Digest()
  : digest_len_(0)
{
  MEMSET(digest_, 0, sizeof(digest_));
}

ObCachingSha2Digest::ObCachingSha2Digest(common::ObIAllocator &allocator)
  : digest_len_(0)
{
  UNUSED(allocator);
  MEMSET(digest_, 0, sizeof(digest_));
}

void ObCachingSha2Digest::reset()
{
  MEMSET(digest_, 0, sizeof(digest_));
  digest_len_ = 0;
}

int64_t ObCachingSha2Digest::size() const
{
  return sizeof(ObCachingSha2Digest);
}

int ObCachingSha2Digest::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  ObCachingSha2Digest *tmp = NULL;

  if (NULL == buf || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", KP(buf), K(buf_len), K(size()), K(ret));
  } else {
    tmp = new (buf) ObCachingSha2Digest();
    if (digest_len_ > OB_SHA256_DIGEST_LENGTH) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid digest length", K(ret), K(digest_len_));
    } else {
      MEMCPY(tmp->digest_, digest_, digest_len_);
      tmp->digest_len_ = digest_len_;
      value = tmp;
    }
  }

  return ret;
}

int ObCachingSha2Digest::set_digest(const char *digest, int64_t digest_len)
{
  int ret = OB_SUCCESS;

  if (NULL == digest || digest_len != OB_SHA256_DIGEST_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid digest", K(ret), KP(digest), K(digest_len));
  } else {
    MEMCPY(digest_, digest, digest_len);
    digest_len_ = digest_len;
  }

  return ret;
}

// Serialization method implementation
int ObCachingSha2Digest::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, digest_len_))) {
    COMMON_LOG(WARN, "failed to serialize digest_len", K(ret));
  } else if (digest_len_ > 0 && digest_len_ <= OB_SHA256_DIGEST_LENGTH) {
    if (pos + digest_len_ > buf_len) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(WARN, "buffer not enough", K(ret), K(pos), K(digest_len_), K(buf_len));
    } else {
      MEMCPY(buf + pos, digest_, digest_len_);
      pos += digest_len_;
    }
  }

  return ret;
}

int ObCachingSha2Digest::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &digest_len_))) {
    COMMON_LOG(WARN, "failed to deserialize digest_len", K(ret));
  } else if (digest_len_ < 0 || digest_len_ > OB_SHA256_DIGEST_LENGTH) {
    ret = OB_INVALID_DATA;
    COMMON_LOG(WARN, "invalid digest_len", K(ret), K(digest_len_));
  } else if (digest_len_ > 0) {
    if (pos + digest_len_ > data_len) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(WARN, "buffer not enough", K(ret), K(pos), K(digest_len_), K(data_len));
    } else {
      MEMCPY(digest_, buf + pos, digest_len_);
      pos += digest_len_;
    }
  }

  return ret;
}

int64_t ObCachingSha2Digest::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_i64(digest_len_);
  len += digest_len_;
  return len;
}

// ========== ObCachingSha2Key Implementation ==========

ObCachingSha2Key::ObCachingSha2Key()
  : tenant_id_(OB_SERVER_TENANT_ID),
    user_name_(),
    host_name_(),
    password_last_changed_timestamp_(0)
{
}

ObCachingSha2Key::ObCachingSha2Key(const common::ObString &user_name,
                                   const common::ObString &host_name,
                                   uint64_t tenant_id,
                                   int64_t password_last_changed_timestamp)
  : tenant_id_(tenant_id),
    user_name_(user_name),
    host_name_(host_name),
    password_last_changed_timestamp_(password_last_changed_timestamp)
{
}

uint64_t ObCachingSha2Key::hash() const
{
  uint64_t hash_val = 0;

  // Hash tenant_id
  hash_val = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);

  // Hash user_name
  if (!user_name_.empty()) {
    hash_val = murmurhash(user_name_.ptr(), user_name_.length(), hash_val);
  }

  // Hash host_name
  if (!host_name_.empty()) {
    hash_val = murmurhash(host_name_.ptr(), host_name_.length(), hash_val);
  }

  // Hash password_last_changed_timestamp
  hash_val = murmurhash(&password_last_changed_timestamp_,
                        sizeof(password_last_changed_timestamp_),
                        hash_val);

  return hash_val;
}

int ObCachingSha2Key::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

bool ObCachingSha2Key::operator==(const ObIKVCacheKey &other) const
{
  const ObCachingSha2Key &other_key = reinterpret_cast<const ObCachingSha2Key&>(other);
  return tenant_id_ == other_key.tenant_id_
      && user_name_ == other_key.user_name_
      && host_name_ == other_key.host_name_
      && password_last_changed_timestamp_ == other_key.password_last_changed_timestamp_;
}

int64_t ObCachingSha2Key::size() const
{
  return sizeof(ObCachingSha2Key) + user_name_.length() + host_name_.length();
}

int ObCachingSha2Key::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  ObCachingSha2Key *tmp_key = NULL;
  int64_t pos = 0;

  if (NULL == buf || buf_len < size()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments", KP(buf), K(buf_len), K(size()), K(ret));
  } else {
    // Construct Key object
    tmp_key = new (buf) ObCachingSha2Key();
    tmp_key->tenant_id_ = tenant_id_;
    tmp_key->password_last_changed_timestamp_ = password_last_changed_timestamp_;
    pos += sizeof(ObCachingSha2Key);

    // Deep copy user_name
    if (user_name_.length() > 0) {
      MEMCPY(buf + pos, user_name_.ptr(), user_name_.length());
      tmp_key->user_name_.assign_ptr(buf + pos, user_name_.length());
      pos += user_name_.length();
    }

    // Deep copy host_name
    if (host_name_.length() > 0) {
      MEMCPY(buf + pos, host_name_.ptr(), host_name_.length());
      tmp_key->host_name_.assign_ptr(buf + pos, host_name_.length());
      pos += host_name_.length();
    }

    key = tmp_key;
  }

  return ret;
}

bool ObCachingSha2Key::is_valid() const
{
  // user_name cannot be empty, host_name can be empty (represents any host)
  return !user_name_.empty();
}

// ========== ObCachingSha2Cache Implementation ==========

int ObCachingSha2Cache::get_row(const ObCachingSha2Key &key, ObCachingSha2Handle &handle)
{
  int ret = OB_SUCCESS;

  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid caching sha2 cache key", K(key), K(ret));
  } else if (OB_FAIL(get(key, handle.digest_, handle.handle_))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      COMMON_LOG(WARN, "fail to get key from cache", K(key), K(ret));
    }
  } else {
    handle.cache_ = this;
  }

  return ret;
}

int ObCachingSha2Cache::put_row(const ObCachingSha2Key &key, const ObCachingSha2Digest &value)
{
  int ret = OB_SUCCESS;

  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid caching sha2 cache key", K(key), K(ret));
  } else if (OB_FAIL(put(key, value, true/*overwrite*/))) {
    COMMON_LOG(WARN, "put value in cache failed", K(key), K(ret));
  }

  return ret;
}

int ObCachingSha2Cache::put_and_fetch_row(const ObCachingSha2Key &key,
                                          const ObCachingSha2Digest &value,
                                          ObCachingSha2Handle &handle)
{
  int ret = OB_SUCCESS;

  if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid caching sha2 cache key", K(key), K(ret));
  } else if (OB_FAIL(put_and_fetch(key, value, handle.digest_, handle.handle_, true /*overwrite*/))) {
    COMMON_LOG(WARN, "fail to put kvpair to cache", K(ret));
  } else {
    handle.cache_ = this;
  }

  return ret;
}

} // end of namespace common
} // end of namespace oceanbase
