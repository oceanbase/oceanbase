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

#ifndef _OB_CACHING_SHA2_CACHE_H_
#define _OB_CACHING_SHA2_CACHE_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "share/cache/ob_kv_storecache.h"
#include "lib/encrypt/ob_sha256_crypt.h"

namespace oceanbase
{
namespace common
{
class ObCachingSha2Handle;

/**
 * Caching SHA2 authentication digest value class
 * Stores 32-byte SHA256 digest
 */
class ObCachingSha2Digest : public common::ObIKVCacheValue
{
  OB_UNIS_VERSION_V(1);
public:
  ObCachingSha2Digest();
  explicit ObCachingSha2Digest(common::ObIAllocator &allocator);
  ~ObCachingSha2Digest() { reset(); }

  void reset();

  // ObIKVCacheValue interface implementation
  int64_t size() const;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const;

  // Set digest
  int set_digest(const char *digest, int64_t digest_len);

  // Get digest
  const char *get_digest() const { return digest_; }
  int64_t get_digest_len() const { return digest_len_; }

  TO_STRING_KV(K_(digest_len));

private:
  char digest_[OB_SHA256_DIGEST_LENGTH];  // 32-byte SHA256 digest
  int64_t digest_len_;                     // Digest length (should always be 32)
};

/**
 * Caching SHA2 cache Key class
 * Composed of tenant_id + user_name + host_name + password_last_changed_timestamp
 *
 * Important: password_last_changed_timestamp as part of the key automatically solves cache invalidation after password change
 * When password changes, timestamp changes, forming a new cache key, old cache automatically becomes invalid
 */
class ObCachingSha2Key : public common::ObIKVCacheKey
{
public:
  ObCachingSha2Key();
  ObCachingSha2Key(const common::ObString &user_name,
                   const common::ObString &host_name,
                   uint64_t tenant_id,
                   int64_t password_last_changed_timestamp);
  ~ObCachingSha2Key() {}

  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool operator==(const ObIKVCacheKey &other) const;
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t size() const;
  int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const;
  bool is_valid() const;

  // Getter and Setter
  const common::ObString &get_user_name() const { return user_name_; }
  const common::ObString &get_host_name() const { return host_name_; }
  int64_t get_password_last_changed_timestamp() const { return password_last_changed_timestamp_; }
  void set_user_name(const common::ObString &user_name) { user_name_ = user_name; }
  void set_host_name(const common::ObString &host_name) { host_name_ = host_name; }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_password_last_changed_timestamp(int64_t timestamp) { password_last_changed_timestamp_ = timestamp; }

  TO_STRING_KV(K_(tenant_id), K_(user_name), K_(host_name), K_(password_last_changed_timestamp));

private:
  uint64_t tenant_id_;
  common::ObString user_name_;
  common::ObString host_name_;
  int64_t password_last_changed_timestamp_;
};

/**
 * Caching SHA2 cache class
 * Used to cache user's SHA256 authentication digest
 */
class ObCachingSha2Cache : public common::ObKVCache<ObCachingSha2Key, ObCachingSha2Digest>
{
public:
  ObCachingSha2Cache() {}
  ~ObCachingSha2Cache() {}

  /**
   * Get user's authentication digest from cache
   * @param[in] key  Cache key (username and hostname)
   * @param[out] handle  Handle object holding the digest
   * @return Error code
   */
  int get_row(const ObCachingSha2Key &key, ObCachingSha2Handle &handle);

  /**
   * Put user's authentication digest into cache
   * @param[in] key  Cache key (username and hostname)
   * @param[in] value  Authentication digest value
   * @return Error code
   */
  int put_row(const ObCachingSha2Key &key, const ObCachingSha2Digest &value);

  /**
   * Put user's authentication digest into cache and get handle
   * @param[in] key  Cache key (username and hostname)
   * @param[in] value  Authentication digest value
   * @param[out] handle  Handle object holding the digest
   * @return Error code
   */
  int put_and_fetch_row(const ObCachingSha2Key &key,
                        const ObCachingSha2Digest &value,
                        ObCachingSha2Handle &handle);
};

/**
 * Caching SHA2 Handle class
 * Used to hold authentication digest object in cache, preventing it from being released
 * As long as this instance exists, the pointer remains valid (even if the object is removed from cache)
 */
class ObCachingSha2Handle
{
public:
  friend class ObCachingSha2Cache;
  ObCachingSha2Handle() : digest_(nullptr), cache_(nullptr) {}
  ~ObCachingSha2Handle() { digest_ = nullptr; cache_ = nullptr; }

  int assign(const ObCachingSha2Handle& other)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(handle_.assign(other.handle_))) {
      COMMON_LOG(WARN, "fail to assign handle");
      this->digest_ = nullptr;
      this->cache_ = nullptr;
    } else {
      this->digest_ = other.digest_;
      this->cache_ = other.cache_;
    }
    return ret;
  }

  void move_from(ObCachingSha2Handle& other)
  {
    this->digest_ = other.digest_;
    this->cache_ = other.cache_;
    this->handle_.move_from(other.handle_);
    other.reset();
  }

  void reset() { digest_ = nullptr; cache_ = nullptr; handle_.reset(); }

  const ObCachingSha2Digest *digest_;
  TO_STRING_KV(K(digest_));

private:
  ObCachingSha2Cache *cache_;
  ObKVCacheHandle handle_;
};

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OB_CACHING_SHA2_CACHE_H_ */
