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

#ifndef _OB_AUTH_DIGEST_CACHE_H_
#define _OB_AUTH_DIGEST_CACHE_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "share/cache/ob_kv_storecache.h"
#include "lib/encrypt/ob_crypt_common.h"

namespace oceanbase
{
namespace common
{
class ObAuthDigestHandle;

/**
 * Authentication digest value class.
 * Stores 32-byte digest (SHA256 or SM3).
 */
class ObAuthDigest : public common::ObIKVCacheValue
{
  OB_UNIS_VERSION_V(1);
public:
  ObAuthDigest();
  explicit ObAuthDigest(common::ObIAllocator &allocator);
  ~ObAuthDigest() { reset(); }

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
  char digest_[OB_CRYPT_RAW_DIGEST_LEN32];  // 32-byte digest
  int64_t digest_len_;                     // Digest length
};

/**
 * Authentication digest cache Key class.
 * Composed of tenant_id + user_name + host_name + password_last_changed_timestamp.
 *
 * password_last_changed_timestamp as part of the key automatically solves
 * cache invalidation after password change.
 */
class ObAuthDigestKey : public common::ObIKVCacheKey
{
public:
  ObAuthDigestKey();
  ObAuthDigestKey(const common::ObString &user_name,
                   const common::ObString &host_name,
                   uint64_t tenant_id,
                   int64_t password_last_changed_timestamp);
  ~ObAuthDigestKey() {}

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
 * Authentication digest cache class.
 * Used to cache user's authentication digest (SHA256 or SM3).
 */
class ObAuthDigestCache : public common::ObKVCache<ObAuthDigestKey, ObAuthDigest>
{
public:
  ObAuthDigestCache() {}
  ~ObAuthDigestCache() {}

  /**
   * Get user's authentication digest from cache.
   */
  int get_row(const ObAuthDigestKey &key, ObAuthDigestHandle &handle);

  /**
   * Put user's authentication digest into cache.
   */
  int put_row(const ObAuthDigestKey &key, const ObAuthDigest &value);

  /**
   * Put user's authentication digest into cache and get handle.
   */
  int put_and_fetch_row(const ObAuthDigestKey &key,
                        const ObAuthDigest &value,
                        ObAuthDigestHandle &handle);
};

/**
 * Authentication digest Handle class.
 * Holds authentication digest object in cache, preventing it from being released.
 */
class ObAuthDigestHandle
{
public:
  friend class ObAuthDigestCache;
  ObAuthDigestHandle() : digest_(nullptr), cache_(nullptr) {}
  ~ObAuthDigestHandle() { digest_ = nullptr; cache_ = nullptr; }

  int assign(const ObAuthDigestHandle& other)
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

  void move_from(ObAuthDigestHandle& other)
  {
    this->digest_ = other.digest_;
    this->cache_ = other.cache_;
    this->handle_.move_from(other.handle_);
    other.reset();
  }

  void reset() { digest_ = nullptr; cache_ = nullptr; handle_.reset(); }

  const ObAuthDigest *digest_;
  TO_STRING_KV(K(digest_));

private:
  ObAuthDigestCache *cache_;
  ObKVCacheHandle handle_;
};

// ========== Backward-compatible aliases ==========
// Old names preserved so existing callers don't break.
// New code should use the ObAuthDigest* names directly.

typedef ObAuthDigest ObCachingSha2Digest;
typedef ObAuthDigestKey ObCachingSha2Key;
typedef ObAuthDigestCache ObCachingSha2Cache;
typedef ObAuthDigestHandle ObCachingSha2Handle;

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OB_AUTH_DIGEST_CACHE_H_ */
