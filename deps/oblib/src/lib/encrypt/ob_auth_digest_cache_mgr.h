/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ENCRYPT_OB_AUTH_DIGEST_CACHE_MGR_H_
#define OCEANBASE_ENCRYPT_OB_AUTH_DIGEST_CACHE_MGR_H_

#include "lib/encrypt/ob_auth_digest_cache.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{

/**
 * Authentication digest global cache manager.
 * Singleton pattern, manages fast authentication cache for
 * caching_sha2_password and ob_sm3_password authentication.
 *
 * Cache content: SHA256(SHA256(password)) or SM3(SM3(password))
 * Cache key: tenant_id + user_name + host_name + password_last_changed_timestamp
 *
 * Usage scenarios:
 * 1. After successful full authentication, call put_digest to store digest
 * 2. During fast authentication, call get_digest to get digest
 * 3. When password changes, no manual clearing needed (timestamp change automatically invalidates)
 */
class ObAuthDigestCacheMgr
{
public:
  static ObAuthDigestCacheMgr &get_instance();

  /**
   * Initialize cache manager.
   * Should be called in ObServer::init()
   */
  int init();

  /**
   * Destroy cache manager.
   * Should be called in ObServer::destroy()
   */
  void destroy();

  /**
   * Try to get user's authentication digest from cache.
   *
   * @param user_name Username
   * @param host_name Hostname
   * @param tenant_id Tenant ID
   * @param password_last_changed_timestamp Password last changed timestamp
   * @param handle Output: Handle object holding the digest
   * @return OB_SUCCESS Success
   *         OB_ENTRY_NOT_EXIST Cache miss
   *         Other error codes
   */
  int get_digest(const ObString &user_name,
                 const ObString &host_name,
                 uint64_t tenant_id,
                 int64_t password_last_changed_timestamp,
                 ObAuthDigestHandle &handle);

  /**
   * Put user's authentication digest into cache.
   *
   * @param user_name Username
   * @param host_name Hostname
   * @param tenant_id Tenant ID
   * @param password_last_changed_timestamp Password last changed timestamp
   * @param digest SHA256(SHA256(password)) or SM3(SM3(password)) digest (32 bytes)
   * @param digest_len Digest length (must be 32)
   * @return Error code
   */
  int put_digest(const ObString &user_name,
                 const ObString &host_name,
                 uint64_t tenant_id,
                 int64_t password_last_changed_timestamp,
                 const unsigned char *digest,
                 int64_t digest_len);

  /**
   * Check if initialized.
   */
  bool is_inited() const { return is_inited_; }

private:
  ObAuthDigestCacheMgr() : is_inited_(false) {}
  ~ObAuthDigestCacheMgr() { destroy(); }

  // Disable copy and assignment
  ObAuthDigestCacheMgr(const ObAuthDigestCacheMgr&) = delete;
  ObAuthDigestCacheMgr& operator=(const ObAuthDigestCacheMgr&) = delete;

  bool is_inited_;
  ObAuthDigestCache cache_;
};

// ========== Backward-compatible aliases ==========
// Old name preserved for existing callers.
// New code should use ObAuthDigestCacheMgr directly.

typedef ObAuthDigestCacheMgr ObCachingSha2CacheMgr;

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_ENCRYPT_OB_AUTH_DIGEST_CACHE_MGR_H_
