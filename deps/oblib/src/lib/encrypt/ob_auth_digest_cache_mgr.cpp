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

#define USING_LOG_PREFIX COMMON

#include "lib/encrypt/ob_auth_digest_cache_mgr.h"
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace common
{

ObAuthDigestCacheMgr &ObAuthDigestCacheMgr::get_instance()
{
  static ObAuthDigestCacheMgr instance;
  return instance;
}

int ObAuthDigestCacheMgr::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObAuthDigestCacheMgr has already been initialized", K(ret));
  } else {
    // Initialize cache
    // Set priority to 1000 to ensure it won't be easily evicted
    const int64_t priority = 1000;
    if (OB_FAIL(cache_.init("auth_digest_cache", priority))) {
      LOG_WARN("failed to init auth digest cache", K(ret));
    } else {
      is_inited_ = true;
      LOG_INFO("ObAuthDigestCacheMgr initialized successfully");
    }
  }

  return ret;
}

void ObAuthDigestCacheMgr::destroy()
{
  if (is_inited_) {
    cache_.destroy();
    is_inited_ = false;
    LOG_INFO("ObAuthDigestCacheMgr destroyed");
  }
}

int ObAuthDigestCacheMgr::get_digest(
    const ObString &user_name,
    const ObString &host_name,
    uint64_t tenant_id,
    int64_t password_last_changed_timestamp,
    ObAuthDigestHandle &handle)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAuthDigestCacheMgr not initialized", K(ret));
  } else if (user_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(user_name), K(host_name),
             K(tenant_id), K(password_last_changed_timestamp));
  } else {
    // Construct cache key (includes password_last_changed_timestamp)
    ObAuthDigestKey key(user_name, host_name, tenant_id,
                        password_last_changed_timestamp);

    // Get from cache
    if (OB_FAIL(cache_.get_row(key, handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get digest from cache", K(ret), K(key));
      } else {
        LOG_TRACE("digest not found in cache", K(user_name), K(host_name),
                  K(tenant_id), K(password_last_changed_timestamp));
      }
    } else {
      LOG_TRACE("digest found in cache", K(user_name), K(host_name),
                K(tenant_id), K(password_last_changed_timestamp));
    }
  }

  return ret;
}

int ObAuthDigestCacheMgr::put_digest(
    const ObString &user_name,
    const ObString &host_name,
    uint64_t tenant_id,
    int64_t password_last_changed_timestamp,
    const unsigned char *digest,
    int64_t digest_len)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAuthDigestCacheMgr not initialized", K(ret));
  } else if (user_name.empty() || OB_ISNULL(digest) ||
             digest_len != OB_CRYPT_RAW_DIGEST_LEN32) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(user_name), K(host_name),
             K(tenant_id), K(password_last_changed_timestamp), KP(digest), K(digest_len));
  } else {
    // Construct cache key (includes password_last_changed_timestamp)
    ObAuthDigestKey key(user_name, host_name, tenant_id,
                        password_last_changed_timestamp);

    // Construct cache value
    ObAuthDigest value;
    if (OB_FAIL(value.set_digest(reinterpret_cast<const char*>(digest), digest_len))) {
      LOG_WARN("failed to set digest", K(ret), K(digest_len));
    } else if (OB_FAIL(cache_.put_row(key, value))) {
      LOG_WARN("failed to put digest to cache", K(ret), K(key));
    } else {
      LOG_INFO("successfully put digest to cache", K(user_name), K(host_name),
               K(tenant_id), K(password_last_changed_timestamp));
    }
  }

  return ret;
}

} // namespace common
} // namespace oceanbase
