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

#include "lib/encrypt/ob_caching_sha2_cache_mgr.h"
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace common
{

ObCachingSha2CacheMgr &ObCachingSha2CacheMgr::get_instance()
{
  static ObCachingSha2CacheMgr instance;
  return instance;
}

int ObCachingSha2CacheMgr::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCachingSha2CacheMgr has already been initialized", K(ret));
  } else {
    // Initialize cache
    // Set priority to 1000 to ensure it won't be easily evicted
    const int64_t priority = 1000;
    if (OB_FAIL(cache_.init("caching_sha2_cache", priority))) {
      LOG_WARN("failed to init caching sha2 cache", K(ret));
    } else {
      is_inited_ = true;
      LOG_INFO("ObCachingSha2CacheMgr initialized successfully");
    }
  }

  return ret;
}

void ObCachingSha2CacheMgr::destroy()
{
  if (is_inited_) {
    cache_.destroy();
    is_inited_ = false;
    LOG_INFO("ObCachingSha2CacheMgr destroyed");
  }
}

int ObCachingSha2CacheMgr::get_digest(
    const ObString &user_name,
    const ObString &host_name,
    uint64_t tenant_id,
    int64_t password_last_changed_timestamp,
    ObCachingSha2Handle &handle)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCachingSha2CacheMgr not initialized", K(ret));
  } else if (user_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(user_name), K(host_name),
             K(tenant_id), K(password_last_changed_timestamp));
  } else {
    // Construct cache key (includes password_last_changed_timestamp)
    ObCachingSha2Key key(user_name, host_name, tenant_id, password_last_changed_timestamp);

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

int ObCachingSha2CacheMgr::put_digest(
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
    LOG_WARN("ObCachingSha2CacheMgr not initialized", K(ret));
  } else if (user_name.empty() || OB_ISNULL(digest) ||
             digest_len != OB_SHA256_DIGEST_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(user_name), K(host_name),
             K(tenant_id), K(password_last_changed_timestamp), KP(digest), K(digest_len));
  } else {
    // Construct cache key (includes password_last_changed_timestamp)
    ObCachingSha2Key key(user_name, host_name, tenant_id, password_last_changed_timestamp);

    // Construct cache value
    ObCachingSha2Digest value;
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
