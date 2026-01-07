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

#define USING_LOG_PREFIX COMMON
#include <gtest/gtest.h>
#include "lib/encrypt/ob_caching_sha2_cache.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/ob_thread_mgr.h"
#include <openssl/sha.h>
#include <cstring>

using namespace oceanbase::common;

namespace oceanbase
{
namespace common
{

// Simple memory limit getter
class TestMemLimitGetter : public ObITenantMemLimitGetter
{
public:
  virtual bool has_tenant(const uint64_t tenant_id) const override
  {
    UNUSED(tenant_id);
    return true;
  }

  virtual int get_tenant_mem_limit(const uint64_t tenant_id,
                                   int64_t &lower_limit,
                                   int64_t &upper_limit) const override
  {
    UNUSED(tenant_id);
    lower_limit = 0;
    upper_limit = 1024L * 1024L * 1024L; // 1GB
    return OB_SUCCESS;
  }

  virtual int get_all_tenant_id(common::ObIArray<uint64_t> &tenant_ids) const override
  {
    UNUSED(tenant_ids);
    return OB_SUCCESS;
  }
};

class TestCachingSha2Cache : public ::testing::Test
{
public:
  TestCachingSha2Cache() {}
  virtual ~TestCachingSha2Cache() {}

  static void SetUpTestCase()
  {
    // Initialize timer service, which ObKVGlobalCache needs to run background tasks
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  }

  static void TearDownTestCase()
  {
    // Stop and destroy timer service
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }

  virtual void SetUp()
  {
    // Initialize global KV Cache
    int ret = ObKVGlobalCache::get_instance().init(&mem_limit_getter_);
    if (OB_INIT_TWICE == ret) {
      ret = OB_SUCCESS;  // Ignore error if already initialized
    }
    ASSERT_EQ(OB_SUCCESS, ret);

    // Initialize cache
    ASSERT_EQ(OB_SUCCESS, cache_.init("test_caching_sha2_cache", 1));
  }

  virtual void TearDown()
  {
    // Only destroy cache instance
    // Do not manually destroy global cache, as its destructor will call destroy() automatically
    cache_.destroy();
  }

protected:
  ObCachingSha2Cache cache_;
  ObArenaAllocator allocator_;
  TestMemLimitGetter mem_limit_getter_;
};

// Test basic functionality of ObCachingSha2Digest
TEST_F(TestCachingSha2Cache, test_digest_basic)
{
  ObCachingSha2Digest digest;

  // Test initial state
  ASSERT_EQ(0, digest.get_digest_len());

  // Create a test digest (32-byte SHA256 digest)
  char test_digest[OB_SHA256_DIGEST_LENGTH];
  for (int i = 0; i < OB_SHA256_DIGEST_LENGTH; i++) {
    test_digest[i] = static_cast<char>(i);
  }

  // Set digest
  ASSERT_EQ(OB_SUCCESS, digest.set_digest(test_digest, OB_SHA256_DIGEST_LENGTH));
  ASSERT_EQ(OB_SHA256_DIGEST_LENGTH, digest.get_digest_len());

  // Validate digest content
  const char *result = digest.get_digest();
  ASSERT_NE(nullptr, result);
  ASSERT_EQ(0, MEMCMP(result, test_digest, OB_SHA256_DIGEST_LENGTH));

  // Test invalid arguments
  ASSERT_EQ(OB_INVALID_ARGUMENT, digest.set_digest(nullptr, OB_SHA256_DIGEST_LENGTH));
  ASSERT_EQ(OB_INVALID_ARGUMENT, digest.set_digest(test_digest, 16)); // Incorrect length
}

// Test deep copy of ObCachingSha2Digest
TEST_F(TestCachingSha2Cache, test_digest_deep_copy)
{
  ObCachingSha2Digest digest1;

  // Create test digest
  char test_digest[OB_SHA256_DIGEST_LENGTH];
  for (int i = 0; i < OB_SHA256_DIGEST_LENGTH; i++) {
    test_digest[i] = static_cast<char>(i);
  }

  // Set digest1
  ASSERT_EQ(OB_SUCCESS, digest1.set_digest(test_digest, OB_SHA256_DIGEST_LENGTH));

  // Allocate buffer
  int64_t buf_size = digest1.size();
  char *buf = (char *)allocator_.alloc(buf_size);
  ASSERT_NE(nullptr, buf);

  // Deep copy
  ObIKVCacheValue *digest2_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, digest1.deep_copy(buf, buf_size, digest2_ptr));
  ASSERT_NE(nullptr, digest2_ptr);

  ObCachingSha2Digest *digest2 = reinterpret_cast<ObCachingSha2Digest*>(digest2_ptr);

  // Validate copy result
  ASSERT_EQ(digest1.get_digest_len(), digest2->get_digest_len());
  ASSERT_EQ(0, MEMCMP(digest1.get_digest(), digest2->get_digest(), OB_SHA256_DIGEST_LENGTH));
}

// Test basic functionality of ObCachingSha2Key
TEST_F(TestCachingSha2Cache, test_key_basic)
{
  ObString user_name("test_user");
  ObString host_name("localhost");

  ObCachingSha2Key key1(user_name, host_name, 1, 0);

  // Test getter
  ASSERT_EQ(user_name, key1.get_user_name());
  ASSERT_EQ(host_name, key1.get_host_name());

  // Test is_valid
  ASSERT_TRUE(key1.is_valid());

  ObCachingSha2Key key2;
  ASSERT_FALSE(key2.is_valid()); // An empty user_name is invalid

  // Test setter
  key2.set_user_name(user_name);
  key2.set_host_name(host_name);
  ASSERT_TRUE(key2.is_valid());
  ASSERT_EQ(user_name, key2.get_user_name());
  ASSERT_EQ(host_name, key2.get_host_name());
}

// Test hash functionality of ObCachingSha2Key
TEST_F(TestCachingSha2Cache, test_key_hash)
{
  ObString user_name1("user1");
  ObString host_name1("host1");
  ObString user_name2("user2");
  ObString host_name2("host2");

  ObCachingSha2Key key1(user_name1, host_name1, 1, 0);
  ObCachingSha2Key key2(user_name1, host_name1, 1, 0);
  ObCachingSha2Key key3(user_name2, host_name2, 1, 0);

  // Keys that are the same should have the same hash value
  ASSERT_EQ(key1.hash(), key2.hash());

  // Keys that are different should have different hash values (most likely)
  ASSERT_NE(key1.hash(), key3.hash());

  // Test hash method
  uint64_t hash_val1, hash_val2;
  ASSERT_EQ(OB_SUCCESS, key1.hash(hash_val1));
  ASSERT_EQ(OB_SUCCESS, key2.hash(hash_val2));
  ASSERT_EQ(hash_val1, hash_val2);
}

// Test equality comparison of ObCachingSha2Key
TEST_F(TestCachingSha2Cache, test_key_equality)
{
  ObString user_name1("user1");
  ObString host_name1("host1");
  ObString user_name2("user2");
  ObString host_name2("host2");

  ObCachingSha2Key key1(user_name1, host_name1, 1, 0);
  ObCachingSha2Key key2(user_name1, host_name1, 1, 0);
  ObCachingSha2Key key3(user_name2, host_name2, 1, 0);

  // Test equality
  ASSERT_TRUE(key1 == key2);
  ASSERT_FALSE(key1 == key3);
}

// Test deep copy of ObCachingSha2Key
TEST_F(TestCachingSha2Cache, test_key_deep_copy)
{
  ObString user_name("test_user");
  ObString host_name("localhost");

  ObCachingSha2Key key1(user_name, host_name, 1, 0);

  // Allocate buffer
  int64_t buf_size = key1.size();
  char *buf = (char *)allocator_.alloc(buf_size);
  ASSERT_NE(nullptr, buf);

  // Deep copy
  ObIKVCacheKey *key2_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, key1.deep_copy(buf, buf_size, key2_ptr));
  ASSERT_NE(nullptr, key2_ptr);

  ObCachingSha2Key *key2 = reinterpret_cast<ObCachingSha2Key*>(key2_ptr);

  // Validate copy result
  ASSERT_EQ(key1.get_user_name(), key2->get_user_name());
  ASSERT_EQ(key1.get_host_name(), key2->get_host_name());
  ASSERT_TRUE(key1 == *key2);
}

// Test put and get operations of the cache
TEST_F(TestCachingSha2Cache, test_cache_put_get)
{
  // Create key
  ObString user_name("test_user");
  ObString host_name("localhost");
  ObCachingSha2Key key(user_name, host_name, 1, 0);

  // Create digest
  ObCachingSha2Digest digest;
  char test_digest[OB_SHA256_DIGEST_LENGTH];
  for (int i = 0; i < OB_SHA256_DIGEST_LENGTH; i++) {
    test_digest[i] = static_cast<char>(i * 2);
  }
  ASSERT_EQ(OB_SUCCESS, digest.set_digest(test_digest, OB_SHA256_DIGEST_LENGTH));

  // Put into cache
  ASSERT_EQ(OB_SUCCESS, cache_.put_row(key, digest));

  // Get from cache
  ObCachingSha2Handle handle;
  ASSERT_EQ(OB_SUCCESS, cache_.get_row(key, handle));
  ASSERT_NE(nullptr, handle.digest_);

  // Validate retrieved digest
  ASSERT_EQ(OB_SHA256_DIGEST_LENGTH, handle.digest_->get_digest_len());
  ASSERT_EQ(0, MEMCMP(handle.digest_->get_digest(), test_digest, OB_SHA256_DIGEST_LENGTH));
}

// Test get for non-existent key from the cache
TEST_F(TestCachingSha2Cache, test_cache_get_nonexistent)
{
  ObString user_name("nonexistent_user");
  ObString host_name("nonexistent_host");
  ObCachingSha2Key key(user_name, host_name, 1, 0);

  ObCachingSha2Handle handle;
  int ret = cache_.get_row(key, handle);

  // Should return OB_ENTRY_NOT_EXIST
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// Test put_and_fetch operation of the cache
TEST_F(TestCachingSha2Cache, test_cache_put_and_fetch)
{
  // Create key
  ObString user_name("test_user2");
  ObString host_name("localhost2");
  ObCachingSha2Key key(user_name, host_name, 1, 0);

  // Create digest
  ObCachingSha2Digest digest;
  char test_digest[OB_SHA256_DIGEST_LENGTH];
  for (int i = 0; i < OB_SHA256_DIGEST_LENGTH; i++) {
    test_digest[i] = static_cast<char>(i * 3);
  }
  ASSERT_EQ(OB_SUCCESS, digest.set_digest(test_digest, OB_SHA256_DIGEST_LENGTH));

  // Put and fetch
  ObCachingSha2Handle handle;
  ASSERT_EQ(OB_SUCCESS, cache_.put_and_fetch_row(key, digest, handle));
  ASSERT_NE(nullptr, handle.digest_);

  // Validate retrieved digest
  ASSERT_EQ(OB_SHA256_DIGEST_LENGTH, handle.digest_->get_digest_len());
  ASSERT_EQ(0, MEMCMP(handle.digest_->get_digest(), test_digest, OB_SHA256_DIGEST_LENGTH));
}

// Test overwrite operation of the cache
TEST_F(TestCachingSha2Cache, test_cache_overwrite)
{
  // Create key
  ObString user_name("test_user3");
  ObString host_name("localhost3");
  ObCachingSha2Key key(user_name, host_name, 1, 0);

  // First put
  ObCachingSha2Digest digest1;
  char test_digest1[OB_SHA256_DIGEST_LENGTH];
  for (int i = 0; i < OB_SHA256_DIGEST_LENGTH; i++) {
    test_digest1[i] = static_cast<char>(i);
  }
  ASSERT_EQ(OB_SUCCESS, digest1.set_digest(test_digest1, OB_SHA256_DIGEST_LENGTH));
  ASSERT_EQ(OB_SUCCESS, cache_.put_row(key, digest1));

  // Second put (overwrite)
  ObCachingSha2Digest digest2;
  char test_digest2[OB_SHA256_DIGEST_LENGTH];
  for (int i = 0; i < OB_SHA256_DIGEST_LENGTH; i++) {
    test_digest2[i] = static_cast<char>(i + 100);
  }
  ASSERT_EQ(OB_SUCCESS, digest2.set_digest(test_digest2, OB_SHA256_DIGEST_LENGTH));
  ASSERT_EQ(OB_SUCCESS, cache_.put_row(key, digest2));

  // Get should retrieve the second value
  ObCachingSha2Handle handle;
  ASSERT_EQ(OB_SUCCESS, cache_.get_row(key, handle));
  ASSERT_NE(nullptr, handle.digest_);
  ASSERT_EQ(0, MEMCMP(handle.digest_->get_digest(), test_digest2, OB_SHA256_DIGEST_LENGTH));
}

// Test cache for multiple different users
TEST_F(TestCachingSha2Cache, test_cache_multiple_users)
{
  const int num_users = 5;
  ObCachingSha2Key keys[num_users];
  ObCachingSha2Digest digests[num_users];
  char test_digests[num_users][OB_SHA256_DIGEST_LENGTH];
  char user_bufs[num_users][32];  // Keep the string valid for the whole test
  char host_bufs[num_users][32];

  // Create data for multiple users
  for (int i = 0; i < num_users; i++) {
    snprintf(user_bufs[i], sizeof(user_bufs[i]), "user_%d", i);
    snprintf(host_bufs[i], sizeof(host_bufs[i]), "host_%d", i);

    ObString user_name(user_bufs[i]);
    ObString host_name(host_bufs[i]);
    keys[i] = ObCachingSha2Key(user_name, host_name, 1, 0);

    // Generate different digests
    for (int j = 0; j < OB_SHA256_DIGEST_LENGTH; j++) {
      test_digests[i][j] = static_cast<char>(i * 10 + j);
    }
    ASSERT_EQ(OB_SUCCESS, digests[i].set_digest(test_digests[i], OB_SHA256_DIGEST_LENGTH));

    // Put into cache
    ASSERT_EQ(OB_SUCCESS, cache_.put_row(keys[i], digests[i]));
  }

  // Validate each user's data
  for (int i = 0; i < num_users; i++) {
    ObCachingSha2Handle handle;
    ASSERT_EQ(OB_SUCCESS, cache_.get_row(keys[i], handle));
    ASSERT_NE(nullptr, handle.digest_);
    ASSERT_EQ(0, MEMCMP(handle.digest_->get_digest(), test_digests[i], OB_SHA256_DIGEST_LENGTH));
  }
}

// Test assignment operation of Handle
TEST_F(TestCachingSha2Cache, test_handle_assign)
{
  // Create and put into cache
  ObString user_name("test_user");
  ObString host_name("localhost");
  ObCachingSha2Key key(user_name, host_name, 1, 0);

  ObCachingSha2Digest digest;
  char test_digest[OB_SHA256_DIGEST_LENGTH];
  for (int i = 0; i < OB_SHA256_DIGEST_LENGTH; i++) {
    test_digest[i] = static_cast<char>(i);
  }
  ASSERT_EQ(OB_SUCCESS, digest.set_digest(test_digest, OB_SHA256_DIGEST_LENGTH));
  ASSERT_EQ(OB_SUCCESS, cache_.put_row(key, digest));

  // Get handle1
  ObCachingSha2Handle handle1;
  ASSERT_EQ(OB_SUCCESS, cache_.get_row(key, handle1));
  ASSERT_NE(nullptr, handle1.digest_);

  // Assign to handle2
  ObCachingSha2Handle handle2;
  ASSERT_EQ(OB_SUCCESS, handle2.assign(handle1));
  ASSERT_NE(nullptr, handle2.digest_);

  // Validate both handles point to the same data
  ASSERT_EQ(handle1.digest_, handle2.digest_);
  ASSERT_EQ(0, MEMCMP(handle1.digest_->get_digest(), handle2.digest_->get_digest(),
                      OB_SHA256_DIGEST_LENGTH));
}

// Test move operation of Handle
TEST_F(TestCachingSha2Cache, test_handle_move)
{
  // Create and put into cache
  ObString user_name("test_user");
  ObString host_name("localhost");
  ObCachingSha2Key key(user_name, host_name, 1, 0);

  ObCachingSha2Digest digest;
  char test_digest[OB_SHA256_DIGEST_LENGTH];
  for (int i = 0; i < OB_SHA256_DIGEST_LENGTH; i++) {
    test_digest[i] = static_cast<char>(i);
  }
  ASSERT_EQ(OB_SUCCESS, digest.set_digest(test_digest, OB_SHA256_DIGEST_LENGTH));
  ASSERT_EQ(OB_SUCCESS, cache_.put_row(key, digest));

  // Get handle1
  ObCachingSha2Handle handle1;
  ASSERT_EQ(OB_SUCCESS, cache_.get_row(key, handle1));
  ASSERT_NE(nullptr, handle1.digest_);

  const ObCachingSha2Digest *digest_ptr = handle1.digest_;

  // Move to handle2
  ObCachingSha2Handle handle2;
  handle2.move_from(handle1);

  // Validate state after move
  ASSERT_NE(nullptr, handle2.digest_);
  ASSERT_EQ(digest_ptr, handle2.digest_);
  ASSERT_EQ(nullptr, handle1.digest_);  // handle1 should be cleared after move
}

} // end of namespace common
} // end of namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
