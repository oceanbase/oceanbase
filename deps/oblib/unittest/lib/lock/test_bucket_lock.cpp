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

#include <pthread.h>
#include "lib/utility/ob_template_utils.h"
#include "gtest/gtest.h"
#define private public
#include "lib/lock/ob_bucket_lock.h"

namespace oceanbase
{
namespace common
{

TEST(ObBucketLock, succeed_wrlock_all)
{
  ObBucketLock lock;
  const int64_t bucket_count = 1024;
  lock.init(bucket_count);

  ObBucketTryWLockAllGuard guard(lock);
  ASSERT_EQ(OB_SUCCESS, guard.get_ret());

  for (int64_t i = 0; i < bucket_count; ++i) {
    ASSERT_EQ(OB_EAGAIN, lock.try_rdlock(i));
    ASSERT_EQ(OB_EAGAIN, lock.try_wrlock(i));
  }
}

TEST(ObBucketLock, succeed_rdlock_all)
{
  ObBucketLock lock;
  const int64_t bucket_count = 1024;
  lock.init(bucket_count);

  ObBucketTryRLockAllGuard guard(lock);
  ASSERT_EQ(OB_SUCCESS, guard.get_ret());

  for (int64_t i = 0; i < bucket_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, lock.try_rdlock(i));
    ASSERT_EQ(OB_SUCCESS, lock.unlock(i));
    ASSERT_EQ(OB_EAGAIN, lock.try_wrlock(i));
  }
}

TEST(ObBucketLock, failed_lock_all_because_of_wlock)
{
  ObBucketLock lock;

  {
    ObBucketTryWLockAllGuard guard(lock);
    ASSERT_EQ(OB_NOT_INIT, guard.get_ret());
    ObBucketTryRLockAllGuard guard2(lock);
    ASSERT_EQ(OB_NOT_INIT, guard.get_ret());
  }

  lock.init(1024);

  {
    lock.wrlock(100);
    ObBucketTryWLockAllGuard guard(lock);
    ASSERT_EQ(OB_EAGAIN, guard.get_ret());
    ObBucketTryRLockAllGuard guard2(lock);
    ASSERT_EQ(OB_EAGAIN, guard.get_ret());
    lock.unlock(100);
  }

  {
    lock.wrlock(1023);
    ObBucketTryWLockAllGuard guard(lock);
    ASSERT_EQ(OB_EAGAIN, guard.get_ret());
    ObBucketTryRLockAllGuard guard2(lock);
    ASSERT_EQ(OB_EAGAIN, guard.get_ret());
    lock.unlock(1023);
  }

  {
    ObBucketTryWLockAllGuard guard(lock);
    ASSERT_EQ(OB_SUCCESS, guard.get_ret());
  }
  lock.wrlock(100);
  lock.unlock(100);

  {
    ObBucketTryRLockAllGuard guard2(lock);
    ASSERT_EQ(OB_SUCCESS, guard2.get_ret());
  }
  lock.wrlock(100);
  lock.unlock(100);
}

TEST(ObBucketLock, failed_lock_all_because_of_rlock)
{
  ObBucketLock lock;
  lock.init(1024);

  lock.rdlock(100);

  {
    ObBucketTryWLockAllGuard guard(lock);
    ASSERT_EQ(OB_EAGAIN, guard.get_ret());
  }

  {
    ObBucketTryRLockAllGuard guard(lock);
    ASSERT_EQ(OB_SUCCESS, guard.get_ret());
  }

  lock.unlock(100);
  {
    ObBucketTryWLockAllGuard guard(lock);
    ASSERT_EQ(OB_SUCCESS, guard.get_ret());
  }
  lock.rdlock(100);
  lock.unlock(100);
}

TEST(ObBucketLock, wlock_multi_buckets)
{
  ObBucketLock lock;
  ObArray<uint64_t> bucket_array;

  lock.init(1024);

  {
    ObMultiBucketLockGuard guard(lock, true/*is_write_lock*/);
    ObArray<uint64_t> hash_array;
    ASSERT_EQ(OB_SUCCESS, hash_array.push_back(1));
    ASSERT_EQ(OB_SUCCESS, bucket_array.push_back(1 % lock.get_bucket_count()));
    ASSERT_EQ(OB_SUCCESS, hash_array.push_back(300));
    ASSERT_EQ(OB_SUCCESS, bucket_array.push_back(300 % lock.get_bucket_count()));
    ASSERT_EQ(OB_SUCCESS, hash_array.push_back(200));
    ASSERT_EQ(OB_SUCCESS, bucket_array.push_back(200 % lock.get_bucket_count()));
    ASSERT_EQ(OB_SUCCESS, hash_array.push_back(301));
    ASSERT_EQ(OB_SUCCESS, bucket_array.push_back(301 % lock.get_bucket_count()));
    ASSERT_EQ(OB_SUCCESS, guard.lock_multi_buckets(hash_array));
    for (int64_t i = 0; i <  bucket_array.count(); ++i) {
      ASSERT_EQ(OB_EAGAIN, lock.try_rdlock(bucket_array.at(i)));
    }
  }

  for (int64_t i = 0; i < lock.get_bucket_count(); ++i) {
    COMMON_LOG(INFO, "try rdlock", K(i), K(lock));
    ASSERT_EQ(OB_SUCCESS, lock.try_rdlock(i));
    ASSERT_EQ(OB_SUCCESS, lock.unlock(i));
  }
}

TEST(ObBucketLock, rlock_multi_buckets)
{
  ObBucketLock lock;
  ObArray<uint64_t> bucket_array;

  lock.init(1024);

  {
    ObMultiBucketLockGuard guard(lock, false/*is_write_lock*/);
    ObArray<uint64_t> hash_array;
    ASSERT_EQ(OB_SUCCESS, hash_array.push_back(1));
    ASSERT_EQ(OB_SUCCESS, bucket_array.push_back(1 % lock.get_bucket_count()));
    ASSERT_EQ(OB_SUCCESS, hash_array.push_back(300));
    ASSERT_EQ(OB_SUCCESS, bucket_array.push_back(300 % lock.get_bucket_count()));
    ASSERT_EQ(OB_SUCCESS, hash_array.push_back(200));
    ASSERT_EQ(OB_SUCCESS, bucket_array.push_back(200 % lock.get_bucket_count()));
    ASSERT_EQ(OB_SUCCESS, hash_array.push_back(301));
    ASSERT_EQ(OB_SUCCESS, bucket_array.push_back(301 % lock.get_bucket_count()));
    ASSERT_EQ(OB_SUCCESS, guard.lock_multi_buckets(hash_array));
    for (int64_t i = 0; i <  bucket_array.count(); ++i) {
      ASSERT_EQ(OB_EAGAIN, lock.try_wrlock(bucket_array.at(i)));
    }
    for (int64_t i = 0; i <  bucket_array.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, lock.try_rdlock(bucket_array.at(i)));
      ASSERT_EQ(OB_SUCCESS, lock.unlock(bucket_array.at(i)));
    }
  }

  for (int64_t i = 0; i < lock.get_bucket_count(); ++i) {
    COMMON_LOG(INFO, "try rdlock", K(i), K(lock));
    ASSERT_EQ(OB_SUCCESS, lock.try_wrlock(i));
    ASSERT_EQ(OB_SUCCESS, lock.unlock(i));
  }
}


TEST(ObBucketLock, hash_rlock)
{
  ObBucketLock lock;
  lock.init(1024);

  {
    ObBucketHashRLockGuard guard(lock, 1);
    ASSERT_EQ(OB_SUCCESS, guard.get_ret());
    ObBucketHashRLockGuard guard2(lock, 1);
    ASSERT_EQ(OB_SUCCESS, guard2.get_ret());
    ASSERT_EQ(OB_EAGAIN, lock.try_wrlock(1 % lock.get_bucket_count()));
  }
  {
    ObBucketHashWLockGuard guard4(lock, 1);
    ASSERT_EQ(OB_SUCCESS, guard4.get_ret());
  }
}


TEST(ObBucketLock, hash_wlock)
{
  ObBucketLock lock;
  lock.init(1024);

  {
    ObBucketHashWLockGuard guard(lock, 1);
    ASSERT_EQ(OB_SUCCESS, guard.get_ret());
    ASSERT_EQ(OB_EAGAIN, lock.try_rdlock(1 % lock.get_bucket_count()));
    ASSERT_EQ(OB_EAGAIN, lock.try_wrlock(1 % lock.get_bucket_count()));
  }
  {
    ObBucketHashWLockGuard guard4(lock, 1);
    ASSERT_EQ(OB_SUCCESS, guard4.get_ret());
  }
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
