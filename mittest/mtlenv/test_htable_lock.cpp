/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#include "observer/table/ob_htable_lock_mgr.h"
#include "lib/utility/ob_test_util.h"
#include "share/ob_thread_pool.h"
#include "mock_tenant_module_env.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::transaction;
using namespace oceanbase::share;

class TestHTableLock: public ::testing::Test
{
public:
  TestHTableLock() {}
  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }
  void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }
  virtual ~TestHTableLock() {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestHTableLock);
};

class TestConcurrentHTableLock  : public ObThreadPool
{
public:
  explicit TestConcurrentHTableLock(ObHTableLockMgr* mgr, const int64_t thread_cnt, const uint64_t table_id, const ObString &key, ObHTableLockMode mode, bool can_retry = false)
  : mgr(mgr),
    thread_cnt_(thread_cnt),
    table_id_(table_id),
    key_(key),
    mode_(mode),
    fake_addr_(),
    can_retry_(can_retry)
  {
    set_thread_count(thread_cnt);
  }
  virtual ~TestConcurrentHTableLock() {};
  virtual void run1();
  virtual void process() = 0;

private:
  ObHTableLockMgr* mgr;
  const int64_t thread_cnt_;
  const uint64_t table_id_;
  const ObString key_;
  ObHTableLockMode mode_;
  ObAddr fake_addr_;
  bool can_retry_;
};

void TestConcurrentHTableLock::run1()
{
  ObArenaAllocator alloc;
  ObTransID fake_tx_id;
  ObString local_key;
  ObHTableLockHandle *lock_handle = nullptr;
  ASSERT_EQ(OB_SUCCESS, ob_write_string(alloc, key_, local_key));
  ASSERT_EQ(OB_SUCCESS, mgr->acquire_handle(fake_tx_id, lock_handle));
  int ret = OB_SUCCESS;
  ret = mgr->lock_row(table_id_, local_key, mode_, *lock_handle);
  if (can_retry_) {
    while (ret == OB_TRY_LOCK_ROW_CONFLICT) {
      ret = mgr->lock_row(table_id_, local_key, mode_, *lock_handle);
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  process();
  ASSERT_EQ(OB_SUCCESS, mgr->release_handle(*lock_handle));
}

class TestConcurrentHTableSLock  : public TestConcurrentHTableLock
{
public:
  TestConcurrentHTableSLock(ObHTableLockMgr* mgr, const int64_t thread_cnt, const uint64_t table_id, const ObString &key, bool can_retry = false)
  : TestConcurrentHTableLock(mgr, thread_cnt, table_id, key, ObHTableLockMode::SHARED, can_retry)
  {}
  virtual ~TestConcurrentHTableSLock() {};
  virtual void process() override;
};

void TestConcurrentHTableSLock::process()
{
  usleep(1*1000); // 1ms, for processing
}

class TestConcurrentHTableXLock  : public TestConcurrentHTableLock
{
public:
  TestConcurrentHTableXLock(ObHTableLockMgr* mgr, const int64_t thread_cnt, const uint64_t table_id, const ObString &key)
    : TestConcurrentHTableLock(mgr, thread_cnt, table_id, key, ObHTableLockMode::EXCLUSIVE, true),
      value_(0)
  {}
  virtual ~TestConcurrentHTableXLock() {};
  virtual void process() override;
  void check_result();

private:
  uint64_t value_;
};

void TestConcurrentHTableXLock::process()
{
  uint64_t value = value_;
  usleep(1*1000); // 1ms, for processing
  value_ = value + 1;
}

void TestConcurrentHTableXLock::check_result()
{
  ASSERT_EQ(value_, thread_cnt_);
}

TEST_F(TestHTableLock, basic_test)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTransID fake_tx_id;
  ObHTableLockHandle *lock_handle = nullptr;
  uint64_t fake_table_id = 1;
  ObString fake_key = ObString::make_string("k1");
  // 1. acquire htable lock handle for a running tx
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id, lock_handle));
  ASSERT_TRUE(lock_handle != nullptr);
  // 2. add htable lock on htable row in demand during tx
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle));
  // 3. release htable lock handle after transaction commit or rollback
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle));
}

TEST_F(TestHTableLock, lock_different_keys)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTransID fake_tx_id;
  ObHTableLockHandle *lock_handle = nullptr;
  uint64_t fake_table_id = 1;
  ObString fake_key1 = ObString::make_string("k1");
  ObString fake_key2 = ObString::make_string("k2");
  ObHTableLockKey lock_key1(fake_table_id, fake_key1);
  ObHTableLockKey lock_key2(fake_table_id, fake_key2);
  ObHTableLockKey *tmp_lock_key = nullptr;
  ObHTableLock *tmp_lock = nullptr;

  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id, lock_handle));
  ASSERT_TRUE(lock_handle != nullptr);
  ASSERT_EQ(fake_tx_id, lock_handle->tx_id_);
  ASSERT_EQ(nullptr, lock_handle->lock_nodes_);

  // lock fake_key1 in share mode
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key1, ObHTableLockMode::SHARED, *lock_handle));
  // check lock handle
  ASSERT_EQ(ObHTableLockMode::SHARED, lock_handle->lock_nodes_->lock_mode_);
  ASSERT_EQ(fake_table_id, lock_handle->lock_nodes_->lock_key_->table_id_);
  ASSERT_EQ(fake_key1, lock_handle->lock_nodes_->lock_key_->key_);
  // check lock map
  ASSERT_EQ(1, HTABLE_LOCK_MGR->lock_map_.size());
  ObHTableLockMgr::ObHTableLockMap::const_iterator it = HTABLE_LOCK_MGR->lock_map_.begin();
  tmp_lock_key = it->first;
  ASSERT_TRUE(tmp_lock_key != nullptr);
  ASSERT_EQ(lock_key1, *tmp_lock_key);
  ASSERT_TRUE(tmp_lock_key->key_.ptr() != fake_key1.ptr()); // ensure it's deep copy
  ASSERT_EQ(lock_handle->lock_nodes_->lock_key_->key_.ptr(), tmp_lock_key->key_.ptr());
  tmp_lock = it->second;
  ASSERT_TRUE(tmp_lock->is_locked());
  ASSERT_TRUE(tmp_lock->is_rdlocked());
  ASSERT_TRUE(tmp_lock->can_escalate_lock());
  ASSERT_TRUE(!tmp_lock->is_wrlocked());

  // lock fake_key2 in exclusive mode
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key2, ObHTableLockMode::EXCLUSIVE, *lock_handle));
  // check lock handle
  ASSERT_EQ(ObHTableLockMode::EXCLUSIVE, lock_handle->lock_nodes_->lock_mode_);
  ASSERT_EQ(fake_table_id, lock_handle->lock_nodes_->lock_key_->table_id_);
  ASSERT_EQ(fake_key2, lock_handle->lock_nodes_->lock_key_->key_);
  ASSERT_EQ(ObHTableLockMode::SHARED, lock_handle->lock_nodes_->next_->lock_mode_);
  ASSERT_EQ(fake_table_id, lock_handle->lock_nodes_->next_->lock_key_->table_id_);
  ASSERT_EQ(fake_key1, lock_handle->lock_nodes_->next_->lock_key_->key_);
  // check lock map
  tmp_lock = nullptr;
  ASSERT_EQ(2, HTABLE_LOCK_MGR->lock_map_.size());
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_map_.get_refactored(&lock_key1, tmp_lock));
  ASSERT_TRUE(tmp_lock != nullptr);
  ASSERT_TRUE(tmp_lock->is_locked());
  ASSERT_TRUE(tmp_lock->is_rdlocked());
  ASSERT_TRUE(tmp_lock->can_escalate_lock());
  ASSERT_TRUE(!tmp_lock->is_wrlocked());
  tmp_lock = nullptr;
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_map_.get_refactored(&lock_key2, tmp_lock));
  ASSERT_TRUE(tmp_lock != nullptr);
  ASSERT_TRUE(tmp_lock->is_locked());
  ASSERT_TRUE(!tmp_lock->is_rdlocked());
  ASSERT_TRUE(!tmp_lock->can_escalate_lock());
  ASSERT_TRUE(tmp_lock->is_wrlocked());

  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle));
}

TEST_F(TestHTableLock, share_share)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTransID fake_tx_id1(1);
  ObTransID fake_tx_id2(2);
  ObHTableLockHandle *lock_handle1 = nullptr;
  ObHTableLockHandle *lock_handle2 = nullptr;
  uint64_t fake_table_id = 1;
  ObString fake_key = ObString::make_string("k1");
  ObHTableLockKey lock_key(fake_table_id, fake_key);
  ObHTableLockKey *tmp_lock_key = nullptr;
  ObHTableLock *tmp_lock = nullptr;

  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id1, lock_handle1));
  ASSERT_TRUE(lock_handle1 != nullptr);
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id2, lock_handle2));
  ASSERT_TRUE(lock_handle2 != nullptr);

  // two lock handles try to add share lock on one row concurrently return success
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle1));
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle2));

  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle1));
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle2));
}

TEST_F(TestHTableLock, share_exclusive)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTransID fake_tx_id1(1);
  ObTransID fake_tx_id2(2);
  ObHTableLockHandle *lock_handle1 = nullptr;
  ObHTableLockHandle *lock_handle2 = nullptr;
  uint64_t fake_table_id = 1;
  ObString fake_key = ObString::make_string("k1");
  ObHTableLockKey lock_key(fake_table_id, fake_key);
  ObHTableLockKey *tmp_lock_key = nullptr;
  ObHTableLock *tmp_lock = nullptr;

  // acquire lock handle1 and lock handle2
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id1, lock_handle1));
  ASSERT_TRUE(lock_handle1 != nullptr);
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id2, lock_handle2));
  ASSERT_TRUE(lock_handle2 != nullptr);

  // lock handle1 add share lock, and lock handle2 add exclusive lock
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle1));
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::EXCLUSIVE, *lock_handle2));

  // release lock handle1 and handle2 and acquire again
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle1));
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle2));
  lock_handle1 = nullptr;
  lock_handle2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id1, lock_handle1));
  ASSERT_TRUE(lock_handle1 != nullptr);
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id2, lock_handle2));
  ASSERT_TRUE(lock_handle2 != nullptr);

  // lock handle1 add exclusive lock, and lock handle2 add share lock
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::EXCLUSIVE, *lock_handle1));
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle2));

  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle1));
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle2));
}

TEST_F(TestHTableLock, exclusive_exclusive)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTransID fake_tx_id1(1);
  ObTransID fake_tx_id2(2);
  ObHTableLockHandle *lock_handle1 = nullptr;
  ObHTableLockHandle *lock_handle2 = nullptr;
  uint64_t fake_table_id = 1;
  ObString fake_key = ObString::make_string("k1");
  ObHTableLockKey lock_key(fake_table_id, fake_key);
  ObHTableLockKey *tmp_lock_key = nullptr;
  ObHTableLock *tmp_lock = nullptr;

  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id1, lock_handle1));
  ASSERT_TRUE(lock_handle1 != nullptr);
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id2, lock_handle2));
  ASSERT_TRUE(lock_handle2 != nullptr);

  // lock handle1 add exclusive lock, and lock handle2 add exclusive lock
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::EXCLUSIVE, *lock_handle1));
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::EXCLUSIVE, *lock_handle2));

  // release lock handle1
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle1));
  // lock handle2 try to add exclusive lock again
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::EXCLUSIVE, *lock_handle2));
  // release lock handle2
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle2));
}


TEST_F(TestHTableLock, repeat_lock_one_row)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ObTransID fake_tx_id;
  ObHTableLockHandle *lock_handle = nullptr;
  ObHTableLockHandle *lock_handle2 = nullptr;
  uint64_t fake_table_id = 1;
  ObString fake_key = ObString::make_string("k1");
  ObHTableLockKey lock_key(fake_table_id, fake_key);
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id, lock_handle));
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id, lock_handle2));
  ASSERT_TRUE(lock_handle != nullptr);
  ASSERT_TRUE(lock_handle2 != nullptr);

  // add intial share lock
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle));

  // 1. share -> share, noting changed
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle));
  ASSERT_TRUE(lock_handle->lock_nodes_ != nullptr);
  ASSERT_EQ(ObHTableLockMode::SHARED, lock_handle->lock_nodes_->lock_mode_);
  ASSERT_EQ(nullptr, lock_handle->lock_nodes_->next_);
  ASSERT_EQ(lock_key, *lock_handle->lock_nodes_->lock_key_);
  // the others try to add lock
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::EXCLUSIVE, *lock_handle2));
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle2));

  // 2. share -> exclusive, will escalate share lock to exclusive lock when no one else holds the lock
  // lock handle2 hold the share lock too, escalate lock failed
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::EXCLUSIVE, *lock_handle));
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle2));
  lock_handle2 = nullptr;
  // lock handle2 release the shared lock, escalate lock success
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::EXCLUSIVE, *lock_handle));
  ASSERT_EQ(ObHTableLockMode::EXCLUSIVE, lock_handle->lock_nodes_->lock_mode_);
  ASSERT_EQ(nullptr, lock_handle->lock_nodes_->next_);
  ASSERT_EQ(lock_key, *lock_handle->lock_nodes_->lock_key_);
  // the others try to add lock
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->acquire_handle(fake_tx_id, lock_handle2));
  ASSERT_TRUE(lock_handle2 != nullptr);
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::EXCLUSIVE, *lock_handle2));
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle2));

  // 3. exclusive -> share, still hold exclusive lock
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle));
  ASSERT_TRUE(lock_handle->lock_nodes_ != nullptr);
  ASSERT_EQ(ObHTableLockMode::EXCLUSIVE, lock_handle->lock_nodes_->lock_mode_);
  ASSERT_EQ(nullptr, lock_handle->lock_nodes_->next_);
  ASSERT_EQ(lock_key, *lock_handle->lock_nodes_->lock_key_);
  // the others try to add lock
  ASSERT_TRUE(lock_handle->lock_nodes_ != nullptr);
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::EXCLUSIVE, *lock_handle2));
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle2));

  // 4. exclusive -> exclusive, nothing changed
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle));
  ASSERT_TRUE(lock_handle->lock_nodes_ != nullptr);
  ASSERT_EQ(ObHTableLockMode::EXCLUSIVE, lock_handle->lock_nodes_->lock_mode_);
  ASSERT_EQ(nullptr, lock_handle->lock_nodes_->next_);
  ASSERT_EQ(lock_key, *lock_handle->lock_nodes_->lock_key_);
  // the others try to add lock
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::EXCLUSIVE, *lock_handle2));
  ASSERT_EQ(OB_TRY_LOCK_ROW_CONFLICT, HTABLE_LOCK_MGR->lock_row(fake_table_id, fake_key, ObHTableLockMode::SHARED, *lock_handle2));

  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle));
  ASSERT_EQ(OB_SUCCESS, HTABLE_LOCK_MGR->release_handle(*lock_handle2));
}

TEST_F(TestHTableLock, concurrent_shared_lock)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  const uint64_t thread_cnt = 2048;
  const uint64_t fake_table_id = 1;
  ObString key = ObString::make_string("k1");
  const int64_t start = ObTimeUtility::current_time();
  TestConcurrentHTableSLock multi_thread(HTABLE_LOCK_MGR, thread_cnt, fake_table_id, key);
  ASSERT_EQ(OB_SUCCESS, multi_thread.start());
  multi_thread.wait();
  const int64_t duration = ObTimeUtility::current_time() - start;
  printf("thread_cnt: %ld, time elapsed: %ldms\n", thread_cnt, duration/1000);
}

TEST_F(TestHTableLock, concurrent_exclusive_lock)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  const uint64_t thread_cnt = 100;
  const uint64_t fake_table_id = 1;
  ObString key = ObString::make_string("k1");
  const int64_t start = ObTimeUtility::current_time();
  TestConcurrentHTableXLock multi_thread(HTABLE_LOCK_MGR, thread_cnt, fake_table_id, key);
  ASSERT_EQ(OB_SUCCESS, multi_thread.start());
  multi_thread.wait();
  multi_thread.check_result();
  const int64_t duration = ObTimeUtility::current_time() - start;
  printf("thread_cnt: %ld, time elapsed: %ldms\n", thread_cnt, duration/1000);
}

TEST_F(TestHTableLock, concurrent_exclusive_shared_lock)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  const uint64_t xthread_cnt = 100;
  const uint64_t sthread_cnt = 1024;
  const uint64_t fake_table_id = 1;
  ObString key = ObString::make_string("k1");
  const int64_t start = ObTimeUtility::current_time();
  TestConcurrentHTableXLock multi_xthread(HTABLE_LOCK_MGR, xthread_cnt, fake_table_id, key);
  TestConcurrentHTableSLock multi_sthread1(HTABLE_LOCK_MGR, sthread_cnt, fake_table_id, key, true);
  ASSERT_EQ(OB_SUCCESS, multi_xthread.start());
  ASSERT_EQ(OB_SUCCESS, multi_sthread1.start());
  sleep(3);
  TestConcurrentHTableSLock multi_sthread2(HTABLE_LOCK_MGR, sthread_cnt, fake_table_id, key, true);
  ASSERT_EQ(OB_SUCCESS, multi_sthread2.start());
  sleep(3);
  TestConcurrentHTableSLock multi_sthread3(HTABLE_LOCK_MGR, sthread_cnt, fake_table_id, key, true);
  ASSERT_EQ(OB_SUCCESS, multi_sthread3.start());
  multi_xthread.wait();
  multi_sthread1.wait();
  multi_sthread2.wait();
  multi_sthread3.wait();
  multi_xthread.check_result();
  const int64_t duration = ObTimeUtility::current_time() - start;
  printf("exclusive thread cnt: %ld, shared thread cnt: %ld, time elapsed: %ldms\n",
           xthread_cnt, sthread_cnt, duration/1000);
}

// NOTE: Put this test at the end of the whole test case to check whether memory leak exists or not
TEST_F(TestHTableLock, check_mem)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  ASSERT_EQ(0, HTABLE_LOCK_MGR->allocator_.normal_used_);
  ASSERT_EQ(0, HTABLE_LOCK_MGR->allocator_.special_total_);
  ASSERT_EQ(1, HTABLE_LOCK_MGR->allocator_.current_using_->ref_count_);
  ASSERT_EQ(0, HTABLE_LOCK_MGR->allocator_.special_page_list_.size_);
  ASSERT_EQ(0, HTABLE_LOCK_MGR->allocator_.using_page_list_.size_);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_htable_lock.log", true);
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
