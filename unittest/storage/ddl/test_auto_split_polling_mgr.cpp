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

#define ASSERT_OK(x) ASSERT_EQ(OB_SUCCESS, (x))
#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/scheduler/ob_partition_auto_split_helper.h"
#undef private

namespace oceanbase
{

using namespace common;
using namespace lib;
using namespace share;
using namespace storage;
using namespace blocksstable;
using namespace share::schema;
using namespace compaction;

static const uint64_t TEST_TENANT_A_ID = 1002;
static const uint64_t TEST_TENANT_B_ID = 1003;
static const uint64_t TEST_TENANT_C_ID = 1004;
static const ObLSID ls_id{1};

class TestSplitTaskScheduler : public ::testing::Test
{
public:
  TestSplitTaskScheduler()
    : rs_scheduler_(ObRsAutoSplitScheduler::get_instance()), polling_mgr_(rs_scheduler_.polling_mgr_), sys_tenant_(OB_SYS_TENANT_ID)
    {
      ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(TEST_TENANT_A_ID);
      ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(TEST_TENANT_B_ID);
      ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(TEST_TENANT_C_ID);
      init();
    }
  virtual void SetUp();
  void reset();
  int init() { return rs_scheduler_.init(); };

public:
  ObRsAutoSplitScheduler &rs_scheduler_;
  ObAutoSplitTaskPollingMgr &polling_mgr_;
  ObTenantBase sys_tenant_;
};

void pop(ObArray<ObAutoSplitTask> &task_array, ObAutoSplitTaskPollingMgr &polling_mgr)
{
  ObArray<ObArray<ObAutoSplitTask>> tenant_task_array;
  int64_t pop_nums = 1;
  while (ATOMIC_LOAD(&polling_mgr.total_tasks_) > 0) {
    tenant_task_array.reuse();
    ASSERT_OK(polling_mgr.pop_tasks(pop_nums, tenant_task_array));
    if (tenant_task_array.count() == 0) {
      break;
    }
    for (int64_t i = 0; i < tenant_task_array.count(); ++i) {
      task_array.push_back(tenant_task_array.at(i));
    }
    pop_nums%=10;
    pop_nums+=1;
  }
}

void push(const ObArray<ObArray<ObAutoSplitTask>> &tenants_task_array, ObAutoSplitTaskPollingMgr &polling_mgr)
{
  for (int64_t i = 0; i < tenants_task_array.count(); ++i) {
    int ret = polling_mgr.push_tasks(tenants_task_array.at(i));
    ASSERT_OK(ret);
  }
}

void TestSplitTaskScheduler::reset()
{
  rs_scheduler_.reset();
}

void TestSplitTaskScheduler::SetUp()
{
  ObTenantEnv::set_tenant(&sys_tenant_);
}

TEST_F(TestSplitTaskScheduler, simple_push_and_pop)
{
  ObArray<ObAutoSplitTask> task_array;
  ObArray<uint64_t> ids;
  ObAutoSplitTask task_a(TEST_TENANT_A_ID, ls_id, 1/*tablet_id*/, 1/*auto_split_size*/, 2/*used_disk_size*/, 0);
  ObAutoSplitTask task_b(TEST_TENANT_A_ID, ls_id, 2/*tablet_id*/, 1/*auto_split_size*/, 3/*used_disk_size*/, 0);
  ObAutoSplitTask task_c(TEST_TENANT_B_ID, ls_id, 3/*tablet_id*/, 1/*auto_split_size*/, 4/*used_disk_size*/, 0);
  ASSERT_OK(task_array.push_back(task_a));
  ASSERT_OK(polling_mgr_.push_tasks(task_array));
  task_array.reuse();

  ASSERT_OK(task_array.push_back(task_c));
  ASSERT_OK(polling_mgr_.push_tasks(task_array));
  task_array.reuse();

  ASSERT_OK(task_array.push_back(task_b));
  ASSERT_OK(polling_mgr_.push_tasks(task_array));
  task_array.reuse();
  // we expect the first tablet_id popped from mgr to be 2,
  // because the tenant_cache A is at index 0 and the last_access_index is 0, and the tablet with id 2 has the hightest priority in that cache
  ObArray<ObArray<ObAutoSplitTask>> tenants_task_array;
  ASSERT_OK(polling_mgr_.pop_tasks(1, tenants_task_array));
  ASSERT_EQ(1, tenants_task_array.count());
  ASSERT_EQ(1, tenants_task_array.at(0).count());
  ObAutoSplitTask &task1 = tenants_task_array.at(0).at(0);
  ASSERT_OK(ids.push_back(task1.tablet_id_.id_));
  tenants_task_array.reuse();
  // we expect the second tablet_id popped from mgr to be 2,
  // because the tenant_cache B is at index 1, and the last_access_index is 1, the tablet with id 3 has the hightest priority in that cache
  ASSERT_OK(polling_mgr_.pop_tasks(1, tenants_task_array));
  ASSERT_EQ(1, tenants_task_array.count());
  ASSERT_EQ(1, tenants_task_array.at(0).count());
  ObAutoSplitTask &task2 = tenants_task_array.at(0).at(0);
  ASSERT_OK(ids.push_back(task2.tablet_id_.id_));
  tenants_task_array.reuse();
  // we expect the third tablet_id popped from mgr to be 2,
  // because the tenant_cache B is at index 1, and the last_access_index is 0, the tablet with id 3 has the hightest priority in that cache
  ASSERT_OK(polling_mgr_.pop_tasks(1, tenants_task_array));
  ASSERT_EQ(1, tenants_task_array.count());
  ASSERT_EQ(1, tenants_task_array.at(0).count());
  ObAutoSplitTask &task3 = tenants_task_array.at(0).at(0);
  ASSERT_OK(ids.push_back(task3.tablet_id_.id_));
  tenants_task_array.reuse();

  std::sort(ids.begin(), ids.end());
  for (int64_t i = 0; i < ids.count(); ++i) {
    ASSERT_EQ(i + 1, ids.at(i));
  }
}

TEST_F(TestSplitTaskScheduler, test_reset)
{
  ObArray<ObAutoSplitTask> task_array;
  ObAutoSplitTask task_a(TEST_TENANT_A_ID, ls_id, 1/*tablet_id*/, 1/*auto_split_size*/, 2/*used_disk_size*/, 0);
  ObAutoSplitTask task_b(TEST_TENANT_A_ID, ls_id, 2/*tablet_id*/, 1/*auto_split_size*/, 3/*used_disk_size*/, 0);
  ASSERT_OK(task_array.push_back(task_a));
  ASSERT_OK(polling_mgr_.push_tasks(task_array));
  task_array.reuse();
  ASSERT_OK(task_array.push_back(task_b));
  ASSERT_OK(polling_mgr_.push_tasks(task_array));
  task_array.reuse();

  reset();
  init();

  task_array.push_back(task_a);
  ASSERT_OK(polling_mgr_.push_tasks(task_array));
  task_array.reuse();
  ObArray<ObArray<ObAutoSplitTask>> tenants_task_array;
  ASSERT_OK(polling_mgr_.pop_tasks(1, tenants_task_array));
  ASSERT_EQ(1, tenants_task_array.count());
  ASSERT_EQ(1, tenants_task_array.at(0).count());
  ObAutoSplitTask &task1 = tenants_task_array.at(0).at(0);
  ASSERT_EQ(1, task1.tablet_id_.id_);
  tenants_task_array.reuse();
}


TEST_F(TestSplitTaskScheduler, single_tenant_hard_push_and_pop)
{
  ObArray<ObAutoSplitTask> task_array;
  ObAutoSplitTask task_high_prio(TEST_TENANT_A_ID, ls_id, 1000/*tablet_id*/, 1/*auto_split_size*/, 1000/*used_disk_size*/, 0);
  ASSERT_OK(task_array.push_back(task_high_prio));
  ASSERT_OK(polling_mgr_.push_tasks(task_array));
  task_array.reuse();
  for (int64_t i = 2; i < ObAutoSplitTaskCache::CACHE_MAX_CAPACITY + 10; ++i ) {
    ASSERT_OK(task_array.push_back(ObAutoSplitTask(TEST_TENANT_A_ID, ls_id, i/*tablet_id*/, 1/*auto_split_size*/, i/*used_disk_size*/, 0)));
    ASSERT_OK(polling_mgr_.push_tasks(task_array));
    task_array.reuse();
  }
  ObArray<ObArray<ObAutoSplitTask>> tenants_task_array;
  ASSERT_OK(polling_mgr_.pop_tasks(1, tenants_task_array));
  ASSERT_EQ(1, tenants_task_array.count());
  ASSERT_EQ(1, tenants_task_array.at(0).count());
  ObAutoSplitTask &high_prio_task = tenants_task_array.at(0).at(0);
  ASSERT_EQ(1000, high_prio_task.tablet_id_.id_);
  tenants_task_array.reuse();

  int64_t expect_hightest_prio_tablet_id = ObAutoSplitTaskCache::CACHE_MAX_CAPACITY + 9;
  int64_t total_tasks = ObAutoSplitTaskCache::CACHE_MAX_CAPACITY - 1;
  while (total_tasks > 0) {
    tenants_task_array.reuse();
    ASSERT_OK(polling_mgr_.pop_tasks(5, tenants_task_array));
    ASSERT_EQ(1, tenants_task_array.count());
    int64_t task_arr_len = tenants_task_array.at(0).count();
    ASSERT_TRUE(task_arr_len != 0);
    total_tasks -= task_arr_len;
    ASSERT_EQ(total_tasks, polling_mgr_.total_tasks_);
    ASSERT_TRUE(tenants_task_array.at(0).count() >= 4);
    LOG_INFO("tenants_task_array", K(tenants_task_array));
    for (int64_t i = 0; i < tenants_task_array.at(0).count(); ++i) {
      ObAutoSplitTask &task = tenants_task_array.at(0).at(i);
      ASSERT_EQ(expect_hightest_prio_tablet_id--, task.tablet_id_.id_);
    }
  }

  for (hash::ObHashMap<uint64_t, ObAutoSplitTaskCache*>::iterator iter = polling_mgr_.map_tenant_to_cache_.begin(); iter != polling_mgr_.map_tenant_to_cache_.end(); iter++) {
    uint64_t tenant_id = iter->first;
    ObAutoSplitTaskCache *&tenant_cache = iter->second;
    ASSERT_TRUE(tenant_cache != nullptr);
    ASSERT_EQ(0, tenant_cache->get_tasks_num());
  }
}

TEST_F(TestSplitTaskScheduler, mutiple_tenants_hard_push_and_pop)
{
  ObArray<ObAutoSplitTask> task_array;
  ObArray<int64_t> nums;
  for (int64_t i = 2; i < 102; ++i) {
    ASSERT_OK(nums.push_back(i));
  }
  std::random_shuffle(nums.begin(), nums.end());
  for (int64_t i = 0; i < nums.count(); ++i ) {
    int64_t num = nums.at(i);
    ASSERT_OK(task_array.push_back(ObAutoSplitTask(TEST_TENANT_A_ID, ls_id, num/*tablet_id*/, 1/*auto_split_size*/, num/*used_disk_size*/, 0)));
    ASSERT_OK(polling_mgr_.push_tasks(task_array));
    task_array.reuse();

    ASSERT_OK(task_array.push_back(ObAutoSplitTask(TEST_TENANT_B_ID, ls_id, num/*tablet_id*/, 1/*auto_split_size*/, num/*used_disk_size*/, 0)));
    ASSERT_OK(polling_mgr_.push_tasks(task_array));
    task_array.reuse();

    ASSERT_OK(task_array.push_back(ObAutoSplitTask(TEST_TENANT_C_ID, ls_id, num/*tablet_id*/, 1/*auto_split_size*/, num/*used_disk_size*/, 0)));
    ASSERT_OK(polling_mgr_.push_tasks(task_array));
    task_array.reuse();
  }
  ASSERT_EQ(3 * nums.count(), polling_mgr_.total_tasks_);
  ASSERT_EQ(3, polling_mgr_.get_total_tenants());
  ObArray<ObArray<ObAutoSplitTask>> tenants_task_array;
  int64_t total_tasks = nums.count() * 3;

  ObArray<int64_t> tenant_A_task_id;
  ObArray<int64_t> tenant_B_task_id;
  ObArray<int64_t> tenant_C_task_id;

  while (total_tasks > 0) {
    tenants_task_array.reuse();
    ASSERT_OK(polling_mgr_.pop_tasks(5, tenants_task_array));
    for (int64_t i = 0; i < tenants_task_array.count(); ++i) {
      ObArray<ObAutoSplitTask> &task_array = tenants_task_array.at(i);
      ASSERT_TRUE(task_array.count() > 0);
      uint64_t tenant_id = task_array.at(0).tenant_id_;
      for (int64_t j = 0; j < task_array.count(); ++j) {
        total_tasks -= 1;
        ASSERT_EQ(tenant_id, task_array.at(j).tenant_id_);
        if (tenant_id == TEST_TENANT_A_ID) {
          ASSERT_OK(tenant_A_task_id.push_back(task_array.at(j).tablet_id_.id_));
        } else if (tenant_id == TEST_TENANT_B_ID) {
          ASSERT_OK(tenant_B_task_id.push_back(task_array.at(j).tablet_id_.id_));
        } else if (tenant_id == TEST_TENANT_C_ID) {
          ASSERT_OK(tenant_C_task_id.push_back(task_array.at(j).tablet_id_.id_));
        }
      }
    }
  }

  for (hash::ObHashMap<uint64_t, ObAutoSplitTaskCache*>::iterator iter = polling_mgr_.map_tenant_to_cache_.begin(); iter != polling_mgr_.map_tenant_to_cache_.end(); iter++) {
    uint64_t tenant_id = iter->first;
    ObAutoSplitTaskCache *&tenant_cache = iter->second;
    ASSERT_TRUE(tenant_cache != nullptr);
    ASSERT_EQ(0, tenant_cache->get_tasks_num());
  }


  ASSERT_EQ(100, tenant_A_task_id.count());
  ASSERT_EQ(100, tenant_B_task_id.count());
  ASSERT_EQ(100, tenant_C_task_id.count());
  for (int64_t i = 100; i > 0 ; --i) {
    ASSERT_EQ(i+1, tenant_A_task_id.at(100 - i));
    ASSERT_EQ(i+1, tenant_B_task_id.at(100 - i));
    ASSERT_EQ(i+1, tenant_C_task_id.at(100 - i));
  }
}

TEST_F(TestSplitTaskScheduler, parallel_push_and_pop)
{
  ObArray<ObArray<ObAutoSplitTask>> task_array_1;
  ObArray<ObArray<ObAutoSplitTask>> task_array_2;
  ObArray<int64_t> nums_1;
  ObArray<int64_t> nums_2;
  for (int64_t i = 2; i < 50000; ++i) {
    if (i%2 == 0) {
      ASSERT_OK(nums_1.push_back(i));
    } else {
      ASSERT_OK(nums_2.push_back(i));
    }
  }
  std::random_shuffle(nums_1.begin(), nums_1.end());
  ObArray<ObAutoSplitTask> tmp_array;
  for (int64_t i = 0; i < nums_1.count(); ++i ) {
    tmp_array.reuse();
    int64_t num_1 = nums_1.at(i);
    ASSERT_OK(tmp_array.push_back(ObAutoSplitTask(TEST_TENANT_A_ID, ls_id, num_1/*tablet_id*/, 1/*auto_split_size*/, num_1/*used_disk_size*/, 0)));
    ASSERT_OK(task_array_1.push_back(tmp_array));
    tmp_array.reuse();
    int64_t num_2 = nums_2.at(i);
    ASSERT_OK(tmp_array.push_back(ObAutoSplitTask(TEST_TENANT_A_ID, ls_id, num_2/*tablet_id*/, 1/*auto_split_size*/, num_2/*used_disk_size*/, 0)));
    ASSERT_OK(task_array_2.push_back(tmp_array));
  }


  std::thread t1(&push, std::ref(task_array_1), std::ref(polling_mgr_));
  std::thread t2(&push, std::ref(task_array_2), std::ref(polling_mgr_));
  t1.join();
  t2.join();
  ObArray<ObAutoSplitTask> result_array_1;
  ObArray<ObAutoSplitTask> result_array_2;
  std::thread t3(&pop, std::ref(result_array_1), std::ref(polling_mgr_));
  std::thread t4(&pop, std::ref(result_array_2), std::ref(polling_mgr_));
  t3.join();
  t4.join();
  tmp_array.reuse();
  ASSERT_OK(tmp_array.push_back(result_array_1));
  ASSERT_OK(tmp_array.push_back(result_array_2));
  ASSERT_EQ(100, tmp_array.count());
  int64_t max = 49999;
  ObArray<int64_t> id_array;
  for (int64_t i = 0; i < tmp_array.count(); ++i) {
    id_array.push_back(tmp_array.at(i).tablet_id_.id());
  }
  std::sort(id_array.begin(),id_array.end());
  for (int64_t i = id_array.count()-1; i > -1; --i) {
    ASSERT_EQ(max--, id_array.at(i));
  }
}

}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  system("rm -rf test_auto_split_polling_mgr.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_auto_split_polling_mgr.log", true);
  return RUN_ALL_TESTS();
}