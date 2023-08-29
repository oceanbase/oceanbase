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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#define private public
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "share/location_cache/ob_tablet_ls_service.h"
#include "common/ob_clock_generator.h" // ObClockGenerator

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace common::array;
using namespace share;

class TestTabletLSMap: public ::testing::Test
{
public:
  TestTabletLSMap() {}
  virtual ~TestTabletLSMap() {};
  virtual void SetUp()
  {
    LOG_INFO("init begin");
    (void)tablet_ls_map.init();
    LOG_INFO("init finish");
  };
  virtual void TearDown()
  { 
    LOG_INFO("destroy begin");
    tablet_ls_map.destroy();
    LOG_INFO("destroy finish");
  };
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestTabletLSMap);
protected:
  // function members
protected:
  // data members
  ObTabletLSMap tablet_ls_map;
};

TEST_F(TestTabletLSMap, test_single)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 1008;
  ObTabletID tablet_id(50000);
  ObLSID ls_id(1002);
  ObTabletLSKey key(tenant_id, tablet_id);
  ObTabletLSCache value;
  ret = value.init(tenant_id, tablet_id, ls_id, 0, 0);
  ret = tablet_ls_map.update(value);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletLSCache result;
  ret = tablet_ls_map.get(key, result);
  ASSERT_EQ(OB_SUCCESS, ret);
  bool equal = result.mapping_is_same_with(value);
  ASSERT_EQ(true, equal);
  int64_t size = tablet_ls_map.size();
  ASSERT_EQ(1, size);

  result.reset();
  ret = tablet_ls_map.del(key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = tablet_ls_map.get(key, result);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  size = tablet_ls_map.size();
  ASSERT_EQ(0, size);
  LOG_INFO("test_single finished");
}

class ObStressThread : public lib::ThreadPool
{
public:
  ObStressThread() { }
  void run1()
  {
    assert(NULL != tablet_ls_map);
    int ret = OB_SUCCESS;
    uint64_t tid = get_thread_idx();
    LOG_INFO("thread start", K(tid), K(tablet_ls_map->size()));

    // generate data
    ObArray<ObTabletLSCache> data;
    ret = data.reserve(BATCH_COUNT);
    ASSERT_EQ(OB_SUCCESS, ret);
    for (int64_t i = 0; i < BATCH_COUNT; ++i) {
      uint64_t tenant_id = i % 100 * 2 + 1002;
      ObTabletID tablet_id(200001 + i);
      uint64_t uint_ls_id = i % 10 + 1001;
      ObLSID ls_id(uint_ls_id);
      ObTabletLSCache value;
      ret = value.init(tenant_id, tablet_id, ls_id, 0, 0);
      ASSERT_EQ(OB_SUCCESS, ret);
      ret = data.push_back(value);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ASSERT_EQ(BATCH_COUNT, data.count());

    // update
    for (int64_t i = 0; i < BATCH_COUNT; ++i) {
      const ObTabletLSCache &cache = data.at(i);
      ret = tablet_ls_map->update(cache);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ASSERT_EQ(BATCH_COUNT, tablet_ls_map->size());
    LOG_INFO("update finished", K(tid), K(tablet_ls_map->size()));

    print_bucket_count();

    // get
    for (int64_t i = 0; i < BATCH_COUNT; ++i) {
      const ObTabletLSCache &cache = data.at(i);
      ObTabletLSCache value;
      ret = tablet_ls_map->get(cache.get_cache_key(), value);
      if (OB_FAIL(ret)) { LOG_WARN("fail to get", KR(ret), K(cache), K(value)); }
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    LOG_INFO("get finished", K(tid), K(tablet_ls_map->size()));

    // get_all
    ObArray<ObTabletLSCache> cache_array;
    ret = cache_array.reserve(tablet_ls_map->size());
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = tablet_ls_map->get_all(cache_array);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(cache_array.count(), tablet_ls_map->size());
    LOG_INFO("get_all finished", K(tid), K(tablet_ls_map->size()));

    sleep(5); // a simple way to wait all threads finish get_all

    // delete
    for (int64_t i = 0; i < BATCH_COUNT; ++i) {
      const ObTabletLSCache &cache = data.at(i);
      ret = tablet_ls_map->del(cache.get_cache_key());
      bool bret = OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret;
      ASSERT_TRUE(bret);
    }
    LOG_INFO("delete finished", K(tid), K(tablet_ls_map->size()));
    ASSERT_EQ(0, tablet_ls_map->size());
  }

  void print_bucket_count() {
    assert(NULL != tablet_ls_map);
    int64_t cache_count_on_bucket = 0;
    std::map<int64_t, int64_t> count_map;
    ObTabletLSCache *tablet_ls_cache = NULL;
    for (int64_t i = 0; i < tablet_ls_map->BUCKETS_CNT; ++i) {
      cache_count_on_bucket = 0;
      tablet_ls_cache = tablet_ls_map->ls_buckets_[i];
      // foreach bucket
      while (OB_NOT_NULL(tablet_ls_cache)) {
        ++cache_count_on_bucket;
        tablet_ls_cache = static_cast<ObTabletLSCache *>(tablet_ls_cache->next_);
      }
      ++count_map[cache_count_on_bucket];
    }
    LOG_INFO("test_cache_distribution", "set size", count_map.size());
    std::map<int64_t, int64_t>::iterator it;
    for (it = count_map.begin(); it != count_map.end(); ++it) {
      LOG_INFO("bucket ", "count", it->first, "number", it->second);
    }
  }

  // member
  int64_t BATCH_COUNT;
  ObTabletLSMap *tablet_ls_map;
};

TEST_F(TestTabletLSMap, test_batch_concurrent)
{
  const int64_t THREAD_COUNT = 4;
  const int64_t BATCH_COUNT = 1000000; // 1 million
  ObStressThread threads;
  threads.tablet_ls_map = &tablet_ls_map;
  threads.BATCH_COUNT = BATCH_COUNT;

  threads.set_thread_count(THREAD_COUNT);
  threads.start();
  //sleep(10);
  threads.wait();
  ASSERT_EQ(0, tablet_ls_map.size());
}


} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = oceanbase::OB_SUCCESS;
  system("rm -rf test_tablet_ls_map.log*");

  OB_LOGGER.set_file_name("test_tablet_ls_map.log", true);
  OB_LOGGER.set_log_level("INFO");
  if (oceanbase::OB_SUCCESS != oceanbase::ObClockGenerator::init()) {
    LOG_WARN("clock generator init error!");
  } else {
    ::testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}