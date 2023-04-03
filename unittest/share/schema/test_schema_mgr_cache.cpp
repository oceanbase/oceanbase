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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include <gtest/gtest.h>
#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"
#define private public
#include "share/schema/ob_schema_mgr_cache.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::obsys;
using namespace std;

namespace oceanbase
{
namespace tests
{

class TestSchemaMgrCache : public ::testing::Test
{
};

static int64_t global_schema_version = 0;

#define SCHEMA_VERSION_INC_STEP  8
int64_t  gen_new_schema_version()
{
  int64_t schema_version = ATOMIC_LOAD(&global_schema_version);
  schema_version = std::max(schema_version + SCHEMA_VERSION_INC_STEP,
                            ObTimeUtility::current_time());
  schema_version /= SCHEMA_VERSION_INC_STEP;
  schema_version *= SCHEMA_VERSION_INC_STEP;
  return schema_version;
}

void set_schema_version(const int64_t schema_version)
{
  ATOMIC_SET(&global_schema_version, schema_version);
}

int64_t get_schema_version()
{
  return ATOMIC_LOAD(&global_schema_version);
}

static void refresh(ObSchemaMgrCache *mgr_cache)
{
  int ret = OB_SUCCESS;

  LOG_INFO("refresh thread");
  for (int64_t i = 0; i < 128; ++i) {
    int64_t new_schema_version = gen_new_schema_version();
    ObSchemaMgr *mgr = new ObSchemaMgr;
    mgr->set_schema_version(new_schema_version);
    //int64_t eli_schema_version = OB_INVALID_VERSION;
    ObSchemaMgr *eli_schema_mgr = NULL;
    ret = mgr_cache->put(mgr, eli_schema_mgr);
    ASSERT_EQ(OB_SUCCESS , ret);
    set_schema_version(new_schema_version);
    usleep(10*1000);
  }

}

static void consume(ObSchemaMgrCache *mgr_cache)
{
  int ret = OB_SUCCESS;

  LOG_INFO("consume thread");
  for (int64_t i = 0; i < 100; ++i) {
    int64_t schema_version = get_schema_version();
    const ObSchemaMgr *mgr = NULL;
    ObSchemaMgrHandle handle;

    const ObSchemaMgr *mgr1 = NULL;
    const ObSchemaMgr *mgr2 = NULL;
    ObSchemaMgrHandle handle1;
    ObSchemaMgrHandle handle2;
    int ret1 = mgr_cache->get_nearest(schema_version-4, mgr1, handle1);
    int ret2 = mgr_cache->get_nearest(schema_version+4, mgr2, handle2);
    if (OB_SUCCESS != ret1 ||
       OB_SUCCESS != ret2 ||
       mgr1 != mgr2 ||
       !handle1.is_valid() ||
       !handle2.is_valid()) {
      mgr_cache->dump();
      LOG_ERROR("not found nearest schema version (+4/-4)",
          K(schema_version), K(mgr1), K(mgr2));
      OB_ASSERT(0);
    }

    ret = mgr_cache->get(schema_version, mgr, handle);
    if (OB_ENTRY_NOT_EXIST == ret) {
      mgr_cache->dump();
      LOG_INFO("schema version", K(schema_version));
      OB_ASSERT(0);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(NULL != mgr);
    ASSERT_TRUE(handle.is_valid());
    ASSERT_EQ(schema_version, mgr->get_schema_version());
    for (int64_t i = 0; i < 100; ++i) {
      mgr->get_schema_version();
      usleep(10);
    }
    usleep(100*1000);
  }
}

class Consumer : public share::ObThreadPool
{
public:
  void run1()
  {

    ObSchemaMgrCache *schema_mgr_cache = reinterpret_cast<ObSchemaMgrCache*>(arg);
    consume(schema_mgr_cache);
  }
};

class Refresher : public share::ObThreadPool
{
public:
  void run1()
  {

    ObSchemaMgrCache *schema_mgr_cache = reinterpret_cast<ObSchemaMgrCache*>(arg);
    refresh(schema_mgr_cache);
  }
};

TEST_F(TestSchemaMgrCache, multithread_put_and_get)
{
  int ret = OB_SUCCESS;

  ObSchemaMgrCache mgr_cache;
  const int64_t max_cached_num = 64;
  ret = mgr_cache.init(max_cached_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  Refresher refresher;
  Consumer consumer;
  const int refresher_num = 1;
  const int consumer_num = 16;
  CThread refreshers[refresher_num];
  CThread consumers[consumer_num];
  for (int i = 0; i < refresher_num; ++i)
  {
    refreshers[i].start(&refresher, &mgr_cache);
  }
  usleep(2*1000*1000);
  for (int i = 0; i < consumer_num; ++i)
  {
    consumers[i].start(&consumer, &mgr_cache);
  }
  for (int i = 0; i < refresher_num; ++i)
  {
    refreshers[i].join();
  }
  for (int i = 0; i < consumer_num; ++i)
  {
    consumers[i].join();
  }
  // check ref_cnt
  for (int64_t i = 0; i < mgr_cache.max_cached_num_; ++i) {
    const ObSchemaMgrItem &schema_mgr_item = mgr_cache.schema_mgr_items_[i];
    ASSERT_EQ(0, schema_mgr_item.ref_cnt_);
  }
}

class Refresher2 : public share::ObThreadPool
{
public:
  void run1()
  {

    ObSchemaMgrCache *schema_mgr_cache = reinterpret_cast<ObSchemaMgrCache*>(arg);
    ObSchemaMgr *mgr = new ObSchemaMgr;
    //int64_t eli_schema_version = OB_INVALID_VERSION;
    ObSchemaMgr *eli_schema_mgr = NULL;
    int ret = schema_mgr_cache->put(mgr, eli_schema_mgr);
    ASSERT_EQ(OB_EAGAIN, ret);
  }
};

TEST_F(TestSchemaMgrCache, cache_full)
{
  int ret = OB_SUCCESS;

  ObSchemaMgrCache mgr_cache;
  const int64_t max_cached_num = 64;
  ret = mgr_cache.init(max_cached_num);
  ObSchemaMgr mgr;
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < mgr_cache.max_cached_num_; ++i) {
    ObSchemaMgrItem &schema_mgr_item = mgr_cache.schema_mgr_items_[i];
    schema_mgr_item.schema_mgr_ = &mgr;
    schema_mgr_item.ref_cnt_ = 1;
  }
  Refresher2 refresher;
  const int refresher_num = 1;
  CThread refreshers[refresher_num];
  for (int i = 0; i < refresher_num; ++i)
  {
    refreshers[i].start(&refresher, &mgr_cache);
  }
  for (int i = 0; i < refresher_num; ++i)
  {
    refreshers[i].join();
  }
}

} // namespace test
} // namespace oceanbase

int main(int argc, char *argv[])
{
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_mgr_cache.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
