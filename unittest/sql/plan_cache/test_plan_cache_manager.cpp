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

#define USING_LOG_PREFIX SQL
#include <unistd.h>
#include "lib/json/ob_json.h"
#include "test_sql.h"
#include "lib/allocator/ob_malloc.h"
using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::share::schema;

namespace test {
static int MAX_THREAD_COUNT = 1;
static uint64_t TENANT_ID_NUM = 1;

static TestSQL* test_sql = NULL;
static ObPlanCacheManager* plan_cache_mgr = NULL;

void init_pcm()
{
  if (NULL == test_sql) {
    test_sql = new TestSQL(ObString("test_schema.sql"));
    ASSERT_TRUE(test_sql);
    test_sql->init();
  }
  if (NULL == plan_cache_mgr) {
    plan_cache_mgr = new ObPlanCacheManager;
    plan_cache_mgr->init(test_sql->get_part_cache(), test_sql->get_addr());
  }
}

class TestPlanCacheManager : public ::testing::Test {
public:
  TestPlanCacheManager()
  {}
  virtual ~TestPlanCacheManager()
  {}
  void SetUp()
  {}
  void TearDown()
  {}

private:
  // disallow copy
  TestPlanCacheManager(const TestPlanCacheManager& other);
  TestPlanCacheManager& operator=(const TestPlanCacheManager& other);
};

void test_plan_cache_manager()
{
  uint64_t tenant_id = 0;
  ObPlanCache* plan_cache = NULL;

  // test get_plan_cache()
  ObPCMemPctConf conf;
  for (tenant_id = 0; tenant_id < TENANT_ID_NUM; tenant_id++) {
    plan_cache = plan_cache_mgr->get_plan_cache(tenant_id);
    plan_cache = plan_cache_mgr->get_or_create_plan_cache(tenant_id, conf);  // may be plan_cache = NULL in parallel
    LOG_INFO("get_plan_cache", K(tenant_id), K(plan_cache));
    // ob_print_mod_memory_usage();
    if (TENANT_ID_NUM / 2 == tenant_id) {
      plan_cache_mgr->elimination_task_.runTimerTask();
    }
  }

  // test revert_plan_cache()
  EXPECT_TRUE(OB_SUCCESS == plan_cache_mgr->revert_plan_cache(TENANT_ID_NUM - 1));
  EXPECT_TRUE(OB_SUCCESS == plan_cache_mgr->revert_plan_cache(TENANT_ID_NUM + 1));
}

class ObPlanCacheManagerRunnable : public share::ObThreadPool {
public:
  void run1()
  {
    test_plan_cache_manager();
  }
};

TEST_F(TestPlanCacheManager, basic)
{
  // test
  ObPlanCacheManagerRunnable pcm_runner;
  for (int i = 0; i < MAX_THREAD_COUNT; ++i) {
    pcm_runner.start();
  }
  for (int i = 0; i < MAX_THREAD_COUNT; ++i) {
    pcm_runner.wait();
  }

  EXPECT_TRUE(OB_SUCCESS == plan_cache_mgr->flush_all_plan_cache());
  plan_cache_mgr->destroy();
}
}  // namespace test

int main(int argc, char** argv)
{
  ::oceanbase::sql::init_sql_factories();
  ::testing::InitGoogleTest(&argc, argv);

  int c = 0;
  while (-1 != (c = getopt(argc, argv, "t::n::"))) {
    switch (c) {
      case 't':
        if (NULL != optarg) {
          test::MAX_THREAD_COUNT = atoi(optarg);
        }
        break;
      case 'n':
        if (NULL != optarg) {
          test::TENANT_ID_NUM = atoi(optarg);
        }
      default:
        break;
    }
  }

  ::test::init_pcm();
  return RUN_ALL_TESTS();
}  // namespace test end
