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

#include <gtest/gtest.h>
#include "mock_multi_version_schema_service_for_fallback.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;

class TestFallbackSchemaMgr : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestFallbackSchemaMgr, fallback_schema_for_liboblog)
{
  ObSchemaService::g_liboblog_mode_ = true;
  MockMultiVersionSchemaServiceForFallback schema_service;
  const int64_t slot_for_cache  = 12;
  const int64_t slot_for_liboblog = 4;
  int ret = schema_service.init(slot_for_cache, slot_for_liboblog);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t max_version = 525;
  schema_service.prepare(max_version);
  schema_service.dump();

  for (int i = 1; i <= 513; ++i) {
    ObSchemaGetterGuard guard;
    ret = schema_service.get_schema_guard(guard, i);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (i % 16 == 0) {
      schema_service.dump_mem_mgr_for_liboblog();
    }
    schema_service.dump_schema_mgr();
  }

  schema_service.dump_mem_mgr_for_liboblog();
  schema_service.destory();
};

static int64_t global_version = 1;

class Worker : public share::ObThreadPool
{
public:
  void run1()
  {

    MockMultiVersionSchemaServiceForFallback *schema_service = reinterpret_cast<MockMultiVersionSchemaServiceForFallback *>(arg);
    int ret = OB_SUCCESS;
    for (int i = 0; i < 20; ++i) {
      ObSchemaGetterGuard guard;
      int64_t fetch_version = ATOMIC_FAA(&global_version, 1);
      do {
        ret = schema_service->get_schema_guard(guard, fetch_version);
        if (OB_FAIL(ret)) {
          SHARE_SCHEMA_LOG(WARN, "get schema guard of version", K(fetch_version));
          schema_service->dump_schema_mgr();
//          schema_service->dump_mem_mgr_for_liboblog();
        }
      } while (ret == OB_EAGAIN);
      ASSERT_EQ(ret, OB_SUCCESS);
    }
  }
};

TEST_F(TestFallbackSchemaMgr, concurrent_fallback)
{
  ObSchemaService::g_liboblog_mode_ = true;
  MockMultiVersionSchemaServiceForFallback schema_service;
  const int64_t slot_for_cache  = 12;
  const int64_t slot_for_liboblog = 18;
  int ret = schema_service.init(slot_for_cache, slot_for_liboblog);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t max_version = 525;
  schema_service.prepare(max_version);
  schema_service.dump();

  const int64_t worker_cnt = 16;
  Worker workers[worker_cnt];

  obsys::CThread worker_thread[worker_cnt];
  for (int i = 0; i < worker_cnt; ++i) {
    worker_thread[i].start(&workers[i], (void *)(&schema_service));
  }
  for (int i = 0; i < worker_cnt; ++i) {
    worker_thread[i].join();
  }

  schema_service.dump_mem_mgr_for_liboblog();
  schema_service.destory();
};




}
}
}


int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  system("rm -rf test_fallback_schema_mgr.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_fallback_schema_mgr.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
