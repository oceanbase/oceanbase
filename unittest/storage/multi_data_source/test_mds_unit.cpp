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
#define UNITTEST_DEBUG
#include <gtest/gtest.h>
#include "lib/ob_errno.h"
#include "lib/utility/utility.h"
#include "share/ob_errno.h"
#include <exception>
#include "lib/allocator/ob_malloc.h"
#include "storage/multi_data_source/mds_node.h"
#include <thread>
#include <iostream>
#include <vector>
#include <chrono>
#include "common/ob_clock_generator.h"
#include "storage/multi_data_source/mds_row.h"
#include "storage/multi_data_source/mds_unit.h"
#include "example_user_helper_define.cpp"
#include "storage/tablet/ob_mds_schema_helper.h"
namespace oceanbase {
namespace storage {
namespace mds {
void *DefaultAllocator::alloc(const int64_t size) {
  void *ptr = std::malloc(size);// ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void DefaultAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  std::free(ptr);// ob_free(ptr);
}
void *MdsAllocator::alloc(const int64_t size) {
  void *ptr = std::malloc(size);// ob_malloc(size, "MDS");
  ATOMIC_INC(&alloc_times_);
  MDS_LOG(DEBUG, "alloc obj", KP(ptr), K(size), K(lbt()));
  return ptr;
}
void MdsAllocator::free(void *ptr) {
  ATOMIC_INC(&free_times_);
  MDS_LOG(DEBUG, "free obj", KP(ptr), K(lbt()));
  std::free(ptr);// ob_free(ptr);
}
}}}
namespace oceanbase {
namespace unittest {

using namespace common;
using namespace std;
using namespace storage;
using namespace mds;

class TestMdsUnit: public ::testing::Test
{
public:
  TestMdsUnit() { ObMdsSchemaHelper::get_instance().init(); };
  virtual ~TestMdsUnit() {};
  virtual void SetUp() {
  };
  virtual void TearDown() {
  };
  static void set_single_row();
  static void get_single_row();
  static MdsUnit<DummyKey, ExampleUserData2> signle_row_unit_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestMdsUnit);
};

MdsUnit<DummyKey, ExampleUserData2> TestMdsUnit::signle_row_unit_;

void TestMdsUnit::set_single_row() {
  ExampleUserData2 data(1);
  MdsCtx ctx(1);

}

void TestMdsUnit::get_single_row() {

}

TEST_F(TestMdsUnit, set_single_row) { TestMdsUnit::set_single_row(); }
TEST_F(TestMdsUnit, get_single_row) { TestMdsUnit::get_single_row(); }

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_mds_unit.log");
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_mds_unit.log", false);
  logger.set_log_level(OB_LOG_LEVEL_TRACE);
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  oceanbase::unittest::TestMdsUnit::signle_row_unit_.~MdsUnit();
  int64_t alloc_times = oceanbase::storage::mds::MdsAllocator::get_alloc_times();
  int64_t free_times = oceanbase::storage::mds::MdsAllocator::get_free_times();
  if (alloc_times != free_times) {
    MDS_LOG(ERROR, "memory may leak", K(free_times), K(alloc_times));
    if (ret == 0) {
      ret = -1;
    }
  } else {
    MDS_LOG(INFO, "all memory released", K(free_times), K(alloc_times));
  }
  return ret;
}