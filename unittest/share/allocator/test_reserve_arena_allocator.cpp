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
#define protected public
#define private public
#include "share/allocator/ob_reserve_arena.h"
#include "share/rc/ob_tenant_base.h"

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))

namespace oceanbase
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace std;

namespace unittest
{
class TestReserveArenaAllocator : public::testing::Test
{
public:
  typedef ObReserveArenaAllocator<1024> ObStorageReserveAllocator;
  TestReserveArenaAllocator();
  virtual ~TestReserveArenaAllocator();

  ObReserveArenaAllocator<1024> test_allocator_;
};
TestReserveArenaAllocator::TestReserveArenaAllocator():
  test_allocator_(ObMemAttr(500, ObModIds::OB_STORE_ROW_EXISTER), OB_MALLOC_NORMAL_BLOCK_SIZE)
{
}

TestReserveArenaAllocator::~TestReserveArenaAllocator()
{
}

TEST_F(TestReserveArenaAllocator, test_reset)
{
  // test reset
  // alloc buf
  int64_t sz = 256;
  void* p;
  for (int64_t i = 0; i < 3; ++i) {
    p = test_allocator_.alloc(sz);
  }
  ASSERT_EQ(test_allocator_.used(), 768);
  ASSERT_EQ(test_allocator_.total(), 768);
  ASSERT_EQ(test_allocator_.pos_, 768);

  test_allocator_.reset();
  ASSERT_EQ(test_allocator_.used(), 0);
  ASSERT_EQ(test_allocator_.total(), 0);
  ASSERT_EQ(test_allocator_.pos_, 0);
  // alloc new page 1k < size < 8k
  sz = 2048;
  p = test_allocator_.alloc(sz);
  STORAGE_LOG(INFO, "alloc 2K", K(test_allocator_.allocator_));
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(test_allocator_.pos_, 0);
  ASSERT_GE(test_allocator_.used(), sz);
  ASSERT_GE(test_allocator_.total(), sz);

  test_allocator_.reset();
  ASSERT_EQ(test_allocator_.pos_, 0);
  ASSERT_EQ(test_allocator_.used(), 0);
  ASSERT_EQ(test_allocator_.total(), 0);

  // alloc new page  size > 8k
  sz = 10240;
  p = test_allocator_.alloc(sz);
  STORAGE_LOG(INFO, "alloc 10K", K(test_allocator_.allocator_));
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(test_allocator_.pos_, 0);
  ASSERT_GE(test_allocator_.used(), sz);
  ASSERT_GE(test_allocator_.total(), sz);

  test_allocator_.reset();
  ASSERT_EQ(test_allocator_.pos_, 0);
  ASSERT_EQ(test_allocator_.used(), 0);
  ASSERT_EQ(test_allocator_.total(), 0);

}

TEST_F(TestReserveArenaAllocator, test_reuse)
{
  // test reuse
  // alloc buf
  int64_t sz = 256;
  void* p;
  for (int64_t i = 0; i < 3; ++i) {
    p = test_allocator_.alloc(sz);
  }
  ASSERT_EQ(test_allocator_.used(), 768);
  ASSERT_EQ(test_allocator_.total(), 768);
  ASSERT_EQ(test_allocator_.pos_, 768);

  test_allocator_.reuse();
  STORAGE_LOG(INFO, "after reuse", K(test_allocator_.allocator_));
  ASSERT_EQ(test_allocator_.pos_, 0);
  // alloc new page 1k < size < 8k
  sz = 2048;
  p = test_allocator_.alloc(sz);
  STORAGE_LOG(INFO, "alloc 2K", K(test_allocator_.allocator_));
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(test_allocator_.pos_, 0);
  ASSERT_GE(test_allocator_.used(), sz);
  ASSERT_GE(test_allocator_.total(), sz);
  int allocate_total = test_allocator_.total();

  test_allocator_.reuse();
  STORAGE_LOG(INFO, "after reuse 2K", K(test_allocator_.allocator_));
  ASSERT_EQ(test_allocator_.pos_, 0);
  ASSERT_EQ(test_allocator_.used(), 0);
  ASSERT_LE(test_allocator_.total(), allocate_total);

  // alloc new page  size > 8k
  sz = 10240;
  p = test_allocator_.alloc(sz);
  STORAGE_LOG(INFO, "alloc 10K", K(test_allocator_.allocator_));
  ASSERT_NE(p, nullptr);
  ASSERT_EQ(test_allocator_.pos_, 0);
  ASSERT_GE(test_allocator_.used(), sz);
  ASSERT_GE(test_allocator_.total(), sz);
  allocate_total = test_allocator_.total();

  test_allocator_.reuse();
  STORAGE_LOG(INFO, "after reuse 10K", K(test_allocator_.allocator_));
  ASSERT_EQ(test_allocator_.pos_, 0);
  ASSERT_EQ(test_allocator_.used(), 0);
  ASSERT_LT(test_allocator_.total(), allocate_total);

}

}//end namespace unittest
}//end namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_reserve_arena_allocator.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_file_name("test_reserve_arena_allocator.log", true);
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
