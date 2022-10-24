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
#include "lib/memory/achunk_mgr.h"
#include "lib/allocator/ob_malloc.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

TEST(ObStatMemory, Basic)
{
  int64_t limit = MSTAT_GET_LIMIT();
  EXPECT_EQ(INT64_MAX, limit);
  MSTAT_SET_LIMIT(1);
  limit = MSTAT_GET_LIMIT();
  EXPECT_EQ(1, limit);

  EXPECT_LT(2<<20, ob_get_memory_size_handled());  // must gt 2M(block size)

  {
    MSTAT_SET_LIMIT(1L << 20);
    int64_t limit =  MSTAT_GET_LIMIT();
    EXPECT_EQ(1L << 20, limit);
    void *ptr = CHUNK_MGR.alloc_chunk();
    EXPECT_EQ(NULL, ptr);

    MSTAT_SET_LIMIT(2L << 20);
    ptr = CHUNK_MGR.alloc_chunk();
    EXPECT_EQ(NULL, ptr);

    MSTAT_SET_LIMIT(4L << 20);
    ptr = CHUNK_MGR.alloc_chunk();
    EXPECT_EQ(NULL, ptr);
  }

  MSTAT_SET_LIMIT(100L << 20);

  int64_t hold = 0;
  int64_t used = 0;
  MSTAT_GET_TENANT(1001, hold, used);
  EXPECT_EQ(0, hold);
  EXPECT_EQ(0, used);

  {
    MSTAT_SET_TENANT_LIMIT(1001, 0);
    MSTAT_ADD(1001, ObModIds::TEST, 100, 10);
    MSTAT_GET_MOD(ObModIds::TEST, hold, used);
    EXPECT_EQ(0, hold);
    EXPECT_EQ(0, used);
  }

  {
    MSTAT_SET_TENANT_LIMIT(1001, 1000);
    int64_t limit = MSTAT_GET_TENANT_LIMIT(1001);
    EXPECT_EQ(1000, limit);
    MSTAT_ADD(1001, ObModIds::TEST, 100, 10);
    MSTAT_GET_MOD(ObModIds::TEST, hold, used);
    EXPECT_EQ(100, hold);
    EXPECT_EQ(10, used);

    // It'll fail to update the statistics if hold exceeds limit
    MSTAT_ADD(1001, ObModIds::TEST, 1000, 10);
    MSTAT_GET_MOD(ObModIds::TEST, hold, used);
    EXPECT_EQ(100, hold);
    EXPECT_EQ(10, used);

    // It'll success if hold doesn't exceed its limit
    MSTAT_ADD(1001, ObModIds::TEST, 100, 10);
    MSTAT_GET_MOD(ObModIds::TEST, hold, used);
    EXPECT_EQ(200, hold);
    EXPECT_EQ(20, used);

    MSTAT_GET_TENANT(1001, hold, used);
    EXPECT_EQ(200, hold);
    EXPECT_EQ(20, used);

    MSTAT_ADD(1001, ObModIds::OB_MOD_DO_NOT_USE_ME, 100, 10);
    MSTAT_GET_TENANT(1001, hold, used);
    EXPECT_EQ(300, hold);
    EXPECT_EQ(30, used);

    MSTAT_GET_TENANT(1002, hold, used);
    EXPECT_EQ(0, hold);
    EXPECT_EQ(0, used);
  }

}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
