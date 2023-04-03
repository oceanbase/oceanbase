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

#include "lib/ob_define.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "lib/alloc/ob_malloc_allocator.h"
using namespace oceanbase::common;
using namespace oceanbase::lib;

class TestSQLArenaAllocator: public ::testing::Test
{
public:
  TestSQLArenaAllocator();
  virtual ~TestSQLArenaAllocator();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestSQLArenaAllocator);
protected:
  // function members
protected:
  // data members
};

TestSQLArenaAllocator::TestSQLArenaAllocator()
{
}

TestSQLArenaAllocator::~TestSQLArenaAllocator()
{
}

void TestSQLArenaAllocator::SetUp()
{
}

void TestSQLArenaAllocator::TearDown()
{
}

#define GET_DEFAULT() \
  ({ \
    ma->get_tenant_ctx_mod_usage(tenant_id, ObCtxIds::DEFAULT_CTX_ID, label, item); \
    int64_t hold = item.hold_; \
    hold; \
  })
#define GET_AREA() \
  ({ \
    ma->get_tenant_ctx_mod_usage(tenant_id, ObCtxIds::WORK_AREA, label, item); \
    int64_t hold = item.hold_; \
    hold; \
  })

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
