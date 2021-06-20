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

#include "lib/allocator/ob_sql_arena_allocator.h"
#include "lib/ob_define.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "lib/alloc/ob_malloc_allocator.h"
using namespace oceanbase::common;
using namespace oceanbase::lib;

class TestSQLArenaAllocator : public ::testing::Test {
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
{}

TestSQLArenaAllocator::~TestSQLArenaAllocator()
{}

void TestSQLArenaAllocator::SetUp()
{}

void TestSQLArenaAllocator::TearDown()
{}

#define GET_DEFAULT()                                                               \
  ({                                                                                \
    ma->get_tenant_ctx_mod_usage(tenant_id, ObCtxIds::DEFAULT_CTX_ID, label, item); \
    int64_t hold = item.hold_;                                                      \
    hold;                                                                           \
  })
#define GET_AREA()                                                             \
  ({                                                                           \
    ma->get_tenant_ctx_mod_usage(tenant_id, ObCtxIds::WORK_AREA, label, item); \
    int64_t hold = item.hold_;                                                 \
    hold;                                                                      \
  })

TEST_F(TestSQLArenaAllocator, basic)
{
  uint64_t tenant_id = OB_SERVER_TENANT_ID;
  const lib::ObLabel& label = ObModIds::OB_SQL_ARENA;
  ObMallocAllocator* ma = ObMallocAllocator::get_instance();
  ObModItem item;
  ObSQLArenaAllocator sql_arena(tenant_id);
  int64_t default_hold = GET_DEFAULT();
  int64_t area_hold = GET_AREA();
  ASSERT_EQ(OB_SUCCESS, ma->create_tenant_ctx_allocator(tenant_id, ObCtxIds::WORK_AREA));

  void* ptr = sql_arena.alloc((1 << 20) - 1);
  ASSERT_NE(nullptr, ptr);
  ASSERT_GT(GET_DEFAULT(), default_hold);
  default_hold = GET_DEFAULT();
  int64_t new_area_hold = GET_AREA();
  ASSERT_EQ(new_area_hold, area_hold);

  ptr = sql_arena.alloc(1 << 20);
  ASSERT_NE(nullptr, ptr);
  int64_t new_default_hold = GET_DEFAULT();
  ASSERT_EQ(new_default_hold, default_hold);

  ASSERT_GT(GET_AREA(), area_hold);
  area_hold = GET_AREA();

  // reset remain one page
  int64_t total = sql_arena.total();
  sql_arena.reset_remain_one_page();
  ASSERT_GT(total, sql_arena.total());
  total = sql_arena.total();

  // reset()
  sql_arena.reset();
  ASSERT_EQ(0, sql_arena.total());
  default_hold = GET_DEFAULT();
  area_hold = GET_AREA();
  ASSERT_EQ(0, default_hold);
  ASSERT_EQ(0, area_hold);

  int64_t last_default_hold = 0;
  int64_t last_area_hold = 0;
  bool flag = false;
  int cnt = 0;
  do {
    last_default_hold = default_hold;
    last_area_hold = area_hold;
    ptr = sql_arena.alloc((1 << 20) / 2);
    ASSERT_NE(nullptr, ptr);
    default_hold = GET_DEFAULT();
    area_hold = GET_AREA();
    if (area_hold > last_area_hold) {
      ASSERT_EQ(last_default_hold, default_hold);
      flag = true;
      ptr = sql_arena.alloc(2 << 20);
      ASSERT_NE(nullptr, ptr);
      ASSERT_GT(GET_AREA(), area_hold);
      break;
    } else {
      cnt++;
      ASSERT_EQ(last_area_hold, area_hold);
    }
  } while (true);
  ASSERT_TRUE(flag);
  ASSERT_GT(cnt, 3);

  // tracer
  total = sql_arena.total();
  ASSERT_TRUE(sql_arena.set_tracer());
  ptr = sql_arena.alloc(4 << 20);
  ASSERT_NE(nullptr, ptr);
  ASSERT_GT(sql_arena.total(), total);
  ASSERT_TRUE(sql_arena.revert_tracer());
  ASSERT_EQ(sql_arena.total(), total);

  // deconstruct
  ptr = sql_arena.alloc(1 << 20);
  ASSERT_NE(nullptr, ptr);
  sql_arena.~ObSQLArenaAllocator();
  ASSERT_EQ(0, sql_arena.total());
  default_hold = GET_DEFAULT();
  area_hold = GET_AREA();
  ASSERT_EQ(default_hold, 0);
  ASSERT_EQ(area_hold, 0);
}

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
