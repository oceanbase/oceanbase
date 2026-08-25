/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#include "observer/virtual_table/ob_gv_sql_audit.h"
#undef protected
#undef private
#include "lib/worker.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

TEST(TestGvSqlAudit, oracle_null_request_id_upper_bound)
{
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::ORACLE);
  ObGvSqlAudit audit;
  ObObj start_objs[4];
  ObObj end_objs[4];
  start_objs[0].set_int(0);
  start_objs[1].set_int(0);
  start_objs[2].set_uint64(1002);
  start_objs[3].set_int(382);
  end_objs[0].set_int(0);
  end_objs[1].set_int(0);
  end_objs[2].set_uint64(1002);
  end_objs[3].set_int(0);
  end_objs[3].set_null();

  ObNewRange range;
  range.start_key_.assign(start_objs, ARRAYSIZEOF(start_objs));
  range.end_key_.assign(end_objs, ARRAYSIZEOF(end_objs));
  ASSERT_EQ(OB_SUCCESS, audit.key_ranges_.push_back(range));

  int64_t start_id = -1;
  int64_t end_id = -1;
  bool is_valid = false;
  ASSERT_EQ(OB_SUCCESS, audit.extract_request_ids(1002, start_id, end_id, is_valid));
  ASSERT_EQ(382, start_id);
  ASSERT_EQ(INT64_MAX, end_id);
  ASSERT_TRUE(is_valid);
}

TEST(TestGvSqlAudit, mysql_null_request_id_upper_bound_is_unchanged)
{
  lib::CompatModeGuard mode_guard(lib::Worker::CompatMode::MYSQL);
  ObGvSqlAudit audit;
  ObObj start_objs[4];
  ObObj end_objs[4];
  start_objs[0].set_int(0);
  start_objs[1].set_int(0);
  start_objs[2].set_uint64(1002);
  start_objs[3].set_int(382);
  end_objs[0].set_int(0);
  end_objs[1].set_int(0);
  end_objs[2].set_uint64(1002);
  end_objs[3].set_int(0);
  end_objs[3].set_null();

  ObNewRange range;
  range.start_key_.assign(start_objs, ARRAYSIZEOF(start_objs));
  range.end_key_.assign(end_objs, ARRAYSIZEOF(end_objs));
  ASSERT_EQ(OB_SUCCESS, audit.key_ranges_.push_back(range));

  int64_t start_id = -1;
  int64_t end_id = -1;
  bool is_valid = true;
  ASSERT_EQ(OB_SUCCESS, audit.extract_request_ids(1002, start_id, end_id, is_valid));
  ASSERT_EQ(382, start_id);
  ASSERT_EQ(1, end_id);
  ASSERT_FALSE(is_valid);
}

} // namespace observer
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
