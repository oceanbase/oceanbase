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
#include "db_initializer.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace common;

TEST(TestDBInitializer, init)
{
  DBInitializer initer;
  int ret = initer.init();
  ASSERT_EQ(OB_SUCCESS, ret);
}

// TEST(TestDBInitializer, create_system_table)
// {
//   {
//     DBInitializer initer;
//     int ret = initer.init();
//     ASSERT_EQ(OB_SUCCESS, ret);

//     const bool only_core_tables = true;
//     ret = initer.create_system_table(only_core_tables);
//     ASSERT_EQ(OB_SUCCESS, ret);
//   }

//   {
//     DBInitializer initer;
//     int ret = initer.init();
//     ASSERT_EQ(OB_SUCCESS, ret);

//     const bool only_core_tables = false;
//     ret = initer.create_system_table(only_core_tables);
//     ASSERT_EQ(OB_SUCCESS, ret);

//     ret = initer.fill_sys_stat_table();
//     ASSERT_EQ(OB_SUCCESS, ret);

//     ret = initer.create_tenant_space(2);
//     ASSERT_EQ(OB_SUCCESS, ret);
//   }
// }

TEST(TestDBInitializer, inactive_sql_client)
{
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  DBInitializer initer;
  int ret = initer.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMySQLProxy &proxy = initer.get_sql_proxy();
  proxy.set_inactive();

  ObSqlString sql;
  sql.assign("select 1");
  int64_t ar = false;
  ret = proxy.write(sql.ptr(), ar);
  ASSERT_EQ(OB_INACTIVE_SQL_CLIENT, ret);

  {
    ObISQLClient::ReadResult rs;
    ret = proxy.read(rs, sql.ptr());
    ASSERT_EQ(OB_INACTIVE_SQL_CLIENT, ret);
  }

  {
    ObMySQLTransaction trans;
    ret = trans.start(&proxy, tenant_id);
    ASSERT_EQ(OB_INACTIVE_SQL_CLIENT, ret);
  }

  {
    proxy.set_active();
    ObMySQLTransaction trans;
    ret = trans.start(&proxy, tenant_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    proxy.set_inactive();
    ret = proxy.write(sql.ptr(), ar);
    ASSERT_EQ(OB_INACTIVE_SQL_CLIENT, ret);
  }
}

} // end namespace schema
} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  return RUN_ALL_TESTS();
}

