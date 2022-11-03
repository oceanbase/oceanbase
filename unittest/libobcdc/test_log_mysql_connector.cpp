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

#include <cstdlib>

#include "gtest/gtest.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/net/ob_addr.h"

#include "ob_log_mysql_connector.h"

using namespace oceanbase;
using namespace common;
using namespace libobcdc;

namespace oceanbase
{
namespace unittest
{

class PrintRow : public MySQLQueryBase
{
public:
  PrintRow()
  {
    sql_ = "select table_id, partition_id, ip, port, role "
           "from __all_root_table";
    sql_len_ = strlen(sql_);
  }
  int print_row()
  {
    int ret = common::OB_SUCCESS;
    while (common::OB_SUCCESS == (ret = next_row())) {
      uint64_t table_id = 0;
      int32_t partition_id = 0;
      ObAddr addr;
      if (OB_SUCC(ret)) {
        if (common::OB_SUCCESS != (ret = get_uint(0, table_id))) {
          OBLOG_LOG(WARN, "err get uint", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t val = 0;
        if (common::OB_SUCCESS != (ret = get_int(1, val))) {
          OBLOG_LOG(WARN, "err get int", K(ret));
        } else {
          partition_id = static_cast<int32_t>(val);
        }
      }
      if (OB_SUCC(ret)) {
        ObString ip_str;
        int64_t port = 0;
        if (common::OB_SUCCESS != (ret = get_varchar(2, ip_str))) {
          OBLOG_LOG(WARN, "err get var char", K(ret));
        } else if (common::OB_SUCCESS != (ret = get_int(3, port))) {
          OBLOG_LOG(WARN, "err get int", K(ret));
        } else {
          addr.set_ip_addr(ip_str, static_cast<int32_t>(port));
        }
      }
      // Print values.
      if (OB_SUCC(ret)) {
        OBLOG_LOG(INFO, "\n>>>", K(table_id),
                                 K(partition_id),
                                 K(addr));
      }
    }
    ret = (common::OB_ITER_END == ret) ? common::OB_SUCCESS : ret;
    return ret;
  }
};

class CreateTable : public MySQLQueryBase
{
public:
  CreateTable(const char *tname)
  {
    snprintf(buf_, 512, "create table %s(c1 int primary key)", tname);
    sql_ = buf_;
    sql_len_ = strlen(sql_);
  }
private:
  char buf_[512];
};

TEST(MySQLConnector, run)
{
  ConnectorConfig cfg;
  cfg.mysql_addr_ = "10.210.177.162";
  cfg.mysql_port_ = 26556;
  cfg.mysql_user_ = "root";
  cfg.mysql_password_ = "";
  cfg.mysql_db_ = "oceanbase";
  cfg.mysql_timeout_ = 100;

  ObLogMySQLConnector conn;

  int ret = conn.init(cfg);
  EXPECT_EQ(OB_SUCCESS, ret);

  // Print rows.
  PrintRow pr;
  ret = conn.query(pr);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = pr.print_row();
  EXPECT_EQ(OB_SUCCESS, ret);

  // Create dup tables.
  CreateTable ct("table_1");
  ret = conn.exec(ct);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = conn.exec(ct);
  EXPECT_EQ(OB_SUCCESS, ret);

  ret = conn.destroy();
  EXPECT_EQ(OB_SUCCESS, ret);

}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("debug");
  testing::InitGoogleTest(&argc,argv);
   testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  return RUN_ALL_TESTS();
}
