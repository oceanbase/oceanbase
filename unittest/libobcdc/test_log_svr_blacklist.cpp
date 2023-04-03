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
#include "share/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "logservice/libobcdc/src/ob_log_svr_blacklist.h"

using namespace oceanbase;
using namespace common;
using namespace libobcdc;

namespace oceanbase
{
namespace unittest
{

class SvrBlacklist : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST(SvrBlacklist, BasicTest1)
{
  int err = OB_SUCCESS;
  ObLogSvrBlacklist svr_blacklist;
  const char *svr_blacklist_str ="|";
  const bool is_sql_server = false;

  err = svr_blacklist.init(svr_blacklist_str, is_sql_server);
  EXPECT_EQ(OB_SUCCESS, err);

  ObAddr svr1(ObAddr::IPV4, "127.0.0.1", 2880);
  EXPECT_FALSE(svr_blacklist.is_exist(svr1));
  ObAddr svr2(ObAddr::IPV4, "127.0.0.2", 2881);
  EXPECT_FALSE(svr_blacklist.is_exist(svr2));
  ObAddr svr3(ObAddr::IPV4, "127.0.0.3", 2882);
  EXPECT_FALSE(svr_blacklist.is_exist(svr3));

  ObAddr svr4(ObAddr::IPV4, "127.0.0.1", 2881);
  EXPECT_FALSE(svr_blacklist.is_exist(svr4));
  ObAddr svr5(ObAddr::IPV4, "127.0.0.4", 2881);
  EXPECT_FALSE(svr_blacklist.is_exist(svr5));

  svr_blacklist.destroy();
}

TEST(SvrBlacklist, BasicTest2)
{
  int err = OB_SUCCESS;

  ObLogSvrBlacklist svr_blacklist;
  const char *svr_blacklist_str ="127.0.0.1:2880";
  const bool is_sql_server = false;

  err = svr_blacklist.init(svr_blacklist_str, is_sql_server);
  EXPECT_EQ(OB_SUCCESS, err);
  ObAddr svr1(ObAddr::IPV4, "127.0.0.1", 2880);
  EXPECT_TRUE(svr_blacklist.is_exist(svr1));
  ObAddr svr2(ObAddr::IPV4, "127.0.0.2", 2881);
  EXPECT_FALSE(svr_blacklist.is_exist(svr2));
  ObAddr svr3(ObAddr::IPV4, "127.0.0.3", 2882);
  EXPECT_FALSE(svr_blacklist.is_exist(svr3));

  const char *svr_blacklist_str2="127.0.0.1:2880|127.0.0.2:2881|127.0.0.3:2882";
  svr_blacklist.refresh(svr_blacklist_str2);
  EXPECT_TRUE(svr_blacklist.is_exist(svr1));
  EXPECT_TRUE(svr_blacklist.is_exist(svr2));
  EXPECT_TRUE(svr_blacklist.is_exist(svr3));

  svr_blacklist.destroy();
}

TEST(SvrBlacklist, BasicTest3)
{
  int err = OB_SUCCESS;

  ObLogSvrBlacklist svr_blacklist;
  const char *svr_blacklist_str ="127.0.0.1:2880|127.0.0.2:2881|127.0.0.3:2882";
  const bool is_sql_server = false;

  err = svr_blacklist.init(svr_blacklist_str, is_sql_server);
  EXPECT_EQ(OB_SUCCESS, err);

  ObAddr svr1(ObAddr::IPV4, "127.0.0.1", 2880);
  EXPECT_TRUE(svr_blacklist.is_exist(svr1));
  ObAddr svr2(ObAddr::IPV4, "127.0.0.2", 2881);
  EXPECT_TRUE(svr_blacklist.is_exist(svr2));
  ObAddr svr3(ObAddr::IPV4, "127.0.0.3", 2882);
  EXPECT_TRUE(svr_blacklist.is_exist(svr3));

  ObAddr svr4(ObAddr::IPV4, "127.0.0.1", 2881);
  EXPECT_FALSE(svr_blacklist.is_exist(svr4));
  ObAddr svr5(ObAddr::IPV4, "127.0.0.4", 2881);
  EXPECT_FALSE(svr_blacklist.is_exist(svr5));

  svr_blacklist.destroy();
}

}
}

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_log_svr_blacklist.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc,argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
