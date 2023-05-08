#/**
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
#include "create_restore_tenant.h"
namespace oceanbase
{
namespace unittest
{
class ObLogRestoreMySQLProxy : public ObLogRestoreProxyTest
{
};

TEST_F(ObLogRestoreMySQLProxy, test_mysql_connector)
{
  const char *user = MYSQL_USER;
  const char *passwd = PASSWD;
  const char *passwd_new = PASSWD_NEW;
  const char *db_name = MYSQL_DB;
  const bool oracle_mode = false;
  const uint64_t fake_tenant_id = 1002;
  int64_t tenant_id = 0;
  common::ObMySQLProxy *mysql_proxy = NULL;

  // create tenant and user
  create_test_tenant(oracle_mode);
  create_user();
  grant_user();

  common::ObAddr addr1(common::ObAddr::VER::IPV4, "127.0.0.1", 100010);
  common::ObAddr addr2;
  common::ObAddr addr3(common::ObAddr::VER::IPV4, "127.0.0.1", 31033);
  get_server(addr2);
  common::ObArray<common::ObAddr> list;
  ASSERT_EQ(OB_SUCCESS, list.push_back(addr1));
  ASSERT_EQ(OB_SUCCESS, list.push_back(addr2));
  ASSERT_EQ(OB_SUCCESS, list.push_back(addr3));

  // init proxy and test it
  share::ObLogRestoreProxyUtil proxy;
  ASSERT_EQ(OB_SUCCESS, proxy.init(fake_tenant_id, list, user, passwd, db_name));
  ASSERT_EQ(OB_SUCCESS, proxy.get_sql_proxy(mysql_proxy));
  ASSERT_EQ(OB_ITER_END, query(mysql_proxy, tenant_id));
  ASSERT_EQ(1002, tenant_id);

  // modify user passwd and test connection refresh
  modify_passwd();
  ASSERT_EQ(OB_SUCCESS, proxy.refresh_conn(list, user, passwd_new, db_name));
  tenant_id = 0;
  ASSERT_EQ(OB_ITER_END, query(mysql_proxy, tenant_id));
  ASSERT_EQ(1002, tenant_id);
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_restore_mysql_proxy.log");
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_restore_mysql_proxy");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
