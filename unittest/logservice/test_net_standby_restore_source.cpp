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

#include "src/share/backup/ob_log_restore_struct.h"
#include <gtest/gtest.h>

namespace oceanbase
{
using namespace common;
using namespace palf;

namespace unittest
{

TEST(TestRestoreSource, test_net_standby_restore_source)
{
  ObSqlString source_str;
  char original_str[1000] = "ip_list=127.0.0.1:1001;127.0.0.1:1002,USER=restore_user@primary_tenant,PASSWORD=38344B42344C61477232324839306F3093231FF975D957829E4813195215E5B0,TENANT_ID=9223372036854775807,CLUSTER_ID=4294967295,COMPATIBILITY_MODE=MYSQL,IS_ENCRYPTED=false";
  char passwd_str[OB_MAX_PASSWORD_LENGTH + 1] = { 0 };
  source_str.assign(original_str);

  ObSqlString user_and_tenant;
  ObSqlString ip_list;
  ObSqlString user_str;
  ObSqlString tenant_id_str;
  ObSqlString cluster_id_str;
  ObSqlString mode_str;

  share::ObRestoreSourceServiceAttr attr, tmp_attr;
  (void)attr.parse_service_attr_from_str(source_str);

  EXPECT_TRUE(attr.is_valid());
  ASSERT_EQ(OB_SUCCESS, tmp_attr.assign(attr));
  EXPECT_TRUE(tmp_attr.is_valid());

// check attr.user_
  EXPECT_EQ(attr.user_.tenant_id_, 9223372036854775807);
  EXPECT_EQ(attr.user_.cluster_id_, 4294967295);
  EXPECT_EQ(0, STRCMP(attr.user_.user_name_, "restore_user"));
  EXPECT_EQ(0, STRCMP(attr.user_.tenant_name_, "primary_tenant"));
  EXPECT_EQ(attr.user_.mode_, ObCompatibilityMode::MYSQL_MODE);
  EXPECT_TRUE(attr.user_ == tmp_attr.user_);


// check attr
  common::ObAddr addr1(common::ObAddr::VER::IPV4, "127.0.0.1", 1001);
  EXPECT_TRUE(attr.addr_.at(0) == addr1);
  common::ObAddr addr2(common::ObAddr::VER::IPV4, "127.0.0.1", 1002);
  EXPECT_TRUE(attr.addr_.at(1) == addr2);

  (void)attr.get_ip_list_str_(ip_list);
  EXPECT_EQ(0, STRCMP(ip_list.ptr(),"127.0.0.1:1001;127.0.0.1:1002"));

  (void)attr.get_user_str_(user_and_tenant);
  EXPECT_EQ(0, STRCMP(user_and_tenant.ptr(), "restore_user@primary_tenant"));

  (void)attr.get_tenant_id_str_(tenant_id_str);
  EXPECT_EQ(0, STRCMP(tenant_id_str.ptr(), "9223372036854775807"));

  (void)attr.get_cluster_id_str_(cluster_id_str);
  EXPECT_EQ(0, STRCMP(cluster_id_str.ptr(), "4294967295"));

  (void)attr.get_compatibility_mode_str_(mode_str);
  EXPECT_EQ(0, STRCMP(mode_str.ptr(), "MYSQL"));


  EXPECT_TRUE(attr.service_user_is_valid());
  EXPECT_TRUE(attr.service_host_is_valid());
  EXPECT_TRUE(attr.is_valid());
  EXPECT_TRUE(attr.compare_addr_(tmp_attr.addr_));
  EXPECT_TRUE(attr == tmp_attr);

// check invalid attr
  share::ObRestoreSourceServiceAttr attr1, attr2, attr3;
  (void)attr1.assign(attr);
  attr1.set_service_cluster_id("-1");
  EXPECT_FALSE(attr1.is_valid());

  (void)attr2.assign(attr);
  attr2.set_service_tenant_id("0");
  EXPECT_FALSE(attr2.is_valid());

  (void)attr3.assign(attr);
  attr3.addr_.destroy();
  EXPECT_FALSE(attr3.is_valid());
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_net_standby_restore_source.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
