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
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "sql/resolver/ob_resolver_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class TestResovlerUtils : public ::testing::Test
{
public:
  TestResovlerUtils() = default;
  ~TestResovlerUtils() = default;
};

TEST_F(TestResovlerUtils, check_secure_path)
{
  int ret = OB_SUCCESS;

  {
    ObString secure_file_priv("/");
    ObString directory_path1("/tmp");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path1);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString directory_path2("/Tmp");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path2);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString directory_path3("/");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path3);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  {
    ObString secure_file_priv("null");
    ObString directory_path1("/tmp");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path1);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
    ObString directory_path2("/");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path2);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
  }

  {
    ObString secure_file_priv("/tmp");
    ObString directory_path1("/tmp");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path1);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString directory_path2("/Tmp");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path2);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
    ObString directory_path3("/tmp/test");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString directory_path4("/home");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path4);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
    ObString directory_path5("/tmpabc");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path5);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
  }

  {
    ObString secure_file_priv("/tmp/");
    ObString directory_path1("/tmp");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path1);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString directory_path2("/Tmp");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path2);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
    ObString directory_path3("/tmp/test");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString directory_path4("/home");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path4);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
    ObString directory_path5("/tmpabc");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path5);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
  }

  {
    system("mkdir -p /tmp/test_resolver_utils_check_secure_path");
    ObString secure_file_priv("/tmp/test_resolver_utils_check_secure_path");
    ObString directory_path1("/");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path1);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
    ObString directory_path2("/a");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path2);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
    ObString directory_path3("/tmp");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path3);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
    ObString directory_path4("/tmp/test_resolver_utils_check_secure_path");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path4);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObString directory_path5("/TMP/test_resolver_utils_check_secure_path");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path5);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
    ObString directory_path6("/tmp/test_resolver_utils_Check_Secure_Path");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path6);
    ASSERT_EQ(OB_ERR_NO_PRIVILEGE, ret);
    ObString directory_path7("/tmp/test_resolver_utils_check_secure_path/hehe");
    ret = ObResolverUtils::check_secure_path(secure_file_priv, directory_path7);
    ASSERT_EQ(OB_SUCCESS, ret);
    system("rm -rf /tmp/test_resolver_utils_check_secure_path");
  }
}
} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_resolver_utils.log*");
  OB_LOGGER.set_file_name("test_resolver_utils.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}