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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "share/ob_encryption_util.h"
#include "share/ob_web_service_root_addr.h"
#include "share/config/ob_system_config.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
using namespace common;
namespace share {
TEST(TestEncryptionUtil, basic)
{
  const int64_t invalid_buf_len = 12;
  char invalid_key[32] = "aabb";
  char invalid_data[invalid_buf_len] = "123456789";
  const int64_t buf_len = 128;
  char key[32] = "abababab";
  char origin_data[buf_len] = "123456789";
  char origin_data2[buf_len] = "12345678";
  char data[buf_len] = "123456789";
  char data2[buf_len] = "12345678";
  int64_t invalid_data_len = strlen(invalid_data);
  int64_t data_len = strlen(data);
  ASSERT_EQ(
      OB_INVALID_ARGUMENT, ObDesEncryption::des_encrypt(invalid_key, invalid_data, invalid_data_len, invalid_buf_len));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObDesEncryption::des_encrypt(invalid_key, data, data_len, invalid_buf_len));
  ASSERT_EQ(OB_SUCCESS, ObDesEncryption::des_encrypt(key, data, data_len, buf_len));
  ASSERT_EQ(OB_SUCCESS, ObDesEncryption::des_decrypt(key, data, 16));
  ASSERT_EQ(0, STRNCMP(data, origin_data, strlen(origin_data)));
  ASSERT_EQ(OB_SUCCESS, ObDesEncryption::des_encrypt(key, data2, data_len, buf_len));
  ASSERT_EQ(OB_SUCCESS, ObDesEncryption::des_decrypt(key, data2, 8));
  ASSERT_EQ(0, STRNCMP(data2, origin_data2, strlen(origin_data2)));
}

// TEST(TestWebService, store)
//{
//  ObWebServiceRootAddr ws;
//  ObSystemConfig sys_config;
//  ASSERT_EQ(OB_SUCCESS, sys_config.init());
//  ObServerConfig &config = ObServerConfig::get_instance();
//  ASSERT_EQ(OB_SUCCESS, config.init(sys_config));
//  ws.init(config);
//  config.obconfig_url.set_value("http://api.test.ocp.oceanbase.alibaba.net/services?Action=ObRootServiceInfo&User_ID=ocptest&UID=rongwei.drw&ObRegion=xr.admin");
//  config.cluster_id.set_value("1");
//  config.cluster.set_value("xr.admin");
//  ObArray<ObRootAddr> rs_list;
//  ObArray<ObRootAddr> readonly_rs_list;
//  for (int64_t i = 0; i < 10; i++) {
//    ObRootAddr rs;
//    rs.server_.set_ip_addr("127.0.0.1", 9988);
//    rs.sql_port_ = 1;
//    ASSERT_EQ(OB_SUCCESS, rs_list.push_back(rs));
//  }
//  for (int64_t i = 0; i < 5; i++) {
//    ObRootAddr rs;
//    rs.server_.set_ip_addr("127.0.0.1", 9988);
//    rs.sql_port_ = 1;
//    ASSERT_EQ(OB_SUCCESS, readonly_rs_list.push_back(rs));
//  }
//  ASSERT_EQ(OB_SUCCESS, ws.store(rs_list, readonly_rs_list, true));
//  for (int64_t i = 0; i < 800; i++) {
//    ObRootAddr rs;
//    rs.server_.set_ip_addr("127.0.0.1", 9988);
//    rs.sql_port_ = 1;
//    ASSERT_EQ(OB_SUCCESS, rs_list.push_back(rs));
//  }
//  for (int64_t i = 0; i < 300; i++) {
//    ObRootAddr rs;
//    rs.server_.set_ip_addr("127.0.0.1", 9988);
//    rs.sql_port_ = 1;
//    ASSERT_EQ(OB_SUCCESS, readonly_rs_list.push_back(rs));
//  }
//  ASSERT_EQ(OB_OBCONFIG_RETURN_ERROR, ws.store(rs_list, readonly_rs_list, true));
//
//}
}  // end namespace share
}  // end namespace oceanbase
int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
