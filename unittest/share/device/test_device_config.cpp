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
#include "lib/file/file_directory_utils.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "share/object_storage/ob_device_config_parser.h"
#define private public
#undef private

namespace oceanbase {
namespace unittest {

using namespace oceanbase::common;
using namespace oceanbase::share;

class TestDeviceConfig: public ::testing::Test
{
public:
  TestDeviceConfig() {}
  virtual ~TestDeviceConfig() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestDeviceConfig);
};

TEST_F(TestDeviceConfig, parser_and_dump_load_and_get)
{
  // 1. test device config parser
  ObDeviceConfig config;
  const char *used_for_str = "used_for=LOG";
  const char *path_str = "path=oss://bucket-name/root_dir";
  const char *endpoint_str = "endpoint=host=cn-xxx.com";
  const char *access_info_str = "access_info=access_id=aaa&encrypt_key=bbb";
  const char *extension_str = "extension=appid=123&checksum_type=crc32";
  const char *state_str = "state=ADDED";
  const char *state_info_str = "state_info=is_connective=true";
  const char *create_timestamp_str = "create_timestamp=1679579590156";
  const char *last_check_timestamp_str = "last_check_timestamp=1679579590256";
  const char *op_id_str = "op_id=1";
  const char *sub_op_id_str = "sub_op_id=2";
  const char *storage_id_str = "storage_id=1";
  const char *max_iops_str = "max_iops=10000";
  const char *max_bandwidth_str = "max_bandwidth=1024000000";
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(used_for_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(path_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(endpoint_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(access_info_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(extension_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(state_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(state_info_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(create_timestamp_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(last_check_timestamp_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(op_id_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(sub_op_id_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(storage_id_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(max_iops_str, config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_device_config_field(max_bandwidth_str, config));
  ASSERT_EQ(0, STRCMP(config.used_for_, "LOG"));
  ASSERT_EQ(0, STRCMP(config.path_, "oss://bucket-name/root_dir"));
  ASSERT_EQ(0, STRCMP(config.endpoint_, "host=cn-xxx.com"));
  ASSERT_EQ(0, STRCMP(config.access_info_, "access_id=aaa&encrypt_key=bbb"));
  ASSERT_EQ(0, STRCMP(config.extension_, "appid=123&checksum_type=crc32"));
  ASSERT_EQ(0, STRCMP(config.state_, "ADDED"));
  ASSERT_EQ(0, STRCMP(config.state_info_, "is_connective=true"));
  ASSERT_EQ(1679579590156, config.create_timestamp_);
  ASSERT_EQ(1679579590256, config.last_check_timestamp_);
  ASSERT_EQ(1, config.op_id_);
  ASSERT_EQ(2, config.sub_op_id_);
  ASSERT_EQ(1, config.storage_id_);
  ASSERT_EQ(10000, config.max_iops_);
  ASSERT_EQ(1024000000, config.max_bandwidth_);


  // 2. test device manifest dump2file and load
  ObDeviceManifest device_manifest;
  ObArray<ObDeviceConfig> dump_configs;
  ObArray<ObDeviceConfig> load_configs;
  ASSERT_EQ(OB_SUCCESS, dump_configs.push_back(config));
  ObDeviceConfig config2 = config;
  MEMSET(config2.used_for_, 0, sizeof(config2.used_for_));
  STRCPY(config2.used_for_, "DATA");
  MEMSET(config2.old_access_info_, 0, sizeof(config2.old_access_info_));
  STRCPY(config2.old_access_info_, "access_id=aaa&encrypt_key=bbb");
  MEMSET(config2.access_info_, 0, sizeof(config2.access_info_));
  STRCPY(config2.access_info_, "access_id=ccccc&encrypt_key=ddddd");
  MEMSET(config2.old_extension_, 0, sizeof(config2.old_extension_));
  STRCPY(config2.old_extension_, "appid=123&checksum_type=crc32");
  MEMSET(config2.extension_, 0, sizeof(config2.extension_));
  STRCPY(config2.extension_, "appid=456&checksum_type=md5");
  config2.op_id_ = 3;
  config2.sub_op_id_ = 4;
  config2.storage_id_ = 2;
  ASSERT_EQ(OB_SUCCESS, dump_configs.push_back(config2));
  ObDeviceManifest::HeadSection dump_head;
  dump_head.device_num_ = 2;
  dump_head.modify_timestamp_us_ = ObTimeUtil::fast_current_time();
  dump_head.last_op_id_ = 3;
  dump_head.last_sub_op_id_ = 4;
  ObDeviceManifest::HeadSection load_head;

  const char *data_dir = "./TestDeviceConfigDir";
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::create_directory(data_dir));
  ASSERT_EQ(OB_SUCCESS, device_manifest.init(data_dir));
  ASSERT_EQ(OB_SUCCESS, device_manifest.dump2file(dump_configs, dump_head));
  ASSERT_EQ(OB_SUCCESS, device_manifest.load(load_configs, load_head));

  // sort by used_for, path and endpoint
  ObArray<ObDeviceConfig> sorted_dump_configs;
  ASSERT_EQ(OB_SUCCESS, sorted_dump_configs.push_back(dump_configs[1]));
  ASSERT_EQ(OB_SUCCESS, sorted_dump_configs.push_back(dump_configs[0]));
  ASSERT_EQ(2, load_configs.count());
  for (int64_t i = 0; i < load_configs.count(); ++i) {
    ASSERT_EQ(0, STRCMP(sorted_dump_configs[i].used_for_, load_configs[i].used_for_));
    ASSERT_EQ(0, STRCMP(sorted_dump_configs[i].path_, load_configs[i].path_));
    ASSERT_EQ(0, STRCMP(sorted_dump_configs[i].endpoint_, load_configs[i].endpoint_));
    ASSERT_EQ(0, STRCMP(sorted_dump_configs[i].access_info_, load_configs[i].access_info_));
    ASSERT_EQ(0, STRCMP(sorted_dump_configs[i].encrypt_info_, load_configs[i].encrypt_info_));
    ASSERT_EQ(0, STRCMP(sorted_dump_configs[i].extension_, load_configs[i].extension_));
    ASSERT_EQ(0, STRCMP(sorted_dump_configs[i].old_access_info_, load_configs[i].old_access_info_));
    ASSERT_EQ(0, STRCMP(sorted_dump_configs[i].old_encrypt_info_, load_configs[i].old_encrypt_info_));
    ASSERT_EQ(0, STRCMP(sorted_dump_configs[i].old_extension_, load_configs[i].old_extension_));
    ASSERT_EQ(0, STRCMP(sorted_dump_configs[i].state_, load_configs[i].state_));
    ASSERT_EQ(0, STRCMP(sorted_dump_configs[i].state_info_, load_configs[i].state_info_));
    ASSERT_EQ(sorted_dump_configs[i].create_timestamp_, load_configs[i].create_timestamp_);
    ASSERT_EQ(sorted_dump_configs[i].last_check_timestamp_, load_configs[i].last_check_timestamp_);
    ASSERT_EQ(sorted_dump_configs[i].op_id_, load_configs[i].op_id_);
    ASSERT_EQ(sorted_dump_configs[i].sub_op_id_, load_configs[i].sub_op_id_);
    ASSERT_EQ(sorted_dump_configs[i].storage_id_, load_configs[i].storage_id_);
    ASSERT_EQ(sorted_dump_configs[i].max_iops_, load_configs[i].max_iops_);
    ASSERT_EQ(sorted_dump_configs[i].max_bandwidth_, load_configs[i].max_bandwidth_);
  }
  ASSERT_EQ(dump_head.version_, load_head.version_);
  ASSERT_EQ(dump_head.head_checksum_, load_head.head_checksum_);
  ASSERT_EQ(dump_head.device_checksum_, load_head.device_checksum_);
  ASSERT_EQ(dump_head.device_num_, load_head.device_num_);
  ASSERT_EQ(dump_head.modify_timestamp_us_, load_head.modify_timestamp_us_);
  ASSERT_EQ(dump_head.last_op_id_, load_head.last_op_id_);
  ASSERT_EQ(dump_head.last_sub_op_id_, load_head.last_sub_op_id_);


  // 3. test device config mgr
  ObDeviceConfigMgr &config_mgr = ObDeviceConfigMgr::get_instance();
  ASSERT_EQ(OB_SUCCESS, config_mgr.init(data_dir));
  ASSERT_EQ(OB_SUCCESS, config_mgr.load_configs());
  // test get_all_device_configs
  ObArray<ObDeviceConfig> all_configs;
  ASSERT_EQ(OB_SUCCESS, config_mgr.get_all_device_configs(all_configs));
  ASSERT_EQ(2, all_configs.count());
  // test get_device_config
  ObDeviceConfig data_device_config;
  ASSERT_EQ(OB_SUCCESS, config_mgr.get_device_config(ObStorageUsedType::TYPE::USED_TYPE_DATA,
                                                     data_device_config));
  ASSERT_EQ(0, STRCMP(config2.used_for_, data_device_config.used_for_));
  ASSERT_EQ(0, STRCMP(config2.path_, data_device_config.path_));
  ASSERT_EQ(0, STRCMP(config2.endpoint_, data_device_config.endpoint_));
  ASSERT_EQ(0, STRCMP(config2.access_info_, data_device_config.access_info_));
  ASSERT_EQ(0, STRCMP(config2.encrypt_info_, data_device_config.encrypt_info_));
  ASSERT_EQ(0, STRCMP(config2.extension_, data_device_config.extension_));
  ASSERT_EQ(0, STRCMP(config2.old_access_info_, data_device_config.old_access_info_));
  ASSERT_EQ(0, STRCMP(config2.old_encrypt_info_, data_device_config.old_encrypt_info_));
  ASSERT_EQ(0, STRCMP(config2.old_extension_, data_device_config.old_extension_));
  ASSERT_EQ(0, STRCMP(config2.state_, data_device_config.state_));
  ASSERT_EQ(0, STRCMP(config2.state_info_, data_device_config.state_info_));
  ASSERT_EQ(config2.create_timestamp_, data_device_config.create_timestamp_);
  ASSERT_EQ(config2.last_check_timestamp_, data_device_config.last_check_timestamp_);
  ASSERT_EQ(config2.op_id_, data_device_config.op_id_);
  ASSERT_EQ(config2.sub_op_id_, data_device_config.sub_op_id_);
  ASSERT_EQ(config2.storage_id_, data_device_config.storage_id_);
  ASSERT_EQ(config2.max_iops_, data_device_config.max_iops_);
  ASSERT_EQ(config2.max_bandwidth_, data_device_config.max_bandwidth_);
  ObDeviceConfig log_device_config;
  ASSERT_EQ(OB_SUCCESS, config_mgr.get_device_config(ObStorageUsedType::TYPE::USED_TYPE_LOG,
                                                     log_device_config));
  ASSERT_EQ(0, STRCMP(config.used_for_, log_device_config.used_for_));
  ASSERT_EQ(0, STRCMP(config.path_, log_device_config.path_));
  ASSERT_EQ(0, STRCMP(config.endpoint_, log_device_config.endpoint_));
  ASSERT_EQ(0, STRCMP(config.access_info_, log_device_config.access_info_));
  ASSERT_EQ(0, STRCMP(config.encrypt_info_, log_device_config.encrypt_info_));
  ASSERT_EQ(0, STRCMP(config.extension_, log_device_config.extension_));
  ASSERT_EQ(0, STRCMP(config.old_access_info_, log_device_config.old_access_info_));
  ASSERT_EQ(0, STRCMP(config.old_encrypt_info_, log_device_config.old_encrypt_info_));
  ASSERT_EQ(0, STRCMP(config.old_extension_, log_device_config.old_extension_));
  ASSERT_EQ(0, STRCMP(config.state_, log_device_config.state_));
  ASSERT_EQ(0, STRCMP(config.state_info_, log_device_config.state_info_));
  ASSERT_EQ(config.create_timestamp_, log_device_config.create_timestamp_);
  ASSERT_EQ(config.last_check_timestamp_, log_device_config.last_check_timestamp_);
  ASSERT_EQ(config.op_id_, log_device_config.op_id_);
  ASSERT_EQ(config.sub_op_id_, log_device_config.sub_op_id_);
  ASSERT_EQ(config.storage_id_, log_device_config.storage_id_);
  ASSERT_EQ(config.max_iops_, log_device_config.max_iops_);
  ASSERT_EQ(config.max_bandwidth_, log_device_config.max_bandwidth_);

  // test get_last_op_id and get_last_sub_op_id
  uint64_t last_op_id = UINT64_MAX;
  uint64_t last_sub_op_id = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, config_mgr.get_last_op_id(last_op_id));
  ASSERT_EQ(OB_SUCCESS, config_mgr.get_last_sub_op_id(last_sub_op_id));
  ASSERT_EQ(3, last_op_id);
  ASSERT_EQ(4, last_sub_op_id);

  // test is_op_done
  bool is_done_3_4 = false;
  config_mgr.is_op_done(3, 4, is_done_3_4);
  ASSERT_TRUE(is_done_3_4);
  bool is_done_1_2 = false;
  config_mgr.is_op_done(1, 2, is_done_1_2);
  ASSERT_TRUE(is_done_1_2);
  bool is_done_3_5 = false;
  config_mgr.is_op_done(3, 5, is_done_3_5);
  ASSERT_FALSE(is_done_3_5);

  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::delete_directory_rec(data_dir));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::FileDirectoryUtils::delete_directory_rec("./TestDeviceConfigDir");
  system("rm -f test_device_config.log");
  OB_LOGGER.set_file_name("test_device_config.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
