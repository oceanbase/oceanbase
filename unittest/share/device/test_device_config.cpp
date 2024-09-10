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
#include "lib/utility/ob_test_util.h"
#include "lib/file/file_directory_utils.h"
#include "lib/container/ob_array_iterator.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "share/object_storage/ob_device_config_parser.h"
#include "share/object_storage/ob_device_manifest.h"
#include "share/object_storage/ob_object_storage_struct.h"
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
  virtual void SetUp()
  {
    char buf_log_id[] = "used_for=LOG&path=oss://antsys-oceanbasebackup/backup_clog_1&endpoint="
      "host=cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com&access_mode=access_by_id&access_id=a&ac"
      "cess_key=b&encrypt_info=encrypt_key=b&extension=null&old_access_mode=access_by_id&old_access"
      "_id=c&old_access_key=d&old_encrypt_info=encrypt_key=b&old_extension=null&state=ADDING&state"
      "_info=is_connective=true&create_timestamp=1679579590156&last_check_timestamp=1679579590256"
      "&op_id=1&sub_op_id=1&storage_id=1&max_iops=0&max_bandwidth=0";
    char buf_log_ram_url[] = "used_for=LOG&path=oss://antsys-oceanbasebackup/backup_clog_2&endpoint"
      "=host=cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com&access_mode=access_by_ram_url&ram_url=xxxx"
      "&state=ADDING&state_info=is_connective=true&create_timestamp=1679579590356&last_check_timest"
      "amp=1679579590456&op_id=2&sub_op_id=1&storage_id=2&max_iops=0&max_bandwidth=0";
    char buf_data[] = "used_for=DATA&path=oss://antsys-oceanbasebackup/backup_sstable&endpoint="
      "host=cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com&access_mode=access_by_id&access_id=a&ac"
      "cess_key=b&encrypt_info=encrypt_key=b&extension=null&old_access_mode=access_by_id&old_access"
      "_id=c&old_access_key=d&old_encrypt_info=encrypt_key=b&old_extension=null&state=ADDING&state"
      "_info=is_connective=false&create_timestamp=1679579590556&last_check_timestamp=1679579590656"
      "&op_id=3&sub_op_id=1&storage_id=3&max_iops=0&max_bandwidth=0";
    memcpy(buf_log_id_, buf_log_id, sizeof(buf_log_id));
    memcpy(buf_log_ram_url_, buf_log_ram_url, sizeof(buf_log_ram_url));
    memcpy(buf_data_, buf_data, sizeof(buf_data));
  }
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestDeviceConfig);
protected:
  char buf_log_id_[16384];
  char buf_log_ram_url_[16384];
  char buf_data_[16384];
};

TEST_F(TestDeviceConfig, DISABLED_test_device_config_parser)
{
  ObDeviceConfig config;
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_one_device_config(buf_log_id_, config));
  ASSERT_EQ(0, STRCMP(config.used_for_, "LOG"));
  ASSERT_EQ(0, STRCMP(config.path_, "oss://antsys-oceanbasebackup/backup_clog_1"));
  ASSERT_EQ(0, STRCMP(config.endpoint_, "host=cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com"));
  ASSERT_EQ(0, STRCMP(config.access_info_, "access_mode=access_by_id&access_id=a&access_key=b"));
  ASSERT_EQ(0, STRCMP(config.encrypt_info_, "encrypt_key=b"));
  ASSERT_EQ(0, STRCMP(config.extension_, "null"));
  ASSERT_EQ(0, STRCMP(config.old_access_info_, "old_access_mode=access_by_id&old_access_id=c&old_access_key=d"));
  ASSERT_EQ(0, STRCMP(config.old_encrypt_info_, "encrypt_key=b"));
  ASSERT_EQ(0, STRCMP(config.old_extension_, "null"));
  ASSERT_EQ(0, STRCMP(config.state_, "ADDING"));
  ASSERT_EQ(0, STRCMP(config.state_info_, "is_connective=true"));
  ASSERT_EQ(1679579590156, config.create_timestamp_);
  ASSERT_EQ(1679579590256, config.last_check_timestamp_);
  ASSERT_EQ(1, config.op_id_);
  ASSERT_EQ(1, config.sub_op_id_);
  ASSERT_EQ(1, config.storage_id_);
  ASSERT_EQ(0, config.max_iops_);
  ASSERT_EQ(0, config.max_bandwidth_);

  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_one_device_config(buf_log_ram_url_, config));
  ASSERT_EQ(0, STRCMP(config.used_for_, "LOG"));
  ASSERT_EQ(0, STRCMP(config.path_, "oss://antsys-oceanbasebackup/backup_clog_2"));
  ASSERT_EQ(0, STRCMP(config.endpoint_, "host=cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com"));
  ASSERT_EQ(0, STRCMP(config.access_info_, "access_mode=access_by_ram_url&ram_url=xxxx"));
  ASSERT_EQ(0, STRCMP(config.state_, "ADDING"));
  ASSERT_EQ(0, STRCMP(config.state_info_, "is_connective=true"));
  ASSERT_EQ(1679579590356, config.create_timestamp_);
  ASSERT_EQ(1679579590456, config.last_check_timestamp_);
  ASSERT_EQ(2, config.op_id_);
  ASSERT_EQ(1, config.sub_op_id_);
  ASSERT_EQ(2, config.storage_id_);
  ASSERT_EQ(0, config.max_iops_);
  ASSERT_EQ(0, config.max_bandwidth_);
}

TEST_F(TestDeviceConfig, DISABLED_test_device_manifest)
{
  ObDeviceManifest device_manifest;
  ObArray<ObDeviceConfig> dump_configs;
  ObArray<ObDeviceConfig> load_configs;
  ObDeviceConfig config;
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_one_device_config(buf_log_id_, config));
  ASSERT_EQ(OB_SUCCESS, dump_configs.push_back(config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_one_device_config(buf_log_ram_url_, config));
  ASSERT_EQ(OB_SUCCESS, dump_configs.push_back(config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_one_device_config(buf_data_, config));
  ASSERT_EQ(OB_SUCCESS, dump_configs.push_back(config));

  ObDeviceManifest::HeadSection dump_head;
  dump_head.version_ = 1;
  dump_head.device_num_ = 3;
  dump_head.modify_timestamp_us_ = ObTimeUtil::fast_current_time();
  dump_head.last_op_id_ = 3;
  dump_head.last_sub_op_id_ = 1;
  ObDeviceManifest::HeadSection load_head;

  const char *data_dir = "./TestDeviceConfigDir";
  ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::create_directory(data_dir));

  ASSERT_EQ(OB_SUCCESS, device_manifest.init(data_dir));
  ASSERT_EQ(OB_SUCCESS, device_manifest.dump2file(dump_configs, dump_head));
  ASSERT_EQ(OB_SUCCESS, device_manifest.load(load_configs, load_head));

  // sort by used_for, path and endpoint
  ObArray<ObDeviceConfig> sorted_dump_configs;
  ASSERT_EQ(OB_SUCCESS, sorted_dump_configs.push_back(dump_configs[2]));
  ASSERT_EQ(OB_SUCCESS, sorted_dump_configs.push_back(dump_configs[0]));
  ASSERT_EQ(OB_SUCCESS, sorted_dump_configs.push_back(dump_configs[1]));
  ASSERT_EQ(3, load_configs.count());
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

  // ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::delete_directory_rec(data_dir));
}

TEST_F(TestDeviceConfig, DISABLED_test_device_config_mgr)
{
  char data_dir[MAX_PATH_SIZE] = "./TestDeviceConfigDir";
  // char shared_storage_info[share::OB_MAX_BACKUP_DEST_LENGTH] = "file://./test_bukcet/";
  // ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::create_directory(data_dir));

  ObDeviceConfigMgr &config_mgr = ObDeviceConfigMgr::get_instance();
  ASSERT_EQ(OB_SUCCESS, config_mgr.init(data_dir));
  ASSERT_EQ(OB_SUCCESS, config_mgr.load_configs());

  // test get_all_device_configs
  ObArray<ObDeviceConfig> configs;
  ObDeviceConfig config;
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_one_device_config(buf_data_, config));
  ASSERT_EQ(OB_SUCCESS, configs.push_back(config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_one_device_config(buf_log_id_, config));
  ASSERT_EQ(OB_SUCCESS, configs.push_back(config));
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigParser::parse_one_device_config(buf_log_ram_url_, config));
  ASSERT_EQ(OB_SUCCESS, configs.push_back(config));
  ObArray<ObDeviceConfig> all_configs;
  ASSERT_EQ(OB_SUCCESS, config_mgr.get_all_device_configs(all_configs));
  ASSERT_EQ(3, all_configs.count());

  // test get_device_configs
  char used_for_data[] = "DATA";
  ObArray<ObDeviceConfig> used_for_configs;
  ASSERT_EQ(OB_SUCCESS, config_mgr.get_device_configs(used_for_data, used_for_configs));
  ASSERT_EQ(1, used_for_configs.count());
  ASSERT_EQ(0, STRCMP(configs[0].used_for_, used_for_configs[0].used_for_));
  ASSERT_EQ(0, STRCMP(configs[0].path_, used_for_configs[0].path_));
  ASSERT_EQ(0, STRCMP(configs[0].endpoint_, used_for_configs[0].endpoint_));
  ASSERT_EQ(0, STRCMP(configs[0].access_info_, used_for_configs[0].access_info_));
  ASSERT_EQ(0, STRCMP(configs[0].encrypt_info_, used_for_configs[0].encrypt_info_));
  ASSERT_EQ(0, STRCMP(configs[0].extension_, used_for_configs[0].extension_));
  ASSERT_EQ(0, STRCMP(configs[0].old_access_info_, used_for_configs[0].old_access_info_));
  ASSERT_EQ(0, STRCMP(configs[0].old_encrypt_info_, used_for_configs[0].old_encrypt_info_));
  ASSERT_EQ(0, STRCMP(configs[0].old_extension_, used_for_configs[0].old_extension_));
  ASSERT_EQ(0, STRCMP(configs[0].state_, used_for_configs[0].state_));
  ASSERT_EQ(0, STRCMP(configs[0].state_info_, used_for_configs[0].state_info_));
  ASSERT_EQ(configs[0].create_timestamp_, used_for_configs[0].create_timestamp_);
  ASSERT_EQ(configs[0].last_check_timestamp_, used_for_configs[0].last_check_timestamp_);
  ASSERT_EQ(configs[0].op_id_, used_for_configs[0].op_id_);
  ASSERT_EQ(configs[0].sub_op_id_, used_for_configs[0].sub_op_id_);
  ASSERT_EQ(configs[0].storage_id_, used_for_configs[0].storage_id_);
  ASSERT_EQ(configs[0].max_iops_, used_for_configs[0].max_iops_);
  ASSERT_EQ(configs[0].max_bandwidth_, used_for_configs[0].max_bandwidth_);
  char used_for_log[] = "LOG";
  // used_for_configs.reuse();
  ASSERT_EQ(OB_SUCCESS, config_mgr.get_device_configs(used_for_log, used_for_configs));
  ASSERT_EQ(2, used_for_configs.count());

  // test get_last_op_id and get_last_sub_op_id
  uint64_t last_op_id = UINT64_MAX;
  uint64_t last_sub_op_id = UINT64_MAX;
  ASSERT_EQ(OB_SUCCESS, config_mgr.get_last_op_id(last_op_id));
  ASSERT_EQ(OB_SUCCESS, config_mgr.get_last_sub_op_id(last_sub_op_id));
  ASSERT_EQ(3, last_op_id);
  ASSERT_EQ(1, last_sub_op_id);

  // test is_op_done
  bool is_done_3_1 = false;
  config_mgr.is_op_done(3, 1, is_done_3_1);
  ASSERT_TRUE(is_done_3_1);
  bool is_done_1_1 = false;
  config_mgr.is_op_done(1, 1, is_done_1_1);
  ASSERT_TRUE(is_done_1_1);

  // ASSERT_EQ(OB_SUCCESS, FileDirectoryUtils::delete_directory_rec(data_dir));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::FileDirectoryUtils::delete_directory_rec("./TestDeviceConfigDir");
  system("rm -f test_device_config.log");
  OB_LOGGER.set_file_name("test_device_config.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
