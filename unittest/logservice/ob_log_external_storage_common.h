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

#include <vector>
#include <thread>
#define private public
#include "logservice/shared_log/ob_shared_log_utils.h"
#include "logservice/ob_log_external_storage_handler.h"
#include "logservice/palf/log_block_header.h"
#include "share/object_storage/ob_device_config_mgr.h"
#undef private
#include "share/ob_device_manager.h"
#include "share/backup/ob_backup_struct.h"
#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace share;
using namespace common;
using namespace logservice;
using namespace palf;

class TestLogEXTUtils : public ::testing::Test
{
public:
  TestLogEXTUtils() {}
  ~TestLogEXTUtils() {}
  static void SetUpTestCase()
  {
    ObDeviceConfig device_config;
    char base_dir_c[1024];
    ASSERT_NE(nullptr, getcwd(base_dir_c, 1024));
    std::string base_dir = std::string(base_dir_c) + "/test_log_ext_utils";
    std::string mkdir = "mkdir " + base_dir;
    std::string rmdir = "rm -rf " + base_dir;
    system(rmdir.c_str());
    std::string root_path = "file://" + base_dir;
    const char *used_for = ObStorageUsedType::get_str(ObStorageUsedType::TYPE::USED_TYPE_ALL);
    const char *state = "ADDED";
    MEMCPY(device_config.path_, root_path.c_str(), root_path.size());
    MEMCPY(device_config.used_for_, used_for, strlen(used_for));
    device_config.sub_op_id_ = 1;
    STRCPY(device_config.state_, state);
    device_config.op_id_ = 1;
    device_config.last_check_timestamp_ = 0;
    device_config.storage_id_ = 1;
    ASSERT_EQ(OB_SUCCESS, GLOBAL_CONFIG_MGR.init(base_dir.c_str()));
    device_config.max_iops_ = 0;
    device_config.max_bandwidth_ = 0;
    ASSERT_EQ(OB_SUCCESS, GLOBAL_CONFIG_MGR.add_device_config(device_config));
    ASSERT_EQ(OB_SUCCESS, GLOBAL_EXT_HANDLER.init());
    ASSERT_EQ(OB_SUCCESS, GLOBAL_EXT_HANDLER.start(0));
  }

  static void TearDownTestCase()
  {}

  void SetUp() override
  {}

  void TearDown() override
  {}

  int upload_blocks(const uint64_t tenant_id,
                    const int64_t palf_id,
                    const palf::block_id_t start_block_id,
                    const std::vector<SCN> &scns)
  {
    int ret = OB_SUCCESS;
    if (!is_valid_tenant_id(tenant_id)
        || !is_valid_palf_id(palf_id)
        || !is_valid_block_id(start_block_id)
        || scns.empty()) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K(tenant_id), K(palf_id), K(start_block_id));
    } else {
      ret = upload_blocks_impl_(tenant_id, palf_id, start_block_id, scns);
    }
    return ret;
  }

  int upload_blocks(const uint64_t tenant_id,
                    const int64_t palf_id,
                    const palf::block_id_t start_block_id,
                    const int64_t block_count)
  {
    int ret = OB_SUCCESS;
    if (!is_valid_tenant_id(tenant_id)
        || !is_valid_palf_id(palf_id)
        || !is_valid_block_id(start_block_id)
        || 0 >= block_count) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K(tenant_id), K(palf_id), K(start_block_id), K(block_count));
    } else {
      std::vector<SCN> scns(block_count, SCN::min_scn());
      int64_t start_ts = ObTimeUtility::current_time();
      (void)srand(start_ts);
      int64_t hour = 60 * 60;
      for (auto &scn : scns) {
        scn.convert_from_ts(start_ts);
        int64_t interval = random()%hour + 1;
        start_ts += interval;
      }
      ret = upload_blocks_impl_(tenant_id, palf_id, start_block_id, scns);
    }
    return ret;
  }

  int upload_block(const uint64_t tenant_id,
                   const int64_t palf_id,
                   const palf::block_id_t block_id,
                   const SCN &scn)
  {
    int ret = OB_SUCCESS;
    if (!is_valid_tenant_id(tenant_id)
        || !is_valid_palf_id(palf_id)
        || !is_valid_block_id(block_id)
        || !scn.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K(tenant_id), K(palf_id), K(block_id), K(scn));
    } else {
      ret = upload_block_impl_(tenant_id, palf_id, block_id, scn);
    }
    return ret;
  }

  int create_tenant(const uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    char tenant_cstr[OB_MAX_URI_LENGTH] = { '\0' };
    ObBackupDest dest;
    uint64_t storage_id = OB_INVALID_ID;
    if (OB_FAIL(GLOBAL_UTILS.get_storage_dest_and_id_(dest, storage_id))) {
    } else if (OB_FAIL(GLOBAL_UTILS.construct_tenant_str_(dest, tenant_id, tenant_cstr))) {
    } else {
      std::string tenant_str = tenant_cstr + strlen(OB_FILE_PREFIX);
      std::string mkdir_str = "mkdir -p " + tenant_str;
      system(mkdir_str.c_str());
    }
    return ret;
  }
  int create_palf(const uint64_t tenant_id,
                  const int64_t palf_id)
  {
    int ret = OB_SUCCESS;
    char palf_cstr[OB_MAX_URI_LENGTH] = { '\0' };
    ObBackupDest dest;
    uint64_t storage_id = OB_INVALID_ID;
    if (OB_FAIL(GLOBAL_UTILS.get_storage_dest_and_id_(dest, storage_id))) {
    } else if (OB_FAIL(GLOBAL_UTILS.construct_palf_str_(dest, tenant_id, palf_id, palf_cstr))) {
    } else {
      std::string palf_str = palf_cstr + strlen(OB_FILE_PREFIX);
      std::string mkdir_str = "mkdir -p " + palf_str;
      system(mkdir_str.c_str());
    }
    return ret;

  }
  int delete_tenant(const uint64_t tenant_id)
  {
    for (int64_t palf_id = 1; palf_id < MAX_PALF_ID; palf_id++) {
      delete_palf(tenant_id, palf_id);
    }
    // 本地盘的delete_tenant操作要求tenant目录下不为空
    return GLOBAL_UTILS.delete_tenant(tenant_id);
  }
  int delete_palf(const uint64_t tenant_id,
                  const int64_t palf_id)
  {
    // 本地盘的delete_palf操作要求palf目录下不为空
    GLOBAL_UTILS.delete_blocks(tenant_id, palf_id, 0, MAX_BLOCK_ID+1);
    return GLOBAL_UTILS.delete_palf(tenant_id, palf_id);    
  }

private:
  int fsync(uint64_t tenant_id, int64_t palf_id)
  {
    int ret = OB_SUCCESS;
    char palf_cstr[OB_MAX_URI_LENGTH] = { '\0' };
    ObBackupDest dest;
    uint64_t storage_id = OB_INVALID_ID;
    if (OB_FAIL(GLOBAL_UTILS.get_storage_dest_and_id_(dest, storage_id))) {
    } else if (OB_FAIL(GLOBAL_UTILS.construct_palf_str_(dest, tenant_id, palf_id, palf_cstr))) {
    } else {
      std::string palf_str = palf_cstr + strlen(OB_FILE_PREFIX);
      int fd = ::open(palf_str.c_str(), LOG_READ_FLAG);
      ::fsync(fd);
    }
    return ret;
  }
  int upload_blocks_impl_(const uint64_t tenant_id,
                          const int64_t palf_id,
                          const palf::block_id_t start_block_id,
                          const std::vector<SCN> &scns)
  {
    int ret = OB_SUCCESS;
    const int64_t block_count = scns.size();
    for (int64_t index = 0; index < block_count && OB_SUCC(ret); index++) {
      ret = upload_block_impl_(tenant_id, palf_id, start_block_id + index, scns[index]);
    }
    return ret;
  }
  int upload_block_impl_(const uint64_t tenant_id,
                         const int64_t palf_id,
                         const palf::block_id_t block_id,
                         const SCN &scn)
  {
    int ret = OB_SUCCESS;
    LogBlockHeader log_block_header;
    char buf[MAX_INFO_BLOCK_SIZE] = {'\0'};
    LSN lsn(block_id*PALF_PHY_BLOCK_SIZE);
    int64_t pos = 0;
    log_block_header.update_lsn_and_scn(lsn, scn);
    if (OB_FAIL(log_block_header.serialize(buf, MAX_INFO_BLOCK_SIZE, pos))) {
      CLOG_LOG(ERROR, "serialize failed", K(pos));
    } else if (OB_FAIL(GLOBAL_EXT_HANDLER.upload(tenant_id, palf_id, block_id, buf, MAX_INFO_BLOCK_SIZE))) {
      CLOG_LOG(ERROR, "upload failed", K(tenant_id), K(palf_id), K(block_id));
    } else {
      CLOG_LOG(INFO, "upload_block success", K(tenant_id), K(palf_id), K(block_id), K(log_block_header));
      fsync(tenant_id, palf_id);
    }
    return ret;
  }
public:
  static const int64_t MAX_PALF_ID;
  static const int64_t MAX_BLOCK_ID;
  static const std::string BASE_DIR;
  static ObDeviceConfigMgr &GLOBAL_CONFIG_MGR;
  static ObLogExternalStorageHandler GLOBAL_EXT_HANDLER;
};

const int64_t TestLogEXTUtils::MAX_PALF_ID = 1010;
const int64_t TestLogEXTUtils::MAX_BLOCK_ID= 1100;
const std::string TestLogEXTUtils::BASE_DIR = "test_log_ext_utils";
ObDeviceConfigMgr& TestLogEXTUtils::GLOBAL_CONFIG_MGR = ObDeviceConfigMgr::get_instance();
ObLogExternalStorageHandler TestLogEXTUtils::GLOBAL_EXT_HANDLER;

TEST_F(TestLogEXTUtils, basic_interface)
{
  uint64_t tenant_id = 1002;
  int64_t palf_id = 1001;

  // 前置条件准备
  EXPECT_EQ(OB_SUCCESS, create_tenant(tenant_id));
  EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, palf_id));
  
  // 产生4个文件，最小的block_id为10
  std::vector<SCN> scns(4, SCN::min_scn());
  int64_t start_ts = ObTimeUtility::current_time();
  for (auto &scn : scns) {
    scn.convert_from_ts(start_ts++);
  }
  const block_id_t start_block_id = 10;
  const block_id_t end_block_id = 15;
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, palf_id, 10, scns));
  
  uint64_t invalid_tenant_id = OB_INVALID_TENANT_ID;  
  uint64_t valid_tenant_id = 1;
  int64_t invalid_palf_id = INVALID_PALF_ID;
  int64_t valid_palf_id = 1;
  block_id_t invalid_start_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_start_block_id = 1;
  block_id_t invalid_end_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_end_block_id = 1;
  block_id_t invalid_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_block_id = 1;
  char *invalid_uri = NULL;
  char valid_uri[OB_MAX_URI_LENGTH] = {'\0'};

  // case1: 验证ObSharedLogUtils::get_oldest_block
  {
    CLOG_LOG(INFO, "begin case1 invalid argument");
    block_id_t oldest_block_id = LOG_INVALID_BLOCK_ID;
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_oldest_block(
      invalid_tenant_id, invalid_palf_id, oldest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_oldest_block(
      valid_tenant_id, invalid_palf_id, oldest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_oldest_block(
      invalid_tenant_id, valid_palf_id, oldest_block_id));
    EXPECT_EQ(LOG_INVALID_BLOCK_ID, oldest_block_id);

    CLOG_LOG(INFO, "begin case1 success");
    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
      tenant_id, palf_id, oldest_block_id));
    EXPECT_EQ(start_block_id, oldest_block_id);

    // OB_ALLOCATE_MEMORY_FAILED AND OB_OBJECT_STORAGE_IO_ERROR
    
    CLOG_LOG(INFO, "begin case1 not exist");
    // OB_ENTRY_NOT_EXIST
    uint64_t not_exist_tenant_id = 500;
    uint64_t no_block_palf_id = 500;
    // 日志流不存在
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(
      not_exist_tenant_id, palf_id, oldest_block_id));
    // 日志流存在
    EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, no_block_palf_id));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(
      tenant_id, no_block_palf_id, oldest_block_id));
  }

  // case2: 验证get_newest_block
  {
    CLOG_LOG(INFO, "begin case2 invalid argument");
    block_id_t newest_block_id = LOG_INVALID_BLOCK_ID;
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_newest_block(
      invalid_tenant_id, invalid_palf_id, invalid_start_block_id, newest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_newest_block(
      valid_tenant_id, invalid_palf_id, invalid_start_block_id, newest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_newest_block(
      invalid_tenant_id, valid_palf_id, invalid_start_block_id, newest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_newest_block(
      invalid_tenant_id, invalid_palf_id, valid_start_block_id, newest_block_id));
    EXPECT_EQ(LOG_INVALID_BLOCK_ID, newest_block_id);

    CLOG_LOG(INFO, "begin case2 success");
    // OB_SUCCESS
    block_id_t tmp_start_block_id = 0;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, palf_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, start_block_id);
    // 起始点文件oss上最小文件相同
    tmp_start_block_id = start_block_id;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, palf_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, tmp_start_block_id);
    tmp_start_block_id = start_block_id+1;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, palf_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, tmp_start_block_id);
    tmp_start_block_id = start_block_id+2;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, palf_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, tmp_start_block_id);
    tmp_start_block_id = start_block_id+3;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, palf_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, tmp_start_block_id);

    // OB_ALLOCATE_MEMORY_FAILED AND OB_OBJECT_STORAGE_IO_ERROR
    
    CLOG_LOG(INFO, "begin case2 not exist");

    // OB_ENTRY_NOT_EXIST
    uint64_t not_exist_tenant_id = 500;
    uint64_t no_block_palf_id = 500;

    // 日志流不存在
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_newest_block(
      not_exist_tenant_id, palf_id, tmp_start_block_id, newest_block_id));

    // 空日志流
    EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, no_block_palf_id));
    tmp_start_block_id = LOG_INITIAL_BLOCK_ID;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, palf_id, tmp_start_block_id, newest_block_id));

    // 起点文件在oss上不存在 
    tmp_start_block_id = end_block_id;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_newest_block(
      tenant_id, palf_id, tmp_start_block_id, newest_block_id));
  }
  // case3: 验证locate_by_scn_corasely

  // case4: 验证ObSharedLogUtils::get_block_min_scn
  {
    CLOG_LOG(INFO, "begin case2 invalid argument");
    SCN block_min_scn = SCN::min_scn();
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_block_min_scn(
      invalid_tenant_id, invalid_palf_id, invalid_block_id, block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_block_min_scn(
      valid_tenant_id, invalid_palf_id, invalid_block_id, block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_block_min_scn(
      invalid_tenant_id, valid_palf_id, invalid_block_id, block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_block_min_scn(
      invalid_tenant_id, invalid_palf_id, valid_block_id, block_min_scn));

    CLOG_LOG(INFO, "begin case2 success");
    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_block_min_scn(
      tenant_id, palf_id, start_block_id, block_min_scn));
    EXPECT_EQ(block_min_scn, scns[0]);

    // OB_ALLOCATE_MEMORY_FAILED AND OB_OBJECT_STORAGE_IO_ERROR
    
    CLOG_LOG(INFO, "begin case2 entry not exist");
    // OB_ENTRY_NOT_EXIST
    uint64_t not_exist_tenant_id = 500;
    block_id_t not_exist_block_id = start_block_id + 100000;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_block_min_scn(
      tenant_id, palf_id, not_exist_block_id, block_min_scn));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_block_min_scn(
      not_exist_tenant_id, palf_id, start_block_id, block_min_scn));
  }

  // case5: 验证ObSharedLogUtils::delete_blocks
  {
    CLOG_LOG(INFO, "begin case5");
    // OB_INVLAID_ARGUMENT
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      invalid_tenant_id, invalid_palf_id, invalid_start_block_id, invalid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      valid_tenant_id, invalid_palf_id, invalid_start_block_id, invalid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      invalid_tenant_id, valid_palf_id, invalid_start_block_id, invalid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      invalid_tenant_id, invalid_palf_id, valid_start_block_id, invalid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      invalid_tenant_id, invalid_palf_id, invalid_start_block_id, valid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      valid_tenant_id, valid_palf_id, start_block_id, start_block_id));

    block_id_t oldest_block_id;
    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      tenant_id, palf_id, start_block_id, start_block_id+2));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
      tenant_id, palf_id, oldest_block_id));
    EXPECT_EQ(oldest_block_id, start_block_id+2);
    // 生成MAX_BLOCK_ID个文件
    {
      uint64_t tmp_tenant_id = 1004;
      int64_t tmp_palf_id = 1003;
      block_id_t tmp_start_block = 3;
      block_id_t tmp_end_block = tmp_start_block + MAX_BLOCK_ID;
      EXPECT_EQ(OB_SUCCESS, create_tenant(tmp_tenant_id));
      EXPECT_EQ(OB_SUCCESS, create_palf(tmp_tenant_id, tmp_palf_id));
      EXPECT_EQ(OB_SUCCESS, upload_blocks(tmp_tenant_id, tmp_palf_id, tmp_start_block, MAX_BLOCK_ID - tmp_start_block));
      block_id_t tmp_oldest_block = 0;
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
        tmp_tenant_id, tmp_palf_id, tmp_oldest_block));
      EXPECT_EQ(tmp_start_block, tmp_oldest_block);
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(tmp_tenant_id, tmp_palf_id,
                                                                     tmp_start_block, tmp_start_block+1003));
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
        tmp_tenant_id, tmp_palf_id, tmp_oldest_block));
      EXPECT_EQ(tmp_start_block+1003, tmp_oldest_block);
      EXPECT_EQ(OB_SUCCESS, delete_palf(tmp_tenant_id, tmp_palf_id));
      bool tmp_palf_exist = false; 
      bool tmp_tenant_exist = false; 
      CLOG_LOG(INFO, "runlin trace delete_palf", K(tmp_tenant_id), K(tmp_palf_id));
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_palf_exist(
        tmp_tenant_id, tmp_palf_id, tmp_palf_exist));
      EXPECT_EQ(false, tmp_palf_exist);
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
        tmp_tenant_id, tmp_tenant_exist));
      // 尽管租户目录存在，但在oss上没有目录的概念，因此租户不存在
      EXPECT_EQ(false, tmp_tenant_exist);
      sleep(10);
      EXPECT_EQ(OB_SUCCESS, delete_tenant(tmp_tenant_id));
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
        tmp_tenant_id, tmp_tenant_exist));
      EXPECT_EQ(false, tmp_tenant_exist);
    }
    // 保证OSS上依旧存在相同的文件文件数目
    std::vector<SCN> tmp_scns{scns[0], scns[1]};
    EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, palf_id, start_block_id, tmp_scns));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
      tenant_id, palf_id, oldest_block_id));
    EXPECT_EQ(oldest_block_id, start_block_id);

    // OB_ALLOCATE_MEMORY_FAILED AND OB_OBJECT_STORAGE_IO_ERROR
    
    // 删除不存在文件
    uint64_t not_exist_tenant_id = 500;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      tenant_id, palf_id, start_block_id+10000, start_block_id+30000));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      not_exist_tenant_id, palf_id, start_block_id, start_block_id+10));
  }

  // case6: 验证ObSharedLogUtils::construct_external_storage_access_info
  {
    CLOG_LOG(INFO, "begin case6");
    ObBackupDest dest;
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, invalid_palf_id, invalid_block_id, invalid_uri, dest));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      valid_tenant_id, invalid_palf_id, invalid_block_id, invalid_uri, dest));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, valid_palf_id, invalid_block_id, invalid_uri, dest));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, invalid_palf_id, valid_block_id, invalid_uri, dest));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, invalid_palf_id, invalid_block_id, valid_uri, dest));

    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::construct_external_storage_access_info(
      tenant_id, palf_id, valid_block_id, valid_uri, dest));
    EXPECT_EQ(0, STRNCMP(valid_uri, OB_FILE_PREFIX, strlen(OB_FILE_PREFIX)));
  }
  
  // case7: 验证ObSharedLogUtils::delete_tenant和delete_palf
  // case7.1 验证ObSharedLogUtils::check_palf_exist
  // case7.2 验证ObSharedLogUtils::check_tenant_exist
  {
    CLOG_LOG(INFO, "begin case7");
    bool palf_exist = false;
    bool tenant_exist = false;
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_tenant(
      invalid_tenant_id));

    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_palf(
      valid_tenant_id, invalid_palf_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_palf(
      invalid_tenant_id, valid_palf_id));

    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::check_tenant_exist(
      invalid_tenant_id, tenant_exist));

    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::check_palf_exist(
      invalid_tenant_id, invalid_palf_id, palf_exist));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::check_palf_exist(
      valid_tenant_id, invalid_palf_id, palf_exist));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::check_palf_exist(
      invalid_tenant_id, valid_palf_id, palf_exist));

    GLOBAL_UTILS.delete_blocks(tenant_id, palf_id, start_block_id, end_block_id);
    EXPECT_EQ(OB_SUCCESS, delete_palf(tenant_id, palf_id));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_palf_exist(
      tenant_id, palf_id, palf_exist));
    EXPECT_EQ(false, palf_exist);
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
      tenant_id, tenant_exist));
    EXPECT_EQ(false, tenant_exist);
    // 创建空日志流，本地盘上依旧存在空目录，但oss没有目录的概念，因此check_palf_exist返回false
    EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, palf_id));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_palf_exist(
      tenant_id, palf_id, palf_exist));
    EXPECT_EQ(false, palf_exist);


    EXPECT_EQ(OB_SUCCESS, delete_tenant(tenant_id));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_palf_exist(
      tenant_id, palf_id, palf_exist));
    EXPECT_EQ(false, palf_exist);
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
      tenant_id, tenant_exist));
    EXPECT_EQ(false, tenant_exist);

    block_id_t oldest_block_id = LOG_INVALID_BLOCK_ID;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(
      tenant_id, palf_id, oldest_block_id));
    // 验证多日志流场景
    {
      uint64_t tmp_tenant_id = 1004;
      std::vector<int64_t> palf_ids = {1, 1001, 1002, 1003, 1004};
      EXPECT_EQ(OB_SUCCESS, create_tenant(tmp_tenant_id));
      for (auto tmp_palf_id : palf_ids) {
        EXPECT_EQ(OB_SUCCESS, create_palf(tmp_tenant_id, tmp_palf_id));
      }
      block_id_t tmp_start_block_id = 0;
      block_id_t tmp_end_block_id = 0;
      // 每个日志流准备1001个文件
      for (auto tmp_palf_id: palf_ids) {
        EXPECT_EQ(OB_SUCCESS, upload_blocks(tmp_tenant_id, tmp_palf_id, tmp_start_block_id, 1001));
      }
      block_id_t tmp_oldoest_block_id = 0;
      bool tmp_palf_exist = false;
      for (auto tmp_palf_id: palf_ids) {
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
          tmp_tenant_id, tmp_palf_id, tmp_oldoest_block_id));
        EXPECT_EQ(tmp_oldoest_block_id, tmp_start_block_id);
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_palf_exist(
          tmp_tenant_id, tmp_palf_id, tmp_palf_exist));
        EXPECT_EQ(tmp_palf_exist, true);
      }
      for (auto tmp_palf_id : palf_ids) {
        EXPECT_EQ(OB_SUCCESS, delete_palf(tmp_tenant_id, tmp_palf_id));
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_palf_exist(
          tmp_tenant_id, tmp_palf_id, tmp_palf_exist));
        EXPECT_EQ(tmp_palf_exist, false);
      }
      bool tmp_tenant_exist = false;
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
        tmp_tenant_id, tmp_tenant_exist));
      EXPECT_EQ(false, tmp_tenant_exist);
      EXPECT_EQ(OB_SUCCESS, delete_tenant(tmp_tenant_id));
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
        tmp_tenant_id, tmp_tenant_exist));
      EXPECT_EQ(false, tmp_tenant_exist);
    }
    // 验证多租户场景
    {
      std::vector<uint64_t> tenant_ids = {1, 1001, 1002, 1003, 1004};
      std::vector<int64_t> palf_ids = {1, 1001, 1002, 1003, 1004};
      block_id_t tmp_start_block_id = 0;
      block_id_t tmp_end_block_id = 0;
      block_id_t tmp_oldest_block_id = 0;
      bool tmp_tenant_exist = false;
      bool tmp_palf_exist = false;
      for (auto tmp_tenant_id : tenant_ids) {
        EXPECT_EQ(OB_SUCCESS, create_tenant(tmp_tenant_id));
        for (auto tmp_palf_id : palf_ids) {
          EXPECT_EQ(OB_SUCCESS, create_palf(tmp_tenant_id, tmp_palf_id));
          EXPECT_EQ(OB_SUCCESS, upload_blocks(tmp_tenant_id, tmp_palf_id, tmp_start_block_id, 1001));
        }
      }

      for (auto tmp_tenant_id : tenant_ids) {
        for (auto tmp_palf_id : palf_ids) {
          EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
            tmp_tenant_id, tmp_palf_id, tmp_oldest_block_id));
          EXPECT_EQ(tmp_oldest_block_id, tmp_start_block_id);
        }
      }

      for (auto tmp_tenant_id : tenant_ids) {
        for (auto tmp_palf_id : palf_ids) {
          EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
            tmp_tenant_id, tmp_palf_id, tmp_oldest_block_id));
          EXPECT_EQ(tmp_oldest_block_id, tmp_start_block_id);
        }
      }

      for (auto tmp_tenant_id : tenant_ids) {
        for (auto tmp_palf_id : palf_ids) {
          EXPECT_EQ(OB_SUCCESS, delete_palf(tmp_tenant_id, tmp_palf_id));
          EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_palf_exist(
            tmp_tenant_id, tmp_palf_id, tmp_palf_exist));
          EXPECT_EQ(false, tmp_palf_exist);
        }
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
            tmp_tenant_id, tmp_tenant_exist));
        EXPECT_EQ(false, tmp_tenant_exist);

        EXPECT_EQ(OB_SUCCESS, delete_tenant(tmp_tenant_id));
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
          tmp_tenant_id, tmp_tenant_exist));
        EXPECT_EQ(false, tmp_tenant_exist);
      } // end 
    }
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -rf test_shared_log_utils.log*");
  OB_LOGGER.set_file_name("test_shared_log_utils.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_shared_log_utils");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
