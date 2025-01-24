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

#define private public
#define protected public
#include "unittest/logservice/test_shared_log_common.h"
#ifdef OB_BUILD_SHARED_STORAGE
#endif
#undef protected
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace share;
using namespace common;
using namespace logservice;
using namespace palf;

const std::string TestLogExternalStorageCommom::BASE_DIR = "test_log_ext_utils";
const int64_t default_block_size = 4 * 1024;
class TestLogEXTUtils: public TestLogExternalStorageCommom {
public:
private:
virtual int upload_block_impl_(const uint64_t tenant_id,
                               const int64_t palf_id,
                               const palf::block_id_t block_id,
                               const SCN &scn)
{
  int ret = OB_SUCCESS;
  LogBlockHeader log_block_header;
  char *write_buf = reinterpret_cast<char*>(mtl_malloc(default_block_size, "test"));
  if (NULL == write_buf){
    return OB_ALLOCATE_MEMORY_FAILED;
  }
  memset(write_buf, 'c', default_block_size);
  LSN lsn(block_id*default_block_size);
  int64_t pos = 0;
  log_block_header.update_lsn_and_scn(lsn, scn);
  log_block_header.calc_checksum();
  if (OB_FAIL(log_block_header.serialize(write_buf, MAX_INFO_BLOCK_SIZE, pos))) {
    CLOG_LOG(ERROR, "serialize failed", K(pos));
  } else if (OB_FAIL(GLOBAL_EXT_HANDLER.upload(tenant_id, palf_id, block_id, write_buf, default_block_size))) {
    CLOG_LOG(ERROR, "upload failed", K(tenant_id), K(palf_id), K(block_id));
  } else {
    CLOG_LOG(INFO, "upload_block success", K(tenant_id), K(palf_id), K(block_id), K(log_block_header));
  }
  if (NULL != write_buf) {
    mtl_free(write_buf);
  }
  return ret;
}
};

TEST_F(TestLogEXTUtils, basic_interface)
{
  uint64_t tenant_id = 1002;
  ObLSID ls_id(1001);

  // 前置条件准备
  EXPECT_EQ(OB_SUCCESS, create_tenant(tenant_id));
  EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, ls_id.id_));

  {
    const block_id_t start_block_id = 0;
    const block_id_t end_block_id = 0;
    EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), start_block_id, 1));
    block_id_t tmp_start_block_id = 0;
    block_id_t newest_block_id = LOG_INVALID_BLOCK_ID;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, end_block_id);
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(tenant_id, ls_id, start_block_id, end_block_id+1));
  }
  // 产生10个文件，最小的block_id为10
  std::vector<SCN> scns(10, SCN::min_scn());
  int64_t start_ts = ObTimeUtility::current_time();
  for (auto &scn : scns) {
    scn.convert_from_ts(start_ts++);
  }
  const block_id_t start_block_id = 10;
  const block_id_t end_block_id = 19;
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id_, start_block_id, scns));

  uint64_t invalid_tenant_id = OB_INVALID_TENANT_ID;
  uint64_t valid_tenant_id = 1;
  ObLSID invalid_ls_id;
  ObLSID valid_ls_id(1);
  block_id_t invalid_start_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_start_block_id = 1;
  block_id_t invalid_end_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_end_block_id = 1;
  block_id_t invalid_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_block_id = 1;
  char *invalid_uri = NULL;
  const int64_t invalid_uri_len = 0;
  char valid_uri[OB_MAX_URI_LENGTH] = {'\0'};
  const int64_t valid_uri_len = OB_MAX_URI_LENGTH;

  // case1: 验证ObSharedLogUtils::get_oldest_block
  {
    CLOG_LOG(INFO, "begin case1 invalid argument");
    block_id_t oldest_block_id = LOG_INVALID_BLOCK_ID;
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_oldest_block(
      invalid_tenant_id, ObLSID(invalid_ls_id), oldest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_oldest_block(
      valid_tenant_id, ObLSID(invalid_ls_id), oldest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_oldest_block(
      invalid_tenant_id, ObLSID(valid_ls_id), oldest_block_id));
    EXPECT_EQ(LOG_INVALID_BLOCK_ID, oldest_block_id);

    CLOG_LOG(INFO, "begin case1 success");
    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
      tenant_id, ObLSID(ls_id), oldest_block_id));
    EXPECT_EQ(start_block_id, oldest_block_id);

    // OB_ALLOCATE_MEMORY_FAILED AND OB_OBJECT_STORAGE_IO_ERROR

    CLOG_LOG(INFO, "begin case1 not exist");
    // OB_ENTRY_NOT_EXIST
    uint64_t not_exist_tenant_id = 500;
    ObLSID no_block_ls_id(500);
    // 日志流不存在
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(
      not_exist_tenant_id, ls_id, oldest_block_id));
    // 日志流存在
    EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, no_block_ls_id.id_));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(
      tenant_id, no_block_ls_id, oldest_block_id));
  }

  // case2: 验证get_newest_block
  {
    CLOG_LOG(INFO, "begin case2 invalid argument");
    block_id_t newest_block_id = LOG_INVALID_BLOCK_ID;
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_newest_block(
      invalid_tenant_id, invalid_ls_id, invalid_start_block_id, newest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_newest_block(
      valid_tenant_id, invalid_ls_id, invalid_start_block_id, newest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_newest_block(
      invalid_tenant_id, valid_ls_id, invalid_start_block_id, newest_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_newest_block(
      invalid_tenant_id, invalid_ls_id, valid_start_block_id, newest_block_id));
    EXPECT_EQ(LOG_INVALID_BLOCK_ID, newest_block_id);

    CLOG_LOG(INFO, "begin case2 success");
    // OB_SUCCESS
    block_id_t tmp_start_block_id = 0;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, end_block_id);
    // 起始点文件oss上最小文件相同
    tmp_start_block_id = start_block_id;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, end_block_id);
    tmp_start_block_id = start_block_id+1;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, end_block_id);
    tmp_start_block_id = start_block_id+2;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, end_block_id);
    tmp_start_block_id = start_block_id+3;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
    EXPECT_EQ(newest_block_id, end_block_id);


    // 文件有空洞
    // 10 11 12 13...19  24 25 26
    {
      const block_id_t start_block_id_hole = 24;
      const block_id_t end_block_id_hole = 26;
      EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), start_block_id_hole, 3));
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
        tenant_id, ls_id, tmp_start_block_id, newest_block_id));
      EXPECT_EQ(newest_block_id, end_block_id_hole);
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
        tenant_id, ls_id, start_block_id_hole, end_block_id_hole+1));
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_newest_block(
        tenant_id, ls_id, tmp_start_block_id, newest_block_id));
      EXPECT_EQ(newest_block_id, end_block_id);
    }

    // OB_ALLOCATE_MEMORY_FAILED AND OB_OBJECT_STORAGE_IO_ERROR

    CLOG_LOG(INFO, "begin case2 not exist");

    // OB_ENTRY_NOT_EXIST
    uint64_t not_exist_tenant_id = 500;
    ObLSID no_block_ls_id(500);

    // 日志流不存在
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_newest_block(
      not_exist_tenant_id, ls_id, tmp_start_block_id, newest_block_id));

    // 空日志流
    EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, no_block_ls_id.id()));
    tmp_start_block_id = LOG_INITIAL_BLOCK_ID;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_newest_block(
      tenant_id, no_block_ls_id, tmp_start_block_id, newest_block_id));

    // 起点文件在oss上不存在
    tmp_start_block_id = end_block_id+1;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_newest_block(
      tenant_id, ls_id, tmp_start_block_id, newest_block_id));
  }
  // case3: 验证locate_by_scn_corasely

  // case4: 验证ObSharedLogUtils::get_block_min_scn
  {
    CLOG_LOG(INFO, "begin case4 invalid argument");
    SCN block_min_scn = SCN::min_scn();
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_block_min_scn(
      invalid_tenant_id, invalid_ls_id, invalid_block_id, block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_block_min_scn(
      valid_tenant_id, invalid_ls_id, invalid_block_id, block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_block_min_scn(
      invalid_tenant_id, valid_ls_id, invalid_block_id, block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::get_block_min_scn(
      invalid_tenant_id, invalid_ls_id, valid_block_id, block_min_scn));

    CLOG_LOG(INFO, "begin case4 success");
    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_block_min_scn(
      tenant_id, ls_id, start_block_id, block_min_scn));
    EXPECT_EQ(block_min_scn, scns[0]);

    // OB_ALLOCATE_MEMORY_FAILED AND OB_OBJECT_STORAGE_IO_ERROR

    CLOG_LOG(INFO, "begin case4 entry not exist");
    // OB_ENTRY_NOT_EXIST
    uint64_t not_exist_tenant_id = 500;
    block_id_t not_exist_block_id = start_block_id + 100000;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_block_min_scn(
      tenant_id, ls_id, not_exist_block_id, block_min_scn));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_block_min_scn(
      not_exist_tenant_id, ls_id, start_block_id, block_min_scn));
  }

  // case5: 验证ObSharedLogUtils::delete_blocks
  {
    CLOG_LOG(INFO, "begin case5");
    // OB_INVLAID_ARGUMENT
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      invalid_tenant_id, invalid_ls_id, invalid_start_block_id, invalid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      valid_tenant_id, invalid_ls_id, invalid_start_block_id, invalid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      invalid_tenant_id, valid_ls_id, invalid_start_block_id, invalid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      invalid_tenant_id, invalid_ls_id, valid_start_block_id, invalid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      invalid_tenant_id, invalid_ls_id, invalid_start_block_id, valid_end_block_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      valid_tenant_id, valid_ls_id, start_block_id, start_block_id-1));
    // delete_blocks仅支持左闭右开区间
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_blocks(
      valid_tenant_id, valid_ls_id, start_block_id, start_block_id));

    block_id_t oldest_block_id;
    // OB_SUCCESS
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      tenant_id, ls_id, start_block_id, start_block_id+2));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
      tenant_id, ls_id, oldest_block_id));
    EXPECT_EQ(oldest_block_id, start_block_id+2);
    // 生成MAX_BLOCK_ID个文件
    {
      uint64_t tmp_tenant_id = 1004;
      ObLSID tmp_ls_id(1003);
      block_id_t tmp_start_block = 3;
      block_id_t tmp_end_block = tmp_start_block + MAX_BLOCK_ID;
      EXPECT_EQ(OB_SUCCESS, create_tenant(tmp_tenant_id));
      EXPECT_EQ(OB_SUCCESS, create_palf(tmp_tenant_id, tmp_ls_id.id()));
      EXPECT_EQ(OB_SUCCESS, upload_blocks(tmp_tenant_id, tmp_ls_id.id(), tmp_start_block, MAX_BLOCK_ID - tmp_start_block));
      block_id_t tmp_oldest_block = 0;
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
        tmp_tenant_id, tmp_ls_id, tmp_oldest_block));
      EXPECT_EQ(tmp_start_block, tmp_oldest_block);
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(tmp_tenant_id, tmp_ls_id,
                                                            tmp_start_block, tmp_start_block+1003));
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
        tmp_tenant_id, tmp_ls_id, tmp_oldest_block));
      EXPECT_EQ(tmp_start_block+1003, tmp_oldest_block);
      EXPECT_EQ(OB_SUCCESS, delete_palf(tmp_tenant_id, tmp_ls_id.id()));
      bool tmp_palf_exist = false;
      bool tmp_tenant_exist = false;
      CLOG_LOG(INFO, "runlin trace delete_palf", K(tmp_tenant_id), K(tmp_ls_id));
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
        tmp_tenant_id, tmp_ls_id, tmp_palf_exist));
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
    EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), start_block_id, tmp_scns));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
      tenant_id, ls_id, oldest_block_id));
    EXPECT_EQ(oldest_block_id, start_block_id);

    // OB_ALLOCATE_MEMORY_FAILED AND OB_OBJECT_STORAGE_IO_ERROR

    // 删除不存在文件
    uint64_t not_exist_tenant_id = 500;
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      tenant_id, ls_id, start_block_id+10000, start_block_id+30000));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      not_exist_tenant_id, ls_id, start_block_id, start_block_id+10));
  }

  // case6: 验证ObSharedLogUtils::construct_external_storage_access_info
  {
    CLOG_LOG(INFO, "begin case6");
    ObBackupDest dest;
    uint64_t storage_id = OB_INVALID_ID;
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, invalid_ls_id, invalid_block_id, invalid_uri, invalid_uri_len, dest, storage_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      valid_tenant_id, invalid_ls_id, invalid_block_id, invalid_uri, invalid_uri_len, dest, storage_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, valid_ls_id, invalid_block_id, invalid_uri, invalid_uri_len, dest, storage_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, invalid_ls_id, valid_block_id, invalid_uri, invalid_uri_len, dest, storage_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, invalid_ls_id, invalid_block_id, valid_uri, invalid_uri_len, dest, storage_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::construct_external_storage_access_info(
      invalid_tenant_id, invalid_ls_id, invalid_block_id, invalid_uri, valid_uri_len, dest, storage_id));

    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::construct_external_storage_access_info(
      tenant_id, ls_id, valid_block_id, valid_uri, valid_uri_len, dest, storage_id));
    EXPECT_EQ(0, STRNCMP(valid_uri, OB_FILE_PREFIX, strlen(OB_FILE_PREFIX)));
  }

  // case7: 验证ObSharedLogUtils::delete_tenant和delete_palf
  // case7.1 验证ObSharedLogUtils::check_ls_exist
  // case7.2 验证ObSharedLogUtils::check_tenant_exist
  {
    CLOG_LOG(INFO, "begin case7");
    bool palf_exist = false;
    bool tenant_exist = false;
    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_tenant(
      invalid_tenant_id));

    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_ls(
      valid_tenant_id, invalid_ls_id));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::delete_ls(
      invalid_tenant_id, valid_ls_id));

    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::check_tenant_exist(
      invalid_tenant_id, tenant_exist));

    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::check_ls_exist(
      invalid_tenant_id, invalid_ls_id, palf_exist));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::check_ls_exist(
      valid_tenant_id, invalid_ls_id, palf_exist));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::check_ls_exist(
      invalid_tenant_id, valid_ls_id, palf_exist));

    SHARED_LOG_GLOBAL_UTILS.delete_blocks(tenant_id, ls_id, start_block_id, end_block_id+1);
    EXPECT_EQ(OB_SUCCESS, delete_palf(tenant_id, ls_id.id()));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
      tenant_id, ls_id, palf_exist));
    EXPECT_EQ(false, palf_exist);
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
      tenant_id, tenant_exist));
    EXPECT_EQ(false, tenant_exist);
    // 创建空日志流，本地盘上依旧存在空目录，但oss没有目录的概念，因此check_ls_exist返回false
    EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, ls_id.id()));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
      tenant_id, ls_id, palf_exist));
    EXPECT_EQ(false, palf_exist);


    EXPECT_EQ(OB_SUCCESS, delete_tenant(tenant_id));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
      tenant_id, ls_id, palf_exist));
    EXPECT_EQ(false, palf_exist);
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_tenant_exist(
      tenant_id, tenant_exist));
    EXPECT_EQ(false, tenant_exist);

    block_id_t oldest_block_id = LOG_INVALID_BLOCK_ID;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::get_oldest_block(
      tenant_id, ls_id, oldest_block_id));
    // 验证多日志流场景
    {
      uint64_t tmp_tenant_id = 1004;
      std::vector<int64_t> ls_ids = {1, 1001, 1002, 1003, 1004};
      EXPECT_EQ(OB_SUCCESS, create_tenant(tmp_tenant_id));
      for (auto tmp_ls_id : ls_ids) {
        EXPECT_EQ(OB_SUCCESS, create_palf(tmp_tenant_id, tmp_ls_id));
      }
      block_id_t tmp_start_block_id = 0;
      block_id_t tmp_end_block_id = 0;
      // 每个日志流准备13个文件
      for (auto tmp_ls_id: ls_ids) {
        EXPECT_EQ(OB_SUCCESS, upload_blocks(tmp_tenant_id, tmp_ls_id, tmp_start_block_id, 13));
      }
      block_id_t tmp_oldoest_block_id = 0;
      bool tmp_palf_exist = false;
      for (auto tmp_ls_id: ls_ids) {
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
          tmp_tenant_id, ObLSID(tmp_ls_id), tmp_oldoest_block_id));
        EXPECT_EQ(tmp_oldoest_block_id, tmp_start_block_id);
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
          tmp_tenant_id, ObLSID(tmp_ls_id), tmp_palf_exist));
        EXPECT_EQ(tmp_palf_exist, true);
      }
      for (auto tmp_ls_id : ls_ids) {
        EXPECT_EQ(OB_SUCCESS, delete_palf(tmp_tenant_id, tmp_ls_id));
        EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
          tmp_tenant_id, ObLSID(tmp_ls_id), tmp_palf_exist));
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
      std::vector<int64_t> ls_ids = {1, 1001, 1002, 1003, 1004};
      block_id_t tmp_start_block_id = 0;
      block_id_t tmp_end_block_id = 0;
      block_id_t tmp_oldest_block_id = 0;
      bool tmp_tenant_exist = false;
      bool tmp_palf_exist = false;
      for (auto tmp_tenant_id : tenant_ids) {
        EXPECT_EQ(OB_SUCCESS, create_tenant(tmp_tenant_id));
        for (auto tmp_ls_id : ls_ids) {
          EXPECT_EQ(OB_SUCCESS, create_palf(tmp_tenant_id, tmp_ls_id));
          EXPECT_EQ(OB_SUCCESS, upload_blocks(tmp_tenant_id, tmp_ls_id, tmp_start_block_id, 10));
        }
      }

      for (auto tmp_tenant_id : tenant_ids) {
        for (auto tmp_ls_id : ls_ids) {
          EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
            tmp_tenant_id, ObLSID(tmp_ls_id), tmp_oldest_block_id));
          EXPECT_EQ(tmp_oldest_block_id, tmp_start_block_id);
        }
      }

      for (auto tmp_tenant_id : tenant_ids) {
        for (auto tmp_ls_id : ls_ids) {
          EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::get_oldest_block(
            tmp_tenant_id, ObLSID(tmp_ls_id), tmp_oldest_block_id));
          EXPECT_EQ(tmp_oldest_block_id, tmp_start_block_id);
        }
      }

      for (auto tmp_tenant_id : tenant_ids) {
        for (auto tmp_ls_id : ls_ids) {
          EXPECT_EQ(OB_SUCCESS, delete_palf(tmp_tenant_id, tmp_ls_id));
          EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::check_ls_exist(
            tmp_tenant_id, ObLSID(tmp_ls_id), tmp_palf_exist));
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

TEST_F(TestLogEXTUtils, test_locate_by_scn_coarsely)
{
  uint64_t tenant_id = 1002;
  ObLSID ls_id(1001);
  CLOG_LOG(INFO, "begin test_locate_by_scn_coarsely");
  // 前置条件准备
  EXPECT_EQ(OB_SUCCESS, create_tenant(tenant_id));
  EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, ls_id.id_));

  // 产生5个文件，最小的block_id为10
  std::vector<SCN> scns(5, SCN::min_scn());
  int64_t start_ts = ObTimeUtility::current_time();
  for (SCN &scn : scns) {
    scn.convert_from_ts(start_ts++);
  }
  const block_id_t start_block_id = 10;
  const block_id_t end_block_id = 14;
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), start_block_id, scns));

  uint64_t invalid_tenant_id = OB_INVALID_TENANT_ID;
  uint64_t valid_tenant_id = 1;
  ObLSID invalid_ls_id;
  ObLSID valid_ls_id(1001);
  block_id_t invalid_start_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_start_block_id = 1;
  block_id_t invalid_end_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_end_block_id = 1;
  block_id_t invalid_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t valid_block_id = 1;
  SCN invalid_scn = SCN::invalid_scn();

  // case3: 验证locate_by_scn_corasely
  {
    CLOG_LOG(INFO, "begin case3 invalid argument");
    block_id_t out_block_id = LOG_INVALID_BLOCK_ID;
    SCN out_block_min_scn;
    SCN target_scn = scns[0];

    // invalid argument
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
      invalid_tenant_id, invalid_ls_id, invalid_start_block_id, invalid_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
      valid_tenant_id, invalid_ls_id, invalid_start_block_id, invalid_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
      valid_tenant_id, valid_ls_id, invalid_start_block_id, invalid_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
      valid_tenant_id, valid_ls_id, start_block_id, invalid_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
      valid_tenant_id, valid_ls_id, end_block_id, start_block_id,
      target_scn, out_block_id, out_block_min_scn));
    // locate_by_scn_corasely仅支持左闭右开区间
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
      valid_tenant_id, valid_ls_id, start_block_id, start_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(LOG_INVALID_BLOCK_ID, out_block_id);
    EXPECT_EQ(invalid_scn, out_block_min_scn);

    CLOG_LOG(INFO, "begin case3 abnormal situations");
    block_id_t temp_start_block_id, temp_end_block_id;
    // case 3.1: 本地日志区间和OSS日志区间不存在交集，return OB_ENTRY_NOT_EXIST
    // temp_end_block_id < start_block_id
    temp_start_block_id = 1;
    temp_end_block_id = 9;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, valid_ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
    // temp_start_block_id > end_block_id
    temp_start_block_id = 15;
    temp_end_block_id = 20;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, valid_ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));

    // case 3.2: the min scn of each block is greater than target scn, return OB_ERR_OUT_OF_LOWER_BOUND
    temp_start_block_id = 11;
    temp_end_block_id = 16;
    target_scn = SCN::min_scn();
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, valid_ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));

    // case 3.3: 日志流不存在和空日志流, return OB_ENTRY_NOT_EXIST
    uint64_t not_exist_tenant_id = 500;
    ObLSID no_block_ls_id(500);

    // EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::locate_by_scn_coarsely(
    //   not_exist_tenant_id, valid_ls_id, temp_start_block_id, temp_end_block_id,
    //   target_scn, out_block_id, out_block_min_scn));
    CLOG_LOG(INFO, "begin case3 empty logstream");
    EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, no_block_ls_id.id_));
    temp_start_block_id = LOG_INITIAL_BLOCK_ID;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, no_block_ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));

    CLOG_LOG(INFO, "begin case3 success");
    // case 3.4: success situations
    temp_start_block_id = 8;
    temp_end_block_id = 16;
    for (int i = 0; i < 5; i ++) {
      target_scn = scns[i];
      EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
        tenant_id, valid_ls_id, temp_start_block_id, temp_end_block_id,
        target_scn, out_block_id, out_block_min_scn));
      EXPECT_EQ(start_block_id + i, out_block_id);
      EXPECT_EQ(scns[i], out_block_min_scn);
    }

    // case 3.5: target scn is greater than the min scn of each block
    temp_start_block_id = 8;
    temp_end_block_id = 16;
    target_scn = SCN::max_scn();
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, valid_ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(14, out_block_id);
    EXPECT_EQ(scns[4], out_block_min_scn);
  }
}

TEST_F(TestLogEXTUtils, test_locate_by_scn_coarsely_with_holes)
{
  CLOG_LOG(INFO, "begin test_locate_by_scn_coarsely_with_holes");
  uint64_t tenant_id = 1002;
  ObLSID ls_id(1001);

  // 前置条件准备
  EXPECT_EQ(OB_SUCCESS, create_tenant(tenant_id));
  EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, ls_id.id_));

  // 产生10个连续的文件，最小的block_id为10，最大的block_id为19
  int block_count = 10;
  std::vector<SCN> scns(block_count, SCN::min_scn());
  int64_t start_ts = ObTimeUtility::current_time();
  (void)srand(start_ts);
  int64_t hour = 60 * 60;
  std::vector<SCN> target_scns(block_count, SCN::min_scn());
  for (int i = 0; i < block_count; i++) {
    scns[i].convert_from_ts(start_ts);
    target_scns[i].convert_from_ts(start_ts + 1);
    int64_t interval = random()%hour + 1;
    start_ts += interval;
  }
  const block_id_t start_block_id = 10;
  const block_id_t end_block_id = 19;
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), 10, scns));

  block_id_t temp_start_block_id = 7;
  block_id_t temp_end_block_id = 18;
  SCN target_scn = target_scns[5];
  SCN out_block_min_scn;
  block_id_t out_block_id = LOG_INVALID_BLOCK_ID;

  CLOG_LOG(INFO, "begin case1");
  // case 1: OSS上不存在空洞
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(15, out_block_id);
  EXPECT_EQ(scns[5], out_block_min_scn);
  CLOG_LOG(INFO, "begin case1", K(scns[5]), K(target_scn));

  // case 2: 假设全局checkpoint点是15，删除block 12，第一次二分查询的block不存在。
  // OSS上日志：10 11  13 14 15 16 17 18 19
  CLOG_LOG(INFO, "begin case2");
  block_id_t deleted_block_id = 12;
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      tenant_id, ls_id, deleted_block_id, deleted_block_id+1));
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(15, out_block_id);
  EXPECT_EQ(scns[5], out_block_min_scn);

  // case 3: 继续删除block_id为13、14和15的块，第一次和第二次二分查询的block都不存在
  // OSS上日志：10 11  16 17 18 19
  CLOG_LOG(INFO, "begin case3");
  target_scn = target_scns[6];
  deleted_block_id = 13;
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(
      tenant_id, ls_id, deleted_block_id, deleted_block_id+2));
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(16, out_block_id);
  EXPECT_EQ(scns[6], out_block_min_scn);

  // case 4: 测试第一次二分查询的block存在，但其右边在全局checkpoint之前的block都不存在
  CLOG_LOG(INFO, "begin case4");
  temp_start_block_id = 4;
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, temp_start_block_id, temp_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(16, out_block_id);
  EXPECT_EQ(scns[6], out_block_min_scn);

  // case 5: 测试第一次二分查询时落在全局checkpoint的右边，并且mid_block存在
  CLOG_LOG(INFO, "begin case5");
  temp_start_block_id = 12;
  temp_end_block_id = 24;
  target_scn = target_scns[9];
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
        tenant_id, ls_id, temp_start_block_id, temp_end_block_id,
        target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(19, out_block_id);
  EXPECT_EQ(scns[9], out_block_min_scn);

  // case 6: 测试第一次二分查询时落在全局checkpoint的右边，并且mid_block不存在
  CLOG_LOG(INFO, "begin case6");
  temp_start_block_id = 15;
  temp_end_block_id = 25;
  target_scn = target_scns[8];
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
        tenant_id, ls_id, temp_start_block_id, temp_end_block_id,
        target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(18, out_block_id);
  EXPECT_EQ(scns[8], out_block_min_scn);
}

TEST_F(TestLogEXTUtils, locate_with_gc)
{
  CLOG_LOG(INFO, "begin locate_with_gc");
  uint64_t tenant_id = 1002;
  ObLSID ls_id(1001);
  const int64_t block_num = 6000;
  // 前置条件准备
  EXPECT_EQ(OB_SUCCESS, create_tenant(tenant_id));
  EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, ls_id.id_));
  int64_t start_ts = ObTimeUtility::current_time();
  (void)srand(start_ts);
  int64_t hour = 60 * 60;
  std::vector<SCN> block_scns(block_num);
  for (SCN &scn : block_scns) {
    scn.convert_from_ts(start_ts++);
  }
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), 0, block_scns));
  block_id_t gc_start_block_id = 0;
  block_id_t min_block_id = 0;
  block_id_t gc_end_block_id = random() % 1000 + 1;

  sleep(1);
  {
    // case 1: 测试locate范围完全落后OSS范围
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(tenant_id, ls_id, gc_start_block_id, gc_end_block_id));
    block_id_t locate_start_block_id = 0;
    block_id_t locate_end_block_id = gc_end_block_id;
    block_id_t target_idx = random() % (locate_end_block_id - locate_start_block_id);
    SCN target_scn = block_scns[target_idx];
    SCN out_block_min_scn;
    block_id_t out_block_id;
    CLOG_LOG(INFO, "locate_by_scn_coarsely with case 1", K(locate_start_block_id), K(locate_end_block_id), K(target_idx),
                                                         K(gc_start_block_id), K(gc_end_block_id), K(min_block_id));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, locate_start_block_id, locate_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
  }

  gc_start_block_id = gc_end_block_id;
  gc_end_block_id = gc_start_block_id + random() % 1000 + 1;
  min_block_id = gc_end_block_id;

  {
    // case 2: 测试locate范围部分落后于OSS范围
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(tenant_id, ls_id, gc_start_block_id, gc_end_block_id));
    block_id_t locate_start_block_id = random() % min_block_id;
    block_id_t locate_end_block_id = gc_end_block_id + 100;
    block_id_t target_idx = locate_start_block_id + random() % (min_block_id - locate_start_block_id);
    SCN target_scn = block_scns[target_idx];
    SCN out_block_min_scn;
    block_id_t out_block_id;
    CLOG_LOG(INFO, "locate_by_scn_coarsely with case 2.1", K(locate_start_block_id), K(locate_end_block_id), K(target_idx),
                                                           K(gc_start_block_id), K(gc_end_block_id), K(min_block_id));
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, locate_start_block_id, locate_end_block_id,
      target_scn, out_block_id, out_block_min_scn));

    target_idx = gc_end_block_id + 1;
    target_scn = block_scns[target_idx];
    CLOG_LOG(INFO, "locate_by_scn_coarsely with case 2.2", K(locate_start_block_id), K(locate_end_block_id), K(target_idx),
                                                           K(gc_start_block_id), K(gc_end_block_id), K(min_block_id));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, locate_start_block_id, locate_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(target_idx, out_block_id);
    EXPECT_EQ(block_scns[target_idx], out_block_min_scn);
  }

  gc_start_block_id = gc_end_block_id;
  gc_end_block_id = gc_start_block_id + random() % 1000 + 1;
  min_block_id = gc_end_block_id;

  {
    // case 3: 测试locate范围处于OSS范围之内
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(tenant_id, ls_id, gc_start_block_id, gc_end_block_id));
    block_id_t locate_start_block_id = min_block_id + 100;
    block_id_t locate_end_block_id = gc_end_block_id + 1000;
    block_id_t target_idx = locate_start_block_id + random() % (locate_end_block_id - locate_start_block_id);
    SCN target_scn = block_scns[target_idx];
    SCN out_block_min_scn;
    block_id_t out_block_id;
    CLOG_LOG(INFO, "locate_by_scn_coarsely with case 3.1", K(locate_start_block_id), K(locate_end_block_id), K(target_idx),
                                                           K(gc_start_block_id), K(gc_end_block_id), K(min_block_id));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, locate_start_block_id, locate_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(target_idx, out_block_id);
    EXPECT_EQ(block_scns[target_idx], out_block_min_scn);
    // case 3.2: 测试target_scn小于查找范围内的所有scn
    locate_start_block_id = gc_end_block_id + 1000;
    locate_end_block_id = locate_start_block_id + random() % 1000 + 1;
    target_idx = min_block_id - 1;
    target_scn = block_scns[target_idx];
    CLOG_LOG(INFO, "locate_by_scn_coarsely with case 3.2", K(locate_start_block_id), K(locate_end_block_id), K(target_idx),
                                                           K(gc_start_block_id), K(gc_end_block_id), K(min_block_id));
    EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, locate_start_block_id, locate_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
  }

  gc_start_block_id = gc_end_block_id;
  gc_end_block_id = gc_start_block_id + random() % 1000 + 1;
  min_block_id = gc_end_block_id;

  {
    // case 4: 测试locate范围部分超出OSS范围
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::delete_blocks(tenant_id, ls_id, gc_start_block_id, gc_end_block_id));
    block_id_t locate_start_block_id = gc_end_block_id + 1000;
    block_id_t locate_end_block_id = block_num + 3000;
    block_id_t target_idx = locate_start_block_id + random() % (block_num - locate_start_block_id);
    SCN target_scn = block_scns[target_idx];
    SCN out_block_min_scn;
    block_id_t out_block_id;
    CLOG_LOG(INFO, "locate_by_scn_coarsely with case 4", K(locate_start_block_id), K(locate_end_block_id), K(target_idx),
                                                         K(gc_start_block_id), K(gc_end_block_id), K(min_block_id));
    EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
      tenant_id, ls_id, locate_start_block_id, locate_end_block_id,
      target_scn, out_block_id, out_block_min_scn));
    EXPECT_EQ(target_idx, out_block_id);
    EXPECT_EQ(block_scns[target_idx], out_block_min_scn);
  }
}

TEST_F(TestLogEXTUtils, boundary_test)
{
  CLOG_LOG(INFO, "begin boundary_test");
  uint64_t tenant_id = 1002;
  ObLSID ls_id(1001);
  block_id_t start_block_id = 3;
  int64_t block_num = MAX_BLOCK_ID - start_block_id + 1;
  EXPECT_EQ(OB_SUCCESS, create_tenant(tenant_id));
  EXPECT_EQ(OB_SUCCESS, create_palf(tenant_id, ls_id.id()));

  int64_t start_ts = ObTimeUtility::current_time();
  (void)srand(start_ts);
  int64_t hour = 60 * 60;
  std::vector<SCN> block_scns(block_num);
  for (SCN &scn : block_scns) {
    scn.convert_from_ts(start_ts++);
  }
  EXPECT_EQ(OB_SUCCESS, upload_blocks(tenant_id, ls_id.id(), start_block_id, block_scns));
  SCN target_scn = block_scns[block_num - 1];
  block_id_t out_block_id;
  SCN out_block_min_scn;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
    tenant_id, ls_id, start_block_id, LOG_INITIAL_BLOCK_ID,
    target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObSharedLogUtils::locate_by_scn_coarsely(
    tenant_id, ls_id, start_block_id, LOG_MAX_BLOCK_ID,
    target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
    tenant_id, ls_id, LOG_INITIAL_BLOCK_ID, MAX_BLOCK_ID+1, target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(MAX_BLOCK_ID, out_block_id);
  EXPECT_EQ(target_scn, out_block_min_scn);
  EXPECT_EQ(OB_SUCCESS, ObSharedLogUtils::locate_by_scn_coarsely(
    tenant_id, ls_id, MAX_BLOCK_ID, MAX_BLOCK_ID+1, target_scn, out_block_id, out_block_min_scn));
  EXPECT_EQ(MAX_BLOCK_ID, out_block_id);
  EXPECT_EQ(target_scn, out_block_min_scn);

  target_scn.convert_for_logservice(block_scns[0].get_val_for_logservice() - 10);
  EXPECT_EQ(OB_ERR_OUT_OF_LOWER_BOUND, ObSharedLogUtils::locate_by_scn_coarsely(
    tenant_id, ls_id, LOG_INITIAL_BLOCK_ID, MAX_BLOCK_ID, target_scn, out_block_id, out_block_min_scn));
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
