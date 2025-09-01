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
#define USING_LOG_PREFIX STORAGETEST

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/ob_ss_file_util.h"
#include "storage/shared_storage/ob_ss_format_util.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"

#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::share;

#define PRINT_MACRO_ID_TO_PATH(OBJ_TYPE, IS_LOCAL_CACHE)          \
  {                                                               \
    file_id.set_storage_object_type(OBJ_TYPE);                    \
    ret = ctx.set_file_ctx(file_id, ls_epoch_id, IS_LOCAL_CACHE); \
    LOG_INFO("file id to path", KR(ret), K(file_id), K(ls_epoch_id), K(IS_LOCAL_CACHE), K(ctx.get_path())); \
  }

#define PRINT_MACRO_ID_PARENT_DIR_PATH(OBJ_TYPE) \
  {                                              \
    file_id.set_storage_object_type(OBJ_TYPE);   \
    ret = ObFileHelper::get_file_parent_dir(dir_path, common::MAX_PATH_SIZE, file_id, ls_epoch_id); \
    LOG_INFO("file id's parent dir path", KR(ret), K(file_id), K(ls_epoch_id), K(dir_path)); \
  }

#define CHECK_MACRO_ID_TO_PATH(OBJ_TYPE, IS_LOCAL_CACHE, EXPECTED_RET, EXPECTED_PATH)        \
  {                                                                                          \
    file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::OBJ_TYPE));   \
    ASSERT_EQ(EXPECTED_RET, ctx.set_file_ctx(file_id, ls_epoch_id, IS_LOCAL_CACHE));         \
    if (OB_SUCCESS == EXPECTED_RET) {                                                        \
      expected_path[0] = '\0';                                                               \
      if (IS_LOCAL_CACHE) {                                                                  \
        ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, common::MAX_PATH_SIZE, "%s/%s", \
                                              OB_DIR_MGR.get_local_cache_root_dir(), EXPECTED_PATH)); \
      } else {                                                                               \
        ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, common::MAX_PATH_SIZE, "%s/%s", object_storage_root_dir, EXPECTED_PATH)); \
      }                                                                                      \
      ASSERT_EQ(0, STRCMP(ctx.get_path(), expected_path)) << ctx.get_path() << "\n" << expected_path; \
    }                                                                                        \
  }

#define CHECK_MACRO_ID_PARENT_DIR(OBJ_TYPE, EXPECTED_RET, EXPECTED_PATH)                            \
  {                                                                                                 \
    file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::OBJ_TYPE));          \
    dir_path[0] = '\0';                                                                             \
    ASSERT_EQ(EXPECTED_RET, ObFileHelper::get_file_parent_dir(dir_path, common::MAX_PATH_SIZE, file_id, ls_epoch_id)); \
    if (OB_SUCCESS == EXPECTED_RET) {                                                               \
      expected_dir_path[0] = '\0';                                                                  \
      ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_dir_path, common::MAX_PATH_SIZE, "%s/%s",      \
                                            OB_DIR_MGR.get_local_cache_root_dir(), EXPECTED_PATH)); \
      ASSERT_EQ(0, STRCMP(dir_path, expected_dir_path))<< dir_path << "\n" << expected_dir_path;    \
    }                                                                                               \
  }

#define CHECK_INC_MACRO_ID_TO_PATH(OBJ_TYPE, IS_LOCAL_CACHE, INNER_TABLET, EXPECTED_PATH)           \
  {                                                                                                 \
    inc_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::OBJ_TYPE));      \
    expected_path[0] = '\0';                                                                        \
    ASSERT_EQ(OB_SUCCESS, ctx.set_file_ctx(inc_file_id, ls_epoch_id, IS_LOCAL_CACHE));              \
    if (IS_LOCAL_CACHE) {                                                                           \
      ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, common::MAX_PATH_SIZE, "%s/%s",          \
                                            OB_DIR_MGR.get_local_cache_root_dir(), EXPECTED_PATH)); \
    } else {                                                                                        \
      ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, common::MAX_PATH_SIZE, "%s/%s", object_storage_root_dir, EXPECTED_PATH)); \
    }                                                                                               \
    ASSERT_EQ(0, STRCMP(ctx.get_path(), expected_path)) << ctx.get_path() << "\n" << expected_path; \
  }

class TestFileDelOp : public share::ObScanDirOp
{
public:
  TestFileDelOp(const char *dir = nullptr) {}
  virtual int func(const dirent *entry) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(entry)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid entry", KR(ret));
    } else {
      char full_path[common::MAX_PATH_SIZE] = { 0 };
      int p_ret = snprintf(full_path, sizeof(full_path), "%s/%s", dir_, entry->d_name);
      if (p_ret <= 0 || p_ret >= sizeof(full_path)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("file name too long", KR(ret), K_(dir), K(entry->d_name));
      } else if (OB_FAIL(ObIODeviceLocalFileOp::unlink(full_path))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to del file", KR(ret), K(full_path));
        }
      }
    }
    return ret;
  }
};

class TestDirDelOp : public share::ObScanDirOp
{
public:
  TestDirDelOp(const char *dir = nullptr) {}
  virtual int func(const dirent *entry) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(entry)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid entry", KR(ret));
    } else {
      char full_path[common::MAX_PATH_SIZE] = { 0 };
      int p_ret = snprintf(full_path, sizeof(full_path), "%s/%s", dir_, entry->d_name);
      if (p_ret <= 0 || p_ret >= sizeof(full_path)) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("file name too long", KR(ret), K_(dir), K(entry->d_name));
      } else if (OB_FAIL(ObIODeviceLocalFileOp::rmdir(full_path))) {
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to del dir", KR(ret), K(full_path));
        }
      }
    }
    return ret;
  }
};

class TestFileManager : public ::testing::Test
{
public:
  TestFileManager() = default;
  virtual ~TestFileManager() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  void get_macro_block_scatter_dir_size(int64_t &scatter_dir_size);
  void get_ls_id_dir_size(const int64_t ls_id,
                          const int64_t ls_epoch_id,
                          int64_t &scatter_dir_size);
};

void TestFileManager::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void TestFileManager::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestFileManager::get_macro_block_scatter_dir_size(int64_t &scatter_dir_size)
{
  // scatter dirs of private data/meta macro, shared mini/minor/major macro, external table file
  scatter_dir_size = 0;
  ObIODFileStat statbuf;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  char scatter_dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tablet_data_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID()));
  for (int64_t i = 0; i < ObDirManager::PRIVATE_MACRO_SCATTER_DIR_NUM; ++i) {
    scatter_dir_path[0] = '\0';
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_path,
              sizeof(scatter_dir_path), "%s/%02ld", dir_path, i));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(scatter_dir_path, statbuf));
    scatter_dir_size += statbuf.size_;
  }
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_macro_cache_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ObSharedMacroType::MINI));
  for (int64_t i = 0; i < ObDirManager::SHARED_MACRO_SCATTER_DIR_NUM; ++i) {
    scatter_dir_path[0] = '\0';
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_path,
              sizeof(scatter_dir_path), "%s/%02lX", dir_path, i));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(scatter_dir_path, statbuf));
    scatter_dir_size += (statbuf.size_ * 3); // mini + minor + major, thus multiply with 3
  }
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_external_table_file_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID()));
  for (int64_t i = 0; i < ObDirManager::EXTERNAL_TABLE_FILE_SCATTER_DIR_NUM; ++i) {
    scatter_dir_path[0] = '\0';
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_path,
              sizeof(scatter_dir_path), "%s/%02lX", dir_path, i));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(scatter_dir_path, statbuf));
    scatter_dir_size += statbuf.size_;
  }
  dir_path[0] ='\0';
}

void TestFileManager::get_ls_id_dir_size(
    const int64_t ls_id,
    const int64_t ls_epoch_id,
    int64_t &dir_size)
{
  dir_size = 0;
  ObIODFileStat statbuf;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_meta_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  dir_size += statbuf.size_;
  dir_path[0] = '\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_ls_id_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  dir_size += statbuf.size_;
  dir_path[0] = '\0';

  // scatter dirs of tablet meta
  int64_t scatter_dir_size = 0;
  char scatter_dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_meta_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id));
  for (int64_t i = 0; i < ObDirManager::PRIVATE_TABLET_META_SCATTER_DIR_NUM; ++i) {
    scatter_dir_path[0] = '\0';
    ASSERT_EQ(OB_SUCCESS, databuff_printf(scatter_dir_path,
              sizeof(scatter_dir_path), "%s/%02ld", dir_path, i));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(scatter_dir_path, statbuf));
    scatter_dir_size += statbuf.size_;
  }
  dir_size += scatter_dir_size;
  dir_path[0] ='\0';
}

TEST_F(TestFileManager, test_check_micro_cache_file_size)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tnt_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tnt_file_mgr);
  ObTenantDiskSpaceManager *tnt_disk_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tnt_disk_mgr);
  const int64_t total_disk_size = tnt_disk_mgr->total_disk_size_;
  const int64_t ori_micro_cache_file_size = tnt_disk_mgr->get_micro_cache_file_size();
  const double def_micro_pct = ori_micro_cache_file_size / static_cast<double>(total_disk_size);
  const int64_t exp_micro_cache_file_size = total_disk_size * def_micro_pct;
  ASSERT_EQ(ori_micro_cache_file_size, exp_micro_cache_file_size);

  char micro_cache_file_path[512] = {0};
  ret = tnt_file_mgr->get_micro_cache_file_path(micro_cache_file_path, sizeof(micro_cache_file_path), MTL_ID(), MTL_EPOCH_ID());
  ASSERT_EQ(OB_SUCCESS, ret);
  int open_flag = O_DIRECT | O_RDWR | O_LARGEFILE;
  int micro_cache_file_fd = OB_INVALID_FD;
  ret = ObSSFileUtil::open(micro_cache_file_path, open_flag, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH, micro_cache_file_fd);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObIODFileStat f_stat;
  ret = ObIODeviceLocalFileOp::stat(micro_cache_file_path, f_stat);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(f_stat.size_, ori_micro_cache_file_size);

  bool succ_adjust = false;
  int64_t new_micro_cache_size = 0;
  ret = tnt_disk_mgr->try_adjust_cache_file_size(10, succ_adjust, new_micro_cache_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(false, succ_adjust);

  ret = tnt_disk_mgr->try_adjust_cache_file_size(25, succ_adjust, new_micro_cache_size);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, succ_adjust);
  ObIODFileStat f_stat1;
  ret = ObIODeviceLocalFileOp::stat(micro_cache_file_path, f_stat1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(f_stat1.size_, new_micro_cache_size);
  ASSERT_LT(ori_micro_cache_file_size, new_micro_cache_size);
}

/* Test whether the data written by pwrite_cache_block is 4K aligned when the valid data of buf is smaller than
 * size. */
TEST_F(TestFileManager, test_micro_cache_pread_and_pwrite)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tenant_file_mgr);

  const int64_t alignment = 4096;
  const int64_t WRITE_IO_SIZE = 1 << 14;      // 16K
  const int64_t valid_data_size = 15 * 1024;  // 15K
  ObMemAttr attr(MTL_ID(), "test");
  char *write_buf = static_cast<char *>(ob_malloc_align(alignment, WRITE_IO_SIZE, attr));
  char *read_buf = static_cast<char *>(ob_malloc_align(alignment, WRITE_IO_SIZE, attr));

  MEMSET(write_buf, '\0', WRITE_IO_SIZE);
  MEMSET(write_buf, 'a', valid_data_size);  // 15K
  int64_t written_size = 0;
  int64_t read_size = 0;
  int64_t write_offset = 2 * 2 * 1024 * 1024L;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->pwrite_cache_block(write_offset, WRITE_IO_SIZE, write_buf, written_size));
  ASSERT_EQ(written_size, WRITE_IO_SIZE);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->fsync_cache_file());
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->pread_cache_block(write_offset, WRITE_IO_SIZE, read_buf, read_size));
  ASSERT_EQ(read_size, WRITE_IO_SIZE);
  ASSERT_EQ(0, memcmp(write_buf, read_buf, read_size));

  ob_free_align(write_buf);
  ob_free_align(read_buf);
}

TEST_F(TestFileManager, test_path_convert)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  ObPathContext ctx;
  char expected_path[common::MAX_PATH_SIZE] = {0};
  bool is_local_cache = true;
  int64_t ls_epoch_id = 4;
  file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  file_id.set_second_id(3);
  file_id.set_third_id(2);
  file_id.set_macro_transfer_seq(0);
  file_id.set_tenant_seq(5);
  char *object_storage_root_dir = nullptr;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_object_storage_root_dir(object_storage_root_dir));

  MacroBlockId inc_file_id;
  inc_file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  inc_file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  inc_file_id.set_second_id(3); // tablet_id
  inc_file_id.set_third_id(4294967297); // op_id = 1, macro_seq = 1
  inc_file_id.set_ss_fourth_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);

  // each object type and print out the path log
  for (uint64_t obj_id = static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO); obj_id < static_cast<uint64_t>(ObStorageObjectType::MAX); ++obj_id) {
    PRINT_MACRO_ID_TO_PATH(obj_id, true/*is_in_local*/);
    PRINT_MACRO_ID_TO_PATH(obj_id, false/*is_in_local*/);
  }

  // 0.PRIVATE_DATA_MACRO
  // tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/data/macro_server_id_seq_id
  CHECK_MACRO_ID_TO_PATH(PRIVATE_DATA_MACRO, true/*is_in_local*/, OB_SUCCESS, "1_0/tablet_data/03/3/0/data/svr2seq5.T0");
  // cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/data/macro_server_id_seq_id
  CHECK_MACRO_ID_TO_PATH(PRIVATE_DATA_MACRO, false/*is_in_local*/, OB_SUCCESS, "cluster_1/server_2/1_0/tablet_data/3/0/data/svr2seq5.T0");

  // 1.PRIVATE_META_MACRO
  // tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/meta/macro_server_id_seq_id
  CHECK_MACRO_ID_TO_PATH(PRIVATE_META_MACRO, true/*is_in_local*/, OB_SUCCESS, "1_0/tablet_data/03/3/0/meta/svr2seq5.T1");
  // cluster_id/server_id/tenant_id_epoch_id/tablet_data/tablet_id/transfer_seq/meta/macro_server_id_seq_id
  CHECK_MACRO_ID_TO_PATH(PRIVATE_META_MACRO, false/*is_in_local*/, OB_SUCCESS, "cluster_1/server_2/1_0/tablet_data/3/0/meta/svr2seq5.T1");

  // 2. SHARED_MINI_DATA_MACRO
  // inner tablet 49401
  inc_file_id.set_second_id(49401);
  inc_file_id.set_ss_fourth_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_mini_macro_cache/tablet_id_op_id_macro_seq_id_ls_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_DATA_MACRO, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/ls/1001/TX_CTX_op1seq1.T2");
  // cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/data/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_DATA_MACRO, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_CTX/mini/sstable/op_1/data/seq1.T2");
  //user tablet 200001
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_fourth_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_mini_macro_cache/tablet_id_reorganization_scn_op_id_macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_DATA_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/F9/t200001rs0op1seq1.T2");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/data/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_DATA_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200001/0/mini/sstable/op_1/data/seq1.T2");

  // 3. SHARED_MINI_META_MACRO
  // inner tablet 49401
  inc_file_id.set_second_id(49401);
  inc_file_id.set_ss_fourth_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_mini_macro_cache/tablet_id_op_id_macro_seq_id_ls_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_META_MACRO, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/ls/1001/TX_CTX_op1seq1.T3");
  // cluster_id/tenant_id/ls/ls_id/tablet_name/mini/sstable/op_id/meta/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_META_MACRO, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_CTX/mini/sstable/op_1/meta/seq1.T3");
  //user tablet 200001
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_fourth_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_mini_macro_cache/tablet_id_reorganization_scn_op_id_macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_META_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/58/t200001rs0op1seq1.T3");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mini/sstable/op_id/meta/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINI_META_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200001/0/mini/sstable/op_1/meta/seq1.T3");

  // 4. SHARED_MINOR_DATA_MACRO
  // inner tablet 49403
  inc_file_id.set_second_id(49403); // TX_LOCK
  inc_file_id.set_ss_fourth_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_minor_macro_cache/tablet_id_op_id_macro_seq_id_ls_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_DATA_MACRO, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/ls/1001/TX_LOCK_op1seq1.T4");
  // cluster_id/tenant_id/ls/ls_id/tablet_name/minor/sstable/op_id/data/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_DATA_MACRO, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_LOCK/minor/sstable/op_1/data/seq1.T4");
  //user tablet 200001
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_fourth_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_minor_macro_cache/tablet_id_reorganization_scn_op_id_macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_DATA_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/7C/t200001rs0op1seq1.T4");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/data/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_DATA_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200001/0/minor/sstable/op_1/data/seq1.T4");

  // 5. SHARED_MINOR_META_MACRO
  // inner tablet 49402
  inc_file_id.set_second_id(49402);// TX_DATA
  inc_file_id.set_ss_fourth_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_minor_macro_cache/tablet_id_op_id_macro_seq_id_ls_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_META_MACRO, true/*is_in_local*/, true/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/ls/1001/TX_DATA_op1seq1.T5");
  // cluster_id/tenant_id/tablet_name/minor/sstable/op_id/meta/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_META_MACRO, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_DATA/minor/sstable/op_1/meta/seq1.T5");
  //user tablet 200001
  inc_file_id.set_second_id(200001);
  inc_file_id.set_ss_fourth_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  // tenant_id_epoch_id/shared_minor_macro_cache/tablet_id_reorganization_scn_op_id_macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_META_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/52/t200001rs0op1seq1.T5");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/minor/sstable/op_id/meta/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MINOR_META_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200001/0/minor/sstable/op_1/meta/seq1.T5");

  // 6.SHARED_MAJOR_DATA_MACRO
  // tenant_id_epoch_id/shared_major_macro_cache/tablet_id_reorganization_scn_cg_id_macro_seq_id
  CHECK_MACRO_ID_TO_PATH(SHARED_MAJOR_DATA_MACRO, true/*is_in_local*/, OB_SUCCESS, "1_0/shared_major_macro_cache/31/t3rs5242880cg0seq2.T6");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/cg_id/data/macro_seq_id
  CHECK_MACRO_ID_TO_PATH(SHARED_MAJOR_DATA_MACRO, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5242880/major/sstable/cg_0/data/seq2.T6");

  // 7.SHARED_MAJOR_META_MACRO
  // tenant_id_epoch_id/shared_major_macro_cache/tablet_id_reorganization_scn_cg_id_macro_seq_id
  CHECK_MACRO_ID_TO_PATH(SHARED_MAJOR_META_MACRO, true/*is_in_local*/, OB_SUCCESS, "1_0/shared_major_macro_cache/C6/t3rs5242880cg0seq2.T7");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/major/cg_id/meta/macro_seq_id
  CHECK_MACRO_ID_TO_PATH(SHARED_MAJOR_META_MACRO, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5242880/major/sstable/cg_0/meta/seq2.T7");

  // 8.TMP_FILE
  // tenant_id_epoch_id/tmp_data/tmp_file_id/segment_id
  CHECK_MACRO_ID_TO_PATH(TMP_FILE, true/*is_in_local*/, OB_SUCCESS, "1_0/tmp_data/3/seg2.T8");

  expected_path[0] = '\0';
  // tenant_id_epoch_id/tmp_data/tmp_file_id/segment_id.deleted
  ASSERT_EQ(OB_SUCCESS, ctx.set_logical_delete_ctx(file_id, ls_epoch_id));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(expected_path, common::MAX_PATH_SIZE, "%s/1_0/%s/3/seg2.T8.deleted",
                                        OB_DIR_MGR.get_local_cache_root_dir(), TMP_DATA_DIR_STR));
  ASSERT_EQ(0, STRCMP(ctx.get_path(), expected_path));

  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/segment_id
  CHECK_MACRO_ID_TO_PATH(TMP_FILE, false/*is_in_local*/, OB_SUCCESS, "cluster_1/server_1/1_0/tmp_data/3/seg2.T8");

  // 9.SERVER_META
  // super_block
  CHECK_MACRO_ID_TO_PATH(SERVER_META, true/*is_in_local*/, OB_SUCCESS, "SERVER_META.T9");
  CHECK_MACRO_ID_TO_PATH(SERVER_META, false/*is_in_local*/, OB_NOT_SUPPORTED, "");

  // 17.PRIVATE_TABLET_META
  // tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/tablet_id/tablet_meta_version_transfer_seq
  CHECK_MACRO_ID_TO_PATH(PRIVATE_TABLET_META, true/*is_in_local*/, OB_SUCCESS, "1_0/ls/3_4/tablet_meta/02/2/0/ver5.T10");
  CHECK_MACRO_ID_TO_PATH(PRIVATE_TABLET_META, false/*is_in_local*/, OB_SUCCESS, "cluster_1/server_1/1_0/ls/3/tablet_meta/2/0/ver5.T10");

  // 27. MAJOR_PREWARM_DATA
  // cluster_id/tenant_id/tablet/tablet_id/major/compaction_scn
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_DATA, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_DATA, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5242880/major/prewarm_info/scn2.T13");

  // 28. MAJOR_PREWARM_DATA_INDEX
  // cluster_id/tenant_id/tablet/tablet_id/major/compaction_scn
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_DATA_INDEX, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_DATA_INDEX, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5242880/major/prewarm_info/scn2.T14");

  // 29. MAJOR_PREWARM_META
  // cluster_id/tenant_id/tablet/tablet_id/major/scn_id_compaction_scn_prewarm_data
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_META, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_META, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5242880/major/prewarm_info/scn2.T15");

  // 30. MAJOR_PREWARM_META_INDEX
  // cluster_id/tenant_id/tablet/tablet_id/major/scn_id_compaction_scn_prewarm_data
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_META_INDEX, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(MAJOR_PREWARM_META_INDEX, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_1/tablet/3/5242880/major/prewarm_info/scn2.T16");

  // 31.TENANT_DISK_SPACE_META
  // tenant_id_epoch_id/tenant_disk_space_meta
  CHECK_MACRO_ID_TO_PATH(TENANT_DISK_SPACE_META, true/*is_in_local*/, OB_SUCCESS, "3_2/TENANT_DISK_SPACE_META.T17");
  CHECK_MACRO_ID_TO_PATH(TENANT_DISK_SPACE_META, false/*is_in_local*/, OB_NOT_SUPPORTED, "");

  // 35. IS_SHARED_TENANT_DELETED
  // cluster_id/tenant_id/is_shared_tenant_deleted
  CHECK_MACRO_ID_TO_PATH(IS_SHARED_TENANT_DELETED, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(IS_SHARED_TENANT_DELETED, false/*is_in_local*/, OB_SUCCESS, "cluster_1/tenant_3/IS_SHARED_TENANT_DELETED.T18");

  // 37. SHARED_MICRO_DATA_MACRO
  CHECK_MACRO_ID_TO_PATH(SHARED_MICRO_DATA_MACRO, true/*is_in_local*/, OB_INVALID_ARGUMENT, "");
  CHECK_MACRO_ID_TO_PATH(SHARED_MICRO_DATA_MACRO, false/*is_in_local*/, OB_INVALID_ARGUMENT, "");

  // 38. SHARED_MICRO_META_MACRO
  CHECK_MACRO_ID_TO_PATH(SHARED_MICRO_META_MACRO, true/*is_in_local*/, OB_INVALID_ARGUMENT, "");
  CHECK_MACRO_ID_TO_PATH(SHARED_MICRO_META_MACRO, false/*is_in_local*/, OB_INVALID_ARGUMENT, "");

  file_id.set_fourth_id(5);
  // 39. UNSEALED_REMOTE_SEG_FILE
  // cluster_id/server_id/tenant_id_epoch_id/tmp_data/tmp_file_id/segment_id_valid_length
  CHECK_MACRO_ID_TO_PATH(UNSEALED_REMOTE_SEG_FILE, true/*is_in_local*/, OB_NOT_SUPPORTED, "");
  CHECK_MACRO_ID_TO_PATH(UNSEALED_REMOTE_SEG_FILE, false/*is_in_local*/, OB_SUCCESS, "cluster_1/server_1/1_0/tmp_data/3/seg2len5.T21");

  // MDS: 55-68
  // -- MDS mini and MDS minor -- //
  inc_file_id.set_second_id(3);
  // tenant_id_epoch_id/shared_mini_macro_cache/tablet_create_id_op_id_macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MDS_MINI_DATA_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/0A/t3rs0op1seq1.T22");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/mini/sstable/op_id/data/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MDS_MINI_DATA_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/3/0/mds/mini/sstable/op_1/data/seq1.T22");
  // tenant_id_epoch_id/shared_mini_macro_cache/tablet_id_reorganization_scn_op_id_macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MDS_MINI_META_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_mini_macro_cache/CC/t3rs0op1seq1.T23");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/mini/sstable/op_id/meta/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MDS_MINI_META_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/3/0/mds/mini/sstable/op_1/meta/seq1.T23");
  // tenant_id_epoch_id/shared_minor_macro_cache/tablet_id_reorganization_scn_op_id_macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MDS_MINOR_DATA_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/B6/t3rs0op1seq1.T24");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/minor/sstable/op_id/data/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MDS_MINOR_DATA_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/3/0/mds/minor/sstable/op_1/data/seq1.T24");
  // tenant_id_epoch_id/shared_minor_macro_cache/tablet_id_reorganization_scn_op_id_macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MDS_MINOR_META_MACRO, true/*is_in_local*/, false/*is_inner_tablet*/, "1_0/shared_minor_macro_cache/38/t3rs0op1seq1.T25");
  // cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/mds/minor/sstable/op_id/meta/macro_seq_id
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_MDS_MINOR_META_MACRO, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/3/0/mds/minor/sstable/op_1/meta/seq1.T25");
  // --- Atomic protocol files -- //
  inc_file_id.set_third_id(1); // set op_id = 1

  // 74. SHARED_TABLET_SUB_META
  // user_tablet : cluster_id/tenant_id/tablet/tablet_id/reorganization_scn/tablet_meta/data/op_id_data_seq
  inc_file_id.set_second_id(200001);
  inc_file_id.set_third_id(4294967297); // op_id = 1, macro_seq = 1
  inc_file_id.set_ss_fourth_id(false/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_TABLET_SUB_META, false/*is_in_local*/, false/*is_inner_tablet*/, "cluster_1/tenant_1/tablet/200001/0/meta/op1seq1.T76");
  // inner_tablet : cluster_id/tenant_id/ls/ls_id/tablet_name/tablet_meta/data/op_id_data_seq
  inc_file_id.set_third_id(4294967297); // op_id = 1, macro_seq = 1
  inc_file_id.set_second_id(49401); // TX_CTX
  inc_file_id.set_ss_fourth_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_TABLET_SUB_META, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_CTX/meta/op1seq1.T76");
  inc_file_id.set_second_id(49402); // TX_DATA
  inc_file_id.set_ss_fourth_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_TABLET_SUB_META, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_DATA/meta/op1seq1.T76");
  inc_file_id.set_second_id(49403); // TX_LOCK
  inc_file_id.set_ss_fourth_id(true/*is_inner_tablet*/, 1001/*ls_id*/, 0/*reorganization_scn*/);
  CHECK_INC_MACRO_ID_TO_PATH(SHARED_TABLET_SUB_META, false/*is_in_local*/, true/*is_inner_tablet*/, "cluster_1/tenant_1/ls/1001/TX_LOCK/meta/op1seq1.T76");

}

TEST_F(TestFileManager, test_get_file_parent_dir)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  char expected_dir_path[common::MAX_PATH_SIZE] = {0};
  int64_t ls_epoch_id = 4;
  file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  file_id.set_second_id(3);
  file_id.set_third_id(2);
  file_id.set_macro_transfer_seq(0);
  file_id.set_tenant_seq(5);

  // each object type and print out the parent dir path log
  for (uint64_t obj_id = static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO); obj_id < static_cast<uint64_t>(ObStorageObjectType::MAX); ++obj_id) {
    PRINT_MACRO_ID_PARENT_DIR_PATH(obj_id);
  }

  // 1.PRIVATE_DATA_MACRO
  // tenant_id_epoch_id/tablet_data/scatter_id/tablet_id/transfer_seq/data/
  CHECK_MACRO_ID_PARENT_DIR(PRIVATE_DATA_MACRO, OB_SUCCESS, "1_0/tablet_data/03/3/0/data");

  // 2.SHARED_MAJOR_DATA_MACRO
  CHECK_MACRO_ID_PARENT_DIR(SHARED_MAJOR_DATA_MACRO, OB_NOT_SUPPORTED, "");

  // 3.TMP_FILE
  // tenant_id_epoch_id/tmp_data/tmp_file_id/segment_id
  CHECK_MACRO_ID_PARENT_DIR(TMP_FILE, OB_SUCCESS, "1_0/tmp_data/3");

  // 4.SERVER_META
  // CHECK_MACRO_ID_PARENT_DIR(SERVER_META, OB_SUCCESS, "");

  // 12.PRIVATE_TABLET_META
  // tenant_id_epoch_id/ls/ls_id_epoch_id/tablet_meta/scatter_id/tablet_id/tablet_meta_version_transfer_seq
  CHECK_MACRO_ID_PARENT_DIR(PRIVATE_TABLET_META, OB_SUCCESS, "1_0/ls/3_4/tablet_meta/02/2/0");

  // 18.PRIVATE_META_MACRO
  CHECK_MACRO_ID_PARENT_DIR(PRIVATE_META_MACRO, OB_SUCCESS, "1_0/tablet_data/03/3/0/meta");

  // 19.SHARED_MAJOR_META_MACRO
  CHECK_MACRO_ID_PARENT_DIR(SHARED_MAJOR_META_MACRO, OB_NOT_SUPPORTED, "");
  // 24.TENANT_DISK_SPACE_META
  CHECK_MACRO_ID_PARENT_DIR(TENANT_DISK_SPACE_META, OB_SUCCESS, "3_2");
  // 28. IS_SHARED_TENANT_DELETED
  CHECK_MACRO_ID_PARENT_DIR(IS_SHARED_TENANT_DELETED, OB_NOT_SUPPORTED, "");
}

TEST_F(TestFileManager, test_private_macro_file_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  file_id.set_second_id(3);  //tablet_id
  file_id.set_third_id(2);   //seq_id
  file_id.set_macro_transfer_seq(0); // transfer_seq
  file_id.set_tenant_seq(5);  //tenant_seq

  // step 1: create dir
  int64_t tablet_id = 3;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*transfer_seq*/));

  // step 2: test write private_macro_file
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  const int64_t write_io_size = 4096; // 4KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));

  // step 3: test read private_macro_file
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = file_id;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));

  // test 4 : test fsync private_macro_file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->fsync_file(file_id));

  // step 5: test is exist file
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(file_id, 0 /*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // step 6: test get file length
  int64_t file_length = 0;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_file_length(file_id, 0 /*ls_epoch_id*/, file_length));
  ASSERT_EQ(write_io_size, file_length);

  // step 7: test list tablet_dir
  ObArray<int64_t> tablet_dirs;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tablet_data_dir(tablet_dirs));
  ASSERT_EQ(1, tablet_dirs.count());
  ASSERT_EQ(tablet_id, tablet_dirs.at(0));

  // test 8: test calc_private_macro_disk_space
  ObCalibrateDiskSpaceResult calibrate_res;
  int64_t expected_disk_size = write_io_size;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_data_tablet_id_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(), tablet_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_data_tablet_id_transfer_seq_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*transfer_seq*/));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tablet_id_macro_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*transfer_seq*/, ObMacroType::DATA_MACRO));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tablet_id_macro_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*transfer_seq*/, ObMacroType::META_MACRO));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';
  int64_t shared_mini_ls_dir_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_macro_cache_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ObSharedMacroType::MINI));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_path + STRLEN(dir_path), sizeof(dir_path), "/ls"));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  shared_mini_ls_dir_size = statbuf.size_;
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';
  int64_t shared_minor_ls_dir_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_macro_cache_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ObSharedMacroType::MINOR));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_path + STRLEN(dir_path), sizeof(dir_path), "/ls"));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  shared_minor_ls_dir_size = statbuf.size_;
  expected_disk_size += statbuf.size_;
  dir_path[0] ='\0';

  int64_t scatter_dir_size = 0;
  get_macro_block_scatter_dir_size(scatter_dir_size);
  expected_disk_size += scatter_dir_size;

  ob_usleep(2000*1000);
  int64_t start_calc_size_time_s = ObTimeUtility::current_time_s();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calc_macro_block_disk_space(start_calc_size_time_s, calibrate_res));
  ASSERT_EQ(expected_disk_size, calibrate_res.total_file_size_);

  // step 9: test delete file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(file_id));

  // step 10: test delete dir
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.delete_tablet_data_tablet_id_transfer_seq_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*transfer_seq*/));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.delete_tablet_data_tablet_id_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id));
  // step 11: test calc_private_macro_disk_space after delete file
  ob_usleep(2000*1000);
  start_calc_size_time_s = ObTimeUtility::current_time_s();
  calibrate_res.reset();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calc_macro_block_disk_space(start_calc_size_time_s, calibrate_res));
  get_macro_block_scatter_dir_size(scatter_dir_size);
  ASSERT_EQ(shared_mini_ls_dir_size + shared_minor_ls_dir_size + scatter_dir_size, calibrate_res.total_file_size_);

}

TEST_F(TestFileManager, test_tmp_file_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId file_id;
  file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  file_id.set_second_id(3);  //tmp_file_id
  file_id.set_third_id(2);   //segment_id

  // step 1: create dir
  int64_t tmp_file_id = 3;
  // ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(tmp_file_id));

  // step 2: test write tmp_file
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_tmp_file_valid_length(write_io_size);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));

  // step 3: test read tmp_file
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = file_id;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));

  // test 4: test fsync tmp_file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->fsync_file(file_id));

  // test 5: test append tmp_file
  ObStorageObjectWriteInfo append_info;
  append_info.io_desc_.set_wait_event(1);
  append_info.io_desc_.set_unsealed();
  append_info.buffer_ = write_buf;
  append_info.offset_ = write_io_size;
  append_info.size_ = write_io_size;
  append_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  append_info.mtl_tenant_id_ = MTL_ID();
  append_info.set_tmp_file_valid_length(write_io_size + write_io_size);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(append_info, write_object_handle));

  // step 6: test read tmp_file
  read_object_handle.reset();
  read_info.macro_block_id_ = file_id;
  read_info.io_desc_.set_wait_event(1);
  read_buf[0] = '\0';
  read_info.buf_ = read_buf;
  read_info.offset_ = write_io_size;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));

  // step 7: test is exist file
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(file_id, 0, is_exist));
  ASSERT_TRUE(is_exist);

  // step 8: test get file length
  int64_t file_length = 0;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_file_length(file_id, 0, file_length));
  ASSERT_EQ(2 * write_io_size, file_length);

  // test 9: test calc_tmp_file_disk_space
  ObCalibrateDiskSpaceResult calibrate_res;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  int64_t expected_disk_size = 2 * write_io_size;
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  ob_usleep(2000*1000);
  int64_t start_calc_size_time_s = ObTimeUtility::current_time_s();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calc_tmp_file_disk_space(start_calc_size_time_s, calibrate_res));
  ASSERT_EQ(expected_disk_size, calibrate_res.total_file_size_);

  // step 10: test delete file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(file_id));

}

TEST_F(TestFileManager, test_meta_file_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  // step 1: construct MacroBlockId
  MacroBlockId file_id;
  const int64_t ls_id = 7;
  const int64_t ls_epoch_id = 0;
  const int64_t tablet_id = 8;
  const int64_t meta_transfer_seq = 0;
  const int64_t meta_version_id = 9;
  file_id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  file_id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_TABLET_META));
  file_id.set_second_id(ls_id);
  file_id.set_third_id(tablet_id);
  file_id.set_meta_transfer_seq(meta_transfer_seq);
  file_id.set_meta_version_id(meta_version_id);

  // step 2: test write private_tablet_meta
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(file_id));
  const int64_t write_io_size = 4096; // 4KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_ls_epoch_id(ls_epoch_id);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));

  // step 3: test read private_tablet_meta
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = file_id;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  read_info.set_ls_epoch_id(ls_epoch_id);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));

  // test 4 : test fsync private_tablet_meta
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->fsync_file(file_id));

  // step 5: test is exist file
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(file_id, ls_epoch_id, is_exist));
  ASSERT_TRUE(is_exist);

  // step 6: test get file length
  int64_t file_length = 0;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->get_file_length(file_id, ls_epoch_id, file_length));
  ASSERT_EQ(write_io_size, file_length);

  // step 7: test list tablet_dir
  ObArray<int64_t> tablet_dirs;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tablet_meta_dir(ls_id, ls_epoch_id, tablet_dirs));
  ASSERT_EQ(1, tablet_dirs.count());
  ASSERT_EQ(tablet_id, tablet_dirs.at(0));

  // test 8: test calc_meta_file_disk_space
  ObCalibrateDiskSpaceResult calibrate_res;
  int64_t expected_disk_size = write_io_size;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_meta_tablet_id_transfer_seq_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id, meta_transfer_seq));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  dir_path[0] = '\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_meta_tablet_id_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  dir_path[0] = '\0';

  int64_t ls_id_dir_size = 0;
  get_ls_id_dir_size(ls_id, ls_epoch_id, ls_id_dir_size);
  expected_disk_size += ls_id_dir_size;

  ob_usleep(2000*1000);
  int64_t start_calc_size_time_s = ObTimeUtility::current_time_s();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calc_meta_file_disk_space(start_calc_size_time_s, calibrate_res));
  // private_tablet_meta + tablet_id_dir
  ASSERT_EQ(expected_disk_size, calibrate_res.total_file_size_);

  // step 9: test delete file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(file_id));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(file_id, ls_epoch_id, is_exist));
  ASSERT_FALSE(is_exist);

  // step 10: test delete dir
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.delete_tablet_meta_tablet_id_transfer_seq_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id, meta_transfer_seq));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.delete_tablet_meta_tablet_id_dir(MTL_ID(), MTL_EPOCH_ID(), ls_id, ls_epoch_id, tablet_id));
  ob_usleep(2000*1000);
  start_calc_size_time_s = ObTimeUtility::current_time_s();
  get_ls_id_dir_size(ls_id, ls_epoch_id, ls_id_dir_size);
  calibrate_res.reset();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calc_meta_file_disk_space(start_calc_size_time_s, calibrate_res));
  ASSERT_EQ(ls_id_dir_size, calibrate_res.total_file_size_);
}

TEST_F(TestFileManager, test_list_private_tablet_meta)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  // step 0: construct MacroBlockId
  MacroBlockId meta_file_1;
  const int64_t ls_id = 6;
  const int64_t ls_epoch_id = 0;
  const int64_t tablet_id = 8;
  const int64_t meta_transfer_seq = 0;
  const int64_t meta_version_id_1 = 1;
  ObLinkedMacroBlockItemWriter tablet_meta_writer;
  ObSEArray<int64_t, ObBaseFileManager::OB_DEFAULT_ARRAY_CAPACITY> meta_vers_array;
  ObIArray<int64_t> &tablet_meta_vers = meta_vers_array;
  meta_file_1.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  meta_file_1.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_TABLET_META));
  meta_file_1.set_second_id(ls_id);
  meta_file_1.set_third_id(tablet_id);
  meta_file_1.set_meta_transfer_seq(meta_transfer_seq);
  meta_file_1.set_meta_version_id(meta_version_id_1);

  MacroBlockId meta_file_2;
  const int64_t meta_version_id_2 = 2;
  meta_file_2.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  meta_file_2.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_TABLET_META));
  meta_file_2.set_second_id(ls_id);
  meta_file_2.set_third_id(tablet_id);
  meta_file_2.set_meta_transfer_seq(meta_transfer_seq);
  meta_file_2.set_meta_version_id(meta_version_id_2);

  ObStorageObjectHandle write_object_handle_1;
  ASSERT_EQ(OB_SUCCESS, write_object_handle_1.set_macro_block_id(meta_file_1));
  ObStorageObjectHandle write_object_handle_2;
  ASSERT_EQ(OB_SUCCESS, write_object_handle_2.set_macro_block_id(meta_file_2));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();


  // step 1: test .tmp.seq private tablet meta
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle_1));
  ObPathContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.set_file_ctx(meta_file_1, ls_epoch_id, true/*is_local_cache*/));
  char tmp_seq_path[MAX_PATH_SIZE] = {0};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(tmp_seq_path, sizeof(tmp_seq_path), "%s.tmp.100", ctx.get_path()));
  ::rename(ctx.get_path(), tmp_seq_path); // rename filename to filename.tmp.seq
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_tablet_meta(ls_id, ls_epoch_id, tablet_id, meta_transfer_seq, tablet_meta_vers));
  ASSERT_EQ(0, tablet_meta_vers.count());
  ::rename(tmp_seq_path, ctx.get_path()); // rename filename.tmp.seq to filename
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(meta_file_1));
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(meta_file_1, ls_epoch_id, is_exist));
  ASSERT_FALSE(is_exist);


  // step 2: test private tablet meta exists only in local cache
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle_1));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_tablet_meta(ls_id, ls_epoch_id, tablet_id, meta_transfer_seq, tablet_meta_vers));
  ASSERT_EQ(1, tablet_meta_vers.count());
  ASSERT_EQ(meta_version_id_1, tablet_meta_vers.at(0));

  // step 3: test private tablet meta only exists in object storage
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(meta_file_1));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(meta_file_1, ls_epoch_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(meta_file_1, ls_epoch_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_TRUE(meta_file_2.is_macro_write_cache_ctrl_obj_type());
  ASSERT_FALSE(is_macro_write_cache_disabled());
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  ObSSLocalCacheControlMode control_mode;
  control_mode.set_micro_cache_mode(ObSSLocalCacheControlMode::MODE_OFF);
  control_mode.set_macro_write_cache_mode(ObSSLocalCacheControlMode::MODE_OFF);
  local_cache_service->set_local_cache_control_mode(control_mode);
  ASSERT_TRUE(local_cache_service->is_macro_write_cache_disabled());
  ASSERT_TRUE(is_macro_write_cache_disabled());
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle_2));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(meta_file_2, ls_epoch_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(meta_file_2, ls_epoch_id, is_exist));
  ASSERT_TRUE(is_exist);
  tablet_meta_vers.reset();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_tablet_meta(ls_id, ls_epoch_id, tablet_id, meta_transfer_seq, tablet_meta_vers));
  ASSERT_EQ(1, tablet_meta_vers.count());
  ASSERT_EQ(meta_version_id_2, tablet_meta_vers.at(0));

  // step 4: test private tablet meta exists in local cache and object storage
  tablet_meta_vers.reset();
  ASSERT_EQ(0, tablet_meta_vers.count());
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(meta_file_1, ls_epoch_id, is_exist));
  ASSERT_FALSE(is_exist);
  control_mode.set_micro_cache_mode(ObSSLocalCacheControlMode::MODE_ON);
  control_mode.set_macro_write_cache_mode(ObSSLocalCacheControlMode::MODE_ON);
  local_cache_service->set_local_cache_control_mode(control_mode);
  ASSERT_FALSE(local_cache_service->is_macro_write_cache_disabled());
  ASSERT_FALSE(is_macro_write_cache_disabled());
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle_1));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(meta_file_1, ls_epoch_id, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(meta_file_1, ls_epoch_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(meta_file_2, ls_epoch_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(meta_file_2, ls_epoch_id, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_tablet_meta(ls_id, ls_epoch_id, tablet_id, meta_transfer_seq, tablet_meta_vers));
  ASSERT_EQ(2, tablet_meta_vers.count());
  ASSERT_TRUE(
    tablet_meta_vers.at(0) == meta_version_id_1 ||
    tablet_meta_vers.at(0) == meta_version_id_2
  );
  ASSERT_TRUE(
    tablet_meta_vers.at(1) == meta_version_id_1 ||
    tablet_meta_vers.at(1) == meta_version_id_2
  );

  // step 5: test delete private tablet meta
  is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(meta_file_1));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(meta_file_1, ls_epoch_id, is_exist));
  ASSERT_FALSE(is_exist);
  is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(meta_file_2));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(meta_file_2, ls_epoch_id, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestFileManager, test_tenant_disk_space_meta)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);

  // step 1: write tmp_file
  MacroBlockId tmp_file;
  int64_t tmp_file_id = 4;
  int64_t segment_id = 5;
  tmp_file.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  tmp_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  tmp_file.set_second_id(tmp_file_id);
  tmp_file.set_third_id(segment_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tmp_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.set_tmp_file_valid_length(write_io_size);
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  int64_t expected_disk_size = write_io_size;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  expected_disk_size += statbuf.size_;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  ObSSMacroCacheStat tmp_file_cache_stat;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, tmp_file_cache_stat));
  // step 2: write tenant disk space meta
  ObTenantDiskSpaceMeta disk_space_meta(MTL_ID());
  ASSERT_EQ(OB_SUCCESS, disk_space_meta.body_.set_macro_cache_stats());
  ASSERT_EQ(OB_SUCCESS, disk_space_meta.header_.construct_header(disk_space_meta.body_));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->write_tenant_disk_space_meta(disk_space_meta));
  const uint8_t idx = static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE);
  ASSERT_EQ(tmp_file_cache_stat.used_, disk_space_meta.body_.macro_cache_stats_.at(idx).used_);
  // step 3: read tenant disk space meta
  disk_space_meta.reset();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->read_tenant_disk_space_meta(disk_space_meta));
  ASSERT_EQ(tmp_file_cache_stat.used_, disk_space_meta.body_.macro_cache_stats_.at(idx).used_);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(tmp_file));
}

TEST_F(TestFileManager, test_ss_format)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  ObBackupDest storage_dest;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_storage_dest(storage_dest));
  // step 1: is exist ss_format
  ASSERT_EQ(OB_SUCCESS, ObSSFormatUtil::is_exist_ss_format(storage_dest, is_exist));
  ASSERT_EQ(false, is_exist);
  // step 2: write ss_format
  ObSSFormat ss_format;
  const uint64_t cur_cluster_version = CLUSTER_CURRENT_VERSION;
  const int64_t cur_us = ObTimeUtility::fast_current_time();
  ss_format.body_.cluster_version_ = cur_cluster_version;
  ss_format.body_.create_timestamp_ = cur_us;
  ASSERT_EQ(OB_SUCCESS, ss_format.header_.construct_header(ss_format.body_));
  ASSERT_EQ(OB_SUCCESS, ObSSFormatUtil::write_ss_format(storage_dest, ss_format));
  // step 3: is exist ss_format
  ASSERT_EQ(OB_SUCCESS, ObSSFormatUtil::is_exist_ss_format(storage_dest, is_exist));
  ASSERT_EQ(true, is_exist);
  // step 4: read ss_format
  ss_format.reset();
  ASSERT_EQ(OB_SUCCESS, ObSSFormatUtil::read_ss_format(storage_dest, ss_format));
  ASSERT_EQ(cur_cluster_version, ss_format.body_.cluster_version_);
  ASSERT_EQ(cur_us, ss_format.body_.create_timestamp_);
}

TEST_F(TestFileManager, test_list_and_delete_dir_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  // test1: list tmp file
  MacroBlockId tmp_file;
  int64_t tmp_file_id = 3;
  int64_t segment_file_id = 2;
  tmp_file.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  tmp_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  tmp_file.set_second_id(tmp_file_id);
  tmp_file.set_third_id(segment_file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tmp_file));
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_tmp_file_valid_length(write_io_size);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  ObArray<MacroBlockId> tmp_files;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tmp_file(tmp_files));
  ASSERT_EQ(1, tmp_files.count());
  ASSERT_EQ(tmp_file_id, tmp_files.at(0).second_id());

  // test2: list private data macro
  MacroBlockId tablet_data_macro;
  int64_t tablet_id = 4;
  int64_t server_id = 5;
  int64_t seq_id = 6;
  tablet_data_macro.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  tablet_data_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  tablet_data_macro.set_second_id(tablet_id);
  tablet_data_macro.set_third_id(server_id);
  tablet_data_macro.set_macro_transfer_seq(0); // transfer_seq
  tablet_data_macro.set_tenant_seq(seq_id);  //tenant_seq
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tablet_data_macro));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  ObArray<MacroBlockId> tablet_data_macros;
  ObArray<int64_t> tablet_ids;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tablet_data_dir(tablet_ids));
  ASSERT_EQ(1, tablet_ids.count());
  ASSERT_EQ(tablet_id, tablet_ids.at(0));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_macro_file(tablet_ids.at(0), 0/*transfer_seq*/, tablet_data_macros));
  ASSERT_EQ(1, tablet_data_macros.count());
  ASSERT_EQ(tablet_data_macro.second_id(), tablet_data_macros.at(0).second_id());
  ASSERT_EQ(tablet_data_macro.third_id(), tablet_data_macros.at(0).third_id());
  ASSERT_EQ(tablet_data_macro.tenant_seq(), tablet_data_macros.at(0).tenant_seq());
  ASSERT_EQ(tablet_data_macro.macro_transfer_seq(), tablet_data_macros.at(0).macro_transfer_seq());

  // test3: list private meta macro
  MacroBlockId tablet_meta_macro;
  tablet_id = 4;
  server_id = 5;
  seq_id = 7;
  tablet_meta_macro.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  tablet_meta_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_META_MACRO));
  tablet_meta_macro.set_second_id(tablet_id);
  tablet_meta_macro.set_third_id(server_id);
  tablet_meta_macro.set_macro_transfer_seq(0); // transfer_seq
  tablet_meta_macro.set_tenant_seq(seq_id);  //tenant_seq
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tablet_meta_macro));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  ObArray<MacroBlockId> tablet_meta_macros;
  tablet_ids.reset();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tablet_data_dir(tablet_ids));
  ASSERT_EQ(1, tablet_ids.count());
  ASSERT_EQ(tablet_id, tablet_ids.at(0));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_private_macro_file(tablet_ids.at(0), 0/*transfer_seq*/, tablet_meta_macros));
  ASSERT_EQ(2, tablet_meta_macros.count());
  ASSERT_EQ(tablet_meta_macro.second_id(), tablet_meta_macros.at(1).second_id());
  ASSERT_EQ(tablet_meta_macro.third_id(), tablet_meta_macros.at(1).third_id());
  ASSERT_EQ(tablet_meta_macro.tenant_seq(), tablet_meta_macros.at(1).tenant_seq());
  ASSERT_EQ(tablet_meta_macro.macro_transfer_seq(), tablet_meta_macros.at(1).macro_transfer_seq());
  ASSERT_EQ(tablet_data_macro.second_id(), tablet_meta_macros.at(0).second_id());
  ASSERT_EQ(tablet_data_macro.third_id(), tablet_meta_macros.at(0).third_id());
  ASSERT_EQ(tablet_data_macro.tenant_seq(), tablet_meta_macros.at(0).tenant_seq());
  ASSERT_EQ(tablet_data_macro.macro_transfer_seq(), tablet_meta_macros.at(0).macro_transfer_seq());

  // test4: list private tablet meta version
  int64_t ls_id = 8;
  int64_t ls_epoch_id = 0;
  int64_t meta_version_id = 10;
  MacroBlockId tablet_meta_version;
  tablet_meta_version.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  tablet_meta_version.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_TABLET_META));
  tablet_meta_version.set_second_id(ls_id);
  tablet_meta_version.set_third_id(tablet_id);
  tablet_data_macro.set_meta_transfer_seq(0); // transfer_seq
  tablet_data_macro.set_meta_version_id(meta_version_id);  //tenant_seq
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tablet_meta_version));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));

  // test6: list shared tenant dir
  MacroBlockId tenant_root_key;
  tenant_root_key.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  tenant_root_key.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TENANT_ROOT_KEY));
  tenant_root_key.set_second_id(1);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tenant_root_key));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));

  ObArray<uint64_t> tenant_ids;
  uint64_t tenant_id = 1;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.list_shared_tenant_ids(MTL_ID(), tenant_ids));
  ASSERT_EQ(1, tenant_ids.count());
  ASSERT_EQ(tenant_id, tenant_ids.at(0));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(tenant_root_key));
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(tenant_root_key, 0, is_exist));
  ASSERT_EQ(false, is_exist);
  tenant_ids.reset();
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.list_shared_tenant_ids(MTL_ID(), tenant_ids));
  // NOTICE: if object_storage opens multi_version mode, here 'tenant_ids.count()=1'
  ASSERT_EQ(0, tenant_ids.count());

  // test7: list shared major data macro
  MacroBlockId shared_major_data_macro;
  tablet_id = 13;
  seq_id = 14;
  int64_t reorganization_scn = 0;
  shared_major_data_macro.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  shared_major_data_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_META_MACRO));
  shared_major_data_macro.set_second_id(tablet_id);
  shared_major_data_macro.set_third_id(seq_id);
  shared_major_data_macro.set_reorganization_scn(reorganization_scn);
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(shared_major_data_macro));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));

  // test8: delete shared tablet data dir
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(shared_major_data_macro, 0, is_exist));
  ASSERT_EQ(true, is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_shared_tablet_data_dir(tablet_id, reorganization_scn));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(shared_major_data_macro, 0, is_exist));
  ASSERT_EQ(false, is_exist);

  // test11: delete tmp file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_tmp_file(tmp_file));

  // test12: delete private macro files
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_files(tablet_meta_macros));

  // test13: delete ls dir file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_ls_dir(ls_id, ls_epoch_id));

}

TEST_F(TestFileManager, test_list_local_files_rec)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  // test1: list tmp file
  MacroBlockId tmp_file;
  int64_t tmp_file_id = 3;
  int64_t segment_file_id = 2;
  tmp_file.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  tmp_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  tmp_file.set_second_id(tmp_file_id);
  tmp_file.set_third_id(segment_file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tmp_file));
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_resource_group_id(0);
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_tmp_file_valid_length(write_io_size);
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  TestDirDelOp dir_del_op;
  TestFileDelOp file_del_op;
  char file_path[common::MAX_PATH_SIZE] = {0};
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_data_dir(file_path, sizeof(file_path), MTL_ID(), MTL_EPOCH_ID()));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_local_files_rec(file_path, file_del_op, dir_del_op));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(tmp_file, 0, is_exist));
  ASSERT_EQ(false, is_exist);
  ObArray<MacroBlockId> tmp_files;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tmp_file(tmp_files));
  ASSERT_EQ(0, tmp_files.count());

  // test2: list private tablet data macro
  MacroBlockId tablet_data_macro;
  int64_t tablet_id = 4;
  int64_t server_id = 5;
  int64_t seq_id = 6;
  tablet_data_macro.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  tablet_data_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_DATA_MACRO));
  tablet_data_macro.set_second_id(tablet_id);
  tablet_data_macro.set_third_id(server_id);
  tablet_data_macro.set_macro_transfer_seq(0); // transfer_seq
  tablet_data_macro.set_tenant_seq(seq_id);  //tenant_seq
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tablet_data_macro));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  MacroBlockId tablet_meta_macro;
  tablet_id = 4;
  server_id = 5;
  seq_id = 7;
  tablet_meta_macro.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  tablet_meta_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_META_MACRO));
  tablet_meta_macro.set_second_id(tablet_id);
  tablet_meta_macro.set_third_id(server_id);
  tablet_meta_macro.set_macro_transfer_seq(0); // transfer_seq
  tablet_meta_macro.set_tenant_seq(seq_id);  //tenant_seq
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tablet_meta_macro));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));

  file_path[0] = '\0';
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tablet_data_dir(file_path, sizeof(file_path), MTL_ID(), MTL_EPOCH_ID()));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_local_files_rec(file_path, file_del_op, dir_del_op));
  is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(tablet_data_macro, 0, is_exist));
  ASSERT_EQ(false, is_exist);
  is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(tablet_meta_macro, 0, is_exist));
  ASSERT_EQ(false, is_exist);

  ObArray<int64_t> tablet_ids;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->list_tablet_data_dir(tablet_ids));
  ASSERT_EQ(0, tablet_ids.count());
}

TEST_F(TestFileManager, test_delete_tmp_seq_files)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  int64_t seq_id = 100;
  char disk_space_meta_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(disk_space_meta_path, ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH, "%s/%lu_%ld/%s%s%ld",
                        OB_DIR_MGR.get_local_cache_root_dir(), MTL_ID(), MTL_EPOCH_ID(),
                        get_storage_objet_type_str(ObStorageObjectType::TENANT_DISK_SPACE_META), DEFAULT_TMP_STR, seq_id));
  ObIOFd fd;
  const int64_t write_io_size = 4096; // 4KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  int64_t written_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::open(disk_space_meta_path, O_RDWR | O_CREAT | O_TRUNC, S_IWUSR | S_IRUSR, fd));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::write(fd, write_buf, write_io_size, written_size));
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(disk_space_meta_path, statbuf));
  volatile bool is_stop = false;
  ObDirCalcSizeOp del_tmp_seq_file_op(is_stop);
  del_tmp_seq_file_op.set_for_del_tmp_seq_files();
  const int64_t del_security_time_s = 5;  // 5s
  ASSERT_EQ(OB_SUCCESS, del_tmp_seq_file_op.set_tmp_seq_file_del_secy_time_s(del_security_time_s));
  ob_usleep(2 * 1000 * 1000L); // sleep 2s
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.delete_tmp_seq_files(del_tmp_seq_file_op));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(disk_space_meta_path, statbuf));
  ob_usleep(5 * 1000 * 1000L); // sleep 5s
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.delete_tmp_seq_files(del_tmp_seq_file_op));
  ASSERT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY, ObIODeviceLocalFileOp::stat(disk_space_meta_path, statbuf));
}

TEST_F(TestFileManager, test_gc_local_major_data)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  MacroBlockId shared_data_macro;
  int64_t tablet_id = 100;
  int64_t seq_id = 101;
  shared_data_macro.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  shared_data_macro.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
  shared_data_macro.set_second_id(tablet_id);
  shared_data_macro.set_third_id(seq_id);

  ObPathContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.set_file_ctx(shared_data_macro, 0/*ls_epoch_id*/, true/*is_local_cache*/));
  ObIOFd fd;
  const int64_t write_io_size = 4096; // 4KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  int64_t written_size = 0;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::open(ctx.get_path(), O_RDWR | O_CREAT | O_TRUNC, S_IWUSR | S_IRUSR, fd));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::write(fd, write_buf, write_io_size, written_size));
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(ctx.get_path(), statbuf));
  ob_usleep(2 * 1000 * 1000L); // sleep 2s
  int64_t del_time_s = ObTimeUtility::current_time_s();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_local_major_data_dir(del_time_s));
  ASSERT_EQ(OB_NO_SUCH_FILE_OR_DIRECTORY, ObIODeviceLocalFileOp::stat(ctx.get_path(), statbuf));
}

TEST_F(TestFileManager, test_deleted_file_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);
  MacroBlockId tmp_file;
  int64_t tmp_file_id = 10;
  int64_t segment_file_id = 11;
  tmp_file.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  tmp_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::TMP_FILE));
  tmp_file.set_second_id(tmp_file_id);
  tmp_file.set_third_id(segment_file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(tmp_file));
  const int64_t write_io_size = 8 * 1024; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_resource_group_id(0);
  write_info.io_desc_.set_wait_event(1);
  write_info.io_desc_.set_unsealed();
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.set_tmp_file_valid_length(write_io_size);
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  // step 1: write tmp_file
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  int64_t expected_disk_size = write_io_size;
  char dir_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  ObIODFileStat statbuf;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(dir_path, sizeof(dir_path),
            MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::stat(dir_path, statbuf));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::append_file(write_info, write_object_handle));
  ObSSMacroCacheStat tmp_file_cache_stat;
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, tmp_file_cache_stat));
  int64_t tmp_file_used_size = tmp_file_cache_stat.used_;
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(tmp_file, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // step 2: delete tmp_file when pause_gc
  tenant_file_mgr->set_tmp_file_cache_pause_gc();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::delete_local_file(tmp_file));
  // old path has been rename, old path is not exist
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(tmp_file, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ObPathContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.set_logical_delete_ctx(tmp_file, 0/*ls_epoch_id*/));
  // old path has been rename, new path is exist
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(ctx.get_path(), is_exist));
  ASSERT_TRUE(is_exist);
  // delete tmp_file when pause_gc, rename file path, so tmp_file_write_cache_alloc_size does not changed
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, tmp_file_cache_stat));
  ASSERT_EQ(tmp_file_used_size, tmp_file_cache_stat.used_);

  // step 3: delete .deleted file
  tenant_file_mgr->set_tmp_file_cache_allow_gc();
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.rm_logical_deleted_file());
  // after calibrate, rm_logical_deleted_file, so renamed file path is not exist
  ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(ctx.get_path(), is_exist));
  ASSERT_FALSE(is_exist);
  // delete .deleted file, so tmp_file_write_cache_alloc_size reduce
  ASSERT_EQ(OB_SUCCESS, tenant_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::TMP_FILE, tmp_file_cache_stat));
  ASSERT_EQ(tmp_file_used_size - write_io_size, tmp_file_cache_stat.used_);

  // step 4: test rename not exist file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->logical_delete_local_tmp_file(tmp_file, true/*is_del_seg_meta*/));
}

TEST_F(TestFileManager, test_user_tenant_slog_io_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);

  // step 1: write slog_file
  MacroBlockId slog_file;
  int64_t file_id = 101;
  slog_file.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  slog_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_SLOG_FILE));
  slog_file.set_second_id(MTL_ID());
  slog_file.set_third_id(MTL_EPOCH_ID());
  slog_file.set_fourth_id(file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(slog_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  write_info.offset_ = write_io_size;
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(slog_file));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  int64_t slog_min_id = -1;
  int64_t slog_max_id = -1;
  int64_t used_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(MTL_ID(), MTL_EPOCH_ID(), slog_min_id, slog_max_id));
  ASSERT_EQ(file_id, slog_min_id);
  ASSERT_EQ(file_id, slog_max_id);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_used_size(MTL_ID(), MTL_EPOCH_ID(), used_size));
  ASSERT_EQ(write_io_size * 2, used_size);
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  // step 2: seal slog_file
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.seal_object(slog_file, 0/*ls_epoch_id*/));
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  // wait flush success
  ObSSMacroCacheFlushTask &flush_task = macro_cache_mgr->flush_task_;
  ob_usleep(1 * 1000L * 1000L);
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while (tenant_file_mgr->flushed_slog_seq_ < file_id) {
    ob_usleep(10 * 1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wait time is too long", KR(ret), K(tenant_file_mgr->flushed_slog_seq_));
      break;
    }
  }
  // flush success
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  // step 3: read slog_file
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = slog_file;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::read_object(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));

  // step 4: list slog dir and get used size
  slog_min_id = -1;
  slog_max_id = -1;
  used_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(MTL_ID(), MTL_EPOCH_ID(), slog_min_id, slog_max_id));
  ASSERT_EQ(file_id, slog_min_id);
  ASSERT_EQ(file_id, slog_max_id);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_used_size(MTL_ID(), MTL_EPOCH_ID(), used_size));
  ASSERT_EQ(write_io_size * 2, used_size);

  // step 5: delete slog_file
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(slog_file));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestFileManager, test_get_slog_id_range)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);
  const int64_t min_file_id = 1;

  // test 1: test slog dir is empty
  int64_t slog_min_id = -1;
  int64_t slog_max_id = -1;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
  ASSERT_EQ(min_file_id, slog_min_id);
  ASSERT_EQ(min_file_id, slog_max_id);

  // step 2: test slog file exists only in local cache
  MacroBlockId slog_file;
  int64_t file_id = 104;
  slog_file.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  slog_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_SLOG_FILE));
  slog_file.set_second_id(OB_SERVER_TENANT_ID);
  slog_file.set_third_id(0/*epoch_id*/);
  slog_file.set_fourth_id(file_id);
  MacroBlockId slog_file2(slog_file);
  int64_t file_id2 = 105;
  slog_file2.set_fourth_id(file_id2);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(slog_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  slog_min_id = -1;
  slog_max_id = -1;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
  ASSERT_EQ(min_file_id, slog_min_id);
  ASSERT_EQ(file_id, slog_max_id);

  // step 3: test slog file exists only in object storage
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.push_to_flush_map(slog_file));
  // wait flush success
  ob_usleep(1 * 1000L * 1000L);
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while (OB_SERVER_FILE_MGR.flushed_slog_seq_ < file_id) {
    ob_usleep(10 * 1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wait time is too long", KR(ret), K(OB_SERVER_FILE_MGR.flushed_slog_seq_));
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  slog_min_id = -1;
  slog_max_id = -1;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
  ASSERT_EQ(min_file_id, slog_min_id);
  ASSERT_EQ(file_id, slog_max_id);

  // step 4: test slog file exists in local cache and object storage
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  slog_min_id = -1;
  slog_max_id = -1;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
  ASSERT_EQ(min_file_id, slog_min_id);
  ASSERT_EQ(file_id, slog_max_id);

  // step 5: test two slog files
  write_object_handle.reset();
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(slog_file2));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  slog_min_id = -1;
  slog_max_id = -1;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
  ASSERT_EQ(min_file_id, slog_min_id);
  ASSERT_EQ(file_id2, slog_max_id);

  // step 6: delete slog_file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(slog_file, 0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_file(slog_file));
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_file(slog_file2));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(slog_file, 0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
}

TEST_F(TestFileManager, test_server_tenant_slog_io_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);
  const int64_t min_file_id = 1;

  // step 1: write slog_file
  MacroBlockId slog_file;
  int64_t file_id = 106;
  slog_file.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  slog_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_SLOG_FILE));
  slog_file.set_second_id(OB_SERVER_TENANT_ID);
  slog_file.set_third_id(0/*epoch_id*/);
  slog_file.set_fourth_id(file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(slog_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
  int64_t old_used_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_used_size(OB_SERVER_TENANT_ID, 0, old_used_size));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  // step 2: seal slog_file
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.push_to_flush_map(slog_file));
  // wait flush success
  ob_usleep(1 * 1000L * 1000L);
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while (OB_SERVER_FILE_MGR.flushed_slog_seq_ < file_id) {
    ob_usleep(10 * 1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wait time is too long", KR(ret), K(OB_SERVER_FILE_MGR.flushed_slog_seq_));
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  // step 3: read slog_file
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = slog_file;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, ObObjectManager::read_object(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));
  // step 4: list slog dir and get used size
  int64_t slog_min_id = -1;
  int64_t slog_max_id = -1;
  int64_t used_size = 0;
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_id_range(OB_SERVER_TENANT_ID, 0, slog_min_id, slog_max_id));
  ASSERT_EQ(min_file_id, slog_min_id);
  ASSERT_EQ(file_id, slog_max_id);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.get_slog_used_size(OB_SERVER_TENANT_ID, 0, used_size));
  ASSERT_EQ(old_used_size + write_io_size, used_size);
  // step 5: delete slog_file
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_file(slog_file));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestFileManager, test_slog_data_out_of_range)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  // step 1: write slog_file
  MacroBlockId slog_file;
  int64_t file_id = 107;
  slog_file.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  slog_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_SLOG_FILE));
  slog_file.set_second_id(MTL_ID());
  slog_file.set_third_id(MTL_EPOCH_ID());
  slog_file.set_fourth_id(file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(slog_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  write_object_handle.reset();
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // step 2: read local slog_file out of range
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = slog_file;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();

  // (offset + size) > file_len
  read_info.offset_ = write_io_size / 2;
  read_info.size_ = write_io_size;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, ObObjectManager::read_object(read_info, read_object_handle));
  read_object_handle.reset();

  // offset > file_len
  read_info.offset_ = write_io_size + 4096;
  read_info.size_ = write_io_size;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, ObObjectManager::read_object(read_info, read_object_handle));
  read_object_handle.reset();

  // offset = file_len
  read_info.offset_ = write_io_size;
  read_info.size_ = write_io_size;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, ObObjectManager::read_object(read_info, read_object_handle));
  read_object_handle.reset();

  // step 3: seal slog_file
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.seal_object(slog_file, 0/*ls_epoch_id*/));
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  // wait flush success
  ObSSMacroCacheFlushTask &flush_task = macro_cache_mgr->flush_task_;
  ob_usleep(1 * 1000L * 1000L);
  const int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L;
  while (tenant_file_mgr->flushed_slog_seq_ < file_id) {
    ob_usleep(10 * 1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wait time is too long", KR(ret), K(tenant_file_mgr->flushed_slog_seq_));
      break;
    }
  }
  // flush success
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);

  // step 4: read remote slog_file out of range
  // (offset + size) > file_len
  read_info.offset_ = write_io_size / 2;
  read_info.size_ = write_io_size;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, ObObjectManager::read_object(read_info, read_object_handle));
  read_object_handle.reset();

  // offset > file_len
  read_info.offset_ = write_io_size + 4096;
  read_info.size_ = write_io_size;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, ObObjectManager::read_object(read_info, read_object_handle));
  read_object_handle.reset();

  // offset = file_len
  read_info.offset_ = write_io_size;
  read_info.size_ = write_io_size;
  ASSERT_EQ(OB_DATA_OUT_OF_RANGE, ObObjectManager::read_object(read_info, read_object_handle));
  read_object_handle.reset();

  // step 5: delete slog_file
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(slog_file));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(slog_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestFileManager, test_user_tenant_ckpt_io_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);

  // step 1: write ckpt_file
  MacroBlockId ckpt_file;
  int64_t file_id = 101;
  ckpt_file.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  ckpt_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_CKPT_FILE));
  ckpt_file.set_second_id(MTL_ID());
  ckpt_file.set_third_id(MTL_EPOCH_ID());
  ckpt_file.set_fourth_id(file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(ckpt_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  // step 2: read ckpt_file
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = ckpt_file;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));
  // step 3: delete ckpt_file
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(ckpt_file));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(ckpt_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestFileManager, test_server_tenant_ckpt_io_operator)
{
  int ret = OB_SUCCESS;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  ObTenantDiskSpaceManager *tenant_disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, tenant_disk_space_mgr);

  // step 1: write ckpt_file
  MacroBlockId ckpt_file;
  int64_t file_id = 102;
  ckpt_file.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
  ckpt_file.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::PRIVATE_CKPT_FILE));
  ckpt_file.set_second_id(OB_SERVER_TENANT_ID);
  ckpt_file.set_third_id(MTL_EPOCH_ID());
  ckpt_file.set_fourth_id(file_id);
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(ckpt_file));
  const int64_t write_io_size = 4096 * 2; // 8KB
  char write_buf[write_io_size] = { 0 };
  memset(write_buf, 'a', write_io_size);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = write_io_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::write_file(write_info, write_object_handle));
  // step 2: read ckpt_file
  ObStorageObjectHandle read_object_handle;
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = ckpt_file;
  read_info.io_desc_.set_wait_event(1);
  char read_buf[write_io_size] = { 0 };
  read_info.buf_ = read_buf;
  read_info.offset_ = 0;
  read_info.size_ = write_io_size;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = OB_SERVER_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::pread_file(read_info, read_object_handle));
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf, read_object_handle.get_buffer(), write_io_size));
  // step 3: delete ckpt_file
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(ckpt_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_file(ckpt_file, 0/*ls_epoch_id*/));
  ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_file(ckpt_file,0/*ls_epoch_id*/, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestFileManager, test_delete_tenant)
{
  // delete local tenant dir file
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_local_tenant_dir(MTL_ID(), MTL_EPOCH_ID()));
  // delete remote tenant dir file
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_FILE_MGR.delete_remote_tenant_dir(MTL_ID(), MTL_EPOCH_ID()));
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_file_manager.log*");
  OB_LOGGER.set_file_name("test_file_manager.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
