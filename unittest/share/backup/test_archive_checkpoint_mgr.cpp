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
#define private public
#include "lib/restore/ob_storage.h"
#include "lib/restore/ob_storage_oss_base.cpp"
#undef private

#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_struct.h"
#define private public
#include "share/backup/ob_archive_checkpoint_mgr.h"
#undef private
#include "test_ob_backup_dest_config.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_force_print_log.h"
#include "share/backup/ob_backup_path.h"
#include "share/ob_thread_pool.h"
#include "apr_allocator.h"


using namespace oceanbase::common;
using namespace oceanbase::share;

const ObStorageType TEST_STORAGE_TYPES[] = {
    OB_STORAGE_FILE,
    OB_STORAGE_OSS,
};

class TestArchiveCheckpointMgr: public ::testing::Test
{
public:
  TestArchiveCheckpointMgr() {}
  virtual ~TestArchiveCheckpointMgr() {}
  virtual void SetUp()
  {
  }
  virtual void TearDown()
  {
  }

  static void SetUpTestCase()
  {
  }

  static void TearDownTestCase()
  {
  }

  // function members
  static int get_root_path(const ObStorageType &type, ObBackupPath &root_path);
  static int get_storage_info(const ObStorageType &type, ObString &storage_info);
  int clean_root_dir(const ObStorageType &type);
  int clean_dir(const ObStorageType &type, const ObString &dir_uri);
  int generate_simple_files(const ObStorageType &type);

  // test cases
  typedef int (TestArchiveCheckpointMgr::*TEST_FUNCTION) (const ObStorageType &type);
  int run_test_func(TEST_FUNCTION function);
  int test_write_and_read_checkpoint(const ObStorageType &type);

  ObBackupStorageInfo storage_info_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestArchiveCheckpointMgr);
};

int TestArchiveCheckpointMgr::run_test_func(TEST_FUNCTION f)
{
  int ret = OB_SUCCESS;
  const int64_t type_count = sizeof(TEST_STORAGE_TYPES) / sizeof(ObStorageType);
  ObBackupPath path;
  ObString storage_info;
  for (int64_t i = 0; OB_SUCC(ret) && i < type_count; ++i) {
    const ObStorageType type = TEST_STORAGE_TYPES[i];
    const int64_t start_ts = ObTimeUtility::current_time();
    storage_info_.reset();
    FLOG_INFO("start run_test_func", K(type));
    if (OB_FAIL(get_root_path(type, path))) {
      LOG_WARN("failed to get root patch", K(ret), K(type));
    } else if (path.is_empty()) {
      LOG_INFO("path is not set, skip unittest", K(type));
    } else if (OB_FAIL(get_storage_info(type, storage_info))) {
      LOG_WARN("failed to get storage info", K(ret), K(type));
    } else if (OB_FAIL(storage_info_.set(type, storage_info.ptr()))) {
      LOG_INFO("path is not set, skip unittest", K(type));
    } else if (OB_FAIL(clean_root_dir(type))) {
      LOG_WARN("failed to clean root dir", K(ret), K(type));
    } else if (OB_FAIL((this->*f)(type))) {
      LOG_WARN("failed to to test function", K(ret), K(type));
    } else if (OB_FAIL(clean_root_dir(type))) {
      LOG_WARN("failed to clean root dir", K(ret), K(type));
    }
    const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    FLOG_INFO("end run_test_func", K(ret), K(type), K(cost_ts));
  }

  return ret;
}

int TestArchiveCheckpointMgr::get_root_path(const ObStorageType &type, ObBackupPath &root_path)
{
  int ret = OB_SUCCESS;
  root_path.reset();

  if (OB_STORAGE_OSS == type) {
    if (OB_FAIL(root_path.init(oss_root_path))) {
      LOG_WARN("failed to init oss root path", K(ret));
    }
  } else if (OB_STORAGE_FILE == type) {
    if (OB_FAIL(root_path.init(file_root_path))) {
      LOG_WARN("failed to init file root path", K(ret));
    }
  } else {
    ret = OB_ERROR;
    LOG_ERROR("unknown type", K(type));
  }
  return ret;
}

int TestArchiveCheckpointMgr::get_storage_info(const ObStorageType &type, ObString &storage_info)
{
  int ret = OB_SUCCESS;
  storage_info.reset();
  if (OB_STORAGE_OSS == type) {
    storage_info = ObString(oss_storage_info);
  } else if (OB_STORAGE_FILE == type) {
    storage_info = ObString(file_storage_info);
  } else {
    ret = OB_ERROR;
    LOG_ERROR("unknown type", K(type));
  }
  return ret;
}

int TestArchiveCheckpointMgr::generate_simple_files(const ObStorageType &type)
{
  int ret = OB_SUCCESS;
  ObBackupPath root_path;
  ObBackupPath tmp_root_path;
  ObBackupIoAdapter util;
  char buf[] = "test";
  int64_t start_ts = ObTimeUtility::current_time();

  LOG_INFO("start generate_simple_files", K(type));
  if (OB_FAIL(get_root_path(type, root_path))) {
    LOG_WARN("failed to get root path", K(ret), K(root_path));
  }

  //├── test_checkpoint
  //│   ├── file1
  //│   ├── file2
  //│   ├── file3

  for (int64_t i = 1; OB_SUCC(ret) && i <= 4; ++i) {
    tmp_root_path.reset();
    tmp_root_path = root_path;
    if (OB_FAIL(tmp_root_path.join_checkpoint_info_file(OB_STR_CHECKPOINT_FILE_NAME, i, ObBackupFileSuffix::ARCHIVE))) {
      LOG_WARN("failed to join path", K(ret));
    } else if (OB_FAIL(util.mk_parent_dir(tmp_root_path.get_ptr(), &storage_info_))) {
      LOG_WARN("failed to mk dir", K(ret));
    } else if (OB_FAIL(util.write_single_file(tmp_root_path.get_ptr(), &storage_info_, buf, sizeof(buf)))) {
      LOG_WARN("failed to write dir less", K(ret));
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("finish prepare simple dir", K(type), K(cost_ts), K(ret));
  return ret;
}

int TestArchiveCheckpointMgr::clean_root_dir(const ObStorageType &type)
{
  int ret = OB_SUCCESS;
  ObBackupPath root_path;
  if (OB_FAIL(get_root_path(type, root_path))) {
    LOG_WARN("failed to get root path", K(ret), K(root_path));
  } else if (OB_FAIL(clean_dir(type, root_path.get_ptr()))) {
    LOG_WARN("failed to clean dir", K(ret), K(type), K(root_path));
  }
  return ret;
}

int TestArchiveCheckpointMgr::clean_dir(const ObStorageType &type, const ObString &dir_uri)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  char path[OB_MAX_BACKUP_PATH_LENGTH] = "";
  ObArray <ObString> file_names;
  ObArenaAllocator allocator;
  char uri_del[OB_MAX_URI_LENGTH];
  ObFileListArrayOp op(file_names, allocator);

  if (OB_FAIL(util.list_files(dir_uri, &storage_info_, op))) {
    LOG_WARN("failed to list files", K(ret), K(dir_uri));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_names.count(); ++i) {
      const ObString file = file_names.at(i);
      if (OB_FAIL(databuff_printf(path, sizeof(path), "%.*s/%.*s",
          dir_uri.length(), dir_uri.ptr(), file.length(), file.ptr()))) {
        LOG_WARN("failed to join path", K(ret));
      } else if (OB_FAIL(util.del_file(path, &storage_info_))) {
        LOG_WARN("failed to clean dir", K(ret), K(type), K(path));
      }
      LOG_INFO("del_file", K(path));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(util.del_dir(dir_uri, &storage_info_))) {
      LOG_WARN("failed to del dir", K(ret), K(dir_uri));
    }
    LOG_INFO("del_dir",  K(dir_uri));
  }

  return ret;
};

int TestArchiveCheckpointMgr::test_write_and_read_checkpoint(const ObStorageType &type)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  ObBackupPath root_path;
  ObBackupPath tmp_root_path;
  ObBackupIoAdapter util;
  if (OB_FAIL(get_root_path(type, root_path))) {
    LOG_WARN("failed to get root path", K(ret), K(root_path));
  } else {
    ObArchiveCheckpointMgr mgr;
    bool is_exist = true;
    uint64_t checkpoint = 0;
    if (OB_FAIL(mgr.init(root_path, OB_STR_CHECKPOINT_FILE_NAME, ObBackupFileSuffix::ARCHIVE, &storage_info_))) {
      LOG_WARN("failed to init checkpoint mgr", K(ret), K(root_path));
    } else if (OB_FAIL(mgr.write(10))) {
      LOG_WARN("failed to write files", K(ret), K(root_path));
    } else if (OB_FAIL(mgr.write(9))) {
      LOG_WARN("failed to write files", K(ret), K(root_path));
    } else if (OB_FAIL(mgr.read(checkpoint))) {
      LOG_WARN("failed to read files", K(ret));
    } else if (checkpoint != 10) {
      ret = OB_ERROR;
    } else if (OB_FAIL(mgr.write(100))) {
      LOG_WARN("failed to write files", K(ret), K(root_path));
    } else if (OB_FAIL(mgr.write(50))) {
      LOG_WARN("failed to write files", K(ret), K(root_path));
    } else if (OB_FAIL(mgr.read(checkpoint))) {
      LOG_WARN("failed to read files", K(ret));
    } else if (checkpoint != 100) {
      ret = OB_ERROR;
    } else {
      tmp_root_path.reset();
      tmp_root_path = root_path;
      if (OB_FAIL(tmp_root_path.join_checkpoint_info_file(OB_STR_CHECKPOINT_FILE_NAME, 10, ObBackupFileSuffix::ARCHIVE))) {
        LOG_WARN("failed to join path", K(ret));
      } else if (OB_FAIL(util.is_exist(tmp_root_path.get_ptr(), &storage_info_, is_exist))) {
        LOG_WARN("failed to judge file exist", K(ret), K(tmp_root_path));
      } else if (is_exist) {
        ret = OB_ERROR;
      }
      tmp_root_path.reset();
      tmp_root_path = root_path;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tmp_root_path.join_checkpoint_info_file(OB_STR_CHECKPOINT_FILE_NAME, 9, ObBackupFileSuffix::ARCHIVE))) {
          LOG_WARN("failed to join path", K(ret));
        } else if (OB_FAIL(util.is_exist(tmp_root_path.get_ptr(), &storage_info_, is_exist))) {
          LOG_WARN("failed to judge file exist", K(ret), K(tmp_root_path));
        } else if (is_exist) {
          ret = OB_ERROR;
        }
      }
      tmp_root_path.reset();
      tmp_root_path = root_path;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(tmp_root_path.join_checkpoint_info_file(OB_STR_CHECKPOINT_FILE_NAME, 50, ObBackupFileSuffix::ARCHIVE))) {
          LOG_WARN("failed to join path", K(ret));
        } else if (OB_FAIL(util.is_exist(tmp_root_path.get_ptr(), &storage_info_, is_exist))) {
          LOG_WARN("failed to judge file exist", K(ret), K(tmp_root_path));
        } else if (!is_exist) {
          ret = OB_ERROR;
        }
      }
    }
  }

  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("finish test_list_util", K(type), K(cost_ts), K(ret));
  return ret;
}

TEST_F(TestArchiveCheckpointMgr, test_write_read_op)
{
  TEST_FUNCTION f = &TestArchiveCheckpointMgr::test_write_and_read_checkpoint;
  int ret = run_test_func(f);
  ASSERT_EQ(OB_SUCCESS, ret);
}

int main(int argc, char **argv)
{
  system("rm -f test_archive_checkpoint_mgr.log");
  OB_LOGGER.set_file_name("test_archive_checkpoint_mgr.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
