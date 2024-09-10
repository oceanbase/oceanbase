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

#include "lib/allocator/ob_malloc.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_struct.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_device_manager.h"
#include "logservice/archiveservice/ob_archive_io.h"
#include <cstdlib>
#include <gtest/gtest.h>
#include <string>

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace archive;
using namespace std;

TEST(TestObArchiveOverwrite, oss_push_log)
{
  int64_t rand_num = rand();
  ObBackupDest dest;
  ObBackupIoAdapter util;
  ObArchiveIO archive_io;
  const int64_t part_size = 5 * 1024 * 1024L;
  const int64_t total_size = 10 * 1024 * 1024L;
  const uint64_t test_memory = 6L * 1024L * 1024L * 1024L;
  ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
  EXPECT_EQ(0, ObIOManager::get_instance().init(test_memory));
  char *data = (char*)ob_malloc(total_size, "ArchiveTest");
  string path = "oss://antsys-oceanbasebackup/test_archive/test_archive_overwrite/" + to_string(rand_num) + "?host=cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com&access_id=LTAI4Fdwx9iFgZso4CqyHPs7&" + "access_" + "key=ER51kMn" + "lmu3zXwcxczJ" + "MbYzJIgrY9O";

  EXPECT_EQ(false, NULL == data);

  EXPECT_EQ(0, dest.set(path.c_str()));

  const ObString &uri = dest.get_root_path();
  const ObBackupStorageInfo *storage_info = dest.get_storage_info();

  // delete file
  EXPECT_EQ(0, util.del_file(uri, storage_info));

  // scene 1: append & append
  EXPECT_EQ(0, archive_io.push_log(uri, storage_info, data, part_size, 0, false /*append*/));
  EXPECT_EQ(0, archive_io.push_log(uri, storage_info, data, total_size, 0, false /*append*/));
  // delete file
  EXPECT_EQ(0, util.del_file(uri, storage_info));

  // scene 2: append & put
  EXPECT_EQ(0, archive_io.push_log(uri, storage_info, data, part_size, 0, false /*append*/));
  EXPECT_EQ(0, archive_io.push_log(uri, storage_info, data, total_size, 0, true /*put*/));
  // delete file
  EXPECT_EQ(0, util.del_file(uri, storage_info));

  // scene 3: put & put
  EXPECT_EQ(0, archive_io.push_log(uri, storage_info, data, total_size, 0, true /*put*/));
  EXPECT_EQ(0, archive_io.push_log(uri, storage_info, data, total_size, 0, true /*put*/));
  // delete file
  EXPECT_EQ(0, util.del_file(uri, storage_info));

  // scene 4: put & append
  EXPECT_EQ(0, archive_io.push_log(uri, storage_info, data, total_size, 0, true /*put*/));
  EXPECT_EQ(0, archive_io.push_log(uri, storage_info, data, part_size, 0, false /*append*/));
  // delete file
  EXPECT_EQ(0, util.del_file(uri, storage_info));

  if (NULL != data) {
    ob_free(data);
    data = NULL;
  }
}

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_file_name("test_archive_overwrite.log", true);
  ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
