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
#include "lib/restore/ob_storage_info.h"

using namespace oceanbase::common;

TEST(ObObjectStorageInfo, file)
{
  const char *uri = "file:///backup_dir/?&delete_mode=tagging";
  ObObjectStorageInfo info1;
  ASSERT_FALSE(info1.is_valid());

  // wrong storage info
  const char *storage_info = NULL;
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "access_key=xxx";    // nfs, endpoint/access_id/access_key should be empty
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "access_key=xxx&encrypt_key=xxx";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "delete_mode=wrong_mode";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "delete_mode=delete";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));

  ObObjectStorageInfo info2;
  ObObjectStorageInfo info3;
  storage_info = "";
  ObStorageType device_type = OB_STORAGE_FILE;
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
  ASSERT_EQ(OB_SUCCESS, info2.set(device_type, storage_info));
  ASSERT_EQ(OB_SUCCESS, info3.assign(info1));

  ASSERT_EQ(info1, info2);
  ASSERT_EQ(info1, info3);

  // get
  ASSERT_EQ(device_type, info1.get_type());

  char buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  ASSERT_EQ(OB_SUCCESS, info1.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ("", buf);
  ASSERT_EQ(OB_SUCCESS, info1.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ("", buf);

  // clean
  ASSERT_TRUE(info1.is_valid());
  info1.reset();
  ASSERT_FALSE(info1.is_valid());
}

TEST(ObObjectStorageInfo, oss)
{
  const char *uri = "oss://backup_dir?host=xxx.com&access_id=111&access_key=222";
  ObObjectStorageInfo info1;

  const char *storage_info = "";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "access_id=111&access_key=222";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));

  storage_info = "host=xxx.com&access_id=111&access_key=222";
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));

  char buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  ASSERT_EQ(OB_SUCCESS, info1.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ(storage_info, buf);

  storage_info = "host=xxx.com&access_id=111&access_key=222&delete_mode=delete";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
}

TEST(ObObjectStorageInfo, cos)
{
  const char *uri = "cos://backup_dir?host=xxx.com&access_id=111&access_key=222&appid=333";
  ObObjectStorageInfo info1;

  const char *storage_info = "";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));
  storage_info = "host=xxx.com&access_id=111&access_key=222";
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, info1.set(uri, storage_info));

  storage_info = "host=xxx.com&access_id=111&access_key=222&appid=333";
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));

  char buf[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  ASSERT_EQ(OB_SUCCESS, info1.get_storage_info_str(buf, sizeof(buf)));
  ASSERT_STREQ(storage_info, buf);

  storage_info = "host=xxx.com&access_id=111&access_key=222&appid=333&delete_mode=delete";
  info1.reset();
  ASSERT_EQ(OB_SUCCESS, info1.set(uri, storage_info));
}

int main(int argc, char **argv)
{
  system("rm -f test_storage_info.log*");
  OB_LOGGER.set_file_name("test_storage_info.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}