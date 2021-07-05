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

#define protected public
#include "lib/restore/ob_storage.h"

#ifdef _WITH_OSS
#include "lib/restore/ob_storage_oss_base.cpp"
#endif

#undef protected

#include "lib/profile/ob_trace_id.h"
#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "lib/thread/ob_dynamic_thread_pool.h"

using namespace oceanbase::common;
class TestStorageFile : public ::testing::Test {
public:
  TestStorageFile()
  {}
  virtual ~TestStorageFile()
  {}
  virtual void SetUp()
  {
    ASSERT_EQ(OB_SUCCESS, databuff_printf(test_dir_, sizeof(test_dir_), "%s/test_storage", get_current_dir_name()));
    ASSERT_EQ(OB_SUCCESS, databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_));
    STORAGE_LOG(INFO, "clean test_storageg dir");
    ASSERT_EQ(0, ::system("rm -fr test_storage"));
  }
  virtual void TearDown()
  {
    STORAGE_LOG(INFO, "clean test_storageg dir");
    ASSERT_EQ(0, ::system("rm -fr test_storage"));
  }

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestStorageFile);

protected:
  // function members
protected:
  char test_dir_[OB_MAX_URI_LENGTH];
  char test_dir_uri_[OB_MAX_URI_LENGTH];
};

TEST_F(TestStorageFile, test_util)
{
  ObStorageUtil util(0 /*0s*/, 0 /*0s*/);
  char uri[OB_MAX_URI_LENGTH];
  const ObString storage_info;
  bool is_exist = false;
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  char read_buf[OB_MAX_URI_LENGTH];
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, storage_info));

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "file://%s/test_file", test_dir_));
  ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, storage_info, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info, test_content, strlen(test_content)));
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, storage_info, read_size));
  ASSERT_EQ(strlen(test_content), read_size);
  ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, storage_info, is_exist));
  ASSERT_TRUE(is_exist);

  ASSERT_EQ(OB_SUCCESS, util.del_file(uri, storage_info));
  ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, storage_info, is_exist));
  ASSERT_FALSE(is_exist);

  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info, test_content, strlen(test_content)));
  ASSERT_EQ(OB_SUCCESS, util.read_single_file(uri, storage_info, read_buf, sizeof(read_buf), read_size));
  ASSERT_EQ(strlen(test_content), read_size);
  ASSERT_EQ(0, strncmp(test_content, read_buf, read_size));
  ASSERT_EQ(OB_SUCCESS, util.read_single_text_file(uri, storage_info, read_buf, sizeof(read_buf)));
  ASSERT_EQ(strlen(test_content), strlen(read_buf));
  ASSERT_EQ(0, strcmp(test_content, read_buf));

  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.is_exist("bad://", storage_info, is_exist));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.get_file_length("bad://", storage_info, read_size));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.del_file("bad://", storage_info));
  ASSERT_EQ(
      OB_INVALID_BACKUP_DEST, util.read_single_file("bad://", storage_info, read_buf, sizeof(read_buf), read_size));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.read_single_text_file("bad://", storage_info, read_buf, sizeof(read_buf)));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.write_single_file("bad://", storage_info, test_content, strlen(test_content)));
}

TEST_F(TestStorageFile, test_util_retry)
{
  const int64_t max_retry_duration = 5 * 1000 * 1000 /*total 5s*/;
  ObStorageUtil util(max_retry_duration, 1 * 1000 * 1000 /*sleep 1s*/);
  const ObString storage_info;
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  char read_buf[OB_MAX_URI_LENGTH];
  int64_t read_size = 0;
  char error_uri[OB_MAX_URI_LENGTH];
  ASSERT_EQ(OB_SUCCESS, databuff_printf(error_uri, sizeof(error_uri), "file://%s/test_file/", test_dir_));
  ASSERT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, storage_info));

  int64_t start_ts1 = ObTimeUtil::current_time();
  ASSERT_EQ(
      OB_INVALID_BACKUP_DEST, util.read_single_file("bad://", storage_info, read_buf, sizeof(read_buf), read_size));
  int64_t cost_ts1 = ObTimeUtil::current_time() - start_ts1;
  ASSERT_GT(cost_ts1, max_retry_duration);

  int64_t start_ts2 = ObTimeUtil::current_time();
  ASSERT_EQ(OB_IO_ERROR, util.write_single_file(error_uri, storage_info, test_content, strlen(test_content)));
  int64_t cost_ts2 = ObTimeUtil::current_time() - start_ts2;
  ASSERT_GT(cost_ts2, max_retry_duration);
}

TEST_F(TestStorageFile, test_reader)
{
  ObStorageUtil util(0 /*0s*/, 0 /*0s*/);
  ObStorageReader reader;
  ObStorageReader reader2;
  char uri[OB_MAX_URI_LENGTH];
  const ObString storage_info;
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  char read_buf[OB_MAX_URI_LENGTH];
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, storage_info));

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "file://%s/test_file", test_dir_));

  ASSERT_EQ(OB_NOT_INIT, reader.pread(read_buf, sizeof(read_buf), 0, read_size));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, reader.open("bad://", storage_info));
  ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, reader.open(uri, storage_info));

  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info, test_content, strlen(test_content)));
  ASSERT_EQ(OB_SUCCESS, reader.open(uri, storage_info));
  ASSERT_EQ(OB_INIT_TWICE, reader.open(uri, storage_info));
  ASSERT_EQ(OB_NOT_INIT, reader.pread(read_buf, sizeof(read_buf), 0, read_size));

  ASSERT_EQ(OB_SUCCESS, reader2.open(uri, storage_info));
  ASSERT_EQ(OB_SUCCESS, reader2.pread(read_buf, sizeof(read_buf), 0, read_size));
  ASSERT_EQ(strlen(test_content), read_size);
  ASSERT_EQ(0, strncmp(test_content, read_buf, read_size));
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader2.pread(read_buf, sizeof(read_buf), read_size + 1, read_size));
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader2.pread(read_buf, sizeof(read_buf), -1, read_size));
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader2.pread(NULL, sizeof(read_buf), 0, read_size));
  ASSERT_EQ(OB_INVALID_ARGUMENT, reader2.pread(read_buf, 0, 0, read_size));
  ASSERT_EQ(OB_SUCCESS, reader2.close());

  ASSERT_EQ(OB_NOT_INIT, reader2.close());
}

TEST_F(TestStorageFile, test_writer)
{
  ObStorageUtil util(0 /*0s*/, 0 /*0s*/);
  ObStorageWriter writer;
  ObStorageWriter writer2;
  char uri[OB_MAX_URI_LENGTH];
  const ObString storage_info;
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  char read_buf[OB_MAX_URI_LENGTH];
  ASSERT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, storage_info));

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "file://%s/test_file", test_dir_));

  ASSERT_EQ(OB_INVALID_BACKUP_DEST, writer.open("bad://", storage_info));
  ASSERT_EQ(OB_SUCCESS, writer.open(uri, storage_info));

  ASSERT_EQ(OB_INIT_TWICE, writer.open(uri, storage_info));

  ASSERT_EQ(OB_SUCCESS, writer2.open(uri, storage_info));
  ASSERT_EQ(OB_SUCCESS, writer2.write(read_buf, strlen(test_content)));
  ASSERT_EQ(OB_SUCCESS, writer2.close());

  ASSERT_EQ(OB_NOT_INIT, writer2.close());
}

TEST_F(TestStorageFile, test_file_writer_fail)
{
  ObStorageUtil util(0 /*0s*/, 0 /*0s*/);
  ObStorageFileWriter writer;
  ObStorageFileWriter writer2;
  char uri[OB_MAX_URI_LENGTH];
  const ObString storage_info;
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  char test_content2[OB_MAX_URI_LENGTH] = "just_for_test2";
  ASSERT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, storage_info));

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "file://%s/test_file", test_dir_));

  ASSERT_EQ(OB_SUCCESS, writer.open(uri, storage_info));
  ASSERT_EQ(OB_SUCCESS, writer.write(test_content, strlen(test_content)));
  ASSERT_EQ(OB_SUCCESS, writer.close());
  ASSERT_EQ(OB_SUCCESS, writer2.open(uri, storage_info));
  ASSERT_EQ(OB_SUCCESS, writer2.write(test_content2, strlen(test_content2)));
  writer2.has_error_ = true;
  ASSERT_EQ(OB_SUCCESS, writer2.close());

  char read_buf[OB_MAX_URI_LENGTH];
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, util.read_single_file(uri, storage_info, read_buf, sizeof(read_buf), read_size));
  COMMON_LOG(INFO, "dump buf", K(test_content), K(test_content2), K(read_buf));
  ASSERT_EQ(0, MEMCMP(test_content, read_buf, read_size));
}

TEST_F(TestStorageFile, test_meta)
{
  ObStorageUtil util(0 /*0s*/, 0 /*0s*/);
  ObStorageMetaWrapper meta;
  char uri[OB_MAX_URI_LENGTH];
  const ObString storage_info;
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  char read_buf[OB_MAX_URI_LENGTH];
  int64_t read_size = 0;

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "file://%s/test_file", test_dir_));
  ASSERT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, storage_info));

  ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, meta.get(uri, storage_info, read_buf, sizeof(read_buf), read_size));
  ASSERT_EQ(OB_SUCCESS, meta.set(uri, storage_info, test_content, strlen(test_content)));
  ASSERT_EQ(OB_SUCCESS, meta.set(uri, storage_info, test_content, strlen(test_content)));
  ASSERT_EQ(OB_SUCCESS, meta.get(uri, storage_info, read_buf, sizeof(read_buf), read_size));
  ASSERT_EQ(read_size, strlen(test_content));

  ASSERT_EQ(OB_INVALID_BACKUP_DEST, meta.get("bad_file:///", storage_info, read_buf, sizeof(read_buf), read_size));
  ASSERT_EQ(OB_INVALID_BACKUP_DEST, meta.set("bad_file:///", storage_info, test_content, strlen(test_content)));
}

TEST_F(TestStorageFile, test_mkdir)
{
  ObStorageUtil util(0 /*0s*/, 0 /*0s*/);
  char uri[OB_MAX_URI_LENGTH];
  const ObString storage_info;
  bool is_exist = false;

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "file://%s/nfs/test/ob/7/1001/113415412451/0", test_dir_));
  ASSERT_EQ(OB_SUCCESS, util.mkdir(uri, storage_info));
  ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, storage_info, is_exist));
  ASSERT_TRUE(is_exist);
}

TEST_F(TestStorageFile, test_parallel_mkdir)
{
  class MakedirTask : public ObDynamicThreadTask {
  public:
    int init(const char* dir)
    {
      EXPECT_FALSE(NULL == dir);
      int64_t timestamp = ObTimeUtility::current_time();
      EXPECT_EQ(OB_SUCCESS,
          databuff_printf(
              uri_, OB_MAX_URI_LENGTH, "file://%s/%ld/nfs/test/parallel/mkdir/1/2/3/4/5/6/7/8", dir, timestamp));
      return OB_SUCCESS;
    }
    int process(const bool& is_stop)
    {
      UNUSED(is_stop);
      ObStorageUtil util(0 /*0s*/, 0 /*0s*/);
      const ObString storage_info;
      bool is_exist = false;
      EXPECT_EQ(OB_SUCCESS, util.mkdir(uri_, storage_info));
      EXPECT_EQ(OB_SUCCESS, util.is_exist(uri_, storage_info, is_exist));
      EXPECT_TRUE(is_exist);
      return OB_SUCCESS;
    }

  private:
    char uri_[OB_MAX_URI_LENGTH];
  };

  ObDynamicThreadPool pool;
  const int64_t task_count = 30;
  ASSERT_EQ(OB_SUCCESS, pool.init());
  ASSERT_EQ(OB_SUCCESS, pool.set_task_thread_num(30));
  MakedirTask task;
  task.init(test_dir_);
  for (int64_t i = 0; i < task_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, pool.add_task(&task));
  }
  sleep(10);
  ASSERT_EQ(0, pool.get_task_count());
  pool.stop();
  pool.destroy();
}

TEST_F(TestStorageFile, test_get_pkeys_from_dir)
{
  ObStorageUtil util(0 /*0s*/, 0 /*0s*/);
  const ObString storage_info;

  char uri[OB_MAX_URI_LENGTH];
  char dir_uri[OB_MAX_URI_LENGTH];
  char dir_name[OB_MAX_URI_LENGTH];

  const int64_t ts = ObTimeUtility::current_time();

  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_name, sizeof(dir_name), "test_get_pkeys_from_dir/%ld", ts));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s", test_dir_uri_, dir_name));

  char buffer[1024] = "abcdefghijklmnopqrstuvwxyz";

  ASSERT_EQ(OB_SUCCESS, util.mkdir(dir_uri, storage_info));

  // create 10 partitions
  constexpr int partitions_cnt = 10;
  for (int i = 0; i < partitions_cnt; i++) {
    // format object path uri
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%ld_%d", dir_uri, ts, i));

    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info, buffer, sizeof(buffer)));
  }

  // create 10 tmp partition key file
  for (int i = 0; i < partitions_cnt; i++) {
    // format object path uri %s.tmp.%ld
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%ld_%d.tmp.%ld", dir_uri, ts, i, ts));

    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info, buffer, sizeof(buffer)));
  }

  ObArray<ObPartitionKey> pkeys;
  ASSERT_EQ(OB_SUCCESS, util.get_pkeys_from_dir(dir_uri, storage_info, pkeys));

  bool flags[partitions_cnt] = {false};
  ASSERT_EQ(partitions_cnt, pkeys.count());
  for (int i = 0; i < partitions_cnt; i++) {
    ASSERT_EQ(ts, pkeys[i].get_table_id());
    uint64_t pid = pkeys[i].get_partition_id();
    ASSERT_FALSE(flags[pid]);
    flags[pid] = true;
  }
}

int main(int argc, char** argv)
{
  system("rm -f test_storage_file.log");
  OB_LOGGER.set_file_name("test_storage_file.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
