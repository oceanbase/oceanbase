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
#include "test_object_storage.h"
#include "lib/utility/ob_test_util.h"
#include "lib/restore/ob_storage.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array_serialization.h"
#include <thread>

using namespace oceanbase::common;

class TestObjectStorageListOp : public ObBaseDirEntryOperator
{
public:
  TestObjectStorageListOp(const int64_t expect_file_size = -1) : expect_file_size_(expect_file_size) {}
  virtual ~TestObjectStorageListOp() {}
  int func(const dirent *entry) override;
  virtual bool need_get_file_size() const { return true; }

  std::set<std::string> object_names_;
  const int64_t expect_file_size_;
};

int TestObjectStorageListOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  OB_LOG(INFO, "TestObjectStorageListOp list file", K(entry->d_name));
  const int64_t size = get_size();
  if (expect_file_size_ != -1 && size != expect_file_size_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "wrong file size",
        K_(expect_file_size), K(size), K(entry->d_name));
  }
  std::string tmp(entry->d_name);
  object_names_.emplace(std::move(tmp));
  return ret;
}

class TestObjectStorage : public ::testing::Test
{
public:
  TestObjectStorage() {}
  virtual ~TestObjectStorage(){}
  virtual void SetUp() {}
  virtual void TearDown() {}

  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, init_oss_env());
    ASSERT_EQ(OB_SUCCESS, init_cos_env());
    ASSERT_EQ(OB_SUCCESS, init_s3_env());
    if (enable_test) {
      ASSERT_EQ(OB_SUCCESS, set_storage_info(bucket, endpoint, secretid, secretkey,
                                             appid, region, extension, info_base));
    }
  }
  static void TearDownTestCase()
  {
    fin_oss_env();
    fin_cos_env();
    fin_s3_env();
  }

  int del_appendable_file(const ObString &uri)
  {
    int ret = OB_SUCCESS;
    ObStorageUtil util;
    TestObjectStorageListOp op;
    if (OB_FAIL(util.open(&info_base))) {
      OB_LOG(WARN, "fail to open util", K(ret), K(uri));
    } else if (OB_FAIL(util.list_files(uri, op))) {
      OB_LOG(WARN, "fail to list files", K(ret), K(uri));
    } else {
      for (auto &obj_name : op.object_names_) {
        std::string tmp = std::string(uri.ptr()) + "/" + obj_name;
        if (OB_FAIL(util.del_file(tmp.c_str()))) {
          OB_LOG(WARN, "fail to del file", K(ret), K(tmp.c_str()));
          break;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(util.del_file(uri))) {
        OB_LOG(WARN, "fail to del normal file", K(ret), K(uri));
      }
    }
    util.close();
    return ret;
  }

  static int set_storage_info(const char *bucket, const char *endpoint,
      const char *secretid, const char *secretkey, const char *appid, const char *region,
      const char *extension, ObObjectStorageInfo &info_base)
  {
    int ret = OB_SUCCESS;
    char account[OB_MAX_URI_LENGTH] = { 0 };
    ObStorageType storage_type = ObStorageType::OB_STORAGE_MAX_TYPE;
    if (OB_FAIL(get_storage_type_from_path(bucket, storage_type))) {
      OB_LOG(WARN, "fail to get storage type", K(ret), K(bucket));
    } else if (OB_UNLIKELY(ObStorageType::OB_STORAGE_MAX_TYPE == storage_type)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "storage type invalid", K(ret), K(storage_type), K(bucket));
    }

    if (OB_FAIL(ret)) {
    } else if (storage_type == ObStorageType::OB_STORAGE_FILE) {
      if (OB_FAIL(info_base.set(storage_type, account))) {
        OB_LOG(WARN, "fail to set storage info", K(ret), K(storage_type));
      }
    } else {
      int64_t pos = 0;
      if (OB_FAIL(databuff_printf(account, sizeof(account), pos,
                                  "host=%s&access_id=%s&access_key=%s",
                                  endpoint, secretid, secretkey))) {
        OB_LOG(WARN, "fail to set account", K(ret), K(endpoint), K(secretid), K(secretkey));
      } else if (ObStorageType::OB_STORAGE_COS == storage_type) {
        if (OB_FAIL(databuff_printf(account, sizeof(account), pos, "&appid=%s", appid))) {
          OB_LOG(WARN, "fail to set appid", K(ret), K(pos), K(account), K(appid));
        }
      } else if (ObStorageType::OB_STORAGE_S3 == storage_type) {
        if (OB_FAIL(databuff_printf(account, sizeof(account), pos, "&s3_region=%s", region))) {
          OB_LOG(WARN, "fail to set region", K(ret), K(pos), K(account), K(region));
        }
      }

      if (OB_SUCC(ret) && OB_NOT_NULL(extension)) {
        if (OB_FAIL(databuff_printf(account, sizeof(account), pos, "&%s", extension))) {
          OB_LOG(WARN, "fail to set extension", K(ret), K(pos), K(account), K(extension));
        }
      }

      if (OB_SUCC(ret) && OB_FAIL(info_base.set(storage_type, account))) {
        OB_LOG(WARN, "fail to set storage info", K(ret), K(storage_type), K(account));
      }
    }
    return ret;
  }

protected:
  static char account[OB_MAX_URI_LENGTH];
  static const char *dir_name;
  static char uri[OB_MAX_URI_LENGTH];
  static char dir_uri[OB_MAX_URI_LENGTH];
  static ObObjectStorageInfo info_base;

  int object_prefix_len = 5;
};

char TestObjectStorage::account[OB_MAX_URI_LENGTH] = { 0 };
const char *TestObjectStorage::dir_name = "object_storage_unittest_dir";
char TestObjectStorage::uri[OB_MAX_URI_LENGTH] = { 0 };
char TestObjectStorage::dir_uri[OB_MAX_URI_LENGTH] = { 0 };
ObObjectStorageInfo TestObjectStorage::info_base;

int test_gen_object_meta(
    const char **fragments,
    const int64_t n_fragments,
    const int64_t n_remained_fragments,
    const int64_t *expected_start,
    const int64_t *expected_end,
    const int64_t expected_file_length,
    ObStorageObjectMeta &appendable_obj_meta)
{
  int ret = OB_SUCCESS;
  dirent entry;
  ListAppendableObjectFragmentOp op(false);
  appendable_obj_meta.reset();

  for (int64_t i = 0; OB_SUCC(ret) && i < n_fragments; i++) {
    if (OB_FAIL(databuff_printf(entry.d_name, sizeof(entry.d_name), "%s%s",
        OB_S3_APPENDABLE_FRAGMENT_PREFIX, fragments[i]))) {
      OB_LOG(WARN, "fail to databuff printf", K(ret), K(i), K(fragments[i]));
    } else if (OB_FAIL(op.func(&entry))) {
      OB_LOG(WARN, "fail to execute op", K(ret), K(i));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(op.gen_object_meta(appendable_obj_meta))) {
    OB_LOG(WARN, "fail to gen object meta", K(ret), K(appendable_obj_meta));
  } else if (ObStorageObjectMetaType::OB_OBJ_SIMULATE_APPEND != appendable_obj_meta.type_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "not expected value", K(ret), K(appendable_obj_meta.type_));
  } else if (expected_file_length != appendable_obj_meta.length_) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "not expected value", K(ret), K(expected_file_length), K(appendable_obj_meta.length_));
  } else if (n_remained_fragments != appendable_obj_meta.fragment_metas_.count()) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "not expected value", K(ret), K(n_remained_fragments), K(appendable_obj_meta.fragment_metas_.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < n_remained_fragments; i++) {
    if (!appendable_obj_meta.fragment_metas_[i].is_data() || !appendable_obj_meta.fragment_metas_[i].is_valid() ||
        expected_start[i] != appendable_obj_meta.fragment_metas_[i].start_ ||
        expected_end[i] != appendable_obj_meta.fragment_metas_[i].end_) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "not expected value", K(ret), K(i), K(appendable_obj_meta.fragment_metas_[i]), K(expected_start[i]), K(expected_end[i]));
    }
  }
  return ret;
}

int test_get_needed_fragments(
    const int64_t start,
    const int64_t end,
    const int64_t n_expected_fragments,
    const int64_t *expected_start,
    const int64_t *expected_end,
    ObStorageObjectMeta &appendable_obj_meta)
{
  int ret = OB_SUCCESS;
  ObArray<ObAppendableFragmentMeta> fragments_need_to_read;
  if (OB_FAIL(appendable_obj_meta.get_needed_fragments(start, end, fragments_need_to_read))) {
    OB_LOG(WARN, "fail to get needed fragments", K(ret), K(start), K(end));
  } else if (n_expected_fragments != fragments_need_to_read.count()) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "not expected value", K(ret), K(n_expected_fragments), K(fragments_need_to_read.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < n_expected_fragments; i++) {
    if (!fragments_need_to_read[i].is_data() || !fragments_need_to_read[i].is_valid() ||
        expected_start[i] != fragments_need_to_read[i].start_ ||
        expected_end[i] != fragments_need_to_read[i].end_) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "not expected value", K(ret), K(i), K(fragments_need_to_read[i]), K(expected_start[i]), K(expected_end[i]));
    }
  }
  return ret;
}

TEST_F(TestObjectStorage, test_appendable_object_util)
{
  int ret = OB_SUCCESS;
  if (enable_test) {
    {
      ListAppendableObjectFragmentOp op;
      dirent entry;

      // meta file
      ASSERT_EQ(OB_SUCCESS, databuff_printf(entry.d_name, sizeof(entry.d_name), "%s%s",
                                            OB_S3_APPENDABLE_FRAGMENT_PREFIX,
                                            OB_S3_APPENDABLE_SEAL_META));
      ASSERT_EQ(OB_SUCCESS, op.func(&entry));
      // foramt file
      ASSERT_EQ(OB_SUCCESS, databuff_printf(entry.d_name, sizeof(entry.d_name), "%s%s",
                                            OB_S3_APPENDABLE_FRAGMENT_PREFIX,
                                            OB_S3_APPENDABLE_FORMAT_META));
      ASSERT_EQ(OB_SUCCESS, op.func(&entry));

      // invalid fragment name
      const char *invalid_fragments[] = {"-2--1", "-1-1", "2--2", "3-2", "xxx"};
      for (int64_t i = 0; i < sizeof(invalid_fragments) / sizeof(char *); i++) {
        ASSERT_TRUE(sizeof(entry.d_name) >= strlen(invalid_fragments[i]) + 1);
        STRCPY(entry.d_name, invalid_fragments[i]);
        ASSERT_EQ(OB_SUCCESS, databuff_printf(entry.d_name, sizeof(entry.d_name), "%s%s",
                                              OB_S3_APPENDABLE_FRAGMENT_PREFIX,
                                              invalid_fragments[i]));
        ASSERT_EQ(OB_INVALID_ARGUMENT, op.func(&entry));
      }
    }

    {
      // empty appendable object
      const char *fragments[] = {};
      int64_t n_fragments = sizeof(fragments) / sizeof(char *);
      int64_t expected_start[] = {};
      int64_t expected_end[] = {};
      int64_t n_remained_fragments = sizeof(expected_start) / sizeof(int64_t);
      int64_t expected_file_length = 0;
      ObStorageObjectMeta appendable_obj_meta;
      ASSERT_EQ(OB_SUCCESS, test_gen_object_meta(fragments, n_fragments, n_remained_fragments,
        expected_start, expected_end, expected_file_length, appendable_obj_meta));

      ObArray<ObAppendableFragmentMeta> fragments_need_to_read;
      ASSERT_EQ(OB_INVALID_ARGUMENT,
          appendable_obj_meta.get_needed_fragments(-1, 1, fragments_need_to_read));
      ASSERT_EQ(OB_INVALID_ARGUMENT,
          appendable_obj_meta.get_needed_fragments(5, 4, fragments_need_to_read));
      ASSERT_EQ(OB_INVALID_ARGUMENT,
          appendable_obj_meta.get_needed_fragments(5, 5, fragments_need_to_read));
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(0, 1, fragments_need_to_read));
    }

    {
      // one fragment
      const char *fragments[] = {"10-20.back"};
      int64_t n_fragments = sizeof(fragments) / sizeof(char *);
      int64_t expected_start[] = {10};
      int64_t expected_end[] = {20};
      int64_t n_remained_fragments = sizeof(expected_start) / sizeof(int64_t);
      int64_t expected_file_length = 20;
      ObStorageObjectMeta appendable_obj_meta;
      ASSERT_EQ(OB_SUCCESS, test_gen_object_meta(fragments, n_fragments, n_remained_fragments,
        expected_start, expected_end, expected_file_length, appendable_obj_meta));

      ObArray<ObAppendableFragmentMeta> fragments_need_to_read;
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(5, 8, fragments_need_to_read));
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(5, 10, fragments_need_to_read));
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(5, 15, fragments_need_to_read));
      ASSERT_EQ(OB_INVALID_ARGUMENT,
          appendable_obj_meta.get_needed_fragments(11, 11, fragments_need_to_read));

      {
        int64_t start = 10;
        int64_t end = 18;
        int64_t expected_start[] = {10};
        int64_t expected_end[] = {20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start,
                  expected_end, appendable_obj_meta));
      }
      {
        int64_t start = 15;
        int64_t end = 18;
        int64_t expected_start[] = {10};
        int64_t expected_end[] = {20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start,
                  expected_end, appendable_obj_meta));
      }
      {
        int64_t start = 19;
        int64_t end = 20;
        int64_t expected_start[] = {10};
        int64_t expected_end[] = {20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start,
                  expected_end, appendable_obj_meta));
      }
      {
        int64_t start = 19;
        int64_t end = 21;
        int64_t expected_start[] = {10};
        int64_t expected_end[] = {20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start,
                  expected_end, appendable_obj_meta));
      }
      {
        int64_t start = 20;
        int64_t end = 21;
        int64_t expected_start[] = {};
        int64_t expected_end[] = {};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start,
                  expected_end, appendable_obj_meta));
      }
    }

    {
      // valid fragment name
      const char *valid_fragments[] = {
          OB_S3_APPENDABLE_FORMAT_META, OB_S3_APPENDABLE_SEAL_META,
          "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",    // covered by "1-7"
          "0-3", "0-3", "0-1", "1-2", "2-3",                  // covered "0-3"
          "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",    // no gap
          // "22-25" & "21-24" are covered by "15-25", and there is a gap from "15-25" to "26-30"
          "15-25", "22-25", "21-24", "26-30",
          "30-1234567", "10000-1234567", "28-1234566"
      };
      int64_t n_fragments = sizeof(valid_fragments) / sizeof(char *);
      int64_t expected_start[] = {0, 1, 7, 8, 9, 10, 11, 12, 15, 26, 30};
      int64_t expected_end[] = {3, 7, 8, 9, 10, 11, 12, 20, 25, 30, 1234567};
      int64_t n_remained_fragments = sizeof(expected_start) / sizeof(int64_t);
      int64_t expected_file_length = 1234567;
      ObStorageObjectMeta appendable_obj_meta;
      // (ObStorageObjectMetaType::OB_OBJ_SIMULATE_APPEND);
      ASSERT_EQ(OB_SUCCESS, test_gen_object_meta(valid_fragments, n_fragments, n_remained_fragments,
        expected_start, expected_end, expected_file_length, appendable_obj_meta));

      {
        int64_t start = 0;
        int64_t end = 3;
        int64_t expected_start[] = {0};
        int64_t expected_end[] = {3};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start,
                  expected_end, appendable_obj_meta));
      }
      {
        int64_t start = 1;
        int64_t end = 4;
        int64_t expected_start[] = {1};
        int64_t expected_end[] = {7};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start,
                  expected_end, appendable_obj_meta));
      }
      {
        int64_t start = 0;
        int64_t end = 15;
        int64_t expected_start[] = {0, 1, 7, 8, 9, 10, 11, 12};
        int64_t expected_end[] = {3, 7, 8, 9, 10, 11, 12, 20};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start,
                  expected_end, appendable_obj_meta));
      }
      {
        ObArray<ObAppendableFragmentMeta> fragments_need_to_read;
        ASSERT_EQ(OB_ERR_UNEXPECTED,
          appendable_obj_meta.get_needed_fragments(15, 30, fragments_need_to_read));
      }
      {
        int64_t start = 28;
        int64_t end = 2345678;
        int64_t expected_start[] = {26, 30};
        int64_t expected_end[] = {30, 1234567};
        int64_t n_expected_fragments = sizeof(expected_start) / sizeof(int64_t);
        ASSERT_EQ(OB_SUCCESS, test_get_needed_fragments(start, end, n_expected_fragments, expected_start,
                  expected_end, appendable_obj_meta));
      }
    }
  }
}

TEST_F(TestObjectStorage, test_check_storage_obj_meta)
{
  int ret = OB_SUCCESS;
  if (enable_test) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base));
    const char *tmp_append_dir = "test_check_storage_obj_meta";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_append_dir, ts));

    {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri),
                                            "%s/test_appendable_%ld.back",
                                            dir_uri, ObTimeUtility::current_time()));
      // util before append
      ObStorageAppender appender;
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base));
      const char *content = "123456789ABC";
      bool is_obj_exist = true;
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, false/*is_adaptive*/, is_obj_exist));
      ASSERT_FALSE(is_obj_exist);
      is_obj_exist = true;
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, true/*is_adaptive*/, is_obj_exist));
      ASSERT_FALSE(is_obj_exist);

      // util after append
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(content, strlen(content), 0));
      is_obj_exist = true;
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, false/*is_adaptive*/, is_obj_exist));
      if (ObStorageType::OB_STORAGE_S3 == info_base.get_type()) {
        ASSERT_FALSE(is_obj_exist);
      } else {
        ASSERT_TRUE(is_obj_exist);
        is_obj_exist = false;
      }
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, true/*is_adaptive*/, is_obj_exist));
      ASSERT_TRUE(is_obj_exist);

      int64_t file_length = -1;
      if (info_base.get_type() == ObStorageType::OB_STORAGE_S3) {
        ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.get_file_length(uri, false/*is_adaptive*/, file_length));
      } else {
        ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, false/*is_adaptive*/, file_length));
        ASSERT_EQ(strlen(content), file_length);
        file_length = -1;
      }
      ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, true/*is_adaptive*/, file_length));
      ASSERT_EQ(strlen(content), file_length);

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, false/*is_adaptive*/, is_obj_exist));
      ASSERT_FALSE(is_obj_exist);
      is_obj_exist = true;
      ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, true/*is_adaptive*/, is_obj_exist));
      ASSERT_FALSE(is_obj_exist);

      // open twice
      ASSERT_EQ(OB_INIT_TWICE, util.open(&info_base));
      util.close();

      // invalid storage info
      ASSERT_EQ(OB_INVALID_ARGUMENT, util.open(NULL));
    }
  }
}

TEST_F(TestObjectStorage, test_util_list_files_with_consecutive_slash)
{
  int ret = OB_SUCCESS;
  if (enable_test) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base));

    // uri has two '//;
    TestObjectStorageListOp op;
    if (info_base.get_type() == ObStorageType::OB_STORAGE_FILE) {
      ASSERT_EQ(OB_SUCCESS,
          databuff_printf(uri, sizeof(uri), "%s%s//", OB_FILE_PREFIX ,get_current_dir_name()));
      ASSERT_EQ(OB_SUCCESS, util.list_files(uri, true/*is_adaptive*/, op));
    } else {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s//test",
                                            bucket, dir_name));
      ASSERT_EQ(OB_INVALID_ARGUMENT, util.list_files(uri, true/*is_adaptive*/, op));
    }
  }
}

TEST_F(TestObjectStorage, test_util_list_files)
{
  int ret = OB_SUCCESS;
  if (enable_test) {
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util_list_files";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_util_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base));

    {
      // wrong uri
      uri[0] = '\0';
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.list_files(uri, op));
    }

    int64_t file_num = 1001;
    const char *write_content = "0123456789";

    // list objects
    {
      const char *format = "%s/%ld_%ld";
      std::set<std::string> files;
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri, file_idx, file_idx));
        files.emplace(std::string(uri + strlen(dir_uri) + 1));
        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));
      }

      // list and check and clean
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, false, op));
      ASSERT_EQ(file_num, op.object_names_.size());
      ASSERT_EQ(files, op.object_names_);
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri, file_idx, file_idx));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
      }
    }

    // list subfolders' objects
    {
      const char *format = "%s/%ld/%ld";
      std::set<std::string> files;
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri, file_idx, file_idx));
        files.emplace(std::string(uri + strlen(dir_uri) + 1));
        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));
      }

      // list and check and clean
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, false, op));
      ASSERT_EQ(file_num, op.object_names_.size());
      ASSERT_EQ(files, op.object_names_);
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri, file_idx, file_idx));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
      }
    }

    {
      // list empty dir, now dir_uri should be empty after delete
      TestObjectStorageListOp list_empty_op;
      ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, false, list_empty_op));
      ASSERT_EQ(0, list_empty_op.object_names_.size());
    }

    util.close();
  }
}

TEST_F(TestObjectStorage, test_util_list_directories)
{
  int ret = OB_SUCCESS;
  if (enable_test) {
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util_list_directories";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_util_dir, ts));
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base));

    {
      // wrong uri
      uri[0] = '\0';
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_INVALID_BACKUP_DEST, util.list_directories(uri, false/*is_adaptive*/, op));
    }

    int64_t file_num = 1001;
    const char *write_content = "0123456789";

    // list objects
    {
      const char *format = "%s/%ld/%ld";
      std::set<std::string> files;
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri, file_idx, file_idx));
        files.emplace(std::string(std::to_string(file_idx)));
        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));
      }

      // list and check and clean
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_directories(dir_uri, false, op));
      ASSERT_EQ(file_num, op.object_names_.size());
      ASSERT_EQ(files, op.object_names_);
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri, file_idx, file_idx));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
      }
    }

    // no sub dirs
    {
      const char *format = "%s/%ld_%ld";
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri, file_idx, file_idx));
        ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));
      }

      // list and check and clean
      TestObjectStorageListOp op;
      ASSERT_EQ(OB_SUCCESS, util.list_directories(dir_uri, false, op));
      ASSERT_EQ(0, op.object_names_.size());
      for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), format,
                                              dir_uri, file_idx, file_idx));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
      }
    }

    util.close();
  }
}

TEST_F(TestObjectStorage, test_util_list_adaptive_files)
{
  int ret = OB_SUCCESS;
  if (enable_test) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base));
    const char *tmp_append_dir = "test_util_list_files";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_append_dir, ts));

    {
      std::set<std::string> files;
      ObStorageAppender appender;
      const char *write_content = "012345678";
      auto write_group_files = [&](const int64_t file_num, const char *prefix) {
        for (int64_t file_idx = 0; file_idx < file_num; file_idx++) {
          std::string file_name;
          // normal file
          ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s%ld-normal",
                                                dir_uri, prefix, file_idx));
          files.emplace(std::string(uri + strlen(dir_uri) + 1));
          ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));

          // appendable_file
          ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s%ld-appendable",
                                                dir_uri, prefix, file_idx));
          files.emplace(std::string(uri + strlen(dir_uri) + 1));
          ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base));
          ASSERT_EQ(OB_SUCCESS, appender.pwrite(write_content, strlen(write_content), 0));
          ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
          ASSERT_EQ(OB_SUCCESS, appender.close());

          // appendable_file with suffix
          ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s%ld-appendable.back",
                                                dir_uri, prefix, file_idx));
          files.emplace(std::string(uri + strlen(dir_uri) + 1));
          ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base));
          ASSERT_EQ(OB_SUCCESS, appender.pwrite(write_content, strlen(write_content), 0));
          ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
          ASSERT_EQ(OB_SUCCESS, appender.close());
        }
      };

      // 文件层级
      // 第一级：0-normal, 0-appendable, 0-appendable.back .... 9-normal, 9-appendable, 9-appendable.back
      // 第二级：0/0-normal, 0/0-appendable, 0/0-appendable.back, ... 9/9-normal, 9/9-appendable, 9/9-appendable.back
      // 第三级：0/0/0-normal, 0/0/0-appendable, 0/0/0-appendable.back, ... 9/4/4-normal, 9/4/4-appendable, 9/4/4-appendable.back、
      write_group_files(10, "");
      std::string prefix;
      for (int64_t i = 0; i < 10; i++) {
        prefix = std::to_string(i) + "/";
        write_group_files(10, prefix.c_str());
      }
      for (int64_t i = 0; i < 10; i++) {
        for (int64_t j = 0; j < 5; j++) {
          prefix = std::to_string(i) + "/" + std::to_string(j) + "/";
          write_group_files(5, prefix.c_str());
        }
      }

      // list and check and clean
      TestObjectStorageListOp op(strlen(write_content));
      ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, true/*is_adaptive*/, op));
      ASSERT_EQ(files, op.object_names_);

      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/", dir_uri));
      op.object_names_.clear();
      ASSERT_EQ(OB_SUCCESS, util.list_files(uri, true/*is_adaptive*/, op));
      ASSERT_EQ(files, op.object_names_);

      for (auto &file : files) {
        ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s", dir_uri, file.c_str()));
        ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
      }
    }
  }
}

void test_read_appendable_object(const char *content, const int64_t content_length,
    const int64_t n_run, const int64_t *read_start, const int64_t *read_end,
    ObStorageAdaptiveReader &reader)
{
  int64_t read_size = -1;
  int64_t expected_read_size = -2;
  char buf[content_length];
  for (int64_t i = 0; i < n_run; i++) {
    read_size = -1;
    expected_read_size = MIN(read_end[i], content_length) - MIN(read_start[i], content_length);
    ASSERT_TRUE(read_start[i] <= read_end[i]);
    ASSERT_EQ(OB_SUCCESS,
        reader.pread(buf, read_end[i] - read_start[i], read_start[i], read_size));
    ASSERT_EQ(expected_read_size, read_size);
    ASSERT_EQ(0, memcmp(buf, content + read_start[i], read_size));
  }
}

TEST_F(TestObjectStorage, test_append_rw)
{
  int ret = OB_SUCCESS;
  if (enable_test) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base));
    const char *tmp_append_dir = "test_append";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_append_dir, ts));

    {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri, ObTimeUtility::current_time()));
      ObStorageAppender appender;
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base));
      ASSERT_EQ(OB_INVALID_ARGUMENT, appender.pwrite(NULL, 10, 0));
      ASSERT_EQ(OB_INVALID_ARGUMENT, appender.pwrite("1", 0, 0));
      ASSERT_EQ(OB_INVALID_ARGUMENT, appender.pwrite("1", 1, -1));

      ObStorageAdaptiveReader reader;
      ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, reader.open(uri, &info_base));
    }

    {
      ObStorageAppender appender;
      const char write_content[] = "123";
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/a.b/%ld.back",
                                            dir_uri, ObTimeUtility::current_time()));
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(write_content, strlen(write_content), 0));
      ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
      ASSERT_EQ(OB_SUCCESS, appender.close());

      ObStorageAdaptiveReader reader;
      char read_buf[5] = {0};
      int64_t read_size = 0;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 5, 0, read_size));
      ASSERT_EQ(strlen(write_content), read_size);
      ASSERT_EQ('2', read_buf[1]);
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
    }

    {
      ObStorageAppender appender;
      const char write_content[] = "123";
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/a.b/%ld",
                                            dir_uri, ObTimeUtility::current_time()));
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(write_content, strlen(write_content), 0));
      ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
      ASSERT_EQ(OB_SUCCESS, appender.close());

      ObStorageAdaptiveReader reader;
      char read_buf[5] = {0};
      int64_t read_size = 0;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 5, 0, read_size));
      ASSERT_EQ(strlen(write_content), read_size);
      ASSERT_EQ('2', read_buf[1]);
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
    }

    {
      ObStorageAppender appender;
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri, ObTimeUtility::current_time()));
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base));
      // first append
      const char first_write[] = "123";
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(first_write, strlen(first_write), 0));
      ASSERT_EQ(strlen(first_write), appender.get_length());

      // second append
      const char second_write[] = "4567";
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(second_write, strlen(second_write), strlen(first_write)));
      ASSERT_EQ(strlen(first_write) + strlen(second_write), appender.get_length());

      // check size
      int64_t file_length = 0;
      ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, true/*is_adaptive*/, file_length));
      ASSERT_EQ(strlen(first_write) + strlen(second_write), file_length);

      // check data and clean
      ObStorageAdaptiveReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
      ASSERT_EQ(strlen(first_write) + strlen(second_write), reader.get_length());
      char read_buf[5] = {0};
      int64_t read_size = 0;
      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 5, 2, read_size));
      ASSERT_EQ('3', read_buf[0]);
      ASSERT_EQ('7', read_buf[4]);
      ASSERT_EQ(5, read_size);
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
      ASSERT_EQ(OB_SUCCESS, appender.close());
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
      ASSERT_FALSE(appender.is_opened());
    }

    if (info_base.get_type() == ObStorageType::OB_STORAGE_S3) {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri, ObTimeUtility::current_time()));

      ObStorageAppender appender;
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base));

      const int64_t content_length = 100;
      char content[content_length] = { 0 };
      for (int64_t i = 0; i < content_length; i++) {
        content[i] = '0' + (i % 10);
      }
      // "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",  // covered by "1-7"
      // "0-3",  "0-3", "0-1", "1-2", "2-3",               // covered "0-3"
      // "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",  // no gap
      // "22-25" & "21-24" are covered by "15-25", and there is a gap from "15-25" to "26-30"
      // "15-25", "22-25", "21-24", "26-30", "30-100", "28-100"
      int64_t fragment_start[] = {1,2,3,4,1,1,1,0,0,0,1,2,7,8,9,10,11,12,15,22,21,26,30,28};
      int64_t fragment_end[] = {7,5,6,7,7,7,5,3,3,1,2,3,8,9,10,11,12,20,25,25,24,30,100,100};
      ASSERT_EQ(sizeof(fragment_start), sizeof(fragment_end));
      for (int64_t i = 0; i < sizeof(fragment_start) / sizeof(int64_t); i++) {
        ASSERT_TRUE(fragment_start[i] < fragment_end[i]);
        ASSERT_EQ(OB_SUCCESS, appender.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
      }
      ASSERT_EQ(content_length, appender.get_length());

      // read before close
      ObStorageAdaptiveReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
      ASSERT_EQ(content_length, reader.get_length());
      int64_t read_start[] = {0, 1, 0, 4, 26, 30, 26};
      int64_t read_end[] = {3, 4, 15, 25, 27, 100, 101};
      ASSERT_EQ(sizeof(read_start), sizeof(read_end));
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);

      char buf[content_length];
      int64_t read_size = -1;
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          reader.pread(buf, content_length, 0, read_size));
      ASSERT_EQ(content_length, reader.get_length());
      {
        ASSERT_EQ(OB_INVALID_ARGUMENT, reader.pread(buf, 0, 0, read_size));
        ASSERT_EQ(OB_INVALID_ARGUMENT, reader.pread(buf, 1, -1, read_size));
        ASSERT_EQ(OB_INVALID_ARGUMENT, reader.pread(NULL, 1, 0, read_size));
      }

      OB_LOG(INFO, "-=-=-=-=-===========-=-=====-=-=-=-=-=-=-=-=-=-=-=-=");
      ASSERT_EQ(OB_SUCCESS, appender.close());
      // open before close, read after close
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
          ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      OB_LOG(INFO, "-=-=-=-=-===========-=-=====-=-=-=-=-=-=-=-=-=-=-=-=");

      // open/read after close
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());
      OB_LOG(INFO, "-=-=-=-=-===========-=-=====-=-=-=-=-=-=-=-=-=-=-=-=");

      // read after seal
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, appender.close());
      OB_LOG(INFO, "-=-=-=-=-===========-=-=====-=-=-=-=-=-=-=-=-=-=-=-=");

      // open/read after seal
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
    }

    if (info_base.get_type() == ObStorageType::OB_STORAGE_S3) {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri, ObTimeUtility::current_time()));

      ObStorageAppender appender;
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base));

      const int64_t content_length = 100;
      char content[content_length] = { 0 };
      for (int64_t i = 0; i < content_length; i++) {
        content[i] = '0' + (i % 10);
      }
      // "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",  // covered by "1-7"
      // "0-3",  "0-3", "0-1", "1-2", "2-3",               // covered "0-3"
      // "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",  // no gap
      // "22-25" & "21-24" are covered by "15-25", and there is a gap from "15-25" to "26-30"
      // "15-25", "22-25", "21-24", "26-30", "30-100", "28-100"
      int64_t fragment_start[] = {1,2,3,4,1,1,1,0,0,0,1,2,7,8,9,10,11,12,15,22,21,26,30,28};
      int64_t fragment_end[] = {7,5,6,7,7,7,5,3,3,1,2,3,8,9,10,11,12,20,25,25,24,30,100,100};
      ASSERT_EQ(sizeof(fragment_start), sizeof(fragment_end));
      for (int64_t i = 0; i < sizeof(fragment_start) / sizeof(int64_t); i++) {
        ASSERT_TRUE(fragment_start[i] < fragment_end[i]);
        ASSERT_EQ(OB_SUCCESS, appender.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
      }
      ASSERT_EQ(content_length, appender.get_length());

      // read before close
      ObStorageAdaptiveReader reader;
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
      ASSERT_EQ(content_length, reader.get_length());

      char buf[content_length];
      int64_t read_size = -1;
      ASSERT_EQ(OB_ERR_UNEXPECTED,
          reader.pread(buf, content_length, 0, read_size));
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // fill gap
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(content + 25, 1, 25));

      // now gap is filled
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS,
          reader.pread(buf, content_length, 0, read_size));
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // test appendable put is valid
      // ObStorageUtil s3_util;
      // ObStorageObjectMeta appendable_obj_meta;
      // ASSERT_EQ(OB_SUCCESS, s3_util.open(&info_base));
      // ASSERT_EQ(OB_SUCCESS, s3_util.list_appendable_file_fragments(uri, appendable_obj_meta));
      // ASSERT_EQ(1, appendable_obj_meta.fragment_metas_.count());
      // ASSERT_EQ(0, appendable_obj_meta.fragment_metas_[0].start_);
      // ASSERT_EQ(9223372036854775807, appendable_obj_meta.fragment_metas_[0].end_);
      // ASSERT_EQ(content_length, appendable_obj_meta.length_);

      ASSERT_EQ(OB_SUCCESS, appender.close());
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
      // open before close, read after close
      int64_t read_start[] = {0, 1, 0, 4, 26, 30, 26};
      int64_t read_end[] = {3, 4, 15, 25, 27, 100, 101};
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
          ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());

      // open/read after close
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
      // ASSERT_EQ(content_length - 1, reader.get_appendable_object_size());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());

      // read after seal
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, appender.seal_for_adaptive());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
          ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());
      ASSERT_EQ(OB_SUCCESS, appender.close());

      // open/read after seal
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
      // ASSERT_EQ(content_length - 1, reader.get_appendable_object_size());
      test_read_appendable_object(content, content_length,
          sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
      ASSERT_EQ(content_length, reader.get_length());
      ASSERT_EQ(OB_SUCCESS, reader.close());

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
    }

    if (info_base.get_type() == ObStorageType::OB_STORAGE_S3) {
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_append_file_%ld.back",
                                            dir_uri, ObTimeUtility::current_time()));

      ObStorageAppender appender_a;
      ObStorageAppender appender_b;
      ObStorageAppender appender_c;
      ASSERT_EQ(OB_SUCCESS, appender_a.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, appender_b.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, appender_c.open(uri, &info_base));

      const int64_t content_length = 100;
      char content[content_length] = { 0 };
      for (int64_t i = 0; i < content_length; i++) {
        content[i] = '0' + (i % 10);
      }
      // "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",  // covered by "1-7"
      // "0-3",  "0-3", "0-1", "1-2", "2-3",               // covered "0-3"
      // "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",  // no gap
      // "22-25" & "21-24" are covered by "15-25"
      // "15-25", "22-25", "21-24", "25-30", "30-100", "28-100"
      int64_t fragment_start[] = {1,2,3,4,1,1,1,0,0,0,1,2,7,8,9,10,11,12,15,22,21,25,30,28};
      int64_t fragment_end[] = {7,5,6,7,7,7,5,3,3,1,2,3,8,9,10,11,12,20,25,25,24,30,100,100};
      ASSERT_EQ(sizeof(fragment_start), sizeof(fragment_end));
      for (int64_t i = 0; i < sizeof(fragment_start) / sizeof(int64_t); i++) {
        ASSERT_TRUE(fragment_start[i] < fragment_end[i]);
        if (i % 3 == 0) {
          ASSERT_EQ(OB_SUCCESS, appender_a.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
        } else if (i % 3 == 1) {
          ASSERT_EQ(OB_SUCCESS, appender_b.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
        } else {
          ASSERT_EQ(OB_SUCCESS, appender_c.pwrite(content + fragment_start[i],
            fragment_end[i] - fragment_start[i], fragment_start[i]));
        }
      }

      // read before close
      ObStorageAdaptiveReader reader_a;
      ObStorageAdaptiveReader reader_b;
      ASSERT_EQ(OB_SUCCESS, reader_a.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, reader_b.open(uri, &info_base));
      int64_t read_start[] = {0, 1, 0, 4, 26, 30, 26, 0};
      int64_t read_end[] = {3, 4, 15, 25, 27, 100, 101, 100};
      ASSERT_EQ(sizeof(read_start), sizeof(read_end));
      int64_t n_run = sizeof(read_start) / sizeof(int64_t);
      std::thread read_thread_a(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_a));
      std::thread read_thread_b(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_b));
      read_thread_a.join();
      read_thread_b.join();
      ASSERT_EQ(content_length, reader_a.get_length());
      ASSERT_EQ(content_length, reader_b.get_length());

      // open before close, read after close
      ASSERT_EQ(OB_SUCCESS, appender_a.close());
      ASSERT_EQ(OB_SUCCESS, appender_b.close());
      ASSERT_EQ(OB_SUCCESS, appender_c.close());
      std::thread read_thread_c(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_a));
      std::thread read_thread_d(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_b));
      read_thread_c.join();
      read_thread_d.join();
      ASSERT_EQ(content_length, reader_a.get_length());
      ASSERT_EQ(content_length, reader_b.get_length());
      ASSERT_EQ(OB_SUCCESS, reader_a.close());
      ASSERT_EQ(OB_SUCCESS, reader_b.close());

      // open/read after close
      ASSERT_EQ(OB_SUCCESS, reader_a.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, reader_b.open(uri, &info_base));
      std::thread read_thread_e(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_a));
      std::thread read_thread_f(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_b));
      read_thread_e.join();
      read_thread_f.join();
      ASSERT_EQ(content_length, reader_a.get_length());
      ASSERT_EQ(content_length, reader_b.get_length());

      // read after seal
      ASSERT_EQ(OB_SUCCESS, appender_a.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, appender_a.seal_for_adaptive());
      std::thread read_thread_g(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_a));
      std::thread read_thread_h(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_b));
      read_thread_g.join();
      read_thread_h.join();
      ASSERT_EQ(content_length, reader_a.get_length());
      ASSERT_EQ(content_length, reader_b.get_length());
      ASSERT_EQ(OB_SUCCESS, reader_a.close());
      ASSERT_EQ(OB_SUCCESS, reader_b.close());
      ASSERT_EQ(OB_SUCCESS, appender_a.close());

      // open/read after seal
      ASSERT_EQ(OB_SUCCESS, reader_a.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, reader_b.open(uri, &info_base));
      std::thread read_thread_i(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_a));
      std::thread read_thread_j(test_read_appendable_object, content, content_length,
                                n_run, read_start, read_end, std::ref(reader_b));
      read_thread_i.join();
      read_thread_j.join();
      ASSERT_EQ(content_length, reader_a.get_length());
      ASSERT_EQ(content_length, reader_b.get_length());
      ASSERT_EQ(OB_SUCCESS, reader_a.close());
      ASSERT_EQ(OB_SUCCESS, reader_b.close());

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
    }
  }
}

TEST_F(TestObjectStorage, test_cross_testing)
{
  char * a = NULL;
  printf("testtttt %s\n", a);
  int ret = OB_SUCCESS;
  ObObjectStorageInfo cross_info;
  if (enable_test) {
    ASSERT_EQ(OB_SUCCESS, set_storage_info(bucket, endpoint, secretid, secretkey,
                                           appid, region, nullptr, cross_info));
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&cross_info));

    // constuct obj name, should be compatible with nfs
    const char *tmp_dir = "test_cross_appendable";
    const int64_t ts = ObTimeUtility::current_time();
    if (cross_info.get_type() == ObStorageType::OB_STORAGE_FILE) {
      ASSERT_EQ(OB_SUCCESS,
          databuff_printf(uri, sizeof(uri), "file://%s/%s_%ld.back", get_current_dir_name(), tmp_dir, ts));
      ASSERT_EQ(OB_SUCCESS, util.mkdir(uri));
    } else {
      ASSERT_EQ(OB_SUCCESS,
          databuff_printf(uri, sizeof(uri), "%s/%s/%s_%ld", bucket, dir_name, tmp_dir, ts));
    }

    // simulate append
    bool is_obj_exist = true;
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, true/*is_adaptive*/, is_obj_exist));
    ASSERT_FALSE(is_obj_exist);

    // write format
    int64_t file_length = -1;
    char fragment_name[OB_MAX_URI_LENGTH] = { 0 };
    ASSERT_EQ(OB_SUCCESS,
        construct_fragment_full_name(uri, OB_S3_APPENDABLE_FORMAT_META, fragment_name, sizeof(fragment_name)));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(fragment_name, OB_S3_APPENDABLE_FORMAT_CONTENT_V1,
                                                 strlen(OB_S3_APPENDABLE_FORMAT_CONTENT_V1)));
    ASSERT_EQ(OB_SUCCESS, util.is_exist(uri, true/*is_adaptive*/, is_obj_exist));
    ASSERT_TRUE(is_obj_exist);
    ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, true/*is_adaptive*/, file_length));
    ASSERT_EQ(0, file_length);

    // put fragments
    // "1-7", "2-5", "3-6", "4-7", "1-7", "1-7", "1-5",  // covered by "1-7"
    // "0-3",  "0-3", "0-1", "1-2", "2-3",               // covered "0-3"
    // "7-8", "8-9", "9-10", "10-11", "11-12", "12-20",  // no gap
    // "22-25" & "21-24" are covered by "15-25"
    // "15-25", "22-25", "21-24", "25-30", "30-100", "28-100"
    int64_t fragment_start[] = {1,2,3,4,1,1,1,0,0,0,1,2,7,8,9,10,11,12,15,22,21,25,30,28};
    int64_t fragment_end[] = {7,5,6,7,7,7,5,3,3,1,2,3,8,9,10,11,12,20,25,25,24,30,100,100};
    const int64_t content_length = 100;
    char content[content_length + 5] = { 0 };
    for (int64_t i = 0; i < content_length + 5; i++) {
      content[i] = '0' + (i % 10);
    }
    ASSERT_EQ(sizeof(fragment_start), sizeof(fragment_end));
    for (int64_t i = 0; i < sizeof(fragment_start) / sizeof(int64_t); i++) {
      ASSERT_TRUE(fragment_start[i] < fragment_end[i]);
      ASSERT_EQ(OB_SUCCESS,
        construct_fragment_full_name(uri, fragment_start[i], fragment_end[i], fragment_name, sizeof(fragment_name)));
      ASSERT_EQ(OB_SUCCESS, util.write_single_file(fragment_name, content + fragment_start[i],
                                                   fragment_end[i] - fragment_start[i]));
    }
    ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, true/*is_adaptive*/, file_length));
    ASSERT_EQ(content_length, file_length);

    // open before close, read after close
    ObStorageAdaptiveReader reader;
    ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cross_info));
    int64_t read_start[] = {0, 1, 0, 4, 26, 30, 26};
    int64_t read_end[] = {3, 4, 15, 25, 27, 100, 101};
    test_read_appendable_object(content, content_length,
        sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
    ASSERT_EQ(content_length, reader.get_length());
    ASSERT_EQ(OB_SUCCESS, reader.close());

    // append a fragment and seal
    ASSERT_EQ(OB_SUCCESS,
        construct_fragment_full_name(uri, 100, 105, fragment_name, sizeof(fragment_name)));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(fragment_name, content, 5));
    char *buf = NULL;
    char seal_meta_uri[OB_MAX_URI_LENGTH] = { 0 };
    ListAppendableObjectFragmentOp op;
    ObStorageObjectMeta appendable_obj_meta;
    ObArenaAllocator allocator;
    int64_t pos = 0;              // for serializing appendable_obj_meta
    int64_t serialize_size = -1;  // for serializing appendable_obj_meta
    ASSERT_EQ(OB_SUCCESS, util.list_appendable_file_fragments(uri, appendable_obj_meta));
    serialize_size = appendable_obj_meta.get_serialize_size() + 1;
    buf = static_cast<char *>(allocator.alloc(serialize_size));
    ASSERT_TRUE(OB_NOT_NULL(buf));
    ASSERT_EQ(OB_SUCCESS, appendable_obj_meta.serialize(buf, serialize_size, pos));
    ASSERT_EQ(OB_SUCCESS, construct_fragment_full_name(uri, OB_S3_APPENDABLE_SEAL_META,
                                                       seal_meta_uri, sizeof(seal_meta_uri)));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(seal_meta_uri, buf, pos));
    ASSERT_EQ(OB_SUCCESS, reader.open(uri, &cross_info));
    ASSERT_EQ(105, reader.get_length());
    test_read_appendable_object(content, 105,
        sizeof(read_start) / sizeof(int64_t), read_start, read_end, reader);
    ASSERT_EQ(OB_SUCCESS, reader.close());

    if (cross_info.get_type() != ObStorageType::OB_STORAGE_FILE) {
      ASSERT_EQ(OB_SUCCESS, util.del_file(uri, true));
    }
  }
}

TEST_F(TestObjectStorage, test_read_single_file)
{
  int ret = OB_SUCCESS;
  if (enable_test) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base));
    const char *tmp_dir = "test_read_single_file";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_dir, ts));

    {
      // normal
      ObStorageWriter writer;
      ObStorageReader reader;
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_normal", dir_uri));
      ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, writer.write("123456", 6));
      ASSERT_EQ(OB_SUCCESS, writer.close());

      int64_t read_size = -1;
      char read_buf[100];
      memset(read_buf, 0, sizeof(read_buf));
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 100, 0, read_size));
      ASSERT_EQ(6, read_size);
      ASSERT_EQ(0, strncmp(read_buf, "123456", 6));
      ASSERT_EQ(OB_SUCCESS, reader.close());

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    }

    {
      // appendable
      ObStorageAppender appender;
      ObStorageAdaptiveReader reader;

      const char *write_content = "0123456789";
      const int64_t first_content_len = 6;
      ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_appendable", dir_uri));
      ASSERT_EQ(OB_SUCCESS, appender.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, appender.pwrite(write_content, first_content_len, 0));

      int64_t read_size = -1;
      char read_buf[100];
      memset(read_buf, 0, sizeof(read_buf));
      ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));

      ASSERT_EQ(OB_SUCCESS, appender.pwrite(write_content, 1, first_content_len));

      ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, 100, 0, read_size));
      ASSERT_EQ(first_content_len, read_size);
      ASSERT_EQ(0, strncmp(read_buf, write_content, first_content_len));
      ASSERT_EQ(OB_SUCCESS, reader.close());

      ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    }
  }
}

TEST_F(TestObjectStorage, test_multipart_write)
{
  int ret = OB_SUCCESS;
  if (enable_test) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base));
    const char *tmp_dir = "test_multipart_write_files";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_dir, ts));

    const int64_t content_size = 20 * 1024 * 1024L + 7; // 20M + 7B
    ObArenaAllocator allocator;
    char *write_buf = (char *)allocator.alloc(content_size);
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    for (int64_t i = 0; i < content_size - 1; i++) {
      write_buf[i] = alphanum[ObRandom::rand(0, sizeof(alphanum) - 2)];
    }
    write_buf[content_size - 1] = '\0';

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/test_multipart", dir_uri));

    {
      // test abort
      ObStorageMultiPartWriter writer;
      ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base));
      ASSERT_EQ(OB_SUCCESS, writer.abort());
      ASSERT_NE(OB_SUCCESS, writer.complete());
      ASSERT_EQ(OB_SUCCESS, writer.close());
    }

    ObStorageMultiPartWriter writer;
    // ObStorageWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base));
    ASSERT_EQ(OB_SUCCESS, writer.write(write_buf, content_size));
    ASSERT_EQ(content_size, writer.get_length());
    ASSERT_EQ(OB_SUCCESS, writer.complete());
    ASSERT_EQ(OB_SUCCESS, writer.close());
    OB_LOG(INFO, "-----------------------------------------------------------------------------");

    ObStorageReader reader;
    int64_t read_size = -1;
    const int64_t read_start = 1024;
    const int64_t read_buf_size = 1024;
    char read_buf[read_buf_size];
    memset(read_buf, 0, sizeof(read_buf));
    ASSERT_EQ(OB_SUCCESS, reader.open(uri, &info_base));
    ASSERT_EQ(content_size, reader.get_length());
    ASSERT_EQ(OB_SUCCESS, reader.pread(read_buf, read_buf_size, read_start, read_size));
    ASSERT_EQ(read_buf_size, read_size);
    ASSERT_EQ(0, memcmp(write_buf + read_start, read_buf, read_size));
    ASSERT_EQ(OB_SUCCESS, reader.close());
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
  }
}

TEST_F(TestObjectStorage, test_del_unmerged_parts)
{
  int ret = OB_SUCCESS;
  if (enable_test && false) {
    ObStorageUtil util;
    ASSERT_EQ(OB_SUCCESS, util.open(&info_base));
    const char *tmp_dir = "test_del_unmerged_parts";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
        bucket, dir_name, tmp_dir, ts));

    const int64_t content_size = 20 * 1024 * 1024L + 7; // 20M + 7B
    ObArenaAllocator allocator;
    char *write_buf = (char *)allocator.alloc(content_size);
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    for (int64_t i = 0; i < content_size - 1; i++) {
      write_buf[i] = alphanum[ObRandom::rand(0, sizeof(alphanum) - 2)];
    }
    write_buf[content_size - 1] = '\0';

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s", dir_uri, tmp_dir));
    ObStorageMultiPartWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.open(uri, &info_base));
    ASSERT_EQ(OB_SUCCESS, writer.write(write_buf, content_size));
    ASSERT_EQ(content_size, writer.get_length());

    ASSERT_EQ(OB_SUCCESS, util.del_unmerged_parts(uri));
    if (info_base.get_type() == ObStorageType::OB_STORAGE_OSS) {
      ASSERT_EQ(OB_OSS_ERROR, writer.close());
    } else if (ObStorageType::OB_STORAGE_S3 == info_base.get_type()) {
      ASSERT_EQ(OB_S3_ERROR, writer.close());
    } else if (info_base.get_type() == ObStorageType::OB_STORAGE_COS) {
      ASSERT_EQ(OB_COS_ERROR, writer.close());
    }

  }
}

TEST_F(TestObjectStorage, test_util_is_tagging)
{
  int ret = OB_SUCCESS;
  if (enable_test) {
    ObStorageUtil util;
    const char *tmp_util_dir = "test_util_is_tagging";
    const int64_t ts = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/%s_%ld",
      bucket, dir_name, tmp_util_dir, ts));

    bool is_tagging = true;
    char tmp_account[OB_MAX_URI_LENGTH];
    ObObjectStorageInfo tmp_info_base;
    const char *write_content = "123456789ABCDEF";

    // wrong tag mode
    ASSERT_EQ(OB_INVALID_ARGUMENT, set_storage_info(bucket, endpoint, secretid, secretkey,
                                                    appid, region, "delete_mode=tag", tmp_info_base));
    tmp_info_base.reset();


    ASSERT_EQ(OB_INVALID_ARGUMENT, set_storage_info(bucket, endpoint, secretid, secretkey,
                                                    appid, region, "delete_mode=delete_delete", tmp_info_base));
    tmp_info_base.reset();

    // delete mode
    ASSERT_EQ(OB_SUCCESS, set_storage_info(bucket, endpoint, secretid, secretkey,
                                           appid, region, "delete_mode=delete", tmp_info_base));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/delete_mode", dir_uri));
    ASSERT_EQ(OB_SUCCESS, util.open(&tmp_info_base));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));

    ASSERT_EQ(OB_SUCCESS, util.is_tagging(uri, is_tagging));
    ASSERT_FALSE(is_tagging);

    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.is_tagging(uri, is_tagging));
    tmp_info_base.reset();
    util.close();

    // tagging mode
    ASSERT_EQ(OB_SUCCESS, set_storage_info(bucket, endpoint, secretid, secretkey,
                                           appid, region, "delete_mode=tagging", tmp_info_base));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/tagging_mode", dir_uri));
    ASSERT_EQ(OB_SUCCESS, util.open(&tmp_info_base));
    ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.is_tagging(uri, is_tagging));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, write_content, strlen(write_content)));

    is_tagging = true;
    ASSERT_EQ(OB_SUCCESS, util.is_tagging(uri, is_tagging));
    ASSERT_FALSE(is_tagging);

    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_SUCCESS, util.is_tagging(uri, is_tagging));
    ASSERT_TRUE(is_tagging);

    tmp_info_base.reset();
    util.close();

    // clean
    ASSERT_EQ(OB_SUCCESS, set_storage_info(bucket, endpoint, secretid, secretkey,
                                           appid, region, nullptr, tmp_info_base));

    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/tagging_mode", dir_uri));
    ASSERT_EQ(OB_SUCCESS, util.open(&tmp_info_base));
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri));
    ASSERT_EQ(OB_BACKUP_FILE_NOT_EXIST, util.is_tagging(uri, is_tagging));
    util.close();
  }
}

int main(int argc, char **argv)
{
  system("rm -f test_object_storage.log*");
  OB_LOGGER.set_file_name("test_object_storage.log", true, true);
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}