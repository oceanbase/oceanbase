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
#define private public
#include "lib/utility/ob_test_util.h"
#include "lib/restore/ob_storage.h"
#include "lib/restore/ob_storage_file.h"
#include "lib/allocator/page_arena.h"

using namespace oceanbase::common;

class TestStorageOss: public ::testing::Test
{
public:
  TestStorageOss() {}
  virtual ~TestStorageOss(){}
  virtual void SetUp()
  {
    ObOssEnvIniter::get_instance().global_init();
  }
  virtual void TearDown()
  {
    ObOssEnvIniter::get_instance().global_destroy();
  }

  static void SetUpTestCase()
  {
  }

  static void TearDownTestCase()
  {
  }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestStorageOss);
protected:
  // function members
protected:
  // oss://ob-unit-test
  char test_oss_bucket_[OB_MAX_URI_LENGTH] = "oss://antsys-oceanbasebackup";

  // oss config
  char oss_host_[OB_MAX_URI_LENGTH] = "";
  char oss_id_[MAX_OSS_ID_LENGTH] = "fill_test_id";
  char oss_key_[MAX_OSS_KEY_LENGTH] = "fill_test_key";
};

TEST_F(TestStorageOss, test_del)
{
  ObStorageUtil util(false);
  char uri[OB_MAX_URI_LENGTH];
  char dir_uri[OB_MAX_URI_LENGTH];
  char storage_infos[OB_MAX_URI_LENGTH];
  ObArenaAllocator allocator;

  ASSERT_EQ(OB_SUCCESS, databuff_printf(storage_infos, sizeof(storage_infos), "host=%s&access_id=%s&access_key=%s",
        oss_host_, oss_id_, oss_key_));
  const ObString storage_info(storage_infos);
  const char *dir_name = "test";
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  int64_t read_size = 0;

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s/test_file", test_oss_bucket_, dir_name));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", test_oss_bucket_, dir_name));

  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info, test_content, 0));
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, storage_info, read_size));
  ObArray <ObString> file_names;
  ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, storage_info, allocator, file_names));
  for (int64_t i = 0; i < file_names.count(); ++i) {
    STORAGE_LOG(INFO, "dump", K(read_size), K(i), K(file_names.at(i)));
  }

}

TEST_F(TestStorageOss, test_oss_acl)
{
  ObStorageUtil util(false);
  char uri[OB_MAX_URI_LENGTH];
  char storage_infos[OB_MAX_URI_LENGTH];
  ObArenaAllocator allocator;

  ASSERT_EQ(OB_SUCCESS, databuff_printf(storage_infos, sizeof(storage_infos), "host=%s&access_id=%s&access_key=%s",
      oss_host_, oss_id_, oss_key_));
  const ObString storage_info(storage_infos);
  const char* dir_name = "buju_dir/put_test/acl";
  
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s", test_oss_bucket_, dir_name));
  // oss://antsys-oceanbasebackup/buju_dir/put_test/acl

  // put test object
  char msg[OB_MAX_URI_LENGTH] = "this is for test";
  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info, msg, sizeof(msg)));
  
  // change acl and assert
  aos_string_t bucket;
  aos_string_t object;
  char test_oss_bucket[50] = "antsys-oceanbasebackup";
  aos_str_set(&bucket, test_oss_bucket);
  aos_str_set(&object, dir_name);

  aos_pool_t *aos_pool;
  aos_pool_create(&aos_pool, NULL);
  oss_request_options_t *options = NULL;
  options = oss_request_options_create(aos_pool);
  options->config = oss_config_create(options->pool);
  options->config->is_cname = 0;
  aos_str_set(&options->config->endpoint, oss_host_);
  aos_str_set(&options->config->access_key_id, oss_id_);
  aos_str_set(&options->config->access_key_secret, oss_key_);
  options->ctl = aos_http_controller_create(options->pool, 0);
  options->ctl->options = aos_http_request_options_create(options->pool);

  ObStorageOssUtil ossutil;
  aos_table_t *resp_headers = NULL;
  aos_status_t *s = NULL;
  const char *modified_time_before = NULL;
  int64_t time_before = 0;
  aos_string_t acl_before;

  s = oss_get_object_acl(options, &bucket, &object, &acl_before, &resp_headers);
  ASSERT_TRUE(s->code == 200);
  printf("acl type before: %s\n", acl_before.data);
  // get modify time
  s = oss_get_object_meta(options, &bucket, &object, &resp_headers);
  ASSERT_TRUE(s->code == 200);
  modified_time_before = apr_table_get(resp_headers, "Last-Modified");
  ossutil.ob_strtotime(modified_time_before, time_before);

  const char *modified_time_after = NULL;
  int64_t time_after = 0;
  aos_string_t acl_after;
  
  s = oss_get_object_acl(options, &bucket, &object, &acl_after, &resp_headers);
  ASSERT_TRUE(s->code == 200);
  printf("acl type after: %s\n", acl_after.data);
  // get modify time, must larger
  s = oss_get_object_meta(options, &bucket, &object, &resp_headers);
  ASSERT_TRUE(s->code == 200);
  modified_time_after = apr_table_get(resp_headers, "Last-Modified");
  ossutil.ob_strtotime(modified_time_after, time_after);

  ASSERT_TRUE(time_before < time_after);

  if(strcasecmp(acl_before.data, "public-read") == 0 || strcasecmp(acl_before.data, "default") == 0)
  {
    ASSERT_EQ(strcasecmp(acl_after.data, "private"), 0);// OSS_ACL_PRIVATE
  } else if(strcasecmp(acl_before.data, "private") == 0){
    ASSERT_EQ(strcasecmp(acl_after.data, "default"), 0);// OSS_ACL_DEFAULT
  }

  aos_string_t acl_after_again;
  const char *modified_after_again = NULL;
  int64_t time_after_again = 0;

  s = oss_get_object_acl(options, &bucket, &object, &acl_after_again, &resp_headers);
  ASSERT_TRUE(s->code == 200);
  printf("acl type after again: %s\n", acl_after_again.data);
  // get modify time, must larger
  s = oss_get_object_meta(options, &bucket, &object, &resp_headers);
  ASSERT_TRUE(s->code == 200);
  modified_after_again = apr_table_get(resp_headers, "Last-Modified");
  ossutil.ob_strtotime(modified_after_again, time_after_again);
  
  ASSERT_TRUE(time_after < time_after_again);

  if(strcasecmp(acl_after.data, "public-read") == 0 || strcasecmp(acl_after.data, "default") == 0)
  {
    ASSERT_EQ(strcasecmp(acl_after_again.data , "private"), 0);
  } else if(strcasecmp(acl_after.data, "private") == 0){
    ASSERT_EQ(strcasecmp(acl_after_again.data , "default"), 0);
  }

  aos_pool_destroy(aos_pool);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
