#include <gtest/gtest.h>
#include "lib/utility/ob_test_util.h"
#include "lib/restore/ob_storage.h"
#include "lib/restore/ob_storage_file.h"
#include "lib/allocator/page_arena.h"

using namespace oceanbase::common;

class TestStorageOss : public ::testing::Test {
public:
  TestStorageOss()
  {}
  virtual ~TestStorageOss()
  {}
  virtual void SetUp()
  {
    ObOssEnvIniter::get_instance().global_init();
  }
  virtual void TearDown()
  {
    ObOssEnvIniter::get_instance().global_destroy();
  }

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestStorageOss);

protected:
  // function members
protected:
  // oss://ob-unit-test
  char test_oss_bucket_[OB_MAX_URI_LENGTH] = "oss://antsys-oceanbasebackup";

  // oss config
  char oss_host_[OB_MAX_URI_LENGTH] = "cn-hangzhou-alipay-b.oss-cdn.aliyun-inc.com";
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

  ASSERT_EQ(OB_SUCCESS,
      databuff_printf(
          storage_infos, sizeof(storage_infos), "host=%s&access_id=%s&access_key=%s", oss_host_, oss_id_, oss_key_));
  const ObString storage_info(storage_infos);
  const char *dir_name = "test";
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  int64_t read_size = 0;

  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/%s/test_file", test_oss_bucket_, dir_name));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", test_oss_bucket_, dir_name));

  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info, test_content, 0));
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, storage_info, read_size));
  ObArray<ObString> file_names;
  ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, storage_info, allocator, file_names));
  for (int64_t i = 0; i < file_names.count(); ++i) {
    STORAGE_LOG(INFO, "dump", K(read_size), K(i), K(file_names.at(i)));
  }
}

TEST_F(TestStorageOss, test_get_pkey_from_path)
{
  ObStorageUtil util(false);
  char uri[OB_MAX_URI_LENGTH];
  char dir_uri[OB_MAX_URI_LENGTH];
  char storage_infos[OB_MAX_URI_LENGTH];
  ObArenaAllocator allocator;

  ASSERT_EQ(OB_SUCCESS,
      databuff_printf(
          storage_infos, sizeof(storage_infos), "host=%s&access_id=%s&access_key=%s", oss_host_, oss_id_, oss_key_));
  const ObString storage_info(storage_infos);
  const char *dir_name = "test/pkeys";
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  int64_t read_size = 0;
  const ObPartitionKey pkey(1100611139403779, 0, 0);
  ObArray<ObPartitionKey> pkeys;

  ASSERT_EQ(OB_SUCCESS,
      databuff_printf(uri,
          sizeof(uri),
          "%s/%s/%lu/%ld/test_file",
          test_oss_bucket_,
          dir_name,
          pkey.get_table_id(),
          pkey.get_partition_id()));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", test_oss_bucket_, dir_name));
  STORAGE_LOG(INFO, "uri and dir uri", K(uri), K(dir_uri));

  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info, test_content, 0));
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, storage_info, read_size));
  ASSERT_EQ(OB_SUCCESS, util.get_pkeys_from_dir(dir_uri, storage_info, pkeys));
  ASSERT_TRUE(!pkeys.empty());
  bool found = false;
  for (int64_t i = 0; pkeys.count() && !found; ++i) {
    const ObPartitionKey &tmp_pkey = pkeys.at(i);
    if (tmp_pkey == pkey) {
      found = true;
    }
  }
  ASSERT_TRUE(found);
}

TEST_F(TestStorageOss, test_get_bucket_lifecycle)
{
  ObStorageUtil util(false);
  char uri[OB_MAX_URI_LENGTH];
  char storage_infos[OB_MAX_URI_LENGTH];
  ObArenaAllocator allocator;
  bool is_set_lifecycle = false;

  ASSERT_EQ(OB_SUCCESS,
      databuff_printf(
          storage_infos, sizeof(storage_infos), "host=%s&access_id=%s&access_key=%s", oss_host_, oss_id_, oss_key_));
  const ObString storage_info(storage_infos);
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/xuanyi_61/hahaha", test_oss_bucket_));

  ASSERT_EQ(OB_SUCCESS, util.check_bucket_lifecycle(uri, storage_info, is_set_lifecycle));
  ASSERT_EQ(true, is_set_lifecycle);
}

TEST_F(TestStorageOss, test_get_pkey_from_path)
{
  ObStorageUtil util(false);
  char uri[OB_MAX_URI_LENGTH];
  char dir_uri[OB_MAX_URI_LENGTH];
  char storage_infos[OB_MAX_URI_LENGTH];
  ObArenaAllocator allocator;

  ASSERT_EQ(OB_SUCCESS,
      databuff_printf(
          storage_infos, sizeof(storage_infos), "host=%s&access_id=%s&access_key=%s", oss_host_, oss_id_, oss_key_));
  const ObString storage_info(storage_infos);
  const char *dir_name = "test/pkeys";
  char test_content[OB_MAX_URI_LENGTH] = "just_for_test";
  int64_t read_size = 0;
  const ObPartitionKey pkey(1100611139403779, 0, 0);
  ObArray<ObPartitionKey> pkeys;

  ASSERT_EQ(OB_SUCCESS,
      databuff_printf(uri,
          sizeof(uri),
          "%s/%s/%lu/%ld/test_file",
          test_oss_bucket_,
          dir_name,
          pkey.get_table_id(),
          pkey.get_partition_id()));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", test_oss_bucket_, dir_name));
  STORAGE_LOG(INFO, "uri and dir uri", K(uri), K(dir_uri));

  ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info, test_content, 0));
  ASSERT_EQ(OB_SUCCESS, util.get_file_length(uri, storage_info, read_size));
  ASSERT_EQ(OB_SUCCESS, util.get_pkeys_from_dir(dir_uri, storage_info, pkeys));
  ASSERT_TRUE(!pkeys.empty());
  bool found = false;
  for (int64_t i = 0; pkeys.count() && !found; ++i) {
    const ObPartitionKey &tmp_pkey = pkeys.at(i);
    if (tmp_pkey == pkey) {
      found = true;
    }
  }
  ASSERT_TRUE(found);
}

TEST_F(TestStorageOss, test_get_bucket_lifecycle)
{
  ObStorageUtil util(false);
  char uri[OB_MAX_URI_LENGTH];
  char storage_infos[OB_MAX_URI_LENGTH];
  ObArenaAllocator allocator;
  bool is_set_lifecycle = false;

  ASSERT_EQ(OB_SUCCESS,
      databuff_printf(
          storage_infos, sizeof(storage_infos), "host=%s&access_id=%s&access_key=%s", oss_host_, oss_id_, oss_key_));
  const ObString storage_info(storage_infos);
  ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s/xuanyi_61/hahaha", test_oss_bucket_));

  ASSERT_EQ(OB_SUCCESS, util.check_bucket_lifecycle(uri, storage_info, is_set_lifecycle));
  ASSERT_EQ(true, is_set_lifecycle);
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
