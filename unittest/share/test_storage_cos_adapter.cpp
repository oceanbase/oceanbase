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
#include "share/backup/ob_backup_io_adapter.h"
#include "common/storage/ob_fd_simulator.h"
#include "share/ob_device_manager.h"

using namespace oceanbase::common;

class TestCosShareCommon
{
public:
  TestCosShareCommon()
  {
    databuff_printf(storage_info, sizeof(storage_info), "host=%s&access_id=%s&access_key=%s&appid=%s",
        endpoint, secretid, secretkey, appid);
  }
  ~TestCosShareCommon()
  {}
protected:
  char storage_info[OB_MAX_URI_LENGTH];
  const char* dir_name = "cos_unittest_share_dir";
  char bucket[OB_MAX_URI_LENGTH] = "cos://ob-dbt3-test-1304889018";
  char endpoint[OB_MAX_URI_LENGTH] = "cos.ap-shanghai.myqcloud.com";
  char secretid[OB_MAX_URI_LENGTH] = "";
  char secretkey[OB_MAX_URI_LENGTH] = "";
  char appid[OB_MAX_URI_LENGTH] = "";
  char uri[OB_MAX_URI_LENGTH];
  ObBackupIoAdapter util;
};

//use to clean the env
class TestCosShareCommonCleanOp : public ObBaseDirEntryOperator, public TestCosShareCommon
{
public:
  TestCosShareCommonCleanOp()
  {
  }
  ~TestCosShareCommonCleanOp() {}
  int func(const dirent *entry) override;
private :
};

int TestCosShareCommonCleanOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(uri, sizeof(uri), "%s/%s/%s", bucket, dir_name, entry->d_name))) {
  } else if (OB_FAIL(util.del_file(uri, storage_info))) {
  }
  return ret;
}

class TestStorageCosShare: public ::testing::Test, public TestCosShareCommon
{
public:
  TestStorageCosShare() {}
  virtual ~TestStorageCosShare(){}
  virtual void SetUp()
  {
    //clean the test dir, do it in setup or teardown
    char dir_uri[OB_MAX_URI_LENGTH];
    TestCosShareCommonCleanOp clean_op;
    ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name));
    ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, storage_info, clean_op));
  }
  virtual void TearDown() {}

  static void SetUpTestCase() {}

  static void TearDownTestCase() {}
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestStorageCosShare);
protected:
  // function members
protected:
};

TEST_F(TestStorageCosShare, del_1001)
{
  char dir_uri[OB_MAX_URI_LENGTH];
  ObArray<ObString> del_files;
  ObArenaAllocator allocator;
  ObFileListArrayOp del_op(del_files, allocator);
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, "data/1/yuanzhi.zy.backup"));
  ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, storage_info, del_op));
  //ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, "data/1/yuanzhi.zy.backup.backup"));
  //ASSERT_EQ(OB_SUCCESS, util.list_files(dir_uri, storage_info, del_op));
  for (int i = 0; i < del_files.count(); i++) {
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s%s", dir_uri, del_files[i].ptr()));
    ASSERT_EQ(OB_SUCCESS, util.del_file(uri, storage_info));
  }
}

//COS test
TEST_F(TestStorageCosShare, test_get_pkeys_from_dir)
{
  char dir_uri[OB_MAX_URI_LENGTH];
  char dir_name_sp[OB_MAX_URI_LENGTH];
  const int64_t ts = ObTimeUtility::current_time();

  // format dir path uri
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_name_sp, sizeof(dir_name_sp), "cos_unittest_share_dir/%ld", ts));
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dir_uri, sizeof(dir_uri), "%s/%s/", bucket, dir_name_sp));

  char buffer[1024] = "abcdefghijklmnopqrstuvwxyz";
  // create 10 partitions
  const int partitions_cnt = 10;
  for (int i = 0; i < partitions_cnt; i++) {
    // format object path uri
    ASSERT_EQ(OB_SUCCESS, databuff_printf(uri, sizeof(uri), "%s%ld_%d", dir_uri, ts, i));
    ASSERT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  buffer, strlen(buffer)));
  }

}

TEST_F(TestStorageCosShare, test_set_append_strategy)
{
    ObIODOpts opts;
    ObIODOpt opt[4];
    opts.opts_ = opt;
    opts.opt_cnt_ = 0;
    int64_t epoch = 99;

    ASSERT_EQ(OB_SUCCESS, util.set_append_strategy(&opts, true, epoch, 4));
    ASSERT_EQ(2, opts.opt_cnt_);
    ASSERT_EQ(0, STRCMP(opt[0].key_, "AppendStrategy"));
    ASSERT_EQ(0, STRCMP(opt[0].value_.value_str, "OB_APPEND_USE_SLICE_PUT"));
    ASSERT_EQ(0, STRCMP(opt[1].key_, "AppendVersion"));
    ASSERT_EQ(epoch, opt[1].value_.value_int64);

    opts.opt_cnt_ = 0;
    ASSERT_EQ(OB_SUCCESS, util.set_append_strategy(&opts, false, epoch, 4));
    ASSERT_EQ(1, opts.opt_cnt_);
    ASSERT_EQ(0, STRCMP(opt[0].key_, "AppendStrategy"));
    ASSERT_EQ(0, STRCMP(opt[0].value_.value_str, "OB_APPEND_USE_OVERRITE"));
}

/*TODO: ObBackupIoAdapter::set_append_strategy need UT*/
int main(int argc, char **argv)
{
  system("rm -f test_storage_cos_adapter.log");
  OB_LOGGER.set_file_name("test_storage_cos_adapter.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
