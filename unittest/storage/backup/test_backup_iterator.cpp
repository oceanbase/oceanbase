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

#define USING_LOG_PREFIX STORAGE
#include "gtest/gtest.h"
#define private public
#define protected public

#include "lib/ob_errno.h"
#include "storage/backup/ob_backup_tmp_file.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_iterator.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "test_backup.h"
#include "test_backup_include.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "lib/string/ob_string.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;

namespace oceanbase {
namespace backup {

static ObSimpleMemLimitGetter getter;

class TestBackupIndexIterator : public TestDataFilePrepare {
public:
  TestBackupIndexIterator();
  virtual ~TestBackupIndexIterator();
  virtual void SetUp();
  virtual void TearDown();

private:
  void fake_init_macro_index_merger_(const int64_t file_count, const int64_t per_file_item_count,
    ObFakeBackupMacroIndexMerger &merger);
  void build_backup_index_merge_param_(ObBackupIndexMergeParam &merge_param);
  void init_macro_range_index_iterator(ObBackupMacroRangeIndexIterator &iter);
  void iterate_macro_range_index_iterator(const int64_t start_id, const int64_t end_id,
      ObBackupMacroRangeIndexIterator &iter);
  void inner_init_();
  void clean_env_();

protected:
  ObTenantBase tenant_base_;
  ObBackupJobDesc job_desc_;
  ObBackupDest backup_dest_;
  int64_t task_id_;
  int64_t incarnation_;
  int64_t tenant_id_;
  int64_t dest_id_;
  ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  ObBackupDataType backup_data_type_;
  int64_t turn_id_;
  int64_t retry_id_;
  common::ObInOutBandwidthThrottle throttle_;
  char test_dir_[OB_MAX_URI_LENGTH];
  char test_dir_uri_[OB_MAX_URI_LENGTH];
  ObInOutBandwidthThrottle bandwidth_throttle_;
  DISALLOW_COPY_AND_ASSIGN(TestBackupIndexIterator);
};


TestBackupIndexIterator::TestBackupIndexIterator()
    : TestDataFilePrepare(&getter, "TestBackupIndexIterator", OB_DEFAULT_MACRO_BLOCK_SIZE, 500),
      tenant_base_(500),
      job_desc_(),
      backup_dest_(),
      incarnation_(),
      tenant_id_(OB_INVALID_ID),
      dest_id_(0),
      backup_set_desc_(),
      ls_id_(),
      backup_data_type_(),
      turn_id_(-1),
      retry_id_(-1),
      test_dir_(""),
      test_dir_uri_("")
{}

TestBackupIndexIterator::~TestBackupIndexIterator()
{}

void TestBackupIndexIterator::SetUp()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  TestDataFilePrepare::SetUp();

  ret = getter.add_tenant(1, 8L * 1024L * 1024L, 2L * 1024L * 1024L * 1024L);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  // set observer memory limit
  CHUNK_MGR.set_limit(8L * 1024L * 1024L * 1024L);
  ret = ObTmpFileManager::get_instance().init();
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  EXPECT_EQ(OB_SUCCESS, io_service->start());
  tenant_ctx.set(io_service);
  ObTenantEnv::set_tenant(&tenant_ctx);
  inner_init_();
  ASSERT_EQ(OB_SUCCESS, bandwidth_throttle_.init(1024 * 1024 * 60));
}

void TestBackupIndexIterator::TearDown()
{
  ObTmpFileManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
}

void TestBackupIndexIterator::inner_init_()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ret = databuff_printf(test_dir_, sizeof(test_dir_), "%s/test_backup_index_iterator_dir", get_current_dir_name());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_);
  ASSERT_EQ(OB_SUCCESS, ret);
  clean_env_();
  ret = backup_dest_.set(test_dir_uri_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, backup_dest_.get_storage_info()));
  task_id_ = 1;
  job_desc_.job_id_ = 1;
  job_desc_.task_id_ = 1;
  backup_set_desc_.backup_set_id_ = 1;
  backup_set_desc_.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  backup_data_type_.set_major_data_backup();
  incarnation_ = 1;
  tenant_id_ = 1;
  dest_id_ = 1;
  ls_id_ = ObLSID(0);
  turn_id_ = 1;
  retry_id_ = 0;
  ret = throttle_.init(10);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_BACKUP_INDEX_CACHE.init();
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

void TestBackupIndexIterator::fake_init_macro_index_merger_(const int64_t file_count,
  const int64_t per_file_item_count, ObFakeBackupMacroIndexMerger &merger)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy sql_proxy;
  ObBackupIndexMergeParam merge_param;
  build_backup_index_merge_param_(merge_param);
  merger.set_count(file_count, per_file_item_count);
  ret = merger.init(merge_param, sql_proxy, bandwidth_throttle_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestBackupIndexIterator::build_backup_index_merge_param_(ObBackupIndexMergeParam &merge_param)
{
  merge_param.task_id_ = task_id_;
  merge_param.backup_dest_.deep_copy(backup_dest_);
  merge_param.tenant_id_ = tenant_id_;
  merge_param.dest_id_ = dest_id_;
  merge_param.backup_set_desc_ = backup_set_desc_;
  merge_param.backup_data_type_ = backup_data_type_;
  merge_param.index_level_ = ObBackupIndexLevel::BACKUP_INDEX_LEVEL_TENANT;
  merge_param.ls_id_ = ls_id_;
  merge_param.turn_id_ = turn_id_;
  merge_param.retry_id_ = retry_id_;
}

void TestBackupIndexIterator::init_macro_range_index_iterator(ObBackupMacroRangeIndexIterator &iter)
{
  int ret = OB_SUCCESS;
  ret = iter.init(task_id_, backup_dest_, tenant_id_, backup_set_desc_, ls_id_, backup_data_type_,
      turn_id_, retry_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestBackupIndexIterator::iterate_macro_range_index_iterator(
     const int64_t start_id, const int64_t end_id, ObBackupMacroRangeIndexIterator &iter)
{
  int ret = OB_SUCCESS;
  int64_t idx = start_id;
  ObBackupMacroRangeIndex range_index;
  while (OB_SUCC(ret)) {
    ret = iter.get_cur_index(range_index);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("iterator meet end", K(idx), K(start_id), K(end_id));
      ASSERT_EQ(idx, end_id + 1);
      break;
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(range_index.end_key_.tablet_id_, idx);
    idx++;
    ret = iter.next();
  }
}

void TestBackupIndexIterator::clean_env_()
{
  system((std::string("rm -rf ") + test_dir_ + std::string("*")).c_str());
}

TEST_F(TestBackupIndexIterator, test_extract_backup_file_id)
{
  int ret = OB_SUCCESS;
  bool file_match = false;
  int64_t tmp_file_id = -1;
  common::ObString prefix = "macro_block_data";

  common::ObString file_name1 = "macro_block_data.0";
  ObIBackupIndexIterator::extract_backup_file_id_(file_name1, prefix, tmp_file_id, file_match);
  ASSERT_EQ(true, file_match);
  ASSERT_EQ(0, tmp_file_id);

  common::ObString file_name2 = "macro_block_data0";
  ObIBackupIndexIterator::extract_backup_file_id_(file_name2, prefix, tmp_file_id, file_match);
  ASSERT_EQ(false, file_match);

  common::ObString file_name3 = "macro_block_clog.0";
  ObIBackupIndexIterator::extract_backup_file_id_(file_name3, prefix, tmp_file_id, file_match);
  ASSERT_EQ(false, file_match);

  common::ObString file_name4 = "macro_block_data.10";
  ObIBackupIndexIterator::extract_backup_file_id_(file_name4, prefix, tmp_file_id, file_match);
  ASSERT_EQ(true, file_match);
  ASSERT_EQ(10, tmp_file_id);
}

TEST_F(TestBackupIndexIterator, test_backup_macro_range_index_iterator_921_KB)
{
  int ret = OB_SUCCESS;
  clean_env_();
  ObFakeBackupMacroIndexMerger merger;
  const int64_t file_count = 20;
  const int64_t per_file_item_count = 1024;
  fake_init_macro_index_merger_(file_count, per_file_item_count, merger);
  ret = merger.merge_index();
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t start_id = 1;
  const int64_t end_id = file_count * per_file_item_count;
  ObBackupMacroRangeIndexIterator range_index_iterator;
  init_macro_range_index_iterator(range_index_iterator);
  iterate_macro_range_index_iterator(start_id, end_id, range_index_iterator);
}

TEST_F(TestBackupIndexIterator, test_backup_macro_range_index_iterator_1_8_MB)
{
  int ret = OB_SUCCESS;
  clean_env_();
  ObFakeBackupMacroIndexMerger merger;
  const int64_t file_count = 40;
  const int64_t per_file_item_count = 1024;
  fake_init_macro_index_merger_(file_count, per_file_item_count, merger);
  ret = merger.merge_index();
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t start_id = 1;
  const int64_t end_id = file_count * per_file_item_count;
  ObBackupMacroRangeIndexIterator range_index_iterator;
  init_macro_range_index_iterator(range_index_iterator);
  iterate_macro_range_index_iterator(start_id, end_id, range_index_iterator);
}

TEST_F(TestBackupIndexIterator, test_backup_macro_range_index_iterator_2_8_MB)
{
  int ret = OB_SUCCESS;
  clean_env_();
  ObFakeBackupMacroIndexMerger merger;
  const int64_t file_count = 60;
  const int64_t per_file_item_count = 1024;
  fake_init_macro_index_merger_(file_count, per_file_item_count, merger);
  ret = merger.merge_index();
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t start_id = 1;
  const int64_t end_id = file_count * per_file_item_count;
  ObBackupMacroRangeIndexIterator range_index_iterator;
  init_macro_range_index_iterator(range_index_iterator);
  iterate_macro_range_index_iterator(start_id, end_id, range_index_iterator);
}

TEST_F(TestBackupIndexIterator, test_backup_macro_range_index_iterator_3_7_MB)
{
  int ret = OB_SUCCESS;
  clean_env_();
  ObFakeBackupMacroIndexMerger merger;
  const int64_t file_count = 80;
  const int64_t per_file_item_count = 1024;
  fake_init_macro_index_merger_(file_count, per_file_item_count, merger);
  ret = merger.merge_index();
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t start_id = 1;
  const int64_t end_id = file_count * per_file_item_count;
  ObBackupMacroRangeIndexIterator range_index_iterator;
  init_macro_range_index_iterator(range_index_iterator);
  iterate_macro_range_index_iterator(start_id, end_id, range_index_iterator);
}

TEST_F(TestBackupIndexIterator, test_backup_macro_range_index_iterator_4_6_MB)
{
  int ret = OB_SUCCESS;
  clean_env_();
  ObFakeBackupMacroIndexMerger merger;
  const int64_t file_count = 100;
  const int64_t per_file_item_count = 1024;
  fake_init_macro_index_merger_(file_count, per_file_item_count, merger);
  ret = merger.merge_index();
  ASSERT_EQ(OB_SUCCESS, ret);
  const int64_t start_id = 1;
  const int64_t end_id = file_count * per_file_item_count;
  ObBackupMacroRangeIndexIterator range_index_iterator;
  init_macro_range_index_iterator(range_index_iterator);
  iterate_macro_range_index_iterator(start_id, end_id, range_index_iterator);
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_iterator.log*");
  OB_LOGGER.set_file_name("test_backup_iterator.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
