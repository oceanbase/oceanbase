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

#include <string>
#include <algorithm>

#include "lib/ob_errno.h"
#include "storage/backup/ob_backup_tmp_file.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_iterator.h"
#include "storage/backup/ob_backup_index_merger.h"
#include "storage/backup/ob_backup_index_store.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "test_backup.h"
#include "test_backup_include.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

using namespace testing;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;

namespace oceanbase {
namespace backup {

static ObSimpleMemLimitGetter getter;

/* ObMockBackupMacroBlockIndexStore */

class ObMockBackupMacroBlockIndexStore : public ObBackupMacroBlockIndexStore {
public:
  ObMockBackupMacroBlockIndexStore()
  {}
  virtual ~ObMockBackupMacroBlockIndexStore()
  {}

  MOCK_METHOD3(fill_backup_set_descs_,
      int(const uint64_t tenant_id, const int64_t backup_set_id, common::ObMySQLProxy &sql_proxy));
};

/* ObFakeBackupMetaIndexIterator */

class ObFakeBackupMetaIndexIterator : public ObBackupMetaIndexIterator {
public:
  ObFakeBackupMetaIndexIterator();
  virtual ~ObFakeBackupMetaIndexIterator();
  int init(const int64_t task_id, const share::ObBackupDest &backup_dest, const uint64_t tenant_id,
      const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
      const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
      const bool is_sec_meta);
  virtual int next() override;
  virtual bool is_iter_end() const override;

private:
  int generate_random_meta_index_(
      const int64_t backup_set_id, const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id);
  void make_random_meta_index_(const int64_t tablet_id, const ObBackupMetaType &meta_type, const int64_t backup_set_id,
      const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id, const int64_t file_id,
      ObBackupMetaIndex &range_index);

private:
  int64_t cur_tablet_id_;
  int64_t random_count_;
  DISALLOW_COPY_AND_ASSIGN(ObFakeBackupMetaIndexIterator);
};

ObFakeBackupMetaIndexIterator::ObFakeBackupMetaIndexIterator() : cur_tablet_id_(0), random_count_(0)
{}

ObFakeBackupMetaIndexIterator::~ObFakeBackupMetaIndexIterator()
{}

int ObFakeBackupMetaIndexIterator::init(const int64_t task_id, const share::ObBackupDest &backup_dest,
    const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
    const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
    const bool is_sec_meta)
{
  int ret = OB_SUCCESS;
  max_tablet_id = 0;
  task_id_ = task_id;
  backup_dest_.deep_copy(backup_dest);
  tenant_id_ = tenant_id;
  backup_set_desc_ = backup_set_desc;
  ls_id_ = ls_id;
  backup_data_type_ = backup_data_type;
  turn_id_ = turn_id;
  retry_id_ = retry_id;
  cur_idx_ = 0;
  cur_file_id_ = -1;
  is_sec_meta_ = is_sec_meta;
  generate_random_meta_index_(backup_set_desc.backup_set_id_, ls_id, turn_id, retry_id);
  is_inited_ = true;
  return ret;
}

int ObFakeBackupMetaIndexIterator::next()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator do not init", K(ret));
  } else if (cur_idx_ >= random_count_) {
    ret = OB_ITER_END;
    LOG_WARN("iter end", K(ret), K_(cur_idx), K_(cur_index_list));
  } else {
    cur_idx_++;
  }
  return ret;
}

bool ObFakeBackupMetaIndexIterator::is_iter_end() const
{
  bool bret = false;
  bret = cur_idx_ >= cur_index_list_.count() || -1 == cur_idx_;
  return bret;
}

int ObFakeBackupMetaIndexIterator::generate_random_meta_index_(
    const int64_t backup_set_id, const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id)
{
  int ret = OB_SUCCESS;
  ObBackupMetaIndex meta_index;
  const int64_t file_count = 80;
  for (int64_t i = 0; OB_SUCC(ret) && i < file_count; ++i) {
    int64_t item_count = 1024;
    const int64_t file_id = i;
    for (int64_t j = 0; OB_SUCC(ret) && j < item_count; ++j) {
      cur_tablet_id_++;
      meta_index.reset();
      make_random_meta_index_(
          cur_tablet_id_, BACKUP_SSTABLE_META, backup_set_id, ls_id, turn_id, retry_id, file_id, meta_index);
      ret = cur_index_list_.push_back(meta_index);
      EXPECT_EQ(OB_SUCCESS, ret);
      make_random_meta_index_(
          cur_tablet_id_, BACKUP_TABLET_META, backup_set_id, ls_id, turn_id, retry_id, file_id, meta_index);
      ret = cur_index_list_.push_back(meta_index);
      EXPECT_EQ(OB_SUCCESS, ret);
      random_count_ += 2;
      max_tablet_id = std::max(max_tablet_id, cur_tablet_id_);
    }
  }
  return ret;
}

void ObFakeBackupMetaIndexIterator::make_random_meta_index_(const int64_t tablet_id, const ObBackupMetaType &meta_type,
    const int64_t backup_set_id, const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id,
    const int64_t file_id, ObBackupMetaIndex &meta_index)
{
  meta_index.meta_key_.tablet_id_ = ObTabletID(tablet_id);
  meta_index.meta_key_.meta_type_ = meta_type;
  meta_index.backup_set_id_ = backup_set_id;
  meta_index.ls_id_ = ls_id;
  meta_index.turn_id_ = turn_id;
  meta_index.retry_id_ = retry_id;
  meta_index.file_id_ = file_id;
  make_random_offset(meta_index.offset_);
  make_random_offset(meta_index.length_);
}

/* ObFakeBackupMetaIndexMerger */

class ObFakeBackupMetaIndexMerger : public ObBackupMetaIndexMerger {
public:
  ObFakeBackupMetaIndexMerger();
  virtual ~ObFakeBackupMetaIndexMerger();

private:
  virtual int get_all_retries_(const int64_t task_id, const uint64_t tenant_id,
      const share::ObBackupDataType &backup_data_type, const share::ObLSID &ls_id, common::ObISQLClient &sql_proxy,
      common::ObIArray<ObBackupRetryDesc> &retry_list) override;
  virtual int alloc_merge_iter_(const ObBackupIndexMergeParam &merge_param, const ObBackupRetryDesc &retry_desc,
      const bool is_sec_meta, ObBackupMetaIndexIterator *&iter) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFakeBackupMetaIndexMerger);
};

ObFakeBackupMetaIndexMerger::ObFakeBackupMetaIndexMerger()
{}

ObFakeBackupMetaIndexMerger::~ObFakeBackupMetaIndexMerger()
{}

int ObFakeBackupMetaIndexMerger::get_all_retries_(const int64_t task_id, const uint64_t tenant_id,
    const share::ObBackupDataType &backup_data_type, const share::ObLSID &ls_id, common::ObISQLClient &sql_proxy,
    common::ObIArray<ObBackupRetryDesc> &retry_list)
{
  int ret = OB_SUCCESS;
  UNUSEDx(task_id, tenant_id, backup_data_type, sql_proxy);
  retry_list.reset();
  ObBackupRetryDesc desc;
  desc.ls_id_ = ls_id;
  desc.turn_id_ = 1;
  desc.retry_id_ = 0;
  desc.last_file_id_ = 10;
  if (OB_FAIL(retry_list.push_back(desc))) {
    LOG_WARN("failed to push back", K(ret), K(desc));
  } else {
    LOG_INFO("fake get all retries", K(retry_list));
  }
  return ret;
}

int ObFakeBackupMetaIndexMerger::alloc_merge_iter_(const ObBackupIndexMergeParam &merge_param,
    const ObBackupRetryDesc &retry_desc, const bool is_sec_meta, ObBackupMetaIndexIterator *&iter)
{
  int ret = OB_SUCCESS;
  ObFakeBackupMetaIndexIterator *tmp_iter = NULL;
  if (!retry_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret));
  } else if (OB_ISNULL(tmp_iter = OB_NEW(ObFakeBackupMetaIndexIterator, ObModIds::BACKUP))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc iterator", K(ret));
  } else if (OB_FAIL(tmp_iter->init(merge_param.task_id_,
                 merge_param.backup_dest_,
                 merge_param.tenant_id_,
                 merge_param.backup_set_desc_,
                 retry_desc.ls_id_,
                 merge_param.backup_data_type_,
                 retry_desc.turn_id_,
                 retry_desc.retry_id_,
                 is_sec_meta))) {
    LOG_WARN("failed to init meta index iterator", K(ret), K(merge_param));
  } else {
    iter = tmp_iter;
  }
  return ret;
}

class TestBackupIndexMerger : public TestDataFilePrepare {
public:
  TestBackupIndexMerger();
  virtual ~TestBackupIndexMerger();
  virtual void SetUp();
  virtual void TearDown();

private:
  void fake_init_meta_index_merger_(ObFakeBackupMetaIndexMerger &merger);
  void fake_init_macro_index_merger_(ObFakeBackupMacroIndexMerger &merger);
  void init_backup_meta_index_store_(ObBackupMetaIndexStore &meta_index_store);
  void init_backup_macro_index_store_(ObMockBackupMacroBlockIndexStore &macro_index_store);
  void iterate_meta_index_store_(
      const int64_t start_id, const int64_t end_id, ObBackupMetaIndexStore &meta_index_store);
  void iterate_macro_index_store_(
      const int64_t start_id, const int64_t end_id, ObBackupMacroBlockIndexStore &macro_index_store);
  void build_backup_index_merge_param_(ObBackupIndexMergeParam &merge_param);
  void build_backup_index_store_param_(ObBackupIndexStoreParam &store_param);
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
  DISALLOW_COPY_AND_ASSIGN(TestBackupIndexMerger);
};

TestBackupIndexMerger::TestBackupIndexMerger()
    : TestDataFilePrepare(&getter, "TestBackupIndexMerger", OB_DEFAULT_MACRO_BLOCK_SIZE, 800),
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

TestBackupIndexMerger::~TestBackupIndexMerger()
{}

void TestBackupIndexMerger::SetUp()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  TestDataFilePrepare::SetUp();

  ret = getter.add_tenant(1, 8L * 1024L * 1024L, 2L * 1024L * 1024L * 1024L);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    EXPECT_EQ(OB_SUCCESS, ret);
  }
  // set observer memory limit
  CHUNK_MGR.set_limit(8L * 1024L * 1024L * 1024L);
  ret = ObTmpFileManager::get_instance().init();
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  static ObTenantBase tenant_ctx(1);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  inner_init_();
}

void TestBackupIndexMerger::TearDown()
{
  ObTmpFileManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
}

void TestBackupIndexMerger::inner_init_()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ret = databuff_printf(test_dir_, sizeof(test_dir_), "%s/test_backup_index_merger_dir", get_current_dir_name());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_);
  EXPECT_EQ(OB_SUCCESS, ret);
  clean_env_();
  ret = backup_dest_.set(test_dir_uri_);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, backup_dest_.get_storage_info()));
  task_id_ = 1;
  job_desc_.job_id_ = 1;
  job_desc_.task_id_ = 1;
  backup_set_desc_.backup_set_id_ = 1;
  backup_set_desc_.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  backup_data_type_.set_major_data_backup();
  incarnation_ = 1;
  tenant_id_ = 1;
  dest_id_ = 1;
  ls_id_ = ObLSID(1001);
  turn_id_ = 1;
  retry_id_ = 0;
  ret = throttle_.init(10);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = OB_BACKUP_INDEX_CACHE.init();
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    EXPECT_EQ(OB_SUCCESS, ret);
  }
}

void TestBackupIndexMerger::fake_init_meta_index_merger_(ObFakeBackupMetaIndexMerger &merger)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy sql_proxy;
  ObBackupIndexMergeParam merge_param;
  build_backup_index_merge_param_(merge_param);
  const bool is_sec_meta = false;
  ret = merger.init(merge_param, is_sec_meta, sql_proxy);
  EXPECT_EQ(OB_SUCCESS, ret);
}

void TestBackupIndexMerger::fake_init_macro_index_merger_(ObFakeBackupMacroIndexMerger &merger)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy sql_proxy;
  ObBackupIndexMergeParam merge_param;
  build_backup_index_merge_param_(merge_param);
  const int64_t file_count = 80;
  const int64_t per_file_item_count = 1024;
  merger.set_count(file_count, per_file_item_count);
  ret = merger.init(merge_param, sql_proxy);
  EXPECT_EQ(OB_SUCCESS, ret);
}

void TestBackupIndexMerger::init_backup_meta_index_store_(ObBackupMetaIndexStore &meta_index_store)
{
  int ret = OB_SUCCESS;
  ObBackupRestoreMode mode = ObBackupRestoreMode::BACKUP_MODE;
  ObBackupIndexStoreParam store_param;
  build_backup_index_store_param_(store_param);
  const bool is_sec_meta = false;
  ret = meta_index_store.init(mode, store_param, backup_dest_, backup_set_desc_, is_sec_meta, OB_BACKUP_INDEX_CACHE);
  EXPECT_EQ(OB_SUCCESS, ret);
}

void TestBackupIndexMerger::init_backup_macro_index_store_(ObMockBackupMacroBlockIndexStore &macro_index_store)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy sql_proxy;
  ObBackupRestoreMode mode = ObBackupRestoreMode::BACKUP_MODE;
  ObBackupIndexStoreParam store_param;
  build_backup_index_store_param_(store_param);
  ret = macro_index_store.init(mode, store_param, backup_dest_, backup_set_desc_, OB_BACKUP_INDEX_CACHE, sql_proxy);
  EXPECT_EQ(OB_SUCCESS, ret);
}

void TestBackupIndexMerger::build_backup_index_merge_param_(ObBackupIndexMergeParam &merge_param)
{
  merge_param.task_id_ = task_id_;
  merge_param.backup_dest_.deep_copy(backup_dest_);
  merge_param.tenant_id_ = tenant_id_;
  merge_param.dest_id_ = dest_id_;
  merge_param.backup_set_desc_ = backup_set_desc_;
  merge_param.backup_data_type_ = backup_data_type_;
  merge_param.index_level_ = ObBackupIndexLevel::BACKUP_INDEX_LEVEL_LOG_STREAM;
  merge_param.ls_id_ = ls_id_;
  merge_param.turn_id_ = turn_id_;
  merge_param.retry_id_ = retry_id_;
}

void TestBackupIndexMerger::build_backup_index_store_param_(ObBackupIndexStoreParam &store_param)
{
  store_param.index_level_ = ObBackupIndexLevel::BACKUP_INDEX_LEVEL_LOG_STREAM;
  store_param.tenant_id_ = tenant_id_;
  store_param.backup_set_id_ = backup_set_desc_.backup_set_id_;
  store_param.ls_id_ = ls_id_;
  store_param.is_tenant_level_ = false;
  store_param.backup_data_type_ = backup_data_type_;
  store_param.turn_id_ = turn_id_;
  store_param.retry_id_ = retry_id_;
}

void TestBackupIndexMerger::iterate_meta_index_store_(
    const int64_t start_id, const int64_t end_id, ObBackupMetaIndexStore &meta_index_store)
{
  int ret = OB_SUCCESS;
  for (int64_t i = start_id; OB_SUCC(ret) && i <= end_id; i = i + 8) {
    const common::ObTabletID &tablet_id = ObTabletID(i);
    ObBackupMetaIndex tablet_meta_index;
    ret = meta_index_store.get_backup_meta_index(tablet_id, BACKUP_TABLET_META, tablet_meta_index);
    ASSERT_EQ(OB_SUCCESS, ret);
    ObBackupMetaIndex sstable_meta_index;
    ret = meta_index_store.get_backup_meta_index(tablet_id, BACKUP_SSTABLE_META, sstable_meta_index);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

void TestBackupIndexMerger::iterate_macro_index_store_(
    const int64_t start_id, const int64_t end_id, ObBackupMacroBlockIndexStore &macro_index_store)
{
  int ret = OB_SUCCESS;
  for (int64_t i = start_id; OB_SUCC(ret) && i <= end_id; i = i + 8) {
    blocksstable::ObLogicMacroBlockId logic_id(1, 1, i);
    ObBackupMacroRangeIndex range_index;
    ret = macro_index_store.get_macro_range_index(logic_id, range_index);
    LOG_INFO("get macro range index", K(logic_id), K(range_index));
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

void TestBackupIndexMerger::clean_env_()
{
  system((std::string("rm -rf ") + test_dir_ + std::string("*")).c_str());
}

TEST_F(TestBackupIndexMerger, test_backup_meta_index_merger)
{
  int ret = OB_SUCCESS;
  clean_env_();
  ObFakeBackupMetaIndexMerger merger;
  fake_init_meta_index_merger_(merger);
  ret = merger.merge_index();
  EXPECT_EQ(OB_SUCCESS, ret);
  ObBackupMetaIndexStore index_store;
  init_backup_meta_index_store_(index_store);
  const int64_t start_id = 1;
  const int64_t end_id = max_tablet_id;
  iterate_meta_index_store_(start_id, end_id, index_store);
}

TEST_F(TestBackupIndexMerger, test_backup_macro_index_merger)
{
  int ret = OB_SUCCESS;
  clean_env_();
  ObFakeBackupMacroIndexMerger merger;
  fake_init_macro_index_merger_(merger);
  ret = merger.merge_index();
  EXPECT_EQ(OB_SUCCESS, ret);
  ObMockBackupMacroBlockIndexStore index_store;
  init_backup_macro_index_store_(index_store);
  const int64_t start_id = 1;
  const int64_t end_id = max_tablet_id;
  iterate_macro_index_store_(start_id, end_id, index_store);
}

TEST_F(TestBackupIndexMerger, test_backup_meta_index_same_meta_index_different_retry)
{
  ObBackupMetaIndex meta_index1;
  meta_index1.meta_key_.tablet_id_ = ObTabletID(200001);
  meta_index1.meta_key_.meta_type_ = BACKUP_SSTABLE_META;
  meta_index1.turn_id_ = 1;
  meta_index1.retry_id_ = 0;
  ObBackupMetaIndexIterator iter1;
  iter1.is_sec_meta_ = false;
  iter1.cur_idx_ = 0;
  iter1.cur_file_id_ = 0;
  iter1.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, iter1.file_id_list_.push_back(0));
  ASSERT_EQ(OB_SUCCESS, iter1.cur_index_list_.push_back(meta_index1));

  ObBackupMetaIndex meta_index2;
  meta_index2.meta_key_.tablet_id_ = ObTabletID(200001);
  meta_index2.meta_key_.meta_type_ = BACKUP_SSTABLE_META;
  meta_index2.turn_id_ = 1;
  meta_index2.retry_id_ = 1;
  ObBackupMetaIndexIterator iter2;
  iter2.is_sec_meta_ = false;
  iter2.cur_idx_ = 0;
  iter2.cur_file_id_ = 0;
  iter2.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, iter2.file_id_list_.push_back(0));
  ASSERT_EQ(OB_SUCCESS, iter2.cur_index_list_.push_back(meta_index2));

  typedef ObSEArray<ObBackupMetaIndexIterator *, 2> MERGE_ITER_ARRAY;
  MERGE_ITER_ARRAY merge_iter_array;
  ASSERT_EQ(OB_SUCCESS, merge_iter_array.push_back(&iter1));
  ASSERT_EQ(OB_SUCCESS, merge_iter_array.push_back(&iter2));
  MERGE_ITER_ARRAY min_iters;

  ObBackupMetaIndexMerger merger;
  merger.find_minimum_iters_(merge_iter_array, min_iters);
  ASSERT_EQ(2, min_iters.count());
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_index_merger.log*");
  OB_LOGGER.set_file_name("test_backup_index_merger.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
