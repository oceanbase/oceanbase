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
#define private public
#define protected public

#include "observer/omt/ob_tenant_mtl_helper.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/blocksstable/ob_data_file_prepare.h"

namespace oceanbase
{
namespace backup
{

static ObSimpleMemLimitGetter getter;

class FakeMacroBlockIndexIterator : public ObIMacroBlockIndexIterator
{
public:
  FakeMacroBlockIndexIterator() {}
  virtual ~FakeMacroBlockIndexIterator() {}
  virtual ObBackupIndexIteratorType get_type() const
  {
    return BACKUP_ORDERED_MACRO_BLOCK_INDEX_ITERATOR;
  }
  virtual int next()
  {
    cur_idx_++;
    return OB_SUCCESS;
  }
  virtual bool is_iter_end() const
  {
    return cur_idx_ > total_count_;
  }
  virtual int get_cur_index(ObBackupMacroRangeIndex &index) override
  {
    return OB_NOT_SUPPORTED;
  }
  virtual int get_cur_index(ObBackupMacroBlockIndex &index) override
  {
    index.logic_id_.tablet_id_ = 200001;
    index.logic_id_.logic_version_ = cur_idx_;
    index.logic_id_.data_seq_ = cur_idx_;
    index.backup_set_id_ = 1;
    index.ls_id_ = ObLSID(1001);
    index.turn_id_ = 1;
    index.retry_id_ = 0;
    index.file_id_ = cur_idx_ % 2048;
    index.offset_ = cur_idx_ * 4096;
    index.length_ = (cur_idx_) * (cur_idx_ + 1) * 4096;
    index.reusable_ = true;
    return OB_SUCCESS;
  }
private:
  int64_t cur_idx_ = 1;
  int64_t total_count_ = 10000;
};

class FakeUnorderedMacroBlockIndexMerger : public ObBackupUnorderdMacroBlockIndexMerger {
public:
  virtual int get_all_retries_(const int64_t task_id, const uint64_t tenant_id, const share::ObBackupDataType &backup_data_type,
      const share::ObLSID &ls_id, common::ObISQLClient &sql_proxy, common::ObIArray<ObBackupRetryDesc> &retry_list) override
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

  virtual int get_prev_tenant_index_retry_id_(const ObBackupIndexMergeParam &merge_param,
      const share::ObBackupSetDesc &prev_backup_set_desc, const int64_t prev_turn_id, int64_t &retry_id) override
  {
    retry_id = 0;
    return OB_SUCCESS;
  }

  virtual int prepare_macro_block_iterators_(
      const ObBackupIndexMergeParam &merge_param, const common::ObIArray<ObBackupRetryDesc> &retry_list,
      common::ObISQLClient &sql_proxy, common::ObIArray<ObIMacroBlockIndexIterator *> &iterators) override
  {
    int ret = OB_SUCCESS;
    const int64_t task_id = merge_param.task_id_;
    const ObBackupDest &backup_dest = merge_param.backup_dest_;
    const ObBackupSetDesc &backup_set_desc = merge_param.backup_set_desc_;
    const uint64_t tenant_id = merge_param.tenant_id_;
    const ObBackupDataType &backup_data_type = merge_param.backup_data_type_;
    const share::ObLSID &ls_id = ObLSID(1001);
    const int64_t turn_id = 1;
    const int64_t retry_id = 0;
    const ObBackupIndexIteratorType type = BACKUP_ORDERED_MACRO_BLOCK_INDEX_ITERATOR;
    FakeMacroBlockIndexIterator *tmp_iter = new FakeMacroBlockIndexIterator;
    if (OB_FAIL(iterators.push_back(tmp_iter))) {
      LOG_WARN("failed to push back", K(ret));
    }
    return ret;
  }
};

class TestBackupMacroIndexMerger : public TestDataFilePrepare {
public:
  TestBackupMacroIndexMerger();
  virtual ~TestBackupMacroIndexMerger();
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
    // ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
    // EXPECT_EQ(0, ObIOManager::get_instance().init(1024L * 1024L * 1024L, 10));
  }
  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }

private:
  void inner_init_();
  void clean_env_();
  void fake_init_macro_index_merger_(FakeUnorderedMacroBlockIndexMerger &merger);
  void build_backup_index_merge_param_(ObBackupIndexMergeParam &merge_param);
  void build_backup_index_store_param_(ObBackupIndexStoreParam &store_param);
  void init_backup_macro_index_store_(ObBackupOrderedMacroBlockIndexStore &macro_index_store);

protected:
  ObTenantBase tenant_base_;
  ObBackupJobDesc job_desc_;
  ObBackupDest backup_dest_;
  int64_t task_id_;
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
  DISALLOW_COPY_AND_ASSIGN(TestBackupMacroIndexMerger);
};

TestBackupMacroIndexMerger::TestBackupMacroIndexMerger()
    : TestDataFilePrepare(&getter, "TestBackupMacroIndexMerger", OB_DEFAULT_MACRO_BLOCK_SIZE, 800),
      tenant_base_(500),
      job_desc_(),
      backup_dest_(),
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

TestBackupMacroIndexMerger::~TestBackupMacroIndexMerger()
{}

void TestBackupMacroIndexMerger::SetUp()
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
  ASSERT_EQ(OB_SUCCESS, tmp_file::ObTmpPageCache::get_instance().init("sn_tmp_page_cache", 1));

  static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  EXPECT_EQ(OB_SUCCESS, io_service->start());
  tenant_ctx.set(io_service);

  ObTimerService *timer_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTimerService::mtl_new(timer_service));
  EXPECT_EQ(OB_SUCCESS, ObTimerService::mtl_start(timer_service));
  tenant_ctx.set(timer_service);

  tmp_file::ObTenantTmpFileManager *tf_mgr = nullptr;
  EXPECT_EQ(OB_SUCCESS, mtl_new_default(tf_mgr));
  EXPECT_EQ(OB_SUCCESS, tmp_file::ObTenantTmpFileManager::mtl_init(tf_mgr));
  tf_mgr->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.default_wbp_memory_limit_ = 40*1024*1024;
  EXPECT_EQ(OB_SUCCESS, tf_mgr->start());
  tenant_ctx.set(tf_mgr);

  ObTenantEnv::set_tenant(&tenant_ctx);
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
  inner_init_();
}

void TestBackupMacroIndexMerger::TearDown()
{
  tmp_file::ObTmpPageCache::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  common::ObClockGenerator::destroy();
  ObTimerService *timer_service = MTL(ObTimerService *);
  ASSERT_NE(nullptr, timer_service);
  timer_service->stop();
  timer_service->wait();
  timer_service->destroy();
}

void TestBackupMacroIndexMerger::clean_env_()
{
  system((std::string("rm -rf ") + test_dir_ + std::string("*")).c_str());
}

void TestBackupMacroIndexMerger::inner_init_()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ret = databuff_printf(test_dir_, sizeof(test_dir_), "%s/test_backup_macro_block_index_merger_dir", get_current_dir_name());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = databuff_printf(test_dir_uri_, sizeof(test_dir_uri_), "file://%s", test_dir_);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = backup_dest_.set(test_dir_uri_);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(OB_SUCCESS, util.mkdir(test_dir_uri_, backup_dest_.get_storage_info()));
  task_id_ = 1;
  job_desc_.job_id_ = 1;
  job_desc_.task_id_ = 1;
  backup_set_desc_.backup_set_id_ = 1;
  backup_set_desc_.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  backup_data_type_.set_user_data_backup();
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

void TestBackupMacroIndexMerger::build_backup_index_merge_param_(ObBackupIndexMergeParam &merge_param)
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
  merge_param.compressor_type_ = ObCompressorType::ZSTD_1_3_8_COMPRESSOR;
}

void TestBackupMacroIndexMerger::build_backup_index_store_param_(ObBackupIndexStoreParam &store_param)
{
  store_param.index_level_ = ObBackupIndexLevel::BACKUP_INDEX_LEVEL_LOG_STREAM;
  store_param.tenant_id_ = tenant_id_;
  store_param.backup_set_id_ = backup_set_desc_.backup_set_id_;
  store_param.ls_id_ = ls_id_;
  store_param.is_tenant_level_ = false;
  store_param.backup_data_type_ = backup_data_type_;
  store_param.turn_id_ = turn_id_;
  store_param.retry_id_ = retry_id_;
  store_param.dest_id_ = dest_id_;
}

void TestBackupMacroIndexMerger::fake_init_macro_index_merger_(FakeUnorderedMacroBlockIndexMerger &merger)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy sql_proxy;
  ObBackupIndexMergeParam merge_param;
  build_backup_index_merge_param_(merge_param);
  ret = merger.init(merge_param, sql_proxy, throttle_);
  EXPECT_EQ(OB_SUCCESS, ret);
}

void TestBackupMacroIndexMerger::init_backup_macro_index_store_(ObBackupOrderedMacroBlockIndexStore &macro_index_store)
{
  int ret = OB_SUCCESS;
  ObBackupRestoreMode mode = ObBackupRestoreMode::BACKUP_MODE;
  ObBackupIndexStoreParam store_param;
  build_backup_index_store_param_(store_param);
  ret = macro_index_store.init(mode, store_param, backup_dest_, backup_set_desc_, OB_BACKUP_INDEX_CACHE);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestBackupMacroIndexMerger, test_backup_macro_block_index_merger)
{
  int ret = OB_SUCCESS;
  clean_env_();
  FakeUnorderedMacroBlockIndexMerger merger;
  fake_init_macro_index_merger_(merger);
  ret = merger.merge_index();
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestBackupMacroIndexMerger, test_backup_macro_block_index_store)
{
  int ret = OB_SUCCESS;
  ObBackupOrderedMacroBlockIndexStore macro_index_store;
  init_backup_macro_index_store_(macro_index_store);

  const int64_t upper_bound = 9999;
  for (int64_t i = 1; i <= upper_bound; ++i) {
    ObLogicMacroBlockId logic_id;
    logic_id.tablet_id_ = 200001;
    logic_id.logic_version_ = i;
    logic_id.data_seq_ = i;
    ObBackupMacroBlockIndex macro_index;
    ret = macro_index_store.get_macro_block_index(logic_id, macro_index);
    EXPECT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestBackupMacroIndexMerger, test_backup_macro_block_index_iterator)
{
  int ret = OB_SUCCESS;
  ObBackupOrderedMacroBlockIndexIterator iter;
  ret = iter.init(task_id_, backup_dest_, tenant_id_, backup_set_desc_, ls_id_, backup_data_type_, turn_id_, retry_id_);
  EXPECT_EQ(OB_SUCCESS, ret);
  ObBackupMacroBlockIndex macro_block_index;
  while (OB_SUCC(ret) && !iter.is_iter_end()) {
    ret = iter.get_cur_index(macro_block_index);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = iter.next();
    EXPECT_EQ(OB_SUCCESS, ret);
  }
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_backup_macro_block_index_merger.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_macro_block_index_merger.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
