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

#include "test_backup.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "src/storage/backup/ob_backup_iterator.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;
using namespace oceanbase::blocksstable;

namespace oceanbase {
namespace backup {

static ObSimpleMemLimitGetter getter;

class ObFakeBackupTabletProvider : public ObIBackupTabletProvider {
public:
  ObFakeBackupTabletProvider();
  virtual ~ObFakeBackupTabletProvider();
  int init(const int64_t total_item_count);
  void reset() override;
  void reuse() override;
  bool is_run_out() override;
  void set_backup_data_type(const share::ObBackupDataType &backup_data_type)
  {
    UNUSED(backup_data_type);
  }
  share::ObBackupDataType get_backup_data_type() const override
  {
    ObBackupDataType type;
    return type;
  }
  int get_next_batch_items(common::ObIArray<ObBackupProviderItem> &items, int64_t &task_id) override;
  ObBackupTabletProviderType get_type() const override
  {
    return BACKUP_TABLET_PROVIDER;
  }

private:
  bool is_inited_;
  lib::ObMutex mutex_;
  int64_t task_id_;
  int64_t supplied_item_count_;
  int64_t total_item_count_;
  DISALLOW_COPY_AND_ASSIGN(ObFakeBackupTabletProvider);
};

ObFakeBackupTabletProvider::ObFakeBackupTabletProvider()
    : is_inited_(false), mutex_(), task_id_(-1), supplied_item_count_(0), total_item_count_(0)
{}

ObFakeBackupTabletProvider::~ObFakeBackupTabletProvider()
{}

int ObFakeBackupTabletProvider::init(const int64_t total_item_count)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (total_item_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(total_item_count));
  } else {
    task_id_ = 0;
    supplied_item_count_ = 0;
    total_item_count_ = total_item_count;
    is_inited_ = true;
  }
  return ret;
}

void ObFakeBackupTabletProvider::reset()
{
  is_inited_ = false;
}

void ObFakeBackupTabletProvider::reuse()
{
  task_id_ = 0;
  supplied_item_count_ = 0;
}

bool ObFakeBackupTabletProvider::is_run_out()
{
  bool run_out = false;
  lib::ObMutexGuard guard(mutex_);
  run_out = supplied_item_count_ >= total_item_count_;
  return run_out;
}

int ObFakeBackupTabletProvider::get_next_batch_items(common::ObIArray<ObBackupProviderItem> &items, int64_t &task_id)
{
  int ret = OB_SUCCESS;
  items.reset();
  lib::ObMutexGuard guard(mutex_);
  ObBackupDataType backup_data_type;
  backup_data_type.set_user_data_backup();
  if (supplied_item_count_ >= total_item_count_) {
    ret = OB_ITER_END;
  } else {
    int64_t i = 0;
    int64_t total = random(256, 1024);
    while (OB_SUCC(ret) && i < total) {
      blocksstable::ObLogicMacroBlockId fake_logic_id;
      blocksstable::MacroBlockId fake_macro_block_id;
      storage::ObITable::TableKey fake_table_key;
      common::ObTabletID tablet_id(supplied_item_count_ + 1);
      ObBackupProviderItem item;
      ObBackupMacroBlockId backup_macro_id;
      backup_macro_id.macro_block_id_ = fake_macro_block_id;
      backup_macro_id.logic_id_ = fake_logic_id;
      if (OB_FAIL(item.set(PROVIDER_ITEM_TABLET_AND_SSTABLE_META, backup_data_type, backup_macro_id, fake_table_key, tablet_id))) {
        LOG_WARN("failed to set item", K(ret));
      } else if (OB_FAIL(items.push_back(item))) {
        LOG_WARN("failed to push back", K(ret), K(item));
      } else {
        i++;
        supplied_item_count_++;
      }
    }
  }
  if (OB_SUCC(ret)) {
    task_id = task_id_;
    ++task_id_;
  }
  return ret;
}

void process(ObIBackupTabletProvider *provider, ObBackupMacroBlockTaskMgr *task_mgr)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  ObArray<ObBackupProviderItem> item_list;
  while (OB_SUCC(ret)) {
    const bool is_run_out = provider->is_run_out();
    if (!is_run_out) {
      if (OB_FAIL(provider->get_next_batch_items(item_list, task_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next batch");
        }
      } else if (OB_FAIL(task_mgr->receive(task_id, item_list))) {
        LOG_WARN("failed to receive from task mgr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObArray<ObBackupProviderItem> items;
      const bool has_remain = task_mgr->has_remain();
      int64_t file_id = 0;
      if (OB_FAIL(task_mgr->deliver(items, file_id))) {
        if (OB_EAGAIN == ret) {
          if (has_remain && !is_run_out) {
            continue;
          } else if (is_run_out) {
            LOG_INFO("run out", K(items));
            break;
          }
        }
      } else {
        const ObTabletID &first = items.at(0).tablet_id_;
        const ObTabletID &last = items.at(items.count() - 1).tablet_id_;
        LOG_INFO("backup items infos", K(first), K(last), K(items.count()));
      }
    }
  }
}

void test_task_mgr(const int64_t batch_count, const int64_t total_item_count, const int64_t concurrency)
{
  int ret = OB_SUCCESS;
  const int64_t thread_count = concurrency;
  share::ObBackupDataType backup_data_type;
  backup_data_type.set_minor_data_backup();
  ObBackupMacroBlockTaskMgr *task_mgr = new ObBackupMacroBlockTaskMgr;
  ObFakeBackupTabletProvider *provider = new ObFakeBackupTabletProvider;
  ObLSBackupCtx ls_backup_ctx;
  ret = task_mgr->init(backup_data_type, batch_count, ls_backup_ctx);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = provider->init(total_item_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  std::vector<std::thread> workers;
  for (int64_t i = 0; i < thread_count; ++i) {
    std::thread th(process, provider, task_mgr);
    workers.push_back(std::move(th));
  }
  for (auto &it : workers) {
    it.join();
  }
  delete task_mgr;
  delete provider;
}

TEST(TestBackupUtils, test_task_mgr)
{
  test_task_mgr(1024, 10240, 1);
}

void make_macro_block_id_array(const int64_t tablet_id, const int64_t logic_version,
    const common::ObIArray<int64_t> &data_seq_list, common::ObIArray<ObBackupMacroBlockIDPair> &id_list)
{
  int ret = OB_SUCCESS;
  ObBackupMacroBlockIDPair macro_id;
  for (int64_t i = 0; OB_SUCC(ret) && i < data_seq_list.count(); ++i) {
    const int64_t data_seq = data_seq_list.at(i);
    macro_id.logic_id_ = ObLogicMacroBlockId(data_seq, logic_version, tablet_id);
    ret = id_list.push_back(macro_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

class TestBackupExternalSort : public blocksstable::TestDataFilePrepare
{
public:
  TestBackupExternalSort();
  virtual ~TestBackupExternalSort() {}
  int init_tenant_mgr();
  void destroy_tenant_mgr();
  int64_t calculate_min_item_count();
  int generate_items(const int64_t min_count, common::ObIAllocator &allocator, ObVector<ObBackupProviderItem *> &items);
  virtual void SetUp();
  virtual void TearDown();
public:
  static const int64_t MACRO_BLOCK_SIZE = 2 * 1024 * 1024;
  static const int64_t MACRO_BLOCK_COUNT = 15 * 1024;
private:
  common::ObArenaAllocator allocator_;
};

TestBackupExternalSort::TestBackupExternalSort()
  : TestDataFilePrepare(&getter, "TestBackupExternalSort", MACRO_BLOCK_SIZE, MACRO_BLOCK_COUNT),
    allocator_(ObModIds::TEST)
{
}

void TestBackupExternalSort::SetUp()
{
  TestDataFilePrepare::SetUp();
  EXPECT_EQ(OB_SUCCESS, init_tenant_mgr());
  ASSERT_EQ(OB_SUCCESS, common::ObClockGenerator::init());
  ASSERT_EQ(OB_SUCCESS, tmp_file::ObTmpBlockCache::get_instance().init("tmp_block_cache", 1));
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
}

void TestBackupExternalSort::TearDown()
{
  allocator_.reuse();

  tmp_file::ObTmpBlockCache::get_instance().destroy();
  tmp_file::ObTmpPageCache::get_instance().destroy();
  ObTimerService *timer_service = MTL(ObTimerService *);
  ASSERT_NE(nullptr, timer_service);
  timer_service->stop();
  timer_service->wait();
  timer_service->destroy();
  TestDataFilePrepare::TearDown();
  common::ObClockGenerator::destroy();

  destroy_tenant_mgr();
}

int TestBackupExternalSort::init_tenant_mgr()
{
  int ret = OB_SUCCESS;
  ObAddr self;
  self.set_ip_addr("127.0.0.1", 8086);
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  const int64_t ulmt = 128LL << 30;
  const int64_t llmt = 128LL << 30;
  ret = getter.add_tenant(OB_SYS_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = getter.add_tenant(OB_SERVER_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  lib::set_memory_limit(128LL << 32);
  return ret;
}

void TestBackupExternalSort::destroy_tenant_mgr()
{
}

int64_t TestBackupExternalSort::calculate_min_item_count()
{
  ObBackupProviderItem item;
  const int64_t min_memory_limit = ObExternalSortConstant::MIN_MEMORY_LIMIT;
  const int64_t item_size = sizeof(item) + item.get_deep_copy_size();
  int64_t min_count = min_memory_limit / item_size;
  LOG_INFO("calculate min item count", K(min_memory_limit), K(item_size), K(min_count));
  return min_count;
}

int TestBackupExternalSort::generate_items(const int64_t min_count, common::ObIAllocator &allocator, ObVector<ObBackupProviderItem *> &items)
{
  int ret = OB_SUCCESS;
  ObBackupProviderItem *item = NULL;
  items.reset();
  ObBackupDataType backup_data_type;
  backup_data_type.set_user_data_backup();
  if (min_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(min_count));
  } else {
    void *buf = NULL;
    ObTabletID tablet_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < min_count; ++i) {
      ObBackupProviderItemType item_type = PROVIDER_ITEM_TABLET_AND_SSTABLE_META;
      make_random_tablet_id(tablet_id);
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObBackupProviderItem)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else if (OB_ISNULL(item = new (buf) ObBackupProviderItem())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to placement new item", K(ret));
      } else if (OB_FAIL(item->set_with_fake(item_type, tablet_id, backup_data_type))) {
        LOG_WARN("failed to set item", K(ret), K(tablet_id));
      } else if (OB_FAIL(items.push_back(item))) {
        LOG_WARN("failed to push back item", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_utils.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_utils.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
