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

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))

#include "lib/ob_errno.h"
#include "storage/backup/ob_backup_tmp_file.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_utils.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "test_backup.h"
#include "mtlenv/mock_tenant_module_env.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;

namespace oceanbase {
namespace backup {

static ObSimpleMemLimitGetter getter;

class TestBackupTmpFileQueue : public TestDataFilePrepare {
public:
  TestBackupTmpFileQueue();
  virtual ~TestBackupTmpFileQueue();
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();

private:
  DISALLOW_COPY_AND_ASSIGN(TestBackupTmpFileQueue);
};

TestBackupTmpFileQueue::TestBackupTmpFileQueue() : TestDataFilePrepare(&getter, "TestBackupTmpFileQueue")
{}

TestBackupTmpFileQueue::~TestBackupTmpFileQueue()
{}

void TestBackupTmpFileQueue::SetUp()
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
  ASSERT_EQ(OB_SUCCESS, common::ObClockGenerator::init());
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
  tenant_ctx.set(tf_mgr);
  EXPECT_EQ(OB_SUCCESS, tmp_file::ObTenantTmpFileManager::mtl_init(tf_mgr));
  tf_mgr->get_sn_file_manager().write_cache_.default_memory_limit_ = 40*1024*1024;
  EXPECT_EQ(OB_SUCCESS, tf_mgr->start());

  ObTenantEnv::set_tenant(&tenant_ctx);
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
}

void TestBackupTmpFileQueue::TearDown()
{
  tmp_file::ObTmpPageCache::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  ObTimerService *timer_service = MTL(ObTimerService *);
  ASSERT_NE(nullptr, timer_service);
  timer_service->stop();
  timer_service->wait();
  timer_service->destroy();
  TestDataFilePrepare::TearDown();
  common::ObClockGenerator::destroy();
}

void TestBackupTmpFileQueue::SetUpTestCase()
{
  ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
}

void TestBackupTmpFileQueue::TearDownTestCase()
{
  ObTimerService::get_instance().stop();
  ObTimerService::get_instance().wait();
  ObTimerService::get_instance().destroy();
}

TEST_F(TestBackupTmpFileQueue, test_backup_tmp_file)
{
  int ret = OB_SUCCESS;
  ObBackupTmpFileQueue queue;
  EXPECT_EQ(OB_SUCCESS, queue.init(1 /*tenant_id*/));
  const int64_t count = 1000000;
  ObBackupDataType backup_data_type;
  backup_data_type.set_major_data_backup();
  for (int64_t i = 1; OB_SUCC(ret) && i <= count; ++i) {
    ObBackupProviderItem item;
    OK(item.set_with_fake(ObBackupProviderItemType::PROVIDER_ITEM_TABLET_AND_SSTABLE_META, ObTabletID(i), backup_data_type));
    OK(queue.put_item(item));
  }
  for (int64_t i = 1; OB_SUCC(ret) && i <= count; ++i) {
    ObBackupProviderItem item;
    OK(queue.get_item(item));
    EXPECT_EQ(item.tablet_id_, ObTabletID(i));
  }
}

}  // namespace backup
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_backup_tmp_file_queue.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_tmp_file_queue.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
