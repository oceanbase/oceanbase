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
#define USING_LOG_PREFIX STORAGETEST
#define protected public
#define private public
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/prewarm/ob_ss_local_cache_prewarm_service.h"
#include "storage/compaction/ob_major_pre_warmer.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::compaction;

class TestSSObjectAccessUtil : public ::testing::Test
{
public:
  TestSSObjectAccessUtil() : write_info_(), read_info_(), write_buf_(), read_buf_() {}
  virtual ~TestSSObjectAccessUtil() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void get_macro_cache_used_size(const ObSSMacroCacheType cache_type, int64_t &used_size);
  void exhaust_macro_block_disk_size(int64_t &avail_size);
  void release_macro_block_disk_size(const int64_t avail_size);

public:
  static const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB
  ObStorageObjectWriteInfo write_info_;
  ObStorageObjectReadInfo read_info_;
  char write_buf_[WRITE_IO_SIZE];
  char read_buf_[WRITE_IO_SIZE];
};

void TestSSObjectAccessUtil::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  MTL(tmp_file::ObTenantTmpFileManager *)->stop();
  MTL(tmp_file::ObTenantTmpFileManager *)->wait();
  MTL(tmp_file::ObTenantTmpFileManager *)->destroy();
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  file_manager->preread_cache_mgr_.preread_task_.is_inited_ = false;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  macro_cache_mgr->evict_task_.is_inited_ = false;
  macro_cache_mgr->flush_task_.is_inited_ = false;
}

void TestSSObjectAccessUtil::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSObjectAccessUtil::SetUp()
{
  // construct write info
  write_buf_[0] = '\0';
  const int64_t mid_offset = WRITE_IO_SIZE / 2;
  memset(write_buf_, 'a', mid_offset);
  memset(write_buf_ + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
  write_info_.io_desc_.set_wait_event(1);
  write_info_.buffer_ = write_buf_;
  write_info_.offset_ = 0;
  write_info_.size_ = WRITE_IO_SIZE;
  write_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info_.mtl_tenant_id_ = MTL_ID();
  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;

  // construct read info
  read_buf_[0] = '\0';
  read_info_.io_desc_.set_wait_event(1);
  read_info_.buf_ = read_buf_;
  read_info_.offset_ = 0;
  read_info_.size_ = WRITE_IO_SIZE;
  read_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info_.mtl_tenant_id_ = MTL_ID();
}

void TestSSObjectAccessUtil::TearDown()
{
  write_buf_[0] = '\0';
  read_buf_[0] = '\0';
}

void TestSSObjectAccessUtil::get_macro_cache_used_size(
    const ObSSMacroCacheType cache_type,
    int64_t &used_size)
{
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  ObSSMacroCacheStat cache_stat;
  ASSERT_EQ(OB_SUCCESS, disk_space_mgr->get_macro_cache_stat(cache_type, cache_stat));
  used_size = cache_stat.used_;
}

void TestSSObjectAccessUtil::exhaust_macro_block_disk_size(int64_t &avail_size)
{
  static int64_t call_times = 0;
  call_times++;
  ObTenantDiskSpaceManager *disk_space_manager = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_manager) << "call_times: " << call_times;
  avail_size = disk_space_manager->get_allocated_shared_macro_cache_free_size_nolock_();
  ASSERT_EQ(OB_SUCCESS, disk_space_manager->alloc_file_size(avail_size,
            ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE)) << "call_times: " << call_times;
  ASSERT_EQ(OB_SERVER_OUTOF_DISK_SPACE, disk_space_manager->alloc_file_size(8192,
            ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE)) << "call_times: " << call_times;
}

void TestSSObjectAccessUtil::release_macro_block_disk_size(const int64_t avail_size)
{
  static int64_t call_times = 0;
  call_times++;
  ObTenantDiskSpaceManager *disk_space_manager = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_manager) << "call_times: " << call_times;
  ASSERT_EQ(OB_SUCCESS, disk_space_manager->free_file_size(avail_size,
            ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE)) << "call_times: " << call_times;
}

TEST_F(TestSSObjectAccessUtil, test_try_write_lcache)
{
  // 1. write SHARED_MAJOR_DATA_MACRO with WRITE_THROUGH_AND_TRY_WRITE_LCACHE strategy, and disk space is enough
  uint64_t tablet_id = 200001;
  uint64_t seq_id = 1;
  uint64_t reorganization_scn = 0;
  // 1.1 write
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(seq_id);
  macro_id.set_fourth_id(reorganization_scn);
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 1.2 wait for write lcache task to write to lcache
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  ASSERT_NE(nullptr, local_cache_service);
  ObLinkQueue &write_lcache_queue = local_cache_service->write_lcache_task_.write_lcache_queue_;
  int64_t wait_times = 0;
  while ((write_lcache_queue.size() > 0) && (wait_times < 10)) {
    usleep(1000 * 1000); // sleep 1s
    wait_times++;
  }
  ASSERT_EQ(0, write_lcache_queue.size());

  // 1.3 check macro cache, expect macro cache hit
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_hit_macro_cache = false;
  ObSSFdCacheHandle fd_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_TRUE(is_hit_macro_cache);
  fd_handle.reset();

  // 1.4 read and compare the read data with the written data
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  ObStorageObjectHandle read_object_handle;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));
  read_object_handle.reset();


  // 2. write SHARED_MAJOR_DATA_MACRO with WRITE_THROUGH_AND_TRY_WRITE_LCACHE strategy, and disk space is not enough.
  // expect succ to write through object storage, but fail to write to lcache
  int64_t avail_size = 0;
  // exhaust disk space to simulate disk space not enough
  exhaust_macro_block_disk_size(avail_size);

  // 2.1 write
  macro_id.set_third_id(seq_id + 1);
  ASSERT_TRUE(macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 2.2 expect none write lcache task is pushed to write_lcache_queue
  ASSERT_EQ(0, write_lcache_queue.size());

  // 2.3 check macro cache, expect macro cache miss
  is_hit_macro_cache = false;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_FALSE(is_hit_macro_cache);
  fd_handle.reset();

  // 2.4 read and compare the read data with the written data
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));
  read_object_handle.reset();

  // release disk space
  release_macro_block_disk_size(avail_size);
}

TEST_F(TestSSObjectAccessUtil, test_try_write_lcache_with_prewarm_level)
{
  ObSSLocalCachePrewarmService *prewarm_service = MTL(ObSSLocalCachePrewarmService *);
  ASSERT_NE(nullptr, prewarm_service);
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  ASSERT_NE(nullptr, local_cache_service);
  ObLinkQueue &write_lcache_queue = local_cache_service->write_lcache_task_.write_lcache_queue_;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  uint64_t tablet_id = 200002;
  uint64_t seq_id = 1;
  uint64_t reorganization_scn = 0;

  // 1. Test with _ss_major_compaction_prewarm_level = 0 (PREWARM_META_AND_DATA_LEVEL)
  // Expect: should write lcache
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  ASSERT_TRUE(tenant_config.is_valid());
  tenant_config->_ss_major_compaction_prewarm_level = 0;
  prewarm_service->mc_prewarm_level_refresh_task_.runTimerTask();
  ASSERT_EQ(ObSSMajorPrewarmLevel::PREWARM_META_AND_DATA_LEVEL, prewarm_service->get_major_prewarm_level());

  // 1.1 write
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(seq_id);
  macro_id.set_fourth_id(reorganization_scn);
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 1.2 wait for write lcache task to write to lcache
  int64_t wait_times = 0;
  while ((write_lcache_queue.size() > 0) && (wait_times < 10)) {
    usleep(1000 * 1000); // sleep 1s
    wait_times++;
  }
  ASSERT_EQ(0, write_lcache_queue.size());

  // 1.3 check macro cache, expect macro cache hit
  bool is_hit_macro_cache = false;
  ObSSFdCacheHandle fd_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_TRUE(is_hit_macro_cache);
  fd_handle.reset();

  // 1.4 read and compare the read data with the written data
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  ObStorageObjectHandle read_object_handle;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));
  read_object_handle.reset();

  // 2. Test with _ss_major_compaction_prewarm_level = 2 (PREWARM_NONE_LEVEL)
  // Expect: should NOT write lcache
  tenant_config->_ss_major_compaction_prewarm_level = 2;
  prewarm_service->mc_prewarm_level_refresh_task_.runTimerTask();
  ASSERT_EQ(ObSSMajorPrewarmLevel::PREWARM_NONE_LEVEL, prewarm_service->get_major_prewarm_level());

  // 2.1 write
  seq_id++;
  macro_id.set_third_id(seq_id);
  ASSERT_TRUE(macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH_AND_TRY_WRITE_LCACHE;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 2.2 expect none write lcache task is pushed to write_lcache_queue
  ASSERT_EQ(0, write_lcache_queue.size());

  // 2.3 check macro cache, expect macro cache miss
  is_hit_macro_cache = false;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_FALSE(is_hit_macro_cache);
  fd_handle.reset();

  // 2.4 read and compare the read data with the written data
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));
  read_object_handle.reset();

  // reset prewarm level to default
  tenant_config->_ss_major_compaction_prewarm_level = 0;
  prewarm_service->mc_prewarm_level_refresh_task_.runTimerTask();
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_write_lcache_task.log*");
  OB_LOGGER.set_file_name("test_ss_write_lcache_task.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
