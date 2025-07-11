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

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "lib/utility/ob_test_util.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "sensitive_test/object_storage/test_object_storage.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#undef private
#undef protected

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::unittest;

namespace oceanbase
{
namespace storage
{

class TestSSMacroCacheStat : public ::testing::Test
{
public:
  TestSSMacroCacheStat() {}
  virtual ~TestSSMacroCacheStat() {}

  virtual void SetUp() override
  {
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    ASSERT_NE(nullptr, macro_cache_mgr);
    OK(TestSSMacroCacheMgrUtil::clean_macro_cache_mgr());
    ASSERT_TRUE(macro_cache_mgr->meta_map_.empty());
    ASSERT_TRUE(macro_cache_mgr->tablet_stat_map_.empty());
  }

  virtual void TearDown() override
  {
  }

  static void SetUpTestCase()
  {
    GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
    OK(MockTenantModuleEnv::get_instance().init());
    ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());

    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    ASSERT_NE(nullptr, macro_cache_mgr);
    ASSERT_TRUE(macro_cache_mgr->meta_map_.empty());
    macro_cache_mgr->flush_task_.is_inited_ = false;
    macro_cache_mgr->evict_task_.is_inited_ = false;
    macro_cache_mgr->expire_task_.is_inited_ = false;
    macro_cache_mgr->ckpt_task_.is_inited_ = false;
  }

  static void TearDownTestCase()
  {
    int ret = OB_SUCCESS;
    MockTenantModuleEnv::get_instance().destroy();
    if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
    }
  }
};
TEST_F(TestSSMacroCacheStat, test_tablet_stat)
{
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB

  // 1. put a macro into mgr
  const uint64_t tablet_id = 212345;
  const uint64_t server_id = 1;

  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(server_id);
  macro_id.set_macro_transfer_seq(0); // transfer_seq
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  ObSSMacroCacheInfo macro_cache_info(
      tablet_id, WRITE_IO_SIZE, ObSSMacroCacheType::MACRO_BLOCK, false/*is_write_cache*/);
  ASSERT_TRUE(macro_cache_info.is_valid());

  OK(macro_cache_mgr->put(macro_id, macro_cache_info));
  ASSERT_EQ(1, macro_cache_mgr->meta_map_.size());
  ASSERT_NE(nullptr, macro_cache_mgr->meta_map_.get(macro_id));

  macro_cache_mgr->tablet_stat_task_.runTimerTask();
  ASSERT_EQ(1, macro_cache_mgr->tablet_stat_map_.size());
  const ObSSMacroCacheTabletStat *tablet_stat = macro_cache_mgr->tablet_stat_map_.get(tablet_id);
  ASSERT_NE(nullptr, tablet_stat);
  ASSERT_EQ(WRITE_IO_SIZE, tablet_stat->cached_size_);
  ASSERT_EQ(0, tablet_stat->cache_hit_cnt_);
  ASSERT_EQ(0, tablet_stat->cache_hit_bytes_);
  ASSERT_EQ(0, tablet_stat->cache_miss_cnt_);
  ASSERT_EQ(0, tablet_stat->cache_miss_bytes_);

  // access the macro
  const int64_t access_size = 11;
  bool is_hit_cache = false;
  ObSSFdCacheHandle fd_handle;
  ObTabletID effective_tablet_id(tablet_id);
  OK(macro_cache_mgr->get(macro_id, effective_tablet_id, access_size, is_hit_cache, fd_handle));
  ASSERT_FALSE(is_hit_cache);
  fd_handle.reset();

  ASSERT_EQ(WRITE_IO_SIZE, tablet_stat->cached_size_);
  ASSERT_EQ(0, tablet_stat->cache_hit_cnt_);
  ASSERT_EQ(0, tablet_stat->cache_hit_bytes_);
  ASSERT_EQ(1, tablet_stat->cache_miss_cnt_);
  ASSERT_EQ(access_size, tablet_stat->cache_miss_bytes_);

  // 3. write to disk
  ObStorageObjectWriteInfo write_info;
  char write_buf[WRITE_IO_SIZE];
  write_buf[0] = '\0';
  const int64_t mid_offset = WRITE_IO_SIZE / 2;
  memset(write_buf, 'a', mid_offset);
  memset(write_buf + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = WRITE_IO_SIZE;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  OK(macro_cache_mgr->get(macro_id, effective_tablet_id, access_size * 2, is_hit_cache, fd_handle));
  ASSERT_TRUE(is_hit_cache);
  fd_handle.reset();

  ASSERT_EQ(WRITE_IO_SIZE, tablet_stat->cached_size_);
  ASSERT_EQ(1, tablet_stat->cache_hit_cnt_);
  ASSERT_EQ(access_size * 2, tablet_stat->cache_hit_bytes_);
  ASSERT_EQ(1, tablet_stat->cache_miss_cnt_);
  ASSERT_EQ(access_size, tablet_stat->cache_miss_bytes_);

  // 4. clean tablet
  ObSSMacroCacheMetaHandle meta_handle;
  OK(macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
  ASSERT_TRUE(meta_handle.is_valid());
  meta_handle()->set_last_access_time_us(ObTimeUtility::fast_current_time() - 3600LL * 1000LL * 1000LL);
  meta_handle.reset();
  macro_cache_mgr->clean_deleted_tablet_();
  ASSERT_TRUE(macro_cache_mgr->tablet_stat_map_.empty());
  ASSERT_TRUE(macro_cache_mgr->meta_map_.empty());
}

TEST_F(TestSSMacroCacheStat, test_tablet_stat_after_evict)
{
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB

  // 1. put a macro into mgr
  const uint64_t tablet_id = 212345;
  const uint64_t server_id = 1;

  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id);
  macro_id.set_third_id(server_id);
  macro_id.set_macro_transfer_seq(0); // transfer_seq
  macro_id.set_tenant_seq(100); // tenant_seq
  ASSERT_TRUE(macro_id.is_valid());

  ObSSMacroCacheInfo macro_cache_info(
      tablet_id, WRITE_IO_SIZE, ObSSMacroCacheType::MACRO_BLOCK, false/*is_write_cache*/);
  ASSERT_TRUE(macro_cache_info.is_valid());

  OK(macro_cache_mgr->put(macro_id, macro_cache_info));
  ASSERT_EQ(1, macro_cache_mgr->meta_map_.size());
  ASSERT_NE(nullptr, macro_cache_mgr->meta_map_.get(macro_id));

  // 2. write to disk
  ObStorageObjectWriteInfo write_info;
  char write_buf[WRITE_IO_SIZE];
  write_buf[0] = '\0';
  const int64_t mid_offset = WRITE_IO_SIZE / 2;
  memset(write_buf, 'a', mid_offset);
  memset(write_buf + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = WRITE_IO_SIZE;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 3. evict the read macro cache
  macro_cache_mgr->tablet_stat_task_.runTimerTask();
  const ObSSMacroCacheTabletStat *tablet_stat = macro_cache_mgr->tablet_stat_map_.get(tablet_id);
  ASSERT_NE(nullptr, tablet_stat);
  ASSERT_EQ(WRITE_IO_SIZE, tablet_stat->cached_size_);
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->force_evict_by_macro_id(macro_id));
  ASSERT_EQ(nullptr, macro_cache_mgr->meta_map_.get(macro_id));
  macro_cache_mgr->tablet_stat_task_.runTimerTask();
  ASSERT_EQ(1, macro_cache_mgr->tablet_stat_map_.size());
  tablet_stat = macro_cache_mgr->tablet_stat_map_.get(tablet_id);
  ASSERT_EQ(0, tablet_stat->cached_size_);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_macro_cache_stat.log*");
  OB_LOGGER.set_file_name("test_ss_macro_cache_stat.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}