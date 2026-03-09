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
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/incremental/ob_inc_ss_macro_seq_define.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;

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

TEST_F(TestSSObjectAccessUtil, test_macro_block)
{
  int ret = OB_SUCCESS;

  uint64_t tablet_id = 100;
  uint64_t server_id = 1;

  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_private_transfer_epoch_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*trasfer_seq*/));

  // 1. record current macro cache used of MACRO_BLOCK
  int64_t used_size_before_write = 0;
  get_macro_cache_used_size(ObSSMacroCacheType::MACRO_BLOCK, used_size_before_write);

  // 2. write
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(100); // seq_id
  macro_id.set_macro_private_transfer_epoch(0); // transfer_seq
  macro_id.set_tenant_seq(server_id);  //tenant_seq
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 3. check disk space, expect increase write_info_.size_
  int64_t used_size_after_write = 0;
  get_macro_cache_used_size(ObSSMacroCacheType::MACRO_BLOCK, used_size_after_write);
  ASSERT_EQ(used_size_after_write, used_size_before_write + write_info_.size_);

  // 4. check macro cache, expect macro cache hit
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_hit_macro_cache = false;
  ObSSFdCacheHandle fd_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_TRUE(is_hit_macro_cache);
  fd_handle.reset();

  // 5. read and compare the read data with the written data
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

  // 6. delete local file
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::delete_local_file(macro_id, 0/*ls_epoch_id*/,
            true/*is_print_log*/, true/*is_del_seg_meta*/, false/*is_logical_delete*/));

  // 7. check disk space, expect decrease write_info_.size_
  int64_t used_size_after_delete = 0;
  get_macro_cache_used_size(ObSSMacroCacheType::MACRO_BLOCK, used_size_after_delete);
  ASSERT_EQ(used_size_after_delete, used_size_after_write - write_info_.size_);

  // 8. check macro cache, expect macro cache hit
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             read_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_FALSE(is_hit_macro_cache);
  fd_handle.reset();
}

TEST_F(TestSSObjectAccessUtil, test_tmp_file)
{
  int ret = OB_SUCCESS;

  const uint64_t tmp_file_id = 100;
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tmp_file_dir(MTL_ID(), MTL_EPOCH_ID(), tmp_file_id));

  // 1. record current macro cache used of TMP_FILE
  int64_t used_size_before_write = 0;
  get_macro_cache_used_size(ObSSMacroCacheType::TMP_FILE, used_size_before_write);

  // 2. write local unsealed 8KB tmp file
  // check local cache tmp file free size is enough, so as to ensure write local cache, instead of object storage
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  ASSERT_LT(8192, disk_space_mgr->get_allocated_shared_macro_cache_free_size_nolock_());

  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
  macro_id.set_second_id(tmp_file_id); // tmp_file_id
  macro_id.set_third_id(1); // segment_id
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.offset_ = 0;
  write_info_.size_ = 8192;
  write_info_.set_tmp_file_valid_length(8192);
  write_info_.io_desc_.set_unsealed();
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_append_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 3. check disk space, expect increase write_info_.size_
  int64_t used_size_after_write = 0;
  get_macro_cache_used_size(ObSSMacroCacheType::TMP_FILE, used_size_after_write);
  ASSERT_EQ(used_size_after_write, used_size_before_write + write_info_.size_);

  // 4. check macro cache, expect macro cache hit
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_hit_macro_cache = false;
  ObSSFdCacheHandle fd_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(ObTabletID::INVALID_TABLET_ID)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_TRUE(is_hit_macro_cache);
  fd_handle.reset();

  // 5. read and compare the read data with the written data
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 0;
  read_info_.size_ = 8192;
  ObStorageObjectHandle read_object_handle;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));
  read_object_handle.reset();

  // 6. delete local file
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::delete_local_file(macro_id, 0/*ls_epoch_id*/,
            true/*is_print_log*/, true/*is_del_seg_meta*/, false/*is_logical_delete*/));

  // 7. check disk space, expect decrease write_info_.size_
  int64_t used_size_after_delete = 0;
  get_macro_cache_used_size(ObSSMacroCacheType::TMP_FILE, used_size_after_delete);
  ASSERT_EQ(used_size_after_delete, used_size_after_write - write_info_.size_);

  // 8. check macro cache, expect macro cache hit
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(ObTabletID::INVALID_TABLET_ID)/*effective_tablet_id*/,
                                             read_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_FALSE(is_hit_macro_cache);
  fd_handle.reset();
}

TEST_F(TestSSObjectAccessUtil, test_write_strategy)
{
  // 1. write SHARED_MINI_V2_DATA_MACRO with WRITE_BACK strategy, and disk space is enough
  uint64_t tablet_id = 200001;
  uint64_t server_id = 1;
  uint64_t reorganization_scn = 0;
  uint64_t data_seq = 0;

  // 1.1 write
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  ASSERT_EQ(OB_SUCCESS, ObIncSSMacroSeqHelper::build_shared_mini_data_seq(
                        0/*source_type*/, 1/*op_id*/, 1/*macro_seq_id*/, data_seq));
  macro_id.set_third_id(data_seq); // source_type + op_id + seq_id
  macro_id.set_fourth_id(reorganization_scn); // reorganization_scn
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 1.2 check macro cache, expect macro cache hit
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_hit_macro_cache = false;
  ObSSFdCacheHandle fd_handle;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_TRUE(is_hit_macro_cache);
  fd_handle.reset();

  // 1.3 read and compare the read data with the written data
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


  // 2. write SHARED_MINI_V2_DATA_MACRO with WRITE_BACK strategy, and disk space is not enough.
  // expect fallback to write through object storage
  int64_t avail_size = 0;
  // exhaust disk space to simulate disk space not enough
  exhaust_macro_block_disk_size(avail_size);

  // 2.1 write
  ASSERT_EQ(OB_SUCCESS, ObIncSSMacroSeqHelper::build_shared_mini_data_seq(
                        0/*source_type*/, 1/*op_id*/, 2/*macro_seq_id*/, data_seq));
  macro_id.set_third_id(data_seq); // source_type + op_id + seq_id
  ASSERT_TRUE(macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 2.2 check macro cache, expect macro cache miss
  is_hit_macro_cache = false;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_FALSE(is_hit_macro_cache);
  fd_handle.reset();

  // 2.3 read and compare the read data with the written data
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


  // 3. write SHARED_MINI_V2_DATA_MACRO with WRITE_THROUGH strategy
  // 3.1 write
  ASSERT_EQ(OB_SUCCESS, ObIncSSMacroSeqHelper::build_shared_mini_data_seq(
                        0/*source_type*/, 1/*op_id*/, 3/*macro_seq_id*/, data_seq));
  macro_id.set_third_id(data_seq); // source_type + op_id + seq_id
  ASSERT_TRUE(macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  write_info_.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
  write_object_handle.reset();

  // 3.2 check macro cache, expect macro cache miss
  is_hit_macro_cache = false;
  ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(macro_id, ObTabletID(tablet_id)/*effective_tablet_id*/,
                                             write_info_.size_, is_hit_macro_cache, fd_handle));
  ASSERT_FALSE(is_hit_macro_cache);
  fd_handle.reset();

  // 3.3 read and compare the read data with the written data
  read_info_.macro_block_id_ = macro_id;
  read_info_.offset_ = 1;
  read_info_.size_ = WRITE_IO_SIZE / 2;
  ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info_.size_, read_object_handle.get_data_size());
  ASSERT_EQ(0, memcmp(write_buf_ + read_info_.offset_, read_object_handle.get_buffer(), read_info_.size_));
  read_object_handle.reset();
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_object_access_util.log*");
  OB_LOGGER.set_file_name("test_ss_object_access_util.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
