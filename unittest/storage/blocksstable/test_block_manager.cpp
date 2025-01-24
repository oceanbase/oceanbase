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

#include <sys/vfs.h>
#include <sys/statvfs.h>
#include <gtest/gtest.h>

#define USING_LOG_PREFIX STORAGE

#define protected public
#define private public

#include "storage/blocksstable/ob_data_file_prepare.h"
#include "mtlenv/mock_tenant_module_env.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;
static ObSimpleMemLimitGetter getter;

namespace unittest
{
class TestBlockManager : public blocksstable::TestDataFilePrepare
{
public:
  TestBlockManager();
  virtual ~TestBlockManager() = default;
  virtual void SetUp() override;
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  }
  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }

private:
  int init_multi_tenant();

private:
  common::ObAddr addr_;
  omt::ObMultiTenant multi_tenant_;
};

TestBlockManager::TestBlockManager()
  : TestDataFilePrepare(&getter, "TestBlockManager", OB_DEFAULT_MACRO_BLOCK_SIZE, 200)
{
}

int TestBlockManager::init_multi_tenant()
{
  int ret = OB_SUCCESS;
  GCONF.cpu_count = 6;
  if (OB_SUCCESS != (ret = multi_tenant_.init(addr_))) {
    STORAGE_LOG(WARN, "init multi_tenant failed", K(ret));
  } else {
    multi_tenant_.start();
    GCTX.omt_ = &multi_tenant_;
  }
  return ret;
}

void TestBlockManager::SetUp()
{
  TestDataFilePrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, init_multi_tenant());
  OB_SERVER_BLOCK_MGR.block_map_.reset();
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
}

TEST_F(TestBlockManager, test_inc_and_dec_ref_cnt)
{
  int ret = OB_SUCCESS;
  ObBlockManager::BlockInfo block_info;
  ObMacroBlockHandle macro_handle;

  ret = OB_SERVER_BLOCK_MGR.alloc_block(macro_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  const MacroBlockId macro_id = macro_handle.get_macro_id();
  ASSERT_TRUE(macro_id.is_valid());
  ret = OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, block_info.ref_cnt_);
  ASSERT_TRUE(block_info.access_time_ > 0);

  ret = OB_SERVER_BLOCK_MGR.inc_ref(macro_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, block_info.ref_cnt_);
  ASSERT_TRUE(block_info.access_time_ > 0);

  ret = OB_SERVER_BLOCK_MGR.dec_ref(macro_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, block_info.ref_cnt_);
  ASSERT_TRUE(block_info.access_time_ > 0);

  macro_handle.reset();
  ret = OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, block_info.ref_cnt_);
  ASSERT_TRUE(block_info.access_time_ > 0);
}

TEST_F(TestBlockManager, test_mark_and_sweep)
{
  int ret = OB_SUCCESS;
  ObBlockManager::BlockInfo block_info;
  ObMacroBlockHandle macro_handle;

  ASSERT_EQ(0, OB_SERVER_BLOCK_MGR.block_map_.count());

  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;

  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = common::OB_SUCCESS;
  } else {
    ASSERT_EQ(common::OB_SUCCESS, ret);
  }

  static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);

  ObTimerService *timer_service = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObTimerService::mtl_new(timer_service));
  ASSERT_EQ(OB_SUCCESS, timer_service->start());
  tenant_ctx.set(timer_service);

  ASSERT_EQ(OB_SUCCESS, tmp_file::ObTmpBlockCache::get_instance().init("tmp_block_cache", 1));
  ASSERT_EQ(OB_SUCCESS, tmp_file::ObTmpPageCache::get_instance().init("sn_tmp_page_cache", 1));

  tmp_file::ObTenantTmpFileManager *tf_mgr = nullptr;
  EXPECT_EQ(OB_SUCCESS, mtl_new_default(tf_mgr));
  EXPECT_EQ(OB_SUCCESS, tmp_file::ObTenantTmpFileManager::mtl_init(tf_mgr));
  tf_mgr->get_sn_file_manager().page_cache_controller_.write_buffer_pool_.default_wbp_memory_limit_ = 40*1024*1024;
  EXPECT_EQ(OB_SUCCESS, tf_mgr->start());
  tenant_ctx.set(tf_mgr);
  ObTenantEnv::set_tenant(&tenant_ctx);
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
  ASSERT_EQ(0, OB_SERVER_BLOCK_MGR.block_map_.count());

  const int blk_cnt = 100;
  int count = blk_cnt;
  while (count--) {
    macro_handle.reset();
    ret = OB_SERVER_BLOCK_MGR.alloc_block(macro_handle);
    ASSERT_EQ(OB_SUCCESS, ret);

    const MacroBlockId &macro_id = macro_handle.get_macro_id();
    ASSERT_TRUE(macro_id.is_valid());
    ret = OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, block_info.ref_cnt_);
    ASSERT_TRUE(block_info.access_time_ > 0);
  }

  ASSERT_EQ(blk_cnt, OB_SERVER_BLOCK_MGR.block_map_.count());

  ObBlockManager::MacroBlkIdMap mark_info;
  ret = mark_info.init(ObModIds::OB_STORAGE_FILE_BLOCK_REF, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> macro_id_set;
  ret = macro_id_set.create(MAX(2, OB_SERVER_BLOCK_MGR.block_map_.count()));
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t safe_ts = ObTimeUtility::current_time();
  int64_t hold_cnt = 0;
  ObBlockManager::GetPendingFreeBlockFunctor functor(1000000, mark_info, hold_cnt);
  ret = OB_SERVER_BLOCK_MGR.block_map_.for_each(functor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(blk_cnt - 1, mark_info.count());

  ObMacroBlockMarkerStatus tmp_status;
  ret = OB_SERVER_BLOCK_MGR.mark_server_meta_blocks(mark_info, macro_id_set, tmp_status);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(blk_cnt - 1, mark_info.count());

  // ret = OB_SERVER_BLOCK_MGR.mark_tmp_file_blocks(mark_info, macro_id_set, tmp_status);

  ObArray<MacroBlockId> macro_block_list;
  tf_mgr->get_sn_file_manager().get_macro_block_list(macro_block_list);
  ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.update_mark_info(macro_block_list, macro_id_set, mark_info));
  tmp_status.tmp_file_count_ += macro_block_list.count();
  tmp_status.hold_count_ -= macro_block_list.count();

  ASSERT_EQ(blk_cnt - 1, mark_info.count());

  OB_SERVER_BLOCK_MGR.mark_and_sweep();

  macro_handle.reset();

  tmp_file::ObTmpBlockCache::get_instance().destroy();
  tmp_file::ObTmpPageCache::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  common::ObClockGenerator::destroy();
}

TEST_F(TestBlockManager, test_mark_and_sweep_skip_mark)
{
  int ret = OB_SUCCESS;
  ObBlockManager::BlockInfo block_info;
  ObMacroBlockHandle macro_handle;

  ASSERT_EQ(0, OB_SERVER_BLOCK_MGR.block_map_.count());

  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;

  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = common::OB_SUCCESS;
  } else {
    ASSERT_EQ(common::OB_SUCCESS, ret);
  }
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ASSERT_EQ(0, OB_SERVER_BLOCK_MGR.block_map_.count());

  ObBlockManager::MacroBlkIdMap mark_info;
  ret = mark_info.init(ObModIds::OB_STORAGE_FILE_BLOCK_REF, OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> macro_id_set;
  ret = macro_id_set.create(MAX(2, OB_SERVER_BLOCK_MGR.block_map_.count()));
  ASSERT_EQ(OB_SUCCESS, ret);
  int64_t safe_ts = ObTimeUtility::current_time();
  int64_t hold_cnt = 0;
  ObBlockManager::GetPendingFreeBlockFunctor functor(1000000, mark_info, hold_cnt);
  ret = OB_SERVER_BLOCK_MGR.block_map_.for_each(functor);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, mark_info.count());

  // first mark_and_sweep, should update start_time_
  OB_SERVER_BLOCK_MGR.mark_and_sweep();
  ASSERT_EQ(false, OB_SERVER_BLOCK_MGR.marker_status_.mark_finished_);
  int64_t first_start_time = OB_SERVER_BLOCK_MGR.marker_status_.start_time_;
  int64_t first_last_end_time = OB_SERVER_BLOCK_MGR.marker_status_.last_end_time_;

  std::cout << ObTimeUtility::fast_current_time() << std::endl;
  sleep(1);
  std::cout << ObTimeUtility::fast_current_time() << std::endl;

  // second mark_and_sweep, should update start_time_, should not update last_end_time_
  OB_SERVER_BLOCK_MGR.mark_and_sweep();
  ASSERT_EQ(false, OB_SERVER_BLOCK_MGR.marker_status_.mark_finished_);
  ASSERT_NE(first_start_time, OB_SERVER_BLOCK_MGR.marker_status_.start_time_);
  ASSERT_EQ(first_last_end_time, OB_SERVER_BLOCK_MGR.marker_status_.last_end_time_);

  macro_handle.reset();

  // FILE_MANAGER_INSTANCE_V2.destroy();
  ObKVGlobalCache::get_instance().destroy();
}

TEST_F(TestBlockManager, test_ref_cnt_wash_and_load)
{
  int ret = OB_SUCCESS;
  ObBlockManager::BlockInfo block_info;
  ObMacroBlockHandle macro_handle;

  ret = OB_SERVER_BLOCK_MGR.alloc_block(macro_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  MacroBlockId macro_id = macro_handle.get_macro_id();
  macro_id.set_write_seq(100);
  ASSERT_TRUE(macro_id.is_valid());
  ret = OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  macro_id = macro_handle.get_macro_id();
  ret = OB_SERVER_BLOCK_MGR.inc_ref(macro_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, block_info.ref_cnt_);
  ASSERT_TRUE(block_info.access_time_ > 0);

  // test wash block
  ret = OB_SERVER_BLOCK_MGR.dec_ref(macro_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, block_info.ref_cnt_);
  ASSERT_TRUE(block_info.access_time_ > 0);

  // test load block
  ret = OB_SERVER_BLOCK_MGR.inc_ref(macro_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, block_info.ref_cnt_);
  ASSERT_TRUE(block_info.access_time_ > 0);

  ret = OB_SERVER_BLOCK_MGR.dec_ref(macro_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, block_info.ref_cnt_);
  ASSERT_TRUE(block_info.access_time_ > 0);

  macro_handle.reset();

  ret = OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, block_info.ref_cnt_);
  ASSERT_TRUE(block_info.access_time_ > 0);
}

TEST_F(TestBlockManager, test_resize_file_1)
{
  struct statvfs svfs;
  statvfs(util_.storage_env_.sstable_dir_, &svfs);
  int64_t free_space = svfs.f_bavail * svfs.f_bsize;
  int used_space = OB_STORAGE_OBJECT_MGR.get_total_macro_block_count() * OB_STORAGE_OBJECT_MGR.get_macro_block_size();

  double percentage = used_space * 1.0 / (used_space + free_space) + 1;
  int ret = OB_STORAGE_OBJECT_MGR.resize_local_device(0, percentage, 0);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  int64_t free_blk_cnt_1 = OB_SERVER_BLOCK_MGR.io_device_->get_free_block_count();
  ret = OB_STORAGE_OBJECT_MGR.resize_local_device(used_space + free_space / 2, 99,  0);
  ASSERT_EQ(common::OB_SUCCESS, ret);
  int64_t free_blk_cnt_2 = OB_SERVER_BLOCK_MGR.io_device_->get_free_block_count();
  ASSERT_TRUE(free_space > 0 ? free_blk_cnt_1 < free_blk_cnt_2 : free_blk_cnt_1 == free_blk_cnt_2);

  ret = OB_STORAGE_OBJECT_MGR.resize_local_device(used_space, 99, 0);
  ASSERT_EQ(common::OB_NOT_SUPPORTED, ret);
  int64_t free_blk_cnt_3 = OB_SERVER_BLOCK_MGR.io_device_->get_free_block_count();
  ASSERT_TRUE(free_blk_cnt_2 == free_blk_cnt_3);
}

TEST_F(TestBlockManager, test_resize_file_2)
{
  struct statvfs svfs;
  statvfs(util_.storage_env_.sstable_dir_, &svfs);
  int64_t free_space = svfs.f_bavail * svfs.f_bsize;
  int used_space = OB_STORAGE_OBJECT_MGR.get_total_macro_block_count() * OB_STORAGE_OBJECT_MGR.get_macro_block_size();
  int ret = OB_STORAGE_OBJECT_MGR.resize_local_device(used_space + 2 * free_space, 99, 0);
  ASSERT_EQ(common::OB_SERVER_OUTOF_DISK_SPACE, ret);

  int64_t delta_space = free_space - 100 * 1024 * 1024 * 1024L;
  int64_t min_space = 0;
  ret = OB_STORAGE_OBJECT_MGR.resize_local_device(used_space + std::max(delta_space, min_space), 99, 0);
  ASSERT_EQ(common::OB_SUCCESS, ret);
}

class TestMacroBlockSeqStress : public share::ObThreadPool
{
public:
  TestMacroBlockSeqStress() : thread_cnt_(0), is_inited_(false) {}
  virtual ~TestMacroBlockSeqStress() {}
  int init(const int64_t thread_cnt);
  virtual void run1();

private:
  const int GENERATE_SEQ_NUMBERS_PER_THREAD = 10000;

  int64_t thread_cnt_;
  ObMacroBlockRewriteSeqGenerator macro_seq_generator;
  hash::ObHashMap<uint32_t, bool, hash::SpinReadWriteDefendMode> blk_seqs_;
  bool is_inited_ = false;
};

int TestMacroBlockSeqStress::init(const int64_t thread_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (thread_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(thread_cnt));
  } else if (OB_FAIL(blk_seqs_.create(thread_cnt * GENERATE_SEQ_NUMBERS_PER_THREAD,
                                      "test_macro_seq",
                                      "test_macro_seq"))) {
    LOG_WARN("fail to create block sequences map", K(ret));
  } else {
    thread_cnt_ = thread_cnt;
    is_inited_ = true;
  }
  return ret;
}

void TestMacroBlockSeqStress::run1()
{
  int ret = OB_SUCCESS;
  uint64_t blk_seq = 0;
  int count = GENERATE_SEQ_NUMBERS_PER_THREAD;
  while (0 < count--) {
    ret = macro_seq_generator.generate_next_sequence(blk_seq);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = blk_seqs_.set_refactored(blk_seq, true);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST_F(TestBlockManager, macor_block_seq)
{
  int ret = OB_SUCCESS;
  uint32_t restart_seq = 0;
  uint64_t blk_seq = 0;

  ObMacroBlockRewriteSeqGenerator macro_seq_generator;

  restart_seq = macro_seq_generator.rewrite_seq_;
  ASSERT_EQ(0, restart_seq);

  ret = macro_seq_generator.generate_next_sequence(blk_seq);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, blk_seq);

  ret = macro_seq_generator.generate_next_sequence(blk_seq);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, blk_seq);

  macro_seq_generator.reset();

  restart_seq = macro_seq_generator.rewrite_seq_;
  ASSERT_EQ(0, restart_seq);

  ret = macro_seq_generator.generate_next_sequence(blk_seq);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, blk_seq);

  ret = macro_seq_generator.generate_next_sequence(blk_seq);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, blk_seq);
}

TEST_F(TestBlockManager, test_multi_thread)
{
  int ret = OB_SUCCESS;
  TestMacroBlockSeqStress stress;
  const int thread_cnt = 16;

  ret = stress.init(thread_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = stress.start();
  ASSERT_EQ(OB_SUCCESS, ret);

  stress.wait();
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_block_manager.log*");
  OB_LOGGER.set_file_name("test_block_manager.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
