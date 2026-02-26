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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include "gtest/gtest.h"

#define private public
#define protected public
#include "lib/ob_errno.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
class TestSSPersistMicroDataTask : public ::testing::Test
{
public:
  TestSSPersistMicroDataTask() {}
  virtual ~TestSSPersistMicroDataTask() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();

  void add_batch_micro_block(const int64_t start_macro_id, const int64_t macro_cnt, const int64_t micro_cnt,
                              ObArray<ObSSMicroBlockCacheKey> &micro_key_arr);

private:
  ObSSMicroCache *micro_cache_;
};

void TestSSPersistMicroDataTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSPersistMicroDataTask::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSPersistMicroDataTask::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 30)));
  micro_cache->start();
  micro_cache_ = micro_cache;
}

void TestSSPersistMicroDataTask::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  micro_cache_ = nullptr;
}

void TestSSPersistMicroDataTask::add_batch_micro_block(
    const int64_t start_macro_id,
    const int64_t macro_cnt,
    const int64_t micro_cnt,
    ObArray<ObSSMicroBlockCacheKey> &micro_key_arr)
{
  ObArenaAllocator allocator;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;

  const int64_t payload_offset = ObSSMemBlock::get_reserved_size();
  const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
  const int32_t micro_size = (DEFAULT_BLOCK_SIZE - payload_offset) / micro_cnt - micro_index_size;
  char *data_buf = static_cast<char*>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  for (int64_t i = 0; i < macro_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(start_macro_id + i);
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
      ObSSMemBlockHandle mem_blk_handle;
      uint32_t micro_crc = 0;
      bool alloc_new = false;
      ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.add_micro_block_data(micro_key, data_buf, micro_size, mem_blk_handle, micro_crc, alloc_new));
      ASSERT_EQ(true, mem_blk_handle.is_valid());
      bool first_add = false;
      const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
      ASSERT_EQ(OB_SUCCESS,
          micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc,
          effective_tablet_id, mem_blk_handle, first_add));
      ASSERT_EQ(true, first_add);
      ASSERT_EQ(OB_SUCCESS, micro_key_arr.push_back(micro_key));
    }
  }
}

TEST_F(TestSSPersistMicroDataTask, persist_sealed_mem_blk)
{
  LOG_INFO("TEST_CASE: start persist_sealed_mem_blk");
  ObArenaAllocator allocator;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  ObSSPersistMicroDataTask &persist_data_task = micro_cache_->task_runner_.persist_data_task_;
  persist_data_task.is_inited_ = false;
  ob_usleep(10 * 1000);

  ObArray<ObSSMicroBlockCacheKey> micro_key_arr;
  const int64_t micro_cnt = 20;
  // 1. Add micro_block and execute persist_task
  {
    const int64_t MACRO_BLK_CNT = mem_blk_mgr.mem_blk_pool_.max_fg_count_;
    const int64_t start_macro_id = 1;
    add_batch_micro_block(start_macro_id, MACRO_BLK_CNT, micro_cnt, micro_key_arr);

    ASSERT_EQ(MACRO_BLK_CNT - 1, mem_blk_mgr.get_sealed_mem_block_cnt());
    ASSERT_EQ(OB_SUCCESS, persist_data_task.persist_data_op_.persist_sealed_mem_blocks());
    ASSERT_EQ(0, mem_blk_mgr.get_sealed_mem_block_cnt());
  }

  // 2. Check micro_meta persisted
  {
    ObSSMicroMetaManager::SSMicroMetaMap &micro_map = micro_meta_mgr.micro_meta_map_;
    const int64_t total_cnt = micro_key_arr.count();
    for (int64_t i = 0; i < total_cnt; ++i) {
      ObSSMicroBlockMetaHandle micro_handle;
      const ObSSMicroBlockCacheKey &micro_key = micro_key_arr[i];
      ASSERT_EQ(OB_SUCCESS, micro_map.get(&micro_key, micro_handle));
      if (i < total_cnt - micro_cnt) {
        ASSERT_EQ(true, micro_handle()->is_data_persisted_);
      } else { // micro_block in unsealed_mem_blk is unpersisted
        ASSERT_EQ(false, micro_handle()->is_data_persisted_);
      }
    }
  }
}

/* Mem_block fail to be flushed if exist inflight write request on it. */
TEST_F(TestSSPersistMicroDataTask, sealed_mem_blk_with_infilght_write_request) {
  LOG_INFO("TEST_CASE: start sealed_mem_blk_with_infilght_write_request");
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  ObSSPersistMicroDataTask &persist_data_task = micro_cache_->task_runner_.persist_data_task_;
  persist_data_task.is_inited_ = false;
  ob_usleep(10 * 1000);

  ObArray<ObSSMicroBlockCacheKey> micro_key_arr;
  ObSSMemBlock *sealed_mem_blk = nullptr;
  char data_buf[1024];
  const int64_t micro_size = 1024;
  const int64_t micro_cnt = 10;

  // 1. Add some micro_block into mem_block and update micro_meta except first one.
  {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(8888);
    for (int64_t i = 0; i < micro_cnt; ++i) {
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, i + 1, micro_size);
      ObSSMemBlockHandle mem_blk_handle;
      uint32_t micro_crc = 0;
      bool alloc_new = false;
      ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.add_micro_block_data(micro_key, data_buf, micro_size, mem_blk_handle, micro_crc, alloc_new));
      ASSERT_EQ(true, mem_blk_handle.is_valid());
      if (i > 0) {
        bool first_add = false;
        const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
        ASSERT_EQ(OB_SUCCESS,
            micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc,
            effective_tablet_id, mem_blk_handle, first_add));
      }
      ASSERT_EQ(OB_SUCCESS, micro_key_arr.push_back(micro_key));
    }
    sealed_mem_blk = mem_blk_mgr.fg_mem_blk_;
    ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.inner_seal_and_alloc_mem_block(true/* is_fg */));
    ASSERT_EQ(1, mem_blk_mgr.get_sealed_mem_block_cnt());
    ASSERT_EQ(false, sealed_mem_blk->can_flush());
  }

  // 2. Execute persist_data_task but fail to persist this mem_blk because existing inflight request on it.
  //    After inflight request finishes, this mem_blk can be persisted.
  {
    ASSERT_EQ(OB_SUCCESS, persist_data_task.persist_data_op_.persist_sealed_mem_blocks());
    ASSERT_EQ(1, mem_blk_mgr.get_sealed_mem_block_cnt());

    ObSSMemBlockHandle mem_blk_handle;
    mem_blk_handle.set_ptr(sealed_mem_blk);
    uint32_t micro_crc = 0;
    bool first_add = false;
    const uint64_t effective_tablet_id = micro_key_arr[0].micro_id_.macro_id_.second_id();
    ASSERT_EQ(OB_SUCCESS,
        micro_meta_mgr.add_or_update_micro_block_meta(micro_key_arr[0], micro_size, micro_crc,
        effective_tablet_id, mem_blk_handle, first_add));

    ASSERT_EQ(true, sealed_mem_blk->can_flush());
    ASSERT_EQ(OB_SUCCESS, persist_data_task.persist_data_op_.persist_sealed_mem_blocks());
    ASSERT_EQ(0, mem_blk_mgr.get_sealed_mem_block_cnt());
  }
}

TEST_F(TestSSPersistMicroDataTask, add_duplicate_micro_block_into_mem_blk)
{
  LOG_INFO("TEST_CASE: start add_duplicate_micro_block_into_mem_blk");
  ObArenaAllocator allocator;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  ObSSPersistMicroDataTask &persist_data_task = micro_cache_->task_runner_.persist_data_task_;
  persist_data_task.is_inited_ = false;
  ob_usleep(10 * 1000);

  ObSSMemBlock *sealed_mem_blk = nullptr;
  const int64_t micro_size = (1L << 14);
  const int64_t micro_cnt = 10;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);

  // 1. Add some micro_block into mem_block and half of them are duplicated
  {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(8888);
    for (int64_t i = 0; i < micro_cnt; ++i) {
      const int64_t second_id = (i < micro_cnt / 2) ? 1000 : (i + 1);
      ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, second_id, micro_size);
      ObSSMemBlockHandle mem_blk_handle;
      uint32_t micro_crc = 0;
      bool alloc_new = false;
      ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.add_micro_block_data(micro_key, data_buf, micro_size, mem_blk_handle, micro_crc, alloc_new));
      ASSERT_EQ(true, mem_blk_handle.is_valid());
      bool first_add = false;
      const uint64_t effective_tablet_id = micro_key.get_macro_tablet_id().id();
      ASSERT_EQ(OB_SUCCESS,
          micro_meta_mgr.add_or_update_micro_block_meta(micro_key, micro_size, micro_crc,
          effective_tablet_id, mem_blk_handle, first_add));
    }
    sealed_mem_blk = mem_blk_mgr.fg_mem_blk_;
    ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.inner_seal_and_alloc_mem_block(true /* is_fg */));
    ASSERT_EQ(1, mem_blk_mgr.get_sealed_mem_block_cnt());
    ASSERT_EQ(true, sealed_mem_blk->can_flush());
  }

  // 2. Check some field of mem_blk and execute persiste_task
  {
    const int64_t valid_cnt = 1 + micro_cnt / 2;
    const int64_t valid_size = valid_cnt * micro_size;
    ASSERT_EQ(micro_cnt, sealed_mem_blk->micro_cnt_);
    ASSERT_EQ(micro_cnt * micro_size, sealed_mem_blk->total_data_size_);
    ASSERT_EQ(valid_cnt, sealed_mem_blk->valid_cnt_);
    ASSERT_EQ(valid_size, sealed_mem_blk->valid_val_);

    ObSSMicroCacheMicroStat &micro_stat = micro_cache_->cache_stat_.micro_stat();
    ASSERT_EQ(valid_cnt, micro_stat.total_micro_cnt_);
    ASSERT_EQ(valid_cnt, micro_stat.valid_micro_cnt_);
    ASSERT_EQ(valid_size, micro_stat.total_micro_size_);
    ASSERT_EQ(valid_size, micro_stat.valid_micro_size_);
    ASSERT_EQ(OB_SUCCESS, persist_data_task.persist_data_op_.persist_sealed_mem_blocks());
    ASSERT_EQ(0, mem_blk_mgr.get_sealed_mem_block_cnt());
  }
}

/* If micro_block fail to persist, it's meta will be erase from micro_meta_map. */
TEST_F(TestSSPersistMicroDataTask, persist_abnormal_micro_block)
{
  LOG_INFO("TEST_CASE: start persist_abnormal_micro_block");
  ObArenaAllocator allocator;
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache_->micro_meta_mgr_;
  ObSSMicroCacheMicroStat &micro_stat = micro_cache_->cache_stat_.micro_stat();
  ObSSMicroMetaManager::SSMicroMetaMap &micro_map = micro_meta_mgr.micro_meta_map_;
  ObSSPersistMicroDataTask &persist_data_task = micro_cache_->task_runner_.persist_data_task_;
  persist_data_task.is_inited_ = false;
  ob_usleep(10 * 1000);

  ObArray<ObSSMicroBlockCacheKey> micro_key_arr;
  const int64_t micro_cnt = 20;
  // 1. Add some micro_block
  {
    const int64_t MACRO_BLK_CNT = 1;
    const int64_t start_macro_id = 1;
    add_batch_micro_block(start_macro_id, MACRO_BLK_CNT, micro_cnt, micro_key_arr);
    ASSERT_EQ(OB_SUCCESS, mem_blk_mgr.inner_seal_and_alloc_mem_block(true /* is_fg */));
    ASSERT_EQ(1, mem_blk_mgr.get_sealed_mem_block_cnt());
  }

  // 2. Mock two micro_meta abnormal
  {
    for (int64_t i = 0; i < 2; ++i) {
      ObSSMicroBlockMetaHandle micro_handle;
      const ObSSMicroBlockCacheKey &micro_key = micro_key_arr[i];
      ASSERT_EQ(OB_SUCCESS, micro_map.get(&micro_key, micro_handle));
      ASSERT_EQ(false, micro_handle()->is_data_persisted_);
      micro_handle()->access_time_s_ = 0;
    }
  }

  // 3. Execute persist_data_task and check for abnormal meta does not exist
  {
    ASSERT_EQ(OB_SUCCESS, persist_data_task.persist_data_op_.persist_sealed_mem_blocks());
    ASSERT_EQ(0, mem_blk_mgr.get_sealed_mem_block_cnt());
    for (int64_t i = 0; i < micro_key_arr.count(); ++i) {
      ObSSMicroBlockMetaHandle micro_handle;
      const ObSSMicroBlockCacheKey &micro_key = micro_key_arr[i];
      if (i < 2) {
        ASSERT_EQ(OB_SUCCESS, micro_map.get(&micro_key, micro_handle));
        ASSERT_EQ(true, micro_handle()->is_meta_deleted_);
      } else {
        ASSERT_EQ(OB_SUCCESS, micro_map.get(&micro_key, micro_handle));
        ASSERT_EQ(true, micro_handle()->is_data_persisted_);
      }
    }
    ASSERT_EQ(micro_cnt, micro_stat.total_micro_cnt_);
    ASSERT_EQ(2, micro_stat.mark_del_micro_cnt_);
    ASSERT_EQ(micro_cnt - 2, micro_stat.valid_micro_cnt_);
  }
}


/* If persist_data_task fail to alloc any phy_block to flush sealed_mem_block, it will exit from loop */
TEST_F(TestSSPersistMicroDataTask, persist_task_dead_loop)
{
  LOG_INFO("TEST_CASE: start persist_task_dead_loop");
  ObSSMemBlockManager &mem_blk_mgr = micro_cache_->mem_blk_mgr_;
  ObSSPersistMicroDataTask &persist_data_task = micro_cache_->task_runner_.persist_data_task_;
  persist_data_task.persist_data_op_.is_inited_ = false;

  // 1. mock cache_data_block used up.
  ObSSPhyBlockCountInfo &blk_cnt_info = micro_cache_->phy_blk_mgr_.blk_cnt_info_;
  const int64_t max_data_blk_cnt = blk_cnt_info.micro_data_blk_max_cnt();
  blk_cnt_info.data_blk_.max_cnt_ = max_data_blk_cnt;
  blk_cnt_info.data_blk_.hold_cnt_ = max_data_blk_cnt;
  blk_cnt_info.data_blk_.used_cnt_ = max_data_blk_cnt;
  blk_cnt_info.shared_blk_used_cnt_ = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;

  // 2. add micro block to make some sealed_mem_blocks.
  const int64_t MACRO_BLK_CNT = mem_blk_mgr.mem_blk_pool_.max_fg_count_;
  const int64_t start_macro_id = 1;
  const int64_t micro_cnt = 20;
  ObArray<ObSSMicroBlockCacheKey> micro_key_arr;
  add_batch_micro_block(start_macro_id, MACRO_BLK_CNT, micro_cnt, micro_key_arr);
  const int64_t sealed_mem_block_cnt = mem_blk_mgr.get_sealed_mem_block_cnt();
  ASSERT_LT(0, sealed_mem_block_cnt);

  persist_data_task.persist_data_op_.is_inited_ = true;
  ob_usleep(1000 * 1000);
  // 3. test persist_data_task can exit loop.
  ObTenantBase *tenant_base = MTL_CTX();
  bool stop = false;
  std::thread t([&]() {
    ObTenantEnv::set_tenant(tenant_base);
    micro_cache_->task_runner_.stop();
    micro_cache_->task_runner_.wait();
    ATOMIC_STORE(&stop, true);
  });

  const int64_t timeout_us = 10 * 1000 * 1000L;
  const int64_t start_us = ObTimeUtility::current_time();
  while (ATOMIC_LOAD(&stop) == false) {
    ob_usleep(1000);
    if (ObTimeUtility::current_time() - start_us > timeout_us) {
      break;
    }
  }
  ASSERT_EQ(true, ATOMIC_LOAD(&stop));
  ASSERT_GE(sealed_mem_block_cnt, mem_blk_mgr.get_sealed_mem_block_cnt());
  t.join();
}
}  // namespace storage
}  // namespace oceanbase
int main(int argc, char **argv)
{
  system("rm -f test_ss_persist_micro_data_task.log*");
  OB_LOGGER.set_file_name("test_ss_persist_micro_data_task.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}