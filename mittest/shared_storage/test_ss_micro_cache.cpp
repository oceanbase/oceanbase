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
#include <gtest/gtest.h>

#define protected public
#define private public
#include "test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::hash;
using namespace oceanbase::blocksstable;

class TestSSMicroCache : public ::testing::Test
{
public:
  TestSSMicroCache() {}
  virtual ~TestSSMicroCache() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void set_basic_read_io_info(ObIOInfo &io_info);
public:
  struct TestSSMicroCacheCtx
  {
  public:
    TestSSMicroCacheCtx()
        : micro_block_info_arr_(),
          micro_key_map_(),
          macro_blk_cnt_(0),
          micro_blk_cnt_(0),
          thread_num_(0),
          random_(false)
    {}
    ~TestSSMicroCacheCtx() { reset(); }
    void reset()
    {
      micro_block_info_arr_.destroy();
      micro_key_map_.destroy();
      macro_blk_cnt_ = 0;
      micro_blk_cnt_ = 0;
      thread_num_ = 0;
      random_ = false;
    }

  public:
    ObArray<TestSSCommonUtil::MicroBlockInfo> micro_block_info_arr_;
    ObHashMap<ObSSMicroBlockCacheKey, int64_t> micro_key_map_;  // map<micro_key, access_cnt>
    int64_t macro_blk_cnt_;
    int64_t micro_blk_cnt_;
    int64_t thread_num_;
    bool random_;
  };

public:
  class TestSSMicroCacheThread : public Threads
  {
  public:
    enum class TestParallelType
    {
      TEST_PARALLEL_GET_MICRO_BLOCK,
      TEST_PARALLEL_ADD_MICRO_BLOCK,
    };

  public:
    TestSSMicroCacheThread(ObTenantBase *tenant_base, TestSSMicroCacheCtx &ctx, TestParallelType type)
        : tenant_base_(tenant_base), ctx_(ctx), type_(type), fail_cnt_(0) {}
    void run(int64_t idx) final;
    int64_t get_fail_cnt() { return ATOMIC_LOAD(&fail_cnt_); }
  private:
    int parallel_get_micro_block(int64_t idx);
    int parallel_add_micro_block(int64_t idx);

  private:
    ObTenantBase *tenant_base_;
    TestSSMicroCacheCtx &ctx_;
    TestParallelType type_;
    int64_t fail_cnt_;
  };
};

void TestSSMicroCache::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMicroCache::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMicroCache::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 30))); // 1G
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
}

void TestSSMicroCache::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSMicroCache::set_basic_read_io_info(ObIOInfo &io_info)
{
  io_info.tenant_id_ = MTL_ID();
  io_info.timeout_us_ = 5 * 1000 * 1000L; // 5s
  io_info.flag_.set_read();
  io_info.flag_.set_wait_event(1);
}

void TestSSMicroCache::TestSSMicroCacheThread::run(int64_t idx)
{
  ObTenantEnv::set_tenant(tenant_base_);
  if (type_ == TestParallelType::TEST_PARALLEL_GET_MICRO_BLOCK) {
    parallel_get_micro_block(idx);
  } else if (type_ == TestParallelType::TEST_PARALLEL_ADD_MICRO_BLOCK) {
    parallel_add_micro_block(idx);
  }
}

int TestSSMicroCache::TestSSMicroCacheThread::parallel_get_micro_block(int64_t idx)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  char *read_buf = nullptr;
  if (OB_ISNULL(read_buf = static_cast<char*>(allocator.alloc(DEFAULT_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else {
    ObArray<TestSSCommonUtil::MicroBlockInfo> micro_arr;
    for (int64_t i = idx; OB_SUCC(ret) && i < ctx_.micro_block_info_arr_.count(); i += 10) {
      TestSSCommonUtil::MicroBlockInfo micro_info = ctx_.micro_block_info_arr_[i];
      if (OB_FAIL(TestSSCommonUtil::get_micro_block(micro_info, read_buf))) {
        LOG_WARN("fail to get micro_block", KR(ret), K(micro_info));
      } else if (OB_FAIL(micro_arr.push_back(micro_info))) {
        LOG_WARN("fail to push micro info", KR(ret), K(micro_info));
      }
    }

    for (int64_t i = idx; OB_SUCC(ret) && i < micro_arr.count(); ++i) {
      TestSSCommonUtil::MicroBlockInfo micro_info = micro_arr[i];
      if (OB_FAIL(TestSSCommonUtil::get_micro_block(micro_info, read_buf))) {
        LOG_WARN("fail to get micro_block", KR(ret), K(micro_info));
      }
    }
  }
  if (OB_FAIL(ret)) {
    ATOMIC_INC(&fail_cnt_);
  }
  return ret;
}

int TestSSMicroCache::TestSSMicroCacheThread::parallel_add_micro_block(int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ctx_.macro_blk_cnt_ <= 0 || ctx_.micro_blk_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ctx", KR(ret), K_(ctx_.macro_blk_cnt), K_(ctx_.micro_blk_cnt));
  } else {
    ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
    ObArenaAllocator allocator;
    const int64_t payload_offset =
        ObSSPhyBlockCommonHeader::get_serialize_size() + ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
    const int32_t micro_index_size = sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN;
    const int32_t micro_cnt = ctx_.micro_blk_cnt_;
    const int32_t micro_size = (DEFAULT_BLOCK_SIZE - payload_offset) / micro_cnt - micro_index_size;
    char *data_buf = nullptr;
    if (OB_ISNULL(data_buf = static_cast<char *>(allocator.alloc(micro_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(micro_size));
    } else {
      MEMSET(data_buf, 'a', micro_size);
      for (int64_t i = 1; OB_SUCC(ret) && i <= ctx_.macro_blk_cnt_; ++i) {
        const int64_t second_id = (ctx_.random_ ? ObRandom::rand(1, ctx_.thread_num_) : idx) * ctx_.macro_blk_cnt_ + i;
        const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(second_id);
        for (int64_t j = 0; OB_SUCC(ret) && j < micro_cnt; ++j) {
          const int32_t offset = payload_offset + j * micro_size;
          const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
          if (OB_FAIL(micro_cache->add_micro_block_cache(
                  micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE))) {
            if (OB_SS_MICRO_CACHE_DISABLED == ret) {
              LOG_WARN("micro_cache disabled, fail to add micro block", KR(ret), K(idx), K(micro_key));
            } else if (OB_EAGAIN == ret || OB_SS_CACHE_REACH_MEM_LIMIT == ret){
              ret = OB_SUCCESS;  // ignore fail to allocate mem_block, normal_phy_blk or micro_meta.
            } else {
              LOG_WARN("fail to add micro_block into cache", KR(ret), K(idx), K(micro_key));
            }
          } else {
            struct UpdateOp
            {
              void operator()(HashMapPair<ObSSMicroBlockCacheKey, int64_t> &pair) { pair.second++; }
            };
            UpdateOp update_op;
            if (OB_FAIL(ctx_.micro_key_map_.set_or_update(micro_key, 1, update_op))) {
              LOG_WARN("fail to set_or_update micro_key", KR(ret), K(micro_key));
            }
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    ATOMIC_INC(&fail_cnt_);
  }
  return ret;
}

TEST_F(TestSSMicroCache, test_get_micro_block)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_get_micro_block");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;

  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSARCInfo &arc_info = micro_meta_mgr.arc_info_;
  const int64_t total_data_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt();
  const int32_t block_size = phy_blk_mgr.block_size_;
  const int64_t macro_blk_cnt = MIN(4, total_data_blk_cnt);
  ASSERT_LT(0, arc_info.p_);
  int64_t available_prewarm_size = 0;
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_available_space_for_prewarm(available_prewarm_size));
  ASSERT_EQ(arc_info.limit_, available_prewarm_size);

  // 1. write data into object_storage
  ObArray<TestSSCommonUtil::MicroBlockInfo> micro_block_info_arr;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::prepare_micro_blocks(macro_blk_cnt, block_size, micro_block_info_arr));
  const int64_t micro_cnt = micro_block_info_arr.count();
  ASSERT_LT(0, micro_cnt);
  ASSERT_EQ(0, cache_stat.hit_stat().add_cnt_);

  // 2. get these micro_block and add it into micro_cache
  ObArenaAllocator allocator;
  for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt); ++i) {
    TestSSCommonUtil::MicroBlockInfo &cur_info = micro_block_info_arr.at(i);
    char *read_buf = static_cast<char*>(allocator.alloc(cur_info.size_));
    ASSERT_NE(nullptr, read_buf);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::get_micro_block(cur_info, read_buf));
  }
  ASSERT_EQ(micro_cnt, cache_stat.micro_stat().total_micro_cnt_);
  ASSERT_EQ(micro_cnt, cache_stat.hit_stat().add_cnt_);

  // 3. wait some time for persist_task
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

  // 4. find a persisted micro_block
  int64_t persist_idx = -1;
  ObSSMicroSnapshotInfo micro_snapshot_info;
  ObSSMicroBlockMetaHandle micro_meta_handle;
  ObSSMemBlockHandle mem_blk_handle;
  ObSSPhysicalBlockHandle phy_blk_handle;
  ObSSMicroBlockMeta *cur_micro_meta = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && (i < micro_cnt); ++i) {
    micro_snapshot_info.reset();
    micro_meta_handle.reset();
    mem_blk_handle.reset();
    phy_blk_handle.reset();

    TestSSCommonUtil::MicroBlockInfo &cur_info = micro_block_info_arr.at(i);
    ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(cur_info.macro_id_, cur_info.offset_, cur_info.size_);
    ObSSCacheHitType hit_type;
    ASSERT_EQ(OB_SUCCESS, micro_cache->inner_get_micro_block_handle(micro_key, micro_snapshot_info, micro_meta_handle, mem_blk_handle,
              phy_blk_handle, hit_type, true));
    if (hit_type == ObSSCacheHitType::SS_CACHE_HIT_DISK) {
      persist_idx = i;
      ASSERT_EQ(true, micro_meta_handle.is_valid());
      ASSERT_EQ(true, phy_blk_handle.is_valid());
      ASSERT_EQ(true, micro_snapshot_info.is_valid());
      cur_micro_meta = micro_meta_handle.get_ptr();
      ASSERT_EQ(false, cur_micro_meta->is_in_l1_);
      ASSERT_EQ(false, cur_micro_meta->is_in_ghost_);
      ASSERT_EQ(true, cur_micro_meta->is_persisted_);
      break;
    }
  }
  ASSERT_NE(-1, persist_idx);

  // 5. mock evict this micro_block
  cur_micro_meta->is_in_ghost_ = true;
  cur_micro_meta->mark_invalid();

  // 6. fetch this evict micro_block again, just like OB_ENTRY_NOT_EXIST
  {
    TestSSCommonUtil::MicroBlockInfo &cur_info = micro_block_info_arr.at(persist_idx);
    char *read_buf = static_cast<char*>(allocator.alloc(cur_info.size_));
    ASSERT_NE(nullptr, read_buf);
    ObSSMicroBlockCacheKey tmp_micro_key = cur_micro_meta->micro_key_;
    ObSSMicroBlockMetaHandle tmp_micro_handle;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_cache->micro_meta_mgr_.get_micro_block_meta_handle(tmp_micro_key, tmp_micro_handle, false));
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::get_micro_block(cur_info, read_buf));
    ASSERT_EQ(true, cur_micro_meta->is_valid());
    ASSERT_EQ(false, cur_micro_meta->is_in_l1_);
    ASSERT_EQ(false, cur_micro_meta->is_in_ghost_);
    ASSERT_EQ(micro_cnt + 1, cache_stat.hit_stat().add_cnt_);
    ASSERT_EQ(micro_cnt + 1, cache_stat.hit_stat().new_add_cnt_);
  }

  allocator.clear();
}

/*
  Multiple threads read micro blocks in parallel, read req may hit object storage, disk or memory.
*/
TEST_F(TestSSMicroCache, test_parallel_get_micro_block)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_parallel_get_micro_block");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;

  TestSSMicroCacheCtx ctx;
  const int64_t total_macro_blk_cnt = 40;
  ASSERT_EQ(OB_SUCCESS,
      TestSSCommonUtil::prepare_micro_blocks(total_macro_blk_cnt, DEFAULT_BLOCK_SIZE, ctx.micro_block_info_arr_));
  ctx.thread_num_ = 8;
  TestSSMicroCache::TestSSMicroCacheThread threads(
      ObTenantEnv::get_tenant(), ctx, TestSSMicroCacheThread::TestParallelType::TEST_PARALLEL_GET_MICRO_BLOCK);
  threads.set_thread_count(ctx.thread_num_);
  threads.start();
  threads.wait();
  ASSERT_EQ(0, threads.get_fail_cnt());
}

TEST_F(TestSSMicroCache, test_get_micro_block_cache)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_get_micro_block_cache");
  ObSSPhysicalBlockManager &phy_blk_mgr = MTL(ObSSMicroCache *)->phy_blk_mgr_;
  const int64_t total_data_blk_cnt = 5;
  const int32_t block_size = phy_blk_mgr.block_size_;
  TestSSMicroCacheCtx ctx;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::prepare_micro_blocks(total_data_blk_cnt, block_size, ctx.micro_block_info_arr_));

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ASSERT_LT(0, ctx.micro_block_info_arr_.count());
  TestSSCommonUtil::MicroBlockInfo &cur_info = ctx.micro_block_info_arr_.at(0);
  ObSSMicroBlockCacheKey cur_micro_key = TestSSCommonUtil::gen_phy_micro_key(cur_info.macro_id_,
                                                                             cur_info.offset_,
                                                                             cur_info.size_);
  ObArenaAllocator allocator;
  char *read_buf = static_cast<char *>(allocator.alloc(block_size));
  ASSERT_NE(nullptr, read_buf);

  // 1. get cached data, expect get empty
  ObIOInfo io_info;
  set_basic_read_io_info(io_info);
  io_info.user_data_buf_ = read_buf;
  io_info.size_ = cur_info.size_;
  ObStorageObjectHandle obj_handle;
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_cached_micro_block(cur_micro_key, io_info, obj_handle,
                                     ObSSMicroCacheAccessType::REPLICA_PREWARM_TYPE));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(0, obj_handle.get_data_size());
  obj_handle.reset();

  // 2. get data for adding cache, expect get data and add cache
  io_info.reset();
  set_basic_read_io_info(io_info);
  io_info.user_data_buf_ = read_buf;
  io_info.offset_ = cur_info.offset_;
  io_info.size_ = cur_info.size_;
  ObSSMicroBlockId cur_ss_micro_id = ObSSMicroBlockId(cur_info.macro_id_, cur_info.offset_, cur_info.size_);
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(cur_micro_key, cur_ss_micro_id,
                                     MicroCacheGetType::GET_CACHE_MISS_DATA, io_info, obj_handle,
                                     ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(cur_info.size_, obj_handle.get_data_size());
  obj_handle.reset();

  // 3. get cached data, expect get data
  io_info.reset();
  set_basic_read_io_info(io_info);
  io_info.user_data_buf_ = read_buf;
  io_info.size_ = cur_info.size_;
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_cached_micro_block(cur_micro_key, io_info, obj_handle,
                                     ObSSMicroCacheAccessType::REPLICA_PREWARM_TYPE));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(cur_info.size_, obj_handle.get_data_size());
  obj_handle.reset();

  // 4. get data for adding cache, expect hit memory, get empty data and do not add again
  io_info.reset();
  set_basic_read_io_info(io_info);
  io_info.user_data_buf_ = read_buf;
  io_info.offset_ = cur_info.offset_;
  io_info.size_ = cur_info.size_;
  ObSSMicroSnapshotInfo micro_snapshot_info;
  ObSSCacheHitType hit_type;
  ASSERT_EQ(OB_SUCCESS, micro_cache->check_micro_block_exist(cur_micro_key, micro_snapshot_info, hit_type));
  EXPECT_EQ(ObSSCacheHitType::SS_CACHE_HIT_MEM, hit_type);
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(cur_micro_key, cur_ss_micro_id,
                                     MicroCacheGetType::GET_CACHE_MISS_DATA, io_info, obj_handle,
                                     ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(0, obj_handle.get_data_size());
  obj_handle.reset();

  // 5. trigger flush mem_block to micro_cache_file
  io_info.reset();
  set_basic_read_io_info(io_info);
  io_info.user_data_buf_ = read_buf;
  io_info.offset_ = 1;
  io_info.size_ = block_size / 2;
  ObSSMicroBlockCacheKey tmp_micro_key1 = TestSSCommonUtil::gen_phy_micro_key(cur_info.macro_id_,
                                                                              io_info.offset_,
                                                                              io_info.size_);
  ObSSMicroBlockId tmp_ss_micro_id1 = ObSSMicroBlockId(cur_info.macro_id_, io_info.offset_, io_info.size_);
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(tmp_micro_key1, tmp_ss_micro_id1,
                                     MicroCacheGetType::GET_CACHE_MISS_DATA, io_info, obj_handle,
                                     ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(io_info.size_, obj_handle.get_data_size());
  obj_handle.reset();

  io_info.reset();
  set_basic_read_io_info(io_info);
  io_info.user_data_buf_ = read_buf;
  io_info.offset_ = 2;
  io_info.size_ = block_size / 2;
  ObSSMicroBlockCacheKey tmp_micro_key2 = TestSSCommonUtil::gen_phy_micro_key(cur_info.macro_id_,
                                                                              io_info.offset_,
                                                                              io_info.size_);
  ObSSMicroBlockId tmp_ss_micro_id2 = ObSSMicroBlockId(cur_info.macro_id_, io_info.offset_, io_info.size_);
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(tmp_micro_key2, tmp_ss_micro_id2,
                                     MicroCacheGetType::GET_CACHE_MISS_DATA, io_info, obj_handle,
                                     ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(io_info.size_, obj_handle.get_data_size());
  obj_handle.reset();
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());

  // 6. get data for adding cache, expect hit disk, get empty data and do not add again
  io_info.reset();
  set_basic_read_io_info(io_info);
  io_info.user_data_buf_ = read_buf;
  io_info.offset_ = cur_info.offset_;
  io_info.size_ = cur_info.size_;
  ASSERT_EQ(OB_SUCCESS, micro_cache->check_micro_block_exist(cur_micro_key, micro_snapshot_info, hit_type));
  EXPECT_EQ(ObSSCacheHitType::SS_CACHE_HIT_DISK, hit_type);
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(cur_micro_key, cur_ss_micro_id,
                                     MicroCacheGetType::GET_CACHE_MISS_DATA, io_info, obj_handle,
                                     ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(0, obj_handle.get_data_size());
  obj_handle.reset();

  // 7. move micro block to ghost, then get cached data, expect get empty data
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSMicroBlockMetaHandle micro_meta_handle;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.micro_meta_map_.get(&cur_micro_key, micro_meta_handle));
  ASSERT_EQ(true, micro_meta_handle.is_valid());
  micro_meta_handle()->is_in_ghost_ = true;
  micro_meta_handle()->mark_invalid();

  io_info.reset();
  set_basic_read_io_info(io_info);
  io_info.user_data_buf_ = read_buf;
  io_info.size_ = cur_info.size_;
  ASSERT_EQ(OB_SUCCESS, micro_cache->check_micro_block_exist(cur_micro_key, micro_snapshot_info, hit_type));
  EXPECT_EQ(ObSSCacheHitType::SS_CACHE_MISS, hit_type);
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_cached_micro_block(cur_micro_key, io_info, obj_handle,
                                     ObSSMicroCacheAccessType::REPLICA_PREWARM_TYPE));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(0, obj_handle.get_data_size());
  obj_handle.reset();
}

/*
  Test five scenarios:
    1. add micro_block for the first time.
    2. add micro_block which has been added into T1.
    3. micro_block is evicted to ghost and added again.
*/
TEST_F(TestSSMicroCache, test_add_micro_block_cache)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_add_micro_block_cache");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;

  // Scenario 1
  const int64_t micro_size = 128;
  const int64_t offset = 1;
  const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(888888);
  const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
  char data_buf[micro_size];
  MEMSET(data_buf, 'c', micro_size);
  ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(
                micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE));

  ObSSMicroBlockMetaHandle micro_meta_handle;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
  ASSERT_EQ(true, micro_meta_handle()->is_in_l1());
  ASSERT_EQ(false, micro_meta_handle()->is_in_ghost());

  // Scenario 2
  micro_meta_handle.reset();
  ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(
                micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
  ASSERT_EQ(false, micro_meta_handle()->is_in_l1());
  ASSERT_EQ(false, micro_meta_handle()->is_in_ghost());

  // Scenario 3
  micro_meta_handle()->mark_invalid();
  micro_meta_handle()->is_in_ghost_ = true;

  micro_meta_handle.reset();
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
  micro_meta_handle.reset();
  ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(
                micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
  ASSERT_EQ(false, micro_meta_handle()->is_in_l1());
  ASSERT_EQ(false, micro_meta_handle()->is_in_ghost());
}

/*  parallelly add micro_block randomly. */
TEST_F(TestSSMicroCache, test_parallel_add_micro_block_randomly)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_parallel_add_micro_block_randomly");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;

  TestSSMicroCacheCtx ctx;
  ctx.macro_blk_cnt_ = 20;
  ctx.micro_blk_cnt_ = 128;
  ctx.thread_num_ = 5;
  ctx.random_ = true;
  ASSERT_EQ(OB_SUCCESS, ctx.micro_key_map_.create(1024,  ObMemAttr(MTL_ID(), "test")));
  TestSSMicroCache::TestSSMicroCacheThread threads(
      ObTenantEnv::get_tenant(), ctx, TestSSMicroCacheThread::TestParallelType::TEST_PARALLEL_ADD_MICRO_BLOCK);
  threads.set_thread_count(ctx.thread_num_);
  threads.start();
  threads.wait();
  ASSERT_EQ(0, threads.get_fail_cnt());
  ASSERT_LT(0, ctx.micro_key_map_.size());

  for (auto iter = ctx.micro_key_map_.begin(); iter != ctx.micro_key_map_.end(); ++iter) {
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(iter->first, micro_meta_handle, false));
    if (iter->second == 1) {
      ASSERT_EQ(true, micro_meta_handle()->is_in_l1());
    } else {
      ASSERT_EQ(false, micro_meta_handle()->is_in_l1());
    }
  }
}

/*
  Test two scenarios:
    1. add micro_block with transfer_seg = false.
    2. add micro_block with transfer_seg = true.
*/
TEST_F(TestSSMicroCache, test_add_micro_block_cache_for_prewarm)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_add_micro_block_cache_for_prewarm");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;

  // Scenario 1
  const int64_t micro_size = 128;
  const int64_t offset = 1;
  const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(888888);
  const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
  char data_buf[micro_size];
  MEMSET(data_buf, 'c', micro_size);
  ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache_for_prewarm(
                micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE, 5, false /*transfer_seg*/));

  ObSSMicroBlockMetaHandle micro_meta_handle;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
  ASSERT_EQ(true, micro_meta_handle()->is_in_l1());
  ASSERT_EQ(false, micro_meta_handle()->is_in_ghost());
  micro_meta_handle.reset();

  // Scenario 2
  const ObSSMicroBlockCacheKey micro_key2 = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset + micro_size, micro_size);
  ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache_for_prewarm(
                micro_key2, data_buf, micro_size, ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE, 5, true /*transfer_seg*/));

  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key2, micro_meta_handle, false));
  ASSERT_EQ(false, micro_meta_handle()->is_in_l1());
  ASSERT_EQ(false, micro_meta_handle()->is_in_ghost());
  micro_meta_handle.reset();
}

/*
  key0 in T, key1 in B, key2 in memory, key3 has never been cached.
  key1 and key 3 will be choosen when call get_not_exist_micro_blocks().
*/
TEST_F(TestSSMicroCache, test_get_not_exist_micro_blocks)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_get_not_exist_micro_blocks");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSReleaseCacheTask &arc_task = micro_cache->task_runner_.release_cache_task_;
  arc_task.is_inited_ = false;

  TestSSMicroCacheCtx ctx;
  ctx.macro_blk_cnt_ = 5;
  ctx.micro_blk_cnt_ = 20;
  ctx.thread_num_ = 4;
  ASSERT_EQ(OB_SUCCESS, ctx.micro_key_map_.create(1024,  ObMemAttr(MTL_ID(), "test")));
  TestSSMicroCache::TestSSMicroCacheThread threads(
      ObTenantEnv::get_tenant(), ctx, TestSSMicroCacheThread::TestParallelType::TEST_PARALLEL_ADD_MICRO_BLOCK);
  threads.set_thread_count(4);
  threads.start();
  threads.wait();
  ASSERT_EQ(0, threads.get_fail_cnt());

  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(888888);
  const int64_t micro_size = 128;
  char data_buf[micro_size];
  MEMSET(data_buf, 'c', micro_size);
  ObArray<ObSSMicroBlockCacheKey> persisted_micro_keys;
  for (auto iter = ctx.micro_key_map_.begin(); iter != ctx.micro_key_map_.end(); ++iter) {
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(iter->first, micro_meta_handle, false));
    if (micro_meta_handle()->is_persisted()) {
      ASSERT_EQ(OB_SUCCESS, persisted_micro_keys.push_back(iter->first));
    }
  }
  ASSERT_LE(2, persisted_micro_keys.size());
  ObSSMicroBlockCacheKey micro_key0 = persisted_micro_keys[0];
  ObSSMicroBlockCacheKey micro_key1 = persisted_micro_keys[1];
  ObSSMicroBlockCacheKey micro_key2 = TestSSCommonUtil::gen_phy_micro_key(macro_id, 1111, micro_size);
  ObSSMicroBlockCacheKey micro_key3 = TestSSCommonUtil::gen_phy_micro_key(macro_id, 2222, micro_size);

  // key0
  ObSSMicroBlockMetaHandle micro_meta_handle;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key0, micro_meta_handle, false));
  ASSERT_EQ(true, micro_meta_handle()->is_persisted());
  ASSERT_EQ(false, micro_meta_handle()->is_in_ghost());
  ASSERT_EQ(true, micro_meta_handle()->is_in_l1());

  // key1
  micro_meta_handle.reset();
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key1, micro_meta_handle, false));
  ASSERT_EQ(true, micro_meta_handle()->is_persisted());
  micro_meta_handle()->is_in_ghost_ = true;
  micro_meta_handle()->mark_invalid();

  // key2
  micro_meta_handle.reset();
  ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(
                micro_key2, data_buf, micro_size, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key2, micro_meta_handle, false));
  ASSERT_EQ(false, micro_meta_handle()->is_persisted());
  ASSERT_EQ(false, micro_meta_handle()->is_in_ghost());
  ASSERT_EQ(true, micro_meta_handle()->is_in_l1());

  ObArray<ObSSMicroBlockCacheKeyMeta> in_arr;
  ObArray<ObSSMicroBlockCacheKeyMeta> out_arr;
  ASSERT_EQ(OB_SUCCESS, in_arr.push_back(ObSSMicroBlockCacheKeyMeta(micro_key0, 0, micro_size, true)));
  ASSERT_EQ(OB_SUCCESS, in_arr.push_back(ObSSMicroBlockCacheKeyMeta(micro_key1, 0, micro_size, true)));
  ASSERT_EQ(OB_SUCCESS, in_arr.push_back(ObSSMicroBlockCacheKeyMeta(micro_key2, 0, micro_size, true)));
  ASSERT_EQ(OB_SUCCESS, in_arr.push_back(ObSSMicroBlockCacheKeyMeta(micro_key3, 0, micro_size, false)));
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_not_exist_micro_blocks(in_arr, out_arr));
  ASSERT_EQ(2, out_arr.size());
  ASSERT_EQ(micro_key1, out_arr[0].micro_key_);
  ASSERT_EQ(micro_key3, out_arr[1].micro_key_);
}

/*
  Test the effect of update_micro_block_heat when set transfer_seg, update_access_time true/false
*/
TEST_F(TestSSMicroCache, test_update_micro_block_heat)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_update_micro_block_heat");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  const int64_t micro_size = 128;
  const int64_t offset = 1;
  const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(888888);
  const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
  char data_buf[micro_size];
  MEMSET(data_buf, 'c', micro_size);
  ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache_for_prewarm(
                micro_key, data_buf, micro_size, ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE, 5, false /*transfer_seg*/));

  ObSSMicroBlockMetaHandle micro_meta_handle;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_meta_handle, false));
  ASSERT_EQ(true, micro_meta_handle()->is_in_l1());
  const int64_t old_heat = micro_meta_handle()->get_heat_val();

  ObArray<ObSSMicroBlockCacheKey> micro_keys;
  ASSERT_EQ(OB_SUCCESS, micro_keys.push_back(micro_key));

  ob_usleep(2 * 1000 * 1000); // in order to make the updated heat_val larger
  ASSERT_EQ(OB_SUCCESS, micro_cache->update_micro_block_heat(micro_keys, false/*transfer_seg*/, false/*update_access_time*/, 0));
  ASSERT_EQ(old_heat, micro_meta_handle()->get_heat_val());
  ASSERT_EQ(true, micro_meta_handle()->is_in_l1());

  ASSERT_EQ(OB_SUCCESS, micro_cache->update_micro_block_heat(micro_keys, false/*transfer_seg*/, true/*update_access_time*/, 0));
  ASSERT_LT(old_heat, micro_meta_handle()->get_heat_val());
  ASSERT_EQ(true, micro_meta_handle()->is_in_l1());

  ASSERT_EQ(OB_SUCCESS, micro_cache->update_micro_block_heat(micro_keys, true/*transfer_seg */, true/*update_access_time*/, 0));
  ASSERT_EQ(false, micro_meta_handle()->is_in_l1());
}

TEST_F(TestSSMicroCache, test_get_available_space_for_prewarm)
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;

  int64_t available_space_size = 0;
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_available_space_for_prewarm(available_space_size));
  ASSERT_LT(0, available_space_size);

  available_space_size = 0;
  const int64_t max_data_blk_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt();
  phy_blk_mgr.blk_cnt_info_.data_blk_.used_cnt_ = max_data_blk_cnt;
  phy_blk_mgr.blk_cnt_info_.data_blk_.hold_cnt_ = max_data_blk_cnt;
  phy_blk_mgr.blk_cnt_info_.shared_blk_used_cnt_ =
      phy_blk_mgr.blk_cnt_info_.data_blk_.hold_cnt_ + phy_blk_mgr.blk_cnt_info_.meta_blk_.hold_cnt_;
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_available_space_for_prewarm(available_space_size));
  ASSERT_EQ(0, available_space_size);
}

TEST_F(TestSSMicroCache, test_free_space_for_prewarm)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_free_space_for_prewarm");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSARCInfo &arc_info = micro_cache->micro_meta_mgr_.arc_info_;

  const int64_t limit = arc_info.limit_;
  const int64_t work_limit = static_cast<int64_t>((static_cast<double>(limit * SS_ARC_LIMIT_SHRINK_PCT) / 100.0));
  micro_cache->begin_free_space_for_prewarm();
  ASSERT_EQ(work_limit, arc_info.work_limit_);
  micro_cache->begin_free_space_for_prewarm();
  ASSERT_EQ(work_limit, arc_info.work_limit_);

  micro_cache->finish_free_space_for_prewarm();
  ASSERT_EQ(limit, arc_info.work_limit_);
  micro_cache->finish_free_space_for_prewarm();
  ASSERT_EQ(limit, arc_info.work_limit_);
}

/*
  One third of the keys belong to the tablet which is in ls, and two thirds of the keys do not. In this case,
  call get_batch_la_micro_keys() will get ont third of the keys.
*/
TEST_F(TestSSMicroCache, test_get_batch_la_micro_keys)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_get_batch_la_micro_keys");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  // mock ls and tablet is inited
  const uint64_t tablet_id = 88888888;
  const uint64_t tablet_id_fake = 999999999;
  ObLS ls;
  ObLSTabletService *tablet_service = ls.get_tablet_svr();
  tablet_service->is_inited_ = true;
  tablet_service->tablet_id_set_.init(ObTabletCommon::BUCKET_LOCK_BUCKET_CNT, MTL_ID());
  tablet_service->bucket_lock_.init(
      ObTabletCommon::BUCKET_LOCK_BUCKET_CNT, ObLatchIds::TABLET_BUCKET_LOCK, "TabletSvrBucket", MTL_ID());
  ASSERT_EQ(OB_SUCCESS, ls.get_tablet_svr()->tablet_id_set_.set(ObTabletID(tablet_id)));

  bool is_filter = false;
  ASSERT_EQ(OB_SUCCESS, micro_cache->is_tablet_id_need_filter(&ls, tablet_id, is_filter));
  ASSERT_FALSE(is_filter);
  ASSERT_EQ(OB_SUCCESS, micro_cache->is_tablet_id_need_filter(&ls, tablet_id_fake, is_filter));
  ASSERT_TRUE(is_filter);

  const int64_t key_cnt = 3000;
  micro_cache->latest_access_micro_key_mgr_.is_stop_record_la_micro_key_ = false;
  for (int64_t i = 0; i < key_cnt; i++) {
    const uint64_t random_tablet_id = (i % 3 == 0) ? tablet_id : tablet_id_fake;
    const MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(random_tablet_id);
    const ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, i + 1, 1);
    const ObSSMicroBlockCacheKeyMeta micro_key_meta(micro_key, 0, 1, false);
    ASSERT_EQ(OB_SUCCESS, micro_cache->latest_access_micro_key_mgr_.push_latest_access_micro_key_to_hashset(micro_key_meta));
  }
  ASSERT_EQ(key_cnt, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.size());
  ObArray<ObSSMicroBlockCacheKeyMeta> keys;
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_batch_la_micro_keys(&ls, keys));
  ASSERT_EQ(key_cnt / 3, keys.count());
  ASSERT_EQ(key_cnt / 3 * 2, micro_cache->latest_access_micro_key_mgr_.latest_access_micro_key_set_.size());
}

/*
  Test three scenarios:
    1. used_data_block_cnt % split_cnt == 0
    2. used_data_block_cnt % split_cnt != 0
    3. used_data_block_cnt < split_cnt
 */
TEST_F(TestSSMicroCache, test_divide_phy_block_range)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_divide_phy_block_range");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObLSID ls_id(100);
  ObArray<ObSSPhyBlockIdxRange> block_ranges;

  // Scenario 1
  int64_t split_cnt = 3;
  const int64_t used_data_block_cnt =
      (phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt() / split_cnt) * split_cnt - split_cnt;
  const int64_t block_cnt_per_range = (used_data_block_cnt + split_cnt - 1) / split_cnt;
  for (int i = 0; i < used_data_block_cnt; i++) {
    int64_t block_idx = 0;
    ObSSPhysicalBlockHandle phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
    phy_blk_handle()->set_valid_len(1024);
  }
  ASSERT_EQ(OB_SUCCESS, micro_cache->divide_phy_block_range(ls_id, split_cnt, block_ranges));
  ASSERT_EQ(split_cnt, block_ranges.count());
  const int64_t start_idx = 1;
  for (int64_t i = 0; i < block_ranges.count(); ++i) {
    const int64_t l = block_ranges[i].start_blk_idx_;
    const int64_t r = block_ranges[i].end_blk_idx_;
    ASSERT_EQ(l, start_idx + i * block_cnt_per_range);
    ASSERT_EQ(r, l + block_cnt_per_range);
  }

  // Scenario 2
  const int64_t used_data_block_cnt2 = used_data_block_cnt + split_cnt - 1;
  const int64_t block_cnt_per_range2 = (used_data_block_cnt2 + split_cnt - 1) / split_cnt;
  while (phy_blk_mgr.get_data_block_used_cnt() < used_data_block_cnt2) {
    int64_t block_idx = 0;
    ObSSPhysicalBlockHandle phy_blk_handle;
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.alloc_block(block_idx, phy_blk_handle, ObSSPhyBlockType::SS_CACHE_DATA_BLK));
    phy_blk_handle()->set_valid_len(1024);
  }
  block_ranges.reset();
  ASSERT_EQ(OB_SUCCESS, micro_cache->divide_phy_block_range(ls_id, split_cnt, block_ranges));
  ASSERT_EQ(split_cnt, block_ranges.count());
  for (int64_t i = 0; i < block_ranges.count(); ++i) {
    const int64_t l = block_ranges[i].start_blk_idx_;
    const int64_t r = block_ranges[i].end_blk_idx_;
    ASSERT_EQ(l, start_idx + i * block_cnt_per_range2);
    if (i == block_ranges.count() - 1) {
      ASSERT_EQ(r, phy_blk_mgr.blk_cnt_info_.total_blk_cnt_ - 1);
    } else {
      ASSERT_EQ(r, l + block_cnt_per_range2);
    }
  }

  // Scenario 3
  split_cnt = used_data_block_cnt2 + 10;
  const int64_t block_cnt_per_range3 = (used_data_block_cnt2 + split_cnt - 1) / split_cnt;
  block_ranges.reset();
  ASSERT_EQ(OB_SUCCESS, micro_cache->divide_phy_block_range(ls_id, split_cnt, block_ranges));
  ASSERT_EQ(used_data_block_cnt2, block_ranges.count());
  ASSERT_EQ(1, block_cnt_per_range3);
  for (int64_t i = 0; i < block_ranges.count(); ++i) {
    const int64_t l = block_ranges[i].start_blk_idx_;
    const int64_t r = block_ranges[i].end_blk_idx_;
    ASSERT_EQ(l, start_idx + i * block_cnt_per_range3);
    ASSERT_EQ(r, l + block_cnt_per_range3);
  }
}

TEST_F(TestSSMicroCache, test_private_macro_cache_miss_cnt)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_private_macro_cache_miss_cnt");
  uint64_t tablet_id = 200001;
  uint64_t server_id = 1;

  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_transfer_seq_dir(MTL_ID(), MTL_EPOCH_ID(), tablet_id, 0/*transfer_seq*/));

  // 1. write
  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
  macro_id.set_second_id(tablet_id); // tablet_id
  macro_id.set_third_id(100); // seq_id
  macro_id.set_macro_transfer_seq(0); // tablet_transfer_seq
  macro_id.set_tenant_seq(server_id); // macro_seq
  ASSERT_TRUE(macro_id.is_valid());
  ObStorageObjectHandle write_object_handle;
  ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));

  // construct write info
  const int64_t size_2MB = 2 * 1024 * 1024L;
  char write_buf[size_2MB];
  write_buf[0] = '\0';
  const int64_t mid_offset = size_2MB / 2;
  memset(write_buf, 'a', mid_offset);
  memset(write_buf + mid_offset, 'b', size_2MB - mid_offset);
  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = size_2MB;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();

  ObSSPrivateMacroWriter private_macro_writer;
  ASSERT_EQ(OB_SUCCESS, private_macro_writer.aio_write(write_info, write_object_handle));
  ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());

  // 2. read, expect do not add cache_miss_cnt
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheStat &micro_cache_stat = micro_cache->get_micro_cache_stat();
  const int64_t ori_cache_miss_cnt = micro_cache_stat.hit_stat_.cache_miss_cnt_;

  // construct read info
  ObStorageObjectReadInfo read_info;
  char read_buf[size_2MB];
  read_buf[0] = '\0';
  read_info.io_desc_.set_wait_event(1);
  read_info.macro_block_id_ = macro_id;
  read_info.buf_ = read_buf;
  read_info.offset_ = 1;
  read_info.size_ = size_2MB / 2;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();

  ObStorageObjectHandle read_object_handle;
  ObSSPrivateMacroReader private_macro_reader;
  ASSERT_EQ(OB_SUCCESS, private_macro_reader.aio_read(read_info, read_object_handle));
  ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
  ASSERT_NE(nullptr, read_object_handle.get_buffer());
  ASSERT_EQ(read_info.size_, read_object_handle.get_data_size());
  read_object_handle.reset();

  ASSERT_EQ(ori_cache_miss_cnt, micro_cache_stat.hit_stat_.cache_miss_cnt_);
}

TEST_F(TestSSMicroCache, test_disable_micro_cache)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_disable_micro_cache");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  const int64_t block_cnt = 1;
  const int32_t block_size = DEFAULT_MACRO_BLOCK_SIZE;
  TestSSMicroCacheCtx ctx;
  ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::prepare_micro_blocks(block_cnt, block_size, ctx.micro_block_info_arr_));
  ObArenaAllocator allocator;
  char *read_buf = static_cast<char *>(allocator.alloc(block_size));
  ASSERT_NE(nullptr, read_buf);

  // ======================== `resize_micro_cache_file_size` ========================
  const int64_t micro_cache_file_size = (1L << 30);
  micro_cache->disable_cache();
  ASSERT_EQ(OB_SS_MICRO_CACHE_DISABLED, micro_cache->resize_micro_cache_file_size(micro_cache_file_size));
  micro_cache->enable_cache();
  ASSERT_EQ(OB_SUCCESS, micro_cache->resize_micro_cache_file_size(micro_cache_file_size));

  // ======================== `get_micro_block_cache` ========================
  ASSERT_EQ(false, ctx.micro_block_info_arr_.empty());
  TestSSCommonUtil::MicroBlockInfo &micro_info = ctx.micro_block_info_arr_.at(0);
  ObSSMicroBlockCacheKey micro_key =
      TestSSCommonUtil::gen_phy_micro_key(micro_info.macro_id_, micro_info.offset_, micro_info.size_);
  ObIOInfo io_info;
  set_basic_read_io_info(io_info);
  io_info.user_data_buf_ = read_buf;
  io_info.size_ = micro_info.size_;
  ObStorageObjectHandle obj_handle;
  ObSSMicroBlockMetaHandle micro_handle;

  // fail to get micro_block if disable cache
  micro_cache->disable_cache();
  ASSERT_EQ(OB_SS_MICRO_CACHE_DISABLED, micro_cache->get_micro_block_cache(micro_key, micro_key.micro_id_,
          MicroCacheGetType::FORCE_GET_DATA, io_info, obj_handle, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(0, obj_handle.get_data_size());
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_handle, false));

  // get micro_block from object_storage and add it into cache if enable cache
  micro_cache->enable_cache();
  micro_handle.reset();
  obj_handle.reset();
  io_info.reset();
  set_basic_read_io_info(io_info);
  io_info.user_data_buf_ = read_buf;
  io_info.size_ = micro_info.size_;
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_micro_block_cache(micro_key, micro_key.micro_id_,
          MicroCacheGetType::FORCE_GET_DATA, io_info, obj_handle, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(io_info.size_, obj_handle.get_data_size());
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_handle, false));

  // ======================== `add_micro_block_cache` ========================
  MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(888888);
  const int64_t offset = 88;
  const int64_t size = 88;
  char *micro_data =  static_cast<char *>(allocator.alloc(block_size));
  ASSERT_NE(nullptr, micro_data);
  MEMSET(micro_data, 'c', size);
  micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, size);

  // fail to add micro_block into cache if disable cache
  micro_cache->disable_cache();
  micro_handle.reset();
  ASSERT_EQ(OB_SS_MICRO_CACHE_DISABLED, micro_cache->add_micro_block_cache(micro_key, micro_data, size, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_handle, false));

  // succeed to add micro_block into cache if enable cache
  micro_cache->enable_cache();
  micro_handle.reset();
  ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_key, micro_data, size, ObSSMicroCacheAccessType::COMMON_IO_TYPE));
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_handle, false));

  // ======================== `get_cached_micro_block` ========================
  // fail to get micro_block for prewarm if disable cache
  micro_cache->disable_cache();
  obj_handle.reset();
  io_info.reset();
  set_basic_read_io_info(io_info);
  io_info.user_data_buf_ = read_buf;
  io_info.size_ = size;
  ASSERT_EQ(OB_SS_MICRO_CACHE_DISABLED, micro_cache->get_cached_micro_block(micro_key, io_info, obj_handle, ObSSMicroCacheAccessType::REPLICA_PREWARM_TYPE));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(0, obj_handle.get_data_size());

  // succeed to get micro_block for prewarm if enable cache
  micro_cache->enable_cache();
  obj_handle.reset();
  io_info.reset();
  set_basic_read_io_info(io_info);
  io_info.user_data_buf_ = read_buf;
  io_info.size_ = size;
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_cached_micro_block(micro_key, io_info, obj_handle, ObSSMicroCacheAccessType::REPLICA_PREWARM_TYPE));
  ASSERT_EQ(OB_SUCCESS, obj_handle.wait());
  ASSERT_EQ(io_info.size_, obj_handle.get_data_size());

  // ======================== `check_micro_block_exist` ========================
  // micro_block doesn't exist if disable cache
  micro_cache->disable_cache();
  ObSSMicroSnapshotInfo micro_snapshot_info;
  ObSSCacheHitType hit_type = ObSSCacheHitType::SS_CACHE_MISS;
  ASSERT_EQ(OB_SS_MICRO_CACHE_DISABLED, micro_cache->check_micro_block_exist(micro_key, micro_snapshot_info, hit_type));
  ASSERT_EQ(ObSSCacheHitType::SS_CACHE_MISS, hit_type);

  // micro_block exists if enable cache
  micro_cache->enable_cache();
  ASSERT_EQ(OB_SUCCESS, micro_cache->check_micro_block_exist(micro_key, micro_snapshot_info, hit_type));
  ASSERT_NE(ObSSCacheHitType::SS_CACHE_MISS, hit_type);
  ObSSMicroBlockCacheKeyMeta micro_key_meta(micro_key, micro_snapshot_info.crc_,
                                            micro_snapshot_info.size_, micro_snapshot_info.is_in_l1_);

  // ======================== `get_not_exist_micro_blocks` ========================
  ObArray<ObSSMicroBlockCacheKeyMeta> in_micro_block_key_metas;
  ObArray<ObSSMicroBlockCacheKeyMeta> out_micro_block_key_metas;
  ObSSMicroBlockCacheKey micro_key2 =
      TestSSCommonUtil::gen_phy_micro_key(TestSSCommonUtil::gen_macro_block_id(999999), 100, 100);
  ObSSMicroBlockCacheKeyMeta micro_key_meta2(micro_key2, 0/*data_crc*/, 4096/*data_size*/, false/*is_in_l1*/);
  ASSERT_EQ(OB_SUCCESS, in_micro_block_key_metas.push_back(micro_key_meta));
  ASSERT_EQ(OB_SUCCESS, in_micro_block_key_metas.push_back(micro_key_meta2));
  //fail to check whether micro_blocks is in cache if disable cache
  micro_cache->disable_cache();
  ASSERT_EQ(OB_SS_MICRO_CACHE_DISABLED, micro_cache->get_not_exist_micro_blocks(in_micro_block_key_metas,
                                                                                out_micro_block_key_metas));
  ASSERT_EQ(0, out_micro_block_key_metas.count());

  // micro_key that is not in cache is pushed into out_micro_block_keys if enable cache
  micro_cache->enable_cache();
  out_micro_block_key_metas.reset();
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_not_exist_micro_blocks(in_micro_block_key_metas, out_micro_block_key_metas));
  ASSERT_EQ(1, out_micro_block_key_metas.count());

  // ======================== `update_micro_block_heat` ========================
  ObArray<ObSSMicroBlockCacheKey> micro_keys_arr;
  ASSERT_EQ(OB_SUCCESS, micro_keys_arr.push_back(micro_key));
  const int64_t time_delta_s = 100;
  micro_handle.reset();
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(micro_key, micro_handle, true));
  const int64_t old_heat_val = micro_handle.get_ptr()->get_heat_val();

  // fail to update heat_val if disable cache
  micro_cache->disable_cache();
  ASSERT_EQ(OB_SS_MICRO_CACHE_DISABLED, micro_cache->update_micro_block_heat(micro_keys_arr, true, true, time_delta_s));
  ASSERT_EQ(old_heat_val, micro_handle.get_ptr()->get_heat_val());

  // succeed to update heat_val if enable cache
  micro_cache->enable_cache();
  ASSERT_EQ(OB_SUCCESS, micro_cache->update_micro_block_heat(micro_keys_arr, true, true, time_delta_s));
  ASSERT_LT(old_heat_val, micro_handle.get_ptr()->get_heat_val());

  // ======================== `get_available_space_for_prewarm` ========================
  int64_t available_space_size = 0;
  // fail to get availabel space if disable cache
  micro_cache->disable_cache();
  ASSERT_EQ(OB_SS_MICRO_CACHE_DISABLED, micro_cache->get_available_space_for_prewarm(available_space_size));
  ASSERT_EQ(0, available_space_size);

  // succeed to get availabel space if enable cache
  micro_cache->enable_cache();
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_available_space_for_prewarm(available_space_size));
  ASSERT_LT(0, available_space_size);
}

TEST_F(TestSSMicroCache, test_clear_micro_cache)
{
  int ret = OB_SUCCESS;
  LOG_INFO("TEST_CASE: start test_clear_micro_cache");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMemDataManager &mem_data_mgr = micro_cache->mem_data_mgr_;
  ObSSMicroCacheTaskRunner &task_runner = micro_cache->task_runner_;
  ObSSARCInfo origin_arc_info;
  origin_arc_info = micro_meta_mgr.get_arc_info();
  ObSSMicroCacheSuperBlock origin_super_blk = phy_blk_mgr.super_block_;
  TestSSMicroCacheCtx ctx;
  ctx.macro_blk_cnt_ = 10;
  ctx.micro_blk_cnt_ = 128;
  ctx.thread_num_ = 5;
  ASSERT_EQ(OB_SUCCESS, ctx.micro_key_map_.create(1024,  ObMemAttr(MTL_ID(), "test")));
  TestSSMicroCache::TestSSMicroCacheThread threads(
      ObTenantEnv::get_tenant(), ctx, TestSSMicroCacheThread::TestParallelType::TEST_PARALLEL_ADD_MICRO_BLOCK);
  threads.set_thread_count(ctx.thread_num_);
  threads.start();
  ob_usleep(5 * 1000);
  task_runner.schedule_do_micro_checkpoint_task(0);
  task_runner.schedule_do_blk_checkpoint_task(0);
  task_runner.schedule_arc_cache_task(0);
  ob_usleep(5 * 1000);
  micro_cache->clear_micro_cache();
  threads.wait();
  ASSERT_LT(0, threads.get_fail_cnt()); // failed due to stop micro cache

  // check micro meta
  ASSERT_EQ(0, micro_cache->flying_req_cnt_);
  ASSERT_LT(0, ctx.micro_key_map_.size());
  for (auto iter = ctx.micro_key_map_.begin(); iter != ctx.micro_key_map_.end(); ++iter) {
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr.get_micro_block_meta_handle(iter->first, micro_meta_handle, false));
  }

  ObSSARCInfo arc_info;
  arc_info = micro_meta_mgr.get_arc_info();
  ASSERT_EQ(arc_info.limit_, origin_arc_info.limit_);
  ASSERT_EQ(arc_info.work_limit_, origin_arc_info.work_limit_);
  ASSERT_EQ(arc_info.p_, origin_arc_info.p_);
  ASSERT_EQ(arc_info.max_p_, origin_arc_info.max_p_);
  ASSERT_EQ(arc_info.min_p_, origin_arc_info.min_p_);
  for (int64_t i = 0; i < SS_ARC_SEG_COUNT; ++i) {
    ASSERT_EQ(0, arc_info.seg_info_arr_[i].cnt_);
    ASSERT_EQ(0, arc_info.seg_info_arr_[i].size_);
  }

  ASSERT_EQ(0, micro_cache->cache_stat_.micro_stat().total_micro_cnt_);
  ASSERT_EQ(0, micro_cache->cache_stat_.micro_stat().total_micro_size_);
  ASSERT_EQ(0, micro_cache->cache_stat_.micro_stat().valid_micro_cnt_);
  ASSERT_EQ(0, micro_cache->cache_stat_.micro_stat().valid_micro_size_);
  ASSERT_EQ(0, micro_cache->cache_stat_.micro_stat().micro_pool_fixed_cnt_);
  ASSERT_EQ(0, micro_cache->cache_stat_.micro_stat().micro_pool_mem_size_);
  ASSERT_EQ(0, micro_cache->cache_stat_.micro_stat().micro_map_mem_size_);
  ASSERT_EQ(0, micro_cache->cache_stat_.micro_stat().micro_total_mem_size_);

  // check mem_data_mgr
  ASSERT_EQ(nullptr, mem_data_mgr.fg_mem_block_);
  ASSERT_EQ(nullptr, mem_data_mgr.bg_mem_block_);
  ASSERT_EQ(0, mem_data_mgr.fg_sealed_mem_blocks_.get_curr_total());
  ASSERT_EQ(0, mem_data_mgr.bg_sealed_mem_blocks_.get_curr_total());
  ASSERT_EQ(0, mem_data_mgr.uncomplete_sealed_mem_blocks_.get_curr_total());

  ASSERT_EQ(0, micro_cache->cache_stat_.mem_blk_stat().mem_blk_fg_used_cnt_);
  ASSERT_EQ(0, micro_cache->cache_stat_.mem_blk_stat().mem_blk_bg_used_cnt_);

  // check phy_block_mgr
  SSPhyBlockCntInfo &blk_cnt_info = phy_blk_mgr.blk_cnt_info_;
  const int64_t shared_blk_used_cnt = blk_cnt_info.data_blk_.hold_cnt_ + blk_cnt_info.meta_blk_.hold_cnt_;
  ASSERT_EQ(0, blk_cnt_info.data_blk_.used_cnt_);
  ASSERT_EQ(0, blk_cnt_info.meta_blk_.used_cnt_);
  ASSERT_EQ(0, blk_cnt_info.phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(blk_cnt_info.data_blk_.min_cnt_, blk_cnt_info.data_blk_.hold_cnt_);
  ASSERT_EQ(blk_cnt_info.meta_blk_.min_cnt_, blk_cnt_info.meta_blk_.hold_cnt_);
  ASSERT_EQ(shared_blk_used_cnt, blk_cnt_info.shared_blk_used_cnt_);
  ASSERT_EQ(0, cache_stat.phy_blk_stat().data_blk_used_cnt_);
  ASSERT_EQ(0, cache_stat.phy_blk_stat().meta_blk_used_cnt_);
  ASSERT_EQ(blk_cnt_info.data_blk_.hold_cnt_, cache_stat.phy_blk_stat().data_blk_cnt_);
  ASSERT_EQ(blk_cnt_info.meta_blk_.hold_cnt_, cache_stat.phy_blk_stat().meta_blk_cnt_);
  ASSERT_EQ(shared_blk_used_cnt, cache_stat.phy_blk_stat().shared_blk_used_cnt_);

  ASSERT_EQ(true, phy_blk_mgr.free_bitmap_->is_all_true());
  ASSERT_EQ(origin_super_blk.cache_file_size_, phy_blk_mgr.super_block_.cache_file_size_);
  ASSERT_EQ(0, phy_blk_mgr.super_block_.micro_ckpt_entry_list_.count());
  ASSERT_EQ(0, phy_blk_mgr.super_block_.blk_ckpt_entry_list_.count());
  ASSERT_EQ(0, phy_blk_mgr.super_block_.ls_info_list_.count());

  ASSERT_EQ(0, micro_cache->cache_stat_.phy_blk_stat().reusable_blk_cnt_);
  ASSERT_EQ(0, micro_cache->cache_stat_.phy_blk_stat().phy_ckpt_blk_used_cnt_);
  ASSERT_EQ(0, micro_cache->cache_stat_.phy_blk_stat().meta_blk_used_cnt_);
  ASSERT_EQ(phy_blk_mgr.blk_cnt_info_.data_blk_.free_blk_cnt(), micro_cache->cache_stat_.phy_blk_stat().data_blk_cnt_ -
                                                                micro_cache->cache_stat_.phy_blk_stat().data_blk_used_cnt_);

  // check CacheHitStat
  ASSERT_EQ(0, micro_cache->cache_stat_.hit_stat().cache_hit_cnt_);
  ASSERT_EQ(0, micro_cache->cache_stat_.hit_stat().cache_hit_bytes_);
  ASSERT_EQ(0, micro_cache->cache_stat_.hit_stat().cache_miss_cnt_);
  ASSERT_EQ(0, micro_cache->cache_stat_.hit_stat().cache_miss_bytes_);
  ASSERT_EQ(0, micro_cache->cache_stat_.hit_stat().fail_get_cnt_);
  ASSERT_EQ(0, micro_cache->cache_stat_.hit_stat().fail_get_bytes_);
  ASSERT_EQ(0, micro_cache->cache_stat_.hit_stat().fail_add_cnt_);
  ASSERT_EQ(0, micro_cache->cache_stat_.hit_stat().fail_add_bytes_);
  ASSERT_EQ(0, micro_cache->cache_stat_.hit_stat().add_cnt_);
  ASSERT_EQ(0, micro_cache->cache_stat_.hit_stat().add_bytes_);
  ASSERT_EQ(0, micro_cache->cache_stat_.hit_stat().new_add_cnt_);

  ctx.micro_key_map_.reuse();
  TestSSMicroCache::TestSSMicroCacheThread threads2(
      ObTenantEnv::get_tenant(), ctx, TestSSMicroCacheThread::TestParallelType::TEST_PARALLEL_ADD_MICRO_BLOCK);
  threads2.set_thread_count(ctx.thread_num_);
  threads2.start();
  threads2.wait();
  ASSERT_EQ(0, threads2.get_fail_cnt());
  ASSERT_LT(0, ctx.micro_key_map_.size());
  // check micro_meta
  for (auto iter = ctx.micro_key_map_.begin(); iter != ctx.micro_key_map_.end(); ++iter) {
    ObSSMicroBlockMetaHandle micro_meta_handle;
    ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta_handle(iter->first, micro_meta_handle, false));
  }
  ASSERT_EQ(ctx.micro_key_map_.size(), SSMicroCacheStat.micro_stat().get_micro_pool_alloc_cnt());
  // check phy_block_mgr
  ASSERT_LT(0, phy_blk_mgr.get_data_block_used_cnt());
  ASSERT_EQ(false, phy_blk_mgr.free_bitmap_->is_all_true());
  ASSERT_LT(0, micro_cache->cache_stat_.mem_blk_stat().get_total_mem_blk_used_cnt());
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_micro_cache.log*");
  OB_LOGGER.set_file_name("test_ss_micro_cache.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
