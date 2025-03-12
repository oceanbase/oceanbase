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
#include "mittest/shared_storage/clean_residual_data.h"
#include "test_ss_common_util.h"
#include "unittest/storage/init_basic_struct.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

class TestSSHAPrewarmStruct : public ::testing::Test
{
public:
  TestSSHAPrewarmStruct() : ls_(nullptr) {}
  virtual ~TestSSHAPrewarmStruct() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void create_ls(const ObLSID &ls_id);
  void remove_ls(const ObLSID &ls_id, const int64_t write_blk_cnt);
  void generate_phy_block(const int64_t write_blk_cnt,
                          const bool simulate_tablet_in_tablet_service,
                          ObIArray<ObSSMicroBlockCacheKey> &micro_keys);
  void generate_micro_keys_not_in_cache(const int64_t micro_num,
                                        ObIArray<ObSSMicroBlockCacheKey> &micro_keys);

public:
  ObLS *ls_;
};

void TestSSHAPrewarmStruct::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSHAPrewarmStruct::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSHAPrewarmStruct::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 30)));
  micro_cache->start();
  micro_cache->task_runner_.release_cache_task_.reorganize_op_.enable_reorganize_ = false;
}

void TestSSHAPrewarmStruct::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache*);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSHAPrewarmStruct::create_ls(const ObLSID &ls_id)
{
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_id, arg));
  ObLSService *ls_service = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_service);
  ASSERT_EQ(OB_SUCCESS, ls_service->create_ls(arg));
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::SS_PREWARM_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ls_ = ls;
}

void TestSSHAPrewarmStruct::remove_ls(const ObLSID &ls_id, const int64_t write_blk_cnt)
{
  ObLSTabletService *ls_tablet_service = ls_->get_tablet_svr();
  ASSERT_NE(nullptr, ls_tablet_service);
  for (int64_t i = 0; i < write_blk_cnt; ++i) {
    // remove simulated tablet in tablet_id_set_ of ObLSTabletService
    ASSERT_EQ(OB_SUCCESS, ls_tablet_service->tablet_id_set_.erase(ObTabletID(i + 500000)));
  }
  ObLSService *ls_service = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_service);
  ASSERT_EQ(OB_SUCCESS, ls_service->remove_ls(ls_id));
}

void TestSSHAPrewarmStruct::generate_phy_block(
    const int64_t write_blk_cnt,
    const bool simulate_tablet_in_tablet_service,
    ObIArray<ObSSMicroBlockCacheKey> &micro_keys)
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;
  ObSSMemDataManager &mem_data_mgr = micro_cache->mem_data_mgr_;
  ObSSPersistMicroDataTask &persist_task = micro_cache->task_runner_.persist_task_;
  const int64_t available_block_cnt = phy_blk_mgr.blk_cnt_info_.cache_limit_blk_cnt();
  ASSERT_LT(write_blk_cnt, available_block_cnt);
  const int32_t micro_cnt = 20;
  const int64_t block_size = micro_cache->phy_block_size_;
  const int64_t payload_offset = ObSSPhyBlockCommonHeader::get_serialize_size() +
                                 ObSSNormalPhyBlockHeader::get_fixed_serialize_size();
  const int32_t micro_size = (block_size - payload_offset) / micro_cnt
                             - (sizeof(ObSSMicroBlockIndex) + SS_SERIALIZE_EXTRA_BUF_LEN);
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char *>(allocator.alloc(micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', micro_size);

  ObLSTabletService *ls_tablet_service = nullptr;
  if (simulate_tablet_in_tablet_service) {
    ls_tablet_service = ls_->get_tablet_svr();
    ASSERT_NE(nullptr, ls_tablet_service);
  }
  for (int64_t i = 0; i < write_blk_cnt; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 500000);
    if (simulate_tablet_in_tablet_service) {
      // simulate tablet in tablet_id_set_ of ObLSTabletService
      ASSERT_EQ(OB_SUCCESS, ls_tablet_service->tablet_id_set_.set(ObTabletID(i + 500000)));
    }
    for (int32_t j = 0; j < micro_cnt; ++j) {
      const int32_t offset = payload_offset + j * micro_size;
      ObSSMicroBlockCacheKey micro_key;
      micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
      micro_key.micro_id_.macro_id_ = macro_id;
      micro_key.micro_id_.offset_ = offset;
      micro_key.micro_id_.size_ = micro_size;
      micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size,
                                         ObSSMicroCacheAccessType::COMMON_IO_TYPE);
      ASSERT_EQ(OB_SUCCESS, micro_keys.push_back(micro_key));
    }
    ASSERT_NE(nullptr, mem_data_mgr.fg_mem_block_);
    ASSERT_EQ(micro_cnt, mem_data_mgr.fg_mem_block_->micro_count_);
    ASSERT_EQ(micro_size * micro_cnt, mem_data_mgr.fg_mem_block_->data_size_);
    ASSERT_EQ(micro_size * micro_cnt, mem_data_mgr.fg_mem_block_->valid_val_);
    ASSERT_EQ(OB_SUCCESS, TestSSCommonUtil::wait_for_persist_task());
  }

  usleep(5 * 1000 * 1000L); // sleep 5s
}

void TestSSHAPrewarmStruct::generate_micro_keys_not_in_cache(
    const int64_t micro_num,
    ObIArray<ObSSMicroBlockCacheKey> &micro_keys)
{
  for (int64_t i = 0; i < micro_num; ++i) {
    MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 600000);
    ObSSMicroBlockCacheKey micro_key;
    micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
    micro_key.micro_id_.macro_id_ = macro_id;
    micro_key.micro_id_.offset_ = 4096;
    micro_key.micro_id_.size_ = 4096;
    ASSERT_EQ(OB_SUCCESS, micro_keys.push_back(micro_key));
  }
}

TEST_F(TestSSHAPrewarmStruct, test_producer)
{
  int ret = OB_SUCCESS;
  ObArray<ObSSMicroBlockCacheKey> micro_keys;

  const ObLSID ls_id(100);
  create_ls(ls_id);

  // generate phy_block of @ls_id into micro_cache_file
  const int64_t WRITE_BLK_CNT = 10;
  generate_phy_block(WRITE_BLK_CNT, true/*simulate_tablet_in_tablet_service*/, micro_keys);

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSPhysicalBlockManager &phy_blk_mgr = micro_cache->phy_blk_mgr_;

  const int64_t split_count = 2;
  ObArray<ObSSPhyBlockIdxRange> block_ranges;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr.divide_cache_data_block_range(ls_id, split_count, block_ranges));
  ASSERT_EQ(split_count, block_ranges.count());

  ObArray<ObMigrationCacheJobInfo> job_infos;
  for (int64_t i = 0; i < block_ranges.count(); ++i) {
    ObSSPhyBlockIdxRange &cur_block_range = block_ranges.at(i);
    ObMigrationCacheJobInfo job_info(cur_block_range.start_blk_idx_, cur_block_range.end_blk_idx_);
    ASSERT_EQ(OB_SUCCESS, job_infos.push_back(job_info));
  }

  for (int64_t i = 0; i < split_count; ++i) {
    ObArray<ObCopyMicroBlockKeySet> key_sets;
    ObCopyMicroBlockKeySetProducer key_set_producer;
    ASSERT_EQ(OB_SUCCESS, key_set_producer.init(job_infos.at(i), ls_id));
    while (OB_SUCC(ret)) {
      ObCopyMicroBlockKeySet key_set;
      ret = key_set_producer.get_next_micro_block_key_set(key_set);
      if (OB_ITER_END == ret) {
        ASSERT_LT(0, key_sets.count());
        OB_LOG(INFO, "finish to get next micro block key set");
        ret = OB_SUCCESS;
        break;
      } else {
        ASSERT_EQ(OB_SUCCESS, ret);
        if (!key_set.micro_block_key_metas_.empty()) {
          ASSERT_EQ(OB_SUCCESS, key_sets.push_back(key_set));
        }
      }
    }

    ObArray<ObArray<ObSSMicroBlockCacheKeyMeta>> key_meta_arrs;
    ObCopyMicroBlockDataProducer data_producer;
    ASSERT_EQ(OB_SUCCESS, data_producer.init(key_sets));
    while (OB_SUCC(ret)) {
      ObArray<ObSSMicroBlockCacheKeyMeta> key_meta_arr;
      ObBufferReader data;
      ret = data_producer.get_next_micro_block_data(key_meta_arr, data);
      if (OB_ITER_END == ret) {
        ASSERT_LT(0, key_meta_arrs.count());
        OB_LOG(INFO, "finish to get next micro block data");
        ret = OB_SUCCESS;
        break;
      } else {
        ASSERT_EQ(OB_SUCCESS, ret);
        if (!key_meta_arr.empty()) {
          ASSERT_EQ(OB_SUCCESS, key_meta_arrs.push_back(key_meta_arr));
        }
      }
    }
  }

  remove_ls(ls_id, WRITE_BLK_CNT);
}

TEST_F(TestSSHAPrewarmStruct, test_get_not_exist_micro_blocks)
{
  // add micro into micro_cache
  ObArray<ObSSMicroBlockCacheKey> micro_keys_in_cache;
  const int64_t WRITE_BLK_CNT = 10;
  generate_phy_block(WRITE_BLK_CNT, false/*simulate_tablet_in_tablet_service*/, micro_keys_in_cache);

  // generate micro keys not in micro_cache
  const int64_t NON_EXIST_MICRO_NUM = 100;
  ObArray<ObSSMicroBlockCacheKey> micro_keys_not_in_cache;
  generate_micro_keys_not_in_cache(NON_EXIST_MICRO_NUM, micro_keys_not_in_cache);

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObArray<ObSSMicroBlockCacheKeyMeta> not_exist_micro_blocks;
  ObArray<ObSSMicroBlockCacheKeyMeta> micro_key_metas_in_cache;
  const int64_t micro_key_in_cache_cnt = micro_keys_in_cache.count();
  for (int64_t i = 0; i < micro_key_in_cache_cnt; ++i) {
    ObSSMicroSnapshotInfo micro_snapshot_info;
    ObSSCacheHitType hit_type;
    ASSERT_EQ(OB_SUCCESS, micro_cache->check_micro_block_exist(micro_keys_in_cache.at(i),
                                                               micro_snapshot_info, hit_type));
    ObSSMicroBlockCacheKeyMeta micro_key_meta(micro_keys_in_cache.at(i), micro_snapshot_info.crc_,
                                              micro_snapshot_info.size_, micro_snapshot_info.is_in_l1_);
    ASSERT_EQ(OB_SUCCESS, micro_key_metas_in_cache.push_back(micro_key_meta));
  }
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_not_exist_micro_blocks(micro_key_metas_in_cache, not_exist_micro_blocks));
  ASSERT_EQ(0, not_exist_micro_blocks.count());

  not_exist_micro_blocks.reuse();
  ObArray<ObSSMicroBlockCacheKeyMeta> micro_key_metas_not_in_cache;
  const int64_t micro_key_not_in_cache_cnt = micro_keys_not_in_cache.count();
  for (int64_t i = 0; i < micro_key_not_in_cache_cnt; ++i) {
    ObSSMicroBlockCacheKeyMeta micro_key_meta(micro_keys_not_in_cache.at(i), 0/*data_crc*/,
                                              4096/*data_size*/, false/*is_in_l1*/);
    ASSERT_EQ(OB_SUCCESS, micro_key_metas_not_in_cache.push_back(micro_key_meta));
  }
  ASSERT_EQ(OB_SUCCESS, micro_cache->get_not_exist_micro_blocks(micro_key_metas_not_in_cache, not_exist_micro_blocks));
  ASSERT_EQ(micro_keys_not_in_cache.count(), not_exist_micro_blocks.count());
}


} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_ha_prewarm_struct.log*");
  OB_LOGGER.set_file_name("test_ss_ha_prewarm_struct.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
