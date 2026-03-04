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
#include "lib/thread/threads.h"
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "share/cache/ob_kvcache_struct.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "unittest/storage/init_basic_struct.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/shared_storage/prewarm/ob_ss_local_cache_prewarm_service.h"

static const int64_t TEST_LS_ID = 1001;

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

class TestSSSyncHotMicroKeyTask : public ::testing::Test
{
public:
  TestSSSyncHotMicroKeyTask() : ls_(nullptr), is_stop_(false) {}
  virtual ~TestSSSyncHotMicroKeyTask() {}
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp() override;
  virtual void TearDown() override;
  int add_micro_block_to_kvcache(const ObMicroBlockCacheKey &key, const char *data, const int64_t size);
  int add_micro_block_to_sscache(const ObSSMicroBlockCacheKey &key, const char *data, const int64_t size, const uint64_t tablet_id);

private:
  DISALLOW_COPY_AND_ASSIGN(TestSSSyncHotMicroKeyTask);

private:
  ObLS *ls_;
  volatile bool is_stop_;
};

void TestSSSyncHotMicroKeyTask::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSSyncHotMicroKeyTask::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  int fail_cnt = ::testing::UnitTest::GetInstance()->failed_test_case_count();

  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }

  _Exit(fail_cnt);
}

void TestSSSyncHotMicroKeyTask::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 30), 1/*micro_split_cnt*/)); // 1G
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());

  if (!OB_LS_PREWARM_MGR.is_inited_) {
    ASSERT_EQ(OB_SUCCESS, OB_LS_PREWARM_MGR.init());
  }
  const ObLSID ls_id(TEST_LS_ID);
  ObLSService *ls_service = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_service);
  ObCreateLSArg arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(MTL_ID(), ls_id, arg));
  int ret = ls_service->create_ls(arg);
  if (OB_LS_EXIST != ret) {
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ObLSHandle ls_handle;
  ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ls_ = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls_);
  is_stop_ = false;
}

void TestSSSyncHotMicroKeyTask::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

int TestSSSyncHotMicroKeyTask::add_micro_block_to_kvcache(const ObMicroBlockCacheKey &key, const char *data, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObDataMicroBlockCache *data_micro_block_cache = nullptr;
  ObKVGlobalCache *kv_global_cache = nullptr;
  ObIMicroBlockCache::BaseBlockCache *kvcache = nullptr;
  char *buf = nullptr;
  ObKVCacheInstHandle inst_handle;
  ObKVCacheHandle cache_handle;
  ObKVCachePair *kvpair = nullptr;

  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, data is null", KR(ret));
  } else if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, size is not positive", KR(ret), K(size));
  } else {
    data_micro_block_cache = &ObStorageCacheSuite::get_instance().get_block_cache();
    kv_global_cache = &ObKVGlobalCache::get_instance();
    if (OB_ISNULL(kv_global_cache) || !kv_global_cache->inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObKVGlobalCache is not inited", KR(ret));
    } else if (OB_ISNULL(data_micro_block_cache) || !data_micro_block_cache->inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("ObDataMicroBlockCache is not inited", KR(ret));
    } else if (OB_FAIL(data_micro_block_cache->get_cache(kvcache))) {
      LOG_WARN("fail to get kvcache", KR(ret));
    } else {
      ObMicroBlockHeader header;
      header.row_count_ = 1;
      header.column_count_ = 1;
      header.rowkey_column_count_ = 1;
      header.row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
      header.data_checksum_ = 0;
      header.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
      header.magic_ = MICRO_BLOCK_HEADER_MAGIC;
      header.has_column_checksum_ = 0;
      header.version_ = ObMicroBlockHeader::MICRO_BLOCK_HEADER_VERSION_LATEST;
      header.header_size_ = header.get_serialize_size();

      const int64_t header_size = header.get_serialize_size();
      const int64_t total_size = header_size + size;
      const int64_t value_size = sizeof(ObMicroBlockCacheValue) + total_size;

      if (OB_FAIL(kvcache->alloc(MTL_ID(), sizeof(ObMicroBlockCacheKey), value_size, kvpair, cache_handle, inst_handle))) {
        LOG_WARN("fail to alloc kvpair", KR(ret));
      } else {
        kvpair->key_ = new (kvpair->key_) ObMicroBlockCacheKey(key);
        buf = reinterpret_cast<char *>(kvpair->value_) + sizeof(ObMicroBlockCacheValue);
        int64_t pos = 0;
        if (OB_FAIL(header.serialize(buf, total_size, pos))) {
          LOG_WARN("fail to serialize header", KR(ret));
        } else {
          MEMCPY(buf + pos, data, size);
          ObMicroBlockCacheValue *cache_value = new (kvpair->value_) ObMicroBlockCacheValue();
          if (OB_FAIL(cache_value->init(buf, total_size, nullptr/*extra_buf*/, 0/*extra_size*/, ObMicroBlockData::Type::DATA_BLOCK))) {
            LOG_WARN("fail to init cache value", KR(ret));
          } else if (OB_FAIL(kvcache->put_kvpair(inst_handle, kvpair, cache_handle, false/*overwrite*/))) {
            if (OB_ENTRY_EXIST == ret) {
              ret = OB_SUCCESS;
              LOG_INFO("micro block cache already exists, ignore", K(key));
            } else {
              LOG_WARN("fail to put kvpair", KR(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int TestSSSyncHotMicroKeyTask::add_micro_block_to_sscache(const ObSSMicroBlockCacheKey &key, const char *data, const int64_t size, const uint64_t tablet_id)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;

  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, data is null", KR(ret));
  } else if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, size is not positive", KR(ret), K(size));
  } else if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_cache is null", KR(ret));
  } else if (OB_FAIL(micro_cache->add_micro_block_cache_for_prewarm(key, data, size, tablet_id,
             ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE, 5, false/*transfer_seg*/))) {
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_INFO("micro block cache already exists, ignore", K(key));
    } else {
      LOG_WARN("fail to add micro block cache for prewarm", KR(ret), K(key));
    }
  }
  return ret;
}

TEST_F(TestSSSyncHotMicroKeyTask, test_update_leader_micro_block_heat)
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSLocalCachePrewarmService *prewarm_service = MTL(ObSSLocalCachePrewarmService *);
  ASSERT_NE(nullptr, prewarm_service);
  // Only leader replica does kvcache heat update; switch to leader so is_sync_hot_micro_key_task_stop_ = false.
  ASSERT_EQ(OB_SUCCESS, ls_->get_ls_prewarm_handler().switch_to_leader());

  ObSSMicroMetaManager &micro_meta_mgr = micro_cache->micro_meta_mgr_;

  const int64_t micro_size = 128;
  const int64_t offset1 = 1;
  const int64_t offset2 = 200;
  const int64_t offset3 = 300;
  const MacroBlockId macro_id1 = TestSSCommonUtil::gen_macro_block_id(888888);
  const MacroBlockId macro_id2 = TestSSCommonUtil::gen_macro_block_id(999999);
  const MacroBlockId macro_id3 = TestSSCommonUtil::gen_macro_block_id(777777);
  const uint64_t tenant_id = ls_->get_tenant_id();
  const uint64_t tablet_id1 = macro_id1.second_id();
  const uint64_t tablet_id2 = macro_id2.second_id();
  const uint64_t tablet_id3 = macro_id3.second_id();

  ObMicroBlockCacheKey kvcache_key1;
  kvcache_key1.set(tenant_id, macro_id1, offset1, micro_size);
  ObMicroBlockCacheKey kvcache_key2;
  kvcache_key2.set(tenant_id, macro_id2, offset2, micro_size);
  ObMicroBlockCacheKey kvcache_key3;
  kvcache_key3.set(tenant_id, macro_id3, offset3, micro_size);

  ObSSMicroBlockCacheKey sscache_key1 = TestSSCommonUtil::gen_phy_micro_key(macro_id1, offset1, micro_size);
  ObSSMicroBlockCacheKey sscache_key2 = TestSSCommonUtil::gen_phy_micro_key(macro_id2, offset2, micro_size);
  ObSSMicroBlockCacheKey sscache_key3 = TestSSCommonUtil::gen_phy_micro_key(macro_id3, offset3, micro_size);


  char data_buf1[micro_size];
  char data_buf2[micro_size];
  char data_buf3[micro_size];
  MEMSET(data_buf1, 'a', micro_size);
  MEMSET(data_buf2, 'b', micro_size);
  MEMSET(data_buf3, 'c', micro_size);

  ASSERT_EQ(OB_SUCCESS, add_micro_block_to_kvcache(kvcache_key1, data_buf1, micro_size));
  ASSERT_EQ(OB_SUCCESS, add_micro_block_to_kvcache(kvcache_key2, data_buf2, micro_size));
  ASSERT_EQ(OB_SUCCESS, add_micro_block_to_kvcache(kvcache_key3, data_buf3, micro_size));

  ObLSTabletService *ls_tablet_svr = ls_->get_tablet_svr();
  ASSERT_NE(nullptr, ls_tablet_svr);
  ASSERT_EQ(OB_SUCCESS, ls_tablet_svr->tablet_id_set_.set(ObTabletID(tablet_id1)));
  ASSERT_EQ(OB_SUCCESS, ls_tablet_svr->tablet_id_set_.set(ObTabletID(tablet_id2)));
  ASSERT_EQ(OB_SUCCESS, ls_tablet_svr->tablet_id_set_.set(ObTabletID(tablet_id3)));

  ASSERT_EQ(OB_SUCCESS, add_micro_block_to_sscache(sscache_key1, data_buf1, micro_size, tablet_id1));
  ASSERT_EQ(OB_SUCCESS, add_micro_block_to_sscache(sscache_key2, data_buf2, micro_size, tablet_id2));
  ob_usleep(500 * 1000);
  OB_LS_PREWARM_MGR.batch_get_kvcache_key_task_.runTimerTask();
  ob_usleep(200 * 1000);

  ObSSMicroBlockMetaHandle micro_meta_handle1;
  ObSSMicroBlockMetaHandle micro_meta_handle2;
  ObSSMicroBlockMetaHandle micro_meta_handle3;
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta(
            sscache_key1, micro_meta_handle1, ObTabletID::INVALID_TABLET_ID, false/*need_check_in_ghost*/));
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta(
            sscache_key2, micro_meta_handle2, ObTabletID::INVALID_TABLET_ID, false/*need_check_in_ghost*/));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr.get_micro_block_meta(
            sscache_key3, micro_meta_handle3, ObTabletID::INVALID_TABLET_ID, false/*need_check_in_ghost*/));
  const int64_t old_heat1_time_s = micro_meta_handle1()->access_time();
  const int64_t old_heat2_time_s = micro_meta_handle2()->access_time();
  ob_usleep(2 * 1000 * 1000);

  ObSSMicroCacheHitStat &hit_stat = micro_cache->get_micro_cache_stat().hit_stat();
  const int64_t old_lack_total = ATOMIC_LOAD(&hit_stat.hot_micro_lack_cnt_) + ATOMIC_LOAD(&hit_stat.temp_hot_micro_lack_cnt_);

  ObSyncHotMicroKeyTask *sync_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, prewarm_service->sync_hot_micro_key_task_map_.get_refactored(TEST_LS_ID, sync_task));
  ASSERT_NE(nullptr, sync_task);
  ASSERT_EQ(OB_SUCCESS, sync_task->update_leader_micro_block_heat());

  micro_meta_handle1.reset();
  micro_meta_handle2.reset();
  micro_meta_handle3.reset();
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta(
            sscache_key1, micro_meta_handle1, ObTabletID::INVALID_TABLET_ID, false/*need_check_in_ghost*/));
  ASSERT_EQ(OB_SUCCESS, micro_meta_mgr.get_micro_block_meta(
            sscache_key2, micro_meta_handle2, ObTabletID::INVALID_TABLET_ID, false/*need_check_in_ghost*/));
  const int64_t new_heat1_time_s = micro_meta_handle1()->access_time();
  const int64_t new_heat2_time_s = micro_meta_handle2()->access_time();
  ASSERT_LT(old_heat1_time_s, new_heat1_time_s);
  ASSERT_LT(old_heat2_time_s, new_heat2_time_s);

  ASSERT_EQ(OB_ENTRY_NOT_EXIST, micro_meta_mgr.get_micro_block_meta(
            sscache_key3, micro_meta_handle3, ObTabletID::INVALID_TABLET_ID, false/*need_check_in_ghost*/));
  const int64_t new_lack_total = ATOMIC_LOAD(&hit_stat.hot_micro_lack_cnt_) + ATOMIC_LOAD(&hit_stat.temp_hot_micro_lack_cnt_);
  ASSERT_EQ(old_lack_total + 1, new_lack_total);

  LOG_INFO("test_update_leader_micro_block_heat done",
           K(old_heat1_time_s), K(new_heat1_time_s), K(old_heat2_time_s), K(new_heat2_time_s), K(old_lack_total), K(new_lack_total),
           K(sscache_key1), K(sscache_key2), K(sscache_key3));
}

} // storage
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_kvcache_prewarm.log*");
  OB_LOGGER.set_file_name("test_ss_kvcache_prewarm.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
