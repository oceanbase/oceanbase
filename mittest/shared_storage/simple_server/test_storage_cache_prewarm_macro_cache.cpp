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
#include "mittest/shared_storage/simple_server/test_storage_cache_common_util.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"

namespace oceanbase
{
namespace sslog
{

oceanbase::unittest::ObMockPalfKV PALF_KV;

int get_sslog_table_guard(const ObSSLogTableType type,
                          const int64_t tenant_id,
                          ObSSLogProxyGuard &guard)
{
  int ret = OB_SUCCESS;

  switch (type)
  {
    case ObSSLogTableType::SSLOG_TABLE: {
      void *proxy = share::mtl_malloc(sizeof(ObSSLogTableProxy), "ObSSLogTable");
      if (nullptr == proxy) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObSSLogTableProxy *sslog_table_proxy = new (proxy) ObSSLogTableProxy(tenant_id);
        if (OB_FAIL(sslog_table_proxy->init())) {
          SSLOG_LOG(WARN, "fail to inint", K(ret));
        } else {
          guard.set_sslog_proxy((ObISSLogProxy *)proxy);
        }
      }
      break;
    }
    case ObSSLogTableType::SSLOG_PALF_KV: {
      void *proxy = share::mtl_malloc(sizeof(ObSSLogKVProxy), "ObSSLogTable");
      if (nullptr == proxy) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObSSLogKVProxy *sslog_kv_proxy = new (proxy) ObSSLogKVProxy(&PALF_KV);
        // if (OB_FAIL(sslog_kv_proxy->init(GCONF.cluster_id, tenant_id))) {
        //   SSLOG_LOG(WARN, "init palf kv failed", K(ret));
        // } else {
          guard.set_sslog_proxy((ObISSLogProxy *)proxy);
        // }
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      SSLOG_LOG(WARN, "invalid sslog type", K(type));
      break;
    }
  }

  return ret;
}
} // namespace sslog
} // namespace oceanbase


using namespace oceanbase::transaction;
using namespace oceanbase::storage;

const char *get_per_file_test_name()
{
    return "test_storage_cache_prewarm_macro_cache_";
}

namespace oceanbase
{
namespace unittest
{
TEST_F(ObStorageCachePolicyPrewarmerTest, test_shared_major_for_macro_cache)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  FLOG_INFO("[TEST] start test_shared_major_for_macro_cache ");
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(tnt_disk_space_mgr, nullptr);
  // 2.test prewarm shared major macro blocks into local cache
  OK(exe_sql("create table test_macro_cache (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_macro_cache");

  OK(exe_sql("insert into test_macro_cache values (1)"));
  sleep(1);
  OK(medium_compact(run_ctx_.tablet_id_.id()));
  OK(exe_sql("alter table test_macro_cache storage_cache_policy (global = 'hot');"));
  FLOG_INFO("[TEST] finish alter storage cache policy");
  check_macro_cache_exist();
  FLOG_INFO("[TEST] finish test_shared_major_for_macro_cache");
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_shared_increase_for_macro_cache)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  FLOG_INFO("[TEST] start test_shared_increase_for_macro_cache");

  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(tnt_disk_space_mgr, nullptr);

  // 3. test prewarm shared minor macro blocks into local cache
  // trigger minor compaction by minor freeze 3 times.
  // Check whether the hot_retention prewarm SHARED_MINI_DATA_MACRO/SHARED_MINOR_DATA_MACRO/SHARED_MDS_MINI_DATA_MACRO/SHARED_MDS_MINOR_DATA_MACRO
  // into macro cache
  OK(exe_sql("create table test_shared_increase (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_shared_increase");
  OK(exe_sql("insert into test_shared_increase values (2);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_shared_increase values (3);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_shared_increase values (4);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  wait_ss_minor_compaction_finish();
  OK(exe_sql("alter table test_shared_increase storage_cache_policy (global = 'auto');"));
  OK(exe_sql("alter table test_shared_increase storage_cache_policy (global = 'hot');"));
  check_macro_cache_exist();
  LOG_INFO("[TEST] finish to test shared minor");

  // 4. test private macro blocks in macro cache
  OK(exe_sql("insert into test_shared_increase values (5)"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();
  check_macro_cache_exist();
  FLOG_INFO("[TEST] finish test_shared_increase_for_macro_cache");
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_macro_cache_space_occupy)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();
  FLOG_INFO("[TEST] start test_macro_cache_space_occupy");

  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(tnt_disk_space_mgr, nullptr);
  OK(exe_sql("create table test_space_occupy (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_space_occupy");

  // 6. test whether occupy space happens when hot tablet macro block space is not enough
  const int64_t macro_free_size = tnt_disk_space_mgr->get_macro_cache_free_size();
  OK(tnt_disk_space_mgr->alloc_file_size((macro_free_size), ObSSMacroCacheType::MACRO_BLOCK, false/*is_for_dir*/));


  ObSSMacroCacheStat macro_cache_stat1;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, macro_cache_stat1));
  ObSSMacroCacheStat hot_macro_cache_stat1;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_macro_cache_stat1));

  OK(exe_sql("insert into test_space_occupy values (7)"));
  OK(exe_sql("insert into test_space_occupy values (8)"));
  OK(exe_sql("insert into test_space_occupy values (9)"));

  OK(medium_compact(run_ctx_.tablet_id_.id()));
  OK(exe_sql("alter table test_space_occupy storage_cache_policy (global = 'auto');"));
  OK(exe_sql("alter table test_space_occupy storage_cache_policy (global = 'hot');"));
  check_macro_cache_exist();

  ObSSMacroCacheStat macro_cache_stat2;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::MACRO_BLOCK, macro_cache_stat2));
  ObSSMacroCacheStat hot_macro_cache_stat2;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, hot_macro_cache_stat2));

  LOG_INFO("[TEST] compare hot_tablet_space with macro_cache_space", K(hot_macro_cache_stat1.used_), K(macro_cache_stat1.used_), K(hot_macro_cache_stat2.used_), K(macro_cache_stat2.used_));
  ASSERT_TRUE(hot_macro_cache_stat1.used_ < hot_macro_cache_stat2.used_);
  ASSERT_TRUE(macro_cache_stat1.used_ >  macro_cache_stat2.used_);

  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->free_file_size(macro_free_size, ObSSMacroCacheType::MACRO_BLOCK,
                                                           false/*is_for_dir*/));
  FLOG_INFO("[TEST] test_macro_cache_space_occupy end", K(tnt_disk_space_mgr->get_macro_cache_free_size()));
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_macro_cache_full)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  ASSERT_NE(nullptr, gc_service);
  gc_service->stop();
  sleep(10);

  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(tnt_disk_space_mgr, nullptr);
  macro_cache_mgr->evict_task_.is_inited_ = false;
  OK(exe_sql("create table test_macro_cache_full (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_macro_cache_full");

  // 7. Test if the space of hot tablet macro cache is full
  int64_t max_hot_tablet_size = tnt_disk_space_mgr->get_macro_cache_free_size();
  OK(tnt_disk_space_mgr->alloc_file_size(max_hot_tablet_size, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, false/*is_for_dir*/));
  int64_t after_alloc_hot_tablet_size = tnt_disk_space_mgr->get_macro_cache_free_size();
  ASSERT_EQ(0, after_alloc_hot_tablet_size);
  OK(exe_sql("insert into test_macro_cache_full values (7)"));
  OK(exe_sql("insert into test_macro_cache_full values (8)"));
  OK(exe_sql("insert into test_macro_cache_full values (9)"));

  OK(medium_compact(run_ctx_.tablet_id_.id()));
  OK(exe_sql("alter table test_macro_cache_full storage_cache_policy (global = 'auto');"));
  OK(exe_sql("alter table test_macro_cache_full storage_cache_policy (global = 'hot');"));
  ObStorageCachePolicyPrewarmer prewarmer2;
  ObStorageCacheTabletTask *task2 = static_cast<ObStorageCacheTabletTask *>(ob_malloc(
      sizeof(ObStorageCacheTabletTask),
      ObMemAttr(run_ctx_.tenant_id_, "TestPrewarm2")));
  ASSERT_NE(nullptr, task2);
  new (task2) ObStorageCacheTabletTask();
  OK(task2->init(
      run_ctx_.tenant_id_,
      run_ctx_.ls_id_.id(),
      run_ctx_.tablet_id_.id(),
      PolicyStatus::HOT));
  task2->inc_ref_count();
  OK(task2->set_status(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING));

  micro_cache->clear_micro_cache();
  OK(prewarmer2.prewarm_hot_tablet(task2));

  ObSEArray<MacroBlockId, 64> data_block_ids;
  ObSEArray<MacroBlockId, 16> meta_block_ids;
  FLOG_INFO("[TEST] start to check macro cache", K(run_ctx_), K(max_hot_tablet_size));
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));

  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  FLOG_INFO("[TEST] get macro blocks", K(prewarmer2.tablet_id_), K(prewarmer2.ls_id_), K(data_block_ids), K(meta_block_ids));

  ObSSPrewarmStat second_stat;
  second_stat = prewarmer2.get_hot_retention_prewarm_stat();
  ASSERT_GE(second_stat.get_macro_block_bytes(), second_stat.get_macro_cache_max_available_size());
  ASSERT_LT(second_stat.get_micro_block_bytes(), second_stat.get_micro_cache_max_available_size());
  ObSSMicroCacheStat &after_cache_stat = micro_cache->cache_stat_;
  OK(task2->generate_comment());

  FLOG_INFO("[TEST] test macro cache full, micro cache is not full", K(second_stat), K(second_stat.get_macro_cache_max_available_size()),
      K(second_stat.get_micro_cache_max_available_size()), K(after_cache_stat.micro_stat()), KPC(task2), K(task2->get_comment()));

  const char *macro_cache_warn_comment = "current macro cache available size";
  ASSERT_NE(nullptr, strstr(task2->get_comment().ptr(), macro_cache_warn_comment));
  const char *size_comment = "size:";
  ASSERT_NE(nullptr, strstr(task2->get_comment().ptr(), size_comment));
  ASSERT_TRUE(data_block_ids.count() + meta_block_ids.count() > second_stat.macro_data_block_num_ + second_stat.macro_block_hit_cnt_);
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->free_file_size(max_hot_tablet_size, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
                                                           false/*is_for_dir*/));
  FLOG_INFO("[TEST] test_prewarm_macro_block end", K(tnt_disk_space_mgr->get_macro_cache_free_size()));
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_micro_cache_full)
{
  FLOG_INFO("[TEST] start test_micro_cache_full");

  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  OK(exe_sql("create table test_micro_cache (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_micro_cache");
  OK(exe_sql("insert into test_micro_cache values (1)"));
  OK(exe_sql("insert into test_micro_cache values (2)"));
  OK(medium_compact(run_ctx_.tablet_id_.id()));
  FLOG_INFO("[TEST] finish to wait major freeze");

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(tnt_disk_space_mgr, nullptr);

  ObTenantFileManager *tnt_file_mgr = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, tnt_file_mgr);
  tnt_file_mgr->persist_disk_space_task_.enable_adjust_size_ = false;
  ob_usleep(5 * 1000 * 1000);

  const int64_t block_size = micro_cache->phy_blk_size_;
  ObSSMicroCacheStat &cache_stat = micro_cache->cache_stat_;
  ObSSARCInfo &arc_info = micro_cache->micro_meta_mgr_.arc_info_;
  const int64_t micro_cache_file_size = micro_cache->cache_file_size_;

  const int64_t max_micro_size = 16 * 1024; // 16KB

  const int64_t WRITE_BLK_CNT = 300;
  ObArenaAllocator allocator;
  char *data_buf = static_cast<char*>(allocator.alloc(max_micro_size));
  ASSERT_NE(nullptr, data_buf);
  MEMSET(data_buf, 'a', max_micro_size);
  ObSSPhyBlockCommonHeader common_header;
  ObSSMicroDataBlockHeader data_blk_header;
  const int64_t payload_offset = common_header.get_serialize_size() + data_blk_header.get_serialize_size();

  int64_t prev_add_micro_cnt = 0;
  int64_t prev_reorgan_cnt = 0;
  const int64_t total_cnt = micro_cache_file_size / block_size;
  const int64_t total_data_blk_cnt = micro_cache->phy_blk_mgr_.blk_cnt_info_.micro_data_blk_max_cnt();
  const int64_t TOTAL_ROUND = (total_cnt / WRITE_BLK_CNT) + 1;
  int64_t before_max_available_size = 0;
  micro_cache->get_available_space_for_prewarm(before_max_available_size);
  FLOG_INFO("[TEST] start to write to micro cache", K(block_size), K(micro_cache_file_size), K(TOTAL_ROUND), K(total_data_blk_cnt), K(total_cnt), K(before_max_available_size));
  micro_cache->task_runner_.disable_release_cache(); // Stop micro cache release task to ensure filling up micro cache
  for (int64_t round = 0; round < TOTAL_ROUND; ++round) {
    for (int64_t i = WRITE_BLK_CNT * round; i < WRITE_BLK_CNT * (round + 1); ++i) {
      MacroBlockId macro_id = TestSSCommonUtil::gen_macro_block_id(i + 1);
      int32_t offset = payload_offset;
      do {
        const int32_t micro_size = max_micro_size;
        ObSSMicroBlockCacheKey micro_key = TestSSCommonUtil::gen_phy_micro_key(macro_id, offset, micro_size);
        micro_cache->add_micro_block_cache(micro_key, data_buf, micro_size, macro_id.second_id(), ObSSMicroCacheAccessType::COMMON_IO_TYPE);
        offset += micro_size;
      } while (offset < block_size);
      ob_usleep((i / 50 + 1) * 5 * 1000);
    }
  }
  int64_t after_max_available_size = 0;
  micro_cache->get_available_space_for_prewarm(after_max_available_size);
  ASSERT_TRUE(before_max_available_size > after_max_available_size);

  FLOG_INFO("[TEST] micro cache has no space now", K(before_max_available_size), K(after_max_available_size), K(cache_stat));
  OK(exe_sql("alter table test_micro_cache storage_cache_policy (global = 'auto');"));
  OK(exe_sql("alter table test_micro_cache storage_cache_policy (global = 'hot');"));
  ObStorageCachePolicyPrewarmer micro_cache_prewarmer;
  ObStorageCacheTabletTask *micro_cache_task = static_cast<ObStorageCacheTabletTask *>(ob_malloc(
      sizeof(ObStorageCacheTabletTask),
      ObMemAttr(run_ctx_.tenant_id_, "TestPrewarm2")));
  ASSERT_NE(nullptr, micro_cache_task);
  new (micro_cache_task) ObStorageCacheTabletTask();
  OK(micro_cache_task->init(
      run_ctx_.tenant_id_,
      run_ctx_.ls_id_.id(),
      run_ctx_.tablet_id_.id(),
      PolicyStatus::HOT));
  micro_cache_task->inc_ref_count();
  OK(micro_cache_task->set_status(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING));
  FLOG_INFO("[TEST] start to prewarm micro_cache_task", KPC(micro_cache_task));

  OK(micro_cache_prewarmer.prewarm_hot_tablet(micro_cache_task));
  ObSSPrewarmStat second_stat;
  second_stat = micro_cache_prewarmer.get_hot_retention_prewarm_stat();
  ObSSMicroCacheStat &after_cache_stat = micro_cache->cache_stat_;
  int64_t after_max_available_size2 = 0;
  micro_cache->get_available_space_for_prewarm(after_max_available_size2);
  FLOG_INFO("[TEST] second stat", K(second_stat), K(after_cache_stat.micro_stat()), K(after_cache_stat), K(after_max_available_size2));
  ASSERT_LT(second_stat.get_macro_block_bytes(), second_stat.get_macro_cache_max_available_size());
  ASSERT_GE(second_stat.get_micro_block_bytes(), second_stat.get_micro_cache_max_available_size());
  OK(micro_cache_task->generate_comment());
  const char *macro_cache_warn_comment = "current micro cache available size";
  ASSERT_NE(nullptr, strstr(micro_cache_task->get_comment().ptr(), macro_cache_warn_comment));
  const char *size_comment = "size:";
  ASSERT_NE(nullptr, strstr(micro_cache_task->get_comment().ptr(), size_comment));

  FLOG_INFO("[TEST] test micro cache full, macro cache is not full", K(second_stat), K(second_stat.get_macro_cache_max_available_size()),
      K(second_stat.get_micro_cache_max_available_size()), K(after_cache_stat.micro_stat()), KPC(micro_cache_task), K(micro_cache_task->get_comment()));
  FLOG_INFO("[TEST] finish test_micro_cache_full");
}
}
}

int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
