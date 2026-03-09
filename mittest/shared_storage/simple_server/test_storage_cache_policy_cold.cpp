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
#include "mittest/shared_storage/simple_server/test_storage_cache_common_util.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#undef private
#undef protected

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
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
        guard.set_sslog_proxy((ObISSLogProxy *)proxy);
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
  return "test_storage_cache_policy_cold_";
}
namespace oceanbase
{
namespace unittest
{

// hot to cold，test evict local cache
TEST_F(ObStorageCachePolicyColdTest, test_convert_hot_to_cold)
{
  FLOG_INFO("[TEST] start test_convert_hot_auto_to_cold_evict");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ASSERT_EQ(OB_SUCCESS, micro_cache->clear_micro_cache());

  // Set max_micro_blk_size_ to allow 16KB micro blocks to be written
  // This is needed after MR 261057 which added rejection logic for large micro blocks
  // Note: We need to set both the config and the cached value because background task
  // (MicCacheMTimer) will periodically update max_micro_blk_size_ from config.
  // The base class SetUpTestCase sets _ss_micro_cache_max_block_size to 1, which would
  // cause all micro blocks larger than 1 byte to be rejected.
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(run_ctx_.tenant_id_));
  ASSERT_TRUE(tenant_config.is_valid());
  tenant_config->_ss_micro_cache_max_block_size = 128 * 1024; // 128KB
  micro_cache->micro_meta_mgr_.max_micro_blk_size_ = 128 * 1024; // 128KB

  // Test case 1: hot to cold
  OK(exe_sql("create table test_hot_to_cold (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_hot_to_cold");

  OK(exe_sql("insert into test_hot_to_cold values (1)"));
  OK(exe_sql("insert into test_hot_to_cold values (2)"));
  OK(exe_sql("insert into test_hot_to_cold values (3)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));

  // 1. set hot policy, wait prewarm task finished
  OK(exe_sql("alter table test_hot_to_cold storage_cache_policy (global = 'hot');"));
  wait_task_finished(run_ctx_.tablet_id_.id());

  // 2. check macro block in cache
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  FLOG_INFO("[TEST] data_block_ids", K(data_block_ids));
  FLOG_INFO("[TEST] meta_block_ids", K(meta_block_ids));
  bool is_exist = false;
  int64_t macro_count_before_evict = 0;
  int64_t micro_count_before_evict = 0;

  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
      if (is_exist) {
        macro_count_before_evict++;
        FLOG_INFO("[TEST] macro block in cache before evict", K(i), K(data_block_ids.at(i)));
      }
    }
  }
  FLOG_INFO("[TEST] macro_count_before_evict", K(macro_count_before_evict));
  ASSERT_GT(macro_count_before_evict, 0);
  FLOG_INFO("[TEST] before evict, macro count in cache", K(macro_count_before_evict));
  micro_count_before_evict = micro_cache->micro_meta_mgr_.micro_meta_map_.count();
  FLOG_INFO("[TEST] micro_count_before_evict", K(micro_count_before_evict));
  ASSERT_GT(micro_count_before_evict, 0);
  bool micro_need_delete = false;
  ASSERT_EQ(OB_SUCCESS, micro_cache->micro_meta_mgr_.is_tablet_need_delete(run_ctx_.tablet_id_, micro_need_delete));
  ASSERT_FALSE(micro_need_delete);

  // 3. convert to cold policy, should trigger evict local cache
  OK(exe_sql("alter table test_hot_to_cold storage_cache_policy (global = 'cold');"));
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  ASSERT_EQ(OB_SUCCESS, micro_cache->micro_meta_mgr_.is_tablet_need_delete(run_ctx_.tablet_id_, micro_need_delete));
  ASSERT_TRUE(micro_need_delete);
  // 4. check macro block and micro block evicted
  int64_t macro_count_after_evict = 0;
  FLOG_INFO("[TEST] after evict, data_block_ids", K(data_block_ids));
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
      if (is_exist) {
        macro_count_after_evict++;
        FLOG_INFO("[TEST] macro block in cache after evict", K(i), K(data_block_ids.at(i)));
      }
    }
  }
  FLOG_INFO("[TEST] after evict, macro count in cache", K(macro_count_after_evict));
  ASSERT_EQ(0, macro_count_after_evict);
  int64_t micro_wait_time = 0;
  while (micro_cache->micro_meta_mgr_.tablets_to_delete_.size() > 0) {
    sleep(1);
    micro_wait_time++;
    if (micro_wait_time % 30 == 0) {
      FLOG_INFO("[TEST] micro tablet to delete remain", K(micro_cache->micro_meta_mgr_.tablets_to_delete_.size()));
    }
  }
  OK(exe_sql("drop table if exists test_hot_to_cold"));
  FLOG_INFO("[TEST] finish test_convert_hot_to_cold");
}

TEST_F(ObStorageCachePolicyColdTest, test_convert_auto_to_cold)
{
  FLOG_INFO("[TEST] start test_convert_hot_auto_to_cold_evict");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  OK(exe_sql("drop table if exists test_auto_to_cold"));
  OK(exe_sql("alter system set default_storage_cache_policy = 'auto'"));
  OK(exe_sql("create table test_auto_to_cold (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_auto_to_cold");

  OK(exe_sql("insert into test_auto_to_cold values (1)"));
  OK(exe_sql("insert into test_auto_to_cold values (2)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));

  // 1. set auto policy
  OK(exe_sql("alter table test_auto_to_cold storage_cache_policy (global = 'auto');"));

  // 2. get macro block id
  ObSEArray<MacroBlockId, 128> auto_data_block_ids;
  ObSEArray<MacroBlockId, 128> auto_meta_block_ids;
  auto_data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "AutoDataBlkIDs"));
  auto_meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "AutoMetaBlkIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, auto_data_block_ids, auto_meta_block_ids));
  FLOG_INFO("[TEST] auto policy before evict, auto_data_block_ids", K(auto_data_block_ids));
  FLOG_INFO("[TEST] auto policy before evict, auto_meta_block_ids", K(auto_meta_block_ids));
  int64_t macro_count_before_evict = 0;
  bool is_exist = false;
  for (int i = 0; i < auto_data_block_ids.count(); i++) {
    if (auto_data_block_ids.at(i).is_shared_data_or_meta()) {
      FLOG_INFO("[TEST] check macro block in cache before evict", K(i), K(auto_data_block_ids.at(i)));
      OK(macro_cache_mgr->exist(auto_data_block_ids.at(i), is_exist));
      if (is_exist) {
        macro_count_before_evict++;
        FLOG_INFO("[TEST] macro block in cache before evict", K(i), K(auto_data_block_ids.at(i)));
      }
    }
  }
  FLOG_INFO("[TEST] auto policy before evict, macro count", K(macro_count_before_evict));

  // 3. convert to cold policy
  OK(exe_sql("alter table test_auto_to_cold storage_cache_policy (global = 'cold');"));
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));

  // 4. check macro block evicted
  int64_t macro_count_after_evict = auto_data_block_ids.count();
  int64_t wait_time = 0;
  while(macro_count_after_evict > 0) {
    sleep(1);
    wait_time++;
    if (wait_time % 60 == 0) {
      FLOG_INFO("[TEST] macros remaining in cache after eviction", K(macro_count_after_evict));
    }
    macro_count_after_evict = 0;
    for (int i = 0; i < auto_data_block_ids.count(); i++) {
      if (auto_data_block_ids.at(i).is_shared_data_or_meta()) {
        OK(macro_cache_mgr->exist(auto_data_block_ids.at(i), is_exist));
        if (is_exist) {
          ObSSMacroCacheMetaHandle meta_handle;
          OK(macro_cache_mgr->get_macro_cache_meta(auto_data_block_ids.at(i), is_exist, meta_handle));
          macro_count_after_evict++;
          FLOG_INFO("[TEST] macro block in cache after evict", K(i), K(auto_data_block_ids.at(i)), K(meta_handle()->get_is_write_cache()));
        }
      }
    }
  }
  OK(exe_sql("drop table if exists test_auto_to_cold"));
  FLOG_INFO("[TEST] finish test_convert_auto_to_cold");
}

// cold to hot，macro will become hot macro
TEST_F(ObStorageCachePolicyColdTest, test_convert_cold_to_hot)
{
  FLOG_INFO("[TEST] start test_convert_cold_to_hot");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  OK(exe_sql("alter system set default_storage_cache_policy = 'cold'"));
  OK(exe_sql("create table test_cold_to_hot (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_cold_to_hot");

  OK(exe_sql("insert into test_cold_to_hot values (1)"));
  OK(exe_sql("insert into test_cold_to_hot values (2)"));
  OK(exe_sql("insert into test_cold_to_hot values (3)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));

  // 1. get macro block id
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));

  // 2. convert to hot policy, wait prewarm task finished
  OK(exe_sql("alter table test_cold_to_hot storage_cache_policy (global = 'hot');"));
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  wait_task_finished(run_ctx_.tablet_id_.id());

  // 3. check macro block type is HOT_TABLET_MACRO_BLOCK
  check_macro_blocks_type(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK);

  // 4. check macro block in cache
  bool is_exist = false;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      ObSSMacroCacheMetaHandle meta_handle;
      OK(macro_cache_mgr->get_macro_cache_meta(data_block_ids.at(i), is_exist, meta_handle));
      if (is_exist) {
        ASSERT_EQ(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, meta_handle()->cache_type_);
        FLOG_INFO("[TEST] macro block type is HOT_TABLET_MACRO_BLOCK", K(data_block_ids.at(i)),
            K(meta_handle()->cache_type_));
      }
    }
  }
  FLOG_INFO("[TEST] finish test_convert_cold_to_hot");
}

// cold to auto，if prewarm is triggered, will be stored in cache
TEST_F(ObStorageCachePolicyColdTest, test_convert_cold_to_auto)
{
  FLOG_INFO("[TEST] start test_convert_cold_to_auto");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  OK(exe_sql("alter system set default_storage_cache_policy = 'cold'"));
  OK(exe_sql("create table test_cold_to_auto (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_cold_to_auto");

  OK(exe_sql("insert into test_cold_to_auto values (1)"));
  OK(exe_sql("insert into test_cold_to_auto values (2)"));
  OK(exe_sql("insert into test_cold_to_auto values (3)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));

  // 1. trigger refresh to ensure tablet status is updated to cold in tablet_status_map_
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));

  // 2. get macro block id
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));

  // 3. check macro block not in cache
  bool is_exist = false;
  int64_t macro_count_before = 0;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
      if (is_exist) {
        macro_count_before++;
        FLOG_INFO("[TEST] macro block in cache before convert cold to auto", K(i), K(data_block_ids.at(i)));
      }
    }
  }
  FLOG_INFO("[TEST] cold policy, macro count is not in cache", K(macro_count_before));

  // 4. convert to auto policy
  OK(exe_sql("alter table test_cold_to_auto storage_cache_policy (global = 'auto');"));
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));

  // 5. trigger query to trigger prewarm
  OK(exe_sql("select * from test_cold_to_auto"));

  // 6. check macro block in cache
  int64_t macro_count_after = 0;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
      if (is_exist) {
        macro_count_after++;
        // check macro block type is MACRO_BLOCK
        ObSSMacroCacheMetaHandle meta_handle;
        bool meta_exist = false;
        OK(macro_cache_mgr->get_macro_cache_meta(data_block_ids.at(i), meta_exist, meta_handle));
        if (meta_exist) {
          ASSERT_EQ(ObSSMacroCacheType::MACRO_BLOCK, meta_handle()->cache_type_);
        }
      }
    }
  }
  FLOG_INFO("[TEST] auto policy after prewarm, macro count in cache", K(macro_count_after));
  FLOG_INFO("[TEST] finish test_convert_cold_to_auto");
}

// cold policy not in macro cache, not in micro cache, but in macro memory cache
TEST_F(ObStorageCachePolicyColdTest, test_cold_policy_cache_behavior)
{
  FLOG_INFO("[TEST] start test_cold_policy_cache_behavior");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
  ASSERT_NE(nullptr, mem_macro_cache);
  OK(exe_sql("alter system set default_storage_cache_policy = 'cold'"));
  OK(exe_sql("create table test_cold_cache_behavior (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_cold_cache_behavior");

  OK(exe_sql("insert into test_cold_cache_behavior values (1)"));
  OK(exe_sql("insert into test_cold_cache_behavior values (2)"));
  OK(exe_sql("insert into test_cold_cache_behavior values (3)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));

  // 1. get macro block id
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));

  // 2. trigger refresh to ensure tablet status is updated to cold in tablet_status_map_
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));

  // 3. try to aio_read twice, no micro cache hit
  FLOG_INFO("[TEST] try to aio_read twice, no micro cache hit");
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  ObStorageCacheHitStat entry;
  local_cache_service->get_local_cache_tablet_stat(run_ctx_.tablet_id_, entry);
  int old_miss_cnt = entry.get_miss_cnt();
  FLOG_INFO("[TEST] old_miss_cnt", K(old_miss_cnt));
  char read_buf[1024] = {0};
  int miss_cnt = 0;

  // First round: read all blocks and erase from mem_macro_cache immediately
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      ObSSShareMacroReader share_macro_reader;
      ObStorageObjectReadInfo read_info;
      read_info.macro_block_id_ = data_block_ids.at(i);
      read_info.offset_ = 0;
      read_info.size_ = 512;
      read_info.buf_ = read_buf;
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
      read_info.mtl_tenant_id_ = MTL_ID();
      read_info.set_effective_tablet_id(run_ctx_.tablet_id_);
      ObStorageObjectHandle object_handle;
      IGNORE_RETURN(mem_macro_cache->meta_mgr_.erase_mem_macro_block_meta(read_info.macro_block_id_));
      OK(share_macro_reader.aio_read(read_info, object_handle));
      OK(object_handle.wait());
      FLOG_INFO("test aio_read", K(read_info), K(object_handle.get_data_size()));
      miss_cnt++;
    }
  }
  local_cache_service->get_local_cache_tablet_stat(run_ctx_.tablet_id_, entry);
  FLOG_INFO("[TEST] first round miss_cnt", K(old_miss_cnt + miss_cnt), K(entry.get_miss_cnt()));

  // Second round: read all blocks again to test micro cache miss
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      ObSSShareMacroReader share_macro_reader;
      ObStorageObjectReadInfo read_info;
      read_info.macro_block_id_ = data_block_ids.at(i);
      read_info.offset_ = 0;
      read_info.size_ = 512;
      read_info.buf_ = read_buf;
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
      read_info.mtl_tenant_id_ = MTL_ID();
      read_info.set_effective_tablet_id(run_ctx_.tablet_id_);
      ObStorageObjectHandle object_handle;
      IGNORE_RETURN(mem_macro_cache->meta_mgr_.erase_mem_macro_block_meta(read_info.macro_block_id_));
      OK(share_macro_reader.aio_read(read_info, object_handle));
      OK(object_handle.wait());
      FLOG_INFO("test aio_read", K(read_info), K(object_handle.get_data_size()));
      miss_cnt++;
    }
  }
  FLOG_INFO("[TEST] total miss_cnt", K(miss_cnt));
  local_cache_service->get_local_cache_tablet_stat(run_ctx_.tablet_id_, entry);
  ASSERT_EQ(old_miss_cnt + miss_cnt, entry.get_miss_cnt());

  // 4. push cold policy file to preread queue
  FLOG_INFO("[TEST] test cold policy in preread task");
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(file_manager->push_to_preread_queue(data_block_ids.at(i), run_ctx_.tablet_id_));
    }
  }
  // 5. wait preread_task read cold policy file to macro memory cache.
  int ret = OB_SUCCESS;
  int64_t start_us = ObTimeUtility::current_time();
  const int64_t timeout_us = 20 * 1000 * 1000L; // 20s
  while ((preread_cache_mgr.preread_queue_.size() != 0) ||
        (preread_task.segment_files_.count() != 0) ||
        (preread_task.free_list_.get_curr_total() != file_manager->preread_cache_mgr_.preread_task_.max_pre_read_parallelism_)) {
    ob_usleep(1000);
    if (timeout_us + start_us < ObTimeUtility::current_time()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("waiting time is too long", KR(ret),
          KR(preread_cache_mgr.preread_queue_.size()), KR(preread_task.segment_files_.count()),
          KR(preread_task.async_read_list_.get_curr_total()), KR(preread_task.async_write_list_.get_curr_total()));
      break;
    }
  }

  // 6. check macro block not in macro cache
  bool is_macro_cache_exist = false;
  int64_t macro_cache_count = 0;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_macro_cache_exist));
      if (is_macro_cache_exist && data_block_ids.at(i).storage_object_type() != ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO) {
        macro_cache_count++;
      }
    }
  }
  ASSERT_EQ(0, macro_cache_count);
  FLOG_INFO("[TEST] cold policy, macro cache count", K(macro_cache_count));

  // 7. check macro block in memory cache
  bool is_mem_macro_cache_exist = false;
  int64_t mem_macro_cache_count = 0;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(mem_macro_cache->check_exist(data_block_ids.at(i), is_mem_macro_cache_exist));
      if (is_mem_macro_cache_exist) {
        mem_macro_cache_count++;
      }
    }
  }
  ASSERT_GT(mem_macro_cache_count, 0);
  OK(exe_sql("alter system set default_storage_cache_policy = 'auto'"));
  FLOG_INFO("[TEST] cold policy, mem macro cache count", K(mem_macro_cache_count));
  FLOG_INFO("[TEST] finish test_cold_policy_cache_behavior");
}

// cold policy evicts write cache with requeue until all write cache is cleared
TEST_F(ObStorageCachePolicyColdTest, test_cold_evict_write_cache_task)
{
  FLOG_INFO("[TEST] start test_cold_evict_write_cache_task");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  // 1. create table and insert data
  OK(exe_sql("create table test_cold_write_cache (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_cold_write_cache");
  const uint64_t tablet_id = run_ctx_.tablet_id_.id();
  const uint64_t server_id = 1;
  OK(exe_sql("insert into test_cold_write_cache values (1)"));
  OK(exe_sql("insert into test_cold_write_cache values (2)"));
  OK(exe_sql("insert into test_cold_write_cache values (3)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));

  OK(exe_sql("alter table test_cold_write_cache storage_cache_policy (global = 'hot');"));
  wait_task_finished(run_ctx_.tablet_id_.id());

  // 2. Directly construct write_cache macro blocks to guarantee that we have write_cache, not read_cache
  const int64_t WRITE_IO_SIZE = 2 * 1024L * 1024L; // 2MB
  char write_buf[WRITE_IO_SIZE];
  memset(write_buf, 'a', WRITE_IO_SIZE);

  ObStorageObjectWriteInfo write_info;
  write_info.io_desc_.set_wait_event(1);
  write_info.buffer_ = write_buf;
  write_info.offset_ = 0;
  write_info.size_ = WRITE_IO_SIZE;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_BACK;
  write_info.set_effective_tablet_id(tablet_id);

  const int64_t MACRO_BLOCK_COUNT = 3;
  ObSEArray<MacroBlockId, 16> constructed_macro_ids;
  for (int64_t i = 0; i < MACRO_BLOCK_COUNT; i++) {
    MacroBlockId macro_id;
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
    macro_id.set_second_id(tablet_id);
    macro_id.set_third_id(server_id + i); // Use different server_id for each block
    macro_id.set_macro_private_transfer_epoch(0);
    macro_id.set_tenant_seq(100 + i);
    ASSERT_TRUE(macro_id.is_valid());

    // Write to macro cache as write_cache
    ObStorageObjectHandle write_object_handle;
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
    ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info, write_object_handle));
    ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
    write_object_handle.reset();

    ASSERT_EQ(OB_SUCCESS, constructed_macro_ids.push_back(macro_id));
    FLOG_INFO("[TEST] constructed write_cache macro block", K(i), K(macro_id));
  }

  // 3. Verify these macro blocks are write_cache
  bool is_exist = false;
  int64_t write_cache_macro_count = 0;
  for (int i = 0; i < constructed_macro_ids.count(); i++) {
    const MacroBlockId &macro_id = constructed_macro_ids.at(i);
    OK(macro_cache_mgr->exist(macro_id, is_exist));
    ASSERT_TRUE(is_exist);
    ObSSMacroCacheMetaHandle meta_handle;
    ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.get_refactored(macro_id, meta_handle));
    ASSERT_TRUE(meta_handle.is_valid());
    ASSERT_TRUE(meta_handle()->get_is_write_cache());
    ASSERT_EQ(tablet_id, meta_handle()->get_effective_tablet_id());
    write_cache_macro_count++;
  }
  FLOG_INFO("[TEST] verified write_cache macro blocks", K(write_cache_macro_count), K(constructed_macro_ids));
  ASSERT_EQ(MACRO_BLOCK_COUNT, write_cache_macro_count);

  // 4. Get initial write cache size
  int64_t initial_write_cache_size = macro_cache_mgr->get_write_cache_size();
  FLOG_INFO("[TEST] initial write cache size", K(initial_write_cache_size));
  // exist other macro blocks in write cache
  ASSERT_GT(initial_write_cache_size, WRITE_IO_SIZE * MACRO_BLOCK_COUNT);

  // 5. Add tablet to active_tablet_ids (required for eviction task)
  ASSERT_EQ(OB_SUCCESS, policy_service->tablet_scheduler_.active_tablet_ids_.set_refactored(tablet_id));

  // 6. Convert to cold policy to trigger eviction
  OK(exe_sql("alter table test_cold_write_cache storage_cache_policy (global = 'cold');"));
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  ASSERT_EQ(OB_HASH_EXIST, policy_service->get_need_evict_cold_tablet_ids().exist_refactored(tablet_id));
  policy_service->refresh_policy_scheduler_.evict_cold_tablet_task_.runTimerTask();

  // 7. Verify all constructed macros are evicted from cache
  int64_t remaining_macro_count = constructed_macro_ids.count();
  is_exist = false;
  int64_t wait_time = 0;
  while(remaining_macro_count > 0) {
    sleep(1);
    wait_time++;
    if (wait_time % 60 == 0) {
      FLOG_INFO("[TEST] macros remaining in cache after eviction", K(remaining_macro_count));
    }
    remaining_macro_count = 0;
    for (int i = 0; i < constructed_macro_ids.count(); i++) {
      OK(macro_cache_mgr->exist(constructed_macro_ids.at(i), is_exist));
      if (is_exist) {
        remaining_macro_count++;
      }
    }
  }
  OK(exe_sql("drop table if exists test_cold_write_cache"));
  FLOG_INFO("[TEST] finish test_cold_evict_write_cache_requeue");
}

// Test macro block time-based cold conversion and real elimination from cache
// This test combines time-based policy verification with actual cold macro eviction
TEST_F(ObStorageCachePolicyColdTest, test_macro_block_cold_elimination)
{
  FLOG_INFO("[TEST] start test_macro_block_cold_elimination");
  share::ObTenantSwitchGuard tguard;
  bool is_exist = false;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
  ASSERT_NE(nullptr, mem_macro_cache);

  OK(exe_sql("create table test_cold_macro (a int primary key, b int)"));
  set_ls_and_tablet_id_for_run_ctx("test_cold_macro");

  OK(exe_sql("insert into test_cold_macro values (1, 1)"));
  OK(exe_sql("insert into test_cold_macro values (2, 2)"));
  OK(exe_sql("insert into test_cold_macro values (3, 3)"));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));

  FLOG_INFO("[TEST] Step 1: Created table and data, waiting for data to be persisted");

  OK(exe_sql("alter table test_cold_macro storage_cache_policy (global = 'hot');"));
  wait_task_finished(run_ctx_.tablet_id_.id());

  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));

  FLOG_INFO("[TEST] Step 2: Get macro blocks", K(data_block_ids.count()), K(meta_block_ids.count()));

  check_macro_blocks_type(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK);

  int64_t initial_macro_count = 0;
  ObSEArray<MacroBlockId, 16> cached_macro_ids;
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
      if (is_exist) {
        initial_macro_count++;
        OK(cached_macro_ids.push_back(data_block_ids.at(i)));
      }
    }
  }
  FLOG_INFO("[TEST] Step 3: Initial cached macro count", K(initial_macro_count), K(cached_macro_ids));
  ASSERT_GT(initial_macro_count, 0);

  OK(exe_sql("alter table test_cold_macro storage_cache_policy "
             "(granularity = 'block', hot_retention = 10 SECOND, mixed_retention = 1 SECOND);"));
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  FLOG_INFO("[TEST] Step 4: Switched to macro_hot time policy");

  int64_t macro_count_after_policy_change = 0;
  for (int i = 0; i < cached_macro_ids.count(); i++) {
    OK(macro_cache_mgr->exist(cached_macro_ids.at(i), is_exist));
    if (is_exist) {
      macro_count_after_policy_change++;
    }
    // clear mem macro cache beacuse ObSSMemMacroCacheEvictTask will refresh mem cache to be local cache
    IGNORE_RETURN(mem_macro_cache->meta_mgr_.erase_mem_macro_block_meta(cached_macro_ids.at(i)));
  }
  FLOG_INFO("[TEST] Step 5: Macro count after policy change", K(macro_count_after_policy_change));
  ASSERT_EQ(initial_macro_count, macro_count_after_policy_change);

  const uint64_t tablet_id = run_ctx_.tablet_id_.id();
  ASSERT_EQ(OB_SUCCESS, policy_service->tablet_scheduler_.active_tablet_ids_.set_refactored(tablet_id));

  const int64_t wait_seconds = 15;
  FLOG_INFO("[TEST] Step 6: Waiting for macros to age beyond retention time",
            "wait_seconds", wait_seconds, "hot_retention", 1, "mixed_retention", 1);

  sleep(wait_seconds);

  FLOG_INFO("[TEST] Step 7 completed: Macros have aged, now triggering eliminate task");

  ObStorageCacheEliminateMacroTask &eliminate_task =
      policy_service->refresh_policy_scheduler_.eliminate_macro_task_;
  ASSERT_EQ(OB_SUCCESS, eliminate_task.eliminate_macro_blocks());

  hash::ObHashSet<MacroBlockId> &cold_macro_ids = policy_service->get_need_evict_cold_macro_block_ids();
  int64_t cold_macro_count = cold_macro_ids.size();
  FLOG_INFO("[TEST] Step 8: Cold macros identified by eliminate task", K(cold_macro_count), K(cold_macro_ids));

  ASSERT_TRUE(cold_macro_ids.created());
  ASSERT_GT(cold_macro_count, 0);

  FLOG_INFO("[TEST] Step 9: Successfully identified cold macros, proceeding with eviction");

  ObStorageCacheEvictColdMacroTask &evict_cold_macro_task =
        policy_service->refresh_policy_scheduler_.evict_cold_macro_task_;
  evict_cold_macro_task.runTimerTask();

  int64_t evict_wait_time = 0;
  int64_t remaining_cold_count = cold_macro_ids.size();
  FLOG_INFO("[TEST] Start waiting for cold macro eviction", K(remaining_cold_count));
  while(remaining_cold_count > 0 && evict_wait_time < 60) {
    sleep(1);
    evict_wait_time++;
    int64_t prev_count = remaining_cold_count;
    remaining_cold_count = cold_macro_ids.size();
    if (remaining_cold_count != prev_count || evict_wait_time % 5 == 0) {
      FLOG_INFO("[TEST] Waiting for cold macro eviction",
                K(remaining_cold_count), K(evict_wait_time), "prev_count", prev_count, "delta", prev_count - remaining_cold_count);
    }
  }

  FLOG_INFO("[TEST] Step 10: Cold macro eviction completed", "final_cold_count", cold_macro_ids.size(), "wait_time", evict_wait_time);

  int64_t evicted_count = 0;
  int64_t still_cached_count = 0;
  for (int i = 0; i < cached_macro_ids.count(); i++) {
    FLOG_INFO("[TEST] check cold macro not in macro cache", K(cached_macro_ids.at(i)));
    OK(macro_cache_mgr->exist(cached_macro_ids.at(i), is_exist));
    FLOG_INFO("[TEST] macro exist check result", K(cached_macro_ids.at(i)), K(is_exist));
    if (is_exist) {
      still_cached_count++;
      FLOG_INFO("[TEST] Warning: Cold macro still exists in cache after eviction!", "macro_id", cached_macro_ids.at(i));
    } else {
      evicted_count++;
    }
    ASSERT_FALSE(is_exist);
  }
  FLOG_INFO("[TEST] Final eviction verification", K(evicted_count), K(still_cached_count),
            "total_count", cached_macro_ids.count());

  OK(exe_sql("drop table if exists test_cold_macro"));
  FLOG_INFO("[TEST] finish test_macro_block_cold_conversion_and_real_elimination");
}

TEST_F(ObStorageCachePolicyColdTest, test_cold_merge)
{
  FLOG_INFO("[TEST] start test_cold_merge");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  OK(exe_sql("create table test_cold_merge (a varchar(4096), b varchar(4096)) storage_cache_policy (global = 'cold');"));
  set_ls_and_tablet_id_for_run_ctx("test_cold_merge");
  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  OK(exe_sql("insert into test_cold_merge select randstr(4096, random()), randstr(4096, random()) from table(generator(1024))"));
  FLOG_INFO("[TEST] Step 1: create table and insert data", K(run_ctx_));
  OK(TestCompactionUtil::medium_compact(run_ctx_.tenant_id_, run_ctx_.tablet_id_.id(), run_ctx_.ls_id_));
  ObSEArray<MacroBlockId, 128> data_block_ids;
  ObSEArray<MacroBlockId, 128> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  FLOG_INFO("[TEST] Step 2: Get macro blocks", K(data_block_ids.count()), K(meta_block_ids.count()));
  check_macro_blocks_type(ObSSMacroCacheType::MACRO_BLOCK);
  FLOG_INFO("[TEST] Step 3: check cold mini/minor/major not in local cache");
  bool is_exist = false;
  ObTenantFileManager* tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);
  for (int i = 0; i < data_block_ids.count(); i++) {
    if (data_block_ids.at(i).is_shared_data_or_meta()) {
      FLOG_INFO("[TEST] check cold mini/minor/major not in local cache", K(data_block_ids.at(i)));
      OK(macro_cache_mgr->exist(data_block_ids.at(i), is_exist));
      ASSERT_FALSE(is_exist);
      tenant_file_mgr->is_exist_remote_file(data_block_ids.at(i), 0/*ls_epoch_id*/, is_exist);
      ASSERT_TRUE(is_exist);
    }
  }
  for (int i = 0; i < meta_block_ids.count(); i++) {
    if (meta_block_ids.at(i).is_shared_data_or_meta()) {
      FLOG_INFO("[TEST] check cold mini/minor/major not in local cache", K(meta_block_ids.at(i)));
      OK(macro_cache_mgr->exist(meta_block_ids.at(i), is_exist));
      ASSERT_FALSE(is_exist);
      tenant_file_mgr->is_exist_remote_file(meta_block_ids.at(i), 0/*ls_epoch_id*/, is_exist);
      ASSERT_TRUE(is_exist);
    }
  }
  OK(exe_sql("drop table if exists test_cold_merge"));
  FLOG_INFO("[TEST] finish test_cold_merge");
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
  GCONF.datafile_size.set_value("20G");
  GCONF.memory_limit.set_value("10G");
  GCONF.system_memory.set_value("3G");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
