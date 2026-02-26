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
#include "storage/shared_storage/ob_ss_reader_writer.h"

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
  return "test_storage_cache_policy_prewarmer_";
}
namespace oceanbase
{
namespace unittest
{
TEST_F(ObStorageCachePolicyPrewarmerTest, test_clean_task_history)
{
  // test clean history
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  FLOG_INFO("[TEST] test_clean_task_history start");

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);

  int ret = OB_SUCCESS;
  // Simulate clean tablet_task_map
  SCPTabletTaskMap &tablet_tasks = policy_service->get_tablet_tasks();
  ASSERT_EQ(OB_SUCCESS, tablet_tasks.clear());
  ASSERT_EQ(0, tablet_tasks.size());
  const int64_t start_time = ObTimeUtility::current_time();
  // Add 25,000 tasks, ensuring tablet_tasks.size() > 20,000
  int64_t finished_part1_end_time = 0;
  for (int i = 0; i < 25000; ++i) {
    const int64_t tablet_id = i + 200001;
    ObStorageCacheTaskStatusType status;
    int64_t end_time = start_time - (i / 1000) * 3600 * 1000LL * 1000LL;

    // The first 5,000 tasks are completed (can be deleted)
    if (i < 5000) {
      status = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED;
      finished_part1_end_time = end_time;
    }
    // The next 5000 tasks are completed but older (can be deleted)
    if (i < 10000) {
      status = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED;
    }
    // The next 5000 tasks are canceled (can be deleted)
    else if (i < 15000) {
      status = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_CANCELED;
    }
    // The middle 5000 tasks are failed (can be deleted)
    else if (i < 20000) {
      status = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FAILED;
    }
    // The remaining 2000 tasks are in the suspended state (cannot be deleted)
    else if (i < 22000) {
      status = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED;
    }
    // The last 3000 tasks are in progress (cannot be deleted)
    else {
      status = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING;
      end_time = ObTimeUtility::current_time();
    }
    add_task(tablet_tasks, status, end_time, tablet_id);
  }

  ASSERT_EQ(tablet_tasks.size(), 25000);


  OK(exe_sql("create table test_clean (col1 int, col2 int)"
              "partition by range(col1)"
              "subpartition by list(col2)"
              "(partition p0 values less than(100)"
              "(subpartition sp0 values in (1, 2, 3),"
              "subpartition sp1 values in (4, 5, 6)),"
              "partition p1 values less than(200)"
              "(subpartition sp2 values in (1, 2, 3),"
              "subpartition sp3 values in (4, 5, 6),"
              "subpartition sp4 values in (7, 8, 9)));"));

  // Simulate clean tablet_status_map
  hash::ObHashMap<int64_t, PolicyStatus> &tablet_status_map = policy_service->get_tablet_status_map();
  ASSERT_EQ(OB_SUCCESS, tablet_status_map.clear());
  ASSERT_EQ(0, tablet_status_map.size());
  for (int i = 0; i < 1000; ++i) {
    // add tasks to be cleaned
    const int64_t tablet_id = i + 200001;
    int ret = tablet_status_map.set_refactored(tablet_id, PolicyStatus::HOT);
    if (OB_FAIL(ret)) {
      LOG_INFO("[TEST] fail to set refactored", KR(ret), K(i));
    }
    ASSERT_EQ((ret == OB_SUCCESS) || (ret == OB_HASH_EXIST), true);
  }
  ASSERT_EQ(1000, tablet_status_map.size());

  // Simulate clean part_policy_map
  hash::ObHashMap<int64_t, ObTabletPolicyInfo> &part_policy_map = policy_service->get_part_policy_map();
  ASSERT_EQ(OB_SUCCESS, part_policy_map.clear());
  ASSERT_EQ(0, part_policy_map.size());
  ObPartition partition;

  for (int i = 0; i < 1000; ++i) {
    partition.reset();
    // add tasks to be cleaned
    const int64_t part_id = i + 500001;
    partition.set_part_id(part_id);
    ObTabletPolicyInfo tablet_policy_info;
    partition.set_part_storage_cache_policy_type(ObStorageCachePolicyType::HOT_POLICY);
    ASSERT_EQ(OB_SUCCESS, tablet_policy_info.init(&partition));
    int ret = part_policy_map.set_refactored(part_id, tablet_policy_info);
    if (OB_FAIL(ret)) {
      LOG_INFO("[TEST] fail to set refactored", KR(ret), K(i));
    }
    ASSERT_EQ((ret == OB_SUCCESS) || (ret == OB_HASH_EXIST), true);
  }
  ASSERT_EQ(1000, tablet_status_map.size());

  // Simulate clean table_policy_map

  hash::ObHashMap<int64_t, ObTablePolicyInfo> &table_policy_map = policy_service->get_table_policy_map();
  ASSERT_EQ(OB_SUCCESS, table_policy_map.clear());
  ASSERT_EQ(0, table_policy_map.size());
  ObStorageCachePolicy table_policy;
  table_policy.set_global_policy(ObStorageCachePolicyType::HOT_POLICY);
  for (int i = 0; i < 1000; ++i) {
    partition.reset();
    // add tasks to be cleaned
    const int64_t table_id = i + 500001;
    ObTablePolicyInfo table_policy_info;

    partition.set_part_storage_cache_policy_type(ObStorageCachePolicyType::HOT_POLICY);
    ASSERT_EQ(OB_SUCCESS, table_policy_info.init(table_id, table_policy, 0));
    int ret = table_policy_map.set_refactored(table_id, table_policy_info);
    if (OB_FAIL(ret)) {
      LOG_INFO("[TEST] fail to set refactored", KR(ret), K(i));
    }
    ASSERT_EQ((ret == OB_SUCCESS) || (ret == OB_HASH_EXIST), true);
  }
  ASSERT_EQ(1000, tablet_status_map.size());

  TG_CANCEL_TASK(policy_service->refresh_policy_scheduler_.tg_id_, policy_service->refresh_policy_scheduler_.clean_history_task_);
  TG_WAIT_TASK(policy_service->refresh_policy_scheduler_.tg_id_, policy_service->refresh_policy_scheduler_.clean_history_task_);
  TG_SCHEDULE(policy_service->refresh_policy_scheduler_.tg_id_, policy_service->refresh_policy_scheduler_.clean_history_task_,
      2 * 1000 * 1000, true);
  sleep(10);

  // Check result after clean task history
  ASSERT_EQ(10000, tablet_tasks.size());
  int64_t suspended_task_count = 0;
  int64_t finished_task_count = 0;
  int64_t doing_task_count = 0;
  int64_t other_task_count = 0;
  for (auto it = tablet_tasks.begin(); it != tablet_tasks.end(); ++it) {
    ObStorageCacheTabletTaskHandle task_handle = it->second;
    int64_t tablet_id = it->first;
    ASSERT_NE(task_handle(), nullptr);
    switch(task_handle()->get_status()) {
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_SUSPENDED:
        suspended_task_count++;
        break;
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_FINISHED:
        finished_task_count++;
        if (task_handle()->get_end_time() < finished_part1_end_time) {
          LOG_WARN("[TEST] check clean history task time", K(tablet_id), K(suspended_task_count), K(finished_task_count),
              K(doing_task_count), K(doing_task_count));
        }
        ASSERT_GE(task_handle()->get_end_time(), finished_part1_end_time);
        break;
      case ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING:
        doing_task_count++;
        break;
      default:
        other_task_count++;
        LOG_WARN("[TEST] unexpected task status", KPC(task_handle()), K(tablet_id), K(suspended_task_count),
            K(finished_task_count), K(doing_task_count), K(doing_task_count));
    }
  }
  ASSERT_EQ(suspended_task_count, 2000);
  ASSERT_EQ(doing_task_count, 3000);
  ASSERT_EQ(finished_task_count, 5000);
  ASSERT_EQ(0, other_task_count);

  ASSERT_EQ(5, tablet_status_map.size());
  ASSERT_EQ(7, part_policy_map.size());
  ASSERT_EQ(1, table_policy_map.size());
  ASSERT_EQ(OB_SUCCESS, tablet_tasks.clear());
  FLOG_INFO("[TEST] test_clean_task_history end");
}

TEST_F(ObStorageCachePolicyPrewarmerTest, basic)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObStorageCachePolicyPrewarmer prewarmer;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  OK(exe_sql("create table test_table (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_table");

  OK(exe_sql("insert into test_table values (1)"));
  sleep(1);
  OK(medium_compact(run_ctx_.tablet_id_.id()));

  // 1.test basic hot retention prewarm
  ObStorageCacheTabletTask *task = static_cast<ObStorageCacheTabletTask *>(ob_malloc(
      sizeof(ObStorageCacheTabletTask),
      ObMemAttr(run_ctx_.tenant_id_, "ObStorageCache")));
  ASSERT_NE(nullptr, task);
  new (task) ObStorageCacheTabletTask();
  PolicyStatus policy_status = PolicyStatus::HOT;
  OK(task->init(
      run_ctx_.tenant_id_,
      run_ctx_.ls_id_.id(),
      run_ctx_.tablet_id_.id(),
      policy_status));
  task->inc_ref_count();
  // read init major
  ObSSPrewarmStat first_stat;
  const int64_t MAX_RETRY_TIMES = 3;
  bool succeed = false;

  task->status_ = ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING;
  for (int i = 0; i < MAX_RETRY_TIMES && !succeed; i++) {
    IGNORE_RETURN micro_cache->clear_micro_cache();
    OK(prewarmer.prewarm_hot_tablet(task));
    first_stat = prewarmer.get_hot_retention_prewarm_stat();
    LOG_INFO("read init major", K(i), K(first_stat));
    ASSERT_TRUE(first_stat.macro_block_fail_cnt_== 0);
    ASSERT_TRUE(first_stat.micro_block_fail_cnt_ == 0);
    // ASSERT_TRUE(first_stat.macro_block_hit_cnt_ == 0);
    ASSERT_TRUE(first_stat.micro_block_hit_cnt_ == 0);
    ASSERT_TRUE(first_stat.micro_block_add_cnt_ == first_stat.micro_block_num_);
    succeed = first_stat.macro_block_fail_cnt_ == 0
        && first_stat.micro_block_fail_cnt_ == 0
        // && first_stat.macro_block_hit_cnt_ == 0
        && first_stat.micro_block_hit_cnt_ == 0
        && first_stat.micro_block_add_cnt_ == first_stat.micro_block_num_;
  }
  ASSERT_TRUE(succeed);

  task->dec_ref_count();
  LOG_INFO("[TEST] basic end");
}
TEST_F(ObStorageCachePolicyPrewarmerTest, test_comment)
{
  FLOG_INFO("[TEST] finish test_comment");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  OK(exe_sql("create table test_comment (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_comment");

  ObSCPTraceIdGuard scp_trace_id_guard;
  ObStorageCacheTabletTask *task = static_cast<ObStorageCacheTabletTask *>(ob_malloc(
      sizeof(ObStorageCacheTabletTask),
      ObMemAttr(run_ctx_.tenant_id_, "TestPrewarm2")));
  ASSERT_NE(nullptr, task);
  new (task) ObStorageCacheTabletTask();
  OK(task->init(
      run_ctx_.tenant_id_,
      run_ctx_.ls_id_.id(),
      run_ctx_.tablet_id_.id(),
      PolicyStatus::HOT));
  task->inc_ref_count();
  task->result_ = OB_INVALID_ARGUMENT;
  OK(task->generate_comment());
  const char *ret_err = "OB_INVALID_ARGUMENT";
  FLOG_INFO("[TEST] finish test_comment", K(task->get_comment()), KPC(task));
  ASSERT_NE(nullptr, strstr(task->get_comment().ptr(), ret_err));
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_convert_hot_to_auto)
{
  FLOG_INFO("[TEST] start test_convert_hot_to_auto");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  SCPTabletTaskMap &tablet_task_map = policy_service->get_tablet_tasks();

  OK(exe_sql("create table test_convert_hot_to_auto (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_convert_hot_to_auto");

  OK(exe_sql("insert into test_convert_hot_to_auto values (1)"));
  OK(exe_sql("insert into test_convert_hot_to_auto values (2)"));
  OK(exe_sql("insert into test_convert_hot_to_auto values (3)"));

  OK(medium_compact(run_ctx_.tablet_id_.id()));
  OK(exe_sql("alter table test_convert_hot_to_auto storage_cache_policy (global = 'hot');"));
  wait_task_finished(run_ctx_.tablet_id_.id());
  check_macro_blocks_type(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK);

  ObStorageCacheTabletTaskHandle hot_tablet_task_handle;
  ASSERT_EQ(OB_SUCCESS, tablet_task_map.get_refactored(run_ctx_.tablet_id_.id(), hot_tablet_task_handle));
  ASSERT_NE(nullptr, hot_tablet_task_handle());
  LOG_INFO("[TEST] finish hot task", KPC(hot_tablet_task_handle()));
  ASSERT_EQ(hot_tablet_task_handle()->get_policy_status(), ObStorageCachePolicyStatus::HOT);
  ASSERT_EQ(hot_tablet_task_handle()->get_status(), ObStorageCacheTaskStatus::TaskStatus::OB_STORAGE_CACHE_TASK_FINISHED);
  hot_tablet_task_handle()->status_ = ObStorageCacheTaskStatus::TaskStatus::OB_STORAGE_CACHE_TASK_DOING;
  ASSERT_EQ(hot_tablet_task_handle()->get_status(), ObStorageCacheTaskStatus::TaskStatus::OB_STORAGE_CACHE_TASK_DOING);
  FLOG_INFO("[TEST] finish change task status", KPC(hot_tablet_task_handle()));
  OK(exe_sql("alter table test_convert_hot_to_auto storage_cache_policy (global = 'auto');"));
  sleep(30);
  check_macro_blocks_type(ObSSMacroCacheType::MACRO_BLOCK);
  FLOG_INFO("[TEST] finish check auto type", KPC(hot_tablet_task_handle()));
  ASSERT_EQ(hot_tablet_task_handle()->get_status(), ObStorageCacheTaskStatus::TaskStatus::OB_STORAGE_CACHE_TASK_CANCELED);
  FLOG_INFO("[TEST] finish test_convert_hot_to_auto");
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_incremental_trigger)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  SCPTabletTaskMap &tablet_task_map = policy_service->get_tablet_tasks();

  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  checkpoint::ObTabletGCService *gc_service = MTL(checkpoint::ObTabletGCService*);
  ASSERT_NE(nullptr, gc_service);
  gc_service->stop();
  sleep(10);

  ObTenantDiskSpaceManager *tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*);
  ASSERT_NE(tnt_disk_space_mgr, nullptr);
  macro_cache_mgr->evict_task_.is_inited_ = false;
  OK(exe_sql("create table test_incremental_trigger (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_incremental_trigger");

  ObSSMacroCacheStat macro_cache_stat;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, macro_cache_stat));
  const int64_t hot_tablet_macro_cache_min_threshold = macro_cache_stat.get_min();
  FLOG_INFO("[TEST] start case2 in test_incremental_trigger");

  // Case 1
  // 1. Simulate hot tablet macro cache is full to get a skipped tablet task
  int64_t max_tablet_size = tnt_disk_space_mgr->get_macro_cache_free_size();
  OK(tnt_disk_space_mgr->alloc_file_size(max_tablet_size, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, ObDiskSpaceType::FILE));
  int64_t after_alloc_tablet_size1 = tnt_disk_space_mgr->get_macro_cache_free_size();
  ASSERT_EQ(0, after_alloc_tablet_size1);

  OK(exe_sql("insert into test_incremental_trigger values (7)"));
  OK(exe_sql("insert into test_incremental_trigger values (8)"));
  OK(exe_sql("insert into test_incremental_trigger values (9)"));
  OK(medium_compact(run_ctx_.tablet_id_.id()));
  OK(exe_sql("alter table test_incremental_trigger storage_cache_policy (global = 'hot');"));
  wait_task_finished(run_ctx_.tablet_id_.id());

  ObStorageCacheTabletTaskHandle tablet_task_handle1;
  ASSERT_EQ(OB_SUCCESS, tablet_task_map.get_refactored(run_ctx_.tablet_id_.id(), tablet_task_handle1));
  ASSERT_NE(nullptr, tablet_task_handle1());
  ASSERT_EQ(true, tablet_task_handle1()->is_skipped_macro_cache());
  ASSERT_EQ(true, policy_service->tablet_scheduler_.get_exist_skipped_tablet());
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->free_file_size(max_tablet_size, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
                                                           ObDiskSpaceType::FILE));

  // 2. The total macro cache is full, but HOT_TABLET_MACRO_BLOCK is below min value
  int64_t max_tablet_size2 = tnt_disk_space_mgr->get_macro_cache_free_size();
  OK(tnt_disk_space_mgr->alloc_file_size(max_tablet_size2, ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE));
  int64_t after_alloc_tablet_size2 = tnt_disk_space_mgr->get_macro_cache_free_size();
  ASSERT_EQ(0, after_alloc_tablet_size2);

  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  ObStorageCacheTabletTaskHandle after_trigger_tablet_task_handle1;
  ASSERT_EQ(OB_SUCCESS, tablet_task_map.get_refactored(run_ctx_.tablet_id_.id(), after_trigger_tablet_task_handle1));
  ASSERT_NE(nullptr, after_trigger_tablet_task_handle1());
  ASSERT_EQ(false, after_trigger_tablet_task_handle1()->is_skipped_macro_cache());
  ASSERT_EQ(false, policy_service->tablet_scheduler_.get_exist_skipped_tablet());
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->free_file_size(max_tablet_size2, ObSSMacroCacheType::MACRO_BLOCK,
                                                           ObDiskSpaceType::FILE));

  // Case 2
  // 1. Simulate hot tablet macro cache is full to get a skipped tablet task
  FLOG_INFO("[TEST] start case2 in test_incremental_trigger");
  int64_t max_tablet_size3 = tnt_disk_space_mgr->get_macro_cache_free_size();
  OK(tnt_disk_space_mgr->alloc_file_size(max_tablet_size3, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, ObDiskSpaceType::FILE));
  int64_t after_alloc_tablet_size3 = tnt_disk_space_mgr->get_macro_cache_free_size();
  ASSERT_EQ(0, after_alloc_tablet_size3);

  OK(exe_sql("create table test_incremental_trigger2 (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_incremental_trigger2");
  OK(exe_sql("insert into test_incremental_trigger2 values (1)"));
  OK(exe_sql("insert into test_incremental_trigger2 values (2)"));
  OK(exe_sql("insert into test_incremental_trigger2 values (3)"));
  OK(medium_compact(run_ctx_.tablet_id_.id()));
  OK(exe_sql("alter table test_incremental_trigger2 storage_cache_policy (global = 'hot');"));
  wait_task_finished(run_ctx_.tablet_id_.id());

  FLOG_INFO("[TEST] prepare for case2 finished");
  ObStorageCacheTabletTaskHandle tablet_task_handle2;
  ASSERT_EQ(OB_SUCCESS, tablet_task_map.get_refactored(run_ctx_.tablet_id_.id(), tablet_task_handle2));
  ASSERT_NE(nullptr, tablet_task_handle2());
  FLOG_INFO("[TEST] tablet task handle2", KPC(tablet_task_handle2()));
  ASSERT_EQ(true, tablet_task_handle2()->is_skipped_macro_cache());
  ASSERT_EQ(true, policy_service->tablet_scheduler_.get_exist_skipped_tablet());
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->free_file_size(max_tablet_size3, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
                                                           ObDiskSpaceType::FILE));

  // 2. Free space is below free threshold while hot tablet space is exceed min value
  int64_t max_tablet_size4 = tnt_disk_space_mgr->get_macro_cache_size();
  // hot tablet macro cache: 75%
  const int64_t alloc_hot_tablet_perc = (hot_tablet_macro_cache_min_threshold + 5);
  int64_t alloc_size1 = (max_tablet_size4 * alloc_hot_tablet_perc) / 100;
  OK(tnt_disk_space_mgr->alloc_file_size(alloc_size1, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, ObDiskSpaceType::FILE));
  ObSSMacroCacheStat macro_cache_stat1;
  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->get_macro_cache_stat(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, macro_cache_stat1));
  ASSERT_GT(macro_cache_stat1.used_, (max_tablet_size4 * hot_tablet_macro_cache_min_threshold) / 100);
  int64_t alloc_size2 = tnt_disk_space_mgr->get_macro_cache_free_size();
  alloc_size2 -= (max_tablet_size4 * (ObStorageCachePolicyService::MACRO_CACHE_FREE_SPACE_THRESHOLD + 2)) / 100;

  OK(tnt_disk_space_mgr->alloc_file_size(alloc_size2, ObSSMacroCacheType::MACRO_BLOCK, ObDiskSpaceType::FILE));
  for (int i=0; i<SS_MACRO_CACHE_MAX_TYPE_VAL; i++) {
    FLOG_INFO("[TEST] print macro cache all stats444", K(tnt_disk_space_mgr->macro_cache_stats_[i]));
  }
  int64_t after_alloc_tablet_size4 = tnt_disk_space_mgr->get_macro_cache_free_size();
  FLOG_INFO("[TEST] max_tablet_size4", K(alloc_hot_tablet_perc), K(max_tablet_size4), K(alloc_size1), K(alloc_size2),
      K(macro_cache_stat1.used_), K(after_alloc_tablet_size4), K((max_tablet_size4 * ObStorageCachePolicyService::MACRO_CACHE_FREE_SPACE_THRESHOLD) / 100));
  ASSERT_GT(after_alloc_tablet_size4, (max_tablet_size4 * ObStorageCachePolicyService::MACRO_CACHE_FREE_SPACE_THRESHOLD) / 100);

  ASSERT_EQ(OB_SUCCESS, policy_service->trigger_or_refresh_tablet(ObStorageCachePolicyRefreshType::REFRESH_TYPE_NORMAL));
  ObStorageCacheTabletTaskHandle after_trigger_tablet_task_handle2;
  ASSERT_EQ(OB_SUCCESS, tablet_task_map.get_refactored(run_ctx_.tablet_id_.id(), after_trigger_tablet_task_handle2));
  ASSERT_NE(nullptr, after_trigger_tablet_task_handle2());
  ASSERT_EQ(false, after_trigger_tablet_task_handle2()->is_skipped_macro_cache());
  ASSERT_EQ(false, policy_service->tablet_scheduler_.get_exist_skipped_tablet());

  ASSERT_EQ(OB_SUCCESS, tnt_disk_space_mgr->free_file_size(alloc_size1, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK,
                                                           ObDiskSpaceType::FILE));
  FLOG_INFO("[TEST] finished test_incremental_trigger");
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_force_refresh_policy_task)
{
  FLOG_INFO("[TEST] start test_force_refresh_policy_task");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  OK(exe_sql("create table test_force_refresh_policy_task (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_force_refresh_policy_task");

  OK(exe_sql("alter table test_force_refresh_policy_task storage_cache_policy (global = 'hot');"));
  wait_task_finished(run_ctx_.tablet_id_.id());
  FLOG_INFO("[TEST] wait task finished in test_force_refresh_policy_task");

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  PolicyStatus policy_status = PolicyStatus::MAX_STATUS;
  ASSERT_EQ(OB_SUCCESS, policy_service->tablet_status_map_.get_refactored(run_ctx_.tablet_id_.id(), policy_status));
  ASSERT_EQ(PolicyStatus::HOT, policy_status);
  ASSERT_EQ(OB_SUCCESS,policy_service->tablet_status_map_.erase_refactored(run_ctx_.tablet_id_.id()));
  policy_service->refresh_policy_scheduler_.force_refresh_policy_task_.runTimerTask();
  ASSERT_EQ(OB_SUCCESS, policy_service->tablet_status_map_.get_refactored(run_ctx_.tablet_id_.id(), policy_status));
  ASSERT_EQ(PolicyStatus::HOT, policy_status);
  ASSERT_TRUE(policy_service->get_need_generate_cache_task());
  FLOG_INFO("[TEST] finish test_force_refresh_policy_task");
}

TEST_F(ObStorageCachePolicyPrewarmerTest, test_prewarm_tablet_if_hot_on_macro_miss)
{
  FLOG_INFO("[TEST] start test_prewarm_tablet_if_hot_on_macro_miss");
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));
  exe_prepare_sql();

  int ret = OB_SUCCESS;

  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);

  // Prepare table and make it HOT so that tablet_status_map_ records HOT status
  OK(exe_sql("create table test_prewarm_if_hot (a int)"));
  set_ls_and_tablet_id_for_run_ctx("test_prewarm_if_hot");

  OK(exe_sql("insert into test_prewarm_if_hot values (1)"));
  sleep(1);
  OK(medium_compact(run_ctx_.tablet_id_.id()));
  OK(exe_sql("alter table test_prewarm_if_hot storage_cache_policy (global = 'hot');"));

  // Wait the initial HOT prewarm task finished to make tablet_status_map_ stable
  wait_task_finished(run_ctx_.tablet_id_.id());

  // Ensure the tablet is recognized as HOT
  bool is_hot = false;
  OK(policy_service->is_hot_tablet(run_ctx_.tablet_id_.id(), is_hot));
  ASSERT_TRUE(is_hot);
  // Make sure active_tablet_ids_ contains this tablet
  OK(policy_service->update_active_tablet_ids());

  // Clear micro cache, macro cache and mem macro cache
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  ASSERT_NE(nullptr, local_cache_service);
  OK(local_cache_service->clear_ss_all_cache(10L * 1000L * 1000L));
  const int64_t start_ts = ObTimeUtility::current_time();

  // Read macro block to trigger prewarm
  ObSEArray<MacroBlockId, 64> data_block_ids;
  ObSEArray<MacroBlockId, 16> meta_block_ids;
  data_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx_.tenant_id_, "MetaBlockIDs"));
  OK(get_macro_blocks(run_ctx_.ls_id_, run_ctx_.tablet_id_, data_block_ids, meta_block_ids));
  FLOG_INFO("[TEST] get macro blocks", K(run_ctx_), K(data_block_ids), K(meta_block_ids));
  ASSERT_GT(data_block_ids.count(), 0);

  //
  const MacroBlockId macro_id = data_block_ids.at(0);
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  OK(macro_cache_mgr->evict_by_macro_id(macro_id));
  sleep(2);
  bool is_exist = false;
  macro_cache_mgr->evict_by_macro_id(macro_id);
  sleep(2);
  OK(macro_cache_mgr->erase(macro_id));
  OK(macro_cache_mgr->exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);

  blocksstable::ObStorageObjectHandle read_handle;
  ObStorageObjectReadInfo read_info;
  char buf[1024] = {0};
  read_info.macro_block_id_ = macro_id;
  read_info.size_ = sizeof(buf);
  read_info.offset_ = 0;
  read_info.io_timeout_ms_ = 10LL * common::S_US / 1000LL;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
  read_info.buf_ = buf;
  read_info.mtl_tenant_id_ = MTL_ID();
  read_info.set_effective_tablet_id(run_ctx_.tablet_id_);
  OK(OB_STORAGE_OBJECT_MGR.read_object(read_info, read_handle));

  sleep(5);
  wait_task_finished(run_ctx_.tablet_id_.id());

  // verify that new tablet task is generated
  ObStorageCacheTabletTaskHandle task_handle;
  OK(policy_service->tablet_scheduler_.tablet_task_map_.get_refactored(run_ctx_.tablet_id_.id(), task_handle));
  LOG_INFO("[TEST] task handle", K(run_ctx_), KPC(task_handle()));
  ASSERT_NE(nullptr, task_handle());
  ASSERT_GT(task_handle()->get_start_time(), start_ts);

  FLOG_INFO("[TEST] finish test_prewarm_tablet_if_hot_on_macro_miss");
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
