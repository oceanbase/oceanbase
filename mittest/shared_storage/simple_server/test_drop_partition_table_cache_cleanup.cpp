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
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/shared_storage/prewarm/ob_ss_base_prewarmer.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_private_block_gc_task.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"

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
  return "test_drop_partition_table_cache_cleanup_";
}

namespace oceanbase
{
namespace unittest
{

class ObDropPartitionTableCacheCleanupTest : public ObStorageCachePolicyPrewarmerTest
{
public:
  virtual void SetUp() override
  {
    ObStorageCachePolicyPrewarmerTest::SetUp();
    {
      share::ObTenantSwitchGuard tguard;
      OK(tguard.switch_to(run_ctx_.tenant_id_));
      OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
    }
  }

  void get_shared_blocks_for_tablet(common::ObIArray<blocksstable::MacroBlockId> &block_ids);
  int check_local_cache(const MacroBlockId &macro_id, const bool expect_exist);
  int check_micro_cache(const MacroBlockId &macro_id, const char *macro_buf, const int64_t macro_size, const bool expect_exist);
  void wait_macro_cache_gc(const common::ObIArray<blocksstable::MacroBlockId> &block_ids);
  void wait_macro_cache_gc_for_tablet(const common::ObIArray<blocksstable::MacroBlockId> &block_ids, const ObTabletID &tablet_id);
  void set_ls_and_tablet_id_for_partition_run_ctx(const char *table_name, const char *partition_name);
};

void ObDropPartitionTableCacheCleanupTest::get_shared_blocks_for_tablet(
    common::ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  bool is_old_version_empty = false;
  int64_t current_tablet_version = -1;
  int64_t current_tablet_trans_seq = -1;
  int64_t last_gc_version = -1;
  uintptr_t tablet_fingerprint = 0;
  bool is_transfer_out_deleted = false;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr*)->get_current_version_for_tablet(run_ctx_.ls_id_,
            run_ctx_.tablet_id_, current_tablet_version, last_gc_version, current_tablet_trans_seq,
            tablet_fingerprint, is_old_version_empty, is_transfer_out_deleted));
  ASSERT_NE(-1, current_tablet_version);
  ASSERT_TRUE(is_old_version_empty);
  ASSERT_GE(last_gc_version, -1);
  ASSERT_LT(last_gc_version, current_tablet_version);

  ObPrivateBlockGCHandler handler(run_ctx_.ls_id_, run_ctx_.ls_epoch_, run_ctx_.tablet_id_,
                                  current_tablet_version, last_gc_version, current_tablet_trans_seq,
                                  tablet_fingerprint, is_transfer_out_deleted);
  ASSERT_EQ(OB_SUCCESS, handler.get_blocks_for_tablet(current_tablet_version, true/*is_shared*/, block_ids));
  LOG_INFO("get shared blocks for tablet", K(block_ids));
}

int ObDropPartitionTableCacheCleanupTest::check_local_cache(
    const MacroBlockId &macro_id, const bool expect_exist)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyPrewarmer macro_reader(1);
  if (OB_FAIL(macro_reader.async_read_from_object_storage(macro_id))) {
    LOG_WARN("fail to read meta macro", KR(ret), K(macro_id), K(expect_exist));
  } else if (OB_FAIL(macro_reader.batch_read_wait())) {
    LOG_WARN("fail to wait read", KR(ret), K(macro_id), K(expect_exist));
  } else {
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    PrewarmLocalCacheType ss_prewarm_local_cache_type = PrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
    macro_reader.check_prewarm_cache_type(macro_reader.read_handles_[0].get_macro_id(), macro_reader.read_handles_[0].get_buffer(),
        macro_reader.read_handles_[0].get_data_size(), ss_prewarm_local_cache_type);
    if (OB_ISNULL(macro_cache_mgr)) {
      LOG_INFO("skip shared object meta magic header macro", K(macro_id), K(macro_reader.read_handles_[0]));
    } else if (ObSSPrewarmLocalCacheType::is_skipped(ss_prewarm_local_cache_type)) {
      LOG_INFO("skip shared object meta magic header macro", K(macro_id), K(macro_reader.read_handles_[0]));
    } else if (ObSSPrewarmLocalCacheType::is_micro_cache(ss_prewarm_local_cache_type)) {
      if (OB_FAIL(check_micro_cache(macro_id, macro_reader.read_handles_[0].get_buffer(), macro_reader.read_handles_[0].get_data_size(), expect_exist))) {
        LOG_WARN("fail to check micro cache", KR(ret), K(macro_id));
      }
    } else if (ObSSPrewarmLocalCacheType::is_macro_cache(ss_prewarm_local_cache_type)) {
      bool is_hit = false;
      ObSSFdCacheHandle fd_handle;
      if (OB_FAIL(macro_cache_mgr->get(macro_id, run_ctx_.tablet_id_, 0, is_hit, fd_handle))) {
        LOG_WARN("fail to get macro cache", KR(ret), K(macro_id));
      } else {
        LOG_INFO("check macro cache", K(macro_id), K(is_hit), K(expect_exist));
        if (expect_exist != is_hit) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect_exist is not equal to is_hit", KR(ret), K(macro_id), K(expect_exist), K(is_hit));
        }
      }
    }
  }
  return ret;
}

int ObDropPartitionTableCacheCleanupTest::check_micro_cache(
    const MacroBlockId &macro_id, const char *macro_buf, const int64_t macro_size, const bool expect_exist)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMicroCache is NULL", KR(ret));
  } else {
    ObMicroBlockBareIterator micro_iter;
    int64_t micro_offset = 0;
    ObMicroBlockData micro_data;
    ObSSMicroBlockCacheKey micro_key;
    ObSSBasePrewarmer macro_reader;
    PrewarmLocalCacheType ss_prewarm_local_cache_type = PrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
    if (OB_FAIL(macro_reader.open_micro_iterator(macro_id, macro_buf, macro_size, micro_iter))) {
      LOG_WARN("fail to open iterator", KR(ret), K(macro_id), K(macro_size));
    } else {
      while (OB_SUCC(ret)) {
        micro_offset = 0;
        micro_data.reset();
        micro_key.reset();

        if (OB_FAIL(micro_iter.get_next_micro_block_data_and_offset(micro_data, micro_offset))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next micro data failed", KR(ret), K(macro_id), K(micro_iter));
          }
        } else {
          micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
          micro_key.micro_id_.macro_id_ = macro_id;
          micro_key.micro_id_.offset_ = micro_offset;
          micro_key.micro_id_.size_ = micro_data.size_;
        }

        ObSSMicroBlockMetaInfo micro_meta_info;
        ObSSMicroCacheHitType hit_type = ObSSMicroCacheHitType::SS_CACHE_MISS;
        if (FAILEDx(micro_cache->check_micro_block_exist(
            micro_key, micro_meta_info, hit_type))) {
          LOG_WARN("fail to check_micro_block_exist", KR(ret), K(micro_key), K(macro_id));
        } else {
          if (OB_UNLIKELY(expect_exist == (ObSSMicroCacheHitType::SS_CACHE_MISS == hit_type))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expect_exist vs hit_type mismatch", KR(ret), K(expect_exist), K(hit_type), K(micro_key), K(macro_id));
          }
        }
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

void ObDropPartitionTableCacheCleanupTest::wait_macro_cache_gc(
    const common::ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  wait_macro_cache_gc_for_tablet(block_ids, run_ctx_.tablet_id_);
}

void ObDropPartitionTableCacheCleanupTest::wait_macro_cache_gc_for_tablet(
    const common::ObIArray<blocksstable::MacroBlockId> &block_ids, const ObTabletID &tablet_id)
{
  LOG_INFO("start to wait macro cache gc", K(block_ids), K(tablet_id));
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_gc_finish = false;
  while (!is_gc_finish) {
    bool exist_not_gc_blocks = false;
    for (int64_t i = 0; i < block_ids.count(); ++i) {
      bool is_hit = false;
      ObSSFdCacheHandle fd_handle;
      ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(block_ids.at(i), tablet_id, 0, is_hit, fd_handle));
      if (is_hit) {
        exist_not_gc_blocks = true;
      }
    }
    if (!exist_not_gc_blocks) {
      is_gc_finish = true;
    }
    sleep(1);
  }
  LOG_INFO("finish to wait macro cache gc", K(block_ids), K(tablet_id));
}

void ObDropPartitionTableCacheCleanupTest::set_ls_and_tablet_id_for_partition_run_ctx(
    const char *table_name, const char *partition_name)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObSqlString sql;
  int64_t tablet_id = 0;
  int64_t ls_id_val = 0;
  OK(sql.assign_fmt("SELECT tablet_id, ls_id FROM oceanbase.DBA_OB_TABLE_LOCATIONS WHERE TABLE_NAME = '%s' AND PARTITION_NAME = '%s' LIMIT 1",
        table_name, partition_name));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    OK(result->next());
    OK(result->get_int("tablet_id", tablet_id));
    OK(result->get_int("ls_id", ls_id_val));
  }
  run_ctx_.tablet_id_ = tablet_id;

  ObLSID ls_id(ls_id_val);

  ObLSHandle ls_handle;
  OK(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::TXSTORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  run_ctx_.ls_id_ = ls->get_ls_id();
  run_ctx_.ls_epoch_ = ls->get_ls_epoch();
  run_ctx_.tenant_epoch_ = MTL_EPOCH_ID();
  LOG_INFO("finish set run ctx for partition", K(run_ctx_), K(table_name), K(partition_name));
}

TEST_F(ObDropPartitionTableCacheCleanupTest, drop_table_hot_cache_cleanup)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  preread_task.is_inited_ = false;

  ObArray<blocksstable::MacroBlockId> block_ids;

  exe_prepare_sql();
  OK(exe_sql("create table test_table (a int) storage_cache_policy (global = 'hot');"));
  set_ls_and_tablet_id_for_run_ctx("test_table");

  OK(exe_sql("insert into test_table values (1);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_table values (2);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_table values (3);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  wait_ss_minor_compaction_finish();
  get_shared_blocks_for_tablet(block_ids);
  for (int64_t i = 0; i < block_ids.count(); ++i) {
    if (block_ids.at(i).is_shared_data_or_meta()) {
      check_local_cache(block_ids.at(i), true);
    }
  }

  OK(exe_sql("drop table test_table;"));
  OK(exe_sql("purge recyclebin;"));
  OK(exe_sql("alter system minor freeze;"));
  wait_macro_cache_gc(block_ids);
}

TEST_F(ObDropPartitionTableCacheCleanupTest, drop_table_auto_cache_cleanup)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  preread_task.is_inited_ = false;

  ObArray<blocksstable::MacroBlockId> block_ids;

  exe_prepare_sql();
  OK(exe_sql("create table test_table (a int) storage_cache_policy (global = 'auto');"));
  set_ls_and_tablet_id_for_run_ctx("test_table");

  OK(exe_sql("insert into test_table values (1);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_table values (2);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_table values (3);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  wait_ss_minor_compaction_finish();
  get_shared_blocks_for_tablet(block_ids);
  for (int64_t i = 0; i < block_ids.count(); ++i) {
    if (block_ids.at(i).is_shared_data_or_meta()) {
      check_local_cache(block_ids.at(i), true);
    }
  }

  OK(exe_sql("drop table test_table;"));
  OK(exe_sql("purge recyclebin;"));
  OK(exe_sql("alter system minor freeze;"));
  wait_macro_cache_gc(block_ids);
}

TEST_F(ObDropPartitionTableCacheCleanupTest, drop_partition_hot_table_cache_cleanup)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  preread_task.is_inited_ = false;

  ObArray<blocksstable::MacroBlockId> block_ids_p0;

  exe_prepare_sql();
  OK(exe_sql("create table test_part_table (a int, b int, primary key(a, b)) "
             "partition by range(a) (partition p0 values less than (10), partition p1 values less than (20));"));
  OK(exe_sql("alter table test_part_table storage_cache_policy = (global = 'hot');"));
  set_ls_and_tablet_id_for_partition_run_ctx("test_part_table", "p0");

  OK(exe_sql("insert into test_part_table values (1, 1);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_part_table values (2, 2);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_part_table values (3, 3);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  wait_ss_minor_compaction_finish();
  get_shared_blocks_for_tablet(block_ids_p0);
  ObTabletID dropped_tablet_id = run_ctx_.tablet_id_;
  for (int64_t i = 0; i < block_ids_p0.count(); ++i) {
    if (block_ids_p0.at(i).is_shared_data_or_meta()) {
      check_local_cache(block_ids_p0.at(i), true);
    }
  }

  OK(exe_sql("alter table test_part_table drop partition p0;"));
  OK(exe_sql("alter system minor freeze;"));
  wait_macro_cache_gc_for_tablet(block_ids_p0, dropped_tablet_id);

  OK(exe_sql("drop table test_part_table;"));
  OK(exe_sql("purge recyclebin;"));
}

TEST_F(ObDropPartitionTableCacheCleanupTest, drop_partition_auto_table_cache_cleanup)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  preread_task.is_inited_ = false;

  ObArray<blocksstable::MacroBlockId> block_ids_p0;

  exe_prepare_sql();
  OK(exe_sql("create table test_part_table (a int, b int, primary key(a, b)) "
             "partition by range(a) (partition p0 values less than (10), partition p1 values less than (20));"));
  OK(exe_sql("alter table test_part_table storage_cache_policy = (global = 'auto');"));
  set_ls_and_tablet_id_for_partition_run_ctx("test_part_table", "p0");

  OK(exe_sql("insert into test_part_table values (1, 1);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_part_table values (2, 2);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_part_table values (3, 3);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  wait_ss_minor_compaction_finish();
  get_shared_blocks_for_tablet(block_ids_p0);
  ObTabletID dropped_tablet_id = run_ctx_.tablet_id_;
  for (int64_t i = 0; i < block_ids_p0.count(); ++i) {
    if (block_ids_p0.at(i).is_shared_data_or_meta()) {
      check_local_cache(block_ids_p0.at(i), true);
    }
  }

  OK(exe_sql("alter table test_part_table drop partition p0;"));
  OK(exe_sql("alter system minor freeze;"));
  wait_macro_cache_gc_for_tablet(block_ids_p0, dropped_tablet_id);

  OK(exe_sql("drop table test_part_table;"));
  OK(exe_sql("purge recyclebin;"));
}

} // end unittest
} // end oceanbase

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
