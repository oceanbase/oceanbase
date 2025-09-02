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
#include "lib/utility/ob_test_util.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "storage/shared_storage/prewarm/ob_ss_base_prewarmer.h"
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

namespace oceanbase
{
char *shared_storage_info = nullptr;
namespace unittest
{

struct TestRunCtx
{
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;

  TO_STRING_KV(K(tenant_id_), K(tenant_epoch_), K(ls_id_), K(ls_epoch_), K(tablet_id_));
};

class ObMacroCacheMultiVersionGCTest : public ObSimpleClusterTestBase
{
public:
  ObMacroCacheMultiVersionGCTest()
    : ObSimpleClusterTestBase("test_macro_cache_multi_version_gc_", "50G", "50G", "50G"),
      tenant_created_(false),
      run_ctx_()
  {}

  virtual void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    if (!tenant_created_) {
      OK(create_tenant("tt1", "5G", "10G", false/*oracle_mode*/, 8));
      OK(get_tenant_id(run_ctx_.tenant_id_));
      ASSERT_NE(0, run_ctx_.tenant_id_);
      OK(get_curr_simple_server().init_sql_proxy2());
      tenant_created_ = true;
      {
        share::ObTenantSwitchGuard tguard;
        OK(tguard.switch_to(run_ctx_.tenant_id_));
        OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
      }
    }
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

  int exe_sql(const char *sql_str)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_ISNULL(sql_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql str is null", KR(ret), KP(sql_str));
    } else if (OB_FAIL(sql.assign(sql_str))) {
      LOG_WARN("fail to assign sql", KR(ret), K(sql_str));
    } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql_str), K(sql));
    }
    return ret;
  }

  int sys_exe_sql(const char *sql_str)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_ISNULL(sql_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql str is null", KR(ret), KP(sql_str));
    } else if (OB_FAIL(sql.assign(sql_str))) {
      LOG_WARN("fail to assign sql", KR(ret), K(sql_str));
    } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy().write(sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql_str), K(sql));
    }
    return ret;
  }

  void get_tablet_version(int64_t &tablet_version);
  void get_shared_blocks_for_tablet(common::ObIArray<blocksstable::MacroBlockId> &block_ids);
  void get_gc_blocks(common::ObIArray<blocksstable::MacroBlockId> &block_ids_v1,
                     common::ObIArray<blocksstable::MacroBlockId> &block_ids_v2,
                     common::ObIArray<blocksstable::MacroBlockId> &gc_block_ids);
  int check_micro_cache(const MacroBlockId &macro_id, const char *macro_buf, const int64_t macro_size, const bool expect_exist);
  int check_local_cache(const MacroBlockId &macro_id, const bool expect_exist);
  void wait_macro_cache_gc(const common::ObIArray<blocksstable::MacroBlockId> &block_ids);
  void wait_minor_finish();
  void wait_ss_minor_compaction_finish();
  void set_ls_and_tablet_id_for_run_ctx();

protected:
  bool tenant_created_;
  TestRunCtx run_ctx_;
};

void ObMacroCacheMultiVersionGCTest::get_tablet_version(int64_t &tablet_version)
{
  ObArray<int64_t> tablet_versions;
  bool is_old_version_empty = false;
  int64_t current_tablet_version = -1;
  int64_t current_tablet_trans_seq = -1;
  int64_t last_gc_version = -1;
  uintptr_t tablet_fingerprint = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr*)->get_current_version_for_tablet(run_ctx_.ls_id_,
              run_ctx_.tablet_id_, current_tablet_version, last_gc_version, current_tablet_trans_seq,
              tablet_fingerprint, is_old_version_empty));
    ASSERT_NE(-1, current_tablet_version);
    ASSERT_GE(last_gc_version, -1);
    ASSERT_LT(last_gc_version, current_tablet_version);
    if (!is_old_version_empty) continue;

    ObPrivateBlockGCHandler handler(run_ctx_.ls_id_, run_ctx_.ls_epoch_, run_ctx_.tablet_id_,
                                    current_tablet_version, last_gc_version,
                                    current_tablet_trans_seq, tablet_fingerprint);
    LOG_INFO("wait old tablet version delete", K(current_tablet_version), K(is_old_version_empty),
             K(run_ctx_.ls_id_), K(run_ctx_.ls_epoch_), K(handler));
    ASSERT_EQ(OB_SUCCESS, handler.list_tablet_meta_version(tablet_versions));
    usleep(100 * 1000);
  } while (1 != tablet_versions.count());
  tablet_version = tablet_versions.at(0);
}

void ObMacroCacheMultiVersionGCTest::get_shared_blocks_for_tablet(
    common::ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  bool is_old_version_empty = false;
  int64_t current_tablet_version = -1;
  int64_t current_tablet_trans_seq = -1;
  int64_t last_gc_version = -1;
  uintptr_t tablet_fingerprint = 0;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr*)->get_current_version_for_tablet(run_ctx_.ls_id_,
            run_ctx_.tablet_id_, current_tablet_version, last_gc_version, current_tablet_trans_seq,
            tablet_fingerprint, is_old_version_empty));
  ASSERT_NE(-1, current_tablet_version);
  ASSERT_TRUE(is_old_version_empty);
  ASSERT_GE(last_gc_version, -1);
  ASSERT_LT(last_gc_version, current_tablet_version);

  ObPrivateBlockGCHandler handler(run_ctx_.ls_id_, run_ctx_.ls_epoch_, run_ctx_.tablet_id_,
                                  current_tablet_version, last_gc_version, current_tablet_trans_seq,
                                  tablet_fingerprint);
  ASSERT_EQ(OB_SUCCESS, handler.get_blocks_for_tablet(current_tablet_version, true/*is_shared*/, block_ids));
  LOG_INFO("get shared blocks for tablet", K(block_ids));
}

void ObMacroCacheMultiVersionGCTest::get_gc_blocks(
    common::ObIArray<blocksstable::MacroBlockId> &block_ids_v1,
    common::ObIArray<blocksstable::MacroBlockId> &block_ids_v2,
    common::ObIArray<blocksstable::MacroBlockId> &gc_block_ids)
{
  common::hash::ObHashSet<blocksstable::MacroBlockId> block_id_set;
  ASSERT_EQ(OB_SUCCESS, block_id_set.create(100/*bucket_num*/));
  for (int64_t i = 0; i < block_ids_v2.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, block_id_set.set_refactored(block_ids_v2.at(i)));
  }
  for (int64_t i = 0; i < block_ids_v1.count(); ++i) {
    int tmp_ret = block_id_set.exist_refactored(block_ids_v1.at(i));
    if (OB_HASH_EXIST == tmp_ret) {
      // reused macro, should not be gc
    } else if (OB_HASH_NOT_EXIST == tmp_ret) {
      // non-reused macro, should be gc
      ASSERT_EQ(OB_SUCCESS, gc_block_ids.push_back(block_ids_v1.at(i)));
    } else {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected error", K(tmp_ret));
    }
  }
  LOG_INFO("get gc blocks", K(block_ids_v1), K(block_ids_v2), K(gc_block_ids));
}

int ObMacroCacheMultiVersionGCTest::check_local_cache(
    const MacroBlockId &macro_id, const bool expect_exist)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyPrewarmer macro_reader(1/*parallelism*/);
  if (OB_FAIL(macro_reader.async_read_from_object_storage(macro_id))) {
    LOG_WARN("fail to read meta macro", KR(ret), K(macro_id), K(expect_exist));
  } else if (OB_FAIL(macro_reader.batch_read_wait())) {
    LOG_WARN("fail to wait read", KR(ret), K(macro_id), K(expect_exist));
  } else {
    ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
    PrewarmLocalCacheType ss_prewarm_local_cache_type = PrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
    macro_reader.check_prewarm_cache_type(macro_reader.read_handles_[0].get_macro_id(), macro_reader.read_handles_[0].get_buffer(),
        macro_reader.read_handles_[0].get_data_size(), ss_prewarm_local_cache_type);
    if(OB_ISNULL(macro_cache_mgr)) {
      LOG_INFO("skip shared object meta magic header macro",
          K(macro_id), K(macro_reader.read_handles_[0]));
    } else if (ObSSPrewarmLocalCacheType::is_skipped(ss_prewarm_local_cache_type)) {
      LOG_INFO("skip shared object meta magic header macro",
          K(macro_id), K(macro_reader.read_handles_[0]));
    } else if (ObSSPrewarmLocalCacheType::is_micro_cache(ss_prewarm_local_cache_type)) {
      if (OB_FAIL(check_micro_cache(macro_id, macro_reader.read_handles_[0].get_buffer(), macro_reader.read_handles_[0].get_data_size(), expect_exist))) {
        LOG_WARN("fail to check micro cache", KR(ret), K(macro_id));
      }
    } else if (ObSSPrewarmLocalCacheType::is_macro_cache(ss_prewarm_local_cache_type)) {
      bool is_hit = false;
      ObSSFdCacheHandle fd_handle;
      if(OB_FAIL(macro_cache_mgr->get(macro_id, run_ctx_.tablet_id_, 0/*hit size*/, is_hit, fd_handle))) {
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

int ObMacroCacheMultiVersionGCTest::check_micro_cache(
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
            LOG_WARN("if expect_exist is true, should hit; if expect_exist is false, should miss",
                KR(ret), K(expect_exist), K(hit_type), K(micro_key), K(macro_id));
          }
        }
      } // end while
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}


void ObMacroCacheMultiVersionGCTest::wait_macro_cache_gc(
    const common::ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  LOG_INFO("start to wait macro cache gc", K(block_ids));
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_gc_finish = false;
  while (!is_gc_finish) {
    bool exist_not_gc_blocks = false;
    for (int64_t i = 0; (i < block_ids.count()); ++i) {
      bool is_hit = false;
      ObSSFdCacheHandle fd_handle;
      ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->get(block_ids.at(i), run_ctx_.tablet_id_, 0/*hit size*/, is_hit, fd_handle));
      if (is_hit) {
        exist_not_gc_blocks = true;
      }
    }
    if (!exist_not_gc_blocks) {
      is_gc_finish = true;
    }
    sleep(1);
  }
  LOG_INFO("finish to wait macro cache gc", K(block_ids));
}

void ObMacroCacheMultiVersionGCTest::wait_minor_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait minor begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=0;",
              run_ctx_.tenant_id_, run_ctx_.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000);
    LOG_INFO("minor result", K(row_cnt));
  } while (row_cnt > 0);
  LOG_INFO("minor finished", K(row_cnt));
}

void ObMacroCacheMultiVersionGCTest::wait_ss_minor_compaction_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait ss minor compaction begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=12;",
              run_ctx_.tenant_id_, run_ctx_.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000);
    LOG_INFO("minor result", K(row_cnt));
  } while (row_cnt > 0);

  row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=11 and table_flag = 1;",
              run_ctx_.tenant_id_, run_ctx_.tablet_id_.id()));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    usleep(100 * 1000);
    LOG_INFO("minor result", K(row_cnt));
  } while (row_cnt <= 0);
  LOG_INFO("wait ss minor compaction finished");
}

void ObMacroCacheMultiVersionGCTest::set_ls_and_tablet_id_for_run_ctx()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  uint64_t tablet_id = 0;
  OK(sql.assign("select tablet_id from oceanbase.__all_virtual_table where table_name='test_table';"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    OK(result->next());
    OK(result->get_uint("tablet_id", tablet_id));
  }
  run_ctx_.tablet_id_ = tablet_id;

  sql.reset();
  int64_t id = 0;
  OK(sql.assign_fmt("select ls_id from oceanbase.__all_tablet_to_ls where tablet_id=%ld;", tablet_id));
  SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res2, sql.ptr()));
    sqlclient::ObMySQLResult *result = res2.get_result();
    ASSERT_NE(nullptr, result);
    OK(result->next());
    OK(result->get_int("ls_id", id));
  }
  ObLSID ls_id(id);

  ObLSHandle ls_handle;
  OK(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::TXSTORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  run_ctx_.ls_id_ = ls->get_ls_id();
  run_ctx_.ls_epoch_ = ls->get_ls_epoch();
  run_ctx_.tenant_epoch_ = MTL_EPOCH_ID();
  LOG_INFO("finish set run ctx", K(run_ctx_));
}

TEST_F(ObMacroCacheMultiVersionGCTest, multi_version_gc_and_tablet_gc)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  preread_task.is_inited_ = false;
  ObArray<blocksstable::MacroBlockId> block_ids_v1;
  ObArray<blocksstable::MacroBlockId> block_ids_v2;
  ObArray<blocksstable::MacroBlockId> gc_block_ids;

  OK(exe_sql("alter system set _ss_schedule_upload_interval = '1s';"));
  OK(exe_sql("alter system set inc_sstable_upload_thread_score = 20;"));
  OK(exe_sql("alter system set _ss_garbage_collect_interval = '10s';"));
  OK(exe_sql("alter system set _ss_garbage_collect_file_expiration_time = '10s';"));
  OK(sys_exe_sql("alter system set ob_compaction_schedule_interval = '3s' tenant tt1;"));
  OK(sys_exe_sql("alter system set minor_compact_trigger = 2 tenant tt1;"));
  OK(sys_exe_sql("alter system set _ss_major_compaction_prewarm_level = 0 tenant tt1;"));
  OK(exe_sql("create table test_table (a int)"));
  set_ls_and_tablet_id_for_run_ctx();
  storage::ObSSBasePrewarmer::OB_SS_PREWARM_CACHE_THRESHOLD = 0;
  // trigger minor compaction by minor freeze 3 times. it prewarms minor sstable into local
  // macro cache, and update tablet table store.
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
  get_shared_blocks_for_tablet(block_ids_v1);
  for (int64_t i = 0; (i < block_ids_v1.count()); ++i) {
    if (block_ids_v1.at(i).is_shared_data_or_meta()) {
      check_local_cache(block_ids_v1.at(i), true/*expect_exist*/);
    }
  }

  // delete existing values and insert new values, and trigger minor compaction by minor freeze 2 times.
  // it prewarms new minor sstable into local macro cache, and update tablet table store.
  OK(exe_sql("delete from test_table;"));
  sleep(1);

  OK(exe_sql("insert into test_table values (4);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_table values (5);"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  wait_ss_minor_compaction_finish();
  get_shared_blocks_for_tablet(block_ids_v2);
  get_gc_blocks(block_ids_v1, block_ids_v2, gc_block_ids);
  wait_macro_cache_gc(gc_block_ids);


  // drop table, and trigger tablet gc
  OK(exe_sql("drop table test_table;"));
  OK(exe_sql("purge recyclebin;"));
  OK(exe_sql("alter system minor freeze;")); // in order to accelerate tablet gc
  wait_macro_cache_gc(block_ids_v2);
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
