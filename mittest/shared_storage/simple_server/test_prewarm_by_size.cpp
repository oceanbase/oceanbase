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
#define protected public
#define private public
#define UNITTEST

#include "storage/tx_storage/ob_ls_service.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/ob_ss_micro_cache.h"
#include "lib/utility/ob_test_util.h"
#include "storage/shared_storage/task/ob_ss_preread_task.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "storage/shared_storage/prewarm/ob_ss_base_prewarmer.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"
#include "storage/shared_storage/mem_macro_cache/ob_ss_mem_macro_cache.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "logservice/palf/election/utils/election_common_define.h"
#include "mittest/shared_storage/test_ss_compaction_util.h"
#include "mittest/shared_storage/simple_server/test_storage_cache_common_util.h"

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

namespace oceanbase
{
bool scp_tenant_created = false;
namespace unittest
{

using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

struct Testrun_ctx
{
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;
  int64_t time_sec_ = 0;

  TO_STRING_KV(K(tenant_id_), K(tenant_epoch_), K(ls_id_), K(ls_epoch_), K(tablet_id_));
};

Testrun_ctx run_ctx;

class ObPrereadMetaMacroTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObPrereadMetaMacroTest() : ObSimpleClusterTestBase("test_preread_meta_macro_", "50G", "50G", "50G")
  {}
   virtual void SetUp() override
  {
    if (!scp_tenant_created) {
      // Increase MAX_TST after start() to override the default 100ms setting
      // This extends lease interval from 400ms to 2s, reducing lease expiration issues
      ObSimpleClusterTestBase::SetUp();
      oceanbase::palf::election::MAX_TST = 500 * 1000;  // 500ms, lease interval = 4 * 500ms = 2s
      OK(create_tenant_with_retry("tt1", "5G", "10G", false/*oracle_mode*/, 8));
      OK(get_tenant_id(run_ctx.tenant_id_));
      ASSERT_NE(0, run_ctx.tenant_id_);
      OK(get_curr_simple_server().init_sql_proxy2());
      scp_tenant_created = true;
      // Wait for tenant services to be ready, this helps avoid LS leader election issues
      {
        share::ObTenantSwitchGuard tguard;
        OK(tguard.switch_to(run_ctx.tenant_id_));
        OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
      }
    }
  }
  void set_ls_and_tablet_id_for_run_ctx(const char *table_name);
  void exe_prepare_sql();
  void wait_minor_finish();
  void wait_ss_minor_compaction_finish();
  int check_exist_in_micro_cache(const MacroBlockId &meta_block_id, const char *buf, const int64_t size);
  void reset_read_info();
  void check_macro_exist_in_local_cache(const MacroBlockId &macro_id, const char *macro_buf, const int64_t macro_size);
  void check_macro_exist_in_macro_cache(const int64_t tablet_id, int64_t &macro_in_macro_cache_cnt);
  void get_macro_cnt_in_local_cache(const int64_t tablet_id, int64_t &in_macro_cache_cnt, int64_t &in_micro_cache_cnt);
  void get_shared_blocks_for_tablet(common::ObIArray<blocksstable::MacroBlockId> &block_ids);
  int check_local_cache(const MacroBlockId &macro_id, const bool expect_exist);
  int check_micro_cache(const MacroBlockId &macro_id, const char *macro_buf, const int64_t macro_size, const bool expect_exist);
  void wait_preread_task_finish();
  static void TearDownTestCase()
    {
      ResidualDataCleanerHelper::clean_in_mock_env();
      ObSimpleClusterTestBase::TearDownTestCase();
    }
public:
  static const int64_t READ_IO_SIZE = DIO_READ_ALIGN_SIZE * 256 * 2; // 2MB
protected:
  char read_buf_[READ_IO_SIZE];
  ObStorageObjectReadInfo read_info_;
};

void ObPrereadMetaMacroTest::set_ls_and_tablet_id_for_run_ctx(const char* table_name)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t tablet_id = 0;
  OK(sql.assign_fmt("select tablet_id from oceanbase.__all_virtual_table where table_name='%s';", table_name));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    OK(result->next());
    OK(result->get_uint("tablet_id", tablet_id));
  }
  run_ctx.tablet_id_ = tablet_id;

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
  run_ctx.ls_id_ = ls->get_ls_id();
  run_ctx.ls_epoch_ = ls->get_ls_epoch();
  run_ctx.tenant_epoch_ = MTL_EPOCH_ID();
  LOG_INFO("[TEST] finish set run ctx", K(run_ctx), K(table_name));
}

void ObPrereadMetaMacroTest::exe_prepare_sql()
{
  int ret = OB_SUCCESS;
  OK(exe_sql("alter system set _ss_schedule_upload_interval = '1s';"));
  OK(exe_sql("alter system set inc_sstable_upload_thread_score = 20;"));
  OK(exe_sql("alter system set _ss_garbage_collect_interval = '10s';"));
  OK(exe_sql("alter system set _ss_garbage_collect_file_expiration_time = '10s';"));
  OK(sys_exe_sql("alter system set ob_compaction_schedule_interval = '3s' tenant tt1;"));
  OK(sys_exe_sql("alter system set minor_compact_trigger = 2 tenant tt1;"));
  OK(sys_exe_sql("alter system set _ss_macro_cache_miss_threshold_for_prefetch = 1 tenant tt1;"));

  sleep(10);
}

void ObPrereadMetaMacroTest::wait_minor_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait minor begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=0;",
              run_ctx.tenant_id_, run_ctx.tablet_id_.id()));
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

void ObPrereadMetaMacroTest::wait_ss_minor_compaction_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait ss minor compaction begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=12;",
              run_ctx.tenant_id_, run_ctx.tablet_id_.id()));
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
              run_ctx.tenant_id_, run_ctx.tablet_id_.id()));
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

int ObPrereadMetaMacroTest::check_exist_in_micro_cache(const MacroBlockId &meta_block_id, const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ObSSBasePrewarmer prewarmer;

  if (OB_ISNULL(micro_cache)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro cache is null", KR(ret), K(meta_block_id));
  } else {
    sleep(10);
    ObMicroBlockBareIterator micro_iter;
    if (OB_FAIL(prewarmer.open_micro_iterator(meta_block_id, buf, size, micro_iter))) {
      LOG_WARN("open micro iterator failed", KR(ret), K(meta_block_id), K(buf), K(size));
    } else {
      ObMicroBlockData micro_data;
      ObSSMicroBlockCacheKey micro_key;
      ObSSMicroBlockMetaInfo micro_meta_info;
      FLOG_INFO("[TEST] get micro block data start", K(meta_block_id), K(size));
      int64_t micro_offset = 0;
      while (OB_SUCC(ret)) {
        count++;
        ret = OB_SUCCESS;
        micro_offset = 0;
        micro_data.reset();
        micro_key.reset();
        micro_meta_info.reset();
        ObSSMicroCacheHitType hit_type = ObSSMicroCacheHitType::SS_CACHE_MISS;
        if (OB_FAIL(micro_iter.get_next_micro_block_data_and_offset(micro_data, micro_offset))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next micro data failed", KR(ret), K(meta_block_id), K(micro_iter));
          }
        } else {
          micro_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
          micro_key.micro_id_.macro_id_ = meta_block_id;
          micro_key.micro_id_.offset_ = micro_offset;
          micro_key.micro_id_.size_ = micro_data.size_;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(micro_cache->check_micro_block_exist(micro_key, micro_meta_info, hit_type))) {
            LOG_WARN("check micro block exist failed", KR(ret), K(count), K(meta_block_id), K(micro_key), K(micro_meta_info));
          } else if (ObSSMicroCacheHitType::SS_CACHE_MISS == hit_type) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get micro block data failed", KR(ret), K(count), K(meta_block_id), K(micro_key), K(micro_meta_info), K(hit_type));
          } else {
            FLOG_INFO("[TEST] get micro block data success", KR(ret), K(count), K(meta_block_id), K(micro_key), K(micro_meta_info), K(hit_type));
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  FLOG_INFO("[TEST] get micro block data end", KR(ret), K(meta_block_id), K(count));

  return ret;
}

void ObPrereadMetaMacroTest::check_macro_exist_in_macro_cache(const int64_t tablet_id, int64_t &macro_in_macro_cache_cnt)
{
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  bool is_exist = false;
  const int64_t prewarm_into_micro_cache_threshold = 128 * 1024;
  FLOG_INFO("[TEST] check macro exist in macro cache", K(tablet_id));

  ObArray<blocksstable::MacroBlockId> block_ids;
  get_shared_blocks_for_tablet(block_ids);

  FLOG_INFO("[TEST] all macro blocks", K(block_ids));
  for (int i = 0; i < block_ids.count(); i++) {
    MacroBlockId data_block_id = block_ids.at(i);
    if (data_block_id.is_shared_data_or_meta()) {
      FLOG_INFO("[TEST] data block id", K(data_block_id));
      OK(macro_cache_mgr->exist(data_block_id, is_exist));
      if (is_exist) {
        macro_in_macro_cache_cnt++;
        FLOG_INFO("[TEST] exist data block in macro cache", K(data_block_id));
      }
    }
  }
  FLOG_INFO("[TEST] data block cnt in macro cache", K(macro_in_macro_cache_cnt));
}

void ObPrereadMetaMacroTest::get_shared_blocks_for_tablet(
    common::ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  bool is_old_version_empty = false;
  int64_t current_tablet_version = -1;
  int64_t current_tablet_trans_seq = -1;
  int64_t last_gc_version = -1;
  uintptr_t tablet_fingerprint = 0;
  bool is_transfer_out_deleted = false;
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr*)->get_current_version_for_tablet(run_ctx.ls_id_,
            run_ctx.tablet_id_, current_tablet_version, last_gc_version, current_tablet_trans_seq,
            tablet_fingerprint, is_old_version_empty, is_transfer_out_deleted));
  ASSERT_NE(-1, current_tablet_version);

  ObPrivateBlockGCHandler handler(run_ctx.ls_id_, run_ctx.ls_epoch_, run_ctx.tablet_id_,
                                  current_tablet_version, last_gc_version,
                                  current_tablet_trans_seq, tablet_fingerprint,
                                  is_transfer_out_deleted);
  ASSERT_EQ(OB_SUCCESS, handler.get_blocks_for_tablet(current_tablet_version, true/*is_shared*/, block_ids));
  LOG_INFO("get shared blocks for tablet", K(block_ids));
}

int ObPrereadMetaMacroTest::check_local_cache(
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
      FLOG_INFO("[TEST] check micro cache", K(macro_id), K(macro_reader.read_handles_[0].get_data_size()), K(expect_exist));
    } else if (ObSSPrewarmLocalCacheType::is_macro_cache(ss_prewarm_local_cache_type)) {
      bool is_hit = false;
      ObSSFdCacheHandle fd_handle;
      if(OB_FAIL(macro_cache_mgr->get(macro_id, run_ctx.tablet_id_, 0/*hit size*/, is_hit, fd_handle))) {
        LOG_WARN("fail to get macro cache", KR(ret), K(macro_id));
      } else {
        LOG_INFO("check macro cache", K(macro_id), K(is_hit), K(expect_exist));
        if (expect_exist != is_hit) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect_exist is not equal to is_hit", KR(ret), K(macro_id), K(expect_exist), K(is_hit));
        }
      }
      FLOG_INFO("[TEST] check macro cache", K(macro_id), K(is_hit), K(expect_exist));
    }
  }
  return ret;
}

int ObPrereadMetaMacroTest::check_micro_cache(
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

void ObPrereadMetaMacroTest::reset_read_info()
{
  read_buf_[0] = '\0';
  read_info_.io_desc_.set_wait_event(1);
  read_info_.buf_ = read_buf_;
  read_info_.offset_ = 0;
  read_info_.size_ = READ_IO_SIZE;
  read_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info_.mtl_tenant_id_ = run_ctx.tenant_id_;
  read_info_.set_effective_tablet_id(run_ctx.tablet_id_);
}

void ObPrereadMetaMacroTest::check_macro_exist_in_local_cache(const MacroBlockId &macro_block_id, const char *macro_buf, const int64_t macro_size)
{
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ObSSMemMacroCache *mem_macro_cache = MTL(ObSSMemMacroCache *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ASSERT_NE(nullptr, mem_macro_cache);
  bool is_exist = false;
  const int64_t prewarm_into_micro_cache_threshold = 128 * 1024;
  FLOG_INFO("[TEST] macro block id", K(macro_block_id));
  if (macro_block_id.is_shared_data_or_meta()) {
    FLOG_INFO("[TEST] object size", K(macro_block_id), K(macro_size));
    if (macro_size < prewarm_into_micro_cache_threshold) {
      ObSSMicroCacheHitType hit_type;
      OK(check_exist_in_micro_cache(macro_block_id, macro_buf, macro_size));
    } else {
      bool is_exist_in_macro_cache = false;
      bool is_exist_in_mem_macro_cache = false;
      ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->exist(macro_block_id, is_exist_in_macro_cache));
      ASSERT_EQ(OB_SUCCESS, mem_macro_cache->check_exist(macro_block_id, is_exist_in_mem_macro_cache));
      if (!(is_exist_in_macro_cache || is_exist_in_mem_macro_cache)) {
        FLOG_INFO("[TEST] macro is not exist", K(run_ctx), K(macro_block_id), K(macro_size), K(is_exist_in_macro_cache), K(is_exist_in_mem_macro_cache));
      }
      ASSERT_TRUE(is_exist_in_macro_cache || is_exist_in_mem_macro_cache);
    }
    FLOG_INFO("[TEST] check macro exist end", K(macro_block_id), K(is_exist), K(macro_size));
  }
}

void ObPrereadMetaMacroTest::get_macro_cnt_in_local_cache(const int64_t tablet_id, int64_t &in_macro_cache_cnt, int64_t &in_micro_cache_cnt)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  const int64_t prewarm_into_micro_cache_threshold = 128 * 1024;
  ObSEArray<MacroBlockId, 64> data_block_ids;
  ObSEArray<MacroBlockId, 64> meta_block_ids;

  data_block_ids.set_attr(ObMemAttr(run_ctx.tenant_id_, "DataBlockIDs"));
  meta_block_ids.set_attr(ObMemAttr(run_ctx.tenant_id_, "MetaBlockIDs"));

  OK(get_macro_blocks(run_ctx.ls_id_, run_ctx.tablet_id_, data_block_ids, meta_block_ids));
  sleep(20);
  for (int i = 0; i < data_block_ids.count(); i++) {
    MacroBlockId data_block_id = data_block_ids.at(i);
    int64_t object_size = 0;
    if (data_block_id.is_shared_data_or_meta()) {
      FLOG_INFO("[TEST] data block id in get_macro_cnt_in_local_cache", K(data_block_id));
      OK(OB_STORAGE_OBJECT_MGR.get_object_size(data_block_id, run_ctx.ls_epoch_, object_size));
      if (object_size > prewarm_into_micro_cache_threshold) {
        in_macro_cache_cnt++;
        FLOG_INFO("[TEST] should data block id in macro block", K(in_macro_cache_cnt), K(in_micro_cache_cnt), K(data_block_id), K(object_size));
      } else {
        in_micro_cache_cnt++;
        FLOG_INFO("[TEST] should data block id in micro block", K(in_macro_cache_cnt), K(in_micro_cache_cnt), K(data_block_id), K(object_size));
      }
      FLOG_INFO("[TEST] get data macro in local cache", K(in_macro_cache_cnt), K(in_micro_cache_cnt), K(data_block_id), K(object_size));
    }
  }
  for (int i = 0; i < meta_block_ids.count(); i++) {
    MacroBlockId meta_block_id = meta_block_ids.at(i);
    int64_t object_size = 0;
    if (meta_block_id.is_shared_data_or_meta()) {
      FLOG_INFO("[TEST] meta block id in get_macro_cnt_in_local_cache", K(meta_block_id));
      OK(OB_STORAGE_OBJECT_MGR.get_object_size(meta_block_id, run_ctx.ls_epoch_, object_size));
      if (object_size > prewarm_into_micro_cache_threshold) {
        in_macro_cache_cnt++;
        FLOG_INFO("[TEST] should meta block id in macro block", K(in_macro_cache_cnt), K(in_micro_cache_cnt), K(meta_block_id), K(object_size));
      } else {
        in_micro_cache_cnt++;
        FLOG_INFO("[TEST] should meta block id in micro block", K(in_macro_cache_cnt), K(in_micro_cache_cnt), K(meta_block_id), K(object_size));
      }
      FLOG_INFO("[TEST] get meta macro in local cache", K(in_macro_cache_cnt), K(in_micro_cache_cnt), K(meta_block_id), K(object_size));
    }
  }
}

void ObPrereadMetaMacroTest::wait_preread_task_finish()
{
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;

  while ((preread_cache_mgr.preread_queue_.size() != 0) ||
         (preread_task.segment_files_.count() != 0) ||
         (preread_task.async_read_list_.get_curr_total() != 0) ||
         (preread_task.free_list_.get_curr_total() != preread_task.max_pre_read_parallelism_)) {
          sleep(1);
          FLOG_INFO("[TEST] try to wait preread task finish", K(preread_cache_mgr.preread_queue_.size()),
            K(preread_task.segment_files_.count()), K(preread_task.async_read_list_.get_curr_total()),
            K(preread_task.free_list_.get_curr_total()));
  }
  FLOG_INFO("[TEST] wait preread task finish end", K(preread_task));
}

TEST_F(ObPrereadMetaMacroTest, test_preread_task)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx.tenant_id_));
  FLOG_INFO("[TEST] start test_preread_meta_macro", K(run_ctx));
  OK(sys_exe_sql("alter system set _ss_major_compaction_prewarm_level = 2 tenant tt1;"));
  exe_prepare_sql();
  // 1. create table and start medium compact

  OK(exe_sql("create table test_macro_cache (a bigint)"));
  set_ls_and_tablet_id_for_run_ctx("test_macro_cache");
  OK(exe_sql("insert into test_macro_cache select random(100) from table(generator(262144))"));
  sleep(1);
  OK(TestCompactionUtil::medium_compact(run_ctx.tenant_id_, run_ctx.tablet_id_.id(), run_ctx.ls_id_));
  FLOG_INFO("[TEST] finish medium compact (test_preread_task)", K(run_ctx.tablet_id_));
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  IGNORE_RETURN micro_cache->clear_micro_cache();

  // 2. get macro cache meta block ids
  ObArray<blocksstable::MacroBlockId> block_ids;
  get_shared_blocks_for_tablet(block_ids);

  FLOG_INFO("[TEST] preread data block begin");
  MacroBlockId data_block_id;
  ObStorageObjectHandle read_object_handle;

  // preread file to trigger preread task for data block
  for (int i = 0; i < block_ids.count(); i++) {
    FLOG_INFO("[TEST] data block id", K(block_ids.at(i)), K(i));
    if (block_ids.at(i).is_shared_data_or_meta()) {
      // 2. read to trigger macro cache miss
      read_object_handle.reset();
      reset_read_info();
      data_block_id.reset();
      data_block_id = block_ids.at(i);
      read_info_.macro_block_id_ = data_block_id;
      ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_pread_file(read_info_, read_object_handle));
      ASSERT_EQ(OB_SUCCESS, read_object_handle.wait());
      FLOG_INFO("[TEST] finish async pread file", K(read_info_), K(data_block_id), K(read_object_handle.get_data_size()));
      ASSERT_NE(nullptr, read_object_handle.get_buffer());
      ASSERT_GE(read_object_handle.get_data_size(), 0);
      wait_preread_task_finish();
      FLOG_INFO("[TEST] finish wait preread task", K(data_block_id));
      check_macro_exist_in_local_cache(data_block_id, read_object_handle.get_buffer(), read_object_handle.get_data_size());
    }
  }
  FLOG_INFO("[TEST] finish test_preread_meta_macro", K(run_ctx));
}


TEST_F(ObPrereadMetaMacroTest, test_minor_compact_prewarm)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx.tenant_id_));
  FLOG_INFO("[TEST] start test_minor_compact_prewarm", K(run_ctx));
  exe_prepare_sql();
  OK(sys_exe_sql("alter system set _ss_major_compaction_prewarm_level = 0 tenant tt1;"));
  sleep(60);
  OK(exe_sql("create table test_minor_compact_prewarm (c1 varchar(1024), c2 varchar(1024), c3 varchar(1024), c4 varchar(1024))"));
  set_ls_and_tablet_id_for_run_ctx("test_minor_compact_prewarm");
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  preread_task.is_inited_ = false;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  IGNORE_RETURN micro_cache->clear_micro_cache();

  ObSSMicroCachePrewarmStat &before_prewarm_stat = micro_cache->cache_stat_.prewarm_stat_;
  int64_t before_prewarm_add_cnt = 0;
  before_prewarm_stat.get_prewarm_add_cnt(ObSSMicroCacheAccessType::MINOR_COMPACTION_PREWARM_TYPE, before_prewarm_add_cnt);
  ASSERT_EQ(0, before_prewarm_add_cnt);
  FLOG_INFO("[TEST] before prewarm add cnt", K(before_prewarm_add_cnt));

  // trigger minor compaction by minor freeze 3 times. it prewarms minor sstable into local
  // macro cache, and update tablet table store.

  OK(exe_sql("insert into test_minor_compact_prewarm select randstr(1024, random()) c1, randstr(1024, random()) c2, randstr(1024, random()) c3, randstr(1024, random()) c4 from table(generator(10000));"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_minor_compact_prewarm select randstr(1024, random()) c1, randstr(1024, random()) c2, randstr(1024, random()) c3, randstr(1024, random()) c4 from table(generator(10000));"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  OK(exe_sql("insert into test_minor_compact_prewarm select randstr(1024, random()) c1, randstr(1024, random()) c2, randstr(1024, random()) c3, randstr(1024, random()) c4 from table(generator(10000));"));
  sleep(1);
  OK(exe_sql("alter system minor freeze;"));
  wait_minor_finish();

  sleep(20);
  wait_ss_minor_compaction_finish();
  FLOG_INFO("[TEST] finish ss minor compaction");
  ObSSMicroCachePrewarmStat &after_prewarm_stat = micro_cache->cache_stat_.prewarm_stat_;
  int64_t after_prewarm_add_cnt = 0;
  after_prewarm_stat.get_prewarm_add_cnt(ObSSMicroCacheAccessType::MINOR_COMPACTION_PREWARM_TYPE, after_prewarm_add_cnt);

  ASSERT_GT(after_prewarm_add_cnt, 0);
  FLOG_INFO("[TEST] finish check micro cache prewarm add cnt", K(after_prewarm_add_cnt));

  int64_t macro_in_macro_cache_cnt = 0;
  check_macro_exist_in_macro_cache(run_ctx.tablet_id_.id(), macro_in_macro_cache_cnt);
  ASSERT_GT(macro_in_macro_cache_cnt, 0);
  int64_t should_in_macro_cache_cnt = 0;
  int64_t should_in_micro_cache_cnt = 0;
  get_macro_cnt_in_local_cache(run_ctx.tablet_id_.id(), should_in_macro_cache_cnt, should_in_micro_cache_cnt);
  ASSERT_GT(should_in_macro_cache_cnt, 0);
  ASSERT_EQ(should_in_macro_cache_cnt, macro_in_macro_cache_cnt);
  ObArray<blocksstable::MacroBlockId> block_ids;
  get_shared_blocks_for_tablet(block_ids);
  for (int64_t i = 0; (i < block_ids.count()); ++i) {
    if (block_ids.at(i).is_shared_data_or_meta()) {
      check_local_cache(block_ids.at(i), true/*expect_exist*/);
    }
  }
  FLOG_INFO("[TEST] finish test_minor_compact_prewarm", K(run_ctx), K(macro_in_macro_cache_cnt), K(should_in_macro_cache_cnt));
}


TEST_F(ObPrereadMetaMacroTest, test_storage_cache_policy_prewarm)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx.tenant_id_));
  exe_prepare_sql();
  FLOG_INFO("[TEST] start test_storage_cache_policy_prewarm", K(run_ctx));
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  preread_task.is_inited_ = false;
  ObStorageCachePolicyPrewarmer prewarmer;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);

  OK(exe_sql("create table test_storage_cache_policy_prewarm (a bigint)"));
  set_ls_and_tablet_id_for_run_ctx("test_storage_cache_policy_prewarm");

  OK(exe_sql("insert into test_storage_cache_policy_prewarm select random(100) from table(generator(262144));"));
  sleep(1);
  OK(TestCompactionUtil::medium_compact(run_ctx.tenant_id_, run_ctx.tablet_id_.id(), run_ctx.ls_id_));
  FLOG_INFO("[TEST] finish medium compact (test_storage_cache_policy_prewarm)", K(run_ctx.tablet_id_));
  // 1.test basic hot retention prewarm
  ObStorageCacheTabletTask *task = static_cast<ObStorageCacheTabletTask *>(ob_malloc(
      sizeof(ObStorageCacheTabletTask),
      ObMemAttr(run_ctx.tenant_id_, "ObStorageCache")));
  ASSERT_NE(nullptr, task);
  new (task) ObStorageCacheTabletTask();
  PolicyStatus policy_status = PolicyStatus::HOT;
  OK(task->init(
      run_ctx.tenant_id_,
      run_ctx.ls_id_.id(),
      run_ctx.tablet_id_.id(),
      policy_status));
  task->inc_ref_count();
  OK(task->set_status(ObStorageCacheTaskStatus::OB_STORAGE_CACHE_TASK_DOING));

  // read init major
  IGNORE_RETURN micro_cache->clear_micro_cache();
  OK(prewarmer.prewarm_hot_tablet(task));
  ObSSMicroCachePrewarmStat &prewarm_stat = micro_cache->cache_stat_.prewarm_stat_;
  int64_t prewarm_add_cnt = 0;
  prewarm_stat.get_prewarm_add_cnt(ObSSMicroCacheAccessType::STORAGE_CACHE_POLICY_PREWARM_TYPE, prewarm_add_cnt);
  ASSERT_GT(prewarm_add_cnt, 0);
  FLOG_INFO("[TEST] check micro cache prewarm add cnt", K(prewarm_add_cnt));
  int64_t macro_in_macro_cache_cnt = 0;
  check_macro_exist_in_macro_cache(run_ctx.tablet_id_.id(), macro_in_macro_cache_cnt);
  ASSERT_GT(macro_in_macro_cache_cnt, 0);
  int64_t should_in_macro_cache_cnt = 0;
  int64_t should_in_micro_cache_cnt = 0;
  get_macro_cnt_in_local_cache(run_ctx.tablet_id_.id(), should_in_macro_cache_cnt, should_in_micro_cache_cnt);
  ASSERT_GT(should_in_macro_cache_cnt, 0);
  ASSERT_EQ(should_in_macro_cache_cnt, macro_in_macro_cache_cnt);

  ObArray<blocksstable::MacroBlockId> block_ids;
  get_shared_blocks_for_tablet(block_ids);
  for (int64_t i = 0; (i < block_ids.count()); ++i) {
    if (block_ids.at(i).is_shared_data_or_meta()) {
      check_local_cache(block_ids.at(i), true/*expect_exist*/);
    }
  }
  FLOG_INFO("[TEST] finish test_storage_cache_policy_prewarm", K(prewarm_add_cnt), K(macro_in_macro_cache_cnt), K(should_in_macro_cache_cnt));
}

TEST_F(ObPrereadMetaMacroTest, test_major_compaction_prewarm)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx.tenant_id_));
  OK(sys_exe_sql("alter system set _enable_adaptive_compaction = false tenant tt1;"));
  exe_prepare_sql();
  FLOG_INFO("[TEST] start test_major_compaction_prewarm", K(run_ctx));
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ASSERT_NE(nullptr, file_manager);
  ObPrereadCacheManager &preread_cache_mgr = file_manager->preread_cache_mgr_;
  ObSSPreReadTask &preread_task = preread_cache_mgr.preread_task_;
  preread_task.is_inited_ = false;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  IGNORE_RETURN micro_cache->clear_micro_cache();

  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);

  OK(exe_sql("create table test_major_compaction_prewarm (a bigint) storage_cache_policy = (global = 'hot')"));
  set_ls_and_tablet_id_for_run_ctx("test_major_compaction_prewarm");
  OK(exe_sql("insert into test_major_compaction_prewarm select random(100) from table(generator(262144))"));

  // wait policy refresh finish before major compaction, to ensure the tablet is HOT.
  OK(TestCompactionUtil::wait_policy_refresh_finish(run_ctx.tenant_id_, run_ctx.tablet_id_.id(), "HOT"/*storage_cache_policy*/));
  OK(TestCompactionUtil::medium_compact(run_ctx.tenant_id_, run_ctx.tablet_id_.id(), run_ctx.ls_id_));
  FLOG_INFO("[TEST] finish medium compact (test_major_compaction_prewarm)", K(run_ctx.tablet_id_));
  ObSSMicroCachePrewarmStat &after_prewarm_stat = micro_cache->cache_stat_.prewarm_stat_;
  int64_t after_prewarm_add_cnt = 0;
  after_prewarm_stat.get_prewarm_add_cnt(ObSSMicroCacheAccessType::MAJOR_COMPACTION_PREWARM_TYPE, after_prewarm_add_cnt);
  ASSERT_GT(after_prewarm_add_cnt, 0);
  FLOG_INFO("[TEST] finish check micro cache prewarm add cnt in major compaction", K(after_prewarm_add_cnt));

  int64_t macro_in_macro_cache_cnt = 0;
  check_macro_exist_in_macro_cache(run_ctx.tablet_id_.id(), macro_in_macro_cache_cnt);
  ASSERT_GT(macro_in_macro_cache_cnt, 0);
  int64_t should_in_macro_cache_cnt = 0;
  int64_t should_in_micro_cache_cnt = 0;
  get_macro_cnt_in_local_cache(run_ctx.tablet_id_.id(), should_in_macro_cache_cnt, should_in_micro_cache_cnt);
  ASSERT_GT(should_in_macro_cache_cnt, 0);
  ASSERT_EQ(should_in_macro_cache_cnt, macro_in_macro_cache_cnt);
  ASSERT_GT(should_in_micro_cache_cnt, 0);

  ObArray<blocksstable::MacroBlockId> block_ids;
  get_shared_blocks_for_tablet(block_ids);
  for (int64_t i = 0; (i < block_ids.count()); ++i) {
    if (block_ids.at(i).is_shared_data_or_meta()) {
      check_local_cache(block_ids.at(i), true/*expect_exist*/);
    }
  }
  FLOG_INFO("[TEST] finish test_major_compaction_prewarm", K(run_ctx), K(macro_in_macro_cache_cnt), K(should_in_macro_cache_cnt), K(should_in_micro_cache_cnt));
}
TEST_F(ObPrereadMetaMacroTest, end)
{
  if (run_ctx.time_sec_ > 0) {
    ::sleep(run_ctx.time_sec_);
  }
}
} // end unittest
} // end oceanbase


int main(int argc, char **argv)
{
  int64_t c = 0;
  int64_t time_sec = 0;
  char *log_level = (char*)"INFO";
  char buf[1000];
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  memset(buf, 1000, sizeof(buf));
  databuff_printf(buf, sizeof(buf), "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT, oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  while(EOF != (c = getopt(argc,argv,"t:l:"))) {
    switch(c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
     log_level = optarg;
     oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
     break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");

  LOG_INFO("main>>>");
  oceanbase::unittest::run_ctx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}