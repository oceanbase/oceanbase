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

#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#define UNITTEST

#include "storage/tx_storage/ob_ls_service.h"
#include "close_modules/shared_storage/storage/incremental/garbage_collector/ob_ss_garbage_collector.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "close_modules/shared_storage/storage/incremental/ob_shared_meta_service.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"
#include "mittest/shared_storage/simple_server/test_gc_util.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_dir_manager.h"

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
char *shared_storage_info = NULL;
namespace unittest
{

class ObCheckDirEmptOp : public ObBaseDirEntryOperator
{
public:
  ObCheckDirEmptOp() : file_cnt_(0) {}
  ~ObCheckDirEmptOp() {}
  void reset() { file_cnt_ = 0; }
  int func(const dirent *entry) override;
  int64_t get_file_cnt() {return file_cnt_;}
  TO_STRING_KV(K_(file_cnt));
private:
  int64_t file_cnt_;
};

int ObCheckDirEmptOp::func(const dirent *entry)
{
  if (OB_ISNULL(entry->d_name)) {
    SERVER_LOG(INFO, "d_name is null");
  } else {
    SERVER_LOG(INFO, "dir_entry", K(entry->d_name));
  }
  file_cnt_++;
  return OB_ERR_EXIST_OBJECT;
}

class ObSharedStorageSharedDirPrivateGC : public ObSimpleClusterTestBase
{
public:
  static const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObSharedStorageSharedDirPrivateGC()
    : ObSimpleClusterTestBase("test_inc_shared_storage_shared_dir_private_gc", "50G", "50G", "50G")
  {}
  void wait_upload_sstable(const int64_t ss_checkpoint_scn);
  void wait_tablet_gc_finish();
  void wait_shared_tablet_gc_finish();
  void get_ss_checkpoint_scn(int64_t &ss_checkpoint_scn);
  void set_ls_and_tablet_id_for_run_ctx();

  void wait_sys_to_leader()
  {
    share::ObTenantSwitchGuard tenant_guard;
    int ret = OB_ERR_UNEXPECTED;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(1));
    ObLS *ls = nullptr;
    ObLSID ls_id(ObLSID::SYS_LS_ID);
    ObLSHandle handle;
    ObLSService *ls_svr = MTL(ObLSService *);
    EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ls = handle.get_ls();
    ASSERT_NE(nullptr, ls);
    ASSERT_EQ(ls_id, ls->get_ls_id());
    for (int i=0; i<100; i++) {
      ObRole role;
      int64_t proposal_id = 0;
      ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
      if (role == ObRole::LEADER) {
        ret = OB_SUCCESS;
        break;
      }
      ob_usleep(10 * 1000);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  void get_major_sstable_root_seq(int64_t &root_macro_seq);

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }
};

void ObSharedStorageSharedDirPrivateGC::wait_upload_sstable(const int64_t ss_checkpoint_scn)
{
  int64_t new_ss_checkpoint_scn = 0;
  while (new_ss_checkpoint_scn <= ss_checkpoint_scn) {
    get_ss_checkpoint_scn(new_ss_checkpoint_scn);
    LOG_INFO("wait upload sstable", K(RunCtx.tenant_epoch_), K(RunCtx.ls_id_), K(RunCtx.ls_epoch_), K(RunCtx.tablet_id_), K(new_ss_checkpoint_scn), K(ss_checkpoint_scn));
    usleep(100 * 1000);
  }
}

void ObSharedStorageSharedDirPrivateGC::get_ss_checkpoint_scn(int64_t &ss_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  ss_checkpoint_scn = 0;
  ObSqlString sql;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ss_checkpoint_scn from oceanbase.__all_virtual_ss_ls_meta where tenant_id=%lu and ls_id=%ld;",
        RunCtx.tenant_id_, RunCtx.ls_id_.id()));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ss_checkpoint_scn", ss_checkpoint_scn));
  }
}

void ObSharedStorageSharedDirPrivateGC::set_ls_and_tablet_id_for_run_ctx()
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t uid = 0;
  int64_t id = 0;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, sql.assign("select tablet_id from oceanbase.__all_virtual_table where table_name='test_table';"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_uint("tablet_id", uid));
  }
  RunCtx.tablet_id_ = uid;

  sql.reset();
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ls_id from oceanbase.__all_tablet_to_ls where tablet_id=%ld;", uid));
  SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res2, sql.ptr()));
    sqlclient::ObMySQLResult *result = res2.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", id));
  }
  ObLSID ls_id(id);

  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::TXSTORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  RunCtx.ls_id_ = ls->get_ls_id();
  RunCtx.ls_epoch_ = ls->get_ls_epoch();
  RunCtx.tenant_epoch_ = MTL_EPOCH_ID();
  RunCtx.ls_ = ls;
  LOG_INFO("finish set run ctx", K(RunCtx.tenant_epoch_), K(RunCtx.ls_id_), K(RunCtx.ls_epoch_), K(RunCtx.tablet_id_));
}

TEST_F(ObSharedStorageSharedDirPrivateGC, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

#define EXE_SQL(sql_str)                                            \
  LOG_INFO("exe sql start", K(sql_str));      \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows)); \
  LOG_INFO("exe sql end", K(sql_str));

#define SYS_EXE_SQL(sql_str)                                            \
  LOG_INFO("sys exe sql start", K(sql_str)); \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write(sql.ptr(), affected_rows));  \
  LOG_INFO("sys exe sql end", K(sql_str));

TEST_F(ObSharedStorageSharedDirPrivateGC, add_tenant)
{
    TRANS_LOG(INFO, "create_tenant start");
    wait_sys_to_leader();
    int ret = OB_SUCCESS;
    int retry_cnt = 0;
    do {
      if (OB_FAIL(create_tenant("tt1"))) {
        TRANS_LOG(WARN, "create_tenant fail, need retry", K(ret), K(retry_cnt));
        ob_usleep(15 * 1000 * 1000); // 15s
      }
      retry_cnt++;
    } while (OB_FAIL(ret) && retry_cnt < 10);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
    ObSqlString sql;
    int64_t affected_rows = 0;
    SYS_EXE_SQL("alter system set _ss_advance_checkpoint_interval = '1m' tenant tt1;");
    SYS_EXE_SQL("alter system set_tp tp_name = EN_COMPACTION_SS_MINOR_MERGE_FAST_SKIP,error_code = 4016,frequency = 1;");
}

TEST_F(ObSharedStorageSharedDirPrivateGC, test_shared_dir_private)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t tablet_version1 = 0;
  int64_t tablet_version2 = 0;
  int64_t tablet_version3 = 0;
  EXE_SQL("create table test_table (a int) storage_cache_policy (global = 'hot');");
  sleep(32);
  LOG_INFO("create_table finish");

  set_ls_and_tablet_id_for_run_ctx();

  share::SCN ss_checkpoint_scn;
  share::SCN gc_start;
  share::SCN gc_end;
  share::SCN snapshot;

  EXE_SQL("alter system set _ss_schedule_upload_interval = '1s';");
  EXE_SQL("alter system set inc_sstable_upload_thread_score = 20;");
  EXE_SQL("alter system set _ss_garbage_collect_interval = '10s';");
  EXE_SQL("alter system set _ss_garbage_collect_file_expiration_time = '10s';");
  //EXE_SQL("alter system set _ss_enable_timeout_garbage_collection = true;");
  EXE_SQL("alter system set _ss_tablet_version_retention_time = '10s';");

  sleep(5);
  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish");

  EXE_SQL("alter system minor freeze;");
  RunCtx.ls_->get_end_scn(ss_checkpoint_scn);
  wait_minor_finish(get_curr_simple_server().get_sql_proxy2());
  wait_upload_sstable(ss_checkpoint_scn.get_val_for_tx());


  ObSSTableGCInfo gc_info;
  gc_info.scn_range_.end_scn_.convert_for_sql(1000);
  gc_info.parallel_cnt_ = 3;
  gc_info.seq_step_ = 1000;
  gc_info.data_seq_bits_ = 32;
  update_sslog(sslog::ObSSLogMetaType::SSLOG_FLUSH_MINI_SSTABLE, 100, ObAtomicMetaInfo::State::INIT, &gc_info);
  gc_info.scn_range_.end_scn_.convert_for_sql(1001);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_FLUSH_MINI_SSTABLE, 101, ObAtomicMetaInfo::State::INIT, &gc_info);
  gc_info.scn_range_.end_scn_.convert_for_sql(1002);
  update_sslog(sslog::ObSSLogMetaType::SSLOG_FLUSH_MINI_SSTABLE, 102, ObAtomicMetaInfo::State::INIT, &gc_info);

  sleep(5);

  MacroBlockId block_01;
  MacroBlockId block_02;
  MacroBlockId block_03;
  MacroBlockId block_04;
  MacroBlockId block_11;
  MacroBlockId block_12;
  MacroBlockId block_13;
  MacroBlockId block_14;
  MacroBlockId block_21;
  MacroBlockId block_22;
  MacroBlockId block_23;
  MacroBlockId block_24;
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO, 1, block_01);
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO, 2, block_02);
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO, 3, block_03);
  gen_block_id(100, ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO, 4, block_04);
  gen_block_id(101, ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO, 11, block_11);
  gen_block_id(101, ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO, 12, block_12);
  gen_block_id(101, ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO, 13, block_13);
  gen_block_id(101, ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO, 14, block_14);
  gen_block_id(102, ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO, 21, block_21);
  gen_block_id(102, ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO, 22, block_22);
  gen_block_id(102, ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO, 23, block_23);
  gen_block_id(102, ObStorageObjectType::SHARED_MINI_V2_DATA_MACRO, 24, block_24);

  block_01.set_source_type(SSMiniSourceType::MINI_FLUSH);
  block_02.set_source_type(SSMiniSourceType::MINI_FLUSH);
  block_03.set_source_type(SSMiniSourceType::MINI_FLUSH);
  block_04.set_source_type(SSMiniSourceType::MINI_FLUSH);
  block_11.set_source_type(SSMiniSourceType::MINI_FLUSH);
  block_12.set_source_type(SSMiniSourceType::MINI_FLUSH);
  block_13.set_source_type(SSMiniSourceType::MINI_FLUSH);
  block_14.set_source_type(SSMiniSourceType::MINI_FLUSH);
  block_21.set_source_type(SSMiniSourceType::MINI_FLUSH);
  block_22.set_source_type(SSMiniSourceType::MINI_FLUSH);
  block_23.set_source_type(SSMiniSourceType::MINI_FLUSH);
  block_24.set_source_type(SSMiniSourceType::MINI_FLUSH);

  write_block(block_01);
  write_block(block_02);
  write_block(block_03);
  write_block(block_04);
  write_block(block_11);
  write_block(block_12);
  write_block(block_13);
  write_block(block_14);
  write_block(block_21);
  write_block(block_22);
  write_block(block_23);
  write_block(block_24);

  BlockIDSet block_id_in_tablet_meta;
  ASSERT_EQ(OB_SUCCESS, block_id_in_tablet_meta.create(128, "BlkIdSetBkt", "BlkIdSetNode", MTL_ID()));

  ASSERT_EQ(OB_SUCCESS, block_id_in_tablet_meta.set_refactored(block_02));
  ASSERT_EQ(OB_SUCCESS, block_id_in_tablet_meta.set_refactored(block_04));
  ASSERT_EQ(OB_SUCCESS, block_id_in_tablet_meta.set_refactored(block_12));
  ASSERT_EQ(OB_SUCCESS, block_id_in_tablet_meta.set_refactored(block_14));
  ASSERT_EQ(OB_SUCCESS, block_id_in_tablet_meta.set_refactored(block_22));
  ASSERT_EQ(OB_SUCCESS, block_id_in_tablet_meta.set_refactored(block_24));


  share::SCN checkpoint_scn;
  SCN curr_scn;
  share::SCN private_block_succ_gc_scn;
  uint64_t retry_op_id;
  share::ObScnRange scn_range;
  int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  ASSERT_EQ(OB_SUCCESS, curr_scn.convert_for_tx(cur_time_ns));
  scn_range.start_scn_ = SCN::min_scn();
  scn_range.end_scn_ = curr_scn;

  ASSERT_EQ(OB_SUCCESS, checkpoint_scn.convert_for_tx(1001));
  ASSERT_EQ(OB_SUCCESS, ObSSGarbageCollector::private_block_gc_(
        ObSSPreciseGCTablet(RunCtx.ls_id_, RunCtx.tablet_id_, SCN::min_scn()),
        PrivateGCInfo(scn_range, 101), checkpoint_scn, block_id_in_tablet_meta,
        private_block_succ_gc_scn, retry_op_id));
  ASSERT_EQ(101, retry_op_id);

  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_01, 0, is_exist));
  ASSERT_FALSE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_02, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_03, 0, is_exist));
  ASSERT_FALSE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_04, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_11, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_12, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_13, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_14, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_21, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_22, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_23, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_24, 0, is_exist));
  ASSERT_TRUE(is_exist);

  cur_time_ns = ObTimeUtility::current_time_ns();
  scn_range.start_scn_ = SCN::min_scn();
  scn_range.end_scn_ = curr_scn;

  ASSERT_EQ(OB_SUCCESS, curr_scn.convert_for_tx(cur_time_ns));
  ASSERT_EQ(OB_SUCCESS, checkpoint_scn.convert_for_tx(1001));
  ASSERT_EQ(OB_SUCCESS, ObSSGarbageCollector::private_block_gc_(
        ObSSPreciseGCTablet(RunCtx.ls_id_, RunCtx.tablet_id_, SCN::min_scn()),
        PrivateGCInfo(scn_range, 102), checkpoint_scn, block_id_in_tablet_meta,
        private_block_succ_gc_scn, retry_op_id));
  ASSERT_EQ(UINT64_MAX, retry_op_id);

  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_01, 0, is_exist));
  ASSERT_FALSE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_02, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_03, 0, is_exist));
  ASSERT_FALSE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_04, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_11, 0, is_exist));
  ASSERT_FALSE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_12, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_13, 0, is_exist));
  ASSERT_FALSE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_14, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_21, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_22, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_23, 0, is_exist));
  ASSERT_TRUE(is_exist);
  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(block_24, 0, is_exist));
  ASSERT_TRUE(is_exist);

  EXE_SQL("drop table test_table;");
  EXE_SQL("purge recyclebin;");
}

TEST_F(ObSharedStorageSharedDirPrivateGC, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}

void ObSharedStorageSharedDirPrivateGC::wait_shared_tablet_gc_finish()
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  ObCheckDirEmptOp shared_macro_op;
  ObMemAttr mem_attr(MTL_ID(), "test_gc");
  ObArenaAllocator allocator(mem_attr);
  do {
    shared_macro_op.reset();
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_tablet_dir(dir_path, sizeof(dir_path), RunCtx.tablet_id_.id()));
    ret = MTL(ObTenantFileManager*)->list_remote_files(dir_path, shared_macro_op);
    LOG_INFO("shared tablet dir", K(dir_path), K(ret), K(shared_macro_op));
    usleep(100 * 1000);
  } while (0 != shared_macro_op.get_file_cnt());

  do {
	ObTabletHandle ss_tablet;
    ret = MTL(ObSSMetaService*)->get_tablet(RunCtx.ls_id_, RunCtx.tablet_id_, share::SCN::min_scn(), allocator, ss_tablet);
    LOG_INFO("wait tablet meta", K(dir_path), K(ret));
  } while (OB_SUCCESS == ret);
}

void ObSharedStorageSharedDirPrivateGC::get_major_sstable_root_seq(
    int64_t &root_macro_seq)
{
  int ret = OB_SUCCESS;
  ObTabletHandle ss_tablet;
  ObArenaAllocator allocator;
  const ObTabletTableStore *table_store = nullptr;
  ObTableStoreIterator table_store_iter(false/*reverse*/, false/*need_load_sstable*/);
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  ObTablet *tablet = NULL;

  while (OB_SUCCESS != ret || !ss_tablet.is_valid()) {
    if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(RunCtx.ls_id_, RunCtx.tablet_id_,
            share::SCN::min_scn(), allocator, ss_tablet))) {
      LOG_WARN("failed to get_tablet", K(ret));
    }
  }

  if (FALSE_IT(tablet = ss_tablet.get_obj())) {
  } else if (OB_FAIL(tablet->fetch_table_store(wrapper))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      LOG_INFO("table_store of this tablet has been recycled, skip it", KR(ret));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed fetch table store", K(ret), KPC(tablet));
    }
  } else if (OB_ISNULL(table_store = wrapper.get_member())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet table store should not be NULL", K(ret), KPC(tablet));
  } else if (OB_FAIL(table_store->get_major_sstables(table_store_iter))) {
    LOG_WARN("failed to get mds sstables", K(ret), K(tablet), KPC(table_store));
  // } else if (1 != table_store_iter.count() && 2 != table_store_iter.count()) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WARN("table_store_iter is unexpected", K(ret), K(table_store_iter.count()));
  } else {
    ObSSTableMetaHandle sst_meta_hdl;
    ObITable *table = NULL;
    ObSSTable *sstable = nullptr;
    if (OB_FAIL(table_store_iter.get_boundary_table(true, table))) {
      LOG_WARN("failed to get table handle", K(ret), K(tablet));
    } else if (OB_ISNULL(sstable = static_cast<ObSSTable *>(table))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable is NULL", K(ret));
    } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
      LOG_WARN("fail to get sstable meta handle", K(ret));
    } else {
      root_macro_seq = const_cast<ObSSTableMeta*>(sst_meta_hdl.meta_)->basic_meta_.root_macro_seq_;
      LOG_WARN("get_major_sstable_root_seq finish", K(root_macro_seq));
    }
  }
}

void ObSharedStorageSharedDirPrivateGC::wait_tablet_gc_finish()
{
  bool is_exist = false;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  do {
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_data_tablet_id_dir(dir_path, sizeof(dir_path), RunCtx.tenant_id_, RunCtx.tenant_epoch_, RunCtx.tablet_id_.id()));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(dir_path, is_exist));
    LOG_INFO("private tablet_data dir exist", K(dir_path));
    usleep(100 *1000);
  } while (is_exist);

  memset(dir_path, 0, sizeof(dir_path));
  do {
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_meta_tablet_id_dir(dir_path, sizeof(dir_path), RunCtx.tenant_id_, RunCtx.tenant_epoch_, RunCtx.ls_id_.id(), RunCtx.ls_epoch_, RunCtx.tablet_id_.id()));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(dir_path, is_exist));
    usleep(100 * 1000);
    LOG_INFO("private tablet_meta dir exist", K(dir_path));
  } while (is_exist);
}

}
}

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
  oceanbase::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
