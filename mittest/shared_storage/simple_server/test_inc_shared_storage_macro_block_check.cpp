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
#include "close_modules/shared_storage/storage/incremental/garbage_collector/ob_shared_block_check.h"
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

class ObSharedStorageBlockCheckTest : public ObSimpleClusterTestBase
{
public:
  static const int64_t WRITE_IO_SIZE = DIO_READ_ALIGN_SIZE * 256; // 1MB
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObSharedStorageBlockCheckTest()
    : ObSimpleClusterTestBase("test_inc_shared_storage_macro_block_check", "50G", "50G", "50G")
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

void ObSharedStorageBlockCheckTest::wait_upload_sstable(const int64_t ss_checkpoint_scn)
{
  int64_t new_ss_checkpoint_scn = 0;
  while (new_ss_checkpoint_scn <= ss_checkpoint_scn) {
    get_ss_checkpoint_scn(new_ss_checkpoint_scn);
    LOG_INFO("wait upload sstable", K(RunCtx.tenant_epoch_), K(RunCtx.ls_id_), K(RunCtx.ls_epoch_), K(RunCtx.tablet_id_), K(new_ss_checkpoint_scn), K(ss_checkpoint_scn));
    usleep(100 * 1000);
  }
}

void ObSharedStorageBlockCheckTest::get_ss_checkpoint_scn(int64_t &ss_checkpoint_scn)
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

void ObSharedStorageBlockCheckTest::set_ls_and_tablet_id_for_run_ctx()
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

TEST_F(ObSharedStorageBlockCheckTest, observer_start)
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

TEST_F(ObSharedStorageBlockCheckTest, add_tenant)
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
}

TEST_F(ObSharedStorageBlockCheckTest, test_inc_sstable)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t tablet_version1 = 0;
  int64_t tablet_version2 = 0;
  int64_t tablet_version3 = 0;
  EXE_SQL("create table test_table (a int)");
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
  EXE_SQL("alter system set _ss_enable_timeout_garbage_collection = true;");

  sleep(5);
  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish");

  EXE_SQL("alter system minor freeze;");
  RunCtx.ls_->get_end_scn(ss_checkpoint_scn);
  wait_minor_finish(get_curr_simple_server().get_sql_proxy2());
  wait_upload_sstable(ss_checkpoint_scn.get_val_for_tx());


  ObSSTableGCInfo unused_gc_info;
  unused_gc_info.parallel_cnt_ = 3;
  unused_gc_info.seq_step_ = 1000;
  unused_gc_info.data_seq_bits_ = 32;
  update_sslog(sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, 100, ObAtomicMetaInfo::State::INIT, &unused_gc_info);

  sleep(5);

  MacroBlockId not_exist_mini_block_id;
  gen_block_id(10, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, not_exist_mini_block_id, 0);

  MacroBlockId exist_mini_block_id;
  gen_block_id(1000, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, exist_mini_block_id, 0);

  write_block(not_exist_mini_block_id);
  write_block(exist_mini_block_id);

  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  SCN multi_version_start;
  ASSERT_EQ(OB_SUCCESS, multi_version_start.convert_for_tx(cur_time_ns));
  int unuse1 = 0;
  int64_t unuse2 = 0;
  storage::SharedBlockCheckTask test_task(unuse1, unuse2);
  storage::ObSSPreciseGCTablet tablet_info(RunCtx.ls_id_, RunCtx.tablet_id_, SCN::min_scn());

  test_task.block_check_for_tablet_(tablet_info, multi_version_start);

  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(not_exist_mini_block_id, 0, is_exist));
  ASSERT_FALSE(is_exist);

  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(exist_mini_block_id, 0, is_exist));
  ASSERT_TRUE(is_exist);

  EXE_SQL("drop table test_table;");
  EXE_SQL("purge recyclebin;");
}

TEST_F(ObSharedStorageBlockCheckTest, test_major_sstable)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t tablet_version1 = 0;
  int64_t tablet_version2 = 0;
  int64_t tablet_version3 = 0;
  EXE_SQL("create table test_table (a int)");
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
  EXE_SQL("alter system set _ss_enable_timeout_garbage_collection = true;");

  int64_t not_exist_block_seq = 0;

  sleep(5);
  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish");
  sleep(5);

  SYS_EXE_SQL("alter system set ob_compaction_schedule_interval = '3s' tenant tt1;");
  EXE_SQL("alter system major freeze;");

  wait_major_finish(get_curr_simple_server().get_sql_proxy());
  sleep(5);
  get_major_sstable_root_seq(not_exist_block_seq);
  not_exist_block_seq--;

  int64_t exist_block_seq = 0;

  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish");

  SYS_EXE_SQL("alter system set ob_compaction_schedule_interval = '3s' tenant tt1;");
  EXE_SQL("alter system major freeze;");

  wait_major_finish(get_curr_simple_server().get_sql_proxy());
  sleep(5);
  get_major_sstable_root_seq(exist_block_seq);
  exist_block_seq += 1000000000000;

  LOG_INFO("block_seq", K(not_exist_block_seq), K(exist_block_seq));

  MacroBlockId not_exist_block_id;
  gen_block_id(100 /* unused */, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, not_exist_block_seq, not_exist_block_id, 0);
  MacroBlockId exist_block_id;
  gen_block_id(100 /* unused */, ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, exist_block_seq, exist_block_id, 0);

  write_block(not_exist_block_id);
  write_block(exist_block_id);

  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  SCN multi_version_start;
  ASSERT_EQ(OB_SUCCESS, multi_version_start.convert_for_tx(cur_time_ns));
  int unuse1 = 0;
  int64_t unuse2 = 0;
  storage::SharedBlockCheckTask test_task(unuse1, unuse2);
  storage::ObSSPreciseGCTablet tablet_info(RunCtx.ls_id_, RunCtx.tablet_id_, SCN::min_scn());

  test_task.block_check_for_tablet_(tablet_info, multi_version_start);

  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(not_exist_block_id, 0, is_exist));
  ASSERT_FALSE(is_exist);

  is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(exist_block_id, 0, is_exist));
  ASSERT_TRUE(is_exist);

  EXE_SQL("drop table test_table;");
  EXE_SQL("purge recyclebin;");
}

TEST_F(ObSharedStorageBlockCheckTest, test_tablet_gc)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t tablet_version1 = 0;
  int64_t tablet_version2 = 0;
  int64_t tablet_version3 = 0;
  EXE_SQL("create table test_table (a int)");
  LOG_INFO("create_table finish");

  set_ls_and_tablet_id_for_run_ctx();

  share::SCN ss_checkpoint_scn;

  ObSSMetaReadParam param;
  share::SCN transfer_scn;
  transfer_scn.set_min();
  param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY, ObSSMetaReadResultType::READ_WHOLE_ROW, false, sslog::ObSSLogMetaType::SSLOG_MINI_SSTABLE, RunCtx.ls_id_, RunCtx.tablet_id_, transfer_scn);

  EXE_SQL("alter system set _ss_schedule_upload_interval = '1s';");
  EXE_SQL("alter system set inc_sstable_upload_thread_score = 20;");
  EXE_SQL("alter system set _ss_garbage_collect_interval = '10s';");
  EXE_SQL("alter system set _ss_garbage_collect_file_expiration_time = '10s';");
  EXE_SQL("alter system set _ss_enable_timeout_garbage_collection = true;");


  sleep(5);
  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish");

  EXE_SQL("alter system minor freeze;");
  RunCtx.ls_->get_end_scn(ss_checkpoint_scn);
  wait_minor_finish(get_curr_simple_server().get_sql_proxy2());
  wait_upload_sstable(ss_checkpoint_scn.get_val_for_tx());


  MacroBlockId not_exist_mini_block_id;
  gen_block_id(1000, ObStorageObjectType::SHARED_MINI_DATA_MACRO, 0, not_exist_mini_block_id, 0);
  write_block(not_exist_mini_block_id);

  EXE_SQL("drop table test_table;");
  EXE_SQL("purge recyclebin;");
  EXE_SQL("alter system minor freeze;");
  wait_tablet_gc_finish();
  wait_shared_tablet_gc_finish();

  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.ss_is_exist_object(not_exist_mini_block_id, 0, is_exist));
  ASSERT_FALSE(is_exist);

}
TEST_F(ObSharedStorageBlockCheckTest, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}

void ObSharedStorageBlockCheckTest::wait_shared_tablet_gc_finish()
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

void ObSharedStorageBlockCheckTest::get_major_sstable_root_seq(
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

void ObSharedStorageBlockCheckTest::wait_tablet_gc_finish()
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
