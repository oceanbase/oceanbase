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
#include "close_modules/shared_storage/storage/shared_storage/ob_dir_manager.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_public_block_gc_service.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/init_basic_struct.h"
#include "rootserver/ob_ls_recovery_reportor.h"

namespace oceanbase
{
char *shared_storage_info = NULL;
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;


class TestRunCtx
{
public:
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;
  int64_t time_sec_ = 0;
};

TestRunCtx RunCtx;

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
  UNUSED(entry);
  file_cnt_++;
  return OB_ERR_EXIST_OBJECT;
}

class ObSharedStorageTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObSharedStorageTest() : ObSimpleClusterTestBase("test_shared_storage_ls_gc_", "50G", "50G", "50G")
  {}
  int get_block_ids_from_dir(
    const ObTabletID &tablet_id,
    ObMacroType macro_type,
    ObIArray<blocksstable::MacroBlockId> &block_ids);
  void check_block_for_private_dir(
      const int64_t tablet_version);
  void check_block_for_shared_dir();
  void wait_minor_finish();
  void wait_tablet_gc_finish();
  void get_tablet_version(
      int64_t &tablet_version);
  void set_ls_and_tablet_id_for_run_ctx();
  void wait_ls_gc_finish(
      const ObLSID &ls_id,
      const int64_t ls_epoch);

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

};

TEST_F(ObSharedStorageTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

#define EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows));

#define SYS_EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write(sql.ptr(), affected_rows));

TEST_F(ObSharedStorageTest, add_tenant)
{
  ASSERT_EQ(OB_SUCCESS, create_tenant("tt1", "5G", "10G", false, 10));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());

}

TEST_F(ObSharedStorageTest, test_tablet_gc_for_private_dir)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t tablet_version1;
  int64_t tablet_version2;
  int64_t tablet_version3;
  EXE_SQL("create table test_table (a int)");
  LOG_INFO("create_table finish");

  set_ls_and_tablet_id_for_run_ctx();


  get_tablet_version(tablet_version1);
  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish");

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();
  get_tablet_version(tablet_version2);
  LOG_INFO("get tablet version", K(tablet_version1), K(tablet_version2));
  ASSERT_LT(tablet_version1, tablet_version2);


  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish");

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  get_tablet_version(tablet_version3);
  LOG_INFO("get tablet version", K(tablet_version2), K(tablet_version3));
  ASSERT_LT(tablet_version2, tablet_version3);

  check_block_for_private_dir(tablet_version3);

  EXE_SQL("drop table test_table;");
  EXE_SQL("purge recyclebin;");
  wait_tablet_gc_finish();
}

TEST_F(ObSharedStorageTest, test_ls_abort)
{
  share::ObTenantSwitchGuard tguard;
  ObLSID ls_id(111);
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  MTL(logservice::ObGarbageCollector*)->stop_create_new_gc_task_ = true;
  MTL(rootserver::ObLSRecoveryReportor*)->stop_ = true;
  ObCreateLSArg arg;
  ObLSHandle handle;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(RunCtx.tenant_id_, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->create_ls(arg));
  EXPECT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ObLS *ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_EQ(OB_SUCCESS, ls->offline());
  ASSERT_EQ(OB_SUCCESS, TENANT_STORAGE_META_PERSISTER.abort_create_ls(ls->get_ls_id(), ls->get_ls_epoch()));
  wait_ls_gc_finish(ls->get_ls_id(), ls->get_ls_epoch());
}

TEST_F(ObSharedStorageTest, test_ls_gc)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  int64_t affected_rows = 0;
  ObSqlString sql;
  EXE_SQL("create table test_table (a int)");
  set_ls_and_tablet_id_for_run_ctx();

  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish");

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(RunCtx.ls_id_));
  wait_ls_gc_finish(RunCtx.ls_id_, RunCtx.ls_epoch_);
}

void ObSharedStorageTest::wait_ls_gc_finish(const ObLSID &ls_id, const int64_t ls_epoch)
{
  bool is_exist = false;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  do {
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_ls_id_dir(dir_path, sizeof(dir_path), RunCtx.tenant_id_, RunCtx.tenant_epoch_, ls_id.id(), ls_epoch));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(dir_path, is_exist));
    LOG_INFO("wait_ls_gc_finish", K(dir_path), K(is_exist));
    usleep(100 *1000);
  } while (is_exist);
}

void ObSharedStorageTest::wait_minor_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait minor begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t row_cnt = 0;
  do {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as row_cnt from oceanbase.__all_virtual_table_mgr where tenant_id=%lu and tablet_id=%lu and table_type=0;",
          RunCtx.tenant_id_, RunCtx.tablet_id_.id()));
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

void ObSharedStorageTest::set_ls_and_tablet_id_for_run_ctx()
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
  LOG_INFO("finish set run ctx", K(RunCtx.tenant_epoch_), K(RunCtx.ls_id_), K(RunCtx.ls_epoch_), K(RunCtx.tablet_id_));
}

TEST_F(ObSharedStorageTest, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
  }
}

void ObSharedStorageTest::get_tablet_version(
      int64_t &tablet_version)
{
  ObArray<int64_t> tablet_versions;
  bool is_old_version_empty = false;
  int64_t current_tablet_version = -1;
  int64_t current_tablet_trans_seq = -1;
  do {
    ASSERT_EQ(OB_SUCCESS, MTL(ObTenantMetaMemMgr*)->get_current_version_for_tablet(RunCtx.ls_id_, RunCtx.tablet_id_, current_tablet_version, current_tablet_trans_seq, is_old_version_empty));
    ASSERT_NE(-1, current_tablet_version);
    if (!is_old_version_empty) continue;

    ObPrivateBlockGCHandler handler(RunCtx.ls_id_, RunCtx.ls_epoch_, RunCtx.tablet_id_, current_tablet_version, current_tablet_trans_seq);
    LOG_INFO("wait old tablet version delete", K(current_tablet_version), K(is_old_version_empty), K(RunCtx.ls_id_), K(RunCtx.ls_epoch_), K(handler));
    ASSERT_EQ(OB_SUCCESS, handler.list_tablet_meta_version(tablet_versions));
    usleep(100 * 1000);
  } while (1 != tablet_versions.count());
  tablet_version = tablet_versions.at(0);
}

void ObSharedStorageTest::wait_tablet_gc_finish()
{
  bool is_exist = false;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  do {
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_data_tablet_id_dir(dir_path, sizeof(dir_path), RunCtx.tenant_id_, RunCtx.tenant_epoch_, RunCtx.tablet_id_.id()));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(dir_path, is_exist));
    usleep(100 *1000);
  } while (is_exist);

  memset(dir_path, 0, sizeof(dir_path));
  do {
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_tablet_meta_tablet_id_dir(dir_path, sizeof(dir_path), RunCtx.tenant_id_, RunCtx.tenant_epoch_, RunCtx.ls_id_.id(), RunCtx.ls_epoch_, RunCtx.tablet_id_.id()));
    ASSERT_EQ(OB_SUCCESS, ObIODeviceLocalFileOp::exist(dir_path, is_exist));
    usleep(100 * 1000);
  } while (is_exist);
}

void ObSharedStorageTest::check_block_for_shared_dir()
{

  ObPublicBlockGCHandler handler(RunCtx.tablet_id_);
  ObArray<blocksstable::MacroBlockId> block_ids_in_tablet;
  ObArray<blocksstable::MacroBlockId> unuse_block_ids;
  ObArray<blocksstable::MacroBlockId> block_ids_in_dir;
  ObArray<blocksstable::MacroBlockId> empty_block_ids;
  ObArray<int64_t> tablet_meta_versions;
  GCTabletOP op_for_result;

  do {
    ASSERT_EQ(OB_SUCCESS, handler.list_tablet_meta_version(tablet_meta_versions));
    LOG_INFO("tablet meta versions", K(tablet_meta_versions));
  } while (1 != tablet_meta_versions.count());

  int64_t tablet_version = tablet_meta_versions.at(0);

  ASSERT_EQ(OB_SUCCESS, handler.get_blocks_for_tablet(tablet_version, block_ids_in_tablet));
  ASSERT_EQ(OB_SUCCESS, op_for_result(block_ids_in_tablet, unuse_block_ids));

  MacroBlockCheckOP op_for_check(op_for_result.result_block_id_set_, 0, UINT64_MAX - 1);
  ASSERT_EQ(OB_SUCCESS, get_block_ids_from_dir(RunCtx.tablet_id_, ObMacroType::DATA_MACRO, block_ids_in_dir));
  ASSERT_EQ(OB_SUCCESS, get_block_ids_from_dir(RunCtx.tablet_id_, ObMacroType::META_MACRO, block_ids_in_dir));
  ASSERT_EQ(OB_SUCCESS, op_for_check(block_ids_in_dir, empty_block_ids));
  LOG_INFO("check block", K(tablet_meta_versions), K(tablet_version), K(empty_block_ids), K(block_ids_in_tablet), K(block_ids_in_dir));
  ASSERT_EQ(block_ids_in_tablet.count(), block_ids_in_dir.count());
  ASSERT_EQ(0, empty_block_ids.count());
}

void ObSharedStorageTest::check_block_for_private_dir(
    const int64_t tablet_version)
{

  ObPrivateBlockGCHandler handler(RunCtx.ls_id_, RunCtx.ls_epoch_, RunCtx.tablet_id_, tablet_version, 0 /*transfer_seq*/);
  ObArray<blocksstable::MacroBlockId> block_ids_in_tablet;
  ObArray<blocksstable::MacroBlockId> unuse_block_ids;
  ObArray<blocksstable::MacroBlockId> block_ids_in_dir;
  ObArray<blocksstable::MacroBlockId> empty_block_ids;
  ObArray<int64_t> tablet_meta_versions;
  GCTabletOP op_for_block_ids_in_tablet_result;
  GCTabletOP op_for_block_ids_in_dir_result;

  ASSERT_EQ(OB_SUCCESS, handler.list_tablet_meta_version(tablet_meta_versions));
  ASSERT_EQ(1, tablet_meta_versions.count());

  ASSERT_EQ(OB_SUCCESS, handler.get_blocks_for_tablet(tablet_version, block_ids_in_tablet));
  ASSERT_EQ(OB_SUCCESS, handler.get_block_ids_from_dir(block_ids_in_dir));
  ASSERT_EQ(block_ids_in_tablet.count(), block_ids_in_dir.count());
  ASSERT_EQ(OB_SUCCESS, op_for_block_ids_in_tablet_result(block_ids_in_tablet, unuse_block_ids));
  MacroBlockCheckOP op_for_check(op_for_block_ids_in_tablet_result.result_block_id_set_, 0, UINT64_MAX - 1);
  ASSERT_EQ(OB_SUCCESS, op_for_check(block_ids_in_dir, empty_block_ids));
  ASSERT_EQ(0, empty_block_ids.count());

  LOG_INFO("check block finish", K(tablet_version), K(empty_block_ids), K(block_ids_in_tablet), K(block_ids_in_dir));
}

int ObSharedStorageTest::get_block_ids_from_dir(
    const ObTabletID &tablet_id,
    ObMacroType macro_type,
    ObIArray<blocksstable::MacroBlockId> &block_ids)
{
  int ret = OB_SUCCESS;
  ObSingleNumFileListOp shared_macro_op;
  char shared_macro_path[storage::ObServerFileManager::OB_MAX_FILE_PATH_LENGTH] = {0};
  const int64_t cluster_id = GCONF.cluster_id;
  char *object_storage_root_dir = nullptr;
  if (OB_FAIL(OB_DIR_MGR.get_object_storage_root_dir(object_storage_root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret), K(object_storage_root_dir));
  // cluster_id/tenant_id/tablet/tablet_id/major/sstable/cg_id/data or meta
  } else if (OB_FAIL(databuff_printf(shared_macro_path, sizeof(shared_macro_path), "%s/%s_%ld/%s_%lu/%s/%ld/%s/%s/cg_0/%s",
                                     object_storage_root_dir, CLUSTER_DIR_STR,
                                     cluster_id, TENANT_DIR_STR, MTL_ID(), TABLET_DIR_STR,
                                     tablet_id.id(), MAJOR_DIR_STR, SHARED_TABLET_SSTABLE_DIR_STR,
                                     get_macro_type_str(macro_type)))) {
    LOG_WARN("fail to databuff printf", KR(ret), K(tablet_id), K(MTL_ID()));
  } else if (OB_FAIL(MTL(ObTenantFileManager*)->list_remote_files(shared_macro_path, shared_macro_op))) {
    LOG_WARN("fail to list remote files", KR(ret), K(shared_macro_op));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < shared_macro_op.file_list_.count(); i++) {
      MacroBlockId file_id;
      file_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
      if (ObMacroType::DATA_MACRO == macro_type) {
        file_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
      } else if (ObMacroType::META_MACRO == macro_type) {
        file_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_META_MACRO);
      }
      file_id.set_second_id(tablet_id.id());
      file_id.set_third_id(shared_macro_op.file_list_.at(i));
      if (OB_FAIL(block_ids.push_back(file_id))) {
        LOG_WARN("fail to push back", KR(ret), K(file_id));
      }
    }
  }
  return ret;
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
  databuff_printf(buf, sizeof(buf), "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=2000&max_bandwidth=200000000B&scope=region",
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
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
