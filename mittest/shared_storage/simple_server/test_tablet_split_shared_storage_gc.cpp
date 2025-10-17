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
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "close_modules/shared_storage/storage/incremental/ob_shared_meta_service.h"
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
  ObTabletID split_src_tablet_id_;
  ObArray<ObTabletID> split_dest_tablet_ids_;
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
  if (OB_ISNULL(entry->d_name)) {
    SERVER_LOG(INFO, "d_name is null");
  } else {
    SERVER_LOG(INFO, "dir_entry", K(entry->d_name));
  }
  file_cnt_++;
  return OB_ERR_EXIST_OBJECT;
}

class ObSharedStorageTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObSharedStorageTest() : ObSimpleClusterTestBase("test_tablet_split_shared_storage_gc_", "50G", "50G", "50G")
  {}
  void wait_minor_finish();
  void set_ls_and_split_source_tablet_id();
  void set_split_dest_tablets_id();
  void wait_shared_split_source_tablet_gc_finish();
  void check_split_dest_tablets_normal();
  void wait_shared_table_tablets_gc_finish();
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


  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

};

void ObSharedStorageTest::set_ls_and_split_source_tablet_id()
{
  int ret = OB_SUCCESS;

  ObLSHandle ls_handle;
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t uid = 0;
  int64_t id = 0;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, sql.assign("select tablet_id from oceanbase.__all_part where part_name='p1' and table_id in (select table_id from oceanbase.__all_table where table_name='test_table');"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_uint("tablet_id", uid));
  }
  RunCtx.split_src_tablet_id_ = uid;

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
  LOG_INFO("get and set split source tablets", K(RunCtx.tenant_epoch_), K(RunCtx.ls_id_), K(RunCtx.ls_epoch_), K(RunCtx.split_src_tablet_id_));
}

void ObSharedStorageTest::set_split_dest_tablets_id()
{
  int ret = OB_SUCCESS;
  RunCtx.split_dest_tablet_ids_.reset();
  ObSqlString sql;
  int64_t affected_rows = 0;
  uint64_t uid = 0;
  int64_t id = 0;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, sql.assign("select tablet_id from oceanbase.__all_part where table_id in (select table_id from oceanbase.__all_table where table_name='test_table');"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    while (OB_SUCC(result->next())) {
      ASSERT_EQ(OB_SUCCESS, result->get_uint("tablet_id", uid));
      ASSERT_EQ(OB_SUCCESS, RunCtx.split_dest_tablet_ids_.push_back(ObTabletID(uid)));
    }
    // ignore OB_ITER_END.
    ASSERT_EQ(2, RunCtx.split_dest_tablet_ids_.count());
    LOG_INFO("get split dest tablets", K(RunCtx.split_dest_tablet_ids_));
  }
}

void ObSharedStorageTest::wait_shared_split_source_tablet_gc_finish()
{
  int ret = OB_SUCCESS;
  bool is_mark_deleted = false;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  ObCheckDirEmptOp shared_macro_op;
  ObMemAttr mem_attr(MTL_ID(), "test_gc");
  ObArenaAllocator allocator(mem_attr);

  do {
	ObTabletHandle ss_tablet;
    ret = MTL(ObSSMetaService*)->check_mark_deleted(RunCtx.ls_id_, RunCtx.split_src_tablet_id_, share::SCN::min_scn(), is_mark_deleted);
    LOG_INFO("wait tablet meta", K(ret), K(RunCtx.ls_id_), K(RunCtx.split_src_tablet_id_), K(is_mark_deleted));
    if (OB_TABLET_NOT_EXIST == ret) {
      // gc finish.
      break;
    }
  } while (true);

  shared_macro_op.reset();
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_tablet_dir(dir_path, sizeof(dir_path), RunCtx.split_src_tablet_id_.id()));
  ret = MTL(ObTenantFileManager*)->list_remote_files(dir_path, shared_macro_op);
  LOG_INFO("shared tablet dir", K(ret),K(dir_path), K(shared_macro_op));
  ASSERT_EQ(OB_ERR_EXIST_OBJECT, ret);
  ASSERT_LT(0, shared_macro_op.get_file_cnt());
  LOG_INFO("shared tablet dir gc finish", K(ret), K(RunCtx.ls_id_), K(RunCtx.split_src_tablet_id_), K(dir_path), K(shared_macro_op));
}

void get_and_test_value(sqlclient::ObMySQLResult &mysql_result,
                        const char *column,
                        int64_t expected_value) {
  int ret = OB_SUCCESS;
  int64_t res = 0;
  EXTRACT_INT_FIELD_MYSQL(mysql_result, column, res, int64_t);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(expected_value, res);
  LOG_INFO("get and test value", K(column), K(res), K(expected_value));
}

void get_and_test_value(sqlclient::ObMySQLResult &mysql_result,
                        const char *column,
                        const char *expected_value) {
  int ret = OB_SUCCESS;
  common::ObString md5_res;
  EXTRACT_VARCHAR_FIELD_MYSQL(mysql_result, column, md5_res);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_STREQ(expected_value, md5_res.ptr());
  LOG_INFO("get and test value", K(column), K(md5_res), K(expected_value));
}

void ObSharedStorageTest::check_split_dest_tablets_normal()
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ASSERT_EQ(OB_SUCCESS, sql.assign("select /*+ query_timeout(3600000000) index(test_mult_sstables c2) */ count(1) as row_count from test_table;"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    get_and_test_value(*result, "row_count", 2000);
  }

  ASSERT_EQ(OB_SUCCESS, sql.assign("select /*+ query_timeout(3600000000) index(test_mult_sstables c2) */ min(c1) as min_c1, max(c1) as max_c1, min(md5(c2)) as min_c2, max(md5(c2)) as max_c2, min(md5(c3)) as min_c3, max(md5(c3)) as max_c3 from test_table partition(p2);"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res2, sql.ptr()));
    sqlclient::ObMySQLResult *result = res2.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    get_and_test_value(*result, "min_c1", 4000);
    get_and_test_value(*result, "max_c1", 4999);
    get_and_test_value(*result, "min_c2", "017f3f8312a1e086f387a26976e76f00");
    get_and_test_value(*result, "max_c2", "ff728bd25826d508ec83c666f3110454");
    get_and_test_value(*result, "min_c3", "00b85a671442d1eeb0d163342a32ae11");
    get_and_test_value(*result, "max_c3", "ff16e75f335d82d91f89b2068c928921");
  }

  ASSERT_EQ(OB_SUCCESS, sql.assign("select /*+ query_timeout(3600000000) index(test_mult_sstables c2) */ min(c1) as min_c1, max(c1) as max_c1, min(md5(c2)) as min_c2, max(md5(c2)) as max_c2, min(md5(c3)) as min_c3, max(md5(c3)) as max_c3 from test_table partition (p3);"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res3) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res3, sql.ptr()));
    sqlclient::ObMySQLResult *result = res3.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    get_and_test_value(*result, "min_c1", 5000);
    get_and_test_value(*result, "max_c1", 5999);
    get_and_test_value(*result, "min_c2", "005e45771b96ed921f6e4a9529287282");
    get_and_test_value(*result, "max_c2", "ff7d39df5c7dce17adcc6948b4ba7949");
    get_and_test_value(*result, "min_c3", "003004a1d696fd47ac64a0a787580561");
    get_and_test_value(*result, "max_c3", "ff8a571c2dd43bd244c544f9f936af2b");
  }
}

void ObSharedStorageTest::wait_shared_table_tablets_gc_finish()
{
  int ret = OB_SUCCESS;
  char dir_path[common::MAX_PATH_SIZE] = {0};
  ObCheckDirEmptOp shared_macro_op;
  ObMemAttr mem_attr(MTL_ID(), "test_gc");
  ObArenaAllocator allocator(mem_attr);
  ObArray<ObTabletID> checked_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, checked_tablet_ids.assign(RunCtx.split_dest_tablet_ids_));
  ASSERT_EQ(OB_SUCCESS, checked_tablet_ids.push_back(RunCtx.split_src_tablet_id_));
  ASSERT_EQ(3, checked_tablet_ids.count());
  for (int i = 0; i < checked_tablet_ids.count(); i++) {
    memset(dir_path, 0, sizeof(dir_path));
    ObTabletID tablet_id = checked_tablet_ids.at(i);
    do {
      shared_macro_op.reset();
      ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_tablet_dir(dir_path, sizeof(dir_path), tablet_id.id()));
      ret = MTL(ObTenantFileManager*)->list_remote_files(dir_path, shared_macro_op);
      LOG_INFO("shared tablet dir", K(ret), K(tablet_id), K(dir_path), K(shared_macro_op));
      usleep(100 * 1000);
    } while (0 != shared_macro_op.get_file_cnt());

    do {
      ObTabletHandle ss_tablet;
      ret = MTL(ObSSMetaService*)->get_tablet(RunCtx.ls_id_, tablet_id, share::SCN::min_scn(), allocator, ss_tablet);
      LOG_INFO("wait tablet meta", K(ret), K(tablet_id), K(dir_path));
    } while (OB_SUCCESS == ret);
  }
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
          RunCtx.tenant_id_, RunCtx.split_src_tablet_id_.id()));
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

TEST_F(ObSharedStorageTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

#define EXE_SQL(sql_str)                                            \
  LOG_INFO("exe sql start", K(sql_str));      \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows));   \
  LOG_INFO("exe sql end", K(sql_str));

#define SYS_EXE_SQL(sql_str)                                            \
  LOG_INFO("sys exe sql start", K(sql_str)); \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write(sql.ptr(), affected_rows));  \
  LOG_INFO("sys exe sql end", K(sql_str));

TEST_F(ObSharedStorageTest, add_tenant)
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

TEST_F(ObSharedStorageTest, test_tablet_gc_for_shared_dir)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  EXE_SQL("create sequence seq_even INCREMENT BY 2 start with 4000 minvalue 1 maxvalue 100000 nocycle noorder nocache;");
  EXE_SQL("create sequence seq_odd INCREMENT BY 2 start with 4001 minvalue 1 maxvalue 100000 nocycle noorder nocache;");
  EXE_SQL("create table test_table(c1 bigint, c2 varchar(1000), c3 varchar(8000), key(c2),primary key(c1)) row_format=redundant compression='none' partition by range(c1) (partition p1 values less than MAXVALUE);");
  EXE_SQL("insert /*+ query_timeout(3600000000) append parallel(8) enable_parallel_dml */ into test_table select seq_even.nextval, randstr(1000, random(10)), randstr(8000, random(10)) from table(generator(1000));");
  EXE_SQL("insert /*+ query_timeout(3600000000) parallel(8) enable_parallel_dml */ into test_table select seq_odd.nextval, randstr(1000, random(10)), randstr(8000, random(10)) from table(generator(1000));");
  LOG_INFO("1. create_table and load data finish");
  set_ls_and_split_source_tablet_id();

  EXE_SQL("alter system set _ss_schedule_upload_interval = '1s';");
  EXE_SQL("alter system set inc_sstable_upload_thread_score = 20;");
  EXE_SQL("alter system set _ss_garbage_collect_interval = '10s';");
  EXE_SQL("alter system set _ss_garbage_collect_file_expiration_time = '10s';");
  EXE_SQL("alter system set _ss_enable_timeout_garbage_collection = true;");

  EXE_SQL("alter table test_table reorganize partition p1 into ( partition p2 values less than (5000), partition p3 values less than MAXVALUE);");
  LOG_INFO("2. tablet split finished", K(RunCtx.ls_id_), K(RunCtx.split_src_tablet_id_));

  wait_shared_split_source_tablet_gc_finish();
  LOG_INFO("3. wait source tablet gc finished", K(RunCtx.ls_id_), K(RunCtx.split_src_tablet_id_));

  set_split_dest_tablets_id();
  check_split_dest_tablets_normal();
  LOG_INFO("4. set split dests and check normal", K(RunCtx.ls_id_), K(RunCtx.split_dest_tablet_ids_));

  EXE_SQL("drop table test_table;");
  wait_shared_table_tablets_gc_finish();
  LOG_INFO("5. wait shared table tablets gc finished", K(RunCtx.ls_id_), K(RunCtx.split_src_tablet_id_));
}

TEST_F(ObSharedStorageTest, end)
{
  if (RunCtx.time_sec_ > 0) {
    ::sleep(RunCtx.time_sec_);
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
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
