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

#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#define UNITTEST

#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_private_block_gc_task.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_dir_manager.h"
#include "share/ob_io_device_helper.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/init_basic_struct.h"
#include "rootserver/ob_ls_recovery_reportor.h"
#include "storage/shared_storage/ob_file_op.h"
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


namespace oceanbase
{
char *shared_storage_info = NULL;
int64_t tx_data_recycle_scn = 0;

namespace storage
{
#ifdef OB_BUILD_SHARED_STORAGE
int ObTxTable::resolve_shared_storage_upload_info_(share::SCN &tablet_recycle_scn)
{
  int ret = OB_SUCCESS;
  const int64_t SHARED_STORAGE_USE_CACHE_DURATION = 1_hour;

  SCN tx_data_table_upload_scn = SCN::invalid_scn();
  SCN data_upload_min_end_scn = SCN::invalid_scn();
  int64_t origin_time = ATOMIC_LOAD(&(ss_upload_scn_cache_.update_ts_));
  int64_t current_time = ObClockGenerator::getClock();

  if (current_time - origin_time > 1_s
      && ATOMIC_BCAS(&(ss_upload_scn_cache_.update_ts_), origin_time, current_time)) {
    share::SCN transfer_scn;
    {
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ls_->get_tablet_svr()->get_tablet(LS_TX_DATA_TABLET, tablet_handle))) {
        TRANS_LOG(WARN, "get tx data tablet fail", K(ret));
      } else {
        transfer_scn = tablet_handle.get_obj()->get_reorganization_scn();
      }
    }
    // Tip1: may output min_scn if no uploads exists or max_uploaded_scn if
    // there exists
    if (FAILEDx(ls_->get_inc_sstable_uploader().
                get_tablet_upload_pos(LS_TX_DATA_TABLET,
                                      transfer_scn,
                                      tx_data_table_upload_scn))) {
      TRANS_LOG(WARN, "get tablet upload pos failed", K(ret));
    // Tip2: may output min_scn if no uploads exists or max_uploaded_scn if
    // there exists
    } else if (OB_FAIL(ls_->get_inc_sstable_uploader().
                       get_upload_min_end_scn_from_ss(data_upload_min_end_scn))) {
      TRANS_LOG(WARN, "get tablet upload min end scn failed", K(ret));
    // We need ensure that no concurrent user after an hour later will
    // concurrently change the cache
    } else if (ATOMIC_BCAS(&(ss_upload_scn_cache_.update_ts_),
                           current_time, current_time + 1)) {
      ss_upload_scn_cache_.tx_table_upload_max_scn_cache_.atomic_store(tx_data_table_upload_scn);
      ss_upload_scn_cache_.data_upload_min_end_scn_cache_.atomic_store(data_upload_min_end_scn);
      TRANS_LOG(INFO, "ss_upload_scn updated successfully", K(ss_upload_scn_cache_));
    }

    if (OB_FAIL(ret)) {
      if (!ATOMIC_BCAS(&(ss_upload_scn_cache_.update_ts_), current_time, origin_time)) {
        TRANS_LOG(WARN, "ss_upload_scn_cache is updated by a concurrent user after an hour!!!",
                  K(ss_upload_scn_cache_));
      }
      // under error during get upload info, we choose to use cache instead
      ret = OB_SUCCESS;
    }
  }


  if (OB_SUCC(ret)) {
    tx_data_table_upload_scn = ss_upload_scn_cache_.tx_table_upload_max_scn_cache_.atomic_load();
    data_upload_min_end_scn = ss_upload_scn_cache_.data_upload_min_end_scn_cache_.atomic_load();

    // Tip1: we need to obtain the upload location for the tx_data_table to
    // ensure that transaction data that hasn't been uploaded is not recycled,
    // thereby maintaining the integrity of transaction data on shared storage.
    // It is important to note that this is not a correctness requirement but
    // rather for facilitating better troubleshooting in the future.
    if (!tx_data_table_upload_scn.is_valid()) {
      // we havenot aleady upload anything in cache
      tablet_recycle_scn.set_min();
    } else if (tx_data_table_upload_scn < tablet_recycle_scn) {
      tablet_recycle_scn = tx_data_table_upload_scn;
    }

    // Tip2: we need to obtain the smallest end_scn among all tablets that have
    // uploaded its sstables. This ensures that uncommitted data, which has not
    // been backfilled, can certainly be interpreted by the transaction data on
    // shared storage. It is important to note that this is a correctness
    // requirement. By also ensuring that computing nodes do not reclaim
    // transaction data, we can guarantee that any data retrieved from shared
    // storage can be interpreted properly.
    if (!data_upload_min_end_scn.is_valid()) {
      // we havenot aleady upload anything in cache
      tablet_recycle_scn.set_min();
    } else if (data_upload_min_end_scn < tablet_recycle_scn) {
      tablet_recycle_scn = data_upload_min_end_scn;
    }

    if (1002 == MTL_ID() && ls_id_.id() > 1000) {
      tx_data_recycle_scn = tablet_recycle_scn.get_val_for_tx();
      TRANS_LOG(INFO, "qianchen debug", K(tx_data_recycle_scn));
    }
  }

  return ret;
}
#endif
}

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

class ObSharedStorageTest : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObSharedStorageTest() : ObSimpleClusterTestBase("test_shared_storage_tx_data_gc_", "50G", "50G", "50G")
  {}
  void wait_minor_finish();
  void set_ls_and_tablet_id_for_run_ctx();
  void get_row_scn(const char *value, int64_t &row_scn);

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


TEST_F(ObSharedStorageTest, test_tx_data_gc)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(RunCtx.tenant_id_));
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t tablet_version1;
  int64_t tablet_version2;
  int64_t tablet_version3;
  EXE_SQL("alter system set _tx_result_retention = 0");
  EXE_SQL("alter system set ob_compaction_schedule_interval = '3s'");
  EXE_SQL("create table test_table (a int)");
  LOG_INFO("create_table finish");

  set_ls_and_tablet_id_for_run_ctx();

  EXE_SQL("insert into test_table values (1)");
  LOG_INFO("insert data finish1");
  ob_usleep(1 * 1000 * 1000);
  int64_t row_scn;
  get_row_scn("1", row_scn);

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  EXE_SQL("insert into test_table values (2)");
  LOG_INFO("insert data finish2");
  ob_usleep(1 * 1000 * 1000);
  int64_t row_scn2;
  get_row_scn("2", row_scn2);

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  EXE_SQL("insert into test_table values (3)");
  LOG_INFO("insert data finish3");

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  ob_usleep(40 * 1000 * 1000);

  ASSERT_GE(tx_data_recycle_scn, row_scn);

  EXE_SQL("insert into test_table values (4)");
  LOG_INFO("insert data finish4");

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  EXE_SQL("insert into test_table values (5)");
  LOG_INFO("insert data finish4");

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  EXE_SQL("insert into test_table values (6)");
  LOG_INFO("insert data finish4");

  EXE_SQL("alter system minor freeze tenant tt1;");
  wait_minor_finish();

  ob_usleep(40 * 1000 * 1000);

  ASSERT_GE(tx_data_recycle_scn, row_scn2);

}

void ObSharedStorageTest::get_row_scn(const char *value,
                                      int64_t &row_scn)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ObSqlString sql;
  int64_t affected_rows = 0;

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ORA_ROWSCN as row_scn from test_table where a = %s", value));
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int("row_scn", row_scn));
  }

  LOG_INFO("get row scn", K(row_scn));
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
