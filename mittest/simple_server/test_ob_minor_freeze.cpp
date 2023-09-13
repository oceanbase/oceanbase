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
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/ls/ob_freezer.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/checkpoint/ob_data_checkpoint.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "storage/ob_storage_table_guard.h"
#include "storage/ob_relative_table.h"
#include "storage/access/ob_rows_info.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
namespace oceanbase
{
const int SLEEP_TIME = 100;
namespace memtable
{
int64_t ObMemtable::inc_write_ref_()
{
  ob_usleep(rand() % SLEEP_TIME);
  return ATOMIC_AAF(&write_ref_cnt_, 1);
}

int64_t ObMemtable::dec_write_ref_()
{
  ob_usleep(rand() % SLEEP_TIME);
  int64_t write_ref_cnt = ATOMIC_SAF(&write_ref_cnt_, 1);
  if (write_ref_cnt < 0) {
    ob_abort();
  }
  return write_ref_cnt;
}

int64_t ObMemtable::inc_unsubmitted_cnt_()
{
  ob_usleep(rand() % SLEEP_TIME);
  return ATOMIC_AAF(&unsubmitted_cnt_, 1);
}

int64_t ObMemtable::dec_unsubmitted_cnt_()
{
  ob_usleep(rand() % SLEEP_TIME);
  int64_t unsubmitted_cnt = ATOMIC_SAF(&unsubmitted_cnt_, 1);
  if (unsubmitted_cnt < 0) {
    ob_abort();
  }
  return unsubmitted_cnt;
}

int64_t ObMemtable::inc_unsynced_cnt_()
{
  ob_usleep(rand() % SLEEP_TIME);
  return ATOMIC_AAF(&unsynced_cnt_, 1);
}

int64_t ObMemtable::dec_unsynced_cnt_()
{
  ob_usleep(rand() % SLEEP_TIME);
  int64_t unsynced_cnt = ATOMIC_SAF(&unsynced_cnt_, 1);
  if (unsynced_cnt < 0) {
    ob_abort();
  }
  return unsynced_cnt;
}

}
namespace storage
{
bool ObFreezer::is_freeze(uint32_t freeze_flag) const
{
  ob_usleep(rand() % SLEEP_TIME);
  if (freeze_flag == UINT32_MAX) {
    freeze_flag = (ATOMIC_LOAD(&freeze_flag_));
  }
  return 1 == (freeze_flag >> 31);
}
namespace checkpoint
{
  int64_t TRAVERSAL_FLUSH_INTERVAL = 100 * 1000L;
}
} // namespace storage
namespace transaction
{
int ObPartTransCtx::submit_redo_log_for_freeze_(bool &try_submit)
{
  int ret = OB_SUCCESS;
  int64_t sleep_time = rand() % SLEEP_TIME;
  ob_usleep(sleep_time);

  ATOMIC_STORE(&is_submitting_redo_log_for_freeze_, true);
  if (OB_FAIL(check_and_submit_redo_log_(try_submit))) {
    TRANS_LOG(WARN, "fail to submit redo log for freeze", K(ret));
  }
  if (try_submit && (OB_SUCC(ret) || OB_BLOCK_FROZEN == ret)) {
    ret = submit_log_impl_(ObTxLogType::TX_MULTI_DATA_SOURCE_LOG);
  }
  ATOMIC_STORE(&is_submitting_redo_log_for_freeze_, false);
  if (sleep_time > 50 && sleep_time < 90) {
    ret = OB_TX_NOLOGCB;
  } else if (sleep_time >= 90) {
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}
}
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
};

TestRunCtx RunCtx;

class ObMinorFreezeTest : public ObSimpleClusterTestBase
{
public:
  ObMinorFreezeTest() : ObSimpleClusterTestBase("test_ob_minor_freeze_") {}
  void get_tablet_id_and_ls_id();
  void get_ls();
  void insert_data(int start);
  void tenant_freeze();
  void logstream_freeze();
  void tablet_freeze();
  void tablet_freeze_for_replace_tablet_meta();
  void batch_tablet_freeze();
  void insert_and_freeze();
  void empty_memtable_flush();
  void all_virtual_minor_freeze_info();
  void check_frozen_memtable();
  void create_tables();
private:
  static const int64_t OB_DEFAULT_TABLE_COUNT = 3;
private:
  int insert_thread_num_ = 10;
  int insert_num_ = 200000;
  int freeze_num_ = 1000;
  int freeze_duration_ = 50 * 1000 * 1000;
  ObSEArray<int64_t, OB_DEFAULT_TABLE_COUNT> table_ids_;
  ObSEArray<ObTabletID, OB_DEFAULT_TABLE_COUNT> tablet_ids_;
  ObSEArray<share::ObLSID, OB_DEFAULT_TABLE_COUNT> ls_ids_;
  ObSEArray<ObLSHandle, OB_DEFAULT_TABLE_COUNT> ls_handles_;
};

void ObMinorFreezeTest::get_tablet_id_and_ls_id()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  int64_t tmp_table_id = 0;
  int64_t tmp_tablet_id = 0;
  int64_t tmp_ls_id = 0;

  OB_LOG(INFO, "get table_id");
  for (int i = 0; i < OB_DEFAULT_TABLE_COUNT; ++i) {
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select table_id from oceanbase.__all_virtual_table where table_name = 't%d'", i));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("table_id", tmp_table_id));
    }
    table_ids_.push_back(tmp_table_id);
    OB_LOG(INFO, "tmp_table_id", K(tmp_table_id));
  }

  OB_LOG(INFO, "get tablet_id");
  for (int i = 0; i < OB_DEFAULT_TABLE_COUNT; ++i) {
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select tablet_id from oceanbase.__all_virtual_tablet_to_ls where table_id = %ld", table_ids_.at(i)));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("tablet_id", tmp_tablet_id));
    }
    tablet_ids_.push_back(ObTabletID(tmp_tablet_id));
    OB_LOG(INFO, "tmp_tablet_id", K(tmp_tablet_id));
  }

  OB_LOG(INFO, "get ls_id");
  for (int i = 0; i < OB_DEFAULT_TABLE_COUNT; ++i) {
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select ls_id from oceanbase.__all_virtual_tablet_to_ls where table_id = %ld", table_ids_.at(i)));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("ls_id", tmp_ls_id));
    }
    ls_ids_.push_back(share::ObLSID(tmp_ls_id));
    OB_LOG(INFO, "tmp_ls_id", K(tmp_ls_id));
  }

  OB_LOG(INFO, "tablet_ids", K(tablet_ids_));
  OB_LOG(INFO, "ls_ids", K(ls_ids_));

  for (int i = 0; i < OB_DEFAULT_TABLE_COUNT; ++i) {
    ASSERT_EQ(true, tablet_ids_.at(i).is_valid());
    ASSERT_EQ(true, ls_ids_.at(i).is_valid());
  }
}

void ObMinorFreezeTest::all_virtual_minor_freeze_info()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  const int64_t start_time = ObTimeUtility::current_time();

  OB_LOG(INFO, "test __all_virtual_minor_freeze_info");
  while (ObTimeUtility::current_time() - start_time <= freeze_duration_) {
    for (int i = 0; i < OB_DEFAULT_TABLE_COUNT; ++i) {
      ObSqlString sql;
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select * from oceanbase.__all_virtual_minor_freeze_info where ls_id = %ld", ls_ids_.at(i).id()));
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
        sqlclient::ObMySQLResult *result = res.get_result();
        ASSERT_NE(nullptr, result);
        ASSERT_EQ(OB_SUCCESS, result->next());
      }
    }
  }

}

void ObMinorFreezeTest::check_frozen_memtable()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  const int64_t start_time = ObTimeUtility::current_time();

  int64_t memtable_cnt = 0;
  OB_LOG(INFO, "check_frozen_memtable");
  while (ObTimeUtility::current_time() - start_time <= freeze_duration_) {
    {
      memtable_cnt = 0;
      ObSqlString sql;
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as cnt from oceanbase.__all_virtual_memstore_info where is_active=\"NO\" and (unix_timestamp() - freeze_ts / 1000000) > %d;", freeze_duration_ / 2));
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
        sqlclient::ObMySQLResult *result = res.get_result();
        ASSERT_NE(nullptr, result);
        ASSERT_EQ(OB_SUCCESS, result->next());
        ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", memtable_cnt));
        ASSERT_EQ(0, memtable_cnt);
      }
    }

    {
      memtable_cnt = 0;
      ObSqlString sql;
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) as cnt from oceanbase.__all_virtual_memstore_info where is_active=\"NO\" and logging_blocked=\"YES\";"));
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
        sqlclient::ObMySQLResult *result = res.get_result();
        ASSERT_NE(nullptr, result);
        ASSERT_EQ(OB_SUCCESS, result->next());
        ASSERT_EQ(OB_SUCCESS, result->get_int("cnt", memtable_cnt));
        ASSERT_EQ(0, memtable_cnt);
      }
    }
  }
}

void ObMinorFreezeTest::get_ls()
{
  OB_LOG(INFO, "get_ls");
  for (int i = 0; i < OB_DEFAULT_TABLE_COUNT; ++i) {
    share::ObTenantSwitchGuard tenant_guard;
    ObLSHandle ls_handle;
    OB_LOG(INFO, "tenant_id", K(RunCtx.tenant_id_));
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(RunCtx.tenant_id_));
    ObLSService *ls_srv = MTL(ObLSService *);
    ASSERT_NE(nullptr, ls_srv);
    OB_LOG(INFO, "ls_id", K(ls_ids_.at(i)));
    ASSERT_EQ(OB_SUCCESS, ls_srv->get_ls(ls_ids_.at(i), ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(true, ls_handle.is_valid());
    ls_handles_.push_back(ls_handle);
  }
}

void ObMinorFreezeTest::insert_data(int start)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  const int64_t start_time = ObTimeUtility::current_time();

  OB_LOG(INFO, "insert data start");
  int i = 0;
  while (ObTimeUtility::current_time() - start_time <= freeze_duration_) {
    for (int j = 0; j < OB_DEFAULT_TABLE_COUNT; ++j) {
      ObSqlString sql;
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t%d values(%d, %d)", j, i + start, i + start));
      int64_t affected_rows = 0;
      ret = sql_proxy.write(sql.ptr(), affected_rows);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ++i;
  }
}

void ObMinorFreezeTest::tenant_freeze()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  const int64_t start = ObTimeUtility::current_time();

  OB_LOG(INFO, "test tenant_freeze");
  while (ObTimeUtility::current_time() - start <= freeze_duration_) {
    ObSqlString sql;
    int64_t affected_rows = 0;

    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system minor freeze tenant tt1"));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }
}

void ObMinorFreezeTest::logstream_freeze()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  share::ObTenantSwitchGuard tenant_guard;
  OB_LOG(INFO, "tenant_id", K(RunCtx.tenant_id_));
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(RunCtx.tenant_id_));
  int64_t i = 0;

  const int64_t start = ObTimeUtility::current_time();
  while (ObTimeUtility::current_time() - start <= freeze_duration_) {
    for (int j = 0; j < OB_DEFAULT_TABLE_COUNT; ++j) {
      int ret = OB_EAGAIN;
      while (OB_EAGAIN == ret) {
        ret = ls_handles_.at(j).get_ls()->logstream_freeze((i % 2 == 0) ? true : false);

        if (OB_EAGAIN == ret) {
          ob_usleep(rand() % SLEEP_TIME);
        }
      }
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    i = i + 1;
  }
}

void ObMinorFreezeTest::tablet_freeze()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  share::ObTenantSwitchGuard tenant_guard;
  OB_LOG(INFO, "tenant_id", K(RunCtx.tenant_id_));
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(RunCtx.tenant_id_));
  int64_t i = 0;

  const int64_t start = ObTimeUtility::current_time();
  while (ObTimeUtility::current_time() - start <= freeze_duration_) {
    for (int j = 0; j < OB_DEFAULT_TABLE_COUNT; ++j) {
      int ret = OB_EAGAIN;
      while (OB_EAGAIN == ret) {
        ret = ls_handles_.at(j).get_ls()->tablet_freeze(tablet_ids_.at(j), (i % 2 == 0) ? true : false);
        if (OB_EAGAIN == ret) {
          ob_usleep(rand() % SLEEP_TIME);
        }
      }
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    i = i + 1;
  }
}

void ObMinorFreezeTest::tablet_freeze_for_replace_tablet_meta()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  share::ObTenantSwitchGuard tenant_guard;
  OB_LOG(INFO, "tenant_id", K(RunCtx.tenant_id_));
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(RunCtx.tenant_id_));
  const int64_t start = ObTimeUtility::current_time();
  while (ObTimeUtility::current_time() - start <= freeze_duration_) {
    for (int j = 0; j < OB_DEFAULT_TABLE_COUNT; ++j) {
      ObTableHandleV2 handle;
      int ret = ls_handles_.at(j).get_ls()->get_freezer()->tablet_freeze_for_replace_tablet_meta(tablet_ids_.at(j), handle);
      if (OB_EAGAIN == ret || OB_ENTRY_EXIST == ret) {
        ret = OB_SUCCESS;
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      if (OB_SUCC(ret)) {
        ASSERT_EQ(OB_SUCCESS, ls_handles_.at(j).get_ls()->get_freezer()->handle_frozen_memtable_for_replace_tablet_meta(tablet_ids_.at(j), handle));
      }
    }
  }
}

void ObMinorFreezeTest::batch_tablet_freeze()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  share::ObTenantSwitchGuard tenant_guard;
  OB_LOG(INFO, "tenant_id", K(RunCtx.tenant_id_));
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(RunCtx.tenant_id_));
  int64_t i = 0;

  const int64_t start = ObTimeUtility::current_time();
  while (ObTimeUtility::current_time() - start <= freeze_duration_) {
    ASSERT_EQ(OB_SUCCESS, ls_handles_.at(0).get_ls()->batch_tablet_freeze(tablet_ids_, (i % 2 == 0) ? true : false));
    i = i + 1;
  }
}

void ObMinorFreezeTest::insert_and_freeze()
{
  std::thread tenant_freeze_thread([this]() { tenant_freeze(); });
  std::thread tablet_freeze_thread([this]() { tablet_freeze(); });
  std::thread logstream_freeze_thread([this]() { logstream_freeze(); });
  std::thread tablet_freeze_for_replace_tablet_meta_thread([this]() { tablet_freeze_for_replace_tablet_meta(); });
  std::thread check_frozen_memtable_thread([this]() { check_frozen_memtable(); });
  std::thread batch_tablet_freeze_thread([this]() { batch_tablet_freeze(); });
  std::vector<std::thread> insert_threads;
  for (int i = 0; i < insert_thread_num_; ++i) {
    int start = i * insert_num_;
    insert_threads.push_back(std::thread([this, start]() { insert_data(start); }));
  }

  tenant_freeze_thread.join();
  tablet_freeze_thread.join();
  logstream_freeze_thread.join();
  tablet_freeze_for_replace_tablet_meta_thread.join();
  check_frozen_memtable_thread.join();
  batch_tablet_freeze_thread.join();
  for (int i = 0; i < insert_thread_num_; ++i) {
    insert_threads[i].join();
  }
}

void ObMinorFreezeTest::empty_memtable_flush()
{
  SERVER_LOG(INFO, "start empty_memtable_flush");
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system minor freeze tenant tt1"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  usleep(100 * 1000); //100ms

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("set autocommit=0"));

  const int64_t start_time = ObTimeUtility::current_time();

  OB_LOG(INFO, "empty memtable flush start");
  int i = 0;
  while (ObTimeUtility::current_time() - start_time <= freeze_duration_) {
    if (i % 2 == 0) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("begin"));
      ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t1 values(%d, %d)", i, i));
      if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
        ret = OB_SUCCESS;
      }
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("rollback"));
      ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system minor freeze tenant tt1"));
      ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    } else {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("begin"));
      ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t1 values(%d, %d)", i, i));
      if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
        ret = OB_SUCCESS;
      }
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("commit"));
      ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
      ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system minor freeze tenant tt1"));
      ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    }
    ++i;
  }
}

void ObMinorFreezeTest::create_tables()
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  for (int i = 0; i < OB_DEFAULT_TABLE_COUNT; ++i) {
    OB_LOG(INFO, "create_table: ", K(i));
    ObSqlString sql;
    sql.assign_fmt("create table if not exists t%d (c1 int, c2 int, primary key(c1))", i);
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    OB_LOG(INFO, "create_table succ", K(i));
  }
}

TEST_F(ObMinorFreezeTest, observer_start)
{
  SERVER_LOG(INFO, "observer_start succ");
}

TEST_F(ObMinorFreezeTest, add_tenant)
{
  // 创建普通租户tt1
  ASSERT_EQ(OB_SUCCESS, create_tenant("tt1"));
  // 获取租户tt1的tenant_id
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(RunCtx.tenant_id_));
  ASSERT_NE(0, RunCtx.tenant_id_);
  // 初始化普通租户tt1的sql proxy
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
}

TEST_F(ObMinorFreezeTest, create_table)
{
  OB_LOG(INFO, "create_table start");
  create_tables();
  OB_LOG(INFO, "create_table all succ");
}

TEST_F(ObMinorFreezeTest, insert_and_freeze)
{
  get_tablet_id_and_ls_id();
  get_ls();
  insert_and_freeze();
  empty_memtable_flush();
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  system("rm -rf test_ob_minor_freeze.log*");
  system("rm -rf test_ob_minor_freeze_*");
  OB_LOGGER.set_log_level("INFO");
  GCONF._enable_defensive_check = false;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
