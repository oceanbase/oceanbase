// owner: lana.lgx
// owner group: data

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

#include "env/ob_simple_cluster_test_base.h"
#include "rootserver/ob_tenant_balance_service.h"
#include "share/balance/ob_balance_job_table_operator.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "storage/concurrency_control/ob_multi_version_garbage_collector.h"
#include "rootserver/mview/ob_mview_maintenance_service.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using namespace oceanbase::rootserver;
using namespace oceanbase::compaction;

#define EQ(x, y) GTEST_ASSERT_EQ(x, y);
#define NE(x, y) GTEST_ASSERT_NE(x, y);
#define LE(x, y) GTEST_ASSERT_LE(x, y);
#define GE(x, y) GTEST_ASSERT_GE(x, y);
#define GT(x, y) GTEST_ASSERT_GT(x, y);
#define LT(x, y) GTEST_ASSERT_LT(x, y);

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int64_t time_sec_ = 0;
  int64_t start_time_ = ObTimeUtil::current_time();
  bool stop_ = false;
  std::thread th_;
  std::thread worker_;
};

TestRunCtx R;

class ObCollectMV : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObCollectMV() : ObSimpleClusterTestBase("test_collect_mv_", "20G", "20G") {}

  int wait_major_mv_refresh_finish(uint64_t scn);
  int wait_acquired_snapshot_advance(uint64_t scn);
  int wait_old_sstable_gc_end(uint64_t scn);
  void process();
  int do_balance_inner_(uint64_t tenant_id);
private:
};

int ObCollectMV::wait_major_mv_refresh_finish(uint64_t scn)
{
  int ret = OB_SUCCESS;

  ObSqlString sql_string;
  sql_string.assign_fmt("select min(last_refresh_scn) val from oceanbase.__all_mview where refresh_mode=4");
  uint64_t val = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(SSH::g_select_uint64(R.tenant_id_, sql_string.ptr(), val))) {
    } else if (val >= scn) {
      break;
    } else {
      LOG_INFO("wait major mv refresh", K(scn), K(val));
      ::sleep(2);
    }
  }
  return ret;
}

int ObCollectMV::wait_acquired_snapshot_advance(uint64_t scn)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  sql_string.assign_fmt("select min(snapshot_scn) val from oceanbase.__all_acquired_snapshot where snapshot_type=5");
  uint64_t val = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(SSH::g_select_uint64(R.tenant_id_, sql_string.ptr(), val))) {
    } else if (val >= scn) {
      break;
    } else {
      LOG_INFO("wait acquired_snapshot_advance", K(scn), K(val));
      MTL_SWITCH(R.tenant_id_) {
        storage::ObTenantFreezeInfoMgr *mgr = MTL(storage::ObTenantFreezeInfoMgr *);
        const int64_t snapshot_for_tx = mgr->get_min_reserved_snapshot_for_tx();
        LOGI("wait_acquired_snapshot_advance: %ld %ld %ld", scn, snapshot_for_tx,val);
      }
      ::sleep(2);
    }
  }
  return ret;
}

int ObCollectMV::wait_old_sstable_gc_end(uint64_t scn)
{
  int ret = OB_SUCCESS;
  ObSqlString sql_string;
  sql_string.assign_fmt("select min(end_log_scn) val from oceanbase.__all_virtual_table_mgr where table_type=10 and tablet_id>200000");
  uint64_t val = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(SSH::g_select_uint64(R.tenant_id_, sql_string.ptr(), val))) {
    } else if (val >= scn) {
      break;
    } else {
      LOG_INFO("wait old sstable gc", K(scn), K(val));
      ::sleep(2);
    }
  }
  return ret;
}

void ObCollectMV::process()
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(R.tenant_id_) {
    ObMViewMaintenanceService *mv_service = MTL(ObMViewMaintenanceService*);
    mv_service->mview_push_refresh_scn_task_.runTimerTask();
    mv_service->mview_push_snapshot_task_.runTimerTask();
    mv_service->replica_safe_check_task_.runTimerTask();
    mv_service->collect_mv_merge_info_task_.runTimerTask();
    mv_service->mview_clean_snapshot_task_.runTimerTask();
    MTL(ObTenantTabletScheduler*)->timer_task_mgr_.sstable_gc_task_.runTimerTask();
  }
}

int ObCollectMV::do_balance_inner_(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  static std::mutex mutex;
  mutex.lock();
  MTL_SWITCH(tenant_id) {
    LOG_INFO("worker to do partition_balance");
    auto b_svr = MTL(rootserver::ObTenantBalanceService*);
    b_svr->reset();
    b_svr->stop();
    int64_t job_cnt = 0;
    int64_t start_time = OB_INVALID_TIMESTAMP, finish_time = OB_INVALID_TIMESTAMP;
    ObBalanceJob job;
    if (OB_FAIL(b_svr->gather_stat_())) {
      LOG_WARN("failed to gather stat", KR(ret));
    } else if (OB_FAIL(b_svr->gather_ls_status_stat(tenant_id, b_svr->ls_array_))) {
      LOG_WARN("failed to gather stat", KR(ret));
    } else if (OB_FAIL(ObBalanceJobTableOperator::get_balance_job(
                   tenant_id, false, *GCTX.sql_proxy_, job, start_time, finish_time))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        //NO JOB, need check current ls status
        ret = OB_SUCCESS;
        job_cnt = 0;
      } else {
        LOG_WARN("failed to get balance job", KR(ret), K(tenant_id));
      }
    } else if (OB_FAIL(b_svr->try_finish_current_job_(job, job_cnt))) {
      LOG_WARN("failed to finish current job", KR(ret), K(job));
    }
    if (OB_SUCC(ret) && job_cnt == 0 && OB_FAIL(b_svr->partition_balance_(true))) {
      LOG_WARN("failed to do partition balance", KR(ret));
    }
  }
  mutex.unlock();
  return ret;

}


TEST_F(ObCollectMV, prepare)
{
  int ret = OB_SUCCESS;
  LOGI("observer start");
  R.th_ = std::thread([]() {
      while (!R.stop_) {
        fflush(stdout);
        ::usleep(100 * 1000);
      }});
  concurrency_control::ObMultiVersionGarbageCollector::GARBAGE_COLLECT_EXEC_INTERVAL = 1_s;
  concurrency_control::ObMultiVersionGarbageCollector::GARBAGE_COLLECT_RETRY_INTERVAL = 3_s;
  concurrency_control::ObMultiVersionGarbageCollector::GARBAGE_COLLECT_RECLAIM_DURATION = 3_s;

  LOGI("create tenant begin");
  // 创建普通租户tt1
  EQ(OB_SUCCESS, create_tenant("tt1", "12G", "16G", false, 10));
  // 获取租户tt1的tenant_id
  EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_));
  NE(0, R.tenant_id_);
  // 初始化普通租户tt1的sql proxy
  EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  int tenant_id = R.tenant_id_;
  LOGI("create tenant finish");

  // 在单节点ObServer下创建新的日志流, 注意避免被RS任务GC掉
  EQ(0, SSH::create_ls(R.tenant_id_, get_curr_observer().self_addr_));
  int64_t ls_count = 0;
  EQ(0, SSH::g_select_int64(R.tenant_id_, "select count(ls_id) as val from oceanbase.__all_ls where ls_id!=1", ls_count));
  EQ(2, ls_count);

  int64_t affected_rows;
  EQ(0, GCTX.sql_proxy_->write("alter system set _enable_parallel_table_creation = false tenant=all", affected_rows));
  EQ(0, GCTX.sql_proxy_->write("alter system set undo_retention='0' tenant=all", affected_rows));
  EQ(0, GCTX.sql_proxy_->write("alter system set ob_compaction_schedule_interval='5s' tenant=all", affected_rows));
  EQ(0, GCTX.sql_proxy_->write("alter system set merger_check_interval='10s' tenant=all", affected_rows));

  MTL_SWITCH(R.tenant_id_) {
    TG_STOP(MTL(ObTenantTabletScheduler *)->timer_task_mgr_.sstable_gc_tg_id_);
  }

  R.worker_ = std::thread([this]() {
      while (!R.stop_) {
        process();
        do_balance_inner_(R.tenant_id_);
        ::sleep(3);
      }});
}

TEST_F(ObCollectMV, read)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows;
  int ret = OB_SUCCESS;
  EQ(0, sql_proxy.write("create table t(c1 int)", affected_rows));

  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0"));
  EQ(0, SSH::write(conn, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"));
  EQ(0, SSH::write(conn, "set ob_query_timeout=99900000000"));
  EQ(0, SSH::write(conn, "set ob_trx_timeout=1000000000"));
  int64_t val = 0;
  EQ(0, SSH::select_int64(conn, "select count(*) val from t", val));
  ObString trace_id;
  ObTransID tx_id;
  int64_t session_id = -1;
  EQ(0, SSH::find_session(conn, session_id));
  EQ(0, SSH::find_trace_id(conn, trace_id));
  EQ(0, SSH::find_tx(conn, tx_id));
  ObTxDesc *tx_desc = NULL;
  EQ(0, SSH::get_tx_desc(R.tenant_id_, tx_id, tx_desc));
  uint64_t read_snapshot = tx_desc->snapshot_version_.get_val_for_gts();
  int tx_state = (int)tx_desc->state_;
  EQ(0, SSH::revert_tx_desc(R.tenant_id_, tx_desc));
  GT(read_snapshot, 0);
  LOGI("session_id:%ld tx_id:%ld read_snapshot:%ld %d", session_id, tx_id.tx_id_,read_snapshot, tx_state);
  bool wait_end = false;
  while (OB_SUCC(ret) && !wait_end) {
    ::sleep(2);
    MTL_SWITCH(R.tenant_id_)  {
      storage::ObTenantFreezeInfoMgr *mgr = MTL(storage::ObTenantFreezeInfoMgr *);
      const int64_t snapshot_for_tx = mgr->get_min_reserved_snapshot_for_tx();
      LOGI("snapshot_for_tx:%ld", snapshot_for_tx);
      if (snapshot_for_tx == read_snapshot) {
        wait_end = true;
      } else if (snapshot_for_tx > read_snapshot) {
        ret = OB_ERR_UNEXPECTED;
        LOGE("snapshot_for_tx big than read_snapshot: %ld %ld", snapshot_for_tx, read_snapshot);
      }
    }
  }
  EQ(OB_SUCCESS, ret);
  EQ(0, SSH::select_int64(conn, "select count(*) val from t", val));
  EQ(0, conn->commit());
  EQ(0, sql_proxy.close(conn, true));
}

TEST_F(ObCollectMV, basic)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;
  LOGI("create tg");
  EQ(0, sql_proxy.write("create tablegroup tg1 sharding='PARTITION'", affected_rows));
  LOGI("create table");
  EQ(0, sql_proxy.write("create table t1(c1 int,c2 int, primary key(c1,c2)) tablegroup='tg1' partition by hash(c1) partitions 2", affected_rows));
  EQ(0, sql_proxy.write("create table t2(c2 int, c3 int, primary key(c2)) duplicate_scope = 'cluster' duplicate_read_consistency='weak'", affected_rows));
  LOGI("create mview");
  EQ(0, sql_proxy.write(
            "create materialized view compact_mv_1 (primary key(t1_c1,t1_c2)) tablegroup='tg1' "
            "partition by hash(t1_c1) partitions 2 "
            "REFRESH FAST ON DEMAND  ENABLE QUERY REWRITE ENABLE ON QUERY COMPUTATION "
            "as select /*+read_consistency(weak) use_nl(t1 t2) leading(t1 t2) "
            "use_das(t2) no_use_nl_materialization(t2)*/ t1.c1 t1_c1,t1.c2 t1_c2,t2.c2 t2_c2,t2.c3 t2_c3 "
            "from t1 join t2 on t1.c2=t2.c2",
            affected_rows));
  LOGI("check refresh_mode");
  int64_t refresh_mode = 0;
  EQ(0, SSH::g_select_int64(R.tenant_id_, "select refresh_mode val from oceanbase.__all_mview t1, oceanbase.__all_table t2 where t1.mview_id=t2.table_id and t2.table_name='compact_mv_1'", refresh_mode));
  EQ(4, refresh_mode);
  LOGI("get refresh_scn");
  uint64_t refresh_scn = 0;
  EQ(0, SSH::g_select_uint64(R.tenant_id_, "select last_refresh_scn val from oceanbase.__all_mview t1, oceanbase.__all_table t2 where t1.mview_id=t2.table_id and t2.table_name='compact_mv_1'", refresh_scn));
  GT(refresh_scn, 0);

  ObSqlString sql_string;
  int64_t now = ObTimeUtil::current_time_ns();
  sql_string.assign_fmt("select /*+no_mv_rewrite*/ count(*) val from compact_mv_1 as of snapshot %ld", now);
  int64_t val = 0;
  EQ(0, SSH::select_int64(sql_proxy, sql_string.ptr(), val));

  LOGI("start major freeze>>>");
  EQ(0, sql_proxy.write("alter system major freeze", affected_rows));
  uint64_t new_freeze_scn = 0;
  EQ(0, SSH::g_select_uint64(R.tenant_id_, "select max(frozen_scn) val from oceanbase.__all_freeze_info", new_freeze_scn));
  GT(new_freeze_scn, refresh_scn);

  LOGI("wait major mv refresh finish>>>");
  EQ(0, wait_major_mv_refresh_finish(new_freeze_scn));
  LOGI("wait acquired snapshot advance>>>");
  EQ(0, wait_acquired_snapshot_advance(new_freeze_scn));
  LOGI("wait old sstable gc>>>");
  EQ(0, wait_old_sstable_gc_end(new_freeze_scn));

  NE(0, SSH::select_int64(sql_proxy, sql_string.ptr(), affected_rows));
  sql_string.assign_fmt("select /*+no_mv_rewrite*/ count(*) val from compact_mv_1 as of snapshot %ld", new_freeze_scn);
  EQ(0, SSH::select_int64(sql_proxy, sql_string.ptr(), val));
}

TEST_F(ObCollectMV, read_safe)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows;
  int ret = OB_SUCCESS;

  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0"));
  EQ(0, SSH::write(conn, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"));
  EQ(0, SSH::write(conn, "set ob_query_timeout=99900000000"));
  EQ(0, SSH::write(conn, "set ob_trx_timeout=1000000000"));
  int64_t val = 0;
  EQ(0, SSH::select_int64(conn, "select count(*) val from compact_mv_1", val));
  ObString trace_id;
  ObTransID tx_id;
  EQ(0, SSH::find_trace_id(conn, trace_id));
  EQ(0, SSH::find_tx(conn, tx_id));
  ObTxDesc *tx_desc = NULL;
  EQ(0, SSH::get_tx_desc(R.tenant_id_, tx_id, tx_desc));
  uint64_t read_snapshot = tx_desc->snapshot_version_.get_val_for_gts();
  EQ(0, SSH::revert_tx_desc(R.tenant_id_, tx_desc));
  LOGI("read trans stmt trace_id: %s tx_id: %ld snapshot: %ld", trace_id.ptr(), tx_id.tx_id_, read_snapshot);

  LOGI("start major freeze >>>");
  EQ(0, sql_proxy.write("alter system major freeze", affected_rows));
  uint64_t new_freeze_scn = 0;
  EQ(0, SSH::g_select_uint64(R.tenant_id_, "select max(frozen_scn) val from oceanbase.__all_freeze_info", new_freeze_scn));
  LOGI("wait major mv refresh finish >>>");
  EQ(0, wait_major_mv_refresh_finish(new_freeze_scn));

  LOGI("read with old read_snapshot");
  EQ(0, SSH::select_int64(conn, "select count(*) val from compact_mv_1", val));
  EQ(0, SSH::find_trace_id(conn, trace_id));
  LOGI("read trans stmt trace_id: %s", trace_id.ptr());
  ::sleep(30);

  int64_t snapshot_for_tx = 0;
  MTL_SWITCH(R.tenant_id_) {
    storage::ObTenantFreezeInfoMgr *mgr = MTL(storage::ObTenantFreezeInfoMgr *);
    snapshot_for_tx = mgr->get_min_reserved_snapshot_for_tx();
  }
  EQ(OB_SUCCESS, ret);
  LOGI("read with old read_snapshot snapshot_for_tx:%ld read_snapshot:%ld", snapshot_for_tx, read_snapshot);
  LE(snapshot_for_tx, read_snapshot);
  EQ(0, SSH::select_int64(conn, "select count(*) val from compact_mv_1", val));
  EQ(0, SSH::find_trace_id(conn, trace_id));
  LOGI("read trans stmt trace_id: %s", trace_id.ptr());
  EQ(0, conn->commit());
  EQ(0, sql_proxy.close(conn, true));
  LOGI("wait old sstable gc>>>");
  EQ(0, wait_old_sstable_gc_end(new_freeze_scn));
}

TEST_F(ObCollectMV, snapshot_gc)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ObSqlString sql_string;
  int64_t affected_rows = 0;
  int64_t base_tablet_id = 0;
  int64_t cnt = 0;
  int64_t snapshot_val = 0;

  LOGI("create tg");
  EQ(0, sql_proxy.write("create tablegroup gc_tg1 sharding='PARTITION'", affected_rows));
  LOGI("create table");
  EQ(0, sql_proxy.write("create table gc_t1(c1 int,c2 int, primary key(c1,c2)) tablegroup='gc_tg1' "
                        "partition by hash(c1) partitions 2",
                        affected_rows));
  EQ(0, sql_proxy.write("create table gc_t2(c2 int, c3 int, primary key(c2)) duplicate_scope = "
                        "'cluster' duplicate_read_consistency='weak'",
                        affected_rows));
  EQ(0, SSH::select_int64(
            sql_proxy, "select t2.tablet_id val from oceanbase.__all_table t1 join oceanbase.__all_tablet_to_ls t2 on t1.table_id = t2.table_id where t1.table_name = 'gc_t1' limit 1",
            base_tablet_id));

  LOGI("create mview1");
  int64_t mv1_tablet_id = 0;
  int64_t snapshot_scn_of_base_by_mv1 = 0;
  EQ(0, sql_proxy.write(
            "create materialized view gc_compact_mv_1 (primary key(t1_c1,t1_c2)) tablegroup='gc_tg1' "
            "partition by hash(t1_c1) partitions 2 "
            "REFRESH FAST ON DEMAND  ENABLE QUERY REWRITE ENABLE ON QUERY COMPUTATION "
            "as select /*+read_consistency(weak) use_nl(t1 t2) leading(t1 t2) "
            "use_das(t2) no_use_nl_materialization(t2)*/ t1.c1 t1_c1,t1.c2 t1_c2,t2.c2 t2_c2,t2.c3 t2_c3 "
            "from gc_t1 t1 join gc_t2 t2 on t1.c2=t2.c2",
            affected_rows));
  EQ(0, SSH::select_int64(
            sql_proxy,
            "select t3.tablet_id val from oceanbase.__all_table t1, oceanbase.__all_table t2, oceanbase.__all_tablet_to_ls t3 where "
            "t1.table_id = t2.data_table_id and t2.table_name='gc_compact_mv_1' and t1.table_id = t3.table_id limit 1",
            mv1_tablet_id));
  sql_string.assign_fmt(
      "select count(*) val from oceanbase.__all_acquired_snapshot where tablet_id = %ld",
      mv1_tablet_id);
  EQ(0, SSH::select_int64(sql_proxy, sql_string.ptr(), cnt));
  EQ(1, cnt);
  sql_string.assign_fmt(
      "select snapshot_scn val from oceanbase.__all_acquired_snapshot where tablet_id = %ld",
      base_tablet_id);
  EQ(0, SSH::select_int64(sql_proxy, sql_string.ptr(), snapshot_scn_of_base_by_mv1));

  LOGI("create mview2");
  int64_t mv2_tablet_id = 0;
  EQ(0, sql_proxy.write(
            "create materialized view gc_compact_mv_2 (primary key(t1_c1,t1_c2)) tablegroup='gc_tg1' "
            "partition by "
            "hash(t1_c1) partitions 2 REFRESH FAST ON DEMAND  ENABLE QUERY REWRITE ENABLE ON QUERY "
            "COMPUTATION as select /*+read_consistency(weak) use_nl(t1 t2) leading(t1 t2) "
            "use_das(t2) no_use_nl_materialization(t2)*/ t1.c1 t1_c1,t1.c2 t1_c2,t2.c2 t2_c2,t2.c3 t2_c3 "
            "from gc_t1 t1 join gc_t2 t2 on t1.c2=t2.c2",
            affected_rows));
  EQ(0, SSH::select_int64(
            sql_proxy,
            "select t3.tablet_id val from oceanbase.__all_table t1, oceanbase.__all_table t2, oceanbase.__all_tablet_to_ls t3 where "
            "t1.table_id = t2.data_table_id and t2.table_name='gc_compact_mv_2' and t1.table_id = t3.table_id limit 1",
            mv2_tablet_id));
  sql_string.assign_fmt(
      "select count(*) val from oceanbase.__all_acquired_snapshot where tablet_id = %ld", mv2_tablet_id);
  EQ(0, SSH::select_int64(sql_proxy, sql_string.ptr(), cnt));
  EQ(1, cnt);

  LOGI("create mview3");
  int64_t mv3_tablet_id = 0;
  EQ(0, sql_proxy.write(
            "create materialized view gc_compact_mv_3 (primary key(t1_c1,t1_c2)) tablegroup='gc_tg1' "
            "partition by hash(t1_c1) partitions 2 "
            "REFRESH FAST ON DEMAND  ENABLE QUERY REWRITE ENABLE ON QUERY COMPUTATION "
            "as select /*+read_consistency(weak) use_nl(t1 t2) leading(t1 t2) "
            "use_das(t2) no_use_nl_materialization(t2)*/ t1.c1 t1_c1,t1.c2 t1_c2,t2.c2 t2_c2,t2.c3 t2_c3 "
            "from gc_t1 t1 join gc_t2 t2 on t1.c2=t2.c2",
            affected_rows));
  EQ(0, SSH::select_int64(
            sql_proxy,
            "select t3.tablet_id val from oceanbase.__all_table t1, oceanbase.__all_table t2, oceanbase.__all_tablet_to_ls t3 where "
            "t1.table_id = t2.data_table_id and t2.table_name='gc_compact_mv_3' and t1.table_id = t3.table_id limit 1",
            mv3_tablet_id));
  sql_string.assign_fmt(
      "select count(*) val from oceanbase.__all_acquired_snapshot where tablet_id = %ld", mv3_tablet_id);
  EQ(0, SSH::select_int64(sql_proxy, sql_string.ptr(), cnt));
  EQ(1, cnt);

  while (true) {
    sql_string.assign_fmt(
        "select count(*) val from oceanbase.__all_acquired_snapshot where tablet_id = %ld",
        base_tablet_id);
    EQ(0, SSH::select_int64(sql_proxy, sql_string.ptr(), cnt));
    if (cnt == 1) {
      break;
    }
    ::sleep(2);
    LOGI("wait remove redundant snapshot");
  }
  sql_string.assign_fmt(
      "select snapshot_scn val from oceanbase.__all_acquired_snapshot where tablet_id = %ld",
      base_tablet_id);
  EQ(0, SSH::select_int64(sql_proxy, sql_string.ptr(), snapshot_val));
  // 清除冗余的快照后，留下的应该是最小的快照
  EQ(snapshot_scn_of_base_by_mv1, snapshot_val);

  LOGI("remove mview1");
  EQ(0, sql_proxy.write("drop materialized view gc_compact_mv_1", affected_rows));
  while (true) {
    sql_string.assign_fmt(
        "select count(*) val from oceanbase.__all_acquired_snapshot where tablet_id = %ld",
        mv1_tablet_id);
    EQ(0, SSH::select_int64(sql_proxy, sql_string.ptr(), cnt));
    if (cnt == 0) {
      break;
    }
    ::sleep(2);
    LOGI("wait remove snapshot of mview1");
  }
  sql_string.assign_fmt(
      "select count(*) val from oceanbase.__all_acquired_snapshot where tablet_id = %ld",
      base_tablet_id);
  EQ(0, SSH::select_int64(sql_proxy, sql_string.ptr(), cnt));
  // 基表的快照还不应该被回收掉
  EQ(1, cnt);

  LOGI("remove mview2 and mview3");
  EQ(0, sql_proxy.write("drop materialized view gc_compact_mv_2", affected_rows));
  EQ(0, sql_proxy.write("drop materialized view gc_compact_mv_3", affected_rows));
  while (true) {
    sql_string.assign_fmt("select count(*) val from oceanbase.__all_acquired_snapshot where "
                          "tablet_id = %ld or tablet_id = %ld",
                          mv2_tablet_id, mv3_tablet_id);
    EQ(0, SSH::select_int64(sql_proxy, sql_string.ptr(), cnt));
    if (cnt == 0) {
      break;
    }
    ::sleep(2);
    LOGI("wait remove snapshot of mview2 and mview3");
  }
  sql_string.assign_fmt(
      "select count(*) val from oceanbase.__all_acquired_snapshot where tablet_id = %ld",
      base_tablet_id);
  EQ(0, SSH::select_int64(sql_proxy, sql_string.ptr(), cnt));
  // 基表的快照应该被回收掉了
  EQ(0, cnt);

  LOGI("clean up");
  EQ(0, sql_proxy.write("drop table gc_t1", affected_rows));
  EQ(0, sql_proxy.write("drop table gc_t2", affected_rows));
}

TEST_F(ObCollectMV, tablegroup)
{
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t affected_rows = 0;

  EQ(0, sql_proxy.write("create table t100(c1 int) partition by hash(c1) partitions 2", affected_rows));
  EQ(0, sql_proxy.write("create tablegroup tg100 sharding='PARTITION'", affected_rows));
  EQ(0, sql_proxy.write("create materialized view mv100 partition by hash(c1) partitions 2 as select * from t100", affected_rows));

  EQ(0, sql_proxy.write("alter table t100 tablegroup='tg100'", affected_rows));
  EQ(0, sql_proxy.write("alter table mv100 tablegroup='tg100'", affected_rows));

  int64_t val = -2;
  EQ(0, SSH::select_int64(sql_proxy, "select tablegroup_id val from oceanbase.__all_table where table_name='t100'", val));
  GT(val, 0);
  // mview
  val = -2;
  EQ(0, SSH::select_int64(sql_proxy, "select tablegroup_id val from oceanbase.__all_table where table_name='mv100'", val));
  EQ(val, -1);
  //mview container
  val = -2;
  EQ(0, SSH::select_int64(sql_proxy, "select tablegroup_id val from oceanbase.__all_table where table_id in ( \
          select data_table_id from oceanbase.__all_table where table_name='mv100')", val));
  GT(val, 0);

  EQ(0, sql_proxy.write("alter table mv100 tablegroup=''", affected_rows));
  EQ(0, sql_proxy.write("alter table t100 tablegroup=''", affected_rows));

  EQ(0, sql_proxy.write("drop tablegroup tg100", affected_rows));
  EQ(0, sql_proxy.write("drop table t100", affected_rows));
  EQ(0, sql_proxy.write("drop materialized view mv100", affected_rows));
}

TEST_F(ObCollectMV, new_ls)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  int64_t val = 0;
  EQ(0, SSH::select_int64(sql_proxy, "select column_value val from oceanbase.__all_core_table where column_name='major_refresh_mv_merge_scn'", val));
  GT(val, 0);
  int64_t merge_scn = val;

  EQ(0, SSH::create_ls(R.tenant_id_, get_curr_observer().self_addr_));

  EQ(0, SSH::select_int64(sql_proxy, "select max(ls_id) val from oceanbase.__all_ls", val));
  ObLSID ls_id(val);

  ObLSMeta ls_meta;
  MTL_SWITCH(R.tenant_id_) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else if (OB_FAIL(ls_handle.get_ls()->get_ls_meta(ls_meta))) {
    }
  }
  EQ(0, ret);
  EQ(ls_meta.major_mv_merge_info_.major_mv_merge_scn_.get_val_for_gts(), merge_scn);
}

TEST_F(ObCollectMV, mv_transfer)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  EQ(0, sql_proxy.write("create table tf_t1(c1 int,c2 int, primary key(c1,c2)) "
                        "partition by hash(c1) partitions 2",
                        affected_rows));
  EQ(0, sql_proxy.write("create table tf_t2(c2 int, c3 int, primary key(c2)) duplicate_scope = "
                        "'cluster' duplicate_read_consistency='weak'",
                        affected_rows));
  EQ(0, sql_proxy.write(
            "create materialized view tf_compact_mv_1 (primary key(t1_c1,t1_c2)) tablegroup='tg1' "
            "partition by hash(t1_c1) partitions 2 "
            "REFRESH FAST ON DEMAND  ENABLE QUERY REWRITE ENABLE ON QUERY COMPUTATION "
            "as select /*+read_consistency(weak) use_nl(t1 t2) leading(t1 t2) "
            "use_das(t2) no_use_nl_materialization(t2)*/ t1.c1 t1_c1,t1.c2 t1_c2,t2.c2 t2_c2,t2.c3 "
            "t2_c3 "
            "from tf_t1 t1 join tf_t2 t2 on t1.c2=t2.c2",
            affected_rows));
  EQ(0, sql_proxy.write("create tablegroup tg200 sharding='NONE'", affected_rows));
  EQ(0, sql_proxy.write("alter table tf_compact_mv_1 tablegroup='tg200'", affected_rows));
  int64_t val = -1;
  while (true) {
    EQ(0, SSH::select_int64(sql_proxy, "select count(distinct ls_id) val from oceanbase.__all_tablet_to_ls where table_id in (select data_table_id from oceanbase.__all_table where table_name='tf_compact_mv_1')", val));
    if (val == 1) {
      break;
    } else {
      LOGI("wait transfer finish:%ld",val);
      ::sleep(1);
    }
  }
}

//TEST_F(ObCollectMV, create_mview_and_major_merge_concurrent)
//{
//  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
//  int64_t affected_rows = 0;
//  LOGI("create tg");
//  EQ(0, sql_proxy.write("create tablegroup tg1000 sharding='PARTITION'", affected_rows));
//  LOGI("create table");
//  EQ(0, sql_proxy.write("create table t1000(c1 int,c2 int, primary key(c1,c2)) tablegroup='tg1000' partition by hash(c1) partitions 2", affected_rows));
//  EQ(0, sql_proxy.write("create table t2000(c2 int, c3 int, primary key(c2)) duplicate_scope = 'cluster' duplicate_read_consistency='weak'", affected_rows));
//
//  LOGI("insert data");
//  EQ(0, sql_proxy.write("insert into t1000 values(1, 1)", affected_rows));;
//  EQ(0, sql_proxy.write("insert into t2000 values(1, 1000)", affected_rows));;
//  int64_t start_time = ObTimeUtil::current_time();
//  while (ObTimeUtil::current_time() - start_time < 10 * 1000 * 1000) {
//    ObSqlString sql;
//    int64_t val = 0;
//    EQ(0, SSH::select_int64(sql_proxy, "select max(c1) val from t1000", val));
//    EQ(0, sql.assign_fmt("insert into t1000 select c1+%ld,c2+%ld from t1000", val, val));
//    EQ(0, sql_proxy.write(sql.ptr(), affected_rows));;
//    EQ(0, sql.assign_fmt("insert into t2000 select c2+%ld,c3 from t2000", val));
//    EQ(0, sql_proxy.write(sql.ptr(), affected_rows));;
//  }
//  int64_t row_count = 0;
//  EQ(0, SSH::select_int64(sql_proxy, "select count(*) val from t1000", row_count));
//
//  uint64_t new_freeze_scn = 0;
//  std::thread th([&]() {
//    ::sleep(1);
//    LOGI("major freeze")
//    sql_proxy.write("alter system major freeze", affected_rows);
//    EQ(0, SSH::g_select_uint64(R.tenant_id_, "select max(frozen_scn) val from oceanbase.__all_freeze_info", new_freeze_scn));
//  });
//
//  LOGI("create mview start");
//  EQ(0, sql_proxy.write(
//            "create materialized view compact_mv_1000 (primary key(t1000_c1,t1000_c2)) tablegroup='tg1000' "
//            "partition by hash(t1000_c1) partitions 2 "
//            "REFRESH FAST ON DEMAND  ENABLE QUERY REWRITE ENABLE ON QUERY COMPUTATION "
//            "as select /*+read_consistency(weak) use_nl(t1000 t2000) leading(t1000 t2000) "
//            "use_das(t2000) no_use_nl_materialization(t2000)*/ t1000.c1 t1000_c1,t1000.c2 t1000_c2,t2000.c2 t2000_c2,t2000.c3 t2000_c3 "
//            "from t1000 join t2000 on t1000.c2=t2000.c2",
//            affected_rows));
//  LOGI("create mview finish");
//  int64_t mv_row_count = 0;
//  EQ(0, SSH::select_int64(sql_proxy, "select /*+no_mv_rewrite*/count(*) val from compact_mv_1000", mv_row_count));
//  EQ(row_count, mv_row_count);
//  LOGI("row_count:%ld %ld", row_count, mv_row_count);
//  th.join();
//  LOGI("wait major merge %ld", new_freeze_scn);
//  EQ(0, wait_major_mv_refresh_finish(new_freeze_scn));
//  LOGI("wait major merge finish %ld", new_freeze_scn);
//
//
//  LOGI("mview fast_refresh");
//  int64_t max_val = 0;
//  EQ(0, SSH::select_int64(sql_proxy, "select max(c1) val from t1000", max_val));
//  ObSqlString sql;
//  sql.assign_fmt("insert into t1000 values(%ld, %ld)", max_val+1, max_val+1);
//  EQ(0, sql_proxy.write(sql.ptr(), affected_rows));
//  sql.assign_fmt("insert into t2000 values(%ld, 1000)", max_val+1);
//  EQ(0, sql_proxy.write(sql.ptr(), affected_rows));
//  EQ(0, sql_proxy.write("update t2000 set c3=2000 where c2<1000", affected_rows));
//  EQ(0, sql_proxy.write("delete from t1000 where c1<1000", affected_rows));
//
//
//  EQ(0, sql_proxy.write("alter system major freeze", affected_rows));
//  EQ(0, SSH::g_select_uint64(R.tenant_id_, "select max(frozen_scn) val from oceanbase.__all_freeze_info", new_freeze_scn));
//  EQ(0, wait_major_mv_refresh_finish(new_freeze_scn));
//
//  EQ(0, SSH::select_int64(sql_proxy, "select count(*) val from t1000", row_count));
//  EQ(0, SSH::select_int64(sql_proxy, "select /*+no_mv_rewrite*/count(*) val from compact_mv_1000", mv_row_count));
//  EQ(row_count, mv_row_count);
//  LOGI("row_count:%ld %ld", row_count, mv_row_count);
//}

TEST_F(ObCollectMV, end)
{
  int64_t wait_us = R.time_sec_ * 1000 * 1000;
  while (ObTimeUtil::current_time() - R.start_time_ < wait_us) {
    ob_usleep(1000 * 1000);
  }
  R.stop_ = true;
  R.th_.join();
  R.worker_.join();
}

} // end unittest
} // end oceanbase


int main(int argc, char **argv)
{
  int c = 0;
  int64_t time_sec = 0;
  char *log_level = (char*)"INFO";
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

  LOG_INFO("main>>>");

  oceanbase::unittest::R.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
