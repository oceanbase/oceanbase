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
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/init_basic_struct.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "rootserver/ob_tenant_balance_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "share/balance/ob_balance_job_table_operator.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "rootserver/ob_balance_group_ls_stat_operator.h"
#include "storage/tablet/ob_tablet.h"
#include "logservice/ob_log_service.h"
#include "mittest/env/ob_simple_server_helper.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

#define EQ(x, y) GTEST_ASSERT_EQ(x, y);
#define NEQ(x, y) GTEST_ASSERT_NE(x, y);
#define LE(x, y) GTEST_ASSERT_LE(x, y);
#define GE(x, y) GTEST_ASSERT_GE(x, y);

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int64_t time_sec_ = 0;
  int64_t start_time_ = ObTimeUtil::current_time();
  bool stop_ = false;
  bool stop_balance_ = false;
  std::thread worker_;
};

TestRunCtx R;

class ObTransferTx : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObTransferTx() : ObSimpleClusterTestBase("test_transfer_tx_", "50G", "50G") {}
  int do_balance(uint64_t tenant_id);
private:
  int do_balance_inner_(uint64_t tenant_id);
  int do_transfer_start_abort(uint64_t tenant_id, ObLSID dest_ls_id, ObLSID src_ls_id, ObTransferTabletInfo tablet_info);
  int wait_balance_clean(uint64_t tenant_id);
};

int ObTransferTx::do_balance_inner_(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  static std::mutex mutex;
  mutex.lock();
  MTL_SWITCH(tenant_id) {
    LOG_INFO("worker to do partition_balance");
    auto b_svr = MTL(rootserver::ObTenantBalanceService*);
    b_svr->reset();
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
    if (OB_SUCC(ret) && job_cnt == 0 && !R.stop_balance_ && OB_FAIL(b_svr->partition_balance_(true))) {
      LOG_WARN("failed to do partition balance", KR(ret));
    }
  }
  mutex.unlock();
  return ret;
}

int ObTransferTx::wait_balance_clean(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t begin_time = ObTimeUtil::current_time();
  while (OB_SUCC(ret)) {
    bool is_clean = false;
    MTL_SWITCH(tenant_id) {
      ObBalanceJob job;
      int64_t start_time = OB_INVALID_TIMESTAMP, finish_time = OB_INVALID_TIMESTAMP;
      if (OB_FAIL(ObBalanceJobTableOperator::get_balance_job(
                     tenant_id, false, *GCTX.sql_proxy_, job, start_time, finish_time))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          is_clean = true;
        }
      } else {
        ob_usleep(200 * 1000);
      }
    }
    if (is_clean) {
      int64_t transfer_task_count = 0;
      if (OB_FAIL(SSH::g_select_int64(tenant_id, "select count(*) as val from __all_transfer_task", transfer_task_count))) {
      } else if (transfer_task_count == 0) {
        break;
      } else {
        ob_usleep(200 * 1000);
      }
    }
  }
  return ret;
}

int ObTransferTx::do_balance(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_balance_inner_(tenant_id))) {
  } else if (OB_FAIL(do_balance_inner_(tenant_id))) {
  }
  return ret;
}

int ObTransferTx::do_transfer_start_abort(uint64_t tenant_id, ObLSID dest_ls_id, ObLSID src_ls_id, ObTransferTabletInfo tablet_info)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    ObTransferHandler *transfer_handler = NULL;
    ObTransferTaskInfo task_info;
    ObMySQLTransaction trans;
    ObTimeoutCtx timeout_ctx;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(src_ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else if (FALSE_IT(transfer_handler = ls_handle.get_ls()->get_transfer_handler())) {
    } else if (FALSE_IT(task_info.tenant_id_ = tenant_id)) {
    } else if (FALSE_IT(task_info.src_ls_id_ = src_ls_id)) {
    } else if (FALSE_IT(task_info.dest_ls_id_ = dest_ls_id)) {
    } else if (FALSE_IT(task_info.task_id_.id_ = 10000)) {
    } else if (FALSE_IT(task_info.trace_id_.set(*ObCurTraceId::get_trace_id()))) {
    } else if (FALSE_IT(task_info.status_ = ObTransferStatus::START)) {
    } else if (FALSE_IT(task_info.table_lock_owner_id_.id_ = 10000)) {
    } else if (OB_FAIL(task_info.tablet_list_.push_back(tablet_info))) {
    } else if (OB_FAIL(transfer_handler->start_trans_(timeout_ctx, trans))) {
      LOG_WARN("failed to start trans", K(ret), K(task_info));
    } else if (OB_FAIL(transfer_handler->precheck_ls_replay_scn_(task_info))) {
      LOG_WARN("failed to precheck ls replay scn", K(ret), K(task_info));
    } else if (OB_FAIL(transfer_handler->check_start_status_transfer_tablets_(task_info))) {
      LOG_WARN("failed to check start status transfer tablets", K(ret), K(task_info));
    } else if (OB_FAIL(transfer_handler->update_all_tablet_to_ls_(task_info, trans))) {
      LOG_WARN("failed to update all tablet to ls", K(ret), K(task_info));
    } else if (OB_FAIL(transfer_handler->lock_tablet_on_dest_ls_for_table_lock_(task_info, trans))) {
      LOG_WARN("failed to lock tablet on dest ls for table lock", KR(ret), K(task_info));
    } else if (OB_FAIL(transfer_handler->do_trans_transfer_start_prepare_(task_info, timeout_ctx, trans))) {
      LOG_WARN("failed to do trans transfer start prepare", K(ret), K(task_info));
    } else if (OB_FAIL(transfer_handler->do_trans_transfer_start_v2_(task_info, timeout_ctx, trans))) {
      LOG_WARN("failed to do trans transfer start", K(ret), K(task_info));
    }

    if (OB_FAIL(trans.end(false))) {
    }
  }
  return ret;
}

TEST_F(ObTransferTx, prepare)
{
  LOGI("observer start");

  LOGI("create tenant begin");
  // 创建普通租户tt1
  EQ(OB_SUCCESS, create_tenant("tt1", "40G", "40G", false, 10));
  // 获取租户tt1的tenant_id
  EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_));
  ASSERT_NE(0, R.tenant_id_);
  // 初始化普通租户tt1的sql proxy
  EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  int tenant_id = R.tenant_id_;
  LOGI("create tenant finish");

  R.worker_ = std::thread([this, tenant_id] () {
    int ret = OB_SUCCESS;
    lib::set_thread_name_inner("MY_BALANCE");
    MTL_SWITCH(R.tenant_id_) {
      MTL(rootserver::ObTenantBalanceService*)->stop();
    }
    while (!R.stop_) {
      do_balance(tenant_id);
      ::sleep(3);
    }
  });

  // 在单节点ObServer下创建新的日志流, 注意避免被RS任务GC掉
  EQ(0, SSH::create_ls(R.tenant_id_, get_curr_observer().self_addr_));
  int64_t ls_count = 0;
  EQ(0, SSH::g_select_int64(R.tenant_id_, "select count(ls_id) as val from __all_ls where ls_id!=1", ls_count));
  EQ(2, ls_count);
}

#define TRANSFER_CASE_PREPARE \
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();  \
  int64_t affected_rows = 0;                                                    \
  EQ(0, wait_balance_clean(R.tenant_id_));                                      \
  EQ(0, sql_proxy.write("drop database if exists test", affected_rows));        \
  EQ(0, sql_proxy.write("create database if not exists test", affected_rows));  \
  EQ(0, sql_proxy.write("use test", affected_rows));                            \
  EQ(0, sql_proxy.write("drop tablegroup if exists tg1", affected_rows));       \
  EQ(0, do_balance(R.tenant_id_));                                              \
  EQ(0, wait_balance_clean(R.tenant_id_));                                      \
  rootserver::ObNewTableTabletAllocator::alloc_tablet_ls_offset_ = 0;           \
  EQ(OB_SUCCESS, sql_proxy.write("create table stu1(col int)", affected_rows)); \
  EQ(OB_SUCCESS, sql_proxy.write("create table stu2(col int)", affected_rows)); \
  ObLSID loc1,loc2;                                                             \
  EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));                          \
  EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));                          \
  NEQ(loc1, loc2);                                                              \
  EQ(0, sql_proxy.write("create tablegroup tg1 sharding='NONE';", affected_rows));

TEST_F(ObTransferTx, tx_exit)
{
  TRANSFER_CASE_PREPARE;
  ObLSID ls_id;
  EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", ls_id));

  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  // timeout with no redo
  // tx exit with no log
  EQ(0, SSH::write(conn, "set session ob_trx_timeout=1000000"));
  EQ(0, SSH::write(conn, "set autocommit=0"));
  EQ(0, SSH::write(conn, "insert into stu1 values(200)"));
  ObTransID tx_id;
  EQ(0, SSH::find_tx(conn, tx_id));
  LOGI("sp no redo ls_id:%ld, tx:%ld", ls_id.id(), tx_id.get_id());
  EQ(OB_TRANS_CTX_NOT_EXIST, SSH::wait_tx_exit(R.tenant_id_, ls_id, tx_id));
  conn->commit();

  // timeout with write redo
  // tx exit with abort log
  // ls1: redo --> abort
  EQ(0, SSH::write(conn, "insert into stu1 values(200)"));
  EQ(0, SSH::find_tx(conn, tx_id));
  LOGI("sp with redo ls_id:%ld, tx:%ld", ls_id.id(), tx_id.get_id());
  EQ(0, SSH::submit_redo(R.tenant_id_, ls_id));
  EQ(OB_TRANS_CTX_NOT_EXIST, SSH::wait_tx_exit(R.tenant_id_, ls_id, tx_id));
  conn->commit();

  // timeout with write redo
  // tx exit with abort log
  // ls1: redo --> abort
  // ls2:
  EQ(0, SSH::write(conn, "insert into stu1 values(200)"));
  EQ(0, SSH::write(conn, "insert into stu2 values(200)"));
  EQ(0, SSH::find_tx(conn, tx_id));
  LOGI("dist trans with redo ls_id:%ld, tx:%ld", ls_id.id(), tx_id.get_id());
  EQ(0, SSH::submit_redo(R.tenant_id_, ls_id));
  EQ(OB_TRANS_CTX_NOT_EXIST, SSH::wait_tx_exit(R.tenant_id_, ls_id, tx_id));
  conn->commit();

  // timeout with write prepare
  // 协调者持续推进prepare阶段，参与者自身事务上下文不存在，给上游发abort消息
  // ls1  redo --> prepare --> abort --> clear
  // ls2:
  EQ(0, SSH::write(conn, "insert into stu1 values(200)"));
  EQ(0, SSH::write(conn, "insert into stu2 values(200)"));
  EQ(0, SSH::find_tx(conn, tx_id));
  LOGI("dist trans with prepare ls_id:%ld, tx:%ld", ls_id.id(), tx_id.get_id());
  InjectTxFaultHelper inject_tx_fault_helper;
  EQ(0, inject_tx_fault_helper.inject_tx_block(R.tenant_id_, loc2, tx_id, ObTxLogType::TX_REDO_LOG));
  std::thread th([conn] () {
    conn->commit();
  });
  EQ(0, SSH::wait_tx(R.tenant_id_, loc1, tx_id, ObTxState::PREPARE));
  EQ(0, SSH::abort_tx(R.tenant_id_, loc2, tx_id));
  inject_tx_fault_helper.release();
  th.join();
  EQ(OB_TRANS_CTX_NOT_EXIST, SSH::wait_tx_exit(R.tenant_id_, loc1, tx_id));
  EQ(OB_TRANS_CTX_NOT_EXIST, SSH::wait_tx_exit(R.tenant_id_, loc2, tx_id));

  // timeout with write prepare
  // ls1  redo --> prepare --> abort --> clear
  // ls2: redo --> abort
  EQ(0, SSH::write(conn, "insert into stu1 values(200)"));
  EQ(0, SSH::write(conn, "insert into stu2 values(200)"));
  EQ(0, SSH::find_tx(conn, tx_id));
  LOGI("dist trans with prepare ls_id:%ld, tx:%ld", ls_id.id(), tx_id.get_id());
  EQ(0, SSH::submit_redo(R.tenant_id_, loc2));
  EQ(0, inject_tx_fault_helper.inject_tx_block(R.tenant_id_, loc2, tx_id, ObTxLogType::TX_PREPARE_LOG));
  std::thread th1([conn] () {
    conn->commit();
  });
  EQ(0, SSH::wait_tx(R.tenant_id_, loc1, tx_id, ObTxState::PREPARE));
  EQ(0, SSH::abort_tx(R.tenant_id_, loc2, tx_id));
  inject_tx_fault_helper.release();
  th1.join();
  EQ(OB_TRANS_CTX_NOT_EXIST, SSH::wait_tx_exit(R.tenant_id_, loc1, tx_id));
  EQ(OB_TRANS_CTX_NOT_EXIST, SSH::wait_tx_exit(R.tenant_id_, loc2, tx_id));


  // timeout with write prepare
  // 参与者持续给上游发prepare response，发现参与者不存在，通知下游abort
  // ls1
  // ls2 redo --> prepare --> abort --> clear
  EQ(0, SSH::write(conn, "insert into stu1 values(200)"));
  EQ(0, SSH::write(conn, "insert into stu2 values(200)"));
  EQ(0, SSH::find_tx(conn, tx_id));
  LOGI("dist trans with prare ls_id:%ld, tx:%ld", ls_id.id(), tx_id.get_id());
  EQ(0, inject_tx_fault_helper.inject_tx_block(R.tenant_id_, loc1, tx_id, ObTxLogType::TX_REDO_LOG));
  std::thread th2([conn] () {
    conn->commit();
  });
  EQ(0, SSH::wait_tx(R.tenant_id_, loc2, tx_id, ObTxState::PREPARE));
  EQ(0, SSH::abort_tx(R.tenant_id_, loc1, tx_id));
  inject_tx_fault_helper.release();
  th2.join();
  EQ(OB_TRANS_CTX_NOT_EXIST, SSH::wait_tx_exit(R.tenant_id_, loc1, tx_id));
  EQ(OB_TRANS_CTX_NOT_EXIST, SSH::wait_tx_exit(R.tenant_id_, loc2, tx_id));
}

/*
TEST_F(ObTransferTx, large_query)
{
  TRANSFER_CASE_PREPARE;

  get_curr_simple_server().get_sql_proxy().write("alter system set writing_throttling_trigger_percentage 100 tenant all", affected_rows);
  //get_curr_simple_server().get_sql_proxy().write("alter system set syslog_level='DEBUG'", affected_rows);

  // prepare data
  EQ(0, sql_proxy.write("insert into stu1 values(100)", affected_rows));
  for (int i = 0; i < 19; i++) {
    EQ(0, sql_proxy.write("insert into stu1 select * from stu1", affected_rows));
  }

  bool stop = false;
  std::thread th([&stop, loc1, loc2]() {
    int ret = OB_SUCCESS;
    while (OB_SUCC(ret) && !stop) {
      ObMySQLTransaction trans;
      trans.start(GCTX.sql_proxy_, R.tenant_id_);
      observer::ObInnerSQLConnection *conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
      SCN end_scn;
      SSH::get_ls_end_scn(R.tenant_id_, loc2, end_scn);
      ObArenaAllocator allocator;
      ObTabletID tablet_id;
      SSH::select_table_tablet(R.tenant_id_, "stu2", tablet_id);
      int64_t pos = 0;
      ObRegisterMdsFlag flag;
      flag.need_flush_redo_instantly_ = true;
      flag.mds_base_scn_ = end_scn;
      ObTransferTabletInfo tablet_info;
      tablet_info.tablet_id_ = tablet_id;
      tablet_info.transfer_seq_ = 0;
      ObTXStartTransferOutInfo start_transfer_out_info;
      start_transfer_out_info.src_ls_id_ = loc2;
      start_transfer_out_info.dest_ls_id_ = loc1;
      start_transfer_out_info.tablet_list_.push_back(tablet_info);
      start_transfer_out_info.transfer_epoch_= 1;
      start_transfer_out_info.data_end_scn_ = end_scn;

      int64_t buf_len = start_transfer_out_info.get_serialize_size();
      char *buf = (char*)allocator.alloc(buf_len);
      start_transfer_out_info.serialize(buf, buf_len, pos);
      if (OB_FAIL(conn->register_multi_data_source(R.tenant_id_, loc1,
          ObTxDataSourceType::START_TRANSFER_OUT, buf, buf_len, flag))) {
        LOG_WARN("failed to register mds", K(ret), K(start_transfer_out_info));
      } else {
        LOG_INFO("register mds", K(start_transfer_out_info));
      }
      ob_usleep(1000 * 1000);
      trans.end(false);
      int rd = rand() % 7000;
      ob_usleep(rd * 1000);
    }
  });
  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0"));
  int64_t session_id = 0;
  EQ(0, SSH::find_session(conn, session_id));
  for (int i=0;i<5;i++) {
    int64_t start_time = ObTimeUtil::current_time();
    EQ(0, SSH::write(conn, "insert into stu2 select * from stu1", affected_rows));
    int64_t end_time = ObTimeUtil::current_time();
    int64_t query_cost = end_time - start_time;
    ObTransID tx_id;
    int64_t request_id = 0;
    ObString trace_id;
    int64_t retry_cnt = 0;
    EQ(0, SSH::find_request(R.tenant_id_, session_id, request_id, tx_id,trace_id, retry_cnt));
    start_time = ObTimeUtil::current_time();
    EQ(0, conn->commit());
    end_time = ObTimeUtil::current_time();
    LOGI("large_query %d: txid:%ld query_cost:%ld commit_cost:%ld row_count:%ld retry_cnt:%ld trace_id:%s", i,tx_id.get_id(), query_cost,
        end_time-start_time, affected_rows, retry_cnt, trace_id.ptr());
  }

  stop = true;
  th.join();
  int64_t row_count = 0;
  EQ(0, SSH::select_int64(sql_proxy, "select count(*) as val from stu2", row_count));
  LOGI("large_query: row_count:%ld", row_count);
  //get_curr_simple_server().get_sql_proxy().write("alter system set syslog_level='INFO'", affected_rows);
}
*/

TEST_F(ObTransferTx, epoch_recover_from_active_info)
{
  TRANSFER_CASE_PREPARE;

  ObTransID tx_id;
  ObLSID ls_id;
  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0"));
  EQ(0, SSH::write(conn, "insert into stu1 values(200)"));
  EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", ls_id));
  EQ(0, SSH::find_tx(conn, tx_id));
  ObPartTransCtx ctx1;
  EQ(0, SSH::find_tx_info(R.tenant_id_, ls_id, tx_id, ctx1));
  LOGI("ls_id:%ld, tx:%ld epoch:%ld", ls_id.id(), tx_id.get_id(), ctx1.epoch_);

  // write active info log
  EQ(0, SSH::ls_reboot(R.tenant_id_, ls_id));

  // recover epoch by TxActiveInfoLog
  ObPartTransCtx ctx2;
  EQ(0, SSH::find_tx_info(R.tenant_id_, ls_id, tx_id, ctx2));
  LOGI("ls_id:%ld, tx:%ld epoch:%ld", ls_id.id(), tx_id.get_id(), ctx2.epoch_);
  EQ(ctx1.epoch_, ctx2.epoch_);
  EQ(0, conn->commit());
}

TEST_F(ObTransferTx, epoch_recover_from_ctx_checkpoint)
{
  TRANSFER_CASE_PREPARE;

  ObTransID tx_id;
  ObLSID ls_id;
  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0"));
  EQ(0, SSH::write(conn, "insert into stu1 values(100)"));
  EQ(0, SSH::write(conn, "insert into stu2 values(200)"));
  EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", ls_id));
  EQ(0, SSH::find_tx(conn, tx_id));
  ObPartTransCtx ctx1;
  EQ(0, SSH::find_tx_info(R.tenant_id_, ls_id, tx_id, ctx1));
  LOGI("ls_id:%ld, tx:%ld epoch:%ld", ls_id.id(), tx_id.get_id(), ctx1.epoch_);

  InjectTxFaultHelper inject_tx_fault_helper;
  EQ(0, inject_tx_fault_helper.inject_tx_block(R.tenant_id_, ls_id, tx_id, ObTxLogType::TX_COMMIT_LOG));

  int commit_ret = -1;
  std::thread th([&conn, &commit_ret]() {
    commit_ret = conn->commit();
  });
  // make tx block prepare phase
  EQ(0, SSH::wait_tx(R.tenant_id_, ls_id, tx_id, ObTxState::PREPARE));
  EQ(0, SSH::find_tx_info(R.tenant_id_, ls_id, tx_id, ctx1));
  GE(ctx1.exec_info_.state_, ObTxState::PREPARE);

  // checkpoint tx ctx to newest
  EQ(0, SSH::wait_checkpoint_newest(R.tenant_id_, ls_id));
  // replay from middle
  EQ(0, SSH::ls_reboot(R.tenant_id_, ls_id));

  ObPartTransCtx ctx2;
  EQ(0, SSH::find_tx_info(R.tenant_id_, ls_id, tx_id, ctx2));
  LOGI("ls_id:%ld, tx:%ld epoch:%ld", ls_id.id(), tx_id.get_id(), ctx2.epoch_);

  EQ(ctx1.epoch_, ctx2.epoch_);

  inject_tx_fault_helper.release();

  th.join();
  EQ(0, commit_ret);
}

TEST_F(ObTransferTx, epoch_recover_from_ctx_checkpoint2)
{
  TRANSFER_CASE_PREPARE;

  ObTransID tx_id;
  ObLSID ls_id;
  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0"));
  EQ(0, SSH::write(conn, "insert into stu1 values(100)"));
  EQ(0, SSH::write(conn, "insert into stu2 values(200)"));
  EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", ls_id));
  EQ(0, SSH::find_tx(conn, tx_id));
  ObPartTransCtx ctx1;
  EQ(0, SSH::find_tx_info(R.tenant_id_, ls_id, tx_id, ctx1));
  LOGI("ls_id:%ld, tx:%ld epoch:%ld", ls_id.id(), tx_id.get_id(), ctx1.epoch_);

  InjectTxFaultHelper inject_tx_fault_helper;
  EQ(0, inject_tx_fault_helper.inject_tx_block(R.tenant_id_, ls_id, tx_id, ObTxLogType::TX_CLEAR_LOG));

  int commit_ret = -1;
  std::thread th([&conn, &commit_ret]() {
    commit_ret = conn->commit();
  });
  // make tx block prepare phase
  EQ(0, SSH::wait_tx(R.tenant_id_, ls_id, tx_id, ObTxState::COMMIT));
  EQ(0, SSH::find_tx_info(R.tenant_id_, ls_id, tx_id, ctx1));
  GE(ctx1.exec_info_.state_, ObTxState::COMMIT);

  // checkpoint tx ctx to newest
  EQ(0, SSH::wait_checkpoint_newest(R.tenant_id_, ls_id));
  // replay from middle
  EQ(0, SSH::ls_reboot(R.tenant_id_, ls_id));

  ObPartTransCtx ctx2;
  EQ(0, SSH::find_tx_info(R.tenant_id_, ls_id, tx_id, ctx2));
  LOGI("ls_id:%ld, tx:%ld epoch:%ld", ls_id.id(), tx_id.get_id(), ctx2.epoch_);
  EQ(ctx1.epoch_, ctx2.epoch_);

  inject_tx_fault_helper.release();

  th.join();
  EQ(0, commit_ret);
}

// 空sstable、没有活跃事务
TEST_F(ObTransferTx, transfer_empty_tablet)
{
  // 关掉observer内部的均衡，防止LS均衡，只调度分区均衡
  TRANSFER_CASE_PREPARE;

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }
  EQ(0, sql_proxy.write("insert into stu2 values(100)", affected_rows));
  int64_t val = 0;
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu2", val));
  EQ(100, val);
}

// sstable有数据，没有活跃事务 transfer
TEST_F(ObTransferTx, transfer_no_active_tx)
{
  TRANSFER_CASE_PREPARE;

  EQ(0, sql_proxy.write("insert into stu1 values(100)", affected_rows));
  EQ(0, sql_proxy.write("insert into stu2 values(100)", affected_rows));

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }
  EQ(0, sql_proxy.write("insert into stu2 values(200)", affected_rows));
  int64_t val = 0;
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu1", val));
  EQ(100, val);
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu2", val));
  EQ(300, val);
}

// sstable有数据，有活跃事务 transfer
TEST_F(ObTransferTx, transfer_active_tx)
{
  TRANSFER_CASE_PREPARE;

  EQ(0, sql_proxy.write("insert into stu1 values(100)", affected_rows));
  EQ(0, sql_proxy.write("insert into stu2 values(100)", affected_rows));

  ObTransID tx_id, tx_id1, tx_id2, tx_id3;
  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0"));
  EQ(0, SSH::write(conn, "insert into stu2 values(200)"));
  EQ(0, SSH::find_tx(conn, tx_id));

  sqlclient::ObISQLConnection *conn1 = NULL;
  EQ(0, sql_proxy.acquire(conn1));
  EQ(0, conn1->execute_write(OB_SYS_TENANT_ID, "set autocommit=0", affected_rows));
  EQ(0, conn1->execute_write(OB_SYS_TENANT_ID, "insert into stu2 values(50)", affected_rows));
  EQ(0, SSH::find_tx(conn1, tx_id1));

  sqlclient::ObISQLConnection *conn2 = NULL;
  EQ(0, sql_proxy.acquire(conn2));
  EQ(0, SSH::write(conn2, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn2, "insert into stu2 values(300)", affected_rows));
  EQ(0, SSH::write(conn2, "insert into stu1 values(200)", affected_rows));
  EQ(0, SSH::find_tx(conn2, tx_id2));

  sqlclient::ObISQLConnection *conn3 = NULL;
  EQ(0, sql_proxy.acquire(conn3));
  EQ(0, SSH::write(conn3, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn3, "insert into stu2 values(400)", affected_rows));
  EQ(0, SSH::write(conn3, "insert into stu1 values(500)", affected_rows));
  EQ(0, SSH::find_tx(conn3, tx_id3));

  LOGI("find_tx:%ld %ld %ld %ld",tx_id.get_id(), tx_id1.get_id(), tx_id2.get_id(), tx_id3.get_id());

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }
  int64_t sum1 = 0;
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu1", sum1));
  EQ(100, sum1);
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu2", sum1));
  EQ(100, sum1);

  EQ(0, SSH::write(conn, "insert into stu2 values(1000)", affected_rows));
  EQ(0, SSH::write(conn2, "insert into stu2 values(1000)", affected_rows));
  EQ(0, SSH::write(conn3, "insert into stu2 values(1000)", affected_rows));

  EQ(0, SSH::submit_redo(R.tenant_id_, loc1));
  EQ(0, SSH::submit_redo(R.tenant_id_, loc2));

  EQ(0, conn->commit());
  EQ(0, conn1->commit());
  EQ(0, conn2->commit());
  EQ(0, conn3->rollback());
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu1", sum1));
  EQ(300, sum1);
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu2", sum1));
  EQ(2650, sum1);
}


// transfer active tx A->B  B->A
TEST_F(ObTransferTx, transfer_A_B_AND_B_A)
{
  TRANSFER_CASE_PREPARE;

  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn, "insert into stu2 values(100)", affected_rows));

  ObTransID tx_id1, tx_id2;
  EQ(0, SSH::find_tx(conn, tx_id1));
  LOGI("find active_tx tx_id:%ld %ld", tx_id1.get_id(), tx_id2.get_id());

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }
  EQ(0, SSH::write(conn, "insert into stu2 values(200)", affected_rows));
  EQ(0, sql_proxy.write("alter table stu2 tablegroup=''", affected_rows));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 != loc2) {
      break;
    }
    ::sleep(1);
  }
  EQ(0, SSH::write(conn, "insert into stu2 values(300)", affected_rows));
  EQ(0, conn->commit());
  int64_t val = 0;
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu2",val));
  EQ(600, val);
}

TEST_F(ObTransferTx, transfer_replay)
{
  TRANSFER_CASE_PREPARE;

  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn, "insert into stu2 values(100)", affected_rows));

  sqlclient::ObISQLConnection *conn2 = NULL;
  EQ(0, sql_proxy.acquire(conn2));
  EQ(0, SSH::write(conn2, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn2, "insert into stu2 values(100)", affected_rows));
  EQ(0, SSH::write(conn2, "insert into stu1 values(100)", affected_rows));

  sqlclient::ObISQLConnection *conn3 = NULL;
  EQ(0, sql_proxy.acquire(conn3));
  EQ(0, SSH::write(conn3, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn3, "insert into stu2 values(100)", affected_rows));
  ObTransID tx_id1, tx_id2, tx_id3;
  EQ(0, SSH::find_tx(conn, tx_id1));
  EQ(0, SSH::find_tx(conn2, tx_id2));
  EQ(0, SSH::find_tx(conn3, tx_id3));
  LOGI("find active_tx tx_id:%ld %ld %ld", tx_id1.get_id(), tx_id2.get_id(), tx_id3.get_id());

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    ObLSID loc1_tmp, loc2_tmp;
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1_tmp));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2_tmp));
    if (loc1_tmp == loc2_tmp) {
      break;
    }
    ::sleep(1);
  }
  EQ(0, conn->commit());
  EQ(0, conn2->commit());
  EQ(OB_SUCCESS, SSH::ls_reboot(R.tenant_id_, loc1));
  EQ(OB_SUCCESS, SSH::ls_reboot(R.tenant_id_, loc2));

  EQ(0, conn3->commit());
  int64_t val = 0;
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu2",val));
  EQ(300, val);
}

// sstable有数据，有活跃事务 transfer
TEST_F(ObTransferTx, transfer_abort_active_tx)
{
  TRANSFER_CASE_PREPARE;

  R.stop_balance_ = true;

  EQ(0, sql_proxy.write("insert into stu1 values(100)", affected_rows));
  EQ(0, sql_proxy.write("insert into stu2 values(100)", affected_rows));

  ObTransID tx_id, tx_id1, tx_id2, tx_id3;
  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn, "insert into stu2 values(100)", affected_rows));
  EQ(0, SSH::find_tx(conn, tx_id));

  sqlclient::ObISQLConnection *conn2 = NULL;
  EQ(0, sql_proxy.acquire(conn2));
  EQ(0, SSH::write(conn2, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn2, "insert into stu2 values(100)", affected_rows));
  EQ(0, SSH::write(conn2, "insert into stu1 values(100)", affected_rows));
  EQ(0, SSH::find_tx(conn2, tx_id2));

  LOGI("find_tx:%ld %ld",tx_id.get_id(), tx_id2.get_id());

  ObTransferTabletInfo tablet_info;
  EQ(0, SSH::select_table_tablet(R.tenant_id_, "stu2", tablet_info.tablet_id_));
  tablet_info.transfer_seq_ = 0;
  EQ(0, do_transfer_start_abort(R.tenant_id_, loc1, loc2, tablet_info));
  EQ(0, SSH::write(conn, "insert into stu1 values(1000)", affected_rows));
  EQ(0, SSH::write(conn, "insert into stu2 values(1000)", affected_rows));
  EQ(0, do_transfer_start_abort(R.tenant_id_, loc1, loc2, tablet_info));

  R.stop_balance_ = false;
  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }

  EQ(0, conn->commit());
  EQ(0, conn2->commit());
  int64_t sum1 = 0;
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu1", sum1));
  EQ(1200, sum1);
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu2", sum1));
  EQ(1300, sum1);
}

TEST_F(ObTransferTx, transfer_resume)
{
  TRANSFER_CASE_PREPARE;

  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn, "insert into stu2 values(100)", affected_rows));

  ObTransID tx_id1, tx_id2;
  EQ(0, SSH::find_tx(conn, tx_id1));
  LOGI("find active_tx tx_id:%ld %ld", tx_id1.get_id(), tx_id2.get_id());

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }
  EQ(0, SSH::ls_resume(R.tenant_id_, loc1));
  EQ(0, SSH::ls_resume(R.tenant_id_, loc2));
  EQ(0, SSH::write(conn, "insert into stu2 values(200)", affected_rows));
  EQ(0, conn->commit());
  int64_t val = 0;
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu2",val));
  EQ(300, val);
}

// sstable有数据，有活跃事务,但事务数据丢失不完整 transfer
TEST_F(ObTransferTx, transfer_query_lost)
{
  TRANSFER_CASE_PREPARE;

  EQ(0, sql_proxy.write("insert into stu1 values(100)", affected_rows));
  EQ(0, sql_proxy.write("insert into stu2 values(100)", affected_rows));

  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn, "insert into stu2 values(300)", affected_rows));
  // 活跃事务操作了stu1
  EQ(0, SSH::write(conn, "insert into stu1 values(200)", affected_rows));

  sqlclient::ObISQLConnection *conn2 = NULL;
  EQ(0, sql_proxy.acquire(conn2));
  EQ(0, SSH::write(conn2, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn2, "insert into stu2 values(400)", affected_rows));
  ObTransID tx_id1, tx_id2;
  EQ(0, SSH::find_tx(conn, tx_id1));
  EQ(0, SSH::find_tx(conn2, tx_id2));
  LOGI("tx_id:%ld %ld", tx_id1.get_id(), tx_id2.get_id());

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }
  // remove tx_ctx simulate sys reboot
  EQ(0, SSH::abort_tx(R.tenant_id_, loc1, tx_id1));
  EQ(0, SSH::abort_tx(R.tenant_id_, loc1, tx_id2));

  // query lost
  NEQ(0, conn->commit());
  // transfer lost
  NEQ(0, conn2->commit());
}

// transfer active tx A->B  A->B
TEST_F(ObTransferTx, transfer_A_B_AND_A_B)
{
  TRANSFER_CASE_PREPARE;

  EQ(0, sql_proxy.write("create table stu3(col int)", affected_rows));
  EQ(0, sql_proxy.write("create table stu4(col int)", affected_rows));
  ObLSID loc3,loc4;
  EQ(0, SSH::select_table_loc(R.tenant_id_, "stu3", loc3));
  EQ(0, SSH::select_table_loc(R.tenant_id_, "stu4", loc4));
  EQ(loc1, loc3);
  EQ(loc2, loc4);
  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn, "insert into stu2 values(100)", affected_rows));

  ObTransID tx_id1, tx_id2;
  EQ(0, SSH::find_tx(conn, tx_id1));
  LOGI("find active_tx tx_id:%ld %ld", tx_id1.get_id(), tx_id2.get_id());

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }
  // abort tx create by move in simulate transfer tx lost
  EQ(0, SSH::abort_tx(R.tenant_id_, loc1, tx_id1));

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu3,stu4;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu3", loc3));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu4", loc4));
    if (loc3 == loc4 && loc1 == loc3) {
      break;
    }
    ::sleep(1);
  }
  // transfer lost
  NEQ(0, conn->commit());
}

TEST_F(ObTransferTx, transfer_AND_ddl)
{
  TRANSFER_CASE_PREPARE;

  bool case_stop = false;
  int case_err = 0;


  std::thread th([&]() {
    int64_t affected_rows = 0;
    int ret = 0;
    DEFER(if (OB_FAIL(ret)) {case_err = ret;});
    while (!case_stop && OB_SUCC(ret)) {
      if (OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID, "truncate table stu2", affected_rows))) {
      }
      ob_usleep(200 * 1000);
    }
  });
  std::thread th2([&]() {
    int64_t affected_rows = 0;
    int ret = 0;
    DEFER(if (OB_FAIL(ret)) {case_err = ret;});
    while (!case_stop && OB_SUCC(ret)) {
      if (OB_FAIL(sql_proxy.write(OB_SYS_TENANT_ID, "insert into stu2 values(100)", affected_rows))) {
      }
    }
  });

  int round = 10;
  while (round > 0) {
    int64_t task = 0;
    ObSqlString sql;
    sql.assign_fmt("select count(*) as val from __all_virtual_transfer_task where tenant_id=%ld", R.tenant_id_);
    EQ(0, SSH::g_select_int64(OB_SYS_TENANT_ID, sql.ptr(), task));
    if (task == 0) {
      EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
      EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
      if (loc1 == loc2) {
        EQ(0, sql_proxy.write("alter table stu2 tablegroup=''", affected_rows));
      } else {
        EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2", affected_rows));
      }
      EQ(0, do_balance(R.tenant_id_));
      round--;
    }
    ob_usleep(3 * 1000 * 1000);
  }
  case_stop = true;
  th.join();
  th2.join();
  EQ(0, case_err);
}

TEST_F(ObTransferTx, transfer_AND_rollback)
{
  TRANSFER_CASE_PREPARE;

  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn, "insert into stu2 values(100)", affected_rows));
  EQ(0, SSH::write(conn, "savepoint sp1", affected_rows));
  EQ(0, SSH::write(conn, "insert into stu2 values(200)", affected_rows));
  int64_t val = 0;
  EQ(0, SSH::select_int64(conn, "select sum(col) as val from stu2", val));
  EQ(300, val);

  ObTransID tx_id1, tx_id2;
  EQ(0, SSH::find_tx(conn, tx_id1));
  LOGI("find active_tx tx_id:%ld %ld", tx_id1.get_id(), tx_id2.get_id());

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }
  EQ(0, SSH::write(conn, "rollback to sp1", affected_rows));
  EQ(0, SSH::select_int64(conn, "select sum(col) as val from stu2", val));
  EQ(100, val);
  EQ(0, SSH::write(conn, "insert into stu2 values(300)", affected_rows));
  EQ(0, conn->commit());
  EQ(0, SSH::select_int64(conn, "select sum(col) as val from stu2", val));
  EQ(400, val);

  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, loc1));
  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, loc2));
  EQ(0, SSH::freeze_tx_data(R.tenant_id_, loc1));
  EQ(0, SSH::freeze_tx_data(R.tenant_id_, loc2));
  EQ(0, SSH::ls_reboot(R.tenant_id_, loc1));
  EQ(0, SSH::ls_reboot(R.tenant_id_, loc2));
}

TEST_F(ObTransferTx, transfer_AND_rollback2)
{
  TRANSFER_CASE_PREPARE;

  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0", affected_rows));
  EQ(0, SSH::write(conn, "insert into stu2 values(100)", affected_rows));
  EQ(0, SSH::write(conn, "savepoint sp1", affected_rows));
  EQ(0, SSH::write(conn, "insert into stu2 values(200)", affected_rows));
  int64_t val = 0;

  ObTransID tx_id1, tx_id2;
  EQ(0, SSH::find_tx(conn, tx_id1));
  LOGI("find active_tx tx_id:%ld %ld", tx_id1.get_id(), tx_id2.get_id());

  ObTabletID tablet_id;
  EQ(0, SSH::select_table_tablet(R.tenant_id_, "stu2", tablet_id));
  EQ(0, SSH::freeze(R.tenant_id_, loc2, tablet_id));
  EQ(0, SSH::wait_flush_finish(R.tenant_id_, loc2, tablet_id));
  //
  EQ(0, SSH::write(conn, "rollback to sp1", affected_rows));
  EQ(0, SSH::select_int64(conn, "select sum(col) as val from stu2", val));
  EQ(100, val);

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }
  EQ(0, SSH::select_int64(conn, "select sum(col) as val from stu2", val));
  EQ(100, val);
  EQ(0, wait_balance_clean(R.tenant_id_));
  EQ(0, SSH::select_int64(conn, "select sum(col) as val from stu2", val));
  EQ(100, val);

  EQ(0, conn->commit());
  EQ(0, SSH::select_int64(conn, "select sum(col) as val from stu2", val));
  EQ(100, val);
}

TEST_F(ObTransferTx, transfer_tx_ctx_merge)
{
  TRANSFER_CASE_PREPARE;

  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0"));
  EQ(0, SSH::write(conn, "insert into stu1 values(100)"));
  EQ(0, SSH::write(conn, "insert into stu2 values(100)"));
  ObTransID tx_id;
  EQ(0, SSH::find_tx(conn, tx_id));
  LOGI("find active_tx tx_id:%ld", tx_id.get_id());

  sqlclient::ObISQLConnection *conn2 = NULL;
  EQ(0, sql_proxy.acquire(conn2));
  EQ(0, SSH::write(conn2, "set autocommit=0"));
  EQ(0, SSH::write(conn2, "insert into stu1 values(100)"));
  EQ(0, SSH::write(conn2, "insert into stu2 values(100)"));
  ObTransID tx_id2;
  EQ(0, SSH::find_tx(conn2, tx_id2));
  LOGI("find active_tx tx_id2:%ld", tx_id2.get_id());

  sqlclient::ObISQLConnection *conn3 = NULL;
  EQ(0, sql_proxy.acquire(conn3));
  EQ(0, SSH::write(conn3, "set autocommit=0"));
  EQ(0, SSH::write(conn3, "insert into stu1 values(100)"));
  EQ(0, SSH::write(conn3, "insert into stu2 values(100)"));
  ObTransID tx_id3;
  EQ(0, SSH::find_tx(conn3, tx_id3));
  LOGI("find active_tx tx_id3:%ld", tx_id3.get_id());

  sqlclient::ObISQLConnection *conn4 = NULL;
  EQ(0, sql_proxy.acquire(conn4));
  EQ(0, SSH::write(conn4, "set autocommit=0"));
  EQ(0, SSH::write(conn4, "insert into stu1 values(100)"));
  EQ(0, SSH::write(conn4, "insert into stu2 values(100)"));
  ObTransID tx_id4;
  EQ(0, SSH::find_tx(conn4, tx_id4));
  LOGI("find active_tx tx_id4:%ld", tx_id4.get_id());

  sqlclient::ObISQLConnection *conn5 = NULL;
  EQ(0, sql_proxy.acquire(conn5));
  EQ(0, SSH::write(conn5, "set autocommit=0"));
  EQ(0, SSH::write(conn5, "insert into stu1 values(100)"));
  EQ(0, SSH::write(conn5, "insert into stu2 values(100)"));
  ObTransID tx_id5;
  EQ(0, SSH::find_tx(conn5, tx_id5));
  LOGI("find active_tx tx_id5:%ld", tx_id5.get_id());


  EQ(0, SSH::submit_redo(R.tenant_id_, loc1));
  InjectTxFaultHelper inject_tx_fault_helper;
  EQ(0, inject_tx_fault_helper.inject_tx_block(R.tenant_id_, loc1, tx_id, ObTxLogType::TX_REDO_LOG));
  EQ(0, inject_tx_fault_helper.inject_tx_block(R.tenant_id_, loc1, tx_id2, ObTxLogType::TX_PREPARE_LOG));
  EQ(0, inject_tx_fault_helper.inject_tx_block(R.tenant_id_, loc1, tx_id3, ObTxLogType::TX_COMMIT_LOG));
  EQ(0, inject_tx_fault_helper.inject_tx_block(R.tenant_id_, loc1, tx_id4, ObTxLogType::TX_CLEAR_LOG));
  EQ(0, inject_tx_fault_helper.inject_tx_block(R.tenant_id_, loc1, tx_id5, ObTxLogType::TX_CLEAR_LOG));

  int commit_ret = -1;
  std::thread th([&conn, &commit_ret]() {
    commit_ret = conn->commit();
  });
  int commit_ret2 = -1;
  std::thread th2([&conn2, &commit_ret2]() {
    commit_ret2 = conn2->commit();
  });
  int commit_ret3 = -1;
  std::thread th3([&conn3, &commit_ret3]() {
    commit_ret3 = conn3->commit();
  });
  int commit_ret4 = -1;
  std::thread th4([&conn4, &commit_ret4]() {
    commit_ret4 = conn4->commit();
  });
  int commit_ret5 = -1;
  EQ(0, SSH::abort_tx(R.tenant_id_, loc1, tx_id5));
  std::thread th5([&conn5, &commit_ret5]() {
    commit_ret5 = conn5->commit();
  });

  ob_usleep(200 * 1000);

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));

  // make wrs check approve
  EQ(0, SSH::modify_wrs(R.tenant_id_, loc2));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }

  inject_tx_fault_helper.release();

  th.join();
  th2.join();
  th3.join();
  th4.join();
  th5.join();

  EQ(0, commit_ret);
  EQ(0, commit_ret2);
  EQ(0, commit_ret3);
  EQ(0, commit_ret4);
  NEQ(0, commit_ret5);
  int64_t val1 = 0;
  int64_t val2 = 0;
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu1", val1));
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu2", val2));

  EQ(400, val1);
  EQ(400, val2);

  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, loc1));
  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, loc2));
  EQ(0, SSH::freeze_tx_data(R.tenant_id_, loc1));
  EQ(0, SSH::freeze_tx_data(R.tenant_id_, loc2));
  EQ(0, SSH::ls_reboot(R.tenant_id_, loc1));
  EQ(0, SSH::ls_reboot(R.tenant_id_, loc2));
}

TEST_F(ObTransferTx, transfer_batch)
{
  TRANSFER_CASE_PREPARE;
  sql_proxy.write("alter system set _transfer_start_trans_timeout='5s'",affected_rows);

  sql_proxy.write("alter system set _transfer_start_trans_timeout = '10s'", affected_rows);
  std::set<sqlclient::ObISQLConnection*> jobs;
  for (int i =0 ;i< 5000;i++) {
    sqlclient::ObISQLConnection *conn = NULL;
    EQ(0, sql_proxy.acquire(conn));
    EQ(0, SSH::write(conn, "set autocommit=0"));
    EQ(0, SSH::write(conn, "insert into stu2 values(100)"));
    jobs.insert(conn);
  }

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }
  for (auto iter = jobs.begin();iter !=jobs.end();iter++) {
    EQ(0, (*iter)->commit());
    sql_proxy.close(*iter, 0);
  }
  int64_t sum = 0;
  EQ(0, SSH::select_int64(sql_proxy, "select sum(col) as val from stu2", sum));
  EQ(100 * 5000, sum);
  sql_proxy.write("alter system set _transfer_start_trans_timeout='1s'",affected_rows);

  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, loc1));
  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, loc2));
  EQ(0, SSH::freeze_tx_data(R.tenant_id_, loc1));
  EQ(0, SSH::freeze_tx_data(R.tenant_id_, loc2));
  EQ(0, SSH::ls_reboot(R.tenant_id_, loc1));
  EQ(0, SSH::ls_reboot(R.tenant_id_, loc2));
}

/*
TEST_F(ObTransferTx, replay_from_middle)
{
  TRANSFER_CASE_PREPARE;

  sqlclient::ObISQLConnection *conn = NULL;
  EQ(0, sql_proxy.acquire(conn));
  EQ(0, SSH::write(conn, "set autocommit=0"));
  EQ(0, SSH::write(conn, "insert into test.stu1 values(100)", affected_rows));
  EQ(0, SSH::wait_checkpoint_newest(R.tenant_id_, loc1));
  int64_t val = -1;
  EQ(0, SSH::select_int64(conn, "select get_lock('test_lock', 10) val", val));
  EQ(1, val);
  EQ(0, conn->commit());

  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, loc1));
  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, loc2));
  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, ObLSID(1)));
  EQ(0, SSH::freeze_tx_data(R.tenant_id_, loc1));
  EQ(0, SSH::freeze_tx_data(R.tenant_id_, loc2));
  EQ(0, SSH::freeze_tx_data(R.tenant_id_, ObLSID(1)));
  EQ(0, SSH::ls_reboot(R.tenant_id_, loc1));
  EQ(0, SSH::ls_reboot(R.tenant_id_, loc2));
  EQ(0, SSH::ls_reboot(R.tenant_id_, ObLSID(1)));

  LOGI("release_lock");
  val = -1;
  EQ(0, SSH::select_int64(conn, "select release_lock('test_lock')", val));
  // session end lock release
  LOGI("get_lock");
  val = -1;
  EQ(0, SSH::select_int64(sql_proxy, "select get_lock('test_lock', 1) val", val));
  EQ(1, val);
  // session end lock release
  LOGI("get_lock");
  val = -1;
  EQ(0, SSH::select_int64(sql_proxy, "select get_lock('test_lock', 1) val", val));
  EQ(1, val);
}
*/
TEST_F(ObTransferTx, transfer_mds_trans)
{
  TRANSFER_CASE_PREPARE;

  ObMySQLTransaction trans;
  EQ(0, trans.start(GCTX.sql_proxy_, R.tenant_id_));
  EQ(0, trans.write(R.tenant_id_, "insert into test.stu1 values(100)", affected_rows));
  EQ(0, trans.write(R.tenant_id_, "insert into test.stu2 values(100)", affected_rows));
  //EQ(0, sql_proxy.write("alter system minor freeze", affected_rows));
  // make it replay from middle
  EQ(0, SSH::wait_checkpoint_newest(R.tenant_id_, loc1));
  EQ(0, SSH::wait_checkpoint_newest(R.tenant_id_, loc2));
  EQ(0, trans.write(R.tenant_id_, "insert into test.stu1 values(200)", affected_rows));
  EQ(0, trans.write(R.tenant_id_, "insert into test.stu2 values(200)", affected_rows));
  observer::ObInnerSQLConnection *conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection());
  char buf[10];
  ObRegisterMdsFlag flag;
  EQ(0, conn->register_multi_data_source(R.tenant_id_,
                                         loc1,
                                         ObTxDataSourceType::TEST3,
                                         buf,
                                         10,
                                         flag));

  EQ(0, conn->register_multi_data_source(R.tenant_id_,
                                         loc2,
                                         ObTxDataSourceType::TEST3,
                                         buf,
                                         10,
                                         flag));

  EQ(0, conn->register_multi_data_source(R.tenant_id_,
                                         loc2,
                                         ObTxDataSourceType::TEST3,
                                         buf,
                                         10,
                                         flag));
  ObTransID tx_id;
  EQ(0, SSH::g_select_int64(R.tenant_id_, "select trans_id as val from __all_virtual_trans_stat where is_exiting=0 and session_id<=1 limit 1", tx_id.tx_id_));
  LOGI("find active_tx tx_id:%ld", tx_id.get_id());

  EQ(0, sql_proxy.write("alter tablegroup tg1 add stu1,stu2;", affected_rows));
  EQ(0, do_balance(R.tenant_id_));

  // wait
  while (true) {
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu1", loc1));
    EQ(0, SSH::select_table_loc(R.tenant_id_, "stu2", loc2));
    if (loc1 == loc2) {
      break;
    }
    ::sleep(1);
  }

  EQ(0, trans.end(false));
  int64_t val1 = 0;
  int64_t val2 = 0;
  EQ(0, SSH::select_int64(sql_proxy, "select count(col) as val from stu1", val1));
  EQ(0, SSH::select_int64(sql_proxy, "select count(col) as val from stu2", val2));

  EQ(0, val1);
  EQ(0, val2);

  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, loc1));
  EQ(0, SSH::freeze_tx_ctx(R.tenant_id_, loc2));
  EQ(0, SSH::freeze_tx_data(R.tenant_id_, loc1));
  EQ(0, SSH::freeze_tx_data(R.tenant_id_, loc2));
  LOGI("ls_reboot");
  EQ(0, SSH::ls_reboot(R.tenant_id_, loc1));
  EQ(0, SSH::ls_reboot(R.tenant_id_, loc2));
}

TEST_F(ObTransferTx, create_more_ls)
{
  for (int i=0;i<8;i++) {
    EQ(0, SSH::create_ls(R.tenant_id_, get_curr_observer().self_addr_));
  }
  int64_t ls_count = 0;
  EQ(0, SSH::g_select_int64(R.tenant_id_, "select count(ls_id) as val from __all_ls where ls_id!=1", ls_count));
  EQ(10, ls_count);
}

TEST_F(ObTransferTx, bench)
{
  int64_t affected_rows = 0;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  // clean
  EQ(0, sql_proxy.write("drop database if exists test", affected_rows));
  EQ(0, sql_proxy.write("create database if not exists test", affected_rows));
  EQ(0, sql_proxy.write("drop tablegroup if exists tg1", affected_rows));
  EQ(0, sql_proxy.write("use test", affected_rows));
  EQ(0, do_balance(R.tenant_id_));
  rootserver::ObNewTableTabletAllocator::alloc_tablet_ls_offset_ = 0;

  std::vector<std::string> tables;
  for (int i=1;i<=10;i++) {
    ObSqlString sql;
    string table_name = "stu"+std::to_string(i);
    sql.assign_fmt("create table %s(col int)", table_name.c_str());
    tables.push_back(table_name);
    EQ(0, sql_proxy.write(sql.ptr(), affected_rows));
  }
  EQ(0, sql_proxy.write("create tablegroup tg1 sharding='NONE';", affected_rows));

  bool bench_stop = false;
  int bench_err = 0;
  std::vector<std::thread> ths;
  for (int idx = 0; idx < 200; idx++) {
   ths.push_back(std::thread([&sql_proxy, &bench_stop, &bench_err, tables] () {
      int ret = OB_SUCCESS;
      DEFER(if (OB_FAIL(ret)) bench_err = ret;);
      int64_t affected_rows;
      sqlclient::ObISQLConnection *conn = NULL;
      if (OB_FAIL(sql_proxy.acquire(conn))) {
        LOG_WARN("acquire conn failed", KR(ret));
      } else if (OB_FAIL(SSH::write(conn, "set autocommit=0", affected_rows))) {
        LOG_WARN("execute write failed", KR(ret));
      }
      while (OB_SUCC(ret) && !bench_stop) {
        std::vector<int> query_count_limit_example = {1, 10, 100, 200};
        int query_count = rand() % query_count_limit_example.at(rand() % query_count_limit_example.size());
        if (query_count == 0) {
          query_count = 1;
        }
        for (int i = 0; OB_SUCC(ret) && i < query_count; i++) {
          ObSqlString sql;
          static int64_t pk = 0;
          sql.assign_fmt("insert into %s values (%ld)", tables.at(rand() % tables.size()).c_str(), ATOMIC_AAF(&pk, 1));
          if (OB_FAIL(SSH::write(conn, sql.ptr()))) {
            LOG_WARN("execute write failed", KR(ret), K(sql));
          } else {
            ob_usleep(rand()%100 * 1000);
          }
        }
        ObTransID tx_id;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(SSH::find_tx(conn, tx_id))) {
          LOG_WARN("find_tx failed", KR(ret));
        } else {
          if (rand() % 100 < 50) {
            if (OB_FAIL(conn->commit())) {
              LOG_WARN("execute commit failed", KR(ret), K(tx_id));
            }
          } else {
            if (OB_FAIL(ret = conn->rollback())) {
              LOG_WARN("execute rollback failed", KR(ret), K(tx_id));
            }
          }
        }
      }
    }));
  }

  int64_t start_time = ObTimeUtil::current_time();
  int64_t wait_time = 2 * 60 * 1000 * 1000;
  ObLSID loc,loc_tmp;
  while (ObTimeUtil::current_time() - start_time < wait_time && !bench_err) {
    int64_t task = 0;
    ObSqlString sql;
    sql.assign_fmt("select count(*) as val from __all_virtual_transfer_task where tenant_id=%ld", R.tenant_id_);
    EQ(0, SSH::g_select_int64(OB_SYS_TENANT_ID, sql.ptr(), task));
    if (task == 0) {
      LOGI("transfer task empty want to generate");
      bool table_loc_same = true;
      for (int i = 0; i < tables.size();i++) {
        EQ(0, SSH::select_table_loc(R.tenant_id_, tables.at(i).c_str(), loc_tmp));
        if (i == 0) {
          loc = loc_tmp;
        } else if (loc != loc_tmp) {
          table_loc_same = false;
        }
      }
      if (!table_loc_same) {
        for (auto &iter : tables) {
          sql.assign_fmt("alter tablegroup tg1 add %s", iter.c_str());
          EQ(0, sql_proxy.write(sql.ptr(), affected_rows));
        }
      } else {
        for (auto &iter : tables) {
          sql.assign_fmt("alter table %s tablegroup=''", iter.c_str());
          EQ(0, sql_proxy.write(sql.ptr(), affected_rows));
        }
      }
      int64_t start_time = ObTimeUtil::current_time();
      EQ(0, do_balance(R.tenant_id_));
      int64_t end_time = ObTimeUtil::current_time();
      LOGI("finish do_balance: timeuse=%ld", end_time -start_time);
    }
    ::sleep(3);
  }
  bench_stop = true;
  for (auto &th : ths) {
    th.join();
  }
  EQ(0, bench_err);
}

TEST_F(ObTransferTx, end)
{
  int64_t wait_us = R.time_sec_ * 1000 * 1000;
  while (ObTimeUtil::current_time() - R.start_time_ < wait_us) {
    ob_usleep(1000 * 1000);
  }
  R.stop_ = true;
  if (R.worker_.joinable()) {
    R.worker_.join();
  }
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
