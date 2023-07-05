// Copyright (c) 2021 OceanBase  cxf262476, 2021-10-18 - add interface of logstream
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include <gtest/gtest.h>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "logservice/ob_log_service.h"
#include "logservice/rcservice/ob_role_change_service.h"
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
using namespace logservice;
using namespace storage;
namespace unittest
{

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};

TestRunCtx RunCtx;
std::string TEST_FILE_NAME="role_change_service";
std::string LOGGER_FILE_NAME=TEST_FILE_NAME + "/role_change_service.log";
class RoleChangeService : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  RoleChangeService () : ObSimpleClusterTestBase(TEST_FILE_NAME) {}
};

TEST_F(RoleChangeService, unique_set)
{
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(1));

  ObLogService *log_service = MTL(ObLogService *);
  ObRoleChangeService *rc_service = &log_service->role_change_service_;
  int64_t size = RoleChangeEventSet::MAX_ARRAY_SIZE;
  int64_t base_ls_id = 10000;
  int64_t i = 0;
  int ret = OB_SUCCESS;
  RoleChangeEventSet &set = rc_service->rc_set_;
  for (i = 0; i < size && OB_SUCC(ret); i++) {
    RoleChangeEvent event(RoleChangeEventType::ROLE_CHANGE_CB_EVENT_TYPE, ObLSID(i+base_ls_id));
    if (OB_FAIL(set.insert(event))) {
      CLOG_LOG(ERROR, "on_role_change failed", K(ret), K(base_ls_id+i));
    }
  }
  EXPECT_EQ(i, size);
  RoleChangeEvent event(RoleChangeEventType::ROLE_CHANGE_CB_EVENT_TYPE, ObLSID(base_ls_id));
  EXPECT_EQ(OB_ENTRY_EXIST, set.insert(event));
  RoleChangeEvent event1(RoleChangeEventType::ROLE_CHANGE_CB_EVENT_TYPE, ObLSID(base_ls_id+1000000));
  EXPECT_EQ(OB_SIZE_OVERFLOW, set.insert(event1));
  // 第一个slot将会被清空
  EXPECT_EQ(OB_SUCCESS, rc_service->on_role_change(base_ls_id));
  RoleChangeEvent event2(RoleChangeEventType::ROLE_CHANGE_CB_EVENT_TYPE, ObLSID(base_ls_id+1));
  EXPECT_EQ(OB_ENTRY_EXIST, set.insert(event2));
  sleep(1);
  {
    // 清空第一个slot
    rc_service->rc_set_.events_[0].reset();
    CLOG_LOG(ERROR, "runlin trace1");
    ObAddr dest_addr(ObAddr::VER::IPV4, "127.0.0.1", 1234);
    RoleChangeEvent event(RoleChangeEventType::CHANGE_LEADER_EVENT_TYPE, ObLSID(base_ls_id), dest_addr);
    EXPECT_EQ(OB_SUCCESS, set.insert(event));
    EXPECT_EQ(OB_ENTRY_EXIST, set.insert(event));

    rc_service->rc_set_.events_[1].reset();
    ObAddr dest_addr1(ObAddr::VER::IPV4, "127.0.0.1", 123);
    RoleChangeEvent event1(RoleChangeEventType::CHANGE_LEADER_EVENT_TYPE, ObLSID(base_ls_id+1), dest_addr1);
    EXPECT_EQ(OB_SUCCESS, set.insert(event1));
    ObAddr dest_addr2(ObAddr::VER::IPV4, "127.0.0.1", 1235);
    event1.dst_addr_ = dest_addr2;
    EXPECT_EQ(OB_ENTRY_EXIST, set.insert(event1));
    CLOG_LOG(ERROR, "runlin trace2");
  }
  for (int i = 0; i < size; i++) {
    rc_service->rc_set_.events_[i].reset();
  }
}

TEST_F(RoleChangeService, basic_func)
{
  CLOG_LOG(INFO, "start basic_func");
  const char *tenant_name = "runlin";
  EXPECT_EQ(OB_SUCCESS, create_tenant(tenant_name));
  uint64_t tenant_id = 0;
  EXPECT_EQ(OB_SUCCESS, get_tenant_id(tenant_id, tenant_name));
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  // 上下文切换为runlin租户
  ASSERT_EQ(OB_SUCCESS, guard.switch_to(tenant_id));
  ObLogService *log_service = MTL(ObLogService*);
  ASSERT_NE(nullptr, log_service);
  ObRoleChangeService *role_change_service = &log_service->role_change_service_;
  ASSERT_NE(nullptr, role_change_service);
  ObLSService *ls_service = MTL(ObLSService*);
  ASSERT_NE(nullptr, ls_service);
  {
    ObLSHandle ls;
    EXPECT_EQ(OB_SUCCESS, ls_service->get_ls(ObLSID(1001), ls, ObLSGetMod::LOG_MOD));
    // 停止回放
    EXPECT_EQ(OB_SUCCESS, ls.get_ls()->disable_replay());
    ObLogHandler *log_handler = &ls.get_ls()->log_handler_;
    EXPECT_EQ(LEADER, log_handler->role_);
    log_handler->role_ = FOLLOWER;
    RoleChangeEvent event_stack;
    event_stack.event_type_ = RoleChangeEventType::ROLE_CHANGE_CB_EVENT_TYPE;
    event_stack.ls_id_ = ObLSID(1001);
    ObRoleChangeService::RetrySubmitRoleChangeEventCtx retry_ctx;
    EXPECT_EQ(OB_TIMEOUT, role_change_service->handle_role_change_cb_event_for_log_handler_(palf::AccessMode::APPEND, ls.get_ls(), retry_ctx));
    EXPECT_EQ(retry_ctx.reason_, ObRoleChangeService::RetrySubmitRoleChangeEventReason::WAIT_REPLAY_DONE_TIMEOUT);
    EXPECT_EQ(retry_ctx.need_retry(), true);
    EXPECT_EQ(OB_SUCCESS, role_change_service->on_role_change(1001));
    sleep(10);
  }
  EXPECT_EQ(OB_SUCCESS, delete_tenant("runlin"));
  CLOG_LOG(INFO, "end basic_func");
}


TEST_F(RoleChangeService, test_offline)
{
  CLOG_LOG(INFO, "start test_offline");
  EXPECT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy_with_short_wait());
  share::ObTenantSwitchGuard tguard;
  ASSERT_EQ(OB_SUCCESS, tguard.switch_to(1));
  ObLogService *log_service = MTL(ObLogService *);
  ObLogApplyService *apply_service = &(log_service->apply_service_);
  ObApplyStatusGuard guard;
  ObLSID id(1);
  EXPECT_EQ(OB_SUCCESS, apply_service->get_apply_status(id, guard));
  // 开启事务，提交日志
  // offline是否卡卡住
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy_with_short_wait();
  ObSqlString sql;
  {
    sql.assign_fmt("create table if not exists t1 (c1 int, c2 int, primary key(c1))");
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    OB_LOG(INFO, "create_table succ");
    sleep(30);
  }
  ObApplyStatus *status = guard.get_apply_status();
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("set autocommit=0"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  CLOG_LOG(INFO, "runlin trace begin set trx");
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("set global ob_trx_timeout=2000000"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  EXPECT_EQ(OB_SUCCESS, status->unregister_file_size_cb());
  sleep(1);
  int ret = OB_SUCCESS;
  //ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  CLOG_LOG(INFO, "runlin trace begin");
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("begin"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t1 values(%d, %d)", 0, 9));
  sql_proxy.write(sql.ptr(), affected_rows);
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t1 values(%d, %d)", 1, 9));
  sql_proxy.write(sql.ptr(), affected_rows);
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("commit"));
  // commit 会卡住
  CLOG_LOG(INFO, "runlin trace before commit");
  sql_proxy.write(sql.ptr(), affected_rows);

  CLOG_LOG(INFO, "runlin trace commit success");
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  EXPECT_EQ(OB_SUCCESS, ls_service->get_ls(id, ls_handle, ObLSGetMod::LOG_MOD));
  ObLS *ls = ls_handle.get_ls();
  int tmp_ret = OB_SUCCESS;
  while (OB_SUCCESS != (tmp_ret = ls->offline())) {
    CLOG_LOG(WARN, "offline failed", K(tmp_ret));
  }
  sleep(5);
  ObLogHandler *log_handler = ls->get_log_handler();
  EXPECT_EQ(-1, log_handler->apply_status_->proposal_id_);
  sleep(5);
  EXPECT_EQ(common::FOLLOWER, log_handler->role_);
  EXPECT_EQ(true, log_handler->is_offline_);
  EXPECT_EQ(-1, log_handler->apply_status_->proposal_id_);
  CLOG_LOG(INFO, "end test_offline");
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  int c = 0;
  int time_sec = 0;
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
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
