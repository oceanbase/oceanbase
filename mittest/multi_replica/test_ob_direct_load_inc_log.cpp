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

#include "env/ob_fast_bootstrap.h"
#include "env/ob_multi_replica_util.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/tx/ob_dup_table_lease.h"

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

#define CUR_TEST_CASE_NAME ObDirectLoadIncLogTest

DEFINE_MULTI_ZONE_TEST_CASE_CLASS

MULTI_REPLICA_TEST_MAIN_FUNCTION(test_direct_load_inc_);

#define DEFAULT_LOAD_ROW_CNT 30

namespace oceanbase
{
namespace unittest
{
class ObTestExtraLogCb : public logservice::AppendCb
{
public:
  int on_success()
  {
    int ret = OB_SUCCESS;
    TRANS_LOG(INFO, "[ObMultiReplicaTestBase] invoke test on_success", K(ret), KP(this));
    return ret;
  }
  int on_failure()
  {
    int ret = OB_SUCCESS;
    TRANS_LOG(INFO, "[ObMultiReplicaTestBase] invoke test on_failure", K(ret), KP(this));
    return ret;
  }
};

void submit_ddl_redo_log(transaction::ObPartTransCtx *tx_ctx)
{
  ObDDLRedoLog ddl_redo;

  for (int i = 0; i < 5; i++) {
    ObTestExtraLogCb *test_log_cb =
        static_cast<ObTestExtraLogCb *>(ob_malloc(sizeof(ObTestExtraLogCb), "TestExtralCb"));
    new (test_log_cb) ObTestExtraLogCb();
    int64_t replay_hint = GETTID();
    share::SCN scn;
    scn.set_max();

    ASSERT_EQ(OB_SUCCESS,
              tx_ctx->submit_direct_load_inc_redo_log(ddl_redo, test_log_cb, replay_hint, scn));

    usleep(1000);
  }
}

void submit_ddl_start_log(transaction::ObPartTransCtx *tx_ctx)
{
  storage::ObDDLIncStartLog ddl_start;
  ddl_start.log_basic_.tablet_id_ = 101;//GETTID();

  ObTestExtraLogCb *test_log_cb =
      static_cast<ObTestExtraLogCb *>(ob_malloc(sizeof(ObTestExtraLogCb), "TestExtralCb"));
  new (test_log_cb) ObTestExtraLogCb();
  int64_t replay_hint = 0;
  share::SCN scn;
  scn.set_max();

  ASSERT_EQ(OB_SUCCESS, tx_ctx->submit_direct_load_inc_start_log(ddl_start, test_log_cb, scn));
}

void submit_ddl_end_log(transaction::ObPartTransCtx *tx_ctx)
{
  storage::ObDDLIncCommitLog ddl_commit;
  ddl_commit.log_basic_.tablet_id_ = 101;//GETTID();

  ObTestExtraLogCb *test_log_cb =
      static_cast<ObTestExtraLogCb *>(ob_malloc(sizeof(ObTestExtraLogCb), "TestExtralCb"));
  new (test_log_cb) ObTestExtraLogCb();
  int64_t replay_hint = 0;
  share::SCN scn;
  scn.set_max();

  ASSERT_EQ(OB_SUCCESS, tx_ctx->submit_direct_load_inc_commit_log(ddl_commit, test_log_cb, scn));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_table)
{

  int ret = OB_SUCCESS;
  CREATE_TEST_TENANT(test_tenant_id);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] create test tenant success", K(test_tenant_id));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  WRITE_SQL_BY_CONN(test_conn,
                    "CREATE TABLE test_t1( "
                    "id_x int, "
                    "id_y int, "
                    "id_z int, "
                    "PRIMARY KEY(id_x)"
                    ") duplicate_scope='cluster' PARTITION BY hash(id_x) partitions 10;");

  std::string primary_zone_sql = "ALTER TENANT " + std::string(DEFAULT_TEST_TENANT_NAME)
                                 + " set primary_zone='zone1; zone3; zone2';";
  WRITE_SQL_BY_CONN(test_conn, primary_zone_sql.c_str());

  READ_SQL_BY_CONN(test_conn, table_info_result,
                   "select table_id, duplicate_scope from "
                   "oceanbase.__all_table where table_name = 'test_t1' ");

  ASSERT_EQ(OB_SUCCESS, table_info_result->next());
  int64_t table_id;
  int64_t dup_scope;
  ASSERT_EQ(OB_SUCCESS, table_info_result->get_int("table_id", table_id));
  ASSERT_EQ(OB_SUCCESS, table_info_result->get_int("duplicate_scope", dup_scope));
  ASSERT_EQ(true, table_id > 0);
  ASSERT_EQ(true, dup_scope != 0);

  std::string tablet_count_sql =
      "select count(*), ls_id from oceanbase.__all_tablet_to_ls where table_id = "
      + std::to_string(table_id) + " group by ls_id order by count(*)";
  READ_SQL_BY_CONN(test_conn, tablet_count_result, tablet_count_sql.c_str());
  int64_t tablet_count = 0;
  int64_t ls_id_num = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_count_result->next());
  ASSERT_EQ(OB_SUCCESS, tablet_count_result->get_int("count(*)", tablet_count));
  ASSERT_EQ(OB_SUCCESS, tablet_count_result->get_int("ls_id", ls_id_num));
  ASSERT_EQ(10, tablet_count);
  ASSERT_EQ(true, share::ObLSID(ls_id_num).is_valid());

  GET_LS(test_tenant_id, ls_id_num, ls_handle);

  WRITE_SQL_BY_CONN(test_conn, "set autocommit = false;");
  WRITE_SQL_BY_CONN(test_conn, "begin;");

  for (int i = 1; i <= DEFAULT_LOAD_ROW_CNT; i++) {
    std::string insert_sql_str = "INSERT INTO test_t1 VALUES(" + std::to_string(i) + ", 0 , 0)";
    WRITE_SQL_BY_CONN(test_conn, insert_sql_str.c_str());
  }

  int64_t tx_id_num;
  // GET_RUNNGING_TRX_ID(test_conn, tx_id_num);
  std::string template_sql_str = "INSERT INTO test_t1 VALUES(" + std::to_string(1) + ", 0 , 0)";
  GET_TX_ID_FROM_SQL_AUDIT(test_conn, template_sql_str.c_str(), tx_id_num);

  transaction::ObPartTransCtx *tx_ctx = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tx_ctx(ObTransID(tx_id_num), false, tx_ctx));

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] get target tx ctx", K(ret), K(tx_id_num), KPC(tx_ctx));

  submit_ddl_start_log(tx_ctx);

  std::thread t1(submit_ddl_redo_log, tx_ctx);
  std::thread t2(submit_ddl_redo_log, tx_ctx);
  std::thread t3(submit_ddl_redo_log, tx_ctx);
  std::thread t4(submit_ddl_redo_log, tx_ctx);

  t1.join();
  t2.join();
  t3.join();
  t4.join();

  // submit_ddl_end_log(tx_ctx);

  WRITE_SQL_BY_CONN(test_conn, "commit;");

  // Don't free log cb in thread
}
} // namespace unittest

} // namespace oceanbase
