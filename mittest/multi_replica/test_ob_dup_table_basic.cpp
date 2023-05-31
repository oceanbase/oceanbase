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

#define CUR_TEST_CASE_NAME ObDupTableBasicTest

DEFINE_MULTI_ZONE_TEST_CASE_CLASS

MULTI_REPLICA_TEST_MAIN_FUNCTION(test_dup_table_basic_);

namespace oceanbase
{
namespace unittest
{

struct DupTableBasicArg
{
  uint64_t tenant_id_;
  int64_t ls_id_num_;
  int64_t table_id_;
  int64_t tablet_count_;
  ObSEArray<int64_t, 10> tablet_id_array_;

  TO_STRING_KV(K(tenant_id_), K(ls_id_num_), K(table_id_), K(tablet_count_), K(tablet_id_array_));

  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER(DupTableBasicArg,
                    tenant_id_,
                    ls_id_num_,
                    table_id_,
                    tablet_count_,
                    tablet_id_array_);

static DupTableBasicArg static_basic_arg_;
sqlclient::ObISQLConnection *static_test_conn_ = nullptr;

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_dup_table)
{
  int ret = OB_SUCCESS;
  CREATE_TEST_TENANT(test_tenant_id);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] create test tenant success", K(test_tenant_id));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  WRITE_SQL_BY_CONN(test_conn,
                    "CREATE TABLE Persons( "
                    "PersonID int, "
                    " LastName varchar(255),"
                    " FirstName varchar(255),"
                    " Address varchar(255), "
                    " City varchar(255),  "
                    " PRIMARY KEY(PersonID)"
                    ") duplicate_scope='cluster' PARTITION BY hash(PersonID) partitions 10;");

  READ_SQL_BY_CONN(test_conn, table_info_result,
                   "select table_id, duplicate_scope from "
                   "oceanbase.__all_table where table_name = 'Persons' ");

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

  std::string tablet_id_sql = "select tablet_id from oceanbase.__all_tablet_to_ls where table_id = "
                              + std::to_string(table_id)
                              + " and ls_id = " + std::to_string(ls_id_num);
  READ_SQL_BY_CONN(test_conn, tablet_id_reult, tablet_id_sql.c_str());
  while (OB_SUCC(tablet_id_reult->next())) {
    int64_t id = 0;
    ASSERT_EQ(OB_SUCCESS, tablet_id_reult->get_int("tablet_id", id));
    ASSERT_EQ(true, ObTabletID(id).is_valid());
    ASSERT_EQ(OB_SUCCESS, static_basic_arg_.tablet_id_array_.push_back(id));
  }
  ASSERT_EQ(tablet_count, static_basic_arg_.tablet_id_array_.count());
  ASSERT_EQ(OB_ITER_END, ret);
  ret = OB_SUCCESS;

  GET_LS(test_tenant_id, ls_id_num, ls_handle);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] -------- before wait dup tablet discover", K(ret),
             K(ls_id_num), K(tablet_count));
  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.get_dup_tablet_count()
                          == tablet_count,
                      20 * 1000 * 1000, 100 * 1000);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] -------- after wait dup tablet discover", K(ret),
             K(ls_id_num), K(ls_handle.get_ls()->dup_table_ls_handler_.get_dup_tablet_count()));
  ASSERT_EQ(OB_SUCCESS, ret /*has_dup_tablet*/);

  // DupTableBasicArg static_basic_arg_;
  static_basic_arg_.tenant_id_ = test_tenant_id;
  static_basic_arg_.ls_id_num_ = ls_id_num;
  static_basic_arg_.table_id_ = table_id;
  static_basic_arg_.tablet_count_ = tablet_count;

  // char tmp_buf[2048];
  // memset(tmp_buf, 0, 2048);
  // int64_t pos = 0;
  // ASSERT_EQ(OB_SUCCESS, static_basic_arg_.serialize(tmp_buf, 2047, pos));
  std::string tmp_str;
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<DupTableBasicArg>::serialize_arg(static_basic_arg_, tmp_str));
  finish_event("CREATE_DUP_TABLE", tmp_str);
}

void check_dup_tablet_replayed(ObMultiReplicaTestBase *multi_replica_test_ptr, int64_t zone_id)
{
  int ret = OB_SUCCESS;

  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, multi_replica_test_ptr->wait_event_finish("CREATE_DUP_TABLE", tmp_event_val,
                                                                  30 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<DupTableBasicArg>::deserialize_arg(static_basic_arg_, tmp_event_val));
  ASSERT_EQ(true, static_basic_arg_.ls_id_num_ > 0);

  uint64_t test_tenant_id = static_basic_arg_.tenant_id_;
  // ASSERT_EQ(OB_SUCCESS, multi_replica_test_ptr->get_tenant_id(test_tenant_id));
  GET_LS(test_tenant_id, static_basic_arg_.ls_id_num_, ls_handle);
  ASSERT_EQ(false, ls_handle.get_ls()->dup_table_ls_handler_.is_master());
  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.get_dup_tablet_count()
                          == static_basic_arg_.tablet_count_,
                      20 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(OB_SUCCESS, ret /*has_dup_tablet*/);
  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()
                              ->dup_table_ls_handler_.lease_mgr_ptr_->follower_lease_info_
                              .durable_lease_.request_ts_
                          > 0,
                      20 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(OB_SUCCESS, ret /*lease request ts*/);
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), follower_replay_dup_tablet)
{
  check_dup_tablet_replayed(this, 2);
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(3), follower_replay_dup_tablet)
{

  check_dup_tablet_replayed(this, 3);
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), leader_lease_info)
{
  int ret = OB_SUCCESS;

  GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.ls_id_num_, ls_handle);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] -------- before wait dup lease request", K(ret),
             K(static_basic_arg_));
  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.lease_mgr_ptr_->leader_lease_map_.size() == 2,
      20 * 1000 * 1000, 100 * 1000);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] -------- after wait dup lease request", K(ret),
             K(static_basic_arg_),
             K(ls_handle.get_ls()->dup_table_ls_handler_.lease_mgr_ptr_->leader_lease_map_.size()));

  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.lease_mgr_ptr_->last_lease_req_cache_handle_time_
              > 0
          && ls_handle.get_ls()->dup_table_ls_handler_.log_operator_ != nullptr
          && !ls_handle.get_ls()->dup_table_ls_handler_.log_operator_->is_busy(),
      5 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(OB_SUCCESS, ret /*logging*/);
  DupTableLeaderLeaseMap::const_iterator iter =
      ls_handle.get_ls()->dup_table_ls_handler_.lease_mgr_ptr_->leader_lease_map_.begin();
  int64_t lease_valid_cnt = 0;
  while (iter
         != ls_handle.get_ls()->dup_table_ls_handler_.lease_mgr_ptr_->leader_lease_map_.end()) {
    EXPECT_EQ(true, iter->second.lease_expired_ts_ > ObTimeUtility::fast_current_time());
    if (iter->second.lease_expired_ts_ > ObTimeUtility::fast_current_time()) {
      lease_valid_cnt++;
    }
    SERVER_LOG(INFO, "[ObMultiReplicaTestBase] check dup table lease expired", K(ret),
               K(static_basic_arg_), K(iter->first), K(iter->second));
    iter++;
  }
  ASSERT_EQ(true, lease_valid_cnt > 0);
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), dup_table_trx_insert_exec)
{
  int ret = OB_SUCCESS;

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  static_test_conn_ = test_conn;

  WRITE_SQL_BY_CONN(test_conn, "begin;");

  WRITE_SQL_BY_CONN(test_conn, "INSERT INTO Persons VALUES (1, 'a','aa' , 'aaa','a')");

  // TODO check dup tx after submit redo

  // fprintf(stdout, "============== cur conn: %d", test_conn->get_sessid());
  // sleep(600);
}

bool check_all_tablet_follower_readable(ObMultiReplicaTestBase *multi_replica_test_ptr,
                                        ObLS *ls_ptr)
{
  int64_t readable_tablet_cnt = 0;
  EXPECT_TRUE(static_basic_arg_.tablet_id_array_.count() > 0);
  for (int i = 0; i < static_basic_arg_.tablet_id_array_.count(); i++) {
    ObTabletID tablet_id(static_basic_arg_.tablet_id_array_[i]);
    bool readable = false;
    share::SCN max_replayed_scn;
    EXPECT_EQ(OB_SUCCESS, ls_ptr->get_max_decided_scn(max_replayed_scn));
    EXPECT_EQ(OB_SUCCESS, ls_ptr->dup_table_ls_handler_.check_dup_tablet_readable(
                              tablet_id, share::SCN::min_scn(), false /*read_from_leader*/,
                              max_replayed_scn, readable));
    if (readable) {
      readable_tablet_cnt++;
    } else {
      SERVER_LOG(INFO, "[ObMultiReplicaTestBase] this dup tablet has not been readable now",
                 K(tablet_id), K(max_replayed_scn), K(readable));
    }
  }

  ls_ptr->dup_table_ls_handler_.interface_stat_.dup_table_follower_read_succ_cnt_ -=
      readable_tablet_cnt;
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] check all dup tablet readable on follower",
             K(static_basic_arg_), K(readable_tablet_cnt),
             K(static_basic_arg_.tablet_id_array_.count()));

  return readable_tablet_cnt == static_basic_arg_.tablet_id_array_.count();
}

void check_dup_table_insert_readable(ObMultiReplicaTestBase *multi_replica_test_ptr,
                                     int64_t expected_row_cnt,
                                     bool expected_follower_read)
{
  int ret = OB_SUCCESS;

  common::ObMySQLProxy &test_tenant_sql_proxy =
      multi_replica_test_ptr->get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.ls_id_num_, ls_handle);
  int64_t origin_follower_read_cnt =
      ls_handle.get_ls()->dup_table_ls_handler_.interface_stat_.dup_table_follower_read_succ_cnt_;

  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] origin follower read cnt",
             K(static_basic_arg_), K(origin_follower_read_cnt),
             K(ls_handle.get_ls()->dup_table_ls_handler_.interface_stat_.dup_table_follower_read_succ_cnt_),
             K(static_basic_arg_.tablet_id_array_.count()));

  if (expected_follower_read) {
    RETRY_UNTIL_TIMEOUT(
        check_all_tablet_follower_readable(multi_replica_test_ptr, ls_handle.get_ls()),
        30 * 1000 * 1000, 1 * 1000 * 1000);
    ASSERT_EQ(OB_SUCCESS, ret /*all tablet readable*/);
  }
  ASSERT_EQ(
      origin_follower_read_cnt,
      ls_handle.get_ls()->dup_table_ls_handler_.interface_stat_.dup_table_follower_read_succ_cnt_);

  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] origin follower read cnt2",
             K(static_basic_arg_), K(origin_follower_read_cnt),
             K(ls_handle.get_ls()->dup_table_ls_handler_.interface_stat_.dup_table_follower_read_succ_cnt_),
             K(static_basic_arg_.tablet_id_array_.count()));

  int64_t row_cnt = 0;
  READ_SQL_BY_CONN(test_conn, read_insert_result, "select count(*) from Persons;");
  ASSERT_EQ(OB_SUCCESS, read_insert_result->next());
  ASSERT_EQ(OB_SUCCESS, read_insert_result->get_int("count(*)", row_cnt));
  ASSERT_EQ(row_cnt, expected_row_cnt);

  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] origin follower read cnt3",
             K(static_basic_arg_), K(origin_follower_read_cnt),
             K(ls_handle.get_ls()->dup_table_ls_handler_.interface_stat_.dup_table_follower_read_succ_cnt_),
             K(static_basic_arg_.tablet_id_array_.count()));

  if (expected_follower_read) {
    ASSERT_EQ(true,
              origin_follower_read_cnt + static_basic_arg_.tablet_count_
                  <= ls_handle.get_ls()
                         ->dup_table_ls_handler_.interface_stat_.dup_table_follower_read_succ_cnt_);
  }

  ASSERT_EQ(expected_row_cnt, row_cnt);
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), dup_table_trx_read_uncommitted)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  check_dup_table_insert_readable(this, 0, true /*expected_follower_read*/);
  ASSERT_EQ(OB_SUCCESS, finish_event("ZONE2_READ_INSERT_UNCOMMITTED", ""));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(3), dup_table_trx_read_uncommitted)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  check_dup_table_insert_readable(this, 0, true /*expected_follower_read*/);
  ASSERT_EQ(OB_SUCCESS, finish_event("ZONE3_READ_INSERT_UNCOMMITTED", ""));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), dup_table_trx_insert_commit)
{
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("ZONE2_READ_INSERT_UNCOMMITTED", tmp_event_val, 30 * 1000 * 1000));
  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("ZONE3_READ_INSERT_UNCOMMITTED", tmp_event_val, 30 * 1000 * 1000));

  int64_t tx_id_num = 0;

  GET_TX_ID_FROM_SQL_AUDIT(static_test_conn_,
                           "INSERT INTO Persons VALUES (1, 'a','aa' , 'aaa','a')", tx_id_num);

  GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.ls_id_num_, ls_handle);
  TestTxCtxGuard tx_ctx_guard(tx_id_num, ls_handle.get_ls());
  ASSERT_EQ(OB_SUCCESS, tx_ctx_guard.init(true));

  // ASSERT_EQ(OB_SUCCESS, tx_ctx_guard.tx_ctx_->submit_redo_log(false));
  WRITE_SQL_BY_CONN(static_test_conn_, "commit;");

  ASSERT_EQ(true, tx_ctx_guard.tx_ctx_->exec_info_.is_dup_tx_);
  ASSERT_EQ(transaction::TransType::DIST_TRANS, tx_ctx_guard.tx_ctx_->exec_info_.trans_type_);

  ASSERT_EQ(OB_SUCCESS, finish_event("INSERT_TRX_COMMIT", ""));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), dup_table_trx_read_committed)
{
  int ret = OB_SUCCESS;
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("INSERT_TRX_COMMIT", tmp_event_val, 30 * 1000 * 1000));
  check_dup_table_insert_readable(this, 1, true /*expected_follower_read*/);
  ASSERT_EQ(OB_SUCCESS, finish_event("ZONE2_READ_INSERT_COMMITTED", ""));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(3), dup_table_trx_read_committed)
{
  int ret = OB_SUCCESS;
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("INSERT_TRX_COMMIT", tmp_event_val, 30 * 1000 * 1000, 100));
  check_dup_table_insert_readable(this, 1, true /*expected_follower_read*/);
  ASSERT_EQ(OB_SUCCESS, finish_event("ZONE3_READ_INSERT_COMMITTED", ""));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), remove_dup_table)
{
  int ret = OB_SUCCESS;

  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("ZONE2_READ_INSERT_COMMITTED", tmp_event_val, 30 * 1000 * 1000));
  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("ZONE3_READ_INSERT_COMMITTED", tmp_event_val, 30 * 1000 * 1000));

  WRITE_SQL_BY_CONN(static_test_conn_, "drop table Persons");
  GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.ls_id_num_, ls_handle);

  oceanbase::transaction::ObLSDupTabletsMgr::GC_DUP_TABLETS_TIME_INTERVAL = 100 * 1000;
  oceanbase::transaction::ObLSDupTabletsMgr::GC_DUP_TABLETS_FAILED_TIMEOUT =
      5 * oceanbase::transaction::ObLSDupTabletsMgr::GC_DUP_TABLETS_TIME_INTERVAL;
  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.get_dup_tablet_count() == 0,
                      5 * 1000 * 1000, 100 * 1000);
}

} // namespace unittest
} // namespace oceanbase
