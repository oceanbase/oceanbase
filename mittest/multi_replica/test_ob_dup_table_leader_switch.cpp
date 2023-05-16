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

#define CUR_TEST_CASE_NAME ObDupTableLeaderSwitch

DEFINE_MULTI_ZONE_TEST_CASE_CLASS

MULTI_REPLICA_TEST_MAIN_FUNCTION(test_dup_table_leader_switch_);

#define DEFAULT_LOAD_ROW_CNT 30

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

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_dup_table_and_load_data)
{

  int ret = OB_SUCCESS;
  CREATE_TEST_TENANT(test_tenant_id);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] create test tenant success", K(test_tenant_id));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);
  ACQUIRE_CONN_FROM_SQL_PROXY(sys_conn, get_curr_simple_server().get_sql_proxy());

  WRITE_SQL_BY_CONN(test_conn,
                    "CREATE TABLE test_t1( "
                    "id_x int, "
                    "id_y int, "
                    "id_z int, "
                    "PRIMARY KEY(id_x)"
                    ") duplicate_scope='cluster' PARTITION BY hash(id_x) partitions 10;");

  WRITE_SQL_BY_CONN(test_conn, "CREATE TABLE test_t2( "
                               "id_x int, "
                               "id_y int, "
                               "id_z int, "
                               "PRIMARY KEY(id_x)"
                               ") PARTITION BY hash(id_x) partitions 10;");

  std::string primary_zone_sql = "ALTER TENANT " + std::string(DEFAULT_TEST_TENANT_NAME)
                                 + " set primary_zone='zone1, zone3; zone2';";
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

  std::string ls_id_str = std::to_string(ls_id_num);
  std::string target_ip = local_ip_ + ":" + std::to_string(rpc_ports_[2]);
  std::string switch_leader_sql = "alter system switch replica leader ls=" + ls_id_str + " server='"
                                  + target_ip + "' tenant='tt1';";
  WRITE_SQL_BY_CONN(sys_conn, switch_leader_sql.c_str());

  ObSEArray<int64_t, 10> tablet_id_array;
  std::string tablet_id_sql = "select tablet_id from oceanbase.__all_tablet_to_ls where table_id = "
                              + std::to_string(table_id)
                              + " and ls_id = " + std::to_string(ls_id_num);
  READ_SQL_BY_CONN(test_conn, tablet_id_reult, tablet_id_sql.c_str());
  while (OB_SUCC(tablet_id_reult->next())) {
    int64_t id = 0;
    ASSERT_EQ(OB_SUCCESS, tablet_id_reult->get_int("tablet_id", id));
    ASSERT_EQ(true, ObTabletID(id).is_valid());
    ASSERT_EQ(OB_SUCCESS, tablet_id_array.push_back(id));
  }
  ASSERT_EQ(tablet_count, tablet_id_array.count());
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

  WRITE_SQL_BY_CONN(test_conn, "set autocommit = true;");

  for (int i = 1; i <= DEFAULT_LOAD_ROW_CNT; i++) {
    std::string insert_sql_str = "INSERT INTO test_t1 VALUES(" + std::to_string(i) + ", 0 , 0)";
    WRITE_SQL_BY_CONN(test_conn, insert_sql_str.c_str());
  }

  for (int i = 1; i <= DEFAULT_LOAD_ROW_CNT; i++) {
    std::string insert_sql_str = "INSERT INTO test_t2 VALUES(" + std::to_string(i) + ", 0 , 0)";
    WRITE_SQL_BY_CONN(test_conn, insert_sql_str.c_str());
  }

  int64_t row_cnt = 0;
  READ_SQL_BY_CONN(test_conn, read_insert_result, "select count(*) from test_t1;");
  ASSERT_EQ(OB_SUCCESS, read_insert_result->next());
  ASSERT_EQ(OB_SUCCESS, read_insert_result->get_int("count(*)", row_cnt));
  ASSERT_EQ(row_cnt, DEFAULT_LOAD_ROW_CNT);

  static_basic_arg_.tenant_id_ = test_tenant_id;
  static_basic_arg_.ls_id_num_ = ls_id_num;
  static_basic_arg_.table_id_ = table_id;
  static_basic_arg_.tablet_count_ = tablet_count;

  std::string tmp_str;
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<DupTableBasicArg>::serialize_arg(static_basic_arg_, tmp_str));
  ASSERT_EQ(OB_SUCCESS, finish_event("CREATE_DUP_TABLE", tmp_str));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(3), switch_to_follwer_forcedly)
{
  int ret = OB_SUCCESS;

  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("CREATE_DUP_TABLE", tmp_event_val, 30 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<DupTableBasicArg>::deserialize_arg(static_basic_arg_, tmp_event_val));

  ASSERT_EQ(OB_SUCCESS, wait_event_finish("INIT_ZONE2_SQL_PROXY", tmp_event_val, 30 * 60 * 1000));

  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());

  ASSERT_EQ(OB_SUCCESS, finish_event("START_RANDOM_SWITCH_FOLLOWER_FORCEDLY", ""));

  GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.ls_id_num_, ls_handle);

  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                      100 * 1000);

  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] -------- before block msg for leader revoke", K(ret));

  for (int i = 0; i < 10; i++) {
    BLOCK_MSG_PROCESSOR(6 * 1000 * 1000L);
    RETRY_UNTIL_TIMEOUT(!ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                        100 * 1000);
    RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                        100 * 1000);
    sleep(1);
  }
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] -------- after block msg for leader revoke", K(ret));

  ASSERT_EQ(OB_SUCCESS, finish_event("END_RANDOM_SWITCH_FOLLOWER_FORCEDLY", ""));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(3), switch_to_follwer_gracefully)
{
  int ret = OB_SUCCESS;

  ASSERT_EQ(OB_SUCCESS, finish_event("START_RANDOM_SWITCH_FOLLOWER_GRACEFULLY", ""));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy();
  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  GET_LS(static_basic_arg_.tenant_id_, static_basic_arg_.ls_id_num_, ls_handle);
  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                      100 * 1000);

  std::string ls_id_str = std::to_string(static_basic_arg_.ls_id_num_);
  std::string target_ip = local_ip_ + ":" + std::to_string(rpc_ports_[1]);

  std::string switch_leader_sql = "alter system switch replica leader ls=" + ls_id_str + " server='"
                                  + target_ip + "' tenant='tt1';";

  for (int i = 0; i < 10; i++) {
    WRITE_SQL_BY_CONN(test_conn, switch_leader_sql.c_str());
    RETRY_UNTIL_TIMEOUT(!ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                        100 * 1000);
    RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                        100 * 1000);
    sleep(1);
  }

  ASSERT_EQ(OB_SUCCESS, finish_event("END_RANDOM_SWITCH_FOLLOWER_GRACEFULLY", ""));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), update_in_leader_switch)
{
  int ret = OB_TIMEOUT;

  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("CREATE_DUP_TABLE", tmp_event_val, 30 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ASSERT_EQ(OB_SUCCESS, finish_event("INIT_ZONE2_SQL_PROXY", ""));
  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  int64_t update_trx_cnt = 0;
  std::string event_content;

  int res_ret_1, res_ret_2, commit_res_ret;
  res_ret_1 = res_ret_2 = commit_res_ret = OB_SUCCESS;

  bool is_waiting_forcedly = true;

  while (ret == OB_TIMEOUT) {
    update_trx_cnt++;
    WRITE_SQL_BY_CONN(test_conn, "begin");
    for (int i = 1; i <= DEFAULT_LOAD_ROW_CNT; i++) {
      std::string insert_sql_str = "UPDATE test_t1 set id_y =  " + std::to_string(update_trx_cnt)
                                   + " where id_x = " + std::to_string(i);
      WRITE_SQL_BY_CONN_INNER(test_conn, insert_sql_str.c_str(), res_ret_1);
      std::string insert_sql_str2 = "UPDATE test_t2 set id_y =  " + std::to_string(update_trx_cnt)
                                    + " where id_x = " + std::to_string(i);
      WRITE_SQL_BY_CONN_INNER(test_conn, insert_sql_str2.c_str(), res_ret_2);
      if (is_waiting_forcedly) {
        EXPECT_EQ(true, res_ret_1 == OB_SUCCESS || res_ret_1 == OB_TIMEOUT);
        EXPECT_EQ(true, res_ret_2 == OB_SUCCESS || res_ret_2 == OB_TIMEOUT);
      } else {
        EXPECT_EQ(OB_SUCCESS, res_ret_1);
        EXPECT_EQ(OB_SUCCESS, res_ret_2);
      }
    }
    WRITE_SQL_BY_CONN_INNER(test_conn, "commit", commit_res_ret);
    if (is_waiting_forcedly) {
      EXPECT_EQ(true, TestTxCtxGuard::is_trx_abort_sql_ret(commit_res_ret));
    } else {
      EXPECT_EQ(OB_SUCCESS, commit_res_ret);
    }

    if (is_waiting_forcedly) {

      ret = wait_event_finish("END_RANDOM_SWITCH_FOLLOWER_FORCEDLY", event_content, 100L, 50L);
      if (OB_SUCCESS == ret) {
        ret = OB_TIMEOUT;
        is_waiting_forcedly = false;
      }
    } else {
      ret = wait_event_finish("END_RANDOM_SWITCH_FOLLOWER_GRACEFULLY", event_content, 100L, 50L);
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

} // namespace unittest
} // namespace oceanbase
