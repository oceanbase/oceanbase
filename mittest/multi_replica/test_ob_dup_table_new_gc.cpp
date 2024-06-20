/*
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
#include "storage/tx/ob_dup_table_util.h"

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

#define CUR_TEST_CASE_NAME ObDupTableNewGCTest

DEFINE_MULTI_ZONE_TEST_CASE_CLASS

MULTI_REPLICA_TEST_MAIN_FUNCTION(test_dup_table_new_gc_);

static const int64_t MAX_DUP_TABLE_COUNT = 10;
static bool STOP_PREPARE_SERIALIZE_DUP_TABLET = false;

namespace oceanbase
{

namespace transaction
{

OB_NOINLINE int ObLSDupTabletsMgr::process_prepare_ser_err_test_()
{
  int ret = OB_SUCCESS;

  if (STOP_PREPARE_SERIALIZE_DUP_TABLET) {
    ret = OB_EAGAIN;
  }

  if (OB_FAIL(ret)) {
    DUP_TABLE_LOG(INFO, "errsim prepare serialize err test in mittest", K(ret), K(ls_id_));
  }
  return ret;
}
} // namespace transaction

namespace unittest
{

struct DupTableBasicArg
{
  uint64_t tenant_id_;
  int64_t ls_id_num_;
  ObSEArray<int64_t, 10> table_id_;
  int64_t tablet_count_;
  ObSEArray<int64_t, 100> tablet_id_array_;

  DupTableBasicArg() { reset(); }
  void reset()
  {
    tenant_id_ = 0;
    ls_id_num_ = 0;
    table_id_.reset();
    tablet_count_ = 0;
    tablet_id_array_.reset();
  }

  TO_STRING_KV(K(tenant_id_), K(ls_id_num_), K(table_id_), K(tablet_count_), K(tablet_id_array_));

  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER(DupTableBasicArg,
                    tenant_id_,
                    ls_id_num_,
                    table_id_,
                    tablet_count_,
                    tablet_id_array_);

static DupTableBasicArg dup_basic_arg_;

static const int64_t CUR_GC_INTERVAL = 2 * 1000 * 1000;

void reduce_dup_table_gc_interval(int64_t tenant_id, int64_t cur_gc_interval)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  MTL(transaction::ObTransService *)->dup_tablet_scan_task_.scan_task_execute_interval_ =
      cur_gc_interval;
  MTL(transaction::ObTransService *)->dup_tablet_scan_task_.max_execute_interval_ = cur_gc_interval;
  oceanbase::transaction::ObLSDupTabletsMgr::MAX_READABLE_SET_SER_INTERVAL = CUR_GC_INTERVAL;
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_dup_table)
{

  int ret = OB_SUCCESS;

  CREATE_TEST_TENANT(test_tenant_id);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] create test tenant success", K(test_tenant_id));

  reduce_dup_table_gc_interval(test_tenant_id, CUR_GC_INTERVAL);

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);
  ACQUIRE_CONN_FROM_SQL_PROXY(sys_conn, get_curr_simple_server().get_sql_proxy());

  std::string primary_zone_sql = "ALTER TENANT " + std::string(DEFAULT_TEST_TENANT_NAME)
                                 + " set primary_zone='zone1; zone3; zone2';";
  WRITE_SQL_BY_CONN(test_conn, primary_zone_sql.c_str());

  for (int i = 0; i < MAX_DUP_TABLE_COUNT; i++) {
    std::string tname = "test_t" + std::to_string(i);
    std::string create_table_sql =
        "CREATE TABLE " + tname
        + "( "
          "id_x int, "
          "id_y int, "
          "id_z int, "
          "PRIMARY KEY(id_x)"
          ") duplicate_scope='cluster' PARTITION BY hash(id_x) partitions 10;";
    std::string get_table_id_sql = "select table_id, duplicate_scope from "
                                   "oceanbase.__all_table where table_name = '"
                                   + tname + "' ";
    WRITE_SQL_BY_CONN(test_conn, create_table_sql.c_str());
    READ_SQL_BY_CONN(test_conn, table_info_result, get_table_id_sql.c_str());

    ASSERT_EQ(OB_SUCCESS, table_info_result->next());
    int64_t table_id = 0;
    int64_t dup_scope = 0;
    ASSERT_EQ(OB_SUCCESS, table_info_result->get_int("table_id", table_id));
    ASSERT_EQ(OB_SUCCESS, table_info_result->get_int("duplicate_scope", dup_scope));
    ASSERT_EQ(true, table_id > 0);
    ASSERT_EQ(true, dup_scope != 0);
    ASSERT_EQ(OB_SUCCESS, dup_basic_arg_.table_id_.push_back(table_id));

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

    if (dup_basic_arg_.ls_id_num_ <= 0) {
      dup_basic_arg_.ls_id_num_ = ls_id_num;
    } else {
      ASSERT_EQ(dup_basic_arg_.ls_id_num_, ls_id_num);
    }
    dup_basic_arg_.tablet_count_ += tablet_count;

    std::string tablet_id_sql =
        "select tablet_id from oceanbase.__all_tablet_to_ls where table_id = "
        + std::to_string(table_id) + " and ls_id = " + std::to_string(ls_id_num);
    READ_SQL_BY_CONN(test_conn, tablet_id_reult, tablet_id_sql.c_str());
    while (OB_SUCC(tablet_id_reult->next())) {
      int64_t id = 0;
      ASSERT_EQ(OB_SUCCESS, tablet_id_reult->get_int("tablet_id", id));
      ASSERT_EQ(true, ObTabletID(id).is_valid());
      ASSERT_EQ(OB_SUCCESS, dup_basic_arg_.tablet_id_array_.push_back(id));
    }
  }

  dup_basic_arg_.tenant_id_ = test_tenant_id;

  std::string tmp_str;
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<DupTableBasicArg>::serialize_arg(dup_basic_arg_, tmp_str));
  ASSERT_EQ(OB_SUCCESS, finish_event("CREATE_DUP_TABLE", tmp_str));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), leader_switch_without_gc)
{
  int ret = OB_SUCCESS;

  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("CREATE_DUP_TABLE", tmp_event_val, 30 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<DupTableBasicArg>::deserialize_arg(dup_basic_arg_, tmp_event_val));

  reduce_dup_table_gc_interval(dup_basic_arg_.tenant_id_, CUR_GC_INTERVAL);

  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  common::ObMySQLProxy &sys_sql_proxy = get_curr_simple_server().get_sql_proxy();
  ACQUIRE_CONN_FROM_SQL_PROXY(sys_conn, sys_sql_proxy);

  GET_LS(dup_basic_arg_.tenant_id_, dup_basic_arg_.ls_id_num_, ls_handle);

  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.is_inited()
          && ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_count()
                 == dup_basic_arg_.tablet_count_,
      30 * 1000 * 1000, 1 * 100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);
  RETRY_UNTIL_TIMEOUT(!ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                      100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);

  usleep(ObDupTabletScanTask::DUP_TABLET_SCAN_INTERVAL + 4 * CUR_GC_INTERVAL);

  std::string ls_id_str = std::to_string(dup_basic_arg_.ls_id_num_);
  std::string zone2_ip = local_ip_ + ":" + std::to_string(rpc_ports_[1]);

  std::string switch_leader_sql = "alter system switch replica leader ls=" + ls_id_str + " server='"
                                  + zone2_ip + "' tenant='tt1';";

  WRITE_SQL_BY_CONN(sys_conn, switch_leader_sql.c_str());
  RETRY_OP_UNTIL_TIMEOUT(
      ASSERT_EQ(
          ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_count(),
          dup_basic_arg_.tablet_count_),
      ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000, 50 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);
  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_count()
          < dup_basic_arg_.tablet_count_,
      10 * 1000 * 1000, 50 * 1000);
  EXPECT_EQ(ret, OB_TIMEOUT);
  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_count()
          == dup_basic_arg_.tablet_count_,
      20 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);

  std::string zone1_ip = local_ip_ + ":" + std::to_string(rpc_ports_[0]);

  std::string switch_leader_to_zone1_sql = "alter system switch replica leader ls=" + ls_id_str
                                           + " server='" + zone1_ip + "' tenant='tt1';";
  WRITE_SQL_BY_CONN(sys_conn, switch_leader_to_zone1_sql.c_str());

  RETRY_UNTIL_TIMEOUT(!ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                      100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_EQ(OB_SUCCESS, finish_event("SWITCH_LEADER_TO_ZONE2_WITHOUT_GC", ""));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), leader_revoke_in_gc)
{
  int ret = OB_SUCCESS;
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("SWITCH_LEADER_TO_ZONE2_WITHOUT_GC", tmp_event_val, 30 * 60 * 1000));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  GET_LS(dup_basic_arg_.tenant_id_, dup_basic_arg_.ls_id_num_, ls_handle);
  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                      100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);

  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_count()
          == dup_basic_arg_.tablet_count_,
      20 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] print dup tablet stat info 1", K(ret),
            KPC(ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_));

  {
    std::string tname = "test_t" + std::to_string(1);
    std::string delete_table_sql = "DROP TABLE " + tname;
    WRITE_SQL_BY_CONN(test_conn, delete_table_sql.c_str());

    std::string get_table_id_sql = "select table_id, duplicate_scope from "
                                   "oceanbase.__all_table where table_name = '"
                                   + tname + "' ";
    READ_SQL_BY_CONN(test_conn, table_info_result, get_table_id_sql.c_str());

    ASSERT_EQ(OB_ITER_END, table_info_result->next());
  }

  // gc t1 successfully
  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_count()
          == dup_basic_arg_.tablet_count_ - 10,
      20 * 1000 * 1000, 100 * 1000);
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] print dup tablet stat info 1.1", K(ret),
            KPC(ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_));
  ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->print_tablet_diag_info_log(true);
  ASSERT_EQ(ret, OB_SUCCESS);
  RETRY_UNTIL_TIMEOUT(
      (ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->removing_old_set_->size() == 0),
      20 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);

  STOP_PREPARE_SERIALIZE_DUP_TABLET = true;

  {
    std::string tname = "test_t" + std::to_string(2);
    std::string delete_table_sql = "DROP TABLE " + tname;
    WRITE_SQL_BY_CONN(test_conn, delete_table_sql.c_str());

    std::string get_table_id_sql = "select table_id, duplicate_scope from "
                                   "oceanbase.__all_table where table_name = '"
                                   + tname + "' ";
    READ_SQL_BY_CONN(test_conn, table_info_result, get_table_id_sql.c_str());

    ASSERT_EQ(OB_ITER_END, table_info_result->next());
  }

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] print dup tablet stat info 2", K(ret),
            KPC(ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_));

  RETRY_UNTIL_TIMEOUT(
      (ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->removing_old_set_->size() > 0),
      20 * 1000 * 1000, 100 * 1000);
  ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->print_tablet_diag_info_log(true);
  ASSERT_EQ(ret, OB_SUCCESS);

  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] print dup tablet stat info 3", K(ret),
            KPC(ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_));

  ASSERT_EQ(ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_count(),
            dup_basic_arg_.tablet_count_ - 10 /*readable 90 + removing 10*/);

  ASSERT_EQ(ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->removing_old_set_->size(),
            10);

  ASSERT_EQ(OB_SUCCESS, finish_event("DROP_TABLE_WITHOUT_LOG", ""));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), continue_gc_in_new_leader)
{
  int ret = OB_SUCCESS;
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("DROP_TABLE_WITHOUT_LOG", tmp_event_val, 30 * 60 * 1000));

  STOP_PREPARE_SERIALIZE_DUP_TABLET = true;

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);
  ACQUIRE_CONN_FROM_SQL_PROXY(sys_conn, get_curr_simple_server().get_sql_proxy());

  GET_LS(dup_basic_arg_.tenant_id_, dup_basic_arg_.ls_id_num_, ls_handle);

  std::string ls_id_str = std::to_string(dup_basic_arg_.ls_id_num_);
  std::string zone2_ip = local_ip_ + ":" + std::to_string(rpc_ports_[1]);

  std::string switch_leader_sql = "alter system switch replica leader ls=" + ls_id_str + " server='"
                                  + zone2_ip + "' tenant='tt1';";

  WRITE_SQL_BY_CONN(sys_conn, switch_leader_sql.c_str());

  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                      100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);

  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_count()
          < dup_basic_arg_.tablet_count_ - 10,
      20 * 1000 * 1000, 100 * 1000);
  TRANS_LOG(INFO, "[ObMultiReplicaTestBase] print dup tablet stat info 4", K(ret),
            KPC(ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_));
  ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->print_tablet_diag_info_log(true);
  ASSERT_EQ(ret, OB_TIMEOUT);

  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_count()
          == dup_basic_arg_.tablet_count_ - 10 /*readable 90 + removing 10*/,
      20 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_EQ(ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->removing_old_set_->size(),
            10);

  STOP_PREPARE_SERIALIZE_DUP_TABLET = false;

  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_count()
          == dup_basic_arg_.tablet_count_ - 10 * 2,
      20 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_EQ(OB_SUCCESS, finish_event("CONTINUE_GC_IN_ZONE2", ""));
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), check_gc_result_in_old_leader)
{

  int ret = OB_SUCCESS;
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("CONTINUE_GC_IN_ZONE2", tmp_event_val, 30 * 60 * 1000));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);
  ACQUIRE_CONN_FROM_SQL_PROXY(sys_conn, get_curr_simple_server().get_sql_proxy());

  GET_LS(dup_basic_arg_.tenant_id_, dup_basic_arg_.ls_id_num_, ls_handle);

  RETRY_UNTIL_TIMEOUT(!ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                      100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);

  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_count()
          == dup_basic_arg_.tablet_count_ - 10 * 2,
      20 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);

  std::string ls_id_str = std::to_string(dup_basic_arg_.ls_id_num_);
  std::string zone1_ip = local_ip_ + ":" + std::to_string(rpc_ports_[0]);

  std::string switch_leader_to_zone1_sql = "alter system switch replica leader ls=" + ls_id_str
                                           + " server='" + zone1_ip + "' tenant='tt1';";
  WRITE_SQL_BY_CONN(sys_conn, switch_leader_to_zone1_sql.c_str());

  RETRY_UNTIL_TIMEOUT(ls_handle.get_ls()->dup_table_ls_handler_.is_master(), 20 * 1000 * 1000,
                      100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);
  RETRY_UNTIL_TIMEOUT(
      ls_handle.get_ls()->dup_table_ls_handler_.tablets_mgr_ptr_->get_readable_tablet_count()
          == dup_basic_arg_.tablet_count_ - 10 * 2,
      20 * 1000 * 1000, 100 * 1000);
  ASSERT_EQ(ret, OB_SUCCESS);
}

// TEST_F(GET_ZONE_TEST_CLASS_NAME(1), random_error)
// {
//   ASSERT_EQ(ObTimeUtility::current_time() % 4 != 1, true);
// }
//
// TEST_F(GET_ZONE_TEST_CLASS_NAME(2), random_error)
// {
//   ASSERT_EQ(ObTimeUtility::current_time() % 4 != 2, true);
// }
//
// TEST_F(GET_ZONE_TEST_CLASS_NAME(3), random_error)
// {
//   ASSERT_EQ(ObTimeUtility::current_time() % 4 != 3, true);
// }
} // namespace unittest
} // namespace oceanbase
