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
#include <thread>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_fast_bootstrap.h"
#include "env/ob_multi_replica_util.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/tx/ob_dup_table_lease.h"

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

#define CUR_TEST_CASE_NAME ObDupTableTabletGC

DEFINE_MULTI_ZONE_TEST_CASE_CLASS

MULTI_REPLICA_TEST_MAIN_FUNCTION(test_dup_table_tablet_gc_);

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

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_and_drop_dup_table)
{
  int ret = OB_SUCCESS;
  CREATE_TEST_TENANT(test_tenant_id);
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] create test tenant success", K(test_tenant_id));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);
  ACQUIRE_CONN_FROM_SQL_PROXY(sys_conn, get_curr_simple_server().get_sql_proxy());

  std::vector<std::thread> ths;
  std::set<int> create_table_set;
  std::mutex w_lock;
  uint64_t loop_cnt = 30;
  uint64_t worker_cnt = 3;
  uint64_t create_times = 0;
  uint64_t drop_times = 0;

  auto do_worker = [&] (uint64_t uid) {
    ACQUIRE_CONN_FROM_SQL_PROXY(thread_test_conn, test_tenant_sql_proxy);
    uint64_t rand = 0;
    uint64_t name_rand = 0;

    std::srand((unsigned)std::time(NULL));

    for (int i = 1; i <= loop_cnt; i++) {
      rand = std::rand() % 10 + 1;
      name_rand = std::rand() % 13331 + uid;

      if (rand <= 7) {
        w_lock.lock();
        std::pair<std::set<int>::iterator, bool> insert_res = create_table_set.insert(name_rand);
        if (insert_res.second) {
          create_times++;
        }
        w_lock.unlock();

        if (insert_res.second) {
          std::string create_table_sql = "CREATE TABLE test_tt" + std::to_string(name_rand)  + " ( "
                                        + "id_x int, "
                                        + "id_y int, "
                                        + "id_z int, "
                                        + "PRIMARY KEY(id_x)"
                                        + ") duplicate_scope='cluster' PARTITION BY hash(id_x) partitions 10;";
          WRITE_SQL_BY_CONN(thread_test_conn, create_table_sql.c_str());
        }
      } else {
        w_lock.lock();
        drop_times++;
        auto table_num = create_table_set.begin();
        create_table_set.erase(table_num);
        w_lock.unlock();

        std::string drop_table_sql = "DROP TABLE IF EXISTS test_tt" + std::to_string(*table_num) + ";";
        WRITE_SQL_BY_CONN(thread_test_conn, drop_table_sql.c_str());
      }
    }
    LOG_STDOUT("Thread %lu finish Loop Work\n", uid);
  };

  for (int i = 0; i < worker_cnt; i++) {
    std::thread th(do_worker, i);
    ths.push_back(std::move(th));
  }

  for (auto &th : ths) {
    th.join();
  }

  LOG_STDOUT("Finish create and drop table, create times:%lu, drop times:%lu\n", create_times, drop_times);
  sleep(10);

  uint64_t remain_tablets = create_table_set.size() * 10;
  READ_SQL_BY_CONN(test_conn, ls_info,
                   "select * from oceanbase.DBA_OB_LS where flag like \"%DUPLICATE%\";");

  ASSERT_EQ(OB_SUCCESS, ls_info->next());
  int64_t ls_id = 0;
  ASSERT_EQ(OB_SUCCESS, ls_info->get_int("LS_ID", ls_id));
  LOG_STDOUT("Check dup ls:%lu\n", ls_id);

  std::string check_tablet_to_ls_sql = "select count(*) from oceanbase.__all_tablet_to_ls where ls_id = "
                                        + std::to_string(ls_id);
  READ_SQL_BY_CONN(test_conn, tablet_info_result, check_tablet_to_ls_sql.c_str());
  ASSERT_EQ(OB_SUCCESS, tablet_info_result->next());
  int64_t dup_tablet_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_info_result->get_int("count(*)", dup_tablet_cnt));
  LOG_STDOUT("Check tablet, remain_tablets:%lu, remain_schema_tablets:%lu\n", remain_tablets, dup_tablet_cnt);
  // ASSERT_EQ(true, dup_tablet_cnt == remain_tablets);

  std::string primary_zone_sql = "ALTER TENANT " + std::string(DEFAULT_TEST_TENANT_NAME)
                                 + " set primary_zone='zone1, zone3; zone2';";
  WRITE_SQL_BY_CONN(test_conn, primary_zone_sql.c_str());
  sleep(200);

  READ_SQL_BY_CONN(test_conn, remain_tablet_count_result,
                   "select count(*) from "
                   "oceanbase.__all_virtual_dup_ls_tablets where ls_state = 'LEADER'");
  uint64_t tablet_count = 0;
  ASSERT_EQ(OB_SUCCESS, remain_tablet_count_result->next());
  ASSERT_EQ(OB_SUCCESS, remain_tablet_count_result->get_uint("count(*)", tablet_count));
  LOG_STDOUT("Check tablet, remain_schema:%lu, remain_dup_table_mgr:%lu\n", dup_tablet_cnt, tablet_count);
  ASSERT_EQ(true, tablet_count == dup_tablet_cnt);

  for (auto create_table_num : create_table_set) {
    std::string drop_table_sql = "DROP TABLE IF EXISTS test_tt" + std::to_string(create_table_num) + ";";
    WRITE_SQL_BY_CONN(test_conn, drop_table_sql.c_str());
  }

  LOG_STDOUT("Drop all dup table\n");
  sleep(300);

  READ_SQL_BY_CONN(test_conn, after_drop_all_tablet_info_result, check_tablet_to_ls_sql.c_str());
  ASSERT_EQ(OB_SUCCESS, after_drop_all_tablet_info_result->next());
  ASSERT_EQ(OB_SUCCESS, after_drop_all_tablet_info_result->get_int("count(*)", dup_tablet_cnt));
  LOG_STDOUT("Check tablet, remain_tablets:%lu\n", dup_tablet_cnt);
  ASSERT_EQ(true, dup_tablet_cnt == 0);

  READ_SQL_BY_CONN(test_conn, after_drop_all_tablet_result,
                   "select count(*) as res from "
                   "oceanbase.__all_virtual_dup_ls_tablets where ls_state = 'LEADER'");
  ASSERT_EQ(OB_SUCCESS, after_drop_all_tablet_result->next());
  ASSERT_EQ(OB_SUCCESS, after_drop_all_tablet_result->get_uint("res", tablet_count));
  LOG_STDOUT("Check tablet, remain_tablets:%lu, real_tablets:%lu\n", dup_tablet_cnt, tablet_count);
  ASSERT_EQ(true, tablet_count == dup_tablet_cnt);

  std::string tmp_str;
  ASSERT_EQ(OB_SUCCESS,
            EventArgSerTool<DupTableBasicArg>::serialize_arg(static_basic_arg_, tmp_str));
  ASSERT_EQ(OB_SUCCESS, finish_event("CREATE_DUP_TABLE", tmp_str));
}

} // namespace unittest
} // namespace oceanbase
