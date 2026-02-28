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
#include <functional>
#include <vector>
#include <mutex>
#include <condition_variable> // std::condition_variable

#define USING_LOG_PREFIX SERVER
#define protected public
#define private public

#include "env/ob_fast_bootstrap.h"
#include "env/ob_multi_replica_util.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "storage/lock_wait_mgr/ob_lock_wait_mgr.h"

using namespace oceanbase::transaction;
using namespace oceanbase::memtable;
using namespace oceanbase::lockwaitmgr;

#define CUR_TEST_CASE_NAME ObLockWaitMgrTest

DEFINE_MULTI_ZONE_TEST_CASE_CLASS

MULTI_REPLICA_TEST_MAIN_FUNCTION(test_ob_lock_wait_mgr_);

namespace oceanbase
{
namespace unittest
{
#define GET_TRX_ID(conn, tx_id)                                                         \
  {                                                                                     \
    ASSERT_EQ(true, conn != nullptr);                                                   \
    std::string sql_str = "select ob_transaction_id() as tx_id;";                       \
    READ_SQL_BY_CONN(conn, process_result, sql_str.c_str());                            \
    ASSERT_EQ(OB_SUCCESS, process_result->next());                                      \
    ASSERT_EQ(OB_SUCCESS, process_result->get_int("tx_id", tx_id));                     \
  }

struct ObLockWaitMgrArg
{
  ObSEArray<int64_t, 1024> tx_id_array_;
  uint64_t tenant_id_;

  TO_STRING_KV(K(tx_id_array_), K(tenant_id_));

  OB_UNIS_VERSION(1);
};

OB_SERIALIZE_MEMBER(ObLockWaitMgrArg,tx_id_array_, tenant_id_);

typedef rpc::ObLockWaitNode Node;
static uint64_t g_tenant_id;
static common::ObMySQLProxy *static_server1_proxy = nullptr;
static common::ObMySQLProxy *static_server2_proxy = nullptr;
// rowkey1 rowkey2 hash相同
static const int rowkey1 = 1;
static const int rowkey2 = 2;

static std::string exec_path = get_current_dir_name();

static std::string result_file_path1 = exec_path  + "/" + "test_ob_lock_wait_mgr__test_data/exec_result";
static std::string result_file_path2 = exec_path + "/" "test_ob_lock_wait_mgr__test_data/row2_result";
static const std::string RESULT_NAME = "tx-order";

static  int local_execution_cnt = 600;
static  int remote_execution_cnt = 600; // row1 row2 , normal ac=0, normal ac=1, das ac=0, px ac=0

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), create_table)
{
  int ret = OB_SUCCESS;
  SERVER_LOG(INFO, "create_tenant start");
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(g_tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  SERVER_LOG(INFO, "create_tenant end", K(g_tenant_id));

  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] create test tenant success", K(g_tenant_id));

  common::ObMySQLProxy &test_tenant_sql_proxy = get_curr_simple_server().get_sql_proxy2();

  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, test_tenant_sql_proxy);

  WRITE_SQL_BY_CONN(test_conn, "create table lock_cflict_test(id int primary key, c1 int);");

  std::string tmp_str;
  ObLockWaitMgrArg arg;
  arg.tenant_id_ = g_tenant_id;
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<ObLockWaitMgrArg>::serialize_arg(arg, tmp_str));
  finish_event("CREATE_TABLE", tmp_str);
  // finish_event("CREATE_TENANT", tmp_str);
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), insert_data)
{
  int ret = OB_SUCCESS;
  static_server1_proxy = &get_curr_simple_server().get_sql_proxy2();
  ACQUIRE_CONN_FROM_SQL_PROXY(test_conn, (*static_server1_proxy));

  WRITE_SQL_BY_CONN(test_conn, "begin;");
  WRITE_SQL_BY_CONN(test_conn, "insert into lock_cflict_test values(1,1),(2,2),(3,3),(4,4),(5,5),(6,6);");
  WRITE_SQL_BY_CONN(test_conn, "commit;");
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), stop_check_timeout)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(g_tenant_id)
  {
    ObLockWaitMgr *lwm = MTL(ObLockWaitMgr *);
    ASSERT_NE(nullptr, lwm);
    lwm->stop_check_timeout();
  }
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), stop_check_timeout)
{
  int ret = OB_SUCCESS;
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("CREATE_TABLE", tmp_event_val, 45 * 1000 * 1000));

  ObLockWaitMgrArg arg;
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<ObLockWaitMgrArg>::deserialize_arg(arg, tmp_event_val));
  g_tenant_id = arg.tenant_id_;

  MTL_SWITCH(g_tenant_id)
  {
    ObLockWaitMgr *lwm = MTL(ObLockWaitMgr *);
    ASSERT_NE(nullptr, lwm);
    lwm->stop_check_timeout();
  }
  std::string tmp_str;
  finish_event("STOP_CHECK_TIMEOUT_ZONE2", tmp_str);
}

int read_result_json_document(const std::string file_path, rapidjson::Document &json_doc)
{
  int ret = OB_SUCCESS;
  FILE *fp = fopen(file_path.c_str(), "rb");
  if (fp == NULL) {
    if (json_doc.IsObject()) {
      fprintf(stdout, "Fail to open file! file_path = %s\n", file_path.c_str());
    }
    ret = OB_ENTRY_NOT_EXIST;
    return ret;
  }

  char read_buffer[4 * 1024];
  rapidjson::FileReadStream rs(fp, read_buffer, sizeof(read_buffer));

  json_doc.ParseStream(rs);

  if (json_doc.HasParseError()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "[ObMultiReplicaTestBase] Parse EVENT JSON ERROR", K(ret),
               K(json_doc.GetParseError()));
    fprintf(stdout, "Parse Event Json Error\n");
  }
  fclose(fp);
  return OB_SUCCESS;
}

int get_result(const std::string &result_name,
               const std::string &file_path,
               std::string &result_content,
               int64_t wait_timeout_ms,
               int64_t retry_interval_ms = 1 * 1000)
{
  int ret = OB_SUCCESS;

  bool find_result = false;
  int64_t start_time = ObTimeUtility::fast_current_time();

  while (OB_SUCC(ret) && !find_result) {

    rapidjson::Document json_doc;

    if (OB_FAIL(read_result_json_document(file_path, json_doc))) {
      SERVER_LOG(WARN, "read existed json document failed", K(ret));
      if (ret == OB_ENTRY_NOT_EXIST) {
        ret = OB_SUCCESS;
      }
    } else {
      rapidjson::Value::ConstMemberIterator iter = json_doc.FindMember(result_name.c_str());
      if (iter == json_doc.MemberEnd()) {
        SERVER_LOG(WARN, "[ObMultiReplicaTestBase] [WAIT EVENT] not find target event", K(ret),
                   K(result_name.c_str()));
        ret = OB_SUCCESS;
      } else {
        find_result = true;
        result_content = std::string(iter->value.GetString(), iter->value.GetStringLength());
        // fprintf(stdout, "[WAIT EVENT] find target event : EVENT_KEY = %s; EVENT_VAL = %s\n",
        //         result_name.c_str(), iter->value.GetString());
        // SERVER_LOG(INFO, "[ObMultiReplicaTestBase] [WAIT EVENT] find target event",
        //            K(result_name.c_str()), K(iter->value.GetString()));
      }
    }

    if (!find_result) {
      if (wait_timeout_ms != INT64_MAX
          && ObTimeUtility::fast_current_time() - start_time > wait_timeout_ms * 1000) {
        ret = OB_TIMEOUT;
        break;
      } else {
        usleep(retry_interval_ms * 1000);
      }
    } else {
      break;
    }
  }

  return ret;
}

int write_result(const std::string &result_name,
                 const std::string &file_path,
                 const std::string &result_content)
{
  int ret = OB_SUCCESS;
  // std::cout<< "file path:" << file_path << std::endl;
  rapidjson::Document json_doc;
  json_doc.Parse("{}");

  if (OB_SUCC(ret)) {
    FILE *fp = fopen(file_path.c_str(), "w");
    if (fp == NULL) {
      if (json_doc.IsObject()) {
        fprintf(stdout, "Fail to open file! file_path = %s\n", file_path.c_str());
      }
      ret = OB_ENTRY_NOT_EXIST;
      return ret;
    }
    char write_buffer[4 * 1024];
    rapidjson::FileWriteStream file_w_stream(fp, write_buffer, sizeof(write_buffer));
    rapidjson::PrettyWriter<rapidjson::FileWriteStream> prettywriter(file_w_stream);
    json_doc.AddMember(rapidjson::StringRef(result_name.c_str(), result_name.size()),
                       rapidjson::StringRef(result_content.c_str(), result_content.size()),
                       json_doc.GetAllocator());
    json_doc.Accept(prettywriter);
    fclose(fp);
  }

  // fprintf(stdout, "[WAIT EVENT] write target event : EVENT_KEY = %s; EVENT_VAL = %s\n",
  //         result_name.c_str(), result_content.c_str());
  SERVER_LOG(INFO, "[ObMultiReplicaTestBase] [WAIT EVENT] write target event",
             K(result_name.c_str()), K(result_content.c_str()));
  return ret;
}

void write_txid_to_result(int64_t txid, const std::string &file_path, int &finish_num)
{
  std::string tmp_result_val;
  ObLockWaitMgrArg arg;
  ASSERT_EQ(OB_SUCCESS, get_result(RESULT_NAME, file_path, tmp_result_val, 30 * 60 * 1000));
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<ObLockWaitMgrArg>::deserialize_arg(arg, tmp_result_val));
  arg.tx_id_array_.push_back(txid);
  std::string tmp_str;
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<ObLockWaitMgrArg>::serialize_arg(arg, tmp_str));
  write_result(RESULT_NAME, file_path, tmp_str);
  ATOMIC_STORE(&finish_num, arg.tx_id_array_.count());
}

TEST_F(GET_ZONE_TEST_CLASS_NAME(1), concurrent_lock_cflict_test)
{
  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS,
            wait_event_finish("STOP_CHECK_TIMEOUT_ZONE2", tmp_event_val, 30 * 1000 * 1000));
  ASSERT_NE(nullptr, static_server1_proxy);
  int ret = OB_SUCCESS;
  int test_time = 10; // 10s
  std::vector<std::thread> ths;

  std::condition_variable cond1_row1;
  std::condition_variable cond2_row1;
  std::mutex mtx1_row1;
  std::mutex mtx2_row1;

  std::condition_variable cond1_row2;
  std::condition_variable cond2_row2;
  std::mutex mtx1_row2;
  std::mutex mtx2_row2;

  // std::vector<int64_t> trx_expected_commit_order_row1;
  // std::vector<int64_t> trx_expected_commit_order_row2;
  int64_t first_hold_tx_id_row1 = 0;
  int64_t first_hold_tx_id_row2 = 0;
  int finish_num1 = 0;
  int finish_num2 = 0;
  bool row1_end = false;
  bool row2_end = false;
  bool row1_ready = false;
  bool row2_ready = false;

  std::string tmp_str1;
  ObLockWaitMgrArg arg_first;
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<ObLockWaitMgrArg>::serialize_arg(arg_first, tmp_str1));
  write_result(RESULT_NAME, result_file_path1, tmp_str1);
  write_result(RESULT_NAME, result_file_path2, tmp_str1);

  // 本地加锁请求
  auto local_worker = [&] (int update_rowkey, bool first_worker) {
    std::condition_variable *cond1 = &cond1_row1;
    std::condition_variable *cond2 = &cond2_row1;
    std::mutex *mtx2 = &mtx2_row1;
    std::string *file_path = &result_file_path1;
    int finish_num = 0;
    int *local_finish_num = &finish_num1;
    if (update_rowkey == rowkey2) {
      file_path = &result_file_path2;
      cond1 = &cond1_row2;
      cond2 = &cond2_row2;
      mtx2 = &mtx2_row2;
      local_finish_num = &finish_num2;
    }
    sqlclient::ObISQLConnection *connection = nullptr;
    ASSERT_NE(nullptr, static_server1_proxy);
    ASSERT_EQ(OB_SUCCESS, static_server1_proxy->acquire(connection));
    ASSERT_NE(nullptr, connection);
    WRITE_SQL_BY_CONN(connection, "set ob_trx_timeout = 3000000000;");
    WRITE_SQL_BY_CONN(connection, "set ob_trx_idle_timeout = 3000000000;");
    WRITE_SQL_BY_CONN(connection, "set ob_query_timeout = 3000000000;");
    WRITE_SQL_BY_CONN(connection, "set autocommit=0;");
    WRITE_SQL_BY_CONN(connection, "begin;");
    std::string update_sql = "update lock_cflict_test set c1=c1+1 where id=" + std::to_string(update_rowkey) + ";";
    WRITE_SQL_BY_CONN(connection, update_sql.c_str());

    int64_t tx_id = 0;

    GET_TRX_ID(connection, tx_id);
    // write_txid_to_result(tx_id, *file_path, finish_num);
    ASSERT_EQ(true, tx_id > 0);
    if (first_worker) {
      if (update_rowkey == rowkey1) {
        ATOMIC_SET(&row1_end, true);
      } else {
        ATOMIC_SET(&row2_end, true);
      }
      if (update_rowkey == rowkey1) {
        first_hold_tx_id_row1 = tx_id;
      } else {
        first_hold_tx_id_row2 = tx_id;
      }
      std::unique_lock<std::mutex> lck(*mtx2);
      cond2->wait(lck);
    }
    // std::cout << "txid begin start" << tx_id << std::endl;
    WRITE_SQL_BY_CONN(connection, "commit;");
    // std::cout << "txid end:" << tx_id << std::endl;
    ATOMIC_INC(local_finish_num);
  };

  FixedHash2<Node> *stored_hash = nullptr;
  FixedHash2<Node> *fixed_hash = nullptr;
  ObLockWaitMgr *lwm = nullptr;
  const int LOCK_BUCKET_COUNT = 2;
  char hash_buf[sizeof(SpHashNode) * LOCK_BUCKET_COUNT];
  memset(hash_buf, 0, sizeof(hash_buf));
  // std::cout << "exchange fixed hash begin:"  << std::endl;
  MTL_SWITCH(g_tenant_id)
  {
    // 将hash槽替换为slot数为2, 为1时FixedHash2实现有bug
    lwm = MTL(ObLockWaitMgr *);
    fixed_hash = new FixedHash2<Node>(hash_buf, sizeof(hash_buf));
    ASSERT_NE(nullptr, lwm);
    stored_hash = lwm->get_fixed_hash();
    lwm->set_fixed_hash(fixed_hash);
    std::cout << "exchange fixed hash end:" << fixed_hash->limit_  << std::endl;
  }


  std::thread first_lock_holder1(local_worker, 1, true);
  std::thread first_lock_holder2(local_worker, 2, true);
  while (!ATOMIC_LOAD(&row1_end) || !ATOMIC_LOAD(&row2_end))
  {
    usleep(10 * 1000);
  }

  std::string tmp_str;
  ObLockWaitMgrArg tmp_arg;
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<ObLockWaitMgrArg>::serialize_arg(tmp_arg, tmp_str));
  finish_event("FIRST_HOLDER_LOCK", tmp_str);

  for (int i = 0; i < local_execution_cnt/2; i++) {
    std::thread th1(local_worker, 1, false);
    std::thread th2(local_worker, 2, false);
    ths.push_back(std::move(th1));
    ths.push_back(std::move(th2));
  }

  // trx_expected_commit_order_row1.push_back(first_hold_tx_id_row1);
  // trx_expected_commit_order_row2.push_back(first_hold_tx_id_row2);
  sleep(10); // 等所有等待者入队

  // {
  //   Node *iter = nullptr;
  //   Node cur_node;
  //   CriticalGuard(lwm->get_qs());
  //   int total_node = 0;
  //   while(OB_NOT_NULL(iter = lwm->next(iter, &cur_node))) {
  //     total_node++;
  //     if (iter->tx_id_ == 0) {
  //       ob_abort();
  //     }
  //   }
  //   ASSERT_EQ(total_node, local_execution_cnt + remote_execution_cnt);
  // }

  cond2_row1.notify_all();
  cond2_row2.notify_all();

  first_lock_holder1.join();
  first_lock_holder2.join();

  for (int i = 0; i < test_time; i++) {
    std::cout << "row1 finish num:" << ATOMIC_LOAD(&finish_num1) << std::endl;
    std::cout << "row2 finish num:" << ATOMIC_LOAD(&finish_num2) << std::endl;
    sleep(5);
  }

  int cnt = 0;
  for (auto &th : ths) {
    cnt++;
    if (cnt % 5 == 0) {
      std::cout << "local finish num:" << cnt << std::endl;
    }
    th.join();
  }

  usleep(30 * 1000 * 1000); // 等所有人执行完毕

  int last_finish_num = 0;
  int now_finish_num = 0;
  int i = 0;

  // std::string tmp_result_val1;
  // ObLockWaitMgrArg arg1;
  // ASSERT_EQ(OB_SUCCESS, get_result(RESULT_NAME, result_file_path1, tmp_result_val1, 30 * 60 * 1000));
  // ASSERT_EQ(OB_SUCCESS, EventArgSerTool<ObLockWaitMgrArg>::deserialize_arg(arg1, tmp_result_val1));

  // std::string tmp_result_val2;
  // ObLockWaitMgrArg arg2;
  // ASSERT_EQ(OB_SUCCESS, get_result(RESULT_NAME, result_file_path2, tmp_result_val2, 30 * 60 * 1000));
  // ASSERT_EQ(OB_SUCCESS, EventArgSerTool<ObLockWaitMgrArg>::deserialize_arg(arg2, tmp_result_val2));

  // ASSERT_EQ(arg1.tx_id_array_.count(), trx_expected_commit_order_row1.size());
  // ASSERT_EQ(arg2.tx_id_array_.count(), trx_expected_commit_order_row2.size());
  // ASSERT_EQ(arg1.tx_id_array_.count() + arg2.tx_id_array_.count(), local_execution_cnt + remote_execution_cnt + 2);
  // ASSERT_EQ(arg1.tx_id_array_.count() + arg2.tx_id_array_.count(), local_execution_cnt + 2);
  ASSERT_EQ(ATOMIC_LOAD(&finish_num1) + ATOMIC_LOAD(&finish_num2), local_execution_cnt + 2);

  ASSERT_EQ(OB_SUCCESS, wait_event_finish("REMOTE_FINISH", tmp_event_val, 30 * 60 * 1000));

  lwm->set_fixed_hash(stored_hash);
  delete fixed_hash;
}

enum REMOTE_TYPE {
  NORMAL_EXECUTION = 0,
  DAS_EXECUTION = 1,
  PX_EXECUTION = 2
};

TEST_F(GET_ZONE_TEST_CLASS_NAME(2), concurrent_lock_cflict_test)
{
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  static_server2_proxy = &get_curr_simple_server().get_sql_proxy2();
  ASSERT_NE(nullptr, static_server2_proxy);
  sqlclient::ObISQLConnection *test_connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, static_server2_proxy->acquire(test_connection));
  ASSERT_NE(nullptr, test_connection);
  int ret = OB_SUCCESS;
  int test_time = 10; // 10s
  std::vector<std::thread> ths;
  int finish_num1 = 0;
  int finish_num2 = 0;

  // 远程加锁请求
  auto remote_worker = [&] (int update_rowkey, bool ac, REMOTE_TYPE type) {
    std::string *file_path = &result_file_path1;
    std::string update_sql_suffix = " or id=3 or id=4;";
    int finish_num = 0;
    if (update_rowkey == rowkey2) {
      file_path = &result_file_path2;
      update_sql_suffix = " or id=5 or id=6;";
    }

    sqlclient::ObISQLConnection *connection = nullptr;
    ASSERT_EQ(OB_SUCCESS, static_server2_proxy->acquire(connection));
    ASSERT_NE(nullptr, connection);
    WRITE_SQL_BY_CONN(connection, "set ob_trx_timeout = 3000000000;");
    WRITE_SQL_BY_CONN(connection, "set ob_trx_idle_timeout = 3000000000;");
    WRITE_SQL_BY_CONN(connection, "set ob_query_timeout = 3000000000;");
    std::string set_ac_sql = "set autocommit=";
    set_ac_sql = set_ac_sql + (ac ? "1;" : "0;");
    WRITE_SQL_BY_CONN(connection, set_ac_sql.c_str());

    std::string update_sql = "";

    if (type == REMOTE_TYPE::NORMAL_EXECUTION) {
      update_sql = "update lock_cflict_test set c1=c1+1 where id=" + std::to_string(update_rowkey) + ";";
    } else if (type == REMOTE_TYPE::DAS_EXECUTION) {
      update_sql = "update /*+use_das(t)*/ lock_cflict_test set c1=c1+1 where id=" + std::to_string(update_rowkey) + update_sql_suffix;
    } else if (type == REMOTE_TYPE::PX_EXECUTION) {
      update_sql = "update /*+parallel(2)*/ lock_cflict_test set c1=c1+1 where id=" + std::to_string(update_rowkey) + update_sql_suffix;
    }
    WRITE_SQL_BY_CONN(connection, update_sql.c_str());

    int64_t tx_id = 0;
    GET_TRX_ID(connection, tx_id);
      // write_txid_to_result(tx_id, *file_path, finish_num);
    if (!ac) {
      WRITE_SQL_BY_CONN(connection, "commit;");
    }
    if (update_rowkey == rowkey1) {
      ATOMIC_INC(&finish_num1);
    } else {
      ATOMIC_INC(&finish_num2);
    }
  };

  FixedHash2<Node> *stored_hash = nullptr;
  FixedHash2<Node> *fixed_hash = nullptr;
  ObLockWaitMgr *lwm = nullptr;
  const int LOCK_BUCKET_COUNT = 2;
  char hash_buf[sizeof(SpHashNode) * LOCK_BUCKET_COUNT];
  memset(hash_buf, 0, sizeof(hash_buf));
  MTL_SWITCH(g_tenant_id)
  {
    // 将hash槽替换为slot数为2, 为1时FixedHash2实现有bug
    lwm = MTL(ObLockWaitMgr *);
    fixed_hash = new FixedHash2<Node>(hash_buf, sizeof(hash_buf));
    ASSERT_NE(nullptr, lwm);
    stored_hash = lwm->get_fixed_hash();
    lwm->set_fixed_hash(fixed_hash);
    std::cout << "exchange fixed hash end:" << fixed_hash->limit_  << std::endl;
  }


  std::string tmp_event_val;
  ASSERT_EQ(OB_SUCCESS, wait_event_finish("FIRST_HOLDER_LOCK", tmp_event_val,
                                                               30 * 60 * 1000));

  for (int i = 0; i < remote_execution_cnt/2/4; i++) {
    std::thread th1_1(remote_worker, rowkey1, false /*ac=0*/, REMOTE_TYPE::NORMAL_EXECUTION);
    std::thread th1_2(remote_worker, rowkey1, true /*ac=1*/, REMOTE_TYPE::NORMAL_EXECUTION);
    std::thread th1_3(remote_worker, rowkey1, false /*ac=0*/, REMOTE_TYPE::DAS_EXECUTION);
    std::thread th1_4(remote_worker, rowkey1, false /*ac=0*/, REMOTE_TYPE::PX_EXECUTION);

    std::thread th2_1(remote_worker, rowkey2, false /*ac=0*/, REMOTE_TYPE::NORMAL_EXECUTION);
    std::thread th2_2(remote_worker, rowkey2, true /*ac=1*/, REMOTE_TYPE::NORMAL_EXECUTION);
    std::thread th2_3(remote_worker, rowkey2, false /*ac=0*/, REMOTE_TYPE::DAS_EXECUTION);
    std::thread th2_4(remote_worker, rowkey2, false /*ac=0*/, REMOTE_TYPE::PX_EXECUTION);

    ths.push_back(std::move(th1_1));
    ths.push_back(std::move(th1_2));
    ths.push_back(std::move(th1_3));
    ths.push_back(std::move(th1_4));

    ths.push_back(std::move(th2_1));
    ths.push_back(std::move(th2_2));
    ths.push_back(std::move(th2_3));
    ths.push_back(std::move(th2_4));
  }

  sleep(5);

  for (int i = 0; i < test_time; i++) {
    std::cout << "remote sql row1 finish num:" << ATOMIC_LOAD(&finish_num1) << std::endl;
    std::cout << "remote sql row2 finish num:" << ATOMIC_LOAD(&finish_num2) << std::endl;
    sleep(5);
  }

  int cnt = 0;
  for (auto &th : ths) {
    cnt++;
    if (cnt % 5 == 0) {
      std::cout << "remote finish num:" << cnt << std::endl;
    }
    th.join();
  }

  std::cout << "remote sql row1 finish num:" << ATOMIC_LOAD(&finish_num1) << std::endl;
  std::cout << "remote sql row2 finish num:" << ATOMIC_LOAD(&finish_num2) << std::endl;

  std::string tmp_str;
  ObLockWaitMgrArg tmp_arg;
  ASSERT_EQ(OB_SUCCESS, EventArgSerTool<ObLockWaitMgrArg>::serialize_arg(tmp_arg, tmp_str));
  finish_event("REMOTE_FINISH", tmp_str);

  ASSERT_EQ(finish_num1 + finish_num2, remote_execution_cnt);
  MTL_SWITCH(g_tenant_id)
  {
    ObLockWaitMgr *lwm = MTL(ObLockWaitMgr *);
    ASSERT_NE(nullptr, lwm);
    ASSERT_EQ(0, lwm->get_not_free_remote_exec_side_node_cnt());
  }
  lwm->set_fixed_hash(stored_hash);
  delete fixed_hash;
}

}
}