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

#include "ob_multi_replica_test_base.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx_storage/ob_ls_service.h"

#ifndef OCEANBASE_MULTI_REPLICA_TEST_UTIL
#define OCEANBASE_MULTI_REPLICA_TEST_UTIL

// need define TEST_CASE_NAME

#define ZONE_TEST_CASE_CALSS_NAME_INNER(TEST_CASE_NAME, ZONE_ID) TEST_CASE_NAME##_ZONE##ZONE_ID
#define ZONE_TEST_CASE_CALSS_NAME(TEST_CASE_NAME, ZONE_ID) \
  ZONE_TEST_CASE_CALSS_NAME_INNER(TEST_CASE_NAME, ZONE_ID)

#define GET_ZONE_TEST_CLASS_NAME(ZONE_ID) ZONE_TEST_CASE_CALSS_NAME(CUR_TEST_CASE_NAME, ZONE_ID)

#define STR_NAME_INNER(x) #x
#define STR_NAME(x) STR_NAME_INNER(x)

#define GET_ZONE_TEST_CLASS_STR(ZONE_ID) \
  STR_NAME(ZONE_TEST_CASE_CALSS_NAME(CUR_TEST_CASE_NAME, ZONE_ID))

#define DEFINE_MULTI_ZONE_TEST_CASE_CLASS                                                  \
  namespace oceanbase                                                                      \
  {                                                                                        \
  namespace unittest                                                                       \
  {                                                                                        \
  std::string ObMultiReplicaTestBase::TEST_CASE_BASE_NAME = STR_NAME(CUR_TEST_CASE_NAME); \
  std::string ObMultiReplicaTestBase::ZONE_TEST_CASE_NAME[MAX_ZONE_COUNT] = {              \
      GET_ZONE_TEST_CLASS_STR(1), GET_ZONE_TEST_CLASS_STR(2), GET_ZONE_TEST_CLASS_STR(3)}; \
                                                                                           \
  class GET_ZONE_TEST_CLASS_NAME(1) : public ObMultiReplicaTestBase                        \
  {                                                                                        \
  public:                                                                                  \
    GET_ZONE_TEST_CLASS_NAME(1)() : ObMultiReplicaTestBase() {}                            \
  };                                                                                       \
                                                                                           \
  class GET_ZONE_TEST_CLASS_NAME(2) : public ObMultiReplicaTestBase                        \
  {                                                                                        \
  public:                                                                                  \
    GET_ZONE_TEST_CLASS_NAME(2)() : ObMultiReplicaTestBase() {}                            \
  };                                                                                       \
  class GET_ZONE_TEST_CLASS_NAME(3) : public ObMultiReplicaTestBase                        \
  {                                                                                        \
  public:                                                                                  \
    GET_ZONE_TEST_CLASS_NAME(3)() : ObMultiReplicaTestBase() {}                            \
  };                                                                                       \
  TEST_F(GET_ZONE_TEST_CLASS_NAME(1), start_observer) {}                                   \
  TEST_F(GET_ZONE_TEST_CLASS_NAME(2), start_observer) {}                                   \
  TEST_F(GET_ZONE_TEST_CLASS_NAME(3), start_observer) {}                                   \
  }                                                                                        \
  }


#define RESTART_ZONE_TEST_CASE_CALSS_NAME_INNER(TEST_CASE_NAME, ZONE_ID, RESTART_NO) \
  TEST_CASE_NAME##_RESTART_##RESTART_NO##_ZONE##ZONE_ID
#define RESTART_ZONE_TEST_CASE_CALSS_NAME(TEST_CASE_NAME, ZONE_ID, RESTART_NO) \
  RESTART_ZONE_TEST_CASE_CALSS_NAME_INNER(TEST_CASE_NAME, ZONE_ID, RESTART_NO)

#define GET_RESTART_ZONE_TEST_CLASS_NAME(ZONE_ID, RESTART_NO) \
  RESTART_ZONE_TEST_CASE_CALSS_NAME(CUR_TEST_CASE_NAME, ZONE_ID, RESTART_NO)

#define GET_RESTART_ZONE_TEST_CLASS_STR(ZONE_ID, REGISTER_NO) \
  STR_NAME(GET_RESTART_ZONE_TEST_CLASS_NAME(ZONE_ID, REGISTER_NO))

#define APPEND_RESTART_TEST_CASE_CLASS(ZONE_ID, REGISTER_NO)                                   \
  namespace oceanbase                                                                          \
  {                                                                                            \
  namespace unittest                                                                           \
  {                                                                                            \
  class GET_RESTART_ZONE_TEST_CLASS_NAME(ZONE_ID, REGISTER_NO) : public ObMultiReplicaTestBase \
  {                                                                                            \
  public:                                                                                      \
    GET_RESTART_ZONE_TEST_CLASS_NAME(ZONE_ID, REGISTER_NO)() : ObMultiReplicaTestBase() {}             \
  };                                                                                           \
  TEST_F(GET_RESTART_ZONE_TEST_CLASS_NAME(ZONE_ID, REGISTER_NO), start_observer) {}            \
  }                                                                                            \
  }
#define MULTI_REPLICA_TEST_MAIN_FUNCTION(TEST_DIR_PREFIX)                             \
  int main(int argc, char **argv)                                                     \
  {                                                                                   \
    int ret = OB_SUCCESS;                                                             \
    int restart_zone_id = -1;                                                         \
    int restart_zone_no = 0;                                                          \
    if (argc < 1) {                                                                   \
      abort();                                                                        \
    }                                                                                 \
    std::string app_name = argv[0];                                                   \
    if (argc >= 3) {                                                                  \
      std::string tmp_str;                                                            \
      tmp_str = argv[1];                                                              \
      restart_zone_id = std::stoi(tmp_str);                                           \
      tmp_str = argv[2];                                                              \
      restart_zone_no = std::stoi(tmp_str);                                           \
    }                                                                                 \
    char *log_level = (char *)"INFO";                                                 \
    OB_LOGGER.set_log_level(log_level);                                               \
    ::testing::InitGoogleTest(&argc, argv);                                           \
    if (OB_FAIL(oceanbase::unittest::ObMultiReplicaTestBase::bootstrap_multi_replica( \
            app_name, restart_zone_id, restart_zone_no, #TEST_DIR_PREFIX))) {         \
      fprintf(stdout, "init test case failed. ret = %d", ret);                        \
      return ret;                                                                     \
    }                                                                                 \
    return RUN_ALL_TESTS();                                                           \
  }

    // oceanbase::unittest::init_log_and_gtest(argc, argv);

namespace oceanbase
{
namespace unittest
{

#define CREATE_TEST_TENANT(tenant_id)                                \
  uint64_t tenant_id;                                                \
  SERVER_LOG(INFO, "create_tenant start");                           \
  ASSERT_EQ(OB_SUCCESS, create_tenant());                            \
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));                   \
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2()); \
  SERVER_LOG(INFO, "create_tenant end", K(tenant_id));

#define GET_LS(tenant_id, ls_id_num, ls_handle)                                       \
  ObLSHandle ls_handle;                                                               \
  {                                                                                   \
    share::ObTenantSwitchGuard tenant_guard;                                          \
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));                         \
    ObLSService *ls_svr = MTL(ObLSService *);                                         \
    ASSERT_NE(nullptr, ls_svr);                                                       \
    share::ObLSID ls_id(ls_id_num);                                                   \
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD)); \
    ASSERT_NE(nullptr, ls_handle.get_ls());                                           \
  }

#define BLOCK_MSG_PROCESSOR(timeout_us) \
  {                                     \
    int ret = OB_SUCCESS;               \
    ATOMIC_STORE(&block_msg_, true);    \
    ob_usleep(timeout_us);              \
    ATOMIC_STORE(&block_msg_, false);   \
  }

#define ACQUIRE_CONN_FROM_SQL_PROXY(CONN_NAME, SQL_PROXY) \
  sqlclient::ObISQLConnection *CONN_NAME = nullptr;       \
  ASSERT_EQ(OB_SUCCESS, SQL_PROXY.acquire(CONN_NAME));    \
  ASSERT_NE(nullptr, CONN_NAME);

#define WRITE_SQL_BY_CONN_INNER(conn, sql_str, ret)                        \
  {                                                                        \
    ObSqlString sql;                                                       \
    int64_t affected_rows = 0;                                             \
    ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                            \
    ret = conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows); \
    SERVER_LOG(INFO, "TEST WRITE SQL: ", K(ret), K(sql));                  \
  }

#define WRITE_SQL_BY_CONN(conn, sql_str)             \
  {                                                  \
    int res_ret = OB_SUCCESS;                        \
    WRITE_SQL_BY_CONN_INNER(conn, sql_str, res_ret); \
    ASSERT_EQ(res_ret, OB_SUCCESS);                  \
  }

#define READ_SQL_BY_CONN(conn, result, sql_str)                                                \
  sqlclient::ObMySQLResult *result = nullptr;                                                  \
  ObISQLClient::ReadResult read_res_##result;                                                  \
  {                                                                                            \
    ObSqlString sql;                                                                           \
    ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                                                \
    SERVER_LOG(INFO, "TEST READ SQL: ", K(sql));                                               \
    ASSERT_EQ(OB_SUCCESS, conn->execute_read(OB_SYS_TENANT_ID, sql.ptr(), read_res_##result)); \
    result = read_res_##result.get_result();                                                   \
    ASSERT_EQ(true, OB_NOT_NULL(result));                                                      \
  }

#define GET_RUNNGING_TRX_ID(conn, tx_id)                                                \
  {                                                                                     \
    ASSERT_EQ(true, conn != nullptr);                                                   \
    std::string sql_str = "select TRANS_ID from oceanbase.V$OB_PROCESSLIST where ID = " \
                          + std::to_string(conn->get_sessid());                         \
    READ_SQL_BY_CONN(conn, process_result, sql_str.c_str());                            \
    ASSERT_EQ(OB_SUCCESS, process_result->next());                                      \
    ASSERT_EQ(OB_SUCCESS, process_result->get_int("TRANS_ID", tx_id));                  \
    ASSERT_EQ(true, ::oceanbase::transaction::ObTransID(tx_id).is_valid());             \
    SERVER_LOG(INFO, "[ObMultiReplicaTestBase] get trans_id in current_connection",     \
               K(conn->get_sessid()), K(tx_id));                                        \
  }

#define GET_TX_ID_FROM_SQL_AUDIT(conn, sql, tx_id)                                                \
  {                                                                                               \
    common::ObString trace_id;                                                                    \
    common::ObString query_sql;                                                                   \
    int64_t request_time = 0;                                                                     \
    int64_t ret_code = OB_SUCCESS;                                                                \
    int64_t retry_cnt = 0;                                                                        \
    ASSERT_EQ(true, conn != nullptr);                                                             \
    std::string sql_str =                                                                         \
        "select TX_ID, TRACE_ID, REQUEST_TIME, RET_CODE, RETRY_CNT, QUERY_SQL from "              \
        "oceanbase.V$OB_SQL_AUDIT where QUERY_SQL like "                                          \
        + std::string(" \"") + std::string(sql) + std::string("\" order by REQUEST_TIME DESC");   \
    READ_SQL_BY_CONN(conn, process_result, sql_str.c_str());                                      \
    ASSERT_EQ(OB_SUCCESS, process_result->next());                                                \
    ASSERT_EQ(OB_SUCCESS, process_result->get_int("TX_ID", tx_id));                               \
    ASSERT_EQ(OB_SUCCESS, process_result->get_varchar("TRACE_ID", trace_id));                     \
    ASSERT_EQ(OB_SUCCESS, process_result->get_int("REQUEST_TIME", request_time));                 \
    ASSERT_EQ(OB_SUCCESS, process_result->get_int("RET_CODE", ret_code));                         \
    ASSERT_EQ(OB_SUCCESS, process_result->get_int("RETRY_CNT", retry_cnt));                       \
    ASSERT_EQ(OB_SUCCESS, process_result->get_varchar("QUERY_SQL", query_sql));                   \
    SERVER_LOG(INFO, "[ObMultiReplicaTestBase] query sql_audit for tx_id", K(trace_id), K(tx_id), \
               K(request_time), K(ret_code), K(retry_cnt), K(query_sql));                         \
  }

#define PREPARE_CONN_ENV(conn)                                           \
  WRITE_SQL_BY_CONN(connection, "set ob_trx_timeout = 3000000000");      \
  WRITE_SQL_BY_CONN(connection, "set ob_trx_idle_timeout = 3000000000"); \
  WRITE_SQL_BY_CONN(connection, "set ob_query_timeout = 3000000000");    \
  WRITE_SQL_BY_CONN(connection, "set autocommit=0");

#define RETRY_UNTIL_TIMEOUT(condition, timeout_us, retry_interval_us)               \
  {                                                                                 \
    int64_t start_time = ObTimeUtility::fast_current_time();                        \
    while (!(condition)) {                                                          \
      if (ObTimeUtility::fast_current_time() - start_time > timeout_us) {           \
        ret = OB_TIMEOUT;                                                           \
        break;                                                                      \
      }                                                                             \
      SERVER_LOG(INFO, "retry one time until timeout", K(condition), K(start_time), \
                 K(timeout_us));                                                    \
      ob_usleep(retry_interval_us);                                                 \
    }                                                                               \
  }

template <typename T>
class EventArgSerTool
{
public:
  static int serialize_arg(const T &arg, std::string &event_arg)
  {
    int ret = OB_SUCCESS;

    char tmp_buf[2048];
    memset(tmp_buf, 0, 2048);
    int64_t pos = 0;
    if (OB_FAIL(arg.serialize(tmp_buf, 2047, pos))) {

    } else {
      event_arg = std::string(tmp_buf, pos);
    }
    SERVER_LOG(INFO, "serialize event arg", K(ret), K(arg));
    return ret;
  }

  static int deserialize_arg(T &arg, const std::string &event_arg)
  {
    int ret = OB_SUCCESS;

    int64_t pos = 0;
    if (OB_FAIL(arg.deserialize(event_arg.c_str(), event_arg.size(), pos))) {
    }
    SERVER_LOG(INFO, "deserialize event arg", K(ret), K(arg));
    return ret;
  }
};

class TestTxCtxGuard
{
public:
  static bool is_trx_abort_sql_ret(int ret)
  {
    return ret == OB_TRANS_ROLLBACKED || ret == OB_TRANS_NEED_ROLLBACK || ret == OB_TRANS_KILLED;
  }

public:
  TestTxCtxGuard(int64_t tx_id_num, storage::ObLS *ls_ptr)
      : tx_id_(tx_id_num), tx_ctx_(nullptr), ls_(ls_ptr)
  {}

  ~TestTxCtxGuard()
  {
    int ret = OB_SUCCESS;
    if (tx_ctx_ != nullptr && ls_ != nullptr) {
      ret = ls_->revert_tx_ctx(tx_ctx_);
      if (ret != OB_SUCCESS) {
        TRANS_LOG(ERROR, "revert tx ctx failed", K(ret), KPC(this));
      }
    }
  }

  int init(bool for_replay)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(ls_) || !tx_id_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(tx_id_), KPC(ls_));
    } else if (OB_NOT_NULL(tx_ctx_)) {
      ret = OB_INIT_TWICE;
      TRANS_LOG(WARN, "init test tx ctx guard twice", K(ret), KPC(this));

    } else if (OB_FAIL(ls_->get_tx_ctx(tx_id_, for_replay, tx_ctx_))) {
      TRANS_LOG(WARN, "get tx ctx failed", K(ret), K(for_replay), KPC(this));
    }

    return ret;
  }

  TO_STRING_KV(K(tx_id_), KPC(tx_ctx_), KPC(ls_));

  transaction::ObTransID tx_id_;
  transaction::ObPartTransCtx *tx_ctx_;
  storage::ObLS *ls_;
};

} // namespace unittest
} // namespace oceanbase

#endif
