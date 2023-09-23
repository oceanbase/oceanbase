#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "logservice/libobcdc/src/ob_log_mysql_connector.h"
#include "gtest/gtest.h"

namespace oceanbase {
namespace unittest {

class TestConfig : public ::testing::Test {
public:
  TestConfig() : mysql_config() {}
  ~TestConfig() {}

  virtual void SetUp() {
    common::ObAddr addr;
    const char *host = "127.0.0.1";
    const int32_t port = 2881;
    addr.set_ip_addr(host, port);
    const int64_t sql_conn_timeout_us = 10L * 1000 * 1000;
    const int64_t sql_query_timeout_us = 10L * 1000 * 1000;
    const char *user = "root@sys";
    const char *pass = "";
    const char *db = "oceanbase";
    mysql_config.reset(addr, user, pass, db, sql_conn_timeout_us / 1000000L,
                       sql_query_timeout_us / 1000000L);
    conn_pool_config.reset();
    conn_pool_config.connection_refresh_interval_ = 60L * 1000L * 1000L;
    conn_pool_config.sqlclient_wait_timeout_ = sql_conn_timeout_us / 1000000L;
    conn_pool_config.connection_pool_warn_time_ = 60L * 1000L * 1000L;
    conn_pool_config.long_query_timeout_ = sql_query_timeout_us;
    conn_pool_config.sqlclient_per_observer_conn_limit_ = 500;
  }
  virtual void Tear() {}

protected:
  libobcdc::MySQLConnConfig mysql_config;
  ObConnPoolConfigParam conn_pool_config;
};
} // namespace unittest
} // namespace oceanbase