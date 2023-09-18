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
 *
 * OBCDCMySQLProxy impl
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_mysql_proxy.h"                           // ObLogMysqlProxy

#include "lib/mysqlclient/ob_mysql_server_provider.h"     // ObMySQLServerProvider
#include "ob_log_utils.h"                                 // is_mysql_client_errno
#include "share/ob_thread_mgr.h"
#include "ob_log_mysql_connector.h"                       // ObLogMySQLConnector
#include "ob_log_config.h"                                // TCONF

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

namespace oceanbase
{
namespace libobcdc
{

ObLogMysqlProxy::ObLogMysqlProxy() :
    inited_(false),
    is_oracle_mode_(false),
    connection_pool_(),
    mysql_proxy_(),
    tg_id_(-1)
{
  cluster_user_[0] = '\0';
  cluster_password_[0] = '\0';
}

ObLogMysqlProxy::~ObLogMysqlProxy()
{
  destroy();
}

int ObLogMysqlProxy::init(
    const char *cluster_user,
    const char *cluster_password,
    const int64_t sql_conn_timeout_us,
    const int64_t sql_query_timeout_us,
    const bool enable_ssl_client_authentication,
    ServerProviderType *server_provider,
    bool is_tenant_server_provider)
{
  int ret = OB_SUCCESS;
  int64_t user_pos = 0;
  int64_t password_pos = 0;
  int64_t db_pos = 0;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogMysqlProxy has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(server_provider)
      || OB_ISNULL(cluster_user)
      || OB_ISNULL(cluster_password)
      || OB_UNLIKELY(sql_conn_timeout_us <= 0)
      || OB_UNLIKELY(sql_query_timeout_us <= 0)) {
    LOG_ERROR("invalid argument", K(server_provider),
        K(cluster_user), K(cluster_password), K(sql_conn_timeout_us),
        K(sql_query_timeout_us));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::LogMysqlPool, tg_id_))) {
    LOG_ERROR("create connection pool timer pool failed", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_ERROR("init connection pool timer fail", KR(ret));
  } else if (OB_FAIL(databuff_printf(cluster_user_, sizeof(cluster_user_), user_pos, "%s", cluster_user))) {
    LOG_ERROR("print cluster_user fail", KR(ret), K(user_pos), K(cluster_user));
  } else if (OB_FAIL(databuff_printf(cluster_password_, sizeof(cluster_password_), password_pos, "%s", cluster_password))) {
    LOG_ERROR("print cluster_password fail", KR(ret), K(password_pos), K(cluster_password));
  } else {
    const char *db_name = nullptr;
    if (is_tenant_server_provider) {
      // tenant conn poll will always query via sys tenant
      db_name = ObLogMySQLConnector::DEFAULT_DB_NAME_MYSQL_MODE;
    } else if (OB_FAIL(detect_tenant_mode_(server_provider))) {
    LOG_ERROR("detect_tenant_mode_ failed", KR(ret), K(cluster_user), K(cluster_password));
    } else if (is_oracle_mode_) {
      db_name = ObLogMySQLConnector::DEFAULT_DB_NAME_ORACLE_MODE;
    } else {
      db_name = ObLogMySQLConnector::DEFAULT_DB_NAME_MYSQL_MODE;
    }
    ObConnPoolConfigParam conn_pool_config;
    conn_pool_config.reset();

    // Configure refresh interval
    // 1. The default is the shortest refresh time when no connection is available
    // 2. When a connection is available, the actual refresh time is (connection_refresh_interval * 50)
    conn_pool_config.connection_refresh_interval_ = 1L * 1000L * 1000L; // us
    conn_pool_config.sqlclient_wait_timeout_ = sql_conn_timeout_us / 1000000L; // s
    conn_pool_config.connection_pool_warn_time_ = 10L * 1000L * 1000L;  // us
    conn_pool_config.long_query_timeout_ = sql_query_timeout_us;     // us
    conn_pool_config.sqlclient_per_observer_conn_limit_ = 20;   // us

    _LOG_INFO("mysql connection pool: sql_conn_timeout_us=%ld us, "
        "sqlclient_wait_timeout=%ld sec, sql_query_timeout_us=%ld us, "
        "long_query_timeout=%ld us, connection_refresh_interval=%ld us, "
        "connection_pool_warn_time=%ld us, sqlclient_per_observer_conn_limit=%ld, is_oracle_mode=%d",
        sql_conn_timeout_us, conn_pool_config.sqlclient_wait_timeout_,
        sql_query_timeout_us, conn_pool_config.long_query_timeout_,
        conn_pool_config.connection_refresh_interval_, conn_pool_config.connection_pool_warn_time_,
        conn_pool_config.sqlclient_per_observer_conn_limit_, is_oracle_mode_);

    connection_pool_.update_config(conn_pool_config);
    connection_pool_.set_server_provider(server_provider);
    if (is_tenant_server_provider) {
      connection_pool_.set_pool_type(MySQLConnectionPoolType::TENANT_POOL);
    }
    if (! enable_ssl_client_authentication) {
      connection_pool_.disable_ssl();
    }
    if (OB_FAIL(connection_pool_.set_db_param(cluster_user_,
        cluster_password_,
        db_name))) {
      LOG_ERROR("set connection pool db param fail", KR(ret),
          K(cluster_user_), K(cluster_password_), K_(is_oracle_mode));
    } else if (OB_FAIL(connection_pool_.start(tg_id_))) {
      // launch ConnectinPool
      LOG_ERROR("start connection pool fail", KR(ret));
    } else if (OB_FAIL(mysql_proxy_.init(&connection_pool_))) { // init MySQL Proxy
      LOG_ERROR("init mysql proxy fail", KR(ret));
    } else {
      LOG_INFO("ObLogMysqlProxy init succ", "use_ssl", connection_pool_.is_use_ssl());
      inited_ = true;
    }
  }

  return ret;
}

void ObLogMysqlProxy::destroy()
{
  inited_ = false;

  connection_pool_.stop();
  // NOTICE: should not stop and wait timer task cause timer task will cancel in connection_pool_
  // deconstruct, stop and wait will invoke by cancel in timer task
  // TG_DESTROY(tg_id_);

  cluster_user_[0] = '\0';
  cluster_password_[0] = '\0';
  is_oracle_mode_ = false;
  tg_id_ = -1;
}

void ObLogMysqlProxy::stop()
{
  connection_pool_.stop();
}

int ObLogMysqlProxy::detect_tenant_mode_(ServerProviderType *server_provider)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(server_provider)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid server_provider", KR(ret));
  } else {
    const int64_t svr_cnt = server_provider->get_server_count();

    if (OB_UNLIKELY(0 >= svr_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("expect valid svr_cnt");
    } else {
      bool detect_succ = false;

      for (int idx = 0; OB_SUCC(ret) && ! detect_succ && idx < svr_cnt; idx++) {
        common::ObAddr server;
        if (OB_FAIL(server_provider->get_server(idx, server))) {
          LOG_ERROR("failed to get server", KR(ret), K(idx), K(svr_cnt));
        } else {
          const char *default_db_name = "";
          const int64_t connect_timeout_sec = TCONF.rs_sql_connect_timeout_sec;
          const int64_t query_timeout_sec = TCONF.rs_sql_query_timeout_sec;
          const bool enable_ssl_client_authentication = (1 == TCONF.ssl_client_authentication);
          ObLogMySQLConnector conn;
          MySQLConnConfig config;
          config.reset(server, cluster_user_, cluster_password_, default_db_name, connect_timeout_sec, query_timeout_sec);

          if (!config.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("invalid mysql_conn_config", KR(ret), K(config));
          } else if (OB_FAIL(conn.init(config, enable_ssl_client_authentication))) {
            LOG_WARN("init ObLogMySQLConnector failed", KR(ret), K(config), K(enable_ssl_client_authentication));
            // reset OB_SUCCESS, need retry
            ret = OB_SUCCESS;
          } else {
            is_oracle_mode_ = conn.is_oracle_mode();
            detect_succ = true;
            LOG_INFO("[DETECT_TENANT_MODE] detect tenant mode success", KR(ret), K_(is_oracle_mode), K(config));
          }
        }
      }
      if (OB_UNLIKELY(!detect_succ)) {
        ret = OB_CONNECT_ERROR;
        LOG_ERROR("[DETECT_TENANT_MODE] connect to all server in tenant endpoint list failed", KR(ret), K(svr_cnt));
      }
    }
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
