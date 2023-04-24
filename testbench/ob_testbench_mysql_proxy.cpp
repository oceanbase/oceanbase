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

#include "share/ob_thread_mgr.h"
#include "ob_testbench_mysql_proxy.h"

namespace oceanbase
{
  namespace testbench
  {
    ObTestbenchMySQLProxy::ObTestbenchMySQLProxy() : is_inited_(false),
                                                     tg_id_(-1),
                                                     svr_provider_(),
                                                     sql_conn_pool_(),
                                                     systable_helper_(),
                                                     conn_pool_config_(),
                                                     mysql_config_()
    {
    }

    ObTestbenchMySQLProxy::~ObTestbenchMySQLProxy()
    {
    }

    int ObTestbenchMySQLProxy::stop_and_destroy()
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(WARN, "ObTestbenchMySQLProxy not init", KR(ret));
      }
      else
      {
        sql_conn_pool_.stop();
        sql_conn_pool_.close_all_connection();
        TG_STOP(tg_id_);
        TG_WAIT(tg_id_);
        svr_provider_.destroy();
        systable_helper_.destroy();
      }
      return ret;
    }

    void ObTestbenchMySQLProxy::set_db_param(const common::ObAddr &addr, const char *cluster_user, const char *cluster_pass, const char *cluster_db)
    {
      const int64_t sql_conn_timeout_us = 10L * 1000 * 1000;
      const int64_t sql_query_timeout_us = 10L * 1000 * 1000;
      conn_pool_config_.reset();
      conn_pool_config_.connection_refresh_interval_ = 60L * 1000L * 1000L;       // us
      conn_pool_config_.sqlclient_wait_timeout_ = sql_conn_timeout_us / 1000000L; // s
      conn_pool_config_.connection_pool_warn_time_ = 60L * 1000L * 1000L;         // us
      conn_pool_config_.long_query_timeout_ = sql_query_timeout_us;               // us
      conn_pool_config_.sqlclient_per_observer_conn_limit_ = 500;                 // @todo as param
      mysql_config_.reset(addr, cluster_user, cluster_pass, cluster_db, sql_conn_timeout_us / 1000000L, sql_query_timeout_us / 1000000L);
      sql_conn_pool_.set_db_param(cluster_user, cluster_pass, cluster_db);
    }

    int ObTestbenchMySQLProxy::init()
    {
      int ret = OB_SUCCESS;
      if (IS_INIT)
      {
        ret = OB_INIT_TWICE;
        TESTBENCH_LOG(WARN, "ObTestbenchMySQLProxy init twice", KR(ret));
      }
      else if (OB_FAIL(systable_helper_.init_conn(mysql_config_)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchMySQLProxy systable_helper_ init_conn fail", KR(ret));
      }
      else if (OB_FAIL(svr_provider_.init(systable_helper_)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchMySQLProxy svr_provider_ init fail", KR(ret));
      }
      else if (FALSE_IT(sql_conn_pool_.update_config(conn_pool_config_)))
      {
        ret = OB_ERR_UNEXPECTED;
        TESTBENCH_LOG(ERROR, "ObTestbenchMySQLProxy sql_conn_pool_ update_config fail", KR(ret));
      }
      else if (FALSE_IT(sql_conn_pool_.set_server_provider(&svr_provider_)))
      {
        ret = OB_ERR_UNEXPECTED;
        TESTBENCH_LOG(ERROR, "ObTestbenchMySQLProxy sql_conn_pool_ set_server_provider fail", KR(ret));
      }
      else
      {
        is_inited_ = true;
        TESTBENCH_LOG(INFO, "ObTestbenchMySQLProxy init success");
      }
      return ret;
    }

    int ObTestbenchMySQLProxy::start_service()
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(WARN, "ObTestbenchMySQLProxy not init", KR(ret));
      }
      else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::MysqlProxyPool, tg_id_)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchMySQLProxy MysqlProxyPool create fail", KR(ret), K_(tg_id));
      }
      else if (OB_FAIL(TG_START(tg_id_)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchMySQLProxy MysqlProxyPool start fail", KR(ret), K_(tg_id));
      }
      else if (OB_FAIL(sql_conn_pool_.start(tg_id_)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchMySQLProxy sql_conn_pool start fail", KR(ret), K_(tg_id));
      }
      else if (OB_FAIL(sql_conn_pool_.create_all_dblink_pool()))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchMySQLProxy sql_conn_pool_ create_all_dblink_pool fail", KR(ret));
      }
      else
      {
        TESTBENCH_LOG(INFO, "ObTestbenchMySQLProxy start_service success");
      }
      return ret;
    }

    int ObTestbenchMySQLProxy::get_mysql_conn(int64_t dblink_id, uint32_t session_id, ObMySQLConnection *&mysql_conn)
    {
      int ret = OB_SUCCESS;
      ObServerConnectionPool *svr_pool = NULL;
      mysql_conn = NULL;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "ObTestbenchMySQLProxy not init", KR(ret));
      }
      if (OB_FAIL(sql_conn_pool_.get_dblink_pool(dblink_id, svr_pool)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchMySQLProxy sql_conn_pool_ get_dblink_pool fail", KR(ret));
      }
      else if (OB_FAIL(svr_pool->acquire(mysql_conn, session_id)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchMySQLProxy svr_pool acquire fail", KR(ret));
      }
      else
      {
        TESTBENCH_LOG(DEBUG, "ObTestbenchMySQLProxy get_mysql_conn success", K(dblink_id), K(session_id));
      }
      return ret;
    }

    int ObTestbenchMySQLProxy::release_conn(uint32_t session_id, bool success, ObISQLConnection *conn)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(sql_conn_pool_.release(conn, success, session_id)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchMySQLProxy sql_conn_pool_ release_conn fail", KR(ret));
      }
      else
      {
        TESTBENCH_LOG(DEBUG, "sql_conn_pool_ release_conn success");
      }
      return ret;
    }
  }
}