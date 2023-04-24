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

#ifndef _OCEANBASE_TESTBENCH_MYSQL_PROXY_H_
#define _OCEANBASE_TESTBENCH_MYSQL_PROXY_H_

#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "ob_testbench_server_provider.h"

namespace oceanbase
{
  namespace testbench
  {
    using namespace common::sqlclient;
    /*
      ObTestbenchMySQLProxy uses a single thread timer task (ObMySQLConnectionPool) to refreash connections with each server-tenant.
      ObTestbenchMySQLProxy uses ObTestbenchSystableHelper to query the metadata of the database.
    */
    class ObTestbenchMySQLProxy
    {
    public:
      ObTestbenchMySQLProxy();
      ~ObTestbenchMySQLProxy();
      int stop_and_destroy();
      void set_db_param(const common::ObAddr &addr, const char *cluster_user, const char *cluster_pass, const char *cluster_db);
      int init();
      int start_service();
      int get_mysql_conn(int64_t dblink_id, uint32_t session_id, ObMySQLConnection *&mysql_conn);
      int release_conn(uint32_t session_id, bool success, ObISQLConnection *conn);

    private:
      bool is_inited_;
      int tg_id_;
      ObTestbenchServerProvider svr_provider_;
      ObMySQLConnectionPool sql_conn_pool_;
      ObTestbenchSystableHelper systable_helper_;
      ObConnPoolConfigParam conn_pool_config_;
      libobcdc::MySQLConnConfig mysql_config_;

    private:
      DISALLOW_COPY_AND_ASSIGN(ObTestbenchMySQLProxy);
    };
  }
}
#endif