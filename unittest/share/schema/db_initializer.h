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

#ifndef OCEANBASE_SCHEMA_DB_INITIALIZER_H_
#define OCEANBASE_SCHEMA_DB_INITIALIZER_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_single_mysql_connection_pool.h"
#include "share/config/ob_server_config.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include <string>

namespace oceanbase
{
namespace share
{
namespace schema
{

// Mysql server backend intialize helper for unittest.
//
// 1. initialize empty database and mysql client, connect to localhost mysql:
//     host: 127.0.0.1
//     port: 3306
//     user: root
//     pass: ""
//     database: ${USER//./_}_unittest
//
// 2. create ocenabase system table schema in mysql server.
//
class DBInitializer
{
public:
  DBInitializer() : is_inited_(false) {}
  virtual ~DBInitializer();

  int init();
  std::string get_db_name() { return db_.db_; }
  int create_system_table(bool only_core_tables, const uint64_t tenant_id = common::OB_SYS_TENANT_ID);
  int fill_sys_stat_table(const uint64_t tenant_id = common::OB_SYS_TENANT_ID);
  int create_tenant_space_tables(const uint64_t tenant_id);
  int create_tenant_space(const uint64_t tenant_id);
  int create_virtual_table(const schema_create_func create_func);
  common::ObMySQLProxy &get_sql_proxy() { return sql_proxy_; }
  common::ObServerConfig &get_config() { return common::ObServerConfig::get_instance(); }

private:
  struct DBConfig
  {
    std::string host_;
    int port_;
    std::string user_;
    std::string pass_;
    std::string db_;
  };

private:
  std::string to_var_name(const std::string &from) const;
  std::string get_env_value(const char *name, const char *def_val);
  int db_gc();
  int mysql_host_detect();
  int create_db();
  int init_sql_proxy();
  int log_init_core_operation();

private:
  bool is_inited_;
  DBConfig db_;
  common::sqlclient::ObSingleMySQLConnectionPool sql_conn_pool_;
  common::ObMySQLProxy sql_proxy_;
  uint64_t tenant_id_;

  DISALLOW_COPY_AND_ASSIGN(DBInitializer);
};

} // end namespace schema
} // end namespace share
} // end namespace oceanbase

// all implemented in .h file for convenient: no need to link
#include "db_initializer.ipp"

#endif // OCEANBASE_SCHEMA_DB_INITIALIZER_H_
