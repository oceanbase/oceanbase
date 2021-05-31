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

#include <stdlib.h>
#include <string>
#include <vector>
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "rootserver/ob_schema2ddl_sql.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_global_stat_proxy.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace share {
namespace schema {

inline std::string DBInitializer::to_var_name(const std::string& from) const
{
  std::string to = from;
  for (std::string::iterator it = to.begin(); it != to.end(); ++it) {
    if (!isalnum(*it)) {
      *it = '_';
    }
  }
  return to;
}

std::string DBInitializer::get_env_value(const char* name, const char* def_val)
{
  std::string val = def_val;
  const char* env_val = getenv(name);
  if (NULL != env_val) {
    val = env_val;
  }
  return val;
}

inline int DBInitializer::init()
{
  int ret = common::OB_SUCCESS;
  GCTX.split_schema_version_ = OB_INVALID_VERSION;
  int64_t begin_us = ObTimeUtility::current_time();
  if (OB_FAIL(mysql_host_detect())) {
    return ret;
  }
  int64_t end_us = ObTimeUtility::current_time();
  _OB_LOG(INFO, "time to detect mysql server: %ld", end_us - begin_us);

  common::ObSqlString cmd;
  ret = cmd.assign_fmt("mysql -h %s -P %d -u %s %s%s -e "
                       "'"
                       "set global sql_mode = strict_all_tables;"
                       "drop database if exists %s; create database if not exists %s;"
                       "use %s; delimiter $$; create function time_to_usec(t timestamp) "
                       "returns bigint(20) deterministic begin return unix_timestamp(t)*1000000; end$$ "
                       "create function usec_to_time(u bigint(20)) "
                       "returns timestamp deterministic begin return from_unixtime(u/1000000); end$$"
                       "'",
      db_.host_.c_str(),
      db_.port_,
      db_.user_.c_str(),
      db_.pass_.size() > 0 ? "-p" : "",
      db_.pass_.c_str(),
      db_.db_.c_str(),
      db_.db_.c_str(),
      db_.db_.c_str());
  if (OB_FAIL(ret)) {
    _OB_LOG(WARN, "sql_string assign string failed, ret[%d]", ret);
    return ret;
  }

  _OB_LOG(INFO, "init database by command %s", cmd.ptr());
  ret = system(cmd.ptr());
  if (0 != ret) {
    _OB_LOG(WARN, "execute cmd %s failed, ret %d", cmd.ptr(), ret);
    return ret;
  }

  sql_conn_pool_.set_db_param(db_.user_.c_str(), db_.pass_.c_str(), db_.db_.c_str());
  common::ObAddr db_addr;
  db_addr.set_ip_addr(db_.host_.c_str(), db_.port_);
  _OB_LOG(INFO, "init sql connect pool, server %s, database name %s", common::to_cstring(db_addr), db_.db_.c_str());

  ObConnPoolConfigParam param;
  // param.sqlclient_wait_timeout_ = 10; // 10s
  // turn up it, make unittest pass
  param.sqlclient_wait_timeout_ = 100;                 // 100s
  param.long_query_timeout_ = 120 * 1000 * 1000;       // 120s
  param.connection_refresh_interval_ = 200 * 1000;     // 200ms
  param.connection_pool_warn_time_ = 1 * 1000 * 1000;  // 1s
  param.sqlclient_per_observer_conn_limit_ = 10;
  if (common::OB_SUCCESS != (ret = sql_conn_pool_.init(db_addr, param))) {
    _OB_LOG(WARN, "init sql connection pool failed, ret %d", ret);
  } else {
    sql_conn_pool_.set_mode(common::sqlclient::ObMySQLConnection::DEBUG_MODE);
    sql_proxy_.init(&sql_conn_pool_);
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(db_gc())) {
      COMMON_LOG(INFO, "gc failed", K(ret));
    }
  }

  return ret;
}

inline int DBInitializer::create_system_table(bool only_core_tables)
{
  int ret = common::OB_SUCCESS;
  static const schema_create_func all_core_table_schema_creator[] = {
      &share::ObInnerTableSchema::all_core_table_schema, NULL};
  const schema_create_func* creator_ptr_array[] = {all_core_table_schema_creator,
      share::core_table_schema_creators,
      (only_core_tables ? NULL : share::sys_table_schema_creators),
      NULL};

  for (const schema_create_func** creator_ptr_ptr = creator_ptr_array;
       common::OB_SUCCESS == ret && NULL != *creator_ptr_ptr;
       ++creator_ptr_ptr) {
    for (const schema_create_func* creator_ptr = *creator_ptr_ptr; common::OB_SUCCESS == ret && NULL != *creator_ptr;
         ++creator_ptr) {
      share::schema::ObTableSchema table_schema;
      if (OB_SUCCESS != (ret = (*creator_ptr)(table_schema))) {
        _OB_LOG(WARN, "create table schema failed, ret %d", ret);
        ret = common::OB_SCHEMA_ERROR;
      } else if (table_schema.is_view_table()) {
      } else {
        int64_t affect_rows = 0;
        char sql[common::OB_MAX_SQL_LENGTH];
        memset(sql, 0, sizeof(sql));
        if (common::OB_SUCCESS != (ret = rootserver::ObSchema2DDLSql::convert(table_schema, sql, sizeof(sql)))) {
          _OB_LOG(WARN, "convert table schema to create table sql failed, ret %d", ret);
        } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql, affect_rows))) {
          _OB_LOG(WARN, "execute sql failed, ret %d, sql %s", ret, sql);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObGlobalStatProxy proxy(sql_proxy_);
    if (OB_FAIL(proxy.set_init_value(1, 1, 1, 0, 0, 0, 1, 1, 1))) {
      _OB_LOG(WARN, "set init value failed, ret %d", ret);
    }
  }
  /*
  if (OB_SUCC(ret)) {
    if (OB_FAIL(log_init_core_operation())) {
      _OB_LOG(WARN, "log_int_core_operation failed, ret [%d]", ret);
    }
  }
  */
  return ret;
}

inline int DBInitializer::fill_sys_stat_table(const uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  // insert max_used_xxxx_id in __all_sys_stat table
  common::ObSqlString sql_string;
  int64_t affect_rows = 0;
  if (common::OB_SUCCESS !=
      (ret = (sql_string.append_fmt("insert into __all_sys_stat "
                                    "(tenant_id, zone, name, data_type, value, info) values "
                                    "(%lu, '', 'ob_max_used_table_id', 1, '%lu', 'max used table id')",
           tenant_id,
           combine_id(tenant_id, OB_MIN_USER_TABLE_ID))))) {
    _OB_LOG(WARN, "sql_string assign string failed, ret[%d]", ret);
  } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql_string.ptr(), affect_rows))) {
    _OB_LOG(WARN, "execute sql failed, sql[%s], ret[%d]", sql_string.ptr(), ret);
  }

  if (OB_SUCC(ret)) {
    sql_string.reset();
    if (common::OB_SUCCESS !=
        (ret = (sql_string.append_fmt("insert into __all_sys_stat "
                                      "(tenant_id, zone, name, data_type, value, info) values "
                                      "(%lu, '', 'ob_max_used_tenant_id', 1, '%lu', 'max used tenant id')",
             tenant_id,
             OB_USER_TENANT_ID)))) {
      _OB_LOG(WARN, "sql_string assign string failed, ret[%d]", ret);
    } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql_string.ptr(), affect_rows))) {
      _OB_LOG(WARN, "execute sql failed, sql[%s], ret[%d]", sql_string.ptr(), ret);
    }
  }

  if (OB_SUCC(ret)) {
    sql_string.reset();
    if (common::OB_SUCCESS !=
        (ret = (sql_string.append_fmt("insert into __all_sys_stat "
                                      "(tenant_id, zone, name, data_type, value, info) values "
                                      "(%lu, '', 'ob_max_used_database_id', 1, '%lu', 'max used database id')",
             tenant_id,
             combine_id(tenant_id, OB_USER_DATABASE_ID))))) {
      _OB_LOG(WARN, "sql_string assign string failed, ret[%d]", ret);
    } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql_string.ptr(), affect_rows))) {
      _OB_LOG(WARN, "execute sql failed, sql[%s], ret[%d]", sql_string.ptr(), ret);
    }
  }

  if (OB_SUCC(ret)) {
    sql_string.reset();
    if (common::OB_SUCCESS !=
        (ret = (sql_string.append_fmt("insert into __all_sys_stat "
                                      "(tenant_id, zone, name, data_type, value, info) values "
                                      "(%lu, '', 'ob_max_used_tablegroup_id', 1, '%lu', 'max used tablegroup id')",
             tenant_id,
             combine_id(tenant_id, OB_USER_TABLEGROUP_ID))))) {
      _OB_LOG(WARN, "sql_string assign string failed, ret[%d]", ret);
    } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql_string.ptr(), affect_rows))) {
      _OB_LOG(WARN, "execute sql failed, sql[%s], ret[%d]", sql_string.ptr(), ret);
    }
  }

  if (OB_SUCC(ret)) {
    sql_string.reset();
    if (common::OB_SUCCESS !=
        (ret = (sql_string.append_fmt("insert into __all_sys_stat "
                                      "(tenant_id, zone, name, data_type, value, info) values "
                                      "(%lu, '', 'ob_max_used_user_id', 1, '%lu', 'max used user id')",
             tenant_id,
             combine_id(tenant_id, OB_USER_ID))))) {
      _OB_LOG(WARN, "sql_string assign string failed, ret[%d]", ret);
    } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql_string.ptr(), affect_rows))) {
      _OB_LOG(WARN, "execute sql failed, sql[%s], ret[%d]", sql_string.ptr(), ret);
    }
  }

  if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_SUCC(ret)) {
      sql_string.reset();
      if (common::OB_SUCCESS !=
          (ret = (sql_string.append_fmt("insert into __all_sys_stat "
                                        "(tenant_id, zone, name, data_type, value, info) values "
                                        "(%lu, '', 'ob_max_used_unit_config_id', 1, '%lu', 'max used unit config id')",
               tenant_id,
               OB_USER_UNIT_CONFIG_ID)))) {
        _OB_LOG(WARN, "sql_string assign string failed, ret[%d]", ret);
      } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql_string.ptr(), affect_rows))) {
        _OB_LOG(WARN, "execute sql failed, sql[%s], ret[%d]", sql_string.ptr(), ret);
      }
    }

    if (OB_SUCC(ret)) {
      sql_string.reset();
      if (common::OB_SUCCESS != (ret = (sql_string.append_fmt(
                                     "insert into __all_sys_stat "
                                     "(tenant_id, zone, name, data_type, value, info) values "
                                     "(%lu, '', 'ob_max_used_resource_pool_id', 1, '%lu', 'max used resource pool id')",
                                     tenant_id,
                                     OB_USER_RESOURCE_POOL_ID)))) {
        _OB_LOG(WARN, "sql_string assign string failed, ret[%d]", ret);
      } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql_string.ptr(), affect_rows))) {
        _OB_LOG(WARN, "execute sql failed, sql[%s], ret[%d]", sql_string.ptr(), ret);
      }
    }

    if (OB_SUCC(ret)) {
      sql_string.reset();
      if (common::OB_SUCCESS !=
          (ret = (sql_string.append_fmt("insert into __all_sys_stat "
                                        "(tenant_id, zone, name, data_type, value, info) values "
                                        "(%lu, '', 'ob_max_used_unit_id', 1, '%lu', 'max used unit id')",
               tenant_id,
               OB_USER_UNIT_ID)))) {
        _OB_LOG(WARN, "sql_string assign string failed, ret[%d]", ret);
      } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql_string.ptr(), affect_rows))) {
        _OB_LOG(WARN, "execute sql failed, sql[%s], ret[%d]", sql_string.ptr(), ret);
      }
    }

    if (OB_SUCC(ret)) {
      sql_string.reset();
      if (common::OB_SUCCESS !=
          (ret = (sql_string.append_fmt("insert into __all_sys_stat "
                                        "(tenant_id, zone, name, data_type, value, info) values "
                                        "(%lu, '', 'ob_max_used_server_id', 1, '%lu', 'max used server id')",
               tenant_id,
               OB_INIT_SERVER_ID)))) {
        _OB_LOG(WARN, "sql_string assign string failed, ret[%d]", ret);
      } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql_string.ptr(), affect_rows))) {
        _OB_LOG(WARN, "execute sql failed, sql[%s], ret[%d]", sql_string.ptr(), ret);
      }
    }
  }

  return ret;
}

inline int DBInitializer::create_tenant_space_tables(const uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  std::string db = get_db_name();
  ObSqlString sql;
  int64_t affect_rows = 0;
  if (OB_FAIL(sql.assign_fmt("drop database if exists %s_%lu", db.c_str(), tenant_id))) {
    _OB_LOG(WARN, "assign sql failed, ret %d", ret);
  } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql.ptr(), affect_rows))) {
    _OB_LOG(WARN, "execute create database sql failed, ret %d, sql %s", ret, sql.ptr());
  } else if (common::OB_SUCCESS != (ret = sql.assign_fmt("create database %s_%lu", db.c_str(), tenant_id))) {
    _OB_LOG(WARN, "assign sql failed, ret %d", ret);
  } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql.ptr(), affect_rows))) {
    _OB_LOG(WARN, "execute create database sql failed, ret %d, sql %s", ret, sql.ptr());
  } else {
    const schema_create_func* creator_ptr_array[] = {
        share::core_table_schema_creators, share::sys_table_schema_creators};
    ObTableSchema tables[ARRAYSIZEOF(core_table_schema_creators) + ARRAYSIZEOF(sys_table_schema_creators)];
    int64_t idx = 0;
    // build system table schema to %tables
    for (int64_t i = 0; common::OB_SUCCESS == ret && i < ARRAYSIZEOF(creator_ptr_array); i++) {
      for (const schema_create_func* creator_ptr = creator_ptr_array[i];
           common::OB_SUCCESS == ret && NULL != *creator_ptr;
           ++creator_ptr) {
        if (OB_SUCCESS != (ret = (*creator_ptr)(tables[idx++]))) {
          _OB_LOG(WARN, "create table schema fialed, ret %d", ret);
          ret = common::OB_SCHEMA_ERROR;
        }
      }
    }

    for (int64_t i = 0;
         common::OB_SUCCESS == ret && i < ARRAYSIZEOF(tenant_space_tables) && !is_virtual_table(tenant_space_tables[i]);
         i++) {
      uint64_t tid = tenant_space_tables[i];
      // found table schema
      for (idx = 0; idx < ARRAYSIZEOF(tables); ++idx) {
        if (extract_pure_id(tables[idx].get_table_id()) == tid) {
          break;
        }
      }
      char csql[common::OB_MAX_SQL_LENGTH];
      memset(csql, 0, sizeof(csql));
      if (tables[idx].is_view_table()) {
      } else if (common::OB_SUCCESS != (ret = rootserver::ObSchema2DDLSql::convert(tables[idx], csql, sizeof(csql)))) {
        _OB_LOG(WARN, "convert table schema to create table sql failed, ret %d", ret);
      } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(tenant_id, csql, affect_rows))) {
        _OB_LOG(WARN, "execute sql failed, ret %d, sql %s", ret, csql);
      }
    }
  }
  return ret;
}

inline int DBInitializer::create_tenant_space(const uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(fill_sys_stat_table(tenant_id))) {
    _OB_LOG(WARN, "fill sys stat table failed, ret %d", ret);
  }
  return ret;
  /*
   * We move system table to system tenant, only fill sys stat table here.
   *
  int ret = common::OB_SUCCESS;
  std::string db = get_db_name();
  ObSqlString sql;
  int64_t affect_rows = 0;
  if (OB_FAIL(sql.assign_fmt("drop database if exists %s_%lu", db.c_str(), tenant_id))) {
    _OB_LOG(WARN, "assign sql failed, ret %d", ret);
  } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql.ptr(), affect_rows))) {
    _OB_LOG(WARN, "execute create database sql failed, ret %d, sql %s", ret, sql.ptr());
  } else if (common::OB_SUCCESS != (ret = sql.assign_fmt("create database %s_%lu",
      db.c_str(), tenant_id))) {
    _OB_LOG(WARN, "assign sql failed, ret %d", ret);
  } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(sql.ptr(), affect_rows))) {
    _OB_LOG(WARN, "execute create database sql failed, ret %d, sql %s", ret, sql.ptr());
  } else {
    const schema_create_func *creator_ptr_array[] = { share::core_table_schema_creators,
      share::sys_table_schema_creators };
    ObTableSchema tables[ARRAYSIZEOF(core_table_schema_creators)
        + ARRAYSIZEOF(sys_table_schema_creators)];
    int64_t idx = 0;
    // build system table schema to %tables
    for (int64_t i = 0; common::OB_SUCCESS == ret && i < ARRAYSIZEOF(creator_ptr_array); i++) {
      for (const schema_create_func *creator_ptr = creator_ptr_array[i];
        common::OB_SUCCESS == ret && NULL != *creator_ptr; ++creator_ptr) {
        if (OB_SUCCESS != (ret = (*creator_ptr)(tables[idx++]))) {
          _OB_LOG(WARN, "create table schema fialed, ret %d", ret);
          ret = common::OB_SCHEMA_ERROR;
        }
      }
    }

    for (int64_t i = 0;
        common::OB_SUCCESS == ret && i < ARRAYSIZEOF(tenant_space_tables)
        && !is_virtual_table(tenant_space_tables[i]); i++) {
      uint64_t tid = tenant_space_tables[i];
      // found table schema
      for (idx = 0; idx < ARRAYSIZEOF(tables); ++idx) {
        if (extract_pure_id(tables[idx].get_table_id()) == tid) {
          break;
        }
      }
      char csql[common::OB_MAX_SQL_LENGTH];
      memset(csql, 0, sizeof(csql));
      if (tables[idx].is_view_table()) {
      } else if (common::OB_SUCCESS != (ret = rootserver::ObSchema2DDLSql::convert(
              tables[idx], csql, sizeof(csql)))) {
        _OB_LOG(WARN, "convert table schema to create table sql failed, ret %d", ret);
      } else if (common::OB_SUCCESS != (ret = sql_proxy_.write(
          tenant_id, csql, affect_rows))) {
        _OB_LOG(WARN, "execute sql failed, ret %d, sql %s", ret, csql);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (common::OB_SUCCESS != (ret = fill_sys_stat_table(tenant_id))) {
      _OB_LOG(WARN, "fill sys stat table failed, ret %d", ret);
    }
  }
  return ret;
  */
}

inline int DBInitializer::create_virtual_table(const schema_create_func create_func)
{
  int ret = OB_SUCCESS;
  ObTableSchema table_schema;
  char sql[common::OB_MAX_SQL_LENGTH];
  memset(sql, 0, sizeof(sql));
  int64_t affected_rows = 0;
  if (OB_FAIL(create_func(table_schema))) {
    _OB_LOG(WARN, "create table schema failed, ret %d", ret);
  } else {
    table_schema.set_part_level(PARTITION_LEVEL_ZERO);
    if (OB_FAIL(rootserver::ObSchema2DDLSql::convert(table_schema, sql, sizeof(sql)))) {
      _OB_LOG(WARN, "convert table schema to sql failed, ret %d", ret);
    } else if (OB_FAIL(sql_proxy_.write(sql, affected_rows))) {
      _OB_LOG(WARN, "execute sql failed, ret %d, sql %s", ret, sql);
    }
  }
  return ret;
}

inline int DBInitializer::log_init_core_operation()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ret = sql_proxy_.write("insert into __all_core_table (table_name, row_id, column_name, column_value) "
                         "values ('__all_ddl_operation', 0, 'core_schema_version', '0')",
      affected_rows);
  if (OB_FAIL(ret)) {
    _OB_LOG(WARN, "execute sql failed, ret %d", ret);
  }
  return ret;
}

inline int DBInitializer::mysql_host_detect()
{
  int ret = OB_SUCCESS;
  std::vector<DBConfig> dbs;
  DBConfig db;
  db.host_ = get_env_value("MYSQL_HOST", "100.81.152.48");
  std::string port = get_env_value("MYSQL_PORT", "3306");
  db.port_ = atoi(port.c_str());
  db.user_ = get_env_value("MYSQL_USER", "root");
  db.pass_ = get_env_value("MYSQL_PASS", "");
  dbs.push_back(db);
  std::string fix_mysql_host = get_env_value("FIX_MYSQL_HOST", "");
  if (fix_mysql_host.empty()) {
    /*
    "100.81.152.31",
      "100.81.152.32",
      "100.81.152.33",
      "100.81.152.34",
      "100.81.152.35",
      "100.81.152.36",
      "100.81.152.37",
      "100.81.152.38",
      "100.81.152.39",
      "100.81.152.40",
      "100.81.152.41",
      "100.81.152.42",
      "100.81.152.43",
      "100.81.152.44",
    */
    const char* extra_hosts[] = {"100.81.152.48"};
    for (int64_t i = 0; i < ARRAYSIZEOF(extra_hosts); ++i) {
      db.host_ = extra_hosts[i];
      dbs.push_back(db);
    }
    std::random_shuffle(dbs.begin(), dbs.end());
  }
  common::ObSqlString cmd;
  db_ = dbs.at(0);
  FOREACH(d, dbs)
  {
    cmd.assign_fmt(
        "x=$(mysql -h %s -P%d -u%s -P '%s' -e 'select version()' 2>/dev/null| tail -n 1) ; [[ \"$x\" > \"5.6\" ]]",
        d->host_.c_str(),
        d->port_,
        d->user_.c_str(),
        d->pass_.c_str());
    if (0 == system(cmd.ptr())) {
      db_ = *d;
      break;
    }
  }
  // generate database name
  std::string host = get_env_value("HOSTNAME", "");
  const std::string user = get_env_value("USER", "");
  // avoid mysql identifier name is too long error
  if (user.length() + host.length() > 28) {
    host = std::string("");
  }
  const int64_t now = ObTimeUtility::current_time();
  const int64_t pid = getpid();
  if (!fix_mysql_host.empty()) {
    cmd.assign_fmt("_unittest_%s", user.c_str());
  } else {
    cmd.assign_fmt("_unittest_%ld_%s_%s_%ld", now, user.c_str(), host.c_str(), pid);
  }
  db_.db_ = to_var_name(std::string(cmd.ptr()));
  COMMON_LOG(INFO,
      "USE MYSQL SERVER",
      "host",
      db_.host_.c_str(),
      "port",
      db_.port_,
      "user",
      db_.user_.c_str(),
      "database",
      db_.db_.c_str());
  return ret;
}

inline int DBInitializer::db_gc()
{
  int ret = OB_SUCCESS;
  const char* prefix = "_unittest_";
  std::vector<std::string> dbs;
  {
    common::ObMySQLProxy::MySQLResult res;
    if (common::OB_SUCCESS == sql_proxy_.read(res, "show databases")) {
      while (common::OB_SUCCESS == res.get_result()->next()) {
        common::ObString v;
        res.get_result()->get_varchar(0L, v);
        if (strncmp(prefix, v.ptr(), std::min((size_t)strlen(prefix), (size_t)v.length())) == 0) {
          std::string db(v.ptr(), v.length());
          dbs.push_back(db);
        }
      }
    }
  }
  FOREACH(db, dbs)
  {
    const int64_t create_time = atol(db->c_str() + strlen(prefix));
    if (create_time > 0 && create_time + 24 * 60 * 60 * 100000L < ObTimeUtility::current_time()) {
      int64_t affacted_row = 0;
      std::string cmd = std::string("drop database if exists ") + *db;
      sql_proxy_.write(cmd.c_str(), affacted_row);
    }
  }
  return ret;
}

inline DBInitializer::~DBInitializer()
{
  std::string fix_mysql_host = get_env_value("FIX_MYSQL_HOST", "");
  if (fix_mysql_host.empty() && !db_.db_.empty()) {
    int64_t affacted_row = 0;
    std::string cmd = std::string("drop database if exists ") + db_.db_;
    sql_proxy_.set_active();
    sql_proxy_.write(cmd.c_str(), affacted_row);
  }
  COMMON_LOG(INFO,
      "USE MYSQL SERVER",
      "host",
      db_.host_.c_str(),
      "port",
      db_.port_,
      "user",
      db_.user_.c_str(),
      "database",
      db_.db_.c_str());
}

}  // end namespace schema
}  // end namespace share
}  // end namespace oceanbase
