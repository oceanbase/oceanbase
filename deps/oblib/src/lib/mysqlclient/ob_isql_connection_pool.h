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

#ifndef OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_POOL_H_
#define OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_POOL_H_

#include <stdint.h>
#include "lib/list/ob_list.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_cached_allocator.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObAddr;
class ObString;

struct ObConnPoolConfigParam
{
  ObConnPoolConfigParam() { reset(); }
  ~ObConnPoolConfigParam() { }
  void reset() { memset(this,0, sizeof(ObConnPoolConfigParam)); }

  int64_t sqlclient_wait_timeout_;      // s
  int64_t connection_refresh_interval_; // us
  int64_t connection_pool_warn_time_;   // us
  int64_t long_query_timeout_;          // us
  int64_t sqlclient_per_observer_conn_limit_;
};
namespace sqlclient
{

class ObISQLConnection;

enum ObSQLConnPoolType
{
  UNKNOWN_POOL,
  MYSQL_POOL,
  INNER_POOL,
};

enum DblinkDriverProto{
  DBLINK_UNKNOWN = -1,
  DBLINK_DRV_OB = 0,
  DBLINK_DRV_OCI,
};

enum DblinkPoolType {
  DBLINK_POOL_UNKNOW = -1,
  DBLINK_POOL_DEF = 0, // for link scan read write
  DBLINK_POOL_SCHEMA = 1, // for schema read
};

struct dblink_param_ctx{
  uint16_t charset_id_; // this link expected charset id for string column
  uint16_t ncharset_id_; // this link expected national charset id for nvarchar
  DblinkPoolType pool_type_;
  DblinkDriverProto link_type_;
  uint32_t sessid_;
  uint64_t tenant_id_;
  uint64_t dblink_id_;
  int64_t sql_request_level_;
  const char *set_sql_mode_cstr_;
  const char *set_client_charset_cstr_;
  const char *set_connection_charset_cstr_;
  const char *set_results_charset_cstr_;
  dblink_param_ctx() :
  charset_id_(static_cast<uint16_t>(common::ObNlsCharsetId::CHARSET_AL32UTF8_ID)), //utf8, deault value, don't modify it cause dblink pull meta need it
  ncharset_id_(static_cast<uint16_t>(common::ObNlsCharsetId::CHARSET_AL32UTF8_ID)), //utf8, deault value, don't modify it cause dblink pull meta need it
  pool_type_(DBLINK_POOL_DEF),
  link_type_(DblinkDriverProto::DBLINK_UNKNOWN),
  sessid_(0),
  tenant_id_(OB_INVALID_ID),
  dblink_id_(OB_INVALID_ID),
  sql_request_level_(0),
  set_sql_mode_cstr_(NULL),
  set_client_charset_cstr_(NULL),
  set_connection_charset_cstr_(NULL),
  set_results_charset_cstr_(NULL)
  { }
  TO_STRING_KV(K_(charset_id),
               K_(ncharset_id),
               K_(pool_type),
               K_(link_type),
               K_(sessid),
               K_(tenant_id),
               K_(dblink_id),
               K_(sql_request_level),
               K_(set_sql_mode_cstr),
               K_(set_client_charset_cstr),
               K_(set_connection_charset_cstr),
               K_(set_results_charset_cstr));
};

class ObISQLConnectionPool
{
public:
  ObISQLConnectionPool() {};
  virtual ~ObISQLConnectionPool() {};

  // sql string escape
  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size) = 0;

  // acquired connection must be released
  virtual int acquire(ObISQLConnection *&conn, ObISQLClient *client_addr)
  {
    return this->acquire(OB_INVALID_TENANT_ID, conn, client_addr, 0);
  }
  virtual int acquire(const uint64_t tenant_id, ObISQLConnection *&conn, ObISQLClient *client_addr, const int32_t group_id) = 0;
  virtual int release(ObISQLConnection *conn, const bool success) = 0;
  virtual int on_client_inactive(ObISQLClient *client_addr) = 0;
  virtual ObSQLConnPoolType get_type() = 0;
  virtual DblinkDriverProto get_pool_link_driver_proto() = 0;

  // for dblink
  virtual int create_dblink_pool(const dblink_param_ctx &param_ctx, const ObAddr &server,
                                 const ObString &db_tenant, const ObString &db_user,
                                 const ObString &db_pass, const ObString &db_name,
                                 const common::ObString &conn_str,
                                 const common::ObString &cluster_str) = 0;
  virtual int acquire_dblink(const dblink_param_ctx &param_ctx, ObISQLConnection *&dblink_conn) = 0;
  virtual int release_dblink(ObISQLConnection *dblink_conn) = 0;
  virtual int do_acquire_dblink(const dblink_param_ctx &param_ctx, ObISQLConnection *&dblink_conn) = 0;
  virtual int try_connect_dblink(ObISQLConnection *dblink_conn, int64_t sql_request_level = 0) = 0;
  virtual int clean_dblink_connection(uint64_t tenant_id) = 0;
};

} // end namespace sqlclient
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_MYSQLCLIENT_OB_ISQL_CONNECTION_POOL_H_
