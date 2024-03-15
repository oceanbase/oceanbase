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

#ifndef OCEANBASE_SQL_OB_DBLINK_UTILS_H
#define OCEANBASE_SQL_OB_DBLINK_UTILS_H

#include "lib/allocator/page_arena.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/net/ob_addr.h"
#include "lib/string/ob_string.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#ifdef OB_BUILD_DBLINK
#include "lib/oracleclient/ob_oci_environment.h"
#endif
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace sql
{

class ObDblinkService
{
public:
#ifdef OB_BUILD_DBLINK
  static common::sqlclient::ObTenantOciEnvs * get_tenant_oci_envs();
  static int init_oci_envs_func_ptr();
#endif
  static int get_charset_id(ObSQLSessionInfo *session_info,
                            uint16_t &charset_id,
                            uint16_t &ncharset_id);
  static int init_dblink_param_ctx(common::sqlclient::dblink_param_ctx &param_ctx,
                                                    sql::ObSQLSessionInfo *session_info,
                                                    ObIAllocator &allocator,
                                                    uint64_t dblink_id,
                                                    common::sqlclient::DblinkDriverProto link_type = common::sqlclient::DblinkDriverProto::DBLINK_DRV_OB,
                                                    common::sqlclient::DblinkPoolType pool_type = common::sqlclient::DblinkPoolType::DBLINK_POOL_DEF);
  static int check_lob_in_result(common::sqlclient::ObMySQLResult *result, bool &have_lob);
  static int get_length_from_type_text(ObString &type_text, int32_t &length);
  static int get_local_session_vars(sql::ObSQLSessionInfo *session_info,
                                    ObIAllocator &allocator,
                                    common::sqlclient::dblink_param_ctx &param_ctx);
  static int get_set_sql_mode_cstr(sql::ObSQLSessionInfo *session_info,
                                   const char *&set_sql_mode_cstr,
                                   ObIAllocator &allocator);
  static int get_set_names_cstr(sql::ObSQLSessionInfo *session_info,
                                const char *&set_client_charset,
                                const char *&set_connection_charset,
                                const char *&set_results_charset);
  static int get_set_transaction_isolation_cstr(sql::ObSQLSessionInfo *session_info,
                                                const char *&set_isolation_level);
public:
  static const char *SET_ISOLATION_LEVEL_READ_COMMITTED;
  static const char *SET_ISOLATION_LEVEL_REPEATABLE_READ;
  static const char *SET_ISOLATION_LEVEL_SERIALIZABLE;
  static const char *SET_ISOLATION_LEVEL_READ_COMMITTED_MYSQL_MODE;
  static const char *SET_ISOLATION_LEVEL_REPEATABLE_READ_MYSQL_MODE;
  static const char *SET_ISOLATION_LEVEL_SERIALIZABLE_MYSQL_MODE;
  static const char *SET_RESULTS_CHARSET_NULL;
  static const char *SET_CLIENT_CHARSET_UTF8MB4;
  static const char *SET_CLIENT_CHARSET_GBK;
  static const char *SET_CLIENT_CHARSET_BINARY;
  static const char *SET_CONNECTION_CHARSET_UTF8MB4;
  static const char *SET_CONNECTION_CHARSET_GBK;
  static const char *SET_CONNECTION_CHARSET_BINARY;
  static const char *SET_RESULTS_CHARSET_UTF8MB4;
  static const char *SET_RESULTS_CHARSET_GBK;
  static const char *SET_RESULTS_CHARSET_BINARY;
};

enum DblinkGetConnType {
  DBLINK_POOL = 0,
  TEMP_CONN,
  TM_CONN
};

class ObReverseLink
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObReverseLink();
  virtual ~ObReverseLink();
  inline void set_user(ObString name) { user_ = name; }
  inline void set_tenant(ObString name) { tenant_ = name; }
  inline void set_cluster(ObString name) { cluster_ = name; }
  inline void set_passwd(ObString name) { passwd_ = name; }
  inline void set_addr(common::ObAddr addr) { addr_ = addr; }
  inline void set_self_addr(common::ObAddr addr) { self_addr_ = addr; }
  inline void set_tx_id(int64_t tx_id) { tx_id_ = tx_id; }
  inline void set_tm_sessid(uint32_t tm_sessid) { tm_sessid_ = tm_sessid; }
  inline void set_session_info(sql::ObSQLSessionInfo *session_info) { session_info_ = session_info; }
  const ObString &get_user() { return user_; }
  const ObString &get_tenant() { return tenant_; }
  const ObString &get_cluster() { return cluster_; }
  const ObString &get_passwd() { return passwd_; }
  const common::ObAddr &get_addr() { return addr_; }
  const common::ObAddr &get_self_addr() { return self_addr_; }
  int64_t get_tx_id() { return tx_id_; }
  uint32_t get_tm_sessid() { return tm_sessid_; }

  int open(int64_t session_sql_req_level);
  int read(const char *sql, ObISQLClient::ReadResult &res);
  int ping();
  int close();
  TO_STRING_KV(K_(user),
              K_(tenant),
              K_(cluster),
              K_(passwd),
              K_(addr),
              K_(self_addr),
              K_(tx_id),
              K_(tm_sessid),
              K_(is_close));
public:
  static const char *SESSION_VARIABLE;
  static const int64_t VARI_LENGTH;
  static const ObString SESSION_VARIABLE_STRING;
  static const int64_t LONG_QUERY_TIMEOUT;
private:
  common::ObArenaAllocator allocator_;
  ObString user_;
  ObString tenant_;
  ObString cluster_;
  ObString passwd_;
  common::ObAddr addr_; // for rm connect to tm
  common::ObAddr self_addr_; // for proxy to route reverse link sql
  int64_t tx_id_;
  uint32_t tm_sessid_;
  bool is_close_;
  common::sqlclient::ObMySQLConnection reverse_conn_; // ailing.lcq to do, ObReverseLink can be used by serval connection, not just one
  char db_user_[OB_MAX_USER_NAME_LENGTH + OB_MAX_TENANT_NAME_LENGTH + OB_MAX_CLUSTER_NAME_LENGTH];
  char db_pass_[OB_MAX_PASSWORD_LENGTH];
  sql::ObSQLSessionInfo *session_info_; // reverse link belongs to which session
};

class ObDblinkUtils
{
public:
  static int has_reverse_link_or_any_dblink(const ObDMLStmt *stmt, bool &has, bool enable_check_any_dblink = false);
  static int gather_dblink_id(const ObDMLStmt *stmt, ObIArray<int64_t> &dblink_id_array);
};

class ObSQLSessionInfo;

class ObDblinkCtxInSession
{
public:
  explicit ObDblinkCtxInSession(ObSQLSessionInfo *session_info)
    :
      session_info_(session_info),
      reverse_dblink_(NULL),
      reverse_dblink_buf_(NULL),
      sys_var_reverse_info_buf_(NULL),
      sys_var_reverse_info_buf_size_(0),
      tx_id_()
  {}
  ~ObDblinkCtxInSession()
  {
    reset();
  }
  inline void reset()
  {
    arena_alloc_.reset();
    const bool force_disconnect = true;
    clean_dblink_conn(force_disconnect);
    free_dblink_conn_pool();
    // session_info_ = NULL; // do not need reset session_info_
    reverse_dblink_ = NULL;
  }
  int register_dblink_conn_pool(common::sqlclient::ObCommonServerConnectionPool *dblink_conn_pool);
  int free_dblink_conn_pool();
  int get_dblink_conn(uint64_t dblink_id, common::sqlclient::ObISQLConnection *&dblink_conn, uint32_t tm_sessid = 0);
  static int revert_dblink_conn(common::sqlclient::ObISQLConnection *&dblink_conn);
  int set_dblink_conn(common::sqlclient::ObISQLConnection *dblink_conn);
  int clean_dblink_conn(const bool force_disconnect);
  inline bool is_dblink_xa_tras() { return !dblink_conn_holder_array_.empty(); }
  int get_reverse_link(ObReverseLink *&reverse_dblink);
  transaction::ObTransID &get_tx_id() { return tx_id_; }
private:
  ObSQLSessionInfo *session_info_;
  ObReverseLink *reverse_dblink_;
  void * reverse_dblink_buf_;
  void * sys_var_reverse_info_buf_;
  int64_t sys_var_reverse_info_buf_size_;
  common::ObArenaAllocator arena_alloc_;
  ObArray<common::sqlclient::ObCommonServerConnectionPool *> dblink_conn_pool_array_;  //for dblink read to free connection when session drop.
  ObArray<int64_t> dblink_conn_holder_array_; //for dblink write to hold connection during transaction.
  ObString last_reverse_info_values_;
  transaction::ObTransID tx_id_;
};

struct ObParamPosIdx
{
  OB_UNIS_VERSION_V(1);
public:
  ObParamPosIdx()
    : pos_(0),
      idx_(0),
      type_value_(0)
  {}
  ObParamPosIdx(int32_t pos, int32_t idx, int8_t type_value)
    : pos_(pos),
      idx_(idx),
      type_value_(type_value)
  {}
  virtual ~ObParamPosIdx()
  {}
  TO_STRING_KV(N_POS, pos_,
               N_IDX, idx_,
               N_TYPE_VALUE, type_value_);
  int32_t pos_;
  int32_t idx_;
  int8_t type_value_;
  /*
    if type_value_ = -1, means TimeOutHint, used in 3.x, unused in 4.x.
    if type_value_ >= int8_t(ObObjType::ObNullType) and type_value_ <= int8_t(ObObjType::ObMaxType), means the value of ObObjType.
    if type_value_ < -1 || type_value > int8_t(ObObjType::ObMaxType), means a invalid type_value_.
  */
};

class ObLinkStmtParam
{
public:
  static int write(char *buf, int64_t buf_len, int64_t &pos, int64_t param_idx, int8_t type_value = 0);
  static int read_next(const char *buf, int64_t buf_len, int64_t &pos, int64_t &param_idx, int8_t &type_value);
  static int64_t get_param_len();
private:
  static const int64_t PARAM_LEN;
};

} // end of namespace sql
} // end of namespace oceanbase

#endif // OCEANBASE_SQL_OB_DBLINK_UTILS_H
