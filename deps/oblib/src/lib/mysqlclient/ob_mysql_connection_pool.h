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

#ifndef __COMMON_OB_MYSQL_CONNECTION_POOL__
#define __COMMON_OB_MYSQL_CONNECTION_POOL__

#include <mysql.h>
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_link_hashmap.h"                   // ObLinkHashMap
#include "lib/task/ob_timer.h"
#include "lib/list/ob_list.h"
#include "lib/allocator/ob_cached_allocator.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/net/ob_addr.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#ifdef OB_BUILD_DBLINK
#include "lib/oracleclient/ob_oracle_oci_connection.h"
#endif

namespace oceanbase
{
namespace common
{

class ObMySQLProxy;
class ObMySQLProxyUtil;
class ObMySQLTransaction;
class ObCommonMySQLProvider;
namespace sqlclient
{
class ObServerConnectionPool;
class ObMySQLServerProvider;
enum MySQLConnectionPoolType
{
  SERVER_POOL = 0,
  TENANT_POOL
};
// Tenant level Server Connection Pool
// hold pointer list of server_conn_pool and provide ServerPool by round-robin
struct TenantMapKey
{
  uint64_t tenant_id_;

  TenantMapKey() : tenant_id_(oceanbase::common::OB_INVALID_TENANT_ID) {}
  TenantMapKey(const uint64_t tenant_id) : tenant_id_(tenant_id) {}
  ~TenantMapKey() { reset(); }
  inline void reset() { tenant_id_ = oceanbase::common::OB_INVALID_TENANT_ID; }

  inline int64_t hash() const
  {
    return static_cast<int64_t>(tenant_id_);
  }
  inline int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  inline int compare(const TenantMapKey &other) const {
    int cmp_ret = 0;

    if (tenant_id_ > other.tenant_id_) {
      cmp_ret = 1;
    } else if (tenant_id_ < other.tenant_id_) {
      cmp_ret = -1;
    } else {
      cmp_ret = 0;
    }

    return cmp_ret;
  }

  TO_STRING_KV(K_(tenant_id));
};

typedef common::ObSEArray<ObServerConnectionPool *, 16> TenantServerConnArray;
class ObTenantServerConnectionPool : public LinkHashValue<TenantMapKey>
{
public:
  ObTenantServerConnectionPool();
  ~ObTenantServerConnectionPool();
  void reset();
public:
  int get_server_pool(ObServerConnectionPool *&server_pool);
  int renew(const TenantServerConnArray &new_server_conn_pool_list);
  TO_STRING_KV(K_(cursor), "tenant_server_cnt", server_pool_list_.count());
private:
  int64_t                 cursor_;
  TenantServerConnArray   server_pool_list_;
};

typedef common::ObList<ObServerConnectionPool *, common::ObArenaAllocator> ServerList;
typedef common::ObLinkHashMap<TenantMapKey, ObTenantServerConnectionPool> TenantServerConnMap;
class ObMySQLConnectionPool : public common::ObTimerTask, public ObISQLConnectionPool
{
public:
  friend class common::ObMySQLProxy;
  friend class common::ObMySQLProxyUtil;
  friend class common::ObMySQLTransaction;

  static const char *const DEFAULT_DB_USER;
  static const char *const DEFAULT_DB_PASS;
  static const char *const DEFAULT_DB_NAME;
  static const int64_t DEFAULT_TRANSACTION_TIMEOUT_US = 100 * 1000 * 1000;
public:
  ObMySQLConnectionPool();
  ~ObMySQLConnectionPool();

  void set_server_provider(ObMySQLServerProvider *provider);
  void update_config(const ObConnPoolConfigParam &config) { config_ = config; }
  const ObConnPoolConfigParam &get_config() const { return config_; }
  int set_db_param(const char *db_user = DEFAULT_DB_USER,
                    const char *db_pass = DEFAULT_DB_PASS, const char *db_name = DEFAULT_DB_NAME);
  int set_db_param(const ObString &db_user, const ObString &db_pass,
                    const ObString &db_name);
  int start(int tg_id);
  void stop();
  void signal_refresh();
  void close_all_connection();
  bool is_updated() const { return is_updated_; }
  bool is_use_ssl() const { return is_use_ssl_; }
  void disable_ssl() { is_use_ssl_ = false; }
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "connection pool task");
    return pos;
  }
  int64_t get_server_count() const;

  virtual int escape(const char *from, const int64_t from_size,
      char *to, const int64_t to_size, int64_t &out_size);

  virtual int acquire(const uint64_t tenant_id, ObISQLConnection *&conn, ObISQLClient *client_addr, const int32_t group_id) override;
  virtual int release(ObISQLConnection *conn, const bool success) override;

  virtual int on_client_inactive(ObISQLClient *client_addr) override
  {
    UNUSED(client_addr);
    return OB_SUCCESS;
  }
  virtual ObSQLConnPoolType get_type() override { return MYSQL_POOL; }

  void set_mode(const ObMySQLConnection::Mode mode) { mode_ = mode; }
  ObMySQLConnection::Mode get_mode() const { return mode_; }
  void set_pool_type(const MySQLConnectionPoolType pool_type) { pool_type_ = pool_type; }
  MySQLConnectionPoolType get_pool_type() const { return pool_type_; }
  const char* get_db_name() const { return db_name_; }
  const char* get_user_name() const { return db_user_; }
  virtual DblinkDriverProto get_pool_link_driver_proto() { return DBLINK_DRV_OB; }

  // dblink
  virtual int create_dblink_pool(const dblink_param_ctx &param_ctx, const ObAddr &server,
                                 const ObString &db_tenant, const ObString &db_user,
                                 const ObString &db_pass, const ObString &db_name,
                                 const common::ObString &conn_str,
                                 const common::ObString &cluster_str);
  virtual int acquire_dblink(const dblink_param_ctx &param_ctx, ObISQLConnection *&dblink_conn);
  virtual int release_dblink(ObISQLConnection *dblink_conn);
  virtual int do_acquire_dblink(const dblink_param_ctx &param_ctx, ObISQLConnection *&dblink_conn);
  virtual int try_connect_dblink(ObISQLConnection *dblink_conn, int64_t sql_request_level = 0);
  int get_dblink_pool(const dblink_param_ctx &param_ctx, ObServerConnectionPool *&dblink_pool);
  void set_check_read_consistency(bool need_check) { check_read_consistency_ = need_check; }
  virtual int clean_dblink_connection(uint64_t tenant_id);
protected:
  // update interval.
  // update ms list in backgroud thread and
  // recycle not-in-use unavaliable ms connections
  //virtual void run(int64_ts);
  virtual void runTimerTask();
  int create_server_connection_pool(const common::ObAddr &server);

  virtual int acquire(const uint64_t tenant_id, ObMySQLConnection *&connection);
  int do_acquire(const uint64_t tenant_id, ObMySQLConnection *&connection);


protected:
  int execute_init_sql(ObMySQLConnection *connection);
  int try_connect(ObMySQLConnection *connection);
  int release(ObMySQLConnection *connection, const bool succ);
  int get_pool(const uint64_t tenant_id, ObServerConnectionPool *&pool);
  int get_tenant_server_pool(const uint64_t tenant_id, ObTenantServerConnectionPool *&tenant_server_pool);
  int purge_connection_pool();
  void mark_all_server_connection_gone();
  int renew_server_connection_pool(common::ObAddr &server);
  int renew_tenant_server_pool_map();
private:
  struct TenantServerConnPoolPurger
  {
    TenantServerConnPoolPurger(
        const ObIArray<uint64_t> &tenant_array,
        oceanbase::common::ObArenaAllocator &allocator)
      : tenant_array_(tenant_array),  purge_count_(0), allocator_(allocator) {}
    ~TenantServerConnPoolPurger() { purge_count_ = 0; }

    const ObIArray<uint64_t> &tenant_array_;
    int64_t purge_count_;
    oceanbase::common::ObArenaAllocator &allocator_; // use to free ObTenantServerConnectionPool

    bool operator()(const TenantMapKey &tenant_key, const ObTenantServerConnectionPool *tenant_server_pool);
    // tenant not serve if tenant_arr is not empty and tenant_id not in tenant_array_.
    bool is_tenant_not_serve_(const uint64_t tenant_id) const;
  };
private:
  int renew_tenant_server_pool_(const uint64_t tenant_id);
  int get_server_pool_(const ObAddr &addr, ObServerConnectionPool *&pool);
  int purge_tenant_server_pool_map_(const ObIArray<uint64_t> &tenant_array);
protected:
  static const int MAX_SERVER_GONE_INTERVAL = 1000 * 1000 * 1; // 1 sec

  bool is_updated_;
  bool is_stop_;
  bool is_use_ssl_;
  ObMySQLConnection::Mode mode_;
  MySQLConnectionPoolType pool_type_;

  int tg_id_;
  ObMySQLServerProvider *server_provider_;
  volatile int64_t busy_conn_count_;

  // user name or password or db maybe modify, add user_info_lock_ to protect user info
  mutable obsys::ObRWLock user_info_lock_;
  char db_user_[OB_MAX_USER_NAME_BUF_LENGTH];
  char db_pass_[OB_MAX_PASSWORD_BUF_LENGTH];
  char db_name_[OB_MAX_DATABASE_NAME_BUF_LENGTH];
  char init_sql_[OB_MAX_SQL_LENGTH];
  ObConnPoolConfigParam config_;
  mutable obsys::ObRWLock get_lock_;
  // ObMySQLConnectionPool::do_acquire use obsys::ObRLockGuard lock(get_lock_)
  // ObMySQLConnectionPool::create_dblink_pool need obsys::ObWLockGuard lock(get_lock_)
  // do_acquire call create_dblink_pool
  // will leading to dead lock
  // need one more lock for create_dblink_pool
  mutable obsys::ObRWLock dblink_pool_lock_;
  common::ObArenaAllocator allocator_;
  ServerList server_list_;
  TenantServerConnMap tenant_server_pool_map_;
  common::ObCachedAllocator<ObServerConnectionPool> server_pool_;
  bool check_read_consistency_;
};

class ObDbLinkConnectionPool
{
  // dblink.
public:
  ObDbLinkConnectionPool() : link_type_(DBLINK_DRV_OB) {}
  virtual ~ObDbLinkConnectionPool() {}

  inline void set_link_type(DblinkDriverProto link_type) { link_type_= link_type; }
  DblinkDriverProto &get_link_type() { return link_type_; }
  ObMySQLConnectionPool &get_mysql_pool() { return mysql_pool_; }
#ifdef OB_BUILD_DBLINK
  ObOciConnectionPool &get_oci_pool() { return oci_pool_; }
#endif
  inline void stop() {}

private:
  // ObISQLConnectionPool &get_pool_from_type(DblinkDriverProto &link_type);
  // ObDbLinkMySQLConnectionPool mysql_pool_;
  ObMySQLConnectionPool mysql_pool_;
#ifdef OB_BUILD_DBLINK
  ObOciConnectionPool oci_pool_;
#endif
  DblinkDriverProto link_type_;
};

}
}
}

#endif // __COMMON_OB_MYSQL_CONNECTION__
