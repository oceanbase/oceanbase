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

#include <mariadb/mysql.h>
#include "lib/container/ob_se_array.h"
#include "lib/task/ob_timer.h"
#include "lib/list/ob_list.h"
#include "lib/allocator/ob_cached_allocator.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/net/ob_addr.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"

namespace oceanbase {
namespace common {

struct ObConnPoolConfigParam {
  ObConnPoolConfigParam()
  {
    reset();
  }
  ~ObConnPoolConfigParam()
  {}
  void reset()
  {
    memset(this, 0, sizeof(ObConnPoolConfigParam));
  }

  int64_t sqlclient_wait_timeout_;       // s
  int64_t connection_refresh_interval_;  // us
  int64_t connection_pool_warn_time_;    // us
  int64_t long_query_timeout_;           // us
  int64_t sqlclient_per_observer_conn_limit_;
};

class ObMySQLProxy;
class ObMySQLProxyUtil;
class ObMySQLTransaction;
class ObCommonMySQLProvider;
namespace sqlclient {
class ObServerConnectionPool;
class ObMySQLServerProvider;
class ObMySQLConnectionPool : public common::ObTimerTask, public ObISQLConnectionPool {
public:
  friend class common::ObMySQLProxy;
  friend class common::ObMySQLProxyUtil;
  friend class common::ObMySQLTransaction;

  static const char* const DEFAULT_DB_USER;
  static const char* const DEFAULT_DB_PASS;
  static const char* const DEFAULT_DB_NAME;
  static const int64_t DEFAULT_TRANSACTION_TIMEOUT_US = 100 * 1000 * 1000;
  typedef common::ObList<ObServerConnectionPool*, common::ObArenaAllocator> ServerList;
  class ClusterServerList {
  public:
    ClusterServerList() = delete;
    ClusterServerList(common::ObArenaAllocator& allocator) : cluster_id_(common::OB_INVALID_ID), server_list_(allocator)
    {}
    virtual ~ClusterServerList()
    {}
    void set_cluster_id(const int64_t cluster_id)
    {
      cluster_id_ = cluster_id;
    }
    int64_t get_cluster_id() const
    {
      return cluster_id_;
    }
    ServerList& get_server_list()
    {
      return server_list_;
    }

  private:
    int64_t cluster_id_;
    ServerList server_list_;
    DISALLOW_COPY_AND_ASSIGN(ClusterServerList);
  };
  typedef common::ObList<ClusterServerList*, common::ObArenaAllocator> ClusterList;

public:
  ObMySQLConnectionPool();
  ~ObMySQLConnectionPool();

  void set_server_provider(ObMySQLServerProvider* provider);
  void update_config(const ObConnPoolConfigParam& config)
  {
    config_ = config;
  }
  const ObConnPoolConfigParam& get_config() const
  {
    return config_;
  }
  int set_db_param(const char* db_user = DEFAULT_DB_USER, const char* db_pass = DEFAULT_DB_PASS,
      const char* db_name = DEFAULT_DB_NAME);
  int set_db_param(const ObString& db_user, const ObString& db_pass, const ObString& db_name);
  int start(int tg_id);
  void stop();
  void signal_refresh();
  void close_all_connection();
  bool is_updated() const
  {
    return is_updated_;
  }
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "connection pool task");
    return pos;
  }
  int64_t get_server_count(const int64_t cluster_id) const;

  virtual int escape(const char* from, const int64_t from_size, char* to, const int64_t to_size, int64_t& out_size);

  virtual int acquire(ObISQLConnection*& conn, ObISQLClient* client_addr) override;
  virtual int acquire(ObISQLConnection*& conn, const int64_t cluster_id, ObISQLClient* client_addr) override;
  virtual int release(ObISQLConnection* conn, const bool success) override;

  virtual int on_client_inactive(ObISQLClient* client_addr) override
  {
    UNUSED(client_addr);
    return OB_SUCCESS;
  }
  virtual ObSQLConnPoolType get_type() override
  {
    return MYSQL_POOL;
  }

  void set_mode(const ObMySQLConnection::Mode mode)
  {
    mode_ = mode;
  }
  ObMySQLConnection::Mode get_mode() const
  {
    return mode_;
  }
  const char* get_db_name() const
  {
    return db_name_;
  }
  const char* get_user_name() const
  {
    return db_user_;
  }

protected:
  // update interval.
  // update ms list in backgroud thread and
  // recycle not-in-use unavaliable ms connections
  // virtual void run(int64_ts);
  virtual void runTimerTask();
  int create_server_connection_pool(const int64_t cluster_id, const common::ObAddr& server);

  virtual int acquire(const int64_t cluster_id, ObMySQLConnection*& connection);
  int do_acquire(const int64_t cluster_id, ObMySQLConnection*& connection);
  int execute_init_sql(ObMySQLConnection* connection);
  int try_connect(ObMySQLConnection* connection);
  int release(ObMySQLConnection* connection, const bool succ);
  int get_pool(const int64_t cluster_id, ObServerConnectionPool*& pool);
  int purge_connection_pool();
  void mark_all_server_connection_gone();
  int renew_server_connection_pool(const int64_t cluster_id, common::ObAddr& server);
  int get_cluster_server_list_(const int64_t cluster_id, ClusterServerList*& cluster_server_list) const;

protected:
  static const int64_t OB_MAX_PASS_WORD_LENGTH = 64;
  static const int MAX_SERVER_GONE_INTERVAL = 1000 * 1000 * 1;  // 1 sec

  bool is_updated_;
  bool is_stop_;
  ObMySQLConnection::Mode mode_;

  int tg_id_;
  ObMySQLServerProvider* server_provider_;
  volatile int64_t busy_conn_count_;

  char db_user_[OB_MAX_USER_NAME_BUF_LENGTH];
  char db_pass_[OB_MAX_PASS_WORD_LENGTH];
  char db_name_[OB_MAX_DATABASE_NAME_BUF_LENGTH];
  char init_sql_[OB_MAX_SQL_LENGTH];
  ObConnPoolConfigParam config_;
  mutable obsys::CRWLock get_lock_;
  common::ObArenaAllocator allocator_;
  ClusterList cluster_list_;
  common::ObCachedAllocator<ObServerConnectionPool> server_pool_;
};

class ObDbLinkConnectionPool : public ObMySQLConnectionPool {
  // dblink.
public:
  int create_dblink_pool(uint64_t dblink_id, const ObAddr& server, const ObString& db_tenant, const ObString& db_user,
      const ObString& db_pass, const ObString& db_name);
  int acquire_dblink(uint64_t dblink_id, ObMySQLConnection*& dblink_conn);
  int release_dblink(ObMySQLConnection* dblink_conn);

private:
  int get_dblink_pool(uint64_t dblink_id, ObServerConnectionPool*& dblink_pool);
  int do_acquire_dblink(uint64_t dblink_id, ObMySQLConnection*& dblink_conn);
  int try_connect_dblink(ObMySQLConnection* dblink_conn);
};

}  // namespace sqlclient
}  // namespace common
}  // namespace oceanbase

#endif  // __COMMON_OB_MYSQL_CONNECTION__
