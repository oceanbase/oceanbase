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

#ifndef __COMMON_OB_SERVER_CONNECTION_POOL__
#define __COMMON_OB_SERVER_CONNECTION_POOL__

#include <mariadb/mysql.h>
#include "lib/lock/ob_spin_lock.h"
#include "lib/mysqlclient/ob_connection_allocator.h"
#include "lib/mysqlclient/ob_mysql_connection.h"

namespace oceanbase {
namespace common {
namespace sqlclient {
class ObMySQLConnectionPool;
class ObServerConnectionPool {
public:
  ObServerConnectionPool();
  ~ObServerConnectionPool();
  int acquire(ObMySQLConnection*& connection);
  int release(ObMySQLConnection* connection, const bool succ);
  uint64_t get_busy_count(void) const;

public:
  int init(ObMySQLConnectionPool* root, const common::ObAddr& server, int64_t max_allowed_conn_count);
  int destroy();
  void reset();
  void renew();
  int64_t last_renew_time(void) const;
  void set_server_gone(bool gone);
  const char* get_db_user() const;
  const char* get_db_pass() const;
  const char* get_db_name() const;
  common::ObAddr& get_server();
  ObMySQLConnectionPool* get_root();
  void close_all_connection();
  void dump();
  TO_STRING_KV(K_(free_conn_count), K_(busy_conn_count));
  // dblink.
  int init_dblink(uint64_t dblink_id, const ObAddr& server, const ObString& db_tenant, const ObString& db_user,
      const ObString& db_pass, const ObString& db_name, ObMySQLConnectionPool* root, int64_t max_allowed_conn_count);
  uint64_t get_dblink_id() const;

private:
  volatile uint64_t free_conn_count_;
  volatile uint64_t busy_conn_count_;
  ObConnectionAllocator<ObMySQLConnection> connection_pool_;
  ObMySQLConnectionPool* root_;
  uint64_t dblink_id_;
  char db_user_[OB_MAX_USER_NAME_LENGTH + OB_MAX_TENANT_NAME_LENGTH + 1];
  char db_pass_[OB_MAX_PASSWORD_LENGTH];
  char db_name_[OB_MAX_DATABASE_NAME_LENGTH];
  common::ObAddr server_;  // shared by connections in this pool
  common::ObSpinLock pool_lock_;
  int64_t last_renew_timestamp_;
  int64_t connection_version_;
  uint64_t max_allowed_conn_count_;
  bool server_not_available_;
};

inline void ObServerConnectionPool::set_server_gone(bool gone)
{
  server_not_available_ = gone;
}

inline const char* ObServerConnectionPool::get_db_user() const
{
  return db_user_;
}

inline const char* ObServerConnectionPool::get_db_pass() const
{
  return db_pass_;
}
inline const char* ObServerConnectionPool::get_db_name() const
{
  return db_name_;
}

inline void ObServerConnectionPool::renew()
{
  server_not_available_ = false;
  last_renew_timestamp_ = ::oceanbase::common::ObTimeUtility::current_time();
}
inline int64_t ObServerConnectionPool::last_renew_time(void) const
{
  return last_renew_timestamp_;
}
inline common::ObAddr& ObServerConnectionPool::get_server()
{
  return server_;
}
inline ObMySQLConnectionPool* ObServerConnectionPool::get_root()
{
  return root_;
}

inline uint64_t ObServerConnectionPool::get_dblink_id() const
{
  return dblink_id_;
}

}  // namespace sqlclient
}  // namespace common
}  // namespace oceanbase

#endif  // __COMMON_OB_SERVER_CONNECTION_POOL__
