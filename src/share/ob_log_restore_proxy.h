/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_LOG_RESTORE_PROXY_H_
#define OCEANBASE_SHARE_OB_LOG_RESTORE_PROXY_H_

#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_server_provider.h"
#include "lib/mysqlclient/ob_single_mysql_connection_pool.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_define.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "share/ob_ls_id.h"
#include "share/ob_tenant_role.h"
#include "share/ob_root_addr_agent.h"//ObRootAddr
#include "share/schema/ob_schema_struct.h"
#include "logservice/palf/palf_options.h"
#include <cstdint>

namespace oceanbase
{
namespace share
{
class ObLogRestoreMySQLProvider : public common::sqlclient::ObMySQLServerProvider
{
public:
  ObLogRestoreMySQLProvider();
  virtual ~ObLogRestoreMySQLProvider();

  int init(const common::ObIArray<common::ObAddr> &server_list);
  void destroy();
  int set_restore_source_server(const common::ObIArray<common::ObAddr> &server_list);
  virtual int get_server(const int64_t svr_idx, common::ObAddr &server) override;
  virtual int64_t get_server_count() const override;
  virtual int get_tenant_ids(ObIArray<uint64_t> &tenant_ids) override;
  virtual int get_tenant_servers(const uint64_t tenant_id, ObIArray<ObAddr> &tenant_servers) override;
  int refresh_server_list(void) override;
  int prepare_refresh() override;
  int end_refresh() override;

private:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard  RLockGuard;
  typedef common::SpinWLockGuard  WLockGuard;

private:
  common::ObArray<common::ObAddr> server_list_;
  mutable RWLock lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreMySQLProvider);
};

class ObLogRestoreConnectionPool : public common::sqlclient::ObMySQLConnectionPool
{
public:
  ObLogRestoreConnectionPool() {}
  virtual ~ObLogRestoreConnectionPool() {}

  int init(const common::ObIArray<common::ObAddr> &server_list,
      const char *user_name,
      const char *user_password,
      const char *db_name);

  void destroy();

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreConnectionPool);
};

// This proxy util supports both Oracle and MySQL tenant, with obclient library.
// And it builds SQL connection when it is inited and the connection will be destroyed when the proxy destructs.
// It also provides refresh_conn interface, to refresh connect user info and server list.
//
// Note: for Oracle tenant, read column is covertted to upper letter implicitly,
// while the column maintains the origin for Mysql tenant, and upper and lower column are different.
//
// eg. 1) select TENANT_ID from DBA_OB_TENANTS;
//     2) select tenant_id from DBA_OB_TENANTS;
// Oracle tenant only support 1) to extract results; while MySQL tenant supports both, but extract results only with the same column value.
//
class ObLogRestoreProxyUtil
{
public:
  ObLogRestoreProxyUtil();
  ~ObLogRestoreProxyUtil();
public:
  // Proxy init, SQL connection will be built with an appropriate server
  //
  // @param[in] tenant_id, user tenant_id
  // @param[in] addr_array, a candidate server list, proxy will choose an available server to build SQL connection
  // @param[in] user_name, the uer_name to build SQL connection, its format 'sql_user@tenant_name'
  // @param[in] user_password, the user_password to build SQL connection
  // @param[in] db_name, the database name to connection. For example, to query inner tables,
  //            for Oracle tenant, it is 'SYS'; for MySQL tenant, it is 'OCEANBASE'.
  // @param[in] connect_timeout_sec, the timeout threshold to build connection
  // @param[in] query_timeout_sec, the timeout threshold to excute query
  int init(const uint64_t tenant_id,
      const common::ObIArray<common::ObAddr> &addr_array,
      const char *user_name,
      const char *user_password,
      const char *db_name);

  // destroy proxy, close all connections
  void destroy();

  bool is_inited() const { return inited_; }

  int refresh_conn(const common::ObIArray<common::ObAddr> &addr_array,
      const char *user_name,
      const char *user_password,
      const char *db_name);

  // get proxy for common usages
  int get_sql_proxy(common::ObMySQLProxy *&proxy);

  // interfaces for log restore specific usage
  int try_init(const uint64_t tenant_id,
      const common::ObIArray<common::ObAddr> &addr_array,
      const char *user_name,
      const char *user_password);

  // get log restore source tenant_id
  int get_tenant_id(char *tenant_name, uint64_t &tenant_id);
  // get log restore source cluster_id
  int get_cluster_id(uint64_t tenant_id, int64_t &cluster_id);
  // get log restore source tenant_mode, oracle or mysql
  int get_compatibility_mode(const uint64_t tenant_id, ObCompatibilityMode &compat_mode);
  // get log restore source tenant access point
  int get_server_ip_list(const uint64_t tenant_id, common::ObArray<common::ObAddr> &addrs);
  //get tenant server ip and prot
  //param[in] tenant_id : primary tenant_id
  int get_server_addr(const uint64_t tenant_id, common::ObIArray<common::ObAddr> &addrs);
  int check_begin_lsn(const uint64_t tenant_id);
  // get log restore source tenant info, includes tenant role and tennat status
  int get_tenant_info(ObTenantRole &role, schema::ObTenantStatus &status);
  // get the access_mode and max_scn of the specific LS in log restore source tenant
  int get_max_log_info(const ObLSID &id, palf::AccessMode &mode, SCN &scn);
  // get ls from dba_ob_ls
  int is_ls_existing(const ObLSID &id);
private:
  // check if user or password changed
  bool is_user_changed_(const char *user_name, const char *user_password);
  void destroy_tg_();
  int detect_tenant_mode_(common::sqlclient::ObMySQLServerProvider *server_provider, const char *user_name, const char *user_password);
private:
  int construct_server_ip_list(const common::ObSqlString &sql, common::ObIArray<common::ObAddr> &addrs);
  bool inited_;
  uint64_t tenant_id_;
  int tg_id_;
  ObLogRestoreMySQLProvider server_prover_;
  ObLogRestoreConnectionPool connection_;
  common::ObFixedLengthString<common::OB_MAX_USER_NAME_BUF_LENGTH> user_name_;
  common::ObFixedLengthString<common::OB_MAX_PASSWORD_LENGTH + 1> user_password_;
  common::ObMySQLProxy sql_proxy_;
  bool is_oracle_mode_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreProxyUtil);
};


} // namespace share
} // namespace oceanbase

#endif /* OCEANBASE_SHARE_OB_LOG_RESTORE_PROXY_H_ */
