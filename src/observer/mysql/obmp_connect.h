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

#ifndef _OBMP_CONNECT_H_
#define _OBMP_CONNECT_H_

#include "rpc/obmysql/packet/ompk_handshake_response.h"
#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace sql
{
class ObMultiStmtItem;
class ObSQLSessionInfo;
}
namespace observer
{
struct ObSMConnection;

ObString extract_user_name(const ObString &in);
int extract_user_tenant(const ObString &in, ObString &user_name, ObString &tenant_name);
int extract_tenant_id(const ObString &tenant_name, uint64_t &tenant_id);
class ObMPConnect
    : public ObMPBase
{
public:
  explicit ObMPConnect(const ObGlobalContext &gctx);
  virtual ~ObMPConnect();

protected:
  int process();
  int deserialize();

  int load_privilege_info(sql::ObSQLSessionInfo &session);

private:
  int get_tenant_id(ObSMConnection &conn, uint64_t &tenant_id);
  int64_t get_user_id();
  int64_t get_database_id();
  int get_conn_id(uint32_t &conn_id) const;
  int get_proxy_conn_id(uint64_t &conn_id) const;
  int get_proxy_sess_create_time(int64_t &sess_create_time) const;
  int get_proxy_capability(uint64_t &cap) const;
  int get_proxy_scramble(ObString &proxy_scramble) const;
  int get_client_ip(ObString &client_ip) const;

  int get_client_attribute_capability(uint64_t &cap) const;

  int get_user_tenant(ObSMConnection &conn);
  int extract_real_scramble(const ObString &proxy_scramble);

  int check_client_property(ObSMConnection &conn);
  int check_common_property(ObSMConnection &conn, obmysql::ObMySQLCapabilityFlags &client_cap);
  int check_update_proxy_capability(ObSMConnection &conn) const;
  int check_user_cluster(const ObString &server_cluster, const int64_t server_cluster_id) const;
  int init_process_single_stmt(const sql::ObMultiStmtItem &multi_stmt_item,
                               sql::ObSQLSessionInfo &session,
                               bool has_more_result) const;
  int init_connect_process(common::ObString &init_sql,
                           sql::ObSQLSessionInfo &session) const;
  int check_update_tenant_id(ObSMConnection &conn, uint64_t &tenant_id);
  int verify_connection(const uint64_t tenant_id) const;
  int verify_identify(ObSMConnection &conn, sql::ObSQLSessionInfo &session, const uint64_t tenant_id);
  int verify_ip_white_list(const uint64_t tenant_id) const;
  int convert_oracle_object_name(const uint64_t tenant_id, ObString &object_name);

  int switch_lock_status_for_current_login_user(const uint64_t tenant_id, bool do_lock);
  int switch_lock_status_for_user(const uint64_t tenant_id, const ObString &host_name,
                                  ObCompatibilityMode compat_mode, bool do_lock);
  int get_last_failed_login_info(const uint64_t tenant_id,
                                 const uint64_t user_id,
                                 ObISQLClient &sql_client,
                                 int64_t &current_failed_login_num,
                                 int64_t &last_failed_timestamp);

  int update_current_user_failed_login_num(const uint64_t tenant_id,
                                           const uint64_t user_id,
                                           ObISQLClient &sql_client,
                                           int64_t new_failed_login_num);
  int clear_current_user_failed_login_num(const uint64_t tenant_id,
                                          const uint64_t user_id,
                                          ObISQLClient &sql_client);

  int update_login_stat_in_trans(const uint64_t tenant_id,
                                 const bool is_login_succ,
                                 share::schema::ObSchemaGetterGuard &schema_guard);
  int update_login_stat_mysql(const uint64_t tenant_id,
                              const bool is_login_succ,
                              ObSchemaGetterGuard &schema_guard,
                              bool &is_unlocked_now);
  int update_login_stat_in_trans_mysql(const uint64_t tenant_id,
                                       const ObUserInfo &user_info,
                                       const bool is_login_succ,
                                       bool &is_locked_now);
  bool is_connection_control_enabled(const uint64_t tenant_id);
  int get_connection_control_stat(const uint64_t tenant_id, const int64_t current_failed_login_num,
                                  const int64_t last_failed_login_timestamp,
                                  bool &need_lock, bool &is_locked);

  int unlock_user_if_time_is_up(const uint64_t tenant_id,
                                share::schema::ObSchemaGetterGuard &schema_guard,
                                bool &is_unlock);
  int unlock_user_if_time_is_up_mysql(const uint64_t tenant_id,
                                      const uint64_t user_id,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      bool &is_unlock);
  int check_password_expired(const uint64_t tenant_id,
                             share::schema::ObSchemaGetterGuard &schema_guard,
                             sql::ObSQLSessionInfo &session);
#ifdef OB_BUILD_AUDIT_SECURITY
  int check_audit_user(const uint64_t tenant_id, ObString &user_name);
#endif

  int set_proxy_version(ObSMConnection &conn);
  int set_client_version(ObSMConnection &conn);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMPConnect);
  obmysql::OMPKHandshakeResponse hsr_;
  common::ObString user_name_;
  common::ObString client_ip_;
  common::ObString tenant_name_;
  common::ObString db_name_;
  char client_ip_buf_[common::MAX_IP_ADDR_LENGTH + 1];
  char user_name_var_[OB_MAX_USER_NAME_BUF_LENGTH];
  char db_name_var_[OB_MAX_DATABASE_NAME_BUF_LENGTH];
  int deser_ret_;
}; // end of class ObMPConnect

} // end of namespace observer
} // end of namespace oceanbase

#endif /* _OBMP_CONNECT_H_ */
