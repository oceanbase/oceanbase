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

#ifndef OCEANBASE_OBSERVER_OBMP_CHANGE_USER
#define OCEANBASE_OBSERVER_OBMP_CHANGE_USER

#include "lib/string/ob_string.h"
#include "observer/mysql/obmp_base.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "sql/parser/parse_node.h"
namespace oceanbase
{
namespace sql
{
class ObBasicSessionInfo;
}
namespace observer
{
class ObMPChangeUser : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_CHANGE_USER;
  explicit ObMPChangeUser(const ObGlobalContext &gctx)
      :ObMPBase(gctx),
      pkt_(),
      username_(),
      auth_response_(),
      auth_plugin_name_(),
      database_(),
      charset_(0),
      sys_vars_(),
      user_vars_(),
      connect_attrs_(),
      client_ip_(),
      client_port_(0)
  {
    client_ip_buf_[0] = '\0';
  }

  virtual ~ObMPChangeUser() {}

protected:
  int process();
  int deserialize();
  int load_login_info(sql::ObSQLSessionInfo *session);
  int update_conn_attrs(ObSMConnection *conn, sql::ObSQLSessionInfo *session);
  int set_service_name(const uint64_t tenant_id, sql::ObSQLSessionInfo &session,
    const ObString &service_name, const bool failover_mode);
private:
  static int decode_string_kv(const char* attrs_end, const char *&pos, obmysql::ObStringKV &kv);
  int decode_session_vars(const char *&pos, const int64_t session_vars_len);
  int replace_user_variables(sql::ObBasicSessionInfo &session) const;
  int parse_var_node(const ParseNode *node, common::ObCastCtx &cast_ctx,
                     sql::ObBasicSessionInfo &session) const;
  int handle_user_var(const common::ObString &var, const common::ObString &val,
                      const common::ObObjType type, common::ObCastCtx &cast_ctx,
                      sql::ObBasicSessionInfo &session) const;
  int verify_connection(const uint64_t tenant_id) const;
  int verify_ip_white_list(const uint64_t tenant_id) const;
  int reset_session_for_change_user(sql::ObSQLSessionInfo *session);

private:
  obmysql::ObMySQLRawPacket pkt_;
  common::ObString username_;
  common::ObString auth_response_;
  common::ObString auth_plugin_name_;
  common::ObString database_;
  uint16_t charset_;
  common::ObSEArray<obmysql::ObStringKV, 128> sys_vars_;
  common::ObSEArray<obmysql::ObStringKV, 16> user_vars_;
  common::ObSEArray<obmysql::ObStringKV, 8> connect_attrs_;
  common::ObString client_ip_;
  char client_ip_buf_[common::MAX_IP_ADDR_LENGTH + 1];
  int32_t client_port_;
  bool has_proxy_connection_id_key_ = false;
  DISALLOW_COPY_AND_ASSIGN(ObMPChangeUser);
};// end of class

} // end of namespace observer
} // end of namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OBMP_CHANGE_USER */
