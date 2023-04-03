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
      user_vars_()
  {
  }

  virtual ~ObMPChangeUser() {}

protected:
  int process();
  int deserialize();
  int load_privilege_info(sql::ObSQLSessionInfo *session);

private:
  static int decode_string_kv(const char* attrs_end, const char *&pos, obmysql::ObStringKV &kv);
  int decode_session_vars(const char *&pos, const int64_t session_vars_len);
  int replace_user_variables(sql::ObBasicSessionInfo &session) const;
  int parse_var_node(const ParseNode *node, common::ObCastCtx &cast_ctx,
                     sql::ObBasicSessionInfo &session) const;
  int handle_user_var(const common::ObString &var, const common::ObString &val,
                      const common::ObObjType type, common::ObCastCtx &cast_ctx,
                      sql::ObBasicSessionInfo &session) const;

private:
  obmysql::ObMySQLRawPacket pkt_;
  common::ObString username_;
  common::ObString auth_response_;
  common::ObString auth_plugin_name_;
  common::ObString database_;
  uint16_t charset_;
  common::ObSEArray<obmysql::ObStringKV, 128> sys_vars_;
  common::ObSEArray<obmysql::ObStringKV, 16> user_vars_;
  DISALLOW_COPY_AND_ASSIGN(ObMPChangeUser);
};// end of class

} // end of namespace observer
} // end of namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OBMP_CHANGE_USER */
