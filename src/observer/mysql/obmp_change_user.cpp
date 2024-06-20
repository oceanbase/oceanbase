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

#define USING_LOG_PREFIX SERVER
#include "observer/mysql/obmp_change_user.h"
#include "observer/mysql/obmp_utils.h"
#include "lib/string/ob_sql_string.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "sql/ob_sql.h"
#include "sql/ob_end_trans_callback.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_user_resource_mgr.h"
#include "sql/parser/ob_parser.h"
#include "sql/parser/ob_parser_utils.h"
#include "rpc/obmysql/obsm_struct.h"
#include "rpc/obmysql/packet/ompk_auth_switch.h"


using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obmysql;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{
const char *AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD = "mysql_native_password";
int ObMPChangeUser::deserialize()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req_) || OB_UNLIKELY(ObRequest::OB_MYSQL != req_->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid request", K(req_));
  } else {
    ObSQLSessionInfo *session = NULL;
    ObMySQLCapabilityFlags capability;
    if (OB_FAIL(get_session(session))) {
      LOG_WARN("get session  fail", K(ret));
    } else if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get session info", K(ret), K(session));
    } else {
      ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
      session->update_last_active_time();
      capability = session->get_capability();
    }
    if (NULL != session) {
      revert_session(session);
    }
    if (OB_SUCC(ret)) {
      pkt_  = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
      const char *buf = pkt_.get_cdata();
      const char *pos = pkt_.get_cdata();
      // need skip command byte
      const int64_t len = pkt_.get_clen() - 1;
      const char *end = buf + len;

      if (OB_LIKELY(pos < end)) {
        username_.assign_ptr(pos, static_cast<int32_t>(STRLEN(pos)));
        pos += username_.length() + 1;
      }

      if (OB_LIKELY(pos < end)) {
        if (capability.cap_flags_.OB_CLIENT_SECURE_CONNECTION) {
          uint8_t auth_response_len = 0;
          ObMySQLUtil::get_uint1(pos, auth_response_len);
          auth_response_.assign_ptr(pos, static_cast<int32_t>(auth_response_len));
          pos += auth_response_len;
        } else {
          auth_response_.assign_ptr(pos, static_cast<int32_t>(STRLEN(pos)));
          pos += auth_response_.length() + 1;
        }
      }

      if (OB_LIKELY(pos < end)) {
        database_.assign_ptr(pos, static_cast<int32_t>(STRLEN(pos)));
        pos += database_.length() + 1;
      }

      if (OB_LIKELY(pos < end)) {
        ObMySQLUtil::get_uint2(pos, charset_);
      }

      if (OB_LIKELY(pos < end)) {
        if (capability.cap_flags_.OB_CLIENT_PLUGIN_AUTH) {
          auth_plugin_name_.assign_ptr(pos, static_cast<int32_t>(STRLEN(pos)));
          pos += auth_plugin_name_.length() + 1;
        }
      }

      if (OB_LIKELY(pos < end)) {
        if (capability.cap_flags_.OB_CLIENT_CONNECT_ATTRS) {
          uint64_t all_attrs_len = 0;
          const char *attrs_end = NULL;
          if (OB_FAIL(ObMySQLUtil::get_length(pos, all_attrs_len))) {
            LOG_WARN("fail to get all_attrs_len", K(ret));
          } else {
            attrs_end = pos + all_attrs_len;
          }
          ObStringKV str_kv;
          while(OB_SUCC(ret) && OB_LIKELY(pos < attrs_end)) {
            if (OB_FAIL(decode_string_kv(attrs_end, pos, str_kv))) {
              OB_LOG(WARN, "fail to decode string kv", K(ret));
            } else {
              if (str_kv.key_ == OB_MYSQL_PROXY_SESSION_VARS) {
                const char *vars_start = str_kv.value_.ptr();
                if (OB_FAIL(decode_session_vars(vars_start, str_kv.value_.length()))) {
                  OB_LOG(WARN, "fail to decode session vars", K(ret));
                }
              } else {
                //do not save it
              }
            }
          }
        } // end connect attrs
      } // end if
    }
  }
  return ret;
}

int ObMPChangeUser::decode_string_kv(const char *attrs_end, const char *&pos, ObStringKV &kv)
{
  int ret = OB_SUCCESS;
  uint64_t key_len = 0;
  uint64_t value_len = 0;
  if (OB_ISNULL(pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalie input value", K(pos), K(ret));
  } else {
    if (OB_FAIL(ObMySQLUtil::get_length(pos, key_len))) {
      OB_LOG(WARN, "fail t get key len", K(pos), K(ret));
    } else if (pos + key_len >= attrs_end) {
      // skip this value
      pos = attrs_end;
    } else {
      kv.key_.assign_ptr(pos, static_cast<uint32_t>(key_len));
      pos += key_len;
      if (OB_FAIL(ObMySQLUtil::get_length(pos, value_len))) {
        OB_LOG(WARN, "fail t get value len", K(pos), K(ret));
      } else {
        kv.value_.assign_ptr(pos, static_cast<uint32_t>(value_len));
        pos += value_len;
      }
    }

  }
  return ret;
}

int ObMPChangeUser::decode_session_vars(const char *&pos, const int64_t session_vars_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pos) || OB_UNLIKELY(session_vars_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalie input value", K(pos), K(session_vars_len), K(ret));
  } else{
    const char *end = pos + session_vars_len;
    bool found_separator = false;
    ObStringKV tmp_kv;
    while (OB_SUCC(ret) && OB_LIKELY(pos < end)) {
      if (OB_FAIL(decode_string_kv(end, pos, tmp_kv))) {
        OB_LOG(WARN, "fail to decode string kv", K(ret));
      } else {
        if (tmp_kv.key_ == ObMySQLPacket::get_separator_kv().key_
            && tmp_kv.value_ == ObMySQLPacket::get_separator_kv().value_) {
          found_separator = true;
          // continue
        } else {
          if (found_separator) {
            if (OB_FAIL(user_vars_.push_back(tmp_kv))) {
              OB_LOG(WARN, "fail to push back user_vars", K(tmp_kv), K(ret));
            }
          } else {
            if (OB_FAIL(sys_vars_.push_back(tmp_kv))) {
              OB_LOG(WARN, "fail to push back sys_vars", K(tmp_kv), K(ret));
            }
          }
        }
      }
    } // end while
  }

  return ret;
}

int ObMPChangeUser::process()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  bool is_proxy_mod = get_conn()->is_proxy_;
  bool need_disconnect = true;
  bool need_response_error = true;
  const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
  if (OB_FAIL(get_session(session))) {
    LOG_ERROR("get session  fail", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get session info", K(ret), K(session));
  } else if (OB_FAIL(process_kill_client_session(*session))) {
    LOG_WARN("client session has been killed", K(ret));
  } else if (FALSE_IT(session->set_txn_free_route(pkt.txn_free_route()))) {
  } else if (OB_FAIL(process_extra_info(*session, pkt, need_response_error))) {
    LOG_WARN("fail get process extra info", K(ret));
  } else if (FALSE_IT(session->post_sync_session_info())) {
  } else {
    need_disconnect = false;
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    session->update_last_active_time();
    if (OB_FAIL(ObSqlTransControl::rollback_trans(session, need_disconnect))) {
      OB_LOG(WARN, "fail to rollback trans for change user", K(ret), K(session));
    } else {
      session->clean_status();
      if (OB_FAIL(load_login_info(session))) {
        OB_LOG(WARN,"load log info failed", K(ret),K(session->get_sessid()));
      } else if (get_conn()->is_support_plugin_auth()
                && get_conn()->client_type_ == common::OB_CLIENT_NON_STANDARD) {
        // do nothing
      } else if (OB_FAIL(load_privilege_info_for_change_user(session))) {
        OB_LOG(WARN,"load privilige info failed", K(ret),K(session->get_sessid()));
      } else {
        if (is_proxy_mod) {
          if (!sys_vars_.empty()) {
            for (int64_t i = 0; OB_SUCC(ret) && i < sys_vars_.count(); ++i) {
              if (OB_FAIL(session->update_sys_variable(sys_vars_.at(i).key_, sys_vars_.at(i).value_))) {
                OB_LOG(WARN, "fail to update session vars", "sys_var", sys_vars_.at(i), K(ret));
              }
            }
          }
          if (OB_SUCC(ret) && !user_vars_.empty()) {
            if (OB_FAIL(replace_user_variables(*session))) {
              OB_LOG(WARN, "fail to replace user variables", K(ret));
            }
          }
        }  // end proxy client mod
      }
    }
  }

  //send packet to client
  if (OB_SUCC(ret)) {
    /*
     In order to be compatible with the behavior of mysql change user,
     an AuthSwitchRequest request will be sent every time to the external client.

     If we're dealing with an older client we can't just send a change plugin
     packet to re-initiate the authentication handshake, because the client
     won't understand it. The good thing is that we don't need to : the old
     client expects us to just check the user credentials here, which we can do
     by just reading the cached data that are placed there by change user's
     passwd field.
     * */
    if (get_conn()->is_support_plugin_auth()
        && get_conn()->client_type_ == common::OB_CLIENT_NON_STANDARD) {
      // send auth switch request
      OMPKAuthSwitch auth_switch;
      auth_switch.set_plugin_name(ObString(AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD));
      auth_switch.set_scramble(ObString(sizeof(get_conn()->scramble_buf_), get_conn()->scramble_buf_));
      if (OB_FAIL(packet_sender_.response_packet(auth_switch, session))) {
        RPC_LOG(WARN, "failed to send error packet", K(auth_switch), K(ret));
        disconnect();
      } else {
        get_conn()->set_auth_switch_phase();
      }
    } else {
      ObOKPParam ok_param;
      ok_param.is_on_change_user_ = true;
      if (OB_FAIL(send_ok_packet(*session, ok_param))) {
        OB_LOG(WARN, "response ok packet fail", K(ret));
      }
    }
  } else if (need_response_error) {
    if (OB_FAIL(send_error_packet(ret, NULL))) {
      OB_LOG(WARN,"response fail packet fail", K(ret));
    }
    need_disconnect = true;
  }

  if (OB_UNLIKELY(need_disconnect) && is_conn_valid()) {
    if (OB_ISNULL(session)) {
      // ignore ret
      LOG_WARN("will disconnect connection", K(ret), K(session));
    } else {
      LOG_WARN("will disconnect connection", K(ret), KPC(session));
    }
    force_disconnect();
  }

  if (session != NULL) {
    revert_session(session);
  }
  return ret;
}
// Attention:in order to get the real type of each user var,
// we should build a standard sql 'SET @var1 = val1,@var2 = val2,......;',
// and then parse the sql
int ObMPChangeUser::replace_user_variables(ObBasicSessionInfo &session) const
{
  int ret = OB_SUCCESS;
  if (!user_vars_.empty()) {
    // 1. build a standard sql
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt("SET"))) {
      OB_LOG(WARN, "fail to append_fmt 'SET'", K(ret));
    }
    ObStringKV kv;
    for (int64_t i = 0; OB_SUCC(ret) && i < user_vars_.count(); ++i) {
      kv = user_vars_.at(i);
      if (OB_FAIL(sql.append_fmt(" @%.*s = %.*s,",
                                 kv.key_.length(), kv.key_.ptr(),
                                 kv.value_.length(), kv.value_.ptr()))) {
        OB_LOG(WARN, "fail to append fmt user var", K(ret), K(kv));
      }
    }
    if (OB_SUCC(ret)) {
      // 2. user parser to parse sql
      *(sql.ptr() + sql.length() - 1) = ';';
      ObString stmt;
      stmt.assign_ptr(sql.ptr(), static_cast<int32_t>(sql.length()));
      ObArenaAllocator allocator(ObModIds::OB_SQL_PARSER);
      ObParser parser(allocator, session.get_sql_mode());
      SMART_VAR(ParseResult, result) {
        if (OB_FAIL(parser.parse(stmt, result))) {
          OB_LOG(WARN, "fail to parse stmt", K(ret), K(stmt));
        } else {
          // 3. parse result node and handle user session var
          ParseNode *node = result.result_tree_;
          ObArenaAllocator calc_buf(ObModIds::OB_SQL_SESSION);
          ObCastCtx cast_ctx(&calc_buf, NULL, CM_NONE, ObCharset::get_system_collation());
          if (OB_FAIL(parse_var_node(node, cast_ctx, session))) {
            OB_LOG(WARN, "fail to parse user var node", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObMPChangeUser::parse_var_node(const ParseNode *node, ObCastCtx &cast_ctx, ObBasicSessionInfo &session) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "node is null", K(ret));
  } else {
    bool found = false;
    ParseNode *tmp_node = NULL;
    ParseNode *val_node = NULL;
    ObString var;
    ObString val;
    ObObjType type;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < node->num_child_; ++i) {
      if (NULL != (tmp_node = node->children_[i])) {
        if (0 == tmp_node->num_child_) {
          if (T_USER_VARIABLE_IDENTIFIER == tmp_node->type_) {
            found = true;
            // handle user var
            if (node->num_child_ != 2) {
              ret = OB_ERR_UNEXPECTED;
              OB_LOG(WARN, "node children num must be 2 if it is VAR SET", K(ret), K_(node->num_child));
            } else if (OB_ISNULL(val_node = node->children_[1 - i])) {
              ret = OB_ERR_UNEXPECTED;
              OB_LOG(WARN, "val node is null", K(ret));
            } else {
              var.assign_ptr(tmp_node->str_value_, static_cast<int32_t>(tmp_node->str_len_));
              val.assign_ptr(val_node->str_value_, static_cast<int32_t>(val_node->str_len_));
              type = (static_cast<ObObjType>(val_node->type_));
              if (OB_FAIL(handle_user_var(var, val, type, cast_ctx, session))) {
                OB_LOG(WARN, "fail to handle user var", K(ret), K(var), K(val), K(type));
              }
            }
          }
        } else if (OB_FAIL(parse_var_node(tmp_node, cast_ctx, session))) {
          OB_LOG(WARN, "fail to parse node", K(ret));
        }
      } // end NULL != tmp_node
    } // end for
  } // end else
  return ret;
}

int ObMPChangeUser::handle_user_var(const ObString &var, const ObString &val,
                                    const ObObjType type, ObCastCtx &cast_ctx,
                                    ObBasicSessionInfo &session) const
{
  int ret = OB_SUCCESS;
  ObObj in_obj;
  ObObj buf_obj;
  const ObObj *out_obj = NULL;
  ObSessionVariable sess_var;
  if (ObNullType == type) {
    sess_var.value_.set_null();
    sess_var.meta_.set_collation_level(CS_LEVEL_IMPLICIT);
    sess_var.meta_.set_collation_type(CS_TYPE_BINARY);
  } else {
    // cast varchar obj to real type
    in_obj.set_varchar(val);
    in_obj.set_collation_type(ObCharset::get_system_collation());
    if (OB_FAIL(ObObjCaster::to_type(type, cast_ctx, in_obj, buf_obj, out_obj))) {
      OB_LOG(WARN, "fail to cast varchar to target type", K(ret), K(type), K(in_obj));
    } else if (OB_ISNULL(out_obj)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "out obj is null", K(ret));
    } else {
      sess_var.value_ = *out_obj;
      sess_var.meta_.set_type(out_obj->get_type());
      sess_var.meta_.set_scale(out_obj->get_scale());
      sess_var.meta_.set_collation_level(CS_LEVEL_IMPLICIT);
      sess_var.meta_.set_collation_type(out_obj->get_collation_type());
    }
    if (OB_SUCC(ret) && OB_FAIL(session.replace_user_variable(var, sess_var))) {
      OB_LOG(WARN, "fail to replace user var", K(ret), K(var), K(sess_var));
    }
  }
  return ret;
}

int ObMPChangeUser::load_login_info(ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  share::schema::ObUserLoginInfo login_info;
  const char *sep_pos = username_.find('@');
  if (NULL != sep_pos) {
    ObString username(sep_pos - username_.ptr(), username_.ptr());
    login_info.user_name_ = username;
    login_info.tenant_name_ = username_.after(sep_pos);
    if (login_info.tenant_name_ != session->get_tenant_name()) {
      ret = OB_OP_NOT_ALLOW;
      OB_LOG(WARN, "failed to change user in different tenant", K(ret),
          K(login_info.tenant_name_), K(session->get_tenant_name()));
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "forbid! change user command in differernt tenant");
    }
  } else {
    login_info.user_name_ = username_;
  }
  if (OB_SUCC(ret)) {
    if (login_info.tenant_name_.empty()) {
      login_info.tenant_name_ = session->get_tenant_name();
    }
    if (!database_.empty()) {
      login_info.db_ = database_;
    }
    login_info.client_ip_ = session->get_client_ip();
    OB_LOG(INFO, "com change user", "username", login_info.user_name_,
          "tenant name", login_info.tenant_name_);
    const ObSMConnection &conn = *get_conn();
    login_info.scramble_str_.assign_ptr(conn.scramble_buf_, sizeof(conn.scramble_buf_));
    login_info.passwd_ = auth_response_;
    if (OB_FAIL(session->set_login_info(login_info))) {
      LOG_WARN("failed to set login_info", K(ret));
    } else if (OB_FAIL(session->set_default_database(database_))) {
      OB_LOG(WARN, "failed to set default database", K(ret), K(database_));
    }
  }
  return ret;
}

} //namespace observer
} //namespace oceanbase
