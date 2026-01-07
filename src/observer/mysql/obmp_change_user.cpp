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
#include "sql/ob_sql.h"
#include "rpc/obmysql/packet/ompk_auth_switch.h"
#include "observer/mysql/obmp_stmt_send_piece_data.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/net/ob_net_util.h"
#include "storage/tablelock/ob_table_lock_live_detector.h"


using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obmysql;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{
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
        int64_t len = strnlen(pos, end - pos);
        username_.assign_ptr(pos, static_cast<int32_t>(len));
        pos += username_.length() + 1;
      }

      if (OB_LIKELY(pos < end)) {
        if (capability.cap_flags_.OB_CLIENT_SECURE_CONNECTION) {
          uint8_t auth_response_len = 0;
          ObMySQLUtil::get_uint1(pos, auth_response_len);
          auth_response_.assign_ptr(pos, static_cast<int32_t>(auth_response_len));
          pos += auth_response_len;
        } else {
          int64_t len = strnlen(pos, end - pos);
          auth_response_.assign_ptr(pos, static_cast<int32_t>(len));
          pos += auth_response_.length() + 1;
        }
      }

      if (OB_LIKELY(pos < end)) {
        int64_t len = strnlen(pos, end - pos);
        database_.assign_ptr(pos, static_cast<int32_t>(len));
        pos += database_.length() + 1;
      }

      if (OB_LIKELY(pos < end)) {
        ObMySQLUtil::get_uint2(pos, charset_);
      }

      if (OB_LIKELY(pos < end)) {
        if (capability.cap_flags_.OB_CLIENT_PLUGIN_AUTH) {
          int64_t len = strnlen(pos, end - pos);
          auth_plugin_name_.assign_ptr(pos, static_cast<int32_t>(len));
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
                connect_attrs_.push_back(str_kv);
                if (str_kv.key_ == OB_MYSQL_PROXY_CONNECTION_ID) {
                  has_proxy_connection_id_key_ = true;
                }
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
  int64_t query_timeout = 0;
  bool need_send_auth_switch =
      get_conn()->is_support_plugin_auth() &&
      get_conn()->client_type_ == common::OB_CLIENT_NON_STANDARD &&
      GCONF._enable_auth_switch &&
      (!is_proxy_mod || get_proxy_version() >= PROXY_VERSION_4_2_3_0);
  if (is_proxy_mod && !auth_plugin_name_.empty() &&
    auth_plugin_name_.compare(AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD) == 0) {
    // proxy is native_pass_word plugin, no need auth_switch
    need_send_auth_switch = false;
  }
  if (OB_FAIL(get_session(session))) {
    LOG_ERROR("get session  fail", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get session info", K(ret), K(session));
  } else if (OB_FAIL(session->get_query_timeout(query_timeout))) {
    LOG_WARN("fail to get query timeout", K(ret));
  } else if (FALSE_IT(THIS_WORKER.set_timeout_ts(get_receive_timestamp() + query_timeout))) {
  } else if (OB_FAIL(process_kill_client_session(*session))) {
    LOG_WARN("client session has been killed", K(ret));
  } else if (FALSE_IT(session->set_txn_free_route(pkt.txn_free_route()))) {
  } else if (OB_FAIL(process_extra_info(*session, pkt, need_response_error))) {
    LOG_WARN("fail get process extra info", K(ret));
  } else if (FALSE_IT(session->post_sync_session_info())) {
  } else {
    need_disconnect = false;
    get_conn()->client_cs_type_ = charset_;
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    session->update_last_active_time();
    if (OB_FAIL(ObSqlTransControl::rollback_trans(session, need_disconnect))) {
      OB_LOG(WARN, "fail to rollback trans for change user", K(ret), K(session));
    } else {
      session->clean_status();
      if (is_proxy_mod && has_proxy_connection_id_key_ &&
          OB_FAIL(update_conn_attrs(get_conn(), session))) {
        LOG_WARN("update conn attrs failed", K(ret));
      } else if (OB_FAIL(load_login_info(session))) {
        OB_LOG(WARN,"load log info failed", K(ret),K(session->get_server_sid()));
      } else if (need_send_auth_switch) {
        // do nothing
      } else if (OB_FAIL(load_privilege_info_for_change_user(session))) {
        OB_LOG(WARN,"load privilige info failed", K(ret),K(session->get_server_sid()));
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

  // Reset session state for change user
  if (OB_SUCC(ret)) {
    if (OB_FAIL(reset_session_for_change_user(session))) {
      LOG_WARN("failed to reset session for change user", K(ret));
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
    if (need_send_auth_switch) {
      // send auth switch request
      OMPKAuthSwitch auth_switch;
      auth_switch.set_plugin_name(ObString(AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD));
      auth_switch.set_scramble(ObString(sizeof(get_conn()->scramble_result_buf_), get_conn()->scramble_result_buf_));
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
    LOG_INFO("MYSQL changeuser", K(session->get_client_ip()),
      K(session->get_client_addr_port()), K(session->get_service_name()),
      K(session->get_failover_mode()), K(get_conn()->client_sessid_), K(get_conn()->sessid_),
      K(get_conn()->client_create_time_), K(get_conn()->proxy_sessid_),
      K(get_conn()->client_addr_port_), K(has_proxy_connection_id_key_));
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

int ObMPChangeUser::reset_session_for_change_user(sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else {
    /*
     * According to MySQL doc https://dev.mysql.com/doc/c-api/8.0/en/mysql-change-user.html,
     * following work needs to be done for COM_CHANGE_USER:
     *  1. Rolls back any active transactions and resets autocommit mode.
     *     (Already done before calling this function)
     *  2. Releases all table locks. (OB use row lock, do not need do this)
     *  3. Closes (and drops) all TEMPORARY tables.
     *  4. Reinitializes session system variables to the values of the corresponding global system variables.
     *     (Already done in load_privilege_info_for_change_user)
     *  5. Loses user-defined variable settings.
     *     (Already done in load_privilege_info_for_change_user)
     *  6. Releases prepared statements. (include ps stmt, ps cursor, piece)
     *  7. Closes HANDLER variables. (OB not support HANDLER)
     *  8. Resets the value of LAST_INSERT_ID() to 0.
     *  9. Releases locks acquired with GET_LOCK().
     *  10. OB unique design
     *      10.1  pl debug
     *      10.2  package state
     *      10.3  sequence currval
     *      10.4  warnings buffer
     *      10.5  client identifier
     *      10.6  session label
     *      10.7  memory context
     */

    // 3. Closes (and drops) all TEMPORARY tables.
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(OB_FAIL(session->drop_temp_tables(false, false, true)))) {
        LOG_WARN("fail to drop temp tables", K(ret));
      }
      session->refresh_temp_tables_sess_active_time();
    }

    // 6. Releases prepared statements. (include ps stmt, ps cursor, piece)
    if (OB_SUCC(ret)) {
      // 6.1 ps stmt
      if (OB_FAIL(session->close_all_ps_stmt())) {
        LOG_WARN("failed to close all stmt", K(ret));
      }

      // 6.2 ps cursor
      if (OB_SUCC(ret) && session->get_cursor_cache().is_inited()) {
        if (OB_FAIL(session->get_cursor_cache().close_all(*session))) {
          LOG_WARN("failed to close all cursor", K(ret));
        } else {
          session->get_cursor_cache().reset();
        }
      }

      // 6.3 piece
      if (OB_SUCC(ret) && NULL != session->get_piece_cache()) {
        observer::ObPieceCache* piece_cache =
          static_cast<observer::ObPieceCache*>(session->get_piece_cache());
        if (OB_FAIL(piece_cache->close_all(*session))) {
          LOG_WARN("failed to close all piece", K(ret));
        }
        piece_cache->reset();
        session->get_session_allocator().free(session->get_piece_cache());
        session->set_piece_cache(NULL);
      }

      if (OB_SUCC(ret)) {
        // 6.4 ps session info
        session->reset_ps_session_info();

        // 6.5 ps name
        session->reset_ps_name();
      }
    }

    // 8. Resets the value of LAST_INSERT_ID() to 0.
    if (OB_SUCC(ret)) {
      ObObj last_insert_id;
      last_insert_id.set_uint64(0);
      if (OB_FAIL(session->update_sys_variable(SYS_VAR_LAST_INSERT_ID, last_insert_id))) {
        LOG_WARN("fail to update last_insert_id", K(ret));
      } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_IDENTITY, last_insert_id))) {
        LOG_WARN("succ update last_insert_id, but fail to update identity", K(ret));
      } else {
        NG_TRACE_EXT(last_insert_id, OB_ID(last_insert_id), 0);
      }
    }

    // 9. Releases locks acquired with GET_LOCK().
    if (OB_SUCC(ret)) {
      ObTableLockOwnerID owner_id;
      if (OB_FAIL(owner_id.convert_from_client_sessid(session->get_sid(),
                                                      session->get_client_create_time()))) {
        LOG_WARN("failed to convert from client sessid", K(ret));
      } else if (OB_FAIL(ObTableLockDetector::remove_lock_by_owner_id(owner_id))) {
        LOG_WARN("failed to remove lock by owner id", K(ret));
      }
    }

    // 10. OB unique design
    if (OB_SUCC(ret)) {
      // 10.1 pl debug, pl profiler, pl code coverage
#ifdef OB_BUILD_ORACLE_PL
      session->reset_pl_debugger_resource();
      session->reset_pl_profiler_resource();
      session->reset_pl_code_coverage_resource();
#endif

      // 10.2 package state
      session->reset_all_package_state();

      // 10.3 sequence currval
      session->reuse_all_sequence_value();

      // 10.4 warnings buffer
      session->reset_warnings_buf();
      session->reset_show_warnings_buf();

      // 10.5 client identifier
      session->get_client_identifier_for_update().reset();

      // 10.6 session label
      session->reuse_labels();

      // 10.7 memory context for dbms_session.create_context
      session->destory_mem_context();
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
    login_info.scramble_str_.assign_ptr(conn.scramble_result_buf_, sizeof(conn.scramble_result_buf_));
    login_info.passwd_ = auth_response_;
    if (OB_FAIL(session->set_login_info(login_info))) {
      LOG_WARN("failed to set login_info", K(ret));
    } else if (OB_FAIL(session->set_default_database(database_))) {
      OB_LOG(WARN, "failed to set default database", K(ret), K(database_));
    }
  }
  return ret;
}

int ObMPChangeUser::update_conn_attrs(ObSMConnection *conn, ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  ObString mysql_conn_id_key(OB_MYSQL_CONNECTION_ID);
  ObString proxy_connection_id_key(OB_MYSQL_PROXY_CONNECTION_ID);
  ObString proxy_scramble_key(OB_MYSQL_SCRAMBLE);
  ObString client_ip_key(OB_MYSQL_CLIENT_IP);
  ObString client_addr_port_key(OB_MYSQL_CLIENT_ADDR_PORT);
  ObString client_conn_id_key(OB_MYSQL_CLIENT_SESSION_ID);
  ObString client_create_time_key(OB_MYSQL_CLIENT_CONNECT_TIME_US);
  ObString failover_mode_key(OB_MYSQL_FAILOVER_MODE);
  ObString service_name_key(OB_MYSQL_SERVICE_NAME);
  ObString global_vars_version_key(OB_MYSQL_GLOBAL_VARS_VERSION);

  ObString service_name;
  bool failover_mode = false;
  bool is_found_failover_mode = false;
  bool is_found_service_name = false;
  bool is_found_client_ip = false;
  bool is_found_addr_port = false;

  for (int64_t i = 0; i < connect_attrs_.count() && OB_SUCC(ret); ++i) {
    const ObStringKV &kv = connect_attrs_.at(i);
    if (kv.key_ == proxy_connection_id_key) {
      ObObj value;
      uint64_t proxy_conn_id = 0;
      value.set_varchar(kv.value_);
      ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
      ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
      EXPR_GET_UINT64_V2(value, proxy_conn_id);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to cast proxy connection id to uint32", K(kv.value_), K(ret));
      } else {
        conn->proxy_sessid_ = proxy_conn_id;
      }
    } else if (kv.key_ == proxy_scramble_key) {
      ObString proxy_scramble;
      ObString server_scamble;
      ObString server_result_scamble;
      proxy_scramble.assign_ptr(kv.value_.ptr(), kv.value_.length());
      server_scamble.assign_ptr(conn->scramble_buf_, ObSMConnection::SCRAMBLE_BUF_SIZE);
      if (OB_UNLIKELY(STRLEN(conn->scramble_buf_) != ObSMConnection::SCRAMBLE_BUF_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server orign scramble is unexpected", "length", STRLEN(conn->scramble_buf_),
                 K(conn->scramble_buf_), K(ret));
      } else {
        if (ObSMConnection::SCRAMBLE_BUF_SIZE == proxy_scramble.length()) {
          unsigned char real_scramble_buf[ObSMConnection::SCRAMBLE_BUF_SIZE] = {0};
          // The value of '__proxy_scramble' is not real scramble of proxy
          // In fact, it __proxy_scramble = proxy's xor server's scramble, just for simple encrypt
          // Here we need get orig proxy's scramble by this -- proxy's scramble = __proxy_scramble xor server's scramble
          if (OB_FAIL(ObEncryptedHelper::my_xor(reinterpret_cast<const unsigned char *>(proxy_scramble.ptr()),
              reinterpret_cast<const unsigned char *>(conn->scramble_buf_),
              static_cast<uint32_t>(ObSMConnection::SCRAMBLE_BUF_SIZE),
              real_scramble_buf))) {
            LOG_WARN("failed to calc xor real_scramble_buf", K(ret));
          } else {
            MEMCPY(conn->scramble_result_buf_, real_scramble_buf, ObSMConnection::SCRAMBLE_BUF_SIZE);
          }
        } else {
          const ObString old_scramble("aaaaaaaabbbbbbbbbbbb");
          MEMCPY(conn->scramble_result_buf_, old_scramble.ptr(), old_scramble.length());
        }
      }
      server_scamble.assign_ptr(conn->scramble_buf_, ObSMConnection::SCRAMBLE_BUF_SIZE);
      server_result_scamble.assign_ptr(conn->scramble_result_buf_, ObSMConnection::SCRAMBLE_BUF_SIZE);
    } else if (kv.key_ == client_ip_key) {
      // check client ip white list, and update login_info.client_ip_
      ObString client_ip;
      client_ip.assign_ptr(kv.value_.ptr(), kv.value_.length());
      if (client_ip.empty()) {
        get_peer().ip_to_string(client_ip_buf_, common::MAX_IP_ADDR_LENGTH);
        const char *peer_ip = client_ip_buf_;
        client_ip_.assign_ptr(peer_ip, static_cast<int32_t>(STRLEN(peer_ip)));
      } else {
        client_ip_ = client_ip;
      }
      uint64_t tenant_id = conn->tenant_id_;
      if (OB_FAIL(verify_connection(tenant_id))) {
        LOG_WARN("failed to set service name", K(ret), K(tenant_id), K(service_name), K(failover_mode));
      }
      is_found_client_ip = true;
    } else if (kv.key_ == client_addr_port_key) {
      int32_t client_addr_port;
      ObObj value;
      value.set_varchar(kv.value_);
      ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
      ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
      EXPR_GET_INT32_V2(value, client_addr_port);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to cast client connection id to int32", K(kv.value_), K(ret));
      } else {
        conn->client_addr_port_ = client_addr_port;
      }
      if (conn->client_addr_port_ == 0) {
        client_port_ = get_peer().get_port();
      } else {
        client_port_ = conn->client_addr_port_;
      }
      is_found_addr_port = true;
    } else if (kv.key_ == client_conn_id_key) {
      uint32_t client_sessid;
      ObObj value;
      value.set_varchar(kv.value_);
      ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
      ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
      EXPR_GET_UINT32_V2(value, client_sessid);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to cast client connection id to uint32", K(kv.value_), K(ret));
      } else {
        conn->client_sessid_ = client_sessid;
      }
    } else if (kv.key_ == client_create_time_key) {
      int64_t client_create_time;
      ObObj value;
      value.set_varchar(kv.value_);
      ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
      ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
      EXPR_GET_INT64_V2(value, client_create_time);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to cast client create time", K(kv.value_), K(ret));
      } else {
        conn->client_create_time_ = client_create_time;
      }
    } else if (kv.key_ == failover_mode_key) {
      ObString failover_mode_off(OB_MYSQL_FAILOVER_MODE_OFF);
      ObString failover_mode_on(OB_MYSQL_FAILOVER_MODE_ON);
      if (failover_mode_off == kv.value_) {
        failover_mode = false;
      } else if (failover_mode_on == kv.value_) {
        failover_mode = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failover_mode should be on or off", KR(ret), K(kv));
      }
      is_found_failover_mode = true;
    } else if (kv.key_ == service_name_key) {
      if (kv.value_.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("service_name should not be empty", KR(ret), K(kv));
      } else {
        conn->has_service_name_ = true;
        (void) service_name.assign_ptr(kv.value_.ptr(), kv.value_.length());
      }
      is_found_service_name = true;
    } else if (kv.key_ == global_vars_version_key) {
      // not used, and proxy will send it for now
    } else {
      LOG_TRACE("UnKnown conn_attr or not used attr", K(kv));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_found_client_ip != is_found_addr_port) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client_ip or addr_port is missing", KR(ret), K(is_found_client_ip), K(is_found_addr_port));
  } else if (is_found_client_ip && is_found_addr_port &&
      OB_FAIL(session->set_real_client_ip_and_port(client_ip_, client_port_))) {
    LOG_WARN("failed to set_real_client_ip_and_port", K(ret));
  } else if (is_found_failover_mode != is_found_service_name) {
    // The 'failover_mode' and 'service_name' must both be specified at the same time.
    // The 'failover_mode' only matters if 'service_name' is not empty.
    // If 'failover_mode' is 'on', it allows connection only to the main tenant.
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failover_mode or service_name is missing", KR(ret), K(is_found_failover_mode), K(is_found_service_name));
  } else if (is_found_failover_mode && is_found_service_name) {
    uint64_t tenant_id = conn->tenant_id_;
    if (OB_FAIL(set_service_name(tenant_id, *session, service_name, failover_mode))) {
      LOG_WARN("failed to set service name", K(ret), K(tenant_id), K(service_name), K(failover_mode));
    }
  }
  return ret;
}

int ObMPChangeUser::set_service_name(const uint64_t tenant_id, ObSQLSessionInfo &session,
  const ObString &service_name, const bool failover_mode)
{
  int ret = OB_SUCCESS;
  (void) session.set_failover_mode(failover_mode);
  if (OB_FAIL(ret) || service_name.empty()) {
    // If the connection is not established via 'service_name', the 'connection_attr'
    // will not contain 'failover_mode' and 'service_name'. Consequently, 'service_name'
    // in 'session_info' will be empty, indicating that any 'service_name' related logic
    // will not be triggered.
  } else if (OB_FAIL(session.set_service_name(service_name))) {
    LOG_WARN("fail to set service_name", KR(ret), K(service_name), K(tenant_id));
  } else if (OB_FAIL(session.check_service_name_and_failover_mode(tenant_id))) {
    LOG_WARN("fail to execute check_service_name_and_failover_mode", KR(ret), K(service_name), K(tenant_id));
  }
  return ret;
}

int ObMPChangeUser::verify_connection(const uint64_t tenant_id) const
{
  // different with ObMPConnect::verify_connection in connect stage, no need check check_max_sess
  int ret = OB_SUCCESS;
  const char *IPV4_LOCAL_STR = "127.0.0.1";
  const char *IPV6_LOCAL_STR = "::1";
  ObSMConnection *conn = get_conn();

  if (OB_SUCC(ret)) {
    //if normal tenant can not login with error variables, sys tenant can recover the error variables
    //but if sys tenant set error variables, no one can recover it.
    //so we need leave a backdoor for root@sys from 127.0.0.1 to skip this verifing
    if (OB_SYS_TENANT_ID == tenant_id
        && 0 == username_.compare(OB_SYS_USER_NAME)
        && (0 == client_ip_.compare(IPV4_LOCAL_STR)
            || 0 == client_ip_.compare(IPV6_LOCAL_STR))) {
      LOG_DEBUG("this is root@sys user from local host, no need verify_ip_white_list", K(ret));
    } else if (OB_SYS_TENANT_ID == tenant_id
               && (SS_INIT == GCTX.status_ || SS_STARTING == GCTX.status_)) {
      LOG_INFO("server is initializing, ignore verify_ip_white_list", "status", GCTX.status_, K(ret));
    } else if (OB_FAIL(verify_ip_white_list(tenant_id))) {
      LOG_WARN("failed to verify_ip_white_list", K(ret));
    }
  }
  return ret;
}

int ObMPChangeUser::verify_ip_white_list(const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  const ObSysVariableSchema *sys_variable_schema = NULL;
  share::schema::ObSchemaGetterGuard schema_guard;
  ObString var_name(OB_SV_TCP_INVITED_NODES);
  const ObSysVarSchema *sysvar = NULL;
  if (OB_UNLIKELY(client_ip_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client_ip is empty", K(ret));
  } else if (0 == client_ip_.compare(UNIX_SOCKET_CLIENT_IP)) {
    LOG_INFO("match unix socket connection", K(tenant_id), K(client_ip_));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("get tenant info failed", K(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_schema is null", K(ret));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", K(ret));
  } else if (OB_ISNULL(sys_variable_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is null", K(ret));
  } else if (OB_FAIL(sys_variable_schema->get_sysvar_schema(var_name, sysvar))) {
    LOG_WARN("fail to get_sysvar_schema",  K(ret));
  } else {
    ObString var_value = sysvar->get_value();
    if (!obsys::ObNetUtil::is_in_white_list(client_ip_, var_value)) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("client is not invited into this tenant", K(ret));
    }
  }
  return ret;
}

} //namespace observer
} //namespace oceanbase
