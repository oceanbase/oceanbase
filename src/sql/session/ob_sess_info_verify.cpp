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


#define USING_LOG_PREFIX SERVER

#include <math.h>
#include "sql/session/ob_sess_info_verify.h"
#include "share/ob_define.h"
#include "share/system_variable/ob_system_variable_factory.h"
#include "lib/trace/ob_trace_event.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"
#include "share/ob_all_server_tracer.h"

namespace oceanbase
{
namespace sql
{

int SessionInfoVerifacation::set_verify_info_sess_id(const uint32_t sess_id) {
  int ret = OB_SUCCESS;
  sess_id_ = sess_id;
  return ret;
}

int SessionInfoVerifacation::set_verify_info_proxy_sess_id(const uint64_t proxy_sess_id) {
  int ret = OB_SUCCESS;
  proxy_sess_id_ = proxy_sess_id;
  return ret;
}

int SessionInfoVerifacation::set_verify_info_addr(const ObAddr addr) {
  int ret = OB_SUCCESS;
  addr_ = addr;
  return ret;
}

// extra_info.sess_info_veri_ to session
int ObSessInfoVerify::sync_sess_info_veri(sql::ObSQLSessionInfo &sess,
                          const common::ObString &sess_info_veri,
                          SessionInfoVerifacation &sess_info_verification)
{
  int ret = OB_SUCCESS;
  const char *buf = sess_info_veri.ptr();
  const char *data = sess_info_veri.ptr();
  const int64_t len = sess_info_veri.length();
  const char *end = buf + len;
  int64_t pos = 0;
  LOG_DEBUG("start sync proxy sess info verification", K(sess.get_is_in_retry()),
            K(sess.get_sessid()), KP(data), K(len), KPHEX(data, len));

  // decode sess_info
  if (NULL != sess_info_veri.ptr() && !sess.get_is_in_retry()) {
    while (OB_SUCC(ret) && pos < len) {
      int16_t extra_id = 0;
      int32_t info_len = 0;
      char *sess_buf = NULL;
      LOG_DEBUG("sync field sess_inf", K(sess.get_sessid()),
                KP(data), K(pos), K(len), KPHEX(data+pos, len-pos));
      if (OB_FAIL(ObProtoTransUtil::resolve_type_and_len(buf, len, pos, extra_id, info_len))) {
        LOG_WARN("failed to resolve type and len", K(ret), K(len), K(pos));
      } else if (extra_id < 0 || info_len <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid session sync info verification extra id", K(ret), K(extra_id));
      // for old version compatible
      // if new version has a val that old version doesn't has, just ignore.
      } else if (extra_id >= sql::SESS_INFO_VERI_MAX_TYPE) {
        pos += info_len;
      } else if (OB_FAIL(ObSessInfoVerify::deserialize_sess_info_veri_id(sess,
                                      static_cast<SessionInfoVerificationId>(extra_id),
                                                              info_len, buf, len, pos,
                                                              sess_info_verification))) {
        LOG_WARN("failed to resolve value", K(extra_id), KP(buf), K(len), K(pos), K(info_len));
      } else {
        LOG_DEBUG("success to resolve value", K(extra_id), K(len), K(pos), K(info_len));
      }
    }
    LOG_DEBUG("success to get sess info verification requied by proxy",
              K(sess_info_verification), K(sess.get_sessid()),
              K(sess.get_proxy_sessid()));
  }

  return ret;
}

// rpc to last server to fetch verify session info
int ObSessInfoVerify::verify_session_info(sql::ObSQLSessionInfo &sess,
                    SessionInfoVerifacation &sess_info_verification)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  obrpc::ObSessInfoVerifyArg arg;
  obrpc::ObSessionInfoVeriRes result;
  // current session verify info
  ObString current_verify_info;
  if (GET_MIN_CLUSTER_VERSION() == CLUSTER_CURRENT_VERSION) {
    if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to get srv_rpc_proxy", K(ret), K(GCTX.srv_rpc_proxy_));
    } else if (OB_FAIL(ObSessInfoVerify::sql_port_to_rpc_port(sess,
                      sess_info_verification))) {
      LOG_WARN("fail to rpc port", K(ret));
      // For compatibility, no error
      ret = OB_SUCCESS;
    } else if (FALSE_IT(addr = sess_info_verification.get_verify_info_addr())) {
    } else if (FALSE_IT(arg.set_sess_id(sess_info_verification.get_verify_info_sess_id()))) {
      LOG_WARN("fail to set session id", K(ret));
    } else if (FALSE_IT(arg.set_proxy_sess_id(
              sess_info_verification.get_verify_info_proxy_sess_id()))) {
      LOG_WARN("fail to set proxy session id", K(ret));
    } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(addr).by(MTL_ID()).
                          session_info_verification(arg, result))) {
      // rpc fail not self-verification.
      LOG_TRACE("fail to rpc", K(ret));
      ret = OB_SUCCESS;
    } else {
      // self-verification
      ObString value_buffer;
      char *ptr = nullptr;
      common::ObArenaAllocator allocator(common::ObModIds::OB_SQL_SESSION,
                                                    OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                    sess.get_effective_tenant_id());
      if (OB_ISNULL(ptr = static_cast<char *> (allocator.alloc(result.verify_info_buf_.length
())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc mem for client identifier", K(ret));
      } else {
        value_buffer.assign_buffer(ptr, result.verify_info_buf_.length());
        value_buffer.write(result.verify_info_buf_.ptr(), result.verify_info_buf_.length());
      }
      LOG_DEBUG("need verify", K(&result), K(result.need_verify_), K(result.verify_info_buf_));
      if (OB_FAIL(ret)) {
      } else if (result.need_verify_) {
        // verification error injection.
        ObSessInfoVerify::veri_err_injection(sess);
        if (OB_FAIL(ObSessInfoVerify::fetch_verify_session_info(sess, current_verify_info,
                allocator))) {
          LOG_WARN("fail to serialize session verify info, no need verify", K(ret));
          ret = OB_SUCCESS;
        } else if (OB_FAIL(ObSessInfoVerify::compare_verify_session_info(sess,
                            current_verify_info, value_buffer))) {
          LOG_ERROR("session info self-verification failed", K(ret), K(sess.get_sessid()),
                  K(sess.get_proxy_sessid()), K(sess_info_verification));
        } else {
          LOG_DEBUG("session info self-verification success", K(ret));
        }
      } else {
        LOG_DEBUG("session info no need self-verification", K(ret));
      }
      LOG_DEBUG("verify end", K(sess.get_sessid()),
          K(sess.get_proxy_sessid()), K(sess_info_verification));
    }
  } else {
    LOG_TRACE("verify version not consistent, no need self-verification", K(sess.get_sessid()),
          K(sess.get_proxy_sessid()), K(GET_MIN_CLUSTER_VERSION()), K(CLUSTER_CURRENT_VERSION));
  }

  return ret;
}

int ObSessInfoVerify::compare_verify_session_info(sql::ObSQLSessionInfo &sess,
                            common::ObString &result1, common::ObString &result2)
{
  int ret = OB_SUCCESS;
  const char *buf1 = result1.ptr();
  const char *data1 = result1.ptr();
  const int64_t len1 = result1.length();
  const char *end1 = buf1 + len1;
  const char *buf2 = result2.ptr();
  const char *data2 = result2.ptr();
  const int64_t len2 = result2.length();
  const char *end2 = buf2 + len2;
  int64_t pos1 = 0;
  int64_t pos2 = 0;
  ObSessInfoEncoder* encoder = NULL;
  LOG_TRACE("compare verify session info start", KP(data1), K(len1), KPHEX(data1, len1),
            KP(data2), K(len2), KPHEX(data2, len2));

  // decode sess_info
  if (NULL != result1.ptr() && NULL != result2.ptr()) {
    while (OB_SUCC(ret) && pos1 < len1 && pos2 < len2) {
      int16_t info_type1 = 0;
      int32_t info_len1 = 0;
      int16_t info_type2 = 0;
      int32_t info_len2 = 0;
      int64_t temp_pos1 = 0;
      int64_t temp_pos2 = 0;
      if (OB_FAIL(ObProtoTransUtil::resolve_type_and_len(
                  buf1, len1, pos1, info_type1, info_len1))) {
        LOG_WARN("failed to resolve type and len", K(ret), K(len1), K(pos1));
      } else if (OB_FAIL(ObProtoTransUtil::resolve_type_and_len(
                        buf2, len2, pos2, info_type2, info_len2))) {
        LOG_WARN("failed to resolve type and len", K(ret), K(len1), K(pos1));
      } else if (info_type1 < 0 || info_len1 <= 0 || info_type2 < 0 || info_len2 <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid session sync info encoder", K(ret), K(info_type1), K(info_type2),
                        K(info_len1), K(info_len2));
      // for old version compatible
      // if new version has a val that old version doesn't has, just ignore.
      } else if (info_type1 >= SESSION_SYNC_MAX_TYPE) {
        pos1 += info_len1;
      } else if (info_type2 >= SESSION_SYNC_MAX_TYPE) {
        pos2 += info_len2;
      } else if (FALSE_IT(temp_pos1 = pos1)) {
      } else if (FALSE_IT(temp_pos2 = pos2)) {
      } else if (info_type1 != info_type2) {
        LOG_WARN("info type is not consistent", K(ret), K(info_type1), K(info_type2));
      } else if (OB_FAIL(sess.get_sess_encoder(SessionSyncInfoType(info_type1), encoder))) {
        LOG_WARN("failed to get session encoder", K(ret));
      } else if (OB_FAIL(encoder->compare_sess_info(sess, buf1 + pos1, info_len1,
                                  buf2 + pos2, info_len2))) {
        LOG_ERROR("fail to compare session info", K(ret),
                    K(sess.get_sessid()),
                  K(sess.get_proxy_sessid()),
                  "info_type", info_type1);
        int temp_ret = ret;
        if (OB_FAIL(encoder->display_sess_info(sess, buf1 + pos1, info_len1,
                                  buf2 + pos2, info_len2))) {
          LOG_WARN("fail to display session info", K(ret));
        } else {
          ret = temp_ret;
        }
      } else {
        LOG_TRACE("session info type and len is consistent", K(ret), K(info_type1));
      }

      if (OB_FAIL(ret)) {
      } else {
        pos1 += info_len1;
        pos2 += info_len2;
      }
    }
  }
  return ret;
}

// fetch session info veri info
int ObSessInfoVerify::fetch_verify_session_info(sql::ObSQLSessionInfo &sess,
                                                  common::ObString &result,
                                                  common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSessInfoEncoder* encoder = NULL;
  char *buf = NULL;
  int64_t size = 0;
  int32_t sess_size[SESSION_SYNC_MAX_TYPE];
  for (int64_t i=0; OB_SUCC(ret) && i < SESSION_SYNC_MAX_TYPE; i++) {
    oceanbase::sql::SessionSyncInfoType info_type = (oceanbase::sql::SessionSyncInfoType)(i);
    sess_size[i] = 0;
    if (OB_FAIL(sess.get_sess_encoder(info_type, encoder))) {
      LOG_WARN("failed to get session encoder", K(ret));
    } else {
      sess_size[i] = encoder->get_fetch_sess_info_size(sess);
      LOG_TRACE("sess info size", K(sess_size[i]), K(info_type));
      size += ObProtoTransUtil::get_serialize_size(sess_size[i]);
    }
  }

  int64_t pos = 0;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_UNLIKELY(size < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid buffer length", K(ret), K(size));
  } else if (NULL == (buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(size), K(ret));
  } else {
    // assamble session info as follows:
    // type(2 byte) | len(4 byte) | session_info_val | ....
    for (int64_t i=0; OB_SUCC(ret) && i < SESSION_SYNC_MAX_TYPE; i++) {
      oceanbase::sql::SessionSyncInfoType encoder_type = (oceanbase::sql::SessionSyncInfoType)(i);
      if (OB_FAIL(sess.get_sess_encoder(encoder_type, encoder))) {
        LOG_WARN("failed to get session encoder", K(ret));
      } else {
        int16_t info_type = (int16_t)i;
        int32_t info_len = sess_size[i];
        if (info_len < 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid session info length", K(info_len), K(info_type), K(ret));
        } else if (info_len == 0) {
          // invalid info len do nothing and skip it.
        } else if (OB_FAIL(ObProtoTransUtil::store_type_and_len(
                          buf, size, pos, info_type, info_len))) {
          LOG_WARN("failed to set type and len", K(info_type), K(info_len), K(ret));
        } else if (OB_FAIL(encoder->fetch_sess_info(sess, buf, size, pos))) {
          LOG_WARN("failed to serialize", K(sess), K(ret), K(size), K(pos));
        } else {
          LOG_TRACE("success to fetch", K(sess_size[i]), K(info_type));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    result.assign_ptr(buf, size);
    LOG_TRACE("fetch serialize buf", KPHEX(buf, size), K(pos), K(size), K(result));
  }
  return ret;
}

// verification error injection.
void ObSessInfoVerify::veri_err_injection(sql::ObSQLSessionInfo &sess)
{
  int64_t code = 0;
  code = OB_E(EventTable::EN_SESS_INFO_VERI_SYS_VAR_ERROR) OB_SUCCESS;
  if (code < 0) {
    share::ObBasicSysVar *sys_var = NULL;
    sys_var = sess.get_sys_var(100);
    sys_var->set_value(ObObj(2000000000));
  }
  code = OB_E(EventTable::EN_SESS_INFO_VERI_APP_INFO_ERROR) OB_SUCCESS;
  if (code < 0) {
    sess.set_action_name("err_name");
  }
  code = OB_E(EventTable::EN_SESS_INFO_VERI_APP_CTX_ERROR) OB_SUCCESS;
  if (code < 0) {
    ObContextsMap &map = sess.get_contexts_map();
    for (auto it = map.begin(); it != map.end(); ++it) {
      auto it2 = it->second->context_map_->begin();
      it2->second->value_ = "err_value";
      break;
    }
  }
  code = OB_E(EventTable::EN_SESS_INFO_VERI_CLIENT_ID_ERROR) OB_SUCCESS;
  if (code < 0) {
    sess.set_client_identifier("err_name");
  }
  code = OB_E(EventTable::EN_SESS_INFO_VERI_CONTROL_INFO_ERROR) OB_SUCCESS;
  if (code < 0) {
    FLTControlInfo temp = sess.get_control_info();
    temp.level_ = 100;
    sess.set_flt_control_info_no_sync(temp);
  }
}

// resolve session info verification from proxy.
int ObSessInfoVerify::deserialize_sess_info_veri_id(sql::ObSQLSessionInfo &sess,
                                    SessionInfoVerificationId extra_id,
                                      const int64_t v_len,
                                      const char *buf, const int64_t len, int64_t &pos,
                                      SessionInfoVerifacation &sess_info_verification)
{
  int ret = OB_SUCCESS;
  switch(extra_id) {
    case SESS_INFO_VERI_ADDR: {
      char* ptr = NULL;
      ObAddr addr;
      char ip_buf[MAX_IP_ADDR_LENGTH] = "";
      uint64_t length = v_len;
      if (OB_FAIL(ObProtoTransUtil::get_str(buf, len, pos, v_len, ptr))) {
        OB_LOG(WARN,"failed to resolve veri level", K(ret));
      } else if (FALSE_IT(length = std::min(length, uint64_t(MAX_IP_ADDR_LENGTH - 1)))) {
      } else if (FALSE_IT(memcpy(ip_buf, ptr, length))) {
      } else if (FALSE_IT(ip_buf[length] = '\0')) {
      } else if (OB_FAIL(addr.parse_from_cstring(ip_buf))) {
        OB_LOG(WARN,"failed to parse from cstring", K(ret));
      } else if (OB_FAIL(sess_info_verification.set_verify_info_addr(addr))) {
        OB_LOG(WARN,"failed to set verify info addr", K(ret));
      }
      break;
    }
    case SESS_INFO_VERI_SESS_ID: {
      int32_t v = 0;
      if (OB_FAIL(ObProtoTransUtil::get_int4(buf, len, pos, v_len, v))) {
        OB_LOG(WARN,"failed to resolve veri level", K(ret));
      } else if (OB_FAIL(sess_info_verification.set_verify_info_sess_id(static_cast<uint32_t>(v)))) {
        OB_LOG(WARN,"failed to set verify info session id", K(ret));
      }
      break;
    }
    case SESS_INFO_VERI_PROXY_SESS_ID: {
      int64_t v = 0;
      if (OB_FAIL(ObProtoTransUtil::get_int8(buf, len, pos, v_len, v))) {
        OB_LOG(WARN,"failed to resolve veri level", K(ret));
      } else if(OB_FAIL(sess_info_verification.set_verify_info_proxy_sess_id(static_cast<uint64_t>(v)))) {
        OB_LOG(WARN,"failed to set verify info proxy session id", K(ret));
      }
      break;
    }
    default: {
      // For compatibility, no error
      OB_LOG(WARN,"not support extra info id", K(extra_id));
      break;
    }
  }
  return ret;
}

// get rpc port by sql port
int ObSessInfoVerify::sql_port_to_rpc_port(sql::ObSQLSessionInfo &sess,
                      SessionInfoVerifacation &sess_info_verification)
{
  int ret = OB_SUCCESS;
  int64_t rpc_port = 0;
  bool exist = false;
  LOG_TRACE("sql port", K(sess_info_verification.get_verify_info_addr()));
  if (OB_FAIL(share::ObAllServerTracer::get_instance().get_server_rpc_port(
      sess_info_verification.get_verify_info_addr(),
      sess_info_verification.get_verify_info_addr().get_port(), rpc_port, exist))) {
    LOG_WARN("fail to get rpc port", K(ret));
  } else if (!exist) {
    // not find rpc port by local cache, need send inner sql to find.
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      ObMySQLTransaction trans;
      bool with_snap_shot = true;

      ObMySQLProxy *mysql_proxy = GCTX.sql_proxy_;
      uint64_t proxy_sessid = sess.get_proxy_sessid();
      if (OB_ISNULL(mysql_proxy)) {
        ret = OB_NOT_INIT;
        SERVER_LOG(WARN, "mysql proxy is null", K(ret));
      } else if (OB_FAIL(trans.start(mysql_proxy, OB_SYS_TENANT_ID, with_snap_shot))) {
        SERVER_LOG(WARN, "failed to start transaction", K(ret));
      } else {
        int sql_len = 0;
        const static int MAX_IP_BUFFER_LEN = 32;
        char ip_buf[MAX_IP_BUFFER_LEN];
        ip_buf[0] = '\0';
        if (sess_info_verification.get_verify_info_addr().ip_to_string(ip_buf,
            MAX_IP_BUFFER_LEN)) {
          SMART_VAR(char[OB_MAX_SQL_LENGTH], sql) {
            const uint64_t exec_tenant_id = OB_SYS_TENANT_ID;
            const char *table_name = share::OB_ALL_SERVER_TNAME;
            sql_len = snprintf(sql, OB_MAX_SQL_LENGTH,
                                "SELECT svr_port "
                                "FROM %s WHERE inner_port = %d and svr_ip = '%s'",
                                table_name,
                                sess_info_verification.get_verify_info_addr().get_port(),
                                ip_buf);
            LOG_TRACE("send inner sql to get rpc port", K(sess.get_proxy_sessid()),
                       K(sess_info_verification.get_verify_info_addr().get_port()));
            if (sql_len >= OB_MAX_SQL_LENGTH || sql_len <= 0) {
              ret = OB_SIZE_OVERFLOW;
              SERVER_LOG(WARN, "failed to format sql. size not enough");
            } else {

              { // make sure %res destructed before execute other sql in the same transaction
                SMART_VAR(ObMySQLProxy::MySQLResult, res) {
                  common::sqlclient::ObMySQLResult *result = NULL;
                  ObISQLClient *sql_client = &trans;
                  uint64_t table_id = share::OB_ALL_SERVER_TID;
                  ObSQLClientRetryWeak sql_client_retry_weak(sql_client,
                                                        OB_SYS_TENANT_ID,
                                                          table_id);
                  // retrive data from client
                  if (OB_FAIL(sql_client_retry_weak.read(res, OB_SYS_TENANT_ID, sql))) {
                    SERVER_LOG(WARN, "failed to read data", K(ret));
                  } else if (NULL == (result = res.get_result())) {
                    ret = OB_ERR_UNEXPECTED;
                    SERVER_LOG(WARN, "failed to get result", K(ret));
                  } else {
                    while (OB_SUCC(ret) && OB_SUCC(result->next())) {
                      if (OB_FAIL(result->get_int(0l, rpc_port))) {
                        LOG_WARN("fail to get varchar.", K(ret));
                      } else {
                        sess_info_verification.get_verify_info_addr().set_port(rpc_port);
                      }
                    }
                    if (OB_ITER_END == ret) {
                      ret = OB_SUCCESS;
                    }
                  }
                }
              }
            }
          }
          LOG_TRACE("get rpc port by inner sql", K(sess_info_verification.get_verify_info_addr()),
                        K(ret), K(sql), K(rpc_port));
        }
      }
    }
  } else {
    sess_info_verification.get_verify_info_addr().set_port(rpc_port);
    LOG_TRACE("not use inner sql to find rpc port", K(rpc_port));
  }


  return ret;
}
// use for display sys var error message.
int ObSessInfoVerify::create_tmp_sys_var(sql::ObSQLSessionInfo &sess,
                    share::ObSysVarClassType sys_var_id, share::ObBasicSysVar *&sys_var,
                    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  share::ObBasicSysVar *sys_var_ptr = NULL;
  if (OB_ISNULL(sys_var_ptr)) {
    share::ObSysVarFactory::create_sys_var(allocator,
                    sys_var_id, sys_var_ptr);
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(sys_var_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ret is OB_SUCCESS, but sys_var_ptr is NULL", K(ret), K(sys_var_id));
    } else {
      sys_var = sys_var_ptr;
    }
  }
  if (OB_FAIL(ret) && sys_var_ptr != nullptr) {
    sys_var_ptr->~ObBasicSysVar();
    sys_var_ptr = NULL;
  }
  return ret;
}

int ObSessInfoVerify::sess_veri_control(obmysql::ObMySQLPacket &pkt, sql::ObSQLSessionInfo *&session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    // do nothing
  } else if (pkt.get_mysql_packet_type() == ObMySQLPacketType::PKT_ERR) {
    // when error scene, can not guarantee the latest session information
    session->set_latest_sess_info(false);
  } else {
    // when other scene, can guarantee the latest session information
    session->set_latest_sess_info(true);
  }
  return ret;
}

// this function use for get another session id when 1:1 server list test
bool GetAnotherSessID::operator()(ObSQLSessionMgr::Key key,
                                             ObSQLSessionInfo *sess_info)
{
  int ret = OB_SUCCESS;
  UNUSED(key);
  LOG_TRACE("current session info", K(sess_info->get_proxy_sessid()),
    K(sess_info->get_sessid()), K(sess_id_), K(proxy_sess_id_));
  if (OB_ISNULL(sess_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", KR(ret));
  } else if (sess_info->get_proxy_sessid() == proxy_sess_id_ &&
            sess_info->get_sessid() != sess_id_) {
    sess_id_ = sess_info->get_sessid();
    LOG_TRACE("find another session id", K(sess_id_));
  } else {
    LOG_INFO("not find another session id", K(sess_id_));
  }
  return OB_SUCCESS == ret;
}

} // end of namespace sql
} // end of namespace oceanbase
