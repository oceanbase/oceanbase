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
#include "obmp_utils.h"
#include "obmp_base.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/utility/ob_proto_trans_util.h"
#include "observer/mysql/obmp_utils.h"
#include "rpc/obmysql/ob_2_0_protocol_utils.h"
#include "sql/monitor/full_link_trace/ob_flt_control_info_mgr.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;
using namespace obmysql;
using namespace sql;

int ObMPUtils::add_changed_session_info(OMPKOK &ok_pkt, sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (session.is_session_info_changed()) {
    ok_pkt.set_state_changed(true);
  }
  if (session.is_database_changed()) {
    ObString db_name = session.get_database_name();
    ok_pkt.set_changed_schema(db_name);
  }

  ObIAllocator &allocator = session.get_allocator();
  if (session.is_sys_var_changed()) {
    const ObIArray<sql::ObBasicSessionInfo::ChangedVar> &sys_var = session.get_changed_sys_var();
    LOG_DEBUG("sys var changed", K(session.get_tenant_name()), K(sys_var.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_var.count(); ++i) {
      sql::ObBasicSessionInfo::ChangedVar change_var = sys_var.at(i);
      ObObj new_val;
      bool changed = true;
      if (OB_FAIL(session.is_sys_var_actully_changed(change_var.id_,
                                                     change_var.old_val_,
                                                     new_val,
                                                     changed))) {
        LOG_WARN("failed to check actully changed", K(ret), K(change_var), K(changed));
      } else if (changed) {
        ObStringKV str_kv;
        if (OB_FAIL(ObSysVarFactory::get_sys_var_name_by_id(change_var.id_, str_kv.key_))) {
          LOG_WARN("failed to get sys variable name", K(ret), K(change_var));
        } else if (OB_FAIL(get_plain_str_literal(allocator, new_val, str_kv.value_))) {
          LOG_WARN("failed to get sys vairable new value string", K(ret), K(new_val));
        } else if (OB_FAIL(ok_pkt.add_system_var(str_kv))) {
          LOG_WARN("failed to add system variable", K(str_kv), K(ret));
        } else {
          LOG_DEBUG("success add system var to ok pack", K(str_kv), K(change_var), K(new_val));
        }
      }
    }
  }

  if (session.is_user_var_changed()) {
    const ObIArray<ObString> &user_var = session.get_changed_user_var();
    ObSessionValMap &user_map = session.get_user_var_val_map();
    for (int64_t i = 0; i < user_var.count() && OB_SUCCESS == ret; ++i) {
      ObString name = user_var.at(i);
      ObSessionVariable sess_var;
      if (name.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid variable name", K(name), K(ret));
      }
      if (OB_FAIL(user_map.get_refactored(name, sess_var))) {
        LOG_WARN("unknown user variable", K(name), K(ret));
      } else {
        ObStringKV str_kv;
        str_kv.key_ = name;
        if (OB_FAIL(get_user_sql_literal(allocator, sess_var.value_, str_kv.value_, session.create_obj_print_params()))) {
          LOG_WARN("fail to get user sql literal", K(sess_var.value_), K(ret));
        } else if (OB_FAIL(ok_pkt.add_user_var(str_kv))) {
          LOG_WARN("fail to add user var", K(str_kv), K(ret));
        } else {
          LOG_DEBUG("succ to add user var", K(str_kv), K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_client_feedback(ok_pkt, session))) {
      LOG_WARN("fail to add client feedback", K(ret));
    }
  }
  return ret;
}

int ObMPUtils::sync_session_info(sql::ObSQLSessionInfo &sess, const common::ObString &sess_infos)
{
  int ret = OB_SUCCESS;
  const char *buf = sess_infos.ptr();
  const char *data = sess_infos.ptr();
  const int64_t len = sess_infos.length();
  const char *end = buf + len;
  int64_t pos = 0;

  LOG_TRACE("sync sess_inf", K(sess.get_sessid()), KP(data), K(len), KPHEX(data, len));

  // decode sess_info
  if (NULL != sess_infos.ptr()) {
    while (OB_SUCC(ret) && pos < len) {
      int16_t info_type = 0;
      int32_t info_len = 0;
      char *sess_buf = NULL;
      LOG_TRACE("sync field sess_inf", K(sess.get_sessid()), KP(data), K(pos), K(len), KPHEX(data+pos, len-pos));
      if (OB_FAIL(ObProtoTransUtil::resolve_type_and_len(buf, len, pos, info_type, info_len))) {
        LOG_WARN("failed to resolve type and len", K(ret), K(len), K(pos));
      } else if (info_type < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid session sync info encoder", K(ret), K(info_type));
      // for old version compatible
      // if new version has a val that old version doesn't has, just ignore.
      } else if (info_type >= SESSION_SYNC_MAX_TYPE) {
        pos += info_len;
      } else if (OB_FAIL(sess.update_sess_sync_info(
                                  (oceanbase::sql::SessionSyncInfoType)(info_type),
                                  buf, (int64_t)info_len + pos, pos))) {
        LOG_WARN("failed to update session sync info",
                                K(ret), K(pos), K(info_len), K(info_len+pos));
      } else {
        //buf += info_len;
      }
    }
  }

  return ret;
}

int ObMPUtils::append_modfied_sess_info(common::ObIAllocator &allocator,
                                        sql::ObSQLSessionInfo &sess,
                                        ObIArray<ObObjKV> *extra_info,
                                        ObIArray<obmysql::Obp20Encoder*> *extra_info_ecds,
                                        bool is_new_extra_info)
{
  int ret = OB_SUCCESS;
  if (!sess.has_sess_info_modified()) {
    LOG_DEBUG("not modified");
    // do nothing
  } else {
    // assemble ok packet's result
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
        if (encoder->is_changed_) {
          sess_size[i] = encoder->get_serialize_size(sess);
          size += ObProtoTransUtil::get_serialize_size(sess_size[i]);
          LOG_DEBUG("get seri size", K(sess_size[i]), K(encoder->get_serialize_size(sess)));
        } else {
          // encoder->is_changed_ = false;
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (size == 0){
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
      int64_t pos = 0;
      for (int64_t i=0; OB_SUCC(ret) && i < SESSION_SYNC_MAX_TYPE; i++) {
        oceanbase::sql::SessionSyncInfoType encoder_type = (oceanbase::sql::SessionSyncInfoType)(i);
        if (OB_FAIL(sess.get_sess_encoder(encoder_type, encoder))) {
          LOG_WARN("failed to get session encoder", K(ret));
        } else if (encoder->is_changed_) {
          int16_t info_type = (int16_t)i;
          int32_t info_len = sess_size[i];
          if (info_len < 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid session info length", K(info_len), K(info_type), K(ret));
          } else if (info_len == 0) {
            // invalid info len do nothing and skip it.
            encoder->is_changed_ = false;
          } else if (OB_FAIL(ObProtoTransUtil::store_type_and_len(buf, size, pos, info_type, info_len))) {
            LOG_WARN("failed to set type and len", K(info_type), K(info_len), K(ret));
          } else if (OB_FAIL(encoder->serialize(sess, buf, size, pos))) {
            LOG_WARN("failed to serialize", K(sess), K(ret), K(size), K(pos));
          } else {
            // reset to not changed
            encoder->is_changed_ = false;
          }
        } else {
          // do nothing
        }
      }

      // set session info to extra info
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (0 == size) {
        // do nothing
      } else if (is_new_extra_info) {
        Obp20SessInfoEncoder* sess_inf_ecd = NULL;
        void* ecd_buf = NULL;
        if (OB_ISNULL(ecd_buf = allocator.alloc(sizeof(Obp20SessInfoEncoder)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory.", K(ret));
        } else {
          sess_inf_ecd = new(ecd_buf)Obp20SessInfoEncoder();
          sess_inf_ecd->sess_info_.assign(buf, size);
          if (OB_FAIL(extra_info_ecds->push_back(sess_inf_ecd))) {
            LOG_WARN("failed to add extra info kv", K(sess_inf_ecd), K(ret));
          } else {
            LOG_TRACE("add extra_info", KPHEX(buf, size), KP(buf));
          }
        }
      } else {
        ObObjKV kv;
        common::ObObj key;
        common::ObObj value;
        ObString key_str = "sess_inf";
        key.set_varchar(key_str);
        value.set_varchar(ObString(size, buf));
        kv.key_ = key;
        kv.value_ = value;
        if (OB_FAIL(extra_info->push_back(kv))) {
          LOG_WARN("failed to add extra info kv", K(kv), K(ret));
        } else {
          LOG_TRACE("add extra_info", K(kv) , KPHEX(buf, size), KP(buf), KP(kv.value_.get_string().ptr()));
        }
      }
    }
  }

  return ret;
}

// add OB_CLIENT_FEEDBACK, treat as user var
int ObMPUtils::add_client_feedback(OMPKOK &ok_pkt, sql::ObSQLSessionInfo &session) {
  INIT_SUCC(ret);
  const ObFeedbackManager &fb_manager = session.get_feedback_manager();
  if (!fb_manager.is_empty()) {
    ObIAllocator &allocator = session.get_allocator();
    const int64_t SER_BUF_LEN = 1024;
    const int64_t RETRY_COUNT = 4;
    const int64_t MULTIPLIER = 8;

    int64_t tmp_len = 0;
    char *tmp_buff = NULL;
    int64_t pos = 0;
    bool need_retry = true;
    // assume: seri buffer never > 512KB
    for (int64_t i = 0; need_retry && (i < RETRY_COUNT) && OB_SUCC(ret); i++) {
      tmp_len = ((0 == i) ? (SER_BUF_LEN) : (SER_BUF_LEN * i * MULTIPLIER));
      tmp_buff = (char *)allocator.alloc(tmp_len);
      pos = 0;
      if (OB_ISNULL(tmp_buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(tmp_len), K(ret));
      } else if (OB_FAIL(fb_manager.serialize(tmp_buff, tmp_len, pos))) {
        if (OB_SIZE_OVERFLOW == ret || OB_BUF_NOT_ENOUGH == ret) {
          // buf not enough, retry
          ret = OB_SUCCESS;
        }
      } else if (OB_UNLIKELY(pos <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pos", K(pos), K(tmp_len), K(ret));
      } else {
        need_retry = false; // break;
      }
    }
    if (OB_SUCC(ret)) {
      ObStringKV str_kv;
      str_kv.key_.assign_ptr(OB_CLIENT_FEEDBACK, static_cast<int32_t>(strlen(OB_CLIENT_FEEDBACK)));
      str_kv.value_.assign(tmp_buff, static_cast<int32_t>(pos));
      if (OB_FAIL(ok_pkt.add_user_var(str_kv))) {
        LOG_WARN("fail to add user var", K(str_kv), K(ret));
      }
    }
  }
  return ret;
}

int ObMPUtils::add_client_reroute_info(OMPKOK &okp,
                                       sql::ObSQLSessionInfo &session,
                                       share::ObFeedbackRerouteInfo &reroute_info)
{
  LOG_DEBUG("adding client reroute info", K(reroute_info), K(okp));
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = session.get_allocator();
  char *tmp_buff = NULL;
  int64_t pos = 0;
  const int64_t SER_BUF_LEN = 1024;
  if (OB_ISNULL(tmp_buff = (char *)allocator.alloc(SER_BUF_LEN))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(reroute_info.serialize(tmp_buff, SER_BUF_LEN, pos))) {
    LOG_WARN("failed to serialize reroute info", K(ret));
  } else {
    ObStringKV str_kv;
    str_kv.key_.assign_ptr(OB_CLIENT_REROUTE_INFO, static_cast<int32_t>(strlen(OB_CLIENT_REROUTE_INFO)));
    str_kv.value_.assign(tmp_buff, static_cast<int32_t>(pos));
    if (OB_FAIL(okp.add_user_var(str_kv))) {
      LOG_WARN("failed to add user var", K(ret), K(str_kv));
    }
  }

  return ret;
}

int ObMPUtils::init_flt_info(Ob20ExtraInfo extra_info,
                             sql::ObSQLSessionInfo &session,
                             bool is_client_support_flt)
{
  int ret = OB_SUCCESS;
  if (extra_info.exist_full_link_trace()) {
    OZ(process_flt_extra_info(extra_info.get_full_link_trace().ptr(),
                              extra_info.get_full_link_trace().length(),
                              session));
    extra_info.get_full_link_trace().reset();
  }
  OZ(init_flt_log_framework(session, is_client_support_flt));

  return ret;
}

int ObMPUtils::append_flt_extra_info(common::ObIAllocator &allocator,
                                     ObIArray<obmysql::ObObjKV> *extra_info,
                                     ObIArray<obmysql::Obp20Encoder*> *extra_info_ecds,
                                     sql::ObSQLSessionInfo &sess,
                                     bool is_new_extra_info)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int size = 0;
  FLTQueryInfo query_info;

  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(sess.get_effective_tenant_id()));

  // reserver memory for control info
  // if sys config in control info and sys parameter has modified, resend this control info.
  if (sess.get_control_info().is_valid_sys_config()
        && !((sess.get_control_info().print_sample_pct_ == ((double)(tenant_config->_print_sample_ppm))/1000000)
        && (sess.get_control_info().slow_query_thres_ == GCONF.trace_log_slow_query_watermark))) {
    sess.set_send_control_info(false);
  }

  if (!sess.is_send_control_info()) {
    size += sess.get_control_info().get_serialize_size();
  }

  // reserver memmory for query info
  if (sess.is_trace_enable()) {
    query_info.query_start_time_ = sess.get_query_start_time();
    query_info.query_end_time_ = ::oceanbase::common::ObTimeUtility::current_time();
    size += query_info.get_serialize_size();
  }

  if (size == 0){
    // has not flt extra info, do nothing
  } else if (OB_UNLIKELY(size < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid buffer length", K(ret), K(size));
  } else if (NULL == (buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc mem", K(size), K(ret));
  } else {
    int64_t pos = 0;
    // assamble control info
    if (!sess.is_send_control_info()) {
      FLTControlInfo con = sess.get_control_info();
      if (!con.is_valid()) {
        con.reset();
      }
      con.print_sample_pct_ = ((double)(tenant_config->_print_sample_ppm))/1000000;
      con.slow_query_thres_ = GCONF.trace_log_slow_query_watermark;

      sess.set_flt_control_info(con);

      if (OB_FAIL(con.serialize(buf, size, pos))) {
        LOG_WARN("failed to serialize control info", K(pos), K(size));
      } else {
        sess.set_send_control_info(true);
      }
    }

    // assamble query info
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (sess.is_trace_enable()) {
      if (OB_FAIL(query_info.serialize(buf, size, pos))) {
        LOG_WARN("failed to serialize query info", K(pos), K(size));
      } else {
        sess.set_trace_enable(false);
      }
    }
  }

  // set session info to extra info
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (size == 0) {
    // nothing to write, do nothing
  } else if (is_new_extra_info) {
    Obp20FullTrcEncoder* full_trc_ecd = NULL;
    void* ecd_buf = NULL;
    if (OB_ISNULL(ecd_buf = allocator.alloc(sizeof(Obp20FullTrcEncoder)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory.", K(ret));
    } else {
      full_trc_ecd = new(ecd_buf)Obp20FullTrcEncoder();
      full_trc_ecd->full_trc_.assign(buf, size);
      if (OB_FAIL(extra_info_ecds->push_back(full_trc_ecd))) {
        LOG_WARN("failed to add extra info kv", K(full_trc_ecd), K(ret));
      }
    }
  } else {
    ObObjKV kv;
    common::ObObj key;
    common::ObObj value;
    ObString key_str = "full_trc";
    key.set_varchar(key_str);
    value.set_varchar(ObString(size, buf));
    kv.key_ = key;
    kv.value_ = value;
    if (OB_FAIL(extra_info->push_back(kv))) {
      LOG_WARN("failed to add extra info kv", K(kv), K(ret));
    }
  }
  return ret;
}

int ObMPUtils::process_flt_extra_info(const char *buf,
                  const int64_t len, sql::ObSQLSessionInfo &sess)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  LOG_TRACE("recieve flt extra info", KP(buf), K(len), KPHEX(buf, len));

  while (OB_SUCC(ret) && pos < len) {
    FullLinkTraceExtraInfoType extra_type;
    int32_t v_len = 0;
    LOG_TRACE("process single flt extra info", KP(buf), K(pos), K(len), KPHEX(buf+pos, len-pos));
    if (OB_FAIL(FLTExtraInfo::resolve_type_and_len(buf, len, pos, extra_type, v_len))) {
      LOG_WARN("failed to resolve type and len", K(len), K(pos));
    } else if (pos+v_len > len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("buf size overflow", K(ret), K(pos), K(v_len), K(len));
    } else {
      switch (extra_type) {
        // for drv types:
        case FLT_TYPE_DRV_LOG: {
          FLTDrvSpan drv_span;
          if (OB_FAIL(drv_span.deserialize(buf, pos+v_len, pos))) {
            LOG_WARN("failed to deserialize full link trace extra info",
                                      KP(buf), K(ret), K(pos), K(v_len));
          } else {
            _FLT_LOG(INFO, "%s", drv_span.span_info_.ptr());
          }
          break;
        }

        // for proxy types: 

        // for public types:
        case FLT_TYPE_APP_INFO: {
          // do nothing
          FLTAppInfo app_info;
          FLTControlInfo con;
          ObFLTControlInfoManager mgr(sess.get_effective_tenant_id());
          if (OB_FAIL(app_info.deserialize(buf, pos+v_len, pos))) {
            LOG_WARN("failed to deserialize full link trace extra info",
                                      KP(buf), K(ret), K(pos), K(v_len));
          } else if (OB_FAIL(mgr.init())) {
            LOG_WARN("failed to init full link trace info manager", K(ret));
          } else {
            if (app_info.trace_client_info_.empty()) {
              // do nothing
            } else if (OB_FAIL(sess.get_app_info_encoder()
                          .set_client_info(&sess, app_info.trace_client_info_))) {
              LOG_WARN("failed to set client info name", K(ret));
            }

            if (OB_FAIL(ret)) {
              // do nothing
            } else if (OB_FAIL(init_app_info(sess, app_info))) {
              LOG_WARN("failed  to init app info from session", K(ret));
            } else if (OB_FAIL(mgr.find_appropriate_con_info(sess))) {
              LOG_WARN("failed to find appropriate control info", K(ret));
            } else {
              // do nothing
            }
          }
          break;
        }
        case FLT_TYPE_QUERY_INFO: {
          // this extra info is written by server, and driver/proxy cannot send this to server;
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("invalid extra_type", K(extra_type), K(ret));
          break;
        }
        case FLT_TYPE_CONTROL_INFO: {
          // this extra info is written by server, and driver/proxy cannot send this to server;
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("invalid extra_type", K(extra_type), K(ret));
          break;
        }
        case FLT_TYPE_SPAN_INFO: {
          FLTSpanInfo span_info;
          if (OB_FAIL(span_info.deserialize(buf, pos+v_len, pos))) {
            LOG_WARN("failed to deserialize full link trace extra info",
                                      KP(buf), K(ret), K(pos), K(v_len));
          } else if (span_info.trace_enable_) {
            if (OB_FAIL(sess.set_flt_trace_id(span_info.trace_id_))) {
              LOG_WARN("failed to set trace id", K(ret));
            } else if (OB_FAIL(sess.set_flt_span_id(span_info.span_id_))) {
              LOG_WARN("failed to set span id", K(ret));
            } else {
              sess.set_trace_enable(span_info.trace_enable_);
              sess.set_auto_flush_trace(span_info.force_print_);
            }
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("invalid extra_type", K(extra_type), K(ret));
          break;
        }
      } // switch ends
    }
  } // while ends

  return ret;
}

int ObMPUtils::init_flt_log_framework(sql::ObSQLSessionInfo &sess, bool is_client_support_flt)
{
  // initialize log framework
  int ret = OB_SUCCESS;
  FLT_RESET_SPAN();
  if (sess.is_server_status_in_transaction()) {
    // in the trans, do nothing
  } else {
    // reset trace_enable and flush trace in transcation granularity
    // init flush trace and trace enable
    if (!is_client_support_flt) {
      // reset log config
      sess.set_trace_enable(false);
      sess.set_auto_flush_trace(false);

      FLTControlInfo con = sess.get_control_info();
      if (con.is_valid()) {
        // init trace enable
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(sess.get_effective_tenant_id()));
        con.print_sample_pct_ = ((double)(tenant_config->_print_sample_ppm))/1000000;
        ObRandom r;
        double rand_num = 1.0 * (r.rand(0, RAND_MAX)/RAND_MAX);
        if (rand_num < con.sample_pct_) {
          sess.set_trace_enable(true);
        } else {
          sess.set_trace_enable(false);
        }

        // init flush trace
        if (con.rp_ == FLTControlInfo::RecordPolicy::RP_ALL) {
          if (sess.is_trace_enable()) {
            sess.set_auto_flush_trace(true);
          } else {
            sess.set_auto_flush_trace(false);
          }
        } else if (con.rp_ == FLTControlInfo::RecordPolicy::RP_ONLY_SLOW_QUERY) {
          // do nothing, slow query will must flush
        } else if (con.rp_ == FLTControlInfo::RecordPolicy::RP_SAMPLE_AND_SLOW_QUERY) {
          double rand_num2 = 1.0 * (r.rand(0, RAND_MAX)/RAND_MAX);
          if (rand_num2 < con.print_sample_pct_) {
            sess.set_auto_flush_trace(true);
          } else {
            sess.set_auto_flush_trace(false);
          }
        }
      } else {
        // invalid control info, do nothing
      }
    } else {
      //set by client
    }
  }

    // init log frame
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!sess.get_control_info().is_valid() || !sess.is_trace_enable()) {
      FLT_SET_TRACE_LEVEL(0);
      FLT_SET_AUTO_FLUSH(false);
      sess.set_auto_flush_trace(false);
      sess.set_trace_enable(false);
    } else if (is_client_support_flt) {
      trace::UUID tid, sid;
      int64_t pos = 0;
      ObString span_id;
      ObString trace_id;
      sess.get_flt_span_id(span_id);
      sess.get_flt_trace_id(trace_id);
      if (span_id.empty() || trace_id.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid span_id or trace_id", K(span_id), K(trace_id), K(ret));
      } else {
        tid.deserialize(trace_id.ptr(), trace_id.length(), pos);
        pos = 0;
        sid.deserialize(span_id.ptr(), span_id.length(), pos);
        OBTRACE->init(tid, sid);
        FLT_SET_TRACE_LEVEL(sess.get_control_info().level_);
        FLT_SET_AUTO_FLUSH(sess.is_auto_flush_trace());
      }
    } else {
      // update trace_id in transaction granularity
      if (!sess.get_in_transaction()) {
        // begin new trace
        FLT_BEGIN_TRACE();
      }
      FLT_SET_TRACE_LEVEL(sess.get_control_info().level_);
      FLT_SET_AUTO_FLUSH(sess.is_auto_flush_trace());
    }

    LOG_TRACE("flt init log", K(sess.is_trace_enable()),
                K(sess.is_auto_flush_trace()), K(sess.get_control_info()));
  return ret;
}

int ObMPUtils::init_app_info(sql::ObSQLSessionInfo &sess, FLTAppInfo &app_info)
{
  int ret = OB_SUCCESS;

  if (!app_info.trace_module_.empty() &&
              OB_FAIL(sess.get_app_info_encoder().
                set_module_name(&sess, app_info.trace_module_))) {
    LOG_WARN("failed to set mod name", K(ret));
  } else if (!app_info.trace_action_.empty() &&
              OB_FAIL(sess.get_app_info_encoder().
                set_action_name(&sess, app_info.trace_action_))) {
    LOG_WARN("failed to set action name", K(ret));
  } else if (!app_info.trace_client_identifier_.empty() &&
              OB_FAIL(sess.set_client_id(app_info.trace_client_identifier_))) {
    LOG_WARN("failed to set client id", K(ret));
  } else {
    // do nothing
  }

  return ret;
}

int ObMPUtils::add_cap_flag(OMPKOK &okp, sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObIAllocator &allocator = session.get_allocator();
  for (int64_t i = 0; OB_SUCC(ret) && i < session.get_sys_var_count(); ++i) {
    const ObBasicSysVar *sys_var = NULL;
    if (NULL == (sys_var = session.get_sys_var(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("sys var is NULL", K(i), "total", session.get_sys_var_count(), K(ret));
    } else if (sys_var->get_type() == SYS_VAR_OB_CAPABILITY_FLAG) {
      ObStringKV str_kv;
      str_kv.key_ = ObSysVarFactory::get_sys_var_name_by_id(sys_var->get_type()); // shadow copy
      if (OB_FAIL(get_plain_str_literal(allocator, sys_var->get_value(), str_kv.value_))) {
        LOG_WARN("fail to get sql literal", K(i), K(ret));
      } else if (OB_FAIL(okp.add_system_var(str_kv))) {
        LOG_WARN("fail to add system var", K(i), K(str_kv), K(ret));
      }
    } else {
      // skip
    }
  }
  return ret;
}

int ObMPUtils::add_nls_format(OMPKOK &okp, sql::ObSQLSessionInfo &session, const bool only_changed/*false*/)
{
  int ret = OB_SUCCESS;
  if (only_changed) {
    if (session.is_sys_var_changed()) {
      const ObIArray<sql::ObBasicSessionInfo::ChangedVar> &sys_var = session.get_changed_sys_var();
      LOG_DEBUG("sys var changed", K(session.get_tenant_name()), K(sys_var.count()));
      int64_t max_add_count = ObNLSFormatEnum::NLS_MAX;
      for (int64_t i = 0; OB_SUCC(ret) && i < sys_var.count() && max_add_count > 0; ++i) {
        const sql::ObBasicSessionInfo::ChangedVar change_var = sys_var.at(i);
        ObObj new_val;
        bool changed = true;
        ObNLSFormatEnum nls_enum = ObNLSFormatEnum::NLS_MAX;
        if (change_var.id_ == SYS_VAR_NLS_DATE_FORMAT) {
          nls_enum = ObNLSFormatEnum::NLS_DATE;
        } else if (change_var.id_ == SYS_VAR_NLS_TIMESTAMP_FORMAT) {
          nls_enum = ObNLSFormatEnum::NLS_TIMESTAMP;
        } else if (change_var.id_ == SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT) {
          nls_enum = ObNLSFormatEnum::NLS_TIMESTAMP_TZ;
        }
        if (nls_enum != ObNLSFormatEnum::NLS_MAX) {
          --max_add_count;
          if (OB_FAIL(session.is_sys_var_actully_changed(change_var.id_,
                                                         change_var.old_val_,
                                                         new_val,
                                                         changed))) {
            LOG_WARN("failed to check actully changed", K(ret), K(change_var), K(changed));
          } else if (changed) {
            ObStringKV str_kv;
            str_kv.key_ = ObSysVarFactory::get_sys_var_name_by_id(change_var.id_); // shadow copy
            if (ObNLSFormatEnum::NLS_DATE == nls_enum) {
              str_kv.value_ = session.get_local_nls_date_format();
            } else if (ObNLSFormatEnum::NLS_TIMESTAMP == nls_enum) {
              str_kv.value_ = session.get_local_nls_timestamp_format();
            } else if (ObNLSFormatEnum::NLS_TIMESTAMP_TZ == nls_enum) {
              str_kv.value_ = session.get_local_nls_timestamp_tz_format();
            }

            if (OB_FAIL(okp.add_system_var(str_kv))) {
              LOG_WARN("failed to add system variable", K(str_kv), K(ret));
            } else {
              //AS ob pkt encoding is different from mysql, we should not set_state_changed true
              okp.set_state_changed(false);
              LOG_DEBUG("success add system var to ok pack", K(str_kv), K(change_var), K(new_val), K(okp));
            }
          }
        }
      }
    }
  } else {
    //AS ob pkt encoding is different from mysql, we should not set_state_changed true
    okp.set_state_changed(false);

    ObStringKV nls_date_str_kv;
    nls_date_str_kv.key_ = ObSysVarFactory::get_sys_var_name_by_id(SYS_VAR_NLS_DATE_FORMAT); // shadow copy
    nls_date_str_kv.value_ = session.get_local_nls_date_format();

    ObStringKV nls_timestamp_str_kv;
    nls_timestamp_str_kv.key_ = ObSysVarFactory::get_sys_var_name_by_id(SYS_VAR_NLS_TIMESTAMP_FORMAT); // shadow copy
    nls_timestamp_str_kv.value_ = session.get_local_nls_timestamp_format();

    ObStringKV nls_timestamp_tz_str_kv;
    nls_timestamp_tz_str_kv.key_ = ObSysVarFactory::get_sys_var_name_by_id(SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT); // shadow copy
    nls_timestamp_tz_str_kv.value_ = session.get_local_nls_timestamp_tz_format();

    if (OB_FAIL(okp.add_system_var(nls_date_str_kv))) {
      LOG_WARN("fail to add system var", K(nls_date_str_kv), K(ret));
    } else if (OB_FAIL(okp.add_system_var(nls_timestamp_str_kv))) {
      LOG_WARN("fail to add system var", K(nls_timestamp_str_kv), K(ret));
    } else if (OB_FAIL(okp.add_system_var(nls_timestamp_tz_str_kv))) {
      LOG_WARN("fail to add system var", K(nls_timestamp_tz_str_kv), K(ret));
    } else {
      LOG_DEBUG("succ to add system var", K(okp), K(ret));
    }
  }
  return ret;
}

int ObMPUtils::add_session_info_on_connect(OMPKOK &okp, sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  // treat it as state changed
  okp.set_state_changed(true);

  // add database name
  if (session.is_database_changed()) {
    ObString db_name = session.get_database_name();
    okp.set_changed_schema(db_name);
  }

  // update_global_vars_version_ to global_vars_last_modified_time
  ObObj value;
  value.set_int(session.get_global_vars_version());
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = session.update_sys_variable(share::SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION, value))) {
    LOG_WARN("failed to update global variables version, we will go on anyway", K(session), K(tmp_ret));
  }

  // add all sys variables
  ObIAllocator &allocator = session.get_allocator();
  for (int64_t i = 0; OB_SUCC(ret) && i < session.get_sys_var_count(); ++i) {
    const ObBasicSysVar *sys_var = NULL;
    if (NULL == (sys_var = session.get_sys_var(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("sys var is NULL", K(i), "total", session.get_sys_var_count(), K(ret));
    } else {
      ObStringKV str_kv;
      str_kv.key_ = ObSysVarFactory::get_sys_var_name_by_id(sys_var->get_type()); // shadow copy
      if (OB_FAIL(get_plain_str_literal(allocator, sys_var->get_value(), str_kv.value_))) {
        LOG_WARN("fail to get sql literal", K(i), K(ret));
      } else if (OB_FAIL(okp.add_system_var(str_kv))) {
        LOG_WARN("fail to add system var", K(i), K(str_kv), K(ret));
      }
    }
  }
  return ret;
}

int ObMPUtils::get_plain_str_literal(ObIAllocator &allocator, const ObObj &obj, ObString &value_str)
{
  int ret = OB_SUCCESS;
  char *data = NULL;
  int64_t pos = 0;
  const bool is_plain = true;
  int64_t plain_str_print_length = 0;
  ObObjPrintParams default_print_params;
  if (obj.is_null()) {
    // if obj is null value , return ""; mysql have the same behavior
    pos = 0;
  } else if (OB_FAIL(get_literal_print_length(obj, is_plain, plain_str_print_length, default_print_params))) {
    LOG_WARN("fail to get buffer length", K(ret), K(obj), K(plain_str_print_length));
  } else if (OB_UNLIKELY(plain_str_print_length <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid buffer length", K(ret), K(obj), K(plain_str_print_length));
  } else if (NULL == (data = static_cast<char *>(allocator.alloc(plain_str_print_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc mem", K(plain_str_print_length), K(ret));
  } else {
    ret = obj.print_plain_str_literal(data, plain_str_print_length, pos);
  }
  if (OB_SUCC(ret)) {
    value_str.assign_ptr(data, static_cast<uint32_t>(pos));
  }
  return ret;
}

int ObMPUtils::get_user_sql_literal(ObIAllocator &allocator, const ObObj &obj, ObString &value_str,
                                    const common::ObObjPrintParams &print_param)
{
  int ret = OB_SUCCESS;
  char *data = NULL;
  int64_t pos = 0;
  const bool is_plain = false;
  int64_t user_sql_print_length = 0;
  if (OB_FAIL(get_literal_print_length(obj, is_plain, user_sql_print_length, print_param))) {
    LOG_WARN("fail to get buffer length", K(ret), K(obj), K(user_sql_print_length));
  } else if (OB_UNLIKELY(user_sql_print_length <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid buffer length", K(ret), K(obj), K(user_sql_print_length));
  } else if (NULL == (data = static_cast<char *>(allocator.alloc(user_sql_print_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc mem", K(user_sql_print_length), K(ret));
  } else if (OB_FAIL(obj.print_sql_literal(data, user_sql_print_length, pos, print_param))) {
    LOG_WARN("fail to print sql  literal", K(ret), K(pos), K(user_sql_print_length), K(obj));
  } else {
    value_str.assign_ptr(data, static_cast<uint32_t>(pos));
  }
  return ret;
}

int ObMPUtils::get_literal_print_length(const ObObj &obj, bool is_plain, int64_t &len,
                                        const common::ObObjPrintParams &print_param)
{
  int ret = OB_SUCCESS;
  len = 0;
  int32_t len_of_string = 0;
  const ObLobLocator *locator = nullptr;
  if (!obj.is_string_or_lob_locator_type() && !obj.is_json()) {
    len = OB_MAX_SYS_VAR_NON_STRING_VAL_LENGTH;
  } else if (OB_UNLIKELY((len_of_string = obj.get_string_len()) < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("string length invalid", K(obj), K(len_of_string));
  } else if (obj.is_char() || obj.is_varchar()
             || obj.is_text() || ob_is_nstring_type(obj.get_type())
             || obj.is_json()) {
    //if is_plain is false, 'j' will be print as "j\0" (with Quotation Marks here)
    //otherwise. as j\0 (withOUT Quotation Marks here)
    ObHexEscapeSqlStr sql_str(obj.get_string());
    len = len_of_string + (is_plain ? 1 : (3 + sql_str.get_extra_length()));
    if (ObCharset::charset_type_by_coll(print_param.cs_type_)
        != ObCharset::charset_type_by_coll(obj.get_collation_type())) {
      len += len_of_string * 4;
    }
    
    if (obj.is_json()) {
      // json add quote for stringbegin and end
      len += 2;
    }
  } else if (obj.is_binary() || obj.is_varbinary() || obj.is_hex_string() || obj.is_blob()) {
    //if is_plain is false, 'j' will be print as "X'6a'\0" (With Quotation Marks Here)
    //otherwise. as X'6a'\0 (Without Quotation Marks Here)
    len = 2 * len_of_string + (is_plain ? 4 : 6);
  } else if (!obj.is_lob_locator()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj type unexpected", K(obj), K(is_plain));
  } else if (OB_ISNULL(locator = obj.get_lob_locator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null lob locator", K(ret), K(obj));
  } else if (obj.is_blob_locator()) {
    if (OB_UNLIKELY(!locator->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected lob locator", K(ret), K(obj), KPC(locator));
    } else {
      len = locator->get_total_size() * 2 + (is_plain ? 4 : 6);
    }
  } else if (obj.is_clob_locator()) {
    ObString payload;
    if (OB_UNLIKELY(!locator->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected lob locator", K(ret), K(obj), K(locator));
    } else if (OB_FAIL(locator->get_payload(payload))) {
      LOG_WARN("Failed to get lob payload from locator", K(obj), KPC(locator));
    } else {
      ObHexEscapeSqlStr sql_str(payload);
      len = locator->get_total_size() + (is_plain ? 1 : (3 + sql_str.get_extra_length()));
      if (CHARSET_UTF8MB4 != ObCharset::charset_type_by_coll(obj.get_collation_type())) {
        len += locator->get_data_length();
      }
    }
  }
  return ret;
}

} // end of namespace observer
} // end of namespace oceanbase
