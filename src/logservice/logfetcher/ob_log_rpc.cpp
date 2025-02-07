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

#define USING_LOG_PREFIX  OBLOG

#include "ob_log_rpc.h"
#include "ob_log_trace_id.h"

#include "lib/utility/ob_macro_utils.h"   // OB_FAIL
#include "lib/oblog/ob_log_module.h"      // LOG_ERROR

#include "ob_log_config.h"                // ObLogFetcherConfig
#include "observer/ob_srv_network_frame.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_encrypt_kms.h"         // ObSSLClient
#endif
#include "logservice/data_dictionary/ob_data_dict_utils.h"

/// The rpc proxy executes the RPC function with two error codes:
/// 1. proxy function return value ret
/// 2. the result code carried by the proxy itself: proxy.get_result_code().rcode_, which indicates the error code returned by RPC processing on the target server
///
/// The two error codes above are related.
/// 1. Synchronous RPC
///   + on success of ret, the result code must be OB_SUCCESS
///   + ret failure, result code failure means that the RPC failed to process on the target machine and returned the packet, including process processing failure, tenant not present, etc.
/// result code success means local RPC delivery failed, or the remote server machine is unresponsive for a long time, no packet return, etc.

/// 2. Asynchronous RPC
///   + result code returned by proxy is meaningless because RPC is executed asynchronously and does not wait for packets to be returned to set the result code
///   + ret failure only means that the local RPC framework sent an error, excluding the case of no packet return from the target server
///
/// Based on the above analysis, for the caller sending the RPC, only the ret return value is of concern, and ret can completely replace the result code

#define SEND_RPC(RPC, tenant_id, SVR, TIMEOUT, REQ, ARG) \
    do { \
      if (IS_NOT_INIT) { \
        ret = OB_NOT_INIT; \
        LOG_ERROR("ObLogRpc not init", KR(ret), K(tenant_id)); \
      } else { \
        obrpc::ObCdcProxy proxy; \
        if (OB_FAIL(net_client_.get_proxy(proxy))) { \
          LOG_ERROR("net client get proxy fail", KR(ret)); \
        } else {\
          int64_t max_rpc_proc_time = \
                  ATOMIC_LOAD(&ObLogRpc::g_rpc_process_handler_time_upper_limit); \
          proxy.set_server((SVR)); \
          if (OB_FAIL(proxy.dst_cluster_id(cluster_id_).by(tenant_id).group_id(share::OBCG_CDCSERVICE) \
                                  .compressed(ATOMIC_LOAD(&compressor_type_)) \
                                  .trace_time(true).timeout((TIMEOUT))\
                                  .max_process_handler_time(static_cast<int32_t>(max_rpc_proc_time))\
                                  .RPC((REQ), (ARG)))) { \
            LOG_ERROR("rpc fail: " #RPC, "tenant_id", tenant_id, "svr", (SVR), "rpc_ret", ret, \
                "result_code", proxy.get_result_code().rcode_, "req", (REQ)); \
          } \
        } \
      } \
    } while(0)

using namespace oceanbase::common;
using namespace oceanbase::obrpc;

namespace oceanbase
{
namespace logfetcher
{

int64_t ObLogRpc::g_rpc_process_handler_time_upper_limit =
    ObLogFetcherConfig::default_rpc_process_handler_time_upper_limit_msec * _MSEC_;

ObLogRpc::ObLogRpc() :
    is_inited_(false),
    cluster_id_(OB_INVALID_CLUSTER_ID),
    self_tenant_id_(OB_INVALID_TENANT_ID),
    client_type_(obrpc::ObCdcClientType::CLIENT_TYPE_UNKNOWN),
    net_client_(),
    last_ssl_info_hash_(UINT64_MAX),
    ssl_key_expired_time_(0),
    client_id_(),
    cfg_(nullptr),
    external_info_val_(),
    compressor_type_(common::INVALID_COMPRESSOR)
{
  external_info_val_[0] = '\0';
}

ObLogRpc::~ObLogRpc()
{
  destroy();
}

// #ifdef ERRSIM
ERRSIM_POINT_DEF(ERRSIM_FETCH_LOG_TIME_OUT);
// #endif

int ObLogRpc::req_start_lsn_by_tstamp(const uint64_t tenant_id,
    const common::ObAddr &svr,
    obrpc::ObCdcReqStartLSNByTsReq &req,
    obrpc::ObCdcReqStartLSNByTsResp &resp,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  req.set_client_id(client_id_);
  if (1 == cfg_->test_mode_force_fetch_archive) {
    req.set_flag(ObCdcRpcTestFlag::OBCDC_RPC_FETCH_ARCHIVE);
  }
  SEND_RPC(req_start_lsn_by_ts, tenant_id, svr, timeout, req, resp);
  LOG_INFO("rpc: request start LSN by tstamp", KR(ret), K(tenant_id), K(svr), K(timeout), K(req), K(resp));
  return ret;
}

int ObLogRpc::async_stream_fetch_log(const uint64_t tenant_id,
    const common::ObAddr &svr,
    obrpc::ObCdcLSFetchLogReq &req,
    obrpc::ObCdcProxy::AsyncCB<obrpc::OB_LS_FETCH_LOG2> &cb,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  req.set_client_id(client_id_);
  req.set_tenant_id(self_tenant_id_);
  req.set_client_type(client_type_);
  if (1 == cfg_->test_mode_force_fetch_archive) {
    req.set_flag(ObCdcRpcTestFlag::OBCDC_RPC_FETCH_ARCHIVE);
  }
  if (1 == cfg_->test_mode_switch_fetch_mode) {
    req.set_flag(ObCdcRpcTestFlag::OBCDC_RPC_TEST_SWITCH_MODE);
  }
  req.set_compressor_type(ATOMIC_LOAD(&compressor_type_));
// #ifdef ERRSIM
  if (OB_SUCCESS != ERRSIM_FETCH_LOG_TIME_OUT &&
    OB_TIMEOUT != ERRSIM_FETCH_LOG_TIME_OUT) {
    ret = ERRSIM_FETCH_LOG_TIME_OUT;
  } else if (OB_TIMEOUT == ERRSIM_FETCH_LOG_TIME_OUT) {
    SEND_RPC(async_stream_fetch_log, tenant_id, svr, 1, req, &cb);
  } else {
// #endif
  SEND_RPC(async_stream_fetch_log, tenant_id, svr, timeout, req, &cb);
  LOG_TRACE("rpc: async fetch stream log", KR(ret), K(svr), K(timeout), K(req));
// #ifdef ERRSIM
  }
// #endif

  return ret;
}

int ObLogRpc::async_stream_fetch_missing_log(const uint64_t tenant_id,
    const common::ObAddr &svr,
    obrpc::ObCdcLSFetchMissLogReq &req,
    obrpc::ObCdcProxy::AsyncCB<obrpc::OB_LS_FETCH_MISSING_LOG> &cb,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  req.set_client_id(client_id_);
  req.set_tenant_id(self_tenant_id_);
  if (1 == cfg_->test_mode_force_fetch_archive) {
    req.set_flag(ObCdcRpcTestFlag::OBCDC_RPC_FETCH_ARCHIVE);
  }
  req.set_compressor_type(ATOMIC_LOAD(&compressor_type_));
  SEND_RPC(async_stream_fetch_miss_log, tenant_id, svr, timeout, req, &cb);
  LOG_TRACE("rpc: async fetch stream missing_log", KR(ret), K(svr), K(timeout), K(req));
  return ret;
}

int ObLogRpc::async_stream_fetch_raw_log(const uint64_t tenant_id,
    const common::ObAddr &svr,
    obrpc::ObCdcFetchRawLogReq &req,
    obrpc::ObCdcProxy::AsyncCB<obrpc::OB_CDC_FETCH_RAW_LOG> &cb,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  req.set_client_id(client_id_);
  req.set_tenant_id(self_tenant_id_);
  req.set_client_type(client_type_);
  if (1 == cfg_->test_mode_force_fetch_archive) {
    req.set_flag(ObCdcRpcTestFlag::OBCDC_RPC_FETCH_ARCHIVE);
  }
  req.set_compressor_type(ATOMIC_LOAD(&compressor_type_));
// #ifdef ERRSIM
  if (OB_SUCCESS != ERRSIM_FETCH_LOG_TIME_OUT &&
    OB_TIMEOUT != ERRSIM_FETCH_LOG_TIME_OUT) {
    ret = ERRSIM_FETCH_LOG_TIME_OUT;
  } else if (OB_TIMEOUT == ERRSIM_FETCH_LOG_TIME_OUT) {
    SEND_RPC(async_stream_fetch_raw_log, tenant_id, svr, 1, req, &cb);
  } else {
// #endif
  SEND_RPC(async_stream_fetch_raw_log, tenant_id, svr, timeout, req, &cb);
  LOG_TRACE("rpc: async stream fetch raw log", KR(ret), K(svr), K(timeout), K(req));
// #ifdef ERRSIM
  }
// #endif

  return ret;
}

int ObLogRpc::init(
    const int64_t cluster_id,
    const uint64_t self_tenant_id,
    const obrpc::ObCdcClientType client_type,
    const int64_t io_thread_num,
    const ObLogFetcherConfig &cfg)
{
  int ret = OB_SUCCESS;
  rpc::frame::ObNetOptions opt;
  opt.rpc_io_cnt_ = static_cast<int>(io_thread_num);
  opt.mysql_io_cnt_ = 0;
  // First set cfg
  cfg_ = &cfg;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogRpc init twice", KR(ret));
  } else if (OB_UNLIKELY(io_thread_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(io_thread_num));
  } else if (OB_FAIL(init_client_id_())) {
    LOG_ERROR("init client identity failed", KR(ret));
  } else if (OB_FAIL(net_client_.init(opt))) {
    LOG_ERROR("init net client fail", KR(ret), K(io_thread_num));
  } else if (OB_FAIL(reload_ssl_config())) {
    LOG_ERROR("reload_ssl_config succ", KR(ret));
  } else {
    cluster_id_ = cluster_id;
    self_tenant_id_ = self_tenant_id;
    client_type_ = client_type;
    is_inited_ = true;
    LOG_INFO("init ObLogRpc succ", K(cluster_id), K(io_thread_num));
  }

  return ret;
}

void ObLogRpc::destroy()
{
  is_inited_ = false;
  cluster_id_ = OB_INVALID_CLUSTER_ID;
  self_tenant_id_ = OB_INVALID_TENANT_ID;
  client_type_ = obrpc::ObCdcClientType::CLIENT_TYPE_UNKNOWN;
  net_client_.destroy();
  last_ssl_info_hash_ = UINT64_MAX;
  ssl_key_expired_time_ = 0;
  client_id_.reset();
  cfg_ = nullptr;
  external_info_val_[0] = '\0';
  compressor_type_ = common::INVALID_COMPRESSOR;
}

int ObLogRpc::reload_ssl_config()
{
  int ret = OB_SUCCESS;
  const bool enable_ssl_client_authentication = (1 == cfg_->ssl_client_authentication);
  const char *ssl_ext_kms_info_conf = cfg_->ssl_external_kms_info.str();
  const bool is_local_file_mode = (0 == strcmp("file", ssl_ext_kms_info_conf));
  const char *ssl_external_kms_info = NULL;
  external_info_val_[0] = '\0';

  if (enable_ssl_client_authentication) {
    if (is_local_file_mode) {
      ssl_external_kms_info = ssl_ext_kms_info_conf;
    } else {
      if (OB_FAIL(common::hex_to_cstr(ssl_ext_kms_info_conf, strlen(ssl_ext_kms_info_conf),
              external_info_val_, OB_MAX_CONFIG_VALUE_LEN))) {
        LOG_ERROR("fail to hex to cstr", KR(ret));
      } else {
        ssl_external_kms_info = external_info_val_;
      }
    }

    if (OB_SUCC(ret)) {
      ObString ssl_config(ssl_external_kms_info);

      bool file_exist = false;
      const char *intl_file[3] = {OB_CLIENT_SSL_CA_FILE, OB_CLIENT_SSL_CERT_FILE, OB_CLIENT_SSL_KEY_FILE};
      const char *sm_file[5] = {NULL, NULL, NULL, NULL, NULL};
      const uint64_t new_hash_value = is_local_file_mode
        ? observer::ObSrvNetworkFrame::get_ssl_file_hash(intl_file, sm_file, file_exist)
        : ssl_config.hash();

      if (ssl_config.empty() && ! file_exist) {
        LOG_ERROR("ssl file not available", K(new_hash_value));
        ret = OB_INVALID_CONFIG;
      } else if (last_ssl_info_hash_ == new_hash_value) {
        LOG_INFO("no need reload_ssl_config", K(new_hash_value));
      } else {
        bool use_bkmi = false;
        bool use_sm = false;
        const char *ca_cert = NULL;
        const char *public_cert = NULL;
        const char *private_key = NULL;
        int64_t ssl_key_expired_time = 0;

        if (is_local_file_mode) {
          if (EASY_OK != easy_ssl_ob_config_check(OB_CLIENT_SSL_CA_FILE, OB_CLIENT_SSL_CERT_FILE,
                OB_CLIENT_SSL_KEY_FILE, NULL, NULL, true/* is_from_file */, false/* is_babassl */)) {
            LOG_ERROR("Local file mode: key and cert not match", KR(ret));
            ret = OB_INVALID_CONFIG;
          } else if (OB_FAIL(observer::ObSrvNetworkFrame::extract_expired_time(OB_CLIENT_SSL_CERT_FILE, ssl_key_expired_time))) {
            LOG_ERROR("extract_expired_time failed", KR(ret), K(use_bkmi));
          } else {
            ca_cert = OB_CLIENT_SSL_CA_FILE;
            public_cert = OB_CLIENT_SSL_CERT_FILE;
            private_key = OB_CLIENT_SSL_KEY_FILE;
          }
        } else {
#ifndef OB_BUILD_TDE_SECURITY
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("only support local file mode", K(ret));
#else
          share::ObSSLClient client;

          if (OB_FAIL(client.init(ssl_config.ptr(), ssl_config.length()))) {
            OB_LOG(WARN, "kms client init", K(ret), K(ssl_config));
          } else if (OB_FAIL(client.check_param_valid())) {
            OB_LOG(WARN, "kms client param is not valid", K(ret));
          } else {
            use_bkmi = client.is_bkmi_mode();
            use_sm = client.is_sm_scene();
            ca_cert = client.get_root_ca().ptr();
            public_cert = client.public_cert_.content_.ptr();
            private_key = client.private_key_.content_.ptr();
            ssl_key_expired_time = client.public_cert_.key_expired_time_;
          }
#endif
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(net_client_.load_ssl_config(use_bkmi, use_sm, ca_cert,
                  public_cert, private_key, NULL, NULL))) {
            LOG_ERROR("ObNetClient load_ssl_config failed", KR(ret), K(use_bkmi), K(use_sm));
          } else {
            last_ssl_info_hash_ = new_hash_value;
            ssl_key_expired_time_ = ssl_key_expired_time;
            LOG_INFO("finish reload_ssl_config", K(use_bkmi), K(use_sm), K(new_hash_value), K(ssl_key_expired_time_));
          }
        }
      }
    }
  } else {
    last_ssl_info_hash_ = UINT64_MAX;
    ssl_key_expired_time_ = 0;

    LOG_INFO("reload_ssl_config: SSL is closed");
  }

  return ret;
}

void ObLogRpc::configure(const ObLogFetcherConfig &cfg)
{
  int64_t rpc_process_handler_time_upper_limit_msec = cfg.rpc_process_handler_time_upper_limit_msec;

  ATOMIC_STORE(&g_rpc_process_handler_time_upper_limit,
      rpc_process_handler_time_upper_limit_msec * _MSEC_);
  LOG_INFO("[CONFIG]", K(rpc_process_handler_time_upper_limit_msec));
}

int ObLogRpc::update_compressor_type(const common::ObCompressorType &compressor_type)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogRpc is not inited", KR(ret), K(cluster_id_));
  } else {
    ATOMIC_SET(&compressor_type_, compressor_type);

    if (REACH_TIME_INTERVAL_THREAD_LOCAL(10 * _SEC_)) {
      const char *compressor_type_name = nullptr;

      if (compressor_type_ < common::MAX_COMPRESSOR) {
        compressor_type_name = common::all_compressor_name[compressor_type];
      }
      LOG_INFO("update compressor type success", K_(compressor_type), K(compressor_type_name));
    }
  }

  return ret;
}

int ObLogRpc::init_client_id_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(client_id_.init(getpid(), get_self_addr()))) {
    LOG_ERROR("init client id failed", KR(ret));
  }

  return ret;
}

}
}
