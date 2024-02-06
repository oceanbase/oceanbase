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
#include "io/easy_maccept.h"
#include "observer/ob_srv_network_frame.h"
#include "rpc/obmysql/ob_sql_nio_server.h"
#include "observer/mysql/obsm_conn_callback.h"
#include "rpc/obrpc/ob_poc_rpc_server.h"
#include "src/share/rc/ob_tenant_base.h"

#include "share/config/ob_server_config.h"
#include "share/ob_rpc_share.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_rpc_intrusion_detect.h"
#include "storage/ob_locality_manager.h"
#include "lib/ssl/ob_ssl_config.h"
extern "C" {
#include "ussl-hook.h"
#include "auth-methods.h"
}
#include <sys/types.h>
#include <sys/stat.h>
#include "storage/ob_locality_manager.h"
#ifdef OB_USE_BABASSL
#include "share/ob_encrypt_kms.h"
#endif

using namespace oceanbase::rpc::frame;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::share;
using namespace oceanbase::obmysql;

ObSrvNetworkFrame::ObSrvNetworkFrame(ObGlobalContext &gctx)
    : gctx_(gctx),
      xlator_(gctx),
      request_qhandler_(xlator_),
      deliver_(request_qhandler_, xlator_.get_session_handler(), gctx),
      rpc_handler_(deliver_),
      mysql_handler_(deliver_, gctx),
      ingress_service_(),
      net_(),
      rpc_transport_(NULL),
      high_prio_rpc_transport_(NULL),
      mysql_transport_(NULL),
      batch_rpc_transport_(NULL),
      last_ssl_info_hash_(UINT64_MAX),
      standby_fetchlog_bw_limit_(0),
      standby_fetchlog_bytes_(0),
      standby_fetchlog_time_(0)
{
  // empty;
}

ObSrvNetworkFrame::~ObSrvNetworkFrame()
{
  // empty
}

static bool enable_new_sql_nio()
{
  return GCONF._enable_new_sql_nio;
}

static int update_tcp_keepalive_parameters_for_sql_nio_server(int tcp_keepalive_enabled, int64_t tcp_keepidle, int64_t tcp_keepintvl, int64_t tcp_keepcnt)
{
  int ret = OB_SUCCESS;
  if (enable_new_sql_nio()) {
    if (NULL != global_sql_nio_server) {
      tcp_keepidle = max(tcp_keepidle/1000000, 1);
      tcp_keepintvl = max(tcp_keepintvl/1000000, 1);
      global_sql_nio_server->update_tcp_keepalive_params(tcp_keepalive_enabled, tcp_keepidle, tcp_keepintvl, tcp_keepcnt);
    }
  }
  return ret;
}

int ObSrvNetworkFrame::init()
{
  int ret = OB_SUCCESS;
  const char* mysql_unix_path = "unix:run/sql.sock";
  const char* rpc_unix_path = "unix:run/rpc.sock";
  const uint32_t rpc_port = static_cast<uint32_t>(GCONF.rpc_port);
  ObNetOptions opts;
  int io_cnt = static_cast<int>(GCONF.net_thread_count);
  // make net thread count adaptive
  if (0 == io_cnt) {
    io_cnt = get_default_net_thread_count();
  }
  const int hp_io_cnt = static_cast<int>(GCONF.high_priority_net_thread_count);
  uint8_t negotiation_enable = 0;
  opts.rpc_io_cnt_ = io_cnt;
  opts.high_prio_rpc_io_cnt_ = hp_io_cnt;
  opts.mysql_io_cnt_ = io_cnt;
  opts.batch_rpc_io_cnt_ = io_cnt;
  opts.use_ipv6_ = GCONF.use_ipv6;
  //TODO(tony.wzh): fix opts.tcp_keepidle  negative
  opts.tcp_user_timeout_ = static_cast<int>(GCONF.dead_socket_detection_timeout);
  opts.tcp_keepidle_     = static_cast<int>(GCONF.tcp_keepidle);
  opts.tcp_keepintvl_    = static_cast<int>(GCONF.tcp_keepintvl);
  opts.tcp_keepcnt_      = static_cast<int>(GCONF.tcp_keepcnt);

  if (GCONF.enable_tcp_keepalive) {
    opts.enable_tcp_keepalive_ = 1;
  } else {
    opts.enable_tcp_keepalive_ = 0;
  }
  LOG_INFO("io thread connection negotiation enabled!");
  negotiation_enable = 1;

  deliver_.set_host(gctx_.self_addr());

  if (OB_FAIL(request_qhandler_.init())) {
    LOG_ERROR("init rpc request qhandler fail", K(ret));

  } else if (OB_FAIL(deliver_.init())) {
    LOG_ERROR("init rpc deliverer fail", K(ret));

  } else if (OB_FAIL(rpc_handler_.init())) {
    LOG_ERROR("init rpc packet handler fail", K(ret));

  } else if (OB_FAIL(ob_rpc_intrusion_detect_patch(
                         rpc_handler_.ez_handler(),
                         gctx_.locality_manager_))) {
    LOG_ERROR("easy_handler_security_patch fail", K(ret));
  } else if (OB_FAIL(net_.init(opts, negotiation_enable))) {
    LOG_ERROR("init rpc easy network fail", K(ret));
  } else if (OB_FAIL(reload_ssl_config())) {
    LOG_ERROR("load_ssl_config fail", K(ret));
  } else if (OB_FAIL(reload_rpc_auth_method())) {
    LOG_ERROR("load rpc auth method fail", K(ret));
  } else if (false == enable_new_sql_nio() && OB_FAIL(net_.add_mysql_listen(static_cast<uint32_t>(GCONF.mysql_port),
                                           mysql_handler_,
                                           mysql_transport_))) {
    LOG_ERROR("listen mysql port fail", "mysql_port", GCONF.mysql_port.get_value(), K(ret));
  } else if (OB_FAIL(net_.add_mysql_unix_listen(mysql_unix_path, mysql_handler_))) {
    LOG_ERROR("listen mysql unix path fail", "mysql_port", GCONF.mysql_port.get_value(), K(ret));
  } else if (OB_FAIL(net_.set_rpc_port(rpc_port))) {
    LOG_ERROR("neteasy set rpc port fail", K(ret));
  } else if (OB_FAIL(net_.rpc_net_register(rpc_handler_, rpc_transport_))) {
    LOG_ERROR("rpc net register fail", K(ret));
  } else if (OB_FAIL(net_.batch_rpc_net_register(rpc_handler_, batch_rpc_transport_))) {
    LOG_ERROR("batch rpc net register fail", K(ret));
  } else if (OB_FAIL(net_.net_keepalive_register())) {
    LOG_ERROR("net keepalive register fail", K(ret));
  } else if (hp_io_cnt > 0 && OB_FAIL(net_.high_prio_rpc_net_register(rpc_handler_, high_prio_rpc_transport_))) {
    LOG_ERROR("high prio rpc net register fail", K(ret));
  } else if (ingress_service_.init(GCONF.cluster_id)) {
    LOG_ERROR("endpoint ingress service init fail", K(ret));
  }
#ifdef OB_USE_BABASSL
  else if (OB_FAIL(net_.add_rpc_unix_listen(rpc_unix_path, rpc_handler_))) {
    LOG_ERROR("listen rpc unix path fail");
  }
#endif
  else {
    if (OB_FAIL(obrpc::global_poc_server.start(rpc_port, io_cnt, &deliver_))) {
      LOG_ERROR("poc rpc server start fail", K(ret));
    } else {
      LOG_INFO("poc rpc server start successfully");
    }
    share::set_obrpc_transport(rpc_transport_);
    batch_rpc_transport_->set_bucket_count(opts.batch_rpc_io_cnt_);
    LOG_INFO("init rpc network frame successfully",
             "ssl_client_authentication", GCONF.ssl_client_authentication.str());
  }
  return ret;
}

void ObSrvNetworkFrame::destroy()
{
  net_.destroy();
  ObNetKeepAlive::get_instance().destroy();
  ingress_service_.destroy();
  if (NULL != obmysql::global_sql_nio_server) {
    obmysql::global_sql_nio_server->destroy();
  }
}

int ObSrvNetworkFrame::start()
{
  int ret = net_.start();

  if (OB_SUCC(ret) && OB_FAIL(ObNetKeepAlive::get_instance().start())) {
    LOG_ERROR("ObNetKeepAlive start failed!", K(ret));
  } else {
    LOG_INFO("ObNetKeepAlive start success!");
  }

  if (OB_SUCC(ret)) {
    if (enable_new_sql_nio()) {
      obmysql::global_sql_nio_server =
          OB_NEW(obmysql::ObSqlNioServer, "SqlNio",
                 obmysql::global_sm_conn_callback, mysql_handler_);
      if (NULL == obmysql::global_sql_nio_server) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("allocate memory for global_sql_nio_server failed", K(ret));
      } else {
        int sql_net_thread_count = (int)GCONF.sql_net_thread_count;
        if (sql_net_thread_count == 0) {
          if (GCONF.net_thread_count == 0) {
            sql_net_thread_count = get_default_net_thread_count();
          } else {
            sql_net_thread_count = GCONF.net_thread_count;
          }
        }
        if (OB_FAIL(obmysql::global_sql_nio_server->start(
                GCONF.mysql_port, &deliver_, sql_net_thread_count))) {
          LOG_ERROR("sql nio server start failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ingress_service_.start())) {
      LOG_ERROR("endpoint register task start fail", K(ret));
    }
  }
  return ret;
}


int ObSrvNetworkFrame::reload_config()
{
  int ret = common::OB_SUCCESS;
  int enable_easy_keepalive = 0;
  int enable_tcp_keepalive  = 0;
  int32_t tcp_keepidle      = static_cast<int>(GCONF.tcp_keepidle);
  int32_t tcp_keepintvl     = static_cast<int>(GCONF.tcp_keepintvl);
  int32_t tcp_keepcnt       = static_cast<int>(GCONF.tcp_keepcnt);
  int32_t user_timeout      = static_cast<int>(GCONF.dead_socket_detection_timeout);

  if (GCONF._enable_easy_keepalive) {
    enable_easy_keepalive = 1;
    LOG_INFO("easy keepalive enabled.");
  } else {
    LOG_INFO("easy keepalive disabled.");
  }

  if (GCONF.enable_tcp_keepalive) {
    enable_tcp_keepalive = 1;
    LOG_INFO("tcp keepalive enabled.");
  } else {
    LOG_INFO("tcp keepalive disabled.");
  }

  if (OB_FAIL(net_.set_easy_keepalive(enable_easy_keepalive))) {
    LOG_WARN("Failed to set easy keepalive.");
  } else if (OB_FAIL(net_.update_rpc_tcp_keepalive_params(user_timeout))) {
    LOG_WARN("Failed to set rpc tcp keepalive parameters.");
  } else if (OB_FAIL(obrpc::global_poc_server.update_tcp_keepalive_params(user_timeout))) {
    LOG_WARN("Failed to set pkt-nio rpc tcp keepalive parameters.");
  } else if (OB_FAIL(net_.update_sql_tcp_keepalive_params(user_timeout, enable_tcp_keepalive,
                                                          tcp_keepidle, tcp_keepintvl,
                                                          tcp_keepcnt))) {
    LOG_WARN("Failed to set sql tcp keepalive parameters.");
  } else if (OB_FAIL(update_tcp_keepalive_parameters_for_sql_nio_server(enable_tcp_keepalive,
                                                                        tcp_keepidle, tcp_keepintvl,
                                                                        tcp_keepcnt))) {
    LOG_WARN("Failed to set sql tcp keepalive parameters for sql nio server", K(ret));
  }
  return ret;
}

int ObSrvNetworkFrame::extract_expired_time(const char *const cert_file, int64_t &expired_time)
{
  int ret = OB_SUCCESS;
  X509 *cert = NULL;
  BIO *b = NULL;
  if (OB_ISNULL(b = BIO_new_file(cert_file, "r"))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "BIO_new_file failed", K(ret), K(cert_file));
  } else if (OB_ISNULL(cert = PEM_read_bio_X509(b, NULL, 0, NULL))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "PEM_read_bio_X509 failed", K(ret), K(cert_file));
  } else {
    ASN1_TIME *notAfter = X509_get_notAfter(cert);
    struct tm tm1;
    memset (&tm1, 0, sizeof (tm1));
    tm1.tm_year = (notAfter->data[ 0] - '0') * 10 + (notAfter->data[ 1] - '0') + 100;
    tm1.tm_mon  = (notAfter->data[ 2] - '0') * 10 + (notAfter->data[ 3] - '0') - 1;
    tm1.tm_mday = (notAfter->data[ 4] - '0') * 10 + (notAfter->data[ 5] - '0');
    tm1.tm_hour = (notAfter->data[ 6] - '0') * 10 + (notAfter->data[ 7] - '0');
    tm1.tm_min  = (notAfter->data[ 8] - '0') * 10 + (notAfter->data[ 9] - '0');
    tm1.tm_sec  = (notAfter->data[10] - '0') * 10 + (notAfter->data[11] - '0');
    expired_time = mktime(&tm1) * 1000000;//us
  }

  if (NULL != cert) {
    X509_free(cert);
  }
  if (NULL != b) {
    BIO_free(b);
  }
  return ret;
}

uint64_t ObSrvNetworkFrame::get_ssl_file_hash(const char *intl_file[3], const char *sm_file[5], bool &file_exist)
{
  file_exist = false;
  uint64_t hash_value = 0;
  struct stat tmp_buf[5];

  if (0 == stat(intl_file[0], tmp_buf + 0)
      && 0 == stat(intl_file[1], tmp_buf + 1)
      && 0 == stat(intl_file[2], tmp_buf + 2)) {
    file_exist = true;
    hash_value = murmurhash(&(tmp_buf[0].st_mtime), sizeof(tmp_buf[0].st_mtime), hash_value);
    hash_value = murmurhash(&(tmp_buf[1].st_mtime), sizeof(tmp_buf[1].st_mtime), hash_value);
    hash_value = murmurhash(&(tmp_buf[2].st_mtime), sizeof(tmp_buf[2].st_mtime), hash_value);
  }

  if (!file_exist) {
    if (0 == stat(sm_file[0], tmp_buf + 0)
      && 0 == stat(sm_file[1], tmp_buf + 1)
      && 0 == stat(sm_file[2], tmp_buf + 2)
      && 0 == stat(sm_file[3], tmp_buf + 3)
      && 0 == stat(sm_file[4], tmp_buf + 4)) {
      file_exist = true;
      hash_value = murmurhash(&(tmp_buf[0].st_mtime), sizeof(tmp_buf[0].st_mtime), hash_value);
      hash_value = murmurhash(&(tmp_buf[1].st_mtime), sizeof(tmp_buf[1].st_mtime), hash_value);
      hash_value = murmurhash(&(tmp_buf[2].st_mtime), sizeof(tmp_buf[2].st_mtime), hash_value);
      hash_value = murmurhash(&(tmp_buf[3].st_mtime), sizeof(tmp_buf[3].st_mtime), hash_value);
      hash_value = murmurhash(&(tmp_buf[4].st_mtime), sizeof(tmp_buf[4].st_mtime), hash_value);
    }
  } else {
    if (0 == stat(sm_file[0], tmp_buf + 0)
      && 0 == stat(sm_file[1], tmp_buf + 1)
      && 0 == stat(sm_file[2], tmp_buf + 2)
      && 0 == stat(sm_file[3], tmp_buf + 3)
      && 0 == stat(sm_file[4], tmp_buf + 4)) {
      file_exist = true;
      hash_value = murmurhash(&(tmp_buf[0].st_mtime), sizeof(tmp_buf[0].st_mtime), hash_value);
      hash_value = murmurhash(&(tmp_buf[1].st_mtime), sizeof(tmp_buf[1].st_mtime), hash_value);
      hash_value = murmurhash(&(tmp_buf[2].st_mtime), sizeof(tmp_buf[2].st_mtime), hash_value);
      hash_value = murmurhash(&(tmp_buf[3].st_mtime), sizeof(tmp_buf[3].st_mtime), hash_value);
      hash_value = murmurhash(&(tmp_buf[4].st_mtime), sizeof(tmp_buf[4].st_mtime), hash_value);
    }
  }

  return hash_value;
}

static int create_ssl_ctx(int ctx_id, int is_from_file, int is_sm, const char *ca_cert,
                          const char *sign_cert, const char *sign_private_key,
                          const char *enc_cert, const char *enc_private_key)
{
  int ret = OB_SUCCESS;
  ssl_config_item_t config_item;
  config_item.is_from_file = is_from_file;
  config_item.is_sm = is_sm;
  config_item.ca_cert = ca_cert;
  config_item.sign_cert = sign_cert;
  config_item.sign_private_key = sign_private_key;
  config_item.enc_cert = enc_cert;
  config_item.enc_private_key = enc_private_key;
  if (ussl_setsockopt(ctx_id, SOL_OB_CTX, SO_OB_CTX_SET_SSL_CONFIG, &config_item, socklen_t(sizeof(config_item))) < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("create ssl ctx failed", K(ctx_id));
  } else {
    LOG_INFO("create ssl ctx success", K(ctx_id));
  }
  return ret;
}

int ObSrvNetworkFrame::reload_ssl_config()
{
  int ret = common::OB_SUCCESS;
  if (GCONF.ssl_client_authentication) {
    ObString invited_nodes(GCONF._ob_ssl_invited_nodes.str());

    ObString ssl_config(GCONF.ssl_external_kms_info.str());
    bool file_exist = false;
    const char *intl_file[3] = {OB_SSL_CA_FILE, OB_SSL_CERT_FILE, OB_SSL_KEY_FILE};
    const char *sm_file[5] = {OB_SSL_CA_FILE, OB_SSL_SM_SIGN_CERT_FILE, OB_SSL_SM_SIGN_KEY_FILE, OB_SSL_SM_ENC_CERT_FILE,
    OB_SSL_SM_ENC_KEY_FILE};
    const uint64_t new_hash_value = ssl_config.empty()
        ? get_ssl_file_hash(intl_file, sm_file, file_exist)
        : ssl_config.hash();
    if (ssl_config.empty() && !file_exist) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("ssl file not available", K(new_hash_value));
      LOG_USER_ERROR(OB_INVALID_CONFIG, "ssl file not available");
    } else if (OB_ISNULL(gctx_.locality_manager_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("locality manager should not be null", K(ret), KP(gctx_.locality_manager_));
    } else if (FALSE_IT(gctx_.locality_manager_->set_ssl_invited_nodes(invited_nodes))) {
    } else if (last_ssl_info_hash_ == new_hash_value) {
      LOG_INFO("no need reload_ssl_config", K(new_hash_value));
    } else {
      bool use_bkmi = false;
      bool use_sm = false;
      const char *ca_cert = NULL;
      const char *public_cert = NULL;
      const char *private_key = NULL;
      const char *enc_cert  = NULL;
      const char *enc_private_key = NULL;

#ifdef OB_USE_BABASSL
      share::ObSSLClient client;
      if (!ssl_config.empty()) {
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
          enc_cert = client.public_cert_for_enc_.content_.ptr();
          enc_private_key = client.private_key_for_enc_.content_.ptr();
        }
      }
#endif

      if (! use_bkmi) {
        if (!use_sm) {
          if (EASY_OK != easy_ssl_ob_config_check(OB_SSL_CA_FILE, OB_SSL_CERT_FILE,
                                                  OB_SSL_KEY_FILE, NULL, NULL, true, false)) {
            ret = OB_INVALID_CONFIG;
            LOG_WARN("key and cert not match", K(ret));
            LOG_USER_ERROR(OB_INVALID_CONFIG, "key and cert not match");
          } else {
            ca_cert = OB_SSL_CA_FILE;
            public_cert = OB_SSL_CERT_FILE;
            private_key = OB_SSL_KEY_FILE;
          }
#ifdef OB_USE_BABASSL
        } else {
          if (EASY_OK != easy_ssl_ob_config_check(OB_SSL_CA_FILE,
                                                   OB_SSL_SM_SIGN_CERT_FILE, OB_SSL_SM_SIGN_KEY_FILE,
                                                   OB_SSL_SM_ENC_CERT_FILE, OB_SSL_SM_ENC_KEY_FILE,
                                                   true, true)) {
            ret = OB_INVALID_CONFIG;
            LOG_WARN("key and cert not match", K(ret));
            LOG_USER_ERROR(OB_INVALID_CONFIG, "key and cert not match");
          } else {
            ca_cert = OB_SSL_CA_FILE;
            public_cert = OB_SSL_SM_SIGN_CERT_FILE;
            private_key = OB_SSL_SM_SIGN_KEY_FILE;
            enc_cert = OB_SSL_SM_ENC_CERT_FILE;
            enc_private_key = OB_SSL_SM_ENC_KEY_FILE;
          }
#endif
        }
      }

      if (OB_SUCC(ret)) {
        int64_t ssl_key_expired_time = 0;
        if (!use_bkmi && !use_sm &&OB_FAIL(extract_expired_time(OB_SSL_CERT_FILE, ssl_key_expired_time))) {
          OB_LOG(WARN, "extract_expired_time intl failed", K(ret), K(use_bkmi));
        }
        #ifdef OB_USE_BABASSL
          else if (!use_bkmi && use_sm && OB_FAIL(extract_expired_time(OB_SSL_SM_ENC_CERT_FILE, ssl_key_expired_time))) {
          OB_LOG(WARN, "extract_expired_time sm failed", K(ret), K(use_bkmi));
        }
        #endif
         else if (OB_FAIL(net_.load_ssl_config(use_bkmi, use_sm, ca_cert,
            public_cert, private_key, enc_cert, enc_private_key))) {
          OB_LOG(WARN, "load_ssl_config failed", K(ret), K(use_bkmi), K(use_sm));
        } else {
          mysql_handler_.ez_handler()->is_ssl = 1;
          mysql_handler_.ez_handler()->is_ssl_opt = 1;
          rpc_handler_.ez_handler()->is_ssl = 1;
          rpc_handler_.ez_handler()->is_ssl_opt = 0;
#ifndef OB_USE_BABASSL
          GCTX.ssl_key_expired_time_ =  ssl_key_expired_time;
#else
          if (use_bkmi) {
            GCTX.ssl_key_expired_time_ =  client.public_cert_.key_expired_time_;
          } else {
            GCTX.ssl_key_expired_time_ =  ssl_key_expired_time;
          }
#endif
          last_ssl_info_hash_ = new_hash_value;
          LOG_INFO("finish reload_ssl_config", K(use_bkmi), K(use_bkmi), K(use_sm),
                   "ssl_key_expired_time", GCTX.ssl_key_expired_time_, K(new_hash_value));
          if (OB_SUCC(ret)) {
            if (enable_new_sql_nio()) {
              common::ObSSLConfig ssl_config(!use_bkmi, use_sm, ca_cert, public_cert, private_key, NULL, NULL);
              if (OB_FAIL(ob_ssl_load_config(OB_SSL_CTX_ID_SQL_NIO, ssl_config))) {
                LOG_WARN("create ssl ctx failed!", K(ret));
              } else {
                LOG_INFO("create ssl ctx success!", K(use_bkmi), K(use_sm));
              }
            }
            if (OB_SUCC(ret)) {
              const int OB_EASY_RPC_SSL_CTX_ID = 0;
              if (OB_FAIL(create_ssl_ctx(OB_EASY_RPC_SSL_CTX_ID, !use_bkmi, use_sm,
                                          ca_cert, public_cert, private_key, NULL, NULL))) {
                LOG_ERROR("create ssl ctx failed", K(OB_EASY_RPC_SSL_CTX_ID));
              }
            }
          }
        }
      }
    }
  } else {
    last_ssl_info_hash_ = UINT64_MAX;
    mysql_handler_.ez_handler()->is_ssl = 0;
    mysql_handler_.ez_handler()->is_ssl_opt = 0;
    rpc_handler_.ez_handler()->is_ssl = 0;
    rpc_handler_.ez_handler()->is_ssl_opt = 0;
    GCTX.ssl_key_expired_time_ =  0;
    LOG_INFO("finish reload_ssl_config, close ssl");
  }
  return ret;
}

int ObSrvNetworkFrame::mysql_shutdown()
{
  return net_.mysql_shutdown();
}

int ObSrvNetworkFrame::rpc_shutdown()
{
  return net_.rpc_shutdown();
}

int ObSrvNetworkFrame::high_prio_rpc_shutdown()
{
  return net_.high_prio_rpc_shutdown();
}

int ObSrvNetworkFrame::batch_rpc_shutdown()
{
  return net_.batch_rpc_shutdown();
}

int ObSrvNetworkFrame::unix_rpc_shutdown()
{
  return net_.unix_rpc_shutdown();
}

void ObSrvNetworkFrame::wait()
{
  net_.wait();
  ObNetKeepAlive::get_instance().wait();
  ingress_service_.wait();
  if (NULL != obmysql::global_sql_nio_server) {
    obmysql::global_sql_nio_server->wait();
  }
  obrpc::global_poc_server.wait();
  ussl_wait();
}

int ObSrvNetworkFrame::stop()
{
  int ret = OB_SUCCESS;
  deliver_.stop();
  // stop easy network after deliver has been stopped.
  // easy.stop will release memory that deliver thread maybe in use.
  if (OB_FAIL(net_.stop())) {
    LOG_WARN("stop easy net fail", K(ret));
  } else {
    rpc_stop();
    ObNetKeepAlive::get_instance().stop();
  }
  ingress_service_.stop();
  return ret;
}

int ObSrvNetworkFrame::get_proxy(obrpc::ObRpcProxy &proxy)
{
  return proxy.init(rpc_transport_);
}

ObReqTransport *ObSrvNetworkFrame::get_req_transport()
{
  return rpc_transport_;
}

ObReqTransport *ObSrvNetworkFrame::get_high_prio_req_transport()
{
  return high_prio_rpc_transport_;
}

ObReqTransport *ObSrvNetworkFrame::get_batch_rpc_req_transport()
{
  return batch_rpc_transport_;
}

ObNetEasy *ObSrvNetworkFrame::get_net_easy()
{
  return &net_;
}

void ObSrvNetworkFrame::set_ratelimit_enable(int ratelimit_enabled)
{
  rpc_transport_->set_ratelimit_enable(ratelimit_enabled);
  batch_rpc_transport_->set_ratelimit_enable(ratelimit_enabled);
}

void ObSrvNetworkFrame::sql_nio_stop()
{
  if (NULL != obmysql::global_sql_nio_server) {
    obmysql::global_sql_nio_server->stop();
  }
}

void ObSrvNetworkFrame::rpc_stop()
{
  ussl_stop();
  obrpc::global_poc_server.stop();
}

int ObSrvNetworkFrame::reload_rpc_auth_method()
{
  int ret = OB_SUCCESS;
  int client_auth_method = USSL_AUTH_NONE;
  int server_auth_methods = 0;
  ObString client_auth_method_str(GCONF.rpc_client_authentication_method.str());
  ObString server_auth_method_str(GCONF.rpc_server_authentication_method.str());
  if (0 == client_auth_method_str.case_compare("NONE")) {
    client_auth_method = USSL_AUTH_NONE;
  } else if (0 == client_auth_method_str.case_compare("SSL_NO_ENCRYPT")) {
    client_auth_method = USSL_AUTH_SSL_HANDSHAKE;
  } else if (0 == client_auth_method_str.case_compare("SSL_IO")) {
    client_auth_method = USSL_AUTH_SSL_IO;
  }
  set_client_auth_methods(client_auth_method);

  if (NULL != strcasestr(server_auth_method_str.ptr(), "ALL")) {
    server_auth_methods = USSL_AUTH_NONE | USSL_AUTH_SSL_HANDSHAKE | USSL_AUTH_SSL_IO;
  } else {
    if (NULL != strcasestr(server_auth_method_str.ptr(), "SSL_NO_ENCRYPT")) {
      server_auth_methods |= USSL_AUTH_SSL_HANDSHAKE;
    }
    if (NULL != strcasestr(server_auth_method_str.ptr(), "SSL_IO")) {
      server_auth_methods |= USSL_AUTH_SSL_IO;
    }
    if (NULL != strcasestr(server_auth_method_str.ptr(), "NONE")) {
      server_auth_methods |= USSL_AUTH_NONE;
    }
  }
  set_server_auth_methods(server_auth_methods);
  return ret;
}

oceanbase::rootserver::ObIngressBWAllocService *ObSrvNetworkFrame::get_ingress_service()
{
  return &ingress_service_;
}
int ObSrvNetworkFrame::net_endpoint_register(const ObNetEndpointKey &endpoint_key, int64_t expire_time)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (!is_sys_tenant(tenant_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("endpoint register is only valid in sys tenant", K(ret), K(endpoint_key));
  } else if (ingress_service_.is_leader() && OB_FAIL(ingress_service_.register_endpoint(endpoint_key, expire_time))) {
    LOG_WARN("net endpoint register failed", K(ret), K(endpoint_key));
  }
  return ret;
}

int ObSrvNetworkFrame::net_endpoint_predict_ingress(const ObNetEndpointKey &endpoint_key, int64_t &predicted_bw)
{
  int ret = OB_SUCCESS;
  int64_t current_time = ObTimeUtility::current_time();
  uint64_t current_fetchlog_bytes = pn_get_rxbytes(obrpc::ObPocRpcServer::RATELIMIT_PNIO_GROUP);
  uint64_t peroid_bytes = current_fetchlog_bytes - standby_fetchlog_bytes_;
  int64_t real_bw = peroid_bytes * 1000000L / (current_time - standby_fetchlog_time_);

  if (real_bw <= standby_fetchlog_bw_limit_) {
    predicted_bw = (uint64_t)(real_bw + max(real_bw / 10, 1024 * 1024L));
  } else {
    predicted_bw = (uint64_t)(real_bw + real_bw / 2);
  }
  standby_fetchlog_time_ = current_time;
  standby_fetchlog_bytes_ = current_fetchlog_bytes;
  return ret;
}

int ObSrvNetworkFrame::net_endpoint_set_ingress(const ObNetEndpointKey &endpoint_key, int64_t assigned_bw)
{
  int ret = OB_SUCCESS;
  if ((int64_t)GCONF._server_standby_fetch_log_bandwidth_limit > 0) {
  } else if (GCONF.standby_fetch_log_bandwidth_limit == 0) {
    // unlimited
    if (OB_FAIL(obrpc::global_poc_server.update_server_standby_fetch_log_bandwidth_limit(RATE_UNLIMITED))) {
      COMMON_LOG(WARN, "Failed to set server-level standby fetchlog bandwidth limit");
    }
  }
  if (OB_UNLIKELY(assigned_bw == -1)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("assigned bandwidtth is invalid", K(ret), K(endpoint_key), K(assigned_bw));
  } else {
    standby_fetchlog_bw_limit_ = assigned_bw;
    standby_fetchlog_time_ = ObTimeUtility::current_time();
    standby_fetchlog_bytes_ = pn_get_rxbytes(obrpc::ObPocRpcServer::RATELIMIT_PNIO_GROUP);
    if (OB_FAIL(obrpc::global_poc_server.update_server_standby_fetch_log_bandwidth_limit(assigned_bw))) {
      LOG_WARN("net endpoint set ingress failed", K(ret), K(endpoint_key), K(assigned_bw));
    }
  }
  return ret;
}