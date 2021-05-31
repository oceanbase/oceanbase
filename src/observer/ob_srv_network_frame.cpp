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

#include "share/config/ob_server_config.h"
#include "share/ob_rpc_share.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_rpc_intrusion_detect.h"
#include "storage/ob_partition_service.h"
#include "share/ob_encrypt_kms.h"
#include <sys/types.h>
#include <sys/stat.h>

using namespace oceanbase::rpc::frame;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::share;

ObSrvNetworkFrame::ObSrvNetworkFrame(ObGlobalContext& gctx)
    : gctx_(gctx),
      xlator_(gctx),
      request_qhandler_(xlator_),
      deliver_(request_qhandler_, xlator_.get_session_handler(), gctx),
      rpc_handler_(deliver_),
      mysql_handler_(deliver_, gctx),
      net_(),
      rpc_transport_(NULL),
      high_prio_rpc_transport_(NULL),
      mysql_transport_(NULL),
      batch_rpc_transport_(NULL),
      last_ssl_info_hash_(UINT64_MAX)
{
  // empty;
}

ObSrvNetworkFrame::~ObSrvNetworkFrame()
{
  // empty
}

int ObSrvNetworkFrame::init()
{
  int ret = OB_SUCCESS;
  const char* mysql_unix_path = "unix:run/mysql.sock";
  const uint32_t rpc_port = static_cast<uint32_t>(GCONF.rpc_port);
  ObNetOptions opts;
  const int io_cnt = static_cast<int>(GCONF.net_thread_count);
  const int hp_io_cnt = static_cast<int>(GCONF.high_priority_net_thread_count);
  const bool use_easy_ma = hp_io_cnt > 0;
  opts.rpc_io_cnt_ = io_cnt;
  opts.high_prio_rpc_io_cnt_ = hp_io_cnt;
  opts.mysql_io_cnt_ = io_cnt;
  opts.batch_rpc_io_cnt_ = io_cnt;
  opts.use_ipv6_ = GCONF.use_ipv6;
  opts.tcp_user_timeout_ = static_cast<int>(GCONF.dead_socket_detection_timeout);
  opts.tcp_keepidle_ = static_cast<int>(GCONF.tcp_keepidle);
  opts.tcp_keepintvl_ = static_cast<int>(GCONF.tcp_keepintvl);
  opts.tcp_keepcnt_ = static_cast<int>(GCONF.tcp_keepcnt);
  if (GCONF.enable_tcp_keepalive) {
    opts.enable_tcp_keepalive_ = 1;
  } else {
    opts.enable_tcp_keepalive_ = 0;
  }

  deliver_.set_host(gctx_.self_addr_);
  easy_ma_init(rpc_port);

  if (OB_FAIL(request_qhandler_.init())) {
    LOG_ERROR("init rpc request qhandler fail", K(ret));

  } else if (OB_FAIL(deliver_.init())) {
    LOG_ERROR("init rpc deliverer fail", K(ret));

  } else if (OB_FAIL(rpc_handler_.init())) {
    LOG_ERROR("init rpc packet handler fail", K(ret));

  } else if (OB_FAIL(ob_rpc_intrusion_detect_patch(
                 rpc_handler_.ez_handler(), storage::ObPartitionService::get_instance().get_locality_manager()))) {
    LOG_ERROR("easy_handler_security_patch fail", K(ret));
  } else if (OB_FAIL(net_.init(opts))) {
    LOG_ERROR("init rpc easy network fail", K(ret));
  } else if (OB_FAIL(reload_ssl_config())) {
    LOG_ERROR("load_ssl_config fail", K(ret));
  } else if (OB_FAIL(
                 net_.add_mysql_listen(static_cast<uint32_t>(GCONF.mysql_port), mysql_handler_, mysql_transport_))) {
    LOG_ERROR("listen mysql port fail", "mysql_port", GCONF.mysql_port.get_value(), K(ret));
  } else if (OB_FAIL(net_.add_mysql_unix_listen(mysql_unix_path, mysql_handler_))) {
    LOG_ERROR("listen mysql port fail", "mysql_port", GCONF.mysql_port.get_value(), K(ret));
  } else if (OB_FAIL(net_.add_rpc_listen(use_easy_ma ? NET_IO_NORMAL_GID : rpc_port, rpc_handler_, rpc_transport_))) {
    LOG_ERROR("listen obrpc port fail", "rpc_port", GCONF.rpc_port.get_value(), K(ret));
  } else if (OB_FAIL(net_.add_batch_rpc_handler(rpc_handler_, batch_rpc_transport_, opts.batch_rpc_io_cnt_))) {
    LOG_ERROR("failed to add batch_rpc_hander", K(io_cnt), K(ret));
  } else if (hp_io_cnt > 0 &&
             OB_FAIL(net_.add_high_prio_rpc_listen(NET_IO_HP_GID, rpc_handler_, high_prio_rpc_transport_))) {
    LOG_ERROR("listen high priority rpc port fail", K(ret), "hp_rpc_port", rpc_port + HIGH_PRIO_RPC_PORT_DELTA);
  } else {
    share::set_obrpc_transport(rpc_transport_);
    rpc_transport_->set_sgid(NET_IO_NORMAL_GID);
    batch_rpc_transport_->set_sgid(NET_IO_NORMAL_GID);
    if (NULL != high_prio_rpc_transport_) {
      high_prio_rpc_transport_->set_sgid(NET_IO_HP_GID);
      LOG_INFO("set high prio rpc gid", K(NET_IO_HP_GID));
    }
    LOG_INFO("init rpc network frame successfully", "ssl_client_authentication", GCONF.ssl_client_authentication.str());
  }
  return ret;
}

void ObSrvNetworkFrame::destroy()
{
  net_.destroy();
}

int ObSrvNetworkFrame::start()
{
  int ret = net_.start();
  if (OB_SUCC(ret)) {
    int eret = easy_ma_start();
    if (EASY_OK == eret) {
      LOG_INFO("easy socket dispatch thread start OK");
    } else {
      ret = OB_SERVER_LISTEN_ERROR;
      LOG_ERROR("easy socket listen thread start fail", K(ret));
    }
  }
  return ret;
}

int ObSrvNetworkFrame::reload_config()
{
  int ret = common::OB_SUCCESS;
  int enable_easy_keepalive = 0;
  int enable_tcp_keepalive = 0;
  int32_t tcp_keepidle = static_cast<int>(GCONF.tcp_keepidle);
  int32_t tcp_keepintvl = static_cast<int>(GCONF.tcp_keepintvl);
  int32_t tcp_keepcnt = static_cast<int>(GCONF.tcp_keepcnt);
  int32_t user_timeout = static_cast<int>(GCONF.dead_socket_detection_timeout);

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
  } else if (OB_FAIL(net_.update_sql_tcp_keepalive_params(
                 user_timeout, enable_tcp_keepalive, tcp_keepidle, tcp_keepintvl, tcp_keepcnt))) {
    LOG_WARN("Failed to set sql tcp keepalive parameters.");
  }

  return ret;
}

int extract_expired_time(const char* const cert_file, int64_t& expired_time)
{
  int ret = OB_SUCCESS;
  X509* cert = NULL;
  BIO* b = NULL;
  if (OB_ISNULL(b = BIO_new_file(cert_file, "r"))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "BIO_new_file failed", K(ret), K(cert_file));
  } else if (OB_ISNULL(cert = PEM_read_bio_X509(b, NULL, 0, NULL))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "PEM_read_bio_X509 failed", K(ret), K(cert_file));
  } else {
    ASN1_TIME* notAfter = X509_get_notAfter(cert);
    struct tm tm1;
    memset(&tm1, 0, sizeof(tm1));
    tm1.tm_year = (notAfter->data[0] - '0') * 10 + (notAfter->data[1] - '0') + 100;
    tm1.tm_mon = (notAfter->data[2] - '0') * 10 + (notAfter->data[3] - '0') - 1;
    tm1.tm_mday = (notAfter->data[4] - '0') * 10 + (notAfter->data[5] - '0');
    tm1.tm_hour = (notAfter->data[6] - '0') * 10 + (notAfter->data[7] - '0');
    tm1.tm_min = (notAfter->data[8] - '0') * 10 + (notAfter->data[9] - '0');
    tm1.tm_sec = (notAfter->data[10] - '0') * 10 + (notAfter->data[11] - '0');
    expired_time = mktime(&tm1) * 1000000;  // us
  }

  if (NULL != cert) {
    X509_free(cert);
  }
  if (NULL != b) {
    BIO_free(b);
  }
  return ret;
}

uint64_t ObSrvNetworkFrame::get_ssl_file_hash(bool& file_exist)
{
  file_exist = false;
  uint64_t hash_value = 0;
  struct stat tmp_buf[3];

  if (0 == stat(OB_SSL_CA_FILE, tmp_buf + 0) && 0 == stat(OB_SSL_CERT_FILE, tmp_buf + 1) &&
      0 == stat(OB_SSL_KEY_FILE, tmp_buf + 2)) {
    file_exist = true;
    hash_value = murmurhash(&(tmp_buf[0].st_mtime), sizeof(tmp_buf[0].st_mtime), hash_value);
    hash_value = murmurhash(&(tmp_buf[1].st_mtime), sizeof(tmp_buf[1].st_mtime), hash_value);
    hash_value = murmurhash(&(tmp_buf[2].st_mtime), sizeof(tmp_buf[2].st_mtime), hash_value);
  }
  return hash_value;
}

int ObSrvNetworkFrame::reload_ssl_config()
{
  int ret = common::OB_SUCCESS;
  if (GCONF.ssl_client_authentication) {
    ObString invited_nodes(GCONF._ob_ssl_invited_nodes.str());
    storage::ObPartitionService::get_instance().get_locality_manager()->set_ssl_invited_nodes(invited_nodes);

    ObString ssl_config(GCONF.ssl_external_kms_info.str());
    bool file_exist = false;
    const uint64_t new_hash_value = ssl_config.empty() ? get_ssl_file_hash(file_exist) : ssl_config.hash();
    if (ssl_config.empty() && !file_exist) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("ssl file not available", K(new_hash_value));
      LOG_USER_ERROR(OB_INVALID_CONFIG, "ssl file not available");
    } else if (last_ssl_info_hash_ == new_hash_value) {
      LOG_INFO("no need reload_ssl_config", K(new_hash_value));

    } else {
      bool use_bkmi = false;
      bool use_sm = false;
      share::ObSSLClient client;
      if (!ssl_config.empty()) {
        if (OB_FAIL(client.init(ssl_config.ptr(), ssl_config.length()))) {
          OB_LOG(WARN, "kms client init", K(ret), K(ssl_config));
        } else if (OB_FAIL(client.check_param_valid())) {
          OB_LOG(WARN, "kms client param is not valid", K(ret));
        } else {
          use_bkmi = client.is_bkmi_mode();
          use_sm = client.is_sm_scene();
        }
      } else {
        if (EASY_OK != easy_ssl_ob_config_check(OB_SSL_CA_FILE, OB_SSL_CERT_FILE, OB_SSL_KEY_FILE, true, false)) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("key and cert not match", K(ret));
          LOG_USER_ERROR(OB_INVALID_CONFIG, "key and cert not match");
        }
      }

      if (OB_SUCC(ret)) {
        int64_t ssl_key_expired_time = 0;
        if (!use_bkmi && OB_FAIL(extract_expired_time(OB_SSL_CERT_FILE, ssl_key_expired_time))) {
          OB_LOG(WARN, "extract_expired_time failed", K(ret), K(use_bkmi));
        } else if (OB_FAIL(net_.load_ssl_config(use_bkmi,
                       use_sm,
                       client.get_root_ca(),
                       client.public_cert_.content_,
                       client.private_key_.content_))) {
          OB_LOG(WARN, "load_ssl_config failed", K(ret), K(use_bkmi), K(use_sm));
        } else {
          mysql_handler_.ez_handler()->is_ssl = 1;
          mysql_handler_.ez_handler()->is_ssl_opt = 1;
          rpc_handler_.ez_handler()->is_ssl = 1;
          rpc_handler_.ez_handler()->is_ssl_opt = 0;
          if (use_bkmi) {
            GCTX.ssl_key_expired_time_ = client.private_key_.key_expired_time_;
          } else {
            GCTX.ssl_key_expired_time_ = ssl_key_expired_time;
          }
          last_ssl_info_hash_ = new_hash_value;
          LOG_INFO("finish reload_ssl_config",
              K(use_bkmi),
              K(use_bkmi),
              K(use_sm),
              "ssl_key_expired_time",
              GCTX.ssl_key_expired_time_,
              K(new_hash_value));
        }
      }
    }
  } else {
    last_ssl_info_hash_ = UINT64_MAX;
    mysql_handler_.ez_handler()->is_ssl = 0;
    mysql_handler_.ez_handler()->is_ssl_opt = 0;
    rpc_handler_.ez_handler()->is_ssl = 0;
    rpc_handler_.ez_handler()->is_ssl_opt = 0;
    GCTX.ssl_key_expired_time_ = 0;
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

void ObSrvNetworkFrame::wait()
{
  net_.wait();
}

int ObSrvNetworkFrame::stop()
{
  int ret = OB_SUCCESS;
  deliver_.stop();
  // stop easy network after deliver has been stopped.
  // easy.stop will release memory that deliver thread maybe in use.
  easy_ma_stop();
  if (OB_FAIL(net_.stop())) {
    LOG_WARN("stop easy net fail", K(ret));
  }
  return ret;
}

int ObSrvNetworkFrame::get_proxy(obrpc::ObRpcProxy& proxy)
{
  return proxy.init(rpc_transport_);
}

ObReqTransport* ObSrvNetworkFrame::get_req_transport()
{
  return rpc_transport_;
}

ObReqTransport* ObSrvNetworkFrame::get_high_prio_req_transport()
{
  return high_prio_rpc_transport_;
}

ObReqTransport* ObSrvNetworkFrame::get_batch_rpc_req_transport()
{
  return batch_rpc_transport_;
}
