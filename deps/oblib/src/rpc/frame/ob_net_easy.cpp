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

#define USING_LOG_PREFIX RPC_FRAME

#include "io/easy_io.h"
#include "rpc/frame/ob_net_easy.h"
#include "rpc/obrpc/ob_poc_rpc_server.h"

#include "lib/ob_define.h"
#include "lib/utility/utility.h"
#include "lib/file/file_directory_utils.h"
#include "lib/thread/ob_thread_name.h"

using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::common;

namespace oceanbase
{
namespace common
{
void update_easy_log_level()
{
  int level = OB_LOGGER.get_id_level_map().get_level(oceanbase::common::OB_LOG_ROOT::M_EASY);
  switch (level) {
    case OB_LOG_LEVEL_DBA_ERROR: // pass
    case OB_LOG_LEVEL_DBA_WARN: {
      easy_log_level = EASY_LOG_FATAL;
      break;
    }
    case OB_LOG_LEVEL_INFO: {
      easy_log_level = EASY_LOG_INFO;
      break;
    }
    case OB_LOG_LEVEL_ERROR: {
      easy_log_level = EASY_LOG_ERROR;
      break;
    }
    case OB_LOG_LEVEL_WARN: {
      easy_log_level = EASY_LOG_WARN;
      break;
    }
    case OB_LOG_LEVEL_TRACE: {
      easy_log_level = EASY_LOG_DEBUG;
      break;
    }
    case OB_LOG_LEVEL_DEBUG: {
      easy_log_level = EASY_LOG_TRACE;
      break;
    }
    default: {
      easy_log_level = EASY_LOG_INFO;
      break;
    }
  }
}

void easy_log_format_adaptor(int level, const char *file, int line, const char *function,
                             uint64_t location_hash_val, const char *fmt, ...)
{
  int ob_level = level;
  switch (level) {
    case EASY_LOG_FATAL: {
      ob_level = OB_LOG_LEVEL_DBA_ERROR;
      break;
    }
    case EASY_LOG_ERROR: // pass
    case EASY_LOG_USER_ERROR: {
      ob_level = OB_LOG_LEVEL_ERROR;
      break;
    }
    case EASY_LOG_WARN: {
      ob_level = OB_LOG_LEVEL_WARN;
      break;
    }
    case EASY_LOG_INFO: {
      ob_level = OB_LOG_LEVEL_INFO;
      break;
    }
    case EASY_LOG_DEBUG: {
      ob_level = OB_LOG_LEVEL_TRACE;
      break;
    }
    case EASY_LOG_TRACE: {
      ob_level = OB_LOG_LEVEL_DEBUG;
      break;
    }
    default: {
      ob_level = OB_LOG_LEVEL_DEBUG;
      break;
    }
  };
  va_list args;
  va_start(args, fmt);
  OB_LOGGER.log_message_va("", ob_level, file, line, function,
                           location_hash_val, 0, fmt, args);
  va_end(args);
}
};
};

easy_addr_t __to_ez_addr(const ObAddr &addr)
{
  easy_addr_t ez;
  memset(&ez, 0, sizeof (ez));
  if (addr.is_valid()) {
    ez.port   = (htons)(static_cast<uint16_t>(addr.get_port()));
    ez.cidx   = 0;
    if (addr.using_ipv4()) {
      ez.family = AF_INET;
      ez.u.addr = htonl(addr.get_ipv4());
    } else {
      ez.family = AF_INET6;
      (void) addr.get_ipv6(&ez.u.addr6, sizeof(ez.u.addr6));
    }
  }
  return ez;
}

static void __on_ioth_start(void *args)
{
  auto net = static_cast<ObNetEasy*>(args);
  net->on_ioth_start();
}

static int __update_s2r_map(void *args)
{
  auto net = static_cast<ObNetEasy*>(args);

  if (net->net_easy_update_s2r_map_cb_) {
    net->net_easy_update_s2r_map_cb_(net->get_s2r_map_cb_args());
  }
  return EASY_OK;
}

ObNetEasy::ObNetEasy()
    : transports_(),
      proto_cnt_(0),
      rpc_eio_(NULL),
      high_prio_rpc_eio_(NULL),
      mysql_eio_(NULL),
      mysql_unix_eio_(NULL),
      batch_rpc_eio_(NULL),
      rpc_unix_eio_(NULL),
      is_inited_(false),
      started_(false),
      rpc_port_(0)
{
  net_easy_update_s2r_map_cb_ = NULL;
  net_easy_update_s2r_map_cb_args_ = NULL;
}

ObNetEasy::~ObNetEasy()
{
  // empty
}

easy_io_t * ObNetEasy::create_eio_(const int thread_num, uint64_t magic, uint8_t negotiation_enable)
{
  int ret = OB_SUCCESS;
  easy_io_t *eio = NULL;
  int io_cnt = thread_num;

  if (io_cnt < 1) {
    io_cnt = 1;
    LOG_WARN("easy io thread count less than 1, reset to 1 thread.");
  }
  if (OB_ISNULL(eio = easy_eio_create(eio, io_cnt))) {
    ret = OB_LIBEASY_ERROR;
  } else {
    eio->negotiation_enable = negotiation_enable;
    eio->magic = magic;
    LOG_INFO("create eio success");
  }
  (void) ret; // make compiler happy
  LOG_INFO("create eio success", K(thread_num), KCSTRING(lbt()));
  return eio;
}

int ObNetEasy::init_rpc_eio_(easy_io_t *eio, const ObNetOptions &opts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eio)) {
    ret = OB_NOT_INIT;
  } else {
    init_eio_(eio, opts);
    update_eio_rpc_tcp_keepalive(eio, opts.tcp_user_timeout_);
  }
  LOG_INFO("init rpc eio success", K(opts), KCSTRING(lbt()));
  return ret;
}

int ObNetEasy::init_mysql_eio_(easy_io_t *eio, const ObNetOptions &opts)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eio)) {
    ret = OB_NOT_INIT;
  } else {
    init_eio_(eio, opts);
    update_eio_sql_tcp_keepalive(eio, opts.tcp_user_timeout_, opts.enable_tcp_keepalive_,
                                opts.tcp_keepidle_, opts.tcp_keepintvl_, opts.tcp_keepcnt_);
  }
  LOG_INFO("init mysql eio success", K(opts), KCSTRING(lbt()));
  return ret;
}

void ObNetEasy::init_eio_(easy_io_t *eio, const ObNetOptions &opts)
{
  if (NULL != eio) {
    //create io thread
    eio->do_signal = 0;
    // eio_->force_destroy_second = OB_CONNECTION_FREE_TIME_S;
    eio->force_destroy_second = 5;
    eio->no_force_destroy = 1;
    eio->checkdrc = 1;
    eio->support_ipv6 = opts.use_ipv6_ ? 1 : 0;
    eio->no_redispatch = 1;
    eio->no_delayack = 1;
    eio->accept_count = 1;

    easy_eio_set_uthread_start(eio, __on_ioth_start, this);
    easy_eio_set_s2r_map_cb(eio, __update_s2r_map, this);
    eio->uthread_enable = 0;
    eio->tcp_defer_accept = 0;
  }
}

void ObNetEasy::update_eio_rpc_tcp_keepalive(easy_io_t* eio, int64_t input_user_timeout)
{
  if (NULL != eio) {
    const uint32_t user_timeout = static_cast<uint32_t>(input_user_timeout);
    eio->tcp_keepalive = (user_timeout > 0) ? 1: 0;
    // tcp keeyalive args
    eio->tcp_keepidle = max(user_timeout/5000000, 1u);
    eio->tcp_keepintvl = eio->tcp_keepidle;
    eio->tcp_keepcnt = 5;
    eio->conn_timeout = user_timeout/1000;
    eio->ack_timeout = user_timeout/1000;
  }
}

void ObNetEasy::update_eio_sql_tcp_keepalive(easy_io_t* eio, int64_t user_timeout,
                                              int enable_tcp_keepalive, int64_t tcp_keepidle,
                                              int64_t tcp_keepintvl, int64_t tcp_keepcnt)
{
  if (NULL != eio) {
    eio->tcp_keepalive = (enable_tcp_keepalive > 0) ? 1u: 0u;
    // tcp keeyalive args
    eio->tcp_keepidle = static_cast<uint32_t>(max(tcp_keepidle/1000000, 1));
    eio->tcp_keepintvl = static_cast<uint32_t>(max(tcp_keepintvl/1000000, 1));
    eio->tcp_keepcnt = static_cast<uint32_t>(tcp_keepcnt);
    eio->conn_timeout = static_cast<uint32_t>(user_timeout/1000);
    eio->ack_timeout = 0;
  }
}

int ObNetEasy::update_rpc_tcp_keepalive_params(int64_t user_timeout)
{
  int ret = OB_SUCCESS;
  update_eio_rpc_tcp_keepalive(rpc_eio_, user_timeout);
  update_eio_rpc_tcp_keepalive(high_prio_rpc_eio_, user_timeout);
  update_eio_rpc_tcp_keepalive(batch_rpc_eio_, user_timeout);
  return ret;
}

int ObNetEasy::update_sql_tcp_keepalive_params(int64_t user_timeout, int enable_tcp_keepalive,
                                               int64_t tcp_keepidle, int64_t tcp_keepintvl,
                                               int64_t tcp_keepcnt)
{
  int ret = OB_SUCCESS;
  update_eio_sql_tcp_keepalive(mysql_eio_, user_timeout, enable_tcp_keepalive,
                           tcp_keepidle, tcp_keepintvl, tcp_keepcnt);
  return ret;
}

int ObNetEasy::add_rpc_handler(ObReqHandler &handler, ObReqTransport *&transport)
{
  int ret = OB_SUCCESS;
  if (proto_cnt_ >= MAX_LISTEN_CNT) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("too many listen handler registered", K(ret));
  } else {
    transports_[proto_cnt_] = OB_NEW(ObReqTransport, ObModIds::OB_RPC, rpc_eio_, handler.ez_handler());
    transport = transports_[proto_cnt_++];
  }
  return ret;
}

int ObNetEasy::add_batch_rpc_handler(ObReqHandler &handler, ObReqTransport *&transport, const int io_cnt)
{
  int ret = OB_SUCCESS;
  if (proto_cnt_ >= MAX_LISTEN_CNT) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("too many listen handler registered", K(ret));
  } else {
    transports_[proto_cnt_] = OB_NEW(ObReqTransport, ObModIds::OB_RPC, batch_rpc_eio_, handler.ez_handler());
    transport = transports_[proto_cnt_++];
    if (OB_ISNULL(transport)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("transport is NULL", K(ret));
    } else {
      transport->set_bucket_count(io_cnt);
      LOG_INFO("add batch rpc handler");
    }
  }
  return ret;
}

int ObNetEasy::add_batch_rpc_listen(const uint32_t batch_rpc_port, const int io_cnt,
                                    ObReqHandler &handler, ObReqTransport *&transport)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_listen_(batch_rpc_port, rpc_eio_, handler, transport))) {
    LOG_WARN("add batch rpc listen failed", K(batch_rpc_port), K(io_cnt), K(ret));
  } else if (OB_ISNULL(transport)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got transport is NULL", K(batch_rpc_port), K(io_cnt), K(ret));
  } else {
    transport->set_bucket_count(io_cnt);
    LOG_INFO("add batch rpc listen");
  }
  return ret;
}

int ObNetEasy::add_rpc_listen(const uint32_t rpc_port, ObReqHandler &handler, ObReqTransport *&transpsort)
{
  return add_listen_(rpc_port, rpc_eio_, handler, transpsort);
}

int ObNetEasy::add_high_prio_rpc_listen(const uint32_t rpc_port, ObReqHandler &handler, ObReqTransport *&transpsort)
{
  return add_listen_(rpc_port, high_prio_rpc_eio_, handler, transpsort);
}

int ObNetEasy::add_mysql_listen(const uint32_t mysql_port, ObReqHandler &handler, ObReqTransport *&transpsort)
{
  return add_listen_(mysql_port, mysql_eio_, handler, transpsort);
}

int ObNetEasy::add_mysql_unix_listen(const char* path, ObReqHandler &handler)
{
  return add_unix_listen_(path, mysql_unix_eio_, handler);
}

int ObNetEasy::add_rpc_unix_listen(const char* path, ObReqHandler &handler)
{
  return add_unix_listen_(path, rpc_unix_eio_, handler);
}

int ObNetEasy::add_unix_listen_(const char* path, easy_io_t * eio, ObReqHandler &handler)
{
  int ret = OB_SUCCESS;
  easy_listen_t *listen = NULL;
  if (OB_ISNULL(eio)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("eio_ is NULL", KP(eio));
  } else if (NULL == (listen = easy_connection_add_listen(eio, path, 0, handler.ez_handler()))) {
    ret = OB_SERVER_LISTEN_ERROR;
    LOG_DBA_ERROR(OB_SERVER_LISTEN_ERROR, "msg", "easy_connection_add_listen error",
                  KERRMSG, K(ret));
  } else {
    LOG_INFO("listen unix domain succ", KCSTRING(path));
  }
  return ret;
}

int ObNetEasy::add_listen_(const uint32_t port, easy_io_t * eio, ObReqHandler &handler, ObReqTransport *&transport)
{
  int ret = OB_SUCCESS;
  easy_listen_t *listen = NULL;
  if (port <= 0 || OB_ISNULL(eio)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("eio_ is NULL", K(port), KP(eio));
  } else if (proto_cnt_ >= MAX_LISTEN_CNT) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("too many listen handler registered", K(ret));
  } else if (NULL == (listen = easy_connection_add_listen(eio, NULL, port, handler.ez_handler()))) {
    ret = OB_SERVER_LISTEN_ERROR;
    LOG_DBA_ERROR(OB_SERVER_LISTEN_ERROR, "msg", "easy_connection_add_listen error",
                  K(port), KERRMSG, K(ret));
  } else {
    transports_[proto_cnt_] = OB_NEW(ObReqTransport, ObModIds::OB_RPC, eio, handler.ez_handler());
    transport = transports_[proto_cnt_++];
    if (port < 1024) {
      LOG_INFO("listen delegate to dispatch thread", "gid", port);
    } else {
      LOG_INFO("listen start", K(port));
    }
  }
  return ret;
}

int ObNetEasy::net_register_and_add_listen_(ObListener &listener, easy_io_t *eio, ObReqHandler &handler, ObReqTransport *&transport)
{
  int ret = OB_SUCCESS;
  easy_listen_t *listen = NULL;
  easy_io_threads_pipefd_pool_t pipefd_pool;
  memset(&pipefd_pool, 0, sizeof(pipefd_pool));

  if (OB_ISNULL(eio)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("eio is NULL", KP(eio));
  } else {
      pipefd_pool.count = eio->io_thread_count;
      if (OB_FAIL(listener.regist(eio->magic, pipefd_pool.count, pipefd_pool.pipefd))) {
        LOG_ERROR("pipefd net register failed!", K(ret));
      } else if (proto_cnt_ >= MAX_LISTEN_CNT) {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("too many listen handler registered", K(ret));
      } else if (NULL == (listen = easy_add_pipefd_listen_for_connection(eio, handler.ez_handler(), &pipefd_pool))) {
        ret = OB_SERVER_LISTEN_ERROR;
        LOG_DBA_ERROR(OB_SERVER_LISTEN_ERROR,
                      "msg", "easy_add_pipefd_listen_for_connection! failed!", K(ret));
      } else {
        transports_[proto_cnt_] = OB_NEW(ObReqTransport, ObModIds::OB_RPC, eio, handler.ez_handler());
        transport = transports_[proto_cnt_++];
        LOG_INFO("listen start,", K(eio->magic));
      }
  }

  return ret;
}

int ObNetEasy::rpc_net_register(ObReqHandler &handler, ObReqTransport *& transport)
{
  return net_register_and_add_listen_(rpc_listener_, rpc_eio_, handler, transport);
}

int ObNetEasy::batch_rpc_net_register(ObReqHandler &handler, ObReqTransport *& transport)
{
  return net_register_and_add_listen_(rpc_listener_, batch_rpc_eio_, handler, transport);
}

int ObNetEasy::net_keepalive_register()
{
  int ret = OB_SUCCESS;
  easy_io_threads_pipefd_pool_t pipefd_pool;
  memset(&pipefd_pool, 0, sizeof(pipefd_pool));

  pipefd_pool.count = 1;
  if (OB_FAIL(rpc_listener_.regist(NET_KEEPALIVE_MAGIC, pipefd_pool.count, pipefd_pool.pipefd))) {
    LOG_ERROR("pipefd net register failed!", K(ret));
  } else if (OB_FAIL(ObNetKeepAlive::get_instance().set_pipefd_listen(pipefd_pool.pipefd[0]))) {
    LOG_ERROR("add pipefd listene failed!", K(ret));
  }

  return ret;
}

int ObNetEasy::high_prio_rpc_net_register(ObReqHandler &handler, ObReqTransport *& transport)
{
  return net_register_and_add_listen_(rpc_listener_, high_prio_rpc_eio_, handler, transport);
}

int ObNetEasy::set_easy_keepalive(int easy_keepalive_enabled)
{
  easy_eio_set_keepalive(rpc_eio_, easy_keepalive_enabled);
  easy_eio_set_keepalive(batch_rpc_eio_, easy_keepalive_enabled);
  if (high_prio_rpc_eio_) {
    easy_eio_set_keepalive(high_prio_rpc_eio_, easy_keepalive_enabled);
  }

  return OB_SUCCESS;
}

/*
 * Get the real bandwidth in latest statistics period.
 */
int ObNetEasy::get_easy_region_latest_bw(const char* region, int64_t *bw, int64_t *max_bw)
{
  int ret = OB_SUCCESS;
  /*
   * Currently, ratelimit is only supportecd on batch RPC.
   */
  if ((region == NULL) || (bw == NULL) || (max_bw == NULL)) {
    LOG_INFO("Wrong parameters ",
             "region", region,
              "bw", bw,
              "max_bw", max_bw);
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = easy_eio_get_region_bw(batch_rpc_eio_, region, bw, max_bw);
    if (ret != EASY_OK) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObNetEasy::set_easy_region_max_bw(const char *region, int64_t max_bw)
{
  easy_eio_set_region_max_bw(batch_rpc_eio_, max_bw, region);
  return OB_SUCCESS;
}

int ObNetEasy::notify_easy_s2r_map_changed()
{
  /*
   * Currently, ratelimit is only supportecd on batch RPC.
   */
  easy_eio_s2r_map_changed(batch_rpc_eio_);
  return OB_SUCCESS;
}

void* ObNetEasy::get_s2r_map_cb_args()
{
  return net_easy_update_s2r_map_cb_args_;
}

void ObNetEasy::set_s2r_map_cb(net_easy_update_s2r_map_pt *cb, void *args)
{
  net_easy_update_s2r_map_cb_ = cb;
  net_easy_update_s2r_map_cb_args_ = args;
  return;
}

int ObNetEasy::set_easy_s2r_map(ObAddr &addr, char *region)
{
  easy_addr_t ez_addr;

  ez_addr = __to_ez_addr(addr);
  easy_eio_set_s2r_map(rpc_eio_, &ez_addr, region);
  return OB_SUCCESS;
}

void ObNetEasy::set_ratelimit_enable(int easy_ratelimit_enabled)
{
  easy_eio_set_ratelimit_enable(rpc_eio_, easy_ratelimit_enabled);
  return;
}

void ObNetEasy::set_easy_ratelimit_stat_period(int64_t stat_period)
{
  easy_eio_set_ratelimit_stat_period(rpc_eio_, stat_period);
  return;
}

int ObNetEasy::load_ssl_config(const bool use_bkmi,
    const bool use_sm,
    const char *ca_ptr,
    const char *cert_ptr,
    const char *key_ptr,
    const char *enc_cert,
    const char *enc_private_key)
{
  int ret = OB_SUCCESS;
  const int from_file = use_bkmi ? 0 : 1;
  const int use_babassl = use_sm ? 1 : 0;

  if (EASY_OK != (easy_ssl_ob_config_load(mysql_eio_, ca_ptr, cert_ptr,
                                          key_ptr, enc_cert, enc_private_key, from_file, use_babassl, 0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("easy_ssl_ob_config_load for_mysql failed ", K(ret));
  } else if (EASY_OK != (easy_ssl_ob_config_load(rpc_eio_, ca_ptr, cert_ptr,
                                                 key_ptr, enc_cert, enc_private_key, from_file, use_babassl, 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("easy_ssl_ob_config_load for_rpc for_rpc_eio failed ", K(ret));
  } else if (NULL != high_prio_rpc_eio_ && EASY_OK != (easy_ssl_ob_config_load(high_prio_rpc_eio_, ca_ptr, cert_ptr,
                                                 key_ptr, enc_cert, enc_private_key, from_file, use_babassl, 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("easy_ssl_ob_config_load for_rpc for_high_prio_rpc_eio failed ", K(ret));
  } else if (EASY_OK != (easy_ssl_ob_config_load(batch_rpc_eio_, ca_ptr, cert_ptr,
                                                 key_ptr, enc_cert, enc_private_key, from_file, use_babassl, 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("easy_ssl_ob_config_load for_rpc for_batch_rpc_eio_ failed ", K(ret));
  }
  return ret;
}

template <class T>
void do_not_optimize(T const& value)
{
  asm volatile("" : : "g"(value) : "memory");
}

// If there is a hang problem in the lbt,
// you can use the disaster recovery capability of the observer to switch traffic
static void rpc_easy_timer_cb(EV_P_ ev_timer *w, int revents)
{
  UNUSED(loop);
  UNUSED(w);
  UNUSED(revents);
  char log_str[256];
  const int64_t DOING_REQUEST_WARN_THRESHOLD = 10 * 10000;

  if (NULL != EASY_IOTH_SELF) {
    snprintf(log_str, 256, "conn count=%d/%d, request done=%" PRIu64 "/%" PRIu64 ", request doing=%d/%d",
        EASY_IOTH_SELF->tx_conn_count,          EASY_IOTH_SELF->rx_conn_count,
        EASY_IOTH_SELF->tx_done_request_count,  EASY_IOTH_SELF->rx_done_request_count,
        EASY_IOTH_SELF->tx_doing_request_count, EASY_IOTH_SELF->rx_doing_request_count);
    LOG_INFO("[RPC EASY STAT]", KCSTRING(log_str));
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "EASY_IOTH_SELF is NULL");
  }
}

static void high_prio_rpc_easy_timer_cb(EV_P_ ev_timer *w, int revents)
{
  UNUSED(loop);
  UNUSED(w);
  UNUSED(revents);
  char log_str[256];
  const int64_t DOING_REQUEST_WARN_THRESHOLD = 10 * 10000;

  if (NULL != EASY_IOTH_SELF) {
    snprintf(log_str, 256, "conn count=%d/%d, request done=%" PRIu64 "/%" PRIu64 ", request doing=%d/%d",
        EASY_IOTH_SELF->tx_conn_count,          EASY_IOTH_SELF->rx_conn_count,
        EASY_IOTH_SELF->tx_done_request_count,  EASY_IOTH_SELF->rx_done_request_count,
        EASY_IOTH_SELF->tx_doing_request_count, EASY_IOTH_SELF->rx_doing_request_count);
    LOG_INFO("[HIGH PRIO RPC EASY STAT]", KCSTRING(log_str));
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "EASY_IOTH_SELF is NULL");
  }
}

static void batch_rpc_easy_timer_cb(EV_P_ ev_timer *w, int revents)
{
  UNUSED(loop);
  UNUSED(w);
  UNUSED(revents);
  char log_str[256];
  const int64_t DOING_REQUEST_WARN_THRESHOLD = 10 * 10000;

  if (NULL != EASY_IOTH_SELF) {
    snprintf(log_str, 256, "conn count=%d/%d, request done=%" PRIu64 "/%" PRIu64 ", request doing=%d/%d",
        EASY_IOTH_SELF->tx_conn_count,          EASY_IOTH_SELF->rx_conn_count,
        EASY_IOTH_SELF->tx_done_request_count,  EASY_IOTH_SELF->rx_done_request_count,
        EASY_IOTH_SELF->tx_doing_request_count, EASY_IOTH_SELF->rx_doing_request_count);
    LOG_INFO("[BATCH_RPC EASY STAT]", KCSTRING(log_str));
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "EASY_IOTH_SELF is NULL");
  }
}

#define DEFINE_CB_WITH_LBT(cb)                             \
static void cb##_with_lbt(EV_P_ ev_timer *w, int revents)  \
{                                                          \
  do_not_optimize(lbt());                                  \
  cb(loop, w, revents);                                    \
}

DEFINE_CB_WITH_LBT(rpc_easy_timer_cb)
DEFINE_CB_WITH_LBT(high_prio_rpc_easy_timer_cb)
DEFINE_CB_WITH_LBT(batch_rpc_easy_timer_cb)

static void mysql_easy_timer_cb(EV_P_ ev_timer *w, int revents)
{
  UNUSED(loop);
  UNUSED(w);
  UNUSED(revents);
  char log_str[256];
  const int64_t DOING_REQUEST_WARN_THRESHOLD = 10 * 10000;

  if (NULL != EASY_IOTH_SELF) {
    snprintf(log_str, 256, "conn count=%d/%d, request done=%" PRIu64 "/%" PRIu64 ", request doing=%d/%d",
        EASY_IOTH_SELF->tx_conn_count,          EASY_IOTH_SELF->rx_conn_count,
        EASY_IOTH_SELF->tx_done_request_count,  EASY_IOTH_SELF->rx_done_request_count,
        EASY_IOTH_SELF->tx_doing_request_count, EASY_IOTH_SELF->rx_doing_request_count);

    if ((EASY_IOTH_SELF->tx_doing_request_count >= DOING_REQUEST_WARN_THRESHOLD) ||
        (EASY_IOTH_SELF->rx_doing_request_count >= DOING_REQUEST_WARN_THRESHOLD)) {
      LOG_ERROR_RET(OB_SUCCESS, "[MYSQL EASY STAT]", KCSTRING(log_str));
    } else {
      LOG_INFO("[MYSQL EASY STAT]", KCSTRING(log_str));
    }
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "EASY_IOTH_SELF is NULL");
  }
}

int ObNetEasy::init(const ObNetOptions &opts, uint8_t negotiation_enable)
{
  int ret = OB_SUCCESS;
  const int64_t EASY_STAT_INTERVAL = 1; // 1s
  const int64_t MYSQL_UNIX_IOTH_COUNT = 1;
  const int64_t RPC_UNIX_IOTH_COUNT = 1;
  bool is_high_prio_rpc_enabled = false;
  easy_log_set_format(easy_log_format_adaptor);
  if (opts.high_prio_rpc_io_cnt_ > 0 ) {
    is_high_prio_rpc_enabled = true;
  }

  if (!is_inited_) {
    if (OB_ISNULL(rpc_eio_ = create_eio_(opts.rpc_io_cnt_, RPC_EIO_MAGIC, negotiation_enable))) {
      LOG_ERROR("create rpc easy io fail", K(ret));
      ret = OB_LIBEASY_ERROR;
    } else if (OB_FAIL(init_rpc_eio_(rpc_eio_, opts))) {
      LOG_ERROR("init rpc easy io fail", K(ret));
    } else if (is_high_prio_rpc_enabled && OB_ISNULL(high_prio_rpc_eio_ = create_eio_(opts.high_prio_rpc_io_cnt_, HIGH_PRI_RPC_EIO_MAGIC, negotiation_enable))) {
      LOG_ERROR("create high priority rpc easy io fail", K(ret));
      ret = OB_LIBEASY_ERROR;
    } else if (is_high_prio_rpc_enabled && OB_FAIL(init_rpc_eio_(high_prio_rpc_eio_, opts))) {
      LOG_ERROR("init high priority rpc easy io fail", K(ret));
    } else if (OB_ISNULL(mysql_eio_ = create_eio_(opts.mysql_io_cnt_))) {
      LOG_ERROR("create mysql easy io fail", K(ret));
      ret = OB_LIBEASY_ERROR;
    } else if (OB_FAIL(init_mysql_eio_(mysql_eio_, opts))) {
      LOG_ERROR("init mysql easy io fail", K(ret));
    } else if (OB_ISNULL(mysql_unix_eio_ = create_eio_(MYSQL_UNIX_IOTH_COUNT))) {
      LOG_ERROR("create mysql unix easy io fail", K(ret));
      ret = OB_LIBEASY_ERROR;
    } else if (OB_FAIL(init_mysql_eio_(mysql_unix_eio_, opts))) {
      LOG_ERROR("init mysql easy io fail", K(ret));
    } else if (OB_ISNULL(batch_rpc_eio_ = create_eio_(opts.batch_rpc_io_cnt_ + 1, BATCH_RPC_EIO_MAGIC, negotiation_enable))) { // Create one more IO thread for the connection with ratelimit enabled.
      LOG_ERROR("create batch rpc easy io fail", K(ret));
      ret = OB_LIBEASY_ERROR;
    } else if (OB_FAIL(init_rpc_eio_(batch_rpc_eio_, opts))) {
      LOG_ERROR("init batch rpc easy io fail", K(ret));
    } else if (OB_ISNULL(rpc_unix_eio_ = create_eio_(RPC_UNIX_IOTH_COUNT))) {
      LOG_ERROR("create rpc unix easy io fail", K(ret));
      ret = OB_LIBEASY_ERROR;
    } else if (OB_FAIL(init_rpc_eio_(rpc_unix_eio_, opts))) {
      LOG_ERROR("init rpc unix io fail");
    } else {
      easy_io_thread_t *ioth = NULL;
      easy_thread_pool_for_each(ioth, rpc_eio_->io_thread_pool, 0)
      {
        if (!OB_ISNULL(ioth)) {
          ev_timer_init(&ioth->user_timer,
                        rpc_easy_timer_cb_with_lbt, EASY_STAT_INTERVAL, EASY_STAT_INTERVAL);
          ev_timer_start(ioth->loop, &(ioth->user_timer));
        }
      }

      if (is_high_prio_rpc_enabled) {
        ioth = NULL;
        easy_thread_pool_for_each(ioth, high_prio_rpc_eio_->io_thread_pool, 0)
        {
          if (!OB_ISNULL(ioth)) {
            ev_timer_init(&ioth->user_timer,
                          high_prio_rpc_easy_timer_cb_with_lbt, EASY_STAT_INTERVAL, EASY_STAT_INTERVAL);
            ev_timer_start(ioth->loop, &(ioth->user_timer));
          }
        }
      }

      ioth = NULL;
      easy_thread_pool_for_each(ioth, batch_rpc_eio_->io_thread_pool, 0)
      {
        if (!OB_ISNULL(ioth)) {
          ev_timer_init(&ioth->user_timer,
                        batch_rpc_easy_timer_cb_with_lbt, EASY_STAT_INTERVAL, EASY_STAT_INTERVAL);
          ev_timer_start(ioth->loop, &(ioth->user_timer));
        }
      }
      easy_eio_set_rlmtr_thread(batch_rpc_eio_);

      ioth = NULL;
      easy_thread_pool_for_each(ioth, mysql_eio_->io_thread_pool, 0)
      {
        if (!OB_ISNULL(ioth)) {
          ev_timer_init(&ioth->user_timer,
                        mysql_easy_timer_cb, EASY_STAT_INTERVAL, EASY_STAT_INTERVAL);
          ev_timer_start(ioth->loop, &(ioth->user_timer));
        }
      }

      ioth = NULL;
      if (NULL != mysql_unix_eio_) {
        easy_thread_pool_for_each(ioth, mysql_unix_eio_->io_thread_pool, 0)
        {
          if (!OB_ISNULL(ioth)) {
            ev_timer_init(&ioth->user_timer,
                          mysql_easy_timer_cb, EASY_STAT_INTERVAL, EASY_STAT_INTERVAL);
            ev_timer_start(ioth->loop, &(ioth->user_timer));
          }
        }
      }

      ioth = NULL;
      if (NULL != rpc_unix_eio_) {
        easy_thread_pool_for_each(ioth, rpc_unix_eio_->io_thread_pool, 0)
        {
          if (!OB_ISNULL(ioth)) {
            ev_timer_init(&ioth->user_timer,
                        rpc_easy_timer_cb_with_lbt, EASY_STAT_INTERVAL, EASY_STAT_INTERVAL);
            ev_timer_start(ioth->loop, &(ioth->user_timer));
          }
        }
      }

      is_inited_ = true;
    }
  } else {
    ret = OB_INIT_TWICE;
    LOG_ERROR("net easy has inited", K(ret));
  }
  return ret;
}

int ObNetEasy::set_rpc_port(uint32_t rpc_port)
{
  rpc_port_ = rpc_port;
  rpc_listener_.set_port(rpc_port);
  return OB_SUCCESS;
}

int ObNetEasy::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_ || OB_ISNULL(rpc_eio_) || OB_ISNULL(mysql_eio_) || OB_ISNULL(batch_rpc_eio_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("eio is NULL, not inited", K(is_inited_), KP_(rpc_eio), KP_(mysql_eio), KP_(batch_rpc_eio), K(ret));
  }

  if (OB_SUCC(ret) && rpc_port_ > 0) {
    ATOMIC_STORE(&global_ob_listener, &rpc_listener_);
    if (!global_poc_server.has_start()) {
      if (OB_FAIL(rpc_listener_.listen_create(rpc_port_))) {
        LOG_ERROR("create listen failed", K(ret));
      } else if (OB_FAIL(rpc_listener_.start())) {
        LOG_ERROR("oblistener start failed!", K(rpc_port_), K(ret));
      }
    }
  }

  // start rpc io thread
  if (OB_SUCC(ret)) {
    int eret = easy_eio_start(rpc_eio_);
    if (EASY_OK == eret) {
      LOG_INFO("start rpc easy io");
    } else {
      ret = OB_LIBEASY_ERROR;
      LOG_ERROR("start rpc easy io fail", K(ret));
    }
  }

  // start high priority rpc io thread
  if (OB_SUCC(ret) && !OB_ISNULL(high_prio_rpc_eio_)) {
    int eret = easy_eio_start(high_prio_rpc_eio_);
    if (EASY_OK == eret) {
      LOG_INFO("start high priority rpc easy io");
    } else {
      ret = OB_LIBEASY_ERROR;
      LOG_ERROR("start high priority rpc easy io fail", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    int eret = easy_eio_start(batch_rpc_eio_);
    if (EASY_OK == eret) {
      LOG_INFO("start batch rpc easy io");
    } else {
      ret = OB_LIBEASY_ERROR;
      LOG_ERROR("start batch rpc easy io fail", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    int eret = easy_eio_start(mysql_eio_);
    if (EASY_OK == eret) {
      LOG_INFO("start mysql easy io");
    } else {
      ret = OB_LIBEASY_ERROR;
      LOG_ERROR("start mysql easy io fail", K(ret));
    }
  }

  if (OB_SUCC(ret) && NULL != mysql_unix_eio_) {
    int eret = easy_eio_start(mysql_unix_eio_);
    if (EASY_OK == eret) {
      LOG_INFO("start mysql unix easy io");
    } else {
      ret = OB_LIBEASY_ERROR;
      LOG_ERROR("start mysql unix easy io fail", K(ret));
    }
  }

  if (OB_SUCC(ret) && NULL != rpc_unix_eio_) {
    int eret = easy_eio_start(rpc_unix_eio_);
    if (EASY_OK == eret) {
      LOG_INFO("start rpc unix easy io");
    } else {
      ret = OB_LIBEASY_ERROR;
      LOG_ERROR("start rpc unix easy io fail", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    started_ = true;
  }

  return ret;
}

int ObNetEasy::mysql_shutdown()
{
  int ret = OB_SUCCESS;
  if (!started_ || !is_inited_ || OB_ISNULL(rpc_eio_) || OB_ISNULL(mysql_eio_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("easy net hasn't started", K(ret));
  } else {
    int eret = easy_eio_shutdown(mysql_eio_);
    if (eret != EASY_OK) {
      ret = OB_IO_ERROR;
      LOG_WARN("shutdown mysql eio error", K(ret));
    }
    if (NULL != mysql_unix_eio_) {
      int eret = easy_eio_shutdown(mysql_unix_eio_);
      if (eret != EASY_OK) {
        ret = OB_IO_ERROR;
        LOG_WARN("shutdown mysql unix eio error", K(ret));
      }
    }
  }

  return ret;
}

int ObNetEasy::rpc_shutdown()
{
  int ret = OB_SUCCESS;
  if (!started_ || !is_inited_ || OB_ISNULL(rpc_eio_) || OB_ISNULL(mysql_eio_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("easy net hasn't started", K(ret));
  } else {
    int eret = easy_eio_shutdown(rpc_eio_);
    if (eret != EASY_OK) {
      ret = OB_IO_ERROR;
      LOG_WARN("shutdown rpc eio error", K(ret));
    }
  }

  return ret;
}

int ObNetEasy::high_prio_rpc_shutdown()
{
  int ret = OB_SUCCESS;
  if (!started_ || !is_inited_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("easy net hasn't started", K(ret));
  } else if (OB_ISNULL(high_prio_rpc_eio_)) {
    LOG_INFO("user does not start high priority net threads");
  } else {
    int eret = easy_eio_shutdown(high_prio_rpc_eio_);
    if (eret != EASY_OK) {
      ret = OB_IO_ERROR;
      LOG_WARN("shutdown rpc eio error", K(ret));
    }
  }
  return ret;
}

int ObNetEasy::batch_rpc_shutdown()
{
  int ret = OB_SUCCESS;
  if (!started_ || !is_inited_ || OB_ISNULL(batch_rpc_eio_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("easy net hasn't started", K(ret));
  } else {
    int eret = easy_eio_shutdown(batch_rpc_eio_);
    if (eret != EASY_OK) {
      ret = OB_IO_ERROR;
      LOG_WARN("shutdown batch rpc eio error", K(ret));
    }
  }

  return ret;
}

int ObNetEasy::unix_rpc_shutdown()
{
  int ret = OB_SUCCESS;
  if (!started_ || !is_inited_ || OB_ISNULL(rpc_unix_eio_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("easy net hasn't started", K(ret));
  } else {
    int eret = easy_eio_shutdown(rpc_unix_eio_);
    if (eret != EASY_OK) {
      ret = OB_IO_ERROR;
      LOG_WARN("shutdown batch rpc eio error", K(ret));
    }
  }

  return ret;
}

int ObNetEasy::stop()
{
  int ret = OB_SUCCESS;
  if (!started_ || !is_inited_ || OB_ISNULL(rpc_eio_) || OB_ISNULL(mysql_eio_) || OB_ISNULL(batch_rpc_eio_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("easy net hasn't started", K(ret));
  } else {
    int eret = easy_eio_stop(rpc_eio_);
    if (eret != EASY_OK) {
      ret = OB_IO_ERROR;
      LOG_WARN("stop rpc eio error", K(ret));
    }

    if (OB_SUCC(ret) && !OB_ISNULL(high_prio_rpc_eio_)) {
      eret = easy_eio_stop(high_prio_rpc_eio_);
      if (eret != EASY_OK) {
        ret = OB_IO_ERROR;
        LOG_WARN("stop high priority rpc eio error", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      eret = easy_eio_stop(mysql_eio_);
      if (eret != EASY_OK) {
        ret = OB_IO_ERROR;
        LOG_WARN("stop mysql eio error", K(ret));
      }
    }

    if (OB_SUCC(ret) && NULL != mysql_unix_eio_) {
      eret = easy_eio_stop(mysql_unix_eio_);
      if (eret != EASY_OK) {
        ret = OB_IO_ERROR;
        LOG_WARN("stop mysql unix eio error", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      eret = easy_eio_stop(batch_rpc_eio_);
      if (eret != EASY_OK) {
        ret = OB_IO_ERROR;
        LOG_WARN("stop batch rpc eio error", K(ret));
      }
      rpc_listener_.stop();
    }

    if (NULL != rpc_unix_eio_) {
      eret = easy_eio_stop(rpc_unix_eio_);
      if (eret != EASY_OK) {
        ret = OB_IO_ERROR;
        LOG_WARN("stop rpc unix eio error", K(ret));
      }
    }
  }

  started_ = false;
  return ret;
}

void ObNetEasy::destroy()
{
  if (started_) {
    stop();
  }
  wait();
  if (is_inited_ && !OB_ISNULL(rpc_eio_) && !OB_ISNULL(mysql_eio_) && !OB_ISNULL(batch_rpc_eio_)) {
    easy_eio_destroy(rpc_eio_);
    rpc_eio_ = NULL;
    if (!OB_ISNULL(high_prio_rpc_eio_)) {
      easy_eio_destroy(high_prio_rpc_eio_);
      high_prio_rpc_eio_ = NULL;
    }
    easy_eio_destroy(batch_rpc_eio_);
    batch_rpc_eio_ = NULL;
    easy_eio_destroy(mysql_eio_);
    mysql_eio_ = NULL;
    if (NULL != mysql_unix_eio_) {
      easy_eio_destroy(mysql_unix_eio_);
    }
    if (NULL != rpc_unix_eio_) {
      easy_eio_destroy(rpc_unix_eio_);
    }
    rpc_listener_.destroy();
    mysql_unix_eio_ = NULL;
    rpc_unix_eio_ = NULL;
    is_inited_ = false;
  }

}

void ObNetEasy::wait()
{
  if (!OB_ISNULL(rpc_eio_) && !OB_ISNULL(mysql_eio_) && !OB_ISNULL(batch_rpc_eio_)) {
    easy_eio_wait(rpc_eio_);
    easy_eio_wait(mysql_eio_);
    easy_eio_wait(batch_rpc_eio_);
  }

  if (NULL != mysql_unix_eio_) {
    easy_eio_wait(mysql_unix_eio_);
  }

  if (NULL != rpc_unix_eio_) {
    easy_eio_wait(rpc_unix_eio_);
  }

  if (!OB_ISNULL(high_prio_rpc_eio_)) {
    easy_eio_wait(high_prio_rpc_eio_);
  }

  rpc_listener_.wait();
}

void ObNetEasy::on_ioth_start()
{
  // Fake routines for current thread, this memory won't be freed
  // before server process exit.
  easy_io_t* const cur_eio = easy_baseth_self->eio;
  if (cur_eio == rpc_eio_) {
    lib::set_thread_name("RpcIO");
  } else if (cur_eio == mysql_eio_) {
    lib::set_thread_name("MysqlIO");
  } else if (cur_eio == mysql_unix_eio_) {
    lib::set_thread_name("MysqlUnix");
  } else if (cur_eio == batch_rpc_eio_) {
    lib::set_thread_name("BatchIO");
  } else if (cur_eio == high_prio_rpc_eio_) {
    lib::set_thread_name("HPIO");
  } else if (cur_eio == rpc_unix_eio_) {
    lib::set_thread_name("RpcUnix");
  } else {
    lib::set_thread_name("EasyIO");
  }
}
