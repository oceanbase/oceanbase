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
#include "ob_req_handler.h"
#include "lib/ob_abort.h"
#include "lib/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/serialization.h"
#include "common/ob_clock_generator.h"

using namespace oceanbase::rpc::frame;
using namespace oceanbase::common::serialization;

const uint8_t ObReqHandler::MAGIC_HEADER_FLAG[4] =
{ ObReqHandler::API_VERSION, 0xDB, 0xDB, 0xCE };
const uint8_t ObReqHandler::MAGIC_COMPRESS_HEADER_FLAG[4] =
{ ObReqHandler::API_VERSION, 0xDB, 0xDB, 0xCC };

void *ObReqHandler::decode(easy_message_t */* m */)
{
  return NULL;
}

int ObReqHandler::encode(easy_request_t */* r */, void */* packet */)
{
  return EASY_OK;
}

int ObReqHandler::process(easy_request_t */* r */)
{
  return EASY_OK;
}

int ObReqHandler::batch_process(easy_message_t */* m */)
{
  return EASY_OK;
}

int ObReqHandler::on_connect(easy_connection_t */* c */)
{
  return EASY_OK;
}

int ObReqHandler::on_disconnect(easy_connection_t */* c */)
{
  return EASY_OK;
}

int ObReqHandler::new_packet(easy_connection_t */* c */)
{
  return EASY_OK;
}

uint64_t ObReqHandler::get_packet_id(easy_connection_t */* c */, void */* packet */)
{
  return 0;
}

void ObReqHandler::set_trace_info(easy_request_t */*  r */, void */*  packet */)
{
}

int ObReqHandler::on_idle(easy_connection_t */* c */)
{
  return EASY_OK;
}

void ObReqHandler::send_buf_done(easy_request_t */* r */)
{
}

void ObReqHandler::sending_data(easy_connection_t */* c */)
{
}

int ObReqHandler::send_data_done(easy_connection_t */* c */)
{
  return EASY_OK;
}

int ObReqHandler::on_redispatch(easy_connection_t */* c */)
{
  return EASY_OK;
}

int ObReqHandler::on_close(easy_connection_t */* c */)
{
  return EASY_OK;
}

int ObReqHandler::cleanup(easy_request_t */* r */, void */* packet */)
{
  return EASY_OK;
}

namespace oceanbase
{
namespace easy
{

// easy callbacks
static ObReqHandler *packet_handler(easy_connection_t *c)
{
  ObReqHandler *handler = NULL;
  if (!OB_ISNULL(c) && !OB_ISNULL(c->handler)) {
    handler = static_cast<ObReqHandler*>(c->handler->user_data);
  }
  return handler;
}

void *decode(easy_message_t *m)
{
  RLOCAL(int64_t, decode_num_tot);
  RLOCAL(int64_t, time_tot);
  void *pret = NULL;
  if (OB_ISNULL(m)) {
    LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "message is NULL", K(m));
  } else {
    const int64_t start_time = common::ObTimeUtility::current_time();
    int64_t end_time = 0;
    ObReqHandler *handler = packet_handler(m->c);
    if (NULL != handler) {
      pret = handler->decode(m);
      end_time = common::ObTimeUtility::current_time();
      time_tot += (end_time - start_time);
      decode_num_tot += 1;
    }
    if (TC_REACH_TIME_INTERVAL(1000 * 1000)) {
      if (0 != decode_num_tot) {
        RPC_LOG(DEBUG, "",
                K(*(&decode_num_tot)), K(time_tot / decode_num_tot), K(*(&time_tot)));
      }
      time_tot = 0;
      decode_num_tot = 0;
    }
    const int64_t diff = end_time - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "decode handler cost too much time", K(diff));
    }
  }

  return pret;
}

int encode(easy_request_t *r, void *packet)
{
  RLOCAL(int64_t, encode_num_tot);
  RLOCAL(int64_t, time_tot);
  int eret = EASY_ERROR;
  if (OB_ISNULL(r) || OB_ISNULL(r->ms) || OB_ISNULL(r->ms->c)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid arguement which is NULL");
  } else {
    const int64_t start_time = common::ObTimeUtility::current_time();
    int64_t end_time = 0;
    ObReqHandler *handler = packet_handler(r->ms->c);
    if (!OB_ISNULL(handler)) {
      eret = handler->encode(r, packet);
      end_time = common::ObTimeUtility::current_time();
      time_tot += (end_time - start_time);
      encode_num_tot += 1;
    }
    if (TC_REACH_TIME_INTERVAL(1000 * 1000)) {
      if (0 != encode_num_tot) {
        RPC_LOG(DEBUG, "",
            K(*(&encode_num_tot)), K(time_tot / encode_num_tot), K(*(&time_tot)));
      }
      time_tot = 0;
      encode_num_tot = 0;
    }
    const int64_t diff = end_time - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "encode handler cost too much time", K(diff));
    }
  }
  return eret;
}

int process(easy_request_t *r)
{
  RLOCAL(int64_t, process_num_tot);
  RLOCAL(int64_t, time_tot);
  int eret = EASY_ERROR;
  if (OB_ISNULL(r) || OB_ISNULL(r->ms) || OB_ISNULL(r->ms->c)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument which is NULL");
  } else {
    int64_t start_time = common::ObTimeUtility::current_time();
    int64_t end_time = 0;
    ObReqHandler *handler = packet_handler(r->ms->c);
    if (!OB_ISNULL(handler)) {
      eret = handler->process(r);
      end_time = common::ObTimeUtility::current_time();
      time_tot += (end_time - start_time);
      process_num_tot += 1;
    }
    if (TC_REACH_TIME_INTERVAL(1000 * 1000)) {
      if (0 != process_num_tot) {
        RPC_LOG(DEBUG, "",
            K(*(&process_num_tot)), K(time_tot / process_num_tot), K(*(&time_tot)));
      }
      time_tot = 0;
      process_num_tot = 0;
    }
    const int64_t diff = end_time - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "process handler cost too much time", K(diff));
    }
  }
  return eret;
}

int batch_process(easy_message_t *m)
{
  int eret = EASY_ERROR;

  if (OB_ISNULL(m)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument", K(m));
  } else {
    const int64_t start_time = common::ObTimeUtility::current_time();
    ObReqHandler *handler = packet_handler(m->c);
    if (!OB_ISNULL(handler)) {
      eret = handler->batch_process(m);
    } else {
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "handler is NULL", K(handler));
    }
    const int64_t diff = common::ObTimeUtility::current_time() - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "process handler cost too much time", K(diff));
    }
  }
  return eret;
}

int on_connect(easy_connection_t *c)
{
  RLOCAL(int64_t, on_connect_num_tot);
  RLOCAL(int64_t, time_tot);
  int eret = EASY_ERROR;
  if (OB_ISNULL(c)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument which is NULL");
  } else {
    int64_t start_time = common::ObTimeUtility::current_time();
    int64_t end_time = 0;
    ObReqHandler *handler = packet_handler(c);
    if (!OB_ISNULL(handler)) {
      eret = handler->on_connect(c);
      end_time = common::ObTimeUtility::current_time();
      time_tot += (end_time - start_time);
      on_connect_num_tot += 1;
    }
    if (TC_REACH_TIME_INTERVAL(1000 * 1000)) {
      if (0 != on_connect_num_tot) {
        RPC_LOG(DEBUG, "",
            K(*(&on_connect_num_tot)),
            K(time_tot / on_connect_num_tot), K(*(&time_tot)));
      }
      time_tot = 0;
      on_connect_num_tot = 0;
    }
    const int64_t diff = end_time - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "on_connect handler cost too much time", K(diff));
    }
  }
  return eret;
}

int on_disconnect(easy_connection_t *c)
{
  RLOCAL(int64_t, on_disconnect_num_tot);
  RLOCAL(int64_t, time_tot);
  int eret = EASY_ERROR;
  if (OB_ISNULL(c)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument which is NULL");
  } else {
    int64_t start_time = common::ObTimeUtility::current_time();
    int64_t end_time = 0;
    ObReqHandler *handler = packet_handler(c);
    if (!OB_ISNULL(handler)) {
      eret = handler->on_disconnect(c);
      end_time = common::ObTimeUtility::current_time();
      time_tot += (end_time - start_time);
      on_disconnect_num_tot += 1;
    }
    if (TC_REACH_TIME_INTERVAL(1000 * 1000)) {
      if (0 != on_disconnect_num_tot) {
        RPC_LOG(DEBUG, "",
            K(*(&on_disconnect_num_tot)),
            K(time_tot / on_disconnect_num_tot), K(*(&time_tot)));
      }
      time_tot = 0;
      on_disconnect_num_tot = 0;
    }
    const int64_t diff = end_time - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "on_connect handler cost too much time", K(diff));
    }
  }
  return eret;
}

int new_packet(easy_connection_t *c)
{
  int eret = EASY_ERROR;
  if (!OB_ISNULL(c)) {
    int64_t start_time = common::ObTimeUtility::current_time();
    ObReqHandler *handler =packet_handler(c);
    if (!OB_ISNULL(handler)) {
      eret = handler->new_packet(c);
    }
    const int64_t diff = common::ObTimeUtility::current_time() - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "new_packet handler cost too much time", K(diff));
    }
  }
  return eret;
}

int new_keepalive_packet(easy_connection_t *conn)
{
  int ret = common::OB_SUCCESS;
  int eret = EASY_ERROR;
  char *payload = NULL;
  easy_buf_t *ebuf = NULL;
  easy_session_t *sess = NULL;
  uint32_t playload_size = 0;
  uint32_t session_alloc_size = 0;
  uint32_t keepalive_payload_size = 0;

  if (OB_ISNULL(conn)) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument, c is NULL.", K(ret));
  } else {
    playload_size = common::OB_NET_HEADER_LENGTH + keepalive_payload_size;
    session_alloc_size = playload_size;
    sess = easy_session_create(session_alloc_size);
    if (sess == NULL) {
      LOG_WARN("failed to create easy_session for keepalive.");
    } else {
      sess->type = EASY_TYPE_KEEPALIVE_SESSION;
      sess->status = EASY_CONNECT_SEND;
      sess->callback = NULL;
      sess->unneed_response = 1;
      sess->enable_trace = 1;

      ebuf = &(sess->r.tx_ebuf);
      payload = (char *)sess->tx_buf;

      {
        /*
         *                   Easy Keepalive Packet Format
         *
         *     0               1               2               3
         *     0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7
         *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *    |       ver     |    mflg[1]    |    mflg[2]    |   mflag[3]    |     |
         *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+     |
         *    |                             dlen                              |     v
         *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+   Header
         *    |                             chid                              |     ^
         *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+     |
         *    |                           reserved                            |     |
         *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *    |               |E|S|           |       |                       |     |
         *    |     type      |R|R|           |  grp  |       reserved        |     v
         *    |               |L|G|           |       |                       |  Payload
         *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+     ^
         *    |                           reserved                            |     |
         *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *
         *    type:
         *        00 - unknown
         *        01 - ratelimit
         *        xx - reserved
         *    ERL: Enable RateLimit
         *         0 - ratelimit disabled on connection.
         *         1 - ratelimit enabled  on connection.
         *    SRG: Set Ratelimit Group
         *         0 - ignore.
         *         1 - set rate limit group.
         *
         */

        // encode keepalive packet
        int32_t dlen = 0;
        int32_t chid = 0;
        int32_t reserved = 0;
        int64_t pos = 4;
        char *version = NULL;

        MEMCPY(payload, ObReqHandler::MAGIC_HEADER_FLAG, 4);
        version = payload;
        *version |= conn->magic_ver << MAGIC_VERSION_OFF;
        *version |= 1 << MAGIC_KEEPALIVE_INDICATOR_OFF;

        if (OB_FAIL(encode_i32(payload, playload_size, pos, dlen))) {
          LOG_WARN("failed to encode dlen of easy netheader", K(ret));
        } else if (OB_FAIL(encode_i32(payload, playload_size, pos, chid))) {
          LOG_WARN("failed to encode chid of easy netheader", K(ret));
        } else if (OB_FAIL(encode_i32(payload, playload_size, pos, reserved))) {
          LOG_WARN("failed to encode reserved  of easy netheader", K(ret));
        } else {
          // sess->r.opacket = payload;
            sess->c = conn;
            easy_buf_set_data(sess->pool, ebuf, payload, playload_size);
            easy_request_addbuf(&(sess->r), ebuf);
          // }
        }
      }

      if (OB_FAIL(ret)) {
        easy_session_destroy(sess);
      } else {
        ret = easy_connection_send_session(conn, sess);
        if (ret != EASY_OK) {
          LOG_WARN("failed to send easy session.", K(ret));
          eret = ret;
        } else {
          eret = EASY_OK;
        }
      }
    }
  }

  return eret;
}

uint64_t get_packet_id(easy_connection_t *c, void *packet)
{
  int64_t start_time = common::ObTimeUtility::current_time();
  abort_unless(NULL != c);
  abort_unless(NULL != packet);
  ObReqHandler *handler = packet_handler(c);
  abort_unless(NULL != handler);

  uint64_t packet_id = 0;
  if (!OB_ISNULL(handler)) {
    packet_id = handler->get_packet_id(c, packet);
  }
  const int64_t diff = common::ObTimeUtility::current_time() - start_time;
  if (diff > common::OB_EASY_HANDLER_COST_TIME) {
    LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "get_packet_id handler cost too much time", K(diff));
  }
  return packet_id;
}

void set_trace_info(easy_request_t *r, void *packet)
{
  abort_unless(NULL != r);
  abort_unless(NULL != packet);

  if (OB_ISNULL(r) || OB_ISNULL(r->ms) || OB_ISNULL(r->ms->c)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid arguement which is NULL");
  } else {
    ObReqHandler *handler = packet_handler(r->ms->c);
    if (!OB_ISNULL(handler)) {
      handler->set_trace_info(r, packet);
    } else {
      LOG_ERROR_RET(common::OB_SUCCESS, "set trace time");
    }
  }
}

int on_idle(easy_connection_t *c)
{
  abort_unless(c);
  ObReqHandler *handler = packet_handler(c);
  abort_unless(NULL != handler);

  int eret = EASY_OK;
  if (!OB_ISNULL(handler)) {
    int64_t start_time = common::ObTimeUtility::current_time();
    eret = handler->on_idle(c);
    const int64_t diff = common::ObTimeUtility::current_time() - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "on_idle handler cost too much time", K(diff));
    }
  }
  return eret;
}

void send_buf_done(easy_request_t *r)
{
  if (OB_ISNULL(r) || OB_ISNULL(r->ms) || OB_ISNULL(r->ms->c)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument which is NULL");
  } else {
    int64_t start_time = common::ObTimeUtility::current_time();
    ObReqHandler *handler = packet_handler(r->ms->c);
    if (!OB_ISNULL(handler)) {
      handler->send_buf_done(r);
    }
    const int64_t diff = common::ObTimeUtility::current_time() - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "send_buf_done handler cost too much time", K(diff));
    }
  }
}

void sending_data(easy_connection_t *c)
{
  if (OB_ISNULL(c)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument which is NULL");
  } else {
    int64_t start_time = common::ObTimeUtility::current_time();
    ObReqHandler *handler = packet_handler(c);
    if (!OB_ISNULL(handler)) {
      handler->sending_data(c);
    }
    const int64_t diff = common::ObTimeUtility::current_time() - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "sending_data handler cost too much time", K(diff));
    }
  }
}

int send_data_done(easy_connection_t *c)
{
  int eret = EASY_ERROR;
  if (OB_ISNULL(c)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument which is NULL");
  } else {
    int64_t start_time = common::ObTimeUtility::current_time();
    ObReqHandler *handler = packet_handler(c);
    if (!OB_ISNULL(handler)) {
      eret = handler->send_data_done(c);
    }
    const int64_t diff = common::ObTimeUtility::current_time() - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "send_data_done handler cost too much time", K(diff));
    }
  }
  return eret;
}

int on_redispatch(easy_connection_t *c)
{
  int eret = EASY_ERROR;
  if (OB_ISNULL(c)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument which is NULL");
  } else {
    int64_t start_time = common::ObTimeUtility::current_time();
    ObReqHandler *handler = packet_handler(c);
    if (!OB_ISNULL(handler)) {
      eret = handler->on_redispatch(c);
    }
    const int64_t diff = common::ObTimeUtility::current_time() - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "on_redispatch handler cost too much time", K(diff));
    }
  }
  return eret;
}

int on_close(easy_connection_t *c)
{
  // easy will ignore the return.
  RLOCAL(int64_t, on_close_tot);
  RLOCAL(int64_t, time_tot);
  int eret = EASY_ERROR;
  if (OB_ISNULL(c)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument which is NULL", K(eret));
  } else {
    int64_t start_time = common::ObTimeUtility::current_time();
    int64_t end_time = 0;
    ObReqHandler *handler = packet_handler(c);
    if (!OB_ISNULL(handler)) {
      eret = handler->on_close(c);
      end_time = common::ObTimeUtility::current_time();
      time_tot += (end_time - start_time);
      on_close_tot += 1;
    } else {
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "get packet handler fail", K(eret));
    }
    if (TC_REACH_TIME_INTERVAL(1000 * 1000)) {
      if (0 != on_close_tot) {
        RPC_LOG(DEBUG, "",
            K(*(&on_close_tot)), K(time_tot / on_close_tot), K(*(&time_tot)));
      }
      time_tot = 0;
      on_close_tot = 0;
    }
    const int64_t diff = end_time - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "on_close handler cost too much time", K(diff));
    }
  }
  return eret;
}

int cleanup(easy_request_t *r, void *apacket)
{
  int eret = EASY_ERROR;
  if (OB_ISNULL(r) || OB_ISNULL(r->ms)) {
    LOG_ERROR_RET(common::OB_INVALID_ARGUMENT, "invalid argument", K(r), K(apacket));
  } else {
    int64_t start_time = common::ObTimeUtility::current_time();
    ObReqHandler *handler = packet_handler(r->ms->c);
    if (!OB_ISNULL(handler)) {
      eret = handler->cleanup(r, apacket);
    } else {
      LOG_ERROR_RET(common::OB_ERR_UNEXPECTED, "handler is NULL", K(handler));
    };
    const int64_t diff = common::ObTimeUtility::current_time() - start_time;
    if (diff > common::OB_EASY_HANDLER_COST_TIME) {
      LOG_WARN_RET(common::OB_ERR_TOO_MUCH_TIME, "cleanup handler cost too much time", K(diff));
    }
  }
  return eret;
}

} // end of namespace easy
} // end of namespace oceanbase
