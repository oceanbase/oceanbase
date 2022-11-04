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

#ifndef _OCEABASE_RPC_FRAME_OBI_EASY_PACKET_HANDLER_H_
#define _OCEABASE_RPC_FRAME_OBI_EASY_PACKET_HANDLER_H_

#include "io/easy_io_struct.h"

namespace oceanbase
{
namespace rpc
{
namespace frame
{

class ObIEasyPacketHandler
{
public:
  virtual ~ObIEasyPacketHandler() {}
  virtual void *decode(easy_message_t *m) = 0;
  virtual int encode(easy_request_t *r, void *packet) = 0;
  virtual int process(easy_request_t *r)  = 0;
  virtual int batch_process(easy_message_t *m)  = 0;
  virtual int new_packet(easy_connection_t *c) = 0;
  virtual uint64_t get_packet_id(easy_connection_t *c, void *packet) = 0;
  virtual void set_trace_info(easy_request_t *r, void *packet) = 0;
  virtual int on_connect(easy_connection_t *c) = 0;
  virtual int on_disconnect(easy_connection_t *c) = 0;
  virtual int on_idle(easy_connection_t *c) = 0;
  virtual void send_buf_done(easy_request_t *r) = 0;
  virtual void sending_data(easy_connection_t *c) = 0;
  virtual int send_data_done(easy_connection_t *c) = 0;
  virtual int on_redispatch(easy_connection_t *c) = 0;
  virtual int on_close(easy_connection_t *c) = 0;
  virtual int cleanup(easy_request_t *r, void *apacket) = 0;
  // virtual void on_ioth_start(void *arg) = 0;
}; // end of class ObIEasyPacketHandler

} // end of namespace frame
} // end of namespace rpc
} // end of namespace oceanbase

#endif /* _OCEABASE_RPC_FRAME_OBI_EASY_PACKET_HANDLER_H_ */
