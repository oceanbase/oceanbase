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

#ifndef _OB_VIRTUAL_CS_PROTOCOL_PROCESSOR_H_
#define _OB_VIRTUAL_CS_PROTOCOL_PROCESSOR_H_

#include "lib/ob_define.h"
#include "rpc/frame/ob_req_handler.h"
#include "rpc/frame/ob_req_deliver.h"

namespace oceanbase {
namespace rpc {
class ObPacket;
}
namespace obmysql {
class ObMySQLHandler;
class ObVirtualCSProtocolProcessor {
public:
  ObVirtualCSProtocolProcessor(ObMySQLHandler& handler) : handler_(handler)
  {}
  virtual ~ObVirtualCSProtocolProcessor()
  {}

  virtual int decode(easy_message_t* m, rpc::ObPacket*& pkt) = 0;
  virtual int process(easy_request_t* r, bool& need_read_more) = 0;

protected:
  inline int set_next_read_len(easy_message_t* m, const int64_t fallback_len, const int64_t next_read_len);

protected:
  ObMySQLHandler& handler_;
};

inline int ObVirtualCSProtocolProcessor::set_next_read_len(
    easy_message_t* m, const int64_t fallback_len, const int64_t next_read_len)
{
  INIT_SUCC(ret);
  if (OB_UNLIKELY(NULL == m) || OB_UNLIKELY(NULL == m->input) || OB_UNLIKELY(fallback_len < 0) ||
      OB_UNLIKELY(next_read_len <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    m->input->pos -= fallback_len;
    m->next_read_len = static_cast<int>(next_read_len);
  }
  return ret;
}

}  // end of namespace obmysql
}  // end of namespace oceanbase

#endif /* _OB_VIRTUAL_CS_PROTOCOL_PROCESSOR_H_ */
