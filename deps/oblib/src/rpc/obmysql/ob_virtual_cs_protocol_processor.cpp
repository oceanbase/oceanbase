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

#define USING_LOG_PREFIX RPC_OBMYSQL
#include "rpc/obmysql/ob_virtual_cs_protocol_processor.h"
#include "rpc/obmysql/obsm_struct.h"


using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::observer;

class ObCSEasyMemPool: public ObICSMemPool
{
public:
  ObCSEasyMemPool(easy_pool_t* easy_pool): easy_pool_(easy_pool) {}
  virtual ~ObCSEasyMemPool() {}
  void* alloc(int64_t sz) { return easy_pool_alloc(easy_pool_, sz); }
private:
  easy_pool_t* easy_pool_;
};

int ObVirtualCSProtocolProcessor::easy_decode(easy_message_t *m, rpc::ObPacket *&pkt)
{
  int ret = OB_SUCCESS;
  ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(m->c->user_data);
  ObCSEasyMemPool pool(m->pool);
  int64_t next_read_bytes = 0;
  if (OB_FAIL(do_decode(*conn, pool, (const char*&)m->input->pos, m->input->last, pkt, next_read_bytes))) {
    LOG_ERROR("fail do_decode", K(ret));
  } else {
    if (next_read_bytes > 0 ) {
      m->next_read_len = next_read_bytes;
    }
  }
  return ret;
}

int ObVirtualCSProtocolProcessor::easy_process(easy_request_t *r, bool &need_read_more)
{
  int ret = OB_SUCCESS;
  ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(r->ms->c->user_data);
  ObCSEasyMemPool pool(r->ms->pool);
  need_read_more = true;
  if (!conn->is_in_authed_phase() && !conn->is_in_auth_switch_phase()) {
    need_read_more = false;
  } else if (OB_FAIL(do_splice(*conn, pool, r->ipacket, need_read_more))) {
    LOG_ERROR("fail to splice mysql packet", K(ret));
  }
  return ret;
}

