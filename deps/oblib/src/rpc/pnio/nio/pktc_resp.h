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

static void pktc_do_cb(pktc_t* io, pktc_cb_t* cb, pktc_msg_t* m) {
  cb->resp_cb(cb, m->payload, m->sz);
}

static void pktc_do_cb_exception(pktc_t* io, pktc_cb_t* cb) {
  pktc_req_t* req = cb->req;
  if (req) {
    // pktc_flush_cb hasn't be executed
    pktc_sk_t* sk = req->sk;
    if (NULL != sk && PNIO_OK == wq_delete(&sk->wq, &req->link)) {
      rk_warn("pktc_req hasn't be flushed before callback, pkt_id=%ld, sock=%p, code=%d", cb->id, sk, cb->errcode);
      // reset the error code to indicate that the request has not been sent out
      if (cb->errcode == PNIO_DISCONNECT) {
        cb->errcode = PNIO_DISCONNECT_NOT_SENT_OUT;
      } else if (cb->errcode == PNIO_TIMEOUT) {
        cb->errcode = PNIO_TIMEOUT_NOT_SENT_OUT;
      }
      pktc_flush_cb(io, req);
    }
  }
  cb->resp_cb(cb, NULL, 0);
}

static void pktc_resp_cb_on_sk_destroy(pktc_t* io, pktc_sk_t* s) {
  dlink_for(&s->cb_head, p) {
    pktc_cb_t* cb = structof(p, pktc_cb_t, sk_dlink);
    ihash_del(&io->cb_map, &cb->id);
    dlink_delete(&cb->timer_dlink);
    rk_info("resp_cb on sk_destroy: packet_id=%lu s=%p", cb->id, s);
    cb->errcode = PNIO_DISCONNECT;
    pktc_do_cb_exception(io, cb);
  }
}

static void pktc_resp_cb_on_timeout(time_wheel_t* tw, dlink_t* l) {
  pktc_cb_t* cb = structof(l, pktc_cb_t, timer_dlink);
  pktc_t* io = structof(tw, pktc_t, cb_tw);
  ihash_del(&io->cb_map, &cb->id);
  dlink_delete(&cb->sk_dlink);
  rk_debug("resp_cb on timeout: packet_id=%lu expire_us=%ld", cb->id, cb->expire_us);
  cb->errcode = PNIO_TIMEOUT;
  pktc_do_cb_exception(io, cb);
}

static void pktc_resp_cb_on_post_fail(pktc_t* io, pktc_cb_t* cb) {
  pktc_do_cb_exception(io, cb);
}

static void pktc_resp_cb_on_msg(pktc_t* io, pktc_msg_t* msg) {
  uint64_t id = pktc_get_id(msg);
  link_t* hlink = ihash_del(&io->cb_map, &id);
  if (hlink) {
    pktc_cb_t* cb = structof(hlink, pktc_cb_t, hash_link);
    dlink_delete(&cb->timer_dlink);
    dlink_delete(&cb->sk_dlink);
    pktc_do_cb(io, cb, msg);
  } else {
    rk_info("resp cb not found: packet_id=%lu", id);
  }
}

static void pktc_resp_cb_on_terminate(pktc_t* io, uint64_t id) {
  link_t* hlink = ihash_del(&io->cb_map, &id);
  if (hlink) {
    pktc_cb_t* cb = structof(hlink, pktc_cb_t, hash_link);
    dlink_delete(&cb->timer_dlink);
    dlink_delete(&cb->sk_dlink);
    cb->errcode = PNIO_PKT_TERMINATE;
    pktc_do_cb_exception(io, cb);
  } else {
    rk_info("resp cb not found: packet_id=%lu", id);
  }
}

static int pktc_sk_handle_msg(pktc_sk_t* s, pktc_msg_t* m) {
  pktc_t* io = structof(s->fty, pktc_t, sf);
  pktc_resp_cb_on_msg(io, m);
  ib_consumed(&s->ib, m->sz);
  return 0;
}
