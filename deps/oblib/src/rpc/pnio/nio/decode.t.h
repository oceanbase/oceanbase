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

static int my_sk_do_decode(my_sk_t* s, my_msg_t* msg, int64_t* avail_bytes) {
  int err = 0;
  void* b = NULL;
  int64_t sz = sizeof(easy_head_t);
  int64_t req_sz = sz;
  while(0 == (err = my_sk_read(&b, s, sz, avail_bytes))
        && NULL != b && (req_sz = my_decode((char*)b, sz)) > 0 && req_sz > sz) {
    sz = req_sz;
    if (0 == s->sk_diag_info.last_read_time) {
      s->sk_diag_info.last_read_time = rk_get_us();
    }
  }
  if (req_sz <= 0) {
    err = EINVAL;
  }
  if (0 == err) {
    *msg = (my_msg_t) { .sz = req_sz, .payload = (char*)b, .ctime_us = s->sk_diag_info.last_read_time};
    if (NULL != b) {
      int64_t read_time = 0;
      int64_t cur_time = rk_get_us();
      if (0 == s->sk_diag_info.last_read_time) {
        s->sk_diag_info.last_read_time = cur_time;
        msg->ctime_us = cur_time;
      } else {
        read_time = cur_time - s->sk_diag_info.last_read_time;
      }
      if (read_time > 100*1000) {
        uint64_t pcode = 0;
        uint64_t tenant_id = 0;
        if (req_sz > sizeof(easy_head_t) + 24) {
          pcode = *(uint64_t*)((char*)b + sizeof(easy_head_t));
          tenant_id = *(uint64_t*)((char*)b + sizeof(easy_head_t) + 16);
        }
        rk_warn("read pkt cost too much time, read_time=%ld, pkt_size=%ld, conn=%p, pcode=0x%lx, tenant_id=%ld", read_time, req_sz, s, pcode, tenant_id);
      }
      s->sk_diag_info.read_cnt ++;
      s->sk_diag_info.read_size += req_sz;
      s->sk_diag_info.read_time += read_time;
      s->sk_diag_info.last_read_time = 0;
    }
  }
  return err;
}
