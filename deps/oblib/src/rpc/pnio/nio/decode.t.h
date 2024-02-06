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
  }
  if (req_sz <= 0) {
    err = EINVAL;
  }
  if (0 == err) {
    *msg = (my_msg_t) { .sz = req_sz, .payload = (char*)b };
  }
  return err;
}
