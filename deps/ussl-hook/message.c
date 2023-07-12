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

# define USSL_DEFAULT_VERSION 1

int send_negotiation_message(int fd, const char *b, int sz)
{
  int err = 0;
  char buf[USSL_BUF_LEN];
  negotiation_head_t *h = (typeof(h))buf;
  if (sz + sizeof(*h) > sizeof(buf)) {
    err = -EINVAL;
  } else {
    h->magic = NEGOTIATION_MAGIC;
    h->version = USSL_DEFAULT_VERSION;
    h->len = sz;
    memcpy(h + 1, b, sz);
    ssize_t wbytes = 0;
    while ((wbytes = libc_write(fd, buf, sizeof(*h) + sz)) < 0 && EINTR == errno)
      ;
    if (wbytes != sz + sizeof(*h)) {
      err = -EIO;
    }
  }
  return err;
}