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

static int sk_translate_io_error(sock_t* s, int64_t bytes, uint32_t epbit) {
  if (bytes > 0) {
    return 0;
  } else if (bytes < 0) {
    if (EWOULDBLOCK == errno || EAGAIN == errno) {
      skclear(s, epbit);
      return EAGAIN;
    } else {
      return errno;
    }
  } else {
    return ENODATA;
  }
}

static int sk_after_io(sock_t* s, const char* buf, int64_t bytes, uint32_t epbit) {
  int ret = sk_translate_io_error(s, bytes, epbit);
  //sk_check_io(&s->debug_check, buf, bytes, epbit);
  return ret;
}
static int sk_after_iov(sock_t* s, struct iovec* iov, int cnt, int64_t bytes, uint32_t epbit) {
  int ret = sk_translate_io_error(s, bytes, epbit);
  //sk_check_iov(&s->debug_check, iov, cnt, bytes, epbit);
  return ret;
}

static int sk_after_read(sock_t* s, const char* buf, int64_t bytes) { return sk_after_io(s, buf, bytes, EPOLLIN); }
static int sk_after_write(sock_t* s, const char* buf, int64_t bytes) { return sk_after_io(s, buf, bytes, EPOLLOUT); }
static int sk_after_readv(sock_t* s, struct iovec* iov, int cnt, int64_t bytes) { return sk_after_iov(s, iov, cnt, bytes, EPOLLIN); }
static int sk_after_writev(sock_t* s, struct iovec* iov, int cnt, int64_t bytes) { return sk_after_iov(s, iov, cnt, bytes, EPOLLOUT); }

int sk_read(sock_t* s, char* buf, size_t size, ssize_t* rbytes) {
  PNIO_DELAY_WARN(STAT_TIME_GUARD(eloop_read_count, eloop_read_time));
  return sk_after_read(s, buf, (*rbytes = uintr_read(s->fd, buf, size)));
}

int sk_readv(sock_t* s, struct iovec* iov, int cnt, ssize_t* rbytes) {
  PNIO_DELAY_WARN(STAT_TIME_GUARD(eloop_read_count, eloop_read_time));
  return sk_after_readv(s, iov, cnt, (*rbytes = uintr_readv(s->fd, iov, cnt)));
}

int sk_write(sock_t* s, const char* buf, size_t size, ssize_t* wbytes) {
  PNIO_DELAY_WARN(STAT_TIME_GUARD(eloop_write_count, eloop_write_time));
  return sk_after_write(s, buf, (*wbytes = uintr_write(s->fd, buf, size)));
}

int sk_writev(sock_t* s, struct iovec* iov, int cnt, ssize_t* wbytes) {
  PNIO_DELAY_WARN(STAT_TIME_GUARD(eloop_write_count, eloop_write_time));
  return sk_after_writev(s, iov, cnt, (*wbytes = uintr_writev(s->fd, iov, cnt)));
}
