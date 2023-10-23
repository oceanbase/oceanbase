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

void evfd_signal(int fd) {
  int64_t c = 1;
  write(fd, &c, sizeof(c));
}

int evfd_drain(int fd) {
  int64_t c = 0;
  return (read(fd, (char*)&c, sizeof(c)) < 0 && EAGAIN != errno)? errno: 0;
}

int evfd_init(eloop_t* ep, evfd_t* s, handle_event_t handle) {
  int err = 0;
  sk_init((sock_t*)s, NULL, (void*)handle, eventfd(0, EFD_NONBLOCK|EFD_CLOEXEC));
  if (s->fd < 0) {
    err = EIO;
  } else {
    err = eloop_regist(ep, (sock_t*)s, EPOLLIN);
  }
  if (0 != err && s->fd >= 0) {
    close(s->fd);
  }
  return err;
}
