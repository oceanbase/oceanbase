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

#include "ussl-deps.h"
#include <dlfcn.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/stat.h>

static __attribute__((constructor(101))) void init_libc_func_ptr()
{
#define INIT_LIBC_FUNC_PTR(fname) libc_##fname = (typeof(libc_##fname))dlsym(RTLD_NEXT, #fname)
  INIT_LIBC_FUNC_PTR(setsockopt);
  INIT_LIBC_FUNC_PTR(listen);
  INIT_LIBC_FUNC_PTR(connect);
  INIT_LIBC_FUNC_PTR(accept);
  INIT_LIBC_FUNC_PTR(accept4);
  INIT_LIBC_FUNC_PTR(epoll_ctl);
  INIT_LIBC_FUNC_PTR(read);
  INIT_LIBC_FUNC_PTR(write);
  INIT_LIBC_FUNC_PTR(close);
  INIT_LIBC_FUNC_PTR(writev);
}

int make_socket_non_blocking(int fd)
{
  int ret = 0, flags = 0;
  flags = fcntl(fd, F_GETFL, 0);
  if (-1 == flags) {
    ussl_log_error("call fcntl with F_GETFL failed, errno:%d", errno);
    ret = -1;
  } else {
    flags |= O_NONBLOCK;
    if (-1 == fcntl(fd, F_SETFL, flags)) {
      ussl_log_error("call fcntl with F_SETFL failed, errno:%d", errno);
      ret = -1;
    }
  }
  return ret;
}
