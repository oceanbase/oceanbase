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

int make_fd_nonblocking(int fd)
{
  int err = 0;
  int flags = 0;
  if ((flags = fcntl(fd, F_GETFL, 0)) < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    err = -errno;
  }
  return err;
}

bool is_pipe(int fd) {
  struct stat st;
  return 0 == fstat(fd, &st) && S_ISFIFO(st.st_mode);
}

ssize_t uintr_pread(int fd, char* buf, int64_t size, int64_t offset) {
  ssize_t cnt = 0;
  while(cnt < size) {
    ssize_t rbytes = pread(fd, buf + cnt, size - cnt, offset + cnt);
    if (rbytes > 0) {
      cnt += rbytes;
    } else if (rbytes < 0) {
      if (errno != EINTR) {
        cnt = -1;
        break;
      }
    } else {
      break;
    }
  }
  return cnt;
}

ssize_t uintr_pwrite(int fd, const char* buf, int64_t size, int64_t offset) {
  ssize_t cnt = 0;
  while(cnt < size) {
    ssize_t wbytes = pwrite(fd, buf + cnt, size - cnt, offset + cnt);
    if (wbytes > 0) {
      cnt += wbytes;
    } else if (wbytes < 0) {
      if (errno != EINTR) {
        cnt = -1;
        break;
      }
    } else {
      break;
    }
  }
  return cnt;
}

ssize_t fsize(const char* path) {
  ssize_t size = 0;
  struct stat _stat;
  if (NULL != path && 0 == stat(path, &_stat)) {
    size = _stat.st_size;
  }
  return size;
}

char* fmap(const char* path, int oflag, int64_t size) {
  char* buf = NULL;
  int fd = 0;
  bool writable = O_RDONLY != oflag;
  if (NULL != path && (fd = open(path, oflag, S_IRWXU|S_IRGRP)) >= 0) {
    if (size < 0) {
      size = fsize(path);
    } else {
      if (writable && 0 != ftruncate(fd, size)) {
        size = -1;
      }
    }
    if (size > 0) {
      if (MAP_FAILED == (buf = (char*)mmap(NULL, size, PROT_READ|(writable? PROT_WRITE: 0), MAP_SHARED, fd, 0))) {
        buf = NULL;
      }
    }
  }
  return buf;
}

#ifndef RW_ERRSIM_FREQ
#define RW_ERRSIM_FREQ 100
#endif
ssize_t uintr_read(int fd, char* buf, size_t size) {
  ssize_t bytes = 0;
#ifdef PNIO_ERRSIM
  int rand_value = rand();
  if (rand_value % RW_ERRSIM_FREQ == 0) {
    rk_warn("uintr_read return 0. rand_value = %d\n", rand_value);
    errno = EIO;
    return bytes;
  }
#endif
  while((bytes = ussl_read(fd, buf, size)) < 0 && EINTR == errno)
    ;
  return bytes;
}

ssize_t uintr_readv(int fd, struct iovec* iov, int cnt) {
  ssize_t bytes = 0;
  while((bytes = readv(fd, iov, cnt)) < 0 && EINTR == errno)
    ;
  return bytes;
}

ssize_t uintr_write(int fd, const char* buf, size_t size) {
  ssize_t bytes = 0;
  while((bytes = write(fd, buf, size)) < 0 && EINTR == errno)
    ;
  return bytes;
}

ssize_t uintr_writev(int fd, struct iovec* iov, int cnt) {
  ssize_t bytes = 0;
#ifdef PNIO_ERRSIM
  int rand_value = rand();
  if (rand_value % RW_ERRSIM_FREQ == 0) {
    rk_warn("uintr_writev return 0. rand_value = %d\n", rand_value);
    errno = EIO;
    return bytes;
  }
#endif
  while((bytes = ussl_writev(fd, iov, cnt)) < 0 && EINTR == errno)
    ;
  return bytes;
}
