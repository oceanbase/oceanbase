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

#include <unistd.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extern int make_fd_nonblocking(int fd);

extern bool is_pipe(int fd);
extern int64_t fsize(const char* path);
extern char* fmap(const char* path, int oflag, int64_t size);
extern ssize_t uintr_pread(int fd, char* buf, int64_t size, int64_t offset);
extern ssize_t uintr_pwrite(int fd, const char* buf, int64_t size, int64_t offset);
extern ssize_t uintr_read(int fd, char* buf, size_t size);
extern ssize_t uintr_readv(int fd, struct iovec* iov, int cnt);
extern ssize_t uintr_write(int fd, const char* buf, size_t size);
extern ssize_t uintr_writev(int fd, struct iovec* iov, int cnt);
