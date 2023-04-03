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

#include "lib/utility/ob_utility.h"
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/mman.h>
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
int ob_get_abs_timeout(const uint64_t timeout_us, struct timespec &abs_timeout)
{
  int ret = OB_SUCCESS;
  struct timeval curtime;
  if (0 != gettimeofday(&curtime, NULL)) {
    ret = OB_ERR_SYS;
  } else {
    uint64_t us = (static_cast<int64_t>(curtime.tv_sec) *
                  static_cast<int64_t>(1000000) +
                  static_cast<int64_t>(curtime.tv_usec) +
                  timeout_us);
    abs_timeout.tv_sec = static_cast<int>(us / static_cast<uint64_t>(1000000));
    abs_timeout.tv_nsec = static_cast<int>(us % static_cast<uint64_t>(1000000)) * 1000;
  }
  return ret;
}

int64_t lower_align(int64_t input, int64_t align)
{
  int64_t ret = input;
  ret = (input + align - 1) & ~(align - 1);
  ret = ret - ((ret - input + align - 1) & ~(align - 1));
  return ret;
};

int64_t upper_align(int64_t input, int64_t align)
{
  int64_t ret = input;
  ret = (input + align - 1) & ~(align - 1);
  return ret;
};

char* upper_align_buf(char *in_buf, int64_t align)
{
  char *out_buf = NULL;

  out_buf = reinterpret_cast<char*>(upper_align(reinterpret_cast<int64_t>(in_buf), align));
  return out_buf;
}

int64_t ob_pwrite(const int fd, const char *buf, const int64_t count, const int64_t offset)
{
  int64_t length2write = count;
  int64_t offset2write = 0;
  int64_t write_ret = 0;
  while (length2write > 0) {
    write_ret = ::pwrite(fd, (char *)buf + offset2write, length2write, offset + offset2write);
    if (0 >= write_ret) {
      if (errno == EINTR) { // Blocking IO does not need to judge EAGAIN
        continue;
      } else {
        offset2write = -1;
        break;
      }
    } else {
      length2write -= write_ret;
      offset2write += write_ret;
    }
  }
  return offset2write;
}

int64_t ob_pread(const int fd, char *buf, const int64_t count, const int64_t offset)
{
  int64_t length2read = count;
  int64_t offset2read = 0;
  int64_t read_ret = 0;
  while (length2read > 0) {
    for (int64_t retry = 0; retry < 3;) {
      read_ret = ::pread(fd, (char *)buf + offset2read, length2read, offset + offset2read);
      if (0 > read_ret) {
        if (errno == EINTR) { // Blocking IO does not need to judge EAGAIN
          continue;
        }
        retry++;
      } else {
        break;
      }
    }
    if (0 >= read_ret) {
      if (errno == EINTR) { // Blocking IO does not need to judge EAGAIN
        continue;
      }
      if (0 > read_ret) {
        offset2read = -1;
      }
      break;
    }
    offset2read += read_ret;
    if (length2read > read_ret) {
      length2read -= read_ret;
    } else {
      break;
    }
  }
  return offset2read;
}

int mprotect_page(const void *mem_ptr, int64_t len, int prot, const char *addr_name)
{
  int ret = OB_SUCCESS;
  int64_t mem_start = reinterpret_cast<int64_t>(mem_ptr);
  int64_t mem_end = mem_start + len;
  int64_t pagesize = sysconf(_SC_PAGE_SIZE);
  const char *action = "protect";
  if ((prot & PROT_WRITE) && (prot & PROT_READ)) {
    action = "release";
  }
  if (mem_start != 0) {
    int64_t page_start = mem_start - (mem_start % pagesize);
    int64_t page_end = mem_end - (mem_end % pagesize) + pagesize;
    int64_t page_cnt = (page_end - page_start) / pagesize;
    void *mem_start_addr = reinterpret_cast<void*>(mem_start);
    void *mem_end_addr = reinterpret_cast<void*>(mem_end);
    void *page_start_addr = reinterpret_cast<void*>(page_start);
    void *page_end_addr = reinterpret_cast<void*>(page_end);
    if (page_cnt <= 0) {
      //The page to mprotect exceeds the protected address, mprotect cannot be added
      LIB_LOG(INFO, "can not mprotect mem_ptr", K(mem_start), K(pagesize), K(len), KCSTRING(addr_name), KCSTRING(action));
    } else if (0 != mprotect(page_start_addr, pagesize * page_cnt, prot)) {
      ret = OB_ERR_UNEXPECTED;
      const char *errmsg = strerror(errno);
      LIB_LOG(WARN, "mprotect mem_ptr failed",
              K(ret), KCSTRING(errmsg), K(mem_start_addr), K(page_start_addr), K(pagesize),
              K(page_cnt), K(prot), KCSTRING(addr_name), KCSTRING(action));
    } else {
      LIB_LOG(INFO, "mprotect success", K(mem_start_addr), K(mem_end_addr),
              K(page_start_addr), K(page_end_addr),
              K(pagesize), K(page_cnt), KCSTRING(addr_name), KCSTRING(action));
    }
  }
  return ret;
}
} //common
} //oceanbase
