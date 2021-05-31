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

#define _GNU_SOURCE 1
#include <unistd.h>
#include <stdio.h>
#include <errno.h>
#include <dlfcn.h>
#include <libaio.h>
#include <libconfig.h>
#include <stdlib.h>
#include <signal.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <fcntl.h>

static ssize_t (*real_pwrite)(int fd, const void* buf, size_t count, off_t offset);

struct pwrite_conf_t {
  int start_fail_index_;
  int fail_cnt_;
  int reset_call_cnt_;
  int error_;
} pwrite_conf_t;

static struct pwrite_conf_t pwrite_conf;
static int is_inited = 0;
static pthread_mutex_t init_mutex;
static time_t load_conf_time = 0;
static pthread_mutex_t load_conf_mutex;
static int call_cnt;

void io_init(void)
{
  *(void**)&real_pwrite = dlsym(RTLD_NEXT, "pwrite");
  printf("init real pwrite succeed\n");
}

void check_init()
{
  pthread_mutex_lock(&init_mutex);
  if (0 == is_inited) {
    io_init();
    is_inited = 1;
  }
  pthread_mutex_unlock(&init_mutex);
}

void load_conf()
{
  int fd = open("pwrite_conf", O_RDONLY);
  char buf[64];
  memset(buf, 0, sizeof(buf));
  read(fd, buf, sizeof(buf));
  close(fd);
  sscanf(buf,
      "%d,%d,%d,%d",
      &pwrite_conf.start_fail_index_,
      &pwrite_conf.fail_cnt_,
      &pwrite_conf.reset_call_cnt_,
      &pwrite_conf.error_);
  if (pwrite_conf.reset_call_cnt_) {
    call_cnt = 0;
    printf("reset call cnt succeed\n");
  }
  printf("load pwrite conf succeed, %s\n", buf);
}

void check_load_conf()
{
  pthread_mutex_lock(&load_conf_mutex);
  time_t now = time(NULL);
  if (now - load_conf_time > 2) {
    load_conf();
    load_conf_time = now;
  }
  pthread_mutex_unlock(&load_conf_mutex);
}

ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset)
{
  ssize_t rt = 0;
  check_init();
  check_load_conf();
  if (call_cnt < pwrite_conf.start_fail_index_ || call_cnt >= pwrite_conf.start_fail_index_ + pwrite_conf.fail_cnt_) {
    errno = 0;
    rt = real_pwrite(fd, buf, count, offset);
  } else {
    errno = pwrite_conf.error_;
    rt = 0;
  }
  ++call_cnt;
  printf("pwrite called");
  return rt;
}
