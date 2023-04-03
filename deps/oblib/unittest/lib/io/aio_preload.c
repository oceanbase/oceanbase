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

static int (*real_io_getevents)(io_context_t ctx_id, long min_nr, long nr, struct io_event *events, struct timespec *timeout);
static int (*real_io_submit)(io_context_t ctx, long nr, struct iocb **iocbpp);

struct aio_conf_t
{
  int work_fd_;
  int io_submit_failed_;
  int io_hang_;
  int io_timeout_;
} aio_conf_t;

static struct aio_conf_t aio_conf;
static int is_inited = 0;
static pthread_mutex_t init_mutex;
static time_t load_conf_time = 0;
static pthread_mutex_t load_conf_mutex;

void io_init(void)
{
  void *handle = dlopen("libaio.so", RTLD_LAZY);
  *(void**) &real_io_getevents = dlsym(handle, "io_getevents");
  *(void**) &real_io_submit = dlsym(handle, "io_submit");
  printf("init read aio func succeed\n");
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
  int fd = open("aio_conf", O_RDONLY);
  char buf[64];
  memset(buf, 0, sizeof(buf));
  read(fd, buf, sizeof(buf));
  close(fd);
  sscanf(buf, "%d,%d,%d,%d", &aio_conf.work_fd_, &aio_conf.io_submit_failed_, &aio_conf.io_hang_, &aio_conf.io_timeout_);
  printf("init read aio func succeed, %s\n", buf);
}

void check_load_conf()
{
  pthread_mutex_lock(&load_conf_mutex);
  time_t now = time(NULL);
  if (now - load_conf_time > 5) {
    load_conf();
    load_conf_time = now;
  }
  pthread_mutex_unlock(&load_conf_mutex);

}
int io_submit(io_context_t ctx_id, long nr, struct iocb **iocbpp)
{
  int ret = 0;
  int is_triggered = 0;
  check_init();
  check_load_conf();
  if (iocbpp[0]->aio_fildes == aio_conf.work_fd_) {
    is_triggered = 1;
  }

  if (!is_triggered || !aio_conf.io_submit_failed_) {
    ret = (*real_io_submit)(ctx_id, nr, iocbpp);
  } else {
    ret = EAGAIN;
  }
  return ret;
}

int io_getevents(io_context_t ctx_id, long min_nr, long nr, struct io_event *events, struct timespec *timeout)
{
  int ret = 0;
  int is_triggered = 0;
  int nevent = 0;
  check_init();
  check_load_conf();
  nevent = (*real_io_getevents)(ctx_id, min_nr, nr, events, timeout);
  if (nevent < 0) {
    printf("real_io_getevents failed\n");
  } else if (0 == nevent) {
    //printf("real_io_getevents get nothing\n");
  } else {
    struct iocb *cb = events[0].obj;
    is_triggered = aio_conf.work_fd_ == cb->aio_fildes;
    if (!is_triggered) {
      ret = nevent;
    } else if (aio_conf.io_hang_) {
      ret = 0; // 0 event finish
    } else if (aio_conf.io_timeout_) {
      ::usleep(10 * 1000 * 1000); // sleep 10s
      printf("finish io_getevents");
      ret = nevent;
    }
  }
  return ret;
}
