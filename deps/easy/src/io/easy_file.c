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

#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#include <unistd.h>
#include <sys/sendfile.h>
#include "io/easy_io.h"
#include "io/easy_connection.h"
#include "io/easy_message.h"
#include "io/easy_request.h"
#include "io/easy_file.h"
#include "io/easy_client.h"
#include "io/easy_socket.h"
#include "io/easy_log.h"
#include <fcntl.h>

easy_file_task_t* easy_file_task_create(easy_request_t* r, int fd, int bufsize)
{
  struct stat fs;
  easy_file_task_t* ft;

  ft = (easy_file_task_t*)easy_pool_calloc(r->ms->pool, sizeof(easy_file_task_t));

  if (ft == NULL)
    return NULL;

  ft->fd = fd;

  if (bufsize < 0) {
    ft->bufsize = -1;
    ft->b = NULL;
    ft->buffer = NULL;
    ft->count = -1;
    return ft;
  }

  if (fstat(fd, &fs) == 0)
    ft->count = fs.st_size;

  if (bufsize == 0)
    bufsize = EASY_MAX_FILE_BUFFER;

  ft->bufsize = easy_min(ft->count, bufsize);
  ft->b = easy_buf_create(r->ms->pool, ft->bufsize);
  if (ft->b == NULL) {
    return NULL;
  }
  ft->buffer = ft->b->pos;

  if (ft->b == NULL)
    return NULL;

  return ft;
}

void easy_file_task_set(easy_file_task_t* ft, char* buffer, int64_t offset, int64_t bufsize, void* args)
{
  ft->buffer = buffer;
  ft->offset = offset;
  ft->count = ft->bufsize = bufsize;
  ft->args = args;
}

void easy_file_task_reset(easy_file_task_t* ft, int type)
{
  easy_list_init(&ft->b->node);
  ft->b->pos = ft->buffer;
  ft->b->last = ft->b->pos;
  ((easy_file_buf_t*)ft->b)->flags = type;
}
