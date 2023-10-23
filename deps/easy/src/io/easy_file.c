#undef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#include <unistd.h>
#include <sys/sendfile.h>
#include "io/easy_io.h"
#include "io/easy_file.h"
#include "io/easy_connection.h"
#include "io/easy_message.h"
#include "io/easy_request.h"
#include "io/easy_file.h"
#include "io/easy_client.h"
#include "io/easy_socket.h"
#include "io/easy_log.h"
#include <fcntl.h>

easy_file_task_t *easy_file_task_create(easy_request_t *r, int fd, int bufsize)
{
    struct stat             fs;
    easy_file_task_t        *ft;

    ft = (easy_file_task_t *)easy_pool_calloc(r->ms->pool, sizeof(easy_file_task_t));

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

    if (bufsize == 0) bufsize = EASY_MAX_FILE_BUFFER;

    ft->bufsize = easy_min(ft->count, bufsize);
    ft->b = easy_buf_create(r->ms->pool, ft->bufsize);
    if (ft->b == NULL)
        return NULL;
    ft->buffer = ft->b->pos;

    return ft;
}

void easy_file_task_set(easy_file_task_t *ft, char *buffer, int64_t offset, int64_t bufsize, void *args)
{
    ft->buffer = buffer;
    ft->offset = offset;
    ft->count = ft->bufsize = bufsize;
    ft->args = args;
}

void easy_file_task_reset(easy_file_task_t *ft, int type)
{
    easy_list_init(&ft->b->node);
    ft->b->pos = ft->buffer;
    ft->b->last = ft->b->pos;
    ((easy_file_buf_t *)ft->b)->flags = type;
}

/**
 * file_task process
 */
/*
int easy_file_task_process(easy_file_task_t *ft)
{
    int64_t                 rc = 0;
    long                    offset = ft->offset;

    do {
        switch(ft->type) {
        case EASY_FILE_WRITE:
            rc = pwrite(ft->fd, ft->buf, ft->count, offset);
            break;

        case EASY_FILE_READ:
            rc = pread(ft->fd, ft->buf, ft->count, offset);
            break;

        case EASY_FILE_SENDFILE:
        case EASY_FILE_WILLNEED:
            rc = posix_fadvise(ft->fd, offset, ft->count, POSIX_FADV_WILLNEED);
            break;
        }
    } while(rc == -1 && errno == EINTR);

    // 结果处理
    if (rc == -1) {
        ft->ret = (errno > 0) ? -errno : EASY_ERROR;
    } else {
        ft->ret = rc;
    }

    ft->done = 1;
    return rc;
}
*/
