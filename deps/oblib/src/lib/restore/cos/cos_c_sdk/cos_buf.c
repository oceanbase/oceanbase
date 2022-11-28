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

#include "cos_buf.h"
#include "cos_log.h"
#include <apr_file_io.h>

cos_buf_t *cos_create_buf(cos_pool_t *p, int size)
{
    cos_buf_t* b;

    b = cos_palloc(p, sizeof(cos_buf_t) + size);
    if (b == NULL) {
        return NULL;
    }

    b->pos = (uint8_t *)b + sizeof(cos_buf_t);
    b->start = b->pos;
    b->last = b->start;
    b->end = b->last + size;
    cos_list_init(&b->node);

    return b;
}

cos_buf_t *cos_buf_pack(cos_pool_t *p, const void *data, int size)
{
    cos_buf_t* b;

    b = cos_palloc(p, sizeof(cos_buf_t));
    if (b == NULL) {
        return NULL;
    }

    b->pos = (uint8_t *)data;
    b->start = b->pos;
    b->last = b->start + size;
    b->end = b->last;
    cos_list_init(&b->node);

    return b;
}

int64_t cos_buf_list_len(cos_list_t *list)
{
    cos_buf_t *b;
    int64_t len = 0;

    cos_list_for_each_entry(cos_buf_t, b, list, node) {
        len += cos_buf_size(b);
    }

    return len;
}

char *cos_buf_list_content(cos_pool_t *p, cos_list_t *list)
{
    int64_t body_len;
    char *buf;
    int64_t pos = 0;
    int64_t size = 0;
    cos_buf_t *content;

    body_len = cos_buf_list_len(list);
    buf = cos_pcalloc(p, (size_t)(body_len + 1));
    buf[body_len] = '\0';
    cos_list_for_each_entry(cos_buf_t, content, list, node) {
        size = cos_buf_size(content);
        memcpy(buf + pos, content->pos, (size_t)(size));
        pos += size;
    }
    return buf;
}

cos_file_buf_t *cos_create_file_buf(cos_pool_t *p)
{
    return (cos_file_buf_t*)cos_pcalloc(p, sizeof(cos_file_buf_t));
}

int cos_open_file_for_read(cos_pool_t *p, const char *path, cos_file_buf_t *fb)
{
    int s;
    char buf[256];
    apr_finfo_t finfo;

    if ((s = apr_file_open(&fb->file, path, APR_READ, APR_UREAD | APR_GREAD, p)) != APR_SUCCESS) {
        cos_error_log("apr_file_open failure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        assert(fb->file == NULL);
        return COSE_OPEN_FILE_ERROR;
    }

    if ((s = apr_file_info_get(&finfo, APR_FINFO_NORM, fb->file)) != APR_SUCCESS) {
        apr_file_close(fb->file);
        cos_error_log("apr_file_open failure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        return COSE_FILE_INFO_ERROR;
    }
    fb->file_pos = 0;
    fb->file_last = finfo.size;
    fb->owner = 1;

    return COSE_OK;
}

int cos_open_file_for_all_read(cos_pool_t *p, const char *path, cos_file_buf_t *fb)
{
    return cos_open_file_for_read(p, path, fb);
}

int cos_open_file_for_range_read(cos_pool_t *p, const char *path,
    int64_t file_pos, int64_t file_last, cos_file_buf_t *fb)
{
    int s;

    s = cos_open_file_for_read(p, path, fb);
    if (s == COSE_OK) {
        if (file_pos > fb->file_pos) {
            if (file_pos > fb->file_last) {
                cos_warn_log("read range beyond file size, read start:%" APR_INT64_T_FMT ", file size:%" APR_INT64_T_FMT "\n",
                    file_pos, fb->file_last);
                file_pos = fb->file_last;
            }
            fb->file_pos = file_pos;
        }
        if (file_last < fb->file_last) {
            fb->file_last = file_last;
        }
        apr_file_seek(fb->file, APR_SET, (apr_off_t *)&fb->file_pos);
    }

    return s;
}

int cos_open_file_for_write(cos_pool_t *p, const char *path, cos_file_buf_t *fb)
{
    int s;
    char buf[256];

    if ((s = apr_file_open(&fb->file, path, APR_CREATE | APR_WRITE | APR_TRUNCATE,
                APR_UREAD | APR_UWRITE | APR_GREAD, p)) != APR_SUCCESS) {
        cos_error_log("apr_file_open failure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        assert(fb->file == NULL);
        return COSE_OPEN_FILE_ERROR;
    }
    fb->owner = 1;

    return COSE_OK;
}

int cos_open_file_for_range_write(cos_pool_t *p, const char *path, int64_t file_pos, int64_t file_last, cos_file_buf_t *fb)
{
    int s;
    char buf[256];

    if ((s = apr_file_open(&fb->file, path, APR_CREATE | APR_WRITE,
                APR_UREAD | APR_UWRITE | APR_GREAD, p)) != APR_SUCCESS) {
        cos_error_log("apr_file_open failure, code:%d %s.", s, apr_strerror(s, buf, sizeof(buf)));
        assert(fb->file == NULL);
        return COSE_OPEN_FILE_ERROR;
    }
    fb->owner = 1;
    fb->file_pos = file_pos;
    fb->file_last = file_last;
    apr_file_seek(fb->file, APR_SET, (apr_off_t *)&fb->file_pos);

    return COSE_OK;
}


void cos_buf_append_string(cos_pool_t *p, cos_buf_t *b, const char *str, int len)
{
    int size;
    int nsize;
    int remain;
    char *buf;

    if (len <= 0) return;

    remain = b->end - b->last;

    if (remain > len + 128) {
        memcpy(b->last, str, len);
        b->last += len;
    } else {
        size = cos_buf_size(b);
        nsize = (size + len) * 2;
        buf = cos_palloc(p, nsize);
        memcpy(buf, b->pos, size);
        memcpy(buf+size, str, len);
        b->start = (uint8_t *)buf;
        b->end = (uint8_t *)buf + nsize;
        b->pos = (uint8_t *)buf;
        b->last = (uint8_t *)buf + size + len;
    }
}
