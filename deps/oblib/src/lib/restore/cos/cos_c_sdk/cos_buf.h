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

#ifndef LIBCOS_BUF_H
#define LIBCOS_BUF_H

#include "cos_sys_define.h"
#include "cos_list.h"

COS_CPP_START

typedef struct {
    cos_list_t node;
    uint8_t *pos;
    uint8_t *last;
    uint8_t *start;
    uint8_t *end;
} cos_buf_t;

typedef struct {
    cos_list_t node;
    int64_t file_pos;
    int64_t file_last;
    apr_file_t *file;
    uint32_t owner:1;
} cos_file_buf_t;

cos_buf_t *cos_create_buf(cos_pool_t *p, int size);
#define cos_buf_size(b) (b->last - b->pos)

cos_file_buf_t *cos_create_file_buf(cos_pool_t *p);

cos_buf_t *cos_buf_pack(cos_pool_t *p, const void *data, int size);

int64_t cos_buf_list_len(cos_list_t *list);

char *cos_buf_list_content(cos_pool_t *p, cos_list_t *list);

void cos_buf_append_string(cos_pool_t *p, cos_buf_t *b, const char *str, int len);

/**
 * @param fb file_pos, file_last equal file_size.
 * @return COSE_OK success, other failure.
 */
int cos_open_file_for_read(cos_pool_t *p, const char *path, cos_file_buf_t *fb);

int cos_open_file_for_all_read(cos_pool_t *p, const char *path, cos_file_buf_t *fb);

int cos_open_file_for_range_read(cos_pool_t *p, const char *path,
                                 int64_t file_pos, int64_t file_last,
                                 cos_file_buf_t *fb);

/**
 * create the file if not there, truncate if file exists.
 * @param fb not check file_pos, file_last.
 * @return COSE_OK success, other failure.
 */
int cos_open_file_for_write(cos_pool_t *p, const char *path, cos_file_buf_t *fb);

int cos_open_file_for_range_write(cos_pool_t *p, const char *path, int64_t file_pos, int64_t file_last, cos_file_buf_t *fb);


COS_CPP_END

#endif
