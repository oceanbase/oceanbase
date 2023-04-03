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

#ifndef PARSER_UTILITY_H_
#define PARSER_UTILITY_H_

#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

int parser_to_hex_cstr(const void *in_data, const int64_t data_length, char *buff,
                const int64_t buff_size);


int parser_to_hex_cstr_(const void *in_data, const int64_t data_length,
                 char *buf, const int64_t buf_len,
                 int64_t *pos,
                 int64_t *cstr_pos);

#ifdef __cplusplus
}
#endif
#endif