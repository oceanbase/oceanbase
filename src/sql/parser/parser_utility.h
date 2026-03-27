/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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