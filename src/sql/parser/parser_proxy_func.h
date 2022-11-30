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

#ifndef PARSER_ALLOC_FUNC_H_
#define PARSER_ALLOC_FUNC_H_

#include <stdint.h>
// ObSQLParser模块抽取了一个静态链接库给Proxy，Proxy必须要自己实现以下几个函数才能正确链接

void *parser_alloc_buffer(void *malloc_pool, const int64_t alloc_size);
void parser_free_buffer(void *malloc_pool, void *buffer);

#ifdef SQL_PARSER_COMPILATION
#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

extern bool check_stack_overflow_c();
extern int check_stack_overflow_in_c(int *check_overflow);
extern void right_to_die_or_duty_to_live_c();

#ifdef __cplusplus
}
#endif // __cplusplus
#endif // SQL_PARSER_COMPILATION

#endif // !PARSER_ALLOC_FUNC_H_
