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

#ifndef OCEANBASE_COMMON_LLVM_SYMBOLIZE_H_
#define OCEANBASE_COMMON_LLVM_SYMBOLIZE_H_
#ifndef ENABLE_SANITY
#else
#include <stdint.h>
#include <stddef.h>
#include <functional>
namespace oceanbase
{
namespace common
{
extern void print_symbol(void *addr, const char *func_name, const char *file_name, uint32_t line);
using SymbolizeCb = std::function<decltype(print_symbol)>;
extern int backtrace_symbolize(void **addrs, int32_t n_addr, SymbolizeCb cb);
extern int backtrace_symbolize(void **addrs, int32_t n_addr);
} // end namespace common
} // end namespace oceanbase

#endif //ENABLE_SANITY
#endif //OCEANBASE_COMMON_LLVM_SYMBOLIZE_H_
